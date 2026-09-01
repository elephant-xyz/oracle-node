#!/usr/bin/env node
/**
 * Extract and normalize embedded Accela `permitInfo` arrays from Hillsborough
 * full-run parcel-data.json files into normalized JSONL with trade classifications.
 *
 * Designed for full-county streaming (524k parcels) with constant memory footprint.
 *
 * @module scripts/hillsborough/extract-full-permits
 */

import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { createInterface } from "node:readline";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";

/**
 * @typedef {object} SeedRow
 * @property {string} folio - 10-digit folio id.
 * @property {string} pin - Appraisal PIN / request_identifier.
 * @property {string} address - Site address from seed CSV.
 * @property {string} city - City name from seed CSV.
 * @property {string} zip - 5-digit ZIP from seed CSV.
 * @property {string} owner - Owner name from seed CSV.
 */

/**
 * @typedef {object} ParcelPermitInfo
 * @property {string | number | null | undefined} [id] - Portal row id when present.
 * @property {string | null | undefined} [permitNum] - Accela permit number.
 * @property {string | null | undefined} [issueDate] - Issue date text (M/D/YYYY).
 * @property {string | null | undefined} [permitType] - Short type code.
 * @property {string | null | undefined} [descr] - Work description.
 * @property {string | null | undefined} [estValue] - Estimated value text.
 * @property {string | null | undefined} [permitUrl] - Accela deep link.
 * @property {string | null | undefined} [propertyType] - Property type code.
 */

/**
 * @typedef {object} ParcelDataFile
 * @property {string | null | undefined} [pin] - Appraisal PIN.
 * @property {string | null | undefined} [siteAddress] - Site address string.
 * @property {ParcelPermitInfo[] | null | undefined} [permitInfo] - Embedded permits.
 */

/**
 * @typedef {object} NormalizedPermitRecord
 * @property {string} source_system - City/agency slug (`tampa_accela` | `hcfl_accela` | `hillsborough_embedded`).
 * @property {string | null} source_url - Accela URL when present.
 * @property {string} city - Jurisdiction label.
 * @property {string} permit_number - Permit number.
 * @property {string} parcel_identifier - Folio digits for display/join.
 * @property {string} request_identifier - PIN used as appraisal folio key.
 * @property {string} work_location - Site address with city/state/ZIP when known.
 * @property {string | null} permit_issue_date - ISO date when parseable.
 * @property {string | null} record_status - Status code or null.
 * @property {string | null} record_type - Permit type or description shorthand.
 * @property {string | null} project_description - Full description text.
 * @property {boolean} is_roof_permit - Heuristic roofing flag.
 * @property {boolean} is_hvac_permit - Heuristic HVAC flag.
 * @property {boolean} is_solar_permit - Heuristic solar flag.
 * @property {boolean} is_pool_permit - Heuristic pool flag.
 * @property {boolean} is_electrical_permit - Heuristic electrical flag.
 * @property {boolean} is_plumbing_permit - Heuristic plumbing flag.
 * @property {string | null} estimated_value - Raw estValue text preserved.
 * @property {"TAMPA" | "HCFL" | null} jurisdiction_hint - TAMPA | HCFL | null from URL.
 * @property {{ detail_id: string, folio: string, pin: string, raw: ParcelPermitInfo }} raw - Provenance.
 */

/**
 * @typedef {object} FullExtractOptions
 * @property {string} runDir - Directory of folio subdirs with parcel-data.json.
 * @property {string} seedPath - Seed CSV path.
 * @property {string} outputJsonl - Output JSONL path.
 * @property {string} scorecardPath - JSON scorecard path.
 */

/**
 * @typedef {object} FullPermitScorecard
 * @property {number} parcelCountScanned - Total parcels scanned.
 * @property {number} parcelsWithPermits - Parcels with ≥1 permitInfo row.
 * @property {number} totalPermitsEmitted - Total permit rows emitted.
 * @property {number} withAccelaUrl - Rows with permitUrl.
 * @property {object} tradeCounts - Trade breakdown.
 * @property {number} tradeCounts.roofing - Roofing permit count.
 * @property {number} tradeCounts.hvac - HVAC permit count.
 * @property {number} tradeCounts.solar - Solar permit count.
 * @property {number} tradeCounts.pool - Pool permit count.
 * @property {number} tradeCounts.electrical - Electrical permit count.
 * @property {number} tradeCounts.plumbing - Plumbing permit count.
 * @property {object} byJurisdiction - URL agency counts.
 * @property {number} byJurisdiction.tampa - City of Tampa count.
 * @property {number} byJurisdiction.hcfl - Hillsborough County count.
 * @property {number} byJurisdiction.unknown - Unknown jurisdiction count.
 * @property {object} issueDateRange - Date range.
 * @property {string | null} issueDateRange.min - Earliest ISO date.
 * @property {string | null} issueDateRange.max - Latest ISO date.
 * @property {Record<string, number>} topRecordTypes - Top permitType frequencies.
 * @property {string[]} sampleRoofingPermitNumbers - Sample roofing permit numbers.
 */

export const ROOFING_PATTERN =
  /\b(roof|reroof|re-roof|shingle|membrane|tpo|built[\s-]?up|tile\s+roof)\b/i;
export const HVAC_PATTERN =
  /\b(hvac|air\s*condition|a\/c|heat\s*pump|furnace|condenser|mechanical|duct)\b/i;
export const SOLAR_PATTERN =
  /\b(solar|photovoltaic|pv\s*system|pv\s*array|inverter)\b/i;
export const POOL_PATTERN =
  /\b(pool|spa|swimming\s*pool|pool\s*cage|pool\s*enclosure)\b/i;
export const ELECTRICAL_PATTERN =
  /\b(electric|panel\s*upgrade|service\s*change|generator|rewir)\b/i;
export const PLUMBING_PATTERN =
  /\b(plumb|water\s*heater|repipe|sewer|backflow)\b/i;

/**
 * @param {readonly string[]} argv - CLI args.
 * @returns {FullExtractOptions}
 */
export function parseFullExtractArgs(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      "run-dir": { type: "string" },
      seed: { type: "string" },
      output: { type: "string" },
      scorecard: { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });
  return {
    runDir:
      typeof values["run-dir"] === "string"
        ? values["run-dir"]
        : "downloads/hillsborough/full-run",
    seedPath:
      typeof values.seed === "string"
        ? values.seed
        : "downloads/hillsborough/full-seed.csv",
    outputJsonl:
      typeof values.output === "string"
        ? values.output
        : "downloads/hillsborough/full-permits/normalized-permits.jsonl",
    scorecardPath:
      typeof values.scorecard === "string"
        ? values.scorecard
        : "downloads/hillsborough/full-permits/scorecard.json",
  };
}

/**
 * Parse M/D/YYYY (or MM/DD/YYYY) into YYYY-MM-DD.
 *
 * @param {unknown} value - Raw date text.
 * @returns {string | null}
 */
export function parseUsDateToIso(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  const match = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(trimmed);
  if (!match) return null;
  const month = Number(match[1]);
  const day = Number(match[2]);
  const year = Number(match[3]);
  if (
    !Number.isFinite(month) ||
    !Number.isFinite(day) ||
    !Number.isFinite(year)
  ) {
    return null;
  }
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

/**
 * @param {string | null | undefined} permitUrl - Accela URL.
 * @returns {"TAMPA" | "HCFL" | null}
 */
export function jurisdictionHintFromUrl(permitUrl) {
  if (typeof permitUrl !== "string" || permitUrl.length === 0) return null;
  const upper = permitUrl.toUpperCase();
  if (upper.includes("/TAMPA/")) return "TAMPA";
  if (upper.includes("/HCFL/")) return "HCFL";
  return null;
}

/**
 * @param {ParcelPermitInfo} permit - Embedded permit row.
 * @returns {object}
 */
export function classifyTradePermit(permit) {
  const haystack = [permit.descr, permit.permitType, permit.permitNum]
    .filter((part) => typeof part === "string")
    .join(" ");

  return {
    isRoof: ROOFING_PATTERN.test(haystack),
    isHvac: HVAC_PATTERN.test(haystack),
    isSolar: SOLAR_PATTERN.test(haystack),
    isPool: POOL_PATTERN.test(haystack),
    isElectrical: ELECTRICAL_PATTERN.test(haystack),
    isPlumbing: PLUMBING_PATTERN.test(haystack),
  };
}

/**
 * @param {object} params
 * @param {ParcelPermitInfo} params.permit - Embedded row.
 * @param {SeedRow} params.seed - Seed metadata for the folio.
 * @param {string | null | undefined} params.siteAddress - ParcelData site address.
 * @returns {NormalizedPermitRecord | null}
 */
export function normalizeEmbeddedPermit(params) {
  const permitNumber =
    typeof params.permit.permitNum === "string"
      ? params.permit.permitNum.trim()
      : "";
  if (!permitNumber) return null;

  const jurisdiction = jurisdictionHintFromUrl(
    typeof params.permit.permitUrl === "string"
      ? params.permit.permitUrl
      : null,
  );
  const sourceSystem =
    jurisdiction === "TAMPA"
      ? "tampa_accela"
      : jurisdiction === "HCFL"
        ? "hcfl_accela"
        : "hillsborough_embedded";
  const city =
    jurisdiction === "TAMPA"
      ? "Tampa"
      : jurisdiction === "HCFL"
        ? "Hillsborough County"
        : params.seed.city || "Hillsborough";

  const site =
    (typeof params.siteAddress === "string" && params.siteAddress.trim()) ||
    params.seed.address;
  const workLocationParts = [site];
  if (
    params.seed.city &&
    !site.toUpperCase().includes(params.seed.city.toUpperCase())
  ) {
    workLocationParts.push(params.seed.city);
  }
  workLocationParts.push("FL");
  if (params.seed.zip) workLocationParts.push(params.seed.zip);
  const workLocation = workLocationParts.filter((p) => p.length > 0).join(", ");

  const detailId =
    params.permit.id !== undefined && params.permit.id !== null
      ? String(params.permit.id)
      : `${params.seed.folio}:${permitNumber}`;

  const recordType =
    typeof params.permit.permitType === "string" &&
    params.permit.permitType.trim()
      ? params.permit.permitType.trim()
      : null;
  const description =
    typeof params.permit.descr === "string" && params.permit.descr.trim()
      ? params.permit.descr.trim()
      : null;

  const trades = classifyTradePermit(params.permit);

  const directAccelaUrl =
    jurisdiction === "TAMPA" || jurisdiction === "HCFL"
      ? `https://aca-prod.accela.com/${jurisdiction}/Cap/CapDetail.aspx?Module=Building&TabName=Building&altId=${encodeURIComponent(permitNumber)}`
      : typeof params.permit.permitUrl === "string" &&
          params.permit.permitUrl.trim()
        ? params.permit.permitUrl.trim()
        : null;

  return {
    source_system: sourceSystem,
    source_url: directAccelaUrl,
    city,
    permit_number: permitNumber,
    parcel_identifier: params.seed.folio,
    request_identifier: params.seed.pin,
    work_location: workLocation,
    permit_issue_date: parseUsDateToIso(params.permit.issueDate),
    record_status: null,
    record_type:
      recordType ?? (description !== null ? description.slice(0, 64) : null),
    project_description: description,
    is_roof_permit: trades.isRoof,
    is_hvac_permit: trades.isHvac,
    is_solar_permit: trades.isSolar,
    is_pool_permit: trades.isPool,
    is_electrical_permit: trades.isElectrical,
    is_plumbing_permit: trades.isPlumbing,
    estimated_value:
      typeof params.permit.estValue === "string"
        ? params.permit.estValue
        : null,
    jurisdiction_hint: jurisdiction,
    raw: {
      detail_id: detailId,
      folio: params.seed.folio,
      pin: params.seed.pin,
      raw: params.permit,
    },
  };
}

/**
 * Split CSV line handling quotes.
 * @param {string} line
 * @returns {string[]}
 */
function splitCsvLine(line) {
  /** @type {string[]} */
  const out = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (line[i + 1] === '"') {
          cur += '"';
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        cur += ch;
      }
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch === ",") {
      out.push(cur);
      cur = "";
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

/**
 * Stream extract permits across full county dataset with constant memory footprint.
 * @param {FullExtractOptions} options
 * @returns {Promise<FullPermitScorecard>}
 */
export async function streamExtractFullCountyPermits(options) {
  await mkdir(path.dirname(options.outputJsonl), { recursive: true });
  await mkdir(path.dirname(options.scorecardPath), { recursive: true });

  const outStream = createWriteStream(options.outputJsonl, {
    encoding: "utf8",
  });

  const rl = createInterface({
    input: createReadStream(options.seedPath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  /** @type {string[] | null} */
  let header = null;
  let parcelCountScanned = 0;
  let parcelsWithPermits = 0;
  let totalPermitsEmitted = 0;
  let withAccelaUrl = 0;
  let minDate = /** @type {string | null} */ (null);
  let maxDate = /** @type {string | null} */ (null);

  const tradeCounts = {
    roofing: 0,
    hvac: 0,
    solar: 0,
    pool: 0,
    electrical: 0,
    plumbing: 0,
  };

  const byJurisdiction = {
    tampa: 0,
    hcfl: 0,
    unknown: 0,
  };

  /** @type {Record<string, number>} */
  const topRecordTypes = {};
  /** @type {string[]} */
  const sampleRoofingPermitNumbers = [];

  let loggedProgress = 0;
  const BATCH_SIZE = 256;
  /** @type {SeedRow[]} */
  let currentBatch = [];

  /**
   * Process a batch of seed rows in parallel.
   * @param {SeedRow[]} batch
   */
  async function processBatch(batch) {
    if (batch.length === 0) return;

    const readPromises = batch.map(async (seed) => {
      const parcelPath = path.join(
        options.runDir,
        seed.folio,
        "parcel-data.json",
      );
      try {
        const rawText = await readFile(parcelPath, "utf8");
        return {
          seed,
          parsed: /** @type {ParcelDataFile} */ (JSON.parse(rawText)),
        };
      } catch {
        return { seed, parsed: null };
      }
    });

    const results = await Promise.all(readPromises);
    let outputBuffer = "";

    for (const { seed, parsed } of results) {
      parcelCountScanned += 1;
      if (!parsed) continue;

      const permitInfo = Array.isArray(parsed.permitInfo)
        ? parsed.permitInfo
        : [];
      if (permitInfo.length === 0) continue;

      parcelsWithPermits += 1;

      for (const rawPermit of permitInfo) {
        const norm = normalizeEmbeddedPermit({
          permit: rawPermit,
          seed,
          siteAddress: parsed.siteAddress,
        });
        if (!norm) continue;

        totalPermitsEmitted += 1;
        if (norm.source_url) withAccelaUrl += 1;

        if (norm.is_roof_permit) {
          tradeCounts.roofing += 1;
          if (sampleRoofingPermitNumbers.length < 25) {
            sampleRoofingPermitNumbers.push(norm.permit_number);
          }
        }
        if (norm.is_hvac_permit) tradeCounts.hvac += 1;
        if (norm.is_solar_permit) tradeCounts.solar += 1;
        if (norm.is_pool_permit) tradeCounts.pool += 1;
        if (norm.is_electrical_permit) tradeCounts.electrical += 1;
        if (norm.is_plumbing_permit) tradeCounts.plumbing += 1;

        if (norm.jurisdiction_hint === "TAMPA") byJurisdiction.tampa += 1;
        else if (norm.jurisdiction_hint === "HCFL") byJurisdiction.hcfl += 1;
        else byJurisdiction.unknown += 1;

        if (norm.record_type) {
          topRecordTypes[norm.record_type] =
            (topRecordTypes[norm.record_type] ?? 0) + 1;
        }

        if (norm.permit_issue_date) {
          if (!minDate || norm.permit_issue_date < minDate)
            minDate = norm.permit_issue_date;
          if (!maxDate || norm.permit_issue_date > maxDate)
            maxDate = norm.permit_issue_date;
        }

        outputBuffer += `${JSON.stringify(norm)}\n`;
      }
    }

    if (outputBuffer.length > 0) {
      outStream.write(outputBuffer);
    }

    if (parcelCountScanned - loggedProgress >= 5000) {
      loggedProgress = parcelCountScanned;
      const progressPayload = {
        status: "running",
        parcelsScanned: parcelCountScanned,
        seedTotal: 527880,
        parcelsWithPermits,
        totalPermitsEmitted,
        withAccelaUrl,
        tradeCounts,
        byJurisdiction,
        updatedAt: new Date().toISOString(),
      };
      const progressPath = path.join(
        path.dirname(options.outputJsonl),
        "progress.json",
      );
      writeFile(
        progressPath,
        JSON.stringify(progressPayload, null, 2),
        "utf8",
      ).catch(() => {});
    }
  }

  for await (const line of rl) {
    if (!line.trim()) continue;
    if (!header) {
      header = splitCsvLine(line.replace(/^\uFEFF/, ""));
      continue;
    }

    const cols = splitCsvLine(line);
    /** @type {Record<string, string>} */
    const obj = {};
    for (let c = 0; c < header.length; c += 1) {
      const key = header[c];
      if (key !== undefined) obj[key] = cols[c] ?? "";
    }

    const folio = (obj.folio ?? obj.parcel_id ?? "").trim();
    const pin = (obj.pin ?? obj.source_identifier ?? "").trim();
    if (!folio || !pin) continue;

    /** @type {SeedRow} */
    const seed = {
      folio,
      pin,
      address: (obj.address ?? "").trim(),
      city: (obj.city ?? "").trim(),
      zip: (obj.zip ?? "").trim().slice(0, 5),
      owner: (obj.owner ?? "").trim(),
    };

    currentBatch.push(seed);

    if (currentBatch.length >= BATCH_SIZE) {
      await processBatch(currentBatch);
      currentBatch = [];
    }
  }

  if (currentBatch.length > 0) {
    await processBatch(currentBatch);
    currentBatch = [];
  }

  outStream.end();

  // Top 20 record types sorted by frequency
  const sortedTypes = Object.fromEntries(
    Object.entries(topRecordTypes)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 20),
  );

  /** @type {FullPermitScorecard} */
  const scorecard = {
    parcelCountScanned,
    parcelsWithPermits,
    totalPermitsEmitted,
    withAccelaUrl,
    tradeCounts,
    byJurisdiction,
    issueDateRange: {
      min: minDate,
      max: maxDate,
    },
    topRecordTypes: sortedTypes,
    sampleRoofingPermitNumbers,
  };

  await writeFile(
    options.scorecardPath,
    JSON.stringify(scorecard, null, 2),
    "utf8",
  );

  const finalProgress = {
    status: "completed",
    parcelsScanned: parcelCountScanned,
    seedTotal: 527880,
    parcelsWithPermits,
    totalPermitsEmitted,
    withAccelaUrl,
    tradeCounts,
    byJurisdiction,
    updatedAt: new Date().toISOString(),
  };
  const progressPath = path.join(
    path.dirname(options.outputJsonl),
    "progress.json",
  );
  await writeFile(progressPath, JSON.stringify(finalProgress, null, 2), "utf8");

  return scorecard;
}

async function main() {
  const options = parseFullExtractArgs(process.argv.slice(2));
  console.log(
    JSON.stringify({
      event: "permit_extraction_started",
      runDir: options.runDir,
      seedPath: options.seedPath,
      outputJsonl: options.outputJsonl,
    }),
  );

  const t0 = Date.now();
  const scorecard = await streamExtractFullCountyPermits(options);
  const elapsedSec = ((Date.now() - t0) / 1000).toFixed(1);

  console.log(
    JSON.stringify({
      event: "permit_extraction_completed",
      elapsedSec: `${elapsedSec}s`,
      scorecard,
    }),
  );
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
