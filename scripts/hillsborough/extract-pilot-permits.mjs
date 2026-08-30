#!/usr/bin/env node
/**
 * Extract embedded Accela `permitInfo` rows from Hillsborough pilot ParcelData
 * JSON into normalized city-permit JSONL for `run-permits-local-load.ts`.
 */

import { createWriteStream } from "node:fs";
import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
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
 * @typedef {object} NormalizedPilotPermit
 * @property {string} source_system - City/agency slug (`tampa_accela` | `hcfl_accela` | `hillsborough_embedded`).
 * @property {string | null} source_url - Accela URL when present.
 * @property {string} city - Jurisdiction label.
 * @property {string} permit_number - Permit number.
 * @property {string} parcel_identifier - Folio digits for display/join.
 * @property {string} request_identifier - PIN used as appraisal folio key.
 * @property {string} work_location - Site address with city/state/ZIP when known.
 * @property {string | null} permit_issue_date - ISO date when parseable.
 * @property {string | null} record_status - Always null for embedded rows.
 * @property {string | null} record_type - Permit type / description shorthand.
 * @property {string | null} project_description - Full description text.
 * @property {boolean} is_roof_permit - Heuristic roofing flag.
 * @property {string | null} estimated_value - Raw estValue text preserved.
 * @property {string | null} jurisdiction_hint - TAMPA | HCFL | null from URL.
 * @property {{ detail_id: string, folio: string, pin: string, raw: ParcelPermitInfo }} raw - Provenance.
 */

/**
 * @typedef {object} ExtractOptions
 * @property {string} pilotRunDir - Directory of folio subdirs with parcel-data.json.
 * @property {string} seedPath - Pilot seed CSV path.
 * @property {string} outputJsonl - Output JSONL path.
 * @property {string} scorecardPath - JSON scorecard path.
 */

/**
 * @typedef {object} PermitScorecard
 * @property {number} parcelCount - Parcels scanned.
 * @property {number} parcelsWithPermits - Parcels with ≥1 permitInfo row.
 * @property {number} permitCount - Total permitInfo rows emitted.
 * @property {number} withAccelaUrl - Rows with permitUrl.
 * @property {number} roofingRelatedCount - Rows matching roofing heuristics.
 * @property {{ tampa: number, hcfl: number, unknown: number }} byJurisdiction - URL agency counts.
 * @property {{ min: string | null, max: string | null }} issueDateRange - ISO date range.
 * @property {Record<string, number>} recordTypeCounts - Top permitType frequencies.
 * @property {string[]} sampleRoofingPermitNumbers - Sample roofing permit numbers.
 */

const ROOFING_PATTERN = /\b(roof|reroof|re-roof|shingle|membrane|tpo|built[\s-]?up)\b/i;

/**
 * @param {readonly string[]} argv - CLI args after script name.
 * @returns {ExtractOptions}
 */
export function parseExtractPilotPermitsArgs(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      "pilot-run-dir": { type: "string" },
      seed: { type: "string" },
      output: { type: "string" },
      scorecard: { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });
  return {
    pilotRunDir:
      typeof values["pilot-run-dir"] === "string"
        ? values["pilot-run-dir"]
        : "downloads/hillsborough/pilot-run",
    seedPath:
      typeof values.seed === "string"
        ? values.seed
        : "downloads/hillsborough/pilot-seed-50.csv",
    outputJsonl:
      typeof values.output === "string"
        ? values.output
        : "downloads/hillsborough/pilot-permits/normalized-permits.jsonl",
    scorecardPath:
      typeof values.scorecard === "string"
        ? values.scorecard
        : "downloads/hillsborough/pilot-permits/scorecard.json",
  };
}

/**
 * Parse a minimal CSV with quoted fields (same shape as pilot seed).
 *
 * @param {string} text - CSV text including header.
 * @returns {SeedRow[]}
 */
export function parseSeedCsvForPermits(text) {
  const lines = text.replace(/^\uFEFF/, "").split(/\r?\n/).filter((line) => line.length > 0);
  if (lines.length === 0) return [];
  const header = splitCsvLine(lines[0] ?? "");
  /** @type {SeedRow[]} */
  const rows = [];
  for (let i = 1; i < lines.length; i += 1) {
    const cols = splitCsvLine(lines[i] ?? "");
    /** @type {Record<string, string>} */
    const obj = {};
    for (let c = 0; c < header.length; c += 1) {
      const key = header[c];
      if (key !== undefined) obj[key] = cols[c] ?? "";
    }
    const folio = (obj.folio ?? obj.parcel_id ?? "").trim();
    const pin = (obj.pin ?? obj.source_identifier ?? "").trim();
    if (!folio || !pin) continue;
    rows.push({
      folio,
      pin,
      address: (obj.address ?? "").trim(),
      city: (obj.city ?? "").trim(),
      zip: (obj.zip ?? "").trim().slice(0, 5),
      owner: (obj.owner ?? "").trim(),
    });
  }
  return rows;
}

/**
 * @param {string} line - One CSV line.
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
  if (!Number.isFinite(month) || !Number.isFinite(day) || !Number.isFinite(year)) {
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
 * @returns {boolean}
 */
export function isRoofingRelatedPermit(permit) {
  const haystack = [permit.descr, permit.permitType, permit.permitNum]
    .filter((part) => typeof part === "string")
    .join(" ");
  return ROOFING_PATTERN.test(haystack);
}

/**
 * @param {object} params
 * @param {ParcelPermitInfo} params.permit - Embedded row.
 * @param {SeedRow} params.seed - Seed metadata for the folio.
 * @param {string | null | undefined} params.siteAddress - ParcelData site address.
 * @returns {NormalizedPilotPermit | null}
 */
export function normalizeEmbeddedPermit(params) {
  const permitNumber =
    typeof params.permit.permitNum === "string" ? params.permit.permitNum.trim() : "";
  if (!permitNumber) return null;

  const jurisdiction = jurisdictionHintFromUrl(
    typeof params.permit.permitUrl === "string" ? params.permit.permitUrl : null,
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
  if (params.seed.city && !site.toUpperCase().includes(params.seed.city.toUpperCase())) {
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
    typeof params.permit.permitType === "string" && params.permit.permitType.trim()
      ? params.permit.permitType.trim()
      : null;
  const description =
    typeof params.permit.descr === "string" && params.permit.descr.trim()
      ? params.permit.descr.trim()
      : null;

  return {
    source_system: sourceSystem,
    source_url:
      typeof params.permit.permitUrl === "string" && params.permit.permitUrl.trim()
        ? params.permit.permitUrl.trim()
        : null,
    city,
    permit_number: permitNumber,
    parcel_identifier: params.seed.folio,
    request_identifier: params.seed.pin,
    work_location: workLocation,
    permit_issue_date: parseUsDateToIso(params.permit.issueDate),
    record_status: null,
    record_type: recordType ?? (description !== null ? description.slice(0, 64) : null),
    project_description: description,
    is_roof_permit: isRoofingRelatedPermit(params.permit),
    estimated_value:
      typeof params.permit.estValue === "string" ? params.permit.estValue : null,
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
 * @param {ExtractOptions} options - Paths.
 * @returns {Promise<{ records: NormalizedPilotPermit[], scorecard: PermitScorecard }>}
 */
export async function extractPilotPermits(options) {
  const seedText = await readFile(options.seedPath, "utf8");
  const seeds = parseSeedCsvForPermits(seedText);
  /** @type {Map<string, SeedRow>} */
  const seedByFolio = new Map(seeds.map((row) => [row.folio, row]));

  const entries = await readdir(options.pilotRunDir, { withFileTypes: true });
  /** @type {NormalizedPilotPermit[]} */
  const records = [];
  /** @type {Record<string, number>} */
  const recordTypeCounts = {};
  /** @type {string[]} */
  const sampleRoofingPermitNumbers = [];
  let parcelsWithPermits = 0;
  let withAccelaUrl = 0;
  let roofingRelatedCount = 0;
  /** @type {{ tampa: number, hcfl: number, unknown: number }} */
  const byJurisdiction = { tampa: 0, hcfl: 0, unknown: 0 };
  /** @type {string[]} */
  const isoDates = [];

  let parcelCount = 0;
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const folio = entry.name;
    const seed = seedByFolio.get(folio);
    if (!seed) continue;
    const parcelPath = path.join(options.pilotRunDir, folio, "parcel-data.json");
    let parsed;
    try {
      parsed = /** @type {ParcelDataFile} */ (
        JSON.parse(await readFile(parcelPath, "utf8"))
      );
    } catch {
      continue;
    }
    parcelCount += 1;
    const permitInfo = Array.isArray(parsed.permitInfo) ? parsed.permitInfo : [];
    if (permitInfo.length > 0) parcelsWithPermits += 1;

    for (const permit of permitInfo) {
      const normalized = normalizeEmbeddedPermit({
        permit,
        seed,
        siteAddress: parsed.siteAddress,
      });
      if (normalized === null) continue;
      records.push(normalized);
      if (normalized.source_url) withAccelaUrl += 1;
      if (normalized.is_roof_permit) {
        roofingRelatedCount += 1;
        if (sampleRoofingPermitNumbers.length < 10) {
          sampleRoofingPermitNumbers.push(normalized.permit_number);
        }
      }
      if (normalized.jurisdiction_hint === "TAMPA") byJurisdiction.tampa += 1;
      else if (normalized.jurisdiction_hint === "HCFL") byJurisdiction.hcfl += 1;
      else byJurisdiction.unknown += 1;
      if (normalized.permit_issue_date) isoDates.push(normalized.permit_issue_date);
      const typeKey = normalized.record_type ?? "(null)";
      recordTypeCounts[typeKey] = (recordTypeCounts[typeKey] ?? 0) + 1;
    }
  }

  isoDates.sort();
  /** @type {PermitScorecard} */
  const scorecard = {
    parcelCount,
    parcelsWithPermits,
    permitCount: records.length,
    withAccelaUrl,
    roofingRelatedCount,
    byJurisdiction,
    issueDateRange: {
      min: isoDates[0] ?? null,
      max: isoDates[isoDates.length - 1] ?? null,
    },
    recordTypeCounts,
    sampleRoofingPermitNumbers,
  };
  return { records, scorecard };
}

/**
 * @param {ExtractOptions} options - CLI options.
 * @returns {Promise<PermitScorecard>}
 */
export async function runExtractPilotPermits(options) {
  const { records, scorecard } = await extractPilotPermits(options);
  await mkdir(path.dirname(options.outputJsonl), { recursive: true });
  await mkdir(path.dirname(options.scorecardPath), { recursive: true });

  const stream = createWriteStream(options.outputJsonl, { encoding: "utf8" });
  for (const record of records) {
    const ok = stream.write(`${JSON.stringify(record)}\n`);
    if (!ok) {
      await new Promise((resolve) => stream.once("drain", resolve));
    }
  }
  await new Promise((resolve, reject) => {
    stream.end(() => resolve(undefined));
    stream.on("error", reject);
  });
  await writeFile(options.scorecardPath, `${JSON.stringify(scorecard, null, 2)}\n`, "utf8");
  return scorecard;
}

/**
 * @returns {Promise<void>}
 */
async function main() {
  const options = parseExtractPilotPermitsArgs(process.argv.slice(2));
  const scorecard = await runExtractPilotPermits(options);
  console.log(
    JSON.stringify(
      {
        event: "hillsborough_pilot_permits_extracted",
        output: options.outputJsonl,
        scorecardPath: options.scorecardPath,
        ...scorecard,
      },
      null,
      2,
    ),
  );
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
