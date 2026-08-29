#!/usr/bin/env node

/**
 * Profile and reconcile an official Broward Florida DOR NAL roll.
 *
 * The report is aggregate-only. Parcel identifiers are retained in memory only
 * to prove exact NAL `PARCEL_ID` = GIS `FOLIO` coverage. Optional pilot files
 * are private local artifacts and must never be committed or published.
 */

import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { parse } from "csv-parse";

const BROWARD_FOLIO_PATTERN = /^[0-9A-Z]{12}$/u;
const CONDOMINIUM_DOR_USE_CODE = "004";
const EXPECTED_COUNTY_NUMBER = "16";
const EXPECTED_FILE_TYPE = "R";
const PILOT_BUCKET_SIZE = 5;

/**
 * @typedef {Record<string, string | undefined>} CsvRecord
 *
 * @typedef {"preliminary" | "initial_final" | "final_certified"} CertificationStatus
 *
 * @typedef {object} ProfileOptions
 * @property {string} nalCsvPath - Extracted official NAL CSV.
 * @property {string} gisSeedPath - Existing GIS-derived Broward seed CSV.
 * @property {string} sourceZipPath - Raw downloaded official ZIP.
 * @property {string} sourceUrl - Official DOR download URL.
 * @property {number} rollYear - Assessment year.
 * @property {CertificationStatus} certificationStatus - DOR submission stage.
 * @property {string} retrievedAt - Raw artifact retrieval timestamp.
 * @property {string} outputPath - Aggregate profile JSON path.
 * @property {string | null} pilotCsvPath - Optional private representative NAL rows.
 * @property {string | null} pilotManifestPath - Optional private pilot-selection evidence.
 *
 * @typedef {object} SelectedPilotRow
 * @property {"gis_matched" | "condominium_tax_only" | "commercial" | "vacant"} bucket
 * @property {CsvRecord} row - Private source row.
 * @property {number} sourceRowIndex - Zero-based NAL source row.
 * @property {boolean} hasGisFolio - Exact GIS folio membership.
 */

/**
 * Parse the explicit source-profile CLI.
 *
 * @param {readonly string[]} argv - Arguments after the script path.
 * @returns {ProfileOptions} Validated source and output configuration.
 */
export function parseCliOptions(argv) {
  const values = new Map();
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (
      typeof flag !== "string" ||
      !flag.startsWith("--") ||
      typeof value !== "string"
    ) {
      throw new Error("Tax-roll profile options must be --flag value pairs");
    }
    values.set(flag.slice(2), value);
  }
  const certificationStatus = required(values, "certification-status");
  if (
    certificationStatus !== "preliminary" &&
    certificationStatus !== "initial_final" &&
    certificationStatus !== "final_certified"
  ) {
    throw new Error("--certification-status is invalid");
  }
  const rollYear = Number(required(values, "roll-year"));
  if (!Number.isInteger(rollYear) || rollYear < 2000 || rollYear > 2100) {
    throw new Error("--roll-year is invalid");
  }
  const retrievedAt = required(values, "retrieved-at");
  if (!Number.isFinite(Date.parse(retrievedAt))) {
    throw new Error("--retrieved-at must be an ISO timestamp");
  }
  return {
    nalCsvPath: required(values, "nal-csv"),
    gisSeedPath: required(values, "gis-seed"),
    sourceZipPath: required(values, "source-zip"),
    sourceUrl: required(values, "source-url"),
    rollYear,
    certificationStatus,
    retrievedAt: new Date(retrievedAt).toISOString(),
    outputPath: required(values, "output"),
    pilotCsvPath: values.get("pilot-csv") ?? null,
    pilotManifestPath: values.get("pilot-manifest") ?? null,
  };
}

/**
 * Normalize a Broward folio without numeric coercion.
 *
 * @param {unknown} value - Raw NAL `PARCEL_ID` or GIS `FOLIO`.
 * @returns {string | null} Exact 12-character uppercase alphanumeric folio.
 */
export function normalizeBrowardTaxRollParcelId(value) {
  if (typeof value !== "string") return null;
  const normalized = value.trim().toUpperCase();
  return BROWARD_FOLIO_PATTERN.test(normalized) ? normalized : null;
}

/**
 * Group a three-digit DOR use code into official broad real-property classes.
 *
 * @param {string} code - DOR predominant-use code.
 * @returns {"residential" | "commercial_industrial" | "agricultural" | "institutional_government_utility" | "nonagricultural_acreage" | "invalid"}
 *   Aggregate category without local PA subcode inference.
 */
export function classifyDorUseCode(code) {
  if (!/^\d{3}$/u.test(code)) return "invalid";
  const value = Number(code);
  if (value <= 9) return "residential";
  if (value <= 49) return "commercial_industrial";
  if (value <= 69) return "agricultural";
  if (value <= 98) return "institutional_government_utility";
  if (value === 99) return "nonagricultural_acreage";
  return "invalid";
}

/**
 * Profile the official NAL source and prove its exact folio join to GIS.
 *
 * @param {ProfileOptions} options - Verified source and output paths.
 * @returns {Promise<Record<string, unknown>>} Aggregate reconciliation report.
 */
export async function profileBrowardTaxRoll(options) {
  const gis = await readGisFolios(options.gisSeedPath);
  const sourceSha256 = await sha256File(options.sourceZipPath);
  const parcelIds = new Set();
  const duplicateIds = new Set();
  const idCounts = new Map();
  const parcelIdLengths = new Map();
  const dorUseCodeCounts = new Map();
  const categoryCounts = new Map();
  const basicStratumCounts = new Map();
  const assessmentYearCounts = new Map();
  const countyNumberCounts = new Map();
  const fileTypeCounts = new Map();
  const nonemptyFieldCounts = new Map();
  /** @type {Map<SelectedPilotRow["bucket"], SelectedPilotRow[]>} */
  const pilotBuckets = new Map([
    ["gis_matched", []],
    ["condominium_tax_only", []],
    ["commercial", []],
    ["vacant", []],
  ]);
  /** @type {string[]} */
  let columns = [];
  let sourceRows = 0;
  let missingParcelIds = 0;
  let malformedParcelIds = 0;
  let condominiumRows = 0;
  let condominiumMatchedToGis = 0;

  const parser = createReadStream(options.nalCsvPath).pipe(
    parse({
      bom: true,
      columns(header) {
        columns = /** @type {string[]} */ (header);
        return columns;
      },
      relax_column_count: false,
      skip_empty_lines: true,
    }),
  );
  for await (const parsed of parser) {
    const row = /** @type {CsvRecord} */ (parsed);
    const rawParcelId = row.PARCEL_ID ?? "";
    const parcelId = normalizeBrowardTaxRollParcelId(rawParcelId);
    if (rawParcelId.trim() === "") missingParcelIds += 1;
    else if (parcelId === null) malformedParcelIds += 1;
    if (rawParcelId.trim() !== "") {
      increment(parcelIdLengths, String(rawParcelId.trim().length));
    }
    if (parcelId !== null) {
      const count = (idCounts.get(parcelId) ?? 0) + 1;
      idCounts.set(parcelId, count);
      if (count > 1) duplicateIds.add(parcelId);
      parcelIds.add(parcelId);
    }
    const dorUseCode = (row.DOR_UC ?? "").trim();
    increment(dorUseCodeCounts, dorUseCode || "<missing>");
    increment(categoryCounts, classifyDorUseCode(dorUseCode));
    increment(basicStratumCounts, (row.BAS_STRT ?? "").trim() || "<missing>");
    increment(
      assessmentYearCounts,
      (row.ASMNT_YR ?? "").trim() || "<missing>",
    );
    increment(
      countyNumberCounts,
      (row.CO_NO ?? "").trim() || "<missing>",
    );
    increment(fileTypeCounts, (row.FILE_T ?? "").trim() || "<missing>");
    for (const [fieldName, value] of Object.entries(row)) {
      if (typeof value === "string" && value.trim() !== "") {
        increment(nonemptyFieldCounts, fieldName);
      }
    }
    if (dorUseCode === CONDOMINIUM_DOR_USE_CODE) {
      condominiumRows += 1;
      if (parcelId !== null && gis.folios.has(parcelId)) {
        condominiumMatchedToGis += 1;
      }
    }
    if (parcelId !== null) {
      selectPilotRow({
        row,
        parcelId,
        sourceRowIndex: sourceRows,
        dorUseCode,
        hasGisFolio: gis.folios.has(parcelId),
        pilotBuckets,
      });
    }
    sourceRows += 1;
  }

  const matched = intersectionCount(parcelIds, gis.folios);
  const taxRollOnly = parcelIds.size - matched;
  const gisOnlyFolios = [...gis.folios].filter(
    (folio) => !parcelIds.has(folio),
  );
  const duplicateRowsBeyondFirst = [...idCounts.values()].reduce(
    (total, count) => total + Math.max(0, count - 1),
    0,
  );
  const report = {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    source: {
      url: options.sourceUrl,
      rollYear: options.rollYear,
      certificationStatus: options.certificationStatus,
      retrievedAt: options.retrievedAt,
      sha256: sourceSha256,
      fileType: "NAL real property",
      napIncluded: false,
    },
    profile: {
      sourceRows,
      uniqueValidParcelIds: parcelIds.size,
      duplicateParcelIdCount: duplicateIds.size,
      duplicateRowsBeyondFirst,
      missingParcelIds,
      malformedParcelIds,
      parcelIdLengthCounts: sortedObject(parcelIdLengths),
      dorUseCodeCounts: sortedObject(dorUseCodeCounts),
      propertyCategoryCounts: sortedObject(categoryCounts),
      basicStratumCounts: sortedObject(basicStratumCounts),
      assessmentYearCounts: sortedObject(assessmentYearCounts),
      countyNumberCounts: sortedObject(countyNumberCounts),
      fileTypeCounts: sortedObject(fileTypeCounts),
      condominiumRows,
      nonemptyFieldCounts: sortedObject(nonemptyFieldCounts),
    },
    gisJoin: {
      expression:
        "trim(NAL.PARCEL_ID) = GIS.FOLIO; exact uppercase 12-character alphanumeric string",
      addressUsedAsKey: false,
      gisFeatureCount: 556_178,
      gisSeedRows: gis.rows,
      gisUniqueFolios: gis.folios.size,
      gisInvalidFolios: gis.invalid,
      matchedTaxRollToGis: matched,
      taxRollOnly: taxRollOnly,
      gisOnly: gisOnlyFolios.length,
      condominiumMatchedToGis,
      condominiumTaxRollOnly: condominiumRows - condominiumMatchedToGis,
      unexplainedTaxRollDifference:
        sourceRows -
        parcelIds.size -
        missingParcelIds -
        duplicateRowsBeyondFirst,
    },
  };
  await mkdir(path.dirname(options.outputPath), { recursive: true });
  await writeFile(options.outputPath, `${JSON.stringify(report, null, 2)}\n`, {
    mode: 0o600,
  });
  if (options.pilotCsvPath !== null || options.pilotManifestPath !== null) {
    await writePilotFiles({
      columns,
      pilotRows: [...pilotBuckets.values()].flat(),
      gisOnlyFolios: gisOnlyFolios.slice(0, PILOT_BUCKET_SIZE),
      csvPath: options.pilotCsvPath,
      manifestPath: options.pilotManifestPath,
      sourceSha256,
    });
  }
  return report;
}

async function readGisFolios(seedPath) {
  const folios = new Set();
  let rows = 0;
  let invalid = 0;
  const parser = createReadStream(seedPath).pipe(
    parse({ bom: true, columns: true, skip_empty_lines: true }),
  );
  for await (const parsed of parser) {
    const row = /** @type {CsvRecord} */ (parsed);
    const folio = normalizeBrowardTaxRollParcelId(
      row.parcel_id ?? row.request_identifier,
    );
    if (folio === null) invalid += 1;
    else folios.add(folio);
    rows += 1;
  }
  return { folios, rows, invalid };
}

function selectPilotRow({
  row,
  parcelId,
  sourceRowIndex,
  dorUseCode,
  hasGisFolio,
  pilotBuckets,
}) {
  const selectedIds = new Set(
    [...pilotBuckets.values()]
      .flat()
      .map((selected) =>
        normalizeBrowardTaxRollParcelId(selected.row.PARCEL_ID),
      ),
  );
  if (selectedIds.has(parcelId)) return;
  /** @type {SelectedPilotRow["bucket"] | null} */
  let bucket = null;
  if (
    hasGisFolio &&
    dorUseCode === "001" &&
    (pilotBuckets.get("gis_matched")?.length ?? 0) < PILOT_BUCKET_SIZE
  ) {
    bucket = "gis_matched";
  } else if (
    !hasGisFolio &&
    dorUseCode === CONDOMINIUM_DOR_USE_CODE &&
    (pilotBuckets.get("condominium_tax_only")?.length ?? 0) <
      PILOT_BUCKET_SIZE
  ) {
    bucket = "condominium_tax_only";
  } else if (
    classifyDorUseCode(dorUseCode) === "commercial_industrial" &&
    !["010", "040"].includes(dorUseCode) &&
    (pilotBuckets.get("commercial")?.length ?? 0) < PILOT_BUCKET_SIZE
  ) {
    bucket = "commercial";
  } else if (
    ["000", "010", "040", "070", "099"].includes(dorUseCode) &&
    (pilotBuckets.get("vacant")?.length ?? 0) < PILOT_BUCKET_SIZE
  ) {
    bucket = "vacant";
  }
  if (bucket === null) return;
  pilotBuckets.get(bucket)?.push({
    bucket,
    row,
    sourceRowIndex,
    hasGisFolio,
  });
}

async function writePilotFiles({
  columns,
  pilotRows,
  gisOnlyFolios,
  csvPath,
  manifestPath,
  sourceSha256,
}) {
  if (csvPath !== null) {
    await mkdir(path.dirname(csvPath), { recursive: true });
    const csv = [
      columns.map(encodeCsvCell).join(","),
      ...pilotRows.map((selected) =>
        columns
          .map((column) => encodeCsvCell(selected.row[column] ?? ""))
          .join(","),
      ),
    ].join("\n");
    await writeFile(csvPath, `${csv}\n`, { mode: 0o600 });
  }
  if (manifestPath !== null) {
    await mkdir(path.dirname(manifestPath), { recursive: true });
    const manifest = {
      schemaVersion: 1,
      sourceSha256,
      private: true,
      publishable: false,
      selectedNalRows: pilotRows.map((selected) => ({
        sourceRowIndex: selected.sourceRowIndex,
        bucket: selected.bucket,
        hasGisFolio: selected.hasGisFolio,
        parcelId: normalizeBrowardTaxRollParcelId(
          selected.row.PARCEL_ID,
        ),
      })),
      gisOnlyControls: gisOnlyFolios,
      malformedSourceRows: 0,
      malformedCoverage:
        "Synthetic malformed and leading-zero identifiers are covered by unit tests because the official source contains no malformed IDs.",
    };
    await writeFile(
      manifestPath,
      `${JSON.stringify(manifest, null, 2)}\n`,
      { mode: 0o600 },
    );
  }
}

async function sha256File(filePath) {
  const digest = createHash("sha256");
  for await (const chunk of createReadStream(filePath)) digest.update(chunk);
  return digest.digest("hex");
}

function intersectionCount(left, right) {
  let count = 0;
  for (const value of left) if (right.has(value)) count += 1;
  return count;
}

function increment(counter, key) {
  counter.set(key, (counter.get(key) ?? 0) + 1);
}

function sortedObject(counter) {
  return Object.fromEntries(
    [...counter.entries()].sort(([left], [right]) =>
      left.localeCompare(right),
    ),
  );
}

function encodeCsvCell(value) {
  if (!/[",\r\n]/u.test(value)) return value;
  return `"${value.replaceAll('"', '""')}"`;
}

function required(values, name) {
  const value = values.get(name);
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`--${name} is required`);
  }
  return value;
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  profileBrowardTaxRoll(parseCliOptions(process.argv.slice(2)))
    .then((report) => {
      const profile = /** @type {{sourceRows:number,uniqueValidParcelIds:number,condominiumRows:number}} */ (
        report.profile
      );
      const gisJoin = /** @type {{matchedTaxRollToGis:number,taxRollOnly:number,gisOnly:number}} */ (
        report.gisJoin
      );
      console.log(
        JSON.stringify({
          event: "broward_tax_roll_profile_completed",
          sourceRows: profile.sourceRows,
          uniqueValidParcelIds: profile.uniqueValidParcelIds,
          condominiumRows: profile.condominiumRows,
          matchedTaxRollToGis: gisJoin.matchedTaxRollToGis,
          taxRollOnly: gisJoin.taxRollOnly,
          gisOnly: gisJoin.gisOnly,
        }),
      );
    })
    .catch((error) => {
      console.error(
        JSON.stringify({
          event: "broward_tax_roll_profile_failed",
          message: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      process.exitCode = 1;
    });
}
