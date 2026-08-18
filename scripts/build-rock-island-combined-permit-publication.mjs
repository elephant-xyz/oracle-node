#!/usr/bin/env node
// @ts-check

/**
 * Build the combined City of Rock Island + Moline public permit table.
 *
 * The output reuses the already-published City closed schema and maps Moline's
 * private normalized rows into that same strict allowlist. No address, parcel,
 * description, contractor, valuation, person, contact, raw payload, private
 * relationship, or derived private hash is written.
 */

import { createHash } from "node:crypto";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { basename, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { createRequire } from "node:module";

import { ParquetReader, ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";

const require = createRequire(import.meta.url);
const ipfsHash = require("ipfs-only-hash");

const CITY_SOURCE_SYSTEM = "rock_island_city_official_monthly_permit_reports";
const MOLINE_SOURCE_SYSTEM = "moline_official_monthly_building_permit_reports";
const EXPECTED_CITY_ROWS = 24_786;
const EXPECTED_MOLINE_ROWS = 22_599;
const EXPECTED_TOTAL_ROWS = EXPECTED_CITY_ROWS + EXPECTED_MOLINE_ROWS;
const EXPECTED_CITY_REPORTS = 112;
const EXPECTED_MOLINE_REPORTS = 102;
const DATASET_ID = "rock-island-county-supported-issued-permit-query";
const DATASET_VERSION = "2026-08-14";
const SCHEMA_VERSION = "1.1.0";
const PUBLIC_PERMIT_FIELDS = Object.freeze([
  "permit_key",
  "source_system",
  "source_report_document_id",
  "source_report_title",
  "source_report_url",
  "permit_number",
  "permit_issue_date",
  "record_status",
  "record_type",
  "city",
  "is_roof_permit",
]);
const FORBIDDEN_KEY_PATTERN =
  /(address|parcel|pin|description|contractor|applicant|owner|person|phone|email|contact|valuation|value|property|raw|payload|hash|latitude|longitude)/iu;
const EMAIL_PATTERN = /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/iu;
const PHONE_PATTERN =
  /(?:\+?1[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]\d{3}[\s.-]\d{4}/u;
const SSN_PATTERN = /\b\d{3}-\d{2}-\d{4}\b/u;
const OFFICIAL_IDENTIFIER_FIELDS = new Set([
  "permit_key",
  "permit_number",
  "source_report_document_id",
  "source_report_url",
]);

/** @typedef {Record<string, unknown>} JsonObject */

/**
 * @typedef {object} CombinedPermitRow
 * @property {string} permit_key
 * @property {string} source_system
 * @property {string} source_report_document_id
 * @property {string} source_report_title
 * @property {string} source_report_url
 * @property {string | null} permit_number
 * @property {string} permit_issue_date
 * @property {string} record_status
 * @property {string} record_type
 * @property {string} city
 * @property {boolean} is_roof_permit
 */

/**
 * Return true only for a JSON object.
 *
 * @param {unknown} value Candidate value.
 * @returns {value is JsonObject} Whether the value is an object.
 */
function isJsonObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Read a required non-empty string.
 *
 * @param {unknown} value Candidate value.
 * @param {string} field Field name for errors.
 * @returns {string} Trimmed value.
 */
function requiredString(value, field) {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`Required combined permit field ${field} is missing`);
  }
  return value.trim();
}

/**
 * Read an optional string.
 *
 * @param {unknown} value Candidate value.
 * @returns {string | null} Trimmed value or null.
 */
function optionalString(value) {
  return typeof value === "string" && value.trim().length > 0
    ? value.trim()
    : null;
}

/**
 * Render deterministic pretty JSON with one trailing newline.
 *
 * @param {unknown} value JSON-compatible value.
 * @returns {string} Stable JSON.
 */
function stableJson(value) {
  return `${JSON.stringify(value, null, 2)}\n`;
}

/**
 * Check a digit string using the Luhn algorithm.
 *
 * @param {string} digits Candidate digits.
 * @returns {boolean} Whether the value passes.
 */
function passesLuhn(digits) {
  let sum = 0;
  let doubleDigit = false;
  for (let index = digits.length - 1; index >= 0; index -= 1) {
    let value = Number(digits[index]);
    if (doubleDigit) {
      value *= 2;
      if (value > 9) value -= 9;
    }
    sum += value;
    doubleDigit = !doubleDigit;
  }
  return sum % 10 === 0;
}

/**
 * Scan one closed public row for forbidden keys and common sensitive values.
 *
 * @param {CombinedPermitRow} row Public row.
 * @returns {string[]} Finding labels.
 */
function scanPublicPermitRow(row) {
  const findings = [];
  for (const key of Object.keys(row)) {
    if (FORBIDDEN_KEY_PATTERN.test(key)) findings.push(`forbidden_key:${key}`);
  }
  for (const [key, value] of Object.entries(row)) {
    if (typeof value !== "string") continue;
    if (EMAIL_PATTERN.test(value)) findings.push(`email_value:${key}`);
    if (PHONE_PATTERN.test(value)) findings.push(`phone_value:${key}`);
    if (SSN_PATTERN.test(value)) findings.push(`ssn_value:${key}`);
    const digits = value.replace(/\D/gu, "");
    if (
      !OFFICIAL_IDENTIFIER_FIELDS.has(key) &&
      digits.length >= 13 &&
      digits.length <= 19 &&
      passesLuhn(digits)
    ) {
      findings.push(`payment_card_like_value:${key}`);
    }
  }
  return findings;
}

/**
 * Build Moline's stable public key from exact official source identity.
 *
 * @param {JsonObject} record Private normalized Moline permit.
 * @returns {string} Public-safe deterministic permit key.
 */
export function molineCombinedPermitKey(record) {
  const permitNumber = optionalString(record.permit_number);
  if (permitNumber !== null) return `${MOLINE_SOURCE_SYSTEM}:${permitNumber}`;
  if (!isJsonObject(record.raw)) {
    throw new Error("Legacy Moline permit has no raw source identity");
  }
  const year = requiredString(
    record.raw.source_application_year,
    "raw.source_application_year",
  );
  const number = requiredString(
    record.raw.source_application_number,
    "raw.source_application_number",
  );
  const code = requiredString(
    record.raw.source_permit_code,
    "raw.source_permit_code",
  );
  const issueDate = requiredString(
    record.permit_issue_date,
    "permit_issue_date",
  );
  return `${MOLINE_SOURCE_SYSTEM}:application:${year}:${number}:${code}:issued:${issueDate}`;
}

/**
 * Map one private Moline row into the exact combined public allowlist.
 *
 * @param {JsonObject} record Private normalized Moline permit.
 * @returns {CombinedPermitRow} Closed public row.
 */
export function toMolineCombinedPermitRow(record) {
  if (record.source_system !== MOLINE_SOURCE_SYSTEM) {
    throw new Error(
      `Unapproved Moline source system ${String(record.source_system)}`,
    );
  }
  if (record.city !== "Moline") {
    throw new Error(`Unapproved Moline jurisdiction ${String(record.city)}`);
  }
  if (!isJsonObject(record.raw)) {
    throw new Error("Moline permit has no reviewed report provenance");
  }
  const status = requiredString(record.record_status, "record_status");
  if (status.toLowerCase() !== "issued") {
    throw new Error(`Unapproved Moline public status ${status}`);
  }
  if (typeof record.is_roof_permit !== "boolean") {
    throw new Error("Moline permit has invalid roof classification");
  }
  const sourceUrl = requiredString(record.source_url, "source_url");
  if (
    !/^https:\/\/www\.moline\.il\.us\/Archive\.aspx\?ADID=\d+$/u.test(sourceUrl)
  ) {
    throw new Error(`Unapproved Moline report URL ${sourceUrl}`);
  }
  return {
    permit_key: molineCombinedPermitKey(record),
    source_system: MOLINE_SOURCE_SYSTEM,
    source_report_document_id: requiredString(
      record.raw.source_archive_id,
      "raw.source_archive_id",
    ),
    source_report_title: requiredString(
      record.raw.source_report_title,
      "raw.source_report_title",
    ),
    source_report_url: sourceUrl,
    permit_number: optionalString(record.permit_number),
    permit_issue_date: requiredString(
      record.permit_issue_date,
      "permit_issue_date",
    ),
    record_status: "Issued",
    record_type: requiredString(record.record_type, "record_type"),
    city: "Moline",
    is_roof_permit: record.is_roof_permit,
  };
}

/**
 * Build the exact combined public Parquet schema.
 *
 * `permit_number` is nullable because supported legacy Moline reports publish
 * official application identity fields instead of a modern permit number.
 *
 * @returns {ParquetSchema} Closed scalar schema.
 */
export function buildCombinedPermitParquetSchema() {
  return new ParquetSchema({
    permit_key: { type: "UTF8" },
    source_system: { type: "UTF8" },
    source_report_document_id: { type: "UTF8" },
    source_report_title: { type: "UTF8" },
    source_report_url: { type: "UTF8" },
    permit_number: { type: "UTF8", optional: true },
    permit_issue_date: { type: "UTF8" },
    record_status: { type: "UTF8" },
    record_type: { type: "UTF8" },
    city: { type: "UTF8" },
    is_roof_permit: { type: "BOOLEAN" },
  });
}

/**
 * Normalize a Parquet row to the combined public contract.
 *
 * @param {Record<string, unknown>} row Existing City public row.
 * @returns {CombinedPermitRow} Validated City row.
 */
function readCityRow(row) {
  if (Object.keys(row).join("|") !== PUBLIC_PERMIT_FIELDS.join("|")) {
    throw new Error("Existing City permit Parquet schema drifted");
  }
  if (row.source_system !== CITY_SOURCE_SYSTEM || row.city !== "Rock Island") {
    throw new Error("Existing City permit row has an unexpected source");
  }
  if (typeof row.is_roof_permit !== "boolean") {
    throw new Error("Existing City permit has invalid roof classification");
  }
  return {
    permit_key: requiredString(row.permit_key, "permit_key"),
    source_system: CITY_SOURCE_SYSTEM,
    source_report_document_id: requiredString(
      row.source_report_document_id,
      "source_report_document_id",
    ),
    source_report_title: requiredString(
      row.source_report_title,
      "source_report_title",
    ),
    source_report_url: requiredString(
      row.source_report_url,
      "source_report_url",
    ),
    permit_number: requiredString(row.permit_number, "permit_number"),
    permit_issue_date: requiredString(
      row.permit_issue_date,
      "permit_issue_date",
    ),
    record_status: requiredString(row.record_status, "record_status"),
    record_type: requiredString(row.record_type, "record_type"),
    city: "Rock Island",
    is_roof_permit: row.is_roof_permit,
  };
}

/**
 * Read every existing City public row.
 *
 * @param {string} parquetPath Existing City-only public Parquet.
 * @returns {Promise<CombinedPermitRow[]>} Validated rows.
 */
async function readCityRows(parquetPath) {
  const reader = await ParquetReader.openFile(parquetPath);
  /** @type {CombinedPermitRow[]} */
  const rows = [];
  try {
    const cursor = reader.getCursor();
    for (
      let row = await cursor.next();
      row !== null;
      row = await cursor.next()
    ) {
      if (!isJsonObject(row))
        throw new Error("Existing City Parquet row is invalid");
      rows.push(readCityRow(row));
    }
  } finally {
    await reader.close();
  }
  return rows;
}

/**
 * Read and map the private Moline JSONL.
 *
 * @param {string} inputPath Private normalized package.
 * @returns {Promise<CombinedPermitRow[]>} Public-safe rows.
 */
async function readMolineRows(inputPath) {
  const text = await readFile(inputPath, "utf8");
  return text
    .trim()
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map((line) => {
      const record = JSON.parse(line);
      if (!isJsonObject(record))
        throw new Error("Moline JSONL row is not an object");
      return toMolineCombinedPermitRow(record);
    });
}

/**
 * Validate counts, provenance, dates, uniqueness, and privacy.
 *
 * @param {CombinedPermitRow[]} rows Combined rows.
 * @param {JsonObject} molineManifest Private source manifest.
 * @returns {{sources: Record<string,{rows:number,reports:number,earliest:string,latest:string}>,privacyFindings:number}} Validation summary.
 */
function validateRows(rows, molineManifest) {
  const keys = new Set();
  const sourceStats = new Map();
  let privacyFindings = 0;
  const privacyFindingCounts = new Map();
  for (const row of rows) {
    if (Object.keys(row).join("|") !== PUBLIC_PERMIT_FIELDS.join("|")) {
      throw new Error(`Combined public schema drift for ${row.permit_key}`);
    }
    if (keys.has(row.permit_key)) {
      throw new Error(`Duplicate combined permit key ${row.permit_key}`);
    }
    keys.add(row.permit_key);
    const rowFindings = scanPublicPermitRow(row);
    privacyFindings += rowFindings.length;
    for (const finding of rowFindings) {
      const category = finding.split(":")[0] ?? finding;
      privacyFindingCounts.set(
        category,
        (privacyFindingCounts.get(category) ?? 0) + 1,
      );
    }
    const stats = sourceStats.get(row.source_system) ?? {
      rows: 0,
      reportIds: new Set(),
      earliest: "9999-99-99",
      latest: "0000-00-00",
    };
    stats.rows += 1;
    stats.reportIds.add(row.source_report_document_id);
    if (row.permit_issue_date < stats.earliest)
      stats.earliest = row.permit_issue_date;
    if (row.permit_issue_date > stats.latest)
      stats.latest = row.permit_issue_date;
    sourceStats.set(row.source_system, stats);
  }
  const city = sourceStats.get(CITY_SOURCE_SYSTEM);
  const moline = sourceStats.get(MOLINE_SOURCE_SYSTEM);
  const diagnostics = {
    totalRows: rows.length,
    uniqueKeys: keys.size,
    cityRows: city?.rows,
    cityReports: city?.reportIds.size,
    cityEarliest: city?.earliest,
    cityLatest: city?.latest,
    molineRows: moline?.rows,
    molineReports: moline?.reportIds.size,
    molineEarliest: moline?.earliest,
    molineLatest: moline?.latest,
    manifestRows: molineManifest.uniqueLoaderKeyCount,
    manifestReports: molineManifest.selectedReportCount,
    blockedReports: Array.isArray(molineManifest.blockedReports)
      ? molineManifest.blockedReports.length
      : null,
    privacyFindings,
    privacyFindingCounts: Object.fromEntries(privacyFindingCounts),
  };
  if (
    rows.length !== EXPECTED_TOTAL_ROWS ||
    keys.size !== EXPECTED_TOTAL_ROWS ||
    city?.rows !== EXPECTED_CITY_ROWS ||
    city?.reportIds.size !== EXPECTED_CITY_REPORTS ||
    city?.earliest !== "2017-01-03" ||
    city?.latest !== "2026-04-30" ||
    moline?.rows !== EXPECTED_MOLINE_ROWS ||
    moline?.reportIds.size !== EXPECTED_MOLINE_REPORTS ||
    moline?.earliest !== "2017-01-03" ||
    moline?.latest !== "2026-06-30" ||
    molineManifest.uniqueLoaderKeyCount !== EXPECTED_MOLINE_ROWS ||
    molineManifest.selectedReportCount !== EXPECTED_MOLINE_REPORTS ||
    !Array.isArray(molineManifest.blockedReports) ||
    molineManifest.blockedReports.length !== 61 ||
    privacyFindings !== 0
  ) {
    throw new Error(
      `Combined permit count, provenance, coverage, or privacy gate failed: ${JSON.stringify(
        diagnostics,
      )}`,
    );
  }
  return {
    sources: {
      [CITY_SOURCE_SYSTEM]: {
        rows: city.rows,
        reports: city.reportIds.size,
        earliest: city.earliest,
        latest: city.latest,
      },
      [MOLINE_SOURCE_SYSTEM]: {
        rows: moline.rows,
        reports: moline.reportIds.size,
        earliest: moline.earliest,
        latest: moline.latest,
      },
    },
    privacyFindings,
  };
}

/**
 * Write the deterministic Parquet and companion artifacts.
 *
 * @param {CombinedPermitRow[]} rows Sorted rows.
 * @param {ReturnType<typeof validateRows>} validation Validation summary.
 * @param {string} outputRoot Output directory.
 * @returns {Promise<Record<string,string>>} Artifact paths.
 */
async function writeArtifacts(rows, validation, outputRoot) {
  await mkdir(outputRoot, { recursive: true });
  const paths = {
    parquet: join(outputRoot, "permit-query.parquet"),
    schema: join(outputRoot, "schema.json"),
    coverage: join(outputRoot, "coverage.json"),
    privacyScan: join(outputRoot, "privacy-scan.json"),
    manifest: join(outputRoot, "manifest.json"),
  };
  const writer = await ParquetWriter.openFile(
    buildCombinedPermitParquetSchema(),
    paths.parquet,
  );
  try {
    for (const row of rows) {
      const output = Object.fromEntries(
        Object.entries(row).filter(([, value]) => value !== null),
      );
      await writer.appendRow(output);
    }
  } finally {
    await writer.close();
  }
  const schema = {
    datasetId: DATASET_ID,
    datasetVersion: DATASET_VERSION,
    schemaVersion: SCHEMA_VERSION,
    additionalProperties: false,
    fields: PUBLIC_PERMIT_FIELDS.map((name) => ({
      name,
      nullable: name === "permit_number",
    })),
    excludedFieldClasses: [
      "addresses, geolocation, parcels, and PIN values",
      "descriptions, contractors, applicants, owners, people, and contacts",
      "valuations, fees, raw payloads, private links, and private hashes",
    ],
  };
  const coverage = {
    datasetId: DATASET_ID,
    datasetVersion: DATASET_VERSION,
    jurisdiction: "Rock Island County supported municipal permit sources",
    publishedCount: EXPECTED_TOTAL_ROWS,
    sourceBreakdown: [
      {
        jurisdiction: "City of Rock Island",
        sourceSystem: CITY_SOURCE_SYSTEM,
        publishedCount: EXPECTED_CITY_ROWS,
        reportCount: EXPECTED_CITY_REPORTS,
        issueDateRange: { earliest: "2017-01-03", latest: "2026-04-30" },
        limitation:
          "Official monthly issued-permit reports currently published by the city; not complete permit lifecycle history.",
      },
      {
        jurisdiction: "Moline",
        sourceSystem: MOLINE_SOURCE_SYSTEM,
        publishedCount: EXPECTED_MOLINE_ROWS,
        reportCount: EXPECTED_MOLINE_REPORTS,
        issueDateRange: { earliest: "2017-01-03", latest: "2026-06-30" },
        limitation:
          "Supported official report layouts only; 61 ambiguous, compacted, contradictory, or conflicting reports are excluded.",
      },
    ],
    relatedCoverageSnapshot: {
      appraisalProperties: 65_806,
      supportedAppraisalAddresses: 65_800,
      nullAppraisalAddresses: 6,
      officialClassLabels: 65_653,
      nonUnknownUsage: 63_123,
      geometryComponents: 66_516,
      geometryRings: 66_560,
      privateCorporateRegistrations: 1_981_254,
      publicCorporateScope: 11_741,
    },
    propertyLinksPublished: 0,
  };
  const privacyScan = {
    datasetId: DATASET_ID,
    rowCount: rows.length,
    scannedFieldCount: PUBLIC_PERMIT_FIELDS.length,
    findings: validation.privacyFindings,
    paymentCardScanExcludedFields: [...OFFICIAL_IDENTIFIER_FIELDS],
    paymentCardScanExclusionReason:
      "Official permit/report identifiers can coincidentally satisfy Luhn; email, phone, and SSN scans still cover every string field.",
    passed: true,
  };
  await writeFile(paths.schema, stableJson(schema), { mode: 0o600 });
  await writeFile(paths.coverage, stableJson(coverage), { mode: 0o600 });
  await writeFile(paths.privacyScan, stableJson(privacyScan), { mode: 0o600 });
  const artifacts = [];
  for (const artifactPath of [
    paths.parquet,
    paths.schema,
    paths.coverage,
    paths.privacyScan,
  ]) {
    const body = await readFile(artifactPath);
    artifacts.push({
      fileName: basename(artifactPath),
      sizeBytes: body.byteLength,
      sha256: createHash("sha256").update(body).digest("hex"),
      cid: await ipfsHash.of(body),
    });
  }
  const manifest = {
    datasetId: DATASET_ID,
    datasetVersion: DATASET_VERSION,
    schemaVersion: SCHEMA_VERSION,
    rowCount: rows.length,
    uniquePermitKeys: rows.length,
    sources: validation.sources,
    excludedMolineReportCount: 61,
    propertyLinksPublished: 0,
    artifacts,
  };
  await writeFile(paths.manifest, stableJson(manifest), { mode: 0o600 });
  return paths;
}

/**
 * Require two complete artifact sets to be byte-identical.
 *
 * @param {Record<string,string>} first First artifact paths.
 * @param {Record<string,string>} second Second artifact paths.
 * @returns {Promise<void>}
 */
async function assertDeterministic(first, second) {
  for (const name of Object.keys(first)) {
    const left = await readFile(first[name]);
    const right = await readFile(second[name]);
    if (!left.equals(right)) throw new Error(`Determinism failed for ${name}`);
  }
}

/**
 * Parse CLI options.
 *
 * @param {readonly string[]} argv Arguments after script path.
 * @returns {{cityParquet:string,molinePrivate:string,molineManifest:string,out:string}}
 */
function parseCli(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      "city-parquet": { type: "string" },
      "moline-private": { type: "string" },
      "moline-manifest": { type: "string" },
      out: { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });
  return {
    cityParquet: requiredString(values["city-parquet"], "--city-parquet"),
    molinePrivate: requiredString(values["moline-private"], "--moline-private"),
    molineManifest: requiredString(
      values["moline-manifest"],
      "--moline-manifest",
    ),
    out: requiredString(values.out, "--out"),
  };
}

async function main() {
  const options = parseCli(process.argv.slice(2));
  const cityRows = await readCityRows(options.cityParquet);
  const molineRows = await readMolineRows(options.molinePrivate);
  const rows = [...cityRows, ...molineRows].sort((left, right) =>
    left.permit_key < right.permit_key
      ? -1
      : left.permit_key > right.permit_key
        ? 1
        : 0,
  );
  const parsedManifest = JSON.parse(
    await readFile(options.molineManifest, "utf8"),
  );
  if (!isJsonObject(parsedManifest)) throw new Error("Invalid Moline manifest");
  const validation = validateRows(rows, parsedManifest);
  const first = await writeArtifacts(rows, validation, options.out);
  const rerunRoot = `${options.out}.deterministic-rerun`;
  await rm(rerunRoot, { recursive: true, force: true });
  const second = await writeArtifacts(rows, validation, rerunRoot);
  await assertDeterministic(first, second);
  await rm(rerunRoot, { recursive: true, force: true });
  const parquet = await readFile(first.parquet);
  console.log(
    JSON.stringify({
      event: "rock_island_combined_permit_publication_built",
      rowCount: rows.length,
      sources: validation.sources,
      privacyFindings: validation.privacyFindings,
      byteIdenticalRerun: true,
      parquetSha256: createHash("sha256").update(parquet).digest("hex"),
      parquetCid: await ipfsHash.of(parquet),
      outputRoot: options.out,
    }),
  );
}

const invokedPath =
  process.argv[1] === undefined ? null : resolve(process.argv[1]);
if (invokedPath !== null && fileURLToPath(import.meta.url) === invokedPath) {
  main().catch((caught) => {
    console.error(
      JSON.stringify({
        event: "rock_island_combined_permit_publication_failed",
        error: caught instanceof Error ? caught.message : String(caught),
      }),
    );
    process.exit(1);
  });
}
