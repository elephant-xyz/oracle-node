#!/usr/bin/env node

/**
 * Build the standalone, public-safe City of Rock Island permit query dataset
 * from the reconciled private normalized JSONL load package.
 *
 * The public schema is intentionally closed. No address, parcel, property,
 * description, contractor, person, valuation, raw payload, or derived hash is
 * copied into the Parquet. The script writes deterministic artifacts twice and
 * requires byte-identical output before declaring the publication ready.
 */

import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { createRequire } from "node:module";

import { ParquetReader, ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";

const require = createRequire(import.meta.url);
const ipfsHash = require("ipfs-only-hash");

const EXPECTED_ROW_COUNT = 24_786;
const EXPECTED_REPORT_COUNT = 112;
const EXPECTED_EARLIEST_DATE = "2017-01-03";
const EXPECTED_LATEST_DATE = "2026-04-30";
const DATASET_ID = "city-of-rock-island-issued-permit-query";
const DATASET_VERSION = "2026-08-14";
const SCHEMA_VERSION = "1.0.0";
const REPORT_SOURCE_SYSTEM = "rock_island_city_official_monthly_permit_reports";

const PRIVATE_INPUT_KEYS = Object.freeze([
  "city",
  "contractor_business_names",
  "is_roof_permit",
  "parcel_identifier",
  "permit_issue_date",
  "permit_number",
  "project_description",
  "raw",
  "record_status",
  "record_type",
  "source_system",
  "source_url",
  "work_location",
]);

export const PUBLIC_PERMIT_FIELDS = Object.freeze([
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

const APPROVED_RECORD_TYPES = new Set([
  "Addition",
  "Building (Commercial)",
  "Building (Residential)",
  "Concrete Flatwork",
  "Demolition",
  "Electrical",
  "Excavation",
  "Mechanical",
  "Plumbing",
  "ReinspFee",
  "Sign",
  "Utility Turn On",
  "bnewconres",
  "bremodel",
  "demo",
  "excavation",
  "flatwork",
  "grading1",
  "grading2",
  "grading3",
  "heating",
  "plumbing",
  "roof",
  "sign",
  "utility",
]);

const FORBIDDEN_KEY_PATTERN =
  /(address|parcel|pin|description|contractor|applicant|owner|person|phone|email|contact|valuation|value|property|raw|payload|hash|latitude|longitude)/i;
const EMAIL_PATTERN = /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i;
const PHONE_PATTERN =
  /(?:\+?1[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]\d{3}[\s.-]\d{4}/;
const SSN_PATTERN = /\b\d{3}-\d{2}-\d{4}\b/;

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * @typedef {object} PublicPermitRow
 * @property {string} permit_key Stable key made only from public fields.
 * @property {string} source_system Official report source system.
 * @property {string} source_report_document_id Official report document id.
 * @property {string} source_report_title Official report title.
 * @property {string} source_report_url Official report URL.
 * @property {string} permit_number Public permit number.
 * @property {string} permit_issue_date Public issue date.
 * @property {string} record_status Public status.
 * @property {string} record_type Public permit type.
 * @property {string} city Jurisdiction name.
 * @property {boolean} is_roof_permit Roof classification.
 */

/**
 * @typedef {object} PublicationPaths
 * @property {string} root Output directory.
 * @property {string} parquet Parquet artifact.
 * @property {string} schema Public schema JSON.
 * @property {string} coverage Honest coverage JSON.
 * @property {string} privacyScan Privacy scan JSON.
 * @property {string} manifest Publication manifest JSON.
 */

/**
 * @typedef {object} ArtifactDigest
 * @property {string} fileName Artifact file name.
 * @property {number} sizeBytes Artifact size.
 * @property {string} sha256 SHA-256 digest.
 * @property {string} cid Locally computed IPFS CID.
 */

/**
 * Return true only for a non-array JSON object.
 *
 * @param {unknown} value Candidate value.
 * @returns {value is JsonObject} Whether value is a JSON object.
 */
function isJsonObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Read a required non-empty string.
 *
 * @param {unknown} value Candidate value.
 * @param {string} field Field label for failures.
 * @returns {string} Trimmed string.
 */
function requiredString(value, field) {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`Required public field ${field} is missing`);
  }
  return value.trim();
}

/**
 * Compare two strings by Unicode code unit for locale-independent ordering.
 *
 * @param {string} left Left value.
 * @param {string} right Right value.
 * @returns {number} Sort order.
 */
function compareText(left, right) {
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

/**
 * Render deterministic pretty JSON with one trailing newline.
 *
 * @param {unknown} value JSON-compatible value.
 * @returns {string} Stable JSON text.
 */
export function stableJson(value) {
  return `${JSON.stringify(value, null, 2)}\n`;
}

/**
 * Validate the exact top-level private load-package schema. Any new field
 * requires explicit review before it can enter this publication pipeline.
 *
 * @param {JsonObject} record Private normalized permit.
 * @returns {void}
 */
export function assertPrivateInputSchema(record) {
  const actual = Object.keys(record).sort(compareText);
  const expected = [...PRIVATE_INPUT_KEYS].sort(compareText);
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(
      `Private permit schema drift: expected ${expected.join(",")} but received ${actual.join(",")}`,
    );
  }
}

/**
 * Map one private load record into the closed public schema.
 *
 * @param {JsonObject} record Private normalized permit.
 * @returns {PublicPermitRow} Public-safe row.
 */
export function toPublicPermitRow(record) {
  assertPrivateInputSchema(record);
  const sourceSystem = requiredString(record.source_system, "source_system");
  const permitNumber = requiredString(record.permit_number, "permit_number");
  const issueDate = requiredString(
    record.permit_issue_date,
    "permit_issue_date",
  );
  const status = requiredString(record.record_status, "record_status");
  const type = requiredString(record.record_type, "record_type");
  const city = requiredString(record.city, "city");
  const sourceUrl = requiredString(record.source_url, "source_url");
  if (!isJsonObject(record.raw)) {
    throw new Error(`Permit ${permitNumber} has no reviewed report provenance`);
  }
  const documentId = requiredString(
    record.raw.source_document_id,
    "raw.source_document_id",
  );
  const reportTitle = requiredString(
    record.raw.source_report_title,
    "raw.source_report_title",
  );
  if (sourceSystem !== REPORT_SOURCE_SYSTEM) {
    throw new Error(`Unapproved source system: ${sourceSystem}`);
  }
  if (status !== "Issued") {
    throw new Error(`Unapproved public status: ${status}`);
  }
  if (city !== "Rock Island") {
    throw new Error(`Unapproved city: ${city}`);
  }
  if (!APPROVED_RECORD_TYPES.has(type)) {
    throw new Error(`Unreviewed permit type: ${type}`);
  }
  if (typeof record.is_roof_permit !== "boolean") {
    throw new Error(`Permit ${permitNumber} has invalid roof classification`);
  }
  if (
    !/^https:\/\/www\.rigov\.org\/DocumentCenter\/View\/\d+(?:\/[^?#]+)?$/.test(
      sourceUrl,
    )
  ) {
    throw new Error(`Permit ${permitNumber} has an unapproved report URL`);
  }
  return {
    permit_key: `${sourceSystem}:${permitNumber}`,
    source_system: sourceSystem,
    source_report_document_id: documentId,
    source_report_title: reportTitle,
    source_report_url: sourceUrl,
    permit_number: permitNumber,
    permit_issue_date: issueDate,
    record_status: status,
    record_type: type,
    city,
    is_roof_permit: record.is_roof_permit,
  };
}

/**
 * Detect common person/contact/payment values in the closed output.
 *
 * @param {PublicPermitRow} row Public row.
 * @returns {string[]} Finding labels.
 */
export function scanPublicPermitRow(row) {
  const findings = [];
  for (const key of Object.keys(row)) {
    if (FORBIDDEN_KEY_PATTERN.test(key)) findings.push(`forbidden_key:${key}`);
  }
  for (const [key, value] of Object.entries(row)) {
    if (typeof value !== "string") continue;
    if (EMAIL_PATTERN.test(value)) findings.push(`email_value:${key}`);
    if (PHONE_PATTERN.test(value)) findings.push(`phone_value:${key}`);
    if (SSN_PATTERN.test(value)) findings.push(`ssn_value:${key}`);
    const digits = value.replace(/\D/g, "");
    if (digits.length >= 13 && digits.length <= 19 && passesLuhn(digits)) {
      findings.push(`payment_card_like_value:${key}`);
    }
  }
  return findings;
}

/**
 * Luhn check for payment-card-like digit sequences.
 *
 * @param {string} digits Digit-only candidate.
 * @returns {boolean} Whether the sequence passes Luhn.
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
 * Build the exact public Parquet schema.
 *
 * @returns {ParquetSchema} Closed scalar schema.
 */
export function buildSafePermitParquetSchema() {
  return new ParquetSchema({
    permit_key: { type: "UTF8" },
    source_system: { type: "UTF8" },
    source_report_document_id: { type: "UTF8" },
    source_report_title: { type: "UTF8" },
    source_report_url: { type: "UTF8" },
    permit_number: { type: "UTF8" },
    permit_issue_date: { type: "UTF8" },
    record_status: { type: "UTF8" },
    record_type: { type: "UTF8" },
    city: { type: "UTF8" },
    is_roof_permit: { type: "BOOLEAN" },
  });
}

/**
 * Read and map the private JSONL package.
 *
 * @param {string} inputPath Private JSONL path.
 * @returns {Promise<PublicPermitRow[]>} Sorted public rows.
 */
async function readPublicRows(inputPath) {
  const rows = [];
  const input = createInterface({
    input: createReadStream(inputPath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });
  for await (const line of input) {
    if (line.trim().length === 0) continue;
    const parsed = JSON.parse(line);
    if (!isJsonObject(parsed))
      throw new Error("Private JSONL row is not an object");
    rows.push(toPublicPermitRow(parsed));
  }
  return rows.sort((left, right) =>
    compareText(left.permit_key, right.permit_key),
  );
}

/**
 * Validate public rows and provenance against the harvested report inventory.
 *
 * @param {PublicPermitRow[]} rows Public rows.
 * @param {string} provenancePath Private provenance inventory path.
 * @returns {Promise<{earliestIssueDate:string,latestIssueDate:string,reportCount:number,privacyFindingCount:number}>} Validation summary.
 */
async function validateRows(rows, provenancePath) {
  const keys = new Set();
  const permitNumbers = new Set();
  const reportIds = new Set();
  let earliest = "9999-99-99";
  let latest = "0000-00-00";
  let privacyFindingCount = 0;
  for (const row of rows) {
    if (Object.keys(row).join("|") !== PUBLIC_PERMIT_FIELDS.join("|")) {
      throw new Error(`Public output schema drift for ${row.permit_key}`);
    }
    if (keys.has(row.permit_key))
      throw new Error(`Duplicate permit key ${row.permit_key}`);
    if (permitNumbers.has(row.permit_number)) {
      throw new Error(`Duplicate permit number ${row.permit_number}`);
    }
    keys.add(row.permit_key);
    permitNumbers.add(row.permit_number);
    reportIds.add(row.source_report_document_id);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(row.permit_issue_date)) {
      throw new Error(`Invalid issue date ${row.permit_issue_date}`);
    }
    if (row.permit_issue_date < earliest) earliest = row.permit_issue_date;
    if (row.permit_issue_date > latest) latest = row.permit_issue_date;
    privacyFindingCount += scanPublicPermitRow(row).length;
  }
  const provenance = JSON.parse(await readFile(provenancePath, "utf8"));
  if (!isJsonObject(provenance) || !Array.isArray(provenance.reports)) {
    throw new Error("Invalid report provenance inventory");
  }
  const expectedReportIds = new Set(
    provenance.reports.map((report) => {
      if (!isJsonObject(report))
        throw new Error("Invalid report provenance row");
      return requiredString(report.documentId, "provenance.documentId");
    }),
  );
  const missingReports = [...expectedReportIds].filter(
    (id) => !reportIds.has(id),
  );
  const unexpectedReports = [...reportIds].filter(
    (id) => !expectedReportIds.has(id),
  );
  if (rows.length !== EXPECTED_ROW_COUNT) {
    throw new Error(
      `Expected ${EXPECTED_ROW_COUNT} rows, received ${rows.length}`,
    );
  }
  if (
    keys.size !== EXPECTED_ROW_COUNT ||
    permitNumbers.size !== EXPECTED_ROW_COUNT
  ) {
    throw new Error("Permit key/number uniqueness reconciliation failed");
  }
  if (
    reportIds.size !== EXPECTED_REPORT_COUNT ||
    expectedReportIds.size !== EXPECTED_REPORT_COUNT ||
    missingReports.length > 0 ||
    unexpectedReports.length > 0
  ) {
    throw new Error(
      `Report provenance mismatch: public=${reportIds.size} expected=${expectedReportIds.size} missing=${missingReports.length} unexpected=${unexpectedReports.length}`,
    );
  }
  if (earliest !== EXPECTED_EARLIEST_DATE || latest !== EXPECTED_LATEST_DATE) {
    throw new Error(
      `Unexpected issue-date range ${earliest} through ${latest}`,
    );
  }
  if (privacyFindingCount !== 0) {
    throw new Error(
      `Semantic privacy scan found ${privacyFindingCount} findings`,
    );
  }
  return {
    earliestIssueDate: earliest,
    latestIssueDate: latest,
    reportCount: reportIds.size,
    privacyFindingCount,
  };
}

/**
 * Resolve all output paths.
 *
 * @param {string} root Output root.
 * @returns {PublicationPaths} Artifact paths.
 */
function publicationPaths(root) {
  return {
    root,
    parquet: path.join(root, "permit-query.parquet"),
    schema: path.join(root, "schema.json"),
    coverage: path.join(root, "coverage.json"),
    privacyScan: path.join(root, "privacy-scan.json"),
    manifest: path.join(root, "manifest.json"),
  };
}

/**
 * Write rows to Parquet.
 *
 * @param {string} parquetPath Destination path.
 * @param {PublicPermitRow[]} rows Sorted public rows.
 * @returns {Promise<void>}
 */
async function writeParquet(parquetPath, rows) {
  const writer = await ParquetWriter.openFile(
    buildSafePermitParquetSchema(),
    parquetPath,
  );
  try {
    for (const row of rows) await writer.appendRow(row);
  } finally {
    await writer.close();
  }
}

/**
 * Compute a SHA-256 and UnixFS CID for one artifact.
 *
 * @param {string} artifactPath Artifact path.
 * @returns {Promise<ArtifactDigest>} Digest metadata.
 */
async function digestArtifact(artifactPath) {
  const body = await readFile(artifactPath);
  return {
    fileName: path.basename(artifactPath),
    sizeBytes: body.byteLength,
    sha256: createHash("sha256").update(body).digest("hex"),
    cid: await ipfsHash.of(body),
  };
}

/**
 * Build one complete deterministic artifact set.
 *
 * @param {PublicPermitRow[]} rows Sorted public rows.
 * @param {{earliestIssueDate:string,latestIssueDate:string,reportCount:number,privacyFindingCount:number}} validation Validation summary.
 * @param {string} root Output root.
 * @returns {Promise<PublicationPaths>} Written paths.
 */
async function writeArtifactSet(rows, validation, root) {
  const paths = publicationPaths(root);
  await mkdir(root, { recursive: true });
  await writeParquet(paths.parquet, rows);
  const schema = {
    datasetId: DATASET_ID,
    datasetVersion: DATASET_VERSION,
    schemaVersion: SCHEMA_VERSION,
    additionalProperties: false,
    fields: [
      { name: "permit_key", type: "string", nullable: false },
      { name: "source_system", type: "string", nullable: false },
      { name: "source_report_document_id", type: "string", nullable: false },
      { name: "source_report_title", type: "string", nullable: false },
      { name: "source_report_url", type: "string", nullable: false },
      { name: "permit_number", type: "string", nullable: false },
      {
        name: "permit_issue_date",
        type: "date",
        format: "YYYY-MM-DD",
        nullable: false,
      },
      { name: "record_status", type: "string", nullable: false },
      { name: "record_type", type: "string", nullable: false },
      { name: "city", type: "string", nullable: false },
      { name: "is_roof_permit", type: "boolean", nullable: false },
    ],
    excludedFieldClasses: [
      "addresses and geolocation",
      "parcel and PIN values",
      "descriptions and free text",
      "contractors, applicants, owners, people, and contacts",
      "phone numbers and email addresses",
      "valuations and fees",
      "property identifiers and links",
      "raw PDF text and source payloads",
      "hashes derived from excluded values",
    ],
  };
  const coverage = {
    datasetId: DATASET_ID,
    datasetVersion: DATASET_VERSION,
    jurisdiction: "City of Rock Island, Illinois",
    publishedCount: EXPECTED_ROW_COUNT,
    expectedCount: EXPECTED_ROW_COUNT,
    expectedCountScope:
      "Only the 112 City of Rock Island monthly issued-permit reports currently published by the city.",
    reportCount: EXPECTED_REPORT_COUNT,
    issueDateRange: {
      earliest: EXPECTED_EARLIEST_DATE,
      latest: EXPECTED_LATEST_DATE,
    },
    coverageStatus: "complete_for_currently_published_monthly_report_surface",
    limitations: [
      "This is not all City of Rock Island permit lifecycle or history.",
      "The source reports contain issued permits, not complete applications, inspections, revisions, closures, or other lifecycle events.",
      "This is not countywide coverage and excludes Moline, East Moline, Carbon Cliff, and other jurisdictions.",
      "No address, parcel, property, contractor, person, valuation, or free-text fields are published.",
    ],
    relatedCoverageSnapshot: {
      rockIslandAppraisalParcels: 65_806,
      rockIslandCorporateRegistrations: 11_741,
      rockIslandBbbProfiles: 0,
      note: "Reference counts only; existing property and corporate publications were not modified.",
    },
  };
  const privacyScan = {
    datasetId: DATASET_ID,
    rowCount: rows.length,
    scannedFieldCount: PUBLIC_PERMIT_FIELDS.length,
    keyFindings: 0,
    valueFindings: validation.privacyFindingCount,
    semanticChecks: [
      "closed public key allowlist",
      "fixed source system, city, status, and reviewed permit-type vocabulary",
      "email pattern",
      "phone pattern",
      "SSN pattern",
      "Luhn-valid 13-to-19-digit payment-card-like values",
    ],
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
    artifacts.push(await digestArtifact(artifactPath));
  }
  const manifest = {
    datasetId: DATASET_ID,
    datasetVersion: DATASET_VERSION,
    schemaVersion: SCHEMA_VERSION,
    rowCount: rows.length,
    uniquePermitKeys: rows.length,
    uniquePermitNumbers: rows.length,
    issueDateRange: {
      earliest: validation.earliestIssueDate,
      latest: validation.latestIssueDate,
    },
    reportProvenanceCount: validation.reportCount,
    standalone: true,
    propertyLinksPublished: 0,
    artifacts,
  };
  await writeFile(paths.manifest, stableJson(manifest), { mode: 0o600 });
  return paths;
}

/**
 * Read a Parquet and verify the exact physical schema and row semantics.
 *
 * @param {string} parquetPath Parquet path.
 * @returns {Promise<{rowCount:number,uniquePermitKeys:number,uniquePermitNumbers:number}>} Inspection summary.
 */
async function inspectParquet(parquetPath) {
  const reader = await ParquetReader.openFile(parquetPath);
  const actualFields = Object.keys(reader.schema.fields);
  if (actualFields.join("|") !== PUBLIC_PERMIT_FIELDS.join("|")) {
    await reader.close();
    throw new Error(`Parquet schema drift: ${actualFields.join(",")}`);
  }
  const keys = new Set();
  const permitNumbers = new Set();
  let rowCount = 0;
  try {
    const cursor = reader.getCursor();
    let row = await cursor.next();
    while (row !== null) {
      rowCount += 1;
      const permitKey = requiredString(row.permit_key, "parquet.permit_key");
      const permitNumber = requiredString(
        row.permit_number,
        "parquet.permit_number",
      );
      keys.add(permitKey);
      permitNumbers.add(permitNumber);
      row = await cursor.next();
    }
  } finally {
    await reader.close();
  }
  if (
    rowCount !== EXPECTED_ROW_COUNT ||
    keys.size !== EXPECTED_ROW_COUNT ||
    permitNumbers.size !== EXPECTED_ROW_COUNT
  ) {
    throw new Error(
      `Parquet reconciliation failed: rows=${rowCount} keys=${keys.size} permitNumbers=${permitNumbers.size}`,
    );
  }
  return {
    rowCount,
    uniquePermitKeys: keys.size,
    uniquePermitNumbers: permitNumbers.size,
  };
}

/**
 * Require two artifact sets to be byte-identical.
 *
 * @param {PublicationPaths} first First artifact set.
 * @param {PublicationPaths} second Second artifact set.
 * @returns {Promise<void>}
 */
async function assertByteIdentical(first, second) {
  for (const key of [
    "parquet",
    "schema",
    "coverage",
    "privacyScan",
    "manifest",
  ]) {
    const firstBody = await readFile(first[key]);
    const secondBody = await readFile(second[key]);
    if (!firstBody.equals(secondBody)) {
      throw new Error(`Deterministic rerun mismatch for ${key}`);
    }
  }
}

/**
 * Parse CLI options.
 *
 * @param {readonly string[]} argv Arguments after the script path.
 * @returns {{input:string,provenance:string,out:string}} Parsed options.
 */
function parseCli(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      input: { type: "string" },
      provenance: { type: "string" },
      out: { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });
  return {
    input:
      values.input ??
      "downloads/rock-island/permit-harvest/city-rock-island-2026-08-14/db-load-package/rock-island-permits.load.private.jsonl",
    provenance:
      values.provenance ??
      "downloads/rock-island/permit-harvest/city-rock-island-2026-08-14/full-source-provenance.json",
    out:
      values.out ??
      "downloads/rock-island/permit-harvest/city-rock-island-2026-08-14/public-permit-query/v1",
  };
}

/**
 * Run the deterministic export and validation gate.
 *
 * @returns {Promise<void>}
 */
async function main() {
  const options = parseCli(process.argv.slice(2));
  const rows = await readPublicRows(options.input);
  const validation = await validateRows(rows, options.provenance);
  const first = await writeArtifactSet(rows, validation, options.out);
  const rerunRoot = `${options.out}.deterministic-rerun`;
  await rm(rerunRoot, { recursive: true, force: true });
  const second = await writeArtifactSet(rows, validation, rerunRoot);
  await assertByteIdentical(first, second);
  const parquet = await inspectParquet(first.parquet);
  const manifestDigest = await digestArtifact(first.manifest);
  await rm(rerunRoot, { recursive: true, force: true });
  console.log(
    JSON.stringify({
      event: "rock_island_safe_permit_publication_built",
      out: options.out,
      ...parquet,
      reportCount: validation.reportCount,
      issueDateRange: {
        earliest: validation.earliestIssueDate,
        latest: validation.latestIssueDate,
      },
      privacyFindings: validation.privacyFindingCount,
      byteIdenticalRerun: true,
      manifestCid: manifestDigest.cid,
      manifestSha256: manifestDigest.sha256,
    }),
  );
}

const invokedPath =
  process.argv[1] === undefined ? null : path.resolve(process.argv[1]);
if (invokedPath !== null && fileURLToPath(import.meta.url) === invokedPath) {
  main().catch((caught) => {
    const message = caught instanceof Error ? caught.message : String(caught);
    console.error(
      JSON.stringify({
        event: "rock_island_safe_permit_publication_failed",
        error: message,
      }),
    );
    process.exit(1);
  });
}
