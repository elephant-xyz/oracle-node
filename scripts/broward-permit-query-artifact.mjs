// @ts-check

import { createHash } from "node:crypto";
import { chmod, mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

import { ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";

/**
 * @typedef {object} BrowardNormalizedPermit
 * @property {string} source_system - County-prefixed permit source system.
 * @property {string} source_url - Official detail URL.
 * @property {string} source_object_id - Official source object identifier.
 * @property {"master" | "permit"} source_record_kind - BCS master application or permit detail.
 * @property {string} record_key - Globally stable source identity.
 * @property {string} parcel_identifier - Exact 12-character BCPA parcel identifier.
 * @property {string} permit_number - Public permit/application number.
 * @property {string} record_status - Public source status.
 * @property {string} record_type - Public source permit/application type.
 * @property {string | null} permit_issue_date - ISO issue date.
 * @property {string | null} application_date - ISO application date.
 * @property {string | null} expiration_date - ISO expiration date.
 * @property {string | null} project_title - Public project title.
 * @property {string | null} project_description - Public project description.
 * @property {number | null} job_value - Public estimated job value.
 * @property {readonly { completed_date: string | null }[]} inspections - Public inspection completion evidence.
 */

/**
 * @typedef {object} DonphanPermitQueryRow
 * @property {string} property_improvement_id - Stable deterministic UUID for the permit/application row.
 * @property {string | null} property_id - Pilot property identity matched by exact BCPA folio.
 * @property {string | null} parcel_identifier - Exact Broward folio.
 * @property {string | null} permit_number - Official permit/application number.
 * @property {string | null} improvement_type - Source permit type.
 * @property {string | null} improvement_status - Source status.
 * @property {string | null} improvement_action - Whether the BCS row is a permit or master application.
 * @property {string | null} permit_issue_date - ISO permit issue date.
 * @property {string | null} application_received_date - ISO application date.
 * @property {string | null} final_inspection_date - Latest source-exposed completed inspection date.
 * @property {string | null} permit_close_date - Explicit close date when available.
 * @property {string | null} completion_date - Explicit completion date when available.
 * @property {string | null} expiration_date - ISO expiration date.
 * @property {string | null} opened_date - Explicit opened/application date.
 * @property {string | null} source_system - County-prefixed source system.
 * @property {string | null} county_name - Human-readable county name.
 * @property {string | null} project_description - Public project description.
 * @property {string | null} description - Public project title.
 * @property {number | null} estimated_job_value - Public estimated job value.
 * @property {number | null} fee - Public fee total when available.
 */

/**
 * Exact scalar column order exported by elephant-query-db's
 * `run-permit-table-export.ts` and consumed by Donphan's `permits` view.
 *
 * @type {readonly (keyof DonphanPermitQueryRow)[]}
 */
export const DONPHAN_PERMIT_QUERY_COLUMNS = Object.freeze([
  "property_improvement_id",
  "property_id",
  "parcel_identifier",
  "permit_number",
  "improvement_type",
  "improvement_status",
  "improvement_action",
  "permit_issue_date",
  "application_received_date",
  "final_inspection_date",
  "permit_close_date",
  "completion_date",
  "expiration_date",
  "opened_date",
  "source_system",
  "county_name",
  "project_description",
  "description",
  "estimated_job_value",
  "fee",
]);

/**
 * Produce a deterministic RFC-4122-shaped UUID from a permit source identity.
 *
 * Query-db normally allocates the `property_improvement_id` UUID in Postgres.
 * The local no-database pilot needs the same stable scalar field, so it derives
 * a version-5-shaped identifier from the complete source-system record key
 * without weakening or truncating source identity.
 *
 * @param {string} recordKey - Complete stable source record key.
 * @returns {string} Deterministic UUID string.
 */
export function deterministicPermitUuid(recordKey) {
  const bytes = createHash("sha256")
    .update(`elephant:broward:permit:${recordKey}`, "utf8")
    .digest()
    .subarray(0, 16);
  const versionByte = bytes[6];
  const variantByte = bytes[8];
  if (versionByte === undefined || variantByte === undefined) {
    throw new Error("SHA-256 digest did not contain 16 UUID bytes");
  }
  bytes[6] = (versionByte & 0x0f) | 0x50;
  bytes[8] = (variantByte & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20),
  ].join("-");
}

/**
 * Normalize one integrated BCS/POSSE output into Donphan's actual permit-table
 * shape. No dates or completion semantics are invented: the latest explicitly
 * completed inspection is retained as `final_inspection_date`, while close and
 * completion dates remain null because BCS does not expose them independently.
 *
 * @param {BrowardNormalizedPermit} record - Strict normalized source permit.
 * @returns {DonphanPermitQueryRow} Exact 20-column Donphan permit query row.
 */
export function mapBrowardPermitToDonphanRow(record) {
  const completedInspectionDates = record.inspections
    .map((inspection) => inspection.completed_date)
    .filter((value) => value !== null)
    .sort();
  const finalInspectionDate = completedInspectionDates.at(-1) ?? null;
  return {
    property_improvement_id: deterministicPermitUuid(record.record_key),
    property_id: `broward:${record.parcel_identifier}`,
    parcel_identifier: record.parcel_identifier,
    permit_number: record.permit_number,
    improvement_type: record.record_type,
    improvement_status: record.record_status,
    improvement_action:
      record.source_record_kind === "master"
        ? "master_application"
        : "permit_record",
    permit_issue_date: record.permit_issue_date,
    application_received_date: record.application_date,
    final_inspection_date: finalInspectionDate,
    permit_close_date: null,
    completion_date: null,
    expiration_date: record.expiration_date,
    opened_date: record.application_date,
    source_system: record.source_system,
    county_name: "Broward",
    project_description: record.project_description,
    description: record.project_title,
    estimated_job_value: record.job_value,
    fee: null,
  };
}

/**
 * Build Donphan's exact county permit-table Parquet schema.
 *
 * The primary key is required; all remaining scalar fields are nullable,
 * matching elephant-query-db's production permit export.
 *
 * @returns {ParquetSchema} Exact Donphan permit query schema.
 */
export function buildDonphanPermitParquetSchema() {
  return new ParquetSchema({
    property_improvement_id: { type: "UTF8" },
    property_id: { type: "UTF8", optional: true },
    parcel_identifier: { type: "UTF8", optional: true },
    permit_number: { type: "UTF8", optional: true },
    improvement_type: { type: "UTF8", optional: true },
    improvement_status: { type: "UTF8", optional: true },
    improvement_action: { type: "UTF8", optional: true },
    permit_issue_date: { type: "UTF8", optional: true },
    application_received_date: { type: "UTF8", optional: true },
    final_inspection_date: { type: "UTF8", optional: true },
    permit_close_date: { type: "UTF8", optional: true },
    completion_date: { type: "UTF8", optional: true },
    expiration_date: { type: "UTF8", optional: true },
    opened_date: { type: "UTF8", optional: true },
    source_system: { type: "UTF8", optional: true },
    county_name: { type: "UTF8", optional: true },
    project_description: { type: "UTF8", optional: true },
    description: { type: "UTF8", optional: true },
    estimated_job_value: { type: "DOUBLE", optional: true },
    fee: { type: "DOUBLE", optional: true },
  });
}

/**
 * Remove null optional values before handing a row to parquetjs.
 *
 * @param {DonphanPermitQueryRow} row - Complete nullable query row.
 * @returns {Record<string, string | number>} Parquet-compatible scalar record.
 */
function toParquetRecord(row) {
  /** @type {Record<string, string | number>} */
  const output = {};
  for (const [key, value] of Object.entries(row)) {
    if (value !== null) output[key] = value;
  }
  return output;
}

/**
 * Write a local private Donphan permit Parquet, including a valid zero-row
 * artifact when every attempted source explicitly returned no permits or was
 * unavailable. This function never contacts AWS, Postgres, IPFS, or a publisher.
 *
 * @param {string} parquetPath - Destination local Parquet path.
 * @param {readonly DonphanPermitQueryRow[]} rows - Deterministically ordered unique permit rows.
 * @returns {Promise<{ parquetPath: string, rowCount: number, sha256: string }>} Artifact identity and row count.
 */
export async function writeDonphanPermitParquet(parquetPath, rows) {
  await mkdir(dirname(parquetPath), { recursive: true, mode: 0o700 });
  const writer = await ParquetWriter.openFile(
    buildDonphanPermitParquetSchema(),
    parquetPath,
  );
  try {
    for (const row of rows) {
      await writer.appendRow(toParquetRecord(row));
    }
  } finally {
    await writer.close();
  }
  await chmod(parquetPath, 0o600);
  const bytes = await readFile(parquetPath);
  return {
    parquetPath,
    rowCount: rows.length,
    sha256: createHash("sha256").update(bytes).digest("hex"),
  };
}

/**
 * Write deterministic private JSONL for audit/debug parity with the Parquet
 * artifact. Empty input produces an empty mode-0600 file.
 *
 * @param {string} outputPath - Destination JSONL path.
 * @param {readonly Record<string, unknown>[]} rows - JSON-compatible records.
 * @returns {Promise<void>} Resolves after the complete artifact is written.
 */
export async function writePrivateJsonl(outputPath, rows) {
  await mkdir(dirname(outputPath), { recursive: true, mode: 0o700 });
  const text =
    rows.length === 0
      ? ""
      : `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`;
  await writeFile(outputPath, text, { encoding: "utf8", mode: 0o600 });
}
