#!/usr/bin/env node

import { createReadStream } from "fs";
import { mkdir, writeFile } from "fs/promises";
import path from "path";
import { pathToFileURL } from "url";
import { parse } from "csv-parse";

const DEFAULT_INPUT_PATH = "downloads/rock-island/rock-island.csv";
const DEFAULT_OUTPUT_PATH =
  "downloads/rock-island/rock-island-validation-sample.csv";
const DEFAULT_LIMIT = 25;

/**
 * Minimal, non-PII columns retained for transform validation provenance.
 *
 * @type {readonly string[]}
 */
export const SAMPLE_COLUMNS = Object.freeze([
  "request_identifier",
  "sample_reasons",
  "source_class",
  "source_zoning",
  "source_municipality",
  "has_site_address",
  "has_assessment",
  "has_structure",
  "source_record_count",
]);

/**
 * @typedef {Record<string, string | undefined>} SeedRecord
 *
 * @typedef {object} ValidationSampleRow
 * @property {string} request_identifier - Canonical ten-digit parcel identifier.
 * @property {string} sample_reasons - Pipe-delimited variability reasons.
 * @property {string} source_class - Uninterpreted county class code.
 * @property {string} source_zoning - Uninterpreted source zoning value.
 * @property {string} source_municipality - Uninterpreted source municipality.
 * @property {"true" | "false"} has_site_address - Whether the source has a site address.
 * @property {"true" | "false"} has_assessment - Whether EAV and EMV are both present.
 * @property {"true" | "false"} has_structure - Whether source square footage or built year indicates a structure.
 * @property {string} source_record_count - Number of ArcGIS records consolidated into the seed row.
 */

/**
 * @typedef {object} SampleCliOptions
 * @property {string} inputPath - Complete Rock Island seed CSV path.
 * @property {string} outputPath - Validation sample CSV destination.
 * @property {number} limit - Maximum selected parcel count.
 */

/**
 * Return a normalized source string without converting missing values to text.
 *
 * @param {string | undefined} value - Parsed seed value.
 * @returns {string} Trimmed value or an empty string.
 */
function clean(value) {
  return typeof value === "string" ? value.trim() : "";
}

/**
 * Return whether a source field contains a finite number greater than zero.
 *
 * @param {string | undefined} value - Parsed numeric source value.
 * @returns {boolean} True when the source value is positive.
 */
function isPositiveNumber(value) {
  const parsed = Number(clean(value));
  return Number.isFinite(parsed) && parsed > 0;
}

/**
 * Classify a seed row by transform-relevant variability without interpreting
 * undocumented county class codes or zoning semantics.
 *
 * @param {SeedRecord} record - Parsed Rock Island seed row.
 * @returns {string[]} Stable reason labels for sample selection.
 */
export function classifySampleReasons(record) {
  const reasons = [];
  const hasAddress = clean(record.source_site_address).length > 0;
  const hasAssessment =
    clean(record.source_EAV).length > 0 && clean(record.source_EMV).length > 0;
  const hasStructure =
    isPositiveNumber(record.source_TOTSQFT) ||
    isPositiveNumber(record.source_YRBuilt);

  if (!hasAddress) reasons.push("missing_site_address");
  if (!hasAssessment) reasons.push("incomplete_assessment");
  reasons.push(hasStructure ? "improved_structure" : "no_recorded_structure");
  if (clean(record.source_class).length === 0) reasons.push("blank_class");
  if (clean(record.source_Zoning).length === 0) reasons.push("blank_zoning");
  if (Number(clean(record.source_record_count)) > 1) {
    reasons.push("consolidated_duplicate_pin");
  }
  return reasons;
}

/**
 * Convert a parsed seed record to the non-PII validation sample contract.
 *
 * @param {SeedRecord} record - Parsed Rock Island seed row.
 * @param {readonly string[]} reasons - Selection reasons assigned to the row.
 * @returns {ValidationSampleRow} Minimal validation sample row.
 */
function toSampleRow(record, reasons) {
  const hasAddress = clean(record.source_site_address).length > 0;
  const hasAssessment =
    clean(record.source_EAV).length > 0 && clean(record.source_EMV).length > 0;
  const hasStructure =
    isPositiveNumber(record.source_TOTSQFT) ||
    isPositiveNumber(record.source_YRBuilt);
  return {
    request_identifier: clean(record.source_identifier),
    sample_reasons: reasons.join("|"),
    source_class: clean(record.source_class),
    source_zoning: clean(record.source_Zoning),
    source_municipality: clean(record.source_municipality),
    has_site_address: hasAddress ? "true" : "false",
    has_assessment: hasAssessment ? "true" : "false",
    has_structure: hasStructure ? "true" : "false",
    source_record_count: clean(record.source_record_count) || "1",
  };
}

/**
 * Select a deterministic, non-PII validation sample. The algorithm first
 * guarantees one row for each observed edge-case reason, then adds one row for
 * each raw class code in lexical order, and finally fills from parcel order.
 * Raw class codes remain uninterpreted because no official class dictionary is
 * available.
 *
 * @param {Iterable<SeedRecord>} records - Complete or test seed records.
 * @param {number} [limit=DEFAULT_LIMIT] - Maximum rows to select.
 * @returns {ValidationSampleRow[]} Deterministically selected sample rows.
 */
export function selectValidationSample(records, limit = DEFAULT_LIMIT) {
  if (!Number.isInteger(limit) || limit <= 0) {
    throw new Error("Validation sample limit must be a positive integer");
  }
  /** @type {Map<string, SeedRecord>} */
  const byReason = new Map();
  /** @type {Map<string, SeedRecord>} */
  const byClass = new Map();
  /** @type {SeedRecord[]} */
  const fallback = [];

  for (const record of records) {
    const requestIdentifier = clean(record.source_identifier);
    if (!/^[0-9]{10}$/.test(requestIdentifier)) continue;
    const reasons = classifySampleReasons(record);
    for (const reason of reasons) {
      if (!byReason.has(reason)) byReason.set(reason, record);
    }
    const classCode = clean(record.source_class) || "<blank>";
    if (!byClass.has(classCode)) byClass.set(classCode, record);
    if (fallback.length < limit * 4) fallback.push(record);
  }

  /** @type {Map<string, { record: SeedRecord, reasons: Set<string> }>} */
  const selected = new Map();
  /**
   * Add a candidate while merging all reasons assigned to the same parcel.
   *
   * @param {SeedRecord} record - Candidate seed row.
   * @param {string} reason - Selection reason to retain.
   * @returns {void}
   */
  const addRecord = (record, reason) => {
    if (selected.size >= limit) return;
    const requestIdentifier = clean(record.source_identifier);
    const existing = selected.get(requestIdentifier);
    if (existing) {
      existing.reasons.add(reason);
      return;
    }
    selected.set(requestIdentifier, {
      record,
      reasons: new Set([...classifySampleReasons(record), reason]),
    });
  };

  for (const [reason, record] of [...byReason.entries()].sort(
    ([left], [right]) => left.localeCompare(right),
  )) {
    addRecord(record, reason);
  }
  for (const [classCode, record] of [...byClass.entries()].sort(
    ([left], [right]) => left.localeCompare(right),
  )) {
    addRecord(record, `class:${classCode}`);
  }
  for (const record of fallback) {
    addRecord(record, "deterministic_fill");
  }

  return [...selected.values()].map(({ record, reasons }) =>
    toSampleRow(record, [...reasons].sort()),
  );
}

/**
 * Quote a value according to RFC 4180 CSV rules.
 *
 * @param {string} value - Cell value.
 * @returns {string} CSV-safe cell.
 */
function encodeCsvCell(value) {
  if (!/[",\r\n]/.test(value)) return value;
  return `"${value.replaceAll('"', '""')}"`;
}

/**
 * Render validation rows in stable column order.
 *
 * @param {readonly ValidationSampleRow[]} rows - Selected validation rows.
 * @returns {string} Complete CSV document.
 */
export function renderValidationSampleCsv(rows) {
  const lines = [SAMPLE_COLUMNS.join(",")];
  for (const row of rows) {
    lines.push(
      SAMPLE_COLUMNS.map((column) =>
        encodeCsvCell(String(row[column] ?? "")),
      ).join(","),
    );
  }
  return `${lines.join("\n")}\n`;
}

/**
 * Parse command-line arguments.
 *
 * @param {readonly string[]} argv - Arguments after the script path.
 * @returns {SampleCliOptions} Validated sample-build options.
 */
export function parseCliOptions(argv) {
  /** @type {SampleCliOptions} */
  const options = {
    inputPath: DEFAULT_INPUT_PATH,
    outputPath: DEFAULT_OUTPUT_PATH,
    limit: DEFAULT_LIMIT,
  };
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (typeof value !== "string" || value.startsWith("--")) {
      throw new Error(`Missing value for ${flag}`);
    }
    if (flag === "--input") options.inputPath = value;
    else if (flag === "--output") options.outputPath = value;
    else if (flag === "--limit") options.limit = Number.parseInt(value, 10);
    else throw new Error(`Unknown option: ${flag}`);
  }
  if (!Number.isInteger(options.limit) || options.limit <= 0) {
    throw new Error("--limit must be a positive integer");
  }
  return options;
}

/**
 * Stream the complete seed and write a bounded validation sample.
 *
 * @param {SampleCliOptions} options - Validated input/output options.
 * @returns {Promise<ValidationSampleRow[]>} Selected rows written to disk.
 */
export async function buildValidationSample(options) {
  /** @type {SeedRecord[]} */
  const records = [];
  const parser = createReadStream(options.inputPath).pipe(
    parse({
      columns: true,
      skip_empty_lines: true,
      trim: true,
    }),
  );
  for await (const record of parser) {
    records.push(/** @type {SeedRecord} */ (record));
  }
  const rows = selectValidationSample(records, options.limit);
  if (rows.length !== options.limit) {
    throw new Error(
      `Expected ${options.limit} validation rows but selected ${rows.length}`,
    );
  }
  await mkdir(path.dirname(options.outputPath), { recursive: true });
  await writeFile(options.outputPath, renderValidationSampleCsv(rows), "utf8");
  return rows;
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const options = parseCliOptions(process.argv.slice(2));
  buildValidationSample(options)
    .then((rows) => {
      console.log(
        JSON.stringify(
          {
            outputPath: path.resolve(options.outputPath),
            rowsWritten: rows.length,
            reasons: [
              ...new Set(rows.flatMap((row) => row.sample_reasons.split("|"))),
            ]
              .filter(Boolean)
              .sort(),
          },
          null,
          2,
        ),
      );
    })
    .catch((error) => {
      console.error(error instanceof Error ? error.message : String(error));
      process.exitCode = 1;
    });
}
