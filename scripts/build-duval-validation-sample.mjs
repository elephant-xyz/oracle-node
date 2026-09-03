#!/usr/bin/env node

import { createReadStream } from "fs";
import { mkdir, writeFile } from "fs/promises";
import path from "path";
import { pathToFileURL } from "url";
import { parse } from "csv-parse";

import { isValidDorParcelId } from "./duval/lib.mjs";
import { selectPilotSample } from "./build-duval-seed.mjs";

const DEFAULT_INPUT_PATH = "downloads/duval/duval.csv";
const DEFAULT_OUTPUT_PATH = "downloads/duval/pilot-seed-50.csv";
const DEFAULT_LIMIT = 50;

/**
 * Minimal, non-PII columns retained for transform validation provenance.
 *
 * @type {readonly string[]}
 */
export const SAMPLE_COLUMNS = Object.freeze([
  "request_identifier",
  "sample_reasons",
  "source_dor_uc",
  "source_pa_uc",
  "has_site_address",
  "has_assessment",
  "has_structure",
  "source_record_count",
]);

/**
 * @param {string | undefined} value
 * @returns {string}
 */
function clean(value) {
  return typeof value === "string" ? value.trim() : "";
}

/**
 * @param {string | undefined} value
 * @returns {boolean}
 */
function isPositiveNumber(value) {
  const parsed = Number(clean(value));
  return Number.isFinite(parsed) && parsed > 0;
}

/**
 * @param {Record<string, string | undefined>} record
 * @returns {string[]}
 */
export function classifySampleReasons(record) {
  const reasons = [];
  const hasAddress = clean(record.source_PHY_ADDR1).length > 0;
  const hasAssessment = clean(record.source_JV).length > 0;
  const hasStructure =
    isPositiveNumber(record.source_TOT_LVG_AREA) ||
    isPositiveNumber(record.source_ACT_YR_BLT);

  if (!hasAddress) reasons.push("missing_site_address");
  if (!hasAssessment) reasons.push("incomplete_assessment");
  reasons.push(hasStructure ? "improved_structure" : "no_recorded_structure");
  if (clean(record.source_DOR_UC).length === 0) reasons.push("blank_dor_uc");
  if (Number(clean(record.source_record_count)) > 1) {
    reasons.push("consolidated_duplicate_pin");
  }
  return reasons;
}

/**
 * @param {Record<string, string | undefined>} record
 * @param {readonly string[]} reasons
 */
function toSampleRow(record, reasons) {
  const hasAddress = clean(record.source_PHY_ADDR1).length > 0;
  const hasAssessment = clean(record.source_JV).length > 0;
  const hasStructure =
    isPositiveNumber(record.source_TOT_LVG_AREA) ||
    isPositiveNumber(record.source_ACT_YR_BLT);
  return {
    request_identifier: clean(record.source_identifier),
    sample_reasons: reasons.join("|"),
    source_dor_uc: clean(record.source_DOR_UC),
    source_pa_uc: clean(record.source_PA_UC),
    has_site_address: hasAddress ? "true" : "false",
    has_assessment: hasAssessment ? "true" : "false",
    has_structure: hasStructure ? "true" : "false",
    source_record_count: clean(record.source_record_count) || "1",
  };
}

/**
 * @param {Iterable<Record<string, string | undefined>>} records
 * @param {number} [limit=DEFAULT_LIMIT]
 */
export function selectValidationSample(records, limit = DEFAULT_LIMIT) {
  if (!Number.isInteger(limit) || limit <= 0) {
    throw new Error("Validation sample limit must be a positive integer");
  }
  /** @type {Map<string, Record<string, string | undefined>>} */
  const byReason = new Map();
  /** @type {Map<string, Record<string, string | undefined>>} */
  const byClass = new Map();
  /** @type {Record<string, string | undefined>[]} */
  const fallback = [];

  for (const record of records) {
    const requestIdentifier = clean(record.source_identifier);
    if (!isValidDorParcelId(requestIdentifier)) continue;
    const reasons = classifySampleReasons(record);
    for (const reason of reasons) {
      if (!byReason.has(reason)) byReason.set(reason, record);
    }
    const classCode = clean(record.source_DOR_UC) || "<blank>";
    if (!byClass.has(classCode)) byClass.set(classCode, record);
    if (fallback.length < limit * 4) fallback.push(record);
  }

  /** @type {Map<string, { record: Record<string, string | undefined>, reasons: Set<string> }>} */
  const selected = new Map();
  /**
   * @param {Record<string, string | undefined>} record
   * @param {string} reason
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
    addRecord(record, `dor_uc:${classCode}`);
  }
  for (const record of fallback) {
    addRecord(record, "deterministic_fill");
  }

  return [...selected.values()].map(({ record, reasons }) =>
    toSampleRow(record, [...reasons].sort()),
  );
}

/**
 * @param {string} value
 */
function encodeCsvCell(value) {
  if (!/[",\r\n]/.test(value)) return value;
  return `"${value.replaceAll('"', '""')}"`;
}

/**
 * @param {readonly ReturnType<typeof toSampleRow>[]} rows
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
 * @param {readonly string[]} argv
 */
export function parseCliOptions(argv) {
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
 * Task 5 writes the ~50-parcel DOR_UC pilot with the same SEED_COLUMNS contract
 * as the full seed (`selectPilotSample`). `selectValidationSample` remains a
 * thinner transform-validation helper and is not this CLI's output.
 *
 * @param {{ inputPath: string, outputPath: string, limit: number }} options
 */
export async function buildValidationSample(options) {
  /** @type {Record<string, string>[]} */
  const records = [];
  const parser = createReadStream(options.inputPath).pipe(
    parse({
      columns: true,
      skip_empty_lines: true,
      trim: true,
    }),
  );
  for await (const record of parser) {
    const compact = /** @type {Record<string, string>} */ (record);
    compact.parcel_polygon = "";
    compact.source_features_json = "";
    records.push(compact);
  }
  const rows = selectPilotSample(records, options.limit);
  if (rows.length !== options.limit) {
    throw new Error(
      `Expected ${options.limit} validation rows but selected ${rows.length}`,
    );
  }
  await mkdir(path.dirname(options.outputPath), { recursive: true });
  const { renderSeedCsv } = await import("./duval/lib.mjs");
  await writeFile(options.outputPath, renderSeedCsv(rows), "utf8");
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
            identifiers: rows.map((row) => row.source_identifier),
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
