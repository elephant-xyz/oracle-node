#!/usr/bin/env node

import { createReadStream } from "fs";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "fs/promises";
import os from "os";
import path from "path";
import { pathToFileURL } from "url";
import AdmZip from "adm-zip";
import { parse } from "csv-parse";
import { parse as parseCsv } from "csv-parse/sync";
import { transform, validate } from "@elephant-xyz/cli/lib";

const DEFAULT_SEED_PATH = "downloads/rock-island/rock-island.csv";
const DEFAULT_SAMPLE_PATH =
  "downloads/rock-island/rock-island-validation-sample.csv";
const DEFAULT_CAPTURES_PATH =
  "downloads/rock-island/rock-island-validation-captures.zip";
const DEFAULT_SCRIPTS_DIRECTORY =
  "../Counties-trasform-scripts/rock island/scripts";
const DEFAULT_OUTPUT_DIRECTORY = "downloads/rock-island/appraisal-validation";

/**
 * @typedef {Record<string, string | undefined>} CsvRecord
 *
 * @typedef {object} ValidationCliOptions
 * @property {string} seedPath - Complete county seed CSV.
 * @property {string} samplePath - Validation sample CSV.
 * @property {string} capturesPath - Prepared capture ZIP.
 * @property {string} scriptsDirectory - County scripts package directory.
 * @property {string} outputDirectory - Per-parcel validation output directory.
 * @property {number | null} limit - Optional number of sample rows to process.
 *
 * @typedef {object} ParcelValidationResult
 * @property {string} requestIdentifier - Canonical parcel identifier.
 * @property {boolean} transformSuccess - Whether the scripts transform completed.
 * @property {boolean} validationSuccess - Whether Lexicon validation passed.
 * @property {string[]} outputFiles - Transformed ZIP entries.
 * @property {string[]} validationIssues - Distinct validator error messages.
 * @property {string | null} transformError - Transform error when present.
 * @property {string | null} validationError - Validation error when present.
 */

/**
 * Quote one value according to RFC 4180.
 *
 * @param {string} value - Cell value.
 * @returns {string} CSV-safe value.
 */
function encodeCsvCell(value) {
  if (!/[",\r\n]/.test(value)) return value;
  return `"${value.replaceAll('"', '""')}"`;
}

/**
 * Render one complete seed record while preserving the original header order.
 *
 * @param {readonly string[]} columns - Seed CSV columns.
 * @param {CsvRecord} record - Parsed seed record.
 * @returns {string} One-row seed CSV.
 */
export function renderSeedRecord(columns, record) {
  const header = columns.map(encodeCsvCell).join(",");
  const row = columns
    .map((column) => encodeCsvCell(record[column] ?? ""))
    .join(",");
  return `${header}\n${row}\n`;
}

/**
 * Extract distinct validator messages from an Elephant CLI error CSV.
 *
 * @param {string} csvText - Complete validation error CSV contents.
 * @returns {string[]} Distinct non-empty error messages in first-seen order.
 */
export function parseValidationIssues(csvText) {
  /** @type {CsvRecord[]} */
  const rows = parseCsv(csvText, {
    columns: true,
    skip_empty_lines: true,
  });
  return [
    ...new Set(
      rows
        .map((row) => row.error_message ?? "")
        .filter((message) => message.length > 0),
    ),
  ];
}

/**
 * Parse command-line options.
 *
 * @param {readonly string[]} argv - Arguments after the script path.
 * @returns {ValidationCliOptions} Validated options.
 */
export function parseCliOptions(argv) {
  /** @type {ValidationCliOptions} */
  const options = {
    seedPath: DEFAULT_SEED_PATH,
    samplePath: DEFAULT_SAMPLE_PATH,
    capturesPath: DEFAULT_CAPTURES_PATH,
    scriptsDirectory: DEFAULT_SCRIPTS_DIRECTORY,
    outputDirectory: DEFAULT_OUTPUT_DIRECTORY,
    limit: null,
  };
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (typeof value !== "string" || value.startsWith("--")) {
      throw new Error(`Missing value for ${flag}`);
    }
    if (flag === "--seed") options.seedPath = value;
    else if (flag === "--sample") options.samplePath = value;
    else if (flag === "--captures") options.capturesPath = value;
    else if (flag === "--scripts") options.scriptsDirectory = value;
    else if (flag === "--output") options.outputDirectory = value;
    else if (flag === "--limit") options.limit = Number.parseInt(value, 10);
    else throw new Error(`Unknown option: ${flag}`);
  }
  if (
    options.limit !== null &&
    (!Number.isInteger(options.limit) || options.limit <= 0)
  ) {
    throw new Error("--limit must be a positive integer");
  }
  return options;
}

/**
 * Read ordered request identifiers from the non-PII validation sample.
 *
 * @param {string} samplePath - Validation sample CSV path.
 * @param {number | null} limit - Optional sample limit.
 * @returns {Promise<string[]>} Ordered canonical parcel identifiers.
 */
async function readSampleIdentifiers(samplePath, limit) {
  const sampleText = await readFile(samplePath, "utf8");
  /** @type {CsvRecord[]} */
  const rows = parseCsv(sampleText, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });
  const identifiers = rows
    .map((row) => row.request_identifier ?? "")
    .filter((value) => /^[0-9]{10}$/.test(value));
  return limit === null ? identifiers : identifiers.slice(0, limit);
}

/**
 * Stream the complete seed once and retain only selected validation records.
 *
 * @param {string} seedPath - Complete seed CSV path.
 * @param {ReadonlySet<string>} identifiers - PINs to retain.
 * @returns {Promise<{ columns: string[], records: Map<string, CsvRecord> }>} Selected records and original columns.
 */
async function readSelectedSeedRecords(seedPath, identifiers) {
  /** @type {Map<string, CsvRecord>} */
  const records = new Map();
  /** @type {string[]} */
  let columns = [];
  const parser = createReadStream(seedPath).pipe(
    parse({
      columns: (header) => {
        columns = header;
        return header;
      },
      skip_empty_lines: true,
    }),
  );
  for await (const parsedRecord of parser) {
    const record = /** @type {CsvRecord} */ (parsedRecord);
    const identifier = record.source_identifier ?? "";
    if (identifiers.has(identifier)) records.set(identifier, record);
  }
  return { columns, records };
}

/**
 * Package the currently checked-out county scripts in the format consumed by
 * the deployed legacy transform worker.
 *
 * @param {string} scriptsDirectory - Directory containing required scripts.
 * @param {string} destination - ZIP destination.
 * @returns {void}
 */
function packageScripts(scriptsDirectory, destination) {
  const zip = new AdmZip();
  zip.addLocalFolder(scriptsDirectory);
  zip.writeZip(destination);
}

/**
 * Create the same root-level county-prepare input assembled by the pre and
 * downloader stages: seed entities, original input row, and one fresh capture.
 *
 * @param {object} params - Prepared input parameters.
 * @param {string} params.seedOutputPath - CLI seed-transform output ZIP.
 * @param {Buffer} params.seedCsv - One-row seed CSV.
 * @param {AdmZip.IZipEntry} params.captureEntry - Fresh prepare capture.
 * @param {string} params.destination - Prepared ZIP destination.
 * @returns {void}
 */
function createPreparedInput({
  seedOutputPath,
  seedCsv,
  captureEntry,
  destination,
}) {
  const seedZip = new AdmZip(seedOutputPath);
  const unnormalizedAddress = seedZip.getEntry(
    "data/unnormalized_address.json",
  );
  const propertySeed = seedZip.getEntry("data/property_seed.json");
  if (unnormalizedAddress === null || propertySeed === null) {
    throw new Error("Seed transform output is missing compatibility entities");
  }
  const prepared = new AdmZip();
  prepared.addFile("unnormalized_address.json", unnormalizedAddress.getData());
  prepared.addFile("property_seed.json", propertySeed.getData());
  prepared.addFile("input.csv", seedCsv);
  prepared.addFile(captureEntry.entryName, captureEntry.getData());
  prepared.writeZip(destination);
}

/**
 * Run seed transform, county transform, and Lexicon validation for one parcel.
 *
 * @param {object} params - Per-parcel validation parameters.
 * @param {string} params.identifier - Canonical PIN.
 * @param {readonly string[]} params.columns - Original seed column order.
 * @param {CsvRecord} params.record - Complete seed record.
 * @param {AdmZip.IZipEntry} params.captureEntry - Fresh prepare capture.
 * @param {string} params.scriptsZipPath - Packaged county scripts.
 * @param {string} params.outputDirectory - Durable output directory.
 * @returns {Promise<ParcelValidationResult>} Transform and validation result.
 */
async function validateParcel({
  identifier,
  columns,
  record,
  captureEntry,
  scriptsZipPath,
  outputDirectory,
}) {
  const temporaryDirectory = await mkdtemp(
    path.join(os.tmpdir(), `rock-island-${identifier}-`),
  );
  try {
    const seedCsvText = renderSeedRecord(columns, record);
    const seedCsvPath = path.join(temporaryDirectory, "seed.csv");
    const seedInputPath = path.join(temporaryDirectory, "seed-input.zip");
    const seedOutputPath = path.join(temporaryDirectory, "seed-output.zip");
    const preparedInputPath = path.join(
      outputDirectory,
      `${identifier}-prepared-input.zip`,
    );
    const transformedOutputPath = path.join(
      outputDirectory,
      `${identifier}.zip`,
    );
    await writeFile(seedCsvPath, seedCsvText, "utf8");
    const seedInput = new AdmZip();
    seedInput.addLocalFile(seedCsvPath, "", "seed.csv");
    seedInput.writeZip(seedInputPath);

    const seedResult = await transform({
      inputZip: seedInputPath,
      outputZip: seedOutputPath,
      cwd: temporaryDirectory,
    });
    if (!seedResult.success) {
      return {
        requestIdentifier: identifier,
        transformSuccess: false,
        validationSuccess: false,
        outputFiles: [],
        validationIssues: [],
        transformError: `Seed transform failed: ${seedResult.error ?? "unknown error"}`,
        validationError: null,
      };
    }
    createPreparedInput({
      seedOutputPath,
      seedCsv: Buffer.from(seedCsvText),
      captureEntry,
      destination: preparedInputPath,
    });
    const transformResult = await transform({
      inputZip: preparedInputPath,
      outputZip: transformedOutputPath,
      scriptsZip: scriptsZipPath,
      cwd: temporaryDirectory,
    });
    if (!transformResult.success) {
      return {
        requestIdentifier: identifier,
        transformSuccess: false,
        validationSuccess: false,
        outputFiles: [],
        validationIssues: [],
        transformError: transformResult.error ?? "Unknown transform error",
        validationError:
          transformResult.scriptFailure?.stderr ??
          transformResult.scriptFailure?.message ??
          null,
      };
    }
    const validationCsvPath = path.join(
      outputDirectory,
      `${identifier}-validation.csv`,
    );
    const validationResult = await validate({
      input: transformedOutputPath,
      outputCsv: validationCsvPath,
      cwd: temporaryDirectory,
    });
    const validationIssues = parseValidationIssues(
      await readFile(validationCsvPath, "utf8"),
    );
    const outputFiles = new AdmZip(transformedOutputPath)
      .getEntries()
      .map((entry) => entry.entryName)
      .sort();
    return {
      requestIdentifier: identifier,
      transformSuccess: true,
      validationSuccess: validationResult.success,
      outputFiles,
      validationIssues,
      transformError: null,
      validationError: validationResult.error ?? null,
    };
  } finally {
    await rm(temporaryDirectory, { recursive: true, force: true });
  }
}

/**
 * Execute the complete local appraisal-validation batch.
 *
 * @param {ValidationCliOptions} options - Validated CLI options.
 * @returns {Promise<ParcelValidationResult[]>} Per-parcel results.
 */
export async function runValidation(options) {
  const outputDirectory = path.resolve(options.outputDirectory);
  const scriptsDirectory = path.resolve(options.scriptsDirectory);
  const identifiers = await readSampleIdentifiers(
    options.samplePath,
    options.limit,
  );
  const identifierSet = new Set(identifiers);
  const { columns, records } = await readSelectedSeedRecords(
    options.seedPath,
    identifierSet,
  );
  if (records.size !== identifiers.length) {
    throw new Error(
      `Located ${records.size} of ${identifiers.length} validation seed rows`,
    );
  }
  const captures = new AdmZip(options.capturesPath);
  await mkdir(outputDirectory, { recursive: true });
  const scriptsZipPath = path.join(outputDirectory, "rock-island-scripts.zip");
  packageScripts(scriptsDirectory, scriptsZipPath);

  /** @type {ParcelValidationResult[]} */
  const results = [];
  for (const identifier of identifiers) {
    const captureEntry = captures.getEntry(`${identifier}.json`);
    const record = records.get(identifier);
    if (captureEntry === null || record === undefined) {
      throw new Error(`Missing capture or seed record for ${identifier}`);
    }
    results.push(
      await validateParcel({
        identifier,
        columns,
        record,
        captureEntry,
        scriptsZipPath,
        outputDirectory,
      }),
    );
  }
  await writeFile(
    path.join(outputDirectory, "summary.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        total: results.length,
        transformsPassed: results.filter((result) => result.transformSuccess)
          .length,
        validationsPassed: results.filter((result) => result.validationSuccess)
          .length,
        results,
      },
      null,
      2,
    ),
    "utf8",
  );
  return results;
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const options = parseCliOptions(process.argv.slice(2));
  runValidation(options)
    .then((results) => {
      console.log(
        JSON.stringify(
          {
            total: results.length,
            transformsPassed: results.filter(
              (result) => result.transformSuccess,
            ).length,
            validationsPassed: results.filter(
              (result) => result.validationSuccess,
            ).length,
            outputDirectory: path.resolve(options.outputDirectory),
          },
          null,
          2,
        ),
      );
      if (results.some((result) => !result.validationSuccess)) {
        process.exitCode = 1;
      }
    })
    .catch((error) => {
      console.error(error instanceof Error ? error.message : String(error));
      process.exitCode = 1;
    });
}
