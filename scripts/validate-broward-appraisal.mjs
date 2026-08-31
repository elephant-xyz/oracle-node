#!/usr/bin/env node

/**
 * Local Broward appraisal prepare → transform → optional Lexicon validate.
 *
 * Runs without AWS. elephant-cli prepare hits the BCPA JSON API; the published
 * Broward extractor is then run against the unwrapped ASP.NET envelope.
 */

import { mkdir, mkdtemp, readFile, rm, writeFile } from "fs/promises";
import os from "os";
import path from "path";
import { pathToFileURL } from "url";
import AdmZip from "adm-zip";
import { parse as parseCsv } from "csv-parse/sync";
import { prepare, transform, validate } from "@elephant-xyz/cli/lib";

import {
  requireParcelRecords,
  unwrapBrowardPrepareCapture,
} from "./capture-broward-parcel.mjs";
import {
  BROWARD_COUNTY_NAME,
  normalizeBrowardFolio,
} from "./broward-folio.mjs";
import {
  BROWARD_USE_CODE_MATCHER_CJS,
  FIXED_BROWARD_USE_CODE_MATCH,
  PUBLISHED_BROWARD_USE_CODE_MATCH,
} from "./broward-use-code.mjs";

const DEFAULT_SEED_PATH = "downloads/broward/broward-pilot.csv";
const DEFAULT_CAPTURES_PATH = "downloads/broward/broward-pilot-captures.zip";
const DEFAULT_FLOW_PATH = "multi-request-flows/Broward.json";
const DEFAULT_SCRIPTS_DIRECTORY =
  "../Counties-trasform-scripts/broward/scripts";
const DEFAULT_OUTPUT_DIRECTORY = "downloads/broward/appraisal-validation";

/**
 * @typedef {Record<string, string | undefined>} CsvRecord
 *
 * @typedef {object} ValidationCliOptions
 * @property {string} seedPath - Pilot or county seed CSV.
 * @property {string} capturesPath - Prepared capture ZIP.
 * @property {string} flowPath - Multi-request flow JSON.
 * @property {string} scriptsDirectory - County scripts package directory.
 * @property {string} outputDirectory - Per-parcel validation output directory.
 * @property {number | null} limit - Optional number of seed rows to process.
 * @property {boolean} prepareCaptures - When true, run elephant-cli prepare first.
 * @property {boolean} skipValidate - When true, skip CLI Lexicon validation.
 * @property {boolean} applyUseCodeFix - When true, patch family-level use-code matching.
 *
 * @typedef {object} BrowardParcelRecord
 * @property {string} [folioNumber] - Appraiser folio.
 * @property {string} [useCode] - Appraiser use-code label.
 * @property {string} [situsAddress1] - Situs street.
 * @property {string} [situsCity] - Situs city.
 *
 * @typedef {object} TransformedProperty
 * @property {string} [property_usage_type] - Lexicon usage type.
 * @property {string} [parcel_identifier] - Transformed folio.
 * @property {string} [property_type] - Lexicon property type.
 * @property {string} [build_status] - VacantLand or Improved.
 *
 * @typedef {object} TransformedAddress
 * @property {string} [county_name] - County label written by the extractor.
 * @property {string} [unnormalized_address] - Situs address from the capture.
 *
 * @typedef {object} ParcelValidationResult
 * @property {string} requestIdentifier - Canonical folio.
 * @property {boolean} captureSuccess - Whether prepare returned a non-empty parcel list.
 * @property {boolean} transformSuccess - Whether the scripts transform completed.
 * @property {boolean} validationSuccess - Whether Lexicon validation passed.
 * @property {string | null} sourceUseCode - Appraiser use-code label.
 * @property {string | null} propertyUsageType - Transformed usage type.
 * @property {string | null} propertyType - Transformed property type.
 * @property {string | null} countyName - Transformed address.county_name.
 * @property {string | null} situsCity - Appraiser situs city.
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
    capturesPath: DEFAULT_CAPTURES_PATH,
    flowPath: DEFAULT_FLOW_PATH,
    scriptsDirectory: DEFAULT_SCRIPTS_DIRECTORY,
    outputDirectory: DEFAULT_OUTPUT_DIRECTORY,
    limit: null,
    prepareCaptures: true,
    skipValidate: false,
    applyUseCodeFix: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index];
    if (flag === "--skip-prepare") {
      options.prepareCaptures = false;
      continue;
    }
    if (flag === "--skip-validate") {
      options.skipValidate = true;
      continue;
    }
    if (flag === "--apply-use-code-fix") {
      options.applyUseCodeFix = true;
      continue;
    }
    const value = argv[index + 1];
    if (typeof value !== "string" || value.startsWith("--")) {
      throw new Error(`Missing value for ${flag}`);
    }
    if (flag === "--seed") options.seedPath = value;
    else if (flag === "--captures") options.capturesPath = value;
    else if (flag === "--flow") options.flowPath = value;
    else if (flag === "--scripts") options.scriptsDirectory = value;
    else if (flag === "--output") options.outputDirectory = value;
    else if (flag === "--limit") options.limit = Number.parseInt(value, 10);
    else throw new Error(`Unknown option: ${flag}`);
    index += 1;
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
 * Read seed rows in file order, keeping only usable Broward folios.
 *
 * @param {string} seedPath - Seed CSV path.
 * @param {number | null} limit - Optional row cap.
 * @returns {Promise<{ columns: string[], records: CsvRecord[] }>} Ordered seed rows.
 */
export async function readSeedRecords(seedPath, limit) {
  const text = await readFile(seedPath, "utf8");
  /** @type {CsvRecord[]} */
  const rows = parseCsv(text, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });
  const records = rows.filter((row) => {
    return normalizeBrowardFolio(row.request_identifier) !== undefined;
  });
  const columns =
    rows.length > 0 && rows[0] !== undefined ? Object.keys(rows[0]) : [];
  return {
    columns,
    records: limit === null ? records : records.slice(0, limit),
  };
}

/**
 * Copy published Broward scripts and patch family-level use-code matching.
 *
 * The live BCPA API often returns `04 - Condominium` instead of `04-01 ...`.
 * The published extractor then reads `propertyMapping.property_type` on
 * `undefined`. This local patch belongs in Counties-trasform-scripts before
 * an AWS run; it is optional here so the published-script result stays honest.
 *
 * @param {string} scriptsDirectory - Published scripts directory.
 * @param {string} destinationDirectory - Patched copy destination.
 * @returns {Promise<string>} Destination directory.
 */
export async function applyBrowardUseCodeFix(
  scriptsDirectory,
  destinationDirectory,
) {
  await mkdir(destinationDirectory, { recursive: true });
  const extractorName = "data_extractor.js";
  const sourceZip = new AdmZip();
  sourceZip.addLocalFolder(scriptsDirectory);
  sourceZip.extractAllTo(destinationDirectory, true);
  const extractorPath = path.join(destinationDirectory, extractorName);
  const extractorSource = await readFile(extractorPath, "utf8");
  if (!extractorSource.includes(PUBLISHED_BROWARD_USE_CODE_MATCH)) {
    throw new Error(
      "Published Broward use-code matcher not found; refuse to patch a drifted extractor",
    );
  }
  await writeFile(
    path.join(destinationDirectory, "findBrowardPropertyMapping.js"),
    BROWARD_USE_CODE_MATCHER_CJS,
    "utf8",
  );
  await writeFile(
    extractorPath,
    extractorSource.replace(
      PUBLISHED_BROWARD_USE_CODE_MATCH,
      FIXED_BROWARD_USE_CODE_MATCH,
    ),
    "utf8",
  );
  return destinationDirectory;
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
 * Read a JSON ZIP entry as an object, or null when missing or invalid.
 *
 * @param {AdmZip} zip - Open ZIP.
 * @param {string} entryName - Entry path.
 * @returns {Record<string, unknown> | null} Parsed object.
 */
function readZipJsonObject(zip, entryName) {
  const entry = zip.getEntry(entryName);
  if (entry === null) return null;
  try {
    const parsed = JSON.parse(entry.getData().toString("utf8"));
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      Array.isArray(parsed)
    ) {
      return null;
    }
    return /** @type {Record<string, unknown>} */ (parsed);
  } catch {
    return null;
  }
}

/**
 * First parcel record from a fail-closed unwrap of a prepare capture.
 *
 * @param {AdmZip.IZipEntry} captureEntry - `{folio}.json` prepare output.
 * @param {string} folio - Canonical folio.
 * @returns {{ envelope: import("./capture-broward-parcel.mjs").BrowardParcelEnvelope, record: BrowardParcelRecord }}
 *   Unwrapped envelope and first parcel.
 */
export function readCaptureParcel(captureEntry, folio) {
  const payload = JSON.parse(captureEntry.getData().toString("utf8"));
  const envelope = unwrapBrowardPrepareCapture(payload);
  const records = requireParcelRecords(envelope, folio);
  const record = /** @type {BrowardParcelRecord} */ (records[0] ?? {});
  return { envelope, record };
}

/**
 * Create the same root-level county-prepare input assembled by the pre and
 * downloader stages. The multi-request wrapper is preserved exactly as the
 * downloader would pass it to the county transform.
 *
 * @param {object} params - Prepared input parameters.
 * @param {string} params.seedOutputPath - CLI seed-transform output ZIP.
 * @param {Buffer} params.seedCsv - One-row seed CSV.
 * @param {AdmZip.IZipEntry} params.captureEntry - Fresh prepare capture.
 * @param {string} params.folio - Canonical folio, used as the capture filename.
 * @param {string} params.destination - Prepared ZIP destination.
 * @returns {void}
 */
function createPreparedInput({
  seedOutputPath,
  seedCsv,
  captureEntry,
  folio,
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
  readCaptureParcel(captureEntry, folio);
  const prepared = new AdmZip();
  prepared.addFile("unnormalized_address.json", unnormalizedAddress.getData());
  prepared.addFile("property_seed.json", propertySeed.getData());
  prepared.addFile("input.csv", seedCsv);
  prepared.addFile(`${folio}.json`, captureEntry.getData());
  prepared.writeZip(destination);
}

/**
 * Run seed transform, county transform, and optional Lexicon validation.
 *
 * @param {object} params - Per-parcel validation parameters.
 * @param {string} params.identifier - Canonical folio.
 * @param {readonly string[]} params.columns - Original seed column order.
 * @param {CsvRecord} params.record - Complete seed record.
 * @param {AdmZip.IZipEntry} params.captureEntry - Fresh prepare capture.
 * @param {string} params.scriptsZipPath - Packaged county scripts.
 * @param {string} params.outputDirectory - Durable output directory.
 * @param {boolean} params.skipValidate - Skip Lexicon validation.
 * @returns {Promise<ParcelValidationResult>} Transform and validation result.
 */
async function validateParcel({
  identifier,
  columns,
  record,
  captureEntry,
  scriptsZipPath,
  outputDirectory,
  skipValidate,
}) {
  /** @type {ParcelValidationResult} */
  const failed = {
    requestIdentifier: identifier,
    captureSuccess: false,
    transformSuccess: false,
    validationSuccess: false,
    sourceUseCode: null,
    propertyUsageType: null,
    propertyType: null,
    countyName: null,
    situsCity: null,
    outputFiles: [],
    validationIssues: [],
    transformError: null,
    validationError: null,
  };
  let sourceUseCode = null;
  let situsCity = null;
  try {
    const { record: parcel } = readCaptureParcel(captureEntry, identifier);
    sourceUseCode = parcel.useCode ?? null;
    situsCity = parcel.situsCity ?? null;
  } catch (error) {
    return {
      ...failed,
      transformError: error instanceof Error ? error.message : String(error),
    };
  }

  const temporaryDirectory = await mkdtemp(
    path.join(os.tmpdir(), `broward-${identifier}-`),
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
        ...failed,
        captureSuccess: true,
        sourceUseCode,
        situsCity,
        transformError: `Seed transform failed: ${seedResult.error ?? "unknown error"}`,
      };
    }
    createPreparedInput({
      seedOutputPath,
      seedCsv: Buffer.from(seedCsvText),
      captureEntry,
      folio: identifier,
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
        ...failed,
        captureSuccess: true,
        sourceUseCode,
        situsCity,
        transformError: transformResult.error ?? "Unknown transform error",
        validationError:
          transformResult.scriptFailure?.stderr ??
          transformResult.scriptFailure?.message ??
          null,
      };
    }
    const transformed = new AdmZip(transformedOutputPath);
    const outputFiles = transformed
      .getEntries()
      .map((entry) => entry.entryName)
      .sort();
    const property = /** @type {TransformedProperty | null} */ (
      readZipJsonObject(transformed, "data/property.json")
    );
    const address = /** @type {TransformedAddress | null} */ (
      readZipJsonObject(transformed, "data/address.json")
    );
    if (property === null) {
      return {
        ...failed,
        captureSuccess: true,
        sourceUseCode,
        situsCity,
        outputFiles,
        transformError:
          "County transform succeeded but data/property.json is missing",
      };
    }
    const propertyUsageType = property.property_usage_type ?? null;
    const countyName = address?.county_name ?? null;
    if (countyName !== BROWARD_COUNTY_NAME) {
      return {
        ...failed,
        captureSuccess: true,
        transformSuccess: true,
        sourceUseCode,
        propertyUsageType,
        propertyType: property.property_type ?? null,
        countyName,
        situsCity,
        outputFiles,
        transformError: `address.county_name is ${JSON.stringify(countyName)}, expected ${BROWARD_COUNTY_NAME}`,
      };
    }
    if (skipValidate) {
      return {
        requestIdentifier: identifier,
        captureSuccess: true,
        transformSuccess: true,
        validationSuccess: true,
        sourceUseCode,
        propertyUsageType,
        propertyType: property.property_type ?? null,
        countyName,
        situsCity,
        outputFiles,
        validationIssues: [],
        transformError: null,
        validationError: null,
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
    return {
      requestIdentifier: identifier,
      captureSuccess: true,
      transformSuccess: true,
      validationSuccess: validationResult.success,
      sourceUseCode,
      propertyUsageType,
      propertyType: property.property_type ?? null,
      countyName,
      situsCity,
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
 * Run elephant-cli prepare against the seed CSV and the Broward multi-request flow.
 *
 * @param {object} params - Prepare parameters.
 * @param {string} params.seedPath - Seed CSV.
 * @param {string} params.flowPath - Multi-request flow JSON.
 * @param {string} params.capturesPath - Destination ZIP.
 * @returns {Promise<void>}
 */
export async function prepareBrowardCaptures({
  seedPath,
  flowPath,
  capturesPath,
}) {
  await mkdir(path.dirname(capturesPath), { recursive: true });
  await prepare("", capturesPath, {
    inputCsv: seedPath,
    multiRequestFlowFile: flowPath,
  });
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
  const seedPath = path.resolve(options.seedPath);
  const capturesPath = path.resolve(options.capturesPath);
  const flowPath = path.resolve(options.flowPath);
  if (options.prepareCaptures) {
    await prepareBrowardCaptures({
      seedPath,
      flowPath,
      capturesPath,
    });
  }
  const { columns, records } = await readSeedRecords(seedPath, options.limit);
  if (records.length === 0) {
    throw new Error(`No usable Broward folios in ${seedPath}`);
  }
  const captures = new AdmZip(capturesPath);
  await mkdir(outputDirectory, { recursive: true });
  let packagedScriptsDirectory = scriptsDirectory;
  if (options.applyUseCodeFix) {
    packagedScriptsDirectory = path.join(
      outputDirectory,
      "broward-scripts-patched",
    );
    await applyBrowardUseCodeFix(scriptsDirectory, packagedScriptsDirectory);
  }
  const scriptsZipPath = path.join(outputDirectory, "broward-scripts.zip");
  packageScripts(packagedScriptsDirectory, scriptsZipPath);

  /** @type {ParcelValidationResult[]} */
  const results = [];
  for (const record of records) {
    const identifier = normalizeBrowardFolio(record.request_identifier);
    if (identifier === undefined) {
      throw new Error("Seed row lost its folio after normalization");
    }
    const captureEntry = captures.getEntry(`${identifier}.json`);
    if (captureEntry === null) {
      results.push({
        requestIdentifier: identifier,
        captureSuccess: false,
        transformSuccess: false,
        validationSuccess: false,
        sourceUseCode: null,
        propertyUsageType: null,
        propertyType: null,
        countyName: null,
        situsCity: null,
        outputFiles: [],
        validationIssues: [],
        transformError: `Missing prepare capture ${identifier}.json`,
        validationError: null,
      });
      continue;
    }
    results.push(
      await validateParcel({
        identifier,
        columns,
        record,
        captureEntry,
        scriptsZipPath,
        outputDirectory,
        skipValidate: options.skipValidate,
      }),
    );
  }
  const usageTypes = [
    ...new Set(
      results
        .map((result) => result.propertyUsageType)
        .filter((value) => typeof value === "string" && value.length > 0),
    ),
  ].sort();
  await writeFile(
    path.join(outputDirectory, "summary.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        total: results.length,
        capturesPassed: results.filter((result) => result.captureSuccess)
          .length,
        transformsPassed: results.filter((result) => result.transformSuccess)
          .length,
        validationsPassed: results.filter((result) => result.validationSuccess)
          .length,
        propertyUsageTypes: usageTypes,
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
            capturesPassed: results.filter((result) => result.captureSuccess)
              .length,
            transformsPassed: results.filter(
              (result) => result.transformSuccess,
            ).length,
            validationsPassed: results.filter(
              (result) => result.validationSuccess,
            ).length,
            propertyUsageTypes: [
              ...new Set(
                results
                  .map((result) => result.propertyUsageType)
                  .filter(
                    (value) => typeof value === "string" && value.length > 0,
                  ),
              ),
            ].sort(),
            outputDirectory: path.resolve(options.outputDirectory),
          },
          null,
          2,
        ),
      );
      if (
        results.some(
          (result) => !result.captureSuccess || !result.transformSuccess,
        )
      ) {
        process.exitCode = 1;
      }
    })
    .catch((error) => {
      console.error(error instanceof Error ? error.message : String(error));
      process.exitCode = 1;
    });
}
