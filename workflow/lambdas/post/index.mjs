import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { promises as fs } from "fs";
import path from "path";
import os from "os";
import { randomUUID } from "crypto";
import {
  transform,
  validate,
  hash,
  upload,
  mirrorValidate,
} from "@elephant-xyz/cli/lib";
import { parse } from "csv-parse/sync";
import AdmZip from "adm-zip";
import { createTransformScriptsManager } from "./scripts-manager.mjs";
import { createErrorsRepository } from "./errors.mjs";

const s3 = new S3Client({});

const PERSISTENT_TRANSFORM_CACHE_DIR = path.join(
  os.tmpdir(),
  "county-transforms",
);
const transformScriptsManager = createTransformScriptsManager({
  s3Client: s3,
  persistentCacheRoot: PERSISTENT_TRANSFORM_CACHE_DIR,
});

/**
 * @typedef {Record<string, string>} CsvRecord
 */

/**
 * @typedef {`${string}.zip`} ZipFile
 */

/**
 * @typedef {object} PostOutput
 * @property {string} status
 * @property {CsvRecord[]} transactionItems
 * @property {number} [mvlMetric] - Global completeness metric from mirrorValidate
 */

/**
 * @typedef {object} PrepareOutput
 * @property {string} output_s3_uri
 */

/**
 * @typedef {object} S3Object
 * @property {string} key
 */

/**
 * @typedef {object} S3Bucket
 * @property {string} name
 */

/**
 * @typedef {object} S3Event
 * @property {S3Bucket} bucket
 * @property {S3Object} object
 */

/**
 * @typedef {object} PostInput
 * @property {PrepareOutput} prepare
 * @property {string} seed_output_s3_uri
 * @property {S3Event} s3
 * @property {boolean} [prepareSkipped] - Flag indicating if prepare step was skipped
 * @property {boolean} [saveErrorsOnValidationFailure] - If true (default), save errors to DynamoDB and return success. If false, throw error on validation failure.
 */

/**
 * @typedef {(level: "info"|"error"|"debug", msg: string, details?: Record<string, unknown>) => void} StructuredLogger
 */

/**
 * @typedef {object} S3Location
 * @property {string} bucket
 * @property {string} key
 */

/**
 * @typedef {object} DownloadInputsParams
 * @property {string} prepareUri - S3 URI pointing to the prepared county archive.
 * @property {string} seedUri - S3 URI pointing to the seed output archive.
 * @property {string} tmpDir - Temporary working directory for downloads.
 * @property {StructuredLogger} log - Structured logger used for diagnostics.
 */

/**
 * @typedef {object} DownloadInputsResult
 * @property {string} countyZipLocal - Local path to the downloaded county archive zip.
 * @property {string} seedZipLocal - Local path to the downloaded seed archive zip.
 */

/**
 * @typedef {object} TransformLocation
 * @property {string} transformBucket - S3 bucket containing transform scripts.
 * @property {string} transformKey - S3 key referencing the county-specific transform archive.
 */

/**
 * @typedef {object} UploadHashOutputsParams
 * @property {string[]} filesForIpfs - Archive paths that are uploaded via the IPFS helper.
 * @property {string} tmpDir - Temporary working directory.
 * @property {StructuredLogger} log - Structured logger used for diagnostics.
 * @property {string} pinataJwt - Authentication token for the upload helper.
 * @property {string} combinedCsvPath - Path to the combined CSV file destined for S3 upload.
 * @property {{ Bucket: string, Key: string }} submissionS3Location - Target S3 location for the submission CSV.
 */

/**
 * Combine two CSV files by keeping a single header row
 * from the first file and appending the data rows from both.
 *
 * @async
 * @function combineCsv
 * @param {string} seedHashCsv - Path to the first CSV file (provides header).
 * @param {string} countyHashCsv - Path to the second CSV file (data appended).
 * @param {string} tmp - Directory path where the combined file will be saved.
 * @returns {Promise<string>} - The path of the newly created combined CSV file.
 *
 * @example
 * const combined = await combineCsv("seed.csv", "county.csv", "/tmp");
 * console.log(`Combined CSV written to: ${combined}`);
 */
async function combineCsv(seedHashCsv, countyHashCsv, tmp) {
  const combinedCsv = path.join(tmp, "combined_hash.csv");

  const seedContent = await fs.readFile(seedHashCsv, "utf8");
  const countyContent = await fs.readFile(countyHashCsv, "utf8");

  const seedLines = seedContent.trim().split("\n");
  const countyLines = countyContent.trim().split("\n");

  const header = seedLines[0];

  const seedRows = seedLines.slice(1);
  const countyRows = countyLines.slice(1);

  const finalContent = [header, ...seedRows, ...countyRows].join("\n");
  await fs.writeFile(combinedCsv, finalContent, "utf8");

  return combinedCsv;
}

/** @typedef {string | null | undefined} MaybeString */
/**
 * Convert CSV file to JSON array
 * @param {string} csvPath - Path to CSV file
 * @returns {Promise<{property_cid: MaybeString,data_group_cid: MaybeString,file_path: MaybeString,error_path: MaybeString,error_message: MaybeString,currentValue: MaybeString,timestamp: MaybeString}[]>} - Array of objects representing CSV rows
 */
async function csvToJson(csvPath) {
  try {
    const csvContent = await fs.readFile(csvPath, "utf8");
    const records = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    });
    return records;
  } catch (error) {
    return [];
  }
}

/**
 * Parse an S3 URI into its bucket and key components.
 *
 * @param {string} uri - S3 URI in the form s3://bucket/key.
 * @returns {{ bucket: string, key: string }} - Parsed bucket and key.
 */
function parseS3Uri(uri) {
  const match = /^s3:\/\/([^/]+)\/(.*)$/.exec(uri);
  if (!match) {
    throw new Error(`Bad S3 URI: ${uri}`);
  }
  const bucket = match[1];
  const key = match[2];
  if (typeof bucket !== "string" || typeof key !== "string") {
    throw new Error("S3 URI must be a string");
  }
  return { bucket, key };
}

/**
 * Download an S3 object to a local file.
 *
 * @param {{ bucket: string, key: string }} location - Bucket and key location for the S3 object.
 * @param {string} destinationPath - Absolute path where the object will be stored.
 * @param {StructuredLogger} log - Structured logger.
 * @returns {Promise<void>}
 */
async function downloadS3Object({ bucket, key }, destinationPath, log) {
  log("info", "download_s3_object", {
    bucket,
    key,
    destination: destinationPath,
  });
  const result = await s3.send(
    new GetObjectCommand({ Bucket: bucket, Key: key }),
  );
  const bytes = await result.Body?.transformToByteArray();
  if (!bytes) {
    throw new Error(`Failed to download ${key} from ${bucket}`);
  }
  await fs.writeFile(destinationPath, Buffer.from(bytes));
}

/**
 * Ensure required environment variables are present.
 *
 * @param {string} name - Environment variable identifier.
 * @returns {string} - Resolved environment variable value.
 */
function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

/** @type {import("./errors.mjs").ErrorsRepository | null} */
let cachedErrorsRepository = null;

/**
 * Lazy-initialize the DynamoDB errors repository.
 *
 * @returns {import("./errors.mjs").ErrorsRepository} - Repository instance.
 */
function getErrorsRepository() {
  if (!cachedErrorsRepository) {
    cachedErrorsRepository = createErrorsRepository({
      tableName: requireEnv("ERRORS_TABLE_NAME"),
    });
  }
  return cachedErrorsRepository;
}

/**
 * Resolve the transform bucket/key pair for a given county.
 *
 * @param {{ countyName: string, transformPrefixUri: string | undefined }} params - Transform location configuration.
 * @returns {TransformLocation} - Derived transform bucket and key.
 */
function resolveTransformLocation({ countyName, transformPrefixUri }) {
  const prefixUri = transformPrefixUri;
  if (!prefixUri) {
    throw new Error("TRANSFORM_S3_PREFIX is required");
  }
  const { bucket, key } = parseS3Uri(prefixUri);
  const normalizedPrefix = key.replace(/\/$/, "");
  const transformKey = `${normalizedPrefix}/${countyName.toLowerCase()}.zip`;
  return { transformBucket: bucket, transformKey };
}

/**
 * Merge two zip archives, with seed files overwriting prepare files on conflict.
 *
 * @param {object} params - Merge parameters.
 * @param {string} params.prepareZipPath - Path to the prepare output zip.
 * @param {string} params.seedZipPath - Path to the seed output zip.
 * @param {string} params.tmpDir - Temporary working directory.
 * @param {StructuredLogger} params.log - Structured logger.
 * @returns {Promise<string>} - Path to the merged zip file.
 */
async function mergeArchives({ prepareZipPath, seedZipPath, tmpDir, log }) {
  log("info", "merge_archives_start", {
    operation: "merge_archives",
  });

  const mergeStart = Date.now();

  // Create temporary directories for extraction and merging
  const prepareExtractDir = path.join(tmpDir, "prepare_extract");
  const seedExtractDir = path.join(tmpDir, "seed_extract");
  const mergedDir = path.join(tmpDir, "merged");

  await fs.mkdir(prepareExtractDir, { recursive: true });
  await fs.mkdir(seedExtractDir, { recursive: true });
  await fs.mkdir(mergedDir, { recursive: true });

  try {
    // Extract prepare output zip
    log("info", "extract_prepare_zip", { operation: "extract_prepare" });
    const prepareZip = new AdmZip(await fs.readFile(prepareZipPath));
    prepareZip.extractAllTo(prepareExtractDir, true);

    // Extract seed output zip
    log("info", "extract_seed_zip", { operation: "extract_seed" });
    const seedZip = new AdmZip(await fs.readFile(seedZipPath));
    seedZip.extractAllTo(seedExtractDir, true);

    // Copy prepare files to merged directory
    log("info", "copy_prepare_files", { operation: "copy_prepare" });
    /**
     * @param {string} src
     * @param {string} dest
     */
    const copyRecursive = async (src, dest) => {
      const entries = await fs.readdir(src, { withFileTypes: true });
      for (const entry of entries) {
        const srcPath = path.join(src, entry.name);
        const destPath = path.join(dest, entry.name);
        if (entry.isDirectory()) {
          await fs.mkdir(destPath, { recursive: true });
          await copyRecursive(srcPath, destPath);
        } else {
          await fs.copyFile(srcPath, destPath);
        }
      }
    };

    await copyRecursive(prepareExtractDir, mergedDir);

    // Copy seed files to merged directory (overwriting conflicts)
    // Seed files are in data/ subdirectory but should be copied to root
    log("info", "copy_seed_files_overwrite", {
      operation: "copy_seed_overwrite",
    });
    const seedDataDir = path.join(seedExtractDir, "data");
    const seedDataExists = await fs
      .access(seedDataDir)
      .then(() => true)
      .catch(() => false);

    if (seedDataExists) {
      await copyRecursive(seedDataDir, mergedDir);
    } else {
      await copyRecursive(seedExtractDir, mergedDir);
    }

    // Create merged zip
    const mergedZipPath = path.join(tmpDir, "merged_input.zip");
    log("info", "create_merged_zip", { operation: "create_merged_zip" });
    const mergedZip = new AdmZip();
    mergedZip.addLocalFolder(mergedDir);
    await fs.writeFile(mergedZipPath, mergedZip.toBuffer());

    const mergeDuration = Date.now() - mergeStart;
    log("info", "merge_archives_complete", {
      operation: "merge_archives",
      duration_ms: mergeDuration,
      duration_seconds: (mergeDuration / 1000).toFixed(2),
    });

    return mergedZipPath;
  } finally {
    // Clean up extraction directories
    try {
      await fs.rm(prepareExtractDir, { recursive: true, force: true });
      await fs.rm(seedExtractDir, { recursive: true, force: true });
      await fs.rm(mergedDir, { recursive: true, force: true });
    } catch (cleanupError) {
      log("error", "merge_cleanup_failed", {
        error: String(cleanupError),
      });
    }
  }
}

/**
 * Extract the county name from the seed metadata archive.
 *
 * @param {string} seedZipPath - Absolute path to the seed output zip file.
 * @param {string} tmpDir - Temporary working directory used for extraction.
 * @returns {Promise<string>} - County jurisdiction name found in metadata.
 */
async function extractCountyName(seedZipPath, tmpDir) {
  const seedZipExtractDir = path.join(tmpDir, "seed_zip_extract");
  await fs.mkdir(seedZipExtractDir, { recursive: true });

  const seedZip = new AdmZip(await fs.readFile(seedZipPath));
  seedZip.extractAllTo(seedZipExtractDir, true);

  const countyMetadataPath = path.join(
    seedZipExtractDir,
    "data",
    "unnormalized_address.json",
  );
  const countyMetadataRaw = await fs.readFile(countyMetadataPath, "utf8");
  /** @type {{ county_jurisdiction?: string }} */
  const countyMetadata = JSON.parse(countyMetadataRaw);
  const countyName = countyMetadata.county_jurisdiction;
  if (!countyName) {
    throw new Error("county_jurisdiction missing in metadata");
  }
  return countyName;
}

/**
 * Run transform and validation pipelines for the county archive.
 *
 * @param {object} params - Execution parameters.
 * @param {string} params.countyZipLocal - Local path to county input zip.
 * @param {string} params.tmpDir - Temporary working directory.
 * @param {string} params.scriptsZipPath - Local path to transform scripts zip.
 * @param {StructuredLogger} params.log - Structured logger.
 * @returns {Promise<string>} - Path to the validated county output zip.
 */
async function runTransformAndValidation({
  countyZipLocal,
  tmpDir,
  scriptsZipPath,
  log,
}) {
  const countyOut = path.join(tmpDir, "county_output.zip");

  log("info", "transform_start", {
    operation: "transform",
    includes_fact_sheet_generation: true,
  });
  const transformStart = Date.now();
  let transformResult;
  try {
    transformResult = await transform({
      inputZip: countyZipLocal,
      outputZip: countyOut,
      scriptsZip: scriptsZipPath,
      cwd: tmpDir,
    });
  } catch (transformError) {
    const transformDurationError = Date.now() - transformStart;
    log("error", "transform_failed", {
      operation: "transform",
      step: "transform",
      duration_ms: transformDurationError,
      duration_seconds: (transformDurationError / 1000).toFixed(2),
      includes_fact_sheet_generation: true,
      error: String(transformError),
    });
    throw transformError;
  }
  const transformDuration = Date.now() - transformStart;
  if (!transformResult.success) {
    log("error", "transform_failed", {
      operation: "transform",
      step: "transform",
      duration_ms: transformDuration,
      duration_seconds: (transformDuration / 1000).toFixed(2),
      includes_fact_sheet_generation: true,
      error: transformResult.error,
      scriptFailures: transformResult.scriptFailure,
    });
    throw new Error(transformResult.error);
  }
  log("info", "transform_complete", {
    operation: "transform",
    duration_ms: transformDuration,
    duration_seconds: (transformDuration / 1000).toFixed(2),
    includes_fact_sheet_generation: true,
  });

  log("info", "validation_start", { operation: "validation" });
  const validationStart = Date.now();
  const validationResult = await validate({ input: countyOut, cwd: tmpDir });
  const validationDuration = Date.now() - validationStart;
  log("info", "validation_complete", {
    operation: "validation",
    duration_ms: validationDuration,
    duration_seconds: (validationDuration / 1000).toFixed(2),
  });
  if (!validationResult.success) {
    const validationError = new Error(validationResult.error);
    validationError.name = "ValidationFailure";
    throw validationError;
  }

  return countyOut;
}

/**
 * @typedef {object} ValidationFailureResult
 * @property {boolean} saved - Whether errors were successfully saved to DynamoDB.
 */

/**
 * Handle transform validation errors by logging and uploading submit_errors.csv.
 *
 * @param {object} params - Failure handling context.
 * @param {string} params.tmpDir - Temporary working directory.
 * @param {StructuredLogger} params.log - Structured logger.
 * @param {string} params.error - Validation error message.
 * @param {string} params.outputBaseUri - Base S3 URI for generated outputs.
 * @param {string | undefined} params.inputKey - Original input S3 key.
 * @param {string} params.executionId - Identifier for the failed execution.
 * @param {string} params.county - County identifier.
 * @param {import("./errors.mjs").ExecutionSource} params.source - Minimal source description.
 * @param {string} params.occurredAt - ISO timestamp of the failure.
 * @param {string} params.preparedS3Uri - S3 location of the output of the prepare step.
 * @returns {Promise<ValidationFailureResult>} - Returns { saved: true } if DynamoDB save succeeds, throws if save fails or no errors to save.
 */
async function handleValidationFailure({
  tmpDir,
  log,
  error,
  outputBaseUri,
  inputKey,
  executionId,
  county,
  source,
  occurredAt,
  preparedS3Uri,
}) {
  const submitErrorsPath = path.join(tmpDir, "submit_errors.csv");
  let submitErrorsS3Uri = null;
  let submitErrors = [];

  try {
    submitErrors = await csvToJson(submitErrorsPath);

    try {
      const submitErrorsCsv = await fs.readFile(submitErrorsPath);
      const resolvedInputKey = path.posix.basename(inputKey || "input.csv");
      const runPrefix = `${outputBaseUri.replace(/\/$/, "")}/${resolvedInputKey}`;
      const errorFileKey = `${runPrefix.replace(/^s3:\/\//, "")}/submit_errors.csv`;
      const [errorBucket, ...errorParts] = errorFileKey.split("/");
      await s3.send(
        new PutObjectCommand({
          Bucket: errorBucket,
          Key: errorParts.join("/"),
          Body: submitErrorsCsv,
        }),
      );
      submitErrorsS3Uri = `s3://${errorBucket}/${errorParts.join("/")}`;
    } catch (uploadError) {
      log("error", "submit_errors_upload_failed", {
        error: String(uploadError),
      });
    }

    log("error", "validation_failed", {
      step: "validate",
      error,
      submit_errors: submitErrors,
      submit_errors_s3_uri: submitErrorsS3Uri,
    });

    if (submitErrors.length > 0) {
      try {
        await getErrorsRepository().saveFailedExecution({
          executionId,
          county,
          errors: submitErrors.map((row) => ({
            errorMessage: row.error_message ?? undefined,
            error_message: row.error_message ?? undefined,
            errorPath: row.error_path ?? undefined,
            error_path: row.error_path ?? undefined,
          })),
          source,
          errorsS3Uri: submitErrorsS3Uri ?? undefined,
          failureMessage: error,
          occurredAt,
          preparedS3Uri,
        });
        // DynamoDB save succeeded, return success indicator
        return { saved: true };
      } catch (repoError) {
        log("error", "errors_repository_save_failed", {
          error: String(repoError),
          execution_id: executionId,
        });
        // DynamoDB save failed, throw the error
        const errorMessage = submitErrorsS3Uri
          ? `Validation failed. Submit errors CSV: ${submitErrorsS3Uri}. DynamoDB save failed: ${String(repoError)}`
          : `Validation failed. DynamoDB save failed: ${String(repoError)}`;
        const errorToThrow = new Error(errorMessage);
        errorToThrow.name = "ValidationFailure";
        throw errorToThrow;
      }
    }
  } catch (csvError) {
    log("error", "validation_failed", {
      step: "validate",
      error,
      submit_errors_read_error: String(csvError),
    });
  }

  // No submitErrors to save, throw error
  const errorMessage = submitErrorsS3Uri
    ? `Validation failed. Submit errors CSV: ${submitErrorsS3Uri}`
    : "Validation failed";
  const errorToThrow = new Error(errorMessage);
  errorToThrow.name = "ValidationFailure";

  throw errorToThrow;
}

/**
 * @typedef {object} MirrorValidationResult
 * @property {object} [comparison] - Comparison result containing entity type comparisons.
 */

/**
 * Handle mirror validation errors by extracting unmatched sources, generating CSV, and saving to DynamoDB.
 *
 * @param {object} params - Failure handling context.
 * @param {StructuredLogger} params.log - Structured logger.
 * @param {string} params.error - Validation error message.
 * @param {string} params.executionId - Identifier for the failed execution.
 * @param {string} params.county - County identifier.
 * @param {import("./errors.mjs").ExecutionSource} params.source - Minimal source description.
 * @param {string} params.occurredAt - ISO timestamp of the failure.
 * @param {string} params.preparedS3Uri - S3 location of the output of the prepare step.
 * @param {string} params.errorsCsvPath - Optional path to pre-generated errors CSV.
 * @param {string} params.errorsS3Uri - S3 URI of saved errors CSV.
 * @returns {Promise<ValidationFailureResult>} - Returns { saved: true } if DynamoDB save succeeds, throws if save fails or no errors to save.
 */
async function handleMirrorValidationFailure({
  log,
  error,
  executionId,
  county,
  source,
  occurredAt,
  preparedS3Uri,
  errorsCsvPath,
  errorsS3Uri
}) {
  let mvlErrorsCsvPath = errorsCsvPath;
  let mvlErrorsS3Uri = errorsS3Uri;
  let mvlErrors = [];

  try {
    if (mvlErrorsCsvPath) {
      try {
        mvlErrors = await csvToJson(mvlErrorsCsvPath);

        log("error", "mirror_validation_failed", {
          step: "mirror_validate",
          error,
          mvl_errors: mvlErrors,
          mvl_errors_s3_uri: mvlErrorsS3Uri,
        });

        if (mvlErrors.length > 0) {
          try {
            await getErrorsRepository().saveFailedExecution({
              executionId,
              county,
              errors: mvlErrors.map((row) => ({
                errorMessage: row.error_message ?? undefined,
                error_message: row.error_message ?? undefined,
                errorPath: row.error_path ?? undefined,
                error_path: row.error_path ?? undefined,
              })),
              source,
              errorsS3Uri: mvlErrorsS3Uri ?? undefined,
              failureMessage: error,
              occurredAt,
              preparedS3Uri,
            });
            // DynamoDB save succeeded, return success indicator
            return { saved: true };
          } catch (repoError) {
            log("error", "errors_repository_save_failed", {
              error: String(repoError),
              execution_id: executionId,
            });
            // DynamoDB save failed, throw the error
            const errorMessage = mvlErrorsS3Uri
              ? `Mirror validation failed. Submit errors CSV: ${mvlErrorsS3Uri}. DynamoDB save failed: ${String(repoError)}`
              : `Mirror validation failed. DynamoDB save failed: ${String(repoError)}`;
            const errorToThrow = new Error(errorMessage);
            errorToThrow.name = "MirrorValidationFailure";
            throw errorToThrow;
          }
        }
      } catch (csvError) {
        log("error", "mirror_validation_failed", {
          step: "mirror_validate",
          error,
          mvl_errors_read_error: String(csvError),
        });
      }
    }
  } catch (extractError) {
    log("error", "mirror_validation_failed", {
      step: "mirror_validate",
      error,
      extraction_error: String(extractError),
    });
  }

  // No mvlErrors to save, throw error
  const errorMessage = mvlErrorsS3Uri
    ? `Mirror validation failed. Submit errors CSV: ${mvlErrorsS3Uri}`
    : "Mirror validation failed";
  const errorToThrow = new Error(errorMessage);
  errorToThrow.name = "MirrorValidationFailure";

  throw errorToThrow;
}

/**
 * Generate seed and county hash artifacts.
 *
 * @param {object} params - Hash generation context.
 * @param {string} params.seedZipLocal - Path to the seed zip archive.
 * @param {string} params.countyOutZip - Path to the validated county output zip.
 * @param {string} params.tmpDir - Temporary working directory.
 * @param {StructuredLogger} params.log - Structured logger.
 * @returns {Promise<{ propertyCid: string | undefined, seedHashCsv: string, countyHashCsv: string, seedHashZip: ZipFile, countyHashZip: ZipFile }>}
 */
async function generateHashArtifacts({
  seedZipLocal,
  countyOutZip,
  tmpDir,
  log,
}) {
  const countyHashZip = /** @type {ZipFile} */ (
    path.join(tmpDir, "county_hash.zip")
  );
  const countyHashCsv = path.join(tmpDir, "county_hash.csv");
  const seedHashZip = /** @type {ZipFile} */ (
    path.join(tmpDir, "seed_hash.zip")
  );
  const seedHashCsv = path.join(tmpDir, "seed_hash.csv");

  log("info", "hash_seed_start", { operation: "seed_hash" });
  const seedHashStart = Date.now();
  const seedHashResult = await hash({
    input: seedZipLocal,
    outputZip: seedHashZip,
    outputCsv: seedHashCsv,
    cwd: tmpDir,
  });
  const seedHashDuration = Date.now() - seedHashStart;
  log("info", "hash_seed_complete", {
    operation: "seed_hash",
    duration_ms: seedHashDuration,
    duration_seconds: (seedHashDuration / 1000).toFixed(2),
  });
  if (!seedHashResult.success) {
    log("error", "hash_failed", {
      step: "hash",
      operation: "seed_hash",
      error: seedHashResult.error,
    });
    throw new Error(`Seed hash failed: ${seedHashResult.error}`);
  }

  const csv = await fs.readFile(seedHashCsv, "utf8");
  const records = parse(csv, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });
  const propertyCid = records[0]?.propertyCid;

  log("info", "hash_county_start", { operation: "county_hash" });
  const countyHashStart = Date.now();
  const countyHashResult = await hash({
    input: countyOutZip,
    outputZip: countyHashZip,
    outputCsv: countyHashCsv,
    propertyCid: propertyCid,
    cwd: tmpDir,
  });
  const countyHashDuration = Date.now() - countyHashStart;
  log("info", "hash_county_complete", {
    operation: "county_hash",
    duration_ms: countyHashDuration,
    duration_seconds: (countyHashDuration / 1000).toFixed(2),
  });
  if (!countyHashResult.success) {
    log("error", "hash_failed", {
      step: "hash",
      operation: "county_hash",
      error: countyHashResult.error,
    });
    throw new Error(`County hash failed: ${countyHashResult.error}`);
  }

  return {
    propertyCid,
    seedHashCsv,
    countyHashCsv,
    seedHashZip,
    countyHashZip,
  };
}

/**
 * Upload hash archives to IPFS-compatible endpoint by combining them into a single archive.
 * This function extracts each zip file from filesForIpfs into separate subdirectories,
 * creates a combined zip archive containing all extracted contents, uploads it to IPFS,
 * and cleans up temporary files.
 *
 * @param {object} params - Upload context.
 * @param {ZipFile[]} params.filesForIpfs - Archive paths that will be extracted and combined for IPFS upload.
 * @param {string} params.tmpDir - Temporary working directory.
 * @param {(level: "info"|"error"|"debug", msg: string, details?: Record<string, unknown>) => void} params.log - Structured logger.
 * @param {string} params.pinataJwt - Pinata authentication token.
 * @returns {Promise<void>}
 */
async function uploadHashOutputs({ filesForIpfs, tmpDir, log, pinataJwt }) {
  log("info", "ipfs_upload_start", { operation: "ipfs_upload" });
  const uploadStart = Date.now();

  // 1. Create a temp directory for unzipping
  const extractTempDir = path.join(tmpDir, "ipfs_upload_temp");
  await fs.mkdir(extractTempDir, { recursive: true });

  try {
    // 2. Unzip each file to a separate subdirectory
    for (const filePath of filesForIpfs) {
      const fileName = path.basename(filePath, ".zip"); // Remove .zip extension
      const extractSubDir = path.join(extractTempDir, fileName);
      await fs.mkdir(extractSubDir, { recursive: true });

      log("info", "ipfs_extract_file_start", { file: fileName });
      const zip = new AdmZip(await fs.readFile(filePath));
      zip.extractAllTo(extractSubDir, true);
      log("info", "ipfs_extract_file_complete", { file: fileName });
    }

    // 3. Zip the entire temp directory
    const combinedZipPath = path.join(tmpDir, "combined_ipfs_upload.zip");
    const combinedZip = new AdmZip();
    combinedZip.addLocalFolder(extractTempDir);
    await fs.writeFile(combinedZipPath, combinedZip.toBuffer());

    // 4. Upload the combined zip
    log("info", "ipfs_upload_combined_start", {});
    const uploadResult = await upload({
      input: combinedZipPath,
      pinataJwt,
      cwd: tmpDir,
    });
    const totalUploadDuration = Date.now() - uploadStart;
    log("info", "ipfs_upload_complete", {
      operation: "ipfs_upload_total",
      duration_ms: totalUploadDuration,
      duration_seconds: (totalUploadDuration / 1000).toFixed(2),
    });

    if (!uploadResult.success) {
      log("error", "upload_failed", {
        step: "upload",
        operation: "ipfs_upload",
        erorrMessage: uploadResult.errorMessage,
        errors: uploadResult.errors,
      });
      throw new Error(`Upload failed: ${uploadResult.errorMessage}`);
    }
  } finally {
    try {
      await fs.rm(extractTempDir, { recursive: true, force: true });
    } catch (cleanupError) {
      log("error", "ipfs_cleanup_failed", {
        error: String(cleanupError),
      });
    }
  }
}

/**
 * Combine seed and county hash CSVs and parse into transaction items.
 *
 * @param {object} params - Combination context.
 * @param {string} params.seedHashCsv - Path to seed hash CSV file.
 * @param {string} params.countyHashCsv - Path to county hash CSV file.
 * @param {string} params.tmpDir - Temporary working directory.
 * @returns {Promise<CsvRecord[]>}
 */
async function combineAndParseTransactions({
  seedHashCsv,
  countyHashCsv,
  tmpDir,
}) {
  const combinedCsvPath = await combineCsv(seedHashCsv, countyHashCsv, tmpDir);
  const combinedCsvContent = await fs.readFile(combinedCsvPath, "utf8");
  const transactionItems = await parse(combinedCsvContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });
  return transactionItems;
}

/**
 * Extract all unmatched source values from mirror validation comparison result.
 *
 * @param {Record<string, { unmatchedFromA?: unknown[] }>} comparison - Comparison result from mirrorValidate containing entity type comparisons.
 * @returns {string[]} - Array of unique source strings extracted from unmatchedFromA arrays.
 */
function extractUnmatchedSources(comparison) {
  /** @type {Set<string>} */
  const sources = new Set();

  const entityTypes = ["QUANTITY", "DATE", "ORGANIZATION", "LOCATION"];

  for (const entityType of entityTypes) {
    const entityComparison = comparison[entityType];
    if (!entityComparison || !entityComparison.unmatchedFromA) {
      continue;
    }

    const unmatched = entityComparison.unmatchedFromA;
    if (Array.isArray(unmatched)) {
      for (const item of unmatched) {
        // Check if item is EntityWithSource (has source property)
        if (typeof item === "object" && item !== null && "source" in item) {
          const source = item.source;
          if (typeof source === "string" && source.trim().length > 0) {
            sources.add(source.trim());
          }
        }
        // If it's a string[], skip it (no source available)
      }
    }
  }

  return Array.from(sources);
}

/**
 * Escape CSV field value by wrapping in quotes and escaping internal quotes.
 *
 * @param {string} value - Value to escape.
 * @returns {string} - Escaped CSV field value.
 */
function escapeCsvField(value) {
  // If value contains comma, quote, or newline, wrap in quotes and escape internal quotes
  if (value.includes(",") || value.includes('"') || value.includes("\n")) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

/**
 * Generate CSV file with mirror validation errors from unmatched sources.
 *
 * @param {string[]} sources - Array of source strings to create error entries for.
 * @param {string} tmpDir - Temporary directory where CSV will be created.
 * @param {string} dataGroupCid - CID of the datagroup, that caused the issue
 * @returns {Promise<string>} - Path to the generated CSV file.
 */
async function generateMirrorValidationErrorsCsv(
  sources,
  tmpDir,
  dataGroupCid,
) {
  const csvPath = path.join(tmpDir, "mvl_errors.csv");

  // CSV header
  const header = "error_path,error_message\n";
  const rows = sources.map(
    (source) =>
      `${escapeCsvField(source)},${escapeCsvField("Value from this selector is not mapped to the output")},${escapeCsvField(dataGroupCid)}`,
  );

  const csvContent = header + rows.join("\n");
  await fs.writeFile(csvPath, csvContent, "utf8");

  return csvPath;
}

/**
 * Run mirror validation to measure completeness of transformed data.
 *
 * @param {object} params - Validation context.
 * @param {string} params.countyZipLocal - Path to input zip (before transform).
 * @param {string} params.countyOutZip - Path to output zip (after transform).
 * @param {string} params.tmpDir - Temporary working directory.
 * @param {StructuredLogger} params.log - Structured logger.
 * @param {string} params.outputS3Uri - Output S3 URI to derive report location.
 * @returns {Promise<number>} - Global completeness metric (0-100).
 */
async function runMirrorValidation({
  countyZipLocal,
  countyOutZip,
  tmpDir,
  log,
  outputS3Uri,
}) {
  // Hardcoded static CSV S3 URI
  const staticCsvS3Uri =
    "s3://elephant-oracle-node-environmentbucket-lw51azqpha64/htmls-static-parts/collier-static-parts.csv";

  log("info", "mvl_download_static_csv_start", {
    operation: "mvl_download_static_csv",
  });

  // Download static CSV file
  const staticCsvLocal = path.join(tmpDir, "static-parts.csv");
  await downloadS3Object(parseS3Uri(staticCsvS3Uri), staticCsvLocal, log);

  log("info", "mvl_validation_start", { operation: "mirror_validation" });
  const mvlStart = Date.now();

  // Run mirrorValidate
  const mvlResult = await mirrorValidate({
    prepareZip: countyZipLocal,
    transformZip: countyOutZip,
    staticParts: staticCsvLocal,
    cwd: tmpDir,
  });

  const mvlDuration = Date.now() - mvlStart;
  log("info", "mvl_validation_complete", {
    operation: "mirror_validation",
    duration_ms: mvlDuration,
    duration_seconds: (mvlDuration / 1000).toFixed(2),
  });

  if (!mvlResult.success) {
    log("error", "mvl_validation_failed", {
      step: "mirror_validate",
      error: mvlResult.error,
    });
    const error = new Error(`Mirror validation failed: ${mvlResult.error}`);
    error.name = "MirrorValidationFailure";
    // @ts-ignore - Attach mvlResult to error for error handling
    error.mvlResult = mvlResult;
    throw error;
  }

  // Save report to S3
  log("info", "mvl_upload_report_start", { operation: "mvl_upload_report" });
  const reportJson = JSON.stringify(mvlResult, null, 2);

  // Derive report S3 location from output_s3_uri
  // e.g., s3://bucket/outputs/2029035U0000000000320U/output.zip -> s3://bucket/outputs/2029035U0000000000320U/mvl-report.json
  const outputDir = outputS3Uri.substring(0, outputS3Uri.lastIndexOf("/"));
  const reportS3Uri = `${outputDir}/mvl-report.json`;
  const { bucket: reportBucket, key: reportKey } = parseS3Uri(reportS3Uri);

  await s3.send(
    new PutObjectCommand({
      Bucket: reportBucket,
      Key: reportKey,
      Body: reportJson,
      ContentType: "application/json",
    }),
  );

  log("info", "mvl_upload_report_complete", {
    operation: "mvl_upload_report",
    report_s3_uri: reportS3Uri,
  });

  // Extract globalCompleteness from result
  const globalCompleteness = mvlResult.globalCompleteness ?? 0;

  log("info", "mvl_completeness_metric", {
    global_completeness: globalCompleteness,
  });

  // Check if completeness is below threshold
  if (globalCompleteness < 80.0) {
    const errorMsg = `Mirror validation failed: global completeness ${globalCompleteness.toFixed(2)}% is below threshold of 80.0%`;
    log("error", "mvl_completeness_below_threshold", {
      global_completeness: globalCompleteness,
      threshold: 80.0,
    });
    const error = new Error(errorMsg);
    error.name = "MirrorValidationFailure";
    // @ts-ignore - Attach mvlResult and errorsCsvPath to error for error handling
    error.mvlResult = mvlResult;
    throw error;
  }

  return globalCompleteness;
}

/**
 * @param {PostInput} event - { prepare: prepare result, seed_output_s3_uri: S3 URI of seed output }
 * @returns {Promise<PostOutput>}
 */
/** @type {(event: PostInput) => Promise<PostOutput>} */
export const handler = async (event) => {
  // Early setup and county extraction before logging
  if (!event.prepare.output_s3_uri)
    throw new Error("prepare.output_s3_uri missing");
  if (!event.seed_output_s3_uri) throw new Error("seed_output_s3_uri missing");
  const outputBase = process.env.OUTPUT_BASE_URI;
  if (!outputBase) throw new Error("OUTPUT_BASE_URI is required");

  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "post-"));
  const prepared = event.prepare.output_s3_uri;
  const seedZipS3 = event.seed_output_s3_uri;

  // Download seed zip early to extract county name
  const seedZipLocal = path.join(tmp, "seed_seed_output.zip");
  await downloadS3Object(
    parseS3Uri(seedZipS3),
    seedZipLocal,
    () => { }, // No logging yet
  );

  const countyName = await extractCountyName(seedZipLocal, tmp);

  /** @type {string} */
  const executionId = randomUUID();
  /** @type {import("./errors.mjs").ExecutionSource} */
  const sourceEvent = {
    s3Bucket: event?.s3?.bucket?.name,
    s3Key: event?.s3?.object?.key,
  };

  const base = {
    component: "post",
    at: new Date().toISOString(),
    county: countyName,
    execution_id: executionId,
  };

  /**
   * Emit structured log messages with consistent shape.
   *
   * @param {"info"|"error"|"debug"} level - Log severity.
   * @param {string} msg - Short message identifier.
   * @param {Record<string, unknown>} [details] - Additional structured data to include.
   */
  const log = (level, msg, details = {}) => {
    const entry = { ...base, level, msg, ...details };
    if (level === "error") {
      console.error(entry);
    } else {
      console.log(entry);
    }
  };

  log("info", "post_lambda_start", {
    input_s3_key: event?.s3?.object?.key,
    prepareSkipped: event.prepareSkipped || false,
  });

  try {
    // Determine the county input zip based on whether prepare was skipped
    let countyZipLocal;

    if (event.prepareSkipped) {
      // Prepare was skipped: merge existing prepare output with seed output
      log("info", "prepare_skipped_merging", {
        operation: "merge_prepare_seed",
      });

      const prepareZipLocal = path.join(tmp, "prepare_output.zip");
      await downloadS3Object(parseS3Uri(prepared), prepareZipLocal, log);

      // Merge the archives with seed files overwriting prepare files
      countyZipLocal = await mergeArchives({
        prepareZipPath: prepareZipLocal,
        seedZipPath: seedZipLocal,
        tmpDir: tmp,
        log,
      });
    } else {
      // Normal flow: use prepare output as-is
      countyZipLocal = path.join(tmp, "county_input.zip");
      await downloadS3Object(parseS3Uri(prepared), countyZipLocal, log);
    }

    const { transformBucket, transformKey } = resolveTransformLocation({
      countyName,
      transformPrefixUri: process.env.TRANSFORM_S3_PREFIX,
    });

    const { scriptsZipPath } =
      await transformScriptsManager.ensureScriptsForCounty({
        countyName,
        transformBucket,
        transformKey,
        log,
      });

    let countyOut;
    try {
      countyOut = await runTransformAndValidation({
        countyZipLocal,
        tmpDir: tmp,
        scriptsZipPath,
        log,
      });
    } catch (runError) {
      if (runError instanceof Error && runError.name === "ValidationFailure") {
        // Check if we should save errors or throw immediately
        const saveErrorsOnValidationFailure =
          event.saveErrorsOnValidationFailure !== false;

        if (!saveErrorsOnValidationFailure) {
          // Flag is false: throw error immediately without saving
          throw runError;
        }

        // Flag is true (default): save errors to DynamoDB
        try {
          const result = await handleValidationFailure({
            tmpDir: tmp,
            log,
            error: runError.message,
            outputBaseUri: outputBase,
            inputKey: event?.s3?.object?.key,
            executionId,
            county: countyName,
            source: sourceEvent,
            occurredAt: base.at,
            preparedS3Uri: event?.prepare?.output_s3_uri,
          });
          // Errors were successfully saved to DynamoDB, return success with empty transactionItems
          if (result.saved) {
            log("info", "validation_failed_but_saved", {
              execution_id: executionId,
              county: countyName,
            });
            const totalOperationDuration = Date.now() - Date.parse(base.at);
            log("info", "post_lambda_complete", {
              operation: "post_lambda_total",
              duration_ms: totalOperationDuration,
              duration_seconds: (totalOperationDuration / 1000).toFixed(2),
              transaction_items_count: 0,
            });
            return { status: "success", transactionItems: [] };
          }
        } catch (saveError) {
          // DynamoDB save failed, re-throw the error
          throw saveError;
        }
      }
      // Re-throw if not a ValidationFailure or if handleValidationFailure didn't return saved: true
      throw runError;
    }

    // Run mirror validation
    let mvlMetric;
    try {
      mvlMetric = await runMirrorValidation({
        countyZipLocal,
        countyOutZip: countyOut,
        tmpDir: tmp,
        log,
        outputS3Uri: event.prepare.output_s3_uri,
      });
    } catch (mvlError) {
      if (
        mvlError instanceof Error &&
        mvlError.name === "MirrorValidationFailure"
      ) {
        // @ts-ignore - Access mvlResult and errorsCsvPath from error object
        const mvlResult = mvlError.mvlResult;
        // @ts-ignore

        // Always generate and upload errors CSV if available, so S3 URI can be extracted from error message
        let errorsS3Uri = null;
        let errorsCsvPath;
        try {
          const unmatchedSources = extractUnmatchedSources(
            /** @type {Record<string, { unmatchedFromA?: unknown[] }>} */(
              /** @type {unknown} */ (mvlResult.comparison)
            ),
          );
          errorsCsvPath = await generateMirrorValidationErrorsCsv(
            unmatchedSources,
            tmp,
            "bafkreigsipwhkwrboi73b3xvn4tjwd26pqyzz5zmyxvbnrgeb2qbq2bz34", // TODO: Dynamically fetch depending on the currently processed data group
          );
          console.log(`Errors csv path: ${errorsCsvPath}`)

          try {
            const errorsCsv = await fs.readFile(errorsCsvPath);
            const resolvedInputKey = path.posix.basename(
              event?.s3?.object?.key || "input.csv",
            );
            const runPrefix = `${outputBase.replace(/\/$/, "")}/${resolvedInputKey}`;
            const errorFileKey = `${runPrefix.replace(/^s3:\/\//, "")}/mvl_errors.csv`;
            const [errorBucket, ...errorParts] = errorFileKey.split("/");
            await s3.send(
              new PutObjectCommand({
                Bucket: errorBucket,
                Key: errorParts.join("/"),
                Body: errorsCsv,
              }),
            );
            errorsS3Uri = `s3://${errorBucket}/${errorParts.join("/")}`;
          } catch (uploadError) {
            log("error", "mvl_errors_upload_failed", {
              error: String(uploadError),
            });
            throw uploadError;
          }
        } catch (csvGenError) {
          log("error", "mvl_errors_csv_generation_failed", {
            error: String(csvGenError),
          });
          throw csvGenError;
        }

        // Check if we should save errors or throw immediately
        const saveErrorsOnValidationFailure =
          event.saveErrorsOnValidationFailure !== false;

        if (!saveErrorsOnValidationFailure) {
          // Flag is false: throw error immediately with S3 URI in message if available
          let errorMessage = mvlError.message;
          if (errorsS3Uri) {
            // Check if message already contains "Submit errors CSV:" pattern
            const existingPattern = /Submit errors CSV:\s*(s3:\/\/[^\s]+)/;
            if (existingPattern.test(errorMessage)) {
              // Replace existing S3 URI with the new one
              errorMessage = errorMessage.replace(
                existingPattern,
                `Submit errors CSV: ${errorsS3Uri}`,
              );
            } else {
              // Append S3 URI if not already present
              errorMessage = `${errorMessage}. Submit errors CSV: ${errorsS3Uri}`;
            }
          }
          const errorToThrow = new Error(errorMessage);
          errorToThrow.name = "MirrorValidationFailure";
          throw errorToThrow;
        }

        // Flag is true (default): save errors to DynamoDB
        try {
          const result = await handleMirrorValidationFailure({
            log,
            error: mvlError.message,
            executionId,
            county: countyName,
            source: sourceEvent,
            occurredAt: base.at,
            preparedS3Uri: event?.prepare?.output_s3_uri,
            errorsCsvPath,
            errorsS3Uri
          });
          // Errors were successfully saved to DynamoDB, return success with empty transactionItems
          if (result.saved) {
            log("info", "mirror_validation_failed_but_saved", {
              execution_id: executionId,
              county: countyName,
            });
            const totalOperationDuration = Date.now() - Date.parse(base.at);
            log("info", "post_lambda_complete", {
              operation: "post_lambda_total",
              duration_ms: totalOperationDuration,
              duration_seconds: (totalOperationDuration / 1000).toFixed(2),
              transaction_items_count: 0,
            });
            return { status: "success", transactionItems: [] };
          }
        } catch (saveError) {
          // DynamoDB save failed, re-throw the error
          throw saveError;
        }
      }
      // Re-throw if not a MirrorValidationFailure or if handleMirrorValidationFailure didn't return saved: true
      throw mvlError;
    }

    const {
      propertyCid,
      seedHashCsv,
      countyHashCsv,
      seedHashZip,
      countyHashZip,
    } = await generateHashArtifacts({
      seedZipLocal,
      countyOutZip: countyOut,
      tmpDir: tmp,
      log,
    });

    if (!propertyCid) {
      log("debug", "property_cid_missing", {});
    }

    const transactionItems = await combineAndParseTransactions({
      seedHashCsv,
      countyHashCsv,
      tmpDir: tmp,
    });
    await uploadHashOutputs({
      filesForIpfs: [seedHashZip, countyHashZip],
      tmpDir: tmp,
      log,
      pinataJwt: requireEnv("ELEPHANT_PINATA_JWT"),
    });

    const totalOperationDuration = Date.now() - Date.parse(base.at);
    log("info", "post_lambda_complete", {
      operation: "post_lambda_total",
      duration_ms: totalOperationDuration,
      duration_seconds: (totalOperationDuration / 1000).toFixed(2),
      transaction_items_count: transactionItems.length,
      mvl_metric: mvlMetric,
    });

    return { status: "success", transactionItems, mvlMetric };
  } catch (err) {
    log("error", "post_lambda_failed", {
      step: "unknown",
      error: String(err),
    });
    throw err;
  } finally {
    try {
      if (tmp) await fs.rm(tmp, { recursive: true, force: true });
    } catch { }
  }
};
