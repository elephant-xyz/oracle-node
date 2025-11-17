import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { promises as fs } from "fs";
import path from "path";
import os from "os";
import { randomUUID } from "crypto";
import { transform, validate, hash, upload } from "@elephant-xyz/cli/lib";
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
 * @property {string} transformedOutputS3Uri - S3 URI of the transformed output zip
 * @property {string} preparedInputS3Uri - S3 URI of the prepared input zip (for MVL)
 * @property {string} executionId - Execution identifier
 * @property {string} county - County name
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
    //@ts-ignore
    validationError.errorPath = path.join(tmpDir, "submit_errors.csv");
    throw validationError;
  }

  return countyOut;
}

/**
 * @typedef {object} ValidationFailureResult
 * @property {boolean} saved - Whether errors were successfully saved to DynamoDB.
 */

/**
 * Saves validation errors to S3.
 * @param {Object} options - Options object.
 * @param {string} options.submitErrorsPath - Path to the submit errors CSV file.
 * @param {string} options.inputKey - Input key.
 * @param {string} options.outputBaseUri - Output base URI.
 * @param {StructuredLogger} options.log - Structured logger used for diagnostics.
 * @returns {Promise<string>} - Submit errors S3 URI.
 */
async function saveValidationErrorToS3({
  submitErrorsPath,
  inputKey,
  outputBaseUri,
  log,
}) {
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
    const submitErrorsS3Uri = `s3://${errorBucket}/${errorParts.join("/")}`;
    return submitErrorsS3Uri;
  } catch (uploadError) {
    log("error", "submit_errors_upload_failed", {
      error: String(uploadError),
    });
    throw uploadError;
  }
}

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
  let submitErrorsS3Uri;
  let submitErrors = [];

  try {
    submitErrors = await csvToJson(submitErrorsPath);

    // Save errors to S3 using the extracted function
    submitErrorsS3Uri = await saveValidationErrorToS3({
      submitErrorsPath,
      inputKey,
      outputBaseUri,
      log,
    });

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
    () => {}, // No logging yet
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

        // @ts-ignore
        const errorPath = runError.errorPath;

        const errorsS3Uri = await saveValidationErrorToS3({
          submitErrorsPath: errorPath,
          log,
          inputKey: event?.s3?.object?.key,
          outputBaseUri: outputBase,
        });
        if (!saveErrorsOnValidationFailure) {
          // Flag is false: throw error immediately without saving
          runError.message = `${runError.message} Submit errors csv: ${errorsS3Uri}`;
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
            // Return with empty items and null S3 URIs to signal validation failure
            return {
              status: "success",
              transactionItems: [],
              transformedOutputS3Uri: null,
              preparedInputS3Uri: prepared,
              executionId,
              county: countyName,
            };
          }
        } catch (saveError) {
          // DynamoDB save failed, re-throw the error
          throw saveError;
        }
      }
      // Re-throw if not a ValidationFailure or if handleValidationFailure didn't return saved: true
      throw runError;
    }

    // Upload transformed output to S3 for mirror validation
    log("info", "upload_transformed_output_start", {
      operation: "upload_transformed_output",
    });
    const resolvedInputKey = path.posix.basename(
      event?.s3?.object?.key || "input.csv",
    );
    const runPrefix = `${outputBase.replace(/\/$/, "")}/${resolvedInputKey}`;
    const transformedOutputKey = `${runPrefix.replace(/^s3:\/\//, "")}/transformed_output.zip`;
    const [transformedBucket, ...transformedParts] =
      transformedOutputKey.split("/");
    const transformedOutputZipContent = await fs.readFile(countyOut);
    await s3.send(
      new PutObjectCommand({
        Bucket: transformedBucket,
        Key: transformedParts.join("/"),
        Body: transformedOutputZipContent,
      }),
    );
    const transformedOutputS3Uri = `s3://${transformedBucket}/${transformedParts.join("/")}`;
    log("info", "upload_transformed_output_complete", {
      operation: "upload_transformed_output",
      transformed_output_s3_uri: transformedOutputS3Uri,
    });

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
      transformed_output_s3_uri: transformedOutputS3Uri,
    });

    return {
      status: "success",
      transactionItems,
      transformedOutputS3Uri,
      preparedInputS3Uri: prepared,
      executionId,
      county: countyName,
    };
  } catch (err) {
    log("error", "post_lambda_failed", {
      step: "unknown",
      error: String(err),
    });
    throw err;
  } finally {
    try {
      if (tmp) await fs.rm(tmp, { recursive: true, force: true });
    } catch {}
  }
};
