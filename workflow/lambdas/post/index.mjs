import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { promises as fs } from "fs";
import path from "path";
import os from "os";
import { transform, validate, hash, upload } from "@elephant-xyz/cli/lib";
import { parse } from "csv-parse/sync";
import AdmZip from "adm-zip";
import { createTransformScriptsManager } from "./scripts-manager.mjs";

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
 * @typedef {object} PostOutput
 * @property {string} status
 * @property {CsvRecord[]} transactionItems
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

/**
 * Convert CSV file to JSON array
 * @param {string} csvPath - Path to CSV file
 * @returns {Promise<Object[]>} - Array of objects representing CSV rows
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
    throw new Error("Bad S3 URI");
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

/**
 * Download prepared county and seed archives to local temporary files.
 *
 * @param {DownloadInputsParams} params - Download configuration.
 * @returns {Promise<DownloadInputsResult>} - Paths to downloaded archives.
 */
async function downloadInputArchives({ prepareUri, seedUri, tmpDir, log }) {
  const { bucket: prepBucket, key: prepKey } = parseS3Uri(prepareUri);
  const { bucket: seedBucket, key: seedKey } = parseS3Uri(seedUri);

  const countyZipLocal = path.join(tmpDir, "county_input.zip");
  const seedZipLocal = path.join(tmpDir, "seed_seed_output.zip");

  log("info", "download_prepared_zip", { bucket: prepBucket, key: prepKey });
  await downloadS3Object(
    { bucket: prepBucket, key: prepKey },
    countyZipLocal,
    log,
  );

  log("info", "download_seed_zip", { bucket: seedBucket, key: seedKey });
  await downloadS3Object(
    { bucket: seedBucket, key: seedKey },
    seedZipLocal,
    log,
  );

  return { countyZipLocal, seedZipLocal };
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
 * Build the submission S3 destination for the combined hash CSV.
 *
 * @param {{ outputBaseUri: string, inputS3ObjectKey: string | undefined }} params - Submission location context.
 * @returns {{ Bucket: string, Key: string }} - Destination bucket/key pair.
 */
function buildSubmissionLocation({ outputBaseUri, inputS3ObjectKey }) {
  const normalizedBase = outputBaseUri.replace(/\/$/, "");
  const inputKey = path.posix.basename(inputS3ObjectKey || "input.csv");
  const submissionUri = `${normalizedBase}/${inputKey}/submission_ready.csv`;
  const { bucket, key } = parseS3Uri(submissionUri);
  return { Bucket: bucket, Key: key };
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
 * Handle transform validation errors by logging and uploading submit_errors.csv.
 *
 * @param {object} params - Failure handling context.
 * @param {string} params.tmpDir - Temporary working directory.
 * @param {StructuredLogger} params.log - Structured logger.
 * @param {string} params.error - Validation error message.
 * @param {string} params.outputBaseUri - Base S3 URI for generated outputs.
 * @param {string | undefined} params.inputKey - Original input S3 key.
 * @returns {Promise<never>} - Throws after handling.
 */
async function handleValidationFailure({
  tmpDir,
  log,
  error,
  outputBaseUri,
  inputKey,
}) {
  const submitErrorsPath = path.join(tmpDir, "submit_errors.csv");
  let submitErrorsS3Uri = null;

  try {
    const submitErrors = await csvToJson(submitErrorsPath);

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
  } catch (csvError) {
    log("error", "validation_failed", {
      step: "validate",
      error,
      submit_errors_read_error: String(csvError),
    });
  }

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
 * @returns {Promise<{ propertyCid: string | undefined, seedHashCsv: string, countyHashCsv: string, seedHashZip: string, countyHashZip: string }>}
 */
async function generateHashArtifacts({
  seedZipLocal,
  countyOutZip,
  tmpDir,
  log,
}) {
  const countyHashZip = path.join(tmpDir, "county_hash.zip");
  const countyHashCsv = path.join(tmpDir, "county_hash.csv");
  const seedHashZip = path.join(tmpDir, "seed_hash.zip");
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
 * Upload hash archives to IPFS-compatible endpoint and submission CSV to S3.
 *
 * @param {object} params - Upload context.
 * @param {string[]} params.filesForIpfs - Paths to archives uploaded via IPFS helper.
 * @param {string} params.tmpDir - Temporary working directory.
 * @param {(level: "info"|"error"|"debug", msg: string, details?: Record<string, unknown>) => void} params.log - Structured logger.
 * @param {string} params.pinataJwt - Pinata authentication token.
 * @param {string} params.combinedCsvPath - Path to the combined CSV file.
 * @param {{ Bucket: string, Key: string }} params.submissionS3Location - Destination S3 details for submission CSV.
 * @returns {Promise<void>}
 */
async function uploadHashOutputs({
  filesForIpfs,
  tmpDir,
  log,
  pinataJwt,
  combinedCsvPath,
  submissionS3Location,
}) {
  log("info", "ipfs_upload_start", { operation: "ipfs_upload" });
  const uploadStart = Date.now();
  await Promise.all(
    filesForIpfs.map(async (filePath) => {
      const fileName = path.basename(filePath);
      log("info", "ipfs_upload_file_start", { file: fileName });
      const fileUploadStart = Date.now();
      const uploadResult = await upload({
        input: filePath,
        pinataJwt,
        cwd: tmpDir,
      });
      const fileUploadDuration = Date.now() - fileUploadStart;
      log("info", "ipfs_upload_file_complete", {
        file: fileName,
        duration_ms: fileUploadDuration,
        duration_seconds: (fileUploadDuration / 1000).toFixed(2),
      });
      if (!uploadResult.success) {
        log("error", "upload_failed", {
          step: "upload",
          operation: "ipfs_upload",
          file: fileName,
          error: uploadResult.error,
        });
        throw new Error(`Upload failed: ${uploadResult.error}`);
      }
    }),
  );
  const totalUploadDuration = Date.now() - uploadStart;
  log("info", "ipfs_upload_complete", {
    operation: "ipfs_upload_total",
    duration_ms: totalUploadDuration,
    duration_seconds: (totalUploadDuration / 1000).toFixed(2),
  });

  const submissionBody = await fs.readFile(combinedCsvPath);
  await s3.send(
    new PutObjectCommand({ ...submissionS3Location, Body: submissionBody }),
  );
}

/**
 * Combine seed and county hash CSVs and parse into transaction items.
 *
 * @param {object} params - Combination context.
 * @param {string} params.seedHashCsv - Path to seed hash CSV file.
 * @param {string} params.countyHashCsv - Path to county hash CSV file.
 * @param {string} params.tmpDir - Temporary working directory.
 * @returns {Promise<{ combinedCsvPath: string, transactionItems: CsvRecord[] }>}
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
  return { combinedCsvPath, transactionItems };
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

  const base = {
    component: "post",
    at: new Date().toISOString(),
    county: countyName,
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
    const serialized = JSON.stringify(entry);
    if (level === "error") {
      console.error(serialized);
    } else {
      console.log(serialized);
    }
  };

  log("info", "post_lambda_start", { input_s3_key: event?.s3?.object?.key });

  try {
    // Download the prepared county archive
    const countyZipLocal = path.join(tmp, "county_input.zip");
    await downloadS3Object(parseS3Uri(prepared), countyZipLocal, log);

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
        await handleValidationFailure({
          tmpDir: tmp,
          log,
          error: runError.message,
          outputBaseUri: outputBase,
          inputKey: event?.s3?.object?.key,
        });
      }
      throw runError;
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

    const { combinedCsvPath, transactionItems } =
      await combineAndParseTransactions({
        seedHashCsv,
        countyHashCsv,
        tmpDir: tmp,
      });

    const submissionS3 = buildSubmissionLocation({
      outputBaseUri: outputBase,
      inputS3ObjectKey: event?.s3?.object?.key,
    });

    await uploadHashOutputs({
      filesForIpfs: [seedHashZip, countyHashZip],
      tmpDir: tmp,
      log,
      pinataJwt: requireEnv("ELEPHANT_PINATA_JWT"),
      combinedCsvPath,
      submissionS3Location: submissionS3,
    });
    const totalOperationDuration = Date.now() - Date.parse(base.at);
    log("info", "post_lambda_complete", {
      operation: "post_lambda_total",
      duration_ms: totalOperationDuration,
      duration_seconds: (totalOperationDuration / 1000).toFixed(2),
      transaction_items_count: transactionItems.length,
    });

    return { status: "success", transactionItems };
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
