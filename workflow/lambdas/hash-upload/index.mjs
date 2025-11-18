import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { promises as fs } from "fs";
import path from "path";
import os from "os";
import { hash, upload } from "@elephant-xyz/cli/lib";
import { parse } from "csv-parse/sync";
import AdmZip from "adm-zip";

const s3 = new S3Client({});

/**
 * @typedef {string} ZipFile
 */

/**
 * @typedef {Record<string, string>} CsvRecord
 */

/**
 * @typedef {object} HashAndUploadInput
 * @property {string} preparedInputS3Uri - S3 URI of the seed/prepared input zip
 * @property {string} transformedOutputS3Uri - S3 URI of the transformed/validated output zip
 * @property {string} executionId - Execution identifier
 * @property {string} county - County name
 * @property {object} [s3] - S3 event object
 * @property {object} [s3.bucket] - S3 bucket info
 * @property {string} [s3.bucket.name] - S3 bucket name
 * @property {object} [s3.object] - S3 object info
 * @property {string} [s3.object.key] - S3 object key
 */

/**
 * @typedef {object} HashAndUploadOutput
 * @property {string} status - Status of the operation
 * @property {CsvRecord[]} transactionItems - Array of transaction records
 */

/**
 * @typedef {(level: "info"|"error"|"debug", msg: string, details?: Record<string, unknown>) => void} StructuredLogger
 */

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
 * @param {StructuredLogger} params.log - Structured logger.
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
        errorMessage: uploadResult.errorMessage,
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
 * Combine seed and county hash CSVs into a single file.
 *
 * @param {string} seedHashCsv - Path to seed hash CSV.
 * @param {string} countyHashCsv - Path to county hash CSV.
 * @param {string} tmpDir - Temporary directory.
 * @returns {Promise<string>} - Path to combined CSV.
 */
async function combineCsv(seedHashCsv, countyHashCsv, tmpDir) {
  const combinedCsvPath = path.join(tmpDir, "combined_hash.csv");
  const seedContent = await fs.readFile(seedHashCsv, "utf8");
  const countyContent = await fs.readFile(countyHashCsv, "utf8");

  const seedLines = seedContent.trim().split("\n");
  const countyLines = countyContent.trim().split("\n");

  // Skip header from county CSV (first line)
  const combinedContent = seedLines.concat(countyLines.slice(1)).join("\n");
  await fs.writeFile(combinedCsvPath, combinedContent, "utf8");

  return combinedCsvPath;
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
  const csvContent = await fs.readFile(combinedCsvPath, "utf8");
  const records = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });
  return records;
}

/**
 * @param {HashAndUploadInput} event
 * @returns {Promise<HashAndUploadOutput>}
 */
/** @type {(event: HashAndUploadInput) => Promise<HashAndUploadOutput>} */
export const handler = async (event) => {
  if (!event.preparedInputS3Uri)
    throw new Error("preparedInputS3Uri is required");
  if (!event.transformedOutputS3Uri)
    throw new Error("transformedOutputS3Uri is required");
  if (!event.executionId) throw new Error("executionId is required");
  if (!event.county) throw new Error("county is required");

  const pinataJwt = requireEnv("ELEPHANT_PINATA_JWT");

  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "hash-upload-"));

  const base = {
    component: "hash-upload",
    at: new Date().toISOString(),
    county: event.county,
    execution_id: event.executionId,
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

  log("info", "hash_upload_lambda_start", {
    prepared_input_s3_uri: event.preparedInputS3Uri,
    transformed_output_s3_uri: event.transformedOutputS3Uri,
  });

  try {
    // Download prepared input (seed)
    const seedZipLocal = path.join(tmp, "prepared_input.zip");
    await downloadS3Object(
      parseS3Uri(event.preparedInputS3Uri),
      seedZipLocal,
      log,
    );

    // Download transformed output
    const countyOutZip = path.join(tmp, "transformed_output.zip");
    await downloadS3Object(
      parseS3Uri(event.transformedOutputS3Uri),
      countyOutZip,
      log,
    );

    // Generate hash artifacts
    const {
      propertyCid,
      seedHashCsv,
      countyHashCsv,
      seedHashZip,
      countyHashZip,
    } = await generateHashArtifacts({
      seedZipLocal,
      countyOutZip,
      tmpDir: tmp,
      log,
    });

    if (!propertyCid) {
      log("debug", "property_cid_missing", {});
    }

    // Combine and parse transaction items
    const transactionItems = await combineAndParseTransactions({
      seedHashCsv,
      countyHashCsv,
      tmpDir: tmp,
    });

    // Upload to IPFS
    await uploadHashOutputs({
      filesForIpfs: [seedHashZip, countyHashZip],
      tmpDir: tmp,
      log,
      pinataJwt,
    });

    const totalOperationDuration = Date.now() - Date.parse(base.at);
    log("info", "hash_upload_lambda_complete", {
      operation: "hash_upload_lambda_total",
      duration_ms: totalOperationDuration,
      duration_seconds: (totalOperationDuration / 1000).toFixed(2),
      transaction_items_count: transactionItems.length,
    });

    return {
      status: "success",
      transactionItems,
    };
  } catch (err) {
    log("error", "hash_upload_lambda_failed", {
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
