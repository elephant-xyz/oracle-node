import { promises as fs } from "fs";
import path from "path";
import os from "os";
import { upload } from "@elephant-xyz/cli/lib";
import AdmZip from "adm-zip";
import {
  executeWithTaskToken,
  parseS3Uri,
  downloadS3Object,
  requireEnv,
  createLogger,
  emitWorkflowEvent,
} from "shared";

/**
 * @typedef {object} UploadInput
 * @property {string} seedHashZipS3Uri - S3 URI of the seed hash zip.
 * @property {string} countyHashZipS3Uri - S3 URI of the county hash zip.
 * @property {string} county - County name.
 * @property {string} executionId - Unique execution identifier.
 */

/**
 * @typedef {object} UploadOutput
 * @property {boolean} uploadSuccess - Whether upload succeeded.
 * @property {string} county - County name.
 * @property {string} executionId - Execution identifier.
 */

/**
 * @typedef {object} SQSMessageBody
 * @property {string} taskToken - Step Functions task token for callback.
 * @property {UploadInput} input - Upload input parameters.
 */

/**
 * Upload hash archives to IPFS-compatible endpoint by combining them into a single archive.
 *
 * @param {object} params - Upload context.
 * @param {string[]} params.filesForIpfs - Archive paths that will be extracted and combined for IPFS upload.
 * @param {string} params.tmpDir - Temporary working directory.
 * @param {ReturnType<typeof createLogger>} params.log - Structured logger.
 * @param {string} params.pinataJwt - Pinata authentication token.
 * @returns {Promise<void>}
 */
async function uploadHashOutputs({ filesForIpfs, tmpDir, log, pinataJwt }) {
  log("info", "ipfs_upload_start", {});

  // Create a temp directory for unzipping
  const extractTempDir = path.join(tmpDir, "ipfs_upload_temp");
  await fs.mkdir(extractTempDir, { recursive: true });

  try {
    // Unzip each file to a separate subdirectory
    for (const filePath of filesForIpfs) {
      const fileName = path.basename(filePath, ".zip");
      const extractSubDir = path.join(extractTempDir, fileName);
      await fs.mkdir(extractSubDir, { recursive: true });

      log("info", "ipfs_extract_file_start", { file: fileName });
      const zip = new AdmZip(await fs.readFile(filePath));
      zip.extractAllTo(extractSubDir, true);
      log("info", "ipfs_extract_file_complete", { file: fileName });
    }

    // Zip the entire temp directory
    const combinedZipPath = path.join(tmpDir, "combined_ipfs_upload.zip");
    const combinedZip = new AdmZip();
    combinedZip.addLocalFolder(extractTempDir);
    await fs.writeFile(combinedZipPath, combinedZip.toBuffer());

    // Upload the combined zip
    log("info", "ipfs_upload_combined_start", {});

    let uploadResult;
    try {
      uploadResult = await upload({
        input: combinedZipPath,
        pinataJwt,
        cwd: tmpDir,
      });
    } catch (uploadErr) {
      // Handle exceptions thrown by upload() (e.g., timeouts, network errors)
      let errorMessage = "Upload to IPFS failed";
      if (uploadErr instanceof Error) {
        if (uploadErr.message && uploadErr.message !== "undefined") {
          errorMessage = `Upload failed: ${uploadErr.message}`;
        } else if (uploadErr.name) {
          errorMessage = `Upload failed: ${uploadErr.name}`;
        }
      } else if (uploadErr && String(uploadErr) !== "undefined") {
        errorMessage = `Upload failed: ${String(uploadErr)}`;
      }

      const err = new Error(errorMessage);
      err.name = "UploadFailedError";
      throw err;
    }

    if (!uploadResult.success) {
      // Build error message with fallback if errorMessage is undefined
      let errorMessage = "Upload to IPFS failed";
      if (
        uploadResult.errorMessage &&
        uploadResult.errorMessage !== "undefined"
      ) {
        errorMessage = `Upload failed: ${uploadResult.errorMessage}`;
      } else if (
        uploadResult.errors &&
        Array.isArray(uploadResult.errors) &&
        uploadResult.errors.length > 0
      ) {
        errorMessage = `Upload failed: ${JSON.stringify(uploadResult.errors)}`;
      }

      const err = new Error(errorMessage);
      err.name = "UploadFailedError";
      throw err;
    }
  } finally {
    try {
      await fs.rm(extractTempDir, { recursive: true, force: true });
    } catch {}
  }
}

/**
 * Run the upload step for hash archives to IPFS.
 *
 * @param {object} params - Upload parameters.
 * @param {string} params.seedHashZipS3Uri - S3 URI of seed hash zip.
 * @param {string} params.countyHashZipS3Uri - S3 URI of county hash zip.
 * @param {string} params.county - County name.
 * @param {string} params.executionId - Execution ID.
 * @param {ReturnType<typeof createLogger>} params.log - Logger.
 * @returns {Promise<UploadOutput>}
 */
async function runUpload({
  seedHashZipS3Uri,
  countyHashZipS3Uri,
  county,
  executionId,
  log,
}) {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "upload-"));

  try {
    // Download hash zips
    const seedHashZipLocal = path.join(tmpDir, "seed_hash.zip");
    const countyHashZipLocal = path.join(tmpDir, "county_hash.zip");

    await Promise.all([
      downloadS3Object(parseS3Uri(seedHashZipS3Uri), seedHashZipLocal, log),
      downloadS3Object(parseS3Uri(countyHashZipS3Uri), countyHashZipLocal, log),
    ]);

    // Upload to IPFS
    const pinataJwt = requireEnv("ELEPHANT_PINATA_JWT");

    await uploadHashOutputs({
      filesForIpfs: [seedHashZipLocal, countyHashZipLocal],
      tmpDir,
      log,
      pinataJwt,
    });

    return {
      uploadSuccess: true,
      county,
      executionId,
    };
  } catch (err) {
    // Re-throw the error as-is - it already contains the error message
    throw err;
  } finally {
    try {
      await fs.rm(tmpDir, { recursive: true, force: true });
    } catch {}
  }
}

/**
 * Lambda handler for Upload worker.
 * Triggered by SQS messages from the Step Functions workflow.
 *
 * @param {import("aws-lambda").SQSEvent} event - SQS event containing messages.
 * @param {import("aws-lambda").Context} context - Lambda context.
 * @returns {Promise<void>}
 */
export const handler = async (event, context) => {
  for (const record of event.Records) {
    /** @type {SQSMessageBody} */
    const messageBody = JSON.parse(record.body);
    const { taskToken, input } = messageBody;

    const log = createLogger({
      component: "upload-worker",
      at: new Date().toISOString(),
      county: input.county,
      executionId: input.executionId,
    });

    log("info", "upload_worker_start", {
      seedHashZipS3Uri: input.seedHashZipS3Uri,
      countyHashZipS3Uri: input.countyHashZipS3Uri,
    });

    // Emit IN_PROGRESS event
    await emitWorkflowEvent({
      executionId: input.executionId,
      county: input.county,
      status: "IN_PROGRESS",
      phase: "Upload",
      step: "Upload",
      taskToken,
      log,
    });

    try {
      const result = await runUpload({
        seedHashZipS3Uri: input.seedHashZipS3Uri,
        countyHashZipS3Uri: input.countyHashZipS3Uri,
        county: input.county,
        executionId: input.executionId,
        log,
      });

      await executeWithTaskToken({
        taskToken,
        log,
        workerFn: async () => result,
      });
    } catch (err) {
      await executeWithTaskToken({
        taskToken,
        log,
        workerFn: async () => {
          throw err;
        },
      });
    }

    log("info", "upload_worker_complete", {});
  }
};
