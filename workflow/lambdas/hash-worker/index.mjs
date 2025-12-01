import { promises as fs } from "fs";
import path from "path";
import os from "os";
import { hash } from "@elephant-xyz/cli/lib";
import { parse } from "csv-parse/sync";
import {
  executeWithTaskToken,
  parseS3Uri,
  downloadS3Object,
  uploadToS3,
  createLogger,
  emitWorkflowEvent,
  createWorkflowError,
} from "../shared/index.mjs";

/**
 * @typedef {object} HashInput
 * @property {string} validatedOutputS3Uri - S3 URI of the validated/transformed output zip (county data).
 * @property {string} seedOutputS3Uri - S3 URI of the seed output zip.
 * @property {string} county - County name.
 * @property {string} outputPrefix - S3 URI prefix for output files.
 * @property {string} executionId - Unique execution identifier.
 */

/**
 * @typedef {object} HashOutput
 * @property {string} seedHashZipS3Uri - S3 URI of the seed hash zip.
 * @property {string} countyHashZipS3Uri - S3 URI of the county hash zip.
 * @property {string} combinedHashCsvS3Uri - S3 URI of the combined hash CSV.
 * @property {string} county - County name.
 * @property {string} executionId - Execution identifier.
 * @property {string} [propertyCid] - Property CID extracted from seed hash.
 */

/**
 * @typedef {object} SQSMessageBody
 * @property {string} taskToken - Step Functions task token for callback.
 * @property {HashInput} input - Hash input parameters.
 */

/**
 * Combine two CSV files by keeping a single header row
 * from the first file and appending the data rows from both.
 *
 * @param {string} seedHashCsv - Path to the first CSV file (provides header).
 * @param {string} countyHashCsv - Path to the second CSV file (data appended).
 * @param {string} tmpDir - Directory path where the combined file will be saved.
 * @returns {Promise<string>} - The path of the newly created combined CSV file.
 */
async function combineCsv(seedHashCsv, countyHashCsv, tmpDir) {
  const combinedCsv = path.join(tmpDir, "combined_hash.csv");

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
 * Run the hash generation step for seed and county data.
 *
 * @param {object} params - Hash parameters.
 * @param {string} params.validatedOutputS3Uri - S3 URI of validated output zip.
 * @param {string} params.seedOutputS3Uri - S3 URI of seed output zip.
 * @param {string} params.county - County name.
 * @param {string} params.outputPrefix - Output S3 prefix.
 * @param {string} params.executionId - Execution ID.
 * @param {ReturnType<typeof createLogger>} params.log - Logger.
 * @returns {Promise<HashOutput>}
 */
async function runHash({
  validatedOutputS3Uri,
  seedOutputS3Uri,
  county,
  outputPrefix,
  executionId,
  log,
}) {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "hash-"));

  try {
    // Download inputs
    const countyZipLocal = path.join(tmpDir, "county_output.zip");
    const seedZipLocal = path.join(tmpDir, "seed_output.zip");

    await Promise.all([
      downloadS3Object(parseS3Uri(validatedOutputS3Uri), countyZipLocal, log),
      downloadS3Object(parseS3Uri(seedOutputS3Uri), seedZipLocal, log),
    ]);

    // Define output paths
    const seedHashZip = path.join(tmpDir, "seed_hash.zip");
    const seedHashCsv = path.join(tmpDir, "seed_hash.csv");
    const countyHashZip = path.join(tmpDir, "county_hash.zip");
    const countyHashCsv = path.join(tmpDir, "county_hash.csv");

    // Hash seed output first to get propertyCid
    log("info", "hash_seed_start", { operation: "seed_hash" });
    const seedHashStart = Date.now();
    const seedHashResult = await hash({
      input: seedZipLocal,
      outputZip: seedHashZip,
      outputCsv: seedHashCsv,
      cwd: tmpDir,
    });
    const seedHashDuration = Date.now() - seedHashStart;

    if (!seedHashResult.success) {
      log("error", "hash_seed_failed", {
        operation: "seed_hash",
        duration_ms: seedHashDuration,
        error: seedHashResult.error,
      });
      throw new Error(`Seed hash failed: ${seedHashResult.error}`);
    }

    log("info", "hash_seed_complete", {
      operation: "seed_hash",
      duration_ms: seedHashDuration,
      duration_seconds: (seedHashDuration / 1000).toFixed(2),
    });

    // Extract propertyCid from seed hash CSV
    const csv = await fs.readFile(seedHashCsv, "utf8");
    const records = parse(csv, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    });
    const propertyCid = records[0]?.propertyCid;

    // Hash county output with propertyCid
    log("info", "hash_county_start", { operation: "county_hash" });
    const countyHashStart = Date.now();
    const countyHashResult = await hash({
      input: countyZipLocal,
      outputZip: countyHashZip,
      outputCsv: countyHashCsv,
      propertyCid,
      cwd: tmpDir,
    });
    const countyHashDuration = Date.now() - countyHashStart;

    if (!countyHashResult.success) {
      log("error", "hash_county_failed", {
        operation: "county_hash",
        duration_ms: countyHashDuration,
        error: countyHashResult.error,
      });
      throw new Error(`County hash failed: ${countyHashResult.error}`);
    }

    log("info", "hash_county_complete", {
      operation: "county_hash",
      duration_ms: countyHashDuration,
      duration_seconds: (countyHashDuration / 1000).toFixed(2),
    });

    // Combine CSVs
    const combinedCsvPath = await combineCsv(
      seedHashCsv,
      countyHashCsv,
      tmpDir,
    );

    // Upload all artifacts to S3
    const { bucket: outputBucket } = parseS3Uri(outputPrefix);
    const baseKey = `${outputPrefix.replace(/^s3:\/\/[^/]+\//, "").replace(/\/$/, "")}/${executionId}`;

    const [seedHashZipS3Uri, countyHashZipS3Uri, combinedHashCsvS3Uri] =
      await Promise.all([
        uploadToS3(
          seedHashZip,
          { bucket: outputBucket, key: `${baseKey}/seed_hash.zip` },
          log,
        ),
        uploadToS3(
          countyHashZip,
          { bucket: outputBucket, key: `${baseKey}/county_hash.zip` },
          log,
        ),
        uploadToS3(
          combinedCsvPath,
          { bucket: outputBucket, key: `${baseKey}/combined_hash.csv` },
          log,
          "text/csv",
        ),
      ]);

    return {
      seedHashZipS3Uri,
      countyHashZipS3Uri,
      combinedHashCsvS3Uri,
      county,
      executionId,
      propertyCid,
    };
  } finally {
    // Cleanup
    try {
      await fs.rm(tmpDir, { recursive: true, force: true });
    } catch {}
  }
}

/**
 * Lambda handler for Hash worker.
 * Triggered by SQS messages from the Step Functions workflow.
 *
 * @param {import("aws-lambda").SQSEvent} event - SQS event containing messages.
 * @returns {Promise<void>}
 */
export const handler = async (event) => {
  for (const record of event.Records) {
    /** @type {SQSMessageBody} */
    const messageBody = JSON.parse(record.body);
    const { taskToken, input } = messageBody;

    const log = createLogger({
      component: "hash-worker",
      at: new Date().toISOString(),
      county: input.county,
      executionId: input.executionId,
    });

    log("info", "hash_worker_start", {
      validatedOutputS3Uri: input.validatedOutputS3Uri,
      seedOutputS3Uri: input.seedOutputS3Uri,
    });

    // Emit IN_PROGRESS event
    await emitWorkflowEvent({
      executionId: input.executionId,
      county: input.county,
      status: "IN_PROGRESS",
      phase: "Hash",
      step: "Hash",
      taskToken,
      log,
    });

    try {
      const result = await runHash({
        validatedOutputS3Uri: input.validatedOutputS3Uri,
        seedOutputS3Uri: input.seedOutputS3Uri,
        county: input.county,
        outputPrefix: input.outputPrefix,
        executionId: input.executionId,
        log,
      });

      // Emit SUCCEEDED event
      await emitWorkflowEvent({
        executionId: input.executionId,
        county: input.county,
        status: "SUCCEEDED",
        phase: "Hash",
        step: "Hash",
        log,
      });

      await executeWithTaskToken({
        taskToken,
        log,
        workerFn: async () => result,
      });
    } catch (err) {
      // Emit FAILED event
      await emitWorkflowEvent({
        executionId: input.executionId,
        county: input.county,
        status: "FAILED",
        phase: "Hash",
        step: "Hash",
        taskToken,
        errors: [
          createWorkflowError("HASH_FAILED", {
            message: err instanceof Error ? err.message : String(err),
          }),
        ],
        log,
      });

      await executeWithTaskToken({
        taskToken,
        log,
        workerFn: async () => {
          throw err;
        },
      });
    }

    log("info", "hash_worker_complete", {});
  }
};
