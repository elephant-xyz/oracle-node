import { promises as fs } from "fs";
import path from "path";
import os from "os";
import { validate } from "@elephant-xyz/cli/lib";
import { parse } from "csv-parse/sync";
import {
  executeWithTaskToken,
  parseS3Uri,
  downloadS3Object,
  uploadToS3,
  createLogger,
} from "../shared/index.mjs";
import { createErrorsRepository } from "../post/errors.mjs";

/**
 * @typedef {object} SvlInput
 * @property {string} transformedOutputS3Uri - S3 URI of the transformed output zip.
 * @property {string} county - County name.
 * @property {string} outputPrefix - S3 URI prefix for output files.
 * @property {string} executionId - Unique execution identifier.
 * @property {string} [preparedS3Uri] - S3 URI of prepared input (for error tracking).
 * @property {object} [s3] - Original S3 event object (for error tracking).
 * @property {object} [s3.bucket] - S3 bucket info.
 * @property {string} [s3.bucket.name] - S3 bucket name.
 * @property {object} [s3.object] - S3 object info.
 * @property {string} [s3.object.key] - S3 object key.
 */

/**
 * @typedef {object} SvlOutput
 * @property {string} validatedOutputS3Uri - S3 URI of the validated output zip (same as input if valid).
 * @property {string} county - County name.
 * @property {string} executionId - Execution identifier.
 * @property {boolean} validationPassed - Whether validation succeeded.
 * @property {string} [errorsS3Uri] - S3 URI of validation errors CSV (if validation failed).
 */

/**
 * @typedef {object} SQSMessageBody
 * @property {string} taskToken - Step Functions task token for callback.
 * @property {SvlInput} input - SVL input parameters.
 */

/** @type {import("../post/errors.mjs").ErrorsRepository | null} */
let cachedErrorsRepository = null;

/**
 * Lazy-initialize the DynamoDB errors repository.
 *
 * @returns {import("../post/errors.mjs").ErrorsRepository} - Repository instance.
 */
function getErrorsRepository() {
  if (!cachedErrorsRepository) {
    const tableName = process.env.ERRORS_TABLE_NAME;
    if (!tableName) {
      throw new Error("ERRORS_TABLE_NAME is required");
    }
    cachedErrorsRepository = createErrorsRepository({ tableName });
  }
  return cachedErrorsRepository;
}

/**
 * Convert CSV file to JSON array
 * @param {string} csvPath - Path to CSV file
 * @returns {Promise<Record<string, string>[]>} - Array of objects representing CSV rows
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
  } catch {
    return [];
  }
}

/**
 * Run the SVL (Schema Validation Layer) step.
 *
 * @param {object} params - SVL parameters.
 * @param {string} params.transformedOutputS3Uri - S3 URI of transformed output zip.
 * @param {string} params.county - County name.
 * @param {string} params.outputPrefix - Output S3 prefix.
 * @param {string} params.executionId - Execution ID.
 * @param {string} [params.preparedS3Uri] - S3 URI of prepared input.
 * @param {object} [params.s3] - Original S3 event object.
 * @param {ReturnType<typeof createLogger>} params.log - Logger.
 * @returns {Promise<SvlOutput>}
 */
async function runSvl({
  transformedOutputS3Uri,
  county,
  outputPrefix,
  executionId,
  preparedS3Uri,
  s3,
  log,
}) {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "svl-"));

  try {
    // Download transformed output zip
    const transformedZipLocal = path.join(tmpDir, "transformed_output.zip");
    await downloadS3Object(
      parseS3Uri(transformedOutputS3Uri),
      transformedZipLocal,
      log,
    );

    // Run validation
    log("info", "svl_start", {
      operation: "schema_validation",
      county,
      executionId,
    });

    const validationStart = Date.now();
    const validationResult = await validate({
      input: transformedZipLocal,
      cwd: tmpDir,
    });
    const validationDuration = Date.now() - validationStart;

    log("info", "svl_complete", {
      operation: "schema_validation",
      duration_ms: validationDuration,
      duration_seconds: (validationDuration / 1000).toFixed(2),
      success: validationResult.success,
    });

    if (!validationResult.success) {
      // Handle validation failure - upload errors CSV to S3 and save to DynamoDB
      const submitErrorsPath = path.join(tmpDir, "submit_errors.csv");
      let errorsS3Uri = null;

      try {
        const errorsExist = await fs
          .access(submitErrorsPath)
          .then(() => true)
          .catch(() => false);

        if (errorsExist) {
          const submitErrors = await csvToJson(submitErrorsPath);

          log("error", "svl_validation_failed", {
            step: "validate",
            error: validationResult.error,
            error_count: submitErrors.length,
          });

          // Upload errors to S3
          const { bucket: outputBucket } = parseS3Uri(outputPrefix);
          const errorsKey = `${outputPrefix.replace(/^s3:\/\/[^/]+\//, "").replace(/\/$/, "")}/${executionId}/svl_errors.csv`;

          errorsS3Uri = await uploadToS3(
            submitErrorsPath,
            { bucket: outputBucket, key: errorsKey },
            log,
          );

          // Save errors to DynamoDB (same as post lambda does)
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
                source: {
                  s3Bucket: s3?.bucket?.name,
                  s3Key: s3?.object?.key,
                },
                errorsS3Uri: errorsS3Uri ?? undefined,
                failureMessage: validationResult.error,
                occurredAt: new Date().toISOString(),
                preparedS3Uri: preparedS3Uri,
              });

              log("info", "svl_errors_saved_to_dynamodb", {
                execution_id: executionId,
                error_count: submitErrors.length,
              });

              // Return success with empty validation (errors saved to DynamoDB)
              return {
                validatedOutputS3Uri: transformedOutputS3Uri,
                county,
                executionId,
                validationPassed: false,
                errorsS3Uri,
              };
            } catch (repoError) {
              log("error", "svl_errors_repository_save_failed", {
                error: String(repoError),
                execution_id: executionId,
              });
            }
          }
        }
      } catch (uploadError) {
        log("error", "svl_errors_upload_failed", {
          error: String(uploadError),
        });
      }

      // Throw error if we couldn't save to DynamoDB
      const err = new Error(
        validationResult.error || "Schema validation failed",
      );
      err.name = "SvlFailedError";
      // @ts-ignore - attach errorsS3Uri for error handling
      err.errorsS3Uri = errorsS3Uri;
      throw err;
    }

    return {
      validatedOutputS3Uri: transformedOutputS3Uri,
      county,
      executionId,
      validationPassed: true,
    };
  } finally {
    // Cleanup
    try {
      await fs.rm(tmpDir, { recursive: true, force: true });
    } catch {}
  }
}

/**
 * Lambda handler for SVL worker.
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
      component: "svl-worker",
      at: new Date().toISOString(),
      county: input.county,
      executionId: input.executionId,
    });

    log("info", "svl_worker_start", {
      transformedOutputS3Uri: input.transformedOutputS3Uri,
    });

    await executeWithTaskToken({
      taskToken,
      log,
      workerFn: async () => {
        return await runSvl({
          transformedOutputS3Uri: input.transformedOutputS3Uri,
          county: input.county,
          outputPrefix: input.outputPrefix,
          executionId: input.executionId,
          preparedS3Uri: input.preparedS3Uri,
          s3: input.s3,
          log,
        });
      },
    });

    log("info", "svl_worker_complete", {});
  }
};
