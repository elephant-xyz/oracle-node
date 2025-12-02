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
  emitWorkflowEvent,
} from "./shared/index.mjs";

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
 * @typedef {object} SvlValidationError
 * @property {string} error_message - Validation error message.
 * @property {string} error_path - JSON path where the error occurred.
 * @property {string} [data_group_cid] - CID of the data group that caused the error.
 */

/**
 * @typedef {object} SvlOutput
 * @property {string} validatedOutputS3Uri - S3 URI of the validated output zip (same as input if valid).
 * @property {string} county - County name.
 * @property {string} executionId - Execution identifier.
 * @property {boolean} validationPassed - Whether validation succeeded.
 * @property {string} [errorsS3Uri] - S3 URI of validation errors CSV (if validation failed).
 * @property {SvlValidationError[]} [svlErrors] - Array of validation errors (if validation failed).
 */

/**
 * @typedef {object} SQSMessageBody
 * @property {string} taskToken - Step Functions task token for callback.
 * @property {SvlInput} input - SVL input parameters.
 */

/**
 * @typedef {object} DirectInvocationInput
 * @property {string} transformedOutputS3Uri - S3 URI of transformed output zip.
 * @property {string} county - County name.
 * @property {string} outputPrefix - S3 URI prefix for output files.
 * @property {string} executionId - Execution identifier.
 * @property {boolean} [directInvocation] - If true, skip EventBridge events and task token handling.
 */

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
 * Normalize raw CSV error rows to a consistent format.
 * Handles both camelCase (errorMessage, errorPath) and snake_case (error_message, error_path) field names.
 * @param {Record<string, string>[]} rawRows - Raw error rows from CSV
 * @returns {SvlValidationError[]} - Normalized validation errors
 */
function normalizeValidationErrors(rawRows) {
  return rawRows.map((row) => {
    const errorMessage =
      row.errorMessage?.trim() || row.error_message?.trim() || "Unknown error";
    const errorPath =
      row.errorPath?.trim() || row.error_path?.trim() || "unknown";
    const dataGroupCid =
      row.dataGroupCid?.trim() || row.data_group_cid?.trim() || undefined;

    /** @type {SvlValidationError} */
    const error = {
      error_message: errorMessage,
      error_path: errorPath,
    };

    // Only include data_group_cid if it has a value
    if (dataGroupCid) {
      error.data_group_cid = dataGroupCid;
    }

    return error;
  });
}

/**
 * Run the SVL (Schema Validation Layer) step.
 *
 * @param {object} params - SVL parameters.
 * @param {string} params.transformedOutputS3Uri - S3 URI of transformed output zip.
 * @param {string} params.county - County name.
 * @param {string} params.outputPrefix - Output S3 prefix.
 * @param {string} params.executionId - Execution ID.
 * @param {ReturnType<typeof createLogger>} params.log - Logger.
 * @returns {Promise<SvlOutput>}
 */
async function runSvl({
  transformedOutputS3Uri,
  county,
  outputPrefix,
  executionId,
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
      // Handle validation failure - upload errors CSV to S3 and return structured errors
      const submitErrorsPath = path.join(tmpDir, "submit_errors.csv");
      let errorsS3Uri = null;
      /** @type {SvlValidationError[]} */
      let svlErrors = [];

      try {
        const errorsExist = await fs
          .access(submitErrorsPath)
          .then(() => true)
          .catch(() => false);

        if (errorsExist) {
          const submitErrorsRaw = await csvToJson(submitErrorsPath);
          svlErrors = normalizeValidationErrors(submitErrorsRaw);

          log("error", "svl_validation_failed", {
            step: "validate",
            error: validationResult.error,
            error_count: svlErrors.length,
          });

          // Upload errors to S3
          const { bucket: outputBucket } = parseS3Uri(outputPrefix);
          const errorsKey = `${outputPrefix.replace(/^s3:\/\/[^/]+\//, "").replace(/\/$/, "")}/${executionId}/svl_errors.csv`;

          errorsS3Uri = await uploadToS3(
            submitErrorsPath,
            { bucket: outputBucket, key: errorsKey },
            log,
          );

          // Return validation failed result with structured errors
          return {
            validatedOutputS3Uri: transformedOutputS3Uri,
            county,
            executionId,
            validationPassed: false,
            errorsS3Uri,
            svlErrors,
          };
        }
      } catch (uploadError) {
        log("error", "svl_errors_upload_failed", {
          error: String(uploadError),
        });
      }

      // Throw error if validation failed (no errors file found or upload failed)
      const err = new Error(
        validationResult.error || "Schema validation failed",
      );
      err.name = "SvlFailedError";
      // @ts-ignore - attach errorsS3Uri and svlErrors for error handling
      err.errorsS3Uri = errorsS3Uri;
      // @ts-ignore
      err.svlErrors = svlErrors;
      throw err;
    }

    return {
      validatedOutputS3Uri: transformedOutputS3Uri,
      county,
      executionId,
      validationPassed: true,
      svlErrors: [],
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
 * Supports two invocation modes:
 * 1. SQS trigger from Step Functions workflow (with task token)
 * 2. Direct invocation from error-resolver/auto-repair (with directInvocation flag)
 *
 * @param {import("aws-lambda").SQSEvent | DirectInvocationInput} event - SQS event or direct invocation input.
 * @returns {Promise<void | SvlOutput>}
 */
export const handler = async (event) => {
  // Check if this is a direct invocation (from error-resolver/auto-repair)
  if ("directInvocation" in event && event.directInvocation) {
    const input = /** @type {DirectInvocationInput} */ (event);

    const log = createLogger({
      component: "svl-worker",
      at: new Date().toISOString(),
      county: input.county,
      executionId: input.executionId,
    });

    log("info", "svl_worker_start_direct", {
      transformedOutputS3Uri: input.transformedOutputS3Uri,
      directInvocation: true,
    });

    // Direct invocation: skip EventBridge events and task token handling
    // For direct invocation, throw on validation failure instead of returning success
    const result = await runSvl({
      transformedOutputS3Uri: input.transformedOutputS3Uri,
      county: input.county,
      outputPrefix: input.outputPrefix,
      executionId: input.executionId,
      log,
    });

    // For direct invocation, if validation failed, throw an error
    if (!result.validationPassed) {
      const err = new Error(
        `SVL validation failed. Errors CSV: ${result.errorsS3Uri}`,
      );
      err.name = "SvlValidationError";
      // @ts-ignore - attach errorsS3Uri for error handling
      err.errorsS3Uri = result.errorsS3Uri;
      throw err;
    }

    log("info", "svl_worker_complete_direct", {});

    return result;
  }

  // SQS trigger mode: process each message with task token pattern
  const sqsEvent = /** @type {import("aws-lambda").SQSEvent} */ (event);

  for (const record of sqsEvent.Records) {
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

    // Emit IN_PROGRESS event
    await emitWorkflowEvent({
      executionId: input.executionId,
      county: input.county,
      status: "IN_PROGRESS",
      phase: "SVL",
      step: "SVL",
      taskToken,
      log,
    });

    try {
      const result = await runSvl({
        transformedOutputS3Uri: input.transformedOutputS3Uri,
        county: input.county,
        outputPrefix: input.outputPrefix,
        executionId: input.executionId,
        log,
      });

      // Note: SUCCEEDED/FAILED events are emitted by the state machine after this step completes
      // The worker returns svlErrors in the result, which the state machine uses for EventBridge emission

      await executeWithTaskToken({
        taskToken,
        log,
        workerFn: async () => result,
      });
    } catch (err) {
      // Note: FAILED event is emitted by the state machine's WaitForSvlExceptionResolution state
      await executeWithTaskToken({
        taskToken,
        log,
        workerFn: async () => {
          throw err;
        },
      });
    }

    log("info", "svl_worker_complete", {});
  }
};
