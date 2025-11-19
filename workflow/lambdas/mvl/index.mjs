import {
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { promises as fs } from "fs";
import path from "path";
import os from "os";
import { mirrorValidate } from "@elephant-xyz/cli/lib";
import { parse } from "csv-parse/sync";
import { createErrorsRepository } from "./errors.mjs";

const s3 = new S3Client({});

/**
 * @typedef {Record<string, string>} CsvRecord
 */

/**
 * @typedef {object} MirrorValidateInput
 * @property {string} preparedInputS3Uri - S3 URI of the input zip (before transform)
 * @property {string} transformedOutputS3Uri - S3 URI of the output zip (after transform)
 * @property {string} preparedOutputS3Uri - S3 URI of the prepare step output (for error reporting)
 * @property {string} county - County name
 * @property {string} executionId - Execution identifier
 * @property {object} [s3] - S3 event object
 * @property {object} [s3.bucket] - S3 bucket info
 * @property {string} [s3.bucket.name] - S3 bucket name
 * @property {object} [s3.object] - S3 object info
 * @property {string} [s3.object.key] - S3 object key
 */

/**
 * @typedef {object} MirrorValidateOutput
 * @property {string} status
 * @property {number} mvlMetric - Global completeness metric (0-100)
 * @property {boolean} mvlPassed - Whether the validation passed the threshold (>= 0.8)
 * @property {string} [errorsS3Uri] - S3 URI of the errors CSV file (when validation fails)
 */

/**
 * @typedef {(level: "info"|"error"|"debug", msg: string, details?: Record<string, unknown>) => void} StructuredLogger
 */

/**
 * @typedef {object} ValidationFailureResult
 * @property {boolean} saved - Whether errors were successfully saved to DynamoDB.
 */

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
  const header = "error_path,error_message,data_group_cid\n";
  const rows = sources.map(
    (source) =>
      `${escapeCsvField(source)},${escapeCsvField("Value from this selector is not mapped to the output")},${escapeCsvField(dataGroupCid)}`,
  );

  const csvContent = header + rows.join("\n");
  await fs.writeFile(csvPath, csvContent, "utf8");

  return csvPath;
}

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
  errorsS3Uri,
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
              ? `Mirror validation failed. Submit errors csv: ${mvlErrorsS3Uri}. DynamoDB save failed: ${String(repoError)}`
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
    ? `Mirror validation failed. Submit errors csv: ${mvlErrorsS3Uri}`
    : "Mirror validation failed";
  const errorToThrow = new Error(errorMessage);
  errorToThrow.name = "MirrorValidationFailure";

  throw errorToThrow;
}

/**
 * Try to download static parts CSV file for a county from S3.
 * Returns the local file path if found, or undefined if not found.
 *
 * @param {string} county - County name (e.g., "Collier", "Palm Beach").
 * @param {string} tmpDir - Temporary directory to download to.
 * @param {StructuredLogger} log - Structured logger.
 * @returns {Promise<string | undefined>} - Local file path if found, undefined otherwise.
 */
async function tryDownloadStaticParts(county, tmpDir, log) {
  const outputBaseUri = process.env.OUTPUT_BASE_URI;
  if (!outputBaseUri) {
    log("error", "mvl_static_parts_skipped", {
      reason: "OUTPUT_BASE_URI not set",
    });
    return undefined;
  }

  // Extract bucket from OUTPUT_BASE_URI (e.g., s3://bucket-name/outputs -> bucket-name)
  const { bucket } = parseS3Uri(outputBaseUri);

  // Normalize county name to match file naming convention (lowercase, replace spaces with hyphens)
  const normalizedCounty = county.toLowerCase().replace(/\s+/g, "-");
  const staticPartsKey = `source-html-static-parts/${normalizedCounty}.csv`;

  log("info", "mvl_static_parts_check", {
    county,
    normalized_county: normalizedCounty,
    s3_key: staticPartsKey,
  });

  try {
    // Check if file exists
    await s3.send(
      new HeadObjectCommand({
        Bucket: bucket,
        Key: staticPartsKey,
      }),
    );

    // File exists, download it
    const staticCsvLocal = path.join(tmpDir, "static-parts.csv");
    await downloadS3Object(
      { bucket, key: staticPartsKey },
      staticCsvLocal,
      log,
    );

    log("info", "mvl_static_parts_downloaded", {
      county,
      s3_uri: `s3://${bucket}/${staticPartsKey}`,
    });

    return staticCsvLocal;
  } catch (error) {
    // File doesn't exist or error occurred
    const err = /** @type {Error & {$metadata?: {httpStatusCode?: number}}} */ (
      error
    );
    if (err.name === "NotFound" || err.$metadata?.httpStatusCode === 404) {
      log("info", "mvl_static_parts_not_found", {
        county,
        normalized_county: normalizedCounty,
        message:
          "Static parts file not found, running MVL without static parts",
      });
    } else {
      log("error", "mvl_static_parts_error", {
        county,
        error: String(err),
        message:
          "Error checking static parts file, running MVL without static parts",
      });
    }
    return undefined;
  }
}

/**
 * Run mirror validation to measure completeness of transformed data.
 *
 * @param {object} params - Validation context.
 * @param {string} params.preparedInputZipLocal - Path to input zip (before transform).
 * @param {string} params.transformedOutputZipLocal - Path to output zip (after transform).
 * @param {string} params.tmpDir - Temporary working directory.
 * @param {StructuredLogger} params.log - Structured logger.
 * @param {string} params.preparedOutputS3Uri - Prepare step output S3 URI to derive report location.
 * @param {string} params.county - County name for looking up static parts file.
 * @returns {Promise<number>} - Global completeness metric (0-100).
 */
async function runMirrorValidation({
  preparedInputZipLocal,
  transformedOutputZipLocal,
  tmpDir,
  log,
  preparedOutputS3Uri,
  county,
}) {
  // Try to download county-specific static parts file
  const staticCsvLocal = await tryDownloadStaticParts(county, tmpDir, log);

  log("info", "mvl_validation_start", {
    operation: "mirror_validation",
    has_static_parts: !!staticCsvLocal,
  });
  const mvlStart = Date.now();

  // Run mirrorValidate with or without static parts
  /** @type {{prepareZip: string, transformZip: string, cwd: string, staticParts?: string}} */
  const mvlOptions = {
    prepareZip: preparedInputZipLocal,
    transformZip: transformedOutputZipLocal,
    cwd: tmpDir,
  };

  // Only add staticParts if we have the file
  if (staticCsvLocal) {
    mvlOptions.staticParts = staticCsvLocal;
  }

  const mvlResult = await mirrorValidate(mvlOptions);

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

  // Derive report S3 location from preparedOutputS3Uri
  // e.g., s3://bucket/outputs/2029035U0000000000320U/output.zip -> s3://bucket/outputs/2029035U0000000000320U/mvl-report.json
  const outputDir = preparedOutputS3Uri.substring(
    0,
    preparedOutputS3Uri.lastIndexOf("/"),
  );
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
  // globalCompleteness is in range [0, 1], so 0.8 = 80%
  if (globalCompleteness < 0.8) {
    const errorMsg = `Mirror validation failed: global completeness ${(globalCompleteness * 100).toFixed(2)}% is below threshold of 80.0%`;
    log("error", "mvl_completeness_below_threshold", {
      global_completeness: globalCompleteness,
      threshold: 0.8,
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
 * @param {MirrorValidateInput} event
 * @returns {Promise<MirrorValidateOutput>}
 */
/** @type {(event: MirrorValidateInput) => Promise<MirrorValidateOutput>} */
export const handler = async (event) => {
  if (!event.preparedInputS3Uri)
    throw new Error("preparedInputS3Uri is required");
  if (!event.transformedOutputS3Uri)
    throw new Error("transformedOutputS3Uri is required");
  if (!event.preparedOutputS3Uri)
    throw new Error("preparedOutputS3Uri is required");
  if (!event.county) throw new Error("county is required");
  if (!event.executionId) throw new Error("executionId is required");

  const outputBase = process.env.OUTPUT_BASE_URI;
  if (!outputBase) throw new Error("OUTPUT_BASE_URI is required");

  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "mvl-"));

  const base = {
    component: "mirror-validator",
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

  log("info", "mvl_lambda_start", {
    input_s3_key: event?.s3?.object?.key,
  });

  /** @type {import("./errors.mjs").ExecutionSource} */
  const sourceEvent = {
    s3Bucket: event?.s3?.bucket?.name,
    s3Key: event?.s3?.object?.key,
  };

  try {
    // Download prepared input (before transform)
    const preparedInputZipLocal = path.join(tmp, "prepared_input.zip");
    await downloadS3Object(
      parseS3Uri(event.preparedInputS3Uri),
      preparedInputZipLocal,
      log,
    );

    // Download transformed output (after transform)
    const transformedOutputZipLocal = path.join(tmp, "transformed_output.zip");
    await downloadS3Object(
      parseS3Uri(event.transformedOutputS3Uri),
      transformedOutputZipLocal,
      log,
    );

    // Run mirror validation
    let mvlMetric;
    try {
      mvlMetric = await runMirrorValidation({
        preparedInputZipLocal,
        transformedOutputZipLocal,
        tmpDir: tmp,
        log,
        preparedOutputS3Uri: event.preparedOutputS3Uri,
        county: event.county,
      });
    } catch (mvlError) {
      if (
        mvlError instanceof Error &&
        mvlError.name === "MirrorValidationFailure"
      ) {
        // @ts-ignore - Access mvlResult from error object
        const mvlResult = mvlError.mvlResult;

        // Always generate and upload errors CSV if available
        let errorsS3Uri = null;
        let errorsCsvPath;
        try {
          const unmatchedSources = extractUnmatchedSources(
            /** @type {Record<string, { unmatchedFromA?: unknown[] }>} */ (
              /** @type {unknown} */ (mvlResult.comparison)
            ),
          );
          errorsCsvPath = await generateMirrorValidationErrorsCsv(
            unmatchedSources,
            tmp,
            "bafkreigsipwhkwrboi73b3xvn4tjwd26pqyzz5zmyxvbnrgeb2qbq2bz34", // TODO: Dynamically fetch depending on the currently processed data group
          );

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

        // Extract the actual globalCompleteness metric from the result
        const actualMvlMetric = mvlResult?.globalCompleteness ?? 0;

        // Save errors to DynamoDB if we have errors
        try {
          // Read the CSV to check if we have actual errors
          const mvlErrors = errorsCsvPath ? await csvToJson(errorsCsvPath) : [];

          if (mvlErrors.length > 0) {
            // We have errors to save, try to save them
            const result = await handleMirrorValidationFailure({
              log,
              error: mvlError.message,
              executionId: event.executionId,
              county: event.county,
              source: sourceEvent,
              occurredAt: base.at,
              preparedS3Uri: event.preparedOutputS3Uri,
              errorsCsvPath,
              errorsS3Uri,
            });
            // Errors were successfully saved to DynamoDB, return success with actual metric
            if (result.saved) {
              log("info", "mirror_validation_failed_but_saved", {
                execution_id: event.executionId,
                county: event.county,
                global_completeness: actualMvlMetric,
              });
              const totalOperationDuration = Date.now() - Date.parse(base.at);
              log("info", "mvl_lambda_complete", {
                operation: "mvl_lambda_total",
                duration_ms: totalOperationDuration,
                duration_seconds: (totalOperationDuration / 1000).toFixed(2),
                mvl_metric: actualMvlMetric,
              });
              return {
                status: "success",
                mvlMetric: actualMvlMetric,
                mvlPassed: false,
                errorsS3Uri,
              };
            }
          } else {
            // No specific errors to save, but validation failed (likely low completeness)
            // Log the failure and return success with actual metric
            log("info", "mirror_validation_failed_no_errors", {
              execution_id: event.executionId,
              county: event.county,
              message:
                "Mirror validation failed but no specific errors to save",
              errors_s3_uri: errorsS3Uri,
              global_completeness: actualMvlMetric,
            });
            const totalOperationDuration = Date.now() - Date.parse(base.at);
            log("info", "mvl_lambda_complete", {
              operation: "mvl_lambda_total",
              duration_ms: totalOperationDuration,
              duration_seconds: (totalOperationDuration / 1000).toFixed(2),
              mvl_metric: actualMvlMetric,
            });
            return {
              status: "success",
              mvlMetric: actualMvlMetric,
              mvlPassed: false,
              errorsS3Uri,
            };
          }
        } catch (saveError) {
          // DynamoDB save failed, re-throw the error
          throw saveError;
        }
      }
      // Re-throw if not a MirrorValidationFailure or if handleMirrorValidationFailure didn't return saved: true
      throw mvlError;
    }

    const totalOperationDuration = Date.now() - Date.parse(base.at);
    log("info", "mvl_lambda_complete", {
      operation: "mvl_lambda_total",
      duration_ms: totalOperationDuration,
      duration_seconds: (totalOperationDuration / 1000).toFixed(2),
      mvl_metric: mvlMetric,
    });

    return { status: "success", mvlMetric, mvlPassed: true };
  } catch (err) {
    log("error", "mvl_lambda_failed", {
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
