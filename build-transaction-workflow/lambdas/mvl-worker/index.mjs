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
import {
  executeWithTaskToken,
  parseS3Uri,
  downloadS3Object,
  uploadContentToS3,
  requireEnv,
  createLogger,
} from "../shared/index.mjs";

const s3 = new S3Client({});

/**
 * @typedef {object} MvlInput
 * @property {string} preparedInputS3Uri - S3 URI of the prepared input zip (before transform).
 * @property {string} transformedOutputS3Uri - S3 URI of the transformed output zip (after transform).
 * @property {string} county - County name.
 * @property {string} outputPrefix - S3 URI prefix for output files.
 * @property {string} executionId - Unique execution identifier.
 */

/**
 * @typedef {object} MvlOutput
 * @property {string} status - Status of the operation.
 * @property {number} mvlMetric - Global completeness metric (0-1).
 * @property {boolean} mvlPassed - Whether validation passed threshold (>= 0.8).
 * @property {string} county - County name.
 * @property {string} executionId - Execution identifier.
 * @property {string} [errorsS3Uri] - S3 URI of errors CSV (if validation failed).
 * @property {string} [reportS3Uri] - S3 URI of the MVL report JSON.
 */

/**
 * @typedef {object} SQSMessageBody
 * @property {string} taskToken - Step Functions task token for callback.
 * @property {MvlInput} input - MVL input parameters.
 */

/**
 * Extract all unmatched source values from mirror validation comparison result.
 *
 * @param {Record<string, { unmatchedFromA?: unknown[] }>} comparison - Comparison result.
 * @returns {string[]} - Array of unique source strings.
 */
function extractUnmatchedSources(comparison) {
  /** @type {Set<string>} */
  const sources = new Set();
  const entityTypes = ["QUANTITY", "DATE", "ORGANIZATION", "LOCATION"];

  for (const entityType of entityTypes) {
    const entityComparison = comparison[entityType];
    if (!entityComparison || !entityComparison.unmatchedFromA) continue;

    const unmatched = entityComparison.unmatchedFromA;
    if (Array.isArray(unmatched)) {
      for (const item of unmatched) {
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
 * Escape CSV field value.
 *
 * @param {string} value - Value to escape.
 * @returns {string} - Escaped value.
 */
function escapeCsvField(value) {
  if (value.includes(",") || value.includes('"') || value.includes("\n")) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

/**
 * Generate CSV file with mirror validation errors.
 *
 * @param {string[]} sources - Array of source strings.
 * @param {string} tmpDir - Temp directory.
 * @param {string} dataGroupCid - CID of the datagroup.
 * @returns {Promise<string>} - Path to generated CSV.
 */
async function generateMirrorValidationErrorsCsv(sources, tmpDir, dataGroupCid) {
  const csvPath = path.join(tmpDir, "mvl_errors.csv");
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
 * Try to download static parts CSV file for a county from S3.
 *
 * @param {string} county - County name.
 * @param {string} tmpDir - Temp directory.
 * @param {ReturnType<typeof createLogger>} log - Logger.
 * @returns {Promise<string | undefined>} - Local file path if found.
 */
async function tryDownloadStaticParts(county, tmpDir, log) {
  const outputBaseUri = process.env.OUTPUT_BASE_URI;
  if (!outputBaseUri) return undefined;

  const { bucket } = parseS3Uri(outputBaseUri);
  const normalizedCounty = county.toLowerCase().replace(/\s+/g, "-");
  const staticPartsKey = `source-html-static-parts/${normalizedCounty}.csv`;

  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: staticPartsKey }));
    const staticCsvLocal = path.join(tmpDir, "static-parts.csv");
    await downloadS3Object({ bucket, key: staticPartsKey }, staticCsvLocal, log);
    log("info", "mvl_static_parts_downloaded", { county, s3_uri: `s3://${bucket}/${staticPartsKey}` });
    return staticCsvLocal;
  } catch (error) {
    const err = /** @type {Error & {$metadata?: {httpStatusCode?: number}}} */ (error);
    if (err.name !== "NotFound" && err.$metadata?.httpStatusCode !== 404) {
      log("error", "mvl_static_parts_error", { county, error: String(err) });
    }
    return undefined;
  }
}

/**
 * Run the MVL (Mirror Validation Layer) step.
 *
 * @param {object} params - MVL parameters.
 * @param {string} params.preparedInputS3Uri - S3 URI of prepared input zip.
 * @param {string} params.transformedOutputS3Uri - S3 URI of transformed output zip.
 * @param {string} params.county - County name.
 * @param {string} params.outputPrefix - Output S3 prefix.
 * @param {string} params.executionId - Execution ID.
 * @param {ReturnType<typeof createLogger>} params.log - Logger.
 * @returns {Promise<MvlOutput>}
 */
async function runMvl({ preparedInputS3Uri, transformedOutputS3Uri, county, outputPrefix, executionId, log }) {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "mvl-"));

  try {
    // Download inputs
    const preparedZipLocal = path.join(tmpDir, "prepared_input.zip");
    const transformedZipLocal = path.join(tmpDir, "transformed_output.zip");

    await Promise.all([
      downloadS3Object(parseS3Uri(preparedInputS3Uri), preparedZipLocal, log),
      downloadS3Object(parseS3Uri(transformedOutputS3Uri), transformedZipLocal, log),
    ]);

    // Try to get static parts file
    const staticCsvLocal = await tryDownloadStaticParts(county, tmpDir, log);

    log("info", "mvl_start", {
      operation: "mirror_validation",
      county,
      executionId,
      has_static_parts: !!staticCsvLocal,
    });

    const mvlStart = Date.now();

    /** @type {{prepareZip: string, transformZip: string, cwd: string, staticParts?: string}} */
    const mvlOptions = {
      prepareZip: preparedZipLocal,
      transformZip: transformedZipLocal,
      cwd: tmpDir,
    };
    if (staticCsvLocal) {
      mvlOptions.staticParts = staticCsvLocal;
    }

    const mvlResult = await mirrorValidate(mvlOptions);
    const mvlDuration = Date.now() - mvlStart;

    log("info", "mvl_complete", {
      operation: "mirror_validation",
      duration_ms: mvlDuration,
      duration_seconds: (mvlDuration / 1000).toFixed(2),
      success: mvlResult.success,
      globalCompleteness: mvlResult.globalCompleteness,
    });

    const { bucket: outputBucket } = parseS3Uri(outputPrefix);
    const baseKey = `${outputPrefix.replace(/^s3:\/\/[^/]+\//, "").replace(/\/$/, "")}/${executionId}`;

    // Upload MVL report
    const reportJson = JSON.stringify(mvlResult, null, 2);
    const reportS3Uri = await uploadContentToS3(
      reportJson,
      { bucket: outputBucket, key: `${baseKey}/mvl-report.json` },
      log,
      "application/json",
    );

    if (!mvlResult.success) {
      log("error", "mvl_failed", { error: mvlResult.error });
      const err = new Error(`Mirror validation failed: ${mvlResult.error}`);
      err.name = "MvlFailedError";
      throw err;
    }

    const globalCompleteness = mvlResult.globalCompleteness ?? 0;
    const mvlPassed = globalCompleteness >= 0.8;

    // If completeness is below threshold, generate errors CSV
    let errorsS3Uri = null;
    if (!mvlPassed && mvlResult.comparison) {
      const unmatchedSources = extractUnmatchedSources(
        /** @type {Record<string, { unmatchedFromA?: unknown[] }>} */ (mvlResult.comparison),
      );
      if (unmatchedSources.length > 0) {
        const errorsCsvPath = await generateMirrorValidationErrorsCsv(
          unmatchedSources,
          tmpDir,
          "bafkreigsipwhkwrboi73b3xvn4tjwd26pqyzz5zmyxvbnrgeb2qbq2bz34", // TODO: Dynamic CID
        );
        const errorsCsv = await fs.readFile(errorsCsvPath);
        errorsS3Uri = await uploadContentToS3(
          errorsCsv,
          { bucket: outputBucket, key: `${baseKey}/mvl_errors.csv` },
          log,
          "text/csv",
        );
      }

      log("error", "mvl_completeness_below_threshold", {
        global_completeness: globalCompleteness,
        threshold: 0.8,
      });

      const err = new Error(
        `Mirror validation failed: global completeness ${(globalCompleteness * 100).toFixed(2)}% is below threshold of 80.0%`,
      );
      err.name = "MvlFailedError";
      // @ts-ignore
      err.errorsS3Uri = errorsS3Uri;
      // @ts-ignore
      err.mvlMetric = globalCompleteness;
      throw err;
    }

    return {
      status: "success",
      mvlMetric: globalCompleteness,
      mvlPassed,
      county,
      executionId,
      reportS3Uri,
    };
  } finally {
    try {
      await fs.rm(tmpDir, { recursive: true, force: true });
    } catch {}
  }
}

/**
 * Lambda handler for MVL worker.
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
      component: "mvl-worker",
      at: new Date().toISOString(),
      county: input.county,
      executionId: input.executionId,
    });

    log("info", "mvl_worker_start", {
      preparedInputS3Uri: input.preparedInputS3Uri,
      transformedOutputS3Uri: input.transformedOutputS3Uri,
    });

    await executeWithTaskToken({
      taskToken,
      log,
      workerFn: async () => {
        return await runMvl({
          preparedInputS3Uri: input.preparedInputS3Uri,
          transformedOutputS3Uri: input.transformedOutputS3Uri,
          county: input.county,
          outputPrefix: input.outputPrefix,
          executionId: input.executionId,
          log,
        });
      },
    });

    log("info", "mvl_worker_complete", {});
  }
};
