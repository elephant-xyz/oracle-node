import { promises as fs } from "fs";
import path from "path";
import os from "os";
import { transform } from "@elephant-xyz/cli/lib";
import {
  executeWithTaskToken,
  parseS3Uri,
  downloadS3Object,
  uploadToS3,
  createLogger,
  emitWorkflowEvent,
  createWorkflowError,
} from "../shared/index.mjs";
import { createTransformScriptsManager } from "./scripts-manager.mjs";
import { S3Client } from "@aws-sdk/client-s3";

const s3Client = new S3Client({});

const PERSISTENT_TRANSFORM_CACHE_DIR = path.join(
  os.tmpdir(),
  "county-transforms",
);

const transformScriptsManager = createTransformScriptsManager({
  s3Client,
  persistentCacheRoot: PERSISTENT_TRANSFORM_CACHE_DIR,
});

/**
 * @typedef {object} TransformInput
 * @property {string} inputS3Uri - S3 URI of the input zip file (merged/prepared input).
 * @property {string} county - County name for transform scripts lookup.
 * @property {string} outputPrefix - S3 URI prefix for output files.
 * @property {string} executionId - Unique execution identifier.
 */

/**
 * @typedef {object} TransformOutput
 * @property {string} transformedOutputS3Uri - S3 URI of the transformed output zip.
 * @property {string} county - County name.
 * @property {string} executionId - Execution identifier.
 */

/**
 * @typedef {object} SQSMessageBody
 * @property {string} taskToken - Step Functions task token for callback.
 * @property {TransformInput} input - Transform input parameters.
 */

/**
 * Resolve the transform bucket/key pair for a given county.
 *
 * @param {{ countyName: string, transformPrefixUri: string | undefined }} params - Transform location configuration.
 * @returns {{ transformBucket: string, transformKey: string }} - Derived transform bucket and key.
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
 * Run the transform step for a county.
 *
 * @param {object} params - Transform parameters.
 * @param {string} params.inputS3Uri - S3 URI of input zip.
 * @param {string} params.county - County name.
 * @param {string} params.outputPrefix - Output S3 prefix.
 * @param {string} params.executionId - Execution ID.
 * @param {ReturnType<typeof createLogger>} params.log - Logger.
 * @returns {Promise<TransformOutput>}
 */
async function runTransform({
  inputS3Uri,
  county,
  outputPrefix,
  executionId,
  log,
}) {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "transform-"));

  try {
    // Download input zip
    const inputZipLocal = path.join(tmpDir, "input.zip");
    await downloadS3Object(parseS3Uri(inputS3Uri), inputZipLocal, log);

    // Get transform scripts
    const { transformBucket, transformKey } = resolveTransformLocation({
      countyName: county,
      transformPrefixUri: process.env.TRANSFORM_S3_PREFIX,
    });

    const { scriptsZipPath } =
      await transformScriptsManager.ensureScriptsForCounty({
        countyName: county,
        transformBucket,
        transformKey,
        log,
      });

    // Run transform
    const outputZipLocal = path.join(tmpDir, "transformed_output.zip");

    log("info", "transform_start", {
      operation: "transform",
      county,
      executionId,
    });

    const transformStart = Date.now();
    const transformResult = await transform({
      inputZip: inputZipLocal,
      outputZip: outputZipLocal,
      scriptsZip: scriptsZipPath,
      cwd: tmpDir,
    });

    const transformDuration = Date.now() - transformStart;

    if (!transformResult.success) {
      log("error", "transform_failed", {
        operation: "transform",
        duration_ms: transformDuration,
        error: transformResult.error,
        scriptFailures: transformResult.scriptFailure,
      });

      // Create a specific error for script failures
      if (transformResult.scriptFailure) {
        const err = new Error(transformResult.error || "Transform scripts failed");
        err.name = "ScriptsFailedError";
        throw err;
      }

      throw new Error(transformResult.error || "Transform failed");
    }

    log("info", "transform_complete", {
      operation: "transform",
      duration_ms: transformDuration,
      duration_seconds: (transformDuration / 1000).toFixed(2),
    });

    // Upload transformed output to S3
    const { bucket: outputBucket } = parseS3Uri(outputPrefix);
    const outputKey = `${outputPrefix.replace(/^s3:\/\/[^/]+\//, "").replace(/\/$/, "")}/${executionId}/transformed_output.zip`;

    const transformedOutputS3Uri = await uploadToS3(
      outputZipLocal,
      { bucket: outputBucket, key: outputKey },
      log,
    );

    return {
      transformedOutputS3Uri,
      county,
      executionId,
    };
  } finally {
    // Cleanup
    try {
      await fs.rm(tmpDir, { recursive: true, force: true });
    } catch {}
  }
}

/**
 * Lambda handler for Transform worker.
 * Triggered by SQS messages from the Step Functions workflow.
 *
 * @param {import("aws-lambda").SQSEvent} event - SQS event containing messages.
 * @returns {Promise<void>}
 */
export const handler = async (event) => {
  // Process each SQS message (typically batch size is 1 for task token pattern)
  for (const record of event.Records) {
    /** @type {SQSMessageBody} */
    const messageBody = JSON.parse(record.body);
    const { taskToken, input } = messageBody;

    const log = createLogger({
      component: "transform-worker",
      at: new Date().toISOString(),
      county: input.county,
      executionId: input.executionId,
    });

    log("info", "transform_worker_start", {
      inputS3Uri: input.inputS3Uri,
    });

    // Emit IN_PROGRESS event
    await emitWorkflowEvent({
      executionId: input.executionId,
      county: input.county,
      status: "IN_PROGRESS",
      phase: "Transform",
      step: "Transform",
      taskToken,
      log,
    });

    try {
      const result = await runTransform({
        inputS3Uri: input.inputS3Uri,
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
        phase: "Transform",
        step: "Transform",
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
        phase: "Transform",
        step: "Transform",
        taskToken,
        errors: [
          createWorkflowError(
            err instanceof Error && err.name === "ScriptsFailedError"
              ? "TRANSFORM_SCRIPTS_FAILED"
              : "TRANSFORM_FAILED",
            { message: err instanceof Error ? err.message : String(err) },
          ),
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

    log("info", "transform_worker_complete", {});
  }
};
