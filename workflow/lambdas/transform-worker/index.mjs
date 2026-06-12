import { promises as fs } from "fs";
import path from "path";
import os from "os";
import AdmZip from "adm-zip";
import { transform } from "@elephant-xyz/cli/lib";
import {
  executeWithTaskToken,
  parseS3Uri,
  downloadS3Object,
  uploadToS3,
  createLogger,
  emitWorkflowEvent,
} from "shared";
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

const DEFAULT_PROPERTY_FIRST_PERMIT_ELIGIBLE_USAGE_TYPES = [
  "AutoSalesRepair",
  "Commercial",
  "DepartmentStore",
  "FinancialInstitution",
  "Hotel",
  "Industrial",
  "LightManufacturing",
  "MobileHomePark",
  "OfficeBuilding",
  "PrivateHospital",
  "Restaurant",
  "ShoppingCenterCommunity",
  "ShoppingCenterRegional",
  "Supermarket",
];

/**
 * @typedef {object} TransformInput
 * @property {string} inputS3Uri - S3 URI of the input zip file (merged/prepared input).
 * @property {string} county - County name for transform scripts lookup.
 * @property {string} outputPrefix - S3 URI prefix for output files.
 * @property {string} executionId - Unique execution identifier.
 */

/**
 * @typedef {object} PropertyFirstPermitEligibility
 * @property {boolean} shouldEnqueue - Whether the appraisal property should be sent to Accela permit retrieval.
 * @property {string} reason - Stable reason for the routing decision.
 * @property {string | null} propertyUsageType - Appraiser usage type read from transformed `data/property.json`.
 * @property {string[]} eligibleUsageTypes - Usage types treated as commercial/permit-priority for this run.
 * @property {string} manifestS3Uri - S3 URI of the persisted routing decision manifest.
 */

/**
 * @typedef {object} DirectInvocationInput
 * @property {string} inputS3Uri - S3 URI of the input zip file.
 * @property {string} county - County name.
 * @property {string} outputPrefix - S3 URI prefix for output files.
 * @property {string} executionId - Execution identifier.
 * @property {boolean} [directInvocation] - If true, skip EventBridge events and task token handling.
 */

/**
 * @typedef {object} TransformOutput
 * @property {string} transformedOutputS3Uri - S3 URI of the transformed output zip.
 * @property {string} county - County name.
 * @property {string} executionId - Execution identifier.
 * @property {PropertyFirstPermitEligibility} propertyFirstPermitEligibility - Property-first permit routing decision.
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
 * Return true when an unknown JSON value is a string-keyed object.
 *
 * @param {unknown} value - Candidate JSON value.
 * @returns {value is Record<string, unknown>} True for non-array objects.
 */
function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Read a trimmed string from an unknown JSON field.
 *
 * @param {unknown} value - Candidate field value.
 * @returns {string | null} Trimmed non-empty string, or null.
 */
function readOptionalString(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/**
 * Resolve appraisal usage types that should continue to property-first permit retrieval.
 *
 * @returns {string[]} Commercial/permit-priority appraiser usage types.
 */
function resolvePropertyFirstPermitEligibleUsageTypes() {
  const raw = process.env.PROPERTY_FIRST_PERMIT_ELIGIBLE_USAGE_TYPES;
  if (raw === undefined || raw.trim().length === 0) {
    return [...DEFAULT_PROPERTY_FIRST_PERMIT_ELIGIBLE_USAGE_TYPES];
  }
  const values = raw
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
  return values.length > 0
    ? values
    : [...DEFAULT_PROPERTY_FIRST_PERMIT_ELIGIBLE_USAGE_TYPES];
}

/**
 * Extract the Lee Appraiser property usage type from a transformed output zip.
 *
 * @param {string} outputZipLocal - Local transformed output zip path.
 * @returns {{ propertyUsageType: string | null, readError: string | null }} Extracted usage type and optional read error.
 */
function readPropertyUsageTypeFromTransformedZip(outputZipLocal) {
  try {
    const zip = new AdmZip(outputZipLocal);
    const propertyEntry = zip.getEntry("data/property.json");
    if (propertyEntry === null) {
      return {
        propertyUsageType: null,
        readError: "missing data/property.json in transformed output",
      };
    }
    const propertyJson = JSON.parse(propertyEntry.getData().toString("utf8"));
    if (!isRecord(propertyJson)) {
      return {
        propertyUsageType: null,
        readError: "data/property.json is not an object",
      };
    }
    return {
      propertyUsageType: readOptionalString(propertyJson.property_usage_type),
      readError: null,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return { propertyUsageType: null, readError: message };
  }
}

/**
 * Build and upload the property-first permit routing manifest for a transformed appraisal artifact.
 *
 * @param {object} params - Eligibility manifest parameters.
 * @param {string} params.outputZipLocal - Local transformed output zip path.
 * @param {string} params.outputBucket - S3 bucket containing the transformed output.
 * @param {string} params.outputKey - S3 key for `transformed_output.zip`.
 * @param {string} params.transformedOutputS3Uri - S3 URI for `transformed_output.zip`.
 * @param {ReturnType<typeof createLogger>} params.log - Structured logger.
 * @returns {Promise<PropertyFirstPermitEligibility>} Persisted routing decision.
 */
async function buildAndUploadPropertyFirstPermitEligibility({
  outputZipLocal,
  outputBucket,
  outputKey,
  transformedOutputS3Uri,
  log,
}) {
  const eligibleUsageTypes = resolvePropertyFirstPermitEligibleUsageTypes();
  const eligibleUsageTypeSet = new Set(eligibleUsageTypes);
  const { propertyUsageType, readError } =
    readPropertyUsageTypeFromTransformedZip(outputZipLocal);
  const shouldEnqueue =
    propertyUsageType !== null && eligibleUsageTypeSet.has(propertyUsageType);
  const reason = shouldEnqueue
    ? "eligible_property_usage_type"
    : readError !== null
      ? "property_usage_type_read_failed"
      : propertyUsageType === null
        ? "missing_property_usage_type"
        : "non_commercial_property_usage_type";
  const manifestKey = outputKey.replace(
    /transformed_output\.zip$/,
    "property_first_permit_eligibility.json",
  );
  const manifestLocalPath = path.join(
    path.dirname(outputZipLocal),
    "property_first_permit_eligibility.json",
  );
  const manifest = {
    schemaVersion: "oracle-node.property-first-permit-eligibility.v1",
    evaluatedAt: new Date().toISOString(),
    shouldEnqueue,
    reason,
    propertyUsageType,
    eligibleUsageTypes,
    transformedOutputS3Uri,
    ...(readError === null ? {} : { readError }),
  };
  await fs.writeFile(manifestLocalPath, JSON.stringify(manifest, null, 2));
  const manifestS3Uri = await uploadToS3(
    manifestLocalPath,
    { bucket: outputBucket, key: manifestKey },
    log,
    "application/json; charset=utf-8",
  );
  log("info", "property_first_permit_eligibility_resolved", {
    shouldEnqueue,
    reason,
    propertyUsageType,
    manifestS3Uri,
  });
  return {
    shouldEnqueue,
    reason,
    propertyUsageType,
    eligibleUsageTypes,
    manifestS3Uri,
  };
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
        // Include stderr in the error cause for Step Functions to capture
        const errorCause = {
          message: transformResult.error || "Transform scripts failed",
          stderr: transformResult.scriptFailure.stderr,
        };
        const err = new Error(JSON.stringify(errorCause));
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
    const propertyFirstPermitEligibility =
      await buildAndUploadPropertyFirstPermitEligibility({
        outputZipLocal,
        outputBucket,
        outputKey,
        transformedOutputS3Uri,
        log,
      });

    return {
      transformedOutputS3Uri,
      county,
      executionId,
      propertyFirstPermitEligibility,
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
 * Supports two invocation modes:
 * 1. SQS trigger from Step Functions workflow (with task token)
 * 2. Direct invocation from error-resolver/auto-repair (with directInvocation flag)
 *
 * @param {import("aws-lambda").SQSEvent | DirectInvocationInput} event - SQS event or direct invocation input.
 * @returns {Promise<void | TransformOutput>}
 */
export const handler = async (event) => {
  // Check if this is a direct invocation (from error-resolver/auto-repair)
  if ("directInvocation" in event && event.directInvocation) {
    const input = /** @type {DirectInvocationInput} */ (event);

    const log = createLogger({
      component: "transform-worker",
      at: new Date().toISOString(),
      county: input.county,
      executionId: input.executionId,
    });

    log("info", "transform_worker_start_direct", {
      inputS3Uri: input.inputS3Uri,
      directInvocation: true,
    });

    // Direct invocation: skip EventBridge events and task token handling
    const result = await runTransform({
      inputS3Uri: input.inputS3Uri,
      county: input.county,
      outputPrefix: input.outputPrefix,
      executionId: input.executionId,
      log,
    });

    log("info", "transform_worker_complete_direct", {});

    return result;
  }

  // SQS trigger mode: process each message with task token pattern
  const sqsEvent = /** @type {import("aws-lambda").SQSEvent} */ (event);

  for (const record of sqsEvent.Records) {
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

    log("info", "transform_worker_complete", {});
  }
};
