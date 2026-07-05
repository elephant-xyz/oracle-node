#!/usr/bin/env node
/**
 * redrive-orange-transform-only.mjs
 *
 * Checkpointed transform-only redrive driver for the Orange County
 * orange-property-first-seed-all-20260702 job.
 *
 * Enumerates every row-* folder that has output.zip (prepare done) but has no
 * <uuid>/transformed_output.zip (transform missing), then direct-invokes the
 * TransformWorkerFunction for each parcel with concurrency control.
 *
 * Usage:
 *   AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \
 *     node scripts/redrive-orange-transform-only.mjs --dry-run
 *
 *   # Live test — 20 parcels
 *   AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \
 *     node scripts/redrive-orange-transform-only.mjs --limit 20
 *
 *   # Full run (detached, log to .redrive-logs/)
 *   nohup env AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \
 *     node scripts/redrive-orange-transform-only.mjs \
 *     > .redrive-logs/transform-redrive.log 2>&1 &
 */
import { fileURLToPath } from "url";
import { promises as fs } from "fs";
import { randomUUID } from "crypto";
import {
  S3Client,
  ListObjectsV2Command,
  HeadObjectCommand,
  PutObjectCommand,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { Agent as HttpsAgent } from "https";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_REGION = process.env.AWS_REGION ?? "us-east-1";
const DEFAULT_BUCKET = "elephant-oracle-node-environmentbucket-mmsoo3xbdi80";
const DEFAULT_JOB_ID = "orange-property-first-seed-all-20260702";
const DEFAULT_CONCURRENCY = 15;
const DEFAULT_CHECKPOINT_EVERY = 50;
const DEFAULT_TRANSFORM_FUNCTION =
  "elephant-oracle-node-TransformWorkerFunction-LLr08PR6lyg3";

const DEFAULT_OUTPUTS_PREFIX = `outputs/orange-property-first-seed/${DEFAULT_JOB_ID}`;
const DEFAULT_STATE_S3_URI = `s3://${DEFAULT_BUCKET}/permit-harvest/${DEFAULT_JOB_ID}/transform-redrive-state.json`;

const COUNTY = "Orange";

// ---------------------------------------------------------------------------
// AWS clients
// ---------------------------------------------------------------------------

// maxSockets must exceed --concurrency, else the SDK's default 50-socket pool
// throttles high-concurrency Lambda invocations (they queue instead of dispatch).
// maxSockets lives on the https Agent, not as a top-level NodeHttpHandler option.
const requestHandler = new NodeHttpHandler({
  httpsAgent: new HttpsAgent({ maxSockets: 512, keepAlive: true }),
});
const s3 = new S3Client({ region: DEFAULT_REGION, requestHandler });
const lambda = new LambdaClient({ region: DEFAULT_REGION, requestHandler });

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------

function usage() {
  console.log(`
Usage:
  AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \\
    node scripts/redrive-orange-transform-only.mjs [options]

Options:
  --bucket <name>          S3 bucket. Default: ${DEFAULT_BUCKET}
  --job-id <id>            Job id prefix. Default: ${DEFAULT_JOB_ID}
  --transform-fn <name>    Lambda function name/ARN. Default: ${DEFAULT_TRANSFORM_FUNCTION}
  --state-s3-uri <uri>     Checkpoint S3 URI. Default: ${DEFAULT_STATE_S3_URI}
  --concurrency <n>        Max parallel Lambda invocations. Default: ${DEFAULT_CONCURRENCY}
  --limit <n>              Stop after N parcels (0 = unlimited). Default: 0
  --checkpoint-every <n>   Write checkpoint every N completed parcels. Default: ${DEFAULT_CHECKPOINT_EVERY}
  --rows-file <path>       Use a precomputed newline-delimited list of row folders
                           instead of a full S3 scan.
  --dry-run                Enumerate targets without invoking Lambda.
  --reset-checkpoint       Ignore existing checkpoint and start from the beginning.
  --help                   Show this help.
`);
}

function parseArgs(argv) {
  const values = {};
  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];
    if (token === "--help" || token === "-h") {
      usage();
      process.exit(0);
    }
    if (token === "--dry-run" || token === "--reset-checkpoint") {
      values[token.slice(2)] = true;
      continue;
    }
    if (!token.startsWith("--")) {
      throw new Error(`Unexpected argument: ${token}`);
    }
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) {
      throw new Error(`Missing value for ${token}`);
    }
    values[token.slice(2)] = next;
    i += 1;
  }

  const jobId = String(values["job-id"] ?? DEFAULT_JOB_ID);
  const bucket = String(values.bucket ?? DEFAULT_BUCKET);

  return {
    bucket,
    jobId,
    outputsPrefix: `outputs/orange-property-first-seed/${jobId}`,
    transformFn: String(values["transform-fn"] ?? DEFAULT_TRANSFORM_FUNCTION),
    stateS3Uri: String(
      values["state-s3-uri"] ??
        `s3://${bucket}/permit-harvest/${jobId}/transform-redrive-state.json`,
    ),
    concurrency: parseInteger(
      values.concurrency,
      DEFAULT_CONCURRENCY,
      "--concurrency",
      1,
    ),
    limit: parseInteger(values.limit, 0, "--limit", 0),
    checkpointEvery: parseInteger(
      values["checkpoint-every"],
      DEFAULT_CHECKPOINT_EVERY,
      "--checkpoint-every",
      1,
    ),
    dryRun: values["dry-run"] === true,
    resetCheckpoint: values["reset-checkpoint"] === true,
    rowsFile: values["rows-file"] ? String(values["rows-file"]) : undefined,
  };
}

function parseInteger(raw, fallback, name, min) {
  if (raw === undefined) return fallback;
  const parsed = Number.parseInt(String(raw), 10);
  if (!Number.isFinite(parsed) || parsed < min) {
    throw new Error(`${name} must be an integer >= ${min}`);
  }
  return parsed;
}

// ---------------------------------------------------------------------------
// S3 helpers
// ---------------------------------------------------------------------------

function parseS3Uri(uri) {
  const match = /^s3:\/\/([^/]+)\/(.+)$/i.exec(uri);
  if (!match) throw new Error(`Invalid S3 URI: ${uri}`);
  return { bucket: match[1], key: match[2].replace(/\/$/, "") };
}

async function objectExists(bucket, key) {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    return true;
  } catch (error) {
    const status = error?.$metadata?.httpStatusCode;
    if (
      status === 404 ||
      error?.name === "NoSuchKey" ||
      error?.name === "NotFound"
    ) {
      return false;
    }
    throw error;
  }
}

async function putJson(bucket, key, data) {
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: JSON.stringify(
        { ...data, updatedAt: new Date().toISOString() },
        null,
        2,
      ),
      ContentType: "application/json; charset=utf-8",
    }),
  );
}

async function getJson(bucket, key) {
  const response = await s3.send(
    new GetObjectCommand({ Bucket: bucket, Key: key }),
  );
  const body = await response.Body?.transformToString();
  if (body === undefined)
    throw new Error(`Empty S3 body: s3://${bucket}/${key}`);
  return JSON.parse(body);
}

// ---------------------------------------------------------------------------
// Checkpoint
// ---------------------------------------------------------------------------

const SCHEMA_VERSION = "oracle-node.orange-transform-only-redrive.v1";

function createInitialCheckpoint() {
  return {
    schemaVersion: SCHEMA_VERSION,
    // Set of row-folder keys that are done (written as JSON array for portability)
    doneRows: [],
    processedCount: 0,
    succeededCount: 0,
    failedCount: 0,
    verifiedCount: 0,
    failures: [],
  };
}

async function readCheckpoint(stateS3Uri, resetCheckpoint) {
  const { bucket, key } = parseS3Uri(stateS3Uri);
  if (resetCheckpoint || !(await objectExists(bucket, key))) {
    return createInitialCheckpoint();
  }
  const raw = await getJson(bucket, key);
  return {
    schemaVersion: SCHEMA_VERSION,
    doneRows: Array.isArray(raw.doneRows) ? raw.doneRows : [],
    processedCount: Number.isFinite(raw.processedCount)
      ? raw.processedCount
      : 0,
    succeededCount: Number.isFinite(raw.succeededCount)
      ? raw.succeededCount
      : 0,
    failedCount: Number.isFinite(raw.failedCount) ? raw.failedCount : 0,
    verifiedCount: Number.isFinite(raw.verifiedCount) ? raw.verifiedCount : 0,
    failures: Array.isArray(raw.failures) ? raw.failures.slice(-200) : [],
  };
}

async function writeCheckpoint(stateS3Uri, state) {
  const { bucket, key } = parseS3Uri(stateS3Uri);
  await putJson(bucket, key, state);
}

// ---------------------------------------------------------------------------
// Target enumeration
// ---------------------------------------------------------------------------

/**
 * List all row-folder prefixes under outputsPrefix that contain output.zip
 * but have no <uuid>/transformed_output.zip.
 *
 * Strategy: flat S3 listing of all keys under the prefix, then group by row folder,
 * classify each row folder, and return the targets.
 *
 * Authoritative count — this is the single source of truth for what needs redrive.
 */
async function enumerateTargets(bucket, outputsPrefix, doneRowSet) {
  const prefix = `${outputsPrefix}/`;
  let continuationToken = undefined;

  // row key -> { hasOutputZip, hasTransformedOutputZip }
  const rows = new Map();

  let pageCount = 0;
  console.log(`Enumerating S3 keys under s3://${bucket}/${prefix} ...`);

  do {
    const response = await s3.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      }),
    );

    for (const obj of response.Contents ?? []) {
      const key = obj.Key;
      if (!key) continue;

      // Extract row folder: first path segment after the prefix
      // e.g. outputs/.../row-000001-folio-xxx-parcel-yyy/...
      const relative = key.slice(prefix.length);
      const slashIdx = relative.indexOf("/");
      if (slashIdx === -1) continue; // top-level file, skip
      const rowFolder = relative.slice(0, slashIdx);
      const rest = relative.slice(slashIdx + 1);

      if (!rows.has(rowFolder)) {
        rows.set(rowFolder, {
          hasOutputZip: false,
          hasTransformedOutputZip: false,
        });
      }
      const entry = rows.get(rowFolder);

      if (rest === "output.zip") {
        entry.hasOutputZip = true;
      }
      // transformed_output.zip is nested one UUID level deep: <uuid>/transformed_output.zip
      if (rest.endsWith("/transformed_output.zip")) {
        entry.hasTransformedOutputZip = true;
      }
    }

    pageCount += 1;
    if (pageCount % 50 === 0) {
      console.log(
        `  ... ${pageCount} pages scanned, ${rows.size} row folders seen so far`,
      );
    }

    continuationToken = response.IsTruncated
      ? response.NextContinuationToken
      : undefined;
  } while (continuationToken !== undefined);

  console.log(
    `Enumeration complete: ${rows.size} total row folders across ${pageCount} pages`,
  );

  const targets = [];
  for (const [rowFolder, { hasOutputZip, hasTransformedOutputZip }] of rows) {
    if (hasOutputZip && !hasTransformedOutputZip) {
      if (!doneRowSet.has(rowFolder)) {
        targets.push(rowFolder);
      }
    }
  }

  return targets;
}

// ---------------------------------------------------------------------------
// Lambda invocation
// ---------------------------------------------------------------------------

/**
 * Direct-invoke the transform worker for a single row folder.
 * Returns the execution id used so the caller can verify the output.
 */
async function invokeTransform(bucket, outputsPrefix, rowFolder, transformFn) {
  const executionId = randomUUID();
  const inputS3Uri = `s3://${bucket}/${outputsPrefix}/${rowFolder}/output.zip`;
  const outputPrefix = `s3://${bucket}/${outputsPrefix}/${rowFolder}`;

  const payload = {
    inputS3Uri,
    county: COUNTY,
    outputPrefix,
    executionId,
    directInvocation: true,
  };

  const response = await lambda.send(
    new InvokeCommand({
      FunctionName: transformFn,
      InvocationType: "RequestResponse",
      Payload: JSON.stringify(payload),
    }),
  );

  if (response.FunctionError) {
    const errorPayload = JSON.parse(
      new TextDecoder().decode(response.Payload ?? new Uint8Array()),
    );
    throw new Error(
      `Lambda FunctionError: ${errorPayload.errorMessage || errorPayload.errorType || JSON.stringify(errorPayload)}`,
    );
  }

  const result = JSON.parse(
    new TextDecoder().decode(response.Payload ?? new Uint8Array()),
  );

  return { executionId, result };
}

/**
 * Verify that transformed_output.zip was written for the given row folder.
 * The transform worker writes to <outputPrefix>/<executionId>/transformed_output.zip.
 */
async function verifyTransformOutput(
  bucket,
  outputsPrefix,
  rowFolder,
  executionId,
) {
  const key = `${outputsPrefix}/${rowFolder}/${executionId}/transformed_output.zip`;
  return objectExists(bucket, key);
}

// ---------------------------------------------------------------------------
// Concurrency pool
// ---------------------------------------------------------------------------

/**
 * Process an array of tasks with a bounded concurrency pool.
 * Each task is a zero-arg async function. Results are collected in order.
 */
async function runWithConcurrency(tasks, concurrency, onComplete) {
  const results = [];
  let index = 0;

  async function worker() {
    while (index < tasks.length) {
      const i = index++;
      try {
        const result = await tasks[i]();
        results[i] = { ok: true, value: result };
      } catch (error) {
        results[i] = {
          ok: false,
          error: error instanceof Error ? error.message : String(error),
        };
      }
      if (onComplete) onComplete(i, results[i]);
    }
  }

  const workers = Array.from(
    { length: Math.min(concurrency, tasks.length) },
    () => worker(),
  );
  await Promise.all(workers);
  return results;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const options = parseArgs(process.argv.slice(2));

  console.log(
    JSON.stringify(
      {
        mode: options.dryRun ? "dry-run" : "live",
        bucket: options.bucket,
        jobId: options.jobId,
        outputsPrefix: options.outputsPrefix,
        transformFn: options.transformFn,
        stateS3Uri: options.stateS3Uri,
        concurrency: options.concurrency,
        limit: options.limit,
        checkpointEvery: options.checkpointEvery,
        resetCheckpoint: options.resetCheckpoint,
      },
      null,
      2,
    ),
  );

  // Load checkpoint
  const state = await readCheckpoint(
    options.stateS3Uri,
    options.resetCheckpoint,
  );
  const doneRowSet = new Set(state.doneRows);
  console.log(
    `Checkpoint loaded: ${doneRowSet.size} already done, ${state.processedCount} total processed so far`,
  );

  // Enumerate targets — either from a precomputed rows file or by full S3 scan.
  let allTargets;
  if (options.rowsFile) {
    const raw = await fs.readFile(options.rowsFile, "utf8");
    allTargets = raw
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0 && !doneRowSet.has(line));
    console.log(
      `Loaded ${allTargets.length} targets from rows file ${options.rowsFile} (excluding ${doneRowSet.size} already done)`,
    );
  } else {
    allTargets = await enumerateTargets(
      options.bucket,
      options.outputsPrefix,
      doneRowSet,
    );
  }
  console.log(
    `Targets remaining (output.zip present, no transformed_output.zip, not done): ${allTargets.length}`,
  );

  if (options.dryRun) {
    console.log(
      JSON.stringify(
        {
          dryRun: true,
          targetsRemaining: allTargets.length,
          alreadyDone: doneRowSet.size,
          checkpointUri: options.stateS3Uri,
          estimatedDurationHours:
            Math.ceil(
              ((allTargets.length * 30) / options.concurrency / 3600) * 10,
            ) / 10,
        },
        null,
        2,
      ),
    );
    return;
  }

  // Apply --limit
  const targets =
    options.limit > 0 ? allTargets.slice(0, options.limit) : allTargets;
  console.log(
    `Will process ${targets.length} parcels at concurrency ${options.concurrency}`,
  );

  let completedCount = 0;

  const tasks = targets.map((rowFolder) => async () => {
    const { executionId } = await invokeTransform(
      options.bucket,
      options.outputsPrefix,
      rowFolder,
      options.transformFn,
    );

    const verified = await verifyTransformOutput(
      options.bucket,
      options.outputsPrefix,
      rowFolder,
      executionId,
    );

    return { rowFolder, executionId, verified };
  });

  // Process with concurrency pool, updating checkpoint as we go
  const results = await runWithConcurrency(
    tasks,
    options.concurrency,
    async (taskIndex, result) => {
      const rowFolder = targets[taskIndex];
      completedCount += 1;

      if (result.ok) {
        const { verified } = result.value;
        state.doneRows.push(rowFolder);
        state.processedCount += 1;
        state.succeededCount += 1;
        if (verified) state.verifiedCount += 1;

        if (completedCount % 100 === 0) {
          console.log(
            `[${completedCount}/${targets.length}] ok row=${rowFolder} verified=${verified}`,
          );
        }
      } else {
        state.processedCount += 1;
        state.failedCount += 1;
        state.failures = [
          ...state.failures,
          { rowFolder, error: result.error, at: new Date().toISOString() },
        ].slice(-200);
        console.error(
          `[${completedCount}/${targets.length}] FAIL row=${rowFolder} error=${result.error}`,
        );
      }

      if (completedCount % options.checkpointEvery === 0) {
        await writeCheckpoint(options.stateS3Uri, state);
      }
    },
  );

  // Final checkpoint
  await writeCheckpoint(options.stateS3Uri, state);

  const succeeded = results.filter((r) => r.ok).length;
  const failed = results.filter((r) => !r.ok).length;
  const verified = results.filter((r) => r.ok && r.value.verified).length;

  const summary = {
    targetsAttempted: targets.length,
    succeeded,
    failed,
    verified,
    unverified: succeeded - verified,
    checkpointUri: options.stateS3Uri,
    failures: state.failures.slice(-20),
  };

  console.log(JSON.stringify(summary, null, 2));

  if (failed > 0) {
    process.exitCode = 1;
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
