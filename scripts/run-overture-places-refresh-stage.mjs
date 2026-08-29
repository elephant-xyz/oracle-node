#!/usr/bin/env node

/**
 * Stage runner used by the Overture places Step Functions workflow.
 *
 * Fargate callback stages return compact JSON directly to Step Functions via a
 * task token. Local invocation prints the same JSON. No stage logs secrets.
 */

import {
  SendTaskFailureCommand,
  SendTaskSuccessCommand,
  SFNClient,
} from "@aws-sdk/client-sfn";
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { mkdir, readFile, readdir, rm } from "node:fs/promises";
import { readFileSync } from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import pg from "pg";

import { runExport } from "./export-overture-places-table.mjs";
import { discoverRelease, runExtract } from "./extract-overture-places.mjs";
import { runIncrementalPlacesExtract } from "./extract-overture-places-changelog.mjs";
import {
  buildRefreshPlan,
  parseRefreshInput,
} from "./overture-places-refresh-lib.mjs";

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * @typedef {"plan" | "extract" | "validate" | "export" | "verify" | "finalize"} RefreshStage
 */

/**
 * @typedef {object} RefreshPlan
 * @property {"noop" | "full" | "incremental"} action Planned path.
 * @property {string} county County key.
 * @property {string} countyFips Five-digit FIPS.
 * @property {string} boundarySource Boundary source.
 * @property {string} release Pinned release.
 * @property {string | null} previousRelease Previous durable release.
 * @property {boolean} dryRun Dry-run flag.
 * @property {string} idempotencyKey County/release key.
 * @property {string} workBucket Internal artifact bucket.
 * @property {string} runPrefix Internal artifact prefix.
 * @property {string} runId Operator run identifier.
 */

/**
 * Execute one workflow stage.
 *
 * @param {RefreshStage} stage Stage name.
 * @param {JsonObject} input Step Functions or local input.
 * @param {string | null} envFile Optional dotenv file for local validation.
 * @returns {Promise<JsonObject>} Stage output.
 */
export async function runRefreshStage(stage, input, envFile = null) {
  if (envFile !== null) loadEnvFile(envFile);
  if (stage === "plan") return runPlanStage(input);
  if (stage === "extract") return runExtractStage(input);
  if (stage === "validate") return runValidateStage(input);
  if (stage === "export") return runExportStage(input);
  if (stage === "verify") return runVerifyStage(input);
  if (stage === "finalize") return runFinalizeStage(input);
  throw new Error(`Unsupported Overture refresh stage: ${stage}`);
}

/**
 * Resolve STAC and Neon durable successful state without mutation.
 *
 * @param {JsonObject} input Untrusted workflow input.
 * @returns {Promise<JsonObject>} Read-only pinned plan.
 */
async function runPlanStage(input) {
  const refreshInput = parseRefreshInput(input);
  const discovery = await discoverRelease(
    typeof input.stacCatalogUrl === "string"
      ? input.stacCatalogUrl
      : "https://stac.overturemaps.org/catalog.json",
  );
  const databaseUrl = requireDatabaseUrl();
  const lastSuccessfulRelease = await readLastSuccessfulRelease({
    databaseUrl,
    county: refreshInput.county,
  });
  const plan = buildRefreshPlan({
    input: refreshInput,
    latestRelease: discovery.latest,
    lastSuccessfulRelease,
  });
  const workBucket = requireString(input.workBucket, "workBucket");
  const runId =
    refreshInput.runId ??
    `manual-${new Date().toISOString().replaceAll(/[:.]/g, "-")}`;
  const runPrefix = [
    "overture-places-refresh",
    "runs",
    refreshInput.county,
    plan.release,
    sanitizeRunId(runId),
  ].join("/");
  const lockNowEpoch = Math.floor(Date.now() / 1_000);
  return {
    ...plan,
    stac: discovery,
    workBucket,
    runId,
    runPrefix,
    publishApproved: input.publishApproved === true,
    lockNowEpoch,
    lockExpiresAtEpoch: lockNowEpoch + 21_600,
    sourceOfTruth: "neon:overture_place_extractions.run_status=succeeded",
    zeroMutation: true,
  };
}

/**
 * Run the full-first-load or changelog extraction.
 *
 * @param {JsonObject} input Stage input containing `plan`.
 * @returns {Promise<JsonObject>} Extract summary and S3 URI.
 */
async function runExtractStage(input) {
  const plan = readPlan(input);
  if (plan.action === "noop") {
    throw new Error("Extract stage cannot run for a no-op plan");
  }
  if (plan.dryRun === true) {
    throw new Error("Extract stage refuses dryRun=true");
  }
  const databaseUrl = requireDatabaseUrl();
  const outputS3Uri = `s3://${plan.workBucket}/${plan.runPrefix}/`;
  if (plan.action === "full") {
    const summary = await runExtract([
      "--county",
      plan.county,
      "--county-fips",
      plan.countyFips,
      "--county-name",
      titleCaseCounty(plan.county),
      "--release",
      plan.release,
      "--boundary-source",
      plan.boundarySource,
      "--hosted-service-list",
      ".agents/skills/overture-places-ingest/config/hosted-service-categories.txt",
      "--cache-dir",
      "/tmp/overture-places/cache",
      "--output-s3-uri",
      outputS3Uri,
      "--no-keep-parquet",
    ]);
    return { ...summary, artifactS3Uri: outputS3Uri };
  }
  if (typeof plan.previousRelease !== "string") {
    throw new Error("Incremental plan is missing previousRelease");
  }
  return runIncrementalPlacesExtract({
    county: plan.county,
    countyFips: plan.countyFips,
    countyName: titleCaseCounty(plan.county),
    release: plan.release,
    previousRelease: plan.previousRelease,
    boundarySource: plan.boundarySource,
    cacheDir: "/tmp/overture-places/cache",
    hostedServiceListPath:
      ".agents/skills/overture-places-ingest/config/hosted-service-categories.txt",
    workDir: `/tmp/overture-places/${sanitizeRunId(plan.runId)}`,
    outputBucket: plan.workBucket,
    outputPrefix: plan.runPrefix,
    databaseUrl,
    partRecordLimit: 5_000,
  });
}

/**
 * Enforce incoming licence and taxonomy gates before Neon load.
 *
 * @param {JsonObject} input Stage input containing `extraction`.
 * @returns {Promise<JsonObject>} Gate report.
 */
async function runValidateStage(input) {
  const extraction = requireObject(input.extraction, "extraction");
  const licenceGate = requireObject(extraction.licenceGate, "licenceGate");
  if (licenceGate.passed !== true) {
    const error = new Error(
      typeof licenceGate.message === "string"
        ? licenceGate.message
        : "Incoming Overture licence gate failed",
    );
    error.name = "LicenceGateError";
    throw error;
  }
  const taxonomyDrift =
    extraction.taxonomyDrift === undefined
      ? { blocking: false, reasons: [] }
      : requireObject(extraction.taxonomyDrift, "taxonomyDrift");
  if (taxonomyDrift.blocking === true) {
    const reasons = Array.isArray(taxonomyDrift.reasons)
      ? taxonomyDrift.reasons.map(String).join("; ")
      : "hosted-service taxonomy drift requires review";
    const error = new Error(reasons);
    error.name = "TaxonomyDriftError";
    throw error;
  }
  return {
    passed: true,
    licenceGate,
    taxonomyDrift,
    activeChangeCount: Number(extraction.activeChangeCount ?? 0),
    deactivationCount: Number(extraction.deactivationCount ?? 0),
    validatedAt: new Date().toISOString(),
  };
}

/**
 * Export the full current county table from Neon, validate it, and stage the
 * publication artifact in internal AWS S3. This stage does not call Filebase.
 *
 * @param {JsonObject} input Stage input containing `plan`.
 * @returns {Promise<JsonObject>} Export report and internal artifact URI.
 */
async function runExportStage(input) {
  const plan = readPlan(input);
  const exportRoot = `/tmp/overture-places-export/${sanitizeRunId(plan.runId)}`;
  await rm(exportRoot, { recursive: true, force: true });
  await mkdir(exportRoot, { recursive: true });
  const report = await runExport([
    "--from-neon",
    "--county",
    plan.county,
    "--release",
    plan.release,
    "--out",
    exportRoot,
  ]);
  const prefix = `${plan.runPrefix}/publish`;
  await uploadDirectoryToS3({
    localRoot: exportRoot,
    bucket: plan.workBucket,
    prefix,
  });
  return {
    ...report,
    artifactS3Uri: `s3://${plan.workBucket}/${prefix}/`,
    published: false,
  };
}

/**
 * Verify the new stable IPNS target before Neon coverage is finalized.
 *
 * @param {JsonObject} input Stage input containing `publishResult`.
 * @returns {Promise<JsonObject>} Verified public pointer.
 */
async function runVerifyStage(input) {
  const publishResult = requireObject(input.publishResult, "publishResult");
  requireString(publishResult.cid, "publishResult.cid");
  requireString(publishResult.ipnsName, "publishResult.ipnsName");
  const gatewayUrls = requireObject(
    publishResult.gatewayUrls,
    "publishResult.gatewayUrls",
  );
  const parquetUrl = requireString(
    gatewayUrls.filebaseParquet,
    "publishResult.gatewayUrls.filebaseParquet",
  );
  const indexUrl = requireString(
    gatewayUrls.filebaseIndex,
    "publishResult.gatewayUrls.filebaseIndex",
  );
  const noticeUrl = requireString(
    gatewayUrls.filebaseNotice,
    "publishResult.gatewayUrls.filebaseNotice",
  );
  const parquetResponse = await fetch(parquetUrl, {
    headers: { Range: "bytes=0-3", "Cache-Control": "no-cache" },
  });
  if (!parquetResponse.ok) {
    throw new Error(
      `Published parquet verification returned HTTP ${parquetResponse.status}`,
    );
  }
  const parquetPrefix = Buffer.from(await parquetResponse.arrayBuffer())
    .subarray(0, 4)
    .toString("ascii");
  if (parquetPrefix !== "PAR1") {
    throw new Error("Published places artifact is not Parquet");
  }
  const [indexResponse, noticeResponse] = await Promise.all([
    fetch(indexUrl, { headers: { "Cache-Control": "no-cache" } }),
    fetch(noticeUrl, { headers: { "Cache-Control": "no-cache" } }),
  ]);
  if (!indexResponse.ok || !noticeResponse.ok) {
    throw new Error(
      `Published sidecar verification failed: index=${indexResponse.status}, NOTICE=${noticeResponse.status}`,
    );
  }
  const index = await indexResponse.json();
  const indexObject = requireObject(index, "published index");
  if (
    indexObject.artifact !== "places-table" ||
    typeof indexObject.rowCount !== "number"
  ) {
    throw new Error("Published places index has an invalid artifact/count");
  }
  const notice = await noticeResponse.text();
  if (!notice.includes("Overture Maps")) {
    throw new Error("Published NOTICE is missing Overture attribution");
  }
  return {
    ...publishResult,
    verified: true,
    verifiedRowCount: indexObject.rowCount,
    verifiedAt: new Date().toISOString(),
  };
}

/**
 * Mark the run successful only after public verification and update coverage
 * metadata as the final mutation.
 *
 * @param {JsonObject} input Stage input containing plan/publish result.
 * @returns {Promise<JsonObject>} Durable finalization record.
 */
async function runFinalizeStage(input) {
  const plan = readPlan(input);
  const publishResult = requireObject(input.publishResult, "publishResult");
  const cid = requireString(publishResult.cid, "publishResult.cid");
  const ipnsLabel = requireString(
    publishResult.ipnsLabel,
    "publishResult.ipnsLabel",
  );
  const ipnsName = requireString(
    publishResult.ipnsName,
    "publishResult.ipnsName",
  );
  const gatewayUrls = requireObject(
    publishResult.gatewayUrls,
    "publishResult.gatewayUrls",
  );
  const parquetUrl = requireString(
    gatewayUrls.filebaseParquet,
    "publishResult.gatewayUrls.filebaseParquet",
  );
  await assertCatalogPointerStable({
    county: plan.county,
    ipnsName,
    parquetUrl,
  });
  const databaseUrl = requireDatabaseUrl();
  const client = createPgClient(
    databaseUrl,
    "oracle-node-overture-refresh-finalize",
  );
  await client.connect();
  try {
    await client.query("BEGIN");
    const countResult = await client.query(
      `SELECT count(*)::int AS count
       FROM business_locations
       WHERE source_system = 'overture_places'
         AND county_key = $1
         AND is_current = true`,
      [plan.county],
    );
    const ingestedCount = Number(countResult.rows[0]?.count ?? 0);
    await client.query(
      `UPDATE overture_place_extractions
       SET run_status = 'succeeded',
           published_cid = $3,
           published_ipns_name = $4,
           finished_at = now(),
           updated_at = now()
       WHERE county_key = $1
         AND overture_release = $2`,
      [plan.county, plan.release, cid, ipnsName],
    );
    await client.query(
      `INSERT INTO oracle_dataset_coverage
         (county, source, ingested_count, expected_count, first_loaded_at,
          last_loaded_at, cid, ipns_label)
       VALUES ($1, 'overture_places', $2, NULL, now(), now(), $3, $4)
       ON CONFLICT (county, source) DO UPDATE SET
         ingested_count = EXCLUDED.ingested_count,
         expected_count = NULL,
         last_loaded_at = now(),
         cid = EXCLUDED.cid,
         ipns_label = EXCLUDED.ipns_label`,
      [plan.county, ingestedCount, cid, ipnsLabel],
    );
    await client.query("COMMIT");
    return {
      status: "succeeded",
      county: plan.county,
      release: plan.release,
      idempotencyKey: plan.idempotencyKey,
      ingestedCount,
      expectedCount: null,
      cid,
      ipnsLabel,
      ipnsName,
      parquetUrl,
      finishedAt: new Date().toISOString(),
    };
  } catch (caught) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw caught;
  } finally {
    await client.end();
  }
}

/**
 * Query the latest run whose load and public pointer both completed.
 *
 * Before migration 0008 is deployed, the verified pilot is recognized only
 * when its extraction has a passing licence gate and the coverage row has a CID.
 *
 * @param {object} params Query inputs.
 * @param {string} params.databaseUrl Neon URL.
 * @param {string} params.county County key.
 * @returns {Promise<string | null>} Latest successfully published release.
 */
async function readLastSuccessfulRelease(params) {
  const client = createPgClient(
    params.databaseUrl,
    "oracle-node-overture-refresh-plan",
  );
  await client.connect();
  try {
    try {
      const result = await client.query(
        `SELECT overture_release
         FROM overture_place_extractions
         WHERE county_key = $1
           AND run_status = 'succeeded'
         ORDER BY overture_release DESC
         LIMIT 1`,
        [params.county],
      );
      return result.rows[0]?.overture_release
        ? String(result.rows[0].overture_release)
        : null;
    } catch (caught) {
      if (!isUndefinedColumnError(caught)) throw caught;
      const legacy = await client.query(
        `SELECT e.overture_release
         FROM overture_place_extractions e
         JOIN oracle_dataset_coverage c
           ON c.county = e.county_key
          AND c.source = 'overture_places'
          AND c.cid IS NOT NULL
         WHERE e.county_key = $1
           AND e.licence_gate_passed = true
         ORDER BY e.overture_release DESC
         LIMIT 1`,
        [params.county],
      );
      return legacy.rows[0]?.overture_release
        ? String(legacy.rows[0].overture_release)
        : null;
    }
  } finally {
    await client.end();
  }
}

/**
 * Ensure the stable catalog URL already references the same IPNS network key.
 * Monthly refreshes update the IPNS pointer, not the catalog URL.
 *
 * @param {object} params Catalog inputs.
 * @param {string} params.county County key.
 * @param {string} params.ipnsName Stable IPNS network key.
 * @param {string} params.parquetUrl Verified public parquet URL.
 * @returns {Promise<void>}
 */
async function assertCatalogPointerStable(params) {
  const catalog = JSON.parse(
    await readFile("catalog/published-counties.json", "utf8"),
  );
  if (
    catalog === null ||
    typeof catalog !== "object" ||
    !Array.isArray(catalog.counties)
  ) {
    throw new Error("Published county catalog has an invalid shape");
  }
  const county = catalog.counties.find(
    (entry) =>
      entry !== null &&
      typeof entry === "object" &&
      entry.countyKey === params.county,
  );
  if (
    county === undefined ||
    typeof county.placesTableUrl !== "string" ||
    !county.placesTableUrl.includes(params.ipnsName)
  ) {
    throw new Error(
      `Catalog placesTableUrl for ${params.county} does not use verified IPNS ${params.ipnsName}`,
    );
  }
  if (!params.parquetUrl.includes(params.ipnsName)) {
    throw new Error(
      "Published parquet URL does not use the verified IPNS name",
    );
  }
}

/**
 * Parse and execute the CLI, optionally completing a Step Functions callback.
 *
 * @param {readonly string[]} argv CLI arguments.
 * @returns {Promise<void>}
 */
async function main(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      stage: { type: "string" },
      "input-json": { type: "string" },
      "env-file": { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });
  const stage = requireString(values.stage, "--stage");
  if (
    !["plan", "extract", "validate", "export", "verify", "finalize"].includes(
      stage,
    )
  ) {
    throw new Error(`Unsupported --stage ${stage}`);
  }
  const inputJson =
    typeof values["input-json"] === "string"
      ? values["input-json"]
      : process.env.WORKFLOW_INPUT;
  if (typeof inputJson !== "string") {
    throw new Error("--input-json or WORKFLOW_INPUT is required");
  }
  const input = JSON.parse(inputJson);
  const result = await runRefreshStage(
    /** @type {RefreshStage} */ (stage),
    requireObject(input, "input"),
    typeof values["env-file"] === "string" ? values["env-file"] : null,
  );
  const taskToken = process.env.TASK_TOKEN;
  if (typeof taskToken === "string" && taskToken.length > 0) {
    await new SFNClient({}).send(
      new SendTaskSuccessCommand({
        taskToken,
        output: JSON.stringify(result),
      }),
    );
    return;
  }
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

/**
 * Upload all files beneath a local artifact root.
 *
 * @param {object} params Upload inputs.
 * @param {string} params.localRoot Local directory.
 * @param {string} params.bucket Internal S3 bucket.
 * @param {string} params.prefix Key prefix.
 * @returns {Promise<void>}
 */
async function uploadDirectoryToS3(params) {
  const client = new S3Client({});
  /**
   * @param {string} dir Current directory.
   * @param {string} relative Relative key.
   * @returns {Promise<void>}
   */
  const walk = async (dir, relative) => {
    const entries = await readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
      const childRelative = relative ? `${relative}/${entry.name}` : entry.name;
      const childPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        await walk(childPath, childRelative);
        continue;
      }
      await client.send(
        new PutObjectCommand({
          Bucket: params.bucket,
          Key: `${params.prefix.replace(/\/+$/, "")}/${childRelative}`,
          Body: await readFile(childPath),
        }),
      );
    }
  };
  await walk(params.localRoot, "");
}

/**
 * @returns {string} Required Neon connection URL.
 */
function requireDatabaseUrl() {
  const value =
    process.env.DATABASE_URL_UNPOOLED ?? process.env.DATABASE_URL ?? "";
  if (value.trim().length === 0) {
    throw new Error("DATABASE_URL_UNPOOLED or DATABASE_URL is required");
  }
  return value.trim();
}

/**
 * @param {JsonObject} input Stage input.
 * @returns {RefreshPlan} Validated plan object.
 */
function readPlan(input) {
  const candidate =
    input.plan !== undefined ? requireObject(input.plan, "plan") : input;
  for (const key of [
    "action",
    "county",
    "countyFips",
    "boundarySource",
    "release",
    "workBucket",
    "runPrefix",
    "runId",
    "idempotencyKey",
  ]) {
    requireString(candidate[key], `plan.${key}`);
  }
  if (
    !["noop", "full", "incremental"].includes(String(candidate.action)) ||
    typeof candidate.dryRun !== "boolean" ||
    !(
      candidate.previousRelease === null ||
      typeof candidate.previousRelease === "string"
    )
  ) {
    throw new Error("plan has an invalid action, dryRun, or previousRelease");
  }
  return /** @type {RefreshPlan} */ (candidate);
}

/**
 * @param {string} county County key.
 * @returns {string} Human county name.
 */
function titleCaseCounty(county) {
  return county
    .split("-")
    .map((part) => part.slice(0, 1).toUpperCase() + part.slice(1))
    .join(" ");
}

/**
 * @param {string} value Raw execution/run id.
 * @returns {string} Filesystem/S3-safe run id.
 */
function sanitizeRunId(value) {
  return value.replace(/[^A-Za-z0-9._-]+/g, "-").slice(0, 120);
}

/**
 * @param {unknown} value Unknown object.
 * @param {string} field Field name.
 * @returns {JsonObject} Validated object.
 */
function requireObject(value, field) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${field} must be a JSON object`);
  }
  return /** @type {JsonObject} */ (value);
}

/**
 * @param {unknown} value Unknown string.
 * @param {string} field Field name.
 * @returns {string} Non-empty string.
 */
function requireString(value, field) {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${field} is required`);
  }
  return value.trim();
}

/**
 * @param {string} databaseUrl Neon URL.
 * @param {string} applicationName Postgres application name.
 * @returns {pg.Client} Configured client.
 */
function createPgClient(databaseUrl, applicationName) {
  return new pg.Client({
    connectionString: databaseUrl,
    ssl: { rejectUnauthorized: false },
    connectionTimeoutMillis: 20_000,
    application_name: applicationName,
  });
}

/**
 * @param {unknown} caught PostgreSQL error.
 * @returns {boolean} True for undefined-column SQLSTATE 42703.
 */
function isUndefinedColumnError(caught) {
  return (
    caught !== null &&
    typeof caught === "object" &&
    "code" in caught &&
    caught.code === "42703"
  );
}

/**
 * Load dotenv-style values without overwriting explicit process variables.
 *
 * @param {string} envFile Dotenv path.
 */
function loadEnvFile(envFile) {
  try {
    const text = readFileSync(envFile, "utf8");
    for (const line of text.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (trimmed.length === 0 || trimmed.startsWith("#")) continue;
      const separator = trimmed.indexOf("=");
      if (separator <= 0) continue;
      const key = trimmed.slice(0, separator);
      let value = trimmed.slice(separator + 1);
      if (
        value.length >= 2 &&
        ((value.startsWith('"') && value.endsWith('"')) ||
          (value.startsWith("'") && value.endsWith("'")))
      ) {
        value = value.slice(1, -1);
      }
      process.env[key] ??= value;
    }
  } catch (caught) {
    if (
      caught !== null &&
      typeof caught === "object" &&
      "code" in caught &&
      caught.code === "ENOENT"
    ) {
      return;
    }
    throw caught;
  }
}

if (
  process.argv[1] !== undefined &&
  fileURLToPath(import.meta.url) === path.resolve(process.argv[1])
) {
  main(process.argv.slice(2)).catch(async (caught) => {
    const message = caught instanceof Error ? caught.message : String(caught);
    const errorName =
      caught instanceof Error ? caught.name : "RefreshStageError";
    const taskToken = process.env.TASK_TOKEN;
    if (typeof taskToken === "string" && taskToken.length > 0) {
      await new SFNClient({})
        .send(
          new SendTaskFailureCommand({
            taskToken,
            error: errorName.slice(0, 256),
            cause: message.slice(0, 32_768),
          }),
        )
        .catch(() => undefined);
    }
    process.stderr.write(
      `${JSON.stringify({ event: "overture_places_refresh_stage_failed", stage: process.env.REFRESH_STAGE, error: message })}\n`,
    );
    process.exitCode = 1;
  });
}
