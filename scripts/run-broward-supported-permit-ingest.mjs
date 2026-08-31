#!/usr/bin/env node
// @ts-check

/**
 * Durable property-first Broward permit ingestion for implemented anonymous routes.
 *
 * The candidate population is the completed Broward appraiser property set. Situs
 * addresses route each folio through the exact 32-jurisdiction registry. Only
 * routes explicitly marked implemented are called. Durable Neon hashes checkpoint
 * records, explicit no-record results, bounded truncations, and exhausted errors.
 */

import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { mkdir, readFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import pg from "pg";

import {
  BROWARD_ACCELA_ADAPTER_KEY,
  BROWARD_BCS_ADAPTER_KEY,
  BROWARD_CITIZENSERVE_ADAPTER_KEY,
  BROWARD_PERMIT_REGISTRY_VERSION,
  BROWARD_TYLER_CIVIC_ACCESS_ADAPTER_KEY,
  resolveBrowardPermitJurisdiction,
} from "./broward-permit-jurisdictions.mjs";
import { loadBrowardPermitPilotToNeon } from "./load-broward-permit-pilot-to-neon.mjs";

const { Client } = pg;
const EXPECTED_PROJECT_ID = "raspy-frost-51580436";
const PRODUCTION_ENDPOINT_PREFIX = "ep-mute-leaf";
const CONTROL_SCHEMA = "ingest_control";
const LOCK_NAMESPACE = 12_011;
const LOCK_KEY = 4;
const MAX_CONCURRENCY = 4;
const DEFAULT_WORK_DIR = "downloads/broward/supported-permit-ingest";
const TERMINAL_STATUSES = new Set([
  "records",
  "no_permits",
  "truncated",
  "failed_exhausted",
]);

/**
 * @typedef {"records" | "no_permits" | "truncated" | "failed" | "failed_exhausted"} SupportedPermitItemStatus
 *
 * @typedef {object} SupportedPermitOptions
 * @property {string} jobId - Stable operator-selected job identifier.
 * @property {number} concurrency - Maximum simultaneous jurisdiction probes.
 * @property {number} maxAttempts - Attempts before one source result is terminal.
 * @property {number | null} limit - Optional deterministic candidate cap.
 * @property {string} workDirectory - Private source artifact and checkpoint root.
 *
 * @typedef {object} SupportedPermitCandidate
 * @property {string} folio - Exact private-in-process Broward folio.
 * @property {string} parcelHash - One-way durable item identity.
 * @property {string} situsAddress - Private routing/search address.
 * @property {string} jurisdictionKey - Registry jurisdiction key.
 * @property {string} adapterKey - Implemented adapter identity.
 *
 * @typedef {object} ProbeOutcome
 * @property {SupportedPermitItemStatus} status - Explicit source outcome.
 * @property {number} recordCount - Valid normalized records loaded from this attempt.
 * @property {string | null} errorClass - Fixed aggregate-safe failure class.
 *
 * @typedef {object} CommandResult
 * @property {number} exitCode - Child exit status.
 * @property {number} stderrBytes - Private child diagnostic byte count.
 */

/**
 * Parse a supported-routes run without accepting source-bypass options.
 *
 * @param {readonly string[]} argv - Arguments after the script path.
 * @returns {SupportedPermitOptions} Validated durable run configuration.
 */
export function parseSupportedPermitOptions(argv) {
  const values = new Map();
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (
      typeof flag !== "string" ||
      !flag.startsWith("--") ||
      typeof value !== "string" ||
      value.startsWith("--")
    ) {
      throw new Error("Supported permit options must be --flag value pairs");
    }
    values.set(flag.slice(2), value);
  }
  const jobId = values.get("job-id");
  if (
    typeof jobId !== "string" ||
    !/^broward-permits-[a-z0-9-]+$/u.test(jobId)
  ) {
    throw new Error("--job-id must begin broward-permits-");
  }
  const concurrency = boundedInteger(
    values.get("concurrency") ?? "4",
    "concurrency",
    1,
    MAX_CONCURRENCY,
  );
  const maxAttempts = boundedInteger(
    values.get("max-attempts") ?? "3",
    "max-attempts",
    1,
    5,
  );
  const limitRaw = values.get("limit");
  const limit =
    limitRaw === undefined
      ? null
      : boundedInteger(limitRaw, "limit", 1, 1_000_000);
  const workDirectory = values.get("work-dir") ?? DEFAULT_WORK_DIR;
  if (workDirectory.trim() === "") {
    throw new Error("--work-dir must not be empty");
  }
  return { jobId, concurrency, maxAttempts, limit, workDirectory };
}

/**
 * Run or resume supported anonymous permit routes.
 *
 * @param {SupportedPermitOptions} options - Durable run configuration.
 * @returns {Promise<{candidateCount:number,processed:number,terminal:number,failed:number}>}
 *   Aggregate run result.
 */
export async function runSupportedPermitIngest(options) {
  const target = requireTarget(process.env);
  const client = new Client({
    connectionString: target.connectionString,
    application_name: "broward-supported-permit-ingest",
    connectionTimeoutMillis: 10_000,
    statement_timeout: 120_000,
  });
  await client.connect();
  try {
    await verifyTarget(client, target);
    await ensureControlTables(client);
    await acquireRunLock(client);
    const candidates = await readCandidates(client, options.limit);
    const signature = candidateSignature(candidates, options);
    await registerRun(client, options, signature, candidates.length);
    const completed = await readCompletedItems(
      client,
      options.jobId,
      options.maxAttempts,
    );
    const pending = candidates.filter(
      (candidate) => !completed.has(candidate.parcelHash),
    );
    await mkdir(path.resolve(options.workDirectory), {
      recursive: true,
      mode: 0o700,
    });

    let processed = 0;
    let terminal = completed.size;
    let failed = 0;
    /** @type {Map<string, Promise<void>>} */
    const sourceTails = new Map();
    await processWithConcurrency(
      pending,
      options.concurrency,
      async (candidate) => {
        const prior = sourceTails.get(candidate.jurisdictionKey) ??
          Promise.resolve();
        const work = prior
          .catch(() => undefined)
          .then(async () => {
            const attempt = await readAttemptCount(
              client,
              options.jobId,
              candidate.parcelHash,
            );
            try {
              const outcome = await probeAndLoadCandidate(
                candidate,
                options,
              );
              const finalStatus =
                outcome.status === "failed" &&
                attempt + 1 >= options.maxAttempts
                  ? "failed_exhausted"
                  : outcome.status;
              await checkpointItem(
                client,
                options.jobId,
                candidate,
                finalStatus,
                outcome.recordCount,
                attempt + 1,
                outcome.errorClass,
              );
              processed += 1;
              if (TERMINAL_STATUSES.has(finalStatus)) terminal += 1;
              if (
                finalStatus === "failed" ||
                finalStatus === "failed_exhausted"
              ) {
                failed += 1;
              }
            } catch {
              const finalStatus =
                attempt + 1 >= options.maxAttempts
                  ? "failed_exhausted"
                  : "failed";
              await checkpointItem(
                client,
                options.jobId,
                candidate,
                finalStatus,
                0,
                attempt + 1,
                "source_or_load_error",
              );
              processed += 1;
              failed += 1;
              if (finalStatus === "failed_exhausted") terminal += 1;
            }
          });
        sourceTails.set(candidate.jurisdictionKey, work);
        await work;
      },
    );
    const aggregate = await readRunAggregate(client, options.jobId);
    await client.query(
      `UPDATE ${CONTROL_SCHEMA}.broward_supported_permit_runs
       SET phase = $2,
           terminal_count = $3,
           record_count = $4,
           failure_count = $5,
           heartbeat_at = now(),
           completed_at = CASE WHEN $2 = 'source_exhausted' THEN now() ELSE NULL END
       WHERE job_id = $1`,
      [
        options.jobId,
        aggregate.terminalCount >= candidates.length
          ? "source_exhausted"
          : "paused",
        aggregate.terminalCount,
        aggregate.recordCount,
        aggregate.failureCount,
      ],
    );
    return {
      candidateCount: candidates.length,
      processed,
      terminal: aggregate.terminalCount,
      failed: aggregate.failureCount,
    };
  } finally {
    await client.end();
  }
}

/**
 * Read completed appraisal properties and deterministically interleave routes.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @param {number | null} limit - Optional candidate limit.
 * @returns {Promise<SupportedPermitCandidate[]>} Supported-route candidates.
 */
async function readCandidates(client, limit) {
  const result = await client.query(
    `SELECT p.request_identifier, a.unnormalized_address
     FROM public.properties p
     JOIN public.addresses a ON a.address_id = p.address_id
     WHERE p.source_system = 'broward_appraiser'
       AND p.request_identifier IS NOT NULL
       AND a.unnormalized_address IS NOT NULL`,
  );
  /** @type {Map<string, SupportedPermitCandidate[]>} */
  const byJurisdiction = new Map();
  for (const row of result.rows) {
    if (
      typeof row.request_identifier !== "string" ||
      typeof row.unnormalized_address !== "string"
    ) {
      continue;
    }
    const resolution = resolveBrowardPermitJurisdiction({
      situsAddress: row.unnormalized_address,
    });
    const route = resolution.jurisdiction?.primarySource;
    if (
      resolution.jurisdiction === null ||
      route?.status !== "implemented" ||
      route.adapterKey === null
    ) {
      continue;
    }
    const candidate = {
      folio: row.request_identifier,
      parcelHash: hashFolio(row.request_identifier),
      situsAddress: row.unnormalized_address,
      jurisdictionKey: resolution.jurisdiction.key,
      adapterKey: route.adapterKey,
    };
    const group = byJurisdiction.get(candidate.jurisdictionKey) ?? [];
    group.push(candidate);
    byJurisdiction.set(candidate.jurisdictionKey, group);
  }
  for (const group of byJurisdiction.values()) {
    group.sort((left, right) => left.parcelHash.localeCompare(right.parcelHash));
  }
  const interleaved = [];
  let offset = 0;
  while (true) {
    let added = false;
    for (const key of [...byJurisdiction.keys()].sort()) {
      const candidate = byJurisdiction.get(key)?.[offset];
      if (candidate === undefined) continue;
      interleaved.push(candidate);
      added = true;
      if (limit !== null && interleaved.length >= limit) return interleaved;
    }
    if (!added) return interleaved;
    offset += 1;
  }
}

/**
 * Probe one source, load normalized records, and return explicit completeness.
 *
 * @param {SupportedPermitCandidate} candidate - Routed property.
 * @param {SupportedPermitOptions} options - Run paths and bounds.
 * @returns {Promise<ProbeOutcome>} Source and load outcome.
 */
async function probeAndLoadCandidate(candidate, options) {
  const itemDirectory = path.join(
    path.resolve(options.workDirectory),
    candidate.jurisdictionKey,
    candidate.parcelHash,
  );
  await mkdir(itemDirectory, { recursive: true, mode: 0o700 });
  if (candidate.adapterKey === BROWARD_BCS_ADAPTER_KEY) {
    return probeBcs(candidate, itemDirectory);
  }
  if (candidate.adapterKey === BROWARD_ACCELA_ADAPTER_KEY) {
    return probeAccela(candidate, itemDirectory);
  }
  if (
    candidate.adapterKey === BROWARD_TYLER_CIVIC_ACCESS_ADAPTER_KEY ||
    candidate.adapterKey === BROWARD_CITIZENSERVE_ADAPTER_KEY
  ) {
    return probeMunicipal(candidate, itemDirectory);
  }
  return { status: "failed_exhausted", recordCount: 0, errorClass: "unsupported_adapter" };
}

/**
 * Run one bounded BCS lookup and load complete normalized records.
 *
 * @param {SupportedPermitCandidate} candidate - Routed BCS property.
 * @param {string} itemDirectory - Private hash-keyed working directory.
 * @returns {Promise<ProbeOutcome>} Explicit BCS outcome.
 */
async function probeBcs(candidate, itemDirectory) {
  const recordsPath = path.join(itemDirectory, "records.private.jsonl");
  const summaryPath = path.join(itemDirectory, "summary.private.json");
  const command = await runNode([
    "scripts/probe-broward-bcs-permits.mjs",
    "--parcel-id",
    candidate.folio,
    "--output",
    recordsPath,
    "--summary",
    summaryPath,
    "--property-delay-ms",
    "1500",
    "--detail-delay-ms",
    "300",
  ]);
  if (command.exitCode !== 0) {
    return { status: "failed", recordCount: 0, errorClass: "bcs_probe_failed" };
  }
  const summary = await readJson(summaryPath);
  const recordCount = numberField(summary, "recordCount");
  if (recordCount > 0) {
    await loadBrowardPermitPilotToNeon({
      inputPath: recordsPath,
      expectedRecords: recordCount,
      includeBcs: true,
      accelaInputPath: null,
      expectedAccelaRecords: null,
      municipalInputPaths: [],
      expectedMunicipalRecords: null,
    });
  }
  return {
    status: recordCount > 0 ? "records" : "no_permits",
    recordCount,
    errorClass: null,
  };
}

/**
 * Run one bounded Accela lookup and load only a complete result set.
 *
 * @param {SupportedPermitCandidate} candidate - Routed Accela property.
 * @param {string} itemDirectory - Private hash-keyed working directory.
 * @returns {Promise<ProbeOutcome>} Explicit Accela outcome.
 */
async function probeAccela(candidate, itemDirectory) {
  const recordsPath = path.join(itemDirectory, "records.private.jsonl");
  const summaryPath = path.join(itemDirectory, "summary.private.json");
  const command = await runNode([
    "scripts/probe-broward-accela-permits.mjs",
    "--target",
    `${candidate.jurisdictionKey}:${candidate.folio}`,
    "--output",
    recordsPath,
    "--summary",
    summaryPath,
    "--checkpoint",
    path.join(itemDirectory, "checkpoint.private.json"),
    "--capture-dir",
    path.join(itemDirectory, "raw-private-captures"),
    "--max-pages",
    "10",
    "--max-details",
    "25",
    "--target-delay-ms",
    "1500",
    "--detail-delay-ms",
    "300",
  ]);
  if (command.exitCode !== 0) {
    return {
      status: command.stderrBytes > 0 ? "truncated" : "failed",
      recordCount: 0,
      errorClass: "accela_bounded_failure",
    };
  }
  const summary = await readJson(summaryPath);
  const recordCount = numberField(summary, "normalizedRecordCount");
  if (recordCount > 0) {
    await loadBrowardPermitPilotToNeon({
      inputPath: "",
      expectedRecords: null,
      includeBcs: false,
      accelaInputPath: recordsPath,
      expectedAccelaRecords: recordCount,
      municipalInputPaths: [],
      expectedMunicipalRecords: null,
    });
  }
  return {
    status: recordCount > 0 ? "records" : "no_permits",
    recordCount,
    errorClass: null,
  };
}

/**
 * Run one bounded Tyler/Citizenserve lookup and load returned records.
 *
 * @param {SupportedPermitCandidate} candidate - Routed municipal property.
 * @param {string} itemDirectory - Private hash-keyed working directory.
 * @returns {Promise<ProbeOutcome>} Explicit municipal outcome.
 */
async function probeMunicipal(candidate, itemDirectory) {
  const recordsPath = path.join(itemDirectory, "records.private.jsonl");
  const summaryPath = path.join(itemDirectory, "summary.json");
  const command = await runNode([
    "scripts/probe-broward-municipal-permits.mjs",
    "--jurisdiction",
    registryKeyToMunicipalKey(candidate.jurisdictionKey),
    "--folio",
    candidate.folio,
    "--output-dir",
    itemDirectory,
    "--max-pages",
    "3",
    "--max-details",
    "10",
    "--search-delay-ms",
    "1500",
    "--detail-delay-ms",
    "500",
  ]);
  if (command.exitCode !== 0) {
    return { status: "failed", recordCount: 0, errorClass: "municipal_probe_failed" };
  }
  const summary = await readJson(summaryPath);
  const recordCount = numberField(summary, "capturedPermitCount");
  if (recordCount > 0) {
    await loadBrowardPermitPilotToNeon({
      inputPath: "",
      expectedRecords: null,
      includeBcs: false,
      accelaInputPath: null,
      expectedAccelaRecords: null,
      municipalInputPaths: [recordsPath],
      expectedMunicipalRecords: recordCount,
    });
  }
  const truncated =
    summary.paginationTruncated === true || summary.detailsTruncated === true;
  return {
    status: truncated
      ? "truncated"
      : recordCount > 0
        ? "records"
        : "no_permits",
    recordCount,
    errorClass: truncated ? "bounded_source_truncation" : null,
  };
}

/**
 * Run a bounded child without exposing private stdout/stderr.
 *
 * @param {readonly string[]} args - Node script and non-secret arguments.
 * @returns {Promise<CommandResult>} Aggregate-safe process result.
 */
function runNode(args) {
  return new Promise((resolvePromise) => {
    const child = spawn(process.execPath, [...args], {
      cwd: process.cwd(),
      stdio: ["ignore", "ignore", "pipe"],
    });
    let stderrBytes = 0;
    child.stderr.on("data", (chunk) => {
      stderrBytes += Buffer.byteLength(chunk);
    });
    child.once("error", () => {
      resolvePromise({ exitCode: -1, stderrBytes });
    });
    child.once("exit", (code) => {
      resolvePromise({ exitCode: code ?? -1, stderrBytes });
    });
  });
}

/**
 * Atomically upsert one aggregate-safe parcel outcome and heartbeat.
 *
 * @param {import("pg").Client} client - Verified control client.
 * @param {string} jobId - Stable run identifier.
 * @param {SupportedPermitCandidate} candidate - Private in-process property.
 * @param {SupportedPermitItemStatus} status - Explicit source outcome.
 * @param {number} recordCount - Records committed from this property.
 * @param {number} attemptCount - Durable source attempt count.
 * @param {string | null} errorClass - Fixed aggregate-safe failure class.
 * @returns {Promise<void>} Resolves after item and heartbeat writes.
 */
async function checkpointItem(
  client,
  jobId,
  candidate,
  status,
  recordCount,
  attemptCount,
  errorClass,
) {
  await client.query(
    `INSERT INTO ${CONTROL_SCHEMA}.broward_supported_permit_items (
       job_id, parcel_hash, jurisdiction_key, adapter_key, status,
       record_count, attempt_count, error_class
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     ON CONFLICT (job_id, parcel_hash) DO UPDATE SET
       status=EXCLUDED.status, record_count=EXCLUDED.record_count,
       attempt_count=EXCLUDED.attempt_count, error_class=EXCLUDED.error_class,
       updated_at=now()`,
    [
      jobId,
      candidate.parcelHash,
      candidate.jurisdictionKey,
      candidate.adapterKey,
      status,
      recordCount,
      attemptCount,
      errorClass,
    ],
  );
  await client.query(
    `UPDATE ${CONTROL_SCHEMA}.broward_supported_permit_runs
     SET heartbeat_at=now() WHERE job_id=$1`,
    [jobId],
  );
}

/**
 * Create additive supported-run control tables after identity verification.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @returns {Promise<void>} Resolves after idempotent DDL.
 */
async function ensureControlTables(client) {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${CONTROL_SCHEMA}`);
  await client.query(
    `CREATE TABLE IF NOT EXISTS ${CONTROL_SCHEMA}.broward_supported_permit_runs (
       job_id text PRIMARY KEY,
       config_signature text NOT NULL,
       registry_version text NOT NULL,
       candidate_count integer NOT NULL,
       concurrency integer NOT NULL,
       max_attempts integer NOT NULL,
       phase text NOT NULL CHECK (phase IN ('running','paused','source_exhausted','failed')),
       terminal_count integer NOT NULL DEFAULT 0,
       record_count integer NOT NULL DEFAULT 0,
       failure_count integer NOT NULL DEFAULT 0,
       started_at timestamptz NOT NULL DEFAULT now(),
       heartbeat_at timestamptz NOT NULL DEFAULT now(),
       completed_at timestamptz
     )`,
  );
  await client.query(
    `CREATE TABLE IF NOT EXISTS ${CONTROL_SCHEMA}.broward_supported_permit_items (
       job_id text NOT NULL REFERENCES ${CONTROL_SCHEMA}.broward_supported_permit_runs(job_id),
       parcel_hash text NOT NULL CHECK (parcel_hash ~ '^[a-f0-9]{64}$'),
       jurisdiction_key text NOT NULL,
       adapter_key text NOT NULL,
       status text NOT NULL CHECK (
         status IN ('records','no_permits','truncated','failed','failed_exhausted')
       ),
       record_count integer NOT NULL CHECK (record_count >= 0),
       attempt_count integer NOT NULL CHECK (attempt_count > 0),
       error_class text,
       updated_at timestamptz NOT NULL DEFAULT now(),
       PRIMARY KEY (job_id, parcel_hash)
     )`,
  );
}

/**
 * Register or verify one immutable supported-routes run contract.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @param {SupportedPermitOptions} options - Run configuration.
 * @param {string} signature - Candidate/config SHA-256.
 * @param {number} candidateCount - Exact supported candidate count.
 * @returns {Promise<void>} Resolves after registration and heartbeat.
 */
async function registerRun(client, options, signature, candidateCount) {
  await client.query(
    `INSERT INTO ${CONTROL_SCHEMA}.broward_supported_permit_runs (
       job_id, config_signature, registry_version, candidate_count,
       concurrency, max_attempts, phase
     ) VALUES ($1,$2,$3,$4,$5,$6,'running')
     ON CONFLICT (job_id) DO NOTHING`,
    [
      options.jobId,
      signature,
      BROWARD_PERMIT_REGISTRY_VERSION,
      candidateCount,
      options.concurrency,
      options.maxAttempts,
    ],
  );
  const result = await client.query(
    `SELECT config_signature,candidate_count,concurrency,max_attempts
     FROM ${CONTROL_SCHEMA}.broward_supported_permit_runs WHERE job_id=$1`,
    [options.jobId],
  );
  const row = result.rows[0];
  if (
    row?.config_signature !== signature ||
    Number(row.candidate_count) !== candidateCount ||
    Number(row.concurrency) !== options.concurrency ||
    Number(row.max_attempts) !== options.maxAttempts
  ) {
    throw new Error("Existing supported permit run config does not match");
  }
  await client.query(
    `UPDATE ${CONTROL_SCHEMA}.broward_supported_permit_runs
     SET phase='running',heartbeat_at=now() WHERE job_id=$1`,
    [options.jobId],
  );
}

/**
 * Read terminal or exhausted one-way parcel hashes.
 *
 * @param {import("pg").Client} client - Verified control client.
 * @param {string} jobId - Stable run identifier.
 * @param {number} maxAttempts - Failure exhaustion threshold.
 * @returns {Promise<Set<string>>} Durable completed parcel hashes.
 */
async function readCompletedItems(client, jobId, maxAttempts) {
  const result = await client.query(
    `SELECT parcel_hash,status,attempt_count
     FROM ${CONTROL_SCHEMA}.broward_supported_permit_items WHERE job_id=$1`,
    [jobId],
  );
  return new Set(
    result.rows.flatMap((row) =>
      typeof row.parcel_hash === "string" &&
      (TERMINAL_STATUSES.has(row.status) ||
        Number(row.attempt_count) >= maxAttempts)
        ? [row.parcel_hash]
        : [],
    ),
  );
}

/**
 * Read a prior parcel attempt count.
 *
 * @param {import("pg").Client} client - Verified control client.
 * @param {string} jobId - Stable run identifier.
 * @param {string} parcelHash - One-way parcel identity.
 * @returns {Promise<number>} Existing attempt count or zero.
 */
async function readAttemptCount(client, jobId, parcelHash) {
  const result = await client.query(
    `SELECT attempt_count FROM ${CONTROL_SCHEMA}.broward_supported_permit_items
     WHERE job_id=$1 AND parcel_hash=$2`,
    [jobId, parcelHash],
  );
  return Number(result.rows[0]?.attempt_count ?? 0);
}

/**
 * Rebuild run counters from durable item truth.
 *
 * @param {import("pg").Client} client - Verified control client.
 * @param {string} jobId - Stable run identifier.
 * @returns {Promise<{terminalCount:number,recordCount:number,failureCount:number}>}
 *   Aggregate durable counters.
 */
async function readRunAggregate(client, jobId) {
  const result = await client.query(
    `SELECT
       count(*) FILTER (WHERE status IN ('records','no_permits','truncated','failed_exhausted'))::integer AS terminal_count,
       coalesce(sum(record_count),0)::integer AS record_count,
       count(*) FILTER (WHERE status IN ('failed','failed_exhausted'))::integer AS failure_count
     FROM ${CONTROL_SCHEMA}.broward_supported_permit_items WHERE job_id=$1`,
    [jobId],
  );
  return {
    terminalCount: Number(result.rows[0]?.terminal_count ?? 0),
    recordCount: Number(result.rows[0]?.record_count ?? 0),
    failureCount: Number(result.rows[0]?.failure_count ?? 0),
  };
}

/**
 * Acquire the session-scoped supported permit writer lock.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @returns {Promise<void>} Resolves only for the sole runner.
 */
async function acquireRunLock(client) {
  const result = await client.query(
    "SELECT pg_try_advisory_lock($1,$2) AS acquired",
    [LOCK_NAMESPACE, LOCK_KEY],
  );
  if (result.rows[0]?.acquired !== true) {
    throw new Error("Another supported permit runner owns the writer lock");
  }
}

/**
 * Prove immutable Neon project, branch, and endpoint identity.
 *
 * @param {import("pg").Client} client - Connected direct client.
 * @param {{expectedBranchId:string,expectedEndpointId:string}} target - Expected IDs.
 * @returns {Promise<void>} Resolves after exact identity match.
 */
async function verifyTarget(client, target) {
  const result = await client.query(
    `SELECT current_setting('neon.project_id',true) AS project_id,
            current_setting('neon.branch_id',true) AS branch_id,
            current_setting('neon.endpoint_id',true) AS endpoint_id`,
  );
  const row = result.rows[0];
  if (
    row?.project_id !== EXPECTED_PROJECT_ID ||
    row.branch_id !== target.expectedBranchId ||
    row.endpoint_id !== target.expectedEndpointId
  ) {
    throw new Error("Supported permit target is not isolated broward-ingest");
  }
}

/**
 * Read and validate the direct Neon runtime target.
 *
 * @param {NodeJS.ProcessEnv} environment - Runtime secrets.
 * @returns {{connectionString:string,expectedBranchId:string,expectedEndpointId:string}}
 *   Validated target without logging values.
 */
function requireTarget(environment) {
  const connectionString = environment.DATABASE_URL_UNPOOLED;
  const expectedBranchId = environment.BROWARD_INGEST_NEON_BRANCH_ID;
  const expectedEndpointId = environment.BROWARD_INGEST_NEON_ENDPOINT_ID;
  if (
    typeof connectionString !== "string" ||
    typeof expectedBranchId !== "string" ||
    !/^br-[a-z0-9-]+$/u.test(expectedBranchId) ||
    typeof expectedEndpointId !== "string" ||
    !/^ep-[a-z0-9-]+$/u.test(expectedEndpointId) ||
    expectedEndpointId.startsWith(PRODUCTION_ENDPOINT_PREFIX)
  ) {
    throw new Error("Verified direct Broward Neon target is required");
  }
  return { connectionString, expectedBranchId, expectedEndpointId };
}

/**
 * Read one source summary as an object.
 *
 * @param {string} filePath - Private source summary path.
 * @returns {Promise<Record<string, unknown>>} Parsed summary object.
 */
async function readJson(filePath) {
  const value = /** @type {unknown} */ (
    JSON.parse(await readFile(filePath, "utf8"))
  );
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("Permit probe summary is not a JSON object");
  }
  return /** @type {Record<string, unknown>} */ (value);
}

/**
 * Read a non-negative integer source summary field.
 *
 * @param {Record<string, unknown>} record - Source summary.
 * @param {string} key - Required numeric field.
 * @returns {number} Validated non-negative integer.
 */
function numberField(record, key) {
  const value = record[key];
  if (!Number.isInteger(value) || Number(value) < 0) {
    throw new Error(`Permit summary has invalid ${key}`);
  }
  return Number(value);
}

/**
 * Hash exact ordered candidates and run configuration.
 *
 * @param {readonly SupportedPermitCandidate[]} candidates - Ordered candidate set.
 * @param {SupportedPermitOptions} options - Run bounds.
 * @returns {string} Lowercase SHA-256 signature.
 */
function candidateSignature(candidates, options) {
  const digest = createHash("sha256");
  digest.update(BROWARD_PERMIT_REGISTRY_VERSION);
  digest.update(`\0${String(options.limit)}\0${String(options.concurrency)}\0`);
  for (const candidate of candidates) {
    digest.update(
      `${candidate.parcelHash}:${candidate.jurisdictionKey}:${candidate.adapterKey}\n`,
    );
  }
  return digest.digest("hex");
}

/**
 * Produce one one-way durable parcel identity.
 *
 * @param {string} folio - Exact private-in-process folio.
 * @returns {string} Lowercase SHA-256.
 */
function hashFolio(folio) {
  return createHash("sha256")
    .update(`broward-permit:${folio}`)
    .digest("hex");
}

/**
 * Convert registry kebab keys to municipal adapter keys.
 *
 * @param {string} key - Exact registry jurisdiction key.
 * @returns {string} Municipal adapter configuration key.
 */
function registryKeyToMunicipalKey(key) {
  return key.replaceAll("-", "_");
}

/**
 * Process items with bounded total concurrency.
 *
 * @template Item
 * @param {readonly Item[]} items - Ordered pending items.
 * @param {number} concurrency - Maximum simultaneous handlers.
 * @param {(item:Item)=>Promise<void>} handler - Per-item operation.
 * @returns {Promise<void>} Resolves after all items settle successfully.
 */
async function processWithConcurrency(items, concurrency, handler) {
  const executing = new Set();
  for (const item of items) {
    const task = Promise.resolve().then(() => handler(item));
    executing.add(task);
    void task.finally(() => executing.delete(task));
    if (executing.size >= concurrency) await Promise.race(executing);
  }
  await Promise.all(executing);
}

/**
 * Parse an integer within an inclusive range.
 *
 * @param {string} raw - Raw CLI value.
 * @param {string} name - Option name without dashes.
 * @param {number} minimum - Inclusive minimum.
 * @param {number} maximum - Inclusive maximum.
 * @returns {number} Validated integer.
 */
function boundedInteger(raw, name, minimum, maximum) {
  const value = Number(raw);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(
      `--${name} must be an integer from ${String(minimum)} through ${String(maximum)}`,
    );
  }
  return value;
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  runSupportedPermitIngest(
    parseSupportedPermitOptions(process.argv.slice(2)),
  )
    .then((result) => {
      console.log(
        JSON.stringify({
          event: "broward_supported_permit_ingest_finished",
          ...result,
        }),
      );
    })
    .catch((error) => {
      console.error(
        JSON.stringify({
          event: "broward_supported_permit_ingest_failed",
          message: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      process.exitCode = 1;
    });
}
