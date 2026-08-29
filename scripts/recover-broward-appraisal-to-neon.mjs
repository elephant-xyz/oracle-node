#!/usr/bin/env node

/**
 * Reboot-safe Broward appraisal capture, warm transform, and Neon load.
 *
 * The official GIS seed and all local artifacts are disposable. Durable state
 * is reconstructed from deterministic `broward_appraiser` source keys in Neon,
 * terminal source-miss hashes, and aggregate chunk commits. A chunk is recorded
 * only after every expected logical row is visible following the loader commit.
 */

import { createReadStream } from "node:fs";
import {
  access,
  copyFile,
  mkdir,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { parse } from "csv-parse";
import pg from "pg";

import {
  BROWARD_PILOT_FOLIOS,
  normalizeBrowardFolio,
} from "./broward-folio.mjs";
import { BROWARD_ROW_DENOMINATOR } from "./broward-ingestion-dashboard.mjs";
import { SEED_COLUMNS, renderCsvRow } from "./build-broward-seed.mjs";
import { runLocalIngestion } from "./ingest-broward-appraisal-local.mjs";
import {
  inspectQueryDataOnlyArtifact,
  QUERY_DATA_ONLY_MODE,
  QUERY_DATA_ONLY_SUFFIX,
} from "./broward-query-data-only.mjs";

const { Client } = pg;
const SOURCE_SYSTEM = "broward_appraiser";
const EXPECTED_PROJECT_ID = "raspy-frost-51580436";
const PRODUCTION_ENDPOINT_PREFIX = "ep-mute-leaf";
const CONTROL_SCHEMA = "ingest_control";
const CONTROL_LOCK_NAMESPACE = 12_011;
const CONTROL_LOCK_KEY = 1;
const DEFAULT_SEED_PATH = "downloads/broward/broward.csv";
const DEFAULT_SCRIPTS_DIRECTORY =
  "/tmp/Counties-trasform-scripts/broward/scripts";
const DEFAULT_QUERY_DB_DIRECTORY = "/tmp/elephant-query-db";
const DEFAULT_WORK_DIRECTORY = "downloads/broward/neon-recovery";
const DEFAULT_CHUNK_SIZE = 100;
const DEFAULT_CONCURRENCY = 4;
const MAX_CONCURRENCY = 4;
const MAX_CHUNK_SIZE = 500;
const PILOT_PROPERTY_COUNT = 50;
const PILOT_FALLBACK_COUNT = 2_000;

/**
 * @typedef {Record<string, string | undefined>} CsvRecord
 *
 * @typedef {"pilot" | "full"} RecoveryMode
 *
 * @typedef {object} RecoveryOptions
 * @property {RecoveryMode} mode - Pilot gate or full missing-folio recovery.
 * @property {string} seedPath - Reconstructable official GIS seed CSV.
 * @property {string} scriptsDirectory - Broward county transform scripts.
 * @property {string} queryDbDirectory - Exactly patched query-db checkout.
 * @property {string} workDirectory - Disposable private working root.
 * @property {number} chunkSize - Maximum seed candidates in one load commit.
 * @property {number} concurrency - Maximum simultaneous source/transform pipelines.
 * @property {string} expectedBranchId - Neon branch ID assigned to `broward-ingest`.
 * @property {string} expectedEndpointId - Neon endpoint ID assigned to `broward-ingest`.
 *
 * @typedef {object} SeedCandidate
 * @property {number} sourceRowIndex - Zero-based row in the official full seed.
 * @property {string} folio - Canonical Broward folio kept only in private process memory.
 * @property {CsvRecord} row - Complete source seed row.
 *
 * @typedef {object} SeedStats
 * @property {number} rowCount - Valid source rows.
 * @property {number} distinctFolios - Distinct canonical folios.
 * @property {string} signature - SHA-256 of ordered folios, without geometry or PII logs.
 *
 * @typedef {object} RedactedIngestResult
 * @property {number} rowIndex - Zero-based row in the bounded chunk.
 * @property {"succeeded" | "skipped_existing" | "source_error" | "transform_error"} status
 *   Pipeline outcome.
 * @property {"source_miss" | "source_error" | "transform_error" | null} failureClass
 *   Aggregate-safe failure category.
 *
 * @typedef {object} ExpectedChunkRows
 * @property {number} preparedRows - Mapper rows before per-table source-key deduplication.
 * @property {number} propertyRows - Exactly one expected property per transformed folio.
 * @property {number} distinctFolios - Distinct property folios.
 * @property {ReadonlyMap<string, ReadonlySet<string>>} sourceKeysByTable
 *   Expected unique source keys by logical table.
 *
 * @typedef {object} ChunkResult
 * @property {number} attempted - Seed candidates attempted.
 * @property {number} loaded - Properties committed and verified.
 * @property {number} sourceMisses - Terminal GIS-only source misses.
 * @property {number} sourceErrors - Retryable source errors.
 * @property {number} transformErrors - Retryable transform errors.
 * @property {number} preparedRows - Prepared mapper rows committed for the chunk.
 * @property {readonly string[]} loadedFolios - Loaded folios retained only in process memory.
 * @property {readonly string[]} terminalFolioHashes - Terminal source-miss hashes.
 *
 * @typedef {object} DurableCompletion
 * @property {Set<string>} loadedFolios
 *   Property identifiers already visible in Neon, including any interrupted partial load.
 * @property {Set<string>} completedHashes
 *   Seed-key hashes checkpointed only after all expected logical rows were verified.
 * @property {Set<string>} terminalHashes - Confirmed source-miss seed-key hashes.
 */

/**
 * Parse the fail-closed recovery CLI.
 *
 * Branch and endpoint IDs are mandatory because a human-readable Neon branch
 * label is not exposed over PostgreSQL. Both are checked against server-side
 * `neon.*` settings before any schema or data mutation.
 *
 * @param {readonly string[]} argv - Arguments after the script filename.
 * @returns {RecoveryOptions} Validated recovery configuration.
 */
export function parseRecoveryOptions(argv) {
  /** @type {Partial<RecoveryOptions> & { mode?: RecoveryMode }} */
  const values = {
    seedPath: DEFAULT_SEED_PATH,
    scriptsDirectory: DEFAULT_SCRIPTS_DIRECTORY,
    queryDbDirectory: DEFAULT_QUERY_DB_DIRECTORY,
    workDirectory: DEFAULT_WORK_DIRECTORY,
    chunkSize: DEFAULT_CHUNK_SIZE,
    concurrency: DEFAULT_CONCURRENCY,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index];
    if (flag === "--pilot" || flag === "--full") {
      if (values.mode !== undefined) {
        throw new Error("Choose exactly one of --pilot or --full");
      }
      values.mode = flag === "--pilot" ? "pilot" : "full";
      continue;
    }
    const raw = argv[index + 1];
    if (typeof raw !== "string" || raw.startsWith("--")) {
      throw new Error(`Missing value for ${String(flag)}`);
    }
    if (flag === "--seed") values.seedPath = raw;
    else if (flag === "--scripts") values.scriptsDirectory = raw;
    else if (flag === "--query-db") values.queryDbDirectory = raw;
    else if (flag === "--work-dir") values.workDirectory = raw;
    else if (flag === "--chunk-size") {
      values.chunkSize = parseBoundedInteger(flag, raw, 1, MAX_CHUNK_SIZE);
    } else if (flag === "--concurrency") {
      values.concurrency = parseBoundedInteger(flag, raw, 1, MAX_CONCURRENCY);
    } else if (flag === "--expected-branch-id") {
      values.expectedBranchId = raw;
    } else if (flag === "--expected-endpoint-id") {
      values.expectedEndpointId = raw;
    } else {
      throw new Error(`Unknown option: ${String(flag)}`);
    }
    index += 1;
  }
  if (values.mode === undefined) {
    throw new Error("Choose exactly one of --pilot or --full");
  }
  if (
    typeof values.expectedBranchId !== "string" ||
    !/^br-[a-z0-9-]+$/u.test(values.expectedBranchId)
  ) {
    throw new Error("--expected-branch-id must be an explicit Neon br-* ID");
  }
  if (
    typeof values.expectedEndpointId !== "string" ||
    !/^ep-[a-z0-9-]+$/u.test(values.expectedEndpointId)
  ) {
    throw new Error("--expected-endpoint-id must be an explicit Neon ep-* ID");
  }
  return /** @type {RecoveryOptions} */ (values);
}

/**
 * Parse an integer constrained by an inclusive range.
 *
 * @param {string} flag - CLI flag used in errors.
 * @param {string} raw - Untrusted CLI value.
 * @param {number} minimum - Inclusive minimum.
 * @param {number} maximum - Inclusive maximum.
 * @returns {number} Validated integer.
 */
function parseBoundedInteger(flag, raw, minimum, maximum) {
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(
      `${flag} must be an integer from ${String(minimum)} through ${String(maximum)}`,
    );
  }
  return parsed;
}

/**
 * Return whether a path is readable.
 *
 * @param {string} targetPath - File or directory path.
 * @returns {Promise<boolean>} True only for a readable path.
 */
async function pathExists(targetPath) {
  try {
    await access(targetPath);
    return true;
  } catch {
    return false;
  }
}

/**
 * Run a bounded child command while retaining output only for machine parsing.
 *
 * Secrets are passed through the child environment and never placed in argv or
 * returned errors. Raw stderr is deliberately discarded from operator output.
 *
 * @param {object} params - Child process parameters.
 * @param {string} params.command - Executable name.
 * @param {readonly string[]} params.args - Non-secret arguments.
 * @param {string} params.cwd - Child working directory.
 * @param {NodeJS.ProcessEnv} [params.env] - Optional environment override.
 * @param {string} params.stage - Aggregate-safe stage name.
 * @returns {Promise<string>} Captured UTF-8 stdout.
 */
async function runCommand({ command, args, cwd, env, stage }) {
  return new Promise((resolvePromise, rejectPromise) => {
    const child = spawn(command, [...args], {
      cwd,
      env: env ?? process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderrBytes = 0;
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout = `${stdout}${String(chunk)}`.slice(-10_000_000);
    });
    child.stderr.on("data", (chunk) => {
      stderrBytes += Buffer.byteLength(chunk);
    });
    child.once("error", () => {
      rejectPromise(new Error(`${stage} could not start`));
    });
    child.once("exit", (code) => {
      if (code === 0) {
        resolvePromise(stdout);
        return;
      }
      rejectPromise(
        new Error(
          `${stage} failed with exit ${String(code)}; private stderr bytes=${String(stderrBytes)}`,
        ),
      );
    });
  });
}

/**
 * Build the official seed when a reboot removed it.
 *
 * @param {string} seedPath - Expected full-seed path.
 * @returns {Promise<void>} Resolves after the seed exists.
 */
async function ensureOfficialSeed(seedPath) {
  if (await pathExists(seedPath)) return;
  await runCommand({
    command: process.execPath,
    args: [
      path.resolve("scripts/build-broward-seed.mjs"),
      "--output",
      seedPath,
      "--page-size",
      "50",
      "--concurrency",
      "4",
    ],
    cwd: process.cwd(),
    stage: "official_seed_rebuild",
  });
}

/**
 * Validate the full seed and compute its independently reconstructable identity.
 *
 * @param {string} seedPath - Official GIS seed CSV.
 * @returns {Promise<SeedStats>} Exact row, distinct-folio, and signature evidence.
 */
export async function readSeedStats(seedPath) {
  const folios = new Set();
  const digest = createHash("sha256");
  let rowCount = 0;
  const parser = createReadStream(seedPath).pipe(
    parse({ columns: true, skip_empty_lines: true }),
  );
  for await (const parsed of parser) {
    const row = /** @type {CsvRecord} */ (parsed);
    const folio = normalizeBrowardFolio(row.request_identifier);
    if (folio === undefined) {
      throw new Error("Official Broward seed contains an invalid folio");
    }
    if (folios.has(folio)) {
      throw new Error("Official Broward seed contains a duplicate folio");
    }
    folios.add(folio);
    digest.update(folio);
    digest.update("\n");
    rowCount += 1;
  }
  return {
    rowCount,
    distinctFolios: folios.size,
    signature: digest.digest("hex"),
  };
}

/**
 * Require the preserved full-county denominator.
 *
 * @param {SeedStats} stats - Rebuilt seed evidence.
 * @returns {void}
 */
export function assertFullSeed(stats) {
  if (
    stats.rowCount !== BROWARD_ROW_DENOMINATOR ||
    stats.distinctFolios !== BROWARD_ROW_DENOMINATOR
  ) {
    throw new Error(
      `Official Broward seed must contain ${String(BROWARD_ROW_DENOMINATOR)} distinct folios`,
    );
  }
}

/**
 * Open a direct Neon client without exposing its connection string.
 *
 * @returns {Promise<import("pg").Client>} Connected direct client.
 */
async function connectToNeon() {
  const databaseUrl = process.env.DATABASE_URL_UNPOOLED;
  if (typeof databaseUrl !== "string" || databaseUrl.trim().length === 0) {
    throw new Error("DATABASE_URL_UNPOOLED is required");
  }
  const client = new Client({
    connectionString: databaseUrl,
    application_name: "broward-durable-appraisal-recovery",
    connectionTimeoutMillis: 10_000,
    statement_timeout: 120_000,
  });
  await client.connect();
  return client;
}

/**
 * Prove the connection target and existing Broward row isolation read-only.
 *
 * @param {import("pg").Client} client - Connected Neon client.
 * @param {RecoveryOptions} options - Explicit expected branch and endpoint IDs.
 * @returns {Promise<{ projectId: string, branchId: string, endpointId: string, propertyCount: number, distinctFolios: number }>}
 *   Safe identity and aggregate inventory.
 */
export async function verifyNeonTarget(client, options) {
  await client.query("BEGIN READ ONLY");
  try {
    const identityResult = await client.query(
      `SELECT
         current_setting('neon.project_id', true) AS project_id,
         current_setting('neon.branch_id', true) AS branch_id,
         current_setting('neon.endpoint_id', true) AS endpoint_id`,
    );
    const identity = identityResult.rows[0];
    const projectId =
      typeof identity?.project_id === "string" ? identity.project_id : "";
    const branchId =
      typeof identity?.branch_id === "string" ? identity.branch_id : "";
    const endpointId =
      typeof identity?.endpoint_id === "string" ? identity.endpoint_id : "";
    if (
      projectId !== EXPECTED_PROJECT_ID ||
      branchId !== options.expectedBranchId ||
      endpointId !== options.expectedEndpointId ||
      endpointId.startsWith(PRODUCTION_ENDPOINT_PREFIX)
    ) {
      throw new Error(
        "Neon target identity does not match isolated broward-ingest",
      );
    }
    const countsResult = await client.query(
      `SELECT
         count(*)::bigint AS property_count,
         count(DISTINCT p.request_identifier)::bigint AS distinct_folios,
         count(*) FILTER (
           WHERE p.request_identifier IS NULL
              OR pa.jurisdiction_key IS DISTINCT FROM $1
         )::bigint AS invalid_property_rows
       FROM public.properties p
       LEFT JOIN public.parcels pa ON pa.parcel_id = p.parcel_id
       WHERE p.source_system = $1`,
      [SOURCE_SYSTEM],
    );
    const addressResult = await client.query(
      `SELECT count(*)::bigint AS invalid_address_rows
       FROM public.addresses
       WHERE source_system = $1
         AND county_name IS DISTINCT FROM 'Broward'`,
      [SOURCE_SYSTEM],
    );
    const counts = countsResult.rows[0];
    const propertyCount = Number(counts?.property_count ?? 0);
    const distinctFolios = Number(counts?.distinct_folios ?? 0);
    const invalidPropertyRows = Number(counts?.invalid_property_rows ?? 0);
    const invalidAddressRows = Number(
      addressResult.rows[0]?.invalid_address_rows ?? 0,
    );
    if (
      propertyCount !== distinctFolios ||
      invalidPropertyRows !== 0 ||
      invalidAddressRows !== 0
    ) {
      throw new Error(
        "Existing broward_appraiser rows are not safely isolated",
      );
    }
    await client.query("ROLLBACK");
    return {
      projectId,
      branchId,
      endpointId,
      propertyCount,
      distinctFolios,
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

/**
 * Create aggregate-only durable checkpoint tables after the safety gate.
 *
 * @param {import("pg").Client} client - Verified isolated-branch client.
 * @returns {Promise<void>} Resolves after transactional DDL commit.
 */
async function ensureControlTables(client) {
  await client.query("BEGIN");
  try {
    await client.query(`CREATE SCHEMA IF NOT EXISTS ${CONTROL_SCHEMA}`);
    await client.query(
      `CREATE TABLE IF NOT EXISTS ${CONTROL_SCHEMA}.broward_appraisal_chunks (
         chunk_id text PRIMARY KEY,
         run_mode text NOT NULL CHECK (run_mode IN ('pilot', 'full')),
         seed_signature text NOT NULL,
         attempted_count integer NOT NULL CHECK (attempted_count >= 0),
         property_count integer NOT NULL CHECK (property_count >= 0),
         distinct_folio_count integer NOT NULL CHECK (distinct_folio_count >= 0),
         prepared_row_count integer NOT NULL CHECK (prepared_row_count >= 0),
         committed_row_count integer NOT NULL CHECK (committed_row_count >= 0),
         source_miss_count integer NOT NULL CHECK (source_miss_count >= 0),
         source_error_count integer NOT NULL CHECK (source_error_count >= 0),
         transform_error_count integer NOT NULL CHECK (transform_error_count >= 0),
         branch_id text NOT NULL,
         endpoint_id text NOT NULL,
         committed_at timestamptz NOT NULL DEFAULT now()
       )`,
    );
    await client.query(
      `CREATE TABLE IF NOT EXISTS ${CONTROL_SCHEMA}.broward_appraisal_terminal_items (
         seed_key_hash text PRIMARY KEY,
         outcome text NOT NULL CHECK (outcome = 'source_miss'),
         recorded_at timestamptz NOT NULL DEFAULT now()
       )`,
    );
    await client.query(
      `CREATE TABLE IF NOT EXISTS ${CONTROL_SCHEMA}.broward_appraisal_completed_items (
         seed_key_hash text PRIMARY KEY,
         outcome text NOT NULL CHECK (outcome = 'loaded'),
         recorded_at timestamptz NOT NULL DEFAULT now()
       )`,
    );
    await client.query(
      `CREATE TABLE IF NOT EXISTS ${CONTROL_SCHEMA}.broward_appraisal_gates (
         gate_name text PRIMARY KEY CHECK (gate_name = 'pilot-50'),
         seed_signature text NOT NULL,
         property_count integer NOT NULL CHECK (property_count = 50),
         distinct_folio_count integer NOT NULL CHECK (distinct_folio_count = 50),
         branch_id text NOT NULL,
         endpoint_id text NOT NULL,
         committed_at timestamptz NOT NULL DEFAULT now()
       )`,
    );
    await client.query(
      `CREATE TABLE IF NOT EXISTS ${CONTROL_SCHEMA}.broward_appraisal_events (
         event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
         stage text NOT NULL CHECK (
           stage IN ('source_miss', 'source_error', 'transform_error', 'load_error')
         ),
         event_count integer NOT NULL CHECK (event_count > 0),
         recorded_at timestamptz NOT NULL DEFAULT now()
       )`,
    );
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

/**
 * Acquire a session advisory lock so two recoveries cannot overlap.
 *
 * @param {import("pg").Client} client - Verified control client.
 * @returns {Promise<void>} Resolves only when this process owns the lock.
 */
async function acquireRecoveryLock(client) {
  const result = await client.query(
    "SELECT pg_try_advisory_lock($1, $2) AS acquired",
    [CONTROL_LOCK_NAMESPACE, CONTROL_LOCK_KEY],
  );
  if (result.rows[0]?.acquired !== true) {
    throw new Error("Another Broward recovery process already owns the lock");
  }
}

/**
 * Reject a changed official seed once any verified chunk has committed.
 *
 * @param {import("pg").Client} client - Verified control client.
 * @param {string} seedSignature - Ordered full-seed SHA-256 for this run.
 * @returns {Promise<void>} Resolves only when prior chunks use the same seed.
 */
async function assertSeedSignatureCompatible(client, seedSignature) {
  const result = await client.query(
    `SELECT DISTINCT seed_signature
     FROM ${CONTROL_SCHEMA}.broward_appraisal_chunks`,
  );
  if (
    result.rows.some(
      (row) =>
        typeof row.seed_signature !== "string" ||
        row.seed_signature !== seedSignature,
    )
  ) {
    throw new Error(
      "Official Broward seed signature differs from durable recovery",
    );
  }
}

/**
 * Read source-of-truth folios and checkpointed seed hashes for reboot resume.
 *
 * A property row alone is not considered complete: the patched loader commits
 * logical tables separately. If a VM disappears after `properties` commits but
 * before a later table, the missing completed hash forces an idempotent replay.
 *
 * @param {import("pg").Client} client - Verified control client.
 * @returns {Promise<DurableCompletion>}
 *   Durable completion sets.
 */
async function readDurableCompletion(client) {
  const [loadedResult, completedResult, terminalResult] = await Promise.all([
    client.query(
      `SELECT request_identifier
       FROM public.properties
       WHERE source_system = $1
         AND request_identifier IS NOT NULL`,
      [SOURCE_SYSTEM],
    ),
    client.query(
      `SELECT seed_key_hash
       FROM ${CONTROL_SCHEMA}.broward_appraisal_completed_items`,
    ),
    client.query(
      `SELECT seed_key_hash
       FROM ${CONTROL_SCHEMA}.broward_appraisal_terminal_items`,
    ),
  ]);
  return {
    loadedFolios: new Set(
      loadedResult.rows.flatMap((row) =>
        typeof row.request_identifier === "string"
          ? [row.request_identifier]
          : [],
      ),
    ),
    completedHashes: new Set(
      completedResult.rows.flatMap((row) =>
        typeof row.seed_key_hash === "string" ? [row.seed_key_hash] : [],
      ),
    ),
    terminalHashes: new Set(
      terminalResult.rows.flatMap((row) =>
        typeof row.seed_key_hash === "string" ? [row.seed_key_hash] : [],
      ),
    ),
  };
}

/**
 * Hash a folio for durable terminal-miss tracking without retaining it in logs.
 *
 * @param {string} folio - Canonical Broward folio.
 * @returns {string} Hex SHA-256 seed key.
 */
export function hashSeedKey(folio) {
  return createHash("sha256")
    .update(`${SOURCE_SYSTEM}\0${folio}`)
    .digest("hex");
}

/**
 * Decide whether a seed still needs a source/transform/load replay.
 *
 * Visible property rows are intentionally not an input. The query-db loader
 * commits one logical table at a time, so only a post-verification completed
 * hash or a terminal source-miss hash is safe to skip after a reboot.
 *
 * @param {string} folio - Canonical Broward folio.
 * @param {ReadonlySet<string>} completedHashes - Fully verified seed-key hashes.
 * @param {ReadonlySet<string>} terminalHashes - Confirmed terminal seed-key hashes.
 * @returns {boolean} True when the folio must be processed or replayed.
 */
export function isSeedPending(folio, completedHashes, terminalHashes) {
  const seedKeyHash = hashSeedKey(folio);
  return !completedHashes.has(seedKeyHash) && !terminalHashes.has(seedKeyHash);
}

/**
 * Select the stable 25-folio pilot plus official-seed fallbacks.
 *
 * Fallbacks make a fresh 50-property gate reconstructable even when a GIS folio
 * has disappeared from the detail API. Only a bounded prefix is retained.
 *
 * @param {string} seedPath - Full official seed.
 * @returns {Promise<SeedCandidate[]>} Ordered pilot candidate pool.
 */
async function readPilotCandidates(seedPath) {
  const priority = new Map();
  const pilotOrder = new Map(
    BROWARD_PILOT_FOLIOS.map((folio, index) => [folio, index]),
  );
  /** @type {SeedCandidate[]} */
  const fallbacks = [];
  let sourceRowIndex = 0;
  const parser = createReadStream(seedPath).pipe(
    parse({ columns: true, skip_empty_lines: true }),
  );
  for await (const parsed of parser) {
    const row = /** @type {CsvRecord} */ (parsed);
    const folio = normalizeBrowardFolio(row.request_identifier);
    if (folio === undefined) {
      throw new Error("Invalid folio while selecting pilot candidates");
    }
    const candidate = { sourceRowIndex, folio, row };
    if (pilotOrder.has(folio)) priority.set(folio, candidate);
    else if (fallbacks.length < PILOT_FALLBACK_COUNT) fallbacks.push(candidate);
    sourceRowIndex += 1;
  }
  if (priority.size !== BROWARD_PILOT_FOLIOS.length) {
    throw new Error(
      "Official seed is missing one or more curated pilot folios",
    );
  }
  return [
    ...BROWARD_PILOT_FOLIOS.map((folio) => {
      const candidate = priority.get(folio);
      if (candidate === undefined) {
        throw new Error("Curated pilot candidate disappeared");
      }
      return candidate;
    }),
    ...fallbacks,
  ];
}

/**
 * Stream uncompleted full-seed rows in official order.
 *
 * @param {string} seedPath - Full official GIS seed.
 * @param {ReadonlySet<string>} completedHashes - Fully verified seed-key hashes.
 * @param {ReadonlySet<string>} terminalHashes - Confirmed source-miss hashes.
 * @returns {AsyncGenerator<SeedCandidate, void, void>} Missing candidates.
 */
async function* streamPendingCandidates(
  seedPath,
  completedHashes,
  terminalHashes,
) {
  let sourceRowIndex = 0;
  const parser = createReadStream(seedPath).pipe(
    parse({ columns: true, skip_empty_lines: true }),
  );
  for await (const parsed of parser) {
    const row = /** @type {CsvRecord} */ (parsed);
    const folio = normalizeBrowardFolio(row.request_identifier);
    if (folio === undefined) {
      throw new Error("Invalid folio while streaming full recovery");
    }
    if (isSeedPending(folio, completedHashes, terminalHashes)) {
      yield { sourceRowIndex, folio, row };
    }
    sourceRowIndex += 1;
  }
}

/**
 * Write one bounded private seed without logging identifiers.
 *
 * @param {string} seedPath - Destination CSV.
 * @param {readonly SeedCandidate[]} candidates - Ordered chunk rows.
 * @returns {Promise<void>} Resolves after durable local write.
 */
async function writeChunkSeed(seedPath, candidates) {
  await writeFile(
    seedPath,
    `${SEED_COLUMNS.join(",")}\n${candidates
      .map((candidate) =>
        renderCsvRow(/** @type {Record<string, string>} */ (candidate.row)),
      )
      .join("")}`,
    { encoding: "utf8", mode: 0o600 },
  );
}

/**
 * Parse the explicitly redacted parcel result journal.
 *
 * @param {string} resultsPath - Private bounded result path.
 * @param {number} candidateCount - Maximum accepted row index.
 * @returns {Promise<RedactedIngestResult[]>} Validated row outcomes.
 */
async function readRedactedResults(resultsPath, candidateCount) {
  const text = await readFile(resultsPath, "utf8");
  /** @type {RedactedIngestResult[]} */
  const results = [];
  for (const line of text.split(/\r?\n/u)) {
    if (line.trim() === "") continue;
    const parsed = /** @type {Record<string, unknown>} */ (JSON.parse(line));
    if (
      !Number.isInteger(parsed.rowIndex) ||
      Number(parsed.rowIndex) < 0 ||
      Number(parsed.rowIndex) >= candidateCount ||
      ![
        "succeeded",
        "skipped_existing",
        "source_error",
        "transform_error",
      ].includes(String(parsed.status)) ||
      !["source_miss", "source_error", "transform_error", null].includes(
        parsed.failureClass === null ? null : String(parsed.failureClass),
      ) ||
      "folio" in parsed ||
      "error" in parsed
    ) {
      throw new Error("Recovery result journal is not aggregate-safe");
    }
    results.push(/** @type {RedactedIngestResult} */ (parsed));
  }
  if (results.length !== candidateCount) {
    throw new Error("Recovery result journal does not cover the bounded chunk");
  }
  return results.sort((left, right) => left.rowIndex - right.rowIndex);
}

/**
 * Load the patched query-db mapper module from the required checkout.
 *
 * @param {string} queryDbDirectory - Patched query-db root.
 * @returns {Promise<{ mapAppraisalArtifactZip: (params: {
 *   artifactBuffer: Buffer,
 *   artifactUri: string,
 *   countyName: string,
 *   sourceSystem: string,
 *   stateCode: string
 * }) => { rows: readonly { tableName: string, values: Record<string, unknown> }[] } }>}
 *   Runtime mapper API.
 */
async function loadQueryDbMapper(queryDbDirectory) {
  const modulePath = path.join(queryDbDirectory, "dist", "loader", "index.js");
  const loaded = /** @type {Record<string, unknown>} */ (
    await import(pathToFileURL(modulePath).href)
  );
  if (typeof loaded.mapAppraisalArtifactZip !== "function") {
    throw new Error("Patched query-db appraisal mapper is unavailable");
  }
  return /** @type {Awaited<ReturnType<typeof loadQueryDbMapper>>} */ (loaded);
}

/**
 * Validate data-only artifacts and derive exact logical source keys.
 *
 * @param {object} params - Chunk mapping inputs.
 * @param {readonly { folio: string, canonicalPath: string }[]} params.artifacts
 *   Stable canonical artifact files.
 * @param {string} params.queryDbDirectory - Patched query-db checkout.
 * @returns {Promise<ExpectedChunkRows>} Pre-load reconciliation evidence.
 */
async function mapExpectedChunkRows({ artifacts, queryDbDirectory }) {
  const { mapAppraisalArtifactZip } = await loadQueryDbMapper(queryDbDirectory);
  /** @type {Map<string, Set<string>>} */
  const sourceKeysByTable = new Map();
  const propertyFolios = new Set();
  let preparedRows = 0;
  let propertyRows = 0;
  for (const artifact of artifacts) {
    const inspection = await inspectQueryDataOnlyArtifact(
      artifact.canonicalPath,
    );
    if (inspection.manifest.folio !== artifact.folio) {
      throw new Error("Query-data-only marker does not match its seed folio");
    }
    const mapped = mapAppraisalArtifactZip({
      artifactBuffer: await readFile(artifact.canonicalPath),
      artifactUri: pathToFileURL(artifact.canonicalPath).href,
      countyName: "Broward",
      sourceSystem: SOURCE_SYSTEM,
      stateCode: "FL",
    });
    preparedRows += mapped.rows.length;
    for (const row of mapped.rows) {
      const sourceRecordKey = row.values.source_record_key;
      if (typeof sourceRecordKey !== "string" || sourceRecordKey.length === 0) {
        throw new Error("Prepared Broward row has no deterministic source key");
      }
      const keys = sourceKeysByTable.get(row.tableName) ?? new Set();
      keys.add(sourceRecordKey);
      sourceKeysByTable.set(row.tableName, keys);
      if (row.tableName === "properties") {
        propertyRows += 1;
        const folio = row.values.request_identifier;
        if (typeof folio !== "string") {
          throw new Error(
            "Prepared Broward property has no request identifier",
          );
        }
        propertyFolios.add(folio);
      }
    }
  }
  if (
    propertyRows !== artifacts.length ||
    propertyFolios.size !== artifacts.length
  ) {
    throw new Error("Prepared chunk is not one property per distinct folio");
  }
  return {
    preparedRows,
    propertyRows,
    distinctFolios: propertyFolios.size,
    sourceKeysByTable,
  };
}

/**
 * Run the patched local bulk loader and return its prepared-row count.
 *
 * @param {object} params - Loader paths and bounds.
 * @param {RecoveryOptions} params.options - Recovery configuration.
 * @param {string} params.canonicalDirectory - Stable canonical artifact root.
 * @param {string} params.stageDirectory - Disposable loader stage root.
 * @param {number} params.artifactCount - Exact transformed artifact count.
 * @returns {Promise<number>} Prepared rows reported by the production loader.
 */
async function runPatchedLoader({
  options,
  canonicalDirectory,
  stageDirectory,
  artifactCount,
}) {
  const databaseUrl = process.env.DATABASE_URL_UNPOOLED;
  if (typeof databaseUrl !== "string" || databaseUrl.trim().length === 0) {
    throw new Error("DATABASE_URL_UNPOOLED is required for the loader");
  }
  const stdout = await runCommand({
    command: "npm",
    args: [
      "run",
      "load:bulk",
      "--",
      "--tracks",
      "appraisal",
      "--jurisdiction-key",
      SOURCE_SYSTEM,
      "--appraisal-local-dir",
      canonicalDirectory,
      "--batch-size",
      String(artifactCount),
      "--concurrency",
      String(options.concurrency),
      "--stage-dir",
      stageDirectory,
    ],
    cwd: options.queryDbDirectory,
    env: { ...process.env, DATABASE_URL: databaseUrl },
    stage: "query_db_load",
  });
  let preparedRows = 0;
  for (const line of stdout.split(/\r?\n/u)) {
    if (!line.startsWith("{")) continue;
    try {
      const event = /** @type {Record<string, unknown>} */ (JSON.parse(line));
      if (
        event.event === "appraisal_batch_staged" &&
        typeof event.batchCounters === "object" &&
        event.batchCounters !== null &&
        !Array.isArray(event.batchCounters)
      ) {
        const counters = /** @type {Record<string, unknown>} */ (
          event.batchCounters
        );
        if (typeof counters.preparedRows === "number") {
          preparedRows += counters.preparedRows;
        }
      }
    } catch {
      // Non-JSON child output is ignored; final reconciliation remains strict.
    }
  }
  return preparedRows;
}

/**
 * Verify every prepared source key and direct property identity after commit.
 *
 * @param {import("pg").Client} client - Verified Neon client.
 * @param {ExpectedChunkRows} expected - Exact mapper output.
 * @param {readonly string[]} folios - Transformed folios kept in process memory.
 * @returns {Promise<number>} Total committed unique logical rows.
 */
async function verifyCommittedChunk(client, expected, folios) {
  const propertyResult = await client.query(
    `SELECT
       count(*)::bigint AS property_count,
       count(DISTINCT request_identifier)::bigint AS distinct_folios
     FROM public.properties
     WHERE source_system = $1
       AND request_identifier = ANY($2::text[])`,
    [SOURCE_SYSTEM, folios],
  );
  if (
    Number(propertyResult.rows[0]?.property_count ?? 0) !==
      expected.propertyRows ||
    Number(propertyResult.rows[0]?.distinct_folios ?? 0) !==
      expected.distinctFolios
  ) {
    throw new Error(
      "Committed property and distinct-folio counts do not reconcile",
    );
  }
  let committedRows = 0;
  for (const [tableName, sourceKeys] of expected.sourceKeysByTable) {
    if (!/^[a-z_]+$/u.test(tableName)) {
      throw new Error("Mapper returned an unsafe logical table name");
    }
    const result = await client.query(
      `SELECT count(*)::bigint AS row_count
       FROM public.${tableName}
       WHERE source_system = $1
         AND source_record_key = ANY($2::text[])`,
      [SOURCE_SYSTEM, [...sourceKeys]],
    );
    const rowCount = Number(result.rows[0]?.row_count ?? 0);
    if (rowCount !== sourceKeys.size) {
      throw new Error(
        "Committed logical rows do not match prepared source keys",
      );
    }
    committedRows += rowCount;
  }
  return committedRows;
}

/**
 * Record one failed load attempt without advancing a chunk checkpoint.
 *
 * @param {import("pg").Client} client - Verified Neon client.
 * @returns {Promise<void>} Resolves after the aggregate event commits.
 */
async function recordLoadFailure(client) {
  await client.query(
    `INSERT INTO ${CONTROL_SCHEMA}.broward_appraisal_events (stage, event_count)
     VALUES ('load_error', 1)`,
  );
}

/**
 * Commit terminal misses, aggregate events, and the verified chunk checkpoint.
 *
 * @param {object} params - Durable checkpoint values.
 * @param {import("pg").Client} params.client - Verified Neon client.
 * @param {RecoveryOptions} params.options - Recovery mode and identity.
 * @param {SeedStats} params.seedStats - Reconstructable seed identity.
 * @param {readonly SeedCandidate[]} params.candidates - Attempted private candidates.
 * @param {ChunkResult} params.result - Verified aggregate chunk result.
 * @param {number} params.committedRows - Verified unique logical row total.
 * @returns {Promise<void>} Resolves only after database COMMIT.
 */
async function commitChunkCheckpoint({
  client,
  options,
  seedStats,
  candidates,
  result,
  committedRows,
}) {
  const chunkId = createHash("sha256")
    .update(seedStats.signature)
    .update("\0")
    .update(options.mode)
    .update("\0")
    .update(candidates.map((candidate) => candidate.folio).join("\n"))
    .digest("hex");
  await client.query("BEGIN");
  try {
    for (const folio of result.loadedFolios) {
      await client.query(
        `INSERT INTO ${CONTROL_SCHEMA}.broward_appraisal_completed_items
           (seed_key_hash, outcome)
         VALUES ($1, 'loaded')
         ON CONFLICT (seed_key_hash) DO NOTHING`,
        [hashSeedKey(folio)],
      );
    }
    for (const seedKeyHash of result.terminalFolioHashes) {
      await client.query(
        `INSERT INTO ${CONTROL_SCHEMA}.broward_appraisal_terminal_items
           (seed_key_hash, outcome)
         VALUES ($1, 'source_miss')
         ON CONFLICT (seed_key_hash) DO NOTHING`,
        [seedKeyHash],
      );
    }
    for (const [stage, eventCount] of [
      ["source_miss", result.sourceMisses],
      ["source_error", result.sourceErrors],
      ["transform_error", result.transformErrors],
    ]) {
      if (eventCount === 0) continue;
      await client.query(
        `INSERT INTO ${CONTROL_SCHEMA}.broward_appraisal_events
           (stage, event_count)
         VALUES ($1, $2)`,
        [stage, eventCount],
      );
    }
    await client.query(
      `INSERT INTO ${CONTROL_SCHEMA}.broward_appraisal_chunks (
         chunk_id,
         run_mode,
         seed_signature,
         attempted_count,
         property_count,
         distinct_folio_count,
         prepared_row_count,
         committed_row_count,
         source_miss_count,
         source_error_count,
         transform_error_count,
         branch_id,
         endpoint_id
       ) VALUES ($1, $2, $3, $4, $5, $5, $6, $7, $8, $9, $10, $11, $12)
       ON CONFLICT (chunk_id) DO NOTHING`,
      [
        chunkId,
        options.mode,
        seedStats.signature,
        result.attempted,
        result.loaded,
        result.preparedRows,
        committedRows,
        result.sourceMisses,
        result.sourceErrors,
        result.transformErrors,
        options.expectedBranchId,
        options.expectedEndpointId,
      ],
    );
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

/**
 * Capture, warm-transform, load, verify, checkpoint, and clean one chunk.
 *
 * @param {object} params - Chunk dependencies.
 * @param {import("pg").Client} params.client - Verified Neon client.
 * @param {RecoveryOptions} params.options - Runtime paths and bounds.
 * @param {SeedStats} params.seedStats - Stable full-seed identity.
 * @param {readonly SeedCandidate[]} params.candidates - Bounded source rows.
 * @returns {Promise<ChunkResult>} Verified aggregate outcome.
 */
async function processChunk({ client, options, seedStats, candidates }) {
  const activeDirectory = path.join(options.workDirectory, "active-chunk");
  const ingestDirectory = path.join(
    activeDirectory,
    "query-data-only-ingestion",
  );
  const canonicalDirectory = path.join(
    options.workDirectory,
    "canonical-artifacts",
  );
  const stageDirectory = path.join(activeDirectory, "loader-stage");
  await rm(activeDirectory, { recursive: true, force: true });
  await rm(canonicalDirectory, { recursive: true, force: true });
  await mkdir(activeDirectory, { recursive: true, mode: 0o700 });
  await mkdir(canonicalDirectory, { recursive: true, mode: 0o700 });
  const chunkSeedPath = path.join(activeDirectory, "seed.csv");
  await writeChunkSeed(chunkSeedPath, candidates);

  await runLocalIngestion({
    seedPath: chunkSeedPath,
    scriptsDirectory: options.scriptsDirectory,
    outputDirectory: ingestDirectory,
    concurrency: options.concurrency,
    limit: null,
    resetCheckpoint: true,
    artifactMode: QUERY_DATA_ONLY_MODE,
    captureSource: null,
    startRow: 0,
    redactResults: true,
  });
  const results = await readRedactedResults(
    path.join(ingestDirectory, "results.ndjson"),
    candidates.length,
  );
  /** @type {{ folio: string, canonicalPath: string }[]} */
  const artifacts = [];
  /** @type {string[]} */
  const loadedFolios = [];
  /** @type {string[]} */
  const terminalFolioHashes = [];
  let sourceMisses = 0;
  let sourceErrors = 0;
  let transformErrors = 0;
  for (const result of results) {
    const candidate = candidates[result.rowIndex];
    if (candidate === undefined) {
      throw new Error("Result row does not resolve to a private candidate");
    }
    if (result.status === "succeeded") {
      const sourcePath = path.join(
        ingestDirectory,
        "query-data-only-artifacts",
        candidate.folio.slice(0, 4),
        `${candidate.folio}${QUERY_DATA_ONLY_SUFFIX}`,
      );
      const canonicalPath = path.join(
        canonicalDirectory,
        candidate.folio.slice(0, 4),
        `${candidate.folio}.zip`,
      );
      await mkdir(path.dirname(canonicalPath), {
        recursive: true,
        mode: 0o700,
      });
      await copyFile(sourcePath, canonicalPath);
      artifacts.push({ folio: candidate.folio, canonicalPath });
      loadedFolios.push(candidate.folio);
    } else if (result.failureClass === "source_miss") {
      sourceMisses += 1;
      terminalFolioHashes.push(hashSeedKey(candidate.folio));
    } else if (result.failureClass === "source_error") {
      sourceErrors += 1;
    } else if (result.failureClass === "transform_error") {
      transformErrors += 1;
    } else {
      throw new Error("Failed recovery result has no safe failure class");
    }
  }

  let preparedRows = 0;
  let committedRows = 0;
  if (artifacts.length > 0) {
    const expected = await mapExpectedChunkRows({
      artifacts,
      queryDbDirectory: options.queryDbDirectory,
    });
    try {
      const loaderPreparedRows = await runPatchedLoader({
        options,
        canonicalDirectory,
        stageDirectory,
        artifactCount: artifacts.length,
      });
      if (loaderPreparedRows !== expected.preparedRows) {
        throw new Error(
          "Loader prepared-row count differs from mapper evidence",
        );
      }
      committedRows = await verifyCommittedChunk(
        client,
        expected,
        loadedFolios,
      );
      preparedRows = expected.preparedRows;
    } catch (error) {
      await recordLoadFailure(client);
      throw error;
    }
  }
  const result = {
    attempted: candidates.length,
    loaded: artifacts.length,
    sourceMisses,
    sourceErrors,
    transformErrors,
    preparedRows,
    loadedFolios,
    terminalFolioHashes,
  };
  await commitChunkCheckpoint({
    client,
    options,
    seedStats,
    candidates,
    result,
    committedRows,
  });
  await rm(activeDirectory, { recursive: true, force: true });
  await rm(canonicalDirectory, { recursive: true, force: true });
  return result;
}

/**
 * Assert that the durable pilot gate is exactly 50 properties and folios.
 *
 * @param {import("pg").Client} client - Verified Neon client.
 * @param {ReadonlySet<string>} completedHashes - Fully verified seed-key hashes.
 * @returns {Promise<void>} Resolves only for the exact accepted count and completion set.
 */
async function assertPilotLoaded(client, completedHashes) {
  const result = await client.query(
    `SELECT
       request_identifier
     FROM public.properties
     WHERE source_system = $1
       AND request_identifier IS NOT NULL`,
    [SOURCE_SYSTEM],
  );
  const folios = result.rows.flatMap((row) =>
    typeof row.request_identifier === "string" ? [row.request_identifier] : [],
  );
  const distinctFolios = new Set(folios);
  if (
    folios.length !== PILOT_PROPERTY_COUNT ||
    distinctFolios.size !== PILOT_PROPERTY_COUNT ||
    folios.some((folio) => !completedHashes.has(hashSeedKey(folio)))
  ) {
    throw new Error(
      "Pilot gate requires exactly 50 fully verified properties and folios",
    );
  }
}

/**
 * Commit the exact pilot gate after all 50 property hashes are durable.
 *
 * @param {import("pg").Client} client - Verified Neon client.
 * @param {RecoveryOptions} options - Verified branch identity.
 * @param {SeedStats} seedStats - Exact official-seed identity.
 * @returns {Promise<void>} Resolves after the pilot-gate transaction commits.
 */
async function commitPilotGate(client, options, seedStats) {
  await client.query("BEGIN");
  try {
    await client.query(
      `INSERT INTO ${CONTROL_SCHEMA}.broward_appraisal_gates (
         gate_name,
         seed_signature,
         property_count,
         distinct_folio_count,
         branch_id,
         endpoint_id
       ) VALUES ('pilot-50', $1, 50, 50, $2, $3)
       ON CONFLICT (gate_name) DO NOTHING`,
      [
        seedStats.signature,
        options.expectedBranchId,
        options.expectedEndpointId,
      ],
    );
    const result = await client.query(
      `SELECT
         seed_signature,
         property_count,
         distinct_folio_count,
         branch_id,
         endpoint_id
       FROM ${CONTROL_SCHEMA}.broward_appraisal_gates
       WHERE gate_name = 'pilot-50'`,
    );
    const row = result.rows[0];
    if (
      row?.seed_signature !== seedStats.signature ||
      Number(row.property_count) !== PILOT_PROPERTY_COUNT ||
      Number(row.distinct_folio_count) !== PILOT_PROPERTY_COUNT ||
      row.branch_id !== options.expectedBranchId ||
      row.endpoint_id !== options.expectedEndpointId
    ) {
      throw new Error("Existing pilot gate does not match this recovery");
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

/**
 * Run or resume the exact 50-property pilot.
 *
 * @param {object} params - Pilot dependencies.
 * @param {import("pg").Client} params.client - Verified Neon client.
 * @param {RecoveryOptions} params.options - Recovery configuration.
 * @param {SeedStats} params.seedStats - Full seed identity.
 * @param {Set<string>} params.loadedFolios - Existing property identifiers.
 * @param {Set<string>} params.completedHashes - Fully verified seed-key hashes.
 * @param {Set<string>} params.terminalHashes - Confirmed source misses.
 * @returns {Promise<void>} Resolves only after the 50/50 gate passes.
 */
async function runPilot({
  client,
  options,
  seedStats,
  loadedFolios,
  completedHashes,
  terminalHashes,
}) {
  if (loadedFolios.size > PILOT_PROPERTY_COUNT) {
    throw new Error(
      "Pilot mode refuses a branch with more than 50 Broward properties",
    );
  }
  const candidates = await readPilotCandidates(options.seedPath);
  const pilotIsIncomplete = () =>
    loadedFolios.size < PILOT_PROPERTY_COUNT ||
    [...loadedFolios].some((folio) => !completedHashes.has(hashSeedKey(folio)));
  while (pilotIsIncomplete()) {
    /** @type {SeedCandidate[]} */
    const selected = [];
    let newPropertySlots = PILOT_PROPERTY_COUNT - loadedFolios.size;
    for (const candidate of candidates) {
      if (!isSeedPending(candidate.folio, completedHashes, terminalHashes)) {
        continue;
      }
      if (loadedFolios.has(candidate.folio)) {
        selected.push(candidate);
      } else if (newPropertySlots > 0) {
        selected.push(candidate);
        newPropertySlots -= 1;
      }
      if (selected.length >= options.chunkSize) break;
    }
    if (selected.length === 0) {
      throw new Error("Pilot candidate pool exhausted before 50 properties");
    }
    const result = await processChunk({
      client,
      options,
      seedStats,
      candidates: selected,
    });
    for (const folio of result.loadedFolios) loadedFolios.add(folio);
    for (const folio of result.loadedFolios) {
      completedHashes.add(hashSeedKey(folio));
    }
    for (const hash of result.terminalFolioHashes) terminalHashes.add(hash);
    console.log(
      JSON.stringify({
        event: "broward_recovery_chunk_committed",
        mode: "pilot",
        attempted: result.attempted,
        loaded: result.loaded,
        preparedRows: result.preparedRows,
        sourceMisses: result.sourceMisses,
        sourceErrors: result.sourceErrors,
        transformErrors: result.transformErrors,
        durableProperties: loadedFolios.size,
      }),
    );
    if (result.sourceErrors > 0 || result.transformErrors > 0) {
      throw new Error(
        "Pilot stopped after retryable source or transform failures",
      );
    }
  }
  await assertPilotLoaded(client, completedHashes);
  await commitPilotGate(client, options, seedStats);
}

/**
 * Require an already committed 50-property pilot before full recovery.
 *
 * @param {import("pg").Client} client - Verified Neon client.
 * @param {RecoveryOptions} options - Verified branch identity.
 * @param {SeedStats} seedStats - Exact official-seed identity.
 * @returns {Promise<void>} Resolves only when a durable pilot chunk proves the gate.
 */
async function requirePilotCheckpoint(client, options, seedStats) {
  const result = await client.query(
    `SELECT
       seed_signature,
       property_count,
       distinct_folio_count,
       branch_id,
       endpoint_id
     FROM ${CONTROL_SCHEMA}.broward_appraisal_gates
     WHERE gate_name = 'pilot-50'`,
  );
  const row = result.rows[0];
  if (
    row?.seed_signature !== seedStats.signature ||
    Number(row.property_count) !== PILOT_PROPERTY_COUNT ||
    Number(row.distinct_folio_count) !== PILOT_PROPERTY_COUNT ||
    row.branch_id !== options.expectedBranchId ||
    row.endpoint_id !== options.expectedEndpointId
  ) {
    throw new Error(
      "Full recovery requires a committed 50-property pilot checkpoint",
    );
  }
}

/**
 * Run one full official-seed pass, skipping every durable Neon source key.
 *
 * @param {object} params - Full-run dependencies.
 * @param {import("pg").Client} params.client - Verified Neon client.
 * @param {RecoveryOptions} params.options - Recovery configuration.
 * @param {SeedStats} params.seedStats - Full seed identity.
 * @param {Set<string>} params.loadedFolios - Existing property identifiers.
 * @param {Set<string>} params.completedHashes - Fully verified seed-key hashes.
 * @param {Set<string>} params.terminalHashes - Confirmed source misses.
 * @returns {Promise<void>} Resolves after one complete seed scan.
 */
async function runFull({
  client,
  options,
  seedStats,
  loadedFolios,
  completedHashes,
  terminalHashes,
}) {
  await requirePilotCheckpoint(client, options, seedStats);
  /** @type {SeedCandidate[]} */
  let chunk = [];
  const flush = async () => {
    if (chunk.length === 0) return;
    const candidates = chunk;
    chunk = [];
    const result = await processChunk({
      client,
      options,
      seedStats,
      candidates,
    });
    for (const folio of result.loadedFolios) loadedFolios.add(folio);
    for (const folio of result.loadedFolios) {
      completedHashes.add(hashSeedKey(folio));
    }
    for (const hash of result.terminalFolioHashes) terminalHashes.add(hash);
    console.log(
      JSON.stringify({
        event: "broward_recovery_chunk_committed",
        mode: "full",
        attempted: result.attempted,
        loaded: result.loaded,
        preparedRows: result.preparedRows,
        sourceMisses: result.sourceMisses,
        sourceErrors: result.sourceErrors,
        transformErrors: result.transformErrors,
        durableProperties: loadedFolios.size,
        durableTerminalMisses: terminalHashes.size,
        durableCompleted: loadedFolios.size + terminalHashes.size,
        denominator: BROWARD_ROW_DENOMINATOR,
      }),
    );
  };
  for await (const candidate of streamPendingCandidates(
    options.seedPath,
    completedHashes,
    terminalHashes,
  )) {
    chunk.push(candidate);
    if (chunk.length >= options.chunkSize) await flush();
  }
  await flush();
}

/**
 * Execute the durable Broward recovery after all safety checks.
 *
 * @param {RecoveryOptions} options - Validated CLI configuration.
 * @returns {Promise<void>} Resolves after the selected pilot or full pass.
 */
export async function runRecovery(options) {
  const resolved = {
    ...options,
    seedPath: path.resolve(options.seedPath),
    scriptsDirectory: path.resolve(options.scriptsDirectory),
    queryDbDirectory: path.resolve(options.queryDbDirectory),
    workDirectory: path.resolve(options.workDirectory),
  };
  await ensureOfficialSeed(resolved.seedPath);
  const seedStats = await readSeedStats(resolved.seedPath);
  assertFullSeed(seedStats);
  const client = await connectToNeon();
  try {
    const identity = await verifyNeonTarget(client, resolved);
    console.log(
      JSON.stringify({
        event: "broward_neon_safety_gate_passed",
        branchLabel: "broward-ingest",
        projectId: identity.projectId,
        branchId: identity.branchId,
        endpointId: identity.endpointId,
        existingProperties: identity.propertyCount,
        existingDistinctFolios: identity.distinctFolios,
        seedRows: seedStats.rowCount,
        seedDistinctFolios: seedStats.distinctFolios,
        sourceConcurrencyMaximum: resolved.concurrency,
      }),
    );
    await ensureControlTables(client);
    await assertSeedSignatureCompatible(client, seedStats.signature);
    await acquireRecoveryLock(client);
    const completion = await readDurableCompletion(client);
    if (resolved.mode === "pilot") {
      await runPilot({
        client,
        options: resolved,
        seedStats,
        ...completion,
      });
    } else {
      await runFull({
        client,
        options: resolved,
        seedStats,
        ...completion,
      });
    }
    console.log(
      JSON.stringify({
        event: "broward_recovery_pass_finished",
        mode: resolved.mode,
        durableProperties: completion.loadedFolios.size,
        durableTerminalMisses: completion.terminalHashes.size,
      }),
    );
  } finally {
    await client.end();
  }
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  runRecovery(parseRecoveryOptions(process.argv.slice(2))).catch((error) => {
    console.error(
      JSON.stringify({
        event: "broward_recovery_failed",
        message:
          error instanceof Error
            ? error.message.replace(/[A-Z0-9]{12}/gu, "[redacted]")
            : "Unknown recovery failure",
      }),
    );
    process.exitCode = 1;
  });
}
