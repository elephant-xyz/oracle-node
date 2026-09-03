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
import { performance } from "node:perf_hooks";
import { pathToFileURL } from "node:url";

import pg from "pg";

import {
  BROWARD_ACCELA_ADAPTER_KEY,
  BROWARD_BCS_ADAPTER_KEY,
  BROWARD_CITIZENSERVE_ADAPTER_KEY,
  BROWARD_PERMIT_JURISDICTIONS,
  BROWARD_PERMIT_REGISTRY_VERSION,
  BROWARD_TYLER_CIVIC_ACCESS_ADAPTER_KEY,
  resolveBrowardPermitJurisdiction,
} from "./broward-permit-jurisdictions.mjs";
import { loadBrowardPermitPilotToNeon } from "./load-broward-permit-pilot-to-neon.mjs";
import {
  closeCitizenserveBrowser,
  createCitizenserveBrowser,
} from "./permit-source-adapters/citizenserve.mjs";
import { runProbe as runMunicipalPermitProbe } from "./probe-broward-municipal-permits.mjs";

const { Client } = pg;
const EXPECTED_PROJECT_ID = "raspy-frost-51580436";
const PRODUCTION_ENDPOINT_PREFIX = "ep-mute-leaf";
const CONTROL_SCHEMA = "ingest_control";
const LOCK_NAMESPACE = 12_011;
const MAX_CONCURRENCY = 4;
const DEFAULT_PROBE_TIMEOUT_MS = 15 * 60_000;
const DEFAULT_WORK_DIR = "downloads/broward/supported-permit-ingest";
const DEFAULT_WARM_BROWSER_MAX_IDLE_MS = 5 * 60_000;
const DEFAULT_WARM_BROWSER_MAX_CLOCK_SKEW_MS = 30_000;
const DEFAULT_WARM_BROWSER_MAX_OPERATION_WALL_MS = 10 * 60_000;
const CITIZENSERVE_HOST_CONCURRENCY_KEY = "host:www6.citizenserve.com";
const ADAPTER_LOCK_KEYS = new Map([
  [BROWARD_BCS_ADAPTER_KEY, 41],
  [BROWARD_CITIZENSERVE_ADAPTER_KEY, 42],
  [BROWARD_ACCELA_ADAPTER_KEY, 43],
  [BROWARD_TYLER_CIVIC_ACCESS_ADAPTER_KEY, 44],
]);
const BROWARD_BCS_SOURCE_SYSTEM = "broward_county_bcs_posse_permits";
const TERMINAL_STATUSES = new Set([
  "records",
  "no_permits",
  "truncated",
  "failed_exhausted",
]);
const WARM_CITIZENSERVE_BROWSER_KEYS = new Map([
  ["lauderdale-by-the-sea", "www6.citizenserve.com:117"],
  ["southwest-ranches", "www6.citizenserve.com:117"],
]);

/**
 * @typedef {"records" | "no_permits" | "truncated" | "failed" | "failed_exhausted"} SupportedPermitItemStatus
 * @typedef {"all" | "roofing"} SupportedPermitScope
 * @typedef {"enabled" | "disabled"} SupportedPermitBrowserReuse
 *
 * @typedef {object} SupportedPermitOptions
 * @property {string} jobId - Stable operator-selected job identifier.
 * @property {number} concurrency - Maximum simultaneous jurisdiction probes.
 * @property {number} maxAttempts - Attempts before one source result is terminal.
 * @property {number | null} limit - Optional deterministic candidate cap.
 * @property {number | null} maxItems - Optional work cap for this invocation only.
 * @property {string} workDirectory - Private source artifact and checkpoint root.
 * @property {SupportedPermitScope} scope - All bounded permits or roofing-only.
 * @property {readonly string[]} jurisdictionKeys - Exact current registry routes.
 * @property {string | null} migrateFromJobId - Compatible prior checkpoint job.
 * @property {SupportedPermitBrowserReuse} browserReuse
 *   Whether the measured-safe Citizenserve route allowlist may retain a browser
 *   process between sequential properties. `disabled` is an immediate
 *   checkpoint-compatible fallback.
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
 * @property {boolean} timedOut - Whether the hard probe deadline terminated it.
 *
 * @typedef {object} WarmCitizenserveBrowserMetrics
 * @property {number} launches - New Chromium processes opened by this pool.
 * @property {number} reuses - Sequential operations that reused a connected process.
 * @property {number} invalidations - Browsers discarded after source errors, excessive idle time, or unsafe clock movement.
 *
 * @typedef {object} WarmCitizenserveBrowserPool
 * @property {<Result>(key:string,operation:(browser:import("puppeteer").Browser)=>Promise<Result>)=>Promise<Result>} run
 *   Run one sequential, caller-supplied property operation with a revalidated
 *   browser identified by public host and tenant.
 * @property {()=>WarmCitizenserveBrowserMetrics} snapshot - Return aggregate-only process metrics.
 * @property {()=>Promise<void>} close - Close every retained process.
 *
 * @typedef {object} WarmCitizenserveBrowserEntry
 * @property {import("puppeteer").Browser} browser - Connected browser retained only for its public host/tenant key.
 * @property {number} wallAt - Last safe wall-clock observation in milliseconds.
 * @property {number} monotonicAt - Matching monotonic observation in milliseconds.
 *
 * @typedef {object} MigratedPermitItem
 * @property {string} parcelHash - Compatible one-way property identity.
 * @property {string} jurisdictionKey - Exact current registry route.
 * @property {string} adapterKey - Exact current adapter family.
 * @property {SupportedPermitItemStatus} status - Preserved or retryable state.
 * @property {number} recordCount - Previously committed records.
 * @property {number} attemptCount - Preserved finite source attempts.
 * @property {string | null} errorClass - Aggregate-safe prior failure class.
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
  const maxItemsRaw = values.get("max-items");
  const maxItems =
    maxItemsRaw === undefined
      ? null
      : boundedInteger(maxItemsRaw, "max-items", 1, 1_000_000);
  const workDirectory = values.get("work-dir") ?? DEFAULT_WORK_DIR;
  if (workDirectory.trim() === "") {
    throw new Error("--work-dir must not be empty");
  }
  const scope = values.get("scope") ?? "all";
  if (scope !== "all" && scope !== "roofing") {
    throw new Error("--scope must be all or roofing");
  }
  const jurisdictionKeys = readJurisdictionKeys(values.get("jurisdictions"));
  const migrateFromJobId = values.get("migrate-from-job") ?? null;
  if (
    migrateFromJobId !== null &&
    (!/^broward-permits-[a-z0-9-]+$/u.test(migrateFromJobId) ||
      migrateFromJobId === jobId)
  ) {
    throw new Error(
      "--migrate-from-job must name a different Broward permit job",
    );
  }
  const browserReuse = values.get("browser-reuse") ?? "enabled";
  if (browserReuse !== "enabled" && browserReuse !== "disabled") {
    throw new Error("--browser-reuse must be enabled or disabled");
  }
  return {
    jobId,
    concurrency,
    maxAttempts,
    limit,
    maxItems,
    workDirectory,
    scope,
    jurisdictionKeys,
    migrateFromJobId,
    browserReuse,
  };
}

/**
 * Create a sequential warm-browser pool with explicit suspension and stale
 * session invalidation.
 *
 * Wall and monotonic clocks are sampled together before and after every source
 * operation. A VM sleep/wake jump, clock regression, excessive idle period,
 * disconnected process, or source exception discards the process before the
 * next property. The source operation itself remains responsible for opening a
 * fresh page and re-running the rendered form/challenge/identity checks.
 *
 * @param {object} [dependencies={}] - Injectable browser lifecycle and clocks.
 * @param {()=>Promise<import("puppeteer").Browser>} [dependencies.launchBrowser] - Browser launcher.
 * @param {(browser:import("puppeteer").Browser)=>Promise<void>} [dependencies.closeBrowser] - Best-effort closer.
 * @param {(browser:import("puppeteer").Browser)=>boolean} [dependencies.isConnected] - Synchronous liveness test.
 * @param {()=>number} [dependencies.wallNow] - Wall-clock milliseconds.
 * @param {()=>number} [dependencies.monotonicNow] - Monotonic milliseconds.
 * @param {number} [dependencies.maxIdleMs] - Maximum wall-clock idle interval.
 * @param {number} [dependencies.maxClockSkewMs] - Maximum wall/monotonic disagreement.
 * @param {number} [dependencies.maxOperationWallMs] - Maximum successful operation wall duration retained for reuse.
 * @returns {WarmCitizenserveBrowserPool} Empty sequential browser pool.
 */
export function createWarmCitizenserveBrowserPool(dependencies = {}) {
  const launchBrowser = dependencies.launchBrowser ?? createCitizenserveBrowser;
  const closeBrowser = dependencies.closeBrowser ?? closeCitizenserveBrowser;
  const isConnected =
    dependencies.isConnected ?? ((browser) => browser.connected);
  const wallNow = dependencies.wallNow ?? Date.now;
  const monotonicNow = dependencies.monotonicNow ?? (() => performance.now());
  const maxIdleMs = dependencies.maxIdleMs ?? DEFAULT_WARM_BROWSER_MAX_IDLE_MS;
  const maxClockSkewMs =
    dependencies.maxClockSkewMs ?? DEFAULT_WARM_BROWSER_MAX_CLOCK_SKEW_MS;
  const maxOperationWallMs =
    dependencies.maxOperationWallMs ??
    DEFAULT_WARM_BROWSER_MAX_OPERATION_WALL_MS;
  for (const [name, value] of [
    ["maxIdleMs", maxIdleMs],
    ["maxClockSkewMs", maxClockSkewMs],
    ["maxOperationWallMs", maxOperationWallMs],
  ]) {
    if (!Number.isInteger(value) || value < 1_000) {
      throw new Error(`${name} must be an integer of at least 1000`);
    }
  }

  /** @type {Map<string,WarmCitizenserveBrowserEntry>} */
  const entries = new Map();
  /** @type {Map<string,Promise<void>>} */
  const operationTails = new Map();
  /** @type {WarmCitizenserveBrowserMetrics} */
  const metrics = { launches: 0, reuses: 0, invalidations: 0 };

  /**
   * Close and remove one unsafe retained browser.
   *
   * @param {string} key - Public host/tenant pool key.
   * @returns {Promise<void>} Resolves after best-effort cleanup.
   */
  const invalidate = async (key) => {
    const entry = entries.get(key);
    if (entry === undefined) return;
    entries.delete(key);
    metrics.invalidations += 1;
    await closeBrowser(entry.browser);
  };

  /**
   * Test paired clock observations for safe browser reuse.
   *
   * @param {WarmCitizenserveBrowserEntry} entry - Retained browser state.
   * @param {number} wallAt - Current wall-clock observation.
   * @param {number} monotonicAt - Current monotonic observation.
   * @returns {boolean} True only when time and process state remain safe.
   */
  const reusable = (entry, wallAt, monotonicAt) => {
    const wallElapsed = wallAt - entry.wallAt;
    const monotonicElapsed = monotonicAt - entry.monotonicAt;
    return (
      isConnected(entry.browser) &&
      wallElapsed >= 0 &&
      monotonicElapsed >= 0 &&
      wallElapsed <= maxIdleMs &&
      Math.abs(wallElapsed - monotonicElapsed) <= maxClockSkewMs
    );
  };

  /**
   * Execute one operation after its public host/tenant queue is acquired.
   *
   * @template Result
   * @param {string} key - Public host/tenant pool key.
   * @param {(browser:import("puppeteer").Browser)=>Promise<Result>} operation
   *   Complete bounded property probe.
   * @returns {Promise<Result>} Exact operation result.
   */
  const execute = async (key, operation) => {
    const acquisitionWall = wallNow();
    const acquisitionMonotonic = monotonicNow();
    let entry = entries.get(key);
    if (
      entry !== undefined &&
      !reusable(entry, acquisitionWall, acquisitionMonotonic)
    ) {
      await invalidate(key);
      entry = undefined;
    }
    if (entry === undefined) {
      entry = {
        browser: await launchBrowser(),
        wallAt: acquisitionWall,
        monotonicAt: acquisitionMonotonic,
      };
      entries.set(key, entry);
      metrics.launches += 1;
    } else {
      metrics.reuses += 1;
    }

    const operationWall = wallNow();
    const operationMonotonic = monotonicNow();
    try {
      const result = await operation(entry.browser);
      const completedWall = wallNow();
      const completedMonotonic = monotonicNow();
      const wallElapsed = completedWall - operationWall;
      const monotonicElapsed = completedMonotonic - operationMonotonic;
      const unsafeElapsed =
        wallElapsed < 0 ||
        monotonicElapsed < 0 ||
        wallElapsed > maxOperationWallMs ||
        Math.abs(wallElapsed - monotonicElapsed) > maxClockSkewMs;
      if (unsafeElapsed) {
        await invalidate(key);
        throw new Error(
          "Warm Citizenserve browser invalidated after unsafe clock movement",
        );
      }
      if (!isConnected(entry.browser)) {
        await invalidate(key);
      } else {
        entry.wallAt = completedWall;
        entry.monotonicAt = completedMonotonic;
      }
      return result;
    } catch (error) {
      await invalidate(key);
      throw error;
    }
  };

  return {
    run: (key, operation) => {
      if (typeof key !== "string" || key.length === 0) {
        return Promise.reject(
          new Error("Warm Citizenserve browser key is required"),
        );
      }
      if (typeof operation !== "function") {
        return Promise.reject(
          new Error("Warm Citizenserve browser operation is required"),
        );
      }
      const prior = operationTails.get(key) ?? Promise.resolve();
      const result = prior.then(() => execute(key, operation));
      const settled = result.then(
        () => undefined,
        () => undefined,
      );
      operationTails.set(key, settled);
      void settled.finally(() => {
        if (operationTails.get(key) === settled) {
          operationTails.delete(key);
        }
      });
      return result;
    },
    snapshot: () => ({ ...metrics }),
    close: async () => {
      await Promise.all([...operationTails.values()]);
      const retained = [...entries.values()];
      entries.clear();
      await Promise.all(retained.map((entry) => closeBrowser(entry.browser)));
    },
  };
}

/**
 * Parse an exact, deduplicated implemented-route allowlist.
 *
 * An omitted value preserves the legacy all-implemented-routes behavior.
 * Operational gap jobs should always provide an explicit allowlist so a
 * registry expansion cannot silently acquire another worker's source.
 *
 * @param {string | undefined} raw - Optional comma-delimited registry keys.
 * @returns {readonly string[]} Sorted current implemented jurisdiction keys.
 */
export function readJurisdictionKeys(raw) {
  const implemented = new Set(
    BROWARD_PERMIT_JURISDICTIONS.filter(
      (entry) =>
        entry.primarySource.status === "implemented" &&
        entry.primarySource.adapterKey !== null,
    ).map((entry) => entry.key),
  );
  const keys =
    raw === undefined
      ? [...implemented]
      : raw
          .split(",")
          .map((value) => value.trim())
          .filter((value) => value.length > 0);
  if (
    keys.length === 0 ||
    new Set(keys).size !== keys.length ||
    keys.some((key) => !implemented.has(key))
  ) {
    throw new Error(
      "--jurisdictions must contain unique implemented Broward registry keys",
    );
  }
  return Object.freeze([...keys].sort());
}

/**
 * Build a long-running Neon client configuration that remains bounded while a
 * source child performs browser/detail work between control queries.
 *
 * TCP keepalive prevents an otherwise idle advisory-lock connection from being
 * silently dropped during a bounded source probe. The client-side query timeout
 * complements PostgreSQL's statement timeout when the network stops delivering
 * a response.
 *
 * @param {string} connectionString - Verified Neon connection URL.
 * @returns {import("pg").ClientConfig} Bounded control-session configuration.
 */
export function supportedPermitClientConfig(connectionString) {
  return {
    connectionString,
    application_name: "broward-supported-permit-ingest",
    connectionTimeoutMillis: 10_000,
    statement_timeout: 120_000,
    query_timeout: 120_000,
    keepAlive: true,
    keepAliveInitialDelayMillis: 10_000,
  };
}

/**
 * Run or resume supported anonymous permit routes.
 *
 * @param {SupportedPermitOptions} options - Durable run configuration.
 * @returns {Promise<{
 *   candidateCount:number,
 *   processed:number,
 *   terminal:number,
 *   failed:number,
 *   cooling:number,
 *   migrated:number,
 *   browserLaunches:number,
 *   browserReuses:number,
 *   browserInvalidations:number
 * }>}
 *   Aggregate run result.
 */
export async function runSupportedPermitIngest(options) {
  const target = requireTarget(process.env);
  const client = new Client(
    supportedPermitClientConfig(target.connectionString),
  );
  let controlConnectionFailed = false;
  client.on("error", () => {
    controlConnectionFailed = true;
  });
  const browserPool =
    options.browserReuse === "enabled"
      ? createWarmCitizenserveBrowserPool()
      : null;
  await client.connect();
  try {
    await verifyTarget(client, target);
    await ensureControlTables(client);
    const candidates = await readCandidates(
      client,
      options.limit,
      new Set(options.jurisdictionKeys),
    );
    await acquireRunLocks(
      client,
      new Set(candidates.map((candidate) => candidate.adapterKey)),
    );
    const signature = candidateSignature(candidates, options);
    await registerRun(client, options, signature, candidates);
    const migrated = await migrateCompatibleItems(client, options, candidates);
    await reconcileReusableBcsArtifacts(client, options, candidates);
    await refreshRouteAggregates(client, options.jobId);
    const disposition = await readItemDisposition(
      client,
      options.jobId,
      options.maxAttempts,
    );
    const pending = candidates.filter(
      (candidate) =>
        !disposition.completed.has(candidate.parcelHash) &&
        !disposition.cooling.has(candidate.parcelHash),
    );
    const selectedPending =
      options.maxItems === null ? pending : pending.slice(0, options.maxItems);
    await mkdir(path.resolve(options.workDirectory), {
      recursive: true,
      mode: 0o700,
    });

    let processed = 0;
    let terminal = disposition.completed.size;
    let failed = 0;
    await processByRouteWithConcurrency(
      selectedPending,
      options.concurrency,
      supportedPermitConcurrencyKey,
      async (candidate) => {
        const attempt = await readAttemptCount(
          client,
          options.jobId,
          candidate.parcelHash,
        );
        try {
          const outcome = await probeAndLoadCandidate(
            candidate,
            options,
            browserPool,
          );
          if (controlConnectionFailed) {
            throw new Error("Permit control connection failed");
          }
          const finalStatus =
            outcome.status === "failed" && attempt + 1 >= options.maxAttempts
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
            finalStatus === "failed"
              ? failureCooldownTimestamp(attempt + 1)
              : null,
          );
          processed += 1;
          if (TERMINAL_STATUSES.has(finalStatus)) terminal += 1;
          if (finalStatus === "failed" || finalStatus === "failed_exhausted") {
            failed += 1;
          }
        } catch {
          if (controlConnectionFailed) {
            throw new Error("Permit control connection failed");
          }
          const finalStatus =
            attempt + 1 >= options.maxAttempts ? "failed_exhausted" : "failed";
          await checkpointItem(
            client,
            options.jobId,
            candidate,
            finalStatus,
            0,
            attempt + 1,
            "source_or_load_error",
            finalStatus === "failed"
              ? failureCooldownTimestamp(attempt + 1)
              : null,
          );
          processed += 1;
          failed += 1;
          if (finalStatus === "failed_exhausted") terminal += 1;
        }
      },
    );
    await refreshRouteAggregates(client, options.jobId);
    const aggregate = await readRunAggregate(client, options.jobId);
    const browserMetrics = browserPool?.snapshot() ?? {
      launches: 0,
      reuses: 0,
      invalidations: 0,
    };
    await finalizeRoutePhases(client, options.jobId);
    await client.query(
      `UPDATE ${CONTROL_SCHEMA}.broward_supported_permit_runs
       SET phase = $2,
           terminal_count = $3,
           record_count = $4,
           failure_count = $5,
           next_attempt_at = $6,
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
        aggregate.nextAttemptAt,
      ],
    );
    return {
      candidateCount: candidates.length,
      processed,
      terminal: aggregate.terminalCount,
      failed: aggregate.failureCount,
      cooling: aggregate.coolingCount,
      migrated,
      browserLaunches: browserMetrics.launches,
      browserReuses: browserMetrics.reuses,
      browserInvalidations: browserMetrics.invalidations,
    };
  } finally {
    if (browserPool !== null) await browserPool.close();
    await client.end();
  }
}

/**
 * Read completed appraisal properties and deterministically interleave routes.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @param {number | null} limit - Optional candidate limit.
 * @param {ReadonlySet<string>} jurisdictionKeys - Explicit registry allowlist.
 * @returns {Promise<SupportedPermitCandidate[]>} Supported-route candidates.
 */
async function readCandidates(client, limit, jurisdictionKeys) {
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
      !jurisdictionKeys.has(resolution.jurisdiction.key) ||
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
    group.sort((left, right) =>
      left.parcelHash.localeCompare(right.parcelHash),
    );
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
 * @param {WarmCitizenserveBrowserPool | null} browserPool
 *   Optional measured-safe warm browser owner.
 * @returns {Promise<ProbeOutcome>} Source and load outcome.
 */
async function probeAndLoadCandidate(candidate, options, browserPool) {
  const itemDirectory = path.join(
    path.resolve(options.workDirectory),
    candidate.jurisdictionKey,
    candidate.parcelHash,
  );
  await mkdir(itemDirectory, { recursive: true, mode: 0o700 });
  if (candidate.adapterKey === BROWARD_BCS_ADAPTER_KEY) {
    return probeBcs(candidate, itemDirectory, options);
  }
  if (candidate.adapterKey === BROWARD_ACCELA_ADAPTER_KEY) {
    return probeAccela(candidate, itemDirectory, options);
  }
  if (
    candidate.adapterKey === BROWARD_TYLER_CIVIC_ACCESS_ADAPTER_KEY ||
    candidate.adapterKey === BROWARD_CITIZENSERVE_ADAPTER_KEY
  ) {
    return probeMunicipal(candidate, itemDirectory, options, browserPool);
  }
  return {
    status: "failed_exhausted",
    recordCount: 0,
    errorClass: "unsupported_adapter",
  };
}

/**
 * Reconcile completed BCS child artifacts that were durably written before a
 * prior parent attempt failed to checkpoint or load them.
 *
 * Only failed item rows are considered. Successful terminal source outcomes
 * are never reopened, and an exhausted row is changed only when its existing
 * private artifact proves the exact folio and record count without another
 * source request.
 *
 * @param {import("pg").Client} client - Verified locked control client.
 * @param {SupportedPermitOptions} options - Stable run and artifact identity.
 * @param {readonly SupportedPermitCandidate[]} candidates - Signed seed rows.
 * @returns {Promise<number>} Count of saved BCS outcomes reconciled.
 */
async function reconcileReusableBcsArtifacts(client, options, candidates) {
  const candidatesByHash = new Map(
    candidates
      .filter((candidate) => candidate.adapterKey === BROWARD_BCS_ADAPTER_KEY)
      .map((candidate) => [candidate.parcelHash, candidate]),
  );
  if (candidatesByHash.size === 0) return 0;
  const result = await client.query(
    `SELECT parcel_hash,attempt_count
     FROM ${CONTROL_SCHEMA}.broward_supported_permit_items
     WHERE job_id=$1
       AND adapter_key=$2
       AND status IN ('failed','failed_exhausted')
     ORDER BY parcel_hash`,
    [options.jobId, BROWARD_BCS_ADAPTER_KEY],
  );
  let recovered = 0;
  for (const row of result.rows) {
    const candidate =
      typeof row.parcel_hash === "string"
        ? candidatesByHash.get(row.parcel_hash)
        : undefined;
    const attemptCount = Number(row.attempt_count);
    if (
      candidate === undefined ||
      !Number.isInteger(attemptCount) ||
      attemptCount < 1
    ) {
      throw new Error("Existing BCS checkpoint item is invalid");
    }
    const outcome = await readReusableBcsOutcome(candidate, options);
    if (outcome === null) continue;
    await checkpointItem(
      client,
      options.jobId,
      candidate,
      outcome.status,
      outcome.recordCount,
      attemptCount,
      null,
      null,
    );
    recovered += 1;
  }
  return recovered;
}

/**
 * Validate and load one already completed private BCS child artifact.
 *
 * Artifact absence or an incompatible summary returns `null` so a nonterminal
 * property can use the normal bounded source probe. A loader failure propagates
 * because repeating the source query cannot repair a database failure.
 *
 * @param {SupportedPermitCandidate} candidate - Exact signed BCS property.
 * @param {SupportedPermitOptions} options - Stable artifact and scope options.
 * @returns {Promise<ProbeOutcome | null>} Reusable outcome or no valid artifact.
 */
async function readReusableBcsOutcome(candidate, options) {
  const itemDirectory = path.join(
    path.resolve(options.workDirectory),
    candidate.jurisdictionKey,
    candidate.parcelHash,
  );
  const recordsPath = path.join(itemDirectory, "records.private.jsonl");
  const summaryPath = path.join(itemDirectory, "summary.private.json");
  /** @type {number} */
  let recordCount;
  try {
    const summary = await readJson(summaryPath);
    recordCount = readBcsSummaryRecordCount(
      summary,
      candidate.folio,
      options.scope === "roofing",
    );
  } catch {
    return null;
  }
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
 * Run one bounded BCS lookup and load complete normalized records.
 *
 * @param {SupportedPermitCandidate} candidate - Routed BCS property.
 * @param {string} itemDirectory - Private hash-keyed working directory.
 * @param {SupportedPermitOptions} options - Parent scope and run bounds.
 * @returns {Promise<ProbeOutcome>} Explicit BCS outcome.
 */
async function probeBcs(candidate, itemDirectory, options) {
  const recordsPath = path.join(itemDirectory, "records.private.jsonl");
  const summaryPath = path.join(itemDirectory, "summary.private.json");
  const reusable = await readReusableBcsOutcome(candidate, options);
  if (reusable !== null) return reusable;
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
    ...roofScopeArgs(options),
  ]);
  if (command.exitCode !== 0) {
    return {
      status: "failed",
      recordCount: 0,
      errorClass: command.timedOut ? "probe_timeout" : "bcs_probe_failed",
    };
  }
  const summary = await readJson(summaryPath);
  const recordCount = readBcsSummaryRecordCount(
    summary,
    candidate.folio,
    options.scope === "roofing",
  );
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
 * @param {SupportedPermitOptions} options - Parent scope and run bounds.
 * @returns {Promise<ProbeOutcome>} Explicit Accela outcome.
 */
async function probeAccela(candidate, itemDirectory, options) {
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
    ...roofScopeArgs(options),
  ]);
  if (command.exitCode !== 0) {
    return {
      status:
        command.timedOut || command.stderrBytes === 0 ? "failed" : "truncated",
      recordCount: 0,
      errorClass: command.timedOut ? "probe_timeout" : "accela_bounded_failure",
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
 * @param {SupportedPermitOptions} options - Parent scope and run bounds.
 * @param {WarmCitizenserveBrowserPool | null} browserPool
 *   Optional pool used only by the measured-safe route allowlist.
 * @returns {Promise<ProbeOutcome>} Explicit municipal outcome.
 */
async function probeMunicipal(candidate, itemDirectory, options, browserPool) {
  const recordsPath = path.join(itemDirectory, "records.private.jsonl");
  const summaryPath = path.join(itemDirectory, "summary.json");
  const warmBrowserKey =
    candidate.adapterKey === BROWARD_CITIZENSERVE_ADAPTER_KEY
      ? WARM_CITIZENSERVE_BROWSER_KEYS.get(candidate.jurisdictionKey)
      : undefined;
  /** @type {Readonly<Record<string, unknown>>} */
  let summary;
  if (
    browserPool !== null &&
    options.browserReuse === "enabled" &&
    warmBrowserKey !== undefined
  ) {
    /** @type {Parameters<typeof runMunicipalPermitProbe>[0]} */
    const probeOptions = {
      jurisdictionKey: registryKeyToMunicipalKey(candidate.jurisdictionKey),
      query: { kind: "folio", value: candidate.folio },
      outputDirectory: itemDirectory,
      maxPages: 3,
      maxDetails: 10,
      searchDelayMs: 1_500,
      detailDelayMs: 500,
      roofOnly: options.scope === "roofing",
    };
    try {
      summary = await browserPool.run(warmBrowserKey, (browser) =>
        runMunicipalPermitProbe(probeOptions, {
          citizenserveBrowser: browser,
        }),
      );
    } catch {
      return {
        status: "failed",
        recordCount: 0,
        errorClass: "municipal_probe_failed",
      };
    }
  } else {
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
      ...roofScopeArgs(options),
    ]);
    if (command.exitCode !== 0) {
      return {
        status: "failed",
        recordCount: 0,
        errorClass: command.timedOut
          ? "probe_timeout"
          : "municipal_probe_failed",
      };
    }
    summary = await readJson(summaryPath);
  }
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
 * @param {number} [timeoutMs=DEFAULT_PROBE_TIMEOUT_MS] - Hard process deadline.
 * @returns {Promise<CommandResult>} Aggregate-safe process result.
 */
export function runNode(args, timeoutMs = DEFAULT_PROBE_TIMEOUT_MS) {
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1) {
    throw new Error("Probe timeout must be a positive integer");
  }
  return new Promise((resolvePromise) => {
    const child = spawn(process.execPath, [...args], {
      cwd: process.cwd(),
      detached: process.platform !== "win32",
      stdio: ["ignore", "ignore", "pipe"],
    });
    let stderrBytes = 0;
    let timedOut = false;
    let settled = false;
    /**
     * @param {CommandResult} result - Final aggregate-safe process outcome.
     * @returns {void}
     */
    const finish = (result) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      resolvePromise(result);
    };
    const timeout = setTimeout(() => {
      timedOut = true;
      if (process.platform !== "win32" && typeof child.pid === "number") {
        try {
          process.kill(-child.pid, "SIGKILL");
        } catch {
          child.kill("SIGKILL");
        }
      } else {
        child.kill("SIGKILL");
      }
    }, timeoutMs);
    child.stderr.on("data", (chunk) => {
      stderrBytes += Buffer.byteLength(chunk);
    });
    child.once("error", () => {
      finish({ exitCode: -1, stderrBytes, timedOut });
    });
    child.once("exit", (code) => {
      finish({ exitCode: code ?? -1, stderrBytes, timedOut });
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
 * @param {string | null} nextAttemptAt - Earliest retry after a failure.
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
  nextAttemptAt,
) {
  await client.query(
    `INSERT INTO ${CONTROL_SCHEMA}.broward_supported_permit_items (
       job_id, parcel_hash, jurisdiction_key, adapter_key, status,
       record_count, attempt_count, error_class, next_attempt_at
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
     ON CONFLICT (job_id, parcel_hash) DO UPDATE SET
       status=EXCLUDED.status, record_count=EXCLUDED.record_count,
       attempt_count=EXCLUDED.attempt_count, error_class=EXCLUDED.error_class,
       next_attempt_at=EXCLUDED.next_attempt_at, updated_at=now()`,
    [
      jobId,
      candidate.parcelHash,
      candidate.jurisdictionKey,
      candidate.adapterKey,
      status,
      recordCount,
      attemptCount,
      errorClass,
      nextAttemptAt,
    ],
  );
  await client.query(
    `UPDATE ${CONTROL_SCHEMA}.broward_supported_permit_runs
     SET heartbeat_at=now() WHERE job_id=$1`,
    [jobId],
  );
  await refreshRouteAggregates(client, jobId, candidate.jurisdictionKey);
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
       scope text NOT NULL DEFAULT 'all' CHECK (scope IN ('all','roofing')),
       candidate_count integer NOT NULL,
       concurrency integer NOT NULL,
       max_attempts integer NOT NULL,
       phase text NOT NULL CHECK (phase IN ('running','paused','source_exhausted','failed')),
       terminal_count integer NOT NULL DEFAULT 0,
       record_count integer NOT NULL DEFAULT 0,
       failure_count integer NOT NULL DEFAULT 0,
       next_attempt_at timestamptz,
       started_at timestamptz NOT NULL DEFAULT now(),
       heartbeat_at timestamptz NOT NULL DEFAULT now(),
       completed_at timestamptz
     )`,
  );
  await client.query(
    `ALTER TABLE ${CONTROL_SCHEMA}.broward_supported_permit_runs
     ADD COLUMN IF NOT EXISTS scope text NOT NULL DEFAULT 'all'
     CHECK (scope IN ('all','roofing'))`,
  );
  await client.query(
    `ALTER TABLE ${CONTROL_SCHEMA}.broward_supported_permit_runs
     ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz`,
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
       next_attempt_at timestamptz,
       updated_at timestamptz NOT NULL DEFAULT now(),
       PRIMARY KEY (job_id, parcel_hash)
     )`,
  );
  await client.query(
    `ALTER TABLE ${CONTROL_SCHEMA}.broward_supported_permit_items
     ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz`,
  );
  await client.query(
    `CREATE TABLE IF NOT EXISTS ${CONTROL_SCHEMA}.broward_supported_permit_routes (
       job_id text NOT NULL REFERENCES ${CONTROL_SCHEMA}.broward_supported_permit_runs(job_id),
       jurisdiction_key text NOT NULL,
       adapter_key text NOT NULL,
       candidate_count integer NOT NULL CHECK (candidate_count >= 0),
       phase text NOT NULL CHECK (phase IN ('running','paused','cooling','complete')),
       terminal_count integer NOT NULL DEFAULT 0 CHECK (terminal_count >= 0),
       record_count integer NOT NULL DEFAULT 0 CHECK (record_count >= 0),
       terminal_missing_count integer NOT NULL DEFAULT 0 CHECK (terminal_missing_count >= 0),
       next_attempt_at timestamptz,
       heartbeat_at timestamptz NOT NULL DEFAULT now(),
       PRIMARY KEY (job_id,jurisdiction_key)
     )`,
  );
}

/**
 * Register or verify one immutable supported-routes run contract.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @param {SupportedPermitOptions} options - Run configuration.
 * @param {string} signature - Candidate/config SHA-256.
 * @param {readonly SupportedPermitCandidate[]} candidates - Exact supported candidates.
 * @returns {Promise<void>} Resolves after registration and heartbeat.
 */
async function registerRun(client, options, signature, candidates) {
  const candidateCount = candidates.length;
  await client.query(
    `INSERT INTO ${CONTROL_SCHEMA}.broward_supported_permit_runs (
       job_id, config_signature, registry_version, candidate_count,
       concurrency, max_attempts, scope, phase
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,'running')
     ON CONFLICT (job_id) DO NOTHING`,
    [
      options.jobId,
      signature,
      BROWARD_PERMIT_REGISTRY_VERSION,
      candidateCount,
      options.concurrency,
      options.maxAttempts,
      options.scope,
    ],
  );
  const result = await client.query(
    `SELECT config_signature,candidate_count,concurrency,max_attempts,scope
     FROM ${CONTROL_SCHEMA}.broward_supported_permit_runs WHERE job_id=$1`,
    [options.jobId],
  );
  const row = result.rows[0];
  if (
    row?.config_signature !== signature ||
    Number(row.candidate_count) !== candidateCount ||
    Number(row.concurrency) !== options.concurrency ||
    Number(row.max_attempts) !== options.maxAttempts ||
    row.scope !== options.scope
  ) {
    throw new Error("Existing supported permit run config does not match");
  }
  await client.query(
    `UPDATE ${CONTROL_SCHEMA}.broward_supported_permit_runs
     SET phase='running',heartbeat_at=now() WHERE job_id=$1`,
    [options.jobId],
  );
  const routeCounts = countCandidateRoutes(candidates);
  for (const candidate of routeCounts) {
    await client.query(
      `INSERT INTO ${CONTROL_SCHEMA}.broward_supported_permit_routes (
         job_id,jurisdiction_key,adapter_key,candidate_count,phase
       ) VALUES ($1,$2,$3,$4,'running')
       ON CONFLICT (job_id,jurisdiction_key) DO NOTHING`,
      [
        options.jobId,
        candidate.jurisdictionKey,
        candidate.adapterKey,
        candidate.candidateCount,
      ],
    );
  }
  const routeRows = await client.query(
    `SELECT jurisdiction_key,adapter_key,candidate_count
     FROM ${CONTROL_SCHEMA}.broward_supported_permit_routes
     WHERE job_id=$1`,
    [options.jobId],
  );
  if (
    routeRows.rows.length !== routeCounts.length ||
    routeRows.rows.some((row) => {
      const expected = routeCounts.find(
        (candidate) => candidate.jurisdictionKey === row.jurisdiction_key,
      );
      return (
        expected === undefined ||
        expected.adapterKey !== row.adapter_key ||
        expected.candidateCount !== Number(row.candidate_count)
      );
    })
  ) {
    throw new Error("Existing supported permit route contract does not match");
  }
  await client.query(
    `UPDATE ${CONTROL_SCHEMA}.broward_supported_permit_routes
     SET phase='running',heartbeat_at=now()
     WHERE job_id=$1`,
    [options.jobId],
  );
}

/**
 * Count deterministic candidates without retaining private query values.
 *
 * @param {readonly SupportedPermitCandidate[]} candidates - Signed candidates.
 * @returns {{
 *   jurisdictionKey:string,
 *   adapterKey:string,
 *   candidateCount:number
 * }[]} Current deterministic route counts.
 */
function countCandidateRoutes(candidates) {
  const counts = new Map();
  for (const candidate of candidates) {
    const existing = counts.get(candidate.jurisdictionKey);
    if (
      existing !== undefined &&
      existing.adapterKey !== candidate.adapterKey
    ) {
      throw new Error("Supported permit route changed adapter within one run");
    }
    counts.set(candidate.jurisdictionKey, {
      jurisdictionKey: candidate.jurisdictionKey,
      adapterKey: candidate.adapterKey,
      candidateCount: (existing?.candidateCount ?? 0) + 1,
    });
  }
  return [...counts.values()].sort((left, right) =>
    left.jurisdictionKey.localeCompare(right.jurisdictionKey),
  );
}

/**
 * Validate and preserve a prior job item against the current signed seed.
 *
 * A previously exhausted failure becomes retryable only when the new job has a
 * strictly larger finite attempt budget. Successful, empty, and truncated
 * evidence retains its exact status and record count.
 *
 * @param {Record<string, unknown>} row - Prior aggregate checkpoint row.
 * @param {ReadonlyMap<string, SupportedPermitCandidate>} candidatesByHash
 *   Current signed property candidates keyed by compatible one-way identity.
 * @param {number} maxAttempts - New finite source attempt ceiling.
 * @returns {MigratedPermitItem} Validated compatible migration item.
 */
export function normalizeMigratedPermitItem(
  row,
  candidatesByHash,
  maxAttempts,
) {
  const parcelHash = row.parcel_hash;
  const jurisdictionKey = row.jurisdiction_key;
  const adapterKey = row.adapter_key;
  const status = row.status;
  const recordCount = Number(row.record_count);
  const attemptCount = Number(row.attempt_count);
  const errorClass =
    row.error_class === null || typeof row.error_class === "string"
      ? row.error_class
      : undefined;
  const candidate =
    typeof parcelHash === "string"
      ? candidatesByHash.get(parcelHash)
      : undefined;
  if (
    candidate === undefined ||
    typeof parcelHash !== "string" ||
    typeof jurisdictionKey !== "string" ||
    typeof adapterKey !== "string" ||
    candidate.jurisdictionKey !== jurisdictionKey ||
    candidate.adapterKey !== adapterKey ||
    typeof status !== "string" ||
    ![
      "records",
      "no_permits",
      "truncated",
      "failed",
      "failed_exhausted",
    ].includes(status) ||
    !Number.isSafeInteger(recordCount) ||
    recordCount < 0 ||
    !Number.isSafeInteger(attemptCount) ||
    attemptCount < 1 ||
    errorClass === undefined
  ) {
    throw new Error(
      "Prior supported permit item is not compatible with the current signed seed",
    );
  }
  const migratedStatus =
    status === "failed_exhausted" && attemptCount < maxAttempts
      ? "failed"
      : status;
  return {
    parcelHash,
    jurisdictionKey,
    adapterKey,
    status: /** @type {SupportedPermitItemStatus} */ (migratedStatus),
    recordCount,
    attemptCount,
    errorClass,
  };
}

/**
 * Copy only identity-compatible prior item evidence into a new immutable job.
 *
 * @param {import("pg").Client} client - Verified control client.
 * @param {SupportedPermitOptions} options - New source-scoped run contract.
 * @param {readonly SupportedPermitCandidate[]} candidates - Signed candidates.
 * @returns {Promise<number>} Newly inserted compatible item count.
 */
async function migrateCompatibleItems(client, options, candidates) {
  if (options.migrateFromJobId === null) return 0;
  const sourceRun = await client.query(
    `SELECT scope FROM ${CONTROL_SCHEMA}.broward_supported_permit_runs
     WHERE job_id=$1`,
    [options.migrateFromJobId],
  );
  if (
    sourceRun.rows.length !== 1 ||
    sourceRun.rows[0]?.scope !== options.scope
  ) {
    throw new Error(
      "Prior supported permit job is absent or scope-incompatible",
    );
  }
  const prior = await client.query(
    `SELECT parcel_hash,jurisdiction_key,adapter_key,status,record_count,
            attempt_count,error_class
     FROM ${CONTROL_SCHEMA}.broward_supported_permit_items
     WHERE job_id=$1 AND jurisdiction_key=ANY($2::text[])`,
    [options.migrateFromJobId, options.jurisdictionKeys],
  );
  const candidatesByHash = new Map(
    candidates.map((candidate) => [candidate.parcelHash, candidate]),
  );
  const items = prior.rows.map((row) =>
    normalizeMigratedPermitItem(
      /** @type {Record<string, unknown>} */ (row),
      candidatesByHash,
      options.maxAttempts,
    ),
  );
  if (items.length === 0) {
    throw new Error(
      "Prior supported permit job has no compatible selected items",
    );
  }
  const inserted = await client.query(
    `INSERT INTO ${CONTROL_SCHEMA}.broward_supported_permit_items (
       job_id,parcel_hash,jurisdiction_key,adapter_key,status,record_count,
       attempt_count,error_class,next_attempt_at
     )
     SELECT $1,item.parcel_hash,item.jurisdiction_key,item.adapter_key,
            item.status,item.record_count,item.attempt_count,item.error_class,NULL
     FROM jsonb_to_recordset($2::jsonb) AS item(
       parcel_hash text,jurisdiction_key text,adapter_key text,status text,
       record_count integer,attempt_count integer,error_class text
     )
     ON CONFLICT (job_id,parcel_hash) DO NOTHING
     RETURNING parcel_hash`,
    [
      options.jobId,
      JSON.stringify(
        items.map((item) => ({
          parcel_hash: item.parcelHash,
          jurisdiction_key: item.jurisdictionKey,
          adapter_key: item.adapterKey,
          status: item.status,
          record_count: item.recordCount,
          attempt_count: item.attemptCount,
          error_class: item.errorClass,
        })),
      ),
    ],
  );
  return inserted.rowCount ?? 0;
}

/**
 * Rebuild each route row from item truth after migration or source work.
 *
 * @param {import("pg").Client} client - Verified control client.
 * @param {string} jobId - Stable source-scoped run identity.
 * @param {string | null} [jurisdictionKey=null] - Optional single-route bound.
 * @returns {Promise<void>} Resolves after aggregate route checkpointing.
 */
async function refreshRouteAggregates(client, jobId, jurisdictionKey = null) {
  await client.query(
    `UPDATE ${CONTROL_SCHEMA}.broward_supported_permit_routes AS route
     SET phase='running',
         terminal_count = (
           SELECT count(*)::integer
           FROM ${CONTROL_SCHEMA}.broward_supported_permit_items AS item
           WHERE item.job_id=route.job_id
             AND item.jurisdiction_key=route.jurisdiction_key
             AND item.status IN (
               'records','no_permits','truncated','failed_exhausted'
             )
         ),
         record_count = (
           SELECT coalesce(sum(item.record_count),0)::integer
           FROM ${CONTROL_SCHEMA}.broward_supported_permit_items AS item
           WHERE item.job_id=route.job_id
             AND item.jurisdiction_key=route.jurisdiction_key
         ),
         terminal_missing_count = (
           SELECT count(*)::integer
           FROM ${CONTROL_SCHEMA}.broward_supported_permit_items AS item
           WHERE item.job_id=route.job_id
             AND item.jurisdiction_key=route.jurisdiction_key
             AND item.status IN ('truncated','failed_exhausted')
         ),
         next_attempt_at = (
           SELECT min(item.next_attempt_at)
           FROM ${CONTROL_SCHEMA}.broward_supported_permit_items AS item
           WHERE item.job_id=route.job_id
             AND item.jurisdiction_key=route.jurisdiction_key
             AND item.status='failed'
             AND item.next_attempt_at > now()
         ),
         heartbeat_at=now()
     WHERE route.job_id=$1
       AND ($2::text IS NULL OR route.jurisdiction_key=$2)`,
    [jobId, jurisdictionKey],
  );
}

/**
 * Mark route aggregates complete, cooling, or durably paused after invocation.
 *
 * @param {import("pg").Client} client - Verified control client.
 * @param {string} jobId - Stable source-scoped run identity.
 * @returns {Promise<void>} Resolves after route phase finalization.
 */
async function finalizeRoutePhases(client, jobId) {
  await client.query(
    `UPDATE ${CONTROL_SCHEMA}.broward_supported_permit_routes AS route
     SET phase = CASE
       WHEN route.terminal_count >= route.candidate_count THEN 'complete'
       WHEN route.next_attempt_at IS NOT NULL
         AND (
           SELECT count(*)::integer
           FROM ${CONTROL_SCHEMA}.broward_supported_permit_items AS item
           WHERE item.job_id=route.job_id
             AND item.jurisdiction_key=route.jurisdiction_key
         ) >= route.candidate_count
         THEN 'cooling'
       ELSE 'paused'
     END,
     heartbeat_at=now()
     WHERE route.job_id=$1`,
    [jobId],
  );
}

/**
 * Read terminal and currently cooling one-way parcel hashes.
 *
 * @param {import("pg").Client} client - Verified control client.
 * @param {string} jobId - Stable run identifier.
 * @param {number} maxAttempts - Failure exhaustion threshold.
 * @returns {Promise<{completed:Set<string>,cooling:Set<string>}>}
 *   Durable item disposition without exposing source identities.
 */
async function readItemDisposition(client, jobId, maxAttempts) {
  const result = await client.query(
    `SELECT parcel_hash,status,attempt_count,next_attempt_at
     FROM ${CONTROL_SCHEMA}.broward_supported_permit_items WHERE job_id=$1`,
    [jobId],
  );
  const completed = new Set();
  const cooling = new Set();
  const now = Date.now();
  for (const row of result.rows) {
    if (typeof row.parcel_hash !== "string") continue;
    if (
      TERMINAL_STATUSES.has(row.status) ||
      Number(row.attempt_count) >= maxAttempts
    ) {
      completed.add(row.parcel_hash);
    } else if (
      row.status === "failed" &&
      timestampMillis(row.next_attempt_at) > now
    ) {
      cooling.add(row.parcel_hash);
    }
  }
  return { completed, cooling };
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
 * @returns {Promise<{
 *   terminalCount:number,
 *   recordCount:number,
 *   failureCount:number,
 *   coolingCount:number,
 *   nextAttemptAt:string|null
 * }>}
 *   Aggregate durable counters.
 */
async function readRunAggregate(client, jobId) {
  const result = await client.query(
    `SELECT
       count(*) FILTER (WHERE status IN ('records','no_permits','truncated','failed_exhausted'))::integer AS terminal_count,
       coalesce(sum(record_count),0)::integer AS record_count,
       count(*) FILTER (WHERE status IN ('failed','failed_exhausted'))::integer AS failure_count
       ,count(*) FILTER (
         WHERE status='failed' AND next_attempt_at > now()
       )::integer AS cooling_count
       ,min(next_attempt_at) FILTER (
         WHERE status='failed' AND next_attempt_at > now()
       )::text AS next_attempt_at
     FROM ${CONTROL_SCHEMA}.broward_supported_permit_items WHERE job_id=$1`,
    [jobId],
  );
  return {
    terminalCount: Number(result.rows[0]?.terminal_count ?? 0),
    recordCount: Number(result.rows[0]?.record_count ?? 0),
    failureCount: Number(result.rows[0]?.failure_count ?? 0),
    coolingCount: Number(result.rows[0]?.cooling_count ?? 0),
    nextAttemptAt:
      typeof result.rows[0]?.next_attempt_at === "string"
        ? result.rows[0].next_attempt_at
        : null,
  };
}

/**
 * Acquire one session-scoped source lock per selected adapter family.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @param {ReadonlySet<string>} adapterKeys - Selected current adapter families.
 * @returns {Promise<void>} Resolves only for the sole runner.
 */
async function acquireRunLocks(client, adapterKeys) {
  const lockKeys = [...adapterKeys]
    .map((adapterKey) => ADAPTER_LOCK_KEYS.get(adapterKey))
    .sort((left, right) => Number(left) - Number(right));
  if (
    lockKeys.length === 0 ||
    lockKeys.some((lockKey) => !Number.isInteger(lockKey))
  ) {
    throw new Error("Supported permit adapter lock configuration is invalid");
  }
  for (const lockKey of lockKeys) {
    const result = await client.query(
      "SELECT pg_try_advisory_lock($1,$2) AS acquired",
      [LOCK_NAMESPACE, lockKey],
    );
    if (result.rows[0]?.acquired !== true) {
      throw new Error("Another supported permit runner owns this source lock");
    }
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
 * Validate a completed one-property BCS child summary against the exact source
 * query before its private JSONL can be reused or loaded.
 *
 * @param {Record<string, unknown>} summary - Parsed private BCS summary.
 * @param {string} expectedParcelIdentifier - Exact signed property folio.
 * @param {boolean} expectedRoofOnly - Active run scope.
 * @returns {number} Reconciled normalized record count.
 */
export function readBcsSummaryRecordCount(
  summary,
  expectedParcelIdentifier,
  expectedRoofOnly,
) {
  const observations = summary.observations;
  if (
    summary.event !== "broward_bcs_permit_probe_completed" ||
    summary.sourceSystem !== BROWARD_BCS_SOURCE_SYSTEM ||
    summary.parcelCount !== 1 ||
    summary.roofOnly !== expectedRoofOnly ||
    !Array.isArray(observations) ||
    observations.length !== 1
  ) {
    throw new Error("BCS permit summary does not match the active probe");
  }
  const observation = observations[0];
  if (
    typeof observation !== "object" ||
    observation === null ||
    Array.isArray(observation)
  ) {
    throw new Error("BCS permit summary observation is invalid");
  }
  const observationRecord = /** @type {Record<string, unknown>} */ (
    observation
  );
  if (observationRecord.parcelIdentifier !== expectedParcelIdentifier) {
    throw new Error("BCS permit summary property identity changed");
  }
  const recordCount = numberField(summary, "normalizedRecordCount");
  if (
    !Number.isInteger(observationRecord.normalizedRecordCount) ||
    Number(observationRecord.normalizedRecordCount) !== recordCount
  ) {
    throw new Error("BCS permit summary record counts do not reconcile");
  }
  return recordCount;
}

/**
 * Read a non-negative integer source summary field.
 *
 * @param {Readonly<Record<string, unknown>>} record - Source summary.
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
 * Return the finite attempt-based source cooldown.
 *
 * @param {number} attemptCount - One-based failed attempt count.
 * @returns {number} Cooldown duration in milliseconds.
 */
export function failureCooldownDelayMs(attemptCount) {
  if (!Number.isInteger(attemptCount) || attemptCount < 1) {
    throw new Error("Failure attempt count must be a positive integer");
  }
  const delays = [5 * 60_000, 15 * 60_000, 60 * 60_000, 4 * 60 * 60_000];
  return (
    delays[Math.min(attemptCount - 1, delays.length - 1)] ?? 4 * 60 * 60_000
  );
}

/**
 * Produce an ISO retry timestamp without retaining raw source errors.
 *
 * @param {number} attemptCount - One-based failed attempt count.
 * @returns {string} Earliest safe retry time.
 */
function failureCooldownTimestamp(attemptCount) {
  return new Date(
    Date.now() + failureCooldownDelayMs(attemptCount),
  ).toISOString();
}

/**
 * Parse a PostgreSQL timestamp value for private eligibility decisions.
 *
 * @param {unknown} value - Date object, ISO string, or absent timestamp.
 * @returns {number} Epoch milliseconds, or negative infinity when absent.
 */
function timestampMillis(value) {
  if (value instanceof Date) return value.getTime();
  if (typeof value === "string") {
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : Number.NEGATIVE_INFINITY;
  }
  return Number.NEGATIVE_INFINITY;
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
  digest.update(`jurisdictions:${options.jurisdictionKeys.join(",")}\0`);
  digest.update(`migrate:${options.migrateFromJobId ?? "none"}\0`);
  if (options.scope !== "all") {
    digest.update(`scope:${options.scope}\0`);
  }
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
  return createHash("sha256").update(`broward-permit:${folio}`).digest("hex");
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
 * Build the child CLI scope flag without changing all-permit command identity.
 *
 * @param {SupportedPermitOptions} options - Parent run configuration.
 * @returns {readonly string[]} Roofing-only flag or an empty all-permit list.
 */
function roofScopeArgs(options) {
  return options.scope === "roofing" ? ["--roof-only"] : [];
}

/**
 * Return the public source-serialization key for one supported property.
 *
 * Every Citizenserve installation currently shares one public host, so all
 * four routes remain serial even when the parent permits inter-host
 * concurrency. Other adapters retain one-at-a-time jurisdiction ordering.
 * The warm-browser allowlist is narrower than this scheduling boundary.
 *
 * @param {SupportedPermitCandidate} candidate - Routed property.
 * @returns {string} Public host or jurisdiction serialization key.
 */
export function supportedPermitConcurrencyKey(candidate) {
  return candidate.adapterKey === BROWARD_CITIZENSERVE_ADAPTER_KEY
    ? CITIZENSERVE_HOST_CONCURRENCY_KEY
    : `jurisdiction:${candidate.jurisdictionKey}`;
}

/**
 * Process items with bounded total concurrency.
 *
 * @template Item
 * @param {readonly Item[]} items - Ordered pending items.
 * @param {number} concurrency - Maximum simultaneous handlers.
 * @param {(item:Item)=>string} routeKey - Stable per-source serialization key.
 * @param {(item:Item)=>Promise<void>} handler - Per-item operation.
 * @returns {Promise<void>} Resolves after all items settle successfully.
 */
export async function processByRouteWithConcurrency(
  items,
  concurrency,
  routeKey,
  handler,
) {
  /** @type {Map<string, Item[]>} */
  const routes = new Map();
  for (const item of items) {
    const key = routeKey(item);
    const queue = routes.get(key) ?? [];
    queue.push(item);
    routes.set(key, queue);
  }

  let available = concurrency;
  /** @type {((release:()=>void)=>void)[]} */
  const waiters = [];
  const release = () => {
    const waiter = waiters.shift();
    if (waiter !== undefined) {
      waiter(release);
      return;
    }
    available += 1;
  };
  const acquire = () => {
    if (available > 0) {
      available -= 1;
      return Promise.resolve(release);
    }
    return new Promise((resolvePromise) => {
      waiters.push(resolvePromise);
    });
  };

  await Promise.all(
    [...routes.values()].map(async (queue) => {
      for (const item of queue) {
        const releaseSlot = await acquire();
        try {
          await handler(item);
        } finally {
          releaseSlot();
        }
      }
    }),
  );
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
  runSupportedPermitIngest(parseSupportedPermitOptions(process.argv.slice(2)))
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
