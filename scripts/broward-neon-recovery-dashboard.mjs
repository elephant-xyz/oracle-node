#!/usr/bin/env node

/**
 * Aggregate-only dashboard for durable Broward appraisal recovery.
 *
 * Every metric comes from Neon source keys or aggregate `ingest_control`
 * checkpoints. No parcel identifiers, addresses, owner/contact values, source
 * payloads, raw errors, or connection strings are returned or logged.
 */

import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import pg from "pg";

import { BROWARD_ROW_DENOMINATOR } from "./broward-ingestion-dashboard.mjs";

const { Pool } = pg;
const EXPECTED_PROJECT_ID = "raspy-frost-51580436";
const PRODUCTION_ENDPOINT_PREFIX = "ep-mute-leaf";
const CONTROL_SCHEMA = "ingest_control";
const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_PORT = 47_832;

/**
 * @typedef {object} DashboardOptions
 * @property {string} host - HTTP listen interface.
 * @property {number} port - HTTP listen port.
 * @property {string} expectedBranchId - Exact isolated Neon branch ID.
 * @property {string} expectedEndpointId - Exact isolated Neon endpoint ID.
 *
 * @typedef {object} RecoveryAggregateRow
 * @property {string | number} property_count - Loaded Broward properties.
 * @property {string | number} distinct_folios - Loaded distinct folios.
 * @property {string | number} verified_properties
 *   Seed keys checkpointed after every expected logical row was verified.
 * @property {string | number} terminal_source_misses - Durable source misses.
 * @property {string | number} committed_chunks - Verified chunk commits.
 * @property {string | number} prepared_rows - Prepared rows across commits.
 * @property {string | number} committed_rows - Verified logical rows across commits.
 * @property {string | number} source_miss_attempts - Source-miss attempts.
 * @property {string | number} source_error_attempts - Retryable source-error attempts.
 * @property {string | number} transform_error_attempts - Transform-error attempts.
 * @property {string | number} load_error_attempts - Load-error attempts.
 * @property {string | number} recent_properties - Properties committed in the recent window.
 * @property {string | null} last_commit_at - Latest verified chunk timestamp.
 * @property {boolean} recovery_lock_held - Whether the recovery advisory lock exists.
 * @property {string | Date | null} permit_recorded_at - Durable pilot projection time.
 * @property {string | number | null} permit_sample_parcels - Bounded pilot sample count.
 * @property {string | number | null} permit_source_attempts - Bounded source requests.
 * @property {string | number | null} permit_source_unavailable - Explicit unavailable outcomes.
 * @property {string | number | null} permit_source_failures - Attempted source failures.
 * @property {string | number | null} permit_unique_records - Reconciled unique records.
 * @property {string | number | null} permit_query_rows - Queryable permit rows.
 * @property {boolean | null} permit_all_input_terminal - Terminal-input reconciliation.
 * @property {boolean | null} permit_all_records_accounted - Record reconciliation.
 * @property {boolean | null} permit_query_rows_match - Query-row reconciliation.
 * @property {boolean | null} permit_pilot_passed - Bounded pilot acceptance.
 * @property {boolean | null} permit_county_complete - Countywide completeness.
 * @property {string | number} permit_registry_jurisdictions - Current registry size.
 * @property {string | number} permit_sources_implemented - Implemented current routes.
 * @property {string | number} permit_sources_blocked - Blocked current routes.
 * @property {string | number} permit_inventory_records - Current logical permit rows.
 * @property {string | number} permit_inventory_matched - Permit rows linked to properties.
 * @property {string | number} permit_inventory_unmatched - Valid unlinked permit rows.
 * @property {string | number} permit_inventory_roofing - Explicit roofing rows.
 * @property {string | number} permit_inventory_parcels - Distinct source parcel IDs.
 * @property {string | number} permit_inventory_sources - Distinct source systems.
 * @property {string | null} permit_inventory_loaded_at - Latest permit load time.
 * @property {string | number} permit_bulk_source_rows - Largest bulk source snapshot.
 * @property {string | number} permit_bulk_committed_rows - Durable bulk source rows.
 * @property {string | number} permit_bulk_chunks - Durable bulk chunks.
 * @property {string | number} permit_list_loaded_rows - Completed list-load rows.
 * @property {string | number} permit_list_chunks - Completed list-load chunks.
 * @property {string | number} sunbiz_match_roles - Exact matched address roles.
 * @property {string | number} sunbiz_match_registrations - Distinct linked registrations.
 * @property {string | number} sunbiz_match_properties - Distinct linked properties.
 * @property {string | number} sunbiz_match_chunks - Durable full-run match chunks.
 *
 * @typedef {object} PermitEnumerationWorkerStatus
 * @property {string} source - Public jurisdiction label.
 * @property {"accela_csv" | "tyler_api"} family - Source mechanism.
 * @property {"not_started" | "running" | "paused" | "complete"} status
 *   Aggregate checkpoint activity state.
 * @property {number} completedWindows - Durable completed windows.
 * @property {number} pendingWindows - Remaining windows.
 * @property {number} totalWindows - Initial source windows.
 * @property {number} completionPercent - Window completion percentage.
 * @property {number} accessibleRecords - Source records retained for loading.
 * @property {number} excludedRecords - Explicit non-permit rows.
 * @property {number} invalidRecords - Malformed source rows.
 * @property {number} sourceMissingRecords - Reported but inaccessible rows.
 * @property {string | null} updatedAt - Last durable checkpoint time.
 *
 * @typedef {object} PermitEnumerationStatus
 * @property {PermitEnumerationWorkerStatus[]} workers - Fixed aggregate source list.
 * @property {number} activeWorkers - Recently advancing workers.
 * @property {number} completedWorkers - Fully exhausted workers.
 * @property {number} completedWindows - Durable windows across workers.
 * @property {number} totalWindows - Initial windows across workers.
 * @property {number} accessibleRecords - Inventory rows captured locally.
 * @property {number} excludedRecords - Explicit non-permit source rows.
 * @property {number} invalidRecords - Malformed source rows.
 * @property {number} sourceMissingRecords - Reported but inaccessible rows.
 *
 * @typedef {object} RecoveryDashboardStatus
 * @property {1} schemaVersion - Response schema version.
 * @property {string} generatedAt - Snapshot timestamp.
 * @property {"Broward"} county - Fixed county label.
 * @property {"broward-ingest"} branch - Fixed verified branch label.
 * @property {number} denominator - Official distinct seed-folio count.
 * @property {{ running: boolean, lastCommitAt: string | null }} process
 *   Durable process-lock and activity summary.
 * @property {{
 *   properties: number,
 *   distinctFolios: number,
 *   verifiedProperties: number,
 *   terminalSourceMisses: number,
 *   durableCompleted: number,
 *   remaining: number,
 *   completionPercent: number,
 *   committedChunks: number,
 *   preparedRows: number,
 *   committedRows: number
 * }} progress - Durable Neon-backed progress.
 * @property {{
 *   sourceMissAttempts: number,
 *   sourceErrorAttempts: number,
 *   transformErrorAttempts: number,
 *   loadErrorAttempts: number
 * }} failures - Aggregate attempt failures.
 * @property {{ windowMinutes: 15, propertiesPerMinute: number }} throughput
 *   Recent verified load throughput.
 * @property {{
 *   pilotState: "not_recorded" | "passed" | "failed",
 *   countyCompleteness: "not_established" | "not_complete" | "complete",
 *   recordedAt: string | null,
 *   sampleParcels: number | null,
 *   sourceAttempts: number | null,
 *   sourceUnavailable: number | null,
 *   sourceFailures: number | null,
 *   uniqueRecords: number | null,
 *   queryRows: number | null,
 *   allInputTerminal: boolean | null,
 *   allRecordsAccounted: boolean | null,
 *   queryRowsMatch: boolean | null,
 *   registryJurisdictions: number,
 *   currentSourcesImplemented: number,
 *   currentSourcesBlocked: number
 * }} permit - Durable bounded-pilot evidence and honest completeness state.
 * @property {{
 *   records:number,
 *   matched:number,
 *   unmatched:number,
 *   roofing:number,
 *   distinctParcels:number,
 *   sourceSystems:number,
 *   lastLoadedAt:string|null,
 *   bulkSourceRows:number,
 *   bulkCommittedRows:number,
 *   bulkChunks:number,
 *   listLoadedRows:number,
 *   listChunks:number
 * }} permitInventory - Current Neon permit inventory and durable load receipts.
 * @property {PermitEnumerationStatus} permitEnumeration - Local tenant workers.
 * @property {{
 *   matchedAddressRoles:number,
 *   registrations:number,
 *   properties:number,
 *   chunks:number
 * }} sunbizMatch - Verified exact Broward Sunbiz property links.
 * @property {{
 *   stale:boolean,
 *   lastSuccessfulAt:string,
 *   snapshotAgeSeconds:number
 * } | undefined} [dashboardHealth] - Resilient-reader snapshot state.
 */

/**
 * Parse dashboard network and branch-safety options.
 *
 * @param {readonly string[]} argv - CLI arguments after the script path.
 * @param {NodeJS.ProcessEnv} [environment=process.env] - Trusted runtime
 *   environment containing independently verified Neon IDs.
 * @returns {DashboardOptions} Validated fixed-purpose configuration.
 */
export function parseDashboardOptions(argv, environment = process.env) {
  /** @type {Partial<DashboardOptions>} */
  const options = {
    host: DEFAULT_HOST,
    port: DEFAULT_PORT,
    expectedBranchId: environment.BROWARD_INGEST_NEON_BRANCH_ID,
    expectedEndpointId: environment.BROWARD_INGEST_NEON_ENDPOINT_ID,
  };
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (typeof value !== "string" || value.startsWith("--")) {
      throw new Error(`Missing value for ${String(flag)}`);
    }
    if (flag === "--host") options.host = value;
    else if (flag === "--port") {
      const port = Number(value);
      if (!Number.isInteger(port) || port < 1_024 || port > 65_535) {
        throw new Error("--port must be an integer from 1024 through 65535");
      }
      options.port = port;
    } else if (flag === "--expected-branch-id") {
      options.expectedBranchId = value;
    } else if (flag === "--expected-endpoint-id") {
      options.expectedEndpointId = value;
    } else {
      throw new Error(`Unknown option: ${String(flag)}`);
    }
  }
  if (
    typeof options.host !== "string" ||
    options.host.trim() === "" ||
    /[\s/]/u.test(options.host)
  ) {
    throw new Error("--host must be a hostname or IP address");
  }
  if (
    typeof options.expectedBranchId !== "string" ||
    !/^br-[a-z0-9-]+$/u.test(options.expectedBranchId)
  ) {
    throw new Error(
      "BROWARD_INGEST_NEON_BRANCH_ID or --expected-branch-id must be an explicit Neon br-* ID",
    );
  }
  if (
    typeof options.expectedEndpointId !== "string" ||
    !/^ep-[a-z0-9-]+$/u.test(options.expectedEndpointId)
  ) {
    throw new Error(
      "BROWARD_INGEST_NEON_ENDPOINT_ID or --expected-endpoint-id must be an explicit Neon ep-* ID",
    );
  }
  return /** @type {DashboardOptions} */ (options);
}

/**
 * Convert a PostgreSQL aggregate to a finite non-negative integer.
 *
 * @param {string | number} value - Driver-returned bigint or numeric value.
 * @returns {number} Validated aggregate.
 */
function count(value) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error("Neon returned an invalid recovery aggregate");
  }
  return parsed;
}

/**
 * Preserve an absent durable permit aggregate as null rather than zero.
 *
 * @param {string | number | null} value - Nullable PostgreSQL aggregate.
 * @returns {number | null} Parsed count or null.
 */
function nullableCount(value) {
  return value === null ? null : count(value);
}

/**
 * Build the aggregate-only permit status from control and optional pilot rows.
 *
 * @param {RecoveryAggregateRow} row - Combined recovery dashboard row.
 * @returns {RecoveryDashboardStatus["permit"]} Public permit status.
 */
function buildPermitStatus(row) {
  const registryJurisdictions = count(row.permit_registry_jurisdictions);
  const currentSourcesImplemented = count(row.permit_sources_implemented);
  const currentSourcesBlocked = count(row.permit_sources_blocked);
  if (
    currentSourcesImplemented + currentSourcesBlocked !==
    registryJurisdictions
  ) {
    throw new Error("Permit route aggregates do not reconcile");
  }
  const recordedAt =
    row.permit_recorded_at instanceof Date
      ? row.permit_recorded_at.toISOString()
      : typeof row.permit_recorded_at === "string"
        ? row.permit_recorded_at
        : null;
  const nullableValues = [
    row.permit_sample_parcels,
    row.permit_source_attempts,
    row.permit_source_unavailable,
    row.permit_source_failures,
    row.permit_unique_records,
    row.permit_query_rows,
    row.permit_all_input_terminal,
    row.permit_all_records_accounted,
    row.permit_query_rows_match,
    row.permit_pilot_passed,
    row.permit_county_complete,
  ];
  if (recordedAt === null) {
    if (nullableValues.some((value) => value !== null)) {
      throw new Error("Unrecorded permit status contains inferred aggregates");
    }
    return {
      pilotState: "not_recorded",
      countyCompleteness: "not_established",
      recordedAt: null,
      sampleParcels: null,
      sourceAttempts: null,
      sourceUnavailable: null,
      sourceFailures: null,
      uniqueRecords: null,
      queryRows: null,
      allInputTerminal: null,
      allRecordsAccounted: null,
      queryRowsMatch: null,
      registryJurisdictions,
      currentSourcesImplemented,
      currentSourcesBlocked,
    };
  }
  if (
    !Number.isFinite(Date.parse(recordedAt)) ||
    typeof row.permit_all_input_terminal !== "boolean" ||
    typeof row.permit_all_records_accounted !== "boolean" ||
    typeof row.permit_query_rows_match !== "boolean" ||
    typeof row.permit_pilot_passed !== "boolean" ||
    typeof row.permit_county_complete !== "boolean"
  ) {
    throw new Error("Recorded permit status is incomplete");
  }
  const sourceFailures = nullableCount(row.permit_source_failures);
  if (
    row.permit_pilot_passed &&
    (!row.permit_all_input_terminal ||
      !row.permit_all_records_accounted ||
      !row.permit_query_rows_match ||
      sourceFailures !== 0)
  ) {
    throw new Error("Permit pilot pass does not reconcile");
  }
  if (
    row.permit_county_complete &&
    (!row.permit_pilot_passed || currentSourcesBlocked !== 0)
  ) {
    throw new Error("Permit county completeness does not reconcile");
  }
  return {
    pilotState: row.permit_pilot_passed ? "passed" : "failed",
    countyCompleteness: row.permit_county_complete
      ? "complete"
      : "not_complete",
    recordedAt,
    sampleParcels: nullableCount(row.permit_sample_parcels),
    sourceAttempts: nullableCount(row.permit_source_attempts),
    sourceUnavailable: nullableCount(row.permit_source_unavailable),
    sourceFailures,
    uniqueRecords: nullableCount(row.permit_unique_records),
    queryRows: nullableCount(row.permit_query_rows),
    allInputTerminal: row.permit_all_input_terminal,
    allRecordsAccounted: row.permit_all_records_accounted,
    queryRowsMatch: row.permit_query_rows_match,
    registryJurisdictions,
    currentSourcesImplemented,
    currentSourcesBlocked,
  };
}

/**
 * Return an empty aggregate worker status before local checkpoints are read.
 *
 * @returns {PermitEnumerationStatus} Empty fixed-shape status.
 */
function emptyPermitEnumerationStatus() {
  return {
    workers: [],
    activeWorkers: 0,
    completedWorkers: 0,
    completedWindows: 0,
    totalWindows: 0,
    accessibleRecords: 0,
    excludedRecords: 0,
    invalidRecords: 0,
    sourceMissingRecords: 0,
  };
}

/**
 * Build the public aggregate response from one database row.
 *
 * @param {RecoveryAggregateRow} row - Aggregate-only query result.
 * @param {number} nowMs - Snapshot epoch milliseconds.
 * @returns {RecoveryDashboardStatus} PII-free dashboard payload.
 */
export function buildRecoveryStatus(row, nowMs) {
  const properties = count(row.property_count);
  const distinctFolios = count(row.distinct_folios);
  const verifiedProperties = count(row.verified_properties);
  const terminalSourceMisses = count(row.terminal_source_misses);
  if (properties !== distinctFolios) {
    throw new Error("Durable Broward property and folio counts differ");
  }
  if (verifiedProperties > properties) {
    throw new Error("Verified Broward properties exceed visible properties");
  }
  const durableCompleted = verifiedProperties + terminalSourceMisses;
  const remaining = Math.max(0, BROWARD_ROW_DENOMINATOR - durableCompleted);
  const recentProperties = count(row.recent_properties);
  return {
    schemaVersion: 1,
    generatedAt: new Date(nowMs).toISOString(),
    county: "Broward",
    branch: "broward-ingest",
    denominator: BROWARD_ROW_DENOMINATOR,
    process: {
      running: row.recovery_lock_held,
      lastCommitAt: row.last_commit_at,
    },
    progress: {
      properties,
      distinctFolios,
      verifiedProperties,
      terminalSourceMisses,
      durableCompleted,
      remaining,
      completionPercent:
        Math.round(
          Math.min(1, durableCompleted / BROWARD_ROW_DENOMINATOR) * 100 * 1_000,
        ) / 1_000,
      committedChunks: count(row.committed_chunks),
      preparedRows: count(row.prepared_rows),
      committedRows: count(row.committed_rows),
    },
    failures: {
      sourceMissAttempts: count(row.source_miss_attempts),
      sourceErrorAttempts: count(row.source_error_attempts),
      transformErrorAttempts: count(row.transform_error_attempts),
      loadErrorAttempts: count(row.load_error_attempts),
    },
    throughput: {
      windowMinutes: 15,
      propertiesPerMinute: Math.round((recentProperties / 15) * 100) / 100,
    },
    permit: buildPermitStatus(row),
    permitInventory: {
      records: count(row.permit_inventory_records),
      matched: count(row.permit_inventory_matched),
      unmatched: count(row.permit_inventory_unmatched),
      roofing: count(row.permit_inventory_roofing),
      distinctParcels: count(row.permit_inventory_parcels),
      sourceSystems: count(row.permit_inventory_sources),
      lastLoadedAt: row.permit_inventory_loaded_at,
      bulkSourceRows: count(row.permit_bulk_source_rows),
      bulkCommittedRows: count(row.permit_bulk_committed_rows),
      bulkChunks: count(row.permit_bulk_chunks),
      listLoadedRows: count(row.permit_list_loaded_rows),
      listChunks: count(row.permit_list_chunks),
    },
    permitEnumeration: emptyPermitEnumerationStatus(),
    sunbizMatch: {
      matchedAddressRoles: count(row.sunbiz_match_roles),
      registrations: count(row.sunbiz_match_registrations),
      properties: count(row.sunbiz_match_properties),
      chunks: count(row.sunbiz_match_chunks),
    },
  };
}

const PERMIT_ENUMERATION_CHECKPOINTS = Object.freeze([
  {
    source: "Hollywood",
    family: "accela_csv",
    relativePath:
      "downloads/broward/accela-csv-windows/hollywood-full/checkpoint.private.json",
  },
  {
    source: "Plantation",
    family: "accela_csv",
    relativePath:
      "downloads/broward/accela-csv-windows/plantation-full-v2/checkpoint.private.json",
  },
  {
    source: "Cooper City",
    family: "accela_csv",
    relativePath:
      "downloads/broward/accela-csv-windows/cooper-city-full/checkpoint.private.json",
  },
  {
    source: "Weston",
    family: "accela_csv",
    relativePath:
      "downloads/broward/accela-csv-windows/weston-full/checkpoint.private.json",
  },
  {
    source: "Pembroke Pines",
    family: "tyler_api",
    relativePath:
      "downloads/broward/tyler-date-windows/pembroke-pines-full-30d/checkpoint.private.json",
  },
  {
    source: "Hallandale Beach",
    family: "tyler_api",
    relativePath:
      "downloads/broward/tyler-date-windows/hallandale-beach-full-30d/checkpoint.private.json",
  },
  {
    source: "Miramar",
    family: "tyler_api",
    relativePath:
      "downloads/broward/tyler-date-windows/miramar-full-2019/checkpoint.private.json",
  },
  {
    source: "Oakland Park",
    family: "tyler_api",
    relativePath:
      "downloads/broward/tyler-date-windows/oakland-park-full-30d/checkpoint.private.json",
  },
]);

/**
 * Read aggregate-only local permit enumerator checkpoints.
 *
 * @param {string} repositoryRoot - Repository root containing downloads.
 * @param {number} [nowMs=Date.now()] - Snapshot time.
 * @returns {Promise<PermitEnumerationStatus>} PII-free worker status.
 */
export async function readPermitEnumerationStatus(
  repositoryRoot,
  nowMs = Date.now(),
) {
  const workers = await Promise.all(
    PERMIT_ENUMERATION_CHECKPOINTS.map(async (definition) => {
      try {
        const parsed = /** @type {unknown} */ (
          JSON.parse(
            await readFile(
              path.resolve(repositoryRoot, definition.relativePath),
              "utf8",
            ),
          )
        );
        if (
          parsed === null ||
          typeof parsed !== "object" ||
          Array.isArray(parsed)
        ) {
          throw new Error("Permit enumeration checkpoint is not an object");
        }
        const checkpoint = /** @type {Record<string, unknown>} */ (parsed);
        if (
          !Array.isArray(checkpoint.pendingWindows) ||
          checkpoint.completedWindows === null ||
          typeof checkpoint.completedWindows !== "object" ||
          Array.isArray(checkpoint.completedWindows) ||
          typeof checkpoint.updatedAt !== "string"
        ) {
          throw new Error("Permit enumeration checkpoint is malformed");
        }
        const receipts = Object.values(
          /** @type {Record<string, Record<string, unknown>>} */ (
            checkpoint.completedWindows
          ),
        );
        let accessibleRecords = 0;
        let excludedRecords = 0;
        let invalidRecords = 0;
        let sourceMissingRecords = 0;
        for (const receipt of receipts) {
          if (definition.family === "accela_csv") {
            accessibleRecords += safeAggregate(receipt.exportedRecordCount);
            excludedRecords += safeAggregate(receipt.excludedNonPermitCount);
          } else {
            const total = safeAggregate(receipt.totalFound);
            const invalid = safeAggregate(receipt.invalidRecordCount);
            const missing = safeAggregate(receipt.sourceMissingRecordCount);
            invalidRecords += invalid;
            sourceMissingRecords += missing;
            accessibleRecords += Math.max(0, total - invalid - missing);
          }
        }
        const completedWindows = receipts.length;
        const pendingWindows = checkpoint.pendingWindows.length;
        const totalWindows = completedWindows + pendingWindows;
        const updatedAt = checkpoint.updatedAt;
        const updatedMs = Date.parse(updatedAt);
        const complete = pendingWindows === 0;
        const recentlyActive =
          Number.isFinite(updatedMs) &&
          nowMs - updatedMs >= 0 &&
          nowMs - updatedMs <= 5 * 60_000;
        return {
          source: definition.source,
          family: /** @type {"accela_csv" | "tyler_api"} */ (definition.family),
          status: complete
            ? /** @type {"complete"} */ ("complete")
            : recentlyActive
              ? /** @type {"running"} */ ("running")
              : /** @type {"paused"} */ ("paused"),
          completedWindows,
          pendingWindows,
          totalWindows,
          completionPercent:
            totalWindows === 0
              ? 0
              : Math.round((completedWindows / totalWindows) * 100_000) / 1_000,
          accessibleRecords,
          excludedRecords,
          invalidRecords,
          sourceMissingRecords,
          updatedAt,
        };
      } catch (error) {
        if (isNodeError(error) && error.code === "ENOENT") {
          return {
            source: definition.source,
            family: /** @type {"accela_csv" | "tyler_api"} */ (
              definition.family
            ),
            status: /** @type {"not_started"} */ ("not_started"),
            completedWindows: 0,
            pendingWindows: 0,
            totalWindows: 0,
            completionPercent: 0,
            accessibleRecords: 0,
            excludedRecords: 0,
            invalidRecords: 0,
            sourceMissingRecords: 0,
            updatedAt: null,
          };
        }
        throw error;
      }
    }),
  );
  return {
    workers,
    activeWorkers: workers.filter((worker) => worker.status === "running")
      .length,
    completedWorkers: workers.filter((worker) => worker.status === "complete")
      .length,
    completedWindows: workers.reduce(
      (sum, worker) => sum + worker.completedWindows,
      0,
    ),
    totalWindows: workers.reduce((sum, worker) => sum + worker.totalWindows, 0),
    accessibleRecords: workers.reduce(
      (sum, worker) => sum + worker.accessibleRecords,
      0,
    ),
    excludedRecords: workers.reduce(
      (sum, worker) => sum + worker.excludedRecords,
      0,
    ),
    invalidRecords: workers.reduce(
      (sum, worker) => sum + worker.invalidRecords,
      0,
    ),
    sourceMissingRecords: workers.reduce(
      (sum, worker) => sum + worker.sourceMissingRecords,
      0,
    ),
  };
}

/**
 * Convert an optional checkpoint aggregate to a non-negative integer.
 *
 * @param {unknown} value - Optional receipt count.
 * @returns {number} Safe count, with absent pre-version fields treated as zero.
 */
function safeAggregate(value) {
  if (value === undefined || value === null) return 0;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error("Permit enumeration checkpoint has an invalid count");
  }
  return parsed;
}

/**
 * Narrow an unknown error to a Node error with a string code.
 *
 * @param {unknown} value - Caught value.
 * @returns {value is Error & {code:string}} Whether a code exists.
 */
function isNodeError(value) {
  return (
    value instanceof Error && "code" in value && typeof value.code === "string"
  );
}

/**
 * Verify dashboard connection identity using Neon server settings.
 *
 * @param {import("pg").Client | import("pg").PoolClient} client - Connected direct Neon client.
 * @param {DashboardOptions} options - Required branch and endpoint IDs.
 * @returns {Promise<void>} Resolves only for the isolated target.
 */
async function verifyIdentity(client, options) {
  const result = await client.query(
    `SELECT
       current_setting('neon.project_id', true) AS project_id,
       current_setting('neon.branch_id', true) AS branch_id,
       current_setting('neon.endpoint_id', true) AS endpoint_id`,
  );
  const row = result.rows[0];
  if (
    row?.project_id !== EXPECTED_PROJECT_ID ||
    row?.branch_id !== options.expectedBranchId ||
    row?.endpoint_id !== options.expectedEndpointId ||
    options.expectedEndpointId.startsWith(PRODUCTION_ENDPOINT_PREFIX)
  ) {
    throw new Error("Dashboard Neon identity is not isolated broward-ingest");
  }
}

/**
 * Create a durable aggregate snapshot reader.
 *
 * @param {import("pg").Client | import("pg").PoolClient} client - Identity-verified Neon client.
 * @param {string} [repositoryRoot=process.cwd()] - Local checkpoint root.
 * @returns {() => Promise<RecoveryDashboardStatus>} Async snapshot function.
 */
export function createRecoveryStatusReader(
  client,
  repositoryRoot = process.cwd(),
) {
  /** @type {Promise<RecoveryDashboardStatus> | null} */
  let inFlight = null;
  return () => {
    if (inFlight !== null) return inFlight;
    inFlight = (async () => {
      const [result, permitEnumeration] = await Promise.all([
        client.query(
          `WITH property_stats AS (
         SELECT
           count(*)::bigint AS property_count,
           count(*)::bigint AS distinct_folios
         FROM ${CONTROL_SCHEMA}.broward_appraisal_completed_items
       ),
       terminal_stats AS (
         SELECT count(*)::bigint AS terminal_source_misses
         FROM ${CONTROL_SCHEMA}.broward_appraisal_terminal_items
       ),
       completed_stats AS (
         SELECT
           count(*)::bigint AS verified_properties,
           count(*) FILTER (
             WHERE recorded_at >= now() - interval '15 minutes'
           )::bigint AS recent_properties
         FROM ${CONTROL_SCHEMA}.broward_appraisal_completed_items
       ),
       chunk_stats AS (
         SELECT
           count(*)::bigint AS committed_chunks,
           COALESCE(sum(prepared_row_count), 0)::bigint AS prepared_rows,
           COALESCE(sum(committed_row_count), 0)::bigint AS committed_rows,
           max(committed_at)::text AS last_commit_at
         FROM ${CONTROL_SCHEMA}.broward_appraisal_chunks
       ),
       event_stats AS (
         SELECT
           COALESCE(sum(event_count) FILTER (WHERE stage = 'source_miss'), 0)::bigint
             AS source_miss_attempts,
           COALESCE(sum(event_count) FILTER (WHERE stage = 'source_error'), 0)::bigint
             AS source_error_attempts,
           COALESCE(sum(event_count) FILTER (WHERE stage = 'transform_error'), 0)::bigint
             AS transform_error_attempts,
           COALESCE(sum(event_count) FILTER (WHERE stage = 'load_error'), 0)::bigint
             AS load_error_attempts
         FROM ${CONTROL_SCHEMA}.broward_appraisal_events
       ),
       permit_stats AS (
         SELECT
           status.recorded_at AS permit_recorded_at,
           status.sample_parcels AS permit_sample_parcels,
           status.permit_source_attempts AS permit_source_attempts,
           status.source_unavailable_outcomes AS permit_source_unavailable,
           status.source_failures AS permit_source_failures,
           status.unique_permit_records AS permit_unique_records,
           status.query_rows AS permit_query_rows,
           status.all_input_parcels_terminal AS permit_all_input_terminal,
           status.all_records_accounted_for AS permit_all_records_accounted,
           status.query_rows_match_unique_records AS permit_query_rows_match,
           status.local_pilot_passed AS permit_pilot_passed,
           status.county_permit_complete AS permit_county_complete,
           control.registry_jurisdiction_count
             AS permit_registry_jurisdictions,
           control.current_source_implemented_count
             AS permit_sources_implemented,
           control.current_source_blocked_count AS permit_sources_blocked
         FROM ${CONTROL_SCHEMA}.broward_permit_control AS control
         LEFT JOIN ${CONTROL_SCHEMA}.broward_permit_status AS status
           ON status.pipeline_key = control.pipeline_key
         WHERE control.pipeline_key = 'broward-permit'
       ),
       permit_inventory_stats AS (
         SELECT
           coalesce(permit_records,0)::bigint AS permit_inventory_records,
           coalesce(permit_matched,0)::bigint AS permit_inventory_matched,
           coalesce(permit_unmatched,0)::bigint AS permit_inventory_unmatched,
           coalesce(permit_roofing,0)::bigint AS permit_inventory_roofing,
           coalesce(permit_parcels,0)::bigint AS permit_inventory_parcels,
           coalesce(permit_source_systems,0)::bigint
             AS permit_inventory_sources,
           permit_last_loaded_at::text AS permit_inventory_loaded_at
         FROM ${CONTROL_SCHEMA}.broward_dashboard_rollup
         WHERE pipeline_key='broward'
       ),
       permit_bulk_stats AS (
         SELECT
           coalesce(max(source_object_id_count),0)::bigint
             AS permit_bulk_source_rows,
           coalesce(max(committed_source_record_count),0)::bigint
             AS permit_bulk_committed_rows,
           coalesce(max(committed_chunk_count),0)::bigint
             AS permit_bulk_chunks
         FROM ${CONTROL_SCHEMA}.broward_bulk_permit_runs
       ),
       permit_list_load_stats AS (
         SELECT
           coalesce(sum(record_count),0)::bigint AS permit_list_loaded_rows,
           count(*)::bigint AS permit_list_chunks
         FROM ${CONTROL_SCHEMA}.broward_permit_list_load_chunks
       ),
       sunbiz_match_stats AS (
         SELECT
           sunbiz_matched_roles::bigint AS sunbiz_match_roles,
           sunbiz_registrations::bigint AS sunbiz_match_registrations,
           sunbiz_properties::bigint AS sunbiz_match_properties,
           (
             SELECT count(*)::bigint
             FROM ${CONTROL_SCHEMA}.broward_sunbiz_match_chunks
             WHERE job_id='broward-sunbiz-property-full-20260831'
           ) AS sunbiz_match_chunks
         FROM ${CONTROL_SCHEMA}.broward_dashboard_rollup
         WHERE pipeline_key='broward'
       )
       SELECT
         property_stats.*,
         terminal_stats.*,
         completed_stats.*,
         chunk_stats.*,
         event_stats.*,
         permit_stats.*,
         permit_inventory_stats.*,
         permit_bulk_stats.*,
         permit_list_load_stats.*,
         sunbiz_match_stats.*,
         EXISTS (
           SELECT 1
           FROM pg_locks
           WHERE locktype = 'advisory'
             AND classid = 12011
             AND objid = 1
         ) AS recovery_lock_held
       FROM property_stats,
            terminal_stats,
            completed_stats,
            chunk_stats,
            event_stats,
            permit_stats,
            permit_inventory_stats,
            permit_bulk_stats,
            permit_list_load_stats,
            sunbiz_match_stats`,
        ),
        readPermitEnumerationStatus(repositoryRoot),
      ]);
      const row = result.rows[0];
      if (row === undefined) {
        throw new Error("Neon returned no Broward recovery aggregate");
      }
      const status = buildRecoveryStatus(
        /** @type {RecoveryAggregateRow} */ (row),
        Date.now(),
      );
      status.permitEnumeration = permitEnumeration;
      return status;
    })().finally(() => {
      inFlight = null;
    });
    return inFlight;
  };
}

/**
 * Create a reconnecting, coalesced dashboard reader with stale fallback.
 *
 * A fresh pool client is identity-verified for every uncached snapshot. A
 * broken Neon socket is destroyed instead of becoming a permanently stuck
 * shared client. When a refresh fails after a successful read, callers receive
 * the last verified aggregate marked stale.
 *
 * @param {import("pg").Pool} pool - Direct Neon connection pool.
 * @param {DashboardOptions} options - Required isolated target IDs.
 * @param {string} [repositoryRoot=process.cwd()] - Local checkpoint root.
 * @param {number} [cacheMs=10000] - Fresh snapshot cache duration.
 * @param {number} [timeoutMs=25000] - Hard refresh wall timeout.
 * @returns {() => Promise<RecoveryDashboardStatus>} Resilient status reader.
 */
export function createResilientRecoveryStatusReader(
  pool,
  options,
  repositoryRoot = process.cwd(),
  cacheMs = 10_000,
  timeoutMs = 25_000,
) {
  /** @type {Promise<RecoveryDashboardStatus> | null} */
  let inFlight = null;
  /** @type {RecoveryDashboardStatus | null} */
  let lastSuccessful = null;
  let lastSuccessfulAtMs = 0;

  return () => {
    const nowMs = Date.now();
    if (
      lastSuccessful !== null &&
      nowMs - lastSuccessfulAtMs >= 0 &&
      nowMs - lastSuccessfulAtMs < cacheMs
    ) {
      return Promise.resolve(lastSuccessful);
    }
    if (inFlight !== null) return inFlight;
    inFlight = (async () => {
      const client = await pool.connect();
      let destroyClient = false;
      try {
        await verifyIdentity(client, options);
        const status = await withDashboardTimeout(
          createRecoveryStatusReader(client, repositoryRoot)(),
          timeoutMs,
        );
        const completedAtMs = Date.now();
        status.dashboardHealth = {
          stale: false,
          lastSuccessfulAt: new Date(completedAtMs).toISOString(),
          snapshotAgeSeconds: 0,
        };
        lastSuccessful = status;
        lastSuccessfulAtMs = completedAtMs;
        return status;
      } catch (error) {
        destroyClient = true;
        if (lastSuccessful !== null) {
          const fallback = structuredClone(lastSuccessful);
          fallback.generatedAt = new Date().toISOString();
          fallback.dashboardHealth = {
            stale: true,
            lastSuccessfulAt: new Date(lastSuccessfulAtMs).toISOString(),
            snapshotAgeSeconds: Math.max(
              0,
              Math.round((Date.now() - lastSuccessfulAtMs) / 1_000),
            ),
          };
          return fallback;
        }
        throw error;
      } finally {
        client.release(destroyClient);
      }
    })().finally(() => {
      inFlight = null;
    });
    return inFlight;
  };
}

/**
 * Apply a hard wall timeout to one aggregate refresh.
 *
 * @template Result
 * @param {Promise<Result>} promise - Refresh operation.
 * @param {number} timeoutMs - Maximum wall time.
 * @returns {Promise<Result>} Result before timeout.
 */
async function withDashboardTimeout(promise, timeoutMs) {
  /** @type {NodeJS.Timeout | undefined} */
  let timeout;
  try {
    return await Promise.race([
      promise,
      new Promise((_, rejectPromise) => {
        timeout = setTimeout(
          () => rejectPromise(new Error("Dashboard refresh timed out")),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

/**
 * Write a no-store JSON response.
 *
 * @param {import("node:http").ServerResponse} response - HTTP response.
 * @param {number} statusCode - HTTP status.
 * @param {Record<string, unknown>} payload - Aggregate-only body.
 * @returns {void}
 */
function writeJson(response, statusCode, payload) {
  const body = `${JSON.stringify(payload)}\n`;
  response.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body),
    "cache-control": "no-store",
    "x-content-type-options": "nosniff",
  });
  response.end(body);
}

/**
 * Create the fixed-purpose recovery dashboard server.
 *
 * @param {() => Promise<RecoveryDashboardStatus>} readStatus - Snapshot reader.
 * @returns {import("node:http").Server} Unstarted HTTP server.
 */
export function createRecoveryDashboardServer(readStatus) {
  return createServer((request, response) => {
    void (async () => {
      const requestUrl = new URL(request.url ?? "/", "http://dashboard.local");
      if (request.method !== "GET" && request.method !== "HEAD") {
        writeJson(response, 405, { error: "Method not allowed" });
        return;
      }
      if (requestUrl.pathname === "/healthz") {
        writeJson(response, 200, {
          ok: true,
          service: "broward-neon-recovery-dashboard",
        });
        return;
      }
      if (requestUrl.pathname === "/api/status") {
        try {
          writeJson(
            response,
            200,
            /** @type {Record<string, unknown>} */ (await readStatus()),
          );
        } catch {
          writeJson(response, 503, {
            error: "Aggregate status is temporarily unavailable",
          });
        }
        return;
      }
      if (requestUrl.pathname === "/") {
        response.writeHead(200, {
          "content-type": "text/html; charset=utf-8",
          "content-length": Buffer.byteLength(DASHBOARD_HTML),
          "cache-control": "no-store",
          "content-security-policy":
            "default-src 'none'; connect-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'",
          "referrer-policy": "no-referrer",
          "x-content-type-options": "nosniff",
          "x-frame-options": "DENY",
        });
        response.end(request.method === "HEAD" ? "" : DASHBOARD_HTML);
        return;
      }
      writeJson(response, 404, { error: "Not found" });
    })();
  });
}

const DASHBOARD_HTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Broward durable ingestion</title>
  <style>
    :root { color-scheme: dark; font-family: system-ui, sans-serif; }
    body { margin: 0; background: #07111f; color: #edf6ff; }
    main { width: min(72rem, 100%); margin: auto; padding: 1.25rem; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(13rem, 1fr)); gap: 1rem; }
    article { padding: 1rem; border: 1px solid #29415a; border-radius: .8rem; background: #102034; }
    h1 { margin-bottom: .25rem; } h2 { color: #a9bed2; font-size: 1rem; }
    strong { display: block; font-size: 1.8rem; font-variant-numeric: tabular-nums; }
    progress { width: 100%; height: 1rem; }
    table { width: 100%; margin: 1rem 0; border-collapse: collapse; }
    th, td { padding: .55rem; border-bottom: 1px solid #29415a; text-align: left; }
    td:nth-child(n+3) { font-variant-numeric: tabular-nums; text-align: right; }
    #error { color: #ff8290; }
  </style>
</head>
<body><main>
  <h1>Broward durable ingestion status</h1>
  <p>Verified appraisal progress and bounded permit evidence for the isolated broward-ingest branch.</p>
  <progress id="bar" max="100" value="0"></progress>
  <p id="summary">Loading…</p>
  <section class="grid">
    <article><h2>Properties / folios</h2><strong id="properties">—</strong></article>
    <article><h2>Durable completed</h2><strong id="completed">—</strong></article>
    <article><h2>Prepared rows</h2><strong id="prepared">—</strong></article>
    <article><h2>Committed rows</h2><strong id="committed">—</strong></article>
    <article><h2>Recent throughput</h2><strong id="rate">—</strong></article>
    <article><h2>Source misses</h2><strong id="misses">—</strong></article>
    <article><h2>Source errors</h2><strong id="source-errors">—</strong></article>
    <article><h2>Transform errors</h2><strong id="transform-errors">—</strong></article>
    <article><h2>Load errors</h2><strong id="load-errors">—</strong></article>
  </section>
  <h2>Permit ingestion</h2>
  <p id="permit-inventory-summary">Loading permit inventory…</p>
  <section class="grid">
    <article><h2>Neon permit records</h2><strong id="permit-inventory">—</strong></article>
    <article><h2>Matched / unlinked</h2><strong id="permit-links">—</strong></article>
    <article><h2>Roofing permits</h2><strong id="permit-roofing">—</strong></article>
    <article><h2>Loaded source systems</h2><strong id="permit-systems">—</strong></article>
    <article><h2>Bulk source rows</h2><strong id="permit-bulk">—</strong></article>
    <article><h2>Local captured records</h2><strong id="permit-captured">—</strong></article>
    <article><h2>Active / complete workers</h2><strong id="permit-workers">—</strong></article>
    <article><h2>Completed windows</h2><strong id="permit-windows">—</strong></article>
  </section>
  <table aria-label="Permit tenant worker status">
    <thead><tr><th>Jurisdiction</th><th>Source</th><th>Status</th><th>Windows</th><th>Records</th><th>Gaps</th></tr></thead>
    <tbody id="permit-worker-rows"></tbody>
  </table>
  <h2>Sunbiz property matching</h2>
  <section class="grid">
    <article><h2>Matched registrations</h2><strong id="sunbiz-registrations">—</strong></article>
    <article><h2>Matched properties</h2><strong id="sunbiz-properties">—</strong></article>
    <article><h2>Exact address roles</h2><strong id="sunbiz-roles">—</strong></article>
    <article><h2>Durable chunks</h2><strong id="sunbiz-chunks">—</strong></article>
  </section>
  <h2>Permit coverage boundary</h2>
  <p id="permit-summary">Loading durable permit evidence…</p>
  <section class="grid">
    <article><h2>Pilot status</h2><strong id="permit-pilot">—</strong></article>
    <article><h2>County completeness</h2><strong id="permit-completeness">—</strong></article>
    <article><h2>Pilot sample</h2><strong id="permit-sample">—</strong></article>
    <article><h2>Bounded source attempts</h2><strong id="permit-attempts">—</strong></article>
    <article><h2>Queryable records</h2><strong id="permit-records">—</strong></article>
    <article><h2>Current routes</h2><strong id="permit-routes">—</strong></article>
  </section>
  <p id="error"></p>
  <p>Only aggregate counts and timestamps are exposed. Refreshes every five seconds.</p>
<script>
  "use strict";
  const format = new Intl.NumberFormat();
  const set = (id, value) => { const node = document.getElementById(id); if (node) node.textContent = value; };
  const nullable = (value) => value === null ? "Not recorded" : format.format(value);
  async function refresh() {
    try {
      const response = await fetch("/api/status", { cache: "no-store" });
      if (!response.ok) throw new Error("unavailable");
      const status = await response.json();
      const progress = status.progress;
      const failures = status.failures;
      document.getElementById("bar").value = progress.completionPercent;
      set("summary", progress.completionPercent.toFixed(3) + "% · " + format.format(progress.remaining) + " remaining · " + (status.process.running ? "running" : "stopped"));
      set("properties", format.format(progress.properties) + " / " + format.format(progress.distinctFolios));
      set("completed", format.format(progress.durableCompleted));
      set("prepared", format.format(progress.preparedRows));
      set("committed", format.format(progress.committedRows));
      set("rate", status.throughput.propertiesPerMinute.toFixed(2) + "/min");
      set("misses", format.format(failures.sourceMissAttempts));
      set("source-errors", format.format(failures.sourceErrorAttempts));
      set("transform-errors", format.format(failures.transformErrorAttempts));
      set("load-errors", format.format(failures.loadErrorAttempts));
      const inventory = status.permitInventory;
      const enumeration = status.permitEnumeration;
      set("permit-inventory", format.format(inventory.records));
      set("permit-links", format.format(inventory.matched) + " / " + format.format(inventory.unmatched));
      set("permit-roofing", format.format(inventory.roofing));
      set("permit-systems", format.format(inventory.sourceSystems));
      set("permit-bulk", format.format(inventory.bulkCommittedRows) + " / " + format.format(inventory.bulkSourceRows));
      set("permit-captured", format.format(enumeration.accessibleRecords));
      set("permit-workers", format.format(enumeration.activeWorkers) + " / " + format.format(enumeration.completedWorkers));
      set("permit-windows", format.format(enumeration.completedWindows) + " / " + format.format(enumeration.totalWindows));
      set(
        "permit-inventory-summary",
        format.format(inventory.records) + " loaded · " +
          format.format(enumeration.accessibleRecords) + " locally captured · " +
          format.format(enumeration.excludedRecords) + " excluded · " +
          format.format(enumeration.invalidRecords + enumeration.sourceMissingRecords) + " source gaps",
      );
      const workerBody = document.getElementById("permit-worker-rows");
      if (workerBody) {
        const rows = enumeration.workers.map((worker) => {
          const row = document.createElement("tr");
          for (const value of [
            worker.source,
            worker.family.replaceAll("_", " "),
            worker.status,
            format.format(worker.completedWindows) + " / " + format.format(worker.totalWindows),
            format.format(worker.accessibleRecords),
            format.format(worker.invalidRecords + worker.sourceMissingRecords),
          ]) {
            const cell = document.createElement("td");
            cell.textContent = value;
            row.appendChild(cell);
          }
          return row;
        });
        workerBody.replaceChildren(...rows);
      }
      const sunbiz = status.sunbizMatch;
      set("sunbiz-registrations", format.format(sunbiz.registrations));
      set("sunbiz-properties", format.format(sunbiz.properties));
      set("sunbiz-roles", format.format(sunbiz.matchedAddressRoles));
      set("sunbiz-chunks", format.format(sunbiz.chunks));
      const permit = status.permit;
      set("permit-pilot", permit.pilotState.replaceAll("_", " "));
      set("permit-completeness", permit.countyCompleteness.replaceAll("_", " "));
      set("permit-sample", nullable(permit.sampleParcels));
      set("permit-attempts", nullable(permit.sourceAttempts));
      set("permit-records", nullable(permit.queryRows));
      set("permit-routes", format.format(permit.currentSourcesImplemented) + " implemented / " + format.format(permit.currentSourcesBlocked) + " blocked");
      set(
        "permit-summary",
        permit.pilotState === "not_recorded"
          ? "No durable permit pilot evidence is recorded; missing counts are not zero."
          : "Bounded pilot " + permit.pilotState + "; countywide completeness is " + permit.countyCompleteness.replaceAll("_", " ") + ".",
      );
      set("error", "");
    } catch { set("error", "Aggregate status is temporarily unavailable; retrying."); }
  }
  void refresh(); setInterval(() => void refresh(), 10000);
</script>
</main></body></html>`;

/**
 * Start the read-only dashboard after target identity verification.
 *
 * @param {DashboardOptions} options - Network and safety configuration.
 * @returns {Promise<void>} Resolves after the server starts listening.
 */
async function runDashboard(options) {
  const databaseUrl = process.env.DATABASE_URL_UNPOOLED;
  if (typeof databaseUrl !== "string" || databaseUrl.trim().length === 0) {
    throw new Error("DATABASE_URL_UNPOOLED is required");
  }
  const pool = new Pool({
    connectionString: databaseUrl,
    application_name: "broward-neon-recovery-dashboard",
    connectionTimeoutMillis: 10_000,
    statement_timeout: 30_000,
    query_timeout: 30_000,
    max: 2,
    idleTimeoutMillis: 30_000,
  });
  pool.on("error", (error) => {
    console.error(
      JSON.stringify({
        event: "broward_recovery_dashboard_pool_error",
        message: error instanceof Error ? error.message : "Unknown pool error",
      }),
    );
  });
  const identityClient = await pool.connect();
  try {
    await verifyIdentity(identityClient, options);
  } finally {
    identityClient.release();
  }
  const server = createRecoveryDashboardServer(
    createResilientRecoveryStatusReader(pool, options),
  );
  server.listen(options.port, options.host, () => {
    console.log(
      JSON.stringify({
        event: "broward_recovery_dashboard_listening",
        host: options.host,
        port: options.port,
        branch: "broward-ingest",
      }),
    );
  });
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  runDashboard(parseDashboardOptions(process.argv.slice(2))).catch((error) => {
    console.error(
      error instanceof Error
        ? error.message
        : "Broward recovery dashboard failed",
    );
    process.exitCode = 1;
  });
}
