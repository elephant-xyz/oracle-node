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
import {
  BROWARD_BCS_ADAPTER_KEY,
  BROWARD_CITIZENSERVE_ADAPTER_KEY,
  BROWARD_PERMIT_JURISDICTIONS,
  BROWARD_PERMIT_REGISTRY_VERSION,
} from "./broward-permit-jurisdictions.mjs";
import {
  createActivePermitEnumerationTracker,
  markActivePermitEnumerationSnapshotStale,
  readActiveEnumerationProcessSnapshot,
} from "./broward-active-permit-enumeration.mjs";
import { BROWARD_MUNICIPAL_PERMIT_JURISDICTIONS } from "./permit-source-adapters/broward-municipal-config.mjs";

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
 * @typedef {import("./broward-active-permit-enumeration.mjs").ActiveEnumerationRouteDefinition} ActiveEnumerationRouteDefinition
 * @typedef {import("./broward-active-permit-enumeration.mjs").ActiveEnumerationProcessSnapshot} ActiveEnumerationProcessSnapshot
 * @typedef {import("./broward-active-permit-enumeration.mjs").ActivePermitEnumerationStatus} ActivePermitEnumerationStatus
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
 * @property {string | number} coral_etrakit_loaded - Loaded Coral slice rows.
 * @property {string | number} coral_etrakit_linked - Exact-folio-linked Coral rows.
 * @property {string | number} coral_etrakit_roofing - Loaded roofing rows.
 * @property {string | number} pembroke_park_gov_easy_loaded
 *   Durable loaded-record aggregate for the bounded Pembroke Park slice.
 * @property {string | number} hillsboro_beach_communitycore_loaded
 *   Durable loaded-record aggregate for Hillsboro Beach.
 * @property {string | number} sunbiz_match_roles - Exact matched address roles.
 * @property {string | number} sunbiz_match_registrations - Distinct linked registrations.
 * @property {string | number} sunbiz_match_properties - Distinct linked properties.
 * @property {string | number} sunbiz_match_chunks - Durable full-run match chunks.
 *
 * @typedef {object} PermitEnumerationWorkerStatus
 * @property {string} source - Public jurisdiction label.
 * @property {"accela_csv" | "tyler_api" | "property_first" | "municipal_type" | "municipal_property"} family - Source mechanism.
 * @property {"not_started" | "running" | "cooling_down" | "paused" | "complete"} status
 *   Aggregate checkpoint activity state.
 * @property {number} completedWindows - Durable completed windows.
 * @property {number} pendingWindows - Remaining windows.
 * @property {number} totalWindows - Initial source windows.
 * @property {number} completionPercent - Window completion percentage.
 * @property {number} accessibleRecords - Source records retained for loading.
 * @property {number} excludedRecords - Explicit non-permit rows.
 * @property {number} invalidRecords - Malformed source rows.
 * @property {number} sourceMissingRecords - Reported but inaccessible rows.
 * @property {number} deferredCapCount - Unresolved item-level exclusive-cap queries.
 * @property {string | null} updatedAt - Last durable checkpoint time.
 * @property {"timeout" | "missing_controls" | "missing_export" | "source_cap" | "incomplete_pagination" | "checkpoint_stale" | "supervisor_not_running" | "process_unknown" | null} pauseReason
 *   Allowlisted operational reason when this worker is paused.
 * @property {"timeout" | "source_cap" | "incomplete_pagination" | "source_error" | "operator_hold" | null} cooldownReason
 *   Allowlisted source circuit-breaker reason while cooling down.
 * @property {string | null} nextAttemptAt - Earliest safe automatic retry.
 * @property {string | null} coverageBoundary - Public custody/history boundary.
 * @property {string | null} [startBlocker] - Allowlisted no-start gate.
 * @property {boolean | null} [processAlive] - Live supervisor evidence.
 * @property {boolean | null} [detailActive] - Recent bounded child evidence.
 * @property {string | null} [operatorNotBeforeAt] - Live operator hold boundary.
 *
 * @typedef {object} PausedPermitEnumerationWorker
 * @property {string} source - Public jurisdiction label.
 * @property {"timeout" | "missing_controls" | "missing_export" | "source_cap" | "incomplete_pagination" | "checkpoint_stale" | "supervisor_not_running" | "process_unknown"} reason
 *   Allowlisted operational reason containing no source record or raw error.
 *
 * @typedef {object} CoolingPermitEnumerationWorker
 * @property {string} source - Public jurisdiction label.
 * @property {"timeout" | "source_cap" | "incomplete_pagination" | "source_error" | "operator_hold"} reason
 *   Allowlisted circuit-breaker reason.
 * @property {string} nextAttemptAt - Earliest safe automatic retry.
 * @property {boolean | null} [processAlive] - Live supervisor evidence.
 * @property {boolean | null} [detailActive] - Recent bounded detail evidence.
 * @property {string | null} [operatorNotBeforeAt] - Effective operator hold.
 *
 * @typedef {object} PermitEnumerationStatus
 * @property {PermitEnumerationWorkerStatus[]} workers - Fixed aggregate source list.
 * @property {PausedPermitEnumerationWorker[]} pausedWorkers
 *   Paused jobs, kept separate from current source-route blockers.
 * @property {CoolingPermitEnumerationWorker[]} coolingWorkers
 *   Workers waiting for their durable source cooldown.
 * @property {number} activeWorkers - Recently advancing workers.
 * @property {number} completedWorkers - Fully exhausted workers.
 * @property {number} completedWindows - Durable windows across workers.
 * @property {number} totalWindows - Initial windows across workers.
 * @property {number} accessibleRecords - Inventory rows captured locally.
 * @property {number} excludedRecords - Explicit non-permit source rows.
 * @property {number} invalidRecords - Malformed source rows.
 * @property {number} sourceMissingRecords - Reported but inaccessible rows.
 * @property {number} deferredCapCount - Unresolved item-level exclusive-cap queries.
 *
 * @typedef {"software_or_transport" | "login_required" | "no_anonymous_search" | "custodian_only"} PermitRouteHardBlockKey
 *
 * @typedef {object} PermitRouteHardBlockCategory
 * @property {PermitRouteHardBlockKey} key - Stable hard-block category.
 * @property {"software_transport" | "source_policy"} kind
 *   Whether implementation work can address the route or the source imposes the barrier.
 * @property {string} label - Public hard-block category label.
 * @property {number} count - Number of hard-blocked routes in this category.
 * @property {string[]} jurisdictions - Sorted public jurisdiction names.
 *
 * @typedef {object} BrowardPermitRouteStatus
 * @property {string} registryVersion - Executable registry version.
 * @property {number} totalCurrentRoutes - Current primary routes only.
 * @property {number} implementedCurrentRoutes - Implemented current primary routes.
 * @property {number} manualCaptchaCurrentRoutes
 *   Routes requiring an expiring manually authorized CAPTCHA session.
 * @property {number} hardBlockedCurrentRoutes
 *   Routes unavailable because of software, login, anonymous-search, or
 *   custodian-only barriers.
 * @property {number} unattendedUnavailableCurrentRoutes
 *   Explicit aggregate of manual CAPTCHA and hard-blocked routes.
 * @property {string[]} implementedJurisdictions - Sorted implemented jurisdiction names.
 * @property {string[]} manualCaptchaJurisdictions
 *   Sorted jurisdictions requiring manually authorized CAPTCHA sessions.
 * @property {PermitRouteHardBlockCategory[]} hardBlockCategories
 *   Exhaustive deterministic categories whose counts sum independently to
 *   hardBlockedCurrentRoutes.
 *
 * @typedef {"awaiting_manual_captcha" | "bounded_capture_in_progress" | "bounded_slice_captured" | "bounded_slice_loaded"} ManualCaptchaProgressState
 *
 * @typedef {"private_capture_checkpoint" | "durable_loaded_aggregate" | "no_captured_aggregate"} ManualCaptchaEvidence
 *
 * @typedef {"bounded_capped_slice" | "bounded_slice" | "not_captured"} ManualCaptchaCoverageBoundary
 *
 * @typedef {object} ManualCaptchaRouteProgress
 * @property {string} jurisdiction - Public jurisdiction label.
 * @property {"captcha_required"} registryStatus
 *   Executable route status; manual evidence never promotes the adapter.
 * @property {ManualCaptchaProgressState} progressState
 *   Aggregate-only state of bounded manual evidence.
 * @property {ManualCaptchaEvidence} evidence
 *   Durable aggregate source without artifact or session details.
 * @property {ManualCaptchaCoverageBoundary} coverageBoundary
 *   Explicitly non-countywide evidence boundary.
 * @property {number} capturedRecords - Reconciled bounded captured count.
 * @property {number} loadedRecords - Durable source-system record count.
 * @property {true} manualSessionRequired - CAPTCHA must be completed manually.
 * @property {true} sessionsExpire - Manual browser authorization is temporary.
 * @property {true} validSearchCaptchaRequired
 *   A valid search CAPTCHA is required for another source request.
 * @property {false} countyComplete - Bounded evidence is never county completeness.
 *
 * @typedef {object} ManualCaptchaProgress
 * @property {"manual_captcha_sessions_expire"} sessionPolicy
 *   Public-safe session lifecycle statement.
 * @property {false} countyComplete
 *   Manual bounded evidence does not establish county completeness.
 * @property {ManualCaptchaRouteProgress[]} routes
 *   One aggregate-only row per CAPTCHA-dependent current route.
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
 *   currentSourcesManualCaptcha: number,
 *   currentSourcesHardBlocked: number,
 *   currentSourcesUnattendedUnavailable: number
 * }} permit - Durable bounded-pilot evidence and honest completeness state.
 * @property {BrowardPermitRouteStatus} permitRoutes
 *   Registry-derived implementation, manual-CAPTCHA, and hard-block status.
 * @property {ManualCaptchaProgress} manualCaptchaProgress
 *   Aggregate-only progress for manually authorized CAPTCHA routes.
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
 * @property {{
 *   reported:number,
 *   exposed:number,
 *   paged:number,
 *   unique:number,
 *   details:number,
 *   loaded:number,
 *   linked:number,
 *   roofing:number,
 *   completedPages:number,
 *   totalPages:number,
 *   captureComplete:boolean,
 *   completenessBoundary:"bounded_capped_keyword_slice",
 *   captchaPrerequisite:"manual_authorization_required",
 *   registryStatus:"captcha_required"
 * }} coralSpringsPermit - Separate capped Coral Springs evidence.
 * @property {PermitEnumerationStatus} permitEnumeration - Local tenant workers.
 * @property {ActivePermitEnumerationStatus} activePermitEnumeration
 *   Dedicated live status for the ten current full/property-first enumerators.
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
 * Ordered, exhaustive mapping from non-CAPTCHA unavailable registry statuses
 * to public hard-block categories.
 *
 * @type {readonly {
 *   key: PermitRouteHardBlockKey,
 *   kind: "software_transport" | "source_policy",
 *   label: string,
 *   statuses: readonly string[]
 * }[]}
 */
const PERMIT_ROUTE_HARD_BLOCK_DEFINITIONS = Object.freeze([
  Object.freeze({
    key: "software_or_transport",
    kind: "software_transport",
    label: "Software / transport",
    statuses: Object.freeze(["adapter_unavailable", "egress_unavailable"]),
  }),
  Object.freeze({
    key: "login_required",
    kind: "source_policy",
    label: "Login required",
    statuses: Object.freeze(["login_required"]),
  }),
  Object.freeze({
    key: "no_anonymous_search",
    kind: "source_policy",
    label: "No anonymous search",
    statuses: Object.freeze(["no_anonymous_search"]),
  }),
  Object.freeze({
    key: "custodian_only",
    kind: "source_policy",
    label: "Custodian only",
    statuses: Object.freeze(["custodian_only"]),
  }),
]);

/**
 * Build the current-route implementation summary directly from the executable
 * jurisdiction registry. Only each jurisdiction's primary `current` route is
 * counted; historical and supplemental routes never enter this denominator.
 *
 * @returns {BrowardPermitRouteStatus} Reconciled public route status.
 */
export function buildBrowardPermitRouteStatus() {
  /** @type {string[]} */
  const implementedJurisdictions = [];
  /** @type {string[]} */
  const manualCaptchaJurisdictions = [];
  /** @type {Map<PermitRouteHardBlockKey, string[]>} */
  const hardBlockedJurisdictions = new Map(
    PERMIT_ROUTE_HARD_BLOCK_DEFINITIONS.map((definition) => [
      definition.key,
      [],
    ]),
  );
  const currentSourceKeys = new Set();
  for (const entry of BROWARD_PERMIT_JURISDICTIONS) {
    const route = entry.primarySource;
    if (route.coverageKind !== "current") {
      throw new Error(
        `Broward primary permit route is not current: ${entry.name}`,
      );
    }
    if (currentSourceKeys.has(route.sourceKey)) {
      throw new Error(
        `Duplicate Broward current permit route: ${route.sourceKey}`,
      );
    }
    currentSourceKeys.add(route.sourceKey);
    if (route.status === "implemented") {
      implementedJurisdictions.push(entry.name);
      continue;
    }
    if (route.status === "captcha_required") {
      manualCaptchaJurisdictions.push(entry.name);
      continue;
    }
    const definition = PERMIT_ROUTE_HARD_BLOCK_DEFINITIONS.find((candidate) =>
      candidate.statuses.includes(route.status),
    );
    if (definition === undefined) {
      throw new Error(
        `Unclassified Broward permit hard block: ${route.status}`,
      );
    }
    hardBlockedJurisdictions.get(definition.key)?.push(entry.name);
  }
  implementedJurisdictions.sort((left, right) => left.localeCompare(right));
  manualCaptchaJurisdictions.sort((left, right) => left.localeCompare(right));
  const hardBlockCategories = PERMIT_ROUTE_HARD_BLOCK_DEFINITIONS.map(
    (definition) => {
      const names = hardBlockedJurisdictions.get(definition.key) ?? [];
      names.sort((left, right) => left.localeCompare(right));
      return {
        key: /** @type {PermitRouteHardBlockKey} */ (definition.key),
        kind: /** @type {"software_transport" | "source_policy"} */ (
          definition.kind
        ),
        label: definition.label,
        count: names.length,
        jurisdictions: names,
      };
    },
  );
  const hardBlockedCurrentRoutes = hardBlockCategories.reduce(
    (sum, category) => sum + category.count,
    0,
  );
  const manualCaptchaCurrentRoutes = manualCaptchaJurisdictions.length;
  const unattendedUnavailableCurrentRoutes =
    manualCaptchaCurrentRoutes + hardBlockedCurrentRoutes;
  const totalCurrentRoutes = BROWARD_PERMIT_JURISDICTIONS.length;
  if (
    implementedJurisdictions.length +
      manualCaptchaCurrentRoutes +
      hardBlockedCurrentRoutes !==
      totalCurrentRoutes ||
    unattendedUnavailableCurrentRoutes !==
      manualCaptchaCurrentRoutes + hardBlockedCurrentRoutes
  ) {
    throw new Error("Broward permit route categories do not reconcile");
  }
  return {
    registryVersion: BROWARD_PERMIT_REGISTRY_VERSION,
    totalCurrentRoutes,
    implementedCurrentRoutes: implementedJurisdictions.length,
    manualCaptchaCurrentRoutes,
    hardBlockedCurrentRoutes,
    unattendedUnavailableCurrentRoutes,
    implementedJurisdictions,
    manualCaptchaJurisdictions,
    hardBlockCategories,
  };
}

/**
 * Build privacy-safe progress for the three manually authorized CAPTCHA
 * routes. Counts come only from the Coral checkpoint and durable
 * source-system rollups; route configuration contains no transient totals.
 *
 * @param {RecoveryAggregateRow} row - Durable source-system aggregates.
 * @param {RecoveryDashboardStatus["coralSpringsPermit"]} coralSpringsPermit
 *   Reconciled aggregate checkpoint and loaded counts.
 * @param {BrowardPermitRouteStatus} permitRoutes
 *   Executable-registry route classification.
 * @returns {ManualCaptchaProgress} Reconciled manual route progress.
 */
function buildManualCaptchaProgress(row, coralSpringsPermit, permitRoutes) {
  const pembrokeLoaded = count(row.pembroke_park_gov_easy_loaded);
  const hillsboroLoaded = count(row.hillsboro_beach_communitycore_loaded);
  const coralCaptured = coralSpringsPermit.unique;
  const coralLoaded = coralSpringsPermit.loaded;
  /** @type {ManualCaptchaRouteProgress[]} */
  const routes = [
    {
      jurisdiction: "Coral Springs",
      registryStatus: "captcha_required",
      progressState: coralSpringsPermit.captureComplete
        ? "bounded_slice_captured"
        : coralCaptured > 0
          ? "bounded_capture_in_progress"
          : coralLoaded > 0
            ? "bounded_slice_loaded"
            : "awaiting_manual_captcha",
      evidence:
        coralCaptured > 0
          ? "private_capture_checkpoint"
          : coralLoaded > 0
            ? "durable_loaded_aggregate"
            : "no_captured_aggregate",
      coverageBoundary: "bounded_capped_slice",
      capturedRecords: coralCaptured,
      loadedRecords: coralLoaded,
      manualSessionRequired: true,
      sessionsExpire: true,
      validSearchCaptchaRequired: true,
      countyComplete: false,
    },
    {
      jurisdiction: "Hillsboro Beach",
      registryStatus: "captcha_required",
      progressState:
        hillsboroLoaded > 0
          ? "bounded_slice_loaded"
          : "awaiting_manual_captcha",
      evidence:
        hillsboroLoaded > 0
          ? "durable_loaded_aggregate"
          : "no_captured_aggregate",
      coverageBoundary: hillsboroLoaded > 0 ? "bounded_slice" : "not_captured",
      capturedRecords: hillsboroLoaded,
      loadedRecords: hillsboroLoaded,
      manualSessionRequired: true,
      sessionsExpire: true,
      validSearchCaptchaRequired: true,
      countyComplete: false,
    },
    {
      jurisdiction: "Pembroke Park",
      registryStatus: "captcha_required",
      progressState:
        pembrokeLoaded > 0 ? "bounded_slice_loaded" : "awaiting_manual_captcha",
      evidence:
        pembrokeLoaded > 0
          ? "durable_loaded_aggregate"
          : "no_captured_aggregate",
      coverageBoundary: pembrokeLoaded > 0 ? "bounded_slice" : "not_captured",
      capturedRecords: pembrokeLoaded,
      loadedRecords: pembrokeLoaded,
      manualSessionRequired: true,
      sessionsExpire: true,
      validSearchCaptchaRequired: true,
      countyComplete: false,
    },
  ];
  routes.sort((left, right) =>
    left.jurisdiction.localeCompare(right.jurisdiction),
  );
  const expectedNames = permitRoutes.manualCaptchaJurisdictions;
  if (
    routes.length !== permitRoutes.manualCaptchaCurrentRoutes ||
    routes.some((route, index) => route.jurisdiction !== expectedNames[index])
  ) {
    throw new Error("Broward manual CAPTCHA routes do not reconcile");
  }
  return {
    sessionPolicy: "manual_captcha_sessions_expire",
    countyComplete: false,
    routes,
  };
}

/**
 * Build the aggregate-only permit status from control and optional pilot rows.
 *
 * @param {RecoveryAggregateRow} row - Combined recovery dashboard row.
 * @param {BrowardPermitRouteStatus} permitRoutes
 *   Current executable-registry route status.
 * @returns {RecoveryDashboardStatus["permit"]} Public permit status.
 */
function buildPermitStatus(row, permitRoutes) {
  const registryJurisdictions = permitRoutes.totalCurrentRoutes;
  const currentSourcesImplemented = permitRoutes.implementedCurrentRoutes;
  const currentSourcesManualCaptcha = permitRoutes.manualCaptchaCurrentRoutes;
  const currentSourcesHardBlocked = permitRoutes.hardBlockedCurrentRoutes;
  const currentSourcesUnattendedUnavailable =
    permitRoutes.unattendedUnavailableCurrentRoutes;
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
      currentSourcesManualCaptcha,
      currentSourcesHardBlocked,
      currentSourcesUnattendedUnavailable,
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
    (!row.permit_pilot_passed || currentSourcesUnattendedUnavailable !== 0)
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
    currentSourcesManualCaptcha,
    currentSourcesHardBlocked,
    currentSourcesUnattendedUnavailable,
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
    pausedWorkers: [],
    coolingWorkers: [],
    activeWorkers: 0,
    completedWorkers: 0,
    completedWindows: 0,
    totalWindows: 0,
    accessibleRecords: 0,
    excludedRecords: 0,
    invalidRecords: 0,
    sourceMissingRecords: 0,
    deferredCapCount: 0,
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
  const permitRoutes = buildBrowardPermitRouteStatus();
  /** @type {RecoveryDashboardStatus["coralSpringsPermit"]} */
  const coralSpringsPermit = {
    reported: 59_379,
    exposed: 1_000,
    paged: 0,
    unique: 0,
    details: 0,
    loaded: count(row.coral_etrakit_loaded),
    linked: count(row.coral_etrakit_linked),
    roofing: count(row.coral_etrakit_roofing),
    completedPages: 0,
    totalPages: 50,
    captureComplete: false,
    completenessBoundary: "bounded_capped_keyword_slice",
    captchaPrerequisite: "manual_authorization_required",
    registryStatus: "captcha_required",
  };
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
    permit: buildPermitStatus(row, permitRoutes),
    permitRoutes,
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
    coralSpringsPermit,
    manualCaptchaProgress: buildManualCaptchaProgress(
      row,
      coralSpringsPermit,
      permitRoutes,
    ),
    permitEnumeration: emptyPermitEnumerationStatus(),
    activePermitEnumeration: {
      generatedAt: new Date(nowMs).toISOString(),
      snapshotStale: false,
      observationWindowSeconds: 0,
      workers: [],
    },
    sunbizMatch: {
      matchedAddressRoles: count(row.sunbiz_match_roles),
      registrations: count(row.sunbiz_match_registrations),
      properties: count(row.sunbiz_match_properties),
      chunks: count(row.sunbiz_match_chunks),
    },
  };
}

const CORAL_ETRAKIT_CHECKPOINT_PATH =
  "downloads/broward/coral-springs-etrakit/roof-permit-type-capped-20260901/checkpoint.private.json";

/**
 * Read aggregate-only Coral Springs capture progress.
 *
 * Stable IDs, row digests, list values, and session material are never
 * returned. Missing private state is represented as an uncaptured capped
 * source, not as complete or anonymously accessible.
 *
 * @param {string} repositoryRoot - Repository containing ignored checkpoint.
 * @returns {Promise<Pick<
 *   RecoveryDashboardStatus["coralSpringsPermit"],
 *   "reported" | "exposed" | "paged" | "unique" | "details" |
 *   "completedPages" | "totalPages" | "captureComplete" |
 *   "completenessBoundary" | "captchaPrerequisite" | "registryStatus"
 * >>} Aggregate private-checkpoint projection.
 */
export async function readCoralSpringsEtrakitStatus(repositoryRoot) {
  try {
    const parsed = /** @type {unknown} */ (
      JSON.parse(
        await readFile(
          path.resolve(repositoryRoot, CORAL_ETRAKIT_CHECKPOINT_PATH),
          "utf8",
        ),
      )
    );
    if (!isPlainRecord(parsed) || !isPlainRecord(parsed.completedPages)) {
      throw new Error("Coral Springs eTRAKiT checkpoint is malformed");
    }
    const reported = safeAggregate(parsed.sourceReportedCount);
    const totalPages = safeAggregate(parsed.expectedPageCount);
    const pageSize = safeAggregate(parsed.expectedPageSize);
    const paged = safeAggregate(parsed.capturedRowCount);
    const unique = safeAggregate(parsed.uniqueRecordCount);
    const duplicate = safeAggregate(parsed.duplicateRecordCount);
    const conflicts = safeAggregate(parsed.conflictRecordCount);
    const completedPages = Object.keys(parsed.completedPages).length;
    if (
      reported !== 59_379 ||
      totalPages !== 50 ||
      pageSize !== 20 ||
      completedPages > totalPages ||
      paged !== unique + duplicate ||
      conflicts !== 0 ||
      typeof parsed.completed !== "boolean" ||
      (parsed.completed &&
        (completedPages !== totalPages || paged !== totalPages * pageSize))
    ) {
      throw new Error("Coral Springs eTRAKiT aggregates do not reconcile");
    }
    return {
      reported,
      exposed: totalPages * pageSize,
      paged,
      unique,
      details: 0,
      completedPages,
      totalPages,
      captureComplete: parsed.completed,
      completenessBoundary: "bounded_capped_keyword_slice",
      captchaPrerequisite: "manual_authorization_required",
      registryStatus: "captcha_required",
    };
  } catch (error) {
    if (isNodeError(error) && error.code === "ENOENT") {
      return {
        reported: 59_379,
        exposed: 1_000,
        paged: 0,
        unique: 0,
        details: 0,
        completedPages: 0,
        totalPages: 50,
        captureComplete: false,
        completenessBoundary: "bounded_capped_keyword_slice",
        captchaPrerequisite: "manual_authorization_required",
        registryStatus: "captcha_required",
      };
    }
    throw error;
  }
}

const PERMIT_ENUMERATION_CHECKPOINTS = Object.freeze([
  {
    source: "Hollywood",
    family: "accela_csv",
    pauseReason: "checkpoint_stale",
    gapRelativePath: null,
    relativePath:
      "downloads/broward/accela-csv-windows/hollywood-full/checkpoint.private.json",
  },
  {
    source: "Plantation",
    family: "accela_csv",
    pauseReason: "timeout",
    gapRelativePath:
      "downloads/broward/accela-csv-windows/plantation-full-v2/property-gap-fill/checkpoint.private.json",
    relativePath:
      "downloads/broward/accela-csv-windows/plantation-full-v2/checkpoint.private.json",
  },
  {
    source: "Cooper City",
    family: "accela_csv",
    pauseReason: "source_cap",
    gapRelativePath:
      "downloads/broward/accela-csv-windows/cooper-city-full/property-gap-fill/checkpoint.private.json",
    relativePath:
      "downloads/broward/accela-csv-windows/cooper-city-full/checkpoint.private.json",
  },
  {
    source: "Weston",
    family: "accela_csv",
    pauseReason: "source_cap",
    gapRelativePath:
      "downloads/broward/accela-csv-windows/weston-full/property-gap-fill/checkpoint.private.json",
    relativePath:
      "downloads/broward/accela-csv-windows/weston-full/checkpoint.private.json",
  },
  {
    source: "Pembroke Pines",
    family: "tyler_api",
    pauseReason: "checkpoint_stale",
    gapRelativePath: null,
    relativePath:
      "downloads/broward/tyler-date-windows/pembroke-pines-full-30d/checkpoint.private.json",
  },
  {
    source: "Hallandale Beach",
    family: "tyler_api",
    pauseReason: "timeout",
    gapRelativePath: null,
    relativePath:
      "downloads/broward/tyler-date-windows/hallandale-beach-full-30d/checkpoint.private.json",
  },
  {
    source: "Miramar",
    family: "tyler_api",
    pauseReason: "checkpoint_stale",
    gapRelativePath: null,
    relativePath:
      "downloads/broward/tyler-date-windows/miramar-full-2019/checkpoint.private.json",
  },
  {
    source: "Oakland Park",
    family: "tyler_api",
    pauseReason: "checkpoint_stale",
    gapRelativePath: null,
    relativePath:
      "downloads/broward/tyler-date-windows/oakland-park-full-30d/checkpoint.private.json",
  },
  {
    source: "Coconut Creek",
    family: "municipal_property",
    reader: "municipal_property",
    activeEnumeration: {
      key: "coconut-creek",
      method: "full",
      processJurisdictionKey: "coconut_creek",
    },
    pauseReason: "checkpoint_stale",
    gapRelativePath: null,
    coverageBoundary: "BCPA property-first folio seed",
    noStartReason: "awaiting_reconciled_property_seed",
    relativePath:
      "downloads/broward/municipal-property-enumeration/coconut-creek-full/checkpoint.private.json",
  },
  {
    source: "Dania Beach",
    family: "municipal_type",
    reader: "municipal_type",
    pauseReason: "checkpoint_stale",
    gapRelativePath: null,
    coverageBoundary: "Complete official eSuite exact-type option universe",
    noStartReason: "awaiting_exact_type_partition_pilot",
    relativePath:
      "downloads/broward/municipal-type-enumeration/dania-beach-full/checkpoint.private.json",
  },
  {
    source: "Davie",
    family: "municipal_type",
    reader: "municipal_type",
    pauseReason: "checkpoint_stale",
    gapRelativePath: null,
    coverageBoundary:
      "Legacy eSuite exact-type universe; login-gated 2026 OAS excluded",
    noStartReason: "awaiting_exact_type_partition_pilot",
    relativePath:
      "downloads/broward/municipal-type-enumeration/davie-full/checkpoint.private.json",
  },
  {
    source: "Lauderhill",
    family: "municipal_property",
    reader: "municipal_property",
    activeEnumeration: {
      key: "lauderhill",
      method: "full",
      processJurisdictionKey: "lauderhill",
    },
    pauseReason: "checkpoint_stale",
    gapRelativePath: null,
    coverageBoundary: "BCPA property-first folio seed",
    noStartReason: "awaiting_reconciled_property_seed",
    relativePath:
      "downloads/broward/municipal-property-enumeration/lauderhill-full/checkpoint.private.json",
  },
  {
    source: "Lighthouse Point",
    family: "municipal_type",
    reader: "municipal_type",
    activeEnumeration: {
      key: "lighthouse-point",
      method: "full",
      processJurisdictionKey: "lighthouse_point",
    },
    pauseReason: "checkpoint_stale",
    gapRelativePath: null,
    coverageBoundary: "Complete official SmartGov exact-type option universe",
    noStartReason: "positive_detail_reconciliation_required",
    relativePath:
      "downloads/broward/municipal-type-enumeration/lighthouse-point-full/checkpoint.private.json",
  },
  {
    source: "Margate",
    family: "municipal_property",
    reader: "municipal_property",
    activeEnumeration: {
      key: "margate",
      method: "full",
      processJurisdictionKey: "margate",
    },
    pauseReason: "checkpoint_stale",
    gapRelativePath: null,
    coverageBoundary:
      "Partial BCPA property-first seed: 15,059 represented properties; 1,450 private seed gaps",
    noStartReason: "representable_property_worker_not_started",
    relativePath:
      "downloads/broward/municipal-property-enumeration/margate-full/checkpoint.private.json",
  },
  {
    source: "Pompano Beach",
    family: "municipal_property",
    reader: "municipal_property",
    pauseReason: "source_cap",
    gapRelativePath: null,
    coverageBoundary:
      "Partial BCPA property-first seed: 23,900 address queries; 2,961 private seed gaps; exclusive client-all cap",
    noStartReason: "representable_property_worker_not_started",
    relativePath:
      "downloads/broward/municipal-property-enumeration/pompano-beach-full/checkpoint.private.json",
  },
  {
    source: "Sunrise",
    family: "tyler_api",
    pauseReason: "checkpoint_stale",
    gapRelativePath: null,
    coverageBoundary:
      "EnerGov electronic application dates 1900-01-01 through 2026-09-02; legacy custodian separate",
    noStartReason: "full_date_worker_not_started",
    relativePath:
      "downloads/broward/tyler-date-windows/sunrise-full-1900-present/checkpoint.private.json",
  },
  {
    source: "Tamarac",
    family: "municipal_property",
    reader: "municipal_property",
    activeEnumeration: {
      key: "tamarac",
      method: "full",
      processJurisdictionKey: "tamarac",
    },
    pauseReason: "checkpoint_stale",
    gapRelativePath: null,
    coverageBoundary:
      "Partial BCPA property-first seed: 19,800 represented properties; 1,378 private seed gaps",
    noStartReason: "representable_property_worker_not_started",
    relativePath:
      "downloads/broward/municipal-property-enumeration/tamarac-full/checkpoint.private.json",
  },
]);

const PROPERTY_FIRST_PERMIT_ROUTES = Object.freeze([
  {
    key: "unincorporated-broward",
    source: "BMSD / unincorporated",
    activeEnumeration: true,
    coverageBoundary: "BCS current-custody property-first records",
  },
  {
    key: "lauderdale-by-the-sea",
    source: "Lauderdale-by-the-Sea",
    activeEnumeration: true,
    coverageBoundary:
      "Current Citizenserve only; historical BCS evidence remains supplemental",
  },
  {
    key: "lazy-lake",
    source: "Lazy Lake",
    coverageBoundary: "BCS current-custody property-first records",
  },
  {
    key: "southwest-ranches",
    source: "Southwest Ranches",
    activeEnumeration: true,
    coverageBoundary:
      "Citizenserve building permits only; other Town approvals excluded",
  },
  {
    key: "west-park",
    source: "West Park",
    activeEnumeration: true,
    coverageBoundary: "Citizenserve public search; no complete-history claim",
  },
  {
    key: "wilton-manors",
    source: "Wilton Manors",
    activeEnumeration: true,
    coverageBoundary:
      "Citizenserve available files; unavailable files remain a custodian gap",
  },
]);

/**
 * Build the ten active-enumeration definitions from the same executable
 * checkpoint/query configurations used by the dashboard readers. Adapter
 * families come from the current permit registry; no transient counts or
 * process IDs are embedded here.
 *
 * @returns {readonly ActiveEnumerationRouteDefinition[]} Fixed active routes.
 */
function buildActiveEnumerationRouteDefinitions() {
  /** @type {ActiveEnumerationRouteDefinition[]} */
  const definitions = [];
  for (const candidate of PERMIT_ENUMERATION_CHECKPOINTS) {
    if (!("activeEnumeration" in candidate)) continue;
    const active = candidate.activeEnumeration;
    if (
      active === undefined ||
      active.method !== "full" ||
      (candidate.family !== "municipal_property" &&
        candidate.family !== "municipal_type")
    ) {
      throw new Error("Active municipal enumeration definition is invalid");
    }
    definitions.push({
      key: active.key,
      jurisdiction: candidate.source,
      method: "full",
      family: candidate.family,
      countSource: "local_checkpoint",
      processScript: "run-broward-municipal-enumeration-supervisor.mjs",
      processJurisdictionKey: active.processJurisdictionKey,
    });
  }
  for (const candidate of PROPERTY_FIRST_PERMIT_ROUTES) {
    if (!("activeEnumeration" in candidate)) continue;
    const registryRoute = BROWARD_PERMIT_JURISDICTIONS.find(
      (route) => route.key === candidate.key,
    );
    const adapterKey = registryRoute?.primarySource.adapterKey;
    const family =
      adapterKey === BROWARD_BCS_ADAPTER_KEY
        ? "bcs_posse"
        : adapterKey === BROWARD_CITIZENSERVE_ADAPTER_KEY
          ? "citizenserve"
          : null;
    if (family === null) {
      throw new Error("Active property-first adapter family is invalid");
    }
    definitions.push({
      key: candidate.key,
      jurisdiction: candidate.source,
      method: "property_first",
      family,
      countSource: "durable_route_checkpoint",
      processScript: "run-broward-supported-permit-ingest.mjs",
      processJurisdictionKey: candidate.key,
    });
  }
  if (definitions.length !== 10) {
    throw new Error("Active permit enumeration definitions are incomplete");
  }
  return Object.freeze(definitions);
}

const ACTIVE_ENUMERATION_ROUTE_DEFINITIONS =
  buildActiveEnumerationRouteDefinitions();

/**
 * Resolve one municipal dashboard definition to the executable jurisdiction
 * configuration used by the runner. This avoids duplicating process argument
 * keys in dashboard-only state.
 *
 * @param {Record<string, unknown>} definition - Municipal dashboard definition.
 * @returns {ActiveEnumerationRouteDefinition} Aggregate process definition.
 */
function buildMunicipalProcessDefinition(definition) {
  const source = definition.source;
  const family = definition.family;
  if (
    typeof source !== "string" ||
    (family !== "municipal_property" && family !== "municipal_type")
  ) {
    throw new Error("Municipal process definition is malformed");
  }
  const executable = BROWARD_MUNICIPAL_PERMIT_JURISDICTIONS.find(
    (candidate) => candidate.jurisdiction === source,
  );
  if (executable === undefined) {
    throw new Error("Municipal process configuration is missing");
  }
  const active =
    isPlainRecord(definition.activeEnumeration) &&
    typeof definition.activeEnumeration.key === "string"
      ? definition.activeEnumeration
      : null;
  return {
    key: active?.key ?? `municipal-${executable.key}`,
    jurisdiction: source,
    method: "full",
    family,
    countSource: "local_checkpoint",
    processScript: "run-broward-municipal-enumeration-supervisor.mjs",
    processJurisdictionKey: executable.key,
  };
}

/**
 * Build process definitions for every local municipal checkpoint plus the
 * active property-first parents. The active tracker still projects only its
 * fixed ten routes; the broader snapshot lets the inventory report scheduled
 * operator holds such as Pompano Beach.
 *
 * @returns {readonly ActiveEnumerationRouteDefinition[]} Process routes.
 */
function buildPermitProcessRouteDefinitions() {
  const municipal = PERMIT_ENUMERATION_CHECKPOINTS.filter(
    (definition) =>
      "reader" in definition &&
      (definition.reader === "municipal_property" ||
        definition.reader === "municipal_type"),
  ).map((definition) =>
    buildMunicipalProcessDefinition(
      /** @type {Record<string, unknown>} */ (definition),
    ),
  );
  const propertyFirst = ACTIVE_ENUMERATION_ROUTE_DEFINITIONS.filter(
    (definition) => definition.method === "property_first",
  );
  const routes = [...municipal, ...propertyFirst];
  if (
    new Set(routes.map((route) => route.key)).size !== routes.length ||
    routes.length !== municipal.length + 5
  ) {
    throw new Error("Permit process route definitions do not reconcile");
  }
  return Object.freeze(routes);
}

const PERMIT_PROCESS_ROUTE_DEFINITIONS =
  buildPermitProcessRouteDefinitions();

/** @type {ActiveEnumerationProcessSnapshot} */
const UNAVAILABLE_PROCESS_SNAPSHOT = Object.freeze({
  available: false,
  routeKeys: new Set(),
  detailRouteKeys: new Set(),
  supervisorNotBeforeByKey: new Map(),
});

/**
 * Read only the public-safe fields from a durable Accela circuit breaker.
 *
 * @param {unknown} value - Optional checkpoint cooldown.
 * @param {number} nowMs - Dashboard snapshot epoch.
 * @returns {{reason:"timeout" | "source_cap" | "incomplete_pagination" | "source_error",nextAttemptAt:string} | null}
 *   Active future cooldown, or null when absent/expired.
 */
function readPermitEnumerationCooldown(value, nowMs) {
  if (value === undefined || value === null) return null;
  if (typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Permit enumeration cooldown is malformed");
  }
  const cooldown = /** @type {Record<string, unknown>} */ (value);
  const allowedReasons = new Set([
    "timeout",
    "source_cap",
    "incomplete_pagination",
    "source_error",
  ]);
  if (
    typeof cooldown.reason !== "string" ||
    !allowedReasons.has(cooldown.reason) ||
    typeof cooldown.nextAttemptAt !== "string"
  ) {
    throw new Error("Permit enumeration cooldown is malformed");
  }
  const nextAttemptMs = Date.parse(cooldown.nextAttemptAt);
  if (!Number.isFinite(nextAttemptMs)) {
    throw new Error("Permit enumeration cooldown timestamp is invalid");
  }
  if (nextAttemptMs <= nowMs) return null;
  return {
    reason:
      /** @type {"timeout" | "source_cap" | "incomplete_pagination" | "source_error"} */ (
        cooldown.reason
      ),
    nextAttemptAt: cooldown.nextAttemptAt,
  };
}

/**
 * Read aggregate-only property gap-fill activity. Property evidence can make a
 * capped source operationally active or cooling, but it never completes the
 * parent date window.
 *
 * @param {string} repositoryRoot - Repository root containing private state.
 * @param {string | null} relativePath - Optional gap checkpoint path.
 * @param {number} nowMs - Dashboard snapshot epoch.
 * @returns {Promise<{
 *   activePlan:boolean,
 *   recentlyActive:boolean,
 *   retainedRecordCount:number,
 *   updatedAt:string,
 *   updatedMs:number,
 *   cooldown:{reason:"timeout" | "source_cap" | "incomplete_pagination" | "source_error",nextAttemptAt:string} | null
 * } | null>} Public-safe partial strategy activity, when present.
 */
async function readPermitGapFillActivity(repositoryRoot, relativePath, nowMs) {
  if (relativePath === null) return null;
  try {
    const parsed = /** @type {unknown} */ (
      JSON.parse(
        await readFile(path.resolve(repositoryRoot, relativePath), "utf8"),
      )
    );
    if (
      parsed === null ||
      typeof parsed !== "object" ||
      Array.isArray(parsed)
    ) {
      throw new Error("Permit property gap-fill checkpoint is not an object");
    }
    const checkpoint = /** @type {Record<string, unknown>} */ (parsed);
    if (
      !isPlainRecord(checkpoint.plans) ||
      typeof checkpoint.updatedAt !== "string"
    ) {
      throw new Error("Permit property gap-fill checkpoint is malformed");
    }
    let retainedRecordCount = 0;
    let activePlan = false;
    for (const value of Object.values(checkpoint.plans)) {
      if (!isPlainRecord(value) || typeof value.seedExhausted !== "boolean") {
        throw new Error("Permit property gap-fill plan is malformed");
      }
      retainedRecordCount += safeAggregate(value.retainedRecordCount);
      if (!value.seedExhausted) activePlan = true;
    }
    const updatedMs = Date.parse(checkpoint.updatedAt);
    if (!Number.isFinite(updatedMs)) {
      throw new Error("Permit property gap-fill timestamp is invalid");
    }
    const recentlyActive =
      activePlan && nowMs - updatedMs >= 0 && nowMs - updatedMs <= 5 * 60_000;
    return {
      activePlan,
      recentlyActive,
      retainedRecordCount,
      updatedAt: checkpoint.updatedAt,
      updatedMs,
      cooldown: activePlan
        ? readPermitEnumerationCooldown(checkpoint.cooldown, nowMs)
        : null,
    };
  } catch (error) {
    if (isNodeError(error) && error.code === "ENOENT") return null;
    throw error;
  }
}

/**
 * Narrow an unknown parsed JSON value to a non-array record.
 *
 * @param {unknown} value - Candidate JSON value.
 * @returns {value is Record<string, unknown>} True for plain object-shaped data.
 */
function isPlainRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * Project one local municipal type/property checkpoint to aggregate-only
 * dashboard state. Private option IDs, property queries, record identities,
 * artifact paths, and source rows are never returned.
 *
 * @param {Record<string, unknown>} definition - Fixed public worker definition.
 * @param {Record<string, unknown>} checkpoint - Parsed private checkpoint.
 * @param {number} nowMs - Dashboard snapshot epoch.
 * @param {ActiveEnumerationProcessSnapshot} processes
 *   Bounded aggregate process evidence.
 * @returns {PermitEnumerationWorkerStatus} Public-safe municipal worker row.
 */
function buildMunicipalEnumerationWorker(
  definition,
  checkpoint,
  nowMs,
  processes,
) {
  const reader = definition.reader;
  const family = definition.family;
  const source = definition.source;
  const coverageBoundary = definition.coverageBoundary;
  if (
    (reader !== "municipal_type" && reader !== "municipal_property") ||
    (family !== "municipal_type" && family !== "municipal_property") ||
    typeof source !== "string" ||
    typeof coverageBoundary !== "string" ||
    typeof checkpoint.updatedAt !== "string" ||
    typeof checkpoint.status !== "string"
  ) {
    throw new Error("Municipal permit worker definition is malformed");
  }
  let completedWindows;
  let totalWindows;
  let deferredCapCount = 0;
  if (reader === "municipal_type") {
    if (
      !Array.isArray(checkpoint.pendingPartitionValues) ||
      !isPlainRecord(checkpoint.completedPartitions)
    ) {
      throw new Error("Municipal type checkpoint is malformed");
    }
    const cappedPartitionValues = checkpoint.cappedPartitionValues;
    if (
      cappedPartitionValues !== undefined &&
      (!Array.isArray(cappedPartitionValues) ||
        !cappedPartitionValues.every((value) => typeof value === "string") ||
        new Set(cappedPartitionValues).size !== cappedPartitionValues.length)
    ) {
      throw new Error("Municipal type source caps are malformed");
    }
    const cappedValues = new Set(
      cappedPartitionValues === undefined ? [] : cappedPartitionValues,
    );
    const completedValues = Object.keys(checkpoint.completedPartitions);
    const cappedCompletedCount = completedValues.filter((value) =>
      cappedValues.has(value),
    ).length;
    completedWindows = completedValues.length - cappedCompletedCount;
    const sourcePartitionCount = safeAggregate(checkpoint.sourcePartitionCount);
    totalWindows = sourcePartitionCount;
    deferredCapCount = cappedValues.size;
    if (
      sourcePartitionCount !==
      completedWindows +
        checkpoint.pendingPartitionValues.length +
        cappedCompletedCount
    ) {
      throw new Error("Municipal type partition counts do not reconcile");
    }
  } else {
    completedWindows = safeAggregate(checkpoint.completedQueries);
    totalWindows = safeAggregate(checkpoint.totalQueries);
    const deferredCapItems = checkpoint.deferredCapItems;
    if (deferredCapItems !== undefined && !isPlainRecord(deferredCapItems)) {
      throw new Error("Municipal property deferrals are malformed");
    }
    deferredCapCount =
      deferredCapItems === undefined ? 0 : Object.keys(deferredCapItems).length;
    if (completedWindows > totalWindows) {
      throw new Error("Municipal property query counts do not reconcile");
    }
    const nextQueryIndex = safeAggregate(checkpoint.nextQueryIndex);
    if (
      completedWindows + deferredCapCount !== nextQueryIndex ||
      nextQueryIndex > totalWindows
    ) {
      throw new Error("Municipal property processed counts do not reconcile");
    }
  }
  const pendingWindows = totalWindows - completedWindows;
  const updatedMs = Date.parse(checkpoint.updatedAt);
  if (!Number.isFinite(updatedMs)) {
    throw new Error("Municipal permit worker timestamp is invalid");
  }
  const blocker =
    typeof checkpoint.blocker === "string" ? checkpoint.blocker : null;
  const allowedBlockers = new Set([
    "source_cap",
    "timeout",
    "incomplete_pagination",
    "source_error",
  ]);
  if (blocker !== null && !allowedBlockers.has(blocker)) {
    throw new Error("Municipal permit worker blocker is invalid");
  }
  const nextAttemptAt =
    typeof checkpoint.nextAttemptAt === "string"
      ? checkpoint.nextAttemptAt
      : null;
  const nextAttemptMs =
    nextAttemptAt === null ? Number.NaN : Date.parse(nextAttemptAt);
  if (nextAttemptAt !== null && !Number.isFinite(nextAttemptMs)) {
    throw new Error("Municipal permit retry timestamp is invalid");
  }
  const processDefinition = buildMunicipalProcessDefinition(definition);
  const processAlive = processes.available
    ? processes.routeKeys.has(processDefinition.key)
    : null;
  const detailActive = processes.available
    ? processes.detailRouteKeys.has(processDefinition.key)
    : null;
  const operatorNotBeforeAt =
    processes.supervisorNotBeforeByKey.get(processDefinition.key) ?? null;
  const operatorNotBeforeMs =
    operatorNotBeforeAt === null
      ? Number.NaN
      : Date.parse(operatorNotBeforeAt);
  if (
    operatorNotBeforeAt !== null &&
    !Number.isFinite(operatorNotBeforeMs)
  ) {
    throw new Error("Municipal operator boundary is invalid");
  }
  const complete = checkpoint.status === "complete" && pendingWindows === 0;
  const checkpointCooling =
    checkpoint.status === "cooling" &&
    Number.isFinite(nextAttemptMs) &&
    nextAttemptMs > nowMs;
  const operatorCooling =
    Number.isFinite(operatorNotBeforeMs) && operatorNotBeforeMs > nowMs;
  const operatorControlsCooldown =
    operatorCooling &&
    (!checkpointCooling || operatorNotBeforeMs >= nextAttemptMs);
  const cooling =
    processAlive === true && (checkpointCooling || operatorCooling);
  const recentlyActive =
    checkpoint.status === "running" &&
    nowMs - updatedMs >= 0 &&
    nowMs - updatedMs <= 5 * 60_000;
  const running =
    (processAlive === true && (recentlyActive || detailActive === true)) ||
    (processAlive === null && recentlyActive);
  const status = complete
    ? /** @type {"complete"} */ ("complete")
    : cooling
      ? /** @type {"cooling_down"} */ ("cooling_down")
      : running
        ? /** @type {"running"} */ ("running")
        : /** @type {"paused"} */ ("paused");
  const effectiveNextAttemptAt =
    cooling && operatorControlsCooldown
      ? operatorNotBeforeAt
      : cooling && checkpointCooling
        ? nextAttemptAt
        : null;
  return {
    source,
    family,
    status,
    completedWindows,
    pendingWindows,
    totalWindows,
    completionPercent:
      totalWindows === 0
        ? 0
        : Math.round((completedWindows / totalWindows) * 100_000) / 1_000,
    accessibleRecords: safeAggregate(checkpoint.uniqueRecords),
    excludedRecords: 0,
    invalidRecords: 0,
    sourceMissingRecords: 0,
    deferredCapCount,
    updatedAt: checkpoint.updatedAt,
    pauseReason:
      status === "paused"
        ? (checkpointCooling || operatorCooling) && processAlive === false
          ? "supervisor_not_running"
          : (checkpointCooling || operatorCooling) && processAlive === null
            ? "process_unknown"
            : blocker === "source_cap"
              ? "source_cap"
              : blocker === "incomplete_pagination"
                ? "incomplete_pagination"
                : "checkpoint_stale"
        : null,
    cooldownReason:
      status === "cooling_down"
        ? operatorControlsCooldown
          ? "operator_hold"
          : /** @type {"timeout" | "source_cap" | "incomplete_pagination" | "source_error"} */ (
              blocker
            )
        : null,
    nextAttemptAt: effectiveNextAttemptAt,
    coverageBoundary,
    startBlocker: null,
    processAlive,
    detailActive,
    operatorNotBeforeAt,
  };
}

/**
 * Read aggregate-only local permit enumerator checkpoints.
 *
 * @param {string} repositoryRoot - Repository root containing downloads.
 * @param {number} [nowMs=Date.now()] - Snapshot time.
 * @param {ActiveEnumerationProcessSnapshot} [processes=UNAVAILABLE_PROCESS_SNAPSHOT]
 *   Bounded process evidence.
 * @returns {Promise<PermitEnumerationStatus>} PII-free worker status.
 */
export async function readPermitEnumerationStatus(
  repositoryRoot,
  nowMs = Date.now(),
  processes = UNAVAILABLE_PROCESS_SNAPSHOT,
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
          "reader" in definition &&
          (definition.reader === "municipal_type" ||
            definition.reader === "municipal_property")
        ) {
          return buildMunicipalEnumerationWorker(
            /** @type {Record<string, unknown>} */ (definition),
            checkpoint,
            nowMs,
            processes,
          );
        }
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
            accessibleRecords += readAccelaCsvReceiptAccessibleCount(receipt);
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
        const parentUpdatedAt = checkpoint.updatedAt;
        const parentUpdatedMs = Date.parse(parentUpdatedAt);
        const complete = pendingWindows === 0;
        const parentCooldown = readPermitEnumerationCooldown(
          checkpoint.cooldown,
          nowMs,
        );
        const gapFill = await readPermitGapFillActivity(
          repositoryRoot,
          definition.gapRelativePath,
          nowMs,
        );
        accessibleRecords += gapFill?.retainedRecordCount ?? 0;
        const gapFillOwnsActivity = gapFill?.activePlan === true;
        const cooldown = gapFillOwnsActivity
          ? (gapFill.cooldown ?? parentCooldown)
          : parentCooldown;
        const recentlyActive = gapFillOwnsActivity
          ? gapFill.recentlyActive
          : Number.isFinite(parentUpdatedMs) &&
            nowMs - parentUpdatedMs >= 0 &&
            nowMs - parentUpdatedMs <= 5 * 60_000;
        const updatedAt =
          gapFill !== null && gapFill.updatedMs > parentUpdatedMs
            ? gapFill.updatedAt
            : parentUpdatedAt;
        const status = complete
          ? /** @type {"complete"} */ ("complete")
          : cooldown !== null
            ? /** @type {"cooling_down"} */ ("cooling_down")
            : recentlyActive
              ? /** @type {"running"} */ ("running")
              : /** @type {"paused"} */ ("paused");
        return {
          source: definition.source,
          family: /** @type {"accela_csv" | "tyler_api"} */ (definition.family),
          status,
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
          deferredCapCount: 0,
          updatedAt,
          pauseReason:
            status === "paused"
              ? /** @type {"timeout" | "missing_controls" | "missing_export" | "source_cap" | "checkpoint_stale"} */ (
                  definition.pauseReason
                )
              : null,
          cooldownReason: cooldown?.reason ?? null,
          nextAttemptAt: cooldown?.nextAttemptAt ?? null,
          coverageBoundary:
            "coverageBoundary" in definition &&
            typeof definition.coverageBoundary === "string"
              ? definition.coverageBoundary
              : null,
          startBlocker: null,
        };
      } catch (error) {
        if (isNodeError(error) && error.code === "ENOENT") {
          return {
            source: definition.source,
            family:
              /** @type {"accela_csv" | "tyler_api" | "municipal_type" | "municipal_property"} */ (
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
            deferredCapCount: 0,
            updatedAt: null,
            pauseReason: null,
            cooldownReason: null,
            nextAttemptAt: null,
            coverageBoundary:
              "coverageBoundary" in definition &&
              typeof definition.coverageBoundary === "string"
                ? definition.coverageBoundary
                : null,
            startBlocker:
              "noStartReason" in definition &&
              typeof definition.noStartReason === "string"
                ? definition.noStartReason
                : "worker_not_started",
          };
        }
        throw error;
      }
    }),
  );
  return summarizePermitWorkers(workers);
}

/**
 * Read the latest current-registry property-first aggregate for six gap routes.
 *
 * @param {import("pg").Client | import("pg").PoolClient} client
 *   Identity-verified Broward Neon client.
 * @param {number} [nowMs=Date.now()] - Snapshot time.
 * @returns {Promise<PermitEnumerationStatus>} Six privacy-safe route rows.
 */
export async function readPropertyFirstPermitStatus(
  client,
  nowMs = Date.now(),
) {
  /** @type {readonly Record<string, unknown>[]} */
  let rows;
  try {
    const result = await client.query(
      `WITH ranked AS (
         SELECT route.jurisdiction_key,route.candidate_count,
                route.terminal_count,route.record_count,
                route.terminal_missing_count,route.phase,
                route.next_attempt_at::text,route.heartbeat_at::text,
                row_number() OVER (
                  PARTITION BY route.jurisdiction_key
                  ORDER BY run.started_at DESC
                ) AS position
         FROM ${CONTROL_SCHEMA}.broward_supported_permit_routes AS route
         JOIN ${CONTROL_SCHEMA}.broward_supported_permit_runs AS run
           ON run.job_id=route.job_id
         WHERE run.registry_version=$1
           AND route.jurisdiction_key=ANY($2::text[])
       )
       SELECT * FROM ranked WHERE position=1`,
      [
        BROWARD_PERMIT_REGISTRY_VERSION,
        PROPERTY_FIRST_PERMIT_ROUTES.map((route) => route.key),
      ],
    );
    rows = result.rows;
  } catch (error) {
    if (!isNodeError(error) || error.code !== "42P01") throw error;
    rows = [];
  }
  const byKey = new Map(
    rows.map((row) => [
      typeof row.jurisdiction_key === "string" ? row.jurisdiction_key : "",
      row,
    ]),
  );
  const workers = PROPERTY_FIRST_PERMIT_ROUTES.map((definition) => {
    const row = byKey.get(definition.key);
    if (row === undefined) {
      return {
        source: definition.source,
        family: /** @type {"property_first"} */ ("property_first"),
        status: /** @type {"not_started"} */ ("not_started"),
        completedWindows: 0,
        pendingWindows: 0,
        totalWindows: 0,
        completionPercent: 0,
        accessibleRecords: 0,
        excludedRecords: 0,
        invalidRecords: 0,
        sourceMissingRecords: 0,
        deferredCapCount: 0,
        updatedAt: null,
        pauseReason: null,
        cooldownReason: null,
        nextAttemptAt: null,
        coverageBoundary: definition.coverageBoundary,
      };
    }
    const candidateCount = safeAggregate(row.candidate_count);
    const terminalCount = safeAggregate(row.terminal_count);
    if (terminalCount > candidateCount) {
      throw new Error("Property-first permit counts do not reconcile");
    }
    const heartbeatAt =
      typeof row.heartbeat_at === "string" ? row.heartbeat_at : null;
    const heartbeatMs =
      heartbeatAt === null ? Number.NaN : Date.parse(heartbeatAt);
    const nextAttemptAt =
      typeof row.next_attempt_at === "string" ? row.next_attempt_at : null;
    const nextAttemptMs =
      nextAttemptAt === null ? Number.NaN : Date.parse(nextAttemptAt);
    const recentlyActive =
      row.phase === "running" &&
      Number.isFinite(heartbeatMs) &&
      nowMs - heartbeatMs >= 0 &&
      nowMs - heartbeatMs <= 20 * 60_000;
    const cooling =
      row.phase === "cooling" &&
      Number.isFinite(nextAttemptMs) &&
      nextAttemptMs > nowMs;
    const complete =
      row.phase === "complete" ||
      (candidateCount > 0 && terminalCount === candidateCount);
    const status =
      complete
        ? /** @type {"complete"} */ ("complete")
        : recentlyActive
          ? /** @type {"running"} */ ("running")
          : cooling
            ? /** @type {"cooling_down"} */ ("cooling_down")
            : /** @type {"paused"} */ ("paused");
    return {
      source: definition.source,
      family: /** @type {"property_first"} */ ("property_first"),
      status,
      completedWindows: terminalCount,
      pendingWindows: Math.max(0, candidateCount - terminalCount),
      totalWindows: candidateCount,
      completionPercent:
        candidateCount === 0
          ? 0
          : Math.round((terminalCount / candidateCount) * 100_000) / 1_000,
      accessibleRecords: safeAggregate(row.record_count),
      excludedRecords: 0,
      invalidRecords: 0,
      sourceMissingRecords: safeAggregate(row.terminal_missing_count),
      deferredCapCount: 0,
      updatedAt: heartbeatAt,
      pauseReason:
        status === "paused"
          ? /** @type {"checkpoint_stale"} */ ("checkpoint_stale")
          : null,
      cooldownReason:
        status === "cooling_down"
          ? /** @type {"source_error"} */ ("source_error")
          : null,
      nextAttemptAt: status === "cooling_down" ? nextAttemptAt : null,
      coverageBoundary: definition.coverageBoundary,
    };
  });
  return summarizePermitWorkers(workers);
}

/**
 * Merge local date-window workers and Neon property-first route aggregates.
 *
 * @param {PermitEnumerationStatus} local - Existing Accela/Tyler workers.
 * @param {PermitEnumerationStatus} propertyFirst - Six scoped route workers.
 * @returns {PermitEnumerationStatus} Recomputed aggregate status.
 */
export function mergePermitEnumerationStatus(local, propertyFirst) {
  return summarizePermitWorkers([...local.workers, ...propertyFirst.workers]);
}

/**
 * Recompute public aggregate counters and allowlisted operational states.
 *
 * @param {PermitEnumerationWorkerStatus[]} workers - Public-safe worker rows.
 * @returns {PermitEnumerationStatus} Aggregate worker status.
 */
function summarizePermitWorkers(workers) {
  /** @type {PausedPermitEnumerationWorker[]} */
  const pausedWorkers = [];
  /** @type {CoolingPermitEnumerationWorker[]} */
  const coolingWorkers = [];
  for (const worker of workers) {
    if (worker.status === "paused" && worker.pauseReason !== null) {
      pausedWorkers.push({
        source: worker.source,
        reason: worker.pauseReason,
      });
    }
    if (
      worker.status === "cooling_down" &&
      worker.cooldownReason !== null &&
      worker.nextAttemptAt !== null
    ) {
      coolingWorkers.push({
        source: worker.source,
        reason: worker.cooldownReason,
        nextAttemptAt: worker.nextAttemptAt,
        ...(worker.processAlive === undefined
          ? {}
          : { processAlive: worker.processAlive }),
        ...(worker.detailActive === undefined
          ? {}
          : { detailActive: worker.detailActive }),
        ...(worker.operatorNotBeforeAt === undefined
          ? {}
          : { operatorNotBeforeAt: worker.operatorNotBeforeAt }),
      });
    }
  }
  return {
    workers,
    pausedWorkers,
    coolingWorkers,
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
    deferredCapCount: workers.reduce(
      (sum, worker) => sum + worker.deferredCapCount,
      0,
    ),
  };
}

/**
 * Read either legacy CSV, canonical v2, or transitional list-only record
 * counts without silently preferring a conflicting field. This keeps existing
 * completed receipts immutable while allowing Weston list-only receipts to
 * report their reconciled records.
 *
 * @param {Record<string, unknown>} receipt - One completed Accela receipt.
 * @returns {number} Reconciled accessible records.
 */
export function readAccelaCsvReceiptAccessibleCount(receipt) {
  const candidates = [
    receipt.recordCount,
    receipt.exportedRecordCount,
    receipt.listRecordCount,
  ].filter((value) => value !== undefined && value !== null);
  if (candidates.length === 0) return 0;
  const counts = candidates.map((value) => safeAggregate(value));
  if (new Set(counts).size !== 1) {
    throw new Error("Accela CSV receipt record counts conflict");
  }
  const first = counts[0];
  if (first === undefined) {
    throw new Error("Accela CSV receipt count disappeared");
  }
  return first;
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
 * @param {ActiveEnumerationProcessSnapshot} [processes=UNAVAILABLE_PROCESS_SNAPSHOT]
 *   One bounded process snapshot shared by inventory and active projections.
 * @returns {() => Promise<RecoveryDashboardStatus>} Async snapshot function.
 */
export function createRecoveryStatusReader(
  client,
  repositoryRoot = process.cwd(),
  processes = UNAVAILABLE_PROCESS_SNAPSHOT,
) {
  /** @type {Promise<RecoveryDashboardStatus> | null} */
  let inFlight = null;
  return () => {
    if (inFlight !== null) return inFlight;
    inFlight = (async () => {
      const [
        result,
        permitEnumeration,
        propertyFirstPermitEnumeration,
        coralCapture,
      ] = await Promise.all([
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
           permit_last_loaded_at::text AS permit_inventory_loaded_at,
           coalesce(coral_etrakit_records,0)::bigint
             AS coral_etrakit_loaded,
           coalesce(coral_etrakit_matched,0)::bigint
             AS coral_etrakit_linked,
           coalesce(coral_etrakit_roofing,0)::bigint
             AS coral_etrakit_roofing,
           coalesce(pembroke_park_gov_easy_records,0)::bigint
             AS pembroke_park_gov_easy_loaded,
           coalesce(hillsboro_beach_communitycore_records,0)::bigint
             AS hillsboro_beach_communitycore_loaded
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
        readPermitEnumerationStatus(repositoryRoot, Date.now(), processes),
        readPropertyFirstPermitStatus(client),
        readCoralSpringsEtrakitStatus(repositoryRoot),
      ]);
      const row = result.rows[0];
      if (row === undefined) {
        throw new Error("Neon returned no Broward recovery aggregate");
      }
      const status = buildRecoveryStatus(
        /** @type {RecoveryAggregateRow} */ (row),
        Date.now(),
      );
      status.permitEnumeration = mergePermitEnumerationStatus(
        permitEnumeration,
        propertyFirstPermitEnumeration,
      );
      status.coralSpringsPermit = {
        ...status.coralSpringsPermit,
        ...coralCapture,
      };
      status.manualCaptchaProgress = buildManualCaptchaProgress(
        /** @type {RecoveryAggregateRow} */ (row),
        status.coralSpringsPermit,
        status.permitRoutes,
      );
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
  const observeActiveEnumeration = createActivePermitEnumerationTracker(
    ACTIVE_ENUMERATION_ROUTE_DEFINITIONS,
  );

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
        const [, processSnapshot] = await Promise.all([
          verifyIdentity(client, options),
          readActiveEnumerationProcessSnapshot(
            PERMIT_PROCESS_ROUTE_DEFINITIONS,
          ),
        ]);
        const status = await withDashboardTimeout(
          createRecoveryStatusReader(
            client,
            repositoryRoot,
            processSnapshot,
          )(),
          timeoutMs,
        );
        const completedAtMs = Date.now();
        status.activePermitEnumeration = observeActiveEnumeration(
          status.permitEnumeration.workers,
          processSnapshot,
          completedAtMs,
        );
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
          const fallbackAtMs = Date.now();
          fallback.generatedAt = new Date(fallbackAtMs).toISOString();
          fallback.activePermitEnumeration =
            markActivePermitEnumerationSnapshotStale(
              fallback.activePermitEnumeration,
              fallbackAtMs,
            );
          fallback.dashboardHealth = {
            stale: true,
            lastSuccessfulAt: new Date(lastSuccessfulAtMs).toISOString(),
            snapshotAgeSeconds: Math.max(
              0,
              Math.round((fallbackAtMs - lastSuccessfulAtMs) / 1_000),
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
    .table-scroll { overflow-x: auto; }
    .active-enumeration td { white-space: nowrap; }
    .active-enumeration td:first-child, .active-enumeration td:nth-child(8), .active-enumeration td:nth-child(10) { white-space: normal; }
    .route-groups { display: grid; grid-template-columns: repeat(auto-fit, minmax(15rem, 1fr)); gap: .75rem; }
    .route-group { padding: .8rem; border: 1px solid #29415a; border-radius: .6rem; background: #0b1929; }
    .route-group h3 { margin: 0 0 .4rem; font-size: .95rem; }
    .route-group p, .route-note { color: #a9bed2; }
    .route-group p { margin: 0; }
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
    <article><h2>Completed work units</h2><strong id="permit-windows">—</strong></article>
  </section>
  <h2>Active permit enumeration</h2>
  <p class="route-note">Dedicated current view for five full municipal enumerators and five property-first routes. Process presence and checkpoint movement are independent; this section excludes route implementation/access blockers and the original eight Accela/Tyler date-window workers.</p>
  <div class="table-scroll">
    <table class="active-enumeration" aria-label="Active permit enumeration status">
      <thead><tr><th>Jurisdiction</th><th>Method / family</th><th>State</th><th>Process / movement</th><th>Work units</th><th>Local / Neon records</th><th>Deferred / missing</th><th>Checkpoint</th><th>Recent throughput</th><th>ETA</th></tr></thead>
      <tbody id="active-permit-worker-rows"><tr><td colspan="10">Loading active enumeration…</td></tr></tbody>
    </table>
  </div>
  <h2>Coral Springs eTRAKiT capped slice</h2>
  <p class="route-note">Manual CAPTCHA authorization is still required and sessions expire. Captured rows are a bounded capped slice of the reported source matches, not complete jurisdiction coverage.</p>
  <section class="grid">
    <article><h2>Reported / exposed</h2><strong id="coral-reported">—</strong></article>
    <article><h2>Paged / unique</h2><strong id="coral-captured">—</strong></article>
    <article><h2>Loaded / linked</h2><strong id="coral-loaded">—</strong></article>
    <article><h2>Loaded roofing</h2><strong id="coral-roofing">—</strong></article>
    <article><h2>Capture pages</h2><strong id="coral-pages">—</strong></article>
    <article><h2>Coverage</h2><strong id="coral-coverage">—</strong></article>
  </section>
  <h2>All permit enumeration inventory</h2>
  <p class="route-note">Preserved combined inventory, including the original eight Accela/Tyler date-window workers and other implemented municipal routes.</p>
  <table aria-label="Permit tenant worker status">
    <thead><tr><th>Jurisdiction</th><th>Source</th><th>Status</th><th>Work units</th><th>Records</th><th>Deferred caps</th><th>Gaps / blocker</th><th>Coverage boundary</th></tr></thead>
    <tbody id="permit-worker-rows"></tbody>
  </table>
  <h2>Paused operational workers</h2>
  <p class="route-note">Checkpoint pauses are operational states and are not source-route blockers.</p>
  <ul id="permit-paused-workers"><li>Loading worker state…</li></ul>
  <h2>Cooling-down operational workers</h2>
  <p class="route-note">Only workers with a live supervisor are scheduled to retry at the displayed safe time. Operator holds and source cooldowns are not source-route blockers.</p>
  <ul id="permit-cooling-workers"><li>Loading cooldown state…</li></ul>
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
  <h2>Current permit route implementation</h2>
  <p class="route-note">Counts include one current primary route per jurisdiction. Historical and supplemental routes are excluded.</p>
  <section class="grid">
    <article><h2>Total current routes</h2><strong id="permit-route-total">—</strong></article>
    <article><h2>Automated / implemented</h2><strong id="permit-route-implemented">—</strong></article>
    <article><h2>Manual CAPTCHA</h2><strong id="permit-route-manual">—</strong></article>
    <article><h2>Hard blocked</h2><strong id="permit-route-hard-blocked">—</strong></article>
    <article><h2>Unattended unavailable</h2><strong id="permit-route-unattended">—</strong></article>
  </section>
  <div class="route-groups">
    <section class="route-group">
      <h3>Implemented jurisdictions</h3>
      <p id="permit-route-implemented-names">Loading route status…</p>
    </section>
    <section class="route-group">
      <h3>Manual CAPTCHA route progress</h3>
      <p class="route-note">Manual sessions expire, each route remains <code>captcha_required</code>, and bounded captures do not establish county completeness.</p>
      <ul id="permit-route-manual-progress"><li>Loading manual route progress…</li></ul>
    </section>
    <section class="route-group">
      <h3>Hard-block categories</h3>
      <div id="permit-route-blocker-groups"></div>
    </section>
  </div>
  <p id="error"></p>
  <p>Only aggregate counts and timestamps are exposed. Refreshes every five seconds.</p>
<script>
  "use strict";
  const format = new Intl.NumberFormat();
  const set = (id, value) => { const node = document.getElementById(id); if (node) node.textContent = value; };
  const nullable = (value) => value === null ? "Not recorded" : format.format(value);
  /**
   * Format an optional aggregate count without inventing a zero.
   * @param {number | null} value Aggregate count or unavailable marker.
   * @returns {string} Localized count or "unknown".
   */
  const optionalCount = (value) => value === null ? "unknown" : format.format(value);
  /**
   * Format a checkpoint age from a bounded number of seconds.
   * @param {number | null} seconds Snapshot-relative checkpoint age.
   * @returns {string} Compact human-readable age.
   */
  const formatAge = (seconds) => {
    if (seconds === null) return "unknown age";
    if (seconds < 60) return format.format(seconds) + "s ago";
    if (seconds < 3600) return format.format(Math.round(seconds / 60)) + "m ago";
    return (seconds / 3600).toFixed(1) + "h ago";
  };
  /**
   * Format an ETA only when the server-side validity checks accepted it.
   * @param {{kind:string,estimatedHours:number|null,lowHours:number|null,highHours:number|null,reason:string}} eta Conditional ETA.
   * @returns {string} Complete, range, or explicit unknown reason.
   */
  const formatEta = (eta) => {
    if (eta.kind === "complete") return "complete";
    if (eta.kind !== "estimate") return "unknown — " + eta.reason.replaceAll("_", " ");
    return eta.estimatedHours.toFixed(1) + "h (" + eta.lowHours.toFixed(1) + "–" + eta.highHours.toFixed(1) + "h)";
  };
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
      const activeEnumeration = status.activePermitEnumeration;
      const activeWorkerBody = document.getElementById("active-permit-worker-rows");
      if (activeWorkerBody && activeEnumeration) {
        const rows = activeEnumeration.workers.map((worker) => {
          const row = document.createElement("tr");
          const processState = worker.processAlive === null
            ? "process unknown"
            : worker.processAlive
              ? "process alive"
              : "process absent";
          const detailState = worker.detailActive === null
            ? "detail unknown"
            : worker.detailActive
              ? "detail active"
              : "no detail child";
          const rate = worker.throughput.unitsPerHour === null
            ? "unknown"
            : worker.throughput.unitsPerHour.toFixed(1) + " units/h";
          for (const value of [
            worker.jurisdiction,
            worker.method.replaceAll("_", " ") + " / " + worker.family.replaceAll("_", " "),
            worker.state + (activeEnumeration.snapshotStale ? " (stale snapshot)" : ""),
            processState + " / " + detailState + " / " + worker.checkpointActivity.replaceAll("_", " "),
            format.format(worker.completedUnits) + " / " + format.format(worker.totalUnits) +
              " · " + format.format(worker.remainingUnits) + " left · " + worker.completionPercent.toFixed(3) + "%",
            optionalCount(worker.locallyCapturedRecords) + " / " + optionalCount(worker.durableLoadedRecords),
            format.format(worker.deferredCapCount) + " / " + format.format(worker.sourceMissingCount),
            (worker.lastCheckpointAt ?? "unknown") + " · " + formatAge(worker.checkpointAgeSeconds),
            rate + " over " + format.format(worker.throughput.windowSeconds) + "s",
            formatEta(worker.eta),
          ]) {
            const cell = document.createElement("td");
            cell.textContent = value;
            row.appendChild(cell);
          }
          return row;
        });
        activeWorkerBody.replaceChildren(...rows);
      }
      const coral = status.coralSpringsPermit;
      set("coral-reported", format.format(coral.reported) + " / " + format.format(coral.exposed));
      set("coral-captured", format.format(coral.paged) + " / " + format.format(coral.unique));
      set("coral-loaded", format.format(coral.loaded) + " / " + format.format(coral.linked));
      set("coral-roofing", format.format(coral.roofing));
      set("coral-pages", format.format(coral.completedPages) + " / " + format.format(coral.totalPages));
      set("coral-coverage", coral.captureComplete ? "bounded slice complete" : "bounded slice partial");
      set(
        "permit-inventory-summary",
        format.format(inventory.records) + " loaded · " +
          format.format(enumeration.accessibleRecords) + " locally captured · " +
          format.format(enumeration.excludedRecords) + " excluded · " +
          format.format(enumeration.deferredCapCount) + " deferred caps · " +
          format.format(enumeration.invalidRecords + enumeration.sourceMissingRecords) + " source gaps",
      );
      const workerBody = document.getElementById("permit-worker-rows");
      if (workerBody) {
        const rows = enumeration.workers.map((worker) => {
          const row = document.createElement("tr");
          for (const value of [
            worker.source,
            worker.family.replaceAll("_", " "),
            worker.status === "not_started" ? "no-start" : worker.status,
            format.format(worker.completedWindows) + " / " + format.format(worker.totalWindows),
            format.format(worker.accessibleRecords),
            format.format(worker.deferredCapCount),
            worker.startBlocker
              ? worker.startBlocker.replaceAll("_", " ")
              : format.format(worker.invalidRecords + worker.sourceMissingRecords),
            worker.coverageBoundary ?? "Date-window inventory; municipal history boundary applies",
          ]) {
            const cell = document.createElement("td");
            cell.textContent = value;
            row.appendChild(cell);
          }
          return row;
        });
        workerBody.replaceChildren(...rows);
      }
      const pausedWorkers = enumeration.pausedWorkers;
      const pausedWorkerList = document.getElementById("permit-paused-workers");
      if (pausedWorkerList) {
        if (pausedWorkers.length === 0) {
          const item = document.createElement("li");
          item.textContent = "No workers are paused.";
          pausedWorkerList.replaceChildren(item);
        } else {
          pausedWorkerList.replaceChildren(
            ...pausedWorkers.map((worker) => {
              const item = document.createElement("li");
              item.textContent = worker.source + " — " + worker.reason.replaceAll("_", " ");
              return item;
            }),
          );
        }
      }
      const coolingWorkers = enumeration.coolingWorkers;
      const coolingWorkerList = document.getElementById("permit-cooling-workers");
      if (coolingWorkerList) {
        if (coolingWorkers.length === 0) {
          const item = document.createElement("li");
          item.textContent = "No workers are cooling down.";
          coolingWorkerList.replaceChildren(item);
        } else {
          coolingWorkerList.replaceChildren(
            ...coolingWorkers.map((worker) => {
              const item = document.createElement("li");
              const processState = worker.processAlive === true
                ? "supervisor alive"
                : worker.processAlive === false
                  ? "supervisor absent"
                  : "supervisor unknown";
              item.textContent = worker.source + " — " + worker.reason.replaceAll("_", " ") + "; " + processState + "; next safe retry " + worker.nextAttemptAt;
              return item;
            }),
          );
        }
      }
      const sunbiz = status.sunbizMatch;
      set("sunbiz-registrations", format.format(sunbiz.registrations));
      set("sunbiz-properties", format.format(sunbiz.properties));
      set("sunbiz-roles", format.format(sunbiz.matchedAddressRoles));
      set("sunbiz-chunks", format.format(sunbiz.chunks));
      const permit = status.permit;
      const routes = status.permitRoutes;
      set("permit-pilot", permit.pilotState.replaceAll("_", " "));
      set("permit-completeness", permit.countyCompleteness.replaceAll("_", " "));
      set("permit-sample", nullable(permit.sampleParcels));
      set("permit-attempts", nullable(permit.sourceAttempts));
      set("permit-records", nullable(permit.queryRows));
      set(
        "permit-routes",
        format.format(permit.currentSourcesImplemented) + " automated / " +
          format.format(permit.currentSourcesManualCaptcha) + " manual CAPTCHA / " +
          format.format(permit.currentSourcesHardBlocked) + " hard blocked",
      );
      set("permit-route-total", format.format(routes.totalCurrentRoutes));
      set("permit-route-implemented", format.format(routes.implementedCurrentRoutes));
      set("permit-route-manual", format.format(routes.manualCaptchaCurrentRoutes));
      set("permit-route-hard-blocked", format.format(routes.hardBlockedCurrentRoutes));
      set("permit-route-unattended", format.format(routes.unattendedUnavailableCurrentRoutes));
      set("permit-route-implemented-names", routes.implementedJurisdictions.join(", "));
      const manualRouteList = document.getElementById("permit-route-manual-progress");
      if (manualRouteList) {
        manualRouteList.replaceChildren(
          ...status.manualCaptchaProgress.routes.map((route) => {
            const item = document.createElement("li");
            const state = route.progressState.replaceAll("_", " ");
            item.textContent =
              route.jurisdiction + " — " + state + "; " +
              format.format(route.capturedRecords) + " captured, " +
              format.format(route.loadedRecords) + " loaded; session expires; county complete: no";
            return item;
          }),
        );
      }
      const routeGroups = document.getElementById("permit-route-blocker-groups");
      if (routeGroups) {
        routeGroups.replaceChildren(
          ...routes.hardBlockCategories.map((category) => {
            const group = document.createElement("section");
            group.className = "route-group";
            const heading = document.createElement("h3");
            heading.textContent = category.label + " (" + format.format(category.count) + ")";
            const names = document.createElement("p");
            names.textContent = category.jurisdictions.length === 0
              ? "None"
              : category.jurisdictions.join(", ");
            group.append(heading, names);
            return group;
          }),
        );
      }
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
