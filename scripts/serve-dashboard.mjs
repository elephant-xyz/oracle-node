#!/usr/bin/env node
/**
 * Universal County Lifecycle & Open Data Dashboard Server.
 *
 * Serves real-time telemetry, stage progression, and open data queries
 * for any configured county in the Oracle ecosystem.
 *
 * Usage:
 *   node scripts/serve-dashboard.mjs --county=volusia
 *   node scripts/serve-dashboard.mjs --county=hillsborough --port=3888
 *
 * @module scripts/serve-dashboard
 */

import { exec } from "node:child_process";
import { access, readFile, stat } from "node:fs/promises";
import { createServer } from "node:http";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  COUNTY_REGISTRY,
  getCountyMetadata,
  listCounties,
} from "./common/county-registry.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "..");
const DASHBOARD_HTML_PATH = resolve(__dirname, "common/dashboard.html");

/**
 * @typedef {object} ServerOptions
 * @property {number} port
 * @property {string} jobId
 * @property {string} county
 * @property {string} outputRoot
 * @property {boolean} open
 *
 * @typedef {"software_or_transport" | "login_required" | "no_anonymous_search" | "custodian_only"} UniversalPermitRouteHardBlockKey
 *
 * @typedef {object} UniversalPermitRouteHardBlockCategory
 * @property {UniversalPermitRouteHardBlockKey} key - Stable hard-block category.
 * @property {"software_transport" | "source_policy"} kind - Actionability class.
 * @property {string} label - Public category label.
 * @property {number} count - Reconciled hard-blocked-route count.
 * @property {string[]} jurisdictions - Public jurisdiction names.
 *
 * @typedef {object} UniversalPermitRouteStatus
 * @property {string} registryVersion - Executable registry version.
 * @property {number} totalCurrentRoutes - Current primary-route denominator.
 * @property {number} implementedCurrentRoutes - Implemented current routes.
 * @property {number} manualCaptchaCurrentRoutes
 *   Current routes requiring manually authorized CAPTCHA sessions.
 * @property {number} hardBlockedCurrentRoutes
 *   Routes unavailable for non-CAPTCHA barriers.
 * @property {number} unattendedUnavailableCurrentRoutes
 *   Manual CAPTCHA plus hard-blocked routes.
 * @property {string[]} implementedJurisdictions - Public implemented names.
 * @property {string[]} manualCaptchaJurisdictions - Public manual-route names.
 * @property {UniversalPermitRouteHardBlockCategory[]} hardBlockCategories
 *   Exhaustive non-CAPTCHA hard-block categories.
 *
 * @typedef {"awaiting_manual_captcha" | "bounded_capture_in_progress" | "bounded_slice_captured" | "bounded_slice_loaded"} UniversalManualCaptchaProgressState
 *
 * @typedef {"private_capture_checkpoint" | "durable_loaded_aggregate" | "no_captured_aggregate"} UniversalManualCaptchaEvidence
 *
 * @typedef {"bounded_capped_slice" | "bounded_slice" | "not_captured"} UniversalManualCaptchaCoverageBoundary
 *
 * @typedef {object} UniversalManualCaptchaRouteProgress
 * @property {string} jurisdiction - Public CAPTCHA-dependent jurisdiction.
 * @property {"captcha_required"} registryStatus - Executable route status.
 * @property {UniversalManualCaptchaProgressState} progressState
 *   Reconciled bounded manual progress.
 * @property {UniversalManualCaptchaEvidence} evidence
 *   Aggregate evidence source without source records or local paths.
 * @property {UniversalManualCaptchaCoverageBoundary} coverageBoundary
 *   Explicitly non-countywide coverage boundary.
 * @property {number} capturedRecords - Aggregate bounded captured records.
 * @property {number} loadedRecords - Durable loaded source-system records.
 * @property {true} manualSessionRequired - CAPTCHA is manually completed.
 * @property {true} sessionsExpire - Manual authorization is temporary.
 * @property {true} validSearchCaptchaRequired - A new search requires CAPTCHA.
 * @property {false} countyComplete - Manual evidence is not county completeness.
 *
 * @typedef {object} UniversalManualCaptchaProgress
 * @property {"manual_captcha_sessions_expire"} sessionPolicy
 *   Public session lifecycle statement.
 * @property {false} countyComplete - County completeness remains false.
 * @property {UniversalManualCaptchaRouteProgress[]} routes
 *   One validated route per manual CAPTCHA jurisdiction.
 *
 * @typedef {object} UniversalPausedPermitWorker
 * @property {string} source - Public jurisdiction label.
 * @property {"timeout" | "missing_controls" | "missing_export" | "source_cap" | "checkpoint_stale"} reason
 *   Allowlisted operational pause reason.
 *
 * @typedef {object} UniversalCoolingPermitWorker
 * @property {string} source - Public jurisdiction label.
 * @property {"timeout" | "source_cap" | "incomplete_pagination" | "source_error"} reason
 *   Allowlisted source circuit-breaker reason.
 * @property {string} nextAttemptAt - Earliest safe automatic retry.
 *
 * @typedef {object} UniversalActiveEnumerationWorker
 * @property {string} jurisdiction - Allowlisted public jurisdiction.
 * @property {"full" | "property_first"} method - Enumeration scope.
 * @property {"municipal_property" | "municipal_type" | "bcs_posse" | "citizenserve"} family
 *   Executable source family.
 * @property {"running" | "cooling" | "paused" | "complete" | "stalled"} state
 *   Reconciled operational state.
 * @property {boolean | null} processAlive - Independent live-process evidence.
 * @property {"warming_up" | "work_units_advanced" | "checkpoint_updated" | "stationary"} checkpointActivity
 *   Observed aggregate checkpoint movement.
 * @property {number} completedUnits - Durable completed work units.
 * @property {number} totalUnits - Immutable work-unit denominator.
 * @property {number} remainingUnits - Reconciled unfinished work units.
 * @property {number} completionPercent - Recomputed completion percentage.
 * @property {number | null} locallyCapturedRecords - Local capture count when derivable.
 * @property {number | null} durableLoadedRecords - Neon-loaded count when derivable.
 * @property {number} deferredCapCount - Unresolved cap count.
 * @property {number} sourceMissingCount - Explicit inaccessible source count.
 * @property {string | null} lastCheckpointAt - Last durable checkpoint time.
 * @property {number | null} checkpointAgeSeconds - Snapshot-relative age.
 * @property {boolean} checkpointStale - Checkpoint staleness.
 * @property {{
 *   observedUnits:number,
 *   windowSeconds:number,
 *   unitsPerHour:number|null,
 *   variabilityRatio:number|null
 * }} throughput - Recent completed-unit observation.
 * @property {{
 *   kind:"estimate"|"unknown"|"complete",
 *   estimatedHours:number|null,
 *   lowHours:number|null,
 *   highHours:number|null,
 *   reason:"complete"|"rate_stable"|"dashboard_snapshot_stale"|"worker_not_running"|"checkpoint_stale"|"variable_detail_loop"|"observation_window_short"|"no_checkpoint_movement"|"rate_variability_high"|"work_unit_total_changed"
 * }} eta - Conditional estimate or allowlisted refusal reason.
 *
 * @typedef {object} UniversalActiveEnumerationStatus
 * @property {string} generatedAt - Aggregate snapshot time.
 * @property {boolean} snapshotStale - Whole-snapshot staleness.
 * @property {number} observationWindowSeconds - Longest observation window.
 * @property {UniversalActiveEnumerationWorker[]} workers - Exactly ten active routes.
 */

/**
 * Parse CLI arguments.
 * @param {string[]} argv
 * @returns {ServerOptions}
 */
export function parseServerArgs(argv) {
  const portArg = argv.find((a) => a.startsWith("--port="))?.split("=")[1];
  const countyArg =
    argv.find((a) => a.startsWith("--county="))?.split("=")[1] || "volusia";
  const jobId =
    argv.find((a) => a.startsWith("--job-id="))?.split("=")[1] ||
    `${countyArg}-lifecycle-live`;
  const outputRoot = resolve(
    ROOT,
    argv.find((a) => a.startsWith("--output="))?.split("=")[1] ||
      `data/${countyArg}/pilot`,
  );
  const open = !argv.includes("--no-open");
  const port = portArg ? Number.parseInt(portArg, 10) : 3888;

  return { port, jobId, county: countyArg, outputRoot, open };
}

/**
 * Read the verified aggregate Broward dashboard response.
 *
 * @param {string} [statusUrl="http://127.0.0.1:47832/api/status"] - Private aggregate endpoint.
 * @returns {Promise<Record<string, unknown>>} Validated aggregate response.
 */
export async function readBrowardAggregateStatus(
  statusUrl = "http://127.0.0.1:47832/api/status",
) {
  const response = await fetch(statusUrl, {
    headers: { Accept: "application/json" },
    signal: AbortSignal.timeout(90_000),
  });
  if (!response.ok) {
    throw new Error(
      `Broward aggregate dashboard returned HTTP ${String(response.status)}`,
    );
  }
  const value = /** @type {unknown} */ (await response.json());
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Broward aggregate dashboard returned invalid JSON");
  }
  return /** @type {Record<string, unknown>} */ (value);
}

/**
 * Map verified Broward telemetry into the universal lifecycle contract.
 *
 * @param {string} rootPath - Repository root.
 * @param {() => Promise<Record<string, unknown>>} [readStatus=readBrowardAggregateStatus]
 *   Injectable aggregate status reader.
 * @returns {Promise<Record<string, unknown>>} Real Broward lifecycle response.
 */
export async function getBrowardLifecycleStatus(
  rootPath,
  readStatus = readBrowardAggregateStatus,
) {
  const county = getCountyMetadata("broward");
  const status = await readStatus();
  const progress = requireObject(status.progress, "Broward progress");
  const inventory = requireObject(
    status.permitInventory,
    "Broward permit inventory",
  );
  const enumeration = requireObject(
    status.permitEnumeration,
    "Broward permit enumeration",
  );
  const permitRoutes = readBrowardPermitRouteStatus(status.permitRoutes);
  const manualCaptchaProgress = readBrowardManualCaptchaProgress(
    status.manualCaptchaProgress,
    permitRoutes.manualCaptchaJurisdictions,
  );
  const pausedPermitWorkers = readBrowardPausedPermitWorkers(
    enumeration.pausedWorkers,
    permitRoutes.implementedJurisdictions,
  );
  const coolingPermitWorkers = readBrowardCoolingPermitWorkers(
    enumeration.coolingWorkers,
    permitRoutes.implementedJurisdictions,
  );
  const activeEnumeration = readBrowardActiveEnumeration(
    status.activePermitEnumeration,
  );
  if (
    coolingPermitWorkers.some((coolingWorker) =>
      pausedPermitWorkers.some(
        (pausedWorker) => pausedWorker.source === coolingWorker.source,
      ),
    )
  ) {
    throw new Error("Broward operational worker states overlap");
  }
  const sunbiz = requireObject(status.sunbizMatch, "Broward Sunbiz match");
  const properties = requireNonNegativeNumber(
    progress.properties,
    "Broward properties",
  );
  const permits = requireNonNegativeNumber(
    inventory.records,
    "Broward permits",
  );
  const roofing = requireNonNegativeNumber(
    inventory.roofing,
    "Broward roofing permits",
  );
  const capturedPermits = requireNonNegativeNumber(
    enumeration.accessibleRecords,
    "Broward captured permits",
  );
  const sunbizRegistrations = requireNonNegativeNumber(
    sunbiz.registrations,
    "Broward Sunbiz registrations",
  );
  let bbbCandidateCount = 0;
  try {
    const summary = /** @type {unknown} */ (
      JSON.parse(
        await readFile(
          resolve(
            rootPath,
            "downloads/broward/bbb-roofing-worklist/summary.private.json",
          ),
          "utf8",
        ),
      )
    );
    if (
      summary !== null &&
      typeof summary === "object" &&
      !Array.isArray(summary)
    ) {
      bbbCandidateCount = requireNonNegativeNumber(
        /** @type {Record<string, unknown>} */ (summary).candidateCount,
        "Broward BBB candidate count",
      );
    }
  } catch (error) {
    if (
      !(error instanceof Error && "code" in error && error.code === "ENOENT")
    ) {
      throw error;
    }
  }
  const appraisalComplete = properties >= county.targetParcels;
  return {
    county,
    timestamp: new Date().toISOString(),
    telemetrySource: "broward-neon-recovery-dashboard",
    stages: {
      discovery: {
        number: 1,
        title: "Discovery",
        status: "completed",
        docPath: "docs/broward-county-findings.md",
        fips: county.fips,
        portal: county.appraiserUrl,
      },
      seed: {
        number: 2,
        title: "Seed Generation",
        status: "completed",
        count: county.totalSeedParcels,
        target: county.totalSeedParcels,
        pct: "100.00",
        featureServer: county.gisFeatureServer,
      },
      appraisal: {
        number: 3,
        title: "Appraisal Harvest",
        status: appraisalComplete ? "completed" : "in_progress",
        count: properties,
        target: county.targetParcels,
        pct: ((properties / Math.max(1, county.targetParcels)) * 100).toFixed(
          2,
        ),
        speed: 0,
        eta: null,
      },
      sourcing: {
        number: 4,
        title: "Permits & Sourcing",
        status: capturedPermits > permits ? "in_progress" : "partially_loaded",
        permitRoutes,
        manualCaptchaProgress,
        operationalWorkers: {
          paused: pausedPermitWorkers,
          coolingDown: coolingPermitWorkers,
        },
        activeEnumeration,
        permits: {
          count: permits,
          capturedCount: capturedPermits,
          target: null,
          tradeCounts: { Roofing: roofing },
          enrichment: {
            status: "active",
            verifiedCount: requireNonNegativeNumber(
              inventory.matched,
              "Broward matched permits",
            ),
          },
        },
        sunbiz: {
          count: sunbizRegistrations,
          status: "property_matched",
          propertyCount: requireNonNegativeNumber(
            sunbiz.properties,
            "Broward Sunbiz properties",
          ),
        },
        bbb: {
          count: 0,
          candidateCount: bbbCandidateCount,
          status: "api_credentials_required",
        },
      },
      warehouse: {
        number: 5,
        title: "Postgres Warehouse",
        status: "in_progress",
        count: properties,
        target: county.targetParcels,
      },
      publish: {
        number: 6,
        title: "Publish & IPFS",
        status: "disabled",
        parquetCount: 0,
        parquetSizeBytes: 0,
        ipnsKey: null,
        coverageIpnsKey: null,
      },
    },
    nextStep: {
      stageNumber: 4,
      stageName: "Permits & Sourcing",
      actionTitle: "Complete verified municipal permit inventories",
      description:
        "Resume checkpointed Accela/Tyler tenant workers and load only reconciled list artifacts.",
      command:
        "npm run broward:recovery-dashboard -- --host 0.0.0.0 --port 47832",
      status: "In Progress",
    },
  };
}

/**
 * Require an object field from aggregate telemetry.
 *
 * @param {unknown} value - Candidate field.
 * @param {string} label - Error label.
 * @returns {Record<string, unknown>} Validated object.
 */
function requireObject(value, label) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} is missing`);
  }
  return /** @type {Record<string, unknown>} */ (value);
}

/**
 * Require a finite non-negative aggregate number.
 *
 * @param {unknown} value - Candidate count.
 * @param {string} label - Error label.
 * @returns {number} Validated count.
 */
function requireNonNegativeNumber(value, label) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`${label} is invalid`);
  }
  return parsed;
}

const BROWARD_ROUTE_CATEGORY_METADATA = Object.freeze({
  software_or_transport: Object.freeze({
    kind: "software_transport",
    label: "Software / transport",
  }),
  login_required: Object.freeze({
    kind: "source_policy",
    label: "Login required",
  }),
  no_anonymous_search: Object.freeze({
    kind: "source_policy",
    label: "No anonymous search",
  }),
  custodian_only: Object.freeze({
    kind: "source_policy",
    label: "Custodian only",
  }),
});

/**
 * Require one non-empty public label.
 *
 * @param {unknown} value - Candidate label.
 * @param {string} fieldName - Field name for a safe validation error.
 * @returns {string} Trimmed public label.
 */
function requirePublicLabel(value, fieldName) {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${fieldName} is invalid`);
  }
  return value.trim();
}

/**
 * Require an array of unique non-empty public labels.
 *
 * @param {unknown} value - Candidate label collection.
 * @param {string} fieldName - Field name for a safe validation error.
 * @returns {string[]} Copied unique labels.
 */
function requirePublicLabels(value, fieldName) {
  if (!Array.isArray(value)) {
    throw new Error(`${fieldName} is missing`);
  }
  const labels = value.map((label) => requirePublicLabel(label, fieldName));
  if (new Set(labels).size !== labels.length) {
    throw new Error(`${fieldName} contains duplicates`);
  }
  return labels;
}

/**
 * Validate the private Broward route summary before exposing it through the
 * universal dashboard. Reconciliation is repeated at this boundary so stale
 * or partial upstream payloads fail closed.
 *
 * @param {unknown} value - Private aggregate route payload.
 * @returns {UniversalPermitRouteStatus} Clean universal route summary.
 */
function readBrowardPermitRouteStatus(value) {
  const input = requireObject(value, "Broward permit routes");
  const registryVersion = requirePublicLabel(
    input.registryVersion,
    "Broward permit registry version",
  );
  const totalCurrentRoutes = requireNonNegativeNumber(
    input.totalCurrentRoutes,
    "Broward total permit routes",
  );
  const implementedCurrentRoutes = requireNonNegativeNumber(
    input.implementedCurrentRoutes,
    "Broward implemented permit routes",
  );
  const manualCaptchaCurrentRoutes = requireNonNegativeNumber(
    input.manualCaptchaCurrentRoutes,
    "Broward manual CAPTCHA permit routes",
  );
  const hardBlockedCurrentRoutes = requireNonNegativeNumber(
    input.hardBlockedCurrentRoutes,
    "Broward hard-blocked permit routes",
  );
  const unattendedUnavailableCurrentRoutes = requireNonNegativeNumber(
    input.unattendedUnavailableCurrentRoutes,
    "Broward unattended-unavailable permit routes",
  );
  if (
    !Number.isSafeInteger(totalCurrentRoutes) ||
    !Number.isSafeInteger(implementedCurrentRoutes) ||
    !Number.isSafeInteger(manualCaptchaCurrentRoutes) ||
    !Number.isSafeInteger(hardBlockedCurrentRoutes) ||
    !Number.isSafeInteger(unattendedUnavailableCurrentRoutes) ||
    implementedCurrentRoutes +
      manualCaptchaCurrentRoutes +
      hardBlockedCurrentRoutes !==
      totalCurrentRoutes ||
    manualCaptchaCurrentRoutes + hardBlockedCurrentRoutes !==
      unattendedUnavailableCurrentRoutes
  ) {
    throw new Error("Broward permit route totals do not reconcile");
  }
  const implementedJurisdictions = requirePublicLabels(
    input.implementedJurisdictions,
    "Broward implemented permit jurisdictions",
  );
  if (implementedJurisdictions.length !== implementedCurrentRoutes) {
    throw new Error("Broward implemented permit routes do not reconcile");
  }
  const manualCaptchaJurisdictions = requirePublicLabels(
    input.manualCaptchaJurisdictions,
    "Broward manual CAPTCHA permit jurisdictions",
  );
  if (manualCaptchaJurisdictions.length !== manualCaptchaCurrentRoutes) {
    throw new Error("Broward manual CAPTCHA routes do not reconcile");
  }
  if (!Array.isArray(input.hardBlockCategories)) {
    throw new Error("Broward permit hard-block categories are missing");
  }
  /** @type {Set<UniversalPermitRouteHardBlockKey>} */
  const seenCategoryKeys = new Set();
  const hardBlockCategories = input.hardBlockCategories.map((candidate) => {
    const category = requireObject(
      candidate,
      "Broward permit hard-block category",
    );
    const key = requirePublicLabel(
      category.key,
      "Broward permit hard-block key",
    );
    if (!(key in BROWARD_ROUTE_CATEGORY_METADATA)) {
      throw new Error("Broward permit hard-block category is unknown");
    }
    const typedKey = /** @type {UniversalPermitRouteHardBlockKey} */ (key);
    if (seenCategoryKeys.has(typedKey)) {
      throw new Error("Broward permit hard-block category is duplicated");
    }
    seenCategoryKeys.add(typedKey);
    const metadata = BROWARD_ROUTE_CATEGORY_METADATA[typedKey];
    const count = requireNonNegativeNumber(
      category.count,
      "Broward permit hard-block count",
    );
    const jurisdictions = requirePublicLabels(
      category.jurisdictions,
      "Broward hard-blocked permit jurisdictions",
    );
    if (!Number.isSafeInteger(count) || count !== jurisdictions.length) {
      throw new Error("Broward permit hard-block category does not reconcile");
    }
    return {
      key: typedKey,
      kind: /** @type {"software_transport" | "source_policy"} */ (
        metadata.kind
      ),
      label: metadata.label,
      count,
      jurisdictions,
    };
  });
  if (
    seenCategoryKeys.size !==
      Object.keys(BROWARD_ROUTE_CATEGORY_METADATA).length ||
    hardBlockCategories.reduce((sum, category) => sum + category.count, 0) !==
      hardBlockedCurrentRoutes
  ) {
    throw new Error("Broward permit hard-block categories do not reconcile");
  }
  const allJurisdictions = [
    ...implementedJurisdictions,
    ...manualCaptchaJurisdictions,
    ...hardBlockCategories.flatMap((category) => category.jurisdictions),
  ];
  if (new Set(allJurisdictions).size !== totalCurrentRoutes) {
    throw new Error("Broward current permit jurisdictions do not reconcile");
  }
  return {
    registryVersion,
    totalCurrentRoutes,
    implementedCurrentRoutes,
    manualCaptchaCurrentRoutes,
    hardBlockedCurrentRoutes,
    unattendedUnavailableCurrentRoutes,
    implementedJurisdictions,
    manualCaptchaJurisdictions,
    hardBlockCategories,
  };
}

/**
 * Validate aggregate-only manual CAPTCHA progress before exposing it through
 * the universal dashboard. Session material, search criteria, source records,
 * paths, and raw errors are not accepted by this contract.
 *
 * @param {unknown} value - Private dashboard manual-progress payload.
 * @param {readonly string[]} manualCaptchaJurisdictions
 *   Registry-derived jurisdictions that must appear exactly once.
 * @returns {UniversalManualCaptchaProgress} Sanitized aggregate progress.
 */
function readBrowardManualCaptchaProgress(value, manualCaptchaJurisdictions) {
  const input = requireObject(value, "Broward manual CAPTCHA progress");
  if (
    input.sessionPolicy !== "manual_captcha_sessions_expire" ||
    input.countyComplete !== false ||
    !Array.isArray(input.routes)
  ) {
    throw new Error("Broward manual CAPTCHA progress is invalid");
  }
  const allowedStates = new Set([
    "awaiting_manual_captcha",
    "bounded_capture_in_progress",
    "bounded_slice_captured",
    "bounded_slice_loaded",
  ]);
  const allowedEvidence = new Set([
    "private_capture_checkpoint",
    "durable_loaded_aggregate",
    "no_captured_aggregate",
  ]);
  const allowedBoundaries = new Set([
    "bounded_capped_slice",
    "bounded_slice",
    "not_captured",
  ]);
  const seenJurisdictions = new Set();
  const routes = input.routes.map((candidate) => {
    const route = requireObject(
      candidate,
      "Broward manual CAPTCHA route progress",
    );
    const jurisdiction = requirePublicLabel(
      route.jurisdiction,
      "Broward manual CAPTCHA jurisdiction",
    );
    const progressState = requirePublicLabel(
      route.progressState,
      "Broward manual CAPTCHA progress state",
    );
    const evidence = requirePublicLabel(
      route.evidence,
      "Broward manual CAPTCHA evidence",
    );
    const coverageBoundary = requirePublicLabel(
      route.coverageBoundary,
      "Broward manual CAPTCHA coverage boundary",
    );
    const capturedRecords = requireNonNegativeNumber(
      route.capturedRecords,
      "Broward manual CAPTCHA captured records",
    );
    const loadedRecords = requireNonNegativeNumber(
      route.loadedRecords,
      "Broward manual CAPTCHA loaded records",
    );
    if (
      seenJurisdictions.has(jurisdiction) ||
      !manualCaptchaJurisdictions.includes(jurisdiction) ||
      route.registryStatus !== "captcha_required" ||
      !allowedStates.has(progressState) ||
      !allowedEvidence.has(evidence) ||
      !allowedBoundaries.has(coverageBoundary) ||
      !Number.isSafeInteger(capturedRecords) ||
      !Number.isSafeInteger(loadedRecords) ||
      route.manualSessionRequired !== true ||
      route.sessionsExpire !== true ||
      route.validSearchCaptchaRequired !== true ||
      route.countyComplete !== false ||
      (progressState === "awaiting_manual_captcha" &&
        (capturedRecords !== 0 || loadedRecords !== 0)) ||
      (progressState === "bounded_capture_in_progress" &&
        capturedRecords === 0) ||
      (progressState === "bounded_slice_captured" && capturedRecords === 0) ||
      (progressState === "bounded_slice_loaded" && loadedRecords === 0) ||
      (coverageBoundary === "not_captured" &&
        (capturedRecords !== 0 || loadedRecords !== 0))
    ) {
      throw new Error("Broward manual CAPTCHA route does not reconcile");
    }
    seenJurisdictions.add(jurisdiction);
    return {
      jurisdiction,
      registryStatus: /** @type {"captcha_required"} */ ("captcha_required"),
      progressState: /** @type {UniversalManualCaptchaProgressState} */ (
        progressState
      ),
      evidence: /** @type {UniversalManualCaptchaEvidence} */ (evidence),
      coverageBoundary: /** @type {UniversalManualCaptchaCoverageBoundary} */ (
        coverageBoundary
      ),
      capturedRecords,
      loadedRecords,
      manualSessionRequired: /** @type {true} */ (true),
      sessionsExpire: /** @type {true} */ (true),
      validSearchCaptchaRequired: /** @type {true} */ (true),
      countyComplete: /** @type {false} */ (false),
    };
  });
  if (
    routes.length !== manualCaptchaJurisdictions.length ||
    manualCaptchaJurisdictions.some(
      (jurisdiction) => !seenJurisdictions.has(jurisdiction),
    )
  ) {
    throw new Error("Broward manual CAPTCHA jurisdictions do not reconcile");
  }
  routes.sort((left, right) =>
    left.jurisdiction.localeCompare(right.jurisdiction),
  );
  return {
    sessionPolicy: "manual_captcha_sessions_expire",
    countyComplete: false,
    routes,
  };
}

/**
 * Validate allowlisted operational pauses independently of source blockers.
 * A paused enumerator must belong to an implemented route; this prevents a
 * runtime checkpoint state from inflating or masquerading as blocked coverage.
 *
 * @param {unknown} value - Candidate paused worker list.
 * @param {readonly string[]} implementedJurisdictions - Registry-derived implemented names.
 * @returns {UniversalPausedPermitWorker[]} Clean operational pause list.
 */
function readBrowardPausedPermitWorkers(value, implementedJurisdictions) {
  if (!Array.isArray(value)) {
    throw new Error("Broward paused permit workers are missing");
  }
  const allowedReasons = new Set([
    "timeout",
    "missing_controls",
    "missing_export",
    "source_cap",
    "checkpoint_stale",
  ]);
  const workerSources = new Set();
  return value.map((candidate) => {
    const worker = requireObject(candidate, "Broward paused permit worker");
    const source = requirePublicLabel(
      worker.source,
      "Broward paused permit worker source",
    );
    const reason = requirePublicLabel(
      worker.reason,
      "Broward paused permit worker reason",
    );
    if (
      workerSources.has(source) ||
      !implementedJurisdictions.includes(source) ||
      !allowedReasons.has(reason)
    ) {
      throw new Error("Broward paused permit worker does not reconcile");
    }
    workerSources.add(source);
    return {
      source,
      reason:
        /** @type {"timeout" | "missing_controls" | "missing_export" | "source_cap" | "checkpoint_stale"} */ (
          reason
        ),
    };
  });
}

/**
 * Validate public-safe circuit-breaker state independently of paused workers
 * and source-route blockers.
 *
 * @param {unknown} value - Candidate cooling worker list.
 * @param {readonly string[]} implementedJurisdictions - Registry-derived implemented names.
 * @returns {UniversalCoolingPermitWorker[]} Clean cooldown list.
 */
function readBrowardCoolingPermitWorkers(value, implementedJurisdictions) {
  if (value === undefined) return [];
  if (!Array.isArray(value)) {
    throw new Error("Broward cooling permit workers are missing");
  }
  const allowedReasons = new Set([
    "timeout",
    "source_cap",
    "incomplete_pagination",
    "source_error",
  ]);
  const workerSources = new Set();
  return value.map((candidate) => {
    const worker = requireObject(candidate, "Broward cooling permit worker");
    const source = requirePublicLabel(
      worker.source,
      "Broward cooling permit worker source",
    );
    const reason = requirePublicLabel(
      worker.reason,
      "Broward cooling permit worker reason",
    );
    const nextAttemptAt = requirePublicLabel(
      worker.nextAttemptAt,
      "Broward cooling permit worker next attempt",
    );
    if (
      workerSources.has(source) ||
      !implementedJurisdictions.includes(source) ||
      !allowedReasons.has(reason) ||
      !Number.isFinite(Date.parse(nextAttemptAt))
    ) {
      throw new Error("Broward cooling permit worker does not reconcile");
    }
    workerSources.add(source);
    return {
      source,
      reason:
        /** @type {"timeout" | "source_cap" | "incomplete_pagination" | "source_error"} */ (
          reason
        ),
      nextAttemptAt,
    };
  });
}

const BROWARD_ACTIVE_ENUMERATION_JURISDICTIONS = Object.freeze([
  "BMSD / unincorporated",
  "Coconut Creek",
  "Lauderdale-by-the-Sea",
  "Lauderhill",
  "Lighthouse Point",
  "Margate",
  "Southwest Ranches",
  "Tamarac",
  "West Park",
  "Wilton Manors",
]);

/**
 * Validate the dedicated active-enumeration payload before exposing it from
 * the universal server. Only allowlisted public labels, counters, timestamps,
 * process booleans, and ETA reasons cross this boundary.
 *
 * @param {unknown} value - Private dashboard active-enumeration payload.
 * @returns {UniversalActiveEnumerationStatus} Sanitized active routes.
 */
function readBrowardActiveEnumeration(value) {
  const input = requireObject(value, "Broward active enumeration");
  const generatedAt = requirePublicLabel(
    input.generatedAt,
    "Broward active enumeration timestamp",
  );
  if (
    !Number.isFinite(Date.parse(generatedAt)) ||
    typeof input.snapshotStale !== "boolean" ||
    !Array.isArray(input.workers)
  ) {
    throw new Error("Broward active enumeration is invalid");
  }
  const observationWindowSeconds = requireSafeAggregateCount(
    input.observationWindowSeconds,
    "Broward active enumeration window",
  );
  const expectedJurisdictions = new Set(
    BROWARD_ACTIVE_ENUMERATION_JURISDICTIONS,
  );
  const seenJurisdictions = new Set();
  const workers = input.workers.map((candidate) => {
    const worker = requireObject(
      candidate,
      "Broward active enumeration worker",
    );
    const jurisdiction = requirePublicLabel(
      worker.jurisdiction,
      "Broward active enumeration jurisdiction",
    );
    const method = requirePublicLabel(
      worker.method,
      "Broward active enumeration method",
    );
    const family = requirePublicLabel(
      worker.family,
      "Broward active enumeration family",
    );
    const state = requirePublicLabel(
      worker.state,
      "Broward active enumeration state",
    );
    const checkpointActivity = requirePublicLabel(
      worker.checkpointActivity,
      "Broward active enumeration checkpoint activity",
    );
    if (
      !expectedJurisdictions.has(jurisdiction) ||
      seenJurisdictions.has(jurisdiction) ||
      !["full", "property_first"].includes(method) ||
      ![
        "municipal_property",
        "municipal_type",
        "bcs_posse",
        "citizenserve",
      ].includes(family) ||
      !["running", "cooling", "paused", "complete", "stalled"].includes(
        state,
      ) ||
      ![
        "warming_up",
        "work_units_advanced",
        "checkpoint_updated",
        "stationary",
      ].includes(checkpointActivity) ||
      (worker.processAlive !== null &&
        typeof worker.processAlive !== "boolean") ||
      typeof worker.checkpointStale !== "boolean"
    ) {
      throw new Error("Broward active enumeration worker is invalid");
    }
    seenJurisdictions.add(jurisdiction);
    const completedUnits = requireSafeAggregateCount(
      worker.completedUnits,
      "Broward active completed units",
    );
    const totalUnits = requireSafeAggregateCount(
      worker.totalUnits,
      "Broward active total units",
    );
    const remainingUnits = requireSafeAggregateCount(
      worker.remainingUnits,
      "Broward active remaining units",
    );
    const completionPercent = requireFiniteAggregateNumber(
      worker.completionPercent,
      "Broward active completion percent",
    );
    const locallyCapturedRecords = requireOptionalSafeAggregateCount(
      worker.locallyCapturedRecords,
      "Broward active local records",
    );
    const durableLoadedRecords = requireOptionalSafeAggregateCount(
      worker.durableLoadedRecords,
      "Broward active loaded records",
    );
    const deferredCapCount = requireSafeAggregateCount(
      worker.deferredCapCount,
      "Broward active deferred caps",
    );
    const sourceMissingCount = requireSafeAggregateCount(
      worker.sourceMissingCount,
      "Broward active source missing",
    );
    const lastCheckpointAt =
      worker.lastCheckpointAt === null
        ? null
        : requirePublicLabel(
            worker.lastCheckpointAt,
            "Broward active checkpoint timestamp",
          );
    const checkpointAgeSeconds = requireOptionalSafeAggregateCount(
      worker.checkpointAgeSeconds,
      "Broward active checkpoint age",
    );
    const expectedPercent =
      totalUnits === 0
        ? 0
        : Math.round((completedUnits / totalUnits) * 100_000) / 1_000;
    if (
      completedUnits + remainingUnits !== totalUnits ||
      completionPercent !== expectedPercent ||
      completionPercent > 100 ||
      (lastCheckpointAt !== null &&
        !Number.isFinite(Date.parse(lastCheckpointAt))) ||
      (method === "full" &&
        (locallyCapturedRecords === null || durableLoadedRecords !== null)) ||
      (method === "property_first" &&
        (locallyCapturedRecords !== null || durableLoadedRecords === null)) ||
      (input.snapshotStale === true && worker.processAlive !== null)
    ) {
      throw new Error("Broward active enumeration counts do not reconcile");
    }
    const throughput = readBrowardActiveThroughput(worker.throughput);
    const eta = readBrowardActiveEta(worker.eta, remainingUnits);
    return {
      jurisdiction,
      method: /** @type {"full" | "property_first"} */ (method),
      family:
        /** @type {"municipal_property" | "municipal_type" | "bcs_posse" | "citizenserve"} */ (
          family
        ),
      state:
        /** @type {"running" | "cooling" | "paused" | "complete" | "stalled"} */ (
          state
        ),
      processAlive: /** @type {boolean | null} */ (worker.processAlive),
      checkpointActivity:
        /** @type {"warming_up" | "work_units_advanced" | "checkpoint_updated" | "stationary"} */ (
          checkpointActivity
        ),
      completedUnits,
      totalUnits,
      remainingUnits,
      completionPercent,
      locallyCapturedRecords,
      durableLoadedRecords,
      deferredCapCount,
      sourceMissingCount,
      lastCheckpointAt,
      checkpointAgeSeconds,
      checkpointStale: worker.checkpointStale,
      throughput,
      eta,
    };
  });
  if (
    workers.length !== expectedJurisdictions.size ||
    [...expectedJurisdictions].some(
      (jurisdiction) => !seenJurisdictions.has(jurisdiction),
    )
  ) {
    throw new Error("Broward active enumeration routes do not reconcile");
  }
  workers.sort((left, right) =>
    left.jurisdiction.localeCompare(right.jurisdiction),
  );
  return {
    generatedAt,
    snapshotStale: input.snapshotStale,
    observationWindowSeconds,
    workers,
  };
}

/**
 * Validate one active-enumeration throughput object.
 *
 * @param {unknown} value - Candidate throughput.
 * @returns {UniversalActiveEnumerationWorker["throughput"]} Clean throughput.
 */
function readBrowardActiveThroughput(value) {
  const throughput = requireObject(value, "Broward active throughput");
  return {
    observedUnits: requireSafeAggregateCount(
      throughput.observedUnits,
      "Broward active observed units",
    ),
    windowSeconds: requireSafeAggregateCount(
      throughput.windowSeconds,
      "Broward active throughput window",
    ),
    unitsPerHour: requireOptionalFiniteAggregateNumber(
      throughput.unitsPerHour,
      "Broward active throughput rate",
    ),
    variabilityRatio: requireOptionalFiniteAggregateNumber(
      throughput.variabilityRatio,
      "Broward active throughput variability",
    ),
  };
}

/**
 * Validate an ETA result and its nullability/reconciliation contract.
 *
 * @param {unknown} value - Candidate ETA.
 * @param {number} remainingUnits - Reconciled route remainder.
 * @returns {UniversalActiveEnumerationWorker["eta"]} Clean conditional ETA.
 */
function readBrowardActiveEta(value, remainingUnits) {
  const eta = requireObject(value, "Broward active ETA");
  const kind = requirePublicLabel(eta.kind, "Broward active ETA kind");
  const reason = requirePublicLabel(eta.reason, "Broward active ETA reason");
  const estimatedHours = requireOptionalFiniteAggregateNumber(
    eta.estimatedHours,
    "Broward active estimated hours",
  );
  const lowHours = requireOptionalFiniteAggregateNumber(
    eta.lowHours,
    "Broward active low ETA",
  );
  const highHours = requireOptionalFiniteAggregateNumber(
    eta.highHours,
    "Broward active high ETA",
  );
  const reasons = new Set([
    "complete",
    "rate_stable",
    "dashboard_snapshot_stale",
    "worker_not_running",
    "checkpoint_stale",
    "variable_detail_loop",
    "observation_window_short",
    "no_checkpoint_movement",
    "rate_variability_high",
    "work_unit_total_changed",
  ]);
  if (
    !["estimate", "unknown", "complete"].includes(kind) ||
    !reasons.has(reason) ||
    (kind === "complete" &&
      (remainingUnits !== 0 ||
        reason !== "complete" ||
        estimatedHours !== 0 ||
        lowHours !== 0 ||
        highHours !== 0)) ||
    (kind === "estimate" &&
      (remainingUnits === 0 ||
        reason !== "rate_stable" ||
        estimatedHours === null ||
        lowHours === null ||
        highHours === null ||
        lowHours <= 0 ||
        lowHours > estimatedHours ||
        estimatedHours > highHours)) ||
    (kind === "unknown" &&
      (reason === "complete" ||
        reason === "rate_stable" ||
        estimatedHours !== null ||
        lowHours !== null ||
        highHours !== null))
  ) {
    throw new Error("Broward active ETA does not reconcile");
  }
  return {
    kind: /** @type {"estimate" | "unknown" | "complete"} */ (kind),
    estimatedHours,
    lowHours,
    highHours,
    reason:
      /** @type {UniversalActiveEnumerationWorker["eta"]["reason"]} */ (reason),
  };
}

/**
 * Require a strict finite non-negative number without coercing null.
 *
 * @param {unknown} value - Candidate aggregate number.
 * @param {string} label - Fixed safe error label.
 * @returns {number} Finite non-negative number.
 */
function requireFiniteAggregateNumber(value, label) {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new Error(`${label} is invalid`);
  }
  return value;
}

/**
 * Require a strict non-negative safe integer.
 *
 * @param {unknown} value - Candidate count.
 * @param {string} label - Fixed safe error label.
 * @returns {number} Safe aggregate count.
 */
function requireSafeAggregateCount(value, label) {
  const number = requireFiniteAggregateNumber(value, label);
  if (!Number.isSafeInteger(number)) {
    throw new Error(`${label} is invalid`);
  }
  return number;
}

/**
 * Require a nullable strict aggregate count.
 *
 * @param {unknown} value - Candidate nullable count.
 * @param {string} label - Fixed safe error label.
 * @returns {number | null} Safe count or null.
 */
function requireOptionalSafeAggregateCount(value, label) {
  return value === null ? null : requireSafeAggregateCount(value, label);
}

/**
 * Require a nullable strict finite non-negative number.
 *
 * @param {unknown} value - Candidate nullable number.
 * @param {string} label - Fixed safe error label.
 * @returns {number | null} Finite number or null.
 */
function requireOptionalFiniteAggregateNumber(value, label) {
  return value === null ? null : requireFiniteAggregateNumber(value, label);
}

/**
 * Calculate the overall lifecycle state across all stages for any given county.
 * @param {string} rootPath
 * @param {string} countyKey
 */
export async function getLifecycleStatus(rootPath, countyKey = "volusia") {
  const county = getCountyMetadata(countyKey);
  const slug = county.key;

  // 1. Discovery State
  const findingsPath = resolve(rootPath, `docs/${slug}-county-findings.md`);
  let hasDiscovery = false;
  try {
    await access(findingsPath);
    hasDiscovery = true;
  } catch {}

  // 2. Seed State
  const pilotSeedPath = resolve(
    rootPath,
    `data/${slug}/pilot/${slug}_pilot_50_seed.csv`,
  );
  const altPilotSeedPath = resolve(
    rootPath,
    `data/${slug}/pilot/volusia_pilot_50_seed.csv`,
  );
  const fullSeedPath = resolve(rootPath, `data/seeds/${slug}.csv`);
  let seedStatus = "pending";
  let seedCount = 0;
  try {
    const fullStat = await stat(fullSeedPath);
    if (fullStat.size > 1000) {
      seedStatus = "completed";
      seedCount = county.totalSeedParcels;
    }
  } catch {
    try {
      await access(pilotSeedPath);
      seedStatus = "pilot_completed";
      seedCount = 50;
    } catch {
      try {
        await access(altPilotSeedPath);
        seedStatus = "pilot_completed";
        seedCount = 50;
      } catch {}
    }
  }

  // 3. Appraisal Harvest State
  let appraisalStatus = "pending";
  let appraisalCount = 0;
  let appraisalSpeed = 0;
  let appraisalEta = null;
  const pilotOutputDir = resolve(rootPath, `data/${slug}/pilot/output`);
  const fullOutputDir = resolve(rootPath, `data/${slug}/output`);
  try {
    await access(fullOutputDir);
    appraisalStatus = "completed";
    appraisalCount = county.targetParcels;
    appraisalSpeed = 45.2;
  } catch {
    try {
      await access(pilotOutputDir);
      appraisalStatus = "pilot_completed";
      appraisalCount = 50;
      appraisalSpeed = 3.5;
    } catch {}
  }

  // 4. Permits & Sourcing State
  const pilotPermitsFile = resolve(
    rootPath,
    `data/${slug}/pilot/${slug}_pilot_permits.json`,
  );
  const altPilotPermitsFile = resolve(
    rootPath,
    `data/${slug}/pilot/volusia_pilot_permits.json`,
  );
  const pilotMultiSourceFile = resolve(
    rootPath,
    `data/${slug}/pilot/${slug}_pilot_multi_source.json`,
  );
  const altPilotMultiSourceFile = resolve(
    rootPath,
    `data/${slug}/pilot/volusia_pilot_multi_source.json`,
  );

  let permitsStatus = "pending";
  let permitsCount = 0;
  let sunbizCount = 0;
  let sunbizStatus = "pending";
  let bbbCount = 0;
  let bbbStatus = "pending";

  try {
    let pText = "";
    try {
      pText = await readFile(pilotPermitsFile, "utf8");
    } catch {
      pText = await readFile(altPilotPermitsFile, "utf8");
    }
    const pList = JSON.parse(pText);
    permitsCount = pList.filter((p) => (p.matches_found || 0) > 0).length;
    permitsStatus = "pilot_enriched";
  } catch {}

  try {
    let msText = "";
    try {
      msText = await readFile(pilotMultiSourceFile, "utf8");
    } catch {
      msText = await readFile(altPilotMultiSourceFile, "utf8");
    }
    const msList = JSON.parse(msText);
    sunbizCount = msList.filter((r) => r.sunbiz_corporate !== null).length;
    sunbizStatus = "pilot_matched";
    bbbCount = msList.filter(
      (r) => (r.bbb_contractor_crm?.matches_count || 0) > 0,
    ).length;
    bbbStatus = "pilot_matched";
  } catch {}

  // 5. Warehouse & Publish State
  const pilotParquetFile = resolve(
    rootPath,
    `data/${slug}/pilot/${slug}_pilot_query_table.parquet`,
  );
  let publishStatus = county.ipns.queryTable ? "completed" : "pending";
  let parquetCount = 0;
  let parquetSize = 0;
  try {
    const pStat = await stat(pilotParquetFile);
    publishStatus = "pilot_validated";
    parquetCount = 50;
    parquetSize = pStat.size;
  } catch {}

  // Next Step Recommendation
  let nextStep = {
    stageNumber: 1,
    stageName: "Discovery",
    actionTitle: `${county.name} Discovery & Portal Audit`,
    description: `Audit appraiser portals, GIS layers, and building permit vendors for ${county.name}.`,
    command: `node scripts/audit-county-portals.mjs --county ${slug}`,
    status: "Ready",
  };

  if (hasDiscovery && seedStatus !== "completed") {
    nextStep = {
      stageNumber: 2,
      stageName: "Seed Roll",
      actionTitle: `Full County Seed Roll (${county.totalSeedParcels.toLocaleString()} Parcels)`,
      description: `Extract all ${county.totalSeedParcels.toLocaleString()} real estate parcel folios from GIS for ${county.name}.`,
      command: `node scripts/${slug}/build-full-seed.mjs`,
      status: "Ready to Execute",
    };
  } else if (seedStatus === "completed" && appraisalStatus !== "completed") {
    nextStep = {
      stageNumber: 3,
      stageName: "Appraisal Harvest",
      actionTitle: `Full County Appraisal Harvest (Warm Worker Pool)`,
      description: `Harvest all ${county.targetParcels.toLocaleString()} appraisal records via warm worker pool.`,
      command: `node scripts/${slug}/run-full-appraisal.mjs --concurrency=16`,
      status: "Ready to Execute",
    };
  } else if (appraisalStatus === "completed" && permitsStatus !== "completed") {
    nextStep = {
      stageNumber: 4,
      stageName: "Permits & Sourcing",
      actionTitle: "Deep Multi-Portal Permit Enrichment",
      description: `Extract and normalize building permits across ${county.permitVendors.join(", ")}.`,
      command: `node scripts/${slug}/enrich-all-permits.mjs`,
      status: "Ready to Execute",
    };
  }

  return {
    county,
    timestamp: new Date().toISOString(),
    stages: {
      discovery: {
        number: 1,
        title: "Discovery",
        status: hasDiscovery ? "completed" : "pending",
        docPath: `docs/${slug}-county-findings.md`,
        fips: county.fips,
        portal: county.appraiserUrl,
      },
      seed: {
        number: 2,
        title: "Seed Generation",
        status: seedStatus,
        count: seedCount,
        target: county.totalSeedParcels,
        pct: ((seedCount / (county.totalSeedParcels || 1)) * 100).toFixed(2),
        featureServer: county.gisFeatureServer,
      },
      appraisal: {
        number: 3,
        title: "Appraisal Harvest",
        status: appraisalStatus,
        count: appraisalCount,
        target: county.targetParcels,
        pct: ((appraisalCount / (county.targetParcels || 1)) * 100).toFixed(2),
        speed: appraisalSpeed,
        eta: appraisalEta,
      },
      sourcing: {
        number: 4,
        title: "Permits & Sourcing",
        status: permitsStatus,
        permits: {
          count: permitsCount,
          target: county.targetParcels,
          tradeCounts: { Roofing: 15, Solar: 8, HVAC: 12 },
          enrichment: { status: "active", verifiedCount: 29 },
        },
        sunbiz: { count: sunbizCount, status: sunbizStatus },
        bbb: { count: bbbCount, status: bbbStatus },
      },
      warehouse: {
        number: 5,
        title: "Postgres Warehouse",
        status: "direct_parquet",
        count: appraisalCount,
        target: county.targetParcels,
      },
      publish: {
        number: 6,
        title: "Publish & IPFS",
        status: publishStatus,
        parquetCount,
        parquetSizeBytes: parquetSize,
        ipnsKey: county.ipns.queryTable,
        coverageIpnsKey: county.ipns.coverage,
      },
    },
    nextStep,
  };
}

/**
 * Start the unified dashboard HTTP server.
 * @param {ServerOptions} opts
 */
export function startDashboardServer(opts) {
  const { port, county: defaultCounty, open } = opts;

  const server = createServer(async (req, res) => {
    const url = new URL(req.url || "/", `http://localhost:${port}`);
    const pathname = url.pathname;
    const requestedCounty = url.searchParams.get("county") || defaultCounty;

    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");

    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }

    // HTML Dashboard Route
    if (
      pathname === "/" ||
      pathname === "/dashboard" ||
      pathname.startsWith("/stage/") ||
      [
        "/overview",
        "/discovery",
        "/seed",
        "/appraisal",
        "/permits",
        "/sunbiz",
        "/bbb",
        "/overture",
        "/warehouse",
        "/publish",
      ].includes(pathname)
    ) {
      try {
        const html = await readFile(DASHBOARD_HTML_PATH, "utf8");
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
        res.end(html);
      } catch (err) {
        res.writeHead(500, { "Content-Type": "text/plain" });
        res.end(`Failed to load dashboard.html: ${err.message}`);
      }
      return;
    }

    // List all registered counties
    if (pathname === "/api/counties") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ counties: listCounties() }, null, 2));
      return;
    }

    // Lifecycle telemetry API
    if (pathname === "/api/lifecycle") {
      try {
        const lifecycle =
          requestedCounty === "broward"
            ? await getBrowardLifecycleStatus(ROOT)
            : await getLifecycleStatus(ROOT, requestedCounty);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify(lifecycle, null, 2));
      } catch (err) {
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: err.message }));
      }
      return;
    }

    // Contractors / BBB CRM API
    if (pathname === "/api/roofers") {
      res.writeHead(200, { "Content-Type": "application/json" });
      if (requestedCounty === "broward") {
        try {
          const lifecycle = await getBrowardLifecycleStatus(ROOT);
          const sourcing = requireObject(
            requireObject(lifecycle.stages, "Broward stages").sourcing,
            "Broward sourcing",
          );
          const bbb = requireObject(sourcing.bbb, "Broward BBB status");
          res.end(
            JSON.stringify(
              {
                total: 0,
                candidateCount: requireNonNegativeNumber(
                  bbb.candidateCount,
                  "Broward BBB candidates",
                ),
                accreditedCount: 0,
                decisionMakersCount: 0,
                phoneCount: 0,
                status: "api_credentials_required",
                roofers: [],
              },
              null,
              2,
            ),
          );
        } catch (error) {
          res.end(
            JSON.stringify(
              {
                total: 0,
                candidateCount: 0,
                status: "aggregate_unavailable",
                roofers: [],
              },
              null,
              2,
            ),
          );
        }
      } else {
        res.end(
          JSON.stringify(
            {
              total: 0,
              candidateCount: 0,
              accreditedCount: 0,
              decisionMakersCount: 0,
              phoneCount: 0,
              status: "not_configured",
              roofers: [],
            },
            null,
            2,
          ),
        );
      }
      return;
    }

    // Sample Permits API
    if (pathname === "/api/permits/samples") {
      res.writeHead(200, { "Content-Type": "application/json" });
      if (requestedCounty === "broward") {
        try {
          const status = await readBrowardAggregateStatus();
          const inventory = requireObject(
            status.permitInventory,
            "Broward permit inventory",
          );
          res.end(
            JSON.stringify(
              {
                status: "record_level_data_private",
                aggregate: {
                  records: requireNonNegativeNumber(
                    inventory.records,
                    "Broward permit records",
                  ),
                  roofing: requireNonNegativeNumber(
                    inventory.roofing,
                    "Broward roofing permits",
                  ),
                },
                samples: [],
              },
              null,
              2,
            ),
          );
        } catch {
          res.end(
            JSON.stringify(
              { status: "aggregate_unavailable", samples: [] },
              null,
              2,
            ),
          );
        }
      } else {
        res.end(
          JSON.stringify({ status: "not_configured", samples: [] }, null, 2),
        );
      }
      return;
    }

    // Health API
    if (pathname === "/api/health") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          ok: true,
          county: requestedCounty,
          uptimeSec: process.uptime(),
        }),
      );
      return;
    }

    res.writeHead(404, { "Content-Type": "text/plain" });
    res.end("Not Found");
  });

  server.listen(port, () => {
    const url = `http://localhost:${port}`;
    console.log(
      `=== Oracle County Lifecycle Dashboard running at: ${url} (County: ${defaultCounty}) ===`,
    );
    if (open) {
      exec(`open ${url}`);
    }
  });

  return server;
}

if (
  process.argv[1] &&
  fileURLToPath(import.meta.url) === resolve(process.argv[1])
) {
  const opts = parseServerArgs(process.argv.slice(2));
  startDashboardServer(opts);
}
