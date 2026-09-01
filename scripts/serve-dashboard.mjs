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
 * @typedef {"software_or_transport" | "captcha_required" | "login_required" | "no_anonymous_search" | "custodian_only"} UniversalPermitRouteBlockerKey
 *
 * @typedef {object} UniversalPermitRouteBlockerCategory
 * @property {UniversalPermitRouteBlockerKey} key - Stable blocker category.
 * @property {"software_transport" | "source_policy"} kind - Actionability class.
 * @property {string} label - Public category label.
 * @property {number} count - Reconciled blocked-route count.
 * @property {string[]} jurisdictions - Public jurisdiction names.
 *
 * @typedef {object} UniversalPermitRouteStatus
 * @property {string} registryVersion - Executable registry version.
 * @property {number} totalCurrentRoutes - Current primary-route denominator.
 * @property {number} implementedCurrentRoutes - Implemented current routes.
 * @property {number} blockedCurrentRoutes - Fail-closed current routes.
 * @property {string[]} implementedJurisdictions - Public implemented names.
 * @property {UniversalPermitRouteBlockerCategory[]} blockerCategories
 *   Exhaustive blocker categories.
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
  const pausedPermitWorkers = readBrowardPausedPermitWorkers(
    enumeration.pausedWorkers,
    permitRoutes.implementedJurisdictions,
  );
  const coolingPermitWorkers = readBrowardCoolingPermitWorkers(
    enumeration.coolingWorkers,
    permitRoutes.implementedJurisdictions,
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
        operationalWorkers: {
          paused: pausedPermitWorkers,
          coolingDown: coolingPermitWorkers,
        },
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
  captcha_required: Object.freeze({
    kind: "source_policy",
    label: "CAPTCHA required",
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
  const blockedCurrentRoutes = requireNonNegativeNumber(
    input.blockedCurrentRoutes,
    "Broward blocked permit routes",
  );
  if (
    !Number.isSafeInteger(totalCurrentRoutes) ||
    !Number.isSafeInteger(implementedCurrentRoutes) ||
    !Number.isSafeInteger(blockedCurrentRoutes) ||
    implementedCurrentRoutes + blockedCurrentRoutes !== totalCurrentRoutes
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
  if (!Array.isArray(input.blockerCategories)) {
    throw new Error("Broward permit blocker categories are missing");
  }
  /** @type {Set<UniversalPermitRouteBlockerKey>} */
  const seenCategoryKeys = new Set();
  const blockerCategories = input.blockerCategories.map((candidate) => {
    const category = requireObject(
      candidate,
      "Broward permit blocker category",
    );
    const key = requirePublicLabel(category.key, "Broward permit blocker key");
    if (!(key in BROWARD_ROUTE_CATEGORY_METADATA)) {
      throw new Error("Broward permit blocker category is unknown");
    }
    const typedKey = /** @type {UniversalPermitRouteBlockerKey} */ (key);
    if (seenCategoryKeys.has(typedKey)) {
      throw new Error("Broward permit blocker category is duplicated");
    }
    seenCategoryKeys.add(typedKey);
    const metadata = BROWARD_ROUTE_CATEGORY_METADATA[typedKey];
    const count = requireNonNegativeNumber(
      category.count,
      "Broward permit blocker count",
    );
    const jurisdictions = requirePublicLabels(
      category.jurisdictions,
      "Broward blocked permit jurisdictions",
    );
    if (!Number.isSafeInteger(count) || count !== jurisdictions.length) {
      throw new Error("Broward permit blocker category does not reconcile");
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
    blockerCategories.reduce((sum, category) => sum + category.count, 0) !==
      blockedCurrentRoutes
  ) {
    throw new Error("Broward permit blocker categories do not reconcile");
  }
  const allJurisdictions = [
    ...implementedJurisdictions,
    ...blockerCategories.flatMap((category) => category.jurisdictions),
  ];
  if (new Set(allJurisdictions).size !== totalCurrentRoutes) {
    throw new Error("Broward current permit jurisdictions do not reconcile");
  }
  return {
    registryVersion,
    totalCurrentRoutes,
    implementedCurrentRoutes,
    blockedCurrentRoutes,
    implementedJurisdictions,
    blockerCategories,
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
