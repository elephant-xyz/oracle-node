/**
 * Pure planning and validation helpers for the monthly Overture places refresh.
 *
 * The workflow uses Neon `overture_place_extractions.run_status = 'succeeded'`
 * as the durable release source of truth. Overture STAC remains the source of
 * truth for the newest available release. All comparisons use parsed release
 * parts rather than lexical string ordering.
 */

import { HOSTED_SERVICE_REVIEW_LEAF_HINTS } from "./overture-places-lib.mjs";

/** @typedef {"added" | "data_changed" | "removed"} ProcessedChangeType */

/**
 * @typedef {object} ParsedReleaseId
 * @property {string} raw Original release identifier.
 * @property {number} year Four-digit release year.
 * @property {number} month One-based release month.
 * @property {number} day Release day.
 * @property {number} revision Numeric revision after the dot.
 */

/**
 * @typedef {object} RefreshInput
 * @property {string} county Lower-kebab county key.
 * @property {string} countyFips Five-digit county FIPS.
 * @property {string} boundarySource TIGER boundary token.
 * @property {string | null} releaseOverride Optional pinned release.
 * @property {number} costCeilingUsd Maximum approved estimated run cost.
 * @property {boolean} dryRun True when no mutation is permitted.
 * @property {string | null} runId Optional caller-provided run identifier.
 */

/**
 * @typedef {object} RefreshPlan
 * @property {"full" | "incremental" | "noop"} action Planned execution mode.
 * @property {string} county County key.
 * @property {string} countyFips County FIPS.
 * @property {string} boundarySource TIGER boundary token.
 * @property {string} release Pinned release for this run.
 * @property {string | null} previousRelease Last successfully published release.
 * @property {string} idempotencyKey Stable `(county, release)` key.
 * @property {number} estimatedCostUsd Conservative estimated AWS cost.
 * @property {number} costCeilingUsd Caller-approved ceiling.
 * @property {boolean} withinCostCeiling Whether the plan may proceed.
 * @property {boolean} dryRun Whether mutation is disabled.
 * @property {number} lockExpiresAt Epoch seconds used by the county lock.
 * @property {string} plannedAt ISO timestamp.
 * @property {string} reason Human-readable plan decision.
 */

/**
 * @typedef {object} ChangelogRow
 * @property {string} id GERS identifier.
 * @property {string} changeType Raw changelog partition value.
 */

/**
 * @typedef {object} ClassifiedPlaceChanges
 * @property {string[]} activeIds Added/data-changed IDs currently inside the county.
 * @property {string[]} addedIds New IDs currently inside the county.
 * @property {string[]} updatedIds Existing IDs still inside the county.
 * @property {string[]} movedInIds Data-changed IDs newly assigned to the county by geometry.
 * @property {string[]} deactivateIds Existing IDs removed or moved outside the county.
 * @property {string[]} removedIds Existing IDs explicitly removed from the release.
 * @property {string[]} movedOutIds Existing data-changed IDs no longer inside the county.
 * @property {Record<ProcessedChangeType, number>} counts Counts written to the run record.
 */

/**
 * @typedef {object} TaxonomyDriftReport
 * @property {string} release Current Overture release.
 * @property {string | null} previousRelease Previous successful release.
 * @property {boolean} quarterlyRelease True for March/June/September/December.
 * @property {string[]} configuredPaths Committed full hosted-service paths.
 * @property {string[]} missingConfiguredPaths Configured paths absent from the county release.
 * @property {{configuredPath: string, observedPaths: string[]}[]} repathedConfiguredPaths Same leaf under a different path.
 * @property {string[]} unresolvedReviewCandidates Hosted-looking paths not covered by committed rules.
 * @property {boolean} blocking Whether publication requires human taxonomy review.
 * @property {string[]} reasons Blocking reasons.
 */

/** Columns documented and observed in Overture changelog Parquet. */
export const REQUIRED_CHANGELOG_COLUMNS = Object.freeze([
  "id",
  "bbox",
  "change_type",
  "theme",
  "type",
]);

/** Change partitions processed by refreshes. `unchanged` is intentionally absent. */
export const PROCESSED_CHANGE_TYPES = Object.freeze([
  "added",
  "data_changed",
  "removed",
]);

const COUNTY_KEY_PATTERN = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;
const FIPS_PATTERN = /^\d{5}$/;
const BOUNDARY_PATTERN = /^tiger\/tl_\d{4}_us_county$/;
const RELEASE_PATTERN = /^(\d{4})-(\d{2})-(\d{2})\.(\d+)$/;

/**
 * Parse and validate one Overture release identifier.
 *
 * @param {string} release Overture release (`YYYY-MM-DD.N`).
 * @returns {ParsedReleaseId} Numeric parts suitable for ordering.
 */
export function parseOvertureReleaseId(release) {
  const match = RELEASE_PATTERN.exec(release.trim());
  if (
    match === null ||
    match[1] === undefined ||
    match[2] === undefined ||
    match[3] === undefined ||
    match[4] === undefined
  ) {
    throw new Error(`Invalid Overture release id: ${release}`);
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const revision = Number(match[4]);
  const instant = new Date(Date.UTC(year, month - 1, day));
  if (
    instant.getUTCFullYear() !== year ||
    instant.getUTCMonth() !== month - 1 ||
    instant.getUTCDate() !== day
  ) {
    throw new Error(`Invalid Overture release date: ${release}`);
  }
  return { raw: release.trim(), year, month, day, revision };
}

/**
 * Compare two Overture releases chronologically.
 *
 * @param {string} left Left release id.
 * @param {string} right Right release id.
 * @returns {-1 | 0 | 1} Ordering result.
 */
export function compareOvertureReleases(left, right) {
  const a = parseOvertureReleaseId(left);
  const b = parseOvertureReleaseId(right);
  const aDate = Date.UTC(a.year, a.month - 1, a.day);
  const bDate = Date.UTC(b.year, b.month - 1, b.day);
  if (aDate < bDate) return -1;
  if (aDate > bDate) return 1;
  if (a.revision < b.revision) return -1;
  if (a.revision > b.revision) return 1;
  return 0;
}

/**
 * Validate untrusted manual/scheduled workflow input.
 *
 * `costCeilingUsd` is mandatory. A disabled schedule is initially deployed with
 * zero, so enabling it alone cannot authorize a cost-bearing refresh.
 *
 * @param {unknown} value Untrusted workflow input.
 * @returns {RefreshInput} Validated input.
 */
export function parseRefreshInput(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Refresh input must be a JSON object");
  }
  const record = /** @type {Record<string, unknown>} */ (value);
  const county = readString(record.county);
  const countyFips = readString(record.countyFips);
  const boundarySource = readString(record.boundarySource);
  const releaseOverride = readNullableString(record.releaseOverride);
  const costCeilingUsd = readFiniteNumber(record.costCeilingUsd);
  const dryRun = record.dryRun === true;
  const runId = readNullableString(record.runId);
  if (county === null || !COUNTY_KEY_PATTERN.test(county)) {
    throw new Error("county must be normalized lower-kebab");
  }
  if (countyFips === null || !FIPS_PATTERN.test(countyFips)) {
    throw new Error("countyFips must be five digits");
  }
  if (boundarySource === null || !BOUNDARY_PATTERN.test(boundarySource)) {
    throw new Error("boundarySource must look like tiger/tl_2024_us_county");
  }
  if (releaseOverride !== null) parseOvertureReleaseId(releaseOverride);
  if (costCeilingUsd === null || costCeilingUsd < 0) {
    throw new Error("costCeilingUsd must be a finite non-negative number");
  }
  return {
    county,
    countyFips,
    boundarySource,
    releaseOverride,
    costCeilingUsd,
    dryRun,
    runId,
  };
}

/**
 * Estimate one serial Fargate workflow run.
 *
 * The estimate deliberately rounds up. It uses the largest configured
 * 4-vCPU/16-GiB task size across the serial stage runtime,
 * S3 request/data overhead, Step Functions transitions, and a safety factor.
 * Filebase storage is outside AWS and is reported separately by the publisher.
 *
 * @param {"full" | "incremental"} mode Processing mode.
 * @param {number} [estimatedChangedRows] Optional changed-row estimate.
 * @returns {number} Estimated AWS cost in USD, rounded to cents.
 */
export function estimateRefreshCostUsd(mode, estimatedChangedRows = 40_191) {
  const changedRows = Math.max(0, estimatedChangedRows);
  const taskMinutes =
    mode === "full"
      ? 150
      : 45 + Math.min(90, Math.ceil(changedRows / 2_000) * 3);
  const vCpuHours = (taskMinutes / 60) * 4;
  const gibHours = (taskMinutes / 60) * 16;
  // us-east-1 Linux/x86 Fargate public rates, intentionally rounded upward.
  const fargate = vCpuHours * 0.041 + gibHours * 0.0046;
  const requestsAndTransitions = 0.15;
  const transferSafetyAllowance = mode === "full" ? 0.75 : 0.35;
  const estimate =
    (fargate + requestsAndTransitions + transferSafetyAllowance) * 1.25;
  return Math.ceil(estimate * 100) / 100;
}

/**
 * Resolve a pinned plan from STAC and durable successful state.
 *
 * @param {object} params Planning inputs.
 * @param {RefreshInput} params.input Validated workflow input.
 * @param {string} params.latestRelease Latest STAC release.
 * @param {string | null} params.lastSuccessfulRelease Latest Neon run with `run_status=succeeded`.
 * @param {Date} [params.now] Clock used for deterministic tests.
 * @returns {RefreshPlan} Read-only plan.
 */
export function buildRefreshPlan(params) {
  const now = params.now ?? new Date();
  const release = params.input.releaseOverride ?? params.latestRelease;
  parseOvertureReleaseId(release);
  parseOvertureReleaseId(params.latestRelease);
  const previousRelease = params.lastSuccessfulRelease;
  if (previousRelease !== null) {
    const ordering = compareOvertureReleases(release, previousRelease);
    if (ordering < 0) {
      throw new Error(
        `Refusing release rollback from ${previousRelease} to ${release}`,
      );
    }
    if (ordering === 0) {
      return {
        action: "noop",
        county: params.input.county,
        countyFips: params.input.countyFips,
        boundarySource: params.input.boundarySource,
        release,
        previousRelease,
        idempotencyKey: `${params.input.county}:${release}`,
        estimatedCostUsd: 0,
        costCeilingUsd: params.input.costCeilingUsd,
        withinCostCeiling: true,
        dryRun: params.input.dryRun,
        lockExpiresAt: Math.floor(now.getTime() / 1000) + 6 * 60 * 60,
        plannedAt: now.toISOString(),
        reason: `latest release ${release} is already successfully published`,
      };
    }
  }
  const action = previousRelease === null ? "full" : "incremental";
  const estimatedCostUsd = estimateRefreshCostUsd(action);
  return {
    action,
    county: params.input.county,
    countyFips: params.input.countyFips,
    boundarySource: params.input.boundarySource,
    release,
    previousRelease,
    idempotencyKey: `${params.input.county}:${release}`,
    estimatedCostUsd,
    costCeilingUsd: params.input.costCeilingUsd,
    withinCostCeiling: estimatedCostUsd <= params.input.costCeilingUsd,
    dryRun: params.input.dryRun,
    lockExpiresAt: Math.floor(now.getTime() / 1000) + 6 * 60 * 60,
    plannedAt: now.toISOString(),
    reason:
      action === "full"
        ? `no successful release exists; full extraction required for ${release}`
        : `new release ${release} follows ${previousRelease}`,
  };
}

/**
 * Build the three public changelog partition paths used by a refresh.
 *
 * @param {string} release Pinned Overture release.
 * @returns {string[]} Added/data_changed/removed S3 globs.
 */
export function overturePlacesChangelogGlobs(release) {
  parseOvertureReleaseId(release);
  return PROCESSED_CHANGE_TYPES.map(
    (changeType) =>
      `s3://overturemaps-us-west-2/changelog/${release}/theme=places/type=place/change_type=${changeType}/*`,
  );
}

/**
 * Validate a live DuckDB `DESCRIBE` result for the changelog.
 *
 * @param {readonly string[]} columns Column names returned by the public Parquet schema.
 * @returns {{passed: true, columns: string[]}} Validated schema record.
 */
export function assertOvertureChangelogSchema(columns) {
  const normalized = [
    ...new Set(columns.map((column) => column.trim()).filter(Boolean)),
  ];
  const missing = REQUIRED_CHANGELOG_COLUMNS.filter(
    (column) => !normalized.includes(column),
  );
  if (missing.length > 0) {
    throw new Error(
      `Overture changelog schema is missing required column(s): ${missing.join(", ")}`,
    );
  }
  return { passed: true, columns: normalized };
}

/**
 * Classify relevant changelog IDs using geometry-derived current membership.
 *
 * A `data_changed` ID currently inside the county is an update or move-in. A
 * previously-current ID that is `removed`, or `data_changed` and no longer
 * inside, is deactivated. Rows are never deleted and disappearance is not
 * interpreted as closure.
 *
 * @param {object} params Change membership inputs.
 * @param {readonly string[]} params.existingCurrentIds Current Neon county IDs before load.
 * @param {readonly string[]} params.currentCountyIds Current-release IDs after bbox + `ST_Within`.
 * @param {readonly ChangelogRow[]} params.changelogRows Relevant changelog rows.
 * @returns {ClassifiedPlaceChanges} Deterministic classifications.
 */
export function classifyPlaceChanges(params) {
  const existing = new Set(params.existingCurrentIds);
  const current = new Set(params.currentCountyIds);
  /** @type {Map<string, ProcessedChangeType>} */
  const changes = new Map();
  for (const row of params.changelogRows) {
    if (!PROCESSED_CHANGE_TYPES.includes(row.changeType)) {
      throw new Error(`Unsupported Overture change_type: ${row.changeType}`);
    }
    changes.set(row.id, /** @type {ProcessedChangeType} */ (row.changeType));
  }
  /** @type {string[]} */
  const addedIds = [];
  /** @type {string[]} */
  const updatedIds = [];
  /** @type {string[]} */
  const movedInIds = [];
  /** @type {string[]} */
  const removedIds = [];
  /** @type {string[]} */
  const movedOutIds = [];
  for (const [id, changeType] of changes) {
    if (current.has(id) && changeType === "added") addedIds.push(id);
    if (current.has(id) && changeType === "data_changed") {
      if (existing.has(id)) updatedIds.push(id);
      else movedInIds.push(id);
    }
    if (!existing.has(id)) continue;
    if (changeType === "removed") removedIds.push(id);
    if (changeType === "data_changed" && !current.has(id)) movedOutIds.push(id);
  }
  const sort = (values) =>
    [...new Set(values)].sort((a, b) => a.localeCompare(b));
  const activeIds = sort([...addedIds, ...updatedIds, ...movedInIds]);
  const deactivateIds = sort([...removedIds, ...movedOutIds]);
  return {
    activeIds,
    addedIds: sort(addedIds),
    updatedIds: sort(updatedIds),
    movedInIds: sort(movedInIds),
    deactivateIds,
    removedIds: sort(removedIds),
    movedOutIds: sort(movedOutIds),
    counts: {
      added: sort(addedIds).length,
      data_changed: sort([...updatedIds, ...movedInIds, ...movedOutIds]).length,
      removed: sort(removedIds).length,
    },
  };
}

/**
 * Compare current taxonomy paths with committed full-path hosted-service rules.
 *
 * @param {object} params Drift inputs.
 * @param {string} params.release Current release.
 * @param {string | null} params.previousRelease Previous successful release.
 * @param {readonly string[]} params.currentPaths Distinct full hierarchy paths in current county data.
 * @param {readonly string[]} params.configuredPaths Committed hosted-service paths.
 * @returns {TaxonomyDriftReport} Human-review gate report.
 */
export function buildTaxonomyDriftReport(params) {
  const release = parseOvertureReleaseId(params.release);
  const quarterlyRelease = [3, 6, 9, 12].includes(release.month);
  const currentPaths = uniqueSorted(params.currentPaths);
  const configuredPaths = uniqueSorted(params.configuredPaths);
  const currentSet = new Set(currentPaths);
  const configuredSet = new Set(configuredPaths);
  const missingConfiguredPaths = configuredPaths.filter(
    (path) => !currentSet.has(path),
  );
  const repathedConfiguredPaths = missingConfiguredPaths.flatMap(
    (configuredPath) => {
      const leaf = taxonomyLeaf(configuredPath);
      const observedPaths = currentPaths.filter(
        (path) => taxonomyLeaf(path) === leaf && path !== configuredPath,
      );
      return observedPaths.length === 0
        ? []
        : [{ configuredPath, observedPaths }];
    },
  );
  const unresolvedReviewCandidates = currentPaths.filter(
    (path) =>
      !configuredSet.has(path) &&
      HOSTED_SERVICE_REVIEW_LEAF_HINTS.some((hint) =>
        taxonomyLeaf(path).includes(hint),
      ),
  );
  /** @type {string[]} */
  const reasons = [];
  if (missingConfiguredPaths.length > 0) {
    reasons.push(
      `${missingConfiguredPaths.length} configured hosted-service path(s) disappeared`,
    );
  }
  if (repathedConfiguredPaths.length > 0) {
    reasons.push(
      `${repathedConfiguredPaths.length} configured hosted-service path(s) were repathed`,
    );
  }
  if (quarterlyRelease && unresolvedReviewCandidates.length > 0) {
    reasons.push(
      `${unresolvedReviewCandidates.length} hosted-service candidate path(s) require quarterly review`,
    );
  }
  return {
    release: params.release,
    previousRelease: params.previousRelease,
    quarterlyRelease,
    configuredPaths,
    missingConfiguredPaths,
    repathedConfiguredPaths,
    unresolvedReviewCandidates,
    blocking: reasons.length > 0,
    reasons,
  };
}

/**
 * Return whether a place is explicitly closed from source fields.
 *
 * Removal/deactivation is deliberately ignored. Closure comes only from
 * `operating_status` or Overture's documented confidence-zero signal.
 *
 * @param {unknown} operatingStatus Source operating status.
 * @param {unknown} confidence Source confidence.
 * @returns {boolean} True only for explicit closure evidence.
 */
export function isExplicitlyClosed(operatingStatus, confidence) {
  return (
    operatingStatus === "permanently_closed" ||
    (typeof confidence === "number" && confidence === 0)
  );
}

/**
 * @param {unknown} value Unknown scalar.
 * @returns {string | null} Trimmed non-empty string.
 */
function readString(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/**
 * @param {unknown} value Unknown optional scalar.
 * @returns {string | null} Trimmed string or null.
 */
function readNullableString(value) {
  if (value === undefined || value === null || value === "") return null;
  return readString(value);
}

/**
 * @param {unknown} value Unknown numeric scalar.
 * @returns {number | null} Finite number or null.
 */
function readFiniteNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

/**
 * @param {string} path Full taxonomy path.
 * @returns {string} Final hierarchy segment.
 */
function taxonomyLeaf(path) {
  const segments = path.split("/").filter(Boolean);
  return segments[segments.length - 1] ?? "";
}

/**
 * @param {readonly string[]} values Input values.
 * @returns {string[]} Sorted unique non-empty values.
 */
function uniqueSorted(values) {
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))].sort(
    (a, b) => a.localeCompare(b),
  );
}
