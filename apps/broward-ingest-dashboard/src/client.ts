import { createMockDashboardStatus } from "./shared/mock";
import type {
  DashboardCategoryCoverage,
  DashboardHealthState,
  DashboardStatus,
} from "./shared/status";

const REFRESH_INTERVAL_MS = 10_000;
const REQUEST_TIMEOUT_MS = 8_000;
const LOCAL_MOCK_MODE =
  import.meta.env.DEV && import.meta.env.VITE_DASHBOARD_MOCK_MODE === "true";
const numberFormatter = new Intl.NumberFormat();
const rateFormatter = new Intl.NumberFormat(undefined, {
  maximumFractionDigits: 2,
  minimumFractionDigits: 0,
});

/**
 * Set safe plain text on one known dashboard element.
 *
 * @param id - Fixed DOM element identifier.
 * @param value - Aggregate display text.
 */
function setText(id: string, value: string): void {
  const element = document.getElementById(id);
  if (element !== null) element.textContent = value;
}

/**
 * Format an aggregate count with locale separators.
 *
 * @param value - Non-negative aggregate integer.
 * @returns Human-readable integer.
 */
function formatCount(value: number): string {
  return numberFormatter.format(value);
}

/**
 * Format a permit aggregate while preserving missing durable evidence.
 *
 * @param value - Recorded count or null when no pilot status exists.
 * @returns Human-readable count or explicit unavailable label.
 */
function formatNullableCount(value: number | null): string {
  return value === null ? "Not recorded" : formatCount(value);
}

/**
 * Format a compact elapsed or estimated duration.
 *
 * @param seconds - Whole seconds or null when no reliable estimate exists.
 * @returns Compact duration or an explicit unavailable label.
 */
function formatDuration(seconds: number | null): string {
  if (seconds === null) return "Unavailable";
  const safeSeconds = Math.max(0, Math.round(seconds));
  if (safeSeconds < 60) return `${String(safeSeconds)}s`;
  const minutes = Math.round(safeSeconds / 60);
  if (minutes < 60) return `${String(minutes)}m`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `${String(hours)}h`;
  return `${String(Math.round(hours / 24))}d`;
}

/**
 * Format an ISO timestamp in the operator's local timezone.
 *
 * @param value - ISO timestamp or null.
 * @returns Local date/time or explicit unavailable label.
 */
function formatTimestamp(value: string | null): string {
  return value === null
    ? "Unavailable"
    : new Date(value).toLocaleString(undefined, {
        dateStyle: "medium",
        timeStyle: "medium",
      });
}

/**
 * Convert a camel-case Lexicon key to a readable label.
 *
 * @param category - Sanitized category key from the aggregate API.
 * @returns Text-only category label.
 */
function formatCategory(category: string): string {
  return category.replace(/([a-z0-9])([A-Z])/gu, "$1 $2");
}

/**
 * Render explicit writer health and an operational explanation.
 *
 * @param status - Validated aggregate status response.
 */
function renderHealth(status: DashboardStatus): void {
  const badge = document.getElementById("health-badge");
  const state = status.health.state;
  if (badge !== null) {
    badge.className = `health-badge ${state}`;
    badge.textContent = state.replace("_", " ");
  }
  const notice = document.getElementById("system-notice");
  if (notice === null) return;
  notice.className = `system-notice ${state}`;
  const descriptions: Record<DashboardHealthState, string> = {
    online:
      "Writer heartbeat is fresh. Durable aggregate progress is updating.",
    stale: `Stale: the last heartbeat is older than ${formatDuration(status.health.staleAfterSeconds)}. No fresh progress should be assumed.`,
    offline:
      "Offline: the writer is paused, failed, or no heartbeat is available for existing progress.",
    not_started:
      "Not started: the aggregate contract is initialized, but no writer heartbeat has been recorded.",
    complete:
      "Complete: all county rows have a durable success or terminal source-miss outcome.",
  };
  notice.textContent =
    status.dataSource === "mock"
      ? `Mock data — ${descriptions[state]}`
      : descriptions[state];
}

/**
 * Render category coverage using DOM text nodes only.
 *
 * @param categories - Sanitized aggregate category coverage.
 */
function renderCategoryCoverage(
  categories: readonly DashboardCategoryCoverage[],
): void {
  const list = document.getElementById("category-coverage");
  if (!(list instanceof HTMLOListElement)) return;
  list.replaceChildren();
  if (categories.length === 0) {
    const item = document.createElement("li");
    item.className = "empty-state";
    item.textContent = "No category aggregates have been checkpointed yet.";
    list.append(item);
    return;
  }
  const maximum = Math.max(...categories.map((entry) => entry.succeeded), 1);
  for (const entry of categories) {
    const item = document.createElement("li");
    item.className = "coverage-item";
    const label = document.createElement("span");
    label.className = "coverage-label";
    label.textContent = formatCategory(entry.category);
    const bar = document.createElement("progress");
    bar.className = "coverage-bar";
    bar.max = maximum;
    bar.value = entry.succeeded;
    bar.setAttribute(
      "aria-label",
      `${formatCategory(entry.category)} category relative coverage`,
    );
    const count = document.createElement("strong");
    count.className = "coverage-count";
    count.textContent = formatCount(entry.succeeded);
    count.title = `${String(entry.percentOfSucceeded)}% of successful records`;
    item.append(label, bar, count);
    list.append(item);
  }
}

/**
 * Render bounded permit-pilot evidence separately from county completeness.
 *
 * @param status - Validated aggregate status response.
 */
function renderPermitStatus(status: DashboardStatus): void {
  const permit = status.permit;
  setText("permit-pilot-state", permit.pilotState.replace("_", " "));
  setText(
    "permit-completeness",
    permit.countyCompleteness.replaceAll("_", " "),
  );
  setText("permit-recorded-at", formatTimestamp(permit.recordedAt));
  setText("permit-sample", formatNullableCount(permit.sampleParcels));
  setText(
    "permit-source-attempts",
    formatNullableCount(permit.permitSourceAttempts),
  );
  setText("permit-query-rows", formatNullableCount(permit.queryRows));
  setText(
    "permit-route-coverage",
    `${formatCount(permit.currentSourceImplemented)} implemented / ${formatCount(permit.currentSourceBlocked)} blocked`,
  );
  const notice = document.getElementById("permit-notice");
  if (notice !== null) {
    notice.textContent =
      permit.pilotState === "not_recorded"
        ? "No durable permit pilot evidence has been recorded; missing counts are not treated as zero."
        : permit.countyCompleteness === "complete"
          ? "All current jurisdiction routes and bounded pilot reconciliation gates are complete."
          : `Bounded pilot ${permit.pilotState}; countywide permit completeness is not established while ${formatCount(permit.currentSourceBlocked)} current routes remain blocked.`;
  }
}

/**
 * Render one complete aggregate status response.
 *
 * @param status - Validated API payload.
 */
function renderStatus(status: DashboardStatus): void {
  renderHealth(status);
  const { progress, throughput, health } = status;
  setText("completion-percent", `${progress.completionPercent.toFixed(3)}%`);
  setText(
    "completion-detail",
    `${formatCount(progress.completed)} complete of ${formatCount(progress.denominator)}`,
  );
  const progressElement = document.getElementById("completion-progress");
  if (progressElement instanceof HTMLProgressElement) {
    progressElement.value = progress.completionPercent;
    progressElement.setAttribute(
      "aria-valuetext",
      `${progress.completionPercent.toFixed(3)} percent complete`,
    );
  }
  setText("attempted", formatCount(progress.attempted));
  setText("succeeded", formatCount(progress.succeeded));
  setText("source-misses", formatCount(progress.sourceMisses));
  setText("source-failures", formatCount(progress.sourceFailures));
  setText("transform-failures", formatCount(progress.transformFailures));
  setText("load-failures", formatCount(progress.loadFailures));
  setText(
    "throughput",
    throughput.attemptedPerMinute === null
      ? "Unavailable"
      : `${rateFormatter.format(throughput.attemptedPerMinute)}/min`,
  );
  setText(
    "throughput-detail",
    `${formatCount(throughput.attemptedInWindow)} attempts over ${formatDuration(throughput.windowSeconds)}`,
  );
  setText("eta", formatDuration(throughput.etaSeconds));
  setText(
    "eta-detail",
    throughput.projectedCompletionAt === null
      ? "Requires a fresh heartbeat and positive recent rate"
      : `Continuous-run projection: ${formatTimestamp(throughput.projectedCompletionAt)}`,
  );
  setText("phase", status.phase.replace("_", " "));
  setText("heartbeat", formatTimestamp(health.lastHeartbeatAt));
  setText(
    "heartbeat-age",
    health.heartbeatAgeSeconds === null
      ? "Unavailable"
      : `${formatDuration(health.heartbeatAgeSeconds)} ago`,
  );
  setText("remaining", formatCount(progress.remaining));
  setText("denominator", formatCount(progress.denominator));
  setText("snapshot-time", `Snapshot ${formatTimestamp(status.generatedAt)}`);
  renderCategoryCoverage(status.categoryCoverage);
  renderPermitStatus(status);
}

/**
 * Mark the browser view offline after an API or validation failure.
 *
 * Previously rendered aggregate values remain visible but are explicitly
 * labeled as stale, preventing a transient outage from looking current.
 */
function renderServiceOffline(): void {
  const badge = document.getElementById("health-badge");
  if (badge !== null) {
    badge.className = "health-badge offline";
    badge.textContent = "offline";
  }
  const notice = document.getElementById("system-notice");
  if (notice !== null) {
    notice.className = "system-notice offline";
    notice.textContent =
      "Offline: the aggregate status service is unavailable. Displayed values may be outdated; retrying automatically.";
  }
}

/**
 * Fetch one no-store aggregate response or generate local mock data.
 *
 * @returns A structurally validated dashboard response.
 */
async function loadStatus(): Promise<DashboardStatus> {
  if (LOCAL_MOCK_MODE) return createMockDashboardStatus(Date.now());
  const response = await fetch("/api/status", {
    cache: "no-store",
    headers: { accept: "application/json" },
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  if (!response.ok) throw new Error("Aggregate status request failed");
  return parseDashboardStatus(await response.json());
}

/**
 * Refresh and render the dashboard once.
 */
async function refresh(): Promise<void> {
  try {
    renderStatus(await loadStatus());
  } catch {
    renderServiceOffline();
  }
}

/**
 * Validate the minimal public response shape before rendering.
 *
 * The API performs stronger server-side validation. This browser boundary
 * prevents malformed JSON from causing misleading metrics or DOM failures.
 *
 * @param value - Unknown JSON-decoded API response.
 * @returns The narrowed dashboard response.
 */
function parseDashboardStatus(value: unknown): DashboardStatus {
  if (!isRecord(value)) throw new Error("Status is not an object");
  const health = value.health;
  const progress = value.progress;
  const throughput = value.throughput;
  const categories = value.categoryCoverage;
  const permit = value.permit;
  if (
    value.schemaVersion !== 1 ||
    value.county !== "Broward" ||
    value.pipeline !== "Appraisal" ||
    !["neon", "mock"].includes(String(value.dataSource)) ||
    typeof value.phase !== "string" ||
    typeof value.generatedAt !== "string" ||
    !isRecord(health) ||
    !["online", "stale", "offline", "not_started", "complete"].includes(
      String(health.state),
    ) ||
    !isRecord(progress) ||
    ![
      progress.denominator,
      progress.attempted,
      progress.succeeded,
      progress.sourceMisses,
      progress.sourceFailures,
      progress.transformFailures,
      progress.loadFailures,
      progress.completed,
      progress.remaining,
      progress.completionPercent,
    ].every(isFiniteNumber) ||
    !isRecord(throughput) ||
    ![throughput.windowSeconds, throughput.attemptedInWindow].every(
      isFiniteNumber,
    ) ||
    !isNullableFiniteNumber(throughput.attemptedPerMinute) ||
    !isNullableFiniteNumber(throughput.etaSeconds) ||
    !Array.isArray(categories) ||
    !categories.every(isCategoryCoverage) ||
    !isRecord(permit) ||
    !["not_recorded", "passed", "failed"].includes(String(permit.pilotState)) ||
    !["not_established", "not_complete", "complete"].includes(
      String(permit.countyCompleteness),
    ) ||
    ![
      permit.sampleParcels,
      permit.appraisalResolved,
      permit.jurisdictionResolved,
      permit.jurisdictionUnresolved,
      permit.sourceUnavailableOutcomes,
      permit.permitSourceAttempts,
      permit.permitAttemptedParcels,
      permit.sourceFailures,
      permit.uniquePermitRecords,
      permit.queryRows,
    ].every(isNullableFiniteNumber) ||
    ![
      permit.registryJurisdictions,
      permit.currentSourceImplemented,
      permit.currentSourceBlocked,
    ].every(isFiniteNumber) ||
    ![
      permit.allInputParcelsTerminal,
      permit.allRecordsAccountedFor,
      permit.queryRowsMatchUniqueRecords,
    ].every(isNullableBoolean)
  ) {
    throw new Error("Status response has an invalid aggregate contract");
  }
  return value as unknown as DashboardStatus;
}

/**
 * Narrow an unknown value to a non-null object record.
 *
 * @param value - Candidate value.
 * @returns Whether the value is a record.
 */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Check a finite numeric aggregate.
 *
 * @param value - Candidate aggregate.
 * @returns Whether the value is a finite number.
 */
function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

/**
 * Check a nullable finite numeric aggregate.
 *
 * @param value - Candidate optional aggregate.
 * @returns Whether the value is null or a finite number.
 */
function isNullableFiniteNumber(value: unknown): value is number | null {
  return value === null || isFiniteNumber(value);
}

/**
 * Check a nullable aggregate reconciliation flag.
 *
 * @param value - Candidate optional boolean.
 * @returns Whether the value is null or boolean.
 */
function isNullableBoolean(value: unknown): value is boolean | null {
  return value === null || typeof value === "boolean";
}

/**
 * Validate one category coverage object.
 *
 * @param value - Candidate category value.
 * @returns Whether the value has text and finite aggregate fields.
 */
function isCategoryCoverage(
  value: unknown,
): value is DashboardCategoryCoverage {
  return (
    isRecord(value) &&
    typeof value.category === "string" &&
    isFiniteNumber(value.succeeded) &&
    isFiniteNumber(value.percentOfSucceeded)
  );
}

void refresh();
window.setInterval(() => {
  if (!document.hidden) void refresh();
}, REFRESH_INTERVAL_MS);
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) void refresh();
});
