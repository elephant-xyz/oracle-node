export const BROWARD_DENOMINATOR = 534_309;
export const DASHBOARD_SCHEMA_VERSION = 1 as const;
export const DASHBOARD_PIPELINE_KEY = "broward-appraisal" as const;

export type DashboardPhase =
  | "not_started"
  | "pilot"
  | "capturing"
  | "transforming"
  | "loading"
  | "verifying"
  | "full"
  | "paused"
  | "failed"
  | "complete";

export type DashboardHealthState =
  | "online"
  | "stale"
  | "offline"
  | "not_started"
  | "complete";

export type DashboardDataSource = "neon" | "mock";

export interface CategorySnapshot {
  readonly categoryKey: string;
  readonly succeededCount: number;
}

export interface PermitStatusSnapshot {
  readonly recordedAt: string | null;
  readonly sampleParcels: number | null;
  readonly appraisalResolved: number | null;
  readonly jurisdictionResolved: number | null;
  readonly jurisdictionUnresolved: number | null;
  readonly sourceUnavailableOutcomes: number | null;
  readonly permitSourceAttempts: number | null;
  readonly permitAttemptedParcels: number | null;
  readonly sourceFailures: number | null;
  readonly uniquePermitRecords: number | null;
  readonly queryRows: number | null;
  readonly allInputParcelsTerminal: boolean | null;
  readonly allRecordsAccountedFor: boolean | null;
  readonly queryRowsMatchUniqueRecords: boolean | null;
  readonly localPilotPassed: boolean | null;
  readonly countyPermitComplete: boolean | null;
  readonly registryJurisdictions: number;
  readonly currentSourceImplemented: number;
  readonly currentSourceBlocked: number;
}

/**
 * Aggregate-only database values used to create a public dashboard response.
 *
 * This contract deliberately has no identifier, address, owner, source
 * payload, artifact path, error text, endpoint, or connection-string field.
 * Failure counters are cumulative attempts and can overlap rows that
 * subsequently succeeded.
 */
export interface StatusSnapshot {
  readonly attempted: number;
  readonly categories: readonly CategorySnapshot[];
  readonly denominator: number;
  readonly heartbeatAt: string | null;
  readonly loadFailures: number;
  readonly phase: DashboardPhase;
  readonly sourceFailures: number;
  readonly sourceMisses: number;
  readonly staleAfterSeconds: number;
  readonly startedAt: string | null;
  readonly succeeded: number;
  readonly permit: PermitStatusSnapshot;
  readonly throughputAttempted: number;
  readonly throughputWindowSeconds: number;
  readonly transformFailures: number;
}

export interface DashboardCategoryCoverage {
  readonly category: string;
  readonly percentOfSucceeded: number;
  readonly succeeded: number;
}

export interface DashboardStatus {
  readonly schemaVersion: typeof DASHBOARD_SCHEMA_VERSION;
  readonly generatedAt: string;
  readonly county: "Broward";
  readonly pipeline: "Appraisal";
  readonly dataSource: DashboardDataSource;
  readonly phase: DashboardPhase;
  readonly health: {
    readonly state: DashboardHealthState;
    readonly lastHeartbeatAt: string | null;
    readonly heartbeatAgeSeconds: number | null;
    readonly staleAfterSeconds: number;
  };
  readonly progress: {
    readonly denominator: number;
    readonly attempted: number;
    readonly succeeded: number;
    readonly sourceMisses: number;
    readonly sourceFailures: number;
    readonly transformFailures: number;
    readonly loadFailures: number;
    readonly completed: number;
    readonly remaining: number;
    readonly completionPercent: number;
  };
  readonly throughput: {
    readonly windowSeconds: number;
    readonly attemptedInWindow: number;
    readonly attemptedPerMinute: number | null;
    readonly etaSeconds: number | null;
    readonly projectedCompletionAt: string | null;
  };
  readonly categoryCoverage: readonly DashboardCategoryCoverage[];
  readonly permit: {
    readonly pilotState: "not_recorded" | "passed" | "failed";
    readonly countyCompleteness:
      | "not_established"
      | "not_complete"
      | "complete";
    readonly recordedAt: string | null;
    readonly sampleParcels: number | null;
    readonly appraisalResolved: number | null;
    readonly jurisdictionResolved: number | null;
    readonly jurisdictionUnresolved: number | null;
    readonly sourceUnavailableOutcomes: number | null;
    readonly permitSourceAttempts: number | null;
    readonly permitAttemptedParcels: number | null;
    readonly sourceFailures: number | null;
    readonly uniquePermitRecords: number | null;
    readonly queryRows: number | null;
    readonly allInputParcelsTerminal: boolean | null;
    readonly allRecordsAccountedFor: boolean | null;
    readonly queryRowsMatchUniqueRecords: boolean | null;
    readonly registryJurisdictions: number;
    readonly currentSourceImplemented: number;
    readonly currentSourceBlocked: number;
  };
}

const CATEGORY_KEY_PATTERN = /^[A-Za-z][A-Za-z0-9]{0,63}$/u;
const NON_RUNNING_PHASES: ReadonlySet<DashboardPhase> = new Set([
  "paused",
  "failed",
]);

/**
 * Build the stable, privacy-safe response returned by the status endpoint.
 *
 * Completion uses durable successes plus terminal source misses. Retryable
 * source, transform, and load failures remain visible but never advance the
 * completion percentage. ETA is emitted only while a fresh writer heartbeat
 * proves that the pipeline is online.
 *
 * @param snapshot - Validated aggregate counters read from Neon.
 * @param nowMs - Current Unix epoch time in milliseconds.
 * @param dataSource - Whether the values came from Neon or local mock mode.
 * @returns A complete API response containing aggregate operational data only.
 */
export function buildDashboardStatus(
  snapshot: StatusSnapshot,
  nowMs: number,
  dataSource: DashboardDataSource = "neon",
): DashboardStatus {
  validateSnapshot(snapshot);
  if (!Number.isFinite(nowMs) || nowMs < 0) {
    throw new Error("Dashboard clock is invalid");
  }

  const completed = Math.min(
    snapshot.denominator,
    snapshot.succeeded + snapshot.sourceMisses,
  );
  const remaining = Math.max(0, snapshot.denominator - completed);
  const heartbeatMs =
    snapshot.heartbeatAt === null ? null : Date.parse(snapshot.heartbeatAt);
  const heartbeatAgeSeconds =
    heartbeatMs === null
      ? null
      : Math.max(0, Math.floor((nowMs - heartbeatMs) / 1_000));
  const healthState = determineHealthState({
    attempted: snapshot.attempted,
    completed,
    denominator: snapshot.denominator,
    heartbeatAgeSeconds,
    phase: snapshot.phase,
    staleAfterSeconds: snapshot.staleAfterSeconds,
  });
  const attemptedPerMinute =
    snapshot.throughputWindowSeconds > 0 && snapshot.throughputAttempted > 0
      ? round(
          snapshot.throughputAttempted /
            (snapshot.throughputWindowSeconds / 60),
          2,
        )
      : null;
  const etaSeconds =
    completed >= snapshot.denominator
      ? 0
      : healthState === "online" &&
          attemptedPerMinute !== null &&
          attemptedPerMinute > 0
        ? Math.round((remaining / attemptedPerMinute) * 60)
        : null;

  return {
    schemaVersion: DASHBOARD_SCHEMA_VERSION,
    generatedAt: new Date(nowMs).toISOString(),
    county: "Broward",
    pipeline: "Appraisal",
    dataSource,
    phase: snapshot.phase,
    health: {
      state: healthState,
      lastHeartbeatAt: snapshot.heartbeatAt,
      heartbeatAgeSeconds,
      staleAfterSeconds: snapshot.staleAfterSeconds,
    },
    progress: {
      denominator: snapshot.denominator,
      attempted: snapshot.attempted,
      succeeded: snapshot.succeeded,
      sourceMisses: snapshot.sourceMisses,
      sourceFailures: snapshot.sourceFailures,
      transformFailures: snapshot.transformFailures,
      loadFailures: snapshot.loadFailures,
      completed,
      remaining,
      completionPercent: round(
        Math.min(1, completed / snapshot.denominator) * 100,
        3,
      ),
    },
    throughput: {
      windowSeconds: snapshot.throughputWindowSeconds,
      attemptedInWindow: snapshot.throughputAttempted,
      attemptedPerMinute,
      etaSeconds,
      projectedCompletionAt:
        etaSeconds === null
          ? null
          : new Date(nowMs + etaSeconds * 1_000).toISOString(),
    },
    categoryCoverage: sanitizeCategoryCoverage(
      snapshot.categories,
      snapshot.succeeded,
    ),
    permit: buildPermitStatus(snapshot.permit),
  };
}

/**
 * Validate and expose aggregate permit-pilot evidence without converting an
 * absent durable status row into misleading zero counts.
 *
 * @param permit - Aggregate permit control and optional pilot status values.
 * @returns Public permit pilot and county-completeness status.
 */
function buildPermitStatus(
  permit: PermitStatusSnapshot,
): DashboardStatus["permit"] {
  for (const [name, value] of [
    ["registryJurisdictions", permit.registryJurisdictions],
    ["currentSourceImplemented", permit.currentSourceImplemented],
    ["currentSourceBlocked", permit.currentSourceBlocked],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new Error(`Invalid permit aggregate: ${name}`);
    }
  }
  if (
    permit.currentSourceImplemented + permit.currentSourceBlocked !==
    permit.registryJurisdictions
  ) {
    throw new Error("Permit route aggregates do not reconcile");
  }

  const nullableCounts = [
    ["sampleParcels", permit.sampleParcels],
    ["appraisalResolved", permit.appraisalResolved],
    ["jurisdictionResolved", permit.jurisdictionResolved],
    ["jurisdictionUnresolved", permit.jurisdictionUnresolved],
    ["sourceUnavailableOutcomes", permit.sourceUnavailableOutcomes],
    ["permitSourceAttempts", permit.permitSourceAttempts],
    ["permitAttemptedParcels", permit.permitAttemptedParcels],
    ["sourceFailures", permit.sourceFailures],
    ["uniquePermitRecords", permit.uniquePermitRecords],
    ["queryRows", permit.queryRows],
  ] as const;
  const optionalValues = [
    ...nullableCounts.map((entry) => entry[1]),
    permit.allInputParcelsTerminal,
    permit.allRecordsAccountedFor,
    permit.queryRowsMatchUniqueRecords,
    permit.localPilotPassed,
    permit.countyPermitComplete,
  ];
  const publicFields = {
    recordedAt: permit.recordedAt,
    sampleParcels: permit.sampleParcels,
    appraisalResolved: permit.appraisalResolved,
    jurisdictionResolved: permit.jurisdictionResolved,
    jurisdictionUnresolved: permit.jurisdictionUnresolved,
    sourceUnavailableOutcomes: permit.sourceUnavailableOutcomes,
    permitSourceAttempts: permit.permitSourceAttempts,
    permitAttemptedParcels: permit.permitAttemptedParcels,
    sourceFailures: permit.sourceFailures,
    uniquePermitRecords: permit.uniquePermitRecords,
    queryRows: permit.queryRows,
    allInputParcelsTerminal: permit.allInputParcelsTerminal,
    allRecordsAccountedFor: permit.allRecordsAccountedFor,
    queryRowsMatchUniqueRecords: permit.queryRowsMatchUniqueRecords,
    registryJurisdictions: permit.registryJurisdictions,
    currentSourceImplemented: permit.currentSourceImplemented,
    currentSourceBlocked: permit.currentSourceBlocked,
  };
  if (permit.recordedAt === null) {
    if (optionalValues.some((value) => value !== null)) {
      throw new Error("Unrecorded permit pilot contains inferred aggregates");
    }
    return {
      ...publicFields,
      pilotState: "not_recorded",
      countyCompleteness: "not_established",
    };
  }
  if (!Number.isFinite(Date.parse(permit.recordedAt))) {
    throw new Error("Invalid permit aggregate: recordedAt");
  }
  for (const [name, value] of nullableCounts) {
    if (value === null || !Number.isSafeInteger(value) || value < 0) {
      throw new Error(`Invalid permit aggregate: ${name}`);
    }
  }
  if (
    permit.allInputParcelsTerminal === null ||
    permit.allRecordsAccountedFor === null ||
    permit.queryRowsMatchUniqueRecords === null ||
    permit.localPilotPassed === null ||
    permit.countyPermitComplete === null
  ) {
    throw new Error("Recorded permit pilot is missing reconciliation evidence");
  }
  if (
    permit.localPilotPassed &&
    (!permit.allInputParcelsTerminal ||
      !permit.allRecordsAccountedFor ||
      !permit.queryRowsMatchUniqueRecords ||
      permit.sourceFailures !== 0)
  ) {
    throw new Error("Permit pilot pass does not reconcile");
  }
  if (
    permit.countyPermitComplete &&
    (!permit.localPilotPassed || permit.currentSourceBlocked !== 0)
  ) {
    throw new Error("Permit county completeness does not reconcile");
  }
  return {
    ...publicFields,
    pilotState: permit.localPilotPassed ? "passed" : "failed",
    countyCompleteness: permit.countyPermitComplete
      ? "complete"
      : "not_complete",
  };
}

/**
 * Validate invariants before aggregate data crosses the public API boundary.
 *
 * @param snapshot - Candidate database snapshot.
 * @throws When counters, timestamps, or reconciliation invariants are invalid.
 */
function validateSnapshot(snapshot: StatusSnapshot): void {
  for (const [name, value] of [
    ["denominator", snapshot.denominator],
    ["attempted", snapshot.attempted],
    ["succeeded", snapshot.succeeded],
    ["sourceMisses", snapshot.sourceMisses],
    ["sourceFailures", snapshot.sourceFailures],
    ["transformFailures", snapshot.transformFailures],
    ["loadFailures", snapshot.loadFailures],
    ["staleAfterSeconds", snapshot.staleAfterSeconds],
    ["throughputAttempted", snapshot.throughputAttempted],
    ["throughputWindowSeconds", snapshot.throughputWindowSeconds],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new Error(`Invalid dashboard aggregate: ${name}`);
    }
  }
  if (
    snapshot.denominator < 1 ||
    snapshot.attempted > snapshot.denominator ||
    snapshot.succeeded > snapshot.attempted ||
    snapshot.sourceMisses > snapshot.attempted ||
    snapshot.succeeded + snapshot.sourceMisses > snapshot.attempted ||
    snapshot.staleAfterSeconds < 30
  ) {
    throw new Error("Dashboard progress aggregates do not reconcile");
  }
  for (const [name, value] of [
    ["heartbeatAt", snapshot.heartbeatAt],
    ["startedAt", snapshot.startedAt],
  ] as const) {
    if (value !== null && !Number.isFinite(Date.parse(value))) {
      throw new Error(`Invalid dashboard timestamp: ${name}`);
    }
  }
}

/**
 * Derive a user-visible writer state from phase and heartbeat freshness.
 *
 * @param input - Reconciled completion and heartbeat values.
 * @returns Explicit online, stale, offline, not-started, or complete state.
 */
function determineHealthState(input: {
  readonly attempted: number;
  readonly completed: number;
  readonly denominator: number;
  readonly heartbeatAgeSeconds: number | null;
  readonly phase: DashboardPhase;
  readonly staleAfterSeconds: number;
}): DashboardHealthState {
  if (input.phase === "complete" || input.completed >= input.denominator) {
    return "complete";
  }
  if (input.heartbeatAgeSeconds === null) {
    return input.attempted === 0 ? "not_started" : "offline";
  }
  if (NON_RUNNING_PHASES.has(input.phase)) return "offline";
  if (input.heartbeatAgeSeconds > input.staleAfterSeconds) return "stale";
  return "online";
}

/**
 * Sanitize trusted Lexicon category keys and combine invalid keys into Other.
 *
 * The database also constrains category keys. Repeating this boundary check
 * prevents arbitrary source text from being exposed if the table contract is
 * bypassed. At most 40 named categories cross the API.
 *
 * @param categories - Aggregate category counters from the status table.
 * @param totalSucceeded - Durable succeeded-row denominator for percentages.
 * @returns Sorted category coverage suitable for direct text rendering.
 */
function sanitizeCategoryCoverage(
  categories: readonly CategorySnapshot[],
  totalSucceeded: number,
): readonly DashboardCategoryCoverage[] {
  const combined = new Map<string, number>();
  let otherCount = 0;
  for (const category of categories) {
    if (
      !Number.isSafeInteger(category.succeededCount) ||
      category.succeededCount < 0
    ) {
      throw new Error("Invalid category coverage count");
    }
    if (!CATEGORY_KEY_PATTERN.test(category.categoryKey)) {
      otherCount += category.succeededCount;
      continue;
    }
    combined.set(
      category.categoryKey,
      (combined.get(category.categoryKey) ?? 0) + category.succeededCount,
    );
  }
  const sorted = [...combined.entries()].sort(
    (left, right) => right[1] - left[1] || left[0].localeCompare(right[0]),
  );
  const visible = sorted.slice(0, 40);
  otherCount += sorted.slice(40).reduce((total, entry) => total + entry[1], 0);
  if (otherCount > 0) visible.push(["Other", otherCount]);
  return visible.map(([category, succeeded]) => ({
    category,
    succeeded,
    percentOfSucceeded:
      totalSucceeded === 0
        ? 0
        : round(Math.min(1, succeeded / totalSucceeded) * 100, 2),
  }));
}

/**
 * Round a finite aggregate to a fixed number of decimal places.
 *
 * @param value - Finite number to round.
 * @param places - Non-negative number of decimal places.
 * @returns Rounded number.
 */
function round(value: number, places: number): number {
  const scale = 10 ** places;
  return Math.round(value * scale) / scale;
}
