import { attachDatabasePool } from "@vercel/functions";
import { Pool, type QueryResultRow } from "pg";

import {
  DASHBOARD_PIPELINE_KEY,
  buildDashboardStatus,
  type CategorySnapshot,
  type DashboardPhase,
  type DashboardStatus,
  type PermitStatusSnapshot,
  type StatusSnapshot,
} from "../shared/status";
import {
  BROWARD_NEON_PROJECT_ID,
  type BrowardNeonIdentity,
} from "./neon-identity";

interface StatusDatabaseRow extends QueryResultRow {
  readonly attempted_count: unknown;
  readonly categories: unknown;
  readonly denominator_count: unknown;
  readonly heartbeat_at: unknown;
  readonly load_failure_count: unknown;
  readonly phase: unknown;
  readonly source_failure_count: unknown;
  readonly source_miss_count: unknown;
  readonly stale_after_seconds: unknown;
  readonly started_at: unknown;
  readonly succeeded_count: unknown;
  readonly throughput_attempted_count: unknown;
  readonly throughput_window_seconds: unknown;
  readonly transform_failure_count: unknown;
  readonly permit_recorded_at: unknown;
  readonly permit_sample_parcels: unknown;
  readonly permit_appraisal_resolved: unknown;
  readonly permit_jurisdiction_resolved: unknown;
  readonly permit_jurisdiction_unresolved: unknown;
  readonly permit_source_unavailable_outcomes: unknown;
  readonly permit_source_attempts: unknown;
  readonly permit_attempted_parcels: unknown;
  readonly permit_source_failures: unknown;
  readonly permit_unique_records: unknown;
  readonly permit_query_rows: unknown;
  readonly permit_all_input_terminal: unknown;
  readonly permit_all_records_accounted: unknown;
  readonly permit_query_rows_match: unknown;
  readonly permit_local_pilot_passed: unknown;
  readonly permit_county_complete: unknown;
  readonly permit_registry_jurisdictions: unknown;
  readonly permit_current_source_implemented: unknown;
  readonly permit_current_source_blocked: unknown;
}

const PHASES: ReadonlySet<DashboardPhase> = new Set([
  "not_started",
  "pilot",
  "capturing",
  "transforming",
  "loading",
  "verifying",
  "full",
  "paused",
  "failed",
  "complete",
]);

let attachedPool: Pool | null = null;

/**
 * Return the process-reused Vercel database pool.
 *
 * Only `DATABASE_URL` is read. The URL must identify Neon's pooled endpoint,
 * which contains `-pooler` in its hostname. The direct
 * `DATABASE_URL_UNPOOLED` migration credential is intentionally ignored by
 * runtime code.
 *
 * @param environment - Runtime environment-variable record.
 * @returns A lifecycle-attached node-postgres pool limited to two clients.
 */
export function getDashboardPool(
  environment: NodeJS.ProcessEnv = process.env,
): Pool {
  if (attachedPool !== null) return attachedPool;
  const connectionString = requirePooledDatabaseUrl(environment.DATABASE_URL);
  const pool = new Pool({
    allowExitOnIdle: true,
    application_name: "broward-ingest-dashboard",
    connectionString,
    connectionTimeoutMillis: 5_000,
    idleTimeoutMillis: 5_000,
    max: 2,
    query_timeout: 5_000,
  });
  attachDatabasePool(pool);
  attachedPool = pool;
  return pool;
}

/**
 * Validate the server-only runtime connection string as a pooled Neon URL.
 *
 * The returned value must never be logged or included in an error. Rejecting
 * direct endpoints prevents Vercel function concurrency from consuming direct
 * database connections.
 *
 * @param value - Candidate `DATABASE_URL` value.
 * @returns The unchanged pooled PostgreSQL connection string.
 */
export function requirePooledDatabaseUrl(value: string | undefined): string {
  if (value === undefined || value.trim() === "") {
    throw new Error("Pooled DATABASE_URL is not configured");
  }
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error("DATABASE_URL is not a valid PostgreSQL URL");
  }
  if (
    !["postgres:", "postgresql:"].includes(parsed.protocol) ||
    !/(?:^|\.)[^.]*-pooler(?:\.|$)/u.test(parsed.hostname)
  ) {
    throw new Error("DATABASE_URL must use a pooled Neon endpoint");
  }
  return value;
}

/**
 * Read one transactionally consistent aggregate snapshot from Neon.
 *
 * The query touches only `ingest_control` aggregate tables. It cannot return
 * folios, people, addresses, raw errors, source payloads, database identity,
 * or artifact metadata because those columns are absent from this contract.
 *
 * @param pool - Lifecycle-managed pooled server-side database connection.
 * @param expectedIdentity - IDs independently mapped to `broward-ingest`.
 * @param nowMs - Current Unix epoch time in milliseconds.
 * @returns Privacy-safe status response ready for JSON serialization.
 */
export async function readDashboardStatus(
  pool: Pool,
  expectedIdentity: BrowardNeonIdentity,
  nowMs: number = Date.now(),
): Promise<DashboardStatus> {
  const result = await pool.query<StatusDatabaseRow>(
    `WITH appraisal AS (
       SELECT
         status.denominator_count,
         status.attempted_count,
         status.succeeded_count,
         status.source_miss_count,
         status.source_failure_count,
         status.transform_failure_count,
         status.load_failure_count,
         status.phase,
         status.started_at,
         status.heartbeat_at,
         status.stale_after_seconds,
         status.throughput_window_seconds,
         status.throughput_attempted_count,
         COALESCE(
           jsonb_agg(
             jsonb_build_object(
               'categoryKey', coverage.category_key,
               'succeededCount', coverage.succeeded_count
             )
             ORDER BY coverage.succeeded_count DESC, coverage.category_key
           ) FILTER (WHERE coverage.category_key IS NOT NULL),
           '[]'::jsonb
         ) AS categories
       FROM ingest_control.broward_ingest_status AS status
       LEFT JOIN ingest_control.broward_ingest_category_coverage AS coverage
         ON coverage.pipeline_key = status.pipeline_key
       CROSS JOIN (
         SELECT
           current_setting('neon.project_id', true) AS project_id,
           current_setting('neon.branch_id', true) AS branch_id,
           current_setting('neon.endpoint_id', true) AS endpoint_id
       ) AS identity
       WHERE status.pipeline_key = $1
         AND identity.project_id = $2
         AND identity.branch_id = $3
         AND identity.endpoint_id = $4
       GROUP BY status.pipeline_key
     ),
     permit AS (
       SELECT
         status.recorded_at AS permit_recorded_at,
         status.sample_parcels AS permit_sample_parcels,
         status.appraisal_resolved AS permit_appraisal_resolved,
         status.jurisdiction_resolved AS permit_jurisdiction_resolved,
         status.jurisdiction_unresolved AS permit_jurisdiction_unresolved,
         status.source_unavailable_outcomes
           AS permit_source_unavailable_outcomes,
         status.permit_source_attempts AS permit_source_attempts,
         status.permit_attempted_parcels AS permit_attempted_parcels,
         status.source_failures AS permit_source_failures,
         status.unique_permit_records AS permit_unique_records,
         status.query_rows AS permit_query_rows,
         status.all_input_parcels_terminal AS permit_all_input_terminal,
         status.all_records_accounted_for AS permit_all_records_accounted,
         status.query_rows_match_unique_records AS permit_query_rows_match,
         status.local_pilot_passed AS permit_local_pilot_passed,
         status.county_permit_complete AS permit_county_complete,
         control.registry_jurisdiction_count
           AS permit_registry_jurisdictions,
         control.current_source_implemented_count
           AS permit_current_source_implemented,
         control.current_source_blocked_count
           AS permit_current_source_blocked
       FROM ingest_control.broward_permit_control AS control
       LEFT JOIN ingest_control.broward_permit_status AS status
         ON status.pipeline_key = control.pipeline_key
       WHERE control.pipeline_key = 'broward-permit'
     )
     SELECT appraisal.*, permit.*
     FROM appraisal
     CROSS JOIN permit`,
    [
      DASHBOARD_PIPELINE_KEY,
      BROWARD_NEON_PROJECT_ID,
      expectedIdentity.branchId,
      expectedIdentity.endpointId,
    ],
  );
  const row = result.rows[0];
  if (row === undefined) {
    throw new Error("Aggregate Broward ingest status is not initialized");
  }
  return buildDashboardStatus(parseStatusRow(row), nowMs);
}

/**
 * Narrow untrusted PostgreSQL driver values into the internal status contract.
 *
 * @param row - One aggregate-only query row.
 * @returns Validated, typed counters and timestamps.
 */
function parseStatusRow(row: StatusDatabaseRow): StatusSnapshot {
  return {
    attempted: readSafeCount(row.attempted_count, "attempted_count"),
    categories: readCategories(row.categories),
    denominator: readSafeCount(row.denominator_count, "denominator_count"),
    heartbeatAt: readNullableTimestamp(row.heartbeat_at, "heartbeat_at"),
    loadFailures: readSafeCount(row.load_failure_count, "load_failure_count"),
    phase: readPhase(row.phase),
    sourceFailures: readSafeCount(
      row.source_failure_count,
      "source_failure_count",
    ),
    sourceMisses: readSafeCount(row.source_miss_count, "source_miss_count"),
    staleAfterSeconds: readSafeCount(
      row.stale_after_seconds,
      "stale_after_seconds",
    ),
    startedAt: readNullableTimestamp(row.started_at, "started_at"),
    succeeded: readSafeCount(row.succeeded_count, "succeeded_count"),
    permit: parsePermitStatus(row),
    throughputAttempted: readSafeCount(
      row.throughput_attempted_count,
      "throughput_attempted_count",
    ),
    throughputWindowSeconds: readSafeCount(
      row.throughput_window_seconds,
      "throughput_window_seconds",
    ),
    transformFailures: readSafeCount(
      row.transform_failure_count,
      "transform_failure_count",
    ),
  };
}

/**
 * Parse fixed permit control counts and an optional durable pilot projection.
 * Nullable pilot fields remain null when no status row has been recorded.
 *
 * @param row - Combined appraisal and permit aggregate database row.
 * @returns Validated permit status snapshot for the shared response builder.
 */
function parsePermitStatus(row: StatusDatabaseRow): PermitStatusSnapshot {
  return {
    recordedAt: readNullableTimestamp(
      row.permit_recorded_at,
      "permit_recorded_at",
    ),
    sampleParcels: readNullableSafeCount(
      row.permit_sample_parcels,
      "permit_sample_parcels",
    ),
    appraisalResolved: readNullableSafeCount(
      row.permit_appraisal_resolved,
      "permit_appraisal_resolved",
    ),
    jurisdictionResolved: readNullableSafeCount(
      row.permit_jurisdiction_resolved,
      "permit_jurisdiction_resolved",
    ),
    jurisdictionUnresolved: readNullableSafeCount(
      row.permit_jurisdiction_unresolved,
      "permit_jurisdiction_unresolved",
    ),
    sourceUnavailableOutcomes: readNullableSafeCount(
      row.permit_source_unavailable_outcomes,
      "permit_source_unavailable_outcomes",
    ),
    permitSourceAttempts: readNullableSafeCount(
      row.permit_source_attempts,
      "permit_source_attempts",
    ),
    permitAttemptedParcels: readNullableSafeCount(
      row.permit_attempted_parcels,
      "permit_attempted_parcels",
    ),
    sourceFailures: readNullableSafeCount(
      row.permit_source_failures,
      "permit_source_failures",
    ),
    uniquePermitRecords: readNullableSafeCount(
      row.permit_unique_records,
      "permit_unique_records",
    ),
    queryRows: readNullableSafeCount(
      row.permit_query_rows,
      "permit_query_rows",
    ),
    allInputParcelsTerminal: readNullableBoolean(
      row.permit_all_input_terminal,
      "permit_all_input_terminal",
    ),
    allRecordsAccountedFor: readNullableBoolean(
      row.permit_all_records_accounted,
      "permit_all_records_accounted",
    ),
    queryRowsMatchUniqueRecords: readNullableBoolean(
      row.permit_query_rows_match,
      "permit_query_rows_match",
    ),
    localPilotPassed: readNullableBoolean(
      row.permit_local_pilot_passed,
      "permit_local_pilot_passed",
    ),
    countyPermitComplete: readNullableBoolean(
      row.permit_county_complete,
      "permit_county_complete",
    ),
    registryJurisdictions: readSafeCount(
      row.permit_registry_jurisdictions,
      "permit_registry_jurisdictions",
    ),
    currentSourceImplemented: readSafeCount(
      row.permit_current_source_implemented,
      "permit_current_source_implemented",
    ),
    currentSourceBlocked: readSafeCount(
      row.permit_current_source_blocked,
      "permit_current_source_blocked",
    ),
  };
}

/**
 * Convert a PostgreSQL bigint/integer value to a non-negative safe integer.
 *
 * @param value - Driver value, commonly a string for bigint columns.
 * @param fieldName - Fixed aggregate column name used in safe diagnostics.
 * @returns Parsed non-negative safe integer.
 */
function readSafeCount(value: unknown, fieldName: string): number {
  const parsed =
    typeof value === "number" || typeof value === "string"
      ? Number(value)
      : Number.NaN;
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`Invalid aggregate database field: ${fieldName}`);
  }
  return parsed;
}

/**
 * Validate a count that is null until durable permit evidence is recorded.
 *
 * @param value - Driver value or null for an absent permit status row.
 * @param fieldName - Fixed aggregate column name used in safe diagnostics.
 * @returns Parsed non-negative safe integer or null.
 */
function readNullableSafeCount(
  value: unknown,
  fieldName: string,
): number | null {
  return value === null ? null : readSafeCount(value, fieldName);
}

/**
 * Validate a nullable aggregate reconciliation flag.
 *
 * @param value - Driver boolean or null for an absent permit status row.
 * @param fieldName - Fixed aggregate column name used in safe diagnostics.
 * @returns Boolean evidence or null.
 */
function readNullableBoolean(
  value: unknown,
  fieldName: string,
): boolean | null {
  if (value === null) return null;
  if (typeof value !== "boolean") {
    throw new Error(`Invalid aggregate database field: ${fieldName}`);
  }
  return value;
}

/**
 * Validate a nullable timestamp without retaining the driver's original value.
 *
 * @param value - PostgreSQL timestamp represented as a Date or string.
 * @param fieldName - Fixed aggregate column name used in safe diagnostics.
 * @returns ISO timestamp or null.
 */
function readNullableTimestamp(
  value: unknown,
  fieldName: string,
): string | null {
  if (value === null) return null;
  const timestamp =
    value instanceof Date
      ? value.toISOString()
      : typeof value === "string"
        ? value
        : "";
  if (!Number.isFinite(Date.parse(timestamp))) {
    throw new Error(`Invalid aggregate database field: ${fieldName}`);
  }
  return new Date(timestamp).toISOString();
}

/**
 * Validate a fixed operational phase.
 *
 * @param value - Driver-returned phase.
 * @returns A supported dashboard phase.
 */
function readPhase(value: unknown): DashboardPhase {
  if (typeof value !== "string" || !PHASES.has(value as DashboardPhase)) {
    throw new Error("Invalid aggregate database field: phase");
  }
  return value as DashboardPhase;
}

/**
 * Validate aggregate category objects created by the SQL JSON expression.
 *
 * @param value - Driver-parsed JSON array.
 * @returns Typed category counters; keys receive final privacy sanitization in
 *   the public response builder.
 */
function readCategories(value: unknown): readonly CategorySnapshot[] {
  if (!Array.isArray(value)) {
    throw new Error("Invalid aggregate database field: categories");
  }
  return value.map((entry) => {
    if (typeof entry !== "object" || entry === null || Array.isArray(entry)) {
      throw new Error("Invalid aggregate category entry");
    }
    const record = entry as Record<string, unknown>;
    if (typeof record.categoryKey !== "string") {
      throw new Error("Invalid aggregate category key");
    }
    return {
      categoryKey: record.categoryKey,
      succeededCount: readSafeCount(
        record.succeededCount,
        "category_succeeded_count",
      ),
    };
  });
}
