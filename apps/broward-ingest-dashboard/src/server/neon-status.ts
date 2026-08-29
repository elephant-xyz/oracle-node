import { attachDatabasePool } from "@vercel/functions";
import { Pool, type QueryResultRow } from "pg";

import {
  DASHBOARD_PIPELINE_KEY,
  buildDashboardStatus,
  type CategorySnapshot,
  type DashboardPhase,
  type DashboardStatus,
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
    `SELECT
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
     GROUP BY status.pipeline_key`,
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
