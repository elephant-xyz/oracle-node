import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { Client } from "pg";

import {
  assertBrowardNeonIdentity,
  requireBrowardNeonIdentity,
  type BrowardNeonIdentity,
  type BrowardNeonIdentityRow,
} from "../src/server/neon-identity";

const MIGRATION_URL = new URL(
  "../migrations/001_broward_ingest_status.sql",
  import.meta.url,
);

/**
 * Validate that schema changes use the direct Neon endpoint.
 *
 * Runtime application traffic has the inverse guard and requires `-pooler`.
 * Keeping these credentials separate prevents accidental migration through
 * PgBouncer transaction mode.
 *
 * @param value - Candidate `DATABASE_URL_UNPOOLED` value.
 * @returns The unchanged direct PostgreSQL connection string.
 */
export function requireUnpooledMigrationUrl(value: string | undefined): string {
  if (value === undefined || value.trim() === "") {
    throw new Error("DATABASE_URL_UNPOOLED is required for migrations");
  }
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error("DATABASE_URL_UNPOOLED is not a valid PostgreSQL URL");
  }
  if (
    !["postgres:", "postgresql:"].includes(parsed.protocol) ||
    parsed.hostname.includes("-pooler")
  ) {
    throw new Error(
      "DATABASE_URL_UNPOOLED must use a direct PostgreSQL endpoint",
    );
  }
  return value;
}

/**
 * Verify the direct connection against immutable server-side Neon metadata in
 * a read-only transaction before allowing any schema change.
 *
 * @param client - Connected direct PostgreSQL client.
 * @param expected - IDs independently mapped to the `broward-ingest` branch.
 * @returns Promise resolved only after a matching read-only identity check.
 */
export async function verifyMigrationTarget(
  client: Client,
  expected: BrowardNeonIdentity,
): Promise<void> {
  await client.query("BEGIN READ ONLY");
  try {
    const result = await client.query<BrowardNeonIdentityRow>(
      `SELECT
         current_setting('neon.project_id', true) AS project_id,
         current_setting('neon.branch_id', true) AS branch_id,
         current_setting('neon.endpoint_id', true) AS endpoint_id`,
    );
    assertBrowardNeonIdentity(result.rows[0], expected);
    await client.query("ROLLBACK");
  } catch {
    await client.query("ROLLBACK");
    throw new Error(
      "Migration target is not the independently verified broward-ingest branch",
    );
  }
}

/**
 * Apply the versioned aggregate-status migration transaction.
 *
 * The connection string remains in process memory and is never printed.
 */
async function migrate(): Promise<void> {
  const connectionString = requireUnpooledMigrationUrl(
    process.env.DATABASE_URL_UNPOOLED,
  );
  const expectedIdentity = requireBrowardNeonIdentity(process.env);
  const migrationSql = await readFile(MIGRATION_URL, "utf8");
  const client = new Client({
    application_name: "broward-ingest-dashboard-migration",
    connectionString,
    connectionTimeoutMillis: 10_000,
    statement_timeout: 30_000,
  });
  await client.connect();
  try {
    await verifyMigrationTarget(client, expectedIdentity);
    await client.query(migrationSql);
    process.stdout.write(
      "Applied aggregate Broward ingestion status migration.\n",
    );
  } finally {
    await client.end();
  }
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  migrate().catch(() => {
    process.stderr.write(
      "Broward dashboard migration failed; no credentials were printed.\n",
    );
    process.exitCode = 1;
  });
}
