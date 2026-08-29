import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import { requireUnpooledMigrationUrl } from "../scripts/migrate";
import {
  assertBrowardNeonIdentity,
  requireBrowardNeonIdentity,
} from "../src/server/neon-identity";
import { requirePooledDatabaseUrl } from "../src/server/neon-status";

describe("database boundary contract", () => {
  it("requires pooled DATABASE_URL for Vercel runtime traffic", () => {
    const pooled =
      "postgresql://dashboard:secret@ep-example-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require";
    const direct =
      "postgresql://dashboard:secret@ep-example.us-east-2.aws.neon.tech/neondb?sslmode=require";

    expect(requirePooledDatabaseUrl(pooled)).toBe(pooled);
    expect(() => requirePooledDatabaseUrl(direct)).toThrow(/pooled/u);
    expect(() => requirePooledDatabaseUrl(undefined)).toThrow(
      /not configured/u,
    );
  });

  it("requires unpooled DATABASE_URL_UNPOOLED for migrations", () => {
    const pooled =
      "postgresql://migrator:secret@ep-example-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require";
    const direct =
      "postgresql://migrator:secret@ep-example.us-east-2.aws.neon.tech/neondb?sslmode=require";

    expect(requireUnpooledMigrationUrl(direct)).toBe(direct);
    expect(() => requireUnpooledMigrationUrl(pooled)).toThrow(/direct/u);
    expect(() => requireUnpooledMigrationUrl(undefined)).toThrow(/required/u);
  });

  it("requires independently supplied non-production Broward branch identity", () => {
    const expected = requireBrowardNeonIdentity({
      BROWARD_INGEST_NEON_BRANCH_ID: "br-isolated-broward",
      BROWARD_INGEST_NEON_ENDPOINT_ID: "ep-isolated-broward",
    });

    expect(expected).toEqual({
      branchId: "br-isolated-broward",
      endpointId: "ep-isolated-broward",
    });
    expect(() => requireBrowardNeonIdentity({})).toThrow(/branch/iu);
    expect(() =>
      requireBrowardNeonIdentity({
        BROWARD_INGEST_NEON_BRANCH_ID: "br-isolated-broward",
        BROWARD_INGEST_NEON_ENDPOINT_ID: "ep-mute-leaf-production",
      }),
    ).toThrow(/non-production/u);
    expect(() =>
      assertBrowardNeonIdentity(
        {
          project_id: "raspy-frost-51580436",
          branch_id: "br-wrong",
          endpoint_id: "ep-isolated-broward",
        },
        expected,
      ),
    ).toThrow(/verified broward-ingest/u);
    expect(() =>
      assertBrowardNeonIdentity(
        {
          project_id: "raspy-frost-51580436",
          branch_id: "br-isolated-broward",
          endpoint_id: "ep-isolated-broward",
        },
        expected,
      ),
    ).not.toThrow();
  });

  it("defines only aggregate appraisal and permit status relations", async () => {
    const migration = (
      await Promise.all([
        readFile(
          new URL(
            "../migrations/001_broward_ingest_status.sql",
            import.meta.url,
          ),
          "utf8",
        ),
        readFile(
          new URL(
            "../migrations/002_broward_permit_status.sql",
            import.meta.url,
          ),
          "utf8",
        ),
      ])
    ).join("\n");

    expect(migration).toContain(
      "CREATE TABLE IF NOT EXISTS ingest_control.broward_ingest_status",
    );
    expect(migration).toContain("denominator_count");
    expect(migration).toContain("heartbeat_at");
    expect(migration).toContain("throughput_attempted_count");
    expect(migration).toContain(
      "CREATE TABLE IF NOT EXISTS ingest_control.broward_ingest_category_coverage",
    );
    expect(migration).toContain(
      "CREATE TABLE IF NOT EXISTS ingest_control.broward_permit_control",
    );
    expect(migration).toContain(
      "CREATE TABLE IF NOT EXISTS ingest_control.broward_permit_status",
    );
    expect(migration).toContain(
      "CREATE TABLE IF NOT EXISTS ingest_control.broward_permit_events",
    );
    expect(migration).toContain("record_broward_permit_pilot_status");
    expect(migration).toContain("county_permit_complete");
    expect(migration).toContain("current_source_blocked_count");
    expect(migration).not.toMatch(
      /\b(folio|owner|address|credential|artifact_path|error_text)\s+(?:text|json|jsonb|varchar)/iu,
    );
    expect(migration).toContain("Category coverage exceeds verified successes");
  });

  it("keeps migration and recovery status writes behind static safety contracts", async () => {
    const [migrationScript, recoveryScript, readme] = await Promise.all([
      readFile(new URL("../scripts/migrate.ts", import.meta.url), "utf8"),
      readFile(
        new URL(
          "../../../scripts/recover-broward-appraisal-to-neon.mjs",
          import.meta.url,
        ),
        "utf8",
      ),
      readFile(new URL("../README.md", import.meta.url), "utf8"),
    ]);

    expect(migrationScript).toContain('client.query("BEGIN READ ONLY")');
    expect(migrationScript.indexOf("verifyMigrationTarget")).toBeLessThan(
      migrationScript.indexOf("client.query(statement)"),
    );
    expect(recoveryScript).toContain("record_broward_ingest_status(");
    expect(recoveryScript).toContain("broward_appraisal_completed_items");
    expect(recoveryScript).toContain("broward_appraisal_terminal_items");
    const chunkCommit = recoveryScript.indexOf(
      "async function commitChunkCheckpoint",
    );
    const durableCommit = recoveryScript.indexOf(
      'await client.query("COMMIT")',
      chunkCommit,
    );
    const statusProjection = recoveryScript.indexOf(
      "await recordBrowardIngestStatus(client, options.mode)",
      chunkCommit,
    );
    expect(chunkCommit).toBeGreaterThan(-1);
    expect(durableCommit).toBeGreaterThan(chunkCommit);
    expect(statusProjection).toBeGreaterThan(durableCommit);
    expect(readme).toContain(
      "Do not migrate while the current database branch identity is unresolved or",
    );
    expect(readme).toContain(
      "Do not deploy while either URL's branch identity is unresolved/wrong.",
    );
  });
});
