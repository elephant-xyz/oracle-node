import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import { requireUnpooledMigrationUrl } from "../scripts/migrate";
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

  it("defines only aggregate progress, heartbeat, and category columns", async () => {
    const migration = await readFile(
      new URL(
        "../migrations/001_broward_ingest_status.sql",
        import.meta.url,
      ),
      "utf8",
    );

    expect(migration).toContain(
      "CREATE TABLE IF NOT EXISTS ingest_control.broward_ingest_status",
    );
    expect(migration).toContain("denominator_count");
    expect(migration).toContain("heartbeat_at");
    expect(migration).toContain("throughput_attempted_count");
    expect(migration).toContain(
      "CREATE TABLE IF NOT EXISTS ingest_control.broward_ingest_category_coverage",
    );
    expect(migration).not.toMatch(
      /\b(folio|owner|address|credential|artifact_path|error_text)\s+(?:text|json|jsonb|varchar)/iu,
    );
  });
});
