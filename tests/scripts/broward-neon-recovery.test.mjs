import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  assertBrowardStatusWriterInstalled,
  assertFullSeed,
  hashSeedKey,
  isSeedPending,
  parseRecoveryOptions,
  readSeedStats,
  recordBrowardIngestStatus,
  verifyNeonTarget,
} from "../../scripts/recover-broward-appraisal-to-neon.mjs";
import {
  buildRecoveryStatus,
  parseDashboardOptions,
} from "../../scripts/broward-neon-recovery-dashboard.mjs";

/** @type {string[]} */
const temporaryDirectories = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

describe("durable Broward Neon recovery", () => {
  it("requires exact branch identity and caps source concurrency at four", () => {
    expect(
      parseRecoveryOptions([
        "--pilot",
        "--expected-branch-id",
        "br-old-cloud-aqz2hqjl",
        "--expected-endpoint-id",
        "ep-still-flower-aq04hhgg",
        "--concurrency",
        "4",
        "--chunk-size",
        "50",
      ]),
    ).toMatchObject({
      mode: "pilot",
      expectedBranchId: "br-old-cloud-aqz2hqjl",
      expectedEndpointId: "ep-still-flower-aq04hhgg",
      concurrency: 4,
      chunkSize: 50,
    });
    expect(() =>
      parseRecoveryOptions([
        "--full",
        "--expected-branch-id",
        "br-old-cloud-aqz2hqjl",
        "--expected-endpoint-id",
        "ep-still-flower-aq04hhgg",
        "--concurrency",
        "5",
      ]),
    ).toThrow(/through 4/u);
    expect(() => parseRecoveryOptions(["--pilot"])).toThrow(
      /expected-branch-id/u,
    );
  });

  it("builds a deterministic seed signature without numeric folio coercion", async () => {
    const directory = await mkdtemp(
      path.join(tmpdir(), "broward-recovery-seed-"),
    );
    temporaryDirectories.push(directory);
    const seedPath = path.join(directory, "seed.csv");
    await writeFile(
      seedPath,
      [
        "request_identifier,parcel_id",
        "504108BJ0140,504108BJ0140",
        "000000000001,000000000001",
        "",
      ].join("\n"),
    );
    const first = await readSeedStats(seedPath);
    const second = await readSeedStats(seedPath);
    expect(first).toEqual({
      rowCount: 2,
      distinctFolios: 2,
      signature: second.signature,
    });
    expect(hashSeedKey("504108BJ0140")).toHaveLength(64);
    expect(hashSeedKey("504108BJ0140")).not.toContain("504108BJ0140");
    const completedHashes = new Set([hashSeedKey("504108BJ0140")]);
    const terminalHashes = new Set([hashSeedKey("000000000001")]);
    expect(isSeedPending("504108BJ0140", completedHashes, terminalHashes)).toBe(
      false,
    );
    expect(isSeedPending("000000000001", completedHashes, terminalHashes)).toBe(
      false,
    );
    expect(isSeedPending("504108BJ0141", completedHashes, terminalHashes)).toBe(
      true,
    );
    expect(() => assertFullSeed(first)).toThrow(/534309/u);
    expect(() =>
      assertFullSeed({
        rowCount: 534_309,
        distinctFolios: 534_309,
        signature: first.signature,
      }),
    ).not.toThrow();
  });

  it("does not return secret-sourced Neon identity values after verification", async () => {
    const client = {
      /**
       * Return matching identity and aggregate-safe Broward inventory rows.
       *
       * @param {string} sql - Recovery safety query.
       * @returns {Promise<{ rows: Record<string, unknown>[] }>} Mock query rows.
       */
      query(sql) {
        if (sql.includes("neon.project_id")) {
          return Promise.resolve({
            rows: [
              {
                project_id: "raspy-frost-51580436",
                branch_id: "br-broward-test",
                endpoint_id: "ep-broward-test",
              },
            ],
          });
        }
        if (sql.includes("FROM public.properties")) {
          return Promise.resolve({
            rows: [
              {
                property_count: "0",
                distinct_folios: "0",
                invalid_property_rows: "0",
              },
            ],
          });
        }
        if (sql.includes("FROM public.addresses")) {
          return Promise.resolve({
            rows: [{ invalid_address_rows: "0" }],
          });
        }
        return Promise.resolve({ rows: [] });
      },
    };
    await expect(
      verifyNeonTarget(
        /** @type {import("pg").Client} */ (client),
        parseRecoveryOptions([
          "--pilot",
          "--expected-branch-id",
          "br-broward-test",
          "--expected-endpoint-id",
          "ep-broward-test",
        ]),
      ),
    ).resolves.toEqual({ propertyCount: 0, distinctFolios: 0 });
  });

  it("fails closed when server-side Neon identity differs", async () => {
    /** @type {{ statements: string[] }} */
    const state = { statements: [] };
    const client = {
      /**
       * @param {string} sql - Recovery safety query.
       * @returns {Promise<{ rows: Record<string, unknown>[] }>} Mock query rows.
       */
      query(sql) {
        state.statements.push(sql);
        if (sql.includes("neon.project_id")) {
          return Promise.resolve({
            rows: [
              {
                project_id: "raspy-frost-51580436",
                branch_id: "br-production",
                endpoint_id: "ep-mute-leaf-production",
              },
            ],
          });
        }
        return Promise.resolve({ rows: [] });
      },
    };
    await expect(
      verifyNeonTarget(
        /** @type {import("pg").Client} */ (client),
        parseRecoveryOptions([
          "--pilot",
          "--expected-branch-id",
          "br-old-cloud-aqz2hqjl",
          "--expected-endpoint-id",
          "ep-still-flower-aq04hhgg",
        ]),
      ),
    ).rejects.toThrow(/does not match isolated/u);
    expect(state.statements[0]).toBe("BEGIN READ ONLY");
    expect(state.statements.at(-1)).toBe("ROLLBACK");
  });

  it("requires the exact migrated aggregate status writer signature", async () => {
    const expectedSignature =
      "ingest_control.record_broward_ingest_status(text,bigint,bigint,bigint,bigint,bigint,bigint,integer,bigint,jsonb,timestamp with time zone)";
    const installedClient = {
      /**
       * Resolve the exact procedure signature without performing a write.
       *
       * @param {string} sql - Procedure-catalog lookup.
       * @param {readonly unknown[]} values - Exact signature lookup value.
       * @returns {Promise<{ rows: Record<string, unknown>[] }>} Installed writer row.
       */
      query(sql, values) {
        expect(sql).toContain("to_regprocedure");
        expect(values).toEqual([expectedSignature]);
        return Promise.resolve({
          rows: [{ procedure_name: expectedSignature }],
        });
      },
    };
    await expect(
      assertBrowardStatusWriterInstalled(
        /** @type {import("pg").Client} */ (installedClient),
      ),
    ).resolves.toBeUndefined();

    const missingClient = {
      /**
       * Return a missing exact procedure signature.
       *
       * @returns {Promise<{ rows: Record<string, unknown>[] }>} Missing writer row.
       */
      query() {
        return Promise.resolve({ rows: [{ procedure_name: null }] });
      },
    };
    await expect(
      assertBrowardStatusWriterInstalled(
        /** @type {import("pg").Client} */ (missingClient),
      ),
    ).rejects.toThrow(/migration is required/u);
  });

  it("projects status from durable aggregate truth with no private arguments", async () => {
    /** @type {{ sql: string, values: readonly unknown[] }[]} */
    const calls = [];
    const client = {
      /**
       * Capture the single aggregate status projection query.
       *
       * @param {string} sql - Aggregate-only projection statement.
       * @param {readonly unknown[]} values - Phase and throughput-window values.
       * @returns {Promise<{ rows: Record<string, unknown>[] }>} Empty command result.
       */
      query(sql, values) {
        calls.push({ sql, values });
        return Promise.resolve({ rows: [] });
      },
    };

    await recordBrowardIngestStatus(
      /** @type {import("pg").Client} */ (client),
      "full",
    );

    expect(calls).toHaveLength(1);
    expect(calls[0]?.values).toEqual(["full", 900]);
    expect(calls[0]?.sql).toContain("broward_appraisal_completed_items");
    expect(calls[0]?.sql).toContain("broward_appraisal_terminal_items");
    expect(calls[0]?.sql).toContain("broward_appraisal_events");
    expect(calls[0]?.sql).toContain("broward_appraisal_chunks");
    expect(calls[0]?.sql).toContain("record_broward_ingest_status");
    expect(calls[0]?.sql).not.toMatch(
      /request_identifier|seed_key_hash|source_payload|owner|address|artifact_path/iu,
    );
  });

  it("renders only aggregate durable progress and stage failures", () => {
    const status = buildRecoveryStatus(
      {
        property_count: "50",
        distinct_folios: "50",
        verified_properties: "49",
        terminal_source_misses: "2",
        committed_chunks: "1",
        prepared_rows: "2500",
        committed_rows: "2400",
        source_miss_attempts: "2",
        source_error_attempts: "3",
        transform_error_attempts: "4",
        load_error_attempts: "1",
        recent_properties: "45",
        last_commit_at: "2026-08-29T00:00:00.000Z",
        recovery_lock_held: true,
      },
      Date.parse("2026-08-29T00:01:00.000Z"),
    );
    expect(status.progress).toMatchObject({
      properties: 50,
      distinctFolios: 50,
      verifiedProperties: 49,
      terminalSourceMisses: 2,
      durableCompleted: 51,
      committedChunks: 1,
      preparedRows: 2_500,
      committedRows: 2_400,
    });
    expect(status.failures).toEqual({
      sourceMissAttempts: 2,
      sourceErrorAttempts: 3,
      transformErrorAttempts: 4,
      loadErrorAttempts: 1,
    });
    expect(status.throughput.propertiesPerMinute).toBe(3);
    expect(JSON.stringify(status)).not.toContain("504108BJ0140");
    expect(JSON.stringify(status)).not.toContain("owner");
    expect(JSON.stringify(status)).not.toContain("address");
  });

  it("requires branch IDs for the recovery dashboard", () => {
    expect(
      parseDashboardOptions([
        "--host",
        "0.0.0.0",
        "--port",
        "47832",
        "--expected-branch-id",
        "br-old-cloud-aqz2hqjl",
        "--expected-endpoint-id",
        "ep-still-flower-aq04hhgg",
      ]),
    ).toEqual({
      host: "0.0.0.0",
      port: 47_832,
      expectedBranchId: "br-old-cloud-aqz2hqjl",
      expectedEndpointId: "ep-still-flower-aq04hhgg",
    });
    expect(() => parseDashboardOptions([])).toThrow(/expected-branch-id/u);
  });
});
