import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
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
  runOneAheadPipeline,
  verifyNeonTarget,
} from "../../scripts/recover-broward-appraisal-to-neon.mjs";
import {
  buildBrowardPermitRouteStatus,
  buildRecoveryStatus,
  parseDashboardOptions,
  readAccelaCsvReceiptAccessibleCount,
  readPermitEnumerationStatus,
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
    expect(() => parseRecoveryOptions(["--pilot"], {})).toThrow(
      /expected-branch-id/u,
    );
  });

  it("reads verified identity from runtime secrets without putting IDs in argv", () => {
    const environment = {
      BROWARD_INGEST_NEON_BRANCH_ID: "br-broward-runtime-test",
      BROWARD_INGEST_NEON_ENDPOINT_ID: "ep-broward-runtime-test",
    };
    expect(parseRecoveryOptions(["--pilot"], environment)).toMatchObject({
      mode: "pilot",
      expectedBranchId: "br-broward-runtime-test",
      expectedEndpointId: "ep-broward-runtime-test",
    });
    expect(
      parseDashboardOptions(
        ["--host", "0.0.0.0", "--port", "47832"],
        environment,
      ),
    ).toMatchObject({
      host: "0.0.0.0",
      port: 47_832,
      expectedBranchId: "br-broward-runtime-test",
      expectedEndpointId: "ep-broward-runtime-test",
    });
  });

  it("prepares one chunk ahead while preserving durable commit order", async () => {
    /** @type {string[]} */
    const events = [];
    /**
     * Yield three ordered chunks without retaining source identifiers.
     *
     * @returns {AsyncGenerator<readonly number[], void, void>} Numeric test chunks.
     */
    async function* chunks() {
      yield [1];
      yield [2];
      yield [3];
    }

    await runOneAheadPipeline({
      chunks: chunks(),
      prepare(chunk, slotIndex) {
        const value = chunk[0];
        if (value === undefined) throw new Error("Test chunk is empty");
        events.push(`prepare:${String(value)}:${String(slotIndex)}`);
        return Promise.resolve({ value });
      },
      commit(prepared) {
        events.push(`commit:${String(prepared.value)}`);
        return Promise.resolve(prepared.value);
      },
      afterCommit(committed) {
        events.push(`after:${String(committed)}`);
      },
    });

    expect(events).toEqual([
      "prepare:1:0",
      "prepare:2:1",
      "commit:1",
      "after:1",
      "prepare:3:0",
      "commit:2",
      "after:2",
      "commit:3",
      "after:3",
    ]);
  });

  it("never advances a later prepared chunk after the current commit fails", async () => {
    /** @type {string[]} */
    const events = [];
    /**
     * Yield two ordered chunks for the commit-failure boundary.
     *
     * @returns {AsyncGenerator<readonly number[], void, void>} Numeric test chunks.
     */
    async function* chunks() {
      yield [1];
      yield [2];
    }

    await expect(
      runOneAheadPipeline({
        chunks: chunks(),
        prepare(chunk, slotIndex) {
          const value = chunk[0];
          if (value === undefined) throw new Error("Test chunk is empty");
          events.push(`prepare:${String(value)}:${String(slotIndex)}`);
          return Promise.resolve({ value });
        },
        commit(prepared) {
          events.push(`commit:${String(prepared.value)}`);
          return Promise.reject(new Error("durable commit failed"));
        },
        afterCommit(committed) {
          events.push(`after:${String(committed)}`);
        },
      }),
    ).rejects.toThrow(/durable commit failed/u);

    expect(events).toEqual(["prepare:1:0", "prepare:2:1", "commit:1"]);
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
        permit_recorded_at: "2026-08-29T00:00:30.000Z",
        permit_sample_parcels: "25",
        permit_source_attempts: "2",
        permit_source_unavailable: "17",
        permit_source_failures: "0",
        permit_unique_records: "73",
        permit_query_rows: "73",
        permit_all_input_terminal: true,
        permit_all_records_accounted: true,
        permit_query_rows_match: true,
        permit_pilot_passed: true,
        permit_county_complete: false,
        permit_registry_jurisdictions: "32",
        permit_sources_implemented: "15",
        permit_sources_blocked: "17",
        permit_inventory_records: "243939",
        permit_inventory_matched: "192813",
        permit_inventory_unmatched: "51126",
        permit_inventory_roofing: "17483",
        permit_inventory_parcels: "42522",
        permit_inventory_sources: "13",
        permit_inventory_loaded_at: "2026-08-31T20:50:00.000Z",
        permit_bulk_source_rows: "204760",
        permit_bulk_committed_rows: "204760",
        permit_bulk_chunks: "205",
        permit_list_loaded_rows: "28946",
        permit_list_chunks: "29",
        sunbiz_match_roles: "21512",
        sunbiz_match_registrations: "12432",
        sunbiz_match_properties: "9023",
        sunbiz_match_chunks: "22",
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
    expect(status.permit).toEqual({
      pilotState: "passed",
      countyCompleteness: "not_complete",
      recordedAt: "2026-08-29T00:00:30.000Z",
      sampleParcels: 25,
      sourceAttempts: 2,
      sourceUnavailable: 17,
      sourceFailures: 0,
      uniqueRecords: 73,
      queryRows: 73,
      allInputTerminal: true,
      allRecordsAccounted: true,
      queryRowsMatch: true,
      registryJurisdictions: 32,
      currentSourcesImplemented: 24,
      currentSourcesBlocked: 8,
    });
    expect(status.permitRoutes).toEqual(buildBrowardPermitRouteStatus());
    expect(status.permitInventory).toEqual({
      records: 243_939,
      matched: 192_813,
      unmatched: 51_126,
      roofing: 17_483,
      distinctParcels: 42_522,
      sourceSystems: 13,
      lastLoadedAt: "2026-08-31T20:50:00.000Z",
      bulkSourceRows: 204_760,
      bulkCommittedRows: 204_760,
      bulkChunks: 205,
      listLoadedRows: 28_946,
      listChunks: 29,
    });
    expect(status.sunbizMatch).toEqual({
      matchedAddressRoles: 21_512,
      registrations: 12_432,
      properties: 9_023,
      chunks: 22,
    });
    expect(JSON.stringify(status)).not.toContain("504108BJ0140");
    expect(JSON.stringify(status)).not.toContain("owner");
    expect(JSON.stringify(status)).not.toContain("address");
  });

  it("reconciles current permit routes without counting supplemental coverage", () => {
    const routes = buildBrowardPermitRouteStatus();
    expect(routes).toMatchObject({
      registryVersion: "2026-09-01.2",
      totalCurrentRoutes: 32,
      implementedCurrentRoutes: 24,
      blockedCurrentRoutes: 8,
    });
    expect(routes.implementedJurisdictions).toHaveLength(24);
    expect(routes.blockerCategories).toEqual([
      {
        key: "software_or_transport",
        kind: "software_transport",
        label: "Software / transport",
        count: 1,
        jurisdictions: ["Lauderdale Lakes"],
      },
      {
        key: "captcha_required",
        kind: "source_policy",
        label: "CAPTCHA required",
        count: 3,
        jurisdictions: ["Coral Springs", "Hillsboro Beach", "Pembroke Park"],
      },
      {
        key: "login_required",
        kind: "source_policy",
        label: "Login required",
        count: 2,
        jurisdictions: ["North Lauderdale", "Parkland"],
      },
      {
        key: "no_anonymous_search",
        kind: "source_policy",
        label: "No anonymous search",
        count: 1,
        jurisdictions: ["Deerfield Beach"],
      },
      {
        key: "custodian_only",
        kind: "source_policy",
        label: "Custodian only",
        count: 1,
        jurisdictions: ["Sea Ranch Lakes"],
      },
    ]);
    expect(routes.implementedCurrentRoutes + routes.blockedCurrentRoutes).toBe(
      routes.totalCurrentRoutes,
    );
    expect(
      routes.blockerCategories.reduce(
        (sum, category) => sum + category.count,
        0,
      ),
    ).toBe(routes.blockedCurrentRoutes);
    expect(
      routes.blockerCategories.find(
        (category) => category.key === "captcha_required",
      )?.jurisdictions,
    ).not.toContain("Deerfield Beach");
    expect(
      routes.blockerCategories.find(
        (category) => category.key === "custodian_only",
      )?.jurisdictions,
    ).not.toContain("Sunrise");
  });

  it("reads aggregate permit worker checkpoints without exposing source rows", async () => {
    const root = await mkdtemp(
      path.join(tmpdir(), "broward-recovery-dashboard-permits-"),
    );
    temporaryDirectories.push(root);
    const hollywoodDirectory = path.join(
      root,
      "downloads/broward/accela-csv-windows/hollywood-full",
    );
    const oaklandDirectory = path.join(
      root,
      "downloads/broward/tyler-date-windows/oakland-park-full-30d",
    );
    const plantationDirectory = path.join(
      root,
      "downloads/broward/accela-csv-windows/plantation-full-v2",
    );
    const cooperDirectory = path.join(
      root,
      "downloads/broward/accela-csv-windows/cooper-city-full",
    );
    await Promise.all([
      mkdir(hollywoodDirectory, { recursive: true }),
      mkdir(oaklandDirectory, { recursive: true }),
      mkdir(plantationDirectory, { recursive: true }),
      mkdir(path.join(cooperDirectory, "property-gap-fill"), {
        recursive: true,
      }),
    ]);
    await writeFile(
      path.join(hollywoodDirectory, "checkpoint.private.json"),
      JSON.stringify({
        pendingWindows: [{ startDate: "PRIVATE", endDate: "PRIVATE" }],
        completedWindows: {
          one: {
            exportedRecordCount: 43,
            excludedNonPermitCount: 2,
          },
        },
        updatedAt: "2026-08-31T21:59:00.000Z",
      }),
    );
    await writeFile(
      path.join(oaklandDirectory, "checkpoint.private.json"),
      JSON.stringify({
        pendingWindows: [],
        completedWindows: {
          one: {
            totalFound: 10,
            invalidRecordCount: 0,
            sourceMissingRecordCount: 1,
          },
        },
        updatedAt: "2026-08-31T21:00:00.000Z",
      }),
    );
    await writeFile(
      path.join(plantationDirectory, "checkpoint.private.json"),
      JSON.stringify({
        pendingWindows: [{ startDate: "PRIVATE", endDate: "PRIVATE" }],
        completedWindows: {},
        updatedAt: "2026-08-31T20:00:00.000Z",
        cooldown: {
          reason: "timeout",
          attemptCount: 2,
          cooldownMs: 3_600_000,
          scheduledAt: "2026-08-31T22:00:00.000Z",
          nextAttemptAt: "2026-08-31T23:00:00.000Z",
        },
      }),
    );
    await writeFile(
      path.join(cooperDirectory, "checkpoint.private.json"),
      JSON.stringify({
        pendingWindows: [{ startDate: "PRIVATE", endDate: "PRIVATE" }],
        completedWindows: {},
        updatedAt: "2026-08-31T20:00:00.000Z",
        cooldown: null,
      }),
    );
    await writeFile(
      path.join(
        cooperDirectory,
        "property-gap-fill",
        "checkpoint.private.json",
      ),
      JSON.stringify({
        plans: {
          one: {
            inspectedPropertyCount: 1,
            retainedRecordCount: 2,
            seedExhausted: false,
          },
        },
        cooldown: null,
        updatedAt: "2026-08-31T21:59:30.000Z",
      }),
    );
    const status = await readPermitEnumerationStatus(
      root,
      Date.parse("2026-08-31T22:00:00.000Z"),
    );
    expect(status).toMatchObject({
      activeWorkers: 2,
      completedWorkers: 1,
      completedWindows: 2,
      totalWindows: 4,
      accessibleRecords: 54,
      excludedRecords: 2,
      invalidRecords: 0,
      sourceMissingRecords: 1,
    });
    expect(status.workers).toHaveLength(8);
    expect(status.pausedWorkers).toEqual([]);
    expect(status.coolingWorkers).toEqual([
      {
        source: "Plantation",
        reason: "timeout",
        nextAttemptAt: "2026-08-31T23:00:00.000Z",
      },
    ]);
    expect(status.workers).toContainEqual(
      expect.objectContaining({
        source: "Plantation",
        status: "cooling_down",
        cooldownReason: "timeout",
        nextAttemptAt: "2026-08-31T23:00:00.000Z",
      }),
    );
    expect(status.workers).toContainEqual(
      expect.objectContaining({
        source: "Cooper City",
        status: "running",
        accessibleRecords: 2,
        updatedAt: "2026-08-31T21:59:30.000Z",
      }),
    );
    expect(status.workers).toContainEqual(
      expect.objectContaining({
        source: "Hollywood",
        status: "running",
        completedWindows: 1,
        pendingWindows: 1,
        accessibleRecords: 43,
      }),
    );
    expect(JSON.stringify(status)).not.toContain("PRIVATE");
    expect(buildBrowardPermitRouteStatus().blockedCurrentRoutes).toBe(8);
  });

  it("counts legacy, canonical, and list-only Accela receipts compatibly", () => {
    expect(
      readAccelaCsvReceiptAccessibleCount({ exportedRecordCount: 43 }),
    ).toBe(43);
    expect(readAccelaCsvReceiptAccessibleCount({ recordCount: 17 })).toBe(17);
    expect(
      readAccelaCsvReceiptAccessibleCount({
        recordCount: 23,
        listRecordCount: 23,
      }),
    ).toBe(23);
    expect(() =>
      readAccelaCsvReceiptAccessibleCount({
        recordCount: 23,
        exportedRecordCount: 22,
      }),
    ).toThrow("record counts conflict");
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
    expect(() => parseDashboardOptions([], {})).toThrow(/expected-branch-id/u);
  });
});
