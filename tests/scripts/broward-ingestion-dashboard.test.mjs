import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import {
  BROWARD_ROW_DENOMINATOR,
  DEFAULT_DASHBOARD_HOST,
  DEFAULT_DASHBOARD_PORT,
  calculateThroughput,
  combineHandoffStatuses,
  createDashboardServer,
  createDefaultStatusReader,
  createStatusReader,
  parseHandoffManifest,
  parseDashboardCliOptions,
} from "../../scripts/broward-ingestion-dashboard.mjs";

/** @type {string[]} */
const temporaryDirectories = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories.splice(0).map((directory) =>
      rm(directory, { recursive: true, force: true }),
    ),
  );
});

describe("Broward ingestion dashboard", () => {
  it("uses a safe local default while accepting explicit host and port options", () => {
    expect(parseDashboardCliOptions([])).toEqual({
      host: DEFAULT_DASHBOARD_HOST,
      port: DEFAULT_DASHBOARD_PORT,
      help: false,
    });
    expect(
      parseDashboardCliOptions([
        "--host",
        "0.0.0.0",
        "--port",
        "48191",
      ]),
    ).toEqual({
      host: "0.0.0.0",
      port: 48_191,
      help: false,
    });
    expect(() => parseDashboardCliOptions(["--port", "80"])).toThrow(
      /1024 through 65535/u,
    );
    expect(() => parseDashboardCliOptions(["--files", "/tmp"])).toThrow(
      /Unknown option/u,
    );
  });

  it("calculates recent throughput and caps inactive gaps in active runtime", () => {
    const nowMs = Date.parse("2026-08-28T18:00:00.000Z");
    const startedAtMs = nowMs - 60 * 60 * 1_000;
    const rowTimestamps = new Float64Array([
      startedAtMs + 1_000,
      startedAtMs + 2_000,
      nowMs - 60_000,
      nowMs - 30_000,
    ]);
    const metrics = calculateThroughput({
      attempted: 4,
      remaining: 96,
      startedAtMs,
      rowTimestamps,
      nextRowIndex: 4,
      nowMs,
      isActivelyRunning: true,
    });

    expect(metrics.recentAttempted).toBe(2);
    expect(metrics.recentPerMinute).toBe(0.13);
    expect(metrics.activeRuntimeSeconds).toBe(182);
    expect(metrics.activeRuntimeSeconds).toBeLessThan(60 * 60);
    expect(metrics.etaBasis).toBe("recent");
    expect(metrics.etaActiveSeconds).toBe(43_200);
  });

  it("validates and combines an immutable data-only handoff contract", () => {
    const repositoryRoot = "/workspace";
    const manifest = parseHandoffManifest(
      JSON.stringify({
        seedRowCount: 10,
        sourceMode: "live-bcpa",
        sourceConcurrencyMaximum: 4,
        oldOutputDirectory: "downloads/broward/full-ingestion",
        oldCheckpoint: { nextRowIndex: 4, attempted: 4 },
        newOutputDirectory: "downloads/broward/full-query-data-only-from-4",
        newLogPath:
          "downloads/broward/full-query-data-only-from-4/ingestion.log",
        newArtifactMode: "query-data-only",
        newInitialRowIndex: 4,
        reconciliation: {
          excludedOldAtOrAboveBoundary: {
            resultRowIndexes: [],
            artifactRowIndexes: [4],
            captureRowIndexes: [4, 5, 6, 7],
          },
        },
      }),
      repositoryRoot,
    );
    /** @type {Parameters<typeof combineHandoffStatuses>[0]} */
    const baseStatus = {
      schemaVersion: 1,
      generatedAt: "2026-08-28T18:00:00.000Z",
      county: "Broward",
      denominator: 10,
      process: {
        status: "stopped",
        running: false,
        stale: false,
        lastActivityAt: "2026-08-28T17:59:00.000Z",
        activityAgeSeconds: 60,
        staleAfterSeconds: 120,
      },
      progress: {
        attempted: 4,
        succeeded: 3,
        skippedExisting: 0,
        sourceMisses: 1,
        sourceErrors: 0,
        transformErrors: 0,
        unclassifiedFailures: 0,
        failedTotal: 1,
        remaining: 6,
        completionPercent: 40,
      },
      throughput: {
        windowMinutes: 15,
        recentAttempted: 2,
        recentPerMinute: 0.13,
        activeRuntimeSeconds: 60,
        activeAveragePerMinute: 4,
        etaActiveSeconds: 90,
        etaBasis: "recent",
        projectedCompletionAt: "2026-08-28T18:01:30.000Z",
      },
      checkpoint: {
        lastCheckpointAt: "2026-08-28T17:59:00.000Z",
        ageSeconds: 60,
      },
      usageTypes: [{ type: "Residential", count: 3 }],
      storage: {
        available: true,
        totalBytes: 1_000,
        freeBytes: 500,
        usedPercent: 50,
        files: {
          state: {
            available: true,
            sizeBytes: 100,
            modifiedAt: "2026-08-28T17:59:00.000Z",
            ageSeconds: 60,
          },
          results: {
            available: true,
            sizeBytes: 200,
            modifiedAt: "2026-08-28T17:59:00.000Z",
            ageSeconds: 60,
          },
          log: {
            available: true,
            sizeBytes: 300,
            modifiedAt: "2026-08-28T17:59:00.000Z",
            ageSeconds: 60,
          },
        },
        parsedResultRows: 4,
        malformedResultLines: 0,
      },
    };
    const dataOnlyStatus = structuredClone(baseStatus);
    dataOnlyStatus.process.status = "running";
    dataOnlyStatus.process.running = true;
    dataOnlyStatus.progress.attempted = 2;
    dataOnlyStatus.progress.succeeded = 2;
    dataOnlyStatus.progress.sourceMisses = 0;
    dataOnlyStatus.progress.failedTotal = 0;
    dataOnlyStatus.throughput.recentAttempted = 2;
    dataOnlyStatus.throughput.activeRuntimeSeconds = 30;
    dataOnlyStatus.checkpoint.lastCheckpointAt =
      "2026-08-28T18:00:00.000Z";
    dataOnlyStatus.checkpoint.ageSeconds = 0;
    dataOnlyStatus.usageTypes = [{ type: "Residential", count: 2 }];
    dataOnlyStatus.storage.parsedResultRows = 2;

    const combined = combineHandoffStatuses(
      baseStatus,
      dataOnlyStatus,
      manifest,
      Date.parse("2026-08-28T18:00:00.000Z"),
    );

    expect(combined).toMatchObject({
      denominator: 10,
      process: { status: "running", running: true },
      progress: {
        attempted: 6,
        succeeded: 5,
        sourceMisses: 1,
        transformErrors: 0,
        remaining: 4,
        completionPercent: 60,
      },
      handoff: {
        active: true,
        boundaryRowIndex: 4,
        publishableAttempted: 4,
        dataOnlyAttempted: 2,
        dataOnlyTransformErrors: 0,
        excludedOldArtifacts: 1,
        preservedExcludedOldCaptures: 4,
      },
      usageTypes: [{ type: "Residential", count: 5 }],
      storage: { parsedResultRows: 6 },
    });
  });

  it("reads both live segments when the fixed handoff manifest is present", async () => {
    const root = await mkdtemp(
      path.join(tmpdir(), "broward-dashboard-handoff-test-"),
    );
    temporaryDirectories.push(root);
    const browardRoot = path.join(root, "downloads", "broward");
    const oldOutput = path.join(browardRoot, "full-ingestion");
    const newOutput = path.join(
      browardRoot,
      "full-query-data-only-from-2",
    );
    await mkdir(oldOutput, { recursive: true });
    await mkdir(newOutput);
    const oldStartedAt = "2026-08-28T17:00:00.000Z";
    const newStartedAt = "2026-08-28T18:00:00.000Z";
    await writeFile(
      path.join(oldOutput, "state.json"),
      `${JSON.stringify({
        startedAt: oldStartedAt,
        updatedAt: "2026-08-28T17:01:00.000Z",
        nextRowIndex: 2,
        attempted: 2,
        succeeded: 2,
        skippedExisting: 0,
        failed: 0,
        usageTypes: { Residential: 2 },
      })}\n`,
    );
    await writeFile(
      path.join(oldOutput, "results.ndjson"),
      [0, 1]
        .map((rowIndex) =>
          JSON.stringify({
            timestamp: "2026-08-28T17:01:00.000Z",
            rowIndex,
            status: "succeeded",
            folio: `PRIVATE-OLD-${String(rowIndex)}`,
          }),
        )
        .join("\n") + "\n",
    );
    await writeFile(
      path.join(newOutput, "state.json"),
      `${JSON.stringify({
        schemaVersion: "oracle-node.broward-local-ingest-state.v2",
        artifactMode: "query-data-only",
        initialRowIndex: 2,
        startedAt: newStartedAt,
        updatedAt: "2026-08-28T18:01:00.000Z",
        nextRowIndex: 4,
        attempted: 2,
        succeeded: 1,
        skippedExisting: 0,
        failed: 1,
        usageTypes: { Commercial: 1 },
      })}\n`,
    );
    await writeFile(
      path.join(newOutput, "results.ndjson"),
      [
        {
          timestamp: "2026-08-28T18:00:30.000Z",
          rowIndex: 2,
          status: "succeeded",
          folio: "PRIVATE-NEW-2",
        },
        {
          timestamp: "2026-08-28T18:01:00.000Z",
          rowIndex: 3,
          status: "source_error",
          folio: "PRIVATE-NEW-3",
          error: "HTTP 500 near PRIVATE OWNER",
        },
      ]
        .map((result) => JSON.stringify(result))
        .join("\n") + "\n",
    );
    await writeFile(path.join(newOutput, "ingestion.log"), "PRIVATE OWNER\n");
    await writeFile(
      path.join(browardRoot, "active-query-data-only-handoff.json"),
      `${JSON.stringify({
        seedRowCount: 10,
        sourceMode: "live-bcpa",
        sourceConcurrencyMaximum: 4,
        oldOutputDirectory: "downloads/broward/full-ingestion",
        oldCheckpoint: { nextRowIndex: 2, attempted: 2 },
        newOutputDirectory:
          "downloads/broward/full-query-data-only-from-2",
        newLogPath:
          "downloads/broward/full-query-data-only-from-2/ingestion.log",
        newArtifactMode: "query-data-only",
        newInitialRowIndex: 2,
        reconciliation: {
          excludedOldAtOrAboveBoundary: {
            resultRowIndexes: [],
            artifactRowIndexes: [],
            captureRowIndexes: [],
          },
        },
      })}\n`,
    );

    const readStatus = await createDefaultStatusReader(root);
    const status = await readStatus();

    expect(status).toMatchObject({
      denominator: 10,
      progress: {
        attempted: 4,
        succeeded: 3,
        sourceErrors: 1,
        transformErrors: 0,
        remaining: 6,
        completionPercent: 40,
      },
      handoff: {
        boundaryRowIndex: 2,
        publishableAttempted: 2,
        dataOnlyAttempted: 2,
      },
    });
    expect(JSON.stringify(status)).not.toContain("PRIVATE");

    const mismatchedState = JSON.parse(
      await readFile(path.join(newOutput, "state.json"), "utf8"),
    );
    mismatchedState.initialRowIndex = 3;
    await writeFile(
      path.join(newOutput, "state.json"),
      `${JSON.stringify(mismatchedState)}\n`,
    );
    await expect(readStatus()).rejects.toThrow(
      "Checkpoint initial row differs from handoff manifest",
    );
  });

  it("serves aggregate API data without private result or log fields", async () => {
    const root = await mkdtemp(
      path.join(tmpdir(), "broward-dashboard-test-"),
    );
    temporaryDirectories.push(root);
    const outputDirectory = path.join(root, "full-ingestion");
    const logPath = path.join(root, "ingestion.log");
    await mkdir(outputDirectory);
    const nowMs = Date.parse("2026-08-28T18:00:00.000Z");
    const startedAt = "2026-08-28T17:00:00.000Z";
    await writeFile(
      path.join(outputDirectory, "state.json"),
      `${JSON.stringify({
        startedAt,
        updatedAt: "2026-08-28T17:59:55.000Z",
        nextRowIndex: 4,
        attempted: 4,
        succeeded: 1,
        skippedExisting: 0,
        failed: 3,
        usageTypes: {
          Residential: 1,
          "private@example.invalid": 2,
        },
      })}\n`,
    );
    await writeFile(
      path.join(outputDirectory, "results.ndjson"),
      [
        {
          timestamp: "2026-08-28T17:59:51.000Z",
          rowIndex: 0,
          status: "succeeded",
          folio: "PRIVATE-FOLIO-1",
          owner: "PRIVATE OWNER",
          address: "PRIVATE ADDRESS",
          error: null,
        },
        {
          timestamp: "2026-08-28T17:59:52.000Z",
          rowIndex: 1,
          status: "source_error",
          folio: "PRIVATE-FOLIO-2",
          error:
            "Broward appraiser returned no parcelInfok__BackingField for folio PRIVATE-FOLIO-2",
        },
        {
          timestamp: "2026-08-28T17:59:53.000Z",
          rowIndex: 2,
          status: "source_error",
          folio: "PRIVATE-FOLIO-3",
          error: "Broward appraiser returned HTTP 500",
        },
        {
          timestamp: "2026-08-28T17:59:54.000Z",
          rowIndex: 3,
          status: "transform_error",
          folio: "PRIVATE-FOLIO-4",
          error: "Transform failed near PRIVATE OWNER",
        },
      ]
        .map((result) => JSON.stringify(result))
        .join("\n") + "\n",
    );
    await writeFile(logPath, "PRIVATE OWNER PRIVATE ADDRESS\n");

    const readStatus = createStatusReader(
      {
        outputDirectory,
        logPath,
        denominator: BROWARD_ROW_DENOMINATOR,
      },
      {
        now: () => nowMs,
        probeProcess: async () => true,
      },
    );
    const server = createDashboardServer(readStatus);
    await new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    try {
      const address = server.address();
      if (address === null || typeof address === "string") {
        throw new Error("Test dashboard did not bind a TCP port");
      }
      const response = await fetch(
        `http://127.0.0.1:${String(address.port)}/api/status`,
      );
      expect(response.status).toBe(200);
      expect(response.headers.get("cache-control")).toBe("no-store");
      const status = await response.json();
      expect(status).toMatchObject({
        county: "Broward",
        denominator: BROWARD_ROW_DENOMINATOR,
        process: { status: "running", running: true, stale: false },
        progress: {
          attempted: 4,
          succeeded: 1,
          sourceMisses: 1,
          sourceErrors: 1,
          transformErrors: 1,
          unclassifiedFailures: 0,
        },
        usageTypes: [
          { type: "Other", count: 2 },
          { type: "Residential", count: 1 },
        ],
      });
      const responseText = JSON.stringify(status);
      expect(responseText).not.toContain("PRIVATE-FOLIO");
      expect(responseText).not.toContain("PRIVATE OWNER");
      expect(responseText).not.toContain("PRIVATE ADDRESS");
      expect(responseText).not.toContain("private@example.invalid");
      expect(responseText).not.toContain(outputDirectory);

      const healthResponse = await fetch(
        `http://127.0.0.1:${String(address.port)}/healthz`,
      );
      expect(healthResponse.status).toBe(200);
      await expect(healthResponse.json()).resolves.toEqual({
        ok: true,
        service: "broward-ingestion-dashboard",
      });
    } finally {
      await new Promise((resolve, reject) => {
        server.close((error) => {
          if (error === undefined) resolve();
          else reject(error);
        });
      });
    }
  });
});
