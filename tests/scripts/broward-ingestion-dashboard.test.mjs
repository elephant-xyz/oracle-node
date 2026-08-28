import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import {
  BROWARD_ROW_DENOMINATOR,
  DEFAULT_DASHBOARD_HOST,
  DEFAULT_DASHBOARD_PORT,
  calculateThroughput,
  createDashboardServer,
  createStatusReader,
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
