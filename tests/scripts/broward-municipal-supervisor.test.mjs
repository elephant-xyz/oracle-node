import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  parseMunicipalSupervisorOptions,
  runMunicipalEnumerationSupervisor,
} from "../../scripts/run-broward-municipal-enumeration-supervisor.mjs";

/** @type {string[]} */
const temporaryDirectories = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

/**
 * Create one private supervisor test directory.
 *
 * @returns {Promise<string>} Registered temporary output directory.
 */
async function createOutputDirectory() {
  const directory = await mkdtemp(
    path.join(tmpdir(), "broward-municipal-supervisor-"),
  );
  temporaryDirectories.push(directory);
  await mkdir(directory, { recursive: true });
  return directory;
}

/**
 * Write one aggregate property checkpoint accepted by the supervisor.
 *
 * @param {string} outputDirectory - Test runner output root.
 * @param {object} state - Mutable aggregate state.
 * @param {"running" | "paused" | "cooling" | "complete"} state.status - Durable status.
 * @param {"source_cap" | "timeout" | "incomplete_pagination" | "source_error" | null} state.blocker - Safe blocker.
 * @param {string | null} state.nextAttemptAt - Optional retry deadline.
 * @param {number} [state.nextQueryIndex=1] - Current property plan position.
 * @param {number} [state.totalQueries=2] - Property plan size.
 * @param {Record<string, {nextAttemptAt:string | null}>} [state.deferredCapItems={}] - Bounded cap ledger.
 * @returns {Promise<void>} Resolves after atomic test setup.
 */
async function writePropertyCheckpoint(
  outputDirectory,
  {
    status,
    blocker,
    nextAttemptAt,
    nextQueryIndex = 1,
    totalQueries = 2,
    deferredCapItems = {},
  },
) {
  await writeFile(
    path.join(outputDirectory, "checkpoint.private.json"),
    JSON.stringify({
      schemaVersion: "oracle-node.broward-municipal-property-enumeration.v1",
      jurisdictionKey: "margate",
      sourceSystem: "fixture_margate",
      queryPlanSha256: "fixture-plan",
      seedSha256: "fixture-seed",
      status,
      blocker,
      nextAttemptAt,
      nextQueryIndex,
      totalQueries,
      deferredCapItems,
    }),
  );
}

describe("Broward municipal enumeration supervisor", () => {
  it("parses bounded supervisor flags separately from strict runner flags", () => {
    expect(
      parseMunicipalSupervisorOptions([
        "--runner",
        "property",
        "--max-attempts",
        "4",
        "--not-before",
        "2026-09-04T10:06:00.000Z",
        "--",
        "--jurisdiction",
        "margate",
        "--seed",
        "fixture.csv",
        "--output-dir",
        "fixture-output",
      ]),
    ).toMatchObject({
      runnerKind: "property",
      maxAttempts: 4,
      notBeforeAt: "2026-09-04T10:06:00.000Z",
      runnerOptions: {
        jurisdictionKey: "margate",
      },
    });
  });

  it("waits for each checkpoint cooldown and resumes finite source failures", async () => {
    const outputDirectory = await createOutputDirectory();
    let nowMs = Date.parse("2026-09-03T15:00:00.000Z");
    await writePropertyCheckpoint(outputDirectory, {
      status: "cooling",
      blocker: "timeout",
      nextAttemptAt: "2026-09-03T15:00:05.000Z",
    });
    const waits = [];
    const runProperty = vi.fn(async () => {
      if (runProperty.mock.calls.length === 1) {
        await writePropertyCheckpoint(outputDirectory, {
          status: "cooling",
          blocker: "source_error",
          nextAttemptAt: "2026-09-03T15:00:12.000Z",
        });
      } else {
        await writePropertyCheckpoint(outputDirectory, {
          status: "complete",
          blocker: null,
          nextAttemptAt: null,
          nextQueryIndex: 2,
          totalQueries: 2,
        });
      }
      return /** @type {never} */ ({});
    });

    const summary = await runMunicipalEnumerationSupervisor(
      parseMunicipalSupervisorOptions([
        "--runner",
        "property",
        "--max-attempts",
        "3",
        "--",
        "--jurisdiction",
        "margate",
        "--seed",
        "fixture.csv",
        "--output-dir",
        outputDirectory,
      ]),
      {
        now: () => nowMs,
        wait: async (milliseconds) => {
          waits.push(milliseconds);
          nowMs += milliseconds;
        },
        runProperty,
      },
    );

    expect(waits).toEqual([5_000, 7_000]);
    expect(runProperty).toHaveBeenCalledTimes(2);
    expect(summary).toMatchObject({
      status: "complete",
      attempts: 2,
      checkpointStatus: "complete",
      blocker: null,
    });
  });

  it("enforces a later operator boundary before the first runner call", async () => {
    const outputDirectory = await createOutputDirectory();
    let nowMs = Date.parse("2026-09-03T15:00:00.000Z");
    const notBeforeAt = "2026-09-03T15:00:10.000Z";
    await writePropertyCheckpoint(outputDirectory, {
      status: "running",
      blocker: null,
      nextAttemptAt: null,
    });
    const runProperty = vi.fn(async () => {
      expect(nowMs).toBe(Date.parse(notBeforeAt));
      await writePropertyCheckpoint(outputDirectory, {
        status: "complete",
        blocker: null,
        nextAttemptAt: null,
        nextQueryIndex: 2,
        totalQueries: 2,
      });
      return /** @type {never} */ ({});
    });

    const summary = await runMunicipalEnumerationSupervisor(
      parseMunicipalSupervisorOptions([
        "--runner",
        "property",
        "--not-before",
        notBeforeAt,
        "--",
        "--jurisdiction",
        "margate",
        "--seed",
        "fixture.csv",
        "--output-dir",
        outputDirectory,
      ]),
      {
        now: () => nowMs,
        wait: async (milliseconds) => {
          nowMs += milliseconds;
        },
        runProperty,
      },
    );

    expect(runProperty).toHaveBeenCalledOnce();
    expect(summary.status).toBe("complete");
  });

  it("does not restart a terminal source-cap checkpoint", async () => {
    const outputDirectory = await createOutputDirectory();
    await writeFile(
      path.join(outputDirectory, "checkpoint.private.json"),
      JSON.stringify({
        schemaVersion:
          "oracle-node.broward-municipal-record-type-enumeration.v1",
        jurisdictionKey: "lighthouse_point",
        sourceSystem: "fixture_lighthouse",
        configurationSha256: "fixture-configuration",
        status: "paused",
        blocker: "source_cap",
        nextAttemptAt: null,
      }),
    );
    const runType = vi.fn();

    const summary = await runMunicipalEnumerationSupervisor(
      parseMunicipalSupervisorOptions([
        "--runner",
        "type",
        "--",
        "--jurisdiction",
        "lighthouse_point",
        "--output-dir",
        outputDirectory,
      ]),
      { runType },
    );

    expect(runType).not.toHaveBeenCalled();
    expect(summary).toMatchObject({
      status: "terminal_blocker",
      attempts: 0,
      blocker: "source_cap",
    });
  });

  it("stops after the configured finite attempt count", async () => {
    const outputDirectory = await createOutputDirectory();
    let nowMs = Date.parse("2026-09-03T15:00:00.000Z");
    await writePropertyCheckpoint(outputDirectory, {
      status: "running",
      blocker: null,
      nextAttemptAt: null,
    });
    const runProperty = vi.fn(async () => {
      await writePropertyCheckpoint(outputDirectory, {
        status: "running",
        blocker: null,
        nextAttemptAt: null,
      });
      return /** @type {never} */ ({});
    });

    const summary = await runMunicipalEnumerationSupervisor(
      parseMunicipalSupervisorOptions([
        "--runner",
        "property",
        "--max-attempts",
        "2",
        "--",
        "--jurisdiction",
        "margate",
        "--seed",
        "fixture.csv",
        "--output-dir",
        outputDirectory,
      ]),
      {
        now: () => nowMs,
        wait: async (milliseconds) => {
          nowMs += milliseconds;
        },
        runProperty,
      },
    );

    expect(runProperty).toHaveBeenCalledTimes(2);
    expect(summary).toMatchObject({
      status: "attempt_limit",
      attempts: 2,
      checkpointStatus: "running",
    });
  });
});
