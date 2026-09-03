import { describe, expect, it } from "vitest";

import {
  createActivePermitEnumerationTracker,
  detectActiveEnumerationProcessDetails,
  detectActiveEnumerationProcesses,
  markActivePermitEnumerationSnapshotStale,
} from "../../scripts/broward-active-permit-enumeration.mjs";

/**
 * @typedef {import("../../scripts/broward-active-permit-enumeration.mjs").ActiveEnumerationRouteDefinition} ActiveEnumerationRouteDefinition
 * @typedef {import("../../scripts/broward-active-permit-enumeration.mjs").EnumerationWorkerAggregate} EnumerationWorkerAggregate
 */

/** @type {readonly ActiveEnumerationRouteDefinition[]} */
const DEFINITIONS = Object.freeze([
  {
    key: "full-property",
    jurisdiction: "Full Property",
    method: "full",
    family: "municipal_property",
    countSource: "local_checkpoint",
    processScript: "run-broward-municipal-enumeration-supervisor.mjs",
    processJurisdictionKey: "full_property",
  },
  {
    key: "full-type",
    jurisdiction: "Full Type",
    method: "full",
    family: "municipal_type",
    countSource: "local_checkpoint",
    processScript: "run-broward-municipal-enumeration-supervisor.mjs",
    processJurisdictionKey: "full_type",
  },
  ...Array.from({ length: 8 }, (_, index) => ({
    key: `property-${String(index)}`,
    jurisdiction: `Property ${String(index)}`,
    method: /** @type {const} */ ("property_first"),
    family: /** @type {const} */ ("citizenserve"),
    countSource: /** @type {const} */ ("durable_route_checkpoint"),
    processScript: "run-broward-supported-permit-ingest.mjs",
    processJurisdictionKey: `property-${String(index)}`,
  })),
]);

/**
 * Build ten reconciled worker aggregates for one observation.
 *
 * @param {number} completedUnits - Full Property completed count.
 * @param {number} nowMs - Observation epoch used by checkpoint timestamps.
 * @param {{
 *   fullPropertyStatus?:EnumerationWorkerAggregate["status"],
 *   fullPropertyPending?:number,
 *   fullPropertyUpdatedAt?:string
 * }} [overrides={}] - Focused status/count overrides.
 * @returns {EnumerationWorkerAggregate[]} Complete aggregate worker list.
 */
function workersAt(completedUnits, nowMs, overrides = {}) {
  return DEFINITIONS.map((definition) => {
    const isFullProperty = definition.key === "full-property";
    const totalWindows = 100;
    const completedWindows = isFullProperty ? completedUnits : 20;
    const pendingWindows = isFullProperty
      ? (overrides.fullPropertyPending ?? totalWindows - completedWindows)
      : totalWindows - completedWindows;
    return {
      source: definition.jurisdiction,
      family: definition.family,
      status: isFullProperty
        ? (overrides.fullPropertyStatus ?? "running")
        : "running",
      completedWindows,
      pendingWindows,
      totalWindows,
      accessibleRecords: isFullProperty ? 75 : 5,
      sourceMissingRecords: isFullProperty ? 3 : 0,
      deferredCapCount: isFullProperty ? 2 : 0,
      updatedAt: isFullProperty
        ? (overrides.fullPropertyUpdatedAt ??
          new Date(nowMs - 1_000).toISOString())
        : new Date(nowMs - 1_000).toISOString(),
    };
  });
}

/**
 * Build a successful process snapshot for every configured route.
 *
 * @param {readonly string[]} [detailRouteKeys=[]] - Routes with a live detail child.
 * @returns {import("../../scripts/broward-active-permit-enumeration.mjs").ActiveEnumerationProcessSnapshot} Live process map.
 */
function allProcessesAlive(detailRouteKeys = []) {
  return {
    available: true,
    routeKeys: new Set(DEFINITIONS.map((definition) => definition.key)),
    detailRouteKeys: new Set(detailRouteKeys),
    supervisorNotBeforeByKey: new Map(),
  };
}

describe("Broward active permit enumeration telemetry", () => {
  it("detects exact full and property-first runner arguments", () => {
    const live = detectActiveEnumerationProcesses(DEFINITIONS, [
      "node scripts/run-broward-municipal-enumeration-supervisor.mjs --runner property -- --jurisdiction full_property",
      "node scripts/run-broward-supported-permit-ingest.mjs --jurisdictions property-0,property-3",
      "node scripts/unrelated.mjs --jurisdiction full_type",
    ]);
    expect([...live].sort()).toEqual([
      "full-property",
      "property-0",
      "property-3",
    ]);
  });

  it("reports recent bounded detail descendants and operator boundaries", () => {
    const details = detectActiveEnumerationProcessDetails(DEFINITIONS, [
      {
        pid: 10,
        parentPid: 1,
        elapsedSeconds: 3_600,
        command:
          "node scripts/run-broward-municipal-enumeration-supervisor.mjs --runner property --not-before 2026-09-04T10:06:00.000Z -- --jurisdiction full_property",
      },
      {
        pid: 11,
        parentPid: 10,
        elapsedSeconds: 600,
        command: "node scripts/probe-broward-municipal-permits.mjs",
      },
      {
        pid: 20,
        parentPid: 1,
        elapsedSeconds: 3_600,
        command:
          "node scripts/run-broward-supported-permit-ingest.mjs --jurisdictions property-0",
      },
      {
        pid: 21,
        parentPid: 20,
        elapsedSeconds: 901,
        command: "node scripts/probe-broward-bcs-permits.mjs",
      },
    ]);

    expect(details.routeKeys).toEqual(new Set(["full-property", "property-0"]));
    expect(details.detailRouteKeys).toEqual(new Set(["full-property"]));
    expect(details.supervisorNotBeforeByKey.get("full-property")).toBe(
      "2026-09-04T10:06:00.000Z",
    );
  });

  it("classifies process state separately from checkpoint movement", () => {
    const tracker = createActivePermitEnumerationTracker(DEFINITIONS);
    const start = Date.parse("2026-09-03T12:00:00.000Z");
    tracker(workersAt(10, start), allProcessesAlive(), start);
    const moving = tracker(
      workersAt(12, start + 60_000),
      allProcessesAlive(),
      start + 60_000,
    ).workers[0];
    expect(moving).toMatchObject({
      state: "running",
      processAlive: true,
      checkpointActivity: "work_units_advanced",
      completedUnits: 12,
      totalUnits: 100,
      remainingUnits: 88,
      completionPercent: 12,
      locallyCapturedRecords: 75,
      durableLoadedRecords: null,
      deferredCapCount: 2,
      sourceMissingCount: 3,
    });

    const stalledTracker = createActivePermitEnumerationTracker(DEFINITIONS);
    const stalled = stalledTracker(
      workersAt(12, start, {
        fullPropertyUpdatedAt: new Date(start - 10 * 60_000).toISOString(),
      }),
      allProcessesAlive(),
      start,
    ).workers[0];
    expect(stalled).toMatchObject({
      state: "stalled",
      processAlive: true,
      checkpointStale: true,
    });

    const absentTracker = createActivePermitEnumerationTracker(DEFINITIONS);
    const paused = absentTracker(
      workersAt(12, start),
      {
        available: true,
        routeKeys: new Set(),
        detailRouteKeys: new Set(),
        supervisorNotBeforeByKey: new Map(),
      },
      start,
    ).workers[0];
    expect(paused).toMatchObject({ state: "paused", processAlive: false });
  });

  it("keeps a recent live detail child running past checkpoint staleness", () => {
    const tracker = createActivePermitEnumerationTracker(DEFINITIONS);
    const now = Date.parse("2026-09-03T12:10:00.000Z");
    const worker = tracker(
      workersAt(12, now, {
        fullPropertyUpdatedAt: new Date(now - 10 * 60_000).toISOString(),
      }),
      allProcessesAlive(["full-property"]),
      now,
    ).workers[0];

    expect(worker).toMatchObject({
      state: "running",
      processAlive: true,
      detailActive: true,
      checkpointStale: true,
      eta: {
        kind: "unknown",
        reason: "detail_activity",
      },
    });
  });

  it("does not claim automatic cooling without a live supervisor", () => {
    const tracker = createActivePermitEnumerationTracker(DEFINITIONS);
    const now = Date.parse("2026-09-03T12:10:00.000Z");
    const worker = tracker(
      workersAt(12, now, {
        fullPropertyStatus: "cooling_down",
        fullPropertyUpdatedAt: new Date(now - 60_000).toISOString(),
      }),
      {
        available: true,
        routeKeys: new Set(),
        detailRouteKeys: new Set(),
        supervisorNotBeforeByKey: new Map(),
      },
      now,
    ).workers[0];

    expect(worker).toMatchObject({
      state: "paused",
      processAlive: false,
      detailActive: false,
      eta: {
        kind: "unknown",
        reason: "worker_not_running",
      },
    });
  });

  it("emits an ETA range only for stable observed work-unit rates", () => {
    const tracker = createActivePermitEnumerationTracker(DEFINITIONS);
    const start = Date.parse("2026-09-03T12:00:00.000Z");
    for (const [offset, completed] of [
      [0, 0],
      [100_000, 10],
      [200_000, 20],
    ]) {
      tracker(
        workersAt(completed, start + offset),
        allProcessesAlive(),
        start + offset,
      );
    }
    const status = tracker(
      workersAt(30, start + 300_000),
      allProcessesAlive(),
      start + 300_000,
    );
    const stable = status.workers[0];
    expect(stable?.throughput).toEqual({
      observedUnits: 30,
      windowSeconds: 300,
      unitsPerHour: 360,
      variabilityRatio: 1,
    });
    expect(stable?.eta).toEqual({
      kind: "estimate",
      estimatedHours: 0.194,
      lowHours: 0.194,
      highHours: 0.194,
      reason: "rate_stable",
    });
    expect(status.workers[1]?.eta).toMatchObject({
      kind: "unknown",
      reason: "variable_detail_loop",
    });
  });

  it("keeps no-movement and high-variability ETAs unknown", () => {
    const noMovementTracker = createActivePermitEnumerationTracker(DEFINITIONS);
    const start = Date.parse("2026-09-03T12:00:00.000Z");
    for (const offset of [0, 100_000, 200_000, 300_000]) {
      noMovementTracker(
        workersAt(10, start + offset),
        allProcessesAlive(),
        start + offset,
      );
    }
    const noMovement = noMovementTracker(
      workersAt(10, start + 360_000),
      allProcessesAlive(),
      start + 360_000,
    ).workers[0];
    expect(noMovement?.eta).toMatchObject({
      kind: "unknown",
      reason: "no_checkpoint_movement",
    });

    const variableTracker = createActivePermitEnumerationTracker(DEFINITIONS);
    for (const [offset, completed] of [
      [0, 0],
      [100_000, 1],
      [200_000, 20],
      [300_000, 21],
    ]) {
      variableTracker(
        workersAt(completed, start + offset),
        allProcessesAlive(),
        start + offset,
      );
    }
    const variable = variableTracker(
      workersAt(21, start + 300_000),
      allProcessesAlive(),
      start + 300_000,
    ).workers[0];
    expect(variable?.eta).toMatchObject({
      kind: "unknown",
      reason: "rate_variability_high",
    });
  });

  it("marks stale fallback snapshots and refuses their live ETA", () => {
    const tracker = createActivePermitEnumerationTracker(DEFINITIONS);
    const start = Date.parse("2026-09-03T12:00:00.000Z");
    for (const [offset, completed] of [
      [0, 0],
      [100_000, 10],
      [200_000, 20],
      [300_000, 30],
    ]) {
      tracker(
        workersAt(completed, start + offset),
        allProcessesAlive(),
        start + offset,
      );
    }
    const fresh = tracker(
      workersAt(31, start + 310_000),
      allProcessesAlive(),
      start + 310_000,
    );
    const stale = markActivePermitEnumerationSnapshotStale(
      fresh,
      start + 20 * 60_000,
    );
    expect(stale.snapshotStale).toBe(true);
    expect(stale.workers[0]).toMatchObject({
      processAlive: null,
      detailActive: null,
      checkpointStale: true,
      eta: {
        kind: "unknown",
        reason: "dashboard_snapshot_stale",
      },
    });
  });

  it("rejects checkpoint count reconciliation failures", () => {
    const tracker = createActivePermitEnumerationTracker(DEFINITIONS);
    const now = Date.parse("2026-09-03T12:00:00.000Z");
    expect(() =>
      tracker(
        workersAt(10, now, { fullPropertyPending: 91 }),
        allProcessesAlive(),
        now,
      ),
    ).toThrow(/aggregate counters|reconcile/u);
  });
});
