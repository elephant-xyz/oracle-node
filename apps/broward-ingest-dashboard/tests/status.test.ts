import { describe, expect, it } from "vitest";

import {
  buildDashboardStatus,
  type StatusSnapshot,
} from "../src/shared/status";

const NOW_MS = Date.parse("2026-08-29T01:00:00.000Z");

/**
 * Build a valid aggregate fixture with narrow per-test overrides.
 *
 * @param overrides - Optional status fields to replace.
 * @returns Reconciled aggregate snapshot containing no private source values.
 */
function snapshot(overrides: Partial<StatusSnapshot> = {}): StatusSnapshot {
  return {
    attempted: 70,
    categories: [
      { categoryKey: "Residential", succeededCount: 50 },
      { categoryKey: "Commercial", succeededCount: 10 },
    ],
    denominator: 100,
    heartbeatAt: "2026-08-29T00:59:45.000Z",
    loadFailures: 1,
    phase: "full",
    sourceFailures: 4,
    sourceMisses: 5,
    staleAfterSeconds: 180,
    startedAt: "2026-08-28T23:00:00.000Z",
    succeeded: 60,
    throughputAttempted: 30,
    throughputWindowSeconds: 600,
    transformFailures: 2,
    ...overrides,
  };
}

describe("aggregate dashboard status", () => {
  it("calculates durable completion, throughput, ETA, and category coverage", () => {
    const status = buildDashboardStatus(snapshot(), NOW_MS);

    expect(status).toMatchObject({
      county: "Broward",
      pipeline: "Appraisal",
      dataSource: "neon",
      health: {
        state: "online",
        heartbeatAgeSeconds: 15,
        staleAfterSeconds: 180,
      },
      progress: {
        denominator: 100,
        attempted: 70,
        succeeded: 60,
        sourceMisses: 5,
        completed: 65,
        remaining: 35,
        completionPercent: 65,
      },
      throughput: {
        windowSeconds: 600,
        attemptedInWindow: 30,
        attemptedPerMinute: 3,
        etaSeconds: 700,
        projectedCompletionAt: "2026-08-29T01:11:40.000Z",
      },
      categoryCoverage: [
        {
          category: "Residential",
          succeeded: 50,
          percentOfSucceeded: 83.33,
        },
        {
          category: "Commercial",
          succeeded: 10,
          percentOfSucceeded: 16.67,
        },
      ],
    });
  });

  it("marks an old heartbeat stale and withholds a misleading ETA", () => {
    const status = buildDashboardStatus(
      snapshot({
        heartbeatAt: "2026-08-29T00:56:59.000Z",
      }),
      NOW_MS,
    );

    expect(status.health).toMatchObject({
      state: "stale",
      heartbeatAgeSeconds: 181,
    });
    expect(status.throughput.attemptedPerMinute).toBe(3);
    expect(status.throughput.etaSeconds).toBeNull();
    expect(status.throughput.projectedCompletionAt).toBeNull();
  });

  it("distinguishes never-started and offline checkpoint states", () => {
    const neverStarted = buildDashboardStatus(
      snapshot({
        attempted: 0,
        categories: [],
        heartbeatAt: null,
        phase: "not_started",
        sourceMisses: 0,
        succeeded: 0,
        throughputAttempted: 0,
      }),
      NOW_MS,
    );
    const offline = buildDashboardStatus(
      snapshot({
        attempted: 10,
        categories: [],
        heartbeatAt: null,
        sourceMisses: 1,
        succeeded: 8,
        throughputAttempted: 0,
      }),
      NOW_MS,
    );

    expect(neverStarted.health.state).toBe("not_started");
    expect(offline.health.state).toBe("offline");
    expect(offline.throughput.etaSeconds).toBeNull();
  });

  it("treats complete progress as complete even after heartbeats stop", () => {
    const status = buildDashboardStatus(
      snapshot({
        attempted: 100,
        categories: [{ categoryKey: "Residential", succeededCount: 97 }],
        heartbeatAt: "2026-08-28T20:00:00.000Z",
        phase: "complete",
        sourceMisses: 3,
        succeeded: 97,
      }),
      NOW_MS,
    );

    expect(status.health.state).toBe("complete");
    expect(status.progress.completionPercent).toBe(100);
    expect(status.throughput.etaSeconds).toBe(0);
  });

  it("never emits invalid category text or private contract fields", () => {
    const status = buildDashboardStatus(
      snapshot({
        categories: [
          { categoryKey: "Residential", succeededCount: 59 },
          {
            categoryKey: "private-owner@example.invalid",
            succeededCount: 1,
          },
        ],
      }),
      NOW_MS,
    );
    const serialized = JSON.stringify(status);

    expect(status.categoryCoverage).toEqual([
      {
        category: "Residential",
        succeeded: 59,
        percentOfSucceeded: 98.33,
      },
      { category: "Other", succeeded: 1, percentOfSucceeded: 1.67 },
    ]);
    expect(serialized).not.toContain("private-owner");
    expect(serialized).not.toContain("folio");
    expect(serialized).not.toContain("address");
    expect(serialized).not.toContain("owner");
    expect(serialized).not.toContain("DATABASE_URL");
  });

  it("fails closed on inconsistent aggregate counters", () => {
    expect(() =>
      buildDashboardStatus(
        snapshot({ attempted: 10, succeeded: 11 }),
        NOW_MS,
      ),
    ).toThrow(/do not reconcile/u);
  });
});
