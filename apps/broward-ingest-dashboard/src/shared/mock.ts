import {
  BROWARD_DENOMINATOR,
  buildDashboardStatus,
  type DashboardStatus,
  type StatusSnapshot,
} from "./status";

/**
 * Create representative aggregate data for local UI development and tests.
 *
 * Mock mode never reads environment secrets or opens a database connection.
 * The values contain no real parcel, owner, address, or artifact information.
 *
 * @param nowMs - Current Unix epoch time in milliseconds.
 * @returns A fresh, explicitly marked mock dashboard response.
 */
export function createMockDashboardStatus(nowMs: number): DashboardStatus {
  const snapshot: StatusSnapshot = {
    attempted: 188_540,
    categories: [
      { categoryKey: "Residential", succeededCount: 128_420 },
      { categoryKey: "Commercial", succeededCount: 24_870 },
      { categoryKey: "Warehouse", succeededCount: 13_610 },
      { categoryKey: "TransportationTerminal", succeededCount: 7_300 },
      { categoryKey: "GrazingLand", succeededCount: 2_040 },
      { categoryKey: "CroplandClass2", succeededCount: 1_190 },
    ],
    denominator: BROWARD_DENOMINATOR,
    heartbeatAt: new Date(nowMs - 18_000).toISOString(),
    loadFailures: 3,
    phase: "full",
    sourceFailures: 281,
    sourceMisses: 1_240,
    staleAfterSeconds: 180,
    startedAt: new Date(nowMs - 20 * 60 * 60 * 1_000).toISOString(),
    succeeded: 186_731,
    throughputAttempted: 2_044,
    throughputWindowSeconds: 15 * 60,
    transformFailures: 17,
  };
  return buildDashboardStatus(snapshot, nowMs, "mock");
}
