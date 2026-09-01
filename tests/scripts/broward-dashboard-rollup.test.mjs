import { describe, expect, it } from "vitest";

import {
  ensureBrowardDashboardRollup,
  refreshBrowardDashboardRollup,
} from "../../scripts/broward-dashboard-rollup.mjs";

/**
 * Create a query-recording client accepted by rollup helpers.
 *
 * @param {Record<string, unknown>} returnRow - Aggregate row.
 * @returns {{client:{query:(sql:string,values?:unknown[])=>Promise<{rows:Record<string,unknown>[]}>},calls:{sql:string,values:unknown[]|undefined}[]}}
 *   Mock client and recorded calls.
 */
function createClient(returnRow) {
  const calls = [];
  return {
    calls,
    client: {
      query: async (sql, values) => {
        calls.push({ sql, values });
        return {
          rows: sql.includes("RETURNING *") ? [returnRow] : [],
        };
      },
    },
  };
}

describe("Broward dashboard durable rollup", () => {
  it("creates additive aggregate storage and reconciles refreshed counts", async () => {
    const { client, calls } = createClient({
      permit_records: 243_939,
      permit_matched: 192_813,
      permit_unmatched: 51_126,
      permit_roofing: 22_414,
      permit_parcels: 52_179,
      permit_source_systems: 13,
      permit_last_loaded_at: new Date("2026-08-31T20:47:30.000Z"),
      sunbiz_matched_roles: 21_512,
      sunbiz_registrations: 12_432,
      sunbiz_properties: 9_023,
      refreshed_at: new Date("2026-09-01T16:00:00.000Z"),
    });
    await ensureBrowardDashboardRollup(
      /** @type {import("pg").Client} */ (client),
    );
    const rollup = await refreshBrowardDashboardRollup(
      /** @type {import("pg").Client} */ (client),
    );
    expect(rollup).toEqual({
      permitRecords: 243_939,
      permitMatched: 192_813,
      permitUnmatched: 51_126,
      permitRoofing: 22_414,
      permitParcels: 52_179,
      permitSourceSystems: 13,
      permitLastLoadedAt: "2026-08-31T20:47:30.000Z",
      sunbizMatchedRoles: 21_512,
      sunbizRegistrations: 12_432,
      sunbizProperties: 9_023,
      refreshedAt: "2026-09-01T16:00:00.000Z",
    });
    expect(
      calls.some((call) => call.sql.includes("broward_dashboard_rollup")),
    ).toBe(true);
    expect(calls.at(-1)?.values).toEqual(["broward"]);
  });

  it("rejects a rollup whose matched and unmatched permits do not balance", async () => {
    const { client } = createClient({
      permit_records: 10,
      permit_matched: 8,
      permit_unmatched: 1,
      permit_roofing: 2,
      permit_parcels: 5,
      permit_source_systems: 1,
      permit_last_loaded_at: null,
      sunbiz_matched_roles: 0,
      sunbiz_registrations: 0,
      sunbiz_properties: 0,
      refreshed_at: "2026-09-01T16:00:00.000Z",
    });
    await expect(
      refreshBrowardDashboardRollup(
        /** @type {import("pg").Client} */ (client),
      ),
    ).rejects.toThrow(/does not reconcile/u);
  });
});
