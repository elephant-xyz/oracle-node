import { describe, expect, it } from "vitest";

import {
  assertOvertureChangelogSchema,
  buildRefreshPlan,
  buildTaxonomyDriftReport,
  classifyPlaceChanges,
  compareOvertureReleases,
  isExplicitlyClosed,
  overturePlacesChangelogGlobs,
  parseRefreshInput,
} from "../../scripts/overture-places-refresh-lib.mjs";
import { runRefreshStage } from "../../scripts/run-overture-places-refresh-stage.mjs";

const LEE_INPUT = {
  county: "lee",
  countyFips: "12071",
  boundarySource: "tiger/tl_2024_us_county",
  releaseOverride: null,
  costCeilingUsd: 5,
  dryRun: true,
  runId: "manual-plan",
};

describe("Overture release planning", () => {
  it("orders revisions numerically and rejects rollbacks", () => {
    expect(compareOvertureReleases("2026-08-19.10", "2026-08-19.2")).toBe(1);
    expect(compareOvertureReleases("2026-07-22.0", "2026-08-19.0")).toBe(-1);
    expect(() =>
      buildRefreshPlan({
        input: LEE_INPUT,
        latestRelease: "2026-07-22.0",
        lastSuccessfulRelease: "2026-08-19.0",
      }),
    ).toThrow(/rollback/);
  });

  it("returns a zero-cost no-op when STAC matches durable successful state", () => {
    const plan = buildRefreshPlan({
      input: LEE_INPUT,
      latestRelease: "2026-07-22.0",
      lastSuccessfulRelease: "2026-07-22.0",
      now: new Date("2026-08-13T00:00:00.000Z"),
    });
    expect(plan).toMatchObject({
      action: "noop",
      release: "2026-07-22.0",
      previousRelease: "2026-07-22.0",
      estimatedCostUsd: 0,
      withinCostCeiling: true,
      dryRun: true,
      idempotencyKey: "lee:2026-07-22.0",
    });
  });

  it("pins a newer release and requires the explicit cost ceiling", () => {
    const input = parseRefreshInput({
      ...LEE_INPUT,
      dryRun: false,
      costCeilingUsd: 0,
    });
    const plan = buildRefreshPlan({
      input,
      latestRelease: "2026-08-19.0",
      lastSuccessfulRelease: "2026-07-22.0",
    });
    expect(plan.action).toBe("incremental");
    expect(plan.release).toBe("2026-08-19.0");
    expect(plan.estimatedCostUsd).toBeGreaterThan(0);
    expect(plan.withinCostCeiling).toBe(false);
    expect(() =>
      parseRefreshInput({
        county: "lee",
        countyFips: "12071",
        boundarySource: "tiger/tl_2024_us_county",
      }),
    ).toThrow(/costCeilingUsd/);
  });
});

describe("Overture changelog contract", () => {
  it("uses only added, data_changed, and removed partitions", () => {
    const globs = overturePlacesChangelogGlobs("2026-07-22.0");
    expect(globs).toHaveLength(3);
    expect(globs.join("\n")).toContain("change_type=added");
    expect(globs.join("\n")).toContain("change_type=data_changed");
    expect(globs.join("\n")).toContain("change_type=removed");
    expect(globs.join("\n")).not.toContain("unchanged");
  });

  it("validates the documented live changelog fields", () => {
    expect(
      assertOvertureChangelogSchema([
        "id",
        "bbox",
        "change_type",
        "theme",
        "type",
        "filename",
      ]),
    ).toMatchObject({ passed: true });
    expect(() => assertOvertureChangelogSchema(["id", "change_type"])).toThrow(
      /bbox/,
    );
  });
});

describe("move and removal semantics", () => {
  it("upserts moves in and deactivates moves out/removals without closure inference", () => {
    const result = classifyPlaceChanges({
      existingCurrentIds: ["stay", "move-out", "removed"],
      currentCountyIds: ["stay", "move-in", "added"],
      changelogRows: [
        { id: "stay", changeType: "data_changed" },
        { id: "move-out", changeType: "data_changed" },
        { id: "move-in", changeType: "data_changed" },
        { id: "removed", changeType: "removed" },
        { id: "added", changeType: "added" },
      ],
    });
    expect(result.activeIds).toEqual(["added", "move-in", "stay"]);
    expect(result.updatedIds).toEqual(["stay"]);
    expect(result.movedInIds).toEqual(["move-in"]);
    expect(result.deactivateIds).toEqual(["move-out", "removed"]);
    expect(result.movedOutIds).toEqual(["move-out"]);
    expect(result.removedIds).toEqual(["removed"]);
    expect(result.counts).toEqual({
      added: 1,
      data_changed: 3,
      removed: 1,
    });
    expect(isExplicitlyClosed("open", 0.8)).toBe(false);
    expect(isExplicitlyClosed("permanently_closed", 0.8)).toBe(true);
    expect(isExplicitlyClosed(null, 0)).toBe(true);
  });

  it("rejects unknown changelog classifications", () => {
    expect(() =>
      classifyPlaceChanges({
        existingCurrentIds: [],
        currentCountyIds: [],
        changelogRows: [{ id: "x", changeType: "unchanged" }],
      }),
    ).toThrow(/Unsupported/);
  });
});

describe("hosted-service taxonomy drift", () => {
  const configured = [
    "services_and_business/financial_service/atm",
    "services_and_business/financial_service/trusts",
  ];

  it("blocks a quarterly release when a configured path is repathed", () => {
    const report = buildTaxonomyDriftReport({
      release: "2026-09-23.0",
      previousRelease: "2026-08-19.0",
      configuredPaths: configured,
      currentPaths: [
        "services_and_business/banking/atm",
        "services_and_business/financial_service/trusts",
      ],
    });
    expect(report.quarterlyRelease).toBe(true);
    expect(report.missingConfiguredPaths).toEqual([
      "services_and_business/financial_service/atm",
    ]);
    expect(report.repathedConfiguredPaths).toEqual([
      {
        configuredPath: "services_and_business/financial_service/atm",
        observedPaths: ["services_and_business/banking/atm"],
      },
    ]);
    expect(report.blocking).toBe(true);
  });

  it("passes when committed full paths remain stable and no candidate appears", () => {
    const report = buildTaxonomyDriftReport({
      release: "2026-08-19.0",
      previousRelease: "2026-07-22.0",
      configuredPaths: configured,
      currentPaths: [...configured, "food_and_drink/restaurant"],
    });
    expect(report.blocking).toBe(false);
    expect(report.reasons).toEqual([]);
  });
});

describe("pre-load workflow gates", () => {
  it("hard-stops an incoming licence failure", async () => {
    await expect(
      runRefreshStage("validate", {
        extraction: {
          licenceGate: {
            passed: false,
            message: "licence gate FAILED: osm present",
          },
          taxonomyDrift: { blocking: false, reasons: [] },
        },
      }),
    ).rejects.toMatchObject({
      name: "LicenceGateError",
      message: expect.stringContaining("osm"),
    });
  });

  it("hard-stops blocking quarterly taxonomy drift", async () => {
    await expect(
      runRefreshStage("validate", {
        extraction: {
          licenceGate: { passed: true },
          taxonomyDrift: {
            blocking: true,
            reasons: ["configured hosted-service path disappeared"],
          },
        },
      }),
    ).rejects.toMatchObject({
      name: "TaxonomyDriftError",
      message: expect.stringContaining("disappeared"),
    });
  });
});
