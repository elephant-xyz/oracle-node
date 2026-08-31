import { describe, expect, it } from "vitest";
import {
  COUNTY_REGISTRY,
  getCountyMetadata,
  listCounties,
} from "../../scripts/common/county-registry.mjs";
import {
  getLifecycleStatus,
  parseServerArgs,
} from "../../scripts/serve-dashboard.mjs";

describe("universal dashboard server & county registry", () => {
  it("parses CLI arguments with defaults", () => {
    const options = parseServerArgs([]);
    expect(options.port).toBe(3888);
    expect(options.county).toBe("volusia");
    expect(options.open).toBe(true);
  });

  it("parses custom port and county", () => {
    const options = parseServerArgs([
      "--port=4000",
      "--county=palm-beach",
      "--no-open",
    ]);
    expect(options.port).toBe(4000);
    expect(options.county).toBe("palm-beach");
    expect(options.open).toBe(false);
  });

  it("exposes all registered Florida counties", () => {
    const counties = listCounties();
    expect(counties.length).toBeGreaterThanOrEqual(4);
    expect(counties.some((c) => c.key === "volusia")).toBe(true);
    expect(counties.some((c) => c.key === "hillsborough")).toBe(true);
    expect(counties.some((c) => c.key === "pinellas")).toBe(true);
    expect(counties.some((c) => c.key === "lee")).toBe(true);
    expect(counties.some((c) => c.key === "palm-beach")).toBe(true);
  });

  it("resolves county metadata by key with fallback", () => {
    const volusia = getCountyMetadata("volusia");
    expect(volusia.key).toBe("volusia");
    expect(volusia.fips).toBe("12127");
    expect(volusia.seat).toBe("DeLand");

    const fallback = getCountyMetadata("unknown-county");
    expect(fallback.key).toBe("volusia");
  });

  it("computes lifecycle stage status for Volusia correctly", async () => {
    const rootPath = process.cwd();
    const lifecycle = await getLifecycleStatus(rootPath, "volusia");

    expect(lifecycle.county.key).toBe("volusia");
    expect(["completed", "pending", "in_progress"]).toContain(
      lifecycle.stages.discovery.status,
    );
    expect([
      "completed",
      "pending",
      "pilot_completed",
      "in_progress",
    ]).toContain(lifecycle.stages.seed.status);
    expect([
      "completed",
      "pending",
      "pilot_completed",
      "in_progress",
    ]).toContain(lifecycle.stages.appraisal.status);
    expect(lifecycle.nextStep).toBeDefined();
    expect(lifecycle.nextStep.stageNumber).toBeGreaterThanOrEqual(1);
  });
});
