import { describe, expect, it } from "vitest";

import {
  COUNTY_REGISTRY,
  getLifecycleStatus,
  parseServerArgs,
} from "../../scripts/hillsborough/serve-dashboard.mjs";

describe("hillsborough dashboard server & lifecycle API", () => {
  it("parses CLI arguments with defaults", () => {
    const options = parseServerArgs([]);
    expect(options.port).toBe(3888);
    expect(options.jobId).toBe("hillsborough-full-2026-08-27");
    expect(options.county).toBe("hillsborough");
    expect(options.open).toBe(true);
  });

  it("parses custom port, county and flags", () => {
    const options = parseServerArgs([
      "--port=4000",
      "--job-id=custom-job-123",
      "--county=hillsborough",
      "--no-open",
    ]);
    expect(options.port).toBe(4000);
    expect(options.jobId).toBe("custom-job-123");
    expect(options.county).toBe("hillsborough");
    expect(options.open).toBe(false);
  });

  it("exposes county metadata in registry", () => {
    expect(COUNTY_REGISTRY.hillsborough).toBeDefined();
    expect(COUNTY_REGISTRY.hillsborough.fips).toBe("12057");
    expect(COUNTY_REGISTRY.hillsborough.targetParcels).toBe(524196);
  });

  it("computes lifecycle stage status correctly", async () => {
    const rootPath = process.cwd();
    const lifecycle = await getLifecycleStatus(rootPath, "hillsborough");

    expect(lifecycle.county.key).toBe("hillsborough");
    expect(["completed", "pending", "in_progress"]).toContain(
      lifecycle.stages.discovery.status,
    );
    expect(["completed", "pending", "in_progress"]).toContain(
      lifecycle.stages.seed.status,
    );
    expect(["completed", "pending", "enriching", "in_progress"]).toContain(
      lifecycle.stages.sourcing.status,
    );
    expect(lifecycle.stages.sourcing.permits.target).toBe(958002);
    expect(lifecycle.nextStep).toBeDefined();
    expect(lifecycle.nextStep.stageNumber).toBeGreaterThanOrEqual(1);
  });
});
