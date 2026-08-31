import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { describe, expect, it } from "vitest";
import {
  COUNTY_REGISTRY,
  getCountyMetadata,
  listCounties,
} from "../../scripts/common/county-registry.mjs";
import {
  getBrowardLifecycleStatus,
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
    expect(counties.some((c) => c.key === "broward")).toBe(true);
  });

  it("maps Broward only from verified aggregate telemetry", async () => {
    const root = await mkdtemp(
      path.join(tmpdir(), "broward-universal-dashboard-"),
    );
    try {
      const bbbDirectory = path.join(
        root,
        "downloads/broward/bbb-roofing-worklist",
      );
      await mkdir(bbbDirectory, { recursive: true });
      await writeFile(
        path.join(bbbDirectory, "summary.private.json"),
        JSON.stringify({ candidateCount: 1_381 }),
      );
      const lifecycle = await getBrowardLifecycleStatus(root, async () => ({
        progress: { properties: 534_309 },
        permitInventory: {
          records: 243_939,
          roofing: 22_414,
          matched: 192_813,
        },
        permitEnumeration: { accessibleRecords: 430_087 },
        sunbizMatch: { registrations: 12_432, properties: 9_023 },
      }));
      expect(lifecycle).toMatchObject({
        county: {
          key: "broward",
          fips: "12011",
          targetParcels: 534_309,
        },
        telemetrySource: "broward-neon-recovery-dashboard",
        stages: {
          appraisal: {
            status: "completed",
            count: 534_309,
          },
          sourcing: {
            status: "in_progress",
            permits: {
              count: 243_939,
              capturedCount: 430_087,
              tradeCounts: { Roofing: 22_414 },
            },
            sunbiz: {
              count: 12_432,
              propertyCount: 9_023,
            },
            bbb: {
              count: 0,
              candidateCount: 1_381,
              status: "api_credentials_required",
            },
          },
          publish: { status: "disabled" },
        },
      });
      expect(JSON.stringify(lifecycle)).not.toMatch(
        /555-01|Roofing Pros|Atlantic Coast Roofing/iu,
      );
    } finally {
      await rm(root, { recursive: true, force: true });
    }
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
