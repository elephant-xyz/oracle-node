import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
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
import { buildBrowardPermitRouteStatus } from "../../scripts/broward-neon-recovery-dashboard.mjs";

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
      const permitRoutes = buildBrowardPermitRouteStatus();
      const lifecycle = await getBrowardLifecycleStatus(root, async () => ({
        progress: { properties: 534_309 },
        permitInventory: {
          records: 243_939,
          roofing: 22_414,
          matched: 192_813,
        },
        permitRoutes,
        manualCaptchaProgress: {
          sessionPolicy: "manual_captcha_sessions_expire",
          countyComplete: false,
          routes: [
            {
              jurisdiction: "Coral Springs",
              registryStatus: "captcha_required",
              progressState: "bounded_capture_in_progress",
              evidence: "private_capture_checkpoint",
              coverageBoundary: "bounded_capped_slice",
              capturedRecords: 640,
              loadedRecords: 0,
              manualSessionRequired: true,
              sessionsExpire: true,
              validSearchCaptchaRequired: true,
              countyComplete: false,
            },
            {
              jurisdiction: "Hillsboro Beach",
              registryStatus: "captcha_required",
              progressState: "awaiting_manual_captcha",
              evidence: "no_captured_aggregate",
              coverageBoundary: "not_captured",
              capturedRecords: 0,
              loadedRecords: 0,
              manualSessionRequired: true,
              sessionsExpire: true,
              validSearchCaptchaRequired: true,
              countyComplete: false,
            },
            {
              jurisdiction: "Pembroke Park",
              registryStatus: "captcha_required",
              progressState: "bounded_slice_loaded",
              evidence: "durable_loaded_aggregate",
              coverageBoundary: "bounded_slice",
              capturedRecords: 166,
              loadedRecords: 166,
              manualSessionRequired: true,
              sessionsExpire: true,
              validSearchCaptchaRequired: true,
              countyComplete: false,
            },
          ],
        },
        permitEnumeration: {
          accessibleRecords: 430_087,
          pausedWorkers: [
            { source: "Plantation", reason: "timeout" },
            { source: "Weston", reason: "source_cap" },
          ],
          coolingWorkers: [
            {
              source: "Cooper City",
              reason: "source_cap",
              nextAttemptAt: "2026-09-02T01:00:00.000Z",
            },
          ],
        },
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
            permitRoutes: {
              totalCurrentRoutes: 32,
              implementedCurrentRoutes: 24,
              manualCaptchaCurrentRoutes: 3,
              hardBlockedCurrentRoutes: 5,
              unattendedUnavailableCurrentRoutes: 8,
            },
            manualCaptchaProgress: {
              sessionPolicy: "manual_captcha_sessions_expire",
              countyComplete: false,
            },
            operationalWorkers: {
              paused: [
                { source: "Plantation", reason: "timeout" },
                { source: "Weston", reason: "source_cap" },
              ],
              coolingDown: [
                {
                  source: "Cooper City",
                  reason: "source_cap",
                  nextAttemptAt: "2026-09-02T01:00:00.000Z",
                },
              ],
            },
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
      const sourcing = lifecycle.stages.sourcing;
      expect(
        sourcing.permitRoutes.implementedCurrentRoutes +
          sourcing.permitRoutes.manualCaptchaCurrentRoutes +
          sourcing.permitRoutes.hardBlockedCurrentRoutes,
      ).toBe(sourcing.permitRoutes.totalCurrentRoutes);
      expect(
        sourcing.permitRoutes.hardBlockCategories.reduce(
          (sum, category) => sum + category.count,
          0,
        ),
      ).toBe(sourcing.permitRoutes.hardBlockedCurrentRoutes);
      expect(sourcing.operationalWorkers.paused).toHaveLength(2);
      expect(sourcing.operationalWorkers.coolingDown).toHaveLength(1);
      expect(
        sourcing.permitRoutes.unattendedUnavailableCurrentRoutes,
      ).toBe(
        sourcing.permitRoutes.manualCaptchaCurrentRoutes +
          sourcing.permitRoutes.hardBlockedCurrentRoutes,
      );
      expect(sourcing.manualCaptchaProgress.routes).toHaveLength(3);
      expect(
        sourcing.permitRoutes.hardBlockCategories.some(
          (category) => category.key === "captcha_required",
        ),
      ).toBe(false);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("includes a universal permit-route and operational-pause view", async () => {
    const dashboardHtml = await readFile(
      path.resolve(process.cwd(), "scripts/common/dashboard.html"),
      "utf8",
    );
    expect(dashboardHtml).toContain('id="permitRouteStatusCard"');
    expect(dashboardHtml).toContain('id="permitRouteBlockerGroups"');
    expect(dashboardHtml).toContain('id="permitRouteManualProgress"');
    expect(dashboardHtml).toContain('id="permitPausedWorkerList"');
    expect(dashboardHtml).toContain('id="permitCoolingWorkerList"');
    expect(dashboardHtml).toContain("Operational pauses are shown separately");
    expect(dashboardHtml).toContain("Manual sessions expire");
    expect(dashboardHtml).toContain("Hard-Block Categories");
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
