import { describe, expect, it } from "vitest";

import {
  PINELLAS_BBB_CATEGORY_SOURCES,
  PINELLAS_BBB_CITIES,
  PINELLAS_BBB_FULL_HARVEST_CITY_ORDER,
  PINELLAS_BBB_FULL_HARVEST_MAX_PAGES,
  PINELLAS_BBB_FULL_HARVEST_PAGE_DELAY_MS,
  PINELLAS_BBB_FULL_HARVEST_PROFILE_DELAY_MS,
  PINELLAS_BBB_FULL_HARVEST_TRADE_ORDER,
  PINELLAS_BBB_PRODUCTION_API_URL,
  PINELLAS_BBB_ROBOTS_COMPLIANT_MAX_PAGES,
  PINELLAS_BBB_TRADES,
  buildPinellasBbbCategoryUrl,
  buildPinellasBbbFullHarvestJobs,
  buildPinellasBbbHarvestPlan,
  buildPinellasBbbProbeCommand,
} from "../../scripts/pinellas/bbb-harvest-plan.mjs";

describe("Pinellas BBB harvest plan", () => {
  it("anchors on St. Petersburg and Clearwater, not Tampa", () => {
    expect(PINELLAS_BBB_CITIES.map((city) => city.key)).toEqual([
      "st-petersburg",
      "clearwater",
    ]);
    expect(
      PINELLAS_BBB_CATEGORY_SOURCES.every(
        (source) => !source.categoryUrl.includes("/tampa/"),
      ),
    ).toBe(true);
  });

  it("covers roofing, HVAC, and solar for both cities", () => {
    expect(PINELLAS_BBB_TRADES.map((trade) => trade.key)).toEqual([
      "roofing",
      "hvac",
      "solar",
    ]);
    expect(PINELLAS_BBB_CATEGORY_SOURCES).toHaveLength(6);
    expect(PINELLAS_BBB_CATEGORY_SOURCES).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          cityKey: "st-petersburg",
          tradeKey: "roofing",
          categoryUrl:
            "https://www.bbb.org/us/fl/st-petersburg/category/roofing-contractors",
        }),
        expect.objectContaining({
          cityKey: "clearwater",
          tradeKey: "hvac",
          categoryUrl:
            "https://www.bbb.org/us/fl/clearwater/category/heating-and-air-conditioning",
        }),
      ]),
    );
  });

  it("builds category URLs from city and trade slugs", () => {
    expect(
      buildPinellasBbbCategoryUrl("st-petersburg", "solar-energy-contractors"),
    ).toBe(
      "https://www.bbb.org/us/fl/st-petersburg/category/solar-energy-contractors",
    );
  });

  it("emits a page-1-only probe command with conservative bounds", () => {
    const command = buildPinellasBbbProbeCommand("/tmp/pinellas-bbb-probe");
    expect(command).toContain("st-petersburg");
    expect(command).toContain("roofing-contractors");
    expect(command).toContain(
      `--max-pages ${PINELLAS_BBB_ROBOTS_COMPLIANT_MAX_PAGES}`,
    );
    expect(command).not.toContain("--max-pages 2");
    expect(command).toContain("--max-profiles 15");
    expect(command).toContain("--profile-subpages none");
  });

  it("does not recommend paginated harvest in the plan", () => {
    const plan = buildPinellasBbbHarvestPlan("/tmp/pinellas-bbb");
    expect(plan.county).toBe("pinellas");
    expect(plan.categories).toHaveLength(6);
    expect(plan.complete).toBe(false);
    expect(plan.schemaVersion).toBe("oracle-node.pinellas-bbb-harvest-plan.v2");
    expect(plan.paginationPolicy.multiPageScrape).toBe("stop");
    expect(plan.paginationPolicy.localMaxPages).toBe(
      PINELLAS_BBB_ROBOTS_COMPLIANT_MAX_PAGES,
    );
    expect(plan.recommendedProductionPath.applicationUrl).toBe(
      PINELLAS_BBB_PRODUCTION_API_URL,
    );
    expect(plan.operatorNextStep).toContain("Apply for BBB API access");
    expect(plan.doNotRun).toContain("paginated");
    expect(plan.probeCommand).toContain(
      `--max-pages ${PINELLAS_BBB_ROBOTS_COMPLIANT_MAX_PAGES}`,
    );
    for (const category of plan.categories) {
      expect(category.command).toContain(
        `--max-pages ${PINELLAS_BBB_ROBOTS_COMPLIANT_MAX_PAGES}`,
      );
      expect(category.command).not.toContain("--max-pages 2");
    }
  });

  it("builds the operator full-paginate job sequence with conservative delays", () => {
    const jobs = buildPinellasBbbFullHarvestJobs("/tmp/pinellas-bbb-harvest");
    expect(jobs).toHaveLength(6);
    expect(jobs.map((job) => `${job.cityKey}/${job.tradeKey}`)).toEqual([
      "clearwater/roofing",
      "clearwater/hvac",
      "clearwater/solar",
      "st-petersburg/roofing",
      "st-petersburg/hvac",
      "st-petersburg/solar",
    ]);
    expect(PINELLAS_BBB_FULL_HARVEST_CITY_ORDER).toEqual([
      "clearwater",
      "st-petersburg",
    ]);
    expect(PINELLAS_BBB_FULL_HARVEST_TRADE_ORDER).toEqual([
      "roofing",
      "hvac",
      "solar",
    ]);

    for (const job of jobs) {
      const source = PINELLAS_BBB_CATEGORY_SOURCES.find(
        (entry) =>
          entry.cityKey === job.cityKey && entry.tradeKey === job.tradeKey,
      );
      expect(source).toBeDefined();
      expect(job.categoryUrl).toBe(source?.categoryUrl);
      expect(job.outputDirectory).toBe(
        `/tmp/pinellas-bbb-harvest/${job.cityKey}/${job.tradeKey}`,
      );
      expect(job.command).toContain(job.categoryUrl);
      expect(job.command).toContain(
        `--max-pages ${PINELLAS_BBB_FULL_HARVEST_MAX_PAGES}`,
      );
      expect(job.command).toContain(
        `--page-delay-ms ${PINELLAS_BBB_FULL_HARVEST_PAGE_DELAY_MS}`,
      );
      expect(job.command).toContain(
        `--profile-delay-ms ${PINELLAS_BBB_FULL_HARVEST_PROFILE_DELAY_MS}`,
      );
      expect(job.command).not.toMatch(/--max-pages 1(?:\s|$)/);
    }
  });

  it("supports resuming full harvest from job 2 after clearwater roofing completes", () => {
    const jobs = buildPinellasBbbFullHarvestJobs("/tmp/pinellas-bbb-harvest");
    const resumedFromJob2 = jobs.slice(2 - 1);
    expect(resumedFromJob2).toHaveLength(5);
    expect(resumedFromJob2[0]).toMatchObject({
      cityKey: "clearwater",
      tradeKey: "hvac",
    });
  });
});
