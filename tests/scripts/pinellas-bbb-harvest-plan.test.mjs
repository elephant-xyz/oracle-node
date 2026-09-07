import { describe, expect, it } from "vitest";

import {
  PINELLAS_BBB_CATEGORY_SOURCES,
  PINELLAS_BBB_CITIES,
  PINELLAS_BBB_TRADES,
  buildPinellasBbbCategoryUrl,
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
      buildPinellasBbbCategoryUrl(
        "st-petersburg",
        "solar-energy-contractors",
      ),
    ).toBe(
      "https://www.bbb.org/us/fl/st-petersburg/category/solar-energy-contractors",
    );
  });

  it("emits a probe command with conservative bounds", () => {
    const command = buildPinellasBbbProbeCommand("/tmp/pinellas-bbb-probe");
    expect(command).toContain("st-petersburg");
    expect(command).toContain("roofing-contractors");
    expect(command).toContain("--max-pages 2");
    expect(command).toContain("--max-profiles 15");
    expect(command).toContain("--profile-subpages none");
  });

  it("does not claim completion in the harvest plan", () => {
    const plan = buildPinellasBbbHarvestPlan("/tmp/pinellas-bbb");
    expect(plan.county).toBe("pinellas");
    expect(plan.categories).toHaveLength(6);
    expect(plan.complete).toBe(false);
    expect(plan.probeCommand).toContain("--max-pages 2");
  });
});
