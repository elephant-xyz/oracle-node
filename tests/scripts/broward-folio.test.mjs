import { describe, expect, it } from "vitest";

import {
  BROWARD_DETAIL_URL,
  BROWARD_PILOT_FOLIOS,
  browardDetailRequestBody,
  isValidBrowardFolio,
  normalizeBrowardFolio,
} from "../../scripts/broward-folio.mjs";

describe("Broward folio helpers", () => {
  it("keeps condo letters and strips display dashes", () => {
    expect(normalizeBrowardFolio("504108BJ0140")).toBe("504108BJ0140");
    expect(normalizeBrowardFolio("504108bj0140")).toBe("504108BJ0140");
    expect(normalizeBrowardFolio("474135-01-0090")).toBe("474135010090");
    expect(normalizeBrowardFolio(" 494209060010 ")).toBe("494209060010");
  });

  it("refuses malformed or non-canonical folios", () => {
    expect(normalizeBrowardFolio("")).toBeUndefined();
    expect(normalizeBrowardFolio("USA")).toBeUndefined();
    expect(normalizeBrowardFolio("47413501009")).toBeUndefined();
    expect(normalizeBrowardFolio("4741350100900")).toBeUndefined();
    expect(normalizeBrowardFolio("504108_BJ0140")).toBeUndefined();
    expect(isValidBrowardFolio("474135010090")).toBe(true);
    expect(isValidBrowardFolio("504108BJ0140")).toBe(true);
  });

  it("does not pad or drop letters the way Orange PID normalization would", () => {
    const condo = "504108BJ0140";
    expect(condo.replace(/[^0-9]/g, "").padStart(15, "0")).not.toBe(condo);
    expect(normalizeBrowardFolio(condo)).toBe(condo);
  });

  it("builds the fail-closed detail body", () => {
    expect(browardDetailRequestBody("474135010090")).toEqual({
      folioNumber: "474135010090",
      taxyear: "",
      action: "CURRENT",
      use: "",
    });
    expect(BROWARD_DETAIL_URL).toContain("getParcelInformation");
    expect(BROWARD_PILOT_FOLIOS).toHaveLength(25);
    expect(BROWARD_PILOT_FOLIOS).toContain("504108BJ0140");
  });
});
