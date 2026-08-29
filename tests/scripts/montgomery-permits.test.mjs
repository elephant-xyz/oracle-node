import { describe, expect, it } from "vitest";

import {
  isRoofPermit,
  calculateRoofAge,
  buildNormalizedMontgomeryPermit,
} from "../../scripts/montgomery-discovery/montgomery-permits.mjs";

describe("montgomery-permits", () => {
  it("isRoofPermit detects standard roof keywords", () => {
    expect(isRoofPermit("Residential Roof Replacement")).toBe(true);
    expect(isRoofPermit("Re-Roof Asphalt Shingle")).toBe(true);
    expect(isRoofPermit("Install Standing Seam Metal Roof")).toBe(true);
    expect(isRoofPermit("Slate tile repair and underlayment")).toBe(true);
    expect(isRoofPermit("Building electrical inspection")).toBe(false);
    expect(isRoofPermit(null)).toBe(false);
  });

  it("calculateRoofAge prioritizes re-roof permit over built year", () => {
    const res = calculateRoofAge({
      builtYear: 1950,
      remodelYear: 1990,
      reRoofPermitYear: 2018,
      currentYear: 2026,
    });
    expect(res.roofAgeYears).toBe(8);
    expect(res.calculationMethod).toBe("PermitIssueYear");
    expect(res.roofDate).toBe("2018-01-01");
  });

  it("calculateRoofAge falls back to remodel year when no permit exists", () => {
    const res = calculateRoofAge({
      builtYear: 1940,
      remodelYear: 2005,
      reRoofPermitYear: null,
      currentYear: 2026,
    });
    expect(res.roofAgeYears).toBe(21);
    expect(res.calculationMethod).toBe("RemodelYear");
  });

  it("calculateRoofAge falls back to structure built year when no remodel or permit", () => {
    const res = calculateRoofAge({
      builtYear: 1985,
      remodelYear: null,
      reRoofPermitYear: null,
      currentYear: 2026,
    });
    expect(res.roofAgeYears).toBe(41);
    expect(res.calculationMethod).toBe("StructureBuiltYear");
  });

  it("buildNormalizedMontgomeryPermit produces valid structure", () => {
    const permit = buildNormalizedMontgomeryPermit({
      taxpin: "40-00-12345-00-1",
      permitNumber: "BP-2023-0891",
      muniName: "Lower Merion Township",
      issueDate: "2023-06-15",
      permitType: "Roofing Permit",
      description: "Tear-off and install architectural shingles",
      contractor: "Main Line Roofing LLC",
      valuation: 18500,
    });

    expect(permit.parcel_identifier).toBe("40-00-12345-00-1");
    expect(permit.permit_number).toBe("BP-2023-0891");
    expect(permit.municipality_name).toBe("Lower Merion Township");
    expect(permit.is_roof_permit).toBe(true);
    expect(permit.contractor_name).toBe("Main Line Roofing LLC");
  });
});
