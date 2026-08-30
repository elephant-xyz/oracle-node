import { describe, expect, it } from "vitest";

import {
  classifyTradePermit,
  jurisdictionHintFromUrl,
  normalizeEmbeddedPermit,
  parseFullExtractArgs,
  parseUsDateToIso,
} from "../../scripts/hillsborough/extract-full-permits.mjs";

describe("extract-full-permits", () => {
  it("parses CLI defaults and custom overrides", () => {
    expect(parseFullExtractArgs([])).toEqual({
      runDir: "downloads/hillsborough/full-run",
      seedPath: "downloads/hillsborough/full-seed.csv",
      outputJsonl:
        "downloads/hillsborough/full-permits/normalized-permits.jsonl",
      scorecardPath: "downloads/hillsborough/full-permits/scorecard.json",
    });

    expect(
      parseFullExtractArgs([
        "--run-dir",
        "/tmp/custom-run",
        "--seed",
        "/tmp/custom-seed.csv",
        "--output",
        "/tmp/permits.jsonl",
        "--scorecard",
        "/tmp/scorecard.json",
      ]),
    ).toEqual({
      runDir: "/tmp/custom-run",
      seedPath: "/tmp/custom-seed.csv",
      outputJsonl: "/tmp/permits.jsonl",
      scorecardPath: "/tmp/scorecard.json",
    });
  });

  it("classifies multi-trade permits accurately", () => {
    const roof = classifyTradePermit({
      descr: "Re-roof single family shingle",
      permitNum: "R-1",
    });
    expect(roof.isRoof).toBe(true);
    expect(roof.isHvac).toBe(false);

    const hvac = classifyTradePermit({
      descr: "Replace 3-ton heat pump and condenser",
      permitNum: "M-1",
    });
    expect(hvac.isHvac).toBe(true);
    expect(hvac.isRoof).toBe(false);

    const solar = classifyTradePermit({
      descr: "Install 10kW rooftop photovoltaic solar array",
      permitNum: "S-1",
    });
    expect(solar.isSolar).toBe(true);
    expect(solar.isRoof).toBe(false);

    const pool = classifyTradePermit({
      descr: "In-ground swimming pool and screen enclosure",
      permitNum: "P-1",
    });
    expect(pool.isPool).toBe(true);

    const electrical = classifyTradePermit({
      descr: "200A electrical service panel upgrade",
      permitNum: "E-1",
    });
    expect(electrical.isElectrical).toBe(true);

    const plumbing = classifyTradePermit({
      descr: "Replace 50 gallon water heater",
      permitNum: "PL-1",
    });
    expect(plumbing.isPlumbing).toBe(true);
  });

  it("parses dates and Accela jurisdiction hints", () => {
    expect(parseUsDateToIso("10/14/2025")).toBe("2025-10-14");
    expect(parseUsDateToIso("invalid")).toBeNull();
    expect(
      jurisdictionHintFromUrl(
        "https://aca-prod.accela.com/TAMPA/Cap/CapDetail.aspx?capID=1",
      ),
    ).toBe("TAMPA");
    expect(
      jurisdictionHintFromUrl(
        "https://aca-prod.accela.com/hcfl/Cap/CapDetail.aspx?capID=2",
      ),
    ).toBe("HCFL");
  });

  it("normalizes embedded permits with trade flags and deep links", () => {
    const norm = normalizeEmbeddedPermit({
      permit: {
        id: "12345",
        permitNum: "HC-BTR-26-0304812",
        issueDate: "2/1/2026",
        permitType: "Z",
        descr: "ROOF REPLACEMENT AND SHINGLE REPAIR",
        estValue: "$18,500",
        permitUrl:
          "https://aca-prod.accela.com/hcfl/Cap/CapDetail.aspx?Module=Building&capID1=HC&capID2=BTR",
      },
      seed: {
        folio: "0000010000",
        pin: "153323ZZZ000000000200A",
        address: "123 Main St",
        city: "Tampa",
        zip: "33602",
        owner: "John Doe",
      },
      siteAddress: "123 Main St, Tampa, FL 33602",
    });

    expect(norm).not.toBeNull();
    expect(norm?.permit_number).toBe("HC-BTR-26-0304812");
    expect(norm?.is_roof_permit).toBe(true);
    expect(norm?.is_hvac_permit).toBe(false);
    expect(norm?.source_system).toBe("hcfl_accela");
    expect(norm?.permit_issue_date).toBe("2026-02-01");
    expect(norm?.estimated_value).toBe("$18,500");
  });
});
