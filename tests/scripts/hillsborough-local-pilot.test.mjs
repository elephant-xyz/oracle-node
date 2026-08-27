import { describe, expect, it } from "vitest";

import {
  buildInputHtmlFromParcelData,
  formatPropertyUse,
  parsePilotArgs,
  parseSeedCsvText,
} from "../../scripts/hillsborough/lib.mjs";

describe("hillsborough local pilot lib", () => {
  it("parses pilot CLI args", () => {
    const options = parsePilotArgs([
      "--limit=12",
      "--load",
      "--concurrency=3",
      "--seed=downloads/hillsborough/pilot-seed-50.csv",
    ]);
    expect(options.limit).toBe(12);
    expect(options.load).toBe(true);
    expect(options.concurrency).toBe(3);
    expect(options.seedPath).toBe("downloads/hillsborough/pilot-seed-50.csv");
  });

  it("parses quoted CSV rows", () => {
    const rows = parseSeedCsvText(
      [
        "parcel_id,source_identifier,address,parcel_polygon",
        '1125270100,PIN1,"5401 W KENNEDY, TAMPA","POLYGON((1 2, 3 4, 1 2))"',
      ].join("\n"),
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].parcel_id).toBe("1125270100");
    expect(rows[0].address).toBe("5401 W KENNEDY, TAMPA");
    expect(rows[0].parcel_polygon).toMatch(/^POLYGON/);
  });

  it("formats property use for transform mapping", () => {
    expect(
      formatPropertyUse({ code: "1820", description: "OFF MULT-STY B" }),
    ).toBe("1820 OFF MULT-STY B");
  });

  it("builds HTML with required transform selectors", () => {
    const html = buildInputHtmlFromParcelData({
      pin: "182919ZZZ000005494600A",
      owner: "ACME LLC;",
      siteAddress: "5401 W KENNEDY BLVD, TAMPA",
      landUse: { code: "1820", description: "OFF MULT-STY B" },
      mailingAddress: {
        addr1: "385 WASHINGTON ST",
        city: "SAINT PAUL",
        state: "MN",
        zip: "55102",
      },
      propertyCard: {
        displayStrap: "A-19-29-18-ZZZ-000005-49460.0",
        subdivision: { description: "UNPLATTED" },
      },
      valueSummary: [
        {
          taxDist: "County",
          marketVal: 100,
          assessedVal: 90,
          exemptions: 0,
          taxableVal: 90,
        },
      ],
      salesHistory: [],
      permitInfo: [],
      landLines: [],
      buildings: [],
      fullLegal: "LOT 1",
    });
    expect(html).toContain("Property Use:");
    expect(html).toContain("1820 OFF MULT-STY B");
    expect(html).toContain('data-bind="text: displayStrap"');
    expect(html).toContain("Site Address");
    expect(html).toContain("Value Summary");
    expect(html).toContain("publicOwner");
  });
});
