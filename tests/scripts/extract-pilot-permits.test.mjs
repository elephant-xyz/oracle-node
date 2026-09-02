import { describe, expect, it } from "vitest";

import {
  isRoofingRelatedPermit,
  jurisdictionHintFromUrl,
  normalizeEmbeddedPermit,
  parseExtractPilotPermitsArgs,
  parseSeedCsvForPermits,
  parseUsDateToIso,
} from "../../scripts/hillsborough/extract-pilot-permits.mjs";

describe("extract-pilot-permits", () => {
  it("parses CLI defaults and overrides", () => {
    expect(parseExtractPilotPermitsArgs([])).toEqual({
      pilotRunDir: "downloads/hillsborough/pilot-run",
      seedPath: "downloads/hillsborough/pilot-seed-50.csv",
      outputJsonl:
        "downloads/hillsborough/pilot-permits/normalized-permits.jsonl",
      scorecardPath: "downloads/hillsborough/pilot-permits/scorecard.json",
    });
    expect(
      parseExtractPilotPermitsArgs([
        "--pilot-run-dir",
        "x",
        "--seed",
        "y.csv",
        "--output",
        "out.jsonl",
        "--scorecard",
        "sc.json",
      ]),
    ).toEqual({
      pilotRunDir: "x",
      seedPath: "y.csv",
      outputJsonl: "out.jsonl",
      scorecardPath: "sc.json",
    });
  });

  it("parses seed CSV for folio/pin/zip", () => {
    const rows = parseSeedCsvForPermits(
      [
        "folio,pin,address,city,zip,owner",
        '1125270100,PIN1,"5401 W KENNEDY, TAMPA",Tampa,33609,ACME',
      ].join("\n"),
    );
    expect(rows).toEqual([
      {
        folio: "1125270100",
        pin: "PIN1",
        address: "5401 W KENNEDY, TAMPA",
        city: "Tampa",
        zip: "33609",
        owner: "ACME",
      },
    ]);
  });

  it("parses US dates and Accela jurisdiction hints", () => {
    expect(parseUsDateToIso("8/21/2025")).toBe("2025-08-21");
    expect(parseUsDateToIso("bad")).toBeNull();
    expect(
      jurisdictionHintFromUrl(
        "https://aca-prod.accela.com/TAMPA/Cap/GlobalSearchResults.aspx?QueryText=X",
      ),
    ).toBe("TAMPA");
    expect(
      jurisdictionHintFromUrl(
        "https://aca-prod.accela.com/hcfl/Cap/CapHome.aspx",
      ),
    ).toBe("HCFL");
  });

  it("flags roofing descriptions and normalizes embedded rows", () => {
    expect(
      isRoofingRelatedPermit({
        descr: "REROOF SINGLE FAMILY",
        permitNum: "BLD-1",
        permitType: "Z",
      }),
    ).toBe(true);
    const row = normalizeEmbeddedPermit({
      permit: {
        id: 1299220,
        permitNum: "BLD-25-0517809",
        issueDate: "8/21/2025",
        permitType: "Z",
        descr: "REMOVE PIPING",
        estValue: "$20,500",
        permitUrl:
          "https://aca-prod.accela.com/TAMPA/Cap/GlobalSearchResults.aspx?QueryText=BLD-25-0517809",
      },
      seed: {
        folio: "1125270100",
        pin: "182919ZZZ000005494600A",
        address: "5401 W KENNEDY BLVD, TAMPA",
        city: "Tampa",
        zip: "33609",
        owner: "ST PAUL",
      },
      siteAddress: "5401 W KENNEDY BLVD, TAMPA",
    });
    expect(row).not.toBeNull();
    expect(row?.source_system).toBe("tampa_accela");
    expect(row?.request_identifier).toBe("182919ZZZ000005494600A");
    expect(row?.parcel_identifier).toBe("1125270100");
    expect(row?.permit_issue_date).toBe("2025-08-21");
    expect(row?.raw.detail_id).toBe("1299220");
    expect(row?.is_roof_permit).toBe(false);
  });
});
