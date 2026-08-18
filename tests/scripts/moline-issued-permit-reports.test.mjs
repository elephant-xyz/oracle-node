import { describe, expect, it } from "vitest";

import {
  MOLINE_REPORT_SOURCE_SYSTEM,
  dedupeMolineIssuedPermits,
  extractMolineReportLinks,
  isCompactedLegacyMolineReportLayout,
  isConservativeMolineBusinessName,
  isLegacyApplicationMolineReportLayout,
  isRotatedLegacyMolineReportLayout,
  inspectLegacyMolineApplicationIdentities,
  molinePermitLoaderKey,
  parseCurrentMolineIssuedPermitReport,
  parseLegacyApplicationMolineIssuedPermitReport,
  parseMolineIssuedPermitReport,
  parseRotatedLegacyMolineIssuedPermitReport,
  readMolinePrintedPermitTotal,
  renderMolinePrivateJsonl,
  renderMolinePublicJsonl,
  toMolinePublicPermit,
} from "../../scripts/permit-source-adapters/moline-issued-permit-reports.mjs";

const SOURCE = {
  archiveId: "11988",
  reportMonth: "2026-06",
  title: "2026-06 Building Permits Issued",
  url: "https://www.moline.il.us/Archive.aspx?ADID=11988",
};

/**
 * Build one synthetic positioned PDF text item.
 *
 * @param {string} text - Synthetic source text.
 * @param {number} x - Horizontal report coordinate.
 * @param {number} y - Vertical report coordinate.
 * @param {number} [pageNumber=1] - One-based source page.
 * @returns {{text: string, x: number, y: number, pageNumber: number}} Positioned text.
 */
function item(text, x, y, pageNumber = 1) {
  return { text, x, y, pageNumber };
}

/**
 * Build the reviewed current-layout header and two representative rows.
 *
 * @returns {readonly (readonly ReturnType<typeof item>[])[]} Synthetic positioned pages.
 */
function currentPages() {
  return [
    [
      item("PERMIT NUMBER", 55.7, 500),
      item("PERMITTYPE", 110.3, 500),
      item("PERMITSUBTYPE", 257.4, 500),
      item("ISSUED", 386.1, 500),
      item("CONTRACTOR_NAME", 440.6, 500),
      item("ADDRESS", 625.2, 500),
      item("JOB VALUE", 681.1, 500),
      item("BP26-000477", 55.7, 492.6),
      item("BUILDING REHAB", 110.3, 492.6),
      item("RESIDENTIAL", 257.5, 492.6),
      item("06/11/2026", 386.2, 492.6),
      item("EXAMPLE ROOFING LLC", 440.8, 492.6),
      item("2722 PRIVATE AV", 625.5, 492.6),
      item("$2,850.00", 710.7, 492.6),
      item("EP26-000202", 55.7, 485.2),
      item("ELECTRICAL", 110.3, 485.2),
      item("06/19/2026", 386.2, 485.2),
      item("Firstname Lastname", 440.8, 485.2),
      item("3429 PRIVATE CT", 625.4, 485.2),
      item("$0.00", 720.6, 485.2),
    ],
  ];
}

/**
 * Build a clean legacy application-key layout with a carried permit code.
 *
 * @returns {readonly (readonly ReturnType<typeof item>[])[]} Synthetic positioned pages.
 */
function legacyPages() {
  return [
    [
      item("Permit", 33.2, 522.2),
      item("Code", 35.8, 514.3),
      item("Permit Code", 91.4, 522.2),
      item("Description", 94, 514.3),
      item("Permit Issue", 162.7, 522.2),
      item("Date", 173.9, 514.3),
      item("App.", 202.7, 522.2),
      item("Year", 202.9, 506.5),
      item("App. #", 225.1, 522.2),
      item("Name", 292.6, 522.2),
      item("Name Type", 353.4, 522.2),
      item("Township -", 397.2, 522.2),
      item("Street Address", 458.6, 522.2),
      item("Application Type", 573.8, 522.2),
      item("Permit Status", 679.4, 522.2),
      item("Estimated", 730.9, 522.2),
      item("BCMR", 29.5, 498),
      item("B: COML REMODEL", 59.3, 498.2),
      item("3/9/2021", 168.1, 498),
      item("21", 205.8, 498),
      item("300", 229, 498),
      item("EXAMPLE CONSTRUCTION LLC", 251.9, 498.5),
      item("CT", 352.4, 498),
      item("07", 392.2, 498.2),
      item("-13281 -10", 405, 498.2),
      item("1818 53RD ST", 460.4, 498.2),
      item("BLDG: COML REMODEL", 523.9, 498.5),
      item("PERMIT PRINTED", 673.1, 498.2),
      item("$25,000", 738.5, 498),
      item("3/10/2021", 168.1, 480),
      item("21", 205.8, 480),
      item("301", 229, 480),
      item("PROPERTY OWNER", 251.9, 480.5),
      item("CT", 352.4, 480),
      item("08", 392.2, 480.2),
      item("-5506 -", 405, 480.2),
      item("1515 5TH AVE", 460.4, 480.2),
      item("BLDG: COML REMODEL", 523.9, 480.5),
      item("PERMIT PRINTED", 673.1, 480.2),
      item("$2,000", 738.5, 480),
      item("Total permits:", 172.8, 368.4),
      item("2", 220.8, 367.4),
    ],
  ];
}

/**
 * Build the August 2025 reordered layout without a job-value column.
 *
 * @returns {readonly (readonly ReturnType<typeof item>[])[]} Synthetic positioned pages.
 */
function reorderedCurrentPages() {
  return [
    [
      item("ISSUED", 37.8, 538),
      item("PERMIT NUMBER", 97.8, 538),
      item("PERMIT TYPE", 157.8, 538),
      item("PERMIT SUBTYPE", 307.8, 538),
      item("CONTRACTOR_NAME", 421.8, 538),
      item("ADDRESS", 603.2, 538),
      item("11/21/2024", 37.8, 530),
      item("24-00002028", 97.8, 530),
      item("DECKS AND PORCHES", 157.8, 530),
      item("EXAMPLE CONTRACTING LLC", 421.8, 530),
      item("903 PRIVATE ST", 603.2, 530),
    ],
  ];
}

/**
 * Build the reviewed transposed-coordinate February 2020 layout.
 *
 * @returns {readonly (readonly ReturnType<typeof item>[])[]} Synthetic positioned pages.
 */
function rotatedLegacyPages() {
  return [
    [
      item("Permit Code", 89.28, 87.18),
      item("Permit Issue", 89.28, 155.58),
      item("App. #", 89.28, 216.48),
      item("BCI", 112.26, 29.34),
      item("B: COML INVESTIGATION", 112.26, 57.48),
      item("2/7/2020", 112.26, 160.86),
      item("20", 112.26, 198.06),
      item("171", 112.26, 220.5),
      item("EXAMPLE CONSTRUCTION LLC", 112.08, 242.22),
      item("CT", 112.26, 343.26),
      item("08", 112.26, 385.2),
      item("-4394 -", 112.26, 398.34),
      item("501 PRIVATE DR", 112.26, 440.52),
      item("INVESTIGATION", 112.08, 495.6),
      item("PERMIT PRINTED", 112.26, 636.42),
      item("$0", 112.26, 741.3),
      item("2/8/2020", 140.76, 160.86),
      item("20", 140.76, 198.06),
      item("172", 140.76, 220.5),
      item("PROPERTY OWNER", 140.52, 242.22),
      item("CT", 140.76, 343.26),
      item("07", 140.76, 384.66),
      item("-11012 -", 140.76, 397.8),
      item("1811 PRIVATE PL", 140.76, 429.36),
      item("INVESTIGATION", 140.52, 495.6),
      item("PERMIT PRINTED", 140.76, 636.42),
      item("$7,944", 140.76, 735.06),
    ],
  ];
}

describe("Moline official issued-permit reports", () => {
  it("discovers canonical archive links and normalizes spaced month labels", () => {
    const html = `
      <a href="./Archive.aspx?ADID=11988">2026-06 Building Permits Issued</a>
      <a href="/Archive/ViewFile/Item/11892"><strong>2026-05 Building Permits Issued</strong></a>
      <a href="./Archive.aspx?ADID=4042">2017- 05 Building Permits Issued</a>
      <a href="./Archive.aspx?ADID=11988">2026-06</a>
      <a href="/unrelated">Ignore</a>
    `;

    expect(extractMolineReportLinks(html)).toEqual([
      {
        archiveId: "4042",
        reportMonth: "2017-05",
        title: "2017- 05 Building Permits Issued",
        url: "https://www.moline.il.us/Archive.aspx?ADID=4042",
      },
      {
        archiveId: "11892",
        reportMonth: "2026-05",
        title: "2026-05 Building Permits Issued",
        url: "https://www.moline.il.us/Archive.aspx?ADID=11892",
      },
      {
        archiveId: "11988",
        reportMonth: "2026-06",
        title: "2026-06 Building Permits Issued",
        url: "https://www.moline.il.us/Archive.aspx?ADID=11988",
      },
    ]);
  });

  it("parses current reports without inferring parcels or retaining person names", () => {
    const records = parseCurrentMolineIssuedPermitReport(
      currentPages(),
      SOURCE,
    );

    expect(records).toHaveLength(2);
    expect(records[0]).toMatchObject({
      source_system: MOLINE_REPORT_SOURCE_SYSTEM,
      permit_number: "BP26-000477",
      parcel_identifier: null,
      work_location: "2722 PRIVATE AV",
      permit_issue_date: "2026-06-11",
      record_status: "Issued",
      record_type: "BUILDING REHAB",
      project_description: null,
      contractor_business_names: ["EXAMPLE ROOFING LLC"],
      is_roof_permit: false,
      raw: {
        source_archive_id: "11988",
        source_report_month: "2026-06",
        permit_subtype: "RESIDENTIAL",
        project_valuation: 2850,
        parser_layout: "current-2024-10",
      },
    });
    expect(records[1]).toMatchObject({
      permit_number: "EP26-000202",
      contractor_business_names: [],
      raw: {
        permit_subtype: null,
        project_valuation: 0,
      },
    });
    expect(renderMolinePrivateJsonl(records)).not.toContain(
      "Firstname Lastname",
    );
  });

  it("fails closed for legacy layouts without modern printed permit numbers", () => {
    const legacyPages = [
      [
        item("Permit", 36.3, 697.9),
        item("Code", 38.7, 688),
        item("App. #", 314.2, 697.9),
        item("BAWN", 30, 661),
        item("2855", 317.5, 661),
      ],
    ];

    expect(() =>
      parseCurrentMolineIssuedPermitReport(legacyPages, SOURCE),
    ).toThrow(/Unsupported Moline permit report layout/);
  });

  it("deduplicates exact loader keys and rejects conflicting variants", () => {
    const [first] = parseCurrentMolineIssuedPermitReport(
      currentPages(),
      SOURCE,
    );
    expect(first).toBeDefined();
    const duplicate = structuredClone(first);
    const pageBreakVariant = {
      ...structuredClone(first),
      contractor_business_names: ["SECOND CONTRACTOR LLC"],
      raw: {
        ...structuredClone(first.raw),
        source_page: 2,
        source_pages: [2],
        source_reports: [
          {
            ...structuredClone(first.raw.source_reports[0]),
            pages: [2],
          },
        ],
      },
    };
    const conflicting = {
      ...structuredClone(first),
      permit_issue_date: "2026-06-12",
    };

    expect(molinePermitLoaderKey(first)).toBe(
      `${MOLINE_REPORT_SOURCE_SYSTEM}:BP26-000477`,
    );
    expect(dedupeMolineIssuedPermits([first, duplicate])).toEqual([first]);
    expect(
      dedupeMolineIssuedPermits([first, pageBreakVariant])[0],
    ).toMatchObject({
      contractor_business_names: [
        "EXAMPLE ROOFING LLC",
        "SECOND CONTRACTOR LLC",
      ],
      raw: {
        source_page: 1,
        source_pages: [1, 2],
      },
    });
    expect(() => dedupeMolineIssuedPermits([first, conflicting])).toThrow(
      /Conflicting Moline permit variants/,
    );
  });

  it("renders a closed public allowlist without private report fields", () => {
    const [record] = parseCurrentMolineIssuedPermitReport(
      currentPages(),
      SOURCE,
    );
    expect(record).toBeDefined();
    const publicRow = toMolinePublicPermit(record);
    const publicJsonl = renderMolinePublicJsonl([record]);

    expect(publicRow).toEqual({
      permit_key: `${MOLINE_REPORT_SOURCE_SYSTEM}:BP26-000477`,
      source_system: MOLINE_REPORT_SOURCE_SYSTEM,
      source_report_archive_id: "11988",
      source_report_month: "2026-06",
      source_report_title: "2026-06 Building Permits Issued",
      source_report_url: "https://www.moline.il.us/Archive.aspx?ADID=11988",
      source_report_archive_ids: ["11988"],
      source_report_months: ["2026-06"],
      source_report_titles: ["2026-06 Building Permits Issued"],
      source_report_urls: ["https://www.moline.il.us/Archive.aspx?ADID=11988"],
      permit_number: "BP26-000477",
      source_application_year: null,
      source_application_number: null,
      source_permit_code: null,
      permit_issue_date: "2026-06-11",
      record_status: "Issued",
      record_type: "BUILDING REHAB",
      permit_subtype: "RESIDENTIAL",
      city: "Moline",
      is_roof_permit: false,
    });
    expect(publicJsonl).not.toMatch(
      /PRIVATE AV|EXAMPLE ROOFING|contractor|address|valuation|parcel/i,
    );
  });

  it("recognizes organizations conservatively", () => {
    expect(isConservativeMolineBusinessName("EXAMPLE ROOFING LLC")).toBe(true);
    expect(isConservativeMolineBusinessName("CITY OF MOLINE")).toBe(true);
    expect(isConservativeMolineBusinessName("Firstname Lastname")).toBe(false);
  });

  it("parses reordered modern columns and printed numeric permit ids", () => {
    const records = parseCurrentMolineIssuedPermitReport(
      reorderedCurrentPages(),
      {
        archiveId: "10586",
        reportMonth: "2024-11",
        title: "2024-11 Building Permits Issued",
        url: "https://www.moline.il.us/Archive.aspx?ADID=10586",
      },
    );

    expect(records).toHaveLength(1);
    expect(records[0]).toMatchObject({
      permit_number: "24-00002028",
      permit_issue_date: "2024-11-21",
      record_type: "DECKS AND PORCHES",
      contractor_business_names: ["EXAMPLE CONTRACTING LLC"],
      work_location: "903 PRIVATE ST",
      raw: {
        project_valuation: null,
        parser_layout: "current-2024-10-no-value",
      },
    });
  });

  it("parses the explicit transposed-coordinate legacy layout", () => {
    const pages = rotatedLegacyPages();
    const source = {
      archiveId: "6128",
      reportMonth: "2020-02",
      title: "2020-02 Building Permits Issued",
      url: "https://www.moline.il.us/Archive.aspx?ADID=6128",
    };
    const records = parseRotatedLegacyMolineIssuedPermitReport(pages, source);

    expect(isRotatedLegacyMolineReportLayout(pages)).toBe(true);
    expect(records).toHaveLength(2);
    expect(records[0]).toMatchObject({
      permit_number: null,
      permit_issue_date: "2020-02-07",
      record_type: "B: COML INVESTIGATION",
      project_description: "INVESTIGATION",
      contractor_business_names: ["EXAMPLE CONSTRUCTION LLC"],
      work_location: "501 PRIVATE DR",
      raw: {
        source_application_year: "20",
        source_application_number: "171",
        source_permit_code: "BCI",
        source_parcel_text: "08 -4394 -",
        source_permit_status: "PERMIT PRINTED",
        project_valuation: 0,
        parser_layout: "legacy-rotated-v2",
      },
    });
    expect(records[1]?.raw.source_permit_code).toBe("BCI");
    expect(parseMolineIssuedPermitReport(pages, source)).toEqual(records);
  });

  it("parses clean legacy reports using only printed identity fields", () => {
    const source = {
      archiveId: "6985",
      reportMonth: "2021-03",
      title: "2021-03 Building Permits Issued",
      url: "https://www.moline.il.us/Archive.aspx?ADID=6985",
    };
    const pages = legacyPages();
    const records = parseLegacyApplicationMolineIssuedPermitReport(
      pages,
      source,
    );

    expect(isLegacyApplicationMolineReportLayout(pages)).toBe(true);
    expect(inspectLegacyMolineApplicationIdentities(pages)).toMatchObject({
      totalRowCount: 2,
      stableIdentityRowCount: 2,
      ambiguousIdentityRowCount: 0,
    });
    expect(readMolinePrintedPermitTotal(pages)).toBe(2);
    expect(records).toHaveLength(2);
    expect(records[0]).toMatchObject({
      permit_number: null,
      parcel_identifier: null,
      work_location: "1818 53RD ST",
      permit_issue_date: "2021-03-09",
      record_type: "B: COML REMODEL",
      project_description: "BLDG: COML REMODEL",
      contractor_business_names: ["EXAMPLE CONSTRUCTION LLC"],
      raw: {
        source_application_year: "21",
        source_application_number: "300",
        source_permit_code: "BCMR",
        source_parcel_text: "07 -13281 -10",
        source_permit_status: "PERMIT PRINTED",
        project_valuation: 25000,
        parser_layout: "legacy-application-v1",
      },
    });
    expect(records[1]).toMatchObject({
      contractor_business_names: [],
      raw: {
        source_application_number: "301",
        source_permit_code: "BCMR",
      },
    });
    expect(molinePermitLoaderKey(records[0])).toBe(
      `${MOLINE_REPORT_SOURCE_SYSTEM}:application:21:300:BCMR:issued:2021-03-09`,
    );
    expect(parseMolineIssuedPermitReport(pages, source)).toEqual(records);
  });

  it("fails closed when legacy PDF text merges identity columns", () => {
    const pages = [
      [
        item("PermitCodePermitIssueApp.", 90, 520),
        item("DescriptionDateCodeParcel NumberDescriptionValue", 95, 513),
        item("BCMR1/", 26, 497),
        item("2021202356CT$", 169, 497),
      ],
    ];

    expect(isCompactedLegacyMolineReportLayout(pages)).toBe(true);
    expect(() =>
      parseMolineIssuedPermitReport(pages, {
        archiveId: "6830",
        reportMonth: "2021-01",
        title: "2021-01 Building Permits Issued",
        url: "https://www.moline.il.us/Archive.aspx?ADID=6830",
      }),
    ).toThrow(/compacted legacy application identity/);
  });

  it("preserves one-digit legacy application years as printed", () => {
    const pages = legacyPages().map((page) =>
      page.map((value) =>
        value.text === "21" ? { ...value, text: "3" } : value,
      ),
    );
    const records = parseLegacyApplicationMolineIssuedPermitReport(pages, {
      archiveId: "4553",
      reportMonth: "2018-01",
      title: "2018-01 Building Permits Issued",
      url: "https://www.moline.il.us/Archive.aspx?ADID=4553",
    });

    const [first] = records;
    expect(first).toBeDefined();
    if (first === undefined) throw new Error("Expected parsed legacy record");
    expect(first.raw.source_application_year).toBe("3");
    expect(molinePermitLoaderKey(first)).toContain(":application:3:300:");
  });

  it("extracts official identity before merged trailing name text", () => {
    let replacedYear = false;
    const pages = legacyPages().map((page) =>
      page.map((value) => {
        if (value.text === "21" && !replacedYear) {
          replacedYear = true;
          return { ...value, text: "99" };
        }
        if (value.text === "300") {
          return {
            ...value,
            text: "10000025 MCDERMOTT CONSTRUCTION, PETER",
          };
        }
        return value;
      }),
    );
    const records = parseLegacyApplicationMolineIssuedPermitReport(pages, {
      archiveId: "5285",
      reportMonth: "2018-12",
      title: "2018-12 Building Permits Issued",
      url: "https://www.moline.il.us/Archive.aspx?ADID=5285",
    });
    const [first] = records;
    expect(first).toBeDefined();
    if (first === undefined) throw new Error("Expected parsed legacy record");
    expect(first.raw.source_application_year).toBe("99");
    expect(first.raw.source_application_number).toBe("10000025");
  });

  it("rejects a clean legacy report containing a redacted application number", () => {
    const pages = legacyPages().map((page) =>
      page.map((value) =>
        value.text === "300" ? { ...value, text: "########" } : value,
      ),
    );
    const inspection = inspectLegacyMolineApplicationIdentities(pages);

    expect(inspection).toMatchObject({
      totalRowCount: 2,
      stableIdentityRowCount: 1,
      ambiguousIdentityRowCount: 1,
    });
    expect(inspection.ambiguousRows[0]).toMatchObject({
      pageNumber: 1,
      issueDateText: "3/9/2021",
      identityText: "21 ########",
    });
    expect(() =>
      parseLegacyApplicationMolineIssuedPermitReport(pages, {
        archiveId: "2512",
        reportMonth: "2012-12",
        title: "2012-12 Building Permits Issued",
        url: "https://www.moline.il.us/Archive.aspx?ADID=2512",
      }),
    ).toThrow(/1 of 2 rows have ambiguous official application identity/);
  });
});
