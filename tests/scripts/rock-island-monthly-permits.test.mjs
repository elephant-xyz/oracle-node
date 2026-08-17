import { describe, expect, it } from "vitest";

import { parseOptions } from "../../scripts/harvest-rock-island-monthly-permits.mjs";
import {
  dedupeAndSortMonthlyPermits,
  extractRockIslandReportLinks,
  isConservativeBusinessName,
  parseRockIslandMonthlyReport,
  renderMonthlyPermitJsonl,
} from "../../scripts/permit-source-adapters/rock-island-monthly-reports.mjs";

const SOURCE = {
  documentId: "12345",
  title: "Synthetic Month",
  url: "https://www.rigov.org/DocumentCenter/View/12345/synthetic",
};

/**
 * Build one synthetic positioned PDF text item.
 *
 * @param {string} text - Synthetic source text.
 * @param {number} x - Horizontal report coordinate.
 * @param {number} y - Vertical report coordinate.
 * @returns {{text: string, x: number, y: number, pageNumber: number}} Positioned text.
 */
function item(text, x, y) {
  return { text, x, y, pageNumber: 1 };
}

describe("Rock Island monthly permit reports", () => {
  it("discovers and deduplicates official DocumentCenter links", () => {
    const html = `
      <a href="/DocumentCenter/View/12026/February-2017">February</a>
      <a href="/DocumentCenter/View/11935/January-2017">January</a>
      <a href="/DocumentCenter/View/12026/February-2017"><strong>February 2017 report</strong></a>
      <a href="/unrelated">Ignore</a>
    `;

    expect(extractRockIslandReportLinks(html)).toEqual([
      {
        documentId: "11935",
        title: "January",
        url: "https://www.rigov.org/DocumentCenter/View/11935/January-2017",
      },
      {
        documentId: "12026",
        title: "February 2017 report",
        url: "https://www.rigov.org/DocumentCenter/View/12026/February-2017",
      },
    ]);
  });

  it("parses fixed columns while excluding owner and person-like contractor names", () => {
    const pages = [
      [
        item("bremodel", 12, 540),
        item("B250001", 18, 520),
        item("08/01/2025", 65, 520),
        item("Private Owner Name", 117, 520),
        item("Owner", 279, 519.5),
        item("REPLACE EXISTING ROOF", 336, 520),
        item("ALL TO CODE.", 336, 510),
        item("12345-6", 540, 519),
        item("100 TEST AVE", 601, 520),
        item("$12,500.00", 722, 520),
        item("Example Construction", 117, 500),
        item("LLC", 117, 490),
        item("Contractor", 279, 489.5),
        item("$12,500.00", 722, 490),
        item("B250002", 18, 470),
        item("08/02/2025", 65, 470),
        item("Another Private Owner", 117, 470),
        item("Owner", 279, 469.5),
        item("REPAIR EXISTING DECK", 336, 470),
        item("67890", 540, 469),
        item("200 TEST AVE", 601, 470),
        item("$500.00", 730, 470),
        item("Person Looking Name", 117, 450),
        item("Contractor", 279, 449.5),
      ],
    ];

    const records = parseRockIslandMonthlyReport(pages, SOURCE);
    expect(records).toHaveLength(2);
    expect(records[0]).toMatchObject({
      permit_number: "B250001",
      parcel_identifier: null,
      permit_issue_date: "2025-08-01",
      record_status: "Issued",
      record_type: "bremodel",
      work_location: "100 TEST AVE",
      project_description: "REPLACE EXISTING ROOF ALL TO CODE.",
      contractor_business_names: ["Example Construction LLC"],
      is_roof_permit: true,
      raw: {
        source_tax_map: "12345-6",
        parcel_match_evidence: "source_tax_map_only_not_joined",
        project_valuation: 12500,
      },
    });
    expect(records[1].contractor_business_names).toEqual([]);

    const jsonl = renderMonthlyPermitJsonl(records);
    expect(jsonl).not.toContain("Private Owner Name");
    expect(jsonl).not.toContain("Another Private Owner");
    expect(jsonl).not.toContain("Person Looking Name");
    expect(jsonl.trim().split("\n")).toHaveLength(2);
  });

  it("requires explicit organization evidence for contractor businesses", () => {
    expect(isConservativeBusinessName("Example Construction")).toBe(true);
    expect(isConservativeBusinessName("Example Holdings LLC")).toBe(true);
    expect(isConservativeBusinessName("Firstname Lastname")).toBe(false);
  });

  it("merges repeated page-break rows without losing alternate locations", () => {
    const pages = [
      [
        item("roof", 12, 540),
        item("B250003", 18, 520),
        item("08/03/2025", 65, 520),
        item("ROOF HOUSE", 336, 520),
        item("12345", 540, 519),
        item("100-102 TEST AVE", 601, 520),
        item("$5,000.00", 730, 520),
      ],
      [
        item("B250003", 18, 520),
        item("08/03/2025", 65, 520),
        item("ROOF HOUSE AND GARAGE ALL TO CODE", 336, 520),
        item("12345", 540, 519),
        item("102 TEST AVE", 601, 520),
        item("$5,000.00", 730, 520),
      ],
    ];
    const parsed = parseRockIslandMonthlyReport(pages, SOURCE);
    const merged = dedupeAndSortMonthlyPermits(parsed);

    expect(parsed).toHaveLength(2);
    expect(merged).toHaveLength(1);
    expect(merged[0]).toMatchObject({
      work_location: "100-102 TEST AVE",
      project_description: "ROOF HOUSE AND GARAGE ALL TO CODE",
      raw: {
        source_pages: [1],
        alternate_work_locations: ["102 TEST AVE"],
        source_tax_map_variants: ["12345"],
        project_valuation_variants: [5000],
      },
    });
  });

  it("parses the newer Tyler report layout without inferring parcel matches", () => {
    const pages = [
      [
        item("Permit Number", 51.4, 732),
        item("Permit Type", 178.2, 732),
        item("Parcel Number", 530.1, 732),
        item("BLDR-202600001", 51.4, 721),
        item("100 TEST AVE", 108, 726),
        item("ROCK ISLAND, IL 61201", 108, 721),
        item("Building (Residential)", 178.2, 721),
        item("04/02/2026", 239.5, 721),
        item("Replace existing roof", 264, 726),
        item("all to code", 264, 721),
        item("04/01/2026", 470.1, 721),
        item("$20,500.00", 505.6, 721),
        item("1611402007", 536.4, 721),
      ],
      [
        item("PLMB-2026-00002", 51.4, 700),
        item("200 TEST AVE", 108, 705),
        item("ROCK ISLAND, IL 61201", 108, 700),
        item("Plumbing", 178.2, 700),
        item("04/03/2026 Water piping", 239.5, 700),
        item("04/02/2026", 470.1, 700),
        item("$0.00", 516.7, 700),
        item("1611402008", 536.4, 700),
      ],
    ];

    expect(parseRockIslandMonthlyReport(pages, SOURCE)).toEqual([
      expect.objectContaining({
        permit_number: "BLDR-202600001",
        parcel_identifier: null,
        work_location: "100 TEST AVE ROCK ISLAND, IL 61201",
        permit_issue_date: "2026-04-02",
        record_type: "Building (Residential)",
        project_description: "Replace existing roof all to code",
        contractor_business_names: [],
        is_roof_permit: true,
        raw: expect.objectContaining({
          source_tax_map: "1611402007",
          parcel_match_evidence: "source_tax_map_only_not_joined",
          project_valuation: 20500,
        }),
      }),
      expect.objectContaining({
        permit_number: "PLMB-2026-00002",
        permit_issue_date: "2026-04-03",
        record_type: "Plumbing",
        project_description: "Water piping",
        raw: expect.objectContaining({
          source_tax_map: "1611402008",
          project_valuation: 0,
        }),
      }),
    ]);
  });

  it("enforces pilot limits and serial delay guardrails", () => {
    expect(
      parseOptions([
        "--mode",
        "pilot",
        "--max-records",
        "24",
        "--delay-ms",
        "1500",
        "--report-id",
        "19959",
      ]),
    ).toMatchObject({
      mode: "pilot",
      maxRecords: 24,
      delayMs: 1500,
      reportId: "19959",
    });
    expect(() =>
      parseOptions(["--mode", "pilot", "--max-records", "26"]),
    ).toThrow("through 25");
    expect(() => parseOptions(["--mode", "full", "--delay-ms", "999"])).toThrow(
      "at least 1000",
    );
    expect(() =>
      parseOptions(["--mode", "full", "--report-id", "19959"]),
    ).toThrow("only valid in pilot mode");
  });
});
