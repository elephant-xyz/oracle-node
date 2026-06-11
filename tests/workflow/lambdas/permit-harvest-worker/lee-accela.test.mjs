import { describe, expect, it } from "vitest";
import {
  buildPermitOutputStem,
  buildUnavailablePermitDetail,
  cleanRecordStatus,
  extractCurrentDetailPermitLinkFromHtml,
  extractPermitDetail,
  extractPermitLinksFromSearchHtml,
  normalizeParcelSearchValue,
  parseCompletedInspections,
  parseMoreDetails,
  parseResultSummary,
} from "../../../../workflow/lambdas/permit-harvest-worker/lee-accela.mjs";

describe("lee-accela permit harvest helpers", () => {
  it("normalizes appraisal STRAP values for Accela parcel search", () => {
    expect(normalizeParcelSearchValue("08-46-25-57-00000.0130")).toBe(
      "08462557000000130",
    );
    expect(normalizeParcelSearchValue(" 08462557000000130 * ")).toBe(
      "08462557000000130",
    );
    expect(normalizeParcelSearchValue("")).toBeNull();
  });

  it("parses Accela result summaries", () => {
    expect(parseResultSummary("Showing 1-10 of 100 records")).toEqual({
      summary: "1-10 of 100",
      total: 100,
    });
    expect(parseResultSummary("No records found")).toEqual({
      summary: null,
      total: null,
    });
  });

  it("extracts permit links using result-table headers", () => {
    const html = `
      <table>
        <thead>
          <tr>
            <th></th><th>Record Number</th><th>Address</th><th>Description</th>
            <th>Status</th><th>Action</th><th>Related Records</th><th>Submittal Type</th><th></th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td></td>
            <td><a href="/LEECO/Cap/CapDetail.aspx?Module=Permitting&capID1=25CAP&capID2=00000&capID3=00W9O">MEC2025-02353</a></td>
            <td>2121 CRYSTAL DR, 12, FORT MYERS FL 33907</td>
            <td>REPLACE AC SYSTEM</td>
            <td>Closed-CC Issued</td>
            <td></td>
            <td>0</td>
            <td>ePlan</td>
            <td>2121 CRYSTAL DR, 12, FORT MYERS FL 33907</td>
          </tr>
        </tbody>
      </table>`;

    expect(
      extractPermitLinksFromSearchHtml(html, "20250501_20250501", 1),
    ).toEqual([
      {
        recordNumber: "MEC2025-02353",
        url: "https://aca-prod.accela.com/LEECO/Cap/CapDetail.aspx?Module=Permitting&capID1=25CAP&capID2=00000&capID3=00W9O",
        address: "2121 CRYSTAL DR, 12, FORT MYERS FL 33907",
        description: "REPLACE AC SYSTEM",
        status: "Closed-CC Issued",
        action: null,
        relatedRecords: "0",
        submittalType: "ePlan",
        sourceWindowKey: "20250501_20250501",
        sourcePage: 1,
      },
    ]);
  });

  it("treats direct Accela detail redirects as one permit and ignores related-record View links", () => {
    const html = `
      <form id="aspnetForm" action="./CapDetail.aspx?Module=Permitting&capID1=26CAP&capID2=00000&capID3=009YW&agencyCode=LEECO">
        Message Bar Record &nbsp; RES2026-01308 :&nbsp; Residential New Primary Structure
        Record Status: &nbsp; Permit Issued Click here for more information
        Work Location 21000 VERDANA VILLAGE BLVD ESTERO FL 33928 * Record Details
        <table id="tableCapTreeList">
          <caption>Related Records</caption>
          <tr>
            <th>Record Number</th><th>Record Type</th><th>Project Name</th><th>Date</th><th>View</th>
          </tr>
          <tr class="ACA_RelatedCap_Normal">
            <td>RES2023-17583</td><td>Residential New Primary Structure</td><td>RELATED VILLA</td><td>12/21/2023</td>
            <td><a href="../Cap/CapDetail.aspx?Module=Permitting&capID1=23CAP&capID2=00000&capID3=030B9&agencyCode=LEECO">View</a></td>
          </tr>
        </table>
      </form>`;

    expect(
      extractCurrentDetailPermitLinkFromHtml({
        html,
        pageUrl:
          "https://aca-prod.accela.com/LEECO/Cap/CapDetail.aspx?Module=Permitting&capID1=26CAP&capID2=00000&capID3=009YW&agencyCode=LEECO",
        sourceWindowKey: "parcel-294627l40900c1787",
        sourcePage: 1,
      }),
    ).toMatchObject({
      recordNumber: "RES2026-01308",
      url: "https://aca-prod.accela.com/LEECO/Cap/CapDetail.aspx?Module=Permitting&capID1=26CAP&capID2=00000&capID3=009YW&agencyCode=LEECO",
      address: "21000 VERDANA VILLAGE BLVD ESTERO FL 33928",
      description: "Residential New Primary Structure",
      status: "Permit Issued",
      sourceWindowKey: "parcel-294627l40900c1787",
      sourcePage: 1,
    });
    expect(
      extractPermitLinksFromSearchHtml(html, "parcel-294627l40900c1787", 1),
    ).toEqual([]);
  });

  it("parses common More Details fields and completed inspections", () => {
    const moreDetails = parseMoreDetails(
      "Type Mechanical Private Provider Plan Review? No Private Provider Inspections? No Estimated Job Value 4500 Parcel Number 13452432000000120 Block A Lot 12 Subdivision CRYSTAL",
    );

    expect(moreDetails).toMatchObject({
      Type: "Mechanical",
      "Private Provider Plan Review?": "No",
      "Private Provider Inspections?": "No",
      "Estimated Job Value": "4500",
      "Parcel Number": "13452432000000120",
      Block: "A",
      Lot: "12",
      Subdivision: "CRYSTAL",
    });

    expect(
      parseCompletedInspections(
        "Completed (1) Pass 300 Mechanical Final (987654) Result by: Jane Inspector on 05/09/2025 Digital Projects",
      ),
    ).toEqual([
      {
        result: "Pass",
        inspectionCode: "300",
        inspectionType: "Mechanical Final",
        inspectionIdentifier: "987654",
        inspectorName: "Jane Inspector",
        resultedDate: "05/09/2025",
      },
    ]);
  });

  it("extracts a permit detail object while preserving lexicon-gap fields", () => {
    const html = `
      <body>
        Record MEC2025-02353 : Mechanical Record Status: Closed-CC Issued Click here for more information
        Work Location 2121 CRYSTAL DR 12 FORT MYERS FL 33907 * Record Details
        Applicant: ACME OWNER Licensed Professional: COOL AIR LLC Project Description: REPLACE AC SYSTEM More Details
        Type Mechanical Estimated Job Value 4500 Parcel Number 13452432000000120 Fees *Fee Reductions
        Inspections Completed (1) Pass 300 Mechanical Final (987654) Result by: Jane Inspector on 05/09/2025 Digital Projects
        Processing Status Intake Complete Related Records
        <a href="/LEECO/urlrouting.ashx?type=document&id=1">Click here for more information</a>
      </body>`;

    expect(
      extractPermitDetail({
        html,
        sourceUrl: "https://example.test/detail",
        fallbackRecordNumber: "MEC2025-02353",
      }),
    ).toMatchObject({
      schemaVersion: "permit-harvest.lee-accela.v1",
      source: "lee-county-accela",
      sourceUrl: "https://example.test/detail",
      recordNumber: "MEC2025-02353",
      recordType: "Mechanical",
      recordStatus: "Closed-CC Issued",
      workLocation: "2121 CRYSTAL DR 12 FORT MYERS FL 33907",
      applicant: "ACME OWNER",
      licensedProfessional: "COOL AIR LLC",
      projectDescription: "REPLACE AC SYSTEM",
      parcelIdentifier: "13452432000000120",
      moreDetails: {
        Type: "Mechanical",
        "Estimated Job Value": "4500",
        "Parcel Number": "13452432000000120",
      },
      completedInspections: [
        {
          result: "Pass",
          inspectionCode: "300",
          inspectionType: "Mechanical Final",
          inspectionIdentifier: "987654",
          inspectorName: "Jane Inspector",
          resultedDate: "05/09/2025",
        },
      ],
      documentLinks: [
        {
          text: "Click here for more information",
          url: "https://aca-prod.accela.com/LEECO/urlrouting.ashx?type=document&id=1",
          title: null,
        },
      ],
    });
  });

  it("removes Accela collection controls from historic record statuses", () => {
    expect(
      cleanRecordStatus(
        "Closed-Conversion Create a New Collection * Name: Description: spell check Add Cancel Record Info Record Details Processing Status",
      ),
    ).toBe("Closed-Conversion");

    const html = `
      <body>
        Record COM199903906 : Commercial Record Status: Closed-Conversion Create a New Collection
        * Name: Description: spell check Add Cancel Record Info Record Details Processing Status Related Records
        Work Location 100 INDUSTRIAL WAY FORT MYERS FL 33901 * Record Details
      </body>`;

    expect(
      extractPermitDetail({
        html,
        sourceUrl: "https://example.test/detail",
        fallbackRecordNumber: "COM199903906",
      }).recordStatus,
    ).toBe("Closed-Conversion");
  });

  it("builds explicit unavailable-detail permit artifacts from search evidence", () => {
    const extraction = buildUnavailablePermitDetail({
      permit: {
        recordNumber: "TMP2006-00334",
        url: "https://aca-prod.accela.com/LEECO/Cap/CapDetail.aspx?capID1=06HIS&capID3=0M1UU",
        address: "11920 METRO PKWY, FORT MYERS 33912",
        description: "STEWARTS SLEEP CENTER, INC. - BALLOON",
        status: "Closed-Conversion",
        action: null,
        relatedRecords: "0",
        submittalType: null,
        sourceWindowKey: "parcel-07452500000040070",
        sourcePage: 2,
      },
      sourceUrl: "https://aca-prod.accela.com/LEECO/Error.aspx?ErrorId=example",
      rawText: "You are unable to proceed with the requested action due to technical difficulties.",
      errorMessage: "Accela returned unavailable detail page for TMP2006-00334",
    });

    expect(extraction).toMatchObject({
      schemaVersion: "permit-harvest.lee-accela.v1",
      source: "lee-county-accela",
      sourceUrl: "https://aca-prod.accela.com/LEECO/Error.aspx?ErrorId=example",
      recordNumber: "TMP2006-00334",
      recordType: "Temporary Use",
      recordStatus: "Closed-Conversion",
      workLocation: "11920 METRO PKWY, FORT MYERS 33912",
      projectDescription: "STEWARTS SLEEP CENTER, INC. - BALLOON",
      moreDetails: {
        Type: "Temporary Use",
        "Search Result Description": "STEWARTS SLEEP CENTER, INC. - BALLOON",
        "Search Result Related Records": "0",
        "Accela Detail Capture Status": "unavailable_error_page",
      },
      completedInspections: [],
      documentLinks: [],
      relatedLinks: [],
      detailCaptureStatus: "unavailable_error_page",
      detailCaptureEvidence: {
        recordNumber: "TMP2006-00334",
        sourcePage: "2",
      },
    });
    expect(extraction.rawText).toContain("Accela error page text");
  });

  it("builds stable S3-safe permit output stems", () => {
    const stem = buildPermitOutputStem({
      recordNumber: "MEC2025-02353",
      url: "https://example.test/detail?id=1",
      address: null,
      description: null,
      status: null,
      submittalType: null,
      relatedRecords: null,
      action: null,
      sourceWindowKey: "20250501_20250501",
      sourcePage: 1,
    });

    expect(stem).toMatch(/^mec2025-02353-[a-f0-9]{12}$/);
  });
});
