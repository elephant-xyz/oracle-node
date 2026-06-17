import { describe, expect, it, vi } from "vitest";
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

  it("returns a detail link when the detail page has divRecordStatus and a related-conditions sub-grid with a Showing banner", () => {
    // Empirical regression (Fix B structural guard): ~12.5% of real Accela
    // CapDetail pages carry a "Showing X-Y of Z" banner inside a related-records
    // or general-conditions sub-grid (e.g. gdvGeneralConditionsList inside
    // divGeneralConditions). The old text-only guard wrongly returned null for
    // those pages. The structural guard checks for the detail-page marker
    // `divRecordStatus` and the main results-list grid `gdvPermitList` instead.
    // A detail page that has `divRecordStatus` must always return the detail
    // link even when its body text contains "Showing 1-5 of 5" from a sub-grid.
    const html = `
      <html><body>
        <form id="aspnetForm" action="./CapDetail.aspx?Module=Permitting&capID1=26CAP&capID2=00000&capID3=00DEF&agencyCode=LEECO">
          <div id="ctl00_PlaceHolderMain_divRecordStatus">
            Message Bar Record &nbsp; MEC2025-00042 :&nbsp; Mechanical AC Replacement
            Record Status: &nbsp; Permit Issued Click here for more information
            Work Location 555 PALM AVE CAPE CORAL FL 33904 * Record Details
          </div>
          <div id="divGeneralConditions">
            <table id="ctl00_PlaceHolderMain_capConditions_gdvGeneralConditionsList">
              <tbody>
                <tr><td>Condition A</td></tr>
                <tr><td>Condition B</td></tr>
              </tbody>
            </table>
            <span class="ACA_SmLabel ACA_SmLabel_FontSize">Showing 1-5 of 5 </span>
          </div>
        </form>
      </body></html>`;

    expect(
      extractCurrentDetailPermitLinkFromHtml({
        html,
        pageUrl:
          "https://aca-prod.accela.com/LEECO/Cap/CapDetail.aspx?Module=Permitting&capID1=26CAP&capID2=00000&capID3=00DEF&agencyCode=LEECO",
        sourceWindowKey: "parcel-111222333",
        sourcePage: 1,
      }),
    ).toMatchObject({
      recordNumber: "MEC2025-00042",
      url: "https://aca-prod.accela.com/LEECO/Cap/CapDetail.aspx?Module=Permitting&capID1=26CAP&capID2=00000&capID3=00DEF&agencyCode=LEECO",
      address: "555 PALM AVE CAPE CORAL FL 33904",
      description: "Mechanical AC Replacement",
      status: "Permit Issued",
      sourceWindowKey: "parcel-111222333",
      sourcePage: 1,
    });
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
      rawText:
        "You are unable to proceed with the requested action due to technical difficulties.",
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

  // ─── Fix B: extractCurrentDetailPermitLinkFromHtml results-list guard ───────

  it("returns null when the page has the main gdvPermitList results grid and no divRecordStatus (results list, not a detail redirect)", () => {
    // A multi-result search page that happens to contain text matching the
    // PERMIT_RECORD_HEADER_PATTERN (e.g. a record preview in the Accela
    // message bar) must NOT be mistaken for a single-permit detail redirect.
    // The structural discriminator (Fix B): gdvPermitList present + no
    // divRecordStatus → results list → return null.
    const html = `
      <html><body>
        <div class="aca_search_result">Showing 1-10 of 100 records found.</div>
        <div>
          Record ELE2025-00077 : Electrical Record Status: Closed-CC Issued
          Click here for more information
        </div>
        <table id="ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList">
          <thead>
            <tr>
              <th></th><th>Record Number</th><th>Address</th><th>Status</th>
            </tr>
          </thead>
          <tbody>
            <tr class="ACA_TabRow_Odd ACA_TabRow_Odd_FontSize">
              <td><input type="checkbox"></td>
              <td><div><a id="ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList_ctl02_hlPermitNumber"
                href="/LEECO/Cap/CapDetail.aspx?Module=Permitting&TabName=Permitting&capID1=25CAP&capID2=00000&capID3=001SR&agencyCode=LEECO&IsToShowInspection=">
                <strong><span>ELE2025-00077</span></strong></a></div></td>
              <td><span>3150 MATECUMBE KEY RD, PUNTA GORDA FL 33955</span></td>
              <td><span>Closed-CC Issued</span></td>
            </tr>
          </tbody>
        </table>
      </body></html>`;

    expect(
      extractCurrentDetailPermitLinkFromHtml({
        html,
        pageUrl:
          "https://aca-prod.accela.com/LEECO/Cap/CapHome.aspx?module=Permitting",
        sourceWindowKey: "parcel-test001",
        sourcePage: 1,
      }),
    ).toBeNull();
  });

  it("returns a permit link when the page is a genuine detail redirect with no gdvPermitList grid", () => {
    // A true single-record CapDetail.aspx redirect: record header present,
    // no gdvPermitList grid, no divRecordStatus (falls through to
    // PERMIT_RECORD_HEADER_PATTERN — the belt-and-suspenders fallback).
    const html = `
      <html><body>
        <form id="aspnetForm" action="./CapDetail.aspx?Module=Permitting&capID1=25CAP&capID2=00000&capID3=001SR&agencyCode=LEECO">
          Message Bar Record &nbsp; ELE2025-00077 :&nbsp; Electrical
          Record Status: &nbsp; Closed-CC Issued Click here for more information
          Work Location 3150 MATECUMBE KEY RD PUNTA GORDA FL 33955 * Record Details
        </form>
      </body></html>`;

    const result = extractCurrentDetailPermitLinkFromHtml({
      html,
      pageUrl:
        "https://aca-prod.accela.com/LEECO/Cap/CapDetail.aspx?Module=Permitting&capID1=25CAP&capID2=00000&capID3=001SR&agencyCode=LEECO",
      sourceWindowKey: "parcel-test001",
      sourcePage: 1,
    });

    expect(result).not.toBeNull();
    expect(result.recordNumber).toBe("ELE2025-00077");
    expect(result.url).toContain("CapDetail.aspx");
    expect(result.sourceWindowKey).toBe("parcel-test001");
    expect(result.sourcePage).toBe(1);
  });

  it("existing detail-redirect fixture still returns a link (Fix B regression)", () => {
    // The fixture from the 'treats direct Accela detail redirects' test above
    // must continue to resolve — it contains no Showing-banner and the Fix B
    // guard must leave it untouched.
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
    ).toMatchObject({ recordNumber: "RES2026-01308" });
  });

  // ─── Fix C: extractPermitLinksFromSearchHtml logger warn + correctness ──────

  it("extracts all CapDetail links from a gdvPermitList results grid with multiple rows", () => {
    // Verifies the happy path using production markup captured from Lee County
    // Accela: two permit rows inside the standard gdvPermitList table.
    const html = `
      <html><body>
        <div>Showing 1-2 of 2 records found.</div>
        <table id="ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList">
          <thead>
            <tr>
              <th></th><th>Record Number</th><th>Address</th><th>Description</th>
              <th>Status</th><th>Action</th><th>Related Records</th><th>Submittal Type</th><th></th>
            </tr>
          </thead>
          <tbody>
            <tr class="ACA_TabRow_Odd ACA_TabRow_Odd_FontSize">
              <td><input type="checkbox"></td>
              <td><div><a id="ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList_ctl02_hlPermitNumber"
                href="/LEECO/Cap/CapDetail.aspx?Module=Permitting&TabName=Permitting&capID1=25CAP&capID2=00000&capID3=001SR&agencyCode=LEECO&IsToShowInspection=">
                <strong><span>ELE2025-00077</span></strong></a></div></td>
              <td><span>3150 MATECUMBE KEY RD, PUNTA GORDA FL 33955</span></td>
              <td><span>Electrical Service</span></td>
              <td><span>Closed-CC Issued</span></td>
              <td></td><td>0</td><td>ePlan</td><td></td>
            </tr>
            <tr class="ACA_TabRow_Even ACA_TabRow_Even_FontSize">
              <td><input type="checkbox"></td>
              <td><div><a id="ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList_ctl03_hlPermitNumber"
                href="/LEECO/Cap/CapDetail.aspx?Module=Permitting&TabName=Permitting&capID1=25CAP&capID2=00000&capID3=002AB&agencyCode=LEECO&IsToShowInspection=">
                <strong><span>MEC2025-00099</span></strong></a></div></td>
              <td><span>100 MAIN ST, FORT MYERS FL 33901</span></td>
              <td><span>HVAC Replacement</span></td>
              <td><span>Permit Issued</span></td>
              <td></td><td>1</td><td>Paper</td><td></td>
            </tr>
          </tbody>
        </table>
      </body></html>`;

    const links = extractPermitLinksFromSearchHtml(html, "parcel-test999", 1);

    expect(links).toHaveLength(2);
    expect(links[0].recordNumber).toBe("ELE2025-00077");
    expect(links[0].url).toBe(
      "https://aca-prod.accela.com/LEECO/Cap/CapDetail.aspx?Module=Permitting&TabName=Permitting&capID1=25CAP&capID2=00000&capID3=001SR&agencyCode=LEECO&IsToShowInspection=",
    );
    expect(links[0].address).toBe(
      "3150 MATECUMBE KEY RD, PUNTA GORDA FL 33955",
    );
    expect(links[0].status).toBe("Closed-CC Issued");
    expect(links[1].recordNumber).toBe("MEC2025-00099");
    expect(links[1].sourcePage).toBe(1);
    expect(links[1].sourceWindowKey).toBe("parcel-test999");
  });

  it("returns empty array and calls logger.warn when all CapDetail links are inside a related-records tree table", () => {
    // Fix C: when every CapDetail anchor is filtered by isRelatedRecordTreeTable
    // (id contains tableCapTreeList, or caption is 'Related Records'), the
    // function must return [] AND emit the diagnostic warning — distinguishing
    // an Accela table-structure change from a genuinely empty page.
    const html = `
      <html><body>
        <div>Showing 1-2 of 2 records found.</div>
        <table id="tableCapTreeList">
          <caption>Related Records</caption>
          <thead>
            <tr>
              <th>Record Number</th><th>Record Type</th><th>Project Name</th><th>Date</th><th>View</th>
            </tr>
          </thead>
          <tbody>
            <tr class="ACA_RelatedCap_Normal">
              <td>ELE2025-00077</td><td>Electrical</td><td>SERVICE UPGRADE</td><td>01/15/2025</td>
              <td><a href="/LEECO/Cap/CapDetail.aspx?Module=Permitting&capID1=25CAP&capID2=00000&capID3=001SR&agencyCode=LEECO">View</a></td>
            </tr>
            <tr class="ACA_RelatedCap_Normal">
              <td>MEC2025-00099</td><td>Mechanical</td><td>HVAC</td><td>02/10/2025</td>
              <td><a href="/LEECO/Cap/CapDetail.aspx?Module=Permitting&capID1=25CAP&capID2=00000&capID3=002AB&agencyCode=LEECO">View</a></td>
            </tr>
          </tbody>
        </table>
      </body></html>`;

    const mockLogger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };

    const links = extractPermitLinksFromSearchHtml(
      html,
      "parcel-test000",
      2,
      mockLogger,
    );

    expect(links).toEqual([]);
    expect(mockLogger.warn).toHaveBeenCalledOnce();
    expect(mockLogger.warn).toHaveBeenCalledWith(
      "lee_search_links_all_filtered_as_related_records",
      expect.objectContaining({
        windowKey: "parcel-test000",
        pageNumber: 2,
        relatedRecordFilteredCount: 2,
      }),
    );
  });

  it("does not call logger.warn when the page has zero CapDetail links at all (genuinely empty)", () => {
    // The warn is only for the case where links existed but were ALL filtered.
    // A page with no CapDetail anchors at all (e.g. a no-results page) must
    // return [] silently — no warn, because there is nothing to diagnose.
    const html = `
      <html><body>
        <div>Your search returned no results.</div>
      </body></html>`;

    const mockLogger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };

    const links = extractPermitLinksFromSearchHtml(
      html,
      "parcel-empty",
      1,
      mockLogger,
    );

    expect(links).toEqual([]);
    expect(mockLogger.warn).not.toHaveBeenCalled();
  });

  // ─── Fix A contract: parseResultSummary change-detection baseline ───────────

  it("parses the Showing-banner used by Fix A pagination change-detection", () => {
    // Fix A snapshots the current summary before clicking Next, then waits for
    // it to change. The normalized form produced by parseResultSummary must
    // match what the in-browser normalization (match[1].replace(/\s+/g," ").trim())
    // produces, so both sides compare like-for-like.
    expect(parseResultSummary("Showing 1-10 of 100 records found.")).toEqual({
      summary: "1-10 of 100",
      total: 100,
    });
    expect(parseResultSummary("Showing 11-20 of 100 records found.")).toEqual({
      summary: "11-20 of 100",
      total: 100,
    });
    // The two summaries are distinct — change-detection between page 1 and
    // page 2 will not false-positive by comparing equal strings.
    expect(
      parseResultSummary("Showing 1-10 of 100 records found.").summary,
    ).not.toBe(
      parseResultSummary("Showing 11-20 of 100 records found.").summary,
    );
  });
});
