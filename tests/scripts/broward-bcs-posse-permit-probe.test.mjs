import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import {
  BROWARD_BCS_PILOT_PARCEL_IDS,
  BROWARD_BCS_SEARCH_URL,
  dedupeAndSortBrowardBcsPermits,
  isBrowardBcsRoofPermitCandidate,
  normalizeBrowardBcsParcelId,
  parseBrowardBcsDetailHtml,
  parseBrowardBcsPermitListHtml,
  renderBrowardBcsPermitJsonl,
  validateBrowardBcsParcelIds,
} from "../../scripts/permit-source-adapters/broward-bcs-posse.mjs";
import { parseOptions } from "../../scripts/probe-broward-bcs-permits.mjs";

const LIST_URL =
  "https://dpepp.broward.org/BCS/Default.aspx?PossePresentation=ParcelPermitList&PosseObjectId=340335";

const [listHtml, emptyListHtml, masterHtml, permitHtml] = await Promise.all(
  [
    "parcel-permit-list.html",
    "parcel-permit-list-empty.html",
    "view-master-permit.html",
    "view-permit.html",
  ].map((name) =>
    readFile(
      new URL(`../fixtures/broward-bcs-posse/${name}`, import.meta.url),
      "utf8",
    ),
  ),
);

describe("Broward BCS POSSE property-first permit pilot", () => {
  it("filters roofing from BCS list fields before detail requests", () => {
    const base = {
      sourceUrl: "https://example.test/permit",
      sourceObjectId: "1",
      sourceRecordKind: "permit",
      permitNumber: "P-1",
      recordType: "Building Alteration",
      recordStatus: "Open",
      permitIssueDate: null,
      listContractor: null,
    };
    expect(
      isBrowardBcsRoofPermitCandidate({
        ...base,
        recordType: "Residential Re-Roof",
      }),
    ).toBe(true);
    expect(isBrowardBcsRoofPermitCandidate(base)).toBe(false);
  });

  it("preserves letters in exact 12-character parcel IDs and enforces five lookups", () => {
    expect(normalizeBrowardBcsParcelId(" 504108bj0140 ")).toBe("504108BJ0140");
    expect(() => normalizeBrowardBcsParcelId("504108-BJ-0140")).toThrow(
      "exactly 12 alphanumeric",
    );
    expect(() => normalizeBrowardBcsParcelId(5041080140)).toThrow(
      "must be a string",
    );
    expect(() =>
      validateBrowardBcsParcelIds(
        [
          "474135010090",
          "494209060010",
          "494318013550",
          "474236140090",
          "474236140080",
          "504108BJ0140",
        ],
        5,
      ),
    ).toThrow("approved maximum is 5");
    expect(() =>
      validateBrowardBcsParcelIds(["504108BJ0140", "504108bj0140"], 5),
    ).toThrow("must be unique");
  });

  it("parses permit/master rows, excludes plan reviews, and normalizes source dates", () => {
    const parsed = parseBrowardBcsPermitListHtml(listHtml, LIST_URL);

    expect(parsed).toEqual({
      status: "records",
      parcelObjectId: "340335",
      listedRecordCount: 3,
      excludedPlanReviewCount: 1,
      records: [
        {
          sourceUrl:
            "https://dpepp.broward.org/BCS/Default.aspx?PossePresentation=ViewMasterPermit&PosseObjectId=3920351",
          sourceObjectId: "3920351",
          sourceRecordKind: "master",
          permitNumber: "00-01749",
          recordType: "SHUT: BLDG-SHUTTERS,PANELS",
          recordStatus: "Complete",
          permitIssueDate: null,
          listContractor: "Schlitten, Arthur D.",
        },
        {
          sourceUrl:
            "https://dpepp.broward.org/BCS/Default.aspx?PossePresentation=ViewPermit&PosseObjectId=15703657",
          sourceObjectId: "15703657",
          sourceRecordKind: "permit",
          permitNumber: "04-07545",
          recordType: "CALT: BLDG-COMMERCIAL INT. PARTITIONS/ALTER",
          recordStatus: "Cancelled",
          permitIssueDate: "2005-02-17",
          listContractor: "PERRY, DAVID W.",
        },
      ],
    });
  });

  it("accepts only the exact valid-parcel empty marker and otherwise fails closed", () => {
    expect(
      parseBrowardBcsPermitListHtml(
        emptyListHtml,
        "https://dpepp.broward.org/BCS/Default.aspx?PossePresentation=ParcelPermitList&PosseObjectId=837",
      ),
    ).toEqual({
      status: "no_permits",
      parcelObjectId: "837",
      listedRecordCount: 0,
      excludedPlanReviewCount: 0,
      records: [],
    });

    expect(() =>
      parseBrowardBcsPermitListHtml(
        emptyListHtml.replace(
          "No permits were found for this address.",
          "No data.",
        ),
        LIST_URL,
      ),
    ).toThrow("no explicit no-permits marker");
    expect(() =>
      parseBrowardBcsPermitListHtml(
        listHtml.replace("BCS - Permits", "Unexpected Page"),
        LIST_URL,
      ),
    ).toThrow("Unexpected Broward BCS parcel-list title");
  });

  it("normalizes official master detail fields without copying owner data", () => {
    const parsedList = parseBrowardBcsPermitListHtml(listHtml, LIST_URL);
    const masterListRecord = parsedList.records[0];
    expect(masterListRecord).toBeDefined();

    const record = parseBrowardBcsDetailHtml(masterHtml, {
      listRecord: masterListRecord,
      parcelIdentifier: "494318013550",
      sourceSearchUrl: BROWARD_BCS_SEARCH_URL,
      sourceListUrl: LIST_URL,
    });

    expect(record).toMatchObject({
      source_system: "broward_county_bcs_posse_permits",
      source_record_kind: "master",
      record_key: "broward_county_bcs_posse_permits:3920351",
      parcel_identifier: "494318013550",
      source_folio_number: "9318-01-3550",
      issuing_jurisdiction: "Lauderdale by the Sea",
      permit_number: "00-01749",
      record_status: "Complete",
      record_type: "SHUT: BLDG-SHUTTERS,PANELS",
      permit_issue_date: null,
      application_date: "2000-02-17",
      expiration_date: "2001-02-17",
      project_title: null,
      project_description: "INSTALL TEST PANELS",
      work_location: "218 COMMERCIAL BLVD FORT LAUDERDALE, FL 33308",
      contractor_name: "Schlitten, Arthur D.",
      contractor_license: "92-6819-AE-X - Broward County",
      building_use: "Data Unavailable",
      present_use: "Data Unavailable",
      proposed_use: "Data Unavailable",
      job_value: 424,
      square_footage: 1200,
      occupancy_type: "Business",
      construction_type: "Type II",
      occupant_load: 12,
      finish_floor_above_road: 1.5,
      finish_floor_above_sea_level: 7,
      inspections: [],
      is_roof_permit: false,
    });
    expect(JSON.stringify(record)).not.toMatch(
      /FIXTURE OWNER|PRIVATE OWNER ADDRESS/,
    );
  });

  it("normalizes permit detail and inspection provenance", () => {
    const parsedList = parseBrowardBcsPermitListHtml(listHtml, LIST_URL);
    const permitListRecord = parsedList.records[1];
    expect(permitListRecord).toBeDefined();

    const record = parseBrowardBcsDetailHtml(permitHtml, {
      listRecord: permitListRecord,
      parcelIdentifier: "494318013550",
      sourceSearchUrl: BROWARD_BCS_SEARCH_URL,
      sourceListUrl: LIST_URL,
    });

    expect(record).toMatchObject({
      source_record_kind: "permit",
      source_object_id: "15703657",
      permit_issue_date: "2005-02-17",
      project_description: "INTERIOR PARTITIONS",
      work_location: "218 COMMERCIAL BLVD FORT LAUDERDALE, FL 33308",
      raw: {
        search_method: "ParcelID",
        reference_number: "4367660",
        list_contractor: "PERRY, DAVID W.",
        detail_page_title: "BCS - Permit",
      },
      inspections: [
        {
          source_url:
            "https://dpepp.broward.org/BCS/Default.aspx?PossePresentation=ViewInspection&PosseObjectId=16079182",
          source_object_id: "16079182",
          inspection_type: "Drywall",
          requested_date: "2005-05-04",
          result: "Passed",
          completed_date: "2005-05-04",
        },
      ],
    });
  });

  it("reconciles source identity and rejects a mismatched detail folio", () => {
    const permitListRecord = parseBrowardBcsPermitListHtml(listHtml, LIST_URL)
      .records[1];
    expect(permitListRecord).toBeDefined();

    expect(() =>
      parseBrowardBcsDetailHtml(
        permitHtml.replace("9318-01-3550", "111111111111"),
        {
          listRecord: permitListRecord,
          parcelIdentifier: "494318013550",
          sourceSearchUrl: BROWARD_BCS_SEARCH_URL,
          sourceListUrl: LIST_URL,
        },
      ),
    ).toThrow("does not match submitted parcel");
    expect(() =>
      parseBrowardBcsDetailHtml(permitHtml.replace("04-07545", "DIFFERENT"), {
        listRecord: permitListRecord,
        parcelIdentifier: "494318013550",
        sourceSearchUrl: BROWARD_BCS_SEARCH_URL,
        sourceListUrl: LIST_URL,
      }),
    ).toThrow("detail identity differs");
  });

  it("deduplicates exact records, rejects conflicts, and renders deterministic JSONL", () => {
    const parsedList = parseBrowardBcsPermitListHtml(listHtml, LIST_URL);
    const master = parseBrowardBcsDetailHtml(masterHtml, {
      listRecord: parsedList.records[0],
      parcelIdentifier: "494318013550",
      sourceSearchUrl: BROWARD_BCS_SEARCH_URL,
      sourceListUrl: LIST_URL,
    });
    const permit = parseBrowardBcsDetailHtml(permitHtml, {
      listRecord: parsedList.records[1],
      parcelIdentifier: "494318013550",
      sourceSearchUrl: BROWARD_BCS_SEARCH_URL,
      sourceListUrl: LIST_URL,
    });

    expect(
      dedupeAndSortBrowardBcsPermits([permit, master, permit]),
    ).toHaveLength(2);
    const jsonl = renderBrowardBcsPermitJsonl([permit, master, permit]);
    expect(jsonl.trim().split("\n")).toHaveLength(2);
    expect(jsonl.endsWith("\n")).toBe(true);

    expect(() =>
      dedupeAndSortBrowardBcsPermits([
        permit,
        { ...permit, record_status: "Different" },
      ]),
    ).toThrow("Conflicting Broward BCS records");
  });

  it("parses only bounded pilot or explicit CLI input modes", () => {
    expect(parseOptions(["--pilot"])).toEqual({
      parcelIds: BROWARD_BCS_PILOT_PARCEL_IDS,
      isCuratedPilot: true,
      outputPath: null,
      summaryPath: null,
      propertyDelayMs: 1500,
      detailDelayMs: 300,
      roofOnly: false,
    });
    expect(
      parseOptions([
        "--parcel-id",
        "504108bj0140",
        "--output",
        "downloads/broward/bcs.jsonl",
        "--summary=downloads/broward/bcs-summary.json",
        "--property-delay-ms",
        "1250",
        "--detail-delay-ms=275",
      ]),
    ).toEqual({
      parcelIds: ["504108BJ0140"],
      isCuratedPilot: false,
      outputPath: "downloads/broward/bcs.jsonl",
      summaryPath: "downloads/broward/bcs-summary.json",
      propertyDelayMs: 1250,
      detailDelayMs: 275,
      roofOnly: false,
    });
    expect(parseOptions(["--pilot", "--roof-only"])?.roofOnly).toBe(true);
    expect(() => parseOptions([])).toThrow("exactly one");
    expect(() =>
      parseOptions(["--pilot", "--parcel-id", "504108BJ0140"]),
    ).toThrow("exactly one");
    expect(() =>
      parseOptions(["--parcel-id", "504108BJ0140", "--detail-delay-ms", "249"]),
    ).toThrow("at least 250");
  });
});
