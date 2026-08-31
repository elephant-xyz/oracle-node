import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  checkpointCapturedPermit,
  checkpointCompletedSearchPage,
  createPermitAdapterCheckpoint,
  dedupeAndSortMunicipalPermits,
  loadPermitAdapterCheckpoint,
  normalizePermitSearchQuery,
  renderMunicipalPermitJsonl,
  writePermitAdapterCheckpoint,
} from "../../scripts/permit-source-adapters/bounded-permit-common.mjs";
import {
  BROWARD_PERMIT_JURISDICTIONS,
  getBrowardPermitJurisdiction,
} from "../../scripts/permit-source-adapters/broward-permit-jurisdictions.mjs";
import {
  buildCitizenserveSearchUrl,
  isCitizenserveRoofPermitCandidate,
  parseCitizenservePermitDetailHtml,
  parseCitizenserveSearchResultsHtml,
} from "../../scripts/permit-source-adapters/citizenserve.mjs";
import {
  buildTylerDateWindowRequest,
  isTylerRoofPermitCandidate,
  nextSmallerTylerPageSize,
  normalizeTylerPermitDetailResponse,
  readTylerTotalPages,
} from "../../scripts/permit-source-adapters/tyler-civic-access.mjs";
import { parseOptions } from "../../scripts/probe-broward-municipal-permits.mjs";
import {
  createTylerDateWindows,
  parseTylerDateWindowOptions,
  runTylerDateWindows,
} from "../../scripts/run-broward-tyler-date-windows.mjs";

const fixtureDirectory = new URL(
  "../fixtures/broward-municipal-permits/",
  import.meta.url,
);
const [tylerDetail, citizenserveResults, citizenserveDetail] =
  await Promise.all([
    readFile(new URL("tyler-detail-response.json", fixtureDirectory), "utf8"),
    readFile(
      new URL("citizenserve-search-results.html", fixtureDirectory),
      "utf8",
    ),
    readFile(new URL("citizenserve-detail.html", fixtureDirectory), "utf8"),
  ]);

const pembrokePines = getBrowardPermitJurisdiction("pembroke_pines");
const lauderdaleByTheSea = getBrowardPermitJurisdiction(
  "lauderdale_by_the_sea",
);

describe("Broward Tyler and Citizenserve jurisdiction routing", () => {
  it("filters roofing candidates from public result-list fields", () => {
    expect(
      isTylerRoofPermitCandidate({
        caseId: "1",
        permitNumber: "P-1",
        entity: { CaseType: "Re-Roof", CaseWorkclass: "Residential" },
      }),
    ).toBe(true);
    expect(
      isTylerRoofPermitCandidate({
        caseId: "2",
        permitNumber: "P-2",
        entity: { CaseType: "Plumbing", CaseWorkclass: "Repair" },
      }),
    ).toBe(false);
    expect(
      isCitizenserveRoofPermitCandidate({
        permitId: "1",
        workOrderId: "1",
        permitNumber: "P-1",
        detailUrl: "https://example.test/1",
        workLocation: null,
        recordType: "Building",
        workClass: "Roofing",
        recordStatus: "Issued",
        issueDate: null,
        description: null,
      }),
    ).toBe(true);
  });

  it("registers every requested jurisdiction and keeps North Lauderdale disabled", () => {
    expect(Object.keys(BROWARD_PERMIT_JURISDICTIONS)).toEqual([
      "pembroke_pines",
      "hallandale_beach",
      "miramar",
      "oakland_park",
      "north_lauderdale",
      "lauderdale_by_the_sea",
      "southwest_ranches",
      "west_park",
      "wilton_manors",
    ]);
    expect(
      Object.values(BROWARD_PERMIT_JURISDICTIONS).filter(
        (config) => config.vendor === "tyler-civic-access",
      ),
    ).toHaveLength(5);
    expect(
      Object.values(BROWARD_PERMIT_JURISDICTIONS).filter(
        (config) => config.vendor === "citizenserve",
      ),
    ).toHaveLength(4);
    expect(BROWARD_PERMIT_JURISDICTIONS.north_lauderdale).toMatchObject({
      anonymousSearchCertified: false,
      searchKinds: [],
    });
    expect(BROWARD_PERMIT_JURISDICTIONS.north_lauderdale.skipReason).toMatch(
      /requires login/iu,
    );
    expect(BROWARD_PERMIT_JURISDICTIONS.oakland_park.coverageNote).toContain(
      "after 2019-11-01",
    );
  });

  it("preserves exact alphanumeric folios and supports situs-address mode", () => {
    expect(
      normalizePermitSearchQuery({
        kind: "folio",
        value: " 504108bj0140 ",
      }),
    ).toEqual({ kind: "folio", value: "504108BJ0140" });
    expect(() =>
      normalizePermitSearchQuery({
        kind: "folio",
        value: "504108-BJ-0140",
      }),
    ).toThrow("exactly 12 undashed alphanumeric");
    expect(
      normalizePermitSearchQuery({
        kind: "address",
        value: "  218 E  COMMERCIAL BOULEVARD ",
      }),
    ).toEqual({
      kind: "address",
      value: "218 E COMMERCIAL BOULEVARD",
    });
  });
});

describe("Tyler vendor-wide application-date windows", () => {
  it("builds the exact advanced Permit request from the complete UI model", () => {
    const template = {
      Keyword: "parcel",
      ExactMatch: true,
      SearchModule: 1,
      FilterModule: 1,
      PermitCriteria: {
        PermitTypeId: null,
        PermitStatusId: null,
        ApplyDateFrom: null,
        ApplyDateTo: null,
        PageNumber: 0,
        PageSize: 0,
      },
      PlanCriteria: {},
      SortOrderList: [],
    };
    const request = buildTylerDateWindowRequest(
      template,
      "2026-08-30",
      "2026-08-31",
      2,
      100,
    );
    expect(request).toMatchObject({
      Keyword: "",
      ExactMatch: true,
      SearchModule: 2,
      FilterModule: 0,
      PageNumber: 2,
      PageSize: 100,
      SortBy: "PermitNumber.keyword",
      PermitCriteria: {
        PermitTypeId: "none",
        PermitStatusId: "none",
        ApplyDateFrom: "2026-08-30T00:00:00.000Z",
        ApplyDateTo: "2026-08-31T00:00:00.000Z",
        PageNumber: 2,
        PageSize: 100,
      },
      PlanCriteria: {},
      SortOrderList: [],
    });
    expect(template).toMatchObject({
      Keyword: "parcel",
      SearchModule: 1,
      PermitCriteria: { ApplyDateFrom: null },
    });
    expect(nextSmallerTylerPageSize(100)).toBe(50);
    expect(nextSmallerTylerPageSize(50)).toBe(25);
    expect(nextSmallerTylerPageSize(25)).toBe(10);
    expect(nextSmallerTylerPageSize(10)).toBeNull();
  });

  it("creates non-overlapping windows and validates anonymous Tyler tenants", () => {
    expect(
      createTylerDateWindows("2026-08-28", "2026-08-31", 2),
    ).toEqual([
      { startDate: "2026-08-28", endDate: "2026-08-29" },
      { startDate: "2026-08-30", endDate: "2026-08-31" },
    ]);
    expect(
      parseTylerDateWindowOptions([
        "--source",
        "oakland_park",
        "--start-date",
        "2019-11-01",
        "--end-date",
        "2026-08-31",
        "--max-windows",
        "1",
      ]),
    ).toMatchObject({
      sourceKey: "oakland_park",
      startDate: "2019-11-01",
      pageSize: 100,
      maxWindows: 1,
    });
    expect(() =>
      parseTylerDateWindowOptions([
        "--source",
        "north_lauderdale",
        "--start-date",
        "2026-01-01",
        "--end-date",
        "2026-01-02",
      ]),
    ).toThrow(/pembroke_pines/u);
  });

  it("checkpoints and resumes a persistent tenant window run", async () => {
    const outputDirectory = await mkdtemp(
      join(tmpdir(), "broward-tyler-windows-"),
    );
    const searched = [];
    let sessions = 0;
    let closed = 0;
    const baseOptions = {
      sourceKey: "pembroke_pines",
      startDate: "2026-08-28",
      endDate: "2026-08-31",
      windowDays: 2,
      pageSize: 100,
      maxPages: 200,
      delayMs: 1_000,
      maxWindows: 1,
      outputDirectory,
    };
    try {
      const dependencies = {
        createSession: async (config) => {
          sessions += 1;
          return { config };
        },
        closeSession: async () => {
          closed += 1;
        },
        searchWindow: async (session, startDate, endDate) => {
          searched.push(`${startDate}:${endDate}`);
          const caseId = `case-${startDate}`;
          const record = {
            source_system: session.config.sourceSystem,
            source_url: `${session.config.portalBaseUrl}#/permit/${caseId}`,
            city: session.config.city,
            permit_number: `P-${startDate}`,
            parcel_identifier: null,
            work_location: "100 TEST AVE",
            permit_issue_date: null,
            record_status: "Open",
            record_type: "Building",
            project_description: "Permit",
            is_roof_permit: false,
            raw: {
              case_id: caseId,
              work_class: null,
              applied_date: startDate,
              expiration_date: null,
              finalized_date: null,
            },
          };
          return {
            startDate,
            endDate,
            totalFound: 1,
            totalPages: 1,
            records: [record],
            invalidRecordCount: 0,
            pages: [
              {
                pageNumber: 1,
                totalFound: 1,
                totalPages: 1,
                records: [record],
                invalidRecordCount: 0,
                rawJson: JSON.stringify({ Result: { EntityResults: [] } }),
              },
            ],
          };
        },
        wait: async () => undefined,
        now: () => "2026-08-31T18:00:00.000Z",
      };
      const first = await runTylerDateWindows(
        baseOptions,
        dependencies,
      );
      expect(first).toMatchObject({
        status: "paused",
        completedWindowCount: 1,
        pendingWindowCount: 1,
        uniquePermitCount: 1,
      });
      const resumed = await runTylerDateWindows(
        { ...baseOptions, maxWindows: null },
        dependencies,
      );
      expect(resumed).toMatchObject({
        status: "complete",
        completedWindowCount: 2,
        pendingWindowCount: 0,
        uniquePermitCount: 2,
      });
      expect(searched).toEqual([
        "2026-08-28:2026-08-29",
        "2026-08-30:2026-08-31",
      ]);
      expect(sessions).toBe(2);
      expect(closed).toBe(2);
    } finally {
      await rm(outputDirectory, { recursive: true, force: true });
    }
  });
});

describe("bounded Tyler Civic Access detail normalization", () => {
  it("reconciles a folio-backed search result and excludes contact fields", () => {
    const payload = JSON.parse(tylerDetail);
    const record = normalizeTylerPermitDetailResponse(payload, {
      config: pembrokePines,
      query: { kind: "folio", value: "513914101320" },
      searchPage: 1,
      searchUrl:
        "https://pembrokepinesfl-energovweb.tylerhost.net/apps/selfservice#/search?pn=1",
      candidate: {
        caseId: "11111111-2222-4333-8444-555555555555",
        permitNumber: "BUL-200001",
        entity: {
          MainParcel: "513914101320",
          CaseType: "Residential Re-Roofing",
          CaseWorkclass: "RESIDENTIAL RE-ROOF",
        },
      },
    });

    expect(record).toMatchObject({
      source_system: "broward_pembroke_pines_tyler_permits",
      source_vendor: "tyler_energov_civic_access",
      source_record_id: "11111111-2222-4333-8444-555555555555",
      record_key:
        "broward_pembroke_pines_tyler_permits:11111111-2222-4333-8444-555555555555",
      city: "Pembroke Pines",
      permit_number: "BUL-200001",
      parcel_identifier: "513914101320",
      work_location: "470 SW 198 TER PEMBROKE PINES FL 33029",
      permit_issue_date: "2020-01-15",
      application_date: "2020-01-10",
      expiration_date: "2020-07-15",
      finalized_date: "2020-02-01",
      record_status: "Complete",
      record_type: "Residential Re-Roofing",
      work_class: "RESIDENTIAL RE-ROOF",
      square_feet: 1800,
      job_value: 12500,
      is_roof_permit: true,
      provenance: {
        query_kind: "folio",
        query_value: "513914101320",
        search_page: 1,
      },
    });
    expect(JSON.stringify(record)).not.toMatch(
      /EXCLUDED FIXTURE ASSIGNEE|excluded@example|AssignedTo/iu,
    );
  });

  it("fails closed when global-search identity is not the submitted folio", () => {
    const payload = JSON.parse(tylerDetail);
    payload.Result.MainParcelNumber = "999999999999";
    expect(() =>
      normalizeTylerPermitDetailResponse(payload, {
        config: pembrokePines,
        query: { kind: "folio", value: "513914101320" },
        searchPage: 1,
        searchUrl:
          "https://pembrokepinesfl-energovweb.tylerhost.net/apps/selfservice#/search?pn=1",
        candidate: {
          caseId: "11111111-2222-4333-8444-555555555555",
          permitNumber: "BUL-200001",
          entity: { MainParcel: "513914101320" },
        },
      }),
    ).toThrow("detail parcel differs");
  });

  it("accepts zero Tyler pages only as a typed empty pagination count", () => {
    expect(
      readTylerTotalPages({
        Success: true,
        Result: { TotalPages: 0, EntityResults: [], TotalFound: 0 },
      }),
    ).toBe(0);
    expect(() =>
      readTylerTotalPages({
        Success: true,
        Result: { TotalPages: -1 },
      }),
    ).toThrow("non-negative integer");
  });
});

describe("bounded Citizenserve/CAP Government parsing", () => {
  it("parses bounded pagination and filters shared installation 117 by city", () => {
    const page = parseCitizenserveSearchResultsHtml(citizenserveResults, {
      config: lauderdaleByTheSea,
      pageNumber: 1,
    });

    expect(page).toMatchObject({
      pageNumber: 1,
      rangeStart: 1,
      rangeEnd: 2,
      reportedTotal: 32,
      excludedJurisdictionCount: 1,
      nextRange: { start: 2, end: 32 },
    });
    expect(page.candidates).toHaveLength(1);
    expect(page.candidates[0]).toMatchObject({
      permitId: "1400001",
      workOrderId: "70000001",
      permitNumber: "LBS20-000001",
      workLocation: "218 E COMMERCIAL BLVD",
      recordType: "Lauderdale-By-The-Sea Permit",
      issueDate: "2020-02-06",
    });
    expect(page.candidates[0].detailUrl).toBe(
      "https://www6.citizenserve.com/Portal/PortalController?Action=viewPortalCase&type=Permit&workOrder_ID=70000001&permit_ID=1400001&installationID=117",
    );
  });

  it("normalizes detail provenance without traversing account-only tabs", () => {
    const page = parseCitizenserveSearchResultsHtml(citizenserveResults, {
      config: lauderdaleByTheSea,
      pageNumber: 1,
    });
    const searchUrl = buildCitizenserveSearchUrl(lauderdaleByTheSea);
    const record = parseCitizenservePermitDetailHtml(citizenserveDetail, {
      config: lauderdaleByTheSea,
      query: { kind: "folio", value: "494318013550" },
      searchPage: 1,
      searchUrl,
      candidate: page.candidates[0],
    });

    expect(record).toMatchObject({
      source_system: "broward_lauderdale_by_the_sea_citizenserve_permits",
      source_vendor: "citizenserve_cap_government",
      source_record_id: "1400001",
      record_key: "broward_lauderdale_by_the_sea_citizenserve_permits:1400001",
      city: "Lauderdale-by-the-Sea",
      permit_number: "LBS20-000001",
      parcel_identifier: "494318013550",
      permit_issue_date: "2020-02-06",
      expiration_date: "2020-08-06",
      record_status: "Closed",
      record_type: "Lauderdale-By-The-Sea Permit",
      work_class: "Structural",
      project_description: "RE-ROOF",
      is_roof_permit: true,
      provenance: {
        official_source_url:
          "https://lauderdalebythesea-fl.gov/152/Building-Division",
        search_url: searchUrl,
        query_kind: "folio",
        query_value: "494318013550",
        search_page: 1,
      },
      raw: {
        permit_id: "1400001",
        work_order_id: "70000001",
        project_number: "20-000101",
      },
    });
  });

  it("rejects result links that leave the configured public installation", () => {
    expect(() =>
      parseCitizenserveSearchResultsHtml(
        citizenserveResults.replace("installationID=117", "installationID=999"),
        {
          config: lauderdaleByTheSea,
          pageNumber: 1,
        },
      ),
    ).toThrow("left the configured public source");
  });
});

describe("permit checkpointing and local CLI guardrails", () => {
  it("checkpoints exact details/pages and preserves owner-only local state", async () => {
    const page = parseCitizenserveSearchResultsHtml(citizenserveResults, {
      config: lauderdaleByTheSea,
      pageNumber: 1,
    });
    const record = parseCitizenservePermitDetailHtml(citizenserveDetail, {
      config: lauderdaleByTheSea,
      query: { kind: "folio", value: "494318013550" },
      searchPage: 1,
      searchUrl: buildCitizenserveSearchUrl(lauderdaleByTheSea),
      candidate: page.candidates[0],
    });
    let checkpoint = createPermitAdapterCheckpoint(
      lauderdaleByTheSea.sourceSystem,
      { kind: "folio", value: "494318013550" },
    );
    checkpoint = checkpointCapturedPermit(checkpoint, record);
    checkpoint = checkpointCompletedSearchPage(checkpoint, 1);

    const root = await mkdtemp(join(tmpdir(), "broward-permit-checkpoint-"));
    const checkpointPath = join(root, "checkpoint.json");
    try {
      await writePermitAdapterCheckpoint(checkpointPath, checkpoint);
      const loaded = await loadPermitAdapterCheckpoint(
        checkpointPath,
        lauderdaleByTheSea.sourceSystem,
        { kind: "folio", value: "494318013550" },
      );
      expect(loaded).toEqual(checkpoint);
      expect((await stat(checkpointPath)).mode & 0o777).toBe(0o600);
      expect(await readFile(checkpointPath, "utf8")).toContain(
        '"completedSearchPages": [',
      );
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it("deduplicates exact records, rejects conflicts, and renders stable JSONL", () => {
    const page = parseCitizenserveSearchResultsHtml(citizenserveResults, {
      config: lauderdaleByTheSea,
      pageNumber: 1,
    });
    const record = parseCitizenservePermitDetailHtml(citizenserveDetail, {
      config: lauderdaleByTheSea,
      query: { kind: "folio", value: "494318013550" },
      searchPage: 1,
      searchUrl: buildCitizenserveSearchUrl(lauderdaleByTheSea),
      candidate: page.candidates[0],
    });
    expect(dedupeAndSortMunicipalPermits([record, record])).toHaveLength(1);
    expect(renderMunicipalPermitJsonl([record, record]).trim()).toBe(
      JSON.stringify(record),
    );
    expect(() =>
      dedupeAndSortMunicipalPermits([
        record,
        { ...record, record_status: "Issued" },
      ]),
    ).toThrow("Conflicting municipal permit records");
  });

  it("accepts one local query and rejects login-required North Lauderdale", () => {
    expect(
      parseOptions([
        "--jurisdiction",
        "pembroke_pines",
        "--folio",
        "513914101320",
        "--output-dir",
        "downloads/broward/pembroke-permit-probe",
        "--max-pages",
        "2",
        "--max-details",
        "4",
      ]),
    ).toEqual({
      jurisdictionKey: "pembroke_pines",
      query: { kind: "folio", value: "513914101320" },
      outputDirectory: "downloads/broward/pembroke-permit-probe",
      maxPages: 2,
      maxDetails: 4,
      searchDelayMs: 1500,
      detailDelayMs: 500,
      roofOnly: false,
    });
    expect(
      parseOptions([
        "--jurisdiction",
        "pembroke_pines",
        "--folio",
        "513914101320",
        "--output-dir",
        "downloads/broward/pembroke-roof-probe",
        "--roof-only",
      ])?.roofOnly,
    ).toBe(true);
    expect(() =>
      parseOptions([
        "--jurisdiction",
        "north_lauderdale",
        "--address",
        "701 SW 71 AVENUE",
        "--output-dir",
        "downloads/broward/north-lauderdale",
      ]),
    ).toThrow("requires login");
    expect(() =>
      parseOptions([
        "--jurisdiction",
        "miramar",
        "--folio",
        "514123070029",
        "--address",
        "PEMBROKE ROAD",
        "--output-dir",
        "downloads/broward/miramar",
      ]),
    ).toThrow("Exactly one");
  });
});
