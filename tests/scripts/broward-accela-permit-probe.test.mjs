import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { afterEach, describe, expect, it } from "vitest";

import {
  BROWARD_ACCELA_SOURCES,
  BrowardAccelaSourceError,
  buildBrowardAccelaDateWindowKey,
  buildBrowardAccelaDetailUrlFromRecordId,
  buildBrowardAccelaPermitStem,
  buildBrowardAccelaSearchKey,
  classifyBrowardAccelaPage,
  cleanBrowardAccelaRecordStatus,
  countBrowardAccelaExcludedModuleLinks,
  createBrowardAccelaCheckpoint,
  extractBrowardAccelaDirectDetailLink,
  extractBrowardAccelaPermitDetail,
  extractBrowardAccelaPermitLinks,
  isBrowardAccelaRoofPermitCandidate,
  normalizeBrowardPermitFolio,
  parseBrowardAccelaCsvExport,
  parseBrowardAccelaCsvExportSummary,
  parseBrowardAccelaMoreDetails,
  reconcileBrowardAccelaListPages,
  readBrowardAccelaCheckpoint,
  readBrowardAccelaSource,
  writeBrowardAccelaCheckpoint,
} from "../../scripts/permit-source-adapters/broward-accela.mjs";
import {
  parseOptions,
  renderBrowardAccelaPermitJsonl,
} from "../../scripts/probe-broward-accela-permits.mjs";
import {
  createBrowardAccelaDateWindows,
  parseBrowardAccelaDateWindowOptions,
  runBrowardAccelaDateWindows,
  splitBrowardAccelaDateWindow,
} from "../../scripts/run-broward-accela-date-windows.mjs";
import {
  createAccelaCsvDateWindows,
  parseAccelaCsvWindowOptions,
  retryAccelaCsvBackoffMs,
  runAccelaCsvWindows,
  splitAccelaCsvDateWindow,
} from "../../scripts/run-broward-accela-csv-windows.mjs";

const fixtureDirectory = new URL(
  "../fixtures/broward-accela/",
  import.meta.url,
);
const [pageOneHtml, pageTwoHtml, noRecordsHtml, sourceErrorHtml, detailHtml] =
  await Promise.all(
    [
      "search-records-page-1.html",
      "search-records-page-2.html",
      "search-no-records.html",
      "source-error.html",
      "permit-detail.html",
    ].map((name) => readFile(new URL(name, fixtureDirectory), "utf8")),
  );

/** @type {string[]} */
const temporaryDirectories = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

describe("Broward jurisdiction-specific Accela adapters", () => {
  it("uses independent agency/module/source identities and explicit historical boundaries", () => {
    expect(Object.keys(BROWARD_ACCELA_SOURCES)).toEqual([
      "hollywood",
      "plantation",
      "fort-lauderdale",
      "cooper-city",
      "weston",
    ]);
    expect(
      Object.values(BROWARD_ACCELA_SOURCES).map((source) => source.agencyCode),
    ).toEqual(["HOLLYWOOD", "PLANTATION", "FTL", "COOPER", "WESTON"]);
    expect(
      new Set(
        Object.values(BROWARD_ACCELA_SOURCES).map(
          (source) => source.sourceSystem,
        ),
      ).size,
    ).toBe(5);
    expect(BROWARD_ACCELA_SOURCES.hollywood.module).toBe("Building");
    expect(BROWARD_ACCELA_SOURCES.plantation.contentFrameName).toBe("ACAFrame");
    expect(BROWARD_ACCELA_SOURCES["fort-lauderdale"].module).toBe("Permits");
    expect(
      BROWARD_ACCELA_SOURCES.hollywood.separateHistoricalSource,
    ).toMatchObject({
      sourceSystem: "broward_hollywood_bcla_legacy_permits",
      searchMethod: "address",
      coverageStartDate: "1988-01-01",
    });
    expect(
      BROWARD_ACCELA_SOURCES.hollywood.separateHistoricalSource?.sourceSystem,
    ).not.toBe(BROWARD_ACCELA_SOURCES.hollywood.sourceSystem);
    expect(BROWARD_ACCELA_SOURCES.weston.historicalCutoff).toMatchObject({
      date: "1997-01-01",
      disposition: "outside_city_record_coverage",
    });
    expect(BROWARD_ACCELA_SOURCES.plantation.historicalCutoff).toMatchObject({
      date: "2004-01-01",
      disposition: "official_microfilm_route",
    });
    expect(readBrowardAccelaSource("fort-lauderdale").agencyCode).toBe("FTL");
    expect(() => readBrowardAccelaSource("lee")).toThrow(
      "Unknown Broward Accela jurisdiction",
    );
  });

  it("classifies roofing from Accela result-list fields before detail", () => {
    const base = {
      recordNumber: "B24-00001",
      url: "https://example.test/permit",
      address: null,
      description: null,
      status: "Active",
      recordType: "Building",
      sourceSearchKey: "test",
      sourcePage: 1,
    };
    expect(
      isBrowardAccelaRoofPermitCandidate({
        ...base,
        recordNumber: "STRUC-ROOF-25-000925",
      }),
    ).toBe(true);
    expect(
      isBrowardAccelaRoofPermitCandidate({
        ...base,
        description: "Re-roof existing residence",
      }),
    ).toBe(true);
    expect(isBrowardAccelaRoofPermitCandidate(base)).toBe(false);
  });

  it("preserves exact 12-character alphanumeric Broward folios without Lee STRAP normalization", () => {
    expect(normalizeBrowardPermitFolio("504108bj0140")).toBe("504108BJ0140");
    expect(normalizeBrowardPermitFolio("504108-BJ-0140")).toBe("504108BJ0140");
    expect(normalizeBrowardPermitFolio("514111160200")).toBe("514111160200");
    expect(() => normalizeBrowardPermitFolio(514111160200)).toThrow(
      "must be supplied as a string",
    );
    expect(() => normalizeBrowardPermitFolio("514111-160200")).toThrow(
      "exactly 12 alphanumeric",
    );
    expect(
      buildBrowardAccelaSearchKey(
        BROWARD_ACCELA_SOURCES.hollywood,
        "514111160200",
      ),
    ).toBe("hollywood:parcel:514111160200");
  });

  it("parses standard Accela result pages with jurisdiction-relative links", () => {
    const source = BROWARD_ACCELA_SOURCES.hollywood;
    const firstPage = extractBrowardAccelaPermitLinks({
      html: pageOneHtml,
      source,
      searchKey: "hollywood:parcel:514111160200",
      pageNumber: 1,
    });
    const secondPage = extractBrowardAccelaPermitLinks({
      html: pageTwoHtml,
      source,
      searchKey: "hollywood:parcel:514111160200",
      pageNumber: 2,
    });

    expect(firstPage).toHaveLength(2);
    expect(firstPage[0]).toMatchObject({
      recordNumber: "BLD24-12345",
      address: "100 TEST AVE HOLLYWOOD FL 33020",
      description: "WINDOW REPLACEMENT",
      status: "Issued",
      recordType: "Window Door Shutter Permit",
      sourcePage: 1,
    });
    expect(firstPage[0]?.url).toBe(
      "https://aca-prod.accela.com/HOLLYWOOD/Cap/CapDetail.aspx?Module=Building&capID1=24BLD&capID2=00000&capID3=001AA&agencyCode=HOLLYWOOD",
    );
    expect(secondPage).toHaveLength(1);
    expect(secondPage[0]).toMatchObject({
      recordNumber: "MEC23-00991",
      sourcePage: 2,
    });
    expect(buildBrowardAccelaPermitStem(firstPage[0])).toMatch(
      /^bld24-12345-[a-f0-9]{12}$/,
    );
  });

  it("reads Record No. after a date column and excludes cross-module enforcement records", () => {
    const html = `
      <div>Showing 1-2 of 2 records found.</div>
      <table id="ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList">
        <thead><tr>
          <th></th><th>Date</th><th>Record No.</th><th>Record Type</th>
          <th>Sub Type</th><th>Description</th><th>Address</th><th>Status</th>
        </tr></thead>
        <tbody>
          <tr>
            <td></td><td>11/22/2017</td>
            <td><a href="/CitizenAccess/Cap/CapDetail.aspx?Module=Building&capID1=17BLD">B17-04514</a></td>
            <td>Building Project - AAC</td><td>Alteration</td><td>Kitchen</td>
            <td>958 MOCKINGBIRD LN</td><td>Closed</td>
          </tr>
          <tr>
            <td></td><td>06/10/2022</td>
            <td><a href="/CitizenAccess/Cap/CapDetail.aspx?Module=Enforcement&capID1=22ENF">CE22-01397</a></td>
            <td>Building Enforcement</td><td></td><td>Observed work</td>
            <td>958 MOCKINGBIRD LN</td><td>Complied</td>
          </tr>
        </tbody>
      </table>`;
    const source = BROWARD_ACCELA_SOURCES.plantation;
    expect(
      extractBrowardAccelaPermitLinks({
        html,
        source,
        searchKey: "plantation:parcel:504108BJ0140",
        pageNumber: 1,
      }),
    ).toMatchObject([
      {
        recordNumber: "B17-04514",
        recordType: "Building Project - AAC",
        description: "Kitchen",
        address: "958 MOCKINGBIRD LN",
        status: "Closed",
      },
    ]);
    expect(countBrowardAccelaExcludedModuleLinks({ html, source })).toBe(1);
  });

  it("preserves reported temporary rows that have a hidden RecordId but no detail anchor", () => {
    const html = `
      <div>Showing 1-1 of 1</div>
      <table id="ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList">
        <thead><tr>
          <th></th><th>Date</th><th>Record Number</th><th>Record Type</th>
          <th>Project Name</th><th>Address</th><th>Expiration Date</th>
          <th>Status</th>
        </tr></thead>
        <tbody><tr>
          <td></td><td>08/31/2026</td>
          <td><span id="row_lblPermitNumber">26TMP-017930</span>
            <input type="hidden" id="RecordId" value="26EST-00000-17950">
          </td>
          <td>Commercial Mechanical Permit</td><td>Temporary intake</td>
          <td>2700 HOLLYWOOD BLVD</td><td></td><td></td>
        </tr></tbody>
      </table>`;
    const source = BROWARD_ACCELA_SOURCES.hollywood;
    expect(
      buildBrowardAccelaDetailUrlFromRecordId("26EST-00000-17950", source),
    ).toContain("capID3=17950");
    expect(
      extractBrowardAccelaPermitLinks({
        html,
        source,
        searchKey: "hollywood:date:20260830_20260831",
        pageNumber: 1,
      }),
    ).toMatchObject([
      {
        recordNumber: "26TMP-017930",
        recordType: "Commercial Mechanical Permit",
        description: "Temporary intake",
        address: "2700 HOLLYWOOD BLVD",
        sourcePage: 1,
      },
    ]);
  });

  it("distinguishes explicit no records from source errors and unknown pages", () => {
    expect(classifyBrowardAccelaPage(noRecordsHtml)).toBe("no_records");
    expect(classifyBrowardAccelaPage(sourceErrorHtml)).toBe("source_error");
    expect(classifyBrowardAccelaPage(pageOneHtml)).toBe("records");
    expect(
      classifyBrowardAccelaPage(
        detailHtml.replace(
          "</form>",
          "<div>Related Records: No records found.</div></form>",
        ),
      ),
    ).toBe("records");
    expect(
      classifyBrowardAccelaPage("<html><body>Loading...</body></html>"),
    ).toBe("unknown");
  });

  it("maps Broward value aliases and keeps expiration metadata out of permit status", () => {
    expect(
      parseBrowardAccelaMoreDetails(
        "Contract Value: 24,000 Commercial / Residential: Residential Total Square Feet: 2200 Parcel Number: 494212072320 *",
      ),
    ).toMatchObject({
      "Contract Value": "24,000",
      "Estimated Job Value": "24,000",
      "Commercial / Residential": "Residential",
      "Comm/Res": "Residential",
      "Total Square Feet": "2200",
      "Estimated Sq. Ft.": "2200",
    });
    expect(
      cleanBrowardAccelaRecordStatus(
        "Issued Expiration Date: 08/28/2027 Add to Existing Collection",
      ),
    ).toBe("Issued");
  });

  it("normalizes detail data into the existing Accela permit record shape with provenance", () => {
    const source = BROWARD_ACCELA_SOURCES.hollywood;
    const [permit] = extractBrowardAccelaPermitLinks({
      html: pageOneHtml,
      source,
      searchKey: "hollywood:parcel:514111160200",
      pageNumber: 1,
    });
    expect(permit).toBeDefined();
    const record = extractBrowardAccelaPermitDetail({
      html: detailHtml,
      sourceUrl: permit.url,
      source,
      parcelIdentifier: "514111160200",
      permit,
      retrievedAt: "2026-08-29T00:00:00.000Z",
    });

    expect(record).toMatchObject({
      schemaVersion: "permit-harvest.accela.v1",
      source: "broward_hollywood_accela_permits",
      sourceSystem: "broward_hollywood_accela_permits",
      jurisdiction: "Hollywood",
      retrievedAt: "2026-08-29T00:00:00.000Z",
      recordNumber: "BLD24-12345",
      recordType: "Window Door Shutter Permit",
      recordStatus: "Issued",
      workLocation: "100 TEST AVE HOLLYWOOD FL 33020",
      parcelIdentifier: "514111160200",
      sourceParcelIdentifier: "514111160200",
      applicant: "TEST APPLICANT",
      licensedProfessional: "SAMPLE WINDOWS LLC",
      projectDescription: "REPLACE FOUR WINDOWS",
      moreDetails: {
        Type: "Residential",
        "Estimated Job Value": "12500",
        "Parcel Number": "514111160200",
      },
      completedInspections: [
        {
          result: "Pass",
          inspectionCode: "101",
          inspectionType: "Building Final",
          inspectionIdentifier: "778899",
          inspectorName: "Public Inspector",
          resultedDate: "08/20/2024",
        },
      ],
      idempotencyKey: "broward_hollywood_accela_permits:permit:BLD24-12345",
      provenance: {
        searchMethod: "public_anonymous_parcel",
        anonymous: true,
        submittedParcelIdentifier: "514111160200",
        agencyCode: "HOLLYWOOD",
        module: "Building",
        resultPage: 1,
      },
    });
    expect(record.documentLinks).toEqual([
      {
        text: "Public permit document",
        url: "https://aca-prod.accela.com/HOLLYWOOD/urlrouting.ashx?type=document&id=fixture",
        title: null,
      },
    ]);
  });

  it("fails closed when detail identity or source origin differs", () => {
    const source = BROWARD_ACCELA_SOURCES.hollywood;
    const [permit] = extractBrowardAccelaPermitLinks({
      html: pageOneHtml,
      source,
      searchKey: "hollywood:parcel:514111160200",
      pageNumber: 1,
    });
    expect(permit).toBeDefined();

    expect(() =>
      extractBrowardAccelaPermitDetail({
        html: detailHtml.replace("514111160200", "503912010490"),
        sourceUrl: permit.url,
        source,
        parcelIdentifier: "514111160200",
        permit,
      }),
    ).toThrow(BrowardAccelaSourceError);
    expect(() =>
      extractBrowardAccelaPermitDetail({
        html: detailHtml.replace("BLD24-12345", "BLD24-99999"),
        sourceUrl: permit.url,
        source,
        parcelIdentifier: "514111160200",
        permit,
      }),
    ).toThrow("detail identity differs");
    expect(() =>
      extractBrowardAccelaPermitDetail({
        html: detailHtml,
        sourceUrl: "https://example.invalid/detail",
        source,
        parcelIdentifier: "514111160200",
        permit,
      }),
    ).toThrow("escaped configured source origin");
  });

  it("recognizes direct detail redirects without including related-record links", () => {
    const direct = extractBrowardAccelaDirectDetailLink({
      html: detailHtml,
      pageUrl:
        "https://aca-prod.accela.com/HOLLYWOOD/Cap/CapDetail.aspx?Module=Building&capID1=24BLD&capID2=00000&capID3=001AA&agencyCode=HOLLYWOOD",
      source: BROWARD_ACCELA_SOURCES.hollywood,
      searchKey: "hollywood:parcel:514111160200",
      pageNumber: 1,
    });
    expect(direct).toMatchObject({
      recordNumber: "BLD24-12345",
      recordType: "Window Door Shutter Permit",
      sourcePage: 1,
    });
  });

  it("writes and resumes a validated atomic mode-0600 checkpoint", async () => {
    const directory = await mkdtemp(join(tmpdir(), "broward-accela-test-"));
    temporaryDirectories.push(directory);
    const checkpointPath = join(directory, "nested", "checkpoint.json");
    const missing = await readBrowardAccelaCheckpoint(checkpointPath);
    expect(missing).toMatchObject({
      schemaVersion: "broward-accela-local-checkpoint.v1",
      targets: {},
    });

    const checkpoint = createBrowardAccelaCheckpoint(
      "2026-08-29T00:00:00.000Z",
    );
    checkpoint.targets["plantation:parcel:504108BJ0140"] = {
      status: "no_records",
      jurisdictionKey: "plantation",
      parcelIdentifier: "504108BJ0140",
      searchKey: "plantation:parcel:504108BJ0140",
      startedAt: "2026-08-29T00:00:00.000Z",
      completedAt: "2026-08-29T00:01:00.000Z",
      reportedTotal: 0,
      excludedNonPermitCount: 0,
      permits: [],
      details: {},
      searchCapturePaths: ["private/page-001.html"],
      error: null,
    };
    await writeBrowardAccelaCheckpoint(checkpointPath, checkpoint);

    const resumed = await readBrowardAccelaCheckpoint(checkpointPath);
    expect(resumed.targets["plantation:parcel:504108BJ0140"]).toMatchObject({
      status: "no_records",
      parcelIdentifier: "504108BJ0140",
    });
    expect((await stat(checkpointPath)).mode & 0o777).toBe(0o600);
    await writeFile(checkpointPath, '{"schemaVersion":"foreign"}', "utf8");
    await expect(readBrowardAccelaCheckpoint(checkpointPath)).rejects.toThrow(
      "Invalid Broward Accela checkpoint schema",
    );
  });

  it("renders deterministic records and rejects conflicting duplicates", () => {
    const source = BROWARD_ACCELA_SOURCES.hollywood;
    const [permit] = extractBrowardAccelaPermitLinks({
      html: pageOneHtml,
      source,
      searchKey: "hollywood:parcel:514111160200",
      pageNumber: 1,
    });
    const record = extractBrowardAccelaPermitDetail({
      html: detailHtml,
      sourceUrl: permit.url,
      source,
      parcelIdentifier: "514111160200",
      permit,
      retrievedAt: "2026-08-29T00:00:00.000Z",
    });
    expect(renderBrowardAccelaPermitJsonl([record, record]).trim()).toBe(
      JSON.stringify(record),
    );
    expect(() =>
      renderBrowardAccelaPermitJsonl([
        record,
        { ...record, recordStatus: "Different" },
      ]),
    ).toThrow("Conflicting Broward Accela record");
  });

  it("limits the CLI to curated or at-most-two explicit targets per jurisdiction", () => {
    const pilot = parseOptions(["--pilot"]);
    expect(pilot?.targets).toEqual([
      { jurisdictionKey: "hollywood", parcelIdentifier: "514111160200" },
      { jurisdictionKey: "plantation", parcelIdentifier: "504108BJ0140" },
      {
        jurisdictionKey: "fort-lauderdale",
        parcelIdentifier: "494209060010",
      },
      { jurisdictionKey: "cooper-city", parcelIdentifier: "514106100100" },
      { jurisdictionKey: "weston", parcelIdentifier: "503912010490" },
    ]);
    const explicit = parseOptions([
      "--target",
      "hollywood:514111160200",
      "--target=hollywood:514207022070",
      "--max-pages=3",
      "--max-details",
      "4",
    ]);
    expect(explicit).toMatchObject({
      isCuratedPilot: false,
      maxPages: 3,
      maxDetails: 4,
      roofOnly: false,
    });
    expect(parseOptions(["--pilot", "--roof-only"])?.roofOnly).toBe(true);
    expect(() =>
      parseOptions([
        "--target=hollywood:514111160200",
        "--target=hollywood:514207022070",
        "--target=hollywood:504108BJ0140",
      ]),
    ).toThrow("exceeds the approved maximum");
    expect(() => parseOptions([])).toThrow("exactly one input mode");
    expect(() => parseOptions(["--pilot", "--detail-delay-ms=249"])).toThrow(
      "between 250",
    );
  });
});

describe("Broward Accela vendor-wide date windows", () => {
  it("builds exact non-overlapping windows and stable source keys", () => {
    expect(
      createBrowardAccelaDateWindows("2026-08-28", "2026-08-31", 2),
    ).toEqual([
      { startDate: "2026-08-28", endDate: "2026-08-29" },
      { startDate: "2026-08-30", endDate: "2026-08-31" },
    ]);
    expect(
      splitBrowardAccelaDateWindow({
        startDate: "2026-08-28",
        endDate: "2026-08-31",
      }),
    ).toEqual([
      { startDate: "2026-08-28", endDate: "2026-08-29" },
      { startDate: "2026-08-30", endDate: "2026-08-31" },
    ]);
    expect(
      buildBrowardAccelaDateWindowKey(
        BROWARD_ACCELA_SOURCES.hollywood,
        "2026-08-30",
        "2026-08-31",
      ),
    ).toBe("hollywood:date:20260830_20260831");
    expect(() =>
      splitBrowardAccelaDateWindow({
        startDate: "2026-08-31",
        endDate: "2026-08-31",
      }),
    ).toThrow(/cannot be split/u);
  });

  it("requires explicit dates and a certified date-enabled source", () => {
    expect(
      parseBrowardAccelaDateWindowOptions([
        "--source",
        "weston",
        "--start-date",
        "1997-01-01",
        "--end-date",
        "2026-08-31",
        "--window-days",
        "30",
        "--max-windows",
        "2",
      ]),
    ).toMatchObject({
      sourceKey: "weston",
      startDate: "1997-01-01",
      endDate: "2026-08-31",
      initialWindowDays: 30,
      maxWindows: 2,
    });
    expect(() =>
      parseBrowardAccelaDateWindowOptions([
        "--source",
        "fort-lauderdale",
        "--start-date",
        "2026-01-01",
        "--end-date",
        "2026-01-02",
      ]),
    ).toThrow(/hollywood, plantation, cooper-city, or weston/u);
  });

  it("checkpoints a bounded invocation and resumes the same persistent source run", async () => {
    const outputDirectory = await mkdtemp(
      join(tmpdir(), "broward-accela-date-window-"),
    );
    temporaryDirectories.push(outputDirectory);
    let browserCreations = 0;
    const closedBrowsers = [];
    const searchedWindows = [];
    const createBrowser = async () => {
      browserCreations += 1;
      return {
        close: async () => {
          closedBrowsers.push(browserCreations);
        },
      };
    };
    const searchWindow = async ({ source, startDate, endDate }) => {
      searchedWindows.push(`${startDate}:${endDate}`);
      const recordNumber = `B${startDate.replaceAll("-", "")}`;
      return {
        status: "records",
        searchKey: `${source.key}:${startDate}:${endDate}`,
        startDate,
        endDate,
        source,
        permits: [
          {
            recordNumber,
            url: `https://aca-prod.accela.com/HOLLYWOOD/Cap/CapDetail.aspx?Module=Building&capID1=${recordNumber}`,
            address: "100 TEST AVE",
            description: "Building permit",
            status: "Issued",
            recordType: "Building",
            sourceSearchKey: `${source.key}:${startDate}:${endDate}`,
            sourcePage: 1,
          },
        ],
        pages: [
          {
            pageNumber: 1,
            url: source.portalUrl,
            resultSummary: "1-1 of 1",
            html: "<html><body>fixture</body></html>",
          },
        ],
        reportedTotal: 1,
        excludedNonPermitCount: 0,
        truncatedForSplit: false,
      };
    };
    const baseOptions = {
      sourceKey: "hollywood",
      startDate: "2026-08-28",
      endDate: "2026-08-31",
      initialWindowDays: 2,
      splitThreshold: 100,
      maxPages: 200,
      delayMs: 1_000,
      maxWindows: 1,
      outputDirectory,
    };
    const first = await runBrowardAccelaDateWindows(baseOptions, {
      createBrowser,
      searchWindow,
      wait: async () => undefined,
      now: () => "2026-08-31T18:00:00.000Z",
    });
    expect(first).toMatchObject({
      status: "paused",
      windowsProcessedThisInvocation: 1,
      terminalWindowCount: 1,
      pendingWindowCount: 1,
      uniquePermitCount: 1,
    });
    const resumed = await runBrowardAccelaDateWindows(
      { ...baseOptions, maxWindows: null },
      {
        createBrowser,
        searchWindow,
        wait: async () => undefined,
        now: () => "2026-08-31T18:01:00.000Z",
      },
    );
    expect(resumed).toMatchObject({
      status: "complete",
      windowsProcessedThisInvocation: 1,
      terminalWindowCount: 2,
      pendingWindowCount: 0,
      uniquePermitCount: 2,
    });
    expect(searchedWindows).toEqual([
      "2026-08-28:2026-08-29",
      "2026-08-30:2026-08-31",
    ]);
    expect(browserCreations).toBe(2);
    expect(closedBrowsers).toHaveLength(2);
    expect(
      (
        await readFile(
          join(outputDirectory, "normalized-list.private.jsonl"),
          "utf8",
        )
      )
        .trim()
        .split("\n"),
    ).toHaveLength(2);
  });

  it("splits an incomplete multi-day source window instead of accepting partial rows", async () => {
    const outputDirectory = await mkdtemp(
      join(tmpdir(), "broward-accela-incomplete-window-"),
    );
    temporaryDirectories.push(outputDirectory);
    const summary = await runBrowardAccelaDateWindows(
      {
        sourceKey: "weston",
        startDate: "2005-11-17",
        endDate: "2005-11-18",
        initialWindowDays: 2,
        splitThreshold: 100,
        maxPages: 200,
        delayMs: 1_000,
        maxWindows: 1,
        outputDirectory,
      },
      {
        createBrowser: async () => ({
          close: async () => undefined,
        }),
        searchWindow: async ({ source }) => {
          throw new BrowardAccelaSourceError(
            "incomplete_pagination",
            source,
            "Source stopped before reported total",
          );
        },
        wait: async () => undefined,
        now: () => "2026-08-31T19:00:00.000Z",
      },
    );
    expect(summary).toMatchObject({
      status: "paused",
      windowsProcessedThisInvocation: 1,
      terminalWindowCount: 0,
      splitWindowCount: 1,
      pendingWindowCount: 2,
      uniquePermitCount: 0,
    });
    const checkpoint = JSON.parse(
      await readFile(join(outputDirectory, "checkpoint.private.json"), "utf8"),
    );
    expect(checkpoint.pendingWindows).toEqual([
      { startDate: "2005-11-17", endDate: "2005-11-17" },
      { startDate: "2005-11-18", endDate: "2005-11-18" },
    ]);
  });
});

describe("Broward official Accela CSV exports", () => {
  it("accepts list-only mode only after exact page and identity reconciliation", () => {
    const source = BROWARD_ACCELA_SOURCES.hollywood;
    const sourceWindowKey = buildBrowardAccelaDateWindowKey(
      source,
      "2026-08-30",
      "2026-08-31",
    );
    const reconciled = reconcileBrowardAccelaListPages({
      htmlPages: [pageOneHtml, pageTwoHtml],
      source,
      sourceWindowKey,
      displayedTotal: 3,
    });
    expect(reconciled).toMatchObject({
      sourceRowCount: 3,
      excludedNonPermitCount: 0,
      duplicateRecordCount: 0,
    });
    expect(reconciled.records).toHaveLength(3);
    expect(() =>
      reconcileBrowardAccelaListPages({
        htmlPages: [pageOneHtml],
        source,
        sourceWindowKey,
        displayedTotal: 3,
      }),
    ).toThrow("accounted for 2 of 3");
    expect(() =>
      reconcileBrowardAccelaListPages({
        htmlPages: [pageOneHtml, pageTwoHtml],
        source,
        sourceWindowKey,
        displayedTotal: 100,
      }),
    ).toThrow("capped at 100");
  });

  it("parses full exported record numbers and stable detail-compatible keys", () => {
    const records = parseBrowardAccelaCsvExport(
      [
        '"Date","Record Number","Record Type","Project Name","Address","Expiration Date","Status",',
        '"01/16/2025","STRUC-ROOF-25-000185","Roofing Permit",,"6751 Harding St","09/02/2025","Closed - Complete",',
        '"01/16/2025","RES-ELEC-25-000065","Residential Electrical Permit",,"4524 Jackson St","11/17/2025","Closed - Complete",',
      ].join("\n"),
      BROWARD_ACCELA_SOURCES.hollywood,
      "2025-01-16",
      "2025-01-16",
    );
    expect(records).toHaveLength(2);
    expect(records[1]).toMatchObject({
      recordNumber: "STRUC-ROOF-25-000185",
      recordKey: "broward_hollywood_accela_permits:permit:STRUC-ROOF-25-000185",
      recordDate: "2025-01-16",
      expirationDate: "2025-09-02",
      isRoofPermit: true,
      sourceWindowKey: "hollywood:date:20250116_20250116",
    });
    expect(records[1]?.sourceUrl).toContain("altId=STRUC-ROOF-25-000185");
  });

  it("accepts Plantation Record No. and accounts for explicit non-permit modules", () => {
    const summary = parseBrowardAccelaCsvExportSummary(
      [
        '"Date","Record No.","Record Type","Sub Type","Description","Address","Status",',
        '"08/31/2026","CE26-02231","Code Enforcement",,"Bulk trash","100 TEST AVE","Investigate",',
        '"08/31/2026","B26-03481","Building Permit","Misc","Kitchen","200 TEST AVE","Applied",',
      ].join("\n"),
      BROWARD_ACCELA_SOURCES.plantation,
      "2026-08-31",
      "2026-08-31",
    );
    expect(summary).toMatchObject({
      sourceRowCount: 2,
      excludedNonPermitCount: 1,
      records: [
        expect.objectContaining({
          recordNumber: "B26-03481",
          recordType: "Building Permit",
        }),
      ],
    });
  });

  it("splits CSV windows deterministically and bounds jittered retry backoff", () => {
    expect(
      splitAccelaCsvDateWindow({
        startDate: "2026-08-29",
        endDate: "2026-08-31",
      }),
    ).toEqual([
      { startDate: "2026-08-29", endDate: "2026-08-30" },
      { startDate: "2026-08-31", endDate: "2026-08-31" },
    ]);
    expect(retryAccelaCsvBackoffMs(1_000, 1, () => 0.5)).toBe(5_625);
    expect(retryAccelaCsvBackoffMs(1_000, 2, () => 0.5)).toBe(11_250);
    expect(() => retryAccelaCsvBackoffMs(1_000, 1, () => 1)).toThrow(
      "must be in [0,1)",
    );
  });

  it("durably splits a capped multi-day Plantation parent without completing it", async () => {
    const outputDirectory = await mkdtemp(
      join(tmpdir(), "broward-accela-capped-date-split-"),
    );
    temporaryDirectories.push(outputDirectory);
    const summary = await runAccelaCsvWindows(
      {
        sourceKey: "plantation",
        startDate: "2026-08-30",
        endDate: "2026-08-31",
        windowDays: 2,
        delayMs: 1_000,
        maxAttempts: 2,
        maxPages: 200,
        windowTimeoutMs: 120_000,
        maxWindows: 1,
        outputDirectory,
      },
      {
        createBrowser: async () => ({
          close: async () => undefined,
        }),
        captureWindow: async ({
          source,
          startDate,
          endDate,
          recordTypeShard,
        }) => ({
          startDate,
          endDate,
          sourceWindowKey: `${source.key}:${startDate}:${endDate}`,
          displayedTotal: 100,
          displayedTotalCapped: true,
          captureMode: "capped_probe",
          records: [],
          sourceRowCount: 0,
          excludedNonPermitCount: 0,
          duplicateRecordCount: 0,
          pageCount: 1,
          availableRecordTypes: [
            {
              value: "Building/Building Permit/NA/NA",
              label: "Building Permit",
            },
          ],
          recordTypeShard,
          rawCsv: "",
          rawSearchHtml: "<html>capped</html>",
          rawListPages: ["<html>capped</html>"],
        }),
        wait: async () => undefined,
        random: () => 0,
        now: () => "2026-09-01T18:00:00.000Z",
      },
    );
    expect(summary).toMatchObject({
      status: "paused",
      completedWindowCount: 0,
      pendingWindowCount: 2,
    });
    const checkpoint = JSON.parse(
      await readFile(join(outputDirectory, "checkpoint.private.json"), "utf8"),
    );
    expect(checkpoint.pendingWindows).toEqual([
      { startDate: "2026-08-30", endDate: "2026-08-30" },
      { startDate: "2026-08-31", endDate: "2026-08-31" },
    ]);
    expect(checkpoint.completedWindows).toEqual({});
    expect(checkpoint.splitWindows["20260830_20260831"]).toMatchObject({
      reason: "plantation_displayed_total_cap",
    });
  });

  it("splits an unreconciled Weston list-only window instead of recording zero", async () => {
    const outputDirectory = await mkdtemp(
      join(tmpdir(), "broward-accela-weston-list-split-"),
    );
    temporaryDirectories.push(outputDirectory);
    const summary = await runAccelaCsvWindows(
      {
        sourceKey: "weston",
        startDate: "2006-05-14",
        endDate: "2006-05-15",
        windowDays: 2,
        delayMs: 1_000,
        maxAttempts: 1,
        maxPages: 200,
        windowTimeoutMs: 120_000,
        maxWindows: 1,
        outputDirectory,
      },
      {
        createBrowser: async () => ({
          close: async () => undefined,
        }),
        captureWindow: async ({ source }) => {
          throw new BrowardAccelaSourceError(
            "incomplete_pagination",
            source,
            "Displayed total could not be reconciled",
          );
        },
        wait: async () => undefined,
        random: () => 0,
        now: () => "2026-09-01T18:00:00.000Z",
      },
    );
    expect(summary).toMatchObject({
      status: "paused",
      completedWindowCount: 0,
      pendingWindowCount: 2,
      uniquePermitCount: 0,
    });
    const checkpoint = JSON.parse(
      await readFile(join(outputDirectory, "checkpoint.private.json"), "utf8"),
    );
    expect(checkpoint.completedWindows).toEqual({});
    expect(checkpoint.splitWindows["20060514_20060515"]).toMatchObject({
      reason: "unreconciled_source_result",
    });
  });

  it("plans exhaustive record-type shards for capped one-day tenant lists", async () => {
    const outputDirectory = await mkdtemp(
      join(tmpdir(), "broward-accela-tenant-type-shards-"),
    );
    temporaryDirectories.push(outputDirectory);
    const calls = [];
    const dependencies = {
      createBrowser: async () => ({
        close: async () => undefined,
      }),
      captureWindow: async ({
        source,
        startDate,
        endDate,
        recordTypeShard,
        stopAtCappedProbe,
      }) => {
        calls.push({
          sourceKey: source.key,
          recordTypeShard,
          stopAtCappedProbe,
        });
        return {
          startDate,
          endDate,
          sourceWindowKey: `${source.key}:date:${startDate}:${endDate}`,
          displayedTotal: 100,
          displayedTotalCapped: true,
          captureMode: "capped_probe",
          records: [],
          sourceRowCount: 0,
          excludedNonPermitCount: 0,
          duplicateRecordCount: 0,
          pageCount: 1,
          availableRecordTypes: [
            {
              value: "Building/Building Permit/NA/NA",
              label: "Building Permit",
            },
            {
              value: "Building/Electrical Permit/NA/NA",
              label: "Electrical Permit",
            },
          ],
          recordTypeShard,
          rawCsv: "",
          rawSearchHtml: "<html>capped</html>",
          rawListPages: ["<html>capped</html>"],
        };
      },
      wait: async () => undefined,
      random: () => 0,
      now: () => "2026-09-01T20:00:00.000Z",
    };
    for (const sourceKey of ["weston", "cooper-city"]) {
      const sourceOutputDirectory = join(outputDirectory, sourceKey);
      const summary = await runAccelaCsvWindows(
        {
          sourceKey,
          startDate: "2006-04-22",
          endDate: "2006-04-22",
          windowDays: 1,
          delayMs: 1_000,
          maxAttempts: 1,
          maxPages: 200,
          windowTimeoutMs: 120_000,
          maxWindows: 1,
          outputDirectory: sourceOutputDirectory,
        },
        dependencies,
      );
      expect(summary).toMatchObject({
        status: "paused",
        completedWindowCount: 0,
        pendingWindowCount: 1,
      });
      const checkpoint = JSON.parse(
        await readFile(
          join(sourceOutputDirectory, "checkpoint.private.json"),
          "utf8",
        ),
      );
      expect(
        checkpoint.shardPlans["20060422_20060422"].expectedShards,
      ).toHaveLength(2);
      expect(checkpoint.completedWindows).toEqual({});
    }
    expect(calls).toEqual([
      {
        sourceKey: "weston",
        recordTypeShard: null,
        stopAtCappedProbe: true,
      },
      {
        sourceKey: "cooper-city",
        recordTypeShard: null,
        stopAtCappedProbe: true,
      },
    ]);
  });

  it("checkpoints and resumes exhaustive Plantation record-type shards", async () => {
    const outputDirectory = await mkdtemp(
      join(tmpdir(), "broward-accela-record-type-shards-"),
    );
    temporaryDirectories.push(outputDirectory);
    const options = {
      sourceKey: "plantation",
      startDate: "2026-08-31",
      endDate: "2026-08-31",
      windowDays: 1,
      delayMs: 1_000,
      maxAttempts: 2,
      maxPages: 200,
      windowTimeoutMs: 120_000,
      maxWindows: 1,
      outputDirectory,
    };
    const publicOptions = [
      {
        value: "Building/Building Permit/NA/NA",
        label: "Building Permit",
      },
      {
        value: "Building/Electrical Permit/NA/NA",
        label: "Electrical Permit",
      },
    ];
    /** @type {string[]} */
    const operations = [];
    /** @type {number[]} */
    const searchOutcomeTimeouts = [];
    let buildingFailuresRemaining = 2;
    const captureWindow = async ({
      source,
      startDate,
      endDate,
      recordTypeShard,
      searchOutcomeTimeoutMs,
    }) => {
      const operation =
        recordTypeShard === null ? "parent" : recordTypeShard.label;
      operations.push(operation);
      searchOutcomeTimeouts.push(searchOutcomeTimeoutMs);
      if (
        recordTypeShard?.label === "Building Permit" &&
        buildingFailuresRemaining > 0
      ) {
        buildingFailuresRemaining -= 1;
        throw new Error("Waiting failed: 90000ms exceeded");
      }
      const label = recordTypeShard?.label ?? "Building Permit";
      const suffix =
        recordTypeShard === null
          ? "PARENT"
          : label.toUpperCase().replaceAll(" ", "-");
      const record = {
        schemaVersion: "oracle-node.broward-accela-csv-list.v1",
        sourceSystem: source.sourceSystem,
        jurisdiction: source.jurisdiction,
        recordNumber: `B26-${suffix}`,
        sourceUrl: `${source.portalUrl}&altId=B26-${suffix}`,
        recordKey: `${source.sourceSystem}:permit:B26-${suffix}`,
        recordDate: startDate,
        recordType:
          recordTypeShard === null ? label : `${label} public subtype`,
        projectName: null,
        address: null,
        expirationDate: null,
        status: "Issued",
        isRoofPermit: false,
        sourceWindowKey: `${source.key}:date:${startDate}:${endDate}`,
      };
      return {
        startDate,
        endDate,
        sourceWindowKey: record.sourceWindowKey,
        displayedTotal: 100,
        displayedTotalCapped: true,
        captureMode: "csv_export",
        records: [record],
        sourceRowCount: 1,
        excludedNonPermitCount: 0,
        duplicateRecordCount: 0,
        pageCount: 1,
        availableRecordTypes: publicOptions,
        recordTypeShard,
        rawCsv: `fixture-${operation}`,
        rawSearchHtml: "<html>fixture</html>",
        rawListPages: ["<html>fixture</html>"],
      };
    };
    const dependencies = {
      createBrowser: async () => ({
        close: async () => undefined,
      }),
      captureWindow,
      wait: async () => undefined,
      random: () => 0,
      now: () => "2026-09-01T18:00:00.000Z",
    };
    const planned = await runAccelaCsvWindows(options, dependencies);
    expect(planned).toMatchObject({
      status: "paused",
      completedWindowCount: 0,
      pendingWindowCount: 1,
    });
    let checkpoint = JSON.parse(
      await readFile(join(outputDirectory, "checkpoint.private.json"), "utf8"),
    );
    expect(Object.values(checkpoint.shardPlans)[0]).toMatchObject({
      dimension: "record_type",
      expectedShards: expect.arrayContaining([
        expect.objectContaining({ label: "Building Permit" }),
        expect.objectContaining({ label: "Electrical Permit" }),
      ]),
      completedShards: {},
    });

    await runAccelaCsvWindows({ ...options, maxWindows: 2 }, dependencies);
    checkpoint = JSON.parse(
      await readFile(join(outputDirectory, "checkpoint.private.json"), "utf8"),
    );
    expect(
      Object.keys(Object.values(checkpoint.shardPlans)[0].completedShards),
    ).toHaveLength(1);
    expect(Object.values(checkpoint.shardPlans)[0].failedShards).toEqual(
      expect.objectContaining({
        "record-type-25d182bab06ba7fa": expect.objectContaining({
          reason: "timeout",
          attemptCycles: 1,
        }),
      }),
    );

    const completed = await runAccelaCsvWindows(
      { ...options, maxWindows: null },
      dependencies,
    );
    expect(completed).toMatchObject({
      status: "complete",
      completedWindowCount: 1,
      pendingWindowCount: 0,
      uniquePermitCount: 2,
    });
    expect(operations).toEqual([
      "parent",
      "Building Permit",
      "Building Permit",
      "Electrical Permit",
      "Building Permit",
    ]);
    expect(searchOutcomeTimeouts).toEqual([
      60_000, 90_000, 90_000, 90_000, 90_000,
    ]);
    checkpoint = JSON.parse(
      await readFile(join(outputDirectory, "checkpoint.private.json"), "utf8"),
    );
    expect(checkpoint.shardPlans).toEqual({});
    expect(checkpoint.completedWindows["20260831_20260831"]).toMatchObject({
      captureMode: "record_type_shards",
      recordCount: 2,
      sourceRowCount: 2,
    });
  });

  it("creates deterministic CSV windows and checkpointed inventory", async () => {
    const outputDirectory = await mkdtemp(
      join(tmpdir(), "broward-accela-csv-window-"),
    );
    temporaryDirectories.push(outputDirectory);
    expect(createAccelaCsvDateWindows("2025-01-01", "2025-01-03", 2)).toEqual([
      { startDate: "2025-01-01", endDate: "2025-01-02" },
      { startDate: "2025-01-03", endDate: "2025-01-03" },
    ]);
    expect(
      parseAccelaCsvWindowOptions([
        "--source",
        "hollywood",
        "--start-date",
        "2025-01-01",
        "--end-date",
        "2025-01-03",
        "--max-windows",
        "1",
      ]),
    ).toMatchObject({
      sourceKey: "hollywood",
      windowDays: 30,
      maxAttempts: 3,
      maxPages: 200,
      windowTimeoutMs: 120_000,
      maxWindows: 1,
    });
    const options = {
      sourceKey: "hollywood",
      startDate: "2025-01-01",
      endDate: "2025-01-03",
      windowDays: 2,
      delayMs: 1_000,
      maxAttempts: 3,
      maxPages: 200,
      windowTimeoutMs: 120_000,
      maxWindows: null,
      outputDirectory,
    };
    let captureAttempts = 0;
    let browserCreations = 0;
    let browserCloses = 0;
    const summary = await runAccelaCsvWindows(options, {
      createBrowser: async () => {
        browserCreations += 1;
        return {
          close: async () => {
            browserCloses += 1;
          },
        };
      },
      captureWindow: async ({
        source,
        startDate,
        endDate,
        recordTypeShard,
      }) => {
        captureAttempts += 1;
        if (captureAttempts === 1) {
          throw new Error("transient export failure");
        }
        const [record] = parseBrowardAccelaCsvExport(
          [
            '"Date","Record Number","Record Type","Project Name","Address","Expiration Date","Status",',
            `"01/01/2025","B-${startDate.replaceAll("-", "")}","Building Permit",,"100 TEST AVE",,"Open",`,
          ].join("\n"),
          source,
          startDate,
          endDate,
        );
        return {
          startDate,
          endDate,
          sourceWindowKey: record.sourceWindowKey,
          displayedTotal: 100,
          displayedTotalCapped: true,
          captureMode: "csv_export",
          records: [record],
          sourceRowCount: 1,
          excludedNonPermitCount: 0,
          duplicateRecordCount: 0,
          pageCount: 1,
          availableRecordTypes: [],
          recordTypeShard,
          rawCsv: "fixture",
          rawSearchHtml: "<html>fixture</html>",
          rawListPages: ["<html>fixture</html>"],
        };
      },
      wait: async () => undefined,
      random: () => 0,
      now: () => "2026-08-31T19:00:00.000Z",
    });
    expect(summary).toMatchObject({
      status: "complete",
      completedWindowCount: 2,
      uniquePermitCount: 2,
      cappedDisplayedTotalWindowCount: 2,
    });
    expect(captureAttempts).toBe(3);
    expect(browserCreations).toBe(2);
    expect(browserCloses).toBe(2);
    expect(
      (
        await readFile(
          join(outputDirectory, "normalized-list.private.jsonl"),
          "utf8",
        )
      )
        .trim()
        .split("\n"),
    ).toHaveLength(2);
  });
});
