import {
  mkdtemp,
  readFile,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { afterEach, describe, expect, it } from "vitest";

import {
  BROWARD_ACCELA_SOURCES,
  BrowardAccelaSourceError,
  buildBrowardAccelaPermitStem,
  buildBrowardAccelaSearchKey,
  classifyBrowardAccelaPage,
  cleanBrowardAccelaRecordStatus,
  countBrowardAccelaExcludedModuleLinks,
  createBrowardAccelaCheckpoint,
  extractBrowardAccelaDirectDetailLink,
  extractBrowardAccelaPermitDetail,
  extractBrowardAccelaPermitLinks,
  normalizeBrowardPermitFolio,
  parseBrowardAccelaMoreDetails,
  readBrowardAccelaCheckpoint,
  readBrowardAccelaSource,
  writeBrowardAccelaCheckpoint,
} from "../../scripts/permit-source-adapters/broward-accela.mjs";
import {
  parseOptions,
  renderBrowardAccelaPermitJsonl,
} from "../../scripts/probe-broward-accela-permits.mjs";

const fixtureDirectory = new URL("../fixtures/broward-accela/", import.meta.url);
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
    expect(BROWARD_ACCELA_SOURCES.plantation.contentFrameName).toBe(
      "ACAFrame",
    );
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
    expect(
      BROWARD_ACCELA_SOURCES.plantation.historicalCutoff,
    ).toMatchObject({
      date: "2004-01-01",
      disposition: "official_microfilm_route",
    });
    expect(readBrowardAccelaSource("fort-lauderdale").agencyCode).toBe("FTL");
    expect(() => readBrowardAccelaSource("lee")).toThrow(
      "Unknown Broward Accela jurisdiction",
    );
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
    expect(
      countBrowardAccelaExcludedModuleLinks({ html, source }),
    ).toBe(1);
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
    expect(classifyBrowardAccelaPage("<html><body>Loading...</body></html>")).toBe(
      "unknown",
    );
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
      idempotencyKey:
        "broward_hollywood_accela_permits:permit:BLD24-12345",
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
    expect(
      resumed.targets["plantation:parcel:504108BJ0140"],
    ).toMatchObject({
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
    });
    expect(() =>
      parseOptions([
        "--target=hollywood:514111160200",
        "--target=hollywood:514207022070",
        "--target=hollywood:504108BJ0140",
      ]),
    ).toThrow("exceeds the approved maximum");
    expect(() => parseOptions([])).toThrow("exactly one input mode");
    expect(() =>
      parseOptions(["--pilot", "--detail-delay-ms=249"]),
    ).toThrow("between 250");
  });
});
