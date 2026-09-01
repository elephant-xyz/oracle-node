import { readFile } from "node:fs/promises";

import { describe, expect, it, vi } from "vitest";

import {
  createMunicipalCheckpoint,
  decideMunicipalSourceAccess,
  dedupeAndSortMunicipalPermits,
  normalizeMunicipalFolio,
  preserveMunicipalParcelIdentifier,
  renderMunicipalPermitJsonl,
  runBoundedMunicipalCapture,
  validateMunicipalCheckpoint,
  validateMunicipalQueries,
} from "../../scripts/permit-source-adapters/broward-municipal-core.mjs";
import {
  BROWARD_MUNICIPAL_PERMIT_JURISDICTIONS,
  getBrowardMunicipalPermitConfig,
} from "../../scripts/permit-source-adapters/broward-municipal-config.mjs";
import {
  normalizeOpenGovDetailPayload,
  parseClick2GovDetailHtml,
  parseClick2GovSearchHtml,
  parseCoconutCreekDetailHtml,
  parseCoconutCreekSearchHtml,
  parseEgovPlusDetailHtml,
  parseEgovPlusSearchHtml,
  parseOpenGovSearchPayload,
  parseSmartGovDetailHtml,
  parseSmartGovSearchHtml,
  parseTylerEsuiteDetailHtml,
  parseTylerEsuiteSearchHtml,
} from "../../scripts/permit-source-adapters/broward-municipal-protocols.mjs";
import {
  buildClick2GovSearchBody,
  buildCoconutCreekSearchBody,
  buildEgovPlusSearchBody,
  buildSmartGovSearchBody,
  parseMunicipalStreetAddress,
  probeBoundedBrowardMunicipalPermits,
} from "../../scripts/permit-source-adapters/broward-municipal-transport.mjs";
import { parseBrowardMunicipalPilotOptions } from "../../scripts/run-broward-municipal-permit-pilot.mjs";

const FIXTURE_ROOT = new URL(
  "../fixtures/broward-municipal-permits/",
  import.meta.url,
);

const [
  coconutCreekSearch,
  coconutCreekDetail,
  clickSearch,
  clickDetail,
  esuiteSearch,
  esuiteDetail,
  smartGovSearch,
  smartGovDetail,
  egovPlusSearch,
  egovPlusDetail,
  openGovSearch,
  openGovDetail,
] = await Promise.all(
  [
    "coconut-creek-search.html",
    "coconut-creek-detail.html",
    "click2gov-search.html",
    "click2gov-detail.html",
    "esuite-search.html",
    "esuite-detail.html",
    "smartgov-search.html",
    "smartgov-detail.html",
    "egovplus-search.html",
    "egovplus-detail.html",
    "opengov-search.json",
    "opengov-detail.json",
  ].map((name) => readFile(new URL(name, FIXTURE_ROOT), "utf8")),
);

/**
 * Build one fully shaped normalized record for bounded-runner tests.
 *
 * @param {import("../../scripts/permit-source-adapters/broward-municipal-core.mjs").BrowardMunicipalJurisdictionConfig} config - Source configuration.
 * @param {import("../../scripts/permit-source-adapters/broward-municipal-core.mjs").BrowardMunicipalSearchReference} reference - Search identity.
 * @returns {import("../../scripts/permit-source-adapters/broward-municipal-core.mjs").NormalizedBrowardMunicipalPermit} Deterministic fixture record.
 */
function buildNormalizedRecord(config, reference) {
  return {
    source_system: config.sourceSystem,
    source_protocol: config.protocol,
    source_url: reference.detailUrl,
    source_search_url: config.searchUrl,
    source_record_id: reference.sourceRecordId,
    record_key: `${config.sourceSystem}:${reference.sourceRecordId}`,
    jurisdiction: config.jurisdiction,
    permit_number: reference.permitNumber,
    parcel_identifier: "504108BJ0140",
    query_folio: null,
    work_location: "100 SAMPLE BLVD",
    application_date: null,
    permit_issue_date: null,
    expiration_date: null,
    record_status: "Issued",
    record_type: "Roof",
    project_description: "RE-ROOF",
    job_value: null,
    inspections: [],
    is_roof_permit: true,
    raw: {
      source_page: reference.sourcePage,
      query_kind: "permit_number",
    },
  };
}

/**
 * Build a stable search reference for bounded-runner tests.
 *
 * @param {string} id - Source identity suffix.
 * @param {number} page - One-based source page.
 * @returns {import("../../scripts/permit-source-adapters/broward-municipal-core.mjs").BrowardMunicipalSearchReference} Search reference.
 */
function reference(id, page) {
  return {
    sourceRecordId: id,
    permitNumber: `PERMIT-${id}`,
    detailUrl: `https://c2g.pompanobeachfl.gov/Click2GovBP/detail/${id}`,
    sourcePage: page,
    listData: { source_page: page },
  };
}

describe("Broward municipal permit jurisdiction routing", () => {
  it("covers every requested jurisdiction and reusable vendor family", () => {
    expect(
      BROWARD_MUNICIPAL_PERMIT_JURISDICTIONS.map(
        (config) => config.jurisdiction,
      ),
    ).toEqual([
      "Coconut Creek",
      "Pompano Beach",
      "Tamarac",
      "Margate",
      "Davie",
      "Dania Beach",
      "Deerfield Beach",
      "Pembroke Park",
      "Lighthouse Point",
      "Lauderdale Lakes",
      "Hillsboro Beach",
      "Parkland",
      "Lauderhill",
      "Sunrise",
    ]);
    expect(
      new Set(
        BROWARD_MUNICIPAL_PERMIT_JURISDICTIONS.map((config) => config.protocol),
      ),
    ).toEqual(
      new Set([
        "coconut_creek",
        "click2gov",
        "tyler_esuite",
        "tyler_energov",
        "gov_easy",
        "smartgov",
        "opengov",
        "communitycore",
        "mgo_connect",
        "egovplus",
      ]),
    );
  });

  it("records split-system completeness and official records-request routes", () => {
    const deerfield = getBrowardMunicipalPermitConfig("deerfield_beach");
    const sunrise = getBrowardMunicipalPermitConfig("sunrise");
    const davie = getBrowardMunicipalPermitConfig("davie");
    const coconutCreek = getBrowardMunicipalPermitConfig("coconut_creek");

    expect(deerfield.supplementalRoutes).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          purpose: "current_applicant_portal_from_2025",
          accessMode: "no_anonymous_search",
          url: "https://deerfieldbeach.geocivix.com/secure/",
        }),
      ]),
    );
    expect(davie.supplementalRoutes[0]).toMatchObject({
      purpose: "new_2026_submissions",
      accessMode: "login_required",
    });
    expect(coconutCreek).toMatchObject({
      protocol: "coconut_creek",
      accessMode: "anonymous",
      probeStatus: "enabled",
      capabilities: {
        searchBy: ["permit_number", "address", "folio"],
        pagination: "client_all",
      },
    });
    expect(sunrise).toMatchObject({
      sourceSystem: "broward_sunrise_tyler_permits",
      protocol: "tyler_energov",
      accessMode: "anonymous",
      probeStatus: "enabled",
      capabilities: {
        searchBy: ["permit_number", "address", "folio"],
        pagination: "numbered",
      },
    });
    expect(sunrise.supplementalRoutes.map((route) => route.purpose)).toEqual([
      "official_building_records_request_form",
      "city_clerk_public_records_custodian",
    ]);
  });

  it("makes login, CAPTCHA, temporary availability, and records-request skips explicit", () => {
    expect(
      decideMunicipalSourceAccess(
        getBrowardMunicipalPermitConfig("hillsboro_beach"),
      ).reason,
    ).toBe("captcha_required");
    expect(
      decideMunicipalSourceAccess(getBrowardMunicipalPermitConfig("parkland"))
        .reason,
    ).toBe("login_required");
    expect(
      decideMunicipalSourceAccess(
        getBrowardMunicipalPermitConfig("deerfield_beach"),
      ).reason,
    ).toBe("captcha_required");
    expect(
      decideMunicipalSourceAccess(
        getBrowardMunicipalPermitConfig("pembroke_park"),
      ).reason,
    ).toBe("captcha_required");
    expect(
      decideMunicipalSourceAccess(
        getBrowardMunicipalPermitConfig("lauderdale_lakes"),
      ).reason,
    ).toBe("landing_only");
    expect(
      decideMunicipalSourceAccess(getBrowardMunicipalPermitConfig("sunrise"))
        .reason,
    ).toBe("anonymous_certified");
  });
});

describe("Broward municipal folio and query safety", () => {
  it("preserves letters and leading zeroes without numeric coercion", () => {
    expect(normalizeMunicipalFolio(" 504108-bj-0140 ")).toBe("504108BJ0140");
    expect(() => normalizeMunicipalFolio(5041080140)).toThrow(
      "supplied as a string",
    );
    expect(() => normalizeMunicipalFolio("504108_BJ0140")).toThrow(
      "exactly 12 alphanumeric",
    );
    expect(preserveMunicipalParcelIdentifier("  9202-06-1T100HALL  ")).toBe(
      "9202-06-1T100HALL",
    );
  });

  it("bounds and deduplicates exact source queries", () => {
    expect(
      validateMunicipalQueries([
        { kind: "folio", value: "504108bj0140" },
        { kind: "permit_number", value: "  BLD-001  " },
      ]),
    ).toEqual([
      { kind: "folio", value: "504108BJ0140" },
      { kind: "permit_number", value: "BLD-001" },
    ]);
    expect(() =>
      validateMunicipalQueries([
        { kind: "address", value: "100 Sample Blvd" },
        { kind: "address", value: "100 SAMPLE BLVD" },
      ]),
    ).toThrow("must be unique");
  });
});

describe("Broward municipal live transport form contracts", () => {
  it("parses bounded addresses and populates exactly one legacy search mode", () => {
    expect(parseMunicipalStreetAddress("100 NE SAMPLE BLVD")).toEqual({
      houseNumber: "100",
      direction: "NE",
      streetName: "SAMPLE",
      suffix: "BLVD",
    });
    expect(() => parseMunicipalStreetAddress("SAMPLE ADDRESS")).toThrow(
      "house number",
    );

    const coconutBody = buildCoconutCreekSearchBody({
      kind: "folio",
      value: "484205AB0010",
    });
    expect(Object.fromEntries(coconutBody)).toMatchObject({
      permit_no: "",
      parcel_id: "484205AB0010",
      house_num: "",
      street: "",
    });
    const egovBody = buildEgovPlusSearchBody({
      kind: "address",
      value: "5581 W OAKLAND PARK BLVD",
    });
    expect(Object.fromEntries(egovBody)).toMatchObject({
      permit_no: "",
      parcel_id: "",
      house_num: "5581",
      street: "W OAKLAND PARK BLVD",
    });
  });

  it("keeps Click2Gov parcel segmentation disabled and SmartGov people fields blank", () => {
    const clickLanding = `<!doctype html><form>
      <input name="validatePermitView" value="true">
      <input name="searchType" value="0">
      <input name="OWASP_CSRFTOKEN" value="private-token">
    </form>`;
    const clickConfig = getBrowardMunicipalPermitConfig("pompano_beach");
    const click = buildClick2GovSearchBody(clickLanding, clickConfig, {
      kind: "permit_number",
      value: "26-00001234",
    });
    expect(click.body.get("permit.appYear")).toBe("26");
    expect(click.body.get("permit.appNumber")).toBe("1234");
    expect(() =>
      buildClick2GovSearchBody(clickLanding, clickConfig, {
        kind: "folio",
        value: "484205AB0010",
      }),
    ).toThrow("mapping is not certified");

    const smartBody = buildSmartGovSearchBody(
      '<input name="_conv" value="private-conversation">',
      { kind: "folio", value: "484205AB0010" },
    );
    expect(smartBody.get("PrimaryParcel.Parcel.ParcelNumber")).toBe(
      "484205AB0010",
    );
    expect(smartBody.get("PrimaryContact.Contact.DisplayName")).toBe("");
    expect(smartBody.get("PrimaryContractor.Contact.DisplayName")).toBe("");
  });

  it("parses a local-only pilot without exposing query values in fixed metadata", () => {
    const options = parseBrowardMunicipalPilotOptions([
      "--jurisdiction",
      "lauderhill",
      "--folio",
      "494123AB0020",
      "--output-dir",
      "downloads/private",
      "--max-details",
      "2",
    ]);
    expect(options).toMatchObject({
      jurisdictionKey: "lauderhill",
      query: { kind: "folio", value: "494123AB0020" },
      limits: { maxQueries: 1, maxDetailPages: 2 },
      requestTimeoutMs: 30_000,
    });
    expect(() =>
      parseBrowardMunicipalPilotOptions([
        "--jurisdiction",
        "pompano_beach",
        "--folio",
        "484205AB0010",
        "--output-dir",
        "downloads/private",
      ]),
    ).toThrow("does not support");
  });
});

describe("Coconut Creek legacy permit-status protocol", () => {
  it("selects one session detail and omits owner/payment fields", () => {
    const config = getBrowardMunicipalPermitConfig("coconut_creek");
    const page = parseCoconutCreekSearchHtml(coconutCreekSearch, config);
    expect(page.references).toHaveLength(1);
    const sourceReference = page.references[0];
    expect(sourceReference).toMatchObject({
      sourceRecordId: "26001234",
      permitNumber: "26001234",
      listData: {
        record_status: "Issued",
        record_type: "Roof",
      },
    });
    expect(sourceReference).toBeDefined();
    const record = parseCoconutCreekDetailHtml(coconutCreekDetail, {
      config,
      reference: sourceReference,
      query: { kind: "folio", value: "484205AB0010" },
    });
    expect(record).toMatchObject({
      source_protocol: "coconut_creek",
      permit_number: "26001234",
      parcel_identifier: "484205AB0010",
      query_folio: "484205AB0010",
      record_status: "Issued",
      record_type: "Roof",
      is_roof_permit: true,
    });
    expect(JSON.stringify(record)).not.toMatch(/PRIVATE FIXTURE/iu);
  });
});

describe("Click2Gov protocol", () => {
  it("deduplicates contact-expanded rows and strips session tokens", () => {
    const config = getBrowardMunicipalPermitConfig("pompano_beach");
    const page = parseClick2GovSearchHtml(clickSearch, config);

    expect(page.references).toHaveLength(1);
    expect(page.references[0]).toMatchObject({
      sourceRecordId: "26-00001234",
      permitNumber: "26-00001234",
      listData: {
        address: "100 SAMPLE BLVD",
        record_type: "B-*ROOF RE-ROOF",
        record_status: "APPROVED",
      },
    });
    expect(page.references[0]?.detailUrl).not.toContain("OWASP_CSRFTOKEN");
    expect(() =>
      parseClick2GovSearchHtml(clickSearch, config, { maxRows: 1 }),
    ).toThrow("result row limit");
  });

  it("captures bounded detail provenance while excluding owner/contact fields", () => {
    const config = getBrowardMunicipalPermitConfig("pompano_beach");
    const sourcePage = parseClick2GovSearchHtml(clickSearch, config);
    const sourceReference = sourcePage.references[0];
    expect(sourceReference).toBeDefined();

    const record = parseClick2GovDetailHtml(clickDetail, {
      config,
      reference: sourceReference,
      query: { kind: "folio", value: "504108BJ0140" },
    });

    expect(record).toMatchObject({
      source_protocol: "click2gov",
      permit_number: "26-00001234",
      parcel_identifier: "504108BJ0140",
      query_folio: "504108BJ0140",
      application_date: "2026-08-20",
      permit_issue_date: "2026-08-22",
      record_status: "APPROVED",
      record_type: "B-*ROOF RE-ROOF",
      job_value: 8500,
      is_roof_permit: true,
      inspections: [
        {
          source_id: "I-10",
          inspection_type: "ROOF FINAL",
          completed_date: "2026-08-25",
          result: "Passed",
        },
      ],
    });
    expect(JSON.stringify(record)).not.toMatch(
      /PRIVATE FIXTURE OWNER|PRIVATE INSPECTOR|555-0100/,
    );
  });
});

describe("anonymous municipal transport orchestration", () => {
  it("maintains Click2Gov cookies and persists a record before checkpoint advancement", async () => {
    const landing = `<!doctype html><html><body><form action="selectpermit.html" method="post">
      <input name="validatePermitView" value="true">
      <input name="searchType" value="0">
      <input name="OWASP_CSRFTOKEN" value="private-token-1">
    </form></body></html>`;
    const requests = [];
    const fetchImpl = vi.fn(async (url, init = {}) => {
      const parsedUrl = new URL(String(url));
      const method = init.method ?? "GET";
      requests.push({
        method,
        path: parsedUrl.pathname,
        hasCookie: new Headers(init.headers).has("cookie"),
      });
      if (method === "GET" && requests.length === 1) {
        return new Response(landing, {
          status: 200,
          headers: { "Set-Cookie": "JSESSIONID=private-session-1; Path=/" },
        });
      }
      if (method === "POST") {
        return new Response(clickSearch, {
          status: 200,
          headers: { "Set-Cookie": "JSESSIONID=private-session-2; Path=/" },
        });
      }
      return new Response(clickDetail, { status: 200 });
    });
    const durableEvents = [];
    const result = await probeBoundedBrowardMunicipalPermits({
      config: getBrowardMunicipalPermitConfig("pompano_beach"),
      queries: [{ kind: "permit_number", value: "26-00001234" }],
      limits: {
        maxQueries: 1,
        maxSearchPages: 1,
        maxResults: 1,
        maxDetailPages: 1,
        delayMs: 1_000,
      },
      dependencies: { fetchImpl },
      wait: async () => {},
      onRecord: async (record) => {
        durableEvents.push(`record:${record.record_key}`);
      },
      onCheckpoint: async (checkpoint) => {
        durableEvents.push(
          `checkpoint:${String(checkpoint.capturedRecordKeys.length)}`,
        );
      },
    });

    expect(result.status).toBe("completed");
    expect(result.records).toHaveLength(1);
    expect(requests).toEqual([
      {
        method: "GET",
        path: "/Click2GovBP/selectpermit.html",
        hasCookie: false,
      },
      {
        method: "POST",
        path: "/Click2GovBP/selectpermit.html",
        hasCookie: true,
      },
      {
        method: "GET",
        path: "/Click2GovBP/selectpermit.html",
        hasCookie: true,
      },
    ]);
    expect(durableEvents[0]).toMatch(/^record:/u);
    expect(durableEvents[1]).toBe("checkpoint:1");
    expect(durableEvents.at(-1)).toBe("checkpoint:1");
  });
});

describe("Tyler eSuite protocol", () => {
  it("deduplicates responsive links and retains numbered pagination", () => {
    const config = getBrowardMunicipalPermitConfig("davie");
    const page = parseTylerEsuiteSearchHtml(esuiteSearch, config);

    expect(page.nextPage).toBe(2);
    expect(page.references).toHaveLength(1);
    expect(page.references[0]).toMatchObject({
      sourceRecordId: "400068",
      permitNumber: "2026-00004503",
      listData: {
        address: "8800 SW 36 ST BLD A",
        record_status: "Permit Issued",
        record_type: "E-Fire Alarm",
      },
    });
  });

  it("normalizes same-session details and bounded inspection outcomes", () => {
    const config = getBrowardMunicipalPermitConfig("davie");
    const sourceReference = parseTylerEsuiteSearchHtml(esuiteSearch, config)
      .references[0];
    expect(sourceReference).toBeDefined();

    const record = parseTylerEsuiteDetailHtml(esuiteDetail, {
      config,
      reference: sourceReference,
      query: { kind: "address", value: "8800 SW 36 ST BLD A" },
    });

    expect(record).toMatchObject({
      source_protocol: "tyler_esuite",
      source_record_id: "400068",
      permit_number: "2026-00004503",
      parcel_identifier: "504108BJ0140",
      permit_issue_date: "2026-06-23",
      expiration_date: "2027-02-09",
      record_status: "Permit Issued",
      job_value: 300,
      inspections: [
        {
          source_id: "1678209",
          inspection_type: "E4070 Fire Alarm Rough",
          completed_date: "2026-08-13",
          status: "Completed",
          result: "Pass",
        },
      ],
    });
    expect(JSON.stringify(record)).not.toMatch(
      /PRIVATE FIXTURE OWNER|private-fixture|555-0111|PRIVATE FIXTURE COMMENT/,
    );
  });
});

describe("SmartGov and eGovPLUS HTML protocols", () => {
  it("captures SmartGov numbered pages and alphanumeric source parcels", () => {
    const config = getBrowardMunicipalPermitConfig("lighthouse_point");
    const page = parseSmartGovSearchHtml(smartGovSearch, config);
    expect(page.nextPage).toBe(2);
    const sourceReference = page.references[0];
    expect(sourceReference).toBeDefined();

    const record = parseSmartGovDetailHtml(smartGovDetail, {
      config,
      reference: sourceReference,
      query: { kind: "permit_number", value: "BLD26-0012" },
    });

    expect(record).toMatchObject({
      source_protocol: "smartgov",
      source_record_id: "APP-7788",
      permit_number: "BLD26-0012",
      parcel_identifier: "4842-28-AB-0010",
      application_date: "2026-08-01",
      permit_issue_date: "2026-08-10",
      is_roof_permit: true,
    });
    expect(JSON.stringify(record)).not.toContain("PRIVATE FIXTURE CONTACT");
  });

  it("normalizes eGovPLUS details without owner, reviewer, or inspector data", () => {
    const config = getBrowardMunicipalPermitConfig("lauderhill");
    const page = parseEgovPlusSearchHtml(egovPlusSearch, config);
    const sourceReference = page.references[0];
    expect(sourceReference).toBeDefined();

    const record = parseEgovPlusDetailHtml(egovPlusDetail, {
      config,
      reference: sourceReference,
      query: { kind: "folio", value: "494123AB0020" },
    });

    expect(record).toMatchObject({
      source_protocol: "egovplus",
      permit_number: "26020017",
      parcel_identifier: "494123AB0020",
      query_folio: "494123AB0020",
      application_date: "2026-02-02",
      permit_issue_date: "2026-02-17",
      record_status: "Open",
      record_type: "TNC",
      project_description: "TENT-COMM-EVENT",
      job_value: 1200,
      inspections: [
        {
          source_id: "1",
          inspection_type: "BUILDING FINAL",
          completed_date: "2026-02-20",
          result: "Passed",
        },
      ],
    });
    expect(JSON.stringify(record)).not.toMatch(
      /PRIVATE FIXTURE OWNER|PRIVATE FIXTURE INSPECTOR|555-0123/,
    );
  });
});

describe("OpenGov fixture-only cursor prototype", () => {
  it("parses public GraphQL cursor provenance without enabling a live probe", () => {
    const config = getBrowardMunicipalPermitConfig("lauderdale_lakes");
    const page = parseOpenGovSearchPayload(JSON.parse(openGovSearch), config, {
      sourcePage: 1,
    });
    expect(page.nextPage).toBe("fixture-cursor-2");
    expect(page.references[0]).toMatchObject({
      sourceRecordId: "record-100",
      permitNumber: "LL-2026-100",
    });

    const sourceReference = page.references[0];
    expect(sourceReference).toBeDefined();
    const record = normalizeOpenGovDetailPayload(JSON.parse(openGovDetail), {
      config,
      reference: sourceReference,
      query: { kind: "address", value: "4300 NW 36TH ST" },
    });
    expect(record).toMatchObject({
      source_protocol: "opengov",
      parcel_identifier: "494219AB0010",
      record_status: "Issued",
      record_type: "Roof",
      job_value: 12500,
      is_roof_permit: true,
    });
    expect(decideMunicipalSourceAccess(config).reason).toBe("landing_only");
  });
});

describe("reusable bounded capture and checkpoints", () => {
  it("serializes pages/details, deduplicates overlap, and checkpoints after each unit", async () => {
    const config = getBrowardMunicipalPermitConfig("pompano_beach");
    const queries = [{ kind: "permit_number", value: "26-00001234" }];
    const first = reference("1", 1);
    const second = reference("2", 1);
    const duplicateSecond = reference("2", 2);
    const third = reference("3", 2);
    const waits = vi.fn(async () => {});
    const checkpoints = [];

    const result = await runBoundedMunicipalCapture({
      config,
      queries,
      limits: {
        maxQueries: 1,
        maxSearchPages: 2,
        maxResults: 3,
        maxDetailPages: 3,
        delayMs: 1000,
      },
      fetchSearchPage: vi.fn(async (_query, page) =>
        page === 1
          ? { references: [first, second], nextPage: 2 }
          : { references: [duplicateSecond, third], nextPage: null },
      ),
      fetchDetail: vi.fn(async (sourceReference) =>
        buildNormalizedRecord(config, sourceReference),
      ),
      onCheckpoint: async (checkpoint) => {
        checkpoints.push(checkpoint);
      },
      wait: waits,
    });

    expect(result.status).toBe("completed");
    expect(result.searchPageCount).toBe(2);
    expect(result.detailPageCount).toBe(3);
    expect(result.records.map((record) => record.source_record_id)).toEqual([
      "1",
      "2",
      "3",
    ]);
    expect(result.checkpoint).toMatchObject({
      nextQueryIndex: 1,
      nextPage: 1,
      completed: true,
      seenReferenceKeys: [
        "broward_pompano_beach_click2gov_permits:1",
        "broward_pompano_beach_click2gov_permits:2",
        "broward_pompano_beach_click2gov_permits:3",
      ],
      capturedRecordKeys: [
        "broward_pompano_beach_click2gov_permits:1",
        "broward_pompano_beach_click2gov_permits:2",
        "broward_pompano_beach_click2gov_permits:3",
      ],
    });
    expect(checkpoints).toHaveLength(5);
    expect(waits).toHaveBeenCalledTimes(4);
  });

  it("resumes a partially captured page without refetching completed details", async () => {
    const config = getBrowardMunicipalPermitConfig("pompano_beach");
    const queries = validateMunicipalQueries([
      { kind: "permit_number", value: "26-00001234" },
    ]);
    const first = reference("1", 1);
    const second = reference("2", 1);
    const initial = createMunicipalCheckpoint(config, queries);
    const partial = {
      ...initial,
      seenReferenceKeys: ["broward_pompano_beach_click2gov_permits:1"],
      capturedRecordKeys: ["broward_pompano_beach_click2gov_permits:1"],
    };
    const details = vi.fn(async (sourceReference) =>
      buildNormalizedRecord(config, sourceReference),
    );

    const result = await runBoundedMunicipalCapture({
      config,
      queries,
      checkpoint: partial,
      limits: {
        maxQueries: 1,
        maxSearchPages: 1,
        maxResults: 2,
        maxDetailPages: 2,
        delayMs: 1000,
      },
      fetchSearchPage: async () => ({
        references: [first, second],
        nextPage: null,
      }),
      fetchDetail: details,
      wait: async () => {},
    });

    expect(details).toHaveBeenCalledTimes(1);
    expect(details).toHaveBeenCalledWith(second, queries[0]);
    expect(result.records.map((record) => record.source_record_id)).toEqual([
      "2",
    ]);
    expect(result.checkpoint?.completed).toBe(true);
  });

  it("validates checkpoint lineage and refuses access-controlled transports", async () => {
    const config = getBrowardMunicipalPermitConfig("pompano_beach");
    const queries = validateMunicipalQueries([
      { kind: "permit_number", value: "26-00001234" },
    ]);
    const checkpoint = createMunicipalCheckpoint(config, queries);
    expect(validateMunicipalCheckpoint(checkpoint, config, queries)).toEqual(
      checkpoint,
    );
    expect(() =>
      validateMunicipalCheckpoint(checkpoint, config, [
        { kind: "permit_number", value: "DIFFERENT" },
      ]),
    ).toThrow("malformed or mismatched");

    const search = vi.fn();
    const detail = vi.fn();
    const skipped = await runBoundedMunicipalCapture({
      config: getBrowardMunicipalPermitConfig("deerfield_beach"),
      queries: [],
      fetchSearchPage: search,
      fetchDetail: detail,
    });
    expect(skipped.status).toBe("skipped");
    expect(skipped.access.reason).toBe("captcha_required");
    const hillsboroSkipped = await runBoundedMunicipalCapture({
      config: getBrowardMunicipalPermitConfig("hillsboro_beach"),
      queries: [],
      fetchSearchPage: search,
      fetchDetail: detail,
    });
    expect(hillsboroSkipped.status).toBe("skipped");
    expect(hillsboroSkipped.access.reason).toBe("captcha_required");
    expect(search).not.toHaveBeenCalled();
    expect(detail).not.toHaveBeenCalled();
  });

  it("rejects conflicting records and renders deterministic JSONL", () => {
    const config = getBrowardMunicipalPermitConfig("pompano_beach");
    const first = buildNormalizedRecord(config, reference("1", 1));
    const second = buildNormalizedRecord(config, reference("2", 1));

    expect(
      dedupeAndSortMunicipalPermits([second, first, first]).map(
        (record) => record.source_record_id,
      ),
    ).toEqual(["1", "2"]);
    expect(
      renderMunicipalPermitJsonl([second, first, first]).split("\n"),
    ).toHaveLength(3);
    expect(() =>
      dedupeAndSortMunicipalPermits([
        first,
        { ...first, record_status: "Different" },
      ]),
    ).toThrow("Conflicting Broward municipal records");
  });
});
