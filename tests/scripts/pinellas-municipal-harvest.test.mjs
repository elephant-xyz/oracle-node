import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import {
  buildSearchRouteUrl,
  createTylerChromeLaunchOptions,
} from "../../scripts/permit-source-adapters/tyler-civic-access.mjs";
import {
  classifyClick2GovHtml,
  normalizeClick2GovOrigin,
  parseClick2GovSearchResultsHtml,
  validateClick2GovHttpConfig,
} from "../../scripts/permit-source-adapters/click2gov-http.mjs";
import {
  parseParkCivicAccessIntegration,
  parseParkSelections,
} from "../../scripts/probe-pinellas-park-portico.mjs";
import { parsePinellasTylerProbeOptions } from "../../scripts/probe-pinellas-tyler-civic-access.mjs";
import { parseTarponProbeOptions } from "../../scripts/probe-tarpon-springs-click2gov.mjs";
import {
  parseTarponHarvestCli,
  tarponPermitFileStem,
} from "../../scripts/run-tarpon-springs-permit-harvest.mjs";
import {
  parseTylerHarvestCli,
  tylerPermitFileStem,
} from "../../scripts/run-pinellas-tyler-permit-harvest.mjs";
import {
  PINELLAS_TYLER_AGENCIES,
  resolvePinellasTylerAgency,
} from "../../scripts/pinellas/tyler-agencies.mjs";
import { TARPON_CLICK2GOV_ORIGIN } from "../../scripts/pinellas/tarpon-click2gov.mjs";

const tarponFixture = await readFile(
  new URL(
    "../fixtures/pinellas-permits/tarpon-click2gov-search-results.html",
    import.meta.url,
  ),
  "utf8",
);

describe("Pinellas Tyler Civic Access agencies", () => {
  it("resolves Largo tylerhost and Park EnerGov CSS, not Portico apply", () => {
    const largo = resolvePinellasTylerAgency("largo");
    expect(largo.config.portalBaseUrl).toContain(
      "cityoflargofl-energovweb.tylerhost.net",
    );
    expect(largo.defaultProbeQueries).toHaveLength(2);
    expect(largo.sourceStamp).toBe("largo-city-civic-access");
    const park = resolvePinellasTylerAgency("PARK");
    expect(park.config.portalBaseUrl).toBe(
      "https://egcss.pinellas-park.com/energov_prod/selfservice",
    );
    expect(park.sourceStamp).toBe("pinellas-park-city-energov");
    expect(PINELLAS_TYLER_AGENCIES.clearwater).toBeUndefined();
    expect(() => resolvePinellasTylerAgency("st-pete")).toThrow(
      /--agency must be one of/,
    );
  });

  it("builds paged Civic Access hash routes and launches Chrome without a sandbox", () => {
    const url = buildSearchRouteUrl(
      "https://cityoflargofl-energovweb.tylerhost.net/apps/selfservice",
      "West Bay Drive",
      2,
      10,
    );
    expect(url).toContain("#/search?");
    expect(url).toContain("pn=2");
    expect(url).toContain("ps=10");
    expect(url).toContain("st=West+Bay+Drive");
    expect(() => buildSearchRouteUrl("https://example.com/x", "q", 0)).toThrow(
      /pageNumber/,
    );
    const launch = createTylerChromeLaunchOptions();
    expect(launch.args).toContain("--no-sandbox");
    expect(launch.headless).toBe(true);
  });

  it("defaults the Largo probe to two street keywords and delay ≥ 1s", () => {
    const defaults = parsePinellasTylerProbeOptions([]);
    expect(defaults?.agencyKey).toBe("largo");
    expect(defaults?.queries).toEqual(["West Bay Drive", "Seminole Boulevard"]);
    expect(defaults?.delayMs).toBe(1500);
    expect(() => parsePinellasTylerProbeOptions(["--delay-ms", "999"])).toThrow(
      /at least 1000/,
    );
    const harvest = parseTylerHarvestCli([
      "--agency",
      "park",
      "--job-id",
      "pinellas-park-energov-full-20260903",
      "--query",
      "Park Boulevard",
    ]);
    expect(harvest.agencyKey).toBe("park");
    expect(harvest.queries).toEqual(["Park Boulevard"]);
    expect(harvest.skipExisting).toBe(true);
    expect(tylerPermitFileStem("BLD-2026-00001")).toBe("bld-2026-00001");
  });
});

describe("Tarpon Springs Click2Gov HTTP adapter", () => {
  it("parses search-result rows and dedupes duplicate application links", () => {
    const rows = parseClick2GovSearchResultsHtml(tarponFixture);
    expect(rows.map((row) => row.applicationNumber)).toEqual([
      "13-00000647",
      "18-00002513",
    ]);
    expect(rows[0]).toMatchObject({
      workLocation: "100 S PINELLAS AVE",
      parcelIdentifier: "12-27-15-12910-000-0010",
      applicationType: "ROOFING",
      recordStatus: "FINALED",
    });
    expect(rows[0]?.detailPath).toContain(
      "permit.appYearAndNumber=13-00000647",
    );
  });

  it("classifies Status Detail, misses, and unexpected errors", () => {
    expect(classifyClick2GovHtml("Status Detail")).toBe("ok");
    expect(classifyClick2GovHtml("Permit Search Results")).toBe("ok");
    expect(classifyClick2GovHtml("No matching application found.")).toBe(
      "not_found",
    );
    expect(
      classifyClick2GovHtml("Oops...An unexpected error has occurred!"),
    ).toBe("error");
    expect(normalizeClick2GovOrigin(`${TARPON_CLICK2GOV_ORIGIN}/`)).toBe(
      TARPON_CLICK2GOV_ORIGIN,
    );
    expect(() =>
      validateClick2GovHttpConfig({
        origin: "http://tarp-egov.aspgov.com/Click2GovBP",
        city: "Tarpon Springs",
        sourceStamp: "tarpon-springs-click2gov",
      }),
    ).toThrow(/HTTPS/);
  });

  it("parses probe/harvest CLI with street-number pairs and delay floor", () => {
    const probe = parseTarponProbeOptions([]);
    expect(probe.queries).toHaveLength(2);
    expect(probe.queries[0]).toEqual({
      streetNumber: "100",
      streetName: "PINELLAS",
    });
    const harvest = parseTarponHarvestCli([
      "--job-id",
      "tarpon-click2gov-full-20260903",
      "--street-number",
      "100",
      "--street-name",
      "PINELLAS",
      "--max-details",
      "5",
    ]);
    expect(harvest.jobId).toBe("tarpon-click2gov-full-20260903");
    expect(harvest.maxDetails).toBe(5);
    expect(harvest.queries).toEqual([
      { streetNumber: "100", streetName: "PINELLAS" },
    ]);
    expect(tarponPermitFileStem("13-00000647")).toBe("13-00000647");
    expect(() => parseTarponHarvestCli(["--delay-ms", "500"])).toThrow(
      /at least 1000/,
    );
  });
});

describe("Pinellas Park Portico HTTP certification", () => {
  it("treats isCivicAccess false as not the Civic Access API and reads the apply tile", () => {
    expect(
      parseParkCivicAccessIntegration({
        id: 262,
        name: "EnerGov",
        url: "https://egcss.pinellas-park.com/energov_prod/selfservice#",
        integrationTypeName: "EPL Civic Access",
        isCivicAccess: false,
      }),
    ).toEqual({
      isCivicAccess: false,
      energovCssUrl:
        "https://egcss.pinellas-park.com/energov_prod/selfservice#",
      integrationTypeName: "EPL Civic Access",
    });
    expect(
      parseParkSelections([
        { id: 5996, title: "Apply for a Permit" },
        {
          id: 5994,
          destinationUrlAuthenticated:
            "https://egcss.pinellas-park.com/EnerGov_Prod/SelfService#/home",
        },
      ]),
    ).toEqual({
      applyPermitTitle: "Apply for a Permit",
      dashboardUrl:
        "https://egcss.pinellas-park.com/EnerGov_Prod/SelfService#/home",
    });
  });
});
