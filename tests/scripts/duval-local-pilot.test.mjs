import { describe, expect, it } from "vitest";

import { parsePilotArgs } from "../../scripts/hillsborough/lib.mjs";
import {
  assertCaptureCounty,
  assertCojDetailHtml,
  assertHtmlMatchesRequestedRe,
  assertManifestReconciled,
  assertTransformedCounty,
  buildUnnormalizedAddress,
  classifyDuvalFailure,
  extractCanonicalRe,
  parseDuvalPilotArgs,
  seedRowToCsv,
  toCojCaptureUrl,
} from "../../scripts/duval/pilot-lib.mjs";

describe("Duval local pilot", () => {
  it("defaults concurrency to 2 and a Duval job id", () => {
    const options = parseDuvalPilotArgs(["--limit=50"]);
    expect(options.limit).toBe(50);
    expect(options.concurrency).toBe(2);
    expect(options.jobId).toMatch(/^duval-local-/);
    expect(parsePilotArgs(["--limit=50"]).concurrency).toBe(2);
  });

  it("puts county_jurisdiction Duval on the capture address input", () => {
    const address = buildUnnormalizedAddress({
      address: "5627 DART DR, JACKSONVILLE FL 32244",
      city: "JACKSONVILLE",
      state: "FL",
      zip: "32244",
      latitude: "30.245",
      longitude: "-81.773",
      source_identifier: "0969250000R",
    });
    expect(address.county_jurisdiction).toBe("Duval");
    expect(address.full_address).toContain("5627 DART DR");
    expect(address.latitude).toBeCloseTo(30.245);
  });

  it("keeps a parcel with no PIN match at null coordinates, never 0,0", () => {
    const address = buildUnnormalizedAddress({
      address: "1 NOWHERE RD, JACKSONVILLE FL 32234",
      source_identifier: "0000010005R",
      latitude: "",
      longitude: "",
    });
    expect(address.latitude).toBeNull();
    expect(address.longitude).toBeNull();
  });

  it("extracts the canonical RE Number and rejects empty or blocked pages", () => {
    const html = `
      <span id="ctl00_cphBody_lblRealEstateNumber">096925-0000</span>
      <span id="ctl00_cphBody_lblPropertyUse">0100 Single Family</span>
    `;
    expect(extractCanonicalRe(html)).toBe("096925-0000");
    expect(() => assertCojDetailHtml(html)).not.toThrow();
    expect(() => assertCojDetailHtml("")).toThrow(/empty/i);
    expect(() => assertCojDetailHtml("<html>Access Denied</html>")).toThrow(
      /blocked/i,
    );
    expect(() =>
      assertCojDetailHtml("<html><body>no property detail</body></html>"),
    ).toThrow(/RE Number/i);
  });

  it("requires transformed address county_name Duval", () => {
    expect(() =>
      assertTransformedCounty({ county_name: "Duval" }),
    ).not.toThrow();
    expect(() => assertTransformedCounty({ county_name: "Columbia" })).toThrow(
      /Duval/,
    );
    // The capture-input spelling must not satisfy the transform-output check.
    expect(() =>
      assertTransformedCounty({ county_jurisdiction: "Duval" }),
    ).toThrow(/county_name/);
    expect(() => assertTransformedCounty(null)).toThrow(/county_name/);
    expect(() => assertTransformedCounty({})).toThrow(/county_name/);
  });

  it("checks the capture input under its own key", () => {
    expect(() =>
      assertCaptureCounty({ county_jurisdiction: "Duval" }),
    ).not.toThrow();
    expect(() =>
      assertCaptureCounty({ county_jurisdiction: "Columbia" }),
    ).toThrow(/Duval/);
  });

  it("reconciles a retry run against that run's attempts, not the job total", () => {
    expect(() =>
      assertManifestReconciled({
        seedRows: 2,
        attempted: 2,
        success: 2,
        failures: 0,
      }),
    ).not.toThrow();
    expect(() =>
      assertManifestReconciled({
        seedRows: 50,
        attempted: 50,
        success: 48,
        failures: 1,
      }),
    ).toThrow(/attempted/);
  });

  it("builds the COJ capture URL from the seed RE without numeric coercion", () => {
    const url = toCojCaptureUrl({
      source_identifier: "0000160100R",
      url: "https://paopropertysearch.coj.net/Basic/Detail.aspx",
      multiValueQueryString: JSON.stringify({ RE: ["0000160100R"] }),
    });
    expect(url).toBe(
      "https://paopropertysearch.coj.net/Basic/Detail.aspx?RE=0000160100R",
    );
    expect(url).toContain("0000160100R");
    expect(url).not.toContain("RE=160100");
  });

  it("rejects HTML whose RE Number does not match the requested parcel", () => {
    const html = `
      <span id="ctl00_cphBody_lblRealEstateNumber">096925-0000</span>
    `;
    expect(() => assertHtmlMatchesRequestedRe(html, "0000160100R")).toThrow(
      /does not match requested/,
    );
    expect(assertHtmlMatchesRequestedRe(html, "0969250000R")).toBe(
      "096925-0000",
    );
  });

  it("reconciles seedRows to attempted and success plus failures", () => {
    expect(() =>
      assertManifestReconciled({
        seedRows: 50,
        attempted: 50,
        success: 48,
        failures: 2,
      }),
    ).not.toThrow();
    expect(() =>
      assertManifestReconciled({
        seedRows: 50,
        attempted: 49,
        success: 49,
        failures: 0,
      }),
    ).toThrow(/seedRows/);
    expect(classifyDuvalFailure(new Error("COJ detail page is empty"))).toBe(
      "permanent",
    );
  });

  it("treats a wrong transformed county as permanent so retries do not loop", () => {
    const countyError = new Error(
      "transformed county_name must be Duval, got Columbia",
    );
    expect(classifyDuvalFailure(countyError)).toBe("permanent");
    expect(
      classifyDuvalFailure(
        new Error(
          "ENOENT: no such file or directory, open 'data/address.json'",
        ),
      ),
    ).toBe("permanent");
    expect(classifyDuvalFailure(new Error("COJ detail HTTP 503"))).toBe(
      "transient",
    );
  });

  it("writes seed.csv with a quoted parcel polygon and request_identifier", () => {
    const csv = seedRowToCsv({
      parcel_id: "0969250000",
      source_identifier: "0969250000R",
      parcel_polygon: '{"type":"Polygon","coordinates":[[[0,0],[1,1]]]}',
      latitude: "30.2",
      longitude: "-81.7",
    });
    expect(csv).toContain("request_identifier");
    expect(csv).toContain("0969250000R");
    expect(csv).toContain("Polygon");
    expect(csv).toMatch(/".*Polygon.*"/s);
  });
});
