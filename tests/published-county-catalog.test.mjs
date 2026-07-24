import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

import {
  normalizeCountyKey,
  upsertCounty,
  validateCatalog,
  verifyPublishedCountyArtifacts,
} from "../scripts/update-published-county-catalog.mjs";

const baseCatalog = {
  schemaVersion: "1.0",
  generatedAt: "2026-07-24T00:00:00.000Z",
  counties: [
    {
      countyKey: "lee",
      countyName: "Lee",
      stateCode: "FL",
      countyFips: "12071",
      status: "published",
      queryTableUrl: "https://example.com/lee.parquet",
      datasetCoverageUrl: "https://example.com/lee-coverage.json",
      permitQueryTableUrl: null,
      updatedAt: "2026-07-23T00:00:00.000Z",
    },
  ],
};

describe("published county catalog", () => {
  it("validates the tracked canonical catalog", async () => {
    const tracked = JSON.parse(
      await readFile(resolve("catalog/published-counties.json"), "utf8"),
    );

    const result = validateCatalog(tracked);

    expect(result.counties.length).toBeGreaterThan(0);
    expect(result.counties.map((county) => county.countyKey)).toEqual([
      "lee",
      "miami-dade",
      "orange",
      "palm-beach",
    ]);
  });

  it("normalizes county keys", () => {
    expect(normalizeCountyKey("  Miami Dade  ")).toBe("miami-dade");
    expect(normalizeCountyKey("Palm_Beach")).toBe("palm-beach");
  });

  it("upserts a county and keeps entries sorted", () => {
    const updated = upsertCounty(
      validateCatalog(baseCatalog),
      {
        countyKey: "alameda",
        countyName: "Alameda",
        stateCode: "CA",
        countyFips: "06001",
        status: "published",
        queryTableUrl: "https://example.com/alameda.parquet",
        datasetCoverageUrl: "https://example.com/alameda-coverage.json",
        permitQueryTableUrl: null,
        updatedAt: "2026-07-24T10:00:00.000Z",
      },
      "2026-07-24T10:01:00.000Z",
    );

    expect(updated.generatedAt).toBe("2026-07-24T10:01:00.000Z");
    expect(updated.counties.map((county) => county.countyKey)).toEqual([
      "alameda",
      "lee",
    ]);
  });

  it("rejects duplicate county keys", () => {
    expect(() =>
      validateCatalog({
        ...baseCatalog,
        counties: [...baseCatalog.counties, baseCatalog.counties[0]],
      }),
    ).toThrow("duplicate countyKey 'lee'");
  });

  it("rejects published counties without coverage", () => {
    expect(() =>
      validateCatalog({
        ...baseCatalog,
        counties: [
          {
            ...baseCatalog.counties[0],
            datasetCoverageUrl: null,
          },
        ],
      }),
    ).toThrow("datasetCoverageUrl must be an HTTP(S) URL");
  });

  it("rejects duplicate county FIPS identities", () => {
    expect(() =>
      validateCatalog({
        ...baseCatalog,
        counties: [
          ...baseCatalog.counties,
          {
            ...baseCatalog.counties[0],
            countyKey: "different-key",
          },
        ],
      }),
    ).toThrow("duplicate countyFips '12071'");
  });

  it("reads back artifacts and verifies the coverage county", async () => {
    const requests = [];
    const fetchImpl = async (url, init) => {
      requests.push({ url: String(url), method: init?.method ?? "GET" });
      if (init?.method === "HEAD") {
        return new Response(null, { status: 200 });
      }
      return new Response(JSON.stringify({ county: "Lee", datasets: [] }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    };

    await verifyPublishedCountyArtifacts(
      validateCatalog(baseCatalog).counties[0],
      fetchImpl,
    );

    expect(requests).toEqual([
      { url: "https://example.com/lee.parquet", method: "HEAD" },
      { url: "https://example.com/lee-coverage.json", method: "GET" },
    ]);
  });

  it("verifies an optional permit query table", async () => {
    const requests = [];
    const fetchImpl = async (url, init) => {
      requests.push({ url: String(url), method: init?.method ?? "GET" });
      if (init?.method === "HEAD") {
        return new Response(null, { status: 200 });
      }
      return new Response(JSON.stringify({ county: "Lee" }), { status: 200 });
    };
    const county = {
      ...validateCatalog(baseCatalog).counties[0],
      permitQueryTableUrl: "https://example.com/lee-permits.parquet",
    };

    await verifyPublishedCountyArtifacts(county, fetchImpl);

    expect(requests.at(-1)).toEqual({
      url: "https://example.com/lee-permits.parquet",
      method: "HEAD",
    });
  });

  it("rejects coverage for a different county", async () => {
    const fetchImpl = async (_url, init) =>
      init?.method === "HEAD"
        ? new Response(null, { status: 200 })
        : new Response(JSON.stringify({ county: "Orange" }), { status: 200 });

    await expect(
      verifyPublishedCountyArtifacts(
        validateCatalog(baseCatalog).counties[0],
        fetchImpl,
      ),
    ).rejects.toThrow("does not match 'lee'");
  });

  it("rejects non-public query table URLs", () => {
    expect(() =>
      validateCatalog({
        ...baseCatalog,
        counties: [
          {
            ...baseCatalog.counties[0],
            queryTableUrl: "file:///tmp/lee.parquet",
          },
        ],
      }),
    ).toThrow("must use http or https");
  });
});
