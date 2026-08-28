import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

import {
  normalizeCountyKey,
  upsertCounty,
  validateCatalog,
  verifyPublishedCountyArtifacts,
} from "../scripts/update-published-county-catalog.mjs";

const fullPublicationScope = {
  schemaVersion: "1.0",
  level: "full",
  denominatorBasis: "county_total",
};

const baseCatalog = {
  schemaVersion: "1.1",
  generatedAt: "2026-07-24T00:00:00.000Z",
  counties: [
    {
      countyKey: "lee",
      countyName: "Lee",
      stateCode: "FL",
      countyFips: "12071",
      status: "published",
      publicationScope: fullPublicationScope,
      queryTableUrl: "https://example.com/lee.parquet",
      datasetCoverageUrl: "https://example.com/lee-coverage.json",
      permitQueryTableUrl: null,
      placesTableUrl: null,
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
      "hillsborough",
      "lee",
      "miami-dade",
      "orange",
      "palm-beach",
      "rock-island",
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
        publicationScope: fullPublicationScope,
        queryTableUrl: "https://example.com/alameda.parquet",
        datasetCoverageUrl: "https://example.com/alameda-coverage.json",
        permitQueryTableUrl: null,
        placesTableUrl: null,
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

  it("rejects a FIPS code already assigned to another county", () => {
    expect(() =>
      upsertCounty(
        validateCatalog(baseCatalog),
        {
          ...baseCatalog.counties[0],
          countyKey: "orange",
        },
        "2026-07-24T10:01:00.000Z",
      ),
    ).toThrow("countyFips '12071' is already assigned to 'lee'");
  });

  it("rejects changing the FIPS identity of an existing county", () => {
    expect(() =>
      upsertCounty(
        validateCatalog(baseCatalog),
        {
          ...baseCatalog.counties[0],
          countyFips: "12095",
        },
        "2026-07-24T10:01:00.000Z",
      ),
    ).toThrow("countyKey 'lee' is already assigned to FIPS '12071'");
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

  it("keeps a 50-of-50 pilot explicitly sample-scoped", async () => {
    const fixture = JSON.parse(
      await readFile(resolve("fixtures/publication-scope-v1.json"), "utf8"),
    );
    const pilot = fixture.scenarios.find(
      (scenario) => scenario.id === "hillsborough-50-of-50-pilot",
    );

    expect(pilot).toMatchObject({
      countyKey: "hillsborough",
      ingestedCount: 50,
      expectedCount: 50,
      factEligible: false,
      publicationScope: {
        schemaVersion: "1.0",
        level: "pilot",
        denominatorBasis: "published_subset",
      },
    });
  });

  it("rejects missing and unsupported publication scope", () => {
    const { publicationScope: _scope, ...withoutScope } =
      baseCatalog.counties[0];
    expect(() =>
      validateCatalog({ ...baseCatalog, counties: [withoutScope] }),
    ).toThrow("publicationScope must be an object");
    expect(() =>
      validateCatalog({
        ...baseCatalog,
        counties: [
          {
            ...baseCatalog.counties[0],
            publicationScope: {
              ...fullPublicationScope,
              level: "unknown",
            },
          },
        ],
      }),
    ).toThrow("level must be full, partial, or pilot");
  });

  it("changes the catalog when a county transitions from pilot to full", () => {
    const pilotCatalog = validateCatalog({
      ...baseCatalog,
      counties: [
        {
          ...baseCatalog.counties[0],
          publicationScope: {
            schemaVersion: "1.0",
            level: "pilot",
            denominatorBasis: "published_subset",
          },
        },
      ],
    });
    const transitioned = upsertCounty(
      pilotCatalog,
      baseCatalog.counties[0],
      "2026-07-25T00:00:00.000Z",
    );

    expect(transitioned.counties[0].publicationScope).toEqual(
      fullPublicationScope,
    );
  });

  it("reads back artifacts and verifies the coverage county", async () => {
    const requests = [];
    const fetchImpl = async (url, init) => {
      requests.push({ url: String(url), method: init?.method ?? "GET" });
      if (init?.method === "HEAD") {
        return new Response(null, { status: 200 });
      }
      return new Response(
        JSON.stringify({
          county: "Lee",
          publicationScope: fullPublicationScope,
          datasets: [],
        }),
        {
          status: 200,
          headers: { "content-type": "application/json" },
        },
      );
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
      return new Response(
        JSON.stringify({
          county: "Lee",
          publicationScope: fullPublicationScope,
        }),
        { status: 200 },
      );
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

  it("verifies an optional places table", async () => {
    const requests = [];
    const fetchImpl = async (url, init) => {
      requests.push({ url: String(url), method: init?.method ?? "GET" });
      if (init?.method === "HEAD") {
        return new Response(null, { status: 200 });
      }
      return new Response(
        JSON.stringify({
          county: "Lee",
          publicationScope: fullPublicationScope,
        }),
        { status: 200 },
      );
    };
    const county = {
      ...validateCatalog(baseCatalog).counties[0],
      placesTableUrl: "https://example.com/lee-places.parquet",
    };

    await verifyPublishedCountyArtifacts(county, fetchImpl);

    expect(requests.at(-1)).toEqual({
      url: "https://example.com/lee-places.parquet",
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

  it("rejects inconsistent catalog and coverage scope", async () => {
    const fetchImpl = async (_url, init) =>
      init?.method === "HEAD"
        ? new Response(null, { status: 200 })
        : new Response(
            JSON.stringify({
              county: "Lee",
              publicationScope: {
                schemaVersion: "1.0",
                level: "partial",
                denominatorBasis: "county_total",
              },
            }),
            { status: 200 },
          );

    await expect(
      verifyPublishedCountyArtifacts(
        validateCatalog(baseCatalog).counties[0],
        fetchImpl,
      ),
    ).rejects.toThrow(
      "dataset coverage publicationScope does not match catalog",
    );
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
