import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { parse } from "yaml";
import { describe, expect, it } from "vitest";

const REQUIRED_CATEGORIES = [
  "property",
  "geometry",
  "permits",
  "sales-deeds",
  "business-registration",
  "contractor-reputation",
  "business-places",
];

const AVAILABLE_STATUS_PREFIX = "available";

describe("Duval source catalog gaps", () => {
  it("gives every in-scope category an official URL or a written unavailability reason", async () => {
    const catalog = parse(
      await readFile(resolve("docs/duval-sources.yaml"), "utf8"),
    );
    const zipPrefixes = JSON.parse(
      await readFile(resolve("docs/duval-sunbiz-zip-prefixes.json"), "utf8"),
    );

    expect(catalog.county.fips).toBe("12031");
    expect(Array.isArray(catalog.sources)).toBe(true);

    const categories = new Set(
      catalog.sources.map((source) => source.category),
    );
    for (const category of REQUIRED_CATEGORIES) {
      expect(categories.has(category)).toBe(true);
    }

    for (const source of catalog.sources) {
      expect(typeof source.key).toBe("string");
      expect(typeof source.status).toBe("string");
      expect(source.status.length).toBeGreaterThan(0);

      const urls = Array.isArray(source.base_urls) ? source.base_urls : [];
      const hasUrl = urls.some(
        (url) => typeof url === "string" && url.startsWith("http"),
      );
      const hasReason =
        typeof source.reason === "string" && source.reason.trim().length > 0;

      if (source.status.startsWith(AVAILABLE_STATUS_PREFIX)) {
        expect(hasUrl).toBe(true);
      } else {
        expect(hasReason).toBe(true);
      }
    }

    const sunbiz = catalog.sources.find((source) => source.key === "sunbiz");
    expect(sunbiz.zip_prefixes).toEqual(zipPrefixes.prefixes);
    expect(sunbiz.additional_exact_zips).toEqual(
      zipPrefixes.additionalExactZips,
    );
    expect(zipPrefixes.prefixes).toEqual(["322"]);
    expect(zipPrefixes.additionalExactZips).toEqual(["32099"]);

    const geometry = catalog.sources.find(
      (source) => source.key === "gis-geometry",
    );
    expect(geometry.publication_rights).toMatch(/chapter 119/i);

    const skippedPermitRows = catalog.permits.filter(
      (row) => !row.portal || String(row.portal).trim() === "",
    );
    expect(skippedPermitRows.map((row) => row.jurisdiction)).toEqual([
      "Neptune Beach",
      "Baldwin",
    ]);
  });
});
