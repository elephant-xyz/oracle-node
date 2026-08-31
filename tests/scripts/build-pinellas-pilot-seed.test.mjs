import { describe, expect, it } from "vitest";

import {
  EXCLUDED_PII_FIELDS,
  SEED_COLUMNS,
  SOURCE_FIELDS,
  USE_CODE_QUOTAS,
  assertPrintLookup,
  assertSafeSourceFields,
  buildPrintUrl,
  buildUseCodeQueryUrl,
  classifyGeometry,
  dedupeByStrap,
  encodeCsvCell,
  isValidStrap,
  parseGisFeatureCollection,
  pickMixedGeometry,
  printHtmlLooksPopulated,
  toSeedRow,
} from "../../scripts/build-pinellas-pilot-seed.mjs";

describe("Pinellas pilot seed builder", () => {
  it("keeps owner mailing fields out of the GIS allow-list", () => {
    expect(() => assertSafeSourceFields(SOURCE_FIELDS)).not.toThrow();
    for (const excludedField of EXCLUDED_PII_FIELDS) {
      expect(SOURCE_FIELDS).not.toContain(excludedField);
    }
    expect(() => assertSafeSourceFields([...SOURCE_FIELDS, "OWNER1"])).toThrow(
      /PII field is prohibited/,
    );
  });

  it("accepts only 18-digit STRAP values and rejects PARCELID-shaped mistakes silently becoming valid", () => {
    expect(isValidStrap("162805389030000430")).toBe(true);
    expect(isValidStrap("052816389030000430")).toBe(true);
    expect(isValidStrap("16280538903000043")).toBe(false);
    expect(isValidStrap("1628053890300004300")).toBe(false);
    expect(isValidStrap("16-28-05-38903-000-0430")).toBe(false);
    expect(isValidStrap("")).toBe(false);
  });

  it("uses STRAP as parcel_id and keeps PARCELID as an extra column", () => {
    const row = toSeedRow(
      {
        properties: {
          STRAP: "162805389030000430",
          PARCELID: "052816389030000430",
          PARCELID_DSP1: "05-28-16-38903-000-0430",
          USE_CODE: "0110",
          SITE_ADDRESS: "3400 RUGBY CT",
          SITE_CITY: "PALM HARBOR",
          SITE_STATE: "FL",
          SITE_ZIP: "34684",
          Acres: 0.25,
        },
        geometry: {
          type: "Polygon",
          coordinates: [
            [
              [-82.73, 28.08],
              [-82.72, 28.08],
              [-82.72, 28.09],
              [-82.73, 28.09],
              [-82.73, 28.08],
            ],
          ],
        },
      },
      "single-family",
      "2026-08-27T00:00:00.000Z",
    );

    expect(row.parcel_id).toBe("162805389030000430");
    expect(row.source_identifier).toBe("162805389030000430");
    expect(row.parcelid).toBe("052816389030000430");
    expect(row.parcel_id).not.toBe(row.parcelid);
    expect(row.url).toBe("https://www.pcpao.gov/property/detail/print");
    expect(row.url).not.toContain("?");
    expect(buildPrintUrl("162805389030000430")).toContain("s=162805389030000430");
    expect(JSON.parse(row.multiValueQueryString)).toEqual({
      is_print: ["1"],
      s: ["162805389030000430"],
    });
    expect(row.county).toBe("Pinellas");
    expect(row.county_fips).toBe("12103");
    expect(row.geometry_type).toBe("simple-polygon");
    expect(JSON.parse(row.parcel_polygon).type).toBe("Polygon");
    expect(SEED_COLUMNS).toContain("parcel_id");
  });

  it("classifies simple, complex, and multi-ring geometries", () => {
    expect(
      classifyGeometry({
        type: "Polygon",
        coordinates: [
          [
            [0, 0],
            [1, 0],
            [1, 1],
            [0, 0],
          ],
        ],
      }).geometryType,
    ).toBe("simple-polygon");

    const longRing = Array.from({ length: 25 }, (_, index) => [index, 0]);
    expect(
      classifyGeometry({
        type: "Polygon",
        coordinates: [longRing],
      }).geometryType,
    ).toBe("complex-polygon");

    expect(
      classifyGeometry({
        type: "Polygon",
        coordinates: [
          [
            [0, 0],
            [1, 0],
            [0, 1],
            [0, 0],
          ],
          [
            [0.2, 0.2],
            [0.3, 0.2],
            [0.2, 0.3],
            [0.2, 0.2],
          ],
        ],
      }).geometryType,
    ).toBe("multi-polygon");
  });

  it("deduplicates by STRAP and refuses a non-STRAP parcel_id", () => {
    const first = { parcel_id: "162805389030000430" };
    const duplicate = { parcel_id: "162805389030000430" };
    const second = { parcel_id: "163131676080040070" };
    expect(dedupeByStrap([first, duplicate, second])).toHaveLength(2);
    expect(() => dedupeByStrap([{ parcel_id: "05-28-16-38903-000-0430" }])).toThrow(
      /non-STRAP/,
    );
  });

  it("prefers at least one complex geometry when picking a mixed subset", () => {
    const candidates = [
      { parcel_id: "1".repeat(18), geometry_type: "simple-polygon" },
      { parcel_id: "2".repeat(18), geometry_type: "simple-polygon" },
      { parcel_id: "3".repeat(18), geometry_type: "complex-polygon" },
    ];
    const picked = pickMixedGeometry(candidates, 2);
    expect(picked).toHaveLength(2);
    expect(picked.some((row) => row.geometry_type === "complex-polygon")).toBe(
      true,
    );
  });

  it("treats an empty print page as a hard error", async () => {
    await expect(
      assertPrintLookup("162805389030000430", async () => {
        return "Parcel Summary Owner Name Buildings 0 Parcel Map No Property Values on Record.";
      }),
    ).rejects.toThrow(/empty\/placeholder page/);
    await expect(
      assertPrintLookup("162805389030000430", async () => {
        return "Parcel Summary Owner Name Frobose Just/Market Value $316,864 Buildings 1 Parcel Map";
      }),
    ).resolves.toMatchObject({ ok: true, strap: "162805389030000430" });
  });

  it("detects populated vs empty print HTML", () => {
    expect(
      printHtmlLooksPopulated(
        "Parcel Summary Owner Name Frobose Buildings 1 Parcel Map Just/Market Value",
      ),
    ).toBe(true);
    expect(
      printHtmlLooksPopulated(
        "Parcel Summary Owner Name Buildings 0 Parcel Map No Property Values on Record.",
      ),
    ).toBe(false);
  });

  it("requests WGS84 GeoJSON without PII outFields", () => {
    const url = buildUseCodeQueryUrl("0110", 15);
    expect(url.searchParams.get("outSR")).toBe("4326");
    expect(url.searchParams.get("f")).toBe("geojson");
    expect(url.searchParams.get("where")).toContain("USE_CODE='0110'");
    expect(url.searchParams.get("outFields")).not.toContain("OWNER1");
  });

  it("parses a GeoJSON feature collection", () => {
    const features = parseGisFeatureCollection({
      type: "FeatureCollection",
      features: [
        {
          properties: { STRAP: "162805389030000430" },
          geometry: { type: "Polygon", coordinates: [] },
        },
      ],
    });
    expect(features).toHaveLength(1);
    expect(features[0].properties.STRAP).toBe("162805389030000430");
  });

  it("quotes commas in CSV cells", () => {
    expect(encodeCsvCell('3400 RUGBY CT, PALM HARBOR FL 34684')).toBe(
      '"3400 RUGBY CT, PALM HARBOR FL 34684"',
    );
  });

  it("keeps use-code quotas at a 10–50 pilot size", () => {
    const quotaSum = USE_CODE_QUOTAS.reduce((sum, quota) => sum + quota.count, 0);
    expect(quotaSum).toBeGreaterThanOrEqual(10);
    expect(quotaSum).toBeLessThanOrEqual(50);
  });
});
