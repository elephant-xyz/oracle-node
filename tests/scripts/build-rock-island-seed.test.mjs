import { describe, expect, it } from "vitest";

import {
  EXCLUDED_PII_FIELDS,
  SEED_COLUMNS,
  SOURCE_FIELDS,
  assertSafeSourceFields,
  buildPageUrl,
  encodeCsvCell,
  isValidParcelPin,
  mergeFeatureGroup,
  parseCliOptions,
  renderCsvRow,
  toSeedRow,
} from "../../scripts/build-rock-island-seed.mjs";

describe("Rock Island seed builder", () => {
  it("uses a strict non-PII source allow-list", () => {
    expect(() => assertSafeSourceFields(SOURCE_FIELDS)).not.toThrow();
    for (const excludedField of EXCLUDED_PII_FIELDS) {
      expect(SOURCE_FIELDS).not.toContain(excludedField);
      expect(SEED_COLUMNS).not.toContain(`source_${excludedField}`);
    }
    expect(() =>
      assertSafeSourceFields([...SOURCE_FIELDS, "owner1_name"]),
    ).toThrow(/PII field is prohibited/);
  });

  it("preserves a leading-zero PIN and WGS84 geometry", () => {
    const feature = {
      id: 1,
      properties: {
        OBJECTID: 1,
        PIN: "0012345678",
        site_address: "100 MAIN ST",
        Site_City: "MOLINE",
        Site_State: "IL",
        Site_Zip: "61265",
        X_longitude: -90.5,
        Y_latitude: 41.5,
        EAV: 100,
        EMV: 300,
      },
      geometry: {
        type: "Polygon",
        coordinates: [
          [
            [-90.6, 41.4],
            [-90.5, 41.4],
            [-90.6, 41.4],
          ],
        ],
      },
    };

    const row = toSeedRow(
      feature,
      "2026-07-14T00:00:00.000Z",
      "2026-08-03T00:00:00.000Z",
    );

    expect(row.parcel_id).toBe("0012345678");
    expect(row.source_identifier).toBe("0012345678");
    expect(row.address).toBe("100 MAIN ST, MOLINE IL 61265");
    expect(row.county).toBe("Rock Island");
    expect(row.county_fips).toBe("17161");
    expect(row.source_PIN).toBe("0012345678");
    expect(JSON.parse(row.parcel_polygon)).toMatchObject({ type: "Polygon" });
    expect(row.method).toBe("GET");
    expect(row.url).toBe(
      "https://services9.arcgis.com/6FnscPPlUa9DXXOk/ArcGIS/rest/services/Parcels/FeatureServer/0/query",
    );
    expect(JSON.parse(row.multiValueQueryString)).toMatchObject({
      f: ["geojson"],
      outSR: ["4326"],
      returnGeometry: ["true"],
      where: ["PIN='0012345678'"],
    });
    expect(JSON.parse(row.multiValueQueryString).outFields[0]).not.toContain(
      "owner1_name",
    );
  });

  it("quarantines placeholder PIN values from the keyed seed", () => {
    expect(isValidParcelPin("0012345678")).toBe(true);
    expect(isValidParcelPin("USA")).toBe(false);
    expect(isValidParcelPin("RAILROAD")).toBe(false);
    expect(isValidParcelPin("")).toBe(false);
  });

  it("consolidates duplicate PIN geometries and preserves source records", () => {
    const feature = {
      id: 1,
      properties: {
        OBJECTID: 10,
        PIN: "0012345678",
        site_address: "100 MAIN ST",
        Site_City: "MOLINE",
        Site_State: "IL",
        Site_Zip: "61265",
        X_longitude: -90.5,
        Y_latitude: 41.5,
        EAV: 100,
        EMV: 300,
      },
      geometry: {
        type: "Polygon",
        coordinates: [
          [
            [-90.6, 41.4],
            [-90.5, 41.4],
            [-90.6, 41.4],
          ],
        ],
      },
    };
    const secondFeature = {
      ...feature,
      id: 2,
      properties: {
        ...feature.properties,
        OBJECTID: 11,
        EAV: 125,
      },
      geometry: {
        type: "Polygon",
        coordinates: [
          [
            [-90.7, 41.4],
            [-90.6, 41.4],
            [-90.7, 41.4],
          ],
        ],
      },
    };

    const row = mergeFeatureGroup(
      [secondFeature, feature],
      "2026-07-14T00:00:00.000Z",
      "2026-08-03T00:00:00.000Z",
    );

    expect(row.source_record_count).toBe("2");
    expect(row.source_object_ids).toBe("10|11");
    expect(JSON.parse(row.parcel_polygon)).toMatchObject({
      type: "MultiPolygon",
    });
    expect(JSON.parse(row.source_features_json)).toHaveLength(2);
    expect(row.source_EAV).toBe("100");
  });

  it("builds ordered PII-free GeoJSON page requests", () => {
    const url = buildPageUrl(2_000, 2_000);

    expect(url.searchParams.get("f")).toBe("geojson");
    expect(url.searchParams.get("outSR")).toBe("4326");
    expect(url.searchParams.get("orderByFields")).toBe("OBJECTID");
    expect(url.searchParams.get("resultOffset")).toBe("2000");
    expect(url.searchParams.get("resultRecordCount")).toBe("2000");
    expect(url.searchParams.get("outFields")).toBe(SOURCE_FIELDS.join(","));
    expect(url.searchParams.get("outFields")).not.toContain("owner1_name");
    expect(url.searchParams.get("outFields")).not.toContain("taxbill_name");
  });

  it("quotes embedded commas and quotes in CSV output", () => {
    expect(encodeCsvCell('10 "A", MAIN')).toBe('"10 ""A"", MAIN"');
    const row = Object.fromEntries(SEED_COLUMNS.map((column) => [column, ""]));
    row.parcel_id = "0012345678";
    row.address = '10 "A", MAIN';

    const rendered = renderCsvRow(row);

    expect(rendered).toContain("0012345678");
    expect(rendered).toContain('"10 ""A"", MAIN"');
    expect(rendered.endsWith("\n")).toBe(true);
  });

  it("caps source concurrency at the tested maximum", () => {
    expect(
      parseCliOptions(["--page-size", "1000", "--concurrency", "2"]),
    ).toMatchObject({ pageSize: 1_000, concurrency: 2 });
    expect(() => parseCliOptions(["--concurrency", "5"])).toThrow(
      /cannot exceed/,
    );
  });
});
