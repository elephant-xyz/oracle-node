import { createRequire } from "module";
import { fileURLToPath } from "url";
import { describe, expect, it } from "vitest";

const require = createRequire(import.meta.url);
const extractorPath = fileURLToPath(
  new URL(
    "../../../Counties-trasform-scripts/rock island/scripts/data_extractor.js",
    import.meta.url,
  ),
);
const propertyClassMappingPath = fileURLToPath(
  new URL(
    "../../../Counties-trasform-scripts/rock island/scripts/propertyClassMapping.js",
    import.meta.url,
  ),
);
const {
  assertNoPiiFields,
  buildAppraisalEntities,
  buildGeometryEntities,
  parseSiteAddress,
  readDate,
  selectPrimaryFeature,
} = require(extractorPath);
const {
  PROPERTY_CLASS_DEFINITIONS,
  PROPERTY_CLASS_MAPPING_SOURCE_URL,
  PROPERTY_CLASS_MAPPING_VERSION,
  mapPropertyClass,
} = require(propertyClassMappingPath);

/**
 * Build the prepared multi-request payload produced for one Rock Island PIN.
 *
 * @param {Record<string, unknown>[]} properties - ArcGIS feature properties.
 * @param {(Record<string, unknown> | null)[]} [geometries] - Optional GeoJSON geometries.
 * @returns {Record<string, unknown>} Prepared transform input.
 */
function preparedPayload(properties, geometries = []) {
  return {
    ParcelFeature: {
      source_http_request: {
        method: "GET",
        url: "https://services9.arcgis.com/example/FeatureServer/0/query",
        multiValueQueryString: {
          where: ["PIN='0012345678'"],
          outFields: ["PIN,EAV,EMV"],
        },
      },
      response: {
        type: "FeatureCollection",
        features: properties.map((attributes, index) => ({
          type: "Feature",
          id: index + 1,
          properties: attributes,
          geometry: geometries[index] ?? null,
        })),
      },
    },
  };
}

describe("Rock Island appraisal data extractor", () => {
  it("maps PII-free ArcGIS facts using the authoritative county class dictionary", () => {
    const entities = buildAppraisalEntities(
      preparedPayload([
        {
          OBJECTID: 10,
          PIN: "0012345678",
          site_address: "100 N MAIN ST W",
          Site_City: "MOLINE",
          Site_State: "IL",
          Site_Zip: "61265",
          X_longitude: -90.5,
          Y_latitude: 41.5,
          municipality: "MOLINE",
          GIS_acres_num: 0.5,
          class: "0081",
          Zoning: "I2",
          EAV: 100_000,
          EMV: 300_000,
          farm_land: 1_000,
          non_farm_land: 24_000,
          farm_building: 0,
          non_farm_building: 75_000,
          taxbill_year: 2025,
          legal: "LOT 1",
          YRBuilt: 1980,
          TOTSQFT: 2_000,
          date_last_sale: 1_613_260_800_000,
          net_sale_price: 250_000,
        },
      ]),
    );

    expect(entities.property).toMatchObject({
      parcel_identifier: "0012345678",
      property_legal_description_text: "LOT 1",
      property_structure_built_year: 1980,
      property_type: "Building",
      property_usage_type: "Industrial",
      zoning: "I2",
    });
    expect(entities.property).not.toHaveProperty("owner");
    expect(entities.parcel).toMatchObject({
      request_identifier: "0012345678",
      parcel_identifier: "0012345678",
    });
    expect(entities.address).toMatchObject({
      unnormalized_address: "100 N MAIN ST W, MOLINE IL 61265",
      city_name: "MOLINE",
    });
    expect(entities.address).not.toHaveProperty("county_name");
    expect(entities.lot).toMatchObject({
      lot_type: "GreaterThanOneQuarterAcre",
      lot_size_acre: 0.5,
      lot_area_sqft: 21_780,
      lot_length_feet: null,
      lot_width_feet: null,
    });
    expect(entities.tax).toMatchObject({
      tax_year: 2025,
      property_assessed_value_amount: 100_000,
      property_market_value_amount: 300_000,
      property_land_amount: 25_000,
      property_building_amount: 75_000,
      monthly_tax_amount: null,
      period_end_date: null,
      period_start_date: null,
    });
    expect(entities.sale).toMatchObject({
      ownership_transfer_date: "2021-02-14",
      purchase_price_amount: 250_000,
    });
    expect(entities.sourcePayload).toMatchObject({
      request_identifier: "0012345678",
      classification: {
        rawCode: "0081",
        officialLabel: "Industrial Vacant Land",
        propertyUsageType: "Industrial",
        dictionaryStatus: "authoritative_definition",
        mappingVersion: PROPERTY_CLASS_MAPPING_VERSION,
        sourceUrl: PROPERTY_CLASS_MAPPING_SOURCE_URL,
      },
      response: { type: "FeatureCollection" },
    });
  });

  it("covers every county-authored class definition with conservative usage values", () => {
    expect(Object.keys(PROPERTY_CLASS_DEFINITIONS).sort()).toEqual([
      "0010",
      "0011",
      "0020",
      "0021",
      "0028",
      "0029",
      "0030",
      "0032",
      "0040",
      "0041",
      "0050",
      "0052",
      "0060",
      "0062",
      "0065",
      "0070",
      "0072",
      "0080",
      "0081",
      "0082",
      "0085",
      "0090",
    ]);
    expect(mapPropertyClass("0010")).toMatchObject({
      officialLabel: "Rural Non-Farmland with Improvements",
      propertyUsageType: "Residential",
    });
    expect(mapPropertyClass("0011").propertyUsageType).toBe("Agricultural");
    expect(mapPropertyClass("0028").propertyUsageType).toBe("Conservation");
    expect(mapPropertyClass("0029").propertyUsageType).toBe("TimberLand");
    expect(mapPropertyClass("0032").propertyUsageType).toBe("Residential");
    expect(mapPropertyClass("0052").propertyUsageType).toBe("Commercial");
    expect(mapPropertyClass("0065").propertyUsageType).toBe("Commercial");
    expect(mapPropertyClass("0072").propertyUsageType).toBe("Commercial");
    expect(mapPropertyClass("0082").propertyUsageType).toBe("Industrial");
    expect(mapPropertyClass("0085").propertyUsageType).toBe("Industrial");
  });

  it("keeps ambiguous and unpublished class codes unknown without losing raw provenance", () => {
    expect(mapPropertyClass("0020")).toMatchObject({
      officialLabel: "Rural Non-Farmland Vacant",
      propertyUsageType: "Unknown",
      dictionaryStatus: "authoritative_definition",
    });
    expect(mapPropertyClass("0090")).toMatchObject({
      officialLabel: "Tax Exempt",
      propertyUsageType: "Unknown",
      dictionaryStatus: "authoritative_definition",
    });
    for (const rawCode of [
      "0000",
      "0026",
      "4600",
      "5000",
      "80NE",
      "81NE",
      "9999",
    ]) {
      expect(mapPropertyClass(rawCode)).toMatchObject({
        rawCode,
        officialLabel: null,
        propertyUsageType: "Unknown",
        dictionaryStatus: "unmapped_source_code",
      });
    }
    expect(mapPropertyClass(" 0080 ")).toMatchObject({
      rawCode: "0080",
      propertyUsageType: "Industrial",
    });
    expect(mapPropertyClass(null)).toMatchObject({
      rawCode: null,
      dictionaryStatus: "missing_source_code",
      propertyUsageType: "Unknown",
    });
  });

  it("uses the lowest OBJECTID when duplicate PIN records disagree", () => {
    const selected = selectPrimaryFeature([
      { properties: { OBJECTID: 20, EAV: 200 }, geometry: null },
      { properties: { OBJECTID: 10, EAV: 100 }, geometry: null },
    ]);

    expect(selected.properties.EAV).toBe(100);
  });

  it("fails closed if a capture includes prohibited identity fields", () => {
    for (const fieldName of [
      "owner1_name",
      "Owner_city",
      "Owner_State",
      "Owner_Zip",
      "Taxbill_name",
      "Taxbill_addr",
      "Taxbill_CS",
      "Taxbill_Zip",
    ]) {
      expect(() =>
        assertNoPiiFields([
          {
            properties: {
              PIN: "0012345678",
              [fieldName]: "DO NOT RETAIN",
            },
            geometry: null,
          },
        ]),
      ).toThrow(/Prohibited PII field/);
    }
  });

  it("does not guess an unknown suffix into a lexicon enum", () => {
    expect(parseSiteAddress("12 COUNTY ROAD 4")).toEqual({
      street_number: "12",
      street_name: "COUNTY ROAD 4",
    });
  });

  it("normalizes ArcGIS epoch dates and rejects invalid dates", () => {
    expect(readDate(1_613_260_800_000)).toBe("2021-02-14");
    expect(readDate("not-a-date")).toBeNull();
  });

  it("always emits a property while omitting unsupported source-null children", () => {
    const entities = buildAppraisalEntities(
      preparedPayload([
        {
          OBJECTID: 1,
          PIN: "0012345678",
          site_address: null,
          site_csz: null,
          Site_City: null,
          Site_State: null,
          Site_Zip: null,
          GIS_acres_num: null,
          gross_acres: null,
          class: null,
          EAV: null,
          EMV: null,
          YRBuilt: null,
          TOTSQFT: null,
        },
      ]),
    );

    expect(entities.property).toMatchObject({
      parcel_identifier: "0012345678",
      property_type: "LandParcel",
      property_usage_type: "Unknown",
      property_legal_description_text: null,
    });
    expect(entities.address).toBeNull();
    expect(entities.lot).toBeNull();
    expect(entities.tax).toBeNull();
    expect(entities.sourcePayload).toMatchObject({
      request_identifier: "0012345678",
      response: {
        features: [{ properties: { PIN: "0012345678", EAV: null } }],
      },
    });
  });

  it("emits every MultiPolygon component and retains exact source topology", () => {
    const geometry = {
      type: "MultiPolygon",
      coordinates: [
        [
          [
            [-90.5, 41.5],
            [-90.4, 41.5],
            [-90.4, 41.6],
            [-90.5, 41.5],
          ],
          [
            [-90.48, 41.52],
            [-90.46, 41.52],
            [-90.46, 41.54],
            [-90.48, 41.52],
          ],
        ],
        [
          [
            [-90.3, 41.4],
            [-90.2, 41.4],
            [-90.2, 41.5],
            [-90.3, 41.4],
          ],
        ],
      ],
    };
    const payload = preparedPayload(
      [{ OBJECTID: 1, PIN: "0012345678" }],
      [geometry],
    );
    const entities = buildAppraisalEntities(payload);

    expect(entities.geometries).toHaveLength(2);
    expect(entities.geometries[0].polygon).toEqual([
      { latitude: 41.5, longitude: -90.5 },
      { latitude: 41.5, longitude: -90.4 },
      { latitude: 41.6, longitude: -90.4 },
      { latitude: 41.5, longitude: -90.5 },
    ]);
    expect(entities.sourcePayload.response).toMatchObject({
      features: [{ geometry }],
    });
    expect(
      buildGeometryEntities(
        [
          {
            properties: { OBJECTID: 1 },
            geometry,
          },
        ],
        { method: "GET", url: "https://example.com/query" },
      ),
    ).toHaveLength(2);
  });
});
