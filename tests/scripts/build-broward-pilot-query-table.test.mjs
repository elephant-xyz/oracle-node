import { describe, expect, it } from "vitest";

import { buildQueryTableRow } from "../../scripts/build-broward-pilot-query-table.mjs";

describe("Broward pilot Donphan query table", () => {
  it("maps transformed and source facts into the stable query schema", () => {
    const row = buildQueryTableRow({
      folio: "504108BJ0140",
      property: {
        parcel_identifier: "504108BJ0140",
        property_type: "Unit",
        property_usage_type: "Residential",
        property_structure_built_year: 1975,
        livable_floor_area: "1210",
      },
      address: {
        county_name: "Broward",
        latitude: 26.10864266,
        longitude: -80.27918202,
      },
      lot: { lot_area_sqft: 43560 },
      tax: {
        property_assessed_value_amount: 250000,
        property_market_value_amount: 300000,
        property_land_amount: 50000,
      },
      structure: { roof_covering_material: "Tile" },
      sale: {
        ownership_transfer_date: "2024-01-02",
        purchase_price_amount: 275000,
      },
      sourceRecord: {
        situsAddress1: "958 MOCKINGBIRD LANE # 513",
        situsCity: "PLANTATION",
        situsZipCode: "33324",
      },
      ownerNames: ["Example Owner"],
    });

    expect(row).toMatchObject({
      property_id: "broward:504108BJ0140",
      parcel_identifier: "504108BJ0140",
      source_system: "broward_appraiser",
      county_name: "Broward",
      address_city: "PLANTATION",
      address_zip: "33324",
      lot_size_acre: 1,
      property_usage_type: "Residential",
      market_value: 300000,
      owner_count: 1,
      last_sale_date: "2024-01-02",
      has_permits: false,
    });
  });
});
