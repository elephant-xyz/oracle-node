import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  assertQueryTableIds,
  formatOwnerName,
  isCompleteDuvalParcel,
  loadDuvalParcelArtifacts,
  parseUnnormalizedAddress,
  pickLatestSale,
  pickLatestTax,
  rowFromDuvalArtifacts,
} from "../../scripts/duval/query-table-lib.mjs";
import { exportDuvalQueryTable } from "../../scripts/export-duval-query-table.mjs";

describe("Duval query-table mapping", () => {
  it("parses COJ unnormalized site addresses without numeric-coercing the folio", () => {
    expect(
      parseUnnormalizedAddress("N US 301 HWY, JACKSONVILLE FL 32234"),
    ).toEqual({
      street: "N US 301 HWY",
      city: "JACKSONVILLE",
      zip: "32234",
    });
    expect(
      formatOwnerName({ first_name: "Forest", last_name: "Rayonier" }),
    ).toBe("Forest Rayonier");
    expect(formatOwnerName({ name: "Rayonier Forest Resources" })).toBe(
      "Rayonier Forest Resources",
    );
  });

  it("picks the latest tax year and latest sale date", () => {
    expect(
      pickLatestTax([
        { tax_year: 2024, property_assessed_value_amount: 1 },
        { tax_year: 2025, property_assessed_value_amount: 2 },
      ]),
    ).toMatchObject({ tax_year: 2025, property_assessed_value_amount: 2 });
    expect(
      pickLatestSale([
        { ownership_transfer_date: "2019-01-01", purchase_price_amount: 10 },
        { ownership_transfer_date: "2021-10-05", purchase_price_amount: 100 },
      ]),
    ).toMatchObject({
      ownership_transfer_date: "2021-10-05",
      purchase_price_amount: 100,
    });
  });

  it("maps transform artifacts onto the Montgomery query-table contract", () => {
    const row = rowFromDuvalArtifacts({
      folio: "0000010005",
      seed: {
        parcel_id: "0000010005",
        request_identifier: "0000010005R",
      },
      property: {
        property_type: "LandParcel",
        property_usage_type: "TimberLand",
        subdivision: "00000 SECTION LAND",
        total_area: "3198369",
      },
      address: {
        unnormalized_address: "N US 301 HWY, JACKSONVILLE FL 32234",
      },
      geometry: { latitude: 30.353, longitude: -81.957 },
      lot: { lot_area_sqft: 3198369 },
      taxes: [{ tax_year: 2024, property_market_value_amount: 521850 }],
      sales: [],
      owners: [{ first_name: "Forest", last_name: "Rayonier" }],
      structure: null,
    });
    expect(row.parcel_identifier).toBe("0000010005");
    expect(row.request_identifier).toBe("0000010005R");
    expect(row.county_name).toBe("Duval");
    expect(row.market_value).toBe(521850);
    expect(row.avm_value).toBeNull();
    expect(row.hoa_flag).toBeNull();
    expect(row.has_permits).toBe(false);
    expect(row.permit_count).toBe(0);
    expect(row.owner_name).toBe("Forest Rayonier");
    expect(row.lot_size_acre).toBeCloseTo(3198369 / 43_560);
    expect(row.source_system).toBe("duval_appraiser");
    expect(row.state_code).toBe("FL");
    expect(row.address_street).toBe("N US 301 HWY");
    expect(row.property_usage_type).toBe("TimberLand");
    expect(row.owner_count).toBe(1);
  });

  it("falls back to capture address and parcel geometry when transform files are thin", () => {
    const row = rowFromDuvalArtifacts({
      folio: "0000010005",
      seed: { parcel_id: "0000010005" },
      address: { unnormalized_address: null },
      captureAddress: {
        full_address: "N US 301 HWY, JACKSONVILLE FL 32234",
      },
      geometry: { latitude: null, longitude: null },
      geometryParcels: [{ latitude: 30.1, longitude: -81.9 }],
      taxes: [],
      sales: [],
      owners: [],
    });
    expect(row.address_city).toBe("JACKSONVILLE");
    expect(row.latitude).toBe(30.1);
    expect(row.longitude).toBe(-81.9);
  });

  it("rejects a row with no parcel_id", () => {
    expect(() => rowFromDuvalArtifacts({ folio: "  ", seed: {} })).toThrow(
      /missing parcel_id/,
    );
  });

  it("rejects duplicate or missing parcel identifiers", () => {
    expect(() =>
      assertQueryTableIds(
        [
          { parcel_identifier: "0000010005" },
          { parcel_identifier: "0000010005" },
        ],
        2,
      ),
    ).toThrow(/duplicate/);
    expect(() => assertQueryTableIds([{ parcel_identifier: "" }], 1)).toThrow(
      /empty/,
    );
    expect(() =>
      assertQueryTableIds([{ parcel_identifier: "0000010005" }], 50),
    ).toThrow(/!= 50/);
    expect(() =>
      assertQueryTableIds(
        Array.from({ length: 50 }, (_, index) => ({
          parcel_identifier: String(index).padStart(10, "0"),
        })),
      ),
    ).not.toThrow();
  });

  it("loads disk artifacts and skips incomplete parcel directories", async () => {
    const root = mkdtempSync(join(tmpdir(), "duval-query-"));
    try {
      const complete = join(root, "0000010005");
      mkdirSync(join(complete, "data"), { recursive: true });
      writeFileSync(join(complete, "transformed_output.zip"), "zip");
      writeFileSync(
        join(complete, "property_seed.json"),
        JSON.stringify({
          parcel_id: "0000010005",
          request_identifier: "0000010005R",
        }),
      );
      writeFileSync(
        join(complete, "data", "property.json"),
        JSON.stringify({ property_type: "LandParcel" }),
      );
      writeFileSync(
        join(complete, "data", "address.json"),
        JSON.stringify({ unnormalized_address: null }),
      );
      writeFileSync(
        join(complete, "unnormalized_address.json"),
        JSON.stringify({
          full_address: "N US 301 HWY, JACKSONVILLE FL 32234",
        }),
      );
      writeFileSync(
        join(complete, "data", "company_1.json"),
        JSON.stringify({ name: "Acme LLC" }),
      );
      writeFileSync(
        join(complete, "data", "person_1.json"),
        JSON.stringify({ first_name: "Ada", last_name: "Lovelace" }),
      );
      writeFileSync(
        join(complete, "data", "tax_1.json"),
        JSON.stringify({ tax_year: 2025, property_market_value_amount: 9 }),
      );
      const hollow = join(root, "0000020010");
      mkdirSync(hollow, { recursive: true });
      writeFileSync(
        join(hollow, "property_seed.json"),
        JSON.stringify({ parcel_id: "0000020010" }),
      );

      expect(await isCompleteDuvalParcel(complete)).toBe(true);
      expect(await isCompleteDuvalParcel(hollow)).toBe(false);
      const artifacts = await loadDuvalParcelArtifacts(complete, "0000010005");
      expect(artifacts.owners[0]).toMatchObject({
        first_name: "Ada",
        last_name: "Lovelace",
      });
      const row = rowFromDuvalArtifacts(artifacts);
      expect(row.address_street).toBe("N US 301 HWY");
      expect(row.market_value).toBe(9);
      expect(row.owner_name).toBe("Ada Lovelace");

      const parquetPath = join(root, "out.parquet");
      const exported = await exportDuvalQueryTable({
        pilotRoot: root,
        parquetPath,
        limit: 1,
      });
      expect(exported.rowCount).toBe(1);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
