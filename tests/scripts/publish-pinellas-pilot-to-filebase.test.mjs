import { describe, expect, it } from "vitest";

import {
  assertProductionPublishRowCount,
  buildPinellasPilotCoverage,
  fillDerivedFilebaseToken,
  hasFilebaseCredentials,
  mapTransformedFilesToQueryTableRow,
  ownerNameFromRecord,
  parseCliOptions,
  parseUnnormalizedAddress,
  propertyIdForStrap,
  readTransformedZipJsonFiles,
  toNumber,
  toParquetRecord,
  toText,
} from "../../scripts/publish-pinellas-pilot-to-filebase.mjs";

describe("Pinellas local-zip query-table mapping", () => {
  it("splits a Pinellas situs line into street, city, and ZIP", () => {
    expect(
      parseUnnormalizedAddress("1403 CIRCLE DR, TARPON SPRINGS FL 34689"),
    ).toEqual({
      street: "1403 CIRCLE DR",
      city: "TARPON SPRINGS",
      postalCode: "34689",
    });
    expect(parseUnnormalizedAddress("")).toEqual({
      street: null,
      city: null,
      postalCode: null,
    });
  });

  it("maps transform JSON files onto the MCP query-table columns", () => {
    const row = mapTransformedFilesToQueryTableRow({
      strap: "152703878580000500",
      seedRow: {
        parcel_id: "152703878580000500",
        city: "TARPON SPRINGS",
        zip: "34689",
        acres: "0.89",
        latitude: "28.16",
        longitude: "-82.78",
      },
      files: {
        "property.json": {
          parcel_identifier: "03-27-15-87858-000-0500",
          property_type: "LandParcel",
          property_usage_type: "Residential",
          subdivision: "CIRCLE DRIVE",
        },
        "unnormalized_address.json": {
          full_address: "1403 CIRCLE DR, TARPON SPRINGS FL 34689",
        },
        "lot.json": { lot_area_sqft: "38768" },
        "geometry.json": { latitude: 28.1645, longitude: -82.7835 },
        "tax_2025.json": {
          tax_year: 2025,
          property_assessed_value_amount: "100",
          property_market_value_amount: "200",
          property_land_amount: "50",
        },
        "tax_2026.json": {
          tax_year: 2026,
          property_assessed_value_amount: "111",
          property_market_value_amount: "222",
          property_land_amount: "55",
        },
        "sales_history_1.json": {
          ownership_transfer_date: "2019-01-01",
          purchase_price_amount: "10",
        },
        "sales_history_2.json": {
          ownership_transfer_date: "2021-06-15",
          purchase_price_amount: "250000",
        },
        "person_1.json": { first_name: "JANE", last_name: "DOE" },
        "company_1.json": { name: "ACME LLC" },
        "property_improvement_1.json": { permit_number: "B-1" },
      },
    });

    expect(row.request_identifier).toBe("152703878580000500");
    expect(row.source_system).toBe("pinellas_appraiser");
    expect(row.county_name).toBe("Pinellas");
    expect(row.state_code).toBe("FL");
    expect(row.address_street).toBe("1403 CIRCLE DR");
    expect(row.address_city).toBe("TARPON SPRINGS");
    expect(row.address_zip).toBe("34689");
    expect(row.latitude).toBe(28.1645);
    expect(row.longitude).toBe(-82.7835);
    expect(row.assessed_value).toBe(111);
    expect(row.last_sale_date).toBe("2021-06-15");
    expect(row.last_sale_price).toBe(250000);
    expect(row.owner_name).toBe("JANE DOE");
    expect(row.owners_text).toContain("ACME LLC");
    expect(row.owner_count).toBe(2);
    expect(row.has_permits).toBe(true);
    expect(row.permit_count).toBe(1);
    expect(row.has_sunbiz_tenant).toBe(false);
    expect(row.hoa_flag).toBeNull();
    expect(row.property_id).toBe(propertyIdForStrap("152703878580000500"));
    expect(toParquetRecord(row).hoa_flag).toBeUndefined();
  });

  it("builds coverage with appraisal ingested_count equal to the parquet row count", () => {
    const snapshot = buildPinellasPilotCoverage({
      ingestedCount: 311566,
      expectedCount: 311566,
      exportedAt: "2026-09-01T21:13:14.191Z",
    });
    expect(snapshot.county).toBe("pinellas");
    expect(snapshot.datasets).toHaveLength(1);
    expect(snapshot.datasets[0]).toMatchObject({
      source: "appraisal",
      ingested_count: 311566,
      expected_count: 311566,
    });
    expect(snapshot.datasets[0]).not.toHaveProperty("publicationScope");
  });

  it("parses publish flags and reports Filebase credential presence without values", () => {
    expect(parseCliOptions(["--dry-run", "--no-publish"]).dryRun).toBe(true);
    expect(parseCliOptions(["--no-publish"]).publish).toBe(false);
    expect(parseCliOptions(["--allow-missing"]).allowMissing).toBe(true);
    expect(parseCliOptions([]).allowPilotOverwrite).toBe(false);
    expect(
      parseCliOptions(["--allow-pilot-overwrite"]).allowPilotOverwrite,
    ).toBe(true);
    expect(() => assertProductionPublishRowCount(50, false)).toThrow(
      /Refusing to publish 50 Pinellas rows/,
    );
    expect(() => assertProductionPublishRowCount(50, true)).not.toThrow();
    expect(() => assertProductionPublishRowCount(311566, false)).not.toThrow();
    expect(hasFilebaseCredentials({})).toBe(false);
    const env = {
      S3_ACCESS_KEY_ID: "AKIAEXAMPLE",
      S3_SECRET_ACCESS_KEY: "secret",
    };
    fillDerivedFilebaseToken(env);
    expect(typeof env.FILEBASE_API_TOKEN).toBe("string");
    expect(env.FILEBASE_API_TOKEN?.length ?? 0).toBeGreaterThan(8);
    expect(hasFilebaseCredentials(env)).toBe(true);
  });

  it("reads owner names from name or first/last fields", () => {
    expect(ownerNameFromRecord({ name: "ACME LLC" })).toBe("ACME LLC");
    expect(ownerNameFromRecord({ first_name: "A", last_name: "B" })).toBe(
      "A B",
    );
    expect(toText("  x  ")).toBe("x");
    expect(toNumber("12.5")).toBe(12.5);
    expect(toNumber("")).toBeNull();
  });

  it("reads data JSON from a transformed zip and skips relationship files", async () => {
    const { mkdtemp, rm } = await import("node:fs/promises");
    const os = await import("node:os");
    const path = await import("node:path");
    const { createRequire } = await import("node:module");
    const require = createRequire(import.meta.url);
    const AdmZip = require("adm-zip");
    const dir = await mkdtemp(path.join(os.tmpdir(), "pinellas-zip-"));
    const zipPath = path.join(dir, "transformed.zip");
    const zip = new AdmZip();
    zip.addFile(
      "data/property.json",
      Buffer.from(JSON.stringify({ parcel_identifier: "x" })),
    );
    zip.addFile("data/relationship_1.json", Buffer.from("{}"));
    zip.writeZip(zipPath);
    const files = await readTransformedZipJsonFiles(zipPath);
    expect(files["property.json"]).toEqual({ parcel_identifier: "x" });
    expect(files["relationship_1.json"]).toBeUndefined();
    await rm(dir, { recursive: true, force: true });
  });
});
