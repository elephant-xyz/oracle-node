import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";

import { describe, expect, it } from "vitest";

import {
  POLK_EXPORT_SCHEMA_VERSION,
  assertCheckpointCompatible,
  buildConsolidatedProperty,
  buildQueryTableRow,
  classifyPropertyUsage,
  createCheckpoint,
  deterministicPropertyId,
  normalizeDate,
  normalizeParcelIdentifier,
  normalizePolkBuildingCsvLine,
  normalizePolkLegalCsvLine,
  normalizePolkPermitCsvRecord,
  propertyRelativePath,
  readCheckpoint,
  readYear,
  sanitizePublicDescription,
  scanPublicProperty,
  stableJson,
  writeCheckpoint,
} from "../../scripts/polk-local-appraisal-lib.mjs";
import { parsePolkCliOptions } from "../../scripts/build-polk-local-appraisal.mjs";

/**
 * Build a synthetic batch-bounded Polk source bundle.
 *
 * The synthetic sales row deliberately carries source-only party keys at
 * runtime to prove the closed builder ignores them.
 *
 * @returns {import("../../scripts/polk-local-appraisal-lib.mjs").PolkPropertySourceBundle} Source bundle.
 */
function sourceBundle() {
  return {
    parcel: {
      parcel_id: "25-30-06-000000-023190",
      dor_use_code: "2800",
      property_type: "MHP",
      property_type_detail: "MHP - Family Park; Lot Only",
      neighborhood_code: "123",
      neighborhood_description: "BARTOW",
      land_value: "220011",
      building_value: "103202",
      extra_feature_value: "1500",
      market_value: "2818168",
      assessed_value: "1924255",
      taxable_value: "1924255",
      yearly_tax_amount: "39690.66",
      millage_rate: "12.1",
      year_created: "1960",
      year_improved: "1962",
      last_inspection_date: "2026-01-01 00:00:00",
      total_acreage: "2.5",
      related_parcel_identifier: null,
      subdivision_code: "000000",
      subdivision_name: "EXAMPLE SUBDIVISION",
    },
    sites: [
      {
        parcel_id: "253006000000023190",
        line_number: "1",
        building_number: "1",
        street: "DAVIDSON ST",
        street_prefix: null,
        street_number: "780",
        street_number_suffix: null,
        street_suffix: null,
        street_suffix_direction: "W",
        unit: null,
        postal_code: "33830",
        city: "BARTOW",
      },
    ],
    sales: [
      {
        parcel_id: "253006000000023190",
        sale_id: "1",
        line_number: "1",
        sale_date: "04/01/1975",
        price: "165000",
        book: "10",
        page: "20",
        sale_type: "E",
        transfer_code: "Q",
        transfer_description: "QUIT CLAIM",
        instrument_type: "03",
        instrument_description: "OTHER",
        foreclosure: "N",
        grantor: "PRIVATE GRANTOR",
        grantee: "PRIVATE GRANTEE",
      },
    ],
    buildings: [
      {
        parcel_id: "253006000000023190",
        building_number: "1",
        improvement_type: "SF",
        improvement_description: "Single Family",
        style: "R5",
        style_description: "R5 CLASS",
        stories: "1",
        shape: "5",
        shape_description: "RECTANGLE",
        class_code: "0",
        class_description: "NONE",
        bathrooms: "2",
        units: "1",
        bedrooms: "3",
        fireplaces: "1",
        substructure_description: "Continuous Wall",
        frame_description: "MASONRY/BLOCK",
        effective_year: "2000",
        built_year: "1999",
        exterior_wall_description: "HARDY BOARD",
        roof_description: "GABLE-METAL",
        floor_description: "HARD TILE",
        interior_wall_description: "DRYWALL",
        living_area: "1280",
        total_under_roof: "1800",
        traverse: "BAS:0,0:=N10 E10 S10 W10 $",
      },
    ],
    layouts: [
      {
        parcel_id: "253006000000023190",
        building_number: "1",
        line_number: "1",
        code: "BAS",
        description: "BASE AREA",
        actual_area: "1280",
        heated_area: "1280",
      },
    ],
    lands: [
      {
        parcel_id: "253006000000023190",
        line_number: "1",
        land_type: "C",
        use_code: "0130",
        use_description: "Residential",
        frontage: "100",
        depth: "200",
        units: "2.5",
        unit_type: "A",
        unit_type_description: "ACREAGE",
        influence_code: null,
        influence_description: null,
      },
    ],
    legalDescriptions: [
      {
        parcel_id: "253006000000023190",
        line_number: "1",
        description: "LOT 1",
      },
      {
        parcel_id: "253006000000023190",
        line_number: "2",
        description: "BLOCK 2",
      },
    ],
    permits: [
      {
        parcel_id: "253006000000023190",
        permit_id: "1",
        agency_name: "POLK COUNTY",
        permit_number: "20260001",
        status: "C",
        status_description: "Complete",
        description: "CONTACT person@example.com FOR ACCESS",
        permit_type: "RES ADDN",
        issue_date: "2026-06-20 00:00:00",
        final_date: "1899-12-30 00:00:00",
        year: "2026",
        estimated_value: "10000",
        certificate_of_occupancy_date: "2026-07-01 00:00:00",
      },
    ],
    collectedAt: "2026-08-28T10:00:00.000Z",
  };
}

describe("Polk local appraisal normalizers", () => {
  it("normalizes parcel ids, dates, usage, and descriptions deterministically", () => {
    expect(normalizeParcelIdentifier("25-30-06 000000-023190")).toBe(
      "253006000000023190",
    );
    expect(normalizeDate("08/01/1997")).toBe("1997-08-01");
    expect(normalizeDate("1899-12-30 00:00:00")).toBeNull();
    expect(readYear("0")).toBeNull();
    expect(readYear("2025")).toBe(2025);
    expect(classifyPropertyUsage("6002", "Pasture w/Res.")).toBe("RES");
    expect(classifyPropertyUsage("2800", "Parking/Mobile Home Park")).toBe(
      "COM",
    );
    expect(sanitizePublicDescription("ordinary reroof")).toBe(
      "ordinary reroof",
    );
    expect(sanitizePublicDescription("email person@example.com")).toBeNull();
  });

  it("repairs official malformed building, legal, and permit CSV records", () => {
    expect(normalizePolkBuildingCsvLine('"parcel","traverse""\r')).toBe(
      '"parcel","traverse"\r',
    );
    expect(normalizePolkBuildingCsvLine('"parcel",""\r')).toBe('"parcel",""\r');
    expect(
      normalizePolkLegalCsvLine(
        '"parcel","1","01","26","22","000000","011000","N 24 DEG 15\' 30" E"',
        2,
      ),
    ).toBe(
      '"parcel","1","01","26","22","000000","011000","N 24 DEG 15\' 30"" E"',
    );
    expect(
      normalizePolkPermitCsvRecord(
        '"parcel","1","POLK","P-1","C","Complete","INSTALL 3" ROOF","RES ADDN","2026-01-01","","2026","100","","1","","MAIN","ST",""',
        2,
      ),
    ).toContain('"INSTALL 3"" ROOF"');
  });

  it("derives stable UUIDv5 ids and batch-independent property paths", () => {
    const first = deterministicPropertyId("253006000000023190");
    const second = deterministicPropertyId("25-30-06-000000-023190");
    expect(first).toBe(second);
    expect(first).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    expect(deterministicPropertyId("253006000000023191")).not.toBe(first);
    expect(propertyRelativePath("253006000000023190")).toMatch(
      /^properties\/[0-9a-f]{2}\/253006000000023190\.json$/,
    );
  });
});

describe("Polk public property and query-table builders", () => {
  it("maps all required bulk detail while excluding private party data", () => {
    const property = buildConsolidatedProperty(sourceBundle());
    const rendered = stableJson(property);

    expect(property.address).toEqual({
      city: "BARTOW",
      latitude: null,
      longitude: null,
      postalCode: "33830",
      state: "FL",
      street: "780 DAVIDSON ST W",
    });
    expect(property.property).toMatchObject({
      builtYear: 1999,
      legalDescription: "LOT 1 BLOCK 2",
      livableArea: 1280,
      propertyType: "MHP - Family Park; Lot Only",
      usageType: "COM",
    });
    expect(property.sales).toHaveLength(1);
    expect(property.structures).toHaveLength(1);
    expect(property.layouts).toHaveLength(1);
    expect(property.lots).toHaveLength(1);
    expect(property.permits).toHaveLength(1);
    expect(property.permits[0].projectDescription).toBeNull();
    expect(property.ownerships).toEqual([]);
    expect(property.deeds).toEqual([]);
    expect(rendered).not.toMatch(
      /PRIVATE GRANTOR|PRIVATE GRANTEE|person@example\.com/i,
    );
    expect(scanPublicProperty(property)).toEqual([]);
  });

  it("builds the modern scalar query-table row with owner fields null", () => {
    const property = buildConsolidatedProperty(sourceBundle());
    const row = buildQueryTableRow(property, "bafy-test-cid");

    expect(row).toMatchObject({
      property_cid: "bafy-test-cid",
      request_identifier: "253006000000023190",
      parcel_identifier: "253006000000023190",
      address_street: "780 DAVIDSON ST W",
      lot_size_acre: 2.5,
      lot_area_sqft: 108900,
      exterior_wall_material: "HARDY BOARD",
      roof_covering_material: "GABLE-METAL",
      built_year: 1999,
      livable_floor_area: 1280,
      assessed_value: 1924255,
      market_value: 2818168,
      has_permits: true,
      permit_count: 1,
      owner_name: null,
      owners_text: null,
      owner_count: null,
      owner_occupied: null,
    });
  });

  it("rejects a public object carrying owner data", () => {
    const property = buildConsolidatedProperty(sourceBundle());
    expect(
      scanPublicProperty({ ...property, ownerName: "PRIVATE PERSON" }),
    ).toEqual(["forbidden_key:$.ownerName"]);
  });

  it("allows SSN-shaped public record numbers but rejects unstructured SSNs", () => {
    expect(scanPublicProperty({ permitNumber: "DA 181-01-1078" })).toEqual([]);
    expect(scanPublicProperty({ note: "181-01-1078" })).toEqual(["ssn:$.note"]);
  });
});

describe("Polk checkpoint behavior", () => {
  it("atomically round-trips a checkpoint and rejects changed resume options", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "polk-checkpoint-"));
    const checkpointPath = path.join(directory, ".state", "checkpoint.json");
    const checkpoint = createCheckpoint({
      sourceFingerprint: "source-a",
      inputDirectory: "/bulk",
      batchSize: 250,
      limit: 1000,
      startedAt: "2026-08-28T10:00:00.000Z",
    });
    try {
      await writeCheckpoint(checkpointPath, checkpoint);
      const loaded = await readCheckpoint(checkpointPath);
      expect(loaded).toEqual(checkpoint);
      expect(loaded?.schemaVersion).toBe(POLK_EXPORT_SCHEMA_VERSION);
      assertCheckpointCompatible(checkpoint, {
        sourceFingerprint: "source-a",
        inputDirectory: "/bulk",
        batchSize: 250,
        limit: 1000,
      });
      expect(() =>
        assertCheckpointCompatible(checkpoint, {
          sourceFingerprint: "source-a",
          inputDirectory: "/bulk",
          batchSize: 500,
          limit: 1000,
        }),
      ).toThrow(/batch size changed/);
      expect(await readdir(path.dirname(checkpointPath))).toEqual([
        "checkpoint.json",
      ]);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
});

describe("Polk local CLI", () => {
  it("supports explicit limits and batch sizes", () => {
    const options = parsePolkCliOptions([
      "--input-dir",
      "/tmp/polk/input",
      "--out",
      "/tmp/polk/output",
      "--work-db",
      "/tmp/polk/work.duckdb",
      "--limit",
      "1000",
      "--batch-size",
      "250",
    ]);
    expect(options).toEqual({
      inputDirectory: "/tmp/polk/input",
      outputDirectory: "/tmp/polk/output",
      workDatabase: "/tmp/polk/work.duckdb",
      limit: 1000,
      batchSize: 250,
      restart: false,
    });
  });
});
