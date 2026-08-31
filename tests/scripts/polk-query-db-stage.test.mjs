import { describe, expect, it } from "vitest";

import {
  BULK_STAGE_HEADER,
  POLK_APPRAISAL_SOURCE_SYSTEM,
  POLK_PERMIT_SOURCE_SYSTEM,
  buildPolkAppraisalStageSql,
  buildPolkPermitStageSql,
  createPolkAppraisalPreparedRows,
  createPolkPermitPreparedRows,
  normalizeQueryDbAddress,
  parsePolkQueryDbStageOptions,
  serializePolkBulkStageRow,
} from "../../scripts/polk/query-db-stage.mjs";

describe("Polk query-db stage options", () => {
  it("selects deterministic track-specific defaults", () => {
    expect(parsePolkQueryDbStageOptions(["--track", "appraisal"])).toEqual({
      track: "appraisal",
      workDatabase: "tmp/polk/bulk/extracted/polk-appraisal.duckdb",
      output: "tmp/polk/neon/appraisal-stage/appraisal.csv",
      manifest: "tmp/polk/neon/appraisal-stage/appraisal.csv.manifest.json",
      limit: null,
    });
    expect(
      parsePolkQueryDbStageOptions(["--track", "permits", "--limit", "25"]),
    ).toMatchObject({
      track: "permits",
      limit: 25,
    });
  });

  it("rejects unsupported tracks and invalid caps", () => {
    expect(() => parsePolkQueryDbStageOptions(["--track", "sunbiz"])).toThrow(
      /appraisal or permits/,
    );
    expect(() =>
      parsePolkQueryDbStageOptions(["--track", "appraisal", "--limit", "0"]),
    ).toThrow(/positive integer/);
  });
});

describe("Polk query-db source queries", () => {
  it("selects one deterministic site and building per appraisal parcel", () => {
    const sql = buildPolkAppraisalStageSql(50);
    expect(sql).toContain("FROM polk_parcels p");
    expect(sql).toContain("row_number() OVER");
    expect(sql).toContain("principal_site.site_rank = 1");
    expect(sql).toContain("principal_building.building_rank = 1");
    expect(sql).toContain("LIMIT 50");
  });

  it("preserves every official permit row and optional parcel identity", () => {
    const sql = buildPolkPermitStageSql(null);
    expect(sql).toContain("FROM polk_permits");
    expect(sql).toContain('AS "parcelIdentifier"');
    expect(sql).not.toContain("LIMIT");
  });
});

describe("Polk query-db appraisal mapping", () => {
  const appraisalRecord = {
    parcelIdentifier: "22-26-01-000010-000390",
    propertyType: "SINGLE FAMILY",
    propertyTypeDetail: "RESIDENTIAL",
    zoning: "RL-1",
    subdivision: "TEST",
    legalDescription: "LOT 1",
    builtYear: "2001",
    effectiveYear: "2010",
    livingArea: "1800",
    totalArea: "2300",
    numberOfUnits: "1",
    streetPrefix: "N",
    streetNumber: "123",
    streetNumberSuffix: null,
    streetName: "MAIN",
    streetSuffix: "ST",
    streetPostDirectional: null,
    unitIdentifier: "2",
    cityName: "LAKELAND",
    postalCode: "33801-1234",
  };

  it("creates linked parcel, address, and property rows without owner PII", () => {
    const rows = createPolkAppraisalPreparedRows(appraisalRecord);
    expect(rows.map((row) => row.tableName)).toEqual([
      "parcels",
      "addresses",
      "properties",
    ]);
    const property = rows[2];
    expect(property.references).toEqual({
      parcelSourceRecordKey:
        "polk_appraiser:222601000010000390:parcel:property_seed",
      addressSourceRecordKey: "polk_appraiser:222601000010000390:address:site",
    });
    expect(property.values).toMatchObject({
      source_system: POLK_APPRAISAL_SOURCE_SYSTEM,
      parcel_identifier: "222601000010000390",
      property_structure_built_year: 2001,
      livable_floor_area: "1800",
    });
    expect(JSON.stringify(rows)).not.toMatch(/owner|mailing/i);
  });

  it("uses query-db-compatible normalized address hashes", () => {
    expect(normalizeQueryDbAddress("123 N. Main St., Lakeland, FL 33801")).toBe(
      "123 n main st lakeland fl 33801",
    );
    expect(rowsAddressHash()).toMatch(/^[a-f0-9]{64}$/);
  });

  function rowsAddressHash() {
    const rows = createPolkAppraisalPreparedRows(appraisalRecord);
    return rows[1].values.normalized_address_hash;
  }
});

describe("Polk query-db permit mapping", () => {
  it("links official permits to deterministic appraisal parents", () => {
    const rows = createPolkPermitPreparedRows({
      parcelIdentifier: "22-26-01-000010-000390",
      permitIdentifier: "123",
      permitNumber: "2026-000123",
      agencyName: "POLK COUNTY",
      status: "ISSUED",
      statusDescription: null,
      description: "ROOF REPLACEMENT",
      permitType: "BUILDING",
      issueDate: "08/15/2026",
      finalDate: null,
      estimatedValue: "12500",
      certificateOfOccupancyDate: null,
    });
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      tableName: "property_improvements",
      references: {
        parcelSourceRecordKey:
          "polk_appraiser:222601000010000390:parcel:property_seed",
        propertySourceRecordKey:
          "polk_appraiser:222601000010000390:property:property",
      },
      values: {
        source_system: POLK_PERMIT_SOURCE_SYSTEM,
        permit_number: "2026-000123",
        permit_issue_date: "2026-08-15",
        estimated_job_value: 12500,
      },
    });
  });
});

describe("Polk query-db CSV serialization", () => {
  it("matches the generic bulk-stage contract", () => {
    const row = createPolkPermitPreparedRows({
      parcelIdentifier: null,
      permitIdentifier: "1",
      permitNumber: "P-1",
      agencyName: "POLK COUNTY",
      status: "ISSUED",
      statusDescription: null,
      description: "TEST",
      permitType: "BUILDING",
      issueDate: "2026-08-15",
      finalDate: null,
      estimatedValue: null,
      certificateOfOccupancyDate: null,
    })[0];
    const csv = serializePolkBulkStageRow({ rowIndex: 1, row });
    expect(BULK_STAGE_HEADER).toContain("references_json");
    expect(csv).toContain("property_improvements");
    expect(csv).toContain("polk_permits");
    expect(csv).toContain('"{""');
    expect(csv.endsWith("\n")).toBe(true);
  });
});
