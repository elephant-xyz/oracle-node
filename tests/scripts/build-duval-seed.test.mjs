import { describe, expect, it } from "vitest";

import {
  COUNTY_FIPS,
  COUNTY_NAME,
  EXPECTED_NAL_ROWS,
  EXCLUDED_PII_FIELDS,
  NAL_SOURCE_FIELDS,
  SEED_COLUMNS,
  assertSafeSourceFields,
  assertSeedReconciliation,
  classifyDorUseBand,
  classifyPilotReasons,
  hasInRangePinGeometry,
  isValidDorParcelId,
  mergeDuplicateParcels,
  renderSeedCsv,
  toCanonicalReDisplay,
  toCojDetailUrl,
  toSeedRow,
  toUndashedTenDigit,
} from "../../scripts/duval/lib.mjs";
import {
  parseCliOptions,
  parsePinSidecarRights,
  selectPilotSample,
} from "../../scripts/build-duval-seed.mjs";

/**
 * @param {string} parcelId
 * @param {Record<string, unknown>} [overrides]
 */
function nalRecord(parcelId, overrides = {}) {
  return {
    PARCEL_ID: parcelId,
    CO_NO: "26",
    ASMNT_YR: "2026",
    DOR_UC: "001",
    PA_UC: "00",
    JV: "250000",
    AV_NSD: "200000",
    TV_NSD: "180000",
    LND_VAL: "50000",
    LND_SQFOOT: "8000",
    ACT_YR_BLT: "1998",
    EFF_YR_BLT: "2005",
    TOT_LVG_AREA: "1800",
    NO_BULDNG: "1",
    NO_RES_UNTS: "1",
    PHY_ADDR1: "123 MAIN ST",
    PHY_ADDR2: "",
    PHY_CITY: "JACKSONVILLE",
    PHY_ZIPCD: "32202",
    NBRHD_CD: "100",
    MKT_AR: "01",
    CENSUS_BK: "120310001001",
    SALE_PRC1: "240000",
    SALE_YR1: "2024",
    SALE_MO1: "6",
    QUAL_CD1: "01",
    NO_OWN_NM: "1",
    ...overrides,
  };
}

/**
 * @param {string} parcelId
 * @param {Record<string, unknown>} [overrides]
 */
function pinRecord(parcelId, overrides = {}) {
  return {
    PARCELNO: parcelId,
    latitude: 30.332,
    longitude: -81.655,
    geometry: {
      type: "Polygon",
      coordinates: [
        [
          [-81.656, 30.331],
          [-81.654, 30.331],
          [-81.654, 30.333],
          [-81.656, 30.333],
          [-81.656, 30.331],
        ],
      ],
    },
    ...overrides,
  };
}

describe("Duval DOR identifier helpers", () => {
  it("accepts the eleven-character DOR id and never numeric-coerces it", () => {
    expect(isValidDorParcelId("0969250000R")).toBe(true);
    expect(isValidDorParcelId("0901770592R")).toBe(true);
    expect(toUndashedTenDigit("0969250000R")).toBe("0969250000");
    expect(toCanonicalReDisplay("0969250000R")).toBe("096925-0000");
    expect(toCojDetailUrl("0969250000R")).toBe(
      "https://paopropertysearch.coj.net/Basic/Detail.aspx?RE=0969250000R",
    );
    expect(isValidDorParcelId("969250000R")).toBe(false);
    expect(isValidDorParcelId("0969250000")).toBe(false);
    expect(isValidDorParcelId("")).toBe(false);
    expect(
      parsePinSidecarRights(
        "<accconst>Restricted</accconst><useconst>None</useconst>",
        "sidecar.xml",
      ),
    ).toEqual({
      accconst: "Restricted",
      useconst: "None",
      sourcePath: "sidecar.xml",
    });
    expect(parsePinSidecarRights("<metadata/>")).toEqual({
      accconst: null,
      useconst: null,
      sourcePath: "",
    });
  });
});

describe("Duval seed privacy and columns", () => {
  it("uses a strict non-PII NAL allow-list", () => {
    expect(() => assertSafeSourceFields(NAL_SOURCE_FIELDS)).not.toThrow();
    for (const excludedField of EXCLUDED_PII_FIELDS) {
      expect(NAL_SOURCE_FIELDS).not.toContain(excludedField);
      expect(SEED_COLUMNS).not.toContain(`source_${excludedField}`);
    }
    expect(() =>
      assertSafeSourceFields([...NAL_SOURCE_FIELDS, "OWN_NAME"]),
    ).toThrow(/PII field is prohibited/);
  });

  it("maps a joined DOR record onto the Rock Island seed contract", () => {
    const row = toSeedRow({
      nal: nalRecord("0969250000R"),
      pin: pinRecord("0969250000R"),
      sdfSaleCount: 2,
      sourceRevision: "2026-08-27T00:00:00.000Z",
      snapshotAt: "2026-09-01T00:00:00.000Z",
    });

    expect(row.parcel_id).toBe("0969250000");
    expect(row.source_identifier).toBe("0969250000R");
    expect(row.county).toBe(COUNTY_NAME);
    expect(row.county_fips).toBe(COUNTY_FIPS);
    expect(row.address).toBe("123 MAIN ST, JACKSONVILLE FL 32202");
    expect(row.city).toBe("JACKSONVILLE");
    expect(row.state).toBe("FL");
    expect(row.zip).toBe("32202");
    expect(row.method).toBe("GET");
    expect(row.url).toBe(
      "https://paopropertysearch.coj.net/Basic/Detail.aspx",
    );
    expect(JSON.parse(row.multiValueQueryString)).toEqual({
      RE: ["0969250000R"],
    });
    expect(row.source_DOR_UC).toBe("001");
    expect(row.source_PA_UC).toBe("00");
    expect(row.source_PARCEL_ID).toBe("0969250000R");
    expect(row.latitude).toBe("30.332");
    expect(row.longitude).toBe("-81.655");
    expect(JSON.parse(row.parcel_polygon)).toMatchObject({ type: "Polygon" });
    expect(row).not.toHaveProperty("source_OWN_NAME");
    for (const column of SEED_COLUMNS) {
      expect(row).toHaveProperty(column);
    }
  });
});

describe("Duval seed reconciliation", () => {
  it("quarantines non-canonical ids and merges duplicate keyed parcels", () => {
    const keyed = [
      {
        nal: nalRecord("0969250000R", { JV: "100" }),
        pin: pinRecord("0969250000R"),
      },
      {
        nal: nalRecord("0969250000R", { JV: "200", PHY_ADDR1: "125 MAIN ST" }),
        pin: {
          ...pinRecord("0969250000R"),
          geometry: {
            type: "Polygon",
            coordinates: [
              [
                [-81.66, 30.33],
                [-81.65, 30.33],
                [-81.65, 30.34],
                [-81.66, 30.34],
                [-81.66, 30.33],
              ],
            ],
          },
        },
      },
    ];
    const unkeyed = [{ PARCEL_ID: "USA" }, { PARCEL_ID: "" }];

    const merged = mergeDuplicateParcels(keyed, {
      sourceRevision: "2026-08-27T00:00:00.000Z",
      snapshotAt: "2026-09-01T00:00:00.000Z",
    });

    expect(merged).toHaveLength(1);
    expect(merged[0].source_record_count).toBe("2");
    expect(merged[0].address).toContain("125 MAIN ST");
    expect(JSON.parse(merged[0].parcel_polygon)).toMatchObject({
      type: "MultiPolygon",
      coordinates: [
        [
          [
            [-81.66, 30.33],
            [-81.65, 30.33],
            [-81.65, 30.34],
            [-81.66, 30.34],
            [-81.66, 30.33],
          ],
        ],
        pinRecord("0969250000R").geometry.coordinates,
      ],
    });

    expect(() =>
      assertSeedReconciliation({
        rowsWritten: 1,
        uniqueParcelIds: 2,
        expectedSeedRowCount: 1,
        unkeyedSourceRecords: 2,
        invalidRecordCount: 2,
        consolidatedRows: 1,
        duplicateGroups: 1,
      }),
    ).toThrow(/uniqueParcelIds/);
    expect(() =>
      assertSeedReconciliation({
        rowsWritten: 1,
        uniqueParcelIds: 1,
        expectedSeedRowCount: 1,
        unkeyedSourceRecords: 1,
        invalidRecordCount: 2,
        consolidatedRows: 1,
        duplicateGroups: 1,
      }),
    ).toThrow(/unkeyedSourceRecords/);
    expect(() =>
      assertSeedReconciliation({
        rowsWritten: 1,
        uniqueParcelIds: 1,
        expectedSeedRowCount: 1,
        unkeyedSourceRecords: 2,
        invalidRecordCount: 2,
        consolidatedRows: 2,
        duplicateGroups: 1,
      }),
    ).toThrow(/consolidatedRows/);
    expect(EXPECTED_NAL_ROWS).toBe(404023);

    expect(() =>
      assertSeedReconciliation({
        rowsWritten: 1,
        uniqueParcelIds: 1,
        expectedSeedRowCount: 1,
        unkeyedSourceRecords: unkeyed.length,
        invalidRecordCount: 2,
        consolidatedRows: 1,
        duplicateGroups: 1,
      }),
    ).not.toThrow();

    expect(() =>
      assertSeedReconciliation({
        rowsWritten: 2,
        uniqueParcelIds: 1,
        expectedSeedRowCount: 1,
        unkeyedSourceRecords: 2,
        invalidRecordCount: 2,
        consolidatedRows: 1,
        duplicateGroups: 1,
      }),
    ).toThrow(/rowsWritten/);
  });
});

describe("Duval use-code bands and pilot sample", () => {
  it("classifies DOR_UC into the documented stratification bands", () => {
    expect(classifyDorUseBand("000")).toBe("vacant_residential");
    expect(classifyDorUseBand("001")).toBe("single_family");
    expect(classifyDorUseBand("002")).toBe("mobile_home");
    expect(classifyDorUseBand("003")).toBe("multi_family");
    expect(classifyDorUseBand("008")).toBe("multi_family");
    expect(classifyDorUseBand("004")).toBe("condo");
    expect(classifyDorUseBand("027")).toBe("commercial");
    expect(classifyDorUseBand("041")).toBe("industrial");
    expect(classifyDorUseBand("050")).toBe("agricultural");
    expect(classifyDorUseBand("071")).toBe("institutional");
    expect(classifyDorUseBand("080")).toBe("government");
    expect(classifyDorUseBand("099")).toBe("other");
  });

  it("selects a stratified non-PII pilot without repeating parcel ids", () => {
    const rows = [
      toSeedRow({
        nal: nalRecord("0969250000R", { DOR_UC: "001" }),
        pin: pinRecord("0969250000R"),
        sdfSaleCount: 1,
        sourceRevision: "2026-08-27T00:00:00.000Z",
        snapshotAt: "2026-09-01T00:00:00.000Z",
      }),
      toSeedRow({
        nal: nalRecord("0901770592R", { DOR_UC: "004", NO_BULDNG: "1" }),
        pin: pinRecord("0901770592R"),
        sdfSaleCount: 0,
        sourceRevision: "2026-08-27T00:00:00.000Z",
        snapshotAt: "2026-09-01T00:00:00.000Z",
      }),
      toSeedRow({
        nal: nalRecord("1230290100R", {
          DOR_UC: "027",
          NO_BULDNG: "3",
          TOT_LVG_AREA: "20000",
        }),
        pin: pinRecord("1230290100R"),
        sdfSaleCount: 1,
        sourceRevision: "2026-08-27T00:00:00.000Z",
        snapshotAt: "2026-09-01T00:00:00.000Z",
      }),
      toSeedRow({
        nal: nalRecord("0000000002R", {
          DOR_UC: "002",
          PHY_ADDR1: "1 MOBILE LN",
        }),
        pin: pinRecord("0000000002R"),
        sdfSaleCount: 0,
        sourceRevision: "2026-08-27T00:00:00.000Z",
        snapshotAt: "2026-09-01T00:00:00.000Z",
      }),
      toSeedRow({
        nal: nalRecord("0000000000R", {
          DOR_UC: "000",
          TOT_LVG_AREA: "0",
          ACT_YR_BLT: "0",
          NO_BULDNG: "0",
        }),
        pin: pinRecord("0000000000R"),
        sdfSaleCount: 0,
        sourceRevision: "2026-08-27T00:00:00.000Z",
        snapshotAt: "2026-09-01T00:00:00.000Z",
      }),
      toSeedRow({
        nal: nalRecord("0000000008R", { DOR_UC: "008" }),
        pin: pinRecord("0000000008R"),
        sdfSaleCount: 0,
        sourceRevision: "2026-08-27T00:00:00.000Z",
        snapshotAt: "2026-09-01T00:00:00.000Z",
      }),
      toSeedRow({
        nal: nalRecord("0000000041R", { DOR_UC: "041" }),
        pin: pinRecord("0000000041R"),
        sdfSaleCount: 0,
        sourceRevision: "2026-08-27T00:00:00.000Z",
        snapshotAt: "2026-09-01T00:00:00.000Z",
      }),
      toSeedRow({
        nal: nalRecord("0000000050R", { DOR_UC: "050" }),
        pin: pinRecord("0000000050R"),
        sdfSaleCount: 0,
        sourceRevision: "2026-08-27T00:00:00.000Z",
        snapshotAt: "2026-09-01T00:00:00.000Z",
      }),
      toSeedRow({
        nal: nalRecord("0000000071R", { DOR_UC: "071" }),
        pin: pinRecord("0000000071R"),
        sdfSaleCount: 0,
        sourceRevision: "2026-08-27T00:00:00.000Z",
        snapshotAt: "2026-09-01T00:00:00.000Z",
      }),
      toSeedRow({
        nal: nalRecord("0000000080R", { DOR_UC: "080" }),
        pin: pinRecord("0000000080R"),
        sdfSaleCount: 0,
        sourceRevision: "2026-08-27T00:00:00.000Z",
        snapshotAt: "2026-09-01T00:00:00.000Z",
      }),
      toSeedRow({
        nal: nalRecord("0000000198R", {
          DOR_UC: "001",
          ACT_YR_BLT: "1920",
          SALE_YR1: "2025",
          NO_OWN_NM: "3",
        }),
        pin: pinRecord("0000000198R"),
        sdfSaleCount: 3,
        sourceRevision: "2026-08-27T00:00:00.000Z",
        snapshotAt: "2026-09-01T00:00:00.000Z",
      }),
    ];

    expect(classifyPilotReasons(rows[2])).toContain("multiple_buildings");
    expect(classifyPilotReasons(rows[4])).toContain("zero_improvements");
    expect(classifyPilotReasons(rows[10])).toEqual(
      expect.arrayContaining([
        "old_construction",
        "recent_sale",
        "multiple_owners",
      ]),
    );

    const filler = Array.from({ length: 20 }, (_, index) =>
      toSeedRow({
        nal: nalRecord(`001000${String(index).padStart(4, "0")}R`, {
          DOR_UC: "001",
          PHY_ADDR1: `${index} FILL ST`,
        }),
        pin: pinRecord(`001000${String(index).padStart(4, "0")}R`),
        sourceRevision: "2026-08-27T00:00:00.000Z",
        snapshotAt: "2026-09-01T00:00:00.000Z",
      }),
    );
    const outOfRange = toSeedRow({
      nal: nalRecord("0999999999R", { DOR_UC: "080" }),
      pin: pinRecord("0999999999R", { latitude: 25.0, longitude: -80.0 }),
      sourceRevision: "2026-08-27T00:00:00.000Z",
      snapshotAt: "2026-09-01T00:00:00.000Z",
    });
    expect(hasInRangePinGeometry(outOfRange)).toBe(false);

    const sample = selectPilotSample([...filler, ...rows, outOfRange], 10);
    expect(sample).toHaveLength(10);
    const ids = sample.map((row) => row.source_identifier);
    expect(new Set(ids).size).toBe(10);
    expect(ids).toEqual(
      expect.arrayContaining([
        "0969250000R",
        "0901770592R",
        "0000000080R",
        "0000000041R",
        "0000000050R",
      ]),
    );
    expect(ids).not.toContain("0999999999R");
    expect(sample.every((row) => hasInRangePinGeometry(row))).toBe(true);
    expect(renderSeedCsv(sample).split("\n")[0]).toBe(
      "parcel_id,source_identifier,method,url,multiValueQueryString,address,city,state,zip,county,county_fips,latitude,longitude,parcel_polygon,source_url,source_item_id,source_revision,source_snapshot_at,source_record_count,source_object_ids,source_features_json,source_sdf_sale_count,source_PARCEL_ID,source_CO_NO,source_ASMNT_YR,source_DOR_UC,source_PA_UC,source_JV,source_AV_NSD,source_TV_NSD,source_LND_VAL,source_LND_SQFOOT,source_ACT_YR_BLT,source_EFF_YR_BLT,source_TOT_LVG_AREA,source_NO_BULDNG,source_NO_RES_UNTS,source_NO_OWN_NM,source_PHY_ADDR1,source_PHY_ADDR2,source_PHY_CITY,source_PHY_ZIPCD,source_NBRHD_CD,source_MKT_AR,source_CENSUS_BK,source_SALE_PRC1,source_SALE_YR1,source_SALE_MO1,source_QUAL_CD1",
    );
    expect(() => selectPilotSample(filler, 10)).toThrow(/smoke parcels/);
  });

  it("validates CLI output paths", () => {
    expect(parseCliOptions(["--output", "downloads/duval/duval.csv"])).toEqual({
      outputPath: "downloads/duval/duval.csv",
      workDir: "downloads/duval",
      skipDownload: false,
      skipSpotCheck: false,
    });
    expect(() => parseCliOptions(["--unknown", "x"])).toThrow(/Unknown option/);
  });
});
