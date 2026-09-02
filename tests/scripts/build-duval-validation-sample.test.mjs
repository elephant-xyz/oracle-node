import { describe, expect, it } from "vitest";

import {
  SAMPLE_COLUMNS,
  classifySampleReasons,
  parseCliOptions,
  renderValidationSampleCsv,
  selectValidationSample,
} from "../../scripts/build-duval-validation-sample.mjs";

/**
 * @param {string} identifier
 * @param {Record<string, string>} [overrides]
 */
function seedRecord(identifier, overrides = {}) {
  return {
    source_identifier: identifier,
    parcel_id: identifier.replace(/R$/, ""),
    source_DOR_UC: "001",
    source_PA_UC: "00",
    source_PHY_ADDR1: "100 MAIN ST",
    source_JV: "100",
    source_TOT_LVG_AREA: "1000",
    source_ACT_YR_BLT: "2000",
    source_NO_BULDNG: "1",
    source_record_count: "1",
    latitude: "30.33",
    longitude: "-81.65",
    ...overrides,
  };
}

describe("Duval transform validation sample", () => {
  it("classifies missing, vacant, and consolidated source variability", () => {
    expect(
      classifySampleReasons(
        seedRecord("0969250000R", {
          source_PHY_ADDR1: "",
          source_JV: "",
          source_TOT_LVG_AREA: "",
          source_ACT_YR_BLT: "",
          source_DOR_UC: "",
          source_record_count: "2",
        }),
      ),
    ).toEqual([
      "missing_site_address",
      "incomplete_assessment",
      "no_recorded_structure",
      "blank_dor_uc",
      "consolidated_duplicate_pin",
    ]);
  });

  it("selects edge cases before filling DOR_UC coverage", () => {
    const records = [
      seedRecord("0969250000R", { source_DOR_UC: "001" }),
      seedRecord("0901770592R", {
        source_DOR_UC: "004",
        source_PHY_ADDR1: "",
      }),
      seedRecord("1230290100R", {
        source_DOR_UC: "027",
        source_JV: "",
      }),
      seedRecord("0000000002R", {
        source_DOR_UC: "002",
        source_TOT_LVG_AREA: "",
        source_ACT_YR_BLT: "",
      }),
      seedRecord("0000000000R", {
        source_DOR_UC: "",
        source_record_count: "2",
      }),
      seedRecord("not-a-pin"),
    ];

    const selected = selectValidationSample(records, 5);

    expect(selected).toHaveLength(5);
    expect(selected.map((row) => row.request_identifier)).not.toContain(
      "not-a-pin",
    );
    expect(selected.some((row) => row.has_site_address === "false")).toBe(true);
    expect(selected.some((row) => row.has_assessment === "false")).toBe(true);
    expect(selected.some((row) => row.has_structure === "false")).toBe(true);
    expect(
      selected.some((row) =>
        row.sample_reasons.includes("consolidated_duplicate_pin"),
      ),
    ).toBe(true);
  });

  it("renders a minimal non-PII CSV", () => {
    const selected = selectValidationSample(
      [seedRecord("0969250000R", { source_PHY_ADDR1: 'CITY, "CORE"' })],
      1,
    );

    const csv = renderValidationSampleCsv(selected);

    expect(csv).toMatch(/^request_identifier,/);
    expect(SAMPLE_COLUMNS[0]).toBe("request_identifier");
    expect(csv).toContain("0969250000R");
    expect(csv).not.toContain("OWN_NAME");
    expect(csv).not.toContain("owner");
  });

  it("validates CLI limits", () => {
    expect(parseCliOptions(["--limit", "50"])).toMatchObject({ limit: 50 });
    expect(() => parseCliOptions(["--limit", "0"])).toThrow(/positive integer/);
  });
});
