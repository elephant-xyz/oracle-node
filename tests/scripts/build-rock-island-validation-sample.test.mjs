import { describe, expect, it } from "vitest";

import {
  classifySampleReasons,
  parseCliOptions,
  renderValidationSampleCsv,
  selectValidationSample,
} from "../../scripts/build-rock-island-validation-sample.mjs";

/**
 * Build a complete synthetic seed row for sample-selection tests.
 *
 * @param {string} identifier - Canonical parcel identifier.
 * @param {Record<string, string>} [overrides] - Source fields to replace.
 * @returns {Record<string, string>} Synthetic seed record.
 */
function seedRecord(identifier, overrides = {}) {
  return {
    source_identifier: identifier,
    source_site_address: "100 MAIN ST",
    source_EAV: "100",
    source_EMV: "300",
    source_TOTSQFT: "1000",
    source_YRBuilt: "2000",
    source_class: "0011",
    source_Zoning: "R1",
    source_municipality: "MOLINE",
    source_record_count: "1",
    ...overrides,
  };
}

describe("Rock Island transform validation sample", () => {
  it("classifies missing, improved, and consolidated source variability", () => {
    expect(
      classifySampleReasons(
        seedRecord("0012345678", {
          source_site_address: "",
          source_EAV: "",
          source_TOTSQFT: "",
          source_YRBuilt: "",
          source_class: "",
          source_Zoning: "",
          source_record_count: "2",
        }),
      ),
    ).toEqual([
      "missing_site_address",
      "incomplete_assessment",
      "no_recorded_structure",
      "blank_class",
      "blank_zoning",
      "consolidated_duplicate_pin",
    ]);
  });

  it("selects edge cases before filling raw class-code coverage", () => {
    const records = [
      seedRecord("0000000001", { source_class: "0040" }),
      seedRecord("0000000002", {
        source_class: "0030",
        source_site_address: "",
      }),
      seedRecord("0000000003", {
        source_class: "0060",
        source_EAV: "",
      }),
      seedRecord("0000000004", {
        source_class: "0021",
        source_TOTSQFT: "",
        source_YRBuilt: "",
      }),
      seedRecord("0000000005", {
        source_class: "",
        source_Zoning: "",
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

  it("renders a minimal non-PII CSV accepted by prepare input mode", () => {
    const selected = selectValidationSample(
      [seedRecord("0000000001", { source_municipality: 'CITY, "CORE"' })],
      1,
    );

    const csv = renderValidationSampleCsv(selected);

    expect(csv).toMatch(/^request_identifier,/);
    expect(csv).toContain("0000000001");
    expect(csv).toContain('"CITY, ""CORE"""');
    expect(csv).not.toContain("owner");
    expect(csv).not.toContain("taxbill");
  });

  it("validates CLI limits", () => {
    expect(parseCliOptions(["--limit", "15"])).toMatchObject({ limit: 15 });
    expect(() => parseCliOptions(["--limit", "0"])).toThrow(/positive integer/);
  });
});
