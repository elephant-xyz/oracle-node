import { describe, expect, it } from "vitest";

import {
  FULL_SEED_COLUMNS,
  FULL_SEED_SOURCE_FIELDS,
  buildTaxParcelPageUrl,
  parseFullSeedCli,
  parseTaxParcelPage,
  renderFullSeedCsv,
  toFullSeedRow,
} from "../../scripts/build-pinellas-full-seed.mjs";

describe("Pinellas full-county seed builder", () => {
  it("requests tax-parcel pages without geometry and without owner fields", () => {
    const url = buildTaxParcelPageUrl(15000, 15000);
    expect(url).toContain("returnGeometry=false");
    expect(url).toContain("resultOffset=15000");
    expect(url).toContain("resultRecordCount=15000");
    expect(FULL_SEED_SOURCE_FIELDS).toEqual([
      "OBJECTID",
      "STRAP",
      "PARCELID",
      "PARCELID_DSP1",
    ]);
    expect(FULL_SEED_SOURCE_FIELDS.join(" ")).not.toMatch(/OWNER|MAIL|OWNADD/i);
  });

  it("uses STRAP as parcel_id, keeps PARCELID separate, and omits query strings from url", () => {
    const row = toFullSeedRow(
      {
        attributes: {
          STRAP: "162805389030000430",
          PARCELID: "052816389030000430",
          PARCELID_DSP1: "05-28-16-38903-000-0430",
        },
      },
      "2026-08-31T00:00:00.000Z",
    );

    expect(row).not.toBeNull();
    expect(row?.parcel_id).toBe("162805389030000430");
    expect(row?.source_identifier).toBe("162805389030000430");
    expect(row?.parcelid).toBe("052816389030000430");
    expect(row?.county).toBe("Pinellas");
    expect(row?.county_fips).toBe("12103");
    expect(row?.url).toBe("https://www.pcpao.gov/property/detail/print");
    expect(row?.url).not.toContain("?");
    expect(JSON.parse(row?.multiValueQueryString ?? "{}")).toEqual({
      is_print: ["1"],
      s: ["162805389030000430"],
    });
    expect(FULL_SEED_COLUMNS).not.toContain("parcel_polygon");
  });

  it("skips invalid STRAPs instead of staging PARCELID-shaped ids", () => {
    expect(
      toFullSeedRow(
        { attributes: { STRAP: "16-28-05-38903-000-0430" } },
        "2026-08-31T00:00:00.000Z",
      ),
    ).toBeNull();
    expect(
      toFullSeedRow({ attributes: { STRAP: "" } }, "2026-08-31T00:00:00.000Z"),
    ).toBeNull();
  });

  it("fails loud on GIS error payloads", () => {
    expect(() =>
      parseTaxParcelPage({ error: { code: 400, message: "Failed" } }),
    ).toThrow(/Tax parcel GIS query failed/);
  });

  it("renders a header that the workflow feeder can read", () => {
    const csv = renderFullSeedCsv([
      {
        parcel_id: "162805389030000430",
        source_identifier: "162805389030000430",
        situs_address: "",
        method: "GET",
        url: "https://www.pcpao.gov/property/detail/print",
        multiValueQueryString: '{"is_print":["1"],"s":["162805389030000430"]}',
        county: "Pinellas",
        county_fips: "12103",
        parcelid: "052816389030000430",
        parcelid_display: "05-28-16-38903-000-0430",
        source_url: "https://egis.pinellas.gov/example",
        source_snapshot_at: "2026-08-31T00:00:00.000Z",
      },
    ]);
    expect(csv.startsWith("parcel_id,source_identifier,situs_address,")).toBe(
      true,
    );
    expect(csv).toContain("Pinellas");
    expect(csv).toContain("12103");
  });

  it("parses CLI flags", () => {
    expect(parseFullSeedCli(["--output", "tmp/pinellas.csv"])).toEqual({
      outputPath: "tmp/pinellas.csv",
      skipPrintProbe: false,
    });
    expect(parseFullSeedCli(["--skip-print-probe"])).toMatchObject({
      skipPrintProbe: true,
    });
    expect(() => parseFullSeedCli(["--nope"])).toThrow(/Unexpected argument/);
  });
});
