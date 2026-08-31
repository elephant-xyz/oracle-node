import { describe, expect, it } from "vitest";

import { createRequire } from "node:module";

import {
  encodeCsvCell,
  parseCliOptions,
  parseCsvRecords,
  renderSeedCsv,
  selectMixedRows,
  shouldKeepValidationEntry,
  stripQueryFromSourceHttpRequestTree,
  unwrapPropertyPrintHtml,
} from "../../scripts/run-pinellas-local-ingest.mjs";

const require = createRequire(import.meta.url);
const { rewriteIpfsGatewayUrl } = require(
  "../../scripts/local-ipfs-fetch-shim.cjs",
);

describe("Pinellas local ingest helpers", () => {
  it("round-trips quoted JSON and geometry through the CSV parser", () => {
    const row = {
      parcel_id: "163131676080040070",
      use_group: "single-family",
      multiValueQueryString: `{"is_print":["1"],"s":["163131676080040070"]}`,
      parcel_polygon: `{"type":"Polygon","coordinates":[[[-82.7,27.7],[-82.7,27.7]]]}`,
    };
    const parsed = parseCsvRecords(renderSeedCsv(row));
    expect(parsed).toHaveLength(1);
    expect(parsed[0]?.parcel_id).toBe(row.parcel_id);
    expect(JSON.parse(parsed[0]?.multiValueQueryString ?? "{}")).toEqual({
      is_print: ["1"],
      s: ["163131676080040070"],
    });
    expect(JSON.parse(parsed[0]?.parcel_polygon ?? "{}").type).toBe("Polygon");
    expect(encodeCsvCell(`a"b`)).toBe(`"a""b"`);
  });

  it("selects the first STRAP of each use group", () => {
    const selected = selectMixedRows([
      { parcel_id: "1", use_group: "single-family" },
      { parcel_id: "2", use_group: "single-family" },
      { parcel_id: "3", use_group: "condo" },
      { parcel_id: "4", use_group: "" },
    ]);
    expect(selected.map((row) => row.parcel_id)).toEqual(["1", "3"]);
  });

  it("unwraps PropertyPrint HTML and rejects empty captures", () => {
    const html = unwrapPropertyPrintHtml({
      PropertyPrint: {
        response: "<!DOCTYPE html><html><body>Parcel Number</body></html>",
      },
    });
    expect(html).toContain("Parcel Number");
    expect(() => unwrapPropertyPrintHtml({ PropertyPrint: { response: "" } })).toThrow(
      /not HTML/,
    );
  });

  it("parses mixed-ingest flags", () => {
    expect(
      parseCliOptions(["--limit", "8", "--skip-validate", "--output", "tmp/out"]),
    ).toMatchObject({
      limit: 8,
      skipValidate: true,
      outputDirectory: "tmp/out",
      allRows: false,
    });
    expect(parseCliOptions(["--all"]).allRows).toBe(true);
    expect(() => parseCliOptions(["--limit", "-1"])).toThrow(/positive integer/);
  });

  it("drops leftover fact_sheet.json from the validate zip", () => {
    expect(shouldKeepValidationEntry("data/fact_sheet.json")).toBe(false);
    expect(shouldKeepValidationEntry("data/property.json")).toBe(true);
  });

  it("moves print query params off source_http_request.url", () => {
    const sanitized = stripQueryFromSourceHttpRequestTree({
      source_http_request: {
        method: "GET",
        url: "https://www.pcpao.gov/property/detail/print?is_print=1&s=152703878580000500",
        multiValueQueryString: { is_print: ["1"] },
      },
    });
    expect(sanitized).toEqual({
      source_http_request: {
        method: "GET",
        url: "https://www.pcpao.gov/property/detail/print",
        multiValueQueryString: {
          is_print: ["1"],
          s: ["152703878580000500"],
        },
      },
    });
  });

  it("rewrites public IPFS gateway URLs to the local Kubo gateway", () => {
    expect(
      rewriteIpfsGatewayUrl(
        "https://ipfs.io/ipfs/bafkreidi7qno2v5gecjf6tvgo35kqkv42542fq2juh22nemm7sfvhnzzua",
      ),
    ).toBe(
      "http://127.0.0.1:8080/ipfs/bafkreidi7qno2v5gecjf6tvgo35kqkv42542fq2juh22nemm7sfvhnzzua",
    );
    expect(rewriteIpfsGatewayUrl("https://lexicon.elephant.xyz/json-schemas/schema-manifest.json")).toBe(
      "https://lexicon.elephant.xyz/json-schemas/schema-manifest.json",
    );
  });
});
