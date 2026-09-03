import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { promisify } from "node:util";

import { describe, expect, it } from "vitest";

import {
  mcpEnvMapsFromCatalog,
  stringifyMcpEnvMaps,
} from "../../scripts/print-mcp-env-maps.mjs";

const execFileAsync = promisify(execFile);

describe("print-mcp-env-maps", () => {
  it("omits null URLs and keeps only populated fields", () => {
    const maps = mcpEnvMapsFromCatalog({
      counties: [
        {
          countyKey: "lee",
          queryTableUrl: "https://example.com/lee.parquet",
          permitQueryTableUrl: null,
          datasetCoverageUrl: "https://example.com/lee-coverage.json",
        },
        {
          countyKey: "montgomery",
          queryTableUrl: "https://example.com/montgomery.parquet",
          permitQueryTableUrl: "https://example.com/montgomery-permits.parquet",
          datasetCoverageUrl: "",
        },
      ],
    });

    expect(maps.PROPERTY_QUERY_TABLE_MAP).toEqual({
      lee: "https://example.com/lee.parquet",
      montgomery: "https://example.com/montgomery.parquet",
    });
    expect(maps.PERMIT_QUERY_TABLE_MAP).toEqual({
      montgomery: "https://example.com/montgomery-permits.parquet",
    });
    expect(maps.DATASET_COVERAGE_MAP).toEqual({
      lee: "https://example.com/lee-coverage.json",
    });
  });

  it("stringifies maps as JSON-inside-JSON for MCP env", () => {
    const encoded = stringifyMcpEnvMaps({
      PROPERTY_QUERY_TABLE_MAP: { lee: "https://example.com/lee.parquet" },
      PERMIT_QUERY_TABLE_MAP: {},
      DATASET_COVERAGE_MAP: { lee: "https://example.com/lee-coverage.json" },
    });

    expect(JSON.parse(encoded.PROPERTY_QUERY_TABLE_MAP)).toEqual({
      lee: "https://example.com/lee.parquet",
    });
    expect(JSON.parse(encoded.PERMIT_QUERY_TABLE_MAP)).toEqual({});
    expect(JSON.parse(encoded.DATASET_COVERAGE_MAP)).toEqual({
      lee: "https://example.com/lee-coverage.json",
    });
  });

  it("throws on duplicate countyKey for a populated field", () => {
    expect(() =>
      mcpEnvMapsFromCatalog({
        counties: [
          {
            countyKey: "lee",
            queryTableUrl: "https://example.com/lee-a.parquet",
          },
          {
            countyKey: "lee",
            queryTableUrl: "https://example.com/lee-b.parquet",
          },
        ],
      }),
    ).toThrow(/Duplicate countyKey "lee"/);
  });

  it("matches the tracked catalog keys", async () => {
    const catalog = JSON.parse(
      await readFile(resolve("catalog/published-counties.json"), "utf8"),
    );
    const maps = mcpEnvMapsFromCatalog(catalog);
    const countyKeys = catalog.counties.map((county) => county.countyKey);

    expect(Object.keys(maps.PROPERTY_QUERY_TABLE_MAP)).toEqual(countyKeys);
    expect(Object.keys(maps.DATASET_COVERAGE_MAP)).toEqual(countyKeys);
    expect(maps.PERMIT_QUERY_TABLE_MAP).toEqual({
      broward: catalog.counties.find((c) => c.countyKey === "broward")
        .permitQueryTableUrl,
      montgomery: catalog.counties.find((c) => c.countyKey === "montgomery")
        .permitQueryTableUrl,
      "rock-island": catalog.counties.find((c) => c.countyKey === "rock-island")
        .permitQueryTableUrl,
    });
    expect(maps.PROPERTY_QUERY_TABLE_MAP.polk).toMatch(/^https:/);
  });

  it("CLI prints stringified maps for the tracked catalog", async () => {
    const catalog = JSON.parse(
      await readFile(resolve("catalog/published-counties.json"), "utf8"),
    );
    const { stdout } = await execFileAsync(
      process.execPath,
      ["scripts/print-mcp-env-maps.mjs"],
      { cwd: resolve("."), encoding: "utf8" },
    );

    expect(JSON.parse(stdout)).toEqual(
      stringifyMcpEnvMaps(mcpEnvMapsFromCatalog(catalog)),
    );
  });
});
