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
  buildPrintPageUrl,
  buildSeedJsonFiles,
  buildSourceHttpRequest,
  createLimiter,
  createRateLimitGate,
  fetchPropertyPrintHtml,
  hasCompletedTransform,
  isFatalIngestError,
  isRetryableIngestError,
  mapWithConcurrency,
  parseSeedQueryString,
  retryDelayMs,
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
      skipExisting: true,
      concurrency: 4,
      fetchConcurrency: 8,
      transformMode: "scripts",
      useCliPrepare: false,
      fetchTimeoutMs: 12000,
      transformTimeoutMs: 60000,
    });
    expect(parseCliOptions(["--all"]).allRows).toBe(true);
    expect(parseCliOptions(["--force", "--cli-transform", "--concurrency", "4"]))
      .toMatchObject({
        skipExisting: false,
        transformMode: "elephant-cli",
        concurrency: 4,
      });
    expect(parseCliOptions(["--fetch-concurrency", "8", "--fetch-timeout-ms", "5000"]))
      .toMatchObject({
        fetchConcurrency: 8,
        fetchTimeoutMs: 5000,
      });
    expect(
      parseCliOptions(["--transform-timeout-ms", "15000"]).transformTimeoutMs,
    ).toBe(15000);
    expect(() => parseCliOptions(["--concurrency", "0"])).toThrow(
      /positive integer/,
    );
    expect(() => parseCliOptions(["--fetch-timeout-ms", "0"])).toThrow(
      /--fetch-timeout-ms/,
    );
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

  it("keeps print query params off source_http_request.url and on multiValueQueryString", () => {
    const url = buildPrintPageUrl("162805389030000430");
    expect(url).toContain("s=162805389030000430");
    expect(url).toContain("is_print=1");
    expect(parseSeedQueryString("", "162805389030000430")).toEqual({
      is_print: ["1"],
      s: ["162805389030000430"],
    });
    const row = {
      parcel_id: "162805389030000430",
      county: "Pinellas",
      situs_address: "3400 RUGBY CT, PALM HARBOR FL",
      url: "https://www.pcpao.gov/property/detail/print",
      method: "GET",
      multiValueQueryString: `{"is_print":["1"],"s":["162805389030000430"]}`,
    };
    const seed = buildSeedJsonFiles(row);
    expect(seed.propertySeed.parcel_id).toBe("162805389030000430");
    expect(seed.propertySeed.source_http_request.url).not.toContain("?");
    expect(seed.unnormalizedAddress.county_jurisdiction).toBe("Pinellas");
    expect(buildSourceHttpRequest(row).url).toBe(
      "https://www.pcpao.gov/property/detail/print",
    );
  });

  it("fetches print HTML with a Chrome UA and retries 403", async () => {
    /** @type {string[]} */
    const userAgents = [];
    /** @type {number} */
    let calls = 0;
    const html =
      "<!DOCTYPE html><html><body><h1>Parcel Summary</h1>Owner Name</body></html>";
    const fakeFetch = async (input, init) => {
      calls += 1;
      userAgents.push(
        typeof init?.headers === "object" && init.headers !== null
          ? String(
              /** @type {Record<string, string>} */ (init.headers)["User-Agent"],
            )
          : "",
      );
      if (calls === 1) {
        return new Response("denied", { status: 403 });
      }
      expect(String(input)).toContain("s=162805389030000430");
      expect(init?.signal).toBeDefined();
      return new Response(html, { status: 200 });
    };
    const fetched = await fetchPropertyPrintHtml(
      "162805389030000430",
      /** @type {typeof fetch} */ (fakeFetch),
      3,
    );
    expect(fetched).toContain("Parcel Summary");
    expect(calls).toBe(2);
    expect(userAgents[1]).toMatch(/Chrome\/124/);
  });

  it("does not retry empty print HTML that is missing Parcel Summary", async () => {
    /** @type {number} */
    let calls = 0;
    const fakeFetch = async () => {
      calls += 1;
      return new Response("<!DOCTYPE html><html><body>placeholder</body></html>", {
        status: 200,
      });
    };
    await expect(
      fetchPropertyPrintHtml(
        "052816389030000430",
        /** @type {typeof fetch} */ (fakeFetch),
        4,
      ),
    ).rejects.toThrow(/Parcel Summary/);
    expect(calls).toBe(1);
  });

  it("runs a concurrency pool in input order", async () => {
    const seen = [];
    const mapped = await mapWithConcurrency([3, 2, 1], 2, async (value) => {
      seen.push(value);
      await new Promise((resolve) => {
        setTimeout(resolve, value * 5);
      });
      return value * 10;
    });
    expect(mapped).toEqual([30, 20, 10]);
    expect(seen).toHaveLength(3);
  });

  it("leaves remaining items unprocessed when shouldStop becomes true", async () => {
    /** @type {number[]} */
    const seen = [];
    let stop = false;
    const mapped = await mapWithConcurrency(
      [1, 2, 3, 4, 5, 6],
      2,
      async (value) => {
        seen.push(value);
        if (seen.length >= 2) stop = true;
        await new Promise((resolve) => {
          setTimeout(resolve, 10);
        });
        return value;
      },
      () => stop,
    );
    expect(seen.length).toBeLessThan(6);
    expect(mapped.filter((value) => value !== undefined).length).toBe(seen.length);
  });

  it("skips a parcel only when transformed.zip is a real zip of at least 200 bytes", async () => {
    const { mkdtemp, mkdir, writeFile, rm } = await import("node:fs/promises");
    const os = await import("node:os");
    const path = await import("node:path");
    const dir = await mkdtemp(path.join(os.tmpdir(), "pinellas-skip-"));
    const parcelDir = path.join(dir, "162805389030000430");
    await mkdir(parcelDir, { recursive: true });
    expect(hasCompletedTransform(parcelDir)).toBe(false);
    await writeFile(path.join(parcelDir, "transformed.zip"), "ok");
    expect(hasCompletedTransform(parcelDir)).toBe(false);
    await writeFile(
      path.join(parcelDir, "transformed.zip"),
      Buffer.concat([Buffer.from([0x50, 0x4b, 0x03, 0x04]), Buffer.alloc(196, 0x20)]),
    );
    expect(hasCompletedTransform(parcelDir)).toBe(true);
    await rm(dir, { recursive: true, force: true });
  });

  it("retries transient fetch and worker failures with longer backoff on 403/429", () => {
    expect(isRetryableIngestError(new Error("PCPAO print HTTP 403 for x"))).toBe(
      true,
    );
    expect(isRetryableIngestError(new Error("PCPAO print HTTP 429 for x"))).toBe(
      true,
    );
    expect(isRetryableIngestError(new Error("transform timed out after 60000ms"))).toBe(
      true,
    );
    expect(
      isRetryableIngestError(new Error("transform worker exited (1/SIGKILL)")),
    ).toBe(true);
    expect(isRetryableIngestError(new Error("ECONNRESET"))).toBe(true);
    expect(isRetryableIngestError(new Error("PINELLAS_SCRIPT_EXIT_1"))).toBe(
      false,
    );
    expect(isRetryableIngestError(new Error("missing Parcel Summary"))).toBe(
      false,
    );
    expect(retryDelayMs(new Error("PCPAO print HTTP 403 for x"), 1)).toBe(2000);
    expect(retryDelayMs(new Error("PCPAO print HTTP 500 for x"), 2)).toBe(500);
    expect(isFatalIngestError(new Error("ENOSPC: no space left"))).toBe(true);
    expect(isFatalIngestError(new Error("PCPAO print HTTP 403 for x"))).toBe(
      false,
    );
  });

  it("pauses print fetches after a burst of 403s", async () => {
    const gate = createRateLimitGate({
      pauseAfter: 2,
      pauseMs: 40,
      maxPauseMs: 40,
    });
    gate.noteFailure(new Error("PCPAO print HTTP 403 for a"));
    const started = Date.now();
    await gate.beforeFetch();
    expect(Date.now() - started).toBeLessThan(20);
    gate.noteFailure(new Error("PCPAO print HTTP 403 for b"));
    const pausedStarted = Date.now();
    await gate.beforeFetch();
    expect(Date.now() - pausedStarted).toBeGreaterThanOrEqual(30);
    gate.noteSuccess();
    const afterSuccess = Date.now();
    await gate.beforeFetch();
    expect(Date.now() - afterSuccess).toBeLessThan(20);
  });

  it("treats a missing transformed.zip as incomplete", () => {
    expect(hasCompletedTransform("/tmp/does-not-exist-pinellas-strap")).toBe(
      false,
    );
  });

  it("limits concurrent limiter jobs", async () => {
    let inFlight = 0;
    let maxInFlight = 0;
    const limiter = createLimiter(2);
    await Promise.all(
      [1, 2, 3, 4].map((value) =>
        limiter.run(async () => {
          inFlight += 1;
          maxInFlight = Math.max(maxInFlight, inFlight);
          await new Promise((resolve) => {
            setTimeout(resolve, 20);
          });
          inFlight -= 1;
          return value;
        }),
      ),
    );
    expect(maxInFlight).toBe(2);
  });

  it("re-runs county scripts in one process without killing the worker on exit(1)", async () => {
    const { mkdtemp, mkdir, writeFile, rm } = await import("node:fs/promises");
    const os = await import("node:os");
    const path = await import("node:path");
    const root = await mkdtemp(path.join(os.tmpdir(), "pinellas-worker-"));
    const scriptsDirectory = path.join(root, "scripts");
    const workDir = path.join(root, "work");
    await mkdir(scriptsDirectory);
    await mkdir(workDir);
    for (const name of [
      "ownerMapping.js",
      "structureMapping.js",
      "layoutMapping.js",
      "utilityMapping.js",
    ]) {
      await writeFile(path.join(scriptsDirectory, name), "", "utf8");
    }
    await writeFile(
      path.join(scriptsDirectory, "data_extractor.js"),
      `const fs = require("fs");
fs.mkdirSync("data", { recursive: true });
fs.writeFileSync("data/property.json", JSON.stringify({ property_usage_type: "Residential" }));
`,
      "utf8",
    );
    const { transformParcel } = require("../../scripts/pinellas-transform-worker.cjs");
    expect(transformParcel(scriptsDirectory, workDir).propertyUsageType).toBe(
      "Residential",
    );
    await writeFile(
      path.join(scriptsDirectory, "ownerMapping.js"),
      "process.exit(1);\n",
      "utf8",
    );
    expect(() => transformParcel(scriptsDirectory, workDir)).toThrow(
      /PINELLAS_SCRIPT_EXIT_1/,
    );
    await rm(root, { recursive: true, force: true });
  });
});
