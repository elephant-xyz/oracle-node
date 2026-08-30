#!/usr/bin/env node
/**
 * Stream local Sunbiz cordata fixed-width text and emit Hillsborough ZIP-prefix
 * matched corporate JSONL chunks for lexicon transform.
 *
 * Local-only: filesystem paths to cordata*.txt (from system `unzip` of
 * cordata.zip). Does not use S3 or AWS.
 */

import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";

import {
  extractCorporateDataLinesByZip,
  normalizeZipPrefixes,
} from "../../workflow/lambdas/permit-harvest-worker/sunbiz-corporate.mjs";

/**
 * @typedef {object} FilterOptions
 * @property {string[]} sources - Local filesystem paths to cordata*.txt files.
 * @property {string[]} zipPrefixes - ZIP prefixes to match.
 * @property {string} outputDir - Output directory for chunks + manifest.
 * @property {number} chunkRecordLimit - Records per JSONL chunk.
 * @property {number | null} maxRecords - Optional matched-record cap.
 * @property {string} jobId - Job id written into the manifest.
 */

/**
 * @param {readonly string[]} argv - CLI args.
 * @returns {Promise<FilterOptions>}
 */
export async function parseFilterSunbizArgs(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      source: { type: "string", multiple: true },
      "zip-prefixes-file": { type: "string" },
      "zip-prefix": { type: "string", multiple: true },
      "output-dir": { type: "string" },
      "chunk-record-limit": { type: "string" },
      "max-records": { type: "string" },
      "job-id": { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });

  /** @type {string[]} */
  let zipPrefixes = [];
  if (typeof values["zip-prefixes-file"] === "string") {
    const parsed = JSON.parse(
      await readFile(values["zip-prefixes-file"], "utf8"),
    );
    if (Array.isArray(parsed)) {
      zipPrefixes = parsed.map(String);
    } else if (
      parsed &&
      typeof parsed === "object" &&
      Array.isArray(/** @type {{ prefixes?: unknown }} */ (parsed).prefixes)
    ) {
      zipPrefixes = /** @type {{ prefixes: string[] }} */ (parsed).prefixes.map(
        String,
      );
    } else {
      throw new Error(
        "zip-prefixes-file must be a JSON array or { prefixes: string[] }",
      );
    }
  }
  if (Array.isArray(values["zip-prefix"])) {
    zipPrefixes.push(...values["zip-prefix"].map(String));
  }
  if (zipPrefixes.length === 0) {
    zipPrefixes = ["335", "336"];
  }

  const sources = Array.isArray(values.source) ? values.source.map(String) : [];
  if (sources.length === 0) {
    throw new Error(
      "At least one --source local path to cordata*.txt is required",
    );
  }
  for (const source of sources) {
    if (source.startsWith("s3://")) {
      throw new Error(
        `Local-only filter rejects S3 URI: ${source}. Pass a filesystem path.`,
      );
    }
  }

  const chunkRaw = values["chunk-record-limit"];
  const maxRaw = values["max-records"];
  return {
    sources,
    zipPrefixes: normalizeZipPrefixes(zipPrefixes),
    outputDir:
      typeof values["output-dir"] === "string"
        ? values["output-dir"]
        : "downloads/hillsborough/sunbiz-pilot/corporate-by-zip",
    chunkRecordLimit:
      typeof chunkRaw === "string" && Number(chunkRaw) > 0
        ? Math.floor(Number(chunkRaw))
        : 5000,
    maxRecords:
      typeof maxRaw === "string" && Number(maxRaw) > 0
        ? Math.floor(Number(maxRaw))
        : null,
    jobId:
      typeof values["job-id"] === "string"
        ? values["job-id"]
        : "sunbiz-hillsborough-pilot",
  };
}

/**
 * @param {string} source - Local file path.
 * @returns {AsyncIterable<string>}
 */
function openLocalLines(source) {
  return createInterface({
    input: createReadStream(source, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });
}

/**
 * @param {FilterOptions} options - Filter options.
 * @returns {Promise<object>}
 */
export async function runFilterSunbizByZip(options) {
  await mkdir(options.outputDir, { recursive: true });
  const chunksDir = path.join(options.outputDir, "chunks");
  await mkdir(chunksDir, { recursive: true });

  /** @type {object[]} */
  const entrySummaries = [];
  let globalMatched = 0;
  let globalRead = 0;
  let globalInvalid = 0;
  let remaining = options.maxRecords === null ? null : options.maxRecords;

  for (const source of options.sources) {
    if (remaining !== null && remaining <= 0) break;
    const absoluteSource = path.resolve(source);
    const sourceFileName = path.basename(absoluteSource);
    const lines = openLocalLines(absoluteSource);

    /** @type {object[]} */
    const chunkReceipts = [];
    const summary = await extractCorporateDataLinesByZip({
      lines,
      zipPrefixes: options.zipPrefixes,
      chunkRecordLimit: options.chunkRecordLimit,
      maxRecords: remaining,
      sourceFileName,
      sourceDataS3Uri: `file://${absoluteSource}`,
      sourceFormat: "text",
      onChunk: async (chunk) => {
        const fileName = `chunk-${String(chunk.chunkIndex).padStart(4, "0")}-${sourceFileName}.jsonl`;
        const filePath = path.join(chunksDir, fileName);
        const stream = createWriteStream(filePath, { encoding: "utf8" });
        for (const record of chunk.records) {
          const ok = stream.write(`${JSON.stringify(record)}\n`);
          if (!ok) {
            await new Promise((resolve) => stream.once("drain", resolve));
          }
        }
        await new Promise((resolve, reject) => {
          stream.end(() => resolve(undefined));
          stream.on("error", reject);
        });
        const receipt = {
          chunkIndex: chunk.chunkIndex,
          recordCount: chunk.records.length,
          localPath: filePath,
          fileName,
        };
        chunkReceipts.push(receipt);
        return receipt;
      },
    });

    globalMatched += Number(summary.matchedRecordCount ?? 0);
    globalRead += Number(summary.sourceRecordsRead ?? 0);
    globalInvalid += Number(summary.invalidRecordCount ?? 0);
    if (remaining !== null) {
      remaining = Math.max(
        0,
        remaining - Number(summary.matchedRecordCount ?? 0),
      );
    }

    const entrySummaryPath = path.join(
      options.outputDir,
      `entry-${sourceFileName}.summary.json`,
    );
    const entrySummary = {
      ...summary,
      chunks: chunkReceipts,
      source: absoluteSource,
    };
    await writeFile(
      entrySummaryPath,
      `${JSON.stringify(entrySummary, null, 2)}\n`,
    );
    entrySummaries.push({
      source: absoluteSource,
      sourceFileName,
      summaryPath: entrySummaryPath,
      matchedRecordCount: summary.matchedRecordCount,
      sourceRecordsRead: summary.sourceRecordsRead,
      chunks: chunkReceipts,
    });
  }

  const manifest = {
    jobId: options.jobId,
    zipPrefixes: options.zipPrefixes,
    outputDir: options.outputDir,
    matchedRecordCount: globalMatched,
    sourceRecordsRead: globalRead,
    invalidRecordCount: globalInvalid,
    entries: entrySummaries,
  };
  const manifestPath = path.join(options.outputDir, "manifest.json");
  await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
  return manifest;
}

/**
 * Derive ZIP prefixes from the pilot seed CSV.
 *
 * @param {string} seedPath - Pilot seed CSV.
 * @returns {Promise<{ exactZips: string[], prefixes: string[] }>}
 */
export async function deriveZipPrefixesFromSeed(seedPath) {
  const text = await readFile(seedPath, "utf8");
  const lines = text
    .replace(/^\uFEFF/, "")
    .split(/\r?\n/)
    .filter((l) => l.length > 0);
  if (lines.length === 0) return { exactZips: [], prefixes: [] };
  /** @type {Set<string>} */
  const exact = new Set();
  for (const line of lines.slice(1)) {
    const match = /,(33[56]\d{2}),/.exec(line);
    if (match?.[1]) exact.add(match[1]);
  }
  const exactZips = [...exact].sort();
  /** @type {string[]} */
  const prefixes = [...new Set(exactZips.map((z) => z.slice(0, 3)))];
  if (!prefixes.includes("335")) prefixes.push("335");
  if (!prefixes.includes("336")) prefixes.push("336");
  prefixes.sort();
  return { exactZips, prefixes };
}

async function main() {
  const options = await parseFilterSunbizArgs(process.argv.slice(2));
  const manifest = await runFilterSunbizByZip(options);
  console.log(
    JSON.stringify(
      { event: "hillsborough_sunbiz_filter_finished", ...manifest },
      null,
      2,
    ),
  );
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
