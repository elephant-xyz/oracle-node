#!/usr/bin/env node

import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { existsSync } from "node:fs";
import {
  appendFile,
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const require = createRequire(import.meta.url);
const AdmZip = require("adm-zip");

const DEFAULT_SEED_PATH = "data/seeds/pinellas-pilot.csv";
const DEFAULT_FLOW_PATH = "multi-request-flows/Pinellas.json";
const DEFAULT_SCRIPTS_DIRECTORY =
  "../Counties-trasform-scripts/pinellas/scripts";
const DEFAULT_OUTPUT_DIRECTORY = "downloads/pinellas/local-ingest";
const ELEPHANT_CLI_ENTRY = path.join(
  "node_modules",
  "@elephant-xyz",
  "cli",
  "dist",
  "index.js",
);
const LOCAL_IPFS_SHIM_PATH = "scripts/local-ipfs-fetch-shim.cjs";
const LOCAL_IPFS_GATEWAY = "http://127.0.0.1:8080";
const DEFAULT_CONCURRENCY = 2;
const PRINT_URL = "https://www.pcpao.gov/property/detail/print";
const PRINT_USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const MAPPING_SCRIPT_NAMES = Object.freeze([
  "ownerMapping.js",
  "structureMapping.js",
  "layoutMapping.js",
  "utilityMapping.js",
]);
const FALLBACK_SCRIPTS_DIRECTORIES = Object.freeze([
  "/tmp/Counties-trasform-scripts/pinellas/scripts",
  "downloads/Counties-trasform-scripts/pinellas/scripts",
]);

/**
 * @typedef {Record<string, string>} SeedRow
 *
 * @typedef {"scripts" | "elephant-cli"} TransformMode
 *
 * @typedef {object} LocalIngestCliOptions
 * @property {string} seedPath - Pinellas seed CSV.
 * @property {string} flowPath - Multi-request flow JSON.
 * @property {string} scriptsDirectory - Existing Pinellas transform scripts.
 * @property {string} outputDirectory - Durable local output directory.
 * @property {number | null} limit - Optional row cap after mixed selection.
 * @property {boolean} allRows - When true, ingest every seed row instead of one per use group.
 * @property {boolean} skipValidate - When true, skip `elephant-cli validate`.
 * @property {boolean} skipExisting - When true, skip parcels that already have `transformed.zip`.
 * @property {number} concurrency - Maximum in-flight parcels.
 * @property {TransformMode} transformMode - How county scripts are executed.
 * @property {boolean} useCliPrepare - When true, fetch via `elephant-cli prepare` instead of direct HTTP.
 *
 * @typedef {object} SourceHttpRequest
 * @property {string} url - Path-only print URL.
 * @property {string} method - HTTP method.
 * @property {Record<string, string>} headers - Request headers.
 * @property {Record<string, string[]>} multiValueQueryString - Print query parameters.
 *
 * @typedef {object} PropertySeedJson
 * @property {SourceHttpRequest} source_http_request - Lexicon request metadata.
 * @property {string} request_identifier - STRAP.
 * @property {string} parcel_id - STRAP.
 *
 * @typedef {object} UnnormalizedAddressJson
 * @property {SourceHttpRequest} source_http_request - Lexicon request metadata.
 * @property {string} request_identifier - STRAP.
 * @property {string} full_address - Seed situs line, possibly empty.
 * @property {string} county_jurisdiction - County name.
 *
 * @typedef {object} ParcelIngestResult
 * @property {string} parcelId - 18-digit STRAP.
 * @property {string} useGroup - Seed use-group label.
 * @property {boolean} prepareSuccess - Whether print HTML was obtained.
 * @property {boolean} transformSuccess - Whether the Pinellas scripts transform completed.
 * @property {boolean | null} validationSuccess - Lexicon validate result, or null when skipped.
 * @property {string | null} propertyUsageType - Transformed `property.json` usage type.
 * @property {string | null} error - First failure message.
 * @property {boolean} skippedExisting - True when `transformed.zip` was already present.
 *
 * @typedef {object} IngestStatusSnapshot
 * @property {string} startedAt - ISO timestamp when the run began.
 * @property {string} updatedAt - ISO timestamp of this snapshot.
 * @property {number} total - Selected seed rows.
 * @property {number} completed - Finished workers (success, skip, or fail).
 * @property {number} skippedExisting - Parcels reused from disk.
 * @property {number} transformsPassed - Successful transforms including skips.
 * @property {number} transformsFailed - Failed parcels.
 * @property {number} concurrency - Worker count.
 * @property {string} seedPath - Seed CSV path.
 * @property {string} outputDirectory - Output root.
 */

/**
 * Quote one CSV cell using RFC 4180.
 *
 * @param {string} value - Cell value.
 * @returns {string} Encoded cell.
 */
export function encodeCsvCell(value) {
  if (!/[",\r\n]/.test(value)) return value;
  return `"${value.replaceAll('"', '""')}"`;
}

/**
 * Parse RFC 4180 CSV text into row objects.
 *
 * @param {string} text - Complete CSV document.
 * @returns {SeedRow[]} Parsed records.
 */
export function parseCsvRecords(text) {
  /** @type {string[][]} */
  const table = [];
  /** @type {string[]} */
  let row = [];
  let cell = "";
  let inQuotes = false;
  const source = text.endsWith("\n") ? text : `${text}\n`;
  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];
    if (inQuotes) {
      if (character === '"') {
        if (source[index + 1] === '"') {
          cell += '"';
          index += 1;
        } else {
          inQuotes = false;
        }
      } else {
        cell += character;
      }
      continue;
    }
    if (character === '"') {
      inQuotes = true;
      continue;
    }
    if (character === ",") {
      row.push(cell);
      cell = "";
      continue;
    }
    if (character === "\n") {
      row.push(cell);
      table.push(row);
      row = [];
      cell = "";
      continue;
    }
    if (character !== "\r") cell += character;
  }
  if (table.length === 0) return [];
  const [header, ...body] = table;
  return body
    .filter((values) => values.some((value) => value.length > 0))
    .map((values) => {
      /** @type {SeedRow} */
      const record = {};
      for (let index = 0; index < header.length; index += 1) {
        record[header[index]] = values[index] ?? "";
      }
      return record;
    });
}

/**
 * Select one seed row per `use_group`, preserving first-seen order.
 *
 * @param {readonly SeedRow[]} rows - Complete seed rows.
 * @returns {SeedRow[]} Mixed-type subset.
 */
export function selectMixedRows(rows) {
  /** @type {SeedRow[]} */
  const selected = [];
  const seen = new Set();
  for (const row of rows) {
    const useGroup = row.use_group ?? "";
    if (useGroup.length === 0 || seen.has(useGroup)) continue;
    seen.add(useGroup);
    selected.push(row);
  }
  return selected;
}

/**
 * Extract print-page HTML from a multi-request prepare capture.
 *
 * @param {unknown} capture - Parsed `{STRAP}.json` prepare artifact.
 * @returns {string} HTML document.
 */
export function unwrapPropertyPrintHtml(capture) {
  if (capture === null || typeof capture !== "object") {
    throw new Error("Prepare capture is not an object");
  }
  const print = /** @type {{ PropertyPrint?: { response?: unknown } }} */ (
    capture
  ).PropertyPrint;
  const html = print?.response;
  if (typeof html !== "string" || !html.toLowerCase().includes("<html")) {
    throw new Error("PropertyPrint response is not HTML");
  }
  return html;
}

/**
 * Parse local-ingest CLI flags.
 *
 * @param {readonly string[]} argv - Arguments after the script path.
 * @returns {LocalIngestCliOptions} Validated options.
 */
export function parseCliOptions(argv) {
  /** @type {LocalIngestCliOptions} */
  const options = {
    seedPath: DEFAULT_SEED_PATH,
    flowPath: DEFAULT_FLOW_PATH,
    scriptsDirectory: DEFAULT_SCRIPTS_DIRECTORY,
    outputDirectory: DEFAULT_OUTPUT_DIRECTORY,
    limit: null,
    allRows: false,
    skipValidate: false,
    skipExisting: true,
    concurrency: DEFAULT_CONCURRENCY,
    transformMode: "scripts",
    useCliPrepare: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index];
    if (flag === "--all") {
      options.allRows = true;
      continue;
    }
    if (flag === "--skip-validate") {
      options.skipValidate = true;
      continue;
    }
    if (flag === "--skip-existing") {
      options.skipExisting = true;
      continue;
    }
    if (flag === "--force") {
      options.skipExisting = false;
      continue;
    }
    if (flag === "--cli-transform") {
      options.transformMode = "elephant-cli";
      continue;
    }
    if (flag === "--use-cli-prepare") {
      options.useCliPrepare = true;
      continue;
    }
    const value = argv[index + 1];
    if (typeof value !== "string" || value.startsWith("--")) {
      throw new Error(`Missing value for ${flag}`);
    }
    index += 1;
    if (flag === "--seed") options.seedPath = value;
    else if (flag === "--flow") options.flowPath = value;
    else if (flag === "--scripts") options.scriptsDirectory = value;
    else if (flag === "--output") options.outputDirectory = value;
    else if (flag === "--limit") options.limit = Number.parseInt(value, 10);
    else if (flag === "--concurrency") {
      options.concurrency = Number.parseInt(value, 10);
    } else throw new Error(`Unknown option: ${flag}`);
  }
  if (
    options.limit !== null &&
    (!Number.isInteger(options.limit) || options.limit <= 0)
  ) {
    throw new Error("--limit must be a positive integer");
  }
  if (!Number.isInteger(options.concurrency) || options.concurrency <= 0) {
    throw new Error("--concurrency must be a positive integer");
  }
  return options;
}

/**
 * Render one seed row as a complete CSV document.
 *
 * @param {SeedRow} row - Seed record.
 * @returns {string} Header plus one data row.
 */
export function renderSeedCsv(row) {
  const columns = Object.keys(row);
  const header = columns.map(encodeCsvCell).join(",");
  const line = columns.map((column) => encodeCsvCell(row[column] ?? "")).join(
    ",",
  );
  return `${header}\n${line}\n`;
}

/**
 * Keep zip entries that lexicon `validate` is allowed to see.
 *
 * @param {string} entryName - Archive member path.
 * @returns {boolean} False for leftover `fact_sheet.json`.
 */
export function shouldKeepValidationEntry(entryName) {
  const base = entryName.split("/").pop() ?? entryName;
  return base !== "fact_sheet.json";
}

/**
 * Strip `?query` from lexicon `source_http_request.url` values.
 *
 * @param {unknown} value - JSON tree.
 * @returns {unknown} Tree with path-only request URLs.
 */
export function stripQueryFromSourceHttpRequestTree(value) {
  if (Array.isArray(value)) {
    return value.map((item) => stripQueryFromSourceHttpRequestTree(item));
  }
  if (value === null || typeof value !== "object") return value;
  const record = /** @type {Record<string, unknown>} */ (value);
  /** @type {Record<string, unknown>} */
  const next = {};
  for (const [key, child] of Object.entries(record)) {
    next[key] = stripQueryFromSourceHttpRequestTree(child);
  }
  if (typeof next.url === "string" && next.url.includes("?")) {
    const [base, query] = next.url.split("?");
    const params = new URLSearchParams(query);
    const existing = next.multiValueQueryString;
    /** @type {Record<string, string[]>} */
    const multi =
      existing && typeof existing === "object"
        ? { .../** @type {Record<string, string[]>} */ (existing) }
        : {};
    for (const [paramKey, paramValue] of params.entries()) {
      if (!multi[paramKey]) multi[paramKey] = [paramValue];
    }
    next.url = base;
    next.multiValueQueryString = multi;
  }
  return next;
}

/**
 * Build the PCPAO print URL for one STRAP. Query stays on the request, not in
 * lexicon `source_http_request.url`.
 *
 * @param {string} strap - 18-digit STRAP.
 * @returns {string} Absolute print URL including `is_print` and `s`.
 */
export function buildPrintPageUrl(strap) {
  const url = new URL(PRINT_URL);
  url.searchParams.set("is_print", "1");
  url.searchParams.set("s", strap);
  return url.toString();
}

/**
 * Parse a seed `multiValueQueryString` cell.
 *
 * @param {string | undefined} raw - JSON object text.
 * @param {string} strap - Fallback STRAP for `s`.
 * @returns {Record<string, string[]>} Query map.
 */
export function parseSeedQueryString(raw, strap) {
  if (typeof raw === "string" && raw.trim().length > 0) {
    try {
      const parsed = JSON.parse(raw);
      if (parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)) {
        /** @type {Record<string, string[]>} */
        const out = {};
        for (const [key, value] of Object.entries(
          /** @type {Record<string, unknown>} */ (parsed),
        )) {
          if (Array.isArray(value) && value.every((item) => typeof item === "string")) {
            out[key] = value;
          }
        }
        if (Object.keys(out).length > 0) return out;
      }
    } catch {
      // Fall through to the STRAP default.
    }
  }
  return { is_print: ["1"], s: [strap] };
}

/**
 * Build lexicon `source_http_request` for a Pinellas print GET.
 *
 * @param {SeedRow} row - Seed record.
 * @returns {SourceHttpRequest} Path-only request metadata.
 */
export function buildSourceHttpRequest(row) {
  const strap = row.parcel_id;
  return {
    url: row.url && row.url.length > 0 ? row.url : PRINT_URL,
    method: row.method && row.method.length > 0 ? row.method : "GET",
    headers: {
      "User-Agent": PRINT_USER_AGENT,
      Accept: "text/html",
    },
    multiValueQueryString: parseSeedQueryString(row.multiValueQueryString, strap),
  };
}

/**
 * Build the seed JSON files elephant-cli / Pinellas scripts expect.
 *
 * @param {SeedRow} row - Seed record.
 * @returns {{ propertySeed: PropertySeedJson, unnormalizedAddress: UnnormalizedAddressJson, seedCsv: string }}
 *   Seed files.
 */
export function buildSeedJsonFiles(row) {
  const sourceHttpRequest = buildSourceHttpRequest(row);
  const strap = row.parcel_id;
  const situs = row.situs_address || row.address || "";
  return {
    propertySeed: {
      source_http_request: sourceHttpRequest,
      request_identifier: strap,
      parcel_id: strap,
    },
    unnormalizedAddress: {
      source_http_request: sourceHttpRequest,
      request_identifier: strap,
      full_address: situs,
      county_jurisdiction: row.county || "Pinellas",
    },
    seedCsv: renderSeedCsv(row),
  };
}

/**
 * True when a parcel directory already has a transformed archive.
 *
 * @param {string} parcelDir - Per-STRAP output directory.
 * @returns {boolean} Whether `transformed.zip` exists.
 */
export function hasCompletedTransform(parcelDir) {
  return existsSync(path.join(parcelDir, "transformed.zip"));
}

/**
 * Run an async mapper with a fixed worker pool.
 *
 * @template T
 * @template R
 * @param {readonly T[]} items - Work items.
 * @param {number} concurrency - Worker count.
 * @param {(item: T, index: number) => Promise<R>} worker - Per-item mapper.
 * @returns {Promise<R[]>} Results in input order.
 */
export async function mapWithConcurrency(items, concurrency, worker) {
  /** @type {R[]} */
  const results = new Array(items.length);
  let nextIndex = 0;
  const workerCount = Math.max(1, Math.min(concurrency, items.length));
  await Promise.all(
    Array.from({ length: workerCount }, async () => {
      while (true) {
        const index = nextIndex;
        nextIndex += 1;
        if (index >= items.length) return;
        const item = items[index];
        if (item === undefined) return;
        results[index] = await worker(item, index);
      }
    }),
  );
  return results;
}

/**
 * Resolve the Pinellas scripts directory, including this VM's `/tmp` clone.
 *
 * @param {string} configured - CLI `--scripts` value.
 * @param {string} repoRoot - oracle-node root.
 * @returns {string} Existing scripts directory.
 */
export function resolveScriptsDirectory(configured, repoRoot) {
  const candidates = [configured, ...FALLBACK_SCRIPTS_DIRECTORIES];
  for (const candidate of candidates) {
    const resolved = path.resolve(repoRoot, candidate);
    if (existsSync(path.join(resolved, "data_extractor.js"))) return resolved;
  }
  throw new Error(
    `Pinellas transform scripts not found. Tried: ${candidates.join(", ")}`,
  );
}

/**
 * Fetch PCPAO print HTML with a Chrome UA. Retries UA-sensitive 403/429/5xx.
 *
 * @param {string} strap - 18-digit STRAP.
 * @param {typeof fetch} [fetchImpl] - Injected fetch.
 * @param {number} [attempts] - Total tries.
 * @returns {Promise<string>} Print HTML.
 */
export async function fetchPropertyPrintHtml(
  strap,
  fetchImpl = fetch,
  attempts = 4,
) {
  const url = buildPrintPageUrl(strap);
  /** @type {Error} */
  let lastError = new Error(`PCPAO print fetch failed for ${strap}`);
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const response = await fetchImpl(url, {
        headers: {
          "User-Agent": PRINT_USER_AGENT,
          Accept: "text/html",
        },
      });
      if (!response.ok) {
        throw new Error(`PCPAO print HTTP ${response.status} for ${strap}`);
      }
      const html = await response.text();
      if (!html.toLowerCase().includes("<html")) {
        throw new Error(`PropertyPrint response is not HTML for ${strap}`);
      }
      if (!/Parcel Summary/i.test(html)) {
        throw new Error(`PCPAO print HTML is missing Parcel Summary for ${strap}`);
      }
      return html;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      if (attempt === attempts) break;
      await sleep(1000 * 2 ** (attempt - 1));
    }
  }
  throw lastError;
}

/**
 * @param {number} milliseconds - Delay.
 * @returns {Promise<void>} Resolves after the delay.
 */
function sleep(milliseconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

/**
 * @param {string} command - Process to spawn.
 * @param {readonly string[]} args - Process arguments.
 * @param {string} cwd - Working directory.
 * @param {NodeJS.ProcessEnv} [extraEnv] - Extra environment variables.
 * @param {boolean} [inheritStdio] - When true, stream child stdio.
 * @returns {Promise<void>} Resolves when the process exits 0.
 */
function runCommand(command, args, cwd, extraEnv, inheritStdio = false) {
  return new Promise((resolve, reject) => {
    /** @type {Buffer[]} */
    const stdout = [];
    /** @type {Buffer[]} */
    const stderr = [];
    const child = spawn(command, [...args], {
      cwd,
      stdio: inheritStdio ? "inherit" : ["ignore", "pipe", "pipe"],
      env: extraEnv ? { ...process.env, ...extraEnv } : process.env,
    });
    if (!inheritStdio) {
      child.stdout?.on("data", (chunk) => stdout.push(chunk));
      child.stderr?.on("data", (chunk) => stderr.push(chunk));
    }
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      const detail = inheritStdio
        ? ""
        : ` ${Buffer.concat(stderr).toString("utf8") || Buffer.concat(stdout).toString("utf8")}`;
      reject(new Error(`${command} ${args.join(" ")} exited ${code}.${detail}`));
    });
  });
}

/**
 * @param {string} zipPath - Archive to write.
 * @param {Record<string, Buffer | string>} files - Arcname to bytes or UTF-8 text.
 * @returns {void}
 */
function writeZipFromContents(zipPath, files) {
  const zip = new AdmZip();
  for (const [arcname, contents] of Object.entries(files)) {
    const buffer = Buffer.isBuffer(contents)
      ? contents
      : Buffer.from(contents, "utf8");
    zip.addFile(arcname, buffer);
  }
  zip.writeZip(zipPath);
}

/**
 * @param {string} zipPath - Archive to read.
 * @param {string} entryName - Entry path.
 * @returns {Buffer} Entry bytes.
 */
function readZipEntrySync(zipPath, entryName) {
  const zip = new AdmZip(zipPath);
  const entry = zip.getEntry(entryName);
  if (entry === null) {
    throw new Error(`zip entry missing: ${entryName} in ${zipPath}`);
  }
  return entry.getData();
}

/**
 * Rewrite JSON members in a transformed zip so `source_http_request.url` has no query string.
 *
 * @param {string} zipPath - Transformed archive.
 * @returns {Promise<void>} Resolves when the archive is rewritten.
 */
export async function stripQueryFromTransformedZip(zipPath) {
  const source = new AdmZip(zipPath);
  const dest = new AdmZip();
  for (const entry of source.getEntries()) {
    let data = entry.getData();
    if (entry.entryName.endsWith(".json")) {
      try {
        const parsed = JSON.parse(data.toString("utf8"));
        data = Buffer.from(
          JSON.stringify(stripQueryFromSourceHttpRequestTree(parsed), null, 2),
          "utf8",
        );
      } catch {
        // Leave non-JSON members unchanged.
      }
    }
    dest.addFile(entry.entryName, data);
  }
  dest.writeZip(zipPath);
}

/**
 * Inject lexicon request metadata onto every JSON object in `data/`.
 *
 * @param {string} dataDir - Transform output directory.
 * @param {SourceHttpRequest} sourceHttpRequest - Path-only request.
 * @param {string} requestIdentifier - STRAP.
 * @returns {Promise<void>} Resolves when files are rewritten.
 */
async function injectSourceHttpRequest(dataDir, sourceHttpRequest, requestIdentifier) {
  const names = await readdir(dataDir);
  await Promise.all(
    names
      .filter((name) => name.endsWith(".json"))
      .map(async (name) => {
        const filePath = path.join(dataDir, name);
        const parsed = JSON.parse(await readFile(filePath, "utf8"));
        if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
          return;
        }
        const record = /** @type {Record<string, unknown>} */ (parsed);
        if (
          record.source_http_request === undefined ||
          record.source_http_request === null
        ) {
          record.source_http_request = sourceHttpRequest;
        }
        record.request_identifier = requestIdentifier;
        const sanitized = stripQueryFromSourceHttpRequestTree(record);
        await writeFile(filePath, `${JSON.stringify(sanitized, null, 2)}\n`, "utf8");
      }),
  );
}

/**
 * Zip every file in `data/` as `data/<name>`.
 *
 * @param {string} dataDir - Directory of JSON outputs.
 * @param {string} zipPath - Destination archive.
 * @returns {Promise<void>} Resolves when written.
 */
async function zipDataDirectory(dataDir, zipPath) {
  const zip = new AdmZip();
  const names = await readdir(dataDir);
  for (const name of names) {
    zip.addLocalFile(path.join(dataDir, name), "data");
  }
  zip.writeZip(zipPath);
}

/**
 * Run one Pinellas mapping script against a working directory.
 *
 * @param {string} scriptPath - Absolute script path.
 * @param {string} cwd - Working directory containing `input.html`.
 * @param {string} nodeModulesPath - `NODE_PATH` for `cheerio`.
 * @returns {Promise<void>} Resolves on exit 0.
 */
function runMappingScript(scriptPath, cwd, nodeModulesPath) {
  return runCommand(
    process.execPath,
    ["--unhandled-rejections=strict", scriptPath],
    cwd,
    { NODE_PATH: nodeModulesPath },
    false,
  );
}

/**
 * Execute Pinellas county scripts in-process (no elephant-cli, no fact sheet).
 *
 * @param {object} params - Transform inputs.
 * @param {string} params.workDir - Temporary working directory.
 * @param {string} params.scriptsDirectory - Scripts folder.
 * @param {string} params.html - Print HTML.
 * @param {PropertySeedJson} params.propertySeed - Seed JSON.
 * @param {UnnormalizedAddressJson} params.unnormalizedAddress - Address JSON.
 * @param {string} params.repoRoot - oracle-node root.
 * @param {string} params.transformedZip - Output archive path.
 * @returns {Promise<string | null>} `property_usage_type`, or null.
 */
async function transformWithCountyScripts({
  workDir,
  scriptsDirectory,
  html,
  propertySeed,
  unnormalizedAddress,
  repoRoot,
  transformedZip,
}) {
  await writeFile(path.join(workDir, "input.html"), html, "utf8");
  await writeFile(
    path.join(workDir, "property_seed.json"),
    `${JSON.stringify(propertySeed, null, 2)}\n`,
    "utf8",
  );
  await writeFile(
    path.join(workDir, "unnormalized_address.json"),
    `${JSON.stringify(unnormalizedAddress, null, 2)}\n`,
    "utf8",
  );
  await mkdir(path.join(workDir, "data"), { recursive: true });
  await mkdir(path.join(workDir, "owners"), { recursive: true });
  const nodeModulesPath = path.join(repoRoot, "node_modules");
  await Promise.all(
    MAPPING_SCRIPT_NAMES.map((name) =>
      runMappingScript(path.join(scriptsDirectory, name), workDir, nodeModulesPath),
    ),
  );
  await runMappingScript(
    path.join(scriptsDirectory, "data_extractor.js"),
    workDir,
    nodeModulesPath,
  );
  const dataDir = path.join(workDir, "data");
  await injectSourceHttpRequest(
    dataDir,
    propertySeed.source_http_request,
    propertySeed.request_identifier,
  );
  await zipDataDirectory(dataDir, transformedZip);
  const propertyPath = path.join(dataDir, "property.json");
  if (!existsSync(propertyPath)) {
    throw new Error("data_extractor.js did not write data/property.json");
  }
  const propertyJson = JSON.parse(await readFile(propertyPath, "utf8"));
  return typeof propertyJson.property_usage_type === "string"
    ? propertyJson.property_usage_type
    : null;
}

/**
 * Ingest one STRAP locally: direct print GET (or CLI prepare) → county transform.
 *
 * @param {object} params - Parcel parameters.
 * @param {SeedRow} params.row - Seed row.
 * @param {LocalIngestCliOptions} params.options - Run options.
 * @param {string} params.scriptsZipPath - Packaged Pinellas scripts.
 * @param {string} params.scriptsDirectory - Unpacked scripts directory.
 * @param {string} params.repoRoot - oracle-node root.
 * @returns {Promise<ParcelIngestResult>} Per-parcel outcome.
 */
async function ingestParcel({
  row,
  options,
  scriptsZipPath,
  scriptsDirectory,
  repoRoot,
}) {
  const parcelId = row.parcel_id;
  const useGroup = row.use_group ?? "";
  const parcelDir = path.join(options.outputDirectory, parcelId);
  await mkdir(parcelDir, { recursive: true });
  if (options.skipExisting && hasCompletedTransform(parcelDir)) {
    return {
      parcelId,
      useGroup,
      prepareSuccess: true,
      transformSuccess: true,
      validationSuccess: null,
      propertyUsageType: null,
      error: null,
      skippedExisting: true,
    };
  }
  const workDir = await mkdtemp(path.join(os.tmpdir(), `pinellas-${parcelId}-`));
  try {
    const seedFiles = buildSeedJsonFiles(row);
    const transformedZip = path.join(parcelDir, "transformed.zip");
    /** @type {string} */
    let html;
    if (options.useCliPrepare) {
      html = await prepareWithElephantCli({
        row,
        options,
        seedFiles,
        parcelDir,
        workDir,
        repoRoot,
      });
    } else {
      html = await fetchPropertyPrintHtml(parcelId);
    }
    await writeFile(path.join(parcelDir, "input.html"), html, "utf8");
    /** @type {string | null} */
    let propertyUsageType;
    if (options.transformMode === "elephant-cli") {
      propertyUsageType = await transformWithElephantCli({
        html,
        seedFiles,
        parcelId,
        workDir,
        parcelDir,
        scriptsZipPath,
        transformedZip,
        repoRoot,
      });
    } else {
      propertyUsageType = await transformWithCountyScripts({
        workDir,
        scriptsDirectory,
        html,
        propertySeed: seedFiles.propertySeed,
        unnormalizedAddress: seedFiles.unnormalizedAddress,
        repoRoot,
        transformedZip,
      });
    }
    await stripQueryFromTransformedZip(transformedZip);
    if (propertyUsageType === null) {
      const propertyJson = JSON.parse(
        readZipEntrySync(transformedZip, "data/property.json").toString("utf8"),
      );
      propertyUsageType =
        typeof propertyJson.property_usage_type === "string"
          ? propertyJson.property_usage_type
          : null;
    }
    /** @type {boolean | null} */
    let validationSuccess = null;
    if (!options.skipValidate) {
      try {
        const shimPath = path.join(repoRoot, LOCAL_IPFS_SHIM_PATH);
        await runCommand(
          process.execPath,
          [
            path.join(repoRoot, ELEPHANT_CLI_ENTRY),
            "validate",
            transformedZip,
            "--output-csv",
            path.join(parcelDir, "validation.csv"),
          ],
          repoRoot,
          {
            PINELLAS_IPFS_GATEWAY:
              process.env.PINELLAS_IPFS_GATEWAY ?? LOCAL_IPFS_GATEWAY,
            NODE_OPTIONS: `--require ${shimPath}`,
          },
        );
        validationSuccess = true;
      } catch (error) {
        validationSuccess = false;
        return {
          parcelId,
          useGroup,
          prepareSuccess: true,
          transformSuccess: true,
          validationSuccess,
          propertyUsageType,
          error: error instanceof Error ? error.message : String(error),
          skippedExisting: false,
        };
      }
    }
    return {
      parcelId,
      useGroup,
      prepareSuccess: true,
      transformSuccess: true,
      validationSuccess,
      propertyUsageType,
      error: null,
      skippedExisting: false,
    };
  } catch (error) {
    await writeFile(
      path.join(parcelDir, "error.txt"),
      error instanceof Error ? error.message : String(error),
      "utf8",
    );
    return {
      parcelId,
      useGroup,
      prepareSuccess: false,
      transformSuccess: false,
      validationSuccess: null,
      propertyUsageType: null,
      error: error instanceof Error ? error.message : String(error),
      skippedExisting: false,
    };
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}

/**
 * Fetch print HTML via `elephant-cli prepare` (legacy path).
 *
 * @param {object} params - Prepare inputs.
 * @param {SeedRow} params.row - Seed row.
 * @param {LocalIngestCliOptions} params.options - Run options.
 * @param {{ seedCsv: string, propertySeed: PropertySeedJson, unnormalizedAddress: UnnormalizedAddressJson }} params.seedFiles
 *   Seed files.
 * @param {string} params.parcelDir - Durable parcel directory.
 * @param {string} params.workDir - Temp directory.
 * @param {string} params.repoRoot - oracle-node root.
 * @returns {Promise<string>} Print HTML.
 */
async function prepareWithElephantCli({
  row,
  options,
  seedFiles,
  parcelDir,
  workDir,
  repoRoot,
}) {
  const parcelId = row.parcel_id;
  const seedCsvPath = path.join(workDir, "seed.csv");
  await writeFile(seedCsvPath, seedFiles.seedCsv, "utf8");
  const countyPrepZip = path.join(workDir, "county-prep-input.zip");
  const preparedZip = path.join(parcelDir, "prepared.zip");
  writeZipFromContents(countyPrepZip, {
    "unnormalized_address.json": `${JSON.stringify(seedFiles.unnormalizedAddress, null, 2)}\n`,
    "property_seed.json": `${JSON.stringify(seedFiles.propertySeed, null, 2)}\n`,
    "input.csv": seedFiles.seedCsv,
  });
  await runCommand(
    process.execPath,
    [
      path.join(repoRoot, ELEPHANT_CLI_ENTRY),
      "prepare",
      countyPrepZip,
      "--multi-request-flow-file",
      path.resolve(repoRoot, options.flowPath),
      "--output-zip",
      preparedZip,
    ],
    repoRoot,
  );
  const captureBytes = readZipEntrySync(preparedZip, `${parcelId}.json`);
  return unwrapPropertyPrintHtml(JSON.parse(captureBytes.toString("utf8")));
}

/**
 * Transform via `elephant-cli transform --scripts-zip`.
 *
 * @param {object} params - Transform inputs.
 * @param {string} params.html - Print HTML.
 * @param {{ seedCsv: string, propertySeed: PropertySeedJson, unnormalizedAddress: UnnormalizedAddressJson }} params.seedFiles
 *   Seed files.
 * @param {string} params.parcelId - STRAP.
 * @param {string} params.workDir - Temp directory.
 * @param {string} params.parcelDir - Durable parcel directory.
 * @param {string} params.scriptsZipPath - Packaged scripts.
 * @param {string} params.transformedZip - Output archive.
 * @param {string} params.repoRoot - oracle-node root.
 * @returns {Promise<string | null>} Usage type when readable.
 */
async function transformWithElephantCli({
  html,
  seedFiles,
  parcelId,
  workDir,
  parcelDir,
  scriptsZipPath,
  transformedZip,
  repoRoot,
}) {
  const preparedWithHtmlZip = path.join(parcelDir, "prepared-with-html.zip");
  writeZipFromContents(preparedWithHtmlZip, {
    "unnormalized_address.json": `${JSON.stringify(seedFiles.unnormalizedAddress, null, 2)}\n`,
    "property_seed.json": `${JSON.stringify(seedFiles.propertySeed, null, 2)}\n`,
    "input.csv": seedFiles.seedCsv,
    [`${parcelId}.json`]: JSON.stringify({
      PropertyPrint: { response: html },
    }),
    "input.html": html,
  });
  await runCommand(
    process.execPath,
    [
      path.join(repoRoot, ELEPHANT_CLI_ENTRY),
      "transform",
      "--input-zip",
      preparedWithHtmlZip,
      "--scripts-zip",
      scriptsZipPath,
      "--output-zip",
      transformedZip,
    ],
    repoRoot,
  );
  return null;
}

/**
 * Package Pinellas transform scripts, excluding backups.
 *
 * @param {string} scriptsDirectory - Scripts folder.
 * @param {string} destination - Zip path.
 * @returns {Promise<void>} Resolves when packaged.
 */
async function packageScripts(scriptsDirectory, destination) {
  const zip = new AdmZip();
  const entries = await readdir(scriptsDirectory, { withFileTypes: true });
  for (const entry of entries) {
    if (entry.isFile() && entry.name.endsWith(".js")) {
      zip.addLocalFile(path.join(scriptsDirectory, entry.name));
    }
  }
  zip.writeZip(destination);
}

/**
 * Write a compact status snapshot for a long-running ingest.
 *
 * @param {string} outputDirectory - Output root.
 * @param {IngestStatusSnapshot} snapshot - Counts.
 * @returns {Promise<void>} Resolves when written.
 */
async function writeStatusSnapshot(outputDirectory, snapshot) {
  await writeFile(
    path.join(outputDirectory, "status.json"),
    `${JSON.stringify(snapshot, null, 2)}\n`,
    "utf8",
  );
}

/**
 * Run the local Pinellas prepare → transform ingest.
 *
 * @param {LocalIngestCliOptions} options - Validated CLI options.
 * @returns {Promise<ParcelIngestResult[]>} Per-parcel results.
 */
export async function runLocalIngest(options) {
  const repoRoot = process.cwd();
  const outputDirectory = path.resolve(options.outputDirectory);
  await mkdir(outputDirectory, { recursive: true });
  const seedText = await readFile(options.seedPath, "utf8");
  const allRows = parseCsvRecords(seedText);
  const selected = options.allRows ? allRows : selectMixedRows(allRows);
  const rows =
    options.limit === null ? selected : selected.slice(0, options.limit);
  const scriptsDirectory = resolveScriptsDirectory(
    options.scriptsDirectory,
    repoRoot,
  );
  const scriptsZipPath = path.join(outputDirectory, "pinellas-scripts.zip");
  if (options.transformMode === "elephant-cli") {
    await packageScripts(scriptsDirectory, scriptsZipPath);
  }
  const startedAt = new Date().toISOString();
  let completed = 0;
  let skippedExisting = 0;
  let transformsPassed = 0;
  let transformsFailed = 0;
  const failuresPath = path.join(outputDirectory, "failures.jsonl");
  await writeFile(failuresPath, "", "utf8");
  const resolvedOptions = { ...options, outputDirectory };
  const results = await mapWithConcurrency(
    rows,
    options.concurrency,
    async (row) => {
      const result = await ingestParcel({
        row,
        options: resolvedOptions,
        scriptsZipPath,
        scriptsDirectory,
        repoRoot,
      });
      completed += 1;
      if (result.skippedExisting) skippedExisting += 1;
      if (result.transformSuccess) transformsPassed += 1;
      else {
        transformsFailed += 1;
        await appendFile(failuresPath, `${JSON.stringify(result)}\n`, "utf8");
      }
      if (completed === 1 || completed % 25 === 0 || completed === rows.length) {
        const snapshot = {
          startedAt,
          updatedAt: new Date().toISOString(),
          total: rows.length,
          completed,
          skippedExisting,
          transformsPassed,
          transformsFailed,
          concurrency: options.concurrency,
          seedPath: options.seedPath,
          outputDirectory,
        };
        await writeStatusSnapshot(outputDirectory, snapshot);
        console.log(JSON.stringify({ event: "pinellas_ingest_progress", ...snapshot }));
      }
      return result;
    },
  );
  await writeFile(
    path.join(outputDirectory, "summary.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        total: results.length,
        skippedExisting: results.filter((result) => result.skippedExisting).length,
        transformsPassed: results.filter((result) => result.transformSuccess)
          .length,
        validationsPassed: results.filter(
          (result) => result.validationSuccess === true,
        ).length,
        failures: results.filter((result) => !result.transformSuccess),
      },
      null,
      2,
    ),
    "utf8",
  );
  return results;
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const options = parseCliOptions(process.argv.slice(2));
  runLocalIngest(options)
    .then((results) => {
      console.log(
        JSON.stringify(
          {
            total: results.length,
            skippedExisting: results.filter((result) => result.skippedExisting)
              .length,
            transformsPassed: results.filter((result) => result.transformSuccess)
              .length,
            validationsPassed: results.filter(
              (result) => result.validationSuccess === true,
            ).length,
            outputDirectory: path.resolve(options.outputDirectory),
          },
          null,
          2,
        ),
      );
      if (results.some((result) => !result.transformSuccess)) {
        process.exitCode = 1;
      }
    })
    .catch((error) => {
      console.error(error instanceof Error ? error.message : String(error));
      process.exitCode = 1;
    });
}
