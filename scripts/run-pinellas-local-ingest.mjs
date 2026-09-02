#!/usr/bin/env node

import { fork, spawn } from "node:child_process";
import { createRequire } from "node:module";
import { existsSync, closeSync, openSync, readSync, statSync } from "node:fs";
import {
  appendFile,
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  rename,
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
const DEFAULT_CONCURRENCY = 4;
const DEFAULT_FETCH_CONCURRENCY = 8;
const DEFAULT_FETCH_TIMEOUT_MS = 12000;
const DEFAULT_TRANSFORM_TIMEOUT_MS = 60000;
const DEFAULT_WORKER_SPAWN_TIMEOUT_MS = 20000;
const MIN_TRANSFORMED_ZIP_BYTES = 200;
const ZIP_LOCAL_FILE_MAGIC = Buffer.from([0x50, 0x4b, 0x03, 0x04]);
const RATE_LIMIT_PAUSE_AFTER = 6;
const RATE_LIMIT_PAUSE_MS = 15000;
const RATE_LIMIT_PAUSE_MAX_MS = 60000;
const PARCEL_ATTEMPTS = 3;
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
 * @property {number} concurrency - Maximum in-flight transforms (CPU-bound).
 * @property {number} fetchConcurrency - Maximum in-flight PCPAO print GETs.
 * @property {number} fetchTimeoutMs - Abort a hung print GET after this many ms.
 * @property {number} transformTimeoutMs - Kill a stuck county-script worker after this many ms.
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
 * @property {string | null} lastCompletedAt - ISO timestamp of the last finished parcel.
 * @property {string | null} lastCompletedParcelId - STRAP of the last finished parcel.
 * @property {number} inFlight - Parcels currently fetching or transforming.
 * @property {boolean} stopping - True after SIGINT/SIGTERM; remaining seed rows are left for restart.
 * @property {number} total - Selected seed rows.
 * @property {number} completed - Finished workers (success, skip, or fail).
 * @property {number} skippedExisting - Parcels reused from disk.
 * @property {number} transformsPassed - Successful transforms including skips.
 * @property {number} transformsFailed - Failed parcels.
 * @property {number} concurrency - Transform worker count.
 * @property {number} fetchConcurrency - In-flight print GETs.
 * @property {string} seedPath - Seed CSV path.
 * @property {string} outputDirectory - Output root.
 *
 * @typedef {object} TransformPool
 * @property {(workDir: string) => Promise<string | null>} transform - Run county scripts in `workDir`.
 * @property {() => Promise<void>} close - Kill persistent workers.
 *
 * @typedef {object} RateLimitGate
 * @property {() => Promise<void>} beforeFetch - Wait out a 403/429 pause.
 * @property {() => void} noteSuccess - Reset consecutive rate-limit failures.
 * @property {(error: unknown) => void} noteFailure - Record a fetch/transform error.
 *
 * @typedef {object} RateLimitGateOptions
 * @property {number} [pauseAfter] - Consecutive 403/429s before pausing.
 * @property {number} [pauseMs] - Base pause once the threshold is hit.
 * @property {number} [maxPauseMs] - Cap on the pause.
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
    fetchConcurrency: DEFAULT_FETCH_CONCURRENCY,
    fetchTimeoutMs: DEFAULT_FETCH_TIMEOUT_MS,
    transformTimeoutMs: DEFAULT_TRANSFORM_TIMEOUT_MS,
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
    } else if (flag === "--fetch-concurrency") {
      options.fetchConcurrency = Number.parseInt(value, 10);
    } else if (flag === "--fetch-timeout-ms") {
      options.fetchTimeoutMs = Number.parseInt(value, 10);
    } else if (flag === "--transform-timeout-ms") {
      options.transformTimeoutMs = Number.parseInt(value, 10);
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
  if (
    !Number.isInteger(options.fetchConcurrency) ||
    options.fetchConcurrency <= 0
  ) {
    throw new Error("--fetch-concurrency must be a positive integer");
  }
  if (
    !Number.isInteger(options.fetchTimeoutMs) ||
    options.fetchTimeoutMs <= 0
  ) {
    throw new Error("--fetch-timeout-ms must be a positive integer");
  }
  if (
    !Number.isInteger(options.transformTimeoutMs) ||
    options.transformTimeoutMs <= 0
  ) {
    throw new Error("--transform-timeout-ms must be a positive integer");
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
  const line = columns
    .map((column) => encodeCsvCell(row[column] ?? ""))
    .join(",");
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
      if (
        parsed !== null &&
        typeof parsed === "object" &&
        !Array.isArray(parsed)
      ) {
        /** @type {Record<string, string[]>} */
        const out = {};
        for (const [key, value] of Object.entries(
          /** @type {Record<string, unknown>} */ (parsed),
        )) {
          if (
            Array.isArray(value) &&
            value.every((item) => typeof item === "string")
          ) {
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
    multiValueQueryString: parseSeedQueryString(
      row.multiValueQueryString,
      strap,
    ),
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
 * True when a parcel directory already has a non-empty PKZIP `transformed.zip`.
 * Tiny or non-zip files are treated as incomplete so a crashed write is retried.
 *
 * @param {string} parcelDir - Per-STRAP output directory.
 * @returns {boolean} Whether a usable `transformed.zip` exists.
 */
export function hasCompletedTransform(parcelDir) {
  const zipPath = path.join(parcelDir, "transformed.zip");
  try {
    const stats = statSync(zipPath);
    if (!stats.isFile() || stats.size < MIN_TRANSFORMED_ZIP_BYTES) return false;
    const fd = openSync(zipPath, "r");
    try {
      const magic = Buffer.alloc(4);
      const bytesRead = readSync(fd, magic, 0, 4, 0);
      return bytesRead === 4 && magic.equals(ZIP_LOCAL_FILE_MAGIC);
    } finally {
      closeSync(fd);
    }
  } catch {
    return false;
  }
}

/**
 * Whether a parcel failure is worth retrying (timeouts, 403/429/5xx, dead worker).
 *
 * @param {unknown} error - Error or message.
 * @returns {boolean} True when another attempt may succeed.
 */
export function isRetryableIngestError(error) {
  const message = error instanceof Error ? error.message : String(error);
  return (
    /HTTP (403|429|500|502|503|504)/.test(message) ||
    /timed out|timeout|aborted|AbortError/i.test(message) ||
    /transform worker exited|transform timed out/i.test(message) ||
    /ECONNRESET|ECONNREFUSED|ETIMEDOUT|EAI_AGAIN|UND_ERR|fetch failed/i.test(
      message,
    )
  );
}

/**
 * Backoff before retrying a print GET or parcel ingest.
 *
 * @param {unknown} error - Previous failure.
 * @param {number} attempt - 1-based attempt that just failed.
 * @returns {number} Milliseconds to wait.
 */
export function retryDelayMs(error, attempt) {
  const message = error instanceof Error ? error.message : String(error);
  const base = /HTTP (403|429)/.test(message) ? 2000 : 250;
  return base * 2 ** (attempt - 1);
}

/**
 * Errors that should stop the whole ingest instead of skipping one parcel.
 *
 * @param {unknown} error - Error or message.
 * @returns {boolean} True when continuing would make things worse.
 */
export function isFatalIngestError(error) {
  const message = error instanceof Error ? error.message : String(error);
  return /ENOSPC|EROFS|ENOMEM|EMFILE/.test(message);
}

/**
 * Shared 403/429 pause so concurrent print GETs do not stampede PCPAO.
 *
 * @param {RateLimitGateOptions} [options] - Thresholds.
 * @param {number} [options.pauseAfter] - Consecutive 403/429s before pausing.
 * @param {number} [options.pauseMs] - Base pause once the threshold is hit.
 * @param {number} [options.maxPauseMs] - Cap on the pause.
 * @returns {RateLimitGate} Gate used around print fetches.
 */
export function createRateLimitGate(options = {}) {
  const pauseAfter = options.pauseAfter ?? RATE_LIMIT_PAUSE_AFTER;
  const pauseMs = options.pauseMs ?? RATE_LIMIT_PAUSE_MS;
  const maxPauseMs = options.maxPauseMs ?? RATE_LIMIT_PAUSE_MAX_MS;
  let consecutive = 0;
  let pausedUntilMs = 0;
  return {
    async beforeFetch() {
      const waitMs = pausedUntilMs - Date.now();
      if (waitMs > 0) await sleep(waitMs);
    },
    noteSuccess() {
      consecutive = 0;
    },
    noteFailure(error) {
      const message = error instanceof Error ? error.message : String(error);
      if (!/HTTP (403|429)/.test(message)) return;
      consecutive += 1;
      if (consecutive < pauseAfter) return;
      const extra = Math.min(consecutive - pauseAfter, 2);
      const waitMs = Math.min(maxPauseMs, pauseMs * 2 ** extra);
      pausedUntilMs = Math.max(pausedUntilMs, Date.now() + waitMs);
      console.error(
        JSON.stringify({
          event: "pinellas_rate_limit_pause",
          consecutive,
          pauseMs: waitMs,
        }),
      );
    },
  };
}

/**
 * Run an async mapper with a fixed worker pool.
 *
 * @template T
 * @template R
 * @param {readonly T[]} items - Work items.
 * @param {number} concurrency - Worker count.
 * @param {(item: T, index: number) => Promise<R>} worker - Per-item mapper.
 * @param {() => boolean} [shouldStop] - When true, remaining items are left unprocessed.
 * @returns {Promise<R[]>} Results in input order.
 */
export async function mapWithConcurrency(
  items,
  concurrency,
  worker,
  shouldStop = () => false,
) {
  /** @type {R[]} */
  const results = new Array(items.length);
  let nextIndex = 0;
  const workerCount = Math.max(1, Math.min(concurrency, items.length || 1));
  if (items.length === 0) return results;
  await Promise.all(
    Array.from({ length: workerCount }, async () => {
      while (true) {
        if (shouldStop()) return;
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
 * Bound how many async jobs run at once.
 *
 * @param {number} max - Maximum concurrent jobs.
 * @returns {{ run: <T>(job: () => Promise<T>) => Promise<T> }} Limiter.
 */
export function createLimiter(max) {
  let active = 0;
  /** @type {Array<() => void>} */
  const waiters = [];
  return {
    /**
     * @template T
     * @param {() => Promise<T>} job - Work to run.
     * @returns {Promise<T>} Job result.
     */
    async run(job) {
      if (active >= max) {
        await new Promise((resolve) => {
          waiters.push(resolve);
        });
      }
      active += 1;
      try {
        return await job();
      } finally {
        active -= 1;
        const next = waiters.shift();
        if (next !== undefined) next();
      }
    },
  };
}

/**
 * Launch persistent Pinellas script workers (one Node process, many parcels).
 * Dead workers are replaced. A job that does not finish in `timeoutMs` is
 * killed so the pool cannot stall the way unbounded `fetch` did.
 *
 * @param {object} params - Pool parameters.
 * @param {string} params.workerPath - Absolute path to `pinellas-transform-worker.cjs`.
 * @param {string} params.scriptsDirectory - County scripts folder.
 * @param {number} params.size - Worker count.
 * @param {string} params.nodeModulesPath - `NODE_PATH` so scripts can `require("cheerio")`.
 * @param {number} [params.timeoutMs] - Per-job timeout.
 * @returns {Promise<TransformPool>} IPC pool.
 */
export async function createTransformPool({
  workerPath,
  scriptsDirectory,
  size,
  nodeModulesPath,
  timeoutMs = DEFAULT_TRANSFORM_TIMEOUT_MS,
}) {
  /** @type {import("node:child_process").ChildProcess[]} */
  const workers = [];
  /** @type {import("node:child_process").ChildProcess[]} */
  const idle = [];
  /** @type {Array<(worker: import("node:child_process").ChildProcess | null) => void>} */
  const waiters = [];
  let nextJobId = 1;
  let closed = false;

  /**
   * @param {import("node:child_process").ChildProcess} worker - Worker to drop.
   * @returns {void}
   */
  function forgetWorker(worker) {
    const workerIndex = workers.indexOf(worker);
    if (workerIndex >= 0) workers.splice(workerIndex, 1);
    const idleIndex = idle.indexOf(worker);
    if (idleIndex >= 0) idle.splice(idleIndex, 1);
  }

  /**
   * @returns {Promise<import("node:child_process").ChildProcess>} Ready worker.
   */
  function spawnWorker() {
    if (closed) {
      return Promise.reject(new Error("transform pool is closed"));
    }
    const worker = fork(workerPath, [], {
      stdio: ["ignore", "ignore", "ignore", "ipc"],
      env: { ...process.env, NODE_PATH: nodeModulesPath },
    });
    workers.push(worker);
    let accepted = false;
    worker.on("exit", () => {
      forgetWorker(worker);
      if (closed || !accepted) return;
      spawnWorker()
        .then((replacement) => {
          release(replacement);
        })
        .catch((error) => {
          console.error(
            JSON.stringify({
              event: "pinellas_transform_worker_respawn_failed",
              error: error instanceof Error ? error.message : String(error),
            }),
          );
        });
    });
    return new Promise((resolve, reject) => {
      let settled = false;
      const spawnTimer = setTimeout(() => {
        finish(new Error("transform worker spawn timed out"));
        worker.kill("SIGKILL");
      }, DEFAULT_WORKER_SPAWN_TIMEOUT_MS);
      /**
       * @param {Error | null} error - Failure, or null on ready.
       * @returns {void}
       */
      const finish = (error) => {
        if (settled) return;
        settled = true;
        clearTimeout(spawnTimer);
        worker.off("message", onReady);
        worker.off("error", onError);
        if (error !== null) {
          reject(error);
          return;
        }
        accepted = true;
        resolve(worker);
      };
      /**
       * @param {unknown} message - IPC payload.
       * @returns {void}
       */
      const onReady = (message) => {
        if (
          message !== null &&
          typeof message === "object" &&
          "type" in message &&
          message.type === "ready"
        ) {
          finish(null);
        }
      };
      /**
       * @param {Error} error - Spawn failure.
       * @returns {void}
       */
      const onError = (error) => {
        finish(error);
      };
      worker.on("message", onReady);
      worker.once("error", onError);
      worker.once("exit", () => {
        finish(new Error("transform worker exited before ready"));
      });
    });
  }

  /**
   * @returns {Promise<import("node:child_process").ChildProcess>} Idle worker.
   */
  function takeIdle() {
    if (closed) return Promise.reject(new Error("transform pool is closed"));
    const existing = idle.pop();
    if (existing !== undefined) return Promise.resolve(existing);
    return new Promise((resolve, reject) => {
      waiters.push((worker) => {
        if (worker === null) reject(new Error("transform pool is closed"));
        else resolve(worker);
      });
    });
  }

  /**
   * @param {import("node:child_process").ChildProcess} worker - Worker to return.
   * @returns {void}
   */
  function release(worker) {
    if (closed || worker.exitCode !== null || worker.signalCode !== null) {
      return;
    }
    const waiter = waiters.shift();
    if (waiter !== undefined) waiter(worker);
    else idle.push(worker);
  }

  for (let index = 0; index < size; index += 1) {
    idle.push(await spawnWorker());
  }

  return {
    async transform(workDir) {
      const worker = await takeIdle();
      const id = nextJobId;
      nextJobId += 1;
      try {
        return await new Promise((resolve, reject) => {
          let settled = false;
          const timer = setTimeout(() => {
            finish(new Error(`transform timed out after ${timeoutMs}ms`));
            worker.kill("SIGKILL");
          }, timeoutMs);
          /**
           * @param {Error | null} error - Failure, or null on success.
           * @param {string | null} [usageType] - Usage type.
           * @returns {void}
           */
          const finish = (error, usageType) => {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            worker.off("message", onMessage);
            worker.off("exit", onExit);
            if (error !== null) reject(error);
            else resolve(usageType ?? null);
          };
          /**
           * @param {unknown} message - IPC payload.
           * @returns {void}
           */
          const onMessage = (message) => {
            if (message === null || typeof message !== "object") return;
            const record =
              /** @type {{ type?: unknown, id?: unknown, propertyUsageType?: unknown, error?: unknown }} */ (
                message
              );
            if (record.id !== id) return;
            if (record.type === "ok") {
              finish(
                null,
                typeof record.propertyUsageType === "string"
                  ? record.propertyUsageType
                  : null,
              );
              return;
            }
            finish(
              new Error(
                typeof record.error === "string"
                  ? record.error
                  : "transform worker failed",
              ),
            );
          };
          /**
           * @param {number | null} code - Exit code.
           * @param {NodeJS.Signals | null} signal - Kill signal.
           * @returns {void}
           */
          const onExit = (code, signal) => {
            finish(
              new Error(
                `transform worker exited (${code}/${signal ?? "none"})`,
              ),
            );
          };
          worker.on("message", onMessage);
          worker.once("exit", onExit);
          worker.once("error", (error) => {
            finish(error);
          });
          try {
            worker.send({
              type: "run",
              id,
              scriptsDirectory,
              workDir,
            });
          } catch (error) {
            finish(error instanceof Error ? error : new Error(String(error)));
          }
        });
      } finally {
        if (worker.exitCode === null && worker.signalCode === null) {
          release(worker);
        }
      }
    },
    async close() {
      closed = true;
      while (waiters.length > 0) {
        const waiter = waiters.shift();
        if (waiter !== undefined) waiter(null);
      }
      await Promise.all(
        workers.map(
          (worker) =>
            new Promise((resolve) => {
              if (worker.exitCode !== null || worker.signalCode !== null) {
                resolve(undefined);
                return;
              }
              worker.once("exit", () => resolve(undefined));
              worker.kill("SIGKILL");
            }),
        ),
      );
    },
  };
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
 * Fetch PCPAO print HTML with a Chrome UA. Retries UA-sensitive 403/429/5xx
 * and timeouts; does not retry empty or non-HTML pages.
 *
 * @param {string} strap - 18-digit STRAP.
 * @param {typeof fetch} [fetchImpl] - Injected fetch.
 * @param {number} [attempts] - Total tries.
 * @param {number} [timeoutMs] - Abort a hung print GET after this many ms.
 * @param {RateLimitGate | null} [rateLimitGate] - Shared 403/429 pause.
 * @returns {Promise<string>} Print HTML.
 */
export async function fetchPropertyPrintHtml(
  strap,
  fetchImpl = fetch,
  attempts = 4,
  timeoutMs = DEFAULT_FETCH_TIMEOUT_MS,
  rateLimitGate = null,
) {
  const url = buildPrintPageUrl(strap);
  /** @type {Error} */
  let lastError = new Error(`PCPAO print fetch failed for ${strap}`);
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      if (rateLimitGate !== null) await rateLimitGate.beforeFetch();
      const response = await fetchImpl(url, {
        headers: {
          "User-Agent": PRINT_USER_AGENT,
          Accept: "text/html",
        },
        signal: AbortSignal.timeout(timeoutMs),
      });
      if (!response.ok) {
        throw new Error(`PCPAO print HTTP ${response.status} for ${strap}`);
      }
      const html = await response.text();
      if (!html.toLowerCase().includes("<html")) {
        throw new Error(`PropertyPrint response is not HTML for ${strap}`);
      }
      if (!/Parcel Summary/i.test(html)) {
        throw new Error(
          `PCPAO print HTML is missing Parcel Summary for ${strap}`,
        );
      }
      rateLimitGate?.noteSuccess();
      return html;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      rateLimitGate?.noteFailure(lastError);
      if (attempt === attempts || !isRetryableIngestError(lastError)) break;
      await sleep(retryDelayMs(lastError, attempt));
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
      reject(
        new Error(`${command} ${args.join(" ")} exited ${code}.${detail}`),
      );
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
async function injectSourceHttpRequest(
  dataDir,
  sourceHttpRequest,
  requestIdentifier,
) {
  const names = await readdir(dataDir);
  await Promise.all(
    names
      .filter((name) => name.endsWith(".json"))
      .map(async (name) => {
        const filePath = path.join(dataDir, name);
        const parsed = JSON.parse(await readFile(filePath, "utf8"));
        if (
          parsed === null ||
          typeof parsed !== "object" ||
          Array.isArray(parsed)
        ) {
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
        await writeFile(filePath, `${JSON.stringify(sanitized)}\n`, "utf8");
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
  const tmpPath = `${zipPath}.tmp`;
  await rm(tmpPath, { force: true });
  const zip = new AdmZip();
  const names = await readdir(dataDir);
  if (!names.includes("property.json")) {
    throw new Error("transform produced no data/property.json");
  }
  for (const name of names) {
    zip.addLocalFile(path.join(dataDir, name), "data");
  }
  zip.writeZip(tmpPath);
  await rename(tmpPath, zipPath);
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
 * @param {TransformPool} params.transformPool - Persistent county-script workers.
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
  transformPool,
}) {
  await writeFile(path.join(workDir, "input.html"), html, "utf8");
  await writeFile(
    path.join(workDir, "property_seed.json"),
    `${JSON.stringify(propertySeed)}\n`,
    "utf8",
  );
  await writeFile(
    path.join(workDir, "unnormalized_address.json"),
    `${JSON.stringify(unnormalizedAddress)}\n`,
    "utf8",
  );
  await mkdir(path.join(workDir, "data"), { recursive: true });
  await mkdir(path.join(workDir, "owners"), { recursive: true });
  const propertyUsageType = await transformPool.transform(workDir);
  const dataDir = path.join(workDir, "data");
  await injectSourceHttpRequest(
    dataDir,
    propertySeed.source_http_request,
    propertySeed.request_identifier,
  );
  await zipDataDirectory(dataDir, transformedZip);
  return propertyUsageType;
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
 * @param {TransformPool | null} params.transformPool - Persistent workers, or null for CLI transform.
 * @param {string | null} [params.html] - Pre-fetched print HTML.
 * @param {RateLimitGate | null} [params.rateLimitGate] - Shared 403/429 pause for fallback fetches.
 * @returns {Promise<ParcelIngestResult>} Per-parcel outcome.
 */
async function ingestParcel({
  row,
  options,
  scriptsZipPath,
  scriptsDirectory,
  repoRoot,
  transformPool,
  html = null,
  rateLimitGate = null,
}) {
  const parcelId = row.parcel_id;
  const useGroup = row.use_group ?? "";
  const parcelDir = path.join(options.outputDirectory, parcelId);
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
  await mkdir(parcelDir, { recursive: true });
  const workDir = await mkdtemp(
    path.join(os.tmpdir(), `pinellas-${parcelId}-`),
  );
  try {
    const seedFiles = buildSeedJsonFiles(row);
    const transformedZip = path.join(parcelDir, "transformed.zip");
    /** @type {string} */
    let printHtml = html ?? "";
    if (printHtml.length === 0) {
      if (options.useCliPrepare) {
        printHtml = await prepareWithElephantCli({
          row,
          options,
          seedFiles,
          parcelDir,
          workDir,
          repoRoot,
        });
      } else {
        printHtml = await fetchPropertyPrintHtml(
          parcelId,
          fetch,
          4,
          options.fetchTimeoutMs,
          rateLimitGate,
        );
      }
    }
    /** @type {string | null} */
    let propertyUsageType;
    if (options.transformMode === "elephant-cli") {
      propertyUsageType = await transformWithElephantCli({
        html: printHtml,
        seedFiles,
        parcelId,
        workDir,
        parcelDir,
        scriptsZipPath,
        transformedZip,
        repoRoot,
      });
      await stripQueryFromTransformedZip(transformedZip);
    } else {
      if (transformPool === null) {
        throw new Error("transform pool is required for scripts mode");
      }
      propertyUsageType = await transformWithCountyScripts({
        workDir,
        scriptsDirectory,
        html: printHtml,
        propertySeed: seedFiles.propertySeed,
        unnormalizedAddress: seedFiles.unnormalizedAddress,
        repoRoot,
        transformedZip,
        transformPool,
      });
    }
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
    await rm(path.join(parcelDir, "error.txt"), { force: true });
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
    if (isFatalIngestError(error)) throw error;
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
 * @returns {Promise<{ total: number, skippedExisting: number, transformsPassed: number, transformsFailed: number, failures: ParcelIngestResult[] }>}
 *   Compact run totals. Successful parcels are not retained in memory.
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

  /** @type {SeedRow[]} */
  const pending = [];
  let skippedExisting = 0;
  if (options.skipExisting) {
    for (const row of rows) {
      if (hasCompletedTransform(path.join(outputDirectory, row.parcel_id))) {
        skippedExisting += 1;
      } else {
        pending.push(row);
      }
    }
  } else {
    pending.push(...rows);
  }

  const startedAt = new Date().toISOString();
  let completed = skippedExisting;
  let transformsPassed = skippedExisting;
  let transformsFailed = 0;
  /** @type {ParcelIngestResult[]} */
  const failures = [];
  const failuresPath = path.join(outputDirectory, "failures.jsonl");
  const resolvedOptions = { ...options, outputDirectory };
  const workerPath = path.join(
    repoRoot,
    "scripts",
    "pinellas-transform-worker.cjs",
  );
  const transformPool =
    options.transformMode === "scripts"
      ? await createTransformPool({
          workerPath,
          scriptsDirectory,
          size: options.concurrency,
          nodeModulesPath: path.join(repoRoot, "node_modules"),
          timeoutMs: options.transformTimeoutMs,
        })
      : null;
  const transformLimit = createLimiter(options.concurrency);
  const rateLimitGate = createRateLimitGate();
  let stopping = false;
  let inFlight = 0;
  /** @type {string | null} */
  let lastCompletedAt = null;
  /** @type {string | null} */
  let lastCompletedParcelId = null;

  /**
   * @param {NodeJS.Signals} signal - Stop signal.
   * @returns {void}
   */
  const onStopSignal = (signal) => {
    if (stopping) {
      console.error(
        JSON.stringify({ event: "pinellas_ingest_forced_exit", signal }),
      );
      process.exit(130);
    }
    stopping = true;
    console.error(
      JSON.stringify({ event: "pinellas_ingest_stop_requested", signal }),
    );
  };
  process.on("SIGINT", onStopSignal);
  process.on("SIGTERM", onStopSignal);

  /**
   * @returns {IngestStatusSnapshot} Current counts.
   */
  function snapshot() {
    return {
      startedAt,
      updatedAt: new Date().toISOString(),
      lastCompletedAt,
      lastCompletedParcelId,
      inFlight,
      stopping,
      total: rows.length,
      completed,
      skippedExisting,
      transformsPassed,
      transformsFailed,
      concurrency: options.concurrency,
      fetchConcurrency: options.fetchConcurrency,
      seedPath: options.seedPath,
      outputDirectory,
    };
  }

  /**
   * @param {SeedRow} row - Seed row.
   * @param {string} error - Failure message.
   * @returns {ParcelIngestResult} Failed parcel result.
   */
  function failedResult(row, error) {
    return {
      parcelId: row.parcel_id,
      useGroup: row.use_group ?? "",
      prepareSuccess: false,
      transformSuccess: false,
      validationSuccess: null,
      propertyUsageType: null,
      error,
      skippedExisting: false,
    };
  }

  /**
   * @param {IngestStatusSnapshot} status - Counts.
   * @returns {Promise<void>}
   */
  async function recordProgress(status) {
    await writeStatusSnapshot(outputDirectory, status);
    console.log(
      JSON.stringify({ event: "pinellas_ingest_progress", ...status }),
    );
  }

  await recordProgress(snapshot());

  const heartbeat = setInterval(() => {
    recordProgress(snapshot()).catch(() => {});
  }, 30000);
  heartbeat.unref();

  try {
    await mapWithConcurrency(
      pending,
      options.fetchConcurrency,
      async (row) => {
        if (stopping) {
          return {
            parcelId: row.parcel_id,
            useGroup: row.use_group ?? "",
            prepareSuccess: true,
            transformSuccess: true,
            validationSuccess: null,
            propertyUsageType: null,
            error: null,
            skippedExisting: true,
          };
        }
        inFlight += 1;
        try {
          /** @type {ParcelIngestResult | null} */
          let result = null;
          /** @type {string | null | undefined} */
          let html = options.useCliPrepare ? null : undefined;
          for (let attempt = 1; attempt <= PARCEL_ATTEMPTS; attempt += 1) {
            if (stopping) break;
            try {
              if (!options.useCliPrepare && typeof html !== "string") {
                html = await fetchPropertyPrintHtml(
                  row.parcel_id,
                  fetch,
                  4,
                  options.fetchTimeoutMs,
                  rateLimitGate,
                );
              }
              result = await transformLimit.run(() =>
                ingestParcel({
                  row,
                  options: resolvedOptions,
                  scriptsZipPath,
                  scriptsDirectory,
                  repoRoot,
                  transformPool,
                  html: typeof html === "string" ? html : null,
                  rateLimitGate,
                }),
              );
              if (result.transformSuccess) break;
              if (isFatalIngestError(result.error)) {
                throw new Error(result.error ?? "fatal ingest error");
              }
              if (
                attempt === PARCEL_ATTEMPTS ||
                !isRetryableIngestError(result.error ?? "failed")
              ) {
                break;
              }
              if (
                /HTTP |fetch failed|timeout|aborted|ECONN|ETIMEDOUT|EAI_AGAIN/i.test(
                  result.error ?? "",
                )
              ) {
                html = undefined;
              }
              await sleep(retryDelayMs(result.error ?? "failed", attempt));
            } catch (error) {
              if (isFatalIngestError(error)) throw error;
              const message =
                error instanceof Error ? error.message : String(error);
              result = failedResult(row, message);
              html = undefined;
              if (
                attempt === PARCEL_ATTEMPTS ||
                !isRetryableIngestError(error)
              ) {
                break;
              }
              await sleep(retryDelayMs(error, attempt));
            }
          }
          if (result === null) {
            result = failedResult(
              row,
              stopping ? "ingest stopped" : "ingest produced no result",
            );
          }
          if (stopping && !result.transformSuccess) {
            return result;
          }
          completed += 1;
          lastCompletedAt = new Date().toISOString();
          lastCompletedParcelId = row.parcel_id;
          if (result.transformSuccess) transformsPassed += 1;
          else {
            transformsFailed += 1;
            failures.push(result);
            await appendFile(
              failuresPath,
              `${JSON.stringify(result)}\n`,
              "utf8",
            );
          }
          if (
            completed === skippedExisting + 1 ||
            completed % 25 === 0 ||
            completed === rows.length
          ) {
            await recordProgress(snapshot());
          }
          return result;
        } finally {
          inFlight -= 1;
        }
      },
      () => stopping,
    );
  } finally {
    clearInterval(heartbeat);
    process.off("SIGINT", onStopSignal);
    process.off("SIGTERM", onStopSignal);
    await transformPool?.close();
  }

  await recordProgress(snapshot());

  await writeFile(
    path.join(outputDirectory, "summary.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        total: rows.length,
        skippedExisting,
        transformsPassed,
        transformsFailed,
        failures,
      },
      null,
      2,
    ),
    "utf8",
  );
  return {
    total: rows.length,
    skippedExisting,
    transformsPassed,
    transformsFailed,
    failures,
  };
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const options = parseCliOptions(process.argv.slice(2));
  runLocalIngest(options)
    .then((summary) => {
      console.log(
        JSON.stringify(
          {
            total: summary.total,
            skippedExisting: summary.skippedExisting,
            transformsPassed: summary.transformsPassed,
            transformsFailed: summary.transformsFailed,
            outputDirectory: path.resolve(options.outputDirectory),
          },
          null,
          2,
        ),
      );
      if (summary.transformsFailed > 0) {
        process.exitCode = 1;
      }
    })
    .catch((error) => {
      console.error(error instanceof Error ? error.message : String(error));
      process.exitCode = 1;
    });
}
