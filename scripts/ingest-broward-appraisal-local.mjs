#!/usr/bin/env node

/**
 * Checkpointed local Broward appraisal ingestion.
 *
 * Streams a county seed, captures each BCPA response, transforms it with the
 * county scripts, and stores private local artifacts. No AWS service is used.
 */

import { createReadStream } from "fs";
import { fork } from "child_process";
import {
  access,
  appendFile,
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  rename,
  rm,
  writeFile,
} from "fs/promises";
import os from "os";
import path from "path";
import { promisify } from "util";
import { fileURLToPath, pathToFileURL } from "url";
import { gunzip, gzip } from "zlib";
import AdmZip from "adm-zip";
import { parse } from "csv-parse";

import {
  fetchBrowardParcelEnvelope,
  requireParcelRecords,
  unwrapBrowardPrepareCapture,
} from "./capture-broward-parcel.mjs";
import {
  BROWARD_COUNTY_NAME,
  BROWARD_DETAIL_URL,
  browardDetailRequestBody,
  normalizeBrowardFolio,
} from "./broward-folio.mjs";
import { SEED_COLUMNS, renderCsvRow } from "./build-broward-seed.mjs";

const gzipAsync = promisify(gzip);
const gunzipAsync = promisify(gunzip);
const DEFAULT_SEED_PATH = "downloads/broward/broward.csv";
const DEFAULT_SCRIPTS_DIRECTORY =
  "../Counties-trasform-scripts/broward/scripts";
const DEFAULT_OUTPUT_DIRECTORY = "downloads/broward/full-ingestion";
const DEFAULT_CONCURRENCY = 2;
const MAX_CONCURRENCY = 4;
const SOURCE_MAX_ATTEMPTS = 4;
const TRANSFORM_WORKER_PATH = fileURLToPath(
  new URL("./broward-transform-worker-child.mjs", import.meta.url),
);

/**
 * @typedef {Record<string, string | undefined>} CsvRecord
 *
 * @typedef {object} LocalIngestOptions
 * @property {string} seedPath - Complete Broward seed CSV.
 * @property {string} scriptsDirectory - Patched Broward transform directory.
 * @property {string} outputDirectory - Private local output root.
 * @property {number} concurrency - Concurrent parcel pipelines.
 * @property {number | null} limit - Optional count for a bounded run.
 * @property {boolean} resetCheckpoint - Ignore prior progress state.
 *
 * @typedef {object} LocalIngestState
 * @property {string} startedAt - ISO timestamp of the first run.
 * @property {string} updatedAt - ISO timestamp of the latest checkpoint.
 * @property {number} nextRowIndex - Zero-based row offset to process next.
 * @property {number} attempted - Rows attempted, including source misses.
 * @property {number} succeeded - New transformed artifacts written.
 * @property {number} skippedExisting - Existing artifacts reused.
 * @property {number} failed - Source or transform failures.
 * @property {Record<string, number>} usageTypes - Successful transformed usage counts.
 *
 * @typedef {object} LocalIngestResult
 * @property {number} rowIndex - Zero-based seed row index.
 * @property {string} folio - Canonical Broward folio, or raw identifier on validation failure.
 * @property {"succeeded" | "skipped_existing" | "source_error" | "transform_error"} status
 *   Parcel outcome.
 * @property {number} durationMs - End-to-end parcel duration.
 * @property {string | null} propertyUsageType - Transformed Lexicon usage.
 * @property {string | null} error - Sanitized failure message.
 *
 * @typedef {object} BrowardSourceRequest
 * @property {"POST"} method - HTTP method.
 * @property {string} url - BCPA endpoint.
 * @property {Record<string, string>} headers - JSON request headers.
 * @property {{ folioNumber: string, taxyear: string, action: string, use: string }} json
 *   BCPA request body.
 *
 * @typedef {object} BrowardSeedEntities
 * @property {Record<string, unknown>} propertySeed - Legacy property seed entity.
 * @property {Record<string, unknown>} unnormalizedAddress - Legacy address entity.
 *
 * @typedef {object} TransformWorkerResponse
 * @property {number} requestId - Parent correlation identifier.
 * @property {boolean} success - Whether Elephant CLI transform succeeded.
 * @property {string | null} error - Transform failure text.
 */

/**
 * Parse local full-ingestion CLI options.
 *
 * @param {readonly string[]} argv - Arguments after the script path.
 * @returns {LocalIngestOptions} Validated options.
 */
export function parseCliOptions(argv) {
  /** @type {LocalIngestOptions} */
  const options = {
    seedPath: DEFAULT_SEED_PATH,
    scriptsDirectory: DEFAULT_SCRIPTS_DIRECTORY,
    outputDirectory: DEFAULT_OUTPUT_DIRECTORY,
    concurrency: DEFAULT_CONCURRENCY,
    limit: null,
    resetCheckpoint: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index];
    if (flag === "--reset-checkpoint") {
      options.resetCheckpoint = true;
      continue;
    }
    const value = argv[index + 1];
    if (typeof value !== "string" || value.startsWith("--")) {
      throw new Error(`Missing value for ${flag}`);
    }
    if (flag === "--seed") options.seedPath = value;
    else if (flag === "--scripts") options.scriptsDirectory = value;
    else if (flag === "--output") options.outputDirectory = value;
    else if (flag === "--concurrency") {
      options.concurrency = parsePositiveInteger(flag, value);
    } else if (flag === "--limit") {
      options.limit = parsePositiveInteger(flag, value);
    } else {
      throw new Error(`Unknown option: ${flag}`);
    }
    index += 1;
  }
  if (options.concurrency > MAX_CONCURRENCY) {
    throw new Error(`--concurrency cannot exceed ${String(MAX_CONCURRENCY)}`);
  }
  return options;
}

/**
 * Parse one positive integer CLI value.
 *
 * @param {string} option - Option name.
 * @param {string} raw - Raw value.
 * @returns {number} Parsed positive integer.
 */
function parsePositiveInteger(option, raw) {
  const value = Number.parseInt(raw, 10);
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${option} must be a positive integer`);
  }
  return value;
}

/**
 * Build the resolved request stored by Elephant CLI multi-request prepare.
 *
 * @param {string} folio - Canonical Broward folio.
 * @returns {BrowardSourceRequest} Reproducible BCPA POST request.
 */
export function buildBrowardSourceRequest(folio) {
  return {
    method: "POST",
    url: BROWARD_DETAIL_URL,
    headers: {
      "content-type": "application/json",
      accept: "application/json, text/javascript, */*; q=0.01",
      "x-requested-with": "XMLHttpRequest",
      origin: "https://web.bcpa.net",
      referer: "https://web.bcpa.net/BcpaClient/search.aspx",
    },
    json: browardDetailRequestBody(folio),
  };
}

/**
 * Build compatibility seed entities without running a redundant seed transform.
 *
 * @param {CsvRecord} row - One complete seed row.
 * @param {string} folio - Canonical folio.
 * @returns {BrowardSeedEntities} Legacy entities consumed by county scripts.
 */
export function buildBrowardSeedEntities(row, folio) {
  const request = {
    method: "POST",
    url: BROWARD_DETAIL_URL,
    headers: { "content-type": "application/json" },
    json: browardDetailRequestBody(folio),
    multiValueQueryString: {},
  };
  const longitude = parseCoordinate(row.longitude);
  const latitude = parseCoordinate(row.latitude);
  /** @type {Record<string, unknown>} */
  const unnormalizedAddress = {
    source_http_request: request,
    request_identifier: folio,
    full_address: row.address ?? "",
    county_jurisdiction: BROWARD_COUNTY_NAME,
  };
  if (longitude !== null && latitude !== null) {
    unnormalizedAddress.longitude = longitude;
    unnormalizedAddress.latitude = latitude;
  }
  return {
    propertySeed: {
      source_http_request: request,
      request_identifier: folio,
      parcel_id: folio,
    },
    unnormalizedAddress,
  };
}

/**
 * Parse one optional finite coordinate.
 *
 * @param {string | undefined} raw - CSV coordinate.
 * @returns {number | null} Numeric coordinate.
 */
function parseCoordinate(raw) {
  if (raw === undefined || raw.trim() === "") return null;
  const value = Number(raw);
  return Number.isFinite(value) ? value : null;
}

/**
 * Return true when a path is readable.
 *
 * @param {string} targetPath - File path.
 * @returns {Promise<boolean>} Whether the path exists.
 */
async function pathExists(targetPath) {
  try {
    await access(targetPath);
    return true;
  } catch {
    return false;
  }
}

/**
 * Read prior progress or create an empty state.
 *
 * @param {string} statePath - Checkpoint file.
 * @param {boolean} reset - Ignore a prior file.
 * @returns {Promise<LocalIngestState>} Ingestion state.
 */
async function readState(statePath, reset) {
  if (!reset && (await pathExists(statePath))) {
    return /** @type {LocalIngestState} */ (
      JSON.parse(await readFile(statePath, "utf8"))
    );
  }
  const now = new Date().toISOString();
  return {
    startedAt: now,
    updatedAt: now,
    nextRowIndex: 0,
    attempted: 0,
    succeeded: 0,
    skippedExisting: 0,
    failed: 0,
    usageTypes: {},
  };
}

/**
 * Atomically persist progress after one contiguous concurrency window.
 *
 * @param {string} statePath - Checkpoint destination.
 * @param {LocalIngestState} state - Complete state.
 * @returns {Promise<void>}
 */
async function writeState(statePath, state) {
  const temporaryPath = `${statePath}.tmp`;
  await writeFile(temporaryPath, `${JSON.stringify(state, null, 2)}\n`, {
    mode: 0o600,
  });
  await rename(temporaryPath, statePath);
}

/**
 * Fetch one source envelope with bounded retries and fail-closed validation.
 *
 * @param {string} folio - Canonical folio.
 * @returns {Promise<import("./capture-broward-parcel.mjs").BrowardParcelEnvelope>}
 *   Non-empty BCPA envelope.
 */
async function fetchEnvelopeWithRetry(folio) {
  let lastError;
  for (let attempt = 1; attempt <= SOURCE_MAX_ATTEMPTS; attempt += 1) {
    try {
      const envelope = await fetchBrowardParcelEnvelope(folio);
      requireParcelRecords(envelope, folio);
      return envelope;
    } catch (error) {
      lastError = error;
      if (attempt < SOURCE_MAX_ATTEMPTS) {
        await new Promise((resolve) =>
          setTimeout(resolve, 500 * 2 ** (attempt - 1)),
        );
      }
    }
  }
  throw lastError instanceof Error
    ? lastError
    : new Error(`Broward source failed for ${folio}`);
}

/**
 * Read a prior compressed capture or fetch and store a fresh one.
 *
 * @param {string} folio - Canonical folio.
 * @param {string} capturePath - Gzip JSON path.
 * @returns {Promise<Record<string, unknown>>} Multi-request capture wrapper.
 */
async function ensureCapture(folio, capturePath) {
  if (await pathExists(capturePath)) {
    const bytes = await gunzipAsync(await readFile(capturePath));
    const payload = /** @type {Record<string, unknown>} */ (
      JSON.parse(bytes.toString("utf8"))
    );
    requireParcelRecords(unwrapBrowardPrepareCapture(payload), folio);
    return payload;
  }
  const sourceRequest = buildBrowardSourceRequest(folio);
  const envelope = await fetchEnvelopeWithRetry(folio);
  /** @type {Record<string, unknown>} */
  const payload = {
    input: {
      source_http_request: sourceRequest,
      response: envelope,
    },
  };
  await mkdir(path.dirname(capturePath), { recursive: true, mode: 0o700 });
  const bytes = await gzipAsync(Buffer.from(`${JSON.stringify(payload)}\n`), {
    level: 9,
  });
  await writeFile(capturePath, bytes, { mode: 0o600 });
  return payload;
}

/**
 * Package patched county scripts once for the transform runtime.
 *
 * @param {string} scriptsDirectory - Broward scripts directory.
 * @param {string} scriptsZipPath - ZIP destination.
 * @returns {void}
 */
function packageScripts(scriptsDirectory, scriptsZipPath) {
  const zip = new AdmZip();
  zip.addLocalFolder(scriptsDirectory);
  zip.writeZip(scriptsZipPath);
}

/**
 * Read transformed property usage from an output ZIP.
 *
 * @param {string} artifactPath - Transformed ZIP path.
 * @returns {string | null} Lexicon property usage type.
 */
function readPropertyUsageType(artifactPath) {
  const entry = new AdmZip(artifactPath).getEntry("data/property.json");
  if (entry === null) return null;
  const value = /** @type {{ property_usage_type?: unknown }} */ (
    JSON.parse(entry.getData().toString("utf8"))
  ).property_usage_type;
  return typeof value === "string" && value.length > 0 ? value : null;
}

/**
 * Create one exact county-transform input ZIP.
 *
 * @param {object} params - Parcel input data.
 * @param {CsvRecord} params.row - Seed row.
 * @param {string} params.folio - Canonical folio.
 * @param {Record<string, unknown>} params.capture - Multi-request wrapper.
 * @param {string} params.destination - ZIP destination.
 * @returns {void}
 */
function createTransformInput({ row, folio, capture, destination }) {
  const entities = buildBrowardSeedEntities(row, folio);
  const zip = new AdmZip();
  zip.addFile(
    "property_seed.json",
    Buffer.from(`${JSON.stringify(entities.propertySeed)}\n`),
  );
  zip.addFile(
    "unnormalized_address.json",
    Buffer.from(`${JSON.stringify(entities.unnormalizedAddress)}\n`),
  );
  zip.addFile(
    "input.csv",
    Buffer.from(
      `${SEED_COLUMNS.join(",")}\n${renderCsvRow(
        /** @type {Record<string, string>} */ (row),
      )}`,
    ),
  );
  zip.addFile(`${folio}.json`, Buffer.from(`${JSON.stringify(capture)}\n`));
  zip.writeZip(destination);
}

/**
 * @typedef {object} TransformWorker
 * @property {(params: {
 *   workingDirectory: string,
 *   inputZipPath: string,
 *   outputZipPath: string,
 *   scriptsZipPath: string
 * }) => Promise<{ success: boolean, error: string | null }>} run
 *   Run one transform in this worker.
 * @property {() => Promise<void>} close - Stop the child process.
 */

/**
 * Start one long-lived Elephant CLI transform worker.
 *
 * Every worker has a distinct TMPDIR, preventing the fact-sheet generator's
 * fixed `generated-htmls` subdirectory from racing across concurrent parcels.
 *
 * @param {number} workerIndex - Stable worker ordinal.
 * @param {string} workerRoot - Private worker temp root.
 * @returns {Promise<TransformWorker>} Ready worker.
 */
async function createTransformWorker(workerIndex, workerRoot) {
  const workerTmp = path.join(workerRoot, String(workerIndex));
  await mkdir(workerTmp, { recursive: true, mode: 0o700 });
  const child = fork(TRANSFORM_WORKER_PATH, [], {
    env: {
      ...process.env,
      BROWSERSLIST_IGNORE_OLD_DATA: "1",
      TMPDIR: workerTmp,
    },
    stdio: ["ignore", "ignore", "pipe", "ipc"],
  });
  let nextRequestId = 1;
  let stderr = "";
  /** @type {{ requestId: number, resolve: (value: { success: boolean, error: string | null }) => void } | null} */
  let pending = null;
  child.stderr?.on("data", (chunk) => {
    stderr = `${stderr}${String(chunk)}`.slice(-100_000);
  });
  child.on("message", (message) => {
    if (
      typeof message !== "object" ||
      message === null ||
      Array.isArray(message)
    ) {
      return;
    }
    const response = /** @type {Partial<TransformWorkerResponse>} */ (message);
    if (
      pending === null ||
      response.requestId !== pending.requestId ||
      typeof response.success !== "boolean"
    ) {
      return;
    }
    const { resolve } = pending;
    pending = null;
    resolve({
      success: response.success,
      error: typeof response.error === "string" ? response.error : null,
    });
  });
  child.on("exit", (code) => {
    if (pending === null) return;
    const { resolve } = pending;
    pending = null;
    resolve({
      success: false,
      error:
        stderr.trim() ||
        `Transform worker ${String(workerIndex)} exited ${String(code)}`,
    });
  });
  return {
    run(params) {
      if (pending !== null) {
        return Promise.resolve({
          success: false,
          error: `Transform worker ${String(workerIndex)} is already busy`,
        });
      }
      const requestId = nextRequestId;
      nextRequestId += 1;
      return new Promise((resolve) => {
        pending = { requestId, resolve };
        child.send({ requestId, ...params }, (error) => {
          if (error === null || pending?.requestId !== requestId) return;
          pending = null;
          resolve({ success: false, error: error.message });
        });
      });
    },
    close() {
      return new Promise((resolve) => {
        if (child.exitCode !== null || child.killed) {
          resolve();
          return;
        }
        child.once("exit", () => resolve());
        child.kill("SIGTERM");
      });
    },
  };
}

/**
 * Process one parcel into a compressed capture and transformed artifact.
 *
 * @param {object} params - Parcel task.
 * @param {number} params.rowIndex - Zero-based seed row.
 * @param {CsvRecord} params.row - Seed data.
 * @param {string} params.outputDirectory - Private output root.
 * @param {string} params.scriptsZipPath - Patched scripts ZIP.
 * @param {TransformWorker} params.transformWorker - Isolated CLI worker.
 * @returns {Promise<LocalIngestResult>} Parcel outcome.
 */
async function processParcel({
  rowIndex,
  row,
  outputDirectory,
  scriptsZipPath,
  transformWorker,
}) {
  const started = Date.now();
  const rawIdentifier = row.request_identifier ?? row.parcel_id ?? "";
  const folio = normalizeBrowardFolio(rawIdentifier);
  if (folio === undefined) {
    return {
      rowIndex,
      folio: rawIdentifier,
      status: "source_error",
      durationMs: Date.now() - started,
      propertyUsageType: null,
      error: "Seed row has an invalid Broward folio",
    };
  }
  const shard = folio.slice(0, 4);
  const artifactPath = path.join(
    outputDirectory,
    "artifacts",
    shard,
    `${folio}.zip`,
  );
  if (await pathExists(artifactPath)) {
    return {
      rowIndex,
      folio,
      status: "skipped_existing",
      durationMs: Date.now() - started,
      propertyUsageType: readPropertyUsageType(artifactPath),
      error: null,
    };
  }
  const capturePath = path.join(
    outputDirectory,
    "captures",
    shard,
    `${folio}.json.gz`,
  );
  let capture;
  try {
    capture = await ensureCapture(folio, capturePath);
  } catch (error) {
    return {
      rowIndex,
      folio,
      status: "source_error",
      durationMs: Date.now() - started,
      propertyUsageType: null,
      error: error instanceof Error ? error.message : String(error),
    };
  }

  const temporaryDirectory = await mkdtemp(
    path.join(os.tmpdir(), `broward-full-${folio}-`),
  );
  try {
    const inputZipPath = path.join(temporaryDirectory, "input.zip");
    const outputZipPath = path.join(temporaryDirectory, "output.zip");
    createTransformInput({
      row,
      folio,
      capture,
      destination: inputZipPath,
    });
    const result = await transformWorker.run({
      workingDirectory: temporaryDirectory,
      inputZipPath,
      outputZipPath,
      scriptsZipPath,
    });
    if (!result.success) {
      return {
        rowIndex,
        folio,
        status: "transform_error",
        durationMs: Date.now() - started,
        propertyUsageType: null,
        error: result.error ?? "Unknown transform failure",
      };
    }
    await mkdir(path.dirname(artifactPath), {
      recursive: true,
      mode: 0o700,
    });
    await rename(outputZipPath, artifactPath);
    await chmod(artifactPath, 0o600);
    return {
      rowIndex,
      folio,
      status: "succeeded",
      durationMs: Date.now() - started,
      propertyUsageType: readPropertyUsageType(artifactPath),
      error: null,
    };
  } finally {
    await rm(temporaryDirectory, { recursive: true, force: true });
  }
}

/**
 * Apply one contiguous result window to state counters.
 *
 * @param {LocalIngestState} state - Mutable progress state.
 * @param {readonly LocalIngestResult[]} results - Ordered parcel results.
 * @returns {void}
 */
function applyResults(state, results) {
  for (const result of results) {
    state.attempted += 1;
    state.nextRowIndex = result.rowIndex + 1;
    if (result.status === "succeeded") state.succeeded += 1;
    else if (result.status === "skipped_existing") state.skippedExisting += 1;
    else state.failed += 1;
    if (result.propertyUsageType !== null) {
      state.usageTypes[result.propertyUsageType] =
        (state.usageTypes[result.propertyUsageType] ?? 0) + 1;
    }
  }
  state.updatedAt = new Date().toISOString();
}

/**
 * Execute a resumable local county ingestion.
 *
 * @param {LocalIngestOptions} options - Validated run options.
 * @returns {Promise<LocalIngestState>} Final or latest checkpoint.
 */
export async function runLocalIngestion(options) {
  const seedPath = path.resolve(options.seedPath);
  const scriptsDirectory = path.resolve(options.scriptsDirectory);
  const outputDirectory = path.resolve(options.outputDirectory);
  await mkdir(outputDirectory, { recursive: true, mode: 0o700 });
  await chmod(outputDirectory, 0o700);
  const statePath = path.join(outputDirectory, "state.json");
  const resultsPath = path.join(outputDirectory, "results.ndjson");
  const scriptsZipPath = path.join(outputDirectory, "broward-scripts.zip");
  const state = await readState(statePath, options.resetCheckpoint);
  packageScripts(scriptsDirectory, scriptsZipPath);
  await chmod(scriptsZipPath, 0o600);
  const workerRoot = path.join(outputDirectory, ".transform-workers");
  const transformWorkers = await Promise.all(
    Array.from({ length: options.concurrency }, (_, index) =>
      createTransformWorker(index, workerRoot),
    ),
  );

  try {
    const parser = createReadStream(seedPath).pipe(
      parse({ columns: true, skip_empty_lines: true }),
    );
    /** @type {{ rowIndex: number, row: CsvRecord }[]} */
    let window = [];
    let rowIndex = 0;
    let selected = 0;
    let consecutiveSourceFailureWindows = 0;

    /**
     * Process and checkpoint the current ordered window.
     *
     * @returns {Promise<void>}
     */
    const flushWindow = async () => {
      if (window.length === 0) return;
      const tasks = window;
      window = [];
      const results = await Promise.all(
        tasks.map((task, index) =>
          processParcel({
            ...task,
            outputDirectory,
            scriptsZipPath,
            transformWorker: transformWorkers[index],
          }),
        ),
      );
      for (const result of results) {
        await appendFile(
          resultsPath,
          `${JSON.stringify({
            timestamp: new Date().toISOString(),
            ...result,
          })}\n`,
          { mode: 0o600 },
        );
      }
      applyResults(state, results);
      await writeState(statePath, state);
      consecutiveSourceFailureWindows = results.every(
        (result) => result.status === "source_error",
      )
        ? consecutiveSourceFailureWindows + 1
        : 0;
      if (state.attempted % 100 < results.length) {
        console.log(
          JSON.stringify({
            level: "info",
            message: "broward_local_ingest_progress",
            ...state,
          }),
        );
      }
      if (consecutiveSourceFailureWindows >= 3) {
        throw new Error(
          "Stopped after three all-source-error windows; resume after checking BCPA availability",
        );
      }
    };

    for await (const parsedRow of parser) {
      const row = /** @type {CsvRecord} */ (parsedRow);
      if (rowIndex < state.nextRowIndex) {
        rowIndex += 1;
        continue;
      }
      if (options.limit !== null && selected >= options.limit) break;
      window.push({ rowIndex, row });
      selected += 1;
      rowIndex += 1;
      if (window.length >= options.concurrency) {
        await flushWindow();
      }
    }
    await flushWindow();
    console.log(
      JSON.stringify({
        level: "info",
        message: "broward_local_ingest_complete",
        ...state,
      }),
    );
    return state;
  } finally {
    await Promise.all(transformWorkers.map((worker) => worker.close()));
    await rm(workerRoot, { recursive: true, force: true });
  }
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  runLocalIngestion(parseCliOptions(process.argv.slice(2))).catch((error) => {
    console.error(
      JSON.stringify({
        level: "error",
        message: error instanceof Error ? error.message : String(error),
      }),
    );
    process.exitCode = 1;
  });
}
