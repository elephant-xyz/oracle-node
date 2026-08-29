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
  stat,
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
import {
  PUBLISHABLE_MODE,
  QUERY_DATA_ONLY_MODE,
  QUERY_DATA_ONLY_SCHEMA_VERSION,
  QUERY_DATA_ONLY_SUFFIX,
} from "./broward-query-data-only.mjs";

const gzipAsync = promisify(gzip);
const gunzipAsync = promisify(gunzip);
const DEFAULT_SEED_PATH = "downloads/broward/broward.csv";
const DEFAULT_SCRIPTS_DIRECTORY =
  "../Counties-trasform-scripts/broward/scripts";
const DEFAULT_OUTPUT_DIRECTORY = "downloads/broward/full-ingestion";
const DEFAULT_QUERY_DATA_ONLY_OUTPUT_DIRECTORY =
  "downloads/broward/query-data-only-ingestion";
const DEFAULT_CONCURRENCY = 2;
const MAX_CONCURRENCY = 4;
const SOURCE_MAX_ATTEMPTS = 4;
const INGEST_STATE_SCHEMA_VERSION = "oracle-node.broward-local-ingest-state.v2";
const QUERY_DATA_ONLY_RUN_MARKER = "QUERY_DATA_ONLY_DO_NOT_PUBLISH.json";
const TRANSFORM_WORKER_PATH = fileURLToPath(
  new URL("./broward-transform-worker-child.mjs", import.meta.url),
);

/**
 * @typedef {Record<string, string | undefined>} CsvRecord
 *
 * @typedef {"publishable" | "query-data-only"} BrowardArtifactMode
 *
 * @typedef {object} LocalIngestOptions
 * @property {string} seedPath - Complete Broward seed CSV.
 * @property {string} scriptsDirectory - Patched Broward transform directory.
 * @property {string} outputDirectory - Private local output root.
 * @property {number} concurrency - Concurrent parcel pipelines.
 * @property {number | null} limit - Optional count for a bounded run.
 * @property {boolean} resetCheckpoint - Ignore prior progress state.
 * @property {BrowardArtifactMode} artifactMode
 *   Full publication transform or explicitly non-publishable query-data-only transform.
 * @property {string | null} captureSource - Optional ZIP or sharded gzip capture source.
 * @property {number} startRow - Initial source row for a new, explicitly migrated data-only run.
 * @property {boolean} redactResults
 *   Replace folios and free-text errors in the private result journal with aggregate-safe fields.
 *
 * @typedef {object} LocalIngestState
 * @property {string} schemaVersion - Stable local checkpoint schema.
 * @property {BrowardArtifactMode} artifactMode
 *   Artifact contract guarded by this checkpoint.
 * @property {number} initialRowIndex - Immutable first row for this output.
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
 *
 * @typedef {object} CaptureSource
 * @property {string} description - Resolved source identity for run evidence.
 * @property {(folio: string) => Promise<Buffer>} read - Read one validated capture payload.
 *
 * @typedef {object} SeedTask
 * @property {number} rowIndex - Zero-based source row.
 * @property {CsvRecord} row - Parsed seed record.
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
    artifactMode: PUBLISHABLE_MODE,
    captureSource: null,
    startRow: 0,
    redactResults: false,
  };
  let outputWasSet = false;
  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index];
    if (flag === "--reset-checkpoint") {
      options.resetCheckpoint = true;
      continue;
    }
    if (flag === "--query-data-only") {
      options.artifactMode = QUERY_DATA_ONLY_MODE;
      continue;
    }
    if (flag === "--redact-results") {
      options.redactResults = true;
      continue;
    }
    const value = argv[index + 1];
    if (typeof value !== "string" || value.startsWith("--")) {
      throw new Error(`Missing value for ${flag}`);
    }
    if (flag === "--seed") options.seedPath = value;
    else if (flag === "--scripts") options.scriptsDirectory = value;
    else if (flag === "--output") {
      options.outputDirectory = value;
      outputWasSet = true;
    } else if (flag === "--capture-source") {
      options.captureSource = value;
    } else if (flag === "--concurrency") {
      options.concurrency = parsePositiveInteger(flag, value);
    } else if (flag === "--limit") {
      options.limit = parsePositiveInteger(flag, value);
    } else if (flag === "--start-row") {
      const startRow = Number.parseInt(value, 10);
      if (!Number.isInteger(startRow) || startRow < 0) {
        throw new Error("--start-row must be a non-negative integer");
      }
      options.startRow = startRow;
    } else {
      throw new Error(`Unknown option: ${flag}`);
    }
    index += 1;
  }
  if (options.concurrency > MAX_CONCURRENCY) {
    throw new Error(`--concurrency cannot exceed ${String(MAX_CONCURRENCY)}`);
  }
  if (options.artifactMode === QUERY_DATA_ONLY_MODE && !outputWasSet) {
    options.outputDirectory = DEFAULT_QUERY_DATA_ONLY_OUTPUT_DIRECTORY;
  }
  if (
    options.artifactMode === QUERY_DATA_ONLY_MODE &&
    !options.outputDirectory.toLowerCase().includes(QUERY_DATA_ONLY_MODE)
  ) {
    throw new Error(
      "--query-data-only output path must include 'query-data-only'",
    );
  }
  if (options.startRow > 0 && options.artifactMode !== QUERY_DATA_ONLY_MODE) {
    throw new Error("--start-row is supported only with --query-data-only");
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
 * @param {BrowardArtifactMode} artifactMode
 *   Artifact contract requested by this invocation.
 * @param {number} startRow - Immutable first row requested for this output.
 * @returns {Promise<LocalIngestState>} Ingestion state.
 */
async function readState(statePath, reset, artifactMode, startRow) {
  if (!reset && (await pathExists(statePath))) {
    const parsed = /** @type {unknown} */ (
      JSON.parse(await readFile(statePath, "utf8"))
    );
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      Array.isArray(parsed)
    ) {
      throw new Error(`Invalid Broward checkpoint: ${statePath}`);
    }
    const candidate = /** @type {Partial<LocalIngestState>} */ (parsed);
    const priorMode = candidate.artifactMode ?? PUBLISHABLE_MODE;
    if (priorMode !== artifactMode) {
      throw new Error(
        `Checkpoint artifact mode ${priorMode} cannot resume as ${artifactMode}`,
      );
    }
    const priorStartRow = candidate.initialRowIndex ?? 0;
    if (priorStartRow !== startRow) {
      throw new Error(
        `Checkpoint initial row ${String(priorStartRow)} cannot resume with --start-row ${String(startRow)}`,
      );
    }
    if (
      typeof candidate.startedAt !== "string" ||
      typeof candidate.updatedAt !== "string" ||
      !Number.isInteger(candidate.nextRowIndex) ||
      !Number.isInteger(candidate.attempted) ||
      !Number.isInteger(candidate.succeeded) ||
      !Number.isInteger(candidate.skippedExisting) ||
      !Number.isInteger(candidate.failed) ||
      typeof candidate.usageTypes !== "object" ||
      candidate.usageTypes === null ||
      Array.isArray(candidate.usageTypes)
    ) {
      throw new Error(`Invalid Broward checkpoint: ${statePath}`);
    }
    return /** @type {LocalIngestState} */ ({
      ...candidate,
      schemaVersion: INGEST_STATE_SCHEMA_VERSION,
      artifactMode: priorMode,
      initialRowIndex: priorStartRow,
    });
  }
  const now = new Date().toISOString();
  return {
    schemaVersion: INGEST_STATE_SCHEMA_VERSION,
    artifactMode,
    initialRowIndex: startRow,
    startedAt: now,
    updatedAt: now,
    nextRowIndex: startRow,
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
 * Parse and fail-closed validate one uncompressed prepare capture.
 *
 * @param {Buffer} bytes - UTF-8 JSON bytes.
 * @param {string} folio - Canonical folio expected in the envelope.
 * @returns {Record<string, unknown>} Valid multi-request wrapper.
 */
function parseCapture(bytes, folio) {
  const parsed = /** @type {unknown} */ (JSON.parse(bytes.toString("utf8")));
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(`Capture for ${folio} is not a JSON object`);
  }
  const payload = /** @type {Record<string, unknown>} */ (parsed);
  requireParcelRecords(unwrapBrowardPrepareCapture(payload), folio);
  return payload;
}

/**
 * Open an optional read-only ZIP or sharded gzip capture source.
 *
 * When a source is supplied, a missing folio fails instead of falling back to
 * BCPA. This makes capture-only benchmarks and transform redrives provably
 * zero-traffic.
 *
 * @param {string | null} sourcePath - ZIP archive or directory containing `{shard}/{folio}.json.gz`.
 * @returns {Promise<CaptureSource | null>} Reusable read-only capture source.
 */
async function createCaptureSource(sourcePath) {
  if (sourcePath === null) return null;
  const resolved = path.resolve(sourcePath);
  const sourceStat = await stat(resolved);
  if (sourceStat.isDirectory()) {
    return {
      description: resolved,
      async read(folio) {
        const compressedPath = path.join(
          resolved,
          folio.slice(0, 4),
          `${folio}.json.gz`,
        );
        try {
          return await gunzipAsync(await readFile(compressedPath));
        } catch (error) {
          throw new Error(
            `Capture source has no valid compressed capture for ${folio}: ${
              error instanceof Error ? error.message : String(error)
            }`,
          );
        }
      },
    };
  }
  if (!sourceStat.isFile() || path.extname(resolved).toLowerCase() !== ".zip") {
    throw new Error(
      "--capture-source must be a ZIP or sharded capture directory",
    );
  }
  const archive = new AdmZip(resolved);
  return {
    description: resolved,
    read(folio) {
      const entry = archive.getEntry(`${folio}.json`);
      if (entry === null) {
        return Promise.reject(
          new Error(`Capture archive is missing ${folio}.json`),
        );
      }
      return Promise.resolve(entry.getData());
    },
  };
}

/**
 * Atomically store an uncompressed capture as private gzip JSON.
 *
 * @param {string} capturePath - Final `.json.gz` path.
 * @param {Buffer} bytes - Canonical uncompressed capture bytes.
 * @returns {Promise<void>} Resolves after the compressed capture is renamed.
 */
async function writeCompressedCapture(capturePath, bytes) {
  await mkdir(path.dirname(capturePath), { recursive: true, mode: 0o700 });
  const compressed = await gzipAsync(bytes, { level: 9 });
  const temporaryPath = `${capturePath}.${String(process.pid)}.tmp`;
  await writeFile(temporaryPath, compressed, { mode: 0o600 });
  await rename(temporaryPath, capturePath);
}

/**
 * Read a prior compressed capture, import one from a read-only source, or
 * fetch and atomically store a fresh one.
 *
 * @param {string} folio - Canonical folio.
 * @param {string} capturePath - Gzip JSON path.
 * @param {CaptureSource | null} captureSource - Optional zero-traffic source.
 * @returns {Promise<Record<string, unknown>>} Multi-request capture wrapper.
 */
async function ensureCapture(folio, capturePath, captureSource) {
  if (await pathExists(capturePath)) {
    const bytes = await gunzipAsync(await readFile(capturePath));
    return parseCapture(bytes, folio);
  }
  if (captureSource !== null) {
    const bytes = await captureSource.read(folio);
    const payload = parseCapture(bytes, folio);
    await writeCompressedCapture(
      capturePath,
      Buffer.from(`${JSON.stringify(payload)}\n`),
    );
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
  await writeCompressedCapture(
    capturePath,
    Buffer.from(`${JSON.stringify(payload)}\n`),
  );
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
 *   scriptsZipPath: string,
 *   folio: string
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
 * @param {BrowardArtifactMode} artifactMode
 *   Transform behavior fixed for the lifetime of this worker.
 * @returns {Promise<TransformWorker>} Ready worker.
 */
async function createTransformWorker(workerIndex, workerRoot, artifactMode) {
  const workerTmp = path.join(workerRoot, String(workerIndex));
  await mkdir(workerTmp, { recursive: true, mode: 0o700 });
  const child = fork(TRANSFORM_WORKER_PATH, [], {
    env: {
      ...process.env,
      BROWSERSLIST_IGNORE_OLD_DATA: "1",
      TMPDIR: workerTmp,
      BROWARD_ARTIFACT_MODE: artifactMode,
      ...(artifactMode === QUERY_DATA_ONLY_MODE
        ? { BROWARD_QUERY_DATA_ONLY: "1" }
        : {}),
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
 * @param {CaptureSource | null} params.captureSource - Optional zero-traffic capture source.
 * @param {BrowardArtifactMode} params.artifactMode
 *   Output artifact contract.
 * @returns {Promise<LocalIngestResult>} Parcel outcome.
 */
async function processParcel({
  rowIndex,
  row,
  outputDirectory,
  scriptsZipPath,
  transformWorker,
  captureSource,
  artifactMode,
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
  const artifactDirectoryName =
    artifactMode === QUERY_DATA_ONLY_MODE
      ? "query-data-only-artifacts"
      : "artifacts";
  const artifactFileName =
    artifactMode === QUERY_DATA_ONLY_MODE
      ? `${folio}${QUERY_DATA_ONLY_SUFFIX}`
      : `${folio}.zip`;
  const artifactPath = path.join(
    outputDirectory,
    artifactDirectoryName,
    shard,
    artifactFileName,
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
    capture = await ensureCapture(folio, capturePath, captureSource);
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
      folio,
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
 * Classify one parcel failure without retaining source text or its folio.
 *
 * Expected GIS-only appraiser misses have a stable marker emitted by
 * `requireParcelRecords`. All other source and transform failures are reduced
 * to their fixed pipeline stage. Successful results have no failure class.
 *
 * @param {LocalIngestResult} result - Complete in-memory parcel outcome.
 * @returns {"source_miss" | "source_error" | "transform_error" | null}
 *   Aggregate-safe failure category.
 */
export function classifyRedactedFailure(result) {
  if (result.status === "transform_error") return "transform_error";
  if (result.status !== "source_error") return null;
  return typeof result.error === "string" &&
    result.error.includes("returned no parcelInfok__BackingField")
    ? "source_miss"
    : "source_error";
}

/**
 * Serialize one parcel result for the append-only local journal.
 *
 * Recovery mode deliberately omits `folio` and free-text `error`; `rowIndex`
 * is sufficient for the in-process orchestrator to correlate an outcome with
 * its bounded seed chunk. The normal legacy journal remains unchanged unless
 * the caller explicitly enables redaction.
 *
 * @param {LocalIngestResult} result - Complete in-memory parcel outcome.
 * @param {BrowardArtifactMode} artifactMode - Guarded artifact contract.
 * @param {boolean} redactResults - Whether identifiers and source text must be omitted.
 * @param {string} timestamp - ISO timestamp shared with the journal entry.
 * @returns {Record<string, string | number | null>} JSON-safe journal record.
 */
export function serializeIngestResult(
  result,
  artifactMode,
  redactResults,
  timestamp,
) {
  if (!redactResults) {
    return {
      timestamp,
      artifactMode,
      ...result,
    };
  }
  return {
    timestamp,
    artifactMode,
    rowIndex: result.rowIndex,
    status: result.status,
    durationMs: result.durationMs,
    propertyUsageType: result.propertyUsageType,
    failureClass: classifyRedactedFailure(result),
  };
}

/**
 * Keep every long-lived worker busy by handing it the next seed row as soon as
 * its previous transform finishes.
 *
 * Results may finish out of order, but this pool releases only the contiguous
 * source-order prefix to `commitResults`. Therefore the existing
 * `nextRowIndex` checkpoint remains an atomic, resume-safe high-water mark.
 * At most one task per worker is in flight, so source concurrency never exceeds
 * the existing transform concurrency limit.
 *
 * @param {object} params - Pool dependencies.
 * @param {readonly TransformWorker[]} params.workers - Long-lived isolated workers.
 * @param {AsyncIterator<SeedTask>} params.taskIterator - Ordered streaming seed tasks.
 * @param {number} params.firstRowIndex - First row eligible for checkpointing.
 * @param {(task: SeedTask, worker: TransformWorker) => Promise<LocalIngestResult>} params.runTask
 *   Parcel processor bound to run paths and capture policy.
 * @param {(results: readonly LocalIngestResult[]) => Promise<boolean>} params.commitResults
 *   Persist one contiguous result prefix and return true to stop new handoffs.
 * @returns {Promise<{ stopped: boolean }>} Whether the commit policy stopped dispatch.
 */
export async function runWorkerHandoffs({
  workers,
  taskIterator,
  firstRowIndex,
  runTask,
  commitResults,
}) {
  /** @type {Map<number, Promise<{ workerIndex: number, result: LocalIngestResult }>>} */
  const inFlight = new Map();
  /** @type {Map<number, LocalIngestResult>} */
  const completed = new Map();
  let nextCommitRow = firstRowIndex;
  let sourceExhausted = false;
  let stopDispatch = false;

  /**
   * Give one worker its next task unless the source or safety policy stopped.
   *
   * @param {number} workerIndex - Stable worker index.
   * @returns {Promise<void>} Resolves after dispatch or exhaustion.
   */
  async function dispatch(workerIndex) {
    if (sourceExhausted || stopDispatch) return;
    const next = await taskIterator.next();
    if (next.done) {
      sourceExhausted = true;
      return;
    }
    const worker = workers[workerIndex];
    if (worker === undefined) {
      throw new Error(`Missing transform worker ${String(workerIndex)}`);
    }
    inFlight.set(
      workerIndex,
      runTask(next.value, worker).then((result) => ({ workerIndex, result })),
    );
  }

  for (let workerIndex = 0; workerIndex < workers.length; workerIndex += 1) {
    await dispatch(workerIndex);
  }
  while (inFlight.size > 0) {
    const completedWorker = await Promise.race(inFlight.values());
    inFlight.delete(completedWorker.workerIndex);
    const { result } = completedWorker;
    if (completed.has(result.rowIndex) || result.rowIndex < nextCommitRow) {
      throw new Error(
        `Duplicate transform result for row ${String(result.rowIndex)}`,
      );
    }
    completed.set(result.rowIndex, result);
    /** @type {LocalIngestResult[]} */
    const contiguous = [];
    while (completed.has(nextCommitRow)) {
      const orderedResult = completed.get(nextCommitRow);
      completed.delete(nextCommitRow);
      if (orderedResult === undefined) {
        throw new Error(
          `Missing completed result for row ${String(nextCommitRow)}`,
        );
      }
      contiguous.push(orderedResult);
      nextCommitRow += 1;
    }
    if (contiguous.length > 0 && (await commitResults(contiguous))) {
      stopDispatch = true;
    }
    await dispatch(completedWorker.workerIndex);
  }
  if (completed.size > 0) {
    throw new Error("Worker handoff pool ended with a checkpoint ordering gap");
  }
  return { stopped: stopDispatch };
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
  if (
    options.artifactMode === QUERY_DATA_ONLY_MODE &&
    !outputDirectory.toLowerCase().includes(QUERY_DATA_ONLY_MODE)
  ) {
    throw new Error(
      "Query-data-only output path must include 'query-data-only'",
    );
  }
  await mkdir(outputDirectory, { recursive: true, mode: 0o700 });
  await chmod(outputDirectory, 0o700);
  const statePath = path.join(outputDirectory, "state.json");
  const resultsPath = path.join(outputDirectory, "results.ndjson");
  const scriptsZipPath = path.join(outputDirectory, "broward-scripts.zip");
  const queryDataOnlyMarkerPath = path.join(
    outputDirectory,
    QUERY_DATA_ONLY_RUN_MARKER,
  );
  if (
    options.artifactMode === PUBLISHABLE_MODE &&
    (await pathExists(queryDataOnlyMarkerPath))
  ) {
    throw new Error(
      `Refusing publishable mode in marked query-data-only output ${outputDirectory}`,
    );
  }
  const captureSource = await createCaptureSource(options.captureSource);
  const state = await readState(
    statePath,
    options.resetCheckpoint,
    options.artifactMode,
    options.startRow,
  );
  if (options.artifactMode === QUERY_DATA_ONLY_MODE) {
    await writeFile(
      queryDataOnlyMarkerPath,
      `${JSON.stringify(
        {
          schemaVersion: QUERY_DATA_ONLY_SCHEMA_VERSION,
          artifactMode: QUERY_DATA_ONLY_MODE,
          publishable: false,
          captureSource: captureSource?.description ?? "live-bcpa",
          initialRowIndex: options.startRow,
          sourceConcurrencyMaximum: options.concurrency,
          artifactDirectory: "query-data-only-artifacts",
          artifactSuffix: QUERY_DATA_ONLY_SUFFIX,
          regeneration:
            "Use the preserved seed and gzip captures in a separate publishable-mode output; never publish this directory.",
        },
        null,
        2,
      )}\n`,
      { mode: 0o600 },
    );
  }
  packageScripts(scriptsDirectory, scriptsZipPath);
  await chmod(scriptsZipPath, 0o600);
  const workerRoot = path.join(outputDirectory, ".transform-workers");
  const transformWorkers = await Promise.all(
    Array.from({ length: options.concurrency }, (_, index) =>
      createTransformWorker(index, workerRoot, options.artifactMode),
    ),
  );

  try {
    const parser = createReadStream(seedPath).pipe(
      parse({ columns: true, skip_empty_lines: true }),
    );
    /**
     * Stream only the uncheckpointed, bounded seed rows.
     *
     * @returns {AsyncGenerator<SeedTask, void, void>} Ordered parcel tasks.
     */
    async function* selectedTasks() {
      let rowIndex = 0;
      let selected = 0;
      for await (const parsedRow of parser) {
        const row = /** @type {CsvRecord} */ (parsedRow);
        if (rowIndex < state.nextRowIndex) {
          rowIndex += 1;
          continue;
        }
        if (options.limit !== null && selected >= options.limit) return;
        yield { rowIndex, row };
        selected += 1;
        rowIndex += 1;
      }
    }

    let consecutiveSourceErrors = 0;
    const sourceFailureLimit = 3 * options.concurrency;
    const handoffResult = await runWorkerHandoffs({
      workers: transformWorkers,
      taskIterator: selectedTasks()[Symbol.asyncIterator](),
      firstRowIndex: state.nextRowIndex,
      runTask(task, transformWorker) {
        return processParcel({
          ...task,
          outputDirectory,
          scriptsZipPath,
          transformWorker,
          captureSource,
          artifactMode: options.artifactMode,
        });
      },
      async commitResults(results) {
        const previousAttempted = state.attempted;
        await appendFile(
          resultsPath,
          results
            .map((result) =>
              JSON.stringify(
                serializeIngestResult(
                  result,
                  options.artifactMode,
                  options.redactResults,
                  new Date().toISOString(),
                ),
              ),
            )
            .join("\n")
            .concat("\n"),
          { mode: 0o600 },
        );
        for (const result of results) {
          consecutiveSourceErrors =
            result.status === "source_error" ? consecutiveSourceErrors + 1 : 0;
        }
        applyResults(state, results);
        await writeState(statePath, state);
        if (
          Math.floor(previousAttempted / 100) <
          Math.floor(state.attempted / 100)
        ) {
          console.log(
            JSON.stringify({
              level: "info",
              message: "broward_local_ingest_progress",
              captureSource: captureSource?.description ?? "live-bcpa",
              ...state,
            }),
          );
        }
        return consecutiveSourceErrors >= sourceFailureLimit;
      },
    });
    if (handoffResult.stopped) {
      throw new Error(
        `Stopped after ${String(sourceFailureLimit)} consecutive source errors; resume after checking the capture source or BCPA availability`,
      );
    }
    console.log(
      JSON.stringify({
        level: "info",
        message: "broward_local_ingest_complete",
        captureSource: captureSource?.description ?? "live-bcpa",
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
