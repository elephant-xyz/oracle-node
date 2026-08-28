#!/usr/bin/env node

/**
 * Aggregate-only live dashboard for the local Broward appraisal ingestion.
 *
 * The server reads local checkpoint artifacts without changing them. It never
 * returns parcel identifiers, addresses, owner/contact fields, source payloads,
 * result error text, or log contents.
 */

import { createReadStream } from "node:fs";
import {
  readFile,
  readdir,
  readlink,
  stat,
  statfs,
} from "node:fs/promises";
import { createServer } from "node:http";
import path from "node:path";
import { pathToFileURL } from "node:url";

export const BROWARD_ROW_DENOMINATOR = 534_309;
export const DEFAULT_DASHBOARD_HOST = "127.0.0.1";
export const DEFAULT_DASHBOARD_PORT = 47_831;
const DEFAULT_OUTPUT_DIRECTORY = "downloads/broward/full-ingestion";
const DEFAULT_LOG_PATH = "downloads/broward/broward-full-ingestion.log";
const DEFAULT_HANDOFF_MANIFEST_PATH =
  "downloads/broward/active-query-data-only-handoff.json";
const RECENT_WINDOW_MS = 15 * 60 * 1_000;
const STALE_AFTER_MS = 2 * 60 * 1_000;
const ACTIVE_GAP_CAP_MS = STALE_AFTER_MS;

const RESULT_NONE = 0;
const RESULT_SUCCEEDED = 1;
const RESULT_SKIPPED = 2;
const RESULT_SOURCE_MISS = 3;
const RESULT_SOURCE_ERROR = 4;
const RESULT_TRANSFORM_ERROR = 5;

/**
 * @typedef {object} DashboardCliOptions
 * @property {string} host - Interface on which the local HTTP server listens.
 * @property {number} port - Unprivileged TCP port used by the HTTP server.
 * @property {boolean} help - Whether usage text should be printed.
 *
 * @typedef {object} IngestionState
 * @property {string} startedAt - ISO timestamp for the checkpoint lineage.
 * @property {string} updatedAt - ISO timestamp of the latest complete checkpoint.
 * @property {number} nextRowIndex - Next zero-based seed row.
 * @property {number} attempted - Number of rows attempted.
 * @property {number} succeeded - Number of newly transformed rows.
 * @property {number} skippedExisting - Number of existing transformed rows reused.
 * @property {number} failed - Combined source and transform failure count.
 * @property {Record<string, number>} usageTypes - Successful property-use aggregates.
 * @property {string | null} artifactMode - Guarded artifact contract when recorded.
 * @property {number | null} initialRowIndex - Immutable segment start when recorded.
 *
 * @typedef {object} HandoffManifest
 * @property {number} seedRowCount - Full county row denominator.
 * @property {"live-bcpa"} sourceMode - Uncaptured post-boundary source mode.
 * @property {number} sourceConcurrencyMaximum - Maximum post-boundary source concurrency.
 * @property {string} oldOutputDirectory - Pre-boundary publishable output path.
 * @property {{ nextRowIndex: number, attempted: number }} oldCheckpoint
 *   Immutable pre-boundary checkpoint counters.
 * @property {string} newOutputDirectory - Post-boundary data-only output path.
 * @property {string} newLogPath - Post-boundary runner log path.
 * @property {"query-data-only"} newArtifactMode - Guarded post-boundary artifact mode.
 * @property {number} newInitialRowIndex - Exact inclusive handoff row.
 * @property {{
 *   excludedOldAtOrAboveBoundary: {
 *     resultRowIndexes: readonly number[],
 *     artifactRowIndexes: readonly number[],
 *     captureRowIndexes: readonly number[]
 *   }
 * }} reconciliation - Explicitly excluded old higher-row files.
 *
 * @typedef {object} ResultCounts
 * @property {number} succeeded - Deduplicated successful result rows.
 * @property {number} skippedExisting - Deduplicated reused result rows.
 * @property {number} sourceMisses - Source responses with no usable parcel record.
 * @property {number} sourceErrors - Source failures other than an expected empty response.
 * @property {number} transformErrors - County transform failures.
 * @property {number} parsedRows - Valid result records observed, including replacements.
 * @property {number} malformedLines - Lines ignored because they were not valid result records.
 * @property {Float64Array} rowTimestamps - Latest result timestamp by seed row.
 *
 * @typedef {object} FileHealth
 * @property {boolean} available - Whether the fixed local artifact exists.
 * @property {number | null} sizeBytes - Artifact size, or null when unavailable.
 * @property {string | null} modifiedAt - ISO modification time, or null when unavailable.
 * @property {number | null} ageSeconds - Whole seconds since modification.
 *
 * @typedef {object} StorageHealth
 * @property {boolean} available - Whether filesystem capacity information is available.
 * @property {number | null} totalBytes - Filesystem capacity in bytes.
 * @property {number | null} freeBytes - Bytes available to the current user.
 * @property {number | null} usedPercent - Percentage of filesystem capacity in use.
 * @property {{ state: FileHealth, results: FileHealth, log: FileHealth }} files
 *   Health for the three fixed dashboard inputs.
 * @property {number} parsedResultRows - Valid result records observed by the scanner.
 * @property {number} malformedResultLines - Invalid result lines ignored by the scanner.
 *
 * @typedef {"running" | "stale" | "stopped" | "complete" | "unknown"} ProcessStatus
 *
 * @typedef {object} DashboardStatus
 * @property {1} schemaVersion - Aggregate endpoint schema version.
 * @property {string} generatedAt - ISO timestamp when the snapshot was assembled.
 * @property {"Broward"} county - Public county label.
 * @property {number} denominator - Full seed row count.
 * @property {{
 *   status: ProcessStatus,
 *   running: boolean | null,
 *   stale: boolean,
 *   lastActivityAt: string | null,
 *   activityAgeSeconds: number | null,
 *   staleAfterSeconds: number
 * }} process - Local process and freshness summary.
 * @property {{
 *   attempted: number,
 *   succeeded: number,
 *   skippedExisting: number,
 *   sourceMisses: number,
 *   sourceErrors: number,
 *   transformErrors: number,
 *   unclassifiedFailures: number,
 *   failedTotal: number,
 *   remaining: number,
 *   completionPercent: number
 * }} progress - Aggregate checkpoint and deduplicated outcome counts.
 * @property {{
 *   windowMinutes: number,
 *   recentAttempted: number,
 *   recentPerMinute: number | null,
 *   activeRuntimeSeconds: number,
 *   activeAveragePerMinute: number | null,
 *   etaActiveSeconds: number | null,
 *   etaBasis: "recent" | "active_average" | null,
 *   projectedCompletionAt: string | null
 * }} throughput - Recent rate and downtime-capped active-runtime projection.
 * @property {{ lastCheckpointAt: string, ageSeconds: number }} checkpoint
 *   Latest checkpoint metadata.
 * @property {{ type: string, count: number }[]} usageTypes
 *   Sanitized property-use aggregate counts sorted by count.
 * @property {StorageHealth} storage - Local filesystem and input-file health.
 * @property {{
 *   active: true,
 *   boundaryRowIndex: number,
 *   publishableAttempted: number,
 *   publishableSucceeded: number,
 *   dataOnlyAttempted: number,
 *   dataOnlySucceeded: number,
 *   dataOnlyTransformErrors: number,
 *   excludedOldResultRows: number,
 *   excludedOldArtifacts: number,
 *   preservedExcludedOldCaptures: number
 * } | undefined} handoff - Aggregate segment lineage when a handoff is active.
 *
 * @typedef {object} StatusReaderOptions
 * @property {string} outputDirectory - Fixed local ingestion output directory.
 * @property {string} logPath - Fixed local ingestion log path.
 * @property {number} denominator - Full county seed count.
 *
 * @typedef {object} StatusReaderDependencies
 * @property {() => number} now - Current epoch-millisecond provider.
 * @property {(outputDirectory: string) => Promise<boolean | null>} probeProcess
 *   Read-only local ingestion process detector.
 */

/**
 * Parse dashboard host and port options.
 *
 * Only the listening interface and port are configurable. Input paths remain
 * fixed so the server cannot be used as a general local-file browser.
 *
 * @param {readonly string[]} argv - Arguments following the script path.
 * @returns {DashboardCliOptions} Validated dashboard options.
 */
export function parseDashboardCliOptions(argv) {
  /** @type {DashboardCliOptions} */
  const options = {
    host: DEFAULT_DASHBOARD_HOST,
    port: DEFAULT_DASHBOARD_PORT,
    help: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index];
    if (flag === "--help" || flag === "-h") {
      options.help = true;
      continue;
    }
    const value = argv[index + 1];
    if (typeof value !== "string" || value.startsWith("--")) {
      throw new Error(`Missing value for ${String(flag)}`);
    }
    if (flag === "--host") {
      if (value.trim() === "" || /[\s/]/u.test(value)) {
        throw new Error("--host must be a hostname or IP address");
      }
      options.host = value;
    } else if (flag === "--port") {
      const port = Number(value);
      if (!Number.isInteger(port) || port < 1_024 || port > 65_535) {
        throw new Error("--port must be an integer from 1024 through 65535");
      }
      options.port = port;
    } else {
      throw new Error(`Unknown option: ${String(flag)}`);
    }
    index += 1;
  }
  return options;
}

/**
 * Narrow an unknown JSON value to a non-null object record.
 *
 * @param {unknown} value - Value being validated.
 * @returns {value is Record<string, unknown>} Whether the value is a record.
 */
function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Read a finite non-negative number from a validated JSON record.
 *
 * @param {Record<string, unknown>} record - Parsed checkpoint object.
 * @param {string} key - Required numeric property.
 * @returns {number} Validated number.
 */
function readCount(record, key) {
  const value = record[key];
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    value < 0 ||
    !Number.isInteger(value)
  ) {
    throw new Error(`Checkpoint field ${key} is not a non-negative integer`);
  }
  return value;
}

/**
 * Parse and validate the aggregate fields used from state.json.
 *
 * The returned object deliberately omits every field not needed by the
 * dashboard.
 *
 * @param {string} text - UTF-8 checkpoint JSON.
 * @returns {IngestionState} Validated aggregate checkpoint.
 */
function parseIngestionState(text) {
  /** @type {unknown} */
  const parsed = JSON.parse(text);
  if (!isRecord(parsed)) {
    throw new Error("Checkpoint is not a JSON object");
  }
  const startedAt = parsed.startedAt;
  const updatedAt = parsed.updatedAt;
  if (
    typeof startedAt !== "string" ||
    !Number.isFinite(Date.parse(startedAt)) ||
    typeof updatedAt !== "string" ||
    !Number.isFinite(Date.parse(updatedAt))
  ) {
    throw new Error("Checkpoint timestamps are invalid");
  }
  const usageTypesValue = parsed.usageTypes;
  if (!isRecord(usageTypesValue)) {
    throw new Error("Checkpoint usageTypes is not an object");
  }
  /** @type {Record<string, number>} */
  const usageTypes = {};
  for (const [usageType, count] of Object.entries(usageTypesValue)) {
    if (
      typeof count === "number" &&
      Number.isInteger(count) &&
      count >= 0
    ) {
      usageTypes[usageType] = count;
    }
  }
  return {
    startedAt,
    updatedAt,
    nextRowIndex: readCount(parsed, "nextRowIndex"),
    attempted: readCount(parsed, "attempted"),
    succeeded: readCount(parsed, "succeeded"),
    skippedExisting: readCount(parsed, "skippedExisting"),
    failed: readCount(parsed, "failed"),
    usageTypes,
    artifactMode:
      typeof parsed.artifactMode === "string" ? parsed.artifactMode : null,
    initialRowIndex:
      typeof parsed.initialRowIndex === "number" &&
      Number.isInteger(parsed.initialRowIndex) &&
      parsed.initialRowIndex >= 0
        ? parsed.initialRowIndex
        : null,
  };
}

/**
 * Convert one private result record into a small aggregate result code.
 *
 * The only inspected error detail is a fixed source-miss marker. Error text is
 * never retained or returned.
 *
 * @param {Record<string, unknown>} result - Parsed NDJSON result record.
 * @returns {number} Internal aggregate status code.
 */
function classifyResult(result) {
  const status = result.status;
  if (status === "succeeded") return RESULT_SUCCEEDED;
  if (status === "skipped_existing") return RESULT_SKIPPED;
  if (status === "transform_error") return RESULT_TRANSFORM_ERROR;
  if (status === "source_error") {
    return typeof result.error === "string" &&
      result.error.includes("returned no parcelInfok__BackingField")
      ? RESULT_SOURCE_MISS
      : RESULT_SOURCE_ERROR;
  }
  return RESULT_NONE;
}

/**
 * Incrementally scan results.ndjson into fixed-size numeric arrays.
 *
 * Row-index deduplication prevents resumed rows from inflating outcome counts.
 * No source record, identifier, error, address, or transformed entity is kept.
 */
class ResultAccumulator {
  /**
   * @param {string} resultsPath - Private append-only result file.
   * @param {number} denominator - Maximum accepted zero-based row index.
   */
  constructor(resultsPath, denominator) {
    this.resultsPath = resultsPath;
    this.denominator = denominator;
    this.offset = 0;
    this.pending = "";
    this.startedAt = "";
    this.resultCodes = new Uint8Array(denominator);
    this.rowTimestamps = new Float64Array(denominator);
    this.parsedRows = 0;
    this.malformedLines = 0;
  }

  /**
   * Clear scanner state when the file is replaced or checkpoint lineage changes.
   *
   * @param {string} startedAt - Current checkpoint start timestamp.
   * @returns {void}
   */
  reset(startedAt) {
    this.offset = 0;
    this.pending = "";
    this.startedAt = startedAt;
    this.resultCodes.fill(RESULT_NONE);
    this.rowTimestamps.fill(0);
    this.parsedRows = 0;
    this.malformedLines = 0;
  }

  /**
   * Read bytes appended since the previous snapshot.
   *
   * @param {string} startedAt - Current checkpoint lineage timestamp.
   * @returns {Promise<void>}
   */
  async update(startedAt) {
    let resultStat;
    try {
      resultStat = await stat(this.resultsPath);
    } catch (error) {
      if (isMissingFileError(error)) {
        this.reset(startedAt);
        return;
      }
      throw error;
    }
    if (
      this.startedAt !== startedAt ||
      resultStat.size < this.offset
    ) {
      this.reset(startedAt);
    }
    if (resultStat.size === this.offset) return;

    let appended = "";
    const stream = createReadStream(this.resultsPath, {
      encoding: "utf8",
      start: this.offset,
      end: resultStat.size - 1,
    });
    for await (const chunk of stream) {
      appended += chunk;
    }
    this.offset = resultStat.size;
    const lines = `${this.pending}${appended}`.split("\n");
    this.pending = lines.pop() ?? "";
    for (const line of lines) {
      if (line.trim() !== "") this.consumeLine(line, startedAt);
    }
  }

  /**
   * Reduce one complete result line to row status and timestamp.
   *
   * @param {string} line - One complete NDJSON line.
   * @param {string} startedAt - Current checkpoint lineage timestamp.
   * @returns {void}
   */
  consumeLine(line, startedAt) {
    try {
      /** @type {unknown} */
      const value = JSON.parse(line);
      if (!isRecord(value)) {
        this.malformedLines += 1;
        return;
      }
      const rowIndex = value.rowIndex;
      const timestamp = value.timestamp;
      const resultCode = classifyResult(value);
      const timestampMs =
        typeof timestamp === "string" ? Date.parse(timestamp) : Number.NaN;
      if (
        typeof rowIndex !== "number" ||
        !Number.isInteger(rowIndex) ||
        rowIndex < 0 ||
        rowIndex >= this.denominator ||
        resultCode === RESULT_NONE ||
        !Number.isFinite(timestampMs)
      ) {
        this.malformedLines += 1;
        return;
      }
      if (timestampMs < Date.parse(startedAt)) return;
      this.resultCodes[rowIndex] = resultCode;
      this.rowTimestamps[rowIndex] = timestampMs;
      this.parsedRows += 1;
    } catch {
      this.malformedLines += 1;
    }
  }

  /**
   * Count deduplicated outcomes through the latest completed checkpoint row.
   *
   * @param {number} nextRowIndex - Exclusive row-index limit from state.json.
   * @returns {ResultCounts} Aggregate outcomes and row timestamps.
   */
  summarize(nextRowIndex) {
    const limit = Math.min(
      Math.max(0, nextRowIndex),
      this.denominator,
    );
    let succeeded = 0;
    let skippedExisting = 0;
    let sourceMisses = 0;
    let sourceErrors = 0;
    let transformErrors = 0;
    for (let index = 0; index < limit; index += 1) {
      const code = this.resultCodes[index];
      if (code === RESULT_SUCCEEDED) succeeded += 1;
      else if (code === RESULT_SKIPPED) skippedExisting += 1;
      else if (code === RESULT_SOURCE_MISS) sourceMisses += 1;
      else if (code === RESULT_SOURCE_ERROR) sourceErrors += 1;
      else if (code === RESULT_TRANSFORM_ERROR) transformErrors += 1;
    }
    return {
      succeeded,
      skippedExisting,
      sourceMisses,
      sourceErrors,
      transformErrors,
      parsedRows: this.parsedRows,
      malformedLines: this.malformedLines,
      rowTimestamps: this.rowTimestamps,
    };
  }
}

/**
 * Identify an expected missing-file error without weakening unknown-error checks.
 *
 * @param {unknown} error - Filesystem error.
 * @returns {boolean} Whether the error carries the ENOENT code.
 */
function isMissingFileError(error) {
  return isRecord(error) && error.code === "ENOENT";
}

/**
 * Calculate recent and active-runtime throughput.
 *
 * Gaps are capped at the stale threshold, so stopped periods do not dominate
 * active runtime. ETA is the remaining active processing duration at the recent
 * rate, falling back to the downtime-capped lifetime rate when needed.
 *
 * @param {object} input - Inputs for deterministic rate calculation.
 * @param {number} input.attempted - Completed checkpoint attempts.
 * @param {number} input.remaining - Rows remaining in the county denominator.
 * @param {number} input.startedAtMs - Checkpoint lineage start time.
 * @param {Float64Array | readonly number[]} input.rowTimestamps
 *   Latest result timestamp for each row, with zero for missing rows.
 * @param {number} input.nextRowIndex - Exclusive completed row limit.
 * @param {number} input.nowMs - Snapshot time.
 * @param {boolean} input.isActivelyRunning - Whether to include current active tail time.
 * @returns {DashboardStatus["throughput"]} Aggregate rates and active ETA.
 */
export function calculateThroughput({
  attempted,
  remaining,
  startedAtMs,
  rowTimestamps,
  nextRowIndex,
  nowMs,
  isActivelyRunning,
}) {
  const cutoff = nowMs - RECENT_WINDOW_MS;
  const limit = Math.min(nextRowIndex, rowTimestamps.length);
  let recentAttempted = 0;
  let activeRuntimeMs = 0;
  let previousTimestamp = startedAtMs;
  let latestTimestamp = startedAtMs;
  for (let index = 0; index < limit; index += 1) {
    const timestamp = rowTimestamps[index] ?? 0;
    if (
      !Number.isFinite(timestamp) ||
      timestamp <= 0 ||
      timestamp > nowMs + 60_000
    ) {
      continue;
    }
    if (timestamp >= cutoff) recentAttempted += 1;
    if (timestamp >= previousTimestamp) {
      activeRuntimeMs += Math.min(
        timestamp - previousTimestamp,
        ACTIVE_GAP_CAP_MS,
      );
      previousTimestamp = timestamp;
      latestTimestamp = timestamp;
    }
  }
  if (isActivelyRunning && nowMs >= latestTimestamp) {
    activeRuntimeMs += Math.min(
      nowMs - latestTimestamp,
      ACTIVE_GAP_CAP_MS,
    );
  }

  const activeRuntimeSeconds = Math.round(activeRuntimeMs / 1_000);
  const activeMinutes = activeRuntimeMs / 60_000;
  const recentPerMinute =
    recentAttempted > 0
      ? recentAttempted / (RECENT_WINDOW_MS / 60_000)
      : null;
  const activeAveragePerMinute =
    activeMinutes > 0 && attempted > 0 ? attempted / activeMinutes : null;
  const etaRate =
    recentPerMinute !== null && recentPerMinute > 0
      ? recentPerMinute
      : activeAveragePerMinute;
  const etaBasis =
    recentPerMinute !== null && recentPerMinute > 0
      ? "recent"
      : activeAveragePerMinute !== null && activeAveragePerMinute > 0
        ? "active_average"
        : null;
  const etaActiveSeconds =
    etaRate !== null && etaRate > 0 && remaining > 0
      ? Math.round((remaining / etaRate) * 60)
      : remaining === 0
        ? 0
        : null;
  return {
    windowMinutes: RECENT_WINDOW_MS / 60_000,
    recentAttempted,
    recentPerMinute:
      recentPerMinute === null ? null : round(recentPerMinute, 2),
    activeRuntimeSeconds,
    activeAveragePerMinute:
      activeAveragePerMinute === null
        ? null
        : round(activeAveragePerMinute, 2),
    etaActiveSeconds,
    etaBasis,
    projectedCompletionAt:
      etaActiveSeconds === null
        ? null
        : new Date(nowMs + etaActiveSeconds * 1_000).toISOString(),
  };
}

/**
 * Round a finite numeric aggregate to a fixed number of decimal places.
 *
 * @param {number} value - Finite value.
 * @param {number} places - Number of fractional decimal places.
 * @returns {number} Rounded aggregate.
 */
function round(value, places) {
  const scale = 10 ** places;
  return Math.round(value * scale) / scale;
}

/**
 * Convert raw property-use keys to a constrained aggregate list.
 *
 * Unexpected free-text keys are combined into "Other" so arbitrary source text
 * can never cross the API boundary.
 *
 * @param {Record<string, number>} usageTypes - Checkpoint usage counters.
 * @returns {{ type: string, count: number }[]} Sanitized sorted aggregates.
 */
function sanitizeUsageTypes(usageTypes) {
  /** @type {{ type: string, count: number }[]} */
  const aggregates = [];
  let other = 0;
  for (const [type, count] of Object.entries(usageTypes)) {
    if (/^[A-Za-z][A-Za-z0-9]{0,63}$/u.test(type)) {
      aggregates.push({ type, count });
    } else {
      other += count;
    }
  }
  if (other > 0) aggregates.push({ type: "Other", count: other });
  return aggregates.sort(
    (left, right) =>
      right.count - left.count || left.type.localeCompare(right.type),
  );
}

/**
 * Return metadata for one fixed dashboard input without reading its contents.
 *
 * @param {string} filePath - Fixed local artifact path.
 * @param {number} nowMs - Snapshot epoch milliseconds.
 * @returns {Promise<FileHealth>} Safe file-health summary.
 */
async function readFileHealth(filePath, nowMs) {
  try {
    const fileStat = await stat(filePath);
    return {
      available: true,
      sizeBytes: fileStat.size,
      modifiedAt: fileStat.mtime.toISOString(),
      ageSeconds: Math.max(0, Math.floor((nowMs - fileStat.mtimeMs) / 1_000)),
    };
  } catch (error) {
    if (!isMissingFileError(error)) throw error;
    return {
      available: false,
      sizeBytes: null,
      modifiedAt: null,
      ageSeconds: null,
    };
  }
}

/**
 * Read local filesystem capacity and fixed input-file health.
 *
 * @param {object} input - Storage paths and scanner counters.
 * @param {string} input.outputDirectory - Ingestion output directory.
 * @param {string} input.statePath - Checkpoint path.
 * @param {string} input.resultsPath - Result stream path.
 * @param {string} input.logPath - Ingestion log path.
 * @param {number} input.nowMs - Snapshot epoch milliseconds.
 * @param {number} input.parsedRows - Scanner valid-result count.
 * @param {number} input.malformedLines - Scanner invalid-line count.
 * @returns {Promise<StorageHealth>} Safe local storage summary.
 */
async function readStorageHealth({
  outputDirectory,
  statePath,
  resultsPath,
  logPath,
  nowMs,
  parsedRows,
  malformedLines,
}) {
  const [stateHealth, resultsHealth, logHealth] = await Promise.all([
    readFileHealth(statePath, nowMs),
    readFileHealth(resultsPath, nowMs),
    readFileHealth(logPath, nowMs),
  ]);
  let available = false;
  let totalBytes = null;
  let freeBytes = null;
  let usedPercent = null;
  try {
    const filesystem = await statfs(outputDirectory);
    totalBytes = filesystem.blocks * filesystem.bsize;
    freeBytes = filesystem.bavail * filesystem.bsize;
    usedPercent =
      totalBytes > 0
        ? round(((totalBytes - freeBytes) / totalBytes) * 100, 2)
        : null;
    available = true;
  } catch {
    // File health remains useful on platforms without statfs support.
  }
  return {
    available,
    totalBytes,
    freeBytes,
    usedPercent,
    files: {
      state: stateHealth,
      results: resultsHealth,
      log: logHealth,
    },
    parsedResultRows: parsedRows,
    malformedResultLines: malformedLines,
  };
}

/**
 * Locate the matching ingestion process through Linux procfs.
 *
 * This is a read-only probe. It does not signal, reprioritize, attach to, or
 * otherwise alter the active ingestion process.
 *
 * @param {string} outputDirectory - Expected ingestion output directory.
 * @returns {Promise<boolean | null>} True when found, false when absent, or null
 *   when procfs itself is unavailable.
 */
export async function probeLocalIngestionProcess(outputDirectory) {
  let entries;
  try {
    entries = await readdir("/proc", { withFileTypes: true });
  } catch {
    return null;
  }
  const expectedOutput = path.resolve(outputDirectory);
  for (const entry of entries) {
    if (!entry.isDirectory() || !/^\d+$/u.test(entry.name)) continue;
    const processRoot = path.join("/proc", entry.name);
    try {
      const [commandBuffer, workingDirectory] = await Promise.all([
        readFile(path.join(processRoot, "cmdline")),
        readlink(path.join(processRoot, "cwd")),
      ]);
      const command = commandBuffer
        .toString("utf8")
        .split("\0")
        .filter((part) => part !== "");
      if (
        !command.some((part) =>
          part.endsWith("ingest-broward-appraisal-local.mjs"),
        )
      ) {
        continue;
      }
      const outputFlagIndex = command.indexOf("--output");
      const outputArgument = command[outputFlagIndex + 1];
      if (
        outputFlagIndex >= 0 &&
        typeof outputArgument === "string" &&
        path.resolve(workingDirectory, outputArgument) === expectedOutput
      ) {
        return true;
      }
    } catch {
      // Processes can exit while procfs is scanned; continue without failing.
    }
  }
  return false;
}

/**
 * Assemble the process state from procfs and latest artifact activity.
 *
 * @param {object} input - Process freshness inputs.
 * @param {boolean | null} input.processRunning - Result of the procfs probe.
 * @param {number} input.attempted - Current attempted-row count.
 * @param {number} input.denominator - Full seed row count.
 * @param {number | null} input.activityMs - Latest fixed-input modification time.
 * @param {number} input.nowMs - Snapshot epoch milliseconds.
 * @returns {DashboardStatus["process"]} Aggregate process summary.
 */
function calculateProcessStatus({
  processRunning,
  attempted,
  denominator,
  activityMs,
  nowMs,
}) {
  const activityAgeSeconds =
    activityMs === null
      ? null
      : Math.max(0, Math.floor((nowMs - activityMs) / 1_000));
  const stale =
    activityAgeSeconds !== null &&
    activityAgeSeconds * 1_000 > STALE_AFTER_MS;
  /** @type {ProcessStatus} */
  let status = "unknown";
  if (attempted >= denominator) status = "complete";
  else if (processRunning === true) status = stale ? "stale" : "running";
  else if (processRunning === false) status = "stopped";
  return {
    status,
    running: processRunning,
    stale,
    lastActivityAt:
      activityMs === null ? null : new Date(activityMs).toISOString(),
    activityAgeSeconds,
    staleAfterSeconds: STALE_AFTER_MS / 1_000,
  };
}

/**
 * Create a reader for the live aggregate snapshot.
 *
 * @param {StatusReaderOptions} options - Fixed local paths and denominator.
 * @param {Partial<StatusReaderDependencies>} [dependencies] - Deterministic
 *   clock and process-probe overrides used by tests.
 * @returns {() => Promise<DashboardStatus>} Async aggregate snapshot reader.
 */
export function createStatusReader(options, dependencies = {}) {
  const outputDirectory = path.resolve(options.outputDirectory);
  const statePath = path.join(outputDirectory, "state.json");
  const resultsPath = path.join(outputDirectory, "results.ndjson");
  const logPath = path.resolve(options.logPath);
  const accumulator = new ResultAccumulator(
    resultsPath,
    options.denominator,
  );
  const nowProvider = dependencies.now ?? Date.now;
  const processProbe =
    dependencies.probeProcess ?? probeLocalIngestionProcess;

  return async () => {
    const nowMs = nowProvider();
    const state = parseIngestionState(await readFile(statePath, "utf8"));
    await accumulator.update(state.startedAt);
    const resultCounts = accumulator.summarize(state.nextRowIndex);
    const storage = await readStorageHealth({
      outputDirectory,
      statePath,
      resultsPath,
      logPath,
      nowMs,
      parsedRows: resultCounts.parsedRows,
      malformedLines: resultCounts.malformedLines,
    });
    const processRunning = await processProbe(outputDirectory);
    const activityTimestamps = [
      Date.parse(state.updatedAt),
      storage.files.results.modifiedAt === null
        ? Number.NaN
        : Date.parse(storage.files.results.modifiedAt),
      storage.files.log.modifiedAt === null
        ? Number.NaN
        : Date.parse(storage.files.log.modifiedAt),
    ].filter((value) => Number.isFinite(value));
    const activityMs =
      activityTimestamps.length === 0
        ? null
        : Math.max(...activityTimestamps);
    const processStatus = calculateProcessStatus({
      processRunning,
      attempted: state.attempted,
      denominator: options.denominator,
      activityMs,
      nowMs,
    });
    const remaining = Math.max(
      0,
      options.denominator - state.attempted,
    );
    const classifiedFailures =
      resultCounts.sourceMisses +
      resultCounts.sourceErrors +
      resultCounts.transformErrors;
    const checkpointAgeSeconds = Math.max(
      0,
      Math.floor((nowMs - Date.parse(state.updatedAt)) / 1_000),
    );
    return {
      schemaVersion: 1,
      generatedAt: new Date(nowMs).toISOString(),
      county: "Broward",
      denominator: options.denominator,
      process: processStatus,
      progress: {
        attempted: state.attempted,
        succeeded: state.succeeded,
        skippedExisting: state.skippedExisting,
        sourceMisses: resultCounts.sourceMisses,
        sourceErrors: resultCounts.sourceErrors,
        transformErrors: resultCounts.transformErrors,
        unclassifiedFailures: Math.max(
          0,
          state.failed - classifiedFailures,
        ),
        failedTotal: state.failed,
        remaining,
        completionPercent: round(
          Math.min(1, state.attempted / options.denominator) * 100,
          3,
        ),
      },
      throughput: calculateThroughput({
        attempted: state.attempted,
        remaining,
        startedAtMs: Date.parse(state.startedAt),
        rowTimestamps: resultCounts.rowTimestamps,
        nextRowIndex: state.nextRowIndex,
        nowMs,
        isActivelyRunning:
          processStatus.status === "running" ||
          processStatus.status === "stale",
      }),
      checkpoint: {
        lastCheckpointAt: state.updatedAt,
        ageSeconds: checkpointAgeSeconds,
      },
      usageTypes: sanitizeUsageTypes(state.usageTypes),
      storage,
    };
  };
}

/**
 * Parse the fixed local handoff manifest into its aggregate-safe contract.
 *
 * Paths are accepted only under the Broward download root, and the new output
 * must retain the query-data-only classification in its directory name.
 *
 * @param {string} text - UTF-8 handoff manifest JSON.
 * @param {string} repositoryRoot - Absolute repository root used for path checks.
 * @returns {HandoffManifest} Validated immutable handoff configuration.
 */
export function parseHandoffManifest(text, repositoryRoot) {
  /** @type {unknown} */
  const parsed = JSON.parse(text);
  if (!isRecord(parsed)) throw new Error("Handoff manifest is not an object");
  const oldCheckpoint = parsed.oldCheckpoint;
  const reconciliation = parsed.reconciliation;
  if (
    !isRecord(oldCheckpoint) ||
    !isRecord(reconciliation) ||
    !isRecord(reconciliation.excludedOldAtOrAboveBoundary)
  ) {
    throw new Error("Handoff manifest checkpoint contract is invalid");
  }
  const excluded = reconciliation.excludedOldAtOrAboveBoundary;
  const seedRowCount = readCount(parsed, "seedRowCount");
  const sourceConcurrencyMaximum = readCount(
    parsed,
    "sourceConcurrencyMaximum",
  );
  const oldBoundary = readCount(oldCheckpoint, "nextRowIndex");
  const oldAttempted = readCount(oldCheckpoint, "attempted");
  const newInitialRowIndex = readCount(parsed, "newInitialRowIndex");
  const oldOutputDirectory = parsed.oldOutputDirectory;
  const newOutputDirectory = parsed.newOutputDirectory;
  const newLogPath = parsed.newLogPath;
  const expectedOldOutput = path.resolve(
    repositoryRoot,
    DEFAULT_OUTPUT_DIRECTORY,
  );
  const allowedRoot = path.resolve(repositoryRoot, "downloads/broward");
  if (
    parsed.sourceMode !== "live-bcpa" ||
    parsed.newArtifactMode !== "query-data-only" ||
    sourceConcurrencyMaximum < 1 ||
    sourceConcurrencyMaximum > 4 ||
    oldBoundary !== oldAttempted ||
    oldBoundary !== newInitialRowIndex ||
    oldBoundary > seedRowCount ||
    typeof oldOutputDirectory !== "string" ||
    path.resolve(repositoryRoot, oldOutputDirectory) !== expectedOldOutput ||
    typeof newOutputDirectory !== "string" ||
    !path
      .basename(path.resolve(repositoryRoot, newOutputDirectory))
      .toLowerCase()
      .includes("query-data-only") ||
    path.dirname(path.resolve(repositoryRoot, newOutputDirectory)) !==
      allowedRoot ||
    typeof newLogPath !== "string" ||
    path.dirname(path.resolve(repositoryRoot, newLogPath)) !==
      path.resolve(repositoryRoot, newOutputDirectory) ||
    path.basename(newLogPath) !== "ingestion.log"
  ) {
    throw new Error("Handoff manifest migration contract is invalid");
  }
  for (const key of [
    "resultRowIndexes",
    "artifactRowIndexes",
    "captureRowIndexes",
  ]) {
    const values = excluded[key];
    if (
      !Array.isArray(values) ||
      values.some(
        (value) =>
          typeof value !== "number" ||
          !Number.isInteger(value) ||
          value < oldBoundary ||
          value >= seedRowCount,
      )
    ) {
      throw new Error(`Handoff manifest ${key} is invalid`);
    }
  }
  return /** @type {HandoffManifest} */ (parsed);
}

/**
 * Merge sanitized usage-type aggregates from two non-overlapping segments.
 *
 * @param {readonly { type: string, count: number }[]} publishableTypes
 *   Sanitized pre-boundary usage counts.
 * @param {readonly { type: string, count: number }[]} dataOnlyTypes
 *   Sanitized post-boundary usage counts.
 * @returns {{ type: string, count: number }[]} Combined sorted aggregates.
 */
function combineUsageTypes(publishableTypes, dataOnlyTypes) {
  /** @type {Record<string, number>} */
  const combined = {};
  for (const entry of [...publishableTypes, ...dataOnlyTypes]) {
    combined[entry.type] = (combined[entry.type] ?? 0) + entry.count;
  }
  return sanitizeUsageTypes(combined);
}

/**
 * Combine two non-overlapping status snapshots at one immutable row boundary.
 *
 * @param {DashboardStatus} publishable - Frozen pre-boundary status.
 * @param {DashboardStatus} dataOnly - Active post-boundary status.
 * @param {HandoffManifest} manifest - Immutable segment and exclusion contract.
 * @param {number} nowMs - Snapshot epoch milliseconds.
 * @returns {DashboardStatus} Full-county aggregate status.
 */
export function combineHandoffStatuses(
  publishable,
  dataOnly,
  manifest,
  nowMs,
) {
  const boundary = manifest.newInitialRowIndex;
  if (
    publishable.progress.attempted !== boundary ||
    manifest.oldCheckpoint.nextRowIndex !== boundary
  ) {
    throw new Error("Publishable checkpoint moved after handoff");
  }
  const attempted = boundary + dataOnly.progress.attempted;
  const remaining = Math.max(0, manifest.seedRowCount - attempted);
  const recentAttempted =
    publishable.throughput.recentAttempted +
    dataOnly.throughput.recentAttempted;
  const windowMinutes = dataOnly.throughput.windowMinutes;
  const recentPerMinute =
    windowMinutes > 0 ? recentAttempted / windowMinutes : null;
  const activeRuntimeSeconds =
    publishable.throughput.activeRuntimeSeconds +
    dataOnly.throughput.activeRuntimeSeconds;
  const activeAveragePerMinute =
    activeRuntimeSeconds > 0
      ? attempted / (activeRuntimeSeconds / 60)
      : null;
  const etaRate =
    recentPerMinute !== null && recentPerMinute > 0
      ? recentPerMinute
      : activeAveragePerMinute;
  const etaActiveSeconds =
    etaRate !== null && etaRate > 0 && remaining > 0
      ? Math.round((remaining / etaRate) * 60)
      : remaining === 0
        ? 0
        : null;
  const excluded = manifest.reconciliation.excludedOldAtOrAboveBoundary;
  return {
    schemaVersion: 1,
    generatedAt: new Date(nowMs).toISOString(),
    county: "Broward",
    denominator: manifest.seedRowCount,
    process: dataOnly.process,
    progress: {
      attempted,
      succeeded:
        publishable.progress.succeeded + dataOnly.progress.succeeded,
      skippedExisting:
        publishable.progress.skippedExisting +
        dataOnly.progress.skippedExisting,
      sourceMisses:
        publishable.progress.sourceMisses + dataOnly.progress.sourceMisses,
      sourceErrors:
        publishable.progress.sourceErrors + dataOnly.progress.sourceErrors,
      transformErrors:
        publishable.progress.transformErrors +
        dataOnly.progress.transformErrors,
      unclassifiedFailures:
        publishable.progress.unclassifiedFailures +
        dataOnly.progress.unclassifiedFailures,
      failedTotal:
        publishable.progress.failedTotal + dataOnly.progress.failedTotal,
      remaining,
      completionPercent: round(
        Math.min(1, attempted / manifest.seedRowCount) * 100,
        3,
      ),
    },
    throughput: {
      windowMinutes,
      recentAttempted,
      recentPerMinute:
        recentPerMinute === null ? null : round(recentPerMinute, 2),
      activeRuntimeSeconds,
      activeAveragePerMinute:
        activeAveragePerMinute === null
          ? null
          : round(activeAveragePerMinute, 2),
      etaActiveSeconds,
      etaBasis:
        recentPerMinute !== null && recentPerMinute > 0
          ? "recent"
          : activeAveragePerMinute !== null && activeAveragePerMinute > 0
            ? "active_average"
            : null,
      projectedCompletionAt:
        etaActiveSeconds === null
          ? null
          : new Date(nowMs + etaActiveSeconds * 1_000).toISOString(),
    },
    checkpoint: dataOnly.checkpoint,
    usageTypes: combineUsageTypes(
      publishable.usageTypes,
      dataOnly.usageTypes,
    ),
    storage: {
      ...dataOnly.storage,
      parsedResultRows:
        publishable.storage.parsedResultRows +
        dataOnly.storage.parsedResultRows,
      malformedResultLines:
        publishable.storage.malformedResultLines +
        dataOnly.storage.malformedResultLines,
    },
    handoff: {
      active: true,
      boundaryRowIndex: boundary,
      publishableAttempted: publishable.progress.attempted,
      publishableSucceeded: publishable.progress.succeeded,
      dataOnlyAttempted: dataOnly.progress.attempted,
      dataOnlySucceeded: dataOnly.progress.succeeded,
      dataOnlyTransformErrors: dataOnly.progress.transformErrors,
      excludedOldResultRows: excluded.resultRowIndexes.length,
      excludedOldArtifacts: excluded.artifactRowIndexes.length,
      preservedExcludedOldCaptures: excluded.captureRowIndexes.length,
    },
  };
}

/**
 * Build the default reader, automatically activating the fixed local handoff
 * manifest when present and otherwise retaining the original single-run view.
 *
 * @param {string} repositoryRoot - Absolute repository root.
 * @returns {Promise<() => Promise<DashboardStatus>>} Aggregate status reader.
 */
export async function createDefaultStatusReader(repositoryRoot) {
  const publishableReader = createStatusReader({
    outputDirectory: path.resolve(repositoryRoot, DEFAULT_OUTPUT_DIRECTORY),
    logPath: path.resolve(repositoryRoot, DEFAULT_LOG_PATH),
    denominator: BROWARD_ROW_DENOMINATOR,
  });
  const manifestPath = path.resolve(
    repositoryRoot,
    DEFAULT_HANDOFF_MANIFEST_PATH,
  );
  let manifestText;
  try {
    manifestText = await readFile(manifestPath, "utf8");
  } catch (error) {
    if (isMissingFileError(error)) return publishableReader;
    throw error;
  }
  const manifest = parseHandoffManifest(manifestText, repositoryRoot);
  const dataOnlyReader = createStatusReader({
    outputDirectory: path.resolve(
      repositoryRoot,
      manifest.newOutputDirectory,
    ),
    logPath: path.resolve(repositoryRoot, manifest.newLogPath),
    denominator: manifest.seedRowCount,
  });
  return async () => {
    const [publishable, dataOnly] = await Promise.all([
      publishableReader(),
      dataOnlyReader(),
    ]);
    return combineHandoffStatuses(
      publishable,
      dataOnly,
      manifest,
      Date.now(),
    );
  };
}

/**
 * Write a JSON response with browser and intermediary caching disabled.
 *
 * @param {import("node:http").ServerResponse} response - HTTP response.
 * @param {number} statusCode - HTTP status.
 * @param {Record<string, unknown>} payload - Aggregate-safe JSON object.
 * @returns {void}
 */
function writeJson(response, statusCode, payload) {
  const body = `${JSON.stringify(payload)}\n`;
  response.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body),
    "cache-control": "no-store",
    "x-content-type-options": "nosniff",
  });
  response.end(body);
}

/**
 * Create the HTTP dashboard server.
 *
 * @param {() => Promise<DashboardStatus>} readStatus - Aggregate snapshot reader.
 * @returns {import("node:http").Server} Unstarted Node HTTP server.
 */
export function createDashboardServer(readStatus) {
  return createServer((request, response) => {
    void (async () => {
      const requestUrl = new URL(
        request.url ?? "/",
        "http://dashboard.local",
      );
      if (request.method !== "GET" && request.method !== "HEAD") {
        writeJson(response, 405, { error: "Method not allowed" });
        return;
      }
      if (requestUrl.pathname === "/api/status") {
        try {
          const status = await readStatus();
          writeJson(
            response,
            200,
            /** @type {Record<string, unknown>} */ (status),
          );
        } catch {
          writeJson(response, 503, {
            error: "Aggregate status is temporarily unavailable",
          });
        }
        return;
      }
      if (requestUrl.pathname === "/healthz") {
        writeJson(response, 200, {
          ok: true,
          service: "broward-ingestion-dashboard",
        });
        return;
      }
      if (requestUrl.pathname === "/") {
        response.writeHead(200, {
          "content-type": "text/html; charset=utf-8",
          "content-length": Buffer.byteLength(DASHBOARD_HTML),
          "cache-control": "no-store",
          "content-security-policy":
            "default-src 'none'; connect-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'",
          "referrer-policy": "no-referrer",
          "x-content-type-options": "nosniff",
          "x-frame-options": "DENY",
        });
        response.end(request.method === "HEAD" ? "" : DASHBOARD_HTML);
        return;
      }
      writeJson(response, 404, { error: "Not found" });
    })();
  });
}

const DASHBOARD_HTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Broward ingestion dashboard</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #07111f;
      --panel: #0f1d2e;
      --panel-alt: #14263a;
      --text: #f2f7fb;
      --muted: #aab9c9;
      --border: #2a4057;
      --accent: #59c3ff;
      --good: #63dfa3;
      --warn: #ffd166;
      --bad: #ff7b8b;
      --radius: 0.85rem;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: radial-gradient(circle at top right, #102b42, var(--bg) 42rem);
      color: var(--text);
      font: 1rem/1.5 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    main { width: min(90rem, 100%); margin: 0 auto; padding: 1.25rem; }
    header {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 1rem;
      margin-bottom: 1.25rem;
    }
    h1, h2, p { margin-top: 0; }
    h1 { margin-bottom: 0.25rem; font-size: clamp(1.65rem, 4vw, 2.5rem); }
    h2 { font-size: 1.05rem; letter-spacing: 0.02em; }
    .muted, .detail { color: var(--muted); }
    .detail { font-size: 0.85rem; margin: 0.25rem 0 0; }
    .status {
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      min-width: 9rem;
      padding: 0.6rem 0.85rem;
      border: 1px solid var(--border);
      border-radius: 999px;
      background: var(--panel);
      font-weight: 700;
      text-transform: capitalize;
    }
    .status::before {
      width: 0.7rem;
      height: 0.7rem;
      border-radius: 50%;
      background: var(--muted);
      content: "";
    }
    .status.running::before, .status.complete::before { background: var(--good); }
    .status.stale::before { background: var(--warn); }
    .status.stopped::before { background: var(--bad); }
    .panel {
      padding: 1rem;
      border: 1px solid var(--border);
      border-radius: var(--radius);
      background: color-mix(in srgb, var(--panel) 94%, transparent);
      box-shadow: 0 0.8rem 2rem rgb(0 0 0 / 0.18);
    }
    .progress-panel { margin-bottom: 1rem; }
    .progress-line {
      display: flex;
      justify-content: space-between;
      gap: 1rem;
      align-items: baseline;
      margin-bottom: 0.5rem;
    }
    progress {
      width: 100%;
      height: 1rem;
      border: 0;
      border-radius: 1rem;
      overflow: hidden;
      background: var(--panel-alt);
    }
    progress::-webkit-progress-bar { background: var(--panel-alt); }
    progress::-webkit-progress-value { background: linear-gradient(90deg, #2f8cff, var(--accent)); }
    progress::-moz-progress-bar { background: linear-gradient(90deg, #2f8cff, var(--accent)); }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(min(100%, 13rem), 1fr));
      gap: 1rem;
      margin-bottom: 1rem;
    }
    .metric { min-height: 8rem; }
    .metric-value {
      margin: 0;
      font-size: clamp(1.55rem, 4vw, 2.1rem);
      font-variant-numeric: tabular-nums;
      font-weight: 750;
    }
    .good { color: var(--good); }
    .warn { color: var(--warn); }
    .bad { color: var(--bad); }
    .lower-grid {
      display: grid;
      grid-template-columns: minmax(0, 1.5fr) minmax(18rem, 0.7fr);
      gap: 1rem;
    }
    .usage-list, .storage-list { list-style: none; padding: 0; margin: 0; }
    .usage-item {
      display: grid;
      grid-template-columns: minmax(8rem, 1fr) 3fr auto;
      align-items: center;
      gap: 0.75rem;
      padding: 0.35rem 0;
    }
    .bar { height: 0.55rem; overflow: hidden; border-radius: 1rem; background: var(--panel-alt); }
    .bar > span { display: block; height: 100%; background: var(--accent); border-radius: inherit; }
    .count { font-variant-numeric: tabular-nums; }
    .storage-list li {
      display: flex;
      justify-content: space-between;
      gap: 1rem;
      padding: 0.5rem 0;
      border-bottom: 1px solid var(--border);
    }
    .storage-list li:last-child { border-bottom: 0; }
    .notice {
      padding: 0.75rem;
      border-left: 0.25rem solid var(--accent);
      background: var(--panel-alt);
      color: var(--muted);
      font-size: 0.9rem;
    }
    #error { color: var(--bad); min-height: 1.5rem; margin-top: 0.75rem; }
    footer { margin-top: 1rem; color: var(--muted); font-size: 0.85rem; }
    @media (max-width: 760px) {
      main { padding: 0.85rem; }
      .lower-grid { grid-template-columns: 1fr; }
      .usage-item { grid-template-columns: minmax(7rem, 1fr) 1.5fr auto; }
    }
    @media (prefers-reduced-motion: reduce) {
      *, *::before, *::after { scroll-behavior: auto !important; }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>Broward ingestion</h1>
        <p class="muted">Local, aggregate-only live progress</p>
      </div>
      <div id="process-status" class="status unknown" role="status" aria-live="polite">Loading</div>
    </header>

    <section class="panel progress-panel" aria-labelledby="progress-heading">
      <div class="progress-line">
        <h2 id="progress-heading">County completion</h2>
        <strong id="completion">—</strong>
      </div>
      <progress id="progress" max="100" value="0" aria-label="Broward ingestion completion"></progress>
      <p id="attempted-summary" class="detail">Loading checkpoint…</p>
    </section>

    <section class="grid" aria-label="Ingestion metrics">
      <article class="panel metric">
        <h2>Attempted</h2>
        <p id="attempted" class="metric-value">—</p>
        <p class="detail">Seed rows checkpointed</p>
      </article>
      <article class="panel metric">
        <h2>Succeeded</h2>
        <p id="succeeded" class="metric-value good">—</p>
        <p class="detail">Transformed artifacts written</p>
      </article>
      <article class="panel metric">
        <h2>Expected source misses</h2>
        <p id="source-misses" class="metric-value warn">—</p>
        <p class="detail">BCPA returned no usable parcel after retry; no transform ran</p>
      </article>
      <article class="panel metric">
        <h2>Other source errors</h2>
        <p id="source-errors" class="metric-value bad">—</p>
        <p class="detail">Fetch, HTTP, validation, or unclassified source failures</p>
      </article>
      <article class="panel metric">
        <h2>Transform errors</h2>
        <p id="transform-errors" class="metric-value bad">—</p>
        <p class="detail">Source was captured, but the county transform failed</p>
      </article>
      <article class="panel metric">
        <h2>Recent throughput</h2>
        <p id="throughput" class="metric-value">—</p>
        <p id="throughput-detail" class="detail">15-minute attempt rate</p>
      </article>
      <article class="panel metric">
        <h2>Active-runtime ETA</h2>
        <p id="eta" class="metric-value">—</p>
        <p id="eta-detail" class="detail">Excludes long inactive gaps</p>
      </article>
      <article class="panel metric">
        <h2>Last checkpoint</h2>
        <p id="checkpoint-age" class="metric-value">—</p>
        <p id="checkpoint-time" class="detail">—</p>
      </article>
    </section>

    <div class="lower-grid">
      <section class="panel" aria-labelledby="usage-heading">
        <h2 id="usage-heading">Property usage aggregates</h2>
        <ol id="usage-types" class="usage-list">
          <li class="muted">Loading…</li>
        </ol>
      </section>
      <section class="panel" aria-labelledby="storage-heading">
        <h2 id="storage-heading">Local storage health</h2>
        <ul id="storage" class="storage-list">
          <li><span>Loading…</span></li>
        </ul>
        <p class="notice">Only counts, timing, process state, and file/storage metadata leave the server. Parcel, owner, contact, address, error, and log contents remain local.</p>
      </section>
    </div>
    <p id="error" role="alert"></p>
    <footer>Refreshes every 5 seconds. <span id="generated-at"></span></footer>
  </main>
  <script>
    "use strict";

    const numberFormatter = new Intl.NumberFormat();
    const rateFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 });

    /**
     * Set one element's safe plain-text content.
     *
     * @param {string} id - DOM element id.
     * @param {string} value - User-visible aggregate text.
     * @returns {void}
     */
    function setText(id, value) {
      const element = document.getElementById(id);
      if (element !== null) element.textContent = value;
    }

    /**
     * Render a count with locale separators.
     *
     * @param {number} value - Aggregate integer.
     * @returns {string} Human-readable count.
     */
    function formatCount(value) {
      return numberFormatter.format(value);
    }

    /**
     * Render an ISO timestamp in the browser's local timezone.
     *
     * @param {string | null} value - ISO timestamp.
     * @returns {string} Local timestamp or unavailable marker.
     */
    function formatTime(value) {
      return value === null ? "Unavailable" : new Date(value).toLocaleString();
    }

    /**
     * Render a compact duration from whole seconds.
     *
     * @param {number | null} seconds - Duration in seconds.
     * @returns {string} Compact duration.
     */
    function formatDuration(seconds) {
      if (seconds === null) return "Unavailable";
      if (seconds < 60) return Math.max(0, Math.round(seconds)) + "s";
      const minutes = Math.round(seconds / 60);
      if (minutes < 60) return minutes + "m";
      const hours = Math.round(minutes / 60);
      if (hours < 48) return hours + "h";
      return Math.round(hours / 24) + "d";
    }

    /**
     * Render bytes with binary units.
     *
     * @param {number | null} bytes - Byte count.
     * @returns {string} Human-readable storage value.
     */
    function formatBytes(bytes) {
      if (bytes === null) return "Unavailable";
      const units = ["B", "KiB", "MiB", "GiB", "TiB"];
      let value = bytes;
      let unitIndex = 0;
      while (value >= 1024 && unitIndex < units.length - 1) {
        value /= 1024;
        unitIndex += 1;
      }
      return value.toFixed(unitIndex === 0 ? 0 : 1) + " " + units[unitIndex];
    }

    /**
     * Append a safe text-only storage summary row.
     *
     * @param {HTMLUListElement} list - Storage list.
     * @param {string} label - Fixed aggregate label.
     * @param {string} value - Safe aggregate value.
     * @returns {void}
     */
    function appendStorageRow(list, label, value) {
      const item = document.createElement("li");
      const labelNode = document.createElement("span");
      const valueNode = document.createElement("strong");
      labelNode.textContent = label;
      valueNode.textContent = value;
      item.append(labelNode, valueNode);
      list.append(item);
    }

    /**
     * Render all aggregate status sections.
     *
     * @param {Record<string, unknown>} status - Aggregate endpoint payload.
     * @returns {void}
     */
    function renderStatus(status) {
      const progressData = /** @type {Record<string, number>} */ (status.progress);
      const throughputData = /** @type {Record<string, number | string | null>} */ (status.throughput);
      const checkpointData = /** @type {Record<string, number | string>} */ (status.checkpoint);
      const processData = /** @type {Record<string, string | number | boolean | null>} */ (status.process);
      const storageData = /** @type {Record<string, unknown>} */ (status.storage);
      const completion = progressData.completionPercent;

      setText("completion", completion.toFixed(3) + "%");
      const progressElement = document.getElementById("progress");
      if (progressElement instanceof HTMLProgressElement) progressElement.value = completion;
      setText("attempted-summary", formatCount(progressData.attempted) + " of " + formatCount(/** @type {number} */ (status.denominator)) + " rows attempted");
      setText("attempted", formatCount(progressData.attempted));
      setText("succeeded", formatCount(progressData.succeeded));
      setText("source-misses", formatCount(progressData.sourceMisses));
      setText("source-errors", formatCount(progressData.sourceErrors + progressData.unclassifiedFailures));
      setText("transform-errors", formatCount(progressData.transformErrors));
      setText("throughput", throughputData.recentPerMinute === null ? "Unavailable" : rateFormatter.format(/** @type {number} */ (throughputData.recentPerMinute)) + "/min");
      setText("throughput-detail", formatCount(/** @type {number} */ (throughputData.recentAttempted)) + " attempts in " + String(throughputData.windowMinutes) + " minutes");
      setText("eta", formatDuration(/** @type {number | null} */ (throughputData.etaActiveSeconds)));
      setText("eta-detail", throughputData.projectedCompletionAt === null ? "No rate available" : "Continuous-run projection: " + formatTime(/** @type {string} */ (throughputData.projectedCompletionAt)));
      setText("checkpoint-age", formatDuration(/** @type {number} */ (checkpointData.ageSeconds)) + " ago");
      setText("checkpoint-time", formatTime(/** @type {string} */ (checkpointData.lastCheckpointAt)));
      setText("generated-at", "Snapshot " + formatTime(/** @type {string} */ (status.generatedAt)));

      const statusElement = document.getElementById("process-status");
      if (statusElement !== null) {
        const processStatus = String(processData.status);
        statusElement.textContent = processStatus;
        statusElement.className = "status " + processStatus;
      }

      const usageList = document.getElementById("usage-types");
      if (usageList instanceof HTMLOListElement) {
        usageList.replaceChildren();
        const usages = /** @type {{ type: string, count: number }[]} */ (status.usageTypes);
        const maximum = usages.length === 0 ? 1 : Math.max(...usages.map((usage) => usage.count));
        for (const usage of usages) {
          const item = document.createElement("li");
          item.className = "usage-item";
          const label = document.createElement("span");
          const bar = document.createElement("span");
          const fill = document.createElement("span");
          const count = document.createElement("strong");
          label.textContent = usage.type.replace(/([a-z])([A-Z])/g, "$1 $2");
          bar.className = "bar";
          fill.style.width = ((usage.count / maximum) * 100).toFixed(2) + "%";
          bar.append(fill);
          count.className = "count";
          count.textContent = formatCount(usage.count);
          item.append(label, bar, count);
          usageList.append(item);
        }
      }

      const storageList = document.getElementById("storage");
      if (storageList instanceof HTMLUListElement) {
        storageList.replaceChildren();
        const files = /** @type {Record<string, { available: boolean, sizeBytes: number | null, ageSeconds: number | null }>} */ (storageData.files);
        appendStorageRow(storageList, "Filesystem free", formatBytes(/** @type {number | null} */ (storageData.freeBytes)));
        appendStorageRow(storageList, "Filesystem used", storageData.usedPercent === null ? "Unavailable" : String(storageData.usedPercent) + "%");
        for (const name of ["state", "results", "log"]) {
          const file = files[name];
          appendStorageRow(storageList, name + " file", file.available ? formatBytes(file.sizeBytes) + " · " + formatDuration(file.ageSeconds) + " ago" : "Unavailable");
        }
        appendStorageRow(storageList, "Malformed result lines", formatCount(/** @type {number} */ (storageData.malformedResultLines)));
      }
    }

    /**
     * Fetch and render one no-store aggregate snapshot.
     *
     * @returns {Promise<void>} Resolves after the UI is updated.
     */
    async function refresh() {
      try {
        const response = await fetch("/api/status", { cache: "no-store" });
        if (!response.ok) throw new Error("Status endpoint returned " + response.status);
        const status = /** @type {Record<string, unknown>} */ (await response.json());
        renderStatus(status);
        setText("error", "");
      } catch {
        setText("error", "Live status is temporarily unavailable; retrying automatically.");
      }
    }

    void refresh();
    window.setInterval(() => void refresh(), 5_000);
  </script>
</body>
</html>
`;

/**
 * Start the CLI server against the fixed Broward local-ingestion inputs.
 *
 * @param {DashboardCliOptions} options - Validated host and port.
 * @returns {void}
 */
async function runCli(options) {
  if (options.help) {
    process.stdout.write(
      [
        "Usage: node scripts/broward-ingestion-dashboard.mjs [options]",
        "",
        `  --host <host>  Listen interface (default: ${DEFAULT_DASHBOARD_HOST})`,
        `  --port <port>  Unprivileged port (default: ${String(DEFAULT_DASHBOARD_PORT)})`,
        "  --help         Show this help",
        "",
      ].join("\n"),
    );
    return;
  }
  const readStatus = await createDefaultStatusReader(process.cwd());
  const server = createDashboardServer(readStatus);
  server.listen(options.port, options.host, () => {
    process.stdout.write(
      `${JSON.stringify({
        level: "info",
        message: "broward_ingestion_dashboard_listening",
        host: options.host,
        port: options.port,
      })}\n`,
    );
  });
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  runCli(parseDashboardCliOptions(process.argv.slice(2))).catch((error) => {
    process.stderr.write(
      `${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  });
}
