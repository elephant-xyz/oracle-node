#!/usr/bin/env node
// @ts-check

/**
 * Capture official Accela "Download results" CSVs by date window.
 *
 * Several Broward Accela grids cap or misreport pagination even for one day.
 * Their built-in CSV export returns the full source list for the submitted
 * date window. This runner preserves each export, checkpoints per window, and
 * builds a deterministic detail-compatible permit inventory.
 */

import { createHash } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import {
  captureBrowardAccelaCsvWindow,
  createBrowardAccelaBrowser,
  readBrowardAccelaSource,
} from "./permit-source-adapters/broward-accela.mjs";

const CHECKPOINT_SCHEMA_VERSION = "oracle-node.broward-accela-csv-windows.v1";
const SOURCE_KEYS = new Set([
  "hollywood",
  "plantation",
  "cooper-city",
  "weston",
]);

/**
 * @typedef {"hollywood" | "plantation" | "cooper-city" | "weston"} AccelaCsvSourceKey
 *
 * @typedef {object} DateWindow
 * @property {string} startDate - Inclusive ISO start.
 * @property {string} endDate - Inclusive ISO end.
 *
 * @typedef {object} AccelaCsvWindowOptions
 * @property {AccelaCsvSourceKey} sourceKey - Date-enabled Accela source.
 * @property {string} startDate - Inclusive range start.
 * @property {string} endDate - Inclusive range end.
 * @property {number} windowDays - Source export window width.
 * @property {number} delayMs - Delay between exports.
 * @property {number} maxAttempts - Transient attempts per source window.
 * @property {number | null} maxWindows - Optional pilot bound.
 * @property {string} outputDirectory - Private artifact root.
 *
 * @typedef {object} AccelaCsvWindowReceipt
 * @property {string} startDate - Inclusive start.
 * @property {string} endDate - Inclusive end.
 * @property {number | null} displayedTotal - Untrusted UI total.
 * @property {boolean} displayedTotalCapped - Whether UI displayed cap 100.
 * @property {number} exportedRecordCount - Official CSV rows.
 * @property {number | undefined} [excludedNonPermitCount] - Explicit non-permit source rows; absent in pre-v2 receipts means zero.
 * @property {string} recordsPath - Private normalized window artifact.
 * @property {string} rawCsvSha256 - Exact export hash.
 * @property {string} completedAt - ISO completion timestamp.
 *
 * @typedef {object} AccelaCsvCheckpoint
 * @property {typeof CHECKPOINT_SCHEMA_VERSION} schemaVersion - Schema marker.
 * @property {AccelaCsvSourceKey} sourceKey - Source identity.
 * @property {string} configurationSha256 - Immutable run hash.
 * @property {DateWindow[]} pendingWindows - Remaining windows.
 * @property {Record<string, AccelaCsvWindowReceipt>} completedWindows - Receipts.
 * @property {string} startedAt - ISO first start.
 * @property {string} updatedAt - ISO latest update.
 *
 * @typedef {import("./permit-source-adapters/broward-accela.mjs").BrowardAccelaCsvPermitRecord} BrowardAccelaCsvPermitRecord
 *
 * @typedef {object} AccelaCsvRunSummary
 * @property {"paused" | "complete"} status - Run state.
 * @property {string} sourceKey - Source key.
 * @property {string} sourceSystem - Source-system key.
 * @property {number} windowsProcessedThisInvocation - New completed windows.
 * @property {number} completedWindowCount - Durable window count.
 * @property {number} pendingWindowCount - Remaining window count.
 * @property {number} exportedRecordObservations - Sum of window rows.
 * @property {number} uniquePermitCount - Unique permit-number identities.
 * @property {number} duplicatePermitObservations - Cross-window duplicates.
 * @property {number} cappedDisplayedTotalWindowCount - Windows whose UI showed 100.
 * @property {number} excludedNonPermitCount - Whole-run explicit exclusions.
 * @property {string} normalizedListPath - Final private JSONL.
 * @property {string} checkpointPath - Private checkpoint.
 * @property {string} completedAt - ISO summary timestamp.
 */

/**
 * Parse an explicit CSV-export run.
 *
 * @param {readonly string[]} argv - CLI arguments.
 * @returns {AccelaCsvWindowOptions} Validated options.
 */
export function parseAccelaCsvWindowOptions(argv) {
  const values = new Map();
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (
      typeof flag !== "string" ||
      !flag.startsWith("--") ||
      typeof value !== "string" ||
      value.startsWith("--")
    ) {
      throw new Error("Accela CSV options must be --flag value pairs");
    }
    values.set(flag.slice(2), value);
  }
  const rawSource = values.get("source");
  if (typeof rawSource !== "string" || !SOURCE_KEYS.has(rawSource)) {
    throw new Error(
      "--source must be hollywood, plantation, cooper-city, or weston",
    );
  }
  const sourceKey = /** @type {AccelaCsvSourceKey} */ (rawSource);
  const startDate = requireIsoDate(values.get("start-date"), "--start-date");
  const endDate = requireIsoDate(values.get("end-date"), "--end-date");
  if (toMillis(endDate) < toMillis(startDate)) {
    throw new Error("--end-date must not precede --start-date");
  }
  const rawMaxWindows = values.get("max-windows");
  const outputDirectory =
    values.get("output-dir") ??
    `downloads/broward/accela-csv-windows/${sourceKey}`;
  return {
    sourceKey,
    startDate,
    endDate,
    windowDays: boundedInteger(
      values.get("window-days") ?? "30",
      "window-days",
      1,
      366,
    ),
    delayMs: boundedInteger(
      values.get("delay-ms") ?? "1000",
      "delay-ms",
      1_000,
      60_000,
    ),
    maxAttempts: boundedInteger(
      values.get("max-attempts") ?? "3",
      "max-attempts",
      1,
      5,
    ),
    maxWindows:
      rawMaxWindows === undefined
        ? null
        : boundedInteger(rawMaxWindows, "max-windows", 1, 1_000_000),
    outputDirectory,
  };
}

/**
 * Create exhaustive adjacent date windows.
 *
 * @param {string} startDate - Inclusive start.
 * @param {string} endDate - Inclusive end.
 * @param {number} windowDays - Maximum width.
 * @returns {DateWindow[]} Chronological windows.
 */
export function createAccelaCsvDateWindows(startDate, endDate, windowDays) {
  /** @type {DateWindow[]} */
  const windows = [];
  let cursor = startDate;
  while (toMillis(cursor) <= toMillis(endDate)) {
    const candidateEnd = addDays(cursor, windowDays - 1);
    const actualEnd =
      toMillis(candidateEnd) > toMillis(endDate) ? endDate : candidateEnd;
    windows.push({ startDate: cursor, endDate: actualEnd });
    cursor = addDays(actualEnd, 1);
  }
  return windows;
}

/**
 * Run or resume one persistent CSV-export source.
 *
 * @param {AccelaCsvWindowOptions} options - Validated options.
 * @param {{
 *   now?:()=>string,
 *   wait?:(milliseconds:number)=>Promise<void>,
 *   createBrowser?:typeof createBrowardAccelaBrowser,
 *   captureWindow?:typeof captureBrowardAccelaCsvWindow
 * }} [dependencies={}] - Injectable runtime dependencies.
 * @returns {Promise<AccelaCsvRunSummary>} Reconciled run summary.
 */
export async function runAccelaCsvWindows(options, dependencies = {}) {
  const now = dependencies.now ?? (() => new Date().toISOString());
  const wait =
    dependencies.wait ??
    ((milliseconds) =>
      new Promise((resolvePromise) => {
        setTimeout(resolvePromise, milliseconds);
      }));
  const createBrowser =
    dependencies.createBrowser ?? createBrowardAccelaBrowser;
  const captureWindow =
    dependencies.captureWindow ?? captureBrowardAccelaCsvWindow;
  const source = readBrowardAccelaSource(options.sourceKey);
  const outputDirectory = path.resolve(options.outputDirectory);
  const windowsDirectory = path.join(outputDirectory, "windows-private");
  const checkpointPath = path.join(outputDirectory, "checkpoint.private.json");
  const normalizedListPath = path.join(
    outputDirectory,
    "normalized-list.private.jsonl",
  );
  const summaryPath = path.join(outputDirectory, "summary.private.json");
  await Promise.all(
    [outputDirectory, windowsDirectory].map((directory) =>
      mkdir(directory, { recursive: true, mode: 0o700 }),
    ),
  );
  let checkpoint = await readOrCreateCheckpoint(checkpointPath, options, now());
  const logger = {
    info: (
      /** @type {string} */ message,
      /** @type {Record<string, unknown>} */ details = {},
    ) => console.log(JSON.stringify({ level: "info", message, ...details })),
    warn: (
      /** @type {string} */ message,
      /** @type {Record<string, unknown>} */ details = {},
    ) => console.warn(JSON.stringify({ level: "warn", message, ...details })),
    error: (
      /** @type {string} */ message,
      /** @type {Record<string, unknown>} */ details = {},
    ) => console.error(JSON.stringify({ level: "error", message, ...details })),
  };
  let browser = await createBrowser(logger);
  let processed = 0;
  try {
    while (
      checkpoint.pendingWindows.length > 0 &&
      (options.maxWindows === null || processed < options.maxWindows)
    ) {
      const window = checkpoint.pendingWindows[0];
      if (window === undefined) break;
      if (processed > 0) await wait(options.delayMs);
      const windowKey = localWindowKey(window);
      const windowDirectory = path.join(windowsDirectory, windowKey);
      let capture;
      for (let attempt = 1; attempt <= options.maxAttempts; attempt += 1) {
        try {
          capture = await captureWindow({
            browser,
            source,
            startDate: window.startDate,
            endDate: window.endDate,
            downloadDirectory: windowDirectory,
            logger,
          });
          break;
        } catch (error) {
          if (attempt >= options.maxAttempts) throw error;
          logger.warn("broward_accela_csv_window_retry", {
            sourceKey: source.key,
            startDate: window.startDate,
            endDate: window.endDate,
            attempt,
            error: error instanceof Error ? error.message : "Unknown error",
          });
          await browser.close().catch(() => undefined);
          await wait(Math.max(options.delayMs * attempt, 5_000));
          browser = await createBrowser(logger);
        }
      }
      if (capture === undefined) {
        throw new Error("Accela CSV capture exhausted without a result");
      }
      const searchHtmlPath = path.join(windowDirectory, "search.html");
      await writePrivateAtomic(searchHtmlPath, capture.rawSearchHtml);
      const recordsPath = path.join(windowDirectory, "records.private.json");
      await writePrivateAtomic(
        recordsPath,
        `${JSON.stringify(
          {
            schemaVersion: "oracle-node.broward-accela-csv-window.v1",
            sourceKey: source.key,
            sourceSystem: source.sourceSystem,
            startDate: window.startDate,
            endDate: window.endDate,
            displayedTotal: capture.displayedTotal,
            displayedTotalCapped: capture.displayedTotalCapped,
            exportedRecordCount: capture.records.length,
            excludedNonPermitCount: capture.excludedNonPermitCount,
            records: capture.records,
          },
          null,
          2,
        )}\n`,
      );
      checkpoint = {
        ...checkpoint,
        pendingWindows: checkpoint.pendingWindows.slice(1),
        completedWindows: {
          ...checkpoint.completedWindows,
          [windowKey]: {
            startDate: window.startDate,
            endDate: window.endDate,
            displayedTotal: capture.displayedTotal,
            displayedTotalCapped: capture.displayedTotalCapped,
            exportedRecordCount: capture.records.length,
            excludedNonPermitCount: capture.excludedNonPermitCount,
            recordsPath,
            rawCsvSha256: createHash("sha256")
              .update(capture.rawCsv)
              .digest("hex"),
            completedAt: now(),
          },
        },
        updatedAt: now(),
      };
      await writePrivateAtomic(
        checkpointPath,
        `${JSON.stringify(checkpoint, null, 2)}\n`,
      );
      processed += 1;
    }
  } finally {
    await browser.close().catch(() => undefined);
  }
  const aggregate = await aggregateWindows(checkpoint, normalizedListPath);
  const summary = {
    status:
      checkpoint.pendingWindows.length === 0
        ? /** @type {"complete"} */ ("complete")
        : /** @type {"paused"} */ ("paused"),
    sourceKey: source.key,
    sourceSystem: source.sourceSystem,
    windowsProcessedThisInvocation: processed,
    completedWindowCount: Object.keys(checkpoint.completedWindows).length,
    pendingWindowCount: checkpoint.pendingWindows.length,
    exportedRecordObservations: aggregate.exportedRecordObservations,
    uniquePermitCount: aggregate.uniquePermitCount,
    duplicatePermitObservations: aggregate.duplicatePermitObservations,
    cappedDisplayedTotalWindowCount: aggregate.cappedDisplayedTotalWindowCount,
    excludedNonPermitCount: aggregate.excludedNonPermitCount,
    normalizedListPath,
    checkpointPath,
    completedAt: now(),
  };
  await writePrivateAtomic(
    summaryPath,
    `${JSON.stringify(summary, null, 2)}\n`,
  );
  return summary;
}

/**
 * Aggregate official CSV window records by detail-compatible source key.
 *
 * @param {AccelaCsvCheckpoint} checkpoint - Durable source state.
 * @param {string} normalizedListPath - Final private JSONL.
 * @returns {Promise<{
 *   exportedRecordObservations:number,
 *   uniquePermitCount:number,
 *   duplicatePermitObservations:number,
 *   cappedDisplayedTotalWindowCount:number,
 *   excludedNonPermitCount:number
 * }>} Whole-run counts.
 */
async function aggregateWindows(checkpoint, normalizedListPath) {
  /** @type {Map<string, BrowardAccelaCsvPermitRecord>} */
  const records = new Map();
  let observations = 0;
  let cappedDisplayedTotalWindowCount = 0;
  let excludedNonPermitCount = 0;
  for (const receipt of Object.values(checkpoint.completedWindows)) {
    if (receipt.displayedTotalCapped) cappedDisplayedTotalWindowCount += 1;
    excludedNonPermitCount += receipt.excludedNonPermitCount ?? 0;
    const payload = /** @type {unknown} */ (
      JSON.parse(await readFile(receipt.recordsPath, "utf8"))
    );
    if (!isRecord(payload) || !Array.isArray(payload.records)) {
      throw new Error("Accela CSV window artifact is malformed");
    }
    for (const value of payload.records) {
      if (
        !isRecord(value) ||
        typeof value.recordKey !== "string" ||
        typeof value.recordNumber !== "string"
      ) {
        throw new Error("Accela CSV permit identity is malformed");
      }
      observations += 1;
      const record = /** @type {BrowardAccelaCsvPermitRecord} */ (value);
      const existing = records.get(record.recordKey);
      if (
        existing !== undefined &&
        (existing.recordNumber !== record.recordNumber ||
          existing.sourceUrl !== record.sourceUrl)
      ) {
        throw new Error(`Conflicting Accela CSV key ${record.recordKey}`);
      }
      records.set(record.recordKey, record);
    }
  }
  const ordered = [...records.values()].sort((left, right) =>
    left.recordKey.localeCompare(right.recordKey),
  );
  await writePrivateAtomic(
    normalizedListPath,
    ordered.length === 0
      ? ""
      : `${ordered.map((record) => JSON.stringify(record)).join("\n")}\n`,
  );
  return {
    exportedRecordObservations: observations,
    uniquePermitCount: ordered.length,
    duplicatePermitObservations: observations - ordered.length,
    cappedDisplayedTotalWindowCount,
    excludedNonPermitCount,
  };
}

/**
 * Read or initialize immutable CSV window state.
 *
 * @param {string} checkpointPath - Private checkpoint path.
 * @param {AccelaCsvWindowOptions} options - Run configuration.
 * @param {string} startedAt - Initial timestamp.
 * @returns {Promise<AccelaCsvCheckpoint>} Durable state.
 */
async function readOrCreateCheckpoint(checkpointPath, options, startedAt) {
  const configurationSha256 = createHash("sha256")
    .update(
      JSON.stringify({
        sourceKey: options.sourceKey,
        startDate: options.startDate,
        endDate: options.endDate,
        windowDays: options.windowDays,
        delayMs: options.delayMs,
      }),
    )
    .digest("hex");
  try {
    const parsed = /** @type {unknown} */ (
      JSON.parse(await readFile(checkpointPath, "utf8"))
    );
    if (
      !isRecord(parsed) ||
      parsed.schemaVersion !== CHECKPOINT_SCHEMA_VERSION ||
      parsed.sourceKey !== options.sourceKey ||
      parsed.configurationSha256 !== configurationSha256 ||
      !Array.isArray(parsed.pendingWindows) ||
      !isRecord(parsed.completedWindows)
    ) {
      throw new Error(
        "Existing Accela CSV checkpoint does not match run configuration",
      );
    }
    return /** @type {AccelaCsvCheckpoint} */ (parsed);
  } catch (error) {
    if (!isNodeError(error) || error.code !== "ENOENT") throw error;
  }
  return {
    schemaVersion: CHECKPOINT_SCHEMA_VERSION,
    sourceKey: options.sourceKey,
    configurationSha256,
    pendingWindows: createAccelaCsvDateWindows(
      options.startDate,
      options.endDate,
      options.windowDays,
    ),
    completedWindows: {},
    startedAt,
    updatedAt: startedAt,
  };
}

/**
 * Return a compact date-window key.
 *
 * @param {DateWindow} window - Inclusive window.
 * @returns {string} Ordered key.
 */
function localWindowKey(window) {
  return `${window.startDate.replaceAll("-", "")}_${window.endDate.replaceAll("-", "")}`;
}

/**
 * Validate ISO calendar date.
 *
 * @param {unknown} value - Candidate.
 * @param {string} name - Option name.
 * @returns {string} Validated date.
 */
function requireIsoDate(value, name) {
  if (typeof value !== "string") throw new Error(`${name} is required`);
  const match = /^(\d{4})-(\d{2})-(\d{2})$/u.exec(value);
  if (match === null) throw new Error(`${name} must be YYYY-MM-DD`);
  const date = new Date(
    Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])),
  );
  if (
    date.getUTCFullYear() !== Number(match[1]) ||
    date.getUTCMonth() !== Number(match[2]) - 1 ||
    date.getUTCDate() !== Number(match[3])
  ) {
    throw new Error(`${name} is not a valid calendar date`);
  }
  return value;
}

/**
 * Convert ISO date to UTC milliseconds.
 *
 * @param {string} value - ISO date.
 * @returns {number} Epoch milliseconds.
 */
function toMillis(value) {
  return Date.parse(`${value}T00:00:00Z`);
}

/**
 * Add UTC days.
 *
 * @param {string} value - ISO date.
 * @param {number} days - Day delta.
 * @returns {string} Shifted date.
 */
function addDays(value, days) {
  return new Date(toMillis(value) + days * 86_400_000)
    .toISOString()
    .slice(0, 10);
}

/**
 * Parse bounded integer.
 *
 * @param {string} raw - Raw text.
 * @param {string} name - Option name.
 * @param {number} minimum - Inclusive minimum.
 * @param {number} maximum - Inclusive maximum.
 * @returns {number} Validated integer.
 */
function boundedInteger(raw, name, minimum, maximum) {
  const value = Number(raw);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(
      `--${name} must be an integer from ${String(minimum)} through ${String(maximum)}`,
    );
  }
  return value;
}

/**
 * Atomically write a private artifact.
 *
 * @param {string} filePath - Final path.
 * @param {string} content - Complete content.
 * @returns {Promise<void>} Resolves after replacement.
 */
async function writePrivateAtomic(filePath, content) {
  await mkdir(path.dirname(filePath), { recursive: true, mode: 0o700 });
  const temporaryPath = `${filePath}.${String(process.pid)}.tmp`;
  await writeFile(temporaryPath, content, {
    encoding: "utf8",
    mode: 0o600,
  });
  await rename(temporaryPath, filePath);
}

/**
 * Narrow an unknown value to an object.
 *
 * @param {unknown} value - Candidate.
 * @returns {value is Record<string, unknown>} Whether it is an object.
 */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * Narrow an unknown error to a Node error.
 *
 * @param {unknown} value - Caught value.
 * @returns {value is Error & {code:string}} Whether it has a code.
 */
function isNodeError(value) {
  return (
    value instanceof Error && "code" in value && typeof value.code === "string"
  );
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  runAccelaCsvWindows(parseAccelaCsvWindowOptions(process.argv.slice(2)))
    .then((summary) => {
      console.log(
        JSON.stringify({
          event: "broward_accela_csv_windows_finished",
          ...summary,
        }),
      );
    })
    .catch((error) => {
      console.error(
        JSON.stringify({
          event: "broward_accela_csv_windows_failed",
          message: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      process.exitCode = 1;
    });
}
