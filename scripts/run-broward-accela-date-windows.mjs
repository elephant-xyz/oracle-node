#!/usr/bin/env node
// @ts-check

/**
 * Enumerate date-enabled Broward Accela agencies with one persistent browser.
 *
 * This is a list-first discovery stage. It captures every reconciled public
 * search page and permit link before any expensive detail enrichment. Dense
 * multi-day windows split recursively, terminal windows paginate completely,
 * and a private checkpoint makes interruption safe.
 */

import { createHash } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import {
  BrowardAccelaSourceError,
  buildBrowardAccelaDateWindowKey,
  createBrowardAccelaBrowser,
  readBrowardAccelaSource,
  searchBrowardAccelaDateWindow,
} from "./permit-source-adapters/broward-accela.mjs";

const CHECKPOINT_SCHEMA_VERSION =
  "oracle-node.broward-accela-date-windows.v1";
const DATE_WINDOW_SOURCE_KEYS = new Set([
  "hollywood",
  "plantation",
  "cooper-city",
  "weston",
]);

/**
 * @typedef {"hollywood" | "plantation" | "cooper-city" | "weston"} BrowardAccelaDateSourceKey
 *
 * @typedef {object} DateWindow
 * @property {string} startDate - Inclusive ISO start date.
 * @property {string} endDate - Inclusive ISO end date.
 *
 * @typedef {object} BrowardAccelaDateWindowOptions
 * @property {BrowardAccelaDateSourceKey} sourceKey - Date-enabled Accela source.
 * @property {string} startDate - Inclusive ISO range start.
 * @property {string} endDate - Inclusive ISO range end.
 * @property {number} initialWindowDays - Initial message/window width.
 * @property {number} splitThreshold - Dense multi-day split threshold.
 * @property {number} maxPages - Terminal result-page ceiling.
 * @property {number} delayMs - Delay between source windows.
 * @property {number | null} maxWindows - Optional per-invocation pilot bound.
 * @property {string} outputDirectory - Private artifact root.
 *
 * @typedef {object} CompletedDateWindow
 * @property {"terminal" | "split"} status - Whether the window is final.
 * @property {string} startDate - Inclusive start date.
 * @property {string} endDate - Inclusive end date.
 * @property {number | null} reportedTotal - Source total when visible.
 * @property {number} discoveredPermitCount - Unique list links in this window.
 * @property {number} excludedNonPermitCount - Explicit cross-module rows.
 * @property {string | null} linksPath - Terminal private links artifact.
 * @property {string} completedAt - ISO completion timestamp.
 *
 * @typedef {object} BrowardAccelaDateCheckpoint
 * @property {typeof CHECKPOINT_SCHEMA_VERSION} schemaVersion - Checkpoint schema.
 * @property {BrowardAccelaDateSourceKey} sourceKey - Source identity.
 * @property {string} configurationSha256 - Immutable run configuration hash.
 * @property {DateWindow[]} pendingWindows - Source windows not yet terminal.
 * @property {Record<string, CompletedDateWindow>} completedWindows - Window receipts.
 * @property {string} startedAt - ISO initial start.
 * @property {string} updatedAt - ISO latest durable update.
 *
 * @typedef {object} BrowardAccelaListPermit
 * @property {"oracle-node.broward-accela-list.v1"} schemaVersion - List schema.
 * @property {string} sourceSystem - Jurisdiction source identity.
 * @property {string} jurisdiction - Issuing jurisdiction.
 * @property {string} recordNumber - Full public Accela record number.
 * @property {string} sourceUrl - Official detail URL.
 * @property {string | null} address - Public list work address.
 * @property {string | null} description - Public list description.
 * @property {string | null} status - Public list status.
 * @property {string | null} recordType - Public list record type.
 * @property {string} recordKey - Stable source-system permit identity.
 * @property {string[]} sourceWindowKeys - Terminal windows that exposed the record.
 *
 * @typedef {object} DateWindowRunSummary
 * @property {"paused" | "complete"} status - Run completion state.
 * @property {string} sourceKey - Source key.
 * @property {string} sourceSystem - Source-system key.
 * @property {string} startDate - Requested inclusive start.
 * @property {string} endDate - Requested inclusive end.
 * @property {number} windowsProcessedThisInvocation - Newly completed/split windows.
 * @property {number} terminalWindowCount - Durable terminal windows.
 * @property {number} splitWindowCount - Durable split parent windows.
 * @property {number} pendingWindowCount - Remaining windows.
 * @property {number} uniquePermitCount - Whole-run unique list identities.
 * @property {number} duplicatePermitObservations - Cross-window duplicate observations.
 * @property {number} excludedNonPermitCount - Whole-run cross-module observations.
 * @property {string} normalizedListPath - Private deterministic JSONL output.
 * @property {string} checkpointPath - Private durable checkpoint.
 * @property {string} completedAt - ISO summary time.
 */

/**
 * Parse an explicitly bounded date-window run.
 *
 * @param {readonly string[]} argv - Arguments after the script path.
 * @returns {BrowardAccelaDateWindowOptions} Validated options.
 */
export function parseBrowardAccelaDateWindowOptions(argv) {
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
      throw new Error("Accela date-window options must be --flag value pairs");
    }
    values.set(flag.slice(2), value);
  }
  const rawSource = values.get("source");
  if (
    typeof rawSource !== "string" ||
    !DATE_WINDOW_SOURCE_KEYS.has(rawSource)
  ) {
    throw new Error(
      "--source must be hollywood, plantation, cooper-city, or weston",
    );
  }
  const sourceKey = /** @type {BrowardAccelaDateSourceKey} */ (rawSource);
  const startDate = requireIsoDate(values.get("start-date"), "--start-date");
  const endDate = requireIsoDate(values.get("end-date"), "--end-date");
  if (isoDateToMillis(endDate) < isoDateToMillis(startDate)) {
    throw new Error("--end-date must not precede --start-date");
  }
  const initialWindowDays = boundedInteger(
    values.get("window-days") ?? "30",
    "window-days",
    1,
    366,
  );
  const splitThreshold = boundedInteger(
    values.get("split-threshold") ?? "100",
    "split-threshold",
    2,
    10_000,
  );
  const maxPages = boundedInteger(
    values.get("max-pages") ?? "200",
    "max-pages",
    1,
    200,
  );
  const delayMs = boundedInteger(
    values.get("delay-ms") ?? "1000",
    "delay-ms",
    1_000,
    60_000,
  );
  const rawMaxWindows = values.get("max-windows");
  const maxWindows =
    rawMaxWindows === undefined
      ? null
      : boundedInteger(rawMaxWindows, "max-windows", 1, 1_000_000);
  const outputDirectory =
    values.get("output-dir") ??
    `downloads/broward/accela-date-windows/${sourceKey}`;
  if (outputDirectory.trim().length === 0) {
    throw new Error("--output-dir must not be empty");
  }
  return {
    sourceKey,
    startDate,
    endDate,
    initialWindowDays,
    splitThreshold,
    maxPages,
    delayMs,
    maxWindows,
    outputDirectory,
  };
}

/**
 * Build non-overlapping inclusive initial windows.
 *
 * @param {string} startDate - Inclusive ISO start.
 * @param {string} endDate - Inclusive ISO end.
 * @param {number} windowDays - Maximum inclusive days per window.
 * @returns {DateWindow[]} Chronological windows.
 */
export function createBrowardAccelaDateWindows(
  startDate,
  endDate,
  windowDays,
) {
  const start = requireIsoDate(startDate, "startDate");
  const end = requireIsoDate(endDate, "endDate");
  if (!Number.isInteger(windowDays) || windowDays < 1) {
    throw new Error("windowDays must be a positive integer");
  }
  if (isoDateToMillis(end) < isoDateToMillis(start)) {
    throw new Error("endDate must not precede startDate");
  }
  /** @type {DateWindow[]} */
  const windows = [];
  let cursor = start;
  while (isoDateToMillis(cursor) <= isoDateToMillis(end)) {
    const candidateEnd = addDays(cursor, windowDays - 1);
    const actualEnd =
      isoDateToMillis(candidateEnd) > isoDateToMillis(end)
        ? end
        : candidateEnd;
    windows.push({ startDate: cursor, endDate: actualEnd });
    cursor = addDays(actualEnd, 1);
  }
  return windows;
}

/**
 * Split one inclusive multi-day window into adjacent non-overlapping halves.
 *
 * @param {DateWindow} window - Dense source window.
 * @returns {[DateWindow, DateWindow]} Two exhaustive child windows.
 */
export function splitBrowardAccelaDateWindow(window) {
  const span = inclusiveDaySpan(window.startDate, window.endDate);
  if (span < 2) throw new Error("A one-day Accela window cannot be split");
  const firstDays = Math.ceil(span / 2);
  const first = {
    startDate: window.startDate,
    endDate: addDays(window.startDate, firstDays - 1),
  };
  return [
    first,
    {
      startDate: addDays(first.endDate, 1),
      endDate: window.endDate,
    },
  ];
}

/**
 * Run or resume one source with a persistent browser.
 *
 * @param {BrowardAccelaDateWindowOptions} options - Validated run options.
 * @param {{
 *   now?:()=>string,
 *   wait?:(milliseconds:number)=>Promise<void>,
 *   createBrowser?:typeof createBrowardAccelaBrowser,
 *   searchWindow?:typeof searchBrowardAccelaDateWindow
 * }} [dependencies={}] - Injectable test/runtime dependencies.
 * @returns {Promise<DateWindowRunSummary>} Aggregate-safe run summary.
 */
export async function runBrowardAccelaDateWindows(
  options,
  dependencies = {},
) {
  const now = dependencies.now ?? (() => new Date().toISOString());
  const wait =
    dependencies.wait ??
    ((milliseconds) =>
      new Promise((resolvePromise) => {
        setTimeout(resolvePromise, milliseconds);
      }));
  const createBrowser =
    dependencies.createBrowser ?? createBrowardAccelaBrowser;
  const searchWindow =
    dependencies.searchWindow ?? searchBrowardAccelaDateWindow;
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
  let checkpoint = await readOrCreateCheckpoint(
    checkpointPath,
    options,
    now(),
  );
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
  const browser = await createBrowser(logger);
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
      const span = inclusiveDaySpan(window.startDate, window.endDate);
      let result;
      try {
        result = await searchWindow({
          browser,
          source,
          startDate: window.startDate,
          endDate: window.endDate,
          maxPages: options.maxPages,
          stopAfterFirstPageWhenTotalAtLeast:
            span > 1 ? options.splitThreshold : undefined,
          logger,
        });
      } catch (error) {
        if (
          span > 1 &&
          error instanceof BrowardAccelaSourceError &&
          error.code === "incomplete_pagination"
        ) {
          const children = splitBrowardAccelaDateWindow(window);
          checkpoint = {
            ...checkpoint,
            pendingWindows: [
              ...children,
              ...checkpoint.pendingWindows.slice(1),
            ],
            completedWindows: {
              ...checkpoint.completedWindows,
              [localWindowKey(window)]: {
                status: "split",
                startDate: window.startDate,
                endDate: window.endDate,
                reportedTotal: null,
                discoveredPermitCount: 0,
                excludedNonPermitCount: 0,
                linksPath: null,
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
          logger.warn("broward_accela_incomplete_window_split", {
            sourceKey: source.key,
            startDate: window.startDate,
            endDate: window.endDate,
            childWindows: children,
          });
          continue;
        }
        throw error;
      }
      const windowDirectory = path.join(windowsDirectory, windowKey);
      const rawDirectory = path.join(windowDirectory, "raw");
      await mkdir(rawDirectory, { recursive: true, mode: 0o700 });
      for (const page of result.pages) {
        await writePrivateAtomic(
          path.join(
            rawDirectory,
            `page-${String(page.pageNumber).padStart(4, "0")}.html`,
          ),
          page.html,
        );
      }
      const pendingWindows = checkpoint.pendingWindows.slice(1);
      /** @type {CompletedDateWindow} */
      let receipt;
      if (result.truncatedForSplit) {
        const children = splitBrowardAccelaDateWindow(window);
        pendingWindows.unshift(...children);
        receipt = {
          status: "split",
          startDate: window.startDate,
          endDate: window.endDate,
          reportedTotal: result.reportedTotal,
          discoveredPermitCount: result.permits.length,
          excludedNonPermitCount: result.excludedNonPermitCount,
          linksPath: null,
          completedAt: now(),
        };
      } else {
        const linksPath = path.join(windowDirectory, "links.private.json");
        await writePrivateAtomic(
          linksPath,
          `${JSON.stringify(
            {
              schemaVersion: "oracle-node.broward-accela-window-links.v1",
              sourceKey: source.key,
              sourceSystem: source.sourceSystem,
              searchKey: buildBrowardAccelaDateWindowKey(
                source,
                window.startDate,
                window.endDate,
              ),
              startDate: window.startDate,
              endDate: window.endDate,
              reportedTotal: result.reportedTotal,
              excludedNonPermitCount: result.excludedNonPermitCount,
              noRecords: result.status === "no_records",
              permits: result.permits,
            },
            null,
            2,
          )}\n`,
        );
        receipt = {
          status: "terminal",
          startDate: window.startDate,
          endDate: window.endDate,
          reportedTotal: result.reportedTotal,
          discoveredPermitCount: result.permits.length,
          excludedNonPermitCount: result.excludedNonPermitCount,
          linksPath,
          completedAt: now(),
        };
      }
      checkpoint = {
        ...checkpoint,
        pendingWindows,
        completedWindows: {
          ...checkpoint.completedWindows,
          [windowKey]: receipt,
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

  const aggregate = await aggregateTerminalWindows(
    checkpoint,
    normalizedListPath,
    source.sourceSystem,
    source.jurisdiction,
  );
  const summary = {
    status:
      checkpoint.pendingWindows.length === 0
        ? /** @type {"complete"} */ ("complete")
        : /** @type {"paused"} */ ("paused"),
    sourceKey: source.key,
    sourceSystem: source.sourceSystem,
    startDate: options.startDate,
    endDate: options.endDate,
    windowsProcessedThisInvocation: processed,
    terminalWindowCount: aggregate.terminalWindowCount,
    splitWindowCount: aggregate.splitWindowCount,
    pendingWindowCount: checkpoint.pendingWindows.length,
    uniquePermitCount: aggregate.uniquePermitCount,
    duplicatePermitObservations: aggregate.duplicatePermitObservations,
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
 * Aggregate terminal links into deterministic unique list records.
 *
 * @param {BrowardAccelaDateCheckpoint} checkpoint - Durable source state.
 * @param {string} normalizedListPath - Final private JSONL path.
 * @param {string} sourceSystem - Jurisdiction source system.
 * @param {string} jurisdiction - Jurisdiction display name.
 * @returns {Promise<{
 *   terminalWindowCount:number,
 *   splitWindowCount:number,
 *   uniquePermitCount:number,
 *   duplicatePermitObservations:number,
 *   excludedNonPermitCount:number
 * }>} Reconciled aggregate counts.
 */
async function aggregateTerminalWindows(
  checkpoint,
  normalizedListPath,
  sourceSystem,
  jurisdiction,
) {
  /** @type {Map<string, BrowardAccelaListPermit>} */
  const records = new Map();
  let observations = 0;
  let terminalWindowCount = 0;
  let splitWindowCount = 0;
  let excludedNonPermitCount = 0;
  for (const [windowKey, receipt] of Object.entries(
    checkpoint.completedWindows,
  ).sort(([left], [right]) => left.localeCompare(right))) {
    if (receipt.status === "split") {
      splitWindowCount += 1;
      continue;
    }
    terminalWindowCount += 1;
    excludedNonPermitCount += receipt.excludedNonPermitCount;
    if (receipt.linksPath === null) {
      throw new Error(`Terminal Accela window ${windowKey} has no links path`);
    }
    const payload = /** @type {unknown} */ (
      JSON.parse(await readFile(receipt.linksPath, "utf8"))
    );
    if (!isRecord(payload) || !Array.isArray(payload.permits)) {
      throw new Error(`Terminal Accela window ${windowKey} is malformed`);
    }
    for (const value of payload.permits) {
      if (
        !isRecord(value) ||
        typeof value.recordNumber !== "string" ||
        typeof value.url !== "string"
      ) {
        throw new Error(`Terminal Accela window ${windowKey} has invalid links`);
      }
      observations += 1;
      const recordKey = `${sourceSystem}:permit:${value.recordNumber}`;
      const existing = records.get(recordKey);
      if (
        existing !== undefined &&
        (existing.sourceUrl !== value.url ||
          existing.recordNumber !== value.recordNumber)
      ) {
        throw new Error(`Conflicting Accela list identity ${recordKey}`);
      }
      const sourceWindowKeys = [
        ...new Set([...(existing?.sourceWindowKeys ?? []), windowKey]),
      ].sort();
      records.set(recordKey, {
        schemaVersion: "oracle-node.broward-accela-list.v1",
        sourceSystem,
        jurisdiction,
        recordNumber: value.recordNumber,
        sourceUrl: value.url,
        address: optionalString(value.address),
        description: optionalString(value.description),
        status: optionalString(value.status),
        recordType: optionalString(value.recordType),
        recordKey,
        sourceWindowKeys,
      });
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
    terminalWindowCount,
    splitWindowCount,
    uniquePermitCount: ordered.length,
    duplicatePermitObservations: observations - ordered.length,
    excludedNonPermitCount,
  };
}

/**
 * Read or initialize the immutable date-window checkpoint.
 *
 * @param {string} checkpointPath - Private checkpoint path.
 * @param {BrowardAccelaDateWindowOptions} options - Run configuration.
 * @param {string} startedAt - Initial timestamp.
 * @returns {Promise<BrowardAccelaDateCheckpoint>} Validated checkpoint.
 */
async function readOrCreateCheckpoint(checkpointPath, options, startedAt) {
  const configurationSha256 = hashConfiguration(options);
  try {
    const parsed = /** @type {unknown} */ (
      JSON.parse(await readFile(checkpointPath, "utf8"))
    );
    if (!isRecord(parsed)) {
      throw new Error("Accela date-window checkpoint is not an object");
    }
    if (
      parsed.schemaVersion !== CHECKPOINT_SCHEMA_VERSION ||
      parsed.sourceKey !== options.sourceKey ||
      parsed.configurationSha256 !== configurationSha256 ||
      !Array.isArray(parsed.pendingWindows) ||
      !isRecord(parsed.completedWindows) ||
      typeof parsed.startedAt !== "string" ||
      typeof parsed.updatedAt !== "string"
    ) {
      throw new Error(
        "Existing Accela date-window checkpoint does not match run options",
      );
    }
    return /** @type {BrowardAccelaDateCheckpoint} */ (parsed);
  } catch (error) {
    if (!isNodeError(error) || error.code !== "ENOENT") throw error;
  }
  return {
    schemaVersion: CHECKPOINT_SCHEMA_VERSION,
    sourceKey: options.sourceKey,
    configurationSha256,
    pendingWindows: createBrowardAccelaDateWindows(
      options.startDate,
      options.endDate,
      options.initialWindowDays,
    ),
    completedWindows: {},
    startedAt,
    updatedAt: startedAt,
  };
}

/**
 * Hash immutable options while excluding the per-invocation max-window bound.
 *
 * @param {BrowardAccelaDateWindowOptions} options - Run options.
 * @returns {string} Lowercase SHA-256.
 */
function hashConfiguration(options) {
  return createHash("sha256")
    .update(
      JSON.stringify({
        sourceKey: options.sourceKey,
        startDate: options.startDate,
        endDate: options.endDate,
        initialWindowDays: options.initialWindowDays,
        splitThreshold: options.splitThreshold,
        maxPages: options.maxPages,
        delayMs: options.delayMs,
      }),
    )
    .digest("hex");
}

/**
 * Return a filesystem-safe date-window key.
 *
 * @param {DateWindow} window - Inclusive source window.
 * @returns {string} Compact ordered key.
 */
function localWindowKey(window) {
  return `${window.startDate.replaceAll("-", "")}_${window.endDate.replaceAll("-", "")}`;
}

/**
 * Validate an ISO calendar date.
 *
 * @param {unknown} value - Candidate date.
 * @param {string} name - Field name for errors.
 * @returns {string} Validated YYYY-MM-DD.
 */
function requireIsoDate(value, name) {
  if (typeof value !== "string") {
    throw new Error(`${name} is required`);
  }
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
 * Convert an ISO date to UTC epoch milliseconds.
 *
 * @param {string} value - Validated ISO date.
 * @returns {number} UTC midnight.
 */
function isoDateToMillis(value) {
  return Date.parse(`${value}T00:00:00Z`);
}

/**
 * Add whole UTC calendar days.
 *
 * @param {string} value - Validated ISO date.
 * @param {number} days - Whole day delta.
 * @returns {string} Shifted ISO date.
 */
function addDays(value, days) {
  return new Date(isoDateToMillis(value) + days * 86_400_000)
    .toISOString()
    .slice(0, 10);
}

/**
 * Count inclusive calendar days.
 *
 * @param {string} startDate - Inclusive start.
 * @param {string} endDate - Inclusive end.
 * @returns {number} Positive day span.
 */
function inclusiveDaySpan(startDate, endDate) {
  return (
    Math.floor(
      (isoDateToMillis(endDate) - isoDateToMillis(startDate)) / 86_400_000,
    ) + 1
  );
}

/**
 * Atomically write a mode-0600 private artifact.
 *
 * @param {string} filePath - Final file path.
 * @param {string} content - Complete replacement content.
 * @returns {Promise<void>} Resolves after atomic replacement.
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
 * Normalize an optional parsed string.
 *
 * @param {unknown} value - Candidate value.
 * @returns {string | null} Non-empty string or null.
 */
function optionalString(value) {
  return typeof value === "string" && value.length > 0 ? value : null;
}

/**
 * Parse an inclusive bounded integer.
 *
 * @param {string} raw - Raw CLI text.
 * @param {string} name - Option name without dashes.
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
 * Narrow an unknown value to a non-array object.
 *
 * @param {unknown} value - Candidate value.
 * @returns {value is Record<string, unknown>} Whether the value is an object.
 */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * Narrow an unknown error to a Node error with a string code.
 *
 * @param {unknown} value - Caught value.
 * @returns {value is Error & {code:string}} Whether a string code exists.
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
  runBrowardAccelaDateWindows(
    parseBrowardAccelaDateWindowOptions(process.argv.slice(2)),
  )
    .then((summary) => {
      console.log(
        JSON.stringify({
          event: "broward_accela_date_windows_finished",
          ...summary,
        }),
      );
    })
    .catch((error) => {
      console.error(
        JSON.stringify({
          event: "broward_accela_date_windows_failed",
          message: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      process.exitCode = 1;
    });
}
