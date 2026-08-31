#!/usr/bin/env node
// @ts-check

/**
 * Enumerate Broward Tyler Civic Access permits by application-date windows.
 *
 * One bootstrapped browser session is reused for the entire tenant invocation.
 * Each terminal API page is captured privately, source totals are reconciled,
 * and the window checkpoint advances only after its artifacts are durable.
 */

import { createHash } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { BROWARD_PERMIT_JURISDICTIONS } from "./permit-source-adapters/broward-permit-jurisdictions.mjs";
import {
  closeTylerDateWindowSession,
  createTylerDateWindowSession,
  searchTylerDateWindow,
} from "./permit-source-adapters/tyler-civic-access.mjs";

const CHECKPOINT_SCHEMA_VERSION =
  "oracle-node.broward-tyler-date-windows.v1";
const TYLER_SOURCE_KEYS = new Set([
  "pembroke_pines",
  "hallandale_beach",
  "miramar",
  "oakland_park",
]);

/**
 * @typedef {"pembroke_pines" | "hallandale_beach" | "miramar" | "oakland_park"} TylerSourceKey
 *
 * @typedef {object} DateWindow
 * @property {string} startDate - Inclusive application date.
 * @property {string} endDate - Inclusive application date.
 *
 * @typedef {object} TylerDateWindowOptions
 * @property {TylerSourceKey} sourceKey - Anonymous Tyler tenant.
 * @property {string} startDate - Inclusive range start.
 * @property {string} endDate - Inclusive range end.
 * @property {number} windowDays - Initial window width.
 * @property {number} pageSize - Public UI page size.
 * @property {number} maxPages - Hard pages per window.
 * @property {number} delayMs - Delay between API pages and windows.
 * @property {number | null} maxWindows - Optional pilot pause bound.
 * @property {string} outputDirectory - Private artifact root.
 *
 * @typedef {object} CompletedTylerWindow
 * @property {string} startDate - Inclusive window start.
 * @property {string} endDate - Inclusive window end.
 * @property {number} totalFound - Reconciled source records.
 * @property {number} totalPages - Reconciled source pages.
 * @property {number | undefined} [invalidRecordCount] - Raw rows without normalized identity; absent in pre-v2 receipts means zero.
 * @property {number | undefined} [sourceMissingRecordCount] - Reported rows absent from source pages; absent in pre-v2 receipts means zero.
 * @property {string} linksPath - Private normalized page records.
 * @property {string} completedAt - ISO completion timestamp.
 *
 * @typedef {object} TylerDateCheckpoint
 * @property {typeof CHECKPOINT_SCHEMA_VERSION} schemaVersion - Schema marker.
 * @property {TylerSourceKey} sourceKey - Tenant identity.
 * @property {string} configurationSha256 - Immutable run hash.
 * @property {DateWindow[]} pendingWindows - Remaining chronological windows.
 * @property {Record<string, CompletedTylerWindow>} completedWindows - Receipts.
 * @property {string} startedAt - ISO first start.
 * @property {string} updatedAt - ISO durable update.
 *
 * @typedef {import("./permit-source-adapters/tyler-civic-access.mjs").NormalizedCityPermit} NormalizedCityPermit
 *
 * @typedef {object} TylerDateWindowSummary
 * @property {"paused" | "complete"} status - Run state.
 * @property {string} sourceKey - Tenant key.
 * @property {string} sourceSystem - Query source key.
 * @property {number} windowsProcessedThisInvocation - New windows completed.
 * @property {number} completedWindowCount - Durable terminal windows.
 * @property {number} pendingWindowCount - Remaining windows.
 * @property {number} sourceRecordObservations - Rows across all windows.
 * @property {number} uniquePermitCount - Unique CaseId records.
 * @property {number} duplicatePermitObservations - Cross-window duplicates.
 * @property {number} invalidRecordCount - Accounted malformed source rows.
 * @property {number} sourceMissingRecordCount - Reported rows unavailable from paging.
 * @property {string} normalizedListPath - Private deterministic JSONL.
 * @property {string} checkpointPath - Private checkpoint.
 * @property {string} completedAt - ISO summary timestamp.
 */

/**
 * Parse an explicitly bounded Tyler run.
 *
 * @param {readonly string[]} argv - CLI arguments.
 * @returns {TylerDateWindowOptions} Validated options.
 */
export function parseTylerDateWindowOptions(argv) {
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
      throw new Error("Tyler date-window options must be --flag value pairs");
    }
    values.set(flag.slice(2), value);
  }
  const rawSource = values.get("source");
  if (
    typeof rawSource !== "string" ||
    !TYLER_SOURCE_KEYS.has(rawSource)
  ) {
    throw new Error(
      "--source must be pembroke_pines, hallandale_beach, miramar, or oakland_park",
    );
  }
  const sourceKey = /** @type {TylerSourceKey} */ (rawSource);
  const startDate = requireIsoDate(values.get("start-date"), "--start-date");
  const endDate = requireIsoDate(values.get("end-date"), "--end-date");
  if (toMillis(endDate) < toMillis(startDate)) {
    throw new Error("--end-date must not precede --start-date");
  }
  const rawMaxWindows = values.get("max-windows");
  const outputDirectory =
    values.get("output-dir") ??
    `downloads/broward/tyler-date-windows/${sourceKey}`;
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
    pageSize: boundedChoice(
      values.get("page-size") ?? "100",
      "page-size",
      [10, 25, 50, 100],
    ),
    maxPages: boundedInteger(
      values.get("max-pages") ?? "200",
      "max-pages",
      1,
      200,
    ),
    delayMs: boundedInteger(
      values.get("delay-ms") ?? "1000",
      "delay-ms",
      1_000,
      60_000,
    ),
    maxWindows:
      rawMaxWindows === undefined
        ? null
        : boundedInteger(rawMaxWindows, "max-windows", 1, 1_000_000),
    outputDirectory,
  };
}

/**
 * Create exhaustive adjacent application-date windows.
 *
 * @param {string} startDate - Inclusive ISO start.
 * @param {string} endDate - Inclusive ISO end.
 * @param {number} windowDays - Maximum window width.
 * @returns {DateWindow[]} Chronological windows.
 */
export function createTylerDateWindows(startDate, endDate, windowDays) {
  if (!Number.isInteger(windowDays) || windowDays < 1) {
    throw new Error("Tyler windowDays must be positive");
  }
  /** @type {DateWindow[]} */
  const windows = [];
  let cursor = startDate;
  while (toMillis(cursor) <= toMillis(endDate)) {
    const candidateEnd = addDays(cursor, windowDays - 1);
    const actualEnd =
      toMillis(candidateEnd) > toMillis(endDate)
        ? endDate
        : candidateEnd;
    windows.push({ startDate: cursor, endDate: actualEnd });
    cursor = addDays(actualEnd, 1);
  }
  return windows;
}

/**
 * Run or resume a persistent Tyler tenant session.
 *
 * @param {TylerDateWindowOptions} options - Validated options.
 * @param {{
 *   now?:()=>string,
 *   wait?:(milliseconds:number)=>Promise<void>,
 *   createSession?:typeof createTylerDateWindowSession,
 *   closeSession?:typeof closeTylerDateWindowSession,
 *   searchWindow?:typeof searchTylerDateWindow
 * }} [dependencies={}] - Injectable runtime dependencies.
 * @returns {Promise<TylerDateWindowSummary>} Reconciled run summary.
 */
export async function runTylerDateWindows(options, dependencies = {}) {
  const now = dependencies.now ?? (() => new Date().toISOString());
  const wait =
    dependencies.wait ??
    ((milliseconds) =>
      new Promise((resolvePromise) => {
        setTimeout(resolvePromise, milliseconds);
      }));
  const config = BROWARD_PERMIT_JURISDICTIONS[options.sourceKey];
  if (
    config === undefined ||
    config.vendor !== "tyler-civic-access" ||
    !config.anonymousSearchCertified ||
    config.skipReason !== null
  ) {
    throw new Error("Tyler source is not certified for anonymous enumeration");
  }
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
  const createSession =
    dependencies.createSession ?? createTylerDateWindowSession;
  const closeSession =
    dependencies.closeSession ?? closeTylerDateWindowSession;
  const searchWindow = dependencies.searchWindow ?? searchTylerDateWindow;
  const session = await createSession(config, logger);
  let processed = 0;
  try {
    while (
      checkpoint.pendingWindows.length > 0 &&
      (options.maxWindows === null || processed < options.maxWindows)
    ) {
      const window = checkpoint.pendingWindows[0];
      if (window === undefined) break;
      if (processed > 0) await wait(options.delayMs);
      const result = await searchWindow(
        session,
        window.startDate,
        window.endDate,
        options.pageSize,
        options.maxPages,
        options.delayMs,
        wait,
      );
      const windowKey = localWindowKey(window);
      const windowDirectory = path.join(windowsDirectory, windowKey);
      const rawDirectory = path.join(windowDirectory, "raw");
      await mkdir(rawDirectory, { recursive: true, mode: 0o700 });
      for (const page of result.pages) {
        await writePrivateAtomic(
          path.join(
            rawDirectory,
            `page-${String(page.pageNumber).padStart(4, "0")}.json`,
          ),
          page.rawJson,
        );
      }
      const linksPath = path.join(windowDirectory, "records.private.json");
      await writePrivateAtomic(
        linksPath,
        `${JSON.stringify(
          {
            schemaVersion: "oracle-node.broward-tyler-window-records.v1",
            sourceKey: options.sourceKey,
            sourceSystem: config.sourceSystem,
            startDate: window.startDate,
            endDate: window.endDate,
            totalFound: result.totalFound,
            totalPages: result.totalPages,
            invalidRecordCount: result.invalidRecordCount,
            sourceMissingRecordCount: result.sourceMissingRecordCount,
            records: result.records,
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
            totalFound: result.totalFound,
            totalPages: result.totalPages,
            invalidRecordCount: result.invalidRecordCount,
            sourceMissingRecordCount: result.sourceMissingRecordCount,
            linksPath,
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
    await closeSession(session);
  }
  const aggregate = await aggregateWindows(
    checkpoint,
    normalizedListPath,
  );
  const summary = {
    status:
      checkpoint.pendingWindows.length === 0
        ? /** @type {"complete"} */ ("complete")
        : /** @type {"paused"} */ ("paused"),
    sourceKey: options.sourceKey,
    sourceSystem: config.sourceSystem,
    windowsProcessedThisInvocation: processed,
    completedWindowCount: Object.keys(
      checkpoint.completedWindows,
    ).length,
    pendingWindowCount: checkpoint.pendingWindows.length,
    sourceRecordObservations: aggregate.sourceRecordObservations,
    uniquePermitCount: aggregate.uniquePermitCount,
    duplicatePermitObservations: aggregate.duplicatePermitObservations,
    invalidRecordCount: aggregate.invalidRecordCount,
    sourceMissingRecordCount: aggregate.sourceMissingRecordCount,
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
 * Aggregate complete window records by stable Tyler CaseId.
 *
 * @param {TylerDateCheckpoint} checkpoint - Durable source state.
 * @param {string} normalizedListPath - Private deterministic JSONL path.
 * @returns {Promise<{
 *   sourceRecordObservations:number,
 *   uniquePermitCount:number,
 *   duplicatePermitObservations:number,
 *   invalidRecordCount:number,
 *   sourceMissingRecordCount:number
 * }>} Whole-run counts.
 */
async function aggregateWindows(checkpoint, normalizedListPath) {
  /** @type {Map<string, NormalizedCityPermit>} */
  const byCaseId = new Map();
  let observations = 0;
  let invalidRecordCount = 0;
  let sourceMissingRecordCount = 0;
  for (const receipt of Object.values(checkpoint.completedWindows)) {
    invalidRecordCount += receipt.invalidRecordCount ?? 0;
    sourceMissingRecordCount += receipt.sourceMissingRecordCount ?? 0;
    const payload = /** @type {unknown} */ (
      JSON.parse(await readFile(receipt.linksPath, "utf8"))
    );
    if (!isRecord(payload) || !Array.isArray(payload.records)) {
      throw new Error("Tyler terminal window artifact is malformed");
    }
    for (const value of payload.records) {
      if (!isRecord(value) || !isRecord(value.raw)) {
        throw new Error("Tyler terminal record is malformed");
      }
      const caseId = value.raw.case_id;
      if (
        typeof caseId !== "string" ||
        typeof value.source_system !== "string" ||
        typeof value.permit_number !== "string"
      ) {
        throw new Error("Tyler terminal record identity is missing");
      }
      observations += 1;
      const key = `${value.source_system}:${caseId}`;
      const record = /** @type {NormalizedCityPermit} */ (value);
      const existing = byCaseId.get(key);
      if (
        existing !== undefined &&
        existing.permit_number !== record.permit_number
      ) {
        throw new Error(`Conflicting Tyler CaseId ${key}`);
      }
      byCaseId.set(key, record);
    }
  }
  const records = [...byCaseId.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([, record]) => record);
  await writePrivateAtomic(
    normalizedListPath,
    records.length === 0
      ? ""
      : `${records.map((record) => JSON.stringify(record)).join("\n")}\n`,
  );
  return {
    sourceRecordObservations: observations,
    uniquePermitCount: records.length,
    duplicatePermitObservations: observations - records.length,
    invalidRecordCount,
    sourceMissingRecordCount,
  };
}

/**
 * Read or initialize an immutable Tyler checkpoint.
 *
 * @param {string} checkpointPath - Private checkpoint path.
 * @param {TylerDateWindowOptions} options - Run configuration.
 * @param {string} startedAt - Initial timestamp.
 * @returns {Promise<TylerDateCheckpoint>} Durable state.
 */
async function readOrCreateCheckpoint(checkpointPath, options, startedAt) {
  const configurationSha256 = createHash("sha256")
    .update(
      JSON.stringify({
        sourceKey: options.sourceKey,
        startDate: options.startDate,
        endDate: options.endDate,
        windowDays: options.windowDays,
        pageSize: options.pageSize,
        maxPages: options.maxPages,
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
        "Existing Tyler checkpoint does not match run configuration",
      );
    }
    return /** @type {TylerDateCheckpoint} */ (parsed);
  } catch (error) {
    if (!isNodeError(error) || error.code !== "ENOENT") throw error;
  }
  return {
    schemaVersion: CHECKPOINT_SCHEMA_VERSION,
    sourceKey: options.sourceKey,
    configurationSha256,
    pendingWindows: createTylerDateWindows(
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
 * Return a filesystem-safe date-window key.
 *
 * @param {DateWindow} window - Inclusive window.
 * @returns {string} Ordered compact key.
 */
function localWindowKey(window) {
  return `${window.startDate.replaceAll("-", "")}_${window.endDate.replaceAll("-", "")}`;
}

/**
 * Validate an ISO calendar date.
 *
 * @param {unknown} value - Candidate date.
 * @param {string} name - Option name.
 * @returns {string} Validated YYYY-MM-DD.
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
 * Convert ISO date to UTC midnight milliseconds.
 *
 * @param {string} value - ISO date.
 * @returns {number} Epoch milliseconds.
 */
function toMillis(value) {
  return Date.parse(`${value}T00:00:00Z`);
}

/**
 * Add whole UTC days.
 *
 * @param {string} value - ISO date.
 * @param {number} days - Whole day delta.
 * @returns {string} Shifted ISO date.
 */
function addDays(value, days) {
  return new Date(toMillis(value) + days * 86_400_000)
    .toISOString()
    .slice(0, 10);
}

/**
 * Parse an inclusive bounded integer.
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
 * Parse one value from a fixed integer set.
 *
 * @param {string} raw - Raw text.
 * @param {string} name - Option name.
 * @param {readonly number[]} allowed - Allowed values.
 * @returns {number} Validated value.
 */
function boundedChoice(raw, name, allowed) {
  const value = Number(raw);
  if (!Number.isInteger(value) || !allowed.includes(value)) {
    throw new Error(`--${name} must be one of ${allowed.join(", ")}`);
  }
  return value;
}

/**
 * Atomically write a private mode-0600 artifact.
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
 * Narrow an unknown value to a non-array record.
 *
 * @param {unknown} value - Candidate.
 * @returns {value is Record<string, unknown>} Whether it is a record.
 */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * Narrow an unknown error to a Node error with a string code.
 *
 * @param {unknown} value - Caught value.
 * @returns {value is Error & {code:string}} Whether a code exists.
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
  runTylerDateWindows(
    parseTylerDateWindowOptions(process.argv.slice(2)),
  )
    .then((summary) => {
      console.log(
        JSON.stringify({
          event: "broward_tyler_date_windows_finished",
          ...summary,
        }),
      );
    })
    .catch((error) => {
      console.error(
        JSON.stringify({
          event: "broward_tyler_date_windows_failed",
          message: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      process.exitCode = 1;
    });
}
