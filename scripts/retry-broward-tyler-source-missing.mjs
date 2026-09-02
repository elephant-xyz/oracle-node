#!/usr/bin/env node
// @ts-check

/**
 * Bounded recovery for source-reported rows missing from completed Tyler lists.
 *
 * Only completed receipts that already declare a positive source-missing count
 * are retried. Supported UI page sizes are unioned by exact CaseId, recovered
 * details are reconciled in the same anonymous tenant session, and a remaining
 * gap becomes an explicit accepted terminal exception after finite attempts.
 */

import { createHash } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { loadBrowardPermitPilotToNeon } from "./load-broward-permit-pilot-to-neon.mjs";
import { loadPermitListToNeon } from "./load-broward-permit-list-to-neon.mjs";
import { BROWARD_PERMIT_JURISDICTIONS } from "./permit-source-adapters/broward-permit-jurisdictions.mjs";
import {
  captureRecoveredTylerPermitDetail,
  closeTylerDateWindowSession,
  createTylerDateWindowSession,
  searchTylerDateWindow,
} from "./permit-source-adapters/tyler-civic-access.mjs";

const RECOVERY_SCHEMA_VERSION =
  "oracle-node.broward-tyler-source-missing-recovery.v1";
const ALLOWED_SOURCES = new Set(["pembroke_pines", "oakland_park"]);

/**
 * @typedef {"pembroke_pines" | "oakland_park"} RecoverySourceKey
 *
 * @typedef {object} RecoveryOptions
 * @property {RecoverySourceKey} sourceKey - Completed Tyler tenant inventory.
 * @property {string} inventoryDirectory - Private completed inventory root.
 * @property {number} maxAttempts - Finite whole-window recovery attempts.
 * @property {number} delayMs - Delay between public API pages.
 *
 * @typedef {import("./permit-source-adapters/tyler-civic-access.mjs").NormalizedCityPermit} NormalizedCityPermit
 * @typedef {import("./permit-source-adapters/bounded-permit-common.mjs").NormalizedMunicipalPermit} NormalizedMunicipalPermit
 *
 * @typedef {object} RecoveryWindowState
 * @property {number} attemptCount - Durable whole-window attempts.
 * @property {"pending" | "recovered" | "accepted_terminal_missing"} status
 *   Reconciled terminal state.
 * @property {number} originalMissingCount - Initial completed-receipt gap.
 * @property {number} recoveredCount - Exact CaseIds added to the inventory.
 * @property {number} capturedDetailCount - Reconciled recovered details.
 * @property {number} acceptedTerminalMissingCount - Rows still unavailable.
 * @property {string | null} errorClass - Aggregate-safe latest failure.
 * @property {string} updatedAt - Durable checkpoint timestamp.
 *
 * @typedef {object} RecoveryCheckpoint
 * @property {typeof RECOVERY_SCHEMA_VERSION} schemaVersion - Private schema.
 * @property {RecoverySourceKey} sourceKey - Exact tenant identity.
 * @property {Record<string, RecoveryWindowState>} windows - Date-keyed outcomes.
 * @property {NormalizedCityPermit[]} recoveredListRecords - Exact recovered IDs.
 * @property {NormalizedMunicipalPermit[]} recoveredDetailRecords - Detail rows.
 * @property {string} updatedAt - Durable checkpoint timestamp.
 */

/**
 * Parse a source-scoped finite recovery command.
 *
 * @param {readonly string[]} argv - CLI arguments after the script path.
 * @returns {RecoveryOptions} Validated private recovery options.
 */
export function parseRecoveryOptions(argv) {
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
      throw new Error("Tyler recovery options must be --flag value pairs");
    }
    values.set(flag.slice(2), value);
  }
  const rawSource = values.get("source");
  if (typeof rawSource !== "string" || !ALLOWED_SOURCES.has(rawSource)) {
    throw new Error("--source must be pembroke_pines or oakland_park");
  }
  const sourceKey = /** @type {RecoverySourceKey} */ (rawSource);
  const inventoryDirectory =
    values.get("inventory-dir") ??
    `downloads/broward/tyler-date-windows/${sourceKey.replaceAll("_", "-")}-full-30d`;
  const maxAttempts = boundedInteger(
    values.get("max-attempts") ?? "3",
    "max-attempts",
    1,
    3,
  );
  const delayMs = boundedInteger(
    values.get("delay-ms") ?? "1000",
    "delay-ms",
    1_000,
    60_000,
  );
  return { sourceKey, inventoryDirectory, maxAttempts, delayMs };
}

/**
 * Retry only completed windows with explicit unaccepted source gaps.
 *
 * @param {RecoveryOptions} options - Validated source-scoped bounds.
 * @returns {Promise<{
 *   sourceKey:string,
 *   affectedWindows:number,
 *   recoveredRecords:number,
 *   capturedDetails:number,
 *   terminalMissing:number,
 *   reconciliationPassed:boolean
 * }>} Privacy-safe recovery aggregate.
 */
export async function retryTylerSourceMissing(options) {
  const config = BROWARD_PERMIT_JURISDICTIONS[options.sourceKey];
  if (
    config === undefined ||
    config.vendor !== "tyler-civic-access" ||
    !config.anonymousSearchCertified ||
    config.skipReason !== null
  ) {
    throw new Error("Tyler recovery source is not anonymously certified");
  }
  const inventoryDirectory = path.resolve(options.inventoryDirectory);
  const checkpointPath = path.join(
    inventoryDirectory,
    "source-missing-recovery.private.json",
  );
  const inventoryCheckpointPath = path.join(
    inventoryDirectory,
    "checkpoint.private.json",
  );
  const inventoryCheckpoint = requireInventoryCheckpoint(
    await readJson(inventoryCheckpointPath),
    options.sourceKey,
  );
  const recovery = await readRecoveryCheckpoint(
    checkpointPath,
    options.sourceKey,
  );
  const missingReceipts = Object.entries(
    /** @type {Record<string, Record<string, unknown>>} */ (
      inventoryCheckpoint.completedWindows
    ),
  ).filter(([, receipt]) => {
    const missing = nonNegativeInteger(
      receipt.sourceMissingRecordCount ?? 0,
      "source missing count",
    );
    const accepted = nonNegativeInteger(
      receipt.acceptedSourceMissingCount ?? 0,
      "accepted source missing count",
    );
    return missing > accepted;
  });
  if (missingReceipts.length === 0) {
    await rewriteNormalizedInventory(inventoryDirectory, inventoryCheckpoint);
    await loadRecoveredRecords(options.sourceKey, inventoryDirectory, recovery);
    return buildRecoverySummary(options.sourceKey, recovery, 0);
  }

  for (const [windowKey, receipt] of missingReceipts) {
    const startDate = requireString(receipt.startDate, "window start");
    const endDate = requireString(receipt.endDate, "window end");
    const linksPath = requireString(receipt.linksPath, "window records path");
    const payload = requireWindowPayload(
      await readJson(linksPath),
      options.sourceKey,
    );
    const originalMissingCount = nonNegativeInteger(
      receipt.sourceMissingRecordCount,
      "source missing count",
    );
    let state =
      recovery.windows[windowKey] ?? createWindowState(originalMissingCount);
    const records = new Map(
      payload.records.map((record) => [recordIdentity(record), record]),
    );

    while (
      state.attemptCount < options.maxAttempts &&
      state.status === "pending"
    ) {
      if (state.attemptCount > 0) {
        await delay(Math.max(options.delayMs * state.attemptCount, 5_000));
      }
      let session;
      try {
        session = await createTylerDateWindowSession(config, safeLogger());
        const result = await searchTylerDateWindow(
          session,
          startDate,
          endDate,
          100,
          200,
          options.delayMs,
        );
        if (
          result.totalFound !==
            nonNegativeInteger(receipt.totalFound, "window total") ||
          result.invalidRecordCount !==
            nonNegativeInteger(
              receipt.invalidRecordCount ?? 0,
              "window invalid count",
            )
        ) {
          throw new Error(
            "Tyler recovery totals differ from completed receipt",
          );
        }
        await persistRawRecoveryPages(
          inventoryDirectory,
          windowKey,
          state.attemptCount + 1,
          result.pages,
        );
        const beforeCount = records.size;
        for (const record of result.records) {
          const identity = recordIdentity(record);
          const existing = records.get(identity);
          if (
            existing !== undefined &&
            JSON.stringify(existing) !== JSON.stringify(record)
          ) {
            throw new Error("Tyler recovered CaseId conflicts with inventory");
          }
          records.set(identity, record);
        }
        const recovered = [...records.values()].filter(
          (record) =>
            !payload.records.some(
              (existing) => recordIdentity(existing) === recordIdentity(record),
            ),
        );
        if (records.size > payload.totalFound - payload.invalidRecordCount) {
          throw new Error("Tyler recovery exceeds source-reported total");
        }
        mergeRecoveredListRecords(recovery, recovered);
        for (const record of recovered) {
          const identity = recordIdentity(record);
          if (
            recovery.recoveredDetailRecords.some(
              (detail) => detail.record_key === identity,
            )
          ) {
            continue;
          }
          const detail = await captureRecoveredTylerPermitDetail(
            session,
            config,
            record,
          );
          if (detail.record_key !== identity) {
            throw new Error("Tyler recovered detail identity changed");
          }
          recovery.recoveredDetailRecords.push(detail);
          await writeRecoveryCheckpoint(checkpointPath, recovery);
          await delay(options.delayMs);
        }
        const remaining =
          payload.totalFound - payload.invalidRecordCount - records.size;
        const capturedDetailCount = recovered.filter((record) =>
          recovery.recoveredDetailRecords.some(
            (detail) => detail.record_key === recordIdentity(record),
          ),
        ).length;
        state = {
          ...state,
          attemptCount: state.attemptCount + 1,
          status:
            remaining === 0 && capturedDetailCount === recovered.length
              ? "recovered"
              : "pending",
          recoveredCount: records.size - beforeCount + state.recoveredCount,
          capturedDetailCount,
          acceptedTerminalMissingCount: 0,
          errorClass: null,
          updatedAt: new Date().toISOString(),
        };
      } catch {
        state = {
          ...state,
          attemptCount: state.attemptCount + 1,
          errorClass: "source_or_detail_recovery_error",
          updatedAt: new Date().toISOString(),
        };
      } finally {
        if (session !== undefined) {
          await closeTylerDateWindowSession(session).catch(() => undefined);
        }
      }
      recovery.windows[windowKey] = state;
      recovery.updatedAt = state.updatedAt;
      await writeRecoveryCheckpoint(checkpointPath, recovery);
    }

    const remaining =
      payload.totalFound - payload.invalidRecordCount - records.size;
    const recoveredRecords = [...records.values()].filter(
      (record) =>
        !payload.records.some(
          (existing) => recordIdentity(existing) === recordIdentity(record),
        ),
    );
    mergeRecoveredListRecords(recovery, recoveredRecords);
    const missingDetails = recoveredRecords.filter(
      (record) =>
        !recovery.recoveredDetailRecords.some(
          (detail) => detail.record_key === recordIdentity(record),
        ),
    ).length;
    if (
      (remaining > 0 || missingDetails > 0) &&
      state.attemptCount >= options.maxAttempts
    ) {
      state = {
        ...state,
        status: "accepted_terminal_missing",
        recoveredCount: recoveredRecords.length,
        capturedDetailCount: recoveredRecords.length - missingDetails,
        acceptedTerminalMissingCount: remaining + missingDetails,
        updatedAt: new Date().toISOString(),
      };
    }
    recovery.windows[windowKey] = state;
    recovery.updatedAt = state.updatedAt;
    const reconciledRecords = [...records.values()].sort((left, right) =>
      recordIdentity(left).localeCompare(recordIdentity(right)),
    );
    await writePrivateAtomic(
      linksPath,
      `${JSON.stringify(
        {
          ...payload.raw,
          records: reconciledRecords,
          sourceMissingRecordCount: remaining,
          acceptedSourceMissingCount:
            state.status === "accepted_terminal_missing" ? remaining : 0,
          acceptedDetailMissingCount:
            state.status === "accepted_terminal_missing" ? missingDetails : 0,
          recoveredRecordCount:
            reconciledRecords.length - payload.records.length,
        },
        null,
        2,
      )}\n`,
    );
    receipt.sourceMissingRecordCount = remaining;
    receipt.acceptedSourceMissingCount =
      state.status === "accepted_terminal_missing" ? remaining : 0;
    receipt.acceptedDetailMissingCount =
      state.status === "accepted_terminal_missing" ? missingDetails : 0;
    receipt.recoveredRecordCount =
      reconciledRecords.length - payload.records.length;
    receipt.recoveryCompletedAt = state.updatedAt;
    await writeRecoveryCheckpoint(checkpointPath, recovery);
    await writePrivateAtomic(
      inventoryCheckpointPath,
      `${JSON.stringify(inventoryCheckpoint, null, 2)}\n`,
    );
  }

  await rewriteNormalizedInventory(inventoryDirectory, inventoryCheckpoint);
  await loadRecoveredRecords(options.sourceKey, inventoryDirectory, recovery);
  return buildRecoverySummary(
    options.sourceKey,
    recovery,
    missingReceipts.length,
  );
}

/**
 * Build a fixed aggregate without source IDs, rows, or private paths.
 *
 * @param {RecoverySourceKey} sourceKey - Tenant identity.
 * @param {RecoveryCheckpoint} checkpoint - Durable recovery state.
 * @param {number} affectedWindows - Windows considered this invocation.
 * @returns {{
 *   sourceKey:string,
 *   affectedWindows:number,
 *   recoveredRecords:number,
 *   capturedDetails:number,
 *   terminalMissing:number,
 *   reconciliationPassed:boolean
 * }} Privacy-safe summary.
 */
function buildRecoverySummary(sourceKey, checkpoint, affectedWindows) {
  const windows = Object.values(checkpoint.windows);
  const terminalMissing = windows.reduce(
    (sum, window) => sum + window.acceptedTerminalMissingCount,
    0,
  );
  return {
    sourceKey,
    affectedWindows,
    recoveredRecords: checkpoint.recoveredListRecords.length,
    capturedDetails: checkpoint.recoveredDetailRecords.length,
    terminalMissing,
    reconciliationPassed: windows.every(
      (window) =>
        window.status === "recovered" ||
        window.status === "accepted_terminal_missing",
    ),
  };
}

/**
 * Load only recovered exact identities through idempotent existing writers.
 *
 * @param {RecoverySourceKey} sourceKey - Tenant identity.
 * @param {string} inventoryDirectory - Private inventory root.
 * @param {RecoveryCheckpoint} checkpoint - Recovered list/detail rows.
 * @returns {Promise<void>} Resolves after all non-empty idempotent loads.
 */
async function loadRecoveredRecords(sourceKey, inventoryDirectory, checkpoint) {
  if (checkpoint.recoveredListRecords.length === 0) return;
  const listPath = path.join(
    inventoryDirectory,
    "source-missing-recovered-list.private.jsonl",
  );
  const listText = renderJsonl(checkpoint.recoveredListRecords, recordIdentity);
  await writePrivateAtomic(listPath, listText);
  const digest = createHash("sha256")
    .update(listText)
    .digest("hex")
    .slice(0, 12);
  await loadPermitListToNeon({
    jobId: `broward-permits-tyler-recovery-${sourceKey.replaceAll("_", "-")}-${digest}`,
    inputPath: listPath,
    chunkSize: 100,
  });
  if (checkpoint.recoveredDetailRecords.length === 0) return;
  const detailPath = path.join(
    inventoryDirectory,
    "source-missing-recovered-details.private.jsonl",
  );
  await writePrivateAtomic(
    detailPath,
    renderJsonl(
      checkpoint.recoveredDetailRecords,
      (record) => record.record_key,
    ),
  );
  await loadBrowardPermitPilotToNeon({
    inputPath: "",
    expectedRecords: null,
    includeBcs: false,
    accelaInputPath: null,
    expectedAccelaRecords: null,
    municipalInputPaths: [detailPath],
    expectedMunicipalRecords: checkpoint.recoveredDetailRecords.length,
  });
}

/**
 * Regenerate the completed source inventory from every reconciled receipt.
 *
 * @param {string} inventoryDirectory - Private inventory root.
 * @param {Record<string, unknown>} checkpoint - Completed inventory checkpoint.
 * @returns {Promise<void>} Resolves after deterministic atomic replacement.
 */
async function rewriteNormalizedInventory(inventoryDirectory, checkpoint) {
  const completed = /** @type {Record<string, Record<string, unknown>>} */ (
    checkpoint.completedWindows
  );
  /** @type {NormalizedCityPermit[]} */
  const records = [];
  for (const receipt of Object.values(completed)) {
    const payload = requireWindowPayload(
      await readJson(requireString(receipt.linksPath, "window records path")),
      requireString(checkpoint.sourceKey, "checkpoint source"),
    );
    records.push(...payload.records);
  }
  await writePrivateAtomic(
    path.join(inventoryDirectory, "normalized-list.private.jsonl"),
    renderJsonl(records, recordIdentity),
  );
}

/**
 * Render unique, conflict-checked records in stable source-ID order.
 *
 * @template RecordValue
 * @param {readonly RecordValue[]} records - Private normalized records.
 * @param {(record:RecordValue)=>string} identity - Stable source identity.
 * @returns {string} Deterministic JSONL.
 */
function renderJsonl(records, identity) {
  const byIdentity = new Map();
  for (const record of records) {
    const key = identity(record);
    const existing = byIdentity.get(key);
    if (
      existing !== undefined &&
      JSON.stringify(existing) !== JSON.stringify(record)
    ) {
      throw new Error("Recovered Tyler records conflict by source identity");
    }
    byIdentity.set(key, record);
  }
  const sorted = [...byIdentity.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([, record]) => record);
  return sorted.length === 0
    ? ""
    : `${sorted.map((record) => JSON.stringify(record)).join("\n")}\n`;
}

/**
 * Merge newly recovered list rows into durable private checkpoint state.
 *
 * @param {RecoveryCheckpoint} checkpoint - Mutable in-process checkpoint.
 * @param {readonly NormalizedCityPermit[]} records - Exact recovered rows.
 * @returns {void}
 */
function mergeRecoveredListRecords(checkpoint, records) {
  const byIdentity = new Map(
    checkpoint.recoveredListRecords.map((record) => [
      recordIdentity(record),
      record,
    ]),
  );
  for (const record of records) {
    const key = recordIdentity(record);
    const existing = byIdentity.get(key);
    if (
      existing !== undefined &&
      JSON.stringify(existing) !== JSON.stringify(record)
    ) {
      throw new Error("Recovered Tyler list identity changed");
    }
    byIdentity.set(key, record);
  }
  checkpoint.recoveredListRecords = [...byIdentity.values()].sort(
    (left, right) => recordIdentity(left).localeCompare(recordIdentity(right)),
  );
}

/**
 * Persist raw adaptive page evidence under a private attempt directory.
 *
 * @param {string} inventoryDirectory - Private inventory root.
 * @param {string} windowKey - Existing date-window key.
 * @param {number} attempt - One-based finite recovery attempt.
 * @param {readonly import("./permit-source-adapters/tyler-civic-access.mjs").TylerDateWindowPage[]} pages
 *   Raw page-size traversals.
 * @returns {Promise<void>} Resolves after every page is durable.
 */
async function persistRawRecoveryPages(
  inventoryDirectory,
  windowKey,
  attempt,
  pages,
) {
  const directory = path.join(
    inventoryDirectory,
    "source-missing-recovery-private",
    windowKey,
    `attempt-${String(attempt)}`,
  );
  await mkdir(directory, { recursive: true, mode: 0o700 });
  for (const page of pages) {
    await writePrivateAtomic(
      path.join(
        directory,
        `page-size-${String(page.pageSize)}-${String(page.pageNumber).padStart(4, "0")}.json`,
      ),
      page.rawJson,
    );
  }
}

/**
 * Validate one completed Tyler window artifact while retaining extra fields.
 *
 * @param {Record<string, unknown>} value - Parsed private payload.
 * @param {string} sourceKey - Expected tenant key.
 * @returns {{
 *   raw:Record<string,unknown>,
 *   totalFound:number,
 *   invalidRecordCount:number,
 *   records:NormalizedCityPermit[]
 * }} Reconciled private window payload.
 */
function requireWindowPayload(value, sourceKey) {
  if (value.sourceKey !== sourceKey || !Array.isArray(value.records)) {
    throw new Error("Tyler completed window artifact is incompatible");
  }
  const records = value.records.map((record) => {
    if (!isRecord(record) || !isRecord(record.raw)) {
      throw new Error("Tyler completed window record is malformed");
    }
    recordIdentity(/** @type {NormalizedCityPermit} */ (record));
    return /** @type {NormalizedCityPermit} */ (record);
  });
  return {
    raw: value,
    totalFound: nonNegativeInteger(value.totalFound, "window total"),
    invalidRecordCount: nonNegativeInteger(
      value.invalidRecordCount ?? 0,
      "window invalid count",
    ),
    records,
  };
}

/**
 * Return the detail-compatible exact source key for a list record.
 *
 * @param {NormalizedCityPermit} record - Tyler list record.
 * @returns {string} Stable source-system and CaseId identity.
 */
function recordIdentity(record) {
  const caseId = record.raw.case_id;
  if (
    typeof record.source_system !== "string" ||
    typeof caseId !== "string" ||
    caseId.length === 0 ||
    typeof record.permit_number !== "string" ||
    record.permit_number.length === 0
  ) {
    throw new Error("Tyler recovered record lacks stable source identity");
  }
  return `${record.source_system}:${caseId}`;
}

/**
 * Read or initialize a source-bound private recovery checkpoint.
 *
 * @param {string} checkpointPath - Private recovery checkpoint path.
 * @param {RecoverySourceKey} sourceKey - Exact tenant identity.
 * @returns {Promise<RecoveryCheckpoint>} Validated durable state.
 */
async function readRecoveryCheckpoint(checkpointPath, sourceKey) {
  try {
    const parsed = await readJson(checkpointPath);
    if (
      parsed.schemaVersion !== RECOVERY_SCHEMA_VERSION ||
      parsed.sourceKey !== sourceKey ||
      !isRecord(parsed.windows) ||
      !Array.isArray(parsed.recoveredListRecords) ||
      !Array.isArray(parsed.recoveredDetailRecords) ||
      typeof parsed.updatedAt !== "string"
    ) {
      throw new Error("Tyler source-missing checkpoint is incompatible");
    }
    return /** @type {RecoveryCheckpoint} */ (parsed);
  } catch (error) {
    if (!isNodeError(error) || error.code !== "ENOENT") throw error;
    return {
      schemaVersion: RECOVERY_SCHEMA_VERSION,
      sourceKey,
      windows: {},
      recoveredListRecords: [],
      recoveredDetailRecords: [],
      updatedAt: new Date().toISOString(),
    };
  }
}

/**
 * Validate the completed inventory checkpoint's source and terminal state.
 *
 * @param {Record<string, unknown>} value - Parsed private checkpoint.
 * @param {RecoverySourceKey} sourceKey - Expected tenant identity.
 * @returns {Record<string, unknown>} Valid completed inventory checkpoint.
 */
function requireInventoryCheckpoint(value, sourceKey) {
  if (
    value.sourceKey !== sourceKey ||
    !Array.isArray(value.pendingWindows) ||
    value.pendingWindows.length !== 0 ||
    !isRecord(value.completedWindows)
  ) {
    throw new Error("Tyler recovery requires a completed matching inventory");
  }
  return value;
}

/**
 * Create an unattempted window recovery state.
 *
 * @param {number} missingCount - Explicit source-reported initial gap.
 * @returns {RecoveryWindowState} Fresh private state.
 */
function createWindowState(missingCount) {
  return {
    attemptCount: 0,
    status: "pending",
    originalMissingCount: missingCount,
    recoveredCount: 0,
    capturedDetailCount: 0,
    acceptedTerminalMissingCount: 0,
    errorClass: null,
    updatedAt: new Date().toISOString(),
  };
}

/**
 * Return a logger that exposes only fixed event names and aggregate labels.
 *
 * @returns {{
 *   info:(message:string,details?:Record<string,unknown>)=>void,
 *   warn:(message:string,details?:Record<string,unknown>)=>void,
 *   error:(message:string,details?:Record<string,unknown>)=>void
 * }} Privacy-safe adapter logger.
 */
function safeLogger() {
  const write = (
    /** @type {string} */ level,
    /** @type {string} */ message,
  ) => {
    process.stderr.write(`${JSON.stringify({ level, message })}\n`);
  };
  return {
    info: (message) => write("info", message),
    warn: (message) => write("warn", message),
    error: (message) => write("error", message),
  };
}

/**
 * Read one private JSON object without tolerating malformed values.
 *
 * @param {string} filePath - Private JSON path.
 * @returns {Promise<Record<string, unknown>>} Parsed object.
 */
async function readJson(filePath) {
  const parsed = /** @type {unknown} */ (
    JSON.parse(await readFile(filePath, "utf8"))
  );
  if (!isRecord(parsed)) throw new Error("Tyler private JSON is malformed");
  return parsed;
}

/**
 * Atomically write an owner-only private artifact.
 *
 * @param {string} filePath - Final private path.
 * @param {string} content - Complete replacement content.
 * @returns {Promise<void>} Resolves after atomic replacement.
 */
async function writePrivateAtomic(filePath, content) {
  await mkdir(path.dirname(filePath), { recursive: true, mode: 0o700 });
  const temporaryPath = `${filePath}.${String(process.pid)}.tmp`;
  await writeFile(temporaryPath, content, { encoding: "utf8", mode: 0o600 });
  await rename(temporaryPath, filePath);
}

/**
 * Persist the complete private recovery state atomically.
 *
 * @param {string} checkpointPath - Private checkpoint path.
 * @param {RecoveryCheckpoint} checkpoint - Complete recovery state.
 * @returns {Promise<void>} Resolves after atomic replacement.
 */
function writeRecoveryCheckpoint(checkpointPath, checkpoint) {
  return writePrivateAtomic(
    checkpointPath,
    `${JSON.stringify(checkpoint, null, 2)}\n`,
  );
}

/**
 * Parse a bounded integer option.
 *
 * @param {string} raw - Raw CLI value.
 * @param {string} name - Option name without dashes.
 * @param {number} minimum - Inclusive lower bound.
 * @param {number} maximum - Inclusive upper bound.
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
 * Require a non-negative safe aggregate.
 *
 * @param {unknown} value - Candidate aggregate.
 * @param {string} name - Safe field label.
 * @returns {number} Validated aggregate.
 */
function nonNegativeInteger(value, name) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`Tyler ${name} is invalid`);
  }
  return parsed;
}

/**
 * Require a non-empty private string without including it in errors.
 *
 * @param {unknown} value - Candidate string.
 * @param {string} name - Safe field label.
 * @returns {string} Non-empty string.
 */
function requireString(value, name) {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`Tyler ${name} is missing`);
  }
  return value;
}

/**
 * Narrow an unknown value to a non-array object.
 *
 * @param {unknown} value - Candidate value.
 * @returns {value is Record<string, unknown>} Whether the value is a record.
 */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * Narrow an unknown error to a Node error with a string code.
 *
 * @param {unknown} value - Candidate error.
 * @returns {value is Error & {code:string}} Whether a code is present.
 */
function isNodeError(value) {
  return (
    value instanceof Error && "code" in value && typeof value.code === "string"
  );
}

/**
 * Wait between finite recovery attempts or detail requests.
 *
 * @param {number} milliseconds - Positive cooldown duration.
 * @returns {Promise<void>} Resolves after the delay.
 */
function delay(milliseconds) {
  return new Promise((resolvePromise) => {
    setTimeout(resolvePromise, milliseconds);
  });
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  retryTylerSourceMissing(parseRecoveryOptions(process.argv.slice(2)))
    .then((summary) => {
      process.stdout.write(
        `${JSON.stringify({
          event: "broward_tyler_source_missing_recovery_finished",
          ...summary,
        })}\n`,
      );
    })
    .catch(() => {
      process.stderr.write(
        `${JSON.stringify({
          event: "broward_tyler_source_missing_recovery_failed",
          errorClass: "source_or_reconciliation_error",
        })}\n`,
      );
      process.exitCode = 1;
    });
}
