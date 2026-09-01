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
  BrowardAccelaSourceError,
  captureBrowardAccelaCsvWindow,
  createBrowardAccelaBrowser,
  readBrowardAccelaSource,
} from "./permit-source-adapters/broward-accela.mjs";

const CHECKPOINT_SCHEMA_VERSION = "oracle-node.broward-accela-csv-windows.v1";
const MAX_FAILED_SHARDS_PER_INVOCATION = 3;
const RECORD_TYPE_SHARD_SOURCE_KEYS = new Set([
  "plantation",
  "cooper-city",
  "weston",
]);
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
 * @property {number} maxPages - Hard page ceiling for list-only fallback.
 * @property {number} windowTimeoutMs - Hard wall-clock deadline per capture attempt.
 * @property {number | null} maxWindows - Optional pilot bound.
 * @property {string} outputDirectory - Private artifact root.
 *
 * @typedef {"csv_export" | "list_pages" | "direct_detail" | "no_records" | "record_type_shards" | "capped_probe"} AccelaCsvCaptureMode
 *
 * @typedef {object} AccelaCsvWindowReceipt
 * @property {string} startDate - Inclusive start.
 * @property {string} endDate - Inclusive end.
 * @property {number | null} displayedTotal - Untrusted UI total.
 * @property {boolean} displayedTotalCapped - Whether UI displayed cap 100.
 * @property {number | undefined} [recordCount] - Canonical reconciled records for new receipts.
 * @property {number | undefined} [exportedRecordCount] - Legacy/export-mode record count.
 * @property {number | undefined} [listRecordCount] - Transitional list-only record count.
 * @property {number | undefined} [excludedNonPermitCount] - Explicit non-permit source rows; absent in pre-v2 receipts means zero.
 * @property {number | undefined} [sourceRowCount] - Source observations before deduplication/exclusion.
 * @property {number | undefined} [duplicateRecordCount] - Identical repeated source identities.
 * @property {number | undefined} [pageCount] - Fully reconciled list page count.
 * @property {AccelaCsvCaptureMode | undefined} [captureMode] - Source mechanism used.
 * @property {string} recordsPath - Private normalized window artifact.
 * @property {string | undefined} [rawCsvSha256] - Exact export hash for legacy/CSV captures.
 * @property {string | undefined} [artifactSha256] - Exact normalized artifact hash for non-CSV captures.
 * @property {string} completedAt - ISO completion timestamp.
 *
 * @typedef {object} AccelaCsvRecordTypeShard
 * @property {string} key - Stable non-sensitive shard key.
 * @property {string} value - Exact checkpointed public option value.
 * @property {string} label - Exact checkpointed public option label.
 *
 * @typedef {object} AccelaCsvShardReceipt
 * @property {string} key - Stable shard key.
 * @property {string} value - Exact public option value.
 * @property {string} label - Exact public option label.
 * @property {number} recordCount - Reconciled unique records.
 * @property {number} sourceRowCount - Source rows before deduplication/exclusion.
 * @property {number} excludedNonPermitCount - Explicit non-permit rows.
 * @property {number} duplicateRecordCount - Repeated identical identities.
 * @property {number} pageCount - Fully reconciled pages.
 * @property {"csv_export" | "list_pages" | "direct_detail" | "no_records"} captureMode - Exact capture mechanism.
 * @property {string} recordsPath - Private shard artifact.
 * @property {string} artifactSha256 - Exact normalized artifact hash.
 * @property {string | undefined} [rawCsvSha256] - Exact export hash when present.
 * @property {string} completedAt - ISO completion time.
 *
 * @typedef {object} AccelaCsvShardFailure
 * @property {"timeout" | "source_cap" | "incomplete_pagination" | "source_error"} reason
 *   Aggregate-safe terminal classification for one bounded invocation.
 * @property {number} attemptCycles - Number of invocations that exhausted this shard.
 * @property {string} failedAt - ISO latest bounded failure time.
 *
 * @typedef {object} AccelaCsvShardPlan
 * @property {string} startDate - Inclusive parent start.
 * @property {string} endDate - Inclusive parent end.
 * @property {"record_type"} dimension - Exhaustive public split dimension.
 * @property {AccelaCsvRecordTypeShard[]} expectedShards - Frozen complete option set.
 * @property {Record<string, AccelaCsvShardReceipt>} completedShards - Durable shard receipts.
 * @property {Record<string, AccelaCsvShardFailure> | undefined} [failedShards]
 *   Safe failure receipts; these never count as completed coverage.
 * @property {string} createdAt - ISO plan creation time.
 *
 * @typedef {object} AccelaCsvSplitReceipt
 * @property {string} startDate - Inclusive parent start.
 * @property {string} endDate - Inclusive parent end.
 * @property {[DateWindow, DateWindow]} children - Adjacent exhaustive halves.
 * @property {string} reason - Stable split reason.
 * @property {string} completedAt - ISO split time.
 *
 * @typedef {object} AccelaCsvCheckpoint
 * @property {typeof CHECKPOINT_SCHEMA_VERSION} schemaVersion - Schema marker.
 * @property {AccelaCsvSourceKey} sourceKey - Source identity.
 * @property {string} configurationSha256 - Immutable run hash.
 * @property {DateWindow[]} pendingWindows - Remaining windows.
 * @property {Record<string, AccelaCsvWindowReceipt>} completedWindows - Receipts.
 * @property {Record<string, AccelaCsvShardPlan>} shardPlans - In-progress one-day exhaustive shard plans.
 * @property {Record<string, AccelaCsvSplitReceipt>} splitWindows - Non-terminal parent split evidence.
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
    maxPages: boundedInteger(
      values.get("max-pages") ?? "200",
      "max-pages",
      1,
      200,
    ),
    windowTimeoutMs: boundedInteger(
      values.get("window-timeout-ms") ?? "120000",
      "window-timeout-ms",
      30_000,
      300_000,
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
 * Split one inclusive source window into deterministic adjacent halves.
 *
 * @param {DateWindow} window - Multi-day parent window.
 * @returns {[DateWindow, DateWindow]} Exhaustive ordered children.
 */
export function splitAccelaCsvDateWindow(window) {
  const span = inclusiveDaySpan(window);
  if (span < 2) throw new Error("A one-day Accela CSV window cannot be split");
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
 * Run or resume one persistent CSV-export source.
 *
 * @param {AccelaCsvWindowOptions} options - Validated options.
 * @param {{
 *   now?:()=>string,
 *   wait?:(milliseconds:number)=>Promise<void>,
 *   random?:()=>number,
 *   createBrowser?:typeof createBrowardAccelaBrowser,
 *   captureWindow?:typeof captureBrowardAccelaCsvWindow
 * }} [dependencies={}] - Injectable runtime dependencies.
 * @returns {Promise<AccelaCsvRunSummary>} Reconciled run summary.
 */
export async function runAccelaCsvWindows(options, dependencies = {}) {
  const now = dependencies.now ?? (() => new Date().toISOString());
  const random = dependencies.random ?? Math.random;
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
  const maxPages = options.maxPages ?? 200;
  const windowTimeoutMs = options.windowTimeoutMs ?? 120_000;
  const source = readBrowardAccelaSource(options.sourceKey);
  const supportsRecordTypeShards = RECORD_TYPE_SHARD_SOURCE_KEYS.has(
    source.key,
  );
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
  /** @type {import("puppeteer").Browser | null} */
  let browser = null;
  let processed = 0;
  const attemptedShardKeys = new Set();
  const failedShardKeys = new Set();

  /**
   * Capture one source operation with a hard wall deadline and a newly built
   * browser after each retryable failure.
   *
   * @param {DateWindow} window - Parent date window.
   * @param {AccelaCsvRecordTypeShard | null} recordTypeShard - Optional shard.
   * @param {string} downloadDirectory - Private operation directory.
   * @returns {Promise<Awaited<ReturnType<typeof captureBrowardAccelaCsvWindow>>>}
   *   Reconciled source capture.
   */
  const captureRecoverably = async (
    window,
    recordTypeShard,
    downloadDirectory,
  ) => {
    for (let attempt = 1; attempt <= options.maxAttempts; attempt += 1) {
      if (browser === null) browser = await createBrowser(logger);
      try {
        return await promiseWithTimeout(
          captureWindow({
            browser,
            source,
            startDate: window.startDate,
            endDate: window.endDate,
            downloadDirectory,
            maxPages,
            recordTypeShard,
            stopAtCappedProbe:
              supportsRecordTypeShards && recordTypeShard === null,
            searchOutcomeTimeoutMs:
              source.key === "plantation" && recordTypeShard !== null
                ? Math.min(90_000, Math.max(30_000, windowTimeoutMs - 15_000))
                : 60_000,
            logger,
          }),
          windowTimeoutMs,
          `${source.jurisdiction} Accela capture exceeded ${String(windowTimeoutMs)}ms`,
        );
      } catch (error) {
        await closeAccelaBrowserWithinDeadline(browser);
        browser = null;
        if (!isRetryableCaptureError(error) || attempt >= options.maxAttempts) {
          throw error;
        }
        const backoffMs = retryBackoffMs(options.delayMs, attempt, random);
        logger.warn("broward_accela_csv_window_retry", {
          sourceKey: source.key,
          startDate: window.startDate,
          endDate: window.endDate,
          recordTypeShard:
            recordTypeShard === null ? null : recordTypeShard.key,
          attempt,
          backoffMs,
          error: error instanceof Error ? error.message : "Unknown error",
        });
        await wait(backoffMs);
      }
    }
    throw new Error("Accela CSV capture exhausted without a result");
  };

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
      const shardPlan = checkpoint.shardPlans[windowKey];
      if (shardPlan !== undefined) {
        const nextShard = shardPlan.expectedShards.find(
          (shard) =>
            shardPlan.completedShards[shard.key] === undefined &&
            !attemptedShardKeys.has(shard.key),
        );
        if (nextShard !== undefined) {
          const shardDirectory = path.join(
            windowDirectory,
            "record-type-shards",
            nextShard.key,
          );
          /** @type {Awaited<ReturnType<typeof captureBrowardAccelaCsvWindow>> | undefined} */
          let capture;
          try {
            capture = await captureRecoverably(
              window,
              nextShard,
              shardDirectory,
            );
          } catch (error) {
            attemptedShardKeys.add(nextShard.key);
            const priorFailure = shardPlan.failedShards?.[nextShard.key];
            const reason = classifyRecordTypeShardFailure(error);
            checkpoint = {
              ...checkpoint,
              shardPlans: {
                ...checkpoint.shardPlans,
                [windowKey]: {
                  ...shardPlan,
                  failedShards: {
                    ...shardPlan.failedShards,
                    [nextShard.key]: {
                      reason,
                      attemptCycles: (priorFailure?.attemptCycles ?? 0) + 1,
                      failedAt: now(),
                    },
                  },
                },
              },
              updatedAt: now(),
            };
            await writeCheckpoint(checkpointPath, checkpoint);
            processed += 1;
            failedShardKeys.add(nextShard.key);
            logger.warn("broward_accela_csv_record_type_shard_deferred", {
              sourceKey: source.key,
              startDate: window.startDate,
              endDate: window.endDate,
              recordTypeShard: nextShard.key,
              reason,
            });
            if (
              failedShardKeys.size >= MAX_FAILED_SHARDS_PER_INVOCATION
            ) {
              throw new Error(
                `${source.jurisdiction} Accela reached the bounded ${String(MAX_FAILED_SHARDS_PER_INVOCATION)}-shard failure limit`,
              );
            }
            continue;
          }
          if (capture === undefined) {
            throw new Error("Accela record-type capture disappeared");
          }
          const shardReceipt = await writeShardCapture({
            capture,
            shard: nextShard,
            sourceKey: source.key,
            sourceSystem: source.sourceSystem,
            directory: shardDirectory,
            completedAt: now(),
          });
          checkpoint = {
            ...checkpoint,
            shardPlans: {
              ...checkpoint.shardPlans,
              [windowKey]: {
                ...shardPlan,
                failedShards: Object.fromEntries(
                  Object.entries(shardPlan.failedShards ?? {}).filter(
                    ([key]) => key !== nextShard.key,
                  ),
                ),
                completedShards: {
                  ...shardPlan.completedShards,
                  [nextShard.key]: shardReceipt,
                },
              },
            },
            updatedAt: now(),
          };
          attemptedShardKeys.add(nextShard.key);
          await writeCheckpoint(checkpointPath, checkpoint);
          processed += 1;
          continue;
        }
        const incompleteShardCount = shardPlan.expectedShards.filter(
          (shard) => shardPlan.completedShards[shard.key] === undefined,
        ).length;
        if (incompleteShardCount > 0) {
          throw new Error(
            `${source.jurisdiction} Accela has ${String(incompleteShardCount)} unreconciled record-type shards after bounded attempts`,
          );
        }
        checkpoint = await finalizeRecordTypeShardPlan({
          checkpoint,
          checkpointPath,
          window,
          windowKey,
          plan: shardPlan,
          windowDirectory,
          completedAt: now(),
        });
        continue;
      }

      /** @type {Awaited<ReturnType<typeof captureBrowardAccelaCsvWindow>>} */
      let capture;
      try {
        capture = await captureRecoverably(window, null, windowDirectory);
      } catch (error) {
        if (inclusiveDaySpan(window) > 1 && isCompletenessFailure(error)) {
          checkpoint = await splitPendingWindow({
            checkpoint,
            checkpointPath,
            window,
            windowKey,
            reason: "unreconciled_source_result",
            completedAt: now(),
          });
          processed += 1;
          logger.warn("broward_accela_csv_window_split", {
            sourceKey: source.key,
            startDate: window.startDate,
            endDate: window.endDate,
            reason: "unreconciled_source_result",
          });
          continue;
        }
        throw error;
      }

      if (supportsRecordTypeShards && capture.displayedTotalCapped) {
        await writeCaptureArtifacts({
          capture,
          sourceKey: source.key,
          sourceSystem: source.sourceSystem,
          directory: path.join(windowDirectory, "capped-parent-evidence"),
        });
        if (inclusiveDaySpan(window) > 1) {
          checkpoint = await splitPendingWindow({
            checkpoint,
            checkpointPath,
            window,
            windowKey,
            reason: `${source.key}_displayed_total_cap`,
            completedAt: now(),
          });
          processed += 1;
          logger.warn("broward_accela_csv_window_split", {
            sourceKey: source.key,
            startDate: window.startDate,
            endDate: window.endDate,
            reason: `${source.key}_displayed_total_cap`,
          });
          continue;
        }
        const plan = createRecordTypeShardPlan(capture, window, now());
        checkpoint = {
          ...checkpoint,
          shardPlans: {
            ...checkpoint.shardPlans,
            [windowKey]: plan,
          },
          updatedAt: now(),
        };
        await writeCheckpoint(checkpointPath, checkpoint);
        processed += 1;
        logger.warn("broward_accela_csv_record_type_shards_planned", {
          sourceKey: source.key,
          startDate: window.startDate,
          endDate: window.endDate,
          shardCount: plan.expectedShards.length,
        });
        continue;
      }
      if (capture.captureMode === "capped_probe") {
        throw new Error("Non-terminal Accela capped probe escaped recovery");
      }

      const written = await writeCaptureArtifacts({
        capture,
        sourceKey: source.key,
        sourceSystem: source.sourceSystem,
        directory: windowDirectory,
      });
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
            recordCount: capture.records.length,
            exportedRecordCount:
              capture.captureMode === "csv_export"
                ? capture.records.length
                : undefined,
            listRecordCount:
              capture.captureMode === "list_pages"
                ? capture.records.length
                : undefined,
            excludedNonPermitCount: capture.excludedNonPermitCount,
            sourceRowCount: capture.sourceRowCount,
            duplicateRecordCount: capture.duplicateRecordCount,
            pageCount: capture.pageCount,
            captureMode: capture.captureMode,
            recordsPath: written.recordsPath,
            rawCsvSha256: written.rawCsvSha256,
            artifactSha256: written.artifactSha256,
            completedAt: now(),
          },
        },
        updatedAt: now(),
      };
      await writeCheckpoint(checkpointPath, checkpoint);
      processed += 1;
    }
  } finally {
    const finalBrowser = /** @type {import("puppeteer").Browser | null} */ (
      browser
    );
    if (finalBrowser !== null) {
      await closeAccelaBrowserWithinDeadline(finalBrowser);
    }
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
 * Persist one reconciled capture and all private list evidence.
 *
 * @param {object} params - Capture persistence inputs.
 * @param {Awaited<ReturnType<typeof captureBrowardAccelaCsvWindow>>} params.capture - Reconciled capture.
 * @param {string} params.sourceKey - Stable jurisdiction key.
 * @param {string} params.sourceSystem - Stable source-system identity.
 * @param {string} params.directory - Private operation directory.
 * @returns {Promise<{recordsPath:string,artifactSha256:string,rawCsvSha256:string|undefined}>}
 *   Durable artifact paths and hashes.
 */
async function writeCaptureArtifacts({
  capture,
  sourceKey,
  sourceSystem,
  directory,
}) {
  await writePrivateAtomic(
    path.join(directory, "search.html"),
    capture.rawSearchHtml,
  );
  const rawDirectory = path.join(directory, "raw-list-pages");
  for (const [index, html] of capture.rawListPages.entries()) {
    await writePrivateAtomic(
      path.join(
        rawDirectory,
        `page-${String(index + 1).padStart(4, "0")}.html`,
      ),
      html,
    );
  }
  const recordsPath = path.join(directory, "records.private.json");
  const content = `${JSON.stringify(
    {
      schemaVersion: "oracle-node.broward-accela-csv-window.v1",
      sourceKey,
      sourceSystem,
      startDate: capture.startDate,
      endDate: capture.endDate,
      displayedTotal: capture.displayedTotal,
      displayedTotalCapped: capture.displayedTotalCapped,
      captureMode: capture.captureMode,
      recordCount: capture.records.length,
      exportedRecordCount:
        capture.captureMode === "csv_export"
          ? capture.records.length
          : undefined,
      listRecordCount:
        capture.captureMode === "list_pages"
          ? capture.records.length
          : undefined,
      sourceRowCount: capture.sourceRowCount,
      excludedNonPermitCount: capture.excludedNonPermitCount,
      duplicateRecordCount: capture.duplicateRecordCount,
      pageCount: capture.pageCount,
      recordTypeShard: capture.recordTypeShard,
      records: capture.records,
    },
    null,
    2,
  )}\n`;
  await writePrivateAtomic(recordsPath, content);
  return {
    recordsPath,
    artifactSha256: createHash("sha256").update(content).digest("hex"),
    rawCsvSha256:
      capture.rawCsv.length === 0
        ? undefined
        : createHash("sha256").update(capture.rawCsv).digest("hex"),
  };
}

/**
 * Validate and persist one record-type shard before checkpointing it.
 *
 * @param {object} params - Shard persistence inputs.
 * @param {Awaited<ReturnType<typeof captureBrowardAccelaCsvWindow>>} params.capture - Reconciled shard capture.
 * @param {AccelaCsvRecordTypeShard} params.shard - Frozen expected shard.
 * @param {string} params.sourceKey - Stable source key.
 * @param {string} params.sourceSystem - Stable source-system identity.
 * @param {string} params.directory - Private shard directory.
 * @param {string} params.completedAt - ISO completion time.
 * @returns {Promise<AccelaCsvShardReceipt>} Fail-closed durable receipt.
 */
async function writeShardCapture({
  capture,
  shard,
  sourceKey,
  sourceSystem,
  directory,
  completedAt,
}) {
  if (capture.captureMode === "capped_probe") {
    throw new Error("Accela record-type shard returned a capped probe");
  }
  if (
    capture.recordTypeShard === null ||
    capture.recordTypeShard.value !== shard.value ||
    capture.recordTypeShard.label !== shard.label
  ) {
    throw new Error("Accela record-type capture does not match shard plan");
  }
  if (
    capture.sourceRowCount !==
    capture.records.length +
      capture.excludedNonPermitCount +
      capture.duplicateRecordCount
  ) {
    throw new Error("Accela record-type shard source rows do not reconcile");
  }
  const written = await writeCaptureArtifacts({
    capture,
    sourceKey,
    sourceSystem,
    directory,
  });
  return {
    key: shard.key,
    value: shard.value,
    label: shard.label,
    recordCount: capture.records.length,
    sourceRowCount: capture.sourceRowCount,
    excludedNonPermitCount: capture.excludedNonPermitCount,
    duplicateRecordCount: capture.duplicateRecordCount,
    pageCount: capture.pageCount,
    captureMode: capture.captureMode,
    recordsPath: written.recordsPath,
    artifactSha256: written.artifactSha256,
    rawCsvSha256: written.rawCsvSha256,
    completedAt,
  };
}

/**
 * Freeze the complete public record-type option set for a capped one-day
 * Accela window. The exact values, labels, and order become durable state.
 *
 * @param {Awaited<ReturnType<typeof captureBrowardAccelaCsvWindow>>} capture - Capped parent evidence.
 * @param {DateWindow} window - One-day parent.
 * @param {string} createdAt - ISO plan creation time.
 * @returns {AccelaCsvShardPlan} Deterministic empty shard plan.
 */
function createRecordTypeShardPlan(capture, window, createdAt) {
  if (inclusiveDaySpan(window) !== 1 || !capture.displayedTotalCapped) {
    throw new Error("Record-type sharding requires a capped one-day window");
  }
  /** @type {Map<string, AccelaCsvRecordTypeShard>} */
  const shards = new Map();
  for (const option of capture.availableRecordTypes) {
    if (!option.value.startsWith("Building/")) {
      throw new Error("Accela record-type option escaped Building module");
    }
    const key = `record-type-${createHash("sha256")
      .update(option.value)
      .digest("hex")
      .slice(0, 16)}`;
    const existing = shards.get(option.value);
    if (existing !== undefined) {
      throw new Error("Accela record-type options contain a duplicate");
    }
    shards.set(option.value, { key, ...option });
  }
  const expectedShards = [...shards.values()].sort(
    (left, right) =>
      left.value.localeCompare(right.value) ||
      left.label.localeCompare(right.label),
  );
  if (expectedShards.length === 0) {
    throw new Error(
      "Accela capped one-day window exposes no record-type shards",
    );
  }
  if (new Set(expectedShards.map((shard) => shard.key)).size !== shards.size) {
    throw new Error("Accela record-type shard keys conflict");
  }
  return {
    startDate: window.startDate,
    endDate: window.endDate,
    dimension: "record_type",
    expectedShards,
    completedShards: {},
    failedShards: {},
    createdAt,
  };
}

/**
 * Merge a fully checkpointed shard plan into one terminal parent receipt.
 *
 * @param {object} params - Finalization state.
 * @param {AccelaCsvCheckpoint} params.checkpoint - Current durable checkpoint.
 * @param {string} params.checkpointPath - Private checkpoint path.
 * @param {DateWindow} params.window - Pending parent window.
 * @param {string} params.windowKey - Stable parent key.
 * @param {AccelaCsvShardPlan} params.plan - Fully completed plan.
 * @param {string} params.windowDirectory - Parent private directory.
 * @param {string} params.completedAt - ISO finalization time.
 * @returns {Promise<AccelaCsvCheckpoint>} Updated persisted checkpoint.
 */
async function finalizeRecordTypeShardPlan({
  checkpoint,
  checkpointPath,
  window,
  windowKey,
  plan,
  windowDirectory,
  completedAt,
}) {
  if (
    plan.startDate !== window.startDate ||
    plan.endDate !== window.endDate ||
    plan.dimension !== "record_type"
  ) {
    throw new Error("Accela record-type shard plan parent differs");
  }
  const expectedKeys = plan.expectedShards.map((shard) => shard.key).sort();
  const completedKeys = Object.keys(plan.completedShards).sort();
  if (JSON.stringify(expectedKeys) !== JSON.stringify(completedKeys)) {
    throw new Error("Accela record-type shard coverage is incomplete");
  }
  /** @type {Map<string, BrowardAccelaCsvPermitRecord>} */
  const records = new Map();
  let sourceRowCount = 0;
  let excludedNonPermitCount = 0;
  let duplicateRecordCount = 0;
  let pageCount = 0;
  for (const shard of plan.expectedShards) {
    const receipt = plan.completedShards[shard.key];
    if (receipt === undefined) {
      throw new Error("Accela record-type shard receipt disappeared");
    }
    const content = await readFile(receipt.recordsPath, "utf8");
    if (
      createHash("sha256").update(content).digest("hex") !==
      receipt.artifactSha256
    ) {
      throw new Error("Accela record-type shard artifact hash differs");
    }
    const payload = /** @type {unknown} */ (JSON.parse(content));
    if (!isRecord(payload) || !Array.isArray(payload.records)) {
      throw new Error("Accela record-type shard artifact is malformed");
    }
    if (
      !isRecord(payload.recordTypeShard) ||
      payload.recordTypeShard.value !== shard.value ||
      payload.recordTypeShard.label !== shard.label
    ) {
      throw new Error("Accela record-type shard artifact filter differs");
    }
    if (payload.records.length !== receipt.recordCount) {
      throw new Error("Accela record-type shard receipt count differs");
    }
    for (const value of payload.records) {
      if (
        !isRecord(value) ||
        typeof value.recordKey !== "string" ||
        typeof value.recordNumber !== "string"
      ) {
        throw new Error("Accela record-type shard identity is malformed");
      }
      const record = /** @type {BrowardAccelaCsvPermitRecord} */ (value);
      const existing = records.get(record.recordKey);
      if (
        existing !== undefined &&
        JSON.stringify(existing) !== JSON.stringify(record)
      ) {
        throw new Error("Accela record-type shards contain conflicting rows");
      }
      if (existing !== undefined) duplicateRecordCount += 1;
      records.set(record.recordKey, record);
    }
    sourceRowCount += receipt.sourceRowCount;
    excludedNonPermitCount += receipt.excludedNonPermitCount;
    duplicateRecordCount += receipt.duplicateRecordCount;
    pageCount += receipt.pageCount;
  }
  const ordered = [...records.values()].sort((left, right) =>
    left.recordKey.localeCompare(right.recordKey),
  );
  if (
    sourceRowCount !==
    ordered.length + excludedNonPermitCount + duplicateRecordCount
  ) {
    throw new Error("Accela record-type parent source rows do not reconcile");
  }
  const recordsPath = path.join(windowDirectory, "records.private.json");
  const content = `${JSON.stringify(
    {
      schemaVersion: "oracle-node.broward-accela-csv-window.v1",
      sourceKey: checkpoint.sourceKey,
      startDate: window.startDate,
      endDate: window.endDate,
      displayedTotal: 100,
      displayedTotalCapped: true,
      captureMode: "record_type_shards",
      recordCount: ordered.length,
      sourceRowCount,
      excludedNonPermitCount,
      duplicateRecordCount,
      pageCount,
      shardDimension: "record_type",
      expectedShardCount: expectedKeys.length,
      completedShardCount: completedKeys.length,
      records: ordered,
    },
    null,
    2,
  )}\n`;
  await writePrivateAtomic(recordsPath, content);
  const shardPlans = { ...checkpoint.shardPlans };
  delete shardPlans[windowKey];
  const nextCheckpoint = {
    ...checkpoint,
    pendingWindows: checkpoint.pendingWindows.slice(1),
    completedWindows: {
      ...checkpoint.completedWindows,
      [windowKey]: {
        startDate: window.startDate,
        endDate: window.endDate,
        displayedTotal: 100,
        displayedTotalCapped: true,
        recordCount: ordered.length,
        excludedNonPermitCount,
        sourceRowCount,
        duplicateRecordCount,
        pageCount,
        captureMode: /** @type {"record_type_shards"} */ ("record_type_shards"),
        recordsPath,
        artifactSha256: createHash("sha256").update(content).digest("hex"),
        completedAt,
      },
    },
    shardPlans,
    updatedAt: completedAt,
  };
  await writeCheckpoint(checkpointPath, nextCheckpoint);
  return nextCheckpoint;
}

/**
 * Replace the first pending multi-day parent with exhaustive adjacent halves.
 *
 * @param {object} params - Split state.
 * @param {AccelaCsvCheckpoint} params.checkpoint - Current checkpoint.
 * @param {string} params.checkpointPath - Private checkpoint path.
 * @param {DateWindow} params.window - Current first pending parent.
 * @param {string} params.windowKey - Stable parent key.
 * @param {string} params.reason - Stable source-honesty reason.
 * @param {string} params.completedAt - ISO split time.
 * @returns {Promise<AccelaCsvCheckpoint>} Updated persisted checkpoint.
 */
async function splitPendingWindow({
  checkpoint,
  checkpointPath,
  window,
  windowKey,
  reason,
  completedAt,
}) {
  const children = splitAccelaCsvDateWindow(window);
  const nextCheckpoint = {
    ...checkpoint,
    pendingWindows: [...children, ...checkpoint.pendingWindows.slice(1)],
    splitWindows: {
      ...checkpoint.splitWindows,
      [windowKey]: {
        startDate: window.startDate,
        endDate: window.endDate,
        children,
        reason,
        completedAt,
      },
    },
    updatedAt: completedAt,
  };
  await writeCheckpoint(checkpointPath, nextCheckpoint);
  return nextCheckpoint;
}

/**
 * Persist a complete backward-compatible checkpoint atomically.
 *
 * @param {string} checkpointPath - Private checkpoint path.
 * @param {AccelaCsvCheckpoint} checkpoint - Complete durable state.
 * @returns {Promise<void>} Resolves after replacement.
 */
function writeCheckpoint(checkpointPath, checkpoint) {
  return writePrivateAtomic(
    checkpointPath,
    `${JSON.stringify(checkpoint, null, 2)}\n`,
  );
}

/**
 * Determine whether rebuilding a browser could safely recover the failure.
 *
 * @param {unknown} error - Capture failure.
 * @returns {boolean} True only for transient/source-completeness failures.
 */
function isRetryableCaptureError(error) {
  return !(
    error instanceof BrowardAccelaSourceError &&
    (error.code === "access_blocked" || error.code === "identity_mismatch")
  );
}

/**
 * Reduce one exhausted record-type failure to an allowlisted checkpoint code.
 * Raw source errors, URLs, and record values are never persisted.
 *
 * @param {unknown} error - Exhausted shard capture error.
 * @returns {AccelaCsvShardFailure["reason"]} Safe failure classification.
 */
function classifyRecordTypeShardFailure(error) {
  if (
    error instanceof BrowardAccelaSourceError &&
    error.code === "incomplete_pagination"
  ) {
    return "incomplete_pagination";
  }
  const message = error instanceof Error ? error.message : "";
  if (/capped at 100|source cap/iu.test(message)) return "source_cap";
  if (/timed out|timeout|Waiting failed/iu.test(message)) return "timeout";
  return "source_error";
}

/**
 * Identify an exhausted result that can be made safer by date splitting.
 *
 * @param {unknown} error - Final capture failure.
 * @returns {boolean} True for unproven source pagination/export coverage.
 */
function isCompletenessFailure(error) {
  return (
    error instanceof BrowardAccelaSourceError &&
    error.code === "incomplete_pagination"
  );
}

/**
 * Compute bounded exponential retry delay with positive jitter.
 *
 * @param {number} delayMs - Configured minimum inter-request delay.
 * @param {number} attempt - One-based failed attempt.
 * @param {() => number} random - Injectable random fraction.
 * @returns {number} Delay capped at 60 seconds.
 */
export function retryAccelaCsvBackoffMs(delayMs, attempt, random) {
  const fraction = random();
  if (!Number.isFinite(fraction) || fraction < 0 || fraction >= 1) {
    throw new Error("Accela retry random fraction must be in [0,1)");
  }
  const base = Math.min(
    60_000,
    Math.max(5_000, delayMs) * 2 ** Math.max(0, attempt - 1),
  );
  return Math.min(60_000, base + Math.floor(base * 0.25 * fraction));
}

/**
 * Internal alias preserving a compact call site.
 *
 * @param {number} delayMs - Minimum delay.
 * @param {number} attempt - One-based attempt.
 * @param {() => number} random - Random fraction provider.
 * @returns {number} Bounded delay.
 */
function retryBackoffMs(delayMs, attempt, random) {
  return retryAccelaCsvBackoffMs(delayMs, attempt, random);
}

/**
 * Close one browser within a fixed wall deadline. A timed-out Puppeteer close
 * can otherwise strand the worker after its source attempt already failed.
 * Only the child Chromium process owned by this capture is force-terminated.
 *
 * @param {import("puppeteer").Browser} browser - Capture-owned browser.
 * @returns {Promise<void>} Resolves after graceful close or bounded cleanup.
 */
async function closeAccelaBrowserWithinDeadline(browser) {
  const browserProcess =
    typeof browser.process === "function" ? browser.process() : null;
  try {
    await promiseWithTimeout(
      browser.close(),
      15_000,
      "Accela browser close timed out",
    );
  } catch {
    if (browserProcess !== null && browserProcess.exitCode === null) {
      browserProcess.kill("SIGKILL");
    }
  }
}

/**
 * Bound a source operation even when a page-level timeout is ignored.
 *
 * @template Result
 * @param {Promise<Result>} promise - Source operation.
 * @param {number} timeoutMs - Maximum wall time.
 * @param {string} message - Stable timeout message.
 * @returns {Promise<Result>} Result completed before the deadline.
 */
async function promiseWithTimeout(promise, timeoutMs, message) {
  /** @type {NodeJS.Timeout | undefined} */
  let timeout;
  try {
    return await Promise.race([
      promise,
      new Promise((_, rejectPromise) => {
        timeout = setTimeout(
          () => rejectPromise(new Error(message)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

/**
 * Count inclusive UTC days in one validated window.
 *
 * @param {DateWindow} window - Inclusive date window.
 * @returns {number} Positive day span.
 */
function inclusiveDaySpan(window) {
  return (
    Math.floor(
      (toMillis(window.endDate) - toMillis(window.startDate)) / 86_400_000,
    ) + 1
  );
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
      !isRecord(parsed.completedWindows) ||
      (parsed.shardPlans !== undefined && !isRecord(parsed.shardPlans)) ||
      (parsed.splitWindows !== undefined && !isRecord(parsed.splitWindows))
    ) {
      throw new Error(
        "Existing Accela CSV checkpoint does not match run configuration",
      );
    }
    return {
      .../** @type {AccelaCsvCheckpoint} */ (parsed),
      shardPlans:
        parsed.shardPlans === undefined
          ? {}
          : /** @type {Record<string, AccelaCsvShardPlan>} */ (
              parsed.shardPlans
            ),
      splitWindows:
        parsed.splitWindows === undefined
          ? {}
          : /** @type {Record<string, AccelaCsvSplitReceipt>} */ (
              parsed.splitWindows
            ),
    };
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
    shardPlans: {},
    splitWindows: {},
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
