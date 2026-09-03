#!/usr/bin/env node

/**
 * Local Accela PINELLAS date-window permit harvest.
 *
 * Writes under `downloads/pinellas/permits/<jobId>/` (gitignored). Resume with
 * `--skip-existing`. This is the unincorporated / county Building portal only —
 * municipal vendors are not harvested here.
 */

import { mkdir, readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import {
  buildPermitOutputStem,
  buildWindowKey,
  captureLeePermitDetail,
  consoleLogger,
  createBrowser,
  searchLeePermitParcel,
  searchLeePermitWindow,
  safeKeyPart,
} from "../workflow/lambdas/permit-harvest-worker/lee-accela.mjs";
import {
  PINELLAS_DEFAULT_START_DATE,
  PINELLAS_PORTAL_URL,
  PINELLAS_RECORD_NUMBER_PATTERN,
  PINELLAS_SPLIT_THRESHOLD,
  createAccelaDateWindows,
  shouldSplitAccelaWindow,
  splitAccelaWindow,
  todayIsoDate,
} from "./pinellas/accela-pinellas.mjs";

/**
 * @typedef {import("../workflow/lambdas/permit-harvest-worker/lee-accela.mjs").PermitLink} PermitLink
 * @typedef {import("../workflow/lambdas/permit-harvest-worker/lee-accela.mjs").PermitSearchResult} PermitSearchResult
 * @typedef {import("./pinellas/accela-pinellas.mjs").DateWindow} DateWindow
 */

/**
 * @typedef {object} HarvestCliOptions
 * @property {string} jobId Stable job id used in the output directory.
 * @property {string} startDate Inclusive ISO start.
 * @property {string} endDate Inclusive ISO end.
 * @property {number} windowDays Initial list-window size.
 * @property {number} concurrency Parallel CapDetail captures per window.
 * @property {number} settleMs Extra wait after a detail page shows the record number.
 * @property {number} splitThreshold Accela list cap that forces a split.
 * @property {number} maxPages Max result pages on a terminal window.
 * @property {number} maxDetails 0 means unlimited detail captures this process.
 * @property {string} outDirectory Output root (jobId appended).
 * @property {boolean} skipExisting Skip details whose JSON already exists.
 * @property {boolean} probe One short window, one detail, then exit.
 * @property {string | null} parcel Optional property-first STRAP search.
 */

const DEFAULT_OUT_DIRECTORY = "downloads/pinellas/permits";

/**
 * @param {readonly string[]} argv Args after the script path.
 * @returns {HarvestCliOptions} Parsed flags.
 */
export function parseCliOptions(argv) {
  /** @type {Map<string, string>} */
  const values = new Map();
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === undefined || token.startsWith("--") === false) continue;
    const key = token.slice(2);
    const next = argv[index + 1];
    if (next !== undefined && next.startsWith("--") === false) {
      values.set(key, next);
      index += 1;
    } else {
      values.set(key, "true");
    }
  }
  const probe = values.get("probe") === "true";
  const pilot = values.get("pilot") === "true";
  const today = todayIsoDate();
  let startDate = values.get("start-date") ?? PINELLAS_DEFAULT_START_DATE;
  let endDate = values.get("end-date") ?? today;
  let maxDetails = parsePositiveInteger(
    "max-details",
    values.get("max-details"),
    0,
  );
  if (probe) {
    startDate = addIsoDays(today, -2);
    endDate = today;
    maxDetails = 1;
  } else if (pilot) {
    startDate = addIsoDays(today, -13);
    endDate = today;
  }
  return {
    jobId:
      values.get("job-id") ??
      (probe
        ? `pinellas-accela-probe-${today.replaceAll("-", "")}`
        : pilot
          ? `pinellas-accela-pilot-${today.replaceAll("-", "")}`
          : `pinellas-accela-full-${today.replaceAll("-", "")}`),
    startDate,
    endDate,
    windowDays: parsePositiveInteger(
      "window-days",
      values.get("window-days"),
      probe || pilot ? 14 : 1,
    ),
    concurrency: parsePositiveInteger(
      "concurrency",
      values.get("concurrency"),
      3,
    ),
    settleMs: parseNonNegativeInteger(
      "settle-ms",
      values.get("settle-ms"),
      250,
    ),
    splitThreshold: parsePositiveInteger(
      "split-threshold",
      values.get("split-threshold"),
      PINELLAS_SPLIT_THRESHOLD,
    ),
    maxPages: parsePositiveInteger("max-pages", values.get("max-pages"), 200),
    maxDetails,
    outDirectory: values.get("out-dir") ?? DEFAULT_OUT_DIRECTORY,
    skipExisting: values.get("skip-existing") !== "false",
    probe,
    parcel: values.get("parcel") ?? null,
  };
}

/**
 * @param {string} name Flag name.
 * @param {string | undefined} value Raw value.
 * @param {number} fallback Default.
 * @returns {number} Positive integer.
 */
function parsePositiveInteger(name, value, fallback) {
  if (value === undefined) return fallback;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`--${name} must be a positive integer`);
  }
  return parsed;
}

/**
 * @param {string} name Flag name.
 * @param {string | undefined} value Raw value.
 * @param {number} fallback Default, including 0.
 * @returns {number} Non-negative integer.
 */
function parseNonNegativeInteger(name, value, fallback) {
  if (value === undefined) return fallback;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`--${name} must be a non-negative integer`);
  }
  return parsed;
}

/**
 * Run async work over items with a bounded worker pool.
 *
 * @template T
 * @template R
 * @param {readonly T[]} items Inputs.
 * @param {number} concurrency Worker count.
 * @param {(item: T) => Promise<R>} mapper Worker function.
 * @returns {Promise<R[]>} Results in input order.
 */
export async function mapWithConcurrency(items, concurrency, mapper) {
  if (items.length === 0) return [];
  const workerCount = Math.min(Math.max(1, concurrency), items.length);
  /** @type {R[]} */
  const results = new Array(items.length);
  let nextIndex = 0;
  /**
   * @returns {Promise<void>}
   */
  async function worker() {
    while (true) {
      const index = nextIndex;
      nextIndex += 1;
      if (index >= items.length) return;
      const item = items[index];
      if (item === undefined) return;
      results[index] = await mapper(item);
    }
  }
  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  return results;
}

/**
 * @param {string} isoDate YYYY-MM-DD.
 * @param {number} days Signed day offset.
 * @returns {string} New ISO date.
 */
function addIsoDays(isoDate, days) {
  const millis = Date.parse(`${isoDate}T00:00:00Z`) + days * 86400000;
  return new Date(millis).toISOString().slice(0, 10);
}

/**
 * @param {string} dir Directory that must exist.
 * @returns {Promise<void>}
 */
async function ensureDir(dir) {
  await mkdir(dir, { recursive: true });
}

/**
 * @param {object} params Paths.
 * @param {string} params.jobDir Job output directory.
 * @param {PermitLink} params.permit Permit link.
 * @returns {{ jsonPath: string, htmlPath: string }} Artifact paths.
 */
function detailPaths({ jobDir, permit }) {
  const stem = buildPermitOutputStem(permit);
  return {
    jsonPath: path.join(jobDir, "extracted", `${stem}.json`),
    htmlPath: path.join(jobDir, "raw", `${stem}.html`),
  };
}

/**
 * @param {string} jobDir Job output directory.
 * @param {string} windowKey Accela window key.
 * @returns {{ terminalPath: string, splitPath: string }} Persisted window paths.
 */
export function windowArtifactPaths(jobDir, windowKey) {
  return {
    terminalPath: path.join(jobDir, "windows", `${windowKey}.json`),
    splitPath: path.join(jobDir, "windows", `${windowKey}.split.json`),
  };
}

/**
 * True when Puppeteer/Chrome is gone and remaining Accela windows cannot run.
 *
 * @param {unknown} error Thrown value.
 * @returns {boolean} Whether harvest should stop instead of skipping windows.
 */
export function isBrowserDisconnectedError(error) {
  const message = error instanceof Error ? error.message : String(error);
  return /connection closed|target closed|session closed|browser has disconnected|protocol error/i.test(
    message,
  );
}

/**
 * @param {object} params Heartbeat fields.
 * @param {string} params.jobDir Job directory.
 * @param {HarvestCliOptions} params.options CLI options.
 * @param {string} params.phase running | complete | failed
 * @param {number} params.windowCount Windows searched this process.
 * @param {number} params.splitCount Splits this process.
 * @param {number} params.detailCount Details written this process.
 * @param {number} params.startedAt Epoch ms.
 * @param {number} params.queueRemaining Windows still queued.
 * @param {string | null} params.lastWindowKey Last window key touched.
 * @returns {Promise<void>}
 */
async function writeHarvestStatus({
  jobDir,
  options,
  phase,
  windowCount,
  splitCount,
  detailCount,
  startedAt,
  queueRemaining,
  lastWindowKey,
}) {
  const summary = {
    event:
      phase === "complete"
        ? "pinellas_accela_harvest_complete"
        : "pinellas_accela_harvest_status",
    phase,
    county: "pinellas",
    agency: "PINELLAS",
    jobId: options.jobId,
    startDate: options.startDate,
    endDate: options.endDate,
    windowCount,
    splitCount,
    detailCount,
    queueRemaining,
    lastWindowKey,
    elapsedMs: Date.now() - startedAt,
    jobDir,
    updatedAt: new Date().toISOString(),
  };
  await writeFile(
    path.join(jobDir, "status.json"),
    `${JSON.stringify(summary, null, 2)}\n`,
  );
}

/**
 * @param {HarvestCliOptions} options CLI options.
 * @param {string} repoRoot oracle-node root.
 * @returns {Promise<{ jobDir: string, windowCount: number, detailCount: number, splitCount: number }>}
 *   Harvest totals.
 */
export async function runPinellasPermitHarvest(options, repoRoot) {
  process.env.CHROME_EXECUTABLE_PATH ??= "/usr/local/bin/google-chrome";
  const jobDir = path.resolve(repoRoot, options.outDirectory, options.jobId);
  await ensureDir(path.join(jobDir, "windows"));
  await ensureDir(path.join(jobDir, "extracted"));
  await ensureDir(path.join(jobDir, "raw"));
  await ensureDir(path.join(jobDir, "status"));
  const startedAt = Date.now();
  await writeHarvestStatus({
    jobDir,
    options,
    phase: "running",
    windowCount: 0,
    splitCount: 0,
    detailCount: 0,
    startedAt,
    queueRemaining: options.parcel !== null ? 1 : 0,
    lastWindowKey: null,
  });

  const browser = await createBrowser(consoleLogger);
  let windowCount = 0;
  let detailCount = 0;
  let splitCount = 0;
  /** @type {string | null} */
  let lastWindowKey = null;

  try {
    if (options.parcel !== null) {
      const parcelResult = await searchLeePermitParcel({
        browser,
        parcelIdentifier: options.parcel,
        portalUrl: PINELLAS_PORTAL_URL,
        maxPages: options.maxPages,
        recordNumberPattern: PINELLAS_RECORD_NUMBER_PATTERN,
        logger: consoleLogger,
      });
      await writeFile(
        path.join(
          jobDir,
          "windows",
          `${safeKeyPart(parcelResult.searchKey)}.json`,
        ),
        `${JSON.stringify(
          {
            ...parcelResult,
            pages: parcelResult.pages.map((page) => ({
              pageNumber: page.pageNumber,
              url: page.url,
              resultSummary: page.resultSummary,
              htmlBytes: page.html.length,
            })),
          },
          null,
          2,
        )}\n`,
      );
      detailCount += await captureDetails({
        browser,
        jobDir,
        permits: parcelResult.permits,
        options,
        alreadyCaptured: detailCount,
      });
      windowCount = 1;
    } else {
      /** @type {DateWindow[]} */
      const queue = createAccelaDateWindows(
        options.startDate,
        options.endDate,
        options.windowDays,
      );
      /** @type {Map<string, number>} */
      const windowFailures = new Map();
      while (queue.length > 0) {
        const window = queue.shift();
        if (window === undefined) break;
        const windowKey = buildWindowKey(window.startDate, window.endDate);
        lastWindowKey = windowKey;
        const { terminalPath, splitPath } = windowArtifactPaths(
          jobDir,
          windowKey,
        );
        if (options.skipExisting && existsSync(terminalPath)) {
          continue;
        }
        if (options.skipExisting && existsSync(splitPath)) {
          const saved = JSON.parse(await readFile(splitPath, "utf8"));
          if (
            saved?.left?.startDate !== undefined &&
            saved?.right?.startDate !== undefined
          ) {
            queue.unshift(saved.right);
            queue.unshift(saved.left);
            continue;
          }
        }
        /** @type {import("../workflow/lambdas/permit-harvest-worker/lee-accela.mjs").PermitSearchResult | null} */
        let searchResult = null;
        try {
          searchResult = await searchLeePermitWindow({
            browser,
            startDate: window.startDate,
            endDate: window.endDate,
            portalUrl: PINELLAS_PORTAL_URL,
            maxPages: options.maxPages,
            stopAfterFirstPageWhenTotalAtLeast: options.splitThreshold,
            recordNumberPattern: PINELLAS_RECORD_NUMBER_PATTERN,
            logger: consoleLogger,
          });
        } catch (error) {
          const message =
            error instanceof Error ? error.message : String(error);
          const failCount = (windowFailures.get(windowKey) ?? 0) + 1;
          windowFailures.set(windowKey, failCount);
          console.log(
            JSON.stringify({
              event: "pinellas_accela_window_failed",
              windowKey,
              failCount,
              message,
            }),
          );
          await writeHarvestStatus({
            jobDir,
            options,
            phase: isBrowserDisconnectedError(error) ? "failed" : "running",
            windowCount,
            splitCount,
            detailCount,
            startedAt,
            queueRemaining: queue.length,
            lastWindowKey,
          });
          if (isBrowserDisconnectedError(error)) {
            throw error instanceof Error ? error : new Error(message);
          }
          if (failCount < 3) {
            queue.push(window);
          }
          continue;
        }
        windowCount += 1;
        const mustSplit =
          searchResult.truncatedForSplit === true ||
          shouldSplitAccelaWindow({
            startDate: window.startDate,
            endDate: window.endDate,
            reportedTotal: searchResult.reportedTotal,
            splitThreshold: options.splitThreshold,
          });
        if (mustSplit === false) {
          detailCount += await captureDetails({
            browser,
            jobDir,
            permits: searchResult.permits,
            options,
            alreadyCaptured: detailCount,
          });
        }
        if (mustSplit) {
          splitCount += 1;
          const [left, right] = splitAccelaWindow(
            window.startDate,
            window.endDate,
          );
          await writeFile(
            splitPath,
            `${JSON.stringify(
              {
                windowKey,
                reportedTotal: searchResult.reportedTotal,
                truncatedForSplit: searchResult.truncatedForSplit === true,
                left,
                right,
              },
              null,
              2,
            )}\n`,
          );
          queue.unshift(right);
          queue.unshift(left);
          console.log(
            JSON.stringify({
              event: "pinellas_accela_window_split",
              windowKey,
              reportedTotal: searchResult.reportedTotal,
              left,
              right,
            }),
          );
        } else {
          await writeFile(
            terminalPath,
            `${JSON.stringify(
              {
                ...searchResult,
                pages: searchResult.pages.map((page) => ({
                  pageNumber: page.pageNumber,
                  url: page.url,
                  resultSummary: page.resultSummary,
                  htmlBytes: page.html.length,
                })),
              },
              null,
              2,
            )}\n`,
          );
        }
        await writeHarvestStatus({
          jobDir,
          options,
          phase: "running",
          windowCount,
          splitCount,
          detailCount,
          startedAt,
          queueRemaining: queue.length,
          lastWindowKey,
        });
        if (options.maxDetails > 0 && detailCount >= options.maxDetails) {
          break;
        }
      }
    }
  } finally {
    await browser.close().catch(() => undefined);
  }

  const summary = {
    event: "pinellas_accela_harvest_complete",
    phase: "complete",
    county: "pinellas",
    agency: "PINELLAS",
    jobId: options.jobId,
    startDate: options.startDate,
    endDate: options.endDate,
    windowCount,
    splitCount,
    detailCount,
    queueRemaining: 0,
    lastWindowKey,
    elapsedMs: Date.now() - startedAt,
    jobDir,
    updatedAt: new Date().toISOString(),
  };
  await writeFile(
    path.join(jobDir, "status.json"),
    `${JSON.stringify(summary, null, 2)}\n`,
  );
  console.log(JSON.stringify(summary));
  return { jobDir, windowCount, detailCount, splitCount };
}

/**
 * @param {object} params Capture inputs.
 * @param {import("puppeteer").Browser} params.browser Browser.
 * @param {string} params.jobDir Job directory.
 * @param {PermitLink[]} params.permits Links to capture.
 * @param {HarvestCliOptions} params.options CLI options.
 * @param {number} params.alreadyCaptured Details written earlier in this process.
 * @returns {Promise<number>} Newly captured detail count.
 */
async function captureDetails({
  browser,
  jobDir,
  permits,
  options,
  alreadyCaptured,
}) {
  /** @type {PermitLink[]} */
  const pending = [];
  for (const permit of permits) {
    if (
      options.maxDetails > 0 &&
      alreadyCaptured + pending.length >= options.maxDetails
    ) {
      break;
    }
    const paths = detailPaths({ jobDir, permit });
    if (options.skipExisting && existsSync(paths.jsonPath)) {
      continue;
    }
    pending.push(permit);
  }
  if (pending.length === 0) return 0;

  /** @type {number} */
  let captured = 0;
  await mapWithConcurrency(pending, options.concurrency, async (permit) => {
    const paths = detailPaths({ jobDir, permit });
    try {
      const { html, extraction } = await captureLeePermitDetail({
        browser,
        permit,
        logger: consoleLogger,
        settleMs: options.settleMs,
      });
      await writeFile(paths.htmlPath, html);
      await writeFile(
        paths.jsonPath,
        `${JSON.stringify(
          { ...extraction, source: "pinellas-county-accela" },
          null,
          2,
        )}\n`,
      );
    } catch (error) {
      if (isBrowserDisconnectedError(error)) {
        throw error instanceof Error ? error : new Error(String(error));
      }
      console.log(
        JSON.stringify({
          event: "pinellas_accela_detail_failed",
          recordNumber: permit.recordNumber,
          url: permit.url,
          message: error instanceof Error ? error.message : String(error),
        }),
      );
      return;
    }
    captured += 1;
    console.log(
      JSON.stringify({
        event: "pinellas_accela_detail_written",
        recordNumber: permit.recordNumber,
        jsonPath: paths.jsonPath,
      }),
    );
  });
  return captured;
}

/**
 * @returns {Promise<void>}
 */
async function main() {
  const repoRoot = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
  const options = parseCliOptions(process.argv.slice(2));
  await runPinellasPermitHarvest(options, repoRoot);
}

function isInvokedDirectly() {
  const entry = process.argv[1];
  if (entry === undefined) return false;
  try {
    return import.meta.url === pathToFileURL(entry).href;
  } catch {
    return false;
  }
}

if (isInvokedDirectly()) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
