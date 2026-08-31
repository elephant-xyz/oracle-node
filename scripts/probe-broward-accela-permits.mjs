#!/usr/bin/env node
// @ts-check

import { mkdir, rename, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";

import {
  BROWARD_ACCELA_SOURCES,
  BrowardAccelaSourceError,
  buildBrowardAccelaPermitStem,
  buildBrowardAccelaSearchKey,
  captureBrowardAccelaPermitDetail,
  createBrowardAccelaBrowser,
  isBrowardAccelaRoofPermitCandidate,
  normalizeBrowardPermitFolio,
  readBrowardAccelaCheckpoint,
  readBrowardAccelaSource,
  searchBrowardAccelaParcel,
  writeBrowardAccelaCheckpoint,
} from "./permit-source-adapters/broward-accela.mjs";

/**
 * @typedef {import("./permit-source-adapters/broward-accela.mjs").BrowardAccelaJurisdiction} BrowardAccelaJurisdiction
 */

/**
 * @typedef {import("./permit-source-adapters/broward-accela.mjs").BrowardAccelaPermitRecord} BrowardAccelaPermitRecord
 */

/**
 * @typedef {import("./permit-source-adapters/broward-accela.mjs").BrowardAccelaCheckpointTarget} BrowardAccelaCheckpointTarget
 */

/**
 * @typedef {object} BrowardAccelaProbeTarget
 * @property {BrowardAccelaJurisdiction} jurisdictionKey - Jurisdiction-specific adapter key.
 * @property {string} parcelIdentifier - Exact canonical Broward folio.
 */

/**
 * @typedef {object} BrowardAccelaCliOptions
 * @property {readonly BrowardAccelaProbeTarget[]} targets - Bounded jurisdiction/folio targets.
 * @property {boolean} isCuratedPilot - Whether the checked-in one-folio-per-jurisdiction pilot was selected.
 * @property {string} outputPath - Local mode-0600 normalized JSONL destination.
 * @property {string} summaryPath - Local mode-0600 outcome/provenance JSON destination.
 * @property {string} checkpointPath - Local atomic checkpoint used for target/detail resume.
 * @property {string} captureDirectory - Local private raw list/detail HTML root.
 * @property {number} maxPages - Maximum result pages per jurisdiction/folio search.
 * @property {number} maxDetails - Maximum detail pages per jurisdiction/folio search.
 * @property {number} targetDelayMs - Minimum delay between jurisdiction/folio searches.
 * @property {number} detailDelayMs - Minimum delay between detail requests.
 * @property {boolean} roofOnly - Whether search candidates must explicitly identify roofing.
 */

const DEFAULT_OUTPUT_PATH =
  "downloads/broward/accela/normalized-permits.private.jsonl";
const DEFAULT_SUMMARY_PATH =
  "downloads/broward/accela/probe-summary.private.json";
const DEFAULT_CHECKPOINT_PATH =
  "downloads/broward/accela/probe-checkpoint.private.json";
const DEFAULT_CAPTURE_DIRECTORY =
  "downloads/broward/accela/raw-private-captures";
const MAX_TARGETS_PER_JURISDICTION = 2;
const MAX_DETAILS_HARD_LIMIT = 25;

/**
 * @type {Logger}
 */
const consoleLogger = {
  info(message, details = {}) {
    process.stderr.write(
      `${JSON.stringify({ level: "info", message, ...details })}\n`,
    );
  },
  warn(message, details = {}) {
    process.stderr.write(
      `${JSON.stringify({ level: "warn", message, ...details })}\n`,
    );
  },
  error(message, details = {}) {
    process.stderr.write(
      `${JSON.stringify({ level: "error", message, ...details })}\n`,
    );
  },
};

/**
 * @typedef {object} Logger
 * @property {(message: string, details?: Record<string, unknown>) => void} info - Emit informational JSON.
 * @property {(message: string, details?: Record<string, unknown>) => void} warn - Emit warning JSON.
 * @property {(message: string, details?: Record<string, unknown>) => void} error - Emit error JSON.
 */

const USAGE = `Usage:
  node scripts/probe-broward-accela-permits.mjs --pilot [options]

  node scripts/probe-broward-accela-permits.mjs \\
    --target <jurisdiction-key>:<12-character-folio> [--target ...] [options]

Jurisdiction keys:
  hollywood, plantation, fort-lauderdale, cooper-city, weston

Options:
  --output <path>             Normalized private JSONL. Default: ${DEFAULT_OUTPUT_PATH}
  --summary <path>            Probe outcome/provenance JSON. Default: ${DEFAULT_SUMMARY_PATH}
  --checkpoint <path>         Atomic resume checkpoint. Default: ${DEFAULT_CHECKPOINT_PATH}
  --capture-dir <path>        Raw private HTML captures. Default: ${DEFAULT_CAPTURE_DIRECTORY}
  --max-pages <1-10>          Result pages per target. Default: 5
  --max-details <1-25>        Detail pages per target. Default: 20
  --target-delay-ms <>=1000>  Delay between targets. Default: 1500
  --detail-delay-ms <>=250>   Delay between details. Default: 300
  --roof-only                 Detail only list rows explicitly marked roofing
  --help                      Show this text.

Safety and scope:
  - --pilot uses one already-validated Broward appraisal folio in each city.
  - Custom mode accepts at most two exact folios per jurisdiction.
  - Folios remain 12-character alphanumeric strings; letters are retained.
  - Public anonymous parcel search only; no login, CAPTCHA handling, or bypass.
  - No retries, AWS, database load, queue, IPFS, publication, or full harvest.
  - Hollywood Accela and the official 1988-present legacy address source retain
    separate source identities; this command queries current Accela only.
`;

/**
 * Parse a required value following a CLI flag.
 *
 * @param {readonly string[]} args - Raw arguments after the script path.
 * @param {number} index - Current argument index.
 * @param {string} flag - Flag used in diagnostics.
 * @returns {{ value: string, nextIndex: number }} Value and consumed index.
 */
function readFollowingValue(args, index, flag) {
  const value = args[index + 1];
  if (value === undefined || value.startsWith("--")) {
    throw new Error(`${flag} requires a value`);
  }
  return { value, nextIndex: index + 1 };
}

/**
 * Parse one bounded integer CLI value.
 *
 * @param {string} value - Raw integer text.
 * @param {string} flag - Flag used in diagnostics.
 * @param {number} minimum - Inclusive minimum.
 * @param {number} maximum - Inclusive maximum.
 * @returns {number} Validated integer.
 */
function parseBoundedInteger(value, flag, minimum, maximum) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(
      `${flag} must be an integer between ${String(minimum)} and ${String(maximum)}`,
    );
  }
  return parsed;
}

/**
 * Parse `jurisdiction:folio` without splitting the folio or coercing it to a
 * number.
 *
 * @param {string} value - Raw target text.
 * @returns {BrowardAccelaProbeTarget} Validated source target.
 */
function parseTarget(value) {
  const separatorIndex = value.indexOf(":");
  if (separatorIndex <= 0 || separatorIndex === value.length - 1) {
    throw new Error(
      `--target must use jurisdiction-key:folio syntax: ${value}`,
    );
  }
  const source = readBrowardAccelaSource(value.slice(0, separatorIndex));
  const parcelIdentifier = normalizeBrowardPermitFolio(
    value.slice(separatorIndex + 1),
  );
  return { jurisdictionKey: source.key, parcelIdentifier };
}

/**
 * Build the checked-in bounded pilot from the first validated appraisal folio
 * assigned to each Accela jurisdiction.
 *
 * @returns {readonly BrowardAccelaProbeTarget[]} Five targets, one per source.
 */
function buildCuratedPilotTargets() {
  return Object.values(BROWARD_ACCELA_SOURCES).map((source) => {
    const parcelIdentifier = source.pilotParcels[0];
    if (parcelIdentifier === undefined) {
      throw new Error(`Missing pilot parcel for ${source.key}`);
    }
    return {
      jurisdictionKey: source.key,
      parcelIdentifier: normalizeBrowardPermitFolio(parcelIdentifier),
    };
  });
}

/**
 * Validate uniqueness and enforce the hard two-folio-per-jurisdiction limit.
 *
 * @param {readonly BrowardAccelaProbeTarget[]} targets - Candidate targets.
 * @returns {readonly BrowardAccelaProbeTarget[]} Validated targets.
 */
function validateTargets(targets) {
  const seen = new Set();
  /** @type {Map<BrowardAccelaJurisdiction, number>} */
  const counts = new Map();
  for (const target of targets) {
    const key = `${target.jurisdictionKey}:${target.parcelIdentifier}`;
    if (seen.has(key))
      throw new Error(`Duplicate Broward Accela target: ${key}`);
    seen.add(key);
    const count = (counts.get(target.jurisdictionKey) ?? 0) + 1;
    if (count > MAX_TARGETS_PER_JURISDICTION) {
      throw new Error(
        `${target.jurisdictionKey} exceeds the approved maximum of ${String(MAX_TARGETS_PER_JURISDICTION)} folios`,
      );
    }
    counts.set(target.jurisdictionKey, count);
  }
  if (targets.length === 0) throw new Error("At least one target is required");
  return targets;
}

/**
 * Parse the bounded local-only Broward Accela probe CLI.
 *
 * @param {readonly string[]} args - Arguments after the script path.
 * @returns {BrowardAccelaCliOptions | null} Options, or `null` for help.
 */
export function parseOptions(args) {
  /** @type {BrowardAccelaProbeTarget[]} */
  const explicitTargets = [];
  let pilot = false;
  let outputPath = DEFAULT_OUTPUT_PATH;
  let summaryPath = DEFAULT_SUMMARY_PATH;
  let checkpointPath = DEFAULT_CHECKPOINT_PATH;
  let captureDirectory = DEFAULT_CAPTURE_DIRECTORY;
  let maxPages = 5;
  let maxDetails = 20;
  let targetDelayMs = 1_500;
  let detailDelayMs = 300;
  let roofOnly = false;

  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--help" || argument === "-h") return null;
    if (argument === "--pilot") {
      pilot = true;
      continue;
    }
    if (argument === "--roof-only") {
      roofOnly = true;
      continue;
    }
    if (argument === "--target") {
      const parsed = readFollowingValue(args, index, argument);
      explicitTargets.push(parseTarget(parsed.value));
      index = parsed.nextIndex;
      continue;
    }
    if (argument.startsWith("--target=")) {
      explicitTargets.push(parseTarget(argument.slice("--target=".length)));
      continue;
    }
    const acceptedValueFlags = new Set([
      "--output",
      "--summary",
      "--checkpoint",
      "--capture-dir",
      "--max-pages",
      "--max-details",
      "--target-delay-ms",
      "--detail-delay-ms",
    ]);
    const equalsIndex = argument.indexOf("=");
    const flag = equalsIndex < 0 ? argument : argument.slice(0, equalsIndex);
    if (!acceptedValueFlags.has(flag)) {
      throw new Error(`Unknown option: ${argument}`);
    }
    const parsed =
      equalsIndex < 0
        ? readFollowingValue(args, index, flag)
        : { value: argument.slice(equalsIndex + 1), nextIndex: index };
    if (equalsIndex < 0) index = parsed.nextIndex;
    if (flag === "--output") outputPath = parsed.value.trim();
    if (flag === "--summary") summaryPath = parsed.value.trim();
    if (flag === "--checkpoint") checkpointPath = parsed.value.trim();
    if (flag === "--capture-dir") captureDirectory = parsed.value.trim();
    if (flag === "--max-pages") {
      maxPages = parseBoundedInteger(parsed.value, flag, 1, 10);
    }
    if (flag === "--max-details") {
      maxDetails = parseBoundedInteger(
        parsed.value,
        flag,
        1,
        MAX_DETAILS_HARD_LIMIT,
      );
    }
    if (flag === "--target-delay-ms") {
      targetDelayMs = parseBoundedInteger(parsed.value, flag, 1_000, 60_000);
    }
    if (flag === "--detail-delay-ms") {
      detailDelayMs = parseBoundedInteger(parsed.value, flag, 250, 60_000);
    }
  }
  if (pilot === explicitTargets.length > 0) {
    throw new Error("Choose exactly one input mode: --pilot or --target");
  }
  const paths = [outputPath, summaryPath, checkpointPath, captureDirectory];
  if (paths.some((value) => value.length === 0)) {
    throw new Error(
      "Output, summary, checkpoint, and capture paths cannot be empty",
    );
  }
  if (new Set([outputPath, summaryPath, checkpointPath]).size !== 3) {
    throw new Error("Output, summary, and checkpoint must use different paths");
  }
  return {
    targets: validateTargets(
      pilot ? buildCuratedPilotTargets() : explicitTargets,
    ),
    isCuratedPilot: pilot,
    outputPath,
    summaryPath,
    checkpointPath,
    captureDirectory,
    maxPages,
    maxDetails,
    targetDelayMs,
    detailDelayMs,
    roofOnly,
  };
}

/**
 * Write one mode-0600 local artifact atomically after creating its parent.
 *
 * @param {string} outputPath - Local private artifact path.
 * @param {string} content - Complete UTF-8 content.
 * @returns {Promise<void>} Resolves after atomic replacement.
 */
async function writePrivateArtifact(outputPath, content) {
  await mkdir(dirname(outputPath), { recursive: true });
  const temporaryPath = `${outputPath}.${String(process.pid)}.tmp`;
  await writeFile(temporaryPath, content, {
    encoding: "utf8",
    mode: 0o600,
  });
  await rename(temporaryPath, outputPath);
}

/**
 * Write a raw mode-0600 HTML capture without exposing it through stdout.
 *
 * @param {string} capturePath - Raw capture path.
 * @param {string} html - Complete source HTML.
 * @returns {Promise<void>} Resolves after the private capture is written.
 */
async function writeRawCapture(capturePath, html) {
  await mkdir(dirname(capturePath), { recursive: true });
  await writeFile(capturePath, html, {
    encoding: "utf8",
    mode: 0o600,
  });
}

/**
 * Render normalized permit records as deterministic deduplicated JSONL.
 * Conflicting payloads for the same jurisdiction-scoped idempotency key fail
 * closed instead of selecting one silently.
 *
 * @param {readonly BrowardAccelaPermitRecord[]} records - Normalized records.
 * @returns {string} Stable newline-terminated JSONL, or an empty string.
 */
export function renderBrowardAccelaPermitJsonl(records) {
  /** @type {Map<string, BrowardAccelaPermitRecord>} */
  const byKey = new Map();
  for (const record of records) {
    const existing = byKey.get(record.idempotencyKey);
    if (
      existing !== undefined &&
      JSON.stringify(existing) !== JSON.stringify(record)
    ) {
      throw new Error(
        `Conflicting Broward Accela record: ${record.idempotencyKey}`,
      );
    }
    byKey.set(record.idempotencyKey, record);
  }
  return [...byKey.values()]
    .sort(
      (left, right) =>
        left.sourceSystem.localeCompare(right.sourceSystem) ||
        left.recordNumber.localeCompare(right.recordNumber),
    )
    .map((record) => JSON.stringify(record))
    .join("\n")
    .concat(byKey.size > 0 ? "\n" : "");
}

/**
 * Wait for a source-friendly sequential request delay.
 *
 * @param {number} milliseconds - Delay duration.
 * @returns {Promise<void>} Resolves after the delay.
 */
function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

/**
 * Convert a caught failure into stable checkpoint evidence.
 *
 * @param {unknown} caught - Caught source/browser failure.
 * @returns {{ code: string, message: string, url: string | null, failedAt: string }} Serializable failure.
 */
function checkpointError(caught) {
  return {
    code:
      caught instanceof BrowardAccelaSourceError
        ? caught.code
        : "browser_or_local_error",
    message: caught instanceof Error ? caught.message : String(caught),
    url: caught instanceof BrowardAccelaSourceError ? caught.url : null,
    failedAt: new Date().toISOString(),
  };
}

/**
 * Build or reuse a target checkpoint while retaining completed detail captures
 * from an interrupted prior attempt.
 *
 * @param {BrowardAccelaCheckpointTarget | undefined} existing - Existing target state.
 * @param {BrowardAccelaProbeTarget} target - Current target.
 * @param {string} searchKey - Stable target key.
 * @returns {BrowardAccelaCheckpointTarget} Mutable target state.
 */
function initializeTargetState(existing, target, searchKey) {
  if (
    existing !== undefined &&
    existing.jurisdictionKey === target.jurisdictionKey &&
    existing.parcelIdentifier === target.parcelIdentifier &&
    existing.searchKey === searchKey
  ) {
    existing.status = "in_progress";
    existing.error = null;
    existing.completedAt = null;
    existing.excludedNonPermitCount ??= 0;
    return existing;
  }
  return {
    status: "in_progress",
    jurisdictionKey: target.jurisdictionKey,
    parcelIdentifier: target.parcelIdentifier,
    searchKey,
    startedAt: new Date().toISOString(),
    completedAt: null,
    reportedTotal: null,
    excludedNonPermitCount: 0,
    permits: [],
    details: {},
    searchCapturePaths: [],
    error: null,
  };
}

/**
 * Flatten successful checkpoint targets into normalized output records.
 *
 * @param {Record<string, BrowardAccelaCheckpointTarget>} targets - Checkpoint target map.
 * @returns {BrowardAccelaPermitRecord[]} Completed records.
 */
function recordsFromCheckpoint(targets) {
  return Object.values(targets)
    .filter((target) => target.status === "records")
    .flatMap((target) =>
      Object.values(target.details).map((detail) => detail.record),
    );
}

/**
 * Run the bounded local probe with target- and detail-level checkpoint/resume.
 *
 * @param {BrowardAccelaCliOptions} options - Validated CLI options.
 * @returns {Promise<{ failureCount: number, skippedCompletedCount: number, normalizedRecordCount: number, summary: Record<string, unknown> }>} Probe result and summary.
 */
export async function runBrowardAccelaProbe(options) {
  const startedAt = new Date().toISOString();
  const checkpoint = await readBrowardAccelaCheckpoint(options.checkpointPath);
  let failureCount = 0;
  let skippedCompletedCount = 0;
  const browser = await createBrowardAccelaBrowser(consoleLogger);
  try {
    for (const [targetIndex, target] of options.targets.entries()) {
      const source = readBrowardAccelaSource(target.jurisdictionKey);
      const searchKey = buildBrowardAccelaSearchKey(
        source,
        target.parcelIdentifier,
      );
      const completed = checkpoint.targets[searchKey];
      if (
        completed?.status === "records" ||
        completed?.status === "no_records" ||
        completed?.status === "non_permit_records_only"
      ) {
        skippedCompletedCount += 1;
        consoleLogger.info("broward_accela_target_resumed_completed", {
          searchKey,
          status: completed.status,
          recordCount: Object.keys(completed.details).length,
        });
        continue;
      }

      const state = initializeTargetState(completed, target, searchKey);
      checkpoint.targets[searchKey] = state;
      await writeBrowardAccelaCheckpoint(options.checkpointPath, checkpoint);
      try {
        if (state.permits.length === 0) {
          const searchResult = await searchBrowardAccelaParcel({
            browser,
            source,
            parcelIdentifier: target.parcelIdentifier,
            maxPages: options.maxPages,
            logger: consoleLogger,
          });
          state.reportedTotal = searchResult.reportedTotal;
          state.excludedNonPermitCount = searchResult.excludedNonPermitCount;
          state.permits = options.roofOnly
            ? searchResult.permits.filter(
                isBrowardAccelaRoofPermitCandidate,
              )
            : searchResult.permits;
          state.searchCapturePaths = [];
          for (const page of searchResult.pages) {
            const capturePath = join(
              options.captureDirectory,
              source.key,
              target.parcelIdentifier,
              "search",
              `page-${String(page.pageNumber).padStart(3, "0")}.html`,
            );
            await writeRawCapture(capturePath, page.html);
            state.searchCapturePaths.push(capturePath);
          }
          if (searchResult.status !== "records") {
            state.status = searchResult.status;
            state.completedAt = new Date().toISOString();
            await writeBrowardAccelaCheckpoint(
              options.checkpointPath,
              checkpoint,
            );
            if (targetIndex + 1 < options.targets.length) {
              await delay(options.targetDelayMs);
            }
            continue;
          }
          await writeBrowardAccelaCheckpoint(
            options.checkpointPath,
            checkpoint,
          );
        }

        if (state.permits.length > options.maxDetails) {
          throw new BrowardAccelaSourceError(
            "incomplete_pagination",
            source,
            `${source.jurisdiction} target ${target.parcelIdentifier} exposed ${String(state.permits.length)} details, above bounded limit ${String(options.maxDetails)}`,
            source.portalUrl,
          );
        }
        for (const [permitIndex, permit] of state.permits.entries()) {
          if (state.details[permit.recordNumber] !== undefined) continue;
          const capture = await captureBrowardAccelaPermitDetail({
            browser,
            source,
            parcelIdentifier: target.parcelIdentifier,
            permit,
            logger: consoleLogger,
          });
          const capturePath = join(
            options.captureDirectory,
            source.key,
            target.parcelIdentifier,
            "details",
            `${buildBrowardAccelaPermitStem(permit)}.html`,
          );
          await writeRawCapture(capturePath, capture.html);
          state.details[permit.recordNumber] = {
            capturePath,
            record: capture.record,
          };
          await writeBrowardAccelaCheckpoint(
            options.checkpointPath,
            checkpoint,
          );
          if (permitIndex + 1 < state.permits.length) {
            await delay(options.detailDelayMs);
          }
        }
        if (Object.keys(state.details).length !== state.permits.length) {
          throw new Error(
            `${source.jurisdiction} detail checkpoint count does not match discovered records`,
          );
        }
        state.status = "records";
        state.completedAt = new Date().toISOString();
        await writeBrowardAccelaCheckpoint(options.checkpointPath, checkpoint);
      } catch (caught) {
        failureCount += 1;
        state.status = "failed";
        if (
          caught instanceof BrowardAccelaSourceError &&
          caught.responseHtml !== null
        ) {
          const failureCapturePath = join(
            options.captureDirectory,
            source.key,
            target.parcelIdentifier,
            "failure",
            "latest.html",
          );
          await writeRawCapture(failureCapturePath, caught.responseHtml);
          if (!state.searchCapturePaths.includes(failureCapturePath)) {
            state.searchCapturePaths.push(failureCapturePath);
          }
        }
        state.error = checkpointError(caught);
        await writeBrowardAccelaCheckpoint(options.checkpointPath, checkpoint);
        consoleLogger.error("broward_accela_target_failed", {
          searchKey,
          jurisdiction: source.jurisdiction,
          parcelIdentifier: target.parcelIdentifier,
          ...state.error,
        });
      }
      if (targetIndex + 1 < options.targets.length) {
        await delay(options.targetDelayMs);
      }
    }
  } finally {
    await browser.close().catch(() => undefined);
  }

  const records = recordsFromCheckpoint(checkpoint.targets);
  const rendered = renderBrowardAccelaPermitJsonl(records);
  await writePrivateArtifact(options.outputPath, rendered);
  const targetSummaries = options.targets.map((target) => {
    const source = readBrowardAccelaSource(target.jurisdictionKey);
    const searchKey = buildBrowardAccelaSearchKey(
      source,
      target.parcelIdentifier,
    );
    const state = checkpoint.targets[searchKey];
    return {
      jurisdictionKey: target.jurisdictionKey,
      jurisdiction: source.jurisdiction,
      agencyCode: source.agencyCode,
      module: source.module,
      sourceSystem: source.sourceSystem,
      portalUrl: source.portalUrl,
      officialEvidenceUrl: source.officialEvidenceUrl,
      parcelIdentifier: target.parcelIdentifier,
      historicalCutoff: source.historicalCutoff,
      separateHistoricalSource: source.separateHistoricalSource,
      outcome: state?.status ?? "not_attempted",
      reportedTotal: state?.reportedTotal ?? null,
      excludedNonPermitCount: state?.excludedNonPermitCount ?? 0,
      discoveredRecordCount: state?.permits.length ?? 0,
      capturedRecordCount:
        state === undefined ? 0 : Object.keys(state.details).length,
      recordNumbers:
        state === undefined ? [] : Object.keys(state.details).sort(),
      searchCapturePaths: state?.searchCapturePaths ?? [],
      error: state?.error ?? null,
    };
  });
  const summary = {
    event: "broward_accela_local_probe_completed",
    startedAt,
    completedAt: new Date().toISOString(),
    isCuratedPilot: options.isCuratedPilot,
      roofOnly: options.roofOnly,
    publicAnonymousSearch: true,
    targetCount: options.targets.length,
    maxTargetsPerJurisdiction: MAX_TARGETS_PER_JURISDICTION,
    maxPagesPerTarget: options.maxPages,
    maxDetailsPerTarget: options.maxDetails,
    failureCount,
    skippedCompletedCount,
    normalizedRecordCount: records.length,
    outputPath: options.outputPath,
    checkpointPath: options.checkpointPath,
    captureDirectory: options.captureDirectory,
    targets: targetSummaries,
    restrictions: [
      "local private staging only",
      "no login or access-control bypass",
      "no AWS, database load, queue, publication, or full harvest",
      "no historical completeness claim beyond each explicit cutoff",
    ],
  };
  await writePrivateArtifact(
    options.summaryPath,
    `${JSON.stringify(summary, null, 2)}\n`,
  );
  return {
    failureCount,
    skippedCompletedCount,
    normalizedRecordCount: records.length,
    summary,
  };
}

/**
 * CLI entry point.
 *
 * @returns {Promise<void>} Resolves after all bounded targets and local writes.
 */
export async function main() {
  const options = parseOptions(process.argv.slice(2));
  if (options === null) {
    process.stdout.write(USAGE);
    return;
  }
  const result = await runBrowardAccelaProbe(options);
  process.stderr.write(`${JSON.stringify(result.summary)}\n`);
  if (result.failureCount > 0) process.exitCode = 2;
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((caught) => {
    process.stderr.write(
      `${JSON.stringify({
        event: "broward_accela_local_probe_failed",
        error: caught instanceof Error ? caught.message : String(caught),
      })}\n`,
    );
    process.exitCode = 1;
  });
}
