#!/usr/bin/env node
// @ts-check

/**
 * Property-first partial gap filler for unresolved capped Accela windows.
 *
 * This runner reads only the first unresolved record-type shard plan from the
 * existing date-window checkpoint. It searches exact folios from the verified
 * Broward seed, retains only records with a source date inside that unresolved
 * window, and deduplicates against the existing date-window inventory. A
 * property-seed scan never marks the parent window complete because permits
 * without a parcel or standard location are outside its discovery domain.
 */

import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { parse } from "csv-parse";

import {
  BrowardAccelaSourceError,
  createBrowardAccelaBrowser,
  csvRecordsFromPermitLinks,
  normalizeBrowardPermitFolio,
  readBrowardAccelaSource,
  searchBrowardAccelaParcel,
} from "./permit-source-adapters/broward-accela.mjs";
import { retryAccelaCsvCooldownMs } from "./run-broward-accela-csv-windows.mjs";

const GAP_CHECKPOINT_SCHEMA = "oracle-node.broward-accela-property-gap-fill.v1";
const SOURCE_CITIES = Object.freeze({
  plantation: Object.freeze(["PLANTATION"]),
  "cooper-city": Object.freeze(["COOPER CITY"]),
  weston: Object.freeze(["WESTON"]),
});

/**
 * @typedef {"plantation" | "cooper-city" | "weston"} GapFillSourceKey
 *
 * @typedef {object} GapFillOptions
 * @property {GapFillSourceKey} sourceKey - Supported Accela jurisdiction.
 * @property {string} seedPath - Verified Broward property seed.
 * @property {string} outputDirectory - Existing Accela date-window run root.
 * @property {number} maxProperties - Bounded matching properties; zero means scan to exhaustion.
 * @property {number} maxPages - Exact parcel-search page ceiling.
 * @property {number} delayMs - Delay between successful property searches.
 *
 * @typedef {object} GapFillPlan
 * @property {string} startDate - Inclusive unresolved window start.
 * @property {string} endDate - Inclusive unresolved window end.
 * @property {number} nextSeedRowIndex - Zero-based next data row in the immutable seed.
 * @property {number} inspectedPropertyCount - Successfully searched jurisdiction properties.
 * @property {number} retainedRecordCount - Novel dated records retained.
 * @property {number} existingRecordCount - In-window records already in date-window inventory.
 * @property {number} undatedRecordCount - Parcel-search rows not safely attributable to the window.
 * @property {boolean} seedExhausted - Whether every immutable seed row was considered.
 * @property {string} updatedAt - Latest durable progress.
 *
 * @typedef {object} GapFillCooldown
 * @property {"timeout" | "source_cap" | "incomplete_pagination" | "source_error"} reason
 * @property {number} attemptCount - Consecutive failures since successful property progress.
 * @property {number} cooldownMs - Applied exponential cooldown.
 * @property {string} scheduledAt - ISO failure time.
 * @property {string} nextAttemptAt - Earliest safe retry.
 *
 * @typedef {object} GapFillCheckpoint
 * @property {typeof GAP_CHECKPOINT_SCHEMA} schemaVersion - Schema marker.
 * @property {GapFillSourceKey} sourceKey - Jurisdiction identity.
 * @property {string} seedSha256 - Immutable verified seed identity.
 * @property {Record<string, GapFillPlan>} plans - Partial plans by unresolved window.
 * @property {GapFillCooldown | null} cooldown - Durable circuit breaker.
 * @property {string} startedAt - ISO first-run time.
 * @property {string} updatedAt - ISO latest state update.
 *
 * @typedef {Record<string, string | undefined>} SeedRow
 *
 * @typedef {object} GapFillDependencies
 * @property {(() => Promise<import("puppeteer").Browser>)} [createBrowser]
 * @property {typeof searchBrowardAccelaParcel} [searchParcel]
 * @property {() => string} [now]
 * @property {() => number} [random]
 * @property {(milliseconds:number) => Promise<void>} [wait]
 *
 * @typedef {object} GapFillSummary
 * @property {"partial" | "cooling_down" | "seed_exhausted"} status
 * @property {string} sourceKey - Jurisdiction identity.
 * @property {string} windowKey - Unresolved parent key.
 * @property {number} propertiesProcessedThisInvocation - Exact successful searches.
 * @property {number} inspectedPropertyCount - Durable successful searches.
 * @property {number} retainedRecordCount - Durable novel records.
 * @property {number} existingRecordCount - Durable already-captured records.
 * @property {number} undatedRecordCount - Durable unattributable rows.
 * @property {boolean} completenessEstablished - Always false for property-first evidence.
 * @property {string | null} nextAttemptAt - Earliest retry when cooling.
 */

/**
 * Parse a bounded property-first gap-fill command.
 *
 * @param {readonly string[]} argv - CLI arguments.
 * @returns {GapFillOptions} Validated options.
 */
export function parsePropertyGapFillOptions(argv) {
  const values = new Map();
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === undefined || !token.startsWith("--")) continue;
    const [rawName, inline] = token.slice(2).split("=", 2);
    const next = inline ?? argv[index + 1];
    if (rawName === undefined || next === undefined || next.startsWith("--")) {
      throw new Error(`--${rawName ?? "unknown"} requires a value`);
    }
    values.set(rawName, next);
    if (inline === undefined) index += 1;
  }
  const sourceKey = values.get("source");
  if (
    sourceKey !== "plantation" &&
    sourceKey !== "cooper-city" &&
    sourceKey !== "weston"
  ) {
    throw new Error("--source must be plantation, cooper-city, or weston");
  }
  return {
    sourceKey,
    seedPath: path.resolve(
      values.get("seed") ?? "downloads/broward/broward.csv",
    ),
    outputDirectory: path.resolve(
      requireText(values.get("output-dir"), "--output-dir"),
    ),
    maxProperties: readInteger(
      values.get("max-properties") ?? "1",
      "--max-properties",
      0,
      10_000,
    ),
    maxPages: readInteger(
      values.get("max-pages") ?? "10",
      "--max-pages",
      1,
      10,
    ),
    delayMs: readInteger(
      values.get("delay-ms") ?? "30000",
      "--delay-ms",
      1_000,
      3_600_000,
    ),
  };
}

/**
 * Determine whether a verified seed row belongs to the requested municipality.
 *
 * @param {GapFillSourceKey} sourceKey - Jurisdiction key.
 * @param {SeedRow} row - Parsed Broward seed row.
 * @returns {boolean} True only for an exact normalized municipality name.
 */
export function isPropertyGapFillSeedRow(sourceKey, row) {
  const city = (row.city ?? "").replace(/\s+/gu, " ").trim().toUpperCase();
  return SOURCE_CITIES[sourceKey].includes(city);
}

/**
 * Retain only source-dated records inside one unresolved parent and classify
 * identities already present in completed date-window captures.
 *
 * @param {object} params - Reconciliation inputs.
 * @param {readonly import("./permit-source-adapters/broward-accela.mjs").BrowardAccelaCsvPermitRecord[]} params.records
 *   Property-search records.
 * @param {string} params.startDate - Inclusive unresolved start.
 * @param {string} params.endDate - Inclusive unresolved end.
 * @param {ReadonlySet<string>} params.existingRecordKeys - Existing date-window identities.
 * @returns {{retained:import("./permit-source-adapters/broward-accela.mjs").BrowardAccelaCsvPermitRecord[],existingCount:number,undatedCount:number}}
 *   Exact partial evidence accounting.
 */
export function reconcilePropertyGapFillRecords({
  records,
  startDate,
  endDate,
  existingRecordKeys,
}) {
  const retained = new Map();
  let existingCount = 0;
  let undatedCount = 0;
  for (const record of records) {
    if (record.recordDate === null) {
      undatedCount += 1;
      continue;
    }
    if (record.recordDate < startDate || record.recordDate > endDate) continue;
    if (existingRecordKeys.has(record.recordKey)) {
      existingCount += 1;
      continue;
    }
    const prior = retained.get(record.recordKey);
    if (
      prior !== undefined &&
      JSON.stringify(prior) !== JSON.stringify(record)
    ) {
      throw new Error("Property gap-fill record identity conflicts");
    }
    retained.set(record.recordKey, record);
  }
  return {
    retained: [...retained.values()].sort((left, right) =>
      left.recordKey.localeCompare(right.recordKey),
    ),
    existingCount,
    undatedCount,
  };
}

/**
 * Run a checkpoint-compatible property-seed slice. This function never writes
 * the parent Accela checkpoint and therefore cannot claim complete coverage.
 *
 * @param {GapFillOptions} options - Validated command options.
 * @param {GapFillDependencies} [dependencies] - Injectable bounded I/O.
 * @returns {Promise<GapFillSummary>} Aggregate-only partial coverage summary.
 */
export async function runPropertyGapFill(options, dependencies = {}) {
  const now = dependencies.now ?? (() => new Date().toISOString());
  const random = dependencies.random ?? Math.random;
  const wait =
    dependencies.wait ??
    ((milliseconds) =>
      new Promise((resolvePromise) =>
        setTimeout(resolvePromise, milliseconds),
      ));
  const createBrowser =
    dependencies.createBrowser ??
    (() =>
      createBrowardAccelaBrowser({
        info: () => undefined,
        warn: () => undefined,
        error: () => undefined,
      }));
  const searchParcel = dependencies.searchParcel ?? searchBrowardAccelaParcel;
  const source = readBrowardAccelaSource(options.sourceKey);
  const mainCheckpointPath = path.join(
    options.outputDirectory,
    "checkpoint.private.json",
  );
  const gapDirectory = path.join(options.outputDirectory, "property-gap-fill");
  const gapCheckpointPath = path.join(gapDirectory, "checkpoint.private.json");
  const gapRecordsPath = path.join(gapDirectory, "records.private.jsonl");
  await mkdir(gapDirectory, { recursive: true, mode: 0o700 });
  const mainCheckpoint = readMainCheckpoint(
    JSON.parse(await readFile(mainCheckpointPath, "utf8")),
    options.sourceKey,
  );
  const [windowKey, shardPlan] =
    Object.entries(mainCheckpoint.shardPlans)[0] ?? [];
  if (windowKey === undefined || shardPlan === undefined) {
    throw new Error("Accela checkpoint has no unresolved shard plan");
  }
  const seedSha256 = await hashFile(options.seedPath);
  let checkpoint = await readOrCreateGapCheckpoint(
    gapCheckpointPath,
    options.sourceKey,
    seedSha256,
    now(),
  );
  const existingPlan = checkpoint.plans[windowKey];
  const plan = existingPlan ?? {
    startDate: shardPlan.startDate,
    endDate: shardPlan.endDate,
    nextSeedRowIndex: 0,
    inspectedPropertyCount: 0,
    retainedRecordCount: 0,
    existingRecordCount: 0,
    undatedRecordCount: 0,
    seedExhausted: false,
    updatedAt: now(),
  };
  if (
    plan.startDate !== shardPlan.startDate ||
    plan.endDate !== shardPlan.endDate
  ) {
    throw new Error("Property gap-fill plan conflicts with parent window");
  }
  const currentMs = Date.parse(now());
  const parentNextAttemptMs =
    mainCheckpoint.nextAttemptAt === null
      ? Number.NEGATIVE_INFINITY
      : Date.parse(mainCheckpoint.nextAttemptAt);
  const nextAttemptMs =
    checkpoint.cooldown === null
      ? Number.NEGATIVE_INFINITY
      : Date.parse(checkpoint.cooldown.nextAttemptAt);
  const safeAttemptMs = Math.max(parentNextAttemptMs, nextAttemptMs);
  if (Number.isFinite(safeAttemptMs) && safeAttemptMs > currentMs) {
    await wait(safeAttemptMs - currentMs);
  }
  if (plan.seedExhausted) {
    return buildSummary(options.sourceKey, windowKey, plan, 0, checkpoint);
  }
  const existingKeys = await readRecordKeys(
    path.join(options.outputDirectory, "normalized-list.private.jsonl"),
  );
  const gapRecords = await readRecords(gapRecordsPath);
  for (const key of gapRecords.keys()) existingKeys.add(key);
  let propertiesProcessed = 0;
  let rowIndex = 0;
  let reachedEof = true;
  /** @type {import("puppeteer").Browser | null} */
  let browser = null;
  try {
    const parser = createReadStream(options.seedPath).pipe(
      parse({ columns: true, bom: true, skip_empty_lines: true }),
    );
    for await (const value of parser) {
      const row = /** @type {SeedRow} */ (value);
      const currentRowIndex = rowIndex;
      rowIndex += 1;
      if (currentRowIndex < plan.nextSeedRowIndex) continue;
      if (!isPropertyGapFillSeedRow(options.sourceKey, row)) continue;
      if (
        options.maxProperties > 0 &&
        propertiesProcessed >= options.maxProperties
      ) {
        reachedEof = false;
        break;
      }
      const folio = normalizeBrowardPermitFolio(row.request_identifier);
      if (browser === null) browser = await createBrowser();
      try {
        const result = await searchParcel({
          browser,
          source,
          parcelIdentifier: folio,
          maxPages: options.maxPages,
          logger: {
            info: () => undefined,
            warn: () => undefined,
            error: () => undefined,
          },
        });
        const records = csvRecordsFromPermitLinks(
          result.permits,
          source,
          `${source.key}:date:${plan.startDate}:${plan.endDate}`,
        );
        const reconciled = reconcilePropertyGapFillRecords({
          records,
          startDate: plan.startDate,
          endDate: plan.endDate,
          existingRecordKeys: existingKeys,
        });
        for (const record of reconciled.retained) {
          gapRecords.set(record.recordKey, record);
          existingKeys.add(record.recordKey);
        }
        await writePropertyEvidence(
          gapDirectory,
          windowKey,
          folio,
          result.pages.map((page) => page.html),
          reconciled.retained,
        );
        propertiesProcessed += 1;
        const updatedAt = now();
        const nextPlan = {
          ...plan,
          nextSeedRowIndex: currentRowIndex + 1,
          inspectedPropertyCount: plan.inspectedPropertyCount + 1,
          retainedRecordCount:
            plan.retainedRecordCount + reconciled.retained.length,
          existingRecordCount:
            plan.existingRecordCount + reconciled.existingCount,
          undatedRecordCount: plan.undatedRecordCount + reconciled.undatedCount,
          updatedAt,
        };
        Object.assign(plan, nextPlan);
        checkpoint = {
          ...checkpoint,
          plans: { ...checkpoint.plans, [windowKey]: nextPlan },
          cooldown: null,
          updatedAt,
        };
        await writeGapRecords(gapRecordsPath, gapRecords);
        await writePrivateAtomic(
          gapCheckpointPath,
          `${JSON.stringify(checkpoint, null, 2)}\n`,
        );
        if (
          options.maxProperties === 0 ||
          propertiesProcessed < options.maxProperties
        ) {
          await wait(options.delayMs);
        }
      } catch (error) {
        const scheduledAt = now();
        const attemptCount = (checkpoint.cooldown?.attemptCount ?? 0) + 1;
        const cooldownMs = retryAccelaCsvCooldownMs(attemptCount, random);
        checkpoint = {
          ...checkpoint,
          plans: { ...checkpoint.plans, [windowKey]: plan },
          cooldown: {
            reason: classifyGapFillFailure(error),
            attemptCount,
            cooldownMs,
            scheduledAt,
            nextAttemptAt: new Date(
              Date.parse(scheduledAt) + cooldownMs,
            ).toISOString(),
          },
          updatedAt: scheduledAt,
        };
        if (
          error instanceof BrowardAccelaSourceError &&
          typeof error.responseHtml === "string"
        ) {
          await writePrivateAtomic(
            path.join(gapDirectory, "failure-evidence.private.html"),
            error.responseHtml,
          );
        }
        await writePrivateAtomic(
          gapCheckpointPath,
          `${JSON.stringify(checkpoint, null, 2)}\n`,
        );
        break;
      }
    }
  } finally {
    await browser?.close().catch(() => undefined);
  }
  if (reachedEof && checkpoint.cooldown === null) {
    const updatedAt = now();
    const exhaustedPlan = { ...plan, seedExhausted: true, updatedAt };
    checkpoint = {
      ...checkpoint,
      plans: { ...checkpoint.plans, [windowKey]: exhaustedPlan },
      updatedAt,
    };
    Object.assign(plan, exhaustedPlan);
    await writePrivateAtomic(
      gapCheckpointPath,
      `${JSON.stringify(checkpoint, null, 2)}\n`,
    );
  }
  return buildSummary(
    options.sourceKey,
    windowKey,
    plan,
    propertiesProcessed,
    checkpoint,
  );
}

/**
 * @param {GapFillSourceKey} sourceKey
 * @param {string} windowKey
 * @param {GapFillPlan} plan
 * @param {number} propertiesProcessed
 * @param {GapFillCheckpoint} checkpoint
 * @returns {GapFillSummary}
 */
function buildSummary(
  sourceKey,
  windowKey,
  plan,
  propertiesProcessed,
  checkpoint,
) {
  return {
    status:
      checkpoint.cooldown !== null
        ? "cooling_down"
        : plan.seedExhausted
          ? "seed_exhausted"
          : "partial",
    sourceKey,
    windowKey,
    propertiesProcessedThisInvocation: propertiesProcessed,
    inspectedPropertyCount: plan.inspectedPropertyCount,
    retainedRecordCount: plan.retainedRecordCount,
    existingRecordCount: plan.existingRecordCount,
    undatedRecordCount: plan.undatedRecordCount,
    completenessEstablished: false,
    nextAttemptAt: checkpoint.cooldown?.nextAttemptAt ?? null,
  };
}

/**
 * @param {unknown} value
 * @param {GapFillSourceKey} sourceKey
 * @returns {{shardPlans:Record<string,{startDate:string,endDate:string}>,nextAttemptAt:string | null}}
 */
function readMainCheckpoint(value, sourceKey) {
  if (
    !isRecord(value) ||
    value.sourceKey !== sourceKey ||
    !isRecord(value.shardPlans)
  ) {
    throw new Error("Accela parent checkpoint is malformed or mismatched");
  }
  const shardPlans =
    /** @type {Record<string,{startDate:string,endDate:string}>} */ (
      value.shardPlans
    );
  const nextAttemptAt =
    isRecord(value.cooldown) && typeof value.cooldown.nextAttemptAt === "string"
      ? value.cooldown.nextAttemptAt
      : null;
  return { shardPlans, nextAttemptAt };
}

/**
 * @param {string} checkpointPath
 * @param {GapFillSourceKey} sourceKey
 * @param {string} seedSha256
 * @param {string} startedAt
 * @returns {Promise<GapFillCheckpoint>}
 */
async function readOrCreateGapCheckpoint(
  checkpointPath,
  sourceKey,
  seedSha256,
  startedAt,
) {
  try {
    const parsed = /** @type {unknown} */ (
      JSON.parse(await readFile(checkpointPath, "utf8"))
    );
    if (
      !isRecord(parsed) ||
      parsed.schemaVersion !== GAP_CHECKPOINT_SCHEMA ||
      parsed.sourceKey !== sourceKey ||
      parsed.seedSha256 !== seedSha256 ||
      !isRecord(parsed.plans)
    ) {
      throw new Error(
        "Property gap-fill checkpoint is malformed or mismatched",
      );
    }
    return /** @type {GapFillCheckpoint} */ (parsed);
  } catch (error) {
    if (!isNodeError(error) || error.code !== "ENOENT") throw error;
  }
  return {
    schemaVersion: GAP_CHECKPOINT_SCHEMA,
    sourceKey,
    seedSha256,
    plans: {},
    cooldown: null,
    startedAt,
    updatedAt: startedAt,
  };
}

/**
 * @param {string} recordsPath
 * @returns {Promise<Map<string,import("./permit-source-adapters/broward-accela.mjs").BrowardAccelaCsvPermitRecord>>}
 */
async function readRecords(recordsPath) {
  const records = new Map();
  try {
    for (const line of (await readFile(recordsPath, "utf8")).split(/\r?\n/u)) {
      if (line.length === 0) continue;
      const record =
        /** @type {import("./permit-source-adapters/broward-accela.mjs").BrowardAccelaCsvPermitRecord} */ (
          JSON.parse(line)
        );
      records.set(record.recordKey, record);
    }
  } catch (error) {
    if (!isNodeError(error) || error.code !== "ENOENT") throw error;
  }
  return records;
}

/**
 * @param {string} recordsPath
 * @returns {Promise<Set<string>>}
 */
async function readRecordKeys(recordsPath) {
  return new Set((await readRecords(recordsPath)).keys());
}

/**
 * @param {string} recordsPath
 * @param {Map<string,import("./permit-source-adapters/broward-accela.mjs").BrowardAccelaCsvPermitRecord>} records
 * @returns {Promise<void>}
 */
async function writeGapRecords(recordsPath, records) {
  const ordered = [...records.values()].sort((left, right) =>
    left.recordKey.localeCompare(right.recordKey),
  );
  await writePrivateAtomic(
    recordsPath,
    ordered.length === 0
      ? ""
      : `${ordered.map((record) => JSON.stringify(record)).join("\n")}\n`,
  );
}

/**
 * @param {string} gapDirectory
 * @param {string} windowKey
 * @param {string} folio
 * @param {readonly string[]} htmlPages
 * @param {readonly import("./permit-source-adapters/broward-accela.mjs").BrowardAccelaCsvPermitRecord[]} records
 * @returns {Promise<void>}
 */
async function writePropertyEvidence(
  gapDirectory,
  windowKey,
  folio,
  htmlPages,
  records,
) {
  const propertyKey = createHash("sha256")
    .update(`broward-gap:${folio}`)
    .digest("hex")
    .slice(0, 24);
  const directory = path.join(
    gapDirectory,
    "windows-private",
    windowKey,
    propertyKey,
  );
  for (const [index, html] of htmlPages.entries()) {
    await writePrivateAtomic(
      path.join(directory, `page-${String(index + 1).padStart(4, "0")}.html`),
      html,
    );
  }
  await writePrivateAtomic(
    path.join(directory, "records.private.json"),
    `${JSON.stringify({ records }, null, 2)}\n`,
  );
}

/**
 * @param {unknown} error
 * @returns {"timeout" | "source_cap" | "incomplete_pagination" | "source_error"}
 */
function classifyGapFillFailure(error) {
  if (error instanceof BrowardAccelaSourceError) {
    if (error.code === "incomplete_pagination") return "incomplete_pagination";
    return "source_error";
  }
  if (
    error instanceof Error &&
    /timeout|timed out|exceeded/iu.test(error.message)
  ) {
    return "timeout";
  }
  if (error instanceof Error && /\bcap(?:ped)?\b/iu.test(error.message)) {
    return "source_cap";
  }
  return "source_error";
}

/**
 * @param {string} filePath
 * @returns {Promise<string>}
 */
async function hashFile(filePath) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(filePath)) hash.update(chunk);
  return hash.digest("hex");
}

/**
 * @param {string} filePath
 * @param {string} content
 * @returns {Promise<void>}
 */
async function writePrivateAtomic(filePath, content) {
  await mkdir(path.dirname(filePath), { recursive: true, mode: 0o700 });
  const temporaryPath = `${filePath}.${String(process.pid)}.tmp`;
  await writeFile(temporaryPath, content, { mode: 0o600 });
  await rename(temporaryPath, filePath);
}

/**
 * @param {unknown} value
 * @returns {value is Record<string,unknown>}
 */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * @param {unknown} value
 * @returns {value is Error & {code:string}}
 */
function isNodeError(value) {
  return (
    value instanceof Error && "code" in value && typeof value.code === "string"
  );
}

/**
 * @param {string | undefined} value
 * @param {string} name
 * @returns {string}
 */
function requireText(value, name) {
  if (value === undefined || value.trim().length === 0) {
    throw new Error(`${name} is required`);
  }
  return value;
}

/**
 * @param {string} value
 * @param {string} name
 * @param {number} minimum
 * @param {number} maximum
 * @returns {number}
 */
function readInteger(value, name, minimum, maximum) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(
      `${name} must be an integer from ${String(minimum)} through ${String(maximum)}`,
    );
  }
  return parsed;
}

if (
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href
) {
  runPropertyGapFill(parsePropertyGapFillOptions(process.argv.slice(2)))
    .then((summary) => {
      console.log(
        JSON.stringify({
          event: "broward_accela_property_gap_fill_finished",
          ...summary,
        }),
      );
    })
    .catch((error) => {
      console.error(
        JSON.stringify({
          event: "broward_accela_property_gap_fill_failed",
          message: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      process.exitCode = 1;
    });
}
