#!/usr/bin/env node
// @ts-check

import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

import {
  renderMunicipalPermitJsonl,
  validateMunicipalProbeLimits,
} from "./permit-source-adapters/broward-municipal-core.mjs";
import { getBrowardMunicipalPermitConfig } from "./permit-source-adapters/broward-municipal-config.mjs";
import { probeBoundedBrowardMunicipalPermits } from "./permit-source-adapters/broward-municipal-transport.mjs";

/**
 * @typedef {import("./permit-source-adapters/broward-municipal-core.mjs").BrowardMunicipalCheckpoint} BrowardMunicipalCheckpoint
 * @typedef {import("./permit-source-adapters/broward-municipal-core.mjs").BrowardMunicipalProbeLimits} BrowardMunicipalProbeLimits
 * @typedef {import("./permit-source-adapters/broward-municipal-core.mjs").BrowardMunicipalQuery} BrowardMunicipalQuery
 * @typedef {import("./permit-source-adapters/broward-municipal-core.mjs").NormalizedBrowardMunicipalPermit} NormalizedBrowardMunicipalPermit
 */

/**
 * @typedef {object} BrowardMunicipalPilotCliOptions
 * @property {string} jurisdictionKey - Configured jurisdiction key.
 * @property {BrowardMunicipalQuery} query - Exactly one private exact query.
 * @property {string} outputDirectory - Local owner-only artifact directory.
 * @property {BrowardMunicipalProbeLimits} limits - Validated hard process ceilings.
 * @property {number} requestTimeoutMs - Per-request/browser deadline.
 */

const USAGE = `Usage:
  node scripts/run-broward-municipal-permit-pilot.mjs \\
    --jurisdiction <key> \\
    (--permit-number <value> | --address <value> | --folio <12-character-id>) \\
    --output-dir <private-directory> [options]

Options:
  --max-pages <1..6>          default: 3
  --max-results <1..50>       default: 25
  --max-details <1..10>       default: 3
  --delay-ms <>=1000>         default: 1250
  --request-timeout-ms <ms>   default: 30000

Safety:
  One exact anonymous query only. Requests are sequential, deadline-bounded,
  rate-delayed, and checkpointed to owner-only local files. Query values are
  never written to summaries or checkpoints and are never printed. Login,
  CAPTCHA, records-request, and unhealthy landing-only routes are no-request
  skips. No database, AWS, publication, or form-submission workflow is used.
`;

/**
 * Read one required CLI value without accepting another flag as its value.
 *
 * @param {readonly string[]} args - Raw arguments after the script path.
 * @param {number} index - Current argument index.
 * @param {string} flag - Flag used in safe validation errors.
 * @returns {{value:string,nextIndex:number}} Parsed value and consumed index.
 */
function readOptionValue(args, index, flag) {
  const value = args[index + 1];
  if (value === undefined || value.startsWith("--")) {
    throw new Error(`${flag} requires a value`);
  }
  return { value, nextIndex: index + 1 };
}

/**
 * Parse one bounded positive integer.
 *
 * @param {string} value - Candidate numeric text.
 * @param {string} flag - Flag used in validation errors.
 * @param {number} minimum - Inclusive minimum.
 * @param {number} maximum - Inclusive maximum.
 * @returns {number} Validated integer.
 */
function parseInteger(value, flag, minimum, maximum) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(
      `${flag} must be an integer from ${String(minimum)} through ${String(maximum)}`,
    );
  }
  return parsed;
}

/**
 * Parse the local-only municipal pilot command.
 *
 * Query values remain only in this private process object. Errors mention
 * fixed flag names, never the values.
 *
 * @param {readonly string[]} args - Arguments after the script path.
 * @returns {BrowardMunicipalPilotCliOptions | null} Parsed options or help.
 */
export function parseBrowardMunicipalPilotOptions(args) {
  /** @type {string | null} */
  let jurisdictionKey = null;
  /** @type {BrowardMunicipalQuery[]} */
  const queries = [];
  /** @type {string | null} */
  let outputDirectory = null;
  let maxSearchPages = 3;
  let maxResults = 25;
  let maxDetailPages = 3;
  let delayMs = 1_250;
  let requestTimeoutMs = 30_000;

  for (let index = 0; index < args.length; index += 1) {
    const flag = args[index];
    if (flag === "--help" || flag === "-h") return null;
    if (flag === undefined) throw new Error("Empty municipal pilot option");
    const option = readOptionValue(args, index, flag);
    index = option.nextIndex;
    if (flag === "--jurisdiction") jurisdictionKey = option.value.trim();
    else if (flag === "--permit-number") {
      queries.push({ kind: "permit_number", value: option.value });
    } else if (flag === "--address") {
      queries.push({ kind: "address", value: option.value });
    } else if (flag === "--folio") {
      queries.push({ kind: "folio", value: option.value });
    } else if (flag === "--output-dir") {
      outputDirectory = option.value.trim();
    } else if (flag === "--max-pages") {
      maxSearchPages = parseInteger(option.value, flag, 1, 6);
    } else if (flag === "--max-results") {
      maxResults = parseInteger(option.value, flag, 1, 50);
    } else if (flag === "--max-details") {
      maxDetailPages = parseInteger(option.value, flag, 1, 10);
    } else if (flag === "--delay-ms") {
      delayMs = parseInteger(option.value, flag, 1_000, 60_000);
    } else if (flag === "--request-timeout-ms") {
      requestTimeoutMs = parseInteger(option.value, flag, 1_000, 120_000);
    } else {
      throw new Error(`Unknown municipal pilot option: ${flag}`);
    }
  }

  if (jurisdictionKey === null || jurisdictionKey.length === 0) {
    throw new Error("--jurisdiction is required");
  }
  if (queries.length !== 1) {
    throw new Error(
      "Choose exactly one of --permit-number, --address, or --folio",
    );
  }
  if (outputDirectory === null || outputDirectory.length === 0) {
    throw new Error("--output-dir is required");
  }
  const config = getBrowardMunicipalPermitConfig(jurisdictionKey);
  const query = /** @type {BrowardMunicipalQuery} */ (queries[0]);
  if (!config.capabilities.searchBy.includes(query.kind)) {
    throw new Error(
      `${config.jurisdiction} does not support the selected query kind`,
    );
  }
  const limits = validateMunicipalProbeLimits({
    maxQueries: 1,
    maxSearchPages,
    maxResults,
    maxDetailPages,
    delayMs,
  });
  return {
    jurisdictionKey: config.key,
    query,
    outputDirectory,
    limits,
    requestTimeoutMs,
  };
}

/**
 * Read optional parsed JSON without converting malformed data to a fresh run.
 *
 * @param {string} filePath - Private JSON file path.
 * @returns {Promise<unknown | undefined>} Parsed value or undefined when absent.
 */
async function readOptionalJson(filePath) {
  try {
    return /** @type {unknown} */ (JSON.parse(await readFile(filePath, "utf8")));
  } catch (caught) {
    if (
      caught instanceof Error &&
      "code" in caught &&
      caught.code === "ENOENT"
    ) {
      return undefined;
    }
    throw caught;
  }
}

/**
 * Read and minimally validate prior private normalized records.
 *
 * Full identity validation occurs again in the bounded runner for every newly
 * fetched record. Existing records must match the selected source and have
 * unique stable keys before they can participate in resume.
 *
 * @param {string} filePath - Existing private JSONL path.
 * @param {string} sourceSystem - Exact configured source system.
 * @returns {Promise<Map<string, NormalizedBrowardMunicipalPermit>>} Prior record map.
 */
async function readExistingRecords(filePath, sourceSystem) {
  /** @type {Map<string, NormalizedBrowardMunicipalPermit>} */
  const records = new Map();
  let text;
  try {
    text = await readFile(filePath, "utf8");
  } catch (caught) {
    if (
      caught instanceof Error &&
      "code" in caught &&
      caught.code === "ENOENT"
    ) {
      return records;
    }
    throw caught;
  }
  for (const line of text.split(/\r?\n/u)) {
    if (line.trim().length === 0) continue;
    const parsed = /** @type {unknown} */ (JSON.parse(line));
    if (
      parsed === null ||
      typeof parsed !== "object" ||
      Array.isArray(parsed)
    ) {
      throw new Error("Private municipal record JSONL is malformed");
    }
    const candidate = /** @type {Record<string, unknown>} */ (parsed);
    if (
      candidate.source_system !== sourceSystem ||
      typeof candidate.record_key !== "string" ||
      candidate.record_key.length === 0 ||
      records.has(candidate.record_key)
    ) {
      throw new Error("Private municipal record JSONL identity is invalid");
    }
    records.set(
      candidate.record_key,
      /** @type {NormalizedBrowardMunicipalPermit} */ (parsed),
    );
  }
  return records;
}

/**
 * Atomically write one owner-only UTF-8 artifact.
 *
 * @param {string} filePath - Final local artifact path.
 * @param {string} contents - Complete replacement contents.
 * @returns {Promise<void>} Resolves after atomic replacement.
 */
async function writePrivateFile(filePath, contents) {
  const temporaryPath = `${filePath}.${String(process.pid)}.tmp`;
  await writeFile(temporaryPath, contents, {
    encoding: "utf8",
    mode: 0o600,
  });
  await rename(temporaryPath, filePath);
}

/**
 * Execute one resumable local municipal pilot.
 *
 * Records are atomically persisted before their checkpoint identities advance.
 * A crash between those operations safely refetches one detail and the sink
 * accepts only an exact duplicate.
 *
 * @param {BrowardMunicipalPilotCliOptions} options - Validated local options.
 * @returns {Promise<Readonly<Record<string, unknown>>>} Privacy-safe aggregate summary.
 */
export async function runBrowardMunicipalPilot(options) {
  const config = getBrowardMunicipalPermitConfig(options.jurisdictionKey);
  const outputDirectory = resolve(options.outputDirectory);
  await mkdir(outputDirectory, { recursive: true, mode: 0o700 });
  const checkpointPath = join(outputDirectory, "checkpoint.private.json");
  const recordsPath = join(outputDirectory, "records.private.jsonl");
  const summaryPath = join(outputDirectory, "summary.private.json");
  const rawCheckpoint = await readOptionalJson(checkpointPath);
  const records = await readExistingRecords(recordsPath, config.sourceSystem);

  const result = await probeBoundedBrowardMunicipalPermits({
    config,
    queries: [options.query],
    limits: options.limits,
    checkpoint: rawCheckpoint,
    dependencies: { requestTimeoutMs: options.requestTimeoutMs },
    onRecord: async (record) => {
      const existing = records.get(record.record_key);
      if (
        existing !== undefined &&
        JSON.stringify(existing) !== JSON.stringify(record)
      ) {
        throw new Error("Private municipal record identity changed on resume");
      }
      records.set(record.record_key, record);
      await writePrivateFile(
        recordsPath,
        renderMunicipalPermitJsonl([...records.values()]),
      );
    },
    onCheckpoint: async (checkpoint) => {
      await writePrivateFile(
        checkpointPath,
        `${JSON.stringify(checkpoint, null, 2)}\n`,
      );
    },
  });

  const summary = Object.freeze({
    event: "broward_municipal_permit_pilot_completed",
    jurisdictionKey: config.key,
    protocol: config.protocol,
    queryKind: options.query.kind,
    status: result.status,
    accessReason: result.access.reason,
    searchPageCount: result.searchPageCount,
    detailPageCount: result.detailPageCount,
    capturedRecordCount: records.size,
    completed: result.checkpoint?.completed ?? false,
    queryDigest: result.checkpoint?.queryDigest ?? null,
  });
  await writePrivateFile(
    summaryPath,
    `${JSON.stringify(summary, null, 2)}\n`,
  );
  return summary;
}

/**
 * CLI entry point that prints aggregate evidence only.
 *
 * @returns {Promise<void>} Resolves after local artifacts are durable.
 */
export async function main() {
  const options = parseBrowardMunicipalPilotOptions(process.argv.slice(2));
  if (options === null) {
    process.stdout.write(USAGE);
    return;
  }
  const summary = await runBrowardMunicipalPilot(options);
  process.stdout.write(`${JSON.stringify(summary)}\n`);
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((caught) => {
    process.stderr.write(
      `${JSON.stringify({
        event: "broward_municipal_permit_pilot_failed",
        error: caught instanceof Error ? caught.message : String(caught),
      })}\n`,
    );
    process.exitCode = 1;
  });
}
