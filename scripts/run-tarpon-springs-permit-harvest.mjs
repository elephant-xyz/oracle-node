#!/usr/bin/env node
// @ts-check

/**
 * Tarpon Springs Click2Gov harvest (HTTP, no Chrome).
 *
 * Address-search first, then Status Detail per application number.
 * Delay ≥1s. Resume with --skip-existing.
 */

import { mkdir, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import {
  createClick2GovHttpSession,
  fetchClick2GovDetailByApplicationNumber,
  searchClick2GovByAddress,
} from "./permit-source-adapters/click2gov-http.mjs";
import {
  TARPON_CLICK2GOV_CONFIG,
  TARPON_DEFAULT_PROBE_QUERIES,
} from "./pinellas/tarpon-click2gov.mjs";

/**
 * @typedef {import("./permit-source-adapters/click2gov-http.mjs").Click2GovAddressQuery} Click2GovAddressQuery
 * @typedef {import("./permit-source-adapters/click2gov-http.mjs").Click2GovSearchRow} Click2GovSearchRow
 */

/**
 * @typedef {object} TarponHarvestCli
 * @property {string} jobId Output directory name.
 * @property {readonly Click2GovAddressQuery[]} queries Address searches.
 * @property {number} delayMs Delay between HTTP calls.
 * @property {boolean} skipExisting Skip details whose JSON already exists.
 * @property {number} maxDetails 0 means unlimited.
 */

/**
 * @param {string} applicationNumber Portal application number.
 * @returns {string} Filesystem-safe stem.
 */
export function tarponPermitFileStem(applicationNumber) {
  return applicationNumber.toLowerCase().replace(/[^a-z0-9]+/g, "-");
}

/**
 * @param {readonly string[]} args CLI args after the script path.
 * @returns {TarponHarvestCli} Parsed flags.
 */
export function parseTarponHarvestCli(args) {
  /** @type {Click2GovAddressQuery[]} */
  const queries = [];
  /** @type {Map<string, string>} */
  const values = new Map();
  for (let index = 0; index < args.length; index += 1) {
    const token = args[index];
    if (token === "--street-number") {
      const number = args[index + 1];
      const nameFlag = args[index + 2];
      const name = args[index + 3];
      if (
        number === undefined ||
        nameFlag !== "--street-name" ||
        name === undefined
      ) {
        throw new Error(
          "--street-number must be followed by --street-name <name>",
        );
      }
      queries.push({ streetNumber: number, streetName: name });
      index += 3;
      continue;
    }
    if (token === undefined || token.startsWith("--") === false) continue;
    const key = token.slice(2);
    const next = args[index + 1];
    if (next !== undefined && next.startsWith("--") === false) {
      values.set(key, next);
      index += 1;
    } else {
      values.set(key, "true");
    }
  }
  const today = new Date().toISOString().slice(0, 10).replaceAll("-", "");
  const delayMs = Number.parseInt(values.get("delay-ms") ?? "1500", 10);
  if (!Number.isInteger(delayMs) || delayMs < 1_000) {
    throw new Error("--delay-ms must be an integer of at least 1000");
  }
  const maxDetails = Number.parseInt(values.get("max-details") ?? "0", 10);
  if (!Number.isInteger(maxDetails) || maxDetails < 0) {
    throw new Error("--max-details must be a non-negative integer");
  }
  return {
    jobId: values.get("job-id") ?? `tarpon-click2gov-full-${today}`,
    queries: queries.length > 0 ? queries : [...TARPON_DEFAULT_PROBE_QUERIES],
    delayMs,
    skipExisting: values.get("skip-existing") !== "false",
    maxDetails,
  };
}

/**
 * @param {number} milliseconds Delay.
 * @returns {Promise<void>}
 */
function delay(milliseconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

/**
 * @param {TarponHarvestCli} options CLI options.
 * @param {string} repoRoot oracle-node root.
 * @returns {Promise<{ jobDir: string, written: number, skipped: number, rowCount: number }>}
 *   Harvest totals.
 */
export async function runTarponClick2GovHarvest(options, repoRoot) {
  const jobDir = path.resolve(
    repoRoot,
    "downloads/pinellas/permits",
    options.jobId,
  );
  const extractedDir = path.join(jobDir, "extracted");
  await mkdir(extractedDir, { recursive: true });
  const startedAt = Date.now();
  /** @type {Click2GovSearchRow[]} */
  const rows = [];
  /** @type {Set<string>} */
  const seen = new Set();
  for (const [index, query] of options.queries.entries()) {
    if (index > 0) await delay(options.delayMs);
    const session = await createClick2GovHttpSession(
      TARPON_CLICK2GOV_CONFIG.origin,
    );
    const searched = await searchClick2GovByAddress({
      origin: TARPON_CLICK2GOV_CONFIG.origin,
      session,
      query,
    });
    for (const row of searched.rows) {
      if (seen.has(row.applicationNumber)) continue;
      seen.add(row.applicationNumber);
      rows.push(row);
    }
  }
  let written = 0;
  let skipped = 0;
  for (const row of rows) {
    if (options.maxDetails > 0 && written >= options.maxDetails) break;
    const jsonPath = path.join(
      extractedDir,
      `${tarponPermitFileStem(row.applicationNumber)}.json`,
    );
    if (options.skipExisting && existsSync(jsonPath)) {
      skipped += 1;
      continue;
    }
    await delay(options.delayMs);
    const fetched = await fetchClick2GovDetailByApplicationNumber({
      origin: TARPON_CLICK2GOV_CONFIG.origin,
      applicationNumber: row.applicationNumber,
    });
    const payload = {
      source: TARPON_CLICK2GOV_CONFIG.sourceStamp,
      city: TARPON_CLICK2GOV_CONFIG.city,
      applicationNumber: row.applicationNumber,
      searchRow: row,
      detailStatus: fetched.status,
      detail: fetched.data,
      error: fetched.error,
    };
    await writeFile(jsonPath, `${JSON.stringify(payload, null, 2)}\n`);
    written += 1;
    console.log(
      JSON.stringify({
        event: "tarpon_click2gov_detail_written",
        applicationNumber: row.applicationNumber,
        detailStatus: fetched.status,
        jsonPath,
      }),
    );
  }
  const summary = {
    event: "tarpon_click2gov_harvest_complete",
    jobId: options.jobId,
    sourceStamp: TARPON_CLICK2GOV_CONFIG.sourceStamp,
    queryCount: options.queries.length,
    rowCount: rows.length,
    written,
    skipped,
    elapsedMs: Date.now() - startedAt,
    jobDir,
    updatedAt: new Date().toISOString(),
  };
  await writeFile(
    path.join(jobDir, "status.json"),
    `${JSON.stringify(summary, null, 2)}\n`,
  );
  console.log(JSON.stringify(summary));
  return { jobDir, written, skipped, rowCount: rows.length };
}

/**
 * @returns {Promise<void>}
 */
async function main() {
  const repoRoot = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
  const options = parseTarponHarvestCli(process.argv.slice(2));
  await runTarponClick2GovHarvest(options, repoRoot);
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
