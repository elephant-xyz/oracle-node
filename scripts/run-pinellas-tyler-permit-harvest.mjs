#!/usr/bin/env node
// @ts-check

/**
 * Tyler Civic Access / EnerGov CSS keyword harvest for Pinellas cities.
 *
 * One Chrome tab, delay ≥1s between pages, skip-existing by permit number.
 * Writes under downloads/pinellas/permits/<jobId>/.
 */

import { mkdir, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { harvestTylerCivicAccessPages } from "./permit-source-adapters/tyler-civic-access.mjs";
import { resolvePinellasTylerAgency } from "./pinellas/tyler-agencies.mjs";

/**
 * @typedef {import("./permit-source-adapters/tyler-civic-access.mjs").NormalizedCityPermit} NormalizedCityPermit
 */

/**
 * @typedef {object} TylerHarvestCli
 * @property {string} agencyKey `largo` or `park`.
 * @property {string} jobId Output directory name.
 * @property {readonly string[]} queries Keywords to paginate.
 * @property {number} delayMs Delay between page fetches.
 * @property {boolean} skipExisting Skip permit JSON that already exists.
 */

/**
 * @param {readonly string[]} args CLI args after the script path.
 * @returns {TylerHarvestCli} Parsed flags.
 */
export function parseTylerHarvestCli(args) {
  /** @type {Map<string, string>} */
  const values = new Map();
  /** @type {string[]} */
  const queries = [];
  for (let index = 0; index < args.length; index += 1) {
    const token = args[index];
    if (token === undefined || token.startsWith("--") === false) continue;
    const key = token.slice(2);
    const next = args[index + 1];
    if (
      key === "query" &&
      next !== undefined &&
      next.startsWith("--") === false
    ) {
      queries.push(next);
      index += 1;
      continue;
    }
    if (next !== undefined && next.startsWith("--") === false) {
      values.set(key, next);
      index += 1;
    } else {
      values.set(key, "true");
    }
  }
  const agency = resolvePinellasTylerAgency(values.get("agency"));
  const today = new Date().toISOString().slice(0, 10).replaceAll("-", "");
  const resolvedQueries =
    queries.length > 0 ? queries : [...agency.defaultProbeQueries];
  const delayMs = Number.parseInt(values.get("delay-ms") ?? "1500", 10);
  if (!Number.isInteger(delayMs) || delayMs < 1_000) {
    throw new Error("--delay-ms must be an integer of at least 1000");
  }
  return {
    agencyKey: agency.key,
    jobId: values.get("job-id") ?? `${agency.jobIdPrefix}-full-${today}`,
    queries: resolvedQueries,
    delayMs,
    skipExisting: values.get("skip-existing") !== "false",
  };
}

/**
 * @param {string} permitNumber Permit id.
 * @returns {string} Filesystem-safe stem.
 */
export function tylerPermitFileStem(permitNumber) {
  return permitNumber.toLowerCase().replace(/[^a-z0-9]+/g, "-");
}

/**
 * @param {TylerHarvestCli} options CLI options.
 * @param {string} repoRoot oracle-node root.
 * @returns {Promise<{ jobDir: string, written: number, skipped: number, pageCount: number }>}
 *   Harvest totals.
 */
export async function runPinellasTylerPermitHarvest(options, repoRoot) {
  process.env.CHROME_EXECUTABLE_PATH ??= "/usr/local/bin/google-chrome";
  const agency = resolvePinellasTylerAgency(options.agencyKey);
  const jobDir = path.resolve(
    repoRoot,
    "downloads/pinellas/permits",
    options.jobId,
  );
  const extractedDir = path.join(jobDir, "extracted");
  await mkdir(extractedDir, { recursive: true });
  const startedAt = Date.now();
  const result = await harvestTylerCivicAccessPages({
    config: agency.config,
    queries: options.queries,
    delayMs: options.delayMs,
  });
  let written = 0;
  let skipped = 0;
  for (const record of result.records) {
    const jsonPath = path.join(
      extractedDir,
      `${tylerPermitFileStem(record.permit_number)}.json`,
    );
    if (options.skipExisting && existsSync(jsonPath)) {
      skipped += 1;
      continue;
    }
    /** @type {NormalizedCityPermit & { source: string }} */
    const stamped = { ...record, source: agency.sourceStamp };
    await writeFile(jsonPath, `${JSON.stringify(stamped, null, 2)}\n`);
    written += 1;
  }
  const summary = {
    event: "pinellas_tyler_permit_harvest_complete",
    agency: agency.key,
    jobId: options.jobId,
    queries: options.queries,
    pageCount: result.pages.length,
    recordCount: result.records.length,
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
  return {
    jobDir,
    written,
    skipped,
    pageCount: result.pages.length,
  };
}

/**
 * @returns {Promise<void>}
 */
async function main() {
  const repoRoot = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
  const options = parseTylerHarvestCli(process.argv.slice(2));
  await runPinellasTylerPermitHarvest(options, repoRoot);
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
