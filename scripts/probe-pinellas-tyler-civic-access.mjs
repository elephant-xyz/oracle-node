#!/usr/bin/env node
// @ts-check

/**
 * Conservative Tyler Civic Access / EnerGov CSS keyword probe for Pinellas
 * cities (Largo, Pinellas Park). 1–2 lookups, delay ≥1s, max 10.
 *
 * Requires Chrome. Do not run while stacking extra Accela tabs on the same
 * tenant; Largo/Park are different hosts from county Accela.
 */

import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import {
  probeTylerCivicAccess,
  renderNormalizedPermitJsonl,
} from "./permit-source-adapters/tyler-civic-access.mjs";
import { resolvePinellasTylerAgency } from "./pinellas/tyler-agencies.mjs";

/**
 * @typedef {object} PinellasTylerProbeCli
 * @property {string} agencyKey `largo` or `park`.
 * @property {readonly string[]} queries Keyword lookups.
 * @property {number} delayMs Delay between lookups.
 * @property {string} outputDir Directory for JSONL + report.
 */

const USAGE = `Usage:
  node scripts/probe-pinellas-tyler-civic-access.mjs [--agency largo|park] \\
    [--query <keyword>] [--query <keyword>] [--delay-ms 1500]

Defaults to two Largo street keywords. Max 10 queries. Delay ≥ 1000 ms.
`;

/**
 * @param {readonly string[]} args CLI args after the script path.
 * @param {number} index Flag index.
 * @param {string} flag Flag name.
 * @returns {{ value: string, nextIndex: number }} Value + next index.
 */
function readFollowingValue(args, index, flag) {
  const value = args[index + 1];
  if (value === undefined || value.startsWith("--")) {
    throw new Error(`${flag} requires a value`);
  }
  return { value, nextIndex: index + 1 };
}

/**
 * @param {string} value Raw delay.
 * @returns {number} Milliseconds ≥ 1000.
 */
function parseDelayMs(value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1_000) {
    throw new Error("--delay-ms must be an integer of at least 1000");
  }
  return parsed;
}

/**
 * @param {readonly string[]} args Args after the script name.
 * @returns {PinellasTylerProbeCli | null} Options, or null for --help.
 */
export function parsePinellasTylerProbeOptions(args) {
  /** @type {string[]} */
  const queries = [];
  let agencyKey = "largo";
  let delayMs = 1_500;

  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--help" || argument === "-h") return null;
    if (argument === "--agency") {
      const parsed = readFollowingValue(args, index, "--agency");
      agencyKey = parsed.value;
      index = parsed.nextIndex;
      continue;
    }
    if (argument === "--query") {
      const parsed = readFollowingValue(args, index, "--query");
      queries.push(parsed.value);
      index = parsed.nextIndex;
      continue;
    }
    if (argument.startsWith("--query=")) {
      queries.push(argument.slice("--query=".length));
      continue;
    }
    if (argument === "--delay-ms") {
      const parsed = readFollowingValue(args, index, "--delay-ms");
      delayMs = parseDelayMs(parsed.value);
      index = parsed.nextIndex;
      continue;
    }
    throw new Error(`Unknown option: ${argument}`);
  }

  const agency = resolvePinellasTylerAgency(agencyKey);
  const resolvedQueries =
    queries.length > 0 ? queries : [...agency.defaultProbeQueries];
  if (resolvedQueries.length > 10) {
    throw new Error("Refusing more than 10 Tyler Civic Access probe lookups");
  }
  const today = new Date().toISOString().slice(0, 10).replaceAll("-", "");
  return {
    agencyKey: agency.key,
    queries: resolvedQueries,
    delayMs,
    outputDir: path.join(
      "downloads/pinellas/permits",
      `${agency.jobIdPrefix}-probe-${today}`,
    ),
  };
}

/**
 * @returns {Promise<void>}
 */
export async function main() {
  const parsed = parsePinellasTylerProbeOptions(process.argv.slice(2));
  if (parsed === null) {
    process.stdout.write(USAGE);
    return;
  }
  const agency = resolvePinellasTylerAgency(parsed.agencyKey);
  process.env.CHROME_EXECUTABLE_PATH ??= "/usr/local/bin/google-chrome";
  const repoRoot = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
  const outputDir = path.resolve(repoRoot, parsed.outputDir);
  await mkdir(outputDir, { recursive: true });

  const result = await probeTylerCivicAccess({
    config: agency.config,
    queries: parsed.queries,
    maxLookups: 10,
    delayMs: parsed.delayMs,
  });
  const jsonl = renderNormalizedPermitJsonl(result.records);
  const jsonlPath = path.join(outputDir, "normalized.jsonl");
  await writeFile(jsonlPath, jsonl, { encoding: "utf8" });
  const report = {
    event: "pinellas_tyler_civic_access_probe",
    agency: agency.key,
    jurisdiction: agency.jurisdiction,
    portalBaseUrl: agency.config.portalBaseUrl,
    sourceStamp: agency.sourceStamp,
    lookupCount: result.observations.length,
    normalizedPermitCount: result.records.length,
    queries: parsed.queries,
    observations: result.observations,
    jsonlPath,
    probedAt: new Date().toISOString(),
  };
  await writeFile(
    path.join(outputDir, "probe-report.json"),
    `${JSON.stringify(report, null, 2)}\n`,
  );
  console.log(JSON.stringify(report, null, 2));
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
