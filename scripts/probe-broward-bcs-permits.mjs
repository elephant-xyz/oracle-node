#!/usr/bin/env node
// @ts-check

import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { pathToFileURL } from "node:url";

import {
  BROWARD_BCS_PILOT_PARCEL_IDS,
  BROWARD_BCS_SCOPE_URL,
  BROWARD_BCS_SOURCE_SYSTEM,
  probeBrowardBcsPermits,
  renderBrowardBcsPermitJsonl,
  validateBrowardBcsParcelIds,
} from "./permit-source-adapters/broward-bcs-posse.mjs";

/**
 * @typedef {object} BrowardBcsCliOptions
 * @property {readonly string[]} parcelIds - Exact BCPA parcel IDs searched through the BCS Parcel ID field.
 * @property {boolean} isCuratedPilot - Whether the checked-in five-parcel evidence set was selected.
 * @property {string | null} outputPath - Optional local private-staging JSONL path.
 * @property {string | null} summaryPath - Optional local source-outcome JSON path.
 * @property {number} propertyDelayMs - Delay between property searches.
 * @property {number} detailDelayMs - Delay between detail-page requests.
 * @property {boolean} roofOnly - Whether list candidates must explicitly identify roofing.
 */

const USAGE = `Usage:
  node scripts/probe-broward-bcs-permits.mjs --pilot \\
    [--output <local-private.jsonl>] [--summary <local-summary.json>] \\
    [--property-delay-ms 1500] [--detail-delay-ms 300]

  node scripts/probe-broward-bcs-permits.mjs \\
    --parcel-id <12-character-id> [--parcel-id <id> ...] \\
    [--output <local-private.jsonl>] [--summary <local-summary.json>]

Scope and safety:
  - --pilot uses five permit-priority folios from existing Broward validation evidence.
  - Custom mode accepts one through five unique 12-character alphanumeric parcel IDs.
  - Letters are uppercased and retained; dashes, padding, and numeric coercion are rejected.
  - Searches and at most 75 permit/master details per parcel run sequentially.
  - The adapter accepts only records explicitly exposed by official BCS POSSE pages.
  - BCS coverage is BMSD/unincorporated plus BCS-held contract-city records, not countywide.
  - Output is local private staging and includes public contractor/address/legal-description data.
  - No AWS, queues, databases, IPFS, publication, login, CAPTCHA bypass, or full harvest is used.
  - --roof-only filters list rows before detail requests.
`;

/**
 * Read the value following one CLI flag.
 *
 * @param {readonly string[]} args - Raw arguments after the script path.
 * @param {number} index - Current argument index.
 * @param {string} flag - Flag name used in validation failures.
 * @returns {{ value: string, nextIndex: number }} Parsed value and consumed index.
 */
function readFollowingValue(args, index, flag) {
  const value = args[index + 1];
  if (value === undefined || value.startsWith("--")) {
    throw new Error(`${flag} requires a value`);
  }
  return { value, nextIndex: index + 1 };
}

/**
 * Parse one bounded millisecond delay.
 *
 * @param {string} value - Raw integer text.
 * @param {string} flag - Flag used in validation failures.
 * @param {number} minimum - Inclusive safety minimum.
 * @returns {number} Validated integer milliseconds.
 */
function parseDelay(value, flag, minimum) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum) {
    throw new Error(
      `${flag} must be an integer of at least ${String(minimum)}`,
    );
  }
  return parsed;
}

/**
 * Parse the deliberately small, local-only Broward BCS pilot CLI.
 *
 * @param {readonly string[]} args - Arguments after the script path.
 * @returns {BrowardBcsCliOptions | null} Options, or null when help was requested.
 */
export function parseOptions(args) {
  /** @type {string[]} */
  const explicitParcelIds = [];
  let pilot = false;
  let outputPath = null;
  let summaryPath = null;
  let propertyDelayMs = 1_500;
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
    if (argument === "--parcel-id") {
      const parsed = readFollowingValue(args, index, "--parcel-id");
      explicitParcelIds.push(parsed.value);
      index = parsed.nextIndex;
      continue;
    }
    if (argument.startsWith("--parcel-id=")) {
      explicitParcelIds.push(argument.slice("--parcel-id=".length));
      continue;
    }
    if (argument === "--output") {
      const parsed = readFollowingValue(args, index, "--output");
      outputPath = parsed.value.trim();
      index = parsed.nextIndex;
      continue;
    }
    if (argument.startsWith("--output=")) {
      outputPath = argument.slice("--output=".length).trim();
      continue;
    }
    if (argument === "--summary") {
      const parsed = readFollowingValue(args, index, "--summary");
      summaryPath = parsed.value.trim();
      index = parsed.nextIndex;
      continue;
    }
    if (argument.startsWith("--summary=")) {
      summaryPath = argument.slice("--summary=".length).trim();
      continue;
    }
    if (argument === "--property-delay-ms") {
      const parsed = readFollowingValue(args, index, "--property-delay-ms");
      propertyDelayMs = parseDelay(parsed.value, "--property-delay-ms", 1_000);
      index = parsed.nextIndex;
      continue;
    }
    if (argument.startsWith("--property-delay-ms=")) {
      propertyDelayMs = parseDelay(
        argument.slice("--property-delay-ms=".length),
        "--property-delay-ms",
        1_000,
      );
      continue;
    }
    if (argument === "--detail-delay-ms") {
      const parsed = readFollowingValue(args, index, "--detail-delay-ms");
      detailDelayMs = parseDelay(parsed.value, "--detail-delay-ms", 250);
      index = parsed.nextIndex;
      continue;
    }
    if (argument.startsWith("--detail-delay-ms=")) {
      detailDelayMs = parseDelay(
        argument.slice("--detail-delay-ms=".length),
        "--detail-delay-ms",
        250,
      );
      continue;
    }
    throw new Error(`Unknown option: ${argument}`);
  }

  if (pilot === explicitParcelIds.length > 0) {
    throw new Error(
      "Choose exactly one Broward BCS input mode: --pilot or --parcel-id",
    );
  }
  if (outputPath === "" || summaryPath === "") {
    throw new Error("--output and --summary paths must not be empty");
  }
  if (
    outputPath !== null &&
    summaryPath !== null &&
    outputPath === summaryPath
  ) {
    throw new Error("--output and --summary must use different paths");
  }

  const parcelIds = validateBrowardBcsParcelIds(
    pilot ? BROWARD_BCS_PILOT_PARCEL_IDS : explicitParcelIds,
    5,
  );
  return {
    parcelIds,
    isCuratedPilot: pilot,
    outputPath,
    summaryPath,
    propertyDelayMs,
    detailDelayMs,
    roofOnly,
  };
}

/**
 * Write one mode-0600 local artifact after creating its parent directory.
 *
 * @param {string} outputPath - Local artifact path.
 * @param {string} content - Complete UTF-8 artifact content.
 * @returns {Promise<void>} Resolves after the local file is written.
 */
async function writePrivateArtifact(outputPath, content) {
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, content, {
    encoding: "utf8",
    mode: 0o600,
  });
}

/**
 * Run the bounded local Broward BCS pilot and emit records plus provenance.
 *
 * @returns {Promise<void>} Resolves after local output and summary are complete.
 */
export async function main() {
  const options = parseOptions(process.argv.slice(2));
  if (options === null) {
    process.stdout.write(USAGE);
    return;
  }
  const startedAt = new Date().toISOString();
  const result = await probeBrowardBcsPermits({
    parcelIds: options.parcelIds,
    maxFolios: 5,
    propertyDelayMs: options.propertyDelayMs,
    detailDelayMs: options.detailDelayMs,
    maxDetailPagesPerFolio: 75,
    roofOnly: options.roofOnly,
  });
  const jsonl = renderBrowardBcsPermitJsonl(result.records);
  if (options.outputPath === null) {
    process.stdout.write(jsonl);
  } else {
    await writePrivateArtifact(options.outputPath, jsonl);
  }

  const summary = {
    event: "broward_bcs_permit_probe_completed",
    startedAt,
    completedAt: new Date().toISOString(),
    sourceSystem: BROWARD_BCS_SOURCE_SYSTEM,
    sourceScopeUrl: BROWARD_BCS_SCOPE_URL,
    coverageClaim:
      "BMSD/unincorporated and BCS-exposed contract-city records only; not countywide municipal coverage",
    isCuratedPilot: options.isCuratedPilot,
    roofOnly: options.roofOnly,
    parcelCount: options.parcelIds.length,
    normalizedRecordCount: result.records.length,
    outputPath: options.outputPath,
    observations: result.observations,
  };
  const summaryText = `${JSON.stringify(summary, null, 2)}\n`;
  if (options.summaryPath !== null) {
    await writePrivateArtifact(options.summaryPath, summaryText);
  }
  process.stderr.write(`${JSON.stringify(summary)}\n`);
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((caught) => {
    process.stderr.write(
      `${JSON.stringify({
        event: "broward_bcs_permit_probe_failed",
        error: caught instanceof Error ? caught.message : String(caught),
      })}\n`,
    );
    process.exitCode = 1;
  });
}
