#!/usr/bin/env node
// @ts-check

import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { pathToFileURL } from "node:url";

import {
  probeTylerCivicAccess,
  renderNormalizedPermitJsonl,
} from "./permit-source-adapters/tyler-civic-access.mjs";

/**
 * @typedef {object} ProbeCliOptions
 * @property {readonly string[]} queries - Public Rock Island permit numbers or address keywords.
 * @property {string | null} outputPath - Optional local normalized JSONL path.
 * @property {number} delayMs - Delay between lookups.
 */

const ROCK_ISLAND_TYLER_CONFIG = Object.freeze({
  portalBaseUrl:
    "https://cityofrockislandil-energovweb.tylerhost.net/apps/selfservice",
  city: "Rock Island",
  sourceSystem: "rock_island_city_tyler_permits",
});

const USAGE = `Usage:
  node scripts/probe-rock-island-permits.mjs \\
    --query <permit-number-or-address> [--query <value> ...] \\
    [--output <local-normalized.jsonl>] [--delay-ms 1500]

Safety:
  - Requires at least one explicit query.
  - Refuses more than 10 queries.
  - Runs sequentially with at least 1000 ms between lookups.
  - Writes only the normalized city-permit allow-list; no contacts or raw responses.
  - Does not use AWS, queues, Neon, IPFS, CAPTCHA bypasses, or authenticated access.
`;

/**
 * Read the value following a command-line flag.
 *
 * @param {readonly string[]} args - Raw arguments after the script name.
 * @param {number} index - Current flag index.
 * @param {string} flag - Flag name used in validation errors.
 * @returns {{ value: string, nextIndex: number }} Parsed value and next consumed index.
 */
function readFollowingValue(args, index, flag) {
  const value = args[index + 1];
  if (value === undefined || value.startsWith("--")) {
    throw new Error(`${flag} requires a value`);
  }
  return { value, nextIndex: index + 1 };
}

/**
 * Parse a safe integer delay for serialized public lookups.
 *
 * @param {string} value - Raw delay text.
 * @returns {number} Delay in milliseconds.
 */
function parseDelayMs(value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1_000) {
    throw new Error("--delay-ms must be an integer of at least 1000");
  }
  return parsed;
}

/**
 * Parse the deliberately small Rock Island permit-probe CLI surface.
 *
 * @param {readonly string[]} args - Arguments after the script name.
 * @returns {ProbeCliOptions | null} Options, or `null` when help was requested.
 */
export function parseOptions(args) {
  /** @type {string[]} */
  const queries = [];
  let outputPath = null;
  let delayMs = 1_500;

  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--help" || argument === "-h") return null;

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
    if (argument === "--output") {
      const parsed = readFollowingValue(args, index, "--output");
      outputPath = parsed.value;
      index = parsed.nextIndex;
      continue;
    }
    if (argument.startsWith("--output=")) {
      outputPath = argument.slice("--output=".length);
      continue;
    }
    if (argument === "--delay-ms") {
      const parsed = readFollowingValue(args, index, "--delay-ms");
      delayMs = parseDelayMs(parsed.value);
      index = parsed.nextIndex;
      continue;
    }
    if (argument.startsWith("--delay-ms=")) {
      delayMs = parseDelayMs(argument.slice("--delay-ms=".length));
      continue;
    }
    throw new Error(`Unknown option: ${argument}`);
  }

  if (queries.length === 0) {
    throw new Error("At least one --query is required");
  }
  if (queries.length > 10) {
    throw new Error("Refusing more than 10 Rock Island permit lookups");
  }
  if (outputPath !== null && outputPath.trim().length === 0) {
    throw new Error("--output must not be empty");
  }

  return {
    queries,
    outputPath: outputPath === null ? null : outputPath.trim(),
    delayMs,
  };
}

/**
 * Run the approved local-only Rock Island Tyler Civic Access pilot.
 *
 * @returns {Promise<void>} Resolves after deterministic JSONL and a stderr summary are written.
 */
export async function main() {
  const options = parseOptions(process.argv.slice(2));
  if (options === null) {
    process.stdout.write(USAGE);
    return;
  }

  const result = await probeTylerCivicAccess({
    config: ROCK_ISLAND_TYLER_CONFIG,
    queries: options.queries,
    maxLookups: 10,
    delayMs: options.delayMs,
  });
  const jsonl = renderNormalizedPermitJsonl(result.records);

  if (options.outputPath === null) {
    process.stdout.write(jsonl);
  } else {
    await mkdir(dirname(options.outputPath), { recursive: true });
    await writeFile(options.outputPath, jsonl, {
      encoding: "utf8",
      mode: 0o600,
    });
  }

  process.stderr.write(
    `${JSON.stringify({
      event: "rock_island_permit_probe_completed",
      sourceSystem: ROCK_ISLAND_TYLER_CONFIG.sourceSystem,
      lookupCount: result.observations.length,
      normalizedPermitCount: result.records.length,
      outputPath: options.outputPath,
      observations: result.observations,
    })}\n`,
  );
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((caught) => {
    process.stderr.write(
      `${JSON.stringify({
        event: "rock_island_permit_probe_failed",
        error: caught instanceof Error ? caught.message : String(caught),
      })}\n`,
    );
    process.exitCode = 1;
  });
}
