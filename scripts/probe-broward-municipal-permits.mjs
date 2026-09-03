#!/usr/bin/env node
// @ts-check

import { mkdir, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

import {
  loadPermitAdapterCheckpoint,
  normalizePermitSearchQuery,
  renderMunicipalPermitJsonl,
  writePermitAdapterCheckpoint,
} from "./permit-source-adapters/bounded-permit-common.mjs";
import {
  BROWARD_PERMIT_JURISDICTIONS,
  getBrowardPermitJurisdiction,
} from "./permit-source-adapters/broward-permit-jurisdictions.mjs";
import { probeBoundedCitizenserve } from "./permit-source-adapters/citizenserve.mjs";
import { probeBoundedTylerCivicAccess } from "./permit-source-adapters/tyler-civic-access.mjs";

/**
 * @typedef {import("./permit-source-adapters/bounded-permit-common.mjs").PermitSearchQuery} PermitSearchQuery
 */

/**
 * Deliberately narrow local-only CLI options.
 *
 * @typedef {object} BrowardMunicipalPermitProbeOptions
 * @property {string} jurisdictionKey - Configured municipal source key.
 * @property {PermitSearchQuery} query - Exactly one folio or address query.
 * @property {string} outputDirectory - Local private artifact directory.
 * @property {number} maxPages - Search-page ceiling.
 * @property {number} maxDetails - Detail-page ceiling.
 * @property {number} searchDelayMs - Delay between source search pages.
 * @property {number} detailDelayMs - Delay between source detail pages.
 * @property {boolean} roofOnly - Whether result rows must explicitly identify roofing.
 */

const USAGE = `Usage:
  node scripts/probe-broward-municipal-permits.mjs \\
    --jurisdiction <key> (--folio <12-character-id> | --address <situs>) \\
    --output-dir <local-directory> [--max-pages 1] [--max-details 3] \\
    [--search-delay-ms 1500] [--detail-delay-ms 500]
    [--roof-only]

Jurisdictions:
${Object.values(BROWARD_PERMIT_JURISDICTIONS)
  .map(
    (config) =>
      `  ${config.key}${config.anonymousSearchCertified ? "" : " (documented skip: login required)"}`,
  )
  .join("\n")}

Safety:
  - Exactly one explicit property query; no seed input or crawl mode.
  - At most 3 result pages and 10 details, serialized and rate-delayed.
  - Local mode-0600 checkpoint, private JSONL, and summary files only.
  - No AWS, queues, databases, publication, credentials, or login attempts.
  - --roof-only filters result rows before detail requests.
`;

/**
 * Read the value following one CLI flag.
 *
 * @param {readonly string[]} args - Raw arguments after the script name.
 * @param {number} index - Current flag index.
 * @param {string} flag - Flag used in validation errors.
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
 * Parse an integer constrained to an inclusive range.
 *
 * @param {string} value - Raw numeric text.
 * @param {string} flag - Flag used in errors.
 * @param {number} minimum - Inclusive minimum.
 * @param {number} maximum - Inclusive maximum.
 * @returns {number} Validated integer.
 */
function parseBoundedInteger(value, flag, minimum, maximum) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(
      `${flag} must be an integer from ${String(minimum)} through ${String(maximum)}`,
    );
  }
  return parsed;
}

/**
 * Parse the one-property, bounded local probe command.
 *
 * @param {readonly string[]} args - Arguments after the script name.
 * @returns {BrowardMunicipalPermitProbeOptions | null} Options, or `null` for help.
 */
export function parseOptions(args) {
  /** @type {string | null} */
  let jurisdictionKey = null;
  /** @type {string | null} */
  let folio = null;
  /** @type {string | null} */
  let address = null;
  /** @type {string | null} */
  let outputDirectory = null;
  let maxPages = 1;
  let maxDetails = 3;
  let searchDelayMs = 1_500;
  let detailDelayMs = 500;
  let roofOnly = false;

  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--help" || argument === "-h") return null;
    if (argument === "--roof-only") {
      roofOnly = true;
      continue;
    }

    if (argument === "--jurisdiction") {
      const parsed = readFollowingValue(args, index, argument);
      jurisdictionKey = parsed.value;
      index = parsed.nextIndex;
      continue;
    }
    if (argument === "--folio") {
      const parsed = readFollowingValue(args, index, argument);
      folio = parsed.value;
      index = parsed.nextIndex;
      continue;
    }
    if (argument === "--address") {
      const parsed = readFollowingValue(args, index, argument);
      address = parsed.value;
      index = parsed.nextIndex;
      continue;
    }
    if (argument === "--output-dir") {
      const parsed = readFollowingValue(args, index, argument);
      outputDirectory = parsed.value;
      index = parsed.nextIndex;
      continue;
    }

    if (argument === "--max-pages") {
      const parsed = readFollowingValue(args, index, argument);
      maxPages = parseBoundedInteger(parsed.value, argument, 1, 3);
      index = parsed.nextIndex;
      continue;
    }
    if (argument === "--max-details") {
      const parsed = readFollowingValue(args, index, argument);
      maxDetails = parseBoundedInteger(parsed.value, argument, 1, 10);
      index = parsed.nextIndex;
      continue;
    }
    if (argument === "--search-delay-ms") {
      const parsed = readFollowingValue(args, index, argument);
      searchDelayMs = parseBoundedInteger(
        parsed.value,
        argument,
        1_000,
        60_000,
      );
      index = parsed.nextIndex;
      continue;
    }
    if (argument === "--detail-delay-ms") {
      const parsed = readFollowingValue(args, index, argument);
      detailDelayMs = parseBoundedInteger(parsed.value, argument, 250, 60_000);
      index = parsed.nextIndex;
      continue;
    }
    throw new Error(`Unknown option: ${argument}`);
  }

  if (
    typeof jurisdictionKey !== "string" ||
    jurisdictionKey.trim().length === 0
  ) {
    throw new Error("--jurisdiction is required");
  }
  const config = getBrowardPermitJurisdiction(jurisdictionKey);
  if (config.anonymousSearchCertified !== true) {
    throw new Error(config.skipReason ?? "Anonymous search is not certified");
  }
  if ((folio === null) === (address === null)) {
    throw new Error("Exactly one of --folio or --address is required");
  }
  const query =
    folio !== null
      ? normalizePermitSearchQuery({ kind: "folio", value: folio })
      : normalizePermitSearchQuery({
          kind: "address",
          value: /** @type {string} */ (address),
        });
  if (!config.searchKinds.includes(query.kind)) {
    throw new Error(
      `${config.city} does not support configured ${query.kind} search`,
    );
  }
  if (
    typeof outputDirectory !== "string" ||
    outputDirectory.trim().length === 0
  ) {
    throw new Error("--output-dir is required");
  }
  const localDirectory = outputDirectory.trim();
  if (localDirectory.includes("://")) {
    throw new Error("--output-dir must be a local filesystem path");
  }

  return {
    jurisdictionKey: config.key,
    query,
    outputDirectory: localDirectory,
    maxPages,
    maxDetails,
    searchDelayMs,
    detailDelayMs,
    roofOnly,
  };
}

/**
 * Execute one bounded local probe with atomic per-detail checkpointing.
 *
 * @param {BrowardMunicipalPermitProbeOptions} options - Validated CLI options.
 * @param {{citizenserveBrowser?:import("puppeteer").Browser}} [dependencies={}]
 *   Optional caller-owned warm resources. Supplying a browser changes only
 *   Chromium process ownership; every query still follows the complete
 *   rendered search, challenge check, detail reconciliation, and checkpoint
 *   path.
 * @returns {Promise<Readonly<Record<string, unknown>>>} Local run summary.
 */
export async function runProbe(options, dependencies = {}) {
  const config = getBrowardPermitJurisdiction(options.jurisdictionKey);
  if (config.anonymousSearchCertified !== true) {
    throw new Error(config.skipReason ?? "Anonymous search is not certified");
  }
  await mkdir(options.outputDirectory, { recursive: true });
  const checkpointPath = join(options.outputDirectory, "checkpoint.json");
  const recordsPath = join(options.outputDirectory, "records.private.jsonl");
  const summaryPath = join(options.outputDirectory, "summary.json");
  let checkpoint = await loadPermitAdapterCheckpoint(
    checkpointPath,
    config.sourceSystem,
    options.query,
  );
  await writePermitAdapterCheckpoint(checkpointPath, checkpoint);

  const startedAt = new Date().toISOString();
  const common = {
    config,
    query: options.query,
    maxPages: options.maxPages,
    maxDetails: options.maxDetails,
    searchDelayMs: options.searchDelayMs,
    detailDelayMs: options.detailDelayMs,
    roofOnly: options.roofOnly,
    checkpoint,
    onCheckpoint: async (
      /** @type {import("./permit-source-adapters/bounded-permit-common.mjs").PermitAdapterCheckpoint} */ nextCheckpoint,
    ) => {
      checkpoint = nextCheckpoint;
      await writePermitAdapterCheckpoint(checkpointPath, checkpoint);
    },
  };
  const result =
    config.vendor === "tyler-civic-access"
      ? await probeBoundedTylerCivicAccess(common)
      : await probeBoundedCitizenserve({
          ...common,
          config: {
            ...config,
            citizenserveInstallationId:
              requireCitizenserveInstallationId(config),
          },
          browser: dependencies.citizenserveBrowser,
        });

  await writePrivateFile(
    recordsPath,
    renderMunicipalPermitJsonl(result.records),
  );
  const summary = {
    event: "broward_municipal_permit_probe_completed",
    startedAt,
    finishedAt: new Date().toISOString(),
    jurisdictionKey: config.key,
    city: config.city,
    vendor: config.vendor,
    sourceSystem: config.sourceSystem,
    officialSourceUrl: config.officialSourceUrl,
    portalBaseUrl: config.portalBaseUrl,
    coverageNote: config.coverageNote,
    roofOnly: options.roofOnly,
    query: options.query,
    limits: {
      maxPages: options.maxPages,
      maxDetails: options.maxDetails,
      searchDelayMs: options.searchDelayMs,
      detailDelayMs: options.detailDelayMs,
    },
    reportedTotal: result.reportedTotal,
    reportedTotalPages: result.reportedTotalPages,
    capturedPermitCount: result.records.length,
    paginationTruncated: result.paginationTruncated,
    detailsTruncated: result.detailsTruncated,
    completedSearchPages: result.checkpoint.completedSearchPages,
    completedDetailCount: result.checkpoint.completedDetailIds.length,
    observations: result.observations,
    files: {
      checkpoint: checkpointPath,
      records: recordsPath,
      summary: summaryPath,
    },
  };
  await writePrivateFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`);
  return summary;
}

/**
 * Atomically write one owner-only local artifact.
 *
 * @param {string} destination - Final local path.
 * @param {string} contents - UTF-8 contents.
 * @returns {Promise<void>} Resolves after atomic replacement.
 */
async function writePrivateFile(destination, contents) {
  const temporary = `${destination}.${String(process.pid)}.tmp`;
  await writeFile(temporary, contents, {
    encoding: "utf8",
    mode: 0o600,
  });
  await rename(temporary, destination);
}

/**
 * Narrow a configured Citizenserve installation before adapter dispatch.
 *
 * @param {import("./permit-source-adapters/broward-permit-jurisdictions.mjs").BrowardPermitJurisdictionConfig} config - Jurisdiction configuration.
 * @returns {number} Positive Citizenserve installation ID.
 */
function requireCitizenserveInstallationId(config) {
  if (
    config.vendor !== "citizenserve" ||
    config.citizenserveInstallationId === null
  ) {
    throw new Error("Citizenserve jurisdiction has no installation ID");
  }
  return config.citizenserveInstallationId;
}

/**
 * CLI entry point.
 *
 * @returns {Promise<void>} Resolves after local artifacts and summary output.
 */
export async function main() {
  const options = parseOptions(process.argv.slice(2));
  if (options === null) {
    process.stdout.write(USAGE);
    return;
  }
  const summary = await runProbe(options);
  process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((caught) => {
    process.stderr.write(
      `${JSON.stringify({
        event: "broward_municipal_permit_probe_failed",
        error: caught instanceof Error ? caught.message : String(caught),
      })}\n`,
    );
    process.exitCode = 1;
  });
}
