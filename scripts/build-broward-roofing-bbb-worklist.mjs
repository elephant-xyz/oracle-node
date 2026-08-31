#!/usr/bin/env node
// @ts-check

/**
 * Build a private roofing-only contractor worklist for approved BBB API use.
 *
 * No BBB request is issued. Contractor identities come only from loaded
 * roofing permits, are deduplicated by usable license or normalized business
 * name, and exclude owner-builder/unknown placeholders.
 */

import { createHash } from "node:crypto";
import { mkdir, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import pg from "pg";

const { Client } = pg;
const EXPECTED_PROJECT_ID = "raspy-frost-51580436";
const PRODUCTION_ENDPOINT_PREFIX = "ep-mute-leaf";
const DEFAULT_OUTPUT_DIRECTORY =
  "downloads/broward/bbb-roofing-worklist";
const WORKLIST_SCHEMA_VERSION =
  "oracle-node.broward-bbb-roofing-worklist.v1";

/**
 * @typedef {object} RoofingBbbWorklistOptions
 * @property {string} outputDirectory - Private artifact directory.
 * @property {number | null} limit - Optional deterministic pilot limit.
 *
 * @typedef {object} RoofingContractorPermitRow
 * @property {string} source_system - Permit source system.
 * @property {string | null} contractor_name - Public contractor business name.
 * @property {string | null} contractor_license - Public contractor identifier.
 * @property {string | Date | null} permit_date - Best explicit permit date.
 *
 * @typedef {object} RoofingBbbCandidate
 * @property {string} identityKey - Stable license/name identity.
 * @property {string} contractorName - Selected public business name.
 * @property {string | null} contractorLicense - Normalized license ID.
 * @property {number} roofingPermitCount - Roofing permits attributed.
 * @property {string | null} earliestPermitDate - Earliest explicit ISO date.
 * @property {string | null} latestPermitDate - Latest explicit ISO date.
 * @property {string[]} sourceSystems - Permit sources supplying evidence.
 *
 * @typedef {object} RoofingBbbWorklistSummary
 * @property {typeof WORKLIST_SCHEMA_VERSION} schemaVersion - Worklist schema.
 * @property {string} generatedAt - ISO creation timestamp.
 * @property {"Broward"} county - Fixed county label.
 * @property {"roofing"} scope - Fixed trade scope.
 * @property {number} roofingPermitsWithContractor - Source permit rows read.
 * @property {number} excludedPlaceholderPermits - Owner-builder/TBD rows excluded.
 * @property {number} candidateCount - Unique BBB API candidates.
 * @property {number} candidatesWithLicense - Candidates keyed by license.
 * @property {number} candidatesByNameOnly - Candidates lacking usable license.
 * @property {number} accountedPermitCount - Candidate permits plus exclusions.
 * @property {boolean} allPermitRowsAccountedFor - Exact reconciliation flag.
 * @property {string} candidateSha256 - Deterministic candidate JSONL hash.
 * @property {string} worklistPath - Private output path.
 */

/**
 * Parse a bounded worklist command.
 *
 * @param {readonly string[]} argv - CLI arguments.
 * @returns {RoofingBbbWorklistOptions} Validated options.
 */
export function parseRoofingBbbWorklistOptions(argv) {
  const values = new Map();
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (
      typeof flag !== "string" ||
      !flag.startsWith("--") ||
      typeof value !== "string" ||
      value.startsWith("--")
    ) {
      throw new Error("BBB worklist options must be --flag value pairs");
    }
    values.set(flag.slice(2), value);
  }
  const outputDirectory =
    values.get("output-dir") ?? DEFAULT_OUTPUT_DIRECTORY;
  if (outputDirectory.length === 0) {
    throw new Error("--output-dir must not be empty");
  }
  const rawLimit = values.get("limit");
  const limit =
    rawLimit === undefined ? null : Number(rawLimit);
  if (
    limit !== null &&
    (!Number.isInteger(limit) || limit < 1 || limit > 100_000)
  ) {
    throw new Error("--limit must be an integer from 1 through 100000");
  }
  return { outputDirectory, limit };
}

/**
 * Normalize a business name for deterministic name-only deduplication.
 *
 * @param {unknown} value - Public contractor name.
 * @returns {string | null} Uppercase alphanumeric identity or null.
 */
export function normalizeRoofingContractorName(value) {
  if (typeof value !== "string") return null;
  const normalized = value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/gu, "")
    .toUpperCase()
    .replace(/&/gu, " AND ")
    .replace(/\b(?:LLC|L\.L\.C\.?|INCORPORATED|INC|CORPORATION|CORP)\b/gu, " ")
    .replace(/[^A-Z0-9]+/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
  return normalized.length >= 3 ? normalized : null;
}

/**
 * Normalize a usable public contractor license.
 *
 * @param {unknown} value - Public permit contractor identifier.
 * @returns {string | null} Stable uppercase identifier or null.
 */
export function normalizeRoofingContractorLicense(value) {
  if (typeof value !== "string") return null;
  const normalized = value.trim().toUpperCase().replace(/\s+/gu, "");
  if (
    normalized.length < 3 ||
    !/^[A-Z0-9./-]+$/u.test(normalized) ||
    /^(?:TBD|OB|OWNER|UNKNOWN|NONE|N\/A|0+)$/u.test(normalized)
  ) {
    return null;
  }
  return normalized;
}

/**
 * Determine whether a source contractor is not a BBB business candidate.
 *
 * @param {unknown} value - Public contractor name.
 * @returns {boolean} True for explicit placeholder/owner-builder text.
 */
export function isRoofingContractorPlaceholder(value) {
  if (typeof value !== "string") return true;
  return /\b(?:OWNER\s*\/?\s*BUILDER|TO\s+BE\s+DETERMINED|TBD|UNKNOWN|NOT\s+APPLICABLE|N\/A|NONE)\b/iu.test(
    value,
  );
}

/**
 * Aggregate roofing permit rows into a private BBB API candidate list.
 *
 * @param {readonly RoofingContractorPermitRow[]} rows - Roofing permit evidence.
 * @returns {{
 *   candidates:RoofingBbbCandidate[],
 *   excludedPlaceholderPermits:number
 * }} Deterministic candidate set and exclusion count.
 */
export function buildRoofingBbbCandidates(rows) {
  /**
   * @typedef {object} MutableCandidate
   * @property {string} identityKey
   * @property {Map<string,number>} names
   * @property {string | null} contractorLicense
   * @property {number} roofingPermitCount
   * @property {string | null} earliestPermitDate
   * @property {string | null} latestPermitDate
   * @property {Set<string>} sourceSystems
   */
  /** @type {Map<string, MutableCandidate>} */
  const candidates = new Map();
  let excludedPlaceholderPermits = 0;
  for (const row of rows) {
    const rawName =
      typeof row.contractor_name === "string"
        ? row.contractor_name.replace(/\s+/gu, " ").trim()
        : "";
    const normalizedName = normalizeRoofingContractorName(rawName);
    if (
      normalizedName === null ||
      isRoofingContractorPlaceholder(rawName)
    ) {
      excludedPlaceholderPermits += 1;
      continue;
    }
    const license = normalizeRoofingContractorLicense(
      row.contractor_license,
    );
    const identityKey =
      license === null
        ? `name:${normalizedName}`
        : `license:${license}`;
    const existing = candidates.get(identityKey) ?? {
      identityKey,
      names: new Map(),
      contractorLicense: license,
      roofingPermitCount: 0,
      earliestPermitDate: null,
      latestPermitDate: null,
      sourceSystems: new Set(),
    };
    existing.names.set(rawName, (existing.names.get(rawName) ?? 0) + 1);
    existing.roofingPermitCount += 1;
    existing.sourceSystems.add(row.source_system);
    const permitDate = normalizePermitDate(row.permit_date);
    if (permitDate !== null) {
      if (
        existing.earliestPermitDate === null ||
        permitDate < existing.earliestPermitDate
      ) {
        existing.earliestPermitDate = permitDate;
      }
      if (
        existing.latestPermitDate === null ||
        permitDate > existing.latestPermitDate
      ) {
        existing.latestPermitDate = permitDate;
      }
    }
    candidates.set(identityKey, existing);
  }
  return {
    candidates: [...candidates.values()]
      .map((candidate) => ({
        identityKey: candidate.identityKey,
        contractorName:
          [...candidate.names.entries()].sort(
            ([leftName, leftCount], [rightName, rightCount]) =>
              rightCount - leftCount ||
              leftName.localeCompare(rightName),
          )[0]?.[0] ?? "",
        contractorLicense: candidate.contractorLicense,
        roofingPermitCount: candidate.roofingPermitCount,
        earliestPermitDate: candidate.earliestPermitDate,
        latestPermitDate: candidate.latestPermitDate,
        sourceSystems: [...candidate.sourceSystems].sort(),
      }))
      .sort((left, right) =>
        left.identityKey.localeCompare(right.identityKey),
      ),
    excludedPlaceholderPermits,
  };
}

/**
 * Build the complete private worklist from isolated Neon.
 *
 * @param {RoofingBbbWorklistOptions} options - Validated options.
 * @returns {Promise<RoofingBbbWorklistSummary>} Aggregate-safe summary.
 */
export async function buildBrowardRoofingBbbWorklist(options) {
  const target = requireTarget(process.env);
  const client = new Client({
    connectionString: target.connectionString,
    application_name: "broward-bbb-roofing-worklist",
    connectionTimeoutMillis: 10_000,
    statement_timeout: 120_000,
  });
  await client.connect();
  /** @type {RoofingContractorPermitRow[]} */
  let rows;
  try {
    await verifyTarget(client, target);
    await client.query("BEGIN READ ONLY");
    try {
      const result = await client.query(
        `SELECT
           source_system,
           nullif(more_details->>'contractor_name','') AS contractor_name,
           nullif(more_details->>'contractor_license','') AS contractor_license,
           coalesce(
             permit_issue_date,
             application_received_date,
             opened_date
           ) AS permit_date
         FROM public.property_improvements
         WHERE source_system LIKE 'broward%permits'
           AND coalesce(
             more_details->>'is_roof_permit',
             more_details->>'isRoofPermit'
           )='true'
           AND nullif(more_details->>'contractor_name','') IS NOT NULL
         ORDER BY source_system,source_record_key
         LIMIT $1`,
        [options.limit],
      );
      rows =
        /** @type {RoofingContractorPermitRow[]} */ (result.rows);
      await client.query("ROLLBACK");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    }
  } finally {
    await client.end();
  }
  const aggregate = buildRoofingBbbCandidates(rows);
  if (aggregate.candidates.length === 0) {
    throw new Error("No usable roofing BBB candidates were found");
  }
  const outputDirectory = path.resolve(options.outputDirectory);
  const worklistPath = path.join(
    outputDirectory,
    "roofing-contractors.private.jsonl",
  );
  const summaryPath = path.join(outputDirectory, "summary.private.json");
  const worklistText = `${aggregate.candidates
    .map((candidate) => JSON.stringify(candidate))
    .join("\n")}\n`;
  const candidateSha256 = createHash("sha256")
    .update(worklistText)
    .digest("hex");
  const accountedPermitCount =
    aggregate.candidates.reduce(
      (sum, candidate) => sum + candidate.roofingPermitCount,
      0,
    ) + aggregate.excludedPlaceholderPermits;
  const summary = {
    schemaVersion:
      /** @type {typeof WORKLIST_SCHEMA_VERSION} */ (
        WORKLIST_SCHEMA_VERSION
      ),
    generatedAt: new Date().toISOString(),
    county: /** @type {"Broward"} */ ("Broward"),
    scope: /** @type {"roofing"} */ ("roofing"),
    roofingPermitsWithContractor: rows.length,
    excludedPlaceholderPermits: aggregate.excludedPlaceholderPermits,
    candidateCount: aggregate.candidates.length,
    candidatesWithLicense: aggregate.candidates.filter(
      (candidate) => candidate.contractorLicense !== null,
    ).length,
    candidatesByNameOnly: aggregate.candidates.filter(
      (candidate) => candidate.contractorLicense === null,
    ).length,
    accountedPermitCount,
    allPermitRowsAccountedFor: accountedPermitCount === rows.length,
    candidateSha256,
    worklistPath,
  };
  if (!summary.allPermitRowsAccountedFor) {
    throw new Error("Roofing BBB worklist did not reconcile source rows");
  }
  await Promise.all([
    writePrivateAtomic(worklistPath, worklistText),
    writePrivateAtomic(
      summaryPath,
      `${JSON.stringify(summary, null, 2)}\n`,
    ),
  ]);
  return summary;
}

/**
 * Normalize a PostgreSQL date result.
 *
 * @param {string | Date | null} value - Driver date value.
 * @returns {string | null} ISO calendar date.
 */
function normalizePermitDate(value) {
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  if (typeof value !== "string") return null;
  const match = /^(\d{4}-\d{2}-\d{2})/u.exec(value);
  return match?.[1] ?? null;
}

/**
 * Atomically write a mode-0600 private artifact.
 *
 * @param {string} filePath - Final private path.
 * @param {string} content - Complete content.
 * @returns {Promise<void>} Resolves after replacement.
 */
async function writePrivateAtomic(filePath, content) {
  await mkdir(path.dirname(filePath), { recursive: true, mode: 0o700 });
  const temporaryPath = `${filePath}.${String(process.pid)}.tmp`;
  await writeFile(temporaryPath, content, {
    encoding: "utf8",
    mode: 0o600,
  });
  await rename(temporaryPath, filePath);
}

/**
 * Validate isolated Neon configuration.
 *
 * @param {NodeJS.ProcessEnv} environment - Runtime environment.
 * @returns {{connectionString:string,expectedBranchId:string,expectedEndpointId:string}}
 *   Validated target.
 */
function requireTarget(environment) {
  const connectionString = environment.DATABASE_URL_UNPOOLED;
  const expectedBranchId = environment.BROWARD_INGEST_NEON_BRANCH_ID;
  const expectedEndpointId = environment.BROWARD_INGEST_NEON_ENDPOINT_ID;
  if (
    typeof connectionString !== "string" ||
    typeof expectedBranchId !== "string" ||
    !/^br-[a-z0-9-]+$/u.test(expectedBranchId) ||
    typeof expectedEndpointId !== "string" ||
    !/^ep-[a-z0-9-]+$/u.test(expectedEndpointId) ||
    expectedEndpointId.startsWith(PRODUCTION_ENDPOINT_PREFIX)
  ) {
    throw new Error("Verified direct Broward Neon target is required");
  }
  if (new URL(connectionString).hostname.includes("-pooler")) {
    throw new Error("BBB worklist requires direct Neon");
  }
  return { connectionString, expectedBranchId, expectedEndpointId };
}

/**
 * Prove exact Neon project, branch, and endpoint.
 *
 * @param {import("pg").Client} client - Connected direct client.
 * @param {{expectedBranchId:string,expectedEndpointId:string}} target - IDs.
 * @returns {Promise<void>} Resolves only for isolated Broward.
 */
async function verifyTarget(client, target) {
  const result = await client.query(
    `SELECT current_setting('neon.project_id',true) AS project_id,
            current_setting('neon.branch_id',true) AS branch_id,
            current_setting('neon.endpoint_id',true) AS endpoint_id`,
  );
  const row = result.rows[0];
  if (
    row?.project_id !== EXPECTED_PROJECT_ID ||
    row.branch_id !== target.expectedBranchId ||
    row.endpoint_id !== target.expectedEndpointId
  ) {
    throw new Error("BBB worklist target is not isolated broward-ingest");
  }
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  buildBrowardRoofingBbbWorklist(
    parseRoofingBbbWorklistOptions(process.argv.slice(2)),
  )
    .then((summary) => {
      console.log(
        JSON.stringify({
          event: "broward_bbb_roofing_worklist_built",
          roofingPermitsWithContractor:
            summary.roofingPermitsWithContractor,
          excludedPlaceholderPermits:
            summary.excludedPlaceholderPermits,
          candidateCount: summary.candidateCount,
          candidatesWithLicense: summary.candidatesWithLicense,
          candidatesByNameOnly: summary.candidatesByNameOnly,
          candidateSha256: summary.candidateSha256,
        }),
      );
    })
    .catch((error) => {
      console.error(
        JSON.stringify({
          event: "broward_bbb_roofing_worklist_failed",
          message: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      process.exitCode = 1;
    });
}
