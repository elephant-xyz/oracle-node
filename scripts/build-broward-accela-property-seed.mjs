#!/usr/bin/env node
// @ts-check

/**
 * Build a private, jurisdiction-qualified Broward property seed for the three
 * Accela gap fillers. The public GIS seed is intentionally not used because it
 * has parcel geometry but no BCPA situs city/address. This builder reads only
 * the independently verified isolated Broward Neon target and routes the
 * retained BCPA situs string through the executable permit registry.
 */

import { createHash } from "node:crypto";
import { mkdir, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import pg from "pg";

import { normalizeBrowardFolio } from "./broward-folio.mjs";
import { resolveBrowardPermitJurisdiction } from "./broward-permit-jurisdictions.mjs";

const { Client } = pg;
const EXPECTED_PROJECT_ID = "raspy-frost-51580436";
const PRODUCTION_ENDPOINT_PREFIX = "ep-mute-leaf";
const DEFAULT_OUTPUT_PATH =
  "downloads/broward/broward-accela-property-seed.private.csv";
const TARGET_JURISDICTIONS = Object.freeze({
  plantation: "Plantation",
  "cooper-city": "Cooper City",
  weston: "Weston",
});
const SEED_COLUMNS = /** @type {const} */ ([
  "request_identifier",
  "jurisdiction_key",
  "city",
  "address",
]);

/**
 * @typedef {"plantation" | "cooper-city" | "weston"} AccelaSeedJurisdictionKey
 *
 * @typedef {object} AccelaPropertySeedOptions
 * @property {string} outputPath - Private CSV destination.
 *
 * @typedef {object} BrowardPropertyCandidate
 * @property {unknown} request_identifier - Candidate BCPA folio.
 * @property {unknown} unnormalized_address - Candidate BCPA situs string.
 *
 * @typedef {object} AccelaPropertySeedRow
 * @property {string} request_identifier - Canonical 12-character BCPA folio.
 * @property {AccelaSeedJurisdictionKey} jurisdiction_key - Exact registry key.
 * @property {string} city - Canonical public municipality label.
 * @property {string} address - Private BCPA situs routing evidence.
 *
 * @typedef {object} AccelaPropertySeedResult
 * @property {AccelaPropertySeedRow[]} rows - Deterministically ordered rows.
 * @property {number} inputCount - Candidate database rows considered.
 * @property {number} invalidCount - Rows without a valid folio or situs string.
 * @property {number} unresolvedCount - Rows the executable registry could not route.
 * @property {number} otherJurisdictionCount - Valid rows outside the three targets.
 * @property {number} duplicateCount - Repeated identical target folios removed.
 */

/**
 * Parse the private seed output destination.
 *
 * @param {readonly string[]} argv - CLI arguments following the script path.
 * @returns {AccelaPropertySeedOptions} Validated builder options.
 */
export function parseAccelaPropertySeedOptions(argv) {
  if (argv.length === 0) {
    return { outputPath: path.resolve(DEFAULT_OUTPUT_PATH) };
  }
  if (
    argv.length !== 2 ||
    argv[0] !== "--output" ||
    argv[1] === undefined ||
    argv[1].trim().length === 0
  ) {
    throw new Error("Usage: --output <private-csv-path>");
  }
  return { outputPath: path.resolve(argv[1]) };
}

/**
 * Route database candidates into a deterministic, deduplicated private seed.
 *
 * @param {readonly BrowardPropertyCandidate[]} candidates - BCPA property rows.
 * @returns {AccelaPropertySeedResult} Exact routing and exclusion accounting.
 */
export function createBrowardAccelaPropertySeedRows(candidates) {
  /** @type {Map<string, AccelaPropertySeedRow>} */
  const retained = new Map();
  let invalidCount = 0;
  let unresolvedCount = 0;
  let otherJurisdictionCount = 0;
  let duplicateCount = 0;
  for (const candidate of candidates) {
    const folio = normalizeBrowardFolio(candidate.request_identifier);
    const address =
      typeof candidate.unnormalized_address === "string"
        ? candidate.unnormalized_address.replace(/\s+/gu, " ").trim()
        : "";
    if (folio === undefined || address.length === 0) {
      invalidCount += 1;
      continue;
    }
    const resolution = resolveBrowardPermitJurisdiction({
      situsAddress: address,
    });
    const jurisdictionKey = resolution.jurisdiction?.key;
    if (jurisdictionKey === undefined) {
      unresolvedCount += 1;
      continue;
    }
    if (!isAccelaSeedJurisdictionKey(jurisdictionKey)) {
      otherJurisdictionCount += 1;
      continue;
    }
    const row = {
      request_identifier: folio,
      jurisdiction_key: jurisdictionKey,
      city: TARGET_JURISDICTIONS[jurisdictionKey],
      address,
    };
    const prior = retained.get(folio);
    if (prior !== undefined) {
      if (
        prior.jurisdiction_key !== row.jurisdiction_key ||
        prior.address !== row.address
      ) {
        throw new Error("Conflicting BCPA jurisdiction evidence for one folio");
      }
      duplicateCount += 1;
      continue;
    }
    retained.set(folio, row);
  }
  return {
    rows: [...retained.values()].sort(
      (left, right) =>
        left.jurisdiction_key.localeCompare(right.jurisdiction_key) ||
        left.request_identifier.localeCompare(right.request_identifier),
    ),
    inputCount: candidates.length,
    invalidCount,
    unresolvedCount,
    otherJurisdictionCount,
    duplicateCount,
  };
}

/**
 * Render one private seed row with RFC 4180-compatible cell escaping.
 *
 * @param {AccelaPropertySeedRow} row - Validated target property.
 * @returns {string} CSV data line without a trailing newline.
 */
export function renderBrowardAccelaPropertySeedRow(row) {
  return SEED_COLUMNS.map((column) => encodeCsvCell(row[column])).join(",");
}

/**
 * Build the complete private seed after verifying the isolated Neon identity.
 *
 * @param {AccelaPropertySeedOptions} options - Output configuration.
 * @param {NodeJS.ProcessEnv} [environment=process.env] - Runtime target secrets.
 * @returns {Promise<{
 *   rowCount:number,
 *   jurisdictionCounts:Record<AccelaSeedJurisdictionKey,number>,
 *   sha256:string,
 *   outputPath:string
 * }>} Aggregate-only seed receipt.
 */
export async function buildBrowardAccelaPropertySeed(
  options,
  environment = process.env,
) {
  const target = requireTarget(environment);
  const client = new Client({
    connectionString: target.connectionString,
    application_name: "broward-accela-property-seed",
    connectionTimeoutMillis: 10_000,
    statement_timeout: 120_000,
  });
  await client.connect();
  try {
    await verifyTarget(client, target);
    const result = await client.query(
      `SELECT p.request_identifier, a.unnormalized_address
       FROM public.properties p
       JOIN public.addresses a ON a.address_id = p.address_id
       WHERE p.source_system = 'broward_appraiser'
         AND p.request_identifier IS NOT NULL
         AND a.unnormalized_address IS NOT NULL`,
    );
    const candidates =
      /** @type {BrowardPropertyCandidate[]} */ (result.rows);
    const built = createBrowardAccelaPropertySeedRows(candidates);
    if (built.rows.length === 0) {
      throw new Error("Verified Broward target produced no Accela seed rows");
    }
    /** @type {Record<AccelaSeedJurisdictionKey, number>} */
    const jurisdictionCounts = {
      plantation: 0,
      "cooper-city": 0,
      weston: 0,
    };
    for (const row of built.rows) {
      jurisdictionCounts[row.jurisdiction_key] += 1;
    }
    for (const [jurisdictionKey, count] of Object.entries(
      jurisdictionCounts,
    )) {
      if (count === 0) {
        throw new Error(
          `Verified Broward target produced no ${jurisdictionKey} seed rows`,
        );
      }
    }
    const content = `${SEED_COLUMNS.join(",")}\n${built.rows
      .map((row) => renderBrowardAccelaPropertySeedRow(row))
      .join("\n")}\n`;
    await writePrivateAtomic(options.outputPath, content);
    return {
      rowCount: built.rows.length,
      jurisdictionCounts,
      sha256: createHash("sha256").update(content).digest("hex"),
      outputPath: options.outputPath,
    };
  } finally {
    await client.end();
  }
}

/**
 * @param {string} value - CSV field.
 * @returns {string} Escaped field.
 */
function encodeCsvCell(value) {
  return /[",\r\n]/u.test(value)
    ? `"${value.replaceAll('"', '""')}"`
    : value;
}

/**
 * @param {string} value - Candidate registry key.
 * @returns {value is AccelaSeedJurisdictionKey} Supported target assertion.
 */
function isAccelaSeedJurisdictionKey(value) {
  return Object.hasOwn(TARGET_JURISDICTIONS, value);
}

/**
 * @param {import("pg").Client} client - Connected Neon client.
 * @param {{expectedBranchId:string,expectedEndpointId:string}} target
 *   Independently configured isolated identifiers.
 * @returns {Promise<void>} Resolves only for the exact isolated target.
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
    throw new Error("Accela property seed target is not isolated broward-ingest");
  }
}

/**
 * @param {NodeJS.ProcessEnv} environment - Runtime secrets and target IDs.
 * @returns {{connectionString:string,expectedBranchId:string,expectedEndpointId:string}}
 *   Validated target configuration that is never logged.
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
  return { connectionString, expectedBranchId, expectedEndpointId };
}

/**
 * Atomically write private property routing evidence.
 *
 * @param {string} filePath - Private output path.
 * @param {string} content - Complete deterministic CSV.
 * @returns {Promise<void>} Resolves after atomic replacement.
 */
async function writePrivateAtomic(filePath, content) {
  await mkdir(path.dirname(filePath), { recursive: true, mode: 0o700 });
  const temporaryPath = `${filePath}.${String(process.pid)}.tmp`;
  await writeFile(temporaryPath, content, { mode: 0o600 });
  await rename(temporaryPath, filePath);
}

if (
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href
) {
  buildBrowardAccelaPropertySeed(
    parseAccelaPropertySeedOptions(process.argv.slice(2)),
  )
    .then((receipt) => {
      console.log(
        JSON.stringify({
          event: "broward_accela_property_seed_built",
          rowCount: receipt.rowCount,
          jurisdictionCounts: receipt.jurisdictionCounts,
          sha256: receipt.sha256,
        }),
      );
    })
    .catch((error) => {
      console.error(
        JSON.stringify({
          event: "broward_accela_property_seed_failed",
          message: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      process.exitCode = 1;
    });
}
