#!/usr/bin/env node
// @ts-check

/**
 * Build a private BCPA property-first seed for municipal sources that expose
 * no complete broad list. Jurisdiction comes only from the executable situs
 * registry. Coconut Creek and Lauderhill use exact folios; Click2Gov tenants
 * use deduplicated base situs addresses because their segmented parcel fields
 * have no certified BCPA mapping.
 */

import { createHash } from "node:crypto";
import { mkdir, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import pg from "pg";

import { normalizeBrowardFolio } from "./broward-folio.mjs";
import { resolveBrowardPermitJurisdiction } from "./broward-permit-jurisdictions.mjs";
import { parseMunicipalStreetAddress } from "./permit-source-adapters/broward-municipal-transport.mjs";

const { Client } = pg;
const EXPECTED_PROJECT_ID = "raspy-frost-51580436";
const PRODUCTION_ENDPOINT_PREFIX = "ep-mute-leaf";
const DEFAULT_OUTPUT_PATH =
  "downloads/broward/broward-municipal-property-seed.private.csv";
const DEFAULT_GAP_OUTPUT_PATH =
  "downloads/broward/broward-municipal-property-seed-gaps.private.csv";
const TARGET_QUERY_KIND = Object.freeze({
  "coconut-creek": /** @type {const} */ ("folio"),
  lauderhill: /** @type {const} */ ("folio"),
  margate: /** @type {const} */ ("address"),
  "pompano-beach": /** @type {const} */ ("address"),
  tamarac: /** @type {const} */ ("address"),
});
const SEED_COLUMNS = /** @type {const} */ ([
  "jurisdiction_key",
  "query_kind",
  "query_value",
  "property_count",
]);
const GAP_COLUMNS = /** @type {const} */ ([
  "jurisdiction_key",
  "property_identifier",
  "reason",
]);
const FULL_SUFFIX_TO_ABBREVIATION = Object.freeze({
  ALLEY: "ALY",
  ANNEX: "ANX",
  ARCADE: "ARC",
  AVENUE: "AVE",
  BAYOU: "BYU",
  BEACH: "BCH",
  BEND: "BND",
  BLUFF: "BLF",
  BOULEVARD: "BLVD",
  BRANCH: "BR",
  BRIDGE: "BRG",
  BROOK: "BRK",
  BURG: "BG",
  BYPASS: "BYP",
  CAMP: "CP",
  CANYON: "CYN",
  CAPE: "CPE",
  CAUSEWAY: "CSWY",
  CENTER: "CTR",
  CIRCLE: "CIR",
  CLIFF: "CLF",
  CLUB: "CLB",
  COMMON: "CMN",
  CORNER: "COR",
  COURSE: "CRSE",
  COURT: "CT",
  COVE: "CV",
  CREEK: "CRK",
  CRESCENT: "CRES",
  CREST: "CRST",
  CROSSING: "XING",
  DALE: "DL",
  DAM: "DM",
  DIVIDE: "DV",
  DRIVE: "DR",
  ESTATE: "EST",
  EXPRESSWAY: "EXPY",
  EXTENSION: "EXT",
  FALLS: "FLS",
  FERRY: "FRY",
  FIELD: "FLD",
  FLAT: "FLT",
  FORD: "FRD",
  FOREST: "FRST",
  FORGE: "FRG",
  FORK: "FRK",
  FORT: "FT",
  FREEWAY: "FWY",
  GARDEN: "GDN",
  GATEWAY: "GTWY",
  GLEN: "GLN",
  GREEN: "GRN",
  GROVE: "GRV",
  HARBOR: "HBR",
  HAVEN: "HVN",
  HEIGHTS: "HTS",
  HIGHWAY: "HWY",
  HILL: "HL",
  HOLLOW: "HOLW",
  INLET: "INLT",
  ISLAND: "IS",
  JUNCTION: "JCT",
  KEY: "KY",
  KNOLL: "KNL",
  LAKE: "LK",
  LANDING: "LNDG",
  LANE: "LN",
  LIGHT: "LGT",
  LOAF: "LF",
  LOCK: "LCK",
  LODGE: "LDG",
  LOOP: "LOOP",
  MANOR: "MNR",
  MEADOW: "MDW",
  MILL: "ML",
  MISSION: "MSN",
  MOTORWAY: "MTWY",
  MOUNT: "MT",
  MOUNTAIN: "MTN",
  ORCHARD: "ORCH",
  OVAL: "OVAL",
  OVERPASS: "OPAS",
  PARK: "PARK",
  PARKWAY: "PKWY",
  PASS: "PASS",
  PASSAGE: "PSGE",
  PATH: "PATH",
  PIKE: "PIKE",
  PLACE: "PL",
  PLAIN: "PLN",
  PLAZA: "PLZ",
  POINT: "PT",
  PORT: "PRT",
  PRAIRIE: "PR",
  RADIAL: "RADL",
  RANCH: "RNCH",
  RAPID: "RPD",
  REST: "RST",
  RIDGE: "RDG",
  RIVER: "RIV",
  ROAD: "RD",
  ROUTE: "RTE",
  ROW: "ROW",
  RUN: "RUN",
  SHOAL: "SHL",
  SHORE: "SHR",
  SKYWAY: "SKWY",
  SPRING: "SPG",
  SPUR: "SPUR",
  SQUARE: "SQ",
  STATION: "STA",
  STRAVENUE: "STRA",
  STREAM: "STRM",
  STREET: "ST",
  SUMMIT: "SMT",
  TERRACE: "TER",
  TRACE: "TRCE",
  TRACK: "TRAK",
  TRAFFICWAY: "TRFY",
  TRAIL: "TRL",
  TUNNEL: "TUNL",
  TURNPIKE: "TPKE",
  UNDERPASS: "UPAS",
  UNION: "UN",
  VALLEY: "VLY",
  VIADUCT: "VIA",
  VIEW: "VW",
  VILLAGE: "VLG",
  VILLE: "VL",
  VISTA: "VIS",
  WALK: "WALK",
  WALL: "WALL",
  WAY: "WAY",
  WELL: "WL",
});

/**
 * @typedef {"coconut-creek" | "lauderhill" | "margate" | "pompano-beach" | "tamarac"} MunicipalSeedJurisdictionKey
 * @typedef {"folio" | "address"} MunicipalSeedQueryKind
 *
 * @typedef {object} MunicipalPropertySeedOptions
 * @property {string} outputPath - Owner-only CSV destination.
 * @property {string} gapOutputPath - Owner-only unqueryable-property ledger.
 *
 * @typedef {object} BrowardPropertyCandidate
 * @property {unknown} request_identifier - Candidate BCPA folio.
 * @property {unknown} unnormalized_address - Candidate BCPA situs string.
 *
 * @typedef {object} MunicipalPropertySeedRow
 * @property {MunicipalSeedJurisdictionKey} jurisdiction_key - Registry jurisdiction.
 * @property {MunicipalSeedQueryKind} query_kind - Certified exact source field.
 * @property {string} query_value - Private normalized folio or base situs.
 * @property {number} property_count - BCPA properties represented by this query.
 *
 * @typedef {object} MunicipalPropertySeedGapRow
 * @property {MunicipalSeedJurisdictionKey} jurisdiction_key - Registry jurisdiction whose source query cannot represent the property.
 * @property {string} property_identifier - Canonical BCPA folio retained only in the private ledger.
 * @property {"unrepresentable_normalized_address"} reason - Stable fail-closed exclusion class.
 *
 * @typedef {object} MunicipalPropertySeedResult
 * @property {MunicipalPropertySeedRow[]} rows - Deterministic unique source queries.
 * @property {MunicipalPropertySeedGapRow[]} gapRows - Deterministic private property exclusions.
 * @property {number} inputCount - Database candidates considered.
 * @property {number} invalidCount - Missing/invalid folio or situs rows.
 * @property {number} unresolvedCount - Rows without exact registry routing.
 * @property {number} otherJurisdictionCount - Valid rows outside target jurisdictions.
 * @property {Record<MunicipalSeedJurisdictionKey,number>} propertyCounts
 * @property {Record<MunicipalSeedJurisdictionKey,number>} queryCounts
 * @property {Record<MunicipalSeedJurisdictionKey,number>} unqueryableCounts
 */

/**
 * Parse the private output path.
 *
 * @param {readonly string[]} argv - CLI arguments after the script path.
 * @returns {MunicipalPropertySeedOptions} Validated builder options.
 */
export function parseMunicipalPropertySeedOptions(argv) {
  if (argv.length === 0) {
    return {
      outputPath: path.resolve(DEFAULT_OUTPUT_PATH),
      gapOutputPath: path.resolve(DEFAULT_GAP_OUTPUT_PATH),
    };
  }
  const allowed = new Set(["--output", "--gap-output"]);
  const values = new Map();
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (
      typeof flag !== "string" ||
      !allowed.has(flag) ||
      typeof value !== "string" ||
      value.trim() === "" ||
      value.startsWith("--") ||
      values.has(flag)
    ) {
      throw new Error(
        "Municipal seed options must be unique --output/--gap-output pairs",
      );
    }
    values.set(flag, value);
  }
  const outputPath = path.resolve(
    values.get("--output") ?? DEFAULT_OUTPUT_PATH,
  );
  const gapOutputPath = path.resolve(
    values.get("--gap-output") ?? deriveGapOutputPath(outputPath),
  );
  if (gapOutputPath === outputPath) {
    throw new Error("Municipal seed and gap ledger paths must differ");
  }
  return { outputPath, gapOutputPath };
}

/**
 * Convert one full BCPA situs string to the base address accepted by legacy
 * municipal split-field searches. City/state/ZIP tails are removed only after
 * the registry has independently resolved that city. USPS full-word suffixes
 * are normalized, then the shared strict parser validates the final shape.
 *
 * @param {string} rawAddress - Full private BCPA situs string.
 * @param {readonly string[]} jurisdictionAliases - Exact registry city aliases.
 * @returns {string} Canonical base situs query without unit or city tail.
 */
export function normalizeMunicipalPropertyAddress(
  rawAddress,
  jurisdictionAliases,
) {
  let normalized = rawAddress
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/gu, "")
    .toUpperCase()
    .replace(/[,.]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim()
    .replace(/\s+(?:FL|FLORIDA)(?:\s+\d{5}(?:-\d{4})?)?$/u, "")
    .replace(/\s+\d{5}(?:-\d{4})?$/u, "")
    .trim();
  const normalizedAliases = jurisdictionAliases
    .map((alias) =>
      alias
        .toUpperCase()
        .replace(/[^A-Z0-9]+/gu, " ")
        .replace(/\s+/gu, " ")
        .trim(),
    )
    .sort((left, right) => right.length - left.length);
  const matchedAlias = normalizedAliases.find(
    (alias) => normalized === alias || normalized.endsWith(` ${alias}`),
  );
  if (matchedAlias === undefined) {
    throw new Error("BCPA situs address lacks its resolved city tail");
  }
  normalized = normalized.slice(0, -matchedAlias.length).trim();
  const base = normalized
    .replace(
      /\s+(?:APT|APARTMENT|BLDG|BUILDING|LOT|STE|SUITE|UNIT|#)\s*[A-Z0-9-]+$/u,
      "",
    )
    .trim();
  const tokens = base.split(" ");
  const suffix = tokens.at(-1);
  if (
    suffix !== undefined &&
    Object.hasOwn(FULL_SUFFIX_TO_ABBREVIATION, suffix)
  ) {
    tokens[tokens.length - 1] =
      FULL_SUFFIX_TO_ABBREVIATION[
        /** @type {keyof typeof FULL_SUFFIX_TO_ABBREVIATION} */ (suffix)
      ];
  }
  const candidate = tokens.join(" ");
  const parsed = parseMunicipalStreetAddress(candidate);
  return [
    parsed.houseNumber,
    parsed.direction,
    parsed.streetName,
    parsed.suffix,
  ]
    .filter((value) => value.length > 0)
    .join(" ");
}

/**
 * Route candidates and reconcile every target property to an exact query or
 * an explicit unqueryable count.
 *
 * @param {readonly BrowardPropertyCandidate[]} candidates - BCPA source rows.
 * @returns {MunicipalPropertySeedResult} Deterministic seed and exclusions.
 */
export function createBrowardMunicipalPropertySeedRows(candidates) {
  /** @type {Map<string, MunicipalPropertySeedRow>} */
  const rowsByQuery = new Map();
  /** @type {MunicipalPropertySeedGapRow[]} */
  const gapRows = [];
  /** @type {Record<MunicipalSeedJurisdictionKey, number>} */
  const propertyCounts = emptyJurisdictionCounts();
  /** @type {Record<MunicipalSeedJurisdictionKey, number>} */
  const unqueryableCounts = emptyJurisdictionCounts();
  let invalidCount = 0;
  let unresolvedCount = 0;
  let otherJurisdictionCount = 0;
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
    if (!isMunicipalSeedJurisdictionKey(jurisdictionKey)) {
      otherJurisdictionCount += 1;
      continue;
    }
    propertyCounts[jurisdictionKey] += 1;
    const queryKind = TARGET_QUERY_KIND[jurisdictionKey];
    let queryValue;
    try {
      queryValue =
        queryKind === "folio"
          ? folio
          : normalizeMunicipalPropertyAddress(
              address,
              resolution.jurisdiction?.aliases ?? [],
            );
    } catch {
      unqueryableCounts[jurisdictionKey] += 1;
      gapRows.push({
        jurisdiction_key: jurisdictionKey,
        property_identifier: folio,
        reason: "unrepresentable_normalized_address",
      });
      continue;
    }
    const identity = `${jurisdictionKey}\u0000${queryKind}\u0000${queryValue.toUpperCase()}`;
    const prior = rowsByQuery.get(identity);
    if (prior === undefined) {
      rowsByQuery.set(identity, {
        jurisdiction_key: jurisdictionKey,
        query_kind: queryKind,
        query_value: queryValue,
        property_count: 1,
      });
    } else {
      prior.property_count += 1;
    }
  }
  const rows = [...rowsByQuery.values()].sort(
    (left, right) =>
      left.jurisdiction_key.localeCompare(right.jurisdiction_key) ||
      left.query_kind.localeCompare(right.query_kind) ||
      left.query_value.localeCompare(right.query_value),
  );
  gapRows.sort(
    (left, right) =>
      left.jurisdiction_key.localeCompare(right.jurisdiction_key) ||
      left.property_identifier.localeCompare(right.property_identifier),
  );
  /** @type {Record<MunicipalSeedJurisdictionKey, number>} */
  const queryCounts = emptyJurisdictionCounts();
  for (const row of rows) queryCounts[row.jurisdiction_key] += 1;
  return {
    rows,
    gapRows,
    inputCount: candidates.length,
    invalidCount,
    unresolvedCount,
    otherJurisdictionCount,
    propertyCounts,
    queryCounts,
    unqueryableCounts,
  };
}

/**
 * Render one RFC 4180-compatible private seed row.
 *
 * @param {MunicipalPropertySeedRow} row - Validated source query.
 * @returns {string} CSV line without trailing newline.
 */
export function renderMunicipalPropertySeedRow(row) {
  return SEED_COLUMNS.map((column) => encodeCsvCell(String(row[column]))).join(
    ",",
  );
}

/**
 * Render one private unqueryable-property receipt without retaining an address.
 *
 * @param {MunicipalPropertySeedGapRow} row - Explicit fail-closed exclusion.
 * @returns {string} CSV line without trailing newline.
 */
export function renderMunicipalPropertySeedGapRow(row) {
  return GAP_COLUMNS.map((column) => encodeCsvCell(String(row[column]))).join(
    ",",
  );
}

/**
 * Build the complete seed after independently verifying the isolated Neon
 * project, branch, and endpoint.
 *
 * @param {MunicipalPropertySeedOptions} options - Output configuration.
 * @param {NodeJS.ProcessEnv} [environment=process.env] - Target secrets and IDs.
 * @returns {Promise<{
 *   rowCount:number,
 *   propertyCounts:Record<MunicipalSeedJurisdictionKey,number>,
 *   queryCounts:Record<MunicipalSeedJurisdictionKey,number>,
 *   unqueryableCounts:Record<MunicipalSeedJurisdictionKey,number>,
 *   unresolvedCount:number,
 *   sha256:string,
 *   gapRowCount:number,
 *   gapSha256:string
 * }>} Aggregate-only seed receipt.
 */
export async function buildBrowardMunicipalPropertySeed(
  options,
  environment = process.env,
) {
  const target = requireTarget(environment);
  const client = new Client({
    connectionString: target.connectionString,
    application_name: "broward-municipal-property-seed",
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
    const built = createBrowardMunicipalPropertySeedRows(
      /** @type {BrowardPropertyCandidate[]} */ (result.rows),
    );
    for (const jurisdictionKey of Object.keys(TARGET_QUERY_KIND)) {
      const key = /** @type {MunicipalSeedJurisdictionKey} */ (jurisdictionKey);
      if (built.propertyCounts[key] === 0 || built.queryCounts[key] === 0) {
        throw new Error(
          "Verified Broward target lacks one municipal seed jurisdiction",
        );
      }
    }
    const content = `${SEED_COLUMNS.join(",")}\n${built.rows
      .map((row) => renderMunicipalPropertySeedRow(row))
      .join("\n")}\n`;
    const gapContent = `${GAP_COLUMNS.join(",")}\n${built.gapRows
      .map((row) => renderMunicipalPropertySeedGapRow(row))
      .join("\n")}\n`;
    await writePrivateAtomic(options.outputPath, content);
    await writePrivateAtomic(options.gapOutputPath, gapContent);
    return {
      rowCount: built.rows.length,
      propertyCounts: built.propertyCounts,
      queryCounts: built.queryCounts,
      unqueryableCounts: built.unqueryableCounts,
      unresolvedCount: built.unresolvedCount,
      sha256: createHash("sha256").update(content).digest("hex"),
      gapRowCount: built.gapRows.length,
      gapSha256: createHash("sha256").update(gapContent).digest("hex"),
    };
  } finally {
    await client.end();
  }
}

/**
 * Derive the default sibling ledger path for a caller-selected private seed.
 *
 * @param {string} outputPath - Absolute or relative private seed path.
 * @returns {string} Sibling private gap-ledger path.
 */
function deriveGapOutputPath(outputPath) {
  return outputPath.endsWith(".private.csv")
    ? `${outputPath.slice(0, -".private.csv".length)}-gaps.private.csv`
    : `${outputPath}-gaps.private.csv`;
}

/**
 * Create a fully keyed zero-count aggregate.
 *
 * @returns {Record<MunicipalSeedJurisdictionKey, number>} New mutable counts.
 */
function emptyJurisdictionCounts() {
  return {
    "coconut-creek": 0,
    lauderhill: 0,
    margate: 0,
    "pompano-beach": 0,
    tamarac: 0,
  };
}

/**
 * Narrow a registry key to this exact property-first source set.
 *
 * @param {string} value - Candidate registry key.
 * @returns {value is MunicipalSeedJurisdictionKey} True for supported targets.
 */
function isMunicipalSeedJurisdictionKey(value) {
  return Object.hasOwn(TARGET_QUERY_KIND, value);
}

/**
 * Escape one private CSV value.
 *
 * @param {string} value - Source cell.
 * @returns {string} RFC 4180-compatible cell.
 */
function encodeCsvCell(value) {
  return /[",\r\n]/u.test(value) ? `"${value.replaceAll('"', '""')}"` : value;
}

/**
 * Verify the exact isolated Neon identity.
 *
 * @param {import("pg").Client} client - Connected client.
 * @param {{expectedBranchId:string,expectedEndpointId:string}} target - Expected IDs.
 * @returns {Promise<void>} Resolves only for the non-production target.
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
    throw new Error(
      "Municipal property seed target is not isolated broward-ingest",
    );
  }
}

/**
 * Require independently configured target identity without logging secrets.
 *
 * @param {NodeJS.ProcessEnv} environment - Runtime environment.
 * @returns {{connectionString:string,expectedBranchId:string,expectedEndpointId:string}}
 *   Validated private connection and public IDs.
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
 * Atomically write one owner-only artifact.
 *
 * @param {string} filePath - Final private path.
 * @param {string} content - Complete UTF-8 content.
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

if (
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href
) {
  buildBrowardMunicipalPropertySeed(
    parseMunicipalPropertySeedOptions(process.argv.slice(2)),
  )
    .then((receipt) => {
      console.log(
        JSON.stringify({
          event: "broward_municipal_property_seed_built",
          ...receipt,
        }),
      );
    })
    .catch((error) => {
      console.error(
        JSON.stringify({
          event: "broward_municipal_property_seed_failed",
          message: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      process.exitCode = 1;
    });
}
