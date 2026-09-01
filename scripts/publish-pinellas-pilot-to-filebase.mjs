#!/usr/bin/env node

import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createRequire } from "node:module";

import {
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";

import { parseCsvRecords } from "./run-pinellas-local-ingest.mjs";

const require = createRequire(import.meta.url);
const ipfsHash = require("ipfs-only-hash");
const AdmZip = require("adm-zip");

const COUNTY = "pinellas";
const COUNTY_NAME = "Pinellas";
const STATE_CODE = "FL";
const SOURCE_SYSTEM = "pinellas_appraiser";
const DEFAULT_SEED_PATH = "data/seeds/pinellas-pilot.csv";
const DEFAULT_INGEST_DIRECTORY = "downloads/pinellas/local-ingest";
const DEFAULT_OUT_DIRECTORY = "downloads/pinellas/filebase-publish";
const DEFAULT_QUERY_DB_DIR = "../elephant-query-db";
const QUERY_TABLE_BUCKET = "elephant-oracle-query-table";
const FILEBASE_S3_ENDPOINT = "https://s3.filebase.com";
const FILEBASE_NAMES_API = "https://api.filebase.io/v1/names";
const FILEBASE_GATEWAY = "https://ipfs.filebase.io";
const QUERY_TABLE_IPNS_LABEL = "oracle-query-table-pinellas";
const COVERAGE_IPNS_LABEL = "oracle-dataset-coverage-pinellas";
const TRAILING_STATE_ZIP_RE = /\b[A-Za-z]{2}\s+(\d{5})(?:-\d{4})?\s*$/;
const TRAILING_ZIP_RE = /\b(\d{5})(?:-\d{4})?\s*$/;

/**
 * @typedef {Record<string, string>} SeedRow
 *
 * @typedef {Record<string, unknown>} JsonObject
 *
 * @typedef {object} ParsedAddress
 * @property {string | null} street
 * @property {string | null} city
 * @property {string | null} postalCode
 *
 * @typedef {object} QueryTableRow
 * @property {string} property_id
 * @property {string | null} property_cid
 * @property {string | null} request_identifier
 * @property {string | null} parcel_identifier
 * @property {string | null} source_system
 * @property {string | null} county_name
 * @property {string | null} state_code
 * @property {string | null} address_street
 * @property {string | null} address_city
 * @property {string | null} address_zip
 * @property {number | null} latitude
 * @property {number | null} longitude
 * @property {number | null} lot_size_acre
 * @property {number | null} lot_area_sqft
 * @property {string | null} exterior_wall_material
 * @property {string | null} roof_covering_material
 * @property {string | null} property_type
 * @property {string | null} property_usage_type
 * @property {number | null} built_year
 * @property {number | null} livable_floor_area
 * @property {number | null} total_area
 * @property {number | null} assessed_value
 * @property {number | null} market_value
 * @property {number | null} land_value
 * @property {number | null} avm_value
 * @property {string | null} owner_name
 * @property {string | null} owners_text
 * @property {number | null} owner_count
 * @property {boolean | null} owner_occupied
 * @property {string | null} last_sale_date
 * @property {number | null} last_sale_price
 * @property {string | null} subdivision
 * @property {boolean | null} has_permits
 * @property {number | null} permit_count
 * @property {boolean | null} has_sunbiz_tenant
 * @property {boolean | null} has_bbb_contractor
 * @property {boolean | null} hoa_flag
 *
 * @typedef {object} CoverageSnapshot
 * @property {string} county
 * @property {string} exportedAt
 * @property {readonly CoverageDataset[]} datasets
 *
 * @typedef {object} CoverageDataset
 * @property {string} county
 * @property {string} source
 * @property {number} ingested_count
 * @property {number | null} expected_count
 * @property {string | null} first_loaded_at
 * @property {string | null} last_loaded_at
 * @property {string | null} cid
 * @property {string | null} ipns_label
 *
 * @typedef {object} PublishCliOptions
 * @property {string} seedPath
 * @property {string} ingestDirectory
 * @property {string} outDirectory
 * @property {string} queryDbDir
 * @property {string} envFile
 * @property {boolean} publish
 * @property {boolean} dryRun
 * @property {boolean} allowMissing - When true, skip seed STRAPs without transformed.zip.
 */

/**
 * Parse a US free-text address into street / city / ZIP.
 *
 * @param {string | null | undefined} value - Single-line address.
 * @returns {ParsedAddress} Split address fields.
 */
export function parseUnnormalizedAddress(value) {
  /** @type {ParsedAddress} */
  const empty = { street: null, city: null, postalCode: null };
  if (value === null || value === undefined) return empty;
  const trimmed = value.trim();
  if (trimmed.length === 0) return empty;
  const segments = trimmed
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
  if (segments.length === 0) return empty;
  let postalCode = null;
  const last = segments[segments.length - 1] ?? "";
  const stateZip = TRAILING_STATE_ZIP_RE.exec(last);
  const zipOnly = TRAILING_ZIP_RE.exec(last);
  if (stateZip?.[1] !== undefined) {
    postalCode = stateZip[1];
    const head = last.replace(TRAILING_STATE_ZIP_RE, "").trim();
    if (head.length > 0) segments[segments.length - 1] = head;
    else segments.pop();
  } else if (zipOnly?.[1] !== undefined) {
    postalCode = zipOnly[1];
    const head = last.replace(TRAILING_ZIP_RE, "").trim();
    if (head.length > 0) segments[segments.length - 1] = head;
    else segments.pop();
  }
  const street = segments.length > 0 ? (segments[0] ?? null) : null;
  const cityParts = segments.slice(1);
  const city = cityParts.length > 0 ? cityParts.join(", ") : null;
  return { street, city, postalCode };
}

/**
 * Coerce a JSON scalar to a finite number.
 *
 * @param {unknown} value - Raw JSON value.
 * @returns {number | null} Finite number, or null.
 */
export function toNumber(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (trimmed.length === 0) return null;
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Coerce a JSON scalar to a non-empty trimmed string.
 *
 * @param {unknown} value - Raw JSON value.
 * @returns {string | null} Trimmed string, or null.
 */
export function toText(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/**
 * Stable UTF-8 property id from a Pinellas STRAP.
 *
 * @param {string} strap - 18-digit STRAP.
 * @returns {string} Deterministic UUID-shaped id.
 */
export function propertyIdForStrap(strap) {
  const digest = createHash("sha1").update(`pinellas_appraiser:${strap}`).digest();
  const bytes = Buffer.from(digest.subarray(0, 16));
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/**
 * Read an owner display name from a person or company JSON object.
 *
 * @param {JsonObject} record - Transform owner file.
 * @returns {string | null} Display name.
 */
export function ownerNameFromRecord(record) {
  const direct =
    toText(record.name) ?? toText(record.full_name) ?? toText(record.company_name);
  if (direct !== null) return direct;
  const parts = [record.first_name, record.middle_name, record.last_name]
    .map((part) => toText(part))
    .filter((part) => part !== null);
  return parts.length > 0 ? parts.join(" ") : null;
}

/**
 * Map one transformed-zip JSON dictionary plus optional seed row into a query-table row.
 *
 * @param {object} params - Mapping inputs.
 * @param {string} params.strap - Canonical 18-digit STRAP.
 * @param {Record<string, JsonObject>} params.files - `data/*.json` keyed by basename.
 * @param {SeedRow | null} params.seedRow - Matching seed row, when present.
 * @returns {QueryTableRow} Flat query-table row.
 */
export function mapTransformedFilesToQueryTableRow({ strap, files, seedRow }) {
  const property = files["property.json"] ?? {};
  const address = files["address.json"] ?? {};
  const unnormalized = files["unnormalized_address.json"] ?? {};
  const lot = files["lot.json"] ?? {};
  const geometry = files["geometry.json"] ?? {};
  const structure = files["structure.json"] ?? {};

  const situsText =
    toText(unnormalized.full_address) ??
    toText(address.unnormalized_address) ??
    toText(seedRow?.situs_address) ??
    toText(seedRow?.address);
  const parsed = parseUnnormalizedAddress(situsText);
  const situsHasContent =
    parsed.city !== null ||
    parsed.postalCode !== null ||
    (parsed.street !== null && /\d/.test(parsed.street));

  const taxRows = Object.entries(files)
    .filter(([name]) => /^tax_\d+\.json$/.test(name))
    .map(([, record]) => record)
    .sort((left, right) => (toNumber(right.tax_year) ?? 0) - (toNumber(left.tax_year) ?? 0));
  const latestTax = taxRows[0] ?? {};

  const salesRows = Object.entries(files)
    .filter(([name]) => /^sales_history_\d+\.json$/.test(name))
    .map(([, record]) => record)
    .sort((left, right) =>
      String(toText(right.ownership_transfer_date) ?? "").localeCompare(
        String(toText(left.ownership_transfer_date) ?? ""),
      ),
    );
  const latestSale = salesRows[0] ?? {};

  const owners = Object.entries(files)
    .filter(([name]) => /^(person|company)_\d+\.json$/.test(name))
    .map(([, record]) => ownerNameFromRecord(record))
    .filter((name) => name !== null);
  const uniqueOwners = [...new Set(owners)];

  const permitCount = Object.keys(files).filter((name) =>
    /^property_improvement_\d+\.json$/.test(name),
  ).length;

  const lotAreaSqft = toNumber(lot.lot_area_sqft);
  const lotSizeAcre =
    toNumber(lot.lot_size_acre) ??
    toNumber(seedRow?.acres) ??
    (lotAreaSqft !== null ? lotAreaSqft / 43_560 : null);

  return {
    property_id: propertyIdForStrap(strap),
    property_cid: null,
    request_identifier: strap,
    parcel_identifier:
      toText(property.parcel_identifier) ??
      toText(files["parcel.json"]?.parcel_identifier) ??
      strap,
    source_system: SOURCE_SYSTEM,
    county_name: COUNTY_NAME,
    state_code: STATE_CODE,
    address_street: (situsHasContent ? parsed.street : null) ?? parsed.street,
    address_city:
      (situsHasContent ? parsed.city : null) ??
      toText(seedRow?.city) ??
      parsed.city,
    address_zip:
      (situsHasContent ? parsed.postalCode : null) ??
      toText(seedRow?.zip) ??
      parsed.postalCode,
    latitude: toNumber(geometry.latitude) ?? toNumber(seedRow?.latitude),
    longitude: toNumber(geometry.longitude) ?? toNumber(seedRow?.longitude),
    lot_size_acre: lotSizeAcre,
    lot_area_sqft: lotAreaSqft,
    exterior_wall_material:
      toText(structure.exterior_wall_material_primary) ??
      toText(structure.exterior_wall_material),
    roof_covering_material: toText(structure.roof_covering_material),
    property_type: toText(property.property_type),
    property_usage_type: toText(property.property_usage_type),
    built_year: toInteger(property.property_structure_built_year),
    livable_floor_area: toNumber(property.livable_floor_area),
    total_area: toNumber(property.total_area),
    assessed_value: toNumber(latestTax.property_assessed_value_amount),
    market_value: toNumber(latestTax.property_market_value_amount),
    land_value: toNumber(latestTax.property_land_amount),
    avm_value: null,
    owner_name: uniqueOwners[0] ?? null,
    owners_text: uniqueOwners.length > 0 ? uniqueOwners.join(" | ") : null,
    owner_count: uniqueOwners.length > 0 ? uniqueOwners.length : null,
    owner_occupied: null,
    last_sale_date: toText(latestSale.ownership_transfer_date),
    last_sale_price: toNumber(latestSale.purchase_price_amount),
    subdivision: toText(property.subdivision),
    has_permits: permitCount > 0,
    permit_count: permitCount,
    has_sunbiz_tenant: false,
    has_bbb_contractor: false,
    hoa_flag: null,
  };
}

/**
 * Truncate a numeric value to an integer.
 *
 * @param {unknown} value - Raw JSON value.
 * @returns {number | null} Truncated integer, or null.
 */
function toInteger(value) {
  const parsed = toNumber(value);
  return parsed === null ? null : Math.trunc(parsed);
}

/**
 * Query-table parquet schema used by MCP DuckDB.
 *
 * @returns {ParquetSchema} Scalar-only schema.
 */
export function buildQueryTableParquetSchema() {
  return new ParquetSchema({
    property_id: { type: "UTF8" },
    property_cid: { type: "UTF8", optional: true },
    request_identifier: { type: "UTF8", optional: true },
    parcel_identifier: { type: "UTF8", optional: true },
    source_system: { type: "UTF8", optional: true },
    county_name: { type: "UTF8", optional: true },
    state_code: { type: "UTF8", optional: true },
    address_street: { type: "UTF8", optional: true },
    address_city: { type: "UTF8", optional: true },
    address_zip: { type: "UTF8", optional: true },
    latitude: { type: "DOUBLE", optional: true },
    longitude: { type: "DOUBLE", optional: true },
    lot_size_acre: { type: "DOUBLE", optional: true },
    lot_area_sqft: { type: "DOUBLE", optional: true },
    exterior_wall_material: { type: "UTF8", optional: true },
    roof_covering_material: { type: "UTF8", optional: true },
    property_type: { type: "UTF8", optional: true },
    property_usage_type: { type: "UTF8", optional: true },
    built_year: { type: "INT64", optional: true },
    livable_floor_area: { type: "DOUBLE", optional: true },
    total_area: { type: "DOUBLE", optional: true },
    assessed_value: { type: "DOUBLE", optional: true },
    market_value: { type: "DOUBLE", optional: true },
    land_value: { type: "DOUBLE", optional: true },
    avm_value: { type: "DOUBLE", optional: true },
    owner_name: { type: "UTF8", optional: true },
    owners_text: { type: "UTF8", optional: true },
    owner_count: { type: "INT64", optional: true },
    owner_occupied: { type: "BOOLEAN", optional: true },
    last_sale_date: { type: "UTF8", optional: true },
    last_sale_price: { type: "DOUBLE", optional: true },
    subdivision: { type: "UTF8", optional: true },
    has_permits: { type: "BOOLEAN", optional: true },
    permit_count: { type: "INT64", optional: true },
    has_sunbiz_tenant: { type: "BOOLEAN", optional: true },
    has_bbb_contractor: { type: "BOOLEAN", optional: true },
    hoa_flag: { type: "BOOLEAN", optional: true },
  });
}

/**
 * Drop null keys so parquetjs optional fields write as NULL.
 *
 * @param {QueryTableRow} row - Query-table row.
 * @returns {Record<string, unknown>} Sparse parquet record.
 */
export function toParquetRecord(row) {
  /** @type {Record<string, unknown>} */
  const record = {};
  for (const [key, value] of Object.entries(row)) {
    if (value !== null && value !== undefined) record[key] = value;
  }
  return record;
}

/**
 * Build the MCP coverage snapshot for this 50-parcel appraisal pilot.
 *
 * @param {object} params - Snapshot inputs.
 * @param {number} params.ingestedCount - Distinct STRAPs written to parquet.
 * @param {number} params.expectedCount - Seed row count.
 * @param {string} params.exportedAt - ISO timestamp.
 * @returns {CoverageSnapshot} Coverage JSON.
 */
export function buildPinellasPilotCoverage({ ingestedCount, expectedCount, exportedAt }) {
  return {
    county: COUNTY,
    exportedAt,
    datasets: [
      {
        county: COUNTY,
        source: "appraisal",
        ingested_count: ingestedCount,
        expected_count: expectedCount,
        first_loaded_at: exportedAt,
        last_loaded_at: exportedAt,
        cid: null,
        ipns_label: `oracle-dataset-coverage-${COUNTY}`,
      },
    ],
  };
}

/**
 * Parse CLI flags for the local-zip → Filebase publisher.
 *
 * @param {readonly string[]} argv - Arguments after the script path.
 * @returns {PublishCliOptions} Normalized options.
 */
export function parseCliOptions(argv) {
  /** @type {Map<string, string>} */
  const values = new Map();
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === undefined || token.startsWith("--") === false) continue;
    const key = token.slice(2);
    const next = argv[index + 1];
    if (next !== undefined && next.startsWith("--") === false) {
      values.set(key, next);
      index += 1;
    } else {
      values.set(key, "true");
    }
  }
  return {
    seedPath: values.get("seed") ?? DEFAULT_SEED_PATH,
    ingestDirectory: values.get("ingest-dir") ?? DEFAULT_INGEST_DIRECTORY,
    outDirectory: values.get("out-dir") ?? DEFAULT_OUT_DIRECTORY,
    queryDbDir: values.get("query-db-dir") ?? DEFAULT_QUERY_DB_DIR,
    envFile: values.get("env-file") ?? ".env.local",
    publish: values.get("no-publish") !== "true",
    dryRun: values.get("dry-run") === "true",
    allowMissing: values.get("allow-missing") === "true",
  };
}

/**
 * Load dotenv KEY=value pairs into process.env without overwriting existing keys.
 *
 * @param {string} envFile - Path to a dotenv file.
 * @returns {Promise<void>} Resolves after load.
 */
export async function loadEnvFile(envFile) {
  try {
    const text = await readFile(envFile, "utf8");
    for (const line of text.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (trimmed.length === 0 || trimmed.startsWith("#")) continue;
      const equalsIndex = trimmed.indexOf("=");
      if (equalsIndex <= 0) continue;
      const key = trimmed.slice(0, equalsIndex);
      let value = trimmed.slice(equalsIndex + 1);
      if (
        value.length >= 2 &&
        ((value.startsWith('"') && value.endsWith('"')) ||
          (value.startsWith("'") && value.endsWith("'")))
      ) {
        value = value.slice(1, -1);
      }
      if (process.env[key] === undefined) process.env[key] = value;
    }
  } catch (caught) {
    if (caught instanceof Error && "code" in caught && caught.code === "ENOENT") return;
    throw caught;
  }
}

/**
 * Whether Filebase S3 + IPNS credentials are present (values not logged).
 *
 * @param {NodeJS.ProcessEnv} env - Environment map.
 * @returns {boolean} True when the required names are non-empty.
 */
export function hasFilebaseCredentials(env) {
  const names = [
    "S3_ACCESS_KEY_ID",
    "S3_SECRET_ACCESS_KEY",
    "FILEBASE_API_TOKEN",
  ];
  return names.every((name) => {
    const value = env[name];
    return typeof value === "string" && value.trim().length > 0;
  });
}

/**
 * Derive Filebase API token from S3 access/secret when the token is missing.
 *
 * @param {NodeJS.ProcessEnv} env - Mutable environment map.
 * @returns {void}
 */
export function fillDerivedFilebaseToken(env) {
  if (typeof env.FILEBASE_API_TOKEN === "string" && env.FILEBASE_API_TOKEN.trim().length > 0) {
    return;
  }
  const access = env.S3_ACCESS_KEY_ID?.trim();
  const secret = env.S3_SECRET_ACCESS_KEY?.trim();
  if (access === undefined || access.length === 0 || secret === undefined || secret.length === 0) {
    return;
  }
  env.FILEBASE_API_TOKEN = Buffer.from(`${access}:${secret}`, "utf8").toString("base64");
}

/**
 * Read every `data/*.json` object from a transformed.zip (no relationship files).
 *
 * @param {string} zipPath - Path to transformed.zip.
 * @returns {Promise<Record<string, JsonObject>>} Basename → parsed JSON.
 */
export async function readTransformedZipJsonFiles(zipPath) {
  const zip = new AdmZip(zipPath);
  /** @type {Record<string, JsonObject>} */
  const files = {};
  for (const entry of zip.getEntries()) {
    const name = entry.entryName.replaceAll("\\", "/");
    const base = name.split("/").pop() ?? name;
    if (!name.startsWith("data/") || !name.endsWith(".json")) continue;
    if (base.startsWith("relationship_") || base.startsWith("bafk")) continue;
    const parsed = JSON.parse(entry.getData().toString("utf8"));
    if (parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)) {
      files[base] = parsed;
    }
  }
  return files;
}

/**
 * Write query-table parquet and coverage JSON from local Pinellas transformed zips.
 *
 * @param {PublishCliOptions} options - Paths and flags.
 * @param {string} repoRoot - oracle-node root.
 * @returns {Promise<{ parquetPath: string, coveragePath: string, rowCount: number, expectedCount: number }>}
 *   Written artifact paths and counts.
 */
export async function writePinellasPilotPublishArtifacts(options, repoRoot) {
  const seedPath = path.resolve(repoRoot, options.seedPath);
  const ingestDirectory = path.resolve(repoRoot, options.ingestDirectory);
  const outDirectory = path.resolve(repoRoot, options.outDirectory);
  const seedRows = parseCsvRecords(await readFile(seedPath, "utf8"));
  const seedByStrap = new Map(seedRows.map((row) => [row.parcel_id, row]));
  const expectedCount = seedByStrap.size;
  if (expectedCount === 0) {
    throw new Error(`Pinellas seed is empty: ${seedPath}`);
  }
  await mkdir(outDirectory, { recursive: true });

  /** @type {QueryTableRow[]} */
  const rows = [];
  const missing = [];
  const straps = [...seedByStrap.keys()].sort();
  let scanned = 0;
  const startedAt = Date.now();
  for (const strap of straps) {
    scanned += 1;
    const zipPath = path.join(ingestDirectory, strap, "transformed.zip");
    try {
      const files = await readTransformedZipJsonFiles(zipPath);
      if (files["property.json"] === undefined) {
        missing.push(strap);
        continue;
      }
      rows.push(
        mapTransformedFilesToQueryTableRow({
          strap,
          files,
          seedRow: seedByStrap.get(strap) ?? null,
        }),
      );
    } catch {
      missing.push(strap);
    }
    if (scanned === 1 || scanned % 5000 === 0 || scanned === straps.length) {
      const elapsedMs = Date.now() - startedAt;
      const rate = scanned / Math.max(elapsedMs / 1000, 0.001);
      console.log(
        JSON.stringify({
          event: "pinellas_publish_scan_progress",
          scanned,
          total: straps.length,
          rows: rows.length,
          missing: missing.length,
          ratePerSec: Number(rate.toFixed(1)),
        }),
      );
    }
  }
  if (missing.length > 0 && options.allowMissing !== true) {
    throw new Error(
      `Missing transformed.zip/property.json for ${missing.length} seed STRAPs: ${missing.slice(0, 8).join(", ")}`,
    );
  }
  if (missing.length > 0) {
    await writeFile(
      path.join(outDirectory, "missing-straps.json"),
      `${JSON.stringify({ count: missing.length, straps: missing }, null, 2)}\n`,
      "utf8",
    );
  }

  const identifiers = rows.map((row) => row.request_identifier);
  if (new Set(identifiers).size !== identifiers.length) {
    throw new Error("Query table would contain duplicate request_identifier values");
  }

  await mkdir(outDirectory, { recursive: true });
  const parquetPath = path.join(outDirectory, "query-table.parquet");
  const coveragePath = path.join(outDirectory, "dataset-coverage.json");
  const writer = await ParquetWriter.openFile(buildQueryTableParquetSchema(), parquetPath);
  try {
    for (const row of rows) {
      await writer.appendRow(toParquetRecord(row));
    }
  } finally {
    await writer.close();
  }

  const exportedAt = new Date().toISOString();
  const coverage = buildPinellasPilotCoverage({
    ingestedCount: rows.length,
    expectedCount,
    exportedAt,
  });
  await writeFile(coveragePath, `${JSON.stringify(coverage, null, 2)}\n`, "utf8");
  await writeFile(
    path.join(outDirectory, "manifest.json"),
    `${JSON.stringify(
      {
        county: COUNTY,
        rowCount: rows.length,
        expectedCount,
        parquetPath,
        coveragePath,
        exportedAt,
      },
      null,
      2,
    )}\n`,
    "utf8",
  );

  return { parquetPath, coveragePath, rowCount: rows.length, expectedCount };
}

/**
 * Upload parquet + coverage to Filebase and re-point the existing Pinellas IPNS names.
 *
 * Does not create new IPNS labels. Catalog already lists Pinellas.
 *
 * @param {object} params - Upload parameters.
 * @param {PublishCliOptions} params.options - CLI options.
 * @param {string} params.repoRoot - oracle-node root.
 * @param {string} params.parquetPath - Absolute parquet path.
 * @param {string} params.coveragePath - Absolute coverage JSON path.
 * @returns {Promise<{ queryTableCid: string, coverageCid: string, queryTableIpns: string, coverageIpns: string } | { dryRun: true }>}
 *   Published CIDs, or a dry-run marker.
 */
export async function publishPinellasArtifactsToFilebase({
  options,
  repoRoot,
  parquetPath,
  coveragePath,
}) {
  const envFile = path.resolve(repoRoot, options.envFile);
  await loadEnvFile(envFile);
  await loadEnvFile(
    path.resolve(repoRoot, options.queryDbDir, ".env.local"),
  );
  fillDerivedFilebaseToken(process.env);
  if (process.env.S3_ENDPOINT === "https://s3.filebase.io") {
    process.env.S3_ENDPOINT = FILEBASE_S3_ENDPOINT;
  }
  process.env.S3_ENDPOINT ??= FILEBASE_S3_ENDPOINT;
  process.env.S3_BUCKET ??= QUERY_TABLE_BUCKET;
  process.env.FILEBASE_QUERY_TABLE_IPNS_LABEL ??= QUERY_TABLE_IPNS_LABEL;
  if (
    (process.env.FILEBASE_COVERAGE_IPNS_LABEL === undefined ||
      process.env.FILEBASE_COVERAGE_IPNS_LABEL.trim().length === 0) &&
    typeof process.env.FILEBASE_DATASET_COVERAGE_IPNS_LABEL === "string" &&
    process.env.FILEBASE_DATASET_COVERAGE_IPNS_LABEL.trim().length > 0
  ) {
    process.env.FILEBASE_COVERAGE_IPNS_LABEL =
      process.env.FILEBASE_DATASET_COVERAGE_IPNS_LABEL;
  }
  process.env.FILEBASE_COVERAGE_IPNS_LABEL ??= COVERAGE_IPNS_LABEL;

  if (options.dryRun === true) {
    console.log(
      JSON.stringify({
        event: "pinellas_filebase_publish_dry_run",
        parquetPath,
        coveragePath,
        queryTableLabel: process.env.FILEBASE_QUERY_TABLE_IPNS_LABEL,
        coverageLabel: process.env.FILEBASE_COVERAGE_IPNS_LABEL,
      }),
    );
    return { dryRun: true };
  }

  if (!hasFilebaseCredentials(process.env)) {
    throw new Error(
      `Filebase credentials are missing. Set S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, and FILEBASE_API_TOKEN (or access+secret so the token can be derived) in ${envFile}.`,
    );
  }

  const accessKeyId = process.env.S3_ACCESS_KEY_ID?.trim() ?? "";
  const secretAccessKey = process.env.S3_SECRET_ACCESS_KEY?.trim() ?? "";
  const token = process.env.FILEBASE_API_TOKEN?.trim() ?? "";
  const bucket = process.env.S3_BUCKET;
  const endpoint = process.env.S3_ENDPOINT;
  const queryLabel = process.env.FILEBASE_QUERY_TABLE_IPNS_LABEL;
  const coverageLabel = process.env.FILEBASE_COVERAGE_IPNS_LABEL;
  const client = new S3Client({
    region: "us-east-1",
    endpoint,
    credentials: { accessKeyId, secretAccessKey },
    forcePathStyle: true,
  });
  const parquetBody = await readFile(parquetPath);
  const coverageBody = await readFile(coveragePath);
  const queryTableCid = await uploadFilebaseObject({
    client,
    bucket,
    key: `${COUNTY}/query-table.parquet`,
    body: parquetBody,
    contentType: "application/vnd.apache.parquet",
  });
  const coverageCid = await uploadFilebaseObject({
    client,
    bucket,
    key: `${COUNTY}/dataset-coverage.json`,
    body: coverageBody,
    contentType: "application/json",
  });
  const queryName = await upsertFilebaseName(token, queryLabel, queryTableCid);
  const coverageName = await upsertFilebaseName(token, coverageLabel, coverageCid);
  const result = {
    queryTableCid,
    coverageCid,
    queryTableIpns: `${FILEBASE_GATEWAY}/ipns/${queryName.network_key}`,
    coverageIpns: `${FILEBASE_GATEWAY}/ipns/${coverageName.network_key}`,
  };
  console.log(JSON.stringify({ event: "pinellas_filebase_published", ...result }));
  return result;
}

/**
 * @typedef {object} FilebaseName
 * @property {string} label - IPNS label.
 * @property {string} network_key - Resolvable IPNS name.
 * @property {string} cid - Current CID.
 */

/**
 * Upload one object to Filebase and return the CID header.
 *
 * @param {object} params - Upload parameters.
 * @param {S3Client} params.client - Filebase S3 client.
 * @param {string} params.bucket - Bucket name.
 * @param {string} params.key - Object key.
 * @param {Buffer} params.body - Bytes.
 * @param {string} params.contentType - HTTP content type.
 * @returns {Promise<string>} Filebase CID.
 */
async function uploadFilebaseObject({ client, bucket, key, body, contentType }) {
  const localCid = await ipfsHash.of(body);
  const command = new PutObjectCommand({
    Bucket: bucket,
    Key: key,
    Body: body,
    ContentType: contentType,
  });
  /** @type {string | undefined} */
  let headerCid;
  command.middlewareStack.add(
    (next) => async (args) => {
      const result = await next(args);
      const response = result.response;
      if (
        typeof response === "object" &&
        response !== null &&
        "headers" in response &&
        typeof response.headers === "object" &&
        response.headers !== null
      ) {
        const headers = /** @type {Record<string, string>} */ (response.headers);
        headerCid = headers["x-amz-meta-cid"];
      }
      return result;
    },
    { step: "deserialize", name: `captureFilebaseCid-${key}`, priority: "low" },
  );
  await client.send(command);
  const cid = headerCid?.trim() || localCid;
  if (typeof cid !== "string" || cid.length === 0) {
    throw new Error(`Filebase returned no CID for ${key}`);
  }
  return cid;
}

/**
 * Create or update a Filebase IPNS label to point at a CID.
 *
 * @param {string} token - Platform API bearer token.
 * @param {string} label - Existing Pinellas IPNS label.
 * @param {string} cid - Target CID.
 * @returns {Promise<FilebaseName>} Updated name record.
 */
async function upsertFilebaseName(token, label, cid) {
  const listResponse = await fetch(FILEBASE_NAMES_API, {
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
  });
  if (!listResponse.ok) {
    throw new Error(`Filebase name list failed: ${listResponse.status}`);
  }
  const parsed = await listResponse.json();
  if (!Array.isArray(parsed)) throw new Error("Filebase name list is not an array");
  const existing = parsed.find(
    (entry) =>
      typeof entry === "object" &&
      entry !== null &&
      "label" in entry &&
      entry.label === label,
  );
  const response =
    existing === undefined
      ? await fetch(FILEBASE_NAMES_API, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ label, cid, enabled: true }),
        })
      : await fetch(`${FILEBASE_NAMES_API}/${encodeURIComponent(label)}`, {
          method: "PUT",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ cid }),
        });
  if (!response.ok) {
    throw new Error(`Filebase IPNS upsert failed for ${label}: ${response.status}`);
  }
  return /** @type {FilebaseName} */ (await response.json());
}

/**
 * @returns {Promise<void>}
 */
async function main() {
  const repoRoot = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
  const options = parseCliOptions(process.argv.slice(2));
  const artifacts = await writePinellasPilotPublishArtifacts(options, repoRoot);
  console.log(
    JSON.stringify({
      event: "pinellas_pilot_artifacts_written",
      county: COUNTY,
      rowCount: artifacts.rowCount,
      expectedCount: artifacts.expectedCount,
      parquetPath: artifacts.parquetPath,
      coveragePath: artifacts.coveragePath,
    }),
  );

  if (options.publish !== true) {
    console.log(
      JSON.stringify({
        event: "pinellas_filebase_publish_skipped",
        reason: "--no-publish",
      }),
    );
    return;
  }

  await publishPinellasArtifactsToFilebase({
    options,
    repoRoot,
    parquetPath: artifacts.parquetPath,
    coveragePath: artifacts.coveragePath,
  });
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
    const message = error instanceof Error ? error.message : String(error);
    console.error(JSON.stringify({ event: "pinellas_filebase_publish_failed", error: message }));
    process.exit(1);
  });
}
