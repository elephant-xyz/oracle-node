#!/usr/bin/env node

/**
 * Export a flat, scalar-only places parquet. Prefer `--from-neon` so the
 * published set is the current `business_locations` rows, not the extract JSONL.
 * `taxonomy.hierarchy` serialises as a `/`-delimited string.
 *
 *   node scripts/export-overture-places-table.mjs \
 *     --from-neon \
 *     --env-file ../elephant-query-db/.env.local \
 *     --county lee --release 2026-07-22.0 \
 *     --out downloads/overture-places/lee/2026-07-22.0/publish
 *
 * Layout: `<out>/NOTICE.txt` (artifact root) and
 * `<out>/<county>/{places-table.parquet,index.json}` (siblings).
 *
 * Do not upload to Filebase/IPFS from this script.
 */

import { createReadStream, readFileSync } from "node:fs";
import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";
import pg from "pg";

import {
  assertApprovedPlaceDatasets,
  coerceFiniteNumber,
  collectDatasetsFromSources,
  isValidTaxonomyHierarchyScalar,
  renderPlacesNotice,
  taxonomyHierarchyToPath,
  validatePlacesTable,
} from "./overture-places-lib.mjs";

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * @typedef {object} PlacesPublicationPaths
 * @property {string} publicationRoot Artifact root that holds NOTICE.txt.
 * @property {string} parquetPath `<county>/places-table.parquet`.
 * @property {string} indexPath Sibling `index.json` next to the parquet.
 * @property {string} noticePath `NOTICE.txt` at the artifact root.
 */

/**
 * @typedef {object} ExportCliOptions
 * @property {boolean} fromNeon Query current Neon `business_locations` instead of JSONL.
 * @property {string} envFile Path to `.env.local` (Neon mode).
 * @property {string} inputDir Extract root containing `places/` JSONL (JSONL mode).
 * @property {string} outRaw Raw `--out` value (directory or `.parquet` path).
 * @property {string} county County slug.
 * @property {string} release Overture release id (Neon filter).
 * @property {boolean} writeNotice When true, write NOTICE.txt and index.json.
 */

/**
 * @typedef {object} PlacesParquetInspection
 * @property {number} rowCount Rows in the parquet.
 * @property {string[]} gersIds GERS ids in encounter order.
 * @property {number} nullGeometryCount Rows missing finite lon/lat.
 * @property {number} invalidHierarchyCount Rows whose hierarchy is not a `/`-delimited scalar.
 * @property {number} hierarchyPresentCount Rows with a non-empty hierarchy scalar.
 */

/**
 * Flat parquet schema for the published places table. Every column is a scalar.
 *
 * @returns {ParquetSchema} parquetjs schema.
 */
export function buildPlacesTableParquetSchema() {
  return new ParquetSchema({
    gers_id: { type: "UTF8" },
    county_key: { type: "UTF8", optional: true },
    county_fips: { type: "UTF8", optional: true },
    name_primary: { type: "UTF8", optional: true },
    taxonomy_primary: { type: "UTF8", optional: true },
    taxonomy_hierarchy: { type: "UTF8", optional: true },
    basic_category: { type: "UTF8", optional: true },
    legacy_category_primary: { type: "UTF8", optional: true },
    operating_status: { type: "UTF8", optional: true },
    confidence: { type: "DOUBLE", optional: true },
    longitude: { type: "DOUBLE", optional: true },
    latitude: { type: "DOUBLE", optional: true },
    address_freeform: { type: "UTF8", optional: true },
    address_locality: { type: "UTF8", optional: true },
    address_postcode: { type: "UTF8", optional: true },
    address_region: { type: "UTF8", optional: true },
    address_country: { type: "UTF8", optional: true },
    brand_name: { type: "UTF8", optional: true },
    brand_wikidata: { type: "UTF8", optional: true },
    is_hosted_service: { type: "BOOLEAN", optional: true },
    hosted_service_rule: { type: "UTF8", optional: true },
    overture_release: { type: "UTF8", optional: true },
    websites: { type: "UTF8", optional: true },
    phones: { type: "UTF8", optional: true },
    emails: { type: "UTF8", optional: true },
  });
}

/**
 * Resolve the publication directory layout.
 *
 * `--out` as a directory is the artifact root (`NOTICE.txt` lives there).
 * `--out` ending in `.parquet` keeps the file path and puts NOTICE at the
 * artifact root: parent of `<county>/` when the parquet sits in that folder.
 *
 * @param {string} outRaw `--out` value.
 * @param {string} county County slug.
 * @returns {PlacesPublicationPaths} Paths for parquet, sibling index, and NOTICE.
 */
export function resolvePlacesPublicationPaths(outRaw, county) {
  if (outRaw.endsWith(".parquet")) {
    const parquetDir = path.dirname(outRaw);
    const parentName = path.basename(parquetDir);
    const publicationRoot =
      parentName === county ? path.dirname(parquetDir) : parquetDir;
    return {
      publicationRoot,
      parquetPath: outRaw,
      indexPath: path.join(parquetDir, "index.json"),
      noticePath: path.join(publicationRoot, "NOTICE.txt"),
    };
  }
  return {
    publicationRoot: outRaw,
    parquetPath: path.join(outRaw, county, "places-table.parquet"),
    indexPath: path.join(outRaw, county, "index.json"),
    noticePath: path.join(outRaw, "NOTICE.txt"),
  };
}

/**
 * Parse export CLI flags.
 *
 * @param {readonly string[]} argv Arguments after the script name.
 * @returns {ExportCliOptions} Parsed options.
 */
export function parseExportCli(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      "from-neon": { type: "boolean" },
      "env-file": { type: "string" },
      "input-dir": { type: "string" },
      out: { type: "string" },
      county: { type: "string" },
      release: { type: "string" },
      "write-notice": { type: "boolean" },
    },
    strict: true,
    allowPositionals: false,
  });
  const fromNeon = values["from-neon"] === true;
  const county = typeof values.county === "string" ? values.county : "lee";
  const release =
    typeof values.release === "string" ? values.release : "2026-07-22.0";
  const envFile =
    typeof values["env-file"] === "string"
      ? values["env-file"]
      : "../elephant-query-db/.env.local";
  const inputDir =
    typeof values["input-dir"] === "string" ? values["input-dir"] : "";
  if (!fromNeon && inputDir.trim().length === 0) {
    throw new Error("--input-dir is required unless --from-neon is set");
  }
  const outRaw =
    typeof values.out === "string"
      ? values.out
      : fromNeon
        ? path.join("downloads/overture-places", county, release, "publish")
        : path.join(inputDir, county);
  return {
    fromNeon,
    envFile,
    inputDir,
    outRaw,
    county,
    release,
    writeNotice: fromNeon || values["write-notice"] === true,
  };
}

/**
 * Map one place record (JSONL or Neon) to a scalar parquet row.
 *
 * @param {JsonObject} record Place record.
 * @returns {JsonObject} Scalar parquet row (nulls omitted).
 */
export function toPlacesParquetRow(record) {
  const hierarchyPath =
    typeof record.taxonomy_hierarchy_path === "string"
      ? record.taxonomy_hierarchy_path
      : taxonomyHierarchyToPath(record.taxonomy_hierarchy);
  const confidence = coerceFiniteNumber(record.confidence);
  const longitude = coerceFiniteNumber(record.longitude);
  const latitude = coerceFiniteNumber(record.latitude);
  /** @type {JsonObject} */
  const row = {
    gers_id: String(record.gers_id ?? ""),
    county_key: record.county_key ?? null,
    county_fips: record.county_fips ?? null,
    name_primary: record.name_primary ?? null,
    taxonomy_primary: record.taxonomy_primary ?? null,
    taxonomy_hierarchy: hierarchyPath,
    basic_category: record.basic_category ?? null,
    legacy_category_primary: record.legacy_category_primary ?? null,
    operating_status: record.operating_status ?? null,
    confidence,
    longitude,
    latitude,
    address_freeform: record.address_freeform ?? null,
    address_locality: record.address_locality ?? null,
    address_postcode: record.address_postcode ?? null,
    address_region: record.address_region ?? null,
    address_country: record.address_country ?? null,
    brand_name: record.brand_name ?? null,
    brand_wikidata: record.brand_wikidata ?? null,
    is_hosted_service: record.is_hosted_service ?? null,
    hosted_service_rule: record.hosted_service_rule ?? null,
    overture_release: record.overture_release ?? null,
    websites: joinScalar(record.websites),
    phones: joinScalar(record.phones),
    emails: joinScalar(record.emails),
  };
  /** @type {JsonObject} */
  const parquet = {};
  for (const [key, value] of Object.entries(row)) {
    if (value !== null && value !== undefined) parquet[key] = value;
  }
  return parquet;
}

/**
 * Export places to parquet and run the publish gate. Neon is the source of
 * truth when `--from-neon` is set.
 *
 * @param {readonly string[]} argv CLI argv.
 * @returns {Promise<JsonObject>} Validation report.
 */
export async function runExport(argv) {
  const startedAt = Date.now();
  const options = parseExportCli(argv);
  const paths = resolvePlacesPublicationPaths(options.outRaw, options.county);
  const source = options.fromNeon
    ? await loadPlacesFromNeon(options)
    : await loadPlacesFromJsonl(options.inputDir);

  await mkdir(path.dirname(paths.parquetPath), { recursive: true });
  const writer = await ParquetWriter.openFile(
    buildPlacesTableParquetSchema(),
    paths.parquetPath,
  );
  try {
    for (const record of source.records) {
      await writer.appendRow(toPlacesParquetRow(record));
    }
  } finally {
    await writer.close();
  }

  const inspection = await inspectPlacesParquet(paths.parquetPath);
  const validation = validatePlacesTable({
    parquetRowCount: inspection.rowCount,
    businessLocationRowCount: source.businessLocationRowCount,
    gersIds: inspection.gersIds,
    nullGeometryCount: inspection.nullGeometryCount,
    licenceGate: source.licenceGate,
    invalidHierarchyCount: inspection.invalidHierarchyCount,
    hierarchyPresentCount: inspection.hierarchyPresentCount,
  });
  if (options.writeNotice) {
    await writePublicationSidecars({
      paths,
      county: options.county,
      rowCount: inspection.rowCount,
      licenceGate: source.licenceGate,
      overtureRelease: source.overtureRelease,
      accessedDate: source.accessedDate,
    });
  }
  const durationMs = Date.now() - startedAt;
  const report = {
    parquetPath: paths.parquetPath,
    indexPath: paths.indexPath,
    noticePath: paths.noticePath,
    publicationRoot: paths.publicationRoot,
    rowCount: inspection.rowCount,
    businessLocationRowCount: source.businessLocationRowCount,
    licenceGate: source.licenceGate,
    validation,
    durationMs,
    source: options.fromNeon ? "neon" : "jsonl",
    published: false,
  };
  if (!validation.passed) {
    const error = new Error(validation.errors.join("; "));
    error.name = "PlacesTableValidationError";
    throw error;
  }
  process.stdout.write(
    `${JSON.stringify({ event: "overture_places_export_finished", ...report })}\n`,
  );
  return report;
}

/**
 * Validate an already-written places parquet against Neon (preferred) or JSONL.
 *
 * @param {readonly string[]} argv CLI argv.
 * @returns {Promise<JsonObject>} Validation report.
 */
export async function runValidate(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      "from-neon": { type: "boolean" },
      "env-file": { type: "string" },
      "input-dir": { type: "string" },
      parquet: { type: "string" },
      county: { type: "string" },
      release: { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });
  const fromNeon = values["from-neon"] === true;
  const parquetPath = typeof values.parquet === "string" ? values.parquet : "";
  if (parquetPath.trim().length === 0) {
    throw new Error("--parquet is required");
  }
  const county = typeof values.county === "string" ? values.county : "lee";
  const release =
    typeof values.release === "string" ? values.release : "2026-07-22.0";
  const envFile =
    typeof values["env-file"] === "string"
      ? values["env-file"]
      : "../elephant-query-db/.env.local";
  const inputDir =
    typeof values["input-dir"] === "string" ? values["input-dir"] : "";
  if (!fromNeon && inputDir.trim().length === 0) {
    throw new Error("--input-dir is required unless --from-neon is set");
  }

  const inspection = await inspectPlacesParquet(parquetPath);
  const source = fromNeon
    ? await loadPlacesFromNeon({
        fromNeon: true,
        envFile,
        inputDir: "",
        outRaw: parquetPath,
        county,
        release,
        writeNotice: false,
      })
    : await loadPlacesFromJsonl(inputDir);
  const validation = validatePlacesTable({
    parquetRowCount: inspection.rowCount,
    businessLocationRowCount: source.businessLocationRowCount,
    gersIds: inspection.gersIds,
    nullGeometryCount: inspection.nullGeometryCount,
    licenceGate: source.licenceGate,
    invalidHierarchyCount: inspection.invalidHierarchyCount,
    hierarchyPresentCount: inspection.hierarchyPresentCount,
  });
  const report = {
    parquetPath,
    parquetCount: inspection.rowCount,
    businessLocationRowCount: source.businessLocationRowCount,
    licenceGate: source.licenceGate,
    validation,
    source: fromNeon ? "neon" : "jsonl",
  };
  process.stdout.write(
    `${JSON.stringify({ event: "overture_places_validate_finished", ...report })}\n`,
  );
  if (!validation.passed) {
    const error = new Error(validation.errors.join("; "));
    error.name = "PlacesTableValidationError";
    throw error;
  }
  return report;
}

/**
 * Scan a places parquet for the publish-gate counters.
 *
 * @param {string} parquetPath Path to `places-table.parquet`.
 * @returns {Promise<PlacesParquetInspection>} Inspection counters.
 */
export async function inspectPlacesParquet(parquetPath) {
  const { ParquetReader } = await import("@dsnp/parquetjs");
  const reader = await ParquetReader.openFile(parquetPath);
  try {
    const cursor = reader.getCursor();
    /** @type {string[]} */
    const gersIds = [];
    let nullGeometryCount = 0;
    let invalidHierarchyCount = 0;
    let hierarchyPresentCount = 0;
    let rowCount = 0;
    for (;;) {
      const raw = await cursor.next();
      if (raw === undefined || raw === null) break;
      rowCount += 1;
      const record = /** @type {JsonObject} */ (raw);
      gersIds.push(String(record.gers_id ?? ""));
      const longitude = coerceFiniteNumber(record.longitude);
      const latitude = coerceFiniteNumber(record.latitude);
      if (longitude === null || latitude === null) nullGeometryCount += 1;
      const hierarchy = record.taxonomy_hierarchy;
      if (typeof hierarchy === "string" && hierarchy.trim().length > 0) {
        hierarchyPresentCount += 1;
        if (!isValidTaxonomyHierarchyScalar(hierarchy))
          invalidHierarchyCount += 1;
      } else if (
        hierarchy !== undefined &&
        hierarchy !== null &&
        hierarchy !== ""
      ) {
        invalidHierarchyCount += 1;
      }
    }
    return {
      rowCount,
      gersIds,
      nullGeometryCount,
      invalidHierarchyCount,
      hierarchyPresentCount,
    };
  } finally {
    await reader.close();
  }
}

/**
 * @typedef {object} LoadedPlacesSource
 * @property {JsonObject[]} records Place records mapped to parquet rows.
 * @property {number} businessLocationRowCount Neon (or JSONL) current-row count.
 * @property {import("./overture-places-lib.mjs").LicenceGateResult} licenceGate Live licence gate.
 * @property {string} overtureRelease Release stamped on the artifact.
 * @property {string} accessedDate ISO date used in NOTICE.txt.
 */

/**
 * Load current Lee (or `--county`) places from Neon.
 *
 * @param {ExportCliOptions} options CLI options.
 * @returns {Promise<LoadedPlacesSource>} Records plus live licence gate.
 */
export async function loadPlacesFromNeon(options) {
  loadEnvFile(options.envFile);
  const databaseUrl = resolveUnpooledDatabaseUrl();
  if (databaseUrl === null) {
    throw new Error(
      `DATABASE_URL or DATABASE_URL_UNPOOLED is required in ${options.envFile} (not NEO_OPENDATA_DATABASE_URL)`,
    );
  }
  const client = new pg.Client({
    connectionString: databaseUrl,
    ssl: { rejectUnauthorized: false },
    connectionTimeoutMillis: 20_000,
    application_name: "oracle-node-overture-places-export",
  });
  await client.connect();
  try {
    const countResult = await client.query(
      `SELECT count(*)::int AS n
       FROM business_locations
       WHERE source_system = 'overture_places'
         AND county_key = $1
         AND is_current = true`,
      [options.county],
    );
    const businessLocationRowCount = Number(countResult.rows[0]?.n ?? 0);
    const rowsResult = await client.query(
      `SELECT
         gers_id,
         county_key,
         county_fips,
         name_primary,
         taxonomy_primary,
         taxonomy_hierarchy,
         basic_category,
         legacy_category_primary,
         operating_status,
         confidence,
         COALESCE(longitude::double precision, ST_X(geometry)) AS longitude,
         COALESCE(latitude::double precision, ST_Y(geometry)) AS latitude,
         address_freeform,
         address_locality,
         address_postcode,
         address_region,
         address_country,
         brand_name,
         brand_wikidata,
         is_hosted_service,
         hosted_service_rule,
         last_seen_release AS overture_release,
         websites,
         phones,
         emails
       FROM business_locations
       WHERE source_system = 'overture_places'
         AND county_key = $1
         AND is_current = true
       ORDER BY gers_id`,
      [options.county],
    );
    const datasetsResult = await client.query(
      `SELECT DISTINCT s.dataset
       FROM business_location_sources s
       JOIN business_locations l ON l.business_location_id = s.business_location_id
       WHERE l.source_system = 'overture_places'
         AND l.county_key = $1
         AND l.is_current = true
       ORDER BY 1`,
      [options.county],
    );
    const extractionResult = await client.query(
      `SELECT source_payload
       FROM overture_place_extractions
       WHERE county_key = $1 AND overture_release = $2
       LIMIT 1`,
      [options.county, options.release],
    );
    const datasets = datasetsResult.rows.map((row) => String(row.dataset));
    const licenceGate = assertApprovedPlaceDatasets(datasets);
    const accessedDate = accessedDateFromExtraction(
      extractionResult.rows[0]?.source_payload,
    );
    return {
      records: rowsResult.rows.map((row) => neonRowToPlaceRecord(row)),
      businessLocationRowCount,
      licenceGate,
      overtureRelease: options.release,
      accessedDate,
    };
  } finally {
    await client.end();
  }
}

/**
 * Prefer the unpooled Neon URL for bulk reads.
 *
 * @returns {string | null} Connection string, or null when neither URL is set.
 */
export function resolveUnpooledDatabaseUrl() {
  const unpooled = process.env.DATABASE_URL_UNPOOLED;
  if (typeof unpooled === "string" && unpooled.trim().length > 0)
    return unpooled.trim();
  const pooled = process.env.DATABASE_URL;
  if (typeof pooled === "string" && pooled.trim().length > 0)
    return pooled.trim();
  return null;
}

/**
 * Load env vars from a dotenv file without overwriting the process environment.
 *
 * @param {string} envFile Path to `.env.local`.
 */
export function loadEnvFile(envFile) {
  let text;
  try {
    text = readFileSync(envFile, "utf8");
  } catch (caught) {
    if (
      caught instanceof Error &&
      "code" in caught &&
      caught.code === "ENOENT"
    ) {
      return;
    }
    throw caught;
  }
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
    process.env[key] ??= value;
  }
}

/**
 * @param {string} inputDir Extract root.
 * @returns {Promise<LoadedPlacesSource>} JSONL records treated as the count source.
 */
async function loadPlacesFromJsonl(inputDir) {
  const records = await readPlaceJsonl(inputDir);
  const datasets = records.flatMap((record) =>
    collectDatasetsFromSources(record.sources),
  );
  const licenceGate = assertApprovedPlaceDatasets(datasets);
  const summary = await readSummary(inputDir);
  return {
    records,
    businessLocationRowCount: records.length,
    licenceGate,
    overtureRelease: String(summary.overtureRelease ?? "unknown"),
    accessedDate: String(summary.finishedAt ?? new Date().toISOString()).slice(
      0,
      10,
    ),
  };
}

/**
 * @param {JsonObject} row Neon `business_locations` row.
 * @returns {JsonObject} Record shaped for {@link toPlacesParquetRow}.
 */
function neonRowToPlaceRecord(row) {
  return {
    gers_id: row.gers_id,
    county_key: row.county_key,
    county_fips: row.county_fips,
    name_primary: row.name_primary,
    taxonomy_primary: row.taxonomy_primary,
    taxonomy_hierarchy: row.taxonomy_hierarchy,
    basic_category: row.basic_category,
    legacy_category_primary: row.legacy_category_primary,
    operating_status: row.operating_status,
    confidence: row.confidence,
    longitude: row.longitude,
    latitude: row.latitude,
    address_freeform: row.address_freeform,
    address_locality: row.address_locality,
    address_postcode: row.address_postcode,
    address_region: row.address_region,
    address_country: row.address_country,
    brand_name: row.brand_name,
    brand_wikidata: row.brand_wikidata,
    is_hosted_service: row.is_hosted_service,
    hosted_service_rule: row.hosted_service_rule,
    overture_release: row.overture_release,
    websites: row.websites,
    phones: row.phones,
    emails: row.emails,
  };
}

/**
 * @param {unknown} payload `overture_place_extractions.source_payload`.
 * @returns {string} ISO date (YYYY-MM-DD).
 */
function accessedDateFromExtraction(payload) {
  if (
    payload !== null &&
    typeof payload === "object" &&
    !Array.isArray(payload)
  ) {
    const finishedAt = /** @type {Record<string, unknown>} */ (payload)
      .finishedAt;
    if (typeof finishedAt === "string" && finishedAt.length >= 10) {
      return finishedAt.slice(0, 10);
    }
  }
  return new Date().toISOString().slice(0, 10);
}

/**
 * @param {object} params Sidecar inputs.
 * @param {PlacesPublicationPaths} params.paths Publication paths.
 * @param {string} params.county County slug.
 * @param {number} params.rowCount Parquet row count.
 * @param {import("./overture-places-lib.mjs").LicenceGateResult} params.licenceGate Licence gate.
 * @param {string} params.overtureRelease Release id.
 * @param {string} params.accessedDate NOTICE accessed date.
 */
async function writePublicationSidecars(params) {
  const elephantChangedDate = new Date().toISOString().slice(0, 10);
  const notice = renderPlacesNotice({
    overtureRelease: params.overtureRelease,
    accessedDate: params.accessedDate,
    elephantChangedDate,
    distinctDatasets: params.licenceGate.distinctDatasets,
  });
  await mkdir(params.paths.publicationRoot, { recursive: true });
  await writeFile(params.paths.noticePath, notice, "utf8");
  const noticeRelative = path.relative(
    path.dirname(params.paths.indexPath),
    params.paths.noticePath,
  );
  await mkdir(path.dirname(params.paths.indexPath), { recursive: true });
  await writeFile(
    params.paths.indexPath,
    `${JSON.stringify(
      {
        county: params.county,
        artifact: "places-table",
        rowCount: params.rowCount,
        overtureRelease: params.overtureRelease,
        localOnly: true,
        published: false,
        piiGate: "assumed-human-gate-applies",
        attribution: {
          notice: noticeRelative.split(path.sep).join("/"),
          citation: "Overture Maps Foundation Places",
          overtureRelease: params.overtureRelease,
          accessedDate: params.accessedDate,
          elephantChangedDate,
          foursquareCopyright:
            "Copyright 2024 Foursquare Labs, Inc. All rights reserved.",
          themeLicence:
            "CDLA-Permissive-2.0 and Apache-2.0 per record, with no OpenStreetMap lineage",
          licenceGate: params.licenceGate,
        },
      },
      null,
      2,
    )}\n`,
    "utf8",
  );
}

/**
 * @param {string} inputDir Extract root.
 * @returns {Promise<JsonObject[]>} JSONL records.
 */
async function readPlaceJsonl(inputDir) {
  const placesDir = path.join(inputDir, "places");
  const names = (await readdir(placesDir))
    .filter((name) => name.endsWith(".jsonl"))
    .sort();
  /** @type {JsonObject[]} */
  const records = [];
  for (const name of names) {
    const rl = createInterface({
      input: createReadStream(path.join(placesDir, name), "utf8"),
      crlfDelay: Infinity,
    });
    for await (const line of rl) {
      if (line.trim().length === 0) continue;
      records.push(JSON.parse(line));
    }
  }
  return records;
}

/**
 * @param {string} inputDir Extract root.
 * @returns {Promise<JsonObject>} Summary JSON.
 */
async function readSummary(inputDir) {
  try {
    return JSON.parse(
      await readFile(path.join(inputDir, "manifest/summary.json"), "utf8"),
    );
  } catch {
    return {};
  }
}

/**
 * @param {unknown} value Array or scalar.
 * @returns {string | null} Pipe-joined scalar.
 */
function joinScalar(value) {
  if (typeof value === "string") return value;
  if (!Array.isArray(value)) return null;
  const parts = value.filter(
    (item) => typeof item === "string" && item.trim().length > 0,
  );
  return parts.length > 0 ? parts.join("|") : null;
}

if (
  process.argv[1] !== undefined &&
  fileURLToPath(import.meta.url) === path.resolve(process.argv[1])
) {
  const command = process.argv[2];
  const argv =
    command === "validate" ? process.argv.slice(3) : process.argv.slice(2);
  const run = command === "validate" ? runValidate : runExport;
  run(argv).catch((caught) => {
    const message = caught instanceof Error ? caught.message : String(caught);
    process.stderr.write(
      `${JSON.stringify({ event: "overture_places_export_failed", error: message })}\n`,
    );
    process.exitCode = 1;
  });
}
