#!/usr/bin/env node

/**
 * Extract Overture Maps places for one county, clipped to a Census TIGER/Line
 * county polygon. Output is chunked JSONL plus `manifest/summary.json` (BBB
 * harvest layout). DuckDB reads the public Overture S3 bucket with no credentials.
 *
 *   node scripts/extract-overture-places.mjs \
 *     --county lee --county-fips 12071 \
 *     --release 2026-07-22.0 \
 *     --boundary-source tiger/tl_2024_us_county \
 *     --output-dir downloads/overture-places/lee/2026-07-22.0
 *
 * Omit `--release` to discover the latest via the Overture STAC catalog; the
 * resolved id is pinned into the run record. `--counts-only` runs the two-stage
 * clip probe without writing JSONL.
 */

import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { execFile } from "node:child_process";
import { createWriteStream } from "node:fs";
import { mkdir, readFile, unlink, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import * as path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import {
  SCOPING_BASELINE_CLIP_COUNT,
  SCOPING_BASELINE_RELEASE,
  assertApprovedPlaceDatasets,
  buildCountyAssignment,
  buildExtractCopySql,
  buildExtractCountSql,
  collectDatasetsFromSources,
  confidenceDistribution,
  countByOperatingStatus,
  duckdbStringLiteral,
  hostedServiceRuleId,
  matchHostedService,
  overturePlacesParquetGlob,
  parseExtractCli,
  parseHostedServiceCategoryList,
  parseOvertureStacCatalog,
  parseTigerBoundarySource,
  placesPartPath,
  rebuildHostedServicePaths,
  HOSTED_SERVICE_SEED_LEAVES,
  formatHostedServiceCategoryList,
  taxonomyHierarchyToPath,
  taxonomyPathToHierarchy,
} from "./overture-places-lib.mjs";

const require = createRequire(import.meta.url);
const duckdb = require("duckdb");

const execFileAsync = promisify(execFile);

/**
 * @typedef {import("./overture-places-lib.mjs").ExtractCliOptions} ExtractCliOptions
 * @typedef {import("./overture-places-lib.mjs").OutputLocation} OutputLocation
 * @typedef {import("./overture-places-lib.mjs").OvertureStacDiscovery} OvertureStacDiscovery
 * @typedef {import("./overture-places-lib.mjs").LicenceGateResult} LicenceGateResult
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * @typedef {object} DuckdbConnection
 * @property {(sql: string) => Promise<unknown[]>} all
 * @property {(sql: string) => Promise<void>} exec
 * @property {() => Promise<void>} close
 */

/**
 * Run the extract CLI.
 *
 * @param {readonly string[]} argv Arguments after the script name.
 * @returns {Promise<JsonObject>} Summary object written to `manifest/summary.json`.
 */
export async function runExtract(argv) {
  const options = parseExtractCli(argv);
  const startedAt = new Date();
  const stac = await discoverRelease(options.stacCatalogUrl);
  const release = options.release ?? stac.latest;
  const tiger = parseTigerBoundarySource(options.boundarySource);
  const shapefilePath = await ensureTigerShapefile(options.cacheDir, tiger);
  const hostedServicePaths = parseHostedServiceCategoryList(
    await readFile(options.hostedServiceListPath, "utf8").catch(() => ""),
  );
  const ruleId = hostedServiceRuleId(release);
  const countyName = options.countyName ?? titleCaseCounty(options.county);

  const sqlParams = {
    releaseLiteral: duckdbStringLiteral(release),
    countyFipsLiteral: duckdbStringLiteral(options.countyFips),
    boundaryPathLiteral: duckdbStringLiteral(shapefilePath),
    outLiteral: duckdbStringLiteral(""),
    placesGlobLiteral: duckdbStringLiteral(overturePlacesParquetGlob(release)),
    limit: options.limit,
  };

  const db = await openDuckdb();
  try {
    const duckdbTempDir = path.resolve(options.cacheDir, "duckdb-tmp");
    await mkdir(duckdbTempDir, { recursive: true });
    await db.exec("INSTALL spatial; INSTALL httpfs;");
    await db.exec(
      `SET temp_directory = ${duckdbStringLiteral(duckdbTempDir)};`,
    );
    const counts = await runCountQuery(db, sqlParams);
    if (options.countsOnly) {
      const summary = buildCountsOnlySummary({
        options,
        release,
        stac,
        tiger,
        shapefilePath,
        counts,
        startedAt,
        finishedAt: new Date(),
      });
      await writeSummary(options.outputLocation, summary);
      logJson({ event: "overture_places_counts_only", ...summary });
      return summary;
    }

    const localRoot = await localOutputRoot(options.outputLocation);
    const parquetPath = path.join(localRoot, "places.parquet");
    await mkdir(localRoot, { recursive: true });
    sqlParams.outLiteral = duckdbStringLiteral(parquetPath);
    await db.exec(buildExtractCopySql(sqlParams));

    const rows = await db.all(
      `SELECT * FROM read_parquet(${duckdbStringLiteral(parquetPath)})`,
    );
    const jsonl = await writeJsonlChunks({
      rows,
      options,
      countyName,
      hostedServicePaths,
      ruleId,
      localRoot,
    });
    if (!options.keepParquet) {
      await unlink(parquetPath).catch(() => undefined);
    }
    const datasets = uniqueSorted(
      rows.flatMap((row) =>
        collectDatasetsFromSources(/** @type {JsonObject} */ (row).sources),
      ),
    );
    const licenceGate = assertApprovedPlaceDatasets(datasets);
    const taxonomyPaths = uniqueSorted(
      rows
        .map((row) =>
          taxonomyHierarchyToPath(
            /** @type {JsonObject} */ (row).taxonomy_hierarchy,
          ),
        )
        .filter(
          /** @type {(value: string | null) => value is string} */ (value) =>
            value !== null,
        ),
    );
    const hostedRebuild = rebuildHostedServicePaths({
      observedPaths: taxonomyPaths,
      seedLeaves: [...HOSTED_SERVICE_SEED_LEAVES],
    });
    const summary = buildExtractSummary({
      options,
      release,
      stac,
      tiger,
      shapefilePath,
      counts: { ...counts, clip_count: rows.length },
      jsonl,
      licenceGate,
      hostedRebuild,
      hostedServiceFlagCount: jsonl.hostedServiceCount,
      taxonomyPaths,
      rows,
      startedAt,
      finishedAt: new Date(),
      parquetPath: options.keepParquet ? parquetPath : null,
    });
    await writeSummary(options.outputLocation, summary);
    await writeHostedServiceRebuild(
      options.outputLocation,
      release,
      hostedRebuild,
    );
    if (options.outputLocation.kind === "s3") {
      await uploadLocalTree(localRoot, options.outputLocation);
    }
    logJson({
      event: "overture_places_extract_finished",
      ...summarizeForLog(summary),
    });
    if (!licenceGate.passed) {
      const error = new Error(licenceGate.message);
      error.name = "LicenceGateError";
      throw error;
    }
    return summary;
  } finally {
    await db.close();
  }
}

/**
 * Fetch and parse the Overture STAC catalog.
 *
 * @param {string} catalogUrl STAC catalog URL.
 * @returns {Promise<OvertureStacDiscovery>} Discovered releases.
 */
export async function discoverRelease(catalogUrl) {
  const retrievedAt = new Date().toISOString();
  const response = await fetch(catalogUrl);
  if (!response.ok) {
    throw new Error(
      `STAC catalog fetch failed: HTTP ${response.status} from ${catalogUrl}`,
    );
  }
  return parseOvertureStacCatalog(
    await response.json(),
    catalogUrl,
    retrievedAt,
  );
}

/**
 * Download and unzip the TIGER/Line county shapefile when it is not cached.
 *
 * @param {string} cacheDir Local cache root.
 * @param {{ year: string, stem: string, zipUrl: string }} tiger TIGER metadata.
 * @returns {Promise<string>} Absolute path to the `.shp` file.
 */
export async function ensureTigerShapefile(cacheDir, tiger) {
  const dir = path.resolve(cacheDir, tiger.stem);
  const shp = path.join(dir, `${tiger.stem}.shp`);
  try {
    await readFile(shp);
    return shp;
  } catch {
    // continue to download
  }
  await mkdir(dir, { recursive: true });
  const zipPath = path.join(dir, `${tiger.stem}.zip`);
  const response = await fetch(tiger.zipUrl);
  if (!response.ok) {
    throw new Error(
      `TIGER download failed: HTTP ${response.status} from ${tiger.zipUrl}`,
    );
  }
  const body = response.body;
  if (body === null) throw new Error("TIGER download returned an empty body");
  await pipeline(Readable.fromWeb(body), createWriteStream(zipPath));
  await execFileAsync("unzip", ["-o", zipPath, "-d", dir]);
  await readFile(shp);
  return shp;
}

/**
 * Open an in-memory DuckDB connection with promisified exec/all.
 *
 * @returns {Promise<DuckdbConnection>} Connection wrapper.
 */
async function openDuckdb() {
  const db = new duckdb.Database(":memory:");
  return {
    all(sql) {
      return new Promise((resolve, reject) => {
        db.all(sql, (error, rows) => {
          if (error) reject(error);
          else resolve(rows ?? []);
        });
      });
    },
    exec(sql) {
      return new Promise((resolve, reject) => {
        db.exec(sql, (error) => {
          if (error) reject(error);
          else resolve();
        });
      });
    },
    close() {
      return new Promise((resolve, reject) => {
        db.close((error) => {
          if (error) reject(error);
          else resolve();
        });
      });
    },
  };
}

/**
 * Run the bbox vs clip count query.
 *
 * @param {DuckdbConnection} db DuckDB connection.
 * @param {Parameters<typeof buildExtractCountSql>[0]} sqlParams SQL literals.
 * @returns {Promise<{ bbox_count: number, clip_count: number }>} Two-stage counts.
 */
async function runCountQuery(db, sqlParams) {
  const rows = await db.all(buildExtractCountSql(sqlParams));
  const row = /** @type {JsonObject | undefined} */ (rows[0]);
  return {
    bbox_count: Number(row?.bbox_count ?? 0),
    clip_count: Number(row?.clip_count ?? 0),
  };
}

/**
 * Write chunked JSONL place records and return counters.
 *
 * @param {object} params Writer inputs.
 * @param {unknown[]} params.rows DuckDB parquet rows.
 * @param {ExtractCliOptions} params.options CLI options.
 * @param {string} params.countyName Human county name.
 * @param {readonly string[]} params.hostedServicePaths Committed full taxonomy paths.
 * @param {string} params.ruleId Hosted-service rule id.
 * @param {string} params.localRoot Local extract root.
 * @returns {Promise<{ partCount: number, recordCount: number, hostedServiceCount: number, discrepancyCount: number, parts: string[] }>}
 */
async function writeJsonlChunks(params) {
  /** @type {string[]} */
  const parts = [];
  /** @type {JsonObject[]} */
  let buffer = [];
  let partNumber = 1;
  let hostedServiceCount = 0;
  let discrepancyCount = 0;

  /**
   * @param {boolean} flushRemaining Whether to flush a partial last part.
   * @returns {Promise<void>}
   */
  const flush = async (flushRemaining) => {
    if (buffer.length === 0) return;
    if (!flushRemaining && buffer.length < params.options.partRecordLimit)
      return;
    const relativePath = placesPartPath(partNumber);
    const filePath = path.join(params.localRoot, relativePath);
    await mkdir(path.dirname(filePath), { recursive: true });
    const body = buffer.map((record) => `${JSON.stringify(record)}\n`).join("");
    await writeFile(filePath, body, "utf8");
    parts.push(relativePath);
    partNumber += 1;
    buffer = [];
  };

  for (const raw of params.rows) {
    const record = mapPlaceRow({
      row: /** @type {JsonObject} */ (raw),
      options: params.options,
      countyName: params.countyName,
      hostedServicePaths: params.hostedServicePaths,
      ruleId: params.ruleId,
    });
    if (record.is_hosted_service === true) hostedServiceCount += 1;
    const assignment = /** @type {JsonObject} */ (record.county_assignment);
    if (assignment.discrepancy === true) discrepancyCount += 1;
    buffer.push(record);
    await flush(false);
  }
  await flush(true);
  return {
    partCount: parts.length,
    recordCount: params.rows.length,
    hostedServiceCount,
    discrepancyCount,
    parts,
  };
}

/**
 * Map one DuckDB extract row into the JSONL record the query-db loader consumes.
 *
 * @param {object} params Mapper inputs.
 * @param {JsonObject} params.row DuckDB row.
 * @param {ExtractCliOptions} params.options CLI options.
 * @param {string} params.countyName Human county name.
 * @param {readonly string[]} params.hostedServicePaths Committed full taxonomy paths.
 * @param {string} params.ruleId Hosted-service rule id.
 * @returns {JsonObject} JSONL place record.
 */
export function mapPlaceRow(params) {
  const taxonomyPath = taxonomyHierarchyToPath(params.row.taxonomy_hierarchy);
  const hosted = matchHostedService(
    taxonomyPath,
    params.hostedServicePaths,
    params.ruleId,
  );
  const countyAssignment = buildCountyAssignment({
    countyFips: params.options.countyFips,
    countyKey: params.options.county,
    countyName: params.countyName,
    address0: params.row.address0,
  });
  const longitude = toFiniteNumber(params.row.longitude);
  const latitude = toFiniteNumber(params.row.latitude);
  return {
    record_kind: "overture_place",
    schema_version: "oracle-node.overture-places.v1",
    source_system: "overture_places",
    gers_id: String(params.row.gers_id ?? ""),
    overture_version: params.row.overture_version ?? null,
    name_primary: params.row.name_primary ?? null,
    taxonomy_primary: params.row.taxonomy_primary ?? null,
    taxonomy_hierarchy: taxonomyPathToHierarchy(taxonomyPath),
    taxonomy_hierarchy_path: taxonomyPath,
    taxonomy_alternate: asStringArray(params.row.taxonomy_alternate),
    basic_category: params.row.basic_category ?? null,
    legacy_category_primary: params.row.legacy_category_primary ?? null,
    operating_status: params.row.operating_status ?? null,
    confidence: toFiniteNumber(params.row.confidence),
    websites: asStringArray(params.row.websites),
    socials: asStringArray(params.row.socials),
    emails: asStringArray(params.row.emails),
    phones: asStringArray(params.row.phones),
    brand_name: params.row.brand_name ?? null,
    brand_wikidata: params.row.brand_wikidata ?? null,
    address_freeform: params.row.address_freeform ?? null,
    address_locality: params.row.address_locality ?? null,
    address_postcode: params.row.address_postcode ?? null,
    address_region: params.row.address_region ?? null,
    address_country: params.row.address_country ?? null,
    sources: params.row.sources ?? [],
    longitude,
    latitude,
    geometry_geojson: params.row.geometry_geojson ?? null,
    overture_release: params.row.overture_release ?? null,
    county_fips: params.options.countyFips,
    county_key: params.options.county,
    is_hosted_service: hosted.isHostedService,
    hosted_service_rule: hosted.hostedServiceRule,
    county_assignment: countyAssignment,
    source_payload: params.row,
  };
}

/**
 * @param {object} params Summary inputs.
 * @returns {JsonObject} Counts-only summary.
 */
function buildCountsOnlySummary(params) {
  return {
    schemaVersion: "oracle-node.overture-places-extract.v1",
    mode: "counts-only",
    county: params.options.county,
    countyFips: params.options.countyFips,
    overtureRelease: params.release,
    stac: params.stac,
    scopingBaselineRelease: SCOPING_BASELINE_RELEASE,
    scopingBaselineClipCount: SCOPING_BASELINE_CLIP_COUNT,
    clipDeltaVsBaseline:
      params.release === SCOPING_BASELINE_RELEASE
        ? params.counts.clip_count - SCOPING_BASELINE_CLIP_COUNT
        : null,
    boundarySource: params.options.boundarySource,
    tigerYear: params.tiger.year,
    tigerShapefile: params.shapefilePath,
    bboxCount: params.counts.bbox_count,
    clipCount: params.counts.clip_count,
    note: "bboxCount is an optimisation diagnostic and must never be published as the county count.",
    extractionLocation: "laptop",
    startedAt: params.startedAt.toISOString(),
    finishedAt: params.finishedAt.toISOString(),
    durationMs: params.finishedAt.getTime() - params.startedAt.getTime(),
  };
}

/**
 * @param {object} params Summary inputs.
 * @returns {JsonObject} Full extract summary / run record.
 */
function buildExtractSummary(params) {
  const statuses = params.rows.map((row) =>
    String(/** @type {JsonObject} */ (row).operating_status ?? ""),
  );
  const confidences = params.rows.map((row) =>
    toFiniteNumber(/** @type {JsonObject} */ (row).confidence),
  );
  const distinctPrimary = uniqueSorted(
    params.rows
      .map((row) =>
        String(/** @type {JsonObject} */ (row).taxonomy_primary ?? "").trim(),
      )
      .filter((value) => value.length > 0),
  );
  return {
    schemaVersion: "oracle-node.overture-places-extract.v1",
    mode: "extract",
    county: params.options.county,
    countyFips: params.options.countyFips,
    overtureRelease: params.release,
    stac: params.stac,
    scopingBaselineRelease: SCOPING_BASELINE_RELEASE,
    scopingBaselineClipCount: SCOPING_BASELINE_CLIP_COUNT,
    clipDeltaVsBaseline:
      params.release === SCOPING_BASELINE_RELEASE
        ? params.jsonl.recordCount - SCOPING_BASELINE_CLIP_COUNT
        : null,
    boundarySource: params.options.boundarySource,
    tigerYear: params.tiger.year,
    tigerShapefile: params.shapefilePath,
    bboxCount: params.counts.bbox_count,
    clipCount: params.jsonl.recordCount,
    note: "bboxCount is an optimisation diagnostic and must never be published as the county count.",
    distinctTaxonomyPrimary: distinctPrimary.length,
    distinctTaxonomyHierarchyPaths: params.taxonomyPaths.length,
    distinctSourceDatasets: params.licenceGate.distinctDatasets,
    licenceGate: params.licenceGate,
    operatingStatusCounts: countByOperatingStatus(statuses),
    confidenceDistribution: confidenceDistribution(confidences),
    hostedServiceFlagCount: params.hostedServiceFlagCount,
    hostedServiceRebuild: params.hostedRebuild,
    addressCountyDiscrepancyCount: params.jsonl.discrepancyCount,
    jsonl: {
      partCount: params.jsonl.partCount,
      recordCount: params.jsonl.recordCount,
      parts: params.jsonl.parts,
    },
    parquetPath: params.parquetPath,
    extractionLocation: "laptop",
    piiPublishGate: "assumed-human-gate-applies",
    startedAt: params.startedAt.toISOString(),
    finishedAt: params.finishedAt.toISOString(),
    durationMs: params.finishedAt.getTime() - params.startedAt.getTime(),
  };
}

/**
 * Write the rebuilt hosted-service path list next to the extract summary.
 * The committed `config/hosted-service-categories.txt` is updated by the
 * operator after reviewing unresolved leaves and review candidates.
 *
 * @param {OutputLocation} outputLocation Destination.
 * @param {string} release Overture release id.
 * @param {import("./overture-places-lib.mjs").HostedServiceRebuild} hostedRebuild Rebuild result.
 * @returns {Promise<void>}
 */
async function writeHostedServiceRebuild(
  outputLocation,
  release,
  hostedRebuild,
) {
  const body = formatHostedServiceCategoryList({
    release,
    resolved: hostedRebuild.resolved,
    unresolvedLeaves: hostedRebuild.unresolvedLeaves,
    reviewCandidates: hostedRebuild.reviewCandidates,
  });
  const localRoot = await localOutputRoot(outputLocation);
  const filePath = path.join(
    localRoot,
    "manifest/hosted-service-categories.rebuilt.txt",
  );
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, body, "utf8");
}

/**
 * @param {OutputLocation} outputLocation Destination.
 * @param {JsonObject} summary Summary body.
 * @returns {Promise<void>}
 */
async function writeSummary(outputLocation, summary) {
  const body = `${JSON.stringify(summary, null, 2)}\n`;
  if (outputLocation.kind === "local") {
    const filePath = path.join(outputLocation.dir, "manifest/summary.json");
    await mkdir(path.dirname(filePath), { recursive: true });
    await writeFile(filePath, body, "utf8");
    return;
  }
  const localRoot = await localOutputRoot(outputLocation);
  const filePath = path.join(localRoot, "manifest/summary.json");
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, body, "utf8");
}

/**
 * @param {OutputLocation} outputLocation Destination.
 * @returns {Promise<string>} Local directory used as the extract root.
 */
async function localOutputRoot(outputLocation) {
  if (outputLocation.kind === "local") {
    await mkdir(outputLocation.dir, { recursive: true });
    return outputLocation.dir;
  }
  const dir = path.join(
    ".overture-places-runs",
    outputLocation.bucket,
    outputLocation.keyPrefix.replaceAll("/", "_"),
  );
  await mkdir(dir, { recursive: true });
  return dir;
}

/**
 * Upload a local extract tree to S3.
 *
 * @param {string} localRoot Local extract root.
 * @param {import("./overture-places-lib.mjs").S3OutputLocation} output S3 destination.
 * @returns {Promise<void>}
 */
async function uploadLocalTree(localRoot, output) {
  const { readdir } = await import("node:fs/promises");
  const s3 = new S3Client({});
  /**
   * @param {string} dir Directory to walk.
   * @param {string} relative Relative prefix.
   * @returns {Promise<void>}
   */
  const walk = async (dir, relative) => {
    const entries = await readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
      const nextRelative = relative ? `${relative}/${entry.name}` : entry.name;
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        await walk(full, nextRelative);
        continue;
      }
      const body = await readFile(full);
      await s3.send(
        new PutObjectCommand({
          Bucket: output.bucket,
          Key: `${output.keyPrefix}/${nextRelative}`.replace(/^\//, ""),
          Body: body,
        }),
      );
    }
  };
  await walk(localRoot, "");
}

/**
 * @param {string} county County slug.
 * @returns {string} Title-cased name.
 */
function titleCaseCounty(county) {
  return county
    .split("-")
    .map((part) => part.slice(0, 1).toUpperCase() + part.slice(1))
    .join(" ");
}

/**
 * @param {unknown} value Unknown array or DuckDB list.
 * @returns {string[]} String array.
 */
function asStringArray(value) {
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed.startsWith("[") || trimmed.startsWith("{")) {
      try {
        return asStringArray(JSON.parse(trimmed));
      } catch {
        return trimmed.length > 0 ? [trimmed] : [];
      }
    }
    return trimmed.length > 0 ? [trimmed] : [];
  }
  if (!Array.isArray(value)) return [];
  return value.flatMap((item) =>
    typeof item === "string" && item.trim() ? [item.trim()] : [],
  );
}

/**
 * @param {unknown} value Unknown number.
 * @returns {number | null} Finite number or null.
 */
function toFiniteNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

/**
 * @param {readonly string[]} values Strings.
 * @returns {string[]} Sorted unique strings.
 */
function uniqueSorted(values) {
  return [...new Set(values)].sort((a, b) => a.localeCompare(b));
}

/**
 * @param {JsonObject} summary Full summary.
 * @returns {JsonObject} Compact log payload.
 */
function summarizeForLog(summary) {
  return {
    county: summary.county,
    overtureRelease: summary.overtureRelease,
    clipCount: summary.clipCount,
    bboxCount: summary.bboxCount,
    licenceGatePassed: /** @type {LicenceGateResult} */ (summary.licenceGate)
      .passed,
    durationMs: summary.durationMs,
  };
}

/**
 * @param {JsonObject} value JSON log line.
 */
function logJson(value) {
  process.stdout.write(`${JSON.stringify(value)}\n`);
}

if (
  process.argv[1] !== undefined &&
  fileURLToPath(import.meta.url) === path.resolve(process.argv[1])
) {
  runExtract(process.argv.slice(2)).catch((caught) => {
    const message = caught instanceof Error ? caught.message : String(caught);
    process.stderr.write(
      `${JSON.stringify({ event: "overture_places_extract_failed", error: message })}\n`,
    );
    process.exitCode =
      caught instanceof Error && caught.name === "LicenceGateError" ? 2 : 1;
  });
}
