#!/usr/bin/env node

/**
 * Incremental Overture places extraction for one county and pinned release.
 *
 * The changelog is the change selector. Current geometries are first pruned by
 * the county bbox and then clipped with `ST_Within`. Existing Neon GERS IDs are
 * joined to removed/data_changed partitions so moves out and removals are
 * deactivated even when their current geometry is no longer in Lee.
 */

import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { createRequire } from "node:module";
import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import * as path from "node:path";

import {
  assertApprovedPlaceDatasets,
  collectDatasetsFromSources,
  confidenceDistribution,
  countByOperatingStatus,
  duckdbStringLiteral,
  hostedServiceRuleId,
  matchHostedService,
  overturePlacesParquetGlob,
  parseHostedServiceCategoryList,
  parseTigerBoundarySource,
  taxonomyHierarchyToPath,
  taxonomyPathToHierarchy,
} from "./overture-places-lib.mjs";
import {
  assertOvertureChangelogSchema,
  buildTaxonomyDriftReport,
  classifyPlaceChanges,
  overturePlacesChangelogGlobs,
} from "./overture-places-refresh-lib.mjs";
import {
  ensureTigerShapefile,
  mapPlaceRow,
} from "./extract-overture-places.mjs";

const require = createRequire(import.meta.url);
const duckdb = require("duckdb");

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * @typedef {object} IncrementalExtractOptions
 * @property {string} county County key.
 * @property {string} countyFips County FIPS.
 * @property {string} countyName Human county name.
 * @property {string} release Current pinned release.
 * @property {string} previousRelease Previous successful release.
 * @property {string} boundarySource TIGER boundary token.
 * @property {string} cacheDir Local DuckDB/TIGER cache root.
 * @property {string} hostedServiceListPath Committed hosted-service path list.
 * @property {string} workDir Local run directory.
 * @property {string} outputBucket Internal AWS S3 bucket.
 * @property {string} outputPrefix Run-scoped output prefix.
 * @property {string} databaseUrl Direct or pooled Neon URL for read-only ID selection.
 * @property {number} partRecordLimit Maximum JSONL rows per part.
 */

/**
 * @typedef {object} DuckdbConnection
 * @property {(sql: string) => Promise<JsonObject[]>} all Query rows.
 * @property {(sql: string) => Promise<void>} exec Execute statements.
 * @property {() => Promise<void>} close Close database.
 */

/**
 * Run a changelog extraction and stage the run-scoped artifacts to AWS S3.
 *
 * @param {IncrementalExtractOptions} options Validated extraction options.
 * @returns {Promise<JsonObject>} Summary consumed by validation and the loader.
 */
export async function runIncrementalPlacesExtract(options) {
  const startedAt = new Date();
  const runRoot = path.resolve(options.workDir);
  await rm(runRoot, { recursive: true, force: true });
  await mkdir(runRoot, { recursive: true });
  const tiger = parseTigerBoundarySource(options.boundarySource);
  const shapefilePath = await ensureTigerShapefile(options.cacheDir, tiger);
  const hostedServicePaths = parseHostedServiceCategoryList(
    await readFile(options.hostedServiceListPath, "utf8"),
  );
  const existingCurrentIds = await readCurrentCountyGersIds(
    options.databaseUrl,
    options.county,
  );
  const existingPath = path.join(runRoot, "existing-current-ids.json");
  await writeFile(
    existingPath,
    `${existingCurrentIds.map((gersId) => JSON.stringify({ gers_id: gersId })).join("\n")}\n`,
    "utf8",
  );

  const db = await openDuckdb();
  try {
    const duckdbTempDir = path.join(runRoot, "duckdb-tmp");
    await mkdir(duckdbTempDir, { recursive: true });
    await db.exec(
      "INSTALL spatial; INSTALL httpfs; LOAD spatial; LOAD httpfs;",
    );
    await db.exec("SET s3_region = 'us-west-2';");
    await db.exec(
      `SET temp_directory = ${duckdbStringLiteral(duckdbTempDir)};`,
    );
    await createRefreshViews({
      db,
      options,
      shapefilePath,
      existingPath,
    });

    const changelogSchemaRows = await db.all("DESCRIBE SELECT * FROM changes");
    const changelogColumns = changelogSchemaRows.flatMap((row) =>
      typeof row.column_name === "string" ? [row.column_name] : [],
    );
    const changelogSchema = assertOvertureChangelogSchema(changelogColumns);
    const currentCountyIds = (
      await db.all("SELECT id FROM current_county")
    ).map((row) => String(row.id));
    const changelogRows = (
      await db.all(
        `SELECT DISTINCT c.id, c.change_type
         FROM changes c
         JOIN (
           SELECT id FROM current_county
           UNION
           SELECT gers_id AS id FROM existing_ids
         ) relevant ON relevant.id = c.id
         WHERE c.change_type IN ('added', 'data_changed', 'removed')`,
      )
    ).map((row) => ({
      id: String(row.id),
      changeType: String(row.change_type),
    }));
    const classified = classifyPlaceChanges({
      existingCurrentIds,
      currentCountyIds,
      changelogRows,
    });
    const changedRows = await readChangedCurrentRows(db);
    const changedById = new Map(
      changelogRows.map((row) => [row.id, row.changeType]),
    );
    const mappedRows = changedRows.map((row) => {
      const mapped = mapIncrementalPlaceRow({
        row,
        options,
        hostedServicePaths,
      });
      mapped.change_type = changedById.get(String(row.gers_id)) ?? "unknown";
      return mapped;
    });
    const parts = await writeJsonlParts({
      records: mappedRows,
      runRoot,
      partRecordLimit: options.partRecordLimit,
    });
    const deactivationRows = classified.deactivateIds.map((gersId) => {
      const changeType = changedById.get(gersId);
      return {
        gersId,
        changeType,
        reason:
          changeType === "removed"
            ? "removed_from_overture_release"
            : "geometry_outside_county",
      };
    });
    await writeJson(path.join(runRoot, "manifest/deactivations.json"), {
      schemaVersion: "oracle-node.overture-place-deactivations.v1",
      county: options.county,
      countyFips: options.countyFips,
      release: options.release,
      previousRelease: options.previousRelease,
      records: deactivationRows,
    });

    const taxonomyPaths = (
      await db.all(
        `SELECT DISTINCT taxonomy.hierarchy AS hierarchy
         FROM current_county
         WHERE taxonomy.hierarchy IS NOT NULL`,
      )
    ).flatMap((row) => {
      const taxonomyPath = taxonomyHierarchyToPath(row.hierarchy);
      return taxonomyPath === null ? [] : [taxonomyPath];
    });
    const taxonomyDrift = buildTaxonomyDriftReport({
      release: options.release,
      previousRelease: options.previousRelease,
      currentPaths: taxonomyPaths,
      configuredPaths: hostedServicePaths,
    });
    await writeJson(
      path.join(runRoot, "manifest/taxonomy-drift.json"),
      taxonomyDrift,
    );
    await writeJson(
      path.join(runRoot, "manifest/changelog-schema.json"),
      changelogSchema,
    );

    const counters = await readFullCountyCounters(
      db,
      options.release,
      hostedServicePaths,
    );
    const incomingDatasets = uniqueSorted(
      changedRows.flatMap((row) => collectDatasetsFromSources(row.sources)),
    );
    const licenceGate = assertApprovedPlaceDatasets(incomingDatasets);
    const finishedAt = new Date();
    const summary = {
      schemaVersion: "oracle-node.overture-places-extract.v2",
      mode: "incremental",
      county: options.county,
      countyFips: options.countyFips,
      overtureRelease: options.release,
      previousRelease: options.previousRelease,
      boundarySource: options.boundarySource,
      tigerYear: tiger.year,
      bboxCount: counters.bboxCount,
      clipCount: counters.clipCount,
      activeChangeCount: mappedRows.length,
      deactivationCount: deactivationRows.length,
      changeCounts: classified.counts,
      moveCounts: {
        movedIn: classified.movedInIds.length,
        movedOut: classified.movedOutIds.length,
      },
      distinctTaxonomyPrimary: counters.distinctTaxonomyPrimary,
      distinctTaxonomyHierarchyPaths: taxonomyPaths.length,
      distinctSourceDatasets: incomingDatasets,
      licenceGate,
      taxonomyDrift,
      operatingStatusCounts: counters.operatingStatusCounts,
      confidenceDistribution: counters.confidenceDistribution,
      hostedServiceFlagCount: counters.hostedServiceFlagCount,
      jsonl: {
        partCount: parts.length,
        recordCount: mappedRows.length,
        parts,
      },
      deactivations: {
        recordCount: deactivationRows.length,
        path: "manifest/deactivations.json",
      },
      extractionLocation: "ecs-fargate",
      expectedCount: null,
      startedAt: startedAt.toISOString(),
      finishedAt: finishedAt.toISOString(),
      durationMs: finishedAt.getTime() - startedAt.getTime(),
      runStatus: "validated_pending",
    };
    await writeJson(path.join(runRoot, "manifest/summary.json"), summary);
    await uploadRunTree({
      localRoot: runRoot,
      bucket: options.outputBucket,
      prefix: options.outputPrefix,
    });
    return {
      ...summary,
      artifactS3Uri: `s3://${options.outputBucket}/${options.outputPrefix.replace(/\/+$/, "")}/`,
    };
  } finally {
    await db.close();
  }
}

/**
 * Build lazy DuckDB views over the current release and the three processed
 * changelog partitions.
 *
 * @param {object} params View inputs.
 * @param {DuckdbConnection} params.db DuckDB connection.
 * @param {IncrementalExtractOptions} params.options Extraction options.
 * @param {string} params.shapefilePath Local TIGER shapefile.
 * @param {string} params.existingPath Local JSONL containing current Neon IDs.
 * @returns {Promise<void>}
 */
async function createRefreshViews(params) {
  const changelogList = overturePlacesChangelogGlobs(params.options.release)
    .map(duckdbStringLiteral)
    .join(", ");
  await params.db.exec(`
CREATE OR REPLACE TEMP TABLE county_boundary AS
SELECT geom AS geometry
FROM ST_Read(${duckdbStringLiteral(params.shapefilePath)})
WHERE GEOID = ${duckdbStringLiteral(params.options.countyFips)};

CREATE OR REPLACE TEMP TABLE county_bbox AS
SELECT
  ST_XMin(ST_Extent(geometry)) AS xmin,
  ST_XMax(ST_Extent(geometry)) AS xmax,
  ST_YMin(ST_Extent(geometry)) AS ymin,
  ST_YMax(ST_Extent(geometry)) AS ymax
FROM county_boundary;

CREATE OR REPLACE TEMP TABLE existing_ids AS
SELECT gers_id
FROM read_json_auto(${duckdbStringLiteral(params.existingPath)}, format = 'newline_delimited');

CREATE OR REPLACE TEMP VIEW changes AS
SELECT id, bbox, change_type, theme, type
FROM read_parquet([${changelogList}], hive_partitioning = 1);

CREATE OR REPLACE TEMP VIEW current_county AS
SELECT p.*
FROM read_parquet(
       ${duckdbStringLiteral(overturePlacesParquetGlob(params.options.release))},
       hive_partitioning = 1
     ) p,
     county_bbox b,
     county_boundary c
WHERE p.bbox.xmin >= b.xmin
  AND p.bbox.xmax <= b.xmax
  AND p.bbox.ymin >= b.ymin
  AND p.bbox.ymax <= b.ymax
  AND ST_Within(p.geometry, c.geometry);
`);
}

/**
 * Read full current Overture records only for selected changed IDs.
 *
 * @param {DuckdbConnection} db DuckDB connection.
 * @returns {Promise<JsonObject[]>} Aliased place rows compatible with the initial mapper.
 */
async function readChangedCurrentRows(db) {
  return db.all(`
SELECT
  p.id                                AS gers_id,
  p.version                           AS overture_version,
  p.names.primary                     AS name_primary,
  p.taxonomy.primary                  AS taxonomy_primary,
  p.taxonomy.hierarchy                AS taxonomy_hierarchy,
  p.taxonomy.alternates               AS taxonomy_alternate,
  p.basic_category                    AS basic_category,
  p.categories.primary                AS legacy_category_primary,
  p.operating_status                  AS operating_status,
  p.confidence                        AS confidence,
  p.websites                          AS websites,
  p.socials                           AS socials,
  p.emails                            AS emails,
  p.phones                            AS phones,
  p.brand.names.primary               AS brand_name,
  p.brand.wikidata                    AS brand_wikidata,
  p.addresses[1].freeform             AS address_freeform,
  p.addresses[1].locality             AS address_locality,
  p.addresses[1].postcode             AS address_postcode,
  p.addresses[1].region               AS address_region,
  p.addresses[1].country              AS address_country,
  p.addresses[1]                      AS address0,
  p.sources                           AS sources,
  ST_X(p.geometry)                    AS longitude,
  ST_Y(p.geometry)                    AS latitude,
  ST_AsGeoJSON(p.geometry)            AS geometry_geojson
FROM current_county p
JOIN changes c ON c.id = p.id
WHERE c.change_type IN ('added', 'data_changed')
ORDER BY p.id`);
}

/**
 * Map a current-release changed place into loader JSON.
 *
 * @param {object} params Mapper inputs.
 * @param {JsonObject} params.row DuckDB row.
 * @param {IncrementalExtractOptions} params.options Extraction options.
 * @param {readonly string[]} params.hostedServicePaths Committed full paths.
 * @returns {JsonObject} Loader record.
 */
function mapIncrementalPlaceRow(params) {
  const taxonomyPath = taxonomyHierarchyToPath(params.row.taxonomy_hierarchy);
  const hosted = matchHostedService(
    taxonomyPath,
    params.hostedServicePaths,
    hostedServiceRuleId(params.options.release),
  );
  const base = mapPlaceRow({
    row: {
      ...params.row,
      overture_release: params.options.release,
      county_fips: params.options.countyFips,
    },
    options:
      /** @type {import("./overture-places-lib.mjs").ExtractCliOptions} */ ({
        county: params.options.county,
        countyFips: params.options.countyFips,
      }),
    countyName: params.options.countyName,
    hostedServicePaths: params.hostedServicePaths,
    ruleId: hostedServiceRuleId(params.options.release),
  });
  return {
    ...base,
    taxonomy_hierarchy: taxonomyPathToHierarchy(taxonomyPath),
    is_hosted_service: hosted.isHostedService,
    hosted_service_rule: hosted.hostedServiceRule,
  };
}

/**
 * Read full-county diagnostics without turning unchanged rows into loader work.
 *
 * @param {DuckdbConnection} db DuckDB connection.
 * @param {string} release Pinned Overture release.
 * @param {readonly string[]} hostedServicePaths Committed full taxonomy paths.
 * @returns {Promise<JsonObject>} Current county counters.
 */
async function readFullCountyCounters(db, release, hostedServicePaths) {
  const county = await db.all(`
SELECT
  count(*)::BIGINT AS clip_count,
  count(DISTINCT taxonomy.primary)::BIGINT AS distinct_taxonomy_primary
FROM current_county`);
  const statuses = await db.all(`
SELECT coalesce(operating_status, '(blank)') AS status, count(*)::BIGINT AS count
FROM current_county
GROUP BY 1`);
  const confidenceRows = await db.all(
    "SELECT confidence FROM current_county WHERE confidence IS NOT NULL",
  );
  const hierarchyRows = await db.all(
    "SELECT taxonomy.hierarchy AS hierarchy FROM current_county",
  );
  const hostedPathSet = new Set(hostedServicePaths);
  const hostedServiceFlagCount = hierarchyRows.filter((entry) => {
    const taxonomyPath = taxonomyHierarchyToPath(entry.hierarchy);
    return taxonomyPath !== null && hostedPathSet.has(taxonomyPath);
  }).length;
  const bbox = await db.all(`
SELECT count(*)::BIGINT AS bbox_count
FROM read_parquet(
       ${duckdbStringLiteral(overturePlacesParquetGlob(release))},
       hive_partitioning = 1
     ) p,
     county_bbox b
WHERE p.bbox.xmin >= b.xmin
  AND p.bbox.xmax <= b.xmax
  AND p.bbox.ymin >= b.ymin
  AND p.bbox.ymax <= b.ymax`);
  const row = county[0] ?? {};
  /** @type {Record<string, number>} */
  const operatingStatusCounts = {};
  for (const status of statuses) {
    operatingStatusCounts[String(status.status)] = Number(status.count);
  }
  return {
    bboxCount: Number(bbox[0]?.bbox_count ?? row.clip_count ?? 0),
    clipCount: Number(row.clip_count ?? 0),
    distinctTaxonomyPrimary: Number(row.distinct_taxonomy_primary ?? 0),
    hostedServiceFlagCount,
    operatingStatusCounts,
    confidenceDistribution: confidenceDistribution(
      confidenceRows.map((entry) => Number(entry.confidence)),
    ),
  };
}

/**
 * Read existing current county GERS IDs from Neon.
 *
 * @param {string} databaseUrl Neon connection string.
 * @param {string} county County key.
 * @returns {Promise<string[]>} Sorted current GERS IDs.
 */
async function readCurrentCountyGersIds(databaseUrl, county) {
  const pg = await import("pg");
  const client = new pg.default.Client({
    connectionString: databaseUrl,
    ssl: { rejectUnauthorized: false },
    connectionTimeoutMillis: 20_000,
    application_name: "oracle-node-overture-refresh-extract",
  });
  await client.connect();
  try {
    const result = await client.query(
      `SELECT gers_id
       FROM business_locations
       WHERE source_system = 'overture_places'
         AND county_key = $1
         AND is_current = true
       ORDER BY gers_id`,
      [county],
    );
    return result.rows.map((row) => String(row.gers_id));
  } finally {
    await client.end();
  }
}

/**
 * Write JSONL records in bounded parts.
 *
 * @param {object} params Writer inputs.
 * @param {readonly JsonObject[]} params.records Loader records.
 * @param {string} params.runRoot Local run root.
 * @param {number} params.partRecordLimit Rows per part.
 * @returns {Promise<string[]>} Relative part paths.
 */
async function writeJsonlParts(params) {
  /** @type {string[]} */
  const parts = [];
  for (
    let offset = 0, partNumber = 1;
    offset < params.records.length;
    offset += params.partRecordLimit, partNumber += 1
  ) {
    const relative = `places/places-part-${String(partNumber).padStart(4, "0")}.jsonl`;
    const records = params.records.slice(
      offset,
      offset + params.partRecordLimit,
    );
    await mkdir(path.dirname(path.join(params.runRoot, relative)), {
      recursive: true,
    });
    await writeFile(
      path.join(params.runRoot, relative),
      records.map((record) => `${JSON.stringify(record)}\n`).join(""),
      { encoding: "utf8", flag: "w" },
    );
    parts.push(relative);
  }
  return parts;
}

/**
 * Upload a run-scoped local tree to internal AWS S3.
 *
 * @param {object} params Upload inputs.
 * @param {string} params.localRoot Local root.
 * @param {string} params.bucket AWS S3 bucket.
 * @param {string} params.prefix Run-scoped key prefix.
 * @returns {Promise<void>}
 */
async function uploadRunTree(params) {
  const client = new S3Client({});
  /**
   * @param {string} dir Current local directory.
   * @param {string} relative Relative key.
   * @returns {Promise<void>}
   */
  const walk = async (dir, relative) => {
    const entries = await readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
      const nextRelative = relative ? `${relative}/${entry.name}` : entry.name;
      const fullPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        await walk(fullPath, nextRelative);
        continue;
      }
      if (nextRelative.startsWith("duckdb-tmp/")) continue;
      await client.send(
        new PutObjectCommand({
          Bucket: params.bucket,
          Key: `${params.prefix.replace(/\/+$/, "")}/${nextRelative}`,
          Body: await readFile(fullPath),
        }),
      );
    }
  };
  await walk(params.localRoot, "");
}

/**
 * Write formatted JSON, creating its parent directory.
 *
 * @param {string} filePath Destination file.
 * @param {unknown} value JSON value.
 * @returns {Promise<void>}
 */
async function writeJson(filePath, value) {
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

/**
 * Open a promisified in-memory DuckDB connection.
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
 * @param {readonly string[]} values Input strings.
 * @returns {string[]} Sorted unique values.
 */
function uniqueSorted(values) {
  return [...new Set(values)].sort((a, b) => a.localeCompare(b));
}
