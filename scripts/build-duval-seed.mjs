#!/usr/bin/env node

import { once } from "events";
import { createHash } from "crypto";
import { execFile } from "child_process";
import { createWriteStream } from "fs";
import {
  mkdir,
  readFile,
  readdir,
  rm,
  stat,
  writeFile,
} from "fs/promises";
import path from "path";
import { createRequire } from "module";
import { pathToFileURL } from "url";
import { promisify } from "util";

const require = createRequire(import.meta.url);
const duckdb = require("duckdb");

import {
  EXPECTED_NAL_ROWS,
  EXPECTED_PIN_FEATURES,
  EXPECTED_SDF_ROWS,
  NAL_SOURCE_FIELDS,
  NAL_SOURCE_URL,
  PIN_BBOX,
  PIN_SOURCE_URL,
  SDF_SOURCE_URL,
  SEED_COLUMNS,
  SMOKE_PARCEL_IDS,
  assertSafeSourceFields,
  assertSeedReconciliation,
  classifyDorUseBand,
  classifyPilotReasons,
  hasInRangePinGeometry,
  isValidDorParcelId,
  renderCsvRow,
  renderSeedCsv,
  toSeedRow,
} from "./duval/lib.mjs";

const execFileAsync = promisify(execFile);
const DEFAULT_OUTPUT_PATH = "downloads/duval/duval.csv";
const DEFAULT_WORK_DIR = "downloads/duval";
const REQUIRED_BANDS = Object.freeze([
  "vacant_residential",
  "single_family",
  "mobile_home",
  "multi_family",
  "condo",
  "commercial",
  "industrial",
  "agricultural",
  "institutional",
  "government",
]);
const EDGE_REASONS = Object.freeze([
  "multiple_buildings",
  "multiple_owners",
  "recent_sale",
  "zero_improvements",
  "old_construction",
]);

/**
 * @typedef {object} CliOptions
 * @property {string} outputPath
 * @property {string} workDir
 * @property {boolean} skipDownload
 * @property {boolean} skipSpotCheck
 */

/**
 * @param {readonly string[]} argv
 * @returns {CliOptions}
 */
export function parseCliOptions(argv) {
  /** @type {CliOptions} */
  const options = {
    outputPath: DEFAULT_OUTPUT_PATH,
    workDir: DEFAULT_WORK_DIR,
    skipDownload: false,
    skipSpotCheck: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--skip-download") {
      options.skipDownload = true;
      continue;
    }
    if (token === "--skip-spot-check") {
      options.skipSpotCheck = true;
      continue;
    }
    const value = argv[index + 1];
    if (typeof value !== "string" || value.startsWith("--")) {
      throw new Error(`Missing value for ${token}`);
    }
    if (token === "--output") options.outputPath = value;
    else if (token === "--work-dir") options.workDir = value;
    else throw new Error(`Unknown option: ${token}`);
    index += 1;
  }
  return options;
}

/**
 * Deterministic ~50-parcel pilot: smoke fixtures, one row per DOR_UC band,
 * then documented edge cases, then fill. Geometry must be in the Duval PIN bbox.
 *
 * @param {Iterable<Record<string, string>>} rows
 * @param {number} [limit=50]
 * @returns {Record<string, string>[]}
 */
export function selectPilotSample(rows, limit = 50) {
  if (!Number.isInteger(limit) || limit <= 0) {
    throw new Error("Pilot sample limit must be a positive integer");
  }
  const eligible = [...rows]
    .filter(
      (row) =>
        isValidDorParcelId(row.source_identifier) && hasInRangePinGeometry(row),
    )
    .sort((left, right) =>
      left.source_identifier.localeCompare(right.source_identifier),
    );
  /** @type {Map<string, Record<string, string>>} */
  const selected = new Map();
  /**
   * @param {Record<string, string> | undefined} row
   */
  const add = (row) => {
    if (!row || selected.size >= limit) return;
    if (selected.has(row.source_identifier)) return;
    selected.set(row.source_identifier, row);
  };

  for (const smokeId of SMOKE_PARCEL_IDS) {
    add(eligible.find((row) => row.source_identifier === smokeId));
  }
  const missingSmoke = SMOKE_PARCEL_IDS.filter((id) => !selected.has(id));
  if (missingSmoke.length > 0) {
    throw new Error(
      `Pilot sample missing Task 3 smoke parcels: ${missingSmoke.join(", ")}`,
    );
  }
  for (const band of REQUIRED_BANDS) {
    if (
      [...selected.values()].some(
        (row) => classifyDorUseBand(row.source_DOR_UC) === band,
      )
    ) {
      continue;
    }
    add(
      eligible.find(
        (row) =>
          !selected.has(row.source_identifier) &&
          classifyDorUseBand(row.source_DOR_UC) === band,
      ),
    );
  }
  for (const reason of EDGE_REASONS) {
    add(
      eligible.find(
        (row) =>
          !selected.has(row.source_identifier) &&
          classifyPilotReasons(row).includes(reason),
      ),
    );
  }
  for (const row of eligible) add(row);
  const selectedRows = [...selected.values()];
  const missingBands = REQUIRED_BANDS.filter(
    (band) =>
      !selectedRows.some(
        (row) => classifyDorUseBand(row.source_DOR_UC) === band,
      ),
  );
  if (missingBands.length > 0) {
    throw new Error(
      `Pilot sample missing DOR_UC bands: ${missingBands.join(", ")}`,
    );
  }
  return selectedRows;
}

/**
 * @param {string} dir
 * @param {string} extension
 * @returns {Promise<string[]>}
 */
async function filesWithExtension(dir, extension) {
  /** @type {string[]} */
  const matches = [];
  const entries = await readdir(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      matches.push(...(await filesWithExtension(fullPath, extension)));
    } else if (entry.name.toLowerCase().endsWith(extension)) {
      matches.push(fullPath);
    }
  }
  return matches;
}

/**
 * @param {string} uri
 * @returns {Promise<{ etag: string | null, contentLength: number | null }>}
 */
async function probeHead(uri) {
  const response = await fetch(uri, { method: "HEAD" });
  if (!response.ok) throw new Error(`HEAD ${uri} failed: ${response.status}`);
  return {
    etag: response.headers.get("etag"),
    contentLength: Number(response.headers.get("content-length") ?? "0") || null,
  };
}

/**
 * @param {string} uri
 * @param {string} dest
 * @returns {Promise<string>}
 */
async function downloadFile(uri, dest) {
  await mkdir(path.dirname(dest), { recursive: true });
  const response = await fetch(uri);
  if (!response.ok) throw new Error(`GET ${uri} failed: ${response.status}`);
  const bytes = Buffer.from(await response.arrayBuffer());
  await writeFile(dest, bytes);
  return createHash("sha256").update(bytes).digest("hex");
}

/**
 * @param {string} zipPath
 * @param {string} destDir
 */
async function unzipTo(zipPath, destDir) {
  await mkdir(destDir, { recursive: true });
  await execFileAsync("unzip", ["-o", "-q", zipPath, "-d", destDir]);
}

/**
 * @param {string} sql
 * @param {import("duckdb").Connection} connection
 * @returns {Promise<unknown[]>}
 */
function queryAll(connection, sql) {
  return new Promise((resolve, reject) => {
    connection.all(sql, (error, rows) => {
      if (error) reject(error);
      else resolve(rows ?? []);
    });
  });
}

/**
 * @param {string} sql
 * @param {import("duckdb").Connection} connection
 */
function execSql(connection, sql) {
  return new Promise((resolve, reject) => {
    connection.exec(sql, (error) => {
      if (error) reject(error);
      else resolve(undefined);
    });
  });
}

/**
 * @param {string} value
 * @returns {string}
 */
function sqlLiteral(value) {
  return `'${value.replaceAll("'", "''")}'`;
}

/**
 * @param {string} xml
 * @returns {{ accconst: string | null, useconst: string | null, sourcePath: string }}
 */
export function parsePinSidecarRights(xml, sourcePath = "") {
  const accconst = xml.match(/<accconst>([^<]*)<\/accconst>/i)?.[1] ?? null;
  const useconst = xml.match(/<useconst>([^<]*)<\/useconst>/i)?.[1] ?? null;
  return { accconst, useconst, sourcePath };
}

/**
 * @param {string} workDir
 * @param {boolean} skipDownload
 */
async function ensureArtifacts(workDir, skipDownload) {
  const nalZip = path.join(workDir, "nal-2026p.zip");
  const sdfZip = path.join(workDir, "sdf-2026p.zip");
  const pinZip = path.join(workDir, "pin-2026f.zip");
  const fingerprints = [];

  for (const artifact of [
    { uri: NAL_SOURCE_URL, dest: nalZip, name: "nal" },
    { uri: SDF_SOURCE_URL, dest: sdfZip, name: "sdf" },
    { uri: PIN_SOURCE_URL, dest: pinZip, name: "pin" },
  ]) {
    let sha256;
    let probe = { etag: null, contentLength: null };
    if (skipDownload) {
      const fileStat = await stat(artifact.dest);
      sha256 = createHash("sha256")
        .update(await readFile(artifact.dest))
        .digest("hex");
      probe = { etag: null, contentLength: fileStat.size };
    } else {
      probe = await probeHead(artifact.uri);
      sha256 = await downloadFile(artifact.uri, artifact.dest);
    }
    const extractDir = path.join(workDir, artifact.name);
    await rm(extractDir, { recursive: true, force: true });
    await unzipTo(artifact.dest, extractDir);
    fingerprints.push({
      name: artifact.name,
      uri: artifact.uri,
      ...probe,
      sha256,
      zipBytes: (await stat(artifact.dest)).size,
    });
  }

  const nalFiles = [
    ...(await filesWithExtension(path.join(workDir, "nal"), ".csv")),
    ...(await filesWithExtension(path.join(workDir, "nal"), ".txt")),
  ];
  const sdfFiles = [
    ...(await filesWithExtension(path.join(workDir, "sdf"), ".csv")),
    ...(await filesWithExtension(path.join(workDir, "sdf"), ".txt")),
  ];
  const pinShapes = await filesWithExtension(path.join(workDir, "pin"), ".shp");
  const pinXml = await filesWithExtension(path.join(workDir, "pin"), ".xml");
  if (!nalFiles[0]) throw new Error("No NAL txt/csv found after unzip");
  if (!sdfFiles[0]) throw new Error("No SDF txt/csv found after unzip");
  if (!pinShapes[0]) throw new Error("No PIN shapefile found after unzip");

  let sidecarRights = null;
  if (pinXml[0]) {
    sidecarRights = parsePinSidecarRights(
      await readFile(pinXml[0], "utf8"),
      pinXml[0],
    );
  }

  return {
    fingerprints,
    nalPath: nalFiles[0],
    sdfPath: sdfFiles[0],
    pinShp: pinShapes[0],
    sidecarRights,
  };
}


/**
 * @param {Record<string, unknown>} record
 * @param {{ sourceRevision: string, snapshotAt: string }} meta
 * @returns {Record<string, string>}
 */
function joinedRecordToSeedRow(record, meta) {
  /** @type {Record<string, unknown>} */
  const nal = {};
  for (const field of NAL_SOURCE_FIELDS) {
    nal[field] = record[field];
  }
  let geometry = null;
  if (record.geojson) {
    geometry = JSON.parse(String(record.geojson));
  }
  return toSeedRow({
    nal,
    pin: {
      latitude: record.latitude,
      longitude: record.longitude,
      geometry,
    },
    sdfSaleCount: Number(record.sale_count ?? 0),
    sourceRevision: meta.sourceRevision,
    snapshotAt: meta.snapshotAt,
    sourceRecordCount: Math.max(
      Number(record.nal_dup_count ?? 1),
      Number(record.pin_dup_count ?? 1),
      1,
    ),
  });
}

const JOINED_SQL = `WITH nal AS (
         SELECT * EXCLUDE (rn) FROM (
           SELECT *, row_number() OVER (
             PARTITION BY trim(PARCEL_ID)
             ORDER BY TRY_CAST(JV AS BIGINT) DESC NULLS LAST
           ) AS rn
           FROM nal_raw
           WHERE regexp_matches(trim(PARCEL_ID), '^[0-9]{10}R$')
         ) WHERE rn = 1
       ),
       pin AS (
         SELECT * EXCLUDE (rn) FROM (
           SELECT *, row_number() OVER (
             PARTITION BY PARCELNO
             ORDER BY area_sqm DESC NULLS LAST, latitude, longitude
           ) AS rn
           FROM pin_raw
         ) WHERE rn = 1
       ),
       nal_dups AS (
         SELECT trim(PARCEL_ID) AS PARCEL_ID, count(*) AS nal_dup_count
         FROM nal_raw
         WHERE regexp_matches(trim(PARCEL_ID), '^[0-9]{10}R$')
         GROUP BY 1
       ),
       pin_dups AS (
         SELECT PARCELNO, count(*) AS pin_dup_count
         FROM pin_raw
         GROUP BY 1
       ),
       sdf AS (
         SELECT trim(PARCEL_ID) AS PARCEL_ID, count(*) AS sale_count
         FROM sdf_raw
         GROUP BY 1
       )
       SELECT
         n.*,
         p.latitude,
         p.longitude,
         p.geojson,
         coalesce(s.sale_count, 0) AS sale_count,
         coalesce(nd.nal_dup_count, 1) AS nal_dup_count,
         coalesce(pd.pin_dup_count, 1) AS pin_dup_count
       FROM nal n
       LEFT JOIN pin p ON p.PARCELNO = trim(n.PARCEL_ID)
       LEFT JOIN sdf s ON s.PARCEL_ID = trim(n.PARCEL_ID)
       LEFT JOIN nal_dups nd ON nd.PARCEL_ID = trim(n.PARCEL_ID)
       LEFT JOIN pin_dups pd ON pd.PARCELNO = trim(n.PARCEL_ID)`;

/**
 * @param {{ nalPath: string, sdfPath: string, pinShp: string }} artifacts
 * @param {{ sourceRevision: string, snapshotAt: string }} meta
 * @param {{ writeRow: (row: Record<string, string>) => Promise<void>, writeUnkeyed: (record: unknown) => Promise<void> }} writers
 */
async function joinSeedRows(artifacts, meta, writers) {
  assertSafeSourceFields(NAL_SOURCE_FIELDS);
  const database = new duckdb.Database(":memory:");
  const connection = database.connect();
  try {
    await execSql(connection, "INSTALL spatial; LOAD spatial;");
    await execSql(
      connection,
      `CREATE TABLE nal_raw AS
       SELECT * FROM read_csv(${sqlLiteral(artifacts.nalPath)},
         all_varchar = true, header = true, ignore_errors = true, sample_size = -1);`,
    );
    await execSql(
      connection,
      `CREATE TABLE sdf_raw AS
       SELECT * FROM read_csv(${sqlLiteral(artifacts.sdfPath)},
         all_varchar = true, header = true, ignore_errors = true, sample_size = -1);`,
    );
    await execSql(
      connection,
      `CREATE TABLE pin_src AS
       SELECT PARCELNO, geom FROM ST_Read(${sqlLiteral(artifacts.pinShp)});`,
    );
    const pinCountRows = await queryAll(
      connection,
      "SELECT count(*) AS n FROM pin_src",
    );
    const pinCount = Number(pinCountRows[0]?.n);
    if (pinCount !== EXPECTED_PIN_FEATURES) {
      throw new Error(
        `PIN feature count ${pinCount} != published ${EXPECTED_PIN_FEATURES}`,
      );
    }
    await execSql(
      connection,
      `CREATE TABLE pin_raw AS
       SELECT
         trim(PARCELNO) AS PARCELNO,
         ST_Y(centroid) AS latitude,
         ST_X(centroid) AS longitude,
         ST_AsGeoJSON(ST_Transform(geom, 'EPSG:2881', 'EPSG:4326', always_xy := true)) AS geojson,
         ST_Area(geom) AS area_sqm
       FROM (
         SELECT PARCELNO, geom,
                ST_Centroid(ST_Transform(geom, 'EPSG:2881', 'EPSG:4326', always_xy := true)) AS centroid
         FROM pin_src
       )
       WHERE PARCELNO IS NOT NULL AND trim(PARCELNO) <> '';`,
    );

    const nalCountRows = await queryAll(
      connection,
      "SELECT count(*) AS n FROM nal_raw",
    );
    const nalCount = Number(nalCountRows[0]?.n);
    if (nalCount !== EXPECTED_NAL_ROWS) {
      throw new Error(
        `NAL row count ${nalCount} != published ${EXPECTED_NAL_ROWS}`,
      );
    }
    console.error(`NAL rows: ${nalCount}`);

    const sdfCountRows = await queryAll(
      connection,
      "SELECT count(*) AS n FROM sdf_raw",
    );
    const sdfCount = Number(sdfCountRows[0]?.n);
    if (sdfCount !== EXPECTED_SDF_ROWS) {
      throw new Error(
        `SDF row count ${sdfCount} != published ${EXPECTED_SDF_ROWS}`,
      );
    }

    const uniqueValidRows = await queryAll(
      connection,
      `SELECT count(*) AS n FROM (
         SELECT DISTINCT trim(PARCEL_ID) AS id FROM nal_raw
         WHERE regexp_matches(trim(PARCEL_ID), '^[0-9]{10}R$')
       )`,
    );
    const expectedSeedRowCount = Number(uniqueValidRows[0]?.n);
    const duplicateGroupsRows = await queryAll(
      connection,
      `SELECT count(*) AS n FROM (
         SELECT id FROM (
           SELECT trim(PARCEL_ID) AS id FROM nal_raw
           WHERE regexp_matches(trim(PARCEL_ID), '^[0-9]{10}R$')
           GROUP BY 1 HAVING count(*) > 1
           UNION
           SELECT PARCELNO AS id FROM pin_raw
           GROUP BY 1 HAVING count(*) > 1
         ) d
         WHERE id IN (
           SELECT DISTINCT trim(PARCEL_ID) FROM nal_raw
           WHERE regexp_matches(trim(PARCEL_ID), '^[0-9]{10}R$')
         )
       )`,
    );
    const duplicateGroups = Number(duplicateGroupsRows[0]?.n ?? 0);

    const bboxRows = await queryAll(
      connection,
      `SELECT min(latitude) AS min_lat, max(latitude) AS max_lat,
              min(longitude) AS min_lng, max(longitude) AS max_lng
       FROM pin_raw`,
    );
    const bbox = bboxRows[0] ?? {};
    console.error(
      `PIN centroid bbox: lat ${bbox.min_lat}..${bbox.max_lat}, lng ${bbox.min_lng}..${bbox.max_lng}`,
    );
    if (
      Number(bbox.min_lat) < PIN_BBOX.minLat ||
      Number(bbox.max_lat) > PIN_BBOX.maxLat ||
      Number(bbox.min_lng) < PIN_BBOX.minLng ||
      Number(bbox.max_lng) > PIN_BBOX.maxLng
    ) {
      throw new Error(
        `PIN centroids outside Duval bbox: lat ${bbox.min_lat}..${bbox.max_lat}, lng ${bbox.min_lng}..${bbox.max_lng}`,
      );
    }

    const invalidRows = await queryAll(
      connection,
      `SELECT PARCEL_ID FROM nal_raw
       WHERE PARCEL_ID IS NULL
          OR trim(PARCEL_ID) = ''
          OR regexp_matches(trim(PARCEL_ID), '^[0-9]{10}R$') = false`,
    );
    const unkeyedCount = invalidRows.length;
    console.error(`Unkeyed NAL rows: ${unkeyedCount}`);
    for (const record of invalidRows) await writers.writeUnkeyed(record);

    console.error("Joining NAL, SDF, and PIN…");
    await execSql(connection, `CREATE TABLE joined AS ${JOINED_SQL}`);
    const joinedCountRows = await queryAll(
      connection,
      "SELECT count(*) AS n FROM joined",
    );
    const joinedCount = Number(joinedCountRows[0]?.n);
    console.error(`Joined keyed rows: ${joinedCount}`);

    /** @type {Record<string, string>[]} */
    const compactRows = [];
    let rowsWritten = 0;
    const pageSize = 2_000;
    for (let offset = 0; offset < joinedCount; offset += pageSize) {
      const page = await queryAll(
        connection,
        `SELECT * FROM joined ORDER BY trim(PARCEL_ID) LIMIT ${pageSize} OFFSET ${offset}`,
      );
      for (const record of page) {
        const row = joinedRecordToSeedRow(
          /** @type {Record<string, unknown>} */ (record),
          meta,
        );
        await writers.writeRow(row);
        rowsWritten += 1;
        compactRows.push({
          ...row,
          parcel_polygon: "",
          source_features_json: "",
        });
      }
      if (offset === 0 || (offset / pageSize) % 25 === 0) {
        console.error(`Wrote ${rowsWritten} / ${joinedCount} seed rows`);
      }
    }

    const consolidatedRowsQuery = await queryAll(
      connection,
      `SELECT count(*) AS n FROM joined
       WHERE greatest(coalesce(nal_dup_count, 1), coalesce(pin_dup_count, 1)) > 1`,
    );
    const consolidatedRows = Number(consolidatedRowsQuery[0]?.n ?? 0);

    return {
      compactRows,
      rowsWritten,
      unkeyedCount,
      invalidRecordCount: unkeyedCount,
      expectedSeedRowCount,
      duplicateGroups,
      consolidatedRows,
      nalCount,
    };
  } finally {
    connection.close();
    database.close();
  }
}

/**
 * @param {import("fs").WriteStream} stream
 * @param {string} text
 */
async function writeChunk(stream, text) {
  if (stream.write(text)) return;
  await once(stream, "drain");
}

/**
 * @param {readonly Record<string, string>[]} sample
 */
async function spotCheckCoj(sample) {
  const targets = [
    ...SMOKE_PARCEL_IDS.map((id) =>
      sample.find((row) => row.source_identifier === id),
    ),
    ...sample,
  ]
    .filter(Boolean)
    .filter(
      (row, index, all) =>
        all.findIndex((item) => item.source_identifier === row.source_identifier) ===
        index,
    )
    .slice(0, 5);

  if (targets.length < 5) {
    throw new Error(`Need 5 spot-check parcels, got ${targets.length}`);
  }

  for (const row of targets) {
    const url = `${row.url}?RE=${row.source_identifier}`;
    const response = await fetch(url, { redirect: "follow" });
    if (!response.ok) {
      throw new Error(`COJ spot-check failed ${response.status} for ${url}`);
    }
    const html = await response.text();
    if (!html.includes(row.parcel_id) && !html.includes(row.source_identifier)) {
      throw new Error(`COJ page did not echo identifier for ${url}`);
    }
  }
  return targets.map((row) => row.source_identifier);
}

/**
 * @param {CliOptions} options
 */
export async function buildDuvalSeed(options) {
  await mkdir(options.workDir, { recursive: true });
  const snapshotAt = new Date().toISOString();
  const artifacts = await ensureArtifacts(options.workDir, options.skipDownload);
  const sourceRevision =
    artifacts.fingerprints.find((item) => item.name === "nal")?.sha256 ??
    snapshotAt;

  await mkdir(path.dirname(options.outputPath), { recursive: true });
  const writer = createWriteStream(options.outputPath);
  writer.write(`${SEED_COLUMNS.join(",")}\n`);
  const unkeyedPath = `${options.outputPath.replace(/\.csv$/i, "")}.unkeyed-features.jsonl`;
  const unkeyedWriter = createWriteStream(unkeyedPath);

  const {
    compactRows,
    rowsWritten,
    unkeyedCount,
    invalidRecordCount,
    expectedSeedRowCount,
    duplicateGroups,
    consolidatedRows,
  } = await joinSeedRows(
      artifacts,
      { sourceRevision, snapshotAt },
      {
        writeRow: (row) => writeChunk(writer, `${renderCsvRow(row)}\n`),
        writeUnkeyed: (record) =>
          writeChunk(unkeyedWriter, `${JSON.stringify(record)}\n`),
      },
    );

  await new Promise((resolve, reject) => {
    writer.end((error) => (error ? reject(error) : resolve(undefined)));
  });
  await new Promise((resolve, reject) => {
    unkeyedWriter.end((error) => (error ? reject(error) : resolve(undefined)));
  });

  const uniqueParcelIds = new Set(
    compactRows.map((row) => row.source_identifier),
  );
  assertSeedReconciliation({
    rowsWritten,
    uniqueParcelIds: uniqueParcelIds.size,
    expectedSeedRowCount,
    unkeyedSourceRecords: unkeyedCount,
    invalidRecordCount,
    consolidatedRows,
    duplicateGroups,
  });

  await writeFile(
    path.join(options.workDir, "artifact-fingerprints.json"),
    JSON.stringify(
      {
        snapshotAt,
        sidecarRights: artifacts.sidecarRights,
        artifacts: artifacts.fingerprints,
        rowsWritten,
        unkeyedCount,
        duplicateGroups,
      },
      null,
      2,
    ),
  );

  const compactPilot = selectPilotSample(compactRows, 50);
  if (compactPilot.length !== 50) {
    throw new Error(`Expected 50 pilot rows but selected ${compactPilot.length}`);
  }
  const wanted = new Set(
    compactPilot.map((row) => row.source_identifier),
  );
  /** @type {Map<string, Record<string, string>>} */
  const fullById = new Map();
  const { parse } = require("csv-parse");
  const { createReadStream } = require("fs");
  const parser = createReadStream(options.outputPath).pipe(
    parse({ columns: true, skip_empty_lines: true, trim: true }),
  );
  for await (const record of parser) {
    const id = record.source_identifier;
    if (wanted.has(id) && !fullById.has(id)) {
      fullById.set(id, record);
      if (fullById.size === wanted.size) break;
    }
  }
  const pilot = compactPilot.map((row) => {
    const full = fullById.get(row.source_identifier);
    if (!full) {
      throw new Error(`Missing full seed row for ${row.source_identifier}`);
    }
    return full;
  });
  const pilotPath = path.join(options.workDir, "pilot-seed-50.csv");
  await writeFile(pilotPath, renderSeedCsv(pilot), "utf8");

  let spotChecked = [];
  if (!options.skipSpotCheck) {
    spotChecked = await spotCheckCoj(pilot);
  }

  return {
    outputPath: options.outputPath,
    rowsWritten,
    unkeyedPath,
    unkeyedCount,
    duplicateGroups,
    pilotPath,
    pilotCount: pilot.length,
    sidecarRights: artifacts.sidecarRights,
    spotChecked,
    pilotIdentifiers: pilot.map((row) => row.source_identifier),
  };
}

const isMain =
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href;

if (isMain) {
  const options = parseCliOptions(process.argv.slice(2));
  buildDuvalSeed(options)
    .then((result) => {
      console.log(JSON.stringify(result, null, 2));
    })
    .catch((error) => {
      console.error(error instanceof Error ? error.stack || error.message : String(error));
      process.exitCode = 1;
    });
}
