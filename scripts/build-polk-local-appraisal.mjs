#!/usr/bin/env node

/**
 * Build a complete, resumable Polk County appraisal publication from the
 * official local bulk CAMA text files.
 *
 * This path is intentionally local and bulk-first: it makes no browser, AWS,
 * database-service, or county-portal requests. Source columns carrying owners,
 * grantors, grantees, or mailing addresses are not imported into the work DB.
 */

import { createHash, randomUUID } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, readFile, readdir, rename, rm, stat } from "node:fs/promises";
import { createRequire } from "node:module";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { pipeline } from "node:stream/promises";
import { Transform } from "node:stream";
import { parseArgs } from "node:util";

import { ParquetWriter } from "@dsnp/parquetjs";

import {
  POLK_EXPORT_SCHEMA_VERSION,
  assertCheckpointCompatible,
  buildConsolidatedProperty,
  buildQueryTableParquetSchema,
  buildQueryTableRow,
  compareText,
  createCheckpoint,
  isJsonObject,
  normalizePolkBuildingCsvLine,
  normalizePolkLegalCsvLine,
  normalizePolkPermitCsvRecord,
  propertyQualityCounters,
  propertyRelativePath,
  readCheckpoint,
  readText,
  stableJson,
  sumQualityCounters,
  toParquetRecord,
  writeCheckpoint,
  writeFileAtomically,
  writeJsonAtomically,
} from "./polk-local-appraisal-lib.mjs";

const require = createRequire(import.meta.url);
const duckdb = require("duckdb");
const ipfsHash = require("ipfs-only-hash");

const DEFAULT_INPUT_DIRECTORY = "tmp/polk/bulk/extracted/FTP_CAMA";
const DEFAULT_OUTPUT_DIRECTORY = "tmp/polk/full";
const DEFAULT_BATCH_SIZE = 1_000;
const MAX_BATCH_SIZE = 10_000;
const PROPERTY_WRITE_CONCURRENCY = 24;
const WORK_DB_SCHEMA_VERSION = "4";

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * @typedef {object} DuckDbConnection
 * @property {(sql: string) => Promise<unknown[]>} all Run a row-returning SQL statement.
 * @property {(sql: string) => Promise<void>} exec Run a SQL statement.
 * @property {() => Promise<void>} close Close the database.
 */

/**
 * @typedef {object} PolkCliOptions
 * @property {string} inputDirectory Absolute bulk source directory.
 * @property {string} outputDirectory Absolute export directory.
 * @property {string} workDatabase Absolute persistent DuckDB path.
 * @property {number} batchSize Properties per atomic checkpoint.
 * @property {number | null} limit Optional property limit.
 * @property {boolean} restart Whether to replace only this managed output root.
 */

/**
 * @typedef {object} SourceFileSpec
 * @property {string} tableName Persistent DuckDB table name.
 * @property {string} fileName Source text file name.
 * @property {string} selectList Closed source-column projection.
 * @property {string} keyColumn Indexed join key.
 */

/**
 * @typedef {object} SourceFileSnapshot
 * @property {string} tableName DuckDB table name.
 * @property {string} fileName Source file name.
 * @property {string} absolutePath Absolute source file path.
 * @property {number} sizeBytes Source size.
 * @property {number} modifiedTimeMs Source modification time.
 * @property {string} fingerprint Per-table import fingerprint.
 */

/**
 * @typedef {object} SourceSnapshot
 * @property {string} fingerprint Whole-source fingerprint.
 * @property {string} collectedAt Deterministic latest-source timestamp.
 * @property {readonly SourceFileSnapshot[]} files Source files.
 */

/**
 * @typedef {object} PropertyBuildResult
 * @property {string} parcelIdentifier Canonical parcel id.
 * @property {string} propertyId Stable UUIDv5 property id.
 * @property {string} propertyCid Locally computed immutable CID.
 * @property {string} relativeFile Relative property JSON path.
 * @property {number} sizeBytes Property JSON byte count.
 * @property {string} sha256 Property JSON SHA-256.
 * @property {import("./polk-local-appraisal-lib.mjs").QueryTableRow} queryRow Query-table row.
 * @property {Record<string, number>} quality Quality counters.
 */

/**
 * @typedef {object} ShardManifest
 * @property {string} schemaVersion Export schema version.
 * @property {number} shardIndex Zero-based shard number.
 * @property {string} fromParcel First parcel id.
 * @property {string} toParcel Last parcel id.
 * @property {number} rowCount Shard property count.
 * @property {number} propertyBytes Consolidated JSON bytes.
 * @property {Record<string, number>} quality Summed quality counters.
 * @property {readonly JsonObject[]} entries Property file/CID entries.
 * @property {JsonObject} queryTableShard Query-table shard metadata.
 */

/**
 * Closed source projections. Sensitive source columns are omitted here so they
 * never enter the derived DuckDB cache:
 *
 * - `ftp_owner.txt` is not opened.
 * - `GRANTOR` and `GRANTEE` are omitted from sales.
 * - permit-site columns are omitted; only the canonical parcel situs is public.
 */
const SOURCE_FILE_SPECS = /** @type {readonly SourceFileSpec[]} */ ([
  {
    tableName: "polk_parcels",
    fileName: "ftp_parcel.txt",
    keyColumn: "parcel_id",
    selectList: `
      trim(PARCEL_ID) AS parcel_id,
      nullif(trim(DORUS_CODE), '') AS dor_use_code,
      nullif(trim(DORDESC), '') AS property_type,
      nullif(trim(DORDESC1), '') AS property_type_detail,
      nullif(trim(NH_CD), '') AS neighborhood_code,
      nullif(trim(NH_DSCR), '') AS neighborhood_description,
      nullif(trim(TOT_LND_VAL), '') AS land_value,
      nullif(trim(TOT_BLD_VAL), '') AS building_value,
      nullif(trim(TOT_XF_VAL), '') AS extra_feature_value,
      nullif(trim(TOTALVAL), '') AS market_value,
      nullif(trim(ASSESSVAL), '') AS assessed_value,
      nullif(trim(TAXVAL), '') AS taxable_value,
      nullif(trim(AMTDUE), '') AS yearly_tax_amount,
      nullif(trim(MILLRATE), '') AS millage_rate,
      nullif(trim(YR_CREATED), '') AS year_created,
      nullif(trim(YR_IMPROVED), '') AS year_improved,
      nullif(trim(LAST_INSP_DT), '') AS last_inspection_date,
      nullif(trim(TOT_ACREAGE), '') AS total_acreage,
      nullif(trim(PR_STRAP), '') AS related_parcel_identifier,
      nullif(trim(SUB), '') AS subdivision_code
    `,
  },
  {
    tableName: "polk_sites",
    fileName: "ftp_site.txt",
    keyColumn: "parcel_id",
    selectList: `
      trim(PARCEL_ID) AS parcel_id,
      nullif(trim(LN_NUM), '') AS line_number,
      nullif(trim(BLD_NUM), '') AS building_number,
      nullif(trim(STR), '') AS street,
      nullif(trim(STR_PFX), '') AS street_prefix,
      nullif(trim(STR_NUM), '') AS street_number,
      nullif(trim(STR_NUM_SFX), '') AS street_number_suffix,
      nullif(trim(STR_SFX), '') AS street_suffix,
      nullif(trim(STR_SFX_DIR), '') AS street_suffix_direction,
      nullif(trim(STR_UNIT), '') AS unit,
      nullif(trim(ZIP), '') AS postal_code,
      nullif(trim(CITY), '') AS city
    `,
  },
  {
    tableName: "polk_sales",
    fileName: "ftp_sales.txt",
    keyColumn: "parcel_id",
    selectList: `
      trim(PARCEL_ID) AS parcel_id,
      nullif(trim(SALE_ID), '') AS sale_id,
      nullif(trim(LN_NUM), '') AS line_number,
      nullif(trim(SALEDT), '') AS sale_date,
      nullif(trim(PRICE), '') AS price,
      nullif(trim(BOOK), '') AS book,
      nullif(trim(PAGE), '') AS page,
      nullif(trim(SALETYPE), '') AS sale_type,
      nullif(trim(TRNS_CD), '') AS transfer_code,
      nullif(trim(TRNS_DSCR), '') AS transfer_description,
      nullif(trim(INSTRTYP), '') AS instrument_type,
      nullif(trim(INSTRTYP_DSCR), '') AS instrument_description,
      nullif(trim(FORECLOSURE), '') AS foreclosure
    `,
  },
  {
    tableName: "polk_buildings",
    fileName: "ftp_bldg.txt",
    keyColumn: "parcel_id",
    selectList: `
      trim(PARCEL_ID) AS parcel_id,
      nullif(trim(BLD_NUM), '') AS building_number,
      nullif(trim(IMPR_TYPE), '') AS improvement_type,
      nullif(trim(IMPR_TYPE_DESC), '') AS improvement_description,
      nullif(trim(STYLE), '') AS style,
      nullif(trim(STYLE_DESC), '') AS style_description,
      nullif(trim(STORIES), '') AS stories,
      nullif(trim(BLDSHAPE), '') AS shape,
      nullif(trim(BLDSHAPEDESC), '') AS shape_description,
      nullif(trim(CLASS), '') AS class_code,
      nullif(trim(CLASS_DESC), '') AS class_description,
      nullif(trim(BATH), '') AS bathrooms,
      nullif(trim(UNITS), '') AS units,
      nullif(trim(BEDROOM), '') AS bedrooms,
      nullif(trim(FIREPLACE), '') AS fireplaces,
      nullif(trim(SUBDESC), '') AS substructure_description,
      nullif(trim(FRMDESC), '') AS frame_description,
      nullif(trim(EFF_YEAR), '') AS effective_year,
      nullif(trim(YEARBUILT), '') AS built_year,
      nullif(trim(EXWALLDESC), '') AS exterior_wall_description,
      nullif(trim(ROOFTYDESC), '') AS roof_description,
      nullif(trim(FLTYDESC), '') AS floor_description,
      nullif(trim(INTWALDESC), '') AS interior_wall_description,
      nullif(trim(LIVINGAREA), '') AS living_area,
      nullif(trim(TOTALUNDERROOF), '') AS total_under_roof,
      nullif(trim(TRAVERSE), '') AS traverse
    `,
  },
  {
    tableName: "polk_layouts",
    fileName: "ftp_bldg_sar.txt",
    keyColumn: "parcel_id",
    selectList: `
      trim(PARCEL_ID) AS parcel_id,
      nullif(trim(BLD_NUM), '') AS building_number,
      nullif(trim(LN_NUM), '') AS line_number,
      nullif(trim(SAR_CD), '') AS code,
      nullif(trim(SAR_DSCR), '') AS description,
      nullif(trim(ACT_AR), '') AS actual_area,
      nullif(trim(HEAT_AR), '') AS heated_area
    `,
  },
  {
    tableName: "polk_lands",
    fileName: "ftp_land.txt",
    keyColumn: "parcel_id",
    selectList: `
      trim(PARCEL_ID) AS parcel_id,
      nullif(trim(LINENUM), '') AS line_number,
      nullif(trim(LND_TP), '') AS land_type,
      nullif(trim(USECODE), '') AS use_code,
      nullif(trim(USEDESC), '') AS use_description,
      nullif(trim(FRONTAGE), '') AS frontage,
      nullif(trim(DEPTH), '') AS depth,
      nullif(trim(UNITS), '') AS units,
      nullif(trim(UNITTYPE), '') AS unit_type,
      nullif(trim(UNITTPDSCR), '') AS unit_type_description,
      nullif(trim(INFCODE), '') AS influence_code,
      nullif(trim(INFDESC), '') AS influence_description
    `,
  },
  {
    tableName: "polk_legal_descriptions",
    fileName: "ftp_legal.txt",
    keyColumn: "parcel_id",
    selectList: `
      trim(PARCEL_ID) AS parcel_id,
      nullif(trim(NUM), '') AS line_number,
      nullif(trim(DSCR), '') AS description
    `,
  },
  {
    tableName: "polk_permits",
    fileName: "ftp_permit.txt",
    keyColumn: "parcel_id",
    selectList: `
      trim(PARCEL_ID) AS parcel_id,
      nullif(trim(ID), '') AS permit_id,
      nullif(trim(AGENCY_NAME), '') AS agency_name,
      nullif(trim(PERMIT_NUM), '') AS permit_number,
      nullif(trim(STATUS), '') AS status,
      nullif(trim(STATUS_DSCR), '') AS status_description,
      nullif(trim(DSCR), '') AS description,
      nullif(trim(PERMIT_TYPE), '') AS permit_type,
      nullif(trim(ISSUE_DT), '') AS issue_date,
      nullif(trim(FINAL_DT), '') AS final_date,
      nullif(trim(YR), '') AS year,
      nullif(trim(EST_VAL), '') AS estimated_value,
      nullif(trim(CO_DT), '') AS certificate_of_occupancy_date
    `,
  },
  {
    tableName: "polk_subdivisions",
    fileName: "ftp_sub.txt",
    keyColumn: "subdivision_code",
    selectList: `
      trim("SUB NUMBER") AS subdivision_code,
      nullif(trim(NAME), '') AS subdivision_name
    `,
  },
]);

/**
 * Parse and validate CLI arguments.
 *
 * @param {readonly string[]} argv Arguments after the script path.
 * @returns {PolkCliOptions} Fully resolved local options.
 */
export function parsePolkCliOptions(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      "input-dir": { type: "string" },
      out: { type: "string" },
      "work-db": { type: "string" },
      "batch-size": { type: "string" },
      limit: { type: "string" },
      restart: { type: "boolean" },
    },
    strict: true,
    allowPositionals: false,
  });
  const inputDirectory = path.resolve(
    values["input-dir"] ?? DEFAULT_INPUT_DIRECTORY,
  );
  const outputDirectory = path.resolve(values.out ?? DEFAULT_OUTPUT_DIRECTORY);
  const batchSize = parsePositiveInteger(
    values["batch-size"] ?? String(DEFAULT_BATCH_SIZE),
    "--batch-size",
  );
  if (batchSize > MAX_BATCH_SIZE) {
    throw new Error(
      `--batch-size cannot exceed ${MAX_BATCH_SIZE}; batch-bounded memory is a safety contract`,
    );
  }
  const limit =
    values.limit === undefined
      ? null
      : parsePositiveInteger(values.limit, "--limit");
  const workDatabase = path.resolve(
    values["work-db"] ??
      path.join(path.dirname(inputDirectory), "polk-appraisal.duckdb"),
  );
  if (workDatabase.startsWith(`${outputDirectory}${path.sep}`)) {
    throw new Error(
      "--work-db must be outside --out so multiple pilot/full outputs can reuse the imported bulk cache",
    );
  }
  return {
    inputDirectory,
    outputDirectory,
    workDatabase,
    batchSize,
    limit,
    restart: values.restart ?? false,
  };
}

/**
 * Parse one strictly positive integer option.
 *
 * @param {string} value Raw option.
 * @param {string} label Option label.
 * @returns {number} Positive integer.
 */
function parsePositiveInteger(value, label) {
  if (!/^\d+$/.test(value)) throw new Error(`${label} must be an integer`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${label} must be a positive safe integer`);
  }
  return parsed;
}

/**
 * Quote one trusted local path or scalar as a DuckDB SQL string literal.
 *
 * @param {string} value Literal value.
 * @returns {string} SQL literal.
 */
function sqlString(value) {
  return `'${value.replaceAll("'", "''")}'`;
}

/**
 * Open a persistent DuckDB database with promise-based wrappers.
 *
 * @param {string} databasePath Work database path.
 * @returns {Promise<DuckDbConnection>} Open connection.
 */
async function openDuckDb(databasePath) {
  await mkdir(path.dirname(databasePath), { recursive: true });
  const database = new duckdb.Database(databasePath);
  const connection = database.connect();
  return {
    all(sql) {
      return new Promise((resolve, reject) => {
        connection.all(sql, (error, rows) => {
          if (error) reject(error);
          else resolve(rows ?? []);
        });
      });
    },
    exec(sql) {
      return new Promise((resolve, reject) => {
        connection.exec(sql, (error) => {
          if (error) reject(error);
          else resolve();
        });
      });
    },
    close() {
      return new Promise((resolve, reject) => {
        connection.close((connectionError) => {
          if (connectionError) {
            reject(connectionError);
            return;
          }
          database.close((databaseError) => {
            if (databaseError) reject(databaseError);
            else resolve();
          });
        });
      });
    },
  };
}

/**
 * Snapshot required source files using size and modification time. This avoids
 * rereading more than a gigabyte solely for hashing while still preventing a
 * checkpoint from silently resuming against changed files.
 *
 * @param {string} inputDirectory Bulk input directory.
 * @returns {Promise<SourceSnapshot>} Stable source snapshot.
 */
async function snapshotSourceFiles(inputDirectory) {
  /** @type {SourceFileSnapshot[]} */
  const files = [];
  for (const spec of SOURCE_FILE_SPECS) {
    const absolutePath = path.join(inputDirectory, spec.fileName);
    const info = await stat(absolutePath);
    if (!info.isFile() || info.size === 0) {
      throw new Error(
        `Required source is not a non-empty file: ${absolutePath}`,
      );
    }
    const fingerprint = createHash("sha256")
      .update(
        stableJson({
          fileName: spec.fileName,
          modifiedTimeMs: info.mtimeMs,
          normalizationVersion:
            spec.tableName === "polk_buildings"
              ? "repair-extra-final-quote-v1"
              : spec.tableName === "polk_legal_descriptions"
                ? "escape-legal-description-quotes-v1"
                : spec.tableName === "polk_permits"
                  ? "windows-1252-quotes-newlines-v3"
                  : "raw-v1",
          projection: spec.selectList,
          sizeBytes: info.size,
          workDbSchemaVersion: WORK_DB_SCHEMA_VERSION,
        }),
      )
      .digest("hex");
    files.push({
      tableName: spec.tableName,
      fileName: spec.fileName,
      absolutePath,
      sizeBytes: info.size,
      modifiedTimeMs: info.mtimeMs,
      fingerprint,
    });
  }
  const latestModifiedTime = Math.max(
    ...files.map((file) => file.modifiedTimeMs),
  );
  const fingerprint = createHash("sha256")
    .update(
      stableJson(
        files.map((file) => ({
          fileName: file.fileName,
          fingerprint: file.fingerprint,
          modifiedTimeMs: file.modifiedTimeMs,
          sizeBytes: file.sizeBytes,
        })),
      ),
    )
    .digest("hex");
  return {
    fingerprint,
    collectedAt: new Date(latestModifiedTime).toISOString(),
    files,
  };
}

/**
 * Repair known Polk source defects without skipping rows. A small set of
 * building records ends a non-empty TRAVERSE field with two quotes instead of
 * one. Legal descriptions contain unescaped inch/section double quotes in the
 * final field. The permit file uses Windows-1252 rather than UTF-8. These
 * streaming repairs preserve every physical source row.
 *
 * The repaired copy is keyed by source fingerprint and reused by pilot/full
 * outputs. It is streamed, written to a sibling temp file, then renamed.
 *
 * @param {SourceFileSnapshot} source Original building source.
 * @param {string} workDatabase Persistent DuckDB path.
 * @returns {Promise<string>} Original or repaired import path.
 */
async function resolveImportSource(source, workDatabase) {
  if (
    source.tableName !== "polk_buildings" &&
    source.tableName !== "polk_legal_descriptions" &&
    source.tableName !== "polk_permits"
  ) {
    return source.absolutePath;
  }
  const cacheDirectory = `${workDatabase}.sources`;
  const destination = path.join(
    cacheDirectory,
    `${path.basename(source.fileName, path.extname(source.fileName))}.${source.fingerprint}.csv`,
  );
  try {
    const cached = await stat(destination);
    if (cached.isFile() && cached.size > 0) return destination;
  } catch (caught) {
    if (
      !(
        caught instanceof Error &&
        "code" in caught &&
        /** @type {NodeJS.ErrnoException} */ (caught).code === "ENOENT"
      )
    ) {
      throw caught;
    }
  }
  await mkdir(cacheDirectory, { recursive: true });
  const temporary = `${destination}.${process.pid}.${randomUUID()}.tmp`;
  let remainder = "";
  let repairedRows = 0;
  let lineNumber = 0;
  let pendingPermitRecord = "";
  const decoder = new TextDecoder(
    source.tableName === "polk_permits" ? "windows-1252" : "utf-8",
  );

  /**
   * Repair one physical CSV line while preserving its optional carriage return.
   *
   * @param {string} line Physical line without newline.
   * @returns {string} Repaired or unchanged line.
   */
  const repairLine = (line) => {
    const hasCarriageReturn = line.endsWith("\r");
    const content = hasCarriageReturn ? line.slice(0, -1) : line;
    if (
      source.tableName === "polk_buildings" &&
      content.endsWith('""') &&
      !content.endsWith(',""')
    ) {
      repairedRows += 1;
      return normalizePolkBuildingCsvLine(line);
    }
    if (source.tableName === "polk_legal_descriptions") {
      const normalized = normalizePolkLegalCsvLine(line, lineNumber);
      if (normalized !== line) repairedRows += 1;
      return normalized;
    }
    if (source.tableName === "polk_permits") {
      const normalized = normalizePolkPermitCsvRecord(line, lineNumber);
      if (normalized !== line) repairedRows += 1;
      return normalized;
    }
    return line;
  };

  /**
   * Join a malformed multiline permit description before normalizing fields.
   * Permit records have exactly 18 fields and therefore 17 `","` delimiters;
   * continuation lines remain buffered until that boundary is present.
   *
   * @param {string} line Physical source line.
   * @returns {string | null} Complete normalized line, or null while buffering.
   */
  const normalizePhysicalLine = (line) => {
    lineNumber += 1;
    if (source.tableName !== "polk_permits") return repairLine(line);
    const content = line.endsWith("\r") ? line.slice(0, -1) : line;
    pendingPermitRecord =
      pendingPermitRecord.length === 0
        ? content
        : `${pendingPermitRecord} ${content}`;
    const delimiterCount = pendingPermitRecord.split('","').length - 1;
    if (delimiterCount < 17) return null;
    const completeRecord = pendingPermitRecord;
    pendingPermitRecord = "";
    return repairLine(completeRecord);
  };

  const repairTransform = new Transform({
    transform(chunk, _encoding, callback) {
      const text =
        remainder +
        decoder.decode(Buffer.from(chunk), {
          stream: true,
        });
      const lines = text.split("\n");
      remainder = lines.pop() ?? "";
      callback(
        null,
        lines
          .flatMap((line) => {
            const normalized = normalizePhysicalLine(line);
            return normalized === null ? [] : [`${normalized}\n`];
          })
          .join(""),
      );
    },
    flush(callback) {
      const finalText = remainder + decoder.decode();
      const normalized =
        finalText.length === 0 ? null : normalizePhysicalLine(finalText);
      if (pendingPermitRecord.length > 0) {
        callback(
          new Error(
            `Permit CSV ended with an incomplete record near line ${lineNumber}`,
          ),
        );
        return;
      }
      callback(null, normalized ?? "");
    },
  });
  try {
    await pipeline(
      createReadStream(source.absolutePath),
      repairTransform,
      createWriteStream(temporary, { mode: 0o600 }),
    );
    await rename(temporary, destination);
  } catch (caught) {
    await rm(temporary, { force: true });
    throw caught;
  }
  emit({
    event: "polk_source_normalized",
    table: source.tableName,
    repairedRows,
    file: destination,
  });
  return destination;
}

/**
 * Import closed source projections into persistent, indexed DuckDB tables.
 * Each table import and metadata update is one transaction. A crash therefore
 * leaves either the old complete table or no trusted metadata, never a
 * checkpoint that claims a partial import is usable.
 *
 * @param {DuckDbConnection} database DuckDB connection.
 * @param {SourceSnapshot} snapshot Source snapshot.
 * @param {string} workDatabase Persistent DuckDB path.
 * @returns {Promise<void>} Resolves after all projections are reusable.
 */
async function ensureWorkDatabase(database, snapshot, workDatabase) {
  await database.exec(`
    SET preserve_insertion_order = false;
    CREATE TABLE IF NOT EXISTS oracle_local_imports (
      table_name VARCHAR PRIMARY KEY,
      source_fingerprint VARCHAR NOT NULL,
      imported_at TIMESTAMP NOT NULL,
      row_count BIGINT NOT NULL
    );
  `);
  for (const spec of SOURCE_FILE_SPECS) {
    const source = snapshot.files.find(
      (candidate) => candidate.tableName === spec.tableName,
    );
    if (source === undefined) {
      throw new Error(`No source snapshot for ${spec.tableName}`);
    }
    const metadataRows = asJsonObjects(
      await database.all(`
        SELECT source_fingerprint
        FROM oracle_local_imports
        WHERE table_name = ${sqlString(spec.tableName)}
      `),
    );
    if (readText(metadataRows[0]?.source_fingerprint) === source.fingerprint) {
      emit({
        event: "polk_bulk_table_reused",
        table: spec.tableName,
        file: spec.fileName,
      });
      continue;
    }
    emit({
      event: "polk_bulk_table_import_started",
      table: spec.tableName,
      file: spec.fileName,
      sizeBytes: source.sizeBytes,
    });
    const importPath = await resolveImportSource(source, workDatabase);
    const indexName = `idx_${spec.tableName}_${spec.keyColumn}`;
    try {
      await database.exec("BEGIN TRANSACTION");
      await database.exec(`DROP TABLE IF EXISTS ${spec.tableName}`);
      await database.exec(`
        CREATE TABLE ${spec.tableName} AS
        SELECT ${spec.selectList}
        FROM read_csv_auto(
          ${sqlString(importPath)},
          header = true,
          all_varchar = true,
          strict_mode = false,
          delim = ',',
          quote = '"',
          escape = '"',
          max_line_size = 10000000
        )
      `);
      await database.exec(`
        DELETE FROM ${spec.tableName}
        WHERE ${spec.keyColumn} IS NULL OR trim(${spec.keyColumn}) = ''
      `);
      await database.exec(`
        CREATE INDEX ${indexName}
        ON ${spec.tableName} (${spec.keyColumn})
      `);
      const count = await queryScalarNumber(
        database,
        `SELECT count(*)::DOUBLE AS value FROM ${spec.tableName}`,
      );
      await database.exec(`
        INSERT OR REPLACE INTO oracle_local_imports
          (table_name, source_fingerprint, imported_at, row_count)
        VALUES (
          ${sqlString(spec.tableName)},
          ${sqlString(source.fingerprint)},
          current_timestamp,
          ${count}
        )
      `);
      await database.exec("COMMIT");
      emit({
        event: "polk_bulk_table_import_completed",
        table: spec.tableName,
        rowCount: count,
      });
    } catch (caught) {
      try {
        await database.exec("ROLLBACK");
      } catch {
        // The original import error remains authoritative.
      }
      throw caught;
    }
  }
}

/**
 * Convert unknown DuckDB rows to validated JSON objects.
 *
 * @param {readonly unknown[]} rows DuckDB rows.
 * @returns {JsonObject[]} Object rows.
 */
function asJsonObjects(rows) {
  return rows.map((row) => {
    if (!isJsonObject(row)) throw new Error("DuckDB returned a non-object row");
    return row;
  });
}

/**
 * Run a scalar numeric query using the `value` column.
 *
 * @param {DuckDbConnection} database DuckDB connection.
 * @param {string} sql Scalar query.
 * @returns {Promise<number>} Finite number.
 */
async function queryScalarNumber(database, sql) {
  const rows = asJsonObjects(await database.all(sql));
  const value = Number(rows[0]?.value);
  if (!Number.isFinite(value)) throw new Error(`Invalid scalar result: ${sql}`);
  return value;
}

/**
 * Fail when a new output directory contains artifacts without its checkpoint.
 *
 * @param {string} outputDirectory Output root.
 * @param {boolean} hasCheckpoint Whether a valid checkpoint exists.
 * @returns {Promise<void>} Resolves for a safe output.
 */
async function assertOutputDirectorySafe(outputDirectory, hasCheckpoint) {
  if (hasCheckpoint) return;
  let entries;
  try {
    entries = await readdir(outputDirectory);
  } catch (caught) {
    if (
      caught instanceof Error &&
      "code" in caught &&
      /** @type {NodeJS.ErrnoException} */ (caught).code === "ENOENT"
    ) {
      return;
    }
    throw caught;
  }
  if (entries.length > 0) {
    throw new Error(
      `Output directory is non-empty but has no checkpoint: ${outputDirectory}`,
    );
  }
}

/**
 * Select the next keyset-paginated parcel batch and attach subdivision names.
 *
 * @param {DuckDbConnection} database DuckDB connection.
 * @param {string | null} lastParcelIdentifier Last committed parcel id.
 * @param {number} count Maximum rows.
 * @returns {Promise<JsonObject[]>} Sorted parcel rows.
 */
async function selectParcelBatch(database, lastParcelIdentifier, count) {
  const predicate =
    lastParcelIdentifier === null
      ? ""
      : `WHERE p.parcel_id > ${sqlString(lastParcelIdentifier)}`;
  return asJsonObjects(
    await database.all(`
      SELECT
        p.*,
        subdivision.subdivision_name
      FROM polk_parcels p
      LEFT JOIN (
        SELECT
          subdivision_code,
          min(subdivision_name) AS subdivision_name
        FROM polk_subdivisions
        GROUP BY subdivision_code
      ) subdivision USING (subdivision_code)
      ${predicate}
      ORDER BY p.parcel_id
      LIMIT ${count}
    `),
  );
}

/**
 * Replace the temporary current-batch id table.
 *
 * Parcel values have already passed the alphanumeric normalizer, but are still
 * SQL-quoted defensively.
 *
 * @param {DuckDbConnection} database DuckDB connection.
 * @param {readonly string[]} parcelIdentifiers Batch parcel ids.
 * @returns {Promise<void>} Resolves after the table is populated.
 */
async function setCurrentBatch(database, parcelIdentifiers) {
  if (parcelIdentifiers.length === 0) return;
  const values = parcelIdentifiers
    .map((parcelIdentifier) => `(${sqlString(parcelIdentifier)})`)
    .join(",");
  await database.exec(`
    CREATE OR REPLACE TEMP TABLE polk_current_batch (
      parcel_id VARCHAR PRIMARY KEY
    );
    INSERT INTO polk_current_batch VALUES ${values};
  `);
}

/**
 * Read one child table for only the current batch.
 *
 * @param {DuckDbConnection} database DuckDB connection.
 * @param {string} tableName Trusted table name.
 * @param {string} orderBy Trusted deterministic ordering expression.
 * @returns {Promise<JsonObject[]>} Batch-bounded child rows.
 */
async function selectChildRows(database, tableName, orderBy) {
  return asJsonObjects(
    await database.all(`
      SELECT child.*
      FROM ${tableName} child
      INNER JOIN polk_current_batch batch USING (parcel_id)
      ORDER BY child.parcel_id, ${orderBy}
    `),
  );
}

/**
 * Group child rows by parcel id without retaining rows from previous batches.
 *
 * @param {readonly JsonObject[]} rows Current batch rows.
 * @returns {Map<string, JsonObject[]>} Parcel-to-rows map.
 */
function groupByParcel(rows) {
  /** @type {Map<string, JsonObject[]>} */
  const grouped = new Map();
  for (const row of rows) {
    const parcelIdentifier = readText(row.parcel_id);
    if (parcelIdentifier === null) continue;
    const values = grouped.get(parcelIdentifier) ?? [];
    values.push(row);
    grouped.set(parcelIdentifier, values);
  }
  return grouped;
}

/**
 * Read all source detail for one bounded parcel batch. Queries are sequential
 * to keep peak memory predictable even when a parcel has large history.
 *
 * @param {DuckDbConnection} database DuckDB connection.
 * @param {readonly JsonObject[]} parcels Batch parcel rows.
 * @returns {Promise<Map<string, {sites:JsonObject[],sales:JsonObject[],buildings:JsonObject[],layouts:JsonObject[],lands:JsonObject[],legalDescriptions:JsonObject[],permits:JsonObject[]}>>} Child rows grouped by parcel.
 */
async function readBatchChildren(database, parcels) {
  const parcelIdentifiers = parcels.map((parcel) => {
    const value = readText(parcel.parcel_id);
    if (value === null) throw new Error("Batch parcel has no id");
    return value;
  });
  await setCurrentBatch(database, parcelIdentifiers);
  const sites = groupByParcel(
    await selectChildRows(
      database,
      "polk_sites",
      "coalesce(try_cast(child.building_number AS INTEGER), 999999), coalesce(try_cast(child.line_number AS INTEGER), 999999)",
    ),
  );
  const sales = groupByParcel(
    await selectChildRows(
      database,
      "polk_sales",
      "coalesce(try_strptime(child.sale_date, '%m/%d/%Y'), DATE '1900-01-01') DESC, child.sale_id",
    ),
  );
  const buildings = groupByParcel(
    await selectChildRows(
      database,
      "polk_buildings",
      "coalesce(try_cast(child.building_number AS INTEGER), 999999)",
    ),
  );
  const layouts = groupByParcel(
    await selectChildRows(
      database,
      "polk_layouts",
      "coalesce(try_cast(child.building_number AS INTEGER), 999999), coalesce(try_cast(child.line_number AS INTEGER), 999999)",
    ),
  );
  const lands = groupByParcel(
    await selectChildRows(
      database,
      "polk_lands",
      "coalesce(try_cast(child.line_number AS INTEGER), 999999), child.land_type",
    ),
  );
  const legalDescriptions = groupByParcel(
    await selectChildRows(
      database,
      "polk_legal_descriptions",
      "coalesce(try_cast(child.line_number AS INTEGER), 999999)",
    ),
  );
  const permits = groupByParcel(
    await selectChildRows(
      database,
      "polk_permits",
      "coalesce(try_cast(child.permit_id AS INTEGER), 999999)",
    ),
  );
  const children = new Map();
  for (const parcelIdentifier of parcelIdentifiers) {
    children.set(parcelIdentifier, {
      sites: sites.get(parcelIdentifier) ?? [],
      sales: sales.get(parcelIdentifier) ?? [],
      buildings: buildings.get(parcelIdentifier) ?? [],
      layouts: layouts.get(parcelIdentifier) ?? [],
      lands: lands.get(parcelIdentifier) ?? [],
      legalDescriptions: legalDescriptions.get(parcelIdentifier) ?? [],
      permits: permits.get(parcelIdentifier) ?? [],
    });
  }
  return children;
}

/**
 * Map values with fixed concurrency while preserving input order.
 *
 * @template Input
 * @template Output
 * @param {readonly Input[]} values Input values.
 * @param {number} concurrency Maximum active operations.
 * @param {(value: Input, index: number) => Promise<Output>} mapper Async mapper.
 * @returns {Promise<Output[]>} Results in input order.
 */
async function mapWithConcurrency(values, concurrency, mapper) {
  /** @type {Output[]} */
  const results = new Array(values.length);
  let nextIndex = 0;

  /**
   * @returns {Promise<void>} Completes one worker's share.
   */
  const worker = async () => {
    while (nextIndex < values.length) {
      const index = nextIndex;
      nextIndex += 1;
      const value = values[index];
      if (value !== undefined) results[index] = await mapper(value, index);
    }
  };

  await Promise.all(
    Array.from({ length: Math.min(concurrency, values.length) }, async () =>
      worker(),
    ),
  );
  return results;
}

/**
 * Build, privacy-gate, content-address, and atomically write one property.
 *
 * @param {object} params Build parameters.
 * @param {JsonObject} params.parcel Parcel row.
 * @param {{sites:JsonObject[],sales:JsonObject[],buildings:JsonObject[],layouts:JsonObject[],lands:JsonObject[],legalDescriptions:JsonObject[],permits:JsonObject[]}} params.children Batch child rows.
 * @param {string} params.collectedAt Deterministic source timestamp.
 * @param {string} params.outputDirectory Output root.
 * @returns {Promise<PropertyBuildResult>} Property and query metadata.
 */
async function buildAndWriteProperty(params) {
  const parcelIdentifier = readText(params.parcel.parcel_id);
  if (parcelIdentifier === null) throw new Error("Parcel row has no id");
  const property = buildConsolidatedProperty({
    parcel:
      /** @type {import("./polk-local-appraisal-lib.mjs").PolkParcelSource} */ (
        params.parcel
      ),
    sites:
      /** @type {import("./polk-local-appraisal-lib.mjs").PolkSiteSource[]} */ (
        params.children.sites
      ),
    sales:
      /** @type {import("./polk-local-appraisal-lib.mjs").PolkSaleSource[]} */ (
        params.children.sales
      ),
    buildings:
      /** @type {import("./polk-local-appraisal-lib.mjs").PolkBuildingSource[]} */ (
        params.children.buildings
      ),
    layouts:
      /** @type {import("./polk-local-appraisal-lib.mjs").PolkLayoutSource[]} */ (
        params.children.layouts
      ),
    lands:
      /** @type {import("./polk-local-appraisal-lib.mjs").PolkLandSource[]} */ (
        params.children.lands
      ),
    legalDescriptions:
      /** @type {import("./polk-local-appraisal-lib.mjs").PolkLegalSource[]} */ (
        params.children.legalDescriptions
      ),
    permits:
      /** @type {import("./polk-local-appraisal-lib.mjs").PolkPermitSource[]} */ (
        params.children.permits
      ),
    collectedAt: params.collectedAt,
  });
  const body = Buffer.from(stableJson(property), "utf8");
  const propertyCid = await ipfsHash.of(body);
  const relativeFile = propertyRelativePath(parcelIdentifier);
  await writeFileAtomically(
    path.join(params.outputDirectory, relativeFile),
    body,
  );
  const propertyId = readText(property.parcelId);
  if (propertyId === null) throw new Error("Built property has no property id");
  return {
    parcelIdentifier,
    propertyId,
    propertyCid,
    relativeFile,
    sizeBytes: body.byteLength,
    sha256: createHash("sha256").update(body).digest("hex"),
    queryRow: buildQueryTableRow(property, propertyCid),
    quality: propertyQualityCounters(property),
  };
}

/**
 * Write one query-table Parquet shard through a sibling temp path.
 *
 * @param {string} destination Final shard path.
 * @param {readonly PropertyBuildResult[]} properties Ordered property results.
 * @returns {Promise<{sizeBytes:number,sha256:string}>} Shard digest.
 */
async function writeQueryTableShard(destination, properties) {
  await mkdir(path.dirname(destination), { recursive: true });
  const temporary = `${destination}.${process.pid}.${randomUUID()}.tmp`;
  const writer = await ParquetWriter.openFile(
    buildQueryTableParquetSchema(),
    temporary,
  );
  try {
    for (const property of properties) {
      await writer.appendRow(toParquetRecord(property.queryRow));
    }
  } catch (caught) {
    await writer.close();
    await rm(temporary, { force: true });
    throw caught;
  }
  await writer.close();
  await rename(temporary, destination);
  const body = await readFile(destination);
  return {
    sizeBytes: body.byteLength,
    sha256: createHash("sha256").update(body).digest("hex"),
  };
}

/**
 * Build and atomically commit one shard. The caller advances the checkpoint
 * only after this function has completed both the Parquet and shard manifest.
 *
 * @param {object} params Shard parameters.
 * @param {DuckDbConnection} params.database DuckDB connection.
 * @param {readonly JsonObject[]} params.parcels Batch parcel rows.
 * @param {number} params.shardIndex Shard number.
 * @param {string} params.outputDirectory Output root.
 * @param {string} params.collectedAt Deterministic source timestamp.
 * @returns {Promise<{checkpoint:import("./polk-local-appraisal-lib.mjs").ShardCheckpoint,manifest:ShardManifest}>} Committed shard metadata.
 */
async function buildShard(params) {
  const childrenByParcel = await readBatchChildren(
    params.database,
    params.parcels,
  );
  const properties = await mapWithConcurrency(
    params.parcels,
    PROPERTY_WRITE_CONCURRENCY,
    async (parcel) => {
      const parcelIdentifier = readText(parcel.parcel_id);
      if (parcelIdentifier === null) throw new Error("Parcel row has no id");
      const children = childrenByParcel.get(parcelIdentifier);
      if (children === undefined) {
        throw new Error(`Missing child bundle for ${parcelIdentifier}`);
      }
      return buildAndWriteProperty({
        parcel,
        children,
        collectedAt: params.collectedAt,
        outputDirectory: params.outputDirectory,
      });
    },
  );
  const shardStem = `shard-${String(params.shardIndex).padStart(6, "0")}`;
  const relativeShardPath = path.join(
    "query-table-shards",
    `${shardStem}.parquet`,
  );
  const relativeManifestPath = path.join("manifests", `${shardStem}.json`);
  const shardPath = path.join(params.outputDirectory, relativeShardPath);
  const shardDigest = await writeQueryTableShard(shardPath, properties);
  const propertyBytes = properties.reduce(
    (sum, property) => sum + property.sizeBytes,
    0,
  );
  const first = properties[0];
  const last = properties.at(-1);
  if (first === undefined || last === undefined) {
    throw new Error("Cannot commit an empty shard");
  }
  const manifest = {
    schemaVersion: POLK_EXPORT_SCHEMA_VERSION,
    shardIndex: params.shardIndex,
    fromParcel: first.parcelIdentifier,
    toParcel: last.parcelIdentifier,
    rowCount: properties.length,
    propertyBytes,
    quality: sumQualityCounters(properties.map((property) => property.quality)),
    entries: properties.map((property) => ({
      parcelIdentifier: property.parcelIdentifier,
      propertyId: property.propertyId,
      cid: property.propertyCid,
      file: property.relativeFile,
      fileSizeBytes: property.sizeBytes,
      sha256: property.sha256,
    })),
    queryTableShard: {
      file: relativeShardPath,
      rowCount: properties.length,
      sizeBytes: shardDigest.sizeBytes,
      sha256: shardDigest.sha256,
    },
  };
  await writeJsonAtomically(
    path.join(params.outputDirectory, relativeManifestPath),
    manifest,
  );
  return {
    checkpoint: {
      shardIndex: params.shardIndex,
      file: relativeShardPath,
      manifest: relativeManifestPath,
      rowCount: properties.length,
      fromParcel: first.parcelIdentifier,
      toParcel: last.parcelIdentifier,
      propertyBytes,
    },
    manifest,
  };
}

/**
 * Merge committed query-table shards into one final Parquet using DuckDB's
 * streaming Parquet reader/writer and atomically rename the completed result.
 *
 * @param {DuckDbConnection} database DuckDB connection.
 * @param {string} outputDirectory Output root.
 * @returns {Promise<string>} Final Parquet path.
 */
async function mergeQueryTableShards(database, outputDirectory) {
  const shardGlob = path.join(
    outputDirectory,
    "query-table-shards",
    "*.parquet",
  );
  const destination = path.join(outputDirectory, "query-table.parquet");
  const temporary = `${destination}.${process.pid}.${randomUUID()}.tmp`;
  await rm(temporary, { force: true });
  try {
    await database.exec(`
      COPY (
        SELECT *
        FROM read_parquet(${sqlString(shardGlob)})
        ORDER BY parcel_identifier
      )
      TO ${sqlString(temporary)}
      (FORMAT PARQUET, COMPRESSION ZSTD)
    `);
    await rename(temporary, destination);
  } catch (caught) {
    await rm(temporary, { force: true });
    throw caught;
  }
  return destination;
}

/**
 * Validate the merged query-table reconciliation and privacy placeholders.
 *
 * @param {DuckDbConnection} database DuckDB connection.
 * @param {string} queryTablePath Final Parquet path.
 * @param {number} expectedRows Expected committed rows.
 * @returns {Promise<JsonObject>} Validation result.
 */
async function validateMergedQueryTable(
  database,
  queryTablePath,
  expectedRows,
) {
  const rows = asJsonObjects(
    await database.all(`
      SELECT
        count(*)::DOUBLE AS row_count,
        count(DISTINCT parcel_identifier)::DOUBLE AS distinct_parcels,
        count(DISTINCT property_id)::DOUBLE AS distinct_property_ids,
        count(*) FILTER (WHERE property_cid IS NULL)::DOUBLE AS null_cids,
        count(*) FILTER (
          WHERE owner_name IS NOT NULL
             OR owners_text IS NOT NULL
             OR owner_count IS NOT NULL
             OR owner_occupied IS NOT NULL
        )::DOUBLE AS owner_field_violations
      FROM read_parquet(${sqlString(queryTablePath)})
    `),
  );
  const row = rows[0] ?? {};
  const validation = {
    rowCount: Number(row.row_count),
    distinctParcels: Number(row.distinct_parcels),
    distinctPropertyIds: Number(row.distinct_property_ids),
    nullCids: Number(row.null_cids),
    ownerFieldViolations: Number(row.owner_field_violations),
  };
  if (
    validation.rowCount !== expectedRows ||
    validation.distinctParcels !== expectedRows ||
    validation.distinctPropertyIds !== expectedRows ||
    validation.nullCids !== 0 ||
    validation.ownerFieldViolations !== 0
  ) {
    throw new Error(
      `Merged query-table gate failed: ${JSON.stringify(validation)}`,
    );
  }
  return validation;
}

/**
 * Load committed shard manifests and aggregate quality counters.
 *
 * @param {string} outputDirectory Output root.
 * @param {readonly import("./polk-local-appraisal-lib.mjs").ShardCheckpoint[]} shards Committed shards.
 * @returns {Promise<{manifests:ShardManifest[],quality:Record<string,number>}>} Shard manifests and quality totals.
 */
async function loadCommittedQuality(outputDirectory, shards) {
  /** @type {ShardManifest[]} */
  const manifests = [];
  for (const shard of shards) {
    const parsed = /** @type {unknown} */ (
      JSON.parse(
        await readFile(path.join(outputDirectory, shard.manifest), "utf8"),
      )
    );
    if (
      !isJsonObject(parsed) ||
      typeof parsed.rowCount !== "number" ||
      !isJsonObject(parsed.quality)
    ) {
      throw new Error(`Invalid shard manifest ${shard.manifest}`);
    }
    manifests.push(/** @type {ShardManifest} */ (parsed));
  }
  return {
    manifests,
    quality: sumQualityCounters(
      manifests.map(
        (manifest) => /** @type {Record<string, number>} */ (manifest.quality),
      ),
    ),
  };
}

/**
 * Convert present-count quality counters into percentages.
 *
 * @param {Record<string, number>} quality Quality totals.
 * @returns {Record<string, number>} Rounded coverage percentages.
 */
function buildCoveragePercentages(quality) {
  const properties = quality.properties ?? 0;
  const result = {};
  for (const key of [
    "withSiteAddress",
    "withPostalCode",
    "withCoordinates",
    "withPropertyType",
    "withUsageType",
    "withBuiltYear",
    "withLivableArea",
    "withLegalDescription",
    "withSales",
    "withBuildings",
    "withLayouts",
    "withLots",
    "withPermits",
  ]) {
    result[key] =
      properties === 0
        ? 0
        : Number((((quality[key] ?? 0) / properties) * 100).toFixed(2));
  }
  return result;
}

/**
 * Count newline-delimited physical data lines without loading the source file
 * into memory. This intentionally differs from logical CSV rows when quoted
 * values contain embedded newlines.
 *
 * @param {string} filePath Source file path.
 * @returns {Promise<number>} Physical data lines excluding the header.
 */
async function countPhysicalDataLines(filePath) {
  let newlineCount = 0;
  for await (const chunk of createReadStream(filePath)) {
    const bytes = /** @type {Buffer} */ (chunk);
    for (const byte of bytes) {
      if (byte === 10) newlineCount += 1;
    }
  }
  return Math.max(0, newlineCount - 1);
}

/**
 * Finalize root coverage, index, and manifest artifacts.
 *
 * @param {object} params Finalization parameters.
 * @param {DuckDbConnection} params.database DuckDB connection.
 * @param {PolkCliOptions} params.options CLI options.
 * @param {SourceSnapshot} params.snapshot Source snapshot.
 * @param {import("./polk-local-appraisal-lib.mjs").PolkCheckpoint} params.checkpoint Current checkpoint.
 * @param {number} params.logicalSourceParcels DuckDB logical parcel count.
 * @param {number} params.physicalSourceDataLines Physical source data lines.
 * @param {number} params.elapsedMs Current invocation elapsed time.
 * @param {boolean} params.resumedFromCheckpoint Whether this invocation resumed.
 * @returns {Promise<JsonObject>} Final summary.
 */
async function finalizeOutput(params) {
  if (params.checkpoint.shards.length === 0) {
    throw new Error("Cannot finalize an export with no committed shards");
  }
  const queryTablePath = await mergeQueryTableShards(
    params.database,
    params.options.outputDirectory,
  );
  const validation = await validateMergedQueryTable(
    params.database,
    queryTablePath,
    params.checkpoint.processedCount,
  );
  const { manifests, quality } = await loadCommittedQuality(
    params.options.outputDirectory,
    params.checkpoint.shards,
  );
  const queryTableBody = await readFile(queryTablePath);
  const queryTable = {
    file: path.basename(queryTablePath),
    rowCount: validation.rowCount,
    sizeBytes: queryTableBody.byteLength,
    sha256: createHash("sha256").update(queryTableBody).digest("hex"),
  };
  const coverage = {
    schemaVersion: POLK_EXPORT_SCHEMA_VERSION,
    county: "polk",
    sourceSystem: "Polk County Property Appraiser official bulk CAMA text",
    bulkFirst: true,
    browserRequests: 0,
    logicalSourceParcelCount: params.logicalSourceParcels,
    physicalParcelDataLines: params.physicalSourceDataLines,
    physicalVsLogicalExplanation:
      "Physical lines exceed logical CSV records because quoted source values contain embedded newlines.",
    selectedPropertyCount: params.checkpoint.processedCount,
    requestedLimit: params.options.limit,
    completeForRequestedScope: true,
    qualityCounts: quality,
    qualityPercentages: buildCoveragePercentages(quality),
    knownCoverageGaps: [
      "Latitude and longitude are absent from the downloaded Polk bulk CAMA text files.",
      "Owner names, ownerships, grantors, grantees, and mailing addresses are intentionally excluded.",
      "Sunbiz, BBB, AVM, flood, HOA, utility, media, and deed enrichment are outside this local appraisal export.",
      "Permit descriptions containing email, phone, or SSN-like patterns are suppressed rather than partially redacted.",
    ],
    privacy: {
      ownerRowsPublished: 0,
      grantorValuesPublished: 0,
      granteeValuesPublished: 0,
      mailingAddressesPublished: 0,
      queryTableOwnerFieldViolations: validation.ownerFieldViolations,
      passed: validation.ownerFieldViolations === 0,
    },
    childRows: {
      sales: quality.saleRows ?? 0,
      buildings: quality.buildingRows ?? 0,
      layouts: quality.layoutRows ?? 0,
      lots: quality.lotRows ?? 0,
      permits: quality.permitRows ?? 0,
    },
  };
  await writeJsonAtomically(
    path.join(params.options.outputDirectory, "coverage.json"),
    coverage,
  );
  const index = {
    schemaVersion: POLK_EXPORT_SCHEMA_VERSION,
    county: "polk",
    propertyCount: params.checkpoint.processedCount,
    shardSize: params.options.batchSize,
    shards: params.checkpoint.shards,
    queryTable,
  };
  await writeJsonAtomically(
    path.join(params.options.outputDirectory, "index.json"),
    index,
  );
  const manifest = {
    schemaVersion: POLK_EXPORT_SCHEMA_VERSION,
    county: "polk",
    jurisdiction: "Polk County, Florida",
    sourceFingerprint: params.snapshot.fingerprint,
    sourceCollectedAt: params.snapshot.collectedAt,
    sourceFiles: params.snapshot.files.map((file) => ({
      fileName: file.fileName,
      sizeBytes: file.sizeBytes,
      modifiedTimeMs: file.modifiedTimeMs,
      fingerprint: file.fingerprint,
    })),
    run: {
      startedAt: params.checkpoint.startedAt,
      completedAt: new Date().toISOString(),
      elapsedMs: params.elapsedMs,
      batchSize: params.options.batchSize,
      limit: params.options.limit,
      resumed: params.resumedFromCheckpoint,
    },
    output: {
      propertyCount: params.checkpoint.processedCount,
      propertyBytes: params.checkpoint.shards.reduce(
        (sum, shard) => sum + shard.propertyBytes,
        0,
      ),
      shardCount: params.checkpoint.shards.length,
      queryTable,
      validation,
    },
    publicDataPolicy: {
      included: [
        "parcel values and classifications",
        "situs addresses",
        "sales without parties",
        "buildings and building SAR layouts",
        "land and lots",
        "legal descriptions",
        "permits",
      ],
      excluded: [
        "owners and ownership records",
        "grantors and grantees",
        "mailing addresses",
      ],
    },
  };
  await writeJsonAtomically(
    path.join(params.options.outputDirectory, "manifest.json"),
    manifest,
  );
  return {
    event: "polk_local_appraisal_completed",
    outputDirectory: params.options.outputDirectory,
    processedCount: params.checkpoint.processedCount,
    shardCount: params.checkpoint.shards.length,
    queryTable,
    validation,
    qualityCounts: quality,
    qualityPercentages: buildCoveragePercentages(quality),
    propertyBytes: manifest.output.propertyBytes,
    elapsedMs: params.elapsedMs,
  };
}

/**
 * Emit one machine-readable progress event.
 *
 * @param {JsonObject} event Event payload.
 * @returns {void}
 */
function emit(event) {
  process.stdout.write(`${JSON.stringify(event)}\n`);
}

/**
 * Execute the resumable local Polk build.
 *
 * @param {readonly string[]} argv CLI arguments after the script path.
 * @returns {Promise<JsonObject>} Final build summary.
 */
export async function runPolkLocalAppraisal(argv) {
  const invocationStartedAt = Date.now();
  const options = parsePolkCliOptions(argv);
  const snapshot = await snapshotSourceFiles(options.inputDirectory);
  if (options.restart) {
    await rm(options.outputDirectory, { recursive: true, force: true });
  }
  const checkpointPath = path.join(
    options.outputDirectory,
    ".state",
    "checkpoint.json",
  );
  let checkpoint = await readCheckpoint(checkpointPath);
  const resumedFromCheckpoint = checkpoint !== null;
  await assertOutputDirectorySafe(options.outputDirectory, checkpoint !== null);
  if (checkpoint === null) {
    checkpoint = createCheckpoint({
      sourceFingerprint: snapshot.fingerprint,
      inputDirectory: options.inputDirectory,
      batchSize: options.batchSize,
      limit: options.limit,
      startedAt: new Date().toISOString(),
    });
    await writeCheckpoint(checkpointPath, checkpoint);
  } else {
    try {
      assertCheckpointCompatible(checkpoint, {
        sourceFingerprint: snapshot.fingerprint,
        inputDirectory: options.inputDirectory,
        batchSize: options.batchSize,
        limit: options.limit,
      });
    } catch (caught) {
      if (checkpoint.processedCount !== 0 || checkpoint.shards.length !== 0) {
        throw caught;
      }
      checkpoint = createCheckpoint({
        sourceFingerprint: snapshot.fingerprint,
        inputDirectory: options.inputDirectory,
        batchSize: options.batchSize,
        limit: options.limit,
        startedAt: checkpoint.startedAt,
      });
      await writeCheckpoint(checkpointPath, checkpoint);
    }
    emit({
      event: "polk_local_appraisal_resumed",
      processedCount: checkpoint.processedCount,
      nextShardIndex: checkpoint.nextShardIndex,
      lastParcelIdentifier: checkpoint.lastParcelIdentifier,
    });
  }

  const database = await openDuckDb(options.workDatabase);
  try {
    await ensureWorkDatabase(database, snapshot, options.workDatabase);
    const logicalSourceParcels = await queryScalarNumber(
      database,
      "SELECT count(DISTINCT parcel_id)::DOUBLE AS value FROM polk_parcels",
    );
    const parcelSource = snapshot.files.find(
      (file) => file.fileName === "ftp_parcel.txt",
    );
    if (parcelSource === undefined) {
      throw new Error("Parcel source snapshot is missing");
    }
    const physicalSourceDataLines = await countPhysicalDataLines(
      parcelSource.absolutePath,
    );

    if (!checkpoint.complete) {
      while (
        options.limit === null ||
        checkpoint.processedCount < options.limit
      ) {
        const remaining =
          options.limit === null
            ? options.batchSize
            : Math.min(
                options.batchSize,
                options.limit - checkpoint.processedCount,
              );
        const parcels = await selectParcelBatch(
          database,
          checkpoint.lastParcelIdentifier,
          remaining,
        );
        if (parcels.length === 0) break;
        const shardStartedAt = Date.now();
        const { checkpoint: shard } = await buildShard({
          database,
          parcels,
          shardIndex: checkpoint.nextShardIndex,
          outputDirectory: options.outputDirectory,
          collectedAt: snapshot.collectedAt,
        });
        checkpoint = {
          ...checkpoint,
          processedCount: checkpoint.processedCount + shard.rowCount,
          lastParcelIdentifier: shard.toParcel,
          nextShardIndex: checkpoint.nextShardIndex + 1,
          shards: [...checkpoint.shards, shard],
          complete: false,
        };
        await writeCheckpoint(checkpointPath, checkpoint);
        emit({
          event: "polk_local_appraisal_shard_committed",
          shardIndex: shard.shardIndex,
          rowCount: shard.rowCount,
          processedCount: checkpoint.processedCount,
          lastParcelIdentifier: checkpoint.lastParcelIdentifier,
          elapsedMs: Date.now() - shardStartedAt,
        });
      }
    }

    const elapsedMs = Date.now() - invocationStartedAt;
    const summary = await finalizeOutput({
      database,
      options,
      snapshot,
      checkpoint,
      logicalSourceParcels,
      physicalSourceDataLines,
      elapsedMs,
      resumedFromCheckpoint,
    });
    checkpoint = { ...checkpoint, complete: true };
    await writeCheckpoint(checkpointPath, checkpoint);
    emit(summary);
    return summary;
  } finally {
    await database.close();
  }
}

const invokedPath =
  process.argv[1] === undefined ? null : path.resolve(process.argv[1]);
if (invokedPath !== null && fileURLToPath(import.meta.url) === invokedPath) {
  runPolkLocalAppraisal(process.argv.slice(2)).catch((caught) => {
    const message =
      caught instanceof Error
        ? (caught.stack ?? caught.message)
        : String(caught);
    process.stderr.write(
      `${JSON.stringify({
        event: "polk_local_appraisal_failed",
        error: message,
      })}\n`,
    );
    process.exitCode = 1;
  });
}
