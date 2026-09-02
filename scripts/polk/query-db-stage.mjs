import { createHash } from "node:crypto";
import { once } from "node:events";
import { createWriteStream } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";

import {
  normalizeDate,
  normalizeParcelIdentifier,
  normalizePostalCode,
  readInteger,
  readNumber,
  readText,
} from "../polk-local-appraisal-lib.mjs";

const require = createRequire(import.meta.url);
const duckdb = require("duckdb");

export const POLK_APPRAISAL_SOURCE_SYSTEM = "polk_appraiser";
export const POLK_PERMIT_SOURCE_SYSTEM = "polk_permits";
export const POLK_QUERY_DB_STAGE_SCHEMA_VERSION =
  "oracle-node.polk-query-db-stage.v1";
export const BULK_STAGE_HEADER =
  "row_index,table_name,source_system,source_record_key,source_record_hash,source_artifact_uri,values_json,references_json\n";

/**
 * @typedef {"appraisal" | "permits"} PolkQueryDbTrack
 */

/**
 * @typedef {object} PolkQueryDbStageOptions
 * @property {PolkQueryDbTrack} track Local dataset track to stage.
 * @property {string} workDatabase Completed Polk DuckDB cache.
 * @property {string} output Generic query-db bulk-stage CSV destination.
 * @property {string} manifest Stage receipt destination.
 * @property {number | null} limit Optional deterministic pilot cap.
 */

/**
 * @typedef {object} PolkAppraisalStageRecord
 * @property {unknown} parcelIdentifier
 * @property {unknown} propertyType
 * @property {unknown} propertyTypeDetail
 * @property {unknown} zoning
 * @property {unknown} subdivision
 * @property {unknown} legalDescription
 * @property {unknown} builtYear
 * @property {unknown} effectiveYear
 * @property {unknown} livingArea
 * @property {unknown} totalArea
 * @property {unknown} numberOfUnits
 * @property {unknown} streetPrefix
 * @property {unknown} streetNumber
 * @property {unknown} streetNumberSuffix
 * @property {unknown} streetName
 * @property {unknown} streetSuffix
 * @property {unknown} streetPostDirectional
 * @property {unknown} unitIdentifier
 * @property {unknown} cityName
 * @property {unknown} postalCode
 */

/**
 * @typedef {object} PolkPermitStageRecord
 * @property {unknown} parcelIdentifier
 * @property {unknown} permitIdentifier
 * @property {unknown} permitNumber
 * @property {unknown} agencyName
 * @property {unknown} status
 * @property {unknown} statusDescription
 * @property {unknown} description
 * @property {unknown} permitType
 * @property {unknown} issueDate
 * @property {unknown} finalDate
 * @property {unknown} estimatedValue
 * @property {unknown} certificateOfOccupancyDate
 */

/**
 * @typedef {object} PreparedRow
 * @property {string} tableName Query DB logical table.
 * @property {Record<string, unknown>} values Typed target values.
 * @property {Record<string, string> | undefined} [references] Source-key references.
 */

/**
 * Parse the local Polk query-db stage CLI.
 *
 * @param {readonly string[]} argv Command arguments excluding node and script.
 * @returns {PolkQueryDbStageOptions} Validated stage options.
 */
export function parsePolkQueryDbStageOptions(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      track: { type: "string" },
      "work-db": {
        type: "string",
        default: "tmp/polk/bulk/extracted/polk-appraisal.duckdb",
      },
      output: { type: "string" },
      manifest: { type: "string" },
      limit: { type: "string" },
    },
    strict: true,
  });
  if (values.track !== "appraisal" && values.track !== "permits") {
    throw new Error("Polk query-db stage track must be appraisal or permits");
  }
  const defaultOutput = `tmp/polk/neon/${values.track}-stage/${values.track}.csv`;
  const output = values.output ?? defaultOutput;
  const limit =
    values.limit === undefined ? null : Number.parseInt(values.limit, 10);
  if (
    limit !== null &&
    (!Number.isSafeInteger(limit) ||
      limit < 1 ||
      String(limit) !== values.limit)
  ) {
    throw new Error("Polk query-db stage limit must be a positive integer");
  }
  return {
    track: values.track,
    workDatabase: values["work-db"],
    output,
    manifest: values.manifest ?? `${output}.manifest.json`,
    limit,
  };
}

/**
 * Build the deterministic appraisal extraction query.
 *
 * One row is emitted per Polk parcel. Principal site and building rows are
 * selected deterministically so reruns produce identical source records.
 *
 * @param {number | null} limit Optional pilot cap.
 * @returns {string} Read-only DuckDB SQL.
 */
export function buildPolkAppraisalStageSql(limit) {
  assertLimit(limit);
  return `
    WITH principal_site AS (
      SELECT
        *,
        row_number() OVER (
          PARTITION BY parcel_id
          ORDER BY
            coalesce(try_cast(building_number AS BIGINT), 999999),
            coalesce(try_cast(line_number AS BIGINT), 999999)
        ) AS site_rank
      FROM polk_sites
    ),
    principal_building AS (
      SELECT
        *,
        row_number() OVER (
          PARTITION BY parcel_id
          ORDER BY
            coalesce(try_cast(building_number AS BIGINT), 999999)
        ) AS building_rank
      FROM polk_buildings
    ),
    legal AS (
      SELECT
        parcel_id,
        string_agg(trim(description), ' ' ORDER BY try_cast(line_number AS BIGINT))
          AS legal_description
      FROM polk_legal_descriptions
      WHERE description IS NOT NULL AND trim(description) <> ''
      GROUP BY parcel_id
    )
    SELECT
      regexp_replace(upper(trim(p.parcel_id)), '[^A-Z0-9]', '', 'g')
        AS "parcelIdentifier",
      p.property_type AS "propertyType",
      p.property_type_detail AS "propertyTypeDetail",
      p.neighborhood_code AS "zoning",
      p.subdivision_code AS "subdivision",
      legal.legal_description AS "legalDescription",
      principal_building.built_year AS "builtYear",
      principal_building.effective_year AS "effectiveYear",
      principal_building.living_area AS "livingArea",
      principal_building.total_under_roof AS "totalArea",
      principal_building.units AS "numberOfUnits",
      principal_site.street_prefix AS "streetPrefix",
      principal_site.street_number AS "streetNumber",
      principal_site.street_number_suffix AS "streetNumberSuffix",
      principal_site.street AS "streetName",
      principal_site.street_suffix AS "streetSuffix",
      principal_site.street_suffix_direction AS "streetPostDirectional",
      principal_site.unit AS "unitIdentifier",
      principal_site.city AS "cityName",
      principal_site.postal_code AS "postalCode"
    FROM polk_parcels p
    LEFT JOIN principal_site
      ON principal_site.parcel_id = p.parcel_id
      AND principal_site.site_rank = 1
    LEFT JOIN principal_building
      ON principal_building.parcel_id = p.parcel_id
      AND principal_building.building_rank = 1
    LEFT JOIN legal ON legal.parcel_id = p.parcel_id
    WHERE p.parcel_id IS NOT NULL
      AND regexp_replace(upper(trim(p.parcel_id)), '[^A-Z0-9]', '', 'g') <> ''
    ORDER BY "parcelIdentifier"
    ${limit === null ? "" : `LIMIT ${limit}`}
  `;
}

/**
 * Build the deterministic official bulk-permit extraction query.
 *
 * @param {number | null} limit Optional pilot cap.
 * @returns {string} Read-only DuckDB SQL.
 */
export function buildPolkPermitStageSql(limit) {
  assertLimit(limit);
  return `
    SELECT
      nullif(regexp_replace(upper(trim(parcel_id)), '[^A-Z0-9]', '', 'g'), '')
        AS "parcelIdentifier",
      permit_id AS "permitIdentifier",
      permit_number AS "permitNumber",
      agency_name AS "agencyName",
      status AS "status",
      status_description AS "statusDescription",
      description AS "description",
      permit_type AS "permitType",
      issue_date AS "issueDate",
      final_date AS "finalDate",
      estimated_value AS "estimatedValue",
      certificate_of_occupancy_date AS "certificateOfOccupancyDate"
    FROM polk_permits
    WHERE coalesce(trim(permit_number), trim(permit_id), '') <> ''
    ORDER BY
      coalesce(trim(agency_name), ''),
      coalesce(trim(permit_number), ''),
      coalesce(trim(permit_id), ''),
      coalesce(trim(parcel_id), '')
    ${limit === null ? "" : `LIMIT ${limit}`}
  `;
}

/**
 * Convert one local appraisal row into query-db logical rows.
 *
 * @param {PolkAppraisalStageRecord} record DuckDB appraisal row.
 * @returns {PreparedRow[]} Parcel, property, and optional situs address rows.
 */
export function createPolkAppraisalPreparedRows(record) {
  const parcelIdentifier = normalizeParcelIdentifier(record.parcelIdentifier);
  if (parcelIdentifier === null) return [];
  const parcelSourceKey = appraisalSourceKey(
    parcelIdentifier,
    "parcel",
    "property_seed",
  );
  const propertySourceKey = appraisalSourceKey(
    parcelIdentifier,
    "property",
    "property",
  );
  const addressSourceKey = appraisalSourceKey(
    parcelIdentifier,
    "address",
    "site",
  );
  const sourceArtifactUri = `file://polk-appraisal.duckdb#polk_parcels/${parcelIdentifier}`;
  const sourcePayload = compactObject({
    parcel_identifier: parcelIdentifier,
    property_type: readText(record.propertyType),
    property_type_detail: readText(record.propertyTypeDetail),
    zoning: readText(record.zoning),
    subdivision: readText(record.subdivision),
    legal_description: readText(record.legalDescription),
    built_year: readInteger(record.builtYear),
    effective_year: readInteger(record.effectiveYear),
    living_area: readNumber(record.livingArea),
    total_area: readNumber(record.totalArea),
    number_of_units: readInteger(record.numberOfUnits),
  });
  const parcelValues = withSourceMetadata({
    sourceSystem: POLK_APPRAISAL_SOURCE_SYSTEM,
    sourceRecordKey: parcelSourceKey,
    sourceArtifactUri,
    sourcePayload,
    values: {
      request_identifier: parcelIdentifier,
      parcel_identifier: parcelIdentifier,
      county_name: "Polk",
      state_code: "FL",
      jurisdiction_key: POLK_APPRAISAL_SOURCE_SYSTEM,
    },
  });
  const address = buildAppraisalAddress(record);
  const rows = [
    {
      tableName: "parcels",
      values: parcelValues,
    },
  ];
  if (address !== null) {
    rows.push({
      tableName: "addresses",
      values: withSourceMetadata({
        sourceSystem: POLK_APPRAISAL_SOURCE_SYSTEM,
        sourceRecordKey: addressSourceKey,
        sourceArtifactUri,
        sourcePayload: address.sourcePayload,
        values: address.values,
      }),
    });
  }
  rows.push({
    tableName: "properties",
    references: compactObject({
      parcelSourceRecordKey: parcelSourceKey,
      ...(address === null ? {} : { addressSourceRecordKey: addressSourceKey }),
    }),
    values: withSourceMetadata({
      sourceSystem: POLK_APPRAISAL_SOURCE_SYSTEM,
      sourceRecordKey: propertySourceKey,
      sourceArtifactUri,
      sourcePayload,
      values: {
        request_identifier: parcelIdentifier,
        parcel_identifier: parcelIdentifier,
        property_type: readText(record.propertyType),
        property_usage_type: readText(record.propertyTypeDetail),
        property_legal_description_text: readText(record.legalDescription),
        property_structure_built_year: readInteger(record.builtYear),
        property_effective_built_year: readInteger(record.effectiveYear),
        livable_floor_area: numberAsText(record.livingArea),
        total_area: numberAsText(record.totalArea),
        number_of_units: readInteger(record.numberOfUnits),
        subdivision: readText(record.subdivision),
        zoning: readText(record.zoning),
      },
    }),
  });
  return rows;
}

/**
 * Convert one official Polk permit row into a query-db improvement row.
 *
 * @param {PolkPermitStageRecord} record DuckDB permit row.
 * @returns {PreparedRow[]} Zero or one property-improvement row.
 */
export function createPolkPermitPreparedRows(record) {
  const permitNumber =
    readText(record.permitNumber) ?? readText(record.permitIdentifier);
  if (permitNumber === null) return [];
  const agency = readText(record.agencyName) ?? "POLK";
  const permitIdentifier = readText(record.permitIdentifier) ?? permitNumber;
  const parcelIdentifier = normalizeParcelIdentifier(record.parcelIdentifier);
  const sourceRecordKey = [
    POLK_PERMIT_SOURCE_SYSTEM,
    "permit",
    agency.toUpperCase(),
    permitIdentifier,
    parcelIdentifier ?? "NO_PARCEL",
  ].join(":");
  const sourcePayload = compactObject({
    permit_identifier: permitIdentifier,
    permit_number: permitNumber,
    agency_name: agency,
    parcel_identifier: parcelIdentifier,
    status: readText(record.status),
    status_description: readText(record.statusDescription),
    description: readText(record.description),
    permit_type: readText(record.permitType),
    issue_date: normalizeDate(record.issueDate),
    final_date: normalizeDate(record.finalDate),
    estimated_value: readNumber(record.estimatedValue),
    certificate_of_occupancy_date: normalizeDate(
      record.certificateOfOccupancyDate,
    ),
  });
  const references =
    parcelIdentifier === null
      ? undefined
      : {
          parcelSourceRecordKey: appraisalSourceKey(
            parcelIdentifier,
            "parcel",
            "property_seed",
          ),
          propertySourceRecordKey: appraisalSourceKey(
            parcelIdentifier,
            "property",
            "property",
          ),
        };
  return [
    {
      tableName: "property_improvements",
      references,
      values: withSourceMetadata({
        sourceSystem: POLK_PERMIT_SOURCE_SYSTEM,
        sourceRecordKey,
        sourceArtifactUri: `file://polk-appraisal.duckdb#polk_permits/${encodeURIComponent(sourceRecordKey)}`,
        sourcePayload,
        values: {
          request_identifier: sourceRecordKey,
          permit_number: permitNumber,
          improvement_type: readText(record.permitType),
          improvement_status: readText(record.status),
          record_type: readText(record.permitType),
          source_status: readText(record.status),
          record_status: readText(record.status),
          source: agency,
          permit_issue_date: normalizeDate(record.issueDate),
          parcel_identifier: parcelIdentifier,
          project_description:
            readText(record.description) ?? readText(record.statusDescription),
          estimated_job_value: readNumber(record.estimatedValue),
          more_details: {
            agency_name: agency,
            final_date: normalizeDate(record.finalDate),
            certificate_of_occupancy_date: normalizeDate(
              record.certificateOfOccupancyDate,
            ),
          },
        },
      }),
    },
  ];
}

/**
 * Serialize one generic query-db stage row.
 *
 * @param {{rowIndex:number,row:PreparedRow}} params Indexed prepared row.
 * @returns {string} Newline-terminated PostgreSQL COPY-compatible CSV row.
 */
export function serializePolkBulkStageRow(params) {
  const sourceSystem = readText(params.row.values.source_system);
  const sourceRecordKey = readText(params.row.values.source_record_key);
  if (sourceSystem === null || sourceRecordKey === null) {
    throw new Error(
      `Prepared row for ${params.row.tableName} is missing source metadata`,
    );
  }
  return [
    params.rowIndex,
    params.row.tableName,
    sourceSystem,
    sourceRecordKey,
    readText(params.row.values.source_record_hash),
    readText(params.row.values.source_artifact_uri),
    stableJsonStringify(params.row.values),
    stableJsonStringify(params.row.references ?? {}),
  ]
    .map((value) => serializeCsvField(value))
    .join(",")
    .concat("\n");
}

/**
 * Stream one local track into query-db's generic bulk-stage format.
 *
 * @param {PolkQueryDbStageOptions} options Validated stage options.
 * @returns {Promise<Record<string, unknown>>} Persisted stage receipt.
 */
export async function writePolkQueryDbStage(options) {
  const absoluteDatabase = path.resolve(options.workDatabase);
  const absoluteOutput = path.resolve(options.output);
  const absoluteManifest = path.resolve(options.manifest);
  await mkdir(path.dirname(absoluteOutput), { recursive: true });
  await mkdir(path.dirname(absoluteManifest), { recursive: true });
  const database = new duckdb.Database(absoluteDatabase, {
    access_mode: "READ_ONLY",
  });
  const connection = database.connect();
  const output = createWriteStream(absoluteOutput, {
    encoding: "utf8",
    flags: "w",
  });
  let sourceRecordCount = 0;
  let stagedRowCount = 0;
  /** @type {Record<string, number>} */
  const rowCounts = {};
  await writeWithBackpressure(output, BULK_STAGE_HEADER);
  try {
    const sql =
      options.track === "appraisal"
        ? buildPolkAppraisalStageSql(options.limit)
        : buildPolkPermitStageSql(options.limit);
    const sourceRows = connection.stream(sql);
    for await (const sourceRow of sourceRows) {
      sourceRecordCount += 1;
      const rows =
        options.track === "appraisal"
          ? createPolkAppraisalPreparedRows(sourceRow)
          : createPolkPermitPreparedRows(sourceRow);
      for (const row of rows) {
        stagedRowCount += 1;
        rowCounts[row.tableName] = (rowCounts[row.tableName] ?? 0) + 1;
        await writeWithBackpressure(
          output,
          serializePolkBulkStageRow({
            rowIndex: stagedRowCount,
            row,
          }),
        );
      }
    }
  } finally {
    await closeOutput(output);
    await closeDuckDb(connection, database);
  }
  const receipt = {
    schemaVersion: POLK_QUERY_DB_STAGE_SCHEMA_VERSION,
    generatedAt: new Date().toISOString(),
    county: "polk",
    track: options.track,
    workDatabase: absoluteDatabase,
    output: absoluteOutput,
    requestedLimit: options.limit,
    sourceRecordCount,
    stagedRowCount,
    rowCounts,
    complete: sourceRecordCount > 0 && stagedRowCount > 0,
  };
  await writeFile(
    absoluteManifest,
    `${JSON.stringify(receipt, null, 2)}\n`,
    "utf8",
  );
  return receipt;
}

/**
 * Validate a positive optional cap.
 *
 * @param {number | null} limit Optional cap.
 * @returns {void}
 */
function assertLimit(limit) {
  if (limit !== null && (!Number.isSafeInteger(limit) || limit < 1)) {
    throw new Error("Polk query-db stage limit must be a positive integer");
  }
}

/**
 * Build one source key compatible with query-db's appraisal mapper.
 *
 * @param {string} parcelIdentifier Normalized parcel identifier.
 * @param {string} entity Entity segment.
 * @param {string} record Record segment.
 * @returns {string} Deterministic source key.
 */
function appraisalSourceKey(parcelIdentifier, entity, record) {
  return `${POLK_APPRAISAL_SOURCE_SYSTEM}:${parcelIdentifier}:${entity}:${record}`;
}

/**
 * Build a query-facing Polk situs address.
 *
 * @param {PolkAppraisalStageRecord} record Appraisal row.
 * @returns {{values:Record<string,unknown>,sourcePayload:Record<string,unknown>} | null} Address row.
 */
function buildAppraisalAddress(record) {
  const streetParts = [
    readText(record.streetPrefix),
    readText(record.streetNumber),
    readText(record.streetNumberSuffix),
    readText(record.streetName),
    readText(record.streetSuffix),
    readText(record.streetPostDirectional),
  ].filter((part) => part !== null);
  const unitIdentifier = readText(record.unitIdentifier);
  const street =
    streetParts.length === 0
      ? null
      : `${streetParts.join(" ")}${unitIdentifier === null ? "" : ` UNIT ${unitIdentifier}`}`;
  const cityName = readText(record.cityName);
  const postalCode = normalizePostalCode(record.postalCode);
  if (street === null && cityName === null && postalCode === null) return null;
  const unnormalizedAddress = [street, cityName, "FL", postalCode]
    .filter((part) => part !== null)
    .join(", ");
  const normalizedAddressKey = normalizeQueryDbAddress(unnormalizedAddress);
  const sourcePayload = compactObject({
    street,
    city_name: cityName,
    state_code: "FL",
    postal_code: postalCode,
  });
  return {
    sourcePayload,
    values: compactObject({
      request_identifier: normalizeParcelIdentifier(record.parcelIdentifier),
      street_number: readText(record.streetNumber),
      street_pre_directional_text: readText(record.streetPrefix),
      street_name: readText(record.streetName),
      street_suffix_type: readText(record.streetSuffix),
      street_post_directional_text: readText(record.streetPostDirectional),
      unit_identifier: unitIdentifier,
      city_name: cityName,
      county_name: "Polk",
      state_code: "FL",
      postal_code: postalCode,
      country_code: "US",
      unnormalized_address: unnormalizedAddress,
      normalized_address_key: normalizedAddressKey,
      normalized_address_hash:
        normalizedAddressKey === null ? null : hashText(normalizedAddressKey),
    }),
  };
}

/**
 * Normalize public address text the same way query-db hash keys are formed.
 *
 * Polk's bulk source already supplies abbreviated directionals and suffixes, so
 * only punctuation, whitespace, and case normalization are required here.
 *
 * @param {unknown} value Address text.
 * @returns {string | null} Lowercase query-db address key.
 */
export function normalizeQueryDbAddress(value) {
  const text = readText(value);
  if (text === null) return null;
  const normalized = text
    .toUpperCase()
    .replace(/[#.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return normalized.length === 0 ? null : normalized.toLowerCase();
}

/**
 * Add immutable source provenance to one target value object.
 *
 * @param {{sourceSystem:string,sourceRecordKey:string,sourceArtifactUri:string,sourcePayload:Record<string,unknown>,values:Record<string,unknown>}} params Metadata inputs.
 * @returns {Record<string, unknown>} Compact typed values.
 */
function withSourceMetadata(params) {
  return compactObject({
    ...params.values,
    source_system: params.sourceSystem,
    source_record_key: params.sourceRecordKey,
    source_record_hash: hashText(stableJsonStringify(params.sourcePayload)),
    source_artifact_uri: params.sourceArtifactUri,
    source_payload: params.sourcePayload,
  });
}

/**
 * Remove undefined properties while preserving explicit nulls.
 *
 * @template {Record<string, unknown>} Value
 * @param {Value} value Object to compact.
 * @returns {Record<string, unknown>} Compact object.
 */
function compactObject(value) {
  return Object.fromEntries(
    Object.entries(value).filter(([, entry]) => entry !== undefined),
  );
}

/**
 * Render a numeric source value for a query-db text column.
 *
 * @param {unknown} value Numeric source value.
 * @returns {string | null} Finite numeric text.
 */
function numberAsText(value) {
  const number = readNumber(value);
  return number === null ? null : String(number);
}

/**
 * Hash text with SHA-256.
 *
 * @param {string} value Text to hash.
 * @returns {string} Lowercase hexadecimal digest.
 */
function hashText(value) {
  return createHash("sha256").update(value).digest("hex");
}

/**
 * Deterministically serialize JSON while converting DuckDB bigint values.
 *
 * @param {unknown} value JSON-compatible value.
 * @returns {string} Stable compact JSON.
 */
function stableJsonStringify(value) {
  const sanitized = sanitizeJsonValue(value);
  if (sanitized === null) return "null";
  if (typeof sanitized !== "object") return JSON.stringify(sanitized);
  if (Array.isArray(sanitized)) {
    return `[${sanitized.map((entry) => stableJsonStringify(entry)).join(",")}]`;
  }
  return `{${Object.keys(sanitized)
    .sort()
    .map(
      (key) => `${JSON.stringify(key)}:${stableJsonStringify(sanitized[key])}`,
    )
    .join(",")}}`;
}

/**
 * Convert arbitrary source values into PostgreSQL JSONB-safe values.
 *
 * @param {unknown} value Source value.
 * @returns {unknown} JSON-compatible value.
 */
function sanitizeJsonValue(value) {
  if (typeof value === "bigint") return value.toString();
  if (typeof value === "string") return value.replace(/\u0000/g, "�");
  if (Array.isArray(value)) return value.map(sanitizeJsonValue);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [
        key,
        sanitizeJsonValue(entry),
      ]),
    );
  }
  return value;
}

/**
 * Escape one PostgreSQL CSV field.
 *
 * @param {string | number | null} value Field value.
 * @returns {string} Escaped field.
 */
function serializeCsvField(value) {
  if (value === null) return "";
  const text = String(value);
  if (text.length === 0) return '""';
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

/**
 * Write text while respecting stream backpressure.
 *
 * @param {import("node:fs").WriteStream} output Open output stream.
 * @param {string} text Text chunk.
 * @returns {Promise<void>} Resolves when the chunk is accepted.
 */
async function writeWithBackpressure(output, text) {
  if (output.write(text)) return;
  await once(output, "drain");
}

/**
 * End an output stream and surface write failures.
 *
 * @param {import("node:fs").WriteStream} output Open output stream.
 * @returns {Promise<void>} Resolves after the descriptor closes.
 */
async function closeOutput(output) {
  if (output.closed) return;
  output.end();
  await once(output, "close");
}

/**
 * Close DuckDB handles.
 *
 * @param {import("duckdb").Connection} connection Open connection.
 * @param {import("duckdb").Database} database Open database.
 * @returns {Promise<void>} Resolves when both handles close.
 */
async function closeDuckDb(connection, database) {
  await new Promise((resolve) => connection.close(() => resolve()));
  database.close();
}

const invokedPath = process.argv[1];
if (
  invokedPath !== undefined &&
  import.meta.url === pathToFileURL(path.resolve(invokedPath)).href
) {
  const receipt = await writePolkQueryDbStage(
    parsePolkQueryDbStageOptions(process.argv.slice(2)),
  );
  process.stdout.write(`${JSON.stringify(receipt, null, 2)}\n`);
}
