#!/usr/bin/env node
// @ts-check

/**
 * Idempotently load completed Broward Accela/Tyler list inventories to Neon.
 *
 * List records establish permit inventory before expensive detail enrichment.
 * Exact Tyler folios link immediately; Accela address-only rows remain valid
 * unlinked permits. Later detail loads reuse the same source key and preserve
 * richer payloads while updating the existing logical row.
 */

import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

import pg from "pg";

import { refreshBrowardDashboardRollup } from "./broward-dashboard-rollup.mjs";
import { normalizeArcgisBrowardFolio } from "./permit-source-adapters/broward-arcgis-bulk.mjs";

const { Client } = pg;
const EXPECTED_PROJECT_ID = "raspy-frost-51580436";
const PRODUCTION_ENDPOINT_PREFIX = "ep-mute-leaf";
const LOAD_LOCK_NAMESPACE = 12_011;
const LOAD_LOCK_KEY = 3;
const CONTROL_SCHEMA = "ingest_control";

/**
 * @typedef {object} PermitListLoadOptions
 * @property {string} jobId - Stable immutable load identity.
 * @property {string} inputPath - Completed normalized-list private JSONL.
 * @property {number} chunkSize - Rows per durable Neon transaction.
 *
 * @typedef {object} AccelaListRecord
 * @property {"oracle-node.broward-accela-list.v1"} schemaVersion - List schema.
 * @property {string} sourceSystem - Jurisdiction source system.
 * @property {string} jurisdiction - Issuing jurisdiction.
 * @property {string} recordNumber - Full public record number.
 * @property {string} sourceUrl - Official Accela detail URL.
 * @property {string | null} address - Public list work address.
 * @property {string | null} description - Public list description.
 * @property {string | null} status - Public list status.
 * @property {string | null} recordType - Public list record type.
 * @property {string} recordKey - Portal-compatible source key.
 * @property {string[]} sourceWindowKeys - Source date windows.
 *
 * @typedef {object} TylerListRecord
 * @property {string} source_system - Jurisdiction source system.
 * @property {string} source_url - Official Tyler detail URL.
 * @property {string} city - Issuing jurisdiction.
 * @property {string} permit_number - Public permit number.
 * @property {string | null} parcel_identifier - Source parcel text.
 * @property {string | null} work_location - Public location.
 * @property {string | null} permit_issue_date - ISO issue date.
 * @property {string | null} record_status - Public status.
 * @property {string | null} record_type - Public permit type.
 * @property {string | null} project_description - Public description.
 * @property {boolean} is_roof_permit - Conservative list classification.
 * @property {Readonly<{
 *   case_id:string,
 *   work_class:string|null,
 *   applied_date:string|null,
 *   expiration_date:string|null,
 *   finalized_date:string|null
 * }>} raw - Allow-listed Tyler list fields.
 *
 * @typedef {object} AccelaCsvListRecord
 * @property {"oracle-node.broward-accela-csv-list.v1"} schemaVersion - CSV list schema.
 * @property {string} sourceSystem - Jurisdiction source system.
 * @property {string} jurisdiction - Issuing jurisdiction.
 * @property {string} recordNumber - Full exported permit number.
 * @property {string} sourceUrl - Official detail lookup.
 * @property {string} recordKey - Detail-compatible source key.
 * @property {string | null} recordDate - Ambiguous source Date retained without field inference.
 * @property {string | null} recordType - Exported record type.
 * @property {string | null} projectName - Exported project name.
 * @property {string | null} address - Exported work address.
 * @property {string | null} expirationDate - Exported ISO expiration date.
 * @property {string | null} status - Exported status.
 * @property {boolean} isRoofPermit - Conservative classification.
 * @property {string} sourceWindowKey - Source date window.
 *
 * @typedef {object} NormalizedPermitListRecord
 * @property {string} sourceSystem - Jurisdiction source system.
 * @property {string} sourceRecordKey - Stable detail-compatible identity.
 * @property {string} permitNumber - Public permit number.
 * @property {string} sourceUrl - Official detail URL.
 * @property {string} jurisdiction - Issuing jurisdiction.
 * @property {string | null} parcelIdentifier - Canonical Broward folio.
 * @property {string | null} workLocation - Public work location.
 * @property {string | null} applicationDate - ISO application date.
 * @property {string | null} permitIssueDate - ISO issue date.
 * @property {string | null} expirationDate - ISO expiration date.
 * @property {string | null} finalizedDate - ISO finalization date.
 * @property {string | null} recordStatus - Public status.
 * @property {string | null} recordType - Public type.
 * @property {string | null} description - Public description.
 * @property {boolean} isRoofPermit - Conservative roofing classification.
 * @property {Record<string, unknown>} sourcePayload - Complete list record.
 *
 * @typedef {object} PermitListLoadRow
 * @property {string | null} property_id - Exact property parent UUID.
 * @property {string | null} parcel_id - Exact parcel parent UUID.
 * @property {string} request_identifier - Stable source key.
 * @property {string} permit_number - Public permit number.
 * @property {string | null} improvement_type - Public permit type.
 * @property {string | null} improvement_status - Public status.
 * @property {string | null} application_received_date - ISO application date.
 * @property {string | null} permit_issue_date - ISO issue date.
 * @property {string | null} expiration_date - ISO expiration date.
 * @property {string | null} completion_date - ISO source finalization date.
 * @property {string} source - Stable source system.
 * @property {string} source_url - Official detail URL.
 * @property {string | null} record_type - Public type.
 * @property {string | null} record_status - Public status.
 * @property {string | null} opened_date - ISO application date.
 * @property {string | null} work_location - Public work location.
 * @property {string | null} parcel_identifier - Canonical Broward folio.
 * @property {string | null} project_description - Public description.
 * @property {string | null} description - Public description.
 * @property {Record<string, unknown>} more_details - List facts/provenance.
 * @property {Record<string, unknown>} source_http_request - Reproducible request.
 * @property {Record<string, unknown>} source_payload - Complete normalized list record.
 * @property {string} source_system - Stable source system.
 * @property {string} source_record_key - Stable detail-compatible identity.
 * @property {string} source_record_hash - Stable normalized hash.
 * @property {string} source_artifact_uri - Official detail URL.
 * @property {"exact_folio" | "unmatched"} property_match_method - Match method.
 * @property {"exact" | "unmatched"} property_match_confidence - Confidence.
 */

/**
 * Parse a completed list-load command.
 *
 * @param {readonly string[]} argv - CLI arguments.
 * @returns {PermitListLoadOptions} Validated options.
 */
export function parsePermitListLoadOptions(argv) {
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
      throw new Error("Permit list load options must be --flag value pairs");
    }
    values.set(flag.slice(2), value);
  }
  const jobId = values.get("job-id");
  const inputPath = values.get("input");
  if (
    typeof jobId !== "string" ||
    !/^broward-permits-[a-z0-9-]+$/u.test(jobId)
  ) {
    throw new Error("--job-id must begin broward-permits-");
  }
  if (typeof inputPath !== "string" || inputPath.length === 0) {
    throw new Error("--input is required");
  }
  const chunkSize = Number(values.get("chunk-size") ?? "1000");
  if (!Number.isInteger(chunkSize) || chunkSize < 1 || chunkSize > 5_000) {
    throw new Error("--chunk-size must be an integer from 1 through 5000");
  }
  return { jobId, inputPath, chunkSize };
}

/**
 * Read, validate, and deduplicate one completed list artifact.
 *
 * @param {string} inputPath - Private JSONL path.
 * @returns {Promise<{
 *   records:NormalizedPermitListRecord[],
 *   inputSha256:string,
 *   duplicateCount:number
 * }>} Unique deterministic permit inventory.
 */
export async function readPermitListRecords(inputPath) {
  const text = await readFile(inputPath, "utf8");
  /** @type {Map<string, {record:NormalizedPermitListRecord,serialized:string}>} */
  const byKey = new Map();
  let sourceCount = 0;
  for (const line of text.split(/\r?\n/u)) {
    if (line.trim().length === 0) continue;
    sourceCount += 1;
    const value = /** @type {unknown} */ (JSON.parse(line));
    const record = normalizePermitListRecord(value);
    const serialized = stableJson(record);
    const existing = byKey.get(record.sourceRecordKey);
    if (existing !== undefined && existing.serialized !== serialized) {
      throw new Error(
        `Conflicting Broward permit list record ${record.sourceRecordKey}`,
      );
    }
    byKey.set(record.sourceRecordKey, { record, serialized });
  }
  if (sourceCount === 0) {
    throw new Error("Broward permit list input is empty");
  }
  return {
    records: [...byKey.values()]
      .map((entry) => entry.record)
      .sort((left, right) =>
        left.sourceRecordKey.localeCompare(right.sourceRecordKey),
      ),
    inputSha256: createHash("sha256").update(text).digest("hex"),
    duplicateCount: sourceCount - byKey.size,
  };
}

/**
 * Normalize either supported list artifact contract.
 *
 * @param {unknown} value - Parsed JSONL row.
 * @returns {NormalizedPermitListRecord} Unified load record.
 */
export function normalizePermitListRecord(value) {
  if (isAccelaCsvListRecord(value)) {
    return {
      sourceSystem: value.sourceSystem,
      sourceRecordKey: value.recordKey,
      permitNumber: value.recordNumber,
      sourceUrl: value.sourceUrl,
      jurisdiction: value.jurisdiction,
      parcelIdentifier: null,
      workLocation: value.address,
      applicationDate: null,
      permitIssueDate: null,
      expirationDate: value.expirationDate,
      finalizedDate: null,
      recordStatus: value.status,
      recordType: value.recordType,
      description: value.projectName,
      isRoofPermit: value.isRoofPermit,
      sourcePayload: {
        schema_version: "oracle-node.broward-accela-csv-list.v1",
        ...value,
      },
    };
  }
  if (isAccelaListRecord(value)) {
    return {
      sourceSystem: value.sourceSystem,
      sourceRecordKey: value.recordKey,
      permitNumber: value.recordNumber,
      sourceUrl: value.sourceUrl,
      jurisdiction: value.jurisdiction,
      parcelIdentifier: null,
      workLocation: value.address,
      applicationDate: null,
      permitIssueDate: null,
      expirationDate: null,
      finalizedDate: null,
      recordStatus: value.status,
      recordType: value.recordType,
      description: value.description,
      isRoofPermit: /\broof(?:ing)?\b/iu.test(
        [value.recordNumber, value.recordType, value.description]
          .filter((part) => typeof part === "string")
          .join(" "),
      ),
      sourcePayload: {
        schema_version: "oracle-node.broward-accela-list.v1",
        ...value,
      },
    };
  }
  if (isTylerListRecord(value)) {
    return {
      sourceSystem: value.source_system,
      sourceRecordKey: `${value.source_system}:${value.raw.case_id}`,
      permitNumber: value.permit_number,
      sourceUrl: value.source_url,
      jurisdiction: value.city,
      parcelIdentifier: normalizeArcgisBrowardFolio(value.parcel_identifier),
      workLocation: value.work_location,
      applicationDate: value.raw.applied_date,
      permitIssueDate: value.permit_issue_date,
      expirationDate: value.raw.expiration_date,
      finalizedDate: value.raw.finalized_date,
      recordStatus: value.record_status,
      recordType: value.record_type,
      description: value.project_description,
      isRoofPermit: value.is_roof_permit,
      sourcePayload: {
        schema_version: "oracle-node.broward-tyler-list.v1",
        ...value,
      },
    };
  }
  throw new Error("Unsupported Broward permit list row");
}

/**
 * Build one database row with an optional exact property parent.
 *
 * @param {NormalizedPermitListRecord} record - Unified list row.
 * @param {{propertyId:string,parcelId:string} | undefined} parent - Exact parent.
 * @returns {PermitListLoadRow} JSON-recordset row.
 */
export function mapPermitListLoadRow(record, parent) {
  return {
    property_id: parent?.propertyId ?? null,
    parcel_id: parent?.parcelId ?? null,
    request_identifier: record.sourceRecordKey,
    permit_number: record.permitNumber,
    improvement_type: record.recordType,
    improvement_status: record.recordStatus,
    application_received_date: record.applicationDate,
    permit_issue_date: record.permitIssueDate,
    expiration_date: record.expirationDate,
    completion_date: record.finalizedDate,
    source: record.sourceSystem,
    source_url: record.sourceUrl,
    record_type: record.recordType,
    record_status: record.recordStatus,
    opened_date: record.applicationDate,
    work_location: record.workLocation,
    parcel_identifier: record.parcelIdentifier,
    project_description: record.description,
    description: record.description,
    more_details: {
      list_inventory: true,
      list_jurisdiction: record.jurisdiction,
      is_roof_permit: record.isRoofPermit,
      list_source_payload: record.sourcePayload,
    },
    source_http_request: { method: "GET", url: record.sourceUrl },
    source_payload: record.sourcePayload,
    source_system: record.sourceSystem,
    source_record_key: record.sourceRecordKey,
    source_record_hash: stableHash(record),
    source_artifact_uri: record.sourceUrl,
    property_match_method: parent === undefined ? "unmatched" : "exact_folio",
    property_match_confidence: parent === undefined ? "unmatched" : "exact",
  };
}

/**
 * Load a complete immutable list artifact in durable chunks.
 *
 * @param {PermitListLoadOptions} options - Validated load options.
 * @returns {Promise<{
 *   sourceRecordCount:number,
 *   uniqueRecordCount:number,
 *   duplicateRecordCount:number,
 *   matchedRecordCount:number,
 *   unmatchedRecordCount:number,
 *   committedChunkCount:number,
 *   inputSha256:string
 * }>} Reconciled load result.
 */
export async function loadPermitListToNeon(options) {
  const input = await readPermitListRecords(options.inputPath);
  const target = requireTarget(process.env);
  const client = new Client({
    connectionString: target.connectionString,
    application_name: "broward-permit-list-loader",
    connectionTimeoutMillis: 10_000,
    statement_timeout: 180_000,
  });
  await client.connect();
  try {
    await verifyTarget(client, target);
    await acquireLoadLock(client);
    await ensureControlTables(client);
    await registerRun(client, options, input);
    const committedResult = await client.query(
      `SELECT chunk_index FROM ${CONTROL_SCHEMA}.broward_permit_list_load_chunks
       WHERE job_id=$1`,
      [options.jobId],
    );
    const committed = new Set(
      committedResult.rows.map((row) => Number(row.chunk_index)),
    );
    for (
      let offset = 0, chunkIndex = 0;
      offset < input.records.length;
      offset += options.chunkSize, chunkIndex += 1
    ) {
      if (committed.has(chunkIndex)) continue;
      const records = input.records.slice(offset, offset + options.chunkSize);
      await loadChunk(client, options.jobId, chunkIndex, records);
    }
    const aggregate = await client.query(
      `SELECT count(*)::integer AS chunks,
              coalesce(sum(record_count),0)::integer AS records,
              coalesce(sum(matched_count),0)::integer AS matched,
              coalesce(sum(unmatched_count),0)::integer AS unmatched
       FROM ${CONTROL_SCHEMA}.broward_permit_list_load_chunks
       WHERE job_id=$1`,
      [options.jobId],
    );
    const row = aggregate.rows[0];
    if (Number(row?.records) !== input.records.length) {
      throw new Error("Broward permit list chunk receipts do not reconcile");
    }
    await client.query(
      `UPDATE ${CONTROL_SCHEMA}.broward_permit_list_load_runs
       SET status='complete',completed_at=now(),heartbeat_at=now()
       WHERE job_id=$1`,
      [options.jobId],
    );
    await refreshBrowardDashboardRollup(client);
    return {
      sourceRecordCount: input.records.length + input.duplicateCount,
      uniqueRecordCount: input.records.length,
      duplicateRecordCount: input.duplicateCount,
      matchedRecordCount: Number(row.matched),
      unmatchedRecordCount: Number(row.unmatched),
      committedChunkCount: Number(row.chunks),
      inputSha256: input.inputSha256,
    };
  } finally {
    await client.end();
  }
}

/**
 * Load one exact logical chunk and receipt atomically.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @param {string} jobId - Stable load job.
 * @param {number} chunkIndex - Zero-based chunk.
 * @param {readonly NormalizedPermitListRecord[]} records - Unique rows.
 * @returns {Promise<void>} Resolves after commit.
 */
async function loadChunk(client, jobId, chunkIndex, records) {
  const folios = [
    ...new Set(
      records.flatMap((record) =>
        record.parcelIdentifier === null ? [] : [record.parcelIdentifier],
      ),
    ),
  ];
  const parentsResult =
    folios.length === 0
      ? { rows: [] }
      : await client.query(
          `SELECT request_identifier,property_id,parcel_id
           FROM public.properties
           WHERE source_system='broward_appraiser'
             AND request_identifier=ANY($1::text[])`,
          [folios],
        );
  /** @type {Map<string,{propertyId:string,parcelId:string}>} */
  const parents = new Map();
  for (const row of parentsResult.rows) {
    if (
      typeof row.request_identifier === "string" &&
      typeof row.property_id === "string" &&
      typeof row.parcel_id === "string"
    ) {
      parents.set(row.request_identifier, {
        propertyId: row.property_id,
        parcelId: row.parcel_id,
      });
    }
  }
  let matched = 0;
  const loadRows = records.map((record) => {
    const parent =
      record.parcelIdentifier === null
        ? undefined
        : parents.get(record.parcelIdentifier);
    if (parent !== undefined) matched += 1;
    return mapPermitListLoadRow(record, parent);
  });
  await client.query("BEGIN");
  try {
    await client.query(PERMIT_LIST_UPSERT_SQL, [JSON.stringify(loadRows)]);
    await client.query(
      `INSERT INTO ${CONTROL_SCHEMA}.broward_permit_list_load_chunks (
         job_id,chunk_index,record_count,matched_count,unmatched_count
       ) VALUES ($1,$2,$3,$4,$5)`,
      [jobId, chunkIndex, records.length, matched, records.length - matched],
    );
    await client.query(
      `UPDATE ${CONTROL_SCHEMA}.broward_permit_list_load_runs
       SET heartbeat_at=now() WHERE job_id=$1`,
      [jobId],
    );
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

const PERMIT_LIST_UPSERT_SQL = `
  INSERT INTO public.property_improvements AS existing (
    property_id,parcel_id,request_identifier,permit_number,improvement_type,
    improvement_status,improvement_action,application_received_date,
    permit_issue_date,expiration_date,completion_date,source,source_url,
    record_type,source_status,record_status,opened_date,work_location,
    parcel_identifier,project_description,description,more_details,
    source_http_request,source_payload,source_system,source_record_key,
    source_record_hash,source_artifact_uri,property_match_method,
    property_match_confidence
  )
  SELECT
    input.property_id,input.parcel_id,input.request_identifier,
    input.permit_number,input.improvement_type,input.improvement_status,
    'permit_record',input.application_received_date,input.permit_issue_date,
    input.expiration_date,input.completion_date,input.source,input.source_url,
    input.record_type,input.improvement_status,input.record_status,
    input.opened_date,input.work_location,input.parcel_identifier,
    input.project_description,input.description,input.more_details,
    input.source_http_request,input.source_payload,input.source_system,
    input.source_record_key,input.source_record_hash,input.source_artifact_uri,
    input.property_match_method,input.property_match_confidence
  FROM jsonb_to_recordset($1::jsonb) AS input(
    property_id uuid,parcel_id uuid,request_identifier text,permit_number text,
    improvement_type text,improvement_status text,
    application_received_date date,permit_issue_date date,
    expiration_date date,completion_date date,source text,source_url text,
    record_type text,record_status text,opened_date date,work_location text,
    parcel_identifier text,project_description text,description text,
    more_details jsonb,source_http_request jsonb,source_payload jsonb,
    source_system text,source_record_key text,source_record_hash text,
    source_artifact_uri text,property_match_method text,
    property_match_confidence text
  )
  ON CONFLICT (source_system,source_record_key) DO UPDATE SET
    property_id=coalesce(EXCLUDED.property_id,existing.property_id),
    parcel_id=coalesce(EXCLUDED.parcel_id,existing.parcel_id),
    permit_number=EXCLUDED.permit_number,
    improvement_type=coalesce(EXCLUDED.improvement_type,existing.improvement_type),
    improvement_status=coalesce(EXCLUDED.improvement_status,existing.improvement_status),
    application_received_date=coalesce(
      EXCLUDED.application_received_date,
      existing.application_received_date
    ),
    permit_issue_date=coalesce(EXCLUDED.permit_issue_date,existing.permit_issue_date),
    expiration_date=coalesce(EXCLUDED.expiration_date,existing.expiration_date),
    completion_date=coalesce(EXCLUDED.completion_date,existing.completion_date),
    source_url=coalesce(existing.source_url,EXCLUDED.source_url),
    record_type=coalesce(EXCLUDED.record_type,existing.record_type),
    source_status=coalesce(EXCLUDED.source_status,existing.source_status),
    record_status=coalesce(EXCLUDED.record_status,existing.record_status),
    opened_date=coalesce(EXCLUDED.opened_date,existing.opened_date),
    work_location=coalesce(EXCLUDED.work_location,existing.work_location),
    parcel_identifier=coalesce(EXCLUDED.parcel_identifier,existing.parcel_identifier),
    project_description=coalesce(
      EXCLUDED.project_description,
      existing.project_description
    ),
    description=coalesce(EXCLUDED.description,existing.description),
    more_details=coalesce(existing.more_details,'{}'::jsonb)
      || EXCLUDED.more_details,
    source_http_request=CASE
      WHEN existing.source_payload IS NULL OR existing.source_payload->>'schema_version'
        LIKE '%-list.v1' THEN EXCLUDED.source_http_request
      ELSE existing.source_http_request
    END,
    source_payload=CASE
      WHEN existing.source_payload IS NULL OR existing.source_payload->>'schema_version'
        LIKE '%-list.v1' THEN EXCLUDED.source_payload
      ELSE existing.source_payload
    END,
    source_record_hash=CASE
      WHEN existing.source_payload IS NULL OR existing.source_payload->>'schema_version'
        LIKE '%-list.v1' THEN EXCLUDED.source_record_hash
      ELSE existing.source_record_hash
    END,
    source_artifact_uri=coalesce(
      existing.source_artifact_uri,
      EXCLUDED.source_artifact_uri
    ),
    property_match_method=CASE
      WHEN EXCLUDED.property_id IS NOT NULL THEN 'exact_folio'
      ELSE existing.property_match_method
    END,
    property_match_confidence=CASE
      WHEN EXCLUDED.property_id IS NOT NULL THEN 'exact'
      ELSE existing.property_match_confidence
    END,
    loaded_at=now(),
    updated_at=now()
`;

/**
 * Create additive durable list-load control tables.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @returns {Promise<void>} Resolves after idempotent DDL.
 */
async function ensureControlTables(client) {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${CONTROL_SCHEMA}`);
  await client.query(
    `CREATE TABLE IF NOT EXISTS ${CONTROL_SCHEMA}.broward_permit_list_load_runs (
       job_id text PRIMARY KEY,
       input_path text NOT NULL,
       input_sha256 text NOT NULL CHECK (input_sha256 ~ '^[a-f0-9]{64}$'),
       unique_record_count integer NOT NULL CHECK (unique_record_count > 0),
       duplicate_record_count integer NOT NULL CHECK (duplicate_record_count >= 0),
       chunk_size integer NOT NULL CHECK (chunk_size > 0),
       status text NOT NULL CHECK (status IN ('running','complete')),
       started_at timestamptz NOT NULL DEFAULT now(),
       heartbeat_at timestamptz NOT NULL DEFAULT now(),
       completed_at timestamptz
     )`,
  );
  await client.query(
    `CREATE TABLE IF NOT EXISTS ${CONTROL_SCHEMA}.broward_permit_list_load_chunks (
       job_id text NOT NULL REFERENCES
         ${CONTROL_SCHEMA}.broward_permit_list_load_runs(job_id),
       chunk_index integer NOT NULL CHECK (chunk_index >= 0),
       record_count integer NOT NULL CHECK (record_count > 0),
       matched_count integer NOT NULL CHECK (matched_count >= 0),
       unmatched_count integer NOT NULL CHECK (unmatched_count >= 0),
       committed_at timestamptz NOT NULL DEFAULT now(),
       PRIMARY KEY (job_id,chunk_index)
     )`,
  );
}

/**
 * Register or verify one immutable list load.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @param {PermitListLoadOptions} options - Load options.
 * @param {{records:NormalizedPermitListRecord[],inputSha256:string,duplicateCount:number}} input
 *   Validated input.
 * @returns {Promise<void>} Resolves only for matching run identity.
 */
async function registerRun(client, options, input) {
  await client.query(
    `INSERT INTO ${CONTROL_SCHEMA}.broward_permit_list_load_runs (
       job_id,input_path,input_sha256,unique_record_count,
       duplicate_record_count,chunk_size,status
     ) VALUES ($1,$2,$3,$4,$5,$6,'running')
     ON CONFLICT (job_id) DO NOTHING`,
    [
      options.jobId,
      options.inputPath,
      input.inputSha256,
      input.records.length,
      input.duplicateCount,
      options.chunkSize,
    ],
  );
  const result = await client.query(
    `SELECT input_path,input_sha256,unique_record_count,
            duplicate_record_count,chunk_size
     FROM ${CONTROL_SCHEMA}.broward_permit_list_load_runs WHERE job_id=$1`,
    [options.jobId],
  );
  const row = result.rows[0];
  if (
    row?.input_path !== options.inputPath ||
    row.input_sha256 !== input.inputSha256 ||
    Number(row.unique_record_count) !== input.records.length ||
    Number(row.duplicate_record_count) !== input.duplicateCount ||
    Number(row.chunk_size) !== options.chunkSize
  ) {
    throw new Error("Existing permit list load does not match input");
  }
}

/**
 * Acquire the shared permit table writer lock.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @returns {Promise<void>} Resolves only for the single writer.
 */
async function acquireLoadLock(client) {
  const result = await client.query(
    "SELECT pg_try_advisory_lock($1,$2) AS acquired",
    [LOAD_LOCK_NAMESPACE, LOAD_LOCK_KEY],
  );
  if (result.rows[0]?.acquired !== true) {
    throw new Error("Another Broward permit loader owns the writer lock");
  }
}

/**
 * Read and validate the direct isolated Neon target.
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
    throw new Error("Permit list loading requires direct Neon");
  }
  return { connectionString, expectedBranchId, expectedEndpointId };
}

/**
 * Prove exact Neon project/branch/endpoint identity.
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
    throw new Error("Permit list target is not isolated broward-ingest");
  }
}

/**
 * Validate an Accela list row.
 *
 * @param {unknown} value - Candidate row.
 * @returns {value is AccelaListRecord} Whether required fields exist.
 */
function isAccelaListRecord(value) {
  if (!isRecord(value)) return false;
  return (
    value.schemaVersion === "oracle-node.broward-accela-list.v1" &&
    typeof value.sourceSystem === "string" &&
    typeof value.jurisdiction === "string" &&
    typeof value.recordNumber === "string" &&
    typeof value.sourceUrl === "string" &&
    typeof value.recordKey === "string" &&
    Array.isArray(value.sourceWindowKeys)
  );
}

/**
 * Validate an official Accela CSV list row.
 *
 * @param {unknown} value - Candidate row.
 * @returns {value is AccelaCsvListRecord} Whether required fields exist.
 */
function isAccelaCsvListRecord(value) {
  if (!isRecord(value)) return false;
  return (
    value.schemaVersion === "oracle-node.broward-accela-csv-list.v1" &&
    typeof value.sourceSystem === "string" &&
    typeof value.jurisdiction === "string" &&
    typeof value.recordNumber === "string" &&
    typeof value.sourceUrl === "string" &&
    typeof value.recordKey === "string" &&
    typeof value.isRoofPermit === "boolean" &&
    typeof value.sourceWindowKey === "string"
  );
}

/**
 * Validate a Tyler list row.
 *
 * @param {unknown} value - Candidate row.
 * @returns {value is TylerListRecord} Whether required fields exist.
 */
function isTylerListRecord(value) {
  if (!isRecord(value) || !isRecord(value.raw)) return false;
  return (
    /^broward_[a-z0-9_]+_tyler_permits$/u.test(
      typeof value.source_system === "string" ? value.source_system : "",
    ) &&
    typeof value.source_url === "string" &&
    typeof value.city === "string" &&
    typeof value.permit_number === "string" &&
    typeof value.is_roof_permit === "boolean" &&
    typeof value.raw.case_id === "string"
  );
}

/**
 * Stable normalized hash.
 *
 * @param {unknown} value - JSON-compatible value.
 * @returns {string} Lowercase SHA-256.
 */
function stableHash(value) {
  return createHash("sha256").update(stableJson(value)).digest("hex");
}

/**
 * Serialize JSON with recursively sorted object keys.
 *
 * @param {unknown} value - JSON-compatible value.
 * @returns {string} Stable JSON.
 */
function stableJson(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableJson(entry)).join(",")}]`;
  }
  const record = /** @type {Record<string, unknown>} */ (value);
  return `{${Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableJson(record[key])}`)
    .join(",")}}`;
}

/**
 * Narrow an unknown value to a non-array record.
 *
 * @param {unknown} value - Candidate.
 * @returns {value is Record<string, unknown>} Whether it is a record.
 */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  loadPermitListToNeon(parsePermitListLoadOptions(process.argv.slice(2)))
    .then((result) => {
      console.log(
        JSON.stringify({
          event: "broward_permit_list_loaded",
          ...result,
        }),
      );
    })
    .catch((error) => {
      console.error(
        JSON.stringify({
          event: "broward_permit_list_load_failed",
          message: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      process.exitCode = 1;
    });
}
