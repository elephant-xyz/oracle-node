#!/usr/bin/env node
// @ts-check

/**
 * Capture and load official Broward municipal permit bulk feeds.
 *
 * Source rows are snapshotted by ArcGIS OBJECTID, written to private raw files,
 * normalized deterministically, and committed to the isolated Neon branch in
 * durable chunks. Existing portal-detail rows keep their richer raw payload
 * while bulk list fields are merged into the same permit-number identity.
 */

import { createHash } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import pg from "pg";

import {
  FORT_LAUDERDALE_PERMIT_ARCGIS_SOURCE,
  fetchArcgisPermitFeatures,
  fetchArcgisPermitObjectIds,
  hashArcgisObjectIds,
  normalizeFortLauderdaleArcgisPermit,
} from "./permit-source-adapters/broward-arcgis-bulk.mjs";

const { Client } = pg;
const EXPECTED_PROJECT_ID = "raspy-frost-51580436";
const PRODUCTION_ENDPOINT_PREFIX = "ep-mute-leaf";
const CONTROL_SCHEMA = "ingest_control";
const LOAD_LOCK_NAMESPACE = 12_011;
const LOAD_LOCK_KEY = 3;
const MANIFEST_SCHEMA_VERSION =
  "oracle-node.broward-permit-bulk-manifest.v1";
const DEFAULT_OUTPUT_DIRECTORY =
  "downloads/broward/permit-bulk/fort-lauderdale";

/**
 * @typedef {import("./permit-source-adapters/broward-arcgis-bulk.mjs").NormalizedBrowardArcgisPermit} NormalizedBrowardArcgisPermit
 *
 * @typedef {object} BulkPermitOptions
 * @property {string} jobId - Stable snapshot/load identity.
 * @property {"fort-lauderdale"} sourceKey - Verified bulk source key.
 * @property {string} outputDirectory - Private raw/normalized artifact root.
 * @property {number} chunkSize - Exact source rows per durable chunk.
 * @property {number | null} limit - Optional deterministic pilot limit.
 * @property {boolean} load - Whether normalized records are committed to Neon.
 *
 * @typedef {object} BulkPermitChunkReceipt
 * @property {number} chunkIndex - Zero-based source chunk.
 * @property {string} objectIdsSha256 - Exact ordered object-ID hash.
 * @property {number} sourceRecordCount - Raw source rows reconciled.
 * @property {number} normalizedRecordCount - Valid normalized rows before cross-chunk dedupe.
 * @property {number} invalidRecordCount - Rows missing stable required identity.
 * @property {number} roofRecordCount - Valid conservatively classified roofing rows.
 * @property {number} matchedPropertyCount - Rows linked by exact BCPA folio.
 * @property {number} unmatchedPropertyCount - Rows retained without a property link.
 * @property {string} rawSha256 - Raw ArcGIS response hash.
 * @property {string} normalizedSha256 - Deterministic normalized JSONL hash.
 * @property {"captured" | "committed"} status - Durable processing boundary.
 * @property {string} completedAt - ISO completion timestamp.
 *
 * @typedef {object} BulkPermitManifest
 * @property {typeof MANIFEST_SCHEMA_VERSION} schemaVersion - Manifest contract.
 * @property {string} jobId - Stable operator-selected job.
 * @property {"fort-lauderdale"} sourceKey - Bulk source identity.
 * @property {string} sourceSystem - Query-db source identity.
 * @property {string} sourceUrl - Official FeatureServer layer.
 * @property {string} officialEvidenceUrl - Official service documentation.
 * @property {number} sourceObjectIdCount - Full source snapshot count.
 * @property {string} sourceObjectIdsSha256 - Full source snapshot hash.
 * @property {number} selectedObjectIdCount - Pilot/full selected count.
 * @property {string} selectedObjectIdsSha256 - Selected ID hash.
 * @property {number} chunkSize - Immutable source chunk size.
 * @property {boolean} load - Whether the manifest requires Neon commits.
 * @property {string} startedAt - ISO start timestamp.
 * @property {string} updatedAt - ISO latest checkpoint.
 * @property {Record<string, BulkPermitChunkReceipt>} chunks - Receipts by padded index.
 * @property {{
 *   sourceRecords:number,
 *   normalizedRecords:number,
 *   uniquePermitRecords:number,
 *   duplicatePermitRecords:number,
 *   invalidRecords:number,
 *   roofingRecords:number,
 *   matchedProperties:number,
 *   unmatchedProperties:number,
 *   allSourceRowsAccountedFor:boolean,
 *   completedChunks:number,
 *   expectedChunks:number
 * } | null} reconciliation - Final accounting, null while incomplete.
 *
 * @typedef {object} BulkPermitLoadRow
 * @property {string | null} property_id - Exact parent UUID when matched.
 * @property {string | null} parcel_id - Exact parcel UUID when matched.
 * @property {string} request_identifier - Stable source permit key.
 * @property {string} permit_number - Public permit number.
 * @property {string | null} improvement_type - Public permit type.
 * @property {string | null} improvement_status - Public status.
 * @property {string | null} application_received_date - Source submission date.
 * @property {number | null} estimated_job_value - Public construction cost.
 * @property {string} source - Stable source system.
 * @property {string} source_url - Official public permit URL.
 * @property {string | null} record_type - Public permit type.
 * @property {string | null} record_status - Public status.
 * @property {string | null} opened_date - Source submission date.
 * @property {string | null} work_location - Public work location.
 * @property {string | null} parcel_identifier - Canonical Broward folio.
 * @property {string | null} applicant - Public applicant.
 * @property {string | null} licensed_professional - Public contractor identity.
 * @property {string | null} project_description - Public description.
 * @property {string | null} description - Public description.
 * @property {Record<string, unknown>} more_details - Bulk facts and provenance.
 * @property {Record<string, unknown>} source_http_request - Reproducible source request.
 * @property {Record<string, unknown>} source_payload - Allow-listed normalized source payload.
 * @property {string} source_system - Stable source system.
 * @property {string} source_record_key - Portal-compatible permit key.
 * @property {string} source_record_hash - Stable normalized row hash.
 * @property {string} source_artifact_uri - Official public permit URL.
 * @property {"exact_folio" | "unmatched"} property_match_method - Match method.
 * @property {"exact" | "unmatched"} property_match_confidence - Match confidence.
 * @property {string} retrieved_at - Source retrieval timestamp.
 */

/**
 * Parse a bulk permit job without accepting source-bypass options.
 *
 * @param {readonly string[]} argv - Arguments after the script path.
 * @returns {BulkPermitOptions} Validated immutable job configuration.
 */
export function parseBulkPermitOptions(argv) {
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
      throw new Error("Bulk permit options must be --flag value pairs");
    }
    values.set(flag.slice(2), value);
  }
  const jobId = values.get("job-id");
  if (
    typeof jobId !== "string" ||
    !/^broward-permits-[a-z0-9-]+$/u.test(jobId)
  ) {
    throw new Error("--job-id must begin broward-permits-");
  }
  const sourceKey = values.get("source") ?? "fort-lauderdale";
  if (sourceKey !== "fort-lauderdale") {
    throw new Error(
      "--source currently supports only the verified fort-lauderdale feed",
    );
  }
  const outputDirectory =
    values.get("output-dir") ?? DEFAULT_OUTPUT_DIRECTORY;
  if (outputDirectory.trim().length === 0) {
    throw new Error("--output-dir must not be empty");
  }
  const chunkSize = boundedInteger(
    values.get("chunk-size") ?? "1000",
    "chunk-size",
    1,
    FORT_LAUDERDALE_PERMIT_ARCGIS_SOURCE.maxChunkSize,
  );
  const rawLimit = values.get("limit");
  const limit =
    rawLimit === undefined
      ? null
      : boundedInteger(rawLimit, "limit", 1, 1_000_000);
  const rawLoad = values.get("load") ?? "false";
  if (rawLoad !== "true" && rawLoad !== "false") {
    throw new Error("--load must be true or false");
  }
  return {
    jobId,
    sourceKey,
    outputDirectory,
    chunkSize,
    limit,
    load: rawLoad === "true",
  };
}

/**
 * Run or resume one immutable official bulk source snapshot.
 *
 * @param {BulkPermitOptions} options - Validated job configuration.
 * @param {{
 *   fetchImpl?:typeof fetch,
 *   now?:()=>string,
 *   clientFactory?:()=>import("pg").Client
 * }} [dependencies={}] - Injectable transport, clock, and database factory.
 * @returns {Promise<BulkPermitManifest>} Completed reconciled manifest.
 */
export async function runBrowardBulkPermitIngest(
  options,
  dependencies = {},
) {
  const fetchImpl = dependencies.fetchImpl ?? fetch;
  const now = dependencies.now ?? (() => new Date().toISOString());
  const source = FORT_LAUDERDALE_PERMIT_ARCGIS_SOURCE;
  const outputDirectory = path.resolve(options.outputDirectory);
  const rawDirectory = path.join(outputDirectory, "raw-private");
  const normalizedDirectory = path.join(
    outputDirectory,
    "normalized-private",
  );
  const invalidDirectory = path.join(outputDirectory, "invalid-private");
  const manifestPath = path.join(outputDirectory, "manifest.private.json");
  await Promise.all(
    [outputDirectory, rawDirectory, normalizedDirectory, invalidDirectory].map(
      (directory) => mkdir(directory, { recursive: true, mode: 0o700 }),
    ),
  );

  const allObjectIds = await fetchArcgisPermitObjectIds(source, fetchImpl);
  if (allObjectIds.length === 0) {
    throw new Error("Fort Lauderdale bulk permit source returned no object IDs");
  }
  const selectedObjectIds =
    options.limit === null
      ? allObjectIds
      : allObjectIds.slice(0, options.limit);
  const expectedManifest = {
    sourceObjectIdCount: allObjectIds.length,
    sourceObjectIdsSha256: hashArcgisObjectIds(allObjectIds),
    selectedObjectIdCount: selectedObjectIds.length,
    selectedObjectIdsSha256: hashArcgisObjectIds(selectedObjectIds),
  };
  let manifest = await readOrCreateManifest(
    manifestPath,
    options,
    expectedManifest,
    now(),
  );

  /** @type {import("pg").Client | null} */
  let client = null;
  /** @type {ReadonlyMap<string, string>} */
  let portalRecordKeysByCaseKey = new Map();
  if (options.load) {
    const target = requireTarget(process.env);
    client =
      dependencies.clientFactory?.() ??
      new Client({
        connectionString: target.connectionString,
        application_name: "broward-bulk-permit-ingest",
        connectionTimeoutMillis: 10_000,
        statement_timeout: 180_000,
      });
    await client.connect();
    await verifyTarget(client, target);
    await acquireLoadLock(client);
    await ensureControlTables(client);
    await registerBulkRun(client, manifest);
    portalRecordKeysByCaseKey = await readPortalRecordKeysByCaseKey(client);
  }

  /** @type {Set<string>} */
  const uniquePermitKeys = new Set();
  let sourceRecords = 0;
  let normalizedRecords = 0;
  let invalidRecords = 0;
  let roofingRecords = 0;
  let matchedProperties = 0;
  let unmatchedProperties = 0;
  const expectedChunks = Math.ceil(
    selectedObjectIds.length / options.chunkSize,
  );

  try {
    for (
      let chunkIndex = 0;
      chunkIndex < expectedChunks;
      chunkIndex += 1
    ) {
      const start = chunkIndex * options.chunkSize;
      const objectIds = selectedObjectIds.slice(
        start,
        start + options.chunkSize,
      );
      const chunkKey = formatChunkIndex(chunkIndex);
      const existingReceipt = manifest.chunks[chunkKey];
      const normalizedPath = path.join(
        normalizedDirectory,
        `${chunkKey}.jsonl`,
      );
      if (
        existingReceipt !== undefined &&
        existingReceipt.objectIdsSha256 === hashArcgisObjectIds(objectIds) &&
        (!options.load ||
          (existingReceipt.status === "committed" &&
            client !== null &&
            (await isBulkChunkCommitted(
              client,
              manifest.jobId,
              chunkIndex,
              existingReceipt.rawSha256,
            ))))
      ) {
        const records = await readNormalizedChunk(normalizedPath);
        addPermitKeys(uniquePermitKeys, records);
        sourceRecords += existingReceipt.sourceRecordCount;
        normalizedRecords += existingReceipt.normalizedRecordCount;
        invalidRecords += existingReceipt.invalidRecordCount;
        roofingRecords += existingReceipt.roofRecordCount;
        matchedProperties += existingReceipt.matchedPropertyCount;
        unmatchedProperties += existingReceipt.unmatchedPropertyCount;
        continue;
      }

      const retrievedAt = now();
      const capture = await fetchArcgisPermitFeatures(
        source,
        objectIds,
        fetchImpl,
      );
      const rawPath = path.join(rawDirectory, `${chunkKey}.json`);
      await writePrivateAtomic(rawPath, capture.rawText);
      const normalized = capture.features.map((feature) =>
        normalizeFortLauderdaleArcgisPermit(feature, retrievedAt),
      );
      const invalid = normalized.flatMap((result, index) =>
        result.record === null
          ? [
              {
                source_object_id: String(
                  capture.features[index]?.attributes.OBJECTID ?? "",
                ),
                reason: result.invalidReason ?? "unknown_invalid_record",
              },
            ]
          : [],
      );
      const validRecords = normalized.flatMap((result) =>
        result.record === null ? [] : [result.record],
      );
      const normalizedText = renderNormalizedRecords(validRecords);
      await writePrivateAtomic(normalizedPath, normalizedText);
      await writePrivateAtomic(
        path.join(invalidDirectory, `${chunkKey}.jsonl`),
        invalid.length === 0
          ? ""
          : `${invalid.map((entry) => JSON.stringify(entry)).join("\n")}\n`,
      );

      let matchedPropertyCount = 0;
      let unmatchedPropertyCount = validRecords.filter(
        (record) => record.parcel_identifier === null,
      ).length;
      if (options.load) {
        if (client === null) {
          throw new Error("Bulk permit load client was not initialized");
        }
        const loadResult = await loadBulkPermitChunk(
          client,
          manifest,
          chunkIndex,
          objectIds,
          capture.rawText,
          validRecords,
          portalRecordKeysByCaseKey,
        );
        matchedPropertyCount = loadResult.matchedPropertyCount;
        unmatchedPropertyCount = loadResult.unmatchedPropertyCount;
      } else {
        matchedPropertyCount = 0;
      }

      const receipt = {
        chunkIndex,
        objectIdsSha256: hashArcgisObjectIds(objectIds),
        sourceRecordCount: capture.features.length,
        normalizedRecordCount: validRecords.length,
        invalidRecordCount: invalid.length,
        roofRecordCount: validRecords.filter(
          (record) => record.is_roof_permit,
        ).length,
        matchedPropertyCount,
        unmatchedPropertyCount,
        rawSha256: sha256(capture.rawText),
        normalizedSha256: sha256(normalizedText),
        status:
          options.load === true
            ? /** @type {"committed"} */ ("committed")
            : /** @type {"captured"} */ ("captured"),
        completedAt: now(),
      };
      manifest.chunks[chunkKey] = receipt;
      manifest.updatedAt = now();
      manifest.reconciliation = null;
      await writePrivateAtomic(
        manifestPath,
        `${JSON.stringify(manifest, null, 2)}\n`,
      );
      addPermitKeys(uniquePermitKeys, validRecords);
      sourceRecords += receipt.sourceRecordCount;
      normalizedRecords += receipt.normalizedRecordCount;
      invalidRecords += receipt.invalidRecordCount;
      roofingRecords += receipt.roofRecordCount;
      matchedProperties += receipt.matchedPropertyCount;
      unmatchedProperties += receipt.unmatchedPropertyCount;
    }
  } finally {
    if (client !== null) await client.end();
  }

  manifest.reconciliation = {
    sourceRecords,
    normalizedRecords,
    uniquePermitRecords: uniquePermitKeys.size,
    duplicatePermitRecords: normalizedRecords - uniquePermitKeys.size,
    invalidRecords,
    roofingRecords,
    matchedProperties,
    unmatchedProperties,
    allSourceRowsAccountedFor:
      sourceRecords === normalizedRecords + invalidRecords &&
      sourceRecords === selectedObjectIds.length,
    completedChunks: Object.keys(manifest.chunks).length,
    expectedChunks,
  };
  if (
    !manifest.reconciliation.allSourceRowsAccountedFor ||
    manifest.reconciliation.completedChunks !== expectedChunks
  ) {
    throw new Error("Bulk permit source rows did not reconcile");
  }
  manifest.updatedAt = now();
  await writePrivateAtomic(
    manifestPath,
    `${JSON.stringify(manifest, null, 2)}\n`,
  );
  return manifest;
}

/**
 * Read an existing immutable manifest or create its initial snapshot contract.
 *
 * @param {string} manifestPath - Private local manifest path.
 * @param {BulkPermitOptions} options - Current job options.
 * @param {{
 *   sourceObjectIdCount:number,
 *   sourceObjectIdsSha256:string,
 *   selectedObjectIdCount:number,
 *   selectedObjectIdsSha256:string
 * }} sourceSnapshot - Current exact source identity.
 * @param {string} startedAt - Initial timestamp.
 * @returns {Promise<BulkPermitManifest>} Validated or new manifest.
 */
async function readOrCreateManifest(
  manifestPath,
  options,
  sourceSnapshot,
  startedAt,
) {
  try {
    const parsed = /** @type {unknown} */ (
      JSON.parse(await readFile(manifestPath, "utf8"))
    );
    if (
      parsed === null ||
      typeof parsed !== "object" ||
      Array.isArray(parsed)
    ) {
      throw new Error("Bulk permit manifest is not an object");
    }
    const manifest = /** @type {Partial<BulkPermitManifest>} */ (parsed);
    if (
      manifest.schemaVersion !== MANIFEST_SCHEMA_VERSION ||
      manifest.jobId !== options.jobId ||
      manifest.sourceKey !== options.sourceKey ||
      manifest.sourceSystem !==
        FORT_LAUDERDALE_PERMIT_ARCGIS_SOURCE.sourceSystem ||
      manifest.sourceObjectIdCount !== sourceSnapshot.sourceObjectIdCount ||
      manifest.sourceObjectIdsSha256 !==
        sourceSnapshot.sourceObjectIdsSha256 ||
      manifest.selectedObjectIdCount !==
        sourceSnapshot.selectedObjectIdCount ||
      manifest.selectedObjectIdsSha256 !==
        sourceSnapshot.selectedObjectIdsSha256 ||
      manifest.chunkSize !== options.chunkSize ||
      manifest.load !== options.load ||
      manifest.chunks === null ||
      typeof manifest.chunks !== "object" ||
      Array.isArray(manifest.chunks)
    ) {
      throw new Error(
        "Existing bulk permit manifest does not match the source snapshot and job options",
      );
    }
    return /** @type {BulkPermitManifest} */ (manifest);
  } catch (error) {
    if (!isNodeError(error) || error.code !== "ENOENT") throw error;
  }
  const source = FORT_LAUDERDALE_PERMIT_ARCGIS_SOURCE;
  return {
    schemaVersion: MANIFEST_SCHEMA_VERSION,
    jobId: options.jobId,
    sourceKey: options.sourceKey,
    sourceSystem: source.sourceSystem,
    sourceUrl: source.serviceUrl,
    officialEvidenceUrl: source.officialEvidenceUrl,
    ...sourceSnapshot,
    chunkSize: options.chunkSize,
    load: options.load,
    startedAt,
    updatedAt: startedAt,
    chunks: {},
    reconciliation: null,
  };
}

/**
 * Index existing rich Accela portal rows by their complete cap identity.
 *
 * The FeatureServer's visible PERMITID is truncated and cannot safely dedupe
 * records. CASEKEY maps exactly to the capID1/capID2/capID3 tuple in existing
 * portal detail URLs, allowing the bulk row to enrich that row without
 * creating a second property improvement.
 *
 * @param {import("pg").Client} client - Verified direct Neon client.
 * @returns {Promise<ReadonlyMap<string,string>>} CASEKEY to source record key.
 */
async function readPortalRecordKeysByCaseKey(client) {
  const result = await client.query(
    `SELECT source_record_key,source_url
     FROM public.property_improvements
     WHERE source_system=$1
       AND source_payload ? 'schemaVersion'`,
    [FORT_LAUDERDALE_PERMIT_ARCGIS_SOURCE.sourceSystem],
  );
  /** @type {Map<string,string>} */
  const byCaseKey = new Map();
  for (const row of result.rows) {
    if (
      typeof row.source_record_key !== "string" ||
      typeof row.source_url !== "string"
    ) {
      continue;
    }
    const caseKey = accelaCaseKeyFromUrl(row.source_url);
    if (caseKey === null) continue;
    const existing = byCaseKey.get(caseKey);
    if (existing !== undefined && existing !== row.source_record_key) {
      throw new Error(
        `Existing Fort Lauderdale portal rows conflict for CASEKEY ${caseKey}`,
      );
    }
    byCaseKey.set(caseKey, row.source_record_key);
  }
  return byCaseKey;
}

/**
 * Rebuild the complete Accela CASEKEY from a public detail URL.
 *
 * @param {string} value - Candidate Accela detail URL.
 * @returns {string | null} Uppercase capID tuple or null.
 */
export function accelaCaseKeyFromUrl(value) {
  let url;
  try {
    url = new URL(value);
  } catch {
    return null;
  }
  const parts = ["capID1", "capID2", "capID3"].map((name) =>
    url.searchParams.get(name)?.trim().toUpperCase(),
  );
  return parts.every(
    (part) =>
      typeof part === "string" &&
      part.length > 0 &&
      /^[A-Z0-9]+$/u.test(part),
  )
    ? parts.join("-")
    : null;
}

/**
 * Load one raw source chunk and its durable receipt transactionally.
 *
 * @param {import("pg").Client} client - Verified direct Neon client.
 * @param {BulkPermitManifest} manifest - Immutable source snapshot contract.
 * @param {number} chunkIndex - Zero-based source chunk index.
 * @param {readonly number[]} objectIds - Exact source object IDs.
 * @param {string} rawText - Raw source response used for its checksum.
 * @param {readonly NormalizedBrowardArcgisPermit[]} records - Valid rows.
 * @param {ReadonlyMap<string,string>} portalRecordKeysByCaseKey
 *   Existing rich portal keys indexed by complete Accela CASEKEY.
 * @returns {Promise<{matchedPropertyCount:number,unmatchedPropertyCount:number}>}
 *   Exact folio-match reconciliation for the chunk.
 */
async function loadBulkPermitChunk(
  client,
  manifest,
  chunkIndex,
  objectIds,
  rawText,
  records,
  portalRecordKeysByCaseKey,
) {
  const folios = [
    ...new Set(
      records.flatMap((record) =>
        record.parcel_identifier === null
          ? []
          : [record.parcel_identifier],
      ),
    ),
  ];
  const parentResult =
    folios.length === 0
      ? { rows: [] }
      : await client.query(
          `SELECT request_identifier,property_id,parcel_id
           FROM public.properties
           WHERE source_system='broward_appraiser'
             AND request_identifier=ANY($1::text[])`,
          [folios],
        );
  /** @type {Map<string, {propertyId:string,parcelId:string}>} */
  const parents = new Map();
  for (const row of parentResult.rows) {
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
  let matchedPropertyCount = 0;
  const logicalRecords = dedupeBulkPermitRecords(records);
  /** @type {BulkPermitLoadRow[]} */
  const loadRows = logicalRecords.map((record) => {
    const parent =
      record.parcel_identifier === null
        ? undefined
        : parents.get(record.parcel_identifier);
    if (parent !== undefined) matchedPropertyCount += 1;
    const portalRecordKey =
      record.accela_case_key === null
        ? undefined
        : portalRecordKeysByCaseKey.get(record.accela_case_key);
    return mapBulkPermitLoadRow(record, parent, portalRecordKey);
  });
  const matchedSourceRecords = records.filter(
    (record) =>
      record.parcel_identifier !== null &&
      parents.has(record.parcel_identifier),
  ).length;
  const unmatchedPropertyCount = records.length - matchedSourceRecords;
  matchedPropertyCount = matchedSourceRecords;
  const rawSha256 = sha256(rawText);
  const normalizedSha256 = sha256(renderNormalizedRecords(records));

  await client.query("BEGIN");
  try {
    if (loadRows.length > 0) {
      await client.query(BULK_UPSERT_SQL, [JSON.stringify(loadRows)]);
    }
    await client.query(
      `INSERT INTO ${CONTROL_SCHEMA}.broward_bulk_permit_chunks (
         job_id,chunk_index,object_ids_sha256,raw_sha256,normalized_sha256,
         source_record_count,normalized_record_count,matched_property_count,
         unmatched_property_count
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       ON CONFLICT (job_id,chunk_index) DO UPDATE SET
         object_ids_sha256=EXCLUDED.object_ids_sha256,
         raw_sha256=EXCLUDED.raw_sha256,
         normalized_sha256=EXCLUDED.normalized_sha256,
         source_record_count=EXCLUDED.source_record_count,
         normalized_record_count=EXCLUDED.normalized_record_count,
         matched_property_count=EXCLUDED.matched_property_count,
         unmatched_property_count=EXCLUDED.unmatched_property_count,
         committed_at=now()`,
      [
        manifest.jobId,
        chunkIndex,
        hashArcgisObjectIds(objectIds),
        rawSha256,
        normalizedSha256,
        objectIds.length,
        records.length,
        matchedPropertyCount,
        unmatchedPropertyCount,
      ],
    );
    await client.query(
      `UPDATE ${CONTROL_SCHEMA}.broward_bulk_permit_runs
       SET committed_chunk_count=(
             SELECT count(*) FROM ${CONTROL_SCHEMA}.broward_bulk_permit_chunks
             WHERE job_id=$1
           ),
           committed_source_record_count=(
             SELECT coalesce(sum(source_record_count),0)
             FROM ${CONTROL_SCHEMA}.broward_bulk_permit_chunks
             WHERE job_id=$1
           ),
           heartbeat_at=now()
       WHERE job_id=$1`,
      [manifest.jobId],
    );
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
  return { matchedPropertyCount, unmatchedPropertyCount };
}

/**
 * Map one normalized record to the JSON recordset used by the chunk upsert.
 *
 * @param {NormalizedBrowardArcgisPermit} record - Valid normalized source row.
 * @param {{propertyId:string,parcelId:string} | undefined} parent - Exact parent.
 * @param {string | undefined} [existingPortalRecordKey] - Existing rich portal key.
 * @returns {BulkPermitLoadRow} Complete parameter row.
 */
export function mapBulkPermitLoadRow(
  record,
  parent,
  existingPortalRecordKey,
) {
  const sourceRequestUrl = new URL(
    FORT_LAUDERDALE_PERMIT_ARCGIS_SOURCE.queryUrl,
  );
  sourceRequestUrl.searchParams.set(
    "where",
    `OBJECTID=${record.source_record_id}`,
  );
  sourceRequestUrl.searchParams.set("outFields", "*");
  sourceRequestUrl.searchParams.set("returnGeometry", "false");
  sourceRequestUrl.searchParams.set("f", "json");
  const sourceRecordHash = stableHash(record);
  const sourceRecordKey = existingPortalRecordKey ?? record.record_key;
  return {
    property_id: parent?.propertyId ?? null,
    parcel_id: parent?.parcelId ?? null,
    request_identifier: sourceRecordKey,
    permit_number: record.permit_number,
    improvement_type: record.record_type,
    improvement_status: record.record_status,
    application_received_date: record.application_date,
    estimated_job_value: record.job_value,
    source: record.source_system,
    source_url: record.source_url,
    record_type: record.record_type,
    record_status: record.record_status,
    opened_date: record.application_date,
    work_location: record.work_location,
    parcel_identifier: record.parcel_identifier,
    applicant: record.applicant,
    licensed_professional:
      record.contractor_name === null
        ? record.contractor_license
        : record.contractor_license === null
          ? record.contractor_name
          : `${record.contractor_name} (${record.contractor_license})`,
    project_description: record.project_description,
    description: record.project_description,
    more_details: {
      bulk_transport: record.source_vendor,
      bulk_source_object_id: record.source_record_id,
      approved_date: record.approved_date,
      certificate_of_occupancy_date:
        record.certificate_of_occupancy_date,
      contractor_name: record.contractor_name,
      contractor_license: record.contractor_license,
      is_roof_permit: record.is_roof_permit,
      source_last_updated_at: record.source_last_updated_at,
      source_sync_at: record.source_sync_at,
      bulk_source_payload: record.source_payload,
      bulk_provenance: record.provenance,
    },
    source_http_request: {
      method: "GET",
      url: sourceRequestUrl.toString(),
    },
    source_payload:
      /** @type {Record<string, unknown>} */ (
        /** @type {unknown} */ (record)
      ),
    source_system: record.source_system,
    source_record_key: sourceRecordKey,
    source_record_hash: sourceRecordHash,
    source_artifact_uri: record.source_url,
    property_match_method:
      parent === undefined ? "unmatched" : "exact_folio",
    property_match_confidence:
      parent === undefined ? "unmatched" : "exact",
    retrieved_at: record.retrieved_at,
  };
}

const BULK_UPSERT_SQL = `
  INSERT INTO public.property_improvements AS existing (
    property_id,parcel_id,request_identifier,permit_number,improvement_type,
    improvement_status,improvement_action,application_received_date,
    estimated_job_value,source,source_url,record_type,source_status,
    record_status,opened_date,work_location,parcel_identifier,applicant,
    licensed_professional,project_description,description,more_details,
    source_http_request,source_payload,source_system,source_record_key,
    source_record_hash,source_artifact_uri,property_match_method,
    property_match_confidence,retrieved_at
  )
  SELECT
    input.property_id,input.parcel_id,input.request_identifier,
    input.permit_number,input.improvement_type,input.improvement_status,
    'permit_record',input.application_received_date,input.estimated_job_value,
    input.source,input.source_url,input.record_type,input.improvement_status,
    input.record_status,input.opened_date,input.work_location,
    input.parcel_identifier,input.applicant,input.licensed_professional,
    input.project_description,input.description,input.more_details,
    input.source_http_request,input.source_payload,input.source_system,
    input.source_record_key,input.source_record_hash,input.source_artifact_uri,
    input.property_match_method,input.property_match_confidence,
    input.retrieved_at
  FROM jsonb_to_recordset($1::jsonb) AS input(
    property_id uuid,parcel_id uuid,request_identifier text,permit_number text,
    improvement_type text,improvement_status text,
    application_received_date date,estimated_job_value numeric,source text,
    source_url text,record_type text,record_status text,opened_date date,
    work_location text,parcel_identifier text,applicant text,
    licensed_professional text,project_description text,description text,
    more_details jsonb,source_http_request jsonb,source_payload jsonb,
    source_system text,source_record_key text,source_record_hash text,
    source_artifact_uri text,property_match_method text,
    property_match_confidence text,retrieved_at timestamptz
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
    estimated_job_value=coalesce(
      EXCLUDED.estimated_job_value,
      existing.estimated_job_value
    ),
    source_url=coalesce(existing.source_url,EXCLUDED.source_url),
    record_type=coalesce(EXCLUDED.record_type,existing.record_type),
    source_status=coalesce(EXCLUDED.source_status,existing.source_status),
    record_status=coalesce(EXCLUDED.record_status,existing.record_status),
    opened_date=coalesce(EXCLUDED.opened_date,existing.opened_date),
    work_location=coalesce(EXCLUDED.work_location,existing.work_location),
    parcel_identifier=coalesce(
      EXCLUDED.parcel_identifier,
      existing.parcel_identifier
    ),
    applicant=coalesce(existing.applicant,EXCLUDED.applicant),
    licensed_professional=coalesce(
      existing.licensed_professional,
      EXCLUDED.licensed_professional
    ),
    project_description=coalesce(
      EXCLUDED.project_description,
      existing.project_description
    ),
    description=coalesce(EXCLUDED.description,existing.description),
    more_details=coalesce(existing.more_details,'{}'::jsonb)
      || EXCLUDED.more_details,
    source_http_request=CASE
      WHEN existing.source_payload ? 'schemaVersion'
        THEN existing.source_http_request
      ELSE EXCLUDED.source_http_request
    END,
    source_payload=CASE
      WHEN existing.source_payload ? 'schemaVersion'
        THEN existing.source_payload
      ELSE EXCLUDED.source_payload
    END,
    source_record_hash=CASE
      WHEN existing.source_payload ? 'schemaVersion'
        THEN existing.source_record_hash
      ELSE EXCLUDED.source_record_hash
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
    retrieved_at=greatest(existing.retrieved_at,EXCLUDED.retrieved_at),
    loaded_at=now(),
    updated_at=now()
`;

/**
 * Create additive durable control tables after Neon identity verification.
 *
 * @param {import("pg").Client} client - Verified direct Neon client.
 * @returns {Promise<void>} Resolves after idempotent DDL.
 */
async function ensureControlTables(client) {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${CONTROL_SCHEMA}`);
  await client.query(
    `CREATE TABLE IF NOT EXISTS ${CONTROL_SCHEMA}.broward_bulk_permit_runs (
       job_id text PRIMARY KEY,
       source_system text NOT NULL,
       source_url text NOT NULL,
       source_object_id_count integer NOT NULL CHECK (source_object_id_count > 0),
       source_object_ids_sha256 text NOT NULL CHECK (
         source_object_ids_sha256 ~ '^[a-f0-9]{64}$'
       ),
       selected_object_id_count integer NOT NULL CHECK (
         selected_object_id_count > 0
       ),
       selected_object_ids_sha256 text NOT NULL CHECK (
         selected_object_ids_sha256 ~ '^[a-f0-9]{64}$'
       ),
       chunk_size integer NOT NULL CHECK (chunk_size > 0),
       committed_chunk_count integer NOT NULL DEFAULT 0,
       committed_source_record_count integer NOT NULL DEFAULT 0,
       started_at timestamptz NOT NULL DEFAULT now(),
       heartbeat_at timestamptz NOT NULL DEFAULT now()
     )`,
  );
  await client.query(
    `CREATE TABLE IF NOT EXISTS ${CONTROL_SCHEMA}.broward_bulk_permit_chunks (
       job_id text NOT NULL REFERENCES
         ${CONTROL_SCHEMA}.broward_bulk_permit_runs(job_id),
       chunk_index integer NOT NULL CHECK (chunk_index >= 0),
       object_ids_sha256 text NOT NULL CHECK (
         object_ids_sha256 ~ '^[a-f0-9]{64}$'
       ),
       raw_sha256 text NOT NULL CHECK (raw_sha256 ~ '^[a-f0-9]{64}$'),
       normalized_sha256 text NOT NULL CHECK (
         normalized_sha256 ~ '^[a-f0-9]{64}$'
       ),
       source_record_count integer NOT NULL CHECK (source_record_count > 0),
       normalized_record_count integer NOT NULL CHECK (
         normalized_record_count >= 0
       ),
       matched_property_count integer NOT NULL CHECK (
         matched_property_count >= 0
       ),
       unmatched_property_count integer NOT NULL CHECK (
         unmatched_property_count >= 0
       ),
       committed_at timestamptz NOT NULL DEFAULT now(),
       PRIMARY KEY (job_id,chunk_index)
     )`,
  );
}

/**
 * Register or verify the immutable source snapshot contract.
 *
 * @param {import("pg").Client} client - Verified direct Neon client.
 * @param {BulkPermitManifest} manifest - Current source snapshot.
 * @returns {Promise<void>} Resolves only for a matching existing/new run.
 */
async function registerBulkRun(client, manifest) {
  await client.query(
    `INSERT INTO ${CONTROL_SCHEMA}.broward_bulk_permit_runs (
       job_id,source_system,source_url,source_object_id_count,
       source_object_ids_sha256,selected_object_id_count,
       selected_object_ids_sha256,chunk_size
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     ON CONFLICT (job_id) DO NOTHING`,
    [
      manifest.jobId,
      manifest.sourceSystem,
      manifest.sourceUrl,
      manifest.sourceObjectIdCount,
      manifest.sourceObjectIdsSha256,
      manifest.selectedObjectIdCount,
      manifest.selectedObjectIdsSha256,
      manifest.chunkSize,
    ],
  );
  const result = await client.query(
    `SELECT source_system,source_url,source_object_id_count,
            source_object_ids_sha256,selected_object_id_count,
            selected_object_ids_sha256,chunk_size
     FROM ${CONTROL_SCHEMA}.broward_bulk_permit_runs WHERE job_id=$1`,
    [manifest.jobId],
  );
  const row = result.rows[0];
  if (
    row?.source_system !== manifest.sourceSystem ||
    row.source_url !== manifest.sourceUrl ||
    Number(row.source_object_id_count) !== manifest.sourceObjectIdCount ||
    row.source_object_ids_sha256 !== manifest.sourceObjectIdsSha256 ||
    Number(row.selected_object_id_count) !==
      manifest.selectedObjectIdCount ||
    row.selected_object_ids_sha256 !== manifest.selectedObjectIdsSha256 ||
    Number(row.chunk_size) !== manifest.chunkSize
  ) {
    throw new Error("Existing bulk permit run does not match source snapshot");
  }
}

/**
 * Confirm that a local committed receipt has matching durable Neon evidence.
 *
 * @param {import("pg").Client} client - Verified direct Neon client.
 * @param {string} jobId - Stable job identity.
 * @param {number} chunkIndex - Zero-based chunk.
 * @param {string} rawSha256 - Exact raw artifact hash.
 * @returns {Promise<boolean>} Whether the matching commit exists.
 */
async function isBulkChunkCommitted(
  client,
  jobId,
  chunkIndex,
  rawSha256,
) {
  const result = await client.query(
    `SELECT raw_sha256 FROM ${CONTROL_SCHEMA}.broward_bulk_permit_chunks
     WHERE job_id=$1 AND chunk_index=$2`,
    [jobId, chunkIndex],
  );
  return result.rows[0]?.raw_sha256 === rawSha256;
}

/**
 * Acquire the dedicated session-scoped bulk permit writer lock.
 *
 * @param {import("pg").Client} client - Verified direct Neon client.
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
 * Validate the isolated direct Neon target without exposing its credentials.
 *
 * @param {NodeJS.ProcessEnv} environment - Runtime environment.
 * @returns {{connectionString:string,expectedBranchId:string,expectedEndpointId:string}}
 *   Verified-form target configuration.
 */
function requireTarget(environment) {
  const connectionString = environment.DATABASE_URL_UNPOOLED;
  const expectedBranchId = environment.BROWARD_INGEST_NEON_BRANCH_ID;
  const expectedEndpointId = environment.BROWARD_INGEST_NEON_ENDPOINT_ID;
  if (
    typeof connectionString !== "string" ||
    connectionString.length === 0 ||
    typeof expectedBranchId !== "string" ||
    !/^br-[a-z0-9-]+$/u.test(expectedBranchId) ||
    typeof expectedEndpointId !== "string" ||
    !/^ep-[a-z0-9-]+$/u.test(expectedEndpointId) ||
    expectedEndpointId.startsWith(PRODUCTION_ENDPOINT_PREFIX)
  ) {
    throw new Error("Verified direct Broward Neon target is required");
  }
  if (new URL(connectionString).hostname.includes("-pooler")) {
    throw new Error("Bulk permit loading requires a direct Neon endpoint");
  }
  return { connectionString, expectedBranchId, expectedEndpointId };
}

/**
 * Prove the connected Neon project, branch, and endpoint identity.
 *
 * @param {import("pg").Client} client - Connected direct client.
 * @param {{expectedBranchId:string,expectedEndpointId:string}} target - Expected IDs.
 * @returns {Promise<void>} Resolves only for the isolated Broward target.
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
    throw new Error("Bulk permit target is not isolated broward-ingest");
  }
}

/**
 * Render deterministic normalized JSONL.
 *
 * @param {readonly NormalizedBrowardArcgisPermit[]} records - Source-order rows.
 * @returns {string} JSONL with a trailing newline when non-empty.
 */
function renderNormalizedRecords(records) {
  return records.length === 0
    ? ""
    : `${records.map((record) => JSON.stringify(record)).join("\n")}\n`;
}

/**
 * Read a previously checkpointed normalized chunk.
 *
 * @param {string} filePath - Private normalized JSONL path.
 * @returns {Promise<NormalizedBrowardArcgisPermit[]>} Parsed rows.
 */
async function readNormalizedChunk(filePath) {
  const text = await readFile(filePath, "utf8");
  return text
    .split(/\r?\n/u)
    .filter((line) => line.length > 0)
    .map(
      (line) =>
        /** @type {NormalizedBrowardArcgisPermit} */ (JSON.parse(line)),
    );
}

/**
 * Add stable permit keys to whole-run duplicate reconciliation.
 *
 * @param {Set<string>} keys - Mutable whole-run key set.
 * @param {readonly NormalizedBrowardArcgisPermit[]} records - Chunk rows.
 * @returns {void}
 */
function addPermitKeys(keys, records) {
  for (const record of records) keys.add(record.record_key);
}

/**
 * Select one deterministic latest row per portal-compatible permit key.
 *
 * Duplicate source rows remain counted in reconciliation and in private raw
 * artifacts. Only the latest variant is submitted in a single PostgreSQL
 * upsert statement so one conflict key is never affected twice.
 *
 * @param {readonly NormalizedBrowardArcgisPermit[]} records - Source rows.
 * @returns {NormalizedBrowardArcgisPermit[]} Unique latest logical permits.
 */
function dedupeBulkPermitRecords(records) {
  /** @type {Map<string, NormalizedBrowardArcgisPermit>} */
  const byKey = new Map();
  for (const record of records) {
    const existing = byKey.get(record.record_key);
    if (
      existing === undefined ||
      (record.source_last_updated_at ?? "") >
        (existing.source_last_updated_at ?? "") ||
      ((record.source_last_updated_at ?? "") ===
        (existing.source_last_updated_at ?? "") &&
        Number(record.source_record_id) > Number(existing.source_record_id))
    ) {
      byKey.set(record.record_key, record);
    }
  }
  return [...byKey.values()];
}

/**
 * Atomically write a mode-0600 private artifact.
 *
 * @param {string} filePath - Final private artifact path.
 * @param {string} content - Complete replacement content.
 * @returns {Promise<void>} Resolves after atomic replacement.
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

/**
 * Return a stable hash of arbitrary text.
 *
 * @param {string} value - Exact bytes represented as UTF-8 text.
 * @returns {string} Lowercase SHA-256.
 */
function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

/**
 * Stable JSON hash with recursively sorted object keys.
 *
 * @param {unknown} value - JSON-compatible normalized record.
 * @returns {string} Lowercase SHA-256.
 */
function stableHash(value) {
  return sha256(stableJson(value));
}

/**
 * Serialize JSON-compatible values with recursively sorted object keys.
 *
 * @param {unknown} value - JSON-compatible value.
 * @returns {string} Deterministically ordered JSON text.
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
 * Format a chunk index for lexical file ordering.
 *
 * @param {number} index - Zero-based chunk index.
 * @returns {string} Six-digit index.
 */
function formatChunkIndex(index) {
  return String(index).padStart(6, "0");
}

/**
 * Parse an integer within an inclusive range.
 *
 * @param {string} raw - Raw CLI value.
 * @param {string} name - Option name without dashes.
 * @param {number} minimum - Inclusive minimum.
 * @param {number} maximum - Inclusive maximum.
 * @returns {number} Validated integer.
 */
function boundedInteger(raw, name, minimum, maximum) {
  const value = Number(raw);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(
      `--${name} must be an integer from ${String(minimum)} through ${String(maximum)}`,
    );
  }
  return value;
}

/**
 * Narrow an unknown error to a Node error with a string code.
 *
 * @param {unknown} value - Caught value.
 * @returns {value is Error & {code:string}} Whether a string code exists.
 */
function isNodeError(value) {
  return (
    value instanceof Error && "code" in value && typeof value.code === "string"
  );
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  runBrowardBulkPermitIngest(
    parseBulkPermitOptions(process.argv.slice(2)),
  )
    .then((manifest) => {
      console.log(
        JSON.stringify({
          event: "broward_bulk_permit_ingest_finished",
          jobId: manifest.jobId,
          sourceSystem: manifest.sourceSystem,
          reconciliation: manifest.reconciliation,
        }),
      );
    })
    .catch((error) => {
      console.error(
        JSON.stringify({
          event: "broward_bulk_permit_ingest_failed",
          message: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      process.exitCode = 1;
    });
}
