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
const PEMBROKE_PARK_GOV_EASY_SEARCH_URL =
  "https://apps.gov-easy.com/Home/PermitInspection/Search?clientId=d60f9827-2c53-44a4-9037-31e1de2b3f09";
const CORAL_SPRINGS_ETRAKIT_SEARCH_URL =
  "https://etrakit.coralsprings.gov/eTRAKiT/Search/permit.aspx";
const CORAL_SPRINGS_ETRAKIT_SOURCE_SYSTEM =
  "broward_coral_springs_etrakit_permits";

/**
 * @typedef {object} PermitListLoadOptions
 * @property {string} jobId - Stable immutable load identity.
 * @property {string} inputPath - Completed normalized-list private JSONL.
 * @property {number} chunkSize - Rows per durable Neon transaction.
 * @property {string | null} [incrementalManifestPath] - Strict partial-inventory provenance, when used.
 * @property {number} [lockWaitSeconds] - Finite shared-writer lock wait.
 *
 * @typedef {object} IncrementalPermitManifest
 * @property {"oracle-node.broward-permit-incremental-manifest.v1"} schemaVersion
 *   Strict partial-inventory manifest schema.
 * @property {string} sourceSystem - Single stable source represented by the input.
 * @property {string} frozenAt - ISO instant at which the source checkpoint was read.
 * @property {"partial_terminal_artifacts"} coverageBoundary - Explicit non-complete coverage.
 * @property {string} checkpointSha256 - Exact frozen checkpoint digest.
 * @property {string} listSha256 - Exact private load-list digest.
 * @property {string} artifactManifestSha256 - Digest of ordered terminal artifact receipts.
 * @property {number} artifactCount - Terminal artifacts represented.
 * @property {number} artifactRecordCount - Artifact observations before source-key dedupe.
 * @property {number} eligibleRecordCount - Unique rows in the private load list.
 * @property {Readonly<Record<string, number>>} excludedCounts - Aggregate-only exclusions by reason.
 * @property {Record<string, unknown> | null} priorHighWatermark - Previously committed source watermark.
 * @property {Record<string, unknown>} highWatermark - Exact frozen source cursor.
 *
 * @typedef {object} ValidatedIncrementalPermitManifest
 * @property {IncrementalPermitManifest} manifest - Validated manifest.
 * @property {string} manifestSha256 - Exact manifest bytes digest.
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
 * @typedef {object} MunicipalPartialRecord
 * @property {string} source_system - Jurisdiction source system.
 * @property {string} source_protocol - Source protocol family.
 * @property {string} source_url - Official detail URL.
 * @property {string} source_record_id - Stable vendor identity.
 * @property {string} record_key - Source-system-qualified stable identity.
 * @property {string} jurisdiction - Issuing municipality.
 * @property {string} permit_number - Public permit/application number.
 * @property {string | null} parcel_identifier - Source parcel display.
 * @property {string | null} work_location - Public project location.
 * @property {string | null} application_date - ISO application date.
 * @property {string | null} permit_issue_date - ISO issue date.
 * @property {string | null} expiration_date - ISO expiration date.
 * @property {string | null} record_status - Public record status.
 * @property {string | null} record_type - Public record type.
 * @property {string | null} project_description - Public project description.
 * @property {boolean} is_roof_permit - Conservative source-text classification.
 * @property {Readonly<Record<string, unknown>>} raw - Allow-listed source fields.
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
 * @typedef {object} GovEasyListRecord
 * @property {"oracle-node.broward-gov-easy-list.v1"} schemaVersion - Gov-Easy list schema.
 * @property {string} sourceSystem - Broward-prefixed jurisdiction source system.
 * @property {"Pembroke Park"} jurisdiction - Issuing jurisdiction.
 * @property {string} sourceRecordId - Stable Gov-Easy application identity.
 * @property {string} recordKey - Source-system and application identity key.
 * @property {string} permitNumber - Public permit number.
 * @property {string | null} jobName - Public job-name list field.
 * @property {string | null} status - Public permit status.
 * @property {string | null} address - Public job location.
 * @property {string} sourceUrl - Official token-free Gov-Easy search URL.
 * @property {number} sourcePage - One-based source result page.
 * @property {boolean} isRoofPermit - Conservative standalone-word classification.
 * @property {Readonly<{
 *   queryField:"Job Name",
 *   queryValue:"ROOF",
 *   sourceReportedCount:number
 * }>} coverage - Exact keyword-slice provenance and reported denominator.
 *
 * @typedef {object} EtrakitListRecord
 * @property {"oracle-node.broward-etrakit-list.v1"} schemaVersion
 *   Privacy-minimized list schema.
 * @property {"broward_coral_springs_etrakit_permits"} sourceSystem
 *   Stable source system.
 * @property {"Coral Springs"} jurisdiction - Issuing jurisdiction.
 * @property {string} sourceRecordId - Stable eTRAKiT RECORDID.
 * @property {string} recordKey - Source-system-qualified stable identity.
 * @property {string} permitNumber - Public permit number.
 * @property {string | null} recordType - Public permit type.
 * @property {string | null} status - Public permit status.
 * @property {string | null} address - Public site address.
 * @property {string | null} folio - Public list folio.
 * @property {string} sourceUrl - Token-free official search URL.
 * @property {number[]} sourcePages - One-based exposed result pages.
 * @property {boolean} isRoofPermit - Source-query-backed classification.
 * @property {Readonly<{
 *   queryField:"Permit Type",
 *   queryOperator:"Contains",
 *   queryValue:"ROOF",
 *   sourceReportedCount:59379,
 *   exposedRecordCap:1000,
 *   exposedPageCount:50,
 *   pageSize:20,
 *   completenessBoundary:"bounded_capped_keyword_slice",
 *   countEvidence:"operator_observed_source_result"
 * }>} coverage - Explicit capped coverage boundary.
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
  const allowed = new Set([
    "job-id",
    "input",
    "chunk-size",
    "incremental-manifest",
    "lock-wait-seconds",
  ]);
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
    const name = flag.slice(2);
    if (!allowed.has(name) || values.has(name)) {
      throw new Error(
        "Permit list load options must be unique supported flags",
      );
    }
    values.set(name, value);
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
  const incrementalManifestPath = values.get("incremental-manifest") ?? null;
  if (incrementalManifestPath !== null && chunkSize > 1_000) {
    throw new Error("Incremental permit chunks cannot exceed 1000 rows");
  }
  const lockWaitSeconds = Number(values.get("lock-wait-seconds") ?? "0");
  if (
    !Number.isInteger(lockWaitSeconds) ||
    lockWaitSeconds < 0 ||
    lockWaitSeconds > 3_600
  ) {
    throw new Error(
      "--lock-wait-seconds must be an integer from 0 through 3600",
    );
  }
  return {
    jobId,
    inputPath,
    chunkSize,
    incrementalManifestPath,
    lockWaitSeconds,
  };
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
  const records = [...byKey.values()]
    .map((entry) => entry.record)
    .sort((left, right) =>
      left.sourceRecordKey.localeCompare(right.sourceRecordKey),
    );
  validateCompletedEtrakitSlice(records, sourceCount, sourceCount - byKey.size);
  return {
    records,
    inputSha256: createHash("sha256").update(text).digest("hex"),
    duplicateCount: sourceCount - byKey.size,
  };
}

/**
 * Read and prove a strict incremental manifest against its private load list.
 *
 * The manifest is intentionally coverage-neutral: it proves that all accepted
 * rows belong to immutable terminal artifacts at one checkpoint watermark,
 * but it cannot mark a jurisdiction or county complete.
 *
 * @param {string} manifestPath - Private aggregate-only manifest path.
 * @param {{records:NormalizedPermitListRecord[],inputSha256:string,duplicateCount:number}} input
 *   Validated immutable list input.
 * @returns {Promise<ValidatedIncrementalPermitManifest>} Proven manifest.
 */
export async function readIncrementalPermitManifest(manifestPath, input) {
  const text = await readFile(manifestPath, "utf8");
  const value = /** @type {unknown} */ (JSON.parse(text));
  if (!isRecord(value)) {
    throw new Error("Incremental permit manifest must be an object");
  }
  const excludedCounts = value.excludedCounts;
  const priorHighWatermark = value.priorHighWatermark;
  const highWatermark = value.highWatermark;
  const validExcludedCounts =
    isRecord(excludedCounts) &&
    Object.keys(excludedCounts).length > 0 &&
    Object.values(excludedCounts).every(
      (count) => Number.isSafeInteger(count) && Number(count) >= 0,
    );
  if (
    value.schemaVersion !==
      "oracle-node.broward-permit-incremental-manifest.v1" ||
    typeof value.sourceSystem !== "string" ||
    !/^broward_[a-z0-9_]+$/u.test(value.sourceSystem) ||
    typeof value.frozenAt !== "string" ||
    !Number.isFinite(Date.parse(value.frozenAt)) ||
    value.coverageBoundary !== "partial_terminal_artifacts" ||
    !isSha256(value.checkpointSha256) ||
    !isSha256(value.listSha256) ||
    !isSha256(value.artifactManifestSha256) ||
    !Number.isSafeInteger(value.artifactCount) ||
    Number(value.artifactCount) < 1 ||
    !Number.isSafeInteger(value.artifactRecordCount) ||
    Number(value.artifactRecordCount) < 1 ||
    !Number.isSafeInteger(value.eligibleRecordCount) ||
    Number(value.eligibleRecordCount) < 1 ||
    !validExcludedCounts ||
    (priorHighWatermark !== null && !isRecord(priorHighWatermark)) ||
    !isRecord(highWatermark) ||
    Object.keys(highWatermark).length === 0
  ) {
    throw new Error("Incremental permit manifest is malformed");
  }
  if (
    value.listSha256 !== input.inputSha256 ||
    Number(value.eligibleRecordCount) !== input.records.length ||
    input.duplicateCount !== 0 ||
    input.records.some(
      (record) => record.sourceSystem !== value.sourceSystem,
    ) ||
    Number(value.artifactRecordCount) < input.records.length
  ) {
    throw new Error(
      "Incremental permit manifest does not reconcile with its list",
    );
  }
  return {
    manifest: /** @type {IncrementalPermitManifest} */ (value),
    manifestSha256: createHash("sha256").update(text).digest("hex"),
  };
}

/**
 * Require the Coral Springs input to be the fully reconciled exposed slice.
 *
 * This proves all 50 source pages are represented exactly once before any row
 * is loadable. It deliberately does not upgrade the capped keyword slice to
 * complete roofing or jurisdiction coverage.
 *
 * @param {readonly NormalizedPermitListRecord[]} records - Unique input rows.
 * @param {number} sourceCount - Parsed JSONL rows before deduplication.
 * @param {number} duplicateCount - Exact duplicate input rows.
 * @returns {void}
 */
function validateCompletedEtrakitSlice(records, sourceCount, duplicateCount) {
  const etrakitRecords = records.filter(
    (record) => record.sourceSystem === CORAL_SPRINGS_ETRAKIT_SOURCE_SYSTEM,
  );
  if (etrakitRecords.length === 0) return;
  if (
    etrakitRecords.length !== records.length ||
    etrakitRecords.length !== 1_000 ||
    sourceCount !== 1_000 ||
    duplicateCount !== 0
  ) {
    throw new Error(
      "Coral Springs eTRAKiT input is not the reconciled 1000-row exposed slice",
    );
  }
  /** @type {Map<number, number>} */
  const pageCounts = new Map();
  for (const record of etrakitRecords) {
    const payload = record.sourcePayload;
    const pages = payload.sourcePages;
    if (
      !Array.isArray(pages) ||
      pages.length !== 1 ||
      !Number.isInteger(pages[0])
    ) {
      throw new Error("Coral Springs eTRAKiT page provenance is incomplete");
    }
    const page = /** @type {number} */ (pages[0]);
    pageCounts.set(page, (pageCounts.get(page) ?? 0) + 1);
  }
  for (let page = 1; page <= 50; page += 1) {
    if (pageCounts.get(page) !== 20) {
      throw new Error(
        "Coral Springs eTRAKiT page provenance does not reconcile",
      );
    }
  }
  if (pageCounts.size !== 50) {
    throw new Error("Coral Springs eTRAKiT contains an out-of-range page");
  }
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
  if (isGovEasyListRecord(value)) {
    return {
      sourceSystem: value.sourceSystem,
      sourceRecordKey: value.recordKey,
      permitNumber: value.permitNumber,
      sourceUrl: value.sourceUrl,
      jurisdiction: value.jurisdiction,
      parcelIdentifier: null,
      workLocation: value.address,
      applicationDate: null,
      permitIssueDate: null,
      expirationDate: null,
      finalizedDate: null,
      recordStatus: value.status,
      recordType: null,
      description: value.jobName,
      isRoofPermit: value.isRoofPermit,
      sourcePayload: {
        schema_version: "oracle-node.broward-gov-easy-list.v1",
        ...value,
      },
    };
  }
  if (isEtrakitListRecord(value)) {
    return {
      sourceSystem: value.sourceSystem,
      sourceRecordKey: value.recordKey,
      permitNumber: value.permitNumber,
      sourceUrl: value.sourceUrl,
      jurisdiction: value.jurisdiction,
      parcelIdentifier: normalizeArcgisBrowardFolio(value.folio),
      workLocation: value.address,
      applicationDate: null,
      permitIssueDate: null,
      expirationDate: null,
      finalizedDate: null,
      recordStatus: value.status,
      recordType: value.recordType,
      description: null,
      isRoofPermit: value.isRoofPermit,
      sourcePayload: {
        schema_version: "oracle-node.broward-etrakit-list.v1",
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
  if (isMunicipalPartialRecord(value)) {
    return {
      sourceSystem: value.source_system,
      sourceRecordKey: value.record_key,
      permitNumber: value.permit_number,
      sourceUrl: value.source_url,
      jurisdiction: value.jurisdiction,
      parcelIdentifier: normalizeArcgisBrowardFolio(value.parcel_identifier),
      workLocation: value.work_location,
      applicationDate: value.application_date,
      permitIssueDate: value.permit_issue_date,
      expirationDate: value.expiration_date,
      finalizedDate: null,
      recordStatus: value.record_status,
      recordType: value.record_type,
      description: value.project_description,
      isRoofPermit: value.is_roof_permit,
      sourcePayload: {
        schema_version: "oracle-node.broward-municipal-partial-list.v1",
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
    source_http_request:
      record.sourceSystem === CORAL_SPRINGS_ETRAKIT_SOURCE_SYSTEM
        ? {
            method: "POST",
            url: record.sourceUrl,
            access: "manual_captcha_authorized_session",
            payload_persisted: false,
          }
        : { method: "GET", url: record.sourceUrl },
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
 *   insertedRecordCount:number,
 *   updatedRecordCount:number,
 *   roofingRecordCount:number,
 *   committedChunkCount:number,
 *   inputSha256:string,
 *   incrementalManifestSha256:string|null
 * }>} Reconciled load result.
 */
export async function loadPermitListToNeon(options) {
  const input = await readPermitListRecords(options.inputPath);
  const incrementalManifestPath = options.incrementalManifestPath ?? null;
  const incremental =
    incrementalManifestPath === null
      ? null
      : await readIncrementalPermitManifest(incrementalManifestPath, input);
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
    await acquireLoadLock(client, options.lockWaitSeconds ?? 0);
    await ensureControlTables(client);
    await registerRun(client, options, input, incremental);
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
              coalesce(sum(unmatched_count),0)::integer AS unmatched,
              coalesce(sum(inserted_count),0)::integer AS inserted,
              coalesce(sum(updated_count),0)::integer AS updated,
              coalesce(sum(roofing_count),0)::integer AS roofing
       FROM ${CONTROL_SCHEMA}.broward_permit_list_load_chunks
       WHERE job_id=$1`,
      [options.jobId],
    );
    const row = aggregate.rows[0];
    if (Number(row?.records) !== input.records.length) {
      throw new Error("Broward permit list chunk receipts do not reconcile");
    }
    await finalizeRun(client, options.jobId, incremental);
    await refreshBrowardDashboardRollup(client);
    return {
      sourceRecordCount: input.records.length + input.duplicateCount,
      uniqueRecordCount: input.records.length,
      duplicateRecordCount: input.duplicateCount,
      matchedRecordCount: Number(row.matched),
      unmatchedRecordCount: Number(row.unmatched),
      insertedRecordCount: Number(row.inserted),
      updatedRecordCount: Number(row.updated),
      roofingRecordCount: Number(row.roofing),
      committedChunkCount: Number(row.chunks),
      inputSha256: input.inputSha256,
      incrementalManifestSha256: incremental?.manifestSha256 ?? null,
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
  const existingResult = await client.query(
    `SELECT existing.source_system,existing.source_record_key
     FROM public.property_improvements AS existing
     INNER JOIN jsonb_to_recordset($1::jsonb) AS input(
       source_system text,source_record_key text
     )
       ON input.source_system=existing.source_system
      AND input.source_record_key=existing.source_record_key`,
    [
      JSON.stringify(
        loadRows.map((row) => ({
          source_system: row.source_system,
          source_record_key: row.source_record_key,
        })),
      ),
    ],
  );
  const updated = existingResult.rows.length;
  const inserted = records.length - updated;
  const roofing = records.filter((record) => record.isRoofPermit).length;
  await client.query("BEGIN");
  try {
    await client.query(PERMIT_LIST_UPSERT_SQL, [JSON.stringify(loadRows)]);
    await client.query(
      `INSERT INTO ${CONTROL_SCHEMA}.broward_permit_list_load_chunks (
         job_id,chunk_index,record_count,matched_count,unmatched_count,
         inserted_count,updated_count,roofing_count
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
      [
        jobId,
        chunkIndex,
        records.length,
        matched,
        records.length - matched,
        inserted,
        updated,
        roofing,
      ],
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
    `ALTER TABLE ${CONTROL_SCHEMA}.broward_permit_list_load_runs
       ADD COLUMN IF NOT EXISTS incremental_manifest_sha256 text,
       ADD COLUMN IF NOT EXISTS source_system text,
       ADD COLUMN IF NOT EXISTS checkpoint_sha256 text,
       ADD COLUMN IF NOT EXISTS list_sha256 text,
       ADD COLUMN IF NOT EXISTS artifact_manifest_sha256 text,
       ADD COLUMN IF NOT EXISTS prior_high_watermark jsonb,
       ADD COLUMN IF NOT EXISTS high_watermark jsonb,
       ADD COLUMN IF NOT EXISTS excluded_counts jsonb`,
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
  await client.query(
    `ALTER TABLE ${CONTROL_SCHEMA}.broward_permit_list_load_chunks
       ADD COLUMN IF NOT EXISTS inserted_count integer NOT NULL DEFAULT 0,
       ADD COLUMN IF NOT EXISTS updated_count integer NOT NULL DEFAULT 0,
       ADD COLUMN IF NOT EXISTS roofing_count integer NOT NULL DEFAULT 0`,
  );
  await client.query(
    `CREATE TABLE IF NOT EXISTS ${CONTROL_SCHEMA}.broward_permit_incremental_watermarks (
       source_system text PRIMARY KEY,
       manifest_sha256 text NOT NULL CHECK (
         manifest_sha256 ~ '^[a-f0-9]{64}$'
       ),
       checkpoint_sha256 text NOT NULL CHECK (
         checkpoint_sha256 ~ '^[a-f0-9]{64}$'
       ),
       list_sha256 text NOT NULL CHECK (list_sha256 ~ '^[a-f0-9]{64}$'),
       artifact_manifest_sha256 text NOT NULL CHECK (
         artifact_manifest_sha256 ~ '^[a-f0-9]{64}$'
       ),
       high_watermark jsonb NOT NULL,
       job_id text NOT NULL REFERENCES
         ${CONTROL_SCHEMA}.broward_permit_list_load_runs(job_id),
       committed_at timestamptz NOT NULL DEFAULT now()
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
 * @param {ValidatedIncrementalPermitManifest | null} incremental
 *   Optional strict partial-inventory provenance.
 * @returns {Promise<void>} Resolves only for matching run identity.
 */
async function registerRun(client, options, input, incremental) {
  if (incremental !== null) {
    const watermarkResult = await client.query(
      `SELECT high_watermark,manifest_sha256
       FROM ${CONTROL_SCHEMA}.broward_permit_incremental_watermarks
       WHERE source_system=$1`,
      [incremental.manifest.sourceSystem],
    );
    const currentWatermark = watermarkResult.rows[0]?.high_watermark;
    const priorMatches =
      currentWatermark === undefined
        ? incremental.manifest.priorHighWatermark === null
        : stableJson(currentWatermark) ===
          stableJson(incremental.manifest.priorHighWatermark);
    const resumedCompletedManifest =
      currentWatermark !== undefined &&
      watermarkResult.rows[0]?.manifest_sha256 === incremental.manifestSha256 &&
      stableJson(currentWatermark) ===
        stableJson(incremental.manifest.highWatermark);
    if (!priorMatches && !resumedCompletedManifest) {
      throw new Error(
        "Incremental permit manifest does not continue the committed watermark",
      );
    }
  }
  await client.query(
    `INSERT INTO ${CONTROL_SCHEMA}.broward_permit_list_load_runs (
       job_id,input_path,input_sha256,unique_record_count,
       duplicate_record_count,chunk_size,status,
       incremental_manifest_sha256,source_system,checkpoint_sha256,
       list_sha256,artifact_manifest_sha256,prior_high_watermark,
       high_watermark,excluded_counts
     ) VALUES ($1,$2,$3,$4,$5,$6,'running',$7,$8,$9,$10,$11,$12,$13,$14)
     ON CONFLICT (job_id) DO NOTHING`,
    [
      options.jobId,
      options.inputPath,
      input.inputSha256,
      input.records.length,
      input.duplicateCount,
      options.chunkSize,
      incremental?.manifestSha256 ?? null,
      incremental?.manifest.sourceSystem ?? null,
      incremental?.manifest.checkpointSha256 ?? null,
      incremental?.manifest.listSha256 ?? null,
      incremental?.manifest.artifactManifestSha256 ?? null,
      incremental?.manifest.priorHighWatermark ?? null,
      incremental?.manifest.highWatermark ?? null,
      incremental?.manifest.excludedCounts ?? null,
    ],
  );
  const result = await client.query(
    `SELECT input_path,input_sha256,unique_record_count,
            duplicate_record_count,chunk_size,incremental_manifest_sha256,
            source_system,checkpoint_sha256,list_sha256,
            artifact_manifest_sha256,prior_high_watermark,
            high_watermark,excluded_counts
     FROM ${CONTROL_SCHEMA}.broward_permit_list_load_runs WHERE job_id=$1`,
    [options.jobId],
  );
  const row = result.rows[0];
  if (
    row?.input_path !== options.inputPath ||
    row.input_sha256 !== input.inputSha256 ||
    Number(row.unique_record_count) !== input.records.length ||
    Number(row.duplicate_record_count) !== input.duplicateCount ||
    Number(row.chunk_size) !== options.chunkSize ||
    (row.incremental_manifest_sha256 ?? null) !==
      (incremental?.manifestSha256 ?? null) ||
    (row.source_system ?? null) !==
      (incremental?.manifest.sourceSystem ?? null) ||
    (row.checkpoint_sha256 ?? null) !==
      (incremental?.manifest.checkpointSha256 ?? null) ||
    (row.list_sha256 ?? null) !== (incremental?.manifest.listSha256 ?? null) ||
    (row.artifact_manifest_sha256 ?? null) !==
      (incremental?.manifest.artifactManifestSha256 ?? null) ||
    stableJson(row.prior_high_watermark ?? null) !==
      stableJson(incremental?.manifest.priorHighWatermark ?? null) ||
    stableJson(row.high_watermark ?? null) !==
      stableJson(incremental?.manifest.highWatermark ?? null) ||
    stableJson(row.excluded_counts ?? null) !==
      stableJson(incremental?.manifest.excludedCounts ?? null)
  ) {
    throw new Error("Existing permit list load does not match input");
  }
}

/**
 * Complete one reconciled run and advance its strict source watermark.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @param {string} jobId - Stable immutable run identity.
 * @param {ValidatedIncrementalPermitManifest | null} incremental
 *   Optional strict partial-inventory provenance.
 * @returns {Promise<void>} Resolves after atomic completion metadata.
 */
async function finalizeRun(client, jobId, incremental) {
  await client.query("BEGIN");
  try {
    await client.query(
      `UPDATE ${CONTROL_SCHEMA}.broward_permit_list_load_runs
       SET status='complete',completed_at=coalesce(completed_at,now()),
           heartbeat_at=now()
       WHERE job_id=$1`,
      [jobId],
    );
    if (incremental !== null) {
      await client.query(
        `INSERT INTO ${CONTROL_SCHEMA}.broward_permit_incremental_watermarks (
           source_system,manifest_sha256,checkpoint_sha256,list_sha256,
           artifact_manifest_sha256,high_watermark,job_id
         ) VALUES ($1,$2,$3,$4,$5,$6,$7)
         ON CONFLICT (source_system) DO UPDATE SET
           manifest_sha256=EXCLUDED.manifest_sha256,
           checkpoint_sha256=EXCLUDED.checkpoint_sha256,
           list_sha256=EXCLUDED.list_sha256,
           artifact_manifest_sha256=EXCLUDED.artifact_manifest_sha256,
           high_watermark=EXCLUDED.high_watermark,
           job_id=EXCLUDED.job_id,
           committed_at=now()`,
        [
          incremental.manifest.sourceSystem,
          incremental.manifestSha256,
          incremental.manifest.checkpointSha256,
          incremental.manifest.listSha256,
          incremental.manifest.artifactManifestSha256,
          incremental.manifest.highWatermark,
          jobId,
        ],
      );
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

/**
 * Acquire the shared permit table writer lock.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @param {number} waitSeconds - Maximum finite lock wait.
 * @returns {Promise<void>} Resolves only for the single writer.
 */
async function acquireLoadLock(client, waitSeconds) {
  const deadline = Date.now() + waitSeconds * 1_000;
  for (;;) {
    const result = await client.query(
      "SELECT pg_try_advisory_lock($1,$2) AS acquired",
      [LOAD_LOCK_NAMESPACE, LOAD_LOCK_KEY],
    );
    if (result.rows[0]?.acquired === true) return;
    if (Date.now() >= deadline) {
      throw new Error("Another Broward permit loader owns the writer lock");
    }
    await new Promise((resolvePromise) => {
      setTimeout(resolvePromise, Math.min(2_000, deadline - Date.now()));
    });
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
 * Validate a manually CAPTCHA-authorized Gov-Easy list row.
 *
 * The row contract is intentionally list-only: CAPTCHA/session material,
 * owner and contractor names, contacts, payments, and subordinate detail-grid
 * payloads are not accepted. Access remains unavailable to unattended
 * transports; this validator only consumes an already completed private
 * inventory.
 *
 * @param {unknown} value - Candidate row.
 * @returns {value is GovEasyListRecord} Whether the allow-listed row is valid.
 */
function isGovEasyListRecord(value) {
  if (!isRecord(value) || !isRecord(value.coverage)) return false;
  if (
    value.schemaVersion !== "oracle-node.broward-gov-easy-list.v1" ||
    value.sourceSystem !== "broward_pembroke_park_gov_easy_permits" ||
    value.jurisdiction !== "Pembroke Park" ||
    typeof value.sourceRecordId !== "string" ||
    !/^\d{1,20}$/u.test(value.sourceRecordId) ||
    value.recordKey !==
      `${value.sourceSystem}:application:${value.sourceRecordId}` ||
    typeof value.permitNumber !== "string" ||
    value.permitNumber.length === 0 ||
    value.sourceUrl !== PEMBROKE_PARK_GOV_EASY_SEARCH_URL ||
    typeof value.sourcePage !== "number" ||
    !Number.isInteger(value.sourcePage) ||
    value.sourcePage < 1 ||
    typeof value.isRoofPermit !== "boolean" ||
    value.coverage.queryField !== "Job Name" ||
    value.coverage.queryValue !== "ROOF" ||
    value.coverage.sourceReportedCount !== 166
  ) {
    return false;
  }
  return [value.jobName, value.status, value.address].every(
    (fieldValue) => fieldValue === null || typeof fieldValue === "string",
  );
}

/**
 * Validate one privacy-minimized Coral Springs eTRAKiT list row.
 *
 * CAPTCHA responses, ViewState, cookies, owner/contractor/contact fields, and
 * detail payloads are intentionally absent from this accepted contract.
 *
 * @param {unknown} value - Candidate parsed JSONL row.
 * @returns {value is EtrakitListRecord} Whether the capped row is valid.
 */
function isEtrakitListRecord(value) {
  if (!isRecord(value) || !isRecord(value.coverage)) return false;
  return (
    value.schemaVersion === "oracle-node.broward-etrakit-list.v1" &&
    value.sourceSystem === CORAL_SPRINGS_ETRAKIT_SOURCE_SYSTEM &&
    value.jurisdiction === "Coral Springs" &&
    typeof value.sourceRecordId === "string" &&
    /^[A-Z0-9_:-]+$/iu.test(value.sourceRecordId) &&
    value.recordKey ===
      `${CORAL_SPRINGS_ETRAKIT_SOURCE_SYSTEM}:record:${value.sourceRecordId}` &&
    typeof value.permitNumber === "string" &&
    value.permitNumber.length > 0 &&
    (value.recordType === null || typeof value.recordType === "string") &&
    (value.status === null || typeof value.status === "string") &&
    (value.address === null || typeof value.address === "string") &&
    (value.folio === null || typeof value.folio === "string") &&
    value.sourceUrl === CORAL_SPRINGS_ETRAKIT_SEARCH_URL &&
    Array.isArray(value.sourcePages) &&
    value.sourcePages.every(
      (page) => Number.isInteger(page) && page >= 1 && page <= 50,
    ) &&
    value.isRoofPermit === true &&
    value.coverage.queryField === "Permit Type" &&
    value.coverage.queryOperator === "Contains" &&
    value.coverage.queryValue === "ROOF" &&
    value.coverage.sourceReportedCount === 59_379 &&
    value.coverage.exposedRecordCap === 1_000 &&
    value.coverage.exposedPageCount === 50 &&
    value.coverage.pageSize === 20 &&
    value.coverage.completenessBoundary === "bounded_capped_keyword_slice" &&
    value.coverage.countEvidence === "operator_observed_source_result"
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
 * Validate a municipal detail row frozen from a terminal query/page receipt.
 *
 * @param {unknown} value - Candidate row.
 * @returns {value is MunicipalPartialRecord} Whether the strict shared fields exist.
 */
function isMunicipalPartialRecord(value) {
  if (!isRecord(value) || !isRecord(value.raw)) return false;
  if (
    typeof value.source_system !== "string" ||
    !/^broward_[a-z0-9_]+$/u.test(value.source_system) ||
    typeof value.source_protocol !== "string" ||
    typeof value.source_url !== "string" ||
    typeof value.source_record_id !== "string" ||
    value.source_record_id.length === 0 ||
    value.record_key !== `${value.source_system}:${value.source_record_id}` ||
    typeof value.jurisdiction !== "string" ||
    value.jurisdiction.length === 0 ||
    typeof value.permit_number !== "string" ||
    value.permit_number.length === 0 ||
    typeof value.is_roof_permit !== "boolean"
  ) {
    return false;
  }
  return [
    value.parcel_identifier,
    value.work_location,
    value.application_date,
    value.permit_issue_date,
    value.expiration_date,
    value.record_status,
    value.record_type,
    value.project_description,
  ].every(isNullableString);
}

/**
 * Test one optional source field without coercion.
 *
 * @param {unknown} value - Candidate optional text.
 * @returns {value is string | null} Whether the value is nullable text.
 */
function isNullableString(value) {
  return value === null || typeof value === "string";
}

/**
 * Validate a lowercase SHA-256 digest.
 *
 * @param {unknown} value - Candidate digest.
 * @returns {value is string} Whether the digest is canonical.
 */
function isSha256(value) {
  return typeof value === "string" && /^[a-f0-9]{64}$/u.test(value);
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
