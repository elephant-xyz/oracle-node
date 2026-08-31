#!/usr/bin/env node
// @ts-check

/**
 * Idempotently load the reconciled Broward permit pilot into verified Neon.
 *
 * The input is the private normalized JSONL produced by
 * `run-broward-permit-pilot.mjs`. The loader never discovers or fetches permit
 * sources. It writes only records that already passed the pilot's deduplication
 * and reconciliation gates.
 */

import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

import pg from "pg";

const { Client } = pg;
const EXPECTED_NEON_PROJECT_ID = "raspy-frost-51580436";
const PRODUCTION_ENDPOINT_PREFIX = "ep-mute-leaf";
const LOAD_LOCK_NAMESPACE = 12_011;
const LOAD_LOCK_KEY = 3;
const DEFAULT_INPUT =
  "downloads/broward/permit-acceptance-pilot/normalized-permits.private.jsonl";
const LOAD_KEY = "broward-supported-pilots-v3";

/**
 * @typedef {import("./broward-permit-query-artifact.mjs").BrowardNormalizedPermit & {
 *   source_search_url:string,
 *   source_list_url:string,
 *   source_folio_number:string,
 *   issuing_jurisdiction:string,
 *   work_location:string,
 *   legal_description:string,
 *   contractor_name:string|null,
 *   contractor_license:string|null,
 *   building_use:string|null,
 *   present_use:string|null,
 *   proposed_use:string|null,
 *   square_footage:number|null,
 *   occupancy_type:string|null,
 *   construction_type:string|null,
 *   occupant_load:number|null,
 *   finish_floor_above_road:number|null,
 *   finish_floor_above_sea_level:number|null,
 *   is_roof_permit:boolean,
 *   inspections:readonly {
 *     source_url:string,
 *     source_object_id:string,
 *     inspection_type:string,
 *     requested_date:string|null,
 *     result:string,
 *     completed_date:string|null
 *   }[],
 *   raw:Record<string,unknown>
 * }} BrowardNormalizedPermit
 *
 * @typedef {object} BrowardAccelaPermitRecord
 * @property {"permit-harvest.accela.v1"} schemaVersion - Accela artifact schema.
 * @property {string} source - Source-system label.
 * @property {string} sourceSystem - County-prefixed source system.
 * @property {string} jurisdiction - Issuing jurisdiction.
 * @property {string} retrievedAt - ISO retrieval timestamp.
 * @property {string} sourceUrl - Official detail URL.
 * @property {string} recordNumber - Public record number.
 * @property {string | null} recordType - Public record type.
 * @property {string | null} recordStatus - Public record status.
 * @property {string | null} workLocation - Public work location.
 * @property {string} parcelIdentifier - Submitted Broward folio.
 * @property {string | null} sourceParcelIdentifier - Detail-page parcel ID.
 * @property {string | null} applicant - Public applicant.
 * @property {string | null} licensedProfessional - Public professional.
 * @property {string | null} projectDescription - Public description.
 * @property {Record<string, string>} moreDetails - Parsed detail fields.
 * @property {string | null} moreDetailsRawText - Raw details text.
 * @property {string | null} inspectionsRawText - Raw inspections text.
 * @property {readonly Record<string, unknown>[]} completedInspections
 *   Completed shared-Accela inspection records.
 * @property {string | null} processingStatusRawText - Raw status text.
 * @property {readonly Record<string, unknown>[]} documentLinks - Public documents.
 * @property {readonly Record<string, unknown>[]} relatedLinks - Public links.
 * @property {string} rawText - Collapsed detail text.
 * @property {Record<string, unknown>} sourceSearchResult - Search evidence.
 * @property {string} idempotencyKey - Jurisdiction-scoped identity.
 * @property {Record<string, unknown>} provenance - Source-boundary evidence.
 *
 * @typedef {object} BrowardMunicipalPermitRecord
 * @property {string} source_system - County-prefixed municipal source.
 * @property {"tyler_energov_civic_access" | "citizenserve_cap_government"} source_vendor
 *   Exact adapter-family identifier.
 * @property {string} source_url - Official detail URL.
 * @property {string} source_record_id - Stable source object ID.
 * @property {string} record_key - Stable source-system record key.
 * @property {string} city - Issuing municipality.
 * @property {string} permit_number - Public permit number.
 * @property {string} parcel_identifier - Exact Broward folio.
 * @property {string | null} work_location - Public work location.
 * @property {string | null} permit_issue_date - ISO issue date.
 * @property {string | null} application_date - ISO application date.
 * @property {string | null} expiration_date - ISO expiration date.
 * @property {string | null} finalized_date - Explicit source finalization date.
 * @property {string | null} record_status - Public status.
 * @property {string | null} record_type - Public permit type.
 * @property {string | null} work_class - Public work class.
 * @property {string | null} project_description - Public description.
 * @property {number | null} square_feet - Public project area.
 * @property {number | null} job_value - Public estimated value.
 * @property {boolean} is_roof_permit - Conservative roof classification.
 * @property {Record<string, unknown>} provenance - Search evidence.
 * @property {Record<string, unknown>} raw - Source-specific fields.
 *
 * @typedef {object} PermitParent
 * @property {string} propertyId - Canonical Broward appraiser property UUID.
 * @property {string} parcelId - Canonical Broward appraiser parcel UUID.
 *
 * @typedef {object} PermitLoadOptions
 * @property {string} inputPath - Reconciled private normalized permit JSONL.
 * @property {number | null} expectedRecords - Optional exact record count.
 * @property {boolean} includeBcs - Whether to read the BCS-format input.
 * @property {string | null} accelaInputPath - Optional reconciled Accela JSONL.
 * @property {number | null} expectedAccelaRecords - Optional exact Accela count.
 * @property {readonly string[]} municipalInputPaths - Tyler/Citizenserve JSONL inputs.
 * @property {number | null} expectedMunicipalRecords - Optional exact municipal count.
 *
 * @typedef {object} PermitUpsertValues
 * @property {string} sourceSystem - County-prefixed permit source.
 * @property {string} sourceRecordKey - Stable source record identity.
 * @property {string} sourceRecordHash - SHA-256 of normalized source payload.
 * @property {string} sourceArtifactUri - Official permit detail URL.
 * @property {string} requestIdentifier - Stable permit request key.
 * @property {string} propertyId - Matched property UUID.
 * @property {string} parcelId - Matched parcel UUID.
 * @property {string} parcelIdentifier - Exact Broward folio.
 * @property {string} permitNumber - Public permit/application number.
 * @property {string | null} improvementType - Public permit type.
 * @property {string | null} improvementStatus - Public source status.
 * @property {"permit_record" | "master_application"} improvementAction
 *   Explicit BCS source record kind.
 * @property {string | null} applicationReceivedDate - ISO application date.
 * @property {string | null} permitIssueDate - ISO issue date.
 * @property {string | null} finalInspectionDate - Latest explicit completed inspection.
 * @property {string | null} expirationDate - ISO expiration date.
 * @property {string | null} workLocation - Public work location.
 * @property {number | null} estimatedJobValue - Public estimated job value.
 * @property {number | null} estimatedSqFt - Public square footage.
 * @property {boolean} isRoofPermit - Explicit/conservative roofing classification.
 * @property {string | null} projectDescription - Public project description.
 * @property {string | null} description - Public project title.
 * @property {Record<string, unknown>} moreDetails - Preserved public permit facts.
 * @property {Record<string, unknown>} sourcePayload - Complete normalized public record.
 */

/**
 * Parse the bounded loader CLI.
 *
 * @param {readonly string[]} argv - Arguments after the script path.
 * @returns {PermitLoadOptions} Validated local input and reconciliation bound.
 */
export function parsePermitLoadOptions(argv) {
  let inputPath = DEFAULT_INPUT;
  let expectedRecords = null;
  let includeBcs = true;
  let accelaInputPath = null;
  let expectedAccelaRecords = null;
  /** @type {string[]} */
  const municipalInputPaths = [];
  let expectedMunicipalRecords = null;
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (
      typeof flag !== "string" ||
      typeof value !== "string" ||
      value.startsWith("--")
    ) {
      throw new Error("Permit load options must be --flag value pairs");
    }
    if (flag === "--input") inputPath = value;
    else if (flag === "--include-bcs") {
      if (value !== "true" && value !== "false") {
        throw new Error("--include-bcs must be true or false");
      }
      includeBcs = value === "true";
    }
    else if (flag === "--accela-input") accelaInputPath = value;
    else if (flag === "--municipal-input") municipalInputPaths.push(value);
    else if (flag === "--expected-records") {
      expectedRecords = Number(value);
    } else if (flag === "--expected-accela-records") {
      expectedAccelaRecords = Number(value);
    } else if (flag === "--expected-municipal-records") {
      expectedMunicipalRecords = Number(value);
    } else {
      throw new Error(`Unknown permit load option: ${flag}`);
    }
  }
  if (
    expectedRecords !== null &&
    (!Number.isInteger(expectedRecords) || expectedRecords < 1)
  ) {
    throw new Error("--expected-records must be a positive integer");
  }
  if (!includeBcs && expectedRecords !== null) {
    throw new Error("--expected-records cannot be used when BCS is excluded");
  }
  if (
    expectedAccelaRecords !== null &&
    (!Number.isInteger(expectedAccelaRecords) ||
      expectedAccelaRecords < 1)
  ) {
    throw new Error("--expected-accela-records must be a positive integer");
  }
  if (expectedAccelaRecords !== null && accelaInputPath === null) {
    throw new Error(
      "--expected-accela-records requires --accela-input",
    );
  }
  if (
    expectedMunicipalRecords !== null &&
    (!Number.isInteger(expectedMunicipalRecords) ||
      expectedMunicipalRecords < 1)
  ) {
    throw new Error("--expected-municipal-records must be positive");
  }
  if (
    expectedMunicipalRecords !== null &&
    municipalInputPaths.length === 0
  ) {
    throw new Error(
      "--expected-municipal-records requires --municipal-input",
    );
  }
  return {
    inputPath,
    expectedRecords,
    includeBcs,
    accelaInputPath,
    expectedAccelaRecords,
    municipalInputPaths,
    expectedMunicipalRecords,
  };
}

/**
 * Read and validate reconciled normalized permit JSONL.
 *
 * @param {string} inputPath - Private normalized permit JSONL.
 * @returns {Promise<{records:BrowardNormalizedPermit[],sourceSha256:string}>}
 *   Unique source records and exact input checksum.
 */
export async function readNormalizedPermitRecords(inputPath) {
  const text = await readFile(inputPath, "utf8");
  const sourceSha256 = createHash("sha256").update(text).digest("hex");
  /** @type {BrowardNormalizedPermit[]} */
  const records = [];
  const keys = new Set();
  for (const line of text.split(/\r?\n/u)) {
    if (line.trim() === "") continue;
    const parsed = /** @type {unknown} */ (JSON.parse(line));
    if (!isNormalizedPermit(parsed)) {
      throw new Error("Normalized Broward permit JSONL contains an invalid row");
    }
    if (keys.has(parsed.record_key)) {
      throw new Error("Normalized Broward permit JSONL contains a duplicate key");
    }
    keys.add(parsed.record_key);
    records.push(parsed);
  }
  if (records.length === 0) {
    throw new Error("Normalized Broward permit JSONL is empty");
  }
  return { records, sourceSha256 };
}

/**
 * Read and validate deduplicated Broward Accela permit JSONL.
 *
 * @param {string} inputPath - Private Accela normalized JSONL.
 * @returns {Promise<{records:BrowardAccelaPermitRecord[],sourceSha256:string}>}
 *   Unique Accela records and exact input checksum.
 */
export async function readNormalizedAccelaPermitRecords(inputPath) {
  const text = await readFile(inputPath, "utf8");
  const sourceSha256 = createHash("sha256").update(text).digest("hex");
  /** @type {BrowardAccelaPermitRecord[]} */
  const records = [];
  const keys = new Set();
  for (const line of text.split(/\r?\n/u)) {
    if (line.trim() === "") continue;
    const parsed = /** @type {unknown} */ (JSON.parse(line));
    if (!isAccelaPermit(parsed)) {
      throw new Error("Normalized Broward Accela JSONL contains an invalid row");
    }
    if (keys.has(parsed.idempotencyKey)) {
      throw new Error("Normalized Broward Accela JSONL contains a duplicate key");
    }
    keys.add(parsed.idempotencyKey);
    records.push(parsed);
  }
  if (records.length === 0) {
    throw new Error("Normalized Broward Accela JSONL is empty");
  }
  return { records, sourceSha256 };
}

/**
 * Read and reconcile one or more municipal Tyler/Citizenserve JSONL outputs.
 *
 * @param {readonly string[]} inputPaths - Private normalized municipal JSONL files.
 * @returns {Promise<{records:BrowardMunicipalPermitRecord[],sourceSha256:string|null}>}
 *   Unique municipal permit records and combined deterministic checksum.
 */
export async function readNormalizedMunicipalPermitRecords(inputPaths) {
  /** @type {BrowardMunicipalPermitRecord[]} */
  const records = [];
  const keys = new Set();
  /** @type {string[]} */
  const hashes = [];
  for (const inputPath of inputPaths) {
    const text = await readFile(inputPath, "utf8");
    hashes.push(createHash("sha256").update(text).digest("hex"));
    for (const line of text.split(/\r?\n/u)) {
      if (line.trim() === "") continue;
      const parsed = /** @type {unknown} */ (JSON.parse(line));
      if (!isMunicipalPermit(parsed)) {
        throw new Error(
          "Normalized Broward municipal JSONL contains an invalid row",
        );
      }
      if (keys.has(parsed.record_key)) {
        throw new Error(
          "Normalized Broward municipal JSONL contains a duplicate key",
        );
      }
      keys.add(parsed.record_key);
      records.push(parsed);
    }
  }
  return {
    records,
    sourceSha256:
      hashes.length === 0 ? null : stableHash(hashes.sort()),
  };
}

/**
 * Build typed direct-table values for one reconciled permit.
 *
 * @param {BrowardNormalizedPermit} record - Valid normalized source record.
 * @param {PermitParent} parent - Exact appraiser property and parcel UUIDs.
 * @returns {PermitUpsertValues} Idempotent query-db values.
 */
export function buildPermitUpsertValues(record, parent) {
  const completedDates = record.inspections
    .map((inspection) => inspection.completed_date)
    .filter((date) => date !== null)
    .sort();
  return {
    sourceSystem: record.source_system,
    sourceRecordKey: record.record_key,
    sourceRecordHash: stableHash(record),
    sourceArtifactUri: record.source_url,
    requestIdentifier: record.record_key,
    propertyId: parent.propertyId,
    parcelId: parent.parcelId,
    parcelIdentifier: record.parcel_identifier,
    permitNumber: record.permit_number,
    improvementType: record.record_type,
    improvementStatus: record.record_status,
    improvementAction:
      record.source_record_kind === "master"
        ? "master_application"
        : "permit_record",
    applicationReceivedDate: record.application_date,
    permitIssueDate: record.permit_issue_date,
    finalInspectionDate: completedDates.at(-1) ?? null,
    expirationDate: record.expiration_date,
    workLocation: record.work_location,
    estimatedJobValue: record.job_value,
    estimatedSqFt:
      typeof record.square_footage === "number"
        ? record.square_footage
        : null,
    isRoofPermit: record.is_roof_permit,
    projectDescription: record.project_description,
    description: record.project_title,
    moreDetails: {
      is_roof_permit: record.is_roof_permit,
      issuing_jurisdiction: record.issuing_jurisdiction,
      legal_description: record.legal_description,
      contractor_name: record.contractor_name,
      contractor_license: record.contractor_license,
      building_use: record.building_use,
      present_use: record.present_use,
      proposed_use: record.proposed_use,
      occupancy_type: record.occupancy_type,
      construction_type: record.construction_type,
      occupant_load: record.occupant_load,
      finish_floor_above_road: record.finish_floor_above_road,
      finish_floor_above_sea_level: record.finish_floor_above_sea_level,
      source_object_id: record.source_object_id,
      source_record_kind: record.source_record_kind,
    },
    sourcePayload:
      /** @type {Record<string, unknown>} */ (
        /** @type {unknown} */ (record)
      ),
  };
}

/**
 * Build canonical query-db values for one reconciled Broward Accela record.
 *
 * Accela does not expose an unambiguous issue/application date in the bounded
 * detail contract, so those fields remain null rather than being inferred from
 * record numbers or free text.
 *
 * @param {BrowardAccelaPermitRecord} record - Reconciled Accela record.
 * @param {PermitParent} parent - Exact appraiser property and parcel UUIDs.
 * @returns {PermitUpsertValues} Idempotent property-improvement values.
 */
export function buildAccelaPermitUpsertValues(record, parent) {
  return {
    sourceSystem: record.sourceSystem,
    sourceRecordKey: record.idempotencyKey,
    sourceRecordHash: stableHash(record),
    sourceArtifactUri: record.sourceUrl,
    requestIdentifier: record.idempotencyKey,
    propertyId: parent.propertyId,
    parcelId: parent.parcelId,
    parcelIdentifier: record.parcelIdentifier,
    permitNumber: record.recordNumber,
    improvementType: record.recordType,
    improvementStatus: record.recordStatus,
    improvementAction: "permit_record",
    applicationReceivedDate: null,
    permitIssueDate: null,
    finalInspectionDate: null,
    expirationDate: null,
    workLocation: record.workLocation,
    estimatedJobValue: null,
    estimatedSqFt: null,
    isRoofPermit: /\broof(?:ing)?\b/iu.test(
      [
        record.recordNumber,
        record.recordType,
        record.projectDescription,
        record.rawText,
      ]
        .filter((value) => typeof value === "string")
        .join(" "),
    ),
    projectDescription: record.projectDescription,
    description: record.projectDescription,
    moreDetails: {
      ...record.moreDetails,
      schema_version: record.schemaVersion,
      jurisdiction: record.jurisdiction,
      applicant: record.applicant,
      licensed_professional: record.licensedProfessional,
      completed_inspections: record.completedInspections,
      is_roof_permit: /\broof(?:ing)?\b/iu.test(
        [
          record.recordNumber,
          record.recordType,
          record.projectDescription,
          record.rawText,
        ]
          .filter((value) => typeof value === "string")
          .join(" "),
      ),
      processing_status_raw_text: record.processingStatusRawText,
      document_links: record.documentLinks,
      related_links: record.relatedLinks,
      source_search_result: record.sourceSearchResult,
      provenance: record.provenance,
    },
    sourcePayload:
      /** @type {Record<string, unknown>} */ (
        /** @type {unknown} */ (record)
      ),
  };
}

/**
 * Build canonical values for one bounded Tyler/Citizenserve permit record.
 *
 * @param {BrowardMunicipalPermitRecord} record - Reconciled municipal record.
 * @param {PermitParent} parent - Exact appraiser property and parcel UUIDs.
 * @returns {PermitUpsertValues} Idempotent property-improvement values.
 */
export function buildMunicipalPermitUpsertValues(record, parent) {
  return {
    sourceSystem: record.source_system,
    sourceRecordKey: record.record_key,
    sourceRecordHash: stableHash(record),
    sourceArtifactUri: record.source_url,
    requestIdentifier: record.record_key,
    propertyId: parent.propertyId,
    parcelId: parent.parcelId,
    parcelIdentifier: record.parcel_identifier,
    permitNumber: record.permit_number,
    improvementType: record.record_type,
    improvementStatus: record.record_status,
    improvementAction: "permit_record",
    applicationReceivedDate: record.application_date,
    permitIssueDate: record.permit_issue_date,
    finalInspectionDate: null,
    expirationDate: record.expiration_date,
    workLocation: record.work_location,
    estimatedJobValue: record.job_value,
    estimatedSqFt: record.square_feet,
    isRoofPermit: record.is_roof_permit,
    projectDescription: record.project_description,
    description: record.project_description,
    moreDetails: {
      city: record.city,
      source_vendor: record.source_vendor,
      source_record_id: record.source_record_id,
      work_class: record.work_class,
      finalized_date: record.finalized_date,
      is_roof_permit: record.is_roof_permit,
      provenance: record.provenance,
      raw: record.raw,
    },
    sourcePayload:
      /** @type {Record<string, unknown>} */ (
        /** @type {unknown} */ (record)
      ),
  };
}

/**
 * Load all reconciled pilot records in one transaction and verify exact counts.
 *
 * @param {PermitLoadOptions} options - Input and expected-count gate.
 * @returns {Promise<{propertyImprovements:number,inspections:number,sourceSha256:string}>}
 *   Exact committed logical counts and source identity.
 */
export async function loadBrowardPermitPilotToNeon(options) {
  const bcs = options.includeBcs
    ? await readNormalizedPermitRecords(options.inputPath)
    : {
        records: /** @type {BrowardNormalizedPermit[]} */ ([]),
        sourceSha256: /** @type {string | null} */ (null),
      };
  if (
    options.expectedRecords !== null &&
    bcs.records.length !== options.expectedRecords
  ) {
    throw new Error("Permit pilot record count differs from the required count");
  }
  const accela =
    options.accelaInputPath === null
      ? { records: [], sourceSha256: null }
      : await readNormalizedAccelaPermitRecords(options.accelaInputPath);
  if (
    options.expectedAccelaRecords !== null &&
    accela.records.length !== options.expectedAccelaRecords
  ) {
    throw new Error(
      "Accela pilot record count differs from the required count",
    );
  }
  const municipal = await readNormalizedMunicipalPermitRecords(
    options.municipalInputPaths,
  );
  if (
    options.expectedMunicipalRecords !== null &&
    municipal.records.length !== options.expectedMunicipalRecords
  ) {
    throw new Error(
      "Municipal pilot record count differs from the required count",
    );
  }
  const sourceSha256 = stableHash({
    accela: accela.sourceSha256,
    bcs: bcs.sourceSha256,
    municipal: municipal.sourceSha256,
  });
  const permitRecordCount =
    bcs.records.length +
    accela.records.length +
    municipal.records.length;
  const target = requireNeonTarget(process.env);
  const client = new Client({
    connectionString: target.connectionString,
    application_name: "broward-permit-pilot-loader",
    connectionTimeoutMillis: 10_000,
    statement_timeout: 120_000,
  });
  await client.connect();
  try {
    await verifyNeonTarget(client, target);
    await acquireLoadLock(client);
    await ensureLoadControlTable(client);
    const parents = await readPermitParents(
      client,
      [
        ...new Set([
          ...bcs.records.map((record) => record.parcel_identifier),
          ...accela.records.map((record) => record.parcelIdentifier),
          ...municipal.records.map((record) => record.parcel_identifier),
        ]),
      ],
    );
    const permitKeys = [
      ...bcs.records.map((record) => record.record_key),
      ...accela.records.map((record) => record.idempotencyKey),
      ...municipal.records.map((record) => record.record_key),
    ];
    const inspectionKeys = bcs.records.flatMap(buildInspectionSourceKeys);
    let inspectionCount = 0;
    await client.query("BEGIN");
    try {
      for (const record of bcs.records) {
        const parent = parents.get(record.parcel_identifier);
        if (parent === undefined) {
          throw new Error("Permit record has no exact Broward appraisal parent");
        }
        const values = buildPermitUpsertValues(record, parent);
        const result = await upsertPropertyImprovement(client, values);
        inspectionCount += await upsertInspections(
          client,
          result.propertyImprovementId,
          record,
        );
      }
      for (const record of accela.records) {
        const parent = parents.get(record.parcelIdentifier);
        if (parent === undefined) {
          throw new Error("Accela record has no exact Broward appraisal parent");
        }
        await upsertPropertyImprovement(
          client,
          buildAccelaPermitUpsertValues(record, parent),
        );
      }
      for (const record of municipal.records) {
        const parent = parents.get(record.parcel_identifier);
        if (parent === undefined) {
          throw new Error(
            "Municipal permit record has no exact Broward appraisal parent",
          );
        }
        await upsertPropertyImprovement(
          client,
          buildMunicipalPermitUpsertValues(record, parent),
        );
      }
      await verifyLoadedRecords(
        client,
        permitKeys,
        inspectionKeys,
      );
      await client.query(
        `INSERT INTO ingest_control.broward_permit_loads (
           load_key, source_sha256, expected_property_improvements,
           expected_inspections
         ) VALUES ($1,$2,$3,$4)
         ON CONFLICT (load_key) DO UPDATE SET
           source_sha256 = EXCLUDED.source_sha256,
           expected_property_improvements =
             EXCLUDED.expected_property_improvements,
           expected_inspections = EXCLUDED.expected_inspections,
           committed_at = now()`,
        [LOAD_KEY, sourceSha256, permitRecordCount, inspectionCount],
      );
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    }
    return {
      propertyImprovements: permitRecordCount,
      inspections: inspectionCount,
      sourceSha256,
    };
  } finally {
    await client.end();
  }
}

/**
 * @param {unknown} value - Candidate normalized permit.
 * @returns {value is BrowardNormalizedPermit} Whether required source fields exist.
 */
function isNormalizedPermit(value) {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const candidate =
    /** @type {Partial<BrowardNormalizedPermit>} */ (value);
  return (
    typeof candidate.source_system === "string" &&
    /^broward_[a-z0-9_]+_permits$/u.test(candidate.source_system) &&
    typeof candidate.source_url === "string" &&
    typeof candidate.source_object_id === "string" &&
    (candidate.source_record_kind === "master" ||
      candidate.source_record_kind === "permit") &&
    typeof candidate.record_key === "string" &&
    typeof candidate.parcel_identifier === "string" &&
    /^[A-Z0-9]{12}$/u.test(candidate.parcel_identifier) &&
    typeof candidate.permit_number === "string" &&
    typeof candidate.record_status === "string" &&
    typeof candidate.record_type === "string" &&
    Array.isArray(candidate.inspections) &&
    candidate.inspections.every(
      (inspection) =>
        typeof inspection === "object" &&
        inspection !== null &&
        !Array.isArray(inspection) &&
        typeof inspection.source_object_id === "string",
    )
  );
}

/**
 * @param {unknown} value - Candidate normalized Accela permit.
 * @returns {value is BrowardAccelaPermitRecord} Whether stable load fields exist.
 */
function isAccelaPermit(value) {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const candidate =
    /** @type {Partial<BrowardAccelaPermitRecord>} */ (value);
  return (
    typeof candidate.sourceSystem === "string" &&
    /^broward_[a-z0-9_]+_permits$/u.test(candidate.sourceSystem) &&
    typeof candidate.sourceUrl === "string" &&
    typeof candidate.recordNumber === "string" &&
    typeof candidate.recordType === "string" &&
    typeof candidate.recordStatus === "string" &&
    typeof candidate.parcelIdentifier === "string" &&
    /^[A-Z0-9]{12}$/u.test(candidate.parcelIdentifier) &&
    typeof candidate.idempotencyKey === "string" &&
    typeof candidate.moreDetails === "object" &&
    candidate.moreDetails !== null &&
    !Array.isArray(candidate.moreDetails) &&
    typeof candidate.provenance === "object" &&
    candidate.provenance !== null &&
    !Array.isArray(candidate.provenance)
  );
}

/**
 * @param {unknown} value - Candidate Tyler/Citizenserve normalized record.
 * @returns {value is BrowardMunicipalPermitRecord} Whether stable fields exist.
 */
function isMunicipalPermit(value) {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const candidate =
    /** @type {Partial<BrowardMunicipalPermitRecord>} */ (value);
  return (
    typeof candidate.source_system === "string" &&
    /^broward_[a-z0-9_]+_permits$/u.test(candidate.source_system) &&
    (candidate.source_vendor === "tyler_energov_civic_access" ||
      candidate.source_vendor === "citizenserve_cap_government") &&
    typeof candidate.source_url === "string" &&
    typeof candidate.source_record_id === "string" &&
    typeof candidate.record_key === "string" &&
    typeof candidate.city === "string" &&
    typeof candidate.permit_number === "string" &&
    typeof candidate.parcel_identifier === "string" &&
    /^[A-Z0-9]{12}$/u.test(candidate.parcel_identifier) &&
    typeof candidate.provenance === "object" &&
    candidate.provenance !== null &&
    !Array.isArray(candidate.provenance) &&
    typeof candidate.raw === "object" &&
    candidate.raw !== null &&
    !Array.isArray(candidate.raw)
  );
}

/**
 * @param {NodeJS.ProcessEnv} environment - Runtime secrets.
 * @returns {{connectionString:string,expectedBranchId:string,expectedEndpointId:string}}
 *   Validated direct Neon target.
 */
function requireNeonTarget(environment) {
  const connectionString = environment.DATABASE_URL_UNPOOLED;
  const expectedBranchId = environment.BROWARD_INGEST_NEON_BRANCH_ID;
  const expectedEndpointId = environment.BROWARD_INGEST_NEON_ENDPOINT_ID;
  if (typeof connectionString !== "string" || connectionString.trim() === "") {
    throw new Error("DATABASE_URL_UNPOOLED is required");
  }
  const parsed = new URL(connectionString);
  if (parsed.hostname.includes("-pooler")) {
    throw new Error("Permit loading requires direct Neon");
  }
  if (
    typeof expectedBranchId !== "string" ||
    !/^br-[a-z0-9-]+$/u.test(expectedBranchId) ||
    typeof expectedEndpointId !== "string" ||
    !/^ep-[a-z0-9-]+$/u.test(expectedEndpointId) ||
    expectedEndpointId.startsWith(PRODUCTION_ENDPOINT_PREFIX)
  ) {
    throw new Error("Verified Broward Neon IDs are required");
  }
  return { connectionString, expectedBranchId, expectedEndpointId };
}

/**
 * @param {import("pg").Client} client - Connected direct client.
 * @param {{expectedBranchId:string,expectedEndpointId:string}} target - Expected IDs.
 * @returns {Promise<void>} Resolves after read-only identity proof.
 */
async function verifyNeonTarget(client, target) {
  await client.query("BEGIN READ ONLY");
  try {
    const result = await client.query(
      `SELECT current_setting('neon.project_id', true) AS project_id,
              current_setting('neon.branch_id', true) AS branch_id,
              current_setting('neon.endpoint_id', true) AS endpoint_id`,
    );
    const row = result.rows[0];
    if (
      row?.project_id !== EXPECTED_NEON_PROJECT_ID ||
      row.branch_id !== target.expectedBranchId ||
      row.endpoint_id !== target.expectedEndpointId
    ) {
      throw new Error("Permit load target is not isolated broward-ingest");
    }
    await client.query("ROLLBACK");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

/**
 * @param {import("pg").Client} client - Identity-verified direct client.
 * @returns {Promise<void>} Resolves only when this loader owns the permit lock.
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
 * @param {import("pg").Client} client - Identity-verified client.
 * @returns {Promise<void>} Resolves after additive control DDL.
 */
async function ensureLoadControlTable(client) {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ingest_control`);
  await client.query(
    `CREATE TABLE IF NOT EXISTS ingest_control.broward_permit_loads (
       load_key text PRIMARY KEY,
       source_sha256 text NOT NULL CHECK (source_sha256 ~ '^[a-f0-9]{64}$'),
       expected_property_improvements integer NOT NULL CHECK (
         expected_property_improvements > 0
       ),
       expected_inspections integer NOT NULL CHECK (expected_inspections >= 0),
       committed_at timestamptz NOT NULL DEFAULT now()
     )`,
  );
}

/**
 * @param {import("pg").Client} client - Identity-verified client.
 * @param {readonly string[]} folios - Distinct exact Broward folios.
 * @returns {Promise<ReadonlyMap<string, PermitParent>>} Exact canonical parents.
 */
async function readPermitParents(client, folios) {
  const result = await client.query(
    `SELECT p.request_identifier, p.property_id, p.parcel_id
     FROM public.properties p
     WHERE p.source_system = 'broward_appraiser'
       AND p.request_identifier = ANY($1::text[])`,
    [folios],
  );
  /** @type {Map<string, PermitParent>} */
  const parents = new Map();
  for (const row of result.rows) {
    if (
      typeof row.request_identifier !== "string" ||
      typeof row.property_id !== "string" ||
      typeof row.parcel_id !== "string"
    ) {
      throw new Error("Broward permit parent row is incomplete");
    }
    if (parents.has(row.request_identifier)) {
      throw new Error("Broward permit parent identity is duplicated");
    }
    parents.set(row.request_identifier, {
      propertyId: row.property_id,
      parcelId: row.parcel_id,
    });
  }
  if (parents.size !== folios.length) {
    throw new Error("One or more permit folios lack a loaded appraisal parent");
  }
  return parents;
}

/**
 * @param {import("pg").Client} client - Transactional client.
 * @param {PermitUpsertValues} values - Typed normalized permit values.
 * @returns {Promise<{propertyImprovementId:string}>} Stable loaded row identity.
 */
async function upsertPropertyImprovement(client, values) {
  const result = await client.query(
    `INSERT INTO public.property_improvements (
       property_id, parcel_id, request_identifier, permit_number,
       improvement_type, improvement_status, improvement_action,
       application_received_date, permit_issue_date, final_inspection_date,
       expiration_date, estimated_job_value, estimated_sq_ft, source,
       source_url, record_type, source_status, record_status, opened_date,
       work_location, parcel_identifier, property_match_method,
       property_match_confidence, project_description, description,
       more_details, source_http_request, source_payload, source_system,
       source_record_key, source_record_hash, source_artifact_uri
     ) VALUES (
       $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
       $8,$19,$20,'exact_folio','exact',$21,$22,$23::jsonb,$24::jsonb,
       $25::jsonb,$26,$27,$28,$29
     )
     ON CONFLICT (source_system, source_record_key) DO UPDATE SET
       property_id=EXCLUDED.property_id,
       parcel_id=EXCLUDED.parcel_id,
       permit_number=EXCLUDED.permit_number,
       improvement_type=EXCLUDED.improvement_type,
       improvement_status=EXCLUDED.improvement_status,
       improvement_action=EXCLUDED.improvement_action,
       application_received_date=EXCLUDED.application_received_date,
       permit_issue_date=EXCLUDED.permit_issue_date,
       final_inspection_date=EXCLUDED.final_inspection_date,
       expiration_date=EXCLUDED.expiration_date,
       estimated_job_value=EXCLUDED.estimated_job_value,
       estimated_sq_ft=EXCLUDED.estimated_sq_ft,
       source_url=EXCLUDED.source_url,
       record_status=EXCLUDED.record_status,
       work_location=EXCLUDED.work_location,
       project_description=EXCLUDED.project_description,
       description=EXCLUDED.description,
       more_details=EXCLUDED.more_details,
       source_http_request=EXCLUDED.source_http_request,
       source_payload=EXCLUDED.source_payload,
       source_record_hash=EXCLUDED.source_record_hash,
       source_artifact_uri=EXCLUDED.source_artifact_uri,
       loaded_at=now(),
       updated_at=now()
     RETURNING property_improvement_id`,
    [
      values.propertyId,
      values.parcelId,
      values.requestIdentifier,
      values.permitNumber,
      values.improvementType,
      values.improvementStatus,
      values.improvementAction,
      values.applicationReceivedDate,
      values.permitIssueDate,
      values.finalInspectionDate,
      values.expirationDate,
      values.estimatedJobValue,
      values.estimatedSqFt,
      values.sourceSystem,
      values.sourceArtifactUri,
      values.improvementType,
      values.improvementStatus,
      values.improvementStatus,
      values.workLocation,
      values.parcelIdentifier,
      values.projectDescription,
      values.description,
      JSON.stringify(values.moreDetails),
      JSON.stringify({
        method: "GET",
        url: values.sourceArtifactUri,
      }),
      JSON.stringify(values.sourcePayload),
      values.sourceSystem,
      values.sourceRecordKey,
      values.sourceRecordHash,
      values.sourceArtifactUri,
    ],
  );
  const id = result.rows[0]?.property_improvement_id;
  if (typeof id !== "string") {
    throw new Error("Permit upsert returned no property improvement ID");
  }
  return { propertyImprovementId: id };
}

/**
 * @param {import("pg").Client} client - Transactional client.
 * @param {string} propertyImprovementId - Parent permit UUID.
 * @param {BrowardNormalizedPermit} record - Complete normalized permit.
 * @returns {Promise<number>} Number of reconciled inspections.
 */
async function upsertInspections(client, propertyImprovementId, record) {
  let count = 0;
  for (const [index, inspection] of record.inspections.entries()) {
    const sourceObjectId =
      typeof inspection.source_object_id === "string"
        ? inspection.source_object_id
        : String(index);
    const sourceRecordKey = `${record.record_key}:inspection:${sourceObjectId}`;
    const sourceRecordHash = stableHash(inspection);
    await client.query(
      `INSERT INTO public.inspections (
         property_improvement_id, inspection_status, permit_number,
         requested_date, completed_date, result, inspection_type,
         inspection_identifier, source_payload, source_system,
         source_record_key, source_record_hash, source_artifact_uri
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13)
       ON CONFLICT (source_system, source_record_key) DO UPDATE SET
         property_improvement_id=EXCLUDED.property_improvement_id,
         inspection_status=EXCLUDED.inspection_status,
         requested_date=EXCLUDED.requested_date,
         completed_date=EXCLUDED.completed_date,
         result=EXCLUDED.result,
         inspection_type=EXCLUDED.inspection_type,
         source_payload=EXCLUDED.source_payload,
         source_record_hash=EXCLUDED.source_record_hash,
         source_artifact_uri=EXCLUDED.source_artifact_uri,
         loaded_at=now(),
         updated_at=now()`,
      [
        propertyImprovementId,
        inspection.result,
        record.permit_number,
        inspection.requested_date,
        inspection.completed_date,
        inspection.result,
        inspection.inspection_type,
        sourceObjectId,
        JSON.stringify(inspection),
        record.source_system,
        sourceRecordKey,
        sourceRecordHash,
        inspection.source_url ?? record.source_url,
      ],
    );
    count += 1;
  }
  return count;
}

/**
 * Build stable inspection keys exactly as the inspection upsert does.
 *
 * @param {BrowardNormalizedPermit} record - Normalized BCS permit.
 * @returns {string[]} Ordered source keys for all public inspections.
 */
function buildInspectionSourceKeys(record) {
  return record.inspections.map((inspection, index) => {
    const sourceObjectId =
      typeof inspection.source_object_id === "string"
        ? inspection.source_object_id
        : String(index);
    return `${record.record_key}:inspection:${sourceObjectId}`;
  });
}

/**
 * @param {import("pg").Client} client - Transactional client.
 * @param {readonly string[]} permitKeys - Expected permit source keys.
 * @param {readonly string[]} inspectionKeys - Expected inspection source keys.
 * @returns {Promise<void>} Resolves only after exact source-key reconciliation.
 */
async function verifyLoadedRecords(client, permitKeys, inspectionKeys) {
  const permitResult = await client.query(
    `SELECT count(*)::integer AS row_count,
            count(*) FILTER (
              WHERE property_id IS NULL OR parcel_id IS NULL
            )::integer AS unlinked_count
     FROM public.property_improvements
     WHERE source_record_key = ANY($1::text[])`,
    [permitKeys],
  );
  const inspectionResult = await client.query(
    `SELECT count(*)::integer AS row_count,
            count(*) FILTER (
              WHERE property_improvement_id IS NULL
            )::integer AS unlinked_count
     FROM public.inspections
     WHERE source_record_key = ANY($1::text[])`,
    [inspectionKeys],
  );
  if (
    Number(permitResult.rows[0]?.row_count) !== permitKeys.length ||
    Number(permitResult.rows[0]?.unlinked_count) !== 0 ||
    Number(inspectionResult.rows[0]?.row_count) !==
      inspectionKeys.length ||
    Number(inspectionResult.rows[0]?.unlinked_count) !== 0
  ) {
    throw new Error("Loaded Broward permit rows did not reconcile");
  }
}

/**
 * Hash JSON with recursively sorted object keys for stable replay detection.
 *
 * @param {unknown} value - JSON-compatible source payload.
 * @returns {string} Lowercase SHA-256.
 */
function stableHash(value) {
  return createHash("sha256").update(stableJson(value)).digest("hex");
}

/**
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

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  loadBrowardPermitPilotToNeon(
    parsePermitLoadOptions(process.argv.slice(2)),
  )
    .then((result) => {
      console.log(
        JSON.stringify({
          event: "broward_permit_pilot_loaded",
          propertyImprovements: result.propertyImprovements,
          inspections: result.inspections,
          sourceSha256: result.sourceSha256,
        }),
      );
    })
    .catch((error) => {
      console.error(
        JSON.stringify({
          event: "broward_permit_pilot_load_failed",
          message: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      process.exitCode = 1;
    });
}
