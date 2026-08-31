// @ts-check

import { createHash } from "node:crypto";

/**
 * Official Fort Lauderdale permit export. The City FeatureServer is the same
 * LauderBuild permit population exposed by the public Accela portal, so its
 * source system and permit-number key deliberately match existing portal rows.
 */
export const FORT_LAUDERDALE_PERMIT_ARCGIS_SOURCE = Object.freeze({
  key: "fort-lauderdale",
  jurisdiction: "Fort Lauderdale",
  sourceSystem: "broward_fort_lauderdale_lauderbuild_permits",
  serviceUrl:
    "https://gis.fortlauderdale.gov/server/rest/services/BuildingPermits/FeatureServer/0",
  queryUrl:
    "https://gis.fortlauderdale.gov/server/rest/services/BuildingPermits/FeatureServer/0/query",
  officialEvidenceUrl:
    "https://gis.fortlauderdale.gov/server/rest/services/BuildingPermits/FeatureServer",
  maxChunkSize: 2_000,
});

const ROOF_PATTERN =
  /\b(roof|roofing|re[\s-]?roof|shingle|membrane|built[\s-]?up|roof\s*tile|tpo)\b/iu;

/**
 * @typedef {typeof FORT_LAUDERDALE_PERMIT_ARCGIS_SOURCE} BrowardArcgisPermitSource
 *
 * @typedef {object} ArcgisPermitFeature
 * @property {Record<string, unknown>} attributes - ArcGIS feature attributes.
 *
 * @typedef {object} NormalizedBrowardArcgisPermit
 * @property {"oracle-node.broward-permit-arcgis.v1"} schema_version
 *   Stable normalized artifact schema.
 * @property {string} source_system - Existing jurisdiction-level source key.
 * @property {"arcgis_feature_service"} source_vendor - Bulk transport family.
 * @property {string} source_url - Official permit detail or feature URL.
 * @property {string} source_record_id - ArcGIS OBJECTID retained as text.
 * @property {string} record_key - Permit-number key shared with portal records.
 * @property {string} city - Issuing jurisdiction.
 * @property {string} permit_number - Public permit identifier.
 * @property {string | null} parcel_identifier - Canonical Broward folio when valid.
 * @property {string | null} work_location - Public project address.
 * @property {string | null} application_date - ISO submission date.
 * @property {string | null} approved_date - ISO source approval date.
 * @property {string | null} certificate_of_occupancy_date - ISO CO date.
 * @property {string | null} record_status - Public permit status.
 * @property {string | null} record_type - Public permit type.
 * @property {string | null} project_description - Public permit description.
 * @property {number | null} job_value - Public estimated construction cost.
 * @property {string | null} applicant - Public applicant name.
 * @property {string | null} contractor_name - Public contractor name.
 * @property {string | null} contractor_license - Public contractor identifier.
 * @property {boolean} is_roof_permit - Conservative source-text classification.
 * @property {string | null} source_last_updated_at - ISO source update timestamp.
 * @property {string | null} source_sync_at - ISO source synchronization timestamp.
 * @property {string} retrieved_at - ISO retrieval timestamp.
 * @property {Readonly<Record<string, string | number | boolean | null>>} source_payload
 *   Allow-listed source fields; owner addresses and contractor phone are omitted.
 * @property {{
 *   transport:"official_arcgis_feature_service",
 *   service_url:string,
 *   official_evidence_url:string,
 *   source_object_id:string,
 *   anonymous:true
 * }} provenance - Auditable source and transport evidence.
 *
 * @typedef {object} ArcgisObjectIdResponse
 * @property {string | null} [objectIdFieldName] - ArcGIS object ID field.
 * @property {unknown[]} [objectIds] - Complete source object IDs.
 * @property {{code?:unknown,message?:unknown,details?:unknown}} [error]
 *
 * @typedef {object} ArcgisFeatureResponse
 * @property {ArcgisPermitFeature[]} [features] - Returned source features.
 * @property {boolean} [exceededTransferLimit] - Whether the response was truncated.
 * @property {{code?:unknown,message?:unknown,details?:unknown}} [error]
 */

/**
 * Convert a required public source value to trimmed text.
 *
 * @param {unknown} value - Candidate source value.
 * @returns {string | null} Trimmed non-empty text.
 */
function optionalText(value) {
  if (typeof value !== "string") return null;
  const text = value.replace(/\s+/gu, " ").trim();
  return text.length === 0 ? null : text;
}

/**
 * Convert a public numeric value without accepting numeric-looking strings.
 *
 * @param {unknown} value - Candidate source number.
 * @returns {number | null} Finite source number.
 */
function optionalNumber(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

/**
 * Convert an ArcGIS epoch-millisecond date to an ISO calendar date.
 *
 * @param {unknown} value - ArcGIS date value.
 * @returns {string | null} UTC YYYY-MM-DD or null.
 */
export function arcgisEpochToIsoDate(value) {
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    !Number.isInteger(value)
  ) {
    return null;
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? null
    : date.toISOString().slice(0, 10);
}

/**
 * Convert an ArcGIS epoch-millisecond date to a complete ISO timestamp.
 *
 * @param {unknown} value - ArcGIS date value.
 * @returns {string | null} UTC timestamp or null.
 */
export function arcgisEpochToIsoTimestamp(value) {
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    !Number.isInteger(value)
  ) {
    return null;
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

/**
 * Normalize a source parcel ID only when it is an exact Broward folio.
 *
 * Display dashes and whitespace are removed, letters are preserved, and no
 * padding, truncation, or numeric conversion is permitted.
 *
 * @param {unknown} value - ArcGIS PARCELID value.
 * @returns {string | null} Canonical 12-character folio or null.
 */
export function normalizeArcgisBrowardFolio(value) {
  if (typeof value !== "string") return null;
  const normalized = value.trim().replace(/[-\s]/gu, "").toUpperCase();
  return /^[A-Z0-9]{12}$/u.test(normalized) ? normalized : null;
}

/**
 * Determine whether explicit permit list text identifies roofing work.
 *
 * @param {readonly unknown[]} values - Source text fields.
 * @returns {boolean} Whether a roofing term is present.
 */
export function isArcgisRoofPermit(values) {
  return ROOF_PATTERN.test(
    values
      .filter((value) => typeof value === "string")
      .join(" "),
  );
}

/**
 * Build the public Fort Lauderdale Accela detail URL from CASEKEY.
 *
 * @param {unknown} caseKeyValue - ArcGIS CASEKEY.
 * @param {string} permitNumber - Validated public permit number.
 * @returns {string} Official detail URL, or a feature query when CASEKEY is absent.
 */
export function buildFortLauderdalePermitUrl(caseKeyValue, permitNumber) {
  const caseKey = optionalText(caseKeyValue)?.toUpperCase() ?? null;
  const parts =
    caseKey === null
      ? null
      : /^([A-Z0-9]+)-([A-Z0-9]+)-([A-Z0-9]+)$/u.exec(caseKey);
  if (parts !== null) {
    const url = new URL(
      "https://aca-prod.accela.com/FTL/Cap/CapDetail.aspx",
    );
    url.searchParams.set("Module", "Permits");
    url.searchParams.set("TabName", "Permits");
    url.searchParams.set("capID1", parts[1] ?? "");
    url.searchParams.set("capID2", parts[2] ?? "");
    url.searchParams.set("capID3", parts[3] ?? "");
    url.searchParams.set("agencyCode", "FTL");
    url.searchParams.set("IsToShowInspection", "");
    return url.toString();
  }
  const fallback = new URL(FORT_LAUDERDALE_PERMIT_ARCGIS_SOURCE.queryUrl);
  fallback.searchParams.set("where", `PERMITID='${permitNumber.replaceAll("'", "''")}'`);
  fallback.searchParams.set("outFields", "*");
  fallback.searchParams.set("returnGeometry", "false");
  fallback.searchParams.set("f", "json");
  return fallback.toString();
}

/**
 * Normalize one official Fort Lauderdale FeatureServer row.
 *
 * @param {ArcgisPermitFeature} feature - Exact source feature.
 * @param {string} retrievedAt - Capture timestamp shared by its source chunk.
 * @returns {{record:NormalizedBrowardArcgisPermit|null,invalidReason:string|null}}
 *   Valid record or an explicit reason retained by reconciliation.
 */
export function normalizeFortLauderdaleArcgisPermit(feature, retrievedAt) {
  const attributes = feature.attributes;
  const objectId = optionalNumber(attributes.OBJECTID);
  const permitNumber = optionalText(attributes.PERMITID)?.toUpperCase() ?? null;
  if (objectId === null || !Number.isInteger(objectId) || objectId < 0) {
    return { record: null, invalidReason: "invalid_object_id" };
  }
  if (permitNumber === null) {
    return { record: null, invalidReason: "missing_permit_number" };
  }
  const source = FORT_LAUDERDALE_PERMIT_ARCGIS_SOURCE;
  const recordType = optionalText(attributes.PERMITTYPE);
  const description = optionalText(attributes.PERMITDESC);
  const locationDescription = optionalText(attributes.LOCDESC);
  const sourceRecordId = String(objectId);
  const sourceUrl = buildFortLauderdalePermitUrl(
    attributes.CASEKEY,
    permitNumber,
  );
  const sourcePayload = Object.freeze({
    object_id: objectId,
    global_id: optionalText(attributes.GlobalID),
    permit_id: permitNumber,
    case_key: optionalText(attributes.CASEKEY),
    permit_type: recordType,
    permit_status: optionalText(attributes.PERMITSTAT),
    permit_description: description,
    submitted_at: arcgisEpochToIsoTimestamp(attributes.SUBMITDT),
    approved_at: arcgisEpochToIsoTimestamp(attributes.APPROVEDT),
    parcel_id: optionalText(attributes.PARCELID),
    full_address: optionalText(attributes.FULLADDR),
    location_description: locationDescription,
    applicant: optionalText(attributes.APPLICANT),
    contractor: optionalText(attributes.CONTRACTOR),
    contractor_id: optionalText(attributes.CONTRACTID),
    estimated_cost: optionalNumber(attributes.ESTCOST),
    certificate_of_occupancy_id: optionalText(attributes.COID),
    certificate_of_occupancy_status: optionalText(attributes.COSTATUS),
    certificate_of_occupancy_issued_at: arcgisEpochToIsoTimestamp(
      attributes.COISSUE,
    ),
    use_class: optionalText(attributes.USECLASS),
    source_last_updated_at: arcgisEpochToIsoTimestamp(
      attributes.LASTUPDATEDATE,
    ),
    source_sync_at: arcgisEpochToIsoTimestamp(attributes.SYNCDATE),
  });
  return {
    record: {
      schema_version: "oracle-node.broward-permit-arcgis.v1",
      source_system: source.sourceSystem,
      source_vendor: "arcgis_feature_service",
      source_url: sourceUrl,
      source_record_id: sourceRecordId,
      record_key: `${source.sourceSystem}:permit:${permitNumber}`,
      city: source.jurisdiction,
      permit_number: permitNumber,
      parcel_identifier: normalizeArcgisBrowardFolio(attributes.PARCELID),
      work_location:
        optionalText(attributes.FULLADDR) ?? locationDescription,
      application_date: arcgisEpochToIsoDate(attributes.SUBMITDT),
      approved_date: arcgisEpochToIsoDate(attributes.APPROVEDT),
      certificate_of_occupancy_date: arcgisEpochToIsoDate(attributes.COISSUE),
      record_status: optionalText(attributes.PERMITSTAT),
      record_type: recordType,
      project_description: description ?? locationDescription,
      job_value: optionalNumber(attributes.ESTCOST),
      applicant: optionalText(attributes.APPLICANT),
      contractor_name: optionalText(attributes.CONTRACTOR),
      contractor_license: optionalText(attributes.CONTRACTID),
      is_roof_permit: isArcgisRoofPermit([
        permitNumber,
        recordType,
        description,
        locationDescription,
      ]),
      source_last_updated_at: arcgisEpochToIsoTimestamp(
        attributes.LASTUPDATEDATE,
      ),
      source_sync_at: arcgisEpochToIsoTimestamp(attributes.SYNCDATE),
      retrieved_at: retrievedAt,
      source_payload: sourcePayload,
      provenance: {
        transport: "official_arcgis_feature_service",
        service_url: source.serviceUrl,
        official_evidence_url: source.officialEvidenceUrl,
        source_object_id: sourceRecordId,
        anonymous: true,
      },
    },
    invalidReason: null,
  };
}

/**
 * Return a stable lowercase SHA-256 for ordered source object IDs.
 *
 * @param {readonly number[]} objectIds - Sorted ArcGIS object IDs.
 * @returns {string} Snapshot identity.
 */
export function hashArcgisObjectIds(objectIds) {
  return createHash("sha256")
    .update(objectIds.join("\n"))
    .digest("hex");
}

/**
 * Read a JSON response and surface ArcGIS errors without leaking response data.
 *
 * @template ResponseShape
 * @param {Response} response - Fetch response.
 * @returns {Promise<ResponseShape>} Parsed object response.
 */
async function readArcgisJson(response) {
  if (!response.ok) {
    throw new Error(`ArcGIS permit source returned HTTP ${String(response.status)}`);
  }
  const parsed = /** @type {unknown} */ (await response.json());
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("ArcGIS permit source returned a non-object response");
  }
  const candidate = /** @type {Record<string, unknown>} */ (parsed);
  if (candidate.error !== undefined) {
    throw new Error("ArcGIS permit source returned an application error");
  }
  return /** @type {ResponseShape} */ (parsed);
}

/**
 * Fetch the complete, uncapped source object-ID snapshot.
 *
 * @param {BrowardArcgisPermitSource} source - Verified official source.
 * @param {typeof fetch} [fetchImpl=fetch] - Injectable standards-compatible fetch.
 * @returns {Promise<number[]>} Sorted unique non-negative integer object IDs.
 */
export async function fetchArcgisPermitObjectIds(source, fetchImpl = fetch) {
  const body = new URLSearchParams({
    where: "1=1",
    returnIdsOnly: "true",
    f: "json",
  });
  const response = await fetchImpl(source.queryUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
      Accept: "application/json",
    },
    body,
    signal: AbortSignal.timeout(60_000),
  });
  const payload = await readArcgisJson(response);
  const ids = /** @type {ArcgisObjectIdResponse} */ (payload).objectIds;
  if (
    !Array.isArray(ids) ||
    !ids.every(
      (value) =>
        typeof value === "number" &&
        Number.isInteger(value) &&
        value >= 0,
    )
  ) {
    throw new Error("ArcGIS permit source returned invalid object IDs");
  }
  const unique = [...new Set(/** @type {number[]} */ (ids))].sort(
    (left, right) => left - right,
  );
  if (unique.length !== ids.length) {
    throw new Error("ArcGIS permit source returned duplicate object IDs");
  }
  return unique;
}

/**
 * Fetch one exact source chunk by immutable object IDs.
 *
 * @param {BrowardArcgisPermitSource} source - Verified official source.
 * @param {readonly number[]} objectIds - Exact sorted IDs, bounded by source limit.
 * @param {typeof fetch} [fetchImpl=fetch] - Injectable standards-compatible fetch.
 * @returns {Promise<{features:ArcgisPermitFeature[],rawText:string}>}
 *   Exact ordered features and raw private source response.
 */
export async function fetchArcgisPermitFeatures(
  source,
  objectIds,
  fetchImpl = fetch,
) {
  if (
    objectIds.length === 0 ||
    objectIds.length > source.maxChunkSize ||
    !objectIds.every(
      (value) =>
        typeof value === "number" &&
        Number.isInteger(value) &&
        value >= 0,
    )
  ) {
    throw new Error(
      `ArcGIS permit chunk must contain 1 through ${String(source.maxChunkSize)} object IDs`,
    );
  }
  const body = new URLSearchParams({
    objectIds: objectIds.join(","),
    outFields: "*",
    returnGeometry: "false",
    orderByFields: "OBJECTID ASC",
    f: "json",
  });
  const response = await fetchImpl(source.queryUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
      Accept: "application/json",
    },
    body,
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) {
    throw new Error(`ArcGIS permit source returned HTTP ${String(response.status)}`);
  }
  const rawText = await response.text();
  const parsed = /** @type {unknown} */ (JSON.parse(rawText));
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("ArcGIS permit source returned a non-object feature page");
  }
  const payload = /** @type {ArcgisFeatureResponse} */ (parsed);
  if (payload.error !== undefined) {
    throw new Error("ArcGIS permit source returned a feature-page error");
  }
  if (payload.exceededTransferLimit === true) {
    throw new Error("ArcGIS permit source truncated an object-ID chunk");
  }
  if (!Array.isArray(payload.features)) {
    throw new Error("ArcGIS permit source returned no feature array");
  }
  const requested = new Set(objectIds);
  const observed = new Set();
  for (const feature of payload.features) {
    if (
      feature === null ||
      typeof feature !== "object" ||
      Array.isArray(feature) ||
      feature.attributes === null ||
      typeof feature.attributes !== "object" ||
      Array.isArray(feature.attributes)
    ) {
      throw new Error("ArcGIS permit source returned a malformed feature");
    }
    const id = feature.attributes.OBJECTID;
    if (
      typeof id !== "number" ||
      !Number.isInteger(id) ||
      !requested.has(id) ||
      observed.has(id)
    ) {
      throw new Error("ArcGIS permit source returned mismatched feature IDs");
    }
    observed.add(id);
  }
  if (observed.size !== requested.size) {
    throw new Error("ArcGIS permit source omitted requested feature IDs");
  }
  return {
    features: [...payload.features].sort(
      (left, right) =>
        Number(left.attributes.OBJECTID) - Number(right.attributes.OBJECTID),
    ),
    rawText,
  };
}
