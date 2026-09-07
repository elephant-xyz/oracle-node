#!/usr/bin/env node

import { createHash } from "node:crypto";
import { once } from "node:events";
import { createWriteStream } from "node:fs";
import {
  mkdir,
  open,
  readFile,
  readdir,
  rename,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";

/**
 * @typedef {Record<string, unknown>} JsonObject
 *
 * @typedef {object} LakelandArcgisHarvestOptions
 * @property {"probe" | "harvest" | "verify"} stage Operation to perform.
 * @property {string} output Aggregate normalized JSONL destination.
 * @property {string} receipt Receipt destination.
 * @property {string} stateDirectory Content-addressed part directory.
 * @property {string} checkpoint Checkpoint destination.
 * @property {number} pageSize Maximum ArcGIS records requested per page.
 * @property {number} timeoutMs Per-request timeout.
 * @property {number} attempts Total attempts for transient source failures.
 * @property {number} retryDelayMs Initial exponential retry delay.
 * @property {number | null} limit Optional bounded pilot record count.
 * @property {boolean} approveScale Explicit approval for more than 100 rows.
 *
 * @typedef {object} LakelandArcgisCheckpoint
 * @property {"oracle-node.polk-lakeland-arcgis-checkpoint.v1"} schemaVersion Checkpoint schema.
 * @property {string} layerUrl Pinned source layer.
 * @property {string} output Aggregate output path.
 * @property {string} stateDirectory Part directory path.
 * @property {number} pageSize Page size contract.
 * @property {number | null} limit Pilot limit contract.
 * @property {number} snapshotMaxObjectId Inclusive source snapshot boundary.
 * @property {number} snapshotRecordCount Expected rows at the boundary.
 * @property {number} completedPartCount Number of committed contiguous parts.
 * @property {number} harvestedRecordCount Number of committed records.
 * @property {number} lastObjectId Last committed source object ID.
 * @property {boolean} complete Whether the requested snapshot is complete.
 * @property {string} updatedAt Checkpoint update timestamp.
 *
 * @typedef {object} LakelandArcgisPermitRecord
 * @property {"oracle-node.polk-lakeland-arcgis-permit.v1"} schemaVersion Record schema.
 * @property {"lakeland_arcgis_permit_layer"} sourceSystem Stable source identifier.
 * @property {string} sourceRecordKey Global-ID-backed row identity.
 * @property {number} sourceObjectId ArcGIS object ID used for keyset pagination.
 * @property {string} sourceGlobalId ArcGIS stable global ID.
 * @property {string} sourceUrl Direct ArcGIS object query URL.
 * @property {string} retrievedAt Retrieval timestamp.
 * @property {string} permitNumber Permit identifier exactly as published.
 * @property {string | null} description Published work description.
 * @property {string | null} permitType Published permit/project type.
 * @property {string | null} applicantName Published applicant.
 * @property {string | null} appliedAt ISO application timestamp.
 * @property {string | null} approvedAt ISO approval timestamp.
 * @property {string | null} issuedAt ISO issue timestamp.
 * @property {number | null} jobValueUsd Published job value.
 * @property {{line1:string,city:string | null,stateCode:string | null,postalCode:string | null,sourceAddressId:string | null}} siteAddress Published location evidence.
 * @property {{x:number | null,y:number | null,wkid:2237}} sourceCoordinates Source State Plane coordinates.
 * @property {number | null} appliedFiscalYear Published fiscal year.
 * @property {string | null} icon Published source category icon.
 * @property {string | null} createdBy Source audit creator.
 * @property {string | null} createdAt Source audit creation timestamp.
 * @property {string | null} updatedBy Source audit editor.
 * @property {string | null} updatedAt Source audit update timestamp.
 * @property {null} parcelIdentifier No parcel is inferred from an address.
 * @property {null} propertyMatch No property relationship is guessed.
 */

export const LAKELAND_ARCGIS_LAYER_URL =
  "https://services1.arcgis.com/mcbQY5xNGGGM1vBX/ArcGIS/rest/services/IMS_Projects_Permits/FeatureServer/6";
export const LAKELAND_ARCGIS_QUERY_URL = `${LAKELAND_ARCGIS_LAYER_URL}/query`;
export const LAKELAND_ARCGIS_WHERE = "TYPE = 'Permit'";
export const LAKELAND_ARCGIS_FIELDS = Object.freeze([
  "OBJECTID",
  "GLOBALID",
  "PERMIT_NO",
  "DESCRIPTION",
  "SITE_ADDR",
  "SITE_CITY",
  "SITE_STATE",
  "SITE_ZIP",
  "ADDRESSID",
  "XCOORD",
  "YCOORD",
  "PERMITORPROJECTTYPE",
  "APPLICANT_NAME",
  "APPLIED",
  "APPROVED",
  "ISSUED",
  "JOBVALUE",
  "ICON",
  "APPLIEDFY",
  "CREATED_USER",
  "CREATED_DATE",
  "LAST_EDITED_USER",
  "LAST_EDITED_DATE",
]);

const CHECKPOINT_SCHEMA = "oracle-node.polk-lakeland-arcgis-checkpoint.v1";
const RECORD_SCHEMA = "oracle-node.polk-lakeland-arcgis-permit.v1";
const PART_PATTERN = /^part-(\d{6})\.([a-f0-9]{64})\.jsonl$/;

/**
 * Test whether an unknown value is a non-array JSON object.
 *
 * @param {unknown} value Candidate value.
 * @returns {value is JsonObject} Whether the value is an object.
 */
function isJsonObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Parse one bounded positive integer.
 *
 * @param {string | undefined} value Raw CLI value.
 * @param {string} name Option name.
 * @param {number} fallback Default value.
 * @param {number} maximum Inclusive maximum.
 * @returns {number} Parsed integer.
 */
function readPositiveInteger(value, name, fallback, maximum) {
  if (value === undefined) return fallback;
  const parsed = Number.parseInt(value, 10);
  if (
    !Number.isSafeInteger(parsed) ||
    parsed < 1 ||
    parsed > maximum ||
    String(parsed) !== value
  ) {
    throw new Error(`--${name} must be an integer from 1 through ${maximum}`);
  }
  return parsed;
}

/**
 * Parse the Lakeland ArcGIS permit CLI.
 *
 * @param {readonly string[]} argv Arguments excluding node and script.
 * @returns {LakelandArcgisHarvestOptions} Validated options.
 */
export function parseLakelandArcgisHarvestOptions(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      stage: { type: "string", default: "probe" },
      output: {
        type: "string",
        default: "tmp/polk/permits/lakeland-arcgis-permits.jsonl",
      },
      receipt: { type: "string" },
      "state-dir": { type: "string" },
      checkpoint: { type: "string" },
      "page-size": { type: "string" },
      "timeout-ms": { type: "string" },
      attempts: { type: "string" },
      "retry-delay-ms": { type: "string" },
      limit: { type: "string" },
      "approve-scale": { type: "boolean" },
    },
    strict: true,
  });
  if (
    values.stage !== "probe" &&
    values.stage !== "harvest" &&
    values.stage !== "verify"
  ) {
    throw new Error("--stage must be probe, harvest, or verify");
  }
  const limit =
    values.limit === undefined
      ? null
      : readPositiveInteger(values.limit, "limit", 1, 1_000_000);
  return {
    stage: values.stage,
    output: values.output,
    receipt: values.receipt ?? `${values.output}.receipt.json`,
    stateDirectory: values["state-dir"] ?? `${values.output}.parts`,
    checkpoint: values.checkpoint ?? `${values.output}.checkpoint.json`,
    pageSize: readPositiveInteger(
      values["page-size"],
      "page-size",
      2_000,
      16_000,
    ),
    timeoutMs: readPositiveInteger(
      values["timeout-ms"],
      "timeout-ms",
      30_000,
      120_000,
    ),
    attempts: readPositiveInteger(values.attempts, "attempts", 3, 10),
    retryDelayMs: readPositiveInteger(
      values["retry-delay-ms"],
      "retry-delay-ms",
      1_000,
      60_000,
    ),
    limit,
    approveScale: values["approve-scale"] === true,
  };
}

/**
 * Build a frozen-snapshot ArcGIS page URL using OBJECTID keyset pagination.
 *
 * @param {{afterObjectId:number,maxObjectId:number,pageSize:number}} options Page bounds.
 * @returns {string} Query URL.
 */
export function buildLakelandArcgisPageUrl(options) {
  const where = `${LAKELAND_ARCGIS_WHERE} AND OBJECTID > ${options.afterObjectId} AND OBJECTID <= ${options.maxObjectId}`;
  const params = new URLSearchParams({
    where,
    outFields: LAKELAND_ARCGIS_FIELDS.join(","),
    returnGeometry: "false",
    orderByFields: "OBJECTID ASC",
    resultRecordCount: String(options.pageSize),
    f: "json",
  });
  return `${LAKELAND_ARCGIS_QUERY_URL}?${params.toString()}`;
}

/**
 * Build a count URL for the current or frozen source boundary.
 *
 * @param {number | null} maxObjectId Inclusive boundary or null for current.
 * @returns {string} Count query URL.
 */
export function buildLakelandArcgisCountUrl(maxObjectId) {
  const where =
    maxObjectId === null
      ? LAKELAND_ARCGIS_WHERE
      : `${LAKELAND_ARCGIS_WHERE} AND OBJECTID <= ${maxObjectId}`;
  const params = new URLSearchParams({
    where,
    returnCountOnly: "true",
    f: "json",
  });
  return `${LAKELAND_ARCGIS_QUERY_URL}?${params.toString()}`;
}

/**
 * Build a source-bounds statistics URL.
 *
 * @returns {string} Statistics query URL.
 */
export function buildLakelandArcgisBoundsUrl() {
  const statistics = [
    {
      statisticType: "min",
      onStatisticField: "OBJECTID",
      outStatisticFieldName: "min_oid",
    },
    {
      statisticType: "max",
      onStatisticField: "OBJECTID",
      outStatisticFieldName: "max_oid",
    },
    {
      statisticType: "min",
      onStatisticField: "APPLIED",
      outStatisticFieldName: "min_applied",
    },
    {
      statisticType: "max",
      onStatisticField: "APPLIED",
      outStatisticFieldName: "max_applied",
    },
  ];
  const params = new URLSearchParams({
    where: LAKELAND_ARCGIS_WHERE,
    outStatistics: JSON.stringify(statistics),
    returnGeometry: "false",
    f: "json",
  });
  return `${LAKELAND_ARCGIS_QUERY_URL}?${params.toString()}`;
}

/**
 * Read a nullable trimmed source string.
 *
 * @param {unknown} value Source value.
 * @returns {string | null} Trimmed text.
 */
function sourceText(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/**
 * Read a nullable finite source number.
 *
 * @param {unknown} value Source value.
 * @returns {number | null} Finite number.
 */
function sourceNumber(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

/**
 * Convert an ArcGIS millisecond timestamp to ISO text.
 *
 * @param {unknown} value Source timestamp.
 * @returns {string | null} ISO timestamp.
 */
function sourceDate(value) {
  const milliseconds = sourceNumber(value);
  if (milliseconds === null) return null;
  const date = new Date(milliseconds);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

/**
 * Normalize one official Lakeland ArcGIS feature without inferring a parcel.
 *
 * @param {unknown} feature ArcGIS feature.
 * @param {string} retrievedAt Retrieval timestamp.
 * @returns {LakelandArcgisPermitRecord} Normalized source record.
 */
export function normalizeLakelandArcgisPermit(feature, retrievedAt) {
  if (!isJsonObject(feature) || !isJsonObject(feature.attributes)) {
    throw new Error("Lakeland ArcGIS feature is missing attributes");
  }
  const attributes = feature.attributes;
  const objectId = sourceNumber(attributes.OBJECTID);
  const globalId = sourceText(attributes.GLOBALID);
  const permitNumber = sourceText(attributes.PERMIT_NO);
  const siteAddress = sourceText(attributes.SITE_ADDR);
  if (
    objectId === null ||
    !Number.isSafeInteger(objectId) ||
    objectId < 1 ||
    globalId === null ||
    permitNumber === null ||
    siteAddress === null
  ) {
    throw new Error(
      "Lakeland ArcGIS permit requires OBJECTID, GLOBALID, PERMIT_NO, and SITE_ADDR",
    );
  }
  const directParams = new URLSearchParams({
    objectIds: String(objectId),
    outFields: LAKELAND_ARCGIS_FIELDS.join(","),
    returnGeometry: "false",
    f: "json",
  });
  return {
    schemaVersion: RECORD_SCHEMA,
    sourceSystem: "lakeland_arcgis_permit_layer",
    sourceRecordKey: `lakeland_arcgis:${globalId.toLowerCase()}`,
    sourceObjectId: objectId,
    sourceGlobalId: globalId,
    sourceUrl: `${LAKELAND_ARCGIS_QUERY_URL}?${directParams.toString()}`,
    retrievedAt,
    permitNumber,
    description: sourceText(attributes.DESCRIPTION),
    permitType: sourceText(attributes.PERMITORPROJECTTYPE),
    applicantName: sourceText(attributes.APPLICANT_NAME),
    appliedAt: sourceDate(attributes.APPLIED),
    approvedAt: sourceDate(attributes.APPROVED),
    issuedAt: sourceDate(attributes.ISSUED),
    jobValueUsd: sourceNumber(attributes.JOBVALUE),
    siteAddress: {
      line1: siteAddress,
      city: sourceText(attributes.SITE_CITY),
      stateCode: sourceText(attributes.SITE_STATE),
      postalCode: sourceText(attributes.SITE_ZIP),
      sourceAddressId: sourceText(attributes.ADDRESSID),
    },
    sourceCoordinates: {
      x: sourceNumber(attributes.XCOORD),
      y: sourceNumber(attributes.YCOORD),
      wkid: 2237,
    },
    appliedFiscalYear: sourceNumber(attributes.APPLIEDFY),
    icon: sourceText(attributes.ICON),
    createdBy: sourceText(attributes.CREATED_USER),
    createdAt: sourceDate(attributes.CREATED_DATE),
    updatedBy: sourceText(attributes.LAST_EDITED_USER),
    updatedAt: sourceDate(attributes.LAST_EDITED_DATE),
    parcelIdentifier: null,
    propertyMatch: null,
  };
}

/**
 * Validate one normalized record's stable identity.
 *
 * @param {unknown} record Candidate record.
 * @returns {record is LakelandArcgisPermitRecord} Whether the record is reusable.
 */
function isLakelandArcgisPermitRecord(record) {
  return (
    isJsonObject(record) &&
    record.schemaVersion === RECORD_SCHEMA &&
    record.sourceSystem === "lakeland_arcgis_permit_layer" &&
    typeof record.sourceRecordKey === "string" &&
    Number.isSafeInteger(record.sourceObjectId) &&
    Number(record.sourceObjectId) > 0 &&
    typeof record.sourceGlobalId === "string" &&
    typeof record.permitNumber === "string" &&
    record.permitNumber.length > 0 &&
    isJsonObject(record.siteAddress) &&
    typeof record.siteAddress.line1 === "string" &&
    record.siteAddress.line1.length > 0 &&
    record.parcelIdentifier === null &&
    record.propertyMatch === null
  );
}

/**
 * Validate one content-addressed page and its keyset ordering.
 *
 * @param {readonly unknown[]} records Page records.
 * @param {number} afterObjectId Previous page boundary.
 * @param {number} maxObjectId Snapshot upper bound.
 * @param {number} pageSize Configured maximum page size.
 * @returns {asserts records is LakelandArcgisPermitRecord[]} Throws on corruption.
 */
export function assertLakelandArcgisPermitPage(
  records,
  afterObjectId,
  maxObjectId,
  pageSize,
) {
  if (records.length < 1 || records.length > pageSize) {
    throw new Error("Lakeland ArcGIS part has an invalid record count");
  }
  const globalIds = new Set();
  let previousObjectId = afterObjectId;
  for (const record of records) {
    if (!isLakelandArcgisPermitRecord(record)) {
      throw new Error("Lakeland ArcGIS part contains an invalid permit record");
    }
    if (
      record.sourceObjectId <= previousObjectId ||
      record.sourceObjectId > maxObjectId
    ) {
      throw new Error(
        "Lakeland ArcGIS part is not strictly ordered within its snapshot",
      );
    }
    const normalizedGlobalId = record.sourceGlobalId.toLowerCase();
    if (globalIds.has(normalizedGlobalId)) {
      throw new Error("Lakeland ArcGIS part contains a duplicate GLOBALID");
    }
    globalIds.add(normalizedGlobalId);
    previousObjectId = record.sourceObjectId;
  }
}

/**
 * Fetch one ArcGIS JSON response with timeout and bounded retries.
 *
 * @param {string} url Request URL.
 * @param {{timeoutMs:number,attempts:number,retryDelayMs:number}} settings Retry settings.
 * @param {typeof fetch} fetchImpl Fetch implementation.
 * @returns {Promise<JsonObject>} Parsed ArcGIS response.
 */
async function fetchArcgisJson(url, settings, fetchImpl) {
  /** @type {unknown} */
  let lastError = null;
  for (let attempt = 1; attempt <= settings.attempts; attempt += 1) {
    try {
      const response = await fetchImpl(url, {
        headers: { accept: "application/json" },
        signal: AbortSignal.timeout(settings.timeoutMs),
      });
      if (!response.ok) {
        throw new Error(`Lakeland ArcGIS returned HTTP ${response.status}`);
      }
      const value = /** @type {unknown} */ (await response.json());
      if (!isJsonObject(value)) {
        throw new Error("Lakeland ArcGIS returned a non-object response");
      }
      if (isJsonObject(value.error)) {
        throw new Error(
          `Lakeland ArcGIS error ${String(value.error.code ?? "unknown")}: ${String(value.error.message ?? "")}`,
        );
      }
      return value;
    } catch (caught) {
      lastError = caught;
      if (attempt === settings.attempts) break;
      await new Promise((resolve) =>
        setTimeout(resolve, settings.retryDelayMs * 2 ** (attempt - 1)),
      );
    }
  }
  throw lastError instanceof Error
    ? lastError
    : new Error("Lakeland ArcGIS request failed");
}

/**
 * Read the current or bounded source row count.
 *
 * @param {number | null} maxObjectId Snapshot boundary.
 * @param {{timeoutMs:number,attempts:number,retryDelayMs:number}} settings Retry settings.
 * @param {typeof fetch} fetchImpl Fetch implementation.
 * @returns {Promise<number>} Source count.
 */
async function readSourceCount(maxObjectId, settings, fetchImpl) {
  const response = await fetchArcgisJson(
    buildLakelandArcgisCountUrl(maxObjectId),
    settings,
    fetchImpl,
  );
  if (!Number.isSafeInteger(response.count) || Number(response.count) < 0) {
    throw new Error("Lakeland ArcGIS count response is invalid");
  }
  return Number(response.count);
}

/**
 * Read source OBJECTID and application-date bounds.
 *
 * @param {{timeoutMs:number,attempts:number,retryDelayMs:number}} settings Retry settings.
 * @param {typeof fetch} fetchImpl Fetch implementation.
 * @returns {Promise<{minObjectId:number,maxObjectId:number,minAppliedAt:string | null,maxAppliedAt:string | null}>} Source bounds.
 */
async function readSourceBounds(settings, fetchImpl) {
  const response = await fetchArcgisJson(
    buildLakelandArcgisBoundsUrl(),
    settings,
    fetchImpl,
  );
  const firstFeature = Array.isArray(response.features)
    ? response.features[0]
    : null;
  const attributes =
    isJsonObject(firstFeature) && isJsonObject(firstFeature.attributes)
      ? firstFeature.attributes
      : null;
  const minObjectId = attributes ? sourceNumber(attributes.min_oid) : null;
  const maxObjectId = attributes ? sourceNumber(attributes.max_oid) : null;
  if (
    minObjectId === null ||
    maxObjectId === null ||
    !Number.isSafeInteger(minObjectId) ||
    !Number.isSafeInteger(maxObjectId)
  ) {
    throw new Error("Lakeland ArcGIS bounds response is invalid");
  }
  return {
    minObjectId,
    maxObjectId,
    minAppliedAt: attributes ? sourceDate(attributes.min_applied) : null,
    maxAppliedAt: attributes ? sourceDate(attributes.max_applied) : null,
  };
}

/**
 * Atomically replace a UTF-8 text file.
 *
 * @param {string} destination Destination.
 * @param {string} text Complete content.
 * @returns {Promise<void>} Resolves after rename.
 */
async function writeAtomicText(destination, text) {
  await mkdir(path.dirname(destination), { recursive: true });
  const temporary = `${destination}.tmp-${process.pid}-${Date.now()}`;
  try {
    await writeFile(temporary, text, "utf8");
    await rename(temporary, destination);
  } catch (caught) {
    await rm(temporary, { force: true });
    throw caught;
  }
}

/**
 * Compute a SHA-256 digest.
 *
 * @param {string} text Text content.
 * @returns {string} Lowercase hexadecimal digest.
 */
function sha256Text(text) {
  return createHash("sha256").update(text).digest("hex");
}

/**
 * Commit one immutable content-addressed page.
 *
 * @param {string} stateDirectory Part directory.
 * @param {number} partIndex Zero-based part index.
 * @param {readonly LakelandArcgisPermitRecord[]} records Records.
 * @returns {Promise<string>} Final part path.
 */
async function writePermitPart(stateDirectory, partIndex, records) {
  const text =
    records.map((record) => JSON.stringify(record)).join("\n") +
    (records.length > 0 ? "\n" : "");
  const digest = sha256Text(text);
  const fileName = `part-${String(partIndex).padStart(6, "0")}.${digest}.jsonl`;
  const destination = path.join(stateDirectory, fileName);
  await writeAtomicText(destination, text);
  return destination;
}

/**
 * Read a JSON object or return null when absent.
 *
 * @param {string} filePath JSON file.
 * @returns {Promise<JsonObject | null>} Parsed object.
 */
async function readOptionalJsonObject(filePath) {
  try {
    const value = /** @type {unknown} */ (
      JSON.parse(await readFile(filePath, "utf8"))
    );
    if (!isJsonObject(value)) throw new Error(`${filePath} is not an object`);
    return value;
  } catch (caught) {
    if (
      caught instanceof Error &&
      "code" in caught &&
      /** @type {NodeJS.ErrnoException} */ (caught).code === "ENOENT"
    ) {
      return null;
    }
    throw caught;
  }
}

/**
 * Parse and validate a checkpoint.
 *
 * @param {string} checkpointPath Checkpoint path.
 * @returns {Promise<LakelandArcgisCheckpoint | null>} Valid checkpoint.
 */
async function readCheckpoint(checkpointPath) {
  const value = await readOptionalJsonObject(checkpointPath);
  if (value === null) return null;
  if (
    value.schemaVersion !== CHECKPOINT_SCHEMA ||
    typeof value.layerUrl !== "string" ||
    typeof value.output !== "string" ||
    typeof value.stateDirectory !== "string" ||
    !Number.isSafeInteger(value.pageSize) ||
    (value.limit !== null && !Number.isSafeInteger(value.limit)) ||
    !Number.isSafeInteger(value.snapshotMaxObjectId) ||
    !Number.isSafeInteger(value.snapshotRecordCount) ||
    !Number.isSafeInteger(value.completedPartCount) ||
    !Number.isSafeInteger(value.harvestedRecordCount) ||
    !Number.isSafeInteger(value.lastObjectId) ||
    typeof value.complete !== "boolean" ||
    typeof value.updatedAt !== "string"
  ) {
    throw new Error(`Invalid Lakeland ArcGIS checkpoint: ${checkpointPath}`);
  }
  return /** @type {LakelandArcgisCheckpoint} */ (value);
}

/**
 * Assert that a checkpoint can resume with the requested options.
 *
 * @param {LakelandArcgisCheckpoint} checkpoint Checkpoint.
 * @param {LakelandArcgisHarvestOptions} options Current options.
 * @returns {void} Throws on incompatible settings.
 */
function assertCheckpointCompatible(checkpoint, options) {
  if (
    checkpoint.layerUrl !== LAKELAND_ARCGIS_LAYER_URL ||
    checkpoint.output !== options.output ||
    checkpoint.stateDirectory !== options.stateDirectory ||
    checkpoint.pageSize !== options.pageSize ||
    checkpoint.limit !== options.limit
  ) {
    throw new Error(
      "Lakeland ArcGIS checkpoint is incompatible; preserve it and use a new output/state directory.",
    );
  }
}

/**
 * Write a monotonic checkpoint.
 *
 * @param {string} checkpointPath Destination.
 * @param {Omit<LakelandArcgisCheckpoint, "schemaVersion" | "updatedAt">} checkpoint Checkpoint body.
 * @returns {Promise<void>} Resolves after atomic replacement.
 */
async function writeCheckpoint(checkpointPath, checkpoint) {
  const previous = await readCheckpoint(checkpointPath);
  if (
    previous !== null &&
    (checkpoint.completedPartCount < previous.completedPartCount ||
      checkpoint.harvestedRecordCount < previous.harvestedRecordCount ||
      checkpoint.lastObjectId < previous.lastObjectId)
  ) {
    throw new Error("Lakeland ArcGIS checkpoint cannot rewind");
  }
  await writeAtomicText(
    checkpointPath,
    `${JSON.stringify(
      {
        schemaVersion: CHECKPOINT_SCHEMA,
        ...checkpoint,
        updatedAt: new Date().toISOString(),
      },
      null,
      2,
    )}\n`,
  );
}

/**
 * Verify all committed content-addressed parts.
 *
 * @param {string} stateDirectory Part directory.
 * @param {number} maxObjectId Snapshot upper bound.
 * @param {number} pageSize Configured page size.
 * @returns {Promise<{partPaths:string[],recordCount:number,lastObjectId:number,globalIds:Set<string>}>} Verified state.
 */
async function verifyParts(stateDirectory, maxObjectId, pageSize) {
  let entries;
  try {
    entries = await readdir(stateDirectory);
  } catch (caught) {
    if (
      caught instanceof Error &&
      "code" in caught &&
      /** @type {NodeJS.ErrnoException} */ (caught).code === "ENOENT"
    ) {
      return {
        partPaths: [],
        recordCount: 0,
        lastObjectId: 0,
        globalIds: new Set(),
      };
    }
    throw caught;
  }
  const matched = entries
    .flatMap((entry) => {
      const match = PART_PATTERN.exec(entry);
      return match === null
        ? []
        : [
            {
              entry,
              index: Number.parseInt(match[1], 10),
              digest: match[2],
            },
          ];
    })
    .sort((left, right) => left.index - right.index);
  /** @type {string[]} */
  const partPaths = [];
  const globalIds = new Set();
  let recordCount = 0;
  let lastObjectId = 0;
  for (let position = 0; position < matched.length; position += 1) {
    const part = matched[position];
    if (part === undefined || part.index !== position) {
      throw new Error("Lakeland ArcGIS committed parts are not contiguous");
    }
    const partPath = path.join(stateDirectory, part.entry);
    const text = await readFile(partPath, "utf8");
    if (sha256Text(text) !== part.digest) {
      throw new Error(`Lakeland ArcGIS part digest mismatch: ${partPath}`);
    }
    const records = text
      .split(/\r?\n/)
      .filter((line) => line.trim().length > 0)
      .map((line) => /** @type {unknown} */ (JSON.parse(line)));
    assertLakelandArcgisPermitPage(
      records,
      lastObjectId,
      maxObjectId,
      pageSize,
    );
    for (const record of records) {
      if (!isLakelandArcgisPermitRecord(record)) continue;
      const globalId = record.sourceGlobalId.toLowerCase();
      if (globalIds.has(globalId)) {
        throw new Error(
          `Duplicate Lakeland ArcGIS GLOBALID across parts: ${globalId}`,
        );
      }
      globalIds.add(globalId);
      lastObjectId = record.sourceObjectId;
    }
    recordCount += records.length;
    partPaths.push(partPath);
  }
  return { partPaths, recordCount, lastObjectId, globalIds };
}

/**
 * Acquire an exclusive state-directory writer lock.
 *
 * @param {string} stateDirectory State directory.
 * @returns {Promise<() => Promise<void>>} Async release callback.
 */
async function acquireRunLock(stateDirectory) {
  await mkdir(stateDirectory, { recursive: true });
  const lockPath = path.join(stateDirectory, ".run.lock");
  try {
    const handle = await open(lockPath, "wx");
    await handle.writeFile(
      `${JSON.stringify({ pid: process.pid, startedAt: new Date().toISOString() })}\n`,
      "utf8",
    );
    await handle.close();
  } catch (caught) {
    if (
      !(caught instanceof Error) ||
      !("code" in caught) ||
      /** @type {NodeJS.ErrnoException} */ (caught).code !== "EEXIST"
    ) {
      throw caught;
    }
    const existing = await readOptionalJsonObject(lockPath);
    const pid =
      existing !== null && Number.isSafeInteger(existing.pid)
        ? Number(existing.pid)
        : null;
    let active = false;
    if (pid !== null) {
      try {
        process.kill(pid, 0);
        active = true;
      } catch (probeError) {
        if (
          !(probeError instanceof Error) ||
          !("code" in probeError) ||
          /** @type {NodeJS.ErrnoException} */ (probeError).code !== "ESRCH"
        ) {
          throw probeError;
        }
      }
    }
    if (active || pid === null) {
      throw new Error(`Lakeland ArcGIS state is locked: ${lockPath}`);
    }
    await rm(lockPath);
    return acquireRunLock(stateDirectory);
  }
  return async () => {
    await rm(lockPath, { force: true });
  };
}

/**
 * Assemble immutable parts into the normalized aggregate JSONL.
 *
 * @param {readonly string[]} partPaths Ordered part paths.
 * @param {string} output Destination.
 * @returns {Promise<void>} Resolves after atomic replacement.
 */
async function assembleOutput(partPaths, output) {
  await mkdir(path.dirname(output), { recursive: true });
  const temporary = `${output}.tmp-${process.pid}-${Date.now()}`;
  const writer = createWriteStream(temporary, { encoding: "utf8" });
  try {
    for (const partPath of partPaths) {
      const text = await readFile(partPath, "utf8");
      if (!writer.write(text)) await once(writer, "drain");
    }
    writer.end();
    await once(writer, "finish");
    await rename(temporary, output);
  } catch (caught) {
    writer.destroy();
    await rm(temporary, { force: true });
    throw caught;
  }
}

/**
 * Compute a file SHA-256 digest without loading it into memory.
 *
 * @param {string} filePath File path.
 * @returns {Promise<string>} Digest.
 */
async function sha256File(filePath) {
  const hash = createHash("sha256");
  const stream = (await import("node:fs")).createReadStream(filePath);
  for await (const chunk of stream) hash.update(chunk);
  return hash.digest("hex");
}

/**
 * Probe, harvest, or verify the Lakeland ArcGIS source.
 *
 * @param {LakelandArcgisHarvestOptions} options Validated options.
 * @param {typeof fetch} [fetchImpl] Injectable fetch implementation.
 * @returns {Promise<JsonObject>} Receipt.
 */
export async function runLakelandArcgisPermits(
  options,
  fetchImpl = globalThis.fetch,
) {
  const requestSettings = {
    timeoutMs: options.timeoutMs,
    attempts: options.attempts,
    retryDelayMs: options.retryDelayMs,
  };
  if (options.stage === "probe") {
    const [sourceRecordCount, bounds] = await Promise.all([
      readSourceCount(null, requestSettings, fetchImpl),
      readSourceBounds(requestSettings, fetchImpl),
    ]);
    return {
      schemaVersion: "oracle-node.polk-lakeland-arcgis-probe.v1",
      probedAt: new Date().toISOString(),
      layerUrl: LAKELAND_ARCGIS_LAYER_URL,
      where: LAKELAND_ARCGIS_WHERE,
      sourceRecordCount,
      ...bounds,
      maxRecordCount: 16_000,
      pagination: "OBJECTID keyset with inclusive frozen upper bound",
      parcelMatchPolicy: "preserve source row; never infer parcel from address",
      safeConcurrency: 4,
    };
  }

  const checkpoint = await readCheckpoint(options.checkpoint);
  if (checkpoint === null) {
    if (options.stage === "verify") {
      throw new Error("Lakeland ArcGIS verification requires a checkpoint");
    }
    if (
      !options.approveScale &&
      (options.limit === null || options.limit > 100)
    ) {
      throw new Error(
        "Lakeland ArcGIS harvest above 100 rows requires --approve-scale",
      );
    }
  } else {
    assertCheckpointCompatible(checkpoint, options);
  }

  /** @type {() => Promise<void>} */
  let releaseLock = async () => {};
  if (options.stage === "harvest") {
    releaseLock = await acquireRunLock(options.stateDirectory);
  }
  try {
    let activeCheckpoint = checkpoint;
    if (activeCheckpoint === null) {
      const [currentCount, bounds] = await Promise.all([
        readSourceCount(null, requestSettings, fetchImpl),
        readSourceBounds(requestSettings, fetchImpl),
      ]);
      const boundedCount = await readSourceCount(
        bounds.maxObjectId,
        requestSettings,
        fetchImpl,
      );
      if (currentCount !== boundedCount) {
        throw new Error(
          "Lakeland ArcGIS source changed while establishing the snapshot; retry with a new state directory.",
        );
      }
      activeCheckpoint = {
        schemaVersion: CHECKPOINT_SCHEMA,
        layerUrl: LAKELAND_ARCGIS_LAYER_URL,
        output: options.output,
        stateDirectory: options.stateDirectory,
        pageSize: options.pageSize,
        limit: options.limit,
        snapshotMaxObjectId: bounds.maxObjectId,
        snapshotRecordCount: currentCount,
        completedPartCount: 0,
        harvestedRecordCount: 0,
        lastObjectId: 0,
        complete: false,
        updatedAt: new Date().toISOString(),
      };
      await writeCheckpoint(options.checkpoint, {
        layerUrl: activeCheckpoint.layerUrl,
        output: activeCheckpoint.output,
        stateDirectory: activeCheckpoint.stateDirectory,
        pageSize: activeCheckpoint.pageSize,
        limit: activeCheckpoint.limit,
        snapshotMaxObjectId: activeCheckpoint.snapshotMaxObjectId,
        snapshotRecordCount: activeCheckpoint.snapshotRecordCount,
        completedPartCount: 0,
        harvestedRecordCount: 0,
        lastObjectId: 0,
        complete: false,
      });
    }
    const verified = await verifyParts(
      options.stateDirectory,
      activeCheckpoint.snapshotMaxObjectId,
      options.pageSize,
    );
    if (
      activeCheckpoint.completedPartCount > verified.partPaths.length ||
      activeCheckpoint.harvestedRecordCount > verified.recordCount ||
      activeCheckpoint.lastObjectId > verified.lastObjectId
    ) {
      throw new Error(
        "Lakeland ArcGIS checkpoint claims work that did not verify",
      );
    }
    const targetRecordCount =
      options.limit === null
        ? activeCheckpoint.snapshotRecordCount
        : Math.min(options.limit, activeCheckpoint.snapshotRecordCount);

    if (options.stage === "verify") {
      return {
        schemaVersion: "oracle-node.polk-lakeland-arcgis-verification.v1",
        verifiedAt: new Date().toISOString(),
        output: options.output,
        checkpoint: options.checkpoint,
        stateDirectory: options.stateDirectory,
        verifiedPartCount: verified.partPaths.length,
        verifiedRecordCount: verified.recordCount,
        uniqueGlobalIdCount: verified.globalIds.size,
        lastObjectId: verified.lastObjectId,
        snapshotMaxObjectId: activeCheckpoint.snapshotMaxObjectId,
        targetRecordCount,
        complete: verified.recordCount === targetRecordCount,
      };
    }

    const boundedSourceCount = await readSourceCount(
      activeCheckpoint.snapshotMaxObjectId,
      requestSettings,
      fetchImpl,
    );
    if (boundedSourceCount !== activeCheckpoint.snapshotRecordCount) {
      throw new Error(
        "Lakeland ArcGIS frozen source boundary changed; preserve the committed parts and investigate source drift.",
      );
    }

    let partPaths = verified.partPaths;
    let harvestedRecordCount = verified.recordCount;
    let lastObjectId = verified.lastObjectId;
    while (harvestedRecordCount < targetRecordCount) {
      const requestedPageSize = Math.min(
        options.pageSize,
        targetRecordCount - harvestedRecordCount,
      );
      const response = await fetchArcgisJson(
        buildLakelandArcgisPageUrl({
          afterObjectId: lastObjectId,
          maxObjectId: activeCheckpoint.snapshotMaxObjectId,
          pageSize: requestedPageSize,
        }),
        requestSettings,
        fetchImpl,
      );
      if (!Array.isArray(response.features) || response.features.length === 0) {
        throw new Error(
          `Lakeland ArcGIS exhausted after ${harvestedRecordCount} of ${targetRecordCount} expected rows`,
        );
      }
      const retrievedAt = new Date().toISOString();
      const records = response.features.map((feature) =>
        normalizeLakelandArcgisPermit(feature, retrievedAt),
      );
      assertLakelandArcgisPermitPage(
        records,
        lastObjectId,
        activeCheckpoint.snapshotMaxObjectId,
        requestedPageSize,
      );
      for (const record of records) {
        const globalId = record.sourceGlobalId.toLowerCase();
        if (verified.globalIds.has(globalId)) {
          throw new Error(
            `Lakeland ArcGIS returned duplicate GLOBALID ${globalId}`,
          );
        }
        verified.globalIds.add(globalId);
      }
      const partPath = await writePermitPart(
        options.stateDirectory,
        partPaths.length,
        records,
      );
      partPaths = [...partPaths, partPath];
      harvestedRecordCount += records.length;
      lastObjectId =
        records[records.length - 1]?.sourceObjectId ?? lastObjectId;
      await writeCheckpoint(options.checkpoint, {
        layerUrl: LAKELAND_ARCGIS_LAYER_URL,
        output: options.output,
        stateDirectory: options.stateDirectory,
        pageSize: options.pageSize,
        limit: options.limit,
        snapshotMaxObjectId: activeCheckpoint.snapshotMaxObjectId,
        snapshotRecordCount: activeCheckpoint.snapshotRecordCount,
        completedPartCount: partPaths.length,
        harvestedRecordCount,
        lastObjectId,
        complete: harvestedRecordCount === targetRecordCount,
      });
      process.stdout.write(
        `${JSON.stringify({
          event: "polk_lakeland_arcgis_progress",
          completedPartCount: partPaths.length,
          harvestedRecordCount,
          targetRecordCount,
          lastObjectId,
        })}\n`,
      );
    }
    const endBoundedCount = await readSourceCount(
      activeCheckpoint.snapshotMaxObjectId,
      requestSettings,
      fetchImpl,
    );
    if (
      harvestedRecordCount !== targetRecordCount ||
      verified.globalIds.size !== targetRecordCount ||
      endBoundedCount !== activeCheckpoint.snapshotRecordCount
    ) {
      throw new Error("Lakeland ArcGIS final snapshot reconciliation failed");
    }
    await assembleOutput(partPaths, options.output);
    const outputInfo = await stat(options.output);
    const currentSourceRecordCount = await readSourceCount(
      null,
      requestSettings,
      fetchImpl,
    );
    const receipt = {
      schemaVersion: "oracle-node.polk-lakeland-arcgis-harvest.v1",
      harvestedAt: new Date().toISOString(),
      layerUrl: LAKELAND_ARCGIS_LAYER_URL,
      where: LAKELAND_ARCGIS_WHERE,
      output: options.output,
      checkpoint: options.checkpoint,
      stateDirectory: options.stateDirectory,
      pageSize: options.pageSize,
      snapshotMaxObjectId: activeCheckpoint.snapshotMaxObjectId,
      snapshotRecordCount: activeCheckpoint.snapshotRecordCount,
      currentSourceRecordCount,
      harvestedRecordCount,
      uniqueGlobalIdCount: verified.globalIds.size,
      completedPartCount: partPaths.length,
      outputBytes: outputInfo.size,
      outputSha256: await sha256File(options.output),
      pilot: options.limit !== null,
      requestedLimit: options.limit,
      sourceDriftAfterSnapshot:
        currentSourceRecordCount !== activeCheckpoint.snapshotRecordCount,
      parcelMatchPolicy: "unmatched permits preserved; no inferred relation",
      complete: harvestedRecordCount === targetRecordCount,
      countyCoverageComplete: false,
    };
    await writeAtomicText(
      options.receipt,
      `${JSON.stringify(receipt, null, 2)}\n`,
    );
    return receipt;
  } finally {
    await releaseLock();
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  runLakelandArcgisPermits(
    parseLakelandArcgisHarvestOptions(process.argv.slice(2)),
  )
    .then((receipt) => {
      process.stdout.write(`${JSON.stringify(receipt, null, 2)}\n`);
    })
    .catch((caught) => {
      const error = caught instanceof Error ? caught.message : String(caught);
      process.stderr.write(
        `${JSON.stringify({ event: "polk_lakeland_arcgis_failed", error })}\n`,
      );
      process.exitCode = 1;
    });
}
