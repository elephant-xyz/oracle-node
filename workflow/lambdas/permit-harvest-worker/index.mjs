import {
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { createHash } from "crypto";
import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from "@aws-sdk/client-secrets-manager";
import {
  GetQueueAttributesCommand,
  SendMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import AdmZip from "adm-zip";
import { parse } from "csv-parse";
import {
  extractLeeAppraisalMediaLinks,
  inferMediaExtension,
  mapAppraisalTransformedFile,
  mapLeePermitDetail,
  upsertPreparedRows,
} from "./query-db-loader/index.js";
import { Pool } from "pg";
import {
  buildPermitOutputStem,
  buildWindowKey,
  captureLeePermitDetail,
  consoleLogger,
  createBrowser,
  normalizeParcelSearchValue,
  searchLeePermitParcel,
  searchLeePermitWindow,
  shortHash,
} from "./lee-accela.mjs";
import {
  extractCorporateDataS3ObjectByZip,
  matchCorporateDataS3Object,
  safeKeyPart as safeSunbizKeyPart,
} from "./sunbiz-corporate.mjs";

/**
 * @typedef {import("./lee-accela.mjs").PermitLink} PermitLink
 */

/**
 * @typedef {import("./sunbiz-corporate.mjs").SunbizAddressInput} SunbizAddressInput
 */

/**
 * @typedef {object} LeePermitListWindowMessage
 * @property {"lee-permit-list-window"} type - Message discriminator.
 * @property {1} version - Message version.
 * @property {string} jobId - Stable job identifier.
 * @property {string} startDate - ISO start date.
 * @property {string} endDate - ISO end date.
 * @property {string | undefined} [portalUrl] - Optional Accela portal URL.
 * @property {string | undefined} [outputPrefix] - S3 prefix for outputs.
 * @property {number | undefined} [maxPages] - Maximum result pages to crawl.
 * @property {number | undefined} [detailBatchSize] - Detail permits per SQS message.
 * @property {number | undefined} [splitThreshold] - Split multi-day windows at or above this reported result count.
 */

/**
 * @typedef {object} LeePermitDetailBatchMessage
 * @property {"lee-permit-detail-batch"} type - Message discriminator.
 * @property {1} version - Message version.
 * @property {string} jobId - Stable job identifier.
 * @property {string} windowKey - Source date-window key.
 * @property {number} batchIndex - Zero-based batch index.
 * @property {PermitLink[]} permits - Permit detail links to capture.
 * @property {string | undefined} [outputPrefix] - S3 prefix for outputs.
 * @property {boolean | undefined} [skipExisting] - Skip permit outputs whose JSON object already exists.
 */

/**
 * @typedef {object} LeePropertyFirstPermitParcelMessage
 * @property {"lee-property-first-permit-parcel"} type - Message discriminator.
 * @property {1} version - Message version.
 * @property {string} jobId - Stable job identifier.
 * @property {string} parcelIdentifier - Lee Appraiser parcel/STRAP identifier to search in Accela.
 * @property {string | undefined} [requestIdentifier] - Lee Appraiser request/Folio identifier used to locate the already-loaded property row.
 * @property {string | undefined} [appraisalOutputS3Uri] - Transformed appraisal artifact URI used to locate the already-loaded property row.
 * @property {string | undefined} [appraisalPreparedOutputS3Uri] - Prepared appraisal ZIP URI containing raw Lee HTML used for parcel media capture.
 * @property {string | undefined} [propertyId] - Query-db property UUID, included for observability only.
 * @property {string | undefined} [propertyUsageType] - Appraiser usage type, included for observability and prioritization audits.
 * @property {string | undefined} [bestPermitAddress] - Best known appraiser situs address for provenance on harvested permit artifacts.
 * @property {string | undefined} [addressBase] - Normalized appraiser address base for provenance on harvested permit artifacts.
 * @property {string | undefined} [portalUrl] - Optional Accela portal URL.
 * @property {string | undefined} [outputPrefix] - S3 prefix for outputs.
 * @property {number | undefined} [maxPages] - Maximum Accela result pages to capture for this parcel.
 * @property {boolean | undefined} [skipExisting] - Skip permit outputs whose JSON object already exists.
 * @property {boolean | undefined} [skipCompleted] - Skip parcels with an existing property-first completion state object.
 * @property {boolean | undefined} [loadToNeon] - Merge captured permit details into Neon and link them to the appraiser property. Defaults true.
 * @property {boolean | undefined} [loadAppraisalToNeon] - Merge appraisalOutputS3Uri into Neon in the same transaction as permits. Defaults true when appraisalOutputS3Uri is present.
 * @property {boolean | undefined} [onDemand] - On-demand MCP harvest. When true, the parcel harvests regardless of appraiser usage-type eligibility and a missing appraiser property row links zero permits instead of rolling back the permit upsert. Defaults false/absent (bulk/seed behavior).
 */

/**
 * @typedef {object} LeePropertyFirstSeedBackpressureQueue
 * @property {string} name - Human-readable queue label for logs and state snapshots.
 * @property {string} queueUrl - SQS queue URL to inspect before enqueueing more appraisal work.
 * @property {number} maxMessages - Maximum visible + in-flight + delayed messages allowed before the feeder pauses.
 */

/**
 * County-generic property-first seed feeder message. The handler routes any
 * message whose `type` matches `<county-slug>-property-first-seed-feeder` (e.g.
 * `lee-property-first-seed-feeder`, `palm-beach-property-first-seed-feeder`)
 * through the same config-driven feeder; per-parcel workflow routing comes from
 * the seed CSV's `county` column, not this message.
 *
 * @typedef {object} PropertyFirstSeedFeederMessage
 * @property {"lee-property-first-seed-feeder"} type - Canonical discriminant kept as a literal so the `PermitHarvestMessage` union still narrows; at runtime the handler routes any `<county-slug>-property-first-seed-feeder` type (see `isPropertyFirstSeedFeederType`).
 * @property {1} version - Message version.
 * @property {string} jobId - Stable job identifier shared by generated appraisal workflow messages.
 * @property {string} sourceCsvS3Uri - County seed CSV S3 URI.
 * @property {string} workflowQueueUrl - Existing appraisal workflow starter queue URL.
 * @property {string} propertyFirstPermitQueueUrl - Existing property-first permit queue URL, passed into appraisal workflow messages.
 * @property {string} feederQueueUrl - Queue URL where the feeder reschedules itself with an SQS delay.
 * @property {string} generatedSeedPrefix - S3 prefix for generated one-property appraisal seed CSVs.
 * @property {string} workflowOutputBaseUri - S3 base URI for appraisal workflow outputs.
 * @property {string} propertyFirstPermitOutputPrefix - S3 prefix consumed by property-first permit workers; worker appends jobId.
 * @property {string} stateS3Uri - S3 JSON state/checkpoint URI used to resume the next source row.
 * @property {string | undefined} [sourceSystem] - Neon `properties.source_system` value used by the `skipExistingNeon` dedup. Defaults to `lee_appraiser` for backward compatibility.
 * @property {LeePropertyFirstSeedBackpressureQueue[] | undefined} [backpressureQueues] - Queues that must be below their thresholds before enqueueing more work.
 * @property {number | undefined} [batchSize] - Maximum new source rows to enqueue during one feeder wakeup.
 * @property {number | undefined} [maxPages] - Maximum Accela parcel-search pages per property.
 * @property {number | undefined} [requeueDelaySeconds] - SQS delay before the next feeder wakeup.
 * @property {number | undefined} [sendDelayMs] - Optional delay between workflow SQS sends.
 * @property {boolean | undefined} [skipExistingNeon] - Skip source rows already loaded for this `sourceSystem` into Neon. Defaults true.
 */

/**
 * Backward-compatible alias for {@link PropertyFirstSeedFeederMessage}; other
 * code and JSDoc still reference the original Lee-specific name.
 *
 * @typedef {PropertyFirstSeedFeederMessage} LeePropertyFirstSeedFeederMessage
 */

/**
 * @typedef {object} SunbizCorporateAddressMatchMessage
 * @property {"sunbiz-corporate-address-match"} type - Message discriminator.
 * @property {1} version - Message version.
 * @property {string} jobId - Stable job identifier.
 * @property {string} addressBatchKey - Stable address batch identifier used in the output S3 key.
 * @property {string} sourceDataS3Uri - S3 URI for a Sunbiz corporate fixed-width text file or ZIP containing text files.
 * @property {"text" | "zip" | undefined} [sourceFormat] - Source object format. Defaults to `zip` for `.zip`, otherwise `text`.
 * @property {SunbizAddressInput[]} addressInputs - Addresses to match against Sunbiz principal, mailing, registered-agent, and officer addresses.
 * @property {number | undefined} [maxMatchesPerAddress] - Per-address match cap.
 * @property {string | undefined} [outputPrefix] - S3 prefix for outputs.
 */

/**
 * @typedef {object} SunbizCorporateZipExtractMessage
 * @property {"sunbiz-corporate-zip-extract"} type - Message discriminator.
 * @property {1} version - Message version.
 * @property {string} jobId - Stable job identifier.
 * @property {string} extractKey - Stable ZIP extraction identifier used in output S3 keys.
 * @property {string} sourceDataS3Uri - S3 URI for a Sunbiz corporate fixed-width text file or ZIP containing text files.
 * @property {"text" | "zip" | undefined} [sourceFormat] - Source object format. Defaults to `zip` for `.zip`, otherwise `text`.
 * @property {string[]} zipPrefixes - ZIP prefixes to extract from principal, mailing, registered-agent, and officer addresses.
 * @property {number | undefined} [chunkRecordLimit] - Maximum records per output JSONL chunk.
 * @property {number | undefined} [maxRecords] - Optional matched-record cap for smoke runs.
 * @property {string | undefined} [outputPrefix] - S3 prefix for outputs.
 */

/**
 * @typedef {LeePermitListWindowMessage | LeePermitDetailBatchMessage | LeePropertyFirstPermitParcelMessage | LeePropertyFirstSeedFeederMessage | SunbizCorporateAddressMatchMessage | SunbizCorporateZipExtractMessage} PermitHarvestMessage
 */

/**
 * @typedef {Record<string, string | undefined>} LeeSeedRow
 */

/**
 * @typedef {object} ExistingLeeAppraiserIdentifiers
 * @property {Set<string>} requestIdentifiers - Existing Lee Appraiser request/Folio identifiers in Neon.
 * @property {Set<string>} normalizedParcelIdentifiers - Existing normalized Lee Appraiser parcel identifiers in Neon.
 */

/**
 * @typedef {object} LeePropertyFirstSeedFeederState
 * @property {"permit-harvest.property-first-seed-feeder-state.v2" | "permit-harvest.lee-property-first-seed-feeder-state.v1"} schemaVersion - State schema marker (v2 generic; v1 legacy Lee, accepted on read).
 * @property {string} jobId - Job identifier associated with the checkpoint.
 * @property {string} sourceCsvS3Uri - Source seed CSV associated with the checkpoint.
 * @property {number} nextSourceRowNumber - One-based CSV data row number to inspect on the next wakeup.
 * @property {number} enqueuedCount - Total workflow messages enqueued by this feeder state.
 * @property {number} skippedExistingCount - Total rows skipped because Neon already contained them.
 * @property {number} skippedInvalidCount - Total rows skipped because required source identifiers were missing.
 * @property {boolean} sourceExhausted - True when the feeder reached the end of the seed CSV.
 * @property {string} updatedAt - ISO timestamp for the latest state write.
 * @property {Record<string, unknown> | null} lastRun - Last feeder wakeup summary.
 */

/**
 * @typedef {object} DetailWriteResult
 * @property {string} recordNumber - Accela record number requested.
 * @property {string | null} extractedJsonS3Uri - S3 URI for the extracted permit JSON artifact when available.
 * @property {string | null} rawHtmlS3Uri - S3 URI for the raw permit detail HTML artifact when available.
 * @property {boolean} skipped - True when an existing extracted JSON artifact was reused.
 * @property {string | null} error - Terminal capture error, or null on success.
 */

/**
 * @typedef {object} PropertyFirstTarget
 * @property {string} parcelIdentifier - Original Lee Appraiser parcel/STRAP value.
 * @property {string} normalizedParcelIdentifier - Punctuation-free parcel identifier used in Accela.
 * @property {string | null} requestIdentifier - Appraiser request/Folio identifier.
 * @property {string | null} appraisalOutputS3Uri - Appraiser transformed artifact URI.
 * @property {string | null} appraisalPreparedOutputS3Uri - Appraiser prepared output ZIP URI containing raw Lee HTML.
 * @property {string | null} propertyId - Query-db property UUID supplied by the enqueuer.
 * @property {string | null} propertyUsageType - Appraiser usage type supplied by the enqueuer.
 * @property {string | null} bestPermitAddress - Best known appraiser situs address for provenance.
 * @property {string | null} addressBase - Normalized appraiser address base for provenance.
 */

/**
 * @typedef {object} QueryDbLoadSummary
 * @property {number} appraisalArtifactCount - Number of appraisal ZIP artifacts read from S3.
 * @property {number} appraisalMediaDiscoveredCount - Number of Lee Appraiser media links discovered in raw HTML.
 * @property {number} appraisalMediaStoredCount - Number of Lee Appraiser media objects copied to S3.
 * @property {number} appraisalPreparedRowCount - Number of appraisal logical rows prepared for query-db.
 * @property {number} appraisalSkippedRecordCount - Number of appraisal source records skipped by the mapper.
 * @property {number} permitArtifactCount - Number of permit JSON artifacts read from S3.
 * @property {number} permitPreparedRowCount - Number of permit logical rows prepared for query-db.
 * @property {number} permitSkippedRecordCount - Number of permit source artifacts skipped by the mapper.
 * @property {number} artifactCount - Total number of artifacts read from S3.
 * @property {number} preparedRowCount - Total number of logical rows prepared for query-db.
 * @property {number} skippedRecordCount - Total number of source records skipped by mappers.
 * @property {number} attemptedRows - Number of prepared rows attempted in Neon.
 * @property {number} changedRows - Number of prepared rows inserted or updated in Neon.
 * @property {number} unchangedRows - Number of prepared rows skipped because their source hash was unchanged.
 * @property {number} matchedPermitRows - Lee Accela permit rows in Neon matching this parcel after load.
 * @property {number} linkedPermitRows - Lee Accela permit rows linked to the appraiser property by this invocation.
 * @property {boolean} [propertyMissing] - True only on the on-demand path when the appraiser property was not yet loaded, so linking was deferred. Absent/false otherwise.
 */

/**
 * @typedef {object} PropertyFirstPermitEligibility
 * @property {boolean} shouldEnqueue - Whether this appraiser property should spend Accela permit-search capacity.
 * @property {"eligible_property_usage_type" | "non_commercial_property_usage_type" | "missing_property_usage_type" | "property_usage_type_read_failed"} reason - Stable routing reason for audits and skip-state artifacts.
 * @property {string | null} propertyUsageType - Lee Appraiser usage type used for the routing decision, or null when unavailable.
 * @property {string[]} eligibleUsageTypes - Configured commercial/permit-priority appraiser usage types.
 * @property {string | null} readError - Appraisal artifact read error when usage type could not be read, otherwise null.
 */

/**
 * @typedef {import("./query-db-loader/index.js").PreparedRow} PreparedRow
 */

/**
 * @typedef {object} AppraisalArtifactLoadInput
 * @property {PreparedRow[]} rows - Prepared appraisal rows.
 * @property {number} artifactCount - Number of appraisal ZIP artifacts read.
 * @property {number} mediaDiscoveredCount - Number of raw Lee media links discovered.
 * @property {number} mediaStoredCount - Number of raw Lee media objects copied to S3.
 * @property {number} skippedRecordCount - Number of skipped appraisal source records.
 */

/**
 * @typedef {object} S3UriParts
 * @property {string} bucket - S3 bucket name.
 * @property {string} key - S3 object key or prefix.
 */

const s3 = new S3Client({});
const sqs = new SQSClient({});
const secretsManager = new SecretsManagerClient({});

const DEFAULT_OUTPUT_PREFIX = process.env.PERMIT_HARVEST_OUTPUT_PREFIX;
const QUEUE_URL = process.env.PERMIT_HARVEST_QUEUE_URL;
const QUERY_DB_DATABASE_URL = process.env.QUERY_DB_DATABASE_URL;
const QUERY_DB_DATABASE_URL_SECRET_ARN =
  process.env.QUERY_DB_DATABASE_URL_SECRET_ARN;
const PROPERTY_FIRST_DETAIL_CAPTURE_ATTEMPTS = parsePositiveIntegerEnv(
  process.env.PROPERTY_FIRST_DETAIL_CAPTURE_ATTEMPTS,
  3,
);
const PROPERTY_FIRST_DETAIL_RETRY_DELAY_MS = parsePositiveIntegerEnv(
  process.env.PROPERTY_FIRST_DETAIL_RETRY_DELAY_MS,
  5000,
);
const PROPERTY_FIRST_SEARCH_ATTEMPTS = parsePositiveIntegerEnv(
  process.env.PROPERTY_FIRST_SEARCH_ATTEMPTS,
  3,
);
const PROPERTY_FIRST_SEARCH_RETRY_DELAY_MS = parsePositiveIntegerEnv(
  process.env.PROPERTY_FIRST_SEARCH_RETRY_DELAY_MS,
  10000,
);
const APPRAISAL_MEDIA_FETCH_TIMEOUT_MS = parsePositiveIntegerEnv(
  process.env.APPRAISAL_MEDIA_FETCH_TIMEOUT_MS,
  10000,
);
const DEFAULT_LEE_PROPERTY_FIRST_SEED_BATCH_SIZE = 100;
const DEFAULT_LEE_PROPERTY_FIRST_SEED_MAX_PAGES = 200;
const DEFAULT_LEE_PROPERTY_FIRST_SEED_REQUEUE_DELAY_SECONDS = 900;
const MAX_SQS_DELAY_SECONDS = 900;
/** County-generic feeder checkpoint schema written by this worker. */
const PROPERTY_FIRST_SEED_FEEDER_STATE_SCHEMA_VERSION =
  "permit-harvest.property-first-seed-feeder-state.v2";
/** Legacy Lee-specific checkpoint schema still accepted on read for resume. */
const LEGACY_LEE_PROPERTY_FIRST_SEED_FEEDER_STATE_SCHEMA_VERSION =
  "permit-harvest.lee-property-first-seed-feeder-state.v1";
/** Default Neon source_system used when a feeder message omits sourceSystem. */
const DEFAULT_PROPERTY_FIRST_SEED_FEEDER_SOURCE_SYSTEM = "lee_appraiser";
/** Matches `<county-slug>-property-first-seed-feeder` message discriminators. */
const PROPERTY_FIRST_SEED_FEEDER_TYPE_PATTERN =
  /^[a-z0-9-]+-property-first-seed-feeder$/;

/**
 * Return true when a message `type` is a property-first seed feeder discriminator.
 *
 * @param {unknown} type - Candidate message type.
 * @returns {boolean} True for any `<county-slug>-property-first-seed-feeder` type.
 */
function isPropertyFirstSeedFeederType(type) {
  return (
    typeof type === "string" &&
    PROPERTY_FIRST_SEED_FEEDER_TYPE_PATTERN.test(type)
  );
}

/**
 * Narrow a validated permit-harvest message to a property-first seed feeder.
 *
 * @param {PermitHarvestMessage} message - Validated permit-harvest message.
 * @returns {message is LeePropertyFirstSeedFeederMessage} True for feeder messages.
 */
function isPropertyFirstSeedFeederMessage(message) {
  return isPropertyFirstSeedFeederType(message.type);
}

/**
 * Resolve the Neon `properties.source_system` used for feeder dedup.
 *
 * @param {PropertyFirstSeedFeederMessage} message - Feeder message.
 * @returns {string} Source system, defaulting to Lee for backward compatibility.
 */
function resolveFeederSourceSystem(message) {
  return (
    readOptionalString(message.sourceSystem) ??
    DEFAULT_PROPERTY_FIRST_SEED_FEEDER_SOURCE_SYSTEM
  );
}
const DEFAULT_PROPERTY_FIRST_PERMIT_ELIGIBLE_USAGE_TYPES = [
  "AutoSalesRepair",
  "Commercial",
  "DepartmentStore",
  "FinancialInstitution",
  "Hotel",
  "Industrial",
  "LightManufacturing",
  "MobileHomePark",
  "OfficeBuilding",
  "PrivateHospital",
  "Restaurant",
  "ShoppingCenterCommunity",
  "ShoppingCenterRegional",
  "Supermarket",
];
const ALL_PROPERTY_FIRST_PERMIT_USAGE_TYPES_SENTINEL = "__ALL__";

const APPRAISAL_TABLE_ORDER = [
  "unnormalized_addresses",
  "addresses",
  "parcels",
  "properties",
  "property_improvements",
  "people",
  "companies",
  "deeds",
  "fact_sheets",
  "geometries",
  "sales_histories",
  "taxes",
  "property_valuations",
  "structures",
  "utilities",
  "layouts",
  "lots",
  "flood_storm_information",
  "files",
  "ownerships",
];

const PERMIT_TABLE_ORDER = [
  "addresses",
  "people",
  "companies",
  "property_improvements",
  "permit_contacts",
  "inspections",
  "permit_events",
  "permit_fees",
  "permit_links",
  "permit_custom_fields",
  "permit_list_windows",
];

const QUERY_DB_TABLE_ORDER = [
  ...APPRAISAL_TABLE_ORDER,
  ...PERMIT_TABLE_ORDER.filter(
    (tableName) => !APPRAISAL_TABLE_ORDER.includes(tableName),
  ),
];

/** @type {string | null} */
let cachedQueryDatabaseUrl = null;

/** @type {Pool | null} */
let queryDatabasePool = null;

/**
 * Parse an optional positive-integer environment variable.
 *
 * @param {string | undefined} value - Raw environment value.
 * @param {number} fallback - Default positive integer when the value is unset.
 * @returns {number} Parsed positive integer.
 */
function parsePositiveIntegerEnv(value, fallback) {
  if (value === undefined || value.trim().length === 0) return fallback;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(
      `Expected positive integer environment value, received ${value}`,
    );
  }
  return parsed;
}

/**
 * Return a trimmed string, or null when the value is absent or blank.
 *
 * @param {unknown} value - Candidate string value.
 * @returns {string | null} Trimmed string when usable.
 */
function readOptionalString(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/**
 * Return true when the value is a non-array JSON object.
 *
 * @param {unknown} value - Candidate object.
 * @returns {value is Record<string, unknown>} True for JSON-like objects.
 */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * Parse an S3 URI into bucket and key components.
 *
 * @param {string} uri - S3 URI in `s3://bucket/key` format.
 * @returns {S3UriParts} Parsed parts.
 */
function parseS3Uri(uri) {
  const match = /^s3:\/\/([^/]+)\/(.*)$/i.exec(uri);
  if (!match) {
    throw new Error(`Invalid S3 URI: ${uri}`);
  }
  return { bucket: match[1], key: match[2].replace(/\/$/, "") };
}

/**
 * Resolve an output prefix from a message or environment default.
 *
 * @param {string | undefined} messagePrefix - Optional message-level S3 prefix.
 * @param {string} jobId - Job identifier.
 * @returns {S3UriParts & { uri: string }} Resolved prefix parts.
 */
function resolveOutputPrefix(messagePrefix, jobId) {
  const base = (messagePrefix ?? DEFAULT_OUTPUT_PREFIX)?.replace(/\/$/, "");
  if (!base) {
    throw new Error(
      "outputPrefix is required when PERMIT_HARVEST_OUTPUT_PREFIX is not set",
    );
  }
  const uri = `${base}/${jobId}`;
  const { bucket, key } = parseS3Uri(uri);
  return { uri, bucket, key };
}

/**
 * Write a UTF-8 object to S3.
 *
 * @param {object} params - Upload parameters.
 * @param {string} params.bucket - Target bucket.
 * @param {string} params.key - Target key.
 * @param {string} params.body - Text body.
 * @param {string} params.contentType - Content type.
 * @returns {Promise<string>} S3 URI of the object.
 */
async function putTextObject({ bucket, key, body, contentType }) {
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: Buffer.from(body, "utf8"),
      ContentType: contentType,
    }),
  );
  return `s3://${bucket}/${key}`;
}

/**
 * Write a binary object to S3.
 *
 * @param {object} params - Upload parameters.
 * @param {string} params.bucket - Target bucket.
 * @param {string} params.key - Target key.
 * @param {Buffer} params.body - Binary body.
 * @param {string | null} params.contentType - Optional content type.
 * @returns {Promise<string>} S3 URI of the object.
 */
async function putBufferObject({ bucket, key, body, contentType }) {
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: body,
      ...(contentType === null ? {} : { ContentType: contentType }),
    }),
  );
  return `s3://${bucket}/${key}`;
}

/**
 * Read and parse a JSON object from S3.
 *
 * @template T
 * @param {string} uri - S3 URI.
 * @returns {Promise<T>} Parsed JSON value.
 */
async function readJsonFromS3(uri) {
  const { bucket, key } = parseS3Uri(uri);
  const response = await s3.send(
    new GetObjectCommand({ Bucket: bucket, Key: key }),
  );
  const bytes = await response.Body?.transformToByteArray();
  if (!bytes) throw new Error(`Failed to read S3 object body: ${uri}`);
  return JSON.parse(Buffer.from(bytes).toString("utf8"));
}

/**
 * Read a binary S3 object into memory.
 *
 * @param {string} uri - S3 URI.
 * @returns {Promise<Buffer>} Object body bytes.
 */
async function readBufferFromS3(uri) {
  const { bucket, key } = parseS3Uri(uri);
  const response = await s3.send(
    new GetObjectCommand({ Bucket: bucket, Key: key }),
  );
  const bytes = await response.Body?.transformToByteArray();
  if (!bytes) throw new Error(`Failed to read S3 object body: ${uri}`);
  return Buffer.from(bytes);
}

/**
 * Resolve Lee Appraiser usage types that should continue to Accela permit retrieval.
 *
 * @returns {string[]} Commercial/industrial/permit-priority appraiser usage types.
 */
function resolvePropertyFirstPermitEligibleUsageTypes() {
  const raw = process.env.PROPERTY_FIRST_PERMIT_ELIGIBLE_USAGE_TYPES;
  if (raw === undefined || raw.trim().length === 0) {
    return [...DEFAULT_PROPERTY_FIRST_PERMIT_ELIGIBLE_USAGE_TYPES];
  }
  const values = raw
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
  return values.length > 0
    ? values
    : [...DEFAULT_PROPERTY_FIRST_PERMIT_ELIGIBLE_USAGE_TYPES];
}

/**
 * Convert an appraiser usage type into a stable comparison key.
 *
 * @param {string | null} propertyUsageType - Raw appraiser property usage type.
 * @returns {string | null} Case-insensitive comparison key, or null.
 */
function normalizePropertyUsageTypeKey(propertyUsageType) {
  if (propertyUsageType === null) return null;
  const normalized = propertyUsageType.trim().toLowerCase();
  return normalized.length > 0 ? normalized : null;
}

/**
 * Return true when the configured permit coverage scope should include every appraiser usage type.
 *
 * @param {string[]} eligibleUsageTypes - Parsed eligible usage type configuration.
 * @returns {boolean} True when full property-first permit coverage is enabled.
 */
function isFullPropertyFirstPermitCoverageEnabled(eligibleUsageTypes) {
  return eligibleUsageTypes.some(
    (usageType) =>
      usageType.trim().toUpperCase() ===
      ALL_PROPERTY_FIRST_PERMIT_USAGE_TYPES_SENTINEL,
  );
}

/**
 * Build the permit-routing decision from an already-resolved appraiser usage type.
 *
 * @param {object} params - Routing-decision parameters.
 * @param {string | null} params.propertyUsageType - Lee Appraiser usage type used for the routing decision.
 * @param {string | null} params.readError - Artifact read error when usage type could not be read.
 * @param {string[]} params.eligibleUsageTypes - Configured commercial/permit-priority usage type list.
 * @returns {PropertyFirstPermitEligibility} Deterministic routing decision.
 */
function buildPropertyFirstPermitEligibility({
  propertyUsageType,
  readError,
  eligibleUsageTypes,
}) {
  const eligibleUsageTypeKeys = new Set(
    eligibleUsageTypes
      .map((usageType) => normalizePropertyUsageTypeKey(usageType))
      .filter((usageTypeKey) => usageTypeKey !== null),
  );
  const propertyUsageTypeKey = normalizePropertyUsageTypeKey(propertyUsageType);
  const shouldEnqueue =
    isFullPropertyFirstPermitCoverageEnabled(eligibleUsageTypes) ||
    (propertyUsageTypeKey !== null &&
      eligibleUsageTypeKeys.has(propertyUsageTypeKey));
  const reason = shouldEnqueue
    ? "eligible_property_usage_type"
    : readError !== null
      ? "property_usage_type_read_failed"
      : propertyUsageTypeKey === null
        ? "missing_property_usage_type"
        : "non_commercial_property_usage_type";
  return {
    shouldEnqueue,
    reason,
    propertyUsageType,
    eligibleUsageTypes,
    readError,
  };
}

/**
 * Decide whether a property-first parcel should be skipped as ineligible.
 *
 * Bulk/seed work honors the appraiser usage-type gate. On-demand MCP harvests
 * always proceed so donphan can serve their permits regardless of usage type.
 *
 * @param {object} params - Gate parameters.
 * @param {PropertyFirstPermitEligibility} params.eligibility - Resolved routing decision.
 * @param {boolean} params.onDemand - Whether this is an on-demand MCP harvest.
 * @returns {boolean} True when the parcel should be skipped as ineligible.
 */
function shouldSkipIneligiblePropertyFirstParcel({ eligibility, onDemand }) {
  if (onDemand) return false;
  return !eligibility.shouldEnqueue;
}

/**
 * Read the Lee Appraiser property usage type from a transformed appraisal ZIP.
 *
 * @param {string} appraisalOutputS3Uri - S3 URI of `transformed_output.zip`.
 * @returns {Promise<{ propertyUsageType: string | null, readError: string | null }>} Extracted usage type or read failure.
 */
async function readPropertyUsageTypeFromAppraisalOutput(appraisalOutputS3Uri) {
  try {
    const zip = new AdmZip(await readBufferFromS3(appraisalOutputS3Uri));
    const propertyEntry = zip.getEntry("data/property.json");
    if (propertyEntry === null) {
      return {
        propertyUsageType: null,
        readError: "missing data/property.json in transformed output",
      };
    }
    const propertyJson = /** @type {unknown} */ (
      JSON.parse(propertyEntry.getData().toString("utf8"))
    );
    if (!isRecord(propertyJson)) {
      return {
        propertyUsageType: null,
        readError: "data/property.json is not an object",
      };
    }
    return {
      propertyUsageType: readOptionalString(propertyJson.property_usage_type),
      readError: null,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return { propertyUsageType: null, readError: message };
  }
}

/**
 * Resolve whether a property-first permit message should continue to Accela.
 *
 * @param {PropertyFirstTarget} target - Normalized property-first target metadata.
 * @returns {Promise<PropertyFirstPermitEligibility>} Permit-routing decision.
 */
async function resolvePropertyFirstPermitEligibility(target) {
  const eligibleUsageTypes = resolvePropertyFirstPermitEligibleUsageTypes();
  if (target.propertyUsageType !== null) {
    return buildPropertyFirstPermitEligibility({
      propertyUsageType: target.propertyUsageType,
      readError: null,
      eligibleUsageTypes,
    });
  }
  if (target.appraisalOutputS3Uri === null) {
    return buildPropertyFirstPermitEligibility({
      propertyUsageType: null,
      readError: null,
      eligibleUsageTypes,
    });
  }
  const { propertyUsageType, readError } =
    await readPropertyUsageTypeFromAppraisalOutput(target.appraisalOutputS3Uri);
  return buildPropertyFirstPermitEligibility({
    propertyUsageType,
    readError,
    eligibleUsageTypes,
  });
}

/**
 * Sort prepared rows by dependency order while preserving stable order within a table.
 *
 * @param {PreparedRow[]} rows - Prepared rows to sort.
 * @param {string[]} tableOrder - Logical table dependency order.
 * @returns {PreparedRow[]} Sorted prepared rows.
 */
function sortPreparedRows(rows, tableOrder) {
  const order = new Map(
    tableOrder.map((tableName, index) => [tableName, index]),
  );
  return [...rows].sort((left, right) => {
    const leftOrder = order.get(left.tableName) ?? Number.MAX_SAFE_INTEGER;
    const rightOrder = order.get(right.tableName) ?? Number.MAX_SAFE_INTEGER;
    return leftOrder - rightOrder;
  });
}

/**
 * Make a stable S3 key segment from a raw media identity or parcel value.
 *
 * @param {string} value - Source key part.
 * @param {string} fallback - Fallback when the sanitized source is empty.
 * @returns {string} S3-safe key segment.
 */
function safeS3KeyPart(value, fallback) {
  const safe = value.replace(/[^A-Za-z0-9._=-]+/g, "-").replace(/^-+|-+$/g, "");
  return safe.length > 0 ? safe.slice(0, 96) : fallback;
}

/**
 * Build a logical appraisal file record for a Lee media object stored in S3.
 *
 * @param {object} params - Media file row parameters.
 * @param {number} params.index - One-based media record index.
 * @param {import("./query-db-loader/index.js").LeeAppraisalMediaLink} params.link - Lee media link metadata.
 * @param {string} params.requestIdentifier - Lee Appraiser request/Folio identifier.
 * @param {string | null} params.storageUri - S3 URI of the copied media object, or null when download failed.
 * @param {string | null} params.contentSha256 - SHA-256 digest of copied bytes, or null when unavailable.
 * @param {string | null} params.contentType - HTTP content type for copied bytes.
 * @param {string | null} params.error - Download or storage error when media could not be copied.
 * @returns {Record<string, unknown>} JSON payload accepted by the appraisal `file_*.json` mapper.
 */
function buildAppraisalMediaFileRecord({
  index,
  link,
  requestIdentifier,
  storageUri,
  contentSha256,
  contentType,
  error,
}) {
  const storedAt = storageUri === null ? null : new Date().toISOString();
  return {
    request_identifier: requestIdentifier,
    document_type: link.kind,
    file_format: contentType,
    ipfs_url: storageUri,
    name: link.label,
    original_url: link.url,
    source_http_request: {
      url: link.url,
      error,
    },
    source_payload: {
      content_sha256: contentSha256,
      download_error: error,
      media_identity_key: link.identityKey,
      media_index: index,
      media_kind: link.kind,
      original_url: link.url,
      storage_provider: storageUri === null ? null : "s3",
      storage_uri: storageUri,
      stored_at: storedAt,
    },
  };
}

/**
 * Copy one Lee Appraiser media URL into the run's reusable S3 prefix.
 *
 * @param {object} params - Media copy parameters.
 * @param {ReturnType<typeof resolveOutputPrefix>} params.output - Resolved S3 output prefix.
 * @param {PropertyFirstTarget} params.target - Property-first target metadata.
 * @param {import("./query-db-loader/index.js").LeeAppraisalMediaLink} params.link - Media link to copy.
 * @returns {Promise<{ storageUri: string | null, contentSha256: string | null, contentType: string | null, error: string | null }>} Storage result.
 */
async function copyLeeAppraisalMediaToS3({ output, target, link }) {
  const abortController = new AbortController();
  const timeout = setTimeout(
    () => abortController.abort(),
    APPRAISAL_MEDIA_FETCH_TIMEOUT_MS,
  );
  try {
    const response = await fetch(link.url, {
      headers: { "user-agent": "elephant-oracle-node-appraisal-media/0.1" },
      signal: abortController.signal,
    });
    if (!response.ok) {
      throw new Error(`HTTP ${String(response.status)} ${response.statusText}`);
    }
    const contentType = response.headers.get("content-type");
    const bytes = Buffer.from(await response.arrayBuffer());
    const contentSha256 = createHash("sha256").update(bytes).digest("hex");
    const extension = inferMediaExtension({ contentType, url: link.url });
    const key = [
      output.key,
      "lee/appraisal-media",
      safeS3KeyPart(target.normalizedParcelIdentifier, "unknown-parcel"),
      `${safeS3KeyPart(link.identityKey, "media")}-${contentSha256.slice(0, 16)}${extension}`,
    ].join("/");
    const storageUri = await putBufferObject({
      bucket: output.bucket,
      key,
      body: bytes,
      contentType,
    });
    return { storageUri, contentSha256, contentType, error: null };
  } catch (caught) {
    const error = caught instanceof Error ? caught.message : String(caught);
    return { storageUri: null, contentSha256: null, contentType: null, error };
  } finally {
    clearTimeout(timeout);
  }
}

/**
 * Read raw Lee Appraiser HTML from the prepare ZIP, store linked media in S3,
 * and map those media references into appraisal `files` rows.
 *
 * @param {object} params - Media row extraction parameters.
 * @param {string | null} params.preparedOutputS3Uri - Prepared appraisal ZIP URI.
 * @param {PropertyFirstTarget} params.target - Property-first target metadata.
 * @param {ReturnType<typeof resolveOutputPrefix>} params.output - Resolved S3 output prefix.
 * @returns {Promise<{ rows: PreparedRow[], discoveredCount: number, storedCount: number, skippedRecordCount: number }>} Prepared media rows and counters.
 */
async function readAppraisalMediaRowsFromS3({
  preparedOutputS3Uri,
  target,
  output,
}) {
  if (preparedOutputS3Uri === null || target.requestIdentifier === null) {
    return {
      rows: [],
      discoveredCount: 0,
      storedCount: 0,
      skippedRecordCount: 0,
    };
  }
  const zip = new AdmZip(await readBufferFromS3(preparedOutputS3Uri));
  const htmlEntries = zip
    .getEntries()
    .filter(
      (entry) =>
        entry.isDirectory === false && /\.html?$/i.test(entry.entryName),
    )
    .sort((left, right) => left.entryName.localeCompare(right.entryName));
  const mediaLinksByIdentity = new Map();
  for (const entry of htmlEntries) {
    const html = entry.getData().toString("utf8");
    for (const link of extractLeeAppraisalMediaLinks(html)) {
      mediaLinksByIdentity.set(link.identityKey, link);
    }
  }
  const mediaLinks = [...mediaLinksByIdentity.values()].sort((left, right) =>
    left.identityKey.localeCompare(right.identityKey),
  );
  /** @type {PreparedRow[]} */
  const rows = [];
  let skippedRecordCount = 0;
  let storedCount = 0;
  for (const [index, link] of mediaLinks.entries()) {
    const copy = await copyLeeAppraisalMediaToS3({ output, target, link });
    if (copy.storageUri !== null) storedCount += 1;
    const record = buildAppraisalMediaFileRecord({
      index: index + 1,
      link,
      requestIdentifier: target.requestIdentifier,
      storageUri: copy.storageUri,
      contentSha256: copy.contentSha256,
      contentType: copy.contentType,
      error: copy.error,
    });
    const bundle = mapAppraisalTransformedFile({
      artifactUri: preparedOutputS3Uri,
      filePath: `data/file_appraisal_media_${String(index + 1).padStart(3, "0")}.json`,
      record,
      requestIdentifier: target.requestIdentifier,
    });
    rows.push(...bundle.rows);
    skippedRecordCount += bundle.skippedRecords.length;
  }
  return {
    rows,
    discoveredCount: mediaLinks.length,
    storedCount,
    skippedRecordCount,
  };
}

/**
 * Read and map one transformed Lee Appraiser output ZIP from S3.
 *
 * @param {string | null} appraisalOutputS3Uri - Transformed appraisal artifact URI.
 * @returns {Promise<AppraisalArtifactLoadInput>} Prepared appraisal rows and counters.
 */
async function readAppraisalRowsFromS3(appraisalOutputS3Uri) {
  if (appraisalOutputS3Uri === null) {
    return {
      rows: [],
      artifactCount: 0,
      mediaDiscoveredCount: 0,
      mediaStoredCount: 0,
      skippedRecordCount: 0,
    };
  }
  const zip = new AdmZip(await readBufferFromS3(appraisalOutputS3Uri));
  /** @type {PreparedRow[]} */
  const rows = [];
  let skippedRecordCount = 0;
  const entries = zip
    .getEntries()
    .filter(
      (entry) =>
        entry.isDirectory === false && /^data\/.+\.json$/.test(entry.entryName),
    )
    .sort((left, right) => left.entryName.localeCompare(right.entryName));

  for (const entry of entries) {
    const record = /** @type {unknown} */ (
      JSON.parse(entry.getData().toString("utf8"))
    );
    const bundle = mapAppraisalTransformedFile({
      artifactUri: appraisalOutputS3Uri,
      filePath: entry.entryName,
      record,
    });
    rows.push(...bundle.rows);
    skippedRecordCount += bundle.skippedRecords.length;
  }

  return {
    rows: sortPreparedRows(rows, APPRAISAL_TABLE_ORDER),
    artifactCount: 1,
    mediaDiscoveredCount: 0,
    mediaStoredCount: 0,
    skippedRecordCount,
  };
}

/**
 * Check whether an S3 object already exists.
 *
 * @param {string} bucket - S3 bucket.
 * @param {string} key - S3 key.
 * @returns {Promise<boolean>} True when the object exists.
 */
async function objectExists(bucket, key) {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    return true;
  } catch (error) {
    if (error instanceof Error && error.name === "NotFound") return false;
    if (error instanceof Error && error.name === "NoSuchKey") return false;
    if (error && typeof error === "object" && "$metadata" in error) {
      const metadata =
        /** @type {{ $metadata?: { httpStatusCode?: number } }} */ (error)
          .$metadata;
      if (metadata?.httpStatusCode === 404) return false;
    }
    throw error;
  }
}

/**
 * Send a permit-harvest message to the worker queue.
 *
 * @param {PermitHarvestMessage} message - Message to enqueue.
 * @returns {Promise<void>}
 */
async function enqueueMessage(message) {
  if (!QUEUE_URL) {
    throw new Error("PERMIT_HARVEST_QUEUE_URL is not configured");
  }
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: QUEUE_URL,
      MessageBody: JSON.stringify(message),
    }),
  );
}

/**
 * Return the inclusive day span between two ISO dates.
 *
 * @param {string} startDate - ISO start date.
 * @param {string} endDate - ISO end date.
 * @returns {number} Inclusive day count.
 */
function inclusiveDaySpan(startDate, endDate) {
  const start = Date.parse(`${startDate}T00:00:00Z`);
  const end = Date.parse(`${endDate}T00:00:00Z`);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) {
    throw new Error(`Invalid date range: ${startDate}..${endDate}`);
  }
  return Math.floor((end - start) / 86400000) + 1;
}

/**
 * Split an ISO date range into two smaller inclusive windows.
 *
 * @param {string} startDate - ISO start date.
 * @param {string} endDate - ISO end date.
 * @returns {[{ startDate: string, endDate: string }, { startDate: string, endDate: string }]} Split windows.
 */
function splitDateRange(startDate, endDate) {
  const start = Date.parse(`${startDate}T00:00:00Z`);
  const end = Date.parse(`${endDate}T00:00:00Z`);
  const midpoint = start + Math.floor((end - start) / 2);
  const firstEnd = new Date(midpoint).toISOString().slice(0, 10);
  const secondStart = new Date(Date.parse(`${firstEnd}T00:00:00Z`) + 86400000)
    .toISOString()
    .slice(0, 10);
  return [
    { startDate, endDate: firstEnd },
    { startDate: secondStart, endDate },
  ];
}

/**
 * Create batches of permit links.
 *
 * @param {PermitLink[]} permits - Permit links to batch.
 * @param {number} batchSize - Maximum links per batch.
 * @returns {PermitLink[][]} Link batches.
 */
function chunkPermits(permits, batchSize) {
  /** @type {PermitLink[][]} */
  const chunks = [];
  for (let index = 0; index < permits.length; index += batchSize) {
    chunks.push(permits.slice(index, index + batchSize));
  }
  return chunks;
}

/**
 * Build Lee County permit-detail batch messages for permits discovered in a list window.
 *
 * @param {object} params - Detail batch message parameters.
 * @param {LeePermitListWindowMessage} params.sourceMessage - Source list-window message.
 * @param {string} params.windowKey - Source list-window key.
 * @param {PermitLink[]} params.permits - Permit detail links discovered in the source window.
 * @param {number} params.batchSize - Maximum permit links per detail batch.
 * @returns {LeePermitDetailBatchMessage[]} Detail batch messages that preserve the source window and job identifiers.
 */
function buildLeePermitDetailBatchMessages({
  sourceMessage,
  windowKey,
  permits,
  batchSize,
}) {
  return chunkPermits(permits, batchSize).map((batch, batchIndex) => ({
    type: "lee-permit-detail-batch",
    version: 1,
    jobId: sourceMessage.jobId,
    windowKey,
    batchIndex,
    permits: batch,
    outputPrefix: sourceMessage.outputPrefix,
    skipExisting: true,
  }));
}

/**
 * Normalize a CSV parser header row into string column names.
 *
 * @param {unknown[]} header - Raw header values supplied by csv-parse.
 * @returns {string[]} Header values converted to strings.
 */
function normalizeCsvHeader(header) {
  return header.map((column) => String(column));
}

/**
 * Return whether a value behaves like a Node readable stream.
 *
 * @param {unknown} value - Candidate stream value from an AWS SDK S3 response body.
 * @returns {value is NodeJS.ReadableStream} True when `pipe` is available.
 */
function isNodeReadableStream(value) {
  return (
    typeof value === "object" &&
    value !== null &&
    "pipe" in value &&
    typeof (/** @type {{ pipe?: unknown }} */ (value).pipe) === "function"
  );
}

/**
 * Normalize a Lee parcel identifier for duplicate checks and S3 key names.
 *
 * @param {unknown} value - Raw parcel identifier.
 * @returns {string | null} Uppercase alphanumeric parcel identifier.
 */
function normalizeSeedParcelIdentifier(value) {
  const normalized = String(value ?? "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "");
  return normalized.length > 0 ? normalized : null;
}

/**
 * Normalize a parcel identifier to its digit-only form for Lee systems that omit
 * STRAP letters in related permit records.
 *
 * @param {unknown} value - Raw parcel identifier.
 * @returns {string | null} Digit-only parcel identifier, or `null` when no digits are present.
 */
function normalizeParcelDigits(value) {
  const normalized = String(value ?? "").replace(/[^0-9]+/g, "");
  return normalized.length > 0 ? normalized : null;
}

/**
 * Make an arbitrary value safe for a deterministic generated-seed S3 key segment.
 *
 * @param {unknown} value - Source value.
 * @param {string} fallback - Fallback value when the source is blank.
 * @returns {string} S3-key-safe string segment.
 */
function safeSeedKeyPart(value, fallback) {
  const source = readOptionalString(value) ?? fallback;
  return safeS3KeyPart(source, fallback);
}

/**
 * Escape one CSV cell according to RFC4180-style quoting rules.
 *
 * @param {string | undefined} value - Raw cell value.
 * @returns {string} CSV-encoded cell.
 */
function encodeCsvCell(value) {
  const text = value ?? "";
  if (!/[",\r\n]/.test(text)) return text;
  return `"${text.replace(/"/g, '""')}"`;
}

/**
 * Build a one-row CSV body preserving the source header order exactly.
 *
 * @param {string[]} columns - Source CSV column names in order.
 * @param {LeeSeedRow} row - Parsed source seed row.
 * @returns {string} One-row CSV body with header.
 */
function buildOneRowSeedCsv(columns, row) {
  return `${columns.map(encodeCsvCell).join(",")}\n${columns.map((column) => encodeCsvCell(row[column])).join(",")}\n`;
}

/**
 * Return true when an AWS SDK error means the requested S3 object does not exist.
 *
 * @param {unknown} error - Caught AWS SDK error.
 * @returns {boolean} True for 404-style missing object errors.
 */
function isMissingS3ObjectError(error) {
  if (
    error instanceof Error &&
    (error.name === "NotFound" || error.name === "NoSuchKey")
  ) {
    return true;
  }
  if (error && typeof error === "object" && "$metadata" in error) {
    const metadata =
      /** @type {{ $metadata?: { httpStatusCode?: number } }} */ (error)
        .$metadata;
    return metadata?.httpStatusCode === 404;
  }
  return false;
}

/**
 * Build the initial seed-feeder checkpoint state.
 *
 * @param {LeePropertyFirstSeedFeederMessage} message - Feeder message.
 * @returns {LeePropertyFirstSeedFeederState} Initial state.
 */
function createInitialLeePropertyFirstSeedFeederState(message) {
  return {
    schemaVersion: PROPERTY_FIRST_SEED_FEEDER_STATE_SCHEMA_VERSION,
    jobId: message.jobId,
    sourceCsvS3Uri: message.sourceCsvS3Uri,
    nextSourceRowNumber: 1,
    enqueuedCount: 0,
    skippedExistingCount: 0,
    skippedInvalidCount: 0,
    sourceExhausted: false,
    updatedAt: new Date().toISOString(),
    lastRun: null,
  };
}

/**
 * Read the S3 checkpoint used by the seed feeder, or create a new one.
 *
 * @param {LeePropertyFirstSeedFeederMessage} message - Feeder message.
 * @returns {Promise<LeePropertyFirstSeedFeederState>} Current feeder state.
 */
async function readLeePropertyFirstSeedFeederState(message) {
  try {
    const parsed = await readJsonFromS3(message.stateS3Uri);
    if (!isRecord(parsed)) {
      throw new Error(
        `Seed feeder state is not a JSON object: ${message.stateS3Uri}`,
      );
    }
    if (
      parsed.schemaVersion !==
        PROPERTY_FIRST_SEED_FEEDER_STATE_SCHEMA_VERSION &&
      parsed.schemaVersion !==
        LEGACY_LEE_PROPERTY_FIRST_SEED_FEEDER_STATE_SCHEMA_VERSION
    ) {
      throw new Error(
        `Unsupported seed feeder state schema: ${String(parsed.schemaVersion)}`,
      );
    }
    const nextSourceRowNumber =
      typeof parsed.nextSourceRowNumber === "number" &&
      Number.isFinite(parsed.nextSourceRowNumber) &&
      parsed.nextSourceRowNumber > 0
        ? Math.trunc(parsed.nextSourceRowNumber)
        : 1;
    return {
      schemaVersion: PROPERTY_FIRST_SEED_FEEDER_STATE_SCHEMA_VERSION,
      jobId: readOptionalString(parsed.jobId) ?? message.jobId,
      sourceCsvS3Uri:
        readOptionalString(parsed.sourceCsvS3Uri) ?? message.sourceCsvS3Uri,
      nextSourceRowNumber,
      enqueuedCount:
        typeof parsed.enqueuedCount === "number" &&
        Number.isFinite(parsed.enqueuedCount)
          ? Math.trunc(parsed.enqueuedCount)
          : 0,
      skippedExistingCount:
        typeof parsed.skippedExistingCount === "number" &&
        Number.isFinite(parsed.skippedExistingCount)
          ? Math.trunc(parsed.skippedExistingCount)
          : 0,
      skippedInvalidCount:
        typeof parsed.skippedInvalidCount === "number" &&
        Number.isFinite(parsed.skippedInvalidCount)
          ? Math.trunc(parsed.skippedInvalidCount)
          : 0,
      sourceExhausted: parsed.sourceExhausted === true,
      updatedAt:
        readOptionalString(parsed.updatedAt) ?? new Date().toISOString(),
      lastRun: isRecord(parsed.lastRun) ? parsed.lastRun : null,
    };
  } catch (error) {
    if (isMissingS3ObjectError(error)) {
      return createInitialLeePropertyFirstSeedFeederState(message);
    }
    throw error;
  }
}

/**
 * Write the seed-feeder checkpoint to S3.
 *
 * @param {LeePropertyFirstSeedFeederMessage} message - Feeder message with state S3 URI.
 * @param {LeePropertyFirstSeedFeederState} state - State to persist.
 * @returns {Promise<void>} Resolves after the state object is written.
 */
async function writeLeePropertyFirstSeedFeederState(message, state) {
  const target = parseS3Uri(message.stateS3Uri);
  await putTextObject({
    bucket: target.bucket,
    key: target.key,
    body: JSON.stringify(state, null, 2),
    contentType: "application/json; charset=utf-8",
  });
}

/**
 * Read existing appraiser identifiers from Neon so the feeder does not queue
 * properties already loaded by earlier runs for the same source system.
 *
 * @param {boolean} skipExistingNeon - Whether to query Neon for existing identifiers.
 * @param {string} sourceSystem - Neon `properties.source_system` to dedup against.
 * @returns {Promise<ExistingLeeAppraiserIdentifiers>} Existing request and parcel identifiers.
 */
async function readExistingLeeAppraiserIdentifiers(
  skipExistingNeon,
  sourceSystem,
) {
  if (!skipExistingNeon) {
    return {
      requestIdentifiers: new Set(),
      normalizedParcelIdentifiers: new Set(),
    };
  }
  const pool = await getQueryDatabasePool();
  const result = await pool.query(
    `
      select request_identifier, parcel_identifier
      from properties
      where source_system = $1
    `,
    [sourceSystem],
  );
  /** @type {Set<string>} */
  const requestIdentifiers = new Set();
  /** @type {Set<string>} */
  const normalizedParcelIdentifiers = new Set();
  for (const row of result.rows) {
    const record = /** @type {Record<string, unknown>} */ (row);
    const requestIdentifier = readOptionalString(record.request_identifier);
    if (requestIdentifier !== null) requestIdentifiers.add(requestIdentifier);
    const parcelIdentifier = normalizeSeedParcelIdentifier(
      record.parcel_identifier,
    );
    if (parcelIdentifier !== null)
      normalizedParcelIdentifiers.add(parcelIdentifier);
  }
  return { requestIdentifiers, normalizedParcelIdentifiers };
}

/**
 * Return whether a seed row is already represented in Neon.
 *
 * @param {LeeSeedRow} row - Parsed source seed row.
 * @param {ExistingLeeAppraiserIdentifiers} existing - Existing Lee Appraiser identifiers from Neon.
 * @returns {boolean} True when this source row should not be queued again.
 */
function isExistingLeeAppraiserSeedRow(row, existing) {
  const requestIdentifier = readOptionalString(row.source_identifier);
  if (
    requestIdentifier !== null &&
    existing.requestIdentifiers.has(requestIdentifier)
  ) {
    return true;
  }
  const parcelIdentifier = normalizeSeedParcelIdentifier(row.parcel_id);
  return (
    parcelIdentifier !== null &&
    existing.normalizedParcelIdentifiers.has(parcelIdentifier)
  );
}

/**
 * Read approximate SQS queue depth used for feeder backpressure decisions.
 *
 * @param {LeePropertyFirstSeedBackpressureQueue} queue - Queue configuration.
 * @returns {Promise<LeePropertyFirstSeedBackpressureQueue & { visibleMessages: number, notVisibleMessages: number, delayedMessages: number, totalMessages: number }>} Queue depth snapshot.
 */
async function readBackpressureQueueDepth(queue) {
  const response = await sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: queue.queueUrl,
      AttributeNames: [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
      ],
    }),
  );
  const attributes = response.Attributes ?? {};
  const visibleMessages = Number.parseInt(
    attributes.ApproximateNumberOfMessages ?? "0",
    10,
  );
  const notVisibleMessages = Number.parseInt(
    attributes.ApproximateNumberOfMessagesNotVisible ?? "0",
    10,
  );
  const delayedMessages = Number.parseInt(
    attributes.ApproximateNumberOfMessagesDelayed ?? "0",
    10,
  );
  return {
    ...queue,
    visibleMessages: Number.isFinite(visibleMessages) ? visibleMessages : 0,
    notVisibleMessages: Number.isFinite(notVisibleMessages)
      ? notVisibleMessages
      : 0,
    delayedMessages: Number.isFinite(delayedMessages) ? delayedMessages : 0,
    totalMessages:
      (Number.isFinite(visibleMessages) ? visibleMessages : 0) +
      (Number.isFinite(notVisibleMessages) ? notVisibleMessages : 0) +
      (Number.isFinite(delayedMessages) ? delayedMessages : 0),
  };
}

/**
 * Return the backpressure queues to inspect for a feeder wakeup.
 *
 * @param {LeePropertyFirstSeedFeederMessage} message - Feeder message.
 * @returns {LeePropertyFirstSeedBackpressureQueue[]} Queues to inspect.
 */
function resolveBackpressureQueues(message) {
  if (
    message.backpressureQueues !== undefined &&
    message.backpressureQueues.length > 0
  ) {
    return message.backpressureQueues;
  }
  return [
    { name: "workflow", queueUrl: message.workflowQueueUrl, maxMessages: 250 },
  ];
}

/**
 * Send a JSON message to a specific SQS queue.
 *
 * @param {object} params - Queue send parameters.
 * @param {string} params.queueUrl - Target SQS queue URL.
 * @param {Record<string, unknown>} params.message - JSON message body.
 * @param {number | undefined} [params.delaySeconds] - Optional SQS delay in seconds.
 * @returns {Promise<void>} Resolves after SQS accepts the message.
 */
async function sendJsonMessageToQueue({ queueUrl, message, delaySeconds }) {
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: JSON.stringify(message),
      ...(delaySeconds === undefined ? {} : { DelaySeconds: delaySeconds }),
    }),
  );
}

/**
 * Requeue the feeder message with an SQS delay so it keeps topping up work.
 *
 * @param {LeePropertyFirstSeedFeederMessage} message - Feeder message to reschedule.
 * @returns {Promise<void>} Resolves after SQS accepts the delayed feeder message.
 */
async function rescheduleLeePropertyFirstSeedFeeder(message) {
  const delaySeconds = Math.min(
    Math.max(
      Math.trunc(
        message.requeueDelaySeconds ??
          DEFAULT_LEE_PROPERTY_FIRST_SEED_REQUEUE_DELAY_SECONDS,
      ),
      0,
    ),
    MAX_SQS_DELAY_SECONDS,
  );
  await sendJsonMessageToQueue({
    queueUrl: message.feederQueueUrl,
    message: /** @type {Record<string, unknown>} */ (message),
    delaySeconds,
  });
}

/**
 * Upload one generated seed CSV and enqueue its appraisal workflow message.
 *
 * @param {object} params - Upload/enqueue parameters.
 * @param {LeePropertyFirstSeedFeederMessage} params.message - Feeder configuration.
 * @param {string[]} params.columns - Source CSV columns in header order.
 * @param {LeeSeedRow} params.row - Source seed row.
 * @param {number} params.sourceRowNumber - One-based source CSV data row number.
 * @param {number} params.enqueuedIndex - Zero-based index within this feeder wakeup.
 * @returns {Promise<{ seedUri: string, parcelIdentifier: string, requestIdentifier: string | null }>} Generated seed metadata.
 */
async function uploadSeedAndEnqueueWorkflowFromFeeder({
  message,
  columns,
  row,
  sourceRowNumber,
  enqueuedIndex,
}) {
  const seedPrefix = parseS3Uri(message.generatedSeedPrefix);
  const parcelIdentifier = normalizeSeedParcelIdentifier(row.parcel_id);
  if (parcelIdentifier === null) {
    throw new Error(
      `Source row ${String(sourceRowNumber)} is missing parcel_id`,
    );
  }
  const requestIdentifier = readOptionalString(row.source_identifier);
  const requestKeyPart = safeSeedKeyPart(requestIdentifier, "unknown-folio");
  const seedKey = `${seedPrefix.key}/row-${String(sourceRowNumber).padStart(9, "0")}-folio-${requestKeyPart}-parcel-${safeSeedKeyPart(parcelIdentifier, "unknown-parcel")}.csv`;
  const seedUri = `s3://${seedPrefix.bucket}/${seedKey}`;
  await s3.send(
    new PutObjectCommand({
      Bucket: seedPrefix.bucket,
      Key: seedKey,
      Body: Buffer.from(buildOneRowSeedCsv(columns, row), "utf8"),
      ContentType: "text/csv; charset=utf-8",
      Metadata: {
        source: "counties-seeds-lee",
        jobId: message.jobId,
        sourceRowNumber: String(sourceRowNumber),
      },
    }),
  );
  await sendJsonMessageToQueue({
    queueUrl: message.workflowQueueUrl,
    message: {
      s3: {
        bucket: { name: seedPrefix.bucket },
        object: { key: seedKey },
      },
      output_base_uri: message.workflowOutputBaseUri,
      propertyFirstPermitQueueUrl: message.propertyFirstPermitQueueUrl,
      propertyFirstPermitJobId: message.jobId,
      propertyFirstPermitOutputPrefix: message.propertyFirstPermitOutputPrefix,
      propertyFirstPermitMaxPages:
        message.maxPages ?? DEFAULT_LEE_PROPERTY_FIRST_SEED_MAX_PAGES,
      sourceSeedS3Uri: message.sourceCsvS3Uri,
      sourceSeedRowNumber: sourceRowNumber,
      sourceSeedEnqueuedIndex: enqueuedIndex,
    },
  });
  return { seedUri, parcelIdentifier, requestIdentifier };
}

/**
 * Stream the seed CSV from the current checkpoint and enqueue one bounded batch.
 *
 * @param {object} params - Batch parameters.
 * @param {LeePropertyFirstSeedFeederMessage} params.message - Feeder configuration.
 * @param {LeePropertyFirstSeedFeederState} params.state - Current checkpoint.
 * @param {ExistingLeeAppraiserIdentifiers} params.existing - Existing Lee Appraiser identifiers in Neon.
 * @returns {Promise<{ nextSourceRowNumber: number, sourceRowsRead: number, scannedRows: number, enqueued: number, skippedExisting: number, skippedInvalid: number, sourceExhausted: boolean, lastSeedUri: string | null }>} Batch summary.
 */
async function enqueueLeePropertyFirstSeedBatch({ message, state, existing }) {
  const source = parseS3Uri(message.sourceCsvS3Uri);
  const response = await s3.send(
    new GetObjectCommand({ Bucket: source.bucket, Key: source.key }),
  );
  if (!isNodeReadableStream(response.Body)) {
    throw new Error(
      `S3 object body is not a Node readable stream: ${message.sourceCsvS3Uri}`,
    );
  }

  /** @type {string[]} */
  let columns = [];
  const parser = response.Body.pipe(
    parse({
      columns: (header) => {
        columns = normalizeCsvHeader(header);
        return columns;
      },
      relax_column_count: true,
      skip_empty_lines: true,
      trim: false,
    }),
  );
  const batchSize = Math.trunc(
    message.batchSize ?? DEFAULT_LEE_PROPERTY_FIRST_SEED_BATCH_SIZE,
  );
  let sourceRowNumber = 0;
  let scannedRows = 0;
  let enqueued = 0;
  let skippedExisting = 0;
  let skippedInvalid = 0;
  let nextSourceRowNumber = state.nextSourceRowNumber;
  let sourceExhausted = true;
  /** @type {string | null} */
  let lastSeedUri = null;

  for await (const parsedRow of parser) {
    sourceRowNumber += 1;
    if (sourceRowNumber < state.nextSourceRowNumber) continue;
    scannedRows += 1;
    nextSourceRowNumber = sourceRowNumber + 1;
    const row = /** @type {LeeSeedRow} */ (parsedRow);
    const parcelIdentifier = normalizeSeedParcelIdentifier(row.parcel_id);
    const requestIdentifier = readOptionalString(row.source_identifier);
    if (parcelIdentifier === null || requestIdentifier === null) {
      skippedInvalid += 1;
      continue;
    }
    if (isExistingLeeAppraiserSeedRow(row, existing)) {
      skippedExisting += 1;
      continue;
    }
    const result = await uploadSeedAndEnqueueWorkflowFromFeeder({
      message,
      columns,
      row,
      sourceRowNumber,
      enqueuedIndex: enqueued,
    });
    lastSeedUri = result.seedUri;
    enqueued += 1;
    consoleLogger.info("lee_property_first_seed_workflow_enqueued", {
      jobId: message.jobId,
      sourceRowNumber,
      enqueuedIndex: enqueued - 1,
      parcelIdentifier: result.parcelIdentifier,
      requestIdentifier: result.requestIdentifier,
      seedUri: result.seedUri,
    });
    if ((message.sendDelayMs ?? 0) > 0) await delay(message.sendDelayMs ?? 0);
    if (enqueued >= batchSize) {
      sourceExhausted = false;
      break;
    }
  }

  return {
    nextSourceRowNumber,
    sourceRowsRead: sourceRowNumber,
    scannedRows,
    enqueued,
    skippedExisting,
    skippedInvalid,
    sourceExhausted,
    lastSeedUri,
  };
}

/**
 * Validate a parsed SQS message.
 *
 * @param {unknown} value - Parsed message body.
 * @returns {PermitHarvestMessage} Valid message.
 */
function validateMessage(value) {
  if (!value || typeof value !== "object") {
    throw new Error("Permit harvest message must be an object");
  }
  const message = /** @type {Record<string, unknown>} */ (value);
  if (message.version !== 1) {
    throw new Error("Permit harvest message version must be 1");
  }
  if (typeof message.jobId !== "string" || !message.jobId.trim()) {
    throw new Error("Permit harvest message requires jobId");
  }
  if (message.type === "lee-permit-list-window") {
    if (
      typeof message.startDate !== "string" ||
      typeof message.endDate !== "string"
    ) {
      throw new Error("lee-permit-list-window requires startDate and endDate");
    }
    return /** @type {LeePermitListWindowMessage} */ (message);
  }
  if (message.type === "lee-permit-detail-batch") {
    if (!Array.isArray(message.permits)) {
      throw new Error("lee-permit-detail-batch requires permits array");
    }
    return /** @type {LeePermitDetailBatchMessage} */ (message);
  }
  if (message.type === "lee-property-first-permit-parcel") {
    if (
      typeof message.parcelIdentifier !== "string" ||
      !message.parcelIdentifier.trim()
    ) {
      throw new Error(
        "lee-property-first-permit-parcel requires parcelIdentifier",
      );
    }
    if (
      message.maxPages !== undefined &&
      (typeof message.maxPages !== "number" ||
        !Number.isFinite(message.maxPages) ||
        message.maxPages <= 0)
    ) {
      throw new Error(
        "lee-property-first-permit-parcel maxPages must be a positive number",
      );
    }
    if (
      message.onDemand !== undefined &&
      typeof message.onDemand !== "boolean"
    ) {
      throw new Error(
        "lee-property-first-permit-parcel onDemand must be a boolean",
      );
    }
    return /** @type {LeePropertyFirstPermitParcelMessage} */ (message);
  }
  if (isPropertyFirstSeedFeederType(message.type)) {
    const messageType = String(message.type);
    for (const fieldName of [
      "sourceCsvS3Uri",
      "workflowQueueUrl",
      "propertyFirstPermitQueueUrl",
      "feederQueueUrl",
      "generatedSeedPrefix",
      "workflowOutputBaseUri",
      "propertyFirstPermitOutputPrefix",
      "stateS3Uri",
    ]) {
      if (
        typeof message[fieldName] !== "string" ||
        !message[fieldName].trim()
      ) {
        throw new Error(`${messageType} requires ${fieldName}`);
      }
    }
    if (
      message.sourceSystem !== undefined &&
      (typeof message.sourceSystem !== "string" || !message.sourceSystem.trim())
    ) {
      throw new Error(`${messageType} sourceSystem must be a non-empty string`);
    }
    for (const fieldName of ["batchSize", "maxPages", "requeueDelaySeconds"]) {
      const value = message[fieldName];
      if (
        value !== undefined &&
        (typeof value !== "number" || !Number.isFinite(value) || value <= 0)
      ) {
        throw new Error(
          `${messageType} ${fieldName} must be a positive number`,
        );
      }
    }
    if (
      message.sendDelayMs !== undefined &&
      (typeof message.sendDelayMs !== "number" ||
        !Number.isFinite(message.sendDelayMs) ||
        message.sendDelayMs < 0)
    ) {
      throw new Error(
        `${messageType} sendDelayMs must be a non-negative number`,
      );
    }
    if (message.backpressureQueues !== undefined) {
      if (!Array.isArray(message.backpressureQueues)) {
        throw new Error(`${messageType} backpressureQueues must be an array`);
      }
      for (const item of message.backpressureQueues) {
        if (!isRecord(item)) {
          throw new Error(
            `${messageType} backpressure queue entries must be objects`,
          );
        }
        if (typeof item.name !== "string" || !item.name.trim()) {
          throw new Error(`${messageType} backpressure queue requires name`);
        }
        if (typeof item.queueUrl !== "string" || !item.queueUrl.trim()) {
          throw new Error(
            `${messageType} backpressure queue requires queueUrl`,
          );
        }
        if (
          typeof item.maxMessages !== "number" ||
          !Number.isFinite(item.maxMessages) ||
          item.maxMessages <= 0
        ) {
          throw new Error(
            `${messageType} backpressure queue requires positive maxMessages`,
          );
        }
      }
    }
    return /** @type {LeePropertyFirstSeedFeederMessage} */ (message);
  }
  if (message.type === "sunbiz-corporate-address-match") {
    if (typeof message.sourceDataS3Uri !== "string") {
      throw new Error(
        "sunbiz-corporate-address-match requires sourceDataS3Uri",
      );
    }
    if (typeof message.addressBatchKey !== "string") {
      throw new Error(
        "sunbiz-corporate-address-match requires addressBatchKey",
      );
    }
    if (!Array.isArray(message.addressInputs)) {
      throw new Error(
        "sunbiz-corporate-address-match requires addressInputs array",
      );
    }
    return /** @type {SunbizCorporateAddressMatchMessage} */ (message);
  }
  if (message.type === "sunbiz-corporate-zip-extract") {
    if (typeof message.sourceDataS3Uri !== "string") {
      throw new Error("sunbiz-corporate-zip-extract requires sourceDataS3Uri");
    }
    if (typeof message.extractKey !== "string" || !message.extractKey.trim()) {
      throw new Error("sunbiz-corporate-zip-extract requires extractKey");
    }
    if (!Array.isArray(message.zipPrefixes)) {
      throw new Error(
        "sunbiz-corporate-zip-extract requires zipPrefixes array",
      );
    }
    if (!message.zipPrefixes.every((item) => typeof item === "string")) {
      throw new Error(
        "sunbiz-corporate-zip-extract zipPrefixes must be strings",
      );
    }
    return /** @type {SunbizCorporateZipExtractMessage} */ (message);
  }
  throw new Error(
    `Unsupported permit harvest message type: ${String(message.type)}`,
  );
}

/**
 * Resolve the Sunbiz source object format from a message.
 *
 * @param {SunbizCorporateAddressMatchMessage | SunbizCorporateZipExtractMessage} message - Sunbiz source message.
 * @returns {"text" | "zip"} Source object format.
 */
function resolveSunbizSourceFormat(message) {
  if (message.sourceFormat === "text" || message.sourceFormat === "zip") {
    return message.sourceFormat;
  }
  return message.sourceDataS3Uri.toLowerCase().endsWith(".zip")
    ? "zip"
    : "text";
}

/**
 * Process one seed-feeder wakeup: observe queue backpressure, enqueue a bounded
 * batch of new Lee appraisal workflows, checkpoint progress, and reschedule.
 *
 * @param {LeePropertyFirstSeedFeederMessage} message - Seed-feeder message.
 * @returns {Promise<void>} Resolves after the feeder state is updated and, unless exhausted, requeued.
 */
async function processLeePropertyFirstSeedFeeder(message) {
  const state = await readLeePropertyFirstSeedFeederState(message);
  if (state.sourceExhausted) {
    consoleLogger.info("lee_property_first_seed_feeder_exhausted", {
      jobId: message.jobId,
      sourceCsvS3Uri: message.sourceCsvS3Uri,
      stateS3Uri: message.stateS3Uri,
      nextSourceRowNumber: state.nextSourceRowNumber,
      enqueuedCount: state.enqueuedCount,
    });
    return;
  }

  const queueSnapshots = await Promise.all(
    resolveBackpressureQueues(message).map((queue) =>
      readBackpressureQueueDepth(queue),
    ),
  );
  const blockedQueues = queueSnapshots.filter(
    (snapshot) => snapshot.totalMessages >= snapshot.maxMessages,
  );
  if (blockedQueues.length > 0) {
    const blockedState = {
      ...state,
      updatedAt: new Date().toISOString(),
      lastRun: {
        event: "paused_for_backpressure",
        checkedAt: new Date().toISOString(),
        queueSnapshots,
        blockedQueues: blockedQueues.map((queue) => queue.name),
      },
    };
    await writeLeePropertyFirstSeedFeederState(message, blockedState);
    await rescheduleLeePropertyFirstSeedFeeder(message);
    consoleLogger.info("lee_property_first_seed_feeder_paused", {
      jobId: message.jobId,
      stateS3Uri: message.stateS3Uri,
      queueSnapshots,
      blockedQueues: blockedQueues.map((queue) => queue.name),
    });
    return;
  }

  const sourceSystem = resolveFeederSourceSystem(message);
  const existing = await readExistingLeeAppraiserIdentifiers(
    message.skipExistingNeon !== false,
    sourceSystem,
  );
  const batch = await enqueueLeePropertyFirstSeedBatch({
    message,
    state,
    existing,
  });
  const nextState = {
    ...state,
    nextSourceRowNumber: batch.nextSourceRowNumber,
    enqueuedCount: state.enqueuedCount + batch.enqueued,
    skippedExistingCount: state.skippedExistingCount + batch.skippedExisting,
    skippedInvalidCount: state.skippedInvalidCount + batch.skippedInvalid,
    sourceExhausted: batch.sourceExhausted,
    updatedAt: new Date().toISOString(),
    lastRun: {
      event: batch.sourceExhausted ? "source_exhausted" : "batch_enqueued",
      checkedAt: new Date().toISOString(),
      queueSnapshots,
      existingRequestIdentifierCount: existing.requestIdentifiers.size,
      existingParcelIdentifierCount: existing.normalizedParcelIdentifiers.size,
      ...batch,
    },
  };
  await writeLeePropertyFirstSeedFeederState(message, nextState);
  consoleLogger.info("lee_property_first_seed_feeder_complete", {
    jobId: message.jobId,
    stateS3Uri: message.stateS3Uri,
    sourceCsvS3Uri: message.sourceCsvS3Uri,
    nextSourceRowNumber: nextState.nextSourceRowNumber,
    sourceExhausted: nextState.sourceExhausted,
    enqueuedThisRun: batch.enqueued,
    totalEnqueued: nextState.enqueuedCount,
    skippedExistingThisRun: batch.skippedExisting,
    skippedInvalidThisRun: batch.skippedInvalid,
    lastSeedUri: batch.lastSeedUri,
  });
  if (!nextState.sourceExhausted) {
    await rescheduleLeePropertyFirstSeedFeeder(message);
  }
}

/**
 * Process a Lee County date-window search message.
 *
 * @param {LeePermitListWindowMessage} message - Window message.
 * @returns {Promise<void>}
 */
async function processLeePermitListWindow(message) {
  const output = resolveOutputPrefix(message.outputPrefix, message.jobId);
  const maxPages = message.maxPages ?? 200;
  const batchSize = message.detailBatchSize ?? 8;
  const splitThreshold = message.splitThreshold ?? 100;
  const windowKey = buildWindowKey(message.startDate, message.endDate);
  const daySpan = inclusiveDaySpan(message.startDate, message.endDate);
  const browser = await createBrowser(consoleLogger);

  try {
    const result = await searchLeePermitWindow({
      browser,
      startDate: message.startDate,
      endDate: message.endDate,
      portalUrl: message.portalUrl,
      maxPages,
      stopAfterFirstPageWhenTotalAtLeast:
        daySpan > 1 ? splitThreshold : undefined,
      logger: consoleLogger,
    });

    const listPrefix = `${output.key}/lee/permit-lists/${windowKey}`;
    for (const page of result.pages) {
      await putTextObject({
        bucket: output.bucket,
        key: `${listPrefix}/raw/page-${String(page.pageNumber).padStart(3, "0")}.html`,
        body: page.html,
        contentType: "text/html; charset=utf-8",
      });
    }

    const summary = {
      schemaVersion: "permit-harvest.lee-accela.window.v1",
      jobId: message.jobId,
      windowKey,
      startDate: message.startDate,
      endDate: message.endDate,
      retrievedAt: new Date().toISOString(),
      reportedTotal: result.reportedTotal,
      discoveredPermitCount: result.permits.length,
      noResults: result.noResults,
      truncatedForSplit: result.truncatedForSplit,
      maxPages,
      permits: result.permits,
    };
    const summaryUri = await putTextObject({
      bucket: output.bucket,
      key: `${listPrefix}/links.json`,
      body: JSON.stringify(summary, null, 2),
      contentType: "application/json; charset=utf-8",
    });

    consoleLogger.info("lee_window_written", {
      windowKey,
      summaryUri,
      discoveredPermitCount: result.permits.length,
      reportedTotal: result.reportedTotal,
    });

    const detailBatchMessages = buildLeePermitDetailBatchMessages({
      sourceMessage: message,
      windowKey,
      permits: result.permits,
      batchSize,
    });
    for (const detailBatchMessage of detailBatchMessages) {
      await writeLeePermitDetails({
        output,
        browser,
        message: detailBatchMessage,
        continueOnPermitError: true,
      });
    }
    consoleLogger.info("lee_detail_batches_processed_inline", {
      windowKey,
      batchCount: detailBatchMessages.length,
      batchSize,
      sourceWindowWillSplit:
        result.reportedTotal !== null &&
        result.reportedTotal >= splitThreshold &&
        daySpan > 1,
    });

    if (
      result.reportedTotal !== null &&
      result.reportedTotal >= splitThreshold &&
      daySpan > 1
    ) {
      const split = splitDateRange(message.startDate, message.endDate);
      for (const part of split) {
        await enqueueMessage({
          ...message,
          startDate: part.startDate,
          endDate: part.endDate,
        });
      }
      consoleLogger.warn("lee_window_split", {
        windowKey,
        reportedTotal: result.reportedTotal,
        split,
      });
      return;
    }
  } finally {
    await browser.close().catch(() => undefined);
  }
}

/**
 * Capture permit details for a Lee County detail batch and write raw/extracted S3 artifacts.
 *
 * @param {object} params - Detail capture and write parameters.
 * @param {ReturnType<typeof resolveOutputPrefix>} params.output - Resolved S3 output prefix.
 * @param {import("puppeteer").Browser} params.browser - Browser used to capture detail pages.
 * @param {LeePermitDetailBatchMessage} params.message - Detail batch message.
 * @param {boolean} params.continueOnPermitError - Continue the batch after a single permit capture error.
 * @returns {Promise<void>} Resolves after detail artifacts and the batch summary are written.
 */
async function writeLeePermitDetails({
  output,
  browser,
  message,
  continueOnPermitError,
}) {
  const batchPrefix = `${output.key}/lee/permit-details/${message.windowKey}/batch-${String(message.batchIndex).padStart(5, "0")}`;
  /** @type {Array<{ recordNumber: string, extractedJsonS3Uri: string | null, rawHtmlS3Uri: string | null, skipped: boolean, error: string | null }>} */
  const results = [];

  for (const permit of message.permits) {
    try {
      const stem = buildPermitOutputStem(permit);
      const jsonKey = `${output.key}/lee/extracted/permits/${stem}.json`;
      const htmlKey = `${output.key}/lee/raw/permit-details/${stem}.html`;
      const skipExisting = message.skipExisting !== false;
      if (skipExisting && (await objectExists(output.bucket, jsonKey))) {
        results.push({
          recordNumber: permit.recordNumber,
          extractedJsonS3Uri: `s3://${output.bucket}/${jsonKey}`,
          rawHtmlS3Uri: `s3://${output.bucket}/${htmlKey}`,
          skipped: true,
          error: null,
        });
        consoleLogger.info("lee_detail_skipped_existing", {
          recordNumber: permit.recordNumber,
          jsonKey,
        });
        continue;
      }

      const capture = await captureLeePermitDetail({
        browser,
        permit,
        logger: consoleLogger,
      });
      const htmlUri = await putTextObject({
        bucket: output.bucket,
        key: htmlKey,
        body: capture.html,
        contentType: "text/html; charset=utf-8",
      });
      const jsonUri = await putTextObject({
        bucket: output.bucket,
        key: jsonKey,
        body: JSON.stringify(
          {
            ...capture.extraction,
            sourceSearchResult: permit,
            rawHtmlS3Uri: htmlUri,
            idempotencyKey: `lee-permit:${shortHash(permit.url)}`,
            lexiconStatus: {
              mappedToCurrentLexicon: false,
              notes:
                "Permit, inspection, processing-status, and public document fields are preserved for later lexicon expansion.",
            },
          },
          null,
          2,
        ),
        contentType: "application/json; charset=utf-8",
      });
      results.push({
        recordNumber: permit.recordNumber,
        extractedJsonS3Uri: jsonUri,
        rawHtmlS3Uri: htmlUri,
        skipped: false,
        error: null,
      });
    } catch (error) {
      if (!continueOnPermitError) throw error;
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      results.push({
        recordNumber: permit.recordNumber,
        extractedJsonS3Uri: null,
        rawHtmlS3Uri: null,
        skipped: false,
        error: errorMessage,
      });
      consoleLogger.error("lee_detail_inline_capture_failed", {
        recordNumber: permit.recordNumber,
        windowKey: message.windowKey,
        batchIndex: message.batchIndex,
        error: errorMessage,
      });
    }
  }

  await putTextObject({
    bucket: output.bucket,
    key: `${batchPrefix}/summary.json`,
    body: JSON.stringify(
      {
        schemaVersion: "permit-harvest.lee-accela.detail-batch.v1",
        jobId: message.jobId,
        windowKey: message.windowKey,
        batchIndex: message.batchIndex,
        processedAt: new Date().toISOString(),
        requestedCount: message.permits.length,
        processedCount: results.filter((item) => !item.skipped && !item.error)
          .length,
        skippedCount: results.filter((item) => item.skipped).length,
        errorCount: results.filter((item) => item.error).length,
        results,
      },
      null,
      2,
    ),
    contentType: "application/json; charset=utf-8",
  });
}

/**
 * Process a Lee County permit-detail batch message.
 *
 * @param {LeePermitDetailBatchMessage} message - Detail batch message.
 * @returns {Promise<void>}
 */
async function processLeePermitDetailBatch(message) {
  const output = resolveOutputPrefix(message.outputPrefix, message.jobId);
  const browser = await createBrowser(consoleLogger);

  try {
    await writeLeePermitDetails({
      output,
      browser,
      message,
      continueOnPermitError: false,
    });
  } finally {
    await browser.close().catch(() => undefined);
  }
}

/**
 * Build the normalized property-first target carried into permit artifacts and DB linking.
 *
 * @param {LeePropertyFirstPermitParcelMessage} message - Property-first SQS message.
 * @returns {PropertyFirstTarget} Normalized target metadata.
 */
function buildPropertyFirstTarget(message) {
  const normalizedParcelIdentifier = normalizeParcelSearchValue(
    message.parcelIdentifier,
  );
  if (normalizedParcelIdentifier === null) {
    throw new Error(
      `Invalid Lee parcel identifier: ${message.parcelIdentifier}`,
    );
  }
  return {
    parcelIdentifier: message.parcelIdentifier,
    normalizedParcelIdentifier,
    requestIdentifier: readOptionalString(message.requestIdentifier),
    appraisalOutputS3Uri: readOptionalString(message.appraisalOutputS3Uri),
    appraisalPreparedOutputS3Uri: readOptionalString(
      message.appraisalPreparedOutputS3Uri,
    ),
    propertyId: readOptionalString(message.propertyId),
    propertyUsageType: readOptionalString(message.propertyUsageType),
    bestPermitAddress: readOptionalString(message.bestPermitAddress),
    addressBase: readOptionalString(message.addressBase),
  };
}

/**
 * Return the completion-state S3 key used to make property-first enqueue-all runs idempotent.
 *
 * @param {ReturnType<typeof resolveOutputPrefix>} output - Resolved output prefix.
 * @param {string} searchKey - Stable Accela parcel search key.
 * @returns {string} S3 object key for the completion state.
 */
function buildPropertyFirstStateKey(output, searchKey) {
  return `${output.key}/lee/property-first-state/${searchKey}/completed.json`;
}

/**
 * Merge appraiser parcel provenance into a captured permit detail extraction.
 *
 * Accela detail pages occasionally omit `More Details > Parcel Number`, and
 * explicit historical error-page fallbacks have only search-result evidence.
 * The property-first search target is therefore added as source evidence so the
 * scoped Neon linker can attach the permit rows to the already-loaded appraiser
 * property without weakening generic timeout behavior.
 *
 * @param {Record<string, unknown> & { parcelIdentifier?: string | null, moreDetails?: unknown }} extraction - Captured permit extraction.
 * @param {PropertyFirstTarget} target - Appraiser property target for this parcel search.
 * @returns {Record<string, unknown>} Enriched extraction.
 */
function withPropertyFirstParcel(extraction, target) {
  const existingParcel =
    typeof extraction.parcelIdentifier === "string" &&
    extraction.parcelIdentifier.trim().length > 0
      ? extraction.parcelIdentifier
      : null;
  const moreDetails = isRecord(extraction.moreDetails)
    ? extraction.moreDetails
    : {};
  return {
    ...extraction,
    parcelIdentifier: existingParcel ?? target.normalizedParcelIdentifier,
    moreDetails: {
      ...moreDetails,
      ...(typeof moreDetails["Parcel Number"] === "string" &&
      moreDetails["Parcel Number"].trim().length > 0
        ? {}
        : { "Parcel Number": target.normalizedParcelIdentifier }),
    },
  };
}

/**
 * Wait for a fixed number of milliseconds.
 *
 * @param {number} milliseconds - Delay duration.
 * @returns {Promise<void>} Resolves after the delay.
 */
function delay(milliseconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

/**
 * Retry an asynchronous operation with a fixed delay between attempts.
 *
 * @template T
 * @param {object} params - Retry parameters.
 * @param {number} params.maxAttempts - Positive number of attempts before surfacing the final error.
 * @param {number} params.delayMs - Milliseconds to wait between failed attempts.
 * @param {(attempt: number) => Promise<T>} params.operation - Operation to run; receives the one-based attempt number.
 * @param {((details: { attempt: number, maxAttempts: number, errorMessage: string }) => void) | undefined} [params.onRetry] - Optional callback before a retry delay.
 * @returns {Promise<T>} The operation result when any attempt succeeds.
 */
async function retryAsyncOperation({
  maxAttempts,
  delayMs,
  operation,
  onRetry,
}) {
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      return await operation(attempt);
    } catch (caught) {
      const errorMessage =
        caught instanceof Error ? caught.message : String(caught);
      if (attempt >= maxAttempts) throw caught;
      onRetry?.({ attempt, maxAttempts, errorMessage });
      await delay(delayMs);
    }
  }
  throw new Error("retryAsyncOperation exhausted attempts without an error");
}

/**
 * Capture property-first permit details with strict completeness semantics.
 *
 * The caller fails the whole parcel if any result has a terminal error. Explicit
 * Accela unavailable-detail pages are already converted by `captureLeePermitDetail`
 * into full provenance-preserving artifacts; generic browser timeouts remain
 * errors and therefore prevent partial Neon loads.
 *
 * @param {object} params - Detail write parameters.
 * @param {ReturnType<typeof resolveOutputPrefix>} params.output - Resolved output prefix.
 * @param {import("puppeteer").Browser} params.browser - Browser used to capture detail pages.
 * @param {PropertyFirstTarget} params.target - Appraiser property target metadata.
 * @param {PermitLink[]} params.permits - Permit links discovered by the parcel search.
 * @param {boolean} params.skipExisting - Whether existing extracted JSON artifacts can be reused.
 * @returns {Promise<DetailWriteResult[]>} Per-permit detail results.
 */
async function writeLeePropertyFirstPermitDetails({
  output,
  browser,
  target,
  permits,
  skipExisting,
}) {
  /** @type {DetailWriteResult[]} */
  const results = [];

  for (const permit of permits) {
    const stem = buildPermitOutputStem(permit);
    const jsonKey = `${output.key}/lee/extracted/permits/${stem}.json`;
    const htmlKey = `${output.key}/lee/raw/permit-details/${stem}.html`;

    for (
      let attempt = 1;
      attempt <= PROPERTY_FIRST_DETAIL_CAPTURE_ATTEMPTS;
      attempt += 1
    ) {
      try {
        if (skipExisting && (await objectExists(output.bucket, jsonKey))) {
          results.push({
            recordNumber: permit.recordNumber,
            extractedJsonS3Uri: `s3://${output.bucket}/${jsonKey}`,
            rawHtmlS3Uri: `s3://${output.bucket}/${htmlKey}`,
            skipped: true,
            error: null,
          });
          consoleLogger.info("lee_property_first_detail_skipped_existing", {
            recordNumber: permit.recordNumber,
            parcelIdentifier: target.parcelIdentifier,
            jsonKey,
          });
          break;
        }

        const capture = await captureLeePermitDetail({
          browser,
          permit,
          logger: consoleLogger,
        });
        const extraction = withPropertyFirstParcel(capture.extraction, target);
        const htmlUri = await putTextObject({
          bucket: output.bucket,
          key: htmlKey,
          body: capture.html,
          contentType: "text/html; charset=utf-8",
        });
        const jsonUri = await putTextObject({
          bucket: output.bucket,
          key: jsonKey,
          body: JSON.stringify(
            {
              ...extraction,
              sourceSearchResult: permit,
              rawHtmlS3Uri: htmlUri,
              idempotencyKey: `lee-permit:${shortHash(permit.url)}`,
              propertyFirstTarget: target,
              lexiconStatus: {
                mappedToCurrentLexicon: false,
                notes:
                  "Permit, inspection, processing-status, and public document fields are preserved for later lexicon expansion.",
              },
            },
            null,
            2,
          ),
          contentType: "application/json; charset=utf-8",
        });
        results.push({
          recordNumber: permit.recordNumber,
          extractedJsonS3Uri: jsonUri,
          rawHtmlS3Uri: htmlUri,
          skipped: false,
          error: null,
        });
        break;
      } catch (caught) {
        const message =
          caught instanceof Error ? caught.message : String(caught);
        if (attempt < PROPERTY_FIRST_DETAIL_CAPTURE_ATTEMPTS) {
          consoleLogger.warn("lee_property_first_detail_capture_retrying", {
            recordNumber: permit.recordNumber,
            parcelIdentifier: target.parcelIdentifier,
            attempt,
            maxAttempts: PROPERTY_FIRST_DETAIL_CAPTURE_ATTEMPTS,
            error: message,
          });
          await delay(PROPERTY_FIRST_DETAIL_RETRY_DELAY_MS);
          continue;
        }
        results.push({
          recordNumber: permit.recordNumber,
          extractedJsonS3Uri: null,
          rawHtmlS3Uri: null,
          skipped: false,
          error: message,
        });
        consoleLogger.error("lee_property_first_detail_capture_failed", {
          recordNumber: permit.recordNumber,
          parcelIdentifier: target.parcelIdentifier,
          error: message,
        });
        break;
      }
    }
  }

  return results;
}

/**
 * Extract a Neon connection string from a Secrets Manager payload.
 *
 * The production secret can be either the raw Postgres connection string or a
 * JSON object with a common connection-string field. This keeps deployment
 * compatible with both manually-created and managed Neon secrets.
 *
 * @param {string} secretString - SecretString returned by Secrets Manager.
 * @returns {string} Database connection string.
 */
function readDatabaseUrlFromSecretString(secretString) {
  const trimmed = secretString.trim();
  if (
    trimmed.startsWith("postgres://") ||
    trimmed.startsWith("postgresql://")
  ) {
    return trimmed;
  }
  const parsed = /** @type {unknown} */ (JSON.parse(trimmed));
  if (!isRecord(parsed)) {
    throw new Error(
      "Query DB secret must be a connection string or JSON object",
    );
  }
  for (const key of [
    "DATABASE_URL",
    "POSTGRES_URL",
    "databaseUrl",
    "connectionString",
    "url",
  ]) {
    const value = readOptionalString(parsed[key]);
    if (value !== null) return value;
  }
  throw new Error("Query DB secret JSON did not contain a database URL field");
}

/**
 * Resolve the Neon query database connection string from env or Secrets Manager.
 *
 * @returns {Promise<string>} Postgres connection string.
 */
async function getQueryDatabaseUrl() {
  if (cachedQueryDatabaseUrl !== null) return cachedQueryDatabaseUrl;
  const directUrl = readOptionalString(QUERY_DB_DATABASE_URL);
  if (directUrl !== null) {
    cachedQueryDatabaseUrl = directUrl;
    return directUrl;
  }
  const secretArn = readOptionalString(QUERY_DB_DATABASE_URL_SECRET_ARN);
  if (secretArn === null) {
    throw new Error(
      "QUERY_DB_DATABASE_URL_SECRET_ARN or QUERY_DB_DATABASE_URL is required for property-first Neon loads",
    );
  }
  const response = await secretsManager.send(
    new GetSecretValueCommand({ SecretId: secretArn }),
  );
  if (typeof response.SecretString !== "string") {
    throw new Error(`Query DB secret ${secretArn} did not return SecretString`);
  }
  cachedQueryDatabaseUrl = readDatabaseUrlFromSecretString(
    response.SecretString,
  );
  return cachedQueryDatabaseUrl;
}

/**
 * Return a cached Postgres pool for the Neon query database.
 *
 * @returns {Promise<Pool>} Query database pool.
 */
async function getQueryDatabasePool() {
  if (queryDatabasePool !== null) return queryDatabasePool;
  queryDatabasePool = new Pool({
    connectionString: await getQueryDatabaseUrl(),
    application_name: "oracle-node-property-first-permit-worker",
    connectionTimeoutMillis: 15_000,
    max: 1,
    query_timeout: 180_000,
    statement_timeout: 180_000,
  });
  return queryDatabasePool;
}

/**
 * Link loaded Lee Accela permit rows for one parcel to its Lee Appraiser property row.
 *
 * @param {import("pg").PoolClient} client - Transaction-bound Postgres client.
 * @param {PropertyFirstTarget} target - Property-first target metadata.
 * @param {boolean} [onDemand] - On-demand MCP harvest. When true and no matching appraiser property exists, return zero link counters instead of throwing so the permit upsert still commits. Defaults false.
 * @returns {Promise<{ matchedPermitRows: number, linkedPermitRows: number, propertyMissing?: boolean }>} Link counters. `propertyMissing` is true only on the on-demand deferred-link path where the appraiser property was not yet loaded.
 */
async function linkPropertyFirstPermits(client, target, onDemand = false) {
  const targetAlphanumericParcelIdentifier = target.normalizedParcelIdentifier;
  const targetNumericParcelIdentifier = normalizeParcelDigits(
    target.normalizedParcelIdentifier,
  );
  const propertyResult = await client.query(
    `
      select property_id, parcel_id
      from properties
      where source_system = 'lee_appraiser'
        and (
          ($1::text is not null and source_artifact_uri = $1::text)
          or ($2::text is not null and request_identifier = $2::text)
          or upper(regexp_replace(coalesce(parcel_identifier, ''), '[^[:alnum:]]', '', 'g')) = $3::text
          or ($4::text is not null and regexp_replace(coalesce(parcel_identifier, ''), '[^0-9]', '', 'g') = $4::text)
        )
      order by
        case when $1::text is not null and source_artifact_uri = $1::text then 0 else 1 end,
        case when $2::text is not null and request_identifier = $2::text then 0 else 1 end,
        updated_at desc nulls last
      limit 1
    `,
    [
      target.appraisalOutputS3Uri,
      target.requestIdentifier,
      targetAlphanumericParcelIdentifier,
      targetNumericParcelIdentifier,
    ],
  );
  const propertyRow = propertyResult.rows[0];
  if (!isRecord(propertyRow)) {
    if (onDemand) {
      // On-demand harvests may run before the appraiser property is loaded.
      // Return zero link counters so the caller's transaction still commits the
      // permit upsert instead of rolling it back on this throw. Flag
      // propertyMissing so the caller can defer writing the completed.json state
      // and let a future run link these permits once the property exists.
      return {
        matchedPermitRows: 0,
        linkedPermitRows: 0,
        propertyMissing: true,
      };
    }
    throw new Error(
      `No loaded Lee Appraiser property found for parcel ${targetAlphanumericParcelIdentifier}`,
    );
  }

  const matchedResult = await client.query(
    `
      select count(*)::int as matched_permit_rows
      from property_improvements
      where source_system = 'lee_accela'
        and (
          upper(regexp_replace(coalesce(parcel_identifier, ''), '[^[:alnum:]]', '', 'g')) = $1::text
          or upper(regexp_replace(coalesce(source_payload #>> '{propertyFirstTarget,normalizedParcelIdentifier}', ''), '[^[:alnum:]]', '', 'g')) = $1::text
          or upper(regexp_replace(coalesce(source_payload #>> '{propertyFirstTarget,parcelIdentifier}', ''), '[^[:alnum:]]', '', 'g')) = $1::text
          or ($2::text is not null and regexp_replace(coalesce(parcel_identifier, ''), '[^0-9]', '', 'g') = $2::text)
          or ($2::text is not null and regexp_replace(coalesce(source_payload #>> '{propertyFirstTarget,normalizedParcelIdentifier}', ''), '[^0-9]', '', 'g') = $2::text)
          or ($2::text is not null and regexp_replace(coalesce(source_payload #>> '{propertyFirstTarget,parcelIdentifier}', ''), '[^0-9]', '', 'g') = $2::text)
        )
    `,
    [targetAlphanumericParcelIdentifier, targetNumericParcelIdentifier],
  );
  const updateResult = await client.query(
    `
      update property_improvements permit
         set property_id = $3,
             parcel_id = $4,
             property_match_method = 'property_first_appraisal_parcel',
             property_match_confidence = 'high',
             updated_at = now()
      where permit.source_system = 'lee_accela'
        and (
          upper(regexp_replace(coalesce(permit.parcel_identifier, ''), '[^[:alnum:]]', '', 'g')) = $1::text
          or upper(regexp_replace(coalesce(permit.source_payload #>> '{propertyFirstTarget,normalizedParcelIdentifier}', ''), '[^[:alnum:]]', '', 'g')) = $1::text
          or upper(regexp_replace(coalesce(permit.source_payload #>> '{propertyFirstTarget,parcelIdentifier}', ''), '[^[:alnum:]]', '', 'g')) = $1::text
          or ($2::text is not null and regexp_replace(coalesce(permit.parcel_identifier, ''), '[^0-9]', '', 'g') = $2::text)
          or ($2::text is not null and regexp_replace(coalesce(permit.source_payload #>> '{propertyFirstTarget,normalizedParcelIdentifier}', ''), '[^0-9]', '', 'g') = $2::text)
          or ($2::text is not null and regexp_replace(coalesce(permit.source_payload #>> '{propertyFirstTarget,parcelIdentifier}', ''), '[^0-9]', '', 'g') = $2::text)
        )
        and (
          permit.property_id is distinct from $3
          or permit.parcel_id is distinct from $4
          or permit.property_match_method is distinct from 'property_first_appraisal_parcel'
          or permit.property_match_confidence is distinct from 'high'
        )
      returning permit.property_improvement_id
    `,
    [
      targetAlphanumericParcelIdentifier,
      targetNumericParcelIdentifier,
      propertyRow.property_id,
      propertyRow.parcel_id,
    ],
  );
  const matchedRow = matchedResult.rows[0];
  const matchedPermitRows =
    isRecord(matchedRow) && typeof matchedRow.matched_permit_rows === "number"
      ? matchedRow.matched_permit_rows
      : 0;
  return {
    matchedPermitRows,
    linkedPermitRows: updateResult.rowCount ?? 0,
  };
}

/**
 * Decide whether a property-first parcel should write its completed.json state.
 *
 * The completed state is what `skipCompleted` uses to skip a parcel on later
 * runs. On the on-demand path, a harvest can find permits before the appraiser
 * property is loaded, in which case linking is deferred (propertyMissing) even
 * though the permit rows committed. In that single case we must NOT mark the
 * parcel completed, so a future run re-processes and links the permits once the
 * property exists. Every other case (bulk, or on-demand with the property
 * present) writes the completed state exactly as before.
 *
 * @param {{ onDemand?: boolean, propertyMissing?: boolean }} params - Decision inputs.
 * @returns {boolean} True when completed.json should be written.
 */
function shouldWritePropertyFirstCompletedState({ onDemand, propertyMissing }) {
  return !(onDemand === true && propertyMissing === true);
}

/**
 * Read captured permit artifacts from S3, merge them into Neon, and link the scoped parcel.
 *
 * @param {object} params - Load parameters.
 * @param {DetailWriteResult[]} params.detailResults - Detail capture results for one property.
 * @param {PropertyFirstTarget} params.target - Property-first target metadata.
 * @param {ReturnType<typeof resolveOutputPrefix>} params.output - Resolved output prefix used for copied media artifacts.
 * @param {boolean} params.loadAppraisalToNeon - Whether to merge the appraisal artifact before permit rows.
 * @param {boolean} [params.onDemand] - On-demand MCP harvest. When true, a missing appraiser property links zero permits instead of throwing, so the permit upsert still commits. Defaults false.
 * @returns {Promise<QueryDbLoadSummary>} Neon load and link summary.
 */
async function loadPropertyFirstPermitsToQueryDb({
  detailResults,
  target,
  output,
  loadAppraisalToNeon,
  onDemand = false,
}) {
  const artifactUris = detailResults
    .map((result) => result.extractedJsonS3Uri)
    .filter((uri) => uri !== null);
  const permitRecords = await Promise.all(
    artifactUris.map(async (uri) => ({
      uri,
      record: await readJsonFromS3(uri),
    })),
  );
  const permitBundles = permitRecords.map((item) =>
    mapLeePermitDetail({ record: item.record, artifactUri: item.uri }),
  );
  const permitRows = permitBundles.flatMap((bundle) => bundle.rows);
  const permitSkippedRecordCount = permitBundles.reduce(
    (count, bundle) => count + bundle.skippedRecords.length,
    0,
  );
  const appraisalLoad = await readAppraisalRowsFromS3(
    loadAppraisalToNeon ? target.appraisalOutputS3Uri : null,
  );
  const appraisalMediaLoad = loadAppraisalToNeon
    ? await readAppraisalMediaRowsFromS3({
        preparedOutputS3Uri: target.appraisalPreparedOutputS3Uri,
        target,
        output,
      })
    : { rows: [], discoveredCount: 0, storedCount: 0, skippedRecordCount: 0 };
  const rows = sortPreparedRows(
    [...appraisalLoad.rows, ...appraisalMediaLoad.rows, ...permitRows],
    QUERY_DB_TABLE_ORDER,
  );

  const pool = await getQueryDatabasePool();
  const client = await pool.connect();
  try {
    await client.query("begin", []);
    const counters = await upsertPreparedRows(client, rows, {
      missingReferenceBehavior: "omit",
    });
    const linkCounters = await linkPropertyFirstPermits(
      client,
      target,
      onDemand,
    );
    await client.query("commit", []);
    return {
      appraisalArtifactCount: appraisalLoad.artifactCount,
      appraisalMediaDiscoveredCount: appraisalMediaLoad.discoveredCount,
      appraisalMediaStoredCount: appraisalMediaLoad.storedCount,
      appraisalPreparedRowCount:
        appraisalLoad.rows.length + appraisalMediaLoad.rows.length,
      appraisalSkippedRecordCount:
        appraisalLoad.skippedRecordCount +
        appraisalMediaLoad.skippedRecordCount,
      permitArtifactCount: permitRecords.length,
      permitPreparedRowCount: permitRows.length,
      permitSkippedRecordCount,
      artifactCount: appraisalLoad.artifactCount + permitRecords.length,
      preparedRowCount: rows.length,
      skippedRecordCount:
        appraisalLoad.skippedRecordCount +
        appraisalMediaLoad.skippedRecordCount +
        permitSkippedRecordCount,
      attemptedRows: counters.attemptedRows,
      changedRows: counters.changedRows,
      unchangedRows: counters.unchangedRows,
      matchedPermitRows: linkCounters.matchedPermitRows,
      linkedPermitRows: linkCounters.linkedPermitRows,
      propertyMissing: linkCounters.propertyMissing === true,
    };
  } catch (error) {
    await client.query("rollback", []).catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Process one property-first parcel message: harvest all permit details, then immediately load/link in Neon.
 *
 * @param {LeePropertyFirstPermitParcelMessage} message - Property-first parcel message.
 * @returns {Promise<void>} Resolves only after the parcel is complete and, by default, loaded into Neon.
 */
async function processLeePropertyFirstPermitParcel(message) {
  const output = resolveOutputPrefix(message.outputPrefix, message.jobId);
  const target = buildPropertyFirstTarget(message);
  const searchKey = `parcel-${target.normalizedParcelIdentifier.toLowerCase()}`;
  const stateKey = buildPropertyFirstStateKey(output, searchKey);
  const skipCompleted = message.skipCompleted !== false;
  if (skipCompleted && (await objectExists(output.bucket, stateKey))) {
    consoleLogger.info("lee_property_first_parcel_skipped_completed", {
      jobId: message.jobId,
      parcelIdentifier: target.parcelIdentifier,
      normalizedParcelIdentifier: target.normalizedParcelIdentifier,
      stateUri: `s3://${output.bucket}/${stateKey}`,
    });
    return;
  }

  const permitEligibility = await resolvePropertyFirstPermitEligibility(target);
  if (
    target.propertyUsageType === null &&
    permitEligibility.propertyUsageType !== null
  ) {
    target.propertyUsageType = permitEligibility.propertyUsageType;
  }
  if (
    shouldSkipIneligiblePropertyFirstParcel({
      eligibility: permitEligibility,
      onDemand: message.onDemand === true,
    })
  ) {
    const stateUri = await putTextObject({
      bucket: output.bucket,
      key: stateKey,
      body: JSON.stringify(
        {
          schemaVersion: "permit-harvest.lee-property-first-state.v1",
          event: "property_first_permit_skipped_non_priority_appraisal",
          completedAt: new Date().toISOString(),
          jobId: message.jobId,
          target,
          permitEligibility,
          discoveredPermitCount: 0,
          reportedTotal: null,
          noResults: null,
          detailResults: [],
          loadSummary: null,
          neonLoadSkippedReason: permitEligibility.reason,
        },
        null,
        2,
      ),
      contentType: "application/json; charset=utf-8",
    });
    consoleLogger.info("lee_property_first_parcel_skipped_ineligible_usage", {
      jobId: message.jobId,
      parcelIdentifier: target.parcelIdentifier,
      normalizedParcelIdentifier: target.normalizedParcelIdentifier,
      propertyUsageType: permitEligibility.propertyUsageType,
      reason: permitEligibility.reason,
      stateUri,
    });
    return;
  }

  const browser = await createBrowser(consoleLogger);
  try {
    const searchResult = await retryAsyncOperation({
      maxAttempts: PROPERTY_FIRST_SEARCH_ATTEMPTS,
      delayMs: PROPERTY_FIRST_SEARCH_RETRY_DELAY_MS,
      operation: async () =>
        searchLeePermitParcel({
          browser,
          parcelIdentifier: target.normalizedParcelIdentifier,
          portalUrl: message.portalUrl,
          maxPages: message.maxPages ?? 200,
          logger: consoleLogger,
        }),
      onRetry: ({ attempt, maxAttempts, errorMessage }) => {
        consoleLogger.warn("lee_property_first_parcel_search_retrying", {
          jobId: message.jobId,
          parcelIdentifier: target.parcelIdentifier,
          normalizedParcelIdentifier: target.normalizedParcelIdentifier,
          attempt,
          maxAttempts,
          error: errorMessage,
        });
      },
    });
    if (
      searchResult.reportedTotal !== null &&
      searchResult.permits.length < searchResult.reportedTotal
    ) {
      throw new Error(
        `Parcel ${target.parcelIdentifier} captured ${String(searchResult.permits.length)} of ${String(searchResult.reportedTotal)} reported permits; increase maxPages before loading`,
      );
    }

    const listPrefix = `${output.key}/lee/permit-parcel-searches/${searchResult.searchKey}`;
    for (const page of searchResult.pages) {
      await putTextObject({
        bucket: output.bucket,
        key: `${listPrefix}/raw/page-${String(page.pageNumber).padStart(3, "0")}.html`,
        body: page.html,
        contentType: "text/html; charset=utf-8",
      });
    }
    const summaryUri = await putTextObject({
      bucket: output.bucket,
      key: `${listPrefix}/links.json`,
      body: JSON.stringify(
        {
          schemaVersion: "permit-harvest.lee-accela.parcel.v1",
          jobId: message.jobId,
          searchKey: searchResult.searchKey,
          parcelIdentifier: target.parcelIdentifier,
          normalizedParcelIdentifier: target.normalizedParcelIdentifier,
          requestIdentifier: target.requestIdentifier,
          appraisalOutputS3Uri: target.appraisalOutputS3Uri,
          propertyId: target.propertyId,
          propertyUsageType: target.propertyUsageType,
          retrievedAt: new Date().toISOString(),
          reportedTotal: searchResult.reportedTotal,
          discoveredPermitCount: searchResult.permits.length,
          noResults: searchResult.noResults,
          maxPages: message.maxPages ?? 200,
          permits: searchResult.permits,
        },
        null,
        2,
      ),
      contentType: "application/json; charset=utf-8",
    });

    const detailResults = await writeLeePropertyFirstPermitDetails({
      output,
      browser,
      target,
      permits: searchResult.permits,
      skipExisting: message.skipExisting !== false,
    });
    const detailErrors = detailResults.filter(
      (result) => result.error !== null,
    );
    await putTextObject({
      bucket: output.bucket,
      key: `${output.key}/lee/permit-parcel-details/${searchResult.searchKey}/summary.json`,
      body: JSON.stringify(
        {
          schemaVersion:
            "permit-harvest.lee-accela.property-first-detail-summary.v1",
          jobId: message.jobId,
          searchKey: searchResult.searchKey,
          parcelIdentifier: target.parcelIdentifier,
          normalizedParcelIdentifier: target.normalizedParcelIdentifier,
          processedAt: new Date().toISOString(),
          requestedCount: searchResult.permits.length,
          processedCount: detailResults.filter(
            (result) => !result.skipped && result.error === null,
          ).length,
          skippedCount: detailResults.filter((result) => result.skipped).length,
          errorCount: detailErrors.length,
          results: detailResults,
        },
        null,
        2,
      ),
      contentType: "application/json; charset=utf-8",
    });
    if (detailErrors.length > 0) {
      const failedRecordNumbers = detailErrors
        .map((result) => result.recordNumber)
        .join(", ");
      throw new Error(
        `Parcel ${target.parcelIdentifier} is missing ${String(detailErrors.length)} permit detail artifact(s): ${failedRecordNumbers}`,
      );
    }

    const loadSummary =
      message.loadToNeon === false || searchResult.permits.length === 0
        ? null
        : await loadPropertyFirstPermitsToQueryDb({
            detailResults,
            target,
            output,
            loadAppraisalToNeon: message.loadAppraisalToNeon !== false,
            onDemand: message.onDemand === true,
          });
    if (
      !shouldWritePropertyFirstCompletedState({
        onDemand: message.onDemand === true,
        propertyMissing: loadSummary?.propertyMissing === true,
      })
    ) {
      // On-demand harvest found permits before the appraiser property was
      // loaded, so linking was deferred (permit rows are already committed
      // inside loadPropertyFirstPermitsToQueryDb). Do NOT write the
      // completed.json state: leaving the parcel "not completed" lets a future
      // run re-process and link these permits once the property exists.
      consoleLogger.info(
        "lee_property_first_parcel_deferred_property_not_loaded",
        {
          jobId: message.jobId,
          parcelIdentifier: target.parcelIdentifier,
          normalizedParcelIdentifier: target.normalizedParcelIdentifier,
          discoveredPermitCount: searchResult.permits.length,
        },
      );
      return;
    }
    const stateUri = await putTextObject({
      bucket: output.bucket,
      key: stateKey,
      body: JSON.stringify(
        {
          schemaVersion: "permit-harvest.lee-property-first-state.v1",
          event:
            searchResult.permits.length === 0
              ? "property_first_no_permits"
              : "property_first_permits_loaded",
          completedAt: new Date().toISOString(),
          jobId: message.jobId,
          target,
          permitListSummaryS3Uri: summaryUri,
          discoveredPermitCount: searchResult.permits.length,
          reportedTotal: searchResult.reportedTotal,
          noResults: searchResult.noResults,
          detailResults,
          loadSummary,
          neonLoadSkippedReason:
            searchResult.permits.length === 0
              ? "no_permits_found_for_property"
              : null,
        },
        null,
        2,
      ),
      contentType: "application/json; charset=utf-8",
    });
    consoleLogger.info("lee_property_first_parcel_complete", {
      jobId: message.jobId,
      parcelIdentifier: target.parcelIdentifier,
      normalizedParcelIdentifier: target.normalizedParcelIdentifier,
      discoveredPermitCount: searchResult.permits.length,
      stateUri,
      loadSummary,
    });
  } catch (error) {
    // Resilience: a single parcel failure (Accela search timeout, an
    // incomplete-capture guard, or a missing-detail guard) must not dead-letter
    // the message and stall the whole property-first run. Record the failure to
    // a queryable failures ledger and return WITHOUT writing the completion
    // state object, so the parcel stays "not completed" and is re-driven on the
    // next run. No partial data is loaded to Neon (all guards throw before the
    // load step).
    const errorMessage = error instanceof Error ? error.message : String(error);
    const failureKey = `${output.key}/lee/permit-parcel-failures/${searchKey}.json`;
    await putTextObject({
      bucket: output.bucket,
      key: failureKey,
      body: JSON.stringify(
        {
          schemaVersion: "permit-harvest.lee-property-first-state.v1",
          event: "property_first_permit_failed",
          failedAt: new Date().toISOString(),
          jobId: message.jobId,
          target,
          error: errorMessage,
        },
        null,
        2,
      ),
      contentType: "application/json; charset=utf-8",
    }).catch((writeError) => {
      consoleLogger.error("lee_property_first_parcel_failure_record_failed", {
        jobId: message.jobId,
        parcelIdentifier: target.parcelIdentifier,
        error:
          writeError instanceof Error ? writeError.message : String(writeError),
      });
    });
    consoleLogger.error("lee_property_first_parcel_failed", {
      jobId: message.jobId,
      parcelIdentifier: target.parcelIdentifier,
      normalizedParcelIdentifier: target.normalizedParcelIdentifier,
      error: errorMessage,
    });
  } finally {
    await browser.close().catch(() => undefined);
  }
}

/**
 * Process a Sunbiz corporate bulk address-match message.
 *
 * @param {SunbizCorporateAddressMatchMessage} message - Sunbiz address-match message.
 * @returns {Promise<void>}
 */
async function processSunbizCorporateAddressMatch(message) {
  const output = resolveOutputPrefix(message.outputPrefix, message.jobId);
  const sourceFormat = resolveSunbizSourceFormat(message);
  const maxMatchesPerAddress = message.maxMatchesPerAddress ?? 25;
  const summary = await matchCorporateDataS3Object({
    s3,
    sourceDataS3Uri: message.sourceDataS3Uri,
    sourceFormat,
    addressInputs: message.addressInputs,
    maxMatchesPerAddress,
    logger: consoleLogger,
  });
  const key = `${output.key}/sunbiz/address-matches/${safeSunbizKeyPart(message.addressBatchKey)}.json`;
  const uri = await putTextObject({
    bucket: output.bucket,
    key,
    body: JSON.stringify(
      {
        ...summary,
        jobId: message.jobId,
        addressBatchKey: message.addressBatchKey,
        idempotencyKey: `sunbiz-corporate-address-match:${shortHash(
          `${message.sourceDataS3Uri}:${message.addressBatchKey}`,
        )}`,
      },
      null,
      2,
    ),
    contentType: "application/json; charset=utf-8",
  });
  consoleLogger.info("sunbiz_corporate_address_match_written", {
    jobId: message.jobId,
    addressBatchKey: message.addressBatchKey,
    sourceDataS3Uri: message.sourceDataS3Uri,
    sourceFormat,
    totalMatchCount: summary.totalMatchCount,
    uri,
  });
}

/**
 * Process a Sunbiz corporate ZIP-prefix extraction message.
 *
 * @param {SunbizCorporateZipExtractMessage} message - Sunbiz ZIP extraction message.
 * @returns {Promise<void>}
 */
async function processSunbizCorporateZipExtract(message) {
  const output = resolveOutputPrefix(message.outputPrefix, message.jobId);
  const sourceFormat = resolveSunbizSourceFormat(message);
  const extractKey = safeSunbizKeyPart(message.extractKey);
  const chunkRecordLimit = message.chunkRecordLimit ?? 1000;
  const baseKey = `${output.key}/sunbiz/corporate-by-zip/${extractKey}`;
  const summary = await extractCorporateDataS3ObjectByZip({
    s3,
    sourceDataS3Uri: message.sourceDataS3Uri,
    sourceFormat,
    zipPrefixes: message.zipPrefixes,
    chunkRecordLimit,
    maxRecords: message.maxRecords,
    logger: consoleLogger,
    onChunk: async (chunk) => {
      const chunkKey = `${baseKey}/chunks/part-${String(chunk.chunkIndex).padStart(5, "0")}.jsonl`;
      const uri = await putTextObject({
        bucket: output.bucket,
        key: chunkKey,
        body: `${chunk.records
          .map((record) => JSON.stringify(record))
          .join("\n")}\n`,
        contentType: "application/x-ndjson; charset=utf-8",
      });
      consoleLogger.info("sunbiz_corporate_zip_extract_chunk_written", {
        jobId: message.jobId,
        extractKey: message.extractKey,
        chunkIndex: chunk.chunkIndex,
        recordCount: chunk.recordCount,
        uri,
      });
      return {
        chunkIndex: chunk.chunkIndex,
        recordCount: chunk.recordCount,
        uri,
      };
    },
  });

  const summaryKey = `${baseKey}/summary.json`;
  const summaryUri = await putTextObject({
    bucket: output.bucket,
    key: summaryKey,
    body: JSON.stringify(
      {
        ...summary,
        jobId: message.jobId,
        extractKey: message.extractKey,
        idempotencyKey: `sunbiz-corporate-zip-extract:${shortHash(
          `${message.sourceDataS3Uri}:${message.extractKey}`,
        )}`,
      },
      null,
      2,
    ),
    contentType: "application/json; charset=utf-8",
  });
  consoleLogger.info("sunbiz_corporate_zip_extract_summary_written", {
    jobId: message.jobId,
    extractKey: message.extractKey,
    sourceDataS3Uri: message.sourceDataS3Uri,
    sourceFormat,
    matchedRecordCount: summary.matchedRecordCount,
    chunkCount: summary.chunks.length,
    uri: summaryUri,
  });
}

/**
 * Lambda handler for permit harvest SQS events.
 *
 * @param {import("aws-lambda").SQSEvent} event - SQS event.
 * @returns {Promise<void>} Resolves when all records are processed.
 */
export const handler = async (event) => {
  for (const record of event.Records) {
    const message = validateMessage(JSON.parse(record.body));
    consoleLogger.info("permit_harvest_message_start", {
      messageType: message.type,
      jobId: message.jobId,
      messageId: record.messageId,
    });

    if (message.type === "lee-permit-list-window") {
      await processLeePermitListWindow(message);
    } else if (message.type === "lee-permit-detail-batch") {
      await processLeePermitDetailBatch(message);
    } else if (message.type === "lee-property-first-permit-parcel") {
      await processLeePropertyFirstPermitParcel(message);
    } else if (isPropertyFirstSeedFeederMessage(message)) {
      await processLeePropertyFirstSeedFeeder(message);
    } else if (message.type === "sunbiz-corporate-address-match") {
      await processSunbizCorporateAddressMatch(message);
    } else if (message.type === "sunbiz-corporate-zip-extract") {
      await processSunbizCorporateZipExtract(message);
    }

    consoleLogger.info("permit_harvest_message_complete", {
      messageType: message.type,
      jobId: message.jobId,
      messageId: record.messageId,
    });
  }
};

export const _private = {
  parseS3Uri,
  resolveOutputPrefix,
  validateMessage,
  splitDateRange,
  inclusiveDaySpan,
  readJsonFromS3,
  resolveSunbizSourceFormat,
  buildOneRowSeedCsv,
  buildLeePermitDetailBatchMessages,
  buildPropertyFirstPermitEligibility,
  shouldSkipIneligiblePropertyFirstParcel,
  buildPropertyFirstStateKey,
  buildPropertyFirstTarget,
  createInitialLeePropertyFirstSeedFeederState,
  isExistingLeeAppraiserSeedRow,
  linkPropertyFirstPermits,
  shouldWritePropertyFirstCompletedState,
  normalizeParcelDigits,
  resolvePropertyFirstPermitEligibleUsageTypes,
  readDatabaseUrlFromSecretString,
  retryAsyncOperation,
  resolveBackpressureQueues,
  withPropertyFirstParcel,
};
