#!/usr/bin/env node
// @ts-check

import { createHash } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

import pg from "pg";

import {
  BROWARD_BCS_ADAPTER_KEY,
  BROWARD_PERMIT_JURISDICTIONS,
  BROWARD_PERMIT_REGISTRY_VERSION,
  resolveBrowardPermitJurisdiction,
  sourcesForBrowardPermitJurisdiction,
} from "./broward-permit-jurisdictions.mjs";
import {
  BROWARD_DETAIL_URL,
  BROWARD_PILOT_FOLIOS,
  browardDetailRequestBody,
  normalizeBrowardFolio,
} from "./broward-folio.mjs";
import { probeBrowardBcsPermits } from "./permit-source-adapters/broward-bcs-posse.mjs";
import {
  DONPHAN_PERMIT_QUERY_COLUMNS,
  mapBrowardPermitToDonphanRow,
  writeDonphanPermitParquet,
  writePrivateJsonl,
} from "./broward-permit-query-artifact.mjs";

const CHECKPOINT_SCHEMA_VERSION = 1;
const PILOT_REPORT_SCHEMA_VERSION = 1;
const MAX_PILOT_PARCELS = 50;
const MAX_ADAPTER_ATTEMPTS = 5;
const MIN_APPRAISAL_DELAY_MS = 250;
const MIN_PERMIT_DELAY_MS = 1_000;
const MAX_BCPA_RESPONSE_BYTES = 2_000_000;
const EXPECTED_NEON_PROJECT_ID = "raspy-frost-51580436";
const PRODUCTION_ENDPOINT_PREFIX = "ep-mute-leaf";
const PERMIT_STATUS_FUNCTION =
  "ingest_control.record_broward_permit_pilot_status(integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,integer,boolean,boolean,boolean,boolean,boolean,timestamp with time zone)";

const { Client } = pg;

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * @typedef {import("./broward-permit-jurisdictions.mjs").BrowardPermitSourceRoute} BrowardPermitSourceRoute
 */

/**
 * @typedef {import("./broward-permit-query-artifact.mjs").BrowardNormalizedPermit} BrowardNormalizedPermit
 */

/**
 * @typedef {import("./broward-permit-query-artifact.mjs").DonphanPermitQueryRow} DonphanPermitQueryRow
 */

/**
 * @typedef {"records" | "no_permits"} ImplementedSourceResultStatus
 */

/**
 * @typedef {object} ImplementedSourceResult
 * @property {ImplementedSourceResultStatus} status - Adapter's explicit source result.
 * @property {readonly BrowardNormalizedPermit[]} records - Strict normalized source records.
 * @property {JsonObject} observation - Request counts, URLs, and source result evidence.
 */

/**
 * @callback BrowardPermitAdapterRunner
 * @param {string} folio - Exact 12-character BCPA parcel identifier.
 * @returns {Promise<ImplementedSourceResult>} Explicit bounded source result.
 */

/**
 * @typedef {"records" | "no_permits" | "adapter_unavailable" | "captcha_required" | "login_required" | "no_anonymous_search" | "custodian_only" | "egress_unavailable" | "request_cap_reached" | "source_failed"} BrowardPermitSourceOutcomeStatus
 */

/**
 * @typedef {object} BrowardPermitSourceOutcome
 * @property {string} sourceKey - Registry source identity.
 * @property {string} sourceName - Official source display name.
 * @property {string} sourceUrl - Official source/custodian URL.
 * @property {string | null} adapterKey - Vendor adapter key.
 * @property {"current" | "historical" | "supplemental"} coverageKind - Current, historical, or explicitly supplemental custody.
 * @property {boolean} attempted - Whether an official permit endpoint was requested.
 * @property {BrowardPermitSourceOutcomeStatus} status - Explicit source terminal status.
 * @property {number} recordCount - Normalized source records returned for this parcel.
 * @property {string} reason - Source result or unavailable/failure explanation.
 * @property {JsonObject | null} observation - Adapter request/count provenance.
 */

/**
 * @typedef {object} BrowardPilotParcelState
 * @property {string} folio - Exact BCPA parcel identifier.
 * @property {number} appraisalAttemptCount - Bounded BCPA attempts performed by this checkpoint.
 * @property {string | null} appraisalError - BCPA failure, if any.
 * @property {string | null} situsCity - BCPA situs city retained for routing evidence.
 * @property {string | null} situsAddress - BCPA situs address retained for routing evidence.
 * @property {string | null} usageCode - BCPA usage code retained for pilot context.
 * @property {string | null} jurisdictionKey - Registry key derived from BCPA situs evidence.
 * @property {string | null} jurisdictionName - Registry display name.
 * @property {"situs_city" | "situs_address" | "unresolved"} jurisdictionMethod - Field that established jurisdiction.
 * @property {string | null} jurisdictionError - Explicit unresolved-jurisdiction reason.
 * @property {BrowardPermitSourceOutcome[]} sourceOutcomes - One terminal outcome per configured source route.
 * @property {BrowardNormalizedPermit[]} records - Normalized local-private source records.
 */

/**
 * @typedef {object} BrowardPermitPilotCheckpoint
 * @property {1} schemaVersion - Checkpoint schema version.
 * @property {string} sourceSignature - Exact folio/registry/limit signature.
 * @property {readonly string[]} folios - Ordered pilot folios.
 * @property {Record<string, BrowardPilotParcelState>} parcels - Resumable parcel states keyed by folio.
 */

/**
 * @typedef {object} BrowardPermitPilotCounters
 * @property {number} sampleParcels - Unique input parcels.
 * @property {number} appraisalAttempts - BCPA requests made across checkpoint state.
 * @property {number} appraisalResolved - Parcels with valid BCPA records.
 * @property {number} jurisdictionResolved - Parcels mapped to one of 32 registry rows.
 * @property {number} jurisdictionUnresolved - Parcels lacking sufficient BCPA situs evidence.
 * @property {number} sourceOutcomes - Current and historical source outcomes reconciled.
 * @property {number} sourceUnavailableOutcomes - Explicit adapter/login/CAPTCHA/custodian/egress/cap outcomes.
 * @property {number} permitSourceAttempts - Actual bounded permit source requests.
 * @property {number} permitAttemptedParcels - Distinct parcels sent to an implemented permit adapter.
 * @property {number} explicitNoPermitOutcomes - Successful official valid-parcel empty results.
 * @property {number} sourceFailures - Attempted source failures.
 * @property {number} rawPermitRecords - Source records before cross-parcel deduplication.
 * @property {number} duplicatePermitRecords - Exact duplicate source records removed.
 * @property {number} conflictingPermitRecords - Duplicate source identities with differing payloads.
 * @property {number} uniquePermitRecords - Reconciled normalized source records.
 * @property {number} queryRows - Donphan permit-table rows.
 */

/**
 * @typedef {object} BrowardPermitPilotReport
 * @property {1} schemaVersion - Report schema version.
 * @property {"local-checkpointed-property-first-permit-pilot"} mode - Explicit no-AWS mode.
 * @property {string} generatedAt - ISO completion timestamp.
 * @property {{name:"Broward",state:"FL",fips:"12011"}} county - County identity.
 * @property {string} registryVersion - Exact 32-jurisdiction routing registry version.
 * @property {{maxParcels:number,maxAdapterAttempts:number,appraisalDelayMs:number,permitDelayMs:number}} bounds - Hard local request ceilings.
 * @property {BrowardPermitPilotCounters} counters - Attempt/source/record/query reconciliation.
 * @property {{allInputParcelsTerminal:boolean,allRecordsAccountedFor:boolean,queryRowsMatchUniqueRecords:boolean,allJurisdictionsRegistered:boolean,currentSourceJurisdictionsImplemented:number,currentSourceJurisdictionsBlocked:number}} reconciliation - Cross-stage invariants.
 * @property {{localPilotPassed:boolean,countyPermitAcceptancePassed:boolean,reason:string}} acceptance - Pilot execution and full-county acceptance are distinct.
 * @property {readonly {folio:string,jurisdictionKey:string|null,jurisdictionName:string|null,jurisdictionMethod:string,appraisalError:string|null,jurisdictionError:string|null,sourceOutcomes:readonly BrowardPermitSourceOutcome[],recordCount:number}[]} parcels - Per-parcel terminal evidence.
 * @property {readonly {recordKey:string,error:string}[]} conflicts - Conflicting duplicate source identities.
 * @property {{normalizedJsonl:string,queryJsonl:string,parquet:string,coverage:string,checkpoint:string}} artifacts - Local private artifact paths.
 */

/**
 * @typedef {object} BrowardPermitPilotOptions
 * @property {readonly string[]} folios - One through 50 exact Broward folios.
 * @property {string} outputDirectory - Private local artifact directory.
 * @property {string} checkpointPath - Atomic local checkpoint path.
 * @property {number} maxAdapterAttempts - Total permit-adapter request ceiling, never above five.
 * @property {number} appraisalDelayMs - Minimum delay between BCPA requests.
 * @property {number} permitDelayMs - Minimum delay between permit source requests.
 * @property {number} appraisalTimeoutMs - Per-BCPA request timeout.
 * @property {(folio:string)=>Promise<JsonObject>} [fetchAppraisalRecord] - Injectable bounded BCPA record fetcher.
 * @property {Readonly<Record<string, BrowardPermitAdapterRunner>>} [adapterRunners] - Injectable implemented adapters.
 * @property {(milliseconds:number)=>Promise<void>} [sleep] - Injectable delay used by deterministic tests.
 */

/**
 * @typedef {object} BrowardPermitPilotCliOptions
 * @property {"sample" | "pilot" | "folios"} inputMode - Selected bounded input mode.
 * @property {string | null} samplePath - Existing 50-parcel sample CSV/manifest path.
 * @property {readonly string[]} explicitFolios - Explicit CLI folios.
 * @property {string} outputDirectory - Private local artifact directory.
 * @property {string | null} checkpointPath - Optional checkpoint override.
 * @property {number} maxAdapterAttempts - Permit adapter request ceiling.
 * @property {number} appraisalDelayMs - Delay between BCPA lookups.
 * @property {number} permitDelayMs - Delay between permit source requests.
 * @property {boolean} recordNeonStatus - Persist aggregate-only pilot evidence after reconciliation.
 */

const USAGE = `Usage:
  node scripts/run-broward-permit-pilot.mjs --sample <validated-50.csv|manifest.json> [options]
  node scripts/run-broward-permit-pilot.mjs --pilot [options]
  node scripts/run-broward-permit-pilot.mjs --folio <12-char-id> [--folio <id> ...] [options]

Options:
  --output-dir <path>              default: downloads/broward/permit-pilot
  --checkpoint <path>              default: <output-dir>/checkpoint.json
  --max-adapter-attempts <1..5>    default: 5
  --appraisal-delay-ms <>=250>     default: 300
  --permit-delay-ms <>=1000>       default: 1000
  --record-neon-status             write aggregate-only evidence to verified Neon

Safety:
  --sample accepts at most 50 unique validated Broward folios. --pilot uses the
  checked-in 25-folio subset retained by the validated 50-parcel appraisal set.
  Requests are sequential, timeout-bounded, checkpointed, and never use AWS,
  publication, login, CAPTCHA solving, or bypasses. Database writes are disabled
  unless --record-neon-status is explicit; that path sends aggregate counts only
  after a read-only identity gate against the configured isolated Neon branch.
`;

/**
 * Pause without issuing work.
 *
 * @param {number} milliseconds - Non-negative pause duration.
 * @returns {Promise<void>} Resolves after the duration.
 */
function delay(milliseconds) {
  return new Promise((resolvePromise) => {
    setTimeout(resolvePromise, milliseconds);
  });
}

/**
 * Return true only for a non-array JSON object.
 *
 * @param {unknown} value - Candidate parsed value.
 * @returns {value is JsonObject} Whether the value is a JSON object.
 */
function isJsonObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Read a trimmed optional string from an unknown source field.
 *
 * @param {unknown} value - Candidate source value.
 * @returns {string | null} Trimmed non-empty text.
 */
function optionalString(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

/**
 * Normalize and validate a bounded ordered Broward pilot folio list.
 *
 * @param {readonly string[]} values - Candidate folios from CSV, JSON, or CLI.
 * @returns {readonly string[]} Unique canonical folios.
 */
export function validateBrowardPermitPilotFolios(values) {
  const folios = values.map((value) => normalizeBrowardFolio(value));
  if (folios.some((folio) => folio === undefined)) {
    throw new Error(
      "Broward permit pilot folios must be 12-character alphanumeric identifiers",
    );
  }
  const normalized = /** @type {string[]} */ (folios);
  if (normalized.length === 0 || normalized.length > MAX_PILOT_PARCELS) {
    throw new Error(
      `Broward permit pilot requires 1 through ${String(MAX_PILOT_PARCELS)} folios`,
    );
  }
  if (new Set(normalized).size !== normalized.length) {
    throw new Error("Broward permit pilot folios must be unique");
  }
  return normalized;
}

/**
 * Read folios from the existing validation CSV or its JSON selection manifest.
 *
 * The validation seed's first column is the unquoted canonical `parcel_id`;
 * later geometry columns may contain quoted commas but never affect this
 * bounded first-column read. JSON input requires `parcels[].folio`.
 *
 * @param {string} inputPath - Existing validation CSV or manifest path.
 * @returns {Promise<readonly string[]>} Validated ordered folios.
 */
export async function readBrowardPermitPilotFolios(inputPath) {
  const text = await readFile(inputPath, "utf8");
  if (inputPath.toLowerCase().endsWith(".json")) {
    const parsed = /** @type {unknown} */ (JSON.parse(text));
    if (!isJsonObject(parsed) || !Array.isArray(parsed.parcels)) {
      throw new Error("Broward validation manifest must contain parcels[]");
    }
    return validateBrowardPermitPilotFolios(
      parsed.parcels.map((parcel) =>
        isJsonObject(parcel) && typeof parcel.folio === "string"
          ? parcel.folio
          : "",
      ),
    );
  }
  const lines = text.split(/\r?\n/u).filter((line) => line.length > 0);
  if (lines[0]?.split(",", 1)[0] !== "parcel_id") {
    throw new Error(
      "Broward validation CSV must begin with the parcel_id column",
    );
  }
  return validateBrowardPermitPilotFolios(
    lines.slice(1).map((line) => line.split(",", 1)[0] ?? ""),
  );
}

/**
 * Parse the local-only bounded pilot CLI without touching any source.
 *
 * @param {readonly string[]} args - Arguments after the script path.
 * @returns {BrowardPermitPilotCliOptions | null} Parsed options, or null for help.
 */
export function parseBrowardPermitPilotOptions(args) {
  /** @type {string[]} */
  const explicitFolios = [];
  let pilot = false;
  let samplePath = null;
  let outputDirectory = "downloads/broward/permit-pilot";
  let checkpointPath = null;
  let maxAdapterAttempts = MAX_ADAPTER_ATTEMPTS;
  let appraisalDelayMs = 300;
  let permitDelayMs = MIN_PERMIT_DELAY_MS;
  let recordNeonStatus = false;

  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === undefined) {
      throw new Error("Broward permit pilot received an empty CLI argument");
    }
    if (argument === "--help" || argument === "-h") return null;
    if (argument === "--pilot") {
      pilot = true;
      continue;
    }
    if (argument === "--record-neon-status") {
      recordNeonStatus = true;
      continue;
    }
    const [flag, inlineValue] = argument.split("=", 2);
    if (flag === undefined) {
      throw new Error(`Invalid option: ${argument}`);
    }
    const takesValue = [
      "--sample",
      "--folio",
      "--output-dir",
      "--checkpoint",
      "--max-adapter-attempts",
      "--appraisal-delay-ms",
      "--permit-delay-ms",
    ].includes(flag);
    if (!takesValue) throw new Error(`Unknown option: ${argument}`);
    const value = inlineValue ?? args[index + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new Error(`${flag} requires a value`);
    }
    if (inlineValue === undefined) index += 1;
    if (flag === "--sample") samplePath = value.trim();
    else if (flag === "--folio") explicitFolios.push(value);
    else if (flag === "--output-dir") outputDirectory = value.trim();
    else if (flag === "--checkpoint") checkpointPath = value.trim();
    else if (flag === "--max-adapter-attempts") {
      maxAdapterAttempts = Number(value);
    } else if (flag === "--appraisal-delay-ms") {
      appraisalDelayMs = Number(value);
    } else if (flag === "--permit-delay-ms") {
      permitDelayMs = Number(value);
    }
  }

  const inputModeCount =
    Number(pilot) +
    Number(samplePath !== null) +
    Number(explicitFolios.length > 0);
  if (inputModeCount !== 1) {
    throw new Error(
      "Choose exactly one Broward permit input mode: --sample, --pilot, or --folio",
    );
  }
  if (
    outputDirectory.length === 0 ||
    samplePath === "" ||
    checkpointPath === ""
  ) {
    throw new Error("Broward permit pilot paths must not be empty");
  }
  validatePilotBounds({
    folioCount: explicitFolios.length > 0 ? explicitFolios.length : 1,
    maxAdapterAttempts,
    appraisalDelayMs,
    permitDelayMs,
    appraisalTimeoutMs: 30_000,
  });
  const normalizedExplicit =
    explicitFolios.length === 0
      ? []
      : validateBrowardPermitPilotFolios(explicitFolios);
  return {
    inputMode: pilot ? "pilot" : samplePath !== null ? "sample" : "folios",
    samplePath,
    explicitFolios: normalizedExplicit,
    outputDirectory,
    checkpointPath,
    maxAdapterAttempts,
    appraisalDelayMs,
    permitDelayMs,
    recordNeonStatus,
  };
}

/**
 * Fetch one BCPA parcel record with a hard timeout and response-size limit.
 *
 * @param {string} folio - Exact 12-character BCPA folio.
 * @param {number} timeoutMs - Positive request timeout.
 * @returns {Promise<JsonObject>} One exact matching BCPA parcel record.
 */
async function fetchBoundedBcpaRecord(folio, timeoutMs) {
  const response = await fetch(BROWARD_DETAIL_URL, {
    method: "POST",
    headers: {
      accept: "application/json, text/javascript, */*; q=0.01",
      "content-type": "application/json; charset=utf-8",
      origin: "https://web.bcpa.net",
      referer: "https://web.bcpa.net/BcpaClient/search.aspx",
      "x-requested-with": "XMLHttpRequest",
    },
    body: JSON.stringify(browardDetailRequestBody(folio)),
    signal: AbortSignal.timeout(timeoutMs),
  });
  if (!response.ok) {
    throw new Error(
      `BCPA returned HTTP ${String(response.status)} for ${folio}`,
    );
  }
  const contentLength = Number(response.headers.get("content-length") ?? "0");
  if (
    Number.isFinite(contentLength) &&
    contentLength > MAX_BCPA_RESPONSE_BYTES
  ) {
    throw new Error(`BCPA response exceeded the byte limit for ${folio}`);
  }
  const text = await response.text();
  if (
    Buffer.byteLength(text, "utf8") === 0 ||
    Buffer.byteLength(text, "utf8") > MAX_BCPA_RESPONSE_BYTES
  ) {
    throw new Error(`BCPA returned an invalid response size for ${folio}`);
  }
  const parsed = /** @type {unknown} */ (JSON.parse(text));
  if (!isJsonObject(parsed) || !isJsonObject(parsed.d)) {
    throw new Error(`BCPA returned an invalid parcel envelope for ${folio}`);
  }
  const records = parsed.d.parcelInfok__BackingField;
  if (
    !Array.isArray(records) ||
    records.length !== 1 ||
    !isJsonObject(records[0])
  ) {
    throw new Error(`BCPA returned no unique parcel record for ${folio}`);
  }
  const record = records[0];
  if (optionalString(record.folioNumber)?.toUpperCase() !== folio) {
    throw new Error(
      `BCPA parcel identity did not match requested folio ${folio}`,
    );
  }
  return record;
}

/**
 * Run one exact folio through the integrated anonymous BCS/POSSE adapter.
 *
 * @param {string} folio - Exact BCPA parcel identifier.
 * @returns {Promise<ImplementedSourceResult>} Explicit result and request provenance.
 */
async function runBcsAdapter(folio) {
  const result = await probeBrowardBcsPermits({
    parcelIds: [folio],
    maxFolios: 1,
    propertyDelayMs: 1_000,
    detailDelayMs: 300,
    navigationTimeoutMs: 45_000,
    detailTimeoutMs: 30_000,
    maxDetailPagesPerFolio: 75,
  });
  const observation = result.observations[0];
  if (observation === undefined || result.observations.length !== 1) {
    throw new Error(`BCS returned no unique source observation for ${folio}`);
  }
  if (observation.normalizedRecordCount !== result.records.length) {
    throw new Error(`BCS source observation did not reconcile for ${folio}`);
  }
  return {
    status: observation.status,
    records: result.records,
    observation: { ...observation },
  };
}

/**
 * Create a fresh checkpoint parcel state after one bounded BCPA attempt.
 *
 * @param {string} folio - Exact BCPA folio.
 * @param {(folio:string)=>Promise<JsonObject>} fetchAppraisalRecord - Bounded source fetcher.
 * @returns {Promise<BrowardPilotParcelState>} Routed or explicitly failed parcel state.
 */
async function buildParcelState(folio, fetchAppraisalRecord) {
  try {
    const record = await fetchAppraisalRecord(folio);
    const resolution = resolveBrowardPermitJurisdiction(record);
    const situsAddress = [
      optionalString(record.situsAddress1),
      optionalString(record.situsAddress2),
      optionalString(record.situsCity),
      optionalString(record.situsState),
      optionalString(record.situsZipCode),
    ]
      .filter((value) => value !== null)
      .join(" ");
    return {
      folio,
      appraisalAttemptCount: 1,
      appraisalError: null,
      situsCity: optionalString(record.situsCity),
      situsAddress: situsAddress.length === 0 ? null : situsAddress,
      usageCode: optionalString(record.useCode),
      jurisdictionKey: resolution.jurisdiction?.key ?? null,
      jurisdictionName: resolution.jurisdiction?.name ?? null,
      jurisdictionMethod: resolution.method,
      jurisdictionError:
        resolution.jurisdiction === null
          ? `BCPA situs city/address did not match the ${String(BROWARD_PERMIT_JURISDICTIONS.length)}-jurisdiction registry`
          : null,
      sourceOutcomes: [],
      records: [],
    };
  } catch (caught) {
    return {
      folio,
      appraisalAttemptCount: 1,
      appraisalError: caught instanceof Error ? caught.message : String(caught),
      situsCity: null,
      situsAddress: null,
      usageCode: null,
      jurisdictionKey: null,
      jurisdictionName: null,
      jurisdictionMethod: "unresolved",
      jurisdictionError:
        "Permit routing was not attempted without valid BCPA situs evidence",
      sourceOutcomes: [],
      records: [],
    };
  }
}

/**
 * Convert an inaccessible registry route to a terminal no-request outcome.
 *
 * @param {BrowardPermitSourceRoute} source - Current or historical registry route.
 * @returns {BrowardPermitSourceOutcome} Explicit source-unavailable outcome.
 */
function unavailableSourceOutcome(source) {
  if (source.status === "implemented") {
    throw new Error("Implemented source cannot be converted to unavailable");
  }
  return {
    sourceKey: source.sourceKey,
    sourceName: source.sourceName,
    sourceUrl: source.sourceUrl,
    adapterKey: source.adapterKey,
    coverageKind: source.coverageKind,
    attempted: false,
    status: source.status,
    recordCount: 0,
    reason: source.reason,
    observation: null,
  };
}

/**
 * Reconcile all configured sources for one routed parcel.
 *
 * @param {BrowardPilotParcelState} parcel - Mutable checkpoint parcel state.
 * @param {Readonly<Record<string, BrowardPermitAdapterRunner>>} adapters - Implemented local adapter runners.
 * @param {number} attemptedSoFar - Prior actual adapter attempts across this checkpoint.
 * @param {number} maxAdapterAttempts - Total hard request ceiling.
 * @param {number} permitDelayMs - Inter-source request delay.
 * @param {(milliseconds:number)=>Promise<void>} sleep - Delay implementation.
 * @param {{value:number|null}} lastPermitAttemptAt - Mutable request-time holder.
 * @returns {Promise<number>} Number of actual source attempts added.
 */
async function reconcileParcelSources(
  parcel,
  adapters,
  attemptedSoFar,
  maxAdapterAttempts,
  permitDelayMs,
  sleep,
  lastPermitAttemptAt,
) {
  if (parcel.jurisdictionKey === null) return 0;
  const jurisdiction = BROWARD_PERMIT_JURISDICTIONS.find(
    (entry) => entry.key === parcel.jurisdictionKey,
  );
  if (jurisdiction === undefined) {
    throw new Error(
      `Checkpoint references unknown Broward jurisdiction ${parcel.jurisdictionKey}`,
    );
  }
  let addedAttempts = 0;
  for (const source of sourcesForBrowardPermitJurisdiction(jurisdiction)) {
    if (
      parcel.sourceOutcomes.some(
        (outcome) => outcome.sourceKey === source.sourceKey,
      )
    ) {
      continue;
    }
    if (source.status !== "implemented") {
      parcel.sourceOutcomes.push(unavailableSourceOutcome(source));
      continue;
    }
    const runner =
      source.adapterKey === null ? undefined : adapters[source.adapterKey];
    if (runner === undefined) {
      parcel.sourceOutcomes.push({
        sourceKey: source.sourceKey,
        sourceName: source.sourceName,
        sourceUrl: source.sourceUrl,
        adapterKey: source.adapterKey,
        coverageKind: source.coverageKind,
        attempted: false,
        status: "adapter_unavailable",
        recordCount: 0,
        reason: `Registry marks ${source.adapterKey ?? "this source"} implemented, but no runner was configured`,
        observation: null,
      });
      continue;
    }
    if (attemptedSoFar + addedAttempts >= maxAdapterAttempts) {
      parcel.sourceOutcomes.push({
        sourceKey: source.sourceKey,
        sourceName: source.sourceName,
        sourceUrl: source.sourceUrl,
        adapterKey: source.adapterKey,
        coverageKind: source.coverageKind,
        attempted: false,
        status: "request_cap_reached",
        recordCount: 0,
        reason: `Local pilot adapter-attempt cap ${String(maxAdapterAttempts)} reached`,
        observation: null,
      });
      continue;
    }
    const now = Date.now();
    if (lastPermitAttemptAt.value !== null) {
      const remaining = permitDelayMs - (now - lastPermitAttemptAt.value);
      if (remaining > 0) await sleep(remaining);
    }
    lastPermitAttemptAt.value = Date.now();
    addedAttempts += 1;
    try {
      const result = await runner(parcel.folio);
      validateImplementedSourceResult(result, parcel.folio);
      parcel.records.push(...result.records);
      parcel.sourceOutcomes.push({
        sourceKey: source.sourceKey,
        sourceName: source.sourceName,
        sourceUrl: source.sourceUrl,
        adapterKey: source.adapterKey,
        coverageKind: source.coverageKind,
        attempted: true,
        status: result.status,
        recordCount: result.records.length,
        reason:
          result.status === "records"
            ? "Official source returned normalized records"
            : "Official source resolved the parcel and returned an explicit no-permits result",
        observation: result.observation,
      });
    } catch (caught) {
      parcel.sourceOutcomes.push({
        sourceKey: source.sourceKey,
        sourceName: source.sourceName,
        sourceUrl: source.sourceUrl,
        adapterKey: source.adapterKey,
        coverageKind: source.coverageKind,
        attempted: true,
        status: "source_failed",
        recordCount: 0,
        reason: caught instanceof Error ? caught.message : String(caught),
        observation: null,
      });
    }
  }
  return addedAttempts;
}

/**
 * Validate one adapter result and every normalized record before checkpointing.
 *
 * @param {ImplementedSourceResult} result - Candidate adapter response.
 * @param {string} folio - Exact expected BCPA parcel identifier.
 * @returns {void}
 */
function validateImplementedSourceResult(result, folio) {
  if (
    (result.status !== "records" && result.status !== "no_permits") ||
    !Array.isArray(result.records) ||
    !isJsonObject(result.observation)
  ) {
    throw new Error(`Permit adapter returned an invalid result for ${folio}`);
  }
  if (
    (result.status === "records" && result.records.length === 0) ||
    (result.status === "no_permits" && result.records.length !== 0)
  ) {
    throw new Error(
      `Permit adapter result status/count disagreed for ${folio}`,
    );
  }
  for (const record of result.records) {
    if (!isNormalizedPermit(record) || record.parcel_identifier !== folio) {
      throw new Error(
        `Permit adapter returned an invalid parcel record for ${folio}`,
      );
    }
  }
}

/**
 * Check the minimum BCS/query fields required by the county orchestrator.
 *
 * @param {unknown} value - Candidate normalized permit.
 * @returns {value is BrowardNormalizedPermit} Whether the query contract is complete.
 */
function isNormalizedPermit(value) {
  if (!isJsonObject(value)) return false;
  return (
    typeof value.source_system === "string" &&
    typeof value.source_url === "string" &&
    typeof value.source_object_id === "string" &&
    (value.source_record_kind === "master" ||
      value.source_record_kind === "permit") &&
    typeof value.record_key === "string" &&
    typeof value.parcel_identifier === "string" &&
    typeof value.permit_number === "string" &&
    typeof value.record_status === "string" &&
    typeof value.record_type === "string" &&
    (typeof value.permit_issue_date === "string" ||
      value.permit_issue_date === null) &&
    (typeof value.application_date === "string" ||
      value.application_date === null) &&
    (typeof value.expiration_date === "string" ||
      value.expiration_date === null) &&
    (typeof value.project_title === "string" || value.project_title === null) &&
    (typeof value.project_description === "string" ||
      value.project_description === null) &&
    (typeof value.job_value === "number" || value.job_value === null) &&
    Array.isArray(value.inspections) &&
    value.inspections.every(
      (inspection) =>
        isJsonObject(inspection) &&
        (typeof inspection.completed_date === "string" ||
          inspection.completed_date === null),
    )
  );
}

/**
 * Deduplicate normalized records by source identity while preserving conflicts.
 *
 * @param {readonly BrowardNormalizedPermit[]} records - Raw checkpoint records.
 * @returns {{records:readonly BrowardNormalizedPermit[],duplicateCount:number,conflicts:readonly {recordKey:string,error:string}[]}} Unique records and reconciliation findings.
 */
export function dedupeBrowardPermitPilotRecords(records) {
  /** @type {Map<string, BrowardNormalizedPermit>} */
  const byKey = new Map();
  /** @type {{recordKey:string,error:string}[]} */
  const conflicts = [];
  let duplicateCount = 0;
  for (const record of records) {
    const existing = byKey.get(record.record_key);
    if (existing === undefined) {
      byKey.set(record.record_key, record);
      continue;
    }
    if (JSON.stringify(existing) === JSON.stringify(record)) {
      duplicateCount += 1;
    } else {
      conflicts.push({
        recordKey: record.record_key,
        error:
          "Same source record key produced conflicting normalized payloads",
      });
    }
  }
  return {
    records: [...byKey.values()].sort(
      (left, right) =>
        left.parcel_identifier.localeCompare(right.parcel_identifier) ||
        left.record_key.localeCompare(right.record_key),
    ),
    duplicateCount,
    conflicts,
  };
}

/**
 * Execute and checkpoint the bounded property-first Broward permit pilot.
 *
 * Every input parcel first resolves through BCPA situs evidence. The resolved
 * registry row then either calls an implemented adapter or records an explicit
 * unavailable/login/CAPTCHA/custodian result. The only default runnable source
 * is BCS for BMSD, Lazy Lake, and the separately labelled historical
 * Lauderdale-by-the-Sea route. All source calls are sequential and capped.
 *
 * @param {BrowardPermitPilotOptions} options - Inputs, safety bounds, paths, and optional test dependencies.
 * @returns {Promise<BrowardPermitPilotReport>} Reconciled local pilot report.
 */
export async function runBrowardPermitPilot(options) {
  const folios = validateBrowardPermitPilotFolios(options.folios);
  validatePilotBounds({
    folioCount: folios.length,
    maxAdapterAttempts: options.maxAdapterAttempts,
    appraisalDelayMs: options.appraisalDelayMs,
    permitDelayMs: options.permitDelayMs,
    appraisalTimeoutMs: options.appraisalTimeoutMs,
  });
  const outputDirectory = resolve(options.outputDirectory);
  const checkpointPath = resolve(options.checkpointPath);
  const sourceSignature = buildSourceSignature(
    folios,
    options.maxAdapterAttempts,
  );
  const checkpoint = await readCheckpoint(
    checkpointPath,
    sourceSignature,
    folios,
  );
  const fetchAppraisalRecord =
    options.fetchAppraisalRecord ??
    ((folio) => fetchBoundedBcpaRecord(folio, options.appraisalTimeoutMs));
  const adapters =
    options.adapterRunners ??
    Object.freeze({ [BROWARD_BCS_ADAPTER_KEY]: runBcsAdapter });
  const sleep = options.sleep ?? delay;
  const lastPermitAttemptAt = { value: /** @type {number | null} */ (null) };
  let adapterAttempts = Object.values(checkpoint.parcels)
    .flatMap((parcel) => parcel.sourceOutcomes)
    .filter((outcome) => outcome.attempted).length;
  let lastAppraisalAttemptAt = /** @type {number | null} */ (null);

  await mkdir(outputDirectory, { recursive: true, mode: 0o700 });
  for (const folio of folios) {
    let parcel = checkpoint.parcels[folio];
    if (parcel === undefined) {
      const now = Date.now();
      if (lastAppraisalAttemptAt !== null) {
        const remaining =
          options.appraisalDelayMs - (now - lastAppraisalAttemptAt);
        if (remaining > 0) await sleep(remaining);
      }
      lastAppraisalAttemptAt = Date.now();
      parcel = await buildParcelState(folio, fetchAppraisalRecord);
      checkpoint.parcels[folio] = parcel;
      await writeCheckpoint(checkpointPath, checkpoint);
    }
    adapterAttempts += await reconcileParcelSources(
      parcel,
      adapters,
      adapterAttempts,
      options.maxAdapterAttempts,
      options.permitDelayMs,
      sleep,
      lastPermitAttemptAt,
    );
    await writeCheckpoint(checkpointPath, checkpoint);
  }

  const rawRecords = folios.flatMap(
    (folio) => checkpoint.parcels[folio]?.records ?? [],
  );
  const deduplicated = dedupeBrowardPermitPilotRecords(rawRecords);
  const queryRows = deduplicated.records.map(mapBrowardPermitToDonphanRow);
  const normalizedJsonl = join(
    outputDirectory,
    "normalized-permits.private.jsonl",
  );
  const queryJsonl = join(outputDirectory, "permit-query-rows.private.jsonl");
  const parquet = join(outputDirectory, "permit-table.parquet");
  const coverage = join(outputDirectory, "permit-coverage.json");
  const reconciliation = join(outputDirectory, "reconciliation.json");
  await writePrivateJsonl(
    normalizedJsonl,
    deduplicated.records.map((record) => ({ ...record })),
  );
  await writePrivateJsonl(
    queryJsonl,
    queryRows.map((row) => ({ ...row })),
  );
  const parquetArtifact = await writeDonphanPermitParquet(parquet, queryRows);
  const report = buildPilotReport({
    checkpoint,
    checkpointPath,
    conflicts: deduplicated.conflicts,
    duplicateCount: deduplicated.duplicateCount,
    normalizedJsonl,
    outputCoveragePath: coverage,
    outputParquetPath: parquetArtifact.parquetPath,
    outputQueryJsonlPath: queryJsonl,
    queryRows,
    maxAdapterAttempts: options.maxAdapterAttempts,
    appraisalDelayMs: options.appraisalDelayMs,
    permitDelayMs: options.permitDelayMs,
  });
  await writeJsonAtomically(reconciliation, report);
  await writeJsonAtomically(coverage, {
    schemaVersion: 1,
    generatedAt: report.generatedAt,
    county: report.county,
    registryVersion: BROWARD_PERMIT_REGISTRY_VERSION,
    registry: BROWARD_PERMIT_JURISDICTIONS.map((entry) => ({
      key: entry.key,
      name: entry.name,
      primarySource: entry.primarySource,
      supplementalSources: entry.supplementalSources,
    })),
    counters: report.counters,
    acceptance: report.acceptance,
    parquet: {
      path: parquetArtifact.parquetPath,
      rowCount: parquetArtifact.rowCount,
      sha256: parquetArtifact.sha256,
      columns: DONPHAN_PERMIT_QUERY_COLUMNS,
    },
    sourceOutcomes: report.parcels.flatMap((parcel) =>
      parcel.sourceOutcomes.map((outcome) => ({
        folio: parcel.folio,
        jurisdictionKey: parcel.jurisdictionKey,
        ...outcome,
      })),
    ),
  });
  return {
    ...report,
    artifacts: {
      ...report.artifacts,
      coverage,
    },
  };
}

/**
 * Validate hard request ceilings before any source or output operation.
 *
 * @param {{folioCount:number,maxAdapterAttempts:number,appraisalDelayMs:number,permitDelayMs:number,appraisalTimeoutMs:number}} bounds - Candidate local pilot bounds.
 * @returns {void}
 */
function validatePilotBounds(bounds) {
  if (
    !Number.isInteger(bounds.folioCount) ||
    bounds.folioCount <= 0 ||
    bounds.folioCount > MAX_PILOT_PARCELS
  ) {
    throw new Error(
      `Broward permit pilot parcel count must be from 1 through ${String(MAX_PILOT_PARCELS)}`,
    );
  }
  if (
    !Number.isInteger(bounds.maxAdapterAttempts) ||
    bounds.maxAdapterAttempts <= 0 ||
    bounds.maxAdapterAttempts > MAX_ADAPTER_ATTEMPTS
  ) {
    throw new Error(
      `Broward permit maxAdapterAttempts must be from 1 through ${String(MAX_ADAPTER_ATTEMPTS)}`,
    );
  }
  if (
    !Number.isInteger(bounds.appraisalDelayMs) ||
    bounds.appraisalDelayMs < MIN_APPRAISAL_DELAY_MS
  ) {
    throw new Error(
      `Broward appraisal delay must be at least ${String(MIN_APPRAISAL_DELAY_MS)} ms`,
    );
  }
  if (
    !Number.isInteger(bounds.permitDelayMs) ||
    bounds.permitDelayMs < MIN_PERMIT_DELAY_MS
  ) {
    throw new Error(
      `Broward permit delay must be at least ${String(MIN_PERMIT_DELAY_MS)} ms`,
    );
  }
  if (
    !Number.isInteger(bounds.appraisalTimeoutMs) ||
    bounds.appraisalTimeoutMs <= 0
  ) {
    throw new Error("Broward appraisal timeout must be a positive integer");
  }
}

/**
 * Build the immutable checkpoint signature for exact safe resume.
 *
 * @param {readonly string[]} folios - Ordered canonical pilot folios.
 * @param {number} maxAdapterAttempts - Adapter request cap.
 * @returns {string} SHA-256 source signature.
 */
function buildSourceSignature(folios, maxAdapterAttempts) {
  return createHash("sha256")
    .update(
      JSON.stringify({
        checkpointSchemaVersion: CHECKPOINT_SCHEMA_VERSION,
        registryVersion: BROWARD_PERMIT_REGISTRY_VERSION,
        folios,
        maxAdapterAttempts,
      }),
      "utf8",
    )
    .digest("hex");
}

/**
 * Read a compatible local checkpoint or initialize empty state.
 *
 * @param {string} checkpointPath - Atomic checkpoint path.
 * @param {string} sourceSignature - Exact expected input/config signature.
 * @param {readonly string[]} folios - Ordered canonical pilot folios.
 * @returns {Promise<BrowardPermitPilotCheckpoint>} Valid resumable state.
 */
async function readCheckpoint(checkpointPath, sourceSignature, folios) {
  try {
    const parsed = /** @type {unknown} */ (
      JSON.parse(await readFile(checkpointPath, "utf8"))
    );
    if (!isCheckpoint(parsed)) {
      throw new Error(`Invalid Broward permit checkpoint: ${checkpointPath}`);
    }
    if (
      parsed.sourceSignature !== sourceSignature ||
      JSON.stringify(parsed.folios) !== JSON.stringify(folios)
    ) {
      throw new Error(
        `Broward permit checkpoint does not match this bounded run: ${checkpointPath}`,
      );
    }
    return parsed;
  } catch (caught) {
    if (
      caught instanceof Error &&
      "code" in caught &&
      caught.code === "ENOENT"
    ) {
      return {
        schemaVersion: CHECKPOINT_SCHEMA_VERSION,
        sourceSignature,
        folios,
        parcels: {},
      };
    }
    throw caught;
  }
}

/**
 * Strictly validate persisted checkpoint identity and parcel-state shape.
 *
 * @param {unknown} value - Parsed checkpoint JSON.
 * @returns {value is BrowardPermitPilotCheckpoint} Whether the checkpoint is safe to resume.
 */
function isCheckpoint(value) {
  if (!isJsonObject(value)) return false;
  if (
    value.schemaVersion !== CHECKPOINT_SCHEMA_VERSION ||
    typeof value.sourceSignature !== "string" ||
    !Array.isArray(value.folios) ||
    !value.folios.every((folio) => typeof folio === "string") ||
    !isJsonObject(value.parcels)
  ) {
    return false;
  }
  return Object.values(value.parcels).every(isParcelState);
}

/**
 * Validate a persisted parcel checkpoint state.
 *
 * @param {unknown} value - Candidate parcel state.
 * @returns {value is BrowardPilotParcelState} Whether all resumable fields are valid.
 */
function isParcelState(value) {
  if (!isJsonObject(value)) return false;
  return (
    typeof value.folio === "string" &&
    typeof value.appraisalAttemptCount === "number" &&
    (typeof value.appraisalError === "string" ||
      value.appraisalError === null) &&
    (typeof value.situsCity === "string" || value.situsCity === null) &&
    (typeof value.situsAddress === "string" || value.situsAddress === null) &&
    (typeof value.usageCode === "string" || value.usageCode === null) &&
    (typeof value.jurisdictionKey === "string" ||
      value.jurisdictionKey === null) &&
    (typeof value.jurisdictionName === "string" ||
      value.jurisdictionName === null) &&
    (value.jurisdictionMethod === "situs_city" ||
      value.jurisdictionMethod === "situs_address" ||
      value.jurisdictionMethod === "unresolved") &&
    (typeof value.jurisdictionError === "string" ||
      value.jurisdictionError === null) &&
    Array.isArray(value.sourceOutcomes) &&
    value.sourceOutcomes.every(isSourceOutcome) &&
    Array.isArray(value.records) &&
    value.records.every(isNormalizedPermit)
  );
}

/**
 * Validate a persisted explicit source outcome.
 *
 * @param {unknown} value - Candidate source outcome.
 * @returns {value is BrowardPermitSourceOutcome} Whether required terminal fields exist.
 */
function isSourceOutcome(value) {
  if (!isJsonObject(value)) return false;
  return (
    typeof value.sourceKey === "string" &&
    typeof value.sourceName === "string" &&
    typeof value.sourceUrl === "string" &&
    (typeof value.adapterKey === "string" || value.adapterKey === null) &&
    (value.coverageKind === "current" ||
      value.coverageKind === "historical" ||
      value.coverageKind === "supplemental") &&
    typeof value.attempted === "boolean" &&
    [
      "records",
      "no_permits",
      "adapter_unavailable",
      "captcha_required",
      "login_required",
      "no_anonymous_search",
      "custodian_only",
      "egress_unavailable",
      "request_cap_reached",
      "source_failed",
    ].includes(String(value.status)) &&
    typeof value.recordCount === "number" &&
    typeof value.reason === "string" &&
    (value.observation === null || isJsonObject(value.observation))
  );
}

/**
 * Persist checkpoint/report JSON through a sibling temporary file and rename.
 *
 * @param {string} outputPath - Final local JSON path.
 * @param {unknown} value - JSON-compatible checkpoint/report value.
 * @returns {Promise<void>} Resolves after atomic replacement.
 */
async function writeJsonAtomically(outputPath, value) {
  await mkdir(dirname(outputPath), { recursive: true, mode: 0o700 });
  const temporaryPath = `${outputPath}.tmp`;
  await writeFile(temporaryPath, `${JSON.stringify(value, null, 2)}\n`, {
    encoding: "utf8",
    mode: 0o600,
  });
  await rename(temporaryPath, outputPath);
}

/**
 * Persist the current resumable checkpoint atomically.
 *
 * @param {string} checkpointPath - Final checkpoint path.
 * @param {BrowardPermitPilotCheckpoint} checkpoint - Complete resumable state.
 * @returns {Promise<void>} Resolves after checkpoint replacement.
 */
function writeCheckpoint(checkpointPath, checkpoint) {
  return writeJsonAtomically(checkpointPath, checkpoint);
}

/**
 * Build final counters, invariants, and full-county acceptance distinction.
 *
 * @param {object} input - Checkpoint, artifact, bound, and dedupe evidence.
 * @param {BrowardPermitPilotCheckpoint} input.checkpoint - Complete terminal checkpoint.
 * @param {string} input.checkpointPath - Local checkpoint path.
 * @param {readonly {recordKey:string,error:string}[]} input.conflicts - Conflicting duplicate records.
 * @param {number} input.duplicateCount - Exact duplicates removed.
 * @param {string} input.normalizedJsonl - Private normalized source JSONL.
 * @param {string} input.outputCoveragePath - Coverage JSON path.
 * @param {string} input.outputParquetPath - Donphan permit Parquet path.
 * @param {string} input.outputQueryJsonlPath - Private Donphan-shape JSONL.
 * @param {readonly DonphanPermitQueryRow[]} input.queryRows - Unique Donphan rows.
 * @param {number} input.maxAdapterAttempts - Adapter attempt ceiling.
 * @param {number} input.appraisalDelayMs - BCPA request delay.
 * @param {number} input.permitDelayMs - Permit source request delay.
 * @returns {BrowardPermitPilotReport} Reconciled final report.
 */
function buildPilotReport(input) {
  const parcelStates = input.checkpoint.folios.map(
    (folio) => input.checkpoint.parcels[folio],
  );
  const completeParcelStates = parcelStates.filter(
    (parcel) => parcel !== undefined,
  );
  const sourceOutcomes = completeParcelStates.flatMap(
    (parcel) => parcel.sourceOutcomes,
  );
  const rawPermitRecords = completeParcelStates.reduce(
    (total, parcel) => total + parcel.records.length,
    0,
  );
  const unavailableStatuses = new Set([
    "adapter_unavailable",
    "captcha_required",
    "login_required",
    "custodian_only",
    "egress_unavailable",
    "request_cap_reached",
  ]);
  const permitAttemptedFolios = new Set(
    completeParcelStates.flatMap((parcel) =>
      parcel.sourceOutcomes.some((outcome) => outcome.attempted)
        ? [parcel.folio]
        : [],
    ),
  );
  const currentSourceJurisdictionsImplemented =
    BROWARD_PERMIT_JURISDICTIONS.filter(
      (entry) => entry.primarySource.status === "implemented",
    ).length;
  const counters = {
    sampleParcels: input.checkpoint.folios.length,
    appraisalAttempts: completeParcelStates.reduce(
      (total, parcel) => total + parcel.appraisalAttemptCount,
      0,
    ),
    appraisalResolved: completeParcelStates.filter(
      (parcel) => parcel.appraisalError === null,
    ).length,
    jurisdictionResolved: completeParcelStates.filter(
      (parcel) => parcel.jurisdictionKey !== null,
    ).length,
    jurisdictionUnresolved: completeParcelStates.filter(
      (parcel) => parcel.jurisdictionKey === null,
    ).length,
    sourceOutcomes: sourceOutcomes.length,
    sourceUnavailableOutcomes: sourceOutcomes.filter((outcome) =>
      unavailableStatuses.has(outcome.status),
    ).length,
    permitSourceAttempts: sourceOutcomes.filter((outcome) => outcome.attempted)
      .length,
    permitAttemptedParcels: permitAttemptedFolios.size,
    explicitNoPermitOutcomes: sourceOutcomes.filter(
      (outcome) => outcome.status === "no_permits",
    ).length,
    sourceFailures: sourceOutcomes.filter(
      (outcome) => outcome.status === "source_failed",
    ).length,
    rawPermitRecords,
    duplicatePermitRecords: input.duplicateCount,
    conflictingPermitRecords: input.conflicts.length,
    uniquePermitRecords: input.queryRows.length,
    queryRows: input.queryRows.length,
  };
  const allInputParcelsTerminal =
    completeParcelStates.length === input.checkpoint.folios.length &&
    completeParcelStates.every(
      (parcel) =>
        parcel.appraisalError !== null ||
        parcel.jurisdictionKey === null ||
        (() => {
          const jurisdiction = BROWARD_PERMIT_JURISDICTIONS.find(
            (entry) => entry.key === parcel.jurisdictionKey,
          );
          return (
            jurisdiction !== undefined &&
            parcel.sourceOutcomes.length ===
              sourcesForBrowardPermitJurisdiction(jurisdiction).length
          );
        })(),
    );
  const allRecordsAccountedFor =
    counters.rawPermitRecords ===
    counters.uniquePermitRecords +
      counters.duplicatePermitRecords +
      counters.conflictingPermitRecords;
  const queryRowsMatchUniqueRecords =
    counters.queryRows === counters.uniquePermitRecords;
  const localPilotPassed =
    allInputParcelsTerminal &&
    allRecordsAccountedFor &&
    queryRowsMatchUniqueRecords &&
    counters.appraisalResolved === counters.sampleParcels &&
    counters.jurisdictionUnresolved === 0 &&
    counters.sourceFailures === 0 &&
    counters.conflictingPermitRecords === 0;
  const currentSourceJurisdictionsBlocked =
    BROWARD_PERMIT_JURISDICTIONS.length - currentSourceJurisdictionsImplemented;
  const countyPermitAcceptancePassed =
    localPilotPassed &&
    currentSourceJurisdictionsBlocked === 0 &&
    counters.sourceUnavailableOutcomes === 0 &&
    counters.queryRows > 0;

  return {
    schemaVersion: PILOT_REPORT_SCHEMA_VERSION,
    mode: "local-checkpointed-property-first-permit-pilot",
    generatedAt: new Date().toISOString(),
    county: { name: "Broward", state: "FL", fips: "12011" },
    registryVersion: BROWARD_PERMIT_REGISTRY_VERSION,
    bounds: {
      maxParcels: MAX_PILOT_PARCELS,
      maxAdapterAttempts: input.maxAdapterAttempts,
      appraisalDelayMs: input.appraisalDelayMs,
      permitDelayMs: input.permitDelayMs,
    },
    counters,
    reconciliation: {
      allInputParcelsTerminal,
      allRecordsAccountedFor,
      queryRowsMatchUniqueRecords,
      allJurisdictionsRegistered: BROWARD_PERMIT_JURISDICTIONS.length === 32,
      currentSourceJurisdictionsImplemented,
      currentSourceJurisdictionsBlocked,
    },
    acceptance: {
      localPilotPassed,
      countyPermitAcceptancePassed,
      reason: countyPermitAcceptancePassed
        ? "Every current jurisdiction route and pilot/query reconciliation gate passed"
        : "Appraisal acceptance and a bounded permit pilot do not establish full permit acceptance while current municipal sources remain unavailable",
    },
    parcels: completeParcelStates.map((parcel) => ({
      folio: parcel.folio,
      jurisdictionKey: parcel.jurisdictionKey,
      jurisdictionName: parcel.jurisdictionName,
      jurisdictionMethod: parcel.jurisdictionMethod,
      appraisalError: parcel.appraisalError,
      jurisdictionError: parcel.jurisdictionError,
      sourceOutcomes: parcel.sourceOutcomes,
      recordCount: parcel.records.length,
    })),
    conflicts: input.conflicts,
    artifacts: {
      normalizedJsonl: input.normalizedJsonl,
      queryJsonl: input.outputQueryJsonlPath,
      parquet: input.outputParquetPath,
      coverage: input.outputCoveragePath,
      checkpoint: input.checkpointPath,
    },
  };
}

/**
 * Require the direct Neon connection and independently configured isolated
 * branch identity used by the explicit aggregate-status write path.
 *
 * @param {NodeJS.ProcessEnv} environment - Runtime secrets and expected Neon identity.
 * @returns {{connectionString:string,expectedBranchId:string,expectedEndpointId:string}}
 *   Validated values retained only in process memory.
 */
function requirePermitStatusTarget(environment) {
  const connectionString = environment.DATABASE_URL_UNPOOLED;
  const expectedBranchId = environment.BROWARD_INGEST_NEON_BRANCH_ID;
  const expectedEndpointId = environment.BROWARD_INGEST_NEON_ENDPOINT_ID;
  if (typeof connectionString !== "string" || connectionString.trim() === "") {
    throw new Error(
      "DATABASE_URL_UNPOOLED is required to record permit status",
    );
  }
  let parsed;
  try {
    parsed = new URL(connectionString);
  } catch {
    throw new Error("DATABASE_URL_UNPOOLED is not a valid PostgreSQL URL");
  }
  if (
    !["postgres:", "postgresql:"].includes(parsed.protocol) ||
    parsed.hostname.includes("-pooler")
  ) {
    throw new Error(
      "Permit status recording requires a direct PostgreSQL endpoint",
    );
  }
  if (
    typeof expectedBranchId !== "string" ||
    !/^br-[a-z0-9-]+$/u.test(expectedBranchId)
  ) {
    throw new Error("BROWARD_INGEST_NEON_BRANCH_ID is required");
  }
  if (
    typeof expectedEndpointId !== "string" ||
    !/^ep-[a-z0-9-]+$/u.test(expectedEndpointId) ||
    expectedEndpointId.startsWith(PRODUCTION_ENDPOINT_PREFIX)
  ) {
    throw new Error(
      "BROWARD_INGEST_NEON_ENDPOINT_ID must identify non-production Neon",
    );
  }
  return { connectionString, expectedBranchId, expectedEndpointId };
}

/**
 * Prove the permit-status connection target using immutable Neon settings
 * inside a read-only transaction before any aggregate write.
 *
 * @param {import("pg").Client} client - Connected direct Neon client.
 * @param {{expectedBranchId:string,expectedEndpointId:string}} expected - Independently configured identity.
 * @returns {Promise<void>} Resolves only for the isolated Broward branch.
 */
export async function verifyBrowardPermitStatusTarget(client, expected) {
  await client.query("BEGIN READ ONLY");
  try {
    const result = await client.query(
      `SELECT
         current_setting('neon.project_id', true) AS project_id,
         current_setting('neon.branch_id', true) AS branch_id,
         current_setting('neon.endpoint_id', true) AS endpoint_id`,
    );
    const row = result.rows[0];
    if (
      row?.project_id !== EXPECTED_NEON_PROJECT_ID ||
      row?.branch_id !== expected.expectedBranchId ||
      row?.endpoint_id !== expected.expectedEndpointId ||
      expected.expectedEndpointId.startsWith(PRODUCTION_ENDPOINT_PREFIX)
    ) {
      throw new Error("Permit status target identity mismatch");
    }
    await client.query("ROLLBACK");
  } catch {
    await client.query("ROLLBACK");
    throw new Error(
      "Permit status target is not the verified isolated Broward branch",
    );
  }
}

/**
 * Project a reconciled pilot report into the aggregate-only Neon function.
 * Per-parcel evidence and source payloads are intentionally not referenced.
 *
 * @param {import("pg").Client} client - Identity-verified direct Neon client.
 * @param {BrowardPermitPilotReport} report - Reconciled bounded pilot report.
 * @returns {Promise<void>} Resolves after the aggregate projection commits.
 */
export async function recordBrowardPermitPilotStatus(client, report) {
  if (
    report.mode !== "local-checkpointed-property-first-permit-pilot" ||
    report.county.fips !== "12011" ||
    !Number.isFinite(Date.parse(report.generatedAt))
  ) {
    throw new Error("Permit pilot report identity is invalid");
  }
  const counters = report.counters;
  const reconciliation = report.reconciliation;
  const acceptance = report.acceptance;
  await client.query(
    `SELECT ingest_control.record_broward_permit_pilot_status(
       $1::integer, $2::integer, $3::integer, $4::integer, $5::integer,
       $6::integer, $7::integer, $8::integer, $9::integer, $10::integer,
       $11::integer, $12::integer, $13::integer, $14::integer, $15::integer,
       $16::integer, $17::boolean, $18::boolean, $19::boolean, $20::boolean,
       $21::boolean, $22::timestamptz
     )`,
    [
      counters.sampleParcels,
      counters.appraisalAttempts,
      counters.appraisalResolved,
      counters.jurisdictionResolved,
      counters.jurisdictionUnresolved,
      counters.sourceOutcomes,
      counters.sourceUnavailableOutcomes,
      counters.permitSourceAttempts,
      counters.permitAttemptedParcels,
      counters.explicitNoPermitOutcomes,
      counters.sourceFailures,
      counters.rawPermitRecords,
      counters.duplicatePermitRecords,
      counters.conflictingPermitRecords,
      counters.uniquePermitRecords,
      counters.queryRows,
      reconciliation.allInputParcelsTerminal,
      reconciliation.allRecordsAccountedFor,
      reconciliation.queryRowsMatchUniqueRecords,
      acceptance.localPilotPassed,
      acceptance.countyPermitAcceptancePassed,
      report.generatedAt,
    ],
  );
}

/**
 * Open the explicit direct connection, verify its identity, and persist only
 * aggregate permit pilot counters. Connection details are never logged.
 *
 * @param {BrowardPermitPilotReport} report - Reconciled bounded pilot report.
 * @param {NodeJS.ProcessEnv} [environment=process.env] - Runtime target configuration.
 * @returns {Promise<void>} Resolves after the safe aggregate write.
 */
async function persistBrowardPermitPilotStatus(
  report,
  environment = process.env,
) {
  const target = requirePermitStatusTarget(environment);
  const client = new Client({
    application_name: "broward-permit-pilot-status",
    connectionString: target.connectionString,
    connectionTimeoutMillis: 10_000,
    statement_timeout: 30_000,
  });
  await client.connect();
  try {
    await verifyBrowardPermitStatusTarget(client, target);
    const functionResult = await client.query(
      "SELECT to_regprocedure($1) IS NOT NULL AS installed",
      [PERMIT_STATUS_FUNCTION],
    );
    if (functionResult.rows[0]?.installed !== true) {
      throw new Error("Aggregate permit status migration is not installed");
    }
    await recordBrowardPermitPilotStatus(client, report);
  } finally {
    await client.end();
  }
}

/**
 * Run the parsed CLI in local-only mode.
 *
 * @returns {Promise<void>} Resolves after artifacts and one JSON summary line.
 */
async function main() {
  const cli = parseBrowardPermitPilotOptions(process.argv.slice(2));
  if (cli === null) {
    process.stdout.write(USAGE);
    return;
  }
  const folios =
    cli.inputMode === "pilot"
      ? BROWARD_PILOT_FOLIOS
      : cli.inputMode === "sample"
        ? await readBrowardPermitPilotFolios(
            /** @type {string} */ (cli.samplePath),
          )
        : cli.explicitFolios;
  const outputDirectory = resolve(cli.outputDirectory);
  const report = await runBrowardPermitPilot({
    folios,
    outputDirectory,
    checkpointPath: resolve(
      cli.checkpointPath ?? join(outputDirectory, "checkpoint.json"),
    ),
    maxAdapterAttempts: cli.maxAdapterAttempts,
    appraisalDelayMs: cli.appraisalDelayMs,
    permitDelayMs: cli.permitDelayMs,
    appraisalTimeoutMs: 30_000,
  });
  if (cli.recordNeonStatus) {
    await persistBrowardPermitPilotStatus(report);
  }
  process.stdout.write(
    `${JSON.stringify({
      event: "broward_permit_pilot_completed",
      inputMode: cli.inputMode,
      inputName:
        cli.inputMode === "sample" && cli.samplePath !== null
          ? basename(cli.samplePath)
          : cli.inputMode,
      counters: report.counters,
      reconciliation: report.reconciliation,
      acceptance: report.acceptance,
      durableStatusRecorded: cli.recordNeonStatus,
      artifacts: report.artifacts,
    })}\n`,
  );
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((caught) => {
    process.stderr.write(
      `${JSON.stringify({
        event: "broward_permit_pilot_failed",
        error: caught instanceof Error ? caught.message : String(caught),
      })}\n`,
    );
    process.exitCode = 1;
  });
}
