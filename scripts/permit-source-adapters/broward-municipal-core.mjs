// @ts-check

import { createHash } from "node:crypto";

/**
 * @typedef {"coconut_creek" | "click2gov" | "tyler_esuite" | "tyler_energov" | "gov_easy" | "smartgov" | "opengov" | "communitycore" | "mgo_connect" | "egovplus" | "records_request"} BrowardMunicipalProtocol
 */

/**
 * @typedef {"anonymous" | "login_required" | "captcha_required" | "no_anonymous_search" | "records_request"} BrowardMunicipalAccessMode
 */

/**
 * @typedef {"enabled" | "landing_only" | "blocked"} BrowardMunicipalProbeStatus
 */

/**
 * @typedef {"permit_number" | "address" | "folio"} BrowardMunicipalQueryKind
 */

/**
 * @typedef {object} BrowardMunicipalQuery
 * @property {BrowardMunicipalQueryKind} kind - Exact source field used for this lookup.
 * @property {string} value - Trimmed query text; folios are canonical uppercase 12-character strings.
 */

/**
 * @typedef {object} BrowardMunicipalCapability
 * @property {readonly BrowardMunicipalQueryKind[]} searchBy - Anonymous or documented query fields exposed by the source.
 * @property {"none" | "client_all" | "numbered" | "cursor"} pagination - Source pagination contract.
 * @property {"none" | "same_session" | "public_url"} detail - Detail-page access contract.
 * @property {boolean} inspections - Whether public detail can expose inspection status.
 * @property {boolean} planReview - Whether public detail can expose plan-review status.
 */

/**
 * @typedef {object} BrowardMunicipalSupplementalRoute
 * @property {string} purpose - Human-readable scope of the additional official route.
 * @property {string} url - Absolute official city or city-linked vendor route.
 * @property {BrowardMunicipalAccessMode} accessMode - Access condition independently applied to the route.
 * @property {string} note - Completeness or access-control boundary.
 */

/**
 * @typedef {object} BrowardMunicipalJurisdictionConfig
 * @property {string} key - Stable lowercase jurisdiction key.
 * @property {string} jurisdiction - Issuing municipality.
 * @property {string} sourceSystem - Stable normalized source key.
 * @property {BrowardMunicipalProtocol} protocol - Reusable vendor protocol family.
 * @property {string} searchUrl - Official city or city-linked public portal route.
 * @property {string} officialEvidenceUrl - First-party page or portal proving custody and access.
 * @property {BrowardMunicipalAccessMode} accessMode - Access-control disposition.
 * @property {BrowardMunicipalProbeStatus} probeStatus - Whether bounded unattended search is implemented and currently allowed.
 * @property {string} accessNote - Explicit explanation of the source boundary or skip.
 * @property {BrowardMunicipalCapability} capabilities - Documented source capabilities.
 * @property {readonly BrowardMunicipalSupplementalRoute[]} supplementalRoutes - Current/legacy or records-request routes needed for completeness.
 */

/**
 * @typedef {object} BrowardMunicipalInspection
 * @property {string | null} source_id - Source inspection identifier when exposed.
 * @property {string} inspection_type - Public inspection type.
 * @property {string | null} scheduled_date - ISO calendar date when exposed.
 * @property {string | null} completed_date - ISO calendar date when exposed.
 * @property {string | null} status - Public inspection status.
 * @property {string | null} result - Public pass/fail or outcome text.
 */

/**
 * @typedef {object} NormalizedBrowardMunicipalPermit
 * @property {string} source_system - Jurisdiction-level source key.
 * @property {BrowardMunicipalProtocol} source_protocol - Reusable vendor protocol.
 * @property {string} source_url - Canonical official record/detail URL with session tokens removed.
 * @property {string} source_search_url - Official search route used to discover the record.
 * @property {string} source_record_id - Stable vendor record identifier.
 * @property {string} record_key - Stable source-system and record-id identity.
 * @property {string} jurisdiction - Issuing municipality.
 * @property {string} permit_number - Public permit or application number.
 * @property {string | null} parcel_identifier - Source parcel/folio display preserved as text, including letters and punctuation.
 * @property {string | null} query_folio - Exact canonical Broward folio submitted for a folio lookup.
 * @property {string | null} work_location - Public project location retained only for private staging.
 * @property {string | null} application_date - ISO application date.
 * @property {string | null} permit_issue_date - ISO issue date.
 * @property {string | null} expiration_date - ISO expiration date.
 * @property {string | null} record_status - Public permit/application status.
 * @property {string | null} record_type - Public permit/application type.
 * @property {string | null} project_description - Public work description retained only for private staging.
 * @property {number | null} job_value - Public valuation when exposed.
 * @property {readonly BrowardMunicipalInspection[]} inspections - Bounded public inspection summary rows.
 * @property {boolean} is_roof_permit - Conservative source-text classification.
 * @property {Readonly<Record<string, string | number | boolean | null>>} raw - Allow-listed non-contact provenance.
 */

/**
 * @typedef {object} BrowardMunicipalSearchReference
 * @property {string} sourceRecordId - Stable source identity used for dedupe and checkpoints.
 * @property {string} permitNumber - Public permit/application number.
 * @property {string} detailUrl - Canonical official detail route.
 * @property {number} sourcePage - One-based source page.
 * @property {Readonly<Record<string, string | number | boolean | null>>} listData - Allow-listed list-row evidence.
 */

/**
 * @typedef {object} BrowardMunicipalSearchPage
 * @property {readonly BrowardMunicipalSearchReference[]} references - Source-order record references.
 * @property {number | string | null} nextPage - Next one-based page/cursor, or null when source pagination is complete.
 */

/**
 * @typedef {object} BrowardMunicipalProbeLimits
 * @property {number} maxQueries - Maximum exact source lookups, never above three.
 * @property {number} maxSearchPages - Maximum total source result pages, never above six.
 * @property {number} maxResults - Maximum unique source references, never above fifty.
 * @property {number} maxDetailPages - Maximum detail requests, never above ten.
 * @property {number} delayMs - Delay between source requests, at least one second.
 */

/**
 * @typedef {object} BrowardMunicipalCheckpoint
 * @property {1} version - Checkpoint schema version.
 * @property {string} jurisdictionKey - Configuration identity.
 * @property {string} queryDigest - SHA-256 digest of normalized queries; raw addresses are not persisted.
 * @property {number} nextQueryIndex - Zero-based query to resume.
 * @property {number | string} nextPage - One-based page or opaque public cursor to resume.
 * @property {readonly string[]} seenReferenceKeys - Stable references observed across result pages.
 * @property {readonly string[]} capturedRecordKeys - Stable records whose details were normalized.
 * @property {boolean} completed - Whether every bounded query completed.
 */

/**
 * @typedef {object} BrowardMunicipalAccessDecision
 * @property {"probe" | "skip"} action - Whether network search/detail work is allowed.
 * @property {"anonymous_certified" | "login_required" | "captcha_required" | "no_anonymous_search" | "records_request" | "landing_only" | "blocked"} reason - Machine-readable access disposition.
 * @property {string} note - Operator-facing explanation.
 */

/**
 * @typedef {object} BrowardMunicipalProbeResult
 * @property {"completed" | "skipped"} status - Explicit bounded-probe outcome.
 * @property {readonly NormalizedBrowardMunicipalPermit[]} records - Deterministic normalized private-staging records.
 * @property {BrowardMunicipalCheckpoint | null} checkpoint - Final checkpoint for a completed probe.
 * @property {BrowardMunicipalAccessDecision} access - Access decision evaluated before any transport call.
 * @property {number} searchPageCount - Result pages fetched in this invocation.
 * @property {number} detailPageCount - Detail pages fetched in this invocation.
 */

const MAX_QUERIES = 3;
const MAX_SEARCH_PAGES = 6;
const MAX_RESULTS = 50;
const MAX_DETAIL_PAGES = 10;
const MIN_DELAY_MS = 1_000;

/**
 * Collapse source whitespace without changing letters, punctuation, or leading
 * zeroes. Source parcel identifiers must never be converted to numbers.
 *
 * @param {unknown} value - Candidate source parcel/folio display.
 * @returns {string | null} Preserved non-empty source text.
 */
export function preserveMunicipalParcelIdentifier(value) {
  if (typeof value !== "string") return null;
  const preserved = value.replace(/\s+/gu, " ").trim();
  return preserved.length === 0 ? null : preserved;
}

/**
 * Normalize one BCPA folio for a municipal folio search.
 *
 * Surrounding whitespace and display dashes/spaces are removed and letters are
 * uppercased. The function deliberately rejects numbers, padding, truncation,
 * and every non-alphanumeric character so condo folios such as
 * `504108BJ0140` survive end to end.
 *
 * @param {unknown} value - Candidate 12-character Broward folio.
 * @returns {string} Canonical alphanumeric folio.
 */
export function normalizeMunicipalFolio(value) {
  if (typeof value !== "string") {
    throw new Error("Broward municipal folio must be supplied as a string");
  }
  const normalized = value.trim().replace(/[-\s]/gu, "").toUpperCase();
  if (!/^[A-Z0-9]{12}$/u.test(normalized)) {
    throw new Error(
      "Broward municipal folio must contain exactly 12 alphanumeric characters",
    );
  }
  return normalized;
}

/**
 * Normalize and bound exact pilot queries before any source request.
 *
 * @param {readonly BrowardMunicipalQuery[]} queries - Candidate source queries.
 * @param {number} [maxQueries=3] - Operator ceiling, never above three.
 * @returns {readonly BrowardMunicipalQuery[]} Unique normalized queries.
 */
export function validateMunicipalQueries(queries, maxQueries = MAX_QUERIES) {
  if (
    !Number.isInteger(maxQueries) ||
    maxQueries <= 0 ||
    maxQueries > MAX_QUERIES
  ) {
    throw new Error(
      `Broward municipal maxQueries must be from 1 through ${String(MAX_QUERIES)}`,
    );
  }
  if (queries.length === 0 || queries.length > maxQueries) {
    throw new Error(
      `Broward municipal probe requires 1 through ${String(maxQueries)} queries`,
    );
  }

  /** @type {BrowardMunicipalQuery[]} */
  const normalized = [];
  const identities = new Set();
  for (const query of queries) {
    if (!["permit_number", "address", "folio"].includes(query.kind)) {
      throw new Error(
        `Unsupported Broward municipal query kind: ${query.kind}`,
      );
    }
    if (typeof query.value !== "string" || query.value.trim().length === 0) {
      throw new Error("Broward municipal query value must be non-empty text");
    }
    const value =
      query.kind === "folio"
        ? normalizeMunicipalFolio(query.value)
        : query.value.replace(/\s+/gu, " ").trim();
    const identity = `${query.kind}\u0000${value.toUpperCase()}`;
    if (identities.has(identity)) {
      throw new Error("Broward municipal queries must be unique");
    }
    identities.add(identity);
    normalized.push({ kind: query.kind, value });
  }
  return normalized;
}

/**
 * Validate conservative process-wide request ceilings.
 *
 * @param {Partial<BrowardMunicipalProbeLimits>} [limits={}] - Optional stricter limits.
 * @returns {BrowardMunicipalProbeLimits} Complete validated limits.
 */
export function validateMunicipalProbeLimits(limits = {}) {
  const complete = {
    maxQueries: limits.maxQueries ?? MAX_QUERIES,
    maxSearchPages: limits.maxSearchPages ?? 3,
    maxResults: limits.maxResults ?? 25,
    maxDetailPages: limits.maxDetailPages ?? 5,
    delayMs: limits.delayMs ?? 1_250,
  };
  const boundedIntegers = [
    ["maxQueries", complete.maxQueries, MAX_QUERIES],
    ["maxSearchPages", complete.maxSearchPages, MAX_SEARCH_PAGES],
    ["maxResults", complete.maxResults, MAX_RESULTS],
    ["maxDetailPages", complete.maxDetailPages, MAX_DETAIL_PAGES],
  ];
  for (const [name, value, maximum] of boundedIntegers) {
    if (
      typeof value !== "number" ||
      typeof maximum !== "number" ||
      !Number.isInteger(value) ||
      value <= 0 ||
      value > maximum
    ) {
      throw new Error(
        `Broward municipal ${String(name)} must be from 1 through ${String(maximum)}`,
      );
    }
  }
  if (!Number.isInteger(complete.delayMs) || complete.delayMs < MIN_DELAY_MS) {
    throw new Error(
      `Broward municipal delayMs must be at least ${String(MIN_DELAY_MS)}`,
    );
  }
  return complete;
}

/**
 * Explain whether a jurisdiction may run an unattended bounded probe.
 *
 * This decision is evaluated before transport creation so login, CAPTCHA, and
 * records-request sources cannot accidentally receive a search request.
 *
 * @param {BrowardMunicipalJurisdictionConfig} config - Jurisdiction source configuration.
 * @returns {BrowardMunicipalAccessDecision} Explicit probe or skip decision.
 */
export function decideMunicipalSourceAccess(config) {
  if (config.accessMode === "login_required") {
    return {
      action: "skip",
      reason: "login_required",
      note: config.accessNote,
    };
  }
  if (config.accessMode === "captcha_required") {
    return {
      action: "skip",
      reason: "captcha_required",
      note: config.accessNote,
    };
  }
  if (config.accessMode === "no_anonymous_search") {
    return {
      action: "skip",
      reason: "no_anonymous_search",
      note: config.accessNote,
    };
  }
  if (config.accessMode === "records_request") {
    return {
      action: "skip",
      reason: "records_request",
      note: config.accessNote,
    };
  }
  if (config.probeStatus === "landing_only") {
    return { action: "skip", reason: "landing_only", note: config.accessNote };
  }
  if (config.probeStatus === "blocked") {
    return { action: "skip", reason: "blocked", note: config.accessNote };
  }
  return {
    action: "probe",
    reason: "anonymous_certified",
    note: config.accessNote,
  };
}

/**
 * Hash normalized queries without writing raw addresses into checkpoints.
 *
 * @param {readonly BrowardMunicipalQuery[]} queries - Normalized queries.
 * @returns {string} Lowercase SHA-256 digest.
 */
function digestQueries(queries) {
  return createHash("sha256").update(JSON.stringify(queries)).digest("hex");
}

/**
 * Create an empty resumable checkpoint for one exact query set.
 *
 * @param {BrowardMunicipalJurisdictionConfig} config - Jurisdiction configuration.
 * @param {readonly BrowardMunicipalQuery[]} queries - Already normalized queries.
 * @returns {BrowardMunicipalCheckpoint} Initial page-one checkpoint.
 */
export function createMunicipalCheckpoint(config, queries) {
  return {
    version: 1,
    jurisdictionKey: config.key,
    queryDigest: digestQueries(queries),
    nextQueryIndex: 0,
    nextPage: 1,
    seenReferenceKeys: [],
    capturedRecordKeys: [],
    completed: false,
  };
}

/**
 * Validate a deserialized checkpoint against the current jurisdiction/query set.
 *
 * @param {unknown} value - Parsed checkpoint JSON.
 * @param {BrowardMunicipalJurisdictionConfig} config - Current configuration.
 * @param {readonly BrowardMunicipalQuery[]} queries - Current normalized queries.
 * @returns {BrowardMunicipalCheckpoint} Safe immutable checkpoint value.
 */
export function validateMunicipalCheckpoint(value, config, queries) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Broward municipal checkpoint must be an object");
  }
  const candidate = /** @type {Record<string, unknown>} */ (value);
  const seen = candidate.seenReferenceKeys;
  const captured = candidate.capturedRecordKeys;
  if (
    candidate.version !== 1 ||
    candidate.jurisdictionKey !== config.key ||
    candidate.queryDigest !== digestQueries(queries) ||
    !Number.isInteger(candidate.nextQueryIndex) ||
    /** @type {number} */ (candidate.nextQueryIndex) < 0 ||
    /** @type {number} */ (candidate.nextQueryIndex) > queries.length ||
    !(
      (Number.isInteger(candidate.nextPage) &&
        /** @type {number} */ (candidate.nextPage) > 0) ||
      (typeof candidate.nextPage === "string" &&
        candidate.nextPage.length > 0 &&
        candidate.nextPage.length <= 2_048)
    ) ||
    !Array.isArray(seen) ||
    !seen.every((item) => typeof item === "string" && item.length > 0) ||
    new Set(seen).size !== seen.length ||
    !Array.isArray(captured) ||
    !captured.every((item) => typeof item === "string" && item.length > 0) ||
    new Set(captured).size !== captured.length ||
    typeof candidate.completed !== "boolean"
  ) {
    throw new Error("Broward municipal checkpoint is malformed or mismatched");
  }
  if (
    candidate.completed &&
    /** @type {number} */ (candidate.nextQueryIndex) !== queries.length
  ) {
    throw new Error(
      "Completed Broward municipal checkpoint has pending queries",
    );
  }
  return {
    version: 1,
    jurisdictionKey: config.key,
    queryDigest: /** @type {string} */ (candidate.queryDigest),
    nextQueryIndex: /** @type {number} */ (candidate.nextQueryIndex),
    nextPage: /** @type {number | string} */ (candidate.nextPage),
    seenReferenceKeys: Object.freeze([...seen]),
    capturedRecordKeys: Object.freeze([...captured]),
    completed: candidate.completed,
  };
}

/**
 * Produce a stable source identity for one parsed search reference.
 *
 * @param {BrowardMunicipalJurisdictionConfig} config - Jurisdiction configuration.
 * @param {BrowardMunicipalSearchReference} reference - Parsed source reference.
 * @returns {string} Stable checkpoint key.
 */
function referenceKey(config, reference) {
  return `${config.sourceSystem}:${reference.sourceRecordId}`;
}

/**
 * Deduplicate normalized records and fail closed on conflicting variants.
 *
 * @param {readonly NormalizedBrowardMunicipalPermit[]} records - Candidate records.
 * @returns {readonly NormalizedBrowardMunicipalPermit[]} Deterministic unique records.
 */
export function dedupeAndSortMunicipalPermits(records) {
  /** @type {Map<string, NormalizedBrowardMunicipalPermit>} */
  const byKey = new Map();
  for (const record of records) {
    const existing = byKey.get(record.record_key);
    if (
      existing !== undefined &&
      JSON.stringify(existing) !== JSON.stringify(record)
    ) {
      throw new Error(
        `Conflicting Broward municipal records for ${record.record_key}`,
      );
    }
    byKey.set(record.record_key, record);
  }
  return [...byKey.values()].sort(
    (left, right) =>
      left.source_system.localeCompare(right.source_system) ||
      left.permit_number.localeCompare(right.permit_number) ||
      left.source_record_id.localeCompare(right.source_record_id),
  );
}

/**
 * Render deterministic private-staging permit JSONL.
 *
 * @param {readonly NormalizedBrowardMunicipalPermit[]} records - Candidate records.
 * @returns {string} Newline-delimited JSON with a trailing newline when non-empty.
 */
export function renderMunicipalPermitJsonl(records) {
  const stable = dedupeAndSortMunicipalPermits(records);
  return stable.length === 0
    ? ""
    : `${stable.map((record) => JSON.stringify(record)).join("\n")}\n`;
}

/**
 * Run reusable bounded pagination, detail capture, dedupe, and checkpoint logic.
 *
 * Protocol modules own source-specific HTTP/session handling and parsing. This
 * runner owns every cross-vendor safety invariant: access skips happen before
 * transport calls; requests are serialized; result/detail/page ceilings are
 * hard failures; page progress is checkpointed only after that page's details
 * finish; and rerunning a partially completed page skips already captured
 * record identities.
 *
 * @param {object} params - Bounded capture dependencies.
 * @param {BrowardMunicipalJurisdictionConfig} params.config - Jurisdiction configuration.
 * @param {readonly BrowardMunicipalQuery[]} params.queries - Exact bounded source queries.
 * @param {Partial<BrowardMunicipalProbeLimits>} [params.limits] - Optional stricter process limits.
 * @param {unknown} [params.checkpoint] - Optional parsed checkpoint to resume.
 * @param {(query: BrowardMunicipalQuery, page: number | string) => Promise<BrowardMunicipalSearchPage>} params.fetchSearchPage - Protocol search/page transport and parser.
 * @param {(reference: BrowardMunicipalSearchReference, query: BrowardMunicipalQuery) => Promise<NormalizedBrowardMunicipalPermit>} params.fetchDetail - Protocol detail transport and parser.
 * @param {(record: NormalizedBrowardMunicipalPermit) => Promise<void>} [params.onRecord] - Optional idempotent private record sink called before its checkpoint identity advances.
 * @param {(checkpoint: BrowardMunicipalCheckpoint) => Promise<void>} [params.onCheckpoint] - Optional local checkpoint sink.
 * @param {(milliseconds: number) => Promise<void>} [params.wait] - Injectable serialized delay.
 * @returns {Promise<BrowardMunicipalProbeResult>} Explicit skip or completed result.
 */
export async function runBoundedMunicipalCapture({
  config,
  queries: rawQueries,
  limits: rawLimits = {},
  checkpoint: rawCheckpoint,
  fetchSearchPage,
  fetchDetail,
  onRecord = async () => {},
  onCheckpoint = async () => {},
  wait = (milliseconds) =>
    new Promise((resolve) => {
      setTimeout(resolve, milliseconds);
    }),
}) {
  const access = decideMunicipalSourceAccess(config);
  if (access.action === "skip") {
    return {
      status: "skipped",
      records: [],
      checkpoint: null,
      access,
      searchPageCount: 0,
      detailPageCount: 0,
    };
  }

  const limits = validateMunicipalProbeLimits(rawLimits);
  const queries = validateMunicipalQueries(rawQueries, limits.maxQueries);
  let checkpoint =
    rawCheckpoint === undefined
      ? createMunicipalCheckpoint(config, queries)
      : validateMunicipalCheckpoint(rawCheckpoint, config, queries);
  if (checkpoint.completed) {
    return {
      status: "completed",
      records: [],
      checkpoint,
      access,
      searchPageCount: 0,
      detailPageCount: 0,
    };
  }

  /** @type {NormalizedBrowardMunicipalPermit[]} */
  const records = [];
  let searchPageCount = 0;
  let detailPageCount = 0;
  let requestCount = 0;

  while (checkpoint.nextQueryIndex < queries.length) {
    if (searchPageCount >= limits.maxSearchPages) {
      throw new Error(
        `Broward municipal search-page limit ${String(limits.maxSearchPages)} reached`,
      );
    }
    const query = queries[checkpoint.nextQueryIndex];
    if (query === undefined) {
      throw new Error("Broward municipal checkpoint points beyond query list");
    }
    if (requestCount > 0) await wait(limits.delayMs);
    const pageNumber = checkpoint.nextPage;
    const page = await fetchSearchPage(query, pageNumber);
    requestCount += 1;
    searchPageCount += 1;
    const invalidNumericPage =
      typeof page.nextPage === "number" &&
      (!Number.isInteger(page.nextPage) ||
        page.nextPage <= 0 ||
        (typeof pageNumber === "number" && page.nextPage <= pageNumber));
    const invalidCursor =
      typeof page.nextPage === "string" &&
      (page.nextPage.length === 0 ||
        page.nextPage.length > 2_048 ||
        page.nextPage === pageNumber);
    const invalidPageType =
      page.nextPage !== null &&
      typeof page.nextPage !== "number" &&
      typeof page.nextPage !== "string";
    if (invalidNumericPage || invalidCursor || invalidPageType) {
      throw new Error("Broward municipal source returned invalid pagination");
    }

    const pageKeys = page.references.map((reference) =>
      referenceKey(config, reference),
    );
    const seenReferenceKeys = [
      ...new Set([...checkpoint.seenReferenceKeys, ...pageKeys]),
    ].sort((left, right) => left.localeCompare(right));
    if (seenReferenceKeys.length > limits.maxResults) {
      throw new Error(
        `Broward municipal result limit ${String(limits.maxResults)} exceeded`,
      );
    }

    let capturedRecordKeys = [...checkpoint.capturedRecordKeys];
    const capturedSet = new Set(capturedRecordKeys);
    for (const reference of page.references) {
      const key = referenceKey(config, reference);
      if (capturedSet.has(key)) continue;
      if (capturedSet.size >= limits.maxDetailPages) {
        throw new Error(
          `Broward municipal detail-page limit ${String(limits.maxDetailPages)} exceeded`,
        );
      }
      if (requestCount > 0) await wait(limits.delayMs);
      const record = await fetchDetail(reference, query);
      requestCount += 1;
      detailPageCount += 1;
      if (
        record.source_system !== config.sourceSystem ||
        record.source_protocol !== config.protocol ||
        record.source_record_id !== reference.sourceRecordId ||
        record.permit_number !== reference.permitNumber ||
        record.record_key !== key
      ) {
        throw new Error(
          `Broward municipal detail identity mismatch for ${reference.permitNumber}`,
        );
      }
      await onRecord(record);
      records.push(record);
      capturedSet.add(key);
      capturedRecordKeys = [...capturedSet].sort((left, right) =>
        left.localeCompare(right),
      );
      checkpoint = {
        ...checkpoint,
        seenReferenceKeys,
        capturedRecordKeys,
      };
      await onCheckpoint(checkpoint);
    }

    const nextQueryIndex =
      page.nextPage === null
        ? checkpoint.nextQueryIndex + 1
        : checkpoint.nextQueryIndex;
    checkpoint = {
      ...checkpoint,
      nextQueryIndex,
      nextPage: page.nextPage ?? 1,
      seenReferenceKeys,
      capturedRecordKeys,
      completed: nextQueryIndex === queries.length,
    };
    await onCheckpoint(checkpoint);
  }

  return {
    status: "completed",
    records: dedupeAndSortMunicipalPermits(records),
    checkpoint,
    access,
    searchPageCount,
    detailPageCount,
  };
}
