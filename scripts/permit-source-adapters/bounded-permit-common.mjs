// @ts-check

import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

/**
 * Search modes supported by the municipal permit adapters.
 *
 * `folio` is always the exact, undashed, 12-character Broward Property
 * Appraiser identifier. `address` is a public situs-address query and is never
 * populated from an owner mailing address.
 *
 * @typedef {"folio" | "address"} PermitSearchKind
 */

/**
 * One operator-supplied, property-first search.
 *
 * @typedef {object} PermitSearchQuery
 * @property {PermitSearchKind} kind - Exact folio or situs-address mode.
 * @property {string} value - Validated source-ready query value.
 */

/**
 * Closed normalized schema shared by Tyler Civic Access and Citizenserve.
 *
 * The schema intentionally excludes owners, applicants, contacts, assignees,
 * email addresses, phone numbers, attachments, and payment data. Work
 * locations and descriptions remain local/private fields until a separate
 * publication review approves them.
 *
 * @typedef {object} NormalizedMunicipalPermit
 * @property {string} source_system - Stable jurisdiction-level source key.
 * @property {string} source_vendor - Vendor family that supplied the record.
 * @property {string} source_url - Exact official detail URL.
 * @property {string} source_record_id - Vendor record identifier.
 * @property {string} record_key - Stable source-system plus source-record identity.
 * @property {string} city - Issuing Broward municipality.
 * @property {string} permit_number - Permit number printed by the official source.
 * @property {string | null} parcel_identifier - Exact Broward folio when source-linked.
 * @property {string | null} work_location - Public permit situs address.
 * @property {string | null} permit_issue_date - ISO calendar date.
 * @property {string | null} application_date - ISO calendar date.
 * @property {string | null} expiration_date - ISO calendar date.
 * @property {string | null} finalized_date - ISO calendar date.
 * @property {string | null} record_status - Source permit status.
 * @property {string | null} record_type - Source permit type.
 * @property {string | null} work_class - Source work class or subtype.
 * @property {string | null} project_description - Public description retained only for private staging.
 * @property {number | null} square_feet - Source square footage when finite.
 * @property {number | null} job_value - Source valuation when finite.
 * @property {boolean} is_roof_permit - Conservative type/work-class classification.
 * @property {object} provenance - Property-first lookup and capture lineage.
 * @property {string} provenance.official_source_url - Municipal page documenting the portal.
 * @property {string} provenance.search_url - Official portal search URL used.
 * @property {string} provenance.detail_url - Official permit detail URL captured.
 * @property {PermitSearchKind} provenance.query_kind - Submitted lookup field.
 * @property {string} provenance.query_value - Exact submitted folio or address.
 * @property {number} provenance.search_page - One-based source result page.
 * @property {Readonly<Record<string, string | number | boolean | null>>} raw - Allow-listed vendor audit fields.
 */

/**
 * Durable local resume state for one source and one property query.
 *
 * A page is marked complete only after every in-scope detail candidate on that
 * page has been captured. Individual details are checkpointed first, so a
 * stopped process can safely re-read the page and skip completed identities.
 *
 * @typedef {object} PermitAdapterCheckpoint
 * @property {1} version - Checkpoint schema version.
 * @property {string} sourceSystem - Jurisdiction source key.
 * @property {PermitSearchQuery} query - Exact query bound to this checkpoint.
 * @property {readonly number[]} completedSearchPages - Fully processed one-based pages.
 * @property {readonly string[]} completedDetailIds - Captured vendor record IDs.
 * @property {readonly NormalizedMunicipalPermit[]} records - Captured normalized records.
 */

/**
 * Normalize a property-first permit query without weakening its identity.
 *
 * Folios are uppercased but never dashed, stripped, padded, truncated, or
 * coerced to numbers. Addresses collapse whitespace and reject control
 * characters while permitting validated situs values such as `GRIFFIN ROAD`
 * that do not include a house number.
 *
 * @param {object} value - Candidate query.
 * @param {unknown} value.kind - Candidate query kind.
 * @param {unknown} value.value - Candidate query text.
 * @returns {PermitSearchQuery} Source-ready query.
 */
export function normalizePermitSearchQuery({ kind, value }) {
  if (kind !== "folio" && kind !== "address") {
    throw new Error("Permit query kind must be folio or address");
  }
  if (typeof value !== "string") {
    throw new Error(`Permit ${kind} query must be a string`);
  }

  const normalized = value.replace(/\s+/gu, " ").trim();
  if (kind === "folio") {
    const folio = normalized.toUpperCase();
    if (!/^[A-Z0-9]{12}$/u.test(folio)) {
      throw new Error(
        "Permit folio must be exactly 12 undashed alphanumeric characters",
      );
    }
    return { kind, value: folio };
  }

  if (
    normalized.length < 3 ||
    normalized.length > 160 ||
    /[\u0000-\u001f\u007f]/u.test(normalized)
  ) {
    throw new Error(
      "Permit address must contain 3 through 160 printable characters",
    );
  }
  return { kind, value: normalized };
}

/**
 * Validate one HTTPS URL and optionally require an exact portal host.
 *
 * @param {unknown} value - Candidate URL.
 * @param {string} fieldName - Configuration field used in errors.
 * @param {string | null} [requiredHost=null] - Optional exact lowercase host.
 * @returns {string} Normalized absolute HTTPS URL.
 */
export function validateOfficialHttpsUrl(
  value,
  fieldName,
  requiredHost = null,
) {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  const parsed = new URL(value);
  if (parsed.protocol !== "https:") {
    throw new Error(`${fieldName} must use HTTPS`);
  }
  if (
    requiredHost !== null &&
    parsed.hostname.toLowerCase() !== requiredHost.toLowerCase()
  ) {
    throw new Error(`${fieldName} must use host ${requiredHost}`);
  }
  return parsed.toString();
}

/**
 * Create empty resume state for a validated source/query pair.
 *
 * @param {string} sourceSystem - Jurisdiction-level source key.
 * @param {PermitSearchQuery} query - Validated property-first query.
 * @returns {PermitAdapterCheckpoint} Empty checkpoint.
 */
export function createPermitAdapterCheckpoint(sourceSystem, query) {
  validateSourceSystem(sourceSystem);
  return {
    version: 1,
    sourceSystem,
    query: normalizePermitSearchQuery(query),
    completedSearchPages: [],
    completedDetailIds: [],
    records: [],
  };
}

/**
 * Validate unknown checkpoint JSON against the active source and query.
 *
 * @param {unknown} value - Parsed checkpoint JSON.
 * @param {string} expectedSourceSystem - Active jurisdiction source key.
 * @param {PermitSearchQuery} expectedQuery - Active validated query.
 * @returns {PermitAdapterCheckpoint} Validated checkpoint.
 */
export function validatePermitAdapterCheckpoint(
  value,
  expectedSourceSystem,
  expectedQuery,
) {
  validateSourceSystem(expectedSourceSystem);
  const query = normalizePermitSearchQuery(expectedQuery);
  if (!isRecord(value) || value.version !== 1) {
    throw new Error("Permit checkpoint has an unsupported version");
  }
  if (value.sourceSystem !== expectedSourceSystem) {
    throw new Error("Permit checkpoint source does not match this run");
  }
  if (
    !isRecord(value.query) ||
    value.query.kind !== query.kind ||
    value.query.value !== query.value
  ) {
    throw new Error("Permit checkpoint query does not match this run");
  }
  if (
    !isPositiveIntegerArray(value.completedSearchPages) ||
    !isUniqueStringArray(value.completedDetailIds) ||
    !Array.isArray(value.records)
  ) {
    throw new Error("Permit checkpoint collections are malformed");
  }

  /** @type {NormalizedMunicipalPermit[]} */
  const records = [];
  for (const record of value.records) {
    if (!isNormalizedPermitIdentity(record)) {
      throw new Error("Permit checkpoint contains a malformed record");
    }
    records.push(
      /** @type {NormalizedMunicipalPermit} */ (
        /** @type {unknown} */ (record)
      ),
    );
  }
  const deduped = dedupeAndSortMunicipalPermits(records);
  if (deduped.length !== records.length) {
    throw new Error("Permit checkpoint contains duplicate records");
  }
  if (
    new Set(value.completedSearchPages).size !==
    value.completedSearchPages.length
  ) {
    throw new Error("Permit checkpoint contains duplicate completed pages");
  }

  return {
    version: 1,
    sourceSystem: expectedSourceSystem,
    query,
    completedSearchPages: [...value.completedSearchPages].sort(
      (left, right) => left - right,
    ),
    completedDetailIds: [...value.completedDetailIds].sort(),
    records: deduped,
  };
}

/**
 * Return a new checkpoint with one detail capture durably represented.
 *
 * Exact duplicate records are idempotent. A reused detail identity with
 * different normalized content fails closed instead of silently choosing one.
 *
 * @param {PermitAdapterCheckpoint} checkpoint - Current validated state.
 * @param {NormalizedMunicipalPermit} record - Newly captured normalized record.
 * @returns {PermitAdapterCheckpoint} Updated immutable checkpoint value.
 */
export function checkpointCapturedPermit(checkpoint, record) {
  if (checkpoint.sourceSystem !== record.source_system) {
    throw new Error("Captured permit source does not match checkpoint");
  }
  const records = dedupeAndSortMunicipalPermits([
    ...checkpoint.records,
    record,
  ]);
  const completedDetailIds = [
    ...new Set([...checkpoint.completedDetailIds, record.source_record_id]),
  ].sort();
  return {
    ...checkpoint,
    completedDetailIds,
    records,
  };
}

/**
 * Return a new checkpoint marking a fully traversed result page complete.
 *
 * @param {PermitAdapterCheckpoint} checkpoint - Current validated state.
 * @param {number} pageNumber - One-based result page.
 * @returns {PermitAdapterCheckpoint} Updated immutable checkpoint value.
 */
export function checkpointCompletedSearchPage(checkpoint, pageNumber) {
  if (!Number.isInteger(pageNumber) || pageNumber < 1) {
    throw new Error("Completed permit page must be a positive integer");
  }
  return {
    ...checkpoint,
    completedSearchPages: [
      ...new Set([...checkpoint.completedSearchPages, pageNumber]),
    ].sort((left, right) => left - right),
  };
}

/**
 * Load local checkpoint JSON, returning a new state when the file is absent.
 *
 * Only an `ENOENT` is treated as a fresh run. Invalid, truncated, mismatched,
 * or unreadable checkpoint files fail closed.
 *
 * @param {string} checkpointPath - Local checkpoint file.
 * @param {string} sourceSystem - Active jurisdiction source key.
 * @param {PermitSearchQuery} query - Active validated query.
 * @returns {Promise<PermitAdapterCheckpoint>} Resume state.
 */
export async function loadPermitAdapterCheckpoint(
  checkpointPath,
  sourceSystem,
  query,
) {
  try {
    const text = await readFile(checkpointPath, "utf8");
    const parsed = /** @type {unknown} */ (JSON.parse(text));
    return validatePermitAdapterCheckpoint(parsed, sourceSystem, query);
  } catch (caught) {
    if (isRecord(caught) && "code" in caught && caught.code === "ENOENT") {
      return createPermitAdapterCheckpoint(sourceSystem, query);
    }
    throw caught;
  }
}

/**
 * Atomically persist local checkpoint state with owner-only permissions.
 *
 * @param {string} checkpointPath - Final local checkpoint path.
 * @param {PermitAdapterCheckpoint} checkpoint - State to persist.
 * @returns {Promise<void>} Resolves after the atomic rename.
 */
export async function writePermitAdapterCheckpoint(checkpointPath, checkpoint) {
  const parent = dirname(checkpointPath);
  await mkdir(parent, { recursive: true });
  const temporaryPath = `${checkpointPath}.${String(process.pid)}.tmp`;
  await writeFile(temporaryPath, `${JSON.stringify(checkpoint, null, 2)}\n`, {
    encoding: "utf8",
    mode: 0o600,
  });
  await rename(temporaryPath, checkpointPath);
}

/**
 * Deduplicate exact records and sort output by stable source identity.
 *
 * @param {readonly NormalizedMunicipalPermit[]} records - Candidate records.
 * @returns {readonly NormalizedMunicipalPermit[]} Unique deterministic records.
 */
export function dedupeAndSortMunicipalPermits(records) {
  /** @type {Map<string, { record: NormalizedMunicipalPermit, serialized: string }>} */
  const byKey = new Map();
  for (const record of records) {
    if (!isNormalizedPermitIdentity(record)) {
      throw new Error("Cannot deduplicate a malformed municipal permit");
    }
    const serialized = JSON.stringify(record);
    const existing = byKey.get(record.record_key);
    if (existing !== undefined && existing.serialized !== serialized) {
      throw new Error(
        `Conflicting municipal permit records for ${record.record_key}`,
      );
    }
    byKey.set(record.record_key, { record, serialized });
  }
  return [...byKey.values()]
    .map((entry) => entry.record)
    .sort(
      (left, right) =>
        left.source_system.localeCompare(right.source_system) ||
        left.permit_number.localeCompare(right.permit_number) ||
        left.source_record_id.localeCompare(right.source_record_id),
    );
}

/**
 * Render normalized permits as deterministic newline-delimited JSON.
 *
 * @param {readonly NormalizedMunicipalPermit[]} records - Candidate records.
 * @returns {string} JSONL with a trailing newline when non-empty.
 */
export function renderMunicipalPermitJsonl(records) {
  const normalized = dedupeAndSortMunicipalPermits(records);
  return normalized.length === 0
    ? ""
    : `${normalized.map((record) => JSON.stringify(record)).join("\n")}\n`;
}

/**
 * Narrow an unknown value to a non-array object.
 *
 * @param {unknown} value - Candidate value.
 * @returns {value is Record<string, unknown>} Whether value is an object record.
 */
export function isRecord(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    Array.isArray(value) === false
  );
}

/**
 * Read trimmed, whitespace-collapsed source text.
 *
 * @param {unknown} value - Candidate source field.
 * @returns {string | null} Non-empty text or `null`.
 */
export function readSourceText(value) {
  if (typeof value !== "string") return null;
  const normalized = value.replace(/\s+/gu, " ").trim();
  return normalized.length === 0 ? null : normalized;
}

/**
 * Read a finite source number.
 *
 * @param {unknown} value - Candidate source field.
 * @returns {number | null} Finite number or `null`.
 */
export function readSourceNumber(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

/**
 * Reduce ISO or US source dates to `YYYY-MM-DD`.
 *
 * @param {unknown} value - Candidate source date.
 * @returns {string | null} ISO calendar date or `null`.
 */
export function readSourceDate(value) {
  const text = readSourceText(value);
  if (text === null) return null;
  const iso = /^(\d{4})-(\d{2})-(\d{2})(?:T|$)/u.exec(text);
  if (iso !== null) return `${iso[1]}-${iso[2]}-${iso[3]}`;
  const us = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/u.exec(text);
  if (us === null) {
    throw new Error(`Unsupported permit source date: ${text}`);
  }
  const month = Number(us[1]);
  const day = Number(us[2]);
  const year = Number(us[3]);
  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    throw new Error(`Invalid permit source date: ${text}`);
  }
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

/**
 * Pause between official-source requests.
 *
 * @param {number} milliseconds - Non-negative delay.
 * @returns {Promise<void>} Resolves after the delay.
 */
export function waitForPermitDelay(milliseconds) {
  if (!Number.isInteger(milliseconds) || milliseconds < 0) {
    throw new Error("Permit request delay must be a non-negative integer");
  }
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

/**
 * Validate stable source-system configuration.
 *
 * @param {unknown} value - Candidate source key.
 * @returns {asserts value is string} Returns only for valid source keys.
 */
function validateSourceSystem(value) {
  if (typeof value !== "string" || !/^[a-z0-9_]+_permits$/u.test(value)) {
    throw new Error(
      "Permit sourceSystem must be a lowercase underscore key ending in _permits",
    );
  }
}

/**
 * Test a positive-integer collection without accepting coercion.
 *
 * @param {unknown} value - Candidate array.
 * @returns {value is number[]} Whether every value is a positive integer.
 */
function isPositiveIntegerArray(value) {
  return (
    Array.isArray(value) &&
    value.every((entry) => Number.isInteger(entry) && entry >= 1)
  );
}

/**
 * Test a non-empty unique-string collection.
 *
 * @param {unknown} value - Candidate array.
 * @returns {value is string[]} Whether every value is a unique non-empty string.
 */
function isUniqueStringArray(value) {
  return (
    Array.isArray(value) &&
    value.every(
      (entry) => typeof entry === "string" && entry.trim().length > 0,
    ) &&
    new Set(value).size === value.length
  );
}

/**
 * Validate only the normalized identity fields needed by checkpoint/dedupe.
 *
 * @param {unknown} value - Candidate normalized record.
 * @returns {value is NormalizedMunicipalPermit} Whether required identities exist.
 */
function isNormalizedPermitIdentity(value) {
  return (
    isRecord(value) &&
    typeof value.source_system === "string" &&
    /^[a-z0-9_]+_permits$/u.test(value.source_system) &&
    typeof value.source_record_id === "string" &&
    value.source_record_id.length > 0 &&
    typeof value.record_key === "string" &&
    value.record_key === `${value.source_system}:${value.source_record_id}` &&
    typeof value.permit_number === "string" &&
    value.permit_number.length > 0
  );
}
