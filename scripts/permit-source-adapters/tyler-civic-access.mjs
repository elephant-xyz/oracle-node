// @ts-check

import puppeteer from "puppeteer";

import { createBrowardAccelaBrowser } from "./broward-accela.mjs";
import {
  checkpointCapturedPermit,
  checkpointCompletedSearchPage,
  createPermitAdapterCheckpoint,
  normalizePermitSearchQuery,
  readSourceNumber,
  readSourceText,
  validateOfficialHttpsUrl,
  waitForPermitDelay,
} from "./bounded-permit-common.mjs";

/**
 * @typedef {object} TylerCivicAccessConfig
 * @property {string} portalBaseUrl - Civic Access base URL ending at `/apps/selfservice`.
 * @property {string} city - Issuing city written to normalized permit rows.
 * @property {string} sourceSystem - City-level source key ending in `_permits`.
 */

/**
 * @typedef {object} NormalizedCityPermit
 * @property {string} source_system - City-level source key consumed by the normalized-JSONL query loader.
 * @property {string} source_url - Public Civic Access detail URL.
 * @property {string} city - Issuing city.
 * @property {string} permit_number - Public permit number.
 * @property {string | null} parcel_identifier - Parcel identifier returned by Civic Access.
 * @property {string | null} work_location - Public work-location address.
 * @property {string | null} permit_issue_date - ISO calendar date.
 * @property {string | null} record_status - Public permit status.
 * @property {string | null} record_type - Public permit type.
 * @property {string | null} project_description - Public project description; still requires privacy review before publication.
 * @property {boolean} is_roof_permit - Conservative classification based on public type/work-class text.
 * @property {Readonly<Record<string, string | null>>} raw - Allow-listed non-contact source fields retained for audit.
 */

/**
 * @typedef {object} TylerProbeObservation
 * @property {number} lookupIndex - One-based lookup sequence number.
 * @property {number} bootstrapMs - Time from route navigation to the initial rendered document.
 * @property {number} searchMs - Total time from route navigation to the public API response.
 * @property {number} resultCount - Total public entities returned across all Civic Access modules.
 * @property {number} permitCount - Permit entities retained in normalized output.
 * @property {number} httpStatus - Public search API HTTP status.
 */

/**
 * @typedef {object} TylerProbeResult
 * @property {readonly NormalizedCityPermit[]} records - Deduplicated, deterministically sorted normalized permit rows.
 * @property {readonly TylerProbeObservation[]} observations - Per-lookup timing and count evidence.
 */

/**
 * @typedef {object} TylerDateWindowSession
 * @property {import("puppeteer").Browser} browser - Persistent tenant browser.
 * @property {import("puppeteer").Page} page - Bootstrapped same-origin page.
 * @property {TylerCivicAccessConfig} config - Validated tenant configuration.
 * @property {string} endpoint - Tenant search API.
 * @property {Record<string, unknown>} requestTemplate - Complete UI request model.
 * @property {Record<string, string>} tenantHeaders - Tenant headers captured from the UI.
 *
 * @typedef {object} TylerDateWindowPage
 * @property {number} pageNumber - One-based source page.
 * @property {number} totalFound - Source-reported matching permits.
 * @property {number} totalPages - Source-reported total pages.
 * @property {readonly NormalizedCityPermit[]} records - Page-normalized permits.
 * @property {string} rawJson - Exact public response JSON.
 *
 * @typedef {object} TylerDateWindowResult
 * @property {string} startDate - Inclusive ISO application date.
 * @property {string} endDate - Inclusive ISO application date.
 * @property {number} totalFound - Stable source total.
 * @property {number} totalPages - Stable source page count.
 * @property {readonly NormalizedCityPermit[]} records - Deduplicated permit rows.
 * @property {readonly TylerDateWindowPage[]} pages - Raw page responses.
 */

/**
 * @typedef {object} TylerApiResult
 * @property {readonly unknown[]} EntityResults - Public search result entities.
 * @property {number | null} TotalFound - Total matching entities across modules.
 */

const DEFAULT_NAVIGATION_TIMEOUT_MS = 45_000;
const DEFAULT_SEARCH_TIMEOUT_MS = 45_000;
const MIN_DELAY_MS = 1_000;
const MAX_SAFE_LOOKUPS = 10;

/**
 * Normalize the portal base URL once so endpoint and detail URLs remain stable.
 *
 * @param {string} value - Candidate Civic Access base URL.
 * @returns {string} URL without a trailing slash.
 */
function normalizePortalBaseUrl(value) {
  const parsed = new URL(value);
  if (parsed.protocol !== "https:") {
    throw new Error("Tyler Civic Access portalBaseUrl must use HTTPS");
  }
  return parsed.toString().replace(/\/$/, "");
}

/**
 * Validate the immutable jurisdiction configuration used for every normalized row.
 *
 * @param {TylerCivicAccessConfig} config - Candidate source configuration.
 * @returns {TylerCivicAccessConfig} Validated configuration with a normalized URL.
 */
function validateConfig(config) {
  if (typeof config.city !== "string" || config.city.trim().length === 0) {
    throw new Error("Tyler Civic Access city is required");
  }
  if (
    typeof config.sourceSystem !== "string" ||
    !/^[a-z0-9_]+_permits$/.test(config.sourceSystem)
  ) {
    throw new Error(
      "Tyler Civic Access sourceSystem must be a lowercase underscore key ending in _permits",
    );
  }
  return {
    portalBaseUrl: normalizePortalBaseUrl(config.portalBaseUrl),
    city: config.city.trim(),
    sourceSystem: config.sourceSystem,
  };
}

/**
 * Validate conservative pilot query limits before opening a browser.
 *
 * @param {readonly string[]} queries - Public permit numbers or address keywords.
 * @param {number} maxLookups - Operator-approved lookup ceiling.
 * @returns {readonly string[]} Trimmed, non-empty query list.
 */
export function validateProbeQueries(queries, maxLookups = MAX_SAFE_LOOKUPS) {
  if (
    !Number.isInteger(maxLookups) ||
    maxLookups <= 0 ||
    maxLookups > MAX_SAFE_LOOKUPS
  ) {
    throw new Error(
      `maxLookups must be an integer from 1 through ${String(MAX_SAFE_LOOKUPS)}`,
    );
  }
  const normalized = queries
    .map((query) => (typeof query === "string" ? query.trim() : ""))
    .filter((query) => query.length > 0);
  if (normalized.length === 0) {
    throw new Error(
      "At least one non-empty Tyler Civic Access query is required",
    );
  }
  if (normalized.length > maxLookups) {
    throw new Error(
      `Refusing ${String(normalized.length)} Tyler lookups; approved maximum is ${String(maxLookups)}`,
    );
  }
  return normalized;
}

/**
 * Convert an unknown value to a non-empty, collapsed string.
 *
 * @param {unknown} value - Unknown source field.
 * @returns {string | null} Normalized text or `null`.
 */
function readString(value) {
  if (typeof value !== "string") return null;
  const normalized = value.replace(/\s+/g, " ").trim();
  return normalized.length > 0 ? normalized : null;
}

/**
 * Convert an unknown finite number to a number.
 *
 * @param {unknown} value - Unknown source field.
 * @returns {number | null} Finite number or `null`.
 */
function readNumber(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

/**
 * Narrow an unknown value to a plain object record.
 *
 * @param {unknown} value - Unknown source value.
 * @returns {value is Record<string, unknown>} True for non-array object records.
 */
function isRecord(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    Array.isArray(value) === false
  );
}

/**
 * Reduce an ISO timestamp or date string to the query-loader's calendar-date field.
 *
 * @param {unknown} value - Public Civic Access date value.
 * @returns {string | null} `YYYY-MM-DD` or `null`.
 */
function readIsoDate(value) {
  const text = readString(value);
  if (text === null) return null;
  const match = /^(\d{4}-\d{2}-\d{2})/.exec(text);
  return match?.[1] ?? null;
}

/**
 * Read the result block from a Tyler API envelope without trusting its shape.
 *
 * @param {unknown} payload - Parsed public API response.
 * @returns {TylerApiResult} Safe result block.
 */
function readApiResult(payload) {
  if (
    !isRecord(payload) ||
    payload.Success !== true ||
    !isRecord(payload.Result)
  ) {
    const message =
      isRecord(payload) && typeof payload.ErrorMessage === "string"
        ? payload.ErrorMessage
        : "Tyler Civic Access search returned an invalid response";
    throw new Error(message || "Tyler Civic Access search was not successful");
  }
  return {
    EntityResults: Array.isArray(payload.Result.EntityResults)
      ? payload.Result.EntityResults
      : [],
    TotalFound: readNumber(payload.Result.TotalFound),
  };
}

/**
 * Read Tyler's total-page count, accepting zero for an empty result set.
 *
 * @param {unknown} payload - Parsed public search response.
 * @returns {number} Non-negative source page count.
 */
export function readTylerTotalPages(payload) {
  if (
    !isRecord(payload) ||
    payload.Success !== true ||
    !isRecord(payload.Result)
  ) {
    throw new Error("Tyler search response has no pagination result");
  }
  return readNonNegativeInteger(payload.Result.TotalPages, "Tyler TotalPages");
}

/**
 * Determine whether a Civic Access search entity represents a permit.
 *
 * Tyler identifies permits with search module `2`; accepting the literal `"permit"`
 * keeps the normalizer reusable across versions that serialize module labels.
 *
 * @param {Record<string, unknown>} entity - Public search entity.
 * @returns {boolean} True only for permit-module entities.
 */
function isPermitEntity(entity) {
  return (
    entity.ModuleName === 2 ||
    readString(entity.ModuleName)?.toLowerCase() === "permit"
  );
}

/**
 * Build a stable detail URL from a public case identifier.
 *
 * @param {string} portalBaseUrl - Normalized Civic Access base URL.
 * @param {string} caseId - Public permit case identifier.
 * @returns {string} Public detail URL.
 */
function buildPermitDetailUrl(portalBaseUrl, caseId) {
  return `${portalBaseUrl}#/permit/${encodeURIComponent(caseId)}`;
}

/**
 * Build Civic Access's public global-search route for one exact query.
 *
 * Using the portal's own route parameters avoids racing its Angular typeahead while
 * still exercising the same rendered public search and API request as the UI.
 *
 * @param {string} portalBaseUrl - Normalized Civic Access base URL.
 * @param {string} query - Exact public permit number or address keyword.
 * @returns {string} Rendered public search route.
 */
function buildSearchRouteUrl(portalBaseUrl, query) {
  const params = new URLSearchParams({
    m: "1",
    fm: "1",
    ps: "10",
    pn: "1",
    em: "true",
    st: query,
  });
  return `${portalBaseUrl}#/search?${params.toString()}`;
}

/**
 * Normalize one public permit search entity into the existing city-permit JSONL contract.
 *
 * Contact, applicant, assignee, email, business-license, and attachment fields are
 * deliberately excluded. Description and address remain private-ingestion fields until
 * the later publication privacy review approves them.
 *
 * @param {Record<string, unknown>} entity - Public Tyler permit search entity.
 * @param {TylerCivicAccessConfig} config - Validated source configuration.
 * @returns {NormalizedCityPermit | null} Normalized permit or `null` when identity is missing.
 */
function normalizePermitEntity(entity, config) {
  const permitNumber = readString(entity.CaseNumber);
  const caseId = readString(entity.CaseId);
  if (permitNumber === null || caseId === null) return null;

  const recordType = readString(entity.CaseType);
  const workClass = readString(entity.CaseWorkclass);
  const roofText = [recordType, workClass]
    .filter((value) => value !== null)
    .join(" ");

  return {
    source_system: config.sourceSystem,
    source_url: buildPermitDetailUrl(config.portalBaseUrl, caseId),
    city: config.city,
    permit_number: permitNumber,
    parcel_identifier: readString(entity.MainParcel),
    work_location:
      readString(entity.AddressDisplay) ??
      (isRecord(entity.Address)
        ? readString(entity.Address.FullAddress)
        : null),
    permit_issue_date: readIsoDate(entity.IssueDate),
    record_status: readString(entity.CaseStatus),
    record_type: recordType ?? workClass,
    project_description: readString(entity.Description),
    is_roof_permit: /\broof(?:ing)?\b/i.test(roofText),
    raw: {
      case_id: caseId,
      work_class: workClass,
      applied_date: readIsoDate(entity.ApplyDate),
      expiration_date: readIsoDate(entity.ExpireDate),
      finalized_date: readIsoDate(entity.FinalDate),
    },
  };
}

/**
 * Normalize all permit entities in one Tyler public-search response.
 *
 * @param {unknown} payload - Parsed Tyler API response.
 * @param {TylerCivicAccessConfig} rawConfig - Jurisdiction configuration.
 * @returns {readonly NormalizedCityPermit[]} Permit records in source response order.
 */
export function normalizeTylerSearchResponse(payload, rawConfig) {
  const config = validateConfig(rawConfig);
  const result = readApiResult(payload);
  /** @type {NormalizedCityPermit[]} */
  const records = [];
  for (const value of result.EntityResults) {
    if (!isRecord(value) || !isPermitEntity(value)) continue;
    const record = normalizePermitEntity(value, config);
    if (record !== null) records.push(record);
  }
  return records;
}

/**
 * Deduplicate permit records and sort them independently of query or response order.
 *
 * If the same permit appears in multiple lookups, the lexicographically smallest
 * serialized allow-listed record wins so output remains deterministic.
 *
 * @param {readonly NormalizedCityPermit[]} records - Candidate normalized records.
 * @returns {readonly NormalizedCityPermit[]} Stable unique permit rows.
 */
export function dedupeAndSortNormalizedPermits(records) {
  /** @type {Map<string, { record: NormalizedCityPermit, serialized: string }>} */
  const byIdentity = new Map();
  for (const record of records) {
    const identity = `${record.source_system}\u0000${record.permit_number}`;
    const serialized = JSON.stringify(record);
    const existing = byIdentity.get(identity);
    if (existing === undefined || serialized < existing.serialized) {
      byIdentity.set(identity, { record, serialized });
    }
  }
  return [...byIdentity.values()]
    .map((entry) => entry.record)
    .sort(
      (left, right) =>
        left.source_system.localeCompare(right.source_system) ||
        left.permit_number.localeCompare(right.permit_number),
    );
}

/**
 * Render normalized city permits as deterministic newline-delimited JSON.
 *
 * @param {readonly NormalizedCityPermit[]} records - Candidate normalized records.
 * @returns {string} Stable JSONL text with a trailing newline when non-empty.
 */
export function renderNormalizedPermitJsonl(records) {
  const normalized = dedupeAndSortNormalizedPermits(records);
  return normalized.length === 0
    ? ""
    : `${normalized.map((record) => JSON.stringify(record)).join("\n")}\n`;
}

/**
 * Create a persistent anonymous tenant session and capture the UI's complete
 * request model plus tenant headers.
 *
 * @param {TylerCivicAccessConfig} rawConfig - Jurisdiction tenant config.
 * @param {{info:(message:string,details?:Record<string,unknown>)=>void,warn:(message:string,details?:Record<string,unknown>)=>void,error:(message:string,details?:Record<string,unknown>)=>void}} logger
 *   Structured aggregate-safe logger.
 * @returns {Promise<TylerDateWindowSession>} Bootstrapped date-search session.
 */
export async function createTylerDateWindowSession(rawConfig, logger) {
  const config = validateConfig(rawConfig);
  const browser = await createBrowardAccelaBrowser(logger);
  const page = await browser.newPage();
  const endpoint = `${config.portalBaseUrl}/api/energov/search/search`;
  const bootstrapValue = "__cursor_tenant_bootstrap_no_match__";
  const route = `${config.portalBaseUrl}#/search?${new URLSearchParams({
    m: "1",
    fm: "1",
    ps: "10",
    pn: "1",
    em: "true",
    st: bootstrapValue,
  }).toString()}`;
  try {
    const responsePromise = page.waitForResponse(
      (response) =>
        response.url().toLowerCase() === endpoint.toLowerCase() &&
        response.request().method() === "POST",
      { timeout: DEFAULT_SEARCH_TIMEOUT_MS },
    );
    await page.goto(route, {
      waitUntil: "domcontentloaded",
      timeout: DEFAULT_NAVIGATION_TIMEOUT_MS,
    });
    const response = await responsePromise;
    if (!response.ok()) {
      throw new Error(
        `${config.city} Tyler bootstrap returned HTTP ${String(response.status())}`,
      );
    }
    const postData = response.request().postData();
    if (typeof postData !== "string") {
      throw new Error(`${config.city} Tyler bootstrap request body is missing`);
    }
    const requestTemplate = /** @type {unknown} */ (JSON.parse(postData));
    if (
      !isRecord(requestTemplate) ||
      !isRecord(requestTemplate.PermitCriteria)
    ) {
      throw new Error(`${config.city} Tyler bootstrap model is malformed`);
    }
    const observedHeaders = response.request().headers();
    /** @type {Record<string,string>} */
    const tenantHeaders = {};
    for (const name of [
      "tenantid",
      "tenantname",
      "tyler-tenanturl",
      "tyler-tenant-culture",
    ]) {
      const value = observedHeaders[name];
      if (typeof value !== "string" || value.length === 0) {
        throw new Error(
          `${config.city} Tyler bootstrap omitted tenant header ${name}`,
        );
      }
      tenantHeaders[name] = value;
    }
    logger.info("broward_tyler_date_session_ready", {
      city: config.city,
      sourceSystem: config.sourceSystem,
    });
    return {
      browser,
      page,
      config,
      endpoint,
      requestTemplate,
      tenantHeaders,
    };
  } catch (error) {
    await page.close().catch(() => undefined);
    await browser.close().catch(() => undefined);
    throw error;
  }
}

/**
 * Close a persistent Tyler date-window session.
 *
 * @param {TylerDateWindowSession} session - Open tenant session.
 * @returns {Promise<void>} Resolves after browser resources close.
 */
export async function closeTylerDateWindowSession(session) {
  await session.page.close().catch(() => undefined);
  await session.browser.close().catch(() => undefined);
}

/**
 * Build the exact advanced Permit-search request from a captured UI model.
 *
 * @param {Record<string, unknown>} template - Complete tenant UI model.
 * @param {string} startDate - Inclusive ISO application date.
 * @param {string} endDate - Inclusive ISO application date.
 * @param {number} pageNumber - One-based result page.
 * @param {number} pageSize - Result rows per page.
 * @returns {Record<string, unknown>} Complete advanced Permit request.
 */
export function buildTylerDateWindowRequest(
  template,
  startDate,
  endDate,
  pageNumber,
  pageSize,
) {
  const start = requireTylerIsoDate(startDate, "startDate");
  const end = requireTylerIsoDate(endDate, "endDate");
  if (Date.parse(`${end}T00:00:00Z`) < Date.parse(`${start}T00:00:00Z`)) {
    throw new Error("Tyler date-window endDate must not precede startDate");
  }
  if (!Number.isInteger(pageNumber) || pageNumber < 1) {
    throw new Error("Tyler date-window pageNumber must be positive");
  }
  if (![10, 25, 50, 100].includes(pageSize)) {
    throw new Error("Tyler date-window pageSize must be 10, 25, 50, or 100");
  }
  const cloned = /** @type {unknown} */ (
    JSON.parse(JSON.stringify(template))
  );
  if (!isRecord(cloned) || !isRecord(cloned.PermitCriteria)) {
    throw new Error("Tyler date-window request template is malformed");
  }
  cloned.SearchModule = 2;
  cloned.FilterModule = 0;
  cloned.Keyword = "";
  cloned.ExactMatch = true;
  cloned.PageNumber = pageNumber;
  cloned.PageSize = pageSize;
  cloned.SortBy = "PermitNumber.keyword";
  cloned.SortAscending = true;
  Object.assign(cloned.PermitCriteria, {
    PermitNumber: null,
    PermitTypeId: "none",
    PermitWorkclassId: null,
    PermitStatusId: "none",
    ProjectName: null,
    IssueDateFrom: null,
    IssueDateTo: null,
    Address: null,
    Description: null,
    ExpireDateFrom: null,
    ExpireDateTo: null,
    FinalDateFrom: null,
    FinalDateTo: null,
    ApplyDateFrom: `${start}T00:00:00.000Z`,
    ApplyDateTo: `${end}T00:00:00.000Z`,
    SearchMainAddress: false,
    ContactId: null,
    TypeId: null,
    WorkClassIds: null,
    ParcelNumber: null,
    ExcludeCases: null,
    EnableDescriptionSearch: false,
    PageNumber: pageNumber,
    PageSize: pageSize,
    SortBy: "PermitNumber.keyword",
    SortAscending: false,
  });
  return cloned;
}

/**
 * Search one application-date window through the bootstrapped tenant API.
 *
 * @param {TylerDateWindowSession} session - Persistent tenant session.
 * @param {string} startDate - Inclusive ISO application date.
 * @param {string} endDate - Inclusive ISO application date.
 * @param {number} [pageSize=100] - Public UI-supported page size.
 * @param {number} [maxPages=200] - Hard source page ceiling.
 * @param {number} [delayMs=1000] - Delay between API pages.
 * @param {(milliseconds:number)=>Promise<void>} [wait] - Injectable delay.
 * @returns {Promise<TylerDateWindowResult>} Complete page/list result.
 */
export async function searchTylerDateWindow(
  session,
  startDate,
  endDate,
  pageSize = 100,
  maxPages = 200,
  delayMs = 1_000,
  wait = waitForPermitDelay,
) {
  if (!Number.isInteger(maxPages) || maxPages < 1 || maxPages > 200) {
    throw new Error("Tyler date-window maxPages must be 1 through 200");
  }
  if (!Number.isInteger(delayMs) || delayMs < MIN_DELAY_MS) {
    throw new Error(`Tyler date-window delayMs must be at least ${MIN_DELAY_MS}`);
  }
  /** @type {TylerDateWindowPage[]} */
  const pages = [];
  /** @type {NormalizedCityPermit[]} */
  const records = [];
  let expectedTotal = /** @type {number | null} */ (null);
  let expectedPages = /** @type {number | null} */ (null);
  for (let pageNumber = 1; pageNumber <= maxPages; pageNumber += 1) {
    if (pageNumber > 1) await wait(delayMs);
    const requestBody = buildTylerDateWindowRequest(
      session.requestTemplate,
      startDate,
      endDate,
      pageNumber,
      pageSize,
    );
    const response = await session.page.evaluate(
      async (input) => {
        const result = await fetch(input.endpoint, {
          method: "POST",
          headers: {
            Accept: "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=UTF-8",
            ...input.headers,
          },
          credentials: "include",
          body: JSON.stringify(input.body),
        });
        return { status: result.status, text: await result.text() };
      },
      {
        endpoint: session.endpoint,
        headers: session.tenantHeaders,
        body: requestBody,
      },
    );
    if (response.status !== 200) {
      throw new Error(
        `${session.config.city} Tyler date search returned HTTP ${String(response.status)}`,
      );
    }
    const payload = /** @type {unknown} */ (JSON.parse(response.text));
    const result = readApiResult(payload);
    const totalPages = readTylerTotalPages(payload);
    const totalFound = result.TotalFound ?? result.EntityResults.length;
    if (
      (expectedTotal !== null && expectedTotal !== totalFound) ||
      (expectedPages !== null && expectedPages !== totalPages)
    ) {
      throw new Error(
        `${session.config.city} Tyler totals changed during one date window`,
      );
    }
    expectedTotal = totalFound;
    expectedPages = totalPages;
    if (totalPages > maxPages) {
      throw new Error(
        `${session.config.city} Tyler date window exceeds maxPages ${String(maxPages)}`,
      );
    }
    const normalized = normalizeTylerSearchResponse(
      payload,
      session.config,
    );
    pages.push({
      pageNumber,
      totalFound,
      totalPages,
      records: normalized,
      rawJson: response.text,
    });
    records.push(...normalized);
    if (pageNumber >= totalPages) break;
  }
  const totalFound = expectedTotal ?? 0;
  const totalPages = expectedPages ?? 0;
  const deduped = dedupeAndSortNormalizedPermits(records);
  if (deduped.length !== totalFound) {
    throw new Error(
      `${session.config.city} Tyler normalized ${String(deduped.length)} of ${String(totalFound)} date-window permits`,
    );
  }
  return {
    startDate,
    endDate,
    totalFound,
    totalPages,
    records: deduped,
    pages,
  };
}

/**
 * Validate a real ISO calendar date.
 *
 * @param {string} value - Candidate YYYY-MM-DD.
 * @param {string} name - Field name for errors.
 * @returns {string} Validated date.
 */
function requireTylerIsoDate(value, name) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/u.exec(value);
  if (match === null) throw new Error(`Tyler ${name} must be YYYY-MM-DD`);
  const date = new Date(
    Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])),
  );
  if (
    date.getUTCFullYear() !== Number(match[1]) ||
    date.getUTCMonth() !== Number(match[2]) - 1 ||
    date.getUTCDate() !== Number(match[3])
  ) {
    throw new Error(`Tyler ${name} is not a calendar date`);
  }
  return value;
}

/**
 * Resolve a usable local Chrome executable without downloading or repairing browsers.
 *
 * @returns {string | null} Explicit/system Chrome path, or `null` for Puppeteer's default.
 */
function resolveChromeExecutablePath() {
  const configured = process.env.CHROME_EXECUTABLE_PATH?.trim();
  if (configured) return configured;
  try {
    return puppeteer.executablePath("chrome");
  } catch {
    return null;
  }
}

/**
 * Pause between public lookups to keep the pilot single-threaded and low-rate.
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
 * Read the exact keyword Civic Access submitted to its public search API.
 *
 * @param {import("puppeteer").HTTPResponse} response - Captured public search response.
 * @returns {string | null} Submitted keyword, or `null` when the request body is unavailable.
 */
function readSubmittedKeyword(response) {
  const postData = response.request().postData();
  if (typeof postData !== "string" || postData.length === 0) return null;
  try {
    const parsed = /** @type {unknown} */ (JSON.parse(postData));
    return isRecord(parsed) ? readString(parsed.Keyword) : null;
  } catch {
    return null;
  }
}

/**
 * Run conservative anonymous permit-number/address lookups through a rendered
 * Tyler Civic Access session and return only allow-listed normalized records.
 *
 * The browser bootstrap is intentional: a direct API POST without portal tenant
 * initialization returns `Cannot find tenant information`. No CAPTCHA, challenge,
 * login, or security control is bypassed.
 *
 * @param {object} params - Pilot parameters.
 * @param {TylerCivicAccessConfig} params.config - Jurisdiction source configuration.
 * @param {readonly string[]} params.queries - Public permit numbers or address keywords.
 * @param {number} [params.maxLookups=10] - Hard lookup ceiling, never above 10.
 * @param {number} [params.delayMs=1500] - Delay between lookups; minimum 1000 ms.
 * @param {number} [params.navigationTimeoutMs=45000] - Portal bootstrap timeout.
 * @param {number} [params.searchTimeoutMs=45000] - Public search API timeout.
 * @returns {Promise<TylerProbeResult>} Normalized records plus timing/count observations.
 */
export async function probeTylerCivicAccess({
  config: rawConfig,
  queries: rawQueries,
  maxLookups = MAX_SAFE_LOOKUPS,
  delayMs = 1_500,
  navigationTimeoutMs = DEFAULT_NAVIGATION_TIMEOUT_MS,
  searchTimeoutMs = DEFAULT_SEARCH_TIMEOUT_MS,
}) {
  const config = validateConfig(rawConfig);
  const queries = validateProbeQueries(rawQueries, maxLookups);
  if (!Number.isInteger(delayMs) || delayMs < MIN_DELAY_MS) {
    throw new Error(`delayMs must be at least ${String(MIN_DELAY_MS)}`);
  }

  const executablePath = resolveChromeExecutablePath();
  const browser = await puppeteer.launch({
    headless: true,
    ...(executablePath === null ? {} : { executablePath }),
  });
  /** @type {NormalizedCityPermit[]} */
  const records = [];
  /** @type {TylerProbeObservation[]} */
  const observations = [];
  const searchApiUrl = `${config.portalBaseUrl}/api/energov/search/search`;

  try {
    for (const [index, query] of queries.entries()) {
      const page = await browser.newPage();
      try {
        const searchUrl = buildSearchRouteUrl(config.portalBaseUrl, query);
        const bootstrapStarted = Date.now();
        const responsePromise = page.waitForResponse(
          (response) =>
            response.url() === searchApiUrl &&
            response.request().method() === "POST",
          { timeout: searchTimeoutMs },
        );
        await page.goto(searchUrl, {
          waitUntil: "domcontentloaded",
          timeout: navigationTimeoutMs,
        });
        const bootstrapMs = Date.now() - bootstrapStarted;
        const response = await responsePromise;
        const submittedKeyword = readSubmittedKeyword(response);
        if (submittedKeyword !== query) {
          throw new Error(
            `Civic Access submitted an unexpected query value for lookup ${String(index + 1)}`,
          );
        }
        const payload = /** @type {unknown} */ (await response.json());
        const searchMs = Date.now() - bootstrapStarted;
        const result = readApiResult(payload);
        const normalized = normalizeTylerSearchResponse(payload, config);
        records.push(...normalized);
        observations.push({
          lookupIndex: index + 1,
          bootstrapMs,
          searchMs,
          resultCount: result.TotalFound ?? result.EntityResults.length,
          permitCount: normalized.length,
          httpStatus: response.status(),
        });
      } finally {
        await page.close().catch(() => undefined);
      }
      if (index < queries.length - 1) {
        await delay(delayMs);
      }
    }
  } finally {
    await browser.close().catch(() => undefined);
  }

  return {
    records: dedupeAndSortNormalizedPermits(records),
    observations,
  };
}

/**
 * @typedef {import("./bounded-permit-common.mjs").NormalizedMunicipalPermit} NormalizedMunicipalPermit
 * @typedef {import("./bounded-permit-common.mjs").PermitAdapterCheckpoint} PermitAdapterCheckpoint
 * @typedef {import("./bounded-permit-common.mjs").PermitSearchQuery} PermitSearchQuery
 */

/**
 * Broward-compatible configuration for bounded Tyler source traversal.
 *
 * @typedef {TylerCivicAccessConfig & {
 *   officialSourceUrl: string,
 *   anonymousSearchCertified: boolean,
 *   coverageNote: string
 * }} BoundedTylerCivicAccessConfig
 */

/**
 * One public permit entity retained from a Tyler search page.
 *
 * @typedef {object} TylerPermitCandidate
 * @property {string} caseId - Public Civic Access entity identifier.
 * @property {string} permitNumber - Public permit number.
 * @property {Record<string, unknown>} entity - Search result used for reconciliation/fallbacks.
 */

/**
 * Detail-parser context tying a Tyler result to its property-first lookup.
 *
 * @typedef {object} TylerPermitDetailContext
 * @property {BoundedTylerCivicAccessConfig} config - Validated jurisdiction source configuration.
 * @property {PermitSearchQuery} query - Exact submitted property query.
 * @property {number} searchPage - One-based result page.
 * @property {string} searchUrl - Exact public search route.
 * @property {TylerPermitCandidate} candidate - Permit search entity being detailed.
 */

/**
 * Per-page live-source evidence from a bounded Tyler probe.
 *
 * @typedef {object} TylerBoundedPageObservation
 * @property {number} pageNumber - One-based result page.
 * @property {number} httpStatus - Public search API status.
 * @property {number} entityCount - Mixed-module entities on this page.
 * @property {number} permitCandidateCount - Permit entities on this page.
 * @property {number} reportedTotal - Total mixed-module results reported by Tyler.
 * @property {number} reportedTotalPages - Total mixed-module pages reported by Tyler.
 */

/**
 * Result of one bounded Tyler property-first adapter run.
 *
 * @typedef {object} TylerBoundedProbeResult
 * @property {readonly NormalizedMunicipalPermit[]} records - Captured detail-backed records.
 * @property {PermitAdapterCheckpoint} checkpoint - Durable resume state after this run.
 * @property {readonly TylerBoundedPageObservation[]} observations - Search-page evidence.
 * @property {number} reportedTotal - Largest mixed-module total observed.
 * @property {number} reportedTotalPages - Largest page count observed.
 * @property {boolean} paginationTruncated - Whether source pages exceeded the approved ceiling.
 * @property {boolean} detailsTruncated - Whether detail candidates exceeded the approved ceiling.
 */

const MAX_BOUNDED_TYLER_PAGES = 3;
const MAX_BOUNDED_TYLER_DETAILS = 10;
const MIN_BOUNDED_TYLER_DETAIL_DELAY_MS = 250;

/**
 * Normalize a detail-backed public Tyler permit into the municipal contract.
 *
 * Search identity and detail identity must agree. Folio-mode records also must
 * return the exact submitted 12-character parcel identifier; a global-search
 * hit in some unrelated field is not accepted as property provenance.
 * Assignee/contact fields present in Tyler's detail payload are never copied.
 *
 * @param {unknown} payload - Parsed `api/energov/permits/permitdetail` response.
 * @param {TylerPermitDetailContext} context - Search/detail reconciliation context.
 * @returns {NormalizedMunicipalPermit} Closed normalized permit row.
 */
export function normalizeTylerPermitDetailResponse(payload, context) {
  const config = validateBoundedTylerConfig(context.config);
  const query = normalizePermitSearchQuery(context.query);
  if (
    !Number.isInteger(context.searchPage) ||
    context.searchPage < 1 ||
    readSourceText(context.searchUrl) === null
  ) {
    throw new Error("Tyler detail context has invalid search provenance");
  }
  if (
    !isRecord(payload) ||
    payload.Success !== true ||
    !isRecord(payload.Result)
  ) {
    const sourceMessage =
      isRecord(payload) && typeof payload.ErrorMessage === "string"
        ? payload.ErrorMessage
        : null;
    throw new Error(
      sourceMessage ?? "Tyler permit detail returned an invalid response",
    );
  }

  const detail = payload.Result;
  const caseId = readSourceText(context.candidate.caseId);
  const expectedPermitNumber = readSourceText(context.candidate.permitNumber);
  const permitNumber = readSourceText(detail.PermitNumber);
  if (
    caseId === null ||
    expectedPermitNumber === null ||
    permitNumber === null
  ) {
    throw new Error("Tyler permit detail is missing source identity");
  }
  if (permitNumber !== expectedPermitNumber) {
    throw new Error(
      `Tyler detail permit ${permitNumber} differs from search result ${expectedPermitNumber}`,
    );
  }
  const detailPermitId = readSourceText(detail.PermitId);
  if (
    detailPermitId !== null &&
    detailPermitId.toLowerCase() !== caseId.toLowerCase()
  ) {
    throw new Error("Tyler detail entity differs from the search result");
  }

  const searchParcel = readSourceText(context.candidate.entity.MainParcel);
  const detailParcel = readSourceText(detail.MainParcelNumber);
  if (
    searchParcel !== null &&
    detailParcel !== null &&
    searchParcel !== detailParcel
  ) {
    throw new Error("Tyler detail parcel differs from the search result");
  }
  const parcelIdentifier = detailParcel ?? searchParcel;
  if (
    query.kind === "folio" &&
    (parcelIdentifier === null ||
      parcelIdentifier.toUpperCase() !== query.value)
  ) {
    throw new Error(
      `Tyler permit parcel does not match submitted folio ${query.value}`,
    );
  }

  const recordType =
    readSourceText(detail.PermitType) ??
    readSourceText(context.candidate.entity.CaseType);
  const workClass =
    readSourceText(detail.WorkClassName) ??
    readSourceText(context.candidate.entity.CaseWorkclass);
  const detailUrl = buildPermitDetailUrl(config.portalBaseUrl, caseId);
  const workLocation =
    readTylerAddress(detail.MainAddress) ??
    readSourceText(context.candidate.entity.AddressDisplay) ??
    (isRecord(context.candidate.entity.Address)
      ? readTylerAddress(context.candidate.entity.Address)
      : null);

  return {
    source_system: config.sourceSystem,
    source_vendor: "tyler_energov_civic_access",
    source_url: detailUrl,
    source_record_id: caseId,
    record_key: `${config.sourceSystem}:${caseId}`,
    city: config.city,
    permit_number: permitNumber,
    parcel_identifier: parcelIdentifier,
    work_location: workLocation,
    permit_issue_date:
      readIsoDate(detail.IssueDate) ??
      readIsoDate(context.candidate.entity.IssueDate),
    application_date:
      readIsoDate(detail.ApplyDate) ??
      readIsoDate(context.candidate.entity.ApplyDate),
    expiration_date:
      readIsoDate(detail.ExpireDate) ??
      readIsoDate(context.candidate.entity.ExpireDate),
    finalized_date:
      readIsoDate(detail.FinalizeDate) ??
      readIsoDate(context.candidate.entity.FinalDate),
    record_status:
      readSourceText(detail.PermitStatus) ??
      readSourceText(context.candidate.entity.CaseStatus),
    record_type: recordType,
    work_class: workClass,
    project_description:
      readSourceText(detail.Description) ??
      readSourceText(context.candidate.entity.Description),
    square_feet: readSourceNumber(detail.SquareFeet),
    job_value: readSourceNumber(detail.Value),
    is_roof_permit: /\broof(?:ing)?\b/iu.test(
      [recordType, workClass].filter((value) => value !== null).join(" "),
    ),
    provenance: {
      official_source_url: config.officialSourceUrl,
      search_url: context.searchUrl,
      detail_url: detailUrl,
      query_kind: query.kind,
      query_value: query.value,
      search_page: context.searchPage,
    },
    raw: {
      case_id: caseId,
      ivr_number: readSourceText(detail.IVRNumber),
      project_name:
        readSourceText(detail.ProjectName) ??
        readSourceText(context.candidate.entity.ProjectName),
      district_name: readSourceText(detail.DistrictName),
      issued: typeof detail.Issued === "boolean" ? detail.Issued : null,
    },
  };
}

/**
 * Run a checkpointed, low-rate Tyler property-first lookup.
 *
 * The adapter uses the public hash routes exactly as the rendered Civic Access
 * UI does. It validates the submitted keyword/page request, captures at most
 * three ten-result pages and ten permit details, and accepts no credentials.
 * A direct API call is intentionally not used because Tyler requires tenant
 * bootstrap from the public portal session.
 *
 * @param {object} params - Bounded adapter parameters.
 * @param {BoundedTylerCivicAccessConfig} params.config - Jurisdiction configuration.
 * @param {PermitSearchQuery} params.query - Exact folio or situs-address query.
 * @param {number} [params.maxPages=3] - Search-page ceiling, never above three.
 * @param {number} [params.maxDetails=10] - Detail ceiling, never above ten.
 * @param {number} [params.searchDelayMs=1500] - Delay between search pages, minimum 1000 ms.
 * @param {number} [params.detailDelayMs=500] - Delay between detail pages, minimum 250 ms.
 * @param {PermitAdapterCheckpoint} [params.checkpoint] - Optional validated resume state.
 * @param {(checkpoint: PermitAdapterCheckpoint) => Promise<void>} [params.onCheckpoint] - Durable state callback after each detail/page.
 * @param {number} [params.navigationTimeoutMs=45000] - Public route timeout.
 * @param {number} [params.responseTimeoutMs=45000] - Public API response timeout.
 * @param {boolean} [params.roofOnly=false] - Detail only search rows explicitly marked roofing.
 * @returns {Promise<TylerBoundedProbeResult>} Captures, provenance, and resume state.
 */
export async function probeBoundedTylerCivicAccess({
  config: rawConfig,
  query: rawQuery,
  maxPages = MAX_BOUNDED_TYLER_PAGES,
  maxDetails = MAX_BOUNDED_TYLER_DETAILS,
  searchDelayMs = 1_500,
  detailDelayMs = 500,
  checkpoint: suppliedCheckpoint,
  onCheckpoint,
  navigationTimeoutMs = DEFAULT_NAVIGATION_TIMEOUT_MS,
  responseTimeoutMs = DEFAULT_SEARCH_TIMEOUT_MS,
  roofOnly = false,
}) {
  const config = validateBoundedTylerConfig(rawConfig);
  const query = normalizePermitSearchQuery(rawQuery);
  validateBoundedTylerLimits({
    maxPages,
    maxDetails,
    searchDelayMs,
    detailDelayMs,
  });
  let checkpoint =
    suppliedCheckpoint ??
    createPermitAdapterCheckpoint(config.sourceSystem, query);
  assertCheckpointMatches(checkpoint, config.sourceSystem, query);
  if (checkpoint.completedDetailIds.length > maxDetails) {
    throw new Error(
      "Tyler maxDetails is below the number already captured in checkpoint",
    );
  }

  const executablePath = resolveChromeExecutablePath();
  const browser = await puppeteer.launch({
    headless: true,
    ...(executablePath === null ? {} : { executablePath }),
  });
  /** @type {TylerBoundedPageObservation[]} */
  const observations = [];
  let reportedTotal = 0;
  let reportedTotalPages = 0;
  let paginationTruncated = false;
  let detailsTruncated = false;

  try {
    for (let pageNumber = 1; pageNumber <= maxPages; pageNumber += 1) {
      if (pageNumber > 1) await waitForPermitDelay(searchDelayMs);
      const searchUrl = buildPagedSearchRouteUrl(
        config.portalBaseUrl,
        query.value,
        pageNumber,
      );
      const searchPage = await browser.newPage();
      /** @type {unknown} */
      let payload;
      let httpStatus = 0;
      try {
        const searchEndpoint =
          `${config.portalBaseUrl}/api/energov/search/search`.toLowerCase();
        const responsePromise = searchPage.waitForResponse(
          (response) =>
            response.url().toLowerCase() === searchEndpoint &&
            response.request().method() === "POST",
          { timeout: responseTimeoutMs },
        );
        await searchPage.goto(searchUrl, {
          waitUntil: "domcontentloaded",
          timeout: navigationTimeoutMs,
        });
        const response = await responsePromise;
        assertTylerSearchRequest(response, query.value, pageNumber);
        payload = /** @type {unknown} */ (await response.json());
        httpStatus = response.status();
      } finally {
        await searchPage.close().catch(() => undefined);
      }

      const result = readApiResult(payload);
      const totalPages = readTylerTotalPages(payload);
      const totalFound = result.TotalFound ?? result.EntityResults.length;
      reportedTotal = Math.max(reportedTotal, totalFound);
      reportedTotalPages = Math.max(reportedTotalPages, totalPages);
      paginationTruncated ||= totalPages > maxPages;

      const allCandidates = readTylerPermitCandidates(result.EntityResults);
      const candidates = roofOnly
        ? allCandidates.filter(isTylerRoofPermitCandidate)
        : allCandidates;
      observations.push({
        pageNumber,
        httpStatus,
        entityCount: result.EntityResults.length,
        permitCandidateCount: candidates.length,
        reportedTotal: totalFound,
        reportedTotalPages: totalPages,
      });

      if (!checkpoint.completedSearchPages.includes(pageNumber)) {
        let completedWholePage = true;
        for (const candidate of candidates) {
          if (checkpoint.completedDetailIds.includes(candidate.caseId)) {
            continue;
          }
          if (checkpoint.completedDetailIds.length >= maxDetails) {
            detailsTruncated = true;
            completedWholePage = false;
            break;
          }
          if (checkpoint.completedDetailIds.length > 0) {
            await waitForPermitDelay(detailDelayMs);
          }
          const detail = await captureTylerPermitDetail({
            browser,
            config,
            query,
            candidate,
            searchPage: pageNumber,
            searchUrl,
            navigationTimeoutMs,
            responseTimeoutMs,
          });
          checkpoint = checkpointCapturedPermit(checkpoint, detail);
          if (onCheckpoint !== undefined) await onCheckpoint(checkpoint);
        }

        if (completedWholePage) {
          checkpoint = checkpointCompletedSearchPage(checkpoint, pageNumber);
          if (onCheckpoint !== undefined) await onCheckpoint(checkpoint);
        }
      }

      if (
        detailsTruncated ||
        pageNumber >= totalPages ||
        checkpoint.completedDetailIds.length >= maxDetails
      ) {
        if (
          checkpoint.completedDetailIds.length >= maxDetails &&
          (pageNumber < totalPages ||
            !checkpoint.completedSearchPages.includes(pageNumber))
        ) {
          detailsTruncated = true;
        }
        break;
      }
    }
  } finally {
    await browser.close().catch(() => undefined);
  }

  return {
    records: checkpoint.records,
    checkpoint,
    observations,
    reportedTotal,
    reportedTotalPages,
    paginationTruncated,
    detailsTruncated,
  };
}

/**
 * Validate the extended Tyler source configuration and anonymous-access gate.
 *
 * @param {BoundedTylerCivicAccessConfig} rawConfig - Candidate source configuration.
 * @returns {BoundedTylerCivicAccessConfig} Normalized configuration.
 */
function validateBoundedTylerConfig(rawConfig) {
  const base = validateConfig(rawConfig);
  if (rawConfig.anonymousSearchCertified !== true) {
    throw new Error(
      `${base.city} anonymous Tyler record search is not certified; login will not be attempted`,
    );
  }
  const officialSourceUrl = validateOfficialHttpsUrl(
    rawConfig.officialSourceUrl,
    "Tyler officialSourceUrl",
  );
  const coverageNote = readSourceText(rawConfig.coverageNote);
  if (coverageNote === null) {
    throw new Error("Tyler coverageNote is required");
  }
  return {
    ...base,
    officialSourceUrl,
    anonymousSearchCertified: true,
    coverageNote,
  };
}

/**
 * Validate hard request ceilings before launching Chrome.
 *
 * @param {object} value - Candidate limits.
 * @param {number} value.maxPages - Search page ceiling.
 * @param {number} value.maxDetails - Detail ceiling.
 * @param {number} value.searchDelayMs - Inter-page delay.
 * @param {number} value.detailDelayMs - Inter-detail delay.
 * @returns {void}
 */
function validateBoundedTylerLimits({
  maxPages,
  maxDetails,
  searchDelayMs,
  detailDelayMs,
}) {
  if (
    !Number.isInteger(maxPages) ||
    maxPages < 1 ||
    maxPages > MAX_BOUNDED_TYLER_PAGES
  ) {
    throw new Error(
      `Tyler maxPages must be an integer from 1 through ${String(MAX_BOUNDED_TYLER_PAGES)}`,
    );
  }
  if (
    !Number.isInteger(maxDetails) ||
    maxDetails < 1 ||
    maxDetails > MAX_BOUNDED_TYLER_DETAILS
  ) {
    throw new Error(
      `Tyler maxDetails must be an integer from 1 through ${String(MAX_BOUNDED_TYLER_DETAILS)}`,
    );
  }
  if (!Number.isInteger(searchDelayMs) || searchDelayMs < MIN_DELAY_MS) {
    throw new Error(
      `Tyler searchDelayMs must be at least ${String(MIN_DELAY_MS)}`,
    );
  }
  if (
    !Number.isInteger(detailDelayMs) ||
    detailDelayMs < MIN_BOUNDED_TYLER_DETAIL_DELAY_MS
  ) {
    throw new Error(
      `Tyler detailDelayMs must be at least ${String(MIN_BOUNDED_TYLER_DETAIL_DELAY_MS)}`,
    );
  }
}

/**
 * Assert that supplied resume state is bound to the active query.
 *
 * @param {PermitAdapterCheckpoint} checkpoint - Candidate state.
 * @param {string} sourceSystem - Active source key.
 * @param {PermitSearchQuery} query - Active query.
 * @returns {void}
 */
function assertCheckpointMatches(checkpoint, sourceSystem, query) {
  if (
    checkpoint.sourceSystem !== sourceSystem ||
    checkpoint.query.kind !== query.kind ||
    checkpoint.query.value !== query.value
  ) {
    throw new Error("Tyler checkpoint does not match source/query");
  }
}

/**
 * Build the rendered public route for one exact page.
 *
 * @param {string} portalBaseUrl - Validated portal base.
 * @param {string} query - Exact property query.
 * @param {number} pageNumber - One-based page.
 * @returns {string} Public hash route.
 */
function buildPagedSearchRouteUrl(portalBaseUrl, query, pageNumber) {
  const params = new URLSearchParams({
    m: "1",
    fm: "1",
    ps: "10",
    pn: String(pageNumber),
    em: "true",
    st: query,
  });
  return `${portalBaseUrl}#/search?${params.toString()}`;
}

/**
 * Verify the rendered Tyler UI submitted the intended keyword and page.
 *
 * @param {import("puppeteer").HTTPResponse} response - Captured search API response.
 * @param {string} expectedKeyword - Exact property query.
 * @param {number} expectedPage - One-based requested page.
 * @returns {void}
 */
function assertTylerSearchRequest(response, expectedKeyword, expectedPage) {
  const postData = response.request().postData();
  if (typeof postData !== "string") {
    throw new Error("Tyler search request body is unavailable");
  }
  const parsed = /** @type {unknown} */ (JSON.parse(postData));
  if (
    !isRecord(parsed) ||
    readSourceText(parsed.Keyword) !== expectedKeyword ||
    parsed.ExactMatch !== true ||
    parsed.PageNumber !== expectedPage ||
    parsed.PageSize !== 10
  ) {
    throw new Error("Tyler UI submitted unexpected search parameters");
  }
}

/**
 * Retain only permit entities with complete public identity.
 *
 * @param {readonly unknown[]} entities - Mixed-module search entities.
 * @returns {readonly TylerPermitCandidate[]} Permit candidates.
 */
function readTylerPermitCandidates(entities) {
  /** @type {TylerPermitCandidate[]} */
  const candidates = [];
  for (const value of entities) {
    if (!isRecord(value) || !isPermitEntity(value)) continue;
    const caseId = readSourceText(value.CaseId);
    const permitNumber = readSourceText(value.CaseNumber);
    if (caseId === null || permitNumber === null) {
      throw new Error("Tyler permit search result is missing identity");
    }
    candidates.push({ caseId, permitNumber, entity: value });
  }
  return candidates;
}

/**
 * Classify a Tyler search candidate before opening its detail API route.
 *
 * @param {TylerPermitCandidate} candidate - Public Tyler search entity.
 * @returns {boolean} True only when type, work class, or description says roofing.
 */
export function isTylerRoofPermitCandidate(candidate) {
  return /\broof(?:ing)?\b/iu.test(
    [
      candidate.entity.CaseType,
      candidate.entity.CaseWorkclass,
      candidate.entity.Description,
      candidate.entity.ProjectName,
    ]
      .filter((value) => typeof value === "string")
      .join(" "),
  );
}

/**
 * Capture one public detail through the initialized Tyler browser session.
 *
 * @param {object} params - Detail request parameters.
 * @param {import("puppeteer").Browser} params.browser - Initialized browser.
 * @param {BoundedTylerCivicAccessConfig} params.config - Validated source configuration.
 * @param {PermitSearchQuery} params.query - Exact property query.
 * @param {TylerPermitCandidate} params.candidate - Search result identity.
 * @param {number} params.searchPage - One-based source result page.
 * @param {string} params.searchUrl - Exact source search route.
 * @param {number} params.navigationTimeoutMs - Route timeout.
 * @param {number} params.responseTimeoutMs - API response timeout.
 * @returns {Promise<NormalizedMunicipalPermit>} Detail-backed normalized row.
 */
async function captureTylerPermitDetail({
  browser,
  config,
  query,
  candidate,
  searchPage,
  searchUrl,
  navigationTimeoutMs,
  responseTimeoutMs,
}) {
  const page = await browser.newPage();
  try {
    const detailEndpoint =
      `${config.portalBaseUrl}/api/energov/permits/permitdetail`.toLowerCase();
    const responsePromise = page.waitForResponse(
      (response) =>
        response.url().toLowerCase() === detailEndpoint &&
        response.request().method() === "POST",
      { timeout: responseTimeoutMs },
    );
    await page.goto(
      buildPermitDetailUrl(config.portalBaseUrl, candidate.caseId),
      {
        waitUntil: "domcontentloaded",
        timeout: navigationTimeoutMs,
      },
    );
    const response = await responsePromise;
    const body = response.request().postData();
    const parsedBody =
      typeof body === "string"
        ? /** @type {unknown} */ (JSON.parse(body))
        : null;
    if (
      !isRecord(parsedBody) ||
      readSourceText(parsedBody.EntityId)?.toLowerCase() !==
        candidate.caseId.toLowerCase() ||
      parsedBody.ModuleId !== 1
    ) {
      throw new Error("Tyler UI submitted unexpected permit detail identity");
    }
    const payload = /** @type {unknown} */ (await response.json());
    return normalizeTylerPermitDetailResponse(payload, {
      config,
      query,
      searchPage,
      searchUrl,
      candidate,
    });
  } finally {
    await page.close().catch(() => undefined);
  }
}

/**
 * Read a Tyler address serialized either as text or a structured object.
 *
 * @param {unknown} value - Public Tyler address field.
 * @returns {string | null} Display address or `null`.
 */
function readTylerAddress(value) {
  const text = readSourceText(value);
  if (text !== null) return text;
  if (!isRecord(value)) return null;
  return (
    readSourceText(value.FullAddress) ??
    readSourceText(value.AddressDisplay) ??
    readSourceText(value.FormattedAddress)
  );
}

/**
 * Require a non-negative integer from a public pagination envelope.
 *
 * @param {unknown} value - Candidate count.
 * @param {string} fieldName - Source field used in errors.
 * @returns {number} Non-negative integer.
 */
function readNonNegativeInteger(value, fieldName) {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new Error(`${fieldName} must be a non-negative integer`);
  }
  return value;
}
