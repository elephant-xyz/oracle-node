// @ts-check

import puppeteer from "puppeteer";

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
 * @param {number} [pageNumber=1] - 1-based result page (`pn`).
 * @param {number} [pageSize=10] - Page size (`ps`).
 * @returns {string} Rendered public search route.
 */
export function buildSearchRouteUrl(
  portalBaseUrl,
  query,
  pageNumber = 1,
  pageSize = 10,
) {
  if (!Number.isInteger(pageNumber) || pageNumber < 1) {
    throw new Error("pageNumber must be an integer >= 1");
  }
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 100) {
    throw new Error("pageSize must be an integer from 1 through 100");
  }
  const params = new URLSearchParams({
    m: "1",
    fm: "1",
    ps: String(pageSize),
    pn: String(pageNumber),
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
 * Launch options for headless Chrome on this VM (no-sandbox) and local laptops.
 *
 * @returns {import("puppeteer").LaunchOptions} Puppeteer launch options.
 */
export function createTylerChromeLaunchOptions() {
  const executablePath = resolveChromeExecutablePath();
  return {
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
      "--disable-setuid-sandbox",
    ],
    ...(executablePath === null ? {} : { executablePath }),
  };
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

  const browser = await puppeteer.launch(createTylerChromeLaunchOptions());
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
 * @typedef {object} TylerHarvestPageObservation
 * @property {string} query Keyword submitted on this page.
 * @property {number} pageNumber 1-based Civic Access page.
 * @property {number} resultCount TotalFound (or entity count) reported for the page.
 * @property {number} permitCount Permit entities normalized from this page.
 * @property {number} httpStatus Search API HTTP status.
 * @property {number} searchMs Wall time for this page fetch.
 */

/**
 * @typedef {object} TylerHarvestResult
 * @property {readonly NormalizedCityPermit[]} records Deduplicated permits across all pages.
 * @property {readonly TylerHarvestPageObservation[]} pages Per-page timing and counts.
 */

const MAX_HARVEST_QUERIES = 200;
const MAX_PAGES_PER_QUERY = 50;

/**
 * Paginate Civic Access keyword search for a harvest (not the 10-lookup probe helper).
 *
 * Reuses one browser so tenant bootstrap happens once. Delay between pages is at
 * least {@link MIN_DELAY_MS}. Does not raise portal concurrency — one tab only.
 *
 * @param {object} params Harvest parameters.
 * @param {TylerCivicAccessConfig} params.config Jurisdiction source configuration.
 * @param {readonly string[]} params.queries Address or permit-number keywords.
 * @param {number} [params.delayMs=1500] Delay between page fetches; minimum 1000 ms.
 * @param {number} [params.pageSize=10] Civic Access `ps` page size.
 * @param {number} [params.maxPagesPerQuery=50] Hard page ceiling per keyword.
 * @param {number} [params.navigationTimeoutMs=45000] First-page bootstrap timeout.
 * @param {number} [params.searchTimeoutMs=45000] Public search API timeout.
 * @returns {Promise<TylerHarvestResult>} Normalized records plus per-page observations.
 */
export async function harvestTylerCivicAccessPages({
  config: rawConfig,
  queries: rawQueries,
  delayMs = 1_500,
  pageSize = 10,
  maxPagesPerQuery = MAX_PAGES_PER_QUERY,
  navigationTimeoutMs = DEFAULT_NAVIGATION_TIMEOUT_MS,
  searchTimeoutMs = DEFAULT_SEARCH_TIMEOUT_MS,
}) {
  const config = validateConfig(rawConfig);
  if (!Number.isInteger(delayMs) || delayMs < MIN_DELAY_MS) {
    throw new Error(`delayMs must be at least ${String(MIN_DELAY_MS)}`);
  }
  if (
    !Number.isInteger(maxPagesPerQuery) ||
    maxPagesPerQuery < 1 ||
    maxPagesPerQuery > MAX_PAGES_PER_QUERY
  ) {
    throw new Error(
      `maxPagesPerQuery must be an integer from 1 through ${String(MAX_PAGES_PER_QUERY)}`,
    );
  }
  const queries = rawQueries
    .map((query) => (typeof query === "string" ? query.trim() : ""))
    .filter((query) => query.length > 0);
  if (queries.length === 0) {
    throw new Error("At least one Tyler harvest query is required");
  }
  if (queries.length > MAX_HARVEST_QUERIES) {
    throw new Error(
      `Refusing ${String(queries.length)} Tyler harvest queries; maximum is ${String(MAX_HARVEST_QUERIES)}`,
    );
  }

  const browser = await puppeteer.launch(createTylerChromeLaunchOptions());
  /** @type {NormalizedCityPermit[]} */
  const records = [];
  /** @type {TylerHarvestPageObservation[]} */
  const pages = [];
  const searchApiUrl = `${config.portalBaseUrl}/api/energov/search/search`;

  try {
    const page = await browser.newPage();
    try {
      let firstNavigation = true;
      for (const [queryIndex, query] of queries.entries()) {
        for (let pageNumber = 1; pageNumber <= maxPagesPerQuery; pageNumber += 1) {
          if (firstNavigation === false) {
            await delay(delayMs);
          }
          const searchUrl = buildSearchRouteUrl(
            config.portalBaseUrl,
            query,
            pageNumber,
            pageSize,
          );
          const started = Date.now();
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
          firstNavigation = false;
          const response = await responsePromise;
          const payload = /** @type {unknown} */ (await response.json());
          const result = readApiResult(payload);
          const normalized = normalizeTylerSearchResponse(payload, config);
          records.push(...normalized);
          const totalFound = result.TotalFound ?? result.EntityResults.length;
          pages.push({
            query,
            pageNumber,
            resultCount: totalFound,
            permitCount: normalized.length,
            httpStatus: response.status(),
            searchMs: Date.now() - started,
          });
          const fetchedThrough = pageNumber * pageSize;
          if (normalized.length === 0 || fetchedThrough >= totalFound) {
            break;
          }
        }
        if (queryIndex < queries.length - 1) {
          await delay(delayMs);
        }
      }
    } finally {
      await page.close().catch(() => undefined);
    }
  } finally {
    await browser.close().catch(() => undefined);
  }

  return {
    records: dedupeAndSortNormalizedPermits(records),
    pages,
  };
}
