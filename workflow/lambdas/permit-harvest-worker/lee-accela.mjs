import { existsSync } from "fs";
import crypto from "crypto";
import * as cheerio from "cheerio";
import puppeteer from "puppeteer";

/**
 * @typedef {object} Logger
 * @property {(message: string, details?: Record<string, unknown>) => void} info - Emit an informational message.
 * @property {(message: string, details?: Record<string, unknown>) => void} warn - Emit a warning message.
 * @property {(message: string, details?: Record<string, unknown>) => void} error - Emit an error message.
 */

/**
 * @typedef {object} PermitLink
 * @property {string} recordNumber - Accela permit record number displayed in the search result table.
 * @property {string} url - Absolute public Accela detail URL.
 * @property {string | null} address - Search-result address column text when available.
 * @property {string | null} description - Search-result description column text when available.
 * @property {string | null} status - Search-result status column text when available.
 * @property {string | null} submittalType - Search-result submittal type column text when available.
 * @property {string | null} relatedRecords - Search-result related-record count when available.
 * @property {string | null} action - Search-result action column text when available.
 * @property {string} sourceWindowKey - Date-window key that discovered this link.
 * @property {number} sourcePage - One-based search-result page number that discovered this link.
 */

/**
 * @typedef {object} SearchPageCapture
 * @property {number} pageNumber - One-based result page number.
 * @property {string} url - Browser URL after the page was loaded.
 * @property {string | null} resultSummary - Text summary such as "1-10 of 100".
 * @property {string} html - Full captured result page HTML.
 */

/**
 * @typedef {object} PermitSearchResult
 * @property {string} windowKey - Stable date-window key.
 * @property {string} startDate - ISO start date.
 * @property {string} endDate - ISO end date.
 * @property {string} portalUrl - Accela portal URL used for the search.
 * @property {PermitLink[]} permits - Deduped permit detail links discovered in this date window.
 * @property {SearchPageCapture[]} pages - Captured result pages.
 * @property {number | null} reportedTotal - Total result count reported by Accela when present.
 * @property {boolean} noResults - True when Accela returned the no-results message.
 * @property {boolean} truncatedForSplit - True when the crawl intentionally stopped after seeing a high total count.
 */

/**
 * @typedef {object} PermitParcelSearchResult
 * @property {string} searchKey - Stable parcel-search key.
 * @property {string} parcelIdentifier - Raw parcel/STRAP value requested by the caller.
 * @property {string} normalizedParcelIdentifier - Accela parcel field value with punctuation removed.
 * @property {string} portalUrl - Accela portal URL used for the search.
 * @property {PermitLink[]} permits - Deduped permit detail links discovered for this parcel.
 * @property {SearchPageCapture[]} pages - Captured result pages.
 * @property {number | null} reportedTotal - Total result count reported by Accela when present.
 * @property {boolean} noResults - True when Accela returned the no-results message.
 */

/**
 * @typedef {object} InspectionRecord
 * @property {string} result - Inspection result such as Pass or Fail.
 * @property {string} inspectionCode - Inspection code.
 * @property {string} inspectionType - Inspection type label.
 * @property {string} inspectionIdentifier - Accela inspection identifier.
 * @property {string} inspectorName - Inspector name.
 * @property {string} resultedDate - Result date as displayed by Accela.
 */

/**
 * @typedef {object} ExtractedLink
 * @property {string} text - Link text.
 * @property {string} url - Absolute link URL.
 * @property {string | null} title - Link title attribute when present.
 */

/**
 * @typedef {object} PermitDetailExtraction
 * @property {string} schemaVersion - Internal extraction schema version.
 * @property {string} source - Source system name.
 * @property {string} retrievedAt - ISO timestamp of extraction.
 * @property {string} sourceUrl - Accela detail URL.
 * @property {string} recordNumber - Permit record number.
 * @property {string | null} recordType - Permit record type.
 * @property {string | null} recordStatus - Permit status.
 * @property {string | null} workLocation - Work location text.
 * @property {string | null} parcelIdentifier - Parcel/STRAP identifier when visible.
 * @property {string | null} applicant - Applicant block text.
 * @property {string | null} licensedProfessional - Licensed professional block text.
 * @property {string | null} projectDescription - Project description.
 * @property {Record<string, string>} moreDetails - Parsed key-value pairs from the More Details section.
 * @property {string | null} moreDetailsRawText - Raw More Details section text.
 * @property {string | null} inspectionsRawText - Raw inspections section text.
 * @property {InspectionRecord[]} completedInspections - Completed inspections parsed from the public page.
 * @property {string | null} processingStatusRawText - Raw processing status section text.
 * @property {ExtractedLink[]} documentLinks - Public document/ePlan links visible on the page.
 * @property {ExtractedLink[]} relatedLinks - Other public links that may help later extraction.
 * @property {string} rawText - Full page body text with whitespace collapsed.
 * @property {string} [detailCaptureStatus] - Optional capture status when the public detail page could not be read normally.
 * @property {string} [detailCaptureError] - Optional Accela/browser error text explaining why only fallback data was preserved.
 * @property {Record<string, string>} [detailCaptureEvidence] - Optional search-result evidence used to build an unavailable-detail fallback.
 */

/** @type {Logger} */
export const consoleLogger = {
  info(message, details = {}) {
    console.log(JSON.stringify({ level: "info", message, ...details }));
  },
  warn(message, details = {}) {
    console.warn(JSON.stringify({ level: "warn", message, ...details }));
  },
  error(message, details = {}) {
    console.error(JSON.stringify({ level: "error", message, ...details }));
  },
};

const DEFAULT_USER_AGENT =
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";

const LEE_PORTAL_URL =
  "https://aca-prod.accela.com/LEECO/Cap/CapHome.aspx?module=Permitting&TabName=Permitting";

const PERMIT_RECORD_NUMBER_PATTERN =
  /\b[A-Z]{2,4}\d{4}-?\d{5}(?:-[A-Z0-9]+)?\b/i;
const PERMIT_RECORD_HEADER_PATTERN =
  /Record\s+([A-Z]{2,4}\d{4}-?\d{5}(?:-[A-Z0-9]+)?)\s*:\s*(.*?)\s+Record Status:/i;

const RECORD_TYPE_BY_PREFIX = new Map([
  ["COM", "Commercial"],
  ["ELE", "Electrical"],
  ["FIR", "Fire"],
  ["FNC", "Fence"],
  ["MEC", "Mechanical"],
  ["OCC", "Occupancy"],
  ["ROF", "Roof"],
  ["SGN", "Sign"],
  ["TMP", "Temporary Use"],
  ["USE", "Use"],
]);

/**
 * Convert an ISO date (`YYYY-MM-DD`) to Accela's expected `MM/DD/YYYY` display format.
 *
 * @param {string} isoDate - Date in ISO calendar format.
 * @returns {string} Date formatted as `MM/DD/YYYY`.
 */
export function toAccelaDate(isoDate) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(isoDate);
  if (!match) {
    throw new Error(`Invalid ISO date: ${isoDate}`);
  }
  const [, year, month, day] = match;
  return `${month}/${day}/${year}`;
}

/**
 * Build a stable key for a date window.
 *
 * @param {string} startDate - ISO start date.
 * @param {string} endDate - ISO end date.
 * @returns {string} Stable key safe for S3 path segments.
 */
export function buildWindowKey(startDate, endDate) {
  return `${startDate.replaceAll("-", "")}_${endDate.replaceAll("-", "")}`;
}

/**
 * Normalize a Lee County parcel/STRAP value for Accela's `Parcel No.` field.
 *
 * Accela explicitly asks for parcel values without dashes, spaces, dots, or
 * other punctuation. The property appraiser stores formatted STRAP values such
 * as `08-46-25-57-00000.0130`, while existing query-db rows store normalized
 * digits such as `08462557000000130`; this helper accepts either shape.
 *
 * @param {unknown} value - Raw parcel/STRAP value from a manifest, DB row, or appraisal page.
 * @returns {string | null} Uppercase alphanumeric parcel-search value, or `null` when unusable.
 */
export function normalizeParcelSearchValue(value) {
  const normalized = String(value ?? "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "");
  return normalized.length > 0 ? normalized : null;
}

/**
 * Convert arbitrary text into a stable S3-safe path segment.
 *
 * @param {string} value - Raw value to normalize.
 * @returns {string} Lowercase S3-safe segment.
 */
export function safeKeyPart(value) {
  const normalized = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return normalized || "unknown";
}

/**
 * Return a short hash for a string.
 *
 * @param {string} value - Value to hash.
 * @returns {string} First 12 hexadecimal SHA-256 characters.
 */
export function shortHash(value) {
  return crypto.createHash("sha256").update(value).digest("hex").slice(0, 12);
}

/**
 * Collapse whitespace and decode the most common HTML entities left after text extraction.
 *
 * @param {unknown} value - Value to normalize.
 * @returns {string} Normalized text.
 */
export function collapseText(value) {
  return String(value ?? "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Extract text from a fragment of HTML.
 *
 * @param {string} html - HTML content.
 * @returns {string} Text content with scripts/styles removed.
 */
export function htmlToText(html) {
  return collapseText(
    html
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<[^>]+>/g, " "),
  );
}

/**
 * Match and normalize the first capture group from a regular expression.
 *
 * @param {string} text - Text to search.
 * @param {RegExp} pattern - Pattern with at least one capture group.
 * @returns {string | null} First capture group when present.
 */
function matchText(text, pattern) {
  const match = pattern.exec(text);
  return match ? collapseText(match[1]) : null;
}

/**
 * Create a Puppeteer browser for local development or Lambda.
 *
 * @param {Logger} logger - Structured logger.
 * @returns {Promise<import("puppeteer").Browser>} Browser instance.
 */
export async function createBrowser(logger = consoleLogger) {
  const executablePath = process.env.CHROME_EXECUTABLE_PATH
    ? process.env.CHROME_EXECUTABLE_PATH
    : existsSync("/usr/bin/chromium")
      ? "/usr/bin/chromium"
      : undefined;

  if (executablePath) {
    logger.info("launching_system_chromium", { executablePath });
    return puppeteer.launch({
      executablePath,
      headless: true,
      args: ["--no-sandbox", "--disable-web-security"],
      timeout: 30000,
    });
  }

  const { default: chromium } = await import("@sparticuz/chromium");
  logger.info("launching_sparticuz_chromium");
  return puppeteer.launch({
    executablePath: await chromium.executablePath(),
    headless: "shell",
    args: [
      ...chromium.args,
      "--hide-scrollbars",
      "--disable-web-security",
      "--no-sandbox",
      "--disable-features=site-per-process",
    ],
    timeout: 30000,
  });
}

/**
 * Create and configure a new browser page.
 *
 * @param {import("puppeteer").Browser} browser - Browser instance.
 * @returns {Promise<import("puppeteer").Page>} Configured page.
 */
export async function createConfiguredPage(browser) {
  const page = await browser.newPage();
  await page.setUserAgent(DEFAULT_USER_AGENT);
  await page.setExtraHTTPHeaders({ "Accept-Language": "en-US,en;q=0.9" });
  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
  });
  await page.setRequestInterception(true);
  page.on("request", (request) => {
    const blockedTypes = new Set([
      "image",
      "stylesheet",
      "font",
      "media",
      "websocket",
    ]);
    if (blockedTypes.has(request.resourceType())) {
      request.abort().catch(() => undefined);
      return;
    }
    request.continue().catch(() => undefined);
  });
  page.setDefaultTimeout(60000);
  return page;
}

/**
 * Sleep for a fixed amount of time.
 *
 * @param {number} ms - Milliseconds to sleep.
 * @returns {Promise<void>} Resolves after the delay.
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Fill an Accela input with keyboard events so masked fields update correctly.
 *
 * @param {import("puppeteer").Page} page - Browser page.
 * @param {string} selector - CSS selector.
 * @param {string} value - Value to enter.
 * @returns {Promise<void>}
 */
async function setInputValue(page, selector, value) {
  await page.waitForSelector(selector, { timeout: 60000 });
  await page.evaluate(
    (inputSelector, inputValue) => {
      const element = document.querySelector(inputSelector);
      if (element instanceof HTMLInputElement) {
        element.value = inputValue;
        element.dispatchEvent(new Event("input", { bubbles: true }));
        element.dispatchEvent(new Event("change", { bubbles: true }));
        element.blur();
      }
    },
    selector,
    value,
  );
}

/**
 * Wait until the Accela search result area is ready or a known no-results/error state appears.
 *
 * @param {import("puppeteer").Page} page - Browser page.
 * @returns {Promise<void>}
 */
async function waitForAccelaSearchOutcome(page) {
  await page.waitForFunction(
    () => {
      const text = document.body?.innerText ?? "";
      return /Showing\s+\d|Your search returned no results|No records found|error\(s\) occurred on current page|unable to proceed/i.test(
        text,
      );
    },
    { timeout: 90000 },
  );
}

/**
 * Submit the Accela general search form and wait for the postback to settle.
 *
 * @param {import("puppeteer").Page} page - Browser page.
 * @returns {Promise<void>}
 */
async function submitAccelaGeneralSearch(page) {
  await Promise.allSettled([
    page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 90000 }),
    page.click("#ctl00_PlaceHolderMain_btnNewSearch"),
  ]);
  await waitForAccelaSearchOutcome(page);
  await sleep(2500);
}

/**
 * Parse Accela's result summary text into a total count.
 *
 * @param {string} text - Page body text.
 * @returns {{ summary: string | null, total: number | null }} Parsed summary and total.
 */
export function parseResultSummary(text) {
  const match = /Showing\s+([0-9,]+\s*-\s*[0-9,]+\s+of\s+([0-9,]+))/i.exec(
    text,
  );
  if (!match) {
    return { summary: null, total: null };
  }
  const total = Number.parseInt(match[2].replace(/,/g, ""), 10);
  return {
    summary: collapseText(match[1]),
    total: Number.isFinite(total) ? total : null,
  };
}

/**
 * Normalize Accela detail links to absolute URLs.
 *
 * @param {string} url - Raw href from the page.
 * @returns {string} Absolute URL.
 */
function normalizeAccelaUrl(url) {
  return new URL(
    url,
    "https://aca-prod.accela.com/LEECO/Cap/CapHome.aspx",
  ).toString();
}

/**
 * Extract a Lee Accela permit record number from a text fragment.
 *
 * @param {string} value - Raw text from a search-result cell, detail header, or link.
 * @returns {string | null} Uppercase permit record number when one is present.
 */
function readPermitRecordNumber(value) {
  const match = PERMIT_RECORD_NUMBER_PATTERN.exec(collapseText(value));
  return match ? match[0].toUpperCase() : null;
}

/**
 * Return whether a table is the detail page's related-record tree, not the
 * general-search results grid. Related records use `View` links to other permits
 * and can contain hundreds of records unrelated to the scoped parcel search.
 *
 * @param {cheerio.Cheerio<import("domhandler").AnyNode>} table - Candidate table wrapping a CapDetail link.
 * @returns {boolean} True when the table is the Accela related-record tree.
 */
function isRelatedRecordTreeTable(table) {
  const tableId = table.attr("id") ?? "";
  const caption = collapseText(
    table.find("caption").first().text(),
  ).toLowerCase();
  return /tableCapTreeList/i.test(tableId) || caption === "related records";
}

/**
 * Extract the currently-open Accela detail page as a permit link.
 *
 * Parcel searches sometimes redirect directly to `CapDetail.aspx` instead of a
 * search-result grid. In that case the detail page itself is the result; the
 * related-record tree on the page must not be treated as parcel search output.
 *
 * @param {object} params - Detail-page extraction parameters.
 * @param {string} params.html - Full Accela page HTML.
 * @param {string} params.pageUrl - Browser URL for the current page.
 * @param {string} params.sourceWindowKey - Source window or parcel search key.
 * @param {number} params.sourcePage - One-based captured page number.
 * @returns {PermitLink | null} Single current detail record link, or `null` when the page is not a permit detail page.
 */
export function extractCurrentDetailPermitLinkFromHtml({
  html,
  pageUrl,
  sourceWindowKey,
  sourcePage,
}) {
  const text = htmlToText(html);
  const $ = cheerio.load(html);

  // Structural discriminator: distinguish a genuine single-record detail page
  // from a results-list page that may contain a record preview in its message
  // bar (which would otherwise false-match PERMIT_RECORD_HEADER_PATTERN and
  // prematurely break pagination).
  //
  // Key markers verified against real Accela HTML:
  //   - `divRecordStatus`   — present on detail pages; absent on list pages.
  //   - `gdvPermitList`     — the main permit results grid; present on list
  //                           pages; absent on detail pages.
  //
  // Decision:
  //   1. If divRecordStatus is found → definitely a detail page; proceed.
  //   2. If gdvPermitList is found (and divRecordStatus is absent) → results
  //      list; return null.
  //   3. Neither marker present → fall through to PERMIT_RECORD_HEADER_PATTERN
  //      (belt-and-suspenders for pages that predate or omit these markers).
  //
  // Text-only "Showing X-Y of Z" guards were intentionally removed: ~12.5% of
  // real detail pages carry that banner inside a related-records or conditions
  // sub-grid (e.g. gdvGeneralConditionsList), so text matching is unreliable.
  const hasDetailMarker = $("[id*='divRecordStatus']").length > 0;
  const hasListGrid = $("[id*='gdvPermitList']").length > 0;

  if (hasDetailMarker) {
    // Confirmed detail page — skip the null-return checks below.
  } else if (hasListGrid) {
    return null;
  }
  // else: neither marker; fall through and let PERMIT_RECORD_HEADER_PATTERN decide.

  const recordHeader = PERMIT_RECORD_HEADER_PATTERN.exec(text);
  if (recordHeader === null) return null;

  const recordNumber = readPermitRecordNumber(recordHeader[1]);
  if (recordNumber === null) return null;
  const formAction = $("form#aspnetForm").attr("action") ?? null;
  const sourceUrl = /CapDetail\.aspx/i.test(pageUrl)
    ? pageUrl
    : formAction === null
      ? pageUrl
      : normalizeAccelaUrl(formAction);
  const recordStatus = cleanRecordStatus(
    matchText(
      text,
      /Record Status:\s*(.*?)\s+Click here for more information/i,
    ) ??
      matchText(text, /Record Status:\s*(.*?)\s+Create a New Collection/i) ??
      matchText(text, /Record Status:\s*(.*?)\s+Add to Existing Collection/i) ??
      matchText(text, /Record Status:\s*(.*?)\s+Record Info/i) ??
      matchText(text, /Record Status:\s*(.*?)\s+Work Location/i),
  );

  return {
    recordNumber,
    url: normalizeAccelaUrl(sourceUrl),
    address: matchText(text, /Work Location\s+(.*?)\s+\*\s+Record Details/i),
    description: collapseText(recordHeader[2]) || null,
    status: recordStatus,
    action: null,
    relatedRecords: null,
    submittalType: null,
    sourceWindowKey,
    sourcePage,
  };
}

/**
 * Extract permit detail links from an Accela search-result page.
 *
 * @param {string} html - Search result HTML.
 * @param {string} windowKey - Date-window key that produced this page.
 * @param {number} pageNumber - One-based page number.
 * @param {Logger} [logger] - Structured logger used to surface zero-link diagnostics.
 * @returns {PermitLink[]} Extracted permit links.
 */
export function extractPermitLinksFromSearchHtml(
  html,
  windowKey,
  pageNumber,
  logger = consoleLogger,
) {
  const $ = cheerio.load(html);
  /** @type {PermitLink[]} */
  const links = [];
  const capDetailAnchors = $("a[href*='CapDetail.aspx']");
  let relatedRecordFilteredCount = 0;

  capDetailAnchors.each((_, element) => {
    const anchor = $(element);
    const href = anchor.attr("href");
    if (!href) return;

    const row = anchor.closest("tr");
    const table = row.closest("table");
    if (isRelatedRecordTreeTable(table)) {
      relatedRecordFilteredCount += 1;
      return;
    }

    const cells = row
      .find("td")
      .toArray()
      .map((cell) => collapseText($(cell).text()));

    const headers = row
      .closest("table")
      .find("th")
      .toArray()
      .map((header) => collapseText($(header).text()).toLowerCase());

    /**
     * Read a result-table cell by header name, falling back to Lee County's current column index.
     *
     * @param {string} label - Lowercase header label to find.
     * @param {number} fallbackIndex - Fallback cell index.
     * @returns {string | null} Cell text when present.
     */
    const cellByHeader = (label, fallbackIndex) => {
      const index = headers.findIndex((header) => header === label);
      const value = cells[index >= 0 ? index : fallbackIndex];
      return value || null;
    };

    const recordNumber = readPermitRecordNumber(
      cellByHeader("record number", 1) ?? collapseText(anchor.text()),
    );
    if (recordNumber === null) return;

    links.push({
      recordNumber,
      url: normalizeAccelaUrl(href),
      address: cellByHeader("address", 2),
      description:
        cellByHeader("description", 3) ?? cellByHeader("project name", 3),
      status: cellByHeader("status", 4) ?? cellByHeader("record type", 4),
      action: cellByHeader("action", 5),
      relatedRecords: cellByHeader("related records", 6),
      submittalType: cellByHeader("submittal type", 7),
      sourceWindowKey: windowKey,
      sourcePage: pageNumber,
    });
  });

  // Distinguish "0 links because every CapDetail link was inside a
  // related-record tree" from "genuinely 0 CapDetail links on the page".
  // The former points at an Accela table-structure change that defeats
  // isRelatedRecordTreeTable; the latter is an expected empty/no-results page.
  if (links.length === 0 && relatedRecordFilteredCount > 0) {
    logger.warn("lee_search_links_all_filtered_as_related_records", {
      windowKey,
      pageNumber,
      capDetailAnchorCount: capDetailAnchors.length,
      relatedRecordFilteredCount,
    });
  }

  return links;
}

/**
 * Click the next Accela result page if available.
 *
 * @param {import("puppeteer").Page} page - Browser page.
 * @returns {Promise<boolean>} True when a next-page click was started.
 */
async function clickNextResultPage(page) {
  return page.evaluate(() => {
    const anchors = Array.from(document.querySelectorAll("a"));
    const next = anchors.find(
      (anchor) =>
        (anchor.textContent ?? "").replace(/\s+/g, " ").trim() === "Next >" &&
        anchor.getAttribute("href")?.includes("__doPostBack"),
    );
    if (!(next instanceof HTMLAnchorElement)) {
      return false;
    }
    next.click();
    return true;
  });
}

/**
 * Search Lee County Accela permit records for a date window and paginate all visible results.
 *
 * @param {object} params - Search parameters.
 * @param {import("puppeteer").Browser} params.browser - Browser instance.
 * @param {string} params.startDate - ISO start date.
 * @param {string} params.endDate - ISO end date.
 * @param {string | undefined} params.portalUrl - Optional portal URL.
 * @param {number} params.maxPages - Maximum pages to capture.
 * @param {number | undefined} [params.stopAfterFirstPageWhenTotalAtLeast] - Stop after page one when reported total meets this threshold.
 * @param {Logger} params.logger - Structured logger.
 * @returns {Promise<PermitSearchResult>} Search result with captures and links.
 */
export async function searchLeePermitWindow({
  browser,
  startDate,
  endDate,
  portalUrl = LEE_PORTAL_URL,
  maxPages,
  stopAfterFirstPageWhenTotalAtLeast,
  logger,
}) {
  const windowKey = buildWindowKey(startDate, endDate);
  const page = await createConfiguredPage(browser);
  /** @type {SearchPageCapture[]} */
  const pages = [];
  /** @type {PermitLink[]} */
  const permits = [];
  let reportedTotal = null;
  let noResults = false;
  let truncatedForSplit = false;

  try {
    logger.info("lee_window_open", { windowKey, startDate, endDate });
    await page.goto(portalUrl, {
      waitUntil: "domcontentloaded",
      timeout: 90000,
    });
    await page.waitForSelector(
      "#ctl00_PlaceHolderMain_generalSearchForm_txtGSStartDate",
      { timeout: 90000 },
    );

    await setInputValue(
      page,
      "#ctl00_PlaceHolderMain_generalSearchForm_txtGSStartDate",
      toAccelaDate(startDate),
    );
    await setInputValue(
      page,
      "#ctl00_PlaceHolderMain_generalSearchForm_txtGSEndDate",
      toAccelaDate(endDate),
    );

    await submitAccelaGeneralSearch(page);

    for (let pageNumber = 1; pageNumber <= maxPages; pageNumber += 1) {
      const html = await page.content();
      const text = htmlToText(html);
      if (
        /String was not recognized as a valid DateTime|unable to proceed/i.test(
          text,
        )
      ) {
        throw new Error(
          `Accela returned error page for ${windowKey}: ${text.slice(0, 500)}`,
        );
      }

      const detailLink = extractCurrentDetailPermitLinkFromHtml({
        html,
        pageUrl: page.url(),
        sourceWindowKey: windowKey,
        sourcePage: pageNumber,
      });
      if (detailLink !== null) {
        reportedTotal = reportedTotal ?? 1;
        pages.push({
          pageNumber,
          url: page.url(),
          resultSummary: "detail page",
          html,
        });
        permits.push(detailLink);
        logger.info("lee_window_detail_page_captured", {
          windowKey,
          pageNumber,
          recordNumber: detailLink.recordNumber,
        });
        break;
      }

      const { summary, total } = parseResultSummary(text);
      reportedTotal = reportedTotal ?? total;
      noResults = /Your search returned no results|No records found/i.test(
        text,
      );
      pages.push({
        pageNumber,
        url: page.url(),
        resultSummary: summary,
        html,
      });

      const pageLinks = extractPermitLinksFromSearchHtml(
        html,
        windowKey,
        pageNumber,
        logger,
      );
      logger.info("lee_window_page_captured", {
        windowKey,
        pageNumber,
        pageLinks: pageLinks.length,
        summary,
      });
      permits.push(...pageLinks);

      if (
        pageNumber === 1 &&
        stopAfterFirstPageWhenTotalAtLeast !== undefined &&
        total !== null &&
        total >= stopAfterFirstPageWhenTotalAtLeast
      ) {
        truncatedForSplit = true;
        logger.warn("lee_window_high_total_truncated_for_split", {
          windowKey,
          total,
          stopAfterFirstPageWhenTotalAtLeast,
        });
        break;
      }

      if (noResults) break;
      const clicked = await clickNextResultPage(page);
      if (!clicked) break;
      await Promise.race([
        page.waitForNavigation({
          waitUntil: "domcontentloaded",
          timeout: 45000,
        }),
        sleep(6000),
      ]).catch(() => undefined);
      await waitForAccelaSearchOutcome(page);
      await sleep(2000);
    }
  } finally {
    await page.close().catch(() => undefined);
  }

  const deduped = new Map();
  for (const permit of permits) {
    const key = permit.url.toLowerCase();
    if (!deduped.has(key)) {
      deduped.set(key, permit);
    }
  }

  return {
    windowKey,
    startDate,
    endDate,
    portalUrl,
    permits: Array.from(deduped.values()),
    pages,
    reportedTotal,
    noResults,
    truncatedForSplit,
  };
}

/**
 * Search Lee County Accela permit records by a specific property parcel/STRAP.
 *
 * This is the property-first path used when appraisal data is already known:
 * it clears the default date range, fills `Parcel No.`, paginates visible
 * results, and returns the same permit-link shape used by the countywide
 * date-window harvester.
 *
 * @param {object} params - Parcel search parameters.
 * @param {import("puppeteer").Browser} params.browser - Browser instance.
 * @param {string} params.parcelIdentifier - Raw parcel/STRAP value to search.
 * @param {string | undefined} params.portalUrl - Optional portal URL.
 * @param {number} params.maxPages - Maximum result pages to capture.
 * @param {Logger} params.logger - Structured logger.
 * @returns {Promise<PermitParcelSearchResult>} Search result with captures and links.
 */
export async function searchLeePermitParcel({
  browser,
  parcelIdentifier,
  portalUrl = LEE_PORTAL_URL,
  maxPages,
  logger,
}) {
  const normalizedParcelIdentifier =
    normalizeParcelSearchValue(parcelIdentifier);
  if (normalizedParcelIdentifier === null) {
    throw new Error(`Invalid Lee parcel identifier: ${parcelIdentifier}`);
  }
  const searchKey = `parcel-${safeKeyPart(normalizedParcelIdentifier)}`;
  const page = await createConfiguredPage(browser);
  /** @type {SearchPageCapture[]} */
  const pages = [];
  /** @type {PermitLink[]} */
  const permits = [];
  let reportedTotal = null;
  let noResults = false;

  try {
    logger.info("lee_parcel_search_open", {
      searchKey,
      parcelIdentifier,
      normalizedParcelIdentifier,
    });
    await page.goto(portalUrl, {
      waitUntil: "domcontentloaded",
      timeout: 90000,
    });
    await page.waitForSelector(
      "#ctl00_PlaceHolderMain_generalSearchForm_txtGSParcelNo",
      { timeout: 90000 },
    );

    await setInputValue(
      page,
      "#ctl00_PlaceHolderMain_generalSearchForm_txtGSStartDate",
      "",
    );
    await setInputValue(
      page,
      "#ctl00_PlaceHolderMain_generalSearchForm_txtGSEndDate",
      "",
    );
    await setInputValue(
      page,
      "#ctl00_PlaceHolderMain_generalSearchForm_txtGSParcelNo",
      normalizedParcelIdentifier,
    );

    await submitAccelaGeneralSearch(page);

    for (let pageNumber = 1; pageNumber <= maxPages; pageNumber += 1) {
      const html = await page.content();
      const text = htmlToText(html);
      if (/unable to proceed|Object reference not set/i.test(text)) {
        throw new Error(
          `Accela returned error page for ${searchKey}: ${text.slice(0, 500)}`,
        );
      }

      const detailLink = extractCurrentDetailPermitLinkFromHtml({
        html,
        pageUrl: page.url(),
        sourceWindowKey: searchKey,
        sourcePage: pageNumber,
      });
      if (detailLink !== null) {
        reportedTotal = reportedTotal ?? 1;
        pages.push({
          pageNumber,
          url: page.url(),
          resultSummary: "detail page",
          html,
        });
        permits.push(detailLink);
        logger.info("lee_parcel_search_detail_page_captured", {
          searchKey,
          parcelIdentifier,
          normalizedParcelIdentifier,
          pageNumber,
          recordNumber: detailLink.recordNumber,
        });
        break;
      }

      const { summary, total } = parseResultSummary(text);
      reportedTotal = reportedTotal ?? total;
      noResults = /Your search returned no results|No records found/i.test(
        text,
      );
      pages.push({
        pageNumber,
        url: page.url(),
        resultSummary: summary,
        html,
      });

      const pageLinks = extractPermitLinksFromSearchHtml(
        html,
        searchKey,
        pageNumber,
        logger,
      );
      logger.info("lee_parcel_search_page_captured", {
        searchKey,
        parcelIdentifier,
        normalizedParcelIdentifier,
        pageNumber,
        pageLinks: pageLinks.length,
        summary,
      });
      permits.push(...pageLinks);

      if (noResults) break;
      // Snapshot the current page's result summary before clicking "Next >".
      // The click triggers an ASP.NET UpdatePanel partial postback (no full
      // navigation), so we must wait for the "Showing X-Y of Z" banner to
      // CHANGE from this value before re-reading the DOM. Waiting only on
      // waitForAccelaSearchOutcome here is unsafe: the stale prior-page banner
      // still satisfies it, causing page.content() to re-read the old page.
      const priorSummary = summary;
      const clicked = await clickNextResultPage(page);
      if (!clicked) break;
      await page.waitForFunction(
        (previousSummary) => {
          const text = document.body?.innerText ?? "";
          // Resolve immediately on no-results or any Accela error-page pattern.
          // Without this, an error page would spin the full 90s timeout on every
          // retry instead of fast-failing to the loop body's error detection.
          // Patterns mirror waitForAccelaSearchOutcome and the loop body guard.
          if (
            /Your search returned no results|No records found|error\(s\) occurred on current page|unable to proceed|Object reference not set/i.test(
              text,
            )
          ) {
            return true;
          }
          const match =
            /Showing\s+([0-9,]+\s*-\s*[0-9,]+\s+of\s+([0-9,]+))/i.exec(text);
          if (match === null) return false;
          const currentSummary = match[1].replace(/\s+/g, " ").trim();
          return previousSummary === null || currentSummary !== previousSummary;
        },
        { timeout: 90000 },
        priorSummary,
      );
      await sleep(2000);
    }
  } finally {
    await page.close().catch(() => undefined);
  }

  const deduped = new Map();
  for (const permit of permits) {
    const key = permit.url.toLowerCase();
    if (!deduped.has(key)) {
      deduped.set(key, permit);
    }
  }

  return {
    searchKey,
    parcelIdentifier,
    normalizedParcelIdentifier,
    portalUrl,
    permits: Array.from(deduped.values()),
    pages,
    reportedTotal,
    noResults,
  };
}

/**
 * Parse completed inspections from page text.
 *
 * @param {string} text - Full permit detail page text.
 * @returns {InspectionRecord[]} Parsed completed inspections.
 */
export function parseCompletedInspections(text) {
  const start = text.indexOf("Completed (");
  const end = text.indexOf("Digital Projects", start);
  if (start < 0) return [];
  const section = text.slice(start, end > start ? end : start + 4000);
  /** @type {InspectionRecord[]} */
  const inspections = [];
  const pattern =
    /(Pass|Fail|Partial Pass|Cancelled|Canceled)\s+([0-9A-Za-z]+)\s+([^()]+?)\s+\((\d+)\)\s+Result by:\s+(.+?)\s+on\s+(\d{2}\/\d{2}\/\d{4})/gi;
  let item;
  while ((item = pattern.exec(section)) !== null) {
    inspections.push({
      result: collapseText(item[1]),
      inspectionCode: collapseText(item[2]),
      inspectionType: collapseText(item[3]),
      inspectionIdentifier: collapseText(item[4]),
      inspectorName: collapseText(item[5]),
      resultedDate: collapseText(item[6]),
    });
  }
  return inspections;
}

/**
 * Parse key-value pairs from Accela's More Details section.
 *
 * @param {string | null} sectionText - Raw section text.
 * @returns {Record<string, string>} Parsed field map.
 */
export function parseMoreDetails(sectionText) {
  if (!sectionText) return {};
  /** @type {Record<string, string>} */
  const fields = {};
  const knownLabels = [
    "Is the permit being pulled as Owner-Builder?",
    "Is the proposed work a result of hurricane damage?",
    "Type",
    "Comm/Res",
    "Private Provider Plan Review?",
    "Private Provider Inspections?",
    "Estimated Job Value",
    "Estimated Sq. Ft.",
    "Volts",
    "Is this for Site Lighting?",
    "Is Low Voltage work included under an active COM Permit?",
    "Commercial Permit Number",
    "Is this a Mobile Home Park?",
    "Will there be an Access Control System installed?",
    "Data",
    "Parcel Number",
    "Block",
    "Lot",
    "Subdivision",
    "PLANNINGCOMMUNITY",
    "MUNICODE",
    "HISTORIC",
    "FIREDISTRICT",
  ];

  for (const [index, label] of knownLabels.entries()) {
    const nextLabels = knownLabels.slice(index + 1).map(escapeRegExp);
    const boundary =
      nextLabels.length > 0 ? nextLabels.join("|") : "Fees|Inspections|$";
    const pattern = new RegExp(
      `${escapeRegExp(label)}\\s*:?\\s*(.*?)(?=\\s+(?:${boundary})\\s*:?|$)`,
      "i",
    );
    const value = matchText(sectionText, pattern);
    if (value) fields[label] = value;
  }

  return fields;
}

/**
 * Remove Accela portal chrome accidentally captured after the visible permit
 * status. Historic Lee records often render without the newer "Click here for
 * more information" boundary, so a broad text fallback can otherwise include
 * collection controls and tab labels in `recordStatus`.
 *
 * @param {string | null | undefined} value - Raw status text captured from the detail page.
 * @returns {string | null} The visible record status, or `null` when no status is present.
 */
export function cleanRecordStatus(value) {
  if (typeof value !== "string") return null;
  const text = collapseText(value);
  if (text.length === 0) return null;
  const boundary =
    /\s+(?:Click here for more information|Create a New Collection|Add to Existing Collection|Record Info|Record Details|Processing Status|Related Records|Work Location)\b/i.exec(
      text,
    );
  const status =
    boundary === null ? text : text.slice(0, boundary.index).trim();
  return status.length > 0 ? status : null;
}

/**
 * Detect Lee Accela detail pages that loaded successfully at the HTTP layer but
 * only contain the portal's public error message instead of the permit detail
 * tabs. These pages are common for a small number of converted historic records;
 * the search-result row is still public evidence, but the detail page is not
 * retrievable through the public portal.
 *
 * @param {string} text - Collapsed body text from the attempted detail page.
 * @returns {boolean} True when the body is Accela's unavailable/error page.
 */
function isUnavailableAccelaDetailText(text) {
  return /unable to proceed|technical difficulties|Object reference not set|String was not recognized/i.test(
    text,
  );
}

/**
 * Infer a human-readable permit type from Lee Accela's record-number prefix
 * when the full detail page is unavailable and the search-result row is the only
 * remaining public source. This is intentionally conservative: unknown prefixes
 * return `null` so the raw source payload remains authoritative.
 *
 * @param {string} recordNumber - Lee Accela record number from the search result.
 * @returns {string | null} Inferred record type, or `null` when the prefix is unknown.
 */
function inferRecordTypeFromRecordNumber(recordNumber) {
  const prefix = /^[A-Z]+/i.exec(recordNumber)?.[0]?.toUpperCase() ?? null;
  return prefix === null ? null : (RECORD_TYPE_BY_PREFIX.get(prefix) ?? null);
}

/**
 * Remove null and empty-string values from a string dictionary while preserving
 * the exact public evidence keys used in the harvested JSON payload. This keeps
 * JSDoc type checking precise for fallback records assembled from sparse search
 * result columns.
 *
 * @param {Record<string, string | null>} values - Candidate string fields keyed by payload property name.
 * @returns {Record<string, string>} Compact string-only record.
 */
function compactStringRecord(values) {
  /** @type {Record<string, string>} */
  const compacted = {};
  for (const [key, value] of Object.entries(values)) {
    if (typeof value === "string" && value.length > 0) {
      compacted[key] = value;
    }
  }
  return compacted;
}

/**
 * Build a permit-detail extraction from search-result evidence when Accela's
 * public detail page returns its own unavailable/error message. The fallback is
 * deliberately marked with `detailCaptureStatus` and keeps the error text in the
 * source payload so downstream users can distinguish full detail pages from
 * list-derived historic records.
 *
 * @param {object} params - Fallback construction inputs.
 * @param {PermitLink} params.permit - Search-result permit row that discovered the broken detail URL.
 * @param {string} params.sourceUrl - Final browser URL after attempting to open the detail page.
 * @param {string} params.rawText - Collapsed body text from the attempted detail/error page.
 * @param {string} params.errorMessage - Explanation of why the fallback was created.
 * @returns {PermitDetailExtraction} Detail-shaped extraction preserving all public evidence currently available.
 */
export function buildUnavailablePermitDetail({
  permit,
  sourceUrl,
  rawText,
  errorMessage,
}) {
  const inferredRecordType = inferRecordTypeFromRecordNumber(
    permit.recordNumber,
  );
  const evidence = compactStringRecord({
    recordNumber: permit.recordNumber,
    detailUrl: permit.url,
    searchResultAddress: permit.address,
    searchResultDescription: permit.description,
    searchResultStatus: permit.status,
    searchResultAction: permit.action,
    searchResultRelatedRecords: permit.relatedRecords,
    searchResultSubmittalType: permit.submittalType,
    sourceWindowKey: permit.sourceWindowKey,
    sourcePage: String(permit.sourcePage),
  });
  const moreDetails = compactStringRecord({
    Type: inferredRecordType,
    "Search Result Description": permit.description,
    "Search Result Related Records": permit.relatedRecords,
    "Search Result Submittal Type": permit.submittalType,
    "Accela Detail Capture Status": "unavailable_error_page",
    "Accela Detail Capture Error": errorMessage,
  });
  const fallbackLines = [
    `Record ${permit.recordNumber}`,
    inferredRecordType === null ? null : `Record Type: ${inferredRecordType}`,
    permit.status === null ? null : `Record Status: ${permit.status}`,
    permit.address === null ? null : `Work Location: ${permit.address}`,
    permit.description === null
      ? null
      : `Project Description: ${permit.description}`,
    `Accela detail capture status: unavailable_error_page`,
    `Accela detail capture error: ${errorMessage}`,
    rawText.length === 0 ? null : `Accela error page text: ${rawText}`,
  ].filter((line) => line !== null);

  return {
    schemaVersion: "permit-harvest.lee-accela.v1",
    source: "lee-county-accela",
    retrievedAt: new Date().toISOString(),
    sourceUrl,
    recordNumber: permit.recordNumber,
    recordType: inferredRecordType,
    recordStatus: cleanRecordStatus(permit.status),
    workLocation: permit.address,
    parcelIdentifier: null,
    applicant: null,
    licensedProfessional: null,
    projectDescription: permit.description,
    moreDetails,
    moreDetailsRawText: Object.entries(moreDetails)
      .map(([key, value]) => `${key} ${value}`)
      .join(" "),
    inspectionsRawText: null,
    completedInspections: [],
    processingStatusRawText: null,
    documentLinks: [],
    relatedLinks: [],
    rawText: fallbackLines.join("\n"),
    detailCaptureStatus: "unavailable_error_page",
    detailCaptureError: errorMessage,
    detailCaptureEvidence: evidence,
  };
}

/**
 * Escape a string for use in a regular expression.
 *
 * @param {string} value - Raw string.
 * @returns {string} Escaped string.
 */
function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Extract public links from a permit detail page.
 *
 * @param {string} html - Permit detail HTML.
 * @returns {{ documentLinks: ExtractedLink[], relatedLinks: ExtractedLink[] }} Link groups.
 */
export function extractDetailLinks(html) {
  const $ = cheerio.load(html);
  /** @type {ExtractedLink[]} */
  const documentLinks = [];
  /** @type {ExtractedLink[]} */
  const relatedLinks = [];

  $("a[href]").each((_, element) => {
    const anchor = $(element);
    const href = anchor.attr("href");
    if (!href) return;
    const link = {
      text: collapseText(anchor.text()),
      url: normalizeAccelaUrl(href),
      title: anchor.attr("title") ?? null,
    };
    if (
      /urlrouting\.ashx|document|eplan|digitalprojects|GetDocument/i.test(
        href + link.text,
      )
    ) {
      documentLinks.push(link);
    } else if (
      /RelatedRecords|CapDetail|Inspection|Report/i.test(href + link.text)
    ) {
      relatedLinks.push(link);
    }
  });

  return { documentLinks, relatedLinks };
}

/**
 * Extract as much public permit detail data as possible from an Accela detail page.
 *
 * @param {object} params - Extraction parameters.
 * @param {string} params.html - Full detail HTML.
 * @param {string} params.sourceUrl - Detail URL.
 * @param {string} params.fallbackRecordNumber - Record number from the search result.
 * @returns {PermitDetailExtraction} Extracted permit data.
 */
export function extractPermitDetail({ html, sourceUrl, fallbackRecordNumber }) {
  const text = htmlToText(html);
  const recordHeader =
    /Record\s+([A-Z]{2,4}\d{4}-\d{5}(?:-[A-Z0-9]+)?)\s*:\s*(.*?)\s+Record Status:/i.exec(
      text,
    );
  const recordNumber = recordHeader
    ? collapseText(recordHeader[1])
    : fallbackRecordNumber;
  const recordType = recordHeader ? collapseText(recordHeader[2]) : null;
  const recordStatus = cleanRecordStatus(
    matchText(
      text,
      /Record Status:\s*(.*?)\s+Click here for more information/i,
    ) ??
      matchText(text, /Record Status:\s*(.*?)\s+Create a New Collection/i) ??
      matchText(text, /Record Status:\s*(.*?)\s+Add to Existing Collection/i) ??
      matchText(text, /Record Status:\s*(.*?)\s+Record Info/i) ??
      matchText(text, /Record Status:\s*(.*?)\s+Work Location/i),
  );
  const workLocation = matchText(
    text,
    /Work Location\s+(.*?)\s+\*\s+Record Details/i,
  );
  const applicant = matchText(
    text,
    /Applicant:\s*(.*?)\s+Licensed Professional:/i,
  );
  const licensedProfessional = matchText(
    text,
    /Licensed Professional:\s*(.*?)\s+Project Description:/i,
  );
  const projectDescription = matchText(
    text,
    /Project Description:\s*(.*?)\s+More Details/i,
  );
  const moreDetailsRawText = matchText(
    text,
    /More Details\s+(.*?)\s+Fees\s+\*Fee Reductions/i,
  );
  const moreDetails = parseMoreDetails(moreDetailsRawText);
  const parcelIdentifier =
    moreDetails["Parcel Number"] ??
    matchText(text, /Parcel Information\s+Parcel Number:\s*([A-Z0-9]+)/i);
  const inspectionsRawText = matchText(
    text,
    /Inspections\s+(.*?)\s+Digital Projects/i,
  );
  const processingStatusRawText = matchText(
    text,
    /Processing Status\s+(.*?)(?:Related Records|$)/i,
  );
  const { documentLinks, relatedLinks } = extractDetailLinks(html);

  return {
    schemaVersion: "permit-harvest.lee-accela.v1",
    source: "lee-county-accela",
    retrievedAt: new Date().toISOString(),
    sourceUrl,
    recordNumber,
    recordType,
    recordStatus,
    workLocation,
    parcelIdentifier,
    applicant,
    licensedProfessional,
    projectDescription,
    moreDetails,
    moreDetailsRawText,
    inspectionsRawText,
    completedInspections: parseCompletedInspections(text),
    processingStatusRawText,
    documentLinks,
    relatedLinks,
    rawText: text,
  };
}

/**
 * Capture one permit detail page.
 *
 * @param {object} params - Capture parameters.
 * @param {import("puppeteer").Browser} params.browser - Browser instance.
 * @param {PermitLink} params.permit - Permit link to capture.
 * @param {Logger} params.logger - Structured logger.
 * @returns {Promise<{ html: string, extraction: PermitDetailExtraction }>} Raw HTML and extracted data.
 */
export async function captureLeePermitDetail({ browser, permit, logger }) {
  const page = await createConfiguredPage(browser);
  try {
    logger.info("lee_detail_open", {
      recordNumber: permit.recordNumber,
      url: permit.url,
    });
    await page.goto(permit.url, {
      waitUntil: "domcontentloaded",
      timeout: 90000,
    });
    await page.waitForFunction(
      (recordNumber) => {
        const text = document.body?.innerText ?? "";
        return (
          text.includes(recordNumber) ||
          /Record\s+[A-Z]{2,4}\d{4}-\d{5}/.test(text) ||
          /unable to proceed|technical difficulties|Object reference not set|String was not recognized/i.test(
            text,
          )
        );
      },
      { timeout: 90000 },
      permit.recordNumber,
    );
    await sleep(2500);
    const html = await page.content();
    const text = htmlToText(html);
    if (isUnavailableAccelaDetailText(text)) {
      const errorMessage = `Accela returned unavailable detail page for ${permit.recordNumber}: ${text.slice(0, 500)}`;
      logger.warn("lee_detail_unavailable_fallback", {
        recordNumber: permit.recordNumber,
        url: permit.url,
        finalUrl: page.url(),
      });
      return {
        html,
        extraction: buildUnavailablePermitDetail({
          permit,
          sourceUrl: page.url(),
          rawText: text,
          errorMessage,
        }),
      };
    }
    return {
      html,
      extraction: extractPermitDetail({
        html,
        sourceUrl: page.url(),
        fallbackRecordNumber: permit.recordNumber,
      }),
    };
  } finally {
    await page.close().catch(() => undefined);
  }
}

/**
 * Build a stable output stem for a permit detail artifact.
 *
 * @param {PermitLink} permit - Permit link.
 * @returns {string} Stable output stem.
 */
export function buildPermitOutputStem(permit) {
  return `${safeKeyPart(permit.recordNumber)}-${shortHash(permit.url)}`;
}
