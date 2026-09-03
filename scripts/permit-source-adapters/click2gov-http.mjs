// @ts-check

import * as cheerio from "cheerio";

import {
  parseClick2GovAppYearAndNumber,
  parseClick2GovStatusDetailHtml,
} from "../hillsborough/adapters/temple-terrace-click2gov.mjs";

/**
 * @typedef {import("../hillsborough/adapters/temple-terrace-click2gov.mjs").Click2GovPermitDetail} Click2GovPermitDetail
 * @typedef {import("../hillsborough/adapters/temple-terrace-click2gov.mjs").Click2GovFetchResult} Click2GovFetchResult
 */

/**
 * @typedef {object} Click2GovHttpConfig
 * @property {string} origin HTTPS origin + Click2GovBP path, no trailing slash.
 * @property {string} city Issuing city written onto extracted rows.
 * @property {string} sourceStamp Extracted JSON `source` value.
 */

/**
 * @typedef {object} Click2GovSession
 * @property {string} cookies Cookie header for the next request.
 * @property {string} csrf OWASP CSRF token, possibly empty.
 */

/**
 * @typedef {object} Click2GovSearchRow
 * @property {string} applicationNumber Portal application number (`YY-00000NNN`).
 * @property {string | null} workLocation Address column.
 * @property {string | null} parcelIdentifier Parcel column.
 * @property {string | null} contractorName Contractor column.
 * @property {string | null} applicationType Type column.
 * @property {string | null} recordStatus Status column.
 * @property {string} detailPath Relative `selectpermit.html?...` href.
 */

/**
 * @typedef {object} Click2GovAddressQuery
 * @property {string} streetNumber Street number (required by Tarpon search).
 * @property {string} streetName Street name without suffix.
 */

const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const MIN_DELAY_MS = 1_000;

/**
 * @param {string} origin Candidate origin.
 * @returns {string} Origin without a trailing slash.
 */
export function normalizeClick2GovOrigin(origin) {
  const parsed = new URL(origin);
  if (parsed.protocol !== "https:") {
    throw new Error("Click2Gov origin must use HTTPS");
  }
  return parsed.toString().replace(/\/$/, "");
}

/**
 * @param {Click2GovHttpConfig} config Raw config.
 * @returns {Click2GovHttpConfig} Validated config.
 */
export function validateClick2GovHttpConfig(config) {
  if (typeof config.city !== "string" || config.city.trim().length === 0) {
    throw new Error("Click2Gov city is required");
  }
  if (
    typeof config.sourceStamp !== "string" ||
    config.sourceStamp.trim().length === 0
  ) {
    throw new Error("Click2Gov sourceStamp is required");
  }
  return {
    origin: normalizeClick2GovOrigin(config.origin),
    city: config.city.trim(),
    sourceStamp: config.sourceStamp.trim(),
  };
}

/**
 * @param {string} html Select-permit HTML.
 * @returns {string} CSRF token or empty string.
 */
export function extractClick2GovCsrf(html) {
  const match = /name=OWASP_CSRFTOKEN value=([A-Z0-9-]+)/i.exec(html);
  return match?.[1] ?? "";
}

/**
 * @param {Response} response Fetch response.
 * @returns {string} Cookie header assembled from Set-Cookie.
 */
export function cookieHeaderFromResponse(response) {
  if (typeof response.headers.getSetCookie !== "function") {
    const single = response.headers.get("set-cookie");
    return single ? single.split(";")[0] : "";
  }
  return response.headers
    .getSetCookie()
    .map((cookie) => cookie.split(";")[0])
    .filter((part) => part.length > 0)
    .join("; ");
}

/**
 * @param {number} milliseconds Delay duration.
 * @returns {Promise<void>} Resolves after the delay.
 */
function delay(milliseconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

/**
 * Parse the Click2Gov permit-search results table.
 *
 * @param {string} html Search-results HTML.
 * @returns {readonly Click2GovSearchRow[]} Deduplicated rows in table order.
 */
export function parseClick2GovSearchResultsHtml(html) {
  if (typeof html !== "string" || html.length === 0) return [];
  const $ = cheerio.load(html);
  /** @type {Click2GovSearchRow[]} */
  const rows = [];
  /** @type {Set<string>} */
  const seen = new Set();
  $("table.jTable tbody tr").each((_, rowEl) => {
    const cells = $(rowEl).find("td");
    if (cells.length < 1) return;
    const anchor = cells.eq(0).find("a[href]").first();
    const applicationNumber = anchor.text().replace(/\s+/g, " ").trim();
    const detailPath = (anchor.attr("href") ?? "").trim();
    if (applicationNumber.length === 0 || detailPath.length === 0) return;
    if (seen.has(applicationNumber)) return;
    seen.add(applicationNumber);
    rows.push({
      applicationNumber,
      workLocation: textOrNull(cells.eq(1).text()),
      parcelIdentifier: textOrNull(cells.eq(2).text()),
      contractorName: textOrNull(cells.eq(3).text()),
      applicationType: textOrNull(cells.eq(4).text()),
      recordStatus: textOrNull(cells.eq(5).text()),
      detailPath,
    });
  });
  return rows;
}

/**
 * @param {string} value Raw cell text.
 * @returns {string | null} Collapsed text or null.
 */
function textOrNull(value) {
  const normalized = value.replace(/\s+/g, " ").trim();
  return normalized.length > 0 ? normalized : null;
}

/**
 * Classify a Click2Gov HTML body after an application-number POST.
 *
 * @param {string} html Response HTML.
 * @returns {"ok" | "not_found" | "error"} Coarse outcome.
 */
export function classifyClick2GovHtml(html) {
  if (/unexpected error has occurred/i.test(html)) return "error";
  if (/no matching application found/i.test(html)) return "not_found";
  if (/Status Detail/i.test(html)) return "ok";
  if (/Permit Search Results/i.test(html)) return "ok";
  return "error";
}

/**
 * Bootstrap a guest Click2Gov session from `selectpermit.html`.
 *
 * @param {string} origin Normalized Click2Gov origin.
 * @returns {Promise<Click2GovSession>} Cookies + CSRF.
 */
export async function createClick2GovHttpSession(origin) {
  const normalized = normalizeClick2GovOrigin(origin);
  const response = await fetch(`${normalized}/selectpermit.html`, {
    headers: {
      "User-Agent": USER_AGENT,
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    },
    redirect: "follow",
    signal: AbortSignal.timeout(25_000),
  });
  if (response.ok === false) {
    throw new Error(
      `Click2Gov selectpermit GET returned HTTP ${String(response.status)}`,
    );
  }
  const html = await response.text();
  return {
    cookies: cookieHeaderFromResponse(response),
    csrf: extractClick2GovCsrf(html),
  };
}

/**
 * Search Click2Gov by street number + name (searchType=1).
 *
 * @param {object} params Search parameters.
 * @param {string} params.origin Normalized origin.
 * @param {Click2GovSession} params.session Guest session.
 * @param {Click2GovAddressQuery} params.query Street number + name.
 * @returns {Promise<{ html: string, rows: readonly Click2GovSearchRow[], classification: ReturnType<typeof classifyClick2GovHtml> }>}
 *   Results table rows.
 */
export async function searchClick2GovByAddress({ origin, session, query }) {
  const normalized = normalizeClick2GovOrigin(origin);
  const streetNumber = query.streetNumber.trim();
  const streetName = query.streetName.trim();
  if (streetNumber.length === 0 || streetName.length === 0) {
    throw new Error("Click2Gov address search requires streetNumber and streetName");
  }
  const body = new URLSearchParams();
  body.set("searchResultsView", "true");
  body.set("searchType", "1");
  body.set("parcel.streetNumber", streetNumber);
  body.set("parcel.streetName", streetName);
  body.set("target1", "Continue");
  if (session.csrf) body.set("OWASP_CSRFTOKEN", session.csrf);

  const response = await fetch(`${normalized}/selectpermit.html`, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Cookie: session.cookies,
      "User-Agent": USER_AGENT,
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      Referer: `${normalized}/selectpermit.html`,
    },
    body: body.toString(),
    redirect: "follow",
    signal: AbortSignal.timeout(30_000),
  });
  if (response.ok === false) {
    throw new Error(
      `Click2Gov address search returned HTTP ${String(response.status)}`,
    );
  }
  const html = await response.text();
  return {
    html,
    rows: parseClick2GovSearchResultsHtml(html),
    classification: classifyClick2GovHtml(html),
  };
}

/**
 * Fetch one Click2Gov Status Detail by application year + number.
 *
 * @param {object} params Detail parameters.
 * @param {string} params.origin Normalized origin.
 * @param {string} params.applicationNumber `YY-NNNN` or `YY-00000NNN`.
 * @param {number} [params.maxRetries=3] Transient retry budget.
 * @returns {Promise<Click2GovFetchResult>} Parsed detail or a classified miss.
 */
export async function fetchClick2GovDetailByApplicationNumber({
  origin,
  applicationNumber,
  maxRetries = 3,
}) {
  const parsed = parseClick2GovAppYearAndNumber(applicationNumber);
  if (parsed === null) {
    return {
      data: null,
      status: "parse_error",
      error: `Invalid Click2Gov application number: ${applicationNumber}`,
    };
  }
  const normalized = normalizeClick2GovOrigin(origin);
  const selectUrl = `${normalized}/selectpermit.html`;

  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    try {
      const session = await createClick2GovHttpSession(normalized);
      const body = new URLSearchParams();
      body.set("validatePermitView", "true");
      body.set("searchType", "0");
      body.set("permit.appYear", parsed.appYear);
      body.set("permit.appNumber", parsed.appNumber);
      body.set("finish", "Continue");
      if (session.csrf) body.set("OWASP_CSRFTOKEN", session.csrf);

      const response = await fetch(selectUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Cookie: session.cookies,
          "User-Agent": USER_AGENT,
          Accept:
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          Referer: `${selectUrl}?permit.appYearAndNumber=${encodeURIComponent(applicationNumber)}`,
        },
        body: body.toString(),
        redirect: "follow",
        signal: AbortSignal.timeout(30_000),
      });
      if (response.ok === false) {
        if (attempt === maxRetries) {
          return {
            data: null,
            status: "fetch_error",
            error: `POST detail returned HTTP ${String(response.status)}`,
          };
        }
        await delay(MIN_DELAY_MS);
        continue;
      }
      const html = await response.text();
      const classification = classifyClick2GovHtml(html);
      if (classification === "not_found") {
        return {
          data: null,
          status: "not_found",
          error: "No matching application found",
        };
      }
      if (classification === "error") {
        if (attempt === maxRetries) {
          return {
            data: null,
            status: "fetch_error",
            error: "Click2Gov returned an unexpected error page",
          };
        }
        await delay(MIN_DELAY_MS);
        continue;
      }
      const detail = parseClick2GovStatusDetailHtml(html, applicationNumber);
      return {
        data: detail,
        status: detail ? "ok" : "parse_error",
        error: detail ? null : "Failed to parse Status Detail fields from HTML",
      };
    } catch (error) {
      if (attempt === maxRetries) {
        return {
          data: null,
          status: "fetch_error",
          error: error instanceof Error ? error.message : String(error),
        };
      }
      await delay(MIN_DELAY_MS);
    }
  }

  return { data: null, status: "fetch_error", error: "Max retries exceeded" };
}

export { MIN_DELAY_MS };
