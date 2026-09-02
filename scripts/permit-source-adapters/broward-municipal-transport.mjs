// @ts-check

import * as cheerio from "cheerio";
import puppeteer from "puppeteer";

import {
  decideMunicipalSourceAccess,
  runBoundedMunicipalCapture,
  validateMunicipalProbeLimits,
  validateMunicipalQueries,
} from "./broward-municipal-core.mjs";
import {
  parseClick2GovDetailHtml,
  parseClick2GovSearchHtml,
  parseCoconutCreekDetailHtml,
  parseCoconutCreekSearchHtml,
  parseEgovPlusDetailHtml,
  parseEgovPlusSearchHtml,
  parseSmartGovDetailHtml,
  parseSmartGovSearchHtml,
  parseTylerEsuiteDetailHtml,
  parseTylerEsuiteSearchHtml,
} from "./broward-municipal-protocols.mjs";

/**
 * @typedef {import("./broward-municipal-core.mjs").BrowardMunicipalCheckpoint} BrowardMunicipalCheckpoint
 * @typedef {import("./broward-municipal-core.mjs").BrowardMunicipalJurisdictionConfig} BrowardMunicipalJurisdictionConfig
 * @typedef {import("./broward-municipal-core.mjs").BrowardMunicipalProbeLimits} BrowardMunicipalProbeLimits
 * @typedef {import("./broward-municipal-core.mjs").BrowardMunicipalProbeResult} BrowardMunicipalProbeResult
 * @typedef {import("./broward-municipal-core.mjs").BrowardMunicipalQuery} BrowardMunicipalQuery
 * @typedef {import("./broward-municipal-core.mjs").BrowardMunicipalSearchPage} BrowardMunicipalSearchPage
 * @typedef {import("./broward-municipal-core.mjs").BrowardMunicipalSearchReference} BrowardMunicipalSearchReference
 * @typedef {import("./broward-municipal-core.mjs").NormalizedBrowardMunicipalPermit} NormalizedBrowardMunicipalPermit
 */

/**
 * @typedef {object} MunicipalTransportDependencies
 * @property {typeof fetch} [fetchImpl] - Injectable standards-compatible fetch.
 * @property {typeof puppeteer.launch} [launchBrowser] - Injectable headless Chromium launcher.
 * @property {string} [browserExecutablePath] - Explicit isolated-VM Chromium executable.
 * @property {number} [requestTimeoutMs] - Per-request/navigation deadline.
 * @property {number} [maxResponseBytes] - Maximum accepted HTML response bytes.
 * @property {number} [rawResultRowLimit] - Exclusive raw HTML result-row ceiling.
 */

/**
 * @typedef {object} BrowardMunicipalTransport
 * @property {(query:BrowardMunicipalQuery,page:number|string)=>Promise<BrowardMunicipalSearchPage>} fetchSearchPage
 *   Serialized vendor search/page operation.
 * @property {(reference:BrowardMunicipalSearchReference,query:BrowardMunicipalQuery)=>Promise<NormalizedBrowardMunicipalPermit>} fetchDetail
 *   Serialized vendor detail operation.
 * @property {()=>Promise<readonly BrowardMunicipalRecordTypePartition[]>} listRecordTypePartitions
 *   Read the complete official exact-type selector universe when supported.
 * @property {()=>Promise<void>} close - Idempotent transport cleanup.
 */

/**
 * @typedef {object} BrowardMunicipalRecordTypePartition
 * @property {string} value - Exact stable source option value submitted by the transport.
 * @property {string} label - Public source label retained for reconciliation and operator evidence.
 */

/**
 * @typedef {object} MunicipalHttpResult
 * @property {string} text - Bounded UTF-8 response body.
 * @property {URL} finalUrl - Same-origin final response URL.
 * @property {number} status - Successful HTTP status.
 */

const USER_AGENT =
  "oracle-node-broward-permit-pilot/1.0 (+https://github.com/elephant-xyz/oracle-node)";
const BROWSER_IDENTITY_PRODUCT = "oracle-node-broward-permit/1.0";
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_RESPONSE_BYTES = 2_000_000;
const DEFAULT_RAW_RESULT_ROW_LIMIT = 50;
const MAX_REDIRECTS = 3;
const MUNICIPAL_STREET_SUFFIX_PATTERN =
  "ALY|ANX|ARC|AVE|BYU|BCH|BND|BLF|BLFS|BTM|BLVD|BR|BRG|BRK|BRKS|BG|BGS|BYP|CP|CYN|CPE|CSWY|CTR|CTRS|CIR|CIRS|CLF|CLFS|CLB|CMN|CMNS|COR|CORS|CRSE|CT|CTS|CV|CVS|CRK|CRES|CRST|XING|XRD|XRDS|CURV|DL|DM|DV|DR|DRS|EST|ESTS|EXPY|EXT|EXTS|FALL|FLS|FRY|FLD|FLDS|FLT|FLTS|FRD|FRDS|FRST|FRG|FRGS|FRK|FRKS|FT|FWY|GDN|GDNS|GTWY|GLN|GLNS|GRN|GRNS|GRV|GRVS|HBR|HBRS|HVN|HTS|HWY|HL|HLS|HOLW|INLT|IS|ISS|ISLE|JCT|JCTS|KY|KYS|KNL|KNLS|LK|LKS|LAND|LNDG|LN|LGT|LGTS|LF|LCK|LCKS|LDG|LOOP|MALL|MNR|MNRS|MDW|MDWS|MEWS|ML|MLS|MSN|MTWY|MT|MTN|MTNS|NCK|ORCH|OVAL|OPAS|PARK|PARKS|PKWY|PKWYS|PASS|PSGE|PATH|PIKE|PNE|PNES|PL|PLN|PLNS|PLZ|PT|PTS|PRT|PRTS|PR|RADL|RAMP|RNCH|RPD|RPDS|RST|RDG|RDGS|RIV|RD|RDS|RTE|ROW|RUE|RUN|SHL|SHLS|SHR|SHRS|SKWY|SPG|SPGS|SPUR|SPURS|SQ|SQS|STA|STRA|STRM|ST|STS|SMT|TER|TRWY|TRCE|TRAK|TRFY|TRL|TRLS|TUNL|TPKE|UPAS|UN|UNS|VLY|VLYS|VIA|VW|VWS|VLG|VLGS|VL|VIS|WALK|WALKS|WALL|WAY|WAYS|WL|WLS";

/**
 * Return a URL without query, userinfo, or fragment for safe diagnostics.
 *
 * @param {URL} url - Candidate request URL.
 * @returns {string} Origin and pathname only.
 */
function safeRoute(url) {
  return `${url.origin}${url.pathname}`;
}

/**
 * Parse response cookies into a process-private name/value jar.
 *
 * Node's combined `set-cookie` representation can contain an Expires comma.
 * Cookie pairs are recognized only at the start or after a comma followed by
 * a token and equals sign; attributes are never retained.
 *
 * @param {string | null} header - Combined Set-Cookie response header.
 * @param {Map<string, string>} jar - Mutable private cookie jar.
 * @returns {void}
 */
function updateCookieJar(header, jar) {
  if (header === null || header.length === 0) return;
  const cookiePattern = /(?:^|,\s*)([!#$%&'*+\-.^_`|~0-9A-Z]+)=([^;,]*)/giu;
  for (const match of header.matchAll(cookiePattern)) {
    const name = match[1];
    const value = match[2];
    if (name !== undefined && value !== undefined) jar.set(name, value);
  }
}

/**
 * Create a strict same-origin HTTP session with private cookies.
 *
 * Redirects are followed manually so rotated ASP/JSESSION cookies are applied
 * before the next hop. Every response is deadline- and byte-bounded.
 *
 * @param {URL} originUrl - Configured official source URL.
 * @param {typeof fetch} fetchImpl - Injectable fetch implementation.
 * @param {number} timeoutMs - Positive per-hop deadline.
 * @param {number} maxResponseBytes - Positive body ceiling.
 * @returns {{request:(url:string|URL,init?:RequestInit)=>Promise<MunicipalHttpResult>}}
 *   Private same-origin request session.
 */
function createHttpSession(originUrl, fetchImpl, timeoutMs, maxResponseBytes) {
  const cookies = new Map();

  return {
    request: async (url, init = {}) => {
      let currentUrl = new URL(url, originUrl);
      let method = init.method ?? "GET";
      let body = init.body ?? null;
      for (
        let redirectCount = 0;
        redirectCount <= MAX_REDIRECTS;
        redirectCount += 1
      ) {
        if (
          currentUrl.origin !== originUrl.origin ||
          currentUrl.username !== "" ||
          currentUrl.password !== ""
        ) {
          throw new Error("Broward municipal request left its official origin");
        }
        const headers = new Headers(init.headers);
        headers.set("accept", "text/html,application/xhtml+xml");
        headers.set("user-agent", USER_AGENT);
        if (cookies.size > 0) {
          headers.set(
            "cookie",
            [...cookies.entries()]
              .map(([name, value]) => `${name}=${value}`)
              .join("; "),
          );
        }
        const response = await fetchImpl(currentUrl, {
          ...init,
          method,
          body,
          headers,
          redirect: "manual",
          signal: AbortSignal.timeout(timeoutMs),
        });
        updateCookieJar(response.headers.get("set-cookie"), cookies);
        if ([301, 302, 303, 307, 308].includes(response.status)) {
          if (redirectCount === MAX_REDIRECTS) {
            throw new Error("Broward municipal redirect limit reached");
          }
          const location = response.headers.get("location");
          if (location === null) {
            throw new Error("Broward municipal redirect lacks a location");
          }
          currentUrl = new URL(location, currentUrl);
          if (
            response.status === 303 ||
            ((response.status === 301 || response.status === 302) &&
              method.toUpperCase() === "POST")
          ) {
            method = "GET";
            body = null;
          }
          continue;
        }
        if (!response.ok) {
          throw new Error(
            `Broward municipal source returned HTTP ${String(response.status)} at ${safeRoute(currentUrl)}`,
          );
        }
        const declaredBytes = Number(
          response.headers.get("content-length") ?? "0",
        );
        if (
          Number.isFinite(declaredBytes) &&
          declaredBytes > maxResponseBytes
        ) {
          throw new Error("Broward municipal response exceeded its byte limit");
        }
        const text = await response.text();
        const actualBytes = Buffer.byteLength(text, "utf8");
        if (actualBytes === 0 || actualBytes > maxResponseBytes) {
          throw new Error("Broward municipal response size is invalid");
        }
        return { text, finalUrl: currentUrl, status: response.status };
      }
      throw new Error("Broward municipal redirect state is invalid");
    },
  };
}

/**
 * Parse a strict municipal street address for legacy split-field forms.
 *
 * Unit/suite tails are accepted but not submitted because the portals search
 * the base situs. Unsupported free-form strings fail before a source request.
 *
 * @param {string} value - Exact private situs address.
 * @returns {{houseNumber:string,direction:string,streetName:string,suffix:string}}
 *   Legacy form components.
 */
export function parseMunicipalStreetAddress(value) {
  const normalized = value.replace(/\s+/gu, " ").trim().toUpperCase();
  const match = new RegExp(
    `^(\\d+[A-Z]?)\\s+(?:(N|S|E|W|NE|NW|SE|SW)\\s+)?(.+?)\\s+(${MUNICIPAL_STREET_SUFFIX_PATTERN})(?:\\s+(?:APT|BLDG|LOT|STE|UNIT|#)\\s*[A-Z0-9-]+)?$`,
    "u",
  ).exec(normalized);
  if (match === null) {
    throw new Error(
      "Municipal address must contain house number, street name, and supported suffix",
    );
  }
  return {
    houseNumber: /** @type {string} */ (match[1]),
    direction: match[2] ?? "",
    streetName: /** @type {string} */ (match[3]),
    suffix: /** @type {string} */ (match[4]),
  };
}

/**
 * Require an exclusive positive raw-result ceiling.
 *
 * @param {number | undefined} value - Optional caller value.
 * @returns {number} Validated exclusive ceiling.
 */
function validateRawResultRowLimit(value) {
  const limit = value ?? DEFAULT_RAW_RESULT_ROW_LIMIT;
  if (!Number.isInteger(limit) || limit < 2 || limit > 1_000) {
    throw new Error(
      "Municipal raw result row limit must be from 2 through 1000",
    );
  }
  return limit;
}

/**
 * Preserve Chromium's truthful browser capability tokens while appending a
 * stable crawler product identity. Legacy ASP.NET changes form behavior for
 * an unrecognized bare product user agent, so browser transports must retain
 * the actual browser prefix rather than masquerading as a non-browser client.
 *
 * @param {string} browserUserAgent - User agent reported by the launched Chromium instance.
 * @returns {string} Browser-compatible user agent with stable oracle-node identity.
 */
export function buildMunicipalBrowserUserAgent(browserUserAgent) {
  const normalized = browserUserAgent.replace(/\s+/gu, " ").trim();
  if (
    normalized.length === 0 ||
    normalized.length > 1_000 ||
    /[\u0000-\u001f\u007f]/u.test(normalized)
  ) {
    throw new Error("Municipal browser user agent is invalid");
  }
  return `${normalized} ${BROWSER_IDENTITY_PRODUCT}`;
}

/**
 * Build a Click2Gov same-session search body from the live official form.
 *
 * Segmented parcel fields are deliberately unsupported because no Broward
 * folio-to-vendor segment mapping is certified for these three tenants.
 *
 * @param {string} landingHtml - Official Click2Gov landing HTML.
 * @param {BrowardMunicipalJurisdictionConfig} config - Click2Gov tenant.
 * @param {BrowardMunicipalQuery} query - Exact permit or address query.
 * @returns {{body:URLSearchParams,csrfToken:string}} Search body and private token.
 */
export function buildClick2GovSearchBody(landingHtml, config, query) {
  if (config.protocol !== "click2gov") {
    throw new Error("Click2Gov body builder received another protocol");
  }
  const $ = cheerio.load(landingHtml);
  const searchType = query.kind === "permit_number" ? "0" : "1";
  if (query.kind === "folio") {
    throw new Error(
      "Click2Gov BCPA folio mapping is not certified; use permit or address",
    );
  }
  const form = $("form")
    .filter(
      (_index, element) =>
        $(element).find(`input[name="searchType"][value="${searchType}"]`)
          .length === 1,
    )
    .first();
  const csrfToken = form.find('input[name="OWASP_CSRFTOKEN"]').attr("value");
  if (form.length === 0 || csrfToken === undefined || csrfToken.length === 0) {
    throw new Error("Click2Gov landing lacks its expected anonymous form");
  }
  if (query.kind === "permit_number") {
    const match = /^(\d{2})-(\d{1,8})$/u.exec(query.value);
    if (match === null) {
      throw new Error("Click2Gov permit number has an invalid format");
    }
    return {
      body: new URLSearchParams({
        validatePermitView: "true",
        searchType,
        "permit.appYear": /** @type {string} */ (match[1]),
        "permit.appNumber": String(Number(match[2])),
        finish: "Continue",
        OWASP_CSRFTOKEN: csrfToken,
      }),
      csrfToken,
    };
  }
  const address = parseMunicipalStreetAddress(query.value);
  const suffixSupported =
    form.find(
      `select[name="parcel.streetSuffix"] option[value="${address.suffix}"]`,
    ).length === 1;
  return {
    body: new URLSearchParams({
      searchResultsView: "true",
      searchType,
      "parcel.streetNumber": address.houseNumber,
      "parcel.streetDirection": address.direction,
      "parcel.streetName": suffixSupported
        ? address.streetName
        : `${address.streetName} ${address.suffix}`,
      streetSearchType: "contains",
      "parcel.streetSuffix": suffixSupported ? address.suffix : "",
      target1: "Continue",
      OWASP_CSRFTOKEN: csrfToken,
    }),
    csrfToken,
  };
}

/**
 * Build one Coconut Creek legacy exact-search form body.
 *
 * @param {BrowardMunicipalQuery} query - Exact permit, folio, or situs query.
 * @returns {URLSearchParams} Form body with exactly one populated search mode.
 */
export function buildCoconutCreekSearchBody(query) {
  const body = new URLSearchParams({
    permit_no: "",
    parcel_id: "",
    house_num: "",
    street: "",
    submitbutton: "Search",
  });
  if (query.kind === "permit_number") body.set("permit_no", query.value);
  else if (query.kind === "folio") body.set("parcel_id", query.value);
  else {
    const address = parseMunicipalStreetAddress(query.value);
    body.set("house_num", address.houseNumber);
    body.set(
      "street",
      [address.direction, address.streetName, address.suffix]
        .filter((part) => part.length > 0)
        .join(" "),
    );
  }
  return body;
}

/**
 * Recognize only Coconut Creek's fixed anonymous no-match redirect. The
 * source returns to its official search path with `error_num=2` when an exact
 * folio or address has no permit rows; all other redirects remain failures.
 *
 * @param {string | URL} searchUrl - Configured official search endpoint.
 * @param {URL} finalUrl - Same-session terminal response URL.
 * @returns {boolean} True only for the source's certified empty-result state.
 */
export function isCoconutCreekEmptyResultRedirect(searchUrl, finalUrl) {
  const expected = new URL(searchUrl);
  return (
    finalUrl.origin === expected.origin &&
    finalUrl.pathname === expected.pathname &&
    finalUrl.searchParams.get("error_num") === "2"
  );
}

/**
 * Build one Lauderhill eGovPLUS exact-search form body.
 *
 * @param {BrowardMunicipalQuery} query - Exact permit, folio, or situs query.
 * @returns {URLSearchParams} Legacy form body.
 */
export function buildEgovPlusSearchBody(query) {
  const body = new URLSearchParams({
    permit_no: "",
    permtype: "",
    parcel_id: "",
    house_num: "",
    street: "",
    perm_status: "Search",
  });
  if (query.kind === "permit_number") body.set("permit_no", query.value);
  else if (query.kind === "folio") body.set("parcel_id", query.value);
  else {
    const address = parseMunicipalStreetAddress(query.value);
    body.set("house_num", address.houseNumber);
    body.set(
      "street",
      [address.direction, address.streetName, address.suffix]
        .filter((part) => part.length > 0)
        .join(" "),
    );
  }
  return body;
}

/**
 * Build one SmartGov advanced-search body from its private conversation token.
 *
 * Contact/contractor/project fields are always blank and only the selected
 * permit, folio, or situs field is populated.
 *
 * @param {string} landingHtml - Live SmartGov advanced-search HTML.
 * @param {BrowardMunicipalQuery} query - Exact non-person query.
 * @returns {URLSearchParams} Complete advanced-search form body.
 */
export function buildSmartGovSearchBody(landingHtml, query) {
  const $ = cheerio.load(landingHtml);
  const conversation = $('input[name="_conv"]').attr("value");
  if (conversation === undefined || conversation.length === 0) {
    throw new Error("SmartGov landing lacks its conversation token");
  }
  const body = new URLSearchParams({
    _conv: conversation,
    Module: "Permitting",
    CaseNumber: "",
    "CaseType.Description": "",
    "CaseType.LicenseType.IsChild": "",
    "Status.ProcessState": "",
    SubmittedOn: "",
    IssuedOn: "",
    FinaledOn: "",
    "SiteAddress.Street1": "",
    "SiteAddress.City": "",
    "SiteAddress.ZipCode": "",
    "PrimaryParcel.Parcel.ParcelNumber": "",
    "PrimaryContact.Contact.DisplayName": "",
    "PrimaryContractor.Contact.DisplayName": "",
    ProjectName: "",
    Search: "Search",
  });
  if (query.kind === "permit_number") body.set("CaseNumber", query.value);
  else if (query.kind === "folio") {
    body.set("PrimaryParcel.Parcel.ParcelNumber", query.value);
  } else if (query.kind === "record_type") {
    body.set("CaseType.Description", query.value);
  } else body.set("SiteAddress.Street1", query.value);
  return body;
}

/**
 * Create the reusable direct-HTTP transport families.
 *
 * @param {BrowardMunicipalJurisdictionConfig} config - Anonymous jurisdiction configuration.
 * @param {MunicipalTransportDependencies} dependencies - Validated transport dependencies.
 * @returns {BrowardMunicipalTransport} Same-origin protocol transport.
 */
function createDirectHttpTransport(config, dependencies) {
  const timeoutMs = dependencies.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
  const maxResponseBytes =
    dependencies.maxResponseBytes ?? DEFAULT_MAX_RESPONSE_BYTES;
  const rawResultRowLimit = validateRawResultRowLimit(
    dependencies.rawResultRowLimit,
  );
  if (
    !Number.isInteger(timeoutMs) ||
    timeoutMs < 1_000 ||
    timeoutMs > 120_000 ||
    !Number.isInteger(maxResponseBytes) ||
    maxResponseBytes < 10_000 ||
    maxResponseBytes > 5_000_000
  ) {
    throw new Error("Municipal HTTP transport limits are invalid");
  }
  const sourceUrl = new URL(config.searchUrl);
  const session = createHttpSession(
    sourceUrl,
    dependencies.fetchImpl ?? fetch,
    timeoutMs,
    maxResponseBytes,
  );
  /** @type {string | null} */
  let click2GovCsrfToken = null;
  /** @type {Map<string, string>} */
  const click2GovDirectDetails = new Map();
  /** @type {string | null} */
  let smartGovResultHtml = null;

  return {
    fetchSearchPage: async (query, page) => {
      if (typeof page !== "number" || !Number.isInteger(page) || page <= 0) {
        throw new Error("HTML municipal source requires a numeric page");
      }
      if (config.protocol === "click2gov") {
        if (page !== 1) {
          throw new Error("Click2Gov returned unsupported pagination");
        }
        const landing = await session.request(config.searchUrl);
        const request = buildClick2GovSearchBody(landing.text, config, query);
        const result = await session.request(config.searchUrl, {
          method: "POST",
          headers: {
            "content-type": "application/x-www-form-urlencoded",
            origin: sourceUrl.origin,
            referer: config.searchUrl,
          },
          body: request.body,
        });
        const $ = cheerio.load(result.text);
        click2GovCsrfToken =
          $('input[name="OWASP_CSRFTOKEN"]').first().attr("value") ??
          request.csrfToken;
        if (/Status Detail/iu.test($("title").text())) {
          if (query.kind !== "permit_number") {
            throw new Error(
              "Click2Gov returned an unexpected direct detail response",
            );
          }
          const detailUrl = new URL(config.searchUrl);
          detailUrl.searchParams.set("permit.appYearAndNumber", query.value);
          detailUrl.searchParams.set("validatePermitView", "true");
          click2GovDirectDetails.set(query.value, result.text);
          return {
            references: [
              {
                sourceRecordId: query.value,
                permitNumber: query.value,
                detailUrl: detailUrl.toString(),
                sourcePage: page,
                listData: {
                  address: null,
                  record_status: null,
                  record_type: null,
                },
              },
            ],
            nextPage: null,
          };
        }
        return parseClick2GovSearchHtml(result.text, config, {
          sourcePage: page,
          maxRows: rawResultRowLimit,
        });
      }
      if (config.protocol === "coconut_creek") {
        if (page !== 1) {
          throw new Error("Coconut Creek returned unsupported pagination");
        }
        await session.request(config.searchUrl);
        const resultUrl = new URL("permit_status_02.asp", sourceUrl);
        const result = await session.request(resultUrl, {
          method: "POST",
          headers: {
            "content-type": "application/x-www-form-urlencoded",
            origin: sourceUrl.origin,
            referer: config.searchUrl,
          },
          body: buildCoconutCreekSearchBody(query),
        });
        if (
          isCoconutCreekEmptyResultRedirect(config.searchUrl, result.finalUrl)
        ) {
          return { references: [], nextPage: null };
        }
        if (result.finalUrl.pathname !== resultUrl.pathname) {
          throw new Error(
            "Coconut Creek did not return a reconcilable permit result",
          );
        }
        return parseCoconutCreekSearchHtml(result.text, config, {
          maxRows: rawResultRowLimit,
        });
      }
      if (config.protocol === "egovplus") {
        if (page !== 1) {
          throw new Error("eGovPLUS returned unsupported pagination");
        }
        const resultUrl = new URL("perm_status_res.aspx", sourceUrl);
        const result = await session.request(resultUrl, {
          method: "POST",
          headers: {
            "content-type": "application/x-www-form-urlencoded",
            origin: sourceUrl.origin,
            referer: config.searchUrl,
          },
          body: buildEgovPlusSearchBody(query),
        });
        return parseEgovPlusSearchHtml(result.text, config, {
          maxRows: rawResultRowLimit,
        });
      }
      if (config.protocol === "smartgov") {
        if (page === 1) {
          const landing = await session.request(config.searchUrl);
          const result = await session.request(config.searchUrl, {
            method: "POST",
            headers: {
              "content-type": "application/x-www-form-urlencoded",
              origin: sourceUrl.origin,
              referer: config.searchUrl,
            },
            body: buildSmartGovSearchBody(landing.text, query),
          });
          smartGovResultHtml = result.text;
        } else {
          if (smartGovResultHtml === null) {
            throw new Error("SmartGov page resume lacks a search session");
          }
          const $ = cheerio.load(smartGovResultHtml);
          const target = $(`a[data-page="${String(page)}"]`).first();
          const href =
            target.attr("href") ??
            (page > 1 ? $("a[rel='next']").first().attr("href") : undefined);
          if (
            href === undefined ||
            href === "#" ||
            href.toLowerCase().startsWith("javascript:")
          ) {
            throw new Error(
              "SmartGov pagination cannot be reconciled by direct HTTP",
            );
          }
          smartGovResultHtml = (
            await session.request(new URL(href, sourceUrl), {
              headers: { referer: config.searchUrl },
            })
          ).text;
        }
        return parseSmartGovSearchHtml(smartGovResultHtml, config, {
          sourcePage: page,
          maxRows: rawResultRowLimit,
        });
      }
      throw new Error(
        `No direct HTTP municipal transport for ${config.protocol}`,
      );
    },
    fetchDetail: async (reference, query) => {
      if (config.protocol === "click2gov") {
        const directDetail = click2GovDirectDetails.get(
          reference.sourceRecordId,
        );
        if (directDetail !== undefined) {
          click2GovDirectDetails.delete(reference.sourceRecordId);
          return parseClick2GovDetailHtml(directDetail, {
            config,
            reference,
            query,
          });
        }
        const requestUrl = new URL(reference.detailUrl);
        if (click2GovCsrfToken !== null) {
          requestUrl.searchParams.set("OWASP_CSRFTOKEN", click2GovCsrfToken);
        }
        const result = await session.request(requestUrl, {
          headers: { referer: config.searchUrl },
        });
        return parseClick2GovDetailHtml(result.text, {
          config,
          reference,
          query,
        });
      }
      if (config.protocol === "coconut_creek") {
        const selectUrl = new URL("permit_status_02.asp", sourceUrl);
        const detail = await session.request(selectUrl, {
          method: "POST",
          headers: {
            "content-type": "application/x-www-form-urlencoded",
            origin: sourceUrl.origin,
            referer: selectUrl.toString(),
          },
          body: new URLSearchParams({ btnsubmit: reference.permitNumber }),
        });
        const expectedDetail = new URL("permit_status_03.asp", sourceUrl);
        if (detail.finalUrl.pathname !== expectedDetail.pathname) {
          throw new Error(
            "Coconut Creek did not return the selected permit detail",
          );
        }
        return parseCoconutCreekDetailHtml(detail.text, {
          config,
          reference,
          query,
        });
      }
      if (config.protocol === "egovplus") {
        const result = await session.request(reference.detailUrl, {
          headers: { referer: config.searchUrl },
        });
        return parseEgovPlusDetailHtml(result.text, {
          config,
          reference,
          query,
        });
      }
      if (config.protocol === "smartgov") {
        const result = await session.request(reference.detailUrl, {
          headers: { referer: config.searchUrl },
        });
        return parseSmartGovDetailHtml(result.text, {
          config,
          reference,
          query,
        });
      }
      throw new Error(`No direct detail transport for ${config.protocol}`);
    },
    listRecordTypePartitions: async () => {
      throw new Error(
        `${config.jurisdiction} does not expose a certified direct-HTTP type universe`,
      );
    },
    close: async () => {},
  };
}

/**
 * Configure one isolated Chromium page with a stable source identity and hard
 * operation deadline.
 *
 * @param {import("puppeteer").Page} page - Fresh isolated browser page.
 * @param {number} timeoutMs - Navigation and selector deadline.
 * @returns {Promise<void>} Resolves after page controls are installed.
 */
async function configureBrowserPage(page, timeoutMs) {
  await page.setUserAgent(
    buildMunicipalBrowserUserAgent(await page.browser().userAgent()),
  );
  page.setDefaultNavigationTimeout(timeoutMs);
  page.setDefaultTimeout(timeoutMs);
}

/**
 * Select one exact eSuite autocomplete candidate without exposing its value.
 *
 * @param {import("puppeteer").Page} page - eSuite advanced-search page.
 * @param {string} address - Private exact address query.
 * @returns {Promise<void>} Resolves after one unique normalized candidate is selected.
 */
async function selectUniqueEsuiteAddress(page, address) {
  const selector = 'textarea[id$="txtServiceAddress"]';
  await page.type(selector, address, { delay: 10 });
  await page.waitForFunction(() => {
    const candidates = document.querySelectorAll(
      'span[id$="autoCompletePanel"] div',
    );
    return [...candidates].some(
      (candidate) => (candidate.textContent ?? "").trim().length > 0,
    );
  });
  const expected = address.replace(/\s+/gu, " ").trim().toUpperCase();
  const candidates = await page.$$('span[id$="autoCompletePanel"] div');
  /** @type {import("puppeteer").ElementHandle<Element>[]} */
  const matches = [];
  for (const candidate of candidates) {
    const text = await candidate.evaluate(
      (element) => element.textContent ?? "",
    );
    const normalized = text.replace(/\s+/gu, " ").trim().toUpperCase();
    if (
      normalized === expected ||
      normalized.startsWith(`${expected},`) ||
      normalized.startsWith(`${expected} `)
    ) {
      matches.push(candidate);
    }
  }
  if (matches.length !== 1) {
    throw new Error(
      "eSuite address autocomplete did not resolve one exact candidate",
    );
  }
  await matches[0]?.click();
}

/**
 * Advance an eSuite GridView pager to one exact numbered page.
 *
 * @param {import("puppeteer").Page} page - Persistent result page.
 * @param {number} targetPage - Required one-based page.
 * @returns {Promise<void>} Resolves after the full postback navigation.
 */
async function advanceEsuitePage(page, targetPage) {
  const links = await page.$$("a[href]");
  /** @type {import("puppeteer").ElementHandle<Element>[]} */
  const matches = [];
  for (const link of links) {
    const metadata = await link.evaluate((element) => ({
      dataPage: element.getAttribute("data-page"),
      href: element.getAttribute("href") ?? "",
    }));
    if (
      metadata.dataPage === String(targetPage) ||
      metadata.href.includes(`Page$${String(targetPage)}`) ||
      (metadata.href.includes("action=next") &&
        /^next$/iu.test(
          await link.evaluate((element) => element.textContent?.trim() ?? ""),
        ))
    ) {
      matches.push(link);
    }
  }
  if (matches.length !== 1) {
    throw new Error("eSuite numbered pagination could not be reconciled");
  }
  await Promise.all([
    page.waitForNavigation({ waitUntil: "networkidle2" }),
    matches[0]?.click(),
  ]);
}

/**
 * Read and validate the complete non-placeholder option universe from one
 * official exact-type selector. Option values, rather than labels, are the
 * partition identity because eSuite can expose duplicate historical labels
 * backed by distinct non-overlapping source IDs.
 *
 * @param {import("puppeteer").Page} page - Loaded anonymous search page.
 * @param {string} selector - Exact type-select selector.
 * @returns {Promise<readonly BrowardMunicipalRecordTypePartition[]>}
 *   Source-order exact partitions with unique stable values.
 */
async function readRecordTypePartitions(page, selector) {
  const partitions = await page.$$eval(`${selector} option`, (options) =>
    options
      .map((option) => ({
        value: option.getAttribute("value") ?? "",
        label: (option.textContent ?? "").replace(/\s+/gu, " ").trim(),
      }))
      .filter(
        (option) =>
          option.label.length > 0 &&
          option.value.length > 0 &&
          option.value !== "-1",
      ),
  );
  if (
    partitions.length === 0 ||
    partitions.some(
      (partition) =>
        partition.value.length > 500 || partition.label.length > 500,
    ) ||
    new Set(partitions.map((partition) => partition.value)).size !==
      partitions.length
  ) {
    throw new Error("Municipal record-type partition universe is invalid");
  }
  return Object.freeze(
    partitions.map((partition) => Object.freeze({ ...partition })),
  );
}

/**
 * Create the shared persistent-browser transport required by Tyler/New World
 * eSuite's JavaScript autocomplete and ASP.NET postbacks.
 *
 * @param {BrowardMunicipalJurisdictionConfig} config - eSuite jurisdiction.
 * @param {MunicipalTransportDependencies} dependencies - Browser dependencies and deadlines.
 * @returns {Promise<BrowardMunicipalTransport>} Persistent anonymous browser transport.
 */
async function createEsuiteTransport(config, dependencies) {
  const timeoutMs = dependencies.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
  if (
    !Number.isInteger(timeoutMs) ||
    timeoutMs < 1_000 ||
    timeoutMs > 120_000
  ) {
    throw new Error("eSuite browser deadline is invalid");
  }
  const rawResultRowLimit = validateRawResultRowLimit(
    dependencies.rawResultRowLimit,
  );
  const launchBrowser = dependencies.launchBrowser ?? puppeteer.launch;
  const browser = await launchBrowser({
    headless: true,
    executablePath:
      dependencies.browserExecutablePath ?? "/usr/local/bin/google-chrome",
    args: ["--no-sandbox"],
  });
  const searchPage = await browser.newPage();
  await configureBrowserPage(searchPage, timeoutMs);
  /** @type {string | null} */
  let activeQueryIdentity = null;
  let activePage = 0;
  let closed = false;

  return {
    fetchSearchPage: async (query, pageNumber) => {
      if (
        typeof pageNumber !== "number" ||
        !Number.isInteger(pageNumber) ||
        pageNumber <= 0
      ) {
        throw new Error("eSuite requires a positive numbered page");
      }
      const queryIdentity = `${query.kind}\u0000${query.value}`;
      if (pageNumber === 1) {
        await searchPage.goto(config.searchUrl, {
          waitUntil: "networkidle2",
        });
        if (query.kind === "address") {
          await selectUniqueEsuiteAddress(searchPage, query.value);
        } else if (query.kind === "permit_number") {
          await searchPage.type('input[id$="txtPermitNumber"]', query.value, {
            delay: 10,
          });
        } else if (query.kind === "record_type") {
          const selected = await searchPage.select(
            'select[id$="ddlPermitType"]',
            query.value,
          );
          if (selected.length !== 1 || selected[0] !== query.value) {
            throw new Error(
              "eSuite exact record-type partition is no longer available",
            );
          }
        } else {
          throw new Error("eSuite does not expose an anonymous folio field");
        }
        await Promise.all([
          searchPage.waitForNavigation({ waitUntil: "networkidle2" }),
          searchPage.click('input[id$="btnSearch"]'),
        ]);
        activeQueryIdentity = queryIdentity;
        activePage = 1;
      } else {
        if (
          activeQueryIdentity !== queryIdentity ||
          activePage !== pageNumber - 1
        ) {
          throw new Error("eSuite page request does not match active search");
        }
        await advanceEsuitePage(searchPage, pageNumber);
        activePage = pageNumber;
      }
      return parseTylerEsuiteSearchHtml(await searchPage.content(), config, {
        sourcePage: pageNumber,
        maxRows: rawResultRowLimit,
      });
    },
    fetchDetail: async (reference, query) => {
      const detailPage = await browser.newPage();
      try {
        await configureBrowserPage(detailPage, timeoutMs);
        await detailPage.goto(reference.detailUrl, {
          waitUntil: "networkidle2",
        });
        return parseTylerEsuiteDetailHtml(await detailPage.content(), {
          config,
          reference,
          query,
        });
      } finally {
        await detailPage.close();
      }
    },
    listRecordTypePartitions: async () => {
      await searchPage.goto(config.searchUrl, {
        waitUntil: "networkidle2",
      });
      activeQueryIdentity = null;
      activePage = 0;
      return readRecordTypePartitions(
        searchPage,
        'select[id$="ddlPermitType"]',
      );
    },
    close: async () => {
      if (closed) return;
      closed = true;
      await browser.close();
    },
  };
}

/**
 * Advance a SmartGov result set through its first-party AJAX page control.
 * The source uses a zero-based hidden page index while displaying one-based
 * links. Completion requires both the expected hidden index and a changed
 * first result identity so stale DOM content can never be parsed as a page.
 *
 * @param {import("puppeteer").Page} page - Active SmartGov result page.
 * @param {number} targetPage - Required one-based next page.
 * @returns {Promise<void>} Resolves after the exact postback completes.
 */
export async function advanceSmartGovPage(page, targetPage) {
  const links = await page.$$('a[onclick*="gotoPage"]');
  /** @type {import("puppeteer").ElementHandle<Element>[]} */
  const matches = [];
  /** @type {import("puppeteer").ElementHandle<Element>[]} */
  const exactTextMatches = [];
  for (const link of links) {
    const metadata = await link.evaluate((element) => ({
      onclick: element.getAttribute("onclick") ?? "",
      text: (element.textContent ?? "").replace(/\s+/gu, " ").trim(),
    }));
    if (
      new RegExp(`gotoPage\\(\\s*${String(targetPage - 1)}\\s*\\)`, "u").test(
        metadata.onclick,
      )
    ) {
      matches.push(link);
      if (metadata.text === String(targetPage)) exactTextMatches.push(link);
    }
  }
  const selected =
    exactTextMatches.length === 1
      ? exactTextMatches[0]
      : matches.length === 1
        ? matches[0]
        : undefined;
  if (selected === undefined) {
    throw new Error("SmartGov numbered pagination could not be reconciled");
  }
  const previousFirstAction = await page.$eval(
    '.search-result-title a[onclick*="Detail/"]',
    (element) =>
      element.getAttribute("onclick") ?? element.getAttribute("href") ?? "",
  );
  await selected.click();
  await page.waitForFunction(
    (expectedPageIndex, previousAction) => {
      const pageInput = document.querySelector("#_applicationSearchPage");
      const firstResult = document.querySelector(
        '.search-result-title a[onclick*="Detail/"]',
      );
      const currentAction =
        firstResult?.getAttribute("onclick") ??
        firstResult?.getAttribute("href") ??
        "";
      return (
        pageInput instanceof HTMLInputElement &&
        pageInput.value === String(expectedPageIndex) &&
        currentAction.length > 0 &&
        currentAction !== previousAction
      );
    },
    {},
    targetPage - 1,
    previousFirstAction,
  );
}

/**
 * Create one persistent isolated-browser SmartGov transport. The source's
 * official exact-type selector and paging controls are JavaScript actions;
 * the browser executes those controls without authentication, registration,
 * challenge handling, or hidden API reconstruction.
 *
 * @param {BrowardMunicipalJurisdictionConfig} config - SmartGov jurisdiction.
 * @param {MunicipalTransportDependencies} dependencies - Browser dependencies and deadlines.
 * @returns {Promise<BrowardMunicipalTransport>} Persistent anonymous transport.
 */
async function createSmartGovTransport(config, dependencies) {
  const timeoutMs = dependencies.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
  if (
    !Number.isInteger(timeoutMs) ||
    timeoutMs < 1_000 ||
    timeoutMs > 120_000
  ) {
    throw new Error("SmartGov browser deadline is invalid");
  }
  const rawResultRowLimit = validateRawResultRowLimit(
    dependencies.rawResultRowLimit,
  );
  const launchBrowser = dependencies.launchBrowser ?? puppeteer.launch;
  const browser = await launchBrowser({
    headless: true,
    executablePath:
      dependencies.browserExecutablePath ?? "/usr/local/bin/google-chrome",
    args: ["--no-sandbox"],
  });
  const searchPage = await browser.newPage();
  await configureBrowserPage(searchPage, timeoutMs);
  /** @type {string | null} */
  let activeQueryIdentity = null;
  let activePage = 0;
  let closed = false;

  return {
    fetchSearchPage: async (query, pageNumber) => {
      if (
        typeof pageNumber !== "number" ||
        !Number.isInteger(pageNumber) ||
        pageNumber <= 0
      ) {
        throw new Error("SmartGov requires a positive numbered page");
      }
      const queryIdentity = `${query.kind}\u0000${query.value}`;
      if (pageNumber === 1) {
        await searchPage.goto(config.searchUrl, {
          waitUntil: "networkidle2",
        });
        if (query.kind === "record_type") {
          await searchPage.select("#Module", "Permitting");
          const selected = await searchPage.select(
            "#CaseType\\.Description",
            query.value,
          );
          if (selected.length !== 1 || selected[0] !== query.value) {
            throw new Error(
              "SmartGov exact record-type partition is no longer available",
            );
          }
        } else {
          const selector =
            query.kind === "permit_number"
              ? 'input[name="CaseNumber"]'
              : query.kind === "folio"
                ? 'input[name="PrimaryParcel.Parcel.ParcelNumber"]'
                : 'input[name="SiteAddress.Street1"]';
          await searchPage.type(selector, query.value, { delay: 10 });
        }
        await Promise.all([
          searchPage.waitForNavigation({ waitUntil: "networkidle2" }),
          searchPage.click("#Search"),
        ]);
        activeQueryIdentity = queryIdentity;
        activePage = 1;
      } else {
        if (
          activeQueryIdentity !== queryIdentity ||
          activePage !== pageNumber - 1
        ) {
          throw new Error("SmartGov page request does not match active search");
        }
        await advanceSmartGovPage(searchPage, pageNumber);
        activePage = pageNumber;
      }
      return parseSmartGovSearchHtml(await searchPage.content(), config, {
        sourcePage: pageNumber,
        maxRows: rawResultRowLimit,
      });
    },
    fetchDetail: async (reference, query) => {
      const detailPage = await browser.newPage();
      try {
        await configureBrowserPage(detailPage, timeoutMs);
        await detailPage.goto(reference.detailUrl, {
          waitUntil: "networkidle2",
        });
        return parseSmartGovDetailHtml(await detailPage.content(), {
          config,
          reference,
          query,
        });
      } finally {
        await detailPage.close();
      }
    },
    listRecordTypePartitions: async () => {
      await searchPage.goto(config.searchUrl, {
        waitUntil: "networkidle2",
      });
      activeQueryIdentity = null;
      activePage = 0;
      await searchPage.select("#Module", "Permitting");
      return readRecordTypePartitions(searchPage, "#CaseType\\.Description");
    },
    close: async () => {
      if (closed) return;
      closed = true;
      await browser.close();
    },
  };
}

/**
 * Create one anonymous transport only for implemented protocol families.
 *
 * Access policy must be checked before this function. OpenGov remains disabled
 * while its own rendered application reports inaccessible.
 *
 * @param {BrowardMunicipalJurisdictionConfig} config - Anonymous enabled source.
 * @param {MunicipalTransportDependencies} [dependencies={}] - Injectable transport dependencies.
 * @returns {Promise<BrowardMunicipalTransport>} Reusable bounded transport.
 */
export async function createBrowardMunicipalTransport(
  config,
  dependencies = {},
) {
  if (config.protocol === "tyler_esuite") {
    return createEsuiteTransport(config, dependencies);
  }
  if (config.protocol === "smartgov") {
    return createSmartGovTransport(config, dependencies);
  }
  if (["click2gov", "coconut_creek", "egovplus"].includes(config.protocol)) {
    return createDirectHttpTransport(config, dependencies);
  }
  throw new Error(
    `Broward municipal protocol is not transport-enabled: ${config.protocol}`,
  );
}

/**
 * Run one bounded anonymous jurisdiction capture and always close its
 * transport. Access-controlled and landing-only configurations return an
 * explicit skip without constructing HTTP or browser state.
 *
 * @param {object} params - Capture inputs and durable private sinks.
 * @param {BrowardMunicipalJurisdictionConfig} params.config - Jurisdiction configuration.
 * @param {readonly BrowardMunicipalQuery[]} params.queries - One through three exact private queries.
 * @param {Partial<BrowardMunicipalProbeLimits>} [params.limits] - Hard capture ceilings.
 * @param {unknown} [params.checkpoint] - Optional parsed private checkpoint.
 * @param {(record:NormalizedBrowardMunicipalPermit)=>Promise<void>} [params.onRecord] - Idempotent private record sink.
 * @param {(checkpoint:BrowardMunicipalCheckpoint)=>Promise<void>} [params.onCheckpoint] - Atomic private checkpoint sink.
 * @param {(milliseconds:number)=>Promise<void>} [params.wait] - Injectable serialized delay.
 * @param {MunicipalTransportDependencies} [params.dependencies] - Injectable network/browser dependencies.
 * @returns {Promise<BrowardMunicipalProbeResult>} Completed bounded capture or explicit no-request skip.
 */
export async function probeBoundedBrowardMunicipalPermits({
  config,
  queries: rawQueries,
  limits: rawLimits = {},
  checkpoint,
  onRecord,
  onCheckpoint,
  wait,
  dependencies = {},
}) {
  const access = decideMunicipalSourceAccess(config);
  if (access.action === "skip") {
    return runBoundedMunicipalCapture({
      config,
      queries: rawQueries,
      limits: rawLimits,
      checkpoint,
      fetchSearchPage: async () => {
        throw new Error("Skipped municipal source attempted a search");
      },
      fetchDetail: async () => {
        throw new Error("Skipped municipal source attempted a detail");
      },
      onRecord,
      onCheckpoint,
      wait,
    });
  }
  const limits = validateMunicipalProbeLimits(rawLimits);
  const queries = validateMunicipalQueries(rawQueries, limits.maxQueries);
  const transport = await createBrowardMunicipalTransport(config, dependencies);
  try {
    return await runBoundedMunicipalCapture({
      config,
      queries,
      limits,
      checkpoint,
      fetchSearchPage: transport.fetchSearchPage,
      fetchDetail: transport.fetchDetail,
      onRecord,
      onCheckpoint,
      wait,
    });
  } finally {
    await transport.close();
  }
}
