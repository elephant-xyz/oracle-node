// @ts-check

import * as cheerio from "cheerio";
import puppeteer from "puppeteer";

import {
  checkpointCapturedPermit,
  checkpointCompletedSearchPage,
  createPermitAdapterCheckpoint,
  isRecord,
  normalizePermitSearchQuery,
  readSourceDate,
  readSourceText,
  validateOfficialHttpsUrl,
  waitForPermitDelay,
} from "./bounded-permit-common.mjs";

/**
 * @typedef {import("./bounded-permit-common.mjs").NormalizedMunicipalPermit} NormalizedMunicipalPermit
 * @typedef {import("./bounded-permit-common.mjs").PermitAdapterCheckpoint} PermitAdapterCheckpoint
 * @typedef {import("./bounded-permit-common.mjs").PermitSearchQuery} PermitSearchQuery
 */

/**
 * Citizenserve/CAP Government jurisdiction routing.
 *
 * `permitTypeTokens` is required because installation 117 serves two separate
 * Broward municipalities. A search row must identify the configured issuing
 * jurisdiction before it can be emitted under that jurisdiction's source key.
 *
 * @typedef {object} CitizenserveConfig
 * @property {string} portalBaseUrl - Citizenserve portal root ending in `/Portal`.
 * @property {number} citizenserveInstallationId - Public installation/tenant number.
 * @property {string} city - Issuing city written to normalized rows.
 * @property {string} sourceSystem - Stable source key ending `_permits`.
 * @property {string} officialSourceUrl - First-party municipal page documenting the portal/custodian.
 * @property {boolean} anonymousSearchCertified - Whether anonymous record search is approved.
 * @property {string} coverageNote - Historical/custody boundary.
 * @property {readonly string[]} permitTypeTokens - Case-insensitive issuing-jurisdiction markers.
 */

/**
 * One source row from a Citizenserve result page.
 *
 * @typedef {object} CitizenservePermitCandidate
 * @property {string} permitId - Citizenserve public permit identity.
 * @property {string} workOrderId - Citizenserve project/work-order identity.
 * @property {string} permitNumber - Permit number printed in the result.
 * @property {string} detailUrl - Absolute official detail URL.
 * @property {string | null} workLocation - Result-list address.
 * @property {string | null} recordType - Result-list permit type.
 * @property {string | null} workClass - Result-list subtype.
 * @property {string | null} recordStatus - Result-list status.
 * @property {string | null} issueDate - ISO issue date.
 * @property {string | null} description - Result-list description.
 */

/**
 * Parsed public Citizenserve result page.
 *
 * @typedef {object} CitizenserveSearchPage
 * @property {number} pageNumber - One-based source page.
 * @property {number} rangeStart - One-based first displayed result, zero when empty.
 * @property {number} rangeEnd - One-based last displayed result, zero when empty.
 * @property {number} reportedTotal - Total matching source records.
 * @property {readonly CitizenservePermitCandidate[]} candidates - Jurisdiction-matched permit rows.
 * @property {number} excludedJurisdictionCount - Rows belonging to a different tenant jurisdiction.
 * @property {{ start: number, end: number } | null} nextRange - Source paging arguments.
 */

/**
 * Context needed to reconcile a Citizenserve detail with its search row.
 *
 * @typedef {object} CitizenserveDetailContext
 * @property {CitizenserveConfig} config - Validated jurisdiction configuration.
 * @property {PermitSearchQuery} query - Exact submitted property query.
 * @property {number} searchPage - One-based result page.
 * @property {string} searchUrl - Exact official search-page URL.
 * @property {CitizenservePermitCandidate} candidate - Search result being detailed.
 */

/**
 * Per-page evidence retained by the bounded live adapter.
 *
 * @typedef {object} CitizenservePageObservation
 * @property {number} pageNumber - One-based page.
 * @property {number} rangeStart - First displayed result.
 * @property {number} rangeEnd - Last displayed result.
 * @property {number} reportedTotal - Total source records.
 * @property {number} permitCandidateCount - Rows attributed to this jurisdiction.
 * @property {number} excludedJurisdictionCount - Shared-installation rows excluded.
 */

/**
 * Result from one bounded, checkpointed Citizenserve lookup.
 *
 * @typedef {object} CitizenserveProbeResult
 * @property {readonly NormalizedMunicipalPermit[]} records - Detail-backed normalized records.
 * @property {PermitAdapterCheckpoint} checkpoint - Durable resume state.
 * @property {readonly CitizenservePageObservation[]} observations - Search-page evidence.
 * @property {number} reportedTotal - Largest result total observed.
 * @property {number} reportedTotalPages - Thirty-row page count.
 * @property {boolean} paginationTruncated - Whether source pages exceeded the ceiling.
 * @property {boolean} detailsTruncated - Whether detail candidates exceeded the ceiling.
 */

const MAX_CITIZENSERVE_PAGES = 3;
const MAX_CITIZENSERVE_DETAILS = 10;
const MIN_SEARCH_DELAY_MS = 1_000;
const MIN_DETAIL_DELAY_MS = 250;
const RESULTS_PER_PAGE = 30;
const DEFAULT_TIMEOUT_MS = 60_000;

/**
 * Build the official anonymous Citizenserve search URL.
 *
 * @param {CitizenserveConfig} rawConfig - Jurisdiction source configuration.
 * @returns {string} Public search URL.
 */
export function buildCitizenserveSearchUrl(rawConfig) {
  const config = validateCitizenserveConfig(rawConfig);
  const params = new URLSearchParams({
    Action: "showSearchPage",
    ctzPagePrefix: "Portal_",
    installationID: String(config.citizenserveInstallationId),
    original_contactID: "0",
    original_iid: "0",
  });
  return `${config.portalBaseUrl}/PortalController?${params.toString()}`;
}

/**
 * Parse one public Citizenserve result page and fail closed on layout drift.
 *
 * @param {string} html - Full rendered result HTML.
 * @param {object} context - Source parsing context.
 * @param {CitizenserveConfig} context.config - Jurisdiction configuration.
 * @param {number} context.pageNumber - One-based result page.
 * @returns {CitizenserveSearchPage} Bounded page metadata and detail candidates.
 */
export function parseCitizenserveSearchResultsHtml(
  html,
  { config: rawConfig, pageNumber },
) {
  const config = validateCitizenserveConfig(rawConfig);
  if (!Number.isInteger(pageNumber) || pageNumber < 1) {
    throw new Error("Citizenserve pageNumber must be a positive integer");
  }
  if (typeof html !== "string" || html.length === 0) {
    throw new Error("Citizenserve result HTML is empty");
  }

  const $ = cheerio.load(html);
  const heading = readSourceText($("main h1.page-heading").first().text());
  if (heading === null || !/^Permitting Search Results$/iu.test(heading)) {
    throw new Error("Unexpected Citizenserve search-result heading");
  }

  const resultText = readSourceText($("#resultContent").text()) ?? "";
  const errorText = readSourceText(
    $("#resultContent table tbody tr td").first().text(),
  );
  if (
    errorText !== null &&
    /please enter|error|required|invalid/iu.test(errorText)
  ) {
    throw new Error(`Citizenserve search failed: ${errorText}`);
  }

  const rangeMatch =
    /(\d+)\s+to\s+(\d+)\s+of\s+(\d+)\s+records?\s+found/iu.exec(resultText);
  const explicitEmpty = /\bNo records found\b/iu.test(resultText);
  if (rangeMatch === null && !explicitEmpty) {
    throw new Error("Citizenserve result count is missing");
  }

  const rangeStart = rangeMatch === null ? 0 : Number(rangeMatch[1]);
  const rangeEnd = rangeMatch === null ? 0 : Number(rangeMatch[2]);
  const reportedTotal = rangeMatch === null ? 0 : Number(rangeMatch[3]);
  if (
    rangeMatch !== null &&
    (!Number.isInteger(rangeStart) ||
      !Number.isInteger(rangeEnd) ||
      !Number.isInteger(reportedTotal) ||
      rangeStart < 1 ||
      rangeEnd < rangeStart ||
      reportedTotal < rangeEnd)
  ) {
    throw new Error("Citizenserve result range is invalid");
  }

  const expectedHeaders = [
    "Permit#",
    "Address",
    "Permit Type",
    "Sub Type",
    "Status",
    "Issue Date",
    "Work Description",
  ];
  const headers = $("#resultContent table thead th")
    .map((_, element) => readSourceText($(element).text()) ?? "")
    .get();
  if (
    reportedTotal > 0 &&
    (headers.length !== expectedHeaders.length ||
      headers.some((header, index) => header !== expectedHeaders[index]))
  ) {
    throw new Error("Citizenserve permit result columns changed");
  }

  /** @type {CitizenservePermitCandidate[]} */
  const candidates = [];
  let excludedJurisdictionCount = 0;
  $("#resultContent table tbody tr").each((_, row) => {
    if (reportedTotal === 0) return;
    const cells = $(row).find("td");
    if (cells.length !== expectedHeaders.length) {
      throw new Error("Citizenserve permit result row has unexpected columns");
    }
    const anchor = cells.eq(0).find("a").first();
    const permitNumber = readSourceText(anchor.text());
    const detailUrl = parseCitizenserveDetailLink(anchor.attr("href"), config);
    if (permitNumber === null) {
      throw new Error("Citizenserve result row has no permit number");
    }
    const parsedDetailUrl = new URL(detailUrl);
    const permitId = parsedDetailUrl.searchParams.get("permit_ID");
    const workOrderId = parsedDetailUrl.searchParams.get("workOrder_ID");
    if (permitId === null || workOrderId === null) {
      throw new Error("Citizenserve detail link has no source identity");
    }
    const recordType = readSourceText(cells.eq(2).text());
    if (!matchesCitizenserveJurisdiction(recordType, config)) {
      excludedJurisdictionCount += 1;
      return;
    }
    candidates.push({
      permitId,
      workOrderId,
      permitNumber,
      detailUrl,
      workLocation: readSourceText(cells.eq(1).text()),
      recordType,
      workClass: readSourceText(cells.eq(3).text()),
      recordStatus: readSourceText(cells.eq(4).text()),
      issueDate: readSourceDate(cells.eq(5).text()),
      description: readSourceText(cells.eq(6).text()),
    });
  });

  if (
    reportedTotal > 0 &&
    candidates.length + excludedJurisdictionCount !== rangeEnd - rangeStart + 1
  ) {
    throw new Error("Citizenserve parsed row count differs from source range");
  }

  const nextHref = $("#resultContent a")
    .toArray()
    .map((element) => $(element).attr("href") ?? "")
    .find((href) => href.includes("displayResultNPagging"));
  const nextMatch =
    nextHref === undefined
      ? null
      : /displayResultNPagging\('(\d+)','(\d+)'\)/u.exec(nextHref);
  const nextRange =
    nextMatch === null
      ? null
      : { start: Number(nextMatch[1]), end: Number(nextMatch[2]) };
  if (
    nextRange !== null &&
    (nextRange.start !== rangeEnd ||
      nextRange.end !== rangeEnd + RESULTS_PER_PAGE)
  ) {
    throw new Error("Citizenserve next-page range is unexpected");
  }

  return {
    pageNumber,
    rangeStart,
    rangeEnd,
    reportedTotal,
    candidates,
    excludedJurisdictionCount,
    nextRange,
  };
}

/**
 * Classify a Citizenserve result before requesting its detail page.
 *
 * @param {CitizenservePermitCandidate} candidate - Public search-result row.
 * @returns {boolean} True only when type, class, or description says roofing.
 */
export function isCitizenserveRoofPermitCandidate(candidate) {
  return /\broof(?:ing)?\b/iu.test(
    [
      candidate.recordType,
      candidate.workClass,
      candidate.description,
    ]
      .filter((value) => typeof value === "string")
      .join(" "),
  );
}

/**
 * Normalize a public Citizenserve detail page after reconciling its search row.
 *
 * The parser uses only the permit summary/core tab. Reviews, documents,
 * inspections, contacts, payments, and user-account data are not traversed.
 *
 * @param {string} html - Full public detail HTML.
 * @param {CitizenserveDetailContext} context - Search/detail reconciliation context.
 * @returns {NormalizedMunicipalPermit} Detail-backed normalized permit.
 */
export function parseCitizenservePermitDetailHtml(html, context) {
  const config = validateCitizenserveConfig(context.config);
  const query = normalizePermitSearchQuery(context.query);
  if (
    !Number.isInteger(context.searchPage) ||
    context.searchPage < 1 ||
    typeof html !== "string" ||
    html.length === 0
  ) {
    throw new Error("Citizenserve detail context is invalid");
  }

  const $ = cheerio.load(html);
  if (
    readSourceText($("main h1.page-heading").first().text()) !== "View Permit"
  ) {
    throw new Error("Unexpected Citizenserve permit detail page");
  }
  const permitNumber = readCitizenserveDetailRow($, "Permit #:");
  if (
    permitNumber === null ||
    permitNumber !== context.candidate.permitNumber
  ) {
    throw new Error("Citizenserve detail permit differs from search result");
  }

  const recordType = readCitizenserveDetailRow($, "Permit Type:");
  const workClass = readCitizenserveDetailRow($, "Sub Type:");
  const issueDate = readSourceDate(readCitizenserveDetailRow($, "Issue Date:"));
  const expirationDate = readSourceDate(
    readCitizenserveDetailRow($, "Expiration Date:"),
  );
  const status = readCitizenserveSummaryField($, "Status:");
  const workLocation = readCitizenserveSummaryField($, "Address:");
  const description = readCitizenserveSummaryField($, "Description:");
  const projectNumber = readCitizenserveSummaryField($, "Project #:");

  assertSameOptionalText(
    "permit type",
    context.candidate.recordType,
    recordType,
  );
  assertSameOptionalText(
    "permit subtype",
    context.candidate.workClass,
    workClass,
  );
  assertSameOptionalText(
    "permit status",
    context.candidate.recordStatus,
    status,
  );
  if (
    context.candidate.issueDate !== null &&
    issueDate !== context.candidate.issueDate
  ) {
    throw new Error(
      "Citizenserve detail issue date differs from search result",
    );
  }

  const sourceRecordId = context.candidate.permitId;
  const normalizedDescription = description ?? context.candidate.description;
  const recordTypeForRoof = [recordType, workClass, normalizedDescription]
    .filter((value) => value !== null)
    .join(" ");
  return {
    source_system: config.sourceSystem,
    source_vendor: "citizenserve_cap_government",
    source_url: context.candidate.detailUrl,
    source_record_id: sourceRecordId,
    record_key: `${config.sourceSystem}:${sourceRecordId}`,
    city: config.city,
    permit_number: permitNumber,
    parcel_identifier: query.kind === "folio" ? query.value : null,
    work_location: workLocation ?? context.candidate.workLocation,
    permit_issue_date: issueDate ?? context.candidate.issueDate,
    application_date: null,
    expiration_date: expirationDate,
    finalized_date: null,
    record_status: status ?? context.candidate.recordStatus,
    record_type: recordType ?? context.candidate.recordType,
    work_class: workClass ?? context.candidate.workClass,
    project_description: normalizedDescription,
    square_feet: null,
    job_value: null,
    is_roof_permit: /\broof(?:ing)?\b/iu.test(recordTypeForRoof),
    provenance: {
      official_source_url: config.officialSourceUrl,
      search_url: context.searchUrl,
      detail_url: context.candidate.detailUrl,
      query_kind: query.kind,
      query_value: query.value,
      search_page: context.searchPage,
    },
    raw: {
      permit_id: context.candidate.permitId,
      work_order_id: context.candidate.workOrderId,
      project_number: projectNumber,
    },
  };
}

/**
 * Run one checkpointed, low-rate Citizenserve property-first lookup.
 *
 * The first search is submitted by the portal's rendered form. Citizenserve's
 * own page executes its ordinary reCAPTCHA v3 JavaScript; this adapter never
 * obtains, injects, replays, or bypasses a challenge token. Visible challenges,
 * login redirects, malformed responses, or source errors fail the run.
 *
 * @param {object} params - Bounded adapter parameters.
 * @param {CitizenserveConfig} params.config - Jurisdiction source configuration.
 * @param {PermitSearchQuery} params.query - Exact folio or situs-address query.
 * @param {number} [params.maxPages=3] - Search page ceiling, never above three.
 * @param {number} [params.maxDetails=10] - Detail ceiling, never above ten.
 * @param {number} [params.searchDelayMs=1500] - Delay between result pages.
 * @param {number} [params.detailDelayMs=500] - Delay between detail pages.
 * @param {PermitAdapterCheckpoint} [params.checkpoint] - Optional resume state.
 * @param {(checkpoint: PermitAdapterCheckpoint) => Promise<void>} [params.onCheckpoint] - Durable state callback.
 * @param {number} [params.timeoutMs=60000] - Browser navigation/source timeout.
 * @param {boolean} [params.roofOnly=false] - Detail only search rows explicitly marked roofing.
 * @returns {Promise<CitizenserveProbeResult>} Captures, evidence, and resume state.
 */
export async function probeBoundedCitizenserve({
  config: rawConfig,
  query: rawQuery,
  maxPages = MAX_CITIZENSERVE_PAGES,
  maxDetails = MAX_CITIZENSERVE_DETAILS,
  searchDelayMs = 1_500,
  detailDelayMs = 500,
  checkpoint: suppliedCheckpoint,
  onCheckpoint,
  timeoutMs = DEFAULT_TIMEOUT_MS,
  roofOnly = false,
}) {
  const config = validateCitizenserveConfig(rawConfig);
  const query = normalizePermitSearchQuery(rawQuery);
  validateCitizenserveLimits({
    maxPages,
    maxDetails,
    searchDelayMs,
    detailDelayMs,
    timeoutMs,
  });
  let checkpoint =
    suppliedCheckpoint ??
    createPermitAdapterCheckpoint(config.sourceSystem, query);
  assertCheckpointMatches(checkpoint, config.sourceSystem, query);
  if (checkpoint.completedDetailIds.length > maxDetails) {
    throw new Error(
      "Citizenserve maxDetails is below the number already captured in checkpoint",
    );
  }

  const executablePath = resolveChromeExecutablePath();
  const browser = await puppeteer.launch({
    headless: true,
    ...(executablePath === null ? {} : { executablePath }),
  });
  const searchUrl = buildCitizenserveSearchUrl(config);
  const page = await browser.newPage();
  /** @type {CitizenservePageObservation[]} */
  const observations = [];
  let reportedTotal = 0;
  let reportedTotalPages = 0;
  let paginationTruncated = false;
  let detailsTruncated = false;

  try {
    await submitCitizenserveSearch(page, searchUrl, query, timeoutMs);
    for (let pageNumber = 1; pageNumber <= maxPages; pageNumber += 1) {
      const parsed = parseCitizenserveSearchResultsHtml(await page.content(), {
        config,
        pageNumber,
      });
      const candidates = roofOnly
        ? parsed.candidates.filter(isCitizenserveRoofPermitCandidate)
        : parsed.candidates;
      reportedTotal = Math.max(reportedTotal, parsed.reportedTotal);
      const totalPages =
        parsed.reportedTotal === 0
          ? 0
          : Math.ceil(parsed.reportedTotal / RESULTS_PER_PAGE);
      reportedTotalPages = Math.max(reportedTotalPages, totalPages);
      paginationTruncated ||= totalPages > maxPages;
      observations.push({
        pageNumber,
        rangeStart: parsed.rangeStart,
        rangeEnd: parsed.rangeEnd,
        reportedTotal: parsed.reportedTotal,
        permitCandidateCount: candidates.length,
        excludedJurisdictionCount: parsed.excludedJurisdictionCount,
      });

      if (!checkpoint.completedSearchPages.includes(pageNumber)) {
        let completedWholePage = true;
        for (const candidate of candidates) {
          if (checkpoint.completedDetailIds.includes(candidate.permitId)) {
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
          const detail = await captureCitizenserveDetail({
            browser,
            config,
            query,
            candidate,
            searchPage: pageNumber,
            searchUrl,
            timeoutMs,
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
        parsed.nextRange === null ||
        checkpoint.completedDetailIds.length >= maxDetails
      ) {
        if (
          checkpoint.completedDetailIds.length >= maxDetails &&
          (parsed.nextRange !== null ||
            !checkpoint.completedSearchPages.includes(pageNumber))
        ) {
          detailsTruncated = true;
        }
        break;
      }
      if (pageNumber >= maxPages) break;
      await waitForPermitDelay(searchDelayMs);
      await openCitizenserveNextPage(page, parsed.nextRange, timeoutMs);
    }
  } finally {
    await page.close().catch(() => undefined);
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
 * Validate and normalize jurisdiction-level Citizenserve configuration.
 *
 * @param {CitizenserveConfig} rawConfig - Candidate configuration.
 * @returns {CitizenserveConfig} Normalized configuration.
 */
function validateCitizenserveConfig(rawConfig) {
  if (
    typeof rawConfig.city !== "string" ||
    rawConfig.city.trim().length === 0
  ) {
    throw new Error("Citizenserve city is required");
  }
  if (
    typeof rawConfig.sourceSystem !== "string" ||
    !/^[a-z0-9_]+_permits$/u.test(rawConfig.sourceSystem)
  ) {
    throw new Error("Citizenserve sourceSystem must end in lowercase _permits");
  }
  if (rawConfig.anonymousSearchCertified !== true) {
    throw new Error(
      `${rawConfig.city} anonymous Citizenserve search is not certified; login will not be attempted`,
    );
  }
  if (
    !Number.isInteger(rawConfig.citizenserveInstallationId) ||
    rawConfig.citizenserveInstallationId < 1
  ) {
    throw new Error("Citizenserve installation ID must be a positive integer");
  }
  if (
    !Array.isArray(rawConfig.permitTypeTokens) ||
    rawConfig.permitTypeTokens.length === 0 ||
    rawConfig.permitTypeTokens.some(
      (token) => typeof token !== "string" || token.trim().length === 0,
    )
  ) {
    throw new Error("Citizenserve permitTypeTokens are required");
  }
  const portalBaseUrl = validateOfficialHttpsUrl(
    rawConfig.portalBaseUrl,
    "Citizenserve portalBaseUrl",
    "www6.citizenserve.com",
  ).replace(/\/$/u, "");
  if (!/\/Portal$/u.test(new URL(portalBaseUrl).pathname)) {
    throw new Error("Citizenserve portalBaseUrl must end in /Portal");
  }
  const officialSourceUrl = validateOfficialHttpsUrl(
    rawConfig.officialSourceUrl,
    "Citizenserve officialSourceUrl",
  );
  const coverageNote = readSourceText(rawConfig.coverageNote);
  if (coverageNote === null) {
    throw new Error("Citizenserve coverageNote is required");
  }
  return {
    portalBaseUrl,
    citizenserveInstallationId: rawConfig.citizenserveInstallationId,
    city: rawConfig.city.trim(),
    sourceSystem: rawConfig.sourceSystem,
    officialSourceUrl,
    anonymousSearchCertified: true,
    coverageNote,
    permitTypeTokens: rawConfig.permitTypeTokens.map((token) =>
      token.trim().toLowerCase(),
    ),
  };
}

/**
 * Parse and validate a JavaScript-wrapped public detail link.
 *
 * @param {string | undefined} value - Search-result anchor `href`.
 * @param {CitizenserveConfig} config - Validated source configuration.
 * @returns {string} Absolute official detail URL.
 */
function parseCitizenserveDetailLink(value, config) {
  const match =
    typeof value === "string"
      ? /^javascript:openURLLink\('([^']+)'\);?$/u.exec(value)
      : null;
  if (match === null) {
    throw new Error("Citizenserve permit row has an invalid detail link");
  }
  const detailUrl = new URL(match[1], `${config.portalBaseUrl}/`);
  if (
    detailUrl.protocol !== "https:" ||
    detailUrl.hostname !== "www6.citizenserve.com" ||
    detailUrl.pathname !== "/Portal/PortalController" ||
    detailUrl.searchParams.get("Action") !== "viewPortalCase" ||
    detailUrl.searchParams.get("type") !== "Permit" ||
    detailUrl.searchParams.get("installationID") !==
      String(config.citizenserveInstallationId)
  ) {
    throw new Error(
      "Citizenserve detail link left the configured public source",
    );
  }
  return detailUrl.toString();
}

/**
 * Check the result's permit type against configured issuing-jurisdiction text.
 *
 * @param {string | null} recordType - Source permit-type text.
 * @param {CitizenserveConfig} config - Validated jurisdiction configuration.
 * @returns {boolean} Whether the row belongs to this configured jurisdiction.
 */
function matchesCitizenserveJurisdiction(recordType, config) {
  if (recordType === null) return false;
  const normalized = recordType.toLowerCase();
  return config.permitTypeTokens.some((token) => normalized.includes(token));
}

/**
 * Read one label/value row from the Citizenserve core permit tab.
 *
 * @param {cheerio.CheerioAPI} $ - Loaded detail document.
 * @param {string} label - Exact source label.
 * @returns {string | null} Core permit value.
 */
function readCitizenserveDetailRow($, label) {
  /** @type {string | null} */
  let found = null;
  $("#permit .row").each((_, row) => {
    if (found !== null) return;
    const columns = $(row).children("div");
    if (columns.length < 2) return;
    if (readSourceText(columns.eq(0).text()) === label) {
      found = readSourceText(columns.eq(1).text());
    }
  });
  return found;
}

/**
 * Read a bold-label field from the public detail summary.
 *
 * @param {cheerio.CheerioAPI} $ - Loaded detail document.
 * @param {string} label - Exact bold label.
 * @returns {string | null} Text before the next line break.
 */
function readCitizenserveSummaryField($, label) {
  const summary = $("main .configspace > .row font.color-11").first();
  const bold = summary
    .find("b")
    .toArray()
    .find((element) => readSourceText($(element).text()) === label);
  if (bold === undefined) return null;
  /** @type {string[]} */
  const fragments = [];
  let sibling = bold.nextSibling;
  while (sibling !== null) {
    if (
      sibling.type === "tag" &&
      "name" in sibling &&
      sibling.name.toLowerCase() === "br"
    ) {
      break;
    }
    fragments.push($(sibling).text());
    sibling = sibling.nextSibling;
  }
  return readSourceText(fragments.join(" "));
}

/**
 * Reject disagreement between list and detail fields when both are present.
 *
 * @param {string} fieldName - Field used in errors.
 * @param {string | null} listValue - Search-result value.
 * @param {string | null} detailValue - Detail value.
 * @returns {void}
 */
function assertSameOptionalText(fieldName, listValue, detailValue) {
  if (listValue !== null && detailValue !== null && listValue !== detailValue) {
    throw new Error(
      `Citizenserve detail ${fieldName} differs from search result`,
    );
  }
}

/**
 * Submit the public search form through the site's rendered user flow.
 *
 * @param {import("puppeteer").Page} page - Browser page.
 * @param {string} searchUrl - Official search page URL.
 * @param {PermitSearchQuery} query - Validated query.
 * @param {number} timeoutMs - Browser/source timeout.
 * @returns {Promise<void>} Resolves on a rendered result page.
 */
async function submitCitizenserveSearch(page, searchUrl, query, timeoutMs) {
  await page.goto(searchUrl, {
    waitUntil: "networkidle2",
    timeout: timeoutMs,
  });
  if (await hasVisiblePasswordField(page)) {
    throw new Error(
      "Citizenserve unexpectedly requires login; credentials will not be used",
    );
  }

  const fieldsResponse = page.waitForResponse(
    (response) =>
      response.url().includes("getSearchFieldsOnFileType") &&
      response.request().method() === "GET",
    { timeout: timeoutMs },
  );
  await page.select("#filetype", "Permit");
  await fieldsResponse;
  const selector = query.kind === "folio" ? "#parcelNumber" : "#address";
  await page.waitForFunction(
    (fieldSelector) => {
      const field = document.querySelector(fieldSelector);
      return (
        field instanceof HTMLInputElement &&
        field.offsetParent !== null &&
        field.disabled === false
      );
    },
    { timeout: timeoutMs },
    selector,
  );
  await page.type(selector, query.value);
  const submittedValue = await page.$eval(selector, (element) =>
    element instanceof HTMLInputElement ? element.value : null,
  );
  if (submittedValue !== query.value) {
    throw new Error("Citizenserve form changed the submitted property query");
  }

  await Promise.all([
    page.waitForNavigation({
      waitUntil: "networkidle2",
      timeout: timeoutMs,
    }),
    page.click("#submitRow button"),
  ]);
  const visibleChallenge = await page.evaluate(() =>
    [...document.querySelectorAll("iframe[src*='recaptcha']")].some(
      (element) =>
        element instanceof HTMLIFrameElement &&
        element.offsetParent !== null &&
        /challenge/iu.test(element.title),
    ),
  );
  if (visibleChallenge) {
    throw new Error(
      "Citizenserve presented a visible challenge; bypass will not be attempted",
    );
  }
}

/**
 * Open one source-provided next-page range through the rendered result form.
 *
 * @param {import("puppeteer").Page} page - Current result page.
 * @param {{ start: number, end: number }} nextRange - Parsed source range.
 * @param {number} timeoutMs - Browser/source timeout.
 * @returns {Promise<void>} Resolves after navigation.
 */
async function openCitizenserveNextPage(page, nextRange, timeoutMs) {
  const navigation = page.waitForNavigation({
    waitUntil: "networkidle2",
    timeout: timeoutMs,
  });
  const clicked = await page.evaluate(({ start, end }) => {
    const expected = `displayResultNPagging('${String(start)}','${String(end)}')`;
    const link = [...document.querySelectorAll("#resultContent a")].find(
      (candidate) => candidate.getAttribute("href")?.includes(expected),
    );
    if (!(link instanceof HTMLAnchorElement)) return false;
    link.click();
    return true;
  }, nextRange);
  if (!clicked) {
    await navigation.catch(() => undefined);
    throw new Error("Citizenserve next-page link disappeared");
  }
  await navigation;
}

/**
 * Capture and normalize one direct public Citizenserve permit detail.
 *
 * @param {object} params - Detail request parameters.
 * @param {import("puppeteer").Browser} params.browser - Shared browser session.
 * @param {CitizenserveConfig} params.config - Validated source configuration.
 * @param {PermitSearchQuery} params.query - Exact property query.
 * @param {CitizenservePermitCandidate} params.candidate - Search result identity.
 * @param {number} params.searchPage - One-based result page.
 * @param {string} params.searchUrl - Official search URL.
 * @param {number} params.timeoutMs - Browser/source timeout.
 * @returns {Promise<NormalizedMunicipalPermit>} Detail-backed record.
 */
async function captureCitizenserveDetail({
  browser,
  config,
  query,
  candidate,
  searchPage,
  searchUrl,
  timeoutMs,
}) {
  const detailPage = await browser.newPage();
  try {
    const response = await detailPage.goto(candidate.detailUrl, {
      waitUntil: "domcontentloaded",
      timeout: timeoutMs,
    });
    if (response === null || response.status() !== 200) {
      throw new Error("Citizenserve permit detail did not return HTTP 200");
    }
    if (await hasVisiblePasswordField(detailPage)) {
      throw new Error(
        "Citizenserve detail requires login; credentials will not be used",
      );
    }
    return parseCitizenservePermitDetailHtml(await detailPage.content(), {
      config,
      query,
      searchPage,
      searchUrl,
      candidate,
    });
  } finally {
    await detailPage.close().catch(() => undefined);
  }
}

/**
 * Validate hard source-request ceilings.
 *
 * @param {object} value - Candidate limits.
 * @param {number} value.maxPages - Search page ceiling.
 * @param {number} value.maxDetails - Detail ceiling.
 * @param {number} value.searchDelayMs - Inter-page delay.
 * @param {number} value.detailDelayMs - Inter-detail delay.
 * @param {number} value.timeoutMs - Browser/source timeout.
 * @returns {void}
 */
function validateCitizenserveLimits({
  maxPages,
  maxDetails,
  searchDelayMs,
  detailDelayMs,
  timeoutMs,
}) {
  if (
    !Number.isInteger(maxPages) ||
    maxPages < 1 ||
    maxPages > MAX_CITIZENSERVE_PAGES
  ) {
    throw new Error(
      `Citizenserve maxPages must be from 1 through ${String(MAX_CITIZENSERVE_PAGES)}`,
    );
  }
  if (
    !Number.isInteger(maxDetails) ||
    maxDetails < 1 ||
    maxDetails > MAX_CITIZENSERVE_DETAILS
  ) {
    throw new Error(
      `Citizenserve maxDetails must be from 1 through ${String(MAX_CITIZENSERVE_DETAILS)}`,
    );
  }
  if (!Number.isInteger(searchDelayMs) || searchDelayMs < MIN_SEARCH_DELAY_MS) {
    throw new Error(
      `Citizenserve searchDelayMs must be at least ${String(MIN_SEARCH_DELAY_MS)}`,
    );
  }
  if (!Number.isInteger(detailDelayMs) || detailDelayMs < MIN_DETAIL_DELAY_MS) {
    throw new Error(
      `Citizenserve detailDelayMs must be at least ${String(MIN_DETAIL_DELAY_MS)}`,
    );
  }
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1_000) {
    throw new Error("Citizenserve timeoutMs must be at least 1000");
  }
}

/**
 * Assert supplied resume state belongs to the active source/query.
 *
 * @param {PermitAdapterCheckpoint} checkpoint - Candidate state.
 * @param {string} sourceSystem - Active source key.
 * @param {PermitSearchQuery} query - Active property query.
 * @returns {void}
 */
function assertCheckpointMatches(checkpoint, sourceSystem, query) {
  if (
    checkpoint.sourceSystem !== sourceSystem ||
    checkpoint.query.kind !== query.kind ||
    checkpoint.query.value !== query.value
  ) {
    throw new Error("Citizenserve checkpoint does not match source/query");
  }
}

/**
 * Resolve local Chrome without downloading or repairing browsers.
 *
 * @returns {string | null} Configured/Puppeteer Chrome path or `null`.
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
 * Detect an active authentication form without rejecting hidden account modals
 * embedded in otherwise public Citizenserve pages.
 *
 * @param {import("puppeteer").Page} page - Rendered public page.
 * @returns {Promise<boolean>} Whether a visible password field is present.
 */
async function hasVisiblePasswordField(page) {
  return page.evaluate(() =>
    [...document.querySelectorAll("input[type='password']")].some(
      (element) =>
        element instanceof HTMLInputElement &&
        element.offsetParent !== null &&
        getComputedStyle(element).visibility !== "hidden",
    ),
  );
}
