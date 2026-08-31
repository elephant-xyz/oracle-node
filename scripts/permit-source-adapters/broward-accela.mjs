// @ts-check

import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

import * as cheerio from "cheerio";
import { parse as parseCsv } from "csv-parse/sync";

import { normalizeBrowardFolio } from "../broward-folio.mjs";
import {
  cleanRecordStatus,
  collapseText,
  createBrowser,
  createConfiguredPage,
  htmlToText,
  parseCompletedInspections,
  parseMoreDetails,
  parseResultSummary,
  safeKeyPart,
  shortHash,
  toAccelaDate,
} from "../../workflow/lambdas/permit-harvest-worker/lee-accela.mjs";

/**
 * @typedef {"hollywood" | "plantation" | "fort-lauderdale" | "cooper-city" | "weston"} BrowardAccelaJurisdiction
 */

/**
 * @typedef {"unknown_not_certified" | "separate_official_source" | "official_microfilm_route" | "outside_city_record_coverage"} HistoricalDisposition
 */

/**
 * @typedef {object} HistoricalCutoff
 * @property {string | null} date - ISO date at which the documented source boundary begins, or `null` when the official sources do not certify a date.
 * @property {HistoricalDisposition} disposition - Required treatment for records before the date; an unknown date is never interpreted as all-history coverage.
 * @property {string} note - Human-readable source-boundary evidence and operational restriction.
 */

/**
 * @typedef {object} SeparateHistoricalSource
 * @property {string} sourceSystem - Distinct source identity; records from this source must not be mislabeled as Accela records.
 * @property {string} portalUrl - Official public historical portal or custodian route.
 * @property {"address"} searchMethod - Public search key supported by the historical source.
 * @property {string} coverageStartDate - Earliest date stated by the official source.
 * @property {string | null} coverageEndDate - Last certified date, or `null` where overlap with Accela has not been certified.
 * @property {string} note - Operational separation and archive guidance.
 */

/**
 * @typedef {object} BrowardAccelaSource
 * @property {BrowardAccelaJurisdiction} key - Stable CLI/config key.
 * @property {string} jurisdiction - Official jurisdiction display name.
 * @property {string} agencyCode - Accela agency path/code, never inherited from Lee County.
 * @property {string} module - Accela module and tab used for public permit searches.
 * @property {string} portalUrl - Fully qualified anonymous general-search URL.
 * @property {string} sourceSystem - Jurisdiction-specific source identity used in normalized permit records.
 * @property {string} officialEvidenceUrl - Official source/custodian URL documented in the Broward registry.
 * @property {string | null} contentFrameName - Named frame containing Accela when a municipal wrapper page embeds Citizen Access.
 * @property {readonly string[]} pilotParcels - One or two already-validated Broward appraisal folios for bounded source probes.
 * @property {HistoricalCutoff} historicalCutoff - Explicit pre-source treatment; `null` date means unknown, not unlimited history.
 * @property {SeparateHistoricalSource | null} separateHistoricalSource - Separately queried historical source, if one is documented.
 */

/**
 * @typedef {object} Logger
 * @property {(message: string, details?: Record<string, unknown>) => void} info - Emit an informational event.
 * @property {(message: string, details?: Record<string, unknown>) => void} warn - Emit a warning event.
 * @property {(message: string, details?: Record<string, unknown>) => void} error - Emit an error event.
 */

/**
 * @typedef {object} BrowardAccelaPermitLink
 * @property {string} recordNumber - Record number displayed by the jurisdiction's Accela result page.
 * @property {string} url - Absolute official Accela detail URL.
 * @property {string | null} address - Search-result address, when present.
 * @property {string | null} description - Search-result description or project name, when present.
 * @property {string | null} status - Search-result status, when present.
 * @property {string | null} recordType - Search-result record type, when present.
 * @property {string} sourceSearchKey - Stable jurisdiction-and-folio search key.
 * @property {number} sourcePage - One-based result page on which the record was discovered.
 */

/**
 * @typedef {object} BrowardAccelaSearchPage
 * @property {number} pageNumber - One-based Accela result page.
 * @property {string} url - Final browser URL for the captured page.
 * @property {string | null} resultSummary - Parsed Accela result summary or `detail page`.
 * @property {string} html - Complete source HTML captured before pagination.
 */

/**
 * @typedef {object} BrowardAccelaParcelSearchResult
 * @property {"records" | "no_records" | "non_permit_records_only"} status - Explicit successful source outcome.
 * @property {string} searchKey - Stable jurisdiction-and-folio search identity.
 * @property {string} parcelIdentifier - Exact normalized 12-character Broward folio submitted to Accela.
 * @property {BrowardAccelaSource} source - Jurisdiction-specific Accela source configuration.
 * @property {BrowardAccelaPermitLink[]} permits - Deduplicated permit detail links.
 * @property {BrowardAccelaSearchPage[]} pages - Raw list/detail-redirect captures.
 * @property {number | null} reportedTotal - Accela's reported result count, when displayed.
 * @property {number} excludedNonPermitCount - Cross-module records (for example code enforcement) explicitly excluded from permit normalization.
 */

/**
 * @typedef {object} BrowardAccelaDateWindowSearchResult
 * @property {"records" | "no_records"} status - Explicit public source outcome.
 * @property {string} searchKey - Stable jurisdiction/date-window identity.
 * @property {string} startDate - Inclusive ISO source start date.
 * @property {string} endDate - Inclusive ISO source end date.
 * @property {BrowardAccelaSource} source - Jurisdiction source configuration.
 * @property {BrowardAccelaPermitLink[]} permits - Deduplicated list records.
 * @property {BrowardAccelaSearchPage[]} pages - Raw private page captures.
 * @property {number | null} reportedTotal - Source-reported total when visible.
 * @property {number} excludedNonPermitCount - Explicit cross-module rows.
 * @property {boolean} truncatedForSplit - Whether a dense multi-day window intentionally stopped after page one.
 */

/**
 * @typedef {object} BrowardAccelaCsvPermitRecord
 * @property {"oracle-node.broward-accela-csv-list.v1"} schemaVersion - CSV list contract.
 * @property {string} sourceSystem - Jurisdiction source identity.
 * @property {string} jurisdiction - Issuing jurisdiction.
 * @property {string} recordNumber - Full exported permit number.
 * @property {string} sourceUrl - Official Accela detail lookup.
 * @property {string} recordKey - Detail-compatible source key.
 * @property {string | null} recordDate - Exported ISO record date.
 * @property {string | null} recordType - Exported record type.
 * @property {string | null} projectName - Exported project name.
 * @property {string | null} address - Exported work address.
 * @property {string | null} expirationDate - Exported ISO expiration date.
 * @property {string | null} status - Exported status.
 * @property {boolean} isRoofPermit - Conservative list classification.
 * @property {string} sourceWindowKey - Inclusive date-window identity.
 *
 * @typedef {object} BrowardAccelaCsvWindowResult
 * @property {string} startDate - Inclusive ISO start.
 * @property {string} endDate - Inclusive ISO end.
 * @property {string} sourceWindowKey - Stable source/window identity.
 * @property {number | null} displayedTotal - Untrusted/capped UI total.
 * @property {boolean} displayedTotalCapped - Whether UI total equals known cap 100.
 * @property {readonly BrowardAccelaCsvPermitRecord[]} records - Exported unique records.
 * @property {string} rawCsv - Exact official export bytes as UTF-8.
 * @property {string} rawSearchHtml - First result/no-record page.
 */

/**
 * @typedef {object} BrowardAccelaPermitRecord
 * @property {"permit-harvest.accela.v1"} schemaVersion - Existing Accela permit-detail artifact contract.
 * @property {string} source - Jurisdiction-specific source system.
 * @property {string} sourceSystem - Jurisdiction-specific source system, repeated for local normalized-record consumers.
 * @property {string} jurisdiction - Issuing jurisdiction configured for this official portal.
 * @property {string} retrievedAt - ISO retrieval timestamp.
 * @property {string} sourceUrl - Final official Accela detail URL.
 * @property {string} recordNumber - Reconciled Accela record number.
 * @property {string | null} recordType - Public record type.
 * @property {string | null} recordStatus - Public record status.
 * @property {string | null} workLocation - Public work-location text.
 * @property {string} parcelIdentifier - Exact submitted 12-character Broward folio.
 * @property {string | null} sourceParcelIdentifier - Parcel number printed on the detail page, if present.
 * @property {string | null} applicant - Public applicant block.
 * @property {string | null} licensedProfessional - Public licensed-professional block.
 * @property {string | null} projectDescription - Public project description.
 * @property {Record<string, string>} moreDetails - Parsed public Accela detail fields.
 * @property {string | null} moreDetailsRawText - Raw More Details section.
 * @property {string | null} inspectionsRawText - Raw Inspections section.
 * @property {import("../../workflow/lambdas/permit-harvest-worker/lee-accela.mjs").InspectionRecord[]} completedInspections - Completed public inspections parsed by the shared Accela parser.
 * @property {string | null} processingStatusRawText - Raw Processing Status section.
 * @property {import("../../workflow/lambdas/permit-harvest-worker/lee-accela.mjs").ExtractedLink[]} documentLinks - Public document/ePlan links.
 * @property {import("../../workflow/lambdas/permit-harvest-worker/lee-accela.mjs").ExtractedLink[]} relatedLinks - Other public detail links.
 * @property {string} rawText - Complete collapsed detail text.
 * @property {BrowardAccelaPermitLink} sourceSearchResult - Exact search-result evidence that led to this detail.
 * @property {string} idempotencyKey - Jurisdiction-scoped deterministic permit identity.
 * @property {{ searchMethod: "public_anonymous_parcel", anonymous: true, submittedParcelIdentifier: string, searchUrl: string, searchKey: string, resultPage: number, agencyCode: string, module: string, officialEvidenceUrl: string, historicalCutoff: HistoricalCutoff, separateHistoricalSource: SeparateHistoricalSource | null }} provenance - Auditable search and source-boundary provenance.
 */

/**
 * @typedef {object} BrowardAccelaDetailCapture
 * @property {string} html - Complete public Accela detail HTML.
 * @property {BrowardAccelaPermitRecord} record - Normalized permit-detail record.
 */

/**
 * @typedef {"access_blocked" | "source_error" | "unexpected_response" | "incomplete_pagination" | "identity_mismatch"} BrowardAccelaErrorCode
 */

/**
 * @typedef {object} BrowardAccelaCheckpointDetail
 * @property {string} capturePath - Local raw HTML capture path.
 * @property {BrowardAccelaPermitRecord} record - Normalized captured permit record.
 */

/**
 * @typedef {object} BrowardAccelaCheckpointTarget
 * @property {"in_progress" | "records" | "no_records" | "non_permit_records_only" | "failed"} status - Resume state for one jurisdiction/folio target.
 * @property {BrowardAccelaJurisdiction} jurisdictionKey - Source configuration key.
 * @property {string} parcelIdentifier - Exact submitted Broward folio.
 * @property {string} searchKey - Stable source search key.
 * @property {string | null} startedAt - First-attempt timestamp.
 * @property {string | null} completedAt - Successful completion timestamp.
 * @property {number | null} reportedTotal - Source-reported total.
 * @property {number} excludedNonPermitCount - Cross-module records observed and excluded.
 * @property {BrowardAccelaPermitLink[]} permits - Search-result links retained for detail-level resume.
 * @property {Record<string, BrowardAccelaCheckpointDetail>} details - Successfully captured details keyed by record number.
 * @property {string[]} searchCapturePaths - Local raw result-page capture paths.
 * @property {{ code: string, message: string, url: string | null, failedAt: string } | null} error - Explicit source/access failure; never represented as no records.
 */

/**
 * @typedef {object} BrowardAccelaCheckpoint
 * @property {"broward-accela-local-checkpoint.v1"} schemaVersion - Checkpoint schema marker.
 * @property {string} updatedAt - Latest atomic update timestamp.
 * @property {Record<string, BrowardAccelaCheckpointTarget>} targets - Target states keyed by jurisdiction and folio.
 */

const DEFAULT_SELECTORS = Object.freeze({
  parcel: "#ctl00_PlaceHolderMain_generalSearchForm_txtGSParcelNo",
  startDate: "#ctl00_PlaceHolderMain_generalSearchForm_txtGSStartDate",
  endDate: "#ctl00_PlaceHolderMain_generalSearchForm_txtGSEndDate",
  submit: "#ctl00_PlaceHolderMain_btnNewSearch",
});

const NO_RECORDS_PATTERN =
  /Your search returned no results|No records found|No records match your search criteria/i;
const ACCESS_BLOCK_PATTERN =
  /access denied|captcha|verify you are human|sign in to continue|login is required|request rejected|cloudflare/i;
const SOURCE_ERROR_PATTERN =
  /technical difficulties|unable to proceed|Object reference not set|String was not recognized|error\(s\) occurred on current page|an unexpected error (?:has )?occurred|temporarily unavailable/i;
const RECORD_NUMBER_PATTERN = /^[A-Z0-9][A-Z0-9./_-]{1,79}$/i;
const ROOF_PERMIT_PATTERN = /\broof(?:ing)?\b/iu;

/**
 * Classify a result-list permit as roofing before opening its detail page.
 *
 * @param {BrowardAccelaPermitLink} permit - Reconciled Accela search result.
 * @returns {boolean} True only when public list text explicitly identifies roofing.
 */
export function isBrowardAccelaRoofPermitCandidate(permit) {
  return ROOF_PERMIT_PATTERN.test(
    [
      permit.recordNumber,
      permit.recordType,
      permit.description,
    ]
      .filter((value) => typeof value === "string")
      .join(" "),
  );
}

/**
 * Official source-specific configuration. Dates are deliberately conservative:
 * a `null` cutoff records that no date is certified instead of claiming all
 * history. Hollywood's address-only BCLA portal has a separate source identity
 * and is never folded into current Accela output.
 *
 * @type {Readonly<Record<BrowardAccelaJurisdiction, BrowardAccelaSource>>}
 */
export const BROWARD_ACCELA_SOURCES = Object.freeze({
  hollywood: Object.freeze({
    key: "hollywood",
    jurisdiction: "Hollywood",
    agencyCode: "HOLLYWOOD",
    module: "Building",
    portalUrl:
      "https://aca-prod.accela.com/HOLLYWOOD/Cap/CapHome.aspx?module=Building&TabName=Building",
    sourceSystem: "broward_hollywood_accela_permits",
    officialEvidenceUrl:
      "https://apps.hollywoodfl.org/building/PermitStatus.aspx",
    contentFrameName: null,
    pilotParcels: Object.freeze(["514111160200", "514207022070"]),
    historicalCutoff: Object.freeze({
      date: null,
      disposition: "separate_official_source",
      note: "The migration boundary between current Accela and the separate Hollywood BCLA address search is not certified; never infer all-history Accela coverage.",
    }),
    separateHistoricalSource: Object.freeze({
      sourceSystem: "broward_hollywood_bcla_legacy_permits",
      portalUrl: "https://apps.hollywoodfl.org/building/PermitStatus.aspx",
      searchMethod: "address",
      coverageStartDate: "1988-01-01",
      coverageEndDate: null,
      note: "Official address search states 1988-present coverage. Pre-1988 records require City archives. Query and label this source separately because no Accela/BCLA migration cutoff is certified.",
    }),
  }),
  plantation: Object.freeze({
    key: "plantation",
    jurisdiction: "Plantation",
    agencyCode: "PLANTATION",
    module: "Building",
    portalUrl:
      "https://aca.plantation.org/CitizenAccess/Cap/CapHome.aspx?TabName=Building&module=Building",
    sourceSystem: "broward_plantation_accela_permits",
    officialEvidenceUrl:
      "https://aca.plantation.org/CitizenAccess/Cap/CapHome.aspx?TabName=Building&module=Building",
    contentFrameName: "ACAFrame",
    pilotParcels: Object.freeze(["504108BJ0140"]),
    historicalCutoff: Object.freeze({
      date: "2004-01-01",
      disposition: "official_microfilm_route",
      note: "The public portal warns that records prior to 2004 may not be available online and directs users to the City microfilm department; absence before this boundary is not a no-permit claim.",
    }),
    separateHistoricalSource: null,
  }),
  "fort-lauderdale": Object.freeze({
    key: "fort-lauderdale",
    jurisdiction: "Fort Lauderdale",
    agencyCode: "FTL",
    module: "Permits",
    portalUrl:
      "https://aca-prod.accela.com/FTL/Cap/CapHome.aspx?module=Permits&TabName=Permits",
    sourceSystem: "broward_fort_lauderdale_lauderbuild_permits",
    officialEvidenceUrl: "https://aca3.accela.com/FTL/",
    contentFrameName: null,
    pilotParcels: Object.freeze(["494209060010", "494212072320"]),
    historicalCutoff: Object.freeze({
      date: null,
      disposition: "unknown_not_certified",
      note: "LauderBuild basic search is public, but the checked official source does not certify an earliest online date.",
    }),
    separateHistoricalSource: null,
  }),
  "cooper-city": Object.freeze({
    key: "cooper-city",
    jurisdiction: "Cooper City",
    agencyCode: "COOPER",
    module: "Building",
    portalUrl:
      "https://aca-prod.accela.com/COOPER/Cap/CapHome.aspx?module=Building&TabName=Building",
    sourceSystem: "broward_cooper_city_accela_permits",
    officialEvidenceUrl: "https://aca-prod.accela.com/COOPER/",
    contentFrameName: null,
    pilotParcels: Object.freeze(["514106100100"]),
    historicalCutoff: Object.freeze({
      date: null,
      disposition: "unknown_not_certified",
      note: "The public portal exposes historical record types but no certified earliest online date; do not convert no results into an all-history claim.",
    }),
    separateHistoricalSource: null,
  }),
  weston: Object.freeze({
    key: "weston",
    jurisdiction: "Weston",
    agencyCode: "WESTON",
    module: "Building",
    portalUrl:
      "https://aca-prod.accela.com/weston/Cap/CapHome.aspx?TabName=Building&module=Building",
    sourceSystem: "broward_weston_accela_permits",
    officialEvidenceUrl:
      "https://www.westonfl.org/government/building-code-services",
    contentFrameName: null,
    pilotParcels: Object.freeze(["503912010490"]),
    historicalCutoff: Object.freeze({
      date: "1997-01-01",
      disposition: "outside_city_record_coverage",
      note: "Official City source documentation bounds City permit history to post-1997 records; pre-1997 absence is outside this adapter's coverage.",
    }),
    separateHistoricalSource: null,
  }),
});

/**
 * Structured source error that preserves a stable distinction between public
 * access blocks, official source errors, unexpected pages, incomplete
 * pagination, and source-identity mismatches.
 */
export class BrowardAccelaSourceError extends Error {
  /**
   * @param {BrowardAccelaErrorCode} code - Stable failure category.
   * @param {BrowardAccelaSource} source - Jurisdiction source configuration.
   * @param {string} message - Human-readable failure explanation.
   * @param {string | null} [url] - Final source URL when available.
   * @param {string | null} [responseHtml] - Raw source HTML retained in memory so the local runner can write failure evidence.
   */
  constructor(code, source, message, url = null, responseHtml = null) {
    super(message);
    this.name = "BrowardAccelaSourceError";
    this.code = code;
    this.sourceKey = source.key;
    this.url = url;
    this.responseHtml = responseHtml;
  }
}

/**
 * Return a source config by stable jurisdiction key.
 *
 * @param {unknown} value - Candidate jurisdiction key.
 * @returns {BrowardAccelaSource} Frozen source config.
 */
export function readBrowardAccelaSource(value) {
  if (typeof value !== "string" || !(value in BROWARD_ACCELA_SOURCES)) {
    throw new Error(
      `Unknown Broward Accela jurisdiction: ${typeof value === "string" ? value : String(value)}`,
    );
  }
  return BROWARD_ACCELA_SOURCES[
    /** @type {BrowardAccelaJurisdiction} */ (value)
  ];
}

/**
 * Normalize a Broward permit-search folio without Lee STRAP assumptions.
 *
 * Only strings are accepted so leading zeroes and condo letters cannot already
 * have been lost to numeric coercion. The canonical 12-character form and the
 * appraiser's documented 6-2-4 display form are accepted; all other
 * punctuation and padding fail closed.
 *
 * @param {unknown} value - Raw Broward folio from CLI or a validated manifest.
 * @returns {string} Exact uppercase 12-character alphanumeric folio.
 */
export function normalizeBrowardPermitFolio(value) {
  if (typeof value !== "string") {
    throw new Error("Broward permit folio must be supplied as a string");
  }
  const trimmed = value.trim().toUpperCase();
  if (
    !/^[A-Z0-9]{12}$/.test(trimmed) &&
    !/^[A-Z0-9]{6}-[A-Z0-9]{2}-[A-Z0-9]{4}$/.test(trimmed)
  ) {
    throw new Error(
      `Broward permit folio must be exactly 12 alphanumeric characters (optional 6-2-4 display dashes): ${value}`,
    );
  }
  const normalized = normalizeBrowardFolio(trimmed);
  if (normalized === undefined) {
    throw new Error(`Invalid Broward permit folio: ${value}`);
  }
  return normalized;
}

/**
 * Build the stable checkpoint/capture identity for one jurisdiction and folio.
 *
 * @param {BrowardAccelaSource} source - Jurisdiction-specific source.
 * @param {string} parcelIdentifier - Canonical Broward folio.
 * @returns {string} Stable key with no implied countywide agency.
 */
export function buildBrowardAccelaSearchKey(source, parcelIdentifier) {
  return `${source.key}:parcel:${normalizeBrowardPermitFolio(parcelIdentifier)}`;
}

/**
 * Validate a real ISO calendar date without timezone inference.
 *
 * @param {string} value - Candidate YYYY-MM-DD value.
 * @param {string} fieldName - Field name used in errors.
 * @returns {string} Validated date.
 */
function requireIsoDate(value, fieldName) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/u.exec(value);
  if (match === null) {
    throw new Error(`Broward Accela ${fieldName} must be YYYY-MM-DD`);
  }
  const date = new Date(
    Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])),
  );
  if (
    date.getUTCFullYear() !== Number(match[1]) ||
    date.getUTCMonth() !== Number(match[2]) - 1 ||
    date.getUTCDate() !== Number(match[3])
  ) {
    throw new Error(`Broward Accela ${fieldName} is not a calendar date`);
  }
  return value;
}

/**
 * Build a stable source key for one inclusive jurisdiction date window.
 *
 * @param {BrowardAccelaSource} source - Jurisdiction-specific source.
 * @param {string} startDate - Inclusive ISO start date.
 * @param {string} endDate - Inclusive ISO end date.
 * @returns {string} Stable date-window identity.
 */
export function buildBrowardAccelaDateWindowKey(
  source,
  startDate,
  endDate,
) {
  const start = requireIsoDate(startDate, "startDate");
  const end = requireIsoDate(endDate, "endDate");
  if (Date.parse(`${end}T00:00:00Z`) < Date.parse(`${start}T00:00:00Z`)) {
    throw new Error("Broward Accela endDate must not precede startDate");
  }
  return `${source.key}:date:${start.replaceAll("-", "")}_${end.replaceAll("-", "")}`;
}

/**
 * Classify a loaded Accela page before interpreting zero records. Access and
 * source errors take precedence over no-result text so mixed error templates
 * cannot silently become successful empties.
 *
 * @param {string} html - Complete Accela page HTML.
 * @returns {"access_blocked" | "source_error" | "no_records" | "records" | "unknown"} Page classification.
 */
export function classifyBrowardAccelaPage(html) {
  const text = htmlToText(html);
  if (ACCESS_BLOCK_PATTERN.test(text)) return "access_blocked";
  if (SOURCE_ERROR_PATTERN.test(text)) return "source_error";
  const $ = cheerio.load(html);
  if (
    $("[id*='gdvPermitList']").length > 0 ||
    $("[id*='divRecordStatus']").length > 0 ||
    $("a[href*='CapDetail.aspx']").length > 0 ||
    parseResultSummary(text).total !== null
  ) {
    return "records";
  }
  if (NO_RECORDS_PATTERN.test(text)) return "no_records";
  return "unknown";
}

/**
 * Convert an official Accela relative URL to the configured jurisdiction host
 * and reject links that escape that host.
 *
 * @param {string} href - Source href or current page URL.
 * @param {BrowardAccelaSource} source - Jurisdiction source configuration.
 * @returns {string} Absolute official URL.
 */
function normalizeSourceUrl(href, source) {
  const absolute = new URL(href, source.portalUrl);
  const configured = new URL(source.portalUrl);
  if (absolute.origin !== configured.origin) {
    throw new BrowardAccelaSourceError(
      "unexpected_response",
      source,
      `Accela link escaped configured source origin: ${absolute.toString()}`,
      absolute.toString(),
    );
  }
  return absolute.toString();
}

/**
 * Read and validate the visible record number from a CapDetail result anchor.
 *
 * @param {unknown} value - Candidate anchor/cell text.
 * @returns {string | null} Uppercase Accela record number, or `null`.
 */
function readRecordNumber(value) {
  const recordNumber = collapseText(value).toUpperCase();
  return RECORD_NUMBER_PATTERN.test(recordNumber) && /\d/.test(recordNumber)
    ? recordNumber
    : null;
}

/**
 * Construct an official Accela detail route from a complete hidden RecordId.
 *
 * Some temporary/in-process records are included in Accela's reported total
 * but are rendered without an anchor. Their row still carries the complete
 * three-part cap identity in `input#RecordId`; retaining that identity keeps
 * list reconciliation complete without guessing from the permit number.
 *
 * @param {string} recordId - Three-part Accela cap identity.
 * @param {BrowardAccelaSource} source - Jurisdiction source configuration.
 * @returns {string | null} Official detail route or null for malformed identity.
 */
export function buildBrowardAccelaDetailUrlFromRecordId(recordId, source) {
  const match =
    /^([A-Z0-9]+)-([A-Z0-9]+)-([A-Z0-9]+)$/iu.exec(recordId.trim());
  if (match === null) return null;
  const url = new URL("./CapDetail.aspx", source.portalUrl);
  url.searchParams.set("Module", source.module);
  url.searchParams.set("TabName", source.module);
  url.searchParams.set("capID1", match[1] ?? "");
  url.searchParams.set("capID2", match[2] ?? "");
  url.searchParams.set("capID3", match[3] ?? "");
  url.searchParams.set("agencyCode", source.agencyCode);
  url.searchParams.set("IsToShowInspection", "");
  return url.toString();
}

/**
 * Return true when a candidate CapDetail link belongs to the detail page's
 * related-record tree instead of the scoped search results.
 *
 * @param {cheerio.Cheerio<import("domhandler").AnyNode>} table - Wrapping table.
 * @returns {boolean} Whether the table is a related-record tree.
 */
function isRelatedRecordTable(table) {
  const id = table.attr("id") ?? "";
  const caption = collapseText(table.find("caption").first().text());
  return /tableCapTreeList/i.test(id) || /^Related Records$/i.test(caption);
}

/**
 * Extract jurisdiction-neutral Accela CapDetail links from one result page.
 *
 * @param {object} params - Result parsing parameters.
 * @param {string} params.html - Complete result HTML.
 * @param {BrowardAccelaSource} params.source - Jurisdiction source.
 * @param {string} params.searchKey - Stable parcel search key.
 * @param {number} params.pageNumber - One-based result page.
 * @returns {BrowardAccelaPermitLink[]} Reconciled detail links in source order.
 */
export function extractBrowardAccelaPermitLinks({
  html,
  source,
  searchKey,
  pageNumber,
}) {
  const $ = cheerio.load(html);
  /** @type {BrowardAccelaPermitLink[]} */
  const links = [];

  $("a[href*='CapDetail.aspx']").each((_, element) => {
    const anchor = $(element);
    const href = anchor.attr("href");
    if (href === undefined) return;
    const row = anchor.closest("tr");
    if (isRelatedRecordTable(row.closest("table"))) return;
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
     * @param {readonly string[]} labels - Accepted lowercase Accela headers.
     * @param {number} fallbackIndex - Standard Accela grid fallback index.
     * @returns {string | null} Cell text when present.
     */
    const cellByHeader = (labels, fallbackIndex) => {
      const index = headers.findIndex((header) => labels.includes(header));
      const value = cells[index >= 0 ? index : fallbackIndex];
      return value === undefined || value.length === 0 ? null : value;
    };
    const recordNumber =
      readRecordNumber(anchor.text()) ??
      readRecordNumber(
        cellByHeader(["record number", "record no.", "record #"], 1),
      );
    if (recordNumber === null) return;
    const url = normalizeSourceUrl(href, source);
    const detailModule = new URL(url).searchParams.get("Module");
    if (
      detailModule !== null &&
      detailModule.toLowerCase() !== source.module.toLowerCase()
    ) {
      return;
    }
    links.push({
      recordNumber,
      url,
      address: cellByHeader(["address", "work location"], 2),
      description: cellByHeader(["description", "project name"], 3),
      status: cellByHeader(["status", "record status"], 4),
      recordType: cellByHeader(["record type", "type"], -1),
      sourceSearchKey: searchKey,
      sourcePage: pageNumber,
    });
  });

  const linkedRecordNumbers = new Set(
    links.map((link) => link.recordNumber.toUpperCase()),
  );
  $("[id*='gdvPermitList'] tr").each((_, element) => {
    const row = $(element);
    if (row.find("a[href*='CapDetail.aspx']").length > 0) return;
    const hiddenRecordId = collapseText(
      row.find("input[id='RecordId']").first().attr("value"),
    );
    const url = buildBrowardAccelaDetailUrlFromRecordId(
      hiddenRecordId,
      source,
    );
    const recordNumber = readRecordNumber(
      row
        .find("[id$='_lblPermitNumber'],[id$='_lblPermitNumber1']")
        .first()
        .text(),
    );
    if (
      url === null ||
      recordNumber === null ||
      linkedRecordNumbers.has(recordNumber.toUpperCase())
    ) {
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
     * @param {readonly string[]} labels - Accepted lowercase headers.
     * @param {number} fallbackIndex - Standard grid fallback index.
     * @returns {string | null} Matching list value.
     */
    const cellByHeader = (labels, fallbackIndex) => {
      const index = headers.findIndex((header) => labels.includes(header));
      const value = cells[index >= 0 ? index : fallbackIndex];
      return value === undefined || value.length === 0 ? null : value;
    };
    links.push({
      recordNumber,
      url,
      address: cellByHeader(["address", "work location"], 5),
      description: cellByHeader(["description", "project name"], 4),
      status: cellByHeader(["status", "record status"], 7),
      recordType: cellByHeader(["record type", "type"], 3),
      sourceSearchKey: searchKey,
      sourcePage: pageNumber,
    });
    linkedRecordNumbers.add(recordNumber.toUpperCase());
  });

  return links;
}

/**
 * Count result links whose explicit Accela `Module` differs from the configured
 * permit module. These records remain source-count provenance but are not
 * normalized as permits (for example Plantation Building Enforcement cases
 * returned alongside Building permits).
 *
 * @param {object} params - Result parsing parameters.
 * @param {string} params.html - Complete result HTML.
 * @param {BrowardAccelaSource} params.source - Jurisdiction permit source.
 * @returns {number} Number of cross-module detail links excluded.
 */
export function countBrowardAccelaExcludedModuleLinks({ html, source }) {
  const $ = cheerio.load(html);
  let excludedCount = 0;
  $("a[href*='CapDetail.aspx']").each((_, element) => {
    const anchor = $(element);
    const href = anchor.attr("href");
    if (href === undefined) return;
    if (isRelatedRecordTable(anchor.closest("tr").closest("table"))) return;
    const url = normalizeSourceUrl(href, source);
    const detailModule = new URL(url).searchParams.get("Module");
    if (
      detailModule !== null &&
      detailModule.toLowerCase() !== source.module.toLowerCase()
    ) {
      excludedCount += 1;
    }
  });
  return excludedCount;
}

/**
 * Parse a direct detail redirect returned for a single-record parcel search.
 *
 * @param {object} params - Detail-redirect parsing parameters.
 * @param {string} params.html - Current page HTML.
 * @param {string} params.pageUrl - Current browser URL.
 * @param {BrowardAccelaSource} params.source - Jurisdiction source.
 * @param {string} params.searchKey - Stable parcel search key.
 * @param {number} params.pageNumber - One-based captured page.
 * @returns {BrowardAccelaPermitLink | null} Direct result, or `null` for a list page.
 */
export function extractBrowardAccelaDirectDetailLink({
  html,
  pageUrl,
  source,
  searchKey,
  pageNumber,
}) {
  const $ = cheerio.load(html);
  const hasDetailMarker = $("[id*='divRecordStatus']").length > 0;
  if (!hasDetailMarker && $("[id*='gdvPermitList']").length > 0) return null;
  const text = htmlToText(html);
  const header =
    /Record\s+([A-Z0-9][A-Z0-9./_-]{1,79})\s*:\s*(.*?)\s+Record Status:/i.exec(
      text,
    );
  if (header === null) return null;
  const recordNumber = readRecordNumber(header[1]);
  if (recordNumber === null) return null;
  const formAction = $("form#aspnetForm").attr("action");
  const url = normalizeSourceUrl(
    /CapDetail\.aspx/i.test(pageUrl) ? pageUrl : (formAction ?? pageUrl),
    source,
  );
  const detailModule = new URL(url).searchParams.get("Module");
  if (
    detailModule !== null &&
    detailModule.toLowerCase() !== source.module.toLowerCase()
  ) {
    throw new BrowardAccelaSourceError(
      "identity_mismatch",
      source,
      `${source.jurisdiction} Accela redirected a ${source.module} parcel search to cross-module record ${recordNumber}`,
      url,
      html,
    );
  }
  return {
    recordNumber,
    url,
    address: matchCollapsedText(
      text,
      /Work Location\s+(.*?)\s+\*\s+Record Details/i,
    ),
    description: collapseText(header[2]) || null,
    status: cleanBrowardAccelaRecordStatus(
      matchCollapsedText(
        text,
        /Record Status:\s*(.*?)(?:Click here for more information|Create a New Collection|Add to Existing Collection|Record Info|Work Location)/i,
      ),
    ),
    recordType: collapseText(header[2]) || null,
    sourceSearchKey: searchKey,
    sourcePage: pageNumber,
  };
}

/**
 * Read one collapsed regex capture from normalized page text.
 *
 * @param {string} text - Text to search.
 * @param {RegExp} pattern - Pattern with a first capture group.
 * @returns {string | null} Collapsed first capture.
 */
function matchCollapsedText(text, pattern) {
  const match = pattern.exec(text);
  return match === null ? null : collapseText(match[1]);
}

/**
 * @typedef {import("puppeteer").Page | import("puppeteer").Frame} AccelaDomContext
 */

/**
 * Resolve the DOM context that owns the Citizen Access form. Plantation wraps
 * Accela in its named `ACAFrame`; the other configured jurisdictions render
 * the same form in the top-level page.
 *
 * @param {import("puppeteer").Page} page - Top-level browser page.
 * @param {BrowardAccelaSource} source - Jurisdiction source configuration.
 * @returns {Promise<AccelaDomContext>} Top-level page or configured Accela frame.
 */
async function resolveAccelaDomContext(page, source) {
  if (source.contentFrameName === null) return page;
  try {
    return await page.waitForFrame(
      (candidate) => candidate.name() === source.contentFrameName,
      { timeout: 45_000 },
    );
  } catch {
    throw new BrowardAccelaSourceError(
      "unexpected_response",
      source,
      `${source.jurisdiction} wrapper did not expose configured frame ${source.contentFrameName}`,
      page.url(),
      await page.content(),
    );
  }
}

/**
 * Set an Accela input through DOM events so ASP.NET masks and change tracking
 * receive the same value that is visibly present in the public form.
 *
 * @param {AccelaDomContext} context - Page or named Accela frame.
 * @param {string} selector - Accela form selector.
 * @param {string} value - Exact value to submit.
 * @returns {Promise<void>} Resolves after the value is set and verified.
 */
async function setAccelaInput(context, selector, value) {
  await context.waitForSelector(selector, { timeout: 45_000 });
  const observed = await context.evaluate(
    (inputSelector, inputValue) => {
      const element = document.querySelector(inputSelector);
      if (!(element instanceof HTMLInputElement)) return null;
      element.value = inputValue;
      element.dispatchEvent(new Event("input", { bubbles: true }));
      element.dispatchEvent(new Event("change", { bubbles: true }));
      element.blur();
      return element.value;
    },
    selector,
    value,
  );
  if (observed !== value) {
    throw new Error(
      `Accela form did not retain submitted value for ${selector}: ${String(observed)}`,
    );
  }
}

/**
 * Clear an optional Accela date field when the jurisdiction renders it. Some
 * modules, including LauderBuild Permits, omit date inputs entirely; absence
 * is valid because no hidden date constraint then needs to be removed.
 *
 * @param {AccelaDomContext} context - Page or named Accela frame.
 * @param {string} selector - Optional date-field selector.
 * @returns {Promise<void>} Resolves after the existing field is cleared.
 */
async function clearOptionalAccelaInput(context, selector) {
  if ((await context.$(selector)) !== null) {
    await setAccelaInput(context, selector, "");
  }
}

/**
 * Wait for an Accela search form or a known source/access failure.
 *
 * @param {AccelaDomContext} context - Current page or named Accela frame.
 * @returns {Promise<void>} Resolves when classification can proceed.
 */
async function waitForSearchFormOrFailure(context) {
  await context.waitForFunction(
    (parcelSelector) => {
      if (document.querySelector(parcelSelector) !== null) return true;
      const text = document.body?.innerText ?? "";
      return /access denied|captcha|verify you are human|sign in to continue|login is required|request rejected|cloudflare|technical difficulties|unable to proceed|Object reference not set|String was not recognized|temporarily unavailable/i.test(
        text,
      );
    },
    { timeout: 45_000 },
    DEFAULT_SELECTORS.parcel,
  );
}

/**
 * Wait until the submitted public search has produced a list, detail redirect,
 * explicit no-result marker, or explicit source/access failure.
 *
 * @param {AccelaDomContext} context - Current page or named Accela frame.
 * @returns {Promise<void>} Resolves only on a classifiable outcome.
 */
async function waitForSearchOutcome(context) {
  await context.waitForFunction(
    () => {
      const text = document.body?.innerText ?? "";
      return (
        document.querySelector("[id*='gdvPermitList']") !== null ||
        document.querySelector("[id*='divRecordStatus']") !== null ||
        /Showing\s+\d|Your search returned no results|No records found|No records match your search criteria|access denied|captcha|technical difficulties|unable to proceed|Object reference not set|error\(s\) occurred on current page|temporarily unavailable/i.test(
          text,
        )
      );
    },
    { timeout: 60_000 },
  );
}

/**
 * Throw a typed error for any classified page that is not a successful records
 * or no-records outcome.
 *
 * @param {string} html - Complete source HTML.
 * @param {BrowardAccelaSource} source - Jurisdiction source.
 * @param {string} url - Final browser URL.
 * @param {string} context - Search/detail context for diagnostics.
 * @returns {"records" | "no_records"} Successful classification.
 */
function requireSuccessfulPageClassification(html, source, url, context) {
  const classification = classifyBrowardAccelaPage(html);
  const excerpt = htmlToText(html).slice(0, 500);
  if (classification === "access_blocked") {
    throw new BrowardAccelaSourceError(
      "access_blocked",
      source,
      `${source.jurisdiction} Accela access blocked during ${context}: ${excerpt}`,
      url,
      html,
    );
  }
  if (classification === "source_error") {
    throw new BrowardAccelaSourceError(
      "source_error",
      source,
      `${source.jurisdiction} Accela returned an official error during ${context}: ${excerpt}`,
      url,
      html,
    );
  }
  if (classification === "unknown") {
    throw new BrowardAccelaSourceError(
      "unexpected_response",
      source,
      `${source.jurisdiction} Accela returned neither records nor an explicit no-records marker during ${context}: ${excerpt}`,
      url,
      html,
    );
  }
  return classification;
}

/**
 * Locate and click Accela's ASP.NET `Next >` result link.
 *
 * @param {AccelaDomContext} context - Current page or named Accela frame.
 * @returns {Promise<boolean>} Whether a next-page action was started.
 */
async function clickNextPage(context) {
  return context.evaluate(() => {
    const next = Array.from(document.querySelectorAll("a")).find(
      (anchor) =>
        (anchor.textContent ?? "").replace(/\s+/g, " ").trim() === "Next >" &&
        anchor.getAttribute("href")?.includes("__doPostBack"),
    );
    if (!(next instanceof HTMLAnchorElement)) return false;
    next.click();
    return true;
  });
}

/**
 * Determine whether the current result DOM exposes another page.
 *
 * @param {AccelaDomContext} context - Current page or named Accela frame.
 * @returns {Promise<boolean>} True when an ASP.NET `Next >` link is present.
 */
async function hasNextPage(context) {
  return context.evaluate(() =>
    Array.from(document.querySelectorAll("a")).some(
      (anchor) =>
        (anchor.textContent ?? "").replace(/\s+/g, " ").trim() === "Next >" &&
        anchor.getAttribute("href")?.includes("__doPostBack"),
    ),
  );
}

/**
 * Search one date-enabled Broward Accela agency without a parcel filter.
 *
 * A shared browser may call this repeatedly, giving each agency one persistent
 * worker while every window gets a fresh page/session form. Dense multi-day
 * windows may stop after page one so the caller can split them recursively.
 * A terminal window reconciles all visible pages against Accela's total.
 *
 * @param {object} params - Public date-window search parameters.
 * @param {import("puppeteer").Browser} params.browser - Reused browser.
 * @param {BrowardAccelaSource} params.source - Date-enabled jurisdiction.
 * @param {string} params.startDate - Inclusive ISO start date.
 * @param {string} params.endDate - Inclusive ISO end date.
 * @param {number} params.maxPages - Hard terminal pagination ceiling.
 * @param {number | undefined} [params.stopAfterFirstPageWhenTotalAtLeast]
 *   Dense-window split threshold.
 * @param {Logger} params.logger - Structured logger.
 * @returns {Promise<BrowardAccelaDateWindowSearchResult>} Reconciled source result.
 */
export async function searchBrowardAccelaDateWindow({
  browser,
  source,
  startDate,
  endDate,
  maxPages,
  stopAfterFirstPageWhenTotalAtLeast,
  logger,
}) {
  const searchKey = buildBrowardAccelaDateWindowKey(
    source,
    startDate,
    endDate,
  );
  if (!Number.isInteger(maxPages) || maxPages < 1 || maxPages > 200) {
    throw new Error("Broward Accela date-window maxPages must be 1 through 200");
  }
  if (
    stopAfterFirstPageWhenTotalAtLeast !== undefined &&
    (!Number.isInteger(stopAfterFirstPageWhenTotalAtLeast) ||
      stopAfterFirstPageWhenTotalAtLeast < 2)
  ) {
    throw new Error(
      "Broward Accela split threshold must be an integer of at least 2",
    );
  }
  const page = await createConfiguredPage(browser);
  /** @type {BrowardAccelaSearchPage[]} */
  const pages = [];
  /** @type {BrowardAccelaPermitLink[]} */
  const permits = [];
  let reportedTotal = /** @type {number | null} */ (null);
  let excludedNonPermitCount = 0;
  let truncatedForSplit = false;

  try {
    logger.info("broward_accela_date_window_open", {
      sourceKey: source.key,
      jurisdiction: source.jurisdiction,
      startDate,
      endDate,
      searchKey,
    });
    await page.goto(source.portalUrl, {
      waitUntil: "domcontentloaded",
      timeout: 60_000,
    });
    const context = await resolveAccelaDomContext(page, source);
    await waitForSearchFormOrFailure(context);
    const initialHtml = await context.content();
    const initialClassification = classifyBrowardAccelaPage(initialHtml);
    if (
      initialClassification === "access_blocked" ||
      initialClassification === "source_error"
    ) {
      requireSuccessfulPageClassification(
        initialHtml,
        source,
        context.url(),
        "date-window search form load",
      );
    }
    if (
      (await context.$(DEFAULT_SELECTORS.startDate)) === null ||
      (await context.$(DEFAULT_SELECTORS.endDate)) === null
    ) {
      throw new BrowardAccelaSourceError(
        "unexpected_response",
        source,
        `${source.jurisdiction} Accela does not expose start/end date controls`,
        context.url(),
        initialHtml,
      );
    }
    await setAccelaInput(context, DEFAULT_SELECTORS.parcel, "");
    await setAccelaInput(
      context,
      DEFAULT_SELECTORS.startDate,
      toAccelaDate(startDate),
    );
    await setAccelaInput(
      context,
      DEFAULT_SELECTORS.endDate,
      toAccelaDate(endDate),
    );
    // Accela commonly updates this form through an ASP.NET asynchronous
    // postback without a top-level navigation. Waiting for navigation here
    // burns the complete timeout after the result is already visible.
    await context.click(DEFAULT_SELECTORS.submit);
    await waitForSearchOutcome(context);

    for (let pageNumber = 1; pageNumber <= maxPages; pageNumber += 1) {
      const html = await context.content();
      const text = htmlToText(html);
      const classification = requireSuccessfulPageClassification(
        html,
        source,
        context.url(),
        `date-window search ${searchKey} page ${String(pageNumber)}`,
      );
      const directDetail = extractBrowardAccelaDirectDetailLink({
        html,
        pageUrl: context.url(),
        source,
        searchKey,
        pageNumber,
      });
      if (directDetail !== null) {
        pages.push({
          pageNumber,
          url: context.url(),
          resultSummary: "detail page",
          html,
        });
        permits.push(directDetail);
        reportedTotal = 1;
        break;
      }

      const parsedSummary = parseResultSummary(text);
      reportedTotal = reportedTotal ?? parsedSummary.total;
      pages.push({
        pageNumber,
        url: context.url(),
        resultSummary: parsedSummary.summary,
        html,
      });
      if (classification === "no_records") {
        if (pageNumber !== 1 || permits.length > 0) {
          throw new BrowardAccelaSourceError(
            "unexpected_response",
            source,
            `${source.jurisdiction} Accela mixed no-records and record pages`,
            context.url(),
            html,
          );
        }
        return {
          status: "no_records",
          searchKey,
          startDate,
          endDate,
          source,
          permits: [],
          pages,
          reportedTotal: 0,
          excludedNonPermitCount: 0,
          truncatedForSplit: false,
        };
      }

      const pageExcluded = countBrowardAccelaExcludedModuleLinks({
        html,
        source,
      });
      excludedNonPermitCount += pageExcluded;
      const pageLinks = extractBrowardAccelaPermitLinks({
        html,
        source,
        searchKey,
        pageNumber,
      });
      if (pageLinks.length === 0 && pageExcluded === 0) {
        throw new BrowardAccelaSourceError(
          "unexpected_response",
          source,
          `${source.jurisdiction} Accela exposed no list records for ${searchKey}`,
          context.url(),
          html,
        );
      }
      permits.push(...pageLinks);
      logger.info("broward_accela_date_window_page_captured", {
        sourceKey: source.key,
        startDate,
        endDate,
        pageNumber,
        pagePermitCount: pageLinks.length,
        pageExcludedNonPermitCount: pageExcluded,
        reportedTotal,
      });

      if (
        pageNumber === 1 &&
        stopAfterFirstPageWhenTotalAtLeast !== undefined &&
        reportedTotal !== null &&
        reportedTotal >= stopAfterFirstPageWhenTotalAtLeast
      ) {
        truncatedForSplit = true;
        break;
      }
      const nextAvailable = await hasNextPage(context);
      if (!nextAvailable) break;
      if (pageNumber === maxPages) {
        throw new BrowardAccelaSourceError(
          "incomplete_pagination",
          source,
          `${source.jurisdiction} Accela exceeded date-window maxPages ${String(maxPages)}`,
          context.url(),
          html,
        );
      }
      const priorSummary = parsedSummary.summary;
      if (!(await clickNextPage(context))) {
        throw new BrowardAccelaSourceError(
          "incomplete_pagination",
          source,
          `${source.jurisdiction} Accela next-page control disappeared`,
          context.url(),
          html,
        );
      }
      await context.waitForFunction(
        (previousSummary) => {
          const bodyText = document.body?.innerText ?? "";
          if (
            /Your search returned no results|No records found|access denied|captcha|technical difficulties|unable to proceed|Object reference not set|error\(s\) occurred on current page/i.test(
              bodyText,
            )
          ) {
            return true;
          }
          const match =
            /Showing\s+([0-9,]+\s*-\s*[0-9,]+\s+of\s+[0-9,]+)/i.exec(
              bodyText,
            );
          const current =
            match === null ? null : match[1].replace(/\s+/g, " ").trim();
          return current !== null && current !== previousSummary;
        },
        { timeout: 60_000 },
        priorSummary,
      );
    }
  } finally {
    await page.close().catch(() => undefined);
  }

  /** @type {Map<string, BrowardAccelaPermitLink>} */
  const deduped = new Map();
  for (const permit of permits) {
    const identity = permit.url.toLowerCase();
    const existing = deduped.get(identity);
    if (
      existing !== undefined &&
      existing.recordNumber !== permit.recordNumber
    ) {
      throw new BrowardAccelaSourceError(
        "identity_mismatch",
        source,
        `${source.jurisdiction} Accela returned conflicting date-window identities`,
        permit.url,
      );
    }
    deduped.set(identity, permit);
  }
  const accounted = deduped.size + excludedNonPermitCount;
  if (
    !truncatedForSplit &&
    reportedTotal !== null &&
    accounted < reportedTotal
  ) {
    throw new BrowardAccelaSourceError(
      "incomplete_pagination",
      source,
      `${source.jurisdiction} Accela accounted for ${String(accounted)} of ${String(reportedTotal)} date-window records`,
      source.portalUrl,
    );
  }
  return {
    status: "records",
    searchKey,
    startDate,
    endDate,
    source,
    permits: [...deduped.values()],
    pages,
    reportedTotal,
    excludedNonPermitCount,
    truncatedForSplit,
  };
}

/**
 * Parse one official Accela CSV export into deterministic list records.
 *
 * @param {string} csvText - Exact UTF-8 export.
 * @param {BrowardAccelaSource} source - Jurisdiction source.
 * @param {string} startDate - Inclusive search start.
 * @param {string} endDate - Inclusive search end.
 * @returns {BrowardAccelaCsvPermitRecord[]} Unique exported records.
 */
export function parseBrowardAccelaCsvExport(
  csvText,
  source,
  startDate,
  endDate,
) {
  const sourceWindowKey = buildBrowardAccelaDateWindowKey(
    source,
    startDate,
    endDate,
  );
  const parsed = /** @type {unknown} */ (
    parseCsv(csvText, {
      bom: true,
      columns: true,
      skip_empty_lines: true,
      relax_column_count: true,
      trim: true,
    })
  );
  if (!Array.isArray(parsed)) {
    throw new Error(`${source.jurisdiction} Accela CSV is not row data`);
  }
  /** @type {Map<string, BrowardAccelaCsvPermitRecord>} */
  const byKey = new Map();
  for (const value of parsed) {
    if (!isRecord(value)) {
      throw new Error(`${source.jurisdiction} Accela CSV row is malformed`);
    }
    const recordNumber = readRecordNumber(value["Record Number"]);
    if (recordNumber === null) {
      throw new Error(
        `${source.jurisdiction} Accela CSV row has no record number`,
      );
    }
    const recordKey = `${source.sourceSystem}:permit:${recordNumber}`;
    const record = {
      schemaVersion:
        /** @type {"oracle-node.broward-accela-csv-list.v1"} */ (
          "oracle-node.broward-accela-csv-list.v1"
        ),
      sourceSystem: source.sourceSystem,
      jurisdiction: source.jurisdiction,
      recordNumber,
      sourceUrl: buildAccelaAltIdDetailUrl(source, recordNumber),
      recordKey,
      recordDate: parseAccelaCsvDate(value.Date),
      recordType: optionalCollapsedText(value["Record Type"]),
      projectName: optionalCollapsedText(value["Project Name"]),
      address: optionalCollapsedText(value.Address),
      expirationDate: parseAccelaCsvDate(value["Expiration Date"]),
      status: optionalCollapsedText(value.Status),
      isRoofPermit: ROOF_PERMIT_PATTERN.test(
        [
          recordNumber,
          optionalCollapsedText(value["Record Type"]),
          optionalCollapsedText(value["Project Name"]),
        ]
          .filter((part) => typeof part === "string")
          .join(" "),
      ),
      sourceWindowKey,
    };
    const existing = byKey.get(recordKey);
    if (
      existing !== undefined &&
      JSON.stringify(existing) !== JSON.stringify(record)
    ) {
      throw new Error(
        `${source.jurisdiction} Accela CSV conflicts for ${recordNumber}`,
      );
    }
    byKey.set(recordKey, record);
  }
  return [...byKey.values()].sort((left, right) =>
    left.recordKey.localeCompare(right.recordKey),
  );
}

/**
 * Capture the official full-result CSV for one date window.
 *
 * The UI total is retained only as provenance because several agencies report
 * a capped or inconsistent value. The built-in `Download results` response is
 * the source artifact and can contain more rows than the visible grid.
 *
 * @param {object} params - Export capture parameters.
 * @param {import("puppeteer").Browser} params.browser - Persistent browser.
 * @param {BrowardAccelaSource} params.source - Jurisdiction source.
 * @param {string} params.startDate - Inclusive ISO start.
 * @param {string} params.endDate - Inclusive ISO end.
 * @param {string} params.downloadDirectory - Existing/private window directory.
 * @param {Logger} params.logger - Structured logger.
 * @returns {Promise<BrowardAccelaCsvWindowResult>} Official exported window.
 */
export async function captureBrowardAccelaCsvWindow({
  browser,
  source,
  startDate,
  endDate,
  downloadDirectory,
  logger,
}) {
  const sourceWindowKey = buildBrowardAccelaDateWindowKey(
    source,
    startDate,
    endDate,
  );
  await mkdir(downloadDirectory, { recursive: true, mode: 0o700 });
  const page = await browser.newPage();
  try {
    await page.goto(source.portalUrl, {
      waitUntil: "domcontentloaded",
      timeout: 60_000,
    });
    const context = await resolveAccelaDomContext(page, source);
    await waitForSearchFormOrFailure(context);
    const initialHtml = await context.content();
    if (
      (await context.$(DEFAULT_SELECTORS.startDate)) === null ||
      (await context.$(DEFAULT_SELECTORS.endDate)) === null
    ) {
      throw new BrowardAccelaSourceError(
        "unexpected_response",
        source,
        `${source.jurisdiction} Accela does not expose CSV date controls`,
        context.url(),
        initialHtml,
      );
    }
    await setAccelaInput(context, DEFAULT_SELECTORS.parcel, "");
    await setAccelaInput(
      context,
      DEFAULT_SELECTORS.startDate,
      toAccelaDate(startDate),
    );
    await setAccelaInput(
      context,
      DEFAULT_SELECTORS.endDate,
      toAccelaDate(endDate),
    );
    await context.click(DEFAULT_SELECTORS.submit);
    await waitForSearchOutcome(context);
    const searchHtml = await context.content();
    const classification = requireSuccessfulPageClassification(
      searchHtml,
      source,
      context.url(),
      `CSV date-window ${sourceWindowKey}`,
    );
    const displayedTotal = parseResultSummary(
      htmlToText(searchHtml),
    ).total;
    if (classification === "no_records") {
      return {
        startDate,
        endDate,
        sourceWindowKey,
        displayedTotal: 0,
        displayedTotalCapped: false,
        records: [],
        rawCsv: "",
        rawSearchHtml: searchHtml,
      };
    }
    const directDetail = extractBrowardAccelaDirectDetailLink({
      html: searchHtml,
      pageUrl: context.url(),
      source,
      searchKey: sourceWindowKey,
      pageNumber: 1,
    });
    if (directDetail !== null) {
      const record = {
        schemaVersion:
          /** @type {"oracle-node.broward-accela-csv-list.v1"} */ (
            "oracle-node.broward-accela-csv-list.v1"
          ),
        sourceSystem: source.sourceSystem,
        jurisdiction: source.jurisdiction,
        recordNumber: directDetail.recordNumber,
        sourceUrl: directDetail.url,
        recordKey: `${source.sourceSystem}:permit:${directDetail.recordNumber}`,
        recordDate: null,
        recordType: directDetail.recordType,
        projectName: directDetail.description,
        address: directDetail.address,
        expirationDate: null,
        status: directDetail.status,
        isRoofPermit: isBrowardAccelaRoofPermitCandidate(directDetail),
        sourceWindowKey,
      };
      return {
        startDate,
        endDate,
        sourceWindowKey,
        displayedTotal: displayedTotal ?? 1,
        displayedTotalCapped: false,
        records: [record],
        rawCsv: "",
        rawSearchHtml: searchHtml,
      };
    }

    const exportLink = await context.$("a[id$='btnExport']");
    if (exportLink === null) {
      throw new BrowardAccelaSourceError(
        "unexpected_response",
        source,
        `${source.jurisdiction} Accela result page has no CSV export`,
        context.url(),
        searchHtml,
      );
    }
    const cdp = await page.createCDPSession();
    await cdp.send("Browser.setDownloadBehavior", {
      behavior: "allowAndName",
      downloadPath: downloadDirectory,
      eventsEnabled: true,
    });
    const download = waitForAccelaDownload(cdp, 60_000);
    await exportLink.click();
    const completed = await download;
    const downloadedPath = `${downloadDirectory}/${completed.guid}`;
    const rawCsv = await readFile(downloadedPath, "utf8");
    const finalPath = `${downloadDirectory}/results.csv`;
    await rename(downloadedPath, finalPath);
    const records = parseBrowardAccelaCsvExport(
      rawCsv,
      source,
      startDate,
      endDate,
    );
    logger.info("broward_accela_csv_window_captured", {
      sourceKey: source.key,
      startDate,
      endDate,
      displayedTotal,
      exportedRecordCount: records.length,
      finalPath,
    });
    return {
      startDate,
      endDate,
      sourceWindowKey,
      displayedTotal,
      displayedTotalCapped: displayedTotal === 100,
      records,
      rawCsv,
      rawSearchHtml: searchHtml,
    };
  } finally {
    await page.close().catch(() => undefined);
  }
}

/**
 * Await one completed browser download by CDP GUID.
 *
 * @param {import("puppeteer").CDPSession} cdp - Page CDP session.
 * @param {number} timeoutMs - Hard download deadline.
 * @returns {Promise<{guid:string,suggestedFilename:string}>} Completed download.
 */
function waitForAccelaDownload(cdp, timeoutMs) {
  return new Promise((resolvePromise, rejectPromise) => {
    /** @type {Map<string,string>} */
    const filenames = new Map();
    const timeout = setTimeout(() => {
      rejectPromise(new Error("Accela CSV download timed out"));
    }, timeoutMs);
    cdp.on("Browser.downloadWillBegin", (event) => {
      if (
        typeof event.guid === "string" &&
        typeof event.suggestedFilename === "string"
      ) {
        filenames.set(event.guid, event.suggestedFilename);
      }
    });
    cdp.on("Browser.downloadProgress", (event) => {
      if (event.state !== "completed" || typeof event.guid !== "string") {
        return;
      }
      const suggestedFilename = filenames.get(event.guid);
      if (suggestedFilename === undefined) return;
      clearTimeout(timeout);
      resolvePromise({ guid: event.guid, suggestedFilename });
    });
  });
}

/**
 * Build an official detail lookup from a full exported alternate ID.
 *
 * @param {BrowardAccelaSource} source - Jurisdiction source.
 * @param {string} recordNumber - Full exported record number.
 * @returns {string} Official detail lookup URL.
 */
function buildAccelaAltIdDetailUrl(source, recordNumber) {
  const url = new URL("./CapDetail.aspx", source.portalUrl);
  url.searchParams.set("Module", source.module);
  url.searchParams.set("TabName", source.module);
  url.searchParams.set("altId", recordNumber);
  return url.toString();
}

/**
 * Parse an Accela MM/DD/YYYY CSV field.
 *
 * @param {unknown} value - Candidate field.
 * @returns {string | null} ISO date or null.
 */
function parseAccelaCsvDate(value) {
  const text = optionalCollapsedText(value);
  if (text === null) return null;
  const match = /^(\d{2})\/(\d{2})\/(\d{4})$/u.exec(text);
  if (match === null) return null;
  const date = new Date(
    Date.UTC(Number(match[3]), Number(match[1]) - 1, Number(match[2])),
  );
  if (
    date.getUTCFullYear() !== Number(match[3]) ||
    date.getUTCMonth() !== Number(match[1]) - 1 ||
    date.getUTCDate() !== Number(match[2])
  ) {
    return null;
  }
  return date.toISOString().slice(0, 10);
}

/**
 * Collapse an optional source value.
 *
 * @param {unknown} value - Candidate value.
 * @returns {string | null} Non-empty collapsed text.
 */
function optionalCollapsedText(value) {
  if (typeof value !== "string") return null;
  const text = collapseText(value);
  return text.length === 0 ? null : text;
}

/**
 * Search one official Broward Accela jurisdiction by exact appraiser folio,
 * capturing every visible page up to a deliberately bounded maximum.
 *
 * A zero-row page succeeds only with an exact Accela no-records marker. Source
 * errors, access blocks, unknown templates, and truncated pagination throw
 * typed errors and therefore cannot be checkpointed as empty.
 *
 * @param {object} params - Public parcel search parameters.
 * @param {import("puppeteer").Browser} params.browser - Reused anonymous browser.
 * @param {BrowardAccelaSource} params.source - Jurisdiction-specific config.
 * @param {string} params.parcelIdentifier - Exact Broward folio.
 * @param {number} params.maxPages - Hard result-page limit.
 * @param {Logger} params.logger - Structured local logger.
 * @returns {Promise<BrowardAccelaParcelSearchResult>} Explicit records/no-records result.
 */
export async function searchBrowardAccelaParcel({
  browser,
  source,
  parcelIdentifier,
  maxPages,
  logger,
}) {
  const normalizedParcelIdentifier =
    normalizeBrowardPermitFolio(parcelIdentifier);
  if (!Number.isInteger(maxPages) || maxPages < 1 || maxPages > 10) {
    throw new Error("Broward Accela maxPages must be between 1 and 10");
  }
  const searchKey = buildBrowardAccelaSearchKey(
    source,
    normalizedParcelIdentifier,
  );
  const page = await createConfiguredPage(browser);
  /** @type {BrowardAccelaSearchPage[]} */
  const pages = [];
  /** @type {BrowardAccelaPermitLink[]} */
  const permits = [];
  /** @type {number | null} */
  let reportedTotal = null;
  let excludedNonPermitCount = 0;

  try {
    logger.info("broward_accela_parcel_search_open", {
      sourceKey: source.key,
      jurisdiction: source.jurisdiction,
      agencyCode: source.agencyCode,
      module: source.module,
      parcelIdentifier: normalizedParcelIdentifier,
      searchKey,
    });
    await page.goto(source.portalUrl, {
      waitUntil: "domcontentloaded",
      timeout: 60_000,
    });
    const context = await resolveAccelaDomContext(page, source);
    try {
      await waitForSearchFormOrFailure(context);
    } catch (caught) {
      const failureHtml = await context.content();
      const failureUrl = context.url();
      const classification = classifyBrowardAccelaPage(failureHtml);
      if (
        classification === "access_blocked" ||
        classification === "source_error" ||
        classification === "unknown"
      ) {
        requireSuccessfulPageClassification(
          failureHtml,
          source,
          failureUrl,
          "search form load",
        );
      }
      throw new BrowardAccelaSourceError(
        "unexpected_response",
        source,
        `${source.jurisdiction} Accela search form did not become ready: ${caught instanceof Error ? caught.message : String(caught)}`,
        failureUrl,
        failureHtml,
      );
    }
    const initialHtml = await context.content();
    const initialClassification = classifyBrowardAccelaPage(initialHtml);
    if (
      initialClassification === "access_blocked" ||
      initialClassification === "source_error"
    ) {
      requireSuccessfulPageClassification(
        initialHtml,
        source,
        context.url(),
        "search form load",
      );
    }
    if ((await context.$(DEFAULT_SELECTORS.parcel)) === null) {
      throw new BrowardAccelaSourceError(
        "unexpected_response",
        source,
        `${source.jurisdiction} Accela did not expose the configured public parcel field`,
        context.url(),
        initialHtml,
      );
    }

    await clearOptionalAccelaInput(context, DEFAULT_SELECTORS.startDate);
    await clearOptionalAccelaInput(context, DEFAULT_SELECTORS.endDate);
    await setAccelaInput(
      context,
      DEFAULT_SELECTORS.parcel,
      normalizedParcelIdentifier,
    );
    await Promise.allSettled([
      context.waitForNavigation({
        waitUntil: "domcontentloaded",
        timeout: 60_000,
      }),
      context.click(DEFAULT_SELECTORS.submit),
    ]);
    try {
      await waitForSearchOutcome(context);
    } catch (caught) {
      const failureHtml = await context.content();
      const failureUrl = context.url();
      const classification = classifyBrowardAccelaPage(failureHtml);
      if (
        classification === "access_blocked" ||
        classification === "source_error" ||
        classification === "unknown"
      ) {
        requireSuccessfulPageClassification(
          failureHtml,
          source,
          failureUrl,
          `parcel search ${searchKey}`,
        );
      }
      throw new BrowardAccelaSourceError(
        "unexpected_response",
        source,
        `${source.jurisdiction} Accela search outcome did not become ready: ${caught instanceof Error ? caught.message : String(caught)}`,
        failureUrl,
        failureHtml,
      );
    }

    for (let pageNumber = 1; pageNumber <= maxPages; pageNumber += 1) {
      const html = await context.content();
      const text = htmlToText(html);
      const classification = requireSuccessfulPageClassification(
        html,
        source,
        context.url(),
        `parcel search ${searchKey} page ${String(pageNumber)}`,
      );
      const directDetail = extractBrowardAccelaDirectDetailLink({
        html,
        pageUrl: context.url(),
        source,
        searchKey,
        pageNumber,
      });
      if (directDetail !== null) {
        pages.push({
          pageNumber,
          url: context.url(),
          resultSummary: "detail page",
          html,
        });
        permits.push(directDetail);
        reportedTotal = 1;
        break;
      }

      const parsedSummary = parseResultSummary(text);
      reportedTotal = reportedTotal ?? parsedSummary.total;
      pages.push({
        pageNumber,
        url: context.url(),
        resultSummary: parsedSummary.summary,
        html,
      });
      if (classification === "no_records") {
        if (pageNumber !== 1 || permits.length > 0) {
          throw new BrowardAccelaSourceError(
            "unexpected_response",
            source,
            `${source.jurisdiction} Accela mixed an explicit no-records marker with record pages`,
            context.url(),
            html,
          );
        }
        logger.info("broward_accela_parcel_no_records", {
          sourceKey: source.key,
          parcelIdentifier: normalizedParcelIdentifier,
          searchKey,
        });
        return {
          status: "no_records",
          searchKey,
          parcelIdentifier: normalizedParcelIdentifier,
          source,
          permits: [],
          pages,
          reportedTotal: 0,
          excludedNonPermitCount: 0,
        };
      }

      const pageExcludedNonPermitCount = countBrowardAccelaExcludedModuleLinks({
        html,
        source,
      });
      excludedNonPermitCount += pageExcludedNonPermitCount;
      const pageLinks = extractBrowardAccelaPermitLinks({
        html,
        source,
        searchKey,
        pageNumber,
      });
      if (pageLinks.length === 0 && pageExcludedNonPermitCount === 0) {
        throw new BrowardAccelaSourceError(
          "unexpected_response",
          source,
          `${source.jurisdiction} Accela indicated records but exposed no permit detail links on page ${String(pageNumber)}`,
          context.url(),
          html,
        );
      }
      permits.push(...pageLinks);
      logger.info("broward_accela_parcel_page_captured", {
        sourceKey: source.key,
        parcelIdentifier: normalizedParcelIdentifier,
        pageNumber,
        pagePermitCount: pageLinks.length,
        pageExcludedNonPermitCount,
        reportedTotal,
      });

      const nextAvailable = await hasNextPage(context);
      if (!nextAvailable) {
        if (
          reportedTotal !== null &&
          permits.length + excludedNonPermitCount < reportedTotal
        ) {
          throw new BrowardAccelaSourceError(
            "incomplete_pagination",
            source,
            `${source.jurisdiction} Accela accounted for ${String(permits.length + excludedNonPermitCount)} of ${String(reportedTotal)} reported records without a next page`,
            context.url(),
            html,
          );
        }
        break;
      }
      if (pageNumber === maxPages) {
        throw new BrowardAccelaSourceError(
          "incomplete_pagination",
          source,
          `${source.jurisdiction} Accela still exposed a next page at the maxPages limit ${String(maxPages)}`,
          context.url(),
          html,
        );
      }

      const priorSummary = parsedSummary.summary;
      if (!(await clickNextPage(context))) {
        throw new BrowardAccelaSourceError(
          "incomplete_pagination",
          source,
          `${source.jurisdiction} Accela next-page control disappeared before it could be clicked`,
          context.url(),
          html,
        );
      }
      await context.waitForFunction(
        (previousSummary) => {
          const bodyText = document.body?.innerText ?? "";
          if (
            /Your search returned no results|No records found|access denied|captcha|technical difficulties|unable to proceed|Object reference not set|error\(s\) occurred on current page/i.test(
              bodyText,
            )
          ) {
            return true;
          }
          const match =
            /Showing\s+([0-9,]+\s*-\s*[0-9,]+\s+of\s+[0-9,]+)/i.exec(bodyText);
          const current =
            match === null ? null : match[1].replace(/\s+/g, " ").trim();
          return current !== null && current !== previousSummary;
        },
        { timeout: 60_000 },
        priorSummary,
      );
    }
  } finally {
    await page.close().catch(() => undefined);
  }

  /** @type {Map<string, BrowardAccelaPermitLink>} */
  const deduped = new Map();
  for (const permit of permits) {
    const key = permit.recordNumber.toUpperCase();
    const existing = deduped.get(key);
    if (existing !== undefined && existing.url !== permit.url) {
      throw new BrowardAccelaSourceError(
        "identity_mismatch",
        source,
        `${source.jurisdiction} Accela returned conflicting detail URLs for record ${permit.recordNumber}`,
        permit.url,
        pages.at(-1)?.html ?? null,
      );
    }
    deduped.set(key, permit);
  }
  if (deduped.size === 0 && excludedNonPermitCount > 0) {
    return {
      status: "non_permit_records_only",
      searchKey,
      parcelIdentifier: normalizedParcelIdentifier,
      source,
      permits: [],
      pages,
      reportedTotal,
      excludedNonPermitCount,
    };
  }
  if (deduped.size === 0) {
    throw new BrowardAccelaSourceError(
      "unexpected_response",
      source,
      `${source.jurisdiction} Accela completed without records or an explicit no-records marker`,
      source.portalUrl,
    );
  }
  if (
    reportedTotal !== null &&
    deduped.size + excludedNonPermitCount < reportedTotal
  ) {
    throw new BrowardAccelaSourceError(
      "incomplete_pagination",
      source,
      `${source.jurisdiction} Accela accounted for ${String(deduped.size + excludedNonPermitCount)} unique permit/cross-module records from ${String(reportedTotal)} reported records`,
      source.portalUrl,
    );
  }
  return {
    status: "records",
    searchKey,
    parcelIdentifier: normalizedParcelIdentifier,
    source,
    permits: [...deduped.values()],
    pages,
    reportedTotal,
    excludedNonPermitCount,
  };
}

/**
 * Extract public document and related links using the configured jurisdiction
 * origin rather than Lee County's agency path.
 *
 * @param {string} html - Complete Accela detail HTML.
 * @param {BrowardAccelaSource} source - Jurisdiction source.
 * @returns {{ documentLinks: import("../../workflow/lambdas/permit-harvest-worker/lee-accela.mjs").ExtractedLink[], relatedLinks: import("../../workflow/lambdas/permit-harvest-worker/lee-accela.mjs").ExtractedLink[] }} Public link groups.
 */
function extractSourceDetailLinks(html, source) {
  const $ = cheerio.load(html);
  /** @type {import("../../workflow/lambdas/permit-harvest-worker/lee-accela.mjs").ExtractedLink[]} */
  const documentLinks = [];
  /** @type {import("../../workflow/lambdas/permit-harvest-worker/lee-accela.mjs").ExtractedLink[]} */
  const relatedLinks = [];
  $("a[href]").each((_, element) => {
    const anchor = $(element);
    const href = anchor.attr("href");
    if (href === undefined || /^javascript:/i.test(href)) return;
    let url;
    try {
      url = normalizeSourceUrl(href, source);
    } catch (caught) {
      if (caught instanceof BrowardAccelaSourceError) return;
      throw caught;
    }
    const link = {
      text: collapseText(anchor.text()),
      url,
      title: anchor.attr("title") ?? null,
    };
    if (
      /urlrouting\.ashx|document|eplan|digitalprojects|GetDocument/i.test(
        `${href} ${link.text}`,
      )
    ) {
      documentLinks.push(link);
    } else if (
      /RelatedRecords|CapDetail|Inspection|Report/i.test(`${href} ${link.text}`)
    ) {
      relatedLinks.push(link);
    }
  });
  return { documentLinks, relatedLinks };
}

/**
 * Remove expiration metadata appended to the visible status by Broward Accela
 * templates while retaining the complete source text in `rawText`.
 *
 * @param {string | null | undefined} value - Raw visible record status.
 * @returns {string | null} Concise permit status for existing permit fields.
 */
export function cleanBrowardAccelaRecordStatus(value) {
  const sharedStatus = cleanRecordStatus(value);
  if (sharedStatus === null) return null;
  const expirationBoundary = /\s+Expiration Date:\s*/i.exec(sharedStatus);
  const status =
    expirationBoundary === null
      ? sharedStatus
      : sharedStatus.slice(0, expirationBoundary.index).trim();
  return status.length > 0 ? status : null;
}

/**
 * Augment the compatible Lee More Details parser with Broward field aliases
 * that map contract/job value, residential/commercial use, and square footage
 * into the existing permit keys. The original Broward label is also retained.
 *
 * @param {string | null} sectionText - Raw public More Details section.
 * @returns {Record<string, string>} Existing permit-field dictionary plus Broward labels.
 */
export function parseBrowardAccelaMoreDetails(sectionText) {
  const details = parseMoreDetails(sectionText);
  if (sectionText === null) return details;
  const normalizedText = collapseText(sectionText);
  /**
   * @type {readonly { sourceLabel: string, canonicalLabel: string, pattern: RegExp }[]}
   */
  const numericAliases = [
    {
      sourceLabel: "Contract Value",
      canonicalLabel: "Estimated Job Value",
      pattern: /\bContract Value:\s*\$?([\d,]+(?:\.\d+)?)\b/i,
    },
    {
      sourceLabel: "Job Value",
      canonicalLabel: "Estimated Job Value",
      pattern: /\bJob Value:\s*\$?([\d,]+(?:\.\d+)?)\b/i,
    },
    {
      sourceLabel: "Enter Job Cost",
      canonicalLabel: "Estimated Job Value",
      pattern: /\bEnter Job Cost:\s*\$?([\d,]+(?:\.\d+)?)\b/i,
    },
    {
      sourceLabel: "Total Square Feet",
      canonicalLabel: "Estimated Sq. Ft.",
      pattern: /\bTotal Square Feet:\s*([\d,]+(?:\.\d+)?)\b/i,
    },
    {
      sourceLabel: "Square Feet",
      canonicalLabel: "Estimated Sq. Ft.",
      pattern: /\bSquare Feet:\s*([\d,]+(?:\.\d+)?)\b/i,
    },
    {
      sourceLabel: "Livable (SQ FT)",
      canonicalLabel: "Estimated Sq. Ft.",
      pattern: /\bLivable \(SQ FT\):\s*([\d,]+(?:\.\d+)?)\b/i,
    },
  ];
  for (const alias of numericAliases) {
    const value = matchCollapsedText(normalizedText, alias.pattern);
    if (value === null) continue;
    details[alias.sourceLabel] = value;
    details[alias.canonicalLabel] ??= value;
  }
  const useMatch =
    /\b(Commercial \/ Residential|Residential \/ Commercial):\s*([A-Za-z-]+)\b/i.exec(
      normalizedText,
    );
  if (useMatch !== null) {
    const sourceLabel = collapseText(useMatch[1]);
    const value = collapseText(useMatch[2]);
    if (sourceLabel.length > 0 && value.length > 0) {
      details[sourceLabel] = value;
      details["Comm/Res"] ??= value;
    }
  }
  return details;
}

/**
 * Normalize only the documented Broward folio portion of an Accela display
 * value. Accela commonly appends a required-field asterisk; arbitrary suffixes
 * are rejected rather than stripped.
 *
 * @param {string | null | undefined} value - Public detail-page parcel display.
 * @returns {string | null} Canonical 12-character folio when exact.
 */
function normalizeDisplayedBrowardPermitFolio(value) {
  if (typeof value !== "string") return null;
  const match =
    /^\s*([A-Z0-9]{12}|[A-Z0-9]{6}-[A-Z0-9]{2}-[A-Z0-9]{4})(?:\s*\*)?\s*$/i.exec(
      value,
    );
  if (match === null) return null;
  return normalizeBrowardPermitFolio(match[1]);
}

/**
 * Parse one compatible public Accela detail page into the existing permit
 * artifact shape while retaining jurisdiction-specific source identity and the
 * exact submitted Broward folio.
 *
 * @param {object} params - Detail extraction parameters.
 * @param {string} params.html - Complete source HTML.
 * @param {string} params.sourceUrl - Final browser URL.
 * @param {BrowardAccelaSource} params.source - Jurisdiction source.
 * @param {string} params.parcelIdentifier - Exact submitted Broward folio.
 * @param {BrowardAccelaPermitLink} params.permit - Reconciled search-result link.
 * @param {string} [params.retrievedAt] - Injectable ISO timestamp for deterministic fixtures.
 * @returns {BrowardAccelaPermitRecord} Normalized, provenance-rich permit record.
 */
export function extractBrowardAccelaPermitDetail({
  html,
  sourceUrl,
  source,
  parcelIdentifier,
  permit,
  retrievedAt = new Date().toISOString(),
}) {
  const canonicalParcel = normalizeBrowardPermitFolio(parcelIdentifier);
  const text = htmlToText(html);
  const header =
    /Record\s+([A-Z0-9][A-Z0-9./_-]{1,79})\s*:\s*(.*?)\s+Record Status:/i.exec(
      text,
    );
  const detailRecordNumber =
    header === null ? permit.recordNumber : readRecordNumber(header[1]);
  if (
    detailRecordNumber === null ||
    detailRecordNumber.toUpperCase() !== permit.recordNumber.toUpperCase()
  ) {
    throw new BrowardAccelaSourceError(
      "identity_mismatch",
      source,
      `${source.jurisdiction} Accela detail identity differs from search record ${permit.recordNumber}`,
      sourceUrl,
    );
  }
  const recordType =
    header === null ? permit.recordType : collapseText(header[2]) || null;
  const moreDetailsRawText = matchCollapsedText(
    text,
    /More Details\s+(.*?)(?:Fees(?:\s+\*?Fee Reductions|\s+Loading|\s+Paid:|\s+Outstanding:)|Inspections|Processing Status|Related Records|$)/i,
  );
  const moreDetails = parseBrowardAccelaMoreDetails(moreDetailsRawText);
  const rawSourceParcel =
    moreDetails["Parcel Number"] ??
    matchCollapsedText(
      text,
      /Parcel (?:Information\s+)?(?:Number|No\.?):\s*([A-Z0-9-]{12,16})/i,
    );
  /** @type {string | null} */
  const sourceParcelIdentifier =
    normalizeDisplayedBrowardPermitFolio(rawSourceParcel);
  if (
    sourceParcelIdentifier !== null &&
    sourceParcelIdentifier !== canonicalParcel
  ) {
    throw new BrowardAccelaSourceError(
      "identity_mismatch",
      source,
      `${source.jurisdiction} Accela detail parcel ${sourceParcelIdentifier} differs from submitted parcel ${canonicalParcel}`,
      sourceUrl,
    );
  }
  const inspectionsRawText = matchCollapsedText(
    text,
    /Inspections\s+(.*?)(?:Digital Projects|Processing Status|Related Records|$)/i,
  );
  const processingStatusRawText = matchCollapsedText(
    text,
    /Processing Status\s+(.*?)(?:Related Records|$)/i,
  );
  const { documentLinks, relatedLinks } = extractSourceDetailLinks(
    html,
    source,
  );
  const workLocation = matchCollapsedText(
    text,
    /Work Location\s+(.*?)\s+\*\s+Record Details/i,
  );
  const applicant = matchCollapsedText(
    text,
    /Applicant:\s*(.*?)\s+Licensed Professional:/i,
  );
  const licensedProfessional = matchCollapsedText(
    text,
    /Licensed Professional:\s*(.*?)\s+Project Description:/i,
  );
  const projectDescription = matchCollapsedText(
    text,
    /Project Description:\s*(.*?)(?:More Details|Fees|Inspections|Processing Status|Related Records)/i,
  );

  return {
    schemaVersion: "permit-harvest.accela.v1",
    source: source.sourceSystem,
    sourceSystem: source.sourceSystem,
    jurisdiction: source.jurisdiction,
    retrievedAt,
    sourceUrl: normalizeSourceUrl(sourceUrl, source),
    recordNumber: detailRecordNumber,
    recordType,
    recordStatus: cleanBrowardAccelaRecordStatus(
      matchCollapsedText(
        text,
        /Record Status:\s*(.*?)(?:Click here for more information|Create a New Collection|Add to Existing Collection|Record Info|Work Location)/i,
      ) ?? permit.status,
    ),
    workLocation: workLocation ?? permit.address,
    parcelIdentifier: canonicalParcel,
    sourceParcelIdentifier,
    applicant,
    licensedProfessional,
    projectDescription: projectDescription ?? permit.description,
    moreDetails,
    moreDetailsRawText,
    inspectionsRawText,
    completedInspections: parseCompletedInspections(text),
    processingStatusRawText,
    documentLinks,
    relatedLinks,
    rawText: text,
    sourceSearchResult: permit,
    idempotencyKey: `${source.sourceSystem}:permit:${detailRecordNumber}`,
    provenance: {
      searchMethod: "public_anonymous_parcel",
      anonymous: true,
      submittedParcelIdentifier: canonicalParcel,
      searchUrl: source.portalUrl,
      searchKey: permit.sourceSearchKey,
      resultPage: permit.sourcePage,
      agencyCode: source.agencyCode,
      module: source.module,
      officialEvidenceUrl: source.officialEvidenceUrl,
      historicalCutoff: source.historicalCutoff,
      separateHistoricalSource: source.separateHistoricalSource,
    },
  };
}

/**
 * Capture and normalize one public detail page. Explicit source/access errors
 * remain failures; unlike the Lee historic fallback, no Broward record is
 * synthesized from a list row when its jurisdictional detail cannot be read.
 *
 * @param {object} params - Detail capture parameters.
 * @param {import("puppeteer").Browser} params.browser - Reused anonymous browser.
 * @param {BrowardAccelaSource} params.source - Jurisdiction source.
 * @param {string} params.parcelIdentifier - Exact submitted Broward folio.
 * @param {BrowardAccelaPermitLink} params.permit - Search-result permit link.
 * @param {Logger} params.logger - Structured local logger.
 * @returns {Promise<BrowardAccelaDetailCapture>} Raw and normalized detail.
 */
export async function captureBrowardAccelaPermitDetail({
  browser,
  source,
  parcelIdentifier,
  permit,
  logger,
}) {
  const page = await createConfiguredPage(browser);
  try {
    logger.info("broward_accela_detail_open", {
      sourceKey: source.key,
      parcelIdentifier,
      recordNumber: permit.recordNumber,
      url: permit.url,
    });
    await page.goto(permit.url, {
      waitUntil: "domcontentloaded",
      timeout: 60_000,
    });
    const context = await resolveAccelaDomContext(page, source);
    try {
      await context.waitForFunction(
        (recordNumber) => {
          const text = document.body?.innerText ?? "";
          return (
            text.toUpperCase().includes(String(recordNumber).toUpperCase()) ||
            /access denied|captcha|technical difficulties|unable to proceed|Object reference not set|error\(s\) occurred on current page|temporarily unavailable/i.test(
              text,
            )
          );
        },
        { timeout: 60_000 },
        permit.recordNumber,
      );
    } catch (caught) {
      const failureHtml = await context.content();
      const failureUrl = context.url();
      const classification = classifyBrowardAccelaPage(failureHtml);
      if (
        classification === "access_blocked" ||
        classification === "source_error" ||
        classification === "unknown"
      ) {
        requireSuccessfulPageClassification(
          failureHtml,
          source,
          failureUrl,
          `detail ${permit.recordNumber}`,
        );
      }
      throw new BrowardAccelaSourceError(
        "unexpected_response",
        source,
        `${source.jurisdiction} Accela detail did not become ready for ${permit.recordNumber}: ${caught instanceof Error ? caught.message : String(caught)}`,
        failureUrl,
        failureHtml,
      );
    }
    const html = await context.content();
    requireSuccessfulPageClassification(
      html,
      source,
      context.url(),
      `detail ${permit.recordNumber}`,
    );
    return {
      html,
      record: extractBrowardAccelaPermitDetail({
        html,
        sourceUrl: context.url(),
        source,
        parcelIdentifier,
        permit,
      }),
    };
  } finally {
    await page.close().catch(() => undefined);
  }
}

/**
 * Create a reusable anonymous browser with the same tested Chromium mechanism
 * as Lee Accela. Exported here keeps the local CLI independent of AWS.
 *
 * @param {Logger} logger - Structured local logger.
 * @returns {Promise<import("puppeteer").Browser>} Headless browser.
 */
export function createBrowardAccelaBrowser(logger) {
  return createBrowser(logger);
}

/**
 * Build a stable local raw-detail filename without exposing record text as an
 * unbounded path segment.
 *
 * @param {BrowardAccelaPermitLink} permit - Search-result permit link.
 * @returns {string} Stable filename stem.
 */
export function buildBrowardAccelaPermitStem(permit) {
  return `${safeKeyPart(permit.recordNumber)}-${shortHash(permit.url)}`;
}

/**
 * Create a new empty local checkpoint.
 *
 * @param {string} [updatedAt] - Injectable timestamp for deterministic tests.
 * @returns {BrowardAccelaCheckpoint} Empty checkpoint.
 */
export function createBrowardAccelaCheckpoint(
  updatedAt = new Date().toISOString(),
) {
  return {
    schemaVersion: "broward-accela-local-checkpoint.v1",
    updatedAt,
    targets: {},
  };
}

/**
 * Read and validate a local checkpoint. A missing file starts a new checkpoint;
 * malformed or foreign state fails closed instead of silently restarting.
 *
 * @param {string} checkpointPath - Local JSON checkpoint path.
 * @returns {Promise<BrowardAccelaCheckpoint>} Valid checkpoint state.
 */
export async function readBrowardAccelaCheckpoint(checkpointPath) {
  try {
    const parsed = JSON.parse(await readFile(checkpointPath, "utf8"));
    if (
      !isRecord(parsed) ||
      parsed.schemaVersion !== "broward-accela-local-checkpoint.v1" ||
      !isRecord(parsed.targets) ||
      typeof parsed.updatedAt !== "string"
    ) {
      throw new Error(
        `Invalid Broward Accela checkpoint schema: ${checkpointPath}`,
      );
    }
    return /** @type {BrowardAccelaCheckpoint} */ (parsed);
  } catch (caught) {
    if (isNodeError(caught) && caught.code === "ENOENT") {
      return createBrowardAccelaCheckpoint();
    }
    throw caught;
  }
}

/**
 * Atomically replace a mode-0600 local checkpoint after creating its parent.
 *
 * @param {string} checkpointPath - Local JSON checkpoint path.
 * @param {BrowardAccelaCheckpoint} checkpoint - Complete state to persist.
 * @returns {Promise<void>} Resolves after atomic rename.
 */
export async function writeBrowardAccelaCheckpoint(checkpointPath, checkpoint) {
  checkpoint.updatedAt = new Date().toISOString();
  await mkdir(dirname(checkpointPath), { recursive: true });
  const temporaryPath = `${checkpointPath}.${String(process.pid)}.tmp`;
  await writeFile(temporaryPath, `${JSON.stringify(checkpoint, null, 2)}\n`, {
    encoding: "utf8",
    mode: 0o600,
  });
  await rename(temporaryPath, checkpointPath);
}

/**
 * Narrow an unknown value to an object record.
 *
 * @param {unknown} value - Candidate value.
 * @returns {value is Record<string, unknown>} Whether the value is an object record.
 */
function isRecord(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    Array.isArray(value) === false
  );
}

/**
 * Narrow an unknown thrown value to a Node-style error with a string code.
 *
 * @param {unknown} value - Caught value.
 * @returns {value is Error & { code: string }} Whether a string error code is present.
 */
function isNodeError(value) {
  return (
    value instanceof Error && "code" in value && typeof value.code === "string"
  );
}
