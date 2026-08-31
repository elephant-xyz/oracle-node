// @ts-check

import * as cheerio from "cheerio";
import puppeteer from "puppeteer";

/**
 * @typedef {"master" | "permit"} BrowardBcsSourceRecordKind
 */

/**
 * @typedef {"master" | "permit" | "plan_review"} BrowardBcsListRecordKind
 */

/**
 * @typedef {object} BrowardBcsListRecord
 * @property {string} sourceUrl - Canonical official detail URL.
 * @property {string} sourceObjectId - Numeric POSSE object identifier from the official link.
 * @property {BrowardBcsSourceRecordKind} sourceRecordKind - Supported permit or master-application kind.
 * @property {string} permitNumber - Public BCS permit number.
 * @property {string} recordType - Public permit or master-application type.
 * @property {string} recordStatus - Public list status.
 * @property {string | null} permitIssueDate - ISO issue date, or null when BCS prints its placeholder.
 * @property {string | null} listContractor - Contractor text printed in the public list.
 */

/**
 * @typedef {object} BrowardBcsInspection
 * @property {string} source_url - Canonical official inspection-detail URL.
 * @property {string} source_object_id - Numeric POSSE inspection object identifier.
 * @property {string} inspection_type - Public inspection type.
 * @property {string | null} requested_date - ISO requested date when BCS exposes one.
 * @property {string} result - Public inspection outcome.
 * @property {string | null} completed_date - ISO completion date when BCS exposes one.
 */

/**
 * @typedef {object} NormalizedBrowardBcsPermit
 * @property {"broward_county_bcs_posse_permits"} source_system - Stable official-source key.
 * @property {string} source_url - Canonical official detail URL.
 * @property {string} source_search_url - Official property-first search URL used for the lookup.
 * @property {string} source_list_url - Official parcel permit-list URL returned by BCS.
 * @property {string} source_object_id - Numeric POSSE object identifier.
 * @property {BrowardBcsSourceRecordKind} source_record_kind - Master application or issued permit row.
 * @property {string} record_key - Stable source-system and POSSE-object key.
 * @property {string} parcel_identifier - Exact canonical 12-character BCPA parcel ID submitted to BCS.
 * @property {string} source_folio_number - BCS's separately displayed legacy folio number.
 * @property {string} issuing_jurisdiction - Jurisdiction explicitly printed by BCS; not a countywide-coverage claim.
 * @property {string} permit_number - Public permit number.
 * @property {string} record_status - Public record status.
 * @property {string} record_type - Public permit or master-application type.
 * @property {string | null} permit_issue_date - ISO issue date, or null for the BCS placeholder.
 * @property {string | null} application_date - ISO master-application date when exposed.
 * @property {string | null} expiration_date - ISO master expiration date when exposed.
 * @property {string | null} project_title - Public master project title when exposed.
 * @property {string | null} project_description - Public permit/master description when exposed.
 * @property {string} work_location - Public situs address retained for local private staging.
 * @property {string} legal_description - Public parcel legal description retained for local private staging.
 * @property {string | null} contractor_name - Public permit-holder name retained for local private staging.
 * @property {string | null} contractor_license - Public permit-holder license display retained for local private staging.
 * @property {string | null} building_use - Public master building-use display.
 * @property {string | null} present_use - Public master present-use display.
 * @property {string | null} proposed_use - Public master proposed-use display.
 * @property {number | null} job_value - Public master job value in dollars.
 * @property {number | null} square_footage - Public master square footage.
 * @property {string | null} occupancy_type - Public master occupancy type.
 * @property {string | null} construction_type - Public master construction type.
 * @property {number | null} occupant_load - Public master occupant load.
 * @property {number | null} finish_floor_above_road - Public feet-above-road-crown value.
 * @property {number | null} finish_floor_above_sea_level - Public feet-above-mean-sea-level value.
 * @property {readonly BrowardBcsInspection[]} inspections - Inspection rows exposed on a permit detail page.
 * @property {boolean} is_roof_permit - Conservative source-text roof classification.
 * @property {{
 *   search_method: "ParcelID",
 *   reference_number: string | null,
 *   list_contractor: string | null,
 *   detail_page_title: string
 * }} raw - Minimal source-specific provenance and list evidence.
 */

/**
 * @typedef {object} BrowardBcsPermitListParseResult
 * @property {"records" | "no_permits"} status - Explicit source outcome.
 * @property {string} parcelObjectId - POSSE parcel object identifier from the list URL.
 * @property {number} listedRecordCount - Every master, permit, and plan-review row printed by BCS.
 * @property {number} excludedPlanReviewCount - Plan-review rows intentionally not represented as permits.
 * @property {readonly BrowardBcsListRecord[]} records - Supported master and permit rows.
 */

/**
 * @typedef {object} BrowardBcsLookupObservation
 * @property {string} parcelIdentifier - Exact parcel ID submitted to BCS.
 * @property {"records" | "no_permits"} status - Explicit source outcome.
 * @property {string} sourceSearchUrl - Official search URL.
 * @property {string} sourceListUrl - Official resolved parcel-list URL.
 * @property {string} parcelObjectId - Numeric POSSE parcel object identifier.
 * @property {number} listedRecordCount - Every BCS list row.
 * @property {number} excludedPlanReviewCount - Non-permit plan-review rows.
 * @property {number} normalizedRecordCount - Normalized master and permit records.
 * @property {number} detailPageCount - Detail pages fetched sequentially.
 * @property {number} elapsedMs - Total source time for the property lookup.
 */

/**
 * @typedef {object} BrowardBcsProbeResult
 * @property {readonly NormalizedBrowardBcsPermit[]} records - Deduplicated normalized private-staging records.
 * @property {readonly BrowardBcsLookupObservation[]} observations - Per-property source outcomes and provenance.
 */

export const BROWARD_BCS_SEARCH_URL =
  "https://dpepp.broward.org/BCS/Default.aspx?PossePresentation=ParcelSearchByAddress";
export const BROWARD_BCS_SOURCE_SYSTEM = "broward_county_bcs_posse_permits";
export const BROWARD_BCS_SCOPE_URL =
  "https://www.broward.org/Building/Government2Government/Pages/CurrentServiceAgreements.aspx";

/**
 * Five permit-priority parcels already proven by the Broward appraisal pilot
 * and 50-parcel acceptance sample. They cover Commercial, Warehouse,
 * OfficeBuilding, and LightManufacturing appraiser usage types.
 *
 * The BCS result itself determines whether BCS has a record. A city name in
 * appraisal evidence is never treated as proof of current BCS custody.
 *
 * @type {readonly string[]}
 */
export const BROWARD_BCS_PILOT_PARCEL_IDS = Object.freeze([
  "474135010090",
  "494209060010",
  "494318013550",
  "474236140090",
  "474236140080",
]);

const BROWARD_BCS_ORIGIN = "https://dpepp.broward.org";
const BROWARD_BCS_PATH = "/BCS/Default.aspx";
const PARCEL_INPUT_SELECTOR = "#ParcelID_23473057_S0";
const SEARCH_BUTTON_SELECTOR =
  "#ctl00_cphBottomFunctionBand_ctl03_PerformSearch";
const SOURCE_NO_PERMITS_TEXT = "No permits were found for this address.";
const SOURCE_NO_MATCH_TEXT = "There are no permits that match your criteria.";
const MAX_PILOT_FOLIOS = 5;
const MAX_LIST_ROWS_PER_FOLIO = 125;
const MAX_DETAIL_PAGES_PER_FOLIO = 75;
const MIN_PROPERTY_DELAY_MS = 1_000;
const MIN_DETAIL_DELAY_MS = 250;
const MAX_SOURCE_HTML_BYTES = 2_000_000;
const ROOF_PERMIT_PATTERN = /\broof(?:ing)?\b/iu;

/**
 * Classify a BCS list row before requesting its detail page.
 *
 * @param {BrowardBcsListRecord} record - Public BCS permit/master list row.
 * @returns {boolean} True only when number or type explicitly says roofing.
 */
export function isBrowardBcsRoofPermitCandidate(record) {
  return ROOF_PERMIT_PATTERN.test(
    `${record.permitNumber} ${record.recordType}`,
  );
}

/**
 * Collapse source whitespace and convert an empty value to null.
 *
 * @param {unknown} value - Candidate source text.
 * @returns {string | null} Collapsed non-empty text, or null.
 */
function readText(value) {
  if (typeof value !== "string") return null;
  const normalized = value
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return normalized.length === 0 ? null : normalized;
}

/**
 * Read one source element while preserving visual line breaks as spaces.
 *
 * @param {import("cheerio").Cheerio<import("domhandler").AnyNode>} selection - Selected source element.
 * @returns {string | null} Collapsed visual text.
 */
function readSelectionText(selection) {
  const cloned = selection.clone();
  cloned.find("br").replaceWith(" ");
  return readText(cloned.text());
}

/**
 * Require one non-empty source value.
 *
 * @param {string | null} value - Optional source text.
 * @param {string} fieldName - Field name used in the failure.
 * @returns {string} Non-empty source text.
 */
function requireText(value, fieldName) {
  if (value === null) {
    throw new Error(`Broward BCS ${fieldName} is missing`);
  }
  return value;
}

/**
 * Validate an exact 12-character Broward parcel ID for BCS's Parcel ID field.
 *
 * Trimming surrounding CLI whitespace and uppercasing letters are the only
 * transformations. Letters are never removed, characters are never padded,
 * and dashed legacy folios are rejected rather than silently rewritten.
 *
 * @param {unknown} value - Candidate BCPA parcel identifier.
 * @returns {string} Canonical uppercase identifier accepted by the BCS field.
 */
export function normalizeBrowardBcsParcelId(value) {
  if (typeof value !== "string") {
    throw new Error("Broward BCS parcel ID must be a string");
  }
  const normalized = value.trim().toUpperCase();
  if (!/^[A-Z0-9]{12}$/u.test(normalized)) {
    throw new Error(
      "Broward BCS parcel ID must contain exactly 12 alphanumeric characters",
    );
  }
  return normalized;
}

/**
 * Validate the bounded property list before opening a browser.
 *
 * @param {readonly string[]} parcelIds - Candidate BCPA parcel identifiers.
 * @param {number} [maxFolios=5] - Operator ceiling, never above five.
 * @returns {readonly string[]} Unique normalized parcel identifiers.
 */
export function validateBrowardBcsParcelIds(
  parcelIds,
  maxFolios = MAX_PILOT_FOLIOS,
) {
  if (
    !Number.isInteger(maxFolios) ||
    maxFolios <= 0 ||
    maxFolios > MAX_PILOT_FOLIOS
  ) {
    throw new Error(
      `Broward BCS maxFolios must be an integer from 1 through ${String(MAX_PILOT_FOLIOS)}`,
    );
  }
  const normalized = parcelIds.map(normalizeBrowardBcsParcelId);
  if (normalized.length === 0) {
    throw new Error("At least one Broward BCS parcel ID is required");
  }
  if (normalized.length > maxFolios) {
    throw new Error(
      `Refusing ${String(normalized.length)} Broward BCS lookups; approved maximum is ${String(maxFolios)}`,
    );
  }
  if (new Set(normalized).size !== normalized.length) {
    throw new Error("Broward BCS parcel IDs must be unique");
  }
  return normalized;
}

/**
 * Validate and parse one canonical BCS detail/list URL.
 *
 * @param {string} rawUrl - Candidate official BCS URL.
 * @param {string} expectedPresentation - Exact POSSE presentation name.
 * @param {boolean} [requireObjectId=true] - Whether a numeric object identifier is required.
 * @returns {{ url: string, objectId: string | null }} Canonical URL and POSSE object id.
 */
function validateBrowardBcsUrl(
  rawUrl,
  expectedPresentation,
  requireObjectId = true,
) {
  const parsed = new URL(rawUrl, BROWARD_BCS_SEARCH_URL);
  if (
    parsed.origin !== BROWARD_BCS_ORIGIN ||
    parsed.pathname.toLowerCase() !== BROWARD_BCS_PATH.toLowerCase() ||
    parsed.searchParams.get("PossePresentation") !== expectedPresentation
  ) {
    throw new Error(
      `Unexpected Broward BCS source URL for ${expectedPresentation}: ${parsed.toString()}`,
    );
  }
  const objectId = parsed.searchParams.get("PosseObjectId");
  if (requireObjectId && (objectId === null || !/^\d+$/u.test(objectId))) {
    throw new Error(
      `Broward BCS ${expectedPresentation} URL lacks a numeric PosseObjectId`,
    );
  }
  if (objectId !== null && !/^\d+$/u.test(objectId)) {
    throw new Error(
      `Broward BCS ${expectedPresentation} URL has an invalid PosseObjectId`,
    );
  }
  parsed.hash = "";
  return { url: parsed.toString(), objectId };
}

/**
 * Parse a strict BCS calendar date without locale-dependent Date parsing.
 *
 * @param {string | null} value - Source date text.
 * @param {string} fieldName - Field name used in failures.
 * @returns {string | null} ISO `YYYY-MM-DD`, including null for BCS's placeholder.
 */
function parseBrowardBcsDate(value, fieldName) {
  if (value === null) return null;
  if (/^mmm\s+dd,\s+yyyy$/iu.test(value)) return null;

  const numeric = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/u.exec(value);
  if (numeric !== null) {
    return validateCalendarDate(
      Number(numeric[3]),
      Number(numeric[1]),
      Number(numeric[2]),
      fieldName,
      value,
    );
  }

  const named = /^([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})$/u.exec(value);
  if (named !== null) {
    const monthName = named[1];
    if (monthName === undefined) {
      throw new Error(`Invalid Broward BCS ${fieldName}: ${value}`);
    }
    const monthIndex = [
      "jan",
      "feb",
      "mar",
      "apr",
      "may",
      "jun",
      "jul",
      "aug",
      "sep",
      "oct",
      "nov",
      "dec",
    ].indexOf(monthName.toLowerCase());
    if (monthIndex >= 0) {
      return validateCalendarDate(
        Number(named[3]),
        monthIndex + 1,
        Number(named[2]),
        fieldName,
        value,
      );
    }
  }
  throw new Error(`Invalid Broward BCS ${fieldName}: ${value}`);
}

/**
 * Validate date parts and render an ISO calendar date.
 *
 * @param {number} year - Four-digit year.
 * @param {number} month - One-based month.
 * @param {number} day - One-based day of month.
 * @param {string} fieldName - Source field used in failures.
 * @param {string} original - Original source text.
 * @returns {string} ISO calendar date.
 */
function validateCalendarDate(year, month, day, fieldName, original) {
  const parsed = new Date(Date.UTC(year, month - 1, day));
  if (
    parsed.getUTCFullYear() !== year ||
    parsed.getUTCMonth() !== month - 1 ||
    parsed.getUTCDate() !== day
  ) {
    throw new Error(`Invalid Broward BCS ${fieldName}: ${original}`);
  }
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

/**
 * Parse an optional non-negative source number with comma grouping.
 *
 * @param {string | null} value - Source number text.
 * @param {string} fieldName - Source field used in failures.
 * @returns {number | null} Finite non-negative number.
 */
function parseOptionalNumber(value, fieldName) {
  if (value === null) return null;
  if (!/^\d+(?:,\d{3})*(?:\.\d+)?$/u.test(value)) {
    throw new Error(`Invalid Broward BCS ${fieldName}: ${value}`);
  }
  const parsed = Number(value.replace(/,/g, ""));
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`Invalid Broward BCS ${fieldName}: ${value}`);
  }
  return parsed;
}

/**
 * Parse an optional public dollar value.
 *
 * @param {string | null} value - Source currency text.
 * @returns {number | null} Finite non-negative dollar value.
 */
function parseOptionalCurrency(value) {
  if (value === null) return null;
  if (!/^\$\d+(?:,\d{3})*(?:\.\d{2})?$/u.test(value)) {
    throw new Error(`Invalid Broward BCS job value: ${value}`);
  }
  return parseOptionalNumber(value.slice(1), "job value");
}

/**
 * Read a top-level POSSE field whose generated id ends in the page object id.
 *
 * @param {import("cheerio").CheerioAPI} $ - Parsed detail document.
 * @param {string} fieldPrefix - Stable POSSE field-name prefix.
 * @param {string} objectId - Current detail-page object identifier.
 * @returns {string | null} Public field text.
 */
function readDetailField($, fieldPrefix, objectId) {
  const matches = $(
    `span[id^="${fieldPrefix}_"][id$="_${objectId}_sp"]`,
  ).toArray();
  if (matches.length > 1) {
    throw new Error(
      `Broward BCS detail has duplicate ${fieldPrefix} fields for ${objectId}`,
    );
  }
  return matches.length === 0 ? null : readSelectionText($(matches[0]));
}

/**
 * Read a generated field from one POSSE grid row.
 *
 * @param {import("cheerio").Cheerio<import("domhandler").AnyNode>} row - Parsed grid row.
 * @param {string} fieldPrefix - Stable POSSE grid field prefix.
 * @param {string} objectId - Row object identifier.
 * @returns {string | null} Public field text.
 */
function readGridField(row, fieldPrefix, objectId) {
  const matches = row
    .find(`span[id^="${fieldPrefix}_"][id$="_${objectId}_sp"]`)
    .toArray();
  if (matches.length > 1) {
    throw new Error(
      `Broward BCS grid has duplicate ${fieldPrefix} fields for ${objectId}`,
    );
  }
  const match = matches[0];
  return match === undefined
    ? null
    : readSelectionText(row.find(`#${match.attribs.id}`));
}

/**
 * Parse the official parcel-list page into supported record links.
 *
 * A valid parcel with no permits is an explicit successful empty result only
 * when BCS renders its exact no-permits message. Zero rows without that marker,
 * unsupported row kinds, malformed dates, and non-BCS links all fail closed.
 *
 * @param {string} html - Raw official BCS parcel-list HTML.
 * @param {string} listUrl - Final official parcel-list URL.
 * @returns {BrowardBcsPermitListParseResult} Strict list outcome.
 */
export function parseBrowardBcsPermitListHtml(html, listUrl) {
  const validatedList = validateBrowardBcsUrl(listUrl, "ParcelPermitList");
  const parcelObjectId = requireText(
    validatedList.objectId,
    "parcel object id",
  );
  const $ = cheerio.load(html);
  const title = readText($("title").text());
  if (title !== "BCS - Permits") {
    throw new Error(
      `Unexpected Broward BCS parcel-list title: ${title ?? "(missing)"}`,
    );
  }

  const bodyText = readText($("body").text()) ?? "";
  const rows = $("tr.possegrid")
    .toArray()
    .filter((row) =>
      $(row)
        .find('a[href*="PosseObjectId"]')
        .toArray()
        .some((anchor) => {
          const href = $(anchor).attr("href") ?? "";
          return /PossePresentation=(?:ViewMasterPermit|ViewPermit|ViewPlanReview)/u.test(
            href,
          );
        }),
    );
  const hasNoPermitsMarker = bodyText.includes(SOURCE_NO_PERMITS_TEXT);

  if (rows.length === 0) {
    if (!hasNoPermitsMarker) {
      throw new Error(
        "Broward BCS parcel list has no records and no explicit no-permits marker",
      );
    }
    return {
      status: "no_permits",
      parcelObjectId,
      listedRecordCount: 0,
      excludedPlanReviewCount: 0,
      records: [],
    };
  }
  if (hasNoPermitsMarker) {
    throw new Error(
      "Broward BCS parcel list contains both records and a no-permits marker",
    );
  }

  /** @type {BrowardBcsListRecord[]} */
  const records = [];
  let excludedPlanReviewCount = 0;
  for (const rowElement of rows) {
    const row = $(rowElement);
    const cells = row.children("td,th").toArray();
    if (cells.length !== 7) {
      throw new Error(
        `Unexpected Broward BCS parcel-list column count: ${String(cells.length)}`,
      );
    }
    const values = cells.map((cell) => readSelectionText($(cell)));
    const permitNumber = requireText(values[1] ?? null, "permit number");
    const sourceKind = requireText(values[2] ?? null, "record kind");
    const recordType = requireText(values[3] ?? null, "record type");
    const recordStatus = requireText(values[4] ?? null, "record status");
    const issueDate = parseBrowardBcsDate(
      values[5] ?? null,
      "permit issue date",
    );
    const listContractor = values[6] ?? null;
    /** @type {BrowardBcsListRecordKind} */
    let recordKind;
    /** @type {string} */
    let expectedPresentation;
    if (sourceKind === "Master") {
      recordKind = "master";
      expectedPresentation = "ViewMasterPermit";
    } else if (sourceKind === "Permit") {
      recordKind = "permit";
      expectedPresentation = "ViewPermit";
    } else if (sourceKind === "Plan Review") {
      recordKind = "plan_review";
      expectedPresentation = "ViewPlanReview";
    } else {
      throw new Error(`Unsupported Broward BCS record kind: ${sourceKind}`);
    }

    const anchors = row
      .find('a[href*="PosseObjectId"]')
      .toArray()
      .filter((anchor) =>
        ($(anchor).attr("href") ?? "").includes(
          `PossePresentation=${expectedPresentation}`,
        ),
      );
    if (anchors.length !== 1) {
      throw new Error(
        `Broward BCS ${sourceKind} row must have exactly one detail link`,
      );
    }
    const validatedDetail = validateBrowardBcsUrl(
      $(anchors[0]).attr("href") ?? "",
      expectedPresentation,
    );
    const sourceObjectId = requireText(
      validatedDetail.objectId,
      `${sourceKind} object id`,
    );
    if (recordKind === "plan_review") {
      excludedPlanReviewCount += 1;
      continue;
    }
    records.push({
      sourceUrl: validatedDetail.url,
      sourceObjectId,
      sourceRecordKind: recordKind,
      permitNumber,
      recordType,
      recordStatus,
      permitIssueDate: issueDate,
      listContractor,
    });
  }

  if (records.length === 0) {
    throw new Error(
      "Broward BCS parcel list contains plan reviews but no permit or master record",
    );
  }
  return {
    status: "records",
    parcelObjectId,
    listedRecordCount: rows.length,
    excludedPlanReviewCount,
    records,
  };
}

/**
 * Validate BCS's separately displayed legacy folio.
 *
 * BCS currently prints a ten-character, dashed legacy folio while its Parcel ID
 * search accepts the full 12-character BCPA key. No official conversion between
 * those identifiers was found, and observed values are not suffix-equivalent.
 * A ten-character legacy value is therefore preserved but never inferred to be
 * the BCPA key. If BCS prints 12 characters, they must match exactly.
 *
 * @param {string} parcelIdentifier - Exact submitted 12-character parcel ID.
 * @param {string} sourceFolio - BCS legacy folio display.
 * @returns {void}
 */
function assertMatchingSourceFolio(parcelIdentifier, sourceFolio) {
  const compactSourceFolio = sourceFolio.replace(/[-\s]/g, "").toUpperCase();
  if (
    !/^[A-Z0-9]{10}(?:[A-Z0-9]{2})?$/u.test(compactSourceFolio) ||
    (compactSourceFolio.length === 12 &&
      compactSourceFolio !== parcelIdentifier)
  ) {
    throw new Error(
      `Broward BCS detail folio ${sourceFolio} does not match submitted parcel ${parcelIdentifier}`,
    );
  }
}

/**
 * Parse inspection rows exposed directly on one permit detail page.
 *
 * @param {import("cheerio").CheerioAPI} $ - Parsed permit detail document.
 * @returns {readonly BrowardBcsInspection[]} Strict, source-order inspection rows.
 */
function parseBrowardBcsInspections($) {
  /** @type {BrowardBcsInspection[]} */
  const inspections = [];
  const seenObjectIds = new Set();
  for (const rowElement of $("tr.possegrid").toArray()) {
    const row = $(rowElement);
    const links = row
      .find('a[href*="PossePresentation=ViewInspection"]')
      .toArray();
    if (links.length === 0) continue;
    if (links.length !== 1) {
      throw new Error(
        "Broward BCS inspection row must have exactly one detail link",
      );
    }
    const validated = validateBrowardBcsUrl(
      $(links[0]).attr("href") ?? "",
      "ViewInspection",
    );
    const objectId = requireText(validated.objectId, "inspection object id");
    if (seenObjectIds.has(objectId)) {
      throw new Error(`Duplicate Broward BCS inspection object ${objectId}`);
    }
    seenObjectIds.add(objectId);
    inspections.push({
      source_url: validated.url,
      source_object_id: objectId,
      inspection_type: requireText(
        readGridField(row, "InspectionType", objectId),
        "inspection type",
      ),
      requested_date: parseBrowardBcsDate(
        readGridField(row, "RequestedDate", objectId),
        "inspection requested date",
      ),
      result: requireText(
        readGridField(row, "Outcome", objectId),
        "inspection outcome",
      ),
      completed_date: parseBrowardBcsDate(
        readGridField(row, "DateCompleted", objectId),
        "inspection completion date",
      ),
    });
  }
  return inspections;
}

/**
 * Parse and reconcile one official master/permit detail page against its list row.
 *
 * Owner name and owner mailing address are deliberately not copied into the
 * normalized record. Permit-holder/license, work location, legal description,
 * valuation/use fields, dates, and permit-page inspection history are retained
 * only for local private staging.
 *
 * @param {string} html - Raw official BCS detail HTML.
 * @param {object} context - Identity and provenance established by the property search.
 * @param {BrowardBcsListRecord} context.listRecord - Strict source list row.
 * @param {string} context.parcelIdentifier - Exact submitted BCPA parcel ID.
 * @param {string} context.sourceSearchUrl - Official BCS property search URL.
 * @param {string} context.sourceListUrl - Official resolved parcel list URL.
 * @returns {NormalizedBrowardBcsPermit} Reconciled local private-staging record.
 */
export function parseBrowardBcsDetailHtml(
  html,
  {
    listRecord,
    parcelIdentifier: rawParcelIdentifier,
    sourceSearchUrl,
    sourceListUrl,
  },
) {
  const parcelIdentifier = normalizeBrowardBcsParcelId(rawParcelIdentifier);
  const expectedPresentation =
    listRecord.sourceRecordKind === "master"
      ? "ViewMasterPermit"
      : "ViewPermit";
  const validatedDetail = validateBrowardBcsUrl(
    listRecord.sourceUrl,
    expectedPresentation,
  );
  const sourceObjectId = requireText(
    validatedDetail.objectId,
    "detail object id",
  );
  const validatedSearch = validateBrowardBcsUrl(
    sourceSearchUrl,
    "ParcelSearchByAddress",
    false,
  );
  const validatedList = validateBrowardBcsUrl(
    sourceListUrl,
    "ParcelPermitList",
  );

  const $ = cheerio.load(html);
  const detailPageTitle = requireText(
    readText($("title").text()),
    "detail page title",
  );
  const expectedTitle =
    listRecord.sourceRecordKind === "master"
      ? "BCS - Permit Application"
      : "BCS - Permit";
  if (detailPageTitle !== expectedTitle) {
    const body = readText($("body").text()) ?? "";
    const reason = body.includes("invalid POSSE parameter")
      ? "invalid POSSE parameter"
      : detailPageTitle;
    throw new Error(
      `Unexpected Broward BCS ${listRecord.sourceRecordKind} detail response: ${reason}`,
    );
  }

  const permitNumber = requireText(
    readDetailField($, "PermitNumber", sourceObjectId),
    "detail permit number",
  );
  const recordType = requireText(
    readDetailField(
      $,
      listRecord.sourceRecordKind === "master"
        ? "MasterPermitType"
        : "PermitType",
      sourceObjectId,
    ),
    "detail record type",
  );
  if (
    permitNumber !== listRecord.permitNumber ||
    recordType !== listRecord.recordType
  ) {
    throw new Error(
      `Broward BCS detail identity differs from parcel list for object ${sourceObjectId}`,
    );
  }

  const sourceFolioNumber = requireText(
    readDetailField($, "FolioNumber", sourceObjectId),
    "detail folio number",
  );
  assertMatchingSourceFolio(parcelIdentifier, sourceFolioNumber);
  const issuingJurisdiction = requireText(
    readDetailField($, "ParcelJurisdiction", sourceObjectId),
    "issuing jurisdiction",
  );
  const legalDescription = requireText(
    readDetailField($, "ParcelLegalDescription", sourceObjectId),
    "parcel legal description",
  );
  const workLocation =
    listRecord.sourceRecordKind === "master"
      ? requireText(
          [
            readDetailField($, "AddressLine1", sourceObjectId),
            readDetailField($, "AddressLine2", sourceObjectId),
          ]
            .filter((value) => value !== null)
            .join(" ") || null,
          "work location",
        )
      : requireText(
          readDetailField($, "AddressDisplay", sourceObjectId),
          "work location",
        );

  const masterStatus =
    listRecord.sourceRecordKind === "master"
      ? requireText(
          readDetailField($, "PendingWebLanguage", sourceObjectId),
          "master status",
        )
      : listRecord.recordStatus;
  if (masterStatus !== listRecord.recordStatus) {
    throw new Error(
      `Broward BCS detail status differs from parcel list for object ${sourceObjectId}`,
    );
  }

  const projectTitle =
    listRecord.sourceRecordKind === "master"
      ? readDetailField($, "ProjectTitle", sourceObjectId)
      : null;
  const projectDescription = readDetailField(
    $,
    listRecord.sourceRecordKind === "master"
      ? "ProjectDescription"
      : "PermitDescription",
    sourceObjectId,
  );
  const applicationDate =
    listRecord.sourceRecordKind === "master"
      ? parseBrowardBcsDate(
          readDetailField($, "ApplicationDate", sourceObjectId),
          "application date",
        )
      : null;
  const expirationDate =
    listRecord.sourceRecordKind === "master"
      ? parseBrowardBcsDate(
          readDetailField($, "ExpirationDate", sourceObjectId),
          "expiration date",
        )
      : null;
  const buildingUse =
    listRecord.sourceRecordKind === "master"
      ? readDetailField($, "BuildingUse", sourceObjectId)
      : null;
  const presentUse =
    listRecord.sourceRecordKind === "master"
      ? readDetailField($, "PresentUse", sourceObjectId)
      : null;
  const proposedUse =
    listRecord.sourceRecordKind === "master"
      ? readDetailField($, "ProposedUse", sourceObjectId)
      : null;
  const jobValue =
    listRecord.sourceRecordKind === "master"
      ? parseOptionalCurrency(readDetailField($, "JobValue", sourceObjectId))
      : null;
  const squareFootage =
    listRecord.sourceRecordKind === "master"
      ? parseOptionalNumber(
          readDetailField($, "SquareFootage", sourceObjectId),
          "square footage",
        )
      : null;
  const occupantLoad =
    listRecord.sourceRecordKind === "master"
      ? parseOptionalNumber(
          readDetailField($, "OccupantLoad", sourceObjectId),
          "occupant load",
        )
      : null;
  const finishFloorAboveRoad =
    listRecord.sourceRecordKind === "master"
      ? parseOptionalNumber(
          readDetailField($, "FinishFloorAboveRoad", sourceObjectId),
          "finish floor above road",
        )
      : null;
  const finishFloorAboveSeaLevel =
    listRecord.sourceRecordKind === "master"
      ? parseOptionalNumber(
          readDetailField($, "FinishFloorAboveSeaLevel", sourceObjectId),
          "finish floor above sea level",
        )
      : null;
  const inspections =
    listRecord.sourceRecordKind === "permit"
      ? parseBrowardBcsInspections($)
      : [];
  const contractorName =
    readDetailField($, "GeneralContractor", sourceObjectId) ??
    listRecord.listContractor;
  const roofText = [recordType, projectTitle, projectDescription]
    .filter((value) => value !== null)
    .join(" ");

  return {
    source_system: BROWARD_BCS_SOURCE_SYSTEM,
    source_url: validatedDetail.url,
    source_search_url: validatedSearch.url,
    source_list_url: validatedList.url,
    source_object_id: sourceObjectId,
    source_record_kind: listRecord.sourceRecordKind,
    record_key: `${BROWARD_BCS_SOURCE_SYSTEM}:${sourceObjectId}`,
    parcel_identifier: parcelIdentifier,
    source_folio_number: sourceFolioNumber,
    issuing_jurisdiction: issuingJurisdiction,
    permit_number: permitNumber,
    record_status: listRecord.recordStatus,
    record_type: recordType,
    permit_issue_date: listRecord.permitIssueDate,
    application_date: applicationDate,
    expiration_date: expirationDate,
    project_title: projectTitle,
    project_description: projectDescription,
    work_location: workLocation,
    legal_description: legalDescription,
    contractor_name: contractorName,
    contractor_license: readDetailField(
      $,
      "ContractorLicenseDisplay",
      sourceObjectId,
    ),
    building_use: buildingUse,
    present_use: presentUse,
    proposed_use: proposedUse,
    job_value: jobValue,
    square_footage: squareFootage,
    occupancy_type:
      listRecord.sourceRecordKind === "master"
        ? readDetailField($, "OccupancyType", sourceObjectId)
        : null,
    construction_type:
      listRecord.sourceRecordKind === "master"
        ? readDetailField($, "TypeOfConstruction", sourceObjectId)
        : null,
    occupant_load: occupantLoad,
    finish_floor_above_road: finishFloorAboveRoad,
    finish_floor_above_sea_level: finishFloorAboveSeaLevel,
    inspections,
    is_roof_permit: /\broof(?:ing)?\b/iu.test(roofText),
    raw: {
      search_method: "ParcelID",
      reference_number:
        listRecord.sourceRecordKind === "permit"
          ? readDetailField($, "ProjectId", sourceObjectId)
          : null,
      list_contractor: listRecord.listContractor,
      detail_page_title: detailPageTitle,
    },
  };
}

/**
 * Deduplicate records by POSSE object identity and reject conflicting variants.
 *
 * @param {readonly NormalizedBrowardBcsPermit[]} records - Candidate normalized records.
 * @returns {readonly NormalizedBrowardBcsPermit[]} Deterministically sorted unique records.
 */
export function dedupeAndSortBrowardBcsPermits(records) {
  /** @type {Map<string, NormalizedBrowardBcsPermit>} */
  const byKey = new Map();
  for (const record of records) {
    const existing = byKey.get(record.record_key);
    if (
      existing !== undefined &&
      JSON.stringify(existing) !== JSON.stringify(record)
    ) {
      throw new Error(
        `Conflicting Broward BCS records for ${record.record_key}`,
      );
    }
    byKey.set(record.record_key, record);
  }
  return [...byKey.values()].sort(
    (left, right) =>
      left.parcel_identifier.localeCompare(right.parcel_identifier) ||
      left.permit_number.localeCompare(right.permit_number) ||
      left.source_record_kind.localeCompare(right.source_record_kind) ||
      Number(left.source_object_id) - Number(right.source_object_id),
  );
}

/**
 * Render deterministic normalized private-staging JSONL.
 *
 * @param {readonly NormalizedBrowardBcsPermit[]} records - Candidate normalized records.
 * @returns {string} Newline-delimited JSON with a trailing newline when non-empty.
 */
export function renderBrowardBcsPermitJsonl(records) {
  const normalized = dedupeAndSortBrowardBcsPermits(records);
  return normalized.length === 0
    ? ""
    : `${normalized.map((record) => JSON.stringify(record)).join("\n")}\n`;
}

/**
 * Pause between official source requests.
 *
 * @param {number} milliseconds - Delay duration.
 * @returns {Promise<void>} Resolves after the requested delay.
 */
function delay(milliseconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

/**
 * Resolve a usable local Chrome executable without downloading a browser.
 *
 * @returns {string | null} Configured/system Puppeteer executable path.
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
 * Fetch one public BCS detail document with the anonymous browser session.
 *
 * @param {string} sourceUrl - Validated official detail URL.
 * @param {object} context - Anonymous session and timeout context.
 * @param {string} context.cookieHeader - Browser session cookies.
 * @param {string} context.userAgent - Browser user-agent string.
 * @param {number} context.timeoutMs - Per-document timeout.
 * @returns {Promise<string>} Raw source HTML.
 */
async function fetchBrowardBcsDetailHtml(
  sourceUrl,
  { cookieHeader, userAgent, timeoutMs },
) {
  const response = await fetch(sourceUrl, {
    method: "GET",
    redirect: "follow",
    headers: {
      accept: "text/html,application/xhtml+xml",
      "user-agent": userAgent,
      ...(cookieHeader.length === 0 ? {} : { cookie: cookieHeader }),
    },
    signal: AbortSignal.timeout(timeoutMs),
  });
  if (!response.ok || response.url !== sourceUrl) {
    throw new Error(
      `Broward BCS detail request failed (${String(response.status)}) for ${sourceUrl}`,
    );
  }
  const contentType = response.headers.get("content-type") ?? "";
  if (!/^text\/html\b/iu.test(contentType)) {
    throw new Error(
      `Broward BCS detail returned unexpected content type: ${contentType || "(missing)"}`,
    );
  }
  const html = await response.text();
  if (
    Buffer.byteLength(html, "utf8") === 0 ||
    Buffer.byteLength(html, "utf8") > MAX_SOURCE_HTML_BYTES
  ) {
    throw new Error(
      `Broward BCS detail returned an invalid HTML size for ${sourceUrl}`,
    );
  }
  return html;
}

/**
 * Configure one browser page for the official JS/session-cookie search.
 *
 * Image, stylesheet, font, and media resources are irrelevant to POSSE's form
 * logic and are blocked to keep this local pilot light. Documents, scripts,
 * XHR, and fetch requests remain enabled.
 *
 * @param {import("puppeteer").Page} page - Fresh anonymous browser page.
 * @returns {Promise<void>} Resolves after request interception is installed.
 */
async function configureSearchPage(page) {
  await page.setRequestInterception(true);
  page.on("request", (request) => {
    if (
      ["image", "stylesheet", "font", "media"].includes(request.resourceType())
    ) {
      void request.abort();
      return;
    }
    void request.continue();
  });
}

/**
 * Execute the bounded local-only property-first BCS pilot.
 *
 * Searches are sequential through BCS's rendered Parcel ID field. Every
 * supported master/permit detail page is then fetched sequentially using the
 * same anonymous session. More than five properties, 125 listed rows for one
 * property, or 75 detail pages for one property fails before an unbounded
 * traversal can begin. Inspection detail links are preserved as provenance but
 * not traversed.
 *
 * @param {object} params - Bounded pilot parameters.
 * @param {readonly string[]} params.parcelIds - One through five exact BCPA parcel IDs.
 * @param {number} [params.maxFolios=5] - Property ceiling, never above five.
 * @param {number} [params.propertyDelayMs=1500] - Delay between properties, minimum 1000 ms.
 * @param {number} [params.detailDelayMs=300] - Delay between detail pages, minimum 250 ms.
 * @param {number} [params.navigationTimeoutMs=45000] - Rendered search navigation timeout.
 * @param {number} [params.detailTimeoutMs=30000] - Individual detail-fetch timeout.
 * @param {number} [params.maxDetailPagesPerFolio=75] - Detail-page ceiling, never above 75.
 * @param {boolean} [params.roofOnly=false] - Detail only list rows explicitly marked roofing.
 * @returns {Promise<BrowardBcsProbeResult>} Normalized records and explicit source outcomes.
 */
export async function probeBrowardBcsPermits({
  parcelIds: rawParcelIds,
  maxFolios = MAX_PILOT_FOLIOS,
  propertyDelayMs = 1_500,
  detailDelayMs = 300,
  navigationTimeoutMs = 45_000,
  detailTimeoutMs = 30_000,
  maxDetailPagesPerFolio = MAX_DETAIL_PAGES_PER_FOLIO,
  roofOnly = false,
}) {
  const parcelIds = validateBrowardBcsParcelIds(rawParcelIds, maxFolios);
  if (
    !Number.isInteger(propertyDelayMs) ||
    propertyDelayMs < MIN_PROPERTY_DELAY_MS
  ) {
    throw new Error(
      `Broward BCS propertyDelayMs must be at least ${String(MIN_PROPERTY_DELAY_MS)}`,
    );
  }
  if (!Number.isInteger(detailDelayMs) || detailDelayMs < MIN_DETAIL_DELAY_MS) {
    throw new Error(
      `Broward BCS detailDelayMs must be at least ${String(MIN_DETAIL_DELAY_MS)}`,
    );
  }
  if (
    !Number.isInteger(maxDetailPagesPerFolio) ||
    maxDetailPagesPerFolio <= 0 ||
    maxDetailPagesPerFolio > MAX_DETAIL_PAGES_PER_FOLIO
  ) {
    throw new Error(
      `Broward BCS maxDetailPagesPerFolio must be from 1 through ${String(MAX_DETAIL_PAGES_PER_FOLIO)}`,
    );
  }
  if (
    !Number.isInteger(navigationTimeoutMs) ||
    navigationTimeoutMs <= 0 ||
    !Number.isInteger(detailTimeoutMs) ||
    detailTimeoutMs <= 0
  ) {
    throw new Error("Broward BCS source timeouts must be positive integers");
  }

  const executablePath = resolveChromeExecutablePath();
  const browser = await puppeteer.launch({
    headless: true,
    ...(executablePath === null ? {} : { executablePath }),
  });
  /** @type {NormalizedBrowardBcsPermit[]} */
  const records = [];
  /** @type {BrowardBcsLookupObservation[]} */
  const observations = [];
  const userAgent = await browser.userAgent();

  try {
    for (const [parcelIndex, parcelIdentifier] of parcelIds.entries()) {
      const startedAt = Date.now();
      const page = await browser.newPage();
      await configureSearchPage(page);
      /** @type {string} */
      let sourceListUrl;
      /** @type {string} */
      let cookieHeader;
      /** @type {BrowardBcsPermitListParseResult} */
      let parsedList;
      try {
        await page.goto(BROWARD_BCS_SEARCH_URL, {
          waitUntil: "domcontentloaded",
          timeout: navigationTimeoutMs,
        });
        if ((await page.title()) !== "BCS - Search for Permit by Address") {
          throw new Error(
            "Broward BCS search page returned an unexpected title",
          );
        }
        await page.waitForSelector(PARCEL_INPUT_SELECTOR, {
          timeout: navigationTimeoutMs,
        });
        await page.locator(PARCEL_INPUT_SELECTOR).fill(parcelIdentifier);
        await page.$eval(PARCEL_INPUT_SELECTOR, (element) => {
          if (!(element instanceof HTMLInputElement)) {
            throw new Error("BCS Parcel ID control is not an input");
          }
          element.dispatchEvent(new Event("change", { bubbles: true }));
        });
        const submittedValue = await page.$eval(
          PARCEL_INPUT_SELECTOR,
          (element) =>
            element instanceof HTMLInputElement ? element.value : null,
        );
        const dataChanges = await page.$eval("#datachanges", (element) =>
          element instanceof HTMLInputElement ? element.value : null,
        );
        if (
          submittedValue !== parcelIdentifier ||
          typeof dataChanges !== "string" ||
          !dataChanges.includes(parcelIdentifier)
        ) {
          throw new Error(
            `Broward BCS did not preserve parcel ID ${parcelIdentifier} in its submitted form state`,
          );
        }
        await Promise.all([
          page.waitForNavigation({
            waitUntil: "domcontentloaded",
            timeout: navigationTimeoutMs,
          }),
          page.click(SEARCH_BUTTON_SELECTOR),
        ]);

        sourceListUrl = page.url();
        const finalUrl = new URL(sourceListUrl);
        if (
          finalUrl.searchParams.get("PossePresentation") !== "ParcelPermitList"
        ) {
          const bodyText = readText(
            await page.$eval("body", (element) => element.textContent),
          );
          if (bodyText?.includes(SOURCE_NO_MATCH_TEXT)) {
            throw new Error(
              `Broward BCS did not resolve parcel ID ${parcelIdentifier}; source reported no matching criteria`,
            );
          }
          throw new Error(
            `Broward BCS search for ${parcelIdentifier} did not resolve to a parcel permit list`,
          );
        }
        const listHtml = await page.content();
        parsedList = parseBrowardBcsPermitListHtml(listHtml, sourceListUrl);
        if (parsedList.listedRecordCount > MAX_LIST_ROWS_PER_FOLIO) {
          throw new Error(
            `Broward BCS parcel ${parcelIdentifier} has ${String(parsedList.listedRecordCount)} rows; hard limit is ${String(MAX_LIST_ROWS_PER_FOLIO)}`,
          );
        }
        const detailRecords = roofOnly
          ? parsedList.records.filter(isBrowardBcsRoofPermitCandidate)
          : parsedList.records;
        if (detailRecords.length > maxDetailPagesPerFolio) {
          throw new Error(
            `Broward BCS parcel ${parcelIdentifier} needs ${String(detailRecords.length)} detail requests; hard limit is ${String(maxDetailPagesPerFolio)}`,
          );
        }
        const cookies = await page.cookies();
        cookieHeader = cookies
          .map((cookie) => `${cookie.name}=${cookie.value}`)
          .join("; ");
      } finally {
        await page.close().catch(() => undefined);
      }

      /** @type {NormalizedBrowardBcsPermit[]} */
      const parcelRecords = [];
      const detailRecords = roofOnly
        ? parsedList.records.filter(isBrowardBcsRoofPermitCandidate)
        : parsedList.records;
      for (const [detailIndex, listRecord] of detailRecords.entries()) {
        if (detailIndex > 0) await delay(detailDelayMs);
        const detailHtml = await fetchBrowardBcsDetailHtml(
          listRecord.sourceUrl,
          {
            cookieHeader,
            userAgent,
            timeoutMs: detailTimeoutMs,
          },
        );
        parcelRecords.push(
          parseBrowardBcsDetailHtml(detailHtml, {
            listRecord,
            parcelIdentifier,
            sourceSearchUrl: BROWARD_BCS_SEARCH_URL,
            sourceListUrl,
          }),
        );
      }
      records.push(...parcelRecords);
      observations.push({
        parcelIdentifier,
        status: parsedList.status,
        sourceSearchUrl: BROWARD_BCS_SEARCH_URL,
        sourceListUrl,
        parcelObjectId: parsedList.parcelObjectId,
        listedRecordCount: parsedList.listedRecordCount,
        excludedPlanReviewCount: parsedList.excludedPlanReviewCount,
        normalizedRecordCount: parcelRecords.length,
        detailPageCount: detailRecords.length,
        elapsedMs: Date.now() - startedAt,
      });
      if (parcelIndex < parcelIds.length - 1) {
        await delay(propertyDelayMs);
      }
    }
  } finally {
    await browser.close().catch(() => undefined);
  }

  return {
    records: dedupeAndSortBrowardBcsPermits(records),
    observations,
  };
}
