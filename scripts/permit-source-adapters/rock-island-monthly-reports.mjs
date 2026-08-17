// @ts-check

import { getDocument } from "pdfjs-dist/legacy/build/pdf.mjs";

/**
 * @typedef {object} RockIslandReportLink
 * @property {string} documentId - CivicPlus DocumentCenter identifier.
 * @property {string} title - Human-readable anchor text from the official report index.
 * @property {string} url - Absolute official PDF URL.
 */

/**
 * @typedef {object} PositionedText
 * @property {string} text - Collapsed, non-empty PDF text.
 * @property {number} x - Horizontal PDF coordinate.
 * @property {number} y - Vertical PDF coordinate.
 * @property {number} pageNumber - One-based PDF page number.
 */

/**
 * @typedef {object} RockIslandReportSource
 * @property {string} documentId - CivicPlus DocumentCenter identifier.
 * @property {string} title - Official report-index anchor text.
 * @property {string} url - Official PDF URL.
 */

/**
 * @typedef {object} RockIslandMonthlyPermit
 * @property {string} source_system - Stable city/report source key.
 * @property {string} source_url - Official monthly report URL.
 * @property {string} city - Issuing city.
 * @property {string} permit_number - Public permit number.
 * @property {null} parcel_identifier - Always null until an explicit appraisal join proves a match.
 * @property {string | null} work_location - Public project location retained only in private staging.
 * @property {string | null} permit_issue_date - `YYYY-MM-DD` issue date.
 * @property {"Issued"} record_status - Status proven by the official issued-permit report index.
 * @property {string | null} record_type - Verbatim report section code when available.
 * @property {string | null} project_description - Public report purpose retained only in private staging.
 * @property {readonly string[]} contractor_business_names - Conservatively recognized business entities labelled Contractor.
 * @property {boolean} is_roof_permit - Conservative source-text classification.
 * @property {{
 *   source_document_id: string,
 *   source_report_title: string,
 *   source_report_section: string | null,
 *   source_tax_map: string | null,
 *   parcel_match_evidence: "source_tax_map_only_not_joined",
 *   project_valuation: number | null,
 *   source_page: number,
 *   source_pages?: readonly number[],
 *   source_sections?: readonly string[],
 *   alternate_work_locations?: readonly string[],
 *   source_tax_map_variants?: readonly string[],
 *   project_valuation_variants?: readonly number[]
 * }} raw - Allow-listed report provenance and non-contact source fields.
 */

const REPORT_INDEX_URL = "https://www.rigov.org/1276/Permit-Reports";
const REPORT_SOURCE_SYSTEM = "rock_island_city_official_monthly_permit_reports";

/**
 * Narrow an unknown value to a non-array object.
 *
 * @param {unknown} value - Candidate value.
 * @returns {value is Record<string, unknown>} True for object records.
 */
function isRecord(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    Array.isArray(value) === false
  );
}

/**
 * Collapse whitespace and reject empty text.
 *
 * @param {unknown} value - Candidate source text.
 * @returns {string | null} Normalized text.
 */
function readText(value) {
  if (typeof value !== "string") return null;
  const normalized = value.replace(/\s+/g, " ").trim();
  return normalized.length > 0 ? normalized : null;
}

/**
 * Decode the small HTML entity subset used in CivicPlus link labels.
 *
 * @param {string} value - HTML text.
 * @returns {string} Plain text.
 */
function decodeHtmlText(value) {
  return value
    .replace(/<[^>]*>/g, " ")
    .replace(/&nbsp;|&#160;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Discover unique official monthly report links from the CivicPlus index.
 *
 * @param {string} html - Raw official permit-report index HTML.
 * @param {string} [indexUrl=REPORT_INDEX_URL] - Absolute index URL used for resolution.
 * @returns {readonly RockIslandReportLink[]} Links sorted by numeric document id.
 */
export function extractRockIslandReportLinks(
  html,
  indexUrl = REPORT_INDEX_URL,
) {
  /** @type {Map<string, RockIslandReportLink>} */
  const byDocumentId = new Map();
  const hrefPattern =
    /href=["']([^"']*\/DocumentCenter\/View\/(\d+)(?:\/[^"'?#]*)?[^"']*)["']/gi;
  for (const match of html.matchAll(hrefPattern)) {
    const href = match[1];
    const documentId = match[2];
    if (href === undefined || documentId === undefined) continue;
    const hrefEnd = (match.index ?? 0) + match[0].length;
    const openingTagEnd = html.indexOf(">", hrefEnd);
    const contentStart = openingTagEnd === -1 ? hrefEnd : openingTagEnd + 1;
    const closingAnchor = html.indexOf("</a>", contentStart);
    const contentEnd =
      closingAnchor === -1
        ? Math.min(contentStart + 500, html.length)
        : Math.min(closingAnchor, contentStart + 500);
    const title =
      decodeHtmlText(html.slice(contentStart, contentEnd)) ||
      `Document ${documentId}`;
    if (
      !/\b(?:january|february|march|april|may|june|july|august|september|october|november|december)\b/i.test(
        title,
      )
    ) {
      continue;
    }
    const url = new URL(href.replace(/&amp;/gi, "&"), indexUrl).toString();
    const existing = byDocumentId.get(documentId);
    if (existing === undefined || title.length > existing.title.length) {
      byDocumentId.set(documentId, { documentId, title, url });
    }
  }
  return [...byDocumentId.values()].sort(
    (left, right) => Number(left.documentId) - Number(right.documentId),
  );
}

/**
 * Extract positioned text from an official permit-report PDF.
 *
 * Coordinates are retained because the report is a fixed-column table whose reading
 * order interleaves address, owner/contractor, purpose, value, and tax-map fields.
 *
 * @param {Uint8Array} pdfBytes - Complete PDF bytes.
 * @returns {Promise<readonly (readonly PositionedText[])[]>} Positioned text by page.
 */
export async function extractPositionedPdfText(pdfBytes) {
  const loadingTask = getDocument({ data: pdfBytes });
  const pdf = await loadingTask.promise;
  /** @type {PositionedText[][]} */
  const pages = [];
  try {
    for (let pageNumber = 1; pageNumber <= pdf.numPages; pageNumber += 1) {
      const page = await pdf.getPage(pageNumber);
      const content = await page.getTextContent();
      /** @type {PositionedText[]} */
      const items = [];
      for (const value of content.items) {
        if (!isRecord(value) || !("str" in value) || !("transform" in value)) {
          continue;
        }
        const text = readText(value.str);
        const transform = value.transform;
        if (
          text === null ||
          !Array.isArray(transform) ||
          typeof transform[4] !== "number" ||
          typeof transform[5] !== "number"
        ) {
          continue;
        }
        items.push({
          text,
          x: transform[4],
          y: transform[5],
          pageNumber,
        });
      }
      pages.push(items);
      page.cleanup();
    }
  } finally {
    await loadingTask.destroy();
  }
  return pages;
}

/**
 * Recognize a permit number in the first table column.
 *
 * @param {PositionedText} item - Positioned PDF text.
 * @returns {boolean} True for a permit-row starter.
 */
function isPermitNumberItem(item) {
  return (
    item.x >= 15 &&
    item.x < 60 &&
    /^(?=[A-Z0-9-]*\d)[A-Z][A-Z0-9-]{3,}$/i.test(item.text) &&
    !/^permit$/i.test(item.text)
  );
}

/**
 * Recognize a report-section code in the narrow left margin.
 *
 * @param {PositionedText} item - Positioned PDF text.
 * @returns {boolean} True for a source section heading.
 */
function isSectionItem(item) {
  return (
    item.x < 15 &&
    /^[a-z][a-z0-9_-]{2,}$/i.test(item.text) &&
    !/^page$/i.test(item.text)
  );
}

/**
 * Parse one US report date.
 *
 * @param {string} value - Candidate `MM/DD/YYYY` text.
 * @returns {string | null} ISO date.
 */
function parseReportDate(value) {
  const match = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(value);
  if (match === null) return null;
  const month = Number(match[1]);
  const day = Number(match[2]);
  const year = Number(match[3]);
  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    return null;
  }
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

/**
 * Parse a source currency value.
 *
 * @param {string | null} value - Candidate currency text.
 * @returns {number | null} Finite dollar amount.
 */
function parseCurrency(value) {
  if (value === null || !/^\$[\d,]+(?:\.\d{2})?$/.test(value)) return null;
  const parsed = Number(value.replace(/[$,]/g, ""));
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Join positioned items in visual line order.
 *
 * @param {readonly PositionedText[]} items - Table-cell items.
 * @returns {string | null} Collapsed cell text.
 */
function joinVisualText(items) {
  const joined = [...items]
    .sort((left, right) => right.y - left.y || left.x - right.x)
    .map((item) => item.text)
    .join(" ")
    .replace(/\s+/g, " ")
    .trim();
  return joined.length > 0 ? joined : null;
}

/**
 * Keep contractor strings only when source text contains clear organization evidence.
 *
 * This intentionally drops person-looking sole-proprietor names. The full source label
 * remains in the private PDF, but no owner/person rows are copied into JSONL.
 *
 * @param {string} value - Party text labelled Contractor by the source report.
 * @returns {boolean} True when the text has an organization/business marker.
 */
export function isConservativeBusinessName(value) {
  return /\b(?:llc|l\.l\.c\.|inc\.?|incorporated|corp\.?|corporation|company|co\.|construction|contracting|contractor|electric|electrical|plumbing|heating|cooling|roofing|restoration|excavat(?:ing|ion)|development|properties|property|services|service|group|homes|builders?|enterprises?|systems?|city of|church|university|school|foundation|association|authority|department|dba)\b/i.test(
    value,
  );
}

/**
 * Read contractor business names without retaining owner/person fields.
 *
 * @param {readonly PositionedText[]} block - One permit's table block.
 * @returns {readonly string[]} Unique conservatively recognized businesses.
 */
function readContractorBusinessNames(block) {
  /** @type {Set<string>} */
  const names = new Set();
  for (const role of block.filter(
    (item) => item.x >= 270 && item.x < 335 && /^contractor$/i.test(item.text),
  )) {
    const party = joinVisualText(
      block.filter(
        (item) =>
          item.x >= 110 &&
          item.x < 270 &&
          item.y >= role.y - 1.5 &&
          item.y <= role.y + 12,
      ),
    );
    if (party !== null && isConservativeBusinessName(party)) names.add(party);
  }
  return [...names].sort((left, right) => left.localeCompare(right));
}

/**
 * Parse one permit block from the source table.
 *
 * @param {readonly PositionedText[]} block - Positioned items from one permit row group.
 * @param {string | null} section - Verbatim report-section code.
 * @param {RockIslandReportSource} source - Official report provenance.
 * @returns {RockIslandMonthlyPermit | null} Privacy-restricted private record.
 */
function parsePermitBlock(block, section, source) {
  const permitItem = block.find(isPermitNumberItem);
  if (permitItem === undefined) return null;
  const samePrimaryRow = (/** @type {PositionedText} */ item) =>
    Math.abs(item.y - permitItem.y) <= 2;
  const issueDate =
    block
      .filter((item) => item.x >= 55 && item.x < 110)
      .map((item) => parseReportDate(item.text))
      .find((value) => value !== null) ?? null;
  const workLocation = joinVisualText(
    block.filter(
      (item) => item.x >= 600 && item.x < 715 && samePrimaryRow(item),
    ),
  );
  const description = joinVisualText(
    block.filter((item) => item.x >= 330 && item.x < 535),
  );
  const sourceTaxMap =
    joinVisualText(
      block.filter(
        (item) => item.x >= 535 && item.x < 600 && samePrimaryRow(item),
      ),
    ) ?? null;
  const costText =
    joinVisualText(
      block.filter((item) => item.x >= 715 && samePrimaryRow(item)),
    ) ?? null;
  const roofText = `${section ?? ""} ${description ?? ""}`;

  return {
    source_system: REPORT_SOURCE_SYSTEM,
    source_url: source.url,
    city: "Rock Island",
    permit_number: permitItem.text,
    parcel_identifier: null,
    work_location: workLocation,
    permit_issue_date: issueDate,
    record_status: "Issued",
    record_type: section,
    project_description: description,
    contractor_business_names: readContractorBusinessNames(block),
    is_roof_permit: /\broof(?:ing)?\b/i.test(roofText),
    raw: {
      source_document_id: source.documentId,
      source_report_title: source.title,
      source_report_section: section,
      source_tax_map: sourceTaxMap,
      parcel_match_evidence: "source_tax_map_only_not_joined",
      project_valuation: parseCurrency(costText),
      source_page: permitItem.pageNumber,
    },
  };
}

/**
 * Parse one permit block from the newer Tyler-generated report layout.
 *
 * The April 2026 report changed from the legacy landscape table to columns for permit
 * number, address, permit type, issue date/description, application date, valuation,
 * and parcel number. It does not expose owner or contractor columns.
 *
 * @param {readonly PositionedText[]} block - One modern-layout permit block.
 * @param {RockIslandReportSource} source - Official report provenance.
 * @returns {RockIslandMonthlyPermit | null} Privacy-restricted private record.
 */
function parseModernPermitBlock(block, source) {
  const permitItem = block.find(isPermitNumberItem);
  if (permitItem === undefined) return null;
  const primaryY = permitItem.y;
  const issueCell = block.find(
    (item) =>
      item.x >= 230 &&
      item.x < 264 &&
      /^\d{1,2}\/\d{1,2}\/\d{4}\b/.test(item.text),
  );
  const issueMatch =
    issueCell === undefined
      ? null
      : /^(\d{1,2}\/\d{1,2}\/\d{4})(?:\s+(.*))?$/.exec(issueCell.text);
  const inlineDescription = readText(issueMatch?.[2]);
  const descriptionParts = [
    ...(inlineDescription === null
      ? []
      : [
          {
            text: inlineDescription,
            x: 264,
            y: issueCell?.y ?? primaryY,
            pageNumber: permitItem.pageNumber,
          },
        ]),
    ...block.filter((item) => item.x >= 264 && item.x < 440),
  ];
  const description = joinVisualText(descriptionParts);
  const recordType = joinVisualText(
    block.filter(
      (item) =>
        item.x >= 175 && item.x < 230 && Math.abs(item.y - primaryY) <= 2,
    ),
  );
  const workLocation = joinVisualText(
    block.filter(
      (item) =>
        item.x >= 100 &&
        item.x < 175 &&
        item.y >= primaryY - 2 &&
        item.y <= primaryY + 7,
    ),
  );
  const sourceParcel = joinVisualText(
    block.filter((item) => item.x >= 528 && Math.abs(item.y - primaryY) <= 2),
  );
  const valuationText = joinVisualText(
    block.filter(
      (item) =>
        item.x >= 495 && item.x < 528 && Math.abs(item.y - primaryY) <= 2,
    ),
  );
  const roofText = `${recordType ?? ""} ${description ?? ""}`;

  return {
    source_system: REPORT_SOURCE_SYSTEM,
    source_url: source.url,
    city: "Rock Island",
    permit_number: permitItem.text,
    parcel_identifier: null,
    work_location: workLocation,
    permit_issue_date:
      issueMatch === null ? null : parseReportDate(issueMatch[1] ?? ""),
    record_status: "Issued",
    record_type: recordType,
    project_description: description,
    contractor_business_names: [],
    is_roof_permit: /\broof(?:ing)?\b/i.test(roofText),
    raw: {
      source_document_id: source.documentId,
      source_report_title: source.title,
      source_report_section: null,
      source_tax_map: sourceParcel,
      parcel_match_evidence: "source_tax_map_only_not_joined",
      project_valuation: parseCurrency(valuationText),
      source_page: permitItem.pageNumber,
    },
  };
}

/**
 * Parse one fixed-column official monthly issued-permit report.
 *
 * Owner-labelled rows are never copied. Contractor strings are copied only when they
 * have explicit business markers. `TAX_MAP` is retained as unjoined source evidence;
 * `parcel_identifier` remains null until a later database workflow proves a match.
 *
 * @param {readonly (readonly PositionedText[])[]} pages - Positioned PDF text by page.
 * @param {RockIslandReportSource} source - Official report provenance.
 * @returns {readonly RockIslandMonthlyPermit[]} Parsed permits in report order.
 */
export function parseRockIslandMonthlyReport(pages, source) {
  /** @type {RockIslandMonthlyPermit[]} */
  const records = [];
  let section = null;
  const modernLayout =
    pages.some((page) =>
      page.some((item) => /^permit number$/i.test(item.text)),
    ) &&
    pages.some((page) =>
      page.some((item) => /^permit type$/i.test(item.text)),
    ) &&
    pages.some((page) =>
      page.some((item) => /^parcel number$/i.test(item.text)),
    );

  for (const page of pages) {
    /** @type {PositionedText[]} */
    let block = [];
    const flush = () => {
      if (block.length === 0) return;
      const record = modernLayout
        ? parseModernPermitBlock(block, source)
        : parsePermitBlock(block, section, source);
      if (record !== null) records.push(record);
      block = [];
    };

    for (const item of page) {
      if (isSectionItem(item)) {
        flush();
        section = item.text;
        continue;
      }
      if (isPermitNumberItem(item)) flush();
      if (block.length > 0 || isPermitNumberItem(item)) block.push(item);
    }
    flush();
  }

  return records;
}

/**
 * Deduplicate source records and produce stable output independent of report order.
 *
 * @param {readonly RockIslandMonthlyPermit[]} records - Candidate records.
 * @returns {readonly RockIslandMonthlyPermit[]} Deterministic unique records.
 */
export function dedupeAndSortMonthlyPermits(records) {
  /** @type {Map<string, RockIslandMonthlyPermit[]>} */
  const byIdentity = new Map();
  for (const record of records) {
    const identity = `${record.permit_number}\u0000${record.permit_issue_date ?? ""}`;
    const group = byIdentity.get(identity) ?? [];
    group.push(record);
    byIdentity.set(identity, group);
  }
  return [...byIdentity.values()]
    .map((group) => mergePermitVariants(group))
    .sort(
      (left, right) =>
        (left.permit_issue_date ?? "").localeCompare(
          right.permit_issue_date ?? "",
        ) || left.permit_number.localeCompare(right.permit_number),
    );
}

/**
 * Merge repeated rows for the same permit while preserving source variants.
 *
 * Legacy PDFs repeat permits across page breaks and sometimes show different project
 * addresses for the same permit. The earliest source page remains the primary row;
 * alternate addresses, pages, sections, tax-map values, and valuations are retained
 * explicitly instead of silently choosing one conflicting serialization.
 *
 * @param {readonly RockIslandMonthlyPermit[]} variants - Same permit number/date rows.
 * @returns {RockIslandMonthlyPermit} One loss-aware deterministic record.
 */
function mergePermitVariants(variants) {
  const ordered = [...variants].sort(
    (left, right) =>
      left.raw.source_page - right.raw.source_page ||
      JSON.stringify(left).localeCompare(JSON.stringify(right)),
  );
  const primary = ordered[0];
  if (primary === undefined) {
    throw new Error("Cannot merge an empty Rock Island permit group");
  }
  if (ordered.length === 1) return primary;

  const uniqueStrings = (
    /** @type {(value: RockIslandMonthlyPermit) => string | null} */ read,
  ) =>
    [...new Set(ordered.map(read).filter((value) => value !== null))].sort(
      (left, right) => left.localeCompare(right),
    );
  const workLocations = uniqueStrings((record) => record.work_location);
  const descriptions = uniqueStrings((record) => record.project_description);
  const sourceTaxMaps = uniqueStrings((record) => record.raw.source_tax_map);
  const sourceSections = uniqueStrings(
    (record) => record.raw.source_report_section,
  );
  const sourcePages = [
    ...new Set(ordered.map((record) => record.raw.source_page)),
  ].sort((left, right) => left - right);
  const valuationVariants = [
    ...new Set(
      ordered
        .map((record) => record.raw.project_valuation)
        .filter((value) => value !== null),
    ),
  ].sort((left, right) => left - right);
  const contractorBusinessNames = [
    ...new Set(
      ordered.flatMap((record) => [...record.contractor_business_names]),
    ),
  ].sort((left, right) => left.localeCompare(right));
  const longestDescription = [...descriptions].sort(
    (left, right) => right.length - left.length || left.localeCompare(right),
  )[0];

  return {
    ...primary,
    project_description: longestDescription ?? primary.project_description,
    contractor_business_names: contractorBusinessNames,
    is_roof_permit: ordered.some((record) => record.is_roof_permit),
    raw: {
      ...primary.raw,
      source_pages: sourcePages,
      source_sections: sourceSections,
      alternate_work_locations: workLocations.filter(
        (value) => value !== primary.work_location,
      ),
      source_tax_map_variants: sourceTaxMaps,
      project_valuation_variants: valuationVariants,
    },
  };
}

/**
 * Render deterministic private-staging JSONL.
 *
 * @param {readonly RockIslandMonthlyPermit[]} records - Candidate permit records.
 * @returns {string} Newline-delimited JSON with a trailing newline when non-empty.
 */
export function renderMonthlyPermitJsonl(records) {
  const stable = dedupeAndSortMonthlyPermits(records);
  return stable.length === 0
    ? ""
    : `${stable.map((record) => JSON.stringify(record)).join("\n")}\n`;
}

export { REPORT_INDEX_URL, REPORT_SOURCE_SYSTEM };
