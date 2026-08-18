// @ts-check

/**
 * @typedef {object} PositionedText
 * @property {string} text - Collapsed, non-empty PDF text.
 * @property {number} x - Horizontal PDF coordinate.
 * @property {number} y - Vertical PDF coordinate.
 * @property {number} pageNumber - One-based PDF page number.
 */

/**
 * @typedef {object} MolineReportLink
 * @property {string} archiveId - CivicPlus Archive Center item identifier.
 * @property {string} reportMonth - Report month in `YYYY-MM` form.
 * @property {string} title - Official archive title.
 * @property {string} url - Canonical official report URL.
 */

/**
 * @typedef {object} MolineReportSource
 * @property {string} archiveId - CivicPlus Archive Center item identifier.
 * @property {string} reportMonth - Report month in `YYYY-MM` form.
 * @property {string} title - Official archive title.
 * @property {string} url - Canonical official report URL.
 */

/**
 * @typedef {object} MolineSourceReportProvenance
 * @property {string} archive_id - CivicPlus document id.
 * @property {string} report_month - Official archive month.
 * @property {string} report_title - Official archive title.
 * @property {string} report_url - Canonical official URL.
 * @property {readonly number[]} pages - Source PDF pages containing this record.
 */

/**
 * @typedef {object} MolineIssuedPermit
 * @property {string} source_system - Stable official report source key.
 * @property {string} source_url - Official Archive Center report URL.
 * @property {"Moline"} city - Issuing municipality.
 * @property {string | null} permit_number - Public permit identifier printed in modern reports, or null for legacy application-key records.
 * @property {null} parcel_identifier - Always null because the report has no parcel field.
 * @property {string | null} work_location - Public report address retained only in private staging.
 * @property {string} permit_issue_date - Issue date in `YYYY-MM-DD` form.
 * @property {"Issued"} record_status - Status established by the official archive title.
 * @property {string} record_type - Public permit type.
 * @property {string | null} project_description - Legacy application description, or null when the layout has no description field.
 * @property {readonly string[]} contractor_business_names - Conservatively recognized organizations only.
 * @property {boolean} is_roof_permit - Conservative type/subtype classification.
 * @property {{
 *   source_archive_id: string,
 *   source_report_month: string,
 *   source_report_title: string,
 *   source_application_year: string | null,
 *   source_application_number: string | null,
 *   source_permit_code: string | null,
 *   source_parcel_text: string | null,
 *   source_permit_status: string | null,
 *   permit_subtype: string | null,
 *   project_valuation: number | null,
 *   source_page: number,
 *   source_pages: readonly number[],
 *   source_reports: readonly MolineSourceReportProvenance[],
 *   parser_layout: "current-2024-10" | "current-2024-10-no-value" | "legacy-application-v1" | "legacy-rotated-v2"
 * }} raw - Reviewed source provenance and non-contact source fields.
 */

/**
 * @typedef {object} MolinePublicPermit
 * @property {string} permit_key - Loader-compatible stable public key.
 * @property {string} source_system - Stable official report source key.
 * @property {string} source_report_archive_id - CivicPlus archive item identifier.
 * @property {string} source_report_month - Official report month.
 * @property {string} source_report_title - Official report title.
 * @property {string} source_report_url - Official report URL.
 * @property {readonly string[]} source_report_archive_ids - Every official source document id for this record.
 * @property {readonly string[]} source_report_months - Every official archive month for this record.
 * @property {readonly string[]} source_report_titles - Every official archive title for this record.
 * @property {readonly string[]} source_report_urls - Every official source URL for this record.
 * @property {string | null} permit_number - Printed modern permit identifier, or null for legacy records.
 * @property {string | null} source_application_year - Printed legacy application year.
 * @property {string | null} source_application_number - Printed legacy application number.
 * @property {string | null} source_permit_code - Printed legacy permit code.
 * @property {string} permit_issue_date - Issue date in `YYYY-MM-DD` form.
 * @property {"Issued"} record_status - Official report status.
 * @property {string} record_type - Public permit type.
 * @property {string | null} permit_subtype - Public permit subtype.
 * @property {"Moline"} city - Issuing municipality.
 * @property {boolean} is_roof_permit - Conservative type/subtype classification.
 */

export const MOLINE_REPORT_INDEX_URL =
  "https://www.moline.il.us/926/Permit-Reports";
export const MOLINE_REPORT_SOURCE_SYSTEM =
  "moline_official_monthly_building_permit_reports";

/**
 * Collapse whitespace and reject empty values.
 *
 * @param {unknown} value - Candidate source value.
 * @returns {string | null} Normalized text or null.
 */
function readText(value) {
  if (typeof value !== "string") return null;
  const normalized = value.replace(/\s+/g, " ").trim();
  return normalized.length === 0 ? null : normalized;
}

/**
 * Decode the small HTML entity subset present in CivicPlus archive labels.
 *
 * @param {string} value - Raw anchor content.
 * @returns {string} Plain collapsed text.
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
 * Build the canonical public Archive Center URL for one item.
 *
 * CivicPlus renders relative `Archive.aspx` links beneath the `/926/` route, but the
 * public handler lives at the site root and redirects to `/ArchiveCenter/ViewFile/Item`.
 *
 * @param {string} archiveId - Numeric Archive Center item id.
 * @param {string} indexUrl - Official index URL used for origin validation.
 * @returns {string} Canonical official report URL.
 */
function canonicalArchiveUrl(archiveId, indexUrl) {
  const origin = new URL(indexUrl).origin;
  return `${origin}/Archive.aspx?ADID=${archiveId}`;
}

/**
 * Discover unique official monthly issued-permit reports from the Moline archive page.
 *
 * @param {string} html - Raw official archive HTML.
 * @param {string} [indexUrl=MOLINE_REPORT_INDEX_URL] - Official archive URL.
 * @returns {readonly MolineReportLink[]} Reports sorted by month then archive id.
 */
export function extractMolineReportLinks(
  html,
  indexUrl = MOLINE_REPORT_INDEX_URL,
) {
  /** @type {Map<string, MolineReportLink>} */
  const byArchiveId = new Map();
  const anchorPattern =
    /<a[^>]+href=["']([^"']*(?:Archive\.aspx\?ADID=|Archive\/ViewFile\/Item\/)(\d+)[^"']*)["'][^>]*>([\s\S]*?)<\/a>/gi;
  for (const match of html.matchAll(anchorPattern)) {
    const archiveId = match[2];
    const title = decodeHtmlText(match[3] ?? "");
    if (archiveId === undefined || title.length === 0) continue;
    const monthMatch = /\b(20\d{2})\s*-\s*(0[1-9]|1[0-2])\b/.exec(title);
    if (monthMatch === null) continue;
    const reportMonth = `${monthMatch[1]}-${monthMatch[2]}`;
    const candidate = {
      archiveId,
      reportMonth,
      title,
      url: canonicalArchiveUrl(archiveId, indexUrl),
    };
    const existing = byArchiveId.get(archiveId);
    if (
      existing === undefined ||
      candidate.title.length > existing.title.length
    ) {
      byArchiveId.set(archiveId, candidate);
    }
  }
  return [...byArchiveId.values()].sort(
    (left, right) =>
      left.reportMonth.localeCompare(right.reportMonth) ||
      Number(left.archiveId) - Number(right.archiveId),
  );
}

/**
 * Determine whether positioned text matches the verified January 2025+ report layout.
 *
 * @param {readonly (readonly PositionedText[])[]} pages - Positioned PDF pages.
 * @returns {boolean} True only when all reviewed column headers are present.
 */
export function isCurrentMolineReportLayout(pages) {
  const headers = new Set(
    pages.flatMap((page) =>
      page
        .map((item) => readText(item.text))
        .filter((value) => value !== null)
        .map((value) => value.toUpperCase().replace(/\s+/g, "")),
    ),
  );
  return [
    "PERMITNUMBER",
    "PERMITTYPE",
    "PERMITSUBTYPE",
    "ISSUED",
    "CONTRACTOR_NAME",
    "ADDRESS",
  ].every((header) => headers.has(header));
}

/**
 * Determine whether positioned text matches the clean legacy application-key layout.
 *
 * The source used the same logical columns at two PDF page scales. Exact header labels,
 * rather than absolute coordinates, establish this version. A malformed 2020-2021 export
 * interval merges several headers and values into single text items and therefore fails
 * this check closed.
 *
 * @param {readonly (readonly PositionedText[])[]} pages - Positioned PDF pages.
 * @returns {boolean} True only for the reviewed clean legacy layout.
 */
export function isLegacyApplicationMolineReportLayout(pages) {
  const requiredHeaders = [
    "Permit",
    "Permit Code",
    "Permit Issue",
    "App.",
    "App. #",
    "Name",
    "Name Type",
    "Township -",
    "Street Address",
    "Application Type",
    "Permit Status",
    "Estimated",
  ];
  return pages.some((page) => {
    const headers = new Set(page.map((item) => item.text));
    if (!requiredHeaders.every((header) => headers.has(header))) return false;
    const permitCode = page.find((item) => item.text === "Permit Code");
    const permitIssue = page.find((item) => item.text === "Permit Issue");
    return (
      permitCode !== undefined &&
      permitIssue !== undefined &&
      Math.abs(permitCode.y - permitIssue.y) <= 5
    );
  });
}

/**
 * Detect the reviewed transposed-coordinate legacy export.
 *
 * The February 2020 PDF stores visual rows along the PDF x axis and visual columns along
 * the y axis. Text remains readable, but it requires an explicit parser rather than treating
 * it as the normal landscape geometry.
 *
 * @param {readonly (readonly PositionedText[])[]} pages - Positioned PDF pages.
 * @returns {boolean} True for the reviewed transposed legacy geometry.
 */
export function isRotatedLegacyMolineReportLayout(pages) {
  return pages.some((page) => {
    const permitCode = page.find((item) => item.text === "Permit Code");
    const permitIssue = page.find((item) => item.text === "Permit Issue");
    const applicationNumber = page.find((item) => item.text === "App. #");
    return (
      permitCode !== undefined &&
      permitIssue !== undefined &&
      applicationNumber !== undefined &&
      Math.abs(permitCode.x - permitIssue.x) <= 5 &&
      Math.abs(permitCode.y - permitIssue.y) > 20
    );
  });
}

/**
 * Detect the known malformed legacy PDF text extraction interval.
 *
 * These reports visually contain the legacy table, but their embedded text merges permit
 * code, date, application identity, name, parcel, and value columns. No parser may split
 * those strings heuristically because doing so could invent an application key.
 *
 * @param {readonly (readonly PositionedText[])[]} pages - Positioned PDF pages.
 * @returns {boolean} True when known merged header signatures are present.
 */
export function isCompactedLegacyMolineReportLayout(pages) {
  return pages
    .flat()
    .some(
      (item) =>
        item.text.includes("PermitCodePermitIssueApp.") ||
        item.text.includes("DescriptionDateCodeParcel NumberDescriptionValue"),
    );
}

/**
 * Parse a date printed as `M/D/YYYY` or `MM/DD/YYYY`.
 *
 * @param {string | null} value - Candidate report date.
 * @returns {string | null} ISO date or null.
 */
function parseReportDate(value) {
  if (value === null) return null;
  const match = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(value);
  if (match === null) return null;
  const month = Number(match[1]);
  const day = Number(match[2]);
  const year = Number(match[3]);
  const parsed = new Date(Date.UTC(year, month - 1, day));
  if (
    parsed.getUTCFullYear() !== year ||
    parsed.getUTCMonth() !== month - 1 ||
    parsed.getUTCDate() !== day
  ) {
    return null;
  }
  return `${match[3]}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

/**
 * Parse a reviewed report currency value.
 *
 * @param {string | null} value - Candidate dollar value.
 * @returns {number | null} Finite numeric valuation or null.
 */
function parseCurrency(value) {
  if (value === null || !/^\$[\d,]+(?:\.\d{2})?$/.test(value)) return null;
  const parsed = Number(value.replace(/[$,]/g, ""));
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Join positioned report cells in visual order.
 *
 * @param {readonly PositionedText[]} items - One logical cell's positioned items.
 * @returns {string | null} Collapsed cell text.
 */
function joinCell(items) {
  return readText(
    [...items]
      .sort((left, right) => left.x - right.x)
      .map((item) => item.text)
      .join(" "),
  );
}

/**
 * Recognize a current-layout Moline permit number.
 *
 * @param {PositionedText} item - Positioned report text.
 * @returns {boolean} Whether the item starts one permit row.
 */
function isCurrentPermitNumber(item) {
  return /^(?:(?=[A-Z0-9-]*\d)[A-Z][A-Z0-9]*\d{2}-\d{4,8}|\d{2}-\d{8}(?:-[A-Z0-9]+)?)$/i.test(
    item.text,
  );
}

/**
 * Keep contractor text only when it has explicit organization evidence.
 *
 * Person-looking contractor values are intentionally omitted even though they appear in
 * the public report. Public output excludes the entire contractor field.
 *
 * @param {string} value - Contractor cell text.
 * @returns {boolean} True when the value contains a reviewed organization marker.
 */
export function isConservativeMolineBusinessName(value) {
  return /\b(?:llc|l\.l\.c\.|inc\.?|incorporated|corp\.?|corporation|company|co\.|construction|contracting|electric|electrical|plumbing|heating|cooling|roofing|restoration|development|properties|property|services|service|group|homes|builders?|enterprises?|systems?|city of|church|university|school|foundation|association|authority|department|dba)\b/i.test(
    value,
  );
}

/**
 * Read one horizontal cell around a permit row's baseline.
 *
 * @param {readonly PositionedText[]} page - All positioned items on one page.
 * @param {number} rowY - Permit row baseline.
 * @param {number} minimumX - Inclusive left cell boundary.
 * @param {number} maximumX - Exclusive right cell boundary.
 * @returns {string | null} Cell text.
 */
function readRowCell(page, rowY, minimumX, maximumX) {
  return joinCell(
    page.filter(
      (item) =>
        item.x >= minimumX && item.x < maximumX && Math.abs(item.y - rowY) <= 1,
    ),
  );
}

/**
 * Read one exact modern header coordinate without relying on a report-month-specific scale.
 *
 * @param {readonly PositionedText[]} page - Positioned page text.
 * @param {string} header - Uppercase reviewed header.
 * @returns {number} Header x coordinate.
 */
function requireCurrentHeaderX(page, header) {
  const item = page.find(
    (candidate) => candidate.text.toUpperCase().replace(/\s+/g, "") === header,
  );
  if (item === undefined) {
    throw new Error(`Missing current Moline report header: ${header}`);
  }
  return item.x;
}

/**
 * Read one modern-layout cell from header-derived visual order.
 *
 * Moline changed the column order in August 2025, moving `ISSUED` before the permit
 * number. Sorting all reviewed header centers makes the parser explicit but order-neutral.
 *
 * @param {readonly PositionedText[]} page - Positioned page text.
 * @param {number} rowY - Permit row baseline.
 * @param {number} targetX - Target header center.
 * @param {readonly number[]} headerCenters - All reviewed header centers.
 * @returns {string | null} Source cell text.
 */
function readCurrentCell(page, rowY, targetX, headerCenters) {
  const ordered = [...headerCenters].sort((left, right) => left - right);
  const targetIndex = ordered.indexOf(targetX);
  if (targetIndex < 0) {
    throw new Error("Current Moline column was not present in header geometry");
  }
  const previous = ordered[targetIndex - 1];
  const next = ordered[targetIndex + 1];
  return readRowCell(
    page,
    rowY,
    previous === undefined
      ? Number.NEGATIVE_INFINITY
      : midpoint(previous, targetX),
    next === undefined ? Number.POSITIVE_INFINITY : midpoint(targetX, next),
  );
}

/**
 * @typedef {object} LegacyColumnGeometry
 * @property {number} permitDescription - Permit-description column center.
 * @property {number} issueDate - Issue-date column center.
 * @property {number} applicationYear - Application-year column center.
 * @property {number} applicationNumber - Application-number column center.
 * @property {number} name - Name column center.
 * @property {number} nameType - Name-type column center.
 * @property {number} parcel - Township/parcel column center.
 * @property {number} address - Street-address column center.
 * @property {number} applicationDescription - Application-description column center.
 * @property {number} status - Permit-status column center.
 * @property {number} valuation - Estimated-value column center.
 */

/**
 * Read one exact legacy header coordinate.
 *
 * @param {readonly PositionedText[]} page - Positioned page text.
 * @param {string} header - Exact reviewed header text.
 * @returns {number} Header x coordinate.
 */
function requireLegacyHeaderX(page, header) {
  const item = page.find((candidate) => candidate.text === header);
  if (item === undefined) {
    throw new Error(`Missing legacy Moline report header: ${header}`);
  }
  return item.x;
}

/**
 * Derive legacy column geometry from exact source headers.
 *
 * @param {readonly PositionedText[]} page - Positioned page text.
 * @returns {LegacyColumnGeometry} Column centers at either reviewed PDF scale.
 */
function readLegacyColumnGeometry(page) {
  return {
    permitDescription: requireLegacyHeaderX(page, "Permit Code"),
    issueDate: requireLegacyHeaderX(page, "Permit Issue"),
    applicationYear: requireLegacyHeaderX(page, "App."),
    applicationNumber: requireLegacyHeaderX(page, "App. #"),
    name: requireLegacyHeaderX(page, "Name"),
    nameType: requireLegacyHeaderX(page, "Name Type"),
    parcel: requireLegacyHeaderX(page, "Township -"),
    address: requireLegacyHeaderX(page, "Street Address"),
    applicationDescription: requireLegacyHeaderX(page, "Application Type"),
    status: requireLegacyHeaderX(page, "Permit Status"),
    valuation: requireLegacyHeaderX(page, "Estimated"),
  };
}

/**
 * Return the midpoint between adjacent column centers.
 *
 * @param {number} left - Left column center.
 * @param {number} right - Right column center.
 * @returns {number} Boundary between the columns.
 */
function midpoint(left, right) {
  return (left + right) / 2;
}

/**
 * Read one legacy table cell by adjacent header centers.
 *
 * @param {readonly PositionedText[]} page - Positioned page text.
 * @param {number} rowY - Permit row baseline.
 * @param {number} leftCenter - Current or preceding column center.
 * @param {number} center - Target column center.
 * @param {number} rightCenter - Current or following column center.
 * @returns {string | null} Joined source cell.
 */
function readLegacyCell(page, rowY, leftCenter, center, rightCenter) {
  return readRowCell(
    page,
    rowY,
    midpoint(leftCenter, center),
    midpoint(center, rightCenter),
  );
}

/**
 * @typedef {object} LegacyIdentityInspection
 * @property {number} totalRowCount - Legacy date rows present in the report.
 * @property {number} stableIdentityRowCount - Rows with numeric official year and application-number fields.
 * @property {number} ambiguousIdentityRowCount - Rows whose official identity fields are absent, redacted, or merged ambiguously.
 * @property {readonly {
 *   pageNumber: number,
 *   issueDateText: string,
 *   identityText: string | null
 * }[]} ambiguousRows - Minimal private evidence for blocked rows.
 */

/**
 * Derive the left boundary of the legacy name column.
 *
 * @param {LegacyColumnGeometry} columns - Header-derived column centers.
 * @returns {number} Boundary after the application-number field.
 */
function readLegacyApplicationNameBoundary(columns) {
  return (
    columns.applicationNumber +
    (columns.name - columns.applicationNumber) * 0.25
  );
}

/**
 * Read the two separately labelled official application identity fields together.
 *
 * Reading the bounded region as one string handles PDFs that split the values into two
 * text items while still rejecting redacted values such as `12 ########`.
 *
 * @param {readonly PositionedText[]} page - Positioned page text.
 * @param {number} rowY - Permit row baseline.
 * @param {LegacyColumnGeometry} columns - Header-derived column centers.
 * @returns {{applicationYear: string, applicationNumber: string} | null} Stable source identity.
 */
function readLegacyApplicationIdentity(page, rowY, columns) {
  const identityText = readRowCell(
    page,
    rowY,
    midpoint(columns.issueDate, columns.applicationYear),
    readLegacyApplicationNameBoundary(columns),
  );
  if (identityText === null) return null;
  const match = /^(\d{1,4})\s+(\d+)(?:\s+\D.*)?$/.exec(identityText);
  if (match === null || match[1] === undefined || match[2] === undefined) {
    return null;
  }
  return {
    applicationYear: match[1],
    applicationNumber: match[2],
  };
}

/**
 * Inspect whether every clean-layout legacy row has a stable official identity.
 *
 * @param {readonly (readonly PositionedText[])[]} pages - Positioned PDF pages.
 * @returns {LegacyIdentityInspection} Stable and ambiguous row counts.
 */
export function inspectLegacyMolineApplicationIdentities(pages) {
  if (!isLegacyApplicationMolineReportLayout(pages)) {
    throw new Error(
      "Legacy identity inspection requires the clean legacy layout",
    );
  }
  let totalRowCount = 0;
  let stableIdentityRowCount = 0;
  /** @type {{pageNumber: number, issueDateText: string, identityText: string | null}[]} */
  const ambiguousRows = [];
  const headerPage = pages.find((page) =>
    [
      "Permit",
      "Permit Code",
      "Permit Issue",
      "App.",
      "App. #",
      "Name",
      "Name Type",
      "Township -",
      "Street Address",
      "Application Type",
      "Permit Status",
      "Estimated",
    ].every((header) => page.some((item) => item.text === header)),
  );
  if (headerPage === undefined) {
    throw new Error("Clean legacy report has no complete header page");
  }
  const columns = readLegacyColumnGeometry(headerPage);
  for (const page of pages) {
    const rowItems = page.filter(
      (item) =>
        /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(item.text) &&
        item.x > midpoint(columns.permitDescription, columns.issueDate) &&
        item.x < midpoint(columns.issueDate, columns.applicationYear),
    );
    for (const dateItem of rowItems) {
      totalRowCount += 1;
      const identity = readLegacyApplicationIdentity(page, dateItem.y, columns);
      if (identity !== null) {
        stableIdentityRowCount += 1;
        continue;
      }
      ambiguousRows.push({
        pageNumber: dateItem.pageNumber,
        issueDateText: dateItem.text,
        identityText: readRowCell(
          page,
          dateItem.y,
          midpoint(columns.issueDate, columns.applicationYear),
          readLegacyApplicationNameBoundary(columns),
        ),
      });
    }
  }
  return {
    totalRowCount,
    stableIdentityRowCount,
    ambiguousIdentityRowCount: ambiguousRows.length,
    ambiguousRows,
  };
}

/**
 * Read the printed total permit count from either supported layout.
 *
 * @param {readonly (readonly PositionedText[])[]} pages - Positioned PDF pages.
 * @returns {number | null} Printed total, or null when the source has no readable total.
 */
export function readMolinePrintedPermitTotal(pages) {
  for (const page of pages) {
    for (const label of page.filter((item) =>
      /^(?:TOTAL PERMITS|Total permits:)$/.test(item.text),
    )) {
      const count = page.find(
        (item) =>
          /^\d+$/.test(item.text) &&
          Math.abs(item.y - label.y) <= 2 &&
          item.x > label.x &&
          item.x < label.x + 200,
      );
      if (count !== undefined) return Number(count.text);
    }
  }
  return null;
}

/**
 * Parse one verified October 2024+ Moline monthly issued-permit PDF.
 *
 * No owner, applicant, phone, email, or unrestricted contractor text is retained.
 *
 * @param {readonly (readonly PositionedText[])[]} pages - Positioned PDF pages.
 * @param {MolineReportSource} source - Official report provenance.
 * @returns {readonly MolineIssuedPermit[]} Parsed private-staging records.
 */
export function parseCurrentMolineIssuedPermitReport(pages, source) {
  if (!isCurrentMolineReportLayout(pages)) {
    throw new Error(
      "Unsupported Moline permit report layout; only the verified October 2024+ layout is accepted",
    );
  }
  /** @type {MolineIssuedPermit[]} */
  const records = [];
  const headerPage = pages.find((page) =>
    [
      "PERMITNUMBER",
      "PERMITTYPE",
      "PERMITSUBTYPE",
      "ISSUED",
      "CONTRACTOR_NAME",
      "ADDRESS",
    ].every((header) =>
      page.some(
        (item) => item.text.toUpperCase().replace(/\s+/g, "") === header,
      ),
    ),
  );
  if (headerPage === undefined) {
    throw new Error("Current Moline report has no complete header page");
  }
  const permitNumberX = requireCurrentHeaderX(headerPage, "PERMITNUMBER");
  const permitTypeX = requireCurrentHeaderX(headerPage, "PERMITTYPE");
  const permitSubtypeX = requireCurrentHeaderX(headerPage, "PERMITSUBTYPE");
  const issuedX = requireCurrentHeaderX(headerPage, "ISSUED");
  const contractorX = requireCurrentHeaderX(headerPage, "CONTRACTOR_NAME");
  const addressX = requireCurrentHeaderX(headerPage, "ADDRESS");
  const jobValueItem = headerPage.find(
    (item) => item.text.toUpperCase().replace(/\s+/g, "") === "JOBVALUE",
  );
  const jobValueX = jobValueItem?.x ?? null;
  const headerCenters = [
    permitNumberX,
    permitTypeX,
    permitSubtypeX,
    issuedX,
    contractorX,
    addressX,
    ...(jobValueX === null ? [] : [jobValueX]),
  ];
  for (const page of pages) {
    for (const permitItem of page.filter(isCurrentPermitNumber)) {
      const issueDate = parseReportDate(
        readCurrentCell(page, permitItem.y, issuedX, headerCenters),
      );
      const recordType = readCurrentCell(
        page,
        permitItem.y,
        permitTypeX,
        headerCenters,
      );
      if (issueDate === null || recordType === null) {
        throw new Error(
          `Incomplete Moline permit row ${permitItem.text} on page ${String(permitItem.pageNumber)}`,
        );
      }
      const subtype = readCurrentCell(
        page,
        permitItem.y,
        permitSubtypeX,
        headerCenters,
      );
      const contractor = readCurrentCell(
        page,
        permitItem.y,
        contractorX,
        headerCenters,
      );
      const workLocation = readCurrentCell(
        page,
        permitItem.y,
        addressX,
        headerCenters,
      );
      const valuation =
        jobValueX === null
          ? null
          : parseCurrency(
              readCurrentCell(page, permitItem.y, jobValueX, headerCenters),
            );
      const contractorBusinessNames =
        contractor !== null && isConservativeMolineBusinessName(contractor)
          ? [contractor]
          : [];
      const roofText = `${recordType} ${subtype ?? ""}`;
      records.push({
        source_system: MOLINE_REPORT_SOURCE_SYSTEM,
        source_url: source.url,
        city: "Moline",
        permit_number: permitItem.text,
        parcel_identifier: null,
        work_location: workLocation,
        permit_issue_date: issueDate,
        record_status: "Issued",
        record_type: recordType,
        project_description: null,
        contractor_business_names: contractorBusinessNames,
        is_roof_permit: /\broof(?:ing)?\b/i.test(roofText),
        raw: {
          source_archive_id: source.archiveId,
          source_report_month: source.reportMonth,
          source_report_title: source.title,
          source_application_year: null,
          source_application_number: null,
          source_permit_code: null,
          source_parcel_text: null,
          source_permit_status: null,
          permit_subtype: subtype,
          project_valuation: valuation,
          source_page: permitItem.pageNumber,
          source_pages: [permitItem.pageNumber],
          source_reports: [
            {
              archive_id: source.archiveId,
              report_month: source.reportMonth,
              report_title: source.title,
              report_url: source.url,
              pages: [permitItem.pageNumber],
            },
          ],
          parser_layout:
            jobValueX === null ? "current-2024-10-no-value" : "current-2024-10",
        },
      });
    }
  }
  if (records.length === 0) {
    throw new Error(
      `No permit rows parsed from Moline report ${source.archiveId}`,
    );
  }
  return records;
}

/**
 * Parse one clean legacy Moline issued-permit report.
 *
 * Stable identity uses four separately printed official fields: permit code, application
 * year, application number, and issue date. Issue date is required because the source can
 * print the same application/permit-code combination as issued on multiple dates. The parser
 * never converts that composite into a fabricated modern permit number. Township/parcel text
 * is retained as private evidence, but `parcel_identifier` remains null until a separate
 * source-specific normalization proves its meaning.
 *
 * @param {readonly (readonly PositionedText[])[]} pages - Positioned PDF pages.
 * @param {MolineReportSource} source - Official report provenance.
 * @returns {readonly MolineIssuedPermit[]} Parsed private-staging records.
 */
export function parseLegacyApplicationMolineIssuedPermitReport(pages, source) {
  if (!isLegacyApplicationMolineReportLayout(pages)) {
    const reason = isCompactedLegacyMolineReportLayout(pages)
      ? "compacted embedded PDF text makes official application identity ambiguous"
      : "unsupported legacy layout";
    throw new Error(
      `Cannot parse Moline legacy report ${source.archiveId}: ${reason}`,
    );
  }
  const identityInspection = inspectLegacyMolineApplicationIdentities(pages);
  if (identityInspection.ambiguousIdentityRowCount > 0) {
    throw new Error(
      `Cannot parse Moline legacy report ${source.archiveId}: ${String(identityInspection.ambiguousIdentityRowCount)} of ${String(identityInspection.totalRowCount)} rows have ambiguous official application identity`,
    );
  }

  /** @type {MolineIssuedPermit[]} */
  const records = [];
  /** @type {string | null} */
  let carriedPermitCode = null;
  /** @type {string | null} */
  let carriedRecordType = null;

  const headerPage = pages.find((page) =>
    [
      "Permit",
      "Permit Code",
      "Permit Issue",
      "App.",
      "App. #",
      "Name",
      "Name Type",
      "Township -",
      "Street Address",
      "Application Type",
      "Permit Status",
      "Estimated",
    ].every((header) => page.some((item) => item.text === header)),
  );
  if (headerPage === undefined) {
    throw new Error("Clean legacy report has no complete header page");
  }
  const columns = readLegacyColumnGeometry(headerPage);
  for (const page of pages) {
    const identifierCenter =
      columns.permitDescription -
      (columns.issueDate - columns.permitDescription);
    const applicationNameBoundary = readLegacyApplicationNameBoundary(columns);
    const parcelAddressBoundary =
      columns.address - (columns.address - columns.parcel) * 0.25;
    const rows = page
      .filter(
        (item) =>
          /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(item.text) &&
          item.x > midpoint(columns.permitDescription, columns.issueDate) &&
          item.x < midpoint(columns.issueDate, columns.applicationYear),
      )
      .sort((left, right) => right.y - left.y);

    for (const [rowIndex, dateItem] of rows.entries()) {
      const rowY = dateItem.y;
      const nextRowY = rows[rowIndex + 1]?.y ?? rowY - 60;
      const permitCode = readRowCell(
        page,
        rowY,
        Number.NEGATIVE_INFINITY,
        midpoint(identifierCenter, columns.permitDescription),
      );
      const recordType = readLegacyCell(
        page,
        rowY,
        identifierCenter,
        columns.permitDescription,
        columns.issueDate,
      );
      if (permitCode !== null) carriedPermitCode = permitCode;
      if (recordType !== null) carriedRecordType = recordType;

      const applicationIdentity = readLegacyApplicationIdentity(
        page,
        rowY,
        columns,
      );
      const issueDate = parseReportDate(dateItem.text);
      if (
        carriedPermitCode === null ||
        carriedRecordType === null ||
        applicationIdentity === null ||
        issueDate === null
      ) {
        throw new Error(
          `Incomplete legacy Moline permit identity on page ${String(dateItem.pageNumber)} at y=${String(rowY)}`,
        );
      }

      const associatedNames = page
        .filter(
          (item) =>
            item.x >= applicationNameBoundary &&
            item.x < midpoint(columns.name, columns.nameType) &&
            item.y <= rowY + 1.5 &&
            item.y > nextRowY + 1.5,
        )
        .sort((left, right) => right.y - left.y || left.x - right.x)
        .map((item) => item.text)
        .filter(
          (value) =>
            !/^PROPERTY OWNER$/i.test(value) &&
            isConservativeMolineBusinessName(value),
        );
      const contractorBusinessNames = [...new Set(associatedNames)];
      const sourceParcelText = readRowCell(
        page,
        rowY,
        midpoint(columns.nameType, columns.parcel),
        parcelAddressBoundary,
      );
      const workLocation = readRowCell(
        page,
        rowY,
        parcelAddressBoundary,
        midpoint(columns.address, columns.applicationDescription),
      );
      const projectDescription = readLegacyCell(
        page,
        rowY,
        columns.address,
        columns.applicationDescription,
        columns.status,
      );
      const sourcePermitStatus = readLegacyCell(
        page,
        rowY,
        columns.applicationDescription,
        columns.status,
        columns.valuation,
      );
      const valuation = parseCurrency(
        readRowCell(
          page,
          rowY,
          midpoint(columns.status, columns.valuation),
          Number.POSITIVE_INFINITY,
        ),
      );
      const roofText = `${carriedRecordType} ${projectDescription ?? ""}`;
      records.push({
        source_system: MOLINE_REPORT_SOURCE_SYSTEM,
        source_url: source.url,
        city: "Moline",
        permit_number: null,
        parcel_identifier: null,
        work_location: workLocation,
        permit_issue_date: issueDate,
        record_status: "Issued",
        record_type: carriedRecordType,
        project_description: projectDescription,
        contractor_business_names: contractorBusinessNames,
        is_roof_permit: /\broof(?:ing)?\b/i.test(roofText),
        raw: {
          source_archive_id: source.archiveId,
          source_report_month: source.reportMonth,
          source_report_title: source.title,
          source_application_year: applicationIdentity.applicationYear,
          source_application_number: applicationIdentity.applicationNumber,
          source_permit_code: carriedPermitCode,
          source_parcel_text: sourceParcelText,
          source_permit_status: sourcePermitStatus,
          permit_subtype: null,
          project_valuation: valuation,
          source_page: dateItem.pageNumber,
          source_pages: [dateItem.pageNumber],
          source_reports: [
            {
              archive_id: source.archiveId,
              report_month: source.reportMonth,
              report_title: source.title,
              report_url: source.url,
              pages: [dateItem.pageNumber],
            },
          ],
          parser_layout: "legacy-application-v1",
        },
      });
    }
  }

  if (records.length === 0) {
    throw new Error(
      `No permit rows parsed from Moline report ${source.archiveId}`,
    );
  }
  return records;
}

/**
 * Read one cell from the reviewed transposed-coordinate legacy export.
 *
 * @param {readonly PositionedText[]} page - Positioned page text.
 * @param {number} rowX - Visual row coordinate stored on the PDF x axis.
 * @param {number} minimumY - Inclusive visual column boundary.
 * @param {number} maximumY - Exclusive visual column boundary.
 * @returns {string | null} Joined cell text.
 */
function readRotatedLegacyCell(page, rowX, minimumY, maximumY) {
  return readText(
    page
      .filter(
        (item) =>
          Math.abs(item.x - rowX) <= 0.8 &&
          item.y >= minimumY &&
          item.y < maximumY,
      )
      .sort((left, right) => left.y - right.y)
      .map((item) => item.text)
      .join(" "),
  );
}

/**
 * Parse the reviewed transposed-coordinate February 2020 legacy report.
 *
 * All identity fields remain separately printed and numeric. Fixed boundaries are explicit
 * to this version and are not reused for normal landscape reports.
 *
 * @param {readonly (readonly PositionedText[])[]} pages - Positioned PDF pages.
 * @param {MolineReportSource} source - Official report provenance.
 * @returns {readonly MolineIssuedPermit[]} Parsed private-staging records.
 */
export function parseRotatedLegacyMolineIssuedPermitReport(pages, source) {
  if (!isRotatedLegacyMolineReportLayout(pages)) {
    throw new Error(
      `Cannot parse Moline rotated legacy report ${source.archiveId}: unsupported layout`,
    );
  }
  /** @type {MolineIssuedPermit[]} */
  const records = [];
  /** @type {string | null} */
  let carriedPermitCode = null;
  /** @type {string | null} */
  let carriedRecordType = null;
  for (const page of pages) {
    const rows = page
      .filter(
        (item) =>
          /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(item.text) &&
          item.y >= 120 &&
          item.y < 180,
      )
      .sort((left, right) => left.x - right.x);
    for (const [rowIndex, dateItem] of rows.entries()) {
      const rowX = dateItem.x;
      const nextRowX = rows[rowIndex + 1]?.x ?? rowX + 30;
      const permitCode = readRotatedLegacyCell(
        page,
        rowX,
        Number.NEGATIVE_INFINITY,
        45,
      );
      const recordType = readRotatedLegacyCell(page, rowX, 45, 120);
      if (permitCode !== null) carriedPermitCode = permitCode;
      if (recordType !== null) carriedRecordType = recordType;
      const issueDate = parseReportDate(dateItem.text);
      const applicationYear = readRotatedLegacyCell(page, rowX, 180, 210);
      const applicationNumber = readRotatedLegacyCell(page, rowX, 210, 235);
      if (
        issueDate === null ||
        carriedPermitCode === null ||
        carriedRecordType === null ||
        applicationYear === null ||
        !/^\d{1,4}$/.test(applicationYear) ||
        applicationNumber === null ||
        !/^\d+$/.test(applicationNumber)
      ) {
        throw new Error(
          `Incomplete rotated legacy Moline permit identity on page ${String(dateItem.pageNumber)} at x=${String(rowX)}`,
        );
      }
      const associatedNames = page
        .filter(
          (item) =>
            item.x >= rowX - 0.8 &&
            item.x < nextRowX - 0.8 &&
            item.y >= 235 &&
            item.y < 330,
        )
        .sort((left, right) => left.x - right.x || left.y - right.y)
        .map((item) => item.text)
        .filter(
          (value) =>
            !/^PROPERTY OWNER$/i.test(value) &&
            isConservativeMolineBusinessName(value),
        );
      const sourceParcelText = readRotatedLegacyCell(page, rowX, 370, 420);
      const workLocation = readRotatedLegacyCell(page, rowX, 420, 475);
      const projectDescription = readRotatedLegacyCell(page, rowX, 475, 600);
      const sourcePermitStatus = readRotatedLegacyCell(page, rowX, 600, 700);
      const valuation = parseCurrency(
        readRotatedLegacyCell(page, rowX, 700, Number.POSITIVE_INFINITY),
      );
      records.push({
        source_system: MOLINE_REPORT_SOURCE_SYSTEM,
        source_url: source.url,
        city: "Moline",
        permit_number: null,
        parcel_identifier: null,
        work_location: workLocation,
        permit_issue_date: issueDate,
        record_status: "Issued",
        record_type: carriedRecordType,
        project_description: projectDescription,
        contractor_business_names: [...new Set(associatedNames)],
        is_roof_permit: /\broof(?:ing)?\b/i.test(
          `${carriedRecordType} ${projectDescription ?? ""}`,
        ),
        raw: {
          source_archive_id: source.archiveId,
          source_report_month: source.reportMonth,
          source_report_title: source.title,
          source_application_year: applicationYear,
          source_application_number: applicationNumber,
          source_permit_code: carriedPermitCode,
          source_parcel_text: sourceParcelText,
          source_permit_status: sourcePermitStatus,
          permit_subtype: null,
          project_valuation: valuation,
          source_page: dateItem.pageNumber,
          source_pages: [dateItem.pageNumber],
          source_reports: [
            {
              archive_id: source.archiveId,
              report_month: source.reportMonth,
              report_title: source.title,
              report_url: source.url,
              pages: [dateItem.pageNumber],
            },
          ],
          parser_layout: "legacy-rotated-v2",
        },
      });
    }
  }
  if (records.length === 0) {
    throw new Error(
      `No permit rows parsed from rotated Moline report ${source.archiveId}`,
    );
  }
  return records;
}

/**
 * Parse any explicitly supported Moline issued-permit layout.
 *
 * @param {readonly (readonly PositionedText[])[]} pages - Positioned PDF pages.
 * @param {MolineReportSource} source - Official report provenance.
 * @returns {readonly MolineIssuedPermit[]} Parsed records.
 */
export function parseMolineIssuedPermitReport(pages, source) {
  if (isCurrentMolineReportLayout(pages)) {
    return parseCurrentMolineIssuedPermitReport(pages, source);
  }
  if (isRotatedLegacyMolineReportLayout(pages)) {
    return parseRotatedLegacyMolineIssuedPermitReport(pages, source);
  }
  if (isLegacyApplicationMolineReportLayout(pages)) {
    return parseLegacyApplicationMolineIssuedPermitReport(pages, source);
  }
  const reason = isCompactedLegacyMolineReportLayout(pages)
    ? "compacted legacy application identity"
    : "unknown layout";
  throw new Error(`Unsupported Moline report ${source.archiveId}: ${reason}`);
}

/**
 * Return the stable loader key for one parsed permit.
 *
 * @param {MolineIssuedPermit} record - Parsed permit.
 * @returns {string} Source-system and permit-number key.
 */
export function molinePermitLoaderKey(record) {
  if (record.permit_number !== null) {
    return `${record.source_system}:${record.permit_number}`;
  }
  const applicationYear = record.raw.source_application_year;
  const applicationNumber = record.raw.source_application_number;
  const permitCode = record.raw.source_permit_code;
  if (
    applicationYear === null ||
    applicationNumber === null ||
    permitCode === null
  ) {
    throw new Error("Legacy Moline permit is missing official identity fields");
  }
  return `${record.source_system}:application:${applicationYear}:${applicationNumber}:${permitCode}:issued:${record.permit_issue_date}`;
}

/**
 * Deduplicate exact repeated permits by loader key and reject conflicting variants.
 *
 * @param {readonly MolineIssuedPermit[]} records - Candidate records.
 * @returns {readonly MolineIssuedPermit[]} Deterministic unique permits.
 */
export function dedupeMolineIssuedPermits(records) {
  /** @type {Map<string, MolineIssuedPermit>} */
  const byKey = new Map();
  for (const record of records) {
    const key = molinePermitLoaderKey(record);
    const existing = byKey.get(key);
    if (existing !== undefined) {
      const normalizePages = (/** @type {MolineIssuedPermit} */ value) => ({
        ...value,
        source_url: "",
        contractor_business_names: [],
        raw: {
          ...value.raw,
          source_archive_id: "",
          source_report_month: "",
          source_report_title: "",
          source_page: 0,
          source_pages: [],
          source_reports: [],
        },
      });
      if (
        JSON.stringify(normalizePages(existing)) !==
        JSON.stringify(normalizePages(record))
      ) {
        throw new Error(
          `Conflicting Moline permit variants for loader key ${key}`,
        );
      }
      const contractorBusinessNames = [
        ...new Set([
          ...existing.contractor_business_names,
          ...record.contractor_business_names,
        ]),
      ].sort((left, right) => left.localeCompare(right));
      /** @type {Map<string, MolineSourceReportProvenance>} */
      const sourceReportsByKey = new Map();
      for (const sourceReport of [
        ...existing.raw.source_reports,
        ...record.raw.source_reports,
      ]) {
        const sourceReportKey = `${sourceReport.report_month}:${sourceReport.archive_id}`;
        const prior = sourceReportsByKey.get(sourceReportKey);
        const pages = [
          ...new Set([...(prior?.pages ?? []), ...sourceReport.pages]),
        ].sort((left, right) => left - right);
        sourceReportsByKey.set(sourceReportKey, {
          ...sourceReport,
          pages,
        });
      }
      const sourceReports = [...sourceReportsByKey.values()].sort(
        (left, right) =>
          left.report_month.localeCompare(right.report_month) ||
          Number(left.archive_id) - Number(right.archive_id),
      );
      const primarySourceReport = sourceReports[0];
      if (primarySourceReport === undefined) {
        throw new Error(`Moline permit ${key} has no source provenance`);
      }
      byKey.set(key, {
        ...existing,
        source_url: primarySourceReport.report_url,
        contractor_business_names: contractorBusinessNames,
        raw: {
          ...existing.raw,
          source_archive_id: primarySourceReport.archive_id,
          source_report_month: primarySourceReport.report_month,
          source_report_title: primarySourceReport.report_title,
          source_page: primarySourceReport.pages[0] ?? existing.raw.source_page,
          source_pages: primarySourceReport.pages,
          source_reports: sourceReports,
        },
      });
      continue;
    }
    byKey.set(key, record);
  }
  return [...byKey.values()].sort(
    (left, right) =>
      left.permit_issue_date.localeCompare(right.permit_issue_date) ||
      molinePermitLoaderKey(left).localeCompare(molinePermitLoaderKey(right)),
  );
}

/**
 * Map private staging into a closed public-safe permit allowlist.
 *
 * @param {MolineIssuedPermit} record - Reviewed private record.
 * @returns {MolinePublicPermit} Public-safe row.
 */
export function toMolinePublicPermit(record) {
  return {
    permit_key: molinePermitLoaderKey(record),
    source_system: record.source_system,
    source_report_archive_id: record.raw.source_archive_id,
    source_report_month: record.raw.source_report_month,
    source_report_title: record.raw.source_report_title,
    source_report_url: record.source_url,
    source_report_archive_ids: record.raw.source_reports.map(
      (sourceReport) => sourceReport.archive_id,
    ),
    source_report_months: record.raw.source_reports.map(
      (sourceReport) => sourceReport.report_month,
    ),
    source_report_titles: record.raw.source_reports.map(
      (sourceReport) => sourceReport.report_title,
    ),
    source_report_urls: record.raw.source_reports.map(
      (sourceReport) => sourceReport.report_url,
    ),
    permit_number: record.permit_number,
    source_application_year: record.raw.source_application_year,
    source_application_number: record.raw.source_application_number,
    source_permit_code: record.raw.source_permit_code,
    permit_issue_date: record.permit_issue_date,
    record_status: record.record_status,
    record_type: record.record_type,
    permit_subtype: record.raw.permit_subtype,
    city: record.city,
    is_roof_permit: record.is_roof_permit,
  };
}

/**
 * Render deterministic private-staging JSONL.
 *
 * @param {readonly MolineIssuedPermit[]} records - Parsed permits.
 * @returns {string} Newline-delimited JSON with a trailing newline.
 */
export function renderMolinePrivateJsonl(records) {
  const unique = dedupeMolineIssuedPermits(records);
  return unique.length === 0
    ? ""
    : `${unique.map((record) => JSON.stringify(record)).join("\n")}\n`;
}

/**
 * Render deterministic public-safe JSONL.
 *
 * @param {readonly MolineIssuedPermit[]} records - Parsed permits.
 * @returns {string} Closed-allowlist newline-delimited JSON.
 */
export function renderMolinePublicJsonl(records) {
  const unique = dedupeMolineIssuedPermits(records).map(toMolinePublicPermit);
  return unique.length === 0
    ? ""
    : `${unique.map((record) => JSON.stringify(record)).join("\n")}\n`;
}
