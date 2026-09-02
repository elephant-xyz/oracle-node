// @ts-check

import * as cheerio from "cheerio";

import { preserveMunicipalParcelIdentifier } from "./broward-municipal-core.mjs";

/**
 * @typedef {import("./broward-municipal-core.mjs").BrowardMunicipalInspection} BrowardMunicipalInspection
 * @typedef {import("./broward-municipal-core.mjs").BrowardMunicipalJurisdictionConfig} BrowardMunicipalJurisdictionConfig
 * @typedef {import("./broward-municipal-core.mjs").BrowardMunicipalQuery} BrowardMunicipalQuery
 * @typedef {import("./broward-municipal-core.mjs").BrowardMunicipalSearchPage} BrowardMunicipalSearchPage
 * @typedef {import("./broward-municipal-core.mjs").BrowardMunicipalSearchReference} BrowardMunicipalSearchReference
 * @typedef {import("./broward-municipal-core.mjs").NormalizedBrowardMunicipalPermit} NormalizedBrowardMunicipalPermit
 */

/**
 * Convert unknown source text to a collapsed, non-empty string.
 *
 * @param {unknown} value - Candidate source value.
 * @returns {string | null} Collapsed source text or null.
 */
function readText(value) {
  if (typeof value !== "string") return null;
  const normalized = value
    .replace(/\u00a0/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
  return normalized.length === 0 ? null : normalized;
}

/**
 * Read visible text while treating line breaks as spaces.
 *
 * @param {import("cheerio").Cheerio<import("domhandler").AnyNode>} selection - Parsed source selection.
 * @returns {string | null} Visible collapsed text.
 */
function readSelectionText(selection) {
  const cloned = selection.clone();
  cloned.find("br").replaceWith(" ");
  return readText(cloned.text());
}

/**
 * Require one source identity field.
 *
 * @param {string | null} value - Optional source text.
 * @param {string} fieldName - Field name for a fail-closed error.
 * @returns {string} Required source text.
 */
function requireText(value, fieldName) {
  if (value === null) {
    throw new Error(`Broward municipal ${fieldName} is missing`);
  }
  return value;
}

/**
 * Normalize labels so vendor punctuation and capitalization do not change
 * explicit field matching.
 *
 * @param {string} value - Source label.
 * @returns {string} Lowercase alphanumeric label key.
 */
function labelKey(value) {
  return value
    .replace(/^[*\s]+/gu, "")
    .replace(/[:\s]+$/gu, "")
    .replace(/[^a-z0-9]+/giu, " ")
    .replace(/\s+/gu, " ")
    .trim()
    .toLowerCase();
}

/**
 * Collect explicit table, definition-list, and label/value pairs without
 * interpreting unrelated page prose.
 *
 * Duplicate labels retain their first non-empty value. Identity reconciliation
 * performed by protocol parsers catches conflicting permit numbers separately.
 *
 * @param {import("cheerio").CheerioAPI} $ - Parsed source document.
 * @returns {ReadonlyMap<string, string>} Normalized label/value map.
 */
function collectLabeledFields($) {
  /** @type {Map<string, string>} */
  const fields = new Map();
  const add = (
    /** @type {string | null} */ label,
    /** @type {string | null} */ value,
  ) => {
    if (label === null || value === null || label.length > 100) return;
    const key = labelKey(label);
    if (key.length > 0 && !fields.has(key)) fields.set(key, value);
  };

  for (const rowElement of $("tr").toArray()) {
    const cells = $(rowElement).children("th,td").toArray();
    if (cells.length < 2) continue;
    if (cells.length % 2 === 0) {
      for (let index = 0; index < cells.length; index += 2) {
        const labelCell = cells[index];
        const valueCell = cells[index + 1];
        if (labelCell !== undefined && valueCell !== undefined) {
          const value = $(valueCell);
          add(
            readSelectionText($(labelCell)),
            value.hasClass("case-header-field-value")
              ? readSelectionText(
                  value.children().not(".case-header-status-badge"),
                )
              : readSelectionText(value),
          );
        }
      }
    } else {
      add(
        readSelectionText($(cells[0])),
        readText(
          cells
            .slice(1)
            .map((cell) => readSelectionText($(cell)))
            .filter((value) => value !== null)
            .join(" "),
        ),
      );
    }
  }
  for (const termElement of $("dt").toArray()) {
    const term = $(termElement);
    add(readSelectionText(term), readSelectionText(term.next("dd")));
  }
  for (const labelElement of $("label").toArray()) {
    const label = $(labelElement);
    const targetId = label.attr("for");
    let value = null;
    if (targetId !== undefined && targetId.length > 0) {
      const target = $("[id]")
        .filter((_index, element) => $(element).attr("id") === targetId)
        .first();
      value =
        readText(target.attr("value")) ??
        readSelectionText(target) ??
        readSelectionText(label.parent().children().not(label));
    } else {
      value =
        readSelectionText(label.next()) ??
        readSelectionText(label.parent().children().not(label));
    }
    add(readSelectionText(label), value);
  }
  for (const labelElement of $(
    [
      ".field-label",
      ".field_label",
      ".detail-label",
      ".control-label",
      ".case-header-field-label",
      ".project-section-field-label",
    ].join(", "),
  ).toArray()) {
    const label = $(labelElement);
    const structuredValue = label.next(
      ".case-header-field-value, .project-section-field-value, .field-value, .detail-value",
    );
    const caseHeaderValue = label.hasClass("case-header-field-label")
      ? readSelectionText(
          structuredValue.children().not(".case-header-status-badge"),
        )
      : null;
    const adjacentValue = readSelectionText(label.next());
    const parentFallback =
      label.hasClass("field-label") || label.hasClass("field_label")
        ? null
        : readSelectionText(label.parent().children().not(label));
    add(
      readSelectionText(label),
      caseHeaderValue ??
        readSelectionText(structuredValue) ??
        adjacentValue ??
        parentFallback,
    );
  }
  return fields;
}

/**
 * Read the first matching explicit label alias.
 *
 * @param {ReadonlyMap<string, string>} fields - Parsed label/value map.
 * @param {readonly string[]} aliases - Human-readable source label aliases.
 * @returns {string | null} Matching source value.
 */
function field(fields, aliases) {
  for (const alias of aliases) {
    const value = fields.get(labelKey(alias));
    if (value !== undefined) return value;
  }
  return null;
}

/**
 * Parse a strict US source date into ISO form.
 *
 * Supports four-digit source years and the legacy two-digit years used by
 * eGovPLUS. Two-digit years `00` through `69` map to 2000–2069 and `70`
 * through `99` map to 1970–1999.
 *
 * @param {string | null} value - Source date.
 * @param {string} fieldName - Field name for malformed-date errors.
 * @returns {string | null} ISO calendar date.
 */
function parseSourceDate(value, fieldName) {
  if (value === null || value === "" || /^(?:-\s*)+$/u.test(value)) {
    return null;
  }
  const match = /^(\d{1,2})[/-](\d{1,2})[/-](\d{2}|\d{4})$/u.exec(value);
  if (match === null) {
    throw new Error(`Invalid Broward municipal ${fieldName}: ${value}`);
  }
  const month = Number(match[1]);
  const day = Number(match[2]);
  const rawYear = Number(match[3]);
  const year =
    match[3]?.length === 2
      ? rawYear <= 69
        ? 2_000 + rawYear
        : 1_900 + rawYear
      : rawYear;
  const parsed = new Date(Date.UTC(year, month - 1, day));
  if (
    parsed.getUTCFullYear() !== year ||
    parsed.getUTCMonth() !== month - 1 ||
    parsed.getUTCDate() !== day
  ) {
    throw new Error(`Invalid Broward municipal ${fieldName}: ${value}`);
  }
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

/**
 * Parse an optional source currency or plain non-negative number.
 *
 * @param {string | null} value - Source value.
 * @param {string} fieldName - Field name for malformed-number errors.
 * @returns {number | null} Finite non-negative source number.
 */
function parseSourceNumber(value, fieldName) {
  if (value === null || value === "") return null;
  if (!/^\$?\d+(?:,\d{3})*(?:\.\d{1,2})?$/u.test(value)) {
    throw new Error(`Invalid Broward municipal ${fieldName}: ${value}`);
  }
  const parsed = Number(value.replace(/[$,]/gu, ""));
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`Invalid Broward municipal ${fieldName}: ${value}`);
  }
  return parsed;
}

/**
 * Canonicalize and constrain a vendor detail link to the configured origin and
 * protocol path. Session CSRF parameters are removed from persisted provenance.
 *
 * @param {BrowardMunicipalJurisdictionConfig} config - Jurisdiction configuration.
 * @param {string} href - Candidate absolute or relative detail route.
 * @returns {string} Canonical official detail URL.
 */
function canonicalSourceUrl(config, href) {
  const searchUrl = new URL(config.searchUrl);
  const parsed = new URL(href, searchUrl);
  const protocolPrefixes =
    config.protocol === "coconut_creek"
      ? ["/sd/permit/"]
      : config.protocol === "click2gov"
        ? ["/Click2GovBP/"]
        : config.protocol === "tyler_esuite"
          ? [
              searchUrl.pathname.slice(
                0,
                searchUrl.pathname.toLowerCase().indexOf("/esuite.permits/") +
                  "/eSuite.Permits/".length,
              ),
            ]
          : config.protocol === "egovplus"
            ? ["/eGovPlus83/"]
            : config.protocol === "smartgov"
              ? ["/ApplicationPublic/", "/PermittingPublic/"]
              : ["/"];
  if (
    parsed.origin !== searchUrl.origin ||
    !protocolPrefixes.some((prefix) =>
      parsed.pathname.toLowerCase().startsWith(prefix.toLowerCase()),
    ) ||
    parsed.username !== "" ||
    parsed.password !== ""
  ) {
    throw new Error(
      `Broward municipal detail URL is outside ${config.jurisdiction}: ${parsed.toString()}`,
    );
  }
  parsed.searchParams.delete("OWASP_CSRFTOKEN");
  parsed.hash = "";
  return parsed.toString();
}

/**
 * Map one result row to its explicit column headings.
 *
 * @param {import("cheerio").CheerioAPI} $ - Parsed source document.
 * @param {import("cheerio").Cheerio<import("domhandler").AnyNode>} row - Result row.
 * @returns {ReadonlyMap<string, string>} Header-keyed source values.
 */
function mapResultRow($, row) {
  const table = row.closest("table");
  const headerRow = table
    .find("tr")
    .toArray()
    .find((element) => $(element).children("th").toArray().length > 0);
  if (headerRow === undefined) return new Map();
  const headers = $(headerRow)
    .children("th")
    .toArray()
    .map((cell) => labelKey(readSelectionText($(cell)) ?? ""));
  const cells = row.children("td").toArray();
  /** @type {Map<string, string>} */
  const values = new Map();
  for (const [index, cellElement] of cells.entries()) {
    const header = headers[index];
    const value = readSelectionText($(cellElement));
    if (header !== undefined && header.length > 0 && value !== null) {
      values.set(header, value);
    }
  }
  return values;
}

/**
 * Deduplicate repeated responsive/contact rows by stable vendor identity.
 *
 * @param {readonly BrowardMunicipalSearchReference[]} references - Candidate source references.
 * @returns {readonly BrowardMunicipalSearchReference[]} Stable unique references.
 */
function dedupeReferences(references) {
  /** @type {Map<string, BrowardMunicipalSearchReference>} */
  const byId = new Map();
  for (const reference of references) {
    const existing = byId.get(reference.sourceRecordId);
    if (
      existing !== undefined &&
      (existing.permitNumber !== reference.permitNumber ||
        existing.detailUrl !== reference.detailUrl)
    ) {
      throw new Error(
        `Conflicting Broward municipal list identity ${reference.sourceRecordId}`,
      );
    }
    if (existing === undefined) byId.set(reference.sourceRecordId, reference);
  }
  return [...byId.values()].sort(
    (left, right) =>
      left.permitNumber.localeCompare(right.permitNumber) ||
      left.sourceRecordId.localeCompare(right.sourceRecordId),
  );
}

/**
 * Parse bounded inspection rows while omitting inspector names, comments,
 * contacts, and scheduling controls.
 *
 * @param {import("cheerio").CheerioAPI} $ - Parsed detail document.
 * @param {number} [maxInspections=1000] - Hard row ceiling.
 * @returns {readonly BrowardMunicipalInspection[]} Allow-listed inspection summaries.
 */
function parseInspectionTables($, maxInspections = 1_000) {
  /** @type {BrowardMunicipalInspection[]} */
  const inspections = [];
  for (const tableElement of $("table").toArray()) {
    const table = $(tableElement);
    const headerRow = table
      .find("tr")
      .toArray()
      .find((element) => $(element).children("th").toArray().length > 0);
    if (headerRow === undefined) continue;
    const headers = $(headerRow)
      .children("th")
      .toArray()
      .map((cell) => labelKey(readSelectionText($(cell)) ?? ""));
    const typeIndex = headers.findIndex((header) =>
      ["inspection type", "type"].includes(header),
    );
    if (typeIndex < 0) continue;
    const statusIndex = headers.findIndex((header) => header === "status");
    const resultIndex = headers.findIndex((header) =>
      ["pass fail", "result", "outcome"].includes(header),
    );
    const scheduledIndex = headers.findIndex((header) =>
      ["scheduled date", "sched date"].includes(header),
    );
    const completedIndex = headers.findIndex((header) =>
      ["inspection date", "insp date", "completed date", "date"].includes(
        header,
      ),
    );
    const idIndex = headers.findIndex((header) =>
      ["inspection id", "id", "num", "number"].includes(header),
    );
    for (const rowElement of table.find("tr").toArray()) {
      const cells = $(rowElement).children("td").toArray();
      if (cells.length === 0) continue;
      const values = cells.map((cell) => readSelectionText($(cell)));
      const inspectionType = values[typeIndex] ?? null;
      if (inspectionType === null) continue;
      inspections.push({
        source_id: idIndex < 0 ? null : (values[idIndex] ?? null),
        inspection_type: inspectionType,
        scheduled_date:
          scheduledIndex < 0
            ? null
            : parseSourceDate(
                values[scheduledIndex] ?? null,
                "inspection scheduled date",
              ),
        completed_date:
          completedIndex < 0
            ? null
            : parseSourceDate(
                values[completedIndex] ?? null,
                "inspection completed date",
              ),
        status: statusIndex < 0 ? null : (values[statusIndex] ?? null),
        result: resultIndex < 0 ? null : (values[resultIndex] ?? null),
      });
      if (inspections.length > maxInspections) {
        throw new Error(
          `Broward municipal inspection limit ${String(maxInspections)} exceeded`,
        );
      }
    }
  }
  return inspections;
}

/**
 * Read one allow-listed string from search-list provenance.
 *
 * @param {BrowardMunicipalSearchReference} reference - Parsed source reference.
 * @param {string} key - Fixed allow-listed list field.
 * @returns {string | null} Collapsed source value.
 */
function listField(reference, key) {
  return readText(reference.listData[key]);
}

/**
 * Parse the Coconut Creek legacy permit-status result table.
 *
 * The permit number is carried by a submit control rather than visible anchor
 * text. Owner cells are deliberately ignored. A header-only result table is a
 * reconciled empty response; a redirect back to the search form is rejected by
 * the transport before this parser is called.
 *
 * @param {string} html - Official result HTML.
 * @param {BrowardMunicipalJurisdictionConfig} config - Coconut Creek configuration.
 * @param {object} [options] - Parser safety controls.
 * @param {number} [options.maxRows=50] - Exclusive raw result-row ceiling.
 * @returns {BrowardMunicipalSearchPage} Stable same-session result references.
 */
export function parseCoconutCreekSearchHtml(
  html,
  config,
  { maxRows = 50 } = {},
) {
  if (config.protocol !== "coconut_creek") {
    throw new Error(
      "Coconut Creek parser received a different vendor protocol",
    );
  }
  const $ = cheerio.load(html);
  const title = readText($("title").text());
  if (title === null || !/Permit Status.*Coconut Creek/iu.test(title)) {
    throw new Error(
      `Unexpected Coconut Creek result title: ${title ?? "(missing)"}`,
    );
  }
  const table = $("table")
    .filter((_index, element) => {
      const headers = $(element)
        .find("th")
        .toArray()
        .map((cell) => labelKey(readSelectionText($(cell)) ?? ""));
      return (
        headers.includes("permit") &&
        headers.includes("status") &&
        headers.includes("type") &&
        headers.includes("address")
      );
    })
    .first();
  if (table.length === 0) {
    throw new Error("Coconut Creek result response lacks the permit table");
  }
  const rows = table
    .find("tr")
    .toArray()
    .filter(
      (row) => $(row).find('input[name="btnsubmit"][value]').length === 1,
    );
  if (rows.length >= maxRows) {
    throw new Error(
      `Coconut Creek result row limit ${String(maxRows)} reached (${String(rows.length)})`,
    );
  }
  /** @type {BrowardMunicipalSearchReference[]} */
  const references = [];
  for (const rowElement of rows) {
    const row = $(rowElement);
    const permitNumber = requireText(
      readText(row.find('input[name="btnsubmit"]').attr("value")),
      "Coconut Creek permit number",
    );
    if (!/^[A-Z0-9-]+$/iu.test(permitNumber)) {
      throw new Error("Coconut Creek result has an invalid permit identity");
    }
    const values = mapResultRow($, row);
    references.push({
      sourceRecordId: permitNumber,
      permitNumber,
      detailUrl: canonicalSourceUrl(config, "permit_status_03.asp"),
      sourcePage: 1,
      listData: {
        address: values.get("address") ?? null,
        record_status: values.get("status") ?? null,
        record_type: values.get("type") ?? null,
      },
    });
  }
  return { references: dedupeReferences(references), nextPage: null };
}

/**
 * Parse one selected Coconut Creek status detail.
 *
 * The source exposes a compact status record rather than inspection history.
 * Search-list status/type values are reconciled with the selected permit
 * identity, while owner and payment fields are omitted.
 *
 * @param {string} html - Official selected-record HTML.
 * @param {object} context - Search identity and provenance.
 * @param {BrowardMunicipalJurisdictionConfig} context.config - Coconut Creek configuration.
 * @param {BrowardMunicipalSearchReference} context.reference - Search reference selected in the same ASP session.
 * @param {BrowardMunicipalQuery} context.query - Exact query that discovered the record.
 * @returns {NormalizedBrowardMunicipalPermit} Reconciled status record.
 */
export function parseCoconutCreekDetailHtml(
  html,
  { config, reference, query },
) {
  if (config.protocol !== "coconut_creek") {
    throw new Error(
      "Coconut Creek parser received a different vendor protocol",
    );
  }
  const $ = cheerio.load(html);
  const title = readText($("title").text());
  if (title === null || !/Permit Status.*Coconut Creek/iu.test(title)) {
    throw new Error(
      `Unexpected Coconut Creek detail title: ${title ?? "(missing)"}`,
    );
  }
  const fields = collectLabeledFields($);
  const permitNumber = requireText(
    field(fields, ["Permit #", "Permit Number"]),
    "Coconut Creek detail permit number",
  );
  if (permitNumber !== reference.permitNumber) {
    throw new Error("Coconut Creek detail permit identity mismatch");
  }
  const recordType = listField(reference, "record_type");
  const description = field(fields, ["Permit Desc", "Permit Description"]);
  return {
    source_system: config.sourceSystem,
    source_protocol: "coconut_creek",
    source_url: canonicalSourceUrl(config, reference.detailUrl),
    source_search_url: config.searchUrl,
    source_record_id: reference.sourceRecordId,
    record_key: `${config.sourceSystem}:${reference.sourceRecordId}`,
    jurisdiction: config.jurisdiction,
    permit_number: permitNumber,
    parcel_identifier: preserveMunicipalParcelIdentifier(
      field(fields, ["Property ID", "Parcel ID"]),
    ),
    query_folio: query.kind === "folio" ? query.value : null,
    work_location:
      field(fields, ["Property Address"]) ?? listField(reference, "address"),
    application_date: null,
    permit_issue_date: null,
    expiration_date: null,
    record_status: listField(reference, "record_status"),
    record_type: recordType,
    project_description: description,
    job_value: null,
    inspections: [],
    is_roof_permit: /\broof(?:ing)?\b/iu.test(
      `${recordType ?? ""} ${description ?? ""}`,
    ),
    raw: {
      source_page: reference.sourcePage,
      query_kind: query.kind,
      detail_contract: "same_anonymous_asp_session",
    },
  };
}

/**
 * Parse a Click2Gov client-side result table.
 *
 * One permit can appear repeatedly for owner/contractor names. Those contact
 * columns are deliberately not copied and the stable application number is
 * deduplicated before details. The raw source-row ceiling is checked first so
 * a broad search cannot hide behind dedupe.
 *
 * @param {string} html - Official Click2Gov result HTML.
 * @param {BrowardMunicipalJurisdictionConfig} config - Click2Gov jurisdiction configuration.
 * @param {object} [options] - Parser limits and source page.
 * @param {number} [options.sourcePage=1] - One-based source page.
 * @param {number} [options.maxRows=50] - Hard raw result-row ceiling.
 * @returns {BrowardMunicipalSearchPage} Deduplicated detail references.
 */
export function parseClick2GovSearchHtml(
  html,
  config,
  { sourcePage = 1, maxRows = 50 } = {},
) {
  if (config.protocol !== "click2gov") {
    throw new Error("Click2Gov parser received a different vendor protocol");
  }
  const $ = cheerio.load(html);
  const title = readText($("title").text());
  if (
    title === null ||
    !/(?:Click2Gov|Building Permits).*Select Permit Results/iu.test(title)
  ) {
    const body = readSelectionText($("body")) ?? "";
    if (
      /no matching|no permits found|no records found|no results returned/iu.test(
        body,
      )
    ) {
      return { references: [], nextPage: null };
    }
    throw new Error(
      `Unexpected Click2Gov result title: ${title ?? "(missing)"}`,
    );
  }
  const rows = $("tr")
    .toArray()
    .filter(
      (row) =>
        $(row)
          .find(
            'a[href*="permit.appYearAndNumber"][href*="validatePermitView"]',
          )
          .toArray().length > 0,
    );
  if (rows.length >= maxRows) {
    throw new Error(
      `Click2Gov result row limit ${String(maxRows)} exceeded (${String(rows.length)})`,
    );
  }
  /** @type {BrowardMunicipalSearchReference[]} */
  const references = [];
  for (const rowElement of rows) {
    const row = $(rowElement);
    const anchors = row
      .find('a[href*="permit.appYearAndNumber"][href*="validatePermitView"]')
      .toArray();
    if (anchors.length === 0) continue;
    const anchor = $(anchors[0]);
    const permitNumber = requireText(
      readSelectionText(anchor),
      "Click2Gov application number",
    );
    if (!/^\d{2}-\d{8}$/u.test(permitNumber)) {
      throw new Error(`Invalid Click2Gov application number: ${permitNumber}`);
    }
    const rowValues = mapResultRow($, row);
    references.push({
      sourceRecordId: permitNumber,
      permitNumber,
      detailUrl: canonicalSourceUrl(config, anchor.attr("href") ?? ""),
      sourcePage,
      listData: {
        address: rowValues.get("address") ?? null,
        record_type: rowValues.get("application type") ?? null,
        record_status: rowValues.get("application status") ?? null,
      },
    });
  }
  return { references: dedupeReferences(references), nextPage: null };
}

/**
 * Normalize the spaced Click2Gov detail application display and reconcile it
 * with the search-list identity.
 *
 * @param {string} value - Detail display such as `99 - 7758`.
 * @returns {string} Canonical Click2Gov application number.
 */
function normalizeClick2GovApplicationNumber(value) {
  const match = /^(\d{2})\s*-\s*(\d{1,8})$/u.exec(value);
  if (match === null) {
    throw new Error(`Invalid Click2Gov detail application number: ${value}`);
  }
  return `${match[1]}-${match[2]?.padStart(8, "0")}`;
}

/**
 * Parse one session-bound Click2Gov status detail.
 *
 * @param {string} html - Official Click2Gov detail HTML.
 * @param {object} context - Search identity and provenance.
 * @param {BrowardMunicipalJurisdictionConfig} context.config - Click2Gov jurisdiction.
 * @param {BrowardMunicipalSearchReference} context.reference - Parsed search reference.
 * @param {BrowardMunicipalQuery} context.query - Query that discovered the record.
 * @returns {NormalizedBrowardMunicipalPermit} Reconciled private-staging permit.
 */
export function parseClick2GovDetailHtml(html, { config, reference, query }) {
  const $ = cheerio.load(html);
  const title = requireText(readText($("title").text()), "Click2Gov title");
  if (!/(?:Click2Gov|Building Permits).*Status Detail/iu.test(title)) {
    throw new Error(`Unexpected Click2Gov detail title: ${title}`);
  }
  const fields = collectLabeledFields($);
  const detailPermitNumber = normalizeClick2GovApplicationNumber(
    requireText(
      field(fields, [
        "Application #",
        "Application Number",
        "Permit (Application) Number",
      ]),
      "Click2Gov detail application number",
    ),
  );
  if (detailPermitNumber !== reference.permitNumber) {
    throw new Error("Click2Gov detail application identity mismatch");
  }
  const recordType = field(fields, ["Application Type"]);
  const description = field(fields, [
    "Project Description",
    "Description",
    "Tenant Name",
  ]);
  const roofText = `${recordType ?? ""} ${description ?? ""}`;
  return {
    source_system: config.sourceSystem,
    source_protocol: "click2gov",
    source_url: reference.detailUrl,
    source_search_url: config.searchUrl,
    source_record_id: reference.sourceRecordId,
    record_key: `${config.sourceSystem}:${reference.sourceRecordId}`,
    jurisdiction: config.jurisdiction,
    permit_number: detailPermitNumber,
    parcel_identifier: preserveMunicipalParcelIdentifier(
      field(fields, ["Parcel ID", "Parcel Number", "Folio"]),
    ),
    query_folio: query.kind === "folio" ? query.value : null,
    work_location: field(fields, ["Address", "Property Address"]),
    application_date: parseSourceDate(
      field(fields, ["Application Date"]),
      "Click2Gov application date",
    ),
    permit_issue_date: parseSourceDate(
      field(fields, ["Issued Date", "Issue Date"]),
      "Click2Gov issue date",
    ),
    expiration_date: parseSourceDate(
      field(fields, ["Expiration Date", "Expires"]),
      "Click2Gov expiration date",
    ),
    record_status: field(fields, ["Application Status", "Status"]),
    record_type: recordType,
    project_description: description,
    job_value: parseSourceNumber(
      field(fields, ["Valuation", "Job Value"]),
      "Click2Gov valuation",
    ),
    inspections: parseInspectionTables($),
    is_roof_permit: /\broof(?:ing)?\b/iu.test(roofText),
    raw: {
      source_page: reference.sourcePage,
      query_kind: query.kind,
      detail_contract: "same_anonymous_session",
    },
  };
}

/**
 * Parse one Tyler/New World eSuite search page.
 *
 * Responsive markup can repeat the same detail anchor. Stable numeric permit
 * ids are deduplicated, while a bounded numbered next-page marker is retained.
 *
 * @param {string} html - Official eSuite search response.
 * @param {BrowardMunicipalJurisdictionConfig} config - eSuite jurisdiction.
 * @param {object} [options] - Parser context.
 * @param {number} [options.sourcePage=1] - One-based source page.
 * @param {number} [options.maxRows=50] - Hard raw link-row ceiling.
 * @returns {BrowardMunicipalSearchPage} Parsed search page.
 */
export function parseTylerEsuiteSearchHtml(
  html,
  config,
  { sourcePage = 1, maxRows = 50 } = {},
) {
  if (config.protocol !== "tyler_esuite") {
    throw new Error("eSuite parser received a different vendor protocol");
  }
  const $ = cheerio.load(html);
  const title = readText($("title").text());
  if (title !== "Public Search" && title !== "Welcome") {
    throw new Error(`Unexpected eSuite search title: ${title ?? "(missing)"}`);
  }
  const anchors = $('a[href*="ContractorPermitDetails.aspx?id="]').toArray();
  if (anchors.length > maxRows * 2) {
    throw new Error(
      `eSuite result link limit ${String(maxRows * 2)} exceeded (${String(anchors.length)})`,
    );
  }
  /** @type {BrowardMunicipalSearchReference[]} */
  const references = [];
  for (const anchorElement of anchors) {
    const anchor = $(anchorElement);
    const permitNumber = readSelectionText(anchor);
    if (permitNumber === null || !/^[A-Z0-9-]+$/iu.test(permitNumber)) continue;
    const detailUrl = canonicalSourceUrl(config, anchor.attr("href") ?? "");
    const sourceRecordId = new URL(detailUrl).searchParams.get("id");
    if (sourceRecordId === null || !/^\d+$/u.test(sourceRecordId)) {
      throw new Error(`eSuite detail link lacks numeric id: ${detailUrl}`);
    }
    const rowValues = mapResultRow($, anchor.closest("tr"));
    references.push({
      sourceRecordId,
      permitNumber,
      detailUrl,
      sourcePage,
      listData: {
        address: rowValues.get("address") ?? null,
        record_status: rowValues.get("status") ?? null,
        record_type:
          rowValues.get("permit type") ?? rowValues.get("type") ?? null,
      },
    });
  }
  const unique = dedupeReferences(references);
  if (unique.length > maxRows) {
    throw new Error(
      `eSuite result row limit ${String(maxRows)} exceeded (${String(unique.length)})`,
    );
  }
  const nextPageText =
    $(`a[data-page="${String(sourcePage + 1)}"]`).attr("data-page") ??
    $("a[rel='next']").attr("data-page") ??
    $("a[href*='__doPostBack']")
      .toArray()
      .map((element) => $(element).attr("href") ?? "")
      .find((href) => href.includes(`Page$${String(sourcePage + 1)}`))
      ?.match(/Page\$(\d+)/u)?.[1] ??
    ($("a[href*='action=next']")
      .toArray()
      .some((element) => /^next$/iu.test(readSelectionText($(element)) ?? ""))
      ? String(sourcePage + 1)
      : null);
  const nextPage =
    nextPageText === null
      ? null
      : /^\d+$/u.test(nextPageText)
        ? Number(nextPageText)
        : null;
  return { references: unique, nextPage };
}

/**
 * Parse one same-session Tyler eSuite permit detail.
 *
 * @param {string} html - Official eSuite detail HTML.
 * @param {object} context - Search identity and provenance.
 * @param {BrowardMunicipalJurisdictionConfig} context.config - eSuite jurisdiction.
 * @param {BrowardMunicipalSearchReference} context.reference - Search reference.
 * @param {BrowardMunicipalQuery} context.query - Query that discovered the permit.
 * @returns {NormalizedBrowardMunicipalPermit} Reconciled normalized permit.
 */
export function parseTylerEsuiteDetailHtml(html, { config, reference, query }) {
  const $ = cheerio.load(html);
  const title = requireText(readText($("title").text()), "eSuite detail title");
  if (!/PermitDetails|Permit Details/iu.test(title)) {
    throw new Error(`Unexpected eSuite detail title: ${title}`);
  }
  const fields = collectLabeledFields($);
  const detailPermitNumber = field(fields, ["Permit #", "Permit Number"]);
  const detailApplicationNumber = field(fields, [
    "Application #",
    "Application Number",
  ]);
  const permitNumber =
    detailPermitNumber ??
    requireText(
      detailApplicationNumber,
      "eSuite detail permit or application number",
    );
  const publicRecordKind =
    detailPermitNumber === null ? "permit_application" : "permit";
  if (permitNumber !== reference.permitNumber) {
    throw new Error("eSuite detail permit identity mismatch");
  }
  const statusDisplay = field(fields, ["Status"]);
  const issuedMatch =
    statusDisplay === null
      ? null
      : /^(.*?)\s+on\s+(\d{1,2}\/\d{1,2}\/\d{4})$/iu.exec(statusDisplay);
  const recordStatus = readText(issuedMatch?.[1] ?? statusDisplay);
  const recordType = field(fields, ["Permit Type", "Type"]);
  const description = field(fields, ["Description", "Project Description"]);
  const roofText = `${recordType ?? ""} ${description ?? ""}`;
  return {
    source_system: config.sourceSystem,
    source_protocol: "tyler_esuite",
    source_url: canonicalSourceUrl(config, reference.detailUrl),
    source_search_url: config.searchUrl,
    source_record_id: reference.sourceRecordId,
    record_key: `${config.sourceSystem}:${reference.sourceRecordId}`,
    jurisdiction: config.jurisdiction,
    permit_number: permitNumber,
    parcel_identifier: preserveMunicipalParcelIdentifier(
      field(fields, ["Parcel", "Parcel Number", "Folio"]),
    ),
    query_folio: query.kind === "folio" ? query.value : null,
    work_location: field(fields, ["Address", "Service Address"]),
    application_date: parseSourceDate(
      field(fields, ["Application Date", "Applied On"]),
      "eSuite application date",
    ),
    permit_issue_date: parseSourceDate(
      issuedMatch?.[2] ??
        field(fields, ["Issued Date", "Issue Date", "Issued On"]),
      "eSuite issue date",
    ),
    expiration_date: parseSourceDate(
      field(fields, ["Expires", "Expiration Date"]),
      "eSuite expiration date",
    ),
    record_status: recordStatus,
    record_type: recordType,
    project_description: description,
    job_value: parseSourceNumber(
      field(fields, [
        "Est. Improvement Value",
        "Estimated Improvement Value",
        "Valuation",
      ]),
      "eSuite valuation",
    ),
    inspections: parseInspectionTables($),
    is_roof_permit: /\broof(?:ing)?\b/iu.test(roofText),
    raw: {
      source_page: reference.sourcePage,
      query_kind: query.kind,
      detail_contract: "same_anonymous_session",
      public_record_kind: publicRecordKind,
    },
  };
}

/**
 * Parse a SmartGov advanced-search result page and retain its explicit numbered
 * pagination marker.
 *
 * @param {string} html - Official SmartGov result HTML.
 * @param {BrowardMunicipalJurisdictionConfig} config - SmartGov jurisdiction.
 * @param {object} [options] - Parser context.
 * @param {number} [options.sourcePage=1] - One-based current page.
 * @param {number} [options.maxRows=50] - Hard result ceiling.
 * @returns {BrowardMunicipalSearchPage} Search references and next page.
 */
export function parseSmartGovSearchHtml(
  html,
  config,
  { sourcePage = 1, maxRows = 50 } = {},
) {
  if (config.protocol !== "smartgov") {
    throw new Error("SmartGov parser received a different vendor protocol");
  }
  const $ = cheerio.load(html);
  const title = readText($("title").text());
  if (title === null || !/Public Portal/iu.test(title)) {
    throw new Error(
      `Unexpected SmartGov result title: ${title ?? "(missing)"}`,
    );
  }
  const anchors = $(
    [
      'a[href*="/ApplicationPublic/"][href*="ApplicationDetail"]',
      'a[href*="/ApplicationPublic/Application/"]',
      '.search-result-title a[onclick*="FormSupport.submitAction"][onclick*="Detail/"]',
    ].join(", "),
  ).toArray();
  if (anchors.length >= maxRows) {
    throw new Error(
      `SmartGov result row limit ${String(maxRows)} exceeded (${String(anchors.length)})`,
    );
  }
  /** @type {BrowardMunicipalSearchReference[]} */
  const references = [];
  for (const anchorElement of anchors) {
    const anchor = $(anchorElement);
    const permitNumber = readSelectionText(anchor);
    if (permitNumber === null) continue;
    const directHref = anchor.attr("href");
    const detailAction = /Detail\/([A-Z0-9-]+)/iu.exec(
      anchor.attr("onclick") ?? "",
    );
    const detailHref =
      directHref !== undefined &&
      directHref !== "#" &&
      !directHref.toLowerCase().startsWith("javascript:")
        ? directHref
        : detailAction?.[1] === undefined
          ? null
          : `/PermittingPublic/PermitLandingPagePublic/Index/${detailAction[1]}`;
    if (detailHref === null) {
      throw new Error("SmartGov search result lacks a public detail identity");
    }
    const detailUrl = canonicalSourceUrl(config, detailHref);
    const pathParts = new URL(detailUrl).pathname.split("/").filter(Boolean);
    const sourceRecordId = pathParts.at(-1);
    if (
      sourceRecordId === undefined ||
      !/^[A-Z0-9-]+$/iu.test(sourceRecordId)
    ) {
      throw new Error(
        `SmartGov detail link has invalid identity: ${detailUrl}`,
      );
    }
    const resultItem = anchor.closest(".search-result-item");
    const resultColumns = resultItem.children(".row").children("div");
    const firstColumnValues = resultColumns
      .eq(0)
      .children("div")
      .toArray()
      .map((element) => readSelectionText($(element)));
    const secondColumnValues = resultColumns
      .eq(1)
      .children("div")
      .toArray()
      .map((element) => readSelectionText($(element)));
    const rowValues = mapResultRow($, anchor.closest("tr"));
    const statusDisplay = firstColumnValues[1] ?? null;
    references.push({
      sourceRecordId,
      permitNumber,
      detailUrl,
      sourcePage,
      listData: {
        address:
          rowValues.get("site address") ??
          rowValues.get("address") ??
          secondColumnValues[0] ??
          null,
        record_status:
          rowValues.get("process status") ??
          rowValues.get("status") ??
          readText(statusDisplay?.replace(/,\s*\d{1,2}\/\d{1,2}\/\d{4}$/u, "")),
        record_type:
          rowValues.get("application type") ??
          rowValues.get("type") ??
          firstColumnValues[0] ??
          null,
      },
    });
  }
  const nextPageText =
    $(`a[data-page="${String(sourcePage + 1)}"]`).attr("data-page") ??
    $("a[rel='next']").attr("data-page") ??
    ($("a[onclick*='gotoPage']")
      .toArray()
      .some((element) =>
        new RegExp(`gotoPage\\(\\s*${String(sourcePage)}\\s*\\)`, "u").test(
          $(element).attr("onclick") ?? "",
        ),
      )
      ? String(sourcePage + 1)
      : null) ??
    null;
  const resultText = readSelectionText($("#search-results")) ?? "";
  const reportedMatch = /([\d,]+)\s+results\b/iu.exec(resultText);
  const reportedCount =
    reportedMatch?.[1] === undefined
      ? null
      : Number(reportedMatch[1].replaceAll(",", ""));
  if (
    reportedCount !== null &&
    (!Number.isSafeInteger(reportedCount) || reportedCount < references.length)
  ) {
    throw new Error("SmartGov source reported an invalid result total");
  }
  return {
    references: dedupeReferences(references),
    nextPage:
      nextPageText !== null && /^\d+$/u.test(nextPageText)
        ? Number(nextPageText)
        : null,
    reportedCount,
  };
}

/**
 * Parse one public SmartGov application detail.
 *
 * @param {string} html - Official SmartGov detail HTML.
 * @param {object} context - Search identity and provenance.
 * @param {BrowardMunicipalJurisdictionConfig} context.config - SmartGov jurisdiction.
 * @param {BrowardMunicipalSearchReference} context.reference - Search reference.
 * @param {BrowardMunicipalQuery} context.query - Query that discovered the permit.
 * @returns {NormalizedBrowardMunicipalPermit} Reconciled normalized permit.
 */
export function parseSmartGovDetailHtml(html, { config, reference, query }) {
  const $ = cheerio.load(html);
  const title = requireText(
    readText($("title").text()),
    "SmartGov detail title",
  );
  if (!/Public Portal/iu.test(title)) {
    throw new Error(`Unexpected SmartGov detail title: ${title}`);
  }
  const fields = collectLabeledFields($);
  const permitNumber = requireText(
    field(fields, ["Application Number", "Permit Number", "Record Number"]),
    "SmartGov application number",
  );
  if (permitNumber !== reference.permitNumber) {
    throw new Error("SmartGov detail application identity mismatch");
  }
  const recordType =
    field(fields, ["Application Type", "Permit Type", "Type"]) ??
    listField(reference, "record_type");
  const description = field(fields, [
    "Permit Project Name",
    "Project Name",
    "Description",
    "Give your project a name",
    "Describe the purpose of the project",
  ]);
  return {
    source_system: config.sourceSystem,
    source_protocol: "smartgov",
    source_url: canonicalSourceUrl(config, reference.detailUrl),
    source_search_url: config.searchUrl,
    source_record_id: reference.sourceRecordId,
    record_key: `${config.sourceSystem}:${reference.sourceRecordId}`,
    jurisdiction: config.jurisdiction,
    permit_number: permitNumber,
    parcel_identifier: preserveMunicipalParcelIdentifier(
      field(fields, ["Parcel Number", "Parcel"]),
    ),
    query_folio: query.kind === "folio" ? query.value : null,
    work_location: field(fields, ["Site Address", "Address", "Location"]),
    application_date: parseSourceDate(
      field(fields, ["Submitted On", "Submitted", "Application Date"]),
      "SmartGov submitted date",
    ),
    permit_issue_date: parseSourceDate(
      field(fields, ["Issued On", "Issued", "Issued Date"]),
      "SmartGov issued date",
    ),
    expiration_date: parseSourceDate(
      field(fields, ["Expiration Date", "Expires", "Application Expires"]),
      "SmartGov expiration date",
    ),
    record_status:
      field(fields, ["Process Status", "Status"]) ??
      readSelectionText($(".case-header-status-badge").first()) ??
      listField(reference, "record_status"),
    record_type: recordType,
    project_description: description,
    job_value: parseSourceNumber(
      field(fields, ["Valuation", "Project Value"]),
      "SmartGov valuation",
    ),
    inspections: parseInspectionTables($),
    is_roof_permit: /\broof(?:ing)?\b/iu.test(
      `${recordType ?? ""} ${description ?? ""}`,
    ),
    raw: {
      source_page: reference.sourcePage,
      query_kind: query.kind,
      detail_contract: "anonymous_public_url",
    },
  };
}

/**
 * Parse a legacy eGovPLUS permit result table.
 *
 * Owner columns and external map links are intentionally ignored. Sort-header
 * links are not mistaken for permit details.
 *
 * @param {string} html - Official eGovPLUS result HTML.
 * @param {BrowardMunicipalJurisdictionConfig} config - Lauderhill configuration.
 * @param {object} [options] - Parser limits.
 * @param {number} [options.maxRows=50] - Hard source-row ceiling.
 * @returns {BrowardMunicipalSearchPage} Unique permit detail references.
 */
export function parseEgovPlusSearchHtml(html, config, { maxRows = 50 } = {}) {
  if (config.protocol !== "egovplus") {
    throw new Error("eGovPLUS parser received a different vendor protocol");
  }
  const $ = cheerio.load(html);
  const bodyText = readSelectionText($("body")) ?? "";
  const anchors = $('a[href*="permit_all.aspx?permit_no="]').toArray();
  if (anchors.length === 0 && /No matching records found/iu.test(bodyText)) {
    return { references: [], nextPage: null };
  }
  if (anchors.length >= maxRows) {
    throw new Error(
      `eGovPLUS result row limit ${String(maxRows)} exceeded (${String(anchors.length)})`,
    );
  }
  /** @type {BrowardMunicipalSearchReference[]} */
  const references = [];
  for (const anchorElement of anchors) {
    const anchor = $(anchorElement);
    const permitNumber = requireText(
      readSelectionText(anchor),
      "eGovPLUS permit number",
    );
    const detailUrl = canonicalSourceUrl(config, anchor.attr("href") ?? "");
    const sourceRecordId = new URL(detailUrl).searchParams.get("permit_no");
    if (
      sourceRecordId === null ||
      sourceRecordId !== permitNumber ||
      !/^[A-Z0-9-]+$/iu.test(sourceRecordId)
    ) {
      throw new Error("eGovPLUS result/detail permit identity mismatch");
    }
    const rowValues = mapResultRow($, anchor.closest("tr"));
    references.push({
      sourceRecordId,
      permitNumber,
      detailUrl,
      sourcePage: 1,
      listData: {
        address:
          rowValues.get("address click to show map") ??
          rowValues.get("address") ??
          null,
        record_status: rowValues.get("status") ?? null,
        record_type: rowValues.get("permit type") ?? null,
      },
    });
  }
  return { references: dedupeReferences(references), nextPage: null };
}

/**
 * Parse one eGovPLUS permit detail plus bounded inspection summary rows.
 *
 * Owner, applicant, contractor contact, inspector, reviewer, fee, and note
 * details are not copied. Plan-review/inspection existence remains represented
 * through allow-listed status fields and inspection rows only.
 *
 * @param {string} html - Official eGovPLUS detail HTML.
 * @param {object} context - Search identity and provenance.
 * @param {BrowardMunicipalJurisdictionConfig} context.config - Lauderhill source.
 * @param {BrowardMunicipalSearchReference} context.reference - Search reference.
 * @param {BrowardMunicipalQuery} context.query - Query that discovered the permit.
 * @returns {NormalizedBrowardMunicipalPermit} Reconciled normalized permit.
 */
export function parseEgovPlusDetailHtml(html, { config, reference, query }) {
  const $ = cheerio.load(html);
  const bodyText = readSelectionText($("body")) ?? "";
  if (!/Permit Information/iu.test(bodyText)) {
    throw new Error("Unexpected eGovPLUS permit detail response");
  }
  const fields = collectLabeledFields($);
  const permitNumber = requireText(
    field(fields, ["Permit Number"]),
    "eGovPLUS permit number",
  );
  if (permitNumber !== reference.permitNumber) {
    throw new Error("eGovPLUS detail permit identity mismatch");
  }
  const recordType = field(fields, ["Permit Type"]);
  const description = field(fields, [
    "Miscellaneous Information / Notes",
    "Description",
    "Project Description",
  ]);
  return {
    source_system: config.sourceSystem,
    source_protocol: "egovplus",
    source_url: canonicalSourceUrl(config, reference.detailUrl),
    source_search_url: config.searchUrl,
    source_record_id: reference.sourceRecordId,
    record_key: `${config.sourceSystem}:${reference.sourceRecordId}`,
    jurisdiction: config.jurisdiction,
    permit_number: permitNumber,
    parcel_identifier: preserveMunicipalParcelIdentifier(
      field(fields, ["FOLIO NBR", "Parcel ID", "Folio"]),
    ),
    query_folio: query.kind === "folio" ? query.value : null,
    work_location: field(fields, ["Property Address", "Address"]),
    application_date: parseSourceDate(
      field(fields, ["Application Date"]),
      "eGovPLUS application date",
    ),
    permit_issue_date: parseSourceDate(
      field(fields, ["Issued Date"]),
      "eGovPLUS issued date",
    ),
    expiration_date: parseSourceDate(
      field(fields, ["Expiration Date", "Expires"]),
      "eGovPLUS expiration date",
    ),
    record_status: field(fields, ["Status"]),
    record_type: recordType,
    project_description: description,
    job_value: parseSourceNumber(
      field(fields, ["Applied Value", "Valuation"]),
      "eGovPLUS applied value",
    ),
    inspections: parseInspectionTables($),
    is_roof_permit: /\broof(?:ing)?\b/iu.test(
      `${recordType ?? ""} ${description ?? ""}`,
    ),
    raw: {
      source_page: reference.sourcePage,
      query_kind: query.kind,
      detail_contract: "anonymous_public_url",
    },
  };
}

/**
 * Narrow an unknown value to a non-array source object.
 *
 * @param {unknown} value - Candidate JSON value.
 * @returns {value is Record<string, unknown>} True for plain object records.
 */
function isRecord(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    Array.isArray(value) === false
  );
}

/**
 * Find the OpenGov public-search connection across documented GraphQL envelope
 * aliases without trusting arbitrary nested source values.
 *
 * @param {unknown} payload - Parsed OpenGov GraphQL response.
 * @returns {{ edges: readonly unknown[], pageInfo: Record<string, unknown> }} Validated search connection.
 */
function readOpenGovConnection(payload) {
  if (!isRecord(payload) || !isRecord(payload.data)) {
    throw new Error("OpenGov search returned an invalid GraphQL envelope");
  }
  const direct = payload.data.searchRecords;
  const nested = isRecord(payload.data.search)
    ? payload.data.search.records
    : null;
  const connection = isRecord(direct)
    ? direct
    : isRecord(nested)
      ? nested
      : null;
  if (
    connection === null ||
    !Array.isArray(connection.edges) ||
    !isRecord(connection.pageInfo)
  ) {
    throw new Error("OpenGov search response lacks a records connection");
  }
  return { edges: connection.edges, pageInfo: connection.pageInfo };
}

/**
 * Parse an OpenGov/ViewPoint public GraphQL search page.
 *
 * This parser is fixture-certified only while the Lauderdale Lakes rendered
 * application reports itself inaccessible. It never initiates a GraphQL call;
 * cursor output is available for a future healthy anonymous transport.
 *
 * @param {unknown} payload - Parsed official GraphQL search response.
 * @param {BrowardMunicipalJurisdictionConfig} config - OpenGov jurisdiction.
 * @param {object} context - Page provenance.
 * @param {number} context.sourcePage - One-based page ordinal.
 * @param {number} [context.maxRows=50] - Hard edge ceiling.
 * @returns {BrowardMunicipalSearchPage} Detail references and opaque cursor.
 */
export function parseOpenGovSearchPayload(
  payload,
  config,
  { sourcePage, maxRows = 50 },
) {
  if (config.protocol !== "opengov") {
    throw new Error("OpenGov parser received a different vendor protocol");
  }
  const connection = readOpenGovConnection(payload);
  if (connection.edges.length > maxRows) {
    throw new Error(
      `OpenGov result edge limit ${String(maxRows)} exceeded (${String(connection.edges.length)})`,
    );
  }
  /** @type {BrowardMunicipalSearchReference[]} */
  const references = [];
  for (const edge of connection.edges) {
    if (!isRecord(edge) || !isRecord(edge.node)) {
      throw new Error("OpenGov search edge is malformed");
    }
    const node = edge.node;
    const sourceRecordId = requireText(readText(node.id), "OpenGov record id");
    const permitNumber = requireText(
      readText(node.recordNumber) ??
        readText(node.permitNumber) ??
        readText(node.number),
      "OpenGov record number",
    );
    const publicUrl = requireText(
      readText(node.publicUrl),
      "OpenGov public detail URL",
    );
    references.push({
      sourceRecordId,
      permitNumber,
      detailUrl: canonicalSourceUrl(config, publicUrl),
      sourcePage,
      listData: {
        address: readText(node.address),
        record_status: readText(node.status),
        record_type: readText(node.recordType) ?? readText(node.type),
      },
    });
  }
  const hasNextPage = connection.pageInfo.hasNextPage;
  if (typeof hasNextPage !== "boolean") {
    throw new Error("OpenGov pageInfo.hasNextPage must be boolean");
  }
  const endCursor = readText(connection.pageInfo.endCursor);
  if (hasNextPage && endCursor === null) {
    throw new Error("OpenGov pageInfo lacks a required end cursor");
  }
  return {
    references: dedupeReferences(references),
    nextPage: hasNextPage ? endCursor : null,
  };
}

/**
 * Normalize one OpenGov public detail payload.
 *
 * @param {unknown} payload - Parsed record detail JSON.
 * @param {object} context - Search identity and provenance.
 * @param {BrowardMunicipalJurisdictionConfig} context.config - OpenGov jurisdiction.
 * @param {BrowardMunicipalSearchReference} context.reference - Search reference.
 * @param {BrowardMunicipalQuery} context.query - Query that discovered the record.
 * @returns {NormalizedBrowardMunicipalPermit} Reconciled normalized permit.
 */
export function normalizeOpenGovDetailPayload(
  payload,
  { config, reference, query },
) {
  if (!isRecord(payload)) {
    throw new Error("OpenGov detail payload must be an object");
  }
  const record =
    isRecord(payload.data) && isRecord(payload.data.record)
      ? payload.data.record
      : isRecord(payload.record)
        ? payload.record
        : payload;
  const sourceRecordId = requireText(readText(record.id), "OpenGov record id");
  const permitNumber = requireText(
    readText(record.recordNumber) ??
      readText(record.permitNumber) ??
      readText(record.number),
    "OpenGov record number",
  );
  if (
    sourceRecordId !== reference.sourceRecordId ||
    permitNumber !== reference.permitNumber
  ) {
    throw new Error("OpenGov detail record identity mismatch");
  }
  const recordType = readText(record.recordType) ?? readText(record.type);
  const description =
    readText(record.description) ?? readText(record.projectDescription);
  return {
    source_system: config.sourceSystem,
    source_protocol: "opengov",
    source_url: reference.detailUrl,
    source_search_url: config.searchUrl,
    source_record_id: sourceRecordId,
    record_key: `${config.sourceSystem}:${sourceRecordId}`,
    jurisdiction: config.jurisdiction,
    permit_number: permitNumber,
    parcel_identifier: preserveMunicipalParcelIdentifier(
      record.parcelNumber ?? record.folio,
    ),
    query_folio: query.kind === "folio" ? query.value : null,
    work_location: readText(record.address),
    application_date: parseSourceDate(
      readText(record.applicationDate),
      "OpenGov application date",
    ),
    permit_issue_date: parseSourceDate(
      readText(record.issueDate),
      "OpenGov issue date",
    ),
    expiration_date: parseSourceDate(
      readText(record.expirationDate),
      "OpenGov expiration date",
    ),
    record_status: readText(record.status),
    record_type: recordType,
    project_description: description,
    job_value: parseSourceNumber(
      readText(record.valuation),
      "OpenGov valuation",
    ),
    inspections: [],
    is_roof_permit: /\broof(?:ing)?\b/iu.test(
      `${recordType ?? ""} ${description ?? ""}`,
    ),
    raw: {
      source_page: reference.sourcePage,
      query_kind: query.kind,
      detail_contract: "anonymous_public_url_fixture_only",
    },
  };
}
