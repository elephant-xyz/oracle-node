#!/usr/bin/env node

import { createReadStream, createWriteStream } from "node:fs";
import { createRequire } from "node:module";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

const require = createRequire(import.meta.url);
const duckdb = require("duckdb");

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * Narrow an unknown value to a JSON object.
 *
 * @param {unknown} value Candidate value.
 * @returns {value is JsonObject} Whether the value is a non-array object.
 */
function isJsonObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * @typedef {"official_bulk" | "accela" | "ims" | "tyler_esuite" | "iworq" | "municipal_portal" | "none_verified"} PolkPermitPortalKind
 */

/**
 * @typedef {"bulk_only" | "adapter_ready" | "partial_adapter_ready" | "portal_verified_adapter_pending" | "no_public_detail_source_verified"} PolkPermitSourceStatus
 */

/**
 * @typedef {object} PolkPermitSource
 * @property {string} key Stable source key.
 * @property {string} agency Exact agency label from the official Polk bulk permit file.
 * @property {PolkPermitPortalKind} portalKind Portal/vendor classification.
 * @property {PolkPermitSourceStatus} status Evidence-backed automation status.
 * @property {string | null} officialUrl Official agency or portal URL.
 * @property {string | null} searchUrl Public search URL when verified.
 * @property {string | null} adapter Adapter key only when its public request protocol is verified.
 * @property {string} evidence Concise source-discovery evidence.
 * @property {string} verifiedAt ISO date when the source was checked.
 */

/**
 * Truthful registry for agencies present in Polk's official bulk CAMA permit
 * projection. A portal URL is not enough to enable an adapter: `adapter` remains
 * null until anonymous search/detail requests have been verified.
 *
 * @type {readonly PolkPermitSource[]}
 */
export const POLK_PERMIT_SOURCE_REGISTRY = Object.freeze([
  {
    key: "polk_property_appraiser_bulk",
    agency: "POLK COUNTY PROPERTY APPRAISER",
    portalKind: "official_bulk",
    status: "bulk_only",
    officialUrl: "https://www.polkpa.org/",
    searchUrl: null,
    adapter: null,
    evidence:
      "Official ftp_permit bulk projection supplies permit facts but no contractor, licence, inspection, or detail URL fields.",
    verifiedAt: "2026-08-31",
  },
  {
    key: "polk_county_accela",
    agency: "POLK COUNTY",
    portalKind: "accela",
    status: "adapter_ready",
    officialUrl: "https://www.polkfl.gov/services/building/permitting/",
    searchUrl:
      "https://aca-prod.accela.com/POLKCO/Cap/CapHome.aspx?module=Building&TabName=Building",
    adapter: "polk_accela_cap_detail_v1",
    evidence:
      "The official county page links Accela; anonymous CapDetail lookup by altId was verified to expose record status, parcel, contractor/licence, job value, and project description.",
    verifiedAt: "2026-08-31",
  },
  {
    key: "lakeland_ims",
    agency: "LAKELAND",
    portalKind: "ims",
    status: "adapter_ready",
    officialUrl:
      "https://www.lakelandgov.net/departments/community-economic-development/building-inspection/ims/",
    searchUrl: "https://ims.lakelandgov.net/ims/Find3?cat=Permits",
    adapter: "lakeland_ims_permit_detail_v1",
    evidence:
      "Anonymous guest search was certified with the iMS antiforgery token and redirect sequence; public permit details expose trade contractors and Florida licence identifiers.",
    verifiedAt: "2026-08-31",
  },
  {
    key: "winter_haven_tyler_esuite",
    agency: "WINTER HAVEN",
    portalKind: "tyler_esuite",
    status: "partial_adapter_ready",
    officialUrl: "https://www.mywinterhaven.com/342/Building-Permits-Licenses",
    searchUrl:
      "https://myinspections.mywinterhaven.com/eSuite.Permits/AdvancedSearchPage/AdvancedSearch.aspx",
    adapter: "winter_haven_esuite_permit_detail_v1",
    evidence:
      "Anonymous eSuite search and session-scoped detail retrieval are certified for 2025-and-earlier numeric permits. Current WH26-prefixed bulk permits are not indexed there, so countywide Winter Haven coverage remains unsupported.",
    verifiedAt: "2026-08-31",
  },
  {
    key: "haines_city_iworq",
    agency: "HAINES CITY",
    portalKind: "iworq",
    status: "portal_verified_adapter_pending",
    officialUrl: "https://hainescity.com/155/Development-Services-Department",
    searchUrl: "https://haines.portal.iworq.net/portalhome/haines",
    adapter: null,
    evidence:
      "The public iWorQ search enforces invisible reCAPTCHA. Missing or invalid tokens return no result rows, so unattended detail access is not certified.",
    verifiedAt: "2026-08-31",
  },
  {
    key: "lake_wales_public_view",
    agency: "LAKE WALES",
    portalKind: "municipal_portal",
    status: "adapter_ready",
    officialUrl: "https://www.lakewalesfl.gov/909/Contractor-Online-Portal",
    searchUrl: "https://secure.lakewalesfl.gov/permits/",
    adapter: "lake_wales_citizenlink_permit_detail_v1",
    evidence:
      "Anonymous CitizenLink bootstrap, permit-number lookup, and detail requests were certified; permit details expose municipal contractor identity and class but not a Florida state licence number.",
    verifiedAt: "2026-08-31",
  },
  ...[
    "AUBURNDALE",
    "BARTOW",
    "DAVENPORT",
    "DUNDEE",
    "EAGLE LAKE",
    "FORT MEADE",
    "FROSTPROOF",
    "LAKE ALFRED",
    "LAKE HAMILTON",
    "MULBERRY",
    "POLK CITY",
  ].map(
    /**
     * @param {string} agency Official bulk agency label.
     * @returns {PolkPermitSource} Explicit unavailable-source registry row.
     */
    (agency) => ({
      key: agency.toLowerCase().replaceAll(" ", "_"),
      agency,
      portalKind: "none_verified",
      status: "no_public_detail_source_verified",
      officialUrl: null,
      searchUrl: null,
      adapter: null,
      evidence:
        "The agency appears in the official Polk bulk permit projection, but no anonymous detail source and request contract are certified in this registry.",
      verifiedAt: "2026-08-31",
    }),
  ),
]);

/**
 * @typedef {object} PolkPermitCandidateOptions
 * @property {string} workDatabase Completed Polk DuckDB cache.
 * @property {string} output JSONL destination.
 * @property {readonly string[]} agencies Official agency labels to include.
 * @property {number | null} limit Optional deterministic pilot cap.
 */

/**
 * Escape text as a DuckDB SQL string literal.
 *
 * @param {string} value Untrusted text.
 * @returns {string} Escaped SQL literal.
 */
function duckdbStringLiteral(value) {
  return `'${value.replaceAll("'", "''")}'`;
}

/**
 * Build the read-only query for permit adapter candidates.
 *
 * Candidates intentionally preserve one row per official bulk permit row.
 * Duplicate permit numbers can therefore be reconciled to the source row
 * denominator instead of silently collapsing parcel-level evidence.
 *
 * @param {readonly string[]} agencies Official agency labels.
 * @param {number | null} limit Optional pilot cap.
 * @returns {string} Read-only DuckDB SQL.
 */
export function buildPolkPermitCandidateSql(agencies, limit) {
  const normalizedAgencies = [
    ...new Set(
      agencies
        .map((agency) => agency.trim().toUpperCase())
        .filter((agency) => agency.length > 0),
    ),
  ].sort();
  if (normalizedAgencies.length === 0) {
    throw new Error("At least one Polk permit agency is required");
  }
  if (limit !== null && (!Number.isSafeInteger(limit) || limit < 1)) {
    throw new Error("Polk permit candidate limit must be a positive integer");
  }
  const agencySql = normalizedAgencies.map(duckdbStringLiteral).join(", ");
  return `
    SELECT
      trim(permit_number) AS permitNumber,
      upper(trim(agency_name)) AS agency
    FROM polk_permits
    WHERE permit_number IS NOT NULL
      AND trim(permit_number) <> ''
      AND agency_name IS NOT NULL
      AND upper(trim(agency_name)) IN (${agencySql})
    ORDER BY
      CASE
        WHEN try_cast(substr(issue_date, 1, 10) AS DATE)
          BETWEEN DATE '1901-01-01' AND CURRENT_DATE + INTERVAL '2 years'
          THEN try_cast(substr(issue_date, 1, 10) AS DATE)
        ELSE NULL
      END DESC NULLS LAST,
      upper(trim(agency_name)),
      trim(permit_number)
    ${limit === null ? "" : `LIMIT ${limit}`}
  `;
}

/**
 * Execute one read-only DuckDB query.
 *
 * @param {import("duckdb").Connection} connection Open connection.
 * @param {string} sql Read-only SQL.
 * @returns {Promise<JsonObject[]>} Query rows.
 */
function queryDuckDb(connection, sql) {
  return new Promise((resolve, reject) => {
    connection.all(sql, (error, rows) => {
      if (error !== null) {
        reject(error instanceof Error ? error : new Error(String(error)));
        return;
      }
      resolve(Array.isArray(rows) ? rows : []);
    });
  });
}

/**
 * Close a DuckDB connection.
 *
 * @param {import("duckdb").Connection} connection Open connection.
 * @returns {Promise<void>} Resolves when closed.
 */
function closeDuckDbConnection(connection) {
  return new Promise((resolve) => {
    connection.close(() => resolve());
  });
}

/**
 * Materialize deterministic permit adapter candidates from the official bulk
 * cache without network access.
 *
 * @param {PolkPermitCandidateOptions} options Candidate options.
 * @returns {Promise<JsonObject>} Candidate manifest.
 */
export async function writePolkPermitAdapterCandidates(options) {
  const absoluteDatabase = path.resolve(options.workDatabase);
  const absoluteOutput = path.resolve(options.output);
  await mkdir(path.dirname(absoluteOutput), { recursive: true });
  const database = new duckdb.Database(absoluteDatabase, {
    access_mode: "READ_ONLY",
  });
  const connection = database.connect();
  let rows;
  try {
    rows = await queryDuckDb(
      connection,
      buildPolkPermitCandidateSql(options.agencies, options.limit),
    );
  } finally {
    await closeDuckDbConnection(connection);
  }
  const candidates = rows.flatMap((row) => {
    const permitNumber =
      typeof row.permitNumber === "string" ? row.permitNumber.trim() : "";
    const agency = typeof row.agency === "string" ? row.agency.trim() : "";
    return permitNumber.length > 0 && agency.length > 0
      ? [{ permitNumber, agency }]
      : [];
  });
  await writeFile(
    absoluteOutput,
    candidates.map((candidate) => JSON.stringify(candidate)).join("\n") +
      (candidates.length > 0 ? "\n" : ""),
    "utf8",
  );
  return {
    schemaVersion: "oracle-node.polk-permit-adapter-candidates.v1",
    generatedAt: new Date().toISOString(),
    workDatabase: absoluteDatabase,
    output: absoluteOutput,
    agencies: [...options.agencies],
    requestedLimit: options.limit,
    candidateCount: candidates.length,
    complete: candidates.length > 0,
  };
}

/**
 * @typedef {object} PolkAccelaPermitDetail
 * @property {string | null} permitNumber Permit number from the detail heading.
 * @property {string | null} recordType Accela record type.
 * @property {string | null} recordStatus Current record status.
 * @property {string | null} parcelIdentifier Parcel number.
 * @property {string | null} workLocation Work-location text.
 * @property {string | null} projectDescription Project description.
 * @property {number | null} jobValuationUsd Numeric job value.
 * @property {{businessName:string|null,contactName:string|null,licenseNumber:string|null,licenseType:string|null,email:string|null,phone:string|null,raw:string}|null} contractor Licensed-professional evidence.
 */

/**
 * @typedef {object} PolkPermitEnrichmentRecord
 * @property {string} permitNumber Permit identifier requested.
 * @property {string} agency Official bulk agency label.
 * @property {string} sourceKey Registry source key.
 * @property {string | null} sourceUrl Detail URL.
 * @property {"enriched" | "no_detail" | "unsupported_source" | "fetch_error"} status Outcome.
 * @property {PolkAccelaPermitDetail | null} detail Parsed detail evidence.
 * @property {string | null} error Failure detail.
 * @property {string} retrievedAt ISO retrieval timestamp.
 */

/**
 * Resolve an official bulk agency label to its source registry row.
 *
 * @param {unknown} agency Candidate agency value.
 * @returns {PolkPermitSource | null} Matching registry row or null.
 */
export function findPolkPermitSource(agency) {
  if (typeof agency !== "string") return null;
  const normalized = agency.trim().toUpperCase();
  return (
    POLK_PERMIT_SOURCE_REGISTRY.find(
      (source) => source.agency === normalized,
    ) ?? null
  );
}

/**
 * Build the certified anonymous Polk County Accela detail URL.
 *
 * @param {string} permitNumber Official permit number.
 * @returns {string} Accela CapDetail URL using the verified `altId` lookup.
 */
export function buildPolkAccelaDetailUrl(permitNumber) {
  const normalized = permitNumber.trim();
  if (normalized.length === 0) {
    throw new Error("Polk Accela permit number is required");
  }
  const url = new URL("https://aca-prod.accela.com/POLKCO/Cap/CapDetail.aspx");
  url.searchParams.set("Module", "Building");
  url.searchParams.set("TabName", "Building");
  url.searchParams.set("altId", normalized);
  return url.toString();
}

/**
 * Decode the small HTML entity set needed by public permit labels.
 *
 * @param {string} value Raw HTML-derived text.
 * @returns {string} Decoded text.
 */
function decodeHtmlEntities(value) {
  return value
    .replaceAll("&nbsp;", " ")
    .replaceAll("&amp;", "&")
    .replaceAll("&#39;", "'")
    .replaceAll("&quot;", '"')
    .replaceAll("&lt;", "<")
    .replaceAll("&gt;", ">");
}

/**
 * Convert source HTML into stable visible text without a DOM dependency.
 *
 * @param {string} html Raw public portal HTML.
 * @returns {string} Whitespace-normalized visible text.
 */
export function permitHtmlToText(html) {
  return decodeHtmlEntities(
    html
      .replace(/<script\b[\s\S]*?<\/script>/gi, " ")
      .replace(/<style\b[\s\S]*?<\/style>/gi, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim(),
  );
}

/**
 * Return the first trimmed capture from a pattern.
 *
 * @param {string} text Source text.
 * @param {RegExp} pattern Pattern with one capture group.
 * @returns {string | null} Captured text or null.
 */
function firstCapture(text, pattern) {
  const value = pattern.exec(text)?.[1]?.replace(/\s+/g, " ").trim() ?? "";
  return value.length > 0 ? value : null;
}

/**
 * Escape user-derived text for an exact regular-expression segment.
 *
 * @param {string} value Literal text.
 * @returns {string} Escaped pattern source.
 */
function escapeRegularExpression(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Normalize a phone number to its final ten digits.
 *
 * @param {string | null} value Raw phone.
 * @returns {string | null} Ten-digit phone or null.
 */
function normalizePhone(value) {
  if (value === null) return null;
  const digits = value.replace(/\D/g, "");
  return digits.length >= 10 ? digits.slice(-10) : null;
}

/**
 * Extract a Florida contractor licence and its visible classification.
 *
 * Accela places fax numbers and numeric address fragments in the same flattened
 * text as licence identifiers. Prefer tokens immediately following an observed
 * contractor classification, then fall back to a prefix-filtered token scan for
 * older records that omit the classification.
 *
 * @param {string} contractorRaw Flattened licensed-professional text.
 * @returns {{licenseNumber:string|null,licenseToken:string|null,licenseType:string|null}} Parsed licence evidence.
 */
function extractContractorLicenseEvidence(contractorRaw) {
  const typedMatch =
    /\b(General|Building|Residential|Roofing|Plumbing(?:\/Gas)?|Air Condition Class [AB]|Mechanical(?:\/Hood)?|Electric With Alarm|Solar|Private Provider|Irrigation|Alum Specialty Structure)\s+([A-Z]{2,4}\s*[: -]?\s*\d{4,12})\b/i.exec(
      contractorRaw,
    );
  if (typedMatch?.[1] !== undefined && typedMatch[2] !== undefined) {
    return {
      licenseNumber: typedMatch[2].toUpperCase().replace(/[^A-Z0-9]/g, ""),
      licenseToken: typedMatch[2],
      licenseType: typedMatch[1],
    };
  }
  const rejectedPrefixes = new Set(["CORP", "FAX", "INC", "LLC", "TEL"]);
  const tokenMatch = [
    ...contractorRaw.matchAll(/\b([A-Z]{2,4}\s*[: -]?\s*\d{5,10})\b/gi),
  ].find((match) => {
    const token = match[1];
    if (token === undefined) return false;
    const prefix = token.replace(/[^A-Z]/gi, "").toUpperCase();
    return !rejectedPrefixes.has(prefix);
  });
  const licenseToken = tokenMatch?.[1] ?? null;
  return {
    licenseNumber:
      licenseToken === null
        ? null
        : licenseToken.toUpperCase().replace(/[^A-Z0-9]/g, ""),
    licenseToken,
    licenseType:
      licenseToken === null
        ? null
        : firstCapture(
            contractorRaw,
            new RegExp(
              `([A-Za-z][A-Za-z ]{2,50})\\s+${escapeRegularExpression(licenseToken)}\\b`,
              "i",
            ),
          ),
  };
}

/**
 * Parse an anonymous Polk County Accela CapDetail page.
 *
 * The parser promotes only labels observed on the certified public detail page.
 * Missing labels remain null; no fallback values are invented.
 *
 * @param {string} html Raw Accela HTML.
 * @returns {PolkAccelaPermitDetail} Parsed detail evidence.
 */
export function parsePolkAccelaPermitDetailHtml(html) {
  const text = permitHtmlToText(html);
  const permitNumber = firstCapture(text, /\bRecord\s+([^:]+):/i);
  const recordType = firstCapture(
    text,
    /\bRecord\s+[^:]+:\s*(.+?)\s+Record Status:/i,
  );
  const recordStatus = firstCapture(
    text,
    /\bRecord Status:\s*(.+?)(?:\s+Record Info|\s+Instructions:|\s+Work Location\b)/i,
  );
  const parcelIdentifier =
    firstCapture(text, /\bParcel Number:\s*([A-Z0-9-]+)/i)?.replace(
      /\D/g,
      "",
    ) ?? null;
  const workLocation = firstCapture(
    text,
    /\bWork Location\s+(.+?)\s+Record Details\b/i,
  );
  const projectDescription = firstCapture(
    text,
    /\bProject Description:\s*(.+?)(?:\s+Owner:|\s+Additional Information\b)/i,
  );
  const valuationText = firstCapture(
    text,
    /\bJob Value\(\$\):\s*\$?([\d,]+(?:\.\d{1,2})?)/i,
  );
  const jobValuationUsd =
    valuationText === null ? null : Number(valuationText.replaceAll(",", ""));
  const contractorRaw = firstCapture(
    text,
    /\bLicensed Professional:\s*(.+?)(?:\s+Project Description:|\s+Owner:|\s+Additional Information\b)/i,
  );
  let contractor = null;
  if (contractorRaw !== null) {
    const primaryContractorRaw =
      contractorRaw.split(/\bView Additional Licensed Professionals\b/i)[0] ??
      contractorRaw;
    const { licenseNumber, licenseType } =
      extractContractorLicenseEvidence(primaryContractorRaw);
    const email =
      firstCapture(
        primaryContractorRaw,
        /\b([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})\b/i,
      )?.toLowerCase() ?? null;
    const phone = normalizePhone(
      firstCapture(
        primaryContractorRaw,
        /(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})/,
      ),
    );
    const businessSearchText =
      email === null
        ? primaryContractorRaw
        : primaryContractorRaw.slice(
            primaryContractorRaw.toLowerCase().indexOf(email) + email.length,
          );
    const businessName =
      firstCapture(
        businessSearchText,
        /\b([A-Z][A-Z0-9 &'.,-]+?(?:LLC|INC|CORP(?:ORATION)?|COMPANY|CO))\b/,
      ) ??
      firstCapture(businessSearchText, /^(.{2,120}?)\s+\d{2,6}\s+[A-Z0-9]/i) ??
      firstCapture(
        businessSearchText,
        /\b([A-Z0-9][A-Z0-9 &'.,-]+?(?:CONSTRUCTION|CONTRACTING|ROOFING|SERVICES))\b/i,
      );
    const contactName =
      businessName === null
        ? firstCapture(
            primaryContractorRaw,
            /^([A-Z][A-Z .'-]{2,60}?)(?:\s+[A-Z0-9._%+-]+@|\s+[A-Z]{2,4}\d{5})/i,
          )
        : firstCapture(
            primaryContractorRaw,
            /^([A-Z][A-Z .'-]{2,60}?)\s+[A-Z0-9._%+-]+@/i,
          );
    contractor = {
      businessName,
      contactName,
      licenseNumber,
      licenseType,
      email,
      phone,
      raw: contractorRaw,
    };
  }
  return {
    permitNumber,
    recordType,
    recordStatus,
    parcelIdentifier,
    workLocation,
    projectDescription,
    jobValuationUsd:
      jobValuationUsd !== null && Number.isFinite(jobValuationUsd)
        ? jobValuationUsd
        : null,
    contractor,
  };
}

/**
 * Fetch one certified Polk Accela permit detail page.
 *
 * @param {string} permitNumber Permit number.
 * @param {typeof fetch} [fetchImplementation] Injectable fetch for tests.
 * @returns {Promise<{url:string,html:string}>} Successful public response.
 */
export async function fetchPolkAccelaPermitDetail(
  permitNumber,
  fetchImplementation = fetch,
) {
  const url = buildPolkAccelaDetailUrl(permitNumber);
  const response = await fetchImplementation(url, {
    headers: {
      Accept: "text/html,application/xhtml+xml",
      "User-Agent": "oracle-node-polk-permit-evidence/1.0",
    },
  });
  if (!response.ok) {
    throw new Error(`Polk Accela detail returned HTTP ${response.status}`);
  }
  return { url, html: await response.text() };
}

/**
 * Build the certified anonymous Winter Haven eSuite search URL.
 *
 * @param {string} permitNumber Official permit number.
 * @returns {string} Public advanced-search URL.
 */
export function buildWinterHavenPermitSearchUrl(permitNumber) {
  const normalized = permitNumber.trim();
  if (normalized.length === 0) {
    throw new Error("Winter Haven permit number is required");
  }
  const url = new URL(
    "https://myinspections.mywinterhaven.com/eSuite.Permits/AdvancedSearchPage/AdvancedSearch.aspx",
  );
  url.searchParams.set("permitNumber", normalized);
  url.searchParams.set("permitType", "-1");
  url.searchParams.set("serviceAddress", "");
  return url.toString();
}

/**
 * Parse Lakeland's public iMS permit page into the shared detail contract.
 *
 * @param {string} html Public permit detail HTML.
 * @param {string} requestedPermitNumber Permit identifier used for lookup.
 * @returns {PolkAccelaPermitDetail} Parsed public evidence.
 */
export function parseLakelandImsPermitDetailHtml(html, requestedPermitNumber) {
  const text = permitHtmlToText(html);
  const normalizedPermit = requestedPermitNumber.trim();
  const permitNumber =
    normalizedPermit.length > 0
      ? normalizedPermit
      : firstCapture(text, /\bPermit(?: Number)?:\s*([A-Z0-9-]+)/i);
  const headingIndex = text
    .toUpperCase()
    .lastIndexOf(normalizedPermit.toUpperCase());
  const detailHeadingText = headingIndex < 0 ? text : text.slice(headingIndex);
  const permitHeadingPattern = new RegExp(
    `^${escapeRegularExpression(normalizedPermit)}\\s+(.+?)\\s+(?:Complete|Issued|Closed|Active|Pending)\\b`,
    "i",
  );
  const permitStatusPattern = new RegExp(
    `^${escapeRegularExpression(normalizedPermit)}\\s+.+?\\s+(Complete|Issued|Closed|Active|Pending)\\b`,
    "i",
  );
  const contractorMatch =
    /\b(?:Building Contractor|Electrical|Plumbing(?:\/Gas)?|Mechanical(?:\/Hood)?):?\s+([A-Z0-9][A-Z0-9 &'.,-]{2,100}?)\s*\(([A-Z]{2,4}\d{5,10})\)/i.exec(
      text,
    );
  const contractor =
    contractorMatch?.[1] === undefined || contractorMatch[2] === undefined
      ? null
      : {
          businessName: contractorMatch[1].trim(),
          contactName: null,
          licenseNumber: contractorMatch[2].toUpperCase(),
          licenseType: null,
          email: null,
          phone: null,
          raw: contractorMatch[0],
        };
  return {
    permitNumber,
    recordType:
      firstCapture(
        text,
        /\bType:\s*(.+?)(?:\s+Status:|\s+Address:|\s+Location:)/i,
      ) ?? firstCapture(detailHeadingText, permitHeadingPattern),
    recordStatus:
      firstCapture(
        text,
        /\bStatus:\s*(.+?)(?:\s+Type:|\s+Address:|\s+Location:|\s+Description:)/i,
      ) ?? firstCapture(detailHeadingText, permitStatusPattern),
    parcelIdentifier:
      firstCapture(text, /\b(?:Parcel|Folio)(?: Number)?:\s*([A-Z0-9-]+)/i)
        ?.replace(/[^A-Z0-9]/gi, "")
        .toUpperCase() ?? null,
    workLocation:
      firstCapture(
        text,
        /\bLocation & Permit Description\s+Location\s+(.+?)\s+Job Description\b/i,
      ) ??
      firstCapture(
        text,
        /\b(?:Address|Location):\s*(.+?)(?:\s+Description:|\s+Status:|\s+Contractors?\b)/i,
      ),
    projectDescription:
      firstCapture(text, /\bPermit Scope\s+(.+?)\s+Charges\b/i) ??
      firstCapture(
        text,
        /\bDescription:\s*(.+?)(?:\s+Valuation:|\s+Contractors?\b|\s+Fees?\b)/i,
      ),
    jobValuationUsd: parseCurrencyCapture(
      text,
      /\b(?:Job Value|Valuation|Estimated Value):?\s*\$?([\d,]+(?:\.\d{1,2})?)/i,
    ),
    contractor,
  };
}

/**
 * Parse Winter Haven's public eSuite permit page.
 *
 * Contractor fields are intentionally null because the certified public detail
 * leaves those fields blank even for records issued to a contractor.
 *
 * @param {string} html Public detail HTML.
 * @param {string} requestedPermitNumber Permit identifier used for lookup.
 * @returns {PolkAccelaPermitDetail} Parsed public metadata evidence.
 */
export function parseWinterHavenPermitDetailHtml(html, requestedPermitNumber) {
  const text = permitHtmlToText(html);
  const normalizedPermit = requestedPermitNumber.trim();
  return {
    permitNumber:
      normalizedPermit.length > 0
        ? normalizedPermit
        : firstCapture(text, /\bPermit(?: Number)?:\s*([A-Z0-9-]+)/i),
    recordType:
      firstCapture(text, /\bPermit Type\s+(.+?)\s+Permit #/i) ??
      firstCapture(
        text,
        /\bPermit Type:\s*(.+?)(?:\s+Status:|\s+Permit Status:|\s+Address:)/i,
      ),
    recordStatus:
      firstCapture(text, /\bStatus\s+(.+?)\s+Issued To\b/i) ??
      firstCapture(
        text,
        /\b(?:Permit )?Status:\s*(.+?)(?:\s+Issued To:|\s+Address:|\s+Description:)/i,
      ),
    parcelIdentifier:
      firstCapture(text, /\b(?:Parcel|Folio)(?: Number)?:\s*([A-Z0-9-]+)/i)
        ?.replace(/[^A-Z0-9]/gi, "")
        .toUpperCase() ?? null,
    workLocation:
      firstCapture(
        text,
        /\bPrimary Owner Address\s+(.+?)\s+Parcel Description\b/i,
      ) ??
      firstCapture(
        text,
        /\b(?:Address|Service Address):\s*(.+?)(?:\s+Description:|\s+Valuation:|\s+Permit Type:)/i,
      ),
    projectDescription:
      firstCapture(
        text,
        /\bPermit Details Description\s+(.+?)\s+Current Property Value\b/i,
      ) ??
      firstCapture(
        text,
        /\bDescription:\s*(.+?)(?:\s+Valuation:|\s+Fees?:|\s+Expiration:)/i,
      ),
    jobValuationUsd: parseCurrencyCapture(
      text,
      /\b(?:Est\. Improvement Value|Valuation):?\s*\$?([\d,]+(?:\.\d{1,2})?)/i,
    ),
    contractor: null,
  };
}

/**
 * Parse a Lake Wales CitizenLink permit detail response.
 *
 * CitizenLink exposes a municipal contractor number and classification but no
 * Florida state licence identifier, so `licenseNumber` remains null.
 *
 * @param {string} html Decoded CitizenLink detail body.
 * @param {string} requestedPermitNumber Permit identifier used for lookup.
 * @returns {PolkAccelaPermitDetail} Parsed public detail evidence.
 */
export function parseLakeWalesPermitDetailHtml(html, requestedPermitNumber) {
  const text = permitHtmlToText(html);
  const contractorMatch =
    /\bGeneral Contractor:\s*([0-9]+)\s*\/\s*(.+?)(?:\s+Receipt Date:|\s+Status:|\s+Class:|\s+Address:|\s+Contacts?\b)/i.exec(
      text,
    );
  const contractor =
    contractorMatch?.[1] === undefined || contractorMatch[2] === undefined
      ? null
      : {
          businessName: contractorMatch[2].trim(),
          contactName: null,
          licenseNumber: null,
          licenseType: firstCapture(
            text,
            /\b(?:License )?Class:\s*(.+?)(?:\s+Status:|\s+Expiration:|\s+Address:)/i,
          ),
          email: null,
          phone: null,
          raw: contractorMatch[0],
        };
  const normalizedPermit = requestedPermitNumber.trim();
  return {
    permitNumber:
      normalizedPermit.length > 0
        ? normalizedPermit
        : firstCapture(text, /\bPermit(?: Number)?:\s*([A-Z0-9-]+)/i),
    recordType: firstCapture(
      text,
      /\b(?:Permit )?Type:\s*(.+?)(?:\s+Status:|\s+Address:|\s+Description:)/i,
    ),
    recordStatus:
      firstCapture(text, /\bPermit Status:\s*(.+?)\s+Closed Date:/i) ??
      firstCapture(
        text,
        /\bStatus:\s*(.+?)(?:\s+Address:|\s+Description:|\s+Issued:)/i,
      ),
    parcelIdentifier:
      firstCapture(text, /\b(?:Parcel|Folio)(?: Number)?:\s*([A-Z0-9-]+)/i)
        ?.replace(/[^A-Z0-9]/gi, "")
        .toUpperCase() ?? null,
    workLocation: firstCapture(
      text,
      /\b(?:Address|Location):\s*(.+?)(?:\s+Description:|\s+Status:|\s+General Contractor:)/i,
    ),
    projectDescription:
      firstCapture(text, /\bDescription:\s*(.+?)\s+Address:/i) ??
      firstCapture(
        text,
        /\bDescription:\s*(.+?)(?:\s+Valuation:|\s+General Contractor:|\s+Fees?\b)/i,
      ),
    jobValuationUsd: parseCurrencyCapture(
      text,
      /\b(?:Valuation|Estimated Value):\s*\$?([\d,]+(?:\.\d{1,2})?)/i,
    ),
    contractor,
  };
}

/**
 * Fetch a Lakeland permit through its anonymous iMS redirect sequence.
 *
 * @param {string} permitNumber Official permit number.
 * @param {typeof fetch} [fetchImplementation] Injectable fetch for tests.
 * @returns {Promise<{url:string,html:string}>} Public detail response.
 */
export async function fetchLakelandImsPermitDetail(
  permitNumber,
  fetchImplementation = fetch,
) {
  const normalized = permitNumber.trim();
  if (normalized.length === 0)
    throw new Error("Lakeland permit number is required");
  const cookies = new Map();
  let response = await fetchWithCookies(
    "https://ims.lakelandgov.net/ims/Account/Anonymous",
    { redirect: "manual" },
    cookies,
    fetchImplementation,
  );
  const anonymousLocation =
    response.headers.get("location") ?? "https://ims.lakelandgov.net/ims";
  response = await fetchWithCookies(
    new URL(anonymousLocation, response.url).toString(),
    {},
    cookies,
    fetchImplementation,
  );
  assertPublicResponse(response, "Lakeland anonymous entry");
  const searchUrl = "https://ims.lakelandgov.net/ims/Find3?cat=Permits";
  response = await fetchWithCookies(
    searchUrl,
    {},
    cookies,
    fetchImplementation,
  );
  assertPublicResponse(response, "Lakeland permit search");
  const searchHtml = await response.text();
  const requestVerificationToken = firstCapture(
    searchHtml,
    /name=["']__RequestVerificationToken["'][^>]*value=["']([^"']+)["']/i,
  );
  if (requestVerificationToken === null) {
    throw new Error(
      "Lakeland permit search did not expose an antiforgery token",
    );
  }
  const body = new URLSearchParams({
    __RequestVerificationToken: requestVerificationToken,
    bSavedSearchLoaded: "False",
    "find3SearchCriteria[0].find3Definition.PromptType": "Text",
    "find3SearchCriteria[0].find3Definition.bTextAllowTwoCharacters": "False",
    "find3SearchCriteria[0].find3Definition.cat": "Permits",
    "find3SearchCriteria[0].find3Definition.StoredProcedureName":
      "dbo.iMSFind3PermitsPermitNumber",
    "find3SearchCriteria[0].SearchText": normalized,
    "find3SearchCriteria[0].HashText": "",
    "find3SearchCriteria[0].DateRange": "",
    "find3SearchCriteria[0].DateStart": "",
    "find3SearchCriteria[0].DateEnd": "",
  });
  response = await fetchWithCookies(
    "https://ims.lakelandgov.net/ims/Find3?bNewSearch=False&cat=Permits",
    {
      method: "POST",
      redirect: "manual",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Referer: searchUrl,
      },
      body,
    },
    cookies,
    fetchImplementation,
  );
  for (let redirectCount = 0; redirectCount < 4; redirectCount += 1) {
    const location = response.headers.get("location");
    if (location === null) break;
    response = await fetchWithCookies(
      new URL(location, response.url).toString(),
      { redirect: "manual", headers: { Referer: searchUrl } },
      cookies,
      fetchImplementation,
    );
  }
  assertPublicResponse(response, "Lakeland permit detail");
  const html = await response.text();
  if (
    !permitHtmlToText(html).toUpperCase().includes(normalized.toUpperCase())
  ) {
    throw new Error(
      `Lakeland permit ${normalized} did not resolve to a detail page`,
    );
  }
  return { url: response.url, html };
}

/**
 * Fetch a Winter Haven eSuite detail after establishing search state.
 *
 * @param {string} permitNumber Official permit number.
 * @param {typeof fetch} [fetchImplementation] Injectable fetch for tests.
 * @returns {Promise<{url:string,html:string}>} Public detail response.
 */
export async function fetchWinterHavenPermitDetail(
  permitNumber,
  fetchImplementation = fetch,
) {
  const searchUrl = buildWinterHavenPermitSearchUrl(permitNumber);
  const cookies = new Map();
  const searchResponse = await fetchWithCookies(
    searchUrl,
    {},
    cookies,
    fetchImplementation,
  );
  assertPublicResponse(searchResponse, "Winter Haven permit search");
  const searchHtml = await searchResponse.text();
  const rawDetailPath = firstCapture(
    searchHtml,
    /href=["']([^"']*ContractorPermitDetails\.aspx\?id=\d+[^"']*)["']/i,
  );
  if (rawDetailPath === null) {
    throw new Error(
      `Winter Haven permit ${permitNumber} returned no detail link`,
    );
  }
  const detailUrl = new URL(
    decodeHtmlEntities(rawDetailPath),
    searchResponse.url,
  ).toString();
  const detailResponse = await fetchWithCookies(
    detailUrl,
    { headers: { Referer: searchUrl } },
    cookies,
    fetchImplementation,
  );
  assertPublicResponse(detailResponse, "Winter Haven permit detail");
  return { url: detailResponse.url, html: await detailResponse.text() };
}

/**
 * Fetch a Lake Wales CitizenLink permit detail through its public AJAX API.
 *
 * @param {string} permitNumber Official permit number.
 * @param {typeof fetch} [fetchImplementation] Injectable fetch for tests.
 * @returns {Promise<{url:string,html:string}>} Decoded public detail body.
 */
export async function fetchLakeWalesPermitDetail(
  permitNumber,
  fetchImplementation = fetch,
) {
  const normalized = permitNumber.trim();
  if (normalized.length === 0)
    throw new Error("Lake Wales permit number is required");
  const baseUrl = "https://secure.lakewalesfl.gov";
  const portalUrl = `${baseUrl}/permits/`;
  const cookies = new Map();
  let response = await fetchWithCookies(
    portalUrl,
    {},
    cookies,
    fetchImplementation,
  );
  assertPublicResponse(response, "Lake Wales permit portal");
  response = await postCitizenLink(
    `${baseUrl}/adg/citizenlink/common/common/ajax/loadInitialMessages.php`,
    new URLSearchParams({ timeout: "60", SITENAME: "PERMITS" }),
    portalUrl,
    cookies,
    fetchImplementation,
  );
  assertPublicResponse(response, "Lake Wales permit bootstrap");
  response = await postCitizenLink(
    `${baseUrl}/adg/citizenlink/bps/common/ajax/permitByNumber.php`,
    new URLSearchParams({
      q: normalized,
      "searchFilter[term]": normalized,
      "searchFilter[_type]": "query",
      timeout: "60",
      SITENAME: "PERMITS",
      sourceClass: "corePermitByNumber",
      selectOptions: "false",
    }),
    portalUrl,
    cookies,
    fetchImplementation,
  );
  assertPublicResponse(response, "Lake Wales permit lookup");
  const suggestions = /** @type {unknown} */ (await response.json());
  const suggestion = Array.isArray(suggestions)
    ? suggestions.find(
        (candidate) =>
          isJsonObject(candidate) &&
          typeof candidate.id === "string" &&
          typeof candidate.text === "string" &&
          candidate.text.toUpperCase().includes(normalized.toUpperCase()),
      )
    : undefined;
  if (!isJsonObject(suggestion) || typeof suggestion.id !== "string") {
    throw new Error(`Lake Wales permit ${normalized} returned no exact result`);
  }
  const detailBody = new URLSearchParams({
    phpClass: "coreShowFullPermit",
    primaryCode: suggestion.id,
    targetData: suggestion.id,
    subCodeData: "",
    timeout: "60",
    SITENAME: "PERMITS",
    deviceLatitude: "0",
    deviceLongitude: "0",
    qrdata: "",
    userFeature: "0",
    persistentRequest: "1",
    discardRequest: "0",
    loaderType: "0",
    menuAction: "0",
  });
  response = await postCitizenLink(
    `${baseUrl}/adg/citizenlink/common/common/ajax/classDataLoader.php`,
    detailBody,
    portalUrl,
    cookies,
    fetchImplementation,
  );
  assertPublicResponse(response, "Lake Wales permit detail");
  const detailResponse = /** @type {unknown} */ (await response.json());
  if (
    !isJsonObject(detailResponse) ||
    typeof detailResponse.body !== "string"
  ) {
    throw new Error("Lake Wales permit detail returned no encoded body");
  }
  return {
    url: `${portalUrl}#permit-${encodeURIComponent(normalized)}`,
    html: Buffer.from(detailResponse.body, "base64").toString("utf8"),
  };
}

/**
 * Fetch and parse one source-specific certified adapter.
 *
 * @param {string} adapter Certified adapter key.
 * @param {string} permitNumber Official permit number.
 * @param {typeof fetch} [fetchImplementation] Injectable fetch for tests.
 * @returns {Promise<{url:string,detail:PolkAccelaPermitDetail}>} Parsed detail.
 */
export async function fetchPolkPermitAdapterDetail(
  adapter,
  permitNumber,
  fetchImplementation = fetch,
) {
  if (adapter === "polk_accela_cap_detail_v1") {
    const fetched = await fetchPolkAccelaPermitDetail(
      permitNumber,
      fetchImplementation,
    );
    return {
      url: fetched.url,
      detail: parsePolkPermitAdapterHtml(adapter, fetched.html, permitNumber),
    };
  }
  if (adapter === "lakeland_ims_permit_detail_v1") {
    const fetched = await fetchLakelandImsPermitDetail(
      permitNumber,
      fetchImplementation,
    );
    return {
      url: fetched.url,
      detail: parsePolkPermitAdapterHtml(adapter, fetched.html, permitNumber),
    };
  }
  if (adapter === "winter_haven_esuite_permit_detail_v1") {
    const fetched = await fetchWinterHavenPermitDetail(
      permitNumber,
      fetchImplementation,
    );
    return {
      url: fetched.url,
      detail: parsePolkPermitAdapterHtml(adapter, fetched.html, permitNumber),
    };
  }
  if (adapter === "lake_wales_citizenlink_permit_detail_v1") {
    const fetched = await fetchLakeWalesPermitDetail(
      permitNumber,
      fetchImplementation,
    );
    return {
      url: fetched.url,
      detail: parsePolkPermitAdapterHtml(adapter, fetched.html, permitNumber),
    };
  }
  throw new Error(`Unsupported Polk permit adapter: ${adapter}`);
}

/**
 * Parse saved or freshly fetched HTML for one certified adapter.
 *
 * @param {string} adapter Certified adapter key.
 * @param {string} html Public detail HTML.
 * @param {string} permitNumber Requested permit number.
 * @returns {PolkAccelaPermitDetail} Parsed public evidence.
 */
export function parsePolkPermitAdapterHtml(adapter, html, permitNumber) {
  if (adapter === "polk_accela_cap_detail_v1") {
    return parsePolkAccelaPermitDetailHtml(html);
  }
  if (adapter === "lakeland_ims_permit_detail_v1") {
    return parseLakelandImsPermitDetailHtml(html, permitNumber);
  }
  if (adapter === "winter_haven_esuite_permit_detail_v1") {
    return parseWinterHavenPermitDetailHtml(html, permitNumber);
  }
  if (adapter === "lake_wales_citizenlink_permit_detail_v1") {
    return parseLakeWalesPermitDetailHtml(html, permitNumber);
  }
  throw new Error(`Unsupported Polk permit adapter: ${adapter}`);
}

/**
 * Build the public source URL recorded for offline adapter evidence.
 *
 * @param {PolkPermitSource} source Certified source.
 * @param {string} permitNumber Requested permit number.
 * @returns {string} Public search or detail URL.
 */
function buildPolkPermitAdapterUrl(source, permitNumber) {
  if (source.adapter === "polk_accela_cap_detail_v1") {
    return buildPolkAccelaDetailUrl(permitNumber);
  }
  if (source.adapter === "winter_haven_esuite_permit_detail_v1") {
    return buildWinterHavenPermitSearchUrl(permitNumber);
  }
  if (source.searchUrl !== null) return source.searchUrl;
  throw new Error(`Polk permit source ${source.key} has no public URL`);
}

/**
 * Parse a currency capture into a finite number.
 *
 * @param {string} text Visible portal text.
 * @param {RegExp} pattern Currency capture.
 * @returns {number | null} Parsed amount.
 */
function parseCurrencyCapture(text, pattern) {
  const captured = firstCapture(text, pattern);
  if (captured === null) return null;
  const value = Number(captured.replaceAll(",", ""));
  return Number.isFinite(value) ? value : null;
}

/**
 * Perform a public request while retaining response cookies.
 *
 * @param {string} url Request URL.
 * @param {RequestInit} options Fetch options.
 * @param {Map<string, string>} cookies Mutable cookie jar.
 * @param {typeof fetch} fetchImplementation Injectable fetch.
 * @returns {Promise<Response>} Public response.
 */
async function fetchWithCookies(url, options, cookies, fetchImplementation) {
  const headers = new Headers(options.headers);
  headers.set("Accept", headers.get("Accept") ?? "text/html,application/json");
  headers.set(
    "User-Agent",
    headers.get("User-Agent") ?? "oracle-node-polk-permit-evidence/1.0",
  );
  if (cookies.size > 0) {
    headers.set(
      "Cookie",
      [...cookies.entries()]
        .map(([name, value]) => `${name}=${value}`)
        .join("; "),
    );
  }
  const response = await fetchImplementation(url, { ...options, headers });
  for (const cookie of responseSetCookies(response)) {
    const [nameValue] = cookie.split(";", 1);
    const separator = nameValue?.indexOf("=") ?? -1;
    if (nameValue !== undefined && separator > 0) {
      cookies.set(
        nameValue.slice(0, separator).trim(),
        nameValue.slice(separator + 1).trim(),
      );
    }
  }
  return response;
}

/**
 * Read response Set-Cookie values across Node fetch implementations.
 *
 * @param {Response} response Fetch response.
 * @returns {string[]} Individual cookie header values.
 */
function responseSetCookies(response) {
  const getSetCookie = Reflect.get(response.headers, "getSetCookie");
  if (typeof getSetCookie === "function") {
    const values = Reflect.apply(getSetCookie, response.headers, []);
    return Array.isArray(values) ? values.map(String) : [];
  }
  const combined = response.headers.get("set-cookie");
  return combined === null ? [] : combined.split(/,(?=[^;,]+=)/);
}

/**
 * Submit one read-only CitizenLink form request.
 *
 * @param {string} url AJAX endpoint.
 * @param {URLSearchParams} body Form body.
 * @param {string} referer Public portal URL.
 * @param {Map<string, string>} cookies Mutable cookie jar.
 * @param {typeof fetch} fetchImplementation Injectable fetch.
 * @returns {Promise<Response>} AJAX response.
 */
function postCitizenLink(url, body, referer, cookies, fetchImplementation) {
  return fetchWithCookies(
    url,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        Referer: referer,
      },
      body,
    },
    cookies,
    fetchImplementation,
  );
}

/**
 * Reject non-success public portal responses.
 *
 * @param {Response} response Public response.
 * @param {string} label Request label.
 * @returns {void}
 */
function assertPublicResponse(response, label) {
  if (!response.ok) {
    throw new Error(`${label} returned HTTP ${response.status}`);
  }
}

/**
 * Validate an unknown JSON value as an input permit candidate.
 *
 * @param {unknown} value Parsed JSONL value.
 * @returns {{permitNumber:string,agency:string} | null} Candidate or null.
 */
function permitCandidate(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const record = /** @type {JsonObject} */ (value);
  const permitNumber =
    typeof record.permitNumber === "string"
      ? record.permitNumber
      : typeof record.permit_number === "string"
        ? record.permit_number
        : "";
  const agency =
    typeof record.agency === "string"
      ? record.agency
      : typeof record.agency_name === "string"
        ? record.agency_name
        : "";
  return permitNumber.trim().length > 0 && agency.trim().length > 0
    ? { permitNumber: permitNumber.trim(), agency: agency.trim().toUpperCase() }
    : null;
}

/**
 * Build official agency coverage from the source registry.
 *
 * @param {{permitCount:number,agencies:readonly {value:string,count:number}[]}} permitSummary Official bulk permit summary.
 * @returns {{adapterEligiblePermitCount:number,unsupportedPermitCount:number,agencyCoverage:JsonObject[]}} Agency coverage counters.
 */
function buildPolkPermitAgencyCoverage(permitSummary) {
  const registryByAgency = new Map(
    POLK_PERMIT_SOURCE_REGISTRY.map((source) => [source.agency, source]),
  );
  let adapterEligiblePermitCount = 0;
  let unsupportedPermitCount = 0;
  /** @type {JsonObject[]} */
  const agencyCoverage = [];
  for (const agencyRow of permitSummary.agencies) {
    const source = registryByAgency.get(agencyRow.value);
    const adapterReady = source?.status === "adapter_ready";
    if (adapterReady) adapterEligiblePermitCount += agencyRow.count;
    else unsupportedPermitCount += agencyRow.count;
    agencyCoverage.push({
      agency: agencyRow.value,
      permitCount: agencyRow.count,
      sourceKey: source?.key ?? null,
      sourceStatus: source?.status ?? "unregistered",
      adapter: source?.adapter ?? null,
    });
  }
  return {
    adapterEligiblePermitCount,
    unsupportedPermitCount,
    agencyCoverage,
  };
}

/**
 * Build the countywide enrichment receipt from streaming run counters.
 *
 * @param {{permitCount:number,agencies:readonly {value:string,count:number}[]}} permitSummary Official permit summary.
 * @param {{inputRecordCount:number,invalidRecordCount:number,supportedRecordCount:number,partialAdapterAttemptedRecordCount?:number,enrichedRecordCount:number,partialAdapterEnrichedRecordCount?:number,contractorEvidenceCount:number,licenseEvidenceCount:number,fetchErrorCount:number,noDetailCount?:number,unsupportedRecordCount?:number,networkUsed:boolean,input:string,output:string}} run Streaming adapter counters.
 * @returns {JsonObject} Countywide receipt.
 */
export function buildPolkPermitEnrichmentReceiptFromRun(permitSummary, run) {
  const { adapterEligiblePermitCount, unsupportedPermitCount, agencyCoverage } =
    buildPolkPermitAgencyCoverage(permitSummary);
  const unattemptedAdapterRecordCount = Math.max(
    0,
    adapterEligiblePermitCount - run.supportedRecordCount,
  );
  const adapterRecordsWithoutEvidenceCount = Math.max(
    0,
    run.supportedRecordCount - run.enrichedRecordCount,
  );
  const blockerReasons = [
    unsupportedPermitCount > 0
      ? `${unsupportedPermitCount} official bulk permit rows belong to missing/unregistered agencies or agencies without certified anonymous adapters.`
      : null,
    unattemptedAdapterRecordCount > 0
      ? `${unattemptedAdapterRecordCount} adapter-eligible permit row${unattemptedAdapterRecordCount === 1 ? "" : "s"} ${unattemptedAdapterRecordCount === 1 ? "has" : "have"} not been attempted or ${unattemptedAdapterRecordCount === 1 ? "lacks" : "lack"} a requestable permit number.`
      : null,
    adapterRecordsWithoutEvidenceCount > 0
      ? `${adapterRecordsWithoutEvidenceCount} adapter-eligible permit row${adapterRecordsWithoutEvidenceCount === 1 ? "" : "s"} ${adapterRecordsWithoutEvidenceCount === 1 ? "was" : "were"} attempted but ${adapterRecordsWithoutEvidenceCount === 1 ? "lacks" : "lack"} public detail evidence.`
      : null,
    run.fetchErrorCount > 0 || run.invalidRecordCount > 0
      ? "The adapter run contains invalid inputs or exhausted fetch failures."
      : null,
  ].filter((reason) => reason !== null);
  const complete =
    permitSummary.permitCount > 0 &&
    unsupportedPermitCount === 0 &&
    run.invalidRecordCount === 0 &&
    run.fetchErrorCount === 0 &&
    run.supportedRecordCount === adapterEligiblePermitCount &&
    run.enrichedRecordCount === adapterEligiblePermitCount;
  return {
    schemaVersion: "oracle-node.polk-permit-enrichment-receipt.v1",
    generatedAt: new Date().toISOString(),
    county: "polk",
    officialPermitCount: permitSummary.permitCount,
    adapterEligiblePermitCount,
    unsupportedPermitCount,
    attemptedAdapterRecords: run.supportedRecordCount,
    unattemptedAdapterRecordCount,
    partialAdapterAttemptedRecords: run.partialAdapterAttemptedRecordCount ?? 0,
    enrichedRecordCount: run.enrichedRecordCount,
    adapterRecordsWithoutEvidenceCount,
    partialAdapterEnrichedRecordCount:
      run.partialAdapterEnrichedRecordCount ?? 0,
    contractorEvidenceCount: run.contractorEvidenceCount,
    licenseEvidenceCount: run.licenseEvidenceCount,
    invalidRecordCount: run.invalidRecordCount,
    fetchErrorCount: run.fetchErrorCount,
    noDetailCount: run.noDetailCount ?? 0,
    unsupportedInputRecordCount: run.unsupportedRecordCount ?? 0,
    networkUsed: run.networkUsed,
    input: run.input,
    output: run.output,
    agencyCoverage,
    complete,
    blocker: blockerReasons.length > 0 ? blockerReasons.join(" ") : null,
  };
}

/**
 * Build a fail-closed enrichment receipt from official agency denominators and
 * adapter outputs.
 *
 * @param {{permitCount:number,agencies:readonly {value:string,count:number}[]}} permitSummary Official bulk permit summary.
 * @param {readonly PolkPermitEnrichmentRecord[]} records Adapter output records.
 * @returns {JsonObject} Evidence receipt; `complete` is false while any official agency lacks a certified adapter.
 */
export function buildPolkPermitEnrichmentReceipt(permitSummary, records) {
  const enriched = records.filter((record) => record.status === "enriched");
  const withContractor = enriched.filter(
    (record) => record.detail?.contractor !== null,
  );
  const withLicense = enriched.filter(
    (record) =>
      typeof record.detail?.contractor?.licenseNumber === "string" &&
      record.detail.contractor.licenseNumber.length > 0,
  );
  const adapterSourceKeys = new Set(
    POLK_PERMIT_SOURCE_REGISTRY.filter(
      (source) => source.status === "adapter_ready",
    ).map((source) => source.key),
  );
  const attemptedAdapterRecords = records.filter((record) =>
    adapterSourceKeys.has(record.sourceKey),
  ).length;
  return buildPolkPermitEnrichmentReceiptFromRun(permitSummary, {
    inputRecordCount: records.length,
    invalidRecordCount: 0,
    supportedRecordCount: attemptedAdapterRecords,
    enrichedRecordCount: enriched.length,
    contractorEvidenceCount: withContractor.length,
    licenseEvidenceCount: withLicense.length,
    fetchErrorCount: records.filter((record) => record.status === "fetch_error")
      .length,
    networkUsed: false,
    input: "(in-memory)",
    output: "(in-memory)",
  });
}

/**
 * @typedef {object} PolkPermitWorkItem
 * @property {number} inputIndex Zero-based non-empty input-line index.
 * @property {{permitNumber:string,agency:string} | null} candidate Validated candidate.
 */

/**
 * @typedef {object} PolkPermitRunSettings
 * @property {number} concurrency Maximum simultaneous public requests.
 * @property {number} batchSize Deterministic records per atomic output part.
 * @property {number} delayMs Minimum delay between starts for one source.
 * @property {number} retryDelayMs Base retry delay.
 * @property {number} attempts Maximum attempts per public request.
 * @property {boolean} includePartial Whether explicitly requested partial adapters may run.
 * @property {boolean} network Whether to fetch live public pages.
 * @property {string} htmlDirectory Saved-HTML directory for offline runs.
 */

/**
 * @typedef {object} PolkPermitRunCounters
 * @property {number} inputRecordCount Non-empty candidate lines.
 * @property {number} invalidRecordCount Invalid candidate lines.
 * @property {number} supportedRecordCount Fully certified adapter attempts.
 * @property {number} partialAdapterAttemptedRecordCount Partial-adapter attempts.
 * @property {number} enrichedRecordCount Fully certified adapter records with evidence.
 * @property {number} partialAdapterEnrichedRecordCount Partial-adapter records with evidence.
 * @property {number} contractorEvidenceCount Enriched records with contractor evidence.
 * @property {number} licenseEvidenceCount Enriched records with state-license evidence.
 * @property {number} fetchErrorCount Exhausted request failures.
 * @property {number} noDetailCount Successful requests without detail evidence.
 * @property {number} unsupportedRecordCount Unsupported input records.
 */

/**
 * Parse a bounded positive integer option.
 *
 * @param {string | boolean | string[] | undefined} value Raw parseArgs value.
 * @param {string} option CLI option name.
 * @param {number} fallback Default value.
 * @param {number} maximum Safety ceiling.
 * @returns {number} Validated integer.
 */
function readBoundedPositiveInteger(value, option, fallback, maximum) {
  const parsed =
    typeof value === "string" && /^\d+$/.test(value)
      ? Number.parseInt(value, 10)
      : typeof value === "undefined"
        ? fallback
        : Number.NaN;
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > maximum) {
    throw new Error(`--${option} must be an integer between 1 and ${maximum}`);
  }
  return parsed;
}

/**
 * Pause without blocking the event loop.
 *
 * @param {number} milliseconds Delay duration.
 * @returns {Promise<void>} Resolves after the delay.
 */
function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

/**
 * Retry one asynchronous operation with bounded linear backoff.
 *
 * @template T
 * @param {() => Promise<T>} operation Operation to attempt.
 * @param {number} attempts Maximum attempts.
 * @param {number} retryDelayMs Base retry delay.
 * @param {(milliseconds:number) => Promise<void>} [sleepImplementation] Injectable delay.
 * @returns {Promise<T>} Successful result.
 */
export async function retryPolkPermitOperation(
  operation,
  attempts,
  retryDelayMs,
  sleepImplementation = sleep,
) {
  let lastError = new Error("Permit operation was not attempted");
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await operation();
    } catch (caught) {
      lastError = caught instanceof Error ? caught : new Error(String(caught));
      if (attempt < attempts) {
        await sleepImplementation(retryDelayMs * attempt);
      }
    }
  }
  throw lastError;
}

/**
 * Map values with a strict concurrency ceiling while preserving input order.
 *
 * @template T
 * @template R
 * @param {readonly T[]} values Input values.
 * @param {number} concurrency Worker count.
 * @param {(value:T,index:number) => Promise<R>} mapper Async mapper.
 * @returns {Promise<R[]>} Ordered results.
 */
export async function mapPolkPermitWithConcurrency(
  values,
  concurrency,
  mapper,
) {
  /** @type {R[]} */
  const results = new Array(values.length);
  let nextIndex = 0;
  const workers = Array.from(
    { length: Math.min(concurrency, values.length) },
    async () => {
      while (nextIndex < values.length) {
        const index = nextIndex;
        nextIndex += 1;
        const value = values[index];
        if (value !== undefined) results[index] = await mapper(value, index);
      }
    },
  );
  await Promise.all(workers);
  return results;
}

/**
 * Replace only exhausted fetch failures from a completed atomic part.
 *
 * @param {readonly PolkPermitEnrichmentRecord[]} records Existing ordered records.
 * @param {readonly {permitNumber:string,agency:string}[]} candidates Matching candidates.
 * @param {number} concurrency Redrive concurrency.
 * @param {(candidate:{permitNumber:string,agency:string}) => Promise<PolkPermitEnrichmentRecord>} mapper Candidate redrive operation.
 * @returns {Promise<{records:PolkPermitEnrichmentRecord[],redrivenCount:number}>} Updated ordered records.
 */
export async function redrivePolkPermitFetchErrors(
  records,
  candidates,
  concurrency,
  mapper,
) {
  if (records.length !== candidates.length) {
    throw new Error("Permit redrive records and candidates must align");
  }
  const failedIndexes = records.flatMap((record, index) =>
    record.status === "fetch_error" ? [index] : [],
  );
  const replacements = await mapPolkPermitWithConcurrency(
    failedIndexes,
    concurrency,
    async (index) => {
      const candidate = candidates[index];
      if (candidate === undefined) {
        throw new Error(`Missing permit redrive candidate at index ${index}`);
      }
      return mapper(candidate);
    },
  );
  const replacementByIndex = new Map(
    failedIndexes.map((index, replacementIndex) => [
      index,
      replacements[replacementIndex],
    ]),
  );
  return {
    records: records.map(
      (record, index) => replacementByIndex.get(index) ?? record,
    ),
    redrivenCount: failedIndexes.length,
  };
}

/**
 * Build a per-source request scheduler. Starts for one portal are spaced by the
 * configured delay even when unrelated portals run concurrently.
 *
 * @param {number} delayMs Minimum milliseconds between starts per source.
 * @returns {(sourceKey:string,operation:() => Promise<{url:string,detail:PolkAccelaPermitDetail}>) => Promise<{url:string,detail:PolkAccelaPermitDetail}>} Scheduler.
 */
function createPolkPermitSourceScheduler(delayMs) {
  /** @type {Map<string, Promise<void>>} */
  const sourceTails = new Map();
  /** @type {Map<string, number>} */
  const sourceNextStart = new Map();
  return async (sourceKey, operation) => {
    const previous = sourceTails.get(sourceKey) ?? Promise.resolve();
    const gate = previous
      .catch(() => undefined)
      .then(async () => {
        const waitMs = Math.max(
          0,
          (sourceNextStart.get(sourceKey) ?? 0) - Date.now(),
        );
        if (waitMs > 0) await sleep(waitMs);
        sourceNextStart.set(sourceKey, Date.now() + delayMs);
      });
    sourceTails.set(sourceKey, gate);
    await gate;
    return operation();
  };
}

/**
 * Read candidate JSONL into stable, indexed work items.
 *
 * @param {string} input Candidate JSONL path.
 * @returns {Promise<PolkPermitWorkItem[]>} Ordered work items.
 */
async function readPolkPermitWorkItems(input) {
  const reader = createInterface({
    input: createReadStream(input, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });
  /** @type {PolkPermitWorkItem[]} */
  const workItems = [];
  for await (const line of reader) {
    if (line.trim().length === 0) continue;
    let candidate = null;
    try {
      candidate = permitCandidate(JSON.parse(line));
    } catch {
      candidate = null;
    }
    workItems.push({ inputIndex: workItems.length, candidate });
  }
  return workItems;
}

/**
 * Write text through an atomic same-directory rename.
 *
 * @param {string} destination Final path.
 * @param {string} text Complete file contents.
 * @returns {Promise<void>} Resolves after atomic replacement.
 */
async function writePolkPermitAtomicText(destination, text) {
  await mkdir(path.dirname(destination), { recursive: true });
  const temporary = `${destination}.tmp-${process.pid}`;
  await writeFile(temporary, text, "utf8");
  await rename(temporary, destination);
}

/**
 * Read and validate an existing deterministic batch part.
 *
 * @param {string} partPath Existing part path.
 * @param {readonly PolkPermitWorkItem[]} workItems Expected work items.
 * @returns {Promise<PolkPermitEnrichmentRecord[] | null>} Valid records or null when absent.
 */
async function readPolkPermitPart(partPath, workItems) {
  let text;
  try {
    text = await readFile(partPath, "utf8");
  } catch (caught) {
    if (
      caught instanceof Error &&
      "code" in caught &&
      /** @type {NodeJS.ErrnoException} */ (caught).code === "ENOENT"
    ) {
      return null;
    }
    throw caught;
  }
  const expected = workItems.flatMap((item) =>
    item.candidate === null ? [] : [item.candidate],
  );
  const records = text
    .split(/\r?\n/)
    .filter((line) => line.trim().length > 0)
    .map(
      (line) => /** @type {PolkPermitEnrichmentRecord} */ (JSON.parse(line)),
    );
  if (
    records.length !== expected.length ||
    records.some(
      (record, index) =>
        record.permitNumber !== expected[index]?.permitNumber ||
        record.agency !== expected[index]?.agency,
    )
  ) {
    throw new Error(`Stale or incomplete permit enrichment part: ${partPath}`);
  }
  return records;
}

/**
 * Enrich one valid candidate with retries and per-source request pacing.
 *
 * @param {{permitNumber:string,agency:string}} candidate Candidate record.
 * @param {PolkPermitRunSettings} settings Run settings.
 * @param {(sourceKey:string,operation:() => Promise<{url:string,detail:PolkAccelaPermitDetail}>) => Promise<{url:string,detail:PolkAccelaPermitDetail}>} scheduleRequest Per-source scheduler.
 * @returns {Promise<PolkPermitEnrichmentRecord>} Enrichment result.
 */
async function enrichPolkPermitCandidate(candidate, settings, scheduleRequest) {
  const source = findPolkPermitSource(candidate.agency);
  const runnable =
    source !== null &&
    source.adapter !== null &&
    (source.status === "adapter_ready" ||
      (settings.includePartial && source.status === "partial_adapter_ready"));
  if (!runnable || source === null || source.adapter === null) {
    return {
      permitNumber: candidate.permitNumber,
      agency: candidate.agency,
      sourceKey: source?.key ?? "unregistered",
      sourceUrl: source?.searchUrl ?? null,
      status: "unsupported_source",
      detail: null,
      error: source?.evidence ?? "Agency is not registered.",
      retrievedAt: new Date().toISOString(),
    };
  }
  try {
    const fetched = await retryPolkPermitOperation(
      () =>
        settings.network
          ? scheduleRequest(source.key, () =>
              fetchPolkPermitAdapterDetail(
                /** @type {string} */ (source.adapter),
                candidate.permitNumber,
              ),
            )
          : readFile(
              path.join(
                settings.htmlDirectory,
                `${candidate.permitNumber.replace(/[^A-Z0-9_-]/gi, "_")}.html`,
              ),
              "utf8",
            ).then((html) => ({
              url: buildPolkPermitAdapterUrl(source, candidate.permitNumber),
              detail: parsePolkPermitAdapterHtml(
                /** @type {string} */ (source.adapter),
                html,
                candidate.permitNumber,
              ),
            })),
      settings.attempts,
      settings.retryDelayMs,
    );
    const detail = fetched.detail;
    const hasEvidence =
      detail.permitNumber !== null ||
      detail.recordStatus !== null ||
      detail.parcelIdentifier !== null ||
      detail.contractor !== null ||
      detail.jobValuationUsd !== null;
    return {
      permitNumber: candidate.permitNumber,
      agency: candidate.agency,
      sourceKey: source.key,
      sourceUrl: fetched.url,
      status: hasEvidence ? "enriched" : "no_detail",
      detail,
      error: null,
      retrievedAt: new Date().toISOString(),
    };
  } catch (caught) {
    return {
      permitNumber: candidate.permitNumber,
      agency: candidate.agency,
      sourceKey: source.key,
      sourceUrl: buildPolkPermitAdapterUrl(source, candidate.permitNumber),
      status: "fetch_error",
      detail: null,
      error: caught instanceof Error ? caught.message : String(caught),
      retrievedAt: new Date().toISOString(),
    };
  }
}

/**
 * Add one completed part to deterministic run counters.
 *
 * @param {PolkPermitRunCounters} counters Mutable counters.
 * @param {readonly PolkPermitEnrichmentRecord[]} records Completed records.
 * @returns {void}
 */
function countPolkPermitPart(counters, records) {
  for (const record of records) {
    const source = POLK_PERMIT_SOURCE_REGISTRY.find(
      (candidate) => candidate.key === record.sourceKey,
    );
    if (source?.status === "adapter_ready") counters.supportedRecordCount += 1;
    if (source?.status === "partial_adapter_ready")
      counters.partialAdapterAttemptedRecordCount += 1;
    if (record.status === "enriched") {
      if (source?.status === "adapter_ready") counters.enrichedRecordCount += 1;
      if (source?.status === "partial_adapter_ready")
        counters.partialAdapterEnrichedRecordCount += 1;
      if (record.detail?.contractor !== null)
        counters.contractorEvidenceCount += 1;
      if (record.detail?.contractor?.licenseNumber !== null)
        counters.licenseEvidenceCount += 1;
    } else if (record.status === "fetch_error") {
      counters.fetchErrorCount += 1;
    } else if (record.status === "no_detail") {
      counters.noDetailCount += 1;
    } else if (record.status === "unsupported_source") {
      counters.unsupportedRecordCount += 1;
    }
  }
}

/**
 * Concatenate deterministic part files into the legacy single-JSONL handoff.
 *
 * @param {readonly string[]} partPaths Ordered part paths.
 * @param {string} output Destination JSONL.
 * @returns {Promise<void>} Resolves after atomic output replacement.
 */
async function assemblePolkPermitOutput(partPaths, output) {
  const temporary = `${output}.tmp-${process.pid}`;
  const writer = createWriteStream(temporary, { encoding: "utf8" });
  try {
    for (const partPath of partPaths) {
      const text = await readFile(partPath, "utf8");
      if (!writer.write(text)) {
        await new Promise((resolve) => writer.once("drain", resolve));
      }
    }
    await new Promise((resolve, reject) => {
      writer.once("error", reject);
      writer.end(resolve);
    });
    await rename(temporary, output);
  } catch (caught) {
    writer.destroy();
    await rm(temporary, { force: true });
    throw caught;
  }
}

/**
 * Run the local Polk permit evidence adapter against candidate JSONL.
 *
 * Network access is opt-in. Without `--network`, the script consumes previously
 * saved `<permit-number>.html` files from `--html-dir`.
 *
 * @param {readonly string[]} argv CLI arguments.
 * @returns {Promise<JsonObject>} Run receipt.
 */
export async function runPolkPermitEnrichment(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      stage: { type: "string" },
      input: { type: "string" },
      output: { type: "string" },
      receipt: { type: "string" },
      "permit-summary": { type: "string" },
      "html-dir": { type: "string" },
      "work-db": { type: "string" },
      agency: { type: "string", multiple: true },
      limit: { type: "string" },
      network: { type: "boolean" },
      "include-partial": { type: "boolean" },
      concurrency: { type: "string" },
      "batch-size": { type: "string" },
      "delay-ms": { type: "string" },
      attempts: { type: "string" },
      "retry-delay-ms": { type: "string" },
      "state-dir": { type: "string" },
      checkpoint: { type: "string" },
      "reset-checkpoint": { type: "boolean" },
      "redrive-errors": { type: "boolean" },
    },
    strict: true,
    allowPositionals: false,
  });
  const stage = typeof values.stage === "string" ? values.stage : "enrich";
  const input =
    typeof values.input === "string"
      ? values.input
      : "tmp/polk/permits/adapter-candidates.jsonl";
  const output =
    typeof values.output === "string"
      ? values.output
      : "tmp/polk/permits/enriched-permits.jsonl";
  const receiptPath =
    typeof values.receipt === "string"
      ? values.receipt
      : "tmp/polk/permits/enrichment-receipt.json";
  const permitSummaryPath =
    typeof values["permit-summary"] === "string"
      ? values["permit-summary"]
      : "tmp/polk/parity/permit-enrichment.json";
  const htmlDirectory =
    typeof values["html-dir"] === "string"
      ? values["html-dir"]
      : "tmp/polk/permits/html";
  if (stage === "candidates") {
    const limit =
      typeof values.limit === "string"
        ? Number.parseInt(values.limit, 10)
        : null;
    const agencies = Array.isArray(values.agency)
      ? values.agency.map(String)
      : POLK_PERMIT_SOURCE_REGISTRY.filter(
          (source) => source.status === "adapter_ready",
        ).map((source) => source.agency);
    return writePolkPermitAdapterCandidates({
      workDatabase:
        typeof values["work-db"] === "string"
          ? values["work-db"]
          : "tmp/polk/bulk/extracted/polk-appraisal.duckdb",
      output: input,
      agencies,
      limit,
    });
  }
  if (stage !== "enrich") {
    throw new Error("--stage must be candidates or enrich");
  }
  const settings = {
    concurrency: readBoundedPositiveInteger(
      values.concurrency,
      "concurrency",
      3,
      12,
    ),
    batchSize: readBoundedPositiveInteger(
      values["batch-size"],
      "batch-size",
      100,
      10_000,
    ),
    delayMs: readBoundedPositiveInteger(
      values["delay-ms"],
      "delay-ms",
      1_000,
      60_000,
    ),
    attempts: readBoundedPositiveInteger(values.attempts, "attempts", 3, 10),
    retryDelayMs: readBoundedPositiveInteger(
      values["retry-delay-ms"],
      "retry-delay-ms",
      2_000,
      300_000,
    ),
    includePartial: values["include-partial"] === true,
    network: values.network === true,
    htmlDirectory,
  };
  const stateDirectory =
    typeof values["state-dir"] === "string"
      ? values["state-dir"]
      : `${output}.parts`;
  const checkpointPath =
    typeof values.checkpoint === "string"
      ? values.checkpoint
      : `${output}.checkpoint.json`;
  if (values["reset-checkpoint"] === true) {
    await Promise.all([
      rm(stateDirectory, { recursive: true, force: true }),
      rm(checkpointPath, { force: true }),
    ]);
  }
  await Promise.all([
    mkdir(path.dirname(output), { recursive: true }),
    mkdir(path.dirname(receiptPath), { recursive: true }),
    mkdir(stateDirectory, { recursive: true }),
    mkdir(path.dirname(checkpointPath), { recursive: true }),
  ]);
  const workItems = await readPolkPermitWorkItems(input);
  /** @type {PolkPermitRunCounters} */
  const counters = {
    inputRecordCount: workItems.length,
    invalidRecordCount: workItems.filter((item) => item.candidate === null)
      .length,
    supportedRecordCount: 0,
    partialAdapterAttemptedRecordCount: 0,
    enrichedRecordCount: 0,
    partialAdapterEnrichedRecordCount: 0,
    contractorEvidenceCount: 0,
    licenseEvidenceCount: 0,
    fetchErrorCount: 0,
    noDetailCount: 0,
    unsupportedRecordCount: 0,
  };
  const totalPartCount = Math.ceil(workItems.length / settings.batchSize);
  const scheduleRequest = createPolkPermitSourceScheduler(settings.delayMs);
  /** @type {string[]} */
  const partPaths = [];
  let completedPartCount = 0;
  for (
    let offset = 0;
    offset < workItems.length;
    offset += settings.batchSize
  ) {
    const partIndex = Math.floor(offset / settings.batchSize);
    const partItems = workItems.slice(offset, offset + settings.batchSize);
    const partPath = path.join(
      stateDirectory,
      `part-${String(partIndex).padStart(6, "0")}.jsonl`,
    );
    let records = await readPolkPermitPart(partPath, partItems);
    const validItems = partItems.flatMap((item) =>
      item.candidate === null ? [] : [item.candidate],
    );
    if (records !== null && values["redrive-errors"] === true) {
      const redrive = await redrivePolkPermitFetchErrors(
        records,
        validItems,
        settings.concurrency,
        (candidate) =>
          enrichPolkPermitCandidate(candidate, settings, scheduleRequest),
      );
      records = redrive.records;
      if (redrive.redrivenCount > 0) {
        await writePolkPermitAtomicText(
          partPath,
          records.map((record) => JSON.stringify(record)).join("\n") +
            (records.length > 0 ? "\n" : ""),
        );
      }
    }
    if (records === null) {
      records = await mapPolkPermitWithConcurrency(
        validItems,
        settings.concurrency,
        (candidate) =>
          enrichPolkPermitCandidate(candidate, settings, scheduleRequest),
      );
      await writePolkPermitAtomicText(
        partPath,
        records.map((record) => JSON.stringify(record)).join("\n") +
          (records.length > 0 ? "\n" : ""),
      );
    }
    partPaths.push(partPath);
    countPolkPermitPart(counters, records);
    completedPartCount += 1;
    await writePolkPermitAtomicText(
      checkpointPath,
      `${JSON.stringify(
        {
          schemaVersion: "oracle-node.polk-permit-enrichment-checkpoint.v1",
          updatedAt: new Date().toISOString(),
          input,
          output,
          stateDirectory,
          batchSize: settings.batchSize,
          completedPartCount,
          totalPartCount,
          processedInputRecordCount: Math.min(
            workItems.length,
            offset + partItems.length,
          ),
          inputRecordCount: workItems.length,
        },
        null,
        2,
      )}\n`,
    );
    process.stdout.write(
      `${JSON.stringify({
        event: "polk_permit_enrichment_progress",
        completedPartCount,
        totalPartCount,
        processedInputRecordCount: Math.min(
          workItems.length,
          offset + partItems.length,
        ),
        inputRecordCount: workItems.length,
      })}\n`,
    );
  }
  await assemblePolkPermitOutput(partPaths, output);
  const run = {
    input,
    output,
    networkUsed: settings.network,
    ...counters,
    complete:
      counters.inputRecordCount > 0 &&
      counters.invalidRecordCount === 0 &&
      counters.fetchErrorCount === 0 &&
      counters.supportedRecordCount +
        counters.partialAdapterAttemptedRecordCount ===
        counters.inputRecordCount &&
      counters.enrichedRecordCount +
        counters.partialAdapterEnrichedRecordCount ===
        counters.inputRecordCount,
  };
  const permitSummary = /** @type {unknown} */ (
    JSON.parse(await readFile(permitSummaryPath, "utf8"))
  );
  if (
    !isJsonObject(permitSummary) ||
    typeof permitSummary.permitCount !== "number" ||
    !Array.isArray(permitSummary.agencies)
  ) {
    throw new Error(
      `A valid official permit summary is required at ${permitSummaryPath}`,
    );
  }
  const agencies = permitSummary.agencies.flatMap((candidate) =>
    isJsonObject(candidate) &&
    typeof candidate.value === "string" &&
    typeof candidate.count === "number"
      ? [{ value: candidate.value, count: candidate.count }]
      : [],
  );
  const receipt = buildPolkPermitEnrichmentReceiptFromRun(
    { permitCount: permitSummary.permitCount, agencies },
    run,
  );
  await writeFile(receiptPath, `${JSON.stringify(receipt, null, 2)}\n`, "utf8");
  return receipt;
}

if (
  process.argv[1] !== undefined &&
  fileURLToPath(import.meta.url) === path.resolve(process.argv[1])
) {
  runPolkPermitEnrichment(process.argv.slice(2))
    .then((receipt) => {
      process.stdout.write(`${JSON.stringify(receipt, null, 2)}\n`);
    })
    .catch((caught) => {
      const message = caught instanceof Error ? caught.message : String(caught);
      process.stderr.write(
        `${JSON.stringify({ event: "polk_permit_enrichment_failed", error: message })}\n`,
      );
      process.exitCode = 1;
    });
}
