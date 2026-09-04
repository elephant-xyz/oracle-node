#!/usr/bin/env node

import { createHash } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { createRequire } from "node:module";
import {
  mkdir,
  open,
  readFile,
  readdir,
  rename,
  rm,
  writeFile,
} from "node:fs/promises";
import * as path from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

const require = createRequire(import.meta.url);
const duckdb = require("duckdb");
const POLK_PERMIT_ADAPTER_CONTRACT_VERSION = "2026-09-03.2";

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
 * @typedef {"official_bulk" | "accela" | "ims" | "tyler_esuite" | "iworq" | "govbuilt" | "municipal_portal" | "manual_records" | "none_verified"} PolkPermitPortalKind
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
  {
    key: "auburndale_govbuilt",
    agency: "AUBURNDALE",
    portalKind: "govbuilt",
    status: "portal_verified_adapter_pending",
    officialUrl: "https://auburndalefl.com/construction-services/",
    searchUrl: "https://auburndalefl.govbuilt.com/",
    adapter: null,
    evidence:
      "The official city page links GovBuilt. Historical retention, anonymous list access, identifier mapping, and safe throughput remain uncertified.",
    verifiedAt: "2026-09-03",
  },
  {
    key: "bartow_building",
    agency: "BARTOW",
    portalKind: "manual_records",
    status: "no_public_detail_source_verified",
    officialUrl: "https://www.cityofbartow.net/159/Building-Department",
    searchUrl: null,
    adapter: null,
    evidence:
      "The official department publishes applications and forms but no anonymous historical permit-record search or export has been verified.",
    verifiedAt: "2026-09-03",
  },
  {
    key: "davenport_iworq",
    agency: "DAVENPORT",
    portalKind: "iworq",
    status: "portal_verified_adapter_pending",
    officialUrl:
      "https://www.mydavenport.org/index.asp?SEC=54C1C62E-BE5B-43DE-AF31-EF135278CEAD",
    searchUrl: "https://portal.iworq.net/DAVENPORT/permits/600",
    adapter: null,
    evidence:
      "The official city page links iWorQ status, document, and inspection access. Historical retention and predecessor County-held records remain uncertified.",
    verifiedAt: "2026-09-03",
  },
  {
    key: "dundee_mixed_custody",
    agency: "DUNDEE",
    portalKind: "accela",
    status: "portal_verified_adapter_pending",
    officialUrl: "https://townofdundee.com/departments/building-services/",
    searchUrl:
      "https://aca-prod.accela.com/POLKCO/Cap/CapHome.aspx?module=Building&TabName=Building",
    adapter: null,
    evidence:
      "The town administers building services while recent BR records resolve in County Accela. Delegation and predecessor date boundaries require official confirmation.",
    verifiedAt: "2026-09-03",
  },
  {
    key: "eagle_lake_polkco",
    agency: "EAGLE LAKE",
    portalKind: "accela",
    status: "portal_verified_adapter_pending",
    officialUrl: "https://www.eaglelakefl.gov/building",
    searchUrl:
      "https://aca-prod.accela.com/POLKCO/Cap/CapHome.aspx?module=Building&TabName=Building",
    adapter: null,
    evidence:
      "The city links County Accela and a recent BR record resolves there. A bounded historical and pagination pilot is still required.",
    verifiedAt: "2026-09-03",
  },
  {
    key: "fort_meade_polkco",
    agency: "FORT MEADE",
    portalKind: "accela",
    status: "portal_verified_adapter_pending",
    officialUrl: "https://www.cityoffortmeade.org/departments/building.php",
    searchUrl:
      "https://aca-prod.accela.com/POLKCO/Cap/CapHome.aspx?module=Building&TabName=Building",
    adapter: null,
    evidence:
      "The city links County Accela and a recent BR record resolves there. Historical custody and accessible totals remain uncertified.",
    verifiedAt: "2026-09-03",
  },
  {
    key: "frostproof_building",
    agency: "FROSTPROOF",
    portalKind: "manual_records",
    status: "no_public_detail_source_verified",
    officialUrl: "https://cityoffrostproof.com/departments/building/",
    searchUrl: null,
    adapter: null,
    evidence:
      "The city publishes manual application contact paths but no anonymous historical record search or export has been verified.",
    verifiedAt: "2026-09-03",
  },
  {
    key: "lake_alfred_accela",
    agency: "LAKE ALFRED",
    portalKind: "accela",
    status: "portal_verified_adapter_pending",
    officialUrl: "https://www.mylakealfred.com/166/Building-Permits",
    searchUrl: "https://aca-prod.accela.com/COLA/Default.aspx",
    adapter: null,
    evidence:
      "The official city page links its Accela agency and a recent BR record resolves. Historical retention and identifier-family mapping remain uncertified.",
    verifiedAt: "2026-09-03",
  },
  {
    key: "lake_hamilton_iworq",
    agency: "LAKE HAMILTON",
    portalKind: "iworq",
    status: "portal_verified_adapter_pending",
    officialUrl: "https://townoflakehamilton.com/1205/Community-Development",
    searchUrl:
      "https://townoflakehamilton.portal.iworq.net/portalhome/townoflakehamilton",
    adapter: null,
    evidence:
      "The official town page links iWorQ, but record lookup requires permit number plus contractor ID and historical retention is unknown.",
    verifiedAt: "2026-09-03",
  },
  {
    key: "mulberry_accela",
    agency: "MULBERRY",
    portalKind: "accela",
    status: "portal_verified_adapter_pending",
    officialUrl: "https://www.cityofmulberryfl.org/building-department",
    searchUrl: "https://aca-prod.accela.com/MULBERRY/Default.aspx",
    adapter: null,
    evidence:
      "The city has an Accela agency, but sampled Property Appraiser identifiers did not resolve; identifier mapping and historical retention remain uncertified.",
    verifiedAt: "2026-09-03",
  },
  {
    key: "polk_city_polkco",
    agency: "POLK CITY",
    portalKind: "accela",
    status: "portal_verified_adapter_pending",
    officialUrl: "https://www.mypolkcity.org/building",
    searchUrl:
      "https://aca-prod.accela.com/POLKCO/Cap/CapHome.aspx?module=Building&TabName=Building",
    adapter: null,
    evidence:
      "The city explicitly uses County Accela and a recent BR record resolves. A historical boundary and throughput pilot remains required.",
    verifiedAt: "2026-09-03",
  },
  {
    key: "highland_park_polkco",
    agency: "HIGHLAND PARK",
    portalKind: "accela",
    status: "portal_verified_adapter_pending",
    officialUrl:
      "https://www.highlandpark-fl.org/Permit_Authorization_Form_VHP.pdf",
    searchUrl:
      "https://aca-prod.accela.com/POLKCO/Cap/CapHome.aspx?module=Building&TabName=Building",
    adapter: null,
    evidence:
      "Village permit authorization delegates issuance to Polk County under an interlocal arrangement; records are not separately labeled in the bulk projection.",
    verifiedAt: "2026-09-03",
  },
  {
    key: "hillcrest_heights_polkco",
    agency: "HILLCREST HEIGHTS",
    portalKind: "accela",
    status: "portal_verified_adapter_pending",
    officialUrl: "https://www.polkflpa.gov/permitagencies.aspx",
    searchUrl:
      "https://aca-prod.accela.com/POLKCO/Cap/CapHome.aspx?module=Building&TabName=Building",
    adapter: null,
    evidence:
      "The Property Appraiser directory assigns County lookup. County-held records cannot currently be separated from agency-null or Polk County rows.",
    verifiedAt: "2026-09-03",
  },
]);

/**
 * @typedef {object} PolkPermitCandidateOptions
 * @property {string} workDatabase Completed Polk DuckDB cache.
 * @property {string} output JSONL destination.
 * @property {readonly string[]} agencies Official agency labels to include.
 * @property {number | null} limit Optional deterministic pilot cap.
 * @property {boolean} [winterHavenHistoricalOnly] Restrict to the certified legacy Winter Haven identifier shape.
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
 * @param {boolean} [winterHavenHistoricalOnly=false] Restrict to certified legacy Winter Haven identifiers.
 * @returns {string} Read-only DuckDB SQL.
 */
export function buildPolkPermitCandidateSql(
  agencies,
  limit,
  winterHavenHistoricalOnly = false,
) {
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
      ${
        winterHavenHistoricalOnly
          ? "AND regexp_matches(trim(permit_number), '^20[0-9]{2}-[0-9]{8}$')"
          : ""
      }
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
 * Identify Winter Haven records supported by the anonymous legacy eSuite
 * search. New `WHyy-*` identifiers belong to the replacement portal and are
 * intentionally excluded from this partial adapter.
 *
 * @param {string} permitNumber Official permit identifier.
 * @returns {boolean} Whether the legacy adapter supports the identifier shape.
 */
export function isWinterHavenHistoricalPermitNumber(permitNumber) {
  return /^20\d{2}-\d{8}$/.test(permitNumber.trim());
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
      buildPolkPermitCandidateSql(
        options.agencies,
        options.limit,
        options.winterHavenHistoricalOnly === true,
      ),
    );
  } finally {
    await closeDuckDbConnection(connection);
  }
  const candidates = rows.flatMap((row) => {
    const permitNumber =
      typeof row.permitNumber === "string" ? row.permitNumber.trim() : "";
    const agency = typeof row.agency === "string" ? row.agency.trim() : "";
    if (permitNumber.length === 0 || agency.length === 0) return [];
    if (
      options.winterHavenHistoricalOnly === true &&
      (agency.toUpperCase() !== "WINTER HAVEN" ||
        !isWinterHavenHistoricalPermitNumber(permitNumber))
    ) {
      return [];
    }
    return [{ permitNumber, agency }];
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
    winterHavenHistoricalOnly: options.winterHavenHistoricalOnly === true,
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
 * Normalize a permit identifier for exact cross-source comparisons.
 *
 * @param {string} value Raw permit identifier.
 * @returns {string} Uppercase alphanumeric identifier.
 */
function normalizePermitIdentifier(value) {
  return value.toUpperCase().replace(/[^A-Z0-9]/g, "");
}

/**
 * Confirm rendered source text contains the complete requested identifier.
 *
 * @param {string} text Rendered portal text.
 * @param {string} permitNumber Requested permit identifier.
 * @returns {boolean} Whether the complete identifier appears as one token.
 */
function containsExactPermitIdentifier(text, permitNumber) {
  const normalized = permitNumber.trim();
  if (normalized.length === 0) return false;
  return new RegExp(
    `(^|[^A-Z0-9])${escapeRegularExpression(normalized)}([^A-Z0-9]|$)`,
    "i",
  ).test(text);
}

/**
 * Permanent per-record source miss that should not consume retry attempts.
 */
class PolkPermitNotFoundError extends Error {
  /**
   * @param {string} message Evidence-backed missing-record detail.
   */
  constructor(message) {
    super(message);
    this.name = "PolkPermitNotFoundError";
  }
}

/**
 * Add an abort signal to every request in one multi-step portal operation.
 *
 * @param {typeof fetch} fetchImplementation Injectable fetch implementation.
 * @param {number} timeoutMs Per-request timeout in milliseconds.
 * @returns {typeof fetch} Fetch implementation with a default timeout signal.
 */
export function createPolkPermitTimedFetch(fetchImplementation, timeoutMs) {
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1) {
    throw new Error("Polk permit fetch timeout must be a positive integer");
  }
  return /** @type {typeof fetch} */ (
    (input, init = {}) =>
      fetchImplementation(input, {
        ...init,
        signal: init.signal ?? AbortSignal.timeout(timeoutMs),
      })
  );
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
    const message = `Polk Accela detail returned HTTP ${response.status}`;
    if (response.status === 404) throw new PolkPermitNotFoundError(message);
    throw new Error(message);
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
 * Resolve the exact Winter Haven result row instead of accepting the first link.
 *
 * @param {string} html Public eSuite search result HTML.
 * @param {string} permitNumber Requested permit number.
 * @returns {string | null} Matching session-scoped detail path.
 */
export function findWinterHavenPermitDetailPath(html, permitNumber) {
  const rows = [...html.matchAll(/<tr\b[^>]*>[\s\S]*?<\/tr>/gi)].map(
    (match) => match[0],
  );
  for (const row of rows) {
    if (!containsExactPermitIdentifier(permitHtmlToText(row), permitNumber)) {
      continue;
    }
    const detailPath = firstCapture(
      row,
      /href=["']([^"']*ContractorPermitDetails\.aspx\?id=\d+[^"']*)["']/i,
    );
    if (detailPath !== null) return detailPath;
  }
  const allDetailPaths = [
    ...html.matchAll(
      /href=["']([^"']*ContractorPermitDetails\.aspx\?id=\d+[^"']*)["']/gi,
    ),
  ].flatMap((match) => (typeof match[1] === "string" ? [match[1]] : []));
  return allDetailPaths.length === 1 &&
    containsExactPermitIdentifier(permitHtmlToText(html), permitNumber)
    ? (allDetailPaths[0] ?? null)
    : null;
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
  const parsedPermitNumber =
    firstCapture(text, /\bPermit(?: Number)?:\s*([A-Z0-9-]+)/i) ??
    firstCapture(text, /\bPermit\s+([A-Z0-9-]+)\b/i);
  const permitNumber =
    parsedPermitNumber !== null &&
    normalizePermitIdentifier(parsedPermitNumber) ===
      normalizePermitIdentifier(normalizedPermit)
      ? parsedPermitNumber
      : null;
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
  const parsedPermitNumber =
    firstCapture(text, /\bPermit(?: Number)?:\s*([A-Z0-9-]+)/i) ??
    firstCapture(text, /\bPermit\s*#\s*:?\s*([A-Z0-9-]+)/i);
  return {
    permitNumber:
      parsedPermitNumber !== null &&
      normalizePermitIdentifier(parsedPermitNumber) ===
        normalizePermitIdentifier(normalizedPermit)
        ? parsedPermitNumber
        : null,
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
  const parsedPermitNumber = firstCapture(
    text,
    /\bPermit(?: Number)?:\s*([A-Z0-9-]+)/i,
  );
  return {
    permitNumber:
      parsedPermitNumber !== null &&
      normalizePermitIdentifier(parsedPermitNumber) ===
        normalizePermitIdentifier(normalizedPermit)
        ? parsedPermitNumber
        : null,
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
  if (response.status === 404) {
    throw new PolkPermitNotFoundError(
      `Lakeland permit ${normalized} returned HTTP 404`,
    );
  }
  assertPublicResponse(response, "Lakeland permit detail");
  const html = await response.text();
  if (!containsExactPermitIdentifier(permitHtmlToText(html), normalized)) {
    throw new PolkPermitNotFoundError(
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
  const rawDetailPath = findWinterHavenPermitDetailPath(
    searchHtml,
    permitNumber,
  );
  if (rawDetailPath === null) {
    throw new PolkPermitNotFoundError(
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
  if (detailResponse.status === 404) {
    throw new PolkPermitNotFoundError(
      `Winter Haven permit ${permitNumber} returned HTTP 404`,
    );
  }
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
          containsExactPermitIdentifier(candidate.text, normalized),
      )
    : undefined;
  if (!isJsonObject(suggestion) || typeof suggestion.id !== "string") {
    throw new PolkPermitNotFoundError(
      `Lake Wales permit ${normalized} returned no exact result`,
    );
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
 * @param {number} [timeoutMs] Per-request timeout in milliseconds.
 * @returns {Promise<{url:string,detail:PolkAccelaPermitDetail}>} Parsed detail.
 */
export async function fetchPolkPermitAdapterDetail(
  adapter,
  permitNumber,
  fetchImplementation = fetch,
  timeoutMs = 30_000,
) {
  const timedFetch = createPolkPermitTimedFetch(fetchImplementation, timeoutMs);
  if (adapter === "polk_accela_cap_detail_v1") {
    const fetched = await fetchPolkAccelaPermitDetail(permitNumber, timedFetch);
    return {
      url: fetched.url,
      detail: parsePolkPermitAdapterHtml(adapter, fetched.html, permitNumber),
    };
  }
  if (adapter === "lakeland_ims_permit_detail_v1") {
    const fetched = await fetchLakelandImsPermitDetail(
      permitNumber,
      timedFetch,
    );
    return {
      url: fetched.url,
      detail: parsePolkPermitAdapterHtml(adapter, fetched.html, permitNumber),
    };
  }
  if (adapter === "winter_haven_esuite_permit_detail_v1") {
    const fetched = await fetchWinterHavenPermitDetail(
      permitNumber,
      timedFetch,
    );
    return {
      url: fetched.url,
      detail: parsePolkPermitAdapterHtml(adapter, fetched.html, permitNumber),
    };
  }
  if (adapter === "lake_wales_citizenlink_permit_detail_v1") {
    const fetched = await fetchLakeWalesPermitDetail(permitNumber, timedFetch);
    const detail = parsePolkPermitAdapterHtml(
      adapter,
      fetched.html,
      permitNumber,
    );
    return {
      url: fetched.url,
      detail: {
        ...detail,
        // The preceding AJAX lookup selected an exact permit-number result.
        permitNumber: detail.permitNumber ?? permitNumber,
      },
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
    const source = registryByAgency.get(agencyRow.value.trim().toUpperCase());
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
  const candidateInputComplete =
    run.inputRecordCount > 0 &&
    run.invalidRecordCount === 0 &&
    run.fetchErrorCount === 0 &&
    (run.unsupportedRecordCount ?? 0) === 0 &&
    run.supportedRecordCount + (run.partialAdapterAttemptedRecordCount ?? 0) ===
      run.inputRecordCount &&
    run.enrichedRecordCount + (run.partialAdapterEnrichedRecordCount ?? 0) ===
      run.inputRecordCount;
  const countyCoverageComplete =
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
    candidateInputComplete,
    countyCoverageComplete,
    complete: countyCoverageComplete,
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
 * @property {number} timeoutMs Per-request timeout in milliseconds.
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
 * @typedef {object} PolkPermitCheckpoint
 * @property {string} schemaVersion Checkpoint schema identifier.
 * @property {string} updatedAt Last atomic update time.
 * @property {string} input Candidate JSONL path.
 * @property {string} output Aggregate JSONL path.
 * @property {string} stateDirectory Deterministic part directory.
 * @property {number} batchSize Records per part.
 * @property {number} completedPartCount Contiguous verified parts.
 * @property {number} totalPartCount Expected parts.
 * @property {number} processedInputRecordCount Candidate rows represented by parts.
 * @property {number} inputRecordCount Total non-empty input rows.
 * @property {string | undefined} [inputFingerprint] SHA-256 of normalized candidates.
 * @property {string | undefined} [adapterContractFingerprint] SHA-256 of adapter eligibility and parsing contract.
 * @property {boolean | undefined} [includePartial] Whether partial adapters were eligible.
 * @property {boolean | undefined} [aggregateComplete] Whether output was atomically assembled.
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
      if (lastError instanceof PolkPermitNotFoundError) throw lastError;
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
export function createPolkPermitSourceScheduler(delayMs) {
  /** @type {Map<string, Promise<void>>} */
  const sourceTails = new Map();
  /** @type {Map<string, number>} */
  const sourceNextStart = new Map();
  return async (sourceKey, operation) => {
    const previous = sourceTails.get(sourceKey) ?? Promise.resolve();
    const scheduled = previous
      .catch(() => undefined)
      .then(async () => {
        const waitMs = Math.max(
          0,
          (sourceNextStart.get(sourceKey) ?? 0) - Date.now(),
        );
        if (waitMs > 0) await sleep(waitMs);
        sourceNextStart.set(sourceKey, Date.now() + delayMs);
        return operation();
      });
    sourceTails.set(
      sourceKey,
      scheduled.then(
        () => undefined,
        () => undefined,
      ),
    );
    return scheduled;
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
 * Fingerprint normalized candidate identities without depending on file metadata.
 *
 * @param {readonly PolkPermitWorkItem[]} workItems Ordered candidate work.
 * @returns {string} Lowercase SHA-256 digest.
 */
function fingerprintPolkPermitWorkItems(workItems) {
  const hash = createHash("sha256");
  for (const item of workItems) {
    if (item.candidate === null) {
      hash.update("<invalid>\n");
      continue;
    }
    hash.update(item.candidate.agency);
    hash.update("\0");
    hash.update(item.candidate.permitNumber);
    hash.update("\n");
  }
  return hash.digest("hex");
}

/**
 * Pin adapter eligibility and parsing semantics across resumptions.
 *
 * @param {boolean} includePartial Whether partial adapters may run.
 * @returns {string} Lowercase SHA-256 digest.
 */
function fingerprintPolkPermitAdapterContract(includePartial) {
  return createHash("sha256")
    .update(POLK_PERMIT_ADAPTER_CONTRACT_VERSION)
    .update("\n")
    .update(
      JSON.stringify(
        POLK_PERMIT_SOURCE_REGISTRY.map((source) => ({
          key: source.key,
          agency: source.agency,
          status: source.status,
          adapter: source.adapter,
        })),
      ),
    )
    .update(`\nincludePartial=${String(includePartial)}`)
    .digest("hex");
}

/**
 * Read and structurally validate a permit checkpoint when present.
 *
 * @param {string} checkpointPath Checkpoint path.
 * @returns {Promise<PolkPermitCheckpoint | null>} Parsed checkpoint or null.
 */
async function readPolkPermitCheckpoint(checkpointPath) {
  let value;
  try {
    value = /** @type {unknown} */ (
      JSON.parse(await readFile(checkpointPath, "utf8"))
    );
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
  if (
    !isJsonObject(value) ||
    typeof value.schemaVersion !== "string" ||
    typeof value.updatedAt !== "string" ||
    typeof value.input !== "string" ||
    typeof value.output !== "string" ||
    typeof value.stateDirectory !== "string" ||
    typeof value.batchSize !== "number" ||
    typeof value.completedPartCount !== "number" ||
    typeof value.totalPartCount !== "number" ||
    typeof value.processedInputRecordCount !== "number" ||
    typeof value.inputRecordCount !== "number" ||
    (value.inputFingerprint !== undefined &&
      typeof value.inputFingerprint !== "string") ||
    (value.adapterContractFingerprint !== undefined &&
      typeof value.adapterContractFingerprint !== "string") ||
    (value.includePartial !== undefined &&
      typeof value.includePartial !== "boolean") ||
    (value.aggregateComplete !== undefined &&
      typeof value.aggregateComplete !== "boolean")
  ) {
    throw new Error(`Invalid Polk permit checkpoint: ${checkpointPath}`);
  }
  return /** @type {PolkPermitCheckpoint} */ (value);
}

/**
 * Reject reuse of parts against a changed candidate contract.
 *
 * Version 1 checkpoints are accepted only when all fields they carried match;
 * the next write upgrades them with an input fingerprint.
 *
 * @param {PolkPermitCheckpoint} checkpoint Persisted checkpoint.
 * @param {{input:string,output:string,stateDirectory:string,batchSize:number,totalPartCount:number,inputRecordCount:number,inputFingerprint:string,adapterContractFingerprint:string,includePartial:boolean}} expected Current run identity.
 * @returns {void}
 */
function assertPolkPermitCheckpointCompatible(checkpoint, expected) {
  const numericFields = [
    checkpoint.batchSize,
    checkpoint.completedPartCount,
    checkpoint.totalPartCount,
    checkpoint.processedInputRecordCount,
    checkpoint.inputRecordCount,
  ];
  const incompatible =
    !numericFields.every(
      (value) => Number.isSafeInteger(value) && value >= 0,
    ) ||
    checkpoint.completedPartCount > checkpoint.totalPartCount ||
    checkpoint.processedInputRecordCount > checkpoint.inputRecordCount ||
    checkpoint.input !== expected.input ||
    checkpoint.output !== expected.output ||
    checkpoint.stateDirectory !== expected.stateDirectory ||
    checkpoint.batchSize !== expected.batchSize ||
    checkpoint.totalPartCount !== expected.totalPartCount ||
    checkpoint.inputRecordCount !== expected.inputRecordCount ||
    (checkpoint.inputFingerprint !== undefined &&
      checkpoint.inputFingerprint !== expected.inputFingerprint) ||
    (checkpoint.adapterContractFingerprint !== undefined &&
      checkpoint.adapterContractFingerprint !==
        expected.adapterContractFingerprint) ||
    (checkpoint.includePartial !== undefined &&
      checkpoint.includePartial !== expected.includePartial);
  if (incompatible) {
    throw new Error(
      "Polk permit checkpoint is incompatible with the current input or run settings; use a new state directory or explicitly reset the checkpoint.",
    );
  }
}

/**
 * Prevent concurrent writers from sharing one deterministic part directory.
 *
 * A stale lock left by a terminated process is removed only after its PID is
 * confirmed absent. The returned release operation is safe to call repeatedly.
 *
 * @param {string} stateDirectory Deterministic part directory.
 * @returns {Promise<() => Promise<void>>} Idempotent lock release operation.
 */
async function acquirePolkPermitRunLock(stateDirectory) {
  const lockPath = path.join(stateDirectory, ".run.lock");
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const handle = await open(lockPath, "wx");
      await handle.writeFile(
        `${JSON.stringify({
          pid: process.pid,
          startedAt: new Date().toISOString(),
        })}\n`,
        "utf8",
      );
      let released = false;
      return async () => {
        if (released) return;
        released = true;
        await handle.close();
        await rm(lockPath, { force: true });
      };
    } catch (caught) {
      if (
        !(caught instanceof Error) ||
        !("code" in caught) ||
        /** @type {NodeJS.ErrnoException} */ (caught).code !== "EEXIST"
      ) {
        throw caught;
      }
      let activePid = null;
      try {
        const lock = /** @type {unknown} */ (
          JSON.parse(await readFile(lockPath, "utf8"))
        );
        activePid =
          isJsonObject(lock) &&
          typeof lock.pid === "number" &&
          Number.isSafeInteger(lock.pid)
            ? lock.pid
            : null;
      } catch {
        activePid = null;
      }
      if (activePid !== null) {
        try {
          process.kill(activePid, 0);
        } catch (processError) {
          if (
            processError instanceof Error &&
            "code" in processError &&
            /** @type {NodeJS.ErrnoException} */ (processError).code === "ESRCH"
          ) {
            await rm(lockPath, { force: true });
            continue;
          }
          throw processError;
        }
        throw new Error(
          `Polk permit state directory is already owned by process ${activePid}`,
        );
      }
      await rm(lockPath, { force: true });
    }
  }
  throw new Error(`Unable to acquire Polk permit run lock at ${lockPath}`);
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
 * Confirm a parsed detail carries evidence beyond its permit identity.
 *
 * @param {PolkAccelaPermitDetail} detail Parsed detail.
 * @returns {boolean} Whether the source exposed a permit-specific field.
 */
function hasPolkPermitPageEvidence(detail) {
  return (
    detail.recordType !== null ||
    detail.recordStatus !== null ||
    detail.parcelIdentifier !== null ||
    detail.workLocation !== null ||
    detail.projectDescription !== null ||
    detail.contractor !== null ||
    detail.jobValuationUsd !== null
  );
}

/**
 * Validate one committed record against its expected source identity and
 * status-specific invariants.
 *
 * @param {unknown} value Parsed part record.
 * @param {{permitNumber:string,agency:string}} expected Expected candidate.
 * @returns {value is PolkPermitEnrichmentRecord} Whether the record is safe to reuse.
 */
function isReusablePolkPermitRecord(value, expected) {
  if (
    !isJsonObject(value) ||
    typeof value.permitNumber !== "string" ||
    typeof value.agency !== "string" ||
    typeof value.sourceKey !== "string" ||
    (typeof value.sourceUrl !== "string" && value.sourceUrl !== null) ||
    typeof value.status !== "string" ||
    typeof value.retrievedAt !== "string" ||
    !Number.isFinite(Date.parse(value.retrievedAt)) ||
    value.permitNumber !== expected.permitNumber ||
    value.agency !== expected.agency ||
    value.sourceKey !==
      (findPolkPermitSource(expected.agency)?.key ?? "unregistered") ||
    (typeof value.error !== "string" && value.error !== null) ||
    (!isJsonObject(value.detail) && value.detail !== null)
  ) {
    return false;
  }
  if (value.status === "enriched") {
    if (
      !isJsonObject(value.detail) ||
      typeof value.detail.permitNumber !== "string" ||
      normalizePermitIdentifier(value.detail.permitNumber) !==
        normalizePermitIdentifier(expected.permitNumber) ||
      value.error !== null
    ) {
      return false;
    }
    return hasPolkPermitPageEvidence(
      /** @type {PolkAccelaPermitDetail} */ (value.detail),
    );
  }
  if (value.status === "fetch_error") {
    return value.detail === null && typeof value.error === "string";
  }
  if (value.status === "unsupported_source") {
    return value.detail === null && typeof value.error === "string";
  }
  if (value.status === "no_detail") {
    if (value.detail === null) return true;
    const detailPermitNumber = value.detail.permitNumber;
    return (
      (detailPermitNumber === null ||
        (typeof detailPermitNumber === "string" &&
          normalizePermitIdentifier(detailPermitNumber) ===
            normalizePermitIdentifier(expected.permitNumber))) &&
      !hasPolkPermitPageEvidence(
        /** @type {PolkAccelaPermitDetail} */ (value.detail),
      )
    );
  }
  return false;
}

/**
 * Convert only deterministic legacy misclassifications that can be repaired
 * without another source request.
 *
 * @param {unknown} value Parsed committed record.
 * @param {{permitNumber:string,agency:string}} expected Expected candidate.
 * @returns {unknown} Original value or a safely reclassified record.
 */
export function repairLegacyPolkPermitRecord(value, expected) {
  if (
    !isJsonObject(value) ||
    typeof value.permitNumber !== "string" ||
    typeof value.agency !== "string" ||
    typeof value.sourceKey !== "string" ||
    value.permitNumber !== expected.permitNumber ||
    value.agency !== expected.agency ||
    value.sourceKey !==
      (findPolkPermitSource(expected.agency)?.key ?? "unregistered")
  ) {
    return value;
  }
  const record = reclassifyLegacyPolkPermitNotFound(
    /** @type {PolkPermitEnrichmentRecord} */ (value),
  );
  if (
    record.status === "enriched" &&
    isJsonObject(record.detail) &&
    typeof record.detail.permitNumber === "string" &&
    normalizePermitIdentifier(record.detail.permitNumber) ===
      normalizePermitIdentifier(expected.permitNumber) &&
    !hasPolkPermitPageEvidence(
      /** @type {PolkAccelaPermitDetail} */ (record.detail),
    )
  ) {
    return {
      ...record,
      status: "no_detail",
      detail: null,
      error: "Legacy result contained no page-derived permit evidence.",
    };
  }
  return record;
}

/**
 * Read and validate an existing deterministic batch part.
 *
 * @param {string} partPath Existing part path.
 * @param {readonly PolkPermitWorkItem[]} workItems Expected work items.
 * @param {boolean} [repairLegacy] Whether deterministic legacy status repairs are allowed.
 * @returns {Promise<{records:PolkPermitEnrichmentRecord[],reclassifiedRecordCount:number} | null>} Valid records or null when absent.
 */
async function readPolkPermitPart(partPath, workItems, repairLegacy = false) {
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
  const parsedRecords = text
    .split(/\r?\n/)
    .filter((line) => line.trim().length > 0)
    .map((line) => /** @type {unknown} */ (JSON.parse(line)));
  const normalizedRecords = parsedRecords.map((record, index) => {
    const candidate = expected[index];
    return repairLegacy && candidate !== undefined
      ? repairLegacyPolkPermitRecord(record, candidate)
      : record;
  });
  if (
    normalizedRecords.length !== expected.length ||
    normalizedRecords.some(
      (record, index) =>
        expected[index] === undefined ||
        !isReusablePolkPermitRecord(record, expected[index]),
    )
  ) {
    throw new Error(`Stale or incomplete permit enrichment part: ${partPath}`);
  }
  return {
    records: /** @type {PolkPermitEnrichmentRecord[]} */ (normalizedRecords),
    reclassifiedRecordCount: normalizedRecords.filter(
      (record, index) => record !== parsedRecords[index],
    ).length,
  };
}

/**
 * Reclassify legacy permanent source misses that older code recorded as
 * retryable failures. The source response already proved the record absent, so
 * this repair requires no new network request.
 *
 * @param {PolkPermitEnrichmentRecord} record Existing committed record.
 * @returns {PolkPermitEnrichmentRecord} Original or reclassified record.
 */
export function reclassifyLegacyPolkPermitNotFound(record) {
  const permanentMiss =
    record.status === "fetch_error" &&
    typeof record.error === "string" &&
    (record.error.includes("returned no detail link") ||
      record.error.includes("returned no exact result") ||
      record.error.includes("did not resolve to a detail page"));
  return permanentMiss
    ? {
        ...record,
        status: "no_detail",
      }
    : record;
}

/**
 * Verify every contiguous committed part and reject skipped part indexes.
 *
 * @param {string} stateDirectory Deterministic part directory.
 * @param {readonly PolkPermitWorkItem[]} workItems Ordered candidate work.
 * @param {number} batchSize Records per part.
 * @param {boolean} [repairLegacy] Whether deterministic legacy status repairs are allowed.
 * @returns {Promise<{partPath:string,records:PolkPermitEnrichmentRecord[],reclassifiedRecordCount:number}[]>} Verified contiguous parts.
 */
async function verifyCommittedPolkPermitParts(
  stateDirectory,
  workItems,
  batchSize,
  repairLegacy = false,
) {
  const entries = await readdir(stateDirectory);
  const indexedParts = new Map(
    entries.flatMap((entry) => {
      const match = /^part-(\d{6})\.jsonl$/.exec(entry);
      return match?.[1] === undefined
        ? []
        : [[Number.parseInt(match[1], 10), entry]];
    }),
  );
  const totalPartCount = Math.ceil(workItems.length / batchSize);
  for (const partIndex of indexedParts.keys()) {
    if (partIndex >= totalPartCount) {
      throw new Error(
        `Permit part index ${partIndex} exceeds expected part count ${totalPartCount}`,
      );
    }
  }
  /** @type {{partPath:string,records:PolkPermitEnrichmentRecord[],reclassifiedRecordCount:number}[]} */
  const verified = [];
  for (let partIndex = 0; partIndex < totalPartCount; partIndex += 1) {
    const entry = indexedParts.get(partIndex);
    if (entry === undefined) {
      const laterPart = [...indexedParts.keys()].find(
        (candidate) => candidate > partIndex,
      );
      if (laterPart !== undefined) {
        throw new Error(
          `Permit part sequence has a gap at ${partIndex} before committed part ${laterPart}`,
        );
      }
      break;
    }
    const offset = partIndex * batchSize;
    const partPath = path.join(stateDirectory, entry);
    const records = await readPolkPermitPart(
      partPath,
      workItems.slice(offset, offset + batchSize),
      repairLegacy,
    );
    if (records === null) {
      throw new Error(`Committed permit part disappeared: ${partPath}`);
    }
    verified.push({
      partPath,
      records: records.records,
      reclassifiedRecordCount: records.reclassifiedRecordCount,
    });
  }
  return verified;
}

/**
 * Atomically persist monotonic permit progress.
 *
 * @param {string} checkpointPath Checkpoint destination.
 * @param {{input:string,output:string,stateDirectory:string,batchSize:number,completedPartCount:number,totalPartCount:number,processedInputRecordCount:number,inputRecordCount:number,inputFingerprint:string,adapterContractFingerprint:string,includePartial:boolean,aggregateComplete:boolean}} checkpoint Checkpoint fields.
 * @returns {Promise<void>} Resolves after atomic replacement.
 */
function writePolkPermitCheckpoint(checkpointPath, checkpoint) {
  return writePolkPermitAtomicText(
    checkpointPath,
    `${JSON.stringify(
      {
        schemaVersion: "oracle-node.polk-permit-enrichment-checkpoint.v2",
        updatedAt: new Date().toISOString(),
        ...checkpoint,
      },
      null,
      2,
    )}\n`,
  );
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
  if (
    source.key === "winter_haven_tyler_esuite" &&
    !isWinterHavenHistoricalPermitNumber(candidate.permitNumber)
  ) {
    return {
      permitNumber: candidate.permitNumber,
      agency: candidate.agency,
      sourceKey: source.key,
      sourceUrl: source.searchUrl,
      status: "unsupported_source",
      detail: null,
      error:
        "Winter Haven eSuite is certified only for YYYY-NNNNNNNN historical identifiers.",
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
                fetch,
                settings.timeoutMs,
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
    const exactPermitMatch =
      detail.permitNumber !== null &&
      normalizePermitIdentifier(detail.permitNumber) ===
        normalizePermitIdentifier(candidate.permitNumber);
    const hasPageEvidence =
      detail.recordType !== null ||
      detail.recordStatus !== null ||
      detail.parcelIdentifier !== null ||
      detail.workLocation !== null ||
      detail.projectDescription !== null ||
      detail.contractor !== null ||
      detail.jobValuationUsd !== null;
    const hasEvidence = exactPermitMatch && hasPageEvidence;
    return {
      permitNumber: candidate.permitNumber,
      agency: candidate.agency,
      sourceKey: source.key,
      sourceUrl: fetched.url,
      status: hasEvidence ? "enriched" : "no_detail",
      detail: hasEvidence ? detail : null,
      error: hasEvidence
        ? null
        : detail.permitNumber === null
          ? "Public detail did not expose the requested permit identifier."
          : !exactPermitMatch
            ? `Public detail returned permit ${detail.permitNumber} instead of ${candidate.permitNumber}.`
            : "Public detail exposed no permit-specific evidence.",
      retrievedAt: new Date().toISOString(),
    };
  } catch (caught) {
    const notFound = caught instanceof PolkPermitNotFoundError;
    return {
      permitNumber: candidate.permitNumber,
      agency: candidate.agency,
      sourceKey: source.key,
      sourceUrl: buildPolkPermitAdapterUrl(source, candidate.permitNumber),
      status: notFound ? "no_detail" : "fetch_error",
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
    const attempted = record.status !== "unsupported_source";
    if (attempted && source?.status === "adapter_ready")
      counters.supportedRecordCount += 1;
    if (attempted && source?.status === "partial_adapter_ready")
      counters.partialAdapterAttemptedRecordCount += 1;
    if (record.status === "enriched") {
      if (source?.status === "adapter_ready") counters.enrichedRecordCount += 1;
      if (source?.status === "partial_adapter_ready")
        counters.partialAdapterEnrichedRecordCount += 1;
      if (record.detail?.contractor !== null)
        counters.contractorEvidenceCount += 1;
      if (
        typeof record.detail?.contractor?.licenseNumber === "string" &&
        record.detail.contractor.licenseNumber.length > 0
      )
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
      "timeout-ms": { type: "string" },
      "state-dir": { type: "string" },
      checkpoint: { type: "string" },
      "reset-checkpoint": { type: "boolean" },
      "redrive-errors": { type: "boolean" },
      "approve-scale": { type: "boolean" },
      "winter-haven-historical": { type: "boolean" },
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
    const winterHavenHistoricalOnly =
      values["winter-haven-historical"] === true;
    const agencies = Array.isArray(values.agency)
      ? values.agency.map(String)
      : winterHavenHistoricalOnly
        ? ["WINTER HAVEN"]
        : POLK_PERMIT_SOURCE_REGISTRY.filter(
            (source) => source.status === "adapter_ready",
          ).map((source) => source.agency);
    if (
      winterHavenHistoricalOnly &&
      agencies.some((agency) => agency.trim().toUpperCase() !== "WINTER HAVEN")
    ) {
      throw new Error(
        "--winter-haven-historical can only be used with WINTER HAVEN",
      );
    }
    return writePolkPermitAdapterCandidates({
      workDatabase:
        typeof values["work-db"] === "string"
          ? values["work-db"]
          : "tmp/polk/bulk/extracted/polk-appraisal.duckdb",
      output: input,
      agencies,
      limit,
      winterHavenHistoricalOnly,
    });
  }
  if (
    stage !== "enrich" &&
    stage !== "verify" &&
    stage !== "repair" &&
    stage !== "redrive"
  ) {
    throw new Error(
      "--stage must be candidates, verify, repair, redrive, or enrich",
    );
  }
  const redriveRequested =
    stage === "redrive" || values["redrive-errors"] === true;
  if (stage === "repair" && redriveRequested) {
    throw new Error(
      "--stage repair is local-only and cannot be combined with --redrive-errors",
    );
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
    timeoutMs: readBoundedPositiveInteger(
      values["timeout-ms"],
      "timeout-ms",
      30_000,
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
  await Promise.all([
    mkdir(path.dirname(output), { recursive: true }),
    mkdir(path.dirname(receiptPath), { recursive: true }),
    mkdir(stateDirectory, { recursive: true }),
    mkdir(path.dirname(checkpointPath), { recursive: true }),
  ]);
  const releaseRunLock = await acquirePolkPermitRunLock(stateDirectory);
  try {
    if (values["reset-checkpoint"] === true) {
      const committedParts = (await readdir(stateDirectory)).filter((entry) =>
        /^part-\d{6}\.jsonl$/.test(entry),
      );
      let checkpointExists = true;
      try {
        await readFile(checkpointPath, "utf8");
      } catch (caught) {
        if (
          caught instanceof Error &&
          "code" in caught &&
          /** @type {NodeJS.ErrnoException} */ (caught).code === "ENOENT"
        ) {
          checkpointExists = false;
        } else {
          throw caught;
        }
      }
      if (committedParts.length > 0 || checkpointExists) {
        throw new Error(
          "--reset-checkpoint refuses to delete committed Polk permit work; use a new output and state directory.",
        );
      }
    }
    const workItems = await readPolkPermitWorkItems(input);
    const invalidRecordCount = workItems.filter(
      (item) => item.candidate === null,
    ).length;
    if (invalidRecordCount > 0) {
      throw new Error(
        `Permit candidate input contains ${invalidRecordCount} invalid non-empty JSONL record${invalidRecordCount === 1 ? "" : "s"}; no records were processed.`,
      );
    }
    if (
      stage === "enrich" &&
      !redriveRequested &&
      workItems.length > 100 &&
      values["approve-scale"] !== true
    ) {
      throw new Error(
        `Polk permit enrichment has ${workItems.length} candidates; --approve-scale is required after a documented GO decision.`,
      );
    }
    const inputFingerprint = fingerprintPolkPermitWorkItems(workItems);
    const adapterContractFingerprint = fingerprintPolkPermitAdapterContract(
      settings.includePartial,
    );
    /** @type {PolkPermitRunCounters} */
    const counters = {
      inputRecordCount: workItems.length,
      invalidRecordCount: 0,
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
    const checkpoint = await readPolkPermitCheckpoint(checkpointPath);
    if (checkpoint !== null) {
      assertPolkPermitCheckpointCompatible(checkpoint, {
        input,
        output,
        stateDirectory,
        batchSize: settings.batchSize,
        totalPartCount,
        inputRecordCount: workItems.length,
        inputFingerprint,
        adapterContractFingerprint,
        includePartial: settings.includePartial,
      });
    }
    const verifiedParts = await verifyCommittedPolkPermitParts(
      stateDirectory,
      workItems,
      settings.batchSize,
      stage === "repair",
    );
    if (
      checkpoint !== null &&
      checkpoint.completedPartCount > verifiedParts.length
    ) {
      throw new Error(
        `Polk permit checkpoint claims ${checkpoint.completedPartCount} parts but only ${verifiedParts.length} contiguous parts verified.`,
      );
    }
    if (stage === "verify") {
      const records = verifiedParts.flatMap((part) => part.records);
      return {
        schemaVersion: "oracle-node.polk-permit-enrichment-verification.v1",
        verifiedAt: new Date().toISOString(),
        input,
        output,
        stateDirectory,
        checkpoint: checkpointPath,
        checkpointPartCount: checkpoint?.completedPartCount ?? 0,
        verifiedPartCount: verifiedParts.length,
        recoveredPartCount: Math.max(
          0,
          verifiedParts.length - (checkpoint?.completedPartCount ?? 0),
        ),
        totalPartCount,
        verifiedRecordCount: records.length,
        inputRecordCount: workItems.length,
        inputFingerprint,
        statusCounts: Object.fromEntries(
          ["enriched", "no_detail", "unsupported_source", "fetch_error"].map(
            (status) => [
              status,
              records.filter((record) => record.status === status).length,
            ],
          ),
        ),
        complete: verifiedParts.length === totalPartCount,
      };
    }

    /**
     * @param {{permitNumber:string,agency:string}} candidate Permit candidate.
     * @returns {string} Stable duplicate-request key.
     */
    const requestKey = (candidate) =>
      `${candidate.agency}\0${candidate.permitNumber}`;
    /** @type {Map<string, Promise<PolkPermitEnrichmentRecord>>} */
    const resultCache = new Map();
    const statusRank = new Map([
      ["unsupported_source", 1],
      ["no_detail", 2],
      ["enriched", 3],
    ]);
    /** @type {Map<string, PolkPermitEnrichmentRecord>} */
    const cachedRecords = new Map();
    for (const { records } of verifiedParts) {
      for (const record of records) {
        if (record.status === "fetch_error") continue;
        const key = requestKey(record);
        const existing = cachedRecords.get(key);
        if (
          existing === undefined ||
          (statusRank.get(record.status) ?? 0) >
            (statusRank.get(existing.status) ?? 0)
        ) {
          cachedRecords.set(key, record);
        }
      }
    }
    for (const [key, record] of cachedRecords) {
      resultCache.set(key, Promise.resolve(record));
    }
    /**
     * Reuse one source response for duplicate official bulk rows while retaining
     * one output row per input candidate.
     *
     * @param {{permitNumber:string,agency:string}} candidate Candidate record.
     * @returns {Promise<PolkPermitEnrichmentRecord>} Shared enrichment result.
     */
    const enrichWithCache = (candidate) => {
      const key = requestKey(candidate);
      const cached = resultCache.get(key);
      if (cached !== undefined) return cached;
      const pending = enrichPolkPermitCandidate(
        candidate,
        settings,
        scheduleRequest,
      );
      resultCache.set(key, pending);
      void pending.then((record) => {
        if (
          record.status === "fetch_error" &&
          resultCache.get(key) === pending
        ) {
          resultCache.delete(key);
        }
      });
      return pending;
    };

    /** @type {string[]} */
    const partPaths = [];
    let reclassifiedRecordCount = 0;
    let redrivenRecordCount = 0;
    for (let partIndex = 0; partIndex < verifiedParts.length; partIndex += 1) {
      const verified = verifiedParts[partIndex];
      if (verified === undefined) continue;
      const offset = partIndex * settings.batchSize;
      const partItems = workItems.slice(offset, offset + settings.batchSize);
      const validItems = partItems.flatMap((item) =>
        item.candidate === null ? [] : [item.candidate],
      );
      let records = verified.records.map((record) => {
        const repaired = reclassifyLegacyPolkPermitNotFound(record);
        if (repaired !== record) reclassifiedRecordCount += 1;
        return repaired;
      });
      reclassifiedRecordCount += verified.reclassifiedRecordCount;
      const reclassifiedInPart = records.some(
        (record, index) => record !== verified.records[index],
      );
      if (
        redriveRequested &&
        records.some((record) => record.status === "fetch_error")
      ) {
        const redrive = await redrivePolkPermitFetchErrors(
          records,
          validItems,
          settings.concurrency,
          enrichWithCache,
        );
        records = redrive.records;
        redrivenRecordCount += redrive.redrivenCount;
      }
      if (
        verified.reclassifiedRecordCount > 0 ||
        reclassifiedInPart ||
        (redriveRequested &&
          records.some((record, index) => record !== verified.records[index]))
      ) {
        await writePolkPermitAtomicText(
          verified.partPath,
          records.map((record) => JSON.stringify(record)).join("\n") +
            (records.length > 0 ? "\n" : ""),
        );
      }
      verified.records = records;
      partPaths.push(verified.partPath);
      countPolkPermitPart(counters, records);
    }
    let completedPartCount = verifiedParts.length;
    await writePolkPermitCheckpoint(checkpointPath, {
      input,
      output,
      stateDirectory,
      batchSize: settings.batchSize,
      completedPartCount,
      totalPartCount,
      processedInputRecordCount: Math.min(
        workItems.length,
        completedPartCount * settings.batchSize,
      ),
      inputRecordCount: workItems.length,
      inputFingerprint,
      adapterContractFingerprint,
      includePartial: settings.includePartial,
      aggregateComplete:
        completedPartCount === totalPartCount &&
        checkpoint?.aggregateComplete === true &&
        !redriveRequested,
    });
    process.stdout.write(
      `${JSON.stringify({
        event: "polk_permit_enrichment_resume",
        completedPartCount,
        totalPartCount,
        processedInputRecordCount: Math.min(
          workItems.length,
          completedPartCount * settings.batchSize,
        ),
        inputRecordCount: workItems.length,
      })}\n`,
    );
    if (stage === "repair") {
      if (completedPartCount === totalPartCount) {
        await assemblePolkPermitOutput(partPaths, output);
        await writePolkPermitCheckpoint(checkpointPath, {
          input,
          output,
          stateDirectory,
          batchSize: settings.batchSize,
          completedPartCount,
          totalPartCount,
          processedInputRecordCount: workItems.length,
          inputRecordCount: workItems.length,
          inputFingerprint,
          adapterContractFingerprint,
          includePartial: settings.includePartial,
          aggregateComplete: true,
        });
      }
      return {
        schemaVersion: "oracle-node.polk-permit-enrichment-repair.v1",
        repairedAt: new Date().toISOString(),
        input,
        output,
        stateDirectory,
        checkpoint: checkpointPath,
        reclassifiedRecordCount,
        completedPartCount,
        totalPartCount,
        processedInputRecordCount: Math.min(
          workItems.length,
          completedPartCount * settings.batchSize,
        ),
        ...counters,
        complete: completedPartCount === totalPartCount,
      };
    }
    if (redriveRequested) {
      if (completedPartCount === totalPartCount) {
        await assemblePolkPermitOutput(partPaths, output);
        await writePolkPermitCheckpoint(checkpointPath, {
          input,
          output,
          stateDirectory,
          batchSize: settings.batchSize,
          completedPartCount,
          totalPartCount,
          processedInputRecordCount: workItems.length,
          inputRecordCount: workItems.length,
          inputFingerprint,
          adapterContractFingerprint,
          includePartial: settings.includePartial,
          aggregateComplete: true,
        });
      }
      return {
        schemaVersion: "oracle-node.polk-permit-enrichment-redrive.v1",
        redrivenAt: new Date().toISOString(),
        input,
        output,
        stateDirectory,
        checkpoint: checkpointPath,
        reclassifiedRecordCount,
        redrivenRecordCount,
        completedPartCount,
        totalPartCount,
        processedInputRecordCount: Math.min(
          workItems.length,
          completedPartCount * settings.batchSize,
        ),
        ...counters,
        complete: completedPartCount === totalPartCount,
      };
    }

    for (
      let partIndex = completedPartCount;
      partIndex < totalPartCount;
      partIndex += 1
    ) {
      const offset = partIndex * settings.batchSize;
      const partItems = workItems.slice(offset, offset + settings.batchSize);
      const partPath = path.join(
        stateDirectory,
        `part-${String(partIndex).padStart(6, "0")}.jsonl`,
      );
      const validItems = partItems.flatMap((item) =>
        item.candidate === null ? [] : [item.candidate],
      );
      const records = await mapPolkPermitWithConcurrency(
        validItems,
        settings.concurrency,
        enrichWithCache,
      );
      await writePolkPermitAtomicText(
        partPath,
        records.map((record) => JSON.stringify(record)).join("\n") +
          (records.length > 0 ? "\n" : ""),
      );
      partPaths.push(partPath);
      countPolkPermitPart(counters, records);
      completedPartCount = partIndex + 1;
      await writePolkPermitCheckpoint(checkpointPath, {
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
        inputFingerprint,
        adapterContractFingerprint,
        includePartial: settings.includePartial,
        aggregateComplete: false,
      });
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
    await writePolkPermitCheckpoint(checkpointPath, {
      input,
      output,
      stateDirectory,
      batchSize: settings.batchSize,
      completedPartCount,
      totalPartCount,
      processedInputRecordCount: workItems.length,
      inputRecordCount: workItems.length,
      inputFingerprint,
      adapterContractFingerprint,
      includePartial: settings.includePartial,
      aggregateComplete: true,
    });
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
    await writePolkPermitAtomicText(
      receiptPath,
      `${JSON.stringify(receipt, null, 2)}\n`,
    );
    return receipt;
  } finally {
    await releaseRunLock();
  }
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
