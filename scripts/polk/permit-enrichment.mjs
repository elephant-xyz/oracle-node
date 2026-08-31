#!/usr/bin/env node

import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * @typedef {"official_bulk" | "accela" | "ims" | "tyler_esuite" | "iworq" | "municipal_portal" | "none_verified"} PolkPermitPortalKind
 */

/**
 * @typedef {"bulk_only" | "adapter_ready" | "portal_verified_adapter_pending" | "no_public_detail_source_verified"} PolkPermitSourceStatus
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
    status: "portal_verified_adapter_pending",
    officialUrl:
      "https://www.lakelandgov.net/departments/community-economic-development/building-inspection/ims/",
    searchUrl: "https://ims.lakelandgov.net/ims/Account/Login",
    adapter: null,
    evidence:
      "Lakeland officially replaced eTRAKiT with iMS in 2024 and advertises anonymous guest search; no stable anonymous detail request contract has been certified.",
    verifiedAt: "2026-08-31",
  },
  {
    key: "winter_haven_tyler_esuite",
    agency: "WINTER HAVEN",
    portalKind: "tyler_esuite",
    status: "portal_verified_adapter_pending",
    officialUrl: "https://www.mywinterhaven.com/342/Building-Permits-Licenses",
    searchUrl:
      "https://myinspections.mywinterhaven.com/eSuite.Permits/AdvancedSearchPage/AdvancedSearch.aspx",
    adapter: null,
    evidence:
      "The official Tyler eSuite public search exposes permit and inspection lookup, but its stateful form/detail protocol has not been certified for unattended use.",
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
      "The official city Development Services page links the iWorQ contractor portal; anonymous permit-detail requests have not been certified.",
    verifiedAt: "2026-08-31",
  },
  {
    key: "lake_wales_public_view",
    agency: "LAKE WALES",
    portalKind: "municipal_portal",
    status: "portal_verified_adapter_pending",
    officialUrl: "https://www.lakewalesfl.gov/909/Contractor-Online-Portal",
    searchUrl: "https://secure.lakewalesfl.gov/permits/",
    adapter: null,
    evidence:
      "The official city page documents a public permit-number/address view, but the portal was offline during verification and no request protocol was certified.",
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
    const licenseToken = firstCapture(
      contractorRaw,
      /\b([A-Z]{2,4}\s*[: -]?\s*\d{5,10})\b/i,
    );
    const licenseNumber =
      licenseToken === null
        ? null
        : licenseToken.toUpperCase().replace(/[^A-Z0-9]/g, "");
    const email =
      firstCapture(
        contractorRaw,
        /\b([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})\b/i,
      )?.toLowerCase() ?? null;
    const phone = normalizePhone(
      firstCapture(contractorRaw, /(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})/),
    );
    const licenseType =
      licenseToken === null
        ? null
        : firstCapture(
            contractorRaw,
            new RegExp(
              `([A-Za-z][A-Za-z ]{2,50})\\s+${licenseToken.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`,
              "i",
            ),
          );
    const businessSearchText =
      email === null
        ? contractorRaw
        : contractorRaw.slice(
            contractorRaw.toLowerCase().indexOf(email) + email.length,
          );
    const businessName =
      firstCapture(
        businessSearchText,
        /\b([A-Z0-9][A-Z0-9 &'.,-]+?(?:LLC|INC|CORP(?:ORATION)?|COMPANY|CO))\b/i,
      ) ??
      firstCapture(
        businessSearchText,
        /\b([A-Z0-9][A-Z0-9 &'.,-]+?(?:CONSTRUCTION|CONTRACTING|ROOFING|SERVICES))\b/i,
      );
    const contactName =
      businessName === null
        ? firstCapture(
            contractorRaw,
            /^([A-Z][A-Z .'-]{2,60}?)(?:\s+[A-Z0-9._%+-]+@|\s+[A-Z]{2,4}\d{5})/i,
          )
        : firstCapture(
            contractorRaw,
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
 * @param {{inputRecordCount:number,invalidRecordCount:number,supportedRecordCount:number,enrichedRecordCount:number,contractorEvidenceCount:number,licenseEvidenceCount:number,fetchErrorCount:number,networkUsed:boolean,input:string,output:string}} run Streaming adapter counters.
 * @returns {JsonObject} Countywide receipt.
 */
export function buildPolkPermitEnrichmentReceiptFromRun(permitSummary, run) {
  const { adapterEligiblePermitCount, unsupportedPermitCount, agencyCoverage } =
    buildPolkPermitAgencyCoverage(permitSummary);
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
    enrichedRecordCount: run.enrichedRecordCount,
    contractorEvidenceCount: run.contractorEvidenceCount,
    licenseEvidenceCount: run.licenseEvidenceCount,
    invalidRecordCount: run.invalidRecordCount,
    fetchErrorCount: run.fetchErrorCount,
    networkUsed: run.networkUsed,
    input: run.input,
    output: run.output,
    agencyCoverage,
    complete,
    blocker:
      unsupportedPermitCount > 0
        ? `${unsupportedPermitCount} official bulk permit rows belong to missing/unregistered agencies or agencies without certified anonymous adapters.`
        : run.supportedRecordCount !== adapterEligiblePermitCount
          ? `${adapterEligiblePermitCount - run.supportedRecordCount} adapter-eligible permit rows have not been attempted.`
          : run.enrichedRecordCount !== adapterEligiblePermitCount
            ? `${adapterEligiblePermitCount - run.enrichedRecordCount} adapter-eligible permit rows lack public detail evidence.`
            : run.fetchErrorCount > 0 || run.invalidRecordCount > 0
              ? "The adapter run contains invalid inputs or fetch failures."
              : null,
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
  const attemptedAdapterRecords = records.filter(
    (record) => record.sourceKey === "polk_county_accela",
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
      input: { type: "string" },
      output: { type: "string" },
      receipt: { type: "string" },
      "permit-summary": { type: "string" },
      "html-dir": { type: "string" },
      network: { type: "boolean" },
    },
    strict: true,
    allowPositionals: false,
  });
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
  await Promise.all([
    mkdir(path.dirname(output), { recursive: true }),
    mkdir(path.dirname(receiptPath), { recursive: true }),
  ]);
  const writer = createWriteStream(output, { encoding: "utf8" });
  const reader = createInterface({
    input: createReadStream(input, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });
  let inputRecordCount = 0;
  let invalidRecordCount = 0;
  let supportedRecordCount = 0;
  let enrichedRecordCount = 0;
  let contractorEvidenceCount = 0;
  let licenseEvidenceCount = 0;
  let fetchErrorCount = 0;
  for await (const line of reader) {
    if (line.trim().length === 0) continue;
    inputRecordCount += 1;
    let candidate;
    try {
      candidate = permitCandidate(JSON.parse(line));
    } catch {
      candidate = null;
    }
    if (candidate === null) {
      invalidRecordCount += 1;
      continue;
    }
    const source = findPolkPermitSource(candidate.agency);
    /** @type {PolkPermitEnrichmentRecord} */
    let result;
    if (source?.adapter !== "polk_accela_cap_detail_v1") {
      result = {
        permitNumber: candidate.permitNumber,
        agency: candidate.agency,
        sourceKey: source?.key ?? "unregistered",
        sourceUrl: source?.searchUrl ?? null,
        status: "unsupported_source",
        detail: null,
        error: source?.evidence ?? "Agency is not registered.",
        retrievedAt: new Date().toISOString(),
      };
    } else {
      supportedRecordCount += 1;
      try {
        const fetched =
          values.network === true
            ? await fetchPolkAccelaPermitDetail(candidate.permitNumber)
            : {
                url: buildPolkAccelaDetailUrl(candidate.permitNumber),
                html: await readFile(
                  path.join(
                    htmlDirectory,
                    `${candidate.permitNumber.replace(/[^A-Z0-9_-]/gi, "_")}.html`,
                  ),
                  "utf8",
                ),
              };
        const detail = parsePolkAccelaPermitDetailHtml(fetched.html);
        const hasEvidence =
          detail.permitNumber !== null ||
          detail.recordStatus !== null ||
          detail.parcelIdentifier !== null ||
          detail.contractor !== null ||
          detail.jobValuationUsd !== null;
        if (hasEvidence) {
          enrichedRecordCount += 1;
          if (detail.contractor !== null) contractorEvidenceCount += 1;
          if (detail.contractor?.licenseNumber !== null)
            licenseEvidenceCount += 1;
        }
        result = {
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
        fetchErrorCount += 1;
        result = {
          permitNumber: candidate.permitNumber,
          agency: candidate.agency,
          sourceKey: source.key,
          sourceUrl: buildPolkAccelaDetailUrl(candidate.permitNumber),
          status: "fetch_error",
          detail: null,
          error: caught instanceof Error ? caught.message : String(caught),
          retrievedAt: new Date().toISOString(),
        };
      }
    }
    if (!writer.write(`${JSON.stringify(result)}\n`)) {
      await new Promise((resolve) => writer.once("drain", resolve));
    }
  }
  await new Promise((resolve, reject) => {
    writer.once("error", reject);
    writer.end(resolve);
  });
  const run = {
    input,
    output,
    networkUsed: values.network === true,
    inputRecordCount,
    invalidRecordCount,
    supportedRecordCount,
    enrichedRecordCount,
    contractorEvidenceCount,
    licenseEvidenceCount,
    fetchErrorCount,
    complete:
      inputRecordCount > 0 &&
      invalidRecordCount === 0 &&
      supportedRecordCount === inputRecordCount &&
      enrichedRecordCount === supportedRecordCount,
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
