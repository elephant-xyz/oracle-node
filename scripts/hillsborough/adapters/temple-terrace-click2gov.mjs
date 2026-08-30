/**
 * @fileoverview Temple Terrace Click2Gov Permit Portal Adapter.
 * 
 * Fetches and parses permit status details from the City of Temple Terrace
 * Click2Gov Building Permit system (temp-egov.aspgov.com).
 */

import * as cheerio from "cheerio";

/**
 * @typedef {object} Click2GovContractorInfo
 * @property {string} businessName
 * @property {string | null} licenseNumber
 * @property {string | null} qualifierName
 * @property {string | null} phone
 * @property {string | null} email
 */

/**
 * @typedef {object} Click2GovPermitDetail
 * @property {string} permitNumber
 * @property {string | null} parcelIdentifier
 * @property {string | null} workLocation
 * @property {string | null} ownerName
 * @property {string | null} applicationDate
 * @property {string | null} applicationType
 * @property {string | null} recordStatus
 * @property {number | null} jobValuation
 * @property {number | null} squareFeet
 * @property {string | null} zoningDescription
 * @property {Click2GovContractorInfo | null} contractor
 * @property {boolean} isRoofPermit
 */

/**
 * @typedef {object} Click2GovFetchResult
 * @property {Click2GovPermitDetail | null} data
 * @property {string} status - "ok" | "not_found" | "fetch_error" | "parse_error"
 * @property {string | null} error
 */

/**
 * Parse raw dollar valuation string into numeric value.
 * @param {string | null | undefined} raw
 * @returns {number | null}
 */
export function parseClick2GovValuation(raw) {
  if (!raw) return null;
  const clean = raw.replace(/[$,\s]/g, "");
  const val = parseFloat(clean);
  return Number.isFinite(val) && val >= 0 ? val : null;
}

/**
 * Parse raw square footage string into numeric value.
 * @param {string | null | undefined} raw
 * @returns {number | null}
 */
export function parseClick2GovSquareFeet(raw) {
  if (!raw) return null;
  const clean = raw.replace(/[^\d.]/g, "");
  const val = parseFloat(clean);
  return Number.isFinite(val) && val > 0 ? val : null;
}

/**
 * Parse Application Number components from permit string.
 * Supports:
 * - "TT-18-1413" -> { appYear: "18", appNumber: "1413" }
 * - "TT-24-817" -> { appYear: "24", appNumber: "817" }
 * - "TT-08-0723" -> { appYear: "08", appNumber: "723" }
 * - "TT-64926" -> null (Accela legacy ID mislabeled with TT prefix)
 * @param {string} permitNum
 * @returns {{ appYear: string, appNumber: string } | null}
 */
export function parseClick2GovAppYearAndNumber(permitNum) {
  if (!permitNum) return null;
  const clean = permitNum.replace(/^TT-/i, "").trim();
  const parts = clean.split("-");
  if (parts.length >= 2) {
    const yr = parts[0].replace(/^0+/, "") || "0";
    const paddedYr = parts[0].length === 1 ? `0${parts[0]}` : parts[0];
    const num = parts[1].replace(/^0+/, "") || "0";
    if (paddedYr.length === 2 && num.length >= 1) {
      return {
        appYear: paddedYr,
        appNumber: num,
      };
    }
  }
  return null;
}

/**
 * Parse Temple Terrace Click2Gov Status Detail HTML.
 * @param {string} html
 * @param {string} fallbackPermitNumber
 * @returns {Click2GovPermitDetail | null}
 */
export function parseClick2GovStatusDetailHtml(html, fallbackPermitNumber) {
  if (!html || typeof html !== "string") return null;

  const $ = cheerio.load(html);
  const fields = new Map();

  // Extract label-value pairs from Click2Gov grid layout
  $("label").each((_, labelEl) => {
    const key = $(labelEl).text().replace(/[\*\s]+/g, " ").trim().replace(/:$/, "").toLowerCase();
    const val = $(labelEl).next().find(".form-control-static").text().trim() ||
      $(labelEl).next(".form-control-static").text().trim() ||
      $(labelEl).parent().find(".form-control-static").text().trim();
    if (key && val && !fields.has(key)) {
      fields.set(key, val);
    }
  });

  // Fallback scan: any element with preceding label text ending with ":"
  if (fields.size === 0) {
    $(".form-control-static").each((_, el) => {
      const val = $(el).text().trim();
      const prev = $(el).parent().prev("label").text().trim() || $(el).prev("label").text().trim();
      if (prev) {
        const key = prev.replace(/[\*\s]+/g, " ").trim().replace(/:$/, "").toLowerCase();
        if (!fields.has(key) && val) {
          fields.set(key, val);
        }
      }
    });
  }

  const appNum = fields.get("application number") || fallbackPermitNumber;
  const rawValuation = fields.get("valuation");
  const rawSqFt = fields.get("square footage");
  const appType = fields.get("application type") || null;
  const contractorName = fields.get("general contractor") || null;
  const status = fields.get("application status") || null;
  const owner = fields.get("owner") || null;
  const address = fields.get("address") || null;
  const parcelId = fields.get("parcel id") || null;
  const zoning = fields.get("zoning description") || null;
  const appDate = fields.get("application date") || null;

  const isRoof = Boolean(
    (appType && /roof/i.test(appType)) ||
    (contractorName && /roof/i.test(contractorName))
  );

  /** @type {Click2GovContractorInfo | null} */
  let contractor = null;
  if (contractorName && contractorName.length > 2 && !/owner/i.test(contractorName)) {
    // Check if contractor text contains license number or clean business name
    const licMatch = contractorName.match(/\b(C[A-Z]{2}\d{6,8}|CCC\d+|CBC\d+|CGC\d+)\b/i);
    const lic = licMatch ? licMatch[1].toUpperCase() : null;
    const cleanName = contractorName.replace(/\b(C[A-Z]{2}\d{6,8}|CCC\d+|CBC\d+|CGC\d+)\b/gi, "").trim();

    contractor = {
      businessName: cleanName || contractorName,
      licenseNumber: lic,
      qualifierName: null,
      phone: null,
      email: null,
    };
  }

  return {
    permitNumber: appNum,
    parcelIdentifier: parcelId,
    workLocation: address,
    ownerName: owner,
    applicationDate: appDate,
    applicationType: appType,
    recordStatus: status,
    jobValuation: parseClick2GovValuation(rawValuation),
    squareFeet: parseClick2GovSquareFeet(rawSqFt),
    zoningDescription: zoning,
    contractor,
    isRoofPermit: isRoof,
  };
}

/**
 * Global session pool for Temple Terrace Click2Gov.
 * Click2Gov assigns a single active session state per cookie jar.
 * Using a dedicated session fetch per batch of requests ensures 100% Status Detail yield.
 */
export async function createClick2GovSession(appYear, appNumber) {
  const baseUrl = "https://temp-egov.aspgov.com/Click2GovBP/selectpermit.html";
  const getRes = await fetch(`${baseUrl}?permit.appYearAndNumber=${encodeURIComponent(`${appYear}-${appNumber}`)}`, {
    headers: {
      "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    },
  });

  const cookies = getRes.headers.getSetCookie
    ? getRes.headers.getSetCookie().map((c) => c.split(";")[0]).join("; ")
    : "";

  const getHtml = await getRes.text();
  const $get = cheerio.load(getHtml);
  const csrf = $get('input[name="OWASP_CSRFTOKEN"]').val() || "";

  return { cookies, csrf };
}

/**
 * Fetch Temple Terrace Click2Gov Permit Detail with session pooling.
 * @param {string} permitNumber - e.g. "TT-18-1413" or "18-1413"
 * @param {number} [maxRetries=3]
 * @returns {Promise<Click2GovFetchResult>}
 */
export async function fetchClick2GovPermitDetail(permitNumber, maxRetries = 3) {
  const parsed = parseClick2GovAppYearAndNumber(permitNumber);
  if (!parsed) {
    return {
      data: null,
      status: "parse_error",
      error: `Invalid Temple Terrace permit number format: ${permitNumber}`,
    };
  }

  const { appYear, appNumber } = parsed;
  const baseUrl = "https://temp-egov.aspgov.com/Click2GovBP/selectpermit.html";

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const session = await createClick2GovSession(appYear, appNumber);

      const postParams = new URLSearchParams();
      postParams.set("validatePermitView", "true");
      postParams.set("searchType", "0");
      postParams.set("permit.appYear", appYear);
      postParams.set("permit.appNumber", appNumber);
      postParams.set("finish", "Continue");
      if (session.csrf) postParams.set("OWASP_CSRFTOKEN", session.csrf);

      const postRes = await fetch(baseUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          "Cookie": session.cookies,
          "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
          "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        body: postParams.toString(),
      });

      if (!postRes.ok) {
        if (attempt === maxRetries) {
          return { data: null, status: "fetch_error", error: `POST detail returned HTTP ${postRes.status}` };
        }
        continue;
      }

      const postHtml = await postRes.text();
      if (/no structure found|no records found|invalid permit/i.test(postHtml) && !postHtml.includes("Status Detail")) {
        return { data: null, status: "not_found", error: "Record not found on Temple Terrace portal" };
      }

      const detail = parseClick2GovStatusDetailHtml(postHtml, permitNumber);
      return {
        data: detail,
        status: detail ? "ok" : "parse_error",
        error: detail ? null : "Failed to parse Status Detail fields from HTML",
      };
    } catch (err) {
      if (attempt === maxRetries) {
        return {
          data: null,
          status: "fetch_error",
          error: err instanceof Error ? err.message : String(err),
        };
      }
      await new Promise((r) => setTimeout(r, 200));
    }
  }

  return { data: null, status: "fetch_error", error: "Max retries exceeded" };
}
