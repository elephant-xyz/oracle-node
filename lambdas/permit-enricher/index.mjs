/**
 * AWS Lambda Accela Permit Enrichment Worker.
 * Accepts a batch of permit candidates, scrapes CapDetail pages via Accela Citizen Access,
 * parses contractor licensing, job valuation ($), material, and square footage,
 * and returns the structured enriched records along with structured error classification.
 *
 * @module lambdas/permit-enricher/index
 */

import * as cheerio from "cheerio";
import { fetchClick2GovPermitDetail } from "../../scripts/hillsborough/adapters/temple-terrace-click2gov.mjs";
import { searchMaintStarPermits } from "../../scripts/hillsborough/adapters/plant-city-maintstar.mjs";

/**
 * @typedef {object} ContractorInfo
 * @property {string | null} businessName - Extracted contractor company name.
 * @property {string | null} contactName - Individual qualifier / professional name.
 * @property {string | null} licenseNumber - State license ID (e.g. CCC056392, CGC1508234).
 * @property {string | null} licenseType - Certified / Registered trade license class.
 * @property {string | null} email - Contact email address.
 * @property {string | null} phone - Contact phone number (digits only).
 * @property {string | null} address - Business street address.
 * @property {string | null} raw - Raw unparsed licensed professional block text.
 */

/**
 * @typedef {object} EnrichedPermitRecord
 * @property {string} permit_number - Permit identifier.
 * @property {string} source_system - Source system key.
 * @property {string | null} source_url - Deep URL to source portal record.
 * @property {string} parcel_identifier - Folio identifier.
 * @property {string} work_location - Work location address.
 * @property {string | null} permit_issue_date - Issue date in ISO format.
 * @property {string | null} record_status - Current workflow/permit status.
 * @property {string | null} expiration_date - Expiration date text.
 * @property {string | null} project_description - Description of work.
 * @property {boolean} is_roof_permit - Roofing classification flag.
 * @property {number | null} job_valuation - Numeric project valuation in USD.
 * @property {number | null} square_feet - Stated roof or project area in sq ft.
 * @property {string | null} roofing_material - Material (Asphalt Shingle, Tile, Metal, etc.).
 * @property {boolean | null} is_storm_related - Storm/hurricane damage recovery indicator.
 * @property {ContractorInfo | null} contractor - Structured contractor licensing details.
 * @property {string | null} raw_html_snippet - Condensed snippet of source record.
 * @property {string} enrichment_status - Result category: "enriched" | "no_details" | "portal_404" | "rate_limited" | "fetch_error" | "unsupported_portal".
 * @property {string | null} error_message - Detailed diagnostic message if failed.
 * @property {string} enriched_at - ISO timestamp of scraping.
 */

/**
 * Extract clean company name from raw contractor text.
 * @param {string} raw
 * @returns {string | null}
 */
export function extractCleanBusinessName(raw) {
  if (!raw) return null;
  const m = raw.match(
    /(?:[A-Z0-9\s&,.'-]+?)\s+(?:LLC|INC|CORP|CORPORATION|ROOFING|ROOFS|BUILDERS|SERVICES|CONSTRUCTION|CONTRACTING|ENTERPRISES|CO|COMPANY|GROUP)\b/i,
  );
  if (m) {
    let bus = m[0]
      .replace(/^[A-Za-z\s]+@[A-Za-z0-9.-]+\s+/, "")
      .replace(/^[0-9\s]+/, "")
      .trim();
    const emailIdx = bus.indexOf("@");
    if (emailIdx !== -1) {
      const spaceAfter = bus.indexOf(" ", emailIdx);
      if (spaceAfter !== -1) bus = bus.slice(spaceAfter).trim();
    }
    return bus;
  }
  return null;
}

/**
 * Parse an Accela CapDetail HTML string.
 *
 * @param {string} html - Raw HTML from CapDetail.aspx.
 * @returns {{
 *   permitNumber: string | null,
 *   recordStatus: string | null,
 *   expirationDate: string | null,
 *   contractor: ContractorInfo | null,
 *   jobValuation: number | null,
 *   squareFeet: number | null,
 *   material: string | null,
 *   stormRelated: boolean | null,
 *   description: string | null,
 *   ownerName: string | null,
 * }}
 */
export function parseAccelaCapDetailHtml(html) {
  if (!html || typeof html !== "string") {
    return {
      permitNumber: null,
      recordStatus: null,
      expirationDate: null,
      contractor: null,
      jobValuation: null,
      squareFeet: null,
      material: null,
      stormRelated: null,
      description: null,
      ownerName: null,
    };
  }

  const $ = cheerio.load(html);

  // 1. Header parsing (Record Number, Status, Expiration)
  const headerText = $("#ctl00_PlaceHolderMain_dvContent")
    .text()
    .replace(/\s+/g, " ")
    .trim();
  const permitNumMatch = headerText.match(/Record\s+([A-Z0-9_-]+):/i);
  const statusMatch = headerText.match(
    /Record Status:\s*([A-Za-z0-9_\s-]+?)(?:\s+Expiration|\s+function|\s*$)/i,
  );
  const expMatch = headerText.match(/Expiration Date:\s*([0-9/]+)/i);

  // 2. Project Description
  let description = null;
  $("td, div, span").each((_, el) => {
    const text = $(el).text().replace(/\s+/g, " ").trim();
    if (text.startsWith("Project Description:") && !description) {
      description = text.replace(/^Project Description:\s*/, "").trim();
    }
  });

  // 3. Owner Name
  let ownerName = null;
  $("td, div, span").each((_, el) => {
    const text = $(el).text().replace(/\s+/g, " ").trim();
    if (text.startsWith("Owner:") && !ownerName) {
      ownerName = text
        .replace(/^Owner:\s*/, "")
        .replace(/\*.*$/, "")
        .trim();
    }
  });

  // 4. Licensed Professional (Contractor)
  /** @type {ContractorInfo | null} */
  let contractor = null;
  $("td, div").each((_, el) => {
    const text = $(el).text().replace(/\s+/g, " ").trim();
    if (
      text.startsWith("Licensed Professional:") &&
      $(el).hasClass("td_parent_left") &&
      !contractor
    ) {
      const body = text.replace(/^Licensed Professional:\s*/, "");

      const licMatch = body.match(
        /\b(C[A-Z]{2}[0-9]{5,8}|[A-Z]{2,4}[0-9]{5,8})\b/i,
      );
      const emailMatch = body.match(
        /([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)/,
      );
      const phoneMatch = body.match(
        /(?:Phone:|Home Phone:|Business Phone:)?\s*(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})/i,
      );
      const licTypeMatch = body.match(
        /(Certified\s+[A-Za-z\s]+|Registered\s+[A-Za-z\s]+)/i,
      );

      const cleanBus = extractCleanBusinessName(body);
      const qualifier =
        body
          .split(/\s+CAITLIN|\s+MBERNS|\s+TRC|\s+collin|@|[0-9]{5,}/)[0]
          ?.trim() || null;

      contractor = {
        businessName: cleanBus,
        contactName: qualifier,
        licenseNumber: licMatch ? licMatch[1].toUpperCase() : null,
        licenseType: licTypeMatch ? licTypeMatch[1].trim() : null,
        email: emailMatch ? emailMatch[1].toLowerCase() : null,
        phone: phoneMatch ? phoneMatch[1].replace(/\D/g, "") : null,
        address: null,
        raw: body,
      };
    }
  });

  // 5. Application Information / Valuations / Specs
  let jobValuation = null;
  let squareFeet = null;
  let material = null;
  let stormRelated = null;

  $("tr, div, td").each((_, el) => {
    const text = $(el).text().replace(/\s+/g, " ").trim();
    if (text.includes("Total Project Value:")) {
      const m = text.match(
        /Total Project Value:\s*\$?([0-9,]+(?:\.[0-9]{2})?)/i,
      );
      if (m && jobValuation === null) {
        jobValuation = parseFloat(m[1].replace(/,/g, ""));
      }
    }
    if (text.includes("Total Sq Ft:")) {
      const m = text.match(/Total Sq Ft:\s*([0-9,]+(?:\.[0-9]{2})?)/i);
      if (m && squareFeet === null) {
        squareFeet = parseFloat(m[1].replace(/,/g, ""));
      }
    }
    if (text.includes("Type of Material:")) {
      const m = text.match(/Type of Material:\s*([^:]+?)(?:\s+[A-Z\s]+:|$)/i);
      if (m && material === null) {
        material = m[1].trim();
      }
    }
    if (text.includes("Storm Related:")) {
      const m = text.match(/Storm Related:\s*(Yes|No)/i);
      if (m && stormRelated === null) {
        stormRelated = m[1].toLowerCase() === "yes";
      }
    }
  });

  return {
    permitNumber: permitNumMatch ? permitNumMatch[1] : null,
    recordStatus: statusMatch ? statusMatch[1].trim() : null,
    expirationDate: expMatch ? expMatch[1] : null,
    contractor,
    jobValuation,
    squareFeet,
    material,
    stormRelated,
    description,
    ownerName,
  };
}

/**
 * Fetch CapDetail page HTML with detailed status reporting and transient retry.
 *
 * @param {string} url - Target URL.
 * @param {number} [maxRetries=8] - Max retry attempts.
 * @returns {Promise<{ html: string | null, status: string, error: string | null }>}
 */
export async function fetchAccelaCapDetailHtml(url, maxRetries = 8) {
  let lastError = null;
  let lastStatus = "fetch_error";

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const res = await fetch(url, {
        headers: {
          "User-Agent":
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
          Accept:
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "en-US,en;q=0.9",
        },
      });

      if (
        res.status === 429 ||
        res.status === 502 ||
        res.status === 503 ||
        res.status === 504 ||
        res.status === 500
      ) {
        lastStatus = res.status === 429 ? "rate_limited" : "fetch_error";
        lastError = `HTTP ${res.status} Rate Limit / Gateway Error`;
        // Progressive exponential backoff with jitter: 1.5s, 3.0s, 4.5s, 6.0s, 7.5s ...
        const delay = Math.min(
          attempt * 1200 + Math.floor(Math.random() * 800),
          8000,
        );
        await new Promise((r) => setTimeout(r, delay));
        continue;
      }

      if (res.status === 404) {
        return {
          html: null,
          status: "portal_404",
          error: "HTTP 404 Record Not Found on Accela",
        };
      }

      if (!res.ok) {
        lastStatus = "fetch_error";
        lastError = `HTTP ${res.status} ${res.statusText}`;
        const delay = attempt * 1000 + Math.floor(Math.random() * 500);
        await new Promise((r) => setTimeout(r, delay));
        continue;
      }

      const text = await res.text();
      return { html: text, status: "ok", error: null };
    } catch (err) {
      lastStatus = "fetch_error";
      lastError = err instanceof Error ? err.message : String(err);
      if (attempt === maxRetries) {
        return { html: null, status: lastStatus, error: lastError };
      }
      await new Promise((r) =>
        setTimeout(r, attempt * 1000 + Math.floor(Math.random() * 500)),
      );
    }
  }

  return { html: null, status: lastStatus, error: lastError };
}

/**
 * AWS Lambda Handler.
 *
 * @param {{ items: Array<{
 *   permit_number: string,
 *   source_system: string,
 *   source_url?: string,
 *   parcel_identifier: string,
 *   work_location?: string,
 *   permit_issue_date?: string,
 *   record_status?: string,
 *   project_description?: string,
 *   is_roof_permit?: boolean,
 *   jurisdiction_hint?: string,
 * }> }} event
 * @param {object} context
 * @returns {Promise<{
 *   statusCode: number,
 *   processedCount: number,
 *   enrichedCount: number,
 *   failedCount: number,
 *   failureBreakdown: Record<string, number>,
 *   results: Array<EnrichedPermitRecord>,
 *   durationMs: number,
 * }>}
 */
export async function handler(event, context) {
  const startedAt = Date.now();
  const items = event.items || [];
  /** @type {Array<EnrichedPermitRecord>} */
  const results = [];
  let enrichedCount = 0;
  let failedCount = 0;
  const failureBreakdown = {
    portal_404: 0,
    rate_limited: 0,
    fetch_error: 0,
    unsupported_portal: 0,
  };

  // Process items in parallel within the Lambda container (concurrency = 6 for high throughput)
  const CONCURRENCY = 6;
  let cursor = 0;

  const workers = Array.from({ length: CONCURRENCY }, async () => {
    while (true) {
      const idx = cursor++;
      if (idx >= items.length) break;
      const cand = items[idx];

      let url = cand.source_url;
      const isClick2Gov = Boolean(
        (url && url.includes("aspgov.com")) ||
        (cand.permit_number && cand.permit_number.startsWith("TT-")),
      );
      const isMaintStar = Boolean(
        (url && url.includes("maintstar")) ||
        cand.source_system?.includes("plant_city") ||
        (cand.permit_number && /^\d{9}-\d{4}$/.test(cand.permit_number)),
      );
      const isAccela = !isClick2Gov && !isMaintStar;

      if (
        isAccela &&
        url &&
        url.includes("aca-prod.accela.com") &&
        !url.includes("CapDetail.aspx")
      ) {
        const isTampa =
          url.includes("/TAMPA") || cand.jurisdiction_hint === "TAMPA";
        const agency = isTampa ? "TAMPA" : "HCFL";
        url = `https://aca-prod.accela.com/${agency}/Cap/CapDetail.aspx?Module=Building&TabName=Building&altId=${cand.permit_number}`;
      }

      let parsed = null;
      let enrichmentStatus = "no_details";
      let errorMessage = null;

      if (isClick2Gov) {
        // Temple Terrace Click2Gov Adapter
        const c2gRes = await fetchClick2GovPermitDetail(cand.permit_number);
        if (c2gRes.data) {
          const d = c2gRes.data;
          const hasDetails =
            d.contractor !== null ||
            d.jobValuation !== null ||
            d.recordStatus !== null;
          if (hasDetails) {
            enrichmentStatus = "enriched";
            enrichedCount++;
            parsed = {
              contractor: d.contractor
                ? {
                    businessName: d.contractor.businessName,
                    contactName: d.contractor.qualifierName,
                    licenseNumber: d.contractor.licenseNumber,
                    licenseType: null,
                    email: d.contractor.email,
                    phone: d.contractor.phone,
                    address: null,
                    raw: d.contractor.businessName,
                  }
                : null,
              jobValuation: d.jobValuation,
              squareFeet: d.squareFeet,
              material: null,
              stormRelated: null,
              recordStatus: d.recordStatus,
              expirationDate: null,
              description:
                d.applicationType || cand.project_description || null,
            };
          }
        } else if (c2gRes.status === "not_found") {
          enrichmentStatus = "no_details";
          errorMessage = "Record not found on Temple Terrace Click2Gov";
        } else {
          enrichmentStatus = c2gRes.status;
          errorMessage = c2gRes.error;
        }
      } else if (isMaintStar) {
        // Plant City MaintStar Adapter
        const msRes = await searchMaintStarPermits(cand.permit_number);
        if (msRes.records && msRes.records.length > 0) {
          const rec = msRes.records[0];
          enrichmentStatus = "enriched";
          enrichedCount++;
          parsed = {
            contractor: null,
            jobValuation: null,
            squareFeet: null,
            material: null,
            stormRelated: null,
            recordStatus: rec.status,
            expirationDate: null,
            description:
              rec.description || rec.type || cand.project_description || null,
          };
        } else if (msRes.status === "quota_exceeded") {
          enrichmentStatus = "rate_limited";
          errorMessage = msRes.error;
        } else {
          enrichmentStatus = "no_details";
          errorMessage =
            msRes.error || "No matching record in Plant City MaintStar";
        }
      } else if (!url || !url.includes("aca-prod.accela.com")) {
        enrichmentStatus = "no_details";
        errorMessage = `Unrecognized municipal portal: ${cand.source_system}`;
      } else {
        // Accela Portal Adapter
        const fetchRes = await fetchAccelaCapDetailHtml(url);
        if (fetchRes.html) {
          parsed = parseAccelaCapDetailHtml(fetchRes.html);
          const hasDetails =
            parsed !== null &&
            (parsed.contractor !== null || parsed.jobValuation !== null);
          if (hasDetails) {
            enrichmentStatus = "enriched";
            enrichedCount++;
          } else {
            enrichmentStatus = "no_details";
          }
        } else if (fetchRes.status === "portal_404") {
          enrichmentStatus = "no_details";
          errorMessage = "HTTP 404 Record Not Found on Accela";
        } else {
          enrichmentStatus = fetchRes.status;
          errorMessage = fetchRes.error;
          failedCount++;
          if (failureBreakdown[fetchRes.status] !== undefined) {
            failureBreakdown[fetchRes.status]++;
          } else {
            failureBreakdown.fetch_error++;
          }
        }
      }

      /** @type {EnrichedPermitRecord} */
      const record = {
        permit_number: cand.permit_number,
        source_system: cand.source_system,
        source_url: url || null,
        parcel_identifier: cand.parcel_identifier,
        work_location: cand.work_location || "",
        permit_issue_date: cand.permit_issue_date || null,
        record_status: parsed?.recordStatus || cand.record_status || null,
        expiration_date: parsed?.expirationDate || null,
        project_description:
          parsed?.description || cand.project_description || null,
        is_roof_permit: Boolean(cand.is_roof_permit),
        job_valuation: parsed?.jobValuation || null,
        square_feet: parsed?.squareFeet || null,
        roofing_material: parsed?.material || null,
        is_storm_related: parsed?.stormRelated || null,
        contractor: parsed?.contractor || null,
        raw_html_snippet: null,
        enrichment_status: enrichmentStatus,
        error_message: errorMessage,
        enriched_at: new Date().toISOString(),
      };

      results.push(record);
      await new Promise((r) =>
        setTimeout(r, 150 + Math.floor(Math.random() * 100)),
      );
    }
  });

  await Promise.all(workers);

  return {
    statusCode: 200,
    processedCount: results.length,
    enrichedCount,
    failedCount,
    failureBreakdown,
    results,
    durationMs: Date.now() - startedAt,
  };
}
