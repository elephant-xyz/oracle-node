#!/usr/bin/env node
/**
 * Accela Citizen Access & Municipal Deep Permit Enrichment Harvester.
 * High-throughput streaming harvester for Hillsborough County (HCFL) & City of Tampa (TAMPA).
 *
 * Extracts contractor licensing (CCC/CGC numbers), job valuations ($),
 * roofing materials, square footage, and inspection milestones.
 *
 * Designed with memory streaming, worker pooling, rate limiting, and resumable checkpoints.
 *
 * @module scripts/hillsborough/enrich-permits-accela
 */

import { createReadStream, createWriteStream, existsSync } from "node:fs";
import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { createInterface } from "node:readline";
import path from "node:path";
import { parseArgs } from "node:util";
import * as cheerio from "cheerio";
import { runContractorJoin } from "./match-contractors-crm.mjs";

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
 * @property {string} source_system - Source system key (`hcfl_accela`, `tampa_accela`, etc.).
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
 * Parse an Accela CapDetail HTML string (compatible with HCFL and TAMPA).
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

      // License Number: e.g. CCC056392, CGC1508234, CRC123456
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
      const m = text.match(
        /Type of Material:\s*([^<:\n]+?)(?:\s+(?:Storm Related|Total|Project|Owner|Contractor|Expiration|Record Status):|$)/i,
      );
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
 * Fetch CapDetail page HTML with transient retry and rate-limiting.
 *
 * @param {string} url - Target URL.
 * @param {number} [maxRetries=3] - Max retry attempts.
 * @returns {Promise<string | null>}
 */
export async function fetchAccelaCapDetailHtml(url, maxRetries = 3) {
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

      if (res.status === 429 || res.status === 503) {
        const delay = attempt * 1000;
        await new Promise((r) => setTimeout(r, delay));
        continue;
      }

      if (!res.ok) {
        return null;
      }

      const text = await res.text();
      return text;
    } catch (err) {
      if (attempt === maxRetries) return null;
      await new Promise((r) => setTimeout(r, attempt * 500));
    }
  }
  return null;
}

/**
 * Run high-throughput permit enrichment workflow.
 *
 * @param {{
 *   limit?: number | null,
 *   concurrency?: number,
 *   trade?: string,
 *   inputJsonl?: string,
 *   outputJsonl?: string,
 *   checkpointPath?: string,
 * }} [options={}]
 */
export async function runPermitEnrichment(options = {}) {
  const limit = options.limit || null;
  const concurrency = options.concurrency || 24;
  const tradeFilter = options.trade || "all";
  const inputPath =
    options.inputJsonl ||
    path.resolve(
      process.cwd(),
      "downloads/hillsborough/full-permits/normalized-permits.jsonl",
    );
  const outputPath =
    options.outputJsonl ||
    path.resolve(
      process.cwd(),
      "downloads/hillsborough/full-permits/enriched-permits.jsonl",
    );
  const checkpointPath =
    options.checkpointPath ||
    path.resolve(
      process.cwd(),
      "downloads/hillsborough/full-permits/enrichment-progress.json",
    );

  await mkdir(path.dirname(outputPath), { recursive: true });

  let totalTarget = limit || 958002;
  let streamCount = 0;
  let processed = 0;
  let newlyProcessed = 0;
  let enrichedCount = 0;
  let licenseCount = 0;
  let valuationCount = 0;
  const valuationValues = [];
  const contractorTally = new Map();

  // Load previously processed permit IDs for seamless resumption
  /** @type {Set<string>} */
  const completedPermits = new Set();
  if (existsSync(outputPath)) {
    console.log(
      `[enrichment] Checking existing output file for resume: ${outputPath}`,
    );
    const rlExisting = createInterface({
      input: createReadStream(outputPath),
      crlfDelay: Infinity,
    });
    for await (const line of rlExisting) {
      if (!line) continue;
      try {
        const parsed = JSON.parse(line);
        if (parsed.permit_number) {
          completedPermits.add(parsed.permit_number);
          processed++;
          const isEnriched =
            parsed.contractor !== null || parsed.job_valuation !== null;
          if (isEnriched) enrichedCount++;
          if (parsed.contractor?.licenseNumber) {
            licenseCount++;
            const lic = parsed.contractor.licenseNumber;
            contractorTally.set(lic, (contractorTally.get(lic) || 0) + 1);
          }
          if (parsed.job_valuation) {
            valuationCount++;
            valuationValues.push(parsed.job_valuation);
          }
        }
      } catch {}
    }
    console.log(
      `[enrichment] Found ${completedPermits.size} previously completed permits (resuming)`,
    );
  }

  const outStream = createWriteStream(outputPath, {
    flags: "a",
    encoding: "utf8",
  });

  const startedAtMs = Date.now();
  let lastCheckpointWrite = Date.now();
  let lastRateCalcTime = Date.now();
  let lastProcessedCount = processed;
  let rollingRatePerSec = 0;

  /**
   * Write progress checkpoint file (debounced).
   * @param {boolean} [force=false]
   */
  async function flushCheckpoint(force = false) {
    const now = Date.now();
    if (!force && now - lastCheckpointWrite < 2000) return;
    lastCheckpointWrite = now;

    const elapsedTotalSec = Math.max(1, (now - startedAtMs) / 1000);
    const windowSec = Math.max(1, (now - lastRateCalcTime) / 1000);
    if (windowSec >= 3) {
      const delta = processed - lastProcessedCount;
      rollingRatePerSec = Number((delta / windowSec).toFixed(1));
      lastRateCalcTime = now;
      lastProcessedCount = processed;
    }

    const avgRate = Number((newlyProcessed / elapsedTotalSec).toFixed(1));
    const activeRate = rollingRatePerSec > 0 ? rollingRatePerSec : avgRate;
    const remaining = Math.max(0, totalTarget - processed);
    const etaSec = activeRate > 0 ? Math.round(remaining / activeRate) : null;
    const etaIso =
      etaSec !== null ? new Date(now + etaSec * 1000).toISOString() : null;

    const avgValuation =
      valuationValues.length > 0
        ? Math.round(
            valuationValues.reduce((a, b) => a + b, 0) / valuationValues.length,
          )
        : 0;

    const progressData = {
      status:
        totalTarget > 0 && processed >= totalTarget
          ? "completed"
          : "in_progress",
      targetCount: totalTarget,
      processedCount: processed,
      newlyProcessedCount: newlyProcessed,
      enrichedCount,
      enrichmentRatePct:
        processed > 0
          ? ((enrichedCount / processed) * 100).toFixed(1) + "%"
          : "0.0%",
      licenseCount,
      licenseYieldPct:
        processed > 0
          ? ((licenseCount / processed) * 100).toFixed(1) + "%"
          : "0.0%",
      valuationCount,
      valuationYieldPct:
        processed > 0
          ? ((valuationCount / processed) * 100).toFixed(1) + "%"
          : "0.0%",
      averageJobValuationUsd: avgValuation,
      uniqueContractorLicenses: contractorTally.size,
      ratePerSecond: activeRate,
      permitsPerMinute: Math.round(activeRate * 60),
      etaSeconds: etaSec,
      etaIso,
      updatedAt: new Date().toISOString(),
      startedAt: new Date(startedAtMs).toISOString(),
    };

    try {
      await writeFile(
        checkpointPath,
        JSON.stringify(progressData, null, 2),
        "utf8",
      );
    } catch {}
  }

  // Stream candidates through async queue
  const rl = createInterface({
    input: createReadStream(inputPath),
    crlfDelay: Infinity,
  });

  /** @type {Array<object>} */
  let buffer = [];
  const BATCH_CHUNK = 256;

  console.log(
    `[enrichment] Starting streaming enrichment (concurrency: ${concurrency}, trade: ${tradeFilter})...`,
  );

  for await (const line of rl) {
    if (!line) continue;
    const r = JSON.parse(line);

    if (tradeFilter === "roofing" && !r.is_roof_permit) continue;
    streamCount++;

    if (limit && streamCount > limit) break;
    if (completedPermits.has(r.permit_number)) continue;

    buffer.push(r);

    if (buffer.length >= BATCH_CHUNK) {
      await processCandidateBatch(buffer);
      buffer = [];
    }
  }

  if (buffer.length > 0) {
    await processCandidateBatch(buffer);
    buffer = [];
  }

  outStream.end();
  await flushCheckpoint(true);

  // Generate finalized contractor CRM leaderboard
  console.log(
    `[enrichment] Enrichment pass finished. Updating contractor CRM match ledger...`,
  );
  await runContractorJoin({
    enrichedJsonl: outputPath,
  });

  console.log(`[enrichment] All complete! Total processed: ${processed}`);

  /**
   * Process a chunk of candidates in parallel with bounded concurrency.
   * @param {Array<object>} chunk
   */
  async function processCandidateBatch(chunk) {
    let cursor = 0;
    const workerPromises = Array.from({ length: concurrency }, async () => {
      while (true) {
        const itemIdx = cursor++;
        if (itemIdx >= chunk.length) break;
        const cand = chunk[itemIdx];

        let url = cand.source_url;
        if (
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
        if (url && url.includes("aca-prod.accela.com")) {
          const html = await fetchAccelaCapDetailHtml(url);
          if (html) {
            parsed = parseAccelaCapDetailHtml(html);
          }
        }

        const isEnriched =
          parsed !== null &&
          (parsed.contractor !== null || parsed.jobValuation !== null);
        if (isEnriched) enrichedCount++;
        if (parsed?.contractor?.licenseNumber) {
          licenseCount++;
          const lic = parsed.contractor.licenseNumber;
          contractorTally.set(lic, (contractorTally.get(lic) || 0) + 1);
        }
        if (parsed?.jobValuation) {
          valuationCount++;
          valuationValues.push(parsed.jobValuation);
        }

        /** @type {EnrichedPermitRecord} */
        const record = {
          permit_number: cand.permit_number,
          source_system: cand.source_system,
          source_url: url,
          parcel_identifier: cand.parcel_identifier,
          work_location: cand.work_location,
          permit_issue_date: cand.permit_issue_date,
          record_status: parsed?.recordStatus || cand.record_status,
          expiration_date: parsed?.expirationDate || null,
          project_description: parsed?.description || cand.project_description,
          is_roof_permit: Boolean(cand.is_roof_permit),
          job_valuation: parsed?.jobValuation || null,
          square_feet: parsed?.squareFeet || null,
          roofing_material: parsed?.material || null,
          is_storm_related: parsed?.stormRelated || null,
          contractor: parsed?.contractor || null,
          raw_html_snippet: null,
          enriched_at: new Date().toISOString(),
        };

        outStream.write(JSON.stringify(record) + "\n");
        processed++;
        newlyProcessed++;

        if (newlyProcessed % 100 === 0) {
          await flushCheckpoint();
          if (newlyProcessed % 500 === 0) {
            console.log(
              `[enrichment] Processed ${processed}/${totalTarget} (${rollingRatePerSec} req/sec, Licenses: ${licenseCount}, Valuations: ${valuationCount})`,
            );
          }
          if (newlyProcessed % 1000 === 0) {
            runContractorJoin({ enrichedJsonl: outputPath }).catch(() => {});
          }
        }

        // 30ms polite stagger per worker
        await new Promise((r) => setTimeout(r, 30));
      }
    });

    await Promise.all(workerPromises);
  }
}

if (
  import.meta.url.startsWith("file:") &&
  process.argv[1] === new URL(import.meta.url).pathname
) {
  const { values } = parseArgs({
    options: {
      limit: { type: "string" },
      concurrency: { type: "string", default: "24" },
      trade: { type: "string", default: "all" },
    },
  });
  runPermitEnrichment({
    limit: values.limit ? parseInt(values.limit, 10) : null,
    concurrency: parseInt(values.concurrency || "24", 10),
    trade: values.trade || "all",
  });
}
