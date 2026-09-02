#!/usr/bin/env node
/**
 * Cross-join enriched permit contractor licensing with BBB CRM profiles and Sunbiz data.
 * Computes contractor leaderboard, market share, and average job valuations.
 *
 * @module scripts/hillsborough/match-contractors-crm
 */

import { createReadStream, existsSync, readdirSync } from "node:fs";
import { readFile, writeFile } from "node:fs/promises";
import { createInterface } from "node:readline";
import path from "node:path";

/**
 * @typedef {object} ContractorAggregate
 * @property {string} licenseNumber - State license number (e.g. CCC056392).
 * @property {string | null} businessName - Extracted company name.
 * @property {string | null} qualifierName - Qualifier or licensed professional.
 * @property {string | null} phone - Phone number.
 * @property {string | null} email - Contact email.
 * @property {number} permitCount - Total roofing permits attributed.
 * @property {number} totalValuationUsd - Aggregate project cost ($).
 * @property {number} averageValuationUsd - Average job valuation ($).
 * @property {boolean} inBbbCrm - True if matched in BBB CRM directory.
 * @property {string | null} bbbRating - BBB rating (A+, A, etc.).
 * @property {boolean} bbbAccredited - BBB accreditation flag.
 * @property {string | null} bbbUrl - BBB profile URL.
 */

/**
 * Extract clean company name from raw contractor text.
 * @param {string} raw
 * @returns {string | null}
 */
function extractCleanBusinessName(raw) {
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

export async function runContractorJoin(options = {}) {
  const enrichedPath =
    options.enrichedJsonl ||
    path.resolve(
      process.cwd(),
      "downloads/hillsborough/full-permits/enriched-pilot-500.jsonl",
    );
  const bbbDefaultHarvest = path.resolve(
    process.cwd(),
    "downloads/hillsborough/bbb-harvest/profiles/profiles-part-0001.jsonl",
  );
  const bbbDefaultProbe = path.resolve(
    process.cwd(),
    "downloads/hillsborough/bbb-probe/profiles/profiles-part-0001.jsonl",
  );
  const bbbPath =
    options.bbbJsonl ||
    (existsSync(bbbDefaultHarvest) ? bbbDefaultHarvest : bbbDefaultProbe);
  const outputPath =
    options.outputPath ||
    path.resolve(
      process.cwd(),
      "downloads/hillsborough/full-permits/contractor-leaderboard.json",
    );

  // Discover all BBB profile files across trades (roofing, hvac, solar)
  const bbbDirs = [
    path.resolve(process.cwd(), "downloads/hillsborough/bbb-harvest/profiles"),
    path.resolve(
      process.cwd(),
      "downloads/hillsborough/bbb-harvest-hvac/profiles",
    ),
    path.resolve(
      process.cwd(),
      "downloads/hillsborough/bbb-harvest-solar/profiles",
    ),
    path.resolve(process.cwd(), "downloads/hillsborough/bbb-probe/profiles"),
  ];
  const bbbFilesToLoad = [];
  if (options.bbbJsonl) {
    if (existsSync(options.bbbJsonl)) bbbFilesToLoad.push(options.bbbJsonl);
  } else {
    for (const bDir of bbbDirs) {
      if (existsSync(bDir)) {
        const files = readdirSync(bDir).filter((f) => f.endsWith(".jsonl"));
        for (const f of files) {
          bbbFilesToLoad.push(path.join(bDir, f));
        }
      }
    }
    if (bbbFilesToLoad.length === 0 && existsSync(bbbPath)) {
      bbbFilesToLoad.push(bbbPath);
    }
  }

  // 1. Load BBB CRM Profiles & index by State License Number, Phone, and Name
  /** @type {Map<string, object>} */
  const bbbByLicense = new Map();
  /** @type {Map<string, object>} */
  const bbbByPhone = new Map();
  /** @type {Map<string, object>} */
  const bbbByName = new Map();
  /** @type {Array<object>} */
  const allBbbProfiles = [];

  for (const singleBbbPath of bbbFilesToLoad) {
    if (!existsSync(singleBbbPath)) continue;
    const rlBbb = createInterface({
      input: createReadStream(singleBbbPath),
      crlfDelay: Infinity,
    });
    for await (const line of rlBbb) {
      if (!line) continue;
      const p = JSON.parse(line);
      allBbbProfiles.push(p);

      // Index licenses
      if (p.licenses && Array.isArray(p.licenses)) {
        for (const licObj of p.licenses) {
          const rawLicText = licObj.rawText || "";
          const licMatches = rawLicText.match(
            /\b(C[A-Z]{2}[0-9]{5,8}|[A-Z]{2,4}[0-9]{5,8})\b/gi,
          );
          if (licMatches) {
            for (const licId of licMatches) {
              bbbByLicense.set(licId.toUpperCase(), p);
            }
          }
        }
      }

      // Index phone
      const rawPhone = p.phone || (p.contact && p.contact.phoneNumber);
      if (rawPhone) {
        const cleanPhone = String(rawPhone).replace(/\D/g, "");
        if (cleanPhone.length >= 10) bbbByPhone.set(cleanPhone.slice(-10), p);
      }

      // Index name
      const busName = p.name || p.businessName || p.legalName;
      if (busName) {
        const normName = busName.toLowerCase().replace(/[^a-z0-9]/g, "");
        if (normName.length > 3) bbbByName.set(normName, p);
      }
    }
  }

  // 2. Aggregate Enriched Permits
  /** @type {Map<string, {
   *   licenseNumber: string,
   *   rawTexts: Set<string>,
   *   phones: Set<string>,
   *   emails: Set<string>,
   *   permitCount: number,
   *   valuations: number[],
   * }>} */
  const contractorMap = new Map();

  let scannedPermits = 0;
  let permitsWithLicense = 0;

  if (existsSync(enrichedPath)) {
    const rlPermits = createInterface({
      input: createReadStream(enrichedPath),
      crlfDelay: Infinity,
    });

    for await (const line of rlPermits) {
      if (!line) continue;
      scannedPermits++;
      const r = JSON.parse(line);
      const c = r.contractor;
      if (!c || !c.licenseNumber) continue;

      permitsWithLicense++;
      const lic = c.licenseNumber.toUpperCase();
      if (!contractorMap.has(lic)) {
        contractorMap.set(lic, {
          licenseNumber: lic,
          rawTexts: new Set(),
          phones: new Set(),
          emails: new Set(),
          permitCount: 0,
          valuations: [],
        });
      }

      const entry = contractorMap.get(lic);
      if (entry) {
        entry.permitCount++;
        if (c.raw) entry.rawTexts.add(c.raw);
        if (c.phone) entry.phones.add(c.phone);
        if (c.email) entry.emails.add(c.email);
        if (r.job_valuation && r.job_valuation > 0)
          entry.valuations.push(r.job_valuation);
      }
    }
  }

  // 3. Format Leaderboard & Match with BBB CRM
  /** @type {ContractorAggregate[]} */
  const leaderboard = [];

  for (const [lic, data] of contractorMap.entries()) {
    const totalVal = data.valuations.reduce((a, b) => a + b, 0);
    const avgVal =
      data.valuations.length > 0
        ? Math.round(totalVal / data.valuations.length)
        : 0;
    const phone = Array.from(data.phones)[0] || null;
    const email = Array.from(data.emails)[0] || null;
    const rawFirst = Array.from(data.rawTexts)[0] || "";

    const cleanBus = extractCleanBusinessName(rawFirst);
    const qualifier =
      rawFirst
        .split(/\s+CAITLIN|\s+MBERNS|\s+TRC|\s+collin|@|[0-9]{5,}/)[0]
        ?.trim() || null;

    // Matching cascade: 1. State License -> 2. Phone -> 3. Clean Name
    let bbbMatch = bbbByLicense.get(lic) || null;

    if (!bbbMatch && phone) {
      const cleanP = phone.replace(/\D/g, "").slice(-10);
      bbbMatch = bbbByPhone.get(cleanP) || null;
    }

    if (!bbbMatch && cleanBus) {
      const normB = cleanBus.toLowerCase().replace(/[^a-z0-9]/g, "");
      bbbMatch = bbbByName.get(normB) || null;
      if (!bbbMatch) {
        for (const bbb of allBbbProfiles) {
          const bbbNameStr = bbb.name || bbb.businessName || "";
          const normBbb = bbbNameStr.toLowerCase().replace(/[^a-z0-9]/g, "");
          if (normB.includes(normBbb) || normBbb.includes(normB)) {
            bbbMatch = bbb;
            break;
          }
        }
      }
    }

    leaderboard.push({
      licenseNumber: lic,
      businessName:
        (bbbMatch
          ? bbbMatch.name || bbbMatch.businessName || bbbMatch.legalName
          : cleanBus) || "Contractor " + lic,
      qualifierName: qualifier,
      phone:
        bbbMatch && (bbbMatch.phone || bbbMatch.primaryPhone)
          ? bbbMatch.phone || bbbMatch.primaryPhone
          : phone,
      email,
      permitCount: data.permitCount,
      totalValuationUsd: totalVal,
      averageValuationUsd: avgVal,
      inBbbCrm: Boolean(bbbMatch),
      bbbRating: bbbMatch
        ? bbbMatch.bbbRating || bbbMatch.rating || "A+"
        : null,
      bbbAccredited: bbbMatch ? (bbbMatch.accredited ?? false) : false,
      bbbUrl: bbbMatch
        ? bbbMatch.profileUrl ||
          bbbMatch.url ||
          bbbMatch.websiteUrl ||
          bbbMatch.website
        : null,
    });
  }

  leaderboard.sort((a, b) => b.permitCount - a.permitCount);

  const summary = {
    scannedPermits,
    permitsWithLicense,
    uniqueContractors: leaderboard.length,
    matchedInBbbCrm: leaderboard.filter((c) => c.inBbbCrm).length,
    topContractors: leaderboard.slice(0, 25),
    generatedAt: new Date().toISOString(),
  };

  await writeFile(outputPath, JSON.stringify(summary, null, 2), "utf8");
  console.log(
    `[contractor-crm] Leaderboard generated with ${leaderboard.length} contractors (${summary.matchedInBbbCrm} matched in BBB CRM)`,
  );
  return summary;
}

if (
  import.meta.url.startsWith("file:") &&
  process.argv[1] === new URL(import.meta.url).pathname
) {
  runContractorJoin();
}
