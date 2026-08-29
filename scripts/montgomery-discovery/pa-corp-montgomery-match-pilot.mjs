#!/usr/bin/env node
/**
 * Montgomery County PA corporate-registration address-match pilot.
 *
 * Pulls Montgomery-filtered rows from PA DOS open data (Socrata), dedupes to one entity
 * address per filing_number, and matches registered-office addresses to Montgomery
 * property situs addresses from the pilot seed.
 *
 * Usage:
 *   node scripts/montgomery-discovery/pa-corp-montgomery-match-pilot.mjs
 */

import { readFile } from "node:fs/promises";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  buildNormalizedAddressKey,
  hashNormalizedAddressKey,
  normalizePostalCode,
} from "../../workflow/lambdas/permit-harvest-worker/query-db-loader/normalizers.js";
import { parseSeedCsvText } from "../montgomery-local-pilot.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "../..");
const DEFAULT_SEED_CSV = resolve(ROOT, "downloads/montgomery/pilot-seed-50.csv");
const DEFAULT_OUT = resolve(ROOT, "downloads/montgomery/pa-corp-match-pilot.json");
const PA_DOS_RESOURCE = "https://data.pa.gov/resource/xvd7-5r2c.json";

function buildPaDosUnnormalizedAddress(row) {
  const line1 = row.address_line1?.trim();
  const city = row.city?.split(",")[0]?.trim();
  const state = row.state?.trim().toUpperCase() ?? "PA";
  const zip = normalizePostalCode(row.zip);
  if (!line1 || !city || !zip) return null;
  return `${line1.toUpperCase()}, ${city.toUpperCase()} ${state}, ${zip}`;
}

async function fetchPaDosMontgomeryRows(limit = 2000) {
  const url = `${PA_DOS_RESOURCE}?$where=upper(shortcountyname)%20like%20%27%25MONTGOMERY%25%27&$limit=${limit}`;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`PA DOS fetch failed: HTTP ${response.status}`);
  }
  return response.json();
}

async function main() {
  console.log("Loading Montgomery pilot seed addresses...");
  const seedText = await readFile(DEFAULT_SEED_CSV, "utf8");
  const seedRows = parseSeedCsvText(seedText);

  const propertyAddressMap = new Map();
  for (const row of seedRows) {
    const street = row.street?.trim().toUpperCase();
    const city = row.city?.trim().toUpperCase();
    const zip = normalizePostalCode(row.zip);
    if (!street || !zip) continue;

    const unnormalized = `${street}, ${city || "MONTGOMERY"} PA ${zip}`;
    const normKey = buildNormalizedAddressKey(unnormalized);
    const hash = normKey ? hashNormalizedAddressKey(normKey) : null;
    if (hash) {
      propertyAddressMap.set(hash, {
        parcel_id: row.parcel_id,
        street,
        city,
        zip,
        unnormalized,
      });
    }
  }

  console.log(`Loaded ${propertyAddressMap.size} unique property address hashes.`);
  console.log("Fetching sample Montgomery corporate entities from PA Department of State...");

  const rawRows = await fetchPaDosMontgomeryRows(2000);
  console.log(`Received ${rawRows.length} raw entity party rows from PA DOS.`);

  const dedupedEntities = new Map();
  for (const row of rawRows) {
    const fn = row.filing_number;
    if (!fn || dedupedEntities.has(fn)) continue;
    const addr = buildPaDosUnnormalizedAddress(row);
    if (!addr) continue;

    const normKey = buildNormalizedAddressKey(addr);
    const hash = normKey ? hashNormalizedAddressKey(normKey) : null;
    if (hash) {
      dedupedEntities.set(fn, {
        filingNumber: fn,
        businessName: row.business_name,
        address: addr,
        hash,
        partyType: row.party_type ?? null,
      });
    }
  }

  console.log(`Deduped to ${dedupedEntities.size} distinct entities with valid address hashes.`);

  const matches = [];
  for (const entity of dedupedEntities.values()) {
    const prop = propertyAddressMap.get(entity.hash);
    if (prop) {
      matches.push({
        filingNumber: entity.filingNumber,
        businessName: entity.businessName,
        parcel_id: prop.parcel_id,
        propertyAddress: prop.unnormalized,
        entityAddress: entity.address,
        normalizedAddressHash: entity.hash,
      });
    }
  }

  const report = {
    county: "Montgomery",
    state: "PA",
    propertiesChecked: propertyAddressMap.size,
    entitiesEvaluated: dedupedEntities.size,
    matchCount: matches.length,
    matches,
    generatedAt: new Date().toISOString(),
  };

  await mkdir(dirname(DEFAULT_OUT), { recursive: true });
  await writeFile(DEFAULT_OUT, JSON.stringify(report, null, 2), "utf8");
  console.log(`\nPA Corporate Match Pilot Complete:`);
  console.log(`- Properties evaluated: ${report.propertiesChecked}`);
  console.log(`- Entities evaluated: ${report.entitiesEvaluated}`);
  console.log(`- Matches found: ${report.matchCount}`);
  console.log(`- Results saved to: ${DEFAULT_OUT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
