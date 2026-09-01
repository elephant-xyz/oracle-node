#!/usr/bin/env node
/**
 * Export a diverse Montgomery County seed CSV from the PASDA GIS REST layer.
 *
 * Usage:
 *   node scripts/montgomery-export-seed.mjs --target=50 --output=downloads/montgomery/pilot-seed-50.csv
 */

import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  buildPasdaPageUrl,
  seedRowFromGisAttributes,
  serializeSeedCsv,
} from "./montgomery/lib.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "..");

/**
 * @param {number} offset
 * @param {number} pageSize
 * @returns {Promise<Array<Record<string, unknown>>>}
 */
async function fetchPasdaPage(offset, pageSize) {
  const url = buildPasdaPageUrl(offset, pageSize, "YEAR_BUILT > 0");
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(
      `PASDA fetch failed at offset ${offset}: HTTP ${response.status}`,
    );
  }
  const payload = await response.json();
  if (payload.error) {
    throw new Error(
      `PASDA error at offset ${offset}: ${JSON.stringify(payload.error)}`,
    );
  }
  return (payload.features ?? []).map((feature) => feature.attributes ?? {});
}

async function main() {
  const targetArg = process.argv.find((a) => a.startsWith("--target="));
  const outputArg = process.argv.find((a) => a.startsWith("--output="));
  const target = targetArg ? Number.parseInt(targetArg.split("=")[1], 10) : 50;
  const outputPath = outputArg
    ? outputArg.split("=")[1]
    : "downloads/montgomery/pilot-seed-50.csv";

  console.log(
    `Fetching ${target} diverse Montgomery County parcels from PASDA...`,
  );

  const rows = [];
  const seenTaxpin = new Set();
  const seenMuni = new Map();
  let offset = 0;
  const pageSize = 500;
  const maxPerMuni = Math.max(15, Math.ceil(target / 45));

  while (rows.length < target && offset < 100000) {
    const rawAttrs = await fetchPasdaPage(offset, pageSize);
    if (!rawAttrs.length) break;

    for (const attrs of rawAttrs) {
      const row = seedRowFromGisAttributes(attrs);
      if (!row || seenTaxpin.has(row.parcel_id)) continue;

      const muni = row.muni_code || "UNKNOWN";
      const muniCount = seenMuni.get(muni) || 0;
      if (muniCount >= maxPerMuni && rows.length < target * 0.8) {
        continue;
      }

      seenTaxpin.add(row.parcel_id);
      seenMuni.set(muni, muniCount + 1);
      rows.push(row);

      if (rows.length >= target) break;
    }

    offset += pageSize;
  }

  const fullPath = resolve(ROOT, outputPath);
  await mkdir(dirname(fullPath), { recursive: true });
  await writeFile(fullPath, serializeSeedCsv(rows), "utf8");
  console.log(`Saved ${rows.length} Montgomery seed records to ${outputPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
