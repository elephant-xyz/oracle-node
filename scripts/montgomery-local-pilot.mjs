#!/usr/bin/env node
/**
 * Montgomery County local pilot: PASDA GIS fetch -> Transform v2 -> output zip generation.
 *
 * Usage:
 *   node scripts/montgomery-local-pilot.mjs
 *   node scripts/montgomery-local-pilot.mjs --seed=downloads/montgomery/pilot-seed-50.csv --limit=50
 */

import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import AdmZip from "adm-zip";

import {
  PASDA_MONTGOMERY_BASE,
  MONTGOMERY_GIS_FIELDS,
  seedRowFromGisAttributes,
} from "./montgomery/lib.mjs";
import { handler } from "../../Counties-trasform-scripts/montgomery/scripts/handler.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "..");
const DEFAULT_SEED_CSV = resolve(
  ROOT,
  "downloads/montgomery/pilot-seed-50.csv",
);
const DEFAULT_OUTPUT_ROOT = resolve(
  ROOT,
  "downloads/montgomery/pilot-transformed",
);

/**
 * @param {string} text
 * @returns {Array<Record<string, string>>}
 */
export function parseSeedCsvText(text) {
  const lines = text.trim().split("\n");
  if (lines.length < 2) return [];
  const headers = lines[0]
    .split(",")
    .map((h) => h.trim().replace(/^"|"$/g, ""));
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const rawLine = lines[i].trim();
    if (!rawLine) continue;
    // Simple CSV parse supporting quotes
    const values = [];
    let cur = "";
    let inQuotes = false;
    for (let charIdx = 0; charIdx < rawLine.length; charIdx++) {
      const c = rawLine[charIdx];
      if (c === '"') {
        if (inQuotes && rawLine[charIdx + 1] === '"') {
          cur += '"';
          charIdx++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (c === "," && !inQuotes) {
        values.push(cur);
        cur = "";
      } else {
        cur += c;
      }
    }
    values.push(cur);

    const row = {};
    headers.forEach((h, idx) => {
      row[h] = values[idx] ?? "";
    });
    rows.push(row);
  }
  return rows;
}

/**
 * @param {string} taxpin
 * @returns {Promise<Record<string, unknown>>}
 */
async function fetchPasdaFeature(taxpin) {
  const where = encodeURIComponent(`TAXPIN='${taxpin}' OR PARCEL='${taxpin}'`);
  const url = `${PASDA_MONTGOMERY_BASE}?where=${where}&outFields=${MONTGOMERY_GIS_FIELDS}&returnGeometry=false&f=json`;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(
      `PASDA fetch failed for ${taxpin}: HTTP ${response.status}`,
    );
  }
  const payload = await response.json();
  if (!payload.features?.length) {
    throw new Error(`PASDA returned no features for ${taxpin}`);
  }
  return payload;
}

async function runLocalTransform(row, payload, outDir) {
  const rawCaptureStr = JSON.stringify(payload);
  const writtenJsons = new Map();
  const writtenRelationships = [];

  const context = {
    input: {
      parcel: {
        parcel_identifier: row.parcel_id || row.taxpin,
      },
      address: {
        street: row.street,
        city: row.city,
        zip: row.zip,
        owner: row.owner,
      },
    },
    readCapture: async (name) => {
      if (name === "gis-parcel") return rawCaptureStr;
      throw new Error(`Unknown capture requested: ${name}`);
    },
    writeJson: async (name, data) => {
      writtenJsons.set(name, JSON.stringify(data, null, 2));
    },
    writeRelationship: async (rel) => {
      writtenRelationships.push(rel);
    },
  };

  await handler(context);

  await mkdir(outDir, { recursive: true });
  const zip = new AdmZip();
  for (const [name, content] of writtenJsons.entries()) {
    zip.addFile(`data/${name}.json`, Buffer.from(content, "utf8"));
  }
  for (let idx = 0; idx < writtenRelationships.length; idx++) {
    const rel = writtenRelationships[idx];
    zip.addFile(
      `data/relationship_${idx + 1}.json`,
      Buffer.from(JSON.stringify(rel, null, 2), "utf8"),
    );
  }

  const zipPath = join(outDir, "transformed_output.zip");
  zip.writeZip(zipPath);
  return { writtenJsons, writtenRelationships, zipPath };
}

async function main() {
  const seedArg = process.argv.find((a) => a.startsWith("--seed="));
  const limitArg = process.argv.find((a) => a.startsWith("--limit="));
  const seedPath = seedArg ? seedArg.split("=")[1] : DEFAULT_SEED_CSV;
  const limit = limitArg ? Number.parseInt(limitArg.split("=")[1], 10) : 50;

  console.log(`Starting Montgomery County Local Pilot (limit: ${limit})...`);
  const seedText = await readFile(seedPath, "utf8");
  const rows = parseSeedCsvText(seedText).slice(0, limit);
  console.log(`Loaded ${rows.length} seed rows from ${seedPath}`);

  let successCount = 0;
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const taxpin = row.parcel_id || row.taxpin;
    const parcelDir = join(DEFAULT_OUTPUT_ROOT, `row-${taxpin}`);

    try {
      const payload = await fetchPasdaFeature(taxpin);
      await runLocalTransform(row, payload, parcelDir);
      successCount++;
      if ((i + 1) % 10 === 0 || i === rows.length - 1) {
        console.log(`Transformed ${i + 1}/${rows.length} parcels OK`);
      }
    } catch (err) {
      console.error(`Error transforming parcel ${taxpin}:`, err.message);
    }
  }

  console.log(
    `\nMontgomery County Pilot Complete: ${successCount}/${rows.length} transformed successfully.`,
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
