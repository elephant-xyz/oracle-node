#!/usr/bin/env node
/**
 * Build a full Hillsborough seed CSV from HC_ParcelsPublic (ArcGIS FeatureServer).
 * Streams pages to disk so a ~500k county does not OOM; supports --resume.
 *
 * Usage:
 *   node scripts/hillsborough/build-full-seed.mjs
 *   node scripts/hillsborough/build-full-seed.mjs --out=downloads/hillsborough/full-seed.csv --page-size=1000
 *   node scripts/hillsborough/build-full-seed.mjs --resume
 */

import { createWriteStream } from "node:fs";
import { access, mkdir, readFile, stat } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { finished } from "node:stream/promises";

import { GIS_PARCELS_URL } from "./lib.mjs";
import { withTransientRetry } from "./run-state.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "../..");
const DEFAULT_OUT = resolve(ROOT, "downloads/hillsborough/full-seed.csv");

const HEADER = [
  "parcel_id",
  "source_identifier",
  "folio",
  "display_folio",
  "pin",
  "display_pin",
  "address",
  "street",
  "city",
  "zip",
  "owner",
  "land_use",
  "longitude",
  "latitude",
  "parcel_polygon",
];

/**
 * @param {string[]} argv
 * @returns {{ outPath: string; pageSize: number; maxPages: number | null; resume: boolean }}
 */
function parseArgs(argv) {
  const maxRaw = argv.find((a) => a.startsWith("--max-pages="))?.split("=")[1];
  return {
    outPath: resolve(
      ROOT,
      argv.find((a) => a.startsWith("--out="))?.split("=")[1] ?? DEFAULT_OUT,
    ),
    pageSize: Number.parseInt(
      argv.find((a) => a.startsWith("--page-size="))?.split("=")[1] ?? "1000",
      10,
    ),
    maxPages: maxRaw === undefined ? null : Number.parseInt(maxRaw, 10),
    resume: argv.includes("--resume"),
  };
}

/**
 * @param {unknown} geometry
 * @returns {{ lon: string; lat: string; wkt: string }}
 */
function geometryToFields(geometry) {
  const g = /** @type {{ rings?: number[][][]; x?: number; y?: number }} */ (
    geometry || {}
  );
  if (Array.isArray(g.rings) && g.rings[0]?.length) {
    const ring = g.rings[0];
    let sumX = 0;
    let sumY = 0;
    const coords = ring.map(([x, y]) => {
      sumX += x;
      sumY += y;
      return `${x} ${y}`;
    });
    if (coords[0] !== coords[coords.length - 1]) {
      coords.push(coords[0]);
    }
    return {
      lon: String(sumX / ring.length),
      lat: String(sumY / ring.length),
      wkt: `POLYGON((${coords.join(", ")}))`,
    };
  }
  return {
    lon: g.x != null ? String(g.x) : "",
    lat: g.y != null ? String(g.y) : "",
    wkt: "",
  };
}

/**
 * @param {string} value
 * @returns {string}
 */
function csvEscape(value) {
  const text = String(value ?? "");
  if (/[",\n\r]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

/**
 * @param {string} path
 * @returns {Promise<boolean>}
 */
async function pathExists(path) {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

/**
 * @returns {Promise<void>}
 */
async function main() {
  const options = parseArgs(process.argv.slice(2));
  await mkdir(dirname(options.outPath), { recursive: true });

  /** @type {Set<string>} */
  const seen = new Set();
  let offset = 0;
  let page = 0;
  let append = false;

  if (options.resume && (await pathExists(options.outPath))) {
    const existing = await readFile(options.outPath, "utf8");
    for (const line of existing.split("\n").slice(1)) {
      if (!line.trim()) continue;
      const folio = line.split(",")[0]?.replace(/^"|"$/g, "");
      if (folio) seen.add(folio);
    }
    offset = seen.size;
    page = Math.floor(offset / options.pageSize);
    append = true;
    console.log(
      JSON.stringify({
        event: "seed_resume",
        existingFolios: seen.size,
        offset,
      }),
    );
  }

  const stream = createWriteStream(options.outPath, {
    flags: append ? "a" : "w",
  });
  if (!append) {
    stream.write(`${HEADER.join(",")}\n`);
  }

  let exceededTransfer = true;
  while (exceededTransfer) {
    if (options.maxPages != null && page >= options.maxPages) break;
    const url = new URL(GIS_PARCELS_URL);
    url.searchParams.set("where", "1=1");
    url.searchParams.set("outFields", "*");
    url.searchParams.set("returnGeometry", "true");
    url.searchParams.set("outSR", "4326");
    url.searchParams.set("f", "json");
    url.searchParams.set("resultOffset", String(offset));
    url.searchParams.set("resultRecordCount", String(options.pageSize));

    const body = await withTransientRetry(
      async () => {
        const response = await fetch(url);
        if (!response.ok) {
          throw new Error(
            `GIS query HTTP ${response.status} at offset=${offset}`,
          );
        }
        const parsed = /** @type {{
          features?: Array<{ attributes?: Record<string, unknown>; geometry?: unknown }>;
          exceededTransferLimit?: boolean;
          error?: { message?: string };
        }} */ (await response.json());
        if (parsed.error) {
          throw new Error(parsed.error.message || "GIS query error");
        }
        return parsed;
      },
      {
        maxAttempts: 5,
        baseDelayMs: 1500,
        onRetry: ({ attempt, error }) => {
          console.warn(
            JSON.stringify({
              event: "seed_page_retry",
              page,
              offset,
              attempt,
              error: error instanceof Error ? error.message : String(error),
            }),
          );
        },
      },
    );
    const features = body.features || [];
    let written = 0;
    for (const feature of features) {
      const a = feature.attributes || {};
      const folio = String(a.FOLIO ?? a.folio ?? "").replace(/\D/g, "");
      if (!folio || seen.has(folio)) continue;
      seen.add(folio);
      const pin = String(a.STRAP ?? a.PIN ?? a.pin ?? "");
      const street = String(a.SITE_ADDR ?? a.ADDRESS ?? a.SITEADDR ?? "");
      const city = String(a.CITY ?? a.SITE_CITY ?? "");
      const zip = String(a.ZIP ?? a.SITE_ZIP ?? "");
      const address = [street, city, zip].filter(Boolean).join(", ");
      const geo = geometryToFields(feature.geometry);
      stream.write(
        `${[
          folio,
          pin,
          folio,
          folio,
          pin,
          pin,
          address,
          street,
          city,
          zip,
          String(a.OWNER ?? a.OWNER_NAME ?? ""),
          String(a.LAND_USE ?? a.DOR_UC ?? a.USE_CODE ?? ""),
          geo.lon,
          geo.lat,
          geo.wkt,
        ]
          .map(csvEscape)
          .join(",")}\n`,
      );
      written += 1;
    }

    page += 1;
    offset += features.length;
    exceededTransfer =
      Boolean(body.exceededTransferLimit) && features.length > 0;
    console.log(
      JSON.stringify({
        event: "seed_page",
        page,
        offset,
        pageFeatures: features.length,
        written,
        uniqueFolios: seen.size,
        exceededTransferLimit: exceededTransfer,
      }),
    );
    if (features.length === 0) break;
  }

  stream.end();
  await finished(stream);
  const fileStat = await stat(options.outPath);
  console.log(
    JSON.stringify({
      event: "seed_complete",
      outPath: options.outPath,
      uniqueFolios: seen.size,
      pages: page,
      bytes: fileStat.size,
    }),
  );
}

main().catch((error) => {
  console.error(
    error instanceof Error ? (error.stack ?? error.message) : error,
  );
  process.exitCode = 1;
});
