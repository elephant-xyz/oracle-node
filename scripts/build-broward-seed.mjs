#!/usr/bin/env node

/**
 * Build a Broward County seed CSV from the public BCPA GIS parcel layer.
 *
 * GIS exposes FOLIO + polygon only. Appraisal facts come from the BCPA JSON
 * detail API during prepare (`multi-request-flows/Broward.json`). Folio values
 * stay text so condo letters survive.
 */

import { createWriteStream } from "fs";
import { mkdir } from "fs/promises";
import path from "path";
import { pathToFileURL } from "url";

import {
  BROWARD_COUNTY_FIPS,
  BROWARD_COUNTY_NAME,
  BROWARD_DETAIL_URL,
  BROWARD_GIS_LAYER_URL,
  BROWARD_PILOT_FOLIOS,
  normalizeBrowardFolio,
} from "./broward-folio.mjs";

const FEATURE_QUERY_URL = `${BROWARD_GIS_LAYER_URL}/query`;
const DEFAULT_OUTPUT_PATH = "downloads/broward/broward.csv";
const DEFAULT_PILOT_OUTPUT_PATH = "downloads/broward/broward-pilot.csv";
const DEFAULT_PAGE_SIZE = 1_000;
const GIS_MAX_RECORD_COUNT = 1_000;

/**
 * Canonical seed header. `request_identifier` is required by elephant-cli
 * prepare templating (`{{=it.request_identifier}}`).
 *
 * @type {readonly string[]}
 */
export const SEED_COLUMNS = Object.freeze([
  "parcel_id",
  "source_identifier",
  "request_identifier",
  "method",
  "url",
  "address",
  "city",
  "state",
  "zip",
  "county",
  "county_fips",
  "latitude",
  "longitude",
  "parcel_polygon",
  "source_url",
]);

/**
 * @typedef {object} CliOptions
 * @property {string} outputPath - Local CSV destination.
 * @property {number} pageSize - GIS records requested per page.
 * @property {number} limit - Maximum seed rows to write; 0 means no cap.
 * @property {boolean} pilot - Query only {@link BROWARD_PILOT_FOLIOS}.
 */

/**
 * @typedef {object} GeoJsonPosition
 * @property {number} 0 - Longitude.
 * @property {number} 1 - Latitude.
 */

/**
 * @typedef {object} GeoJsonGeometry
 * @property {string} type - GeoJSON geometry type.
 * @property {unknown} coordinates - Geometry coordinates.
 */

/**
 * @typedef {object} GeoJsonFeature
 * @property {Record<string, unknown>} properties - GIS attributes.
 * @property {GeoJsonGeometry | null} geometry - WGS84 parcel geometry.
 */

/**
 * Print command usage.
 *
 * @returns {void}
 */
function showUsage() {
  console.log(`
Usage:
  node scripts/build-broward-seed.mjs [options]

Options:
  --output <path>   Local CSV destination.
                    Default: ${DEFAULT_OUTPUT_PATH} (or ${DEFAULT_PILOT_OUTPUT_PATH} with --pilot)
  --page-size <n>   GIS records per page. Default: ${DEFAULT_PAGE_SIZE}. Max: ${GIS_MAX_RECORD_COUNT}.
  --limit <n>       Stop after N seed rows. Default: 0 (all).
  --pilot           Build only the curated 25-parcel pilot set.
  --help            Show this help.
`);
}

/**
 * Parse command-line flags.
 *
 * @param {readonly string[]} argv - Arguments after the script filename.
 * @returns {CliOptions} Validated options.
 */
export function parseCliOptions(argv) {
  /** @type {CliOptions} */
  const options = {
    outputPath: DEFAULT_OUTPUT_PATH,
    pageSize: DEFAULT_PAGE_SIZE,
    limit: 0,
    pilot: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help" || token === "-h") {
      showUsage();
      process.exit(0);
    }
    if (token === "--pilot") {
      options.pilot = true;
      continue;
    }
    const value = argv[index + 1];
    if (typeof value !== "string" || value.startsWith("--")) {
      throw new Error(`Missing value for ${token}`);
    }
    if (token === "--output") {
      options.outputPath = value;
    } else if (token === "--page-size") {
      options.pageSize = parsePositiveInteger(token, value);
    } else if (token === "--limit") {
      options.limit = parseNonNegativeInteger(token, value);
    } else {
      throw new Error(`Unknown option: ${token}`);
    }
    index += 1;
  }
  if (options.pageSize > GIS_MAX_RECORD_COUNT) {
    throw new Error(
      `--page-size cannot exceed the GIS maxRecordCount of ${String(GIS_MAX_RECORD_COUNT)}`,
    );
  }
  if (options.pilot && options.outputPath === DEFAULT_OUTPUT_PATH) {
    options.outputPath = DEFAULT_PILOT_OUTPUT_PATH;
  }
  return options;
}

/**
 * @param {string} optionName - Flag name.
 * @param {string} rawValue - Raw flag value.
 * @returns {number} Positive integer.
 */
function parsePositiveInteger(optionName, rawValue) {
  const parsed = Number.parseInt(rawValue, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${optionName} must be a positive integer`);
  }
  return parsed;
}

/**
 * @param {string} optionName - Flag name.
 * @param {string} rawValue - Raw flag value.
 * @returns {number} Non-negative integer.
 */
function parseNonNegativeInteger(optionName, rawValue) {
  const parsed = Number.parseInt(rawValue, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`${optionName} must be a non-negative integer`);
  }
  return parsed;
}

/**
 * RFC4180-style CSV cell encoding.
 *
 * @param {string | undefined} value - Raw cell.
 * @returns {string} Encoded cell.
 */
export function encodeCsvCell(value) {
  const text = value ?? "";
  if (!/[",\r\n]/.test(text)) return text;
  return `"${text.replace(/"/g, '""')}"`;
}

/**
 * @param {Record<string, string>} row - Seed row.
 * @returns {string} CSV line including trailing newline.
 */
export function renderCsvRow(row) {
  return `${SEED_COLUMNS.map((column) => encodeCsvCell(row[column] ?? "")).join(",")}\n`;
}

/**
 * Average the exterior ring into a WGS84 centroid. Returns empty strings when
 * the geometry is missing or degenerate.
 *
 * @param {GeoJsonGeometry | null | undefined} geometry - Parcel geometry.
 * @returns {{ latitude: string, longitude: string }} Coordinate strings.
 */
export function centroidFromGeometry(geometry) {
  if (geometry === null || geometry === undefined) {
    return { latitude: "", longitude: "" };
  }
  /** @type {unknown} */
  let ring = null;
  if (geometry.type === "Polygon" && Array.isArray(geometry.coordinates)) {
    ring = geometry.coordinates[0];
  } else if (
    geometry.type === "MultiPolygon" &&
    Array.isArray(geometry.coordinates)
  ) {
    const firstPolygon = geometry.coordinates[0];
    ring = Array.isArray(firstPolygon) ? firstPolygon[0] : null;
  }
  if (!Array.isArray(ring) || ring.length === 0) {
    return { latitude: "", longitude: "" };
  }
  let sumLon = 0;
  let sumLat = 0;
  let count = 0;
  const exclusiveEnd = ring.length > 1 ? ring.length - 1 : ring.length;
  for (let index = 0; index < exclusiveEnd; index += 1) {
    const position = ring[index];
    if (!Array.isArray(position) || position.length < 2) continue;
    const lon = Number(position[0]);
    const lat = Number(position[1]);
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;
    sumLon += lon;
    sumLat += lat;
    count += 1;
  }
  if (count === 0) return { latitude: "", longitude: "" };
  return {
    latitude: (sumLat / count).toFixed(8),
    longitude: (sumLon / count).toFixed(8),
  };
}

/**
 * Convert a GIS GeoJSON feature into a Broward seed row.
 *
 * @param {GeoJsonFeature} feature - GIS feature.
 * @returns {Record<string, string> | undefined} Seed row, or undefined when the folio is invalid.
 */
export function toSeedRow(feature) {
  const folio = normalizeBrowardFolio(feature.properties?.FOLIO);
  if (folio === undefined) return undefined;
  const { latitude, longitude } = centroidFromGeometry(feature.geometry);
  return {
    parcel_id: folio,
    source_identifier: folio,
    request_identifier: folio,
    method: "POST",
    url: BROWARD_DETAIL_URL,
    address: "",
    city: "",
    state: "FL",
    zip: "",
    county: BROWARD_COUNTY_NAME,
    county_fips: BROWARD_COUNTY_FIPS,
    latitude,
    longitude,
    parcel_polygon:
      feature.geometry === null || feature.geometry === undefined
        ? ""
        : JSON.stringify(feature.geometry),
    source_url: FEATURE_QUERY_URL,
  };
}

/**
 * @param {string} where - ArcGIS where clause.
 * @param {number} resultOffset - Page offset.
 * @param {number} pageSize - Page size.
 * @returns {string} Query URL.
 */
export function buildPageUrl(where, resultOffset, pageSize) {
  const params = new URLSearchParams({
    where,
    outFields: "FOLIO",
    returnGeometry: "true",
    outSR: "4326",
    f: "geojson",
    resultRecordCount: String(pageSize),
    resultOffset: String(resultOffset),
  });
  return `${FEATURE_QUERY_URL}?${params.toString()}`;
}

/**
 * @param {readonly string[]} folios - Normalized folios.
 * @returns {string} ArcGIS where clause.
 */
export function buildFolioWhere(folios) {
  const quoted = folios.map((folio) => `'${folio.replace(/'/g, "''")}'`);
  return `FOLIO IN (${quoted.join(",")})`;
}

/**
 * @param {string} url - GIS query URL.
 * @returns {Promise<{ features: GeoJsonFeature[] }>} GeoJSON feature collection.
 */
async function fetchGeoJson(url) {
  const response = await fetch(url, {
    headers: { Accept: "application/geo+json, application/json" },
  });
  if (!response.ok) {
    throw new Error(`Broward GIS returned HTTP ${String(response.status)}`);
  }
  const body =
    /** @type {{ features?: GeoJsonFeature[], error?: { message?: string } }} */ (
      await response.json()
    );
  if (body.error?.message) {
    throw new Error(`Broward GIS error: ${body.error.message}`);
  }
  if (!Array.isArray(body.features)) {
    throw new Error("Broward GIS response is not a GeoJSON feature collection");
  }
  return { features: body.features };
}

/**
 * @param {CliOptions} options - Build options.
 * @returns {Promise<{ rowsWritten: number, uniqueFolios: number, skippedInvalid: number }>}
 *   Write stats.
 */
export async function buildBrowardSeed(options) {
  await mkdir(path.dirname(options.outputPath), { recursive: true });
  const stream = createWriteStream(options.outputPath);
  stream.write(`${SEED_COLUMNS.join(",")}\n`);
  const seen = new Set();
  let rowsWritten = 0;
  let skippedInvalid = 0;
  const where = options.pilot
    ? buildFolioWhere([...BROWARD_PILOT_FOLIOS])
    : "1=1";
  let resultOffset = 0;
  let pageHasRecords = true;
  while (pageHasRecords) {
    if (options.limit > 0 && rowsWritten >= options.limit) break;
    const page = await fetchGeoJson(
      buildPageUrl(where, resultOffset, options.pageSize),
    );
    if (page.features.length === 0) {
      pageHasRecords = false;
      break;
    }
    for (const feature of page.features) {
      if (options.limit > 0 && rowsWritten >= options.limit) break;
      const row = toSeedRow(feature);
      if (row === undefined) {
        skippedInvalid += 1;
        continue;
      }
      if (seen.has(row.parcel_id)) continue;
      seen.add(row.parcel_id);
      stream.write(renderCsvRow(row));
      rowsWritten += 1;
    }
    resultOffset += page.features.length;
    if (page.features.length < options.pageSize) {
      pageHasRecords = false;
    }
  }
  await new Promise((resolve, reject) => {
    stream.end((error) => {
      if (error) reject(error);
      else resolve(undefined);
    });
  });
  return { rowsWritten, uniqueFolios: seen.size, skippedInvalid };
}

/**
 * @returns {Promise<void>}
 */
async function main() {
  const options = parseCliOptions(process.argv.slice(2));
  const stats = await buildBrowardSeed(options);
  console.log(
    JSON.stringify({
      level: "info",
      message: "broward_seed_build_complete",
      outputPath: options.outputPath,
      pilot: options.pilot,
      ...stats,
    }),
  );
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(JSON.stringify({ level: "error", message }));
    process.exit(1);
  });
}
