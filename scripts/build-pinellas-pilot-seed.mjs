#!/usr/bin/env node

import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const GIS_PARCELS_URL =
  "https://egis.pinellas.gov/gis/rest/services/PublicWebGIS/Parcels/MapServer/1";
const GIS_QUERY_URL = `${GIS_PARCELS_URL}/query`;
const PRINT_URL = "https://www.pcpao.gov/property/detail/print";
const COUNTY_NAME = "Pinellas";
const COUNTY_FIPS = "12103";
const DEFAULT_OUTPUT_PATH = "data/seeds/pinellas-pilot.csv";
const TARGET_ROW_COUNT = 50;
const USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

/**
 * Owner and mailing fields from PublicWebGIS that must never enter the seed.
 *
 * @type {readonly string[]}
 */
export const EXCLUDED_PII_FIELDS = Object.freeze([
  "OWNER1",
  "OWNER2",
  "MAILTO",
  "OWNADD_1",
  "OWNADD_2",
  "OWNCITY",
  "OWNSTATE",
  "OWNCOUNTRY",
  "OWNZIP",
]);

/**
 * Non-PII PublicWebGIS fields requested for the Pinellas pilot seed.
 *
 * @type {readonly string[]}
 */
export const SOURCE_FIELDS = Object.freeze([
  "OBJECTID",
  "STRAP",
  "PARCELID",
  "PARCELID_DSP1",
  "USE_CODE",
  "LAND_USE_CODE",
  "SITE_ADDRESS",
  "SITE_CITY",
  "SITE_STATE",
  "SITE_ZIP",
  "SITE_NUM",
  "Acres",
  "TAXABLE_VALUE",
  "LAND_VALUE",
  "IMP_VALUE",
]);

/**
 * Stable CSV column order for the Pinellas pilot seed.
 *
 * @type {readonly string[]}
 */
export const SEED_COLUMNS = Object.freeze([
  "parcel_id",
  "source_identifier",
  "situs_address",
  "method",
  "url",
  "multiValueQueryString",
  "address",
  "city",
  "state",
  "zip",
  "county",
  "county_fips",
  "use_code",
  "use_group",
  "parcelid",
  "parcelid_display",
  "geometry_type",
  "ring_count",
  "vertex_count",
  "acres",
  "latitude",
  "longitude",
  "parcel_polygon",
  "source_url",
  "source_snapshot_at",
]);

/**
 * Mixed property-type quotas that sum to 47. Three additional complex-geometry
 * rows are filled from leftover candidates so the pilot reaches ~50.
 *
 * @type {readonly { useCode: string, count: number, useGroup: string }[]}
 */
export const USE_CODE_QUOTAS = Object.freeze([
  { useCode: "0000", count: 3, useGroup: "vacant-residential" },
  { useCode: "0110", count: 8, useGroup: "single-family" },
  { useCode: "0260", count: 2, useGroup: "manufactured-home" },
  { useCode: "0311", count: 2, useGroup: "apartments" },
  { useCode: "0430", count: 6, useGroup: "condo" },
  { useCode: "0820", count: 3, useGroup: "duplex-triplex" },
  { useCode: "1000", count: 3, useGroup: "vacant-commercial" },
  { useCode: "1120", count: 3, useGroup: "store" },
  { useCode: "1730", count: 4, useGroup: "office" },
  { useCode: "2048", count: 1, useGroup: "marina" },
  { useCode: "3912", count: 1, useGroup: "hotel" },
  { useCode: "4000", count: 2, useGroup: "vacant-industrial" },
  { useCode: "4120", count: 3, useGroup: "light-manufacturing" },
  { useCode: "4800", count: 2, useGroup: "warehouse" },
  { useCode: "7153", count: 2, useGroup: "institutional" },
  { useCode: "8012", count: 2, useGroup: "government" },
]);

/**
 * @typedef {object} GeoJsonPolygon
 * @property {"Polygon"} type
 * @property {number[][][]} coordinates
 */

/**
 * @typedef {object} GeoJsonMultiPolygon
 * @property {"MultiPolygon"} type
 * @property {number[][][][]} coordinates
 */

/**
 * @typedef {GeoJsonPolygon | GeoJsonMultiPolygon} ParcelGeometry
 */

/**
 * @typedef {object} GisFeature
 * @property {Record<string, unknown>} properties
 * @property {ParcelGeometry | null} geometry
 */

/**
 * @typedef {object} GeometryStats
 * @property {"simple-polygon" | "complex-polygon" | "multi-polygon" | "empty"} geometryType
 * @property {number} ringCount
 * @property {number} vertexCount
 * @property {string} latitude
 * @property {string} longitude
 */

/**
 * @typedef {object} SeedBuildStats
 * @property {number} rowsWritten
 * @property {number} uniqueParcelIds
 * @property {number} simplePolygonCount
 * @property {number} complexPolygonCount
 * @property {number} multiPolygonCount
 * @property {string[]} useGroups
 * @property {string} outputPath
 */

/**
 * Throw if a prohibited PII field is present in the source allow-list.
 *
 * @param {readonly string[]} fields - Candidate GIS outFields.
 * @returns {void}
 */
export function assertSafeSourceFields(fields) {
  for (const field of fields) {
    if (EXCLUDED_PII_FIELDS.includes(field)) {
      throw new Error(
        `PII field is prohibited in Pinellas seed source fields: ${field}`,
      );
    }
  }
}

/**
 * Return whether a value is an 18-digit Pinellas STRAP.
 *
 * @param {unknown} value - Candidate parcel identifier.
 * @returns {boolean} True only for exactly 18 digits.
 */
export function isValidStrap(value) {
  return /^[0-9]{18}$/.test(toText(value));
}

/**
 * Coerce an unknown GIS attribute to a trimmed string.
 *
 * @param {unknown} value - Attribute value.
 * @returns {string} Trimmed text, or empty string.
 */
export function toText(value) {
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

/**
 * Build the PCPAO print-page URL for a STRAP. This is the lookup the seed
 * `parcel_id` must satisfy.
 *
 * @param {string} strap - 18-digit STRAP.
 * @returns {string} Absolute print URL.
 */
export function buildPrintUrl(strap) {
  return `${PRINT_URL}?is_print=1&s=${encodeURIComponent(strap)}`;
}

/**
 * Classify WGS84 parcel geometry for the mixed-type pilot.
 *
 * @param {ParcelGeometry | null} geometry - GeoJSON geometry.
 * @returns {GeometryStats} Geometry classification and centroid.
 */
export function classifyGeometry(geometry) {
  if (geometry === null || !Array.isArray(geometry.coordinates)) {
    return {
      geometryType: "empty",
      ringCount: 0,
      vertexCount: 0,
      latitude: "",
      longitude: "",
    };
  }
  /** @type {number[][][]} */
  const rings = [];
  if (geometry.type === "Polygon") {
    for (const ring of geometry.coordinates) rings.push(ring);
  } else if (geometry.type === "MultiPolygon") {
    for (const polygon of geometry.coordinates) {
      for (const ring of polygon) rings.push(ring);
    }
  }
  const vertexCount = rings.reduce((sum, ring) => sum + ring.length, 0);
  const ringCount = rings.length;
  const geometryType =
    ringCount > 1
      ? "multi-polygon"
      : vertexCount > 20
        ? "complex-polygon"
        : "simple-polygon";
  const firstPoint = rings[0]?.[0];
  return {
    geometryType,
    ringCount,
    vertexCount,
    longitude: firstPoint ? String(firstPoint[0]) : "",
    latitude: firstPoint ? String(firstPoint[1]) : "",
  };
}

/**
 * Convert a PublicWebGIS feature into one Pinellas seed row.
 *
 * @param {GisFeature} feature - GIS feature with WGS84 geometry.
 * @param {string} useGroup - Quota label (single-family, condo, …).
 * @param {string} snapshotAt - ISO timestamp shared by the snapshot.
 * @returns {Record<string, string>} CSV row keyed by `SEED_COLUMNS`.
 */
export function toSeedRow(feature, useGroup, snapshotAt) {
  const properties = feature.properties;
  const strap = toText(properties.STRAP);
  if (!isValidStrap(strap)) {
    throw new Error(`Invalid STRAP for seed row: ${strap}`);
  }
  const stats = classifyGeometry(feature.geometry);
  const city = toText(properties.SITE_CITY);
  const zip = toText(properties.SITE_ZIP);
  const street = toText(properties.SITE_ADDRESS);
  const addressParts = [
    street,
    [city, "FL", zip].filter(Boolean).join(" "),
  ].filter((part) => part.length > 0);
  const address = addressParts.join(", ");
  return {
    parcel_id: strap,
    source_identifier: strap,
    situs_address: address,
    method: "GET",
    url: PRINT_URL,
    multiValueQueryString: JSON.stringify({
      is_print: ["1"],
      s: [strap],
    }),
    address,
    city,
    state: toText(properties.SITE_STATE) || "FL",
    zip,
    county: COUNTY_NAME,
    county_fips: COUNTY_FIPS,
    use_code: toText(properties.USE_CODE),
    use_group: useGroup,
    parcelid: toText(properties.PARCELID),
    parcelid_display: toText(properties.PARCELID_DSP1),
    geometry_type: stats.geometryType,
    ring_count: String(stats.ringCount),
    vertex_count: String(stats.vertexCount),
    acres: toText(properties.Acres),
    latitude: stats.latitude,
    longitude: stats.longitude,
    parcel_polygon:
      feature.geometry === null ? "" : JSON.stringify(feature.geometry),
    source_url: GIS_PARCELS_URL,
    source_snapshot_at: snapshotAt,
  };
}

/**
 * Deduplicate candidate rows by STRAP, keeping the first occurrence.
 *
 * @param {readonly Record<string, string>[]} rows - Candidate seed rows.
 * @returns {Record<string, string>[]} Unique STRAP rows.
 */
export function dedupeByStrap(rows) {
  const seen = new Set();
  /** @type {Record<string, string>[]} */
  const unique = [];
  for (const row of rows) {
    const strap = row.parcel_id;
    if (!isValidStrap(strap)) {
      throw new Error(`Cannot stage a non-STRAP parcel_id: ${strap}`);
    }
    if (seen.has(strap)) continue;
    seen.add(strap);
    unique.push(row);
  }
  return unique;
}

/**
 * Pick a mixed simple/complex subset from one use-code candidate pool.
 *
 * @param {readonly Record<string, string>[]} candidates - Rows for one use code.
 * @param {number} count - Desired count.
 * @returns {Record<string, string>[]} Selected rows.
 */
export function pickMixedGeometry(candidates, count) {
  const complex = candidates.filter(
    (row) =>
      row.geometry_type === "complex-polygon" ||
      row.geometry_type === "multi-polygon",
  );
  const simple = candidates.filter(
    (row) => row.geometry_type === "simple-polygon",
  );
  /** @type {Record<string, string>[]} */
  const picked = [];
  if (count > 1 && complex.length > 0) {
    picked.push(complex[0]);
  }
  for (const row of [...simple, ...complex]) {
    if (picked.length >= count) break;
    if (picked.includes(row)) continue;
    picked.push(row);
  }
  return picked.slice(0, count);
}

/**
 * Encode a CSV cell, quoting when required.
 *
 * @param {string} value - Cell text.
 * @returns {string} Encoded cell.
 */
export function encodeCsvCell(value) {
  if (!/[",\r\n]/.test(value)) return value;
  return `"${value.replace(/"/g, '""')}"`;
}

/**
 * Render one complete CSV row in stable column order.
 *
 * @param {Record<string, string>} row - Seed row keyed by `SEED_COLUMNS`.
 * @returns {string} CSV row ending in a newline.
 */
export function renderCsvRow(row) {
  return `${SEED_COLUMNS.map((column) => encodeCsvCell(row[column] ?? "")).join(",")}\n`;
}

/**
 * @typedef {object} GisClient
 * @property {(useCode: string, recordCount: number) => Promise<GisFeature[]>} queryByUseCode
 */

/**
 * Build a GIS query URL for one use code.
 *
 * @param {string} useCode - Four-digit PCPAO use code.
 * @param {number} recordCount - Max records to return.
 * @returns {URL} Query URL.
 */
export function buildUseCodeQueryUrl(useCode, recordCount) {
  const url = new URL(GIS_QUERY_URL);
  url.searchParams.set("where", `USE_CODE='${useCode}' AND STRAP IS NOT NULL`);
  url.searchParams.set("outFields", SOURCE_FIELDS.join(","));
  url.searchParams.set("returnGeometry", "true");
  url.searchParams.set("outSR", "4326");
  url.searchParams.set("f", "geojson");
  url.searchParams.set("resultRecordCount", String(recordCount));
  return url;
}

/**
 * Parse an ArcGIS GeoJSON feature collection into GIS features.
 *
 * @param {unknown} payload - Parsed GeoJSON body.
 * @returns {GisFeature[]} Features with properties + geometry.
 */
export function parseGisFeatureCollection(payload) {
  if (
    payload === null ||
    typeof payload !== "object" ||
    !("features" in payload) ||
    !Array.isArray(payload.features)
  ) {
    throw new Error("PublicWebGIS response is missing a features array");
  }
  if ("error" in payload && payload.error) {
    throw new Error(`PublicWebGIS error: ${JSON.stringify(payload.error)}`);
  }
  /** @type {GisFeature[]} */
  const features = [];
  for (const raw of payload.features) {
    if (raw === null || typeof raw !== "object") continue;
    const record =
      /** @type {{ properties?: Record<string, unknown>, geometry?: ParcelGeometry | null }} */ (
        raw
      );
    features.push({
      properties: record.properties ?? {},
      geometry: record.geometry ?? null,
    });
  }
  return features;
}

/**
 * @param {string} html - PCPAO print HTML.
 * @returns {boolean} True when the page contains real parcel summary data.
 */
export function printHtmlLooksPopulated(html) {
  return (
    /Parcel Summary/i.test(html) &&
    /Owner Name/i.test(html) &&
    !/Buildings\s+0\s+Parcel Map/i.test(html) &&
    !/No Property Values on Record/i.test(html)
  );
}

/**
 * Default GIS client that queries PublicWebGIS.
 *
 * @type {GisClient}
 */
const defaultGisClient = {
  /**
   * @param {string} useCode
   * @param {number} recordCount
   * @returns {Promise<GisFeature[]>}
   */
  async queryByUseCode(useCode, recordCount) {
    const url = buildUseCodeQueryUrl(useCode, recordCount);
    const response = await fetch(url, {
      headers: {
        Accept: "application/json, application/geo+json",
        "User-Agent": USER_AGENT,
      },
    });
    if (!response.ok) {
      throw new Error(
        `PublicWebGIS ${useCode} failed: HTTP ${response.status}`,
      );
    }
    const payload = await response.json();
    return parseGisFeatureCollection(payload);
  },
};

/**
 * Build the mixed ~50-parcel Pinellas pilot seed.
 *
 * @param {GisClient} [gisClient] - GIS query client.
 * @param {string} [snapshotAt] - Shared snapshot timestamp.
 * @returns {Promise<Record<string, string>[]>} Deduplicated seed rows.
 */
export async function buildPilotRows(
  gisClient = defaultGisClient,
  snapshotAt = new Date().toISOString(),
) {
  assertSafeSourceFields(SOURCE_FIELDS);
  /** @type {Record<string, string>[]} */
  const selected = [];
  /** @type {Record<string, string>[]} */
  const leftovers = [];

  for (const quota of USE_CODE_QUOTAS) {
    const features = await gisClient.queryByUseCode(
      quota.useCode,
      Math.max(quota.count * 5, 15),
    );
    const candidates = features
      .filter((feature) => isValidStrap(feature.properties.STRAP))
      .filter((feature) => feature.geometry !== null)
      .map((feature) => toSeedRow(feature, quota.useGroup, snapshotAt));
    const unique = dedupeByStrap(candidates);
    const picked = pickMixedGeometry(unique, quota.count);
    selected.push(...picked);
    leftovers.push(
      ...unique.filter(
        (row) => !picked.some((item) => item.parcel_id === row.parcel_id),
      ),
    );
    if (picked.length < quota.count) {
      console.warn(
        `Use code ${quota.useCode} (${quota.useGroup}) only yielded ${picked.length}/${quota.count}`,
      );
    }
  }

  const uniqueSelected = dedupeByStrap(selected);
  const need = TARGET_ROW_COUNT - uniqueSelected.length;
  if (need > 0) {
    const complexFill = leftovers.filter(
      (row) =>
        row.geometry_type === "complex-polygon" ||
        row.geometry_type === "multi-polygon",
    );
    uniqueSelected.push(
      ...dedupeByStrap([...uniqueSelected, ...complexFill, ...leftovers]).slice(
        uniqueSelected.length,
        uniqueSelected.length + need,
      ),
    );
  }

  const staged = dedupeByStrap(uniqueSelected).slice(0, TARGET_ROW_COUNT);
  if (staged.length < 40) {
    throw new Error(
      `Pilot seed too small: ${staged.length} rows (need at least 40 mixed parcels)`,
    );
  }
  return staged;
}

/**
 * Write the seed CSV and return build stats.
 *
 * @param {string} outputPath - Destination CSV path.
 * @param {readonly Record<string, string>[]} rows - Seed rows.
 * @returns {Promise<SeedBuildStats>} Staging stats.
 */
export async function writeSeedCsv(outputPath, rows) {
  const unique = dedupeByStrap(rows);
  await mkdir(path.dirname(outputPath), { recursive: true });
  const body = `${SEED_COLUMNS.join(",")}\n${unique.map(renderCsvRow).join("")}`;
  await writeFile(outputPath, body, "utf8");
  return {
    rowsWritten: unique.length,
    uniqueParcelIds: unique.length,
    simplePolygonCount: unique.filter(
      (row) => row.geometry_type === "simple-polygon",
    ).length,
    complexPolygonCount: unique.filter(
      (row) => row.geometry_type === "complex-polygon",
    ).length,
    multiPolygonCount: unique.filter(
      (row) => row.geometry_type === "multi-polygon",
    ).length,
    useGroups: [...new Set(unique.map((row) => row.use_group))],
    outputPath,
  };
}

/**
 * Probe the PCPAO print page for a STRAP and fail loud on empty lookups.
 *
 * @param {string} strap - 18-digit STRAP.
 * @param {(url: string) => Promise<string>} [fetchHtml] - HTML getter.
 * @returns {Promise<{ strap: string, ok: true, bytes: number }>} Successful probe.
 */
export async function assertPrintLookup(
  strap,
  fetchHtml = async (url) => {
    const response = await fetch(url, {
      headers: { Accept: "text/html", "User-Agent": USER_AGENT },
    });
    if (!response.ok) {
      throw new Error(
        `Print lookup HTTP ${response.status} for STRAP ${strap}`,
      );
    }
    return response.text();
  },
) {
  if (!isValidStrap(strap)) {
    throw new Error(`Cannot probe a non-STRAP id: ${strap}`);
  }
  const html = await fetchHtml(buildPrintUrl(strap));
  if (!printHtmlLooksPopulated(html)) {
    throw new Error(
      `Print lookup for STRAP ${strap} returned an empty/placeholder page — treat as a hard error, not a skip`,
    );
  }
  return { strap, ok: true, bytes: Buffer.byteLength(html) };
}

/**
 * Parse CLI options.
 *
 * @param {readonly string[]} argv - Process arguments.
 * @returns {{ outputPath: string, skipValidate: boolean }} Options.
 */
export function parseCliOptions(argv) {
  /** @type {{ outputPath: string, skipValidate: boolean }} */
  const options = { outputPath: DEFAULT_OUTPUT_PATH, skipValidate: false };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--output") {
      options.outputPath = argv[index + 1];
      index += 1;
    } else if (arg === "--skip-validate") {
      options.skipValidate = true;
    } else if (arg === "--help") {
      console.log(`
Usage:
  node scripts/build-pinellas-pilot-seed.mjs [--output data/seeds/pinellas-pilot.csv]
`);
      process.exit(0);
    }
  }
  return options;
}

/**
 * @returns {Promise<void>}
 */
async function main() {
  const options = parseCliOptions(process.argv.slice(2));
  const rows = await buildPilotRows();
  const stats = await writeSeedCsv(options.outputPath, rows);
  console.log(JSON.stringify(stats, null, 2));
  if (!options.skipValidate) {
    const sample = rows.slice(0, 5);
    for (const row of sample) {
      const result = await assertPrintLookup(row.parcel_id);
      console.log(`validated ${result.strap} bytes=${result.bytes}`);
    }
  }
}

const isDirectRun =
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href;

if (isDirectRun) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
