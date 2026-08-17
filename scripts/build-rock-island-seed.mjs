#!/usr/bin/env node

import { once } from "events";
import { createWriteStream } from "fs";
import { mkdir } from "fs/promises";
import path from "path";
import { pathToFileURL } from "url";

const FEATURE_LAYER_URL =
  "https://services9.arcgis.com/6FnscPPlUa9DXXOk/ArcGIS/rest/services/Parcels/FeatureServer/0";
const FEATURE_QUERY_URL = `${FEATURE_LAYER_URL}/query`;
const SOURCE_ITEM_ID = "9cae8a64ab0e4cea99758f741ca43b3c";
const DEFAULT_OUTPUT_PATH = "downloads/rock-island/rock-island.csv";
const DEFAULT_PAGE_SIZE = 2_000;
const DEFAULT_CONCURRENCY = 2;
const COUNTY_NAME = "Rock Island";
const COUNTY_FIPS = "17161";

/**
 * Source fields deliberately excluded from every request and output because they
 * identify owners or tax-bill recipients. Keeping this list beside the allow-list
 * makes the public-export boundary reviewable and testable.
 *
 * @type {readonly string[]}
 */
export const EXCLUDED_PII_FIELDS = Object.freeze([
  "taxbill_name",
  "Taxbill_last",
  "Taxbill_first",
  "taxbill_addr1",
  "taxbill_addr2",
  "taxbill_addr",
  "taxbill_csz",
  "Taxbill_CS",
  "Taxbill_zip",
  "owner1_name",
  "owner1_address1",
  "owner1_address2",
  "owner1_csz",
  "Owner_city",
  "Owner_state",
  "Owner_Zip",
]);

/**
 * Explicit non-PII allow-list requested from the county FeatureServer. Never
 * replace this with `outFields=*`; new source columns must undergo a privacy and
 * provenance review before entering the seed.
 *
 * @type {readonly string[]}
 */
export const SOURCE_FIELDS = Object.freeze([
  "OBJECTID",
  "RICO_PARCE",
  "PIN",
  "GIS_acres_num",
  "X_longitude",
  "Y_latitude",
  "TWP_RAN_SE",
  "parcel_number",
  "alternate_parcel_number",
  "site_address",
  "site_csz",
  "Site_City",
  "Site_State",
  "Site_Zip",
  "gross_acres",
  "class",
  "EMV",
  "EAV",
  "farm_land",
  "farm_building",
  "non_farm_land",
  "non_farm_building",
  "date_last_sale",
  "gross_sale_price",
  "net_sale_price",
  "legal",
  "date_of_sale",
  "county",
  "municipality",
  "Jurisdiction",
  "township",
  "tax_code",
  "taxbill_year",
  "Zoning",
  "assessed_last",
  "MODLNAME",
  "YRBuilt",
  "GarSQFT",
  "TOTSQFT",
  "Shape__Area",
  "Shape__Length",
]);

/**
 * Stable CSV column order consumed by the seed pre-processor and the Rock Island
 * transform. Source column names are retained after normalized pipeline aliases
 * so source facts and identifier variants remain auditable.
 *
 * @type {readonly string[]}
 */
export const SEED_COLUMNS = Object.freeze([
  "parcel_id",
  "source_identifier",
  "method",
  "url",
  "multiValueQueryString",
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
  "source_item_id",
  "source_revision",
  "source_snapshot_at",
  "source_record_count",
  "source_object_ids",
  "source_features_json",
  ...SOURCE_FIELDS.map((field) => `source_${field}`),
]);

/**
 * @typedef {object} CliOptions
 * @property {string} outputPath - Local CSV destination.
 * @property {number} pageSize - ArcGIS records requested per page.
 * @property {number} concurrency - Maximum concurrent ArcGIS page requests.
 */

/**
 * @typedef {object} GeoJsonFeature
 * @property {string | number | undefined} [id] - Optional GeoJSON feature id.
 * @property {Record<string, unknown>} properties - ArcGIS feature attributes.
 * @property {Record<string, unknown> | null} geometry - WGS84 parcel geometry.
 */

/**
 * @typedef {object} GeoJsonFeatureCollection
 * @property {readonly GeoJsonFeature[]} features - Features returned for one page.
 * @property {Record<string, unknown> | undefined} [error] - ArcGIS error payload when present.
 */

/**
 * @typedef {object} SeedBuildStats
 * @property {number} sourceRecordCount - Live ArcGIS polygon-record denominator.
 * @property {number} expectedSeedRowCount - Distinct valid PIN denominator.
 * @property {number} rowsWritten - CSV data rows written.
 * @property {number} uniqueParcelIds - Unique PIN values written.
 * @property {number} unkeyedSourceRecords - Source records quarantined because PIN is not a 10-digit parcel id.
 * @property {number} duplicatePinGroups - Valid PIN values represented by multiple source records.
 * @property {number} duplicateExtraRecords - Source records consolidated beyond one row per valid PIN.
 * @property {number} consolidatedRows - Seed rows built from more than one source record.
 * @property {number} blankAddresses - Rows without a site address.
 * @property {number} incompleteAssessments - Rows without EAV or EMV.
 * @property {number} polygonCount - Polygon geometries written.
 * @property {number} multiPolygonCount - MultiPolygon geometries written.
 * @property {number} sourceBytes - HTTP response bytes read across data pages.
 * @property {number} elapsedMs - End-to-end seed generation elapsed time.
 */

/**
 * Print command usage.
 *
 * @returns {void} Writes usage help to stdout.
 */
function showUsage() {
  console.log(`
Usage:
  node scripts/build-rock-island-seed.mjs [options]

Options:
  --output <path>       Local CSV destination. Default: ${DEFAULT_OUTPUT_PATH}
  --page-size <number>  ArcGIS records per page. Default: ${DEFAULT_PAGE_SIZE}
  --concurrency <n>     Concurrent page requests. Default: ${DEFAULT_CONCURRENCY}
  --help                Show this message.

The generated seed uses an explicit non-PII field allow-list. Owner and tax-bill
names and mailing addresses are never requested from the source.
`);
}

/**
 * Parse a positive integer command-line value.
 *
 * @param {string} optionName - Flag name used in validation errors.
 * @param {string} rawValue - Raw flag value.
 * @returns {number} Positive integer value.
 */
function parsePositiveInteger(optionName, rawValue) {
  const parsed = Number.parseInt(rawValue, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${optionName} must be a positive integer`);
  }
  return parsed;
}

/**
 * Parse command-line flags into typed options.
 *
 * @param {readonly string[]} argv - Arguments after the script filename.
 * @returns {CliOptions} Validated seed-build options.
 */
export function parseCliOptions(argv) {
  /** @type {CliOptions} */
  const options = {
    outputPath: DEFAULT_OUTPUT_PATH,
    pageSize: DEFAULT_PAGE_SIZE,
    concurrency: DEFAULT_CONCURRENCY,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help" || token === "-h") {
      showUsage();
      process.exit(0);
    }
    const value = argv[index + 1];
    if (typeof value !== "string" || value.startsWith("--")) {
      throw new Error(`Missing value for ${token}`);
    }
    if (token === "--output") {
      options.outputPath = value;
    } else if (token === "--page-size") {
      options.pageSize = parsePositiveInteger(token, value);
    } else if (token === "--concurrency") {
      options.concurrency = parsePositiveInteger(token, value);
    } else {
      throw new Error(`Unknown option: ${token}`);
    }
    index += 1;
  }
  if (options.pageSize > 2_000) {
    throw new Error("--page-size cannot exceed the source maximum of 2000");
  }
  if (options.concurrency > 4) {
    throw new Error(
      "--concurrency cannot exceed the discovery-tested maximum of 4",
    );
  }
  return options;
}

/**
 * Assert that the source allow-list cannot request known PII fields.
 *
 * @param {readonly string[]} sourceFields - ArcGIS field allow-list to validate.
 * @returns {void} Throws when a prohibited field is present or fields repeat.
 */
export function assertSafeSourceFields(sourceFields) {
  const normalizedExcluded = new Set(
    EXCLUDED_PII_FIELDS.map((field) => field.toLowerCase()),
  );
  const seen = new Set();
  for (const field of sourceFields) {
    const normalized = field.toLowerCase();
    if (normalizedExcluded.has(normalized)) {
      throw new Error(
        `PII field is prohibited in the seed source request: ${field}`,
      );
    }
    if (seen.has(normalized)) {
      throw new Error(`Duplicate source field: ${field}`);
    }
    seen.add(normalized);
  }
}

/**
 * Convert an unknown source value to a trimmed string.
 *
 * @param {unknown} value - ArcGIS property value.
 * @returns {string} Trimmed text, or an empty string for nullish values.
 */
function toText(value) {
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

/**
 * Convert an ArcGIS value to a CSV-safe scalar string without losing zeroes.
 *
 * @param {unknown} value - ArcGIS property value.
 * @returns {string} Source value represented as text.
 */
function sourceValueToText(value) {
  if (value === null || value === undefined) return "";
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return String(value);
  }
  return JSON.stringify(value);
}

/**
 * Build a full site address while keeping blank site addresses valid.
 *
 * @param {Record<string, unknown>} properties - ArcGIS feature attributes.
 * @returns {string} Comma-separated site address.
 */
function buildSiteAddress(properties) {
  const street = toText(properties.site_address);
  const city = toText(properties.Site_City);
  const state = toText(properties.Site_State) || "IL";
  const zip = toText(properties.Site_Zip);
  const locality = [city, state, zip]
    .filter((part) => part.length > 0)
    .join(" ");
  return [street, locality].filter((part) => part.length > 0).join(", ");
}

/**
 * Convert a GeoJSON feature into the canonical Rock Island seed row.
 *
 * @param {GeoJsonFeature} feature - FeatureServer record with WGS84 geometry.
 * @param {string} sourceRevision - ISO source revision timestamp from layer metadata.
 * @param {string} snapshotAt - ISO timestamp shared by the complete seed snapshot.
 * @returns {Record<string, string>} CSV row keyed by `SEED_COLUMNS`.
 */
export function toSeedRow(feature, sourceRevision, snapshotAt) {
  const properties = feature.properties;
  const pin = toText(properties.PIN);
  const row = {
    parcel_id: pin,
    source_identifier: pin,
    method: "GET",
    url: FEATURE_QUERY_URL,
    multiValueQueryString: JSON.stringify({
      f: ["geojson"],
      outFields: [SOURCE_FIELDS.join(",")],
      outSR: ["4326"],
      returnGeometry: ["true"],
      where: [`PIN='${pin}'`],
    }),
    address: buildSiteAddress(properties),
    city: toText(properties.Site_City),
    state: toText(properties.Site_State) || "IL",
    zip: toText(properties.Site_Zip),
    county: COUNTY_NAME,
    county_fips: COUNTY_FIPS,
    latitude: toText(properties.Y_latitude),
    longitude: toText(properties.X_longitude),
    parcel_polygon:
      feature.geometry === null ? "" : JSON.stringify(feature.geometry),
    source_url: FEATURE_LAYER_URL,
    source_item_id: SOURCE_ITEM_ID,
    source_revision: sourceRevision,
    source_snapshot_at: snapshotAt,
    source_record_count: "1",
    source_object_ids: toText(properties.OBJECTID),
    source_features_json: "",
  };
  for (const field of SOURCE_FIELDS) {
    row[`source_${field}`] = sourceValueToText(properties[field]);
  }
  return row;
}

/**
 * Return whether a source PIN is a canonical ten-digit Rock Island parcel id.
 * Placeholder values such as `USA`, `CITY`, and `RAILROAD` are source polygons,
 * not stable parcel identifiers, and must not enter the keyed seed.
 *
 * @param {unknown} value - Source PIN value.
 * @returns {boolean} True only for a ten-digit numeric PIN.
 */
export function isValidParcelPin(value) {
  return /^[0-9]{10}$/.test(toText(value));
}

/**
 * Merge all source geometries for one valid PIN without discarding conflicting
 * source records. The lowest OBJECTID record supplies scalar compatibility
 * columns, while `source_features_json` retains every non-PII source feature and
 * `parcel_polygon` contains the union-by-components geometry.
 *
 * This does not perform a topological dissolve: Polygon components are preserved
 * exactly and represented as one Polygon or MultiPolygon.
 *
 * @param {readonly GeoJsonFeature[]} features - Two or more records sharing one PIN.
 * @param {string} sourceRevision - ISO layer data revision.
 * @param {string} snapshotAt - ISO seed snapshot timestamp.
 * @returns {Record<string, string>} Consolidated seed row.
 */
export function mergeFeatureGroup(features, sourceRevision, snapshotAt) {
  if (features.length < 2) {
    throw new Error(
      "A duplicate feature group must contain at least two records",
    );
  }
  const ordered = [...features].sort(
    (left, right) =>
      Number(left.properties.OBJECTID) - Number(right.properties.OBJECTID),
  );
  const pin = toText(ordered[0].properties.PIN);
  if (!isValidParcelPin(pin)) {
    throw new Error(`Cannot merge records without a canonical PIN: ${pin}`);
  }
  if (ordered.some((feature) => toText(feature.properties.PIN) !== pin)) {
    throw new Error("Cannot merge source records with different PIN values");
  }
  /** @type {unknown[]} */
  const polygonComponents = [];
  const seenGeometry = new Set();
  for (const feature of ordered) {
    if (feature.geometry === null) {
      throw new Error(`Cannot merge null geometry for PIN ${pin}`);
    }
    const serialized = JSON.stringify(feature.geometry);
    if (seenGeometry.has(serialized)) continue;
    seenGeometry.add(serialized);
    const geometryType = feature.geometry.type;
    const coordinates = feature.geometry.coordinates;
    if (geometryType === "Polygon" && Array.isArray(coordinates)) {
      polygonComponents.push(coordinates);
    } else if (geometryType === "MultiPolygon" && Array.isArray(coordinates)) {
      polygonComponents.push(...coordinates);
    } else {
      throw new Error(
        `Unsupported duplicate geometry for PIN ${pin}: ${String(geometryType)}`,
      );
    }
  }
  const mergedGeometry =
    polygonComponents.length === 1
      ? { type: "Polygon", coordinates: polygonComponents[0] }
      : { type: "MultiPolygon", coordinates: polygonComponents };
  const primary = {
    ...ordered[0],
    geometry: mergedGeometry,
  };
  const row = toSeedRow(primary, sourceRevision, snapshotAt);
  row.source_record_count = String(ordered.length);
  row.source_object_ids = ordered
    .map((feature) => toText(feature.properties.OBJECTID))
    .join("|");
  row.source_features_json = JSON.stringify(ordered);
  return row;
}

/**
 * Escape one value according to RFC 4180 CSV quoting rules.
 *
 * @param {string} value - Plain cell text.
 * @returns {string} CSV-encoded cell.
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
 * Read a JSON endpoint and retain response byte size for throughput evidence.
 *
 * @template T
 * @param {URL | string} url - JSON endpoint.
 * @returns {Promise<{ value: T, bytes: number }>} Parsed body and transfer size.
 */
async function fetchJson(url) {
  const response = await fetch(url, {
    headers: {
      Accept: "application/json",
      "User-Agent": "elephant-oracle-node/rock-island-seed",
    },
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Source request failed with HTTP ${response.status}`);
  }
  /** @type {T} */
  const value = JSON.parse(text);
  return { value, bytes: Buffer.byteLength(text) };
}

/**
 * Build a FeatureServer query URL for a single ordered page.
 *
 * @param {number} offset - Zero-based record offset.
 * @param {number} pageSize - Maximum records requested.
 * @returns {URL} ArcGIS GeoJSON query URL.
 */
export function buildPageUrl(offset, pageSize) {
  const url = new URL(FEATURE_QUERY_URL);
  url.searchParams.set("f", "geojson");
  url.searchParams.set("where", "1=1");
  url.searchParams.set("outFields", SOURCE_FIELDS.join(","));
  url.searchParams.set("returnGeometry", "true");
  url.searchParams.set("outSR", "4326");
  url.searchParams.set("orderByFields", "OBJECTID");
  url.searchParams.set("resultOffset", String(offset));
  url.searchParams.set("resultRecordCount", String(pageSize));
  return url;
}

/**
 * Build a narrow ordered request used to audit PIN validity and uniqueness before
 * downloading geometry. This preflight prevents a partial seed from silently
 * collapsing duplicate or placeholder source identifiers.
 *
 * @param {number} offset - Zero-based record offset.
 * @param {number} pageSize - Maximum records requested.
 * @returns {URL} ArcGIS JSON query URL for OBJECTID and PIN only.
 */
export function buildIdAuditUrl(offset, pageSize) {
  const url = new URL(FEATURE_QUERY_URL);
  url.searchParams.set("f", "json");
  url.searchParams.set("where", "1=1");
  url.searchParams.set("outFields", "OBJECTID,PIN");
  url.searchParams.set("returnGeometry", "false");
  url.searchParams.set("orderByFields", "OBJECTID");
  url.searchParams.set("resultOffset", String(offset));
  url.searchParams.set("resultRecordCount", String(pageSize));
  return url;
}

/**
 * Read the live record count and layer data revision.
 *
 * @returns {Promise<{ count: number, sourceRevision: string }>} Source denominator and revision.
 */
async function readSourceMetadata() {
  const countUrl = new URL(FEATURE_QUERY_URL);
  countUrl.searchParams.set("f", "json");
  countUrl.searchParams.set("where", "1=1");
  countUrl.searchParams.set("returnCountOnly", "true");
  const [{ value: countBody }, { value: layerBody }] = await Promise.all([
    fetchJson(countUrl),
    fetchJson(`${FEATURE_LAYER_URL}?f=pjson`),
  ]);
  const count =
    typeof countBody === "object" &&
    countBody !== null &&
    "count" in countBody &&
    typeof countBody.count === "number"
      ? countBody.count
      : null;
  const lastEditDate =
    typeof layerBody === "object" &&
    layerBody !== null &&
    "editingInfo" in layerBody &&
    typeof layerBody.editingInfo === "object" &&
    layerBody.editingInfo !== null &&
    "dataLastEditDate" in layerBody.editingInfo &&
    typeof layerBody.editingInfo.dataLastEditDate === "number"
      ? layerBody.editingInfo.dataLastEditDate
      : null;
  if (count === null || !Number.isSafeInteger(count) || count <= 0) {
    throw new Error("FeatureServer did not return a positive record count");
  }
  if (lastEditDate === null) {
    throw new Error("FeatureServer metadata did not return dataLastEditDate");
  }
  return {
    count,
    sourceRevision: new Date(lastEditDate).toISOString(),
  };
}

/**
 * Fetch ordered FeatureServer pages with bounded concurrency.
 *
 * @param {readonly number[]} offsets - Page offsets to fetch.
 * @param {number} pageSize - Requested records per page.
 * @param {number} concurrency - Maximum simultaneous requests.
 * @returns {Promise<readonly { offset: number, features: readonly GeoJsonFeature[], bytes: number }[]>} Pages in offset order.
 */
async function fetchPageBatch(offsets, pageSize, concurrency) {
  const results = new Array(offsets.length);
  let nextIndex = 0;
  const workers = Array.from(
    { length: Math.min(concurrency, offsets.length) },
    async () => {
      while (nextIndex < offsets.length) {
        const index = nextIndex;
        nextIndex += 1;
        const offset = offsets[index];
        const { value, bytes } = await fetchJson(
          buildPageUrl(offset, pageSize),
        );
        const body = /** @type {GeoJsonFeatureCollection} */ (value);
        if (body.error !== undefined || !Array.isArray(body.features)) {
          throw new Error(
            `ArcGIS page at offset ${offset} was not a feature collection`,
          );
        }
        results[index] = { offset, features: body.features, bytes };
      }
    },
  );
  await Promise.all(workers);
  return results;
}

/**
 * Audit every source record's PIN before geometry download.
 *
 * @param {number} sourceRecordCount - Live source polygon-record count.
 * @param {number} pageSize - Requested records per page.
 * @param {number} concurrency - Maximum simultaneous requests.
 * @returns {Promise<{
 *   duplicatePins: ReadonlySet<string>,
 *   validUniquePinCount: number,
 *   invalidRecordCount: number,
 *   duplicateExtraRecordCount: number,
 *   responseBytes: number
 * }>} PIN denominators and duplicate set.
 */
async function auditParcelIds(sourceRecordCount, pageSize, concurrency) {
  const offsets = Array.from(
    { length: Math.ceil(sourceRecordCount / pageSize) },
    (_, index) => index * pageSize,
  );
  const pinCounts = new Map();
  let invalidRecordCount = 0;
  let responseBytes = 0;
  for (
    let batchStart = 0;
    batchStart < offsets.length;
    batchStart += concurrency
  ) {
    const batchOffsets = offsets.slice(batchStart, batchStart + concurrency);
    let nextIndex = 0;
    const batchResults = new Array(batchOffsets.length);
    await Promise.all(
      Array.from(
        { length: Math.min(concurrency, batchOffsets.length) },
        async () => {
          while (nextIndex < batchOffsets.length) {
            const index = nextIndex;
            nextIndex += 1;
            const offset = batchOffsets[index];
            const { value, bytes } = await fetchJson(
              buildIdAuditUrl(offset, pageSize),
            );
            batchResults[index] = { value, bytes };
          }
        },
      ),
    );
    for (const result of batchResults) {
      responseBytes += result.bytes;
      const body = result.value;
      if (
        typeof body !== "object" ||
        body === null ||
        !("features" in body) ||
        !Array.isArray(body.features)
      ) {
        throw new Error(
          "PIN audit response was not an ArcGIS feature collection",
        );
      }
      for (const feature of body.features) {
        const attributes =
          typeof feature === "object" &&
          feature !== null &&
          "attributes" in feature &&
          typeof feature.attributes === "object" &&
          feature.attributes !== null
            ? feature.attributes
            : {};
        const pin = toText(attributes.PIN);
        if (!isValidParcelPin(pin)) {
          invalidRecordCount += 1;
          continue;
        }
        pinCounts.set(pin, (pinCounts.get(pin) ?? 0) + 1);
      }
    }
  }
  const duplicatePins = new Set(
    [...pinCounts].filter(([, count]) => count > 1).map(([pin]) => pin),
  );
  const duplicateExtraRecordCount = [...pinCounts.values()].reduce(
    (total, count) => total + Math.max(0, count - 1),
    0,
  );
  return {
    duplicatePins,
    validUniquePinCount: pinCounts.size,
    invalidRecordCount,
    duplicateExtraRecordCount,
    responseBytes,
  };
}

/**
 * Write text while honoring stream backpressure.
 *
 * @param {import("fs").WriteStream} stream - Destination file stream.
 * @param {string} text - Text chunk to append.
 * @returns {Promise<void>} Resolves when the chunk is accepted.
 */
async function writeChunk(stream, text) {
  if (stream.write(text)) return;
  await once(stream, "drain");
}

/**
 * Validate and account for a normalized seed row before writing it.
 *
 * @param {Record<string, string>} row - Normalized or consolidated seed row.
 * @param {Set<string>} parcelIds - Mutable unique PIN set.
 * @param {SeedBuildStats} stats - Mutable build counters.
 * @returns {void} Throws on missing/duplicate PIN or invalid geometry.
 */
function validateSeedRow(row, parcelIds, stats) {
  const pin = row.parcel_id;
  if (!isValidParcelPin(pin)) {
    throw new Error(`Invalid Rock Island PIN: ${pin || "<blank>"}`);
  }
  if (parcelIds.has(pin)) {
    throw new Error(`Duplicate Rock Island PIN: ${pin}`);
  }
  parcelIds.add(pin);
  const geometry =
    row.parcel_polygon.length > 0 ? JSON.parse(row.parcel_polygon) : null;
  const geometryType =
    typeof geometry === "object" &&
    geometry !== null &&
    "type" in geometry &&
    typeof geometry.type === "string"
      ? geometry.type
      : "";
  if (geometryType === "Polygon") {
    stats.polygonCount += 1;
  } else if (geometryType === "MultiPolygon") {
    stats.multiPolygonCount += 1;
  } else {
    throw new Error(
      `Unsupported geometry for PIN ${pin}: ${geometryType || "null"}`,
    );
  }
  if (row.source_site_address.length === 0) {
    stats.blankAddresses += 1;
  }
  if (row.source_EAV.length === 0 || row.source_EMV.length === 0) {
    stats.incompleteAssessments += 1;
  }
}

/**
 * Generate and validate the complete non-PII Rock Island seed CSV.
 *
 * @param {CliOptions} options - Validated CLI options.
 * @returns {Promise<SeedBuildStats>} Final denominator and payload statistics.
 */
export async function buildSeed(options) {
  assertSafeSourceFields(SOURCE_FIELDS);
  const startedAt = Date.now();
  const snapshotAt = new Date().toISOString();
  const { count, sourceRevision } = await readSourceMetadata();
  const pinAudit = await auditParcelIds(
    count,
    options.pageSize,
    options.concurrency,
  );
  const outputPath = path.resolve(options.outputPath);
  const unkeyedOutputPath = outputPath.toLowerCase().endsWith(".csv")
    ? `${outputPath.slice(0, -4)}.unkeyed-features.jsonl`
    : `${outputPath}.unkeyed-features.jsonl`;
  await mkdir(path.dirname(outputPath), { recursive: true, mode: 0o700 });
  const stream = createWriteStream(outputPath, {
    encoding: "utf8",
    mode: 0o600,
  });
  const unkeyedStream = createWriteStream(unkeyedOutputPath, {
    encoding: "utf8",
    mode: 0o600,
  });
  /** @type {SeedBuildStats} */
  const stats = {
    sourceRecordCount: count,
    expectedSeedRowCount: pinAudit.validUniquePinCount,
    rowsWritten: 0,
    uniqueParcelIds: 0,
    unkeyedSourceRecords: 0,
    duplicatePinGroups: pinAudit.duplicatePins.size,
    duplicateExtraRecords: pinAudit.duplicateExtraRecordCount,
    consolidatedRows: 0,
    blankAddresses: 0,
    incompleteAssessments: 0,
    polygonCount: 0,
    multiPolygonCount: 0,
    sourceBytes: pinAudit.responseBytes,
    elapsedMs: 0,
  };
  const parcelIds = new Set();
  /** @type {Map<string, GeoJsonFeature[]>} */
  const duplicateFeatures = new Map();
  try {
    await writeChunk(
      stream,
      `${SEED_COLUMNS.map((column) => encodeCsvCell(column)).join(",")}\n`,
    );
    const offsets = Array.from(
      { length: Math.ceil(count / options.pageSize) },
      (_, index) => index * options.pageSize,
    );
    for (
      let batchStart = 0;
      batchStart < offsets.length;
      batchStart += options.concurrency
    ) {
      const batchOffsets = offsets.slice(
        batchStart,
        batchStart + options.concurrency,
      );
      const pages = await fetchPageBatch(
        batchOffsets,
        options.pageSize,
        options.concurrency,
      );
      for (const page of pages) {
        stats.sourceBytes += page.bytes;
        for (const feature of page.features) {
          const pin = toText(feature.properties.PIN);
          if (!isValidParcelPin(pin)) {
            await writeChunk(
              unkeyedStream,
              `${JSON.stringify({
                reason: "source_pin_is_not_a_ten_digit_parcel_identifier",
                sourceRevision,
                snapshotAt,
                feature,
              })}\n`,
            );
            stats.unkeyedSourceRecords += 1;
            continue;
          }
          if (pinAudit.duplicatePins.has(pin)) {
            const group = duplicateFeatures.get(pin) ?? [];
            group.push(feature);
            duplicateFeatures.set(pin, group);
            continue;
          }
          const row = toSeedRow(feature, sourceRevision, snapshotAt);
          validateSeedRow(row, parcelIds, stats);
          await writeChunk(stream, renderCsvRow(row));
          stats.rowsWritten += 1;
        }
      }
    }
    for (const pin of [...duplicateFeatures.keys()].sort()) {
      const features = duplicateFeatures.get(pin) ?? [];
      const row = mergeFeatureGroup(features, sourceRevision, snapshotAt);
      validateSeedRow(row, parcelIds, stats);
      await writeChunk(stream, renderCsvRow(row));
      stats.rowsWritten += 1;
      stats.consolidatedRows += 1;
    }
  } finally {
    stream.end();
    unkeyedStream.end();
    await Promise.all([once(stream, "close"), once(unkeyedStream, "close")]);
  }
  stats.uniqueParcelIds = parcelIds.size;
  stats.elapsedMs = Date.now() - startedAt;
  if (stats.rowsWritten !== stats.expectedSeedRowCount) {
    throw new Error(
      `Seed count mismatch: expected ${stats.expectedSeedRowCount}, wrote ${stats.rowsWritten}`,
    );
  }
  if (stats.uniqueParcelIds !== stats.expectedSeedRowCount) {
    throw new Error(
      `Seed uniqueness mismatch: expected ${stats.expectedSeedRowCount}, got ${stats.uniqueParcelIds}`,
    );
  }
  if (stats.unkeyedSourceRecords !== pinAudit.invalidRecordCount) {
    throw new Error(
      `Unkeyed source count mismatch: expected ${pinAudit.invalidRecordCount}, got ${stats.unkeyedSourceRecords}`,
    );
  }
  if (stats.consolidatedRows !== stats.duplicatePinGroups) {
    throw new Error(
      `Duplicate group mismatch: expected ${stats.duplicatePinGroups}, got ${stats.consolidatedRows}`,
    );
  }
  return stats;
}

/**
 * Command-line entry point.
 *
 * @returns {Promise<void>} Resolves after the full seed is written and summarized.
 */
async function main() {
  const options = parseCliOptions(process.argv.slice(2));
  const stats = await buildSeed(options);
  console.log(
    JSON.stringify(
      {
        outputPath: path.resolve(options.outputPath),
        unkeyedOutputPath: path
          .resolve(options.outputPath)
          .replace(/\.csv$/i, "")
          .concat(".unkeyed-features.jsonl"),
        pageSize: options.pageSize,
        concurrency: options.concurrency,
        ...stats,
      },
      null,
      2,
    ),
  );
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((caught) => {
    console.error(caught instanceof Error ? caught.message : String(caught));
    process.exitCode = 1;
  });
}
