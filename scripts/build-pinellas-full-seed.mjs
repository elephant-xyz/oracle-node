#!/usr/bin/env node

import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import {
  encodeCsvCell,
  isValidStrap,
  toText,
} from "./build-pinellas-pilot-seed.mjs";

const TAX_PARCELS_URL =
  "https://egis.pinellas.gov/pcpagis/rest/services/PcpaBaseMap/BaseMapParcelAerials/MapServer/157";
const TAX_PARCELS_QUERY_URL = `${TAX_PARCELS_URL}/query`;
const PRINT_URL = "https://www.pcpao.gov/property/detail/print";
const COUNTY_NAME = "Pinellas";
const COUNTY_FIPS = "12103";
const DEFAULT_OUTPUT_PATH = "data/seeds/pinellas.csv";
const PAGE_SIZE = 15000;
const EXPECTED_GIS_COUNT_MIN = 300000;
const USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

/**
 * Non-PII tax-parcel attributes used for the full-county seed.
 *
 * @type {readonly string[]}
 */
export const FULL_SEED_SOURCE_FIELDS = Object.freeze([
  "OBJECTID",
  "STRAP",
  "PARCELID",
  "PARCELID_DSP1",
]);

/**
 * Lean CSV columns for the AWS seed feeder. Geometry is omitted so ~311k rows
 * stay a few tens of megabytes.
 *
 * @type {readonly string[]}
 */
export const FULL_SEED_COLUMNS = Object.freeze([
  "parcel_id",
  "source_identifier",
  "situs_address",
  "method",
  "url",
  "multiValueQueryString",
  "county",
  "county_fips",
  "parcelid",
  "parcelid_display",
  "source_url",
  "source_snapshot_at",
]);

/**
 * @typedef {object} TaxParcelAttributes
 * @property {unknown} [OBJECTID]
 * @property {unknown} [STRAP]
 * @property {unknown} [PARCELID]
 * @property {unknown} [PARCELID_DSP1]
 */

/**
 * @typedef {object} TaxParcelFeature
 * @property {TaxParcelAttributes} attributes
 */

/**
 * @typedef {object} FullSeedBuildSummary
 * @property {number} gisCount
 * @property {number} fetchedFeatureCount
 * @property {number} uniqueStrapCount
 * @property {number} skippedInvalidStrapCount
 * @property {string} outputPath
 * @property {string} sourceSnapshotAt
 */

/**
 * Build a GIS query URL for one page of tax parcels (no geometry).
 *
 * @param {number} resultOffset - Feature offset.
 * @param {number} resultRecordCount - Page size.
 * @returns {string} Absolute query URL.
 */
export function buildTaxParcelPageUrl(resultOffset, resultRecordCount) {
  const params = new URLSearchParams({
    where: "1=1",
    outFields: FULL_SEED_SOURCE_FIELDS.join(","),
    returnGeometry: "false",
    resultOffset: String(resultOffset),
    resultRecordCount: String(resultRecordCount),
    f: "json",
  });
  return `${TAX_PARCELS_QUERY_URL}?${params.toString()}`;
}

/**
 * Parse a tax-parcel query payload and fail loud on GIS errors.
 *
 * @param {unknown} payload - Parsed JSON.
 * @returns {{ features: TaxParcelFeature[], exceededTransferLimit: boolean }} Features plus paging flag.
 */
export function parseTaxParcelPage(payload) {
  if (
    typeof payload !== "object" ||
    payload === null ||
    Array.isArray(payload)
  ) {
    throw new Error("Tax parcel GIS response is not a JSON object");
  }
  const record = /** @type {Record<string, unknown>} */ (payload);
  if (record.error !== undefined) {
    throw new Error(
      `Tax parcel GIS query failed: ${JSON.stringify(record.error)}`,
    );
  }
  const features = record.features;
  if (!Array.isArray(features)) {
    throw new Error("Tax parcel GIS response is missing features[]");
  }
  return {
    features: /** @type {TaxParcelFeature[]} */ (features),
    exceededTransferLimit: record.exceededTransferLimit === true,
  };
}

/**
 * Convert one tax-parcel feature into a lean seed row. Invalid STRAPs return null
 * so the caller can count and skip them without aborting the county.
 *
 * @param {TaxParcelFeature} feature - GIS feature.
 * @param {string} snapshotAt - ISO timestamp for this snapshot.
 * @returns {Record<string, string> | null} Seed row, or null when STRAP is not 18 digits.
 */
export function toFullSeedRow(feature, snapshotAt) {
  const attributes = feature.attributes ?? {};
  const strap = toText(attributes.STRAP);
  if (!isValidStrap(strap)) return null;
  return {
    parcel_id: strap,
    source_identifier: strap,
    situs_address: "",
    method: "GET",
    url: PRINT_URL,
    multiValueQueryString: JSON.stringify({
      is_print: ["1"],
      s: [strap],
    }),
    county: COUNTY_NAME,
    county_fips: COUNTY_FIPS,
    parcelid: toText(attributes.PARCELID),
    parcelid_display: toText(attributes.PARCELID_DSP1),
    source_url: TAX_PARCELS_URL,
    source_snapshot_at: snapshotAt,
  };
}

/**
 * Render the full-county seed CSV.
 *
 * @param {readonly Record<string, string>[]} rows - Deduped seed rows.
 * @returns {string} Complete CSV including header.
 */
export function renderFullSeedCsv(rows) {
  const header = FULL_SEED_COLUMNS.join(",");
  const body = rows.map((row) =>
    FULL_SEED_COLUMNS.map((column) => encodeCsvCell(row[column] ?? "")).join(
      ",",
    ),
  );
  return `${[header, ...body].join("\n")}\n`;
}

/**
 * Fetch JSON from a GIS or appraiser URL with the Chrome UA PCPAO/GIS require.
 *
 * @param {string} url - Absolute URL.
 * @returns {Promise<unknown>} Parsed JSON.
 */
async function fetchJson(url) {
  const response = await fetch(url, {
    headers: { "User-Agent": USER_AGENT, Accept: "application/json" },
  });
  if (!response.ok) {
    throw new Error(`HTTP ${String(response.status)} for ${url}`);
  }
  return /** @type {unknown} */ (await response.json());
}

/**
 * Return the GIS-reported tax-parcel count.
 *
 * @returns {Promise<number>} Feature count.
 */
export async function fetchTaxParcelCount() {
  const params = new URLSearchParams({
    where: "1=1",
    returnCountOnly: "true",
    f: "json",
  });
  const payload = await fetchJson(
    `${TAX_PARCELS_QUERY_URL}?${params.toString()}`,
  );
  if (
    typeof payload !== "object" ||
    payload === null ||
    Array.isArray(payload)
  ) {
    throw new Error("Tax parcel count response is not a JSON object");
  }
  const count = /** @type {Record<string, unknown>} */ (payload).count;
  if (typeof count !== "number" || !Number.isFinite(count)) {
    throw new Error("Tax parcel count response is missing a numeric count");
  }
  return count;
}

/**
 * Page the tax-parcel layer and return unique STRAP seed rows.
 *
 * @param {{ pageSize?: number, snapshotAt?: string }} [options] - Paging options.
 * @returns {Promise<{ rows: Record<string, string>[], fetchedFeatureCount: number, skippedInvalidStrapCount: number, sourceSnapshotAt: string }>} Deduped rows plus counters.
 */
export async function fetchAllTaxParcelSeedRows(options = {}) {
  const pageSize = options.pageSize ?? PAGE_SIZE;
  const snapshotAt = options.snapshotAt ?? new Date().toISOString();
  /** @type {Map<string, Record<string, string>>} */
  const byStrap = new Map();
  let fetchedFeatureCount = 0;
  let skippedInvalidStrapCount = 0;
  let offset = 0;
  let more = true;
  while (more) {
    const payload = await fetchJson(buildTaxParcelPageUrl(offset, pageSize));
    const page = parseTaxParcelPage(payload);
    fetchedFeatureCount += page.features.length;
    for (const feature of page.features) {
      const row = toFullSeedRow(feature, snapshotAt);
      if (row === null) {
        skippedInvalidStrapCount += 1;
        continue;
      }
      if (!byStrap.has(row.parcel_id)) {
        byStrap.set(row.parcel_id, row);
      }
    }
    more = page.features.length === pageSize || page.exceededTransferLimit;
    if (page.features.length === 0) more = false;
    offset += page.features.length;
    process.stderr.write(
      JSON.stringify({
        event: "pinellas_full_seed_page",
        offset,
        pageSize: page.features.length,
        uniqueStraps: byStrap.size,
        skippedInvalidStrapCount,
      }) + "\n",
    );
  }
  return {
    rows: [...byStrap.values()],
    fetchedFeatureCount,
    skippedInvalidStrapCount,
    sourceSnapshotAt: snapshotAt,
  };
}

/**
 * Fail if a sampled STRAP does not return populated PCPAO print HTML.
 *
 * @param {string} strap - 18-digit STRAP.
 * @returns {Promise<void>} Resolves when the print page looks populated.
 */
export async function assertPrintLookup(strap) {
  if (!isValidStrap(strap)) {
    throw new Error(`Cannot probe a non-STRAP id: ${strap}`);
  }
  const url = `${PRINT_URL}?is_print=1&s=${encodeURIComponent(strap)}`;
  const response = await fetch(url, {
    headers: { "User-Agent": USER_AGENT, Accept: "text/html" },
  });
  if (!response.ok) {
    throw new Error(
      `Print lookup HTTP ${String(response.status)} for ${strap}`,
    );
  }
  const html = await response.text();
  if (html.length < 1000 || !/parcel/i.test(html)) {
    throw new Error(
      `Print lookup for STRAP ${strap} returned empty/placeholder HTML (${String(html.length)} bytes)`,
    );
  }
}

/**
 * Parse CLI flags.
 *
 * @param {string[]} argv - Args after node/script.
 * @returns {{ outputPath: string, skipPrintProbe: boolean }} Options.
 */
export function parseFullSeedCli(argv) {
  /** @type {{ outputPath: string, skipPrintProbe: boolean }} */
  const options = {
    outputPath: DEFAULT_OUTPUT_PATH,
    skipPrintProbe: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--skip-print-probe") {
      options.skipPrintProbe = true;
      continue;
    }
    if (token === "--output") {
      const value = argv[index + 1];
      if (value === undefined || value.startsWith("--")) {
        throw new Error("--output requires a path");
      }
      options.outputPath = value;
      index += 1;
      continue;
    }
    throw new Error(`Unexpected argument: ${token}`);
  }
  return options;
}

/**
 * Build and write the full Pinellas seed CSV.
 *
 * @param {{ outputPath?: string, skipPrintProbe?: boolean }} [options] - Output options.
 * @returns {Promise<FullSeedBuildSummary>} Build summary.
 */
export async function buildPinellasFullSeed(options = {}) {
  const outputPath = options.outputPath ?? DEFAULT_OUTPUT_PATH;
  const gisCount = await fetchTaxParcelCount();
  if (gisCount < EXPECTED_GIS_COUNT_MIN) {
    throw new Error(
      `Tax parcel GIS count ${String(gisCount)} is below expected minimum ${String(EXPECTED_GIS_COUNT_MIN)}`,
    );
  }
  const fetched = await fetchAllTaxParcelSeedRows();
  if (fetched.rows.length < EXPECTED_GIS_COUNT_MIN) {
    throw new Error(
      `Unique STRAP count ${String(fetched.rows.length)} is below expected minimum ${String(EXPECTED_GIS_COUNT_MIN)} (gisCount=${String(gisCount)}, fetched=${String(fetched.fetchedFeatureCount)}, invalid=${String(fetched.skippedInvalidStrapCount)})`,
    );
  }
  const coverageRatio = fetched.rows.length / gisCount;
  if (coverageRatio < 0.95) {
    throw new Error(
      `Unique STRAPs cover only ${(coverageRatio * 100).toFixed(1)}% of GIS count ${String(gisCount)}`,
    );
  }
  if (options.skipPrintProbe !== true) {
    const probeStraps = fetched.rows.slice(0, 5).map((row) => row.parcel_id);
    for (const strap of probeStraps) {
      await assertPrintLookup(strap);
    }
  }
  await mkdir(path.dirname(outputPath), { recursive: true });
  await writeFile(outputPath, renderFullSeedCsv(fetched.rows), "utf8");
  return {
    gisCount,
    fetchedFeatureCount: fetched.fetchedFeatureCount,
    uniqueStrapCount: fetched.rows.length,
    skippedInvalidStrapCount: fetched.skippedInvalidStrapCount,
    outputPath,
    sourceSnapshotAt: fetched.sourceSnapshotAt,
  };
}

const isDirectRun = process.argv[1]
  ? pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url
  : false;

if (isDirectRun) {
  const cli = parseFullSeedCli(process.argv.slice(2));
  buildPinellasFullSeed(cli)
    .then((summary) => {
      process.stdout.write(
        `${JSON.stringify({ event: "pinellas_full_seed_built", ...summary })}\n`,
      );
    })
    .catch((error) => {
      const message = error instanceof Error ? error.message : String(error);
      process.stderr.write(
        `${JSON.stringify({ event: "pinellas_full_seed_failed", error: message })}\n`,
      );
      process.exit(1);
    });
}
