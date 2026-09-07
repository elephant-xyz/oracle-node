#!/usr/bin/env node

import * as path from "node:path";

/**
 * @typedef {"roofing" | "hvac" | "solar"} PinellasBbbTradeKey
 */

/**
 * @typedef {"st-petersburg" | "clearwater"} PinellasBbbCityKey
 */

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * @typedef {object} PinellasBbbCity
 * @property {PinellasBbbCityKey} key Stable city key used in output paths.
 * @property {string} name Human-readable city label.
 * @property {string} bbbSlug BBB location slug embedded in category URLs.
 */

/**
 * @typedef {object} PinellasBbbTrade
 * @property {PinellasBbbTradeKey} key Stable trade key used in output paths.
 * @property {string} name Human-readable trade label.
 * @property {string} categorySlug BBB category path segment.
 */

/**
 * @typedef {object} PinellasBbbCategorySource
 * @property {PinellasBbbCityKey} cityKey Stable Pinellas city key.
 * @property {string} cityName Human-readable city label.
 * @property {PinellasBbbTradeKey} tradeKey Stable trade key.
 * @property {string} tradeName Human-readable trade label.
 * @property {string} categoryUrl Verified BBB category URL for the city/trade pair.
 */

/**
 * Pinellas BBB city anchors. Tampa is intentionally excluded because Pinellas
 * permit matching should stay on the county's primary municipal markets.
 *
 * @type {readonly PinellasBbbCity[]}
 */
export const PINELLAS_BBB_CITIES = Object.freeze([
  {
    key: "st-petersburg",
    name: "St. Petersburg",
    bbbSlug: "st-petersburg",
  },
  {
    key: "clearwater",
    name: "Clearwater",
    bbbSlug: "clearwater",
  },
]);

/**
 * High-value contractor trades for permit contractor reputation enrichment.
 *
 * @type {readonly PinellasBbbTrade[]}
 */
export const PINELLAS_BBB_TRADES = Object.freeze([
  {
    key: "roofing",
    name: "Roofing Contractors",
    categorySlug: "roofing-contractors",
  },
  {
    key: "hvac",
    name: "Heating and Air Conditioning",
    categorySlug: "heating-and-air-conditioning",
  },
  {
    key: "solar",
    name: "Solar Energy Contractors",
    categorySlug: "solar-energy-contractors",
  },
]);

/**
 * Build a BBB category URL for a Pinellas city and trade slug pair.
 *
 * @param {string} citySlug BBB city slug such as `st-petersburg`.
 * @param {string} categorySlug BBB category slug such as `roofing-contractors`.
 * @returns {string} Absolute BBB category URL.
 */
export function buildPinellasBbbCategoryUrl(citySlug, categorySlug) {
  return `https://www.bbb.org/us/fl/${citySlug}/category/${categorySlug}`;
}

/**
 * Verified Pinellas BBB category sources (two cities × three trades).
 *
 * @type {readonly PinellasBbbCategorySource[]}
 */
export const PINELLAS_BBB_CATEGORY_SOURCES = Object.freeze(
  PINELLAS_BBB_CITIES.flatMap((city) =>
    PINELLAS_BBB_TRADES.map((trade) => ({
      cityKey: city.key,
      cityName: city.name,
      tradeKey: trade.key,
      tradeName: trade.name,
      categoryUrl: buildPinellasBbbCategoryUrl(city.bbbSlug, trade.categorySlug),
    })),
  ),
);

/**
 * @typedef {object} PinellasBbbHarvestPlanOptions
 * @property {string} [chromiumExecutablePath="/usr/local/bin/google-chrome"] Chromium executable for Puppeteer.
 * @property {boolean} [headless=true] Whether Puppeteer should run headless.
 * @property {number | null} [maxPages=null] Optional per-category page cap.
 * @property {number | null} [maxProfiles=null] Optional per-category profile cap.
 * @property {boolean} [includeHtml=false] Whether raw HTML should be retained.
 */

/**
 * Build a shell command for one Pinellas BBB category harvest.
 *
 * @param {PinellasBbbCategorySource} source Verified city/trade category source.
 * @param {string} outputDirectory Absolute or repo-relative output directory.
 * @param {PinellasBbbHarvestPlanOptions} [options] Harvest CLI options.
 * @returns {string} `node scripts/harvest-bbb-category.mjs` command.
 */
export function buildPinellasBbbHarvestCommand(
  source,
  outputDirectory,
  options = {},
) {
  const chromiumExecutablePath =
    options.chromiumExecutablePath ?? "/usr/local/bin/google-chrome";
  const headless = options.headless ?? true;
  const includeHtml = options.includeHtml ?? false;
  const args = [
    "node scripts/harvest-bbb-category.mjs",
    `--category-url ${source.categoryUrl}`,
    `--output-dir ${path.resolve(outputDirectory)}`,
    `--chromium-executable-path ${chromiumExecutablePath}`,
    `--headless ${headless ? "true" : "false"}`,
    "--profile-subpages none",
  ];
  if (!includeHtml) args.push("--no-html");
  if (options.maxPages !== undefined && options.maxPages !== null) {
    args.push(`--max-pages ${options.maxPages}`);
  }
  if (options.maxProfiles !== undefined && options.maxProfiles !== null) {
    args.push(`--max-profiles ${options.maxProfiles}`);
  }
  return args.join(" ");
}

/**
 * Build the recommended Pinellas BBB probe command (St. Petersburg roofing).
 *
 * @param {string} [outputRoot="downloads/pinellas/bbb-probe"] Probe output root.
 * @param {PinellasBbbHarvestPlanOptions} [options] Harvest CLI options.
 * @returns {string} Probe shell command.
 */
export function buildPinellasBbbProbeCommand(
  outputRoot = "downloads/pinellas/bbb-probe",
  options = {},
) {
  const source = PINELLAS_BBB_CATEGORY_SOURCES.find(
    (entry) =>
      entry.cityKey === "st-petersburg" && entry.tradeKey === "roofing",
  );
  if (source === undefined) {
    throw new Error("Missing St. Petersburg roofing BBB source");
  }
  const outputDirectory = path.join(
    path.resolve(outputRoot),
    source.cityKey,
    source.tradeKey,
  );
  return buildPinellasBbbHarvestCommand(source, outputDirectory, {
    ...options,
    maxPages: 2,
    maxProfiles: 15,
  });
}

/**
 * Build a local multi-city, multi-trade BBB harvest plan without launching a browser.
 *
 * @param {string} outputRoot BBB output root.
 * @param {PinellasBbbHarvestPlanOptions} [options] Harvest CLI options.
 * @returns {JsonObject} Planned verified category inputs.
 */
export function buildPinellasBbbHarvestPlan(outputRoot, options = {}) {
  const resolvedRoot = path.resolve(outputRoot);
  return {
    schemaVersion: "oracle-node.pinellas-bbb-harvest-plan.v1",
    county: "pinellas",
    locationBasis:
      "BBB St. Petersburg and Clearwater, FL category search (Tampa excluded)",
    outputRoot: resolvedRoot,
    probeCommand: buildPinellasBbbProbeCommand(resolvedRoot, options),
    categories: PINELLAS_BBB_CATEGORY_SOURCES.map((source) => ({
      ...source,
      outputDirectory: path.join(
        resolvedRoot,
        source.cityKey,
        source.tradeKey,
      ),
      command: buildPinellasBbbHarvestCommand(
        source,
        path.join(resolvedRoot, source.cityKey, source.tradeKey),
        options,
      ),
    })),
    complete: false,
    evidence:
      "Plan only. Completion requires uncapped harvest manifests and zero failed profiles for every configured city/trade pair.",
  };
}
