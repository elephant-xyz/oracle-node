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
      categoryUrl: buildPinellasBbbCategoryUrl(
        city.bbbSlug,
        trade.categorySlug,
      ),
    })),
  ),
);

/**
 * Robots-compliant page cap for any local Pinellas BBB browser harvest.
 * BBB robots.txt disallows `/*?`; `harvest-bbb-category.mjs` paginates with
 * `?page=N`, and page 2+ returns HTTP 403 from a vanilla Puppeteer session.
 *
 * @type {1}
 */
export const PINELLAS_BBB_ROBOTS_COMPLIANT_MAX_PAGES = 1;

/**
 * Official BBB developer portal for production contractor enrichment.
 *
 * @type {"https://developer.bbb.org"}
 */
export const PINELLAS_BBB_PRODUCTION_API_URL = "https://developer.bbb.org";

/**
 * @typedef {object} PinellasBbbHarvestPlanOptions
 * @property {string} [chromiumExecutablePath="/usr/local/bin/google-chrome"] Chromium executable for Puppeteer.
 * @property {boolean} [headless=true] Whether Puppeteer should run headless.
 * @property {number | null} [maxPages=1] Per-category page cap; defaults to robots-compliant page 1 only.
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
  const maxPages =
    options.maxPages === undefined
      ? PINELLAS_BBB_ROBOTS_COMPLIANT_MAX_PAGES
      : options.maxPages;
  if (maxPages !== null) {
    args.push(`--max-pages ${maxPages}`);
  }
  if (options.maxProfiles !== undefined && options.maxProfiles !== null) {
    args.push(`--max-profiles ${options.maxProfiles}`);
  }
  return args.join(" ");
}

/**
 * Build a robots-compliant page-1 Pinellas BBB probe command (St. Petersburg roofing).
 *
 * The 2026-09-07 warm-session sample that reached page 2 is historical evidence only;
 * do not treat a successful `?page=2` fetch as permission to scale paginated crawls.
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
    maxPages: PINELLAS_BBB_ROBOTS_COMPLIANT_MAX_PAGES,
    maxProfiles: 15,
  });
}

/**
 * Build a local multi-city, multi-trade BBB harvest plan without launching a browser.
 *
 * Production enrichment should use the official BBB API (`developer.bbb.org`).
 * Local browser commands are page-1-only fallbacks and must not paginate with `?page=N`.
 *
 * @param {string} outputRoot BBB output root.
 * @param {PinellasBbbHarvestPlanOptions} [options] Harvest CLI options.
 * @returns {JsonObject} Planned verified category inputs and operator guidance.
 */
export function buildPinellasBbbHarvestPlan(outputRoot, options = {}) {
  const resolvedRoot = path.resolve(outputRoot);
  const pageOneOptions = {
    ...options,
    maxPages: PINELLAS_BBB_ROBOTS_COMPLIANT_MAX_PAGES,
  };
  return {
    schemaVersion: "oracle-node.pinellas-bbb-harvest-plan.v2",
    county: "pinellas",
    locationBasis:
      "BBB St. Petersburg and Clearwater, FL category search (Tampa excluded)",
    outputRoot: resolvedRoot,
    paginationPolicy: {
      robotsTxtDisallow: "/*?",
      harvesterPaginationQuery: "?page=N",
      multiPageScrape: "stop",
      localMaxPages: PINELLAS_BBB_ROBOTS_COMPLIANT_MAX_PAGES,
      note: "Do not run uncapped or multi-page category harvests. Page 2+ returns HTTP 403.",
    },
    recommendedProductionPath: {
      method: "bbb-api",
      applicationUrl: PINELLAS_BBB_PRODUCTION_API_URL,
      note: "Apply for official BBB API access for production contractor reputation enrichment.",
    },
    operatorNextStep:
      "Apply for BBB API access at developer.bbb.org. Do not run full paginated browser harvests. If local browser sampling continues, use page-1-only commands from this plan.",
    doNotRun:
      "Full paginated category harvest (omit --max-pages or set --max-pages > 1).",
    probeCommand: buildPinellasBbbProbeCommand(resolvedRoot, pageOneOptions),
    categories: PINELLAS_BBB_CATEGORY_SOURCES.map((source) => ({
      ...source,
      outputDirectory: path.join(resolvedRoot, source.cityKey, source.tradeKey),
      command: buildPinellasBbbHarvestCommand(
        source,
        path.join(resolvedRoot, source.cityKey, source.tradeKey),
        pageOneOptions,
      ),
    })),
    complete: false,
    evidence:
      "Plan only. The 2026-09-07 St. Petersburg roofing probe (2 pages, 15 profiles) ran in a warm browser session and is not a license to scale ?page=N crawls. Production enrichment requires BBB API approval.",
  };
}
