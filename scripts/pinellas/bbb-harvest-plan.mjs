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
 * City execution order for the operator full-paginate harvest runner.
 * Clearwater is sequenced first because it was proven in warm-session runs.
 *
 * @type {readonly PinellasBbbCityKey[]}
 */
export const PINELLAS_BBB_FULL_HARVEST_CITY_ORDER = Object.freeze([
  "clearwater",
  "st-petersburg",
]);

/**
 * Trade execution order within each city for the full-paginate harvest runner.
 *
 * @type {readonly PinellasBbbTradeKey[]}
 */
export const PINELLAS_BBB_FULL_HARVEST_TRADE_ORDER = Object.freeze([
  "roofing",
  "hvac",
  "solar",
]);

/**
 * Default page cap for the operator full-paginate harvest runner.
 *
 * @type {1000}
 */
export const PINELLAS_BBB_FULL_HARVEST_MAX_PAGES = 1000;

/**
 * Default delay between category pages for the full-paginate harvest runner.
 *
 * @type {2000}
 */
export const PINELLAS_BBB_FULL_HARVEST_PAGE_DELAY_MS = 2000;

/**
 * Default delay between profile fetches for the full-paginate harvest runner.
 *
 * @type {1500}
 */
export const PINELLAS_BBB_FULL_HARVEST_PROFILE_DELAY_MS = 1500;

/**
 * @typedef {object} PinellasBbbHarvestPlanOptions
 * @property {string} [chromiumExecutablePath="/usr/local/bin/google-chrome"] Chromium executable for Puppeteer.
 * @property {boolean} [headless=true] Whether Puppeteer should run headless.
 * @property {number | null} [maxPages=1] Per-category page cap; defaults to robots-compliant page 1 only.
 * @property {number | null} [maxProfiles=null] Optional per-category profile cap.
 * @property {boolean} [includeHtml=false] Whether raw HTML should be retained.
 * @property {number} [pageDelayMs] Delay between category pages in milliseconds.
 * @property {number} [profileDelayMs] Delay between profile fetches in milliseconds.
 */

/**
 * @typedef {object} PinellasBbbFullHarvestJob
 * @property {PinellasBbbCityKey} cityKey Stable Pinellas city key.
 * @property {PinellasBbbTradeKey} tradeKey Stable trade key.
 * @property {string} categoryUrl Verified BBB category URL.
 * @property {string} outputDirectory Absolute output directory for the job.
 * @property {string} command Shell-ready harvest command for the job.
 */

/**
 * @typedef {object} PinellasBbbFullHarvestPlanOptions
 * @property {string} [chromiumExecutablePath="/usr/local/bin/google-chrome"] Chromium executable for Puppeteer.
 * @property {boolean} [headless=true] Whether Puppeteer should run headless.
 * @property {number} [maxPages=1000] Per-category page cap for operator full harvest.
 * @property {number} [pageDelayMs=2000] Delay between category pages in milliseconds.
 * @property {number} [profileDelayMs=1500] Delay between profile fetches in milliseconds.
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
  if (options.pageDelayMs !== undefined) {
    args.push(`--page-delay-ms ${options.pageDelayMs}`);
  }
  if (options.profileDelayMs !== undefined) {
    args.push(`--profile-delay-ms ${options.profileDelayMs}`);
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
 * Resolve a verified Pinellas BBB category source by city and trade keys.
 *
 * @param {PinellasBbbCityKey} cityKey Stable Pinellas city key.
 * @param {PinellasBbbTradeKey} tradeKey Stable trade key.
 * @returns {PinellasBbbCategorySource} Matching verified category source.
 */
export function findPinellasBbbCategorySource(cityKey, tradeKey) {
  const source = PINELLAS_BBB_CATEGORY_SOURCES.find(
    (entry) => entry.cityKey === cityKey && entry.tradeKey === tradeKey,
  );
  if (source === undefined) {
    throw new Error(`Missing Pinellas BBB source for ${cityKey}/${tradeKey}`);
  }
  return source;
}

/**
 * Build the ordered operator full-paginate harvest jobs for Pinellas BBB.
 *
 * This is an explicit operator override for warm-session multi-page harvests.
 * It does not replace the robots-compliant page-1 plan in
 * `buildPinellasBbbHarvestPlan`.
 *
 * @param {string} outputRoot BBB output root.
 * @param {PinellasBbbFullHarvestPlanOptions} [options] Full harvest CLI options.
 * @returns {readonly PinellasBbbFullHarvestJob[]} Ordered city/trade jobs.
 */
export function buildPinellasBbbFullHarvestJobs(outputRoot, options = {}) {
  const resolvedRoot = path.resolve(outputRoot);
  const harvestOptions = {
    chromiumExecutablePath:
      options.chromiumExecutablePath ?? "/usr/local/bin/google-chrome",
    headless: options.headless ?? true,
    includeHtml: false,
    maxPages: options.maxPages ?? PINELLAS_BBB_FULL_HARVEST_MAX_PAGES,
    pageDelayMs: options.pageDelayMs ?? PINELLAS_BBB_FULL_HARVEST_PAGE_DELAY_MS,
    profileDelayMs:
      options.profileDelayMs ?? PINELLAS_BBB_FULL_HARVEST_PROFILE_DELAY_MS,
  };

  return PINELLAS_BBB_FULL_HARVEST_CITY_ORDER.flatMap((cityKey) =>
    PINELLAS_BBB_FULL_HARVEST_TRADE_ORDER.map((tradeKey) => {
      const source = findPinellasBbbCategorySource(cityKey, tradeKey);
      const outputDirectory = path.join(
        resolvedRoot,
        source.cityKey,
        source.tradeKey,
      );
      return {
        cityKey: source.cityKey,
        tradeKey: source.tradeKey,
        categoryUrl: source.categoryUrl,
        outputDirectory,
        command: buildPinellasBbbHarvestCommand(
          source,
          outputDirectory,
          harvestOptions,
        ),
      };
    }),
  );
}

/**
 * Build a local multi-city, multi-trade BBB harvest plan without launching a browser.
 *
 * Two paths coexist:
 * - **This function** — robots-compliant page-1-only sampling and API-first guidance
 *   for agents evaluating BBB without an operator override.
 * - **`buildPinellasBbbFullHarvestJobs`** — operator full-ingest path (sequential
 *   multi-page harvest via `scripts/pinellas/run-bbb-full-harvest.mjs`). If tmux
 *   `pinellas-bbb-full` is already running, do not stop it because this plan's
 *   `doNotRun` field discourages *new* unapproved paginated crawls.
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
      note: "Cold-session page 2+ may return HTTP 403. Operator override 2026-09-07: full ingest uses buildPinellasBbbFullHarvestJobs — leave pinellas-bbb-full running.",
    },
    recommendedProductionPath: {
      method: "bbb-api",
      applicationUrl: PINELLAS_BBB_PRODUCTION_API_URL,
      note: "Apply for official BBB API access for production contractor reputation enrichment.",
    },
    operatorFullIngestPath: {
      method: "sequential-browser-pagination",
      planBuilder: "buildPinellasBbbFullHarvestJobs",
      runnerShell: "scripts/pinellas/run-bbb-full-harvest.sh",
      runnerModule: "scripts/pinellas/run-bbb-full-harvest.mjs",
      tmuxSession: "pinellas-bbb-full",
      outputRoot: "downloads/pinellas/bbb-harvest",
      jobCount: 6,
      chromiumExecutablePath: "/usr/local/bin/google-chrome",
      note: "Operator override 2026-09-07. Vanilla Puppeteer (not stealth, not BBB API). If already running, do not kill Accela or BBB.",
    },
    operatorNextStep:
      "If pinellas-bbb-full is already running, monitor downloads/pinellas/bbb-harvest/ and do not stop it. Otherwise Apply for BBB API access at developer.bbb.org, or use page-1-only commands from this plan for sampling.",
    doNotRun:
      "Starting a new unapproved full paginated category harvest (omit --max-pages or set --max-pages > 1) when pinellas-bbb-full is not already running.",
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
      "Plan only. Page-1 probes and cold-session 403 notes are historical context. Operator override 2026-09-07: full ingest runs via buildPinellasBbbFullHarvestJobs (tmux pinellas-bbb-full). Long-term pipeline enrichment may still use BBB API approval.",
  };
}
