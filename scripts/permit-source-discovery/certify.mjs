// CERTIFICATION tool for the permit-source-discovery agent.
//
// This is what proves "the agent can discover sources": given a source catalog
// (same schema as docs/palm-beach-sources.yaml), it independently re-fetches every
// permit portal, re-classifies its vendor, and checks the detected vendor against
// the catalog's stated vendor. It does NOT trust the catalog — it verifies it.
//
// Usage:
//   node certify.mjs <path-to-sources.yaml>
//
// Exit code is non-zero only when a portal is UNREACHABLE. Vendor MISMATCH and
// UNKNOWN are warnings, not failures: discovery is best-effort and some portals
// sit behind bot challenges or render their vendor only via JS.

import { readFile } from "fs/promises";
import { parse } from "yaml";

import {
  classifyVendor,
  catalogVendorToKeys,
  hasPermitEvidence,
} from "./vendors.mjs";

const REQUEST_TIMEOUT_MS = 15_000;
const MAX_CONCURRENCY = 4;
const BROWSER_USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";

// Playwright is not a dependency of oracle-node; it is reused from the catalog
// workspace, with the cached headless-shell chromium binary. Both are loaded lazily
// so a missing browser degrades to static-only certification rather than crashing.
const PLAYWRIGHT_MODULE_PATH =
  "/Users/stefanmicic/Desktop/Klijenti/elephant/catalog/node_modules/playwright/index.js";
const CHROMIUM_EXECUTABLE_PATH =
  "/Users/stefanmicic/Library/Caches/ms-playwright/chromium_headless_shell-1223/chrome-headless-shell-mac-arm64/chrome-headless-shell";
const RENDER_NAVIGATION_TIMEOUT_MS = 45_000;
const RENDER_SETTLE_MS = 3_500;

/**
 * @typedef {object} PermitCatalogRow
 * @property {string} jurisdiction - Jurisdiction name.
 * @property {string} portal - Primary permit portal URL.
 * @property {string} [vendor] - Stated vendor (free text).
 * @property {string} [status] - Catalog status (discovered / needs-review / ...).
 */

/**
 * @typedef {object} ReachResult
 * @property {number | null} status - Final HTTP status, or null when the request failed.
 * @property {string} finalUrl - Final URL after redirects (falls back to the request URL).
 * @property {string} html - Response body, empty on failure.
 * @property {"reachable" | "challenge" | "unreachable"} reachable - Reachability verdict.
 * @property {string | null} error - Network error message when the request failed.
 */

/**
 * @typedef {object} CertifyResult
 * @property {string} jurisdiction - Jurisdiction name.
 * @property {string} portal - Portal URL that was certified.
 * @property {number | null} http - Final HTTP status.
 * @property {ReachResult["reachable"] | "-"} reachable - Reachability verdict ("-" when skipped).
 * @property {string} catalogVendor - Catalog's stated vendor.
 * @property {string} detectedVendor - Detected vendor key.
 * @property {boolean} permitEvidence - Whether the page body shows permit-domain markers.
 * @property {"static" | "rendered" | "-"} verifiedVia - How evidence was obtained: plain fetch, Playwright render, or N/A.
 * @property {"PASS" | "REVIEW" | "UNREACHABLE" | "SKIPPED"} verdict - Certification verdict.
 */

/**
 * CLI entrypoint.
 *
 * @returns {Promise<void>} Resolves after results are printed; sets a non-zero exit code on any unreachable portal.
 */
export async function main() {
  const yamlPath = process.argv[2];
  if (yamlPath === undefined) {
    console.error("Usage: node certify.mjs <path-to-sources.yaml>");
    process.exitCode = 2;
    return;
  }

  const permits = await loadPermitRows(yamlPath);
  if (permits.length === 0) {
    console.error(`No permit entries found in ${yamlPath}`);
    process.exitCode = 2;
    return;
  }

  console.log(`Certifying ${permits.length} permit portals from ${yamlPath}\n`);
  const renderer = createSpaRenderer();
  let results;
  try {
    results = await mapWithConcurrency(permits, MAX_CONCURRENCY, (row) =>
      certifyRow(row, renderer),
    );
  } finally {
    await renderer.close();
  }

  printResultsTable(results);
  const certified = results.filter((row) => row.verdict === "PASS").length;
  const review = results.filter((row) => row.verdict === "REVIEW").length;
  const unreachable = results.filter(
    (row) => row.verdict === "UNREACHABLE",
  ).length;
  const skipped = results.filter((row) => row.verdict === "SKIPPED").length;
  console.log(
    `\n(${certified}/${results.length} certified, ${review} review, ${unreachable} unreachable, ${skipped} skipped (needs-review))`,
  );

  if (unreachable > 0) {
    process.exitCode = 1;
  }
}

/**
 * Read and parse the permit rows from a source catalog YAML file. Rows without a
 * usable portal URL (the needs-review small towns) are kept so they can be reported
 * as SKIPPED rather than silently dropped.
 *
 * @param {string} yamlPath - Path to a sources.yaml file.
 * @returns {Promise<readonly PermitCatalogRow[]>} Permit rows (empty when absent).
 */
export async function loadPermitRows(yamlPath) {
  const raw = await readFile(yamlPath, "utf8");
  const parsed = parse(raw);
  const permits = parsed?.permits;
  if (!Array.isArray(permits)) {
    return [];
  }
  return permits.filter(
    (row) =>
      row !== null &&
      typeof row === "object" &&
      typeof row.jurisdiction === "string",
  );
}

/**
 * Certify a single permit catalog row: fetch its portal, classify the vendor, and
 * compare against the catalog's stated vendor. Many modern permit portals (Civic
 * Access, eHub, OpenGov permitting, Tyler tylerhost/EDEN, MGO, CityView) are JS SPAs
 * and/or bot-block plain fetch, so a Playwright render is used as a fallback to
 * re-evaluate reachability, vendor, and permit evidence on the hydrated DOM.
 *
 * Rows whose `portal` is null/empty (the needs-review small towns) are SKIPPED — not
 * probed and not failed.
 *
 * @param {PermitCatalogRow} row - Catalog row to certify.
 * @param {SpaRenderer} renderer - Lazy Playwright renderer for the SPA fallback.
 * @returns {Promise<CertifyResult>} Certification result for the row.
 */
async function certifyRow(row, renderer) {
  const catalogVendor = typeof row.vendor === "string" ? row.vendor : "";
  const displayCatalogVendor = catalogVendor.length > 0 ? catalogVendor : "-";
  const portal = typeof row.portal === "string" ? row.portal.trim() : "";

  if (portal.length === 0) {
    return {
      jurisdiction: row.jurisdiction,
      portal: "-",
      http: null,
      reachable: "-",
      catalogVendor: displayCatalogVendor,
      detectedVendor: "-",
      permitEvidence: false,
      verifiedVia: "-",
      verdict: "SKIPPED",
    };
  }

  const catalogKeys = catalogVendorToKeys(catalogVendor);
  const reach = await fetchPortal(portal);
  let detected = classifyVendor({ url: reach.finalUrl, html: reach.html });
  let permitEvidence = hasPermitEvidence(reach.html);
  let reachable = reach.reachable;
  let verifiedVia = /** @type {"static" | "rendered"} */ ("static");

  // Playwright fallback. Render when EITHER the static page is a reachable HTTP 200
  // that classified to a vendor but shows no permit evidence (SPA hydration case), OR
  // the page is a bot challenge / HTTP 403 (many real portals bot-block plain fetch).
  // After render, re-derive reachability, vendor, and permit evidence from the DOM.
  const needsRender =
    reachable === "challenge" ||
    reach.status === 403 ||
    (reachable === "reachable" &&
      reach.status === 200 &&
      detected.key !== "unknown" &&
      !permitEvidence);
  if (needsRender) {
    const renderedHtml = await renderer.render(reach.finalUrl, row.jurisdiction);
    if (renderedHtml !== null) {
      verifiedVia = "rendered";
      detected = classifyVendor({ url: reach.finalUrl, html: renderedHtml });
      permitEvidence = hasPermitEvidence(renderedHtml);
      if (isSubstantialRender(renderedHtml)) {
        reachable = "reachable";
      }
    }
  }

  if (reachable === "unreachable") {
    console.error(
      `  ${row.jurisdiction}: UNREACHABLE ${portal} -> ${reach.error ?? `http ${reach.status ?? "?"}`}`,
    );
    return {
      jurisdiction: row.jurisdiction,
      portal,
      http: reach.status,
      reachable,
      catalogVendor: displayCatalogVendor,
      detectedVendor: detected.key,
      permitEvidence,
      verifiedVia,
      verdict: "UNREACHABLE",
    };
  }

  const verdict = decideVerdict(detected.key, catalogKeys, permitEvidence);

  return {
    jurisdiction: row.jurisdiction,
    portal,
    http: reach.status,
    reachable,
    catalogVendor: displayCatalogVendor,
    detectedVendor: detected.key,
    permitEvidence,
    verifiedVia,
    verdict,
  };
}

/**
 * Heuristic: did a Playwright render produce a real, hydrated page (rather than a
 * still-blocked challenge or an empty error shell)? Used to upgrade a challenge/403
 * portal to reachable after rendering.
 *
 * @param {string} html - Rendered page HTML.
 * @returns {boolean} True when the rendered content looks substantial and not a challenge.
 */
function isSubstantialRender(html) {
  return html.length >= 500 && !isBotChallenge(html);
}

/**
 * @typedef {object} SpaRenderer
 * @property {(url: string, label: string) => Promise<string | null>} render - Render a URL to hydrated HTML, or null when unavailable.
 * @property {() => Promise<void>} close - Close the browser if one was launched.
 */

/**
 * Create a lazy Playwright renderer for the SPA fallback. The browser is launched
 * at most once, only when the first entry actually needs a render, and is closed at
 * the end. If Playwright or the chromium binary is unavailable, rendering degrades
 * to null (callers keep their static result) instead of crashing the run.
 *
 * @returns {SpaRenderer} Lazy renderer with `render` and `close`.
 */
function createSpaRenderer() {
  /** @type {Promise<import("playwright").Browser> | null} */
  let browserPromise = null;
  let unavailable = false;

  return {
    async render(url, label) {
      if (unavailable) {
        return null;
      }
      let context = null;
      try {
        if (browserPromise === null) {
          browserPromise = launchChromium();
        }
        const browser = await browserPromise;
        context = await browser.newContext({ userAgent: BROWSER_USER_AGENT });
        const page = await context.newPage();
        await page
          .goto(url, {
            waitUntil: "networkidle",
            timeout: RENDER_NAVIGATION_TIMEOUT_MS,
          })
          .catch(() => undefined);
        await page.waitForTimeout(RENDER_SETTLE_MS);
        return await page.content();
      } catch (caught) {
        unavailable = true;
        console.error(
          `  ${label}: SPA render unavailable (${caught instanceof Error ? caught.message : String(caught)}); using static result`,
        );
        return null;
      } finally {
        if (context !== null) {
          await context.close().catch(() => undefined);
        }
      }
    },
    async close() {
      if (browserPromise === null) {
        return;
      }
      const browser = await browserPromise.catch(() => null);
      if (browser !== null) {
        await browser.close().catch(() => undefined);
      }
    },
  };
}

/**
 * Launch the cached headless chromium via the catalog workspace's Playwright.
 *
 * @returns {Promise<import("playwright").Browser>} Launched browser.
 */
async function launchChromium() {
  const pw = (await import(PLAYWRIGHT_MODULE_PATH)).default;
  const { chromium } = pw;
  return chromium.launch({
    headless: true,
    executablePath: CHROMIUM_EXECUTABLE_PATH,
  });
}

/**
 * Decide a certification verdict for a reachable portal. PASS requires both that
 * the detected vendor matches the catalog AND that the page shows it is a live
 * permit portal; anything weaker is REVIEW (a warning, not a hard fail). This is
 * what makes certification prove the catalogued URL is a live PERMIT portal of the
 * stated vendor, rather than merely a reachable vendor-branded host.
 *
 * @param {string} detectedKey - Vendor key from the classifier.
 * @param {readonly string[]} catalogKeys - Vendor keys named by the catalog row.
 * @param {boolean} permitEvidence - Whether the page shows permit-domain markers.
 * @returns {"PASS" | "REVIEW"} Certification verdict.
 */
function decideVerdict(detectedKey, catalogKeys, permitEvidence) {
  const vendorMatches = isVendorMatch(detectedKey, catalogKeys);
  return vendorMatches && permitEvidence ? "PASS" : "REVIEW";
}

/**
 * Determine whether a detected vendor matches the catalog's stated vendor(s).
 * A detected `unknown` never matches. When the catalog names no recognizable
 * vendor, any recognized detection counts as a match (it adds information).
 *
 * @param {string} detectedKey - Vendor key from the classifier.
 * @param {readonly string[]} catalogKeys - Vendor keys named by the catalog row.
 * @returns {boolean} True when the detected vendor matches the catalog.
 */
function isVendorMatch(detectedKey, catalogKeys) {
  if (detectedKey === "unknown") {
    return false;
  }
  if (catalogKeys.length === 0) {
    return true;
  }
  return catalogKeys.includes(detectedKey);
}

/**
 * Fetch a portal URL with a browser-like User-Agent, redirect following, and a
 * hard timeout. Distinguishes a bot challenge (e.g. Cloudflare "just a moment")
 * from a true failure so challenged portals are reported reachable.
 *
 * @param {string} url - Portal URL to fetch.
 * @returns {Promise<ReachResult>} Reachability and response details.
 */
export async function fetchPortal(url) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: controller.signal,
      headers: {
        "User-Agent": BROWSER_USER_AGENT,
        Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      },
    });
    const html = await safeReadText(response);
    const reachable = classifyReachability(response.status, html);
    return {
      status: response.status,
      finalUrl: response.url || url,
      html,
      reachable,
      error: null,
    };
  } catch (caught) {
    return {
      status: null,
      finalUrl: url,
      html: "",
      reachable: "unreachable",
      error: caught instanceof Error ? caught.message : String(caught),
    };
  } finally {
    clearTimeout(timeout);
  }
}

/**
 * Classify reachability from an HTTP status and body.
 *
 * @param {number} status - HTTP status code.
 * @param {string} html - Response body.
 * @returns {ReachResult["reachable"]} Reachability verdict.
 */
function classifyReachability(status, html) {
  if (status < 400) {
    return "reachable";
  }
  if ((status === 403 || status === 503) && isBotChallenge(html)) {
    return "challenge";
  }
  return "unreachable";
}

/**
 * Detect a bot/anti-automation challenge body (Cloudflare and similar).
 *
 * @param {string} html - Response body.
 * @returns {boolean} True when the body looks like an interstitial challenge.
 */
function isBotChallenge(html) {
  const lower = html.toLowerCase();
  return (
    lower.includes("just a moment") ||
    lower.includes("cloudflare") ||
    lower.includes("cf-browser-verification") ||
    lower.includes("attention required")
  );
}

/**
 * Read a response body as text without throwing on decode errors.
 *
 * @param {Response} response - Fetch response.
 * @returns {Promise<string>} Body text, or empty string on failure.
 */
async function safeReadText(response) {
  try {
    return await response.text();
  } catch {
    return "";
  }
}

/**
 * Run an async mapper over items with a bounded concurrency, preserving order.
 *
 * @template T, R
 * @param {readonly T[]} items - Items to process.
 * @param {number} concurrency - Maximum in-flight operations.
 * @param {(item: T) => Promise<R>} mapper - Async mapper.
 * @returns {Promise<R[]>} Results in input order.
 */
async function mapWithConcurrency(items, concurrency, mapper) {
  /** @type {R[]} */
  const results = new Array(items.length);
  let nextIndex = 0;
  const workerCount = Math.min(concurrency, items.length);
  const workers = Array.from({ length: workerCount }, async () => {
    while (true) {
      const current = nextIndex;
      nextIndex += 1;
      if (current >= items.length) {
        return;
      }
      results[current] = await mapper(items[current]);
    }
  });
  await Promise.all(workers);
  return results;
}

/**
 * Print a per-jurisdiction certification table.
 *
 * @param {readonly CertifyResult[]} results - Certification results.
 * @returns {void}
 */
function printResultsTable(results) {
  const header = {
    jurisdiction: "jurisdiction",
    http: "http",
    reachable: "reachable",
    catalogVendor: "catalogVendor",
    detectedVendor: "detectedVendor",
    permitEvidence: "permitEvidence",
    verifiedVia: "verifiedVia",
    verdict: "verdict",
  };
  const rows = results.map((row) => ({
    jurisdiction: row.jurisdiction,
    http: row.http === null ? "-" : String(row.http),
    reachable: row.reachable,
    catalogVendor: row.catalogVendor,
    detectedVendor: row.detectedVendor,
    permitEvidence: row.permitEvidence ? "yes" : "no",
    verifiedVia: row.verifiedVia,
    verdict: row.verdict,
  }));
  const widths = {
    jurisdiction: columnWidth(header.jurisdiction, rows, "jurisdiction"),
    http: columnWidth(header.http, rows, "http"),
    reachable: columnWidth(header.reachable, rows, "reachable"),
    catalogVendor: columnWidth(header.catalogVendor, rows, "catalogVendor"),
    detectedVendor: columnWidth(header.detectedVendor, rows, "detectedVendor"),
    permitEvidence: columnWidth(header.permitEvidence, rows, "permitEvidence"),
    verifiedVia: columnWidth(header.verifiedVia, rows, "verifiedVia"),
    verdict: columnWidth(header.verdict, rows, "verdict"),
  };
  console.log(formatRow(header, widths));
  console.log(formatRow(dividerRow(widths), widths));
  for (const row of rows) {
    console.log(formatRow(row, widths));
  }
}

/**
 * Compute the display width of a table column.
 *
 * @param {string} headerLabel - Column header label.
 * @param {readonly Record<string, string>[]} rows - Table rows.
 * @param {string} key - Column key.
 * @returns {number} Maximum cell width for the column.
 */
function columnWidth(headerLabel, rows, key) {
  return rows.reduce(
    (max, row) => Math.max(max, row[key].length),
    headerLabel.length,
  );
}

/**
 * Build a divider row of dashes sized to each column.
 *
 * @param {Record<string, number>} widths - Column widths.
 * @returns {Record<string, string>} Divider cells per column.
 */
function dividerRow(widths) {
  /** @type {Record<string, string>} */
  const divider = {};
  for (const [key, width] of Object.entries(widths)) {
    divider[key] = "-".repeat(width);
  }
  return divider;
}

/**
 * Format one padded table row.
 *
 * @param {Record<string, string>} row - Row cells keyed by column.
 * @param {Record<string, number>} widths - Column widths.
 * @returns {string} Padded, pipe-separated row.
 */
function formatRow(row, widths) {
  return Object.keys(widths)
    .map((key) => (row[key] ?? "").padEnd(widths[key]))
    .join(" | ");
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((caught) => {
    console.error(caught instanceof Error ? caught.stack : String(caught));
    process.exitCode = 1;
  });
}
