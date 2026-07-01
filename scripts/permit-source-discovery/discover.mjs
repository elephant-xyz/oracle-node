// Best-effort automated permit-portal discovery for the permit-source-discovery agent.
//
// SCOPE: This is the DETERMINISTIC helper, not the brain. It only resolves and
// classifies *candidate* portal URLs generated from common vendor-host shapes, and
// flags the misses. The genuinely hard part — finding a city's official website
// from its name (web search, disambiguation, reading the city's "permits" page) —
// is the LLM agent's job. The agent feeds verified portal URLs here (or into the
// catalog), and this script + certify.mjs prove and re-prove them deterministically.
//
// Usage:
//   node discover.mjs --county "<name>" --jurisdictions <file.json|comma-list> [--out <sources.yaml>]
//
// --jurisdictions accepts either a path to a JSON array of names, or a comma-separated list.
// With --out, discovered/needs-review rows are merged into that file's `permits:` list
// without clobbering existing rows already marked `discovered`. Without --out, the rows
// are printed to stdout in the same YAML schema as docs/palm-beach-sources.yaml.

import { readFile, writeFile } from "fs/promises";
import { parseArgs } from "util";
import { parse, stringify } from "yaml";

import { classifyVendor, hasPermitEvidence } from "./vendors.mjs";

const REQUEST_TIMEOUT_MS = 15_000;
const MAX_CONCURRENCY = 4;
const BROWSER_USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";

/**
 * @typedef {object} DiscoveredRow
 * @property {string} jurisdiction - Jurisdiction name.
 * @property {string} [portal] - Resolved portal URL when a candidate classified.
 * @property {string} vendor - Detected vendor name, or "unknown".
 * @property {string} status - "discovered" when classified, "needs-review" otherwise.
 * @property {readonly string[]} [candidates_tried] - Candidate URLs attempted (for needs-review handoff).
 */

/**
 * CLI entrypoint.
 *
 * @returns {Promise<void>} Resolves after discovery rows are merged or printed.
 */
export async function main() {
  const options = parseCliOptions(process.argv.slice(2));
  const jurisdictions = await loadJurisdictions(options.jurisdictions);
  if (jurisdictions.length === 0) {
    console.error("No jurisdictions provided.");
    process.exitCode = 2;
    return;
  }

  console.error(
    `Discovering portals for ${jurisdictions.length} jurisdictions in ${options.county}...`,
  );
  const rows = await mapWithConcurrency(
    jurisdictions,
    MAX_CONCURRENCY,
    discoverJurisdiction,
  );

  const discovered = rows.filter((row) => row.status === "discovered").length;
  console.error(
    `Classified ${discovered}/${rows.length}; ${rows.length - discovered} flagged needs-review (LLM agent / human handoff).`,
  );

  if (options.out === undefined) {
    process.stdout.write(renderCatalogYaml(options.county, rows));
    return;
  }
  await mergeIntoCatalog(options.out, rows);
  console.error(`Merged ${rows.length} rows into ${options.out}`);
}

/**
 * Discover a portal for one jurisdiction by resolving and classifying candidate URLs.
 *
 * @param {string} jurisdiction - Jurisdiction name.
 * @returns {Promise<DiscoveredRow>} Discovered or needs-review row.
 */
async function discoverJurisdiction(jurisdiction) {
  const candidates = candidatePortalUrls(jurisdiction);
  for (const candidate of candidates) {
    const reach = await fetchPortal(candidate);
    // A candidate only qualifies as a live permit portal when ALL hold:
    //   (a) final HTTP status is exactly 200 — a redirect that ends in 404/error
    //       (e.g. a vendor host that bounces to a generic error page) does NOT count;
    //   (b) the body shows permit-domain evidence — guards against vendor-branded but
    //       non-permit pages (e.g. OpenGov financial transparency portals);
    //   (c) the vendor classifier recognizes the page.
    // Anything short of that is handed to the LLM agent / human via needs-review.
    if (reach.status !== 200) {
      continue;
    }
    if (!hasPermitEvidence(reach.html)) {
      continue;
    }
    const detected = classifyVendor({ url: reach.finalUrl, html: reach.html });
    if (detected.key !== "unknown") {
      console.error(
        `  ${jurisdiction}: discovered ${detected.name} at ${reach.finalUrl}`,
      );
      return {
        jurisdiction,
        portal: reach.finalUrl,
        vendor: detected.name,
        status: "discovered",
      };
    }
  }
  console.error(
    `  ${jurisdiction}: needs-review (no candidate is a live permit portal)`,
  );
  return {
    jurisdiction,
    vendor: "unknown",
    status: "needs-review",
    candidates_tried: candidates,
  };
}

/**
 * Generate candidate portal URLs for a jurisdiction from common vendor-host shapes.
 * These are heuristics, not authoritative; the LLM agent supplies the real URL when
 * none of these resolve to a recognizable vendor.
 *
 * @param {string} jurisdiction - Jurisdiction name.
 * @returns {readonly string[]} Ordered, de-duplicated candidate URLs.
 */
export function candidatePortalUrls(jurisdiction) {
  const slug = toSlug(jurisdiction);
  const compact = slug.replace(/-/g, "");
  /** @type {string[]} */
  const candidates = [
    // Click2Gov on the common aspgov.com hosting shape.
    `https://${compact}-egov.aspgov.com/Click2GovBP/`,
    `https://${slug}-egov.aspgov.com/Click2GovBP/`,
    // Tyler EnerGov "permit-planner" branded hosts.
    `https://permit-planner.${compact}.org/`,
    `https://permit-planner.${compact}.gov/`,
    // NOTE: bare `<slug>.opengov.com` is intentionally NOT a candidate — that host
    // defaults to OpenGov's financial transparency portal, not permitting. OpenGov
    // permitting lives elsewhere (e.g. viewpointcloud) under per-tenant permit paths
    // the LLM agent resolves; leave it to web-search + needs-review.
    // The city's own official site (the agent normally finds the real permits page).
    `https://www.${compact}.gov/`,
    `https://www.${compact}.org/`,
    `https://${compact}.gov/`,
    `https://${compact}fl.gov/`,
    `https://www.${compact}.us/`,
  ];
  return [...new Set(candidates)];
}

/**
 * Convert a jurisdiction name to a URL-friendly slug.
 *
 * @param {string} jurisdiction - Jurisdiction name.
 * @returns {string} Hyphenated lowercase slug.
 */
function toSlug(jurisdiction) {
  return jurisdiction
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

/**
 * Render discovery rows as a source-catalog YAML document.
 *
 * @param {string} county - County name.
 * @param {readonly DiscoveredRow[]} rows - Discovery rows.
 * @returns {string} YAML document with a `permits:` list.
 */
function renderCatalogYaml(county, rows) {
  const header =
    `# ${county} — permit portals discovered by discover.mjs (deterministic candidate resolution).\n` +
    `# 'discovered' = a candidate URL classified to a known vendor.\n` +
    `# 'needs-review' = no candidate classified; hand to the LLM agent / human to find the real portal.\n`;
  const document = stringify({ county, permits: rows.map(toCatalogRow) });
  return `${header}${document}`;
}

/**
 * Convert a discovery row to a catalog row matching docs/palm-beach-sources.yaml.
 *
 * @param {DiscoveredRow} row - Discovery row.
 * @returns {Record<string, unknown>} Catalog-shaped row.
 */
function toCatalogRow(row) {
  if (row.status === "discovered") {
    return {
      jurisdiction: row.jurisdiction,
      portal: row.portal,
      vendor: row.vendor,
      status: row.status,
    };
  }
  return {
    jurisdiction: row.jurisdiction,
    vendor: row.vendor,
    status: row.status,
    candidates_tried: row.candidates_tried ?? [],
  };
}

/**
 * Merge discovery rows into an existing catalog file's `permits:` list. Existing
 * rows already marked `discovered` are preserved (never clobbered); new rows are
 * appended and prior non-discovered rows for the same jurisdiction are replaced.
 *
 * @param {string} outPath - Path to the catalog YAML file to update.
 * @param {readonly DiscoveredRow[]} rows - Discovery rows to merge.
 * @returns {Promise<void>} Resolves after the file is rewritten.
 */
async function mergeIntoCatalog(outPath, rows) {
  const existing = await readCatalog(outPath);
  const permits = Array.isArray(existing.permits) ? [...existing.permits] : [];
  const indexByJurisdiction = new Map(
    permits.map((row, index) => [normalizeJurisdiction(row?.jurisdiction), index]),
  );

  for (const row of rows) {
    const key = normalizeJurisdiction(row.jurisdiction);
    const existingIndex = indexByJurisdiction.get(key);
    if (existingIndex !== undefined) {
      if (permits[existingIndex]?.status === "discovered") {
        continue;
      }
      permits[existingIndex] = toCatalogRow(row);
      continue;
    }
    permits.push(toCatalogRow(row));
    indexByJurisdiction.set(key, permits.length - 1);
  }

  const merged = { ...existing, permits };
  await writeFile(outPath, stringify(merged), "utf8");
}

/**
 * Read a catalog YAML file, returning an empty document when the file is absent.
 *
 * @param {string} outPath - Catalog file path.
 * @returns {Promise<Record<string, unknown>>} Parsed catalog object.
 */
async function readCatalog(outPath) {
  try {
    const raw = await readFile(outPath, "utf8");
    const parsed = parse(raw);
    return parsed !== null && typeof parsed === "object" ? parsed : {};
  } catch (caught) {
    if (caught instanceof Error && "code" in caught && caught.code === "ENOENT") {
      return {};
    }
    throw caught;
  }
}

/**
 * Normalize a jurisdiction name for de-duplication.
 *
 * @param {unknown} jurisdiction - Raw jurisdiction value.
 * @returns {string} Lowercased, trimmed name.
 */
function normalizeJurisdiction(jurisdiction) {
  return typeof jurisdiction === "string"
    ? jurisdiction.trim().toLowerCase()
    : "";
}

/**
 * Load jurisdiction names from a JSON file path or a comma-separated list.
 *
 * @param {string} source - Path to a JSON array file, or a comma-separated list.
 * @returns {Promise<readonly string[]>} Jurisdiction names.
 */
export async function loadJurisdictions(source) {
  if (source.includes(",") || !looksLikePath(source)) {
    return splitCommaList(source);
  }
  const raw = await readFile(source, "utf8");
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error(
      `Expected a JSON array of jurisdiction names in ${source}`,
    );
  }
  return parsed
    .map((entry) => (typeof entry === "string" ? entry.trim() : ""))
    .filter((entry) => entry.length > 0);
}

/**
 * Heuristic: does a string look like a file path rather than a single name?
 *
 * @param {string} source - Candidate value.
 * @returns {boolean} True when it looks like a path.
 */
function looksLikePath(source) {
  return (
    source.endsWith(".json") || source.includes("/") || source.includes("\\")
  );
}

/**
 * Split a comma-separated jurisdiction list into trimmed names.
 *
 * @param {string} source - Comma-separated list.
 * @returns {readonly string[]} Jurisdiction names.
 */
function splitCommaList(source) {
  return source
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

/**
 * Fetch a candidate portal URL with a browser-like User-Agent, redirect following,
 * and a hard timeout. Network failures resolve to an empty result rather than throw,
 * so one dead candidate never aborts discovery for a jurisdiction.
 *
 * @param {string} url - Candidate URL to fetch.
 * @returns {Promise<{ status: number | null, finalUrl: string, html: string }>} Response details.
 */
async function fetchPortal(url) {
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
    let html = "";
    try {
      html = await response.text();
    } catch {
      html = "";
    }
    return { status: response.status, finalUrl: response.url || url, html };
  } catch (caught) {
    console.error(
      `    candidate failed ${url}: ${caught instanceof Error ? caught.message : String(caught)}`,
    );
    return { status: null, finalUrl: url, html: "" };
  } finally {
    clearTimeout(timeout);
  }
}

/**
 * Run an async mapper over items with bounded concurrency, preserving order.
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
 * Parse CLI options.
 *
 * @param {readonly string[]} args - CLI args after the script name.
 * @returns {{ county: string, jurisdictions: string, out: string | undefined }} Parsed options.
 */
function parseCliOptions(args) {
  const { values } = parseArgs({
    args: [...args],
    options: {
      county: { type: "string" },
      jurisdictions: { type: "string" },
      out: { type: "string" },
    },
  });
  if (typeof values.county !== "string" || values.county.length === 0) {
    throw new Error("--county is required");
  }
  if (
    typeof values.jurisdictions !== "string" ||
    values.jurisdictions.length === 0
  ) {
    throw new Error(
      "--jurisdictions is required (path to a JSON array file or a comma-separated list)",
    );
  }
  return {
    county: values.county,
    jurisdictions: values.jurisdictions,
    out: typeof values.out === "string" ? values.out : undefined,
  };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((caught) => {
    console.error(caught instanceof Error ? caught.stack : String(caught));
    process.exitCode = 1;
  });
}
