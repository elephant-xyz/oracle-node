#!/usr/bin/env node
// @ts-check

/**
 * HTTP certify Pinellas Park Tyler Portico launcher vs EnerGov CSS search.
 *
 * Portico tile 5996 is "Apply for a Permit" (new application), not a search API.
 * Public search is city EnerGov CSS; Civic Access API is not on tylerportico.com.
 * Does not launch Chrome.
 */

import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

export const PINELLAS_PARK_PORTICO_NAVIGATOR_URL =
  "https://pinellasparkfl.tylerportico.com/navigator/public/selections/navigator?parentId=5996";
export const PINELLAS_PARK_CIVIC_ACCESS_INTEGRATION_URL =
  "https://pinellasparkfl.tylerportico.com/navigator/public/api/integrations/civic-access-integration";
export const PINELLAS_PARK_SELECTIONS_URL =
  "https://pinellasparkfl.tylerportico.com/navigator/public/api/selections";
export const PINELLAS_PARK_ENERGOV_CSS_URL =
  "https://egcss.pinellas-park.com/energov_prod/selfservice";

/**
 * @typedef {object} HttpProbeRow
 * @property {string} key Stable probe key.
 * @property {string} url Requested URL.
 * @property {number | null} httpStatus HTTP status or null on network error.
 * @property {number} elapsedMs Wall time.
 * @property {string | null} error Error message.
 * @property {unknown} json Parsed JSON when Content-Type is JSON.
 */

/**
 * @param {string} url HTTPS URL.
 * @returns {Promise<HttpProbeRow>} Status and optional JSON body.
 */
export async function probeJsonOrHtml(url) {
  const started = Date.now();
  try {
    const response = await fetch(url, {
      headers: {
        "User-Agent": USER_AGENT,
        Accept: "application/json,text/html,*/*",
      },
      redirect: "follow",
      signal: AbortSignal.timeout(25_000),
    });
    const contentType = response.headers.get("content-type") ?? "";
    /** @type {unknown} */
    let json = null;
    if (contentType.includes("application/json")) {
      json = await response.json();
    } else {
      await response.arrayBuffer();
    }
    return {
      key: "",
      url,
      httpStatus: response.status,
      elapsedMs: Date.now() - started,
      error: null,
      json,
    };
  } catch (error) {
    return {
      key: "",
      url,
      httpStatus: null,
      elapsedMs: Date.now() - started,
      error: error instanceof Error ? error.message : String(error),
      json: null,
    };
  }
}

/**
 * @param {unknown} payload Civic-access-integration JSON.
 * @returns {{ isCivicAccess: boolean, energovCssUrl: string | null, integrationTypeName: string | null }}
 *   Parsed integration.
 */
export function parseParkCivicAccessIntegration(payload) {
  if (
    payload === null ||
    typeof payload !== "object" ||
    Array.isArray(payload)
  ) {
    return {
      isCivicAccess: false,
      energovCssUrl: null,
      integrationTypeName: null,
    };
  }
  const record = /** @type {Record<string, unknown>} */ (payload);
  const url = typeof record.url === "string" ? record.url : null;
  return {
    isCivicAccess: record.isCivicAccess === true,
    energovCssUrl: url,
    integrationTypeName:
      typeof record.integrationTypeName === "string"
        ? record.integrationTypeName
        : null,
  };
}

/**
 * @param {unknown} payload Selections list JSON.
 * @returns {{ applyPermitTitle: string | null, dashboardUrl: string | null }}
 *   Launcher facts.
 */
export function parseParkSelections(payload) {
  if (Array.isArray(payload) === false) {
    return { applyPermitTitle: null, dashboardUrl: null };
  }
  /** @type {string | null} */
  let applyPermitTitle = null;
  /** @type {string | null} */
  let dashboardUrl = null;
  for (const item of payload) {
    if (item === null || typeof item !== "object" || Array.isArray(item))
      continue;
    const row = /** @type {Record<string, unknown>} */ (item);
    if (row.id === 5996 && typeof row.title === "string") {
      applyPermitTitle = row.title;
    }
    if (
      row.id === 5994 &&
      typeof row.destinationUrlAuthenticated === "string"
    ) {
      dashboardUrl = row.destinationUrlAuthenticated;
    }
  }
  return { applyPermitTitle, dashboardUrl };
}

/**
 * @returns {Promise<object>} Probe report.
 */
export async function probePinellasParkPortico() {
  const navigator = await probeJsonOrHtml(PINELLAS_PARK_PORTICO_NAVIGATOR_URL);
  const integration = await probeJsonOrHtml(
    PINELLAS_PARK_CIVIC_ACCESS_INTEGRATION_URL,
  );
  const selections = await probeJsonOrHtml(PINELLAS_PARK_SELECTIONS_URL);
  const css = await probeJsonOrHtml(PINELLAS_PARK_ENERGOV_CSS_URL);
  const parsedIntegration = parseParkCivicAccessIntegration(integration.json);
  const parsedSelections = parseParkSelections(selections.json);
  const civicAccessApiConfirmed = parsedIntegration.isCivicAccess === true;
  const energovCssHttpOk = css.httpStatus === 200;
  return {
    event: "pinellas_park_portico_probe",
    civicAccessApiConfirmed,
    porticoIsApplyLauncher:
      parsedSelections.applyPermitTitle === "Apply for a Permit",
    energovCssUrl:
      parsedIntegration.energovCssUrl ?? PINELLAS_PARK_ENERGOV_CSS_URL,
    energovCssHttpOk,
    dashboardUrl: parsedSelections.dashboardUrl,
    integrationTypeName: parsedIntegration.integrationTypeName,
    harvestMode: energovCssHttpOk
      ? "Tyler EnerGov CSS via scripts/probe-pinellas-tyler-civic-access.mjs --agency park (Chrome tenant bootstrap)"
      : "blocked: EnerGov CSS homepage not HTTP 200",
    rows: {
      navigator: { ...navigator, json: null },
      integration: { ...integration, json: parsedIntegration },
      selections: {
        httpStatus: selections.httpStatus,
        elapsedMs: selections.elapsedMs,
        error: selections.error,
        applyPermitTitle: parsedSelections.applyPermitTitle,
        dashboardUrl: parsedSelections.dashboardUrl,
      },
      energovCss: { ...css, json: null },
    },
    probedAt: new Date().toISOString(),
  };
}

/**
 * @returns {Promise<void>}
 */
async function main() {
  const repoRoot = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
  const outDir = path.join(repoRoot, "downloads/pinellas/permits");
  await mkdir(outDir, { recursive: true });
  const report = await probePinellasParkPortico();
  const outPath = path.join(outDir, "pinellas-park-portico-probe.json");
  await writeFile(outPath, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify(report, null, 2));
  console.log(outPath);
}

function isInvokedDirectly() {
  const entry = process.argv[1];
  if (entry === undefined) return false;
  try {
    return import.meta.url === pathToFileURL(entry).href;
  } catch {
    return false;
  }
}

if (isInvokedDirectly()) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
