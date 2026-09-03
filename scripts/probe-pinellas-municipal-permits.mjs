#!/usr/bin/env node

/**
 * HTTP probe of Pinellas municipal permit homepages.
 *
 * Does not launch Chrome (county Accela harvest may already own a browser).
 * Writes a gitignored JSON report under downloads/pinellas/permits/.
 */

import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { PINELLAS_MUNICIPAL_PERMIT_SOURCES } from "./pinellas/municipal-sources.mjs";

const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

/**
 * @typedef {object} MunicipalHttpProbeRow
 * @property {string} key Source key.
 * @property {string} jurisdiction City name.
 * @property {string} vendor Adapter family.
 * @property {string} probeUrl Requested URL.
 * @property {number | null} httpStatus HTTP status, or null on network error.
 * @property {number} elapsedMs Wall time.
 * @property {string | null} finalUrl Response URL after redirects.
 * @property {string | null} error Error message when the request failed.
 */

/**
 * @param {string} url HTTPS URL.
 * @returns {Promise<MunicipalHttpProbeRow & { key: string }>} Timing and status.
 */
export async function probeMunicipalUrl(url) {
  const started = Date.now();
  const response = await fetch(url, {
    headers: { "User-Agent": USER_AGENT, Accept: "text/html,*/*" },
    redirect: "follow",
    signal: AbortSignal.timeout(25000),
  });
  return {
    key: "",
    jurisdiction: "",
    vendor: "",
    probeUrl: url,
    httpStatus: response.status,
    elapsedMs: Date.now() - started,
    finalUrl: response.url,
    error: null,
  };
}

/**
 * @returns {Promise<MunicipalHttpProbeRow[]>} One row per catalog source.
 */
export async function probePinellasMunicipalHomepages() {
  /** @type {MunicipalHttpProbeRow[]} */
  const rows = [];
  for (const source of PINELLAS_MUNICIPAL_PERMIT_SOURCES) {
    try {
      const probed = await probeMunicipalUrl(source.probeUrl);
      rows.push({
        ...probed,
        key: source.key,
        jurisdiction: source.jurisdiction,
        vendor: source.vendor,
      });
    } catch (error) {
      rows.push({
        key: source.key,
        jurisdiction: source.jurisdiction,
        vendor: source.vendor,
        probeUrl: source.probeUrl,
        httpStatus: null,
        elapsedMs: 0,
        finalUrl: null,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }
  return rows;
}

/**
 * @returns {Promise<void>}
 */
async function main() {
  const repoRoot = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
  const outDir = path.join(repoRoot, "downloads/pinellas/permits");
  await mkdir(outDir, { recursive: true });
  const rows = await probePinellasMunicipalHomepages();
  const report = {
    event: "pinellas_municipal_http_probe",
    probedAt: new Date().toISOString(),
    rows,
  };
  const outPath = path.join(outDir, "municipal-http-probe.json");
  await writeFile(outPath, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify(report));
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
