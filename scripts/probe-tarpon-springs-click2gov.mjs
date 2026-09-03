#!/usr/bin/env node
// @ts-check

/**
 * HTTP probe of Tarpon Springs Click2Gov (no Chrome).
 *
 * 1–2 address lookups, delay ≥1s. Street number is required.
 */

import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import {
  createClick2GovHttpSession,
  searchClick2GovByAddress,
} from "./permit-source-adapters/click2gov-http.mjs";
import {
  TARPON_CLICK2GOV_CONFIG,
  TARPON_DEFAULT_PROBE_QUERIES,
} from "./pinellas/tarpon-click2gov.mjs";

/**
 * @typedef {import("./permit-source-adapters/click2gov-http.mjs").Click2GovAddressQuery} Click2GovAddressQuery
 */

/**
 * @typedef {object} TarponProbeCli
 * @property {readonly Click2GovAddressQuery[]} queries Address lookups.
 * @property {number} delayMs Delay between lookups.
 */

/**
 * @param {readonly string[]} args Args after the script name.
 * @returns {TarponProbeCli} Parsed flags.
 */
export function parseTarponProbeOptions(args) {
  /** @type {Click2GovAddressQuery[]} */
  const queries = [];
  let delayMs = 1_500;
  for (let index = 0; index < args.length; index += 1) {
    const token = args[index];
    if (token === "--street-number") {
      const number = args[index + 1];
      const nameFlag = args[index + 2];
      const name = args[index + 3];
      if (
        number === undefined ||
        nameFlag !== "--street-name" ||
        name === undefined
      ) {
        throw new Error(
          "--street-number must be followed by --street-name <name>",
        );
      }
      queries.push({ streetNumber: number, streetName: name });
      index += 3;
      continue;
    }
    if (token === "--delay-ms") {
      const parsed = Number.parseInt(args[index + 1] ?? "", 10);
      if (!Number.isInteger(parsed) || parsed < 1_000) {
        throw new Error("--delay-ms must be an integer of at least 1000");
      }
      delayMs = parsed;
      index += 1;
      continue;
    }
    if (token !== undefined && token.startsWith("--")) {
      throw new Error(`Unknown option: ${token}`);
    }
  }
  const resolved =
    queries.length > 0 ? queries : TARPON_DEFAULT_PROBE_QUERIES.slice(0, 2);
  if (resolved.length > 10) {
    throw new Error("Refusing more than 10 Tarpon Click2Gov probe lookups");
  }
  return { queries: resolved, delayMs };
}

/**
 * @param {number} milliseconds Delay.
 * @returns {Promise<void>}
 */
function delay(milliseconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

/**
 * @returns {Promise<void>}
 */
export async function main() {
  const options = parseTarponProbeOptions(process.argv.slice(2));
  const repoRoot = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
  const today = new Date().toISOString().slice(0, 10).replaceAll("-", "");
  const outDir = path.join(
    repoRoot,
    "downloads/pinellas/permits",
    `tarpon-click2gov-probe-${today}`,
  );
  await mkdir(outDir, { recursive: true });
  /** @type {{ streetNumber: string, streetName: string, classification: string, rowCount: number, applicationNumbers: string[], elapsedMs: number }[]} */
  const lookups = [];
  for (const [index, query] of options.queries.entries()) {
    if (index > 0) await delay(options.delayMs);
    const session = await createClick2GovHttpSession(
      TARPON_CLICK2GOV_CONFIG.origin,
    );
    const started = Date.now();
    const searched = await searchClick2GovByAddress({
      origin: TARPON_CLICK2GOV_CONFIG.origin,
      session,
      query,
    });
    lookups.push({
      streetNumber: query.streetNumber,
      streetName: query.streetName,
      classification: searched.classification,
      rowCount: searched.rows.length,
      applicationNumbers: searched.rows.map((row) => row.applicationNumber),
      elapsedMs: Date.now() - started,
    });
  }
  const report = {
    event: "tarpon_springs_click2gov_probe",
    origin: TARPON_CLICK2GOV_CONFIG.origin,
    sourceStamp: TARPON_CLICK2GOV_CONFIG.sourceStamp,
    certified: lookups.some(
      (lookup) => lookup.classification === "ok" && Number(lookup.rowCount) > 0,
    ),
    lookups,
    probedAt: new Date().toISOString(),
  };
  const reportPath = path.join(outDir, "probe-report.json");
  await writeFile(reportPath, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify(report, null, 2));
  console.log(reportPath);
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
