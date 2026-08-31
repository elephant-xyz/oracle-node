#!/usr/bin/env node

import { createReadStream } from "node:fs";
import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

import {
  isJsonObject,
  readOptionalJsonObject,
} from "../polk-local-parity-lib.mjs";

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * @typedef {"roofing" | "hvac" | "solar"} PolkBbbTrade
 */

/**
 * @typedef {object} PolkBbbTradeSource
 * @property {PolkBbbTrade} key Stable trade key.
 * @property {string} name Human-readable trade name.
 * @property {string} categoryUrl Verified BBB Lakeland category URL.
 */

/**
 * Verified multi-trade BBB category sources centered on Lakeland. These are
 * discovery inputs only; a profile is never treated as a Polk contractor until
 * it exact-matches licence evidence from a certified permit detail.
 *
 * @type {readonly PolkBbbTradeSource[]}
 */
export const POLK_BBB_TRADE_SOURCES = Object.freeze([
  {
    key: "roofing",
    name: "Roofing Contractors",
    categoryUrl:
      "https://www.bbb.org/us/fl/lakeland/category/roofing-contractors",
  },
  {
    key: "hvac",
    name: "Heating and Air Conditioning",
    categoryUrl:
      "https://www.bbb.org/us/fl/lakeland/category/heating-and-air-conditioning",
  },
  {
    key: "solar",
    name: "Solar Energy Contractors",
    categoryUrl:
      "https://www.bbb.org/us/fl/lakeland/category/solar-energy-contractors",
  },
]);

/**
 * @typedef {object} PermitContractorEvidence
 * @property {string} permitNumber Permit number.
 * @property {string} sourceKey Certified permit source key.
 * @property {string} sourceUrl Public permit detail URL.
 * @property {string} licenseNumber Normalized licence identifier.
 * @property {string | null} businessName Permit contractor business name.
 */

/**
 * @typedef {object} BbbProfileEvidence
 * @property {string} profileKey Stable BBB profile key.
 * @property {string | null} profileUrl BBB profile URL.
 * @property {string | null} name Business name.
 * @property {string | null} bbbRating BBB rating.
 * @property {boolean | null} accredited Accreditation flag.
 * @property {string[]} licenseNumbers Normalized licence identifiers parsed from BBB evidence.
 * @property {string[]} trades Trade harvests where the profile appeared.
 */

/**
 * Normalize a contractor licence identifier.
 *
 * @param {unknown} value Raw licence.
 * @returns {string | null} Uppercase alphanumeric licence or null.
 */
export function normalizeContractorLicense(value) {
  if (typeof value !== "string") return null;
  const normalized = value.toUpperCase().replace(/[^A-Z0-9]/g, "");
  return /^[A-Z]{2,4}\d{5,10}$/.test(normalized) ? normalized : null;
}

/**
 * Parse licence identifiers from BBB licensing rows.
 *
 * @param {unknown} value BBB `licenses` field.
 * @returns {string[]} Unique normalized licences.
 */
export function extractBbbLicenseNumbers(value) {
  if (!Array.isArray(value)) return [];
  const licenses = new Set();
  for (const row of value) {
    const text =
      typeof row === "string"
        ? row
        : isJsonObject(row)
          ? [row.licenseNumber, row.license_number, row.rawText, row.raw_text]
              .filter((part) => typeof part === "string")
              .join(" ")
          : "";
    for (const match of text.matchAll(/\b([A-Z]{2,4}[- :]*\d{5,10})\b/gi)) {
      const license = normalizeContractorLicense(match[1]);
      if (license !== null) licenses.add(license);
    }
  }
  return [...licenses].sort();
}

/**
 * Read a non-empty string from candidate keys.
 *
 * @param {JsonObject} record Source record.
 * @param {readonly string[]} keys Candidate keys.
 * @returns {string | null} First text value.
 */
function firstText(record, keys) {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim();
    }
  }
  return null;
}

/**
 * Promote only certified permit contractor/licence evidence.
 *
 * Records without all of permit number, certified source key, public source
 * URL, and licence are excluded. Bulk permit descriptions alone never open the
 * BBB matching gate.
 *
 * @param {unknown} value Enriched permit record.
 * @returns {PermitContractorEvidence | null} Strong evidence or null.
 */
export function readPermitContractorEvidence(value) {
  if (!isJsonObject(value)) return null;
  const detail = isJsonObject(value.detail) ? value.detail : value;
  const contractor = isJsonObject(detail.contractor)
    ? detail.contractor
    : isJsonObject(value.contractor)
      ? value.contractor
      : null;
  if (contractor === null) return null;
  const permitNumber = firstText(value, ["permitNumber", "permit_number"]);
  const sourceKey = firstText(value, ["sourceKey", "source_system"]);
  const sourceUrl = firstText(value, ["sourceUrl", "source_url"]);
  const licenseNumber = normalizeContractorLicense(
    contractor.licenseNumber ?? contractor.license_number,
  );
  const status = firstText(value, ["status", "enrichment_status"]);
  if (
    permitNumber === null ||
    sourceKey === null ||
    sourceUrl === null ||
    licenseNumber === null ||
    (status !== null && status !== "enriched")
  ) {
    return null;
  }
  return {
    permitNumber,
    sourceKey,
    sourceUrl,
    licenseNumber,
    businessName: firstText(contractor, [
      "businessName",
      "business_name",
      "name",
    ]),
  };
}

/**
 * Read JSONL objects from one file.
 *
 * @param {string} filePath JSONL path.
 * @returns {Promise<JsonObject[]>} Parsed object rows.
 */
async function readJsonlObjects(filePath) {
  const reader = createInterface({
    input: createReadStream(filePath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });
  /** @type {JsonObject[]} */
  const records = [];
  for await (const line of reader) {
    if (line.trim().length === 0) continue;
    const value = /** @type {unknown} */ (JSON.parse(line));
    if (isJsonObject(value)) records.push(value);
  }
  return records;
}

/**
 * Discover profile JSONL parts under one trade harvest directory.
 *
 * @param {string} tradeDirectory Trade root.
 * @returns {Promise<string[]>} Sorted profile part paths.
 */
async function profileFiles(tradeDirectory) {
  const directory = path.join(tradeDirectory, "profiles");
  try {
    return (await readdir(directory))
      .filter((name) => name.endsWith(".jsonl"))
      .sort()
      .map((name) => path.join(directory, name));
  } catch (caught) {
    if (
      caught instanceof Error &&
      "code" in caught &&
      /** @type {NodeJS.ErrnoException} */ (caught).code === "ENOENT"
    ) {
      return [];
    }
    throw caught;
  }
}

/**
 * Convert a BBB profile record to strong matching evidence.
 *
 * @param {JsonObject} profile Profile row.
 * @param {PolkBbbTrade} trade Harvest trade.
 * @returns {BbbProfileEvidence} Normalized profile.
 */
function toBbbProfileEvidence(profile, trade) {
  const profileUrl = firstText(profile, ["profileUrl", "url"]);
  const providerProfileId = firstText(profile, [
    "providerProfileId",
    "provider_profile_id",
  ]);
  const profileKey =
    providerProfileId ??
    profileUrl ??
    `profile:${firstText(profile, ["name", "legalName"]) ?? "unknown"}`;
  return {
    profileKey,
    profileUrl,
    name: firstText(profile, ["name", "businessName", "legalName"]),
    bbbRating: firstText(profile, ["bbbRating", "rating"]),
    accredited:
      typeof profile.accredited === "boolean"
        ? profile.accredited
        : typeof profile.isAccredited === "boolean"
          ? profile.isAccredited
          : null,
    licenseNumbers: extractBbbLicenseNumbers(profile.licenses),
    trades: [trade],
  };
}

/**
 * Merge a profile observed in multiple category harvests.
 *
 * @param {BbbProfileEvidence} existing Existing evidence.
 * @param {BbbProfileEvidence} incoming New evidence.
 * @returns {BbbProfileEvidence} Merged profile.
 */
function mergeBbbProfileEvidence(existing, incoming) {
  return {
    profileKey: existing.profileKey,
    profileUrl: existing.profileUrl ?? incoming.profileUrl,
    name: existing.name ?? incoming.name,
    bbbRating: existing.bbbRating ?? incoming.bbbRating,
    accredited: existing.accredited ?? incoming.accredited,
    licenseNumbers: [
      ...new Set([...existing.licenseNumbers, ...incoming.licenseNumbers]),
    ].sort(),
    trades: [...new Set([...existing.trades, ...incoming.trades])].sort(),
  };
}

/**
 * Build a local multi-trade harvest plan without launching a browser.
 *
 * @param {string} outputRoot BBB output root.
 * @returns {JsonObject} Planned verified category inputs.
 */
export function buildPolkBbbHarvestPlan(outputRoot) {
  return {
    schemaVersion: "oracle-node.polk-bbb-harvest-plan.v1",
    county: "polk",
    locationBasis: "BBB Lakeland, FL category search",
    outputRoot: path.resolve(outputRoot),
    trades: POLK_BBB_TRADE_SOURCES.map((trade) => ({
      ...trade,
      outputDirectory: path.join(path.resolve(outputRoot), trade.key),
      command: `node scripts/harvest-bbb-category.mjs --category-url ${trade.categoryUrl} --output-dir ${path.join(path.resolve(outputRoot), trade.key)} --headless true --no-html`,
    })),
    complete: false,
    evidence:
      "This is a plan only. Completion requires uncapped harvest manifests and zero failed profiles for every configured trade.",
  };
}

/**
 * Run all verified Polk BBB category harvests locally.
 *
 * @param {{outputRoot:string,maxPages:number|null,maxProfiles:number|null,chromiumExecutablePath:string|null,headless:boolean}} options Browser and output options.
 * @returns {Promise<JsonObject>} Multi-trade harvest receipt.
 */
export async function harvestPolkBbbTrades(options) {
  const { harvestBbbCategory } = await import("../harvest-bbb-category.mjs");
  /** @type {JsonObject[]} */
  const trades = [];
  for (const trade of POLK_BBB_TRADE_SOURCES) {
    const outputDirectory = path.join(options.outputRoot, trade.key);
    await mkdir(outputDirectory, { recursive: true });
    const summary = await harvestBbbCategory({
      categoryUrl: trade.categoryUrl,
      outputLocation: { kind: "local", dir: outputDirectory },
      chromiumExecutablePath: options.chromiumExecutablePath,
      headless: options.headless,
      startPage: 1,
      maxPages: options.maxPages,
      maxProfiles: options.maxProfiles,
      partRecordLimit: 100,
      pageDelayMs: 3_000,
      profileDelayMs: 5_000,
      challengeAttempts: 5,
      challengeCheckIntervalMs: 3_000,
      challengeChecksPerAttempt: 12,
      navigationTimeoutMs: 90_000,
      includeHtml: false,
      profileSubpages: ["customer-reviews", "complaints", "more-info"],
    });
    trades.push({
      key: trade.key,
      categoryUrl: trade.categoryUrl,
      outputDirectory,
      summary,
      uncapped: options.maxPages === null && options.maxProfiles === null,
      complete:
        options.maxPages === null &&
        options.maxProfiles === null &&
        summary.profilesHarvested > 0 &&
        summary.profilesFailed === 0,
    });
  }
  const receipt = {
    schemaVersion: "oracle-node.polk-bbb-multi-trade-harvest.v1",
    generatedAt: new Date().toISOString(),
    county: "polk",
    trades,
    complete:
      trades.length === POLK_BBB_TRADE_SOURCES.length &&
      trades.every((trade) => trade.complete === true),
  };
  await mkdir(path.join(options.outputRoot, "manifest"), { recursive: true });
  await writeFile(
    path.join(options.outputRoot, "manifest", "summary.json"),
    `${JSON.stringify(receipt, null, 2)}\n`,
    "utf8",
  );
  return receipt;
}

/**
 * Load and validate one trade's harvest summary.
 *
 * @param {string} bbbRoot Multi-trade root.
 * @param {PolkBbbTradeSource} trade Trade registry row.
 * @returns {Promise<{trade:PolkBbbTrade,complete:boolean,profilesHarvested:number,profilesFailed:number,profileFiles:string[]}>} Trade evidence.
 */
async function readTradeEvidence(bbbRoot, trade) {
  const tradeDirectory = path.join(bbbRoot, trade.key);
  const summary = await readOptionalJsonObject(
    path.join(tradeDirectory, "manifest", "summary.json"),
  );
  const files = await profileFiles(tradeDirectory);
  const profilesHarvested =
    summary !== null && typeof summary.profilesHarvested === "number"
      ? summary.profilesHarvested
      : 0;
  const profilesFailed =
    summary !== null && typeof summary.profilesFailed === "number"
      ? summary.profilesFailed
      : 0;
  const categoryUrl =
    summary !== null && typeof summary.categoryUrl === "string"
      ? summary.categoryUrl
      : null;
  return {
    trade: trade.key,
    complete:
      categoryUrl === trade.categoryUrl &&
      profilesHarvested > 0 &&
      profilesFailed === 0 &&
      files.length > 0,
    profilesHarvested,
    profilesFailed,
    profileFiles: files,
  };
}

/**
 * Match permit-backed contractor licences to BBB profile licence evidence.
 *
 * No name-only, rating-default, or trade-category-only match is emitted.
 *
 * @param {{permitEvidencePath:string,bbbRoot:string,outputPath:string}} options Input and output paths.
 * @returns {Promise<JsonObject>} CRM evidence summary.
 */
export async function matchPolkPermitContractorsToBbb(options) {
  const permitRows = await readJsonlObjects(options.permitEvidencePath);
  const harvestReceipt = await readOptionalJsonObject(
    path.join(options.bbbRoot, "manifest", "summary.json"),
  );
  const contractorEvidence = permitRows.flatMap((row) => {
    const evidence = readPermitContractorEvidence(row);
    return evidence === null ? [] : [evidence];
  });
  const permitEvidenceByLicense = new Map();
  for (const evidence of contractorEvidence) {
    const existing = permitEvidenceByLicense.get(evidence.licenseNumber) ?? [];
    existing.push(evidence);
    permitEvidenceByLicense.set(evidence.licenseNumber, existing);
  }
  const tradeEvidence = await Promise.all(
    POLK_BBB_TRADE_SOURCES.map((trade) =>
      readTradeEvidence(options.bbbRoot, trade),
    ),
  );
  /** @type {Map<string, BbbProfileEvidence>} */
  const profilesByKey = new Map();
  for (const trade of tradeEvidence) {
    for (const filePath of trade.profileFiles) {
      for (const profile of await readJsonlObjects(filePath)) {
        const incoming = toBbbProfileEvidence(profile, trade.trade);
        const existing = profilesByKey.get(incoming.profileKey);
        profilesByKey.set(
          incoming.profileKey,
          existing === undefined
            ? incoming
            : mergeBbbProfileEvidence(existing, incoming),
        );
      }
    }
  }
  /** @type {JsonObject[]} */
  const matches = [];
  for (const profile of profilesByKey.values()) {
    for (const licenseNumber of profile.licenseNumbers) {
      const permits = permitEvidenceByLicense.get(licenseNumber) ?? [];
      if (permits.length === 0) continue;
      matches.push({
        matchMethod: "permit_license_exact",
        licenseNumber,
        permitEvidenceCount: permits.length,
        permitNumbers: permits.map(
          /** @param {PermitContractorEvidence} permit @returns {string} Permit number. */ (
            permit,
          ) => permit.permitNumber,
        ),
        permitSourceKeys: [
          ...new Set(
            permits.map(
              /** @param {PermitContractorEvidence} permit @returns {string} Source key. */ (
                permit,
              ) => permit.sourceKey,
            ),
          ),
        ],
        profile,
      });
    }
  }
  matches.sort((left, right) =>
    String(left.licenseNumber).localeCompare(String(right.licenseNumber)),
  );
  const matchedLicenses = new Set(
    matches.flatMap((match) =>
      typeof match.licenseNumber === "string" ? [match.licenseNumber] : [],
    ),
  );
  const harvestComplete =
    harvestReceipt?.schemaVersion ===
      "oracle-node.polk-bbb-multi-trade-harvest.v1" &&
    harvestReceipt.county === "polk" &&
    harvestReceipt.complete === true &&
    tradeEvidence.every((trade) => trade.complete);
  const actualPermitEvidenceGate = contractorEvidence.length > 0;
  const summary = {
    schemaVersion: "oracle-node.polk-bbb-contractor-crm.v1",
    generatedAt: new Date().toISOString(),
    county: "polk",
    gate: {
      actualPermitContractorLicenseEvidence: actualPermitEvidenceGate,
      permitEvidenceRecordCount: contractorEvidence.length,
      uniquePermitLicenseCount: permitEvidenceByLicense.size,
      matchMethodsAllowed: ["permit_license_exact"],
    },
    tradeEvidence,
    harvestReceipt: {
      file: path.join(options.bbbRoot, "manifest", "summary.json"),
      complete: harvestComplete,
    },
    harvestedProfileCount: profilesByKey.size,
    profilesWithLicenseEvidence: [...profilesByKey.values()].filter(
      (profile) => profile.licenseNumbers.length > 0,
    ).length,
    matchedContractorCount: matchedLicenses.size,
    matchedPermitCount: matches.reduce(
      (total, match) =>
        total +
        (typeof match.permitEvidenceCount === "number"
          ? match.permitEvidenceCount
          : 0),
      0,
    ),
    matches,
    complete: actualPermitEvidenceGate && harvestComplete,
    blocker: !actualPermitEvidenceGate
      ? "No certified permit detail record contains contractor licence evidence."
      : !harvestComplete
        ? "One or more configured BBB trade harvests is missing, capped, or has failed profiles."
        : null,
  };
  await mkdir(path.dirname(options.outputPath), { recursive: true });
  await writeFile(
    options.outputPath,
    `${JSON.stringify(summary, null, 2)}\n`,
    "utf8",
  );
  return summary;
}

/**
 * Run Polk BBB plan, harvest, or permit-backed match mode.
 *
 * @param {readonly string[]} argv CLI arguments.
 * @returns {Promise<JsonObject>} Plan or receipt.
 */
export async function runPolkBbbCli(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      mode: { type: "string" },
      "output-root": { type: "string" },
      "permit-evidence": { type: "string" },
      output: { type: "string" },
      "max-pages": { type: "string" },
      "max-profiles": { type: "string" },
      "chromium-executable-path": { type: "string" },
      headless: { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });
  const mode = typeof values.mode === "string" ? values.mode : "plan";
  const outputRoot =
    typeof values["output-root"] === "string"
      ? values["output-root"]
      : "tmp/polk/bbb";
  if (mode === "plan") return buildPolkBbbHarvestPlan(outputRoot);
  if (mode === "harvest") {
    const maxPages =
      typeof values["max-pages"] === "string"
        ? Number.parseInt(values["max-pages"], 10)
        : null;
    const maxProfiles =
      typeof values["max-profiles"] === "string"
        ? Number.parseInt(values["max-profiles"], 10)
        : null;
    if (
      (maxPages !== null &&
        (!Number.isSafeInteger(maxPages) || maxPages < 1)) ||
      (maxProfiles !== null &&
        (!Number.isSafeInteger(maxProfiles) || maxProfiles < 1))
    ) {
      throw new Error("--max-pages/--max-profiles must be positive integers");
    }
    return harvestPolkBbbTrades({
      outputRoot,
      maxPages,
      maxProfiles,
      chromiumExecutablePath:
        typeof values["chromium-executable-path"] === "string"
          ? values["chromium-executable-path"]
          : null,
      headless:
        typeof values.headless === "string"
          ? !/^(false|0|no)$/i.test(values.headless)
          : true,
    });
  }
  if (mode === "match") {
    return matchPolkPermitContractorsToBbb({
      permitEvidencePath:
        typeof values["permit-evidence"] === "string"
          ? values["permit-evidence"]
          : "tmp/polk/permits/enriched-permits.jsonl",
      bbbRoot: outputRoot,
      outputPath:
        typeof values.output === "string"
          ? values.output
          : "tmp/polk/bbb/manifest/contractor-crm.json",
    });
  }
  throw new Error("--mode must be plan, harvest, or match");
}

if (
  process.argv[1] !== undefined &&
  fileURLToPath(import.meta.url) === path.resolve(process.argv[1])
) {
  runPolkBbbCli(process.argv.slice(2))
    .then((summary) => {
      process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
    })
    .catch((caught) => {
      const message = caught instanceof Error ? caught.message : String(caught);
      process.stderr.write(
        `${JSON.stringify({ event: "polk_bbb_failed", error: message })}\n`,
      );
      process.exitCode = 1;
    });
}
