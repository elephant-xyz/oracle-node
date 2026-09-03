#!/usr/bin/env node

import { mkdir, stat, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

import { runExport, runValidate } from "../export-overture-places-table.mjs";
import {
  isJsonObject,
  readOptionalJsonObject,
  sha256File,
} from "../polk-local-parity-lib.mjs";

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * Durable Polk policy decision for public Overture business contacts.
 *
 * The approval applies only to Overture's public business phone and email
 * fields. It does not authorize publishing private property-owner data,
 * skipping source/licence validation, or performing an external upload without
 * validated Neon export evidence.
 */
export const POLK_OVERTURE_PII_POLICY = Object.freeze({
  decision: "publish_public_business_contacts",
  approved: true,
  approvedAt: "2026-08-31",
  fields: Object.freeze(["phones", "emails"]),
  scope: "Overture public business contact fields for Polk County places",
});

/**
 * @typedef {object} PolkOverturePublicationOptions
 * @property {string} extractSummaryPath Local extract summary.
 * @property {string} neonReceiptPath Read-only Neon reconciliation receipt.
 * @property {string} outputDirectory Places publication root.
 * @property {string} envFile Authorized Neon env file.
 * @property {string} release Pinned Overture release.
 * @property {boolean} executeExport Whether to perform read-only Neon export and local writes.
 * @property {string} receiptPath Orchestration receipt path.
 */

/**
 * Read a nested non-negative integer.
 *
 * @param {JsonObject | null} object Root object.
 * @param {readonly string[]} keys Nested keys.
 * @returns {number | null} Count or null.
 */
function nestedCount(object, keys) {
  let current = /** @type {unknown} */ (object);
  for (const key of keys) {
    if (!isJsonObject(current)) return null;
    current = current[key];
  }
  return Number.isSafeInteger(current) && Number(current) >= 0
    ? Number(current)
    : null;
}

/**
 * Read a nested boolean.
 *
 * @param {JsonObject | null} object Root object.
 * @param {readonly string[]} keys Nested keys.
 * @returns {boolean | null} Boolean or null.
 */
function nestedBoolean(object, keys) {
  let current = /** @type {unknown} */ (object);
  for (const key of keys) {
    if (!isJsonObject(current)) return null;
    current = current[key];
  }
  return typeof current === "boolean" ? current : null;
}

/**
 * Find the places track in a reconciliation receipt.
 *
 * @param {JsonObject | null} receipt Receipt.
 * @returns {JsonObject | null} Places track or null.
 */
function placesTrack(receipt) {
  if (receipt === null || !Array.isArray(receipt.tracks)) return null;
  return (
    receipt.tracks.find(
      (track) => isJsonObject(track) && track.source === "overture_places",
    ) ?? null
  );
}

/**
 * Build the Polk Overture Neon-export/publication handoff.
 *
 * The plan is complete only through local publication preparation. External
 * Filebase/IPFS/IPNS and catalog URL mutation always remain human-authorized.
 *
 * @param {PolkOverturePublicationOptions} options Orchestration paths.
 * @param {JsonObject | null} extract Local extract summary.
 * @param {JsonObject | null} neonReceipt Neon reconciliation receipt.
 * @returns {JsonObject} Fail-closed plan.
 */
export function buildPolkOverturePublicationPlan(
  options,
  extract,
  neonReceipt,
) {
  const clipCount = nestedCount(extract, ["clipCount"]);
  const jsonlCount = nestedCount(extract, ["jsonl", "recordCount"]);
  const extractReady =
    extract?.schemaVersion === "oracle-node.overture-places-extract.v1" &&
    extract?.county === "polk" &&
    extract?.overtureRelease === options.release &&
    extract?.mode === "extract" &&
    clipCount !== null &&
    clipCount > 0 &&
    jsonlCount === clipCount &&
    nestedBoolean(extract, ["licenceGate", "passed"]) === true &&
    nestedBoolean(extract, ["licenceGate", "osmPresent"]) === false;
  const neonPlacesTrack = placesTrack(neonReceipt);
  const neonReady =
    neonReceipt?.schemaVersion === "oracle-node.polk-neon-reconciliation.v1" &&
    neonReceipt?.county === "polk" &&
    neonPlacesTrack?.passed === true &&
    nestedCount(neonPlacesTrack, ["localCount"]) === clipCount &&
    nestedCount(neonPlacesTrack, ["neonCoverageCount"]) === clipCount;
  const outputParquet = path.join(
    options.outputDirectory,
    "polk",
    "places-table.parquet",
  );
  return {
    schemaVersion: "oracle-node.polk-overture-publication-plan.v1",
    generatedAt: new Date().toISOString(),
    county: "polk",
    release: options.release,
    extract: {
      summaryPath: path.resolve(options.extractSummaryPath),
      clipCount,
      jsonlCount,
      licenceGatePassed:
        nestedBoolean(extract, ["licenceGate", "passed"]) === true,
      osmPresent: nestedBoolean(extract, ["licenceGate", "osmPresent"]),
      ready: extractReady,
    },
    neon: {
      receiptPath: path.resolve(options.neonReceiptPath),
      coverageCount: nestedCount(neonPlacesTrack, ["neonCoverageCount"]),
      placesTrackPassed: neonPlacesTrack?.passed === true,
      ready: neonReady,
    },
    export: {
      source: "neon",
      outputDirectory: path.resolve(options.outputDirectory),
      parquetPath: path.resolve(outputParquet),
      command: `node scripts/export-overture-places-table.mjs --from-neon --env-file ${options.envFile} --county polk --release ${options.release} --out ${options.outputDirectory}`,
      validationCommand: `node scripts/validate-overture-places-table.mjs --from-neon --env-file ${options.envFile} --county polk --release ${options.release} --parquet ${outputParquet}`,
      status:
        extractReady && neonReady
          ? "ready_for_read_only_neon_export"
          : "blocked",
    },
    externalPublication: {
      status: "blocked_until_validated_neon_export",
      requiredBucket: "elephant-oracle-open-data-polk-places",
      requiredIpnsLabel: "oracle-open-data-polk-places",
      piiPolicy: POLK_OVERTURE_PII_POLICY,
      uploadCommand: `npm --prefix ../elephant-query-db run publish:places-table -- --county polk --artifact-dir ${options.outputDirectory} --env-file <authorized-filebase-env-file> --dry-run true`,
      reason:
        "The public-business-contact policy is approved, but this orchestrator never uploads or mutates IPNS before the Neon export and validation receipt passes.",
    },
    catalogHandoff: {
      status: "blocked_until_gateway_verified_places_url",
      placesTableUrl: "<verified-polk-places-ipns-url>",
      commandTemplate:
        "npm run catalog:update -- --county-key polk --county-name Polk --state-code FL --county-fips 12105 --query-table-url <existing-verified-polk-query-table-url> --dataset-coverage-url <existing-verified-polk-coverage-url> --places-table-url <verified-polk-places-ipns-url> --updated-at <verified-publication-timestamp>",
      mcpDiscoveryContract:
        "Elephant MCP reads placesTableUrl from the canonical published-county catalog; no property query-table map entry may point at a places artifact.",
    },
    ready: extractReady && neonReady,
    complete: false,
  };
}

/**
 * Build file digest evidence for a local artifact.
 *
 * @param {string} filePath Artifact path.
 * @returns {Promise<JsonObject>} Path, bytes, and SHA-256.
 */
async function artifactReceipt(filePath) {
  const info = await stat(filePath);
  return {
    path: path.resolve(filePath),
    sizeBytes: info.size,
    sha256: await sha256File(filePath),
  };
}

/**
 * Run Polk Overture publication preparation.
 *
 * @param {PolkOverturePublicationOptions} options Orchestration options.
 * @returns {Promise<JsonObject>} Plan or completed local receipt.
 */
export async function runPolkOverturePublication(options) {
  const [extract, neonReceipt] = await Promise.all([
    readOptionalJsonObject(options.extractSummaryPath),
    readOptionalJsonObject(options.neonReceiptPath),
  ]);
  const plan = buildPolkOverturePublicationPlan(options, extract, neonReceipt);
  let result = plan;
  if (options.executeExport) {
    if (plan.ready !== true) {
      throw new Error(
        "Polk Overture export is blocked until extract and Neon places receipts reconcile",
      );
    }
    const exportReport = await runExport([
      "--from-neon",
      "--env-file",
      options.envFile,
      "--county",
      "polk",
      "--release",
      options.release,
      "--out",
      options.outputDirectory,
    ]);
    const parquetPath =
      typeof exportReport.parquetPath === "string"
        ? exportReport.parquetPath
        : "";
    const validationReport = await runValidate([
      "--from-neon",
      "--env-file",
      options.envFile,
      "--county",
      "polk",
      "--release",
      options.release,
      "--parquet",
      parquetPath,
    ]);
    const clipCount = nestedCount(extract, ["clipCount"]);
    const exportRowCount =
      typeof exportReport.rowCount === "number" ? exportReport.rowCount : null;
    const validationPassed =
      isJsonObject(validationReport.validation) &&
      validationReport.validation.passed === true;
    const reconciled =
      clipCount !== null &&
      exportRowCount === clipCount &&
      validationReport.parquetCount === clipCount &&
      validationReport.businessLocationRowCount === clipCount &&
      validationPassed;
    if (!reconciled) {
      throw new Error(
        "Polk Overture local publication export did not reconcile to the extract and Neon counts",
      );
    }
    const indexPath =
      typeof exportReport.indexPath === "string" ? exportReport.indexPath : "";
    const noticePath =
      typeof exportReport.noticePath === "string"
        ? exportReport.noticePath
        : "";
    result = {
      schemaVersion: "oracle-node.polk-overture-publication-receipt.v1",
      generatedAt: new Date().toISOString(),
      county: "polk",
      release: options.release,
      rowCount: exportRowCount,
      extractClipCount: clipCount,
      neonBusinessLocationCount: validationReport.businessLocationRowCount,
      validation: validationReport.validation,
      artifacts: {
        parquet: await artifactReceipt(parquetPath),
        index: await artifactReceipt(indexPath),
        notice: await artifactReceipt(noticePath),
      },
      sourceLicenceGate: extract?.licenceGate ?? null,
      localPublicationReady: true,
      externalPublication: {
        ...plan.externalPublication,
        status: "ready_for_authorized_filebase_dry_run",
        reason:
          "Neon export and validation reconcile, and Polk public business phone/email publication was approved on 2026-08-31. Run the generated dry-run command before any upload.",
      },
      catalogHandoff: plan.catalogHandoff,
      complete: true,
    };
  }
  await mkdir(path.dirname(options.receiptPath), { recursive: true });
  await writeFile(
    options.receiptPath,
    `${JSON.stringify(result, null, 2)}\n`,
    "utf8",
  );
  return result;
}

/**
 * Parse CLI options.
 *
 * @param {readonly string[]} argv CLI arguments.
 * @returns {PolkOverturePublicationOptions} Options.
 */
export function parsePolkOverturePublicationOptions(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      "extract-summary": { type: "string" },
      "neon-receipt": { type: "string" },
      "output-dir": { type: "string" },
      "env-file": { type: "string" },
      release: { type: "string" },
      "execute-export": { type: "boolean" },
      receipt: { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });
  const release =
    typeof values.release === "string" ? values.release : "2026-08-19.0";
  return {
    extractSummaryPath:
      typeof values["extract-summary"] === "string"
        ? values["extract-summary"]
        : `tmp/polk/overture/${release}/extract/manifest/summary.json`,
    neonReceiptPath:
      typeof values["neon-receipt"] === "string"
        ? values["neon-receipt"]
        : "tmp/polk/neon/reconciliation-receipt.json",
    outputDirectory:
      typeof values["output-dir"] === "string"
        ? values["output-dir"]
        : `tmp/polk/overture/${release}/publication`,
    envFile:
      typeof values["env-file"] === "string"
        ? values["env-file"]
        : "../elephant-query-db/.env.local",
    release,
    executeExport: values["execute-export"] === true,
    receiptPath:
      typeof values.receipt === "string"
        ? values.receipt
        : `tmp/polk/overture/${release}/publication-receipt.json`,
  };
}

if (
  process.argv[1] !== undefined &&
  fileURLToPath(import.meta.url) === path.resolve(process.argv[1])
) {
  runPolkOverturePublication(
    parsePolkOverturePublicationOptions(process.argv.slice(2)),
  )
    .then((result) => {
      process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    })
    .catch((caught) => {
      const message = caught instanceof Error ? caught.message : String(caught);
      process.stderr.write(
        `${JSON.stringify({ event: "polk_overture_publication_failed", error: message })}\n`,
      );
      process.exitCode = 1;
    });
}
