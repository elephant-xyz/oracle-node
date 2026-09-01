#!/usr/bin/env node

import { createWriteStream } from "node:fs";
import {
  link,
  mkdir,
  readFile,
  readdir,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { createRequire } from "node:module";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

import {
  isJsonObject,
  readOptionalJsonObject,
  sha256File,
} from "./polk-local-parity-lib.mjs";

const require = createRequire(import.meta.url);
const ipfsHash = require("ipfs-only-hash");

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * @typedef {object} PolkPublicationCliOptions
 * @property {string} sourceDirectory Completed local Polk export.
 * @property {string} outputDirectory Local family-separated staging root.
 * @property {string} [permitEnrichmentReceiptPath] Deep permit evidence receipt.
 * @property {string} [sunbizManifestPath] Sunbiz transform/match manifest.
 * @property {string} [bbbManifestPath] BBB contractor CRM receipt.
 * @property {string} [neonReceiptPath] Neon reconciliation receipt.
 * @property {string} [overturePublicationReceiptPath] Overture Neon-export receipt.
 * @property {number} shardSize Open-data index entries per shard.
 * @property {boolean} materialize Whether to create local hard-link staging.
 */

/**
 * @typedef {object} PolkPublicationEntry
 * @property {string} propertyId Stable UUID property id.
 * @property {string} parcelIdentifier Polk parcel identifier.
 * @property {string} file Source-relative property JSON path.
 * @property {number} fileSizeBytes Property JSON size.
 * @property {string} sha256 Property JSON SHA-256.
 * @property {string} cid Locally computed immutable JSON CID.
 */

/**
 * @typedef {object} PolkPublicationInventory
 * @property {number} propertyCount Inventory entry count.
 * @property {number} propertyBytes Inventory byte count.
 * @property {number} minimumPropertyBytes Minimum property size.
 * @property {number} maximumPropertyBytes Maximum property size.
 * @property {string[]} sourceManifestNames Sorted source shard manifests.
 */

/**
 * @typedef {object} PolkPublicationPlan
 * @property {string} schemaVersion Plan schema.
 * @property {string} generatedAt Generation timestamp.
 * @property {{key:string,name:string,stateCode:string,countyFips:string}} county County metadata.
 * @property {JsonObject} validation Local reconciliation evidence.
 * @property {JsonObject} families Family-separated local and external targets.
 * @property {JsonObject} catalogRegistration Canonical catalog handoff template.
 * @property {string[]} externalActions Human-authorized external actions still required.
 * @property {"dry_run" | "materialized"} status Local preparation status.
 */

/**
 * @typedef {object} PolkPublicCoverageOptions
 * @property {number} propertyCount Reconciled published property count.
 * @property {number} permitCount Reconciled permit rows nested in property payloads.
 * @property {string} exportedAt Export start timestamp.
 * @property {string} completedAt Export completion timestamp.
 * @property {string} openDataCid CID of the published open-data index.
 * @property {readonly JsonObject[]} [reconciledTracks] Optional passed Neon track receipts.
 */

/**
 * Parse local publication-preparation options.
 *
 * Dry-run is the default. `--materialize` must be explicit because it creates
 * hundreds of thousands of local hard links for a full Polk export.
 *
 * @param {readonly string[]} argv CLI arguments after the script path.
 * @param {string} [rootDirectory] Repository root for relative paths.
 * @returns {PolkPublicationCliOptions} Validated options.
 */
export function parsePolkPublicationCliOptions(
  argv,
  rootDirectory = process.cwd(),
) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      "source-dir": { type: "string" },
      "output-dir": { type: "string" },
      "permit-enrichment-receipt": { type: "string" },
      "sunbiz-manifest": { type: "string" },
      "bbb-manifest": { type: "string" },
      "neon-receipt": { type: "string" },
      "overture-publication-receipt": { type: "string" },
      "shard-size": { type: "string" },
      materialize: { type: "boolean" },
      "dry-run": { type: "boolean" },
    },
    strict: true,
    allowPositionals: false,
  });
  if (values.materialize === true && values["dry-run"] === true) {
    throw new Error("Use only one of --materialize or --dry-run");
  }
  const shardSize =
    typeof values["shard-size"] === "string"
      ? Number.parseInt(values["shard-size"], 10)
      : 10_000;
  if (!Number.isSafeInteger(shardSize) || shardSize < 1) {
    throw new Error("--shard-size must be a positive integer");
  }
  return {
    sourceDirectory: path.resolve(
      rootDirectory,
      typeof values["source-dir"] === "string"
        ? values["source-dir"]
        : "tmp/polk/full",
    ),
    outputDirectory: path.resolve(
      rootDirectory,
      typeof values["output-dir"] === "string"
        ? values["output-dir"]
        : "tmp/polk/publication-prepared",
    ),
    permitEnrichmentReceiptPath: path.resolve(
      rootDirectory,
      typeof values["permit-enrichment-receipt"] === "string"
        ? values["permit-enrichment-receipt"]
        : "tmp/polk/permits/enrichment-receipt.json",
    ),
    sunbizManifestPath: path.resolve(
      rootDirectory,
      typeof values["sunbiz-manifest"] === "string"
        ? values["sunbiz-manifest"]
        : "tmp/polk/sunbiz/transformed/manifest.json",
    ),
    bbbManifestPath: path.resolve(
      rootDirectory,
      typeof values["bbb-manifest"] === "string"
        ? values["bbb-manifest"]
        : "tmp/polk/bbb/manifest/contractor-crm.json",
    ),
    neonReceiptPath: path.resolve(
      rootDirectory,
      typeof values["neon-receipt"] === "string"
        ? values["neon-receipt"]
        : "tmp/polk/neon/reconciliation-receipt.json",
    ),
    overturePublicationReceiptPath: path.resolve(
      rootDirectory,
      typeof values["overture-publication-receipt"] === "string"
        ? values["overture-publication-receipt"]
        : "tmp/polk/overture/2026-08-19.0/publication-receipt.json",
    ),
    shardSize,
    materialize: values.materialize === true,
  };
}

/**
 * Read and validate a JSON object.
 *
 * @param {string} filePath Input JSON path.
 * @returns {Promise<JsonObject>} Parsed object.
 */
async function readJsonObject(filePath) {
  const value = /** @type {unknown} */ (
    JSON.parse(await readFile(filePath, "utf8"))
  );
  if (!isJsonObject(value)) {
    throw new Error(`Expected JSON object at ${filePath}`);
  }
  return value;
}

/**
 * Read a required non-empty string field.
 *
 * @param {JsonObject} object Source object.
 * @param {string} key Field name.
 * @param {string} context Diagnostic context.
 * @returns {string} Required text.
 */
function requiredText(object, key, context) {
  const value = object[key];
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${context}.${key} must be a non-empty string`);
  }
  return value;
}

/**
 * Read a required non-negative safe integer field.
 *
 * @param {JsonObject} object Source object.
 * @param {string} key Field name.
 * @param {string} context Diagnostic context.
 * @returns {number} Required count.
 */
function requiredCount(object, key, context) {
  const value = object[key];
  if (!Number.isSafeInteger(value) || Number(value) < 0) {
    throw new Error(`${context}.${key} must be a non-negative safe integer`);
  }
  return Number(value);
}

/**
 * Read a nested non-negative safe integer.
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
 * Validate and normalize one source manifest entry.
 *
 * @param {unknown} value Candidate entry.
 * @param {string} manifestName Source manifest name.
 * @returns {PolkPublicationEntry} Valid entry.
 */
export function parsePolkPublicationEntry(value, manifestName) {
  if (!isJsonObject(value)) {
    throw new Error(`Invalid entry in ${manifestName}`);
  }
  const entry = {
    propertyId: requiredText(value, "propertyId", manifestName),
    parcelIdentifier: requiredText(value, "parcelIdentifier", manifestName),
    file: requiredText(value, "file", manifestName),
    fileSizeBytes: requiredCount(value, "fileSizeBytes", manifestName),
    sha256: requiredText(value, "sha256", manifestName),
    cid: requiredText(value, "cid", manifestName),
  };
  if (
    !/^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
      entry.propertyId,
    )
  ) {
    throw new Error(`Invalid propertyId in ${manifestName}`);
  }
  if (!/^[A-Z0-9]+$/.test(entry.parcelIdentifier)) {
    throw new Error(`Invalid parcelIdentifier in ${manifestName}`);
  }
  if (!/^[0-9a-f]{64}$/.test(entry.sha256)) {
    throw new Error(`Invalid sha256 in ${manifestName}`);
  }
  if (!entry.cid.startsWith("Qm") && !entry.cid.startsWith("bafy")) {
    throw new Error(`Invalid CID in ${manifestName}`);
  }
  if (
    path.isAbsolute(entry.file) ||
    entry.file.split(path.sep).includes("..")
  ) {
    throw new Error(`Unsafe property path in ${manifestName}`);
  }
  return entry;
}

/**
 * Inventory every source shard manifest without reading property bodies.
 *
 * Duplicate property ids or parcel ids fail closed because the published
 * sharded index must have one unambiguous entry per property.
 *
 * @param {string} sourceDirectory Completed local export root.
 * @returns {Promise<PolkPublicationInventory>} Reconciled source inventory.
 */
export async function inventoryPolkPublicationSource(sourceDirectory) {
  const sourceManifestNames = (
    await readdir(path.join(sourceDirectory, "manifests"))
  )
    .filter((name) => /^shard-\d{6}\.json$/.test(name))
    .sort();
  if (sourceManifestNames.length === 0) {
    throw new Error("No Polk source shard manifests were found");
  }
  /** @type {Set<string>} */
  const propertyIds = new Set();
  /** @type {Set<string>} */
  const parcelIdentifiers = new Set();
  let propertyCount = 0;
  let propertyBytes = 0;
  let minimumPropertyBytes = Number.MAX_SAFE_INTEGER;
  let maximumPropertyBytes = 0;
  for (const manifestName of sourceManifestNames) {
    const manifest = await readJsonObject(
      path.join(sourceDirectory, "manifests", manifestName),
    );
    if (!Array.isArray(manifest.entries)) {
      throw new Error(`Missing entries in ${manifestName}`);
    }
    for (const rawEntry of manifest.entries) {
      const entry = parsePolkPublicationEntry(rawEntry, manifestName);
      if (propertyIds.has(entry.propertyId)) {
        throw new Error(`Duplicate property id ${entry.propertyId}`);
      }
      if (parcelIdentifiers.has(entry.parcelIdentifier)) {
        throw new Error(
          `Duplicate parcel identifier ${entry.parcelIdentifier}`,
        );
      }
      propertyIds.add(entry.propertyId);
      parcelIdentifiers.add(entry.parcelIdentifier);
      propertyCount += 1;
      propertyBytes += entry.fileSizeBytes;
      minimumPropertyBytes = Math.min(
        minimumPropertyBytes,
        entry.fileSizeBytes,
      );
      maximumPropertyBytes = Math.max(
        maximumPropertyBytes,
        entry.fileSizeBytes,
      );
    }
  }
  return {
    propertyCount,
    propertyBytes,
    minimumPropertyBytes,
    maximumPropertyBytes,
    sourceManifestNames,
  };
}

/**
 * Build a publication plan after validating local export cardinality, bytes,
 * query-table digest, checkpoint completion, and privacy evidence.
 *
 * @param {PolkPublicationCliOptions} options Local options.
 * @returns {Promise<{plan:PolkPublicationPlan,inventory:PolkPublicationInventory,manifest:JsonObject}>} Plan and reusable inventory.
 */
export async function buildPolkPublicationPlan(options) {
  const polkRoot = path.resolve(options.sourceDirectory, "..");
  const permitEnrichmentReceiptPath =
    options.permitEnrichmentReceiptPath ??
    path.join(polkRoot, "permits", "enrichment-receipt.json");
  const sunbizManifestPath =
    options.sunbizManifestPath ??
    path.join(polkRoot, "sunbiz", "transformed", "manifest.json");
  const bbbManifestPath =
    options.bbbManifestPath ??
    path.join(polkRoot, "bbb", "manifest", "contractor-crm.json");
  const neonReceiptPath =
    options.neonReceiptPath ??
    path.join(polkRoot, "neon", "reconciliation-receipt.json");
  const overturePublicationReceiptPath =
    options.overturePublicationReceiptPath ??
    path.join(polkRoot, "overture", "2026-08-19.0", "publication-receipt.json");
  const overtureSummaryPath = path.resolve(
    options.sourceDirectory,
    "..",
    "overture",
    "2026-08-19.0",
    "extract",
    "manifest",
    "summary.json",
  );
  const [
    manifest,
    coverage,
    checkpoint,
    inventory,
    overture,
    permitEnrichment,
    sunbiz,
    bbb,
    neonReceipt,
    overturePublication,
  ] = await Promise.all([
    readJsonObject(path.join(options.sourceDirectory, "manifest.json")),
    readJsonObject(path.join(options.sourceDirectory, "coverage.json")),
    readJsonObject(
      path.join(options.sourceDirectory, ".state", "checkpoint.json"),
    ),
    inventoryPolkPublicationSource(options.sourceDirectory),
    readOptionalJsonObject(overtureSummaryPath),
    readOptionalJsonObject(permitEnrichmentReceiptPath),
    readOptionalJsonObject(sunbizManifestPath),
    readOptionalJsonObject(bbbManifestPath),
    readOptionalJsonObject(neonReceiptPath),
    readOptionalJsonObject(overturePublicationReceiptPath),
  ]);
  if (manifest.county !== "polk" || coverage.county !== "polk") {
    throw new Error("Publication input must identify Polk County");
  }
  if (checkpoint.complete !== true) {
    throw new Error("Polk checkpoint is not complete");
  }
  const output = manifest.output;
  if (!isJsonObject(output)) {
    throw new Error("Polk manifest output is missing");
  }
  const validation = output.validation;
  const queryTable = output.queryTable;
  if (!isJsonObject(validation) || !isJsonObject(queryTable)) {
    throw new Error("Polk manifest validation/queryTable is missing");
  }
  const propertyCount = requiredCount(output, "propertyCount", "output");
  const propertyBytes = requiredCount(output, "propertyBytes", "output");
  const queryTableRows = requiredCount(queryTable, "rowCount", "queryTable");
  const queryTableBytes = requiredCount(queryTable, "sizeBytes", "queryTable");
  const queryTableExpectedHash = requiredText(
    queryTable,
    "sha256",
    "queryTable",
  );
  const queryTableFile = requiredText(queryTable, "file", "queryTable");
  const queryTablePath = path.join(options.sourceDirectory, queryTableFile);
  const [queryTableInfo, queryTableActualHash] = await Promise.all([
    stat(queryTablePath),
    sha256File(queryTablePath),
  ]);
  const privacy = coverage.privacy;
  if (!isJsonObject(privacy) || privacy.passed !== true) {
    throw new Error("Polk privacy gate is not recorded as passed");
  }
  const childRows = coverage.childRows;
  if (!isJsonObject(childRows)) {
    throw new Error("Polk source coverage child rows are missing");
  }
  const permitCount = requiredCount(childRows, "permits", "coverage.childRows");
  const countsMatch =
    propertyCount === inventory.propertyCount &&
    propertyCount === queryTableRows &&
    propertyCount === requiredCount(validation, "rowCount", "validation") &&
    propertyCount ===
      requiredCount(validation, "distinctParcels", "validation") &&
    propertyCount ===
      requiredCount(validation, "distinctPropertyIds", "validation");
  const bytesMatch =
    propertyBytes === inventory.propertyBytes &&
    queryTableBytes === queryTableInfo.size;
  const queryTableHashMatches = queryTableExpectedHash === queryTableActualHash;
  const privacyMatches =
    requiredCount(validation, "nullCids", "validation") === 0 &&
    requiredCount(validation, "ownerFieldViolations", "validation") === 0;
  const overtureLicenceGate =
    overture !== null && isJsonObject(overture.licenceGate)
      ? overture.licenceGate
      : null;
  const overtureClipCount =
    overture !== null &&
    typeof overture.clipCount === "number" &&
    Number.isSafeInteger(overture.clipCount)
      ? overture.clipCount
      : null;
  const overtureReady =
    overture?.county === "polk" &&
    overture.mode === "extract" &&
    overtureClipCount !== null &&
    overtureClipCount > 0 &&
    overtureLicenceGate?.passed === true;
  const permitEnrichmentReady =
    permitEnrichment?.schemaVersion ===
      "oracle-node.polk-permit-enrichment-receipt.v1" &&
    permitEnrichment.complete === true &&
    nestedCount(permitEnrichment, ["officialPermitCount"]) === permitCount &&
    nestedCount(permitEnrichment, ["unsupportedPermitCount"]) === 0;
  const sunbizReady =
    sunbiz?.schemaVersion === "oracle-node.polk-sunbiz-transform-match.v1" &&
    sunbiz.county === "polk" &&
    sunbiz.complete === true &&
    nestedCount(sunbiz, ["transformedRecordCount"]) !== null &&
    nestedCount(sunbiz, ["transformedRecordCount"]) > 0;
  const bbbReady =
    bbb?.schemaVersion === "oracle-node.polk-bbb-contractor-crm.v1" &&
    bbb.county === "polk" &&
    bbb.complete === true &&
    nestedBoolean(bbb, ["gate", "actualPermitContractorLicenseEvidence"]) ===
      true;
  const neonTracks =
    neonReceipt !== null && Array.isArray(neonReceipt.tracks)
      ? neonReceipt.tracks.filter(isJsonObject)
      : [];
  const neonReady =
    neonReceipt?.schemaVersion === "oracle-node.polk-neon-reconciliation.v1" &&
    neonReceipt.county === "polk" &&
    neonReceipt.complete === true &&
    neonTracks.length === 5 &&
    neonTracks.every((track) => track.passed === true);
  const overturePublicationReady =
    overturePublication?.schemaVersion ===
      "oracle-node.polk-overture-publication-receipt.v1" &&
    overturePublication.county === "polk" &&
    overturePublication.complete === true &&
    nestedCount(overturePublication, ["rowCount"]) === overtureClipCount &&
    nestedBoolean(overturePublication, ["validation", "passed"]) === true;
  if (
    !countsMatch ||
    !bytesMatch ||
    !queryTableHashMatches ||
    !privacyMatches
  ) {
    throw new Error(
      `Polk publication gate failed: ${JSON.stringify({
        countsMatch,
        bytesMatch,
        queryTableHashMatches,
        privacyMatches,
      })}`,
    );
  }
  const plan = {
    schemaVersion: "oracle-node.polk-publication-plan.v1",
    generatedAt: new Date().toISOString(),
    county: {
      key: "polk",
      name: "Polk",
      stateCode: "FL",
      countyFips: "12105",
    },
    validation: {
      passed: true,
      propertyCount,
      propertyBytes,
      permitCount,
      sourceManifestCount: inventory.sourceManifestNames.length,
      queryTable: {
        file: queryTableFile,
        rowCount: queryTableRows,
        sizeBytes: queryTableInfo.size,
        sha256: queryTableActualHash,
        nullCids: 0,
        ownerFieldViolations: 0,
      },
      privacyPassed: true,
      checkpointComplete: true,
      overture: {
        summaryPath: overtureSummaryPath,
        localExtractReady: overtureReady,
        clipCount: overtureClipCount,
        licenceGatePassed: overtureLicenceGate?.passed === true,
      },
      enrichments: {
        permitEnrichment: {
          receiptPath: permitEnrichmentReceiptPath,
          ready: permitEnrichmentReady,
          licenseEvidenceCount: nestedCount(permitEnrichment, [
            "licenseEvidenceCount",
          ]),
        },
        sunbiz: {
          manifestPath: sunbizManifestPath,
          ready: sunbizReady,
          transformedRecordCount: nestedCount(sunbiz, [
            "transformedRecordCount",
          ]),
          matchedPropertyCount: nestedCount(sunbiz, [
            "propertyMatching",
            "matchedPropertyCount",
          ]),
        },
        bbb: {
          manifestPath: bbbManifestPath,
          ready: bbbReady,
          harvestedProfileCount: nestedCount(bbb, ["harvestedProfileCount"]),
          matchedContractorCount: nestedCount(bbb, ["matchedContractorCount"]),
        },
        neon: {
          receiptPath: neonReceiptPath,
          ready: neonReady,
          tracks: neonTracks,
        },
      },
    },
    families: {
      openData: {
        localDirectory: path.join(options.outputDirectory, "open-data"),
        requiredBucket: "elephant-oracle-open-data-polk",
        requiredIpnsLabel: "oracle-open-data-polk",
        externalStatus: "awaiting_human_approval",
      },
      queryTable: {
        localFile: path.join(
          options.outputDirectory,
          "query-table",
          "query-table.parquet",
        ),
        requiredBucket: "elephant-oracle-query-table-polk",
        requiredIpnsLabel: "oracle-query-table-polk",
        externalStatus: "awaiting_human_approval",
      },
      datasetCoverage: {
        localFile: path.join(
          options.outputDirectory,
          "dataset-coverage",
          "dataset-coverage.json",
        ),
        requiredIpnsLabel: "oracle-dataset-coverage-polk",
        enrichmentStatus: neonReady
          ? "all_five_tracks_reconciled"
          : "property_and_permit_publication_only",
        externalStatus: "awaiting_human_approval",
      },
      places: {
        localExtractPath: overtureSummaryPath,
        localExtractStatus: overturePublicationReady
          ? "validated_neon_export_ready_for_human_review"
          : overtureReady
            ? "ready_for_neon_load"
            : "missing_or_unvalidated",
        localPlaceCount: overtureClipCount,
        localPublicationReceiptPath: overturePublicationReceiptPath,
        requiredBucket: "elephant-oracle-open-data-polk-places",
        requiredIpnsLabel: "oracle-open-data-polk-places",
        externalStatus: overturePublicationReady
          ? "awaiting_human_pii_approval"
          : "blocked_until_neon_load_reconciliation_and_publication_review",
      },
    },
    catalogRegistration: {
      status: "blocked_until_stable_public_urls_are_gateway_verified",
      countyEntryTemplate: {
        countyKey: "polk",
        countyName: "Polk",
        stateCode: "FL",
        countyFips: "12105",
        status: "published",
        queryTableUrl: "<verified-query-table-ipns-url>",
        datasetCoverageUrl: "<verified-coverage-ipns-url>",
        permitQueryTableUrl: null,
        placesTableUrl: overturePublicationReady
          ? "<verified-polk-places-ipns-url>"
          : null,
        updatedAt: "<verified-publication-timestamp>",
      },
      updateCommandTemplate: `npm run catalog:update -- --county-key polk --county-name Polk --state-code FL --county-fips 12105 --query-table-url <verified-query-table-ipns-url> --dataset-coverage-url <verified-coverage-ipns-url>${overturePublicationReady ? " --places-table-url <verified-polk-places-ipns-url>" : ""} --updated-at <verified-publication-timestamp>`,
      mcpMapTemplate: '{"polk":"<verified-query-table-ipns-url>"}',
    },
    externalActions: [
      "Obtain explicit human approval for public publication.",
      "Create or confirm Polk-specific Filebase buckets and IPNS labels for each artifact family.",
      "Upload locally staged artifacts and reconcile remote object counts and CIDs.",
      "Verify gateway query-table bytes begin with PAR1 and coverage identifies Polk.",
      "Register the stable verified URLs with catalog:update; do not hand-edit an unverified published entry.",
      "Merge the Polk URL into the existing MCP county map, redeploy the MCP, and run a Donphan smoke query.",
      overtureReady
        ? overturePublicationReady
          ? "Obtain explicit human PII approval, publish the validated Polk places family, gateway-verify it, and apply the catalog handoff."
          : "Load and reconcile the completed Polk Overture extract in Neon, export from Neon, repeat the licence gate, and obtain a Polk-specific PII publication decision."
        : "Run and validate the full Polk Overture extract before loading or separately publishing the places family.",
    ],
    parityEvidenceReady:
      permitEnrichmentReady &&
      sunbizReady &&
      bbbReady &&
      neonReady &&
      overturePublicationReady,
    status: /** @type {"dry_run" | "materialized"} */ (
      options.materialize ? "materialized" : "dry_run"
    ),
  };
  return { plan, inventory, manifest };
}

/**
 * Write text to a stream while honoring backpressure.
 *
 * @param {import("node:fs").WriteStream} stream Destination stream.
 * @param {string} text Text fragment.
 * @returns {Promise<void>} Resolves after the fragment is accepted.
 */
function writeStreamText(stream, text) {
  return new Promise((resolve, reject) => {
    const onError = (error) => {
      stream.off("drain", onDrain);
      reject(error);
    };
    const onDrain = () => {
      stream.off("error", onError);
      resolve();
    };
    stream.once("error", onError);
    if (stream.write(text)) {
      stream.off("error", onError);
      resolve();
    } else {
      stream.once("drain", onDrain);
    }
  });
}

/**
 * Close a writable stream.
 *
 * @param {import("node:fs").WriteStream} stream Destination stream.
 * @returns {Promise<void>} Resolves after stream close.
 */
function endStream(stream) {
  return new Promise((resolve, reject) => {
    stream.once("error", reject);
    stream.end(resolve);
  });
}

/**
 * Create a hard link, accepting an existing destination only when it resolves
 * to the same inode and expected size.
 *
 * @param {string} source Source file.
 * @param {string} destination Destination file.
 * @param {number} expectedBytes Expected byte size.
 * @returns {Promise<void>} Resolves after verification.
 */
async function linkOrVerify(source, destination, expectedBytes) {
  try {
    await link(source, destination);
  } catch (caught) {
    if (
      !(
        caught instanceof Error &&
        "code" in caught &&
        /** @type {NodeJS.ErrnoException} */ (caught).code === "EEXIST"
      )
    ) {
      throw caught;
    }
    const [sourceInfo, destinationInfo] = await Promise.all([
      stat(source),
      stat(destination),
    ]);
    if (
      sourceInfo.ino !== destinationInfo.ino ||
      destinationInfo.size !== expectedBytes
    ) {
      throw new Error(`Stale publication destination: ${destination}`);
    }
  }
}

/**
 * Write one immutable open-data index shard.
 *
 * @param {string} openDataDirectory Open-data staging root.
 * @param {number} shardIndex Zero-based shard index.
 * @param {readonly JsonObject[]} entries Standard index entries.
 * @returns {Promise<JsonObject>} Top-level shard reference.
 */
async function writeOpenDataShard(openDataDirectory, shardIndex, entries) {
  const first = entries[0];
  const last = entries.at(-1);
  if (first === undefined || last === undefined) {
    throw new Error("Cannot write an empty publication shard");
  }
  const body = Buffer.from(
    `${JSON.stringify(
      {
        schemaVersion: "1",
        shardIndex,
        fromParcel: first.parcelIdentifier,
        toParcel: last.parcelIdentifier,
        count: entries.length,
        entries,
      },
      null,
      2,
    )}\n`,
    "utf8",
  );
  const fileName = `shard-${String(shardIndex).padStart(4, "0")}.json`;
  await writeFile(path.join(openDataDirectory, "shards", fileName), body);
  return {
    shardIndex,
    fromParcel: first.parcelIdentifier,
    toParcel: last.parcelIdentifier,
    count: entries.length,
    shardCid: await ipfsHash.of(body),
  };
}

/**
 * Build the canonical MCP coverage snapshot for locally published Polk data.
 *
 * Appraisal and permit counts are published because both are reconciled into
 * the property payload family. Enrichment tracks remain explicitly zero with
 * unknown expected counts until independently loaded and reconciled.
 *
 * @param {PolkPublicCoverageOptions} options Reconciled publication evidence.
 * @returns {JsonObject} MCP-compatible five-track coverage snapshot.
 */
export function buildPolkPublicCoverageSnapshot(options) {
  /**
   * Build one canonical enrichment row that has not been published.
   *
   * @param {"corporate" | "bbb" | "overture_places"} source Coverage source.
   * @returns {JsonObject} Explicitly incomplete coverage row.
   */
  const emptyTrack = (source) => ({
    county: "polk",
    source,
    ingested_count: 0,
    expected_count: null,
    first_loaded_at: null,
    last_loaded_at: null,
    cid: null,
    ipns_label: null,
  });
  /**
   * Build one enrichment row only from a passed Neon reconciliation track.
   *
   * @param {"corporate" | "bbb" | "overture_places"} publicSource Published coverage source.
   * @param {"sunbiz" | "bbb" | "overture_places"} receiptSource Reconciliation source.
   * @returns {JsonObject} Reconciled or explicitly empty coverage row.
   */
  const reconciledTrack = (publicSource, receiptSource) => {
    const track = options.reconciledTracks?.find(
      (candidate) =>
        candidate.source === receiptSource && candidate.passed === true,
    );
    const localCount =
      track !== undefined &&
      typeof track.localCount === "number" &&
      Number.isSafeInteger(track.localCount) &&
      track.localCount >= 0
        ? track.localCount
        : null;
    const neonCount =
      track !== undefined &&
      typeof track.neonCoverageCount === "number" &&
      Number.isSafeInteger(track.neonCoverageCount) &&
      track.neonCoverageCount >= 0
        ? track.neonCoverageCount
        : null;
    if (
      track === undefined ||
      localCount === null ||
      neonCount === null ||
      localCount !== neonCount
    ) {
      return emptyTrack(publicSource);
    }
    return {
      county: "polk",
      source: publicSource,
      ingested_count: neonCount,
      expected_count: localCount,
      first_loaded_at:
        typeof track.firstLoadedAt === "string" ? track.firstLoadedAt : null,
      last_loaded_at:
        typeof track.lastLoadedAt === "string" ? track.lastLoadedAt : null,
      cid: null,
      ipns_label: null,
    };
  };
  return {
    county: "polk",
    exportedAt: options.completedAt,
    datasets: [
      {
        county: "polk",
        source: "appraisal",
        ingested_count: options.propertyCount,
        expected_count: options.propertyCount,
        first_loaded_at: options.exportedAt,
        last_loaded_at: options.completedAt,
        cid: options.openDataCid,
        ipns_label: "oracle-open-data-polk",
      },
      {
        county: "polk",
        source: "permits",
        ingested_count: options.permitCount,
        expected_count: options.permitCount,
        first_loaded_at: options.exportedAt,
        last_loaded_at: options.completedAt,
        cid: options.openDataCid,
        ipns_label: "oracle-open-data-polk",
      },
      reconciledTrack("corporate", "sunbiz"),
      reconciledTrack("bbb", "bbb"),
      reconciledTrack("overture_places", "overture_places"),
    ],
  };
}

/**
 * Materialize family-separated local staging without external network calls.
 *
 * @param {PolkPublicationCliOptions} options Explicit materialization options.
 * @param {PolkPublicationInventory} inventory Validated inventory.
 * @param {JsonObject} manifest Local export manifest.
 * @param {PolkPublicationPlan} plan Validated plan.
 * @returns {Promise<void>} Resolves after local reconciliation.
 */
async function materializePolkPublication(options, inventory, manifest, plan) {
  await rm(options.outputDirectory, { recursive: true, force: true });
  const openDataDirectory = path.join(options.outputDirectory, "open-data");
  const openDataProperties = path.join(openDataDirectory, "properties");
  const queryTableDirectory = path.join(options.outputDirectory, "query-table");
  const coverageDirectory = path.join(
    options.outputDirectory,
    "dataset-coverage",
  );
  await Promise.all([
    mkdir(openDataProperties, { recursive: true }),
    mkdir(path.join(openDataDirectory, "shards"), { recursive: true }),
    mkdir(queryTableDirectory, { recursive: true }),
    mkdir(coverageDirectory, { recursive: true }),
  ]);

  const exportRun = isJsonObject(manifest.run) ? manifest.run : {};
  const exportedAt =
    typeof exportRun.startedAt === "string"
      ? exportRun.startedAt
      : plan.generatedAt;
  const completedAt =
    typeof exportRun.completedAt === "string"
      ? exportRun.completedAt
      : plan.generatedAt;
  const manifestStream = createWriteStream(
    path.join(openDataDirectory, "manifest.json"),
    { encoding: "utf8", mode: 0o600 },
  );
  await writeStreamText(
    manifestStream,
    `${JSON.stringify({
      schemaVersion: "1",
      county: "polk",
      exportedAt,
      completedAt,
      propertyCount: inventory.propertyCount,
      totalBytes: inventory.propertyBytes,
      minBytes: inventory.minimumPropertyBytes,
      averageBytes: Math.round(
        inventory.propertyBytes / inventory.propertyCount,
      ),
      maxBytes: inventory.maximumPropertyBytes,
    }).slice(0, -1)},"entries":[\n`,
  );

  /** @type {JsonObject[]} */
  let pendingShard = [];
  /** @type {JsonObject[]} */
  const shardReferences = [];
  let emittedCount = 0;
  let emittedBytes = 0;
  for (const manifestName of inventory.sourceManifestNames) {
    const sourceManifest = await readJsonObject(
      path.join(options.sourceDirectory, "manifests", manifestName),
    );
    if (!Array.isArray(sourceManifest.entries)) {
      throw new Error(`Missing entries in ${manifestName}`);
    }
    for (const rawEntry of sourceManifest.entries) {
      const entry = parsePolkPublicationEntry(rawEntry, manifestName);
      const destinationFile = path.join(
        "properties",
        `${entry.propertyId}.json`,
      );
      await linkOrVerify(
        path.join(options.sourceDirectory, entry.file),
        path.join(openDataDirectory, destinationFile),
        entry.fileSizeBytes,
      );
      const standardEntry = {
        propertyId: entry.propertyId,
        parcelIdentifier: entry.parcelIdentifier,
        filePath: destinationFile,
        fileSizeBytes: entry.fileSizeBytes,
        sha256: entry.sha256,
        cid: entry.cid,
      };
      emittedCount += 1;
      emittedBytes += entry.fileSizeBytes;
      await writeStreamText(
        manifestStream,
        `${emittedCount === 1 ? "" : ",\n"}${JSON.stringify(standardEntry)}`,
      );
      pendingShard.push({
        propertyId: entry.propertyId,
        parcelIdentifier: entry.parcelIdentifier,
        cid: entry.cid,
        fileSizeBytes: entry.fileSizeBytes,
      });
      if (pendingShard.length === options.shardSize) {
        shardReferences.push(
          await writeOpenDataShard(
            openDataDirectory,
            shardReferences.length,
            pendingShard,
          ),
        );
        pendingShard = [];
      }
    }
  }
  if (pendingShard.length > 0) {
    shardReferences.push(
      await writeOpenDataShard(
        openDataDirectory,
        shardReferences.length,
        pendingShard,
      ),
    );
  }
  await writeStreamText(manifestStream, "\n]}\n");
  await endStream(manifestStream);
  if (
    emittedCount !== inventory.propertyCount ||
    emittedBytes !== inventory.propertyBytes
  ) {
    throw new Error("Materialized open-data inventory did not reconcile");
  }

  const indexBody = Buffer.from(
    `${JSON.stringify(
      {
        schemaVersion: "1",
        county: "polk",
        exportedAt,
        completedAt,
        propertyCount: emittedCount,
        shardSize: options.shardSize,
        totalBytes: emittedBytes,
        shards: shardReferences,
      },
      null,
      2,
    )}\n`,
    "utf8",
  );
  await writeFile(path.join(openDataDirectory, "index.json"), indexBody);
  const localOpenDataIndexCid = await ipfsHash.of(indexBody);
  const queryTable = isJsonObject(manifest.output)
    ? manifest.output.queryTable
    : null;
  if (!isJsonObject(queryTable)) {
    throw new Error("Manifest query table is missing during materialization");
  }
  const queryTableFile = requiredText(queryTable, "file", "queryTable");
  const queryTableBytes = requiredCount(queryTable, "sizeBytes", "queryTable");
  const validation = /** @type {JsonObject} */ (plan.validation);
  const enrichments = isJsonObject(validation.enrichments)
    ? validation.enrichments
    : null;
  const neon = isJsonObject(enrichments?.neon) ? enrichments.neon : null;
  const reconciledTracks = Array.isArray(neon?.tracks)
    ? neon.tracks.filter(isJsonObject)
    : [];
  const publicCoverage = buildPolkPublicCoverageSnapshot({
    propertyCount: inventory.propertyCount,
    permitCount: requiredCount(validation, "permitCount", "plan.validation"),
    exportedAt,
    completedAt,
    openDataCid: localOpenDataIndexCid,
    reconciledTracks,
  });
  await Promise.all([
    linkOrVerify(
      path.join(options.sourceDirectory, queryTableFile),
      path.join(queryTableDirectory, "query-table.parquet"),
      queryTableBytes,
    ),
    writeFile(
      path.join(coverageDirectory, "dataset-coverage.json"),
      `${JSON.stringify(publicCoverage, null, 2)}\n`,
      { encoding: "utf8", mode: 0o600 },
    ),
  ]);
  const completedPlan = {
    ...plan,
    localOpenDataIndexCid,
  };
  await writeFile(
    path.join(options.outputDirectory, "publication-plan.json"),
    `${JSON.stringify(completedPlan, null, 2)}\n`,
    "utf8",
  );
}

/**
 * Validate and optionally materialize Polk publication inputs.
 *
 * No code path performs S3/Filebase uploads, IPNS changes, catalog writes,
 * deployments, or MCP environment changes.
 *
 * @param {readonly string[]} argv CLI arguments after the script path.
 * @returns {Promise<PolkPublicationPlan>} Validated publication plan.
 */
export async function runPreparePolkLocalPublication(argv) {
  const options = parsePolkPublicationCliOptions(argv);
  const { plan, inventory, manifest } = await buildPolkPublicationPlan(options);
  if (options.materialize) {
    await materializePolkPublication(options, inventory, manifest, plan);
  }
  process.stdout.write(`${JSON.stringify(plan, null, 2)}\n`);
  return plan;
}

if (
  process.argv[1] !== undefined &&
  fileURLToPath(import.meta.url) === path.resolve(process.argv[1])
) {
  runPreparePolkLocalPublication(process.argv.slice(2)).catch((caught) => {
    const message = caught instanceof Error ? caught.message : String(caught);
    process.stderr.write(
      `${JSON.stringify({
        event: "polk_local_publication_prepare_failed",
        error: message,
      })}\n`,
    );
    process.exitCode = 1;
  });
}
