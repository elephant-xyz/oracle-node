#!/usr/bin/env node

import { readFile, writeFile, mkdir } from "node:fs/promises";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import pg from "pg";

import {
  isJsonObject,
  readOptionalJsonObject,
  sha256File,
} from "../polk-local-parity-lib.mjs";

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * @typedef {"appraisal" | "permits" | "sunbiz" | "bbb" | "overture_places"} PolkLoadTrack
 */

/**
 * @typedef {object} PolkLocalTrackEvidence
 * @property {PolkLoadTrack} source Canonical coverage source.
 * @property {number | null} localCount Local artifact denominator.
 * @property {boolean} ready Whether local artifacts pass their stage gates.
 * @property {string} evidence Evidence or blocker.
 * @property {string} manifestPath Local manifest path.
 * @property {string | null} manifestSha256 Manifest hash when present.
 */

/**
 * @typedef {object} PolkLocalLoadManifest
 * @property {string} schemaVersion Manifest schema.
 * @property {string} generatedAt Generation timestamp.
 * @property {"polk"} county County slug.
 * @property {readonly PolkLocalTrackEvidence[]} tracks Local stage evidence.
 * @property {JsonObject} loaderHandoff Non-executing query-db loader handoff.
 * @property {boolean} ready Whether every parity track is locally ready.
 */

/**
 * @typedef {object} PolkNeonCoverageObservation
 * @property {PolkLoadTrack} source Coverage source.
 * @property {number} ingestedCount Canonical Neon coverage count.
 * @property {number | null} expectedCount Canonical expected count.
 * @property {string | null} firstLoadedAt First load timestamp.
 * @property {string | null} lastLoadedAt Last load timestamp.
 */

/**
 * @typedef {object} PolkNeonObservations
 * @property {string} schemaVersion Observation schema.
 * @property {string} observedAt Observation timestamp.
 * @property {"polk"} county County slug.
 * @property {readonly PolkNeonCoverageObservation[]} coverageRows Coverage rows.
 * @property {{release:string,rowCount:number,distinctGersIds:number,extractionClipCount:number|null,licenceGatePassed:boolean|null}} places Direct Overture observations.
 */

/**
 * Read a nested value from a JSON object.
 *
 * @param {JsonObject | null} object Root object.
 * @param {readonly string[]} keys Nested keys.
 * @returns {unknown} Nested value or undefined.
 */
function nestedValue(object, keys) {
  let current = /** @type {unknown} */ (object);
  for (const key of keys) {
    if (!isJsonObject(current)) return undefined;
    current = current[key];
  }
  return current;
}

/**
 * Read a non-negative integer from a nested JSON path.
 *
 * @param {JsonObject | null} object Root object.
 * @param {readonly string[]} keys Nested keys.
 * @returns {number | null} Count or null.
 */
function nestedCount(object, keys) {
  const value = nestedValue(object, keys);
  return Number.isSafeInteger(value) && Number(value) >= 0
    ? Number(value)
    : null;
}

/**
 * Hash an optional local manifest.
 *
 * @param {string} filePath Manifest path.
 * @param {JsonObject | null} value Parsed manifest.
 * @returns {Promise<string | null>} SHA-256 or null when absent.
 */
async function optionalManifestHash(filePath, value) {
  return value === null ? null : sha256File(filePath);
}

/**
 * Build one track row.
 *
 * @param {PolkLoadTrack} source Coverage source.
 * @param {string} manifestPath Manifest path.
 * @param {JsonObject | null} manifest Parsed manifest.
 * @param {number | null} localCount Local denominator.
 * @param {boolean} ready Local gate.
 * @param {string} evidence Evidence text.
 * @returns {Promise<PolkLocalTrackEvidence>} Track row.
 */
async function localTrack(
  source,
  manifestPath,
  manifest,
  localCount,
  ready,
  evidence,
) {
  return {
    source,
    localCount,
    ready,
    evidence,
    manifestPath: path.resolve(manifestPath),
    manifestSha256: await optionalManifestHash(manifestPath, manifest),
  };
}

/**
 * Build a fail-closed local-to-Neon handoff manifest.
 *
 * This does not upload artifacts or mutate Neon. It records exact local
 * denominators and the generic query-db loader contract an authorized operator
 * can use after placing artifacts in a non-production staging prefix.
 *
 * @param {{sourceDirectory:string,permitSummaryPath:string,sunbizManifestPath:string,bbbManifestPath:string,overtureSummaryPath:string}} options Artifact paths.
 * @returns {Promise<PolkLocalLoadManifest>} Local handoff manifest.
 */
export async function buildPolkLocalLoadManifest(options) {
  const appraisalManifestPath = path.join(
    options.sourceDirectory,
    "manifest.json",
  );
  const checkpointPath = path.join(
    options.sourceDirectory,
    ".state",
    "checkpoint.json",
  );
  const [appraisal, checkpoint, permits, sunbiz, bbb, overture] =
    await Promise.all([
      readOptionalJsonObject(appraisalManifestPath),
      readOptionalJsonObject(checkpointPath),
      readOptionalJsonObject(options.permitSummaryPath),
      readOptionalJsonObject(options.sunbizManifestPath),
      readOptionalJsonObject(options.bbbManifestPath),
      readOptionalJsonObject(options.overtureSummaryPath),
    ]);

  const appraisalCount = nestedCount(appraisal, ["output", "propertyCount"]);
  const appraisalRows = nestedCount(appraisal, [
    "output",
    "queryTable",
    "rowCount",
  ]);
  const appraisalDistinct = nestedCount(appraisal, [
    "output",
    "validation",
    "distinctParcels",
  ]);
  const appraisalReady =
    appraisalCount !== null &&
    appraisalCount > 0 &&
    appraisalRows === appraisalCount &&
    appraisalDistinct === appraisalCount &&
    checkpoint?.complete === true;

  const permitCount = nestedCount(permits, ["permitCount"]);
  const permitsReady =
    permits?.schemaVersion === "oracle-node.polk-permit-enrichment.v1" &&
    permitCount !== null &&
    permitCount > 0;

  const sunbizCount = nestedCount(sunbiz, ["transformedRecordCount"]);
  const sunbizReady =
    sunbiz?.schemaVersion === "oracle-node.polk-sunbiz-transform-match.v1" &&
    sunbiz?.county === "polk" &&
    sunbiz?.complete === true &&
    sunbizCount !== null &&
    sunbizCount > 0 &&
    nestedCount(sunbiz, ["sourceRecordCount"]) === sunbizCount &&
    nestedCount(sunbiz, ["invalidRecordCount"]) === 0;

  const bbbCount = nestedCount(bbb, ["harvestedProfileCount"]);
  const bbbReady =
    bbb?.schemaVersion === "oracle-node.polk-bbb-contractor-crm.v1" &&
    bbb?.county === "polk" &&
    bbb?.complete === true &&
    bbbCount !== null &&
    bbbCount > 0 &&
    nestedValue(bbb, ["gate", "actualPermitContractorLicenseEvidence"]) ===
      true;

  const placesCount = nestedCount(overture, ["clipCount"]);
  const placesReady =
    overture?.schemaVersion === "oracle-node.overture-places-extract.v1" &&
    overture?.county === "polk" &&
    overture?.mode === "extract" &&
    placesCount !== null &&
    placesCount > 0 &&
    nestedCount(overture, ["jsonl", "recordCount"]) === placesCount &&
    nestedValue(overture, ["licenceGate", "passed"]) === true;

  const tracks = await Promise.all([
    localTrack(
      "appraisal",
      appraisalManifestPath,
      appraisal,
      appraisalCount,
      appraisalReady,
      appraisalReady
        ? `${appraisalCount} local query-table rows reconcile to distinct Polk parcels and the completed checkpoint.`
        : "Appraisal manifest/query-table/distinct-parcel/checkpoint evidence is incomplete.",
    ),
    localTrack(
      "permits",
      options.permitSummaryPath,
      permits,
      permitCount,
      permitsReady,
      permitsReady
        ? `${permitCount} official bulk permit rows are locally evidenced.`
        : "The official Polk permit summary is absent or empty.",
    ),
    localTrack(
      "sunbiz",
      options.sunbizManifestPath,
      sunbiz,
      sunbizCount,
      sunbizReady,
      sunbizReady
        ? `${sunbizCount} Sunbiz registration records transformed from a complete exact-ZIP slice.`
        : "A complete non-empty Polk Sunbiz transform/match manifest is required.",
    ),
    localTrack(
      "bbb",
      options.bbbManifestPath,
      bbb,
      bbbCount,
      bbbReady,
      bbbReady
        ? `${bbbCount} BBB profiles come from complete trade harvests with permit-licence match gating.`
        : "A complete BBB multi-trade harvest and permit-licence CRM receipt is required.",
    ),
    localTrack(
      "overture_places",
      options.overtureSummaryPath,
      overture,
      placesCount,
      placesReady,
      placesReady
        ? `${placesCount} Overture places reconcile to JSONL and passed the source/licence gate.`
        : "A complete Polk Overture extract with JSONL reconciliation and licence evidence is required.",
    ),
  ]);
  return {
    schemaVersion: "oracle-node.polk-local-load-manifest.v1",
    generatedAt: new Date().toISOString(),
    county: "polk",
    tracks,
    loaderHandoff: {
      repository: "../elephant-query-db",
      loader: "npm run load:bulk",
      requiredJurisdictionKey: "polk_appraiser",
      requiredTracks: ["appraisal", "permits", "sunbiz", "bbb", "places"],
      requiredInputs: {
        bucket: "<authorized-non-production-staging-bucket>",
        appraisalPrefix: "<staged-polk-appraisal-prefix>",
        permitPrefix: "<staged-polk-permit-prefix>",
        sunbizPrefix: "<staged-sunbiz-polk-corporate-classes-prefix>",
        bbbPrefix: "<staged-bbb/category-data/polk-county-prefix>",
        placesPrefix: "<staged-overture-places/polk/release-prefix>",
        envFile: "<authorized-non-production-neon-env-file>",
      },
      commandTemplate:
        "npm --prefix ../elephant-query-db run load:bulk -- --env-file <authorized-non-production-neon-env-file> --bucket <authorized-non-production-staging-bucket> --jurisdiction-key polk_appraiser --tracks appraisal,permits,sunbiz,bbb,places --appraisal-prefix <staged-polk-appraisal-prefix> --permit-prefix <staged-polk-permit-prefix> --sunbiz-prefix <staged-sunbiz-polk-corporate-classes-prefix> --bbb-prefix <staged-bbb/category-data/polk-county-prefix> --places-prefix <staged-overture-places/polk/release-prefix>",
      executed: false,
      reason:
        "This repository prepares evidence only. Artifact staging and Neon mutation require authorized credentials and an explicit target environment.",
    },
    ready: tracks.every((track) => track.ready),
  };
}

/**
 * Normalize an unknown count from JSON/pg.
 *
 * @param {unknown} value Candidate count.
 * @param {string} field Diagnostic field.
 * @returns {number} Non-negative safe integer.
 */
function readCount(value, field) {
  const parsed =
    typeof value === "number"
      ? value
      : typeof value === "string" && /^\d+$/.test(value)
        ? Number(value)
        : Number.NaN;
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`Invalid ${field} count`);
  }
  return parsed;
}

/**
 * Validate observations loaded from a JSON file.
 *
 * @param {unknown} value Candidate observations.
 * @returns {PolkNeonObservations} Valid observations.
 */
export function parsePolkNeonObservations(value) {
  if (
    !isJsonObject(value) ||
    value.county !== "polk" ||
    !Array.isArray(value.coverageRows) ||
    !isJsonObject(value.places)
  ) {
    throw new Error("Invalid Polk Neon observations");
  }
  const coverageRows = value.coverageRows.map((candidate, index) => {
    if (!isJsonObject(candidate)) {
      throw new Error(`Invalid coverage row ${index}`);
    }
    const source = candidate.source;
    if (
      source !== "appraisal" &&
      source !== "permits" &&
      source !== "sunbiz" &&
      source !== "bbb" &&
      source !== "overture_places"
    ) {
      throw new Error(`Invalid coverage source at row ${index}`);
    }
    return {
      source,
      ingestedCount: readCount(
        candidate.ingestedCount ?? candidate.ingested_count,
        `${source} ingested`,
      ),
      expectedCount:
        candidate.expectedCount === null ||
        candidate.expected_count === null ||
        (candidate.expectedCount === undefined &&
          candidate.expected_count === undefined)
          ? null
          : readCount(
              candidate.expectedCount ?? candidate.expected_count,
              `${source} expected`,
            ),
      firstLoadedAt:
        typeof (candidate.firstLoadedAt ?? candidate.first_loaded_at) ===
        "string"
          ? String(candidate.firstLoadedAt ?? candidate.first_loaded_at)
          : null,
      lastLoadedAt:
        typeof (candidate.lastLoadedAt ?? candidate.last_loaded_at) === "string"
          ? String(candidate.lastLoadedAt ?? candidate.last_loaded_at)
          : null,
    };
  });
  const release =
    typeof value.places.release === "string" ? value.places.release : "";
  if (release.length === 0) {
    throw new Error("Polk Neon places observation requires a release");
  }
  return {
    schemaVersion:
      typeof value.schemaVersion === "string"
        ? value.schemaVersion
        : "oracle-node.polk-neon-observations.v1",
    observedAt:
      typeof value.observedAt === "string"
        ? value.observedAt
        : new Date().toISOString(),
    county: "polk",
    coverageRows,
    places: {
      release,
      rowCount: readCount(value.places.rowCount, "places row"),
      distinctGersIds: readCount(
        value.places.distinctGersIds,
        "places distinct GERS",
      ),
      extractionClipCount:
        value.places.extractionClipCount === null ||
        value.places.extractionClipCount === undefined
          ? null
          : readCount(
              value.places.extractionClipCount,
              "places extraction clip",
            ),
      licenceGatePassed:
        typeof value.places.licenceGatePassed === "boolean"
          ? value.places.licenceGatePassed
          : null,
    },
  };
}

/**
 * Reconcile local denominators with canonical Neon coverage and direct places
 * observations.
 *
 * @param {PolkLocalLoadManifest} localManifest Local handoff.
 * @param {PolkNeonObservations} observations Read-only Neon observations.
 * @returns {JsonObject} Fail-closed reconciliation receipt.
 */
export function reconcilePolkNeon(localManifest, observations) {
  const coverageBySource = new Map(
    observations.coverageRows.map((row) => [row.source, row]),
  );
  const placesLocalCount =
    localManifest.tracks.find((track) => track.source === "overture_places")
      ?.localCount ?? null;
  const tracks = localManifest.tracks.map((track) => {
    const coverage = coverageBySource.get(track.source) ?? null;
    const countMatches =
      track.localCount !== null &&
      coverage !== null &&
      coverage.ingestedCount === track.localCount;
    const timestampEvidenced =
      coverage !== null &&
      coverage.firstLoadedAt !== null &&
      coverage.lastLoadedAt !== null;
    const directPlacesMatch =
      track.source !== "overture_places" ||
      (placesLocalCount !== null &&
        observations.places.rowCount === placesLocalCount &&
        observations.places.distinctGersIds === placesLocalCount &&
        observations.places.extractionClipCount === placesLocalCount &&
        observations.places.licenceGatePassed === true);
    return {
      source: track.source,
      localReady: track.ready,
      localCount: track.localCount,
      neonCoverageCount: coverage?.ingestedCount ?? null,
      firstLoadedAt: coverage?.firstLoadedAt ?? null,
      lastLoadedAt: coverage?.lastLoadedAt ?? null,
      countMatches,
      timestampEvidenced,
      directPlacesMatch,
      passed:
        track.ready && countMatches && timestampEvidenced && directPlacesMatch,
    };
  });
  return {
    schemaVersion: "oracle-node.polk-neon-reconciliation.v1",
    generatedAt: new Date().toISOString(),
    county: "polk",
    localManifest: {
      schemaVersion: localManifest.schemaVersion,
      generatedAt: localManifest.generatedAt,
    },
    observations: {
      observedAt: observations.observedAt,
      places: observations.places,
    },
    tracks,
    complete:
      tracks.length === 5 && tracks.every((track) => track.passed === true),
    blocker: tracks.every((track) => track.passed === true)
      ? null
      : "One or more local artifact counts does not have a matching, timestamped Neon coverage row and direct places reconciliation.",
  };
}

/**
 * Load dotenv-style values without overwriting the process environment.
 *
 * @param {string} envFile Env file path.
 * @returns {Promise<void>} Resolves after parsing or when absent.
 */
async function loadEnvFile(envFile) {
  let text;
  try {
    text = await readFile(envFile, "utf8");
  } catch (caught) {
    if (
      caught instanceof Error &&
      "code" in caught &&
      /** @type {NodeJS.ErrnoException} */ (caught).code === "ENOENT"
    ) {
      return;
    }
    throw caught;
  }
  for (const line of text.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (trimmed.length === 0 || trimmed.startsWith("#")) continue;
    const separator = trimmed.indexOf("=");
    if (separator <= 0) continue;
    const key = trimmed.slice(0, separator);
    let value = trimmed.slice(separator + 1);
    if (
      value.length >= 2 &&
      ((value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'")))
    ) {
      value = value.slice(1, -1);
    }
    process.env[key] ??= value;
  }
}

/**
 * Read canonical coverage and direct Overture counts from Neon without writes.
 *
 * @param {{envFile:string,release:string}} options Connection and release.
 * @returns {Promise<PolkNeonObservations>} Read-only observations.
 */
export async function observePolkNeon(options) {
  await loadEnvFile(options.envFile);
  const databaseUrl =
    process.env.DATABASE_URL_UNPOOLED ?? process.env.DATABASE_URL ?? null;
  if (databaseUrl === null || databaseUrl.trim().length === 0) {
    throw new Error(
      `DATABASE_URL_UNPOOLED or DATABASE_URL is required in ${options.envFile}`,
    );
  }
  const client = new pg.Client({
    connectionString: databaseUrl,
    ssl: { rejectUnauthorized: false },
    connectionTimeoutMillis: 20_000,
    application_name: "oracle-node-polk-read-only-reconciliation",
  });
  await client.connect();
  try {
    await client.query("BEGIN READ ONLY");
    const [coverageResult, placesResult, extractionResult] = await Promise.all([
      client.query(
        `
          SELECT source, ingested_count, expected_count,
                 first_loaded_at::text, last_loaded_at::text
          FROM oracle_dataset_coverage
          WHERE county = $1
          ORDER BY source
        `,
        ["polk"],
      ),
      client.query(
        `
          SELECT count(*)::text AS row_count,
                 count(DISTINCT gers_id)::text AS distinct_gers_ids
          FROM business_locations
          WHERE source_system = 'overture_places'
            AND county_key = $1
            AND last_seen_release = $2
            AND is_current = true
        `,
        ["polk", options.release],
      ),
      client.query(
        `
          SELECT clip_count, licence_gate_passed
          FROM overture_place_extractions
          WHERE county_key = $1 AND overture_release = $2
          LIMIT 1
        `,
        ["polk", options.release],
      ),
    ]);
    await client.query("COMMIT");
    return parsePolkNeonObservations({
      schemaVersion: "oracle-node.polk-neon-observations.v1",
      observedAt: new Date().toISOString(),
      county: "polk",
      coverageRows: coverageResult.rows,
      places: {
        release: options.release,
        rowCount: placesResult.rows[0]?.row_count ?? "0",
        distinctGersIds: placesResult.rows[0]?.distinct_gers_ids ?? "0",
        extractionClipCount: extractionResult.rows[0]?.clip_count ?? null,
        licenceGatePassed:
          extractionResult.rows[0]?.licence_gate_passed ?? null,
      },
    });
  } catch (caught) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw caught;
  } finally {
    await client.end();
  }
}

/**
 * Run local manifest or read-only reconciliation mode.
 *
 * @param {readonly string[]} argv CLI arguments.
 * @returns {Promise<JsonObject>} Manifest or receipt.
 */
export async function runPolkNeonReconciliation(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      mode: { type: "string" },
      "source-dir": { type: "string" },
      "permit-summary": { type: "string" },
      "sunbiz-manifest": { type: "string" },
      "bbb-manifest": { type: "string" },
      "overture-summary": { type: "string" },
      observations: { type: "string" },
      "from-neon": { type: "boolean" },
      "env-file": { type: "string" },
      release: { type: "string" },
      out: { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });
  const mode = typeof values.mode === "string" ? values.mode : "manifest";
  const release =
    typeof values.release === "string" ? values.release : "2026-08-19.0";
  const localManifest = await buildPolkLocalLoadManifest({
    sourceDirectory:
      typeof values["source-dir"] === "string"
        ? values["source-dir"]
        : "tmp/polk/full",
    permitSummaryPath:
      typeof values["permit-summary"] === "string"
        ? values["permit-summary"]
        : "tmp/polk/parity/permit-enrichment.json",
    sunbizManifestPath:
      typeof values["sunbiz-manifest"] === "string"
        ? values["sunbiz-manifest"]
        : "tmp/polk/sunbiz/transformed/manifest.json",
    bbbManifestPath:
      typeof values["bbb-manifest"] === "string"
        ? values["bbb-manifest"]
        : "tmp/polk/bbb/manifest/contractor-crm.json",
    overtureSummaryPath:
      typeof values["overture-summary"] === "string"
        ? values["overture-summary"]
        : `tmp/polk/overture/${release}/extract/manifest/summary.json`,
  });
  let result = /** @type {JsonObject} */ (localManifest);
  if (mode === "reconcile") {
    let observations;
    if (typeof values.observations === "string") {
      observations = parsePolkNeonObservations(
        /** @type {unknown} */ (
          JSON.parse(await readFile(values.observations, "utf8"))
        ),
      );
    } else if (values["from-neon"] === true) {
      observations = await observePolkNeon({
        envFile:
          typeof values["env-file"] === "string"
            ? values["env-file"]
            : "../elephant-query-db/.env.local",
        release,
      });
    } else {
      throw new Error(
        "Reconcile mode requires --observations or explicit --from-neon",
      );
    }
    result = reconcilePolkNeon(localManifest, observations);
  } else if (mode !== "manifest") {
    throw new Error("--mode must be manifest or reconcile");
  }
  const outputPath =
    typeof values.out === "string"
      ? values.out
      : mode === "reconcile"
        ? "tmp/polk/neon/reconciliation-receipt.json"
        : "tmp/polk/neon/local-load-manifest.json";
  await mkdir(path.dirname(outputPath), { recursive: true });
  await writeFile(outputPath, `${JSON.stringify(result, null, 2)}\n`, "utf8");
  return result;
}

if (
  process.argv[1] !== undefined &&
  fileURLToPath(import.meta.url) === path.resolve(process.argv[1])
) {
  runPolkNeonReconciliation(process.argv.slice(2))
    .then((result) => {
      process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    })
    .catch((caught) => {
      const message = caught instanceof Error ? caught.message : String(caught);
      process.stderr.write(
        `${JSON.stringify({ event: "polk_neon_reconciliation_failed", error: message })}\n`,
      );
      process.exitCode = 1;
    });
}
