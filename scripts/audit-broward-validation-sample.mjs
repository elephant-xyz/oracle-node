#!/usr/bin/env node

/**
 * Acceptance audit for the 50-parcel Broward appraisal pilot.
 */

import { readFile, writeFile } from "fs/promises";
import path from "path";
import { pathToFileURL } from "url";
import AdmZip from "adm-zip";
import { parse as parseCsv } from "csv-parse/sync";

import {
  requireParcelRecords,
  unwrapBrowardPrepareCapture,
} from "./capture-broward-parcel.mjs";
import { normalizeBrowardFolio } from "./broward-folio.mjs";
import { analyzeGeometry } from "./build-broward-validation-sample.mjs";

const SAMPLE_PATH = "downloads/broward/broward-validation-sample-50.csv";
const SAMPLE_MANIFEST_PATH =
  "downloads/broward/broward-validation-sample-50.json";
const CAPTURES_PATH =
  "downloads/broward/broward-validation-sample-50-captures.zip";
const VALIDATION_DIRECTORY = "downloads/broward/appraisal-validation-50";
const QUERY_MANIFEST_PATH =
  "downloads/broward/pilot-query/query-table-manifest.json";
const DONPHAN_EVIDENCE_PATH =
  "downloads/broward/pilot-query/donphan-verification.json";
const FULL_RESULTS_PATH = "downloads/broward/full-ingestion/results.ndjson";
const OUTPUT_PATH = "downloads/broward/broward-validation-audit-50.json";

/**
 * @typedef {Record<string, unknown>} JsonObject
 * @typedef {Record<string, string | undefined>} CsvRecord
 *
 * @typedef {object} GeometryAudit
 * @property {boolean} valid - Whether every checked coordinate is valid.
 * @property {number} geometryFiles - Transformed geometry file count.
 * @property {number} relationshipFiles - Parcel-to-geometry relationship count.
 * @property {number} vertices - Total transformed polygon vertices.
 */

/**
 * Return true for a non-array JSON object.
 *
 * @param {unknown} value - Candidate.
 * @returns {value is JsonObject} Whether the value is an object.
 */
function isObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Read one required JSON object from a ZIP.
 *
 * @param {AdmZip} zip - Open ZIP.
 * @param {string} entryName - Required entry path.
 * @returns {JsonObject} Parsed object.
 */
function readZipObject(zip, entryName) {
  const entry = zip.getEntry(entryName);
  if (entry === null) throw new Error(`Missing ${entryName}`);
  const value = /** @type {unknown} */ (
    JSON.parse(entry.getData().toString("utf8"))
  );
  if (!isObject(value)) throw new Error(`${entryName} is not a JSON object`);
  return value;
}

/**
 * Verify transformed geometry files and their relationship count.
 *
 * @param {AdmZip} zip - Transformed artifact.
 * @returns {GeometryAudit} Geometry audit.
 */
export function auditTransformedGeometry(zip) {
  const geometryEntries = zip
    .getEntries()
    .filter((entry) =>
      /^data\/geometry_parcel_\d+\.json$/u.test(entry.entryName),
    );
  const relationshipEntries = zip
    .getEntries()
    .filter((entry) =>
      /^data\/relationship_parcel_has_geometry_parcel_\d+\.json$/u.test(
        entry.entryName,
      ),
    );
  let vertices = 0;
  let valid = geometryEntries.length > 0;
  for (const entry of geometryEntries) {
    const geometry = /** @type {{ polygon?: unknown }} */ (
      JSON.parse(entry.getData().toString("utf8"))
    );
    if (!Array.isArray(geometry.polygon) || geometry.polygon.length < 4) {
      valid = false;
      continue;
    }
    vertices += geometry.polygon.length;
    for (const point of geometry.polygon) {
      if (!isObject(point)) {
        valid = false;
        continue;
      }
      const longitude = Number(point.longitude);
      const latitude = Number(point.latitude);
      if (
        !Number.isFinite(longitude) ||
        !Number.isFinite(latitude) ||
        longitude < -180 ||
        longitude > 180 ||
        latitude < -90 ||
        latitude > 90
      ) {
        valid = false;
      }
    }
  }
  if (geometryEntries.length !== relationshipEntries.length) valid = false;
  return {
    valid,
    geometryFiles: geometryEntries.length,
    relationshipFiles: relationshipEntries.length,
    vertices,
  };
}

/**
 * Count full-run failure classes without retaining source data.
 *
 * @param {string} text - Results NDJSON.
 * @returns {{ sourceErrors: number, transformErrors: number }} Failure counts.
 */
export function countFailureClasses(text) {
  let sourceErrors = 0;
  let transformErrors = 0;
  for (const line of text.split(/\r?\n/u)) {
    if (line.trim() === "") continue;
    const item = /** @type {{ status?: unknown }} */ (JSON.parse(line));
    if (item.status === "source_error") sourceErrors += 1;
    if (item.status === "transform_error") transformErrors += 1;
  }
  return { sourceErrors, transformErrors };
}

/**
 * Run every Broward pilot acceptance check and write a durable local report.
 *
 * @returns {Promise<JsonObject>} Acceptance report.
 */
export async function runAudit() {
  /** @type {CsvRecord[]} */
  const seedRows = parseCsv(await readFile(SAMPLE_PATH, "utf8"), {
    columns: true,
    skip_empty_lines: true,
  });
  const sampleManifest =
    /** @type {{ sampleSize?: unknown, uniqueFolios?: unknown, usageTypes?: unknown[], geometryTypes?: unknown[], vertexBuckets?: unknown[], parcels?: unknown[] }} */ (
      JSON.parse(await readFile(SAMPLE_MANIFEST_PATH, "utf8"))
    );
  const validationSummary =
    /** @type {{ total?: unknown, capturesPassed?: unknown, transformsPassed?: unknown, validationsPassed?: unknown, results?: unknown[] }} */ (
      JSON.parse(
        await readFile(path.join(VALIDATION_DIRECTORY, "summary.json"), "utf8"),
      )
    );
  const queryManifest =
    /** @type {{ rowCount?: unknown, distinctFolios?: unknown }} */ (
      JSON.parse(await readFile(QUERY_MANIFEST_PATH, "utf8"))
    );
  const donphan =
    /** @type {{ result?: unknown, schema?: { columnCount?: unknown }, reconciliation?: { propertyCount?: unknown, distinctFolios?: unknown, validCoordinates?: unknown }, knownParcelLookup?: { parcel_identifier?: unknown, address_city?: unknown } }} */ (
      JSON.parse(await readFile(DONPHAN_EVIDENCE_PATH, "utf8"))
    );
  const captures = new AdmZip(CAPTURES_PATH);
  const uniqueFolios = new Set();
  const usageTypes = new Set();
  const propertyTypes = new Set();
  const geometryTypes = new Set();
  const vertexBuckets = new Set();
  let captureRecords = 0;
  let geometryArtifacts = 0;
  let geometryRelationships = 0;
  let geometryVertices = 0;
  let validGeometryArtifacts = 0;
  let matchingPropertyFolios = 0;
  let browardAddresses = 0;
  let nonEmptyAddresses = 0;
  let marketValues = 0;

  for (const row of seedRows) {
    const folio = normalizeBrowardFolio(row.request_identifier);
    if (folio === undefined) continue;
    uniqueFolios.add(folio);
    const sourceGeometry = analyzeGeometry(row.parcel_polygon);
    if (sourceGeometry !== null) {
      geometryTypes.add(sourceGeometry.type);
      vertexBuckets.add(sourceGeometry.vertexBucket);
    }
    const captureEntry = captures.getEntry(`${folio}.json`);
    if (captureEntry === null) continue;
    const envelope = unwrapBrowardPrepareCapture(
      JSON.parse(captureEntry.getData().toString("utf8")),
    );
    const records = requireParcelRecords(envelope, folio);
    if (records.length > 0) captureRecords += 1;

    const artifact = new AdmZip(
      path.join(VALIDATION_DIRECTORY, `${folio}.zip`),
    );
    const property = readZipObject(artifact, "data/property.json");
    const address = readZipObject(artifact, "data/address.json");
    const tax = readZipObject(artifact, "data/tax_1.json");
    if (property.parcel_identifier === folio) matchingPropertyFolios += 1;
    if (address.county_name === "Broward") browardAddresses += 1;
    if (
      typeof address.unnormalized_address === "string" &&
      address.unnormalized_address.trim() !== ""
    ) {
      nonEmptyAddresses += 1;
    }
    if (tax.property_market_value_amount !== null) marketValues += 1;
    if (typeof property.property_usage_type === "string") {
      usageTypes.add(property.property_usage_type);
    }
    if (typeof property.property_type === "string") {
      propertyTypes.add(property.property_type);
    }
    const geometry = auditTransformedGeometry(artifact);
    geometryArtifacts += geometry.geometryFiles;
    geometryRelationships += geometry.relationshipFiles;
    geometryVertices += geometry.vertices;
    if (geometry.valid) validGeometryArtifacts += 1;
  }

  let emptyEnvelopeRejected = false;
  try {
    requireParcelRecords(
      { d: { parcelInfok__BackingField: null } },
      "999999999999",
    );
  } catch {
    emptyEnvelopeRejected = true;
  }
  const malformedFolioRejected =
    normalizeBrowardFolio("504108_BJ0140") === undefined;
  const failureCounts = countFailureClasses(
    await readFile(FULL_RESULTS_PATH, "utf8"),
  );

  const checks = {
    exactSampleSize: seedRows.length === 50,
    deduplicatedFolios: uniqueFolios.size === 50,
    sampleManifestReconciled:
      sampleManifest.sampleSize === 50 && sampleManifest.uniqueFolios === 50,
    sourceCapturesComplete: captureRecords === 50,
    transformsComplete:
      validationSummary.total === 50 &&
      validationSummary.transformsPassed === 50,
    schemaValidationComplete: validationSummary.validationsPassed === 50,
    propertyFoliosReconciled: matchingPropertyFolios === 50,
    countyLabelsCorrect: browardAddresses === 50,
    addressesConsumable: nonEmptyAddresses === 50,
    geometryPresentAndValid: validGeometryArtifacts === 50,
    geometryRelationshipsReconciled:
      geometryArtifacts === geometryRelationships,
    geometryComplexityCovered:
      vertexBuckets.has("small") &&
      vertexBuckets.has("medium") &&
      vertexBuckets.has("large") &&
      vertexBuckets.has("very-large"),
    officialGeometryTypeCovered:
      geometryTypes.size === 1 && geometryTypes.has("Polygon"),
    propertyDiversityCovered: usageTypes.size >= 10 && propertyTypes.size >= 2,
    failurePathsVerified:
      emptyEnvelopeRejected &&
      malformedFolioRejected &&
      failureCounts.sourceErrors > 0 &&
      failureCounts.transformErrors === 0,
    queryRowsReconciled:
      queryManifest.rowCount === 50 && queryManifest.distinctFolios === 50,
    donphanConsumable:
      donphan.result === "pass" &&
      donphan.schema?.columnCount === 37 &&
      donphan.reconciliation?.propertyCount === 50 &&
      donphan.reconciliation?.distinctFolios === 50 &&
      donphan.reconciliation?.validCoordinates === 50 &&
      donphan.knownParcelLookup?.parcel_identifier === "504108BJ0140" &&
      donphan.knownParcelLookup?.address_city === "PLANTATION",
  };
  const report = {
    generatedAt: new Date().toISOString(),
    result: Object.values(checks).every(Boolean) ? "pass" : "fail",
    checks,
    counts: {
      seedRows: seedRows.length,
      uniqueFolios: uniqueFolios.size,
      captureRecords,
      transformed: validationSummary.transformsPassed,
      schemaValidated: validationSummary.validationsPassed,
      matchingPropertyFolios,
      browardAddresses,
      nonEmptyAddresses,
      marketValues,
      propertyUsageTypes: usageTypes.size,
      propertyTypes: propertyTypes.size,
      geometryTypes: [...geometryTypes].sort(),
      vertexBuckets: [...vertexBuckets].sort(),
      geometryArtifacts,
      geometryRelationships,
      geometryVertices,
      validGeometryArtifacts,
      fullRunFailureEvidence: failureCounts,
      donphanRows: donphan.reconciliation?.propertyCount,
    },
    notes: {
      geometry:
        "The official BCPA GIS parcel layer publishes only esriGeometryPolygon. The sample covers four vertex-complexity buckets; no second official geometry type exists.",
      completeness:
        "Elephant CLI validates schema and rejects unused JSON. This audit additionally reconciles seed, capture, transform, relationship, query-table, and Donphan counts.",
    },
  };
  await writeFile(OUTPUT_PATH, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  if (report.result !== "pass") {
    throw new Error(`Broward validation audit failed: ${OUTPUT_PATH}`);
  }
  return report;
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  console.log(JSON.stringify(await runAudit(), null, 2));
}
