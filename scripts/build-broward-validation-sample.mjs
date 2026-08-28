#!/usr/bin/env node

/**
 * Build a deterministic 50-folio Broward appraisal validation sample.
 *
 * The sample preserves the original 25-pilot folios, then adds successful
 * full-run folios that increase transformed usage or geometry diversity.
 */

import { createReadStream } from "fs";
import { mkdir, readFile, writeFile } from "fs/promises";
import path from "path";
import { pathToFileURL } from "url";
import { parse } from "csv-parse";

import { BROWARD_PILOT_FOLIOS } from "./broward-folio.mjs";
import { SEED_COLUMNS, renderCsvRow } from "./build-broward-seed.mjs";

const DEFAULT_SEED_PATH = "downloads/broward/broward.csv";
const DEFAULT_RESULTS_PATH = "downloads/broward/full-ingestion/results.ndjson";
const DEFAULT_PILOT_SUMMARY_PATH =
  "downloads/broward/appraisal-validation-v2/summary.json";
const DEFAULT_OUTPUT_PATH =
  "downloads/broward/broward-validation-sample-50.csv";
const DEFAULT_MANIFEST_PATH =
  "downloads/broward/broward-validation-sample-50.json";
const DEFAULT_SAMPLE_SIZE = 50;

/**
 * @typedef {Record<string, string | undefined>} CsvRecord
 *
 * @typedef {object} CliOptions
 * @property {string} seedPath - Complete Broward seed CSV.
 * @property {string} resultsPath - Full-ingestion NDJSON results.
 * @property {string} pilotSummaryPath - Original pilot validation summary.
 * @property {string} outputPath - Selected CSV destination.
 * @property {string} manifestPath - Selection evidence destination.
 * @property {number} sampleSize - Required selected row count.
 *
 * @typedef {object} GeometryStats
 * @property {"Polygon" | "MultiPolygon"} type - GeoJSON type.
 * @property {number} components - Polygon component count.
 * @property {number} holes - Interior ring count.
 * @property {number} vertices - Total coordinate count.
 * @property {"small" | "medium" | "large" | "very-large"} vertexBucket
 *   Geometry complexity bucket.
 *
 * @typedef {object} Candidate
 * @property {CsvRecord} row - Complete seed row.
 * @property {string} folio - Canonical folio.
 * @property {string} usageType - Transformed property usage type.
 * @property {GeometryStats} geometry - Parsed geometry evidence.
 * @property {boolean} originalPilot - Whether the folio is in the first pilot.
 */

/**
 * Parse validation-sample CLI options.
 *
 * @param {readonly string[]} argv - Arguments after the script path.
 * @returns {CliOptions} Validated options.
 */
export function parseCliOptions(argv) {
  /** @type {CliOptions} */
  const options = {
    seedPath: DEFAULT_SEED_PATH,
    resultsPath: DEFAULT_RESULTS_PATH,
    pilotSummaryPath: DEFAULT_PILOT_SUMMARY_PATH,
    outputPath: DEFAULT_OUTPUT_PATH,
    manifestPath: DEFAULT_MANIFEST_PATH,
    sampleSize: DEFAULT_SAMPLE_SIZE,
  };
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (typeof value !== "string" || value.startsWith("--")) {
      throw new Error(`Missing value for ${flag}`);
    }
    if (flag === "--seed") options.seedPath = value;
    else if (flag === "--results") options.resultsPath = value;
    else if (flag === "--pilot-summary") options.pilotSummaryPath = value;
    else if (flag === "--output") options.outputPath = value;
    else if (flag === "--manifest") options.manifestPath = value;
    else if (flag === "--size") options.sampleSize = Number.parseInt(value, 10);
    else throw new Error(`Unknown option: ${flag}`);
  }
  if (!Number.isInteger(options.sampleSize) || options.sampleSize < 1) {
    throw new Error("--size must be a positive integer");
  }
  return options;
}

/**
 * Analyze one Polygon or MultiPolygon without altering coordinates.
 *
 * @param {string | undefined} raw - GeoJSON geometry JSON.
 * @returns {GeometryStats | null} Geometry statistics.
 */
export function analyzeGeometry(raw) {
  if (raw === undefined || raw.trim() === "") return null;
  const parsed = /** @type {{ type?: unknown, coordinates?: unknown }} */ (
    JSON.parse(raw)
  );
  if (
    (parsed.type !== "Polygon" && parsed.type !== "MultiPolygon") ||
    !Array.isArray(parsed.coordinates)
  ) {
    return null;
  }
  const polygons =
    parsed.type === "Polygon" ? [parsed.coordinates] : parsed.coordinates;
  let holes = 0;
  let vertices = 0;
  for (const polygon of polygons) {
    if (!Array.isArray(polygon)) continue;
    holes += Math.max(0, polygon.length - 1);
    for (const ring of polygon) {
      if (Array.isArray(ring)) vertices += ring.length;
    }
  }
  return {
    type: parsed.type,
    components: polygons.length,
    holes,
    vertices,
    vertexBucket:
      vertices <= 10
        ? "small"
        : vertices <= 50
          ? "medium"
          : vertices <= 200
            ? "large"
            : "very-large",
  };
}

/**
 * Read successful full-run usages keyed by folio.
 *
 * @param {string} resultsPath - NDJSON results path.
 * @returns {Promise<Map<string, string>>} Successful folio usage map.
 */
async function readSuccessfulUsageTypes(resultsPath) {
  const map = new Map();
  const text = await readFile(resultsPath, "utf8");
  for (const line of text.split(/\r?\n/u)) {
    if (line.trim() === "") continue;
    const result =
      /** @type {{ folio?: unknown, status?: unknown, propertyUsageType?: unknown }} */ (
        JSON.parse(line)
      );
    if (
      result.status === "succeeded" &&
      typeof result.folio === "string" &&
      typeof result.propertyUsageType === "string"
    ) {
      map.set(result.folio, result.propertyUsageType);
    }
  }
  return map;
}

/**
 * Add validated original-pilot usages to the successful usage map.
 *
 * @param {Map<string, string>} usageByFolio - Mutable usage map.
 * @param {string} summaryPath - Pilot summary JSON.
 * @returns {Promise<void>}
 */
async function addPilotUsageTypes(usageByFolio, summaryPath) {
  const summary =
    /** @type {{ results?: { requestIdentifier?: unknown, propertyUsageType?: unknown, validationSuccess?: unknown }[] }} */ (
      JSON.parse(await readFile(summaryPath, "utf8"))
    );
  for (const result of summary.results ?? []) {
    if (
      result.validationSuccess === true &&
      typeof result.requestIdentifier === "string" &&
      typeof result.propertyUsageType === "string"
    ) {
      usageByFolio.set(result.requestIdentifier, result.propertyUsageType);
    }
  }
}

/**
 * Select candidates while preferring new usage/geometry signatures.
 *
 * @param {readonly Candidate[]} candidates - Validated candidates.
 * @param {number} sampleSize - Exact target size.
 * @returns {Candidate[]} Deterministic selection.
 */
export function selectCandidates(candidates, sampleSize) {
  /** @type {Candidate[]} */
  const selected = [];
  const selectedFolios = new Set();
  const signatures = new Set();
  const add = (candidate) => {
    if (selectedFolios.has(candidate.folio) || selected.length >= sampleSize) {
      return;
    }
    selected.push(candidate);
    selectedFolios.add(candidate.folio);
    signatures.add(candidateSignature(candidate));
  };
  for (const candidate of candidates) {
    if (candidate.originalPilot) add(candidate);
  }
  const selectedUsageTypes = new Set(
    selected.map((candidate) => candidate.usageType),
  );
  for (const candidate of candidates) {
    if (
      candidate.usageType !== "PendingValidation" &&
      !selectedUsageTypes.has(candidate.usageType)
    ) {
      add(candidate);
      selectedUsageTypes.add(candidate.usageType);
    }
  }
  for (const candidate of candidates) {
    if (
      candidate.geometry.type === "MultiPolygon" ||
      candidate.geometry.holes > 0 ||
      candidate.geometry.vertexBucket === "very-large"
    ) {
      add(candidate);
    }
  }
  for (const candidate of candidates) {
    if (!signatures.has(candidateSignature(candidate))) add(candidate);
  }
  for (const candidate of candidates) add(candidate);
  if (selected.length !== sampleSize) {
    throw new Error(
      `Selected ${String(selected.length)} of ${String(sampleSize)} required parcels`,
    );
  }
  return selected;
}

/**
 * Stable diversity signature for greedy selection.
 *
 * @param {Candidate} candidate - Selection candidate.
 * @returns {string} Usage and geometry signature.
 */
function candidateSignature(candidate) {
  return [
    candidate.usageType,
    candidate.geometry.type,
    candidate.geometry.vertexBucket,
    candidate.geometry.holes > 0 ? "holes" : "no-holes",
    /[A-Z]/u.test(candidate.folio) ? "alphanumeric" : "numeric",
  ].join("|");
}

/**
 * Build the 50-row sample and evidence manifest.
 *
 * @param {CliOptions} options - Validated options.
 * @returns {Promise<Candidate[]>} Selected candidates.
 */
export async function buildValidationSample(options) {
  const usageByFolio = await readSuccessfulUsageTypes(options.resultsPath);
  await addPilotUsageTypes(usageByFolio, options.pilotSummaryPath);
  const originalPilot = new Set(BROWARD_PILOT_FOLIOS);
  /** @type {Candidate[]} */
  const candidates = [];
  /** @type {Candidate[]} */
  const geometryCandidates = [];
  const parser = createReadStream(options.seedPath).pipe(
    parse({ columns: true, skip_empty_lines: true }),
  );
  for await (const parsedRow of parser) {
    const row = /** @type {CsvRecord} */ (parsedRow);
    const folio = row.request_identifier;
    if (typeof folio !== "string") continue;
    const geometry = analyzeGeometry(row.parcel_polygon);
    if (geometry === null) continue;
    const usageType = usageByFolio.get(folio);
    const candidate = {
      row,
      folio,
      usageType: usageType ?? "PendingValidation",
      geometry,
      originalPilot: originalPilot.has(folio),
    };
    if (usageType !== undefined) {
      candidates.push(candidate);
    } else if (
      geometry.type === "MultiPolygon" ||
      geometry.holes > 0 ||
      geometry.vertexBucket === "very-large"
    ) {
      geometryCandidates.push(candidate);
    }
  }
  const selected = selectCandidates(
    [...candidates, ...geometryCandidates],
    options.sampleSize,
  );
  await mkdir(path.dirname(options.outputPath), { recursive: true });
  await writeFile(
    options.outputPath,
    `${SEED_COLUMNS.join(",")}\n${selected
      .map((candidate) =>
        renderCsvRow(/** @type {Record<string, string>} */ (candidate.row)),
      )
      .join("")}`,
    "utf8",
  );
  await writeFile(
    options.manifestPath,
    `${JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        sampleSize: selected.length,
        uniqueFolios: new Set(selected.map((item) => item.folio)).size,
        usageTypes: [...new Set(selected.map((item) => item.usageType))].sort(),
        geometryTypes: [
          ...new Set(selected.map((item) => item.geometry.type)),
        ].sort(),
        vertexBuckets: [
          ...new Set(selected.map((item) => item.geometry.vertexBucket)),
        ].sort(),
        parcels: selected.map((item) => ({
          folio: item.folio,
          usageType: item.usageType,
          geometry: item.geometry,
          originalPilot: item.originalPilot,
        })),
      },
      null,
      2,
    )}\n`,
    "utf8",
  );
  return selected;
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const options = parseCliOptions(process.argv.slice(2));
  const selected = await buildValidationSample(options);
  console.log(
    JSON.stringify({
      level: "info",
      message: "broward_validation_sample_complete",
      outputPath: options.outputPath,
      manifestPath: options.manifestPath,
      selected: selected.length,
      usageTypes: [...new Set(selected.map((item) => item.usageType))].sort(),
      geometryTypes: [
        ...new Set(selected.map((item) => item.geometry.type)),
      ].sort(),
    }),
  );
}
