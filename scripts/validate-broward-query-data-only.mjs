#!/usr/bin/env node

/**
 * Validate Broward query-data-only artifacts without publishing anything.
 *
 * The validator enforces the non-publication marker, rejects deferred
 * fact-sheet files and broken relative links, optionally compares every
 * retained JSON filename with accepted full artifacts, runs Elephant CLI
 * Lexicon validation, and performs a query-loader Parquet dry run.
 */

import { mkdir, readdir, writeFile } from "fs/promises";
import path from "path";
import { pathToFileURL } from "url";
import AdmZip from "adm-zip";
import { validate } from "@elephant-xyz/cli/lib";

import { buildQueryTableFromArtifacts } from "./build-broward-pilot-query-table.mjs";
import {
  inspectQueryDataOnlyArtifact,
  QUERY_DATA_ONLY_SUFFIX,
} from "./broward-query-data-only.mjs";

const DEFAULT_ARTIFACT_DIRECTORY =
  "downloads/broward/query-data-only-benchmark/query-data-only-artifacts";
const DEFAULT_CAPTURE_ARCHIVE =
  "downloads/broward/broward-validation-sample-50-captures.zip";
const DEFAULT_OUTPUT_DIRECTORY = "downloads/broward/query-data-only-validation";

/**
 * @typedef {object} DataOnlyValidationOptions
 * @property {string} artifactDirectory - Root containing sharded classified artifacts.
 * @property {string} capturesPath - Pilot capture ZIP used by the query-loader dry run.
 * @property {string} outputDirectory - Private validation reports and Parquet output.
 * @property {string | null} referenceArtifactDirectory - Optional accepted full ZIP directory.
 * @property {number | null} limit - Optional validation cap.
 *
 * @typedef {object} ClassifiedArtifact
 * @property {string} folio - Canonical folio from the internal safety marker.
 * @property {string} artifactPath - Absolute `.query-data-only.zip` path.
 * @property {number} jsonEntryCount - Retained Lexicon JSON file count.
 */

/**
 * Parse query-data-only validation CLI arguments.
 *
 * @param {readonly string[]} argv - Arguments after the script path.
 * @returns {DataOnlyValidationOptions} Validated paths and optional cap.
 */
export function parseValidationOptions(argv) {
  /** @type {DataOnlyValidationOptions} */
  const options = {
    artifactDirectory: DEFAULT_ARTIFACT_DIRECTORY,
    capturesPath: DEFAULT_CAPTURE_ARCHIVE,
    outputDirectory: DEFAULT_OUTPUT_DIRECTORY,
    referenceArtifactDirectory: null,
    limit: null,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (typeof value !== "string" || value.startsWith("--")) {
      throw new Error(`Missing value for ${flag}`);
    }
    if (flag === "--artifacts") options.artifactDirectory = value;
    else if (flag === "--captures") options.capturesPath = value;
    else if (flag === "--output") options.outputDirectory = value;
    else if (flag === "--reference-artifacts") {
      options.referenceArtifactDirectory = value;
    } else if (flag === "--limit") {
      const limit = Number.parseInt(value, 10);
      if (!Number.isInteger(limit) || limit < 1) {
        throw new Error("--limit must be a positive integer");
      }
      options.limit = limit;
    } else {
      throw new Error(`Unknown option: ${flag}`);
    }
    index += 1;
  }
  return options;
}

/**
 * Recursively list classified artifacts in deterministic path order.
 *
 * @param {string} directory - Current artifact directory.
 * @returns {Promise<string[]>} Absolute classified artifact paths.
 */
async function listArtifactPaths(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const nested = await Promise.all(
    entries.map(async (entry) => {
      const entryPath = path.join(directory, entry.name);
      if (entry.isDirectory()) return listArtifactPaths(entryPath);
      return entry.isFile() && entry.name.endsWith(QUERY_DATA_ONLY_SUFFIX)
        ? [entryPath]
        : [];
    }),
  );
  return nested.flat().sort();
}

/**
 * Return the JSON files a full transform should share with data-only output.
 *
 * Fact-sheet entities and relationships are intentionally absent. The HTML is
 * non-JSON, while all other JSON must be retained.
 *
 * @param {AdmZip} zip - Accepted full transformed archive.
 * @returns {string[]} Sorted expected data-only JSON entry names.
 */
function expectedRetainedJsonEntries(zip) {
  return zip
    .getEntries()
    .filter(
      (entry) =>
        !entry.isDirectory &&
        entry.entryName.startsWith("data/") &&
        entry.entryName.endsWith(".json") &&
        !/fact[_-]?sheet/iu.test(entry.entryName),
    )
    .map((entry) => entry.entryName)
    .sort();
}

/**
 * Prove that a data-only archive retained every non-fact-sheet JSON file from
 * its accepted full-transform reference.
 *
 * @param {AdmZip} dataOnly - Classified data-only archive.
 * @param {AdmZip} reference - Accepted full artifact for the same folio.
 * @returns {void}
 */
export function assertRetainedJsonParity(dataOnly, reference) {
  const actual = dataOnly
    .getEntries()
    .filter(
      (entry) =>
        !entry.isDirectory &&
        entry.entryName.startsWith("data/") &&
        entry.entryName.endsWith(".json"),
    )
    .map((entry) => entry.entryName)
    .sort();
  const expected = expectedRetainedJsonEntries(reference);
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    const missing = expected.filter((entry) => !actual.includes(entry));
    const unexpected = actual.filter((entry) => !expected.includes(entry));
    throw new Error(
      `Retained JSON mismatch; missing=${JSON.stringify(missing)}, unexpected=${JSON.stringify(unexpected)}`,
    );
  }
}

/**
 * Run structural, Lexicon, reference-parity, and query-loader validation.
 *
 * @param {DataOnlyValidationOptions} options - Validated CLI options.
 * @returns {Promise<{
 *   artifactCount: number,
 *   lexiconValidCount: number,
 *   referenceParityCount: number,
 *   queryRowCount: number,
 *   summaryPath: string
 * }>} Complete validation evidence.
 */
export async function validateQueryDataOnlyArtifacts(options) {
  const artifactDirectory = path.resolve(options.artifactDirectory);
  const capturesPath = path.resolve(options.capturesPath);
  const outputDirectory = path.resolve(options.outputDirectory);
  const referenceArtifactDirectory =
    options.referenceArtifactDirectory === null
      ? null
      : path.resolve(options.referenceArtifactDirectory);
  const available = await listArtifactPaths(artifactDirectory);
  const selected =
    options.limit === null ? available : available.slice(0, options.limit);
  if (selected.length === 0) {
    throw new Error(
      `No ${QUERY_DATA_ONLY_SUFFIX} artifacts in ${artifactDirectory}`,
    );
  }
  await mkdir(outputDirectory, { recursive: true, mode: 0o700 });
  /** @type {ClassifiedArtifact[]} */
  const artifacts = [];
  let referenceParityCount = 0;
  let lexiconValidCount = 0;
  for (const artifactPath of selected) {
    const inspection = await inspectQueryDataOnlyArtifact(artifactPath);
    const { folio } = inspection.manifest;
    if (referenceArtifactDirectory !== null) {
      const referencePath = path.join(
        referenceArtifactDirectory,
        `${folio}.zip`,
      );
      assertRetainedJsonParity(
        new AdmZip(artifactPath),
        new AdmZip(referencePath),
      );
      referenceParityCount += 1;
    }
    const validationCsv = path.join(outputDirectory, `${folio}-validation.csv`);
    const result = await validate({
      input: artifactPath,
      outputCsv: validationCsv,
      cwd: outputDirectory,
    });
    if (!result.success) {
      throw new Error(
        `Lexicon validation failed for ${folio}: ${result.error ?? "unknown error"}`,
      );
    }
    lexiconValidCount += 1;
    artifacts.push({
      folio,
      artifactPath,
      jsonEntryCount: inspection.jsonEntryCount,
    });
  }
  const queryResult = await buildQueryTableFromArtifacts({
    artifacts,
    capturesPath,
    outputDirectory: path.join(outputDirectory, "query-loader-dry-run"),
  });
  if (queryResult.rowCount !== artifacts.length) {
    throw new Error(
      `Query-loader row count ${String(queryResult.rowCount)} does not match ${String(artifacts.length)} artifacts`,
    );
  }
  const summaryPath = path.join(outputDirectory, "summary.json");
  await writeFile(
    summaryPath,
    `${JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        artifactMode: "query-data-only",
        publishable: false,
        artifactCount: artifacts.length,
        lexiconValidCount,
        referenceParityCount,
        queryRowCount: queryResult.rowCount,
        totalRetainedJsonEntries: artifacts.reduce(
          (sum, artifact) => sum + artifact.jsonEntryCount,
          0,
        ),
        queryTablePath: queryResult.parquetPath,
      },
      null,
      2,
    )}\n`,
    { mode: 0o600 },
  );
  return {
    artifactCount: artifacts.length,
    lexiconValidCount,
    referenceParityCount,
    queryRowCount: queryResult.rowCount,
    summaryPath,
  };
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  validateQueryDataOnlyArtifacts(parseValidationOptions(process.argv.slice(2)))
    .then((result) => {
      console.log(
        JSON.stringify({
          level: "info",
          message: "broward_query_data_only_validation_complete",
          ...result,
        }),
      );
    })
    .catch((error) => {
      console.error(error instanceof Error ? error.message : String(error));
      process.exitCode = 1;
    });
}
