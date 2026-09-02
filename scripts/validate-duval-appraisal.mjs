#!/usr/bin/env node
/**
 * Duval Task 7: lexicon validate(), geometry bbox, completeness vs static parts,
 * enumerated failures, and ingest-count reconciliation.
 *
 * Usage:
 *   node scripts/validate-duval-appraisal.mjs
 *   node scripts/validate-duval-appraisal.mjs --limit=5 --output=downloads/duval/pilot-run
 */

import {
  mkdtemp,
  mkdir,
  readdir,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { parse as parseCsv } from "csv-parse/sync";
import AdmZip from "adm-zip";
import { transform, validate } from "@elephant-xyz/cli/lib";

import {
  assertGeometryInCounty,
  classifyValidationGap,
  collectGeometryPoints,
  formatValidationIssueLines,
  lexiconFailureNarrative,
  parseDuvalValidateArgs,
  parseStaticPartSelectors,
  reconcileIngestManifest,
  scoreLabeledFieldCoverage,
} from "./duval/validate-lib.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "..");

/**
 * @param {string} csvText
 * @returns {string[]}
 */
function parseValidationIssues(csvText) {
  if (!csvText.trim()) return [];
  /** @type {Array<Record<string, string>>} */
  const rows = parseCsv(csvText, {
    columns: true,
    skip_empty_lines: true,
  });
  return [
    ...new Set(
      rows
        .map((row) => row.error_message ?? "")
        .filter((message) => message.length > 0),
    ),
  ];
}

/**
 * @param {string} parcelDir
 * @returns {Promise<Array<{ latitude: number; longitude: number }>>}
 */
async function loadGeometryPoints(parcelDir) {
  const dataDir = join(parcelDir, "data");
  /** @type {string[]} */
  let names = [];
  try {
    names = await readdir(dataDir);
  } catch {
    return [];
  }
  /** @type {Array<{ latitude: number; longitude: number }>} */
  const points = [];
  for (const name of names) {
    if (!name.startsWith("geometry") || !name.endsWith(".json")) continue;
    const record = JSON.parse(await readFile(join(dataDir, name), "utf8"));
    points.push(...collectGeometryPoints(record));
  }
  return points;
}

/**
 * @param {string} zipPath
 * @returns {string}
 */
function zipJsonBlob(zipPath) {
  const zip = new AdmZip(zipPath);
  return zip
    .getEntries()
    .filter((entry) => entry.entryName.endsWith(".json"))
    .map((entry) => entry.getData().toString("utf8"))
    .join("\n");
}

/**
 * @param {string} scriptsDirectory
 * @param {string} destination
 */
function packageScripts(scriptsDirectory, destination) {
  const zip = new AdmZip();
  zip.addLocalFolder(scriptsDirectory);
  zip.writeZip(destination);
}

/**
 * Elephant CLI validate() looks for lexicon files at the zip root (or a single
 * property folder). Task 6 stores them under `data/`.
 *
 * @param {string} sourceZipPath
 * @param {string} destinationZipPath
 */
function flattenDataPrefix(sourceZipPath, destinationZipPath) {
  const source = new AdmZip(sourceZipPath);
  const dest = new AdmZip();
  for (const entry of source.getEntries()) {
    if (entry.isDirectory) continue;
    const name = entry.entryName.replace(/^data\//, "");
    dest.addFile(name, entry.getData());
  }
  dest.writeZip(destinationZipPath);
}

/**
 * @param {string} parcelDir
 * @param {string} destination
 */
function writePreparedInputZip(parcelDir, destination) {
  const zip = new AdmZip();
  for (const name of [
    "input.html",
    "unnormalized_address.json",
    "property_seed.json",
    "seed.csv",
  ]) {
    zip.addLocalFile(join(parcelDir, name));
  }
  zip.writeZip(destination);
}

/**
 * @param {ReturnType<typeof parseDuvalValidateArgs>} options
 */
export async function runDuvalAppraisalValidation(options) {
  const startedAt = Date.now();
  const pilotRoot = resolve(ROOT, options.pilotRoot);
  const reportPath = resolve(ROOT, options.reportPath);
  const staticPartsPath = resolve(ROOT, options.staticPartsPath);
  const manifestPath = join(pilotRoot, "pilot-manifest.json");
  const validationDir = join(pilotRoot, "_validation");
  await mkdir(validationDir, { recursive: true });

  const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
  reconcileIngestManifest(manifest, 50);

  const staticSelectors = parseStaticPartSelectors(
    await readFile(staticPartsPath, "utf8"),
  );
  const scriptsDirectory = resolve(
    ROOT,
    "../Counties-trasform-scripts/duval/scripts",
  );
  const scriptsZipPath = join(validationDir, "duval-scripts.zip");
  packageScripts(scriptsDirectory, scriptsZipPath);

  const entries = await readdir(pilotRoot, { withFileTypes: true });
  const parcelDirs = entries
    .filter((entry) => entry.isDirectory() && !entry.name.startsWith("_"))
    .map((entry) => entry.name)
    .sort();
  const selected =
    options.limit === null ? parcelDirs : parcelDirs.slice(0, options.limit);

  /** @type {Array<Record<string, unknown>>} */
  const parcelResults = [];
  /** @type {Array<Record<string, unknown>>} */
  const failures = [];

  let captures = 0;
  let transforms = 0;
  let lexiconPassed = 0;
  let geometryPassed = 0;
  let completenessSum = 0;
  let completenessCount = 0;

  for (const folio of selected) {
    const parcelDir = join(pilotRoot, folio);
    const zipPath = join(parcelDir, "transformed_output.zip");
    const htmlPath = join(parcelDir, "input.html");
    /** @type {Record<string, unknown>} */
    const row = { folio, zipPath };
    try {
      const html = await readFile(htmlPath, "utf8");
      captures += 1;
      const existingZip = new AdmZip(zipPath);
      if (existingZip.getEntries().length === 0) {
        throw new Error("transformed_output.zip is empty");
      }
      transforms += 1;
      const coverage = scoreLabeledFieldCoverage(
        html,
        staticSelectors,
        zipJsonBlob(zipPath),
      );
      row.completeness = coverage;
      completenessSum += coverage.ratio;
      completenessCount += 1;

      const points = await loadGeometryPoints(parcelDir);
      assertGeometryInCounty(points);
      geometryPassed += 1;
      row.geometryPoints = points.length;

      const workDir = await mkdtemp(join(tmpdir(), `duval-validate-${folio}-`));
      try {
        const preparedPath = join(workDir, "prepared-input.zip");
        const cliTransformPath = join(workDir, "cli-transformed.zip");
        const validateZipPath = join(workDir, "validate.zip");
        writePreparedInputZip(parcelDir, preparedPath);
        const transformResult = await transform({
          inputZip: preparedPath,
          outputZip: cliTransformPath,
          scriptsZip: scriptsZipPath,
          cwd: workDir,
        });
        if (!transformResult.success) {
          throw new Error(
            transformResult.error ??
              transformResult.scriptFailure?.stderr ??
              "elephant-cli transform failed",
          );
        }
        flattenDataPrefix(cliTransformPath, validateZipPath);
        const csvPath = join(validationDir, `${folio}-validation.csv`);
        const lexicon = await validate({
          input: validateZipPath,
          outputCsv: csvPath,
          cwd: validationDir,
        });
        const issues = parseValidationIssues(
          await readFile(csvPath, "utf8").catch(() => ""),
        );
        row.lexiconSuccess = lexicon.success;
        row.lexiconIssues = issues;
        if (lexicon.success) {
          lexiconPassed += 1;
          row.ok = true;
        } else {
          const classified = issues.map((issue) => ({
            issue,
            class: classifyValidationGap(issue),
          }));
          row.ok = false;
          row.lexiconError = lexicon.error ?? "lexicon validation failed";
          failures.push({
            folio,
            error: row.lexiconError,
            missingArtifacts: [],
            issues: classified,
          });
        }
      } finally {
        await rm(workDir, { recursive: true, force: true });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      row.ok = false;
      row.error = message;
      failures.push({
        folio,
        error: message,
        missingArtifacts: message.includes("geometry") ? ["geometry.json"] : [],
        class: classifyValidationGap(message),
      });
    }
    parcelResults.push(row);
  }

  const elapsedMs = Date.now() - startedAt;
  const validation = {
    generatedAt: new Date().toISOString(),
    elapsedMs,
    captures: `${captures}/${selected.length}`,
    transforms: `${transforms}/${selected.length}`,
    lexicon: `${lexiconPassed}/${selected.length}`,
    geometry: `${geometryPassed}/${selected.length}`,
    meanCompleteness:
      completenessCount === 0 ? 0 : completenessSum / completenessCount,
    completenessNote:
      "@elephant-xyz/cli 1.58.1 does not export mirrorValidate; completeness is labeled-field coverage after subtracting source-html-static-parts/duval.csv chrome.",
    failures,
    parcels: parcelResults,
  };

  manifest.validation = validation;
  await writeFile(manifestPath, JSON.stringify(manifest, null, 2), "utf8");
  await writeFile(
    join(validationDir, "summary.json"),
    JSON.stringify(validation, null, 2),
    "utf8",
  );

  const issueLines = formatValidationIssueLines(failures, selected.length);
  const lexiconNote = lexiconFailureNarrative({
    lexiconPassed,
    selectedCount: selected.length,
    meanCompleteness: validation.meanCompleteness,
  });

  const report = `# Duval appraisal transform validation

Date: ${new Date().toISOString().slice(0, 10)}
County key: \`duval\`
FIPS: \`12031\`

## Result

Pilot ingest from Task 6 was validated in place against lexicon schema, geometry
bbox, labeled-field completeness, enumerated failures, and reconciled ingest counts.

- Fresh COJ captures: **${captures}/${selected.length}**
- County transforms (existing \`transformed_output.zip\`): **${transforms}/${selected.length}**
- CLI Lexicon validations: **${lexiconPassed}/${selected.length}**
- Geometry inside Duval (lat 30.103–30.586, lng −82.05…−81.318): **${geometryPassed}/${selected.length}**
- Mean labeled-field completeness: **${((validation.meanCompleteness ?? 0) * 100).toFixed(1)}%**
- Wall time: **${(elapsedMs / 1000).toFixed(2)} seconds** for ${selected.length} parcels
- Ingest reconciliation: seedRows == attempted == success + failures (see \`pilot-manifest.json\`)

${validation.completenessNote}

${lexiconNote}
Durable local evidence:

- \`downloads/duval/pilot-run/pilot-manifest.json\`
- \`downloads/duval/pilot-run/_validation/summary.json\`
- \`downloads/duval/pilot-run/_validation/<folio>-validation.csv\`

## Issue list

${issueLines}

## Gap classes

- **extractor** — transform script did not emit a required artifact or emitted invalid JSON
- **capture** — a labeled COJ field was on the page but not in transform JSON
- **lexicon** — schema/enum has no home for a captured value

Extractor and capture bugs belong in \`Counties-trasform-scripts\`. Lexicon gaps stay in
the payload and are logged here; they are not dropped.
`;

  await writeFile(reportPath, report, "utf8");
  return { manifestPath, reportPath, validation };
}

async function main() {
  const options = parseDuvalValidateArgs(process.argv.slice(2));
  const result = await runDuvalAppraisalValidation(options);
  console.log(
    JSON.stringify({
      event: "duval_validation_complete",
      ...result.validation,
      manifestPath: result.manifestPath,
      reportPath: result.reportPath,
    }),
  );
}

const isMain =
  process.argv[1] &&
  fileURLToPath(import.meta.url) === resolve(process.argv[1]);
if (isMain) {
  main().catch((error) => {
    console.error(
      error instanceof Error ? (error.stack ?? error.message) : error,
    );
    process.exitCode = 1;
  });
}
