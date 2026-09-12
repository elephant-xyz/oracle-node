#!/usr/bin/env node

import { createHash } from "node:crypto";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const require = createRequire(import.meta.url);
const { ParquetReader } = require("@dsnp/parquetjs");
const ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../..",
);
const DEFAULT_COUNTIES = [
  "baker",
  "st-lucie",
  "highlands",
  "citrus",
  "nassau",
  "putnam",
  "bradford",
  "columbia",
  "desoto",
  "santa-rosa",
  "martin",
];

function parseOptions(argv) {
  const values = new Map();
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token?.startsWith("--"))
      throw new Error(`Unexpected argument: ${token}`);
    const [name, inlineValue] = token.slice(2).split("=", 2);
    if (inlineValue !== undefined) {
      values.set(name, inlineValue);
      continue;
    }
    const value = argv[index + 1];
    if (!value || value.startsWith("--"))
      throw new Error(`Missing value for --${name}`);
    values.set(name, value);
    index += 1;
  }
  const quarter = values.get("quarter") ?? "2026Q3";
  if (!/^\d{4}Q[1-4]$/.test(quarter)) {
    throw new Error("--quarter must use YYYYQn");
  }
  const counties = (values.get("counties") ?? DEFAULT_COUNTIES.join(","))
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  return {
    counties,
    quarter,
    sunbizSourceDir: values.get("sunbiz-source-dir") ?? null,
    sunbizIndexDir: values.get("sunbiz-index") ?? null,
    runtimeDir:
      values.get("runtime-dir") ??
      process.env.HOA_PM_RUNTIME_PATH ??
      path.join(
        ROOT,
        "..",
        "soofi-xyz-team-kit",
        "skills",
        "use-oracle",
        "runtime",
      ),
  };
}

function sha256(body) {
  return createHash("sha256").update(body).digest("hex");
}

async function readParquetRows(parquetPath) {
  const reader = await ParquetReader.openFile(parquetPath);
  const rows = [];
  try {
    const cursor = reader.getCursor();
    let row = await cursor.next();
    while (row) {
      rows.push(row);
      row = await cursor.next();
    }
  } finally {
    await reader.close();
  }
  return rows;
}

function baseCoverage(county, report) {
  return {
    county,
    exportedAt: report.exportedAt,
    publicationScope: report.publicationScope,
    datasets: [
      {
        county,
        source: "appraisal",
        ingested_count: report.rowCount,
        expected_count: report.expectedCount,
        first_loaded_at: report.exportedAt,
        last_loaded_at: report.exportedAt,
        cid: null,
        ipns_label: null,
      },
    ],
  };
}

async function prepareBaseArtifacts(counties) {
  const prepared = [];
  const subdivisions = new Set();
  for (const county of counties) {
    const directory = path.join(ROOT, "data", "artifacts", "publish", county);
    const report = JSON.parse(
      await readFile(path.join(directory, "export-report.json"), "utf8"),
    );
    const parquetPath = path.join(directory, "query-table.parquet");
    const parquetBody = await readFile(parquetPath);
    if (report.sha256 !== sha256(parquetBody)) {
      throw new Error(
        `${county}: base Parquet hash disagrees with export report`,
      );
    }
    const rows = await readParquetRows(parquetPath);
    if (rows.length !== report.rowCount) {
      throw new Error(
        `${county}: base Parquet row count disagrees with export report`,
      );
    }
    for (const row of rows) {
      const subdivision = String(row.subdivision ?? "").trim();
      if (subdivision) subdivisions.add(subdivision);
    }
    const coveragePath = path.join(directory, "dataset-coverage.json");
    await writeFile(
      coveragePath,
      `${JSON.stringify(baseCoverage(county, report), null, 2)}\n`,
    );
    prepared.push({
      county,
      directory,
      parquetPath,
      coveragePath,
      rowCount: rows.length,
      subdivisionCount: rows.filter((row) => row.subdivision).length,
    });
  }
  return { prepared, subdivisions: [...subdivisions].sort() };
}

async function validateEnrichedCounty({
  county,
  outputDir,
  expectedRows,
  parseHoaPmObjects,
  hoaPmPublicationLayers,
}) {
  const parquetPath = path.join(outputDir, "query-table.parquet");
  const rows = await readParquetRows(parquetPath);
  if (rows.length !== expectedRows) {
    throw new Error(`${county}: HOA/PM Parquet row count mismatch`);
  }
  const statuses = {};
  let hoaCidCount = 0;
  let propertyManagerCidCount = 0;
  for (const row of rows) {
    const status = String(row.hoa_pm_status ?? "");
    if (!status) throw new Error(`${county}: row lacks hoa_pm_status`);
    statuses[status] = (statuses[status] ?? 0) + 1;
    for (const [field, counter] of [
      ["hoa_cid", "hoa"],
      ["property_manager_cid", "propertyManager"],
    ]) {
      const value = row[field];
      if (value == null) continue;
      if (!/^sha256:[a-f0-9]{64}$/.test(value)) {
        throw new Error(`${county}: ${field} is not a local canonical CID`);
      }
      if (counter === "hoa") hoaCidCount += 1;
      else propertyManagerCidCount += 1;
    }
  }
  const objectsPath = path.join(outputDir, "objects", "hoa-pm-objects.jsonl");
  const objects = parseHoaPmObjects(await readFile(objectsPath));
  hoaPmPublicationLayers(objects);
  const parquetBody = await readFile(parquetPath);
  const report = {
    county,
    status: "PASS",
    rowCount: rows.length,
    hoaCidCount,
    propertyManagerCidCount,
    uniqueObjectCount: objects.length,
    statusCounts: statuses,
    parquetBytes: parquetBody.byteLength,
    parquetSha256: sha256(parquetBody),
  };
  await writeFile(
    path.join(outputDir, "hoa-pm-validation.json"),
    `${JSON.stringify(report, null, 2)}\n`,
  );
  return report;
}

async function main() {
  const options = parseOptions(process.argv.slice(2));
  const runtime = path.resolve(options.runtimeDir);
  const [
    { buildHoaPmSunbizIndex },
    { enrichQueryTableWithHoaPm },
    publication,
    { publishFilebase },
    { QUERY_TABLE_BUCKET },
  ] = await Promise.all([
    import(
      pathToFileURL(
        path.join(runtime, "src", "enrichment", "hoa-pm-sunbiz-index.mjs"),
      )
    ),
    import(
      pathToFileURL(
        path.join(runtime, "src", "enrichment", "query-table-hoa-pm.mjs"),
      )
    ),
    import(
      pathToFileURL(
        path.join(
          runtime,
          "src",
          "enrichment",
          "hoa-pm-object-publication.mjs",
        ),
      )
    ),
    import(pathToFileURL(path.join(runtime, "src", "core", "filebase.mjs"))),
    import(
      pathToFileURL(
        path.join(runtime, "src", "core", "query-table-publication.mjs"),
      )
    ),
  ]);
  const { prepared, subdivisions } = await prepareBaseArtifacts(
    options.counties,
  );
  const workRoot = path.join(
    ROOT,
    "data",
    "artifacts",
    "hoa-pm",
    options.quarter,
  );
  await mkdir(workRoot, { recursive: true });
  const subdivisionsBody = Buffer.from(
    `${JSON.stringify(subdivisions, null, 2)}\n`,
  );
  const subdivisionsPath = path.join(workRoot, "subdivisions.json");
  await writeFile(subdivisionsPath, subdivisionsBody);
  let indexDir = options.sunbizIndexDir && path.resolve(options.sunbizIndexDir);
  if (!indexDir) {
    if (!options.sunbizSourceDir) {
      throw new Error("Provide --sunbiz-source-dir or --sunbiz-index");
    }
    indexDir = path.join(
      workRoot,
      `sunbiz-index-${sha256(subdivisionsBody).slice(0, 12)}`,
    );
    await buildHoaPmSunbizIndex({
      sourceDir: path.resolve(options.sunbizSourceDir),
      subdivisionsPath,
      outputDir: indexDir,
      quarter: options.quarter,
    });
  }

  const results = [];
  for (const base of prepared) {
    const outputDir = path.join(base.directory, "hoa-pm");
    const temporaryDir = `${outputDir}.${process.pid}.tmp`;
    await rm(temporaryDir, { recursive: true, force: true });
    await enrichQueryTableWithHoaPm({
      countyKey: base.county,
      inputParquet: base.parquetPath,
      inputCoverage: base.coveragePath,
      sunbizExtractDir: indexDir,
      outputParquet: path.join(temporaryDir, "query-table.parquet"),
      outputCoverage: path.join(temporaryDir, "dataset-coverage.json"),
      objectsDir: path.join(temporaryDir, "objects"),
      manifestPath: path.join(temporaryDir, "hoa-pm-enrichment-manifest.json"),
    });
    const result = await validateEnrichedCounty({
      county: base.county,
      outputDir: temporaryDir,
      expectedRows: base.rowCount,
      parseHoaPmObjects: publication.parseHoaPmObjects,
      hoaPmPublicationLayers: publication.hoaPmPublicationLayers,
    });
    const publicationPlan = await publishFilebase(
      {
        county: base.county,
        parquetPath: path.join(temporaryDir, "query-table.parquet"),
        coveragePath: path.join(temporaryDir, "dataset-coverage.json"),
        hoaPmObjectsPath: path.join(
          temporaryDir,
          "objects",
          "hoa-pm-objects.jsonl",
        ),
        bucket: QUERY_TABLE_BUCKET,
        queryTableIpnsLabel: `oracle-query-table-${base.county}-hoa-pm`,
        coverageIpnsLabel: `oracle-dataset-coverage-${base.county}-hoa-pm`,
      },
      { dryRun: true, env: {} },
    );
    await rm(outputDir, { recursive: true, force: true });
    await rename(temporaryDir, outputDir);
    results.push({
      ...result,
      subdivisionCount: base.subdivisionCount,
      publicationPlan,
    });
  }
  const summary = {
    status: "PASS",
    quarter: options.quarter,
    countyCount: results.length,
    rowCount: results.reduce((sum, result) => sum + result.rowCount, 0),
    subdivisionCount: results.reduce(
      (sum, result) => sum + result.subdivisionCount,
      0,
    ),
    hoaCidCount: results.reduce((sum, result) => sum + result.hoaCidCount, 0),
    propertyManagerCidCount: results.reduce(
      (sum, result) => sum + result.propertyManagerCidCount,
      0,
    ),
    indexDir,
    results,
  };
  await writeFile(
    path.join(workRoot, "enrichment-summary.json"),
    `${JSON.stringify(summary, null, 2)}\n`,
  );
  console.log(JSON.stringify(summary, null, 2));
}

if (
  process.argv[1] &&
  fileURLToPath(import.meta.url) === path.resolve(process.argv[1])
) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.stack : error);
    process.exitCode = 1;
  });
}

export { baseCoverage, parseOptions, validateEnrichedCounty };
