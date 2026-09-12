#!/usr/bin/env node

import {
  mkdir,
  readFile,
  writeFile,
} from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const COUNTIES = [
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
    if (!token?.startsWith("--")) throw new Error(`Unexpected argument: ${token}`);
    const name = token.slice(2);
    const value = argv[index + 1];
    if (!value || value.startsWith("--")) throw new Error(`Missing value for --${name}`);
    values.set(name, value);
    index += 1;
  }
  const phase = values.get("phase");
  if (!["base", "hoa-pm"].includes(phase)) {
    throw new Error("--phase must be base or hoa-pm");
  }
  return {
    phase,
    envFile: values.get("env-file") ?? path.join(ROOT, ".env"),
    runtimeDir:
      values.get("runtime-dir") ??
      path.join(ROOT, "..", "soofi-xyz-team-kit", "skills", "use-oracle", "runtime"),
  };
}

function artifactPaths(county, phase) {
  const base = path.join(ROOT, "data", "artifacts", "publish", county);
  const suffix = phase === "hoa-pm" ? "-hoa-pm" : "";
  const directory = phase === "hoa-pm" ? path.join(base, "hoa-pm") : base;
  return {
    county,
    parquetPath: path.join(directory, "query-table.parquet"),
    coveragePath: path.join(directory, "dataset-coverage.json"),
    ...(phase === "hoa-pm"
      ? {
          hoaPmObjectsPath: path.join(
            directory,
            "objects",
            "hoa-pm-objects.jsonl",
          ),
        }
      : {}),
    queryTableIpnsLabel: `oracle-query-table-${county}${suffix}`,
    coverageIpnsLabel: `oracle-dataset-coverage-${county}${suffix}`,
  };
}

async function fetchWithRetry(url, options = {}, attempts = 8) {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const response = await fetch(url, options);
      if (response.ok) return response;
      lastError = new Error(`${url} returned HTTP ${response.status}`);
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, attempt * 1_500));
  }
  throw lastError;
}

async function verifyPublishedResult(county, result) {
  const queryResponse = await fetchWithRetry(
    `https://ipfs.filebase.io/ipfs/${result.queryTableCid}`,
    { headers: { Range: "bytes=0-3" } },
  );
  const queryPrefix = Buffer.from(await queryResponse.arrayBuffer()).subarray(0, 4);
  if (queryPrefix.toString("utf8") !== "PAR1") {
    throw new Error(`${county}: published query-table CID is not Parquet`);
  }
  const coverageResponse = await fetchWithRetry(
    `https://ipfs.filebase.io/ipfs/${result.coverageCid}`,
  );
  const coverage = await coverageResponse.json();
  if (coverage.county !== county) {
    throw new Error(`${county}: published coverage CID has wrong county`);
  }
  return {
    queryTableCidVerified: true,
    coverageCidVerified: true,
  };
}

async function main() {
  const options = parseOptions(process.argv.slice(2));
  const runtime = path.resolve(options.runtimeDir);
  const [{ loadEnvFile, publishFilebase }, { QUERY_TABLE_BUCKET }] =
    await Promise.all([
      import(pathToFileURL(path.join(runtime, "src", "core", "filebase.mjs"))),
      import(
        pathToFileURL(
          path.join(runtime, "src", "core", "query-table-publication.mjs"),
        )
      ),
    ]);
  await loadEnvFile(path.resolve(options.envFile), process.env);

  const receiptRoot = path.join(
    ROOT,
    "data",
    "artifacts",
    "publication-receipts",
    options.phase,
  );
  await mkdir(receiptRoot, { recursive: true });
  const results = [];
  for (const county of COUNTIES) {
    const artifacts = {
      ...artifactPaths(county, options.phase),
      bucket: QUERY_TABLE_BUCKET,
    };
    const result = await publishFilebase(artifacts, {
      dryRun: false,
      approvalManifestPath: path.join(
        ROOT,
        "data",
        "artifacts",
        "publication-review",
        options.phase,
        "approved",
        `${county}.json`,
      ),
      ...(options.phase === "hoa-pm"
        ? { receiptPath: path.join(receiptRoot, `${county}.json`) }
        : {}),
      env: process.env,
      objectConcurrency: 8,
      skipIpns: true,
    });
    const verification = await verifyPublishedResult(county, result);
    const entry = { county, ...result, ...verification };
    results.push(entry);
    await writeFile(
      path.join(receiptRoot, "batch-results.json"),
      `${JSON.stringify(
        {
          phase: options.phase,
          status: "IN_PROGRESS",
          completedCountyCount: results.length,
          results,
        },
        null,
        2,
      )}\n`,
    );
    console.log(JSON.stringify(entry));
  }
  const report = {
    phase: options.phase,
    status: "PASS",
    completedCountyCount: results.length,
    results,
  };
  await writeFile(
    path.join(receiptRoot, "batch-results.json"),
    `${JSON.stringify(report, null, 2)}\n`,
  );
  console.log(JSON.stringify(report, null, 2));
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.stack : error);
    process.exitCode = 1;
  });
}

export { artifactPaths, parseOptions, verifyPublishedResult };
