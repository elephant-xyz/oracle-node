#!/usr/bin/env node

import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const BUCKET = "elephant-oracle-query-table";
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
  const action = values.get("action") ?? "review";
  if (!["review", "approve"].includes(action)) {
    throw new Error("--action must be review or approve");
  }
  return {
    action,
    phase: values.get("phase") ?? "base",
    approvedBy: values.get("approved-by") ?? null,
    approvedAt: values.get("approved-at") ?? null,
    expectedBatchDigest: values.get("expected-batch-digest") ?? null,
  };
}

function sha256(body) {
  return createHash("sha256").update(body).digest("hex");
}

function integrity(body) {
  return { bytes: body.length, sha256: sha256(body) };
}

function artifactPaths(county, phase) {
  const base = path.join(ROOT, "data", "artifacts", "publish", county);
  if (phase === "base") {
    return {
      queryTable: path.join(base, "query-table.parquet"),
      coverage: path.join(base, "dataset-coverage.json"),
    };
  }
  if (phase === "hoa-pm") {
    const hoaPm = path.join(base, "hoa-pm");
    return {
      queryTable: path.join(hoaPm, "query-table.parquet"),
      coverage: path.join(hoaPm, "dataset-coverage.json"),
      hoaPmObjects: path.join(hoaPm, "objects", "hoa-pm-objects.jsonl"),
    };
  }
  throw new Error("--phase must be base or hoa-pm");
}

async function expectedApproval(county, phase) {
  const paths = artifactPaths(county, phase);
  const entries = await Promise.all(
    Object.entries(paths).map(async ([name, filePath]) => [
      name,
      integrity(await readFile(filePath)),
    ]),
  );
  const suffix = phase === "hoa-pm" ? "-hoa-pm" : "";
  return {
    schemaVersion: "elephant.filebase-publish-approval.v1",
    action:
      phase === "hoa-pm"
        ? "publish-query-table-coverage-and-resolvable-hoa-pm-objects"
        : "publish-query-table-and-coverage",
    county,
    bucket: BUCKET,
    queryTableIpnsLabel: `oracle-query-table-${county}${suffix}`,
    coverageIpnsLabel: `oracle-dataset-coverage-${county}${suffix}`,
    artifacts: Object.fromEntries(entries),
  };
}

async function reviewBatch(phase) {
  const approvals = await Promise.all(
    COUNTIES.map((county) => expectedApproval(county, phase)),
  );
  const reviewed = {
    schemaVersion: "elephant.filebase-publication-review.v1",
    phase,
    publicDataWarning:
      "Query tables contain property addresses and owner names and will be publicly accessible through IPFS.",
    approvals,
  };
  const batchDigest = sha256(Buffer.from(JSON.stringify(reviewed)));
  return { ...reviewed, batchDigest };
}

async function main() {
  const options = parseOptions(process.argv.slice(2));
  const batch = await reviewBatch(options.phase);
  const outputRoot = path.join(
    ROOT,
    "data",
    "artifacts",
    "publication-review",
    options.phase,
  );
  await mkdir(outputRoot, { recursive: true });
  await writeFile(
    path.join(outputRoot, "batch-review.json"),
    `${JSON.stringify(batch, null, 2)}\n`,
  );

  if (options.action === "review") {
    console.log(
      JSON.stringify(
        {
          phase: options.phase,
          countyCount: batch.approvals.length,
          batchDigest: batch.batchDigest,
          publicDataWarning: batch.publicDataWarning,
          reviewPath: path.join(outputRoot, "batch-review.json"),
        },
        null,
        2,
      ),
    );
    return;
  }

  if (
    options.expectedBatchDigest !== batch.batchDigest ||
    !options.approvedBy ||
    !options.approvedAt
  ) {
    throw new Error(
      "Approval requires the exact --expected-batch-digest, --approved-by, and --approved-at",
    );
  }
  if (Number.isNaN(Date.parse(options.approvedAt))) {
    throw new Error("--approved-at must be an ISO-8601 timestamp");
  }
  const approvalDir = path.join(outputRoot, "approved");
  await mkdir(approvalDir, { recursive: true });
  for (const expected of batch.approvals) {
    await writeFile(
      path.join(approvalDir, `${expected.county}.json`),
      `${JSON.stringify(
        {
          ...expected,
          approved: true,
          approvedBy: options.approvedBy,
          approvedAt: options.approvedAt,
        },
        null,
        2,
      )}\n`,
    );
  }
  console.log(
    JSON.stringify(
      {
        phase: options.phase,
        countyCount: batch.approvals.length,
        batchDigest: batch.batchDigest,
        approvalDir,
      },
      null,
      2,
    ),
  );
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.stack : error);
    process.exitCode = 1;
  });
}

export { expectedApproval, integrity, parseOptions, reviewBatch };
