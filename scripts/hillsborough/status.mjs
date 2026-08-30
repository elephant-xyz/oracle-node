#!/usr/bin/env node
/**
 * Print Hillsborough local ingest progress for a job id.
 *
 * Usage:
 *   node scripts/hillsborough/status.mjs --job-id=hillsborough-full-2026-08-27
 */

import { readFile, access } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { runStatePaths } from "./run-state.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "../..");
const DEFAULT_OUTPUT = resolve(ROOT, "downloads/hillsborough/pilot-run");

/**
 * @param {string[]} argv
 */
async function main(argv) {
  const jobId =
    argv.find((a) => a.startsWith("--job-id="))?.split("=")[1] ||
    `hillsborough-local-${new Date().toISOString().slice(0, 10)}`;
  const outputRoot = resolve(
    ROOT,
    argv.find((a) => a.startsWith("--output="))?.split("=")[1] ||
      DEFAULT_OUTPUT,
  );
  const paths = runStatePaths(outputRoot, jobId);
  try {
    await access(paths.progressPath);
  } catch {
    console.error(
      JSON.stringify({
        event: "status_missing",
        jobId,
        progressPath: paths.progressPath,
      }),
    );
    process.exitCode = 1;
    return;
  }
  const progress = JSON.parse(await readFile(paths.progressPath, "utf8"));
  let failureLines = 0;
  try {
    const text = await readFile(paths.failuresPath, "utf8");
    failureLines = text.split("\n").filter((l) => l.trim()).length;
  } catch {
    failureLines = 0;
  }
  const done =
    (progress.succeeded || 0) +
    (progress.failed || 0) +
    (progress.skipped || 0);
  const pct =
    progress.seedTotal > 0
      ? Number(((100 * done) / progress.seedTotal).toFixed(2))
      : null;
  console.log(
    JSON.stringify(
      {
        event: "hillsborough_status",
        ...progress,
        done,
        percentComplete: pct,
        failureLedgerLines: failureLines,
        progressPath: paths.progressPath,
        failuresPath: paths.failuresPath,
      },
      null,
      2,
    ),
  );
}

main(process.argv.slice(2)).catch((error) => {
  console.error(
    error instanceof Error ? (error.stack ?? error.message) : error,
  );
  process.exitCode = 1;
});
