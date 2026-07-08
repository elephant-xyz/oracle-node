#!/usr/bin/env node

/**
 * Enqueue Sunbiz corporate ZIP extraction for all cordata-2026q2 expanded files
 * scoped to Miami-Dade County ZIP prefixes.
 */

import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";

/** @typedef {object} EnqueueConfig
 * @property {string} jobId
 * @property {string} bucket
 * @property {string} zipPrefixesJson
 * @property {string} sourcePrefix
 * @property {string} extractKeyPrefix
 */

/** @type {EnqueueConfig} */
const CONFIG = {
  jobId: "sunbiz-miami-dade-corporate-quarterly-2026q2",
  bucket: "elephant-oracle-node-environmentbucket-mmsoo3xbdi80",
  zipPrefixesJson: path.join(
    path.dirname(fileURLToPath(import.meta.url)),
    "miami-dade-county-zips.json",
  ),
  sourcePrefix:
    "s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-source/quarterly/cor/cordata-2026q2-expanded",
  extractKeyPrefix: "miami-dade-county-zips-cordata-2026q2",
};

/**
 * Enqueue one cordata file extraction job.
 *
 * @param {number} index - Cordata file index (0-9).
 * @returns {{ index: number, status: number | null, stdout: string, stderr: string }}
 */
function enqueueCordataFile(index) {
  const fileName = `cordata${index}.txt`;
  const scriptPath = path.join(
    path.dirname(fileURLToPath(import.meta.url)),
    "enqueue-sunbiz-corporate-zip-extract.mjs",
  );
  const args = [
    scriptPath,
    "--job-id",
    CONFIG.jobId,
    "--extract-key",
    `${CONFIG.extractKeyPrefix}-${fileName.replace(".txt", "")}`,
    "--source-data-s3-uri",
    `${CONFIG.sourcePrefix}/${fileName}`,
    "--source-format",
    "text",
    "--zip-prefixes-json",
    CONFIG.zipPrefixesJson,
  ];
  const result = spawnSync(process.execPath, args, {
    encoding: "utf8",
    env: process.env,
  });
  return {
    index,
    status: result.status,
    stdout: result.stdout ?? "",
    stderr: result.stderr ?? "",
  };
}

/**
 * @returns {void}
 */
function main() {
  /** @type {Array<{ index: number, status: number | null, stdout: string, stderr: string }>} */
  const results = [];
  for (let index = 0; index < 10; index += 1) {
    const result = enqueueCordataFile(index);
    results.push(result);
    process.stdout.write(result.stdout);
    if (result.stderr) process.stderr.write(result.stderr);
  }

  const failed = results.filter((result) => result.status !== 0);
  if (failed.length > 0) {
    console.error(
      JSON.stringify({
        level: "error",
        message: "miami_dade_sunbiz_enqueue_failed",
        failed: failed.map((result) => result.index),
      }),
    );
    process.exit(1);
  }

  console.log(
    JSON.stringify({
      level: "info",
      message: "miami_dade_sunbiz_enqueue_complete",
      jobId: CONFIG.jobId,
      files: 10,
    }),
  );
}

main();
