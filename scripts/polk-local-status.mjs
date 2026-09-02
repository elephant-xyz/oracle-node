#!/usr/bin/env node

import * as path from "node:path";
import { fileURLToPath } from "node:url";

import {
  buildPolkLocalParityStatus,
  parsePolkStatusCliOptions,
} from "./polk-local-parity-lib.mjs";
import { writeJsonAtomically } from "./polk-local-appraisal-lib.mjs";

/**
 * Build and optionally persist the evidence-backed Polk lifecycle status.
 *
 * This command performs local reads only. It does not connect to AWS, Neon,
 * Filebase, IPFS/IPNS, Vercel, or GitHub and cannot publish or deploy anything.
 *
 * @param {readonly string[]} argv CLI arguments after the script path.
 * @returns {Promise<import("./polk-local-parity-lib.mjs").PolkLocalParityStatus>} Generated status.
 */
export async function runPolkLocalStatus(argv) {
  const options = parsePolkStatusCliOptions(argv);
  const { status, permitSummary } = await buildPolkLocalParityStatus(options);
  if (options.writeOutput) {
    await Promise.all([
      writeJsonAtomically(options.permitSummaryPath, permitSummary),
      writeJsonAtomically(options.outputPath, status),
    ]);
  }
  process.stdout.write(`${JSON.stringify(status, null, 2)}\n`);
  return status;
}

if (
  process.argv[1] !== undefined &&
  fileURLToPath(import.meta.url) === path.resolve(process.argv[1])
) {
  runPolkLocalStatus(process.argv.slice(2)).catch((caught) => {
    const message = caught instanceof Error ? caught.message : String(caught);
    process.stderr.write(
      `${JSON.stringify({ event: "polk_local_status_failed", error: message })}\n`,
    );
    process.exitCode = 1;
  });
}
