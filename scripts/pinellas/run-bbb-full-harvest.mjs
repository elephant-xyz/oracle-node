#!/usr/bin/env node

import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

import { buildPinellasBbbFullHarvestJobs } from "./bbb-harvest-plan.mjs";

/**
 * @typedef {import("./bbb-harvest-plan.mjs").PinellasBbbFullHarvestJob} PinellasBbbFullHarvestJob
 */

const scriptDirectory = path.dirname(fileURLToPath(import.meta.url));
const repositoryRoot = path.resolve(scriptDirectory, "..", "..");
const defaultOutputRoot = path.join(
  repositoryRoot,
  "downloads",
  "pinellas",
  "bbb-harvest",
);

/**
 * Run one shell command and stream stdout/stderr to the caller.
 *
 * @param {string} command Harvest shell command emitted by the plan module.
 * @returns {Promise<void>} Resolves when the child process exits with code 0.
 */
function runShellCommand(command) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, {
      cwd: repositoryRoot,
      env: {
        ...process.env,
        CHROME_EXECUTABLE_PATH:
          process.env.CHROME_EXECUTABLE_PATH ?? "/usr/local/bin/google-chrome",
      },
      shell: true,
      stdio: "inherit",
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(
        new Error(`Harvest command failed with exit code ${code ?? "null"}`),
      );
    });
  });
}

/**
 * Append a log line to the per-job harvest log file.
 *
 * @param {string} logPath Absolute log file path.
 * @param {string} message Log line.
 * @returns {void}
 */
function appendLogLine(logPath, message) {
  fs.appendFileSync(logPath, `${message}\n`, "utf8");
}

/**
 * Run the sequential Pinellas BBB full-paginate harvest jobs.
 *
 * @param {string} [outputRoot=downloads/pinellas/bbb-harvest] Output root.
 * @returns {Promise<void>} Resolves when every job completes successfully.
 */
export async function runPinellasBbbFullHarvest(
  outputRoot = defaultOutputRoot,
) {
  const resolvedOutputRoot = path.resolve(outputRoot);
  const logDirectory = path.join(resolvedOutputRoot, "logs");
  fs.mkdirSync(logDirectory, { recursive: true });

  const jobs = buildPinellasBbbFullHarvestJobs(resolvedOutputRoot);
  for (const job of jobs) {
    const logPath = path.join(
      logDirectory,
      `${job.cityKey}-${job.tradeKey}.log`,
    );
    const startedAt = new Date().toISOString();
    appendLogLine(
      logPath,
      `=== START ${job.cityKey}/${job.tradeKey} ${startedAt} ===`,
    );
    console.log(`=== START ${job.cityKey}/${job.tradeKey} ${startedAt} ===`);
    await runShellCommand(job.command);
    const finishedAt = new Date().toISOString();
    appendLogLine(
      logPath,
      `=== END ${job.cityKey}/${job.tradeKey} ${finishedAt} ===`,
    );
    console.log(`=== END ${job.cityKey}/${job.tradeKey} ${finishedAt} ===`);
  }

  console.log(
    `=== ALL PINELLAS BBB FULL HARVEST JOBS COMPLETE ${new Date().toISOString()} ===`,
  );
}

if (fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  const outputRoot = process.argv[2] ?? defaultOutputRoot;
  runPinellasBbbFullHarvest(outputRoot).catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
