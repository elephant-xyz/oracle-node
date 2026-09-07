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
 * @typedef {object} PinellasBbbFullHarvestRunOptions
 * @property {number} [fromJob=1] 1-based job index to start from (skips earlier jobs).
 */

/**
 * Run the sequential Pinellas BBB full-paginate harvest jobs.
 *
 * @param {string} [outputRoot=downloads/pinellas/bbb-harvest] Output root.
 * @param {PinellasBbbFullHarvestRunOptions} [options] Resume options.
 * @returns {Promise<void>} Resolves when every job completes successfully.
 */
export async function runPinellasBbbFullHarvest(
  outputRoot = defaultOutputRoot,
  options = {},
) {
  const resolvedOutputRoot = path.resolve(outputRoot);
  const logDirectory = path.join(resolvedOutputRoot, "logs");
  fs.mkdirSync(logDirectory, { recursive: true });

  const fromJob = options.fromJob ?? 1;
  if (!Number.isInteger(fromJob) || fromJob < 1) {
    throw new Error(`fromJob must be a positive integer, received ${fromJob}`);
  }

  const jobs = buildPinellasBbbFullHarvestJobs(resolvedOutputRoot).slice(
    fromJob - 1,
  );
  if (jobs.length === 0) {
    throw new Error(
      `No harvest jobs remain after skipping to fromJob=${fromJob}`,
    );
  }
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

/**
 * Parse CLI args for the full harvest runner.
 *
 * @param {readonly string[]} argv Process argv slice after node executable.
 * @returns {{ outputRoot: string, fromJob: number }} Parsed CLI options.
 */
function parseCliArgs(argv) {
  let outputRoot = defaultOutputRoot;
  let fromJob = 1;
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--from-job") {
      const value = argv[index + 1];
      if (value === undefined) {
        throw new Error("--from-job requires a 1-based job index");
      }
      fromJob = Number.parseInt(value, 10);
      index += 1;
      continue;
    }
    if (!arg.startsWith("-") && outputRoot === defaultOutputRoot) {
      outputRoot = arg;
    }
  }
  return { outputRoot, fromJob };
}

if (fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  try {
    const { outputRoot, fromJob } = parseCliArgs(process.argv.slice(2));
    runPinellasBbbFullHarvest(outputRoot, { fromJob }).catch((error) => {
      console.error(error instanceof Error ? error.message : String(error));
      process.exitCode = 1;
    });
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
