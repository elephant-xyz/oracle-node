#!/usr/bin/env node
// @ts-check

/**
 * Bounded supervisor for Broward municipal full enumerators.
 *
 * The underlying runners remain the sole owners of checkpoint validation,
 * source requests, page/query progress, cap ledgers, and private artifacts.
 * This wrapper only waits for an existing checkpoint deadline and starts a
 * finite number of resumptions after timeout/source-error exits.
 */

import { readFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import {
  parseMunicipalPropertyEnumerationOptions,
  runMunicipalPropertyEnumeration,
} from "./run-broward-municipal-property-enumeration.mjs";
import {
  parseMunicipalTypeEnumerationOptions,
  runMunicipalTypeEnumeration,
} from "./run-broward-municipal-record-type-enumeration.mjs";

const MINIMUM_RESTART_DELAY_MS = 1_000;

/**
 * @typedef {ReturnType<typeof parseMunicipalPropertyEnumerationOptions>} MunicipalPropertyEnumerationOptions
 * @typedef {ReturnType<typeof parseMunicipalTypeEnumerationOptions>} MunicipalTypeEnumerationOptions
 *
 * @typedef {object} MunicipalSupervisorBaseOptions
 * @property {number} maxAttempts - Maximum runner invocations in this process.
 * @property {string | null} notBeforeAt - Optional operator-enforced UTC boundary.
 *
 * @typedef {MunicipalSupervisorBaseOptions & {
 *   runnerKind:"property",
 *   runnerOptions:MunicipalPropertyEnumerationOptions
 * }} MunicipalPropertySupervisorOptions
 *
 * @typedef {MunicipalSupervisorBaseOptions & {
 *   runnerKind:"type",
 *   runnerOptions:MunicipalTypeEnumerationOptions
 * }} MunicipalTypeSupervisorOptions
 *
 * @typedef {MunicipalPropertySupervisorOptions | MunicipalTypeSupervisorOptions} MunicipalSupervisorOptions
 *
 * @typedef {object} MunicipalSupervisorCheckpointState
 * @property {"running" | "paused" | "cooling" | "complete"} status - Durable runner state.
 * @property {"source_cap" | "timeout" | "incomplete_pagination" | "source_error" | null} blocker - Safe aggregate blocker.
 * @property {string | null} nextAttemptAt - Runner-owned retry boundary.
 * @property {number} deferredCapCount - Durable property cap-ledger cardinality.
 * @property {number | null} nextQueryIndex - Property plan position when present.
 * @property {number | null} totalQueries - Property plan cardinality when present.
 *
 * @typedef {object} MunicipalSupervisorSummary
 * @property {"complete" | "terminal_blocker" | "attempt_limit"} status - Supervisor outcome.
 * @property {"property" | "type"} runnerKind - Selected runner family.
 * @property {string} jurisdictionKey - Stable public jurisdiction key.
 * @property {number} attempts - Runner invocations made by this supervisor.
 * @property {MunicipalSupervisorCheckpointState["status"]} checkpointStatus - Final durable state.
 * @property {MunicipalSupervisorCheckpointState["blocker"]} blocker - Final safe blocker.
 * @property {string | null} nextAttemptAt - Remaining durable retry boundary.
 */

/**
 * Parse supervisor flags before `--` and delegate all runner flags after it to
 * the runner's strict parser.
 *
 * @param {readonly string[]} argv - CLI arguments after the script path.
 * @returns {MunicipalSupervisorOptions} Validated bounded supervisor options.
 */
export function parseMunicipalSupervisorOptions(argv) {
  const separatorIndex = argv.indexOf("--");
  if (separatorIndex < 0) {
    throw new Error("Municipal supervisor options require a -- separator");
  }
  const supervisorArguments = argv.slice(0, separatorIndex);
  const runnerArguments = argv.slice(separatorIndex + 1);
  const allowed = new Set(["--runner", "--max-attempts", "--not-before"]);
  const values = new Map();
  for (let index = 0; index < supervisorArguments.length; index += 2) {
    const flag = supervisorArguments[index];
    const value = supervisorArguments[index + 1];
    if (
      typeof flag !== "string" ||
      !allowed.has(flag) ||
      typeof value !== "string" ||
      value.startsWith("--") ||
      values.has(flag)
    ) {
      throw new Error(
        "Municipal supervisor options must be unique --flag value pairs",
      );
    }
    values.set(flag, value);
  }
  const runnerKind = values.get("--runner");
  if (runnerKind !== "property" && runnerKind !== "type") {
    throw new Error("--runner must be property or type");
  }
  const maxAttemptsText = values.get("--max-attempts") ?? "12";
  if (!/^\d+$/u.test(maxAttemptsText)) {
    throw new Error("--max-attempts must be an integer");
  }
  const maxAttempts = Number(maxAttemptsText);
  if (
    !Number.isSafeInteger(maxAttempts) ||
    maxAttempts < 1 ||
    maxAttempts > 100
  ) {
    throw new Error("--max-attempts must be between 1 and 100");
  }
  const notBeforeAt = values.get("--not-before") ?? null;
  if (
    notBeforeAt !== null &&
    (!Number.isFinite(Date.parse(notBeforeAt)) || !notBeforeAt.endsWith("Z"))
  ) {
    throw new Error("--not-before must be an ISO UTC timestamp");
  }
  return runnerKind === "property"
    ? {
        runnerKind,
        runnerOptions:
          parseMunicipalPropertyEnumerationOptions(runnerArguments),
        maxAttempts,
        notBeforeAt,
      }
    : {
        runnerKind,
        runnerOptions: parseMunicipalTypeEnumerationOptions(runnerArguments),
        maxAttempts,
        notBeforeAt,
      };
}

/**
 * Run a bounded number of checkpoint-aware municipal resumptions.
 *
 * @param {MunicipalSupervisorOptions} options - Validated supervisor and runner options.
 * @param {{
 *   now?:()=>number,
 *   wait?:(milliseconds:number)=>Promise<void>,
 *   runProperty?:typeof runMunicipalPropertyEnumeration,
 *   runType?:typeof runMunicipalTypeEnumeration
 * }} [dependencies={}] - Injectable clock, wait, and runner functions.
 * @returns {Promise<MunicipalSupervisorSummary>} Aggregate-only terminal outcome.
 */
export async function runMunicipalEnumerationSupervisor(
  options,
  dependencies = {},
) {
  const now = dependencies.now ?? Date.now;
  const wait =
    dependencies.wait ??
    ((milliseconds) =>
      new Promise((resolvePromise) => {
        setTimeout(resolvePromise, milliseconds);
      }));
  const runProperty =
    dependencies.runProperty ?? runMunicipalPropertyEnumeration;
  const runType = dependencies.runType ?? runMunicipalTypeEnumeration;
  const checkpointPath = path.join(
    options.runnerOptions.outputDirectory,
    "checkpoint.private.json",
  );
  let attempts = 0;

  while (attempts < options.maxAttempts) {
    const state = await readSupervisorCheckpoint(
      checkpointPath,
      options.runnerOptions.jurisdictionKey,
    );
    const terminal = terminalSupervisorStatus(state);
    if (terminal !== null) {
      return createSupervisorSummary(options, attempts, state, terminal);
    }
    const deadlineMs = Math.max(
      parseOptionalDeadline(state.nextAttemptAt),
      parseOptionalDeadline(options.notBeforeAt),
    );
    const waitMs = Math.max(0, deadlineMs - now());
    if (waitMs > 0) {
      await wait(waitMs);
      continue;
    }
    if (
      options.notBeforeAt !== null &&
      now() < Date.parse(options.notBeforeAt)
    ) {
      throw new Error("Municipal supervisor resumed before operator boundary");
    }

    attempts += 1;
    if (options.runnerKind === "property") {
      await runProperty(options.runnerOptions);
    } else {
      await runType(options.runnerOptions);
    }

    const postRunState = await readSupervisorCheckpoint(
      checkpointPath,
      options.runnerOptions.jurisdictionKey,
    );
    const postRunTerminal = terminalSupervisorStatus(postRunState);
    if (postRunTerminal !== null) {
      return createSupervisorSummary(
        options,
        attempts,
        postRunState,
        postRunTerminal,
      );
    }
    if (
      postRunState.nextAttemptAt === null ||
      Date.parse(postRunState.nextAttemptAt) <= now()
    ) {
      await wait(MINIMUM_RESTART_DELAY_MS);
    }
  }

  const finalState = await readSupervisorCheckpoint(
    checkpointPath,
    options.runnerOptions.jurisdictionKey,
  );
  return createSupervisorSummary(
    options,
    attempts,
    finalState,
    "attempt_limit",
  );
}

/**
 * Read and validate only the aggregate fields needed for restart decisions.
 * Runner-specific configuration hashes remain mandatory so an unrelated or
 * legacy checkpoint cannot be supervised under a different command.
 *
 * @param {string} checkpointPath - Runner-owned private checkpoint.
 * @param {string} jurisdictionKey - Expected stable jurisdiction key.
 * @returns {Promise<MunicipalSupervisorCheckpointState>} Validated aggregate state.
 */
async function readSupervisorCheckpoint(checkpointPath, jurisdictionKey) {
  const parsed = /** @type {unknown} */ (
    JSON.parse(await readFile(checkpointPath, "utf8"))
  );
  if (
    !isRecord(parsed) ||
    parsed.jurisdictionKey !== jurisdictionKey ||
    typeof parsed.sourceSystem !== "string" ||
    typeof parsed.schemaVersion !== "string" ||
    (!isNonEmptyString(parsed.configurationSha256) &&
      (!isNonEmptyString(parsed.queryPlanSha256) ||
        !isNonEmptyString(parsed.seedSha256))) ||
    !["running", "paused", "cooling", "complete"].includes(
      typeof parsed.status === "string" ? parsed.status : "",
    )
  ) {
    throw new Error("Municipal supervisor checkpoint signature is invalid");
  }
  const blocker =
    parsed.blocker === null
      ? null
      : [
            "source_cap",
            "timeout",
            "incomplete_pagination",
            "source_error",
          ].includes(typeof parsed.blocker === "string" ? parsed.blocker : "")
        ? /** @type {MunicipalSupervisorCheckpointState["blocker"]} */ (
            parsed.blocker
          )
        : undefined;
  if (blocker === undefined) {
    throw new Error("Municipal supervisor checkpoint blocker is invalid");
  }
  const nextAttemptAt =
    parsed.nextAttemptAt === null
      ? null
      : typeof parsed.nextAttemptAt === "string" &&
          Number.isFinite(Date.parse(parsed.nextAttemptAt))
        ? parsed.nextAttemptAt
        : undefined;
  if (nextAttemptAt === undefined) {
    throw new Error("Municipal supervisor checkpoint deadline is invalid");
  }
  if (
    parsed.deferredCapItems !== undefined &&
    !isRecord(parsed.deferredCapItems)
  ) {
    throw new Error("Municipal supervisor cap ledger is invalid");
  }
  const deferredCapItems = isRecord(parsed.deferredCapItems)
    ? Object.keys(parsed.deferredCapItems).length
    : 0;
  return {
    status: /** @type {MunicipalSupervisorCheckpointState["status"]} */ (
      parsed.status
    ),
    blocker,
    nextAttemptAt,
    deferredCapCount: deferredCapItems,
    nextQueryIndex: readOptionalNonNegativeInteger(parsed.nextQueryIndex),
    totalQueries: readOptionalNonNegativeInteger(parsed.totalQueries),
  };
}

/**
 * Identify only genuinely terminal checkpoint outcomes.
 *
 * @param {MunicipalSupervisorCheckpointState} state - Current durable aggregate state.
 * @returns {"complete" | "terminal_blocker" | null} Terminal outcome or null when resumable.
 */
function terminalSupervisorStatus(state) {
  if (state.status === "complete") return "complete";
  if (
    state.status === "paused" &&
    (state.blocker === "incomplete_pagination" ||
      (state.blocker === "source_cap" && state.deferredCapCount === 0))
  ) {
    return "terminal_blocker";
  }
  if (
    state.deferredCapCount > 0 &&
    state.nextQueryIndex !== null &&
    state.totalQueries !== null &&
    state.nextQueryIndex >= state.totalQueries &&
    state.nextAttemptAt === null
  ) {
    return "terminal_blocker";
  }
  return null;
}

/**
 * Build one aggregate-only supervisor summary.
 *
 * @param {MunicipalSupervisorOptions} options - Supervisor configuration.
 * @param {number} attempts - Completed runner invocations.
 * @param {MunicipalSupervisorCheckpointState} state - Final checkpoint state.
 * @param {MunicipalSupervisorSummary["status"]} status - Supervisor outcome.
 * @returns {MunicipalSupervisorSummary} Public-safe aggregate summary.
 */
function createSupervisorSummary(options, attempts, state, status) {
  return {
    status,
    runnerKind: options.runnerKind,
    jurisdictionKey: options.runnerOptions.jurisdictionKey,
    attempts,
    checkpointStatus: state.status,
    blocker: state.blocker,
    nextAttemptAt: state.nextAttemptAt,
  };
}

/**
 * Parse an optional ISO deadline as an epoch, with null meaning no boundary.
 *
 * @param {string | null} value - Optional validated ISO timestamp.
 * @returns {number} Epoch milliseconds or zero.
 */
function parseOptionalDeadline(value) {
  return value === null ? 0 : Date.parse(value);
}

/**
 * Read an optional non-negative checkpoint integer.
 *
 * @param {unknown} value - Unknown checkpoint field.
 * @returns {number | null} Valid integer, or null when absent.
 */
function readOptionalNonNegativeInteger(value) {
  return Number.isSafeInteger(value) && Number(value) >= 0
    ? Number(value)
    : null;
}

/**
 * Test whether an unknown value is a non-empty string.
 *
 * @param {unknown} value - Unknown candidate.
 * @returns {value is string} True for non-empty strings.
 */
function isNonEmptyString(value) {
  return typeof value === "string" && value.length > 0;
}

/**
 * Test whether an unknown value is a plain record.
 *
 * @param {unknown} value - Unknown candidate.
 * @returns {value is Record<string, unknown>} True for object records.
 */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

const isMain =
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href;

if (isMain) {
  const summary = await runMunicipalEnumerationSupervisor(
    parseMunicipalSupervisorOptions(process.argv.slice(2)),
  );
  process.stdout.write(
    `${JSON.stringify({
      event: "broward_municipal_enumeration_supervisor_finished",
      ...summary,
    })}\n`,
  );
}
