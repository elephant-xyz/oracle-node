/**
 * Process-warm executor for the synchronous Broward county transform scripts.
 *
 * Elephant CLI normally spawns five Node processes for every parcel: four
 * mapping scripts and the final extractor. A long-lived isolated Broward
 * worker already processes only one parcel at a time, so query-data-only mode
 * can execute those unchanged CommonJS entrypoints in that worker and retain
 * Node's warm dependency cache. Calls are serialized because CLI requests the
 * four mapping scripts with `Promise.all`, while `process.chdir` is global.
 *
 * Full publication mode never loads this module. The 50-parcel parity and
 * Lexicon gates prove that this local execution transport does not change the
 * retained JSON contract.
 */

import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

/**
 * @typedef {object} ScriptExecutionResult
 * @property {number} code - Zero for success, negative one for failure.
 * @property {string} stdout - Captured standard output; empty for quiet warm execution.
 * @property {string} stderr - Actionable exception text on failure.
 * @property {NodeJS.Signals | null} signal - Process signal compatibility field.
 * @property {boolean} timedOut - Whether the execution exceeded its limit.
 * @property {number} durationMs - Wall-clock script execution duration.
 * @property {string} script - Absolute executed script path.
 */

/** @type {Promise<void>} */
let executionTail = Promise.resolve();

/**
 * Execute one unchanged CommonJS entrypoint in the current isolated worker.
 *
 * The entry module itself is evicted before every parcel so its top-level
 * extraction runs again. Required libraries stay cached, eliminating process
 * startup and dependency reloads. Broward's accepted scripts are synchronous;
 * elapsed time is still checked after execution and reported with the same
 * result shape as Elephant CLI's child-process runner.
 *
 * @param {string} entryAbsPath - Absolute copied county script entrypoint.
 * @param {readonly string[]} args - Script arguments; Broward expects none.
 * @param {string} cwd - Per-parcel transform working directory.
 * @param {number} timeoutMs - Existing CLI script timeout.
 * @returns {Promise<ScriptExecutionResult>} CLI-compatible execution result.
 */
export function warmExecNode(entryAbsPath, args, cwd, timeoutMs) {
  const queued = executionTail.then(async () => {
    const started = Date.now();
    const priorCwd = process.cwd();
    try {
      if (process.env.BROWARD_QUERY_DATA_ONLY !== "1") {
        throw new Error(
          "Warm Broward script runner used outside data-only mode",
        );
      }
      if (args.length !== 0) {
        throw new Error("Warm Broward scripts do not accept command arguments");
      }
      process.chdir(cwd);
      const requireFromEntry = createRequire(pathToFileURL(entryAbsPath));
      const resolved = requireFromEntry.resolve(entryAbsPath);
      delete requireFromEntry.cache[resolved];
      requireFromEntry(resolved);
      const durationMs = Date.now() - started;
      return {
        code: durationMs > timeoutMs ? -1 : 0,
        stdout: "",
        stderr:
          durationMs > timeoutMs
            ? `[warm-runner] Script exceeded timeout after ${String(durationMs)}ms`
            : "",
        signal: null,
        timedOut: durationMs > timeoutMs,
        durationMs,
        script: entryAbsPath,
      };
    } catch (error) {
      return {
        code: -1,
        stdout: "",
        stderr:
          error instanceof Error
            ? (error.stack ?? error.message)
            : String(error),
        signal: null,
        timedOut: false,
        durationMs: Date.now() - started,
        script: entryAbsPath,
      };
    } finally {
      process.chdir(priorCwd);
    }
  });
  executionTail = queued.then(
    () => undefined,
    () => undefined,
  );
  return queued;
}
