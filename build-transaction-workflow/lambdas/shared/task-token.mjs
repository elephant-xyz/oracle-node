import {
  SFNClient,
  SendTaskSuccessCommand,
  SendTaskFailureCommand,
} from "@aws-sdk/client-sfn";

const sfnClient = new SFNClient({});

/**
 * @typedef {object} TaskSuccessParams
 * @property {string} taskToken - Step Functions task token.
 * @property {unknown} output - Output payload to return to the workflow.
 */

/**
 * @typedef {object} TaskFailureParams
 * @property {string} taskToken - Step Functions task token.
 * @property {string} error - Error code/name.
 * @property {string} cause - Human-readable error message.
 */

/**
 * Send task success callback to Step Functions.
 *
 * @param {TaskSuccessParams} params - Success parameters.
 * @returns {Promise<void>}
 */
export async function sendTaskSuccess({ taskToken, output }) {
  await sfnClient.send(
    new SendTaskSuccessCommand({
      taskToken,
      output: JSON.stringify(output),
    }),
  );
}

/**
 * Send task failure callback to Step Functions.
 *
 * @param {TaskFailureParams} params - Failure parameters.
 * @returns {Promise<void>}
 */
export async function sendTaskFailure({ taskToken, error, cause }) {
  await sfnClient.send(
    new SendTaskFailureCommand({
      taskToken,
      error,
      cause: cause.substring(0, 32768), // SFN has a 32KB limit on cause
    }),
  );
}

/**
 * Wrapper to execute a worker function and handle task token callbacks.
 *
 * @template T
 * @param {object} params - Execution parameters.
 * @param {string} params.taskToken - Step Functions task token.
 * @param {() => Promise<T>} params.workerFn - Worker function to execute.
 * @param {(level: "info"|"error"|"debug", msg: string, details?: Record<string, unknown>) => void} params.log - Structured logger.
 * @returns {Promise<void>}
 */
export async function executeWithTaskToken({ taskToken, workerFn, log }) {
  try {
    const result = await workerFn();
    log("info", "task_success", { taskToken: taskToken.substring(0, 50) + "..." });
    await sendTaskSuccess({ taskToken, output: result });
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    log("error", "task_failure", {
      taskToken: taskToken.substring(0, 50) + "...",
      error: error.name,
      cause: error.message,
    });
    await sendTaskFailure({
      taskToken,
      error: error.name || "WorkerError",
      cause: error.message,
    });
  }
}
