/**
 * Starter Lambda: triggered by SQS with BatchSize=1. Starts Standard SFN and waits for full completion.
 * Enforces concurrency limits and throws error on failure to trigger SQS DLQ redelivery.
 */

import {
  SFNClient,
  StartExecutionCommand,
  ListExecutionsCommand,
  DescribeExecutionCommand,
} from "@aws-sdk/client-sfn";

/**
 * @typedef {Object} S3EventRecord
 * @property {{ name: string }} s3.bucket
 * @property {{ key: string }} s3.object
 */

/**
 * @typedef {Object} SqsEvent
 * @property {{ body: string }[]} Records
 */

const sfn = new SFNClient({});

/**
 * Check how many Step Function executions are currently running
 * @param {string} stateMachineArn
 * @returns {Promise<number>}
 */
async function getRunningExecutionCount(stateMachineArn) {
  try {
    const cmd = new ListExecutionsCommand({
      stateMachineArn: stateMachineArn,
      statusFilter: "RUNNING",
      maxResults: 1000, // Maximum allowed
    });
    const resp = await sfn.send(cmd);
    return resp.executions?.length || 0;
  } catch (err) {
    console.error(
      JSON.stringify({
        component: "starter",
        level: "error",
        msg: "failed_to_check_running_executions",
        error: String(err),
      }),
    );
    // If we can't check, assume we're at limit to be safe
    return 1000;
  }
}

/**
 * Wait for Step Function execution to complete by polling
 * @param {string} executionArn
 * @param {number} maxWaitSeconds - Maximum time to wait in seconds
 * @param {Object} logBase - Base logging object
 * @returns {Promise<{status: string, cause?: string, error?: string}>}
 */
async function waitForCompletion(executionArn, maxWaitSeconds, logBase) {
  const startTime = Date.now();
  const maxWaitMs = maxWaitSeconds * 1000;
  let pollInterval = 2000; // Start with 2 seconds
  const maxPollInterval = 10000; // Max 10 seconds between polls

  while (true) {
    const elapsed = Date.now() - startTime;
    if (elapsed >= maxWaitMs) {
      throw new Error(
        `Step Function execution did not complete within ${maxWaitSeconds} seconds. Execution ARN: ${executionArn}`,
      );
    }

    try {
      const describeCmd = new DescribeExecutionCommand({
        executionArn: executionArn,
      });
      const describeResp = await sfn.send(describeCmd);
      const status = describeResp.status;

      // If execution completed (success or failure), return
      if (status === "SUCCEEDED") {
        console.log(
          JSON.stringify({
            ...logBase,
            level: "info",
            msg: "execution_completed_successfully",
            executionArn: executionArn,
            status: status,
            durationSeconds: Math.round(elapsed / 1000),
          }),
        );
        return { status: "SUCCEEDED" };
      }

      if (
        status === "FAILED" ||
        status === "TIMED_OUT" ||
        status === "ABORTED"
      ) {
        console.error(
          JSON.stringify({
            ...logBase,
            level: "error",
            msg: "execution_failed",
            executionArn: executionArn,
            status: status,
            cause: describeResp.cause,
            error: describeResp.error,
            durationSeconds: Math.round(elapsed / 1000),
          }),
        );
        return {
          status: status,
          cause: describeResp.cause,
          error: describeResp.error,
        };
      }

      // Still running - log progress and continue polling
      if (status === "RUNNING") {
        const remainingSeconds = Math.round((maxWaitMs - elapsed) / 1000);
        if (elapsed % 30000 < pollInterval) {
          // Log every ~30 seconds
          console.log(
            JSON.stringify({
              ...logBase,
              level: "info",
              msg: "execution_running",
              executionArn: executionArn,
              elapsedSeconds: Math.round(elapsed / 1000),
              remainingSeconds: remainingSeconds,
            }),
          );
        }
        // Exponential backoff for polling (cap at maxPollInterval)
        await new Promise((resolve) => setTimeout(resolve, pollInterval));
        pollInterval = Math.min(pollInterval * 1.5, maxPollInterval);
        continue;
      }

      // Unknown status
      throw new Error(`Unexpected execution status: ${status}`);
    } catch (err) {
      const errMessage = err instanceof Error ? err.message : String(err);
      if (errMessage.includes("Step Function execution did not complete")) {
        throw err;
      }
      // Retry on DescribeExecution errors
      console.warn(
        JSON.stringify({
          ...logBase,
          level: "warn",
          msg: "describe_execution_error_retrying",
          executionArn: executionArn,
          error: String(err),
        }),
      );
      await new Promise((resolve) => setTimeout(resolve, pollInterval));
    }
  }
}

/**
 * @param {SqsEvent} event
 * @returns {Promise<{status:string, executionArn?:string, workflowStatus?:string}>}
 */
export const handler = async (event) => {
  const logBase = { component: "starter", at: new Date().toISOString() };
  try {
    console.log(
      JSON.stringify({
        ...logBase,
        level: "info",
        msg: "received",
        eventSize: event?.Records?.length ?? 0,
      }),
    );
    if (!process.env.STATE_MACHINE_ARN) {
      throw new Error("STATE_MACHINE_ARN is required");
    }
    if (!event || !Array.isArray(event.Records) || event.Records.length !== 1) {
      throw new Error("Expect exactly one SQS record per invocation");
    }
    console.log("Event is :", event);
    const record = event.Records[0];
    if (!record || !record.body) {
      throw new Error("Missing SQS record body");
    }
    const bodyRaw = record.body;
    const parsed = JSON.parse(bodyRaw);

    // Check current running executions to enforce concurrency limit
    const maxConcurrency = parseInt(
      process.env.MAX_CONCURRENT_EXECUTIONS || "100",
      10,
    );
    const runningCount = await getRunningExecutionCount(
      process.env.STATE_MACHINE_ARN,
    );

    if (runningCount >= maxConcurrency) {
      const errorMsg = `Step Function concurrency limit reached: ${runningCount}/${maxConcurrency} executions running`;
      console.warn(
        JSON.stringify({
          ...logBase,
          level: "warn",
          msg: "concurrency_limit_reached",
          runningCount: runningCount,
          maxConcurrency: maxConcurrency,
        }),
      );
      // Throw error to trigger SQS redelivery (message will be retried later)
      throw new Error(errorMsg);
    }

    // Start the Standard workflow
    const cmd = new StartExecutionCommand({
      stateMachineArn: process.env.STATE_MACHINE_ARN,
      input: JSON.stringify({ message: parsed }),
    });
    const resp = await sfn.send(cmd);
    const executionArn = resp.executionArn || "arn not found";

    console.log(
      JSON.stringify({
        ...logBase,
        level: "info",
        msg: "execution_started",
        executionArn: executionArn,
      }),
    );

    // Wait for full completion
    // Use Lambda timeout minus buffer (30 seconds) as max wait time
    const lambdaTimeoutSeconds = parseInt(
      process.env.AWS_LAMBDA_FUNCTION_TIMEOUT || "900",
      10,
    );
    const maxWaitSeconds = lambdaTimeoutSeconds - 30; // Leave 30 second buffer

    const completionResult = await waitForCompletion(
      executionArn,
      maxWaitSeconds,
      logBase,
    );

    // If execution failed, throw error to trigger SQS redelivery
    if (completionResult.status !== "SUCCEEDED") {
      throw new Error(
        `Step function execution ${completionResult.status}. Cause: ${completionResult.cause || "N/A"}. Error: ${completionResult.error || "N/A"}`,
      );
    }

    return {
      status: "ok",
      executionArn: executionArn,
      workflowStatus: completionResult.status,
    };
  } catch (err) {
    console.error(
      JSON.stringify({
        ...logBase,
        level: "error",
        msg: "failed",
        error: String(err),
      }),
    );
    // Re-throw error to trigger SQS redelivery
    // Message will be retried based on maxReceiveCount and sent to DLQ
    throw err;
  }
};
