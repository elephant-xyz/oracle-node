/**
 * Starter Lambda: triggered by SQS with BatchSize=1. Starts Standard SFN and waits for full completion.
 * Enforces concurrency limits and throws error on failure to trigger SQS DLQ redelivery.
 */

import {
  SFNClient,
  StartExecutionCommand,
  ListExecutionsCommand,
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

const MAX_RUNNING_EXECUTIONS = parseInt(
  process.env.MAX_RUNNING_EXECUTIONS || "1000",
  10,
);

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
      maxResults: MAX_RUNNING_EXECUTIONS,
    });
    const resp = await sfn.send(cmd);
    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);
    return (
      resp.executions?.filter(
        (execution) =>
          execution.startDate && execution.startDate < fiveMinutesAgo,
      ).length || 0
    );
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
    return MAX_RUNNING_EXECUTIONS;
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

    return {
      status: "ok",
      executionArn: executionArn,
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
