/**
 * Starter Lambda: triggered by SQS with BatchSize=1. Starts Express SFN synchronously.
 * Throws error on step function failure to trigger SQS DLQ redelivery.
 */

import { SFNClient, StartSyncExecutionCommand } from "@aws-sdk/client-sfn";

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
 * @param {SqsEvent} event
 * @returns {Promise<{status:string, executionArn?:string, workflowStatus?:string, error?:string}>}
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
    // Start the Express workflow synchronously
    const cmd = new StartSyncExecutionCommand({
      stateMachineArn: process.env.STATE_MACHINE_ARN,
      input: JSON.stringify({ message: parsed }),
    });
    const resp = await sfn.send(cmd);
    const executionArn = resp.executionArn || "arn not found";

    // Check if the step function execution failed
    if (resp.status === "FAILED" || resp.status === "TIMED_OUT") {
      console.error(
        JSON.stringify({
          ...logBase,
          level: "error",
          msg: "workflow_failed",
          executionArn: executionArn,
          status: resp.status,
          cause: resp.cause,
          error: resp.error,
        }),
      );
      // Throw error to trigger SQS redelivery to DLQ
      throw new Error(
        `Step function execution failed with status: ${resp.status}. Cause: ${resp.cause || "N/A"}`,
      );
    }

    console.log(
      JSON.stringify({
        ...logBase,
        level: "info",
        msg: "completed",
        executionArn: executionArn,
        status: resp.status,
      }),
    );

    return {
      status: "ok",
      executionArn: executionArn,
      workflowStatus: resp.status || "status not found",
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
