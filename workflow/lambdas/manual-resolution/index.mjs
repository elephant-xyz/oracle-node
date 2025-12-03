/**
 * Manual Resolution Lambda: Allows manual marking of failed executions as resolved
 * Can mark executions as manually fixed and optionally retry them
 */

import { StepFunctionsClient, DescribeExecutionCommand, StartExecutionCommand } from "@aws-sdk/client-sfn";
import { EventBridgeClient, PutEventsCommand } from "@aws-sdk/client-eventbridge";
import { SQSClient, SendMessageCommand, GetQueueUrlCommand } from "@aws-sdk/client-sqs";

/**
 * @typedef {Object} ManualResolutionInput
 * @property {string} executionArn - ARN of the failed execution to mark as resolved
 * @property {boolean} [retry=false] - Whether to retry the execution by sending a new message to the queue
 * @property {string} [reason] - Optional reason for manual resolution
 */

const sfnClient = new StepFunctionsClient({
  region: process.env.AWS_REGION || "us-east-1",
});

const eventBridgeClient = new EventBridgeClient({
  region: process.env.AWS_REGION || "us-east-1",
});

const sqsClient = new SQSClient({
  region: process.env.AWS_REGION || "us-east-1",
});

const base = {
  component: "manual-resolution",
  at: new Date().toISOString(),
};

/**
 * Extract execution ID from ARN
 * @param {string} executionArn - Execution ARN
 * @returns {string} Execution ID
 */
function extractExecutionId(executionArn) {
  const parts = executionArn.split(":");
  return parts[parts.length - 1];
}

/**
 * Extract state machine ARN from execution ARN
 * @param {string} executionArn - Execution ARN
 * @returns {string} State machine ARN
 */
function extractStateMachineArn(executionArn) {
  const parts = executionArn.split(":");
  // Remove the last part (execution name) and join back
  return parts.slice(0, -1).join(":");
}

/**
 * Get execution details from Step Functions
 * @param {string} executionArn - Execution ARN
 * @returns {Promise<Object>} Execution details
 */
async function getExecutionDetails(executionArn) {
  try {
    const cmd = new DescribeExecutionCommand({
      executionArn: executionArn,
    });
    const response = await sfnClient.send(cmd);
    return {
      executionArn: response.executionArn,
      status: response.status,
      startDate: response.startDate,
      stopDate: response.stopDate,
      input: response.input,
      output: response.output,
      cause: response.cause,
      error: response.error,
      stateMachineArn: response.stateMachineArn,
    };
  } catch (err) {
    console.error(
      JSON.stringify({
        ...base,
        level: "error",
        msg: "failed_to_describe_execution",
        executionArn: executionArn,
        error: err instanceof Error ? err.message : String(err),
      }),
    );
    throw err;
  }
}

/**
 * Emit EventBridge event for manual resolution
 * @param {Object} params
 * @param {string} params.executionArn - Execution ARN
 * @param {string} params.status - Resolution status
 * @param {string} [params.reason] - Reason for resolution
 * @returns {Promise<void>}
 */
async function emitResolutionEvent({ executionArn, status, reason }) {
  try {
    const executionId = extractExecutionId(executionArn);
    
    await eventBridgeClient.send(
      new PutEventsCommand({
        Entries: [
          {
            Source: "elephant.workflow",
            DetailType: "WorkflowEvent",
            Detail: JSON.stringify({
              executionId: executionArn,
              status: status,
              phase: "ManualResolution",
              step: "MarkAsResolved",
              taskToken: null,
              errors: [],
              manualResolution: {
                reason: reason || "Manually marked as resolved",
                resolvedAt: new Date().toISOString(),
              },
            }),
          },
        ],
      }),
    );
    
    console.log(
      JSON.stringify({
        ...base,
        level: "info",
        msg: "emitted_resolution_event",
        executionArn: executionArn,
        status: status,
      }),
    );
  } catch (err) {
    console.warn(
      JSON.stringify({
        ...base,
        level: "warn",
        msg: "failed_to_emit_resolution_event",
        executionArn: executionArn,
        error: err instanceof Error ? err.message : String(err),
      }),
    );
    // Don't throw - EventBridge failure shouldn't block manual resolution
  }
}

/**
 * Retry execution by sending original input to workflow queue
 * @param {string} executionInput - Original execution input (JSON string)
 * @param {string} workflowQueueUrl - Workflow SQS queue URL
 * @returns {Promise<void>}
 */
async function retryExecution(executionInput, workflowQueueUrl) {
  try {
    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: workflowQueueUrl,
        MessageBody: executionInput,
      }),
    );
    
    console.log(
      JSON.stringify({
        ...base,
        level: "info",
        msg: "execution_retried",
        queueUrl: workflowQueueUrl,
      }),
    );
  } catch (err) {
    console.error(
      JSON.stringify({
        ...base,
        level: "error",
        msg: "failed_to_retry_execution",
        queueUrl: workflowQueueUrl,
        error: err instanceof Error ? err.message : String(err),
      }),
    );
    throw err;
  }
}

/**
 * Lambda handler
 * @param {ManualResolutionInput} event - Manual resolution input
 * @returns {Promise<Object>} Resolution result
 */
export const handler = async (event) => {
  const { executionArn, retry = false, reason } = event;

  if (!executionArn) {
    throw new Error("executionArn is required");
  }

  try {
    // Get execution details
    console.log(
      JSON.stringify({
        ...base,
        level: "info",
        msg: "resolving_execution",
        executionArn: executionArn,
        retry: retry,
      }),
    );

    const executionDetails = await getExecutionDetails(executionArn);

    // Only allow resolution of failed/timed-out/aborted executions
    const allowedStatuses = ["FAILED", "TIMED_OUT", "ABORTED"];
    if (!allowedStatuses.includes(executionDetails.status)) {
      throw new Error(
        `Execution status is ${executionDetails.status}. Only ${allowedStatuses.join(", ")} executions can be manually resolved.`,
      );
    }

    // Emit resolution event
    await emitResolutionEvent({
      executionArn: executionArn,
      status: "MANUALLY_RESOLVED",
      reason: reason,
    });

    let retryResult = null;
    
    // If retry is requested, send original input to workflow queue
    if (retry) {
      const workflowQueueUrl = process.env.WORKFLOW_SQS_QUEUE_URL;
      
      if (!workflowQueueUrl) {
        throw new Error("WORKFLOW_SQS_QUEUE_URL environment variable is required for retry");
      }

      // Use original input if available, otherwise use empty object
      const originalInput = executionDetails.input || "{}";
      
      await retryExecution(originalInput, workflowQueueUrl);
      
      retryResult = {
        retried: true,
        queueUrl: workflowQueueUrl,
        newExecutionInput: originalInput,
      };
    }

    return {
      success: true,
      executionArn: executionArn,
      originalStatus: executionDetails.status,
      resolvedAt: new Date().toISOString(),
      reason: reason || "Manually marked as resolved",
      retry: retryResult,
    };
  } catch (err) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    
    console.error(
      JSON.stringify({
        ...base,
        level: "error",
        msg: "manual_resolution_failed",
        executionArn: executionArn,
        error: errorMsg,
      }),
    );

    return {
      success: false,
      executionArn: executionArn,
      error: errorMsg,
    };
  }
};

