/**
 * Transaction Status Checker Lambda: Checks transaction status on blockchain
 * - If succeeded with block number → succeed
 * - If response but no block number → wait and retry (configurable, default 5 minutes)
 * - If empty/null → transaction dropped, trigger resubmission
 * Supports both SQS events (with task token) and direct invocation
 */

import { checkTransactionStatus } from "@elephant-xyz/cli/lib";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import {
  executeWithTaskToken,
  emitWorkflowEvent,
  createWorkflowError,
  createLogger,
} from "shared";

/**
 * @typedef {Object} TransactionStatusResult
 * @property {string} transactionHash - Transaction hash
 * @property {string} status - "success", "pending", "failed", or "not found"
 * @property {number} [blockNumber] - Block number if mined
 * @property {string} [gasUsed] - Gas used if mined
 * @property {string} [error] - Error message if failed
 */

/**
 * @typedef {Object} TransactionStatusCheckerInput
 * @property {string} transactionHash - Transaction hash to check
 * @property {string} rpcUrl - RPC URL for blockchain
 * @property {number} [waitMinutes] - Minutes to wait before retry if pending (default: 5)
 * @property {string} [resubmitQueueUrl] - SQS queue URL for resubmission if transaction dropped
 * @property {Object} [originalTransactionItems] - Original transaction items for resubmission
 */

const sqsClient = new SQSClient({
  region: process.env.AWS_REGION || "us-east-1",
});

const base = {
  component: "transaction-status-checker",
  at: new Date().toISOString(),
};

/**
 * @typedef {Object} SqsRecord
 * @property {string} body
 * @property {Object} [messageAttributes]
 * @property {Object} [messageAttributes.TaskToken]
 * @property {string} [messageAttributes.TaskToken.stringValue]
 * @property {Object} [messageAttributes.ExecutionArn]
 * @property {string} [messageAttributes.ExecutionArn.stringValue]
 * @property {Object} [messageAttributes.County]
 * @property {string} [messageAttributes.County.stringValue]
 * @property {Object} [messageAttributes.TransactionHash]
 * @property {string} [messageAttributes.TransactionHash.stringValue]
 */

/**
 * Sleep for specified milliseconds
 * @param {number} ms - Milliseconds to sleep
 * @returns {Promise<void>}
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Resubmit transaction items to the submit queue
 * @param {string} queueUrl - SQS queue URL for resubmission
 * @param {Object[]} transactionItems - Transaction items to resubmit
 * @returns {Promise<void>}
 */
async function resubmitTransaction(queueUrl, transactionItems) {
  try {
    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(transactionItems),
      }),
    );
    console.log(
      JSON.stringify({
        ...base,
        level: "info",
        msg: "transaction_resubmitted",
        queueUrl: queueUrl,
        itemCount: transactionItems.length,
      }),
    );
  } catch (err) {
    console.error(
      JSON.stringify({
        ...base,
        level: "error",
        msg: "failed_to_resubmit_transaction",
        error: err instanceof Error ? err.message : String(err),
      }),
    );
    throw err;
  }
}

/**
 * Check transaction status and handle retries/resubmission
 * @param {TransactionStatusCheckerInput} input
 * @returns {Promise<TransactionStatusResult>}
 */
async function checkAndWaitForTransactionStatus(input) {
  const {
    transactionHash,
    rpcUrl,
    waitMinutes = 5,
    resubmitQueueUrl,
    originalTransactionItems,
  } = input;

  if (!rpcUrl) {
    throw new Error("RPC URL is required");
  }

  if (!transactionHash) {
    throw new Error("Transaction hash is required");
  }

  const waitMs = waitMinutes * 60 * 1000;
  let retries = 0;

  // Retry forever until transaction is confirmed or failed
  while (true) {
    try {
      console.log(
        JSON.stringify({
          ...base,
          level: "info",
          msg: "checking_transaction_status",
          transactionHash: transactionHash,
          retry: retries,
        }),
      );

      const results = await checkTransactionStatus({
        transactionHashes: transactionHash,
        rpcUrl: rpcUrl,
      });

      if (!results || results.length === 0) {
        // Transaction not found - likely dropped
        console.log(
          JSON.stringify({
            ...base,
            level: "warn",
            msg: "transaction_not_found_dropped",
            transactionHash: transactionHash,
          }),
        );

        // If resubmit queue is provided, resubmit the transaction
        if (resubmitQueueUrl && originalTransactionItems) {
          await resubmitTransaction(resubmitQueueUrl, originalTransactionItems);
          throw new Error(
            `Transaction ${transactionHash} was dropped and has been resubmitted for processing`,
          );
        } else {
          // If no resubmit queue, wait and retry (transaction might appear later)
          console.log(
            JSON.stringify({
              ...base,
              level: "warn",
              msg: "transaction_not_found_waiting",
              transactionHash: transactionHash,
              waitMinutes: waitMinutes,
              retry: retries,
            }),
          );
          retries++;
          await sleep(waitMs);
          continue;
        }
      }

      const result = results[0];
      const status = result.status || "not found";
      const blockNumber = result.blockNumber;

      console.log(
        JSON.stringify({
          ...base,
          level: "info",
          msg: "transaction_status_retrieved",
          transactionHash: transactionHash,
          status: status,
          blockNumber: blockNumber,
          gasUsed: result.gasUsed,
        }),
      );

      // If transaction succeeded and has block number, return success
      if (status === "success" && blockNumber) {
        return {
          transactionHash: transactionHash,
          status: "success",
          blockNumber: blockNumber,
          gasUsed: result.gasUsed,
        };
      }

      // If transaction failed, throw error (this is a final failure, don't retry)
      if (status === "failed") {
        throw new Error(
          `Transaction ${transactionHash} failed on blockchain. Block: ${blockNumber || "N/A"}`,
        );
      }

      // If pending or no block number, wait and retry forever
      retries++;
      console.log(
        JSON.stringify({
          ...base,
          level: "warn",
          msg: "transaction_pending_waiting",
          transactionHash: transactionHash,
          status: status,
          waitMinutes: waitMinutes,
          retry: retries,
        }),
      );

      await sleep(waitMs);
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err);

      // If it's a resubmission error or transaction failed, throw (don't retry)
      if (
        errorMsg.includes("resubmitted") ||
        errorMsg.includes("failed on blockchain")
      ) {
        throw err;
      }

      // Configuration errors should also throw
      if (
        errorMsg.includes("RPC URL is required") ||
        errorMsg.includes("Transaction hash is required")
      ) {
        throw err;
      }

      // Otherwise wait and retry forever
      console.error(
        JSON.stringify({
          ...base,
          level: "error",
          msg: "transaction_status_check_failed",
          error: errorMsg,
          retry: retries,
        }),
      );

      retries++;
      await sleep(waitMs);
    }
  }
}

/**
 * Lambda handler - supports both SQS events (with task token) and direct invocation
 * @param {TransactionStatusCheckerInput | { Records?: SqsRecord[] }} event
 * @returns {Promise<TransactionStatusResult | void>}
 */
export const handler = async (event) => {
  // Check if invoked from SQS (has Records array)
  const isSqsInvocation = !!event.Records && Array.isArray(event.Records);

  let taskToken;
  let executionArn;
  let county;
  let dataGroupLabel = "County"; // Default for Submit phase
  let transactionHash;
  let originalTransactionItems;

  if (isSqsInvocation) {
    // Invoked from SQS (with task token in message attributes for Step Function callback)
    if (!event.Records || event.Records.length === 0) {
      throw new Error("Missing SQS Records");
    }

    const record = event.Records[0];

    // Extract task token from message attributes if present (Step Function mode)
    if (record.messageAttributes?.TaskToken?.stringValue) {
      taskToken = record.messageAttributes.TaskToken.stringValue;
      executionArn = record.messageAttributes.ExecutionArn?.stringValue;
      county = record.messageAttributes.County?.stringValue;
      // @ts-ignore - DataGroupLabel is added in state machine but not in type definition
      dataGroupLabel =
        record.messageAttributes?.DataGroupLabel?.stringValue || "County";
      transactionHash = record.messageAttributes.TransactionHash?.stringValue;

      console.log(
        JSON.stringify({
          ...base,
          level: "info",
          msg: "invoked_from_sqs_with_task_token",
          executionArn: executionArn,
          county: county,
          transactionHash: transactionHash,
          hasTaskToken: !!taskToken,
        }),
      );

      // Parse body for original transaction items (for resubmission if needed)
      try {
        const body = JSON.parse(record.body || "{}");
        if (body.transactionItems) {
          originalTransactionItems = body.transactionItems;
        } else if (Array.isArray(body)) {
          originalTransactionItems = body;
        }
      } catch (parseErr) {
        console.warn(
          JSON.stringify({
            ...base,
            level: "warn",
            msg: "could_not_parse_body_for_resubmission",
            error:
              parseErr instanceof Error ? parseErr.message : String(parseErr),
          }),
        );
      }

      // Emit IN_PROGRESS event to EventBridge when task token is received
      if (taskToken && executionArn) {
        const log = createLogger({
          component: "transaction-status-checker",
          at: new Date().toISOString(),
          county: county || "unknown",
          executionId: executionArn,
        });
        await emitWorkflowEvent({
          executionId: executionArn,
          county: county || "unknown",
          dataGroupLabel: dataGroupLabel,
          status: "IN_PROGRESS",
          phase: "Submit",
          step: "CheckTransactionStatus",
          taskToken: taskToken,
          errors: [],
          log,
        });
      }
    } else {
      // SQS invocation without task token - parse body for transaction hash
      const body = JSON.parse(record.body || "{}");
      transactionHash = body.transactionHash || body.transaction_hash;
      originalTransactionItems =
        body.transactionItems || body.transaction_items;
    }
  } else {
    // Direct invocation - extract from event
    transactionHash = event.transactionHash || event.transaction_hash;
    taskToken = event.taskToken;
    executionArn = event.executionArn;
    county = event.county;
    originalTransactionItems =
      event.transactionItems || event.transaction_items;
  }

  try {
    const rpcUrl = process.env.ELEPHANT_RPC_URL;
    const waitMinutes = parseFloat(
      process.env.TRANSACTION_STATUS_WAIT_MINUTES || "5",
    );
    const resubmitQueueUrl = process.env.RESUBMIT_QUEUE_URL;

    if (!rpcUrl) {
      const error = "RPC URL is required (ELEPHANT_RPC_URL env var)";
      if (taskToken && executionArn) {
        const errorLog = createLogger({
          component: "transaction-status-checker",
          at: new Date().toISOString(),
          county: county || "unknown",
          executionId: executionArn,
        });
        await emitWorkflowEvent({
          executionId: executionArn,
          county: county || "unknown",
          dataGroupLabel: dataGroupLabel,
          status: "FAILED",
          phase: "Submit",
          step: "CheckTransactionStatus",
          taskToken: taskToken,
          errors: [createWorkflowError("60003", { error })],
          log: errorLog,
        });
        await executeWithTaskToken({
          taskToken,
          log: errorLog,
          workerFn: async () => {
            throw new Error(error);
          },
        });
        return;
      }
      throw new Error(error);
    }

    if (!transactionHash) {
      const error = "Transaction hash is required";
      if (taskToken && executionArn) {
        const errorLog = createLogger({
          component: "transaction-status-checker",
          at: new Date().toISOString(),
          county: county || "unknown",
          executionId: executionArn,
        });
        await emitWorkflowEvent({
          executionId: executionArn,
          county: county || "unknown",
          dataGroupLabel: dataGroupLabel,
          status: "FAILED",
          phase: "Submit",
          step: "CheckTransactionStatus",
          taskToken: taskToken,
          errors: [createWorkflowError("60003", { error })],
          log: errorLog,
        });
        await executeWithTaskToken({
          taskToken,
          log: errorLog,
          workerFn: async () => {
            throw new Error(error);
          },
        });
        return;
      }
      throw new Error(error);
    }

    const result = await checkAndWaitForTransactionStatus({
      transactionHash: transactionHash,
      rpcUrl: rpcUrl,
      waitMinutes: waitMinutes,
      resubmitQueueUrl: resubmitQueueUrl,
      originalTransactionItems: originalTransactionItems,
    });

    console.log(
      JSON.stringify({
        ...base,
        level: "info",
        msg: "transaction_status_check_complete",
        result: result,
      }),
    );

    // Emit SUCCEEDED event to EventBridge and send task success
    if (taskToken && executionArn) {
      const log = createLogger({
        component: "transaction-status-checker",
        at: new Date().toISOString(),
        county: county || "unknown",
        executionId: executionArn,
      });
      await emitWorkflowEvent({
        executionId: executionArn,
        county: county || "unknown",
        dataGroupLabel: dataGroupLabel,
        status: "SUCCEEDED",
        phase: "Submit",
        step: "CheckTransactionStatus",
        taskToken: taskToken,
        errors: [],
        log,
      });
      await executeWithTaskToken({
        taskToken,
        log,
        workerFn: async () => result,
      });
    }

    return result;
  } catch (err) {
    const errMessage = err instanceof Error ? err.message : String(err);
    const errCause = err instanceof Error ? err.stack : undefined;

    console.error(
      JSON.stringify({
        ...base,
        level: "error",
        msg: "handler_failed",
        error: errMessage,
        transactionHash: transactionHash,
      }),
    );

    // Emit FAILED event to EventBridge and send task failure
    if (taskToken && executionArn) {
      const errorLog = createLogger({
        component: "transaction-status-checker",
        at: new Date().toISOString(),
        county: county || "unknown",
        executionId: executionArn,
      });
      await emitWorkflowEvent({
        executionId: executionArn,
        county: county || "unknown",
        dataGroupLabel: dataGroupLabel,
        status: "FAILED",
        phase: "Submit",
        step: "CheckTransactionStatus",
        taskToken: taskToken,
        errors: [
          createWorkflowError("60003", {
            error: errMessage,
            cause: errCause,
            transactionHash: transactionHash,
          }),
        ],
        log: errorLog,
      });
      await executeWithTaskToken({
        taskToken,
        log: errorLog,
        workerFn: async () => {
          throw err;
        },
      });
      // Don't throw after sending failure callback - let SQS know Lambda completed
      return;
    }

    // If no task token, throw to trigger SQS redelivery or fail the Lambda
    throw err;
  }
};
