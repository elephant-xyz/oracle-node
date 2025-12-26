/**
 * Transaction Status Checker Lambda: Checks transaction status on blockchain (single check, no retry)
 *
 * Returns:
 * - success → transaction confirmed with block number
 * - error 60003 with message prefix:
 *   - "TRANSACTION_FAILED:" → transaction reverted on blockchain
 *   - "TRANSACTION_NOT_FOUND:" → transaction not found on chain
 *   - "TRANSACTION_PENDING:" → transaction still in mempool, not yet mined
 *   - "GENERAL_ERROR:" → configuration or RPC errors
 *
 * Supports both SQS events (with task token) and direct invocation
 */

import { checkTransactionStatus } from "@elephant-xyz/cli/lib";
import {
  executeWithTaskToken,
  emitWorkflowEvent,
  createWorkflowError,
  createLogger,
} from "shared";

/**
 * Error code for transaction status checker (all use 60003 for backward compatibility)
 * Different error messages distinguish the failure type:
 * - "TRANSACTION_FAILED:" - Transaction reverted on blockchain
 * - "TRANSACTION_NOT_FOUND:" - Transaction not found on chain
 * - "TRANSACTION_PENDING:" - Transaction still in mempool, not yet mined
 */
const ERROR_CODE = "60003";

/**
 * Error message prefixes to identify failure type
 */
const ErrorPrefix = {
  GENERAL: "GENERAL_ERROR:",
  FAILED: "TRANSACTION_FAILED:",
  NOT_FOUND: "TRANSACTION_NOT_FOUND:",
  PENDING: "TRANSACTION_PENDING:",
};

/**
 * Custom error class with error code
 */
class TransactionStatusError extends Error {
  /**
   * @param {string} code - Error code
   * @param {string} message - Error message
   * @param {string} [transactionHash] - Transaction hash
   */
  constructor(code, message, transactionHash) {
    super(message);
    this.code = code;
    this.transactionHash = transactionHash;
    this.name = "TransactionStatusError";
  }
}

/**
 * @typedef {Object} TransactionStatusResult
 * @property {string} transactionHash - Transaction hash
 * @property {string} status - "success", "pending", "failed", or "dropped"
 * @property {number} [blockNumber] - Block number if mined
 * @property {string} [gasUsed] - Gas used if mined
 * @property {string} [error] - Error message if failed
 * @property {string} [errorCode] - Error code for failed/dropped status
 */

/**
 * @typedef {Object} TransactionStatusCheckerInput
 * @property {string} transactionHash - Transaction hash to check
 * @property {string} rpcUrl - RPC URL for blockchain
 */

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
 * Check transaction status once (no retries)
 * @param {TransactionStatusCheckerInput} input
 * @returns {Promise<TransactionStatusResult>}
 */
async function checkTransactionStatusOnce(input) {
  const { transactionHash, rpcUrl } = input;

  if (!rpcUrl) {
    throw new TransactionStatusError(
      ERROR_CODE,
      `${ErrorPrefix.GENERAL} RPC URL is required`,
      transactionHash,
    );
  }

  if (!transactionHash) {
    throw new TransactionStatusError(
      ERROR_CODE,
      `${ErrorPrefix.GENERAL} Transaction hash is required`,
      transactionHash,
    );
  }

  console.log(
    JSON.stringify({
      ...base,
      level: "info",
      msg: "checking_transaction_status",
      transactionHash: transactionHash,
    }),
  );

  try {
    const results = await checkTransactionStatus({
      transactionHashes: transactionHash,
      rpcUrl: rpcUrl,
    });

    // Transaction not found - empty result from CLI
    if (!results || results.length === 0) {
      console.log(
        JSON.stringify({
          ...base,
          level: "warn",
          msg: "transaction_not_found",
          transactionHash: transactionHash,
        }),
      );

      throw new TransactionStatusError(
        ERROR_CODE,
        `${ErrorPrefix.NOT_FOUND} Transaction ${transactionHash} not found on chain`,
        transactionHash,
      );
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

    // Transaction succeeded with block number
    if (status === "success" && blockNumber) {
      return {
        transactionHash: transactionHash,
        status: "success",
        blockNumber: blockNumber,
        gasUsed: result.gasUsed,
      };
    }

    // Transaction failed on blockchain (reverted)
    if (status === "failed") {
      throw new TransactionStatusError(
        ERROR_CODE,
        `${ErrorPrefix.FAILED} Transaction ${transactionHash} failed on blockchain (reverted). Block: ${blockNumber || "N/A"}`,
        transactionHash,
      );
    }

    // Transaction pending in mempool (has tx but no receipt yet)
    if (status === "pending") {
      throw new TransactionStatusError(
        ERROR_CODE,
        `${ErrorPrefix.PENDING} Transaction ${transactionHash} is pending in mempool`,
        transactionHash,
      );
    }

    // Transaction not found on chain (CLI returned not_found status)
    if (status === "not_found" || status === "not found") {
      throw new TransactionStatusError(
        ERROR_CODE,
        `${ErrorPrefix.NOT_FOUND} Transaction ${transactionHash} not found on chain`,
        transactionHash,
      );
    }

    // Unknown status - treat as not found
    throw new TransactionStatusError(
      ERROR_CODE,
      `${ErrorPrefix.NOT_FOUND} Transaction ${transactionHash} has unknown status: ${status}`,
      transactionHash,
    );
  } catch (err) {
    // Re-throw TransactionStatusError as-is
    if (err instanceof TransactionStatusError) {
      throw err;
    }

    // Wrap other errors
    const errorMsg = err instanceof Error ? err.message : String(err);
    console.error(
      JSON.stringify({
        ...base,
        level: "error",
        msg: "transaction_status_check_error",
        error: errorMsg,
        transactionHash: transactionHash,
      }),
    );

    throw new TransactionStatusError(
      ERROR_CODE,
      `${ErrorPrefix.GENERAL} Failed to check transaction status: ${errorMsg}`,
      transactionHash,
    );
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
          phase: "TransactionStatusCheck",
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
    }
  } else {
    // Direct invocation - extract from event
    transactionHash = event.transactionHash || event.transaction_hash;
    taskToken = event.taskToken;
    executionArn = event.executionArn;
    county = event.county;
  }

  try {
    const rpcUrl = process.env.ELEPHANT_RPC_URL;

    if (!rpcUrl) {
      throw new TransactionStatusError(
        ERROR_CODE,
        `${ErrorPrefix.GENERAL} RPC URL is required (ELEPHANT_RPC_URL env var)`,
        transactionHash,
      );
    }

    if (!transactionHash) {
      throw new TransactionStatusError(
        ERROR_CODE,
        `${ErrorPrefix.GENERAL} Transaction hash is required`,
        transactionHash,
      );
    }

    const result = await checkTransactionStatusOnce({
      transactionHash: transactionHash,
      rpcUrl: rpcUrl,
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
        phase: "TransactionStatusCheck",
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
    const isTransactionStatusError = err instanceof TransactionStatusError;
    const errorCode = isTransactionStatusError ? err.code : ERROR_CODE;
    const errMessage = err instanceof Error ? err.message : String(err);
    const errCause = err instanceof Error ? err.stack : undefined;

    console.error(
      JSON.stringify({
        ...base,
        level: "error",
        msg: "handler_failed",
        errorCode: errorCode,
        error: errMessage,
        transactionHash: transactionHash,
      }),
    );

    // Emit FAILED event to EventBridge and send task failure with specific error code
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
        phase: "TransactionStatusCheck",
        step: "CheckTransactionStatus",
        taskToken: taskToken,
        errors: [
          createWorkflowError(errorCode, {
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
          // Create error with code as the error name for Step Function to catch
          const error = new Error(errMessage);
          error.name = errorCode;
          throw error;
        },
      });
      // Don't throw after sending failure callback - let SQS know Lambda completed
      return;
    }

    // If no task token, throw to trigger SQS redelivery or fail the Lambda
    throw err;
  }
};
