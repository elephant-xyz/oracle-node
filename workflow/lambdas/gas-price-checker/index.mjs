/**
 * Gas Price Checker Lambda: Checks current gas price against configured maximum
 * If gas price is too high, waits and retries until it's acceptable
 * Supports both SQS events (with task token) and direct invocation
 */

import { checkGasPrice } from "@elephant-xyz/cli/lib";
import {
  executeWithTaskToken,
  emitWorkflowEvent,
  createWorkflowError,
  createLogger,
} from "shared";

/**
 * @typedef {Object} GasPriceCheckerInput
 * @property {string} rpcUrl - RPC URL for blockchain
 * @property {number} maxGasPriceGwei - Maximum allowed gas price in Gwei
 * @property {number} [waitMinutes] - Minutes to wait before retry (default: 2)
 */

/**
 * @typedef {Object} GasPriceCheckerOutput
 * @property {string} status - "success" or "error"
 * @property {number} currentGasPriceGwei - Current gas price in Gwei
 * @property {number} maxGasPriceGwei - Maximum allowed gas price in Gwei
 * @property {number} retries - Number of retries performed
 */

const base = {
  component: "gas-price-checker",
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
 * Check gas price and wait if necessary
 * @param {GasPriceCheckerInput} input
 * @returns {Promise<GasPriceCheckerOutput>}
 */
async function checkAndWaitForGasPrice(input) {
  const { rpcUrl, maxGasPriceGwei, waitMinutes = 2 } = input;

  if (!rpcUrl) {
    throw new Error("RPC URL is required");
  }

  if (!maxGasPriceGwei || maxGasPriceGwei <= 0) {
    throw new Error("maxGasPriceGwei must be a positive number");
  }

  const waitMs = waitMinutes * 60 * 1000;
  let retries = 0;

  // Retry forever until gas price is acceptable
  while (true) {
    try {
      console.log(
        JSON.stringify({
          ...base,
          level: "info",
          msg: "checking_gas_price",
          retry: retries,
          maxGasPriceGwei: maxGasPriceGwei,
        }),
      );

      const gasPriceInfo = await checkGasPrice({ rpcUrl });

      // Use EIP-1559 maxFeePerGas if available, otherwise fall back to legacy gasPrice
      // Note: checkGasPrice returns values in Wei (as strings), so we need to convert to Gwei
      const currentGasPriceWei =
        gasPriceInfo.eip1559?.maxFeePerGas ||
        gasPriceInfo.legacy?.gasPrice ||
        null;

      if (currentGasPriceWei === null) {
        throw new Error("Unable to retrieve gas price from RPC");
      }

      // Convert from Wei to Gwei (divide by 1e9)
      const currentGasPriceGwei = parseFloat(currentGasPriceWei) / 1e9;

      console.log(
        JSON.stringify({
          ...base,
          level: "info",
          msg: "gas_price_retrieved",
          currentGasPriceGwei: currentGasPriceGwei,
          maxGasPriceGwei: maxGasPriceGwei,
          isAcceptable: currentGasPriceGwei <= maxGasPriceGwei,
        }),
      );

      // If gas price is acceptable, return success
      if (currentGasPriceGwei <= maxGasPriceGwei) {
        return {
          status: "success",
          currentGasPriceGwei: currentGasPriceGwei,
          maxGasPriceGwei: maxGasPriceGwei,
          retries: retries,
        };
      }

      // Gas price is too high, wait and retry
      retries++;
      console.log(
        JSON.stringify({
          ...base,
          level: "warn",
          msg: "gas_price_too_high_waiting",
          currentGasPriceGwei: currentGasPriceGwei,
          maxGasPriceGwei: maxGasPriceGwei,
          waitMinutes: waitMinutes,
          retry: retries,
        }),
      );

      // Note: We could emit a PARKED event here, but it might be too frequent
      // since we're waiting and retrying. The IN_PROGRESS event at the start
      // and SUCCEEDED/FAILED at the end should be sufficient.

      await sleep(waitMs);
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      console.error(
        JSON.stringify({
          ...base,
          level: "error",
          msg: "gas_price_check_failed",
          error: errorMsg,
          retry: retries,
        }),
      );

      // Wait and retry forever (only throw if it's a configuration error)
      if (
        errorMsg.includes("RPC URL is required") ||
        errorMsg.includes("maxGasPriceGwei must be")
      ) {
        throw err;
      }

      // Otherwise wait and retry
      retries++;
      await sleep(waitMs);
    }
  }
}

/**
 * Lambda handler - supports both SQS events (with task token) and direct invocation
 * @param {GasPriceCheckerInput | { Records?: SqsRecord[] }} event
 * @returns {Promise<GasPriceCheckerOutput | void>}
 */
export const handler = async (event) => {
  // Check if invoked from SQS (has Records array)
  const isSqsInvocation = !!event.Records && Array.isArray(event.Records);

  let taskToken;
  let executionArn;
  let county;
  let dataGroupLabel = "County"; // Default for Submit phase

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
      console.log({
        ...base,
        level: "info",
        msg: "invoked_from_sqs_with_task_token",
        executionArn: executionArn,
        county: county,
        hasTaskToken: !!taskToken,
      });

      // Emit IN_PROGRESS event to EventBridge when task token is received
      if (taskToken && executionArn) {
        const log = createLogger({
          component: "gas-price-checker",
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
          step: "CheckGasPrice",
          taskToken: taskToken,
          errors: [],
          log,
        });
      }
    }
  }

  try {
    const rpcUrl = process.env.ELEPHANT_RPC_URL;
    const maxGasPriceGwei = parseFloat(process.env.GAS_PRICE_MAX_GWEI || "0");
    const waitMinutes = parseFloat(process.env.GAS_PRICE_WAIT_MINUTES || "2");

    if (!rpcUrl) {
      const error = "RPC URL is required (ELEPHANT_RPC_URL env var)";
      if (taskToken && executionArn) {
        const errorLog = createLogger({
          component: "gas-price-checker",
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
          step: "CheckGasPrice",
          taskToken: taskToken,
          errors: [createWorkflowError("60001", { error })],
          log: errorLog,
        });
        await executeWithTaskToken({
          taskToken,
          log: errorLog,
          workerFn: async () => {
            throw new Error(error);
          },
        });
        // Don't throw after sending failure callback - let SQS know Lambda completed
        return;
      }
      throw new Error(error);
    }

    if (!maxGasPriceGwei || maxGasPriceGwei <= 0) {
      const error = "maxGasPriceGwei is required (GAS_PRICE_MAX_GWEI env var)";
      if (taskToken && executionArn) {
        const errorLog = createLogger({
          component: "gas-price-checker",
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
          step: "CheckGasPrice",
          taskToken: taskToken,
          errors: [createWorkflowError("60001", { error })],
          log: errorLog,
        });
        await executeWithTaskToken({
          taskToken,
          log: errorLog,
          workerFn: async () => {
            throw new Error(error);
          },
        });
        // Don't throw after sending failure callback - let SQS know Lambda completed
        return;
      }
      throw new Error(error);
    }

    const result = await checkAndWaitForGasPrice({
      rpcUrl,
      maxGasPriceGwei,
      waitMinutes,
    });

    console.log(
      JSON.stringify({
        ...base,
        level: "info",
        msg: "gas_price_check_complete",
        result: result,
      }),
    );

    // Emit SUCCEEDED event to EventBridge and send task success
    if (taskToken && executionArn) {
      const log = createLogger({
        component: "gas-price-checker",
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
        step: "CheckGasPrice",
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
      }),
    );

    // Emit FAILED event to EventBridge and send task failure
    if (taskToken && executionArn) {
      const errorLog = createLogger({
        component: "gas-price-checker",
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
        step: "CheckGasPrice",
        taskToken: taskToken,
        errors: [
          createWorkflowError("60001", {
            error: errMessage,
            cause: errCause,
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
      // The Step Function will handle the failure via the callback
      return;
    }

    // If no task token, throw to trigger SQS redelivery or fail the Lambda
    throw err;
  }
};
