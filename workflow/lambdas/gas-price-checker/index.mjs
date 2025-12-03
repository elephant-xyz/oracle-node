/**
 * Gas Price Checker Lambda: Checks current gas price against configured maximum
 * If gas price is too high, waits and retries until it's acceptable
 * Supports both SQS events (with task token) and direct invocation
 */

import { checkGasPrice } from "@elephant-xyz/cli/lib";
import {
  SFNClient,
  SendTaskSuccessCommand,
  SendTaskFailureCommand,
} from "@aws-sdk/client-sfn";
import {
  EventBridgeClient,
  PutEventsCommand,
} from "@aws-sdk/client-eventbridge";

/**
 * @typedef {Object} GasPriceCheckerInput
 * @property {string} rpcUrl - RPC URL for blockchain
 * @property {number} maxGasPriceGwei - Maximum allowed gas price in Gwei
 * @property {number} [waitMinutes] - Minutes to wait before retry (default: 2)
 * @property {number} [maxRetries] - Maximum number of retries (default: 10)
 */

/**
 * @typedef {Object} GasPriceCheckerOutput
 * @property {string} status - "success" or "error"
 * @property {number} currentGasPriceGwei - Current gas price in Gwei
 * @property {number} maxGasPriceGwei - Maximum allowed gas price in Gwei
 * @property {number} retries - Number of retries performed
 */

const sfnClient = new SFNClient({
  region: process.env.AWS_REGION || "us-east-1",
});

const eventBridgeClient = new EventBridgeClient({
  region: process.env.AWS_REGION || "us-east-1",
});

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
 * Send success callback to Step Functions
 * @param {string} taskToken
 * @param {Object} output
 * @returns {Promise<void>}
 */
async function sendTaskSuccess(taskToken, output) {
  try {
    const cmd = new SendTaskSuccessCommand({
      taskToken: taskToken,
      output: JSON.stringify(output),
    });
    await sfnClient.send(cmd);
    console.log(
      JSON.stringify({
        ...base,
        level: "info",
        msg: "sent_task_success",
      }),
    );
  } catch (err) {
    console.error({
      ...base,
      level: "error",
      msg: "failed_to_send_task_success",
      error: err instanceof Error ? err.message : String(err),
    });
    throw err;
  }
}

/**
 * Send failure callback to Step Functions
 * @param {string} taskToken
 * @param {string} error
 * @param {string} cause
 * @returns {Promise<void>}
 */
async function sendTaskFailure(taskToken, error, cause) {
  try {
    const cmd = new SendTaskFailureCommand({
      taskToken: taskToken,
      error: error,
      cause: cause,
    });
    await sfnClient.send(cmd);
    console.log(
      JSON.stringify({
        ...base,
        level: "info",
        msg: "sent_task_failure",
        error: error,
      }),
    );
  } catch (err) {
    console.error({
      ...base,
      level: "error",
      msg: "failed_to_send_task_failure",
      error: err instanceof Error ? err.message : String(err),
    });
    throw err;
  }
}

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
  const {
    rpcUrl,
    maxGasPriceGwei,
    waitMinutes = 2,
    maxRetries = 10,
  } = input;

  if (!rpcUrl) {
    throw new Error("RPC URL is required");
  }

  if (!maxGasPriceGwei || maxGasPriceGwei <= 0) {
    throw new Error("maxGasPriceGwei must be a positive number");
  }

  const waitMs = waitMinutes * 60 * 1000;
  let retries = 0;

  while (retries < maxRetries) {
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
      const currentGasPriceGwei =
        gasPriceInfo.eip1559?.maxFeePerGas ||
        gasPriceInfo.legacy?.gasPrice ||
        null;

      if (currentGasPriceGwei === null) {
        throw new Error("Unable to retrieve gas price from RPC");
      }

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
      if (retries >= maxRetries) {
        throw new Error(
          `Gas price ${currentGasPriceGwei} Gwei exceeds maximum ${maxGasPriceGwei} Gwei after ${maxRetries} retries`,
        );
      }

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

      // If it's the last retry, throw the error
      if (retries >= maxRetries - 1) {
        throw err;
      }

      // Otherwise wait and retry
      retries++;
      await sleep(waitMs);
    }
  }

  throw new Error(`Failed to get acceptable gas price after ${maxRetries} retries`);
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
        try {
          await eventBridgeClient.send(
            new PutEventsCommand({
              Entries: [
                {
                  Source: "elephant.workflow",
                  DetailType: "WorkflowEvent",
                  Detail: JSON.stringify({
                    executionId: executionArn,
                    county: county || "unknown",
                    status: "IN_PROGRESS",
                    phase: "Submit",
                    step: "CheckGasPrice",
                    taskToken: taskToken,
                    errors: [],
                  }),
                },
              ],
            }),
          );
        } catch (eventErr) {
          // Log but don't fail on EventBridge errors
          console.warn({
            ...base,
            level: "warn",
            msg: "failed_to_emit_in_progress_event",
            error: eventErr instanceof Error ? eventErr.message : String(eventErr),
          });
        }
      }
    }
  }

  try {
    const rpcUrl = process.env.ELEPHANT_RPC_URL;
    const maxGasPriceGwei = parseFloat(process.env.GAS_PRICE_MAX_GWEI || "0");
    const waitMinutes = parseFloat(process.env.GAS_PRICE_WAIT_MINUTES || "2");
    const maxRetries = parseInt(process.env.GAS_PRICE_MAX_RETRIES || "10", 10);

    if (!rpcUrl) {
      const error = "RPC URL is required (ELEPHANT_RPC_URL env var)";
      if (taskToken) {
        await sendTaskFailure(taskToken, "ConfigurationError", error);
        // Don't throw after sending failure callback - let SQS know Lambda completed
        return;
      }
      throw new Error(error);
    }

    if (!maxGasPriceGwei || maxGasPriceGwei <= 0) {
      const error = "maxGasPriceGwei is required (GAS_PRICE_MAX_GWEI env var)";
      if (taskToken) {
        await sendTaskFailure(taskToken, "ConfigurationError", error);
        // Don't throw after sending failure callback - let SQS know Lambda completed
        return;
      }
      throw new Error(error);
    }

    const result = await checkAndWaitForGasPrice({
      rpcUrl,
      maxGasPriceGwei,
      waitMinutes,
      maxRetries,
    });

    console.log(
      JSON.stringify({
        ...base,
        level: "info",
        msg: "gas_price_check_complete",
        result: result,
      }),
    );

    // Emit SUCCEEDED event to EventBridge
    if (taskToken && executionArn) {
      try {
        await eventBridgeClient.send(
          new PutEventsCommand({
            Entries: [
              {
                Source: "elephant.workflow",
                DetailType: "WorkflowEvent",
                Detail: JSON.stringify({
                  executionId: executionArn,
                  county: county || "unknown",
                  status: "SUCCEEDED",
                  phase: "Submit",
                  step: "CheckGasPrice",
                  taskToken: taskToken,
                  errors: [],
                }),
              },
            ],
          }),
        );
      } catch (eventErr) {
        // Log but don't fail on EventBridge errors
        console.warn({
          ...base,
          level: "warn",
          msg: "failed_to_emit_succeeded_event",
          error: eventErr instanceof Error ? eventErr.message : String(eventErr),
        });
      }
    }

    // Send success callback to Step Functions if task token is present
    if (taskToken) {
      await sendTaskSuccess(taskToken, result);
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

    // Emit FAILED event to EventBridge
    if (taskToken && executionArn) {
      try {
        await eventBridgeClient.send(
          new PutEventsCommand({
            Entries: [
              {
                Source: "elephant.workflow",
                DetailType: "WorkflowEvent",
                Detail: JSON.stringify({
                  executionId: executionArn,
                  county: county || "unknown",
                  status: "FAILED",
                  phase: "Submit",
                  step: "CheckGasPrice",
                  taskToken: taskToken,
                  errors: [
                    {
                      code: "GAS_PRICE_CHECK_FAILED",
                      details: {
                        error: errMessage,
                        cause: errCause,
                      },
                    },
                  ],
                }),
              },
            ],
          }),
        );
      } catch (eventErr) {
        // Log but don't fail on EventBridge errors
        console.warn({
          ...base,
          level: "warn",
          msg: "failed_to_emit_failed_event",
          error: eventErr instanceof Error ? eventErr.message : String(eventErr),
        });
      }
    }

    // Send failure callback to Step Functions if task token is present
    if (taskToken) {
      try {
        await sendTaskFailure(taskToken, "GasPriceCheckFailed", errCause || errMessage);
        // Don't throw after sending failure callback - let SQS know Lambda completed
        // The Step Function will handle the failure via the callback
        return;
      } catch (callbackErr) {
        // If sending failure callback fails, log and throw to trigger SQS redelivery
        console.error(
          JSON.stringify({
            ...base,
            level: "error",
            msg: "failed_to_send_task_failure_callback",
            error: callbackErr instanceof Error ? callbackErr.message : String(callbackErr),
          }),
        );
        throw err;
      }
    }

    // If no task token, throw to trigger SQS redelivery or fail the Lambda
    throw err;
  }
};

