/**
 * Gas Price Checker Lambda: Checks current gas price against configured maximum
 * If gas price is too high, waits and retries until it's acceptable
 * Supports both SQS events (with task token) and direct invocation
 */

import { checkGasPrice } from "@elephant-xyz/cli/lib";
import {
  emitWorkflowEvent,
  createLogger,
  sendTaskSuccess,
  sendTaskFailure,
} from "shared";

/**
 * @typedef {Object} GasPriceCheckerInput
 * @property {string} rpcUrl - RPC URL for blockchain
 * @property {number} maxGasPriceGwei - Maximum allowed gas price in Gwei
 * @property {number} [waitMinutes] - Minutes to wait between retries (default: 2)
 * @property {number} [maxWaitMinutes] - Maximum total minutes to wait before failing (default: 10)
 */

const base = {
  component: "gas-price-checker",
  at: new Date().toISOString(),
};

/**
 * Gas price check error codes - follows same pattern as submit Lambda
 * Error code patterns for GasPriceCheck phase (600xx range)
 */
const GasPriceCheckErrorCodes = {
  // Configuration (60010-60019)
  MISSING_RPC_URL: "60010",
  MISSING_MAX_GAS_PRICE: "60011",
  // RPC/Network (60020-60029)
  RPC_RETRIEVAL_FAILED: "60020",
  RPC_CONNECTION_ERROR: "60021",
  RPC_TIMEOUT: "60022",
  // Gas price (60030-60039)
  GAS_PRICE_TOO_HIGH: "60030",
  // Unknown
  UNKNOWN: "60001",
};

/**
 * Default maximum retry duration in minutes before failing due to high gas price
 * Can be overridden via GAS_PRICE_MAX_WAIT_MINUTES env var
 */
const DEFAULT_MAX_WAIT_MINUTES = 10;

/**
 * RPC error patterns for classification
 * @type {Array<{code: string, patterns: RegExp[], description: string}>}
 */
const RPC_ERROR_PATTERNS = [
  {
    code: GasPriceCheckErrorCodes.RPC_CONNECTION_ERROR,
    patterns: [/ECONNREFUSED/i, /ETIMEDOUT/i, /network error/i],
    description: "RPC connection error",
  },
  {
    code: GasPriceCheckErrorCodes.RPC_TIMEOUT,
    patterns: [/timeout/i, /ESOCKETTIMEDOUT/i],
    description: "RPC timeout",
  },
];

/**
 * Classify an error and return the appropriate error code
 * @param {unknown} error - The error to classify
 * @returns {{code: string, description: string}} Error code and description
 */
function classifyError(error) {
  const errorMessage = error instanceof Error ? error.message : String(error);

  // Check for specific configuration errors
  if (
    errorMessage.includes("RPC URL is required") ||
    errorMessage.includes("ELEPHANT_RPC_URL")
  ) {
    return {
      code: GasPriceCheckErrorCodes.MISSING_RPC_URL,
      description: "Missing RPC URL configuration",
    };
  }
  if (
    errorMessage.includes("maxGasPriceGwei") ||
    errorMessage.includes("GAS_PRICE_MAX_GWEI")
  ) {
    return {
      code: GasPriceCheckErrorCodes.MISSING_MAX_GAS_PRICE,
      description: "Missing max gas price configuration",
    };
  }
  if (errorMessage.includes("Unable to retrieve gas price")) {
    return {
      code: GasPriceCheckErrorCodes.RPC_RETRIEVAL_FAILED,
      description: "Unable to retrieve gas price from RPC",
    };
  }
  if (
    errorMessage.includes("Gas price too high") ||
    errorMessage.includes("exceeded maximum wait time")
  ) {
    return {
      code: GasPriceCheckErrorCodes.GAS_PRICE_TOO_HIGH,
      description: "Gas price exceeded maximum threshold after retry timeout",
    };
  }

  // Check RPC error patterns
  for (const errorDef of RPC_ERROR_PATTERNS) {
    for (const pattern of errorDef.patterns) {
      if (pattern.test(errorMessage)) {
        return { code: errorDef.code, description: errorDef.description };
      }
    }
  }

  // Default: unknown error
  return {
    code: GasPriceCheckErrorCodes.UNKNOWN,
    description: "Unknown gas price check error",
  };
}

/**
 * @typedef {Object} SqsRecord
 * @property {string} messageId - SQS message ID for batch failure reporting
 * @property {string} body
 * @property {Object} [messageAttributes]
 * @property {Object} [messageAttributes.TaskToken]
 * @property {string} [messageAttributes.TaskToken.stringValue]
 * @property {Object} [messageAttributes.ExecutionArn]
 * @property {string} [messageAttributes.ExecutionArn.stringValue]
 * @property {Object} [messageAttributes.County]
 * @property {string} [messageAttributes.County.stringValue]
 * @property {Object} [messageAttributes.DataGroupLabel]
 * @property {string} [messageAttributes.DataGroupLabel.stringValue]
 */

/**
 * @typedef {Object} ParsedMessage
 * @property {string} messageId - SQS message ID
 * @property {string} [taskToken] - Step Functions task token
 * @property {string} [executionArn] - Step Functions execution ARN
 * @property {string} [county] - County name
 * @property {string} [dataGroupLabel] - Data group label
 */

/**
 * @typedef {Object} GasPriceCheckerOutput
 * @property {string} status - "success" or "error"
 * @property {number} currentGasPriceGwei - Current gas price in Gwei
 * @property {number} maxGasPriceGwei - Maximum allowed gas price in Gwei
 * @property {number} retries - Number of retries performed
 * @property {number} [batchedMessageCount] - Number of messages in batch
 * @property {Array<{itemIdentifier: string}>} [batchItemFailures] - Failed message IDs for partial batch response
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
 * Parse and validate an SQS record, extracting task token and metadata
 * @param {SqsRecord} record - SQS record to parse
 * @returns {ParsedMessage}
 */
function parseRecord(record) {
  const messageId = record.messageId;
  const taskToken = record.messageAttributes?.TaskToken?.stringValue;
  const executionArn = record.messageAttributes?.ExecutionArn?.stringValue;
  const county = record.messageAttributes?.County?.stringValue || "unknown";
  const dataGroupLabel =
    record.messageAttributes?.DataGroupLabel?.stringValue || "County";

  return {
    messageId,
    taskToken,
    executionArn,
    county,
    dataGroupLabel,
  };
}

/**
 * Send task success for all successfully processed messages
 * @param {ParsedMessage[]} messages - Successfully processed messages
 * @param {Object} result - Gas price check result to send
 * @returns {Promise<string[]>} Array of message IDs that failed to send task success
 */
async function sendSuccessToAllMessages(messages, result) {
  /** @type {string[]} */
  const failedMessageIds = [];

  const successPromises = messages.map(async (msg) => {
    if (msg.taskToken && msg.executionArn) {
      const log = createLogger({
        component: "gas-price-checker",
        at: new Date().toISOString(),
        county: msg.county,
        executionId: msg.executionArn,
      });

      try {
        await sendTaskSuccess({
          taskToken: msg.taskToken,
          output: result,
        });
        log("info", "task_success_sent", {});
      } catch (sendError) {
        log("error", "failed_to_send_task_success", {
          error:
            sendError instanceof Error ? sendError.message : String(sendError),
        });
        failedMessageIds.push(msg.messageId);
      }
    }
  });

  await Promise.all(successPromises);
  return failedMessageIds;
}

/**
 * Send task failure for all messages due to gas price check error
 * @param {ParsedMessage[]} messages - Messages to send failure for
 * @param {string} errorMessage - Error message
 * @param {string} [errorCause] - Error cause/stack trace
 * @param {string} [errorCode] - Error code for classification (defaults to UNKNOWN)
 * @returns {Promise<string[]>} Array of message IDs that failed to send task failure
 */
async function sendFailureToAllMessages(
  messages,
  errorMessage,
  errorCause,
  errorCode,
) {
  const code = errorCode || GasPriceCheckErrorCodes.UNKNOWN;
  /** @type {string[]} */
  const failedMessageIds = [];

  const failurePromises = messages.map(async (msg) => {
    if (msg.taskToken && msg.executionArn) {
      const log = createLogger({
        component: "gas-price-checker",
        at: new Date().toISOString(),
        county: msg.county,
        executionId: msg.executionArn,
      });

      try {
        await sendTaskFailure({
          taskToken: msg.taskToken,
          error: code,
          cause: errorCause || errorMessage,
        });
        log("info", "task_failure_sent", { errorCode: code });
      } catch (sendError) {
        log("error", "failed_to_send_task_failure", {
          errorCode: code,
          error:
            sendError instanceof Error ? sendError.message : String(sendError),
        });
        failedMessageIds.push(msg.messageId);
      }
    }
  });

  await Promise.all(failurePromises);
  return failedMessageIds;
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
    maxWaitMinutes = DEFAULT_MAX_WAIT_MINUTES,
  } = input;

  if (!rpcUrl) {
    throw new Error("RPC URL is required");
  }

  if (!maxGasPriceGwei || maxGasPriceGwei <= 0) {
    throw new Error("maxGasPriceGwei must be a positive number");
  }

  const waitMs = waitMinutes * 60 * 1000;
  const maxWaitMs = maxWaitMinutes * 60 * 1000;
  const startTime = Date.now();
  let retries = 0;
  /** @type {number | null} */
  let lastGasPriceGwei = null;

  // Retry until gas price is acceptable or max wait time exceeded
  while (true) {
    const elapsedMs = Date.now() - startTime;
    const remainingMs = maxWaitMs - elapsedMs;

    // Check if we've exceeded max wait time (but allow first check)
    if (retries > 0 && elapsedMs >= maxWaitMs) {
      const errorMsg = `Gas price too high: exceeded maximum wait time of ${maxWaitMinutes} minutes. Current: ${lastGasPriceGwei} Gwei, Max: ${maxGasPriceGwei} Gwei`;
      console.error(
        JSON.stringify({
          ...base,
          level: "error",
          msg: "gas_price_max_wait_exceeded",
          currentGasPriceGwei: lastGasPriceGwei,
          maxGasPriceGwei: maxGasPriceGwei,
          maxWaitMinutes: maxWaitMinutes,
          elapsedMinutes: elapsedMs / 60000,
          retries: retries,
        }),
      );
      throw new Error(errorMsg);
    }

    try {
      console.log(
        JSON.stringify({
          ...base,
          level: "info",
          msg: "checking_gas_price",
          retry: retries,
          maxGasPriceGwei: maxGasPriceGwei,
          elapsedMinutes: Math.round((elapsedMs / 60000) * 100) / 100,
          remainingMinutes: Math.round((remainingMs / 60000) * 100) / 100,
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
      lastGasPriceGwei = currentGasPriceGwei;

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
          remainingMinutes: Math.round((remainingMs / 60000) * 100) / 100,
        }),
      );

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

      // Throw if it's a configuration error or max wait exceeded error
      if (
        errorMsg.includes("RPC URL is required") ||
        errorMsg.includes("maxGasPriceGwei must be") ||
        errorMsg.includes("exceeded maximum wait time")
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
 * Lambda handler - supports batch processing of SQS events with Step Function Task Tokens
 * Processes up to 100 messages, checks gas price once, and sends callback to all task tokens.
 * All task tokens receive the same gas price check result via sendTaskSuccess/sendTaskFailure.
 *
 * @param {{ Records?: SqsRecord[] }} event - SQS event with up to 100 records
 * @returns {Promise<GasPriceCheckerOutput>}
 */
export const handler = async (event) => {
  if (!event.Records || event.Records.length === 0) {
    throw new Error("Missing SQS Records");
  }

  const recordCount = event.Records.length;
  console.log({
    ...base,
    level: "info",
    msg: "batch_processing_start",
    recordCount,
  });

  /** @type {ParsedMessage[]} */
  const parsedMessages = [];
  /** @type {Array<{itemIdentifier: string}>} */
  const batchItemFailures = [];

  for (const record of event.Records) {
    parsedMessages.push(parseRecord(record));
  }

  console.log({
    ...base,
    level: "info",
    msg: "batch_parsing_complete",
    messageCount: parsedMessages.length,
    counties: parsedMessages.map((m) => m.county).join(", "),
  });

  for (const msg of parsedMessages) {
    if (msg.taskToken && msg.executionArn) {
      const log = createLogger({
        component: "gas-price-checker",
        at: new Date().toISOString(),
        county: msg.county,
        executionId: msg.executionArn,
      });
      try {
        await emitWorkflowEvent({
          executionId: /** @type {string} */ (
            msg.executionArn.split(":").pop()
          ),
          county: msg.county,
          dataGroupLabel: msg.dataGroupLabel,
          status: "IN_PROGRESS",
          phase: "GasPriceCheck",
          step: "CheckGasPrice",
          taskToken: msg.taskToken,
          errors: [],
          log,
        });
      } catch (eventErr) {
        log("warn", "failed_to_emit_in_progress_event", {
          error:
            eventErr instanceof Error ? eventErr.message : String(eventErr),
        });
      }
    }
  }

  try {
    const rpcUrl = process.env.ELEPHANT_RPC_URL;
    const maxGasPriceGwei = parseFloat(process.env.GAS_PRICE_MAX_GWEI || "0");
    const waitMinutes = parseFloat(process.env.GAS_PRICE_WAIT_MINUTES || "2");
    const maxWaitMinutes = parseFloat(
      process.env.GAS_PRICE_MAX_WAIT_MINUTES ||
        String(DEFAULT_MAX_WAIT_MINUTES),
    );

    if (!rpcUrl) {
      const error = "RPC URL is required (ELEPHANT_RPC_URL env var)";
      const errorCode = GasPriceCheckErrorCodes.MISSING_RPC_URL;
      console.error({
        ...base,
        level: "error",
        msg: "config_validation_failed",
        error,
        errorCode,
      });
      const failedToSendIds = await sendFailureToAllMessages(
        parsedMessages,
        error,
        undefined,
        errorCode,
      );
      for (const messageId of failedToSendIds) {
        batchItemFailures.push({ itemIdentifier: messageId });
      }
      for (const msg of parsedMessages) {
        if (!msg.taskToken) {
          batchItemFailures.push({ itemIdentifier: msg.messageId });
        }
      }
      return batchItemFailures.length > 0
        ? { status: "failed", batchItemFailures }
        : { status: "failed" };
    }

    if (!maxGasPriceGwei || maxGasPriceGwei <= 0) {
      const error = "maxGasPriceGwei is required (GAS_PRICE_MAX_GWEI env var)";
      const errorCode = GasPriceCheckErrorCodes.MISSING_MAX_GAS_PRICE;
      console.error({
        ...base,
        level: "error",
        msg: "config_validation_failed",
        error,
        errorCode,
      });
      const failedToSendIds = await sendFailureToAllMessages(
        parsedMessages,
        error,
        undefined,
        errorCode,
      );
      for (const messageId of failedToSendIds) {
        batchItemFailures.push({ itemIdentifier: messageId });
      }
      for (const msg of parsedMessages) {
        if (!msg.taskToken) {
          batchItemFailures.push({ itemIdentifier: msg.messageId });
        }
      }
      return batchItemFailures.length > 0
        ? { status: "failed", batchItemFailures }
        : { status: "failed" };
    }

    const result = await checkAndWaitForGasPrice({
      rpcUrl,
      maxGasPriceGwei,
      waitMinutes,
      maxWaitMinutes,
    });
    const batchResult = {
      ...result,
      batchedMessageCount: parsedMessages.length,
    };

    console.log({
      ...base,
      level: "info",
      msg: "gas_price_check_complete",
      result: batchResult,
      messageCount: parsedMessages.length,
    });

    // Note: SUCCEEDED event is emitted by step function (EmitGasPriceCheckSucceeded)
    // Lambda only emits IN_PROGRESS, step function handles SCHEDULED/SUCCEEDED/FAILED

    const failedToSendIds = await sendSuccessToAllMessages(
      parsedMessages,
      batchResult,
    );
    for (const messageId of failedToSendIds) {
      batchItemFailures.push({ itemIdentifier: messageId });
    }

    console.log({
      ...base,
      level: "info",
      msg: "batch_processing_complete",
      messageCount: parsedMessages.length,
      failedCallbackCount: failedToSendIds.length,
    });

    if (batchItemFailures.length > 0) {
      return { ...batchResult, batchItemFailures };
    }
    return batchResult;
  } catch (err) {
    const errMessage = err instanceof Error ? err.message : String(err);
    const errCause = err instanceof Error ? err.stack : undefined;
    // Classify the error to get the appropriate error code
    const { code: errorCode } = classifyError(err);

    console.error({
      ...base,
      level: "error",
      msg: "batch_handler_error",
      error: errMessage,
      errorCode,
      messageCount: parsedMessages.length,
    });

    // Note: FAILED event is emitted by step function (EmitGasPriceCheckFailed/WaitForGasPriceCheckResolution)
    // Lambda only emits IN_PROGRESS, step function handles SCHEDULED/SUCCEEDED/FAILED

    const failedToSendIds = await sendFailureToAllMessages(
      parsedMessages,
      errMessage,
      errCause,
      errorCode,
    );
    for (const messageId of failedToSendIds) {
      batchItemFailures.push({ itemIdentifier: messageId });
    }
    for (const msg of parsedMessages) {
      if (!msg.taskToken) {
        batchItemFailures.push({ itemIdentifier: msg.messageId });
      }
    }

    return batchItemFailures.length > 0
      ? { status: "failed", batchItemFailures }
      : { status: "failed" };
  }
};
