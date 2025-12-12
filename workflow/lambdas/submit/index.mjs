import { promises as fs } from "fs";
import path from "path";
import os from "os";
import { createObjectCsvWriter } from "csv-writer";
import { parse } from "csv-parse/sync";
import { submitToContract } from "@elephant-xyz/cli/lib";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import {
  executeWithTaskToken,
  emitWorkflowEvent,
  createLogger,
  sendTaskFailure,
  sendTaskSuccess,
} from "shared";

const s3Client = new S3Client({
  region: process.env.AWS_REGION || "us-east-1",
});

const ssmClient = new SSMClient({
  region: process.env.AWS_REGION || "us-east-1",
});

/**
 * Custom error class for Submit errors with error codes
 */
class SubmitError extends Error {
  /**
   * @param {string} code - Error code (e.g., "60101")
   * @param {string} message - Error message
   */
  constructor(code, message) {
    super(message);
    /** @type {string} */
    this.code = code;
    this.name = "SubmitError";
  }
}

/**
 * Error code patterns for blockchain RPC errors (parsed from errorMessage in submit_errors.csv)
 * The errorMessage column contains JSON-serialized EVM RPC responses
 * @type {Array<{code: string, patterns: RegExp[], description: string}>}
 */
// Note: Order matters! More specific patterns MUST come before more general ones.
// E.g., "contract creation code storage out of gas" (60411) must be checked before "out of gas" (60404)
const BLOCKCHAIN_ERROR_PATTERNS = [
  {
    code: "60401",
    patterns: [/already known/i],
    description: "Nonce already used",
  },
  {
    code: "60402",
    patterns: [/nonce too low/i],
    description: "Nonce too low",
  },
  {
    code: "60403",
    patterns: [/insufficient funds/i],
    description: "Insufficient funds",
  },
  // 60411 must come before 60404 because "contract creation code storage out of gas" contains "out of gas"
  {
    code: "60411",
    patterns: [/contract creation code storage out of gas/i],
    description: "Contract error",
  },
  {
    code: "60404",
    patterns: [/gas required exceeds/i, /out of gas/i],
    description: "Gas estimation failed",
  },
  {
    code: "60405",
    patterns: [/transaction underpriced/i, /replacement transaction/i],
    description: "Transaction underpriced",
  },
  {
    code: "60406",
    patterns: [/execution reverted/i, /revert/i],
    description: "Execution reverted",
  },
  {
    code: "60407",
    patterns: [/invalid transaction/i, /invalid sender/i],
    description: "Invalid transaction",
  },
  {
    code: "60408",
    patterns: [/ECONNREFUSED/i, /ETIMEDOUT/i, /network error/i],
    description: "RPC connection error",
  },
  {
    code: "60409",
    patterns: [/timeout/i, /ESOCKETTIMEDOUT/i],
    description: "RPC timeout",
  },
  {
    code: "60410",
    patterns: [/invalid argument/i, /invalid params/i],
    description: "Invalid parameters",
  },
];

/**
 * Submit error codes - centralized definitions
 */
const SubmitErrorCodes = {
  // Message Parsing (60100-60199)
  MISSING_BODY: "60101",
  INVALID_BODY_FORMAT: "60102",
  EMPTY_TRANSACTION_ITEMS: "60103",
  JSON_PARSE_ERROR: "60104",
  // Environment Config (60200-60299)
  MISSING_ENVIRONMENT_BUCKET: "60201",
  MISSING_KEYSTORE_S3_KEY: "60202",
  MISSING_KEYSTORE_PASSWORD: "60203",
  MISSING_DOMAIN: "60204",
  MISSING_API_KEY: "60205",
  MISSING_ORACLE_KEY_ID: "60206",
  MISSING_FROM_ADDRESS: "60207",
  MISSING_RPC_URL: "60208",
  // S3/Keystore (60300-60399)
  KEYSTORE_BODY_NOT_FOUND: "60301",
  S3_DOWNLOAD_FAILED: "60302",
  // Blockchain/Contract (60400-60499) - see BLOCKCHAIN_ERROR_PATTERNS
  SUBMIT_CLI_FAILURE: "60412",
  // File I/O (60500-60599)
  CSV_WRITE_FAILED: "60501",
  TRANSACTION_STATUS_READ_FAILED: "60502",
  SUBMIT_ERRORS_READ_FAILED: "60503",
  // Unknown
  UNKNOWN: "60999",
};

/**
 * Resolve the error message from a submit error row (CSV row from submit_errors.csv)
 * Reuses logic from codebuild/shared/errors.mjs
 * @param {Object} row - Raw CSV row to inspect
 * @param {string} [row.errorMessage] - Error message field
 * @param {string} [row.error_message] - Error message field (snake_case)
 * @param {string} [row.error] - Error field (from transaction-status.csv failed rows)
 * @returns {string} - Error message extracted from the row
 */
function resolveErrorMessage(row) {
  if (
    typeof row.errorMessage === "string" &&
    row.errorMessage.trim().length > 0
  ) {
    return row.errorMessage.trim();
  } else if (
    typeof row.error_message === "string" &&
    row.error_message.trim().length > 0
  ) {
    return row.error_message.trim();
  } else if (typeof row.error === "string" && row.error.trim().length > 0) {
    return row.error.trim();
  }
  return "";
}

/**
 * Classify a blockchain error message and return the appropriate error code
 * @param {string} errorMessage - The error message to classify (may be JSON)
 * @returns {{code: string, description: string}} Error code and description
 */
function classifyBlockchainError(errorMessage) {
  // Try to parse as JSON if it looks like JSON (RPC error responses are often JSON)
  let messageToMatch = errorMessage;
  if (errorMessage.startsWith("{") || errorMessage.startsWith("[")) {
    try {
      const parsed = JSON.parse(errorMessage);
      // Extract error message from common JSON-RPC error formats
      messageToMatch =
        parsed.error?.message ||
        parsed.message ||
        parsed.error ||
        JSON.stringify(parsed);
    } catch {
      // Not valid JSON, use as-is
    }
  }

  for (const errorDef of BLOCKCHAIN_ERROR_PATTERNS) {
    for (const pattern of errorDef.patterns) {
      if (pattern.test(messageToMatch)) {
        return { code: errorDef.code, description: errorDef.description };
      }
    }
  }
  // Default: unknown blockchain error
  return {
    code: SubmitErrorCodes.UNKNOWN,
    description: "Unknown blockchain error",
  };
}

/**
 * Classify errors from submit_errors.csv rows and return the most specific error code
 * @param {Array<{errorMessage?: string, error_message?: string, error?: string}>} errorRows - Array of error rows from CSV
 * @returns {{code: string, description: string, message: string}} Classified error info
 */
function classifySubmitErrors(errorRows) {
  if (!errorRows || errorRows.length === 0) {
    return {
      code: SubmitErrorCodes.UNKNOWN,
      description: "No error details available",
      message: "Unknown error",
    };
  }

  // Get the first error row and classify it
  const firstRow =
    /** @type {{errorMessage?: string, error_message?: string, error?: string}} */ (
      errorRows[0]
    );
  const errorMessage = resolveErrorMessage(firstRow);

  if (!errorMessage) {
    return {
      code: SubmitErrorCodes.UNKNOWN,
      description: "Empty error message",
      message: "Error message could not be resolved",
    };
  }

  const { code, description } = classifyBlockchainError(errorMessage);
  return { code, description, message: errorMessage };
}

/**
 * Classify a general error (not from CSV) and return the appropriate error code
 * @param {unknown} error - The error to classify
 * @returns {{code: string, description: string}} Error code and description
 */
function classifyGeneralError(error) {
  const errorMessage = error instanceof Error ? error.message : String(error);

  // Check if it's already a SubmitError with a code
  if (error instanceof SubmitError) {
    return { code: error.code, description: errorMessage };
  }

  // Try blockchain error classification first
  const blockchainResult = classifyBlockchainError(errorMessage);
  if (blockchainResult.code !== SubmitErrorCodes.UNKNOWN) {
    return blockchainResult;
  }

  // Check for specific known error patterns
  if (errorMessage.includes("ENVIRONMENT_BUCKET")) {
    return {
      code: SubmitErrorCodes.MISSING_ENVIRONMENT_BUCKET,
      description: "Missing ENVIRONMENT_BUCKET",
    };
  }
  if (errorMessage.includes("ELEPHANT_KEYSTORE_S3_KEY")) {
    return {
      code: SubmitErrorCodes.MISSING_KEYSTORE_S3_KEY,
      description: "Missing ELEPHANT_KEYSTORE_S3_KEY",
    };
  }
  if (errorMessage.includes("ELEPHANT_KEYSTORE_PASSWORD")) {
    return {
      code: SubmitErrorCodes.MISSING_KEYSTORE_PASSWORD,
      description: "Missing ELEPHANT_KEYSTORE_PASSWORD",
    };
  }
  if (errorMessage.includes("ELEPHANT_DOMAIN")) {
    return {
      code: SubmitErrorCodes.MISSING_DOMAIN,
      description: "Missing ELEPHANT_DOMAIN",
    };
  }
  if (errorMessage.includes("ELEPHANT_API_KEY")) {
    return {
      code: SubmitErrorCodes.MISSING_API_KEY,
      description: "Missing ELEPHANT_API_KEY",
    };
  }
  if (errorMessage.includes("ELEPHANT_ORACLE_KEY_ID")) {
    return {
      code: SubmitErrorCodes.MISSING_ORACLE_KEY_ID,
      description: "Missing ELEPHANT_ORACLE_KEY_ID",
    };
  }
  if (errorMessage.includes("ELEPHANT_FROM_ADDRESS")) {
    return {
      code: SubmitErrorCodes.MISSING_FROM_ADDRESS,
      description: "Missing ELEPHANT_FROM_ADDRESS",
    };
  }
  if (errorMessage.includes("ELEPHANT_RPC_URL")) {
    return {
      code: SubmitErrorCodes.MISSING_RPC_URL,
      description: "Missing ELEPHANT_RPC_URL",
    };
  }
  if (
    errorMessage.includes("download keystore") &&
    errorMessage.includes("body not found")
  ) {
    return {
      code: SubmitErrorCodes.KEYSTORE_BODY_NOT_FOUND,
      description: "Keystore body not found in S3",
    };
  }
  if (
    errorMessage.includes("download keystore") ||
    errorMessage.includes("Failed to download keystore")
  ) {
    return {
      code: SubmitErrorCodes.S3_DOWNLOAD_FAILED,
      description: "S3 download failed",
    };
  }

  return {
    code: SubmitErrorCodes.UNKNOWN,
    description: "Unknown submit error",
  };
}

/**
 * @typedef {Object} SubmitOutput
 * @property {string} status - Status of submit
 * @property {Array<{itemIdentifier: string}>} [batchItemFailures] - Failed message IDs for partial batch response
 */

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
 * @typedef {Object} SubmitInput
 * @property {SqsRecord[]} [Records] - SQS event format (can contain task token in message attributes)
 * @property {string} [taskToken] - Step Function task token (when invoked directly from Step Functions)
 * @property {string} [executionArn] - Step Function execution ARN (when invoked directly from Step Functions)
 * @property {string} [county] - County name (when invoked directly from Step Functions)
 * @property {string} [dataGroupLabel] - Data group label (when invoked directly from Step Functions)
 * @property {Object[]} [transactionItems] - Transaction items array (when invoked directly from Step Functions)
 */

/**
 * @typedef {Object} SubmitResultRow
 * @property {string} status - Status of the submission
 * @property {string} [txHash] - Transaction hash
 * @property {string} [transactionHash] - Transaction hash (alternative field name)
 * @property {string} [error] - Error message if failed
 */

/**
 * @typedef {Object} ParsedMessage
 * @property {string} messageId - SQS message ID
 * @property {Object[]} transactionItems - Parsed transaction items
 * @property {string} [taskToken] - Step Functions task token
 * @property {string} [executionArn] - Step Functions execution ARN
 * @property {string} [county] - County name
 * @property {string} [dataGroupLabel] - Data group label
 */

/**
 * @typedef {Object} FailedMessage
 * @property {string} messageId - SQS message ID
 * @property {string} [taskToken] - Step Functions task token (if available)
 * @property {string} [executionArn] - Step Functions execution ARN (if available)
 * @property {string} [county] - County name (if available)
 * @property {string} error - Error message
 * @property {string} [errorCode] - Error code for classification
 */

const base = { component: "submit", at: new Date().toISOString() };

/**
 * @typedef {Object} SubmitToBlockchainParams
 * @property {string} csvFilePath - Path to CSV file with transaction items
 * @property {string} tempDir - Temporary directory path
 * @property {number} itemCount - Number of items being submitted
 */

/**
 * @typedef {Object} SubmitToBlockchainResult
 * @property {boolean} success - Whether submission succeeded
 * @property {string} [error] - Error message if failed
 */

/**
 * Download keystore from S3
 * @param {string} bucket
 * @param {string} key
 * @param {string} targetPath
 * @returns {Promise<string>}
 */
async function downloadKeystoreFromS3(bucket, key, targetPath) {
  try {
    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    });
    const response = await s3Client.send(command);
    /**
     * @param {any} stream
     * @returns {Promise<string>}
     */
    const streamToString = (stream) =>
      new Promise((resolve, reject) => {
        /** @type {Buffer[]} */
        const chunks = [];
        stream.on(
          "data",
          /** @param {Buffer} chunk */ (chunk) => chunks.push(chunk),
        );
        stream.on("error", reject);
        stream.on("end", () =>
          resolve(Buffer.concat(chunks).toString("utf-8")),
        );
      });
    const body = response.Body;
    if (!body) {
      throw new SubmitError(
        SubmitErrorCodes.KEYSTORE_BODY_NOT_FOUND,
        "Failed to download keystore from S3: body not found",
      );
    }
    const bodyContents = await streamToString(body);
    await fs.writeFile(targetPath, bodyContents, "utf8");
    return targetPath;
  } catch (error) {
    // If it's already a SubmitError, re-throw it
    if (error instanceof SubmitError) {
      throw error;
    }
    throw new SubmitError(
      SubmitErrorCodes.S3_DOWNLOAD_FAILED,
      `Failed to download keystore from S3: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

/**
 * Get gas price configuration from AWS SSM Parameter Store
 * Used only for self-custodial oracles (keystore mode) to allow dynamic gas price configuration
 *
 * Supports multiple formats for backward compatibility:
 * 1. Simple string: "25" (legacy - sets gasPrice=25 Gwei, elephant-cli calculates priority fee)
 * 2. JSON object: {"maxFeePerGas":"30","maxPriorityFeePerGas":"5"} (EIP-1559 - full control)
 * 3. JSON object with gasBuffer: {"maxFeePerGas":"30","maxPriorityFeePerGas":"5","gasBuffer":"25"}
 * 4. JSON object with only gasBuffer: {"gasBuffer":"25"} (can be set independently)
 *
 * @returns {Promise<{gasPrice?: string, maxFeePerGas?: string, maxPriorityFeePerGas?: string, gasBuffer?: string} | undefined>}
 */
async function getGasPriceFromSSM() {
  try {
    const parameterName =
      process.env.GAS_PRICE_PARAMETER_NAME || "/elephant-oracle-node/gas-price";

    console.log({
      component: "submit",
      level: "info",
      msg: "Fetching gas price configuration from SSM Parameter Store",
      parameterName,
      at: new Date().toISOString(),
    });

    const command = new GetParameterCommand({
      Name: parameterName,
      WithDecryption: true,
    });

    const response = await ssmClient.send(command);
    const paramValue = response.Parameter?.Value;

    if (!paramValue) {
      console.log({
        component: "submit",
        level: "warn",
        msg: "Gas price parameter exists but has no value",
        at: new Date().toISOString(),
      });
      return undefined;
    }

    // Try to parse as JSON first (EIP-1559 format or with gasBuffer)
    try {
      const parsed = JSON.parse(paramValue);

      // Validate it's an object with expected fields
      if (typeof parsed === "object" && parsed !== null) {
        /** @type {{maxFeePerGas?: string, maxPriorityFeePerGas?: string, gasBuffer?: string}} */
        const result = {};

        if (parsed.maxFeePerGas) {
          result.maxFeePerGas = String(parsed.maxFeePerGas);
        }

        if (parsed.maxPriorityFeePerGas) {
          result.maxPriorityFeePerGas = String(parsed.maxPriorityFeePerGas);
        }

        if (parsed.gasBuffer) {
          result.gasBuffer = String(parsed.gasBuffer);
        }

        // If we found any params (EIP-1559 or gasBuffer), return them
        if (
          result.maxFeePerGas ||
          result.maxPriorityFeePerGas ||
          result.gasBuffer
        ) {
          console.log({
            component: "submit",
            level: "info",
            msg: "Successfully retrieved gas configuration from SSM",
            config: result,
            at: new Date().toISOString(),
          });
          return result;
        }
      }
    } catch (jsonError) {
      // Not JSON, treat as simple string (legacy format)
    }

    // Legacy format: simple string value
    console.log({
      component: "submit",
      level: "info",
      msg: "Successfully retrieved legacy gas price from SSM",
      gasPrice: paramValue,
      at: new Date().toISOString(),
    });

    return { gasPrice: paramValue };
  } catch (error) {
    // If parameter doesn't exist or there's an access issue, log and continue without gas price
    console.log({
      component: "submit",
      level: "warn",
      msg: "Failed to retrieve gas price from SSM, continuing without explicit gas price",
      error: error instanceof Error ? error.message : String(error),
      at: new Date().toISOString(),
    });
    return undefined;
  }
}

/**
 * Validate environment variables required for keystore mode
 * @throws {SubmitError} If required environment variables are missing
 * @returns {void}
 */
function validateKeystoreEnvVars() {
  if (!process.env.ENVIRONMENT_BUCKET) {
    throw new SubmitError(
      SubmitErrorCodes.MISSING_ENVIRONMENT_BUCKET,
      "ENVIRONMENT_BUCKET is required",
    );
  }
  if (!process.env.ELEPHANT_KEYSTORE_S3_KEY) {
    throw new SubmitError(
      SubmitErrorCodes.MISSING_KEYSTORE_S3_KEY,
      "ELEPHANT_KEYSTORE_S3_KEY is required",
    );
  }
  if (!process.env.ELEPHANT_KEYSTORE_PASSWORD) {
    throw new SubmitError(
      SubmitErrorCodes.MISSING_KEYSTORE_PASSWORD,
      "ELEPHANT_KEYSTORE_PASSWORD is required",
    );
  }
}

/**
 * Validate environment variables required for API mode
 * @throws {SubmitError} If required environment variables are missing
 * @returns {void}
 */
function validateApiEnvVars() {
  if (!process.env.ELEPHANT_DOMAIN) {
    throw new SubmitError(
      SubmitErrorCodes.MISSING_DOMAIN,
      "ELEPHANT_DOMAIN is required",
    );
  }
  if (!process.env.ELEPHANT_API_KEY) {
    throw new SubmitError(
      SubmitErrorCodes.MISSING_API_KEY,
      "ELEPHANT_API_KEY is required",
    );
  }
  if (!process.env.ELEPHANT_ORACLE_KEY_ID) {
    throw new SubmitError(
      SubmitErrorCodes.MISSING_ORACLE_KEY_ID,
      "ELEPHANT_ORACLE_KEY_ID is required",
    );
  }
  if (!process.env.ELEPHANT_FROM_ADDRESS) {
    throw new SubmitError(
      SubmitErrorCodes.MISSING_FROM_ADDRESS,
      "ELEPHANT_FROM_ADDRESS is required",
    );
  }
}

/**
 * Submit to blockchain using keystore mode (self-custodial)
 * Downloads keystore from S3, submits transaction, and cleans up keystore file
 *
 * @param {SubmitToBlockchainParams} params - Submission parameters
 * @returns {Promise<SubmitToBlockchainResult>} Submission result
 */
async function submitWithKeystore({ csvFilePath, tempDir, itemCount }) {
  console.log({
    ...base,
    level: "info",
    msg: "Using keystore mode for contract submission (self-custodial)",
    itemCount,
  });

  validateKeystoreEnvVars();

  const gasPriceConfig = await getGasPriceFromSSM();

  const keystorePath = path.resolve(
    tempDir,
    `keystore-${Date.now()}-${Math.random().toString(36).substring(7)}.json`,
  );

  await downloadKeystoreFromS3(
    /** @type {string} */ (process.env.ENVIRONMENT_BUCKET),
    /** @type {string} */ (process.env.ELEPHANT_KEYSTORE_S3_KEY),
    keystorePath,
  );

  try {
    return await submitToContract({
      csvFile: csvFilePath,
      keystoreJson: keystorePath,
      keystorePassword: /** @type {string} */ (
        process.env.ELEPHANT_KEYSTORE_PASSWORD
      ),
      rpcUrl: /** @type {string} */ (process.env.ELEPHANT_RPC_URL),
      cwd: tempDir,
      // Spread gas price configuration (supports both legacy and EIP-1559 formats)
      ...(gasPriceConfig || {}),
    });
  } finally {
    try {
      await fs.unlink(keystorePath);
      console.log({
        ...base,
        level: "info",
        msg: "Keystore file cleaned up successfully",
      });
    } catch (cleanupError) {
      console.error({
        ...base,
        level: "warn",
        msg: "Failed to clean up keystore file",
        error:
          cleanupError instanceof Error
            ? cleanupError.message
            : String(cleanupError),
      });
    }
  }
}

/**
 * Submit to blockchain using API mode (managed by Elephant)
 * Uses API credentials for transaction signing
 *
 * @param {SubmitToBlockchainParams} params - Submission parameters
 * @returns {Promise<SubmitToBlockchainResult>} Submission result
 */
async function submitWithApi({ csvFilePath, tempDir, itemCount }) {
  console.log({
    ...base,
    level: "info",
    msg: "Using API mode for contract submission",
    itemCount,
  });

  validateApiEnvVars();

  // Fetch gas buffer configuration from SSM Parameter Store for API mode
  const gasPriceConfig = await getGasPriceFromSSM();
  const gasBuffer = gasPriceConfig?.gasBuffer;

  return submitToContract({
    csvFile: csvFilePath,
    domain: /** @type {string} */ (process.env.ELEPHANT_DOMAIN),
    apiKey: /** @type {string} */ (process.env.ELEPHANT_API_KEY),
    oracleKeyId: /** @type {string} */ (process.env.ELEPHANT_ORACLE_KEY_ID),
    fromAddress: /** @type {string} */ (process.env.ELEPHANT_FROM_ADDRESS),
    rpcUrl: /** @type {string} */ (process.env.ELEPHANT_RPC_URL),
    cwd: tempDir,
    // Note: Gas price is not passed in API mode - Elephant manages it
    // But gas buffer can be set independently
    ...(gasBuffer ? { gasBuffer } : {}),
  });
}

/**
 * Determine submission mode and execute blockchain submission
 *
 * @param {SubmitToBlockchainParams} params - Submission parameters
 * @returns {Promise<SubmitToBlockchainResult>} Submission result
 */
async function submitToBlockchain(params) {
  const isKeystoreMode = Boolean(process.env.ELEPHANT_KEYSTORE_S3_KEY);

  return isKeystoreMode ? submitWithKeystore(params) : submitWithApi(params);
}

/**
 * Parse and validate an SQS record, extracting transaction items and metadata
 * @param {SqsRecord} record - SQS record to parse
 * @returns {{ success: true, data: ParsedMessage } | { success: false, error: FailedMessage }}
 */
function parseRecord(record) {
  const messageId = record.messageId;
  const taskToken = record.messageAttributes?.TaskToken?.stringValue;
  const executionArn = record.messageAttributes?.ExecutionArn?.stringValue;
  const county = record.messageAttributes?.County?.stringValue || "unknown";
  const dataGroupLabel =
    record.messageAttributes?.DataGroupLabel?.stringValue || "County";

  try {
    if (!record.body) {
      return {
        success: false,
        error: {
          messageId,
          taskToken,
          executionArn,
          county,
          error: "Missing SQS record body",
          errorCode: SubmitErrorCodes.MISSING_BODY,
        },
      };
    }

    const transactionItems = JSON.parse(record.body);

    if (!Array.isArray(transactionItems)) {
      return {
        success: false,
        error: {
          messageId,
          taskToken,
          executionArn,
          county,
          error: "SQS message body must contain an array of transaction items",
          errorCode: SubmitErrorCodes.INVALID_BODY_FORMAT,
        },
      };
    }

    if (transactionItems.length === 0) {
      return {
        success: false,
        error: {
          messageId,
          taskToken,
          executionArn,
          county,
          error: "Transaction items array is empty",
          errorCode: SubmitErrorCodes.EMPTY_TRANSACTION_ITEMS,
        },
      };
    }

    return {
      success: true,
      data: {
        messageId,
        transactionItems,
        taskToken,
        executionArn,
        county,
        dataGroupLabel,
      },
    };
  } catch (parseError) {
    return {
      success: false,
      error: {
        messageId,
        taskToken,
        executionArn,
        county,
        error: `Failed to parse message body: ${parseError instanceof Error ? parseError.message : String(parseError)}`,
        errorCode: SubmitErrorCodes.JSON_PARSE_ERROR,
      },
    };
  }
}

/**
 * Send task failure for a failed message and emit workflow event
 * @param {FailedMessage} failedMessage - Failed message details
 * @returns {Promise<boolean>} True if task failure was sent successfully, false otherwise
 */
async function handleFailedMessage(failedMessage) {
  const { taskToken, executionArn, county, error, errorCode, messageId } =
    failedMessage;
  const code = errorCode || SubmitErrorCodes.UNKNOWN;

  if (taskToken && executionArn) {
    try {
      await sendTaskFailure({
        taskToken,
        error: code,
        cause: error,
      });
      return true;
    } catch (sendError) {
      console.error({
        ...base,
        level: "error",
        msg: "failed_to_send_task_failure",
        messageId,
        county,
        error:
          sendError instanceof Error ? sendError.message : String(sendError),
      });
      return false;
    }
  }
  // No task token means we can't send to Step Functions, treat as success for SQS purposes
  return true;
}

/**
 * Send task success for all successfully processed messages
 * @param {ParsedMessage[]} messages - Successfully processed messages
 * @param {Object} result - Submit result to send
 * @returns {Promise<string[]>} Array of message IDs that failed to send task success
 */
async function sendSuccessToAllMessages(messages, result) {
  /** @type {string[]} */
  const failedMessageIds = [];

  const successPromises = messages.map(async (msg) => {
    if (msg.taskToken && msg.executionArn) {
      const log = createLogger({
        component: "submit",
        at: new Date().toISOString(),
        county: msg.county || "unknown",
        executionId: msg.executionArn,
      });

      try {
        await sendTaskSuccess({
          taskToken: msg.taskToken,
          output: result,
        });

        log("info", "task_success_sent", {
          county: msg.county,
          executionArn: msg.executionArn,
        });
      } catch (sendError) {
        log("error", "failed_to_send_task_success", {
          county: msg.county,
          executionArn: msg.executionArn,
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
 * Send task failure for all messages due to blockchain submission error
 * @param {ParsedMessage[]} messages - Messages to send failure for
 * @param {string} errorMessage - Error message
 * @param {string} [errorCause] - Error cause/stack trace
 * @param {string} [errorCode] - Error code for classification
 * @returns {Promise<string[]>} Array of message IDs that failed to send task failure
 */
async function sendFailureToAllMessages(
  messages,
  errorMessage,
  errorCause,
  errorCode,
) {
  const code = errorCode || SubmitErrorCodes.UNKNOWN;
  /** @type {string[]} */
  const failedMessageIds = [];

  const failurePromises = messages.map(async (msg) => {
    if (msg.taskToken && msg.executionArn) {
      const log = createLogger({
        component: "submit",
        at: new Date().toISOString(),
        county: msg.county || "unknown",
        executionId: msg.executionArn,
      });

      try {
        await sendTaskFailure({
          taskToken: msg.taskToken,
          error: code,
          cause: errorMessage,
        });

        log("info", "task_failure_sent", {
          county: msg.county,
          executionArn: msg.executionArn,
          errorCode: code,
        });
      } catch (sendError) {
        log("error", "failed_to_send_task_failure", {
          county: msg.county,
          executionArn: msg.executionArn,
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
 * Submit handler - supports batch processing of SQS events with Step Function Task Tokens
 * Processes up to 100 messages, aggregates transaction items, and submits to blockchain in single transaction.
 * All task tokens receive the same transaction hash via sendTaskSuccess.
 *
 * @param {SubmitInput} event - SQS event with up to 100 records
 * @returns {Promise<SubmitOutput>}
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

  // Parse all records and separate successes from failures
  /** @type {ParsedMessage[]} */
  const validMessages = [];
  /** @type {FailedMessage[]} */
  const failedMessages = [];
  /** @type {Array<{itemIdentifier: string}>} */
  const batchItemFailures = [];

  for (const record of event.Records) {
    const parseResult = parseRecord(record);
    if (parseResult.success) {
      validMessages.push(parseResult.data);
    } else {
      failedMessages.push(parseResult.error);
    }
  }

  console.log({
    ...base,
    level: "info",
    msg: "batch_parsing_complete",
    validCount: validMessages.length,
    failedCount: failedMessages.length,
  });

  // Handle failed messages - only add to batch failures if we failed to send task failure
  for (const failedMsg of failedMessages) {
    console.log({
      ...base,
      level: "warn",
      msg: "message_parse_failed",
      messageId: failedMsg.messageId,
      error: failedMsg.error,
      county: failedMsg.county,
    });
    const sentSuccessfully = await handleFailedMessage(failedMsg);
    if (!sentSuccessfully) {
      batchItemFailures.push({ itemIdentifier: failedMsg.messageId });
    }
  }

  if (validMessages.length === 0) {
    console.log({
      ...base,
      level: "info",
      msg: "no_valid_messages_in_batch",
    });
    // Return success to SQS unless we failed to send task failures
    if (batchItemFailures.length > 0) {
      return { status: "failed", batchItemFailures };
    }
    return { status: "success" };
  }

  /** @type {Object[]} */
  const aggregatedItems = [];
  for (const msg of validMessages) {
    aggregatedItems.push(...msg.transactionItems);
  }

  console.log({
    ...base,
    level: "info",
    msg: "items_aggregated",
    messageCount: validMessages.length,
    totalItemCount: aggregatedItems.length,
    counties: validMessages.map((m) => m.county).join(", "),
  });

  for (const msg of validMessages) {
    if (msg.taskToken && msg.executionArn) {
      const log = createLogger({
        component: "submit",
        at: new Date().toISOString(),
        county: msg.county || "unknown",
        executionId: msg.executionArn,
      });
      await emitWorkflowEvent({
        executionId: msg.executionArn,
        county: msg.county || "unknown",
        dataGroupLabel: msg.dataGroupLabel || "County",
        status: "IN_PROGRESS",
        phase: "Submit",
        step: "SubmitToBlockchain",
        taskToken: msg.taskToken,
        errors: [],
        log,
      });
    }
  }

  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "submit-"));
  try {
    const csvFilePath = path.resolve(tmp, "submit.csv");
    const writer = createObjectCsvWriter({
      path: csvFilePath,
      // @ts-expect-error - firstItem guaranteed to exist (validated in parseRecord)
      header: Object.keys(validMessages[0].transactionItems[0]).map((k) => ({
        id: k,
        title: k,
      })),
    });
    await writer.writeRecords(aggregatedItems);

    if (!process.env.ELEPHANT_RPC_URL) {
      throw new SubmitError(
        SubmitErrorCodes.MISSING_RPC_URL,
        "ELEPHANT_RPC_URL is required",
      );
    }

    const submitResult = await submitToBlockchain({
      csvFilePath,
      tempDir: tmp,
      itemCount: aggregatedItems.length,
    });

    if (!submitResult.success) {
      const errorMsg = `Submit failed: ${submitResult.error}`;
      // Classify the error from the CLI failure message
      const { code: errorCode } = submitResult.error
        ? classifyBlockchainError(submitResult.error)
        : { code: SubmitErrorCodes.SUBMIT_CLI_FAILURE };

      console.error({
        ...base,
        level: "error",
        msg: "blockchain_submit_failed",
        error: errorMsg,
        errorCode,
      });

      // Send failure to all valid messages with classified error code
      // Only add to batch failures if we failed to send task failure to Step Functions
      const failedToSendIds = await sendFailureToAllMessages(
        validMessages,
        errorMsg,
        undefined,
        errorCode,
      );

      for (const messageId of failedToSendIds) {
        batchItemFailures.push({ itemIdentifier: messageId });
      }

      // Return success to SQS unless we failed to send task failures
      if (batchItemFailures.length > 0) {
        return { status: "failed", batchItemFailures };
      }
      return { status: "success" };
    }

    // Read and parse CSV files, ensuring cleanup even if parsing fails
    const transactionStatusPath = path.join(tmp, "transaction-status.csv");
    const submitErrorsPath = path.join(tmp, "submit_errors.csv");

    /** @type {SubmitResultRow[]} */
    let submitResults;
    /** @type {SubmitResultRow[]} */
    let submitErrors;

    try {
      // Read and parse transaction-status.csv
      const submitResultsCsv = await fs.readFile(transactionStatusPath, "utf8");
      submitResults = parse(submitResultsCsv, {
        columns: true,
        skip_empty_lines: true,
        trim: true,
      });

      console.log({
        ...base,
        level: "info",
        msg: "submit_results_parsed",
        resultCount: submitResults.length,
      });

      // Read and parse submit_errors.csv
      const submitErrorsCsv = await fs.readFile(submitErrorsPath, "utf8");
      submitErrors = parse(submitErrorsCsv, {
        columns: true,
        skip_empty_lines: true,
        trim: true,
      });
    } finally {
      // Always clean up CSV files to free disk space, even if parsing fails
      try {
        await fs.unlink(transactionStatusPath);
        await fs.unlink(submitErrorsPath);
      } catch (cleanupError) {
        // Non-critical, log but continue
        console.log({
          ...base,
          level: "warn",
          msg: "Failed to cleanup CSV files",
          error:
            cleanupError instanceof Error
              ? cleanupError.message
              : String(cleanupError),
        });
      }
    }

    const allErrors = [
      ...submitErrors,
      ...submitResults.filter(
        /** @param {SubmitResultRow} row */ (row) => row.status === "failed",
      ),
    ];

    if (allErrors.length > 0) {
      // Classify the blockchain error from the CSV rows
      const { code: errorCode, message: classifiedMessage } =
        classifySubmitErrors(allErrors);
      const errorMsg =
        "Submit to the blockchain failed: " + JSON.stringify(allErrors);

      console.error({
        ...base,
        level: "error",
        msg: "blockchain_submit_errors",
        errors: allErrors,
        errorCode,
        classifiedMessage,
      });

      // Send failure to all valid messages with classified error code
      // Only add to batch failures if we failed to send task failure to Step Functions
      const failedToSendIds = await sendFailureToAllMessages(
        validMessages,
        errorMsg,
        undefined,
        errorCode,
      );

      for (const messageId of failedToSendIds) {
        batchItemFailures.push({ itemIdentifier: messageId });
      }

      // Return success to SQS unless we failed to send task failures
      if (batchItemFailures.length > 0) {
        return { status: "failed", batchItemFailures };
      }
      return { status: "success" };
    }

    // Build success result with transaction hash
    const result = {
      status: "success",
      submitResults: submitResults,
      batchedMessageCount: validMessages.length,
      totalItemCount: aggregatedItems.length,
    };

    console.log({
      ...base,
      level: "info",
      msg: "batch_submit_completed",
      messageCount: validMessages.length,
      itemCount: aggregatedItems.length,
      transactionHash:
        submitResults[0]?.transactionHash || submitResults[0]?.txHash,
    });

    // Send success to all valid messages with the same result
    // Only add to batch failures if we failed to send task success to Step Functions
    const failedToSendIds = await sendSuccessToAllMessages(
      validMessages,
      result,
    );

    for (const messageId of failedToSendIds) {
      batchItemFailures.push({ itemIdentifier: messageId });
    }

    // Return result with any batch failures (only from failed sends to Step Functions)
    if (batchItemFailures.length > 0) {
      return { ...result, batchItemFailures };
    }

    return result;
  } catch (err) {
    const errMessage = err instanceof Error ? err.message : String(err);
    const errCause = err instanceof Error ? err.stack : undefined;
    // Classify the error - handles SubmitError instances and general errors
    const { code: errorCode } = classifyGeneralError(err);

    console.error({
      ...base,
      level: "error",
      msg: "batch_handler_error",
      error: errMessage,
      errorCode,
      messageCount: validMessages.length,
    });

    // Send failure to all valid messages with classified error code
    // Only add to batch failures if we failed to send task failure to Step Functions
    const failedToSendIds = await sendFailureToAllMessages(
      validMessages,
      errMessage,
      errCause,
      errorCode,
    );

    for (const messageId of failedToSendIds) {
      batchItemFailures.push({ itemIdentifier: messageId });
    }

    // Return success to SQS unless we failed to send task failures
    if (batchItemFailures.length > 0) {
      return { status: "failed", batchItemFailures };
    }
    return { status: "success" };
  } finally {
    await fs.rm(tmp, { recursive: true, force: true });
  }
};
