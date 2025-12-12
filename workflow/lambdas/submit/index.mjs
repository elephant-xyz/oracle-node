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
  createWorkflowError,
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
 */

const base = { component: "submit", at: new Date().toISOString() };

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
          /** @param {Buffer} chunk */(chunk) => chunks.push(chunk),
        );
        stream.on("error", reject);
        stream.on("end", () =>
          resolve(Buffer.concat(chunks).toString("utf-8")),
        );
      });
    const body = response.Body;
    if (!body)
      throw new Error("Failed to download keystore from S3: body not found");
    const bodyContents = await streamToString(body);
    await fs.writeFile(targetPath, bodyContents, "utf8");
    return targetPath;
  } catch (error) {
    throw new Error(
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
      },
    };
  }
}

/**
 * Send task failure for a failed message and emit workflow event
 * @param {FailedMessage} failedMessage - Failed message details
 * @returns {Promise<void>}
 */
async function handleFailedMessage(failedMessage) {
  const { taskToken, executionArn, county, error } = failedMessage;

  if (taskToken && executionArn) {
    const log = createLogger({
      component: "submit",
      at: new Date().toISOString(),
      county: county || "unknown",
      executionId: executionArn,
    });

    try {
      await emitWorkflowEvent({
        executionId: executionArn,
        county: county || "unknown",
        dataGroupLabel: "County",
        status: "FAILED",
        phase: "Submit",
        step: "SubmitToBlockchain",
        taskToken,
        errors: [createWorkflowError("60002", { error })],
        log,
      });
    } catch (eventErr) {
      log("error", "failed_to_emit_failed_event", {
        error: eventErr instanceof Error ? eventErr.message : String(eventErr),
      });
    }

    await sendTaskFailure({
      taskToken,
      error: "MessageParseError",
      cause: error,
    });
  }
}

/**
 * Send task success for all successfully processed messages
 * @param {ParsedMessage[]} messages - Successfully processed messages
 * @param {Object} result - Submit result to send
 * @returns {Promise<void>}
 */
async function sendSuccessToAllMessages(messages, result) {
  const successPromises = messages.map(async (msg) => {
    if (msg.taskToken && msg.executionArn) {
      const log = createLogger({
        component: "submit",
        at: new Date().toISOString(),
        county: msg.county || "unknown",
        executionId: msg.executionArn,
      });

      try {
        await emitWorkflowEvent({
          executionId: msg.executionArn,
          county: msg.county || "unknown",
          dataGroupLabel: msg.dataGroupLabel || "County",
          status: "SUCCEEDED",
          phase: "Submit",
          step: "SubmitToBlockchain",
          taskToken: msg.taskToken,
          errors: [],
          log,
        });
      } catch (eventErr) {
        log("error", "failed_to_emit_succeeded_event", {
          error:
            eventErr instanceof Error ? eventErr.message : String(eventErr),
        });
      }

      await sendTaskSuccess({
        taskToken: msg.taskToken,
        output: result,
      });

      log("info", "task_success_sent", {
        county: msg.county,
        executionArn: msg.executionArn,
      });
    }
  });

  await Promise.all(successPromises);
}

/**
 * Send task failure for all messages due to blockchain submission error
 * @param {ParsedMessage[]} messages - Messages to send failure for
 * @param {string} errorMessage - Error message
 * @param {string} [errorCause] - Error cause/stack trace
 * @returns {Promise<void>}
 */
async function sendFailureToAllMessages(messages, errorMessage, errorCause) {
  const failurePromises = messages.map(async (msg) => {
    if (msg.taskToken && msg.executionArn) {
      const log = createLogger({
        component: "submit",
        at: new Date().toISOString(),
        county: msg.county || "unknown",
        executionId: msg.executionArn,
      });

      try {
        await emitWorkflowEvent({
          executionId: msg.executionArn,
          county: msg.county || "unknown",
          dataGroupLabel: msg.dataGroupLabel || "County",
          status: "FAILED",
          phase: "Submit",
          step: "SubmitToBlockchain",
          taskToken: msg.taskToken,
          errors: [
            createWorkflowError("60002", {
              error: errorMessage,
              cause: errorCause,
            }),
          ],
          log,
        });
      } catch (eventErr) {
        log("error", "failed_to_emit_failed_event", {
          error:
            eventErr instanceof Error ? eventErr.message : String(eventErr),
        });
      }

      await sendTaskFailure({
        taskToken: msg.taskToken,
        error: "BlockchainSubmitError",
        cause: errorMessage,
      });

      log("info", "task_failure_sent", {
        county: msg.county,
        executionArn: msg.executionArn,
      });
    }
  });

  await Promise.all(failurePromises);
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
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }

  console.log({
    ...base,
    level: "info",
    msg: "batch_parsing_complete",
    validCount: validMessages.length,
    failedCount: failedMessages.length,
  });

  // Handle failed messages - send task failure for each
  for (const failedMsg of failedMessages) {
    console.log({
      ...base,
      level: "warn",
      msg: "message_parse_failed",
      messageId: failedMsg.messageId,
      error: failedMsg.error,
      county: failedMsg.county,
    });
    await handleFailedMessage(failedMsg);
  }

  // If no valid messages, return batch failures
  if (validMessages.length === 0) {
    console.log({
      ...base,
      level: "error",
      msg: "no_valid_messages_in_batch",
    });
    return { status: "failed", batchItemFailures };
  }

  // Aggregate all transaction items from valid messages
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

  const firstMessage = validMessages[0];
  if (firstMessage && firstMessage.taskToken && firstMessage.executionArn) {
    const log = createLogger({
      component: "submit",
      at: new Date().toISOString(),
      county: firstMessage.county || "unknown",
      executionId: firstMessage.executionArn,
    });
    await emitWorkflowEvent({
      executionId: firstMessage.executionArn,
      county: firstMessage.county || "unknown",
      dataGroupLabel: firstMessage.dataGroupLabel || "County",
      status: "IN_PROGRESS",
      phase: "Submit",
      step: "SubmitToBlockchain",
      taskToken: firstMessage.taskToken,
      errors: [],
      log,
    });
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

    let submitResult;

    if (!process.env.ELEPHANT_RPC_URL)
      throw new Error("ELEPHANT_RPC_URL is required");

    if (process.env.ELEPHANT_KEYSTORE_S3_KEY) {
      console.log({
        ...base,
        level: "info",
        msg: "Using keystore mode for contract submission (self-custodial)",
        itemCount: aggregatedItems.length,
      });
      if (!process.env.ENVIRONMENT_BUCKET)
        throw new Error("ENVIRONMENT_BUCKET is required");
      if (!process.env.ELEPHANT_KEYSTORE_S3_KEY)
        throw new Error("ELEPHANT_KEYSTORE_S3_KEY is required");
      if (!process.env.ELEPHANT_KEYSTORE_PASSWORD)
        throw new Error("ELEPHANT_KEYSTORE_PASSWORD is required");

      const gasPriceConfig = await getGasPriceFromSSM();

      const keystorePath = path.resolve(
        tmp,
        `keystore-${Date.now()}-${Math.random().toString(36).substring(7)}.json`,
      );
      await downloadKeystoreFromS3(
        process.env.ENVIRONMENT_BUCKET,
        process.env.ELEPHANT_KEYSTORE_S3_KEY,
        keystorePath,
      );

      try {
        submitResult = await submitToContract({
          csvFile: csvFilePath,
          keystoreJson: keystorePath,
          keystorePassword: process.env.ELEPHANT_KEYSTORE_PASSWORD,
          rpcUrl: process.env.ELEPHANT_RPC_URL,
          cwd: tmp,
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
    } else {
      console.log({
        ...base,
        level: "info",
        msg: "Using traditional API credentials for contract submission",
        itemCount: aggregatedItems.length,
      });

      if (!process.env.ELEPHANT_DOMAIN)
        throw new Error("ELEPHANT_DOMAIN is required");
      if (!process.env.ELEPHANT_API_KEY)
        throw new Error("ELEPHANT_API_KEY is required");
      if (!process.env.ELEPHANT_ORACLE_KEY_ID)
        throw new Error("ELEPHANT_ORACLE_KEY_ID is required");
      if (!process.env.ELEPHANT_FROM_ADDRESS)
        throw new Error("ELEPHANT_FROM_ADDRESS is required");
      if (!process.env.ELEPHANT_RPC_URL)
        throw new Error("ELEPHANT_RPC_URL is required");

      // Fetch gas buffer configuration from SSM Parameter Store for API mode
      const gasPriceConfig = await getGasPriceFromSSM();
      const gasBuffer = gasPriceConfig?.gasBuffer;

      submitResult = await submitToContract({
        csvFile: csvFilePath,
        domain: process.env.ELEPHANT_DOMAIN,
        apiKey: process.env.ELEPHANT_API_KEY,
        oracleKeyId: process.env.ELEPHANT_ORACLE_KEY_ID,
        fromAddress: process.env.ELEPHANT_FROM_ADDRESS,
        rpcUrl: process.env.ELEPHANT_RPC_URL,
        cwd: tmp,
        // Note: Gas price is not passed in API mode - Elephant manages it
        // But gas buffer can be set independently
        ...(gasBuffer ? { gasBuffer } : {}),
      });
    }

    if (!submitResult.success) {
      const errorMsg = `Submit failed: ${submitResult.error}`;
      console.error({
        ...base,
        level: "error",
        msg: "blockchain_submit_failed",
        error: errorMsg,
      });

      // Send failure to all valid messages
      await sendFailureToAllMessages(validMessages, errorMsg);

      // Add all valid messages to batch failures since blockchain submission failed
      for (const msg of validMessages) {
        batchItemFailures.push({ itemIdentifier: msg.messageId });
      }

      return { status: "failed", batchItemFailures };
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
        /** @param {SubmitResultRow} row */(row) => row.status === "failed",
      ),
    ];

    if (allErrors.length > 0) {
      const errorMsg =
        "Submit to the blockchain failed: " + JSON.stringify(allErrors);
      console.error({
        ...base,
        level: "error",
        msg: "blockchain_submit_errors",
        errors: allErrors,
      });

      // Send failure to all valid messages
      await sendFailureToAllMessages(validMessages, errorMsg);

      // Add all valid messages to batch failures
      for (const msg of validMessages) {
        batchItemFailures.push({ itemIdentifier: msg.messageId });
      }

      return { status: "failed", batchItemFailures };
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
    await sendSuccessToAllMessages(validMessages, result);

    // Return result with any batch failures from parsing phase
    if (batchItemFailures.length > 0) {
      return { ...result, batchItemFailures };
    }

    return result;
  } catch (err) {
    const errMessage = err instanceof Error ? err.message : String(err);
    const errCause = err instanceof Error ? err.stack : undefined;

    console.error({
      ...base,
      level: "error",
      msg: "batch_handler_error",
      error: errMessage,
      messageCount: validMessages.length,
    });

    // Send failure to all valid messages
    await sendFailureToAllMessages(validMessages, errMessage, errCause);

    // Add all valid messages to batch failures
    for (const msg of validMessages) {
      batchItemFailures.push({ itemIdentifier: msg.messageId });
    }

    // Return batch failures instead of throwing - let Lambda know which messages failed
    return { status: "failed", batchItemFailures };
  } finally {
    await fs.rm(tmp, { recursive: true, force: true });
  }
};
