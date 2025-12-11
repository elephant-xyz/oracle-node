/**
 * Workflow Direct Submit - Queue Drainer
 *
 * This CodeBuild job drains messages from the transactions queues (main and DLQ)
 * and starts workflow executions in "direct submit" mode, bypassing all mining
 * operations (prepare, transform, svl, hash, upload).
 *
 * Message filtering:
 * - Only processes messages WITHOUT message attributes (ExecutionArn)
 * - Messages WITH ExecutionArn are left in the queue (they belong to running workflows)
 *
 * For each valid message:
 * 1. Parse the message body as JSON (array of transaction items)
 * 2. Start a workflow execution with the direct-submit input format
 * 3. Delete the message from the queue only after successful execution start
 */

import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
} from "@aws-sdk/client-sqs";
import { SFNClient, StartExecutionCommand } from "@aws-sdk/client-sfn";

// Initialize AWS clients
const sqsClient = new SQSClient({});
const sfnClient = new SFNClient({});

// Configuration
const MAX_CONCURRENT_EXECUTIONS = 10;
const VISIBILITY_TIMEOUT_SECONDS = 300; // 5 minutes
const MAX_RECEIVE_COUNT = 10; // Max messages per SQS receive call
const RETRY_DELAYS_MS = [1000, 2000, 4000]; // Exponential backoff for retries

/**
 * Logging utilities for clear, structured output
 */
const log = {
  info: (message, data = {}) => {
    const timestamp = new Date().toISOString();
    if (Object.keys(data).length > 0) {
      console.log(`[${timestamp}] INFO: ${message}`, JSON.stringify(data));
    } else {
      console.log(`[${timestamp}] INFO: ${message}`);
    }
  },
  warn: (message, data = {}) => {
    const timestamp = new Date().toISOString();
    if (Object.keys(data).length > 0) {
      console.warn(`[${timestamp}] WARN: ${message}`, JSON.stringify(data));
    } else {
      console.warn(`[${timestamp}] WARN: ${message}`);
    }
  },
  error: (message, data = {}) => {
    const timestamp = new Date().toISOString();
    if (Object.keys(data).length > 0) {
      console.error(`[${timestamp}] ERROR: ${message}`, JSON.stringify(data));
    } else {
      console.error(`[${timestamp}] ERROR: ${message}`);
    }
  },
  progress: (processed, skipped, failed, total) => {
    const timestamp = new Date().toISOString();
    console.log(
      `[${timestamp}] PROGRESS: Processed=${processed}, Skipped=${skipped}, Failed=${failed}, Total=${total}`,
    );
  },
  summary: (title, data) => {
    console.log("\n" + "=".repeat(70));
    console.log(`  ${title}`);
    console.log("=".repeat(70));
    Object.entries(data).forEach(([key, value]) => {
      console.log(`  ${key}: ${value}`);
    });
    console.log("=".repeat(70) + "\n");
  },
};

/**
 * Get required environment variable or throw
 * @param {string} name - Environment variable name
 * @returns {string} - Environment variable value
 */
function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Required environment variable ${name} is not set`);
  }
  return value;
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
 * Get approximate message count from queue
 * @param {string} queueUrl - SQS queue URL
 * @returns {Promise<number>} - Approximate message count
 */
async function getApproximateMessageCount(queueUrl) {
  try {
    const response = await sqsClient.send(
      new GetQueueAttributesCommand({
        QueueUrl: queueUrl,
        AttributeNames: ["ApproximateNumberOfMessages"],
      }),
    );
    return parseInt(
      response.Attributes?.ApproximateNumberOfMessages || "0",
      10,
    );
  } catch (error) {
    log.warn(`Failed to get queue attributes: ${error.message}`);
    return -1;
  }
}

/**
 * Generate a unique execution name
 * @returns {string} - Unique execution name
 */
function generateExecutionName() {
  const timestamp = Date.now();
  const random = Math.random().toString(36).substring(2, 8);
  return `direct-submit-${timestamp}-${random}`;
}

/**
 * Check if a message should be processed (no ExecutionArn attribute)
 * @param {import("@aws-sdk/client-sqs").Message} message - SQS message
 * @returns {boolean} - True if message should be processed
 */
function shouldProcessMessage(message) {
  // Messages without attributes or without ExecutionArn should be processed
  if (!message.MessageAttributes) {
    return true;
  }
  // Skip messages that have ExecutionArn attribute (they belong to running workflows)
  return !message.MessageAttributes.ExecutionArn;
}

/**
 * Parse message body and validate structure
 * @param {string} body - Message body
 * @returns {{ valid: boolean, transactionItems?: Array<object>, error?: string }}
 */
function parseAndValidateMessageBody(body) {
  try {
    const parsed = JSON.parse(body);

    // Validate it's an array
    if (!Array.isArray(parsed)) {
      return { valid: false, error: "Message body is not an array" };
    }

    // Validate array is not empty
    if (parsed.length === 0) {
      return { valid: false, error: "Message body is an empty array" };
    }

    // Validate each item has required fields (basic check)
    for (let i = 0; i < parsed.length; i++) {
      const item = parsed[i];
      if (!item.propertyCid || !item.dataGroupCid) {
        return {
          valid: false,
          error: `Item at index ${i} missing required fields (propertyCid or dataGroupCid)`,
        };
      }
    }

    return { valid: true, transactionItems: parsed };
  } catch (error) {
    return { valid: false, error: `JSON parse error: ${error.message}` };
  }
}

/**
 * Start a workflow execution with retry logic
 * @param {string} stateMachineArn - Step Functions state machine ARN
 * @param {Array<object>} transactionItems - Transaction items from queue message
 * @returns {Promise<{ success: boolean, executionArn?: string, error?: string }>}
 */
async function startWorkflowExecution(stateMachineArn, transactionItems) {
  const executionName = generateExecutionName();

  // Build the direct-submit input format
  const input = {
    pre: {
      county_name: "UNKNOWN",
    },
    hash: {
      transactionItems: transactionItems,
    },
  };

  for (let attempt = 0; attempt <= RETRY_DELAYS_MS.length; attempt++) {
    try {
      const response = await sfnClient.send(
        new StartExecutionCommand({
          stateMachineArn,
          name: executionName,
          input: JSON.stringify(input),
        }),
      );

      return { success: true, executionArn: response.executionArn };
    } catch (error) {
      const isRetryable =
        error.name === "ThrottlingException" ||
        error.name === "ServiceUnavailable" ||
        error.name === "TooManyRequestsException" ||
        error.message?.includes("Rate exceeded") ||
        error.message?.includes("throttl");

      if (isRetryable && attempt < RETRY_DELAYS_MS.length) {
        const delay = RETRY_DELAYS_MS[attempt];
        log.warn(
          `Retryable error starting execution (attempt ${attempt + 1}/${RETRY_DELAYS_MS.length + 1}): ${error.message}. Retrying in ${delay}ms...`,
        );
        await sleep(delay);
        continue;
      }

      return { success: false, error: error.message };
    }
  }

  return { success: false, error: "Max retries exceeded" };
}

/**
 * Delete a message from SQS with retry logic
 * @param {string} queueUrl - SQS queue URL
 * @param {string} receiptHandle - Message receipt handle
 * @returns {Promise<boolean>} - True if deleted successfully
 */
async function deleteMessage(queueUrl, receiptHandle) {
  for (let attempt = 0; attempt <= RETRY_DELAYS_MS.length; attempt++) {
    try {
      await sqsClient.send(
        new DeleteMessageCommand({
          QueueUrl: queueUrl,
          ReceiptHandle: receiptHandle,
        }),
      );
      return true;
    } catch (error) {
      if (attempt < RETRY_DELAYS_MS.length) {
        const delay = RETRY_DELAYS_MS[attempt];
        log.warn(
          `Retryable error deleting message (attempt ${attempt + 1}): ${error.message}. Retrying in ${delay}ms...`,
        );
        await sleep(delay);
        continue;
      }
      log.error(`Failed to delete message after all retries: ${error.message}`);
      return false;
    }
  }
  return false;
}

/**
 * Process a batch of messages concurrently
 * @param {string} queueUrl - SQS queue URL
 * @param {string} stateMachineArn - Step Functions state machine ARN
 * @param {Array<import("@aws-sdk/client-sqs").Message>} messages - Messages to process
 * @returns {Promise<{ processed: number, skipped: number, failed: number }>}
 */
async function processBatch(queueUrl, stateMachineArn, messages) {
  let processed = 0;
  let skipped = 0;
  let failed = 0;

  // Process messages concurrently with limit
  const chunks = [];
  for (let i = 0; i < messages.length; i += MAX_CONCURRENT_EXECUTIONS) {
    chunks.push(messages.slice(i, i + MAX_CONCURRENT_EXECUTIONS));
  }

  for (const chunk of chunks) {
    const results = await Promise.allSettled(
      chunk.map(async (message) => {
        // Check if message should be processed
        if (!shouldProcessMessage(message)) {
          log.info("Skipping message with ExecutionArn attribute", {
            messageId: message.MessageId,
          });
          return { status: "skipped", messageId: message.MessageId };
        }

        // Parse and validate message body
        const parseResult = parseAndValidateMessageBody(message.Body || "");
        if (!parseResult.valid) {
          log.warn(`Invalid message body: ${parseResult.error}`, {
            messageId: message.MessageId,
          });
          return {
            status: "failed",
            messageId: message.MessageId,
            error: parseResult.error,
          };
        }

        // Start workflow execution
        const execResult = await startWorkflowExecution(
          stateMachineArn,
          parseResult.transactionItems,
        );

        if (!execResult.success) {
          log.error(`Failed to start execution: ${execResult.error}`, {
            messageId: message.MessageId,
          });
          return {
            status: "failed",
            messageId: message.MessageId,
            error: execResult.error,
          };
        }

        log.info(`Started execution: ${execResult.executionArn}`, {
          messageId: message.MessageId,
          transactionCount: parseResult.transactionItems.length,
        });

        // Delete message from queue after successful start
        const deleted = await deleteMessage(queueUrl, message.ReceiptHandle);
        if (!deleted) {
          log.warn(
            "Execution started but failed to delete message from queue",
            {
              messageId: message.MessageId,
              executionArn: execResult.executionArn,
            },
          );
        }

        return {
          status: "processed",
          messageId: message.MessageId,
          executionArn: execResult.executionArn,
        };
      }),
    );

    // Count results
    for (const result of results) {
      if (result.status === "fulfilled") {
        const value = result.value;
        if (value.status === "processed") processed++;
        else if (value.status === "skipped") skipped++;
        else if (value.status === "failed") failed++;
      } else {
        failed++;
        log.error(`Unexpected error processing message: ${result.reason}`);
      }
    }
  }

  return { processed, skipped, failed };
}

/**
 * Drain a single queue
 * @param {string} queueUrl - SQS queue URL
 * @param {string} queueName - Queue name for logging
 * @param {string} stateMachineArn - Step Functions state machine ARN
 * @returns {Promise<{ processed: number, skipped: number, failed: number }>}
 */
async function drainQueue(queueUrl, queueName, stateMachineArn) {
  log.info(`Starting to drain queue: ${queueName}`);

  const initialCount = await getApproximateMessageCount(queueUrl);
  log.info(`Approximate messages in ${queueName}: ${initialCount}`);

  let totalProcessed = 0;
  let totalSkipped = 0;
  let totalFailed = 0;
  let emptyReceiveCount = 0;
  let batchNumber = 0;
  const maxEmptyReceives = 3; // Stop after 3 consecutive empty receives

  while (emptyReceiveCount < maxEmptyReceives) {
    // Receive messages from queue
    const receiveResponse = await sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: MAX_RECEIVE_COUNT,
        VisibilityTimeout: VISIBILITY_TIMEOUT_SECONDS,
        MessageAttributeNames: ["All"],
        WaitTimeSeconds: 5, // Long polling
      }),
    );

    const messages = receiveResponse.Messages || [];

    if (messages.length === 0) {
      emptyReceiveCount++;
      log.info(
        `No messages received (${emptyReceiveCount}/${maxEmptyReceives} empty receives)`,
      );
      continue;
    }

    // Reset empty receive counter when we get messages
    emptyReceiveCount = 0;
    batchNumber++;

    // Count messages with ExecutionArn before processing
    const messagesWithExecArn = messages.filter(
      (m) => m.MessageAttributes?.ExecutionArn,
    ).length;
    const messagesWithoutExecArn = messages.length - messagesWithExecArn;

    log.info(
      `Batch #${batchNumber}: Received ${messages.length} messages (${messagesWithoutExecArn} to process, ${messagesWithExecArn} with ExecutionArn to skip)`,
    );

    // Process the batch
    const batchResult = await processBatch(queueUrl, stateMachineArn, messages);

    totalProcessed += batchResult.processed;
    totalSkipped += batchResult.skipped;
    totalFailed += batchResult.failed;

    // Log progress with running totals
    log.info(
      `Batch #${batchNumber} complete: +${batchResult.processed} processed, +${batchResult.skipped} skipped, +${batchResult.failed} failed`,
    );
    log.progress(
      totalProcessed,
      totalSkipped,
      totalFailed,
      totalProcessed + totalSkipped + totalFailed,
    );

    // Small delay between batches to avoid overwhelming the APIs
    await sleep(100);
  }

  log.summary(`Queue Drain Complete: ${queueName}`, {
    "Total Batches": batchNumber,
    "Processed (executions started)": totalProcessed,
    "Skipped (had ExecutionArn)": totalSkipped,
    Failed: totalFailed,
    "Total Messages Seen": totalProcessed + totalSkipped + totalFailed,
  });

  return {
    processed: totalProcessed,
    skipped: totalSkipped,
    failed: totalFailed,
  };
}

/**
 * Main function
 */
async function main() {
  const startTime = Date.now();

  try {
    // Get required environment variables
    const transactionsQueueUrl = requireEnv("TRANSACTIONS_QUEUE_URL");
    const transactionsDlqUrl = requireEnv("TRANSACTIONS_DLQ_URL");
    const stateMachineArn = requireEnv("STATE_MACHINE_ARN");

    // Optional: process only specific queue
    const processOnlyDlq = process.env.PROCESS_ONLY_DLQ === "true";
    const processOnlyMain = process.env.PROCESS_ONLY_MAIN === "true";

    log.summary("Workflow Direct Submit - Queue Drainer", {
      "Transactions Queue URL": transactionsQueueUrl,
      "Transactions DLQ URL": transactionsDlqUrl,
      "State Machine ARN": stateMachineArn,
      "Max Concurrent Executions": MAX_CONCURRENT_EXECUTIONS,
      "Visibility Timeout": `${VISIBILITY_TIMEOUT_SECONDS} seconds`,
      "Process Only DLQ": processOnlyDlq,
      "Process Only Main": processOnlyMain,
      "Start Time": new Date().toISOString(),
    });

    let mainQueueResult = { processed: 0, skipped: 0, failed: 0 };
    let dlqResult = { processed: 0, skipped: 0, failed: 0 };

    // Process main queue (unless processOnlyDlq is set)
    if (!processOnlyDlq) {
      mainQueueResult = await drainQueue(
        transactionsQueueUrl,
        "Transactions Queue (Main)",
        stateMachineArn,
      );
    }

    // Process DLQ (unless processOnlyMain is set)
    if (!processOnlyMain) {
      dlqResult = await drainQueue(
        transactionsDlqUrl,
        "Transactions Queue (DLQ)",
        stateMachineArn,
      );
    }

    // Calculate totals
    const totalProcessed = mainQueueResult.processed + dlqResult.processed;
    const totalSkipped = mainQueueResult.skipped + dlqResult.skipped;
    const totalFailed = mainQueueResult.failed + dlqResult.failed;

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);

    log.summary("Queue Draining Complete", {
      Duration: `${duration} seconds`,
      "Main Queue - Processed": mainQueueResult.processed,
      "Main Queue - Skipped": mainQueueResult.skipped,
      "Main Queue - Failed": mainQueueResult.failed,
      "DLQ - Processed": dlqResult.processed,
      "DLQ - Skipped": dlqResult.skipped,
      "DLQ - Failed": dlqResult.failed,
      "Total Processed": totalProcessed,
      "Total Skipped": totalSkipped,
      "Total Failed": totalFailed,
      "Total Messages": totalProcessed + totalSkipped + totalFailed,
    });

    // Exit with error if there were failures
    if (totalFailed > 0) {
      log.error(
        `Job completed with ${totalFailed} failure(s). Check logs for details.`,
      );
      process.exit(1);
    }

    log.info("Job completed successfully!");
  } catch (error) {
    log.error(`Job failed with unhandled error: ${error.message}`, {
      stack: error.stack,
    });
    process.exit(1);
  }
}

// Run main function
main();
