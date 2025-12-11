/**
 * Workflow Direct Submit - Queue Mover
 *
 * This CodeBuild job drains messages from the transactions queues (main and DLQ)
 * and moves them to the workflow queue for processing via the "direct submit" mode.
 *
 * Message filtering:
 * - Only processes messages WITHOUT message attributes (ExecutionArn)
 * - Messages WITH ExecutionArn are left in the queue (they belong to running workflows)
 *
 * For each valid message:
 * 1. Parse the message body as JSON (array of transaction items)
 * 2. Construct the direct-submit input format
 * 3. Send the message to the workflow queue
 * 4. Delete the original message from the source queue
 */

import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  SendMessageCommand,
  GetQueueAttributesCommand,
} from "@aws-sdk/client-sqs";

// Initialize AWS client
const sqsClient = new SQSClient({});

// Configuration
const MAX_CONCURRENT_MOVES = 10;
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
  progress: (moved, skipped, failed, total) => {
    const timestamp = new Date().toISOString();
    console.log(
      `[${timestamp}] PROGRESS: Moved=${moved}, Skipped=${skipped}, Failed=${failed}, Total=${total}`,
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
 * Build the direct-submit message body for the workflow queue
 * @param {Array<object>} transactionItems - Transaction items from the original message
 * @returns {string} - JSON string for the workflow queue message
 */
function buildWorkflowMessage(transactionItems) {
  const workflowInput = {
    pre: {
      county_name: "UNKNOWN",
    },
    hash: {
      transactionItems: transactionItems,
    },
  };
  return JSON.stringify({ message: workflowInput });
}

/**
 * Send a message to the workflow queue with retry logic
 * @param {string} workflowQueueUrl - Workflow queue URL
 * @param {string} messageBody - Message body to send
 * @returns {Promise<{ success: boolean, messageId?: string, error?: string }>}
 */
async function sendToWorkflowQueue(workflowQueueUrl, messageBody) {
  for (let attempt = 0; attempt <= RETRY_DELAYS_MS.length; attempt++) {
    try {
      const response = await sqsClient.send(
        new SendMessageCommand({
          QueueUrl: workflowQueueUrl,
          MessageBody: messageBody,
        }),
      );

      return { success: true, messageId: response.MessageId };
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
          `Retryable error sending message (attempt ${attempt + 1}/${RETRY_DELAYS_MS.length + 1}): ${error.message}. Retrying in ${delay}ms...`,
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
 * @param {string} sourceQueueUrl - Source SQS queue URL
 * @param {string} workflowQueueUrl - Workflow queue URL
 * @param {Array<import("@aws-sdk/client-sqs").Message>} messages - Messages to process
 * @returns {Promise<{ moved: number, skipped: number, failed: number }>}
 */
async function processBatch(sourceQueueUrl, workflowQueueUrl, messages) {
  let moved = 0;
  let skipped = 0;
  let failed = 0;

  // Process messages concurrently with limit
  const chunks = [];
  for (let i = 0; i < messages.length; i += MAX_CONCURRENT_MOVES) {
    chunks.push(messages.slice(i, i + MAX_CONCURRENT_MOVES));
  }

  for (const chunk of chunks) {
    const results = await Promise.allSettled(
      chunk.map(async (message) => {
        // Check if message should be processed
        if (!shouldProcessMessage(message)) {
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

        // Build the workflow message
        const workflowMessage = buildWorkflowMessage(
          parseResult.transactionItems,
        );

        // Send to workflow queue
        const sendResult = await sendToWorkflowQueue(
          workflowQueueUrl,
          workflowMessage,
        );

        if (!sendResult.success) {
          log.error(`Failed to send to workflow queue: ${sendResult.error}`, {
            messageId: message.MessageId,
          });
          return {
            status: "failed",
            messageId: message.MessageId,
            error: sendResult.error,
          };
        }

        log.info(`Moved message to workflow queue`, {
          sourceMessageId: message.MessageId,
          targetMessageId: sendResult.messageId,
          transactionCount: parseResult.transactionItems.length,
        });

        // Delete message from source queue after successful send
        const deleted = await deleteMessage(
          sourceQueueUrl,
          message.ReceiptHandle,
        );
        if (!deleted) {
          log.warn(
            "Message sent to workflow queue but failed to delete from source",
            {
              messageId: message.MessageId,
            },
          );
        }

        return {
          status: "moved",
          messageId: message.MessageId,
          targetMessageId: sendResult.messageId,
        };
      }),
    );

    // Count results
    for (const result of results) {
      if (result.status === "fulfilled") {
        const value = result.value;
        if (value.status === "moved") moved++;
        else if (value.status === "skipped") skipped++;
        else if (value.status === "failed") failed++;
      } else {
        failed++;
        log.error(`Unexpected error processing message: ${result.reason}`);
      }
    }
  }

  return { moved, skipped, failed };
}

/**
 * Drain a single queue
 * @param {string} sourceQueueUrl - Source SQS queue URL
 * @param {string} queueName - Queue name for logging
 * @param {string} workflowQueueUrl - Workflow queue URL
 * @returns {Promise<{ moved: number, skipped: number, failed: number }>}
 */
async function drainQueue(sourceQueueUrl, queueName, workflowQueueUrl) {
  log.info(`Starting to drain queue: ${queueName}`);

  const initialCount = await getApproximateMessageCount(sourceQueueUrl);
  log.info(`Approximate messages in ${queueName}: ${initialCount}`);

  let totalMoved = 0;
  let totalSkipped = 0;
  let totalFailed = 0;
  let emptyReceiveCount = 0;
  let batchNumber = 0;
  const maxEmptyReceives = 3; // Stop after 3 consecutive empty receives

  while (emptyReceiveCount < maxEmptyReceives) {
    // Receive messages from queue
    const receiveResponse = await sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: sourceQueueUrl,
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
      `Batch #${batchNumber}: Received ${messages.length} messages (${messagesWithoutExecArn} to move, ${messagesWithExecArn} with ExecutionArn to skip)`,
    );

    // Process the batch
    const batchResult = await processBatch(
      sourceQueueUrl,
      workflowQueueUrl,
      messages,
    );

    totalMoved += batchResult.moved;
    totalSkipped += batchResult.skipped;
    totalFailed += batchResult.failed;

    // Log progress with running totals
    log.info(
      `Batch #${batchNumber} complete: +${batchResult.moved} moved, +${batchResult.skipped} skipped, +${batchResult.failed} failed`,
    );
    log.progress(
      totalMoved,
      totalSkipped,
      totalFailed,
      totalMoved + totalSkipped + totalFailed,
    );

    // Small delay between batches to avoid overwhelming the APIs
    await sleep(100);
  }

  log.summary(`Queue Drain Complete: ${queueName}`, {
    "Total Batches": batchNumber,
    "Moved (to workflow queue)": totalMoved,
    "Skipped (had ExecutionArn)": totalSkipped,
    Failed: totalFailed,
    "Total Messages Seen": totalMoved + totalSkipped + totalFailed,
  });

  return {
    moved: totalMoved,
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
    const workflowQueueUrl = requireEnv("WORKFLOW_QUEUE_URL");

    // Optional: process only specific queue
    const processOnlyDlq = process.env.PROCESS_ONLY_DLQ === "true";
    const processOnlyMain = process.env.PROCESS_ONLY_MAIN === "true";

    log.summary("Workflow Direct Submit - Queue Mover", {
      "Transactions Queue URL": transactionsQueueUrl,
      "Transactions DLQ URL": transactionsDlqUrl,
      "Workflow Queue URL": workflowQueueUrl,
      "Max Concurrent Moves": MAX_CONCURRENT_MOVES,
      "Visibility Timeout": `${VISIBILITY_TIMEOUT_SECONDS} seconds`,
      "Process Only DLQ": processOnlyDlq,
      "Process Only Main": processOnlyMain,
      "Start Time": new Date().toISOString(),
    });

    let mainQueueResult = { moved: 0, skipped: 0, failed: 0 };
    let dlqResult = { moved: 0, skipped: 0, failed: 0 };

    // Process main queue (unless processOnlyDlq is set)
    if (!processOnlyDlq) {
      mainQueueResult = await drainQueue(
        transactionsQueueUrl,
        "Transactions Queue (Main)",
        workflowQueueUrl,
      );
    }

    // Process DLQ (unless processOnlyMain is set)
    if (!processOnlyMain) {
      dlqResult = await drainQueue(
        transactionsDlqUrl,
        "Transactions Queue (DLQ)",
        workflowQueueUrl,
      );
    }

    // Calculate totals
    const totalMoved = mainQueueResult.moved + dlqResult.moved;
    const totalSkipped = mainQueueResult.skipped + dlqResult.skipped;
    const totalFailed = mainQueueResult.failed + dlqResult.failed;

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);

    log.summary("Queue Moving Complete", {
      Duration: `${duration} seconds`,
      "Main Queue - Moved": mainQueueResult.moved,
      "Main Queue - Skipped": mainQueueResult.skipped,
      "Main Queue - Failed": mainQueueResult.failed,
      "DLQ - Moved": dlqResult.moved,
      "DLQ - Skipped": dlqResult.skipped,
      "DLQ - Failed": dlqResult.failed,
      "Total Moved": totalMoved,
      "Total Skipped": totalSkipped,
      "Total Failed": totalFailed,
      "Total Messages": totalMoved + totalSkipped + totalFailed,
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
