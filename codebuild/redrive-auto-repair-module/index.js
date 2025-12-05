import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  ScanCommand,
  BatchWriteCommand,
} from "@aws-sdk/lib-dynamodb";
import {
  SQSClient,
  SendMessageCommand,
  GetQueueUrlCommand,
} from "@aws-sdk/client-sqs";
import { deleteExecution, deleteOrphanedErrorAggregates } from "./errors.mjs";

const dynamoClient = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
  marshallOptions: { removeUndefinedValues: true },
});
const sqsClient = new SQSClient({});

// Removed: getQueueArn, disableStarterEventSourceMapping, enableStarterEventSourceMapping, disableAutoRepair, enableAutoRepair
// These functions are no longer needed since we don't disable/enable anything

/**
 * Ensure required environment variables are present.
 *
 * @param {string} name - Environment variable identifier.
 * @returns {string} - Resolved environment variable value.
 */
function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

/**
 * Parse S3 URI into bucket and key.
 *
 * @param {string} s3Uri - S3 URI (e.g., "s3://bucket/key").
 * @returns {{ bucket: string, key: string }} - Parsed bucket and key.
 */
function parseS3Uri(s3Uri) {
  const match = /^s3:\/\/([^/]+)\/(.*)$/.exec(s3Uri);
  if (!match) {
    throw new Error(`Invalid S3 URI: ${s3Uri}`);
  }
  return { bucket: match[1], key: match[2] };
}

/**
 * Normalize county name to lowercase with dashes instead of spaces.
 *
 * @param {string} county - County name.
 * @returns {string} - Normalized county name.
 */
function normalizeCountyName(county) {
  return county.toLowerCase().replace(/\s+/g, "-");
}

/**
 * Resolve the appropriate SQS queue URL for a given county.
 * Checks if a county-specific queue exists, otherwise returns the main workflow queue URL.
 *
 * @param {string} county - County name.
 * @param {string} mainQueueUrl - Main workflow queue URL.
 * @returns {Promise<string>} - Resolved queue URL.
 */
async function resolveQueueUrl(county, mainQueueUrl) {
  // Extract the base queue name from the main queue URL
  // Example: "https://sqs.us-east-1.amazonaws.com/529160768076/elephant-workflow-queue"
  // -> "elephant-workflow-queue"
  const mainQueueNameMatch = mainQueueUrl.match(/\/([^/]+)$/);
  if (!mainQueueNameMatch) {
    console.warn(`Could not parse queue name from URL: ${mainQueueUrl}`);
    return mainQueueUrl;
  }

  const mainQueueName = mainQueueNameMatch[1];
  const normalizedCounty = normalizeCountyName(county);
  const countyQueueName = `${mainQueueName}-${normalizedCounty}`;

  try {
    // Try to get the county-specific queue URL
    const response = await sqsClient.send(
      new GetQueueUrlCommand({
        QueueName: countyQueueName,
      }),
    );

    if (response.QueueUrl) {
      console.log(
        `✓ Found county-specific queue for ${county}: ${response.QueueUrl}`,
      );
      return response.QueueUrl;
    }
  } catch (error) {
    // Queue doesn't exist or we don't have permission to access it
    if (
      error.name === "QueueDoesNotExist" ||
      error.name === "AWS.SimpleQueueService.NonExistentQueue"
    ) {
      console.log(
        `County-specific queue "${countyQueueName}" does not exist, using main queue`,
      );
    } else {
      console.warn(
        `Error checking for county-specific queue "${countyQueueName}": ${error.message}`,
      );
    }
  }

  // Fall back to main queue
  return mainQueueUrl;
}

/**
 * Delete all remaining items from the DynamoDB table.
 * This function scans the entire table and deletes all items in batches.
 *
 * @param {string} tableName - DynamoDB table name.
 * @returns {Promise<number>} - Number of items deleted.
 */
async function deleteAllRemainingItems(tableName) {
  console.log("\n=== Cleaning up all remaining items from DynamoDB ===");

  const BATCH_SIZE = 25; // DynamoDB BatchWrite limit
  let deletedCount = 0;
  let lastEvaluatedKey = undefined;

  do {
    // Scan for all items
    const scanParams = {
      TableName: tableName,
      ProjectionExpression: "PK, SK",
      ExclusiveStartKey: lastEvaluatedKey,
    };

    const scanResponse = await dynamoClient.send(new ScanCommand(scanParams));

    if (scanResponse.Items && scanResponse.Items.length > 0) {
      console.log(`Found ${scanResponse.Items.length} items to delete`);

      // Delete items in batches
      for (let i = 0; i < scanResponse.Items.length; i += BATCH_SIZE) {
        const batch = scanResponse.Items.slice(i, i + BATCH_SIZE);

        const deleteRequests = batch.map((item) => ({
          DeleteRequest: {
            Key: {
              PK: item.PK,
              SK: item.SK,
            },
          },
        }));

        await dynamoClient.send(
          new BatchWriteCommand({
            RequestItems: {
              [tableName]: deleteRequests,
            },
          }),
        );

        deletedCount += batch.length;
        console.log(`Deleted ${deletedCount} items so far...`);
      }
    }

    lastEvaluatedKey = scanResponse.LastEvaluatedKey;
  } while (lastEvaluatedKey);

  console.log(`✓ Total items deleted: ${deletedCount}`);
  return deletedCount;
}

/**
 * Query DynamoDB for ALL failed executions created before a specific timestamp.
 * Uses pagination to retrieve all executions, not just a limited set.
 *
 * @param {string} tableName - DynamoDB table name.
 * @param {string} beforeTimestamp - ISO timestamp - only process executions created before this time.
 * @param {number} maxExecutions - Maximum number of executions to process (safety limit, default: unlimited).
 * @returns {Promise<Array<import("./errors.mjs").FailedExecutionItem>>} - Array of failed executions.
 */
async function getFailedExecutions(
  tableName,
  beforeTimestamp,
  maxExecutions = Infinity,
) {
  console.log(
    `Scanning DynamoDB for ALL failed executions created before ${beforeTimestamp}...`,
  );

  /** @type {Array<import("./errors.mjs").FailedExecutionItem>} */
  const allExecutions = [];
  /** @type {Record<string, unknown> | undefined} */
  let lastEvaluatedKey = undefined;

  do {
    const scanParams = {
      TableName: tableName,
      FilterExpression:
        "#status = :status AND #entityType = :entityType AND createdAt < :beforeTimestamp",
      ExpressionAttributeNames: {
        "#status": "status",
        "#entityType": "entityType",
      },
      ExpressionAttributeValues: {
        ":status": "failed",
        ":entityType": "FailedExecution",
        ":beforeTimestamp": beforeTimestamp,
      },
      ExclusiveStartKey: lastEvaluatedKey,
    };

    const response = await dynamoClient.send(new ScanCommand(scanParams));

    if (response.Items && response.Items.length > 0) {
      // Filter and deduplicate
      const filtered = response.Items.filter((item) => {
        const createdAt = item.createdAt;
        return createdAt && createdAt < beforeTimestamp;
      });

      // Deduplicate by executionId
      const executionMap = new Map();
      for (const item of filtered) {
        const executionId = item.executionId;
        if (executionId && !executionMap.has(executionId)) {
          executionMap.set(executionId, item);
        }
      }

      allExecutions.push(...Array.from(executionMap.values()));

      // Check if we've reached the max limit
      if (allExecutions.length >= maxExecutions) {
        console.log(
          `Reached max executions limit (${maxExecutions}), stopping pagination`,
        );
        break;
      }
    }

    lastEvaluatedKey = response.LastEvaluatedKey;
  } while (lastEvaluatedKey);

  // Apply max limit if specified
  const result =
    maxExecutions === Infinity
      ? allExecutions
      : allExecutions.slice(0, maxExecutions);

  console.log(
    `Found ${allExecutions.length} total failed execution(s) created before ${beforeTimestamp} (${result.length} after applying limit, all unique)`,
  );

  return result;
}

/**
 * Re-queue a failed execution by sending it back to the workflow queue.
 *
 * @param {import("./errors.mjs").FailedExecutionItem} execution - Execution to re-queue.
 * @param {string} tableName - DynamoDB table name.
 * @param {string} mainWorkflowQueueUrl - Main SQS queue URL for workflow.
 * @returns {Promise<{ success: boolean, errorHashes?: string[] }>}
 */
async function processSingleExecution(
  execution,
  tableName,
  mainWorkflowQueueUrl,
) {
  console.log(
    `\n=== Re-queuing execution ${execution.executionId} (${execution.county}) ===`,
  );

  // Step 1: Get the original CSV file location from source (required)
  if (!execution.source?.s3Bucket || !execution.source?.s3Key) {
    console.error(
      `Skipping execution ${execution.executionId}: missing source information (s3Bucket: ${execution.source?.s3Bucket || "missing"}, s3Key: ${execution.source?.s3Key || "missing"})`,
    );
    return { success: false };
  }

  const bucket = execution.source.s3Bucket;
  const key = execution.source.s3Key;
  console.log(`Using original source CSV: s3://${bucket}/${key}`);

  try {
    // Step 2: Resolve the correct queue URL based on county
    const targetQueueUrl = await resolveQueueUrl(
      execution.county,
      mainWorkflowQueueUrl,
    );

    // Step 3: Create S3 event message (format expected by workflow)
    const s3EventMessage = {
      s3: {
        bucket: {
          name: bucket,
        },
        object: {
          key: key,
        },
      },
    };

    // Step 4: Send message to the resolved queue
    console.log(`Sending message to queue: ${targetQueueUrl}`);
    await sqsClient.send(
      new SendMessageCommand({
        QueueUrl: targetQueueUrl,
        MessageBody: JSON.stringify(s3EventMessage),
      }),
    );
    console.log(
      `✓ Message sent to queue for execution ${execution.executionId}`,
    );

    // Step 4: Delete execution from DynamoDB and collect error hashes
    let errorHashes = [];
    try {
      errorHashes = await deleteExecution({
        executionId: execution.executionId,
        tableName,
        documentClient: dynamoClient,
      });
      console.log(
        `✓ Deleted execution ${execution.executionId} and all associated errors from DynamoDB (${errorHashes.length} error hash(es) collected)`,
      );
    } catch (deleteError) {
      const deleteErrorMessage =
        deleteError instanceof Error
          ? deleteError.message
          : String(deleteError);
      console.error(
        `❌ FAILED to delete execution ${execution.executionId}: ${deleteErrorMessage}`,
      );
      throw deleteError; // Re-throw to mark as failed
    }

    return { success: true, errorHashes };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(
      `Error re-queuing execution ${execution.executionId}:`,
      errorMessage,
    );
    return { success: false, errorHashes: [] };
  }
}

/**
 * Main re-queue workflow.
 *
 * @returns {Promise<void>}
 */
async function main() {
  let autoRepairSchedulerFunctionName = null;

  try {
    console.log("Starting re-queue workflow...");

    const tableName = requireEnv("ERRORS_TABLE_NAME");
    const workflowQueueUrl = requireEnv("WORKFLOW_SQS_QUEUE_URL");
    autoRepairSchedulerFunctionName =
      process.env.AUTO_REPAIR_SCHEDULER_FUNCTION_NAME;

    console.log(`Environment variables check:`);
    console.log(`  WORKFLOW_SQS_QUEUE_URL: ${workflowQueueUrl}`);
    console.log(
      `  AUTO_REPAIR_SCHEDULER_FUNCTION_NAME: ${autoRepairSchedulerFunctionName || "(not set)"}`,
    );

    // Don't disable starter Lambda EventSourceMapping - let it continue processing
    console.log(
      "Starter Lambda EventSourceMapping will remain enabled - new messages will continue to be processed",
    );

    // Don't disable auto-repair - instead, we'll set a timestamp so auto-repair only processes NEW executions
    // that come from re-queued items. We'll store this in SSM so auto-repair can read it.
    const requeueStartTime = new Date().toISOString();
    console.log(`Re-queue start time: ${requeueStartTime}`);
    console.log(
      "Auto-repair will continue running but will only process executions created after this time",
    );

    // Store the re-queue start time in SSM Parameter Store so auto-repair can read it
    try {
      const { SSMClient, PutParameterCommand } =
        await import("@aws-sdk/client-ssm");
      const ssmClient = new SSMClient({});
      await ssmClient.send(
        new PutParameterCommand({
          Name: `/elephant-oracle/auto-repair/min-created-at`,
          Value: requeueStartTime,
          Type: "String",
          Overwrite: true,
          Description:
            "Minimum createdAt timestamp for auto-repair to process. Only executions created after this time will be processed.",
        }),
      );
      console.log(
        `✓ Stored re-queue start time in SSM: /elephant-oracle/auto-repair/min-created-at`,
      );
    } catch (error) {
      console.warn(
        `Failed to store re-queue start time in SSM: ${error.message}`,
      );
      console.warn(
        "Auto-repair may process old executions. Consider setting MIN_CREATED_AT environment variable manually.",
      );
    }

    // MAX_EXECUTIONS_PER_RUN is a safety limit (default: unlimited)
    // Set to a number to limit processing, or leave unset/0 to process ALL executions
    const maxExecutionsEnv = process.env.MAX_EXECUTIONS_PER_RUN;
    const maxExecutions = maxExecutionsEnv
      ? parseInt(maxExecutionsEnv, 10)
      : Infinity;

    // Capture the start time to prevent infinite loops
    const lambdaStartTime = new Date().toISOString();

    if (maxExecutions === Infinity) {
      console.log("Processing ALL failed executions (no limit)");
    } else {
      console.log(`Max executions per run: ${maxExecutions}`);
    }
    console.log(
      `Only processing executions created before: ${lambdaStartTime}`,
    );

    const failedExecutions = await getFailedExecutions(
      tableName,
      lambdaStartTime,
      maxExecutions,
    );

    if (failedExecutions.length === 0) {
      console.log("No failed executions found. All done!");
      return;
    }

    console.log(
      `Found ${failedExecutions.length} failed execution(s) to re-queue`,
    );

    // Process executions in parallel for better performance
    // CONCURRENCY controls how many executions are processed simultaneously
    const CONCURRENCY = parseInt(process.env.CONCURRENCY || "20", 10);
    console.log(
      `Processing with concurrency: ${CONCURRENCY} executions at a time`,
    );

    let processed = 0;
    let successful = 0;
    let failed = 0;
    /** @type {Set<string>} */
    const allErrorHashes = new Set();

    // Process in batches with concurrency limit
    for (let i = 0; i < failedExecutions.length; i += CONCURRENCY) {
      const batch = failedExecutions.slice(i, i + CONCURRENCY);
      const batchNum = Math.floor(i / CONCURRENCY) + 1;
      const totalBatches = Math.ceil(failedExecutions.length / CONCURRENCY);

      console.log(
        `\nProcessing batch ${batchNum}/${totalBatches} (${batch.length} executions)...`,
      );

      const results = await Promise.allSettled(
        batch.map((execution) =>
          processSingleExecution(execution, tableName, workflowQueueUrl),
        ),
      );

      for (const result of results) {
        processed++;
        if (result.status === "fulfilled" && result.value.success) {
          successful++;
          // Collect error hashes from successfully deleted executions
          if (
            result.value.errorHashes &&
            Array.isArray(result.value.errorHashes)
          ) {
            for (const hash of result.value.errorHashes) {
              allErrorHashes.add(hash);
            }
          }
        } else {
          failed++;
          if (result.status === "rejected") {
            console.error(`Execution failed with error:`, result.reason);
          }
        }
      }

      console.log(
        `Batch ${batchNum} complete: ${processed}/${failedExecutions.length} processed (${successful} successful, ${failed} failed)`,
      );
    }

    // Step 5: Delete orphaned error aggregates
    if (allErrorHashes.size > 0) {
      console.log(`\n=== Cleaning up orphaned error aggregates ===`);
      console.log(
        `Checking ${allErrorHashes.size} unique error hash(es) for orphaned aggregates...`,
      );
      try {
        const deletedCount = await deleteOrphanedErrorAggregates({
          errorHashes: Array.from(allErrorHashes),
          tableName,
          documentClient: dynamoClient,
        });
        console.log(`✓ Deleted ${deletedCount} orphaned error aggregate(s)`);
      } catch (aggregateError) {
        console.error(
          `Failed to delete orphaned error aggregates:`,
          aggregateError instanceof Error
            ? aggregateError.message
            : String(aggregateError),
        );
        // Don't fail the entire workflow if aggregate cleanup fails
      }
    } else {
      console.log("No error hashes collected, skipping aggregate cleanup");
    }

    // Step 6: Delete all remaining items from DynamoDB
    try {
      const remainingItemsDeleted = await deleteAllRemainingItems(tableName);
      console.log(
        `✓ Cleanup complete: ${remainingItemsDeleted} remaining item(s) deleted from DynamoDB`,
      );
    } catch (cleanupError) {
      console.error(
        `Failed to delete remaining items from DynamoDB:`,
        cleanupError instanceof Error
          ? cleanupError.message
          : String(cleanupError),
      );
      // Don't fail the entire workflow if final cleanup fails
    }

    console.log("\n=== Re-queue workflow summary ===");
    console.log(`Total processed: ${processed}`);
    console.log(`Successful: ${successful}`);
    console.log(`Failed: ${failed}`);
    console.log(`Error hashes collected: ${allErrorHashes.size}`);
    console.log("Re-queue workflow completed!");
  } catch (error) {
    console.error("Re-queue workflow failed:", error);
    process.exit(1);
  } finally {
    // Note: We don't disable/enable anything anymore
    // - Starter Lambda EventSourceMapping stays enabled (new messages continue processing)
    // - Auto-repair stays enabled (it will only process executions created after re-queue start time)
    console.log(
      "Re-queue workflow completed. Starter Lambda and auto-repair remain enabled.",
    );
  }
}

// Run main workflow
main();
