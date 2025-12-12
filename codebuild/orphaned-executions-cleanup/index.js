/**
 * Orphaned Executions Cleanup Migration Script
 *
 * This script identifies and deletes orphaned FailedExecutionItem records that have
 * no corresponding ExecutionErrorLink records. This situation can occur when:
 *
 * 1. EventBridge retries a WorkflowEvent after a Lambda timeout
 * 2. The first invocation partially completed (creating FailedExecutionItem)
 * 3. The second invocation double-counted errors
 * 4. Later, error resolution deleted the ExecutionErrorLink(s)
 * 5. The FailedExecutionItem remains with inflated counts but no error links
 *
 * Detection Criteria for Orphaned Records:
 * - entityType = "FailedExecution"
 * - openErrorCount > 0 (still appears in queries for auto-repair)
 * - No ExecutionErrorLink records exist (SK begins_with "ERROR#" returns 0 items)
 *
 * This migration is idempotent - running it multiple times is safe.
 */

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  ScanCommand,
  QueryCommand,
  DeleteCommand,
} from "@aws-sdk/lib-dynamodb";

// Constants
const ENTITY_TYPE_FAILED_EXECUTION = "FailedExecution";
const BATCH_SIZE = 25; // Number of concurrent operations

// Initialize DynamoDB client
const dynamoClient = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
  marshallOptions: { removeUndefinedValues: true },
});

/**
 * Logging utilities for clear, structured output
 */
const log = {
  info: (message, data = {}) => {
    console.log(
      JSON.stringify({
        level: "INFO",
        message,
        ...data,
        timestamp: new Date().toISOString(),
      }),
    );
  },
  warn: (message, data = {}) => {
    console.warn(
      JSON.stringify({
        level: "WARN",
        message,
        ...data,
        timestamp: new Date().toISOString(),
      }),
    );
  },
  error: (message, data = {}) => {
    console.error(
      JSON.stringify({
        level: "ERROR",
        message,
        ...data,
        timestamp: new Date().toISOString(),
      }),
    );
  },
  summary: (title, data) => {
    console.log("\n" + "=".repeat(60));
    console.log(`  ${title}`);
    console.log("=".repeat(60));
    Object.entries(data).forEach(([key, value]) => {
      console.log(`  ${key}: ${value}`);
    });
    console.log("=".repeat(60) + "\n");
  },
};

/**
 * Get required environment variable or throw
 */
function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Required environment variable ${name} is not set`);
  }
  return value;
}

/**
 * Scan for FailedExecutionItem records with openErrorCount > 0
 * These are candidates for orphan check
 */
async function* scanFailedExecutionItems(tableName) {
  let lastEvaluatedKey;
  let pageNumber = 0;

  do {
    pageNumber++;
    log.info(`Scanning page ${pageNumber} for FailedExecutionItem records...`, {
      lastEvaluatedKey: !!lastEvaluatedKey,
    });

    const command = new ScanCommand({
      TableName: tableName,
      FilterExpression: "entityType = :entityType AND openErrorCount > :zero",
      ExpressionAttributeValues: {
        ":entityType": ENTITY_TYPE_FAILED_EXECUTION,
        ":zero": 0,
      },
      ProjectionExpression:
        "PK, SK, executionId, county, openErrorCount, uniqueErrorCount, totalOccurrences, createdAt, updatedAt, taskToken",
      ExclusiveStartKey: lastEvaluatedKey,
    });

    const response = await dynamoClient.send(command);

    if (response.Items && response.Items.length > 0) {
      log.info(
        `Page ${pageNumber}: Found ${response.Items.length} FailedExecutionItem candidates`,
        {
          scannedCount: response.ScannedCount,
          itemCount: response.Items.length,
        },
      );
      yield* response.Items;
    } else {
      log.info(`Page ${pageNumber}: No candidates on this page`, {
        scannedCount: response.ScannedCount,
      });
    }

    lastEvaluatedKey = response.LastEvaluatedKey;
  } while (lastEvaluatedKey);

  log.info(`Scan complete after ${pageNumber} pages`);
}

/**
 * Check if a FailedExecutionItem has any ExecutionErrorLink records
 * Returns the count of error links found
 */
async function countErrorLinksForExecution(tableName, executionId) {
  const pk = `EXECUTION#${executionId}`;

  const command = new QueryCommand({
    TableName: tableName,
    KeyConditionExpression: "PK = :pk AND begins_with(SK, :skPrefix)",
    ExpressionAttributeValues: {
      ":pk": pk,
      ":skPrefix": "ERROR#",
    },
    Select: "COUNT",
  });

  const response = await dynamoClient.send(command);
  return response.Count || 0;
}

/**
 * Delete an orphaned FailedExecutionItem
 */
async function deleteOrphanedExecution(tableName, item) {
  const { PK, SK, executionId, county } = item;

  try {
    const command = new DeleteCommand({
      TableName: tableName,
      Key: { PK, SK },
      ConditionExpression: "entityType = :entityType",
      ExpressionAttributeValues: {
        ":entityType": ENTITY_TYPE_FAILED_EXECUTION,
      },
    });

    await dynamoClient.send(command);
    return { success: true, executionId, county, PK };
  } catch (error) {
    if (error.name === "ConditionalCheckFailedException") {
      // Item was already deleted or doesn't match criteria
      return { success: true, executionId, county, PK, alreadyDeleted: true };
    }
    return { success: false, executionId, county, PK, error: error.message };
  }
}

/**
 * Check a single execution and determine if it's orphaned
 */
async function checkExecution(tableName, item) {
  const { executionId, county, openErrorCount, uniqueErrorCount } = item;

  const errorLinkCount = await countErrorLinksForExecution(
    tableName,
    executionId,
  );

  return {
    item,
    executionId,
    county,
    openErrorCount,
    uniqueErrorCount,
    errorLinkCount,
    isOrphaned: errorLinkCount === 0,
  };
}

/**
 * Process items in batches for efficient checking
 */
async function checkBatch(tableName, items) {
  const results = await Promise.all(
    items.map((item) => checkExecution(tableName, item)),
  );

  const orphaned = results.filter((r) => r.isOrphaned);
  const valid = results.filter((r) => !r.isOrphaned);

  return { orphaned, valid };
}

/**
 * Delete orphaned executions in batches
 */
async function deleteBatch(tableName, orphanedItems) {
  const results = await Promise.all(
    orphanedItems.map((r) => deleteOrphanedExecution(tableName, r.item)),
  );

  const deleted = results.filter((r) => r.success && !r.alreadyDeleted);
  const alreadyDeleted = results.filter((r) => r.success && r.alreadyDeleted);
  const failed = results.filter((r) => !r.success);

  return { deleted, alreadyDeleted, failed };
}

/**
 * Verify cleanup by checking for remaining orphaned records
 */
async function verifyCleanup(tableName) {
  log.info("Verifying cleanup...");

  // Sample check: scan for FailedExecutionItems with openErrorCount > 0
  // and verify they all have error links
  const scanCommand = new ScanCommand({
    TableName: tableName,
    FilterExpression: "entityType = :entityType AND openErrorCount > :zero",
    ExpressionAttributeValues: {
      ":entityType": ENTITY_TYPE_FAILED_EXECUTION,
      ":zero": 0,
    },
    Limit: 20,
  });

  const response = await dynamoClient.send(scanCommand);

  if (!response.Items || response.Items.length === 0) {
    log.info(
      "Verification: No FailedExecutionItem records with openErrorCount > 0 found",
    );
    return true;
  }

  // Check each one for error links
  let orphanedFound = 0;
  for (const item of response.Items) {
    const errorLinkCount = await countErrorLinksForExecution(
      tableName,
      item.executionId,
    );
    if (errorLinkCount === 0) {
      orphanedFound++;
      log.warn(`Verification: Found orphaned execution ${item.executionId}`, {
        county: item.county,
        openErrorCount: item.openErrorCount,
      });
    }
  }

  if (orphanedFound > 0) {
    log.warn(
      `Verification: Found ${orphanedFound} orphaned executions in sample of ${response.Items.length}`,
    );
    return false;
  }

  log.info(
    `Verification: All ${response.Items.length} sampled executions have valid error links`,
  );
  return true;
}

/**
 * Main cleanup function
 */
async function runCleanup() {
  const startTime = Date.now();
  const tableName = requireEnv("ERRORS_TABLE_NAME");
  const dryRun = process.env.DRY_RUN === "true";

  log.summary("Orphaned Executions Cleanup", {
    "Table Name": tableName,
    "Dry Run": dryRun,
    "Batch Size": BATCH_SIZE,
    "Start Time": new Date().toISOString(),
  });

  if (dryRun) {
    log.warn("DRY RUN MODE - No changes will be made");
  }

  // Statistics
  let totalScanned = 0;
  let totalOrphaned = 0;
  let totalValid = 0;
  let totalDeleted = 0;
  let totalAlreadyDeleted = 0;
  let totalFailed = 0;
  let batchNumber = 0;
  const orphanedDetails = [];
  const failedItems = [];

  // Process items in batches
  let batch = [];

  for await (const item of scanFailedExecutionItems(tableName)) {
    totalScanned++;
    batch.push(item);

    if (batch.length >= BATCH_SIZE) {
      batchNumber++;

      // Check which items are orphaned
      const { orphaned, valid } = await checkBatch(tableName, batch);
      totalOrphaned += orphaned.length;
      totalValid += valid.length;

      if (orphaned.length > 0) {
        log.info(
          `Batch ${batchNumber}: Found ${orphaned.length} orphaned executions`,
          {
            orphanedExecutionIds: orphaned.map((r) => r.executionId),
          },
        );

        // Store details for summary
        orphanedDetails.push(
          ...orphaned.map((r) => ({
            executionId: r.executionId,
            county: r.county,
            openErrorCount: r.openErrorCount,
            uniqueErrorCount: r.uniqueErrorCount,
          })),
        );

        if (!dryRun) {
          // Delete orphaned items
          const { deleted, alreadyDeleted, failed } = await deleteBatch(
            tableName,
            orphaned,
          );
          totalDeleted += deleted.length;
          totalAlreadyDeleted += alreadyDeleted.length;
          totalFailed += failed.length;
          failedItems.push(...failed);

          log.info(`Batch ${batchNumber} deletion complete`, {
            deleted: deleted.length,
            alreadyDeleted: alreadyDeleted.length,
            failed: failed.length,
          });

          if (failed.length > 0) {
            log.error(
              `Batch ${batchNumber} had ${failed.length} deletion failures`,
              {
                errors: failed.slice(0, 5).map((f) => ({
                  executionId: f.executionId,
                  error: f.error,
                })),
              },
            );
          }
        } else {
          log.info(
            `[DRY RUN] Batch ${batchNumber}: Would delete ${orphaned.length} orphaned executions`,
          );
          totalDeleted += orphaned.length;
        }
      } else {
        log.info(`Batch ${batchNumber}: No orphaned executions found`, {
          validCount: valid.length,
        });
      }

      batch = [];
    }
  }

  // Process remaining items
  if (batch.length > 0) {
    batchNumber++;

    const { orphaned, valid } = await checkBatch(tableName, batch);
    totalOrphaned += orphaned.length;
    totalValid += valid.length;

    if (orphaned.length > 0) {
      log.info(
        `Final batch ${batchNumber}: Found ${orphaned.length} orphaned executions`,
        {
          orphanedExecutionIds: orphaned.map((r) => r.executionId),
        },
      );

      orphanedDetails.push(
        ...orphaned.map((r) => ({
          executionId: r.executionId,
          county: r.county,
          openErrorCount: r.openErrorCount,
          uniqueErrorCount: r.uniqueErrorCount,
        })),
      );

      if (!dryRun) {
        const { deleted, alreadyDeleted, failed } = await deleteBatch(
          tableName,
          orphaned,
        );
        totalDeleted += deleted.length;
        totalAlreadyDeleted += alreadyDeleted.length;
        totalFailed += failed.length;
        failedItems.push(...failed);

        log.info(`Final batch ${batchNumber} deletion complete`, {
          deleted: deleted.length,
          alreadyDeleted: alreadyDeleted.length,
          failed: failed.length,
        });
      } else {
        log.info(
          `[DRY RUN] Final batch ${batchNumber}: Would delete ${orphaned.length} orphaned executions`,
        );
        totalDeleted += orphaned.length;
      }
    }
  }

  const duration = ((Date.now() - startTime) / 1000).toFixed(2);

  // Verification (skip in dry run)
  let verificationPassed = true;
  if (!dryRun && totalScanned > 0) {
    verificationPassed = await verifyCleanup(tableName);
  }

  // Orphaned details summary
  if (orphanedDetails.length > 0) {
    log.info("Orphaned executions found", {
      count: orphanedDetails.length,
      items: orphanedDetails.slice(0, 50),
    });
  }

  // Final summary
  log.summary("Cleanup Complete", {
    Duration: `${duration} seconds`,
    "Total Scanned": totalScanned,
    "Total Orphaned": totalOrphaned,
    "Total Valid (has error links)": totalValid,
    "Total Deleted": totalDeleted,
    "Already Deleted (skipped)": totalAlreadyDeleted,
    "Total Failed": totalFailed,
    "Total Batches": batchNumber,
    "Verification Passed": verificationPassed,
    "Dry Run": dryRun,
  });

  if (failedItems.length > 0) {
    log.error("Failed items summary", {
      count: failedItems.length,
      items: failedItems.slice(0, 20).map((f) => ({
        executionId: f.executionId,
        county: f.county,
        PK: f.PK,
        error: f.error,
      })),
    });
  }

  // Exit with error code if there were failures
  if (totalFailed > 0 || !verificationPassed) {
    log.error("Cleanup completed with errors");
    process.exit(1);
  }

  log.info("Cleanup completed successfully");
}

// Run cleanup
runCleanup().catch((error) => {
  log.error("Cleanup failed with unhandled error", {
    error: error.message,
    stack: error.stack,
  });
  process.exit(1);
});
