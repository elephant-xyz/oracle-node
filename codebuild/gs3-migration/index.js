/**
 * GS3 Partition Key Migration Script
 *
 * This script migrates ErrorRecord items to use a separate GS3 partition key
 * from FailedExecutionItem records, enabling partition-level separation for
 * efficient querying without FilterExpression.
 *
 * Migration Details:
 * - Before: ErrorRecord GS3PK = "METRIC#ERRORCOUNT"
 * - After:  ErrorRecord GS3PK = "METRIC#ERRORCOUNT#ERROR"
 *
 * FailedExecutionItem keeps GS3PK = "METRIC#ERRORCOUNT" (unchanged)
 *
 * This migration is idempotent - running it multiple times is safe.
 */

import { DynamoDBClient, ConditionalCheckFailedException } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  ScanCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";

// Constants
const OLD_GS3PK = "METRIC#ERRORCOUNT";
const NEW_GS3PK = "METRIC#ERRORCOUNT#ERROR";
const ENTITY_TYPE_ERROR = "Error";
const BATCH_SIZE = 25; // Number of concurrent updates

// Initialize DynamoDB client
const dynamoClient = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
  marshallOptions: { removeUndefinedValues: true },
});

/**
 * Logging utilities for clear, structured output
 */
const log = {
  info: (message, data = {}) => {
    console.log(JSON.stringify({ level: "INFO", message, ...data, timestamp: new Date().toISOString() }));
  },
  warn: (message, data = {}) => {
    console.warn(JSON.stringify({ level: "WARN", message, ...data, timestamp: new Date().toISOString() }));
  },
  error: (message, data = {}) => {
    console.error(JSON.stringify({ level: "ERROR", message, ...data, timestamp: new Date().toISOString() }));
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
 * Scan for ErrorRecord items that need migration
 * Only returns items where GS3PK = OLD_GS3PK and entityType = "Error"
 */
async function* scanErrorRecordsToMigrate(tableName) {
  let lastEvaluatedKey;
  let pageNumber = 0;

  do {
    pageNumber++;
    log.info(`Scanning page ${pageNumber}...`, { lastEvaluatedKey: !!lastEvaluatedKey });

    const command = new ScanCommand({
      TableName: tableName,
      FilterExpression: "entityType = :entityType AND GS3PK = :oldGS3PK",
      ExpressionAttributeValues: {
        ":entityType": ENTITY_TYPE_ERROR,
        ":oldGS3PK": OLD_GS3PK,
      },
      ProjectionExpression: "PK, SK, errorCode, GS3PK, GS3SK",
      ExclusiveStartKey: lastEvaluatedKey,
    });

    const response = await dynamoClient.send(command);

    if (response.Items && response.Items.length > 0) {
      log.info(`Page ${pageNumber}: Found ${response.Items.length} ErrorRecord items to migrate`, {
        scannedCount: response.ScannedCount,
        itemCount: response.Items.length,
      });
      yield* response.Items;
    } else {
      log.info(`Page ${pageNumber}: No items to migrate on this page`, {
        scannedCount: response.ScannedCount,
      });
    }

    lastEvaluatedKey = response.LastEvaluatedKey;
  } while (lastEvaluatedKey);

  log.info(`Scan complete after ${pageNumber} pages`);
}

/**
 * Update a single ErrorRecord's GS3PK
 */
async function updateErrorRecordGS3PK(tableName, item) {
  const { PK, SK, errorCode } = item;

  try {
    const command = new UpdateCommand({
      TableName: tableName,
      Key: { PK, SK },
      UpdateExpression: "SET GS3PK = :newGS3PK",
      ConditionExpression: "entityType = :entityType AND GS3PK = :oldGS3PK",
      ExpressionAttributeValues: {
        ":newGS3PK": NEW_GS3PK,
        ":entityType": ENTITY_TYPE_ERROR,
        ":oldGS3PK": OLD_GS3PK,
      },
    });

    await dynamoClient.send(command);
    return { success: true, errorCode, PK };
  } catch (error) {
    if (error instanceof ConditionalCheckFailedException) {
      // Item was already migrated or doesn't match criteria - this is expected for idempotency
      return { success: true, errorCode, PK, alreadyMigrated: true };
    }
    return { success: false, errorCode, PK, error: error.message };
  }
}

/**
 * Process items in batches for efficient migration
 */
async function processBatch(tableName, items) {
  const results = await Promise.all(
    items.map((item) => updateErrorRecordGS3PK(tableName, item))
  );

  const successful = results.filter((r) => r.success && !r.alreadyMigrated);
  const alreadyMigrated = results.filter((r) => r.success && r.alreadyMigrated);
  const failed = results.filter((r) => !r.success);

  return { successful, alreadyMigrated, failed };
}

/**
 * Verify migration by checking sample records
 */
async function verifyMigration(tableName) {
  log.info("Verifying migration...");

  // Check for any remaining ErrorRecords with old GS3PK
  const scanCommand = new ScanCommand({
    TableName: tableName,
    FilterExpression: "entityType = :entityType AND GS3PK = :oldGS3PK",
    ExpressionAttributeValues: {
      ":entityType": ENTITY_TYPE_ERROR,
      ":oldGS3PK": OLD_GS3PK,
    },
    Limit: 10,
  });

  const response = await dynamoClient.send(scanCommand);

  if (response.Items && response.Items.length > 0) {
    log.warn(`Found ${response.Items.length} ErrorRecord items still with old GS3PK`, {
      sampleErrorCodes: response.Items.slice(0, 5).map((i) => i.errorCode),
    });
    return false;
  }

  // Verify some records have new GS3PK
  const verifyCommand = new ScanCommand({
    TableName: tableName,
    FilterExpression: "entityType = :entityType AND GS3PK = :newGS3PK",
    ExpressionAttributeValues: {
      ":entityType": ENTITY_TYPE_ERROR,
      ":newGS3PK": NEW_GS3PK,
    },
    Limit: 10,
  });

  const verifyResponse = await dynamoClient.send(verifyCommand);
  const migratedCount = verifyResponse.Items?.length || 0;

  log.info(`Verification: Found ${migratedCount} ErrorRecord items with new GS3PK`);
  return true;
}

/**
 * Main migration function
 */
async function runMigration() {
  const startTime = Date.now();
  const tableName = requireEnv("ERRORS_TABLE_NAME");
  const dryRun = process.env.DRY_RUN === "true";

  log.summary("GS3 Partition Key Migration", {
    "Table Name": tableName,
    "Old GS3PK": OLD_GS3PK,
    "New GS3PK": NEW_GS3PK,
    "Dry Run": dryRun,
    "Batch Size": BATCH_SIZE,
    "Start Time": new Date().toISOString(),
  });

  if (dryRun) {
    log.warn("DRY RUN MODE - No changes will be made");
  }

  // Statistics
  let totalScanned = 0;
  let totalMigrated = 0;
  let totalAlreadyMigrated = 0;
  let totalFailed = 0;
  let batchNumber = 0;
  const failedItems = [];

  // Process items in batches
  let batch = [];

  for await (const item of scanErrorRecordsToMigrate(tableName)) {
    totalScanned++;
    batch.push(item);

    if (batch.length >= BATCH_SIZE) {
      batchNumber++;

      if (dryRun) {
        log.info(`[DRY RUN] Batch ${batchNumber}: Would migrate ${batch.length} items`, {
          sampleErrorCodes: batch.slice(0, 3).map((i) => i.errorCode),
        });
        totalMigrated += batch.length;
      } else {
        const { successful, alreadyMigrated, failed } = await processBatch(tableName, batch);
        totalMigrated += successful.length;
        totalAlreadyMigrated += alreadyMigrated.length;
        totalFailed += failed.length;
        failedItems.push(...failed);

        log.info(`Batch ${batchNumber} complete`, {
          migrated: successful.length,
          alreadyMigrated: alreadyMigrated.length,
          failed: failed.length,
          runningTotal: { migrated: totalMigrated, failed: totalFailed },
        });

        if (failed.length > 0) {
          log.error(`Batch ${batchNumber} had ${failed.length} failures`, {
            errors: failed.slice(0, 5).map((f) => ({ errorCode: f.errorCode, error: f.error })),
          });
        }
      }

      batch = [];
    }
  }

  // Process remaining items
  if (batch.length > 0) {
    batchNumber++;

    if (dryRun) {
      log.info(`[DRY RUN] Final batch ${batchNumber}: Would migrate ${batch.length} items`);
      totalMigrated += batch.length;
    } else {
      const { successful, alreadyMigrated, failed } = await processBatch(tableName, batch);
      totalMigrated += successful.length;
      totalAlreadyMigrated += alreadyMigrated.length;
      totalFailed += failed.length;
      failedItems.push(...failed);

      log.info(`Final batch ${batchNumber} complete`, {
        migrated: successful.length,
        alreadyMigrated: alreadyMigrated.length,
        failed: failed.length,
      });
    }
  }

  const duration = ((Date.now() - startTime) / 1000).toFixed(2);

  // Verification (skip in dry run)
  let verificationPassed = true;
  if (!dryRun && totalScanned > 0) {
    verificationPassed = await verifyMigration(tableName);
  }

  // Final summary
  log.summary("Migration Complete", {
    "Duration": `${duration} seconds`,
    "Total Scanned": totalScanned,
    "Total Migrated": totalMigrated,
    "Already Migrated (skipped)": totalAlreadyMigrated,
    "Total Failed": totalFailed,
    "Total Batches": batchNumber,
    "Verification Passed": verificationPassed,
    "Dry Run": dryRun,
  });

  if (failedItems.length > 0) {
    log.error("Failed items summary", {
      count: failedItems.length,
      items: failedItems.slice(0, 20).map((f) => ({
        errorCode: f.errorCode,
        PK: f.PK,
        error: f.error,
      })),
    });
  }

  // Exit with error code if there were failures
  if (totalFailed > 0 || !verificationPassed) {
    log.error("Migration completed with errors");
    process.exit(1);
  }

  log.info("Migration completed successfully");
}

// Run migration
runMigration().catch((error) => {
  log.error("Migration failed with unhandled error", { error: error.message, stack: error.stack });
  process.exit(1);
});
