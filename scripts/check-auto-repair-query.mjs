#!/usr/bin/env node
/**
 * Script to check what auto-repair query would return and verify validation errors in DynamoDB.
 * This replicates the exact query used by auto-repair to find failed executions.
 *
 * Usage: node scripts/check-auto-repair-query.mjs <account-id>
 * Example: node scripts/check-auto-repair-query.mjs 046651569570
 */

import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  DynamoDBClient,
  QueryCommand,
  ScanCommand,
} from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import {
  CloudFormationClient,
  DescribeStacksCommand,
} from "@aws-sdk/client-cloudformation";

/**
 * Parse accounts.yml file to get AWS credentials for an account.
 */
function getAccountCredentials(accountId) {
  try {
    const __filename = fileURLToPath(import.meta.url);
    const __dirname = dirname(__filename);
    const accountsPath = join(__dirname, "..", "accounts.yml");
    const accountsContent = readFileSync(accountsPath, "utf8");

    const lines = accountsContent.split("\n");
    let currentSection = null;
    const accounts = {};

    for (const line of lines) {
      const trimmed = line.trim();
      if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
        currentSection = trimmed.slice(1, -1);
        accounts[currentSection] = {};
      } else if (currentSection && trimmed.includes("=")) {
        const [key, ...valueParts] = trimmed.split("=");
        const value = valueParts.join("=").trim();
        accounts[currentSection][key.trim()] = value;
      }
    }

    const profileKey = `${accountId}-node`;
    if (!accounts[profileKey]) {
      console.error(`Account ${accountId} not found in accounts.yml`);
      return null;
    }

    return {
      accessKeyId: accounts[profileKey].aws_access_key_id,
      secretAccessKey: accounts[profileKey].aws_secret_access_key,
    };
  } catch (error) {
    console.error(`Error reading accounts.yml: ${error.message}`);
    return null;
  }
}

/**
 * Get table name from CloudFormation stack outputs.
 */
async function getTableNameFromStack(
  cloudFormationClient,
  stackName,
  outputKey,
) {
  try {
    const response = await cloudFormationClient.send(
      new DescribeStacksCommand({ StackName: stackName }),
    );

    if (!response.Stacks || response.Stacks.length === 0) {
      console.error(`Stack ${stackName} not found`);
      return null;
    }

    const outputs = response.Stacks[0].Outputs || [];
    const output = outputs.find((o) => o.OutputKey === outputKey);

    if (!output) {
      console.error(`Output ${outputKey} not found in stack ${stackName}`);
      return null;
    }

    return output.OutputValue;
  } catch (error) {
    console.error(`Error getting table name from stack: ${error.message}`);
    return null;
  }
}

/**
 * Replicate the exact auto-repair query.
 */
async function queryExecutionWithMostErrors(dynamoClient, tableName) {
  console.log("\n=== Running Auto-Repair Query ===");
  console.log("Query parameters:");
  console.log("  TableName:", tableName);
  console.log("  IndexName: ExecutionErrorCountIndex");
  console.log("  KeyConditionExpression: GS3PK = :pk");
  console.log(
    "  FilterExpression: #status = :status AND #entityType = :entityType AND (attribute_not_exists(errorsS3Uri) OR NOT contains(errorsS3Uri, :mvlSuffix))",
  );
  console.log("  ExpressionAttributeValues:");
  console.log("    :pk = METRIC#ERRORCOUNT");
  console.log("    :status = failed");
  console.log("    :entityType = FailedExecution");
  console.log("    :mvlSuffix = mvl_errors.csv");
  console.log(
    "  Note: Excluding mirror validation (MVL) errors - only schema validation errors",
  );
  console.log("  ScanIndexForward: false (descending order)");
  console.log("  Limit: 1");
  console.log();

  try {
    // Query without MVL filter first, then filter client-side (matching auto-repair logic)
    let lastEvaluatedKey = undefined;
    const maxItemsToCheck = 100;

    do {
      const response = await dynamoClient.send(
        new QueryCommand({
          TableName: tableName,
          IndexName: "ExecutionErrorCountIndex",
          KeyConditionExpression: "GS3PK = :pk",
          FilterExpression: "#status = :status AND #entityType = :entityType",
          ExpressionAttributeNames: {
            "#status": "status",
            "#entityType": "entityType",
          },
          ExpressionAttributeValues: {
            ":pk": { S: "METRIC#ERRORCOUNT" },
            ":status": { S: "failed" },
            ":entityType": { S: "FailedExecution" },
          },
          ScanIndexForward: false,
          Limit: maxItemsToCheck,
          ExclusiveStartKey: lastEvaluatedKey,
        }),
      );

      console.log("Query response:");
      console.log(`  Count: ${response.Count || 0}`);
      console.log(`  ScannedCount: ${response.ScannedCount || 0}`);
      console.log(`  Items returned: ${response.Items?.length || 0}`);

      if (response.Items && response.Items.length > 0) {
        // Filter out MVL errors client-side
        const filteredItems = [];
        for (const item of response.Items) {
          const errorsS3Uri = item.errorsS3Uri?.S;
          if (!errorsS3Uri || !errorsS3Uri.endsWith("mvl_errors.csv")) {
            filteredItems.push(item);
          }
        }

        if (filteredItems.length > 0) {
          console.log(
            `\n  After filtering out MVL errors: ${filteredItems.length} validation errors found`,
          );
          console.log(
            "\n=== First Execution Item (what auto-repair would get) ===",
          );
          const item = filteredItems[0];

          // Unmarshall manually to show structure
          const unmarshalled = {};
          for (const [key, value] of Object.entries(item)) {
            if (value.S) unmarshalled[key] = value.S;
            else if (value.N) unmarshalled[key] = Number(value.N);
            else if (value.M) {
              unmarshalled[key] = {};
              for (const [subKey, subValue] of Object.entries(value.M)) {
                if (subValue.S) unmarshalled[key][subKey] = subValue.S;
                else if (subValue.N)
                  unmarshalled[key][subKey] = Number(subValue.N);
              }
            } else unmarshalled[key] = value;
          }

          console.log(JSON.stringify(unmarshalled, null, 2));
          return unmarshalled;
        } else {
          console.log(
            `\n  All ${response.Items.length} items in this batch were MVL errors, checking next batch...`,
          );
          lastEvaluatedKey = response.LastEvaluatedKey;
          continue;
        }
      } else {
        console.log(
          "\nNo items returned - this is why auto-repair says 'No failed executions found'",
        );
        return null;
      }
    } while (lastEvaluatedKey);

    console.log("\nNo validation errors found (only MVL errors exist)");
    return null;
  } catch (error) {
    console.error(`Query error: ${error.message}`);
    throw error;
  }
}

/**
 * Count all failed executions without limit.
 */
async function countAllFailedExecutions(dynamoClient, tableName) {
  console.log("\n=== Counting All Failed Executions ===");

  let count = 0;
  let scannedCount = 0;
  let lastEvaluatedKey = undefined;

  do {
    try {
      const response = await dynamoClient.send(
        new QueryCommand({
          TableName: tableName,
          IndexName: "ExecutionErrorCountIndex",
          KeyConditionExpression: "GS3PK = :pk",
          FilterExpression:
            "#status = :status AND #entityType = :entityType AND (attribute_not_exists(errorsS3Uri) OR NOT contains(errorsS3Uri, :mvlSuffix))",
          ExpressionAttributeNames: {
            "#status": "status",
            "#entityType": "entityType",
          },
          ExpressionAttributeValues: {
            ":pk": { S: "METRIC#ERRORCOUNT" },
            ":status": { S: "failed" },
            ":entityType": { S: "FailedExecution" },
            ":mvlSuffix": { S: "mvl_errors.csv" },
          },
          ScanIndexForward: false,
          Select: "COUNT",
          ExclusiveStartKey: lastEvaluatedKey,
        }),
      );

      count += response.Count || 0;
      scannedCount += response.ScannedCount || 0;
      lastEvaluatedKey = response.LastEvaluatedKey;

      if (scannedCount % 100 === 0) {
        console.log(
          `  Scanned ${scannedCount} items, found ${count} failed executions...`,
        );
      }
    } catch (error) {
      console.error(`Error counting: ${error.message}`);
      break;
    }
  } while (lastEvaluatedKey);

  return { count, scannedCount };
}

/**
 * Check what statuses exist in the table (sampled, not full scan).
 */
async function checkStatuses(dynamoClient, tableName) {
  console.log("\n=== Checking Execution Statuses (sampling first 1000) ===");

  const statusCounts = {};
  let lastEvaluatedKey = undefined;
  let totalScanned = 0;
  const maxSample = 1000; // Only sample first 1000 items for speed

  do {
    try {
      const response = await dynamoClient.send(
        new ScanCommand({
          TableName: tableName,
          FilterExpression: "#entityType = :entityType",
          ExpressionAttributeNames: {
            "#entityType": "entityType",
            "#status": "status",
          },
          ExpressionAttributeValues: {
            ":entityType": { S: "FailedExecution" },
          },
          ProjectionExpression: "#status, #entityType",
          ExclusiveStartKey: lastEvaluatedKey,
          Limit: 100, // Process in smaller batches
        }),
      );

      if (response.Items) {
        for (const item of response.Items) {
          const status = item.status?.S || "unknown";
          statusCounts[status] = (statusCounts[status] || 0) + 1;
        }
      }

      totalScanned += response.ScannedCount || 0;
      lastEvaluatedKey = response.LastEvaluatedKey;

      // Stop after sampling maxSample items
      if (totalScanned >= maxSample) {
        console.log(
          `  Sampled ${totalScanned} items (showing sample, not full table)`,
        );
        break;
      }
    } catch (error) {
      console.error(`Error checking statuses: ${error.message}`);
      break;
    }
  } while (lastEvaluatedKey);

  return { statusCounts, sampleSize: totalScanned };
}

/**
 * Main function.
 */
async function main() {
  const accountId = process.argv[2];

  if (!accountId) {
    console.error(
      "Usage: node scripts/check-auto-repair-query.mjs <account-id>",
    );
    console.error(
      "Example: node scripts/check-auto-repair-query.mjs 046651569570",
    );
    process.exit(1);
  }

  console.log(
    `\n=== Checking Auto-Repair Query for Account: ${accountId} ===\n`,
  );

  // Get credentials
  const credentials = getAccountCredentials(accountId);
  if (!credentials) {
    process.exit(1);
  }

  // Set up AWS clients
  const region = process.env.AWS_REGION || "us-east-1";
  const stackName = process.env.STACK_NAME || "elephant-oracle-node";

  const cloudFormationClient = new CloudFormationClient({
    region,
    credentials: {
      accessKeyId: credentials.accessKeyId,
      secretAccessKey: credentials.secretAccessKey,
    },
  });

  const dynamoClient = new DynamoDBClient({
    region,
    credentials: {
      accessKeyId: credentials.accessKeyId,
      secretAccessKey: credentials.secretAccessKey,
    },
  });

  const documentClient = DynamoDBDocumentClient.from(dynamoClient, {
    marshallOptions: { removeUndefinedValues: true },
  });

  // Get table name
  console.log(`Getting table name from stack: ${stackName}...`);
  const tableName = await getTableNameFromStack(
    cloudFormationClient,
    stackName,
    "ErrorsTableName",
  );

  if (!tableName) {
    console.error("Could not get ErrorsTableName from stack");
    process.exit(1);
  }

  console.log(`Table name: ${tableName}\n`);

  // First, let's check what types of errors exist
  console.log("\n=== Diagnosing Error Types ===");
  let totalFailed = 0;
  let withMvlErrors = 0;
  let withSubmitErrors = 0;
  let withoutErrorsUri = 0;
  let diagnosticLastKey = undefined;
  let diagnosticScanned = 0;

  do {
    try {
      const diagResponse = await dynamoClient.send(
        new QueryCommand({
          TableName: tableName,
          IndexName: "ExecutionErrorCountIndex",
          KeyConditionExpression: "GS3PK = :pk",
          FilterExpression: "#status = :status AND #entityType = :entityType",
          ExpressionAttributeNames: {
            "#status": "status",
            "#entityType": "entityType",
          },
          ExpressionAttributeValues: {
            ":pk": { S: "METRIC#ERRORCOUNT" },
            ":status": { S: "failed" },
            ":entityType": { S: "FailedExecution" },
          },
          ScanIndexForward: false,
          Select: "ALL_ATTRIBUTES",
          Limit: 100,
          ExclusiveStartKey: diagnosticLastKey,
        }),
      );

      if (diagResponse.Items) {
        for (const item of diagResponse.Items) {
          totalFailed++;
          const errorsS3Uri = item.errorsS3Uri?.S;

          if (!errorsS3Uri) {
            withoutErrorsUri++;
          } else if (errorsS3Uri.includes("mvl_errors.csv")) {
            withMvlErrors++;
          } else {
            withSubmitErrors++;
          }
        }
      }

      diagnosticScanned += diagResponse.ScannedCount || 0;
      diagnosticLastKey = diagResponse.LastEvaluatedKey;

      // Only sample first 1000 for diagnostics
      if (diagnosticScanned >= 1000) {
        break;
      }
    } catch (error) {
      console.error(`Diagnostic query error: ${error.message}`);
      break;
    }
  } while (diagnosticLastKey && diagnosticScanned < 1000);

  console.log(
    `\nDiagnostic Results (from sample of ${diagnosticScanned} scanned items):`,
  );
  console.log(`  Total failed executions sampled: ${totalFailed}`);
  console.log(`  With MVL errors (mvl_errors.csv): ${withMvlErrors}`);
  console.log(
    `  With submit errors (submit_errors.csv or other): ${withSubmitErrors}`,
  );
  console.log(`  Without errorsS3Uri: ${withoutErrorsUri}`);

  // Run the exact auto-repair query (using low-level client like in script)
  console.log("\n--- Using low-level DynamoDB client (like in script) ---");
  const execution = await queryExecutionWithMostErrors(dynamoClient, tableName);

  // Also try with documentClient (like auto-repair uses)
  console.log("\n--- Using DynamoDBDocumentClient (like auto-repair) ---");
  try {
    const { QueryCommand: DocQueryCommand } = await import(
      "@aws-sdk/lib-dynamodb"
    );
    const docResponse = await documentClient.send(
      new DocQueryCommand({
        TableName: tableName,
        IndexName: "ExecutionErrorCountIndex",
        KeyConditionExpression: "GS3PK = :pk",
        FilterExpression:
          "#status = :status AND #entityType = :entityType AND (attribute_not_exists(errorsS3Uri) OR NOT contains(errorsS3Uri, :mvlSuffix))",
        ExpressionAttributeNames: {
          "#status": "status",
          "#entityType": "entityType",
        },
        ExpressionAttributeValues: {
          ":pk": "METRIC#ERRORCOUNT",
          ":status": "failed",
          ":entityType": "FailedExecution",
          ":mvlSuffix": "mvl_errors.csv",
        },
        ScanIndexForward: false,
        Limit: 1,
      }),
    );

    console.log(`DocumentClient query result:`);
    console.log(`  Count: ${docResponse.Count || 0}`);
    console.log(`  ScannedCount: ${docResponse.ScannedCount || 0}`);
    console.log(`  Items returned: ${docResponse.Items?.length || 0}`);

    if (docResponse.Items && docResponse.Items.length > 0) {
      const item = docResponse.Items[0];
      console.log("\nExecution from DocumentClient:");
      console.log(JSON.stringify(item, null, 2));

      // Check if it's MVL or validation error
      if (item.errorsS3Uri) {
        if (item.errorsS3Uri.endsWith("mvl_errors.csv")) {
          console.log(
            "\n⚠️  WARNING: This is a MVL (mirror validation) error, not a schema validation error!",
          );
        } else {
          console.log("\n✅ This is a schema validation error (not MVL)");
        }
      }
    } else {
      console.log("\n⚠️  DocumentClient query returned no items");
    }
  } catch (error) {
    console.error(`DocumentClient query error: ${error.message}`);
  }

  // Count all failed executions (validation only, excluding MVL)
  const { count, scannedCount } = await countAllFailedExecutions(
    dynamoClient,
    tableName,
  );
  console.log(
    `\nTotal Failed Executions (Schema Validation Only): ${count.toLocaleString()}`,
  );
  console.log(`Total Items Scanned: ${scannedCount.toLocaleString()}`);

  // Also count MVL errors separately
  console.log("\n=== Counting MVL (Mirror Validation) Errors ===");
  let mvlCount = 0;
  let mvlLastEvaluatedKey = undefined;
  do {
    try {
      const mvlResponse = await dynamoClient.send(
        new QueryCommand({
          TableName: tableName,
          IndexName: "ExecutionErrorCountIndex",
          KeyConditionExpression: "GS3PK = :pk",
          FilterExpression:
            "#status = :status AND #entityType = :entityType AND contains(errorsS3Uri, :mvlSuffix)",
          ExpressionAttributeNames: {
            "#status": "status",
            "#entityType": "entityType",
          },
          ExpressionAttributeValues: {
            ":pk": { S: "METRIC#ERRORCOUNT" },
            ":status": { S: "failed" },
            ":entityType": { S: "FailedExecution" },
            ":mvlSuffix": { S: "mvl_errors.csv" },
          },
          ScanIndexForward: false,
          Select: "COUNT",
          ExclusiveStartKey: mvlLastEvaluatedKey,
        }),
      );
      mvlCount += mvlResponse.Count || 0;
      mvlLastEvaluatedKey = mvlResponse.LastEvaluatedKey;
    } catch (error) {
      console.error(`Error counting MVL errors: ${error.message}`);
      break;
    }
  } while (mvlLastEvaluatedKey);
  console.log(
    `Total MVL (Mirror Validation) Errors: ${mvlCount.toLocaleString()}`,
  );

  // Check what statuses exist (sampled)
  const { statusCounts, sampleSize } = await checkStatuses(
    dynamoClient,
    tableName,
  );
  console.log(
    `\nExecution Status Breakdown (from sample of ${sampleSize.toLocaleString()} items):`,
  );
  for (const [status, count] of Object.entries(statusCounts)) {
    console.log(`  ${status}: ${count.toLocaleString()}`);
  }

  // Summary
  console.log("\n" + "=".repeat(80));
  console.log("SUMMARY");
  console.log("=".repeat(80));
  console.log(
    `Auto-repair query result (low-level client): ${execution ? "FOUND" : "NOT FOUND"}`,
  );
  console.log(`Total schema validation errors: ${count.toLocaleString()}`);
  console.log(
    `Total MVL (mirror validation) errors: ${mvlCount.toLocaleString()}`,
  );
  console.log(
    `Total all failed executions: ${(count + mvlCount).toLocaleString()}`,
  );
  console.log(`Total executions scanned: ${scannedCount.toLocaleString()}`);

  if (count === 0) {
    console.log(
      "\n⚠️  No failed executions found with status='failed' and entityType='FailedExecution'",
    );
    console.log("This could mean:");
    console.log("  1. All executions have been processed/fixed");
    console.log(
      "  2. Executions have different status (maybeSolved, solved, etc.)",
    );
    console.log("  3. Executions have different entityType");
  } else if (count > 0 && !execution) {
    console.log("\n⚠️  Found failed executions but query returned none");
    console.log("This could mean:");
    console.log("  1. The GSI might not be fully populated");
    console.log("  2. The filter expression might be too restrictive");
  } else if (count > 0 && execution) {
    console.log(
      "\n✅ Query is working correctly - found execution with most errors",
    );
    console.log(
      "If auto-repair still says 'No failed executions found', check:",
    );
    console.log("  1. ERRORS_TABLE_NAME environment variable in CodeBuild");
    console.log("  2. AWS credentials/permissions in CodeBuild");
    console.log("  3. Region mismatch between script and CodeBuild");
  }

  console.log("=".repeat(80) + "\n");
}

main().catch((error) => {
  console.error("Error:", error);
  process.exit(1);
});
