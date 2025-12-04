#!/usr/bin/env node
/**
 * E2E Test Script for workflow-events event-handler and get-execution lambdas.
 *
 * This script tests the complete flow:
 * 1. Fire WorkflowEvent events for 2 executions (one with 2 errors, one with 1 error)
 * 2. Query get-execution to verify "most" returns exec-1, "least" returns exec-2
 * 3. Fire ElephantErrorFailedToResolve event for exec-1
 * 4. Query again - both "most" and "least" should return exec-2
 *
 * Usage:
 *   node scripts/e2e-event-handler.mjs
 *
 * Prerequisites:
 *   - AWS credentials configured
 *   - workflow-events-stack deployed
 */

import {
  CloudFormationClient,
  DescribeStacksCommand,
} from "@aws-sdk/client-cloudformation";
import {
  EventBridgeClient,
  PutEventsCommand,
} from "@aws-sdk/client-eventbridge";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import {
  DynamoDBClient,
  QueryCommand,
  BatchWriteItemCommand,
} from "@aws-sdk/client-dynamodb";

// =============================================================================
// Configuration
// =============================================================================

const REGION = process.env.AWS_REGION || "us-east-1";
const STACK_NAME = "workflow-events-stack";
const TEST_PREFIX = `e2e-test-${Date.now()}`;
const EVENT_SOURCE = "elephant.workflow";

// Test data
const EXEC_1_ID = `${TEST_PREFIX}-exec-1`;
const EXEC_2_ID = `${TEST_PREFIX}-exec-2`;
const ERROR_CODE_1 = `TE${TEST_PREFIX}-error-1`;
const ERROR_CODE_2 = `TE${TEST_PREFIX}-error-2`;
const ERROR_CODE_3 = `TE${TEST_PREFIX}-error-3`;
const COUNTY = "TestCounty";

// Clients
const cfnClient = new CloudFormationClient({ region: REGION });
const eventBridgeClient = new EventBridgeClient({ region: REGION });
const lambdaClient = new LambdaClient({ region: REGION });
const dynamoDbClient = new DynamoDBClient({ region: REGION });

// =============================================================================
// Helpers
// =============================================================================

const log = (msg) => console.log(`[${new Date().toISOString()}] ${msg}`);
const success = (msg) => console.log(`[${new Date().toISOString()}] ✅ ${msg}`);
const error = (msg) => console.error(`[${new Date().toISOString()}] ❌ ${msg}`);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Get CloudFormation stack outputs.
 */
async function getStackOutputs() {
  const command = new DescribeStacksCommand({ StackName: STACK_NAME });
  const response = await cfnClient.send(command);
  const stack = response.Stacks?.[0];

  if (!stack) {
    throw new Error(`Stack ${STACK_NAME} not found`);
  }

  const outputs = {};
  for (const output of stack.Outputs || []) {
    if (output.OutputKey && output.OutputValue) {
      outputs[output.OutputKey] = output.OutputValue;
    }
  }

  return outputs;
}

/**
 * Send an event to EventBridge.
 */
async function sendEvent(detailType, detail) {
  const command = new PutEventsCommand({
    Entries: [
      {
        Source: EVENT_SOURCE,
        DetailType: detailType,
        Detail: JSON.stringify(detail),
      },
    ],
  });

  const response = await eventBridgeClient.send(command);

  if (response.FailedEntryCount && response.FailedEntryCount > 0) {
    throw new Error(`Failed to send event: ${JSON.stringify(response.Entries)}`);
  }

  log(`Sent ${detailType} event`);
  return response;
}

/**
 * Invoke get-execution Lambda.
 */
async function invokeGetExecution(functionArn, sortOrder, errorType) {
  const payload = { sortOrder };
  if (errorType) {
    payload.errorType = errorType;
  }

  const command = new InvokeCommand({
    FunctionName: functionArn,
    Payload: JSON.stringify(payload),
  });

  const response = await lambdaClient.send(command);
  const payloadString = new TextDecoder().decode(response.Payload);
  const result = JSON.parse(payloadString);

  if (response.FunctionError) {
    throw new Error(`Lambda error: ${payloadString}`);
  }

  return result;
}

/**
 * Purge test data from DynamoDB.
 */
async function purgeTestData(tableName) {
  log("Purging test data from DynamoDB...");

  // Query all items with our test prefix in PK
  const prefixes = ["EXECUTION#", "ERROR#"];
  let totalDeleted = 0;

  for (const prefix of prefixes) {
    let lastEvaluatedKey;

    do {
      const queryCommand = new QueryCommand({
        TableName: tableName,
        KeyConditionExpression: "begins_with(PK, :prefix)",
        ExpressionAttributeValues: {
          ":prefix": { S: prefix },
        },
        ExclusiveStartKey: lastEvaluatedKey,
      });

      // Use Scan instead since Query requires exact PK
      const { ScanCommand } = await import("@aws-sdk/client-dynamodb");
      const scanCommand = new ScanCommand({
        TableName: tableName,
        FilterExpression: "contains(PK, :testPrefix)",
        ExpressionAttributeValues: {
          ":testPrefix": { S: TEST_PREFIX },
        },
        ExclusiveStartKey: lastEvaluatedKey,
      });

      const response = await dynamoDbClient.send(scanCommand);
      const items = response.Items || [];

      if (items.length > 0) {
        // Delete items in batches of 25
        for (let i = 0; i < items.length; i += 25) {
          const batch = items.slice(i, i + 25);
          const deleteRequests = batch.map((item) => ({
            DeleteRequest: {
              Key: {
                PK: item.PK,
                SK: item.SK,
              },
            },
          }));

          const batchCommand = new BatchWriteItemCommand({
            RequestItems: {
              [tableName]: deleteRequests,
            },
          });

          await dynamoDbClient.send(batchCommand);
          totalDeleted += batch.length;
        }
      }

      lastEvaluatedKey = response.LastEvaluatedKey;
    } while (lastEvaluatedKey);
  }

  log(`Purged ${totalDeleted} test items`);
}

/**
 * Create a WorkflowEvent detail.
 */
function createWorkflowEvent(executionId, errors) {
  return {
    executionId,
    county: COUNTY,
    status: "FAILED",
    phase: "Transform",
    step: "ProcessData",
    errors: errors.map((code) => ({
      code,
      details: { testError: true, code },
    })),
  };
}

// =============================================================================
// Test Assertions
// =============================================================================

function assertEqual(actual, expected, message) {
  if (actual !== expected) {
    throw new Error(`${message}: expected "${expected}", got "${actual}"`);
  }
}

function assertNotNull(value, message) {
  if (value === null || value === undefined) {
    throw new Error(`${message}: expected non-null value`);
  }
}

function assertNull(value, message) {
  if (value !== null && value !== undefined) {
    throw new Error(`${message}: expected null, got "${JSON.stringify(value)}"`);
  }
}

// =============================================================================
// Main Test
// =============================================================================

async function main() {
  log("=".repeat(60));
  log("E2E Test: event-handler and get-execution");
  log("=".repeat(60));

  // Step 1: Get stack outputs
  log("\n📋 Step 1: Getting CloudFormation stack outputs...");
  const outputs = await getStackOutputs();

  const tableName = outputs.WorkflowErrorsTableName;
  const getExecutionArn = outputs.GetExecutionFunctionArn;

  if (!tableName || !getExecutionArn) {
    throw new Error(
      `Missing required stack outputs. Got: ${JSON.stringify(Object.keys(outputs))}`
    );
  }

  success(`Table: ${tableName}`);
  success(`GetExecution ARN: ${getExecutionArn}`);

  // Step 2: Purge any existing test data
  log("\n🧹 Step 2: Purging existing test data...");
  await purgeTestData(tableName);
  success("Test data purged");

  try {
    // Step 3: Send WorkflowEvent for execution 1 with 2 errors
    log("\n📤 Step 3: Sending WorkflowEvent for execution 1 (2 errors)...");
    await sendEvent(
      "WorkflowEvent",
      createWorkflowEvent(EXEC_1_ID, [ERROR_CODE_1, ERROR_CODE_2])
    );
    success(`Sent WorkflowEvent for ${EXEC_1_ID} with 2 errors`);

    // Step 4: Send WorkflowEvent for execution 2 with 1 error
    log("\n📤 Step 4: Sending WorkflowEvent for execution 2 (1 error)...");
    await sendEvent(
      "WorkflowEvent",
      createWorkflowEvent(EXEC_2_ID, [ERROR_CODE_3])
    );
    success(`Sent WorkflowEvent for ${EXEC_2_ID} with 1 error`);

    // Step 5: Wait for Lambda processing
    log("\n⏳ Step 5: Waiting for event processing (5 seconds)...");
    await sleep(5000);
    success("Wait complete");

    // Step 6: Query get-execution with "most" errors
    log("\n🔍 Step 6: Querying execution with MOST errors...");
    const mostResult = await invokeGetExecution(getExecutionArn, "most");

    log(`Result: ${JSON.stringify(mostResult, null, 2)}`);
    assertEqual(mostResult.success, true, "Query should succeed");
    assertNotNull(mostResult.execution, "Should find an execution");
    assertEqual(
      mostResult.execution.executionId,
      EXEC_1_ID,
      "Should return execution 1 (most errors)"
    );
    assertEqual(
      mostResult.execution.openErrorCount,
      2,
      "Execution 1 should have 2 open errors"
    );
    success(`Verified: execution with most errors is ${EXEC_1_ID}`);

    // Step 7: Query get-execution with "least" errors
    log("\n🔍 Step 7: Querying execution with LEAST errors...");
    const leastResult = await invokeGetExecution(getExecutionArn, "least");

    log(`Result: ${JSON.stringify(leastResult, null, 2)}`);
    assertEqual(leastResult.success, true, "Query should succeed");
    assertNotNull(leastResult.execution, "Should find an execution");
    assertEqual(
      leastResult.execution.executionId,
      EXEC_2_ID,
      "Should return execution 2 (least errors)"
    );
    assertEqual(
      leastResult.execution.openErrorCount,
      1,
      "Execution 2 should have 1 open error"
    );
    success(`Verified: execution with least errors is ${EXEC_2_ID}`);

    // Step 8: Send ElephantErrorFailedToResolve event for execution 1
    log("\n📤 Step 8: Sending ElephantErrorFailedToResolve for execution 1...");
    await sendEvent("ElephantErrorFailedToResolve", {
      executionId: EXEC_1_ID,
    });
    success(`Sent ElephantErrorFailedToResolve for ${EXEC_1_ID}`);

    // Step 9: Wait for Lambda processing
    log("\n⏳ Step 9: Waiting for event processing (5 seconds)...");
    await sleep(5000);
    success("Wait complete");

    // Step 10: Query get-execution with "most" errors again
    log(
      "\n🔍 Step 10: Querying execution with MOST errors (after failed-to-resolve)..."
    );
    const mostResult2 = await invokeGetExecution(getExecutionArn, "most");

    log(`Result: ${JSON.stringify(mostResult2, null, 2)}`);
    assertEqual(mostResult2.success, true, "Query should succeed");
    assertNotNull(mostResult2.execution, "Should find an execution");
    assertEqual(
      mostResult2.execution.executionId,
      EXEC_2_ID,
      "Should return execution 2 (exec-1 is now unrecoverable)"
    );
    success(
      `Verified: execution with most errors is now ${EXEC_2_ID} (exec-1 filtered out)`
    );

    // Step 11: Query get-execution with "least" errors again
    log(
      "\n🔍 Step 11: Querying execution with LEAST errors (after failed-to-resolve)..."
    );
    const leastResult2 = await invokeGetExecution(getExecutionArn, "least");

    log(`Result: ${JSON.stringify(leastResult2, null, 2)}`);
    assertEqual(leastResult2.success, true, "Query should succeed");
    assertNotNull(leastResult2.execution, "Should find an execution");
    assertEqual(
      leastResult2.execution.executionId,
      EXEC_2_ID,
      "Should return execution 2 (only one left with FAILED status)"
    );
    success(
      `Verified: execution with least errors is ${EXEC_2_ID} (same as most)`
    );

    log("\n" + "=".repeat(60));
    success("All tests passed! 🎉");
    log("=".repeat(60));
  } finally {
    // Cleanup: Purge test data
    log("\n🧹 Cleanup: Purging test data...");
    await purgeTestData(tableName);
    success("Test data cleaned up");
  }
}

// Run the test
main().catch((err) => {
  error(`Test failed: ${err.message}`);
  console.error(err.stack);
  process.exit(1);
});
