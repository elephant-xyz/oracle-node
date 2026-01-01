import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import {
  DynamoDBDocumentClient,
  TransactWriteCommand,
  UpdateCommand,
  BatchGetCommand,
  QueryCommand,
  GetCommand,
} from "@aws-sdk/lib-dynamodb";
import {
  CloudWatchClient,
  PutMetricDataCommand,
} from "@aws-sdk/client-cloudwatch";
import type { EventBridgeEvent } from "aws-lambda";
import type { WorkflowEventDetail, WorkflowError } from "shared/types.js";

/**
 * Mock the DynamoDB Document Client for all tests.
 */
const ddbMock = mockClient(DynamoDBDocumentClient);

/**
 * Mock the CloudWatch Client for all tests.
 */
const cloudWatchMock = mockClient(CloudWatchClient);

/**
 * Test table name used in all tests.
 */
const TEST_TABLE_NAME = "test-workflow-errors-table";

/**
 * Creates a mock EventBridge event with the given workflow event detail.
 * @param detail - The workflow event detail
 * @returns A mock EventBridge event
 */
const createMockEvent = (
  detail: WorkflowEventDetail,
): EventBridgeEvent<"WorkflowEvent", WorkflowEventDetail> => ({
  id: "test-event-id-12345",
  version: "0",
  account: "123456789012",
  time: new Date().toISOString(),
  region: "us-east-1",
  source: "elephant.workflow",
  "detail-type": "WorkflowEvent",
  resources: [],
  detail,
});

/**
 * Creates a workflow event detail with optional overrides.
 * @param overrides - Partial overrides for the detail
 * @returns A complete WorkflowEventDetail object
 */
const createWorkflowDetail = (
  overrides: Partial<WorkflowEventDetail> = {},
): WorkflowEventDetail => ({
  executionId: "exec-001",
  county: "palm_beach",
  status: "FAILED",
  phase: "scrape",
  step: "login",
  errors: [],
  ...overrides,
});

/**
 * Creates a workflow error with the given code and optional details.
 * @param code - The error code (digit-like format)
 * @param details - Optional error details
 * @returns A WorkflowError object
 */
const createError = (
  code: string,
  details: Record<string, unknown> = {},
): WorkflowError => ({
  code,
  details,
});

describe("event-handler", () => {
  const originalEnv = process.env;

  /**
   * Tracks the current total count for each error code across test operations.
   * Used by the BatchGetCommand mock to return the correct totalCount after transactions.
   */
  let errorCountTracker: Map<string, number>;

  beforeEach(() => {
    vi.resetModules();
    ddbMock.reset();
    cloudWatchMock.reset();
    errorCountTracker = new Map();
    process.env = {
      ...originalEnv,
      WORKFLOW_ERRORS_TABLE_NAME: TEST_TABLE_NAME,
    };
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "info").mockImplementation(() => {});
    vi.spyOn(console, "debug").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});

    // Default mock for CloudWatch PutMetricData
    cloudWatchMock.on(PutMetricDataCommand).resolves({});

    // Default mock for BatchGetCommand - returns totalCount from tracker
    // This simulates reading back the error records after a transaction
    ddbMock.on(BatchGetCommand).callsFake((input) => {
      const keys = input.RequestItems?.[TEST_TABLE_NAME]?.Keys ?? [];
      const items = keys.map((key: { PK: string; SK: string }) => {
        // Extract error code from PK (format: ERROR#{errorCode})
        const errorCode = key.PK.replace("ERROR#", "");
        const totalCount = errorCountTracker.get(errorCode) ?? 1;
        return { errorCode, totalCount };
      });
      return { Responses: { [TEST_TABLE_NAME]: items } };
    });

    // Default mock for UpdateCommand (for updateErrorRecordSortKey calls)
    ddbMock.on(UpdateCommand).resolves({});
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  describe("handler integration tests", () => {
    it("should process event with errors and save to DynamoDB", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { handler } =
        await import("../../../../workflow-events/lambdas/event-handler/index.js");

      const detail = createWorkflowDetail({
        executionId: "exec-integration-001",
        errors: [createError("01256", { reason: "timeout" })],
      });
      const event = createMockEvent(detail);

      await handler(event);

      expect(ddbMock).toHaveReceivedCommand(TransactWriteCommand);
    });

    it("should skip DynamoDB save when event has no errors", async () => {
      const { handler } =
        await import("../../../../workflow-events/lambdas/event-handler/index.js");

      const detail = createWorkflowDetail({
        executionId: "exec-no-errors-001",
        errors: [],
      });
      const event = createMockEvent(detail);

      await handler(event);

      expect(ddbMock).not.toHaveReceivedCommand(TransactWriteCommand);
    });

    it("should propagate DynamoDB errors to caller", async () => {
      ddbMock
        .on(TransactWriteCommand)
        .rejects(new Error("DynamoDB transaction failed"));

      const { handler } =
        await import("../../../../workflow-events/lambdas/event-handler/index.js");

      const detail = createWorkflowDetail({
        executionId: "exec-error-001",
        errors: [createError("99999")],
      });
      const event = createMockEvent(detail);

      await expect(handler(event)).rejects.toThrow(
        "DynamoDB transaction failed",
      );
    });

    it("should throw error when WORKFLOW_ERRORS_TABLE_NAME is not set", async () => {
      delete process.env.WORKFLOW_ERRORS_TABLE_NAME;

      const { handler } =
        await import("../../../../workflow-events/lambdas/event-handler/index.js");

      const detail = createWorkflowDetail({
        errors: [createError("01256")],
      });
      const event = createMockEvent(detail);

      await expect(handler(event)).rejects.toThrow(
        "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
      );
    });
  });

  describe("ErrorRecord retry behavior", () => {
    /**
     * ErrorRecord updates are executed individually with retry logic.
     * These tests verify the retry behavior for transient DynamoDB errors.
     */

    it("should succeed after retrying on ProvisionedThroughputExceededException", async () => {
      const throttleError = new Error("Rate exceeded");
      throttleError.name = "ProvisionedThroughputExceededException";

      // First 2 calls fail with throttling, third succeeds
      let errorRecordCallCount = 0;
      ddbMock.on(UpdateCommand).callsFake((input) => {
        // ErrorRecord updates have :entityType = "Error"
        if (input.ExpressionAttributeValues?.[":entityType"] === "Error") {
          errorRecordCallCount++;
          if (errorRecordCallCount <= 2) {
            throw throttleError;
          }
        }
        return {};
      });
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-retry-001",
        errors: [createError("12345")],
      });

      const result = await saveErrorRecords(detail);

      expect(result.success).toBe(true);
      // ErrorRecord update should have been called 3 times (2 failures + 1 success)
      expect(errorRecordCallCount).toBe(3);
    });

    it("should succeed after retrying on ThrottlingException", async () => {
      const throttleError = new Error("Throttled");
      throttleError.name = "ThrottlingException";

      let errorRecordCallCount = 0;
      ddbMock.on(UpdateCommand).callsFake((input) => {
        if (input.ExpressionAttributeValues?.[":entityType"] === "Error") {
          errorRecordCallCount++;
          if (errorRecordCallCount === 1) {
            throw throttleError;
          }
        }
        return {};
      });
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-retry-002",
        errors: [createError("12345")],
      });

      const result = await saveErrorRecords(detail);

      expect(result.success).toBe(true);
      expect(errorRecordCallCount).toBe(2);
    });

    it("should not retry on non-retryable errors", async () => {
      const validationError = new Error("Validation failed");
      validationError.name = "ValidationException";

      let errorRecordCallCount = 0;
      ddbMock.on(UpdateCommand).callsFake((input) => {
        if (input.ExpressionAttributeValues?.[":entityType"] === "Error") {
          errorRecordCallCount++;
          throw validationError;
        }
        return {};
      });
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-no-retry-001",
        errors: [createError("12345")],
      });

      await expect(saveErrorRecords(detail)).rejects.toThrow(
        "Validation failed",
      );
      // Should only be called once (no retries for non-retryable errors)
      expect(errorRecordCallCount).toBe(1);
    });

    it("should throw error after exhausting all retries", async () => {
      const throttleError = new Error("Persistent throttling");
      throttleError.name = "ProvisionedThroughputExceededException";

      let errorRecordCallCount = 0;
      ddbMock.on(UpdateCommand).callsFake((input) => {
        if (input.ExpressionAttributeValues?.[":entityType"] === "Error") {
          errorRecordCallCount++;
          throw throttleError;
        }
        return {};
      });
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-exhaust-001",
        errors: [createError("12345")],
      });

      await expect(saveErrorRecords(detail)).rejects.toThrow(
        "Persistent throttling",
      );
      // Should have tried maxRetries + 1 times (initial + 10 retries = 11)
      expect(errorRecordCallCount).toBe(11);
    }, 60000); // Increase timeout for retry delays

    it("should retry on TransactionCanceledException", async () => {
      const transactionError = new Error("Transaction cancelled");
      transactionError.name = "TransactionCanceledException";

      let transactionCallCount = 0;
      ddbMock.on(TransactWriteCommand).callsFake(() => {
        transactionCallCount++;
        if (transactionCallCount === 1) {
          throw transactionError;
        }
        return {};
      });
      ddbMock.on(UpdateCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-transaction-retry-001",
        errors: [createError("12345")],
      });

      const result = await saveErrorRecords(detail);

      expect(result.success).toBe(true);
      // Transaction should have been called twice (1 failure + 1 success)
      expect(transactionCallCount).toBe(2);
    });

    it("should retry each ErrorRecord independently", async () => {
      const throttleError = new Error("Throttled");
      throttleError.name = "ThrottlingException";

      const callCounts: Record<string, number> = {};
      ddbMock.on(UpdateCommand).callsFake((input) => {
        if (input.ExpressionAttributeValues?.[":entityType"] === "Error") {
          const pk = input.Key?.PK as string;
          callCounts[pk] = (callCounts[pk] || 0) + 1;
          // Only fail error "11111" on first attempt
          if (pk === "ERROR#11111" && callCounts[pk] === 1) {
            throw throttleError;
          }
        }
        return {};
      });
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-independent-001",
        errors: [createError("11111"), createError("22222")],
      });

      const result = await saveErrorRecords(detail);

      expect(result.success).toBe(true);
      // Error "11111" should have been called twice (1 failure + 1 success)
      expect(callCounts["ERROR#11111"]).toBe(2);
      // Error "22222" should have been called once (immediate success)
      expect(callCounts["ERROR#22222"]).toBe(1);
    });
  });

  describe("single error execution", () => {
    it("should create FailedExecutionItem, ErrorRecord, and ExecutionErrorLink for single error", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-single-001",
        county: "palm_beach",
        errors: [createError("01256", { reason: "login timeout" })],
      });

      const result = await saveErrorRecords(detail);

      expect(result.success).toBe(true);
      expect(result.uniqueErrorCount).toBe(1);
      expect(result.totalOccurrences).toBe(1);
      expect(result.errorCodes).toEqual(["01256"]);

      // Transaction contains: 1 FailedExecutionItem + 1 ExecutionErrorLink = 2 items
      // ErrorRecord is updated separately via UpdateCommand to avoid TransactionConflict
      expect(ddbMock).toHaveReceivedCommandTimes(TransactWriteCommand, 1);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      expect(transactItems).toHaveLength(2);

      // Verify FailedExecutionItem
      const failedExecutionItem = transactItems![0].Update;
      expect(failedExecutionItem?.Key).toEqual({
        PK: "EXECUTION#exec-single-001",
        SK: "EXECUTION#exec-single-001",
      });
      expect(
        failedExecutionItem?.ExpressionAttributeValues?.[":entityType"],
      ).toBe("FailedExecution");
      expect(
        failedExecutionItem?.ExpressionAttributeValues?.[":uniqueErrorCount"],
      ).toBe(1);
      expect(
        failedExecutionItem?.ExpressionAttributeValues?.[":totalOccurrences"],
      ).toBe(1);

      // Verify ExecutionErrorLink (now at index 1)
      const executionErrorLink = transactItems![1].Update;
      expect(executionErrorLink?.Key).toEqual({
        PK: "EXECUTION#exec-single-001",
        SK: "ERROR#01256",
      });
      expect(
        executionErrorLink?.ExpressionAttributeValues?.[":entityType"],
      ).toBe("ExecutionError");
      expect(
        executionErrorLink?.ExpressionAttributeValues?.[":occurrences"],
      ).toBe(1);
      expect(
        executionErrorLink?.ExpressionAttributeValues?.[":errorDetails"],
      ).toBe(JSON.stringify({ reason: "login timeout" }));

      // Verify ErrorRecord is updated via separate UpdateCommand (not in transaction)
      // UpdateCommand is called for: 1 ErrorRecord update + 1 GS2SK/GS3SK refresh = 2 calls
      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      const errorRecordUpdate = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "ERROR#01256" &&
          call.args[0].input.ExpressionAttributeValues?.[":entityType"] ===
            "Error",
      );
      expect(errorRecordUpdate).toBeDefined();
      expect(
        errorRecordUpdate?.args[0].input.ExpressionAttributeValues?.[
          ":errorCode"
        ],
      ).toBe("01256");
      expect(
        errorRecordUpdate?.args[0].input.ExpressionAttributeValues?.[
          ":errorDetails"
        ],
      ).toBe(JSON.stringify({ reason: "login timeout" }));
    });

    it("should include taskToken in FailedExecutionItem when provided", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-with-token-001",
        taskToken: "arn:aws:states:task-token-12345",
        errors: [createError("01256")],
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;
      const failedExecutionItem = transactItems![0].Update;

      expect(
        failedExecutionItem?.ExpressionAttributeValues?.[":taskToken"],
      ).toBe("arn:aws:states:task-token-12345");
    });
  });

  describe("multiple unique errors execution", () => {
    it("should create separate ErrorRecord and ExecutionErrorLink for each unique error", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-multi-001",
        errors: [
          createError("01256", { field: "username" }),
          createError("23456", { field: "password" }),
          createError("34567", { field: "captcha" }),
        ],
      });

      const result = await saveErrorRecords(detail);

      expect(result.success).toBe(true);
      expect(result.uniqueErrorCount).toBe(3);
      expect(result.totalOccurrences).toBe(3);
      expect(result.errorCodes).toContain("01256");
      expect(result.errorCodes).toContain("23456");
      expect(result.errorCodes).toContain("34567");

      // Transaction contains: 1 FailedExecutionItem + 3 ExecutionErrorLinks = 4 items
      // ErrorRecords are updated separately via UpdateCommand
      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      expect(transactItems).toHaveLength(4);

      // Verify FailedExecutionItem counts
      const failedExecutionItem = transactItems![0].Update;
      expect(
        failedExecutionItem?.ExpressionAttributeValues?.[":uniqueErrorCount"],
      ).toBe(3);
      expect(
        failedExecutionItem?.ExpressionAttributeValues?.[":totalOccurrences"],
      ).toBe(3);

      // Verify ErrorRecords are updated via separate UpdateCommand calls
      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      const errorRecordUpdates = updateCalls.filter(
        (call) =>
          call.args[0].input.ExpressionAttributeValues?.[":entityType"] ===
          "Error",
      );
      expect(errorRecordUpdates).toHaveLength(3);
    });
  });

  describe("repeated errors execution (unique count aggregation)", () => {
    it("should aggregate repeated errors and count unique errors correctly", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      // 5 total errors, but only 2 unique error codes
      const detail = createWorkflowDetail({
        executionId: "exec-repeated-001",
        errors: [
          createError("01256", { attempt: 1 }),
          createError("01256", { attempt: 2 }),
          createError("01256", { attempt: 3 }),
          createError("23456", { attempt: 1 }),
          createError("23456", { attempt: 2 }),
        ],
      });

      const result = await saveErrorRecords(detail);

      expect(result.success).toBe(true);
      expect(result.uniqueErrorCount).toBe(2);
      expect(result.totalOccurrences).toBe(5);
      expect(result.errorCodes).toHaveLength(2);

      // Transaction contains: 1 FailedExecutionItem + 2 ExecutionErrorLinks = 3 items
      // ErrorRecords are updated separately via UpdateCommand
      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      expect(transactItems).toHaveLength(3);

      // Verify FailedExecutionItem
      const failedExecutionItem = transactItems![0].Update;
      expect(
        failedExecutionItem?.ExpressionAttributeValues?.[":uniqueErrorCount"],
      ).toBe(2);
      expect(
        failedExecutionItem?.ExpressionAttributeValues?.[":totalOccurrences"],
      ).toBe(5);
    });

    it("should set occurrence count per error code in ExecutionErrorLink", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      // Error "01256" occurs 3 times, "23456" occurs 1 time
      const detail = createWorkflowDetail({
        executionId: "exec-occurrences-001",
        errors: [
          createError("01256"),
          createError("01256"),
          createError("01256"),
          createError("23456"),
        ],
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      // Find ExecutionErrorLink items (they have SK starting with ERROR#)
      const executionErrorLinks = transactItems!
        .filter((item) => {
          const key = item.Update?.Key;
          return (
            key?.PK?.startsWith("EXECUTION#") && key?.SK?.startsWith("ERROR#")
          );
        })
        .map((item) => item.Update);

      // Find the link for error "01256"
      const link01256 = executionErrorLinks.find(
        (link) => link?.Key?.SK === "ERROR#01256",
      );
      expect(link01256?.ExpressionAttributeValues?.[":occurrences"]).toBe(3);

      // Find the link for error "23456"
      const link23456 = executionErrorLinks.find(
        (link) => link?.Key?.SK === "ERROR#23456",
      );
      expect(link23456?.ExpressionAttributeValues?.[":occurrences"]).toBe(1);
    });

    it("should use first occurrence details for error record when same error repeats", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-details-001",
        errors: [
          createError("01256", { attempt: 1, first: true }),
          createError("01256", { attempt: 2, second: true }),
        ],
      });

      await saveErrorRecords(detail);

      // ErrorRecord is now updated via separate UpdateCommand (not in transaction)
      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      const errorRecordUpdate = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "ERROR#01256" &&
          call.args[0].input.ExpressionAttributeValues?.[":entityType"] ===
            "Error",
      );

      // Should use details from the first occurrence
      expect(
        errorRecordUpdate?.args[0].input.ExpressionAttributeValues?.[
          ":errorDetails"
        ],
      ).toBe(JSON.stringify({ attempt: 1, first: true }));
    });
  });

  describe("multiple executions with shared errors", () => {
    it("should correctly increment ErrorRecord totalCount for shared errors across executions", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      // First execution with errors "01256" and "23456"
      const detail1 = createWorkflowDetail({
        executionId: "exec-shared-001",
        errors: [createError("01256"), createError("23456")],
      });

      const result1 = await saveErrorRecords(detail1);

      expect(result1.uniqueErrorCount).toBe(2);
      expect(result1.totalOccurrences).toBe(2);

      // Second execution with error "01256" (shared) and "34567" (new)
      const detail2 = createWorkflowDetail({
        executionId: "exec-shared-002",
        errors: [createError("01256"), createError("34567")],
      });

      const result2 = await saveErrorRecords(detail2);

      expect(result2.uniqueErrorCount).toBe(2);
      expect(result2.totalOccurrences).toBe(2);

      // Verify both executions sent TransactWriteCommands
      expect(ddbMock).toHaveReceivedCommandTimes(TransactWriteCommand, 2);

      // ErrorRecords are now updated via separate UpdateCommand calls (not in transaction)
      // Verify that ErrorRecord for "01256" was updated in both calls with atomic increment
      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      const errorRecordUpdates01256 = updateCalls.filter(
        (call) =>
          call.args[0].input.Key?.PK === "ERROR#01256" &&
          call.args[0].input.ExpressionAttributeValues?.[":entityType"] ===
            "Error",
      );
      // Should have 2 updates for error "01256" (one from each execution)
      expect(errorRecordUpdates01256).toHaveLength(2);
      expect(
        errorRecordUpdates01256[0].args[0].input.ExpressionAttributeValues?.[
          ":increment"
        ],
      ).toBe(1);
      expect(
        errorRecordUpdates01256[1].args[0].input.ExpressionAttributeValues?.[
          ":increment"
        ],
      ).toBe(1);

      // Verify each execution has its own ExecutionErrorLink in the transaction
      const allCalls = ddbMock.commandCalls(TransactWriteCommand);
      const firstTransactItems = allCalls[0].args[0].input.TransactItems;
      const secondTransactItems = allCalls[1].args[0].input.TransactItems;

      const firstExecLinks = firstTransactItems!.filter(
        (item) => item.Update?.Key?.PK === "EXECUTION#exec-shared-001",
      );
      const secondExecLinks = secondTransactItems!.filter(
        (item) => item.Update?.Key?.PK === "EXECUTION#exec-shared-002",
      );

      // First execution should have: 1 FailedExecutionItem + 2 ExecutionErrorLinks = 3
      expect(firstExecLinks).toHaveLength(3);

      // Second execution should have: 1 FailedExecutionItem + 2 ExecutionErrorLinks = 3
      expect(secondExecLinks).toHaveLength(3);
    });

    it("should maintain separate ExecutionErrorLink for each execution even with shared errors", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      // Two executions with the same error
      await saveErrorRecords(
        createWorkflowDetail({
          executionId: "exec-A",
          errors: [createError("01256"), createError("01256")], // 2 occurrences
        }),
      );

      await saveErrorRecords(
        createWorkflowDetail({
          executionId: "exec-B",
          errors: [createError("01256")], // 1 occurrence
        }),
      );

      const allCalls = ddbMock.commandCalls(TransactWriteCommand);

      // Verify exec-A's ExecutionErrorLink has 2 occurrences
      const execAItems = allCalls[0].args[0].input.TransactItems;
      const execALink = execAItems!.find(
        (item) =>
          item.Update?.Key?.PK === "EXECUTION#exec-A" &&
          item.Update?.Key?.SK === "ERROR#01256",
      )?.Update;
      expect(execALink?.ExpressionAttributeValues?.[":occurrences"]).toBe(2);

      // Verify exec-B's ExecutionErrorLink has 1 occurrence
      const execBItems = allCalls[1].args[0].input.TransactItems;
      const execBLink = execBItems!.find(
        (item) =>
          item.Update?.Key?.PK === "EXECUTION#exec-B" &&
          item.Update?.Key?.SK === "ERROR#01256",
      )?.Update;
      expect(execBLink?.ExpressionAttributeValues?.[":occurrences"]).toBe(1);
    });
  });

  describe("error type extraction", () => {
    /**
     * errorType is the first 2 characters of the error code.
     * It should be set in both ErrorRecord and FailedExecutionItem.
     */

    it("should extract errorType as first 2 characters of error code in ErrorRecord", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-errortype-001",
        errors: [createError("01256")], // errorType should be "01"
      });

      await saveErrorRecords(detail);

      // ErrorRecord is now updated via separate UpdateCommand (not in transaction)
      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      const errorRecordUpdate = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "ERROR#01256" &&
          call.args[0].input.ExpressionAttributeValues?.[":entityType"] ===
            "Error",
      );

      expect(
        errorRecordUpdate?.args[0].input.ExpressionAttributeValues?.[
          ":errorType"
        ],
      ).toBe("01");
    });

    it("should extract errorType for each unique error code in ErrorRecord", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-errortype-multi-001",
        errors: [
          createError("01256"), // errorType "01"
          createError("02789"), // errorType "02"
          createError("01999"), // errorType "01"
        ],
      });

      await saveErrorRecords(detail);

      // ErrorRecords are now updated via separate UpdateCommand (not in transaction)
      const updateCalls = ddbMock.commandCalls(UpdateCommand);

      // Find ErrorRecord update for "01256"
      const errorRecord01256 = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "ERROR#01256" &&
          call.args[0].input.ExpressionAttributeValues?.[":entityType"] ===
            "Error",
      );
      expect(
        errorRecord01256?.args[0].input.ExpressionAttributeValues?.[
          ":errorType"
        ],
      ).toBe("01");

      // Find ErrorRecord update for "02789"
      const errorRecord02789 = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "ERROR#02789" &&
          call.args[0].input.ExpressionAttributeValues?.[":entityType"] ===
            "Error",
      );
      expect(
        errorRecord02789?.args[0].input.ExpressionAttributeValues?.[
          ":errorType"
        ],
      ).toBe("02");

      // Find ErrorRecord update for "01999"
      const errorRecord01999 = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "ERROR#01999" &&
          call.args[0].input.ExpressionAttributeValues?.[":entityType"] ===
            "Error",
      );
      expect(
        errorRecord01999?.args[0].input.ExpressionAttributeValues?.[
          ":errorType"
        ],
      ).toBe("01");
    });

    it("should set errorType in FailedExecutionItem when all errors share same type", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-errortype-002",
        errors: [createError("01256"), createError("01999")], // Both type "01"
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      // Find FailedExecutionItem
      const failedExecutionItem = transactItems![0].Update;

      expect(
        failedExecutionItem?.ExpressionAttributeValues?.[":errorType"],
      ).toBe("01");
    });

    it("should include errorType in GS3SK for ErrorRecord", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-gs3-error-001",
        errors: [createError("01256")],
      });

      await saveErrorRecords(detail);

      // GS3SK is set via a separate UpdateCommand after the ErrorRecord update
      // (because it depends on the atomic totalCount increment)
      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      // Find the sort key update (which has :gs3sk, not :entityType)
      const sortKeyUpdate = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "ERROR#01256" &&
          call.args[0].input.ExpressionAttributeValues?.[":gs3sk"],
      );

      // GS3SK format: COUNT#{errorType}#{status}#{paddedCount}#ERROR#{errorCode}
      // For a new error with count 1: COUNT#01#FAILED#0000000001#ERROR#01256
      expect(
        sortKeyUpdate?.args[0].input.ExpressionAttributeValues?.[":gs3sk"],
      ).toMatch(/^COUNT#01#FAILED#\d{10}#ERROR#01256$/);
    });

    it("should include errorType in GS3SK for FailedExecutionItem with uniform error type", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-gs3-exec-001",
        errors: [createError("01256"), createError("01999")], // Both type "01"
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      // Find FailedExecutionItem
      const failedExecutionItem = transactItems![0].Update;

      // GS3SK format: COUNT#{errorType}#{status}#{paddedCount}#EXECUTION#{executionId}
      // For 2 unique errors: COUNT#01#FAILED#0000000002#EXECUTION#exec-gs3-exec-001
      expect(failedExecutionItem?.ExpressionAttributeValues?.[":gs3sk"]).toBe(
        "COUNT#01#FAILED#0000000002#EXECUTION#exec-gs3-exec-001",
      );
    });

    it("should handle error codes shorter than 2 characters for errorType", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-short-code-001",
        errors: [createError("1")], // Single character error code
      });

      await saveErrorRecords(detail);

      // ErrorRecord is now updated via separate UpdateCommand (not in transaction)
      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      const errorRecordUpdate = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "ERROR#1" &&
          call.args[0].input.ExpressionAttributeValues?.[":entityType"] ===
            "Error",
      );

      // For single character codes, errorType should be the code itself ("1")
      expect(
        errorRecordUpdate?.args[0].input.ExpressionAttributeValues?.[
          ":errorType"
        ],
      ).toBe("1");
    });
  });

  describe("DynamoDB key structure", () => {
    it("should use correct composite key format for ErrorRecord", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-keys-001",
        errors: [createError("12345")],
      });

      await saveErrorRecords(detail);

      // ErrorRecord is now updated via separate UpdateCommand (not in transaction)
      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      const errorRecordUpdate = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK?.startsWith("ERROR#") &&
          call.args[0].input.ExpressionAttributeValues?.[":entityType"] ===
            "Error",
      );

      expect(errorRecordUpdate?.args[0].input.Key).toEqual({
        PK: "ERROR#12345",
        SK: "ERROR#12345",
      });
    });

    it("should use correct composite key format for ExecutionErrorLink", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-keys-002",
        errors: [createError("12345")],
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      const executionErrorLink = transactItems!.find(
        (item) =>
          item.Update?.Key?.PK?.startsWith("EXECUTION#") &&
          item.Update?.Key?.SK?.startsWith("ERROR#"),
      )?.Update;

      expect(executionErrorLink?.Key).toEqual({
        PK: "EXECUTION#exec-keys-002",
        SK: "ERROR#12345",
      });
    });

    it("should use correct composite key format for FailedExecutionItem", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-keys-003",
        errors: [createError("12345")],
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      // FailedExecutionItem is first item with same PK and SK
      const failedExecutionItem = transactItems![0].Update;

      expect(failedExecutionItem?.Key).toEqual({
        PK: "EXECUTION#exec-keys-003",
        SK: "EXECUTION#exec-keys-003",
      });
    });
  });

  describe("GSI key structure", () => {
    it("should set correct GSI keys for ErrorRecord", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-gsi-001",
        errors: [createError("12345")],
      });

      await saveErrorRecords(detail);

      // ErrorRecord is now updated via separate UpdateCommand (not in transaction)
      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      const errorRecordUpdate = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "ERROR#12345" &&
          call.args[0].input.ExpressionAttributeValues?.[":entityType"] ===
            "Error",
      );

      // GS1: TYPE#ERROR -> ERROR#errorCode
      expect(
        errorRecordUpdate?.args[0].input.ExpressionAttributeValues?.[":gs1pk"],
      ).toBe("TYPE#ERROR");
      expect(
        errorRecordUpdate?.args[0].input.ExpressionAttributeValues?.[":gs1sk"],
      ).toBe("ERROR#12345");

      // GS2: TYPE#ERROR (GS2SK is updated separately via updateErrorRecordSortKey)
      expect(
        errorRecordUpdate?.args[0].input.ExpressionAttributeValues?.[":gs2pk"],
      ).toBe("TYPE#ERROR");

      // GS3: METRIC#ERRORCOUNT#ERROR (separate partition from FailedExecutionItem)
      // GS3SK is updated separately via updateErrorRecordSortKey
      expect(
        errorRecordUpdate?.args[0].input.ExpressionAttributeValues?.[":gs3pk"],
      ).toBe("METRIC#ERRORCOUNT#ERROR");

      // GS2SK and GS3SK are set via separate UpdateCommand after the ErrorRecord update
      const sortKeyUpdate = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "ERROR#12345" &&
          call.args[0].input.ExpressionAttributeValues?.[":gs2sk"],
      );
      // GS2SK format: COUNT#{status}#{paddedCount}#ERROR#{errorCode}
      expect(
        sortKeyUpdate?.args[0].input.ExpressionAttributeValues?.[":gs2sk"],
      ).toMatch(/^COUNT#FAILED#\d{10}#ERROR#12345$/);
      // GS3SK format: COUNT#{errorType}#{status}#{paddedCount}#ERROR#{errorCode}
      expect(
        sortKeyUpdate?.args[0].input.ExpressionAttributeValues?.[":gs3sk"],
      ).toMatch(/^COUNT#12#FAILED#\d{10}#ERROR#12345$/);
    });

    it("should set correct GSI keys for ExecutionErrorLink", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-gsi-002",
        errors: [createError("12345")],
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      const executionErrorLink = transactItems!.find(
        (item) =>
          item.Update?.Key?.PK === "EXECUTION#exec-gsi-002" &&
          item.Update?.Key?.SK === "ERROR#12345",
      )?.Update;

      // GS1: ERROR#errorCode -> EXECUTION#executionId (reverse lookup)
      expect(executionErrorLink?.ExpressionAttributeValues?.[":gs1pk"]).toBe(
        "ERROR#12345",
      );
      expect(executionErrorLink?.ExpressionAttributeValues?.[":gs1sk"]).toBe(
        "EXECUTION#exec-gsi-002",
      );
    });

    it("should set correct GSI keys for FailedExecutionItem with padded count and errorType", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-gsi-003",
        errors: [createError("12345"), createError("12890")], // Same type "12"
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      const failedExecutionItem = transactItems![0].Update;

      // GS3: METRIC#ERRORCOUNT -> COUNT#{errorType}#{status}#{paddedCount}#EXECUTION#executionId
      expect(failedExecutionItem?.ExpressionAttributeValues?.[":gs3pk"]).toBe(
        "METRIC#ERRORCOUNT",
      );
      expect(failedExecutionItem?.ExpressionAttributeValues?.[":gs3sk"]).toBe(
        "COUNT#12#FAILED#0000000002#EXECUTION#exec-gsi-003",
      );
    });
  });

  describe("empty errors handling", () => {
    it("should return early result when errors array is empty", async () => {
      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-empty-001",
        errors: [],
      });

      const result = await saveErrorRecords(detail);

      expect(result).toEqual({
        success: true,
        uniqueErrorCount: 0,
        totalOccurrences: 0,
        errorCodes: [],
      });

      expect(ddbMock).not.toHaveReceivedCommand(TransactWriteCommand);
    });
  });

  describe("transaction batching for large error sets", () => {
    it("should batch transactions when more than 100 items", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});
      ddbMock.on(UpdateCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      // Create 60 unique errors
      // This will create: 1 FailedExecutionItem + 60 ErrorRecords + 60 ExecutionErrorLinks = 121 items
      const errors: WorkflowError[] = [];
      for (let i = 0; i < 60; i++) {
        errors.push(createError(String(10000 + i).padStart(5, "0")));
      }

      const detail = createWorkflowDetail({
        executionId: "exec-batch-001",
        errors,
      });

      const result = await saveErrorRecords(detail);

      expect(result.success).toBe(true);
      expect(result.uniqueErrorCount).toBe(60);
      expect(result.totalOccurrences).toBe(60);

      // Should use UpdateCommand for FailedExecutionItem (processed separately)
      expect(ddbMock).toHaveReceivedCommand(UpdateCommand);

      // Should use multiple TransactWriteCommands for batched error items
      expect(ddbMock).toHaveReceivedCommand(TransactWriteCommand);
    });

    it("should use single transaction when under 100 items", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      // Create 33 unique errors
      // Transaction now contains: 1 FailedExecutionItem + 33 ExecutionErrorLinks = 34 items
      // ErrorRecords are updated separately via UpdateCommand
      const errors: WorkflowError[] = [];
      for (let i = 0; i < 33; i++) {
        errors.push(createError(String(10000 + i).padStart(5, "0")));
      }

      const detail = createWorkflowDetail({
        executionId: "exec-single-batch-001",
        errors,
      });

      const result = await saveErrorRecords(detail);

      expect(result.success).toBe(true);

      // Should use single TransactWriteCommand
      expect(ddbMock).toHaveReceivedCommandTimes(TransactWriteCommand, 1);

      // UpdateCommand is called:
      // - 33 times for ErrorRecord updates (individual, not in transaction)
      // - 33 times to refresh GS2SK and GS3SK sort keys
      // Total: 66 calls
      expect(ddbMock).toHaveReceivedCommandTimes(UpdateCommand, 66);
    });
  });

  describe("updateErrorRecordSortKey", () => {
    it("should update GS2SK and GS3SK for an error record with padded count", async () => {
      ddbMock.on(UpdateCommand).resolves({});

      const { updateErrorRecordSortKey } = await import("shared/repository.js");

      await updateErrorRecordSortKey("12345", 42);

      expect(ddbMock).toHaveReceivedCommandWith(UpdateCommand, {
        TableName: TEST_TABLE_NAME,
        Key: {
          PK: "ERROR#12345",
          SK: "ERROR#12345",
        },
        UpdateExpression: "SET GS2SK = :gs2sk, GS3SK = :gs3sk",
        ExpressionAttributeValues: {
          // Both GS2SK and GS3SK include status
          ":gs2sk": "COUNT#FAILED#0000000042#ERROR#12345",
          ":gs3sk": "COUNT#12#FAILED#0000000042#ERROR#12345",
        },
      });
    });

    it("should throw error when table name is not set", async () => {
      delete process.env.WORKFLOW_ERRORS_TABLE_NAME;

      const { updateErrorRecordSortKey } = await import("shared/repository.js");

      await expect(updateErrorRecordSortKey("12345", 42)).rejects.toThrow(
        "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
      );
    });
  });

  describe("default status values", () => {
    it("should set default error status to 'failed' for new records", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-status-001",
        errors: [createError("12345")],
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      // Check FailedExecutionItem
      const failedExecutionItem = transactItems![0].Update;
      expect(
        failedExecutionItem?.ExpressionAttributeValues?.[":defaultStatus"],
      ).toBe("failed");

      // Check ErrorRecord (now via UpdateCommand, not in transaction)
      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      const errorRecordUpdate = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "ERROR#12345" &&
          call.args[0].input.ExpressionAttributeValues?.[":entityType"] ===
            "Error",
      );
      expect(
        errorRecordUpdate?.args[0].input.ExpressionAttributeValues?.[
          ":defaultStatus"
        ],
      ).toBe("failed");

      // Check ExecutionErrorLink (now at index 1 in transaction)
      const executionErrorLink = transactItems!.find(
        (item) =>
          item.Update?.Key?.PK === "EXECUTION#exec-status-001" &&
          item.Update?.Key?.SK === "ERROR#12345",
      )?.Update;
      expect(
        executionErrorLink?.ExpressionAttributeValues?.[":defaultStatus"],
      ).toBe("failed");
    });
  });

  describe("county field handling", () => {
    it("should correctly set county in FailedExecutionItem and ExecutionErrorLink", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-county-001",
        county: "broward",
        errors: [createError("12345")],
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      // Check FailedExecutionItem
      const failedExecutionItem = transactItems![0].Update;
      expect(failedExecutionItem?.ExpressionAttributeValues?.[":county"]).toBe(
        "broward",
      );

      // Check ExecutionErrorLink
      const executionErrorLink = transactItems!.find(
        (item) =>
          item.Update?.Key?.PK === "EXECUTION#exec-county-001" &&
          item.Update?.Key?.SK === "ERROR#12345",
      )?.Update;
      expect(executionErrorLink?.ExpressionAttributeValues?.[":county"]).toBe(
        "broward",
      );
    });
  });

  describe("CloudWatch metrics", () => {
    it("should publish phase metric on each event", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { handler } =
        await import("../../../../workflow-events/lambdas/event-handler/index.js");

      const detail = createWorkflowDetail({
        executionId: "exec-metrics-001",
        county: "palm_beach",
        status: "SUCCEEDED",
        phase: "scrape",
        step: "login",
        errors: [],
      });
      const event = createMockEvent(detail);

      await handler(event);

      expect(cloudWatchMock).toHaveReceivedCommand(PutMetricDataCommand);
    });

    it("should publish metric with correct name based on phase", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { handler } =
        await import("../../../../workflow-events/lambdas/event-handler/index.js");

      const detail = createWorkflowDetail({
        executionId: "exec-metrics-002",
        phase: "transform",
        errors: [],
      });
      const event = createMockEvent(detail);

      await handler(event);

      const calls = cloudWatchMock.commandCalls(PutMetricDataCommand);
      const metricData = calls[0].args[0].input.MetricData;

      expect(metricData).toHaveLength(1);
      expect(metricData![0].MetricName).toBe("transformElephantPhase");
    });

    it("should publish metric with correct dimensions", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { handler } =
        await import("../../../../workflow-events/lambdas/event-handler/index.js");

      const detail = createWorkflowDetail({
        executionId: "exec-metrics-003",
        county: "broward",
        status: "FAILED",
        phase: "upload",
        step: "submit",
        errors: [],
      });
      const event = createMockEvent(detail);

      await handler(event);

      const calls = cloudWatchMock.commandCalls(PutMetricDataCommand);
      const metricData = calls[0].args[0].input.MetricData;
      const dimensions = metricData![0].Dimensions;

      expect(dimensions).toContainEqual({ Name: "County", Value: "broward" });
      expect(dimensions).toContainEqual({ Name: "Status", Value: "FAILED" });
      expect(dimensions).toContainEqual({ Name: "Step", Value: "submit" });
    });

    it("should publish metric with correct namespace", async () => {
      const { handler } =
        await import("../../../../workflow-events/lambdas/event-handler/index.js");

      const detail = createWorkflowDetail({
        executionId: "exec-metrics-004",
        errors: [],
      });
      const event = createMockEvent(detail);

      await handler(event);

      const calls = cloudWatchMock.commandCalls(PutMetricDataCommand);
      const namespace = calls[0].args[0].input.Namespace;

      expect(namespace).toBe("Elephant/Workflow");
    });

    it("should publish metric with unit Count and value 1", async () => {
      const { handler } =
        await import("../../../../workflow-events/lambdas/event-handler/index.js");

      const detail = createWorkflowDetail({
        executionId: "exec-metrics-005",
        errors: [],
      });
      const event = createMockEvent(detail);

      await handler(event);

      const calls = cloudWatchMock.commandCalls(PutMetricDataCommand);
      const metricData = calls[0].args[0].input.MetricData;

      expect(metricData![0].Unit).toBe("Count");
      expect(metricData![0].Value).toBe(1);
    });

    it("should publish metric even when event has errors", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { handler } =
        await import("../../../../workflow-events/lambdas/event-handler/index.js");

      const detail = createWorkflowDetail({
        executionId: "exec-metrics-006",
        phase: "scrape",
        errors: [createError("01256")],
      });
      const event = createMockEvent(detail);

      await handler(event);

      expect(cloudWatchMock).toHaveReceivedCommand(PutMetricDataCommand);

      const calls = cloudWatchMock.commandCalls(PutMetricDataCommand);
      expect(calls[0].args[0].input.MetricData![0].MetricName).toBe(
        "scrapeElephantPhase",
      );
    });

    it("should propagate CloudWatch errors to caller", async () => {
      cloudWatchMock
        .on(PutMetricDataCommand)
        .rejects(new Error("CloudWatch error"));

      const { handler } =
        await import("../../../../workflow-events/lambdas/event-handler/index.js");

      const detail = createWorkflowDetail({
        executionId: "exec-metrics-error-001",
        errors: [],
      });
      const event = createMockEvent(detail);

      await expect(handler(event)).rejects.toThrow("CloudWatch error");
    });
  });

  describe("publishPhaseMetric function", () => {
    it("should publish metric with all required fields", async () => {
      const { publishPhaseMetric } =
        await import("../../../../workflow-events/lambdas/event-handler/cloudwatch.js");

      await publishPhaseMetric("scrape", {
        county: "palm_beach",
        status: "SUCCEEDED",
        step: "login",
      });

      expect(cloudWatchMock).toHaveReceivedCommand(PutMetricDataCommand);

      const calls = cloudWatchMock.commandCalls(PutMetricDataCommand);
      const input = calls[0].args[0].input;

      expect(input.Namespace).toBe("Elephant/Workflow");
      expect(input.MetricData).toHaveLength(1);

      const metric = input.MetricData![0];
      expect(metric.MetricName).toBe("scrapeElephantPhase");
      expect(metric.Unit).toBe("Count");
      expect(metric.Value).toBe(1);
      expect(metric.Timestamp).toBeInstanceOf(Date);
      expect(metric.Dimensions).toContainEqual({
        Name: "County",
        Value: "palm_beach",
      });
      expect(metric.Dimensions).toContainEqual({
        Name: "Status",
        Value: "SUCCEEDED",
      });
      expect(metric.Dimensions).toContainEqual({
        Name: "Step",
        Value: "login",
      });
    });

    it("should construct metric name from phase", async () => {
      const { publishPhaseMetric } =
        await import("../../../../workflow-events/lambdas/event-handler/cloudwatch.js");

      await publishPhaseMetric("transform", {
        county: "broward",
        status: "IN_PROGRESS",
        step: "parse",
      });

      const calls = cloudWatchMock.commandCalls(PutMetricDataCommand);
      const metricName = calls[0].args[0].input.MetricData![0].MetricName;

      expect(metricName).toBe("transformElephantPhase");
    });

    it("should handle different status values", async () => {
      const { publishPhaseMetric } =
        await import("../../../../workflow-events/lambdas/event-handler/cloudwatch.js");

      await publishPhaseMetric("upload", {
        county: "miami_dade",
        status: "PARKED",
        step: "submit",
      });

      const calls = cloudWatchMock.commandCalls(PutMetricDataCommand);
      const dimensions = calls[0].args[0].input.MetricData![0].Dimensions;

      expect(dimensions).toContainEqual({ Name: "Status", Value: "PARKED" });
    });
  });

  describe("markErrorsAsUnrecoverableForExecution", () => {
    it("should update ErrorRecord status and GSI keys when marking execution as unrecoverable", async () => {
      // Mock queryExecutionErrorLinks to return a link
      ddbMock.on(QueryCommand).resolves({
        Items: [
          {
            PK: "EXECUTION#exec-unrecoverable-001",
            SK: "ERROR#12345",
            errorCode: "12345",
            executionId: "exec-unrecoverable-001",
            entityType: "ExecutionError",
            occurrences: 1,
          },
        ],
      });

      // Mock getFailedExecutionItem
      ddbMock.on(GetCommand).callsFake((input) => {
        const pk = input.Key?.PK as string;
        if (pk === "EXECUTION#exec-unrecoverable-001") {
          return {
            Item: {
              PK: "EXECUTION#exec-unrecoverable-001",
              SK: "EXECUTION#exec-unrecoverable-001",
              executionId: "exec-unrecoverable-001",
              entityType: "FailedExecution",
              openErrorCount: 1,
              errorType: "12",
            },
          };
        }
        if (pk === "ERROR#12345") {
          return {
            Item: {
              PK: "ERROR#12345",
              SK: "ERROR#12345",
              errorCode: "12345",
              entityType: "Error",
              totalCount: 5,
              errorType: "12",
            },
          };
        }
        return { Item: undefined };
      });

      ddbMock.on(UpdateCommand).resolves({});

      const { markErrorsAsUnrecoverableForExecution } =
        await import("shared/repository.js");

      const result = await markErrorsAsUnrecoverableForExecution(
        "exec-unrecoverable-001",
      );

      expect(result.updatedCount).toBeGreaterThan(0);
      expect(result.affectedExecutionIds).toContain("exec-unrecoverable-001");
      expect(result.updatedErrorCodes).toContain("12345");

      // Verify ErrorRecord was updated with maybeUnrecoverable status
      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      const errorRecordUpdate = updateCalls.find(
        (call) => call.args[0].input.Key?.PK === "ERROR#12345",
      );

      expect(errorRecordUpdate).toBeDefined();
      expect(
        errorRecordUpdate?.args[0].input.ExpressionAttributeValues?.[":status"],
      ).toBe("maybeUnrecoverable");
      // GS2SK format: COUNT#MAYBEUNRECOVERABLE#{paddedCount}#ERROR#{errorCode}
      expect(
        errorRecordUpdate?.args[0].input.ExpressionAttributeValues?.[":gs2sk"],
      ).toBe("COUNT#MAYBEUNRECOVERABLE#0000000005#ERROR#12345");
      // GS3SK format: COUNT#{errorType}#MAYBEUNRECOVERABLE#{paddedCount}#ERROR#{errorCode}
      expect(
        errorRecordUpdate?.args[0].input.ExpressionAttributeValues?.[":gs3sk"],
      ).toBe("COUNT#12#MAYBEUNRECOVERABLE#0000000005#ERROR#12345");
    });

    it("should return empty result when execution has no error links", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { markErrorsAsUnrecoverableForExecution } =
        await import("shared/repository.js");

      const result =
        await markErrorsAsUnrecoverableForExecution("exec-no-errors");

      expect(result.updatedCount).toBe(0);
      expect(result.affectedExecutionIds).toHaveLength(0);
      expect(result.updatedErrorCodes).toHaveLength(0);
    });
  });

  describe("markErrorAsUnrecoverableFromAllExecutions", () => {
    it("should update ErrorRecord status and GSI keys when marking error code as unrecoverable", async () => {
      // Mock queryErrorLinksForErrorCode to return links
      ddbMock.on(QueryCommand).resolves({
        Items: [
          {
            PK: "EXECUTION#exec-001",
            SK: "ERROR#99999",
            errorCode: "99999",
            executionId: "exec-001",
            entityType: "ExecutionError",
            occurrences: 1,
            GS1PK: "ERROR#99999",
            GS1SK: "EXECUTION#exec-001",
          },
        ],
      });

      // Mock GetCommand for both execution and error record
      ddbMock.on(GetCommand).callsFake((input) => {
        const pk = input.Key?.PK as string;
        if (pk === "EXECUTION#exec-001") {
          return {
            Item: {
              PK: "EXECUTION#exec-001",
              SK: "EXECUTION#exec-001",
              executionId: "exec-001",
              entityType: "FailedExecution",
              openErrorCount: 1,
              errorType: "99",
            },
          };
        }
        if (pk === "ERROR#99999") {
          return {
            Item: {
              PK: "ERROR#99999",
              SK: "ERROR#99999",
              errorCode: "99999",
              entityType: "Error",
              totalCount: 3,
              errorType: "99",
            },
          };
        }
        return { Item: undefined };
      });

      ddbMock.on(UpdateCommand).resolves({});

      const { markErrorAsUnrecoverableFromAllExecutions } =
        await import("shared/repository.js");

      const result = await markErrorAsUnrecoverableFromAllExecutions("99999");

      expect(result.updatedCount).toBeGreaterThan(0);
      expect(result.affectedExecutionIds).toContain("exec-001");
      expect(result.updatedErrorCodes).toContain("99999");

      // Verify ErrorRecord was updated with maybeUnrecoverable status
      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      const errorRecordUpdate = updateCalls.find(
        (call) => call.args[0].input.Key?.PK === "ERROR#99999",
      );

      expect(errorRecordUpdate).toBeDefined();
      expect(
        errorRecordUpdate?.args[0].input.ExpressionAttributeValues?.[":status"],
      ).toBe("maybeUnrecoverable");
      // GS2SK format: COUNT#MAYBEUNRECOVERABLE#{paddedCount}#ERROR#{errorCode}
      expect(
        errorRecordUpdate?.args[0].input.ExpressionAttributeValues?.[":gs2sk"],
      ).toBe("COUNT#MAYBEUNRECOVERABLE#0000000003#ERROR#99999");
      // GS3SK format: COUNT#{errorType}#MAYBEUNRECOVERABLE#{paddedCount}#ERROR#{errorCode}
      expect(
        errorRecordUpdate?.args[0].input.ExpressionAttributeValues?.[":gs3sk"],
      ).toBe("COUNT#99#MAYBEUNRECOVERABLE#0000000003#ERROR#99999");
    });

    it("should return empty result when error code has no links", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { markErrorAsUnrecoverableFromAllExecutions } =
        await import("shared/repository.js");

      const result = await markErrorAsUnrecoverableFromAllExecutions("00000");

      expect(result.updatedCount).toBe(0);
      expect(result.affectedExecutionIds).toHaveLength(0);
      expect(result.updatedErrorCodes).toHaveLength(0);
    });
  });
});
