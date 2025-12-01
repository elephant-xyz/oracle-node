import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import {
  DynamoDBDocumentClient,
  TransactWriteCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import type { EventBridgeEvent } from "aws-lambda";
import type {
  WorkflowEventDetail,
  WorkflowError,
} from "../../../../workflow-events/lambdas/event-handler/types.js";

/**
 * Mock the DynamoDB Document Client for all tests.
 */
const ddbMock = mockClient(DynamoDBDocumentClient);

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

  beforeEach(() => {
    vi.resetModules();
    ddbMock.reset();
    process.env = {
      ...originalEnv,
      WORKFLOW_ERRORS_TABLE_NAME: TEST_TABLE_NAME,
    };
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "info").mockImplementation(() => {});
    vi.spyOn(console, "debug").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  describe("handler integration tests", () => {
    it("should process event with errors and save to DynamoDB", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow-events/lambdas/event-handler/index.js"
      );

      const detail = createWorkflowDetail({
        executionId: "exec-integration-001",
        errors: [createError("01256", { reason: "timeout" })],
      });
      const event = createMockEvent(detail);

      await handler(event);

      expect(ddbMock).toHaveReceivedCommand(TransactWriteCommand);
    });

    it("should skip DynamoDB save when event has no errors", async () => {
      const { handler } = await import(
        "../../../../workflow-events/lambdas/event-handler/index.js"
      );

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

      const { handler } = await import(
        "../../../../workflow-events/lambdas/event-handler/index.js"
      );

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

      const { handler } = await import(
        "../../../../workflow-events/lambdas/event-handler/index.js"
      );

      const detail = createWorkflowDetail({
        errors: [createError("01256")],
      });
      const event = createMockEvent(detail);

      await expect(handler(event)).rejects.toThrow(
        "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
      );
    });
  });

  describe("single error execution", () => {
    it("should create FailedExecutionItem, ErrorRecord, and ExecutionErrorLink for single error", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

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

      // Should have exactly one TransactWriteCommand with 3 items:
      // 1 FailedExecutionItem + 1 ErrorRecord + 1 ExecutionErrorLink
      expect(ddbMock).toHaveReceivedCommandTimes(TransactWriteCommand, 1);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      expect(transactItems).toHaveLength(3);

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

      // Verify ErrorRecord
      const errorRecord = transactItems![1].Update;
      expect(errorRecord?.Key).toEqual({
        PK: "ERROR#01256",
        SK: "ERROR#01256",
      });
      expect(errorRecord?.ExpressionAttributeValues?.[":entityType"]).toBe(
        "Error",
      );
      expect(errorRecord?.ExpressionAttributeValues?.[":errorCode"]).toBe(
        "01256",
      );
      expect(errorRecord?.ExpressionAttributeValues?.[":errorDetails"]).toBe(
        JSON.stringify({ reason: "login timeout" }),
      );

      // Verify ExecutionErrorLink
      const executionErrorLink = transactItems![2].Update;
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
    });

    it("should include taskToken in FailedExecutionItem when provided", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

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

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

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

      // Should have: 1 FailedExecutionItem + 3 ErrorRecords + 3 ExecutionErrorLinks = 7 items
      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      expect(transactItems).toHaveLength(7);

      // Verify FailedExecutionItem counts
      const failedExecutionItem = transactItems![0].Update;
      expect(
        failedExecutionItem?.ExpressionAttributeValues?.[":uniqueErrorCount"],
      ).toBe(3);
      expect(
        failedExecutionItem?.ExpressionAttributeValues?.[":totalOccurrences"],
      ).toBe(3);
    });
  });

  describe("repeated errors execution (unique count aggregation)", () => {
    it("should aggregate repeated errors and count unique errors correctly", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

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

      // Should have: 1 FailedExecutionItem + 2 ErrorRecords + 2 ExecutionErrorLinks = 5 items
      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      expect(transactItems).toHaveLength(5);

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

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

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

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

      const detail = createWorkflowDetail({
        executionId: "exec-details-001",
        errors: [
          createError("01256", { attempt: 1, first: true }),
          createError("01256", { attempt: 2, second: true }),
        ],
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      // Find ErrorRecord for "01256"
      const errorRecord = transactItems!.find(
        (item) =>
          item.Update?.Key?.PK === "ERROR#01256" &&
          item.Update?.Key?.SK === "ERROR#01256",
      )?.Update;

      // Should use details from the first occurrence
      expect(errorRecord?.ExpressionAttributeValues?.[":errorDetails"]).toBe(
        JSON.stringify({ attempt: 1, first: true }),
      );
    });
  });

  describe("multiple executions with shared errors", () => {
    it("should correctly increment ErrorRecord totalCount for shared errors across executions", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

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

      // Verify that ErrorRecord for "01256" was updated in both transactions
      // with atomic increment (:increment = 1)
      const allCalls = ddbMock.commandCalls(TransactWriteCommand);

      // First execution's transaction
      const firstTransactItems = allCalls[0].args[0].input.TransactItems;
      const firstErrorRecord01256 = firstTransactItems!.find(
        (item) => item.Update?.Key?.PK === "ERROR#01256",
      )?.Update;
      expect(
        firstErrorRecord01256?.ExpressionAttributeValues?.[":increment"],
      ).toBe(1);

      // Second execution's transaction
      const secondTransactItems = allCalls[1].args[0].input.TransactItems;
      const secondErrorRecord01256 = secondTransactItems!.find(
        (item) => item.Update?.Key?.PK === "ERROR#01256",
      )?.Update;
      expect(
        secondErrorRecord01256?.ExpressionAttributeValues?.[":increment"],
      ).toBe(1);

      // Verify each execution has its own ExecutionErrorLink
      const firstExecLinks = firstTransactItems!.filter(
        (item) => item.Update?.Key?.PK === "EXECUTION#exec-shared-001",
      );
      const secondExecLinks = secondTransactItems!.filter(
        (item) => item.Update?.Key?.PK === "EXECUTION#exec-shared-002",
      );

      // First execution should have: 1 FailedExecutionItem + 2 ExecutionErrorLinks
      expect(firstExecLinks).toHaveLength(3);

      // Second execution should have: 1 FailedExecutionItem + 2 ExecutionErrorLinks
      expect(secondExecLinks).toHaveLength(3);
    });

    it("should maintain separate ExecutionErrorLink for each execution even with shared errors", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

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

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

      const detail = createWorkflowDetail({
        executionId: "exec-errortype-001",
        errors: [createError("01256")], // errorType should be "01"
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      // Find ErrorRecord
      const errorRecord = transactItems!.find(
        (item) => item.Update?.Key?.PK === "ERROR#01256",
      )?.Update;

      expect(errorRecord?.ExpressionAttributeValues?.[":errorType"]).toBe("01");
    });

    it("should extract errorType for each unique error code in ErrorRecord", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

      const detail = createWorkflowDetail({
        executionId: "exec-errortype-multi-001",
        errors: [
          createError("01256"), // errorType "01"
          createError("02789"), // errorType "02"
          createError("01999"), // errorType "01"
        ],
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      // Find ErrorRecord for "01256"
      const errorRecord01256 = transactItems!.find(
        (item) => item.Update?.Key?.PK === "ERROR#01256",
      )?.Update;
      expect(errorRecord01256?.ExpressionAttributeValues?.[":errorType"]).toBe(
        "01",
      );

      // Find ErrorRecord for "02789"
      const errorRecord02789 = transactItems!.find(
        (item) => item.Update?.Key?.PK === "ERROR#02789",
      )?.Update;
      expect(errorRecord02789?.ExpressionAttributeValues?.[":errorType"]).toBe(
        "02",
      );

      // Find ErrorRecord for "01999"
      const errorRecord01999 = transactItems!.find(
        (item) => item.Update?.Key?.PK === "ERROR#01999",
      )?.Update;
      expect(errorRecord01999?.ExpressionAttributeValues?.[":errorType"]).toBe(
        "01",
      );
    });

    it("should set errorType in FailedExecutionItem when all errors share same type", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

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

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

      const detail = createWorkflowDetail({
        executionId: "exec-gs3-error-001",
        errors: [createError("01256")],
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      // Find ErrorRecord
      const errorRecord = transactItems!.find(
        (item) => item.Update?.Key?.PK === "ERROR#01256",
      )?.Update;

      // GS3SK format: COUNT#{errorType}#{paddedCount}#ERROR#{errorCode}
      // For a new error with count 1: COUNT#01#0000000001#ERROR#01256
      expect(errorRecord?.ExpressionAttributeValues?.[":gs3sk"]).toMatch(
        /^COUNT#01#\d{10}#ERROR#01256$/,
      );
    });

    it("should include errorType in GS3SK for FailedExecutionItem with uniform error type", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

      const detail = createWorkflowDetail({
        executionId: "exec-gs3-exec-001",
        errors: [createError("01256"), createError("01999")], // Both type "01"
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      // Find FailedExecutionItem
      const failedExecutionItem = transactItems![0].Update;

      // GS3SK format: COUNT#{errorType}#{paddedCount}#EXECUTION#{executionId}
      // For 2 unique errors: COUNT#01#0000000002#EXECUTION#exec-gs3-exec-001
      expect(failedExecutionItem?.ExpressionAttributeValues?.[":gs3sk"]).toBe(
        "COUNT#01#0000000002#EXECUTION#exec-gs3-exec-001",
      );
    });

    it("should handle error codes shorter than 2 characters for errorType", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

      const detail = createWorkflowDetail({
        executionId: "exec-short-code-001",
        errors: [createError("1")], // Single character error code
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      // Find ErrorRecord
      const errorRecord = transactItems!.find(
        (item) => item.Update?.Key?.PK === "ERROR#1",
      )?.Update;

      // For single character codes, errorType should be the code itself ("1")
      expect(errorRecord?.ExpressionAttributeValues?.[":errorType"]).toBe("1");
    });
  });

  describe("DynamoDB key structure", () => {
    it("should use correct composite key format for ErrorRecord", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

      const detail = createWorkflowDetail({
        executionId: "exec-keys-001",
        errors: [createError("12345")],
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      const errorRecord = transactItems!.find(
        (item) =>
          item.Update?.Key?.PK?.startsWith("ERROR#") &&
          item.Update?.Key?.SK?.startsWith("ERROR#"),
      )?.Update;

      expect(errorRecord?.Key).toEqual({
        PK: "ERROR#12345",
        SK: "ERROR#12345",
      });
    });

    it("should use correct composite key format for ExecutionErrorLink", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

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

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

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

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

      const detail = createWorkflowDetail({
        executionId: "exec-gsi-001",
        errors: [createError("12345")],
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      const errorRecord = transactItems!.find(
        (item) => item.Update?.Key?.PK === "ERROR#12345",
      )?.Update;

      // GS1: TYPE#ERROR -> ERROR#errorCode
      expect(errorRecord?.ExpressionAttributeValues?.[":gs1pk"]).toBe(
        "TYPE#ERROR",
      );
      expect(errorRecord?.ExpressionAttributeValues?.[":gs1sk"]).toBe(
        "ERROR#12345",
      );

      // GS2: TYPE#ERROR (GS2SK is updated separately via updateErrorRecordSortKey)
      expect(errorRecord?.ExpressionAttributeValues?.[":gs2pk"]).toBe(
        "TYPE#ERROR",
      );

      // GS3: METRIC#ERRORCOUNT -> COUNT#{errorType}#{paddedCount}#ERROR#{errorCode}
      expect(errorRecord?.ExpressionAttributeValues?.[":gs3pk"]).toBe(
        "METRIC#ERRORCOUNT",
      );
      expect(errorRecord?.ExpressionAttributeValues?.[":gs3sk"]).toMatch(
        /^COUNT#12#\d{10}#ERROR#12345$/,
      );
    });

    it("should set correct GSI keys for ExecutionErrorLink", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

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

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

      const detail = createWorkflowDetail({
        executionId: "exec-gsi-003",
        errors: [createError("12345"), createError("12890")], // Same type "12"
      });

      await saveErrorRecords(detail);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      const failedExecutionItem = transactItems![0].Update;

      // GS3: METRIC#ERRORCOUNT -> COUNT#{errorType}#{paddedCount}#EXECUTION#executionId
      expect(failedExecutionItem?.ExpressionAttributeValues?.[":gs3pk"]).toBe(
        "METRIC#ERRORCOUNT",
      );
      expect(failedExecutionItem?.ExpressionAttributeValues?.[":gs3sk"]).toBe(
        "COUNT#12#0000000002#EXECUTION#exec-gsi-003",
      );
    });
  });

  describe("empty errors handling", () => {
    it("should return early result when errors array is empty", async () => {
      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

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

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

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

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

      // Create 33 unique errors
      // This will create: 1 FailedExecutionItem + 33 ErrorRecords + 33 ExecutionErrorLinks = 67 items
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

      // Should NOT use UpdateCommand (no separate processing needed)
      expect(ddbMock).not.toHaveReceivedCommand(UpdateCommand);
    });
  });

  describe("updateErrorRecordSortKey", () => {
    it("should update GS2SK for an error record with padded count", async () => {
      ddbMock.on(UpdateCommand).resolves({});

      const { updateErrorRecordSortKey } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

      await updateErrorRecordSortKey("12345", 42);

      expect(ddbMock).toHaveReceivedCommandWith(UpdateCommand, {
        TableName: TEST_TABLE_NAME,
        Key: {
          PK: "ERROR#12345",
          SK: "ERROR#12345",
        },
        UpdateExpression: "SET GS2SK = :gs2sk",
        ExpressionAttributeValues: {
          ":gs2sk": "COUNT#0000000042#ERROR#12345",
        },
      });
    });

    it("should throw error when table name is not set", async () => {
      delete process.env.WORKFLOW_ERRORS_TABLE_NAME;

      const { updateErrorRecordSortKey } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

      await expect(updateErrorRecordSortKey("12345", 42)).rejects.toThrow(
        "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
      );
    });
  });

  describe("default status values", () => {
    it("should set default error status to 'failed' for new records", async () => {
      ddbMock.on(TransactWriteCommand).resolves({});

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

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

      // Check ErrorRecord
      const errorRecord = transactItems!.find(
        (item) => item.Update?.Key?.PK === "ERROR#12345",
      )?.Update;
      expect(errorRecord?.ExpressionAttributeValues?.[":defaultStatus"]).toBe(
        "failed",
      );

      // Check ExecutionErrorLink
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

      const { saveErrorRecords } = await import(
        "../../../../workflow-events/lambdas/event-handler/dynamodb.js"
      );

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
});
