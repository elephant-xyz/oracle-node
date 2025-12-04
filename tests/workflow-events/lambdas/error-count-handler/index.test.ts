import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import {
  DynamoDBDocumentClient,
  UpdateCommand,
  BatchWriteCommand,
} from "@aws-sdk/lib-dynamodb";
import { SFNClient, SendTaskSuccessCommand } from "@aws-sdk/client-sfn";
import type { DynamoDBStreamEvent, DynamoDBRecord } from "aws-lambda";
import { marshall } from "@aws-sdk/util-dynamodb";
import type {
  ExecutionErrorLink,
  FailedExecutionItem,
  ErrorRecord,
} from "shared/types.js";

/**
 * Mock the DynamoDB Document Client for all tests.
 */
const ddbMock = mockClient(DynamoDBDocumentClient);

/**
 * Mock the Step Functions client for task success callbacks.
 */
const sfnMock = mockClient(SFNClient);

/**
 * Test table name used in all tests.
 */
const TEST_TABLE_NAME = "test-workflow-errors-table";

/**
 * Creates a mock ExecutionErrorLink item for DynamoDB stream events.
 *
 * @param executionId - The execution ID
 * @param errorCode - The error code
 * @param occurrences - Number of occurrences (default 1)
 * @returns A mock ExecutionErrorLink item
 */
const createExecutionErrorLink = (
  executionId: string,
  errorCode: string,
  occurrences: number = 1,
): Partial<ExecutionErrorLink> => ({
  PK: `EXECUTION#${executionId}`,
  SK: `ERROR#${errorCode}`,
  entityType: "ExecutionError",
  errorCode,
  executionId,
  county: "test_county",
  status: "failed",
  occurrences,
  errorDetails: "{}",
  createdAt: new Date().toISOString(),
  updatedAt: new Date().toISOString(),
  GS1PK: `ERROR#${errorCode}`,
  GS1SK: `EXECUTION#${executionId}`,
});

/**
 * Creates a mock DynamoDB stream record for a REMOVE event.
 *
 * @param item - The item that was removed
 * @param eventId - Optional event ID
 * @returns A mock DynamoDB stream record
 */
const createRemoveRecord = (
  item: Partial<ExecutionErrorLink>,
  eventId: string = `event-${Date.now()}`,
): DynamoDBRecord => ({
  eventID: eventId,
  eventName: "REMOVE",
  eventVersion: "1.1",
  eventSource: "aws:dynamodb",
  awsRegion: "us-east-1",
  dynamodb: {
    Keys: marshall({ PK: item.PK, SK: item.SK }),
    OldImage: marshall(item),
    SequenceNumber: "123456789",
    SizeBytes: 100,
    StreamViewType: "OLD_IMAGE",
  },
});

/**
 * Creates a mock DynamoDB stream event with the given records.
 *
 * @param records - Array of DynamoDB stream records
 * @returns A mock DynamoDB stream event
 */
const createStreamEvent = (records: DynamoDBRecord[]): DynamoDBStreamEvent => ({
  Records: records,
});

/**
 * Creates a mock FailedExecutionItem for update responses.
 *
 * @param executionId - The execution ID
 * @param openErrorCount - The open error count after update
 * @param taskToken - Optional task token
 * @param errorType - Error type (default "01")
 * @returns A mock FailedExecutionItem
 */
const createFailedExecutionItem = (
  executionId: string,
  openErrorCount: number,
  taskToken?: string,
  errorType: string = "01",
): FailedExecutionItem => ({
  PK: `EXECUTION#${executionId}`,
  SK: `EXECUTION#${executionId}`,
  executionId,
  entityType: "FailedExecution",
  status: "failed",
  errorType,
  county: "test_county",
  totalOccurrences: 5,
  openErrorCount,
  uniqueErrorCount: 5,
  taskToken,
  createdAt: new Date().toISOString(),
  updatedAt: new Date().toISOString(),
  GS1PK: "METRIC#ERRORCOUNT",
  GS1SK: `COUNT#FAILED#0000000005#EXECUTION#${executionId}`,
  GS3PK: "METRIC#ERRORCOUNT",
  GS3SK: `COUNT#${errorType}#FAILED#0000000005#EXECUTION#${executionId}`,
});

/**
 * Creates a mock ErrorRecord for update responses.
 *
 * @param errorCode - The error code
 * @param totalCount - The total count after update
 * @param errorType - Error type (first 2 chars of errorCode)
 * @returns A mock ErrorRecord
 */
const createErrorRecord = (
  errorCode: string,
  totalCount: number,
  errorType?: string,
): ErrorRecord => {
  const type = errorType ?? errorCode.substring(0, 2);
  return {
    PK: `ERROR#${errorCode}`,
    SK: `ERROR#${errorCode}`,
    errorCode,
    errorType: type,
    entityType: "Error",
    errorDetails: "{}",
    errorStatus: "failed",
    totalCount,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    latestExecutionId: "exec-001",
    GS1PK: "TYPE#ERROR",
    GS1SK: `ERROR#${errorCode}`,
    GS2PK: "TYPE#ERROR",
    GS2SK: `COUNT#FAILED#0000000005#ERROR#${errorCode}`,
    GS3PK: "METRIC#ERRORCOUNT",
    GS3SK: `COUNT#${type}#0000000005#ERROR#${errorCode}`,
  };
};

describe("error-count-handler", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.resetModules();
    ddbMock.reset();
    sfnMock.reset();
    process.env = {
      ...originalEnv,
      WORKFLOW_ERRORS_TABLE_NAME: TEST_TABLE_NAME,
    };
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "info").mockImplementation(() => {});
    vi.spyOn(console, "debug").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  describe("handler integration tests", () => {
    it("should process REMOVE events for ExecutionErrorLink items", async () => {
      // Mock decrement to return remaining errors
      ddbMock.on(UpdateCommand).resolves({
        Attributes: createFailedExecutionItem("exec-001", 2),
      });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const item = createExecutionErrorLink("exec-001", "01256");
      const record = createRemoveRecord(item);
      const event = createStreamEvent([record]);

      await handler(event);

      // Should have called UpdateCommand for decrement
      expect(ddbMock).toHaveReceivedCommand(UpdateCommand);
    });

    it("should skip non-REMOVE events", async () => {
      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const record: DynamoDBRecord = {
        eventID: "event-001",
        eventName: "INSERT",
        eventVersion: "1.1",
        eventSource: "aws:dynamodb",
        awsRegion: "us-east-1",
        dynamodb: {
          NewImage: marshall(createExecutionErrorLink("exec-001", "01256")),
        },
      };
      const event = createStreamEvent([record]);

      await handler(event);

      // Should not have called any DynamoDB commands
      expect(ddbMock).not.toHaveReceivedCommand(UpdateCommand);
    });

    it("should skip records without ExecutionError entity type", async () => {
      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const item = {
        PK: "ERROR#01256",
        SK: "ERROR#01256",
        entityType: "Error",
        errorCode: "01256",
      };
      const record: DynamoDBRecord = {
        eventID: "event-001",
        eventName: "REMOVE",
        eventVersion: "1.1",
        eventSource: "aws:dynamodb",
        awsRegion: "us-east-1",
        dynamodb: {
          OldImage: marshall(item),
        },
      };
      const event = createStreamEvent([record]);

      await handler(event);

      // Should not have called any DynamoDB commands
      expect(ddbMock).not.toHaveReceivedCommand(UpdateCommand);
    });

    it("should aggregate multiple removals for the same execution", async () => {
      // Mock decrement to return remaining errors
      ddbMock.on(UpdateCommand).resolves({
        Attributes: createFailedExecutionItem("exec-001", 1),
      });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      // Create 3 removal events for the same execution
      const records = [
        createRemoveRecord(
          createExecutionErrorLink("exec-001", "01256"),
          "event-1",
        ),
        createRemoveRecord(
          createExecutionErrorLink("exec-001", "01257"),
          "event-2",
        ),
        createRemoveRecord(
          createExecutionErrorLink("exec-001", "01258"),
          "event-3",
        ),
      ];
      const event = createStreamEvent(records);

      await handler(event);

      // Should have called UpdateCommand once with decrementBy: 3
      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      const decrementCall = updateCalls.find(
        (call) =>
          call.args[0].input.ExpressionAttributeValues?.[":amount"] === 3,
      );
      expect(decrementCall).toBeDefined();
    });

    it("should process multiple executions in parallel", async () => {
      // Mock decrements with different results
      ddbMock.on(UpdateCommand).callsFake((input) => {
        const pk = input.Key?.PK as string;
        if (pk === "EXECUTION#exec-001") {
          return { Attributes: createFailedExecutionItem("exec-001", 2) };
        }
        if (pk === "EXECUTION#exec-002") {
          return { Attributes: createFailedExecutionItem("exec-002", 0) };
        }
        return { Attributes: createFailedExecutionItem("unknown", 1) };
      });

      // Mock batch delete
      ddbMock.on(BatchWriteCommand).resolves({});

      // Mock task success
      sfnMock.on(SendTaskSuccessCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const records = [
        createRemoveRecord(
          createExecutionErrorLink("exec-001", "01256"),
          "event-1",
        ),
        createRemoveRecord(
          createExecutionErrorLink("exec-002", "01257"),
          "event-2",
        ),
      ];
      const event = createStreamEvent(records);

      await handler(event);

      // Should have called UpdateCommand for both executions
      expect(ddbMock.commandCalls(UpdateCommand).length).toBeGreaterThanOrEqual(
        2,
      );
    });
  });

  describe("task success callbacks", () => {
    it("should send task success when execution reaches zero errors", async () => {
      // Mock decrement to return zero errors with task token
      ddbMock.on(UpdateCommand).resolves({
        Attributes: createFailedExecutionItem(
          "exec-001",
          0,
          "task-token-12345",
        ),
      });

      // Mock batch delete
      ddbMock.on(BatchWriteCommand).resolves({});

      // Mock task success
      sfnMock.on(SendTaskSuccessCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const item = createExecutionErrorLink("exec-001", "01256");
      const record = createRemoveRecord(item);
      const event = createStreamEvent([record]);

      await handler(event);

      // Should have called SendTaskSuccessCommand
      expect(sfnMock).toHaveReceivedCommandWith(SendTaskSuccessCommand, {
        taskToken: "task-token-12345",
        output: JSON.stringify({}),
      });
    });

    it("should not send task success when no task token is present", async () => {
      // Mock decrement to return zero errors without task token
      ddbMock.on(UpdateCommand).resolves({
        Attributes: createFailedExecutionItem("exec-001", 0, undefined),
      });

      // Mock batch delete
      ddbMock.on(BatchWriteCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const item = createExecutionErrorLink("exec-001", "01256");
      const record = createRemoveRecord(item);
      const event = createStreamEvent([record]);

      await handler(event);

      // Should not have called SendTaskSuccessCommand
      expect(sfnMock).not.toHaveReceivedCommand(SendTaskSuccessCommand);
    });

    it("should continue processing when task success fails", async () => {
      // Mock decrement to return zero errors with task token
      ddbMock.on(UpdateCommand).resolves({
        Attributes: createFailedExecutionItem(
          "exec-001",
          0,
          "task-token-12345",
        ),
      });

      // Mock batch delete
      ddbMock.on(BatchWriteCommand).resolves({});

      // Mock task success to fail
      sfnMock
        .on(SendTaskSuccessCommand)
        .rejects(new Error("Task token expired"));

      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const item = createExecutionErrorLink("exec-001", "01256");
      const record = createRemoveRecord(item);
      const event = createStreamEvent([record]);

      // Should not throw
      await expect(handler(event)).resolves.not.toThrow();

      // Should still have called BatchWriteCommand to delete
      expect(ddbMock).toHaveReceivedCommand(BatchWriteCommand);
    });
  });

  describe("GSI key updates", () => {
    it("should update GSI keys for executions with remaining errors", async () => {
      // Mock decrement to return remaining errors for both execution and error record
      ddbMock.on(UpdateCommand).callsFake((input) => {
        const pk = input.Key?.PK as string;
        if (pk.startsWith("EXECUTION#")) {
          return {
            Attributes: createFailedExecutionItem(
              "exec-001",
              3,
              undefined,
              "01",
            ),
          };
        }
        if (pk.startsWith("ERROR#")) {
          return { Attributes: createErrorRecord("01256", 5, "01") };
        }
        return {};
      });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const item = createExecutionErrorLink("exec-001", "01256");
      const record = createRemoveRecord(item);
      const event = createStreamEvent([record]);

      await handler(event);

      // Find the execution GSI update call (contains GS1SK for EXECUTION)
      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      const executionGsiUpdateCall = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "EXECUTION#exec-001" &&
          call.args[0].input.UpdateExpression?.includes("GS1SK"),
      );
      expect(executionGsiUpdateCall).toBeDefined();
      expect(
        executionGsiUpdateCall?.args[0].input.ExpressionAttributeValues?.[
          ":gs1sk"
        ],
      ).toMatch(/^COUNT#FAILED#\d{10}#EXECUTION#exec-001$/);
      expect(
        executionGsiUpdateCall?.args[0].input.ExpressionAttributeValues?.[
          ":gs3sk"
        ],
      ).toMatch(/^COUNT#01#FAILED#\d{10}#EXECUTION#exec-001$/);
    });

    it("should not update GSI keys for executions that reached zero", async () => {
      // Mock decrement to return zero errors for both execution and error record
      ddbMock.on(UpdateCommand).callsFake((input) => {
        const pk = input.Key?.PK as string;
        if (pk.startsWith("EXECUTION#")) {
          return { Attributes: createFailedExecutionItem("exec-001", 0) };
        }
        if (pk.startsWith("ERROR#")) {
          return { Attributes: createErrorRecord("01256", 0) };
        }
        return {};
      });

      // Mock batch delete
      ddbMock.on(BatchWriteCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const item = createExecutionErrorLink("exec-001", "01256");
      const record = createRemoveRecord(item);
      const event = createStreamEvent([record]);

      await handler(event);

      // Should NOT have any GSI update calls for execution (reaches zero = no GSI update)
      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      const executionGsiUpdateCall = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "EXECUTION#exec-001" &&
          call.args[0].input.UpdateExpression?.includes("GS1SK"),
      );
      expect(executionGsiUpdateCall).toBeUndefined();
    });
  });

  describe("batch delete", () => {
    it("should batch delete executions that reached zero errors", async () => {
      // Mock decrement to return zero errors
      ddbMock.on(UpdateCommand).resolves({
        Attributes: createFailedExecutionItem("exec-001", 0),
      });

      // Mock batch delete
      ddbMock.on(BatchWriteCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const item = createExecutionErrorLink("exec-001", "01256");
      const record = createRemoveRecord(item);
      const event = createStreamEvent([record]);

      await handler(event);

      // Should have called BatchWriteCommand
      expect(ddbMock).toHaveReceivedCommand(BatchWriteCommand);

      const batchWriteCalls = ddbMock.commandCalls(BatchWriteCommand);
      const deleteRequest =
        batchWriteCalls[0].args[0].input.RequestItems?.[TEST_TABLE_NAME];
      expect(deleteRequest).toBeDefined();
      expect(deleteRequest?.[0]?.DeleteRequest?.Key).toEqual({
        PK: "EXECUTION#exec-001",
        SK: "EXECUTION#exec-001",
      });
    });

    it("should batch delete multiple executions", async () => {
      // Track which execution is being updated
      let callCount = 0;
      ddbMock.on(UpdateCommand).callsFake(() => {
        callCount++;
        if (callCount === 1) {
          return { Attributes: createFailedExecutionItem("exec-001", 0) };
        }
        return { Attributes: createFailedExecutionItem("exec-002", 0) };
      });

      // Mock batch delete
      ddbMock.on(BatchWriteCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const records = [
        createRemoveRecord(
          createExecutionErrorLink("exec-001", "01256"),
          "event-1",
        ),
        createRemoveRecord(
          createExecutionErrorLink("exec-002", "01257"),
          "event-2",
        ),
      ];
      const event = createStreamEvent(records);

      await handler(event);

      // Should have called BatchWriteCommand with both executions
      expect(ddbMock).toHaveReceivedCommand(BatchWriteCommand);

      const batchWriteCalls = ddbMock.commandCalls(BatchWriteCommand);
      const deleteRequests =
        batchWriteCalls[0].args[0].input.RequestItems?.[TEST_TABLE_NAME];
      expect(deleteRequests?.length).toBe(2);
    });
  });

  describe("error handling", () => {
    it("should handle ConditionalCheckFailedException gracefully", async () => {
      // Mock decrement to throw ConditionalCheckFailedException
      const error = new Error("Condition check failed");
      error.name = "ConditionalCheckFailedException";
      ddbMock.on(UpdateCommand).rejects(error);

      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const item = createExecutionErrorLink("exec-001", "01256");
      const record = createRemoveRecord(item);
      const event = createStreamEvent([record]);

      // Should not throw
      await expect(handler(event)).resolves.not.toThrow();
    });

    it("should continue processing other executions when one fails", async () => {
      // First execution fails, second succeeds
      let callCount = 0;
      ddbMock.on(UpdateCommand).callsFake(() => {
        callCount++;
        if (callCount === 1) {
          throw new Error("First execution failed");
        }
        return { Attributes: createFailedExecutionItem("exec-002", 1) };
      });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const records = [
        createRemoveRecord(
          createExecutionErrorLink("exec-001", "01256"),
          "event-1",
        ),
        createRemoveRecord(
          createExecutionErrorLink("exec-002", "01257"),
          "event-2",
        ),
      ];
      const event = createStreamEvent(records);

      // Should not throw
      await expect(handler(event)).resolves.not.toThrow();

      // Should have attempted both updates
      expect(ddbMock.commandCalls(UpdateCommand).length).toBeGreaterThanOrEqual(
        2,
      );
    });
  });

  describe("empty and edge cases", () => {
    it("should handle empty stream event", async () => {
      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const event = createStreamEvent([]);

      // Should not throw
      await expect(handler(event)).resolves.not.toThrow();

      // Should not call any DynamoDB commands
      expect(ddbMock).not.toHaveReceivedCommand(UpdateCommand);
    });

    it("should handle records without OldImage", async () => {
      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const record: DynamoDBRecord = {
        eventID: "event-001",
        eventName: "REMOVE",
        eventVersion: "1.1",
        eventSource: "aws:dynamodb",
        awsRegion: "us-east-1",
        dynamodb: {
          Keys: marshall({ PK: "EXECUTION#exec-001", SK: "ERROR#01256" }),
          // No OldImage
        },
      };
      const event = createStreamEvent([record]);

      // Should not throw
      await expect(handler(event)).resolves.not.toThrow();

      // Should not call any DynamoDB commands
      expect(ddbMock).not.toHaveReceivedCommand(UpdateCommand);
    });

    it("should handle records without executionId in OldImage", async () => {
      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const item = {
        PK: "EXECUTION#exec-001",
        SK: "ERROR#01256",
        entityType: "ExecutionError",
        // Missing executionId
      };
      const record: DynamoDBRecord = {
        eventID: "event-001",
        eventName: "REMOVE",
        eventVersion: "1.1",
        eventSource: "aws:dynamodb",
        awsRegion: "us-east-1",
        dynamodb: {
          OldImage: marshall(item),
        },
      };
      const event = createStreamEvent([record]);

      // Should not throw
      await expect(handler(event)).resolves.not.toThrow();

      // Should not call any DynamoDB commands
      expect(ddbMock).not.toHaveReceivedCommand(UpdateCommand);
    });
  });
});

describe("batch repository functions", () => {
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
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  describe("batchDecrementOpenErrorCounts", () => {
    it("should decrement multiple executions in parallel", async () => {
      ddbMock.on(UpdateCommand).callsFake((input) => {
        const pk = input.Key?.PK as string;
        const execId = pk.replace("EXECUTION#", "");
        return {
          Attributes: createFailedExecutionItem(execId, 2),
        };
      });

      const { batchDecrementOpenErrorCounts } = await import(
        "shared/repository.js"
      );

      const inputs = [
        { executionId: "exec-001", decrementBy: 1 },
        { executionId: "exec-002", decrementBy: 2 },
        { executionId: "exec-003", decrementBy: 3 },
      ];

      const results = await batchDecrementOpenErrorCounts(inputs);

      expect(results.length).toBe(3);
      expect(results.every((r) => r.success)).toBe(true);
      expect(ddbMock.commandCalls(UpdateCommand).length).toBe(3);
    });

    it("should return correct new counts for each execution", async () => {
      ddbMock.on(UpdateCommand).callsFake((input) => {
        const amount = input.ExpressionAttributeValues?.[":amount"] as number;
        const pk = input.Key?.PK as string;
        const execId = pk.replace("EXECUTION#", "");
        return {
          Attributes: createFailedExecutionItem(execId, 5 - amount),
        };
      });

      const { batchDecrementOpenErrorCounts } = await import(
        "shared/repository.js"
      );

      const inputs = [
        { executionId: "exec-001", decrementBy: 1 },
        { executionId: "exec-002", decrementBy: 3 },
      ];

      const results = await batchDecrementOpenErrorCounts(inputs);

      const result001 = results.find((r) => r.executionId === "exec-001");
      const result002 = results.find((r) => r.executionId === "exec-002");

      expect(result001?.newOpenErrorCount).toBe(4);
      expect(result002?.newOpenErrorCount).toBe(2);
    });

    it("should return empty array for empty input", async () => {
      const { batchDecrementOpenErrorCounts } = await import(
        "shared/repository.js"
      );

      const results = await batchDecrementOpenErrorCounts([]);

      expect(results).toEqual([]);
      expect(ddbMock).not.toHaveReceivedCommand(UpdateCommand);
    });

    it("should handle ConditionalCheckFailedException gracefully", async () => {
      const error = new Error("Condition check failed");
      error.name = "ConditionalCheckFailedException";
      ddbMock.on(UpdateCommand).rejects(error);

      const { batchDecrementOpenErrorCounts } = await import(
        "shared/repository.js"
      );

      const inputs = [{ executionId: "exec-001", decrementBy: 1 }];

      const results = await batchDecrementOpenErrorCounts(inputs);

      expect(results.length).toBe(1);
      expect(results[0].success).toBe(false);
      expect(results[0].found).toBe(false);
    });
  });

  describe("batchUpdateExecutionGsiKeys", () => {
    it("should update GSI keys for multiple executions", async () => {
      ddbMock.on(UpdateCommand).resolves({});

      const { batchUpdateExecutionGsiKeys } = await import(
        "shared/repository.js"
      );

      const updates = [
        { executionId: "exec-001", newOpenErrorCount: 3, errorType: "01" },
        { executionId: "exec-002", newOpenErrorCount: 5, errorType: "02" },
      ];

      await batchUpdateExecutionGsiKeys(updates);

      expect(ddbMock.commandCalls(UpdateCommand).length).toBe(2);

      // Verify GSI key format
      const calls = ddbMock.commandCalls(UpdateCommand);
      const call001 = calls.find(
        (c) => c.args[0].input.Key?.PK === "EXECUTION#exec-001",
      );
      const call002 = calls.find(
        (c) => c.args[0].input.Key?.PK === "EXECUTION#exec-002",
      );

      expect(
        call001?.args[0].input.ExpressionAttributeValues?.[":gs1sk"],
      ).toMatch(/^COUNT#FAILED#\d{10}#EXECUTION#exec-001$/);
      expect(
        call001?.args[0].input.ExpressionAttributeValues?.[":gs3sk"],
      ).toMatch(/^COUNT#01#FAILED#\d{10}#EXECUTION#exec-001$/);

      expect(
        call002?.args[0].input.ExpressionAttributeValues?.[":gs1sk"],
      ).toMatch(/^COUNT#FAILED#\d{10}#EXECUTION#exec-002$/);
      expect(
        call002?.args[0].input.ExpressionAttributeValues?.[":gs3sk"],
      ).toMatch(/^COUNT#02#FAILED#\d{10}#EXECUTION#exec-002$/);
    });

    it("should handle empty input", async () => {
      const { batchUpdateExecutionGsiKeys } = await import(
        "shared/repository.js"
      );

      await batchUpdateExecutionGsiKeys([]);

      expect(ddbMock).not.toHaveReceivedCommand(UpdateCommand);
    });

    it("should continue processing when one update fails", async () => {
      let callCount = 0;
      ddbMock.on(UpdateCommand).callsFake(() => {
        callCount++;
        if (callCount === 1) {
          throw new Error("First update failed");
        }
        return {};
      });

      const { batchUpdateExecutionGsiKeys } = await import(
        "shared/repository.js"
      );

      const updates = [
        { executionId: "exec-001", newOpenErrorCount: 3, errorType: "01" },
        { executionId: "exec-002", newOpenErrorCount: 5, errorType: "02" },
      ];

      // Should not throw
      await expect(batchUpdateExecutionGsiKeys(updates)).resolves.not.toThrow();

      // Should have attempted both updates
      expect(ddbMock.commandCalls(UpdateCommand).length).toBe(2);
    });
  });

  describe("batchDeleteFailedExecutionItems", () => {
    it("should delete multiple executions using BatchWriteCommand", async () => {
      ddbMock.on(BatchWriteCommand).resolves({});

      const { batchDeleteFailedExecutionItems } = await import(
        "shared/repository.js"
      );

      const executionIds = ["exec-001", "exec-002", "exec-003"];

      const deletedIds = await batchDeleteFailedExecutionItems(executionIds);

      expect(deletedIds.length).toBe(3);
      expect(ddbMock).toHaveReceivedCommand(BatchWriteCommand);

      const batchWriteCalls = ddbMock.commandCalls(BatchWriteCommand);
      const deleteRequests =
        batchWriteCalls[0].args[0].input.RequestItems?.[TEST_TABLE_NAME];
      expect(deleteRequests?.length).toBe(3);
    });

    it("should handle empty input", async () => {
      const { batchDeleteFailedExecutionItems } = await import(
        "shared/repository.js"
      );

      const deletedIds = await batchDeleteFailedExecutionItems([]);

      expect(deletedIds).toEqual([]);
      expect(ddbMock).not.toHaveReceivedCommand(BatchWriteCommand);
    });

    it("should batch deletions in groups of 25", async () => {
      ddbMock.on(BatchWriteCommand).resolves({});

      const { batchDeleteFailedExecutionItems } = await import(
        "shared/repository.js"
      );

      // Create 30 execution IDs (should result in 2 batches)
      const executionIds = Array.from(
        { length: 30 },
        (_, i) => `exec-${String(i).padStart(3, "0")}`,
      );

      await batchDeleteFailedExecutionItems(executionIds);

      // Should have called BatchWriteCommand twice (25 + 5)
      expect(ddbMock.commandCalls(BatchWriteCommand).length).toBe(2);

      const calls = ddbMock.commandCalls(BatchWriteCommand);
      const firstBatch =
        calls[0].args[0].input.RequestItems?.[TEST_TABLE_NAME]?.length;
      const secondBatch =
        calls[1].args[0].input.RequestItems?.[TEST_TABLE_NAME]?.length;

      expect(firstBatch).toBe(25);
      expect(secondBatch).toBe(5);
    });

    it("should retry unprocessed items", async () => {
      // First call returns some unprocessed items
      let callCount = 0;
      ddbMock.on(BatchWriteCommand).callsFake((input) => {
        callCount++;
        if (callCount === 1) {
          return {
            UnprocessedItems: {
              [TEST_TABLE_NAME]: [
                {
                  DeleteRequest: {
                    Key: { PK: "EXECUTION#exec-002", SK: "EXECUTION#exec-002" },
                  },
                },
              ],
            },
          };
        }
        return {};
      });

      const { batchDeleteFailedExecutionItems } = await import(
        "shared/repository.js"
      );

      const executionIds = ["exec-001", "exec-002", "exec-003"];

      await batchDeleteFailedExecutionItems(executionIds);

      // Should have called BatchWriteCommand twice (initial + retry)
      expect(ddbMock.commandCalls(BatchWriteCommand).length).toBe(2);
    });

    it("should correctly track deleted items when unprocessed items are in arbitrary order", async () => {
      // Simulate unprocessed items in the middle (not at the end)
      // This tests that we correctly identify deleted items by key comparison, not position
      let callCount = 0;
      ddbMock.on(BatchWriteCommand).callsFake(() => {
        callCount++;
        if (callCount === 1) {
          // Return exec-002 (middle item) as unprocessed
          return {
            UnprocessedItems: {
              [TEST_TABLE_NAME]: [
                {
                  DeleteRequest: {
                    Key: { PK: "EXECUTION#exec-002", SK: "EXECUTION#exec-002" },
                  },
                },
              ],
            },
          };
        }
        // Second call succeeds for exec-002
        return {};
      });

      const { batchDeleteFailedExecutionItems } = await import(
        "shared/repository.js"
      );

      const executionIds = ["exec-001", "exec-002", "exec-003"];

      const deletedIds = await batchDeleteFailedExecutionItems(executionIds);

      // All three should be deleted (exec-001 and exec-003 first, then exec-002 on retry)
      expect(deletedIds).toHaveLength(3);
      expect(deletedIds).toContain("exec-001");
      expect(deletedIds).toContain("exec-002");
      expect(deletedIds).toContain("exec-003");
    });
  });

  describe("batchDecrementErrorRecordCounts", () => {
    it("should decrement multiple error records in parallel", async () => {
      ddbMock.on(UpdateCommand).callsFake((input) => {
        const pk = input.Key?.PK as string;
        const errorCode = pk.replace("ERROR#", "");
        return {
          Attributes: createErrorRecord(errorCode, 2),
        };
      });

      const { batchDecrementErrorRecordCounts } = await import(
        "shared/repository.js"
      );

      const inputs = [
        { errorCode: "01256", decrementBy: 1 },
        { errorCode: "01257", decrementBy: 2 },
        { errorCode: "02001", decrementBy: 3 },
      ];

      const results = await batchDecrementErrorRecordCounts(inputs);

      expect(results.length).toBe(3);
      expect(results.every((r) => r.success)).toBe(true);
      expect(ddbMock.commandCalls(UpdateCommand).length).toBe(3);
    });

    it("should return correct new counts for each error code", async () => {
      ddbMock.on(UpdateCommand).callsFake((input) => {
        const amount = input.ExpressionAttributeValues?.[":amount"] as number;
        const pk = input.Key?.PK as string;
        const errorCode = pk.replace("ERROR#", "");
        return {
          Attributes: createErrorRecord(errorCode, 10 - amount),
        };
      });

      const { batchDecrementErrorRecordCounts } = await import(
        "shared/repository.js"
      );

      const inputs = [
        { errorCode: "01256", decrementBy: 3 },
        { errorCode: "01257", decrementBy: 5 },
      ];

      const results = await batchDecrementErrorRecordCounts(inputs);

      const result01256 = results.find((r) => r.errorCode === "01256");
      const result01257 = results.find((r) => r.errorCode === "01257");

      expect(result01256?.newTotalCount).toBe(7);
      expect(result01257?.newTotalCount).toBe(5);
    });

    it("should return empty array for empty input", async () => {
      const { batchDecrementErrorRecordCounts } = await import(
        "shared/repository.js"
      );

      const results = await batchDecrementErrorRecordCounts([]);

      expect(results).toEqual([]);
      expect(ddbMock).not.toHaveReceivedCommand(UpdateCommand);
    });

    it("should handle ConditionalCheckFailedException gracefully", async () => {
      const error = new Error("Condition check failed");
      error.name = "ConditionalCheckFailedException";
      ddbMock.on(UpdateCommand).rejects(error);

      const { batchDecrementErrorRecordCounts } = await import(
        "shared/repository.js"
      );

      const inputs = [{ errorCode: "01256", decrementBy: 1 }];

      const results = await batchDecrementErrorRecordCounts(inputs);

      expect(results.length).toBe(1);
      expect(results[0].success).toBe(false);
      expect(results[0].found).toBe(false);
    });

    it("should return errorType for successful decrements", async () => {
      ddbMock.on(UpdateCommand).resolves({
        Attributes: createErrorRecord("01256", 5, "01"),
      });

      const { batchDecrementErrorRecordCounts } = await import(
        "shared/repository.js"
      );

      const inputs = [{ errorCode: "01256", decrementBy: 1 }];

      const results = await batchDecrementErrorRecordCounts(inputs);

      expect(results[0].errorType).toBe("01");
    });
  });

  describe("batchUpdateErrorRecordGsiKeys", () => {
    it("should update GSI keys for multiple error records", async () => {
      ddbMock.on(UpdateCommand).resolves({});

      const { batchUpdateErrorRecordGsiKeys } = await import(
        "shared/repository.js"
      );

      const updates = [
        { errorCode: "01256", newTotalCount: 3, errorType: "01" },
        { errorCode: "02001", newTotalCount: 5, errorType: "02" },
      ];

      await batchUpdateErrorRecordGsiKeys(updates);

      expect(ddbMock.commandCalls(UpdateCommand).length).toBe(2);

      // Verify GSI key format
      const calls = ddbMock.commandCalls(UpdateCommand);
      const call01256 = calls.find(
        (c) => c.args[0].input.Key?.PK === "ERROR#01256",
      );
      const call02001 = calls.find(
        (c) => c.args[0].input.Key?.PK === "ERROR#02001",
      );

      // GS2SK includes FAILED status
      expect(
        call01256?.args[0].input.ExpressionAttributeValues?.[":gs2sk"],
      ).toMatch(/^COUNT#FAILED#\d{10}#ERROR#01256$/);
      // GS3SK does not include status for error records
      expect(
        call01256?.args[0].input.ExpressionAttributeValues?.[":gs3sk"],
      ).toMatch(/^COUNT#01#\d{10}#ERROR#01256$/);

      // GS2SK includes FAILED status
      expect(
        call02001?.args[0].input.ExpressionAttributeValues?.[":gs2sk"],
      ).toMatch(/^COUNT#FAILED#\d{10}#ERROR#02001$/);
      // GS3SK does not include status for error records
      expect(
        call02001?.args[0].input.ExpressionAttributeValues?.[":gs3sk"],
      ).toMatch(/^COUNT#02#\d{10}#ERROR#02001$/);
    });

    it("should handle empty input", async () => {
      const { batchUpdateErrorRecordGsiKeys } = await import(
        "shared/repository.js"
      );

      await batchUpdateErrorRecordGsiKeys([]);

      expect(ddbMock).not.toHaveReceivedCommand(UpdateCommand);
    });

    it("should continue processing when one update fails", async () => {
      let callCount = 0;
      ddbMock.on(UpdateCommand).callsFake(() => {
        callCount++;
        if (callCount === 1) {
          throw new Error("First update failed");
        }
        return {};
      });

      const { batchUpdateErrorRecordGsiKeys } = await import(
        "shared/repository.js"
      );

      const updates = [
        { errorCode: "01256", newTotalCount: 3, errorType: "01" },
        { errorCode: "02001", newTotalCount: 5, errorType: "02" },
      ];

      // Should not throw
      await expect(
        batchUpdateErrorRecordGsiKeys(updates),
      ).resolves.not.toThrow();

      // Should have attempted both updates
      expect(ddbMock.commandCalls(UpdateCommand).length).toBe(2);
    });
  });

  describe("batchDeleteErrorRecords", () => {
    it("should delete multiple error records using BatchWriteCommand", async () => {
      ddbMock.on(BatchWriteCommand).resolves({});

      const { batchDeleteErrorRecords } = await import("shared/repository.js");

      const errorCodes = ["01256", "01257", "02001"];

      const deletedCodes = await batchDeleteErrorRecords(errorCodes);

      expect(deletedCodes.length).toBe(3);
      expect(ddbMock).toHaveReceivedCommand(BatchWriteCommand);

      const batchWriteCalls = ddbMock.commandCalls(BatchWriteCommand);
      const deleteRequests =
        batchWriteCalls[0].args[0].input.RequestItems?.[TEST_TABLE_NAME];
      expect(deleteRequests?.length).toBe(3);
    });

    it("should handle empty input", async () => {
      const { batchDeleteErrorRecords } = await import("shared/repository.js");

      const deletedCodes = await batchDeleteErrorRecords([]);

      expect(deletedCodes).toEqual([]);
      expect(ddbMock).not.toHaveReceivedCommand(BatchWriteCommand);
    });

    it("should batch deletions in groups of 25", async () => {
      ddbMock.on(BatchWriteCommand).resolves({});

      const { batchDeleteErrorRecords } = await import("shared/repository.js");

      // Create 30 error codes (should result in 2 batches)
      const errorCodes = Array.from(
        { length: 30 },
        (_, i) => `01${String(i).padStart(3, "0")}`,
      );

      await batchDeleteErrorRecords(errorCodes);

      // Should have called BatchWriteCommand twice (25 + 5)
      expect(ddbMock.commandCalls(BatchWriteCommand).length).toBe(2);

      const calls = ddbMock.commandCalls(BatchWriteCommand);
      const firstBatch =
        calls[0].args[0].input.RequestItems?.[TEST_TABLE_NAME]?.length;
      const secondBatch =
        calls[1].args[0].input.RequestItems?.[TEST_TABLE_NAME]?.length;

      expect(firstBatch).toBe(25);
      expect(secondBatch).toBe(5);
    });
  });
});

describe("error record processing in handler", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.resetModules();
    ddbMock.reset();
    sfnMock.reset();
    process.env = {
      ...originalEnv,
      WORKFLOW_ERRORS_TABLE_NAME: TEST_TABLE_NAME,
    };
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "info").mockImplementation(() => {});
    vi.spyOn(console, "debug").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  describe("error record total count decrement", () => {
    it("should decrement error record totalCount when ExecutionErrorLink is deleted", async () => {
      // Mock execution decrement
      ddbMock.on(UpdateCommand).callsFake((input) => {
        const pk = input.Key?.PK as string;
        if (pk.startsWith("EXECUTION#")) {
          return { Attributes: createFailedExecutionItem("exec-001", 2) };
        }
        // Error record decrement
        if (pk.startsWith("ERROR#")) {
          return { Attributes: createErrorRecord("01256", 5) };
        }
        return {};
      });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const item = createExecutionErrorLink("exec-001", "01256", 3);
      const record = createRemoveRecord(item);
      const event = createStreamEvent([record]);

      await handler(event);

      // Should have called UpdateCommand for both execution and error record
      const updateCalls = ddbMock.commandCalls(UpdateCommand);

      // Find execution decrement call
      const execDecrementCall = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "EXECUTION#exec-001" &&
          call.args[0].input.UpdateExpression?.includes("openErrorCount"),
      );
      expect(execDecrementCall).toBeDefined();

      // Find error record decrement call
      const errorDecrementCall = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "ERROR#01256" &&
          call.args[0].input.UpdateExpression?.includes("totalCount"),
      );
      expect(errorDecrementCall).toBeDefined();
      expect(
        errorDecrementCall?.args[0].input.ExpressionAttributeValues?.[
          ":amount"
        ],
      ).toBe(3); // Should decrement by occurrences value
    });

    it("should aggregate occurrences for same error code from multiple links", async () => {
      // Mock execution decrement
      ddbMock.on(UpdateCommand).callsFake((input) => {
        const pk = input.Key?.PK as string;
        if (pk.startsWith("EXECUTION#")) {
          const execId = pk.replace("EXECUTION#", "");
          return { Attributes: createFailedExecutionItem(execId, 1) };
        }
        // Error record decrement
        if (pk.startsWith("ERROR#")) {
          const errorCode = pk.replace("ERROR#", "");
          return { Attributes: createErrorRecord(errorCode, 5) };
        }
        return {};
      });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      // Two different executions with the same error code
      const records = [
        createRemoveRecord(
          createExecutionErrorLink("exec-001", "01256", 3),
          "event-1",
        ),
        createRemoveRecord(
          createExecutionErrorLink("exec-002", "01256", 5),
          "event-2",
        ),
      ];
      const event = createStreamEvent(records);

      await handler(event);

      // Find error record decrement call - should be called once with sum of occurrences
      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      const errorDecrementCall = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "ERROR#01256" &&
          call.args[0].input.UpdateExpression?.includes("totalCount"),
      );
      expect(errorDecrementCall).toBeDefined();
      expect(
        errorDecrementCall?.args[0].input.ExpressionAttributeValues?.[
          ":amount"
        ],
      ).toBe(8); // 3 + 5
    });

    it("should update GSI keys for error records with remaining count", async () => {
      // Mock decrement to return remaining count
      ddbMock.on(UpdateCommand).callsFake((input) => {
        const pk = input.Key?.PK as string;
        if (pk.startsWith("EXECUTION#")) {
          return { Attributes: createFailedExecutionItem("exec-001", 2) };
        }
        if (pk.startsWith("ERROR#")) {
          return { Attributes: createErrorRecord("01256", 5, "01") };
        }
        return {};
      });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const item = createExecutionErrorLink("exec-001", "01256", 1);
      const record = createRemoveRecord(item);
      const event = createStreamEvent([record]);

      await handler(event);

      // Find GSI update call for error record (contains GS2SK)
      const updateCalls = ddbMock.commandCalls(UpdateCommand);
      const gsiUpdateCall = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "ERROR#01256" &&
          call.args[0].input.UpdateExpression?.includes("GS2SK"),
      );
      expect(gsiUpdateCall).toBeDefined();
      // GS2SK includes FAILED status
      expect(
        gsiUpdateCall?.args[0].input.ExpressionAttributeValues?.[":gs2sk"],
      ).toMatch(/^COUNT#FAILED#\d{10}#ERROR#01256$/);
      // GS3SK does not include status for error records
      expect(
        gsiUpdateCall?.args[0].input.ExpressionAttributeValues?.[":gs3sk"],
      ).toMatch(/^COUNT#01#\d{10}#ERROR#01256$/);
    });

    it("should delete error record when totalCount reaches zero", async () => {
      // Mock decrement to return zero count
      ddbMock.on(UpdateCommand).callsFake((input) => {
        const pk = input.Key?.PK as string;
        if (pk.startsWith("EXECUTION#")) {
          return { Attributes: createFailedExecutionItem("exec-001", 0) };
        }
        if (pk.startsWith("ERROR#")) {
          return { Attributes: createErrorRecord("01256", 0) };
        }
        return {};
      });

      // Mock batch delete
      ddbMock.on(BatchWriteCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const item = createExecutionErrorLink("exec-001", "01256", 5);
      const record = createRemoveRecord(item);
      const event = createStreamEvent([record]);

      await handler(event);

      // Should have called BatchWriteCommand to delete both execution and error record
      expect(ddbMock).toHaveReceivedCommand(BatchWriteCommand);

      const batchWriteCalls = ddbMock.commandCalls(BatchWriteCommand);

      // Find the delete request for the error record
      const allDeleteRequests = batchWriteCalls.flatMap(
        (call) => call.args[0].input.RequestItems?.[TEST_TABLE_NAME] ?? [],
      );

      const errorDeleteRequest = allDeleteRequests.find(
        (req) => req.DeleteRequest?.Key?.PK === "ERROR#01256",
      );
      expect(errorDeleteRequest).toBeDefined();
    });

    it("should process multiple different error codes in parallel", async () => {
      // Mock decrements
      ddbMock.on(UpdateCommand).callsFake((input) => {
        const pk = input.Key?.PK as string;
        if (pk.startsWith("EXECUTION#")) {
          return { Attributes: createFailedExecutionItem("exec-001", 0) };
        }
        if (pk === "ERROR#01256") {
          return { Attributes: createErrorRecord("01256", 3) };
        }
        if (pk === "ERROR#02001") {
          return { Attributes: createErrorRecord("02001", 5) };
        }
        return {};
      });

      // Mock batch delete
      ddbMock.on(BatchWriteCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow-events/lambdas/error-count-handler/index.js"
      );

      const records = [
        createRemoveRecord(
          createExecutionErrorLink("exec-001", "01256", 2),
          "event-1",
        ),
        createRemoveRecord(
          createExecutionErrorLink("exec-001", "02001", 3),
          "event-2",
        ),
      ];
      const event = createStreamEvent(records);

      await handler(event);

      // Find error record decrement calls
      const updateCalls = ddbMock.commandCalls(UpdateCommand);

      const decrement01256 = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "ERROR#01256" &&
          call.args[0].input.UpdateExpression?.includes("totalCount"),
      );
      const decrement02001 = updateCalls.find(
        (call) =>
          call.args[0].input.Key?.PK === "ERROR#02001" &&
          call.args[0].input.UpdateExpression?.includes("totalCount"),
      );

      expect(decrement01256).toBeDefined();
      expect(decrement02001).toBeDefined();
      expect(
        decrement01256?.args[0].input.ExpressionAttributeValues?.[":amount"],
      ).toBe(2);
      expect(
        decrement02001?.args[0].input.ExpressionAttributeValues?.[":amount"],
      ).toBe(3);
    });
  });
});
