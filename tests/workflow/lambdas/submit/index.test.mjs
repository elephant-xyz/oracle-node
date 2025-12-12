import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import {
  SFNClient,
  SendTaskSuccessCommand,
  SendTaskFailureCommand,
} from "@aws-sdk/client-sfn";
import {
  EventBridgeClient,
  PutEventsCommand,
} from "@aws-sdk/client-eventbridge";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import { promises as fs } from "fs";
import path from "path";

const sfnMock = mockClient(SFNClient);
const eventBridgeMock = mockClient(EventBridgeClient);
const s3Mock = mockClient(S3Client);
const ssmMock = mockClient(SSMClient);

// Mock the shared module
const mockExecuteWithTaskToken = vi.fn().mockResolvedValue(undefined);
const mockEmitWorkflowEvent = vi.fn().mockResolvedValue(undefined);
const mockCreateWorkflowError = vi.fn((code, details) => ({
  code,
  ...(details && { details }),
}));
const mockCreateLogger = vi.fn(() => vi.fn());
const mockSendTaskSuccess = vi.fn().mockResolvedValue(undefined);
const mockSendTaskFailure = vi.fn().mockResolvedValue(undefined);
vi.mock("shared", () => ({
  executeWithTaskToken: mockExecuteWithTaskToken,
  emitWorkflowEvent: mockEmitWorkflowEvent,
  createWorkflowError: mockCreateWorkflowError,
  createLogger: mockCreateLogger,
  sendTaskSuccess: mockSendTaskSuccess,
  sendTaskFailure: mockSendTaskFailure,
}));

// Mock @elephant-xyz/cli/lib
const mockSubmitToContract = vi.fn();
vi.mock("@elephant-xyz/cli/lib", () => ({
  submitToContract: mockSubmitToContract,
}));

describe("submit handler", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.clearAllMocks();
    sfnMock.reset();
    eventBridgeMock.reset();
    s3Mock.reset();
    ssmMock.reset();
    mockExecuteWithTaskToken.mockClear();
    mockEmitWorkflowEvent.mockClear();
    mockCreateWorkflowError.mockClear();
    mockCreateLogger.mockClear();
    mockSendTaskSuccess.mockClear();
    mockSendTaskFailure.mockClear();

    process.env = {
      ...originalEnv,
      ELEPHANT_RPC_URL: "https://rpc.example.com",
      ELEPHANT_DOMAIN: "https://api.example.com",
      ELEPHANT_API_KEY: "test-api-key",
      ELEPHANT_ORACLE_KEY_ID: "test-key-id",
      ELEPHANT_FROM_ADDRESS: "0x1234567890123456789012345678901234567890",
      KEYSTORE_S3_BUCKET: "keystore-bucket",
      KEYSTORE_S3_KEY: "keystore.json",
      GAS_PRICE_PARAMETER_NAME: "/elephant-oracle-node/gas-price",
    };

    // Default mock implementations
    s3Mock.on(GetObjectCommand).resolves({
      Body: {
        on: vi.fn((event, callback) => {
          if (event === "data") {
            callback(Buffer.from('{"address":"0x123","crypto":{}}'));
          } else if (event === "end") {
            callback();
          }
        }),
      },
    });

    ssmMock.on(GetParameterCommand).resolves({
      Parameter: { Value: "25" },
    });

    // Mock submitToContract to create CSV files in temp directory
    mockSubmitToContract.mockImplementation(async ({ csvFile }) => {
      const tmpDir = path.dirname(csvFile);
      const statusCsv = path.join(tmpDir, "transaction-status.csv");
      const errorsCsv = path.join(tmpDir, "submit_errors.csv");

      // Create transaction-status.csv
      await fs.writeFile(
        statusCsv,
        "status,transactionHash\nsuccess,0xabc123def456\n",
        "utf8",
      );

      // Create submit_errors.csv (empty for success case)
      await fs.writeFile(errorsCsv, "error_message,error_path\n", "utf8");

      return { success: true };
    });

    vi.spyOn(console, "log").mockImplementation(() => { });
    vi.spyOn(console, "error").mockImplementation(() => { });
    vi.spyOn(console, "warn").mockImplementation(() => { });
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  /**
   * Create a single SQS record for testing
   * @param {string} messageId - SQS message ID
   * @param {string} taskToken - Step Functions task token
   * @param {Object[]} transactionItems - Transaction items array
   * @param {string} [county] - County name
   * @returns {Object} SQS record
   */
  const createSqsRecord = (
    messageId,
    taskToken,
    transactionItems,
    county = "test-county",
  ) => ({
    messageId,
    body: JSON.stringify(transactionItems),
    messageAttributes: {
      TaskToken: { stringValue: taskToken },
      ExecutionArn: {
        stringValue: `arn:aws:states:us-east-1:123456789012:execution:${messageId}`,
      },
      County: { stringValue: county },
      DataGroupLabel: { stringValue: "County" },
    },
  });

  /**
   * Create a batch SQS event with multiple records
   * @param {number} count - Number of records
   * @param {string} [prefix] - Prefix for message IDs
   * @returns {Object} SQS event with Records array
   */
  const createBatchSqsEvent = (count, prefix = "msg") => ({
    Records: Array.from({ length: count }, (_, i) =>
      createSqsRecord(
        `${prefix}-${i}`,
        `task-token-${i}`,
        [
          {
            dataGroupLabel: "County",
            dataGroupCid: `bafkrei-seed-${i}`,
            propertyCid: `prop-${i}`,
          },
          {
            dataGroupLabel: "County",
            dataGroupCid: `bafkrei-county-${i}`,
            propertyCid: `prop-${i}`,
          },
        ],
        `county-${i}`,
      ),
    ),
  });

  /**
   * Create a single-message SQS event (backward compatibility)
   * @param {string} taskToken - Step Functions task token
   * @param {Object[]} transactionItems - Transaction items array
   * @returns {Object} SQS event
   */
  const createSqsEvent = (taskToken, transactionItems) => ({
    Records: [createSqsRecord("single-msg", taskToken, transactionItems)],
  });

  describe("Single message processing (backward compatibility)", () => {
    it("should emit IN_PROGRESS event at start", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-123", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      await handler(event);

      // Verify IN_PROGRESS event was emitted via shared layer
      expect(mockEmitWorkflowEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          status: "IN_PROGRESS",
          phase: "Submit",
          step: "SubmitToBlockchain",
          taskToken: "task-token-123",
        }),
      );
    });

    it("should submit successfully and send task success", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-success", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      await handler(event);

      // Verify submitToContract was called
      expect(mockSubmitToContract).toHaveBeenCalled();

      // Verify task success was sent via shared layer
      expect(mockSendTaskSuccess).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-success",
        }),
      );
    });

    it("should NOT emit SUCCEEDED event on successful submission", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-succeeded", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      await handler(event);

      // Should have only 1 EventBridge call: IN_PROGRESS (no SUCCEEDED)
      expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(1);
      expect(mockEmitWorkflowEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          status: "IN_PROGRESS",
          phase: "Submit",
          step: "SubmitToBlockchain",
        }),
      );
      // Verify no SUCCEEDED event was emitted
      expect(mockEmitWorkflowEvent).not.toHaveBeenCalledWith(
        expect.objectContaining({
          status: "SUCCEEDED",
        }),
      );
    });

    it("should handle submission failure and send task failure", async () => {
      // Override mock to return failure (but still create files for error handling)
      mockSubmitToContract.mockImplementation(async ({ csvFile }) => {
        const tmpDir = path.dirname(csvFile);
        const statusCsv = path.join(tmpDir, "transaction-status.csv");
        const errorsCsv = path.join(tmpDir, "submit_errors.csv");

        // Create empty files for error case
        await fs.writeFile(statusCsv, "status,transactionHash\n", "utf8");
        await fs.writeFile(errorsCsv, "error_message,error_path\n", "utf8");

        return { success: false, error: "Submission failed" };
      });

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-failed", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      // Verify task failure was sent via shared layer
      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-failed",
        }),
      );

      // Should return batch item failures
      expect(result.batchItemFailures).toBeDefined();
      expect(result.batchItemFailures.length).toBe(1);
    });

    it("should NOT emit FAILED event on submission failure", async () => {
      // Override mock to return failure (but still create files for error handling)
      mockSubmitToContract.mockImplementation(async ({ csvFile }) => {
        const tmpDir = path.dirname(csvFile);
        const statusCsv = path.join(tmpDir, "transaction-status.csv");
        const errorsCsv = path.join(tmpDir, "submit_errors.csv");

        // Create empty files for error case
        await fs.writeFile(statusCsv, "status,transactionHash\n", "utf8");
        await fs.writeFile(errorsCsv, "error_message,error_path\n", "utf8");

        return { success: false, error: "Submission failed" };
      });

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-failed-event", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      await handler(event);

      // Should have only 1 EventBridge call: IN_PROGRESS (no FAILED)
      expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(1);
      expect(mockEmitWorkflowEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          status: "IN_PROGRESS",
          phase: "Submit",
          step: "SubmitToBlockchain",
        }),
      );
      // Verify no FAILED event was emitted
      expect(mockEmitWorkflowEvent).not.toHaveBeenCalledWith(
        expect.objectContaining({
          status: "FAILED",
        }),
      );
    });

    it("should handle external SQS invocation without task token", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = {
        Records: [
          {
            messageId: "external-msg",
            body: JSON.stringify([
              { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
            ]),
            // No messageAttributes (external process)
          },
        ],
      };

      const result = await handler(event);

      // Should NOT send Step Function callbacks (no task token)
      expect(mockSendTaskSuccess).not.toHaveBeenCalled();
      expect(mockSendTaskFailure).not.toHaveBeenCalled();

      // Should return result
      expect(result).toBeDefined();
      expect(result.status).toBe("success");
    });
  });

  describe("Batch processing (multiple messages)", () => {
    it("should process multiple messages and aggregate transaction items", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createBatchSqsEvent(5); // 5 messages, each with 2 items = 10 items total

      const result = await handler(event);

      // Verify submitToContract was called with aggregated items
      expect(mockSubmitToContract).toHaveBeenCalledTimes(1);

      // Verify result contains batch info
      expect(result.status).toBe("success");
      expect(result.batchedMessageCount).toBe(5);
      expect(result.totalItemCount).toBe(10);
    });

    it("should send task success to all messages in batch with same result", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createBatchSqsEvent(3);

      await handler(event);

      // Verify task success was sent for all 3 messages
      expect(mockSendTaskSuccess).toHaveBeenCalledTimes(3);

      // All should receive the same transaction hash in result
      const calls = mockSendTaskSuccess.mock.calls;
      expect(calls[0][0].taskToken).toBe("task-token-0");
      expect(calls[1][0].taskToken).toBe("task-token-1");
      expect(calls[2][0].taskToken).toBe("task-token-2");

      // All should have same output structure
      const firstOutput = calls[0][0].output;
      expect(firstOutput.status).toBe("success");
      expect(firstOutput.batchedMessageCount).toBe(3);
    });

    it("should emit IN_PROGRESS event for all messages in batch", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createBatchSqsEvent(5);

      await handler(event);

      // Count IN_PROGRESS events
      const inProgressCalls = mockEmitWorkflowEvent.mock.calls.filter(
        (call) => call[0].status === "IN_PROGRESS",
      );
      expect(inProgressCalls.length).toBe(5);

      // Should have only 5 IN_PROGRESS events (no SUCCEEDED events)
      expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(5);
    });

    it("should NOT emit SUCCEEDED event for each message in batch", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createBatchSqsEvent(3);

      await handler(event);

      // Count SUCCEEDED events - should be none
      const succeededCalls = mockEmitWorkflowEvent.mock.calls.filter(
        (call) => call[0].status === "SUCCEEDED",
      );
      expect(succeededCalls.length).toBe(0);

      // Verify only IN_PROGRESS events were emitted (3 total)
      expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(3);
    });

    it("should handle batch of 100 messages (max batch size)", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createBatchSqsEvent(100);

      const result = await handler(event);

      // Verify single blockchain submission for 200 items
      expect(mockSubmitToContract).toHaveBeenCalledTimes(1);

      // Verify all task tokens received success
      expect(mockSendTaskSuccess).toHaveBeenCalledTimes(100);

      // Verify result
      expect(result.status).toBe("success");
      expect(result.batchedMessageCount).toBe(100);
      expect(result.totalItemCount).toBe(200);
    });
  });

  describe("Partial batch failure handling", () => {
    it("should handle messages with invalid JSON body", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = {
        Records: [
          createSqsRecord("valid-msg", "task-token-valid", [
            { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
          ]),
          {
            messageId: "invalid-msg",
            body: "not-valid-json",
            messageAttributes: {
              TaskToken: { stringValue: "task-token-invalid" },
              ExecutionArn: {
                stringValue: "arn:aws:states:us-east-1:123:execution:invalid",
              },
              County: { stringValue: "invalid-county" },
            },
          },
        ],
      };

      const result = await handler(event);

      // Valid message should succeed
      expect(mockSendTaskSuccess).toHaveBeenCalledWith(
        expect.objectContaining({ taskToken: "task-token-valid" }),
      );

      // Invalid message should get task failure with JSON parse error code
      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-invalid",
          error: "60104", // JSON_PARSE_ERROR
        }),
      );

      // Should return batch item failure for invalid message
      expect(result.batchItemFailures).toContainEqual({
        itemIdentifier: "invalid-msg",
      });
    });

    it("should handle messages with empty transaction items array", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = {
        Records: [
          createSqsRecord("valid-msg", "task-token-valid", [
            { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
          ]),
          {
            messageId: "empty-msg",
            body: JSON.stringify([]), // Empty array
            messageAttributes: {
              TaskToken: { stringValue: "task-token-empty" },
              ExecutionArn: {
                stringValue: "arn:aws:states:us-east-1:123:execution:empty",
              },
              County: { stringValue: "empty-county" },
            },
          },
        ],
      };

      const result = await handler(event);

      // Valid message should succeed
      expect(mockSendTaskSuccess).toHaveBeenCalledWith(
        expect.objectContaining({ taskToken: "task-token-valid" }),
      );

      // Empty message should get task failure with empty transaction items error code
      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-empty",
          error: "60103", // EMPTY_TRANSACTION_ITEMS
        }),
      );

      // Should return batch item failure for empty message
      expect(result.batchItemFailures).toContainEqual({
        itemIdentifier: "empty-msg",
      });
    });

    it("should handle message with non-array body", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = {
        Records: [
          createSqsRecord("valid-msg", "task-token-valid", [
            { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
          ]),
          {
            messageId: "object-msg",
            body: JSON.stringify({ dataGroupLabel: "County" }), // Object instead of array
            messageAttributes: {
              TaskToken: { stringValue: "task-token-object" },
              ExecutionArn: {
                stringValue: "arn:aws:states:us-east-1:123:execution:object",
              },
              County: { stringValue: "object-county" },
            },
          },
        ],
      };

      const result = await handler(event);

      // Valid message should succeed
      expect(mockSendTaskSuccess).toHaveBeenCalledWith(
        expect.objectContaining({ taskToken: "task-token-valid" }),
      );

      // Non-array message should get task failure with invalid body format error code
      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-object",
          error: "60102", // INVALID_BODY_FORMAT
        }),
      );

      // Should return batch item failure
      expect(result.batchItemFailures).toContainEqual({
        itemIdentifier: "object-msg",
      });
    });

    it("should send failure to all messages when blockchain submission fails", async () => {
      mockSubmitToContract.mockImplementation(async ({ csvFile }) => {
        const tmpDir = path.dirname(csvFile);
        const statusCsv = path.join(tmpDir, "transaction-status.csv");
        const errorsCsv = path.join(tmpDir, "submit_errors.csv");
        await fs.writeFile(statusCsv, "status,transactionHash\n", "utf8");
        await fs.writeFile(errorsCsv, "error_message,error_path\n", "utf8");
        return { success: false, error: "Blockchain error" };
      });

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createBatchSqsEvent(5);

      const result = await handler(event);

      // All 5 messages should get task failure
      expect(mockSendTaskFailure).toHaveBeenCalledTimes(5);

      // No success callbacks
      expect(mockSendTaskSuccess).not.toHaveBeenCalled();

      // All messages should be in batch failures
      expect(result.batchItemFailures.length).toBe(5);
    });

    it("should return failed status when all messages fail parsing", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = {
        Records: [
          {
            messageId: "invalid-1",
            body: "not-json-1",
            messageAttributes: {
              TaskToken: { stringValue: "task-1" },
              ExecutionArn: {
                stringValue: "arn:aws:states:us-east-1:123:execution:1",
              },
            },
          },
          {
            messageId: "invalid-2",
            body: "not-json-2",
            messageAttributes: {
              TaskToken: { stringValue: "task-2" },
              ExecutionArn: {
                stringValue: "arn:aws:states:us-east-1:123:execution:2",
              },
            },
          },
        ],
      };

      const result = await handler(event);

      // No blockchain submission should happen
      expect(mockSubmitToContract).not.toHaveBeenCalled();

      // All messages should be failures
      expect(result.status).toBe("failed");
      expect(result.batchItemFailures.length).toBe(2);
    });
  });

  describe("Gas price configuration", () => {
    it("should retrieve gas price from SSM Parameter Store", async () => {
      ssmMock.reset();
      ssmMock.on(GetParameterCommand).resolves({
        Parameter: { Value: "30" },
      });

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-gas", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      await handler(event);

      expect(mockSubmitToContract).toHaveBeenCalled();
    });

    it("should handle EIP-1559 gas price format from SSM", async () => {
      ssmMock.reset();
      ssmMock.on(GetParameterCommand).resolves({
        Parameter: {
          Value: JSON.stringify({
            maxFeePerGas: "30",
            maxPriorityFeePerGas: "5",
          }),
        },
      });

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-eip1559", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      await handler(event);

      expect(mockSubmitToContract).toHaveBeenCalled();
    });

    it("should continue without gas price if SSM parameter doesn't exist", async () => {
      ssmMock.on(GetParameterCommand).rejects({
        name: "ParameterNotFound",
      });

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-no-ssm", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      await handler(event);

      // Should still succeed
      expect(mockSubmitToContract).toHaveBeenCalled();
    });
  });

  describe("Error handling", () => {
    it("should handle submitToContract throwing an error", async () => {
      mockSubmitToContract.mockRejectedValue(new Error("Network error"));

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-error", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      // Verify task failure was sent
      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-error",
        }),
      );

      // Should return batch item failure
      expect(result.batchItemFailures).toBeDefined();
    });

    it("should handle missing SQS Records", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = {};

      await expect(handler(event)).rejects.toThrow("Missing SQS Records");
    });

    it("should handle empty SQS Records array", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = { Records: [] };

      await expect(handler(event)).rejects.toThrow("Missing SQS Records");
    });
  });
});
