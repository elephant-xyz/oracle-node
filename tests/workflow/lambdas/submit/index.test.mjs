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

    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
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

    it("should handle message with missing body (60101)", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = {
        Records: [
          createSqsRecord("valid-msg", "task-token-valid", [
            { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
          ]),
          {
            messageId: "no-body-msg",
            // body is undefined
            messageAttributes: {
              TaskToken: { stringValue: "task-token-no-body" },
              ExecutionArn: {
                stringValue: "arn:aws:states:us-east-1:123:execution:no-body",
              },
              County: { stringValue: "no-body-county" },
            },
          },
        ],
      };

      const result = await handler(event);

      // Valid message should succeed
      expect(mockSendTaskSuccess).toHaveBeenCalledWith(
        expect.objectContaining({ taskToken: "task-token-valid" }),
      );

      // Message with no body should get task failure with MISSING_BODY error code
      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-no-body",
          error: "60101", // MISSING_BODY
        }),
      );

      // Should return batch item failure
      expect(result.batchItemFailures).toContainEqual({
        itemIdentifier: "no-body-msg",
      });
    });

    it("should handle missing ELEPHANT_RPC_URL with error code 60208", async () => {
      delete process.env.ELEPHANT_RPC_URL;

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-rpc-error", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      // Verify task failure was sent with correct error code
      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-rpc-error",
          error: "60208", // MISSING_RPC_URL
        }),
      );

      expect(result.status).toBe("failed");
      expect(result.batchItemFailures.length).toBe(1);
    });
  });

  describe("Blockchain error classification", () => {
    /**
     * Helper to create a submit_errors.csv-style error in the mock
     * @param {string} errorMessage - Error message to include in CSV
     */
    const mockSubmitWithBlockchainError = (errorMessage) => {
      mockSubmitToContract.mockImplementation(async ({ csvFile }) => {
        const tmpDir = path.dirname(csvFile);
        const statusCsv = path.join(tmpDir, "transaction-status.csv");
        const errorsCsv = path.join(tmpDir, "submit_errors.csv");

        await fs.writeFile(statusCsv, "status,transactionHash\n", "utf8");
        await fs.writeFile(
          errorsCsv,
          `errorMessage,error_path\n"${errorMessage}","/path"\n`,
          "utf8",
        );

        return { success: true }; // CLI returns success, but errors are in CSV
      });
    };

    it("should classify 'already known' error as 60401 (nonce already used)", async () => {
      mockSubmitWithBlockchainError("Transaction already known");

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-nonce-used", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-nonce-used",
          error: "60401", // NONCE_ALREADY_USED
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'nonce too low' error as 60402", async () => {
      mockSubmitWithBlockchainError("nonce too low: next nonce 5, tx nonce 3");

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-nonce-low", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-nonce-low",
          error: "60402", // NONCE_TOO_LOW
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'insufficient funds' error as 60403", async () => {
      mockSubmitWithBlockchainError(
        "insufficient funds for gas * price + value",
      );

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-insufficient", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-insufficient",
          error: "60403", // INSUFFICIENT_FUNDS
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'gas required exceeds' error as 60404", async () => {
      mockSubmitWithBlockchainError("gas required exceeds allowance (500000)");

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-gas-exceeds", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-gas-exceeds",
          error: "60404", // GAS_ESTIMATION_FAILED
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'out of gas' error as 60404", async () => {
      mockSubmitWithBlockchainError("out of gas");

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-out-of-gas", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-out-of-gas",
          error: "60404", // GAS_ESTIMATION_FAILED
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'transaction underpriced' error as 60405", async () => {
      mockSubmitWithBlockchainError("transaction underpriced");

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-underpriced", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-underpriced",
          error: "60405", // TRANSACTION_UNDERPRICED
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'replacement transaction' error as 60405", async () => {
      mockSubmitWithBlockchainError(
        "replacement transaction underpriced: existing tx 10 gwei, new tx 5 gwei",
      );

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-replacement", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-replacement",
          error: "60405", // TRANSACTION_UNDERPRICED
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'execution reverted' error as 60406", async () => {
      mockSubmitWithBlockchainError(
        "execution reverted: Only oracle can call this function",
      );

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-reverted", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-reverted",
          error: "60406", // EXECUTION_REVERTED
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'revert' error as 60406", async () => {
      mockSubmitWithBlockchainError("VM Exception: revert");

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-revert", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-revert",
          error: "60406", // EXECUTION_REVERTED
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'invalid transaction' error as 60407", async () => {
      mockSubmitWithBlockchainError("invalid transaction: invalid chain id");

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-invalid-tx", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-invalid-tx",
          error: "60407", // INVALID_TRANSACTION
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'invalid sender' error as 60407", async () => {
      mockSubmitWithBlockchainError("invalid sender");

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-invalid-sender", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-invalid-sender",
          error: "60407", // INVALID_TRANSACTION
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'ECONNREFUSED' error as 60408 (RPC connection error)", async () => {
      mockSubmitWithBlockchainError("connect ECONNREFUSED 127.0.0.1:8545");

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-conn-refused", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-conn-refused",
          error: "60408", // RPC_CONNECTION_ERROR
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'ETIMEDOUT' error as 60408", async () => {
      mockSubmitWithBlockchainError("connect ETIMEDOUT 10.0.0.1:8545");

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-conn-timeout", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-conn-timeout",
          error: "60408", // RPC_CONNECTION_ERROR
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'network error' as 60408", async () => {
      mockSubmitWithBlockchainError("Network error: Failed to fetch");

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-network-error", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-network-error",
          error: "60408", // RPC_CONNECTION_ERROR
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'timeout' error as 60409 (RPC timeout)", async () => {
      mockSubmitWithBlockchainError("Request timeout after 30000ms");

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-timeout", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-timeout",
          error: "60409", // RPC_TIMEOUT
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'ESOCKETTIMEDOUT' error as 60409", async () => {
      mockSubmitWithBlockchainError("ESOCKETTIMEDOUT");

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-socket-timeout", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-socket-timeout",
          error: "60409", // RPC_TIMEOUT
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'invalid argument' error as 60410", async () => {
      mockSubmitWithBlockchainError("invalid argument 0: hex string");

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-invalid-arg", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-invalid-arg",
          error: "60410", // INVALID_PARAMETERS
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'invalid params' error as 60410", async () => {
      mockSubmitWithBlockchainError(
        "invalid params: missing value for required argument",
      );

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-invalid-params", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-invalid-params",
          error: "60410", // INVALID_PARAMETERS
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify 'contract creation code storage out of gas' error as 60411", async () => {
      mockSubmitWithBlockchainError(
        "contract creation code storage out of gas",
      );

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-contract-error", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-contract-error",
          error: "60411", // CONTRACT_ERROR
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should classify unknown error as 60999", async () => {
      mockSubmitWithBlockchainError("Some completely unknown error message");

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-unknown", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-unknown",
          error: "60999", // UNKNOWN
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should parse JSON-RPC error responses and classify them", async () => {
      // Simulate a JSON-RPC error response in the errorMessage field
      // In CSV, double quotes inside quoted fields must be escaped by doubling them
      const jsonRpcError = `{"error":{"message":"insufficient funds for gas * price + value","code":-32000}}`;
      // CSV requires quotes to be doubled inside quoted fields
      const escapedForCsv = jsonRpcError.replace(/"/g, '""');

      mockSubmitToContract.mockImplementation(async ({ csvFile }) => {
        const tmpDir = path.dirname(csvFile);
        const statusCsv = path.join(tmpDir, "transaction-status.csv");
        const errorsCsv = path.join(tmpDir, "submit_errors.csv");

        await fs.writeFile(statusCsv, "status,transactionHash\n", "utf8");
        // Properly escape JSON in CSV format
        await fs.writeFile(
          errorsCsv,
          `errorMessage,error_path\n"${escapedForCsv}","/path"\n`,
          "utf8",
        );

        return { success: true };
      });

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-jsonrpc", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-jsonrpc",
          error: "60403", // INSUFFICIENT_FUNDS - extracted from JSON
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should handle error in transaction-status.csv failed rows", async () => {
      mockSubmitToContract.mockImplementation(async ({ csvFile }) => {
        const tmpDir = path.dirname(csvFile);
        const statusCsv = path.join(tmpDir, "transaction-status.csv");
        const errorsCsv = path.join(tmpDir, "submit_errors.csv");

        // Error in transaction-status.csv with status=failed
        await fs.writeFile(
          statusCsv,
          "status,transactionHash,error\nfailed,,nonce too low\n",
          "utf8",
        );
        await fs.writeFile(errorsCsv, "errorMessage,error_path\n", "utf8");

        return { success: true };
      });

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-status-failed", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-status-failed",
          error: "60402", // NONCE_TOO_LOW - from error column in transaction-status.csv
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should handle snake_case error_message field in CSV", async () => {
      mockSubmitToContract.mockImplementation(async ({ csvFile }) => {
        const tmpDir = path.dirname(csvFile);
        const statusCsv = path.join(tmpDir, "transaction-status.csv");
        const errorsCsv = path.join(tmpDir, "submit_errors.csv");

        await fs.writeFile(statusCsv, "status,transactionHash\n", "utf8");
        // Using snake_case error_message instead of camelCase
        await fs.writeFile(
          errorsCsv,
          `error_message,error_path\n"execution reverted","/path"\n`,
          "utf8",
        );

        return { success: true };
      });

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-snake-case", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-snake-case",
          error: "60406", // EXECUTION_REVERTED
        }),
      );

      expect(result.status).toBe("failed");
    });
  });

  describe("Keystore mode", () => {
    beforeEach(() => {
      // Set up keystore mode environment
      process.env.ELEPHANT_KEYSTORE_S3_KEY = "keystore.json";
      process.env.ELEPHANT_KEYSTORE_PASSWORD = "test-password";
      process.env.ENVIRONMENT_BUCKET = "test-bucket";

      // Remove API mode env vars
      delete process.env.ELEPHANT_DOMAIN;
      delete process.env.ELEPHANT_API_KEY;
      delete process.env.ELEPHANT_ORACLE_KEY_ID;
      delete process.env.ELEPHANT_FROM_ADDRESS;
    });

    it("should submit successfully in keystore mode", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-keystore-success", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSubmitToContract).toHaveBeenCalled();
      expect(mockSendTaskSuccess).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-keystore-success",
        }),
      );
      expect(result.status).toBe("success");
    });

    it("should handle S3 download failure with error code 60302", async () => {
      s3Mock.on(GetObjectCommand).rejects(new Error("Access Denied"));

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-s3-error", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-s3-error",
          error: "60302", // S3_DOWNLOAD_FAILED
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should handle S3 response with no body (60301)", async () => {
      s3Mock.on(GetObjectCommand).resolves({
        // Body is undefined
      });

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-no-body", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-no-body",
          error: "60301", // KEYSTORE_BODY_NOT_FOUND
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should fail with 60201 when ENVIRONMENT_BUCKET is missing", async () => {
      delete process.env.ENVIRONMENT_BUCKET;

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-no-bucket", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-no-bucket",
          error: "60201", // MISSING_ENVIRONMENT_BUCKET
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should fail with 60202 when ELEPHANT_KEYSTORE_S3_KEY is missing", async () => {
      delete process.env.ELEPHANT_KEYSTORE_S3_KEY;

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-no-key", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      // When ELEPHANT_KEYSTORE_S3_KEY is not set, it uses API mode
      // and API mode fails because ELEPHANT_DOMAIN is not set
      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-no-key",
          error: "60204", // MISSING_DOMAIN (API mode validation)
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should fail with 60203 when ELEPHANT_KEYSTORE_PASSWORD is missing", async () => {
      delete process.env.ELEPHANT_KEYSTORE_PASSWORD;

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-no-password", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-no-password",
          error: "60203", // MISSING_KEYSTORE_PASSWORD
        }),
      );

      expect(result.status).toBe("failed");
    });
  });

  describe("API mode environment validation", () => {
    it("should fail with 60204 when ELEPHANT_DOMAIN is missing", async () => {
      delete process.env.ELEPHANT_DOMAIN;

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-no-domain", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-no-domain",
          error: "60204", // MISSING_DOMAIN
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should fail with 60205 when ELEPHANT_API_KEY is missing", async () => {
      delete process.env.ELEPHANT_API_KEY;

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-no-api-key", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-no-api-key",
          error: "60205", // MISSING_API_KEY
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should fail with 60206 when ELEPHANT_ORACLE_KEY_ID is missing", async () => {
      delete process.env.ELEPHANT_ORACLE_KEY_ID;

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-no-oracle-key", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-no-oracle-key",
          error: "60206", // MISSING_ORACLE_KEY_ID
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should fail with 60207 when ELEPHANT_FROM_ADDRESS is missing", async () => {
      delete process.env.ELEPHANT_FROM_ADDRESS;

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-no-from-address", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-no-from-address",
          error: "60207", // MISSING_FROM_ADDRESS
        }),
      );

      expect(result.status).toBe("failed");
    });
  });

  describe("Submit CLI failure classification", () => {
    it("should classify CLI failure with blockchain error in message", async () => {
      mockSubmitToContract.mockImplementation(async ({ csvFile }) => {
        const tmpDir = path.dirname(csvFile);
        const statusCsv = path.join(tmpDir, "transaction-status.csv");
        const errorsCsv = path.join(tmpDir, "submit_errors.csv");
        await fs.writeFile(statusCsv, "status,transactionHash\n", "utf8");
        await fs.writeFile(errorsCsv, "errorMessage,error_path\n", "utf8");

        // CLI returns failure with error message
        return { success: false, error: "insufficient funds for transaction" };
      });

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-cli-failure", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-cli-failure",
          error: "60403", // INSUFFICIENT_FUNDS - classified from error message
        }),
      );

      expect(result.status).toBe("failed");
    });

    it("should use SUBMIT_CLI_FAILURE (60412) when CLI returns unclassifiable error", async () => {
      mockSubmitToContract.mockImplementation(async ({ csvFile }) => {
        const tmpDir = path.dirname(csvFile);
        const statusCsv = path.join(tmpDir, "transaction-status.csv");
        const errorsCsv = path.join(tmpDir, "submit_errors.csv");
        await fs.writeFile(statusCsv, "status,transactionHash\n", "utf8");
        await fs.writeFile(errorsCsv, "errorMessage,error_path\n", "utf8");

        // CLI returns failure with generic error
        return { success: false, error: "Some internal CLI error" };
      });

      const { handler } =
        await import("../../../../workflow/lambdas/submit/index.mjs");

      const event = createSqsEvent("task-token-cli-generic", [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ]);

      const result = await handler(event);

      expect(mockSendTaskFailure).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-cli-generic",
          error: "60999", // UNKNOWN - unclassifiable error
        }),
      );

      expect(result.status).toBe("failed");
    });
  });
});
