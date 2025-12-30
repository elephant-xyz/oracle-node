import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import { SFNClient } from "@aws-sdk/client-sfn";
import { EventBridgeClient } from "@aws-sdk/client-eventbridge";
import { SQSClient } from "@aws-sdk/client-sqs";

const sfnMock = mockClient(SFNClient);
const eventBridgeMock = mockClient(EventBridgeClient);
const sqsMock = mockClient(SQSClient);

// Mock the shared module
const mockExecuteWithTaskToken = vi.fn().mockResolvedValue(undefined);
const mockEmitWorkflowEvent = vi.fn().mockResolvedValue(undefined);
const mockCreateWorkflowError = vi.fn((code, details) => ({
  code,
  ...(details && { details }),
}));
const mockCreateLogger = vi.fn(() => vi.fn());
vi.mock("shared", () => ({
  executeWithTaskToken: mockExecuteWithTaskToken,
  emitWorkflowEvent: mockEmitWorkflowEvent,
  createWorkflowError: mockCreateWorkflowError,
  createLogger: mockCreateLogger,
}));

// Mock @elephant-xyz/cli/lib
const mockCheckTransactionStatus = vi.fn();
vi.mock("@elephant-xyz/cli/lib", () => ({
  checkTransactionStatus: mockCheckTransactionStatus,
}));

describe("transaction-status-checker handler", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.clearAllMocks();
    sfnMock.reset();
    eventBridgeMock.reset();
    sqsMock.reset();
    mockExecuteWithTaskToken.mockClear();
    mockEmitWorkflowEvent.mockClear();
    mockCreateWorkflowError.mockClear();
    mockCreateLogger.mockClear();

    process.env = {
      ...originalEnv,
      ELEPHANT_RPC_URL: "https://rpc.example.com",
    };

    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  const createSqsEvent = (taskToken, transactionHash, transactionItems) => ({
    Records: [
      {
        body: JSON.stringify({
          transactionHash: transactionHash,
          transactionItems: transactionItems || [],
        }),
        messageAttributes: {
          TaskToken: { stringValue: taskToken },
          ExecutionArn: {
            stringValue:
              "arn:aws:states:us-east-1:123456789012:execution:test-exec",
          },
          County: { stringValue: "test-county" },
          TransactionHash: { stringValue: transactionHash },
        },
      },
    ],
  });

  describe("SQS trigger mode (with task token)", () => {
    it("should emit IN_PROGRESS event at start", async () => {
      mockCheckTransactionStatus.mockResolvedValue([
        { status: "success", blockNumber: 12345 },
      ]);

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent("task-token-123", "0xabc123");

      await handler(event);

      expect(mockEmitWorkflowEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          status: "IN_PROGRESS",
          phase: "TransactionStatusCheck",
          step: "CheckTransactionStatus",
          taskToken: "task-token-123",
        }),
      );
    });

    it("should succeed when transaction has succeeded with block number", async () => {
      mockCheckTransactionStatus.mockResolvedValue([
        { status: "success", blockNumber: 12345 },
      ]);

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent("task-token-success", "0xabc123");

      await handler(event);

      // Verify checkTransactionStatus was called once (single check, no retries)
      expect(mockCheckTransactionStatus).toHaveBeenCalledTimes(1);
      expect(mockCheckTransactionStatus).toHaveBeenCalledWith({
        transactionHashes: "0xabc123",
        rpcUrl: "https://rpc.example.com",
      });

      // Verify task success was sent
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-success",
        }),
      );
    });

    it("should fail with error code 60004 when transaction is pending", async () => {
      mockCheckTransactionStatus.mockResolvedValue([{ status: "pending" }]);

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent("task-token-pending", "0xabc123");

      await handler(event);

      // Verify single check (no retries in Lambda - Step Function handles retries)
      expect(mockCheckTransactionStatus).toHaveBeenCalledTimes(1);

      // Verify task failure was sent with error code 60004
      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60004",
        expect.objectContaining({
          error: expect.stringContaining("TRANSACTION_PENDING:"),
          transactionHash: "0xabc123",
        }),
      );

      // Verify FAILED event was emitted
      expect(mockEmitWorkflowEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          status: "FAILED",
          phase: "TransactionStatusCheck",
          step: "CheckTransactionStatus",
        }),
      );
    });

    it("should fail with error code 60005 when transaction is not found", async () => {
      mockCheckTransactionStatus.mockResolvedValue([]); // Empty result = not found

      const transactionItems = [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ];

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent(
        "task-token-not-found",
        "0xabc123",
        transactionItems,
      );

      await handler(event);

      // Verify single check (no retries)
      expect(mockCheckTransactionStatus).toHaveBeenCalledTimes(1);

      // Verify task failure was sent with error code 60005
      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60005",
        expect.objectContaining({
          error: expect.stringContaining("TRANSACTION_NOT_FOUND:"),
          transactionHash: "0xabc123",
        }),
      );

      // Verify NO SQS resubmission (Step Function handles resubmission)
      expect(sqsMock.calls()).toHaveLength(0);
    });

    it("should fail with error code 60003 when transaction failed on chain", async () => {
      mockCheckTransactionStatus.mockResolvedValue([
        { status: "failed", blockNumber: 12345 },
      ]);

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent("task-token-failed", "0xabc123");

      await handler(event);

      // Verify single check (no retries)
      expect(mockCheckTransactionStatus).toHaveBeenCalledTimes(1);

      // Verify task failure was sent with error code 60003
      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60003",
        expect.objectContaining({
          error: expect.stringContaining("TRANSACTION_FAILED:"),
          transactionHash: "0xabc123",
        }),
      );
    });

    it("should emit SUCCEEDED event on successful check", async () => {
      mockCheckTransactionStatus.mockResolvedValue([
        { status: "success", blockNumber: 12345 },
      ]);

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent("task-token-succeeded", "0xabc123");

      await handler(event);

      // Should have 2 EventBridge calls: IN_PROGRESS and SUCCEEDED
      expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(2);
      expect(mockEmitWorkflowEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          status: "SUCCEEDED",
          phase: "TransactionStatusCheck",
          step: "CheckTransactionStatus",
        }),
      );
    });

    it("should fail with error code 60006 when checkTransactionStatus throws an error", async () => {
      mockCheckTransactionStatus.mockRejectedValue(new Error("RPC error"));

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent("task-token-error", "0xabc123");

      await handler(event);

      // Verify single check (no retries)
      expect(mockCheckTransactionStatus).toHaveBeenCalledTimes(1);

      // Verify task failure was sent with error code 60006 (GENERAL_ERROR)
      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60006",
        expect.objectContaining({
          error: expect.stringContaining("GENERAL_ERROR:"),
        }),
      );
    });

    it("should include TRANSACTION_NOT_FOUND prefix in error message when not found", async () => {
      mockCheckTransactionStatus.mockResolvedValue([]);

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent("task-token-not-found-prefix", "0xabc123");

      await handler(event);

      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60005",
        expect.objectContaining({
          error: expect.stringContaining("TRANSACTION_NOT_FOUND:"),
          transactionHash: "0xabc123",
        }),
      );
    });

    it("should include TRANSACTION_PENDING prefix in error message when pending", async () => {
      mockCheckTransactionStatus.mockResolvedValue([{ status: "pending" }]);

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent("task-token-pending-prefix", "0xabc123");

      await handler(event);

      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60004",
        expect.objectContaining({
          error: expect.stringContaining("TRANSACTION_PENDING:"),
          transactionHash: "0xabc123",
        }),
      );
    });

    it("should include TRANSACTION_FAILED prefix in error message when failed on chain", async () => {
      mockCheckTransactionStatus.mockResolvedValue([
        { status: "failed", blockNumber: 12345 },
      ]);

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent("task-token-failed-prefix", "0xabc123");

      await handler(event);

      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60003",
        expect.objectContaining({
          error: expect.stringContaining("TRANSACTION_FAILED:"),
          transactionHash: "0xabc123",
        }),
      );
    });
  });

  describe("Configuration validation", () => {
    it("should fail with error code 60006 when RPC URL is missing", async () => {
      delete process.env.ELEPHANT_RPC_URL;

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent("task-token-no-rpc", "0xabc123");

      await handler(event);

      // Verify task failure was sent
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-no-rpc",
        }),
      );

      // Verify error code 60006 for config error
      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60006",
        expect.objectContaining({
          error: expect.stringContaining("GENERAL_ERROR:"),
        }),
      );
    });
  });

  describe("Error handling", () => {
    it("should fail with error code 60006 when checkTransactionStatus throws an error", async () => {
      mockCheckTransactionStatus.mockRejectedValue(new Error("Network error"));

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent("task-token-network-error", "0xabc123");

      await handler(event);

      // Verify single check (no retries)
      expect(mockCheckTransactionStatus).toHaveBeenCalledTimes(1);

      // Verify task failure was sent
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-network-error",
        }),
      );

      // Verify GENERAL_ERROR prefix in error message
      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60006",
        expect.objectContaining({
          error: expect.stringContaining("GENERAL_ERROR:"),
        }),
      );
    });

    it("should handle missing SQS Records", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = {
        Records: [],
      };

      await expect(handler(event)).rejects.toThrow("Missing SQS Records");
    });

    it("should fail with error code 60006 when missing transaction hash", async () => {
      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = {
        Records: [
          {
            body: JSON.stringify({}), // Missing transactionHash
            messageAttributes: {
              TaskToken: { stringValue: "task-token-no-hash" },
              ExecutionArn: {
                stringValue:
                  "arn:aws:states:us-east-1:123456789012:execution:test-exec",
              },
              County: { stringValue: "test-county" },
            },
          },
        ],
      };

      await handler(event);

      // Verify task failure was sent
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-no-hash",
        }),
      );

      // Verify error code 60006 for missing transaction hash
      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60006",
        expect.objectContaining({
          error: expect.stringContaining("Transaction hash is required"),
        }),
      );
    });
  });

  describe("Error codes for Step Function routing", () => {
    it("should use error code 60003 for on-chain failures (reverted)", async () => {
      mockCheckTransactionStatus.mockResolvedValue([
        { status: "failed", blockNumber: 12345 },
      ]);

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent("task-token-reverted", "0xabc123");

      await handler(event);

      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60003",
        expect.anything(),
      );
    });

    it("should use error code 60004 for pending transactions", async () => {
      mockCheckTransactionStatus.mockResolvedValue([{ status: "pending" }]);

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent("task-token-pending-code", "0xabc123");

      await handler(event);

      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60004",
        expect.anything(),
      );
    });

    it("should use error code 60005 for not found transactions", async () => {
      mockCheckTransactionStatus.mockResolvedValue([]);

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent("task-token-notfound-code", "0xabc123");

      await handler(event);

      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60005",
        expect.anything(),
      );
    });

    it("should use error code 60006 for general errors", async () => {
      mockCheckTransactionStatus.mockRejectedValue(
        new Error("Unexpected error"),
      );

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent("task-token-general-code", "0xabc123");

      await handler(event);

      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60006",
        expect.anything(),
      );
    });

    it("should handle 'not_found' status from CLI", async () => {
      mockCheckTransactionStatus.mockResolvedValue([{ status: "not_found" }]);

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent("task-token-status-not-found", "0xabc123");

      await handler(event);

      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60005",
        expect.objectContaining({
          error: expect.stringContaining("TRANSACTION_NOT_FOUND:"),
        }),
      );
    });

    it("should handle unknown status as not_found", async () => {
      mockCheckTransactionStatus.mockResolvedValue([
        { status: "unknown_status" },
      ]);

      const { handler } =
        await import("../../../../workflow/lambdas/transaction-status-checker/index.mjs");

      const event = createSqsEvent("task-token-status-unknown", "0xabc123");

      await handler(event);

      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60005",
        expect.objectContaining({
          error: expect.stringContaining("TRANSACTION_NOT_FOUND:"),
        }),
      );
    });
  });
});
