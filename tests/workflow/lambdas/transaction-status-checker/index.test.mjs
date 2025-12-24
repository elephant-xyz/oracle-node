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

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-123", "0xabc123");

      await handler(event);

      // Verify IN_PROGRESS and SUCCEEDED events were emitted
      // Verify IN_PROGRESS event was emitted via shared layer
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

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-success", "0xabc123");

      await handler(event);

      // Verify checkTransactionStatus was called once (no retries)
      expect(mockCheckTransactionStatus).toHaveBeenCalledTimes(1);
      expect(mockCheckTransactionStatus).toHaveBeenCalledWith({
        transactionHashes: "0xabc123",
        rpcUrl: "https://rpc.example.com",
      });

      // Verify task success was sent
      // Verify task success was sent via shared layer
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-success",
        }),
      );
    });

    it("should fail immediately when transaction is pending (no retries)", async () => {
      mockCheckTransactionStatus.mockResolvedValue([{ status: "pending" }]);

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-pending", "0xabc123");

      await handler(event);

      // Verify checkTransactionStatus was called only once (no retries)
      expect(mockCheckTransactionStatus).toHaveBeenCalledTimes(1);

      // Verify task failure was sent with TRANSACTION_PENDING prefix
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-pending",
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

    it("should fail immediately when transaction is dropped (no resubmission)", async () => {
      mockCheckTransactionStatus.mockResolvedValue([]); // Empty result = dropped

      const transactionItems = [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ];

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent(
        "task-token-dropped",
        "0xabc123",
        transactionItems,
      );

      await handler(event);

      // Verify checkTransactionStatus was called only once (no retries)
      expect(mockCheckTransactionStatus).toHaveBeenCalledTimes(1);

      // Verify task failure was sent via shared layer (transaction dropped)
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-dropped",
        }),
      );

      // Verify NO SQS resubmission (removed resubmission logic)
      expect(sqsMock.calls()).toHaveLength(0);
    });

    it("should handle failed transaction status", async () => {
      mockCheckTransactionStatus.mockResolvedValue([
        { status: "failed", error: "Transaction reverted" },
      ]);

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-failed", "0xabc123");

      await handler(event);

      // Verify checkTransactionStatus was called only once (no retries)
      expect(mockCheckTransactionStatus).toHaveBeenCalledTimes(1);

      // Verify task failure was sent via shared layer
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-failed",
        }),
      );
    });

    it("should emit SUCCEEDED event on successful check", async () => {
      mockCheckTransactionStatus.mockResolvedValue([
        { status: "success", blockNumber: 12345 },
      ]);

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-succeeded", "0xabc123");

      await handler(event);

      // Should have 2 EventBridge calls: IN_PROGRESS and SUCCEEDED
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

    it("should fail immediately when checkTransactionStatus throws an error", async () => {
      mockCheckTransactionStatus.mockRejectedValue(new Error("RPC error"));

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-error", "0xabc123");

      await handler(event);

      // Verify checkTransactionStatus was called only once (no retries)
      expect(mockCheckTransactionStatus).toHaveBeenCalledTimes(1);

      // Verify task failure was sent via shared layer
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-error",
        }),
      );
    });

    it("should emit FAILED event when transaction is dropped", async () => {
      mockCheckTransactionStatus.mockResolvedValue([]); // Empty result = dropped

      const transactionItems = [
        { dataGroupLabel: "County", dataGroupCid: "bafkrei123" },
      ];

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent(
        "task-token-dropped-event",
        "0xabc123",
        transactionItems,
      );

      await handler(event);

      // Should have 2 EventBridge calls: IN_PROGRESS and FAILED
      expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(2);
      expect(mockEmitWorkflowEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          status: "FAILED",
          phase: "TransactionStatusCheck",
          step: "CheckTransactionStatus",
          errors: expect.arrayContaining([
            expect.objectContaining({
              code: "60003",
            }),
          ]),
        }),
      );
    });

    it("should include TRANSACTION_DROPPED prefix in error message when dropped", async () => {
      mockCheckTransactionStatus.mockResolvedValue([]); // Empty result = dropped

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-dropped-prefix", "0xabc123");

      await handler(event);

      // Verify the error includes TRANSACTION_DROPPED prefix
      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60003",
        expect.objectContaining({
          error: expect.stringContaining("TRANSACTION_DROPPED:"),
          transactionHash: "0xabc123",
        }),
      );
    });

    it("should include TRANSACTION_PENDING prefix in error message when pending", async () => {
      mockCheckTransactionStatus.mockResolvedValue([{ status: "pending" }]);

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-pending-prefix", "0xabc123");

      await handler(event);

      // Verify the error includes TRANSACTION_PENDING prefix
      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60003",
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

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-failed-prefix", "0xabc123");

      await handler(event);

      // Verify the error includes TRANSACTION_FAILED prefix
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
    it("should fail when RPC URL is missing", async () => {
      delete process.env.ELEPHANT_RPC_URL;

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-no-rpc", "0xabc123");

      await handler(event);

      // Verify task failure was sent via shared layer
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-no-rpc",
        }),
      );

      // Verify error includes GENERAL_ERROR prefix
      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60003",
        expect.objectContaining({
          error: expect.stringContaining("GENERAL_ERROR:"),
        }),
      );
    });

    it("should fail immediately when transaction is dropped (no resubmit queue needed)", async () => {
      mockCheckTransactionStatus.mockResolvedValue([]); // Dropped transaction

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-no-queue", "0xabc123");

      await handler(event);

      // Verify checkTransactionStatus was called only once (no retries)
      expect(mockCheckTransactionStatus).toHaveBeenCalledTimes(1);

      // Should NOT have called SQS (no resubmission logic)
      expect(sqsMock.calls()).toHaveLength(0);

      // Verify task failure was sent
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-no-queue",
        }),
      );
    });
  });

  describe("Error handling", () => {
    it("should fail immediately when checkTransactionStatus throws an error", async () => {
      mockCheckTransactionStatus.mockRejectedValue(new Error("Network error"));

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-network-error", "0xabc123");

      await handler(event);

      // Verify checkTransactionStatus was called only once (no retries)
      expect(mockCheckTransactionStatus).toHaveBeenCalledTimes(1);

      // Verify task failure was sent
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-network-error",
        }),
      );

      // Verify GENERAL_ERROR prefix in error message
      expect(mockCreateWorkflowError).toHaveBeenCalledWith(
        "60003",
        expect.objectContaining({
          error: expect.stringContaining("GENERAL_ERROR:"),
        }),
      );
    });

    it("should handle missing SQS Records", async () => {
      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = {
        Records: [],
      };

      await expect(handler(event)).rejects.toThrow("Missing SQS Records");
    });

    it("should handle missing transaction hash in message body", async () => {
      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

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

      // Verify task failure was sent via shared layer
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-no-hash",
        }),
      );
    });
  });
});