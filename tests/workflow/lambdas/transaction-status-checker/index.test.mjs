import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import { SFNClient } from "@aws-sdk/client-sfn";
import { EventBridgeClient } from "@aws-sdk/client-eventbridge";
import { SQSClient } from "@aws-sdk/client-sqs";

const sfnMock = mockClient(SFNClient);
const eventBridgeMock = mockClient(EventBridgeClient);
const sqsMock = mockClient(SQSClient);

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

    process.env = {
      ...originalEnv,
      ELEPHANT_RPC_URL: "https://rpc.example.com",
      TRANSACTION_STATUS_WAIT_MINUTES: "5",
      TRANSACTION_STATUS_MAX_RETRIES: "2",
      RESUBMIT_QUEUE_URL:
        "https://sqs.us-east-1.amazonaws.com/123456789012/transactions-queue",
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
      expect(eventBridgeMock.calls()).toHaveLength(2);
      const putEventsCall = eventBridgeMock.calls()[0];
      expect(putEventsCall.args[0].input.Entries[0].Source).toBe(
        "elephant.workflow",
      );
      expect(putEventsCall.args[0].input.Entries[0].DetailType).toBe(
        "WorkflowEvent",
      );
      const detail = JSON.parse(putEventsCall.args[0].input.Entries[0].Detail);
      expect(detail.status).toBe("IN_PROGRESS");
      expect(detail.phase).toBe("Submit");
      expect(detail.step).toBe("CheckTransactionStatus");
      expect(detail.taskToken).toBe("task-token-123");
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

      // Verify checkTransactionStatus was called
      expect(mockCheckTransactionStatus).toHaveBeenCalledWith({
        transactionHashes: "0xabc123",
        rpcUrl: "https://rpc.example.com",
      });

      // Verify task success was sent
      expect(sfnMock.calls()).toHaveLength(1);
      const sendTaskSuccessCall = sfnMock.calls()[0];
      expect(sendTaskSuccessCall.args[0].input.taskToken).toBe(
        "task-token-success",
      );
    });

    it("should wait and retry when transaction is pending (no block number)", async () => {
      vi.useFakeTimers();

      // First call: pending (no block number)
      // Second call: succeeded with block number
      mockCheckTransactionStatus
        .mockResolvedValueOnce([{ status: "pending" }])
        .mockResolvedValueOnce([{ status: "success", blockNumber: 12345 }]);

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-retry", "0xabc123");

      const handlerPromise = handler(event);

      // Fast-forward time to trigger retry (5 minutes)
      await vi.advanceTimersByTimeAsync(5 * 60 * 1000);

      await handlerPromise;

      // Verify checkTransactionStatus was called twice
      expect(mockCheckTransactionStatus).toHaveBeenCalledTimes(2);

      // Verify task success was sent after retry
      expect(sfnMock.calls()).toHaveLength(1);
      const sendTaskSuccessCall = sfnMock.calls()[0];
      expect(sendTaskSuccessCall.args[0].input.taskToken).toBe(
        "task-token-retry",
      );

      vi.useRealTimers();
    });

    it("should fail after max retries when transaction remains pending", async () => {
      vi.useFakeTimers();

      // All calls return pending
      mockCheckTransactionStatus.mockResolvedValue([{ status: "pending" }]);

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-max-retries", "0xabc123");

      const handlerPromise = handler(event);

      // Fast-forward time to trigger all retries (2 retries = 2 * 5 minutes = 10 minutes)
      await vi.advanceTimersByTimeAsync(10 * 60 * 1000);

      await handlerPromise;

      // Verify checkTransactionStatus was called maxRetries times (loop runs maxRetries times)
      expect(mockCheckTransactionStatus).toHaveBeenCalledTimes(2); // maxRetries = 2

      // Verify task failure was sent
      const sendTaskFailureCalls = sfnMock
        .calls()
        .filter(
          (call) => call.args[0].input.taskToken === "task-token-max-retries",
        );
      expect(sendTaskFailureCalls.length).toBeGreaterThan(0);

      vi.useRealTimers();
    });

    it("should resubmit transaction when it is dropped (empty/null result)", async () => {
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

      // Verify task failure was sent (transaction dropped)
      const sendTaskFailureCalls = sfnMock
        .calls()
        .filter(
          (call) => call.args[0].input.taskToken === "task-token-dropped",
        );
      expect(sendTaskFailureCalls.length).toBeGreaterThan(0);

      // Verify transaction was resubmitted to SQS
      expect(sqsMock.calls()).toHaveLength(1);
      const sendMessageCall = sqsMock.calls()[0];
      expect(sendMessageCall.args[0].input.QueueUrl).toBe(
        "https://sqs.us-east-1.amazonaws.com/123456789012/transactions-queue",
      );
      const messageBody = JSON.parse(sendMessageCall.args[0].input.MessageBody);
      expect(messageBody).toEqual(transactionItems);
    });

    it("should handle failed transaction status", async () => {
      vi.useFakeTimers();
      mockCheckTransactionStatus.mockResolvedValue([
        { status: "failed", error: "Transaction reverted" },
      ]);

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-failed", "0xabc123");

      const handlerPromise = handler(event);
      
      // When transaction status is "failed", it throws immediately, but the catch block
      // may retry once if retries < maxRetries - 1. With maxRetries=2, retries=0, it will retry once.
      // Each retry waits 5 minutes, so 1 * 5 * 60 * 1000 = 300000ms
      await vi.advanceTimersByTimeAsync(300000);
      
      await handlerPromise;
      
      vi.useRealTimers();

      // Verify task failure was sent
      const sendTaskFailureCalls = sfnMock
        .calls()
        .filter((call) => call.args[0].input.taskToken === "task-token-failed");
      expect(sendTaskFailureCalls.length).toBeGreaterThan(0);
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
      expect(eventBridgeMock.calls()).toHaveLength(2);
      const succeededCall = eventBridgeMock.calls()[1];
      const detail = JSON.parse(succeededCall.args[0].input.Entries[0].Detail);
      expect(detail.status).toBe("SUCCEEDED");
      expect(detail.phase).toBe("Submit");
      expect(detail.step).toBe("CheckTransactionStatus");
    });

    it("should emit FAILED event on failure", async () => {
      vi.useFakeTimers();
      // Mock checkTransactionStatus to always throw (will retry maxRetries times)
      mockCheckTransactionStatus.mockRejectedValue(new Error("RPC error"));

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-failed-event", "0xabc123");

      const handlerPromise = handler(event);
      
      // Advance time for retries: maxRetries=2, so 1 retry before final failure
      // Each retry waits 5 minutes, so 1 * 5 * 60 * 1000 = 300000ms
      await vi.advanceTimersByTimeAsync(300000);
      
      await handlerPromise;
      
      vi.useRealTimers();

      // Should have 2 EventBridge calls: IN_PROGRESS and FAILED
      expect(eventBridgeMock.calls()).toHaveLength(2);
      const failedCall = eventBridgeMock.calls()[1];
      const detail = JSON.parse(failedCall.args[0].input.Entries[0].Detail);
      expect(detail.status).toBe("FAILED");
      expect(detail.errors).toBeDefined();
      expect(detail.errors.length).toBeGreaterThan(0);
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
      expect(eventBridgeMock.calls()).toHaveLength(2);
      const failedCall = eventBridgeMock.calls()[1];
      const detail = JSON.parse(failedCall.args[0].input.Entries[0].Detail);
      expect(detail.status).toBe("FAILED");
      expect(detail.errors).toBeDefined();
      expect(detail.errors.length).toBeGreaterThan(0);
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

      // Verify task failure was sent
      const sendTaskFailureCalls = sfnMock
        .calls()
        .filter((call) => call.args[0].input.taskToken === "task-token-no-rpc");
      expect(sendTaskFailureCalls.length).toBeGreaterThan(0);
    });

    it("should fail when RESUBMIT_QUEUE_URL is missing for resubmission", async () => {
      vi.useFakeTimers();
      delete process.env.RESUBMIT_QUEUE_URL;

      mockCheckTransactionStatus.mockResolvedValue([]); // Dropped transaction

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-no-queue", "0xabc123");

      const handlerPromise = handler(event);
      
      // When transaction status is "failed", it throws immediately, but the catch block
      // may retry once if retries < maxRetries - 1. With maxRetries=2, retries=0, it will retry once.
      // Each retry waits 5 minutes, so 1 * 5 * 60 * 1000 = 300000ms
      await vi.advanceTimersByTimeAsync(300000);
      
      await handlerPromise;
      
      vi.useRealTimers();

      // Should still fail (transaction dropped) but without resubmission
      const sendTaskFailureCalls = sfnMock
        .calls()
        .filter(
          (call) => call.args[0].input.taskToken === "task-token-no-queue",
        );
      expect(sendTaskFailureCalls.length).toBeGreaterThan(0);

      // Should NOT have called SQS (no queue URL)
      expect(sqsMock.calls()).toHaveLength(0);
    });

    it("should use default wait minutes when not configured", async () => {
      // Reset env but keep required ones
      const originalWaitMinutes = process.env.TRANSACTION_STATUS_WAIT_MINUTES;
      delete process.env.TRANSACTION_STATUS_WAIT_MINUTES;

      // Ensure other required env vars are set (handler uses RESUBMIT_QUEUE_URL, not TRANSACTIONS_SQS_QUEUE_URL)
      process.env.ELEPHANT_RPC_URL =
        process.env.ELEPHANT_RPC_URL || "https://rpc.example.com";
      process.env.RESUBMIT_QUEUE_URL =
        process.env.RESUBMIT_QUEUE_URL ||
        "https://sqs.us-east-1.amazonaws.com/123456789012/transactions-queue";

      mockCheckTransactionStatus.mockResolvedValue([
        { status: "success", blockNumber: 12345 },
      ]);

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-default-wait", "0xabc123");

      await handler(event);

      // Restore env
      if (originalWaitMinutes) {
        process.env.TRANSACTION_STATUS_WAIT_MINUTES = originalWaitMinutes;
      }

      // Should still succeed
      expect(mockCheckTransactionStatus).toHaveBeenCalled();
      expect(sfnMock.calls()).toHaveLength(1);
    });

    it("should use default max retries when not configured", async () => {
      // Reset env but keep required ones
      const originalMaxRetries = process.env.TRANSACTION_STATUS_MAX_RETRIES;
      delete process.env.TRANSACTION_STATUS_MAX_RETRIES;

      // Ensure other required env vars are set (handler uses RESUBMIT_QUEUE_URL, not TRANSACTIONS_SQS_QUEUE_URL)
      process.env.ELEPHANT_RPC_URL =
        process.env.ELEPHANT_RPC_URL || "https://rpc.example.com";
      process.env.RESUBMIT_QUEUE_URL =
        process.env.RESUBMIT_QUEUE_URL ||
        "https://sqs.us-east-1.amazonaws.com/123456789012/transactions-queue";

      mockCheckTransactionStatus.mockResolvedValue([
        { status: "success", blockNumber: 12345 },
      ]);

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-default-retries", "0xabc123");

      await handler(event);

      // Restore env
      if (originalMaxRetries) {
        process.env.TRANSACTION_STATUS_MAX_RETRIES = originalMaxRetries;
      }

      // Should still succeed
      expect(mockCheckTransactionStatus).toHaveBeenCalled();
      expect(sfnMock.calls()).toHaveLength(1);
    });
  });

  describe("Error handling", () => {
    it("should handle checkTransactionStatus throwing an error", async () => {
      vi.useFakeTimers();
      // Mock checkTransactionStatus to always throw (will retry maxRetries times)
      mockCheckTransactionStatus.mockRejectedValue(new Error("Network error"));

      const { handler } = await import(
        "../../../../workflow/lambdas/transaction-status-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-error", "0xabc123");

      const handlerPromise = handler(event);
      
      // Advance time for retries: maxRetries=2, so 1 retry before final failure
      // Each retry waits 5 minutes, so 1 * 5 * 60 * 1000 = 300000ms
      await vi.advanceTimersByTimeAsync(300000);
      
      await handlerPromise;
      
      vi.useRealTimers();

      // Verify task failure was sent
      const sendTaskFailureCalls = sfnMock
        .calls()
        .filter((call) => call.args[0].input.taskToken === "task-token-error");
      expect(sendTaskFailureCalls.length).toBeGreaterThan(0);
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

      // Verify task failure was sent
      const sendTaskFailureCalls = sfnMock
        .calls()
        .filter(
          (call) => call.args[0].input.taskToken === "task-token-no-hash",
        );
      expect(sendTaskFailureCalls.length).toBeGreaterThan(0);
    });
  });
});
