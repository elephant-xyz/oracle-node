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

const sfnMock = mockClient(SFNClient);
const eventBridgeMock = mockClient(EventBridgeClient);

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
const mockCheckGasPrice = vi.fn();
vi.mock("@elephant-xyz/cli/lib", () => ({
  checkGasPrice: mockCheckGasPrice,
}));

describe("gas-price-checker handler", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.clearAllMocks();
    sfnMock.reset();
    eventBridgeMock.reset();
    mockExecuteWithTaskToken.mockClear();
    mockEmitWorkflowEvent.mockClear();
    mockCreateWorkflowError.mockClear();
    mockCreateLogger.mockClear();

    process.env = {
      ...originalEnv,
      ELEPHANT_RPC_URL: "https://rpc.example.com",
      GAS_PRICE_MAX_GWEI: "25",
      GAS_PRICE_WAIT_MINUTES: "2",
      GAS_PRICE_MAX_RETRIES: "3",
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

  const createSqsEvent = (taskToken) => ({
    Records: [
      {
        body: JSON.stringify({}),
        messageAttributes: {
          TaskToken: { stringValue: taskToken },
          ExecutionArn: {
            stringValue:
              "arn:aws:states:us-east-1:123456789012:execution:test-exec",
          },
          County: { stringValue: "test-county" },
        },
      },
    ],
  });

  describe("SQS trigger mode (with task token)", () => {
    it("should emit IN_PROGRESS event at start", async () => {
      // Mock checkGasPrice to return acceptable gas price immediately
      // Handler expects: gasPriceInfo.eip1559?.maxFeePerGas || gasPriceInfo.legacy?.gasPrice
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "20000000000" }, // 20 Gwei in wei
        legacy: { gasPrice: "20000000000" },
      });

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-123");

      await handler(event);

      // Verify IN_PROGRESS and SUCCEEDED events were emitted
      // Verify IN_PROGRESS event was emitted via shared layer
      expect(mockEmitWorkflowEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          status: "IN_PROGRESS",
          phase: "GasPriceCheck",
          step: "CheckGasPrice",
          taskToken: "task-token-123",
        }),
      );
    });

    it("should succeed when gas price is below threshold", async () => {
      // Mock checkGasPrice to return acceptable gas price (20 Gwei = 20000000000 wei)
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "20000000000" },
        legacy: { gasPrice: "20000000000" },
      }); // Below 25 Gwei threshold

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-success");

      await handler(event);

      // Verify checkGasPrice was called
      expect(mockCheckGasPrice).toHaveBeenCalled();

      // Verify task success was sent
      // Verify task success was sent via shared layer
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-success",
        }),
      );
    });

    it("should succeed when gas price equals threshold", async () => {
      // Mock checkGasPrice to return gas price at threshold (25 Gwei = 25000000000 wei)
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "25000000000" },
        legacy: { gasPrice: "25000000000" },
      }); // Exactly at threshold

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-equal");

      await handler(event);

      // Verify task success was sent
      // Verify task success was sent via shared layer
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-equal",
        }),
      );
    });

    it("should wait and retry when gas price is above threshold", async () => {
      vi.useFakeTimers();

      // First call: gas price too high (30 Gwei = 30000000000 wei)
      // Second call: gas price acceptable (20 Gwei = 20000000000 wei)
      mockCheckGasPrice
        .mockResolvedValueOnce({
          eip1559: { maxFeePerGas: "30000000000" },
          legacy: { gasPrice: "30000000000" },
        }) // Above threshold
        .mockResolvedValueOnce({
          eip1559: { maxFeePerGas: "20000000000" },
          legacy: { gasPrice: "20000000000" },
        }); // Below threshold

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-retry");

      const handlerPromise = handler(event);

      // Fast-forward time to trigger retry
      await vi.advanceTimersByTimeAsync(2 * 60 * 1000); // 2 minutes

      await handlerPromise;

      // Verify checkGasPrice was called twice
      expect(mockCheckGasPrice).toHaveBeenCalledTimes(2);

      // Verify task success was sent after retry
      // Verify task success was sent via shared layer
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-retry",
        }),
      );

      vi.useRealTimers();
    });

    it("should retry indefinitely when gas price remains too high", async () => {
      vi.useFakeTimers();

      // All calls return gas price too high (30 Gwei = 30000000000 wei)
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "30000000000" },
        legacy: { gasPrice: "30000000000" },
      }); // Always above threshold

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-retry-indefinite");

      const handlerPromise = handler(event);

      // Fast-forward time to trigger several retries (3 retries = 3 * 2 minutes = 6 minutes)
      await vi.advanceTimersByTimeAsync(6 * 60 * 1000);

      // Handler should still be running (retries indefinitely)
      // Cancel the promise after verifying retries
      const timeout = setTimeout(() => {
        handlerPromise.catch(() => {});
      }, 100);

      // Verify checkGasPrice was called multiple times (retries indefinitely)
      expect(mockCheckGasPrice).toHaveBeenCalledTimes(4); // 1 initial + 3 retries

      clearTimeout(timeout);
      vi.useRealTimers();
    });

    it("should emit SUCCEEDED event on successful check", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "20000000000" },
        legacy: { gasPrice: "20000000000" },
      });

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-succeeded");

      await handler(event);

      // Should have 2 EventBridge calls: IN_PROGRESS and SUCCEEDED
      // Should have 2 EventBridge calls: IN_PROGRESS and SUCCEEDED
      expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(2);
      expect(mockEmitWorkflowEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          status: "SUCCEEDED",
          phase: "GasPriceCheck",
          step: "CheckGasPrice",
        }),
      );
    });

    it("should retry indefinitely when checkGasPrice throws an error", async () => {
      vi.useFakeTimers();
      // Mock checkGasPrice to always throw (will retry indefinitely)
      mockCheckGasPrice.mockRejectedValue(new Error("RPC error"));

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-error-retry");

      const handlerPromise = handler(event);

      // Advance time for several retries (3 retries = 3 * 2 minutes = 6 minutes)
      await vi.advanceTimersByTimeAsync(6 * 60 * 1000);

      // Handler should still be running (retries indefinitely)
      // Cancel the promise after verifying retries
      const timeout = setTimeout(() => {
        handlerPromise.catch(() => {});
      }, 100);

      // Verify checkGasPrice was called multiple times (retries indefinitely)
      expect(mockCheckGasPrice).toHaveBeenCalledTimes(4); // 1 initial + 3 retries

      clearTimeout(timeout);
      vi.useRealTimers();
    });
  });

  describe("Configuration validation", () => {
    it("should fail when RPC URL is missing", async () => {
      delete process.env.ELEPHANT_RPC_URL;

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-no-rpc");

      await handler(event);

      // Verify task failure was sent
      // Verify task failure was sent via shared layer
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-no-rpc",
        }),
      );
    });

    it("should fail when GAS_PRICE_MAX_GWEI is missing", async () => {
      delete process.env.GAS_PRICE_MAX_GWEI;

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-no-max");

      await handler(event);

      // Verify task failure was sent
      // Verify task failure was sent via shared layer
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
        expect.objectContaining({
          taskToken: "task-token-no-max",
        }),
      );
    });

    it("should use default wait minutes when not configured", async () => {
      delete process.env.GAS_PRICE_WAIT_MINUTES;

      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "20000000000" },
        legacy: { gasPrice: "20000000000" },
      });

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-default-wait");

      await handler(event);

      // Should still succeed
      expect(mockCheckGasPrice).toHaveBeenCalled();
      // Verify task success was sent via shared layer
      expect(mockExecuteWithTaskToken).toHaveBeenCalled();
    });

    it("should use default max retries when not configured", async () => {
      delete process.env.GAS_PRICE_MAX_RETRIES;

      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "20000000000" },
        legacy: { gasPrice: "20000000000" },
      });

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-default-retries");

      await handler(event);

      // Should still succeed
      expect(mockCheckGasPrice).toHaveBeenCalled();
      // Verify task success was sent via shared layer
      expect(mockExecuteWithTaskToken).toHaveBeenCalled();
    });
  });

  describe("Error handling", () => {
    it("should retry indefinitely when checkGasPrice throws an error", async () => {
      vi.useFakeTimers();
      // Mock checkGasPrice to always throw (will retry indefinitely)
      mockCheckGasPrice.mockRejectedValue(new Error("Network error"));

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-checker/index.mjs"
      );

      const event = createSqsEvent("task-token-error");

      const handlerPromise = handler(event);

      // Advance time for several retries (3 retries = 3 * 2 minutes = 6 minutes)
      await vi.advanceTimersByTimeAsync(6 * 60 * 1000);

      // Handler should still be running (retries indefinitely)
      // Cancel the promise after verifying retries
      const timeout = setTimeout(() => {
        handlerPromise.catch(() => {});
      }, 100);

      // Verify checkGasPrice was called multiple times (retries indefinitely)
      expect(mockCheckGasPrice).toHaveBeenCalledTimes(4); // 1 initial + 3 retries

      clearTimeout(timeout);
      vi.useRealTimers();
    });

    it("should handle missing SQS Records", async () => {
      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-checker/index.mjs"
      );

      const event = {
        Records: [],
      };

      await expect(handler(event)).rejects.toThrow("Missing SQS Records");
    });
  });

  it("should still call task token even if emitWorkflowEvent fails for IN_PROGRESS", async () => {
    // Mock emitWorkflowEvent to fail on IN_PROGRESS
    mockEmitWorkflowEvent.mockRejectedValueOnce(
      new Error("EventBridge failure"),
    );
    // But succeed on SUCCEEDED (if it gets there)
    mockEmitWorkflowEvent.mockResolvedValue(undefined);

    mockCheckGasPrice.mockResolvedValue({
      eip1559: { maxFeePerGas: "20000000000" }, // 20 Gwei in wei
      legacy: { gasPrice: "20000000000" },
    });
    mockExecuteWithTaskToken.mockResolvedValue(undefined);

    const { handler } = await import(
      "../../../../workflow/lambdas/gas-price-checker/index.mjs"
    );

    const event = createSqsEvent("task-token-eventbridge-fail");

    // Should not throw - EventBridge failure is caught and logged
    await handler(event);

    // Verify emitWorkflowEvent was attempted
    expect(mockEmitWorkflowEvent).toHaveBeenCalled();

    // CRITICAL: Verify task token is still called despite EventBridge failure
    expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
      expect.objectContaining({
        taskToken: "task-token-eventbridge-fail",
      }),
    );
  });

  it("should still call task token even if emitWorkflowEvent fails for SUCCEEDED", async () => {
    // Mock emitWorkflowEvent to succeed on IN_PROGRESS but fail on SUCCEEDED
    mockEmitWorkflowEvent
      .mockResolvedValueOnce(undefined) // IN_PROGRESS succeeds
      .mockRejectedValueOnce(new Error("EventBridge failure")); // SUCCEEDED fails

    mockCheckGasPrice.mockResolvedValue({
      eip1559: { maxFeePerGas: "20000000000" }, // 20 Gwei in wei
      legacy: { gasPrice: "20000000000" },
    });
    mockExecuteWithTaskToken.mockResolvedValue(undefined);

    const { handler } = await import(
      "../../../../workflow/lambdas/gas-price-checker/index.mjs"
    );

    const event = createSqsEvent("task-token-succeeded-eventbridge-fail");

    // Should not throw - EventBridge failure is caught and logged
    await handler(event);

    // Verify emitWorkflowEvent was called twice (IN_PROGRESS and SUCCEEDED)
    expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(2);

    // CRITICAL: Verify task token is still called despite EventBridge failure on SUCCEEDED
    expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
      expect.objectContaining({
        taskToken: "task-token-succeeded-eventbridge-fail",
      }),
    );
  });

  it("should still call task token on error even if emitWorkflowEvent fails", async () => {
    vi.useFakeTimers();

    // Mock emitWorkflowEvent to fail only once (for IN_PROGRESS)
    mockEmitWorkflowEvent.mockRejectedValueOnce(
      new Error("EventBridge failure"),
    );
    // But succeed on FAILED event (if it gets there)
    mockEmitWorkflowEvent.mockResolvedValue(undefined);

    // Mock checkGasPrice to fail immediately (configuration error so it throws)
    mockCheckGasPrice.mockRejectedValue(new Error("RPC URL is required"));
    mockExecuteWithTaskToken.mockResolvedValue(undefined);

    const { handler } = await import(
      "../../../../workflow/lambdas/gas-price-checker/index.mjs"
    );

    const event = createSqsEvent("task-token-error-eventbridge-fail");

    // Should not throw - EventBridge failure is caught, and RPC error triggers immediate failure
    const handlerPromise = handler(event);

    // Advance timers to allow any async operations
    await vi.advanceTimersByTimeAsync(100);
    await handlerPromise;

    // Verify emitWorkflowEvent was attempted
    expect(mockEmitWorkflowEvent).toHaveBeenCalled();

    // CRITICAL: Verify task token is still called with error despite EventBridge failure
    expect(mockExecuteWithTaskToken).toHaveBeenCalledWith(
      expect.objectContaining({
        taskToken: "task-token-error-eventbridge-fail",
      }),
    );

    vi.useRealTimers();
  }, 10000);
});
