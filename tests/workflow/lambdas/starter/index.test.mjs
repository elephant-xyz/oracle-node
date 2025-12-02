import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import {
  SFNClient,
  StartExecutionCommand,
  ListExecutionsCommand,
  DescribeExecutionCommand,
} from "@aws-sdk/client-sfn";

const sfnMock = mockClient(SFNClient);

describe("starter lambda", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.resetModules();
    sfnMock.reset();
    process.env = {
      ...originalEnv,
      STATE_MACHINE_ARN: "arn:aws:states:us-east-1:123456789:stateMachine:test",
      MAX_CONCURRENT_EXECUTIONS: "100",
    };
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    // Mock ListExecutionsCommand to return empty array by default (no running executions)
    sfnMock.on(ListExecutionsCommand).resolves({
      executions: [],
    });
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  it("should throw error when STATE_MACHINE_ARN is missing", async () => {
    delete process.env.STATE_MACHINE_ARN;

    // Dynamic import to get fresh module with updated env
    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    await expect(handler(event)).rejects.toThrow(
      "STATE_MACHINE_ARN is required",
    );
  });

  it("should throw error when event has no records", async () => {
    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    await expect(handler({ Records: [] })).rejects.toThrow(
      "Expect exactly one SQS record per invocation",
    );
  });

  it("should throw error when event has multiple records", async () => {
    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    const event = {
      Records: [{ body: "{}" }, { body: "{}" }],
    };

    await expect(handler(event)).rejects.toThrow(
      "Expect exactly one SQS record per invocation",
    );
  });

  it("should throw error when record body is missing", async () => {
    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    const event = {
      Records: [{}],
    };

    await expect(handler(event)).rejects.toThrow("Missing SQS record body");
  });

  it("should successfully start workflow and return ok status", async () => {
    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    // Mock DescribeExecutionCommand to return SUCCEEDED
    sfnMock.on(DescribeExecutionCommand).resolves({
      status: "SUCCEEDED",
    });

    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    const inputData = { county: "test-county", data: "test-data" };
    const event = {
      Records: [{ body: JSON.stringify(inputData) }],
    };

    const result = await handler(event);

    expect(result.status).toBe("ok");
    expect(result.executionArn).toBe(
      "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    );
    expect(result.workflowStatus).toBe("SUCCEEDED");

    // Verify AWS SDK was called correctly using custom matchers
    expect(sfnMock).toHaveReceivedCommandWith(StartExecutionCommand, {
      stateMachineArn: "arn:aws:states:us-east-1:123456789:stateMachine:test",
      input: JSON.stringify({ message: inputData }),
    });
  });

  it("should throw error when concurrency limit is reached", async () => {
    // Mock ListExecutionsCommand to return 100 executions older than 5 minutes
    const fiveMinutesAgo = new Date(Date.now() - 6 * 60 * 1000);
    const mockExecutions = Array.from({ length: 100 }, (_, i) => ({
      executionArn: `arn:aws:states:us-east-1:123456789:execution:test:exec${i}`,
      startDate: fiveMinutesAgo,
    }));

    sfnMock.on(ListExecutionsCommand).resolves({
      executions: mockExecutions,
    });

    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    await expect(handler(event)).rejects.toThrow(
      "Step Function concurrency limit reached: 100/100 executions running",
    );
  });

  it("should throw error when StartExecutionCommand fails", async () => {
    sfnMock
      .on(StartExecutionCommand)
      .rejects(new Error("Failed to start execution"));

    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    await expect(handler(event)).rejects.toThrow("Failed to start execution");
  });

  it("should throw error when execution fails", async () => {
    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    sfnMock.on(DescribeExecutionCommand).resolves({
      status: "FAILED",
      cause: "Task failed",
      error: "ValidationError",
    });

    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    await expect(handler(event)).rejects.toThrow(
      "Step function execution FAILED",
    );
  });

  it("should throw error when execution times out", async () => {
    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    sfnMock.on(DescribeExecutionCommand).resolves({
      status: "TIMED_OUT",
      cause: "Execution timed out",
    });

    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    await expect(handler(event)).rejects.toThrow(
      "Step function execution TIMED_OUT",
    );
  });

  it("should throw error when execution is aborted", async () => {
    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    sfnMock.on(DescribeExecutionCommand).resolves({
      status: "ABORTED",
    });

    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    await expect(handler(event)).rejects.toThrow(
      "Step function execution ABORTED",
    );
  });

  it("should poll while execution is running and succeed", async () => {
    vi.useFakeTimers();

    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    // First call returns RUNNING, second call returns SUCCEEDED
    sfnMock
      .on(DescribeExecutionCommand)
      .resolvesOnce({ status: "RUNNING" })
      .resolvesOnce({ status: "SUCCEEDED" });

    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    const handlerPromise = handler(event);

    // Advance timers to allow polling
    await vi.advanceTimersByTimeAsync(3000);

    const result = await handlerPromise;

    expect(result.status).toBe("ok");
    expect(result.workflowStatus).toBe("SUCCEEDED");

    vi.useRealTimers();
  });

  it("should timeout when unknown execution status persists", async () => {
    vi.useFakeTimers();

    // Set a short timeout for testing
    process.env.AWS_LAMBDA_FUNCTION_TIMEOUT = "35"; // 35 - 30 buffer = 5 second max wait

    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    // Unknown status causes retry loop until timeout
    sfnMock.on(DescribeExecutionCommand).resolves({
      status: "UNKNOWN_STATUS",
    });

    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    // Attach rejection handler BEFORE starting, to prevent unhandled rejection warning
    let rejectedError = null;
    const handlerPromise = handler(event).catch((err) => {
      rejectedError = err;
    });

    // Advance time past the max wait (5 seconds = 5000ms)
    await vi.advanceTimersByTimeAsync(6000);

    // Wait for promise to complete
    await handlerPromise;

    // Should have rejected with timeout error
    expect(rejectedError).not.toBeNull();
    expect(rejectedError.message).toContain(
      "Step Function execution did not complete within 5 seconds",
    );

    vi.useRealTimers();
  });

  it("should retry on DescribeExecution errors and eventually succeed", async () => {
    vi.useFakeTimers();

    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    // First call throws error, second call succeeds
    sfnMock
      .on(DescribeExecutionCommand)
      .rejectsOnce(new Error("Temporary network error"))
      .resolvesOnce({ status: "SUCCEEDED" });

    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    const handlerPromise = handler(event);

    // Advance timers to allow retry
    await vi.advanceTimersByTimeAsync(3000);

    const result = await handlerPromise;

    expect(result.status).toBe("ok");
    expect(result.workflowStatus).toBe("SUCCEEDED");

    vi.useRealTimers();
  });

  it("should handle null/undefined event", async () => {
    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    await expect(handler(null)).rejects.toThrow(
      "Expect exactly one SQS record per invocation",
    );
  });

  it("should handle event without Records property", async () => {
    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    await expect(handler({})).rejects.toThrow(
      "Expect exactly one SQS record per invocation",
    );
  });

  it("should return MAX_RUNNING_EXECUTIONS when ListExecutions fails", async () => {
    sfnMock
      .on(ListExecutionsCommand)
      .rejects(new Error("Access Denied"));

    // Set max concurrent to 1000 (the default MAX_RUNNING_EXECUTIONS)
    process.env.MAX_CONCURRENT_EXECUTIONS = "1000";

    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    sfnMock.on(DescribeExecutionCommand).resolves({
      status: "SUCCEEDED",
    });

    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    // Should still succeed because default max concurrent (1000) equals MAX_RUNNING_EXECUTIONS
    await expect(handler(event)).rejects.toThrow("concurrency limit reached");
  });

  it("should only count executions older than 5 minutes", async () => {
    // Mock some executions - only 1 older than 5 minutes
    const fiveMinutesAgo = new Date(Date.now() - 6 * 60 * 1000);
    const now = new Date();
    const mockExecutions = [
      { executionArn: "old-execution", startDate: fiveMinutesAgo },
      { executionArn: "new-execution-1", startDate: now },
      { executionArn: "new-execution-2", startDate: now },
    ];

    sfnMock.on(ListExecutionsCommand).resolves({
      executions: mockExecutions,
    });

    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    sfnMock.on(DescribeExecutionCommand).resolves({
      status: "SUCCEEDED",
    });

    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    // Only 1 execution is older than 5 minutes, which is below the 100 limit
    const result = await handler(event);
    expect(result.status).toBe("ok");
  });

  it("should handle missing executionArn in response", async () => {
    sfnMock.on(StartExecutionCommand).resolves({
      // executionArn is missing
    });

    sfnMock.on(DescribeExecutionCommand).resolves({
      status: "SUCCEEDED",
    });

    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    const result = await handler(event);

    expect(result.status).toBe("ok");
    expect(result.executionArn).toBe("arn not found");
  });
});
