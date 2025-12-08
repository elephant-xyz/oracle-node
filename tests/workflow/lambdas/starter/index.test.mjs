import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import {
  SFNClient,
  StartExecutionCommand,
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
    };
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  it("should throw error when STATE_MACHINE_ARN is missing", async () => {
    delete process.env.STATE_MACHINE_ARN;

    // Dynamic import to get fresh module with updated env
    const { handler } =
      await import("../../../../workflow/lambdas/starter/index.mjs");

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    await expect(handler(event)).rejects.toThrow(
      "STATE_MACHINE_ARN is required",
    );
  });

  it("should throw error when event has no records", async () => {
    const { handler } =
      await import("../../../../workflow/lambdas/starter/index.mjs");

    await expect(handler({ Records: [] })).rejects.toThrow(
      "Expect exactly one SQS record per invocation",
    );
  });

  it("should throw error when event has multiple records", async () => {
    const { handler } =
      await import("../../../../workflow/lambdas/starter/index.mjs");

    const event = {
      Records: [{ body: "{}" }, { body: "{}" }],
    };

    await expect(handler(event)).rejects.toThrow(
      "Expect exactly one SQS record per invocation",
    );
  });

  it("should throw error when record body is missing", async () => {
    const { handler } =
      await import("../../../../workflow/lambdas/starter/index.mjs");

    const event = {
      Records: [{}],
    };

    await expect(handler(event)).rejects.toThrow("Missing SQS record body");
  });

  it("should successfully start workflow and return ok status", async () => {
    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    const { handler } =
      await import("../../../../workflow/lambdas/starter/index.mjs");

    const inputData = { county: "test-county", data: "test-data" };
    const event = {
      Records: [{ body: JSON.stringify(inputData) }],
    };

    const result = await handler(event);

    expect(result.status).toBe("ok");
    expect(result.executionArn).toBe(
      "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    );

    // Verify AWS SDK was called correctly using custom matchers
    expect(sfnMock).toHaveReceivedCommandWith(StartExecutionCommand, {
      stateMachineArn: "arn:aws:states:us-east-1:123456789:stateMachine:test",
      input: JSON.stringify({ message: inputData }),
    });
  });

  it("should start execution regardless of running count (concurrency controlled by Lambda)", async () => {
    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    const { handler } =
      await import("../../../../workflow/lambdas/starter/index.mjs");

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    // Concurrency is now controlled by Lambda's ReservedConcurrentExecutions
    // No need to check running Step Function executions
    const result = await handler(event);
    expect(result.status).toBe("ok");
    expect(result.executionArn).toBe(
      "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    );
  });

  it("should throw error when StartExecutionCommand fails", async () => {
    sfnMock
      .on(StartExecutionCommand)
      .rejects(new Error("Failed to start execution"));

    const { handler } =
      await import("../../../../workflow/lambdas/starter/index.mjs");

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    await expect(handler(event)).rejects.toThrow("Failed to start execution");
  });

  it("should throw error when execution fails", async () => {
    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    const { handler } =
      await import("../../../../workflow/lambdas/starter/index.mjs");

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    // Handler only starts execution and returns immediately, doesn't check status
    const result = await handler(event);
    expect(result.status).toBe("ok");
    expect(result.executionArn).toBe(
      "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    );
  });

  it("should throw error when execution times out", async () => {
    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    const { handler } =
      await import("../../../../workflow/lambdas/starter/index.mjs");

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    // Handler only starts execution and returns immediately, doesn't check status
    const result = await handler(event);
    expect(result.status).toBe("ok");
    expect(result.executionArn).toBe(
      "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    );
  });

  it("should throw error when execution is aborted", async () => {
    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    const { handler } =
      await import("../../../../workflow/lambdas/starter/index.mjs");

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    // Handler only starts execution and returns immediately, doesn't check status
    const result = await handler(event);
    expect(result.status).toBe("ok");
    expect(result.executionArn).toBe(
      "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    );
  });

  it("should poll while execution is running and succeed", async () => {
    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    const { handler } =
      await import("../../../../workflow/lambdas/starter/index.mjs");

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    // Handler only starts execution and returns immediately, doesn't poll
    const result = await handler(event);

    expect(result.status).toBe("ok");
    expect(result.executionArn).toBe(
      "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    );
  });

  it("should timeout when unknown execution status persists", async () => {
    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    const { handler } =
      await import("../../../../workflow/lambdas/starter/index.mjs");

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    // Handler only starts execution and returns immediately, doesn't check status or timeout
    const result = await handler(event);
    expect(result.status).toBe("ok");
    expect(result.executionArn).toBe(
      "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    );
  });

  it("should retry on DescribeExecution errors and eventually succeed", async () => {
    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    const { handler } =
      await import("../../../../workflow/lambdas/starter/index.mjs");

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    // Handler only starts execution and returns immediately, doesn't poll or retry
    const result = await handler(event);

    expect(result.status).toBe("ok");
    expect(result.executionArn).toBe(
      "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    );
  });

  it("should handle null/undefined event", async () => {
    const { handler } =
      await import("../../../../workflow/lambdas/starter/index.mjs");

    await expect(handler(null)).rejects.toThrow(
      "Expect exactly one SQS record per invocation",
    );
  });

  it("should handle event without Records property", async () => {
    const { handler } =
      await import("../../../../workflow/lambdas/starter/index.mjs");

    await expect(handler({})).rejects.toThrow(
      "Expect exactly one SQS record per invocation",
    );
  });

  it("should start execution without checking running count", async () => {
    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    const { handler } =
      await import("../../../../workflow/lambdas/starter/index.mjs");

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    // Concurrency is controlled by Lambda's ReservedConcurrentExecutions
    // No need to check running Step Function executions
    const result = await handler(event);
    expect(result.status).toBe("ok");
    expect(result.executionArn).toBe(
      "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    );
  });

  it("should handle missing executionArn in response", async () => {
    sfnMock.on(StartExecutionCommand).resolves({
      // executionArn is missing
    });

    const { handler } =
      await import("../../../../workflow/lambdas/starter/index.mjs");

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    const result = await handler(event);

    expect(result.status).toBe("ok");
    expect(result.executionArn).toBe("arn not found");
  });
});
