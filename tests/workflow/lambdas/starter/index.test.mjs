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
    // Mock ListExecutionsCommand to return no running executions
    sfnMock.on(ListExecutionsCommand).resolves({
      executions: [],
    });

    // Mock StartExecutionCommand
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

    // Verify AWS SDK was called correctly using custom matchers
    expect(sfnMock).toHaveReceivedCommandWith(StartExecutionCommand, {
      stateMachineArn: "arn:aws:states:us-east-1:123456789:stateMachine:test",
      input: JSON.stringify({ message: inputData }),
    });
  });

  it("should throw error when step function execution fails", async () => {
    // Mock ListExecutionsCommand to return no running executions
    sfnMock.on(ListExecutionsCommand).resolves({
      executions: [],
    });

    // Mock StartExecutionCommand
    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    // Mock DescribeExecutionCommand to return FAILED
    sfnMock.on(DescribeExecutionCommand).resolves({
      status: "FAILED",
      cause: "Some error occurred",
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

  it("should throw error when step function times out", async () => {
    // Mock ListExecutionsCommand to return no running executions
    sfnMock.on(ListExecutionsCommand).resolves({
      executions: [],
    });

    // Mock StartExecutionCommand
    sfnMock.on(StartExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
    });

    // Mock DescribeExecutionCommand to return TIMED_OUT
    sfnMock.on(DescribeExecutionCommand).resolves({
      status: "TIMED_OUT",
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
    // Mock ListExecutionsCommand to return no running executions
    sfnMock.on(ListExecutionsCommand).resolves({
      executions: [],
    });

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
});
