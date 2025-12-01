import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import { SFNClient, StartSyncExecutionCommand } from "@aws-sdk/client-sfn";

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
    sfnMock.on(StartSyncExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
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
    expect(sfnMock).toHaveReceivedCommandWith(StartSyncExecutionCommand, {
      stateMachineArn: "arn:aws:states:us-east-1:123456789:stateMachine:test",
      input: JSON.stringify({ message: inputData }),
    });
  });

  it("should throw error when step function execution fails", async () => {
    sfnMock.on(StartSyncExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
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
      "Step function execution failed with status: FAILED. Cause: Some error occurred",
    );
  });

  it("should throw error when step function times out", async () => {
    sfnMock.on(StartSyncExecutionCommand).resolves({
      executionArn: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
      status: "TIMED_OUT",
    });

    const { handler } = await import(
      "../../../../workflow/lambdas/starter/index.mjs"
    );

    const event = {
      Records: [{ body: JSON.stringify({ test: "data" }) }],
    };

    await expect(handler(event)).rejects.toThrow(
      "Step function execution failed with status: TIMED_OUT. Cause: N/A",
    );
  });
});
