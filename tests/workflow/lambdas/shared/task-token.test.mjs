import { describe, it, expect, beforeEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import {
  SFNClient,
  SendTaskSuccessCommand,
  SendTaskFailureCommand,
} from "@aws-sdk/client-sfn";

const sfnMock = mockClient(SFNClient);

describe("task-token utilities", () => {
  beforeEach(() => {
    vi.resetModules();
    sfnMock.reset();
  });

  describe("sendTaskSuccess", () => {
    it("should send task success with output", async () => {
      sfnMock.on(SendTaskSuccessCommand).resolves({});

      const { sendTaskSuccess } = await import(
        "../../../../workflow/layers/shared/src/task-token.mjs"
      );

      await sendTaskSuccess({
        taskToken: "task-token-123",
        output: { result: "success", data: [1, 2, 3] },
      });

      expect(sfnMock).toHaveReceivedCommandWith(SendTaskSuccessCommand, {
        taskToken: "task-token-123",
        output: JSON.stringify({ result: "success", data: [1, 2, 3] }),
      });
    });

    it("should serialize complex output to JSON", async () => {
      sfnMock.on(SendTaskSuccessCommand).resolves({});

      const { sendTaskSuccess } = await import(
        "../../../../workflow/layers/shared/src/task-token.mjs"
      );

      const complexOutput = {
        transformedOutputS3Uri: "s3://bucket/output.zip",
        county: "test-county",
        executionId: "exec-123",
        nested: { deep: { value: true } },
      };

      await sendTaskSuccess({
        taskToken: "token",
        output: complexOutput,
      });

      expect(sfnMock).toHaveReceivedCommandWith(SendTaskSuccessCommand, {
        taskToken: "token",
        output: JSON.stringify(complexOutput),
      });
    });
  });

  describe("sendTaskFailure", () => {
    it("should send task failure with error and cause", async () => {
      sfnMock.on(SendTaskFailureCommand).resolves({});

      const { sendTaskFailure } = await import(
        "../../../../workflow/layers/shared/src/task-token.mjs"
      );

      await sendTaskFailure({
        taskToken: "task-token-456",
        error: "TransformError",
        cause: "Transform scripts failed",
      });

      expect(sfnMock).toHaveReceivedCommandWith(SendTaskFailureCommand, {
        taskToken: "task-token-456",
        error: "TransformError",
        cause: "Transform scripts failed",
      });
    });

    it("should truncate cause to 32KB limit", async () => {
      sfnMock.on(SendTaskFailureCommand).resolves({});

      const { sendTaskFailure } = await import(
        "../../../../workflow/layers/shared/src/task-token.mjs"
      );

      // Create a cause longer than 32KB
      const longCause = "x".repeat(40000);

      await sendTaskFailure({
        taskToken: "token",
        error: "Error",
        cause: longCause,
      });

      expect(sfnMock).toHaveReceivedCommandWith(SendTaskFailureCommand, {
        taskToken: "token",
        error: "Error",
        cause: "x".repeat(32768), // Should be truncated
      });
    });
  });

  describe("executeWithTaskToken", () => {
    it("should execute worker function and send success on completion", async () => {
      sfnMock.on(SendTaskSuccessCommand).resolves({});

      const { executeWithTaskToken } = await import(
        "../../../../workflow/layers/shared/src/task-token.mjs"
      );

      const log = vi.fn();
      const workerFn = vi.fn().mockResolvedValue({
        transformedOutputS3Uri: "s3://bucket/output.zip",
        county: "test",
      });

      await executeWithTaskToken({
        taskToken: "task-token-abc-very-long-token-string-here",
        workerFn,
        log,
      });

      expect(workerFn).toHaveBeenCalled();
      expect(sfnMock).toHaveReceivedCommandWith(SendTaskSuccessCommand, {
        taskToken: "task-token-abc-very-long-token-string-here",
        output: JSON.stringify({
          transformedOutputS3Uri: "s3://bucket/output.zip",
          county: "test",
        }),
      });

      expect(log).toHaveBeenCalledWith("info", "task_success", {
        taskToken: expect.stringContaining("task-token-abc"),
      });
    });

    it("should send failure when worker function throws Error", async () => {
      sfnMock.on(SendTaskFailureCommand).resolves({});

      const { executeWithTaskToken } = await import(
        "../../../../workflow/layers/shared/src/task-token.mjs"
      );

      const log = vi.fn();
      const error = new Error("Transform failed");
      error.name = "TransformError";
      const workerFn = vi.fn().mockRejectedValue(error);

      await executeWithTaskToken({
        taskToken: "task-token-xyz-very-long-token-string-here",
        workerFn,
        log,
      });

      expect(sfnMock).toHaveReceivedCommandWith(SendTaskFailureCommand, {
        taskToken: "task-token-xyz-very-long-token-string-here",
        error: "TransformError",
        cause: "Transform failed",
      });

      expect(log).toHaveBeenCalledWith("error", "task_failure", {
        taskToken: expect.stringContaining("task-token-xyz"),
        error: "TransformError",
        cause: "Transform failed",
      });
    });

    it("should handle non-Error thrown values", async () => {
      sfnMock.on(SendTaskFailureCommand).resolves({});

      const { executeWithTaskToken } = await import(
        "../../../../workflow/layers/shared/src/task-token.mjs"
      );

      const log = vi.fn();
      const workerFn = vi.fn().mockRejectedValue("string error");

      await executeWithTaskToken({
        taskToken: "token-long-enough-for-substring",
        workerFn,
        log,
      });

      // Non-Error values are converted to Error objects (which have name: "Error")
      expect(sfnMock).toHaveReceivedCommandWith(SendTaskFailureCommand, {
        taskToken: "token-long-enough-for-substring",
        error: "Error",
        cause: "string error",
      });
    });

    it("should use error.name from Error object", async () => {
      sfnMock.on(SendTaskFailureCommand).resolves({});

      const { executeWithTaskToken } = await import(
        "../../../../workflow/layers/shared/src/task-token.mjs"
      );

      const log = vi.fn();
      const error = new Error("Something went wrong");
      // Error without a custom name defaults to "Error"
      const workerFn = vi.fn().mockRejectedValue(error);

      await executeWithTaskToken({
        taskToken: "token-long-enough-for-substring",
        workerFn,
        log,
      });

      // Should use the error name from the Error object
      expect(sfnMock).toHaveReceivedCommandWith(SendTaskFailureCommand, {
        taskToken: "token-long-enough-for-substring",
        error: "Error",
        cause: "Something went wrong",
      });
    });
  });
});
