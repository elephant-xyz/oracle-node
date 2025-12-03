import { describe, it, expect, beforeEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import {
  EventBridgeClient,
  PutEventsCommand,
} from "@aws-sdk/client-eventbridge";

const eventBridgeMock = mockClient(EventBridgeClient);

describe("eventbridge utilities", () => {
  beforeEach(() => {
    vi.resetModules();
    eventBridgeMock.reset();
  });

  describe("emitWorkflowEvent", () => {
    it("should emit event with required fields", async () => {
      eventBridgeMock.on(PutEventsCommand).resolves({});

      const { emitWorkflowEvent } = await import(
        "../../../../workflow/layers/shared/src/eventbridge.mjs"
      );

      await emitWorkflowEvent({
        executionId: "exec-123",
        county: "test-county",
        status: "IN_PROGRESS",
        phase: "Transform",
        step: "Transform",
      });

      expect(eventBridgeMock).toHaveReceivedCommandWith(PutEventsCommand, {
        Entries: [
          {
            Source: "elephant.workflow",
            DetailType: "WorkflowEvent",
            Detail: JSON.stringify({
              executionId: "exec-123",
              county: "test-county",
              status: "IN_PROGRESS",
              phase: "Transform",
              step: "Transform",
            }),
          },
        ],
      });
    });

    it("should include taskToken when provided", async () => {
      eventBridgeMock.on(PutEventsCommand).resolves({});

      const { emitWorkflowEvent } = await import(
        "../../../../workflow/layers/shared/src/eventbridge.mjs"
      );

      await emitWorkflowEvent({
        executionId: "exec-123",
        county: "test-county",
        status: "SCHEDULED",
        phase: "SVL",
        step: "SVL",
        taskToken: "task-token-abc",
      });

      expect(eventBridgeMock).toHaveReceivedCommandWith(PutEventsCommand, {
        Entries: [
          {
            Source: "elephant.workflow",
            DetailType: "WorkflowEvent",
            Detail: JSON.stringify({
              executionId: "exec-123",
              county: "test-county",
              status: "SCHEDULED",
              phase: "SVL",
              step: "SVL",
              taskToken: "task-token-abc",
            }),
          },
        ],
      });
    });

    it("should include errors when provided", async () => {
      eventBridgeMock.on(PutEventsCommand).resolves({});

      const { emitWorkflowEvent } = await import(
        "../../../../workflow/layers/shared/src/eventbridge.mjs"
      );

      const errors = [
        { code: "TRANSFORM_FAILED", details: { message: "Script error" } },
      ];

      await emitWorkflowEvent({
        executionId: "exec-123",
        county: "test-county",
        status: "FAILED",
        phase: "Transform",
        step: "Transform",
        errors,
      });

      expect(eventBridgeMock).toHaveReceivedCommandWith(PutEventsCommand, {
        Entries: [
          {
            Source: "elephant.workflow",
            DetailType: "WorkflowEvent",
            Detail: JSON.stringify({
              executionId: "exec-123",
              county: "test-county",
              status: "FAILED",
              phase: "Transform",
              step: "Transform",
              errors,
            }),
          },
        ],
      });
    });

    it("should not include errors when array is empty", async () => {
      eventBridgeMock.on(PutEventsCommand).resolves({});

      const { emitWorkflowEvent } = await import(
        "../../../../workflow/layers/shared/src/eventbridge.mjs"
      );

      await emitWorkflowEvent({
        executionId: "exec-123",
        county: "test-county",
        status: "SUCCEEDED",
        phase: "Hash",
        step: "Hash",
        errors: [],
      });

      expect(eventBridgeMock).toHaveReceivedCommandWith(PutEventsCommand, {
        Entries: [
          {
            Source: "elephant.workflow",
            DetailType: "WorkflowEvent",
            Detail: JSON.stringify({
              executionId: "exec-123",
              county: "test-county",
              status: "SUCCEEDED",
              phase: "Hash",
              step: "Hash",
            }),
          },
        ],
      });
    });

    it("should log success when logger is provided", async () => {
      eventBridgeMock.on(PutEventsCommand).resolves({});

      const { emitWorkflowEvent } = await import(
        "../../../../workflow/layers/shared/src/eventbridge.mjs"
      );

      const log = vi.fn();

      await emitWorkflowEvent({
        executionId: "exec-123",
        county: "test-county",
        status: "SUCCEEDED",
        phase: "Upload",
        step: "Upload",
        log,
      });

      expect(log).toHaveBeenCalledWith("info", "eventbridge_event_emitted", {
        status: "SUCCEEDED",
        phase: "Upload",
        step: "Upload",
        executionId: "exec-123",
        county: "test-county",
      });
    });

    it("should log error but not throw when EventBridge fails", async () => {
      eventBridgeMock
        .on(PutEventsCommand)
        .rejects(new Error("EventBridge error"));

      const { emitWorkflowEvent } = await import(
        "../../../../workflow/layers/shared/src/eventbridge.mjs"
      );

      const log = vi.fn();

      // Should not throw
      await emitWorkflowEvent({
        executionId: "exec-123",
        county: "test-county",
        status: "IN_PROGRESS",
        phase: "Transform",
        step: "Transform",
        log,
      });

      expect(log).toHaveBeenCalledWith("error", "eventbridge_event_failed", {
        error: "EventBridge error",
        status: "IN_PROGRESS",
        phase: "Transform",
        step: "Transform",
      });
    });

    it("should handle non-Error exception gracefully", async () => {
      eventBridgeMock.on(PutEventsCommand).rejects("string error");

      const { emitWorkflowEvent } = await import(
        "../../../../workflow/layers/shared/src/eventbridge.mjs"
      );

      const log = vi.fn();

      await emitWorkflowEvent({
        executionId: "exec-123",
        county: "test-county",
        status: "IN_PROGRESS",
        phase: "Transform",
        step: "Transform",
        log,
      });

      expect(log).toHaveBeenCalledWith(
        "error",
        "eventbridge_event_failed",
        expect.objectContaining({
          status: "IN_PROGRESS",
        }),
      );
    });

    it("should work without logger provided", async () => {
      eventBridgeMock.on(PutEventsCommand).resolves({});

      const { emitWorkflowEvent } = await import(
        "../../../../workflow/layers/shared/src/eventbridge.mjs"
      );

      // Should not throw when log is undefined
      await expect(
        emitWorkflowEvent({
          executionId: "exec-123",
          county: "test-county",
          status: "IN_PROGRESS",
          phase: "Transform",
          step: "Transform",
        }),
      ).resolves.toBeUndefined();
    });
  });

  describe("createWorkflowError", () => {
    it("should create error with code only", async () => {
      const { createWorkflowError } = await import(
        "../../../../workflow/layers/shared/src/eventbridge.mjs"
      );

      const error = createWorkflowError("TRANSFORM_FAILED");

      expect(error).toEqual({ code: "TRANSFORM_FAILED" });
    });

    it("should create error with code and details", async () => {
      const { createWorkflowError } = await import(
        "../../../../workflow/layers/shared/src/eventbridge.mjs"
      );

      const error = createWorkflowError("SVL_VALIDATION_ERROR", {
        errorsS3Uri: "s3://bucket/errors.csv",
      });

      expect(error).toEqual({
        code: "SVL_VALIDATION_ERROR",
        details: { errorsS3Uri: "s3://bucket/errors.csv" },
      });
    });

    it("should not include details key when details is undefined", async () => {
      const { createWorkflowError } = await import(
        "../../../../workflow/layers/shared/src/eventbridge.mjs"
      );

      const error = createWorkflowError("HASH_FAILED", undefined);

      expect(error).toEqual({ code: "HASH_FAILED" });
      expect("details" in error).toBe(false);
    });

    it("should handle complex details object", async () => {
      const { createWorkflowError } = await import(
        "../../../../workflow/layers/shared/src/eventbridge.mjs"
      );

      const error = createWorkflowError("UPLOAD_FAILED", {
        message: "Network error",
        retryCount: 3,
        statusCode: 500,
      });

      expect(error).toEqual({
        code: "UPLOAD_FAILED",
        details: {
          message: "Network error",
          retryCount: 3,
          statusCode: 500,
        },
      });
    });
  });
});
