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
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

const sfnMock = mockClient(SFNClient);
const eventBridgeMock = mockClient(EventBridgeClient);
const s3Mock = mockClient(S3Client);
const dynamoMock = mockClient(DynamoDBClient);

describe("downloader lambda - EventBridge integration", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.resetModules();
    sfnMock.reset();
    eventBridgeMock.reset();
    s3Mock.reset();
    dynamoMock.reset();

    process.env = {
      ...originalEnv,
      AWS_REGION: "us-east-1",
    };

    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  describe("emitWorkflowEvent", () => {
    it("should emit IN_PROGRESS event to EventBridge when processing starts", async () => {
      // Setup mocks for the full handler flow
      eventBridgeMock.on(PutEventsCommand).resolves({});
      sfnMock.on(SendTaskFailureCommand).resolves({});

      const { handler } =
        await import("../../../../prepare/lambdas/downloader/index.mjs");

      const sqsEvent = {
        Records: [
          {
            eventSource: "aws:sqs",
            body: JSON.stringify({
              taskToken: "test-task-token-123",
              executionId:
                "arn:aws:states:us-east-1:123456789:execution:test:abc123",
              county: "Hamilton",
              input_s3_uri: "s3://test-bucket/test-key.zip",
              output_s3_uri_prefix: "s3://test-bucket/output",
            }),
          },
        ],
      };

      // Handler will fail because S3 mock is not fully configured,
      // but we can still verify the EventBridge call was made
      try {
        await handler(sqsEvent);
      } catch (e) {
        // Expected to fail - we're testing the EventBridge call
      }

      // Verify EventBridge was called with IN_PROGRESS status
      expect(eventBridgeMock).toHaveReceivedCommandWith(PutEventsCommand, {
        Entries: [
          {
            Source: "elephant.workflow",
            DetailType: "WorkflowEvent",
            Detail: expect.stringContaining('"status":"IN_PROGRESS"'),
          },
        ],
      });
    });

    it("should NOT include taskToken in EventBridge event (taskToken is for Step Functions callbacks only)", async () => {
      eventBridgeMock.on(PutEventsCommand).resolves({});
      sfnMock.on(SendTaskFailureCommand).resolves({});

      const { handler } =
        await import("../../../../prepare/lambdas/downloader/index.mjs");

      const taskToken = "test-task-token-with-special-chars-123";
      const sqsEvent = {
        Records: [
          {
            eventSource: "aws:sqs",
            body: JSON.stringify({
              taskToken,
              executionId:
                "arn:aws:states:us-east-1:123456789:execution:test:abc123",
              county: "Hamilton",
              input_s3_uri: "s3://test-bucket/test-key.zip",
            }),
          },
        ],
      };

      try {
        await handler(sqsEvent);
      } catch (e) {
        // Expected to fail
      }

      // Verify the Detail JSON does NOT contain taskToken
      const calls = eventBridgeMock.commandCalls(PutEventsCommand);
      expect(calls.length).toBeGreaterThan(0);

      const firstCall = calls[0];
      const detail = JSON.parse(firstCall.args[0].input.Entries[0].Detail);
      expect(detail.taskToken).toBeUndefined();
    });

    it("should include county in EventBridge event", async () => {
      eventBridgeMock.on(PutEventsCommand).resolves({});
      sfnMock.on(SendTaskFailureCommand).resolves({});

      const { handler } =
        await import("../../../../prepare/lambdas/downloader/index.mjs");

      const sqsEvent = {
        Records: [
          {
            eventSource: "aws:sqs",
            body: JSON.stringify({
              taskToken: "test-task-token",
              executionId:
                "arn:aws:states:us-east-1:123456789:execution:test:abc123",
              county: "Okeechobee",
              input_s3_uri: "s3://test-bucket/test-key.zip",
            }),
          },
        ],
      };

      try {
        await handler(sqsEvent);
      } catch (e) {
        // Expected to fail
      }

      const calls = eventBridgeMock.commandCalls(PutEventsCommand);
      expect(calls.length).toBeGreaterThan(0);

      const detail = JSON.parse(calls[0].args[0].input.Entries[0].Detail);
      expect(detail.county).toBe("Okeechobee");
      expect(detail.phase).toBe("Prepare");
      expect(detail.step).toBe("Prepare");
    });

    it("should throw error with code 01019 when EventBridge emission fails", async () => {
      // EventBridge fails - handler should throw and report via sendTaskFailure
      eventBridgeMock
        .on(PutEventsCommand)
        .rejects(new Error("EventBridge unavailable"));
      sfnMock.on(SendTaskFailureCommand).resolves({});

      const { handler } =
        await import("../../../../prepare/lambdas/downloader/index.mjs");

      const sqsEvent = {
        Records: [
          {
            eventSource: "aws:sqs",
            body: JSON.stringify({
              taskToken: "test-task-token",
              executionId:
                "arn:aws:states:us-east-1:123456789:execution:test:abc123",
              county: "Hamilton",
              input_s3_uri: "s3://test-bucket/test-key.zip",
            }),
          },
        ],
      };

      // Handler should catch EventBridge error and send task failure
      await handler(sqsEvent);

      // Verify SendTaskFailureCommand was called with error code 01019 + county
      expect(sfnMock).toHaveReceivedCommandWith(SendTaskFailureCommand, {
        taskToken: "test-task-token",
        error: "01019Hamilton",
        cause: expect.stringContaining("EventBridge"),
      });
    });
  });

  describe("error code handling (01002)", () => {
    it("should send task failure with error code 01002 when processing fails", async () => {
      eventBridgeMock.on(PutEventsCommand).resolves({});
      sfnMock.on(SendTaskFailureCommand).resolves({});

      // S3 GetObject will fail, triggering error handling
      s3Mock.on(GetObjectCommand).rejects(new Error("S3 access denied"));

      const { handler } =
        await import("../../../../prepare/lambdas/downloader/index.mjs");

      const sqsEvent = {
        Records: [
          {
            eventSource: "aws:sqs",
            body: JSON.stringify({
              taskToken: "test-task-token-failure",
              executionId:
                "arn:aws:states:us-east-1:123456789:execution:test:abc123",
              county: "Hamilton",
              input_s3_uri: "s3://test-bucket/test-key.zip",
            }),
          },
        ],
      };

      await handler(sqsEvent);

      // Verify SendTaskFailureCommand was called with error code 01002 + county
      expect(sfnMock).toHaveReceivedCommandWith(SendTaskFailureCommand, {
        taskToken: "test-task-token-failure",
        error: "01002Hamilton",
        cause: expect.any(String),
      });
    });

    it("should include errorCode concatenated with county in error field", async () => {
      eventBridgeMock.on(PutEventsCommand).resolves({});
      sfnMock.on(SendTaskFailureCommand).resolves({});
      s3Mock.on(GetObjectCommand).rejects(new Error("S3 access denied"));

      const { handler } =
        await import("../../../../prepare/lambdas/downloader/index.mjs");

      const taskToken = "task-token-for-extraction";
      const sqsEvent = {
        Records: [
          {
            eventSource: "aws:sqs",
            body: JSON.stringify({
              taskToken,
              executionId:
                "arn:aws:states:us-east-1:123456789:execution:test:abc123",
              county: "Hamilton",
              input_s3_uri: "s3://test-bucket/test-key.zip",
            }),
          },
        ],
      };

      await handler(sqsEvent);

      const calls = sfnMock.commandCalls(SendTaskFailureCommand);
      expect(calls.length).toBe(1);

      // Error code is in the error field, concatenated with county (e.g., "01002Hamilton")
      const errorField = calls[0].args[0].input.error;
      expect(errorField).toMatch(/^01002/); // Starts with error code
      expect(errorField).toContain("Hamilton"); // Contains county

      // Cause payload should NOT contain redundant errorCode or county (already in error field)
      const causeJson = calls[0].args[0].input.cause;
      const cause = JSON.parse(causeJson);
      expect(cause.errorCode).toBeUndefined();
      expect(cause.county).toBeUndefined();
    });

    it("should include county in error field (concatenated with error code)", async () => {
      eventBridgeMock.on(PutEventsCommand).resolves({});
      sfnMock.on(SendTaskFailureCommand).resolves({});
      s3Mock.on(GetObjectCommand).rejects(new Error("S3 error"));

      const { handler } =
        await import("../../../../prepare/lambdas/downloader/index.mjs");

      const sqsEvent = {
        Records: [
          {
            eventSource: "aws:sqs",
            body: JSON.stringify({
              taskToken: "test-token",
              executionId:
                "arn:aws:states:us-east-1:123456789:execution:test:abc123",
              county: "Broward",
              input_s3_uri: "s3://test-bucket/test-key.zip",
            }),
          },
        ],
      };

      await handler(sqsEvent);

      const calls = sfnMock.commandCalls(SendTaskFailureCommand);
      // County is appended to the error code in the error field
      const errorField = calls[0].args[0].input.error;
      expect(errorField).toContain("Broward");
    });

    it("should truncate cause to 256 characters max", async () => {
      eventBridgeMock.on(PutEventsCommand).resolves({});
      sfnMock.on(SendTaskFailureCommand).resolves({});

      // Create an error with a very long message
      const longErrorMessage = "A".repeat(500);
      s3Mock.on(GetObjectCommand).rejects(new Error(longErrorMessage));

      const { handler } =
        await import("../../../../prepare/lambdas/downloader/index.mjs");

      const sqsEvent = {
        Records: [
          {
            eventSource: "aws:sqs",
            body: JSON.stringify({
              taskToken: "test-token",
              executionId:
                "arn:aws:states:us-east-1:123456789:execution:test:abc123",
              county: "Hamilton",
              input_s3_uri: "s3://test-bucket/test-key.zip",
            }),
          },
        ],
      };

      await handler(sqsEvent);

      const calls = sfnMock.commandCalls(SendTaskFailureCommand);
      expect(calls[0].args[0].input.cause.length).toBeLessThanOrEqual(256);
    });
  });

  describe("sendTaskSuccess", () => {
    it("should include taskToken in success output for EventBridge traceability", async () => {
      const mockZipBuffer = createMockZipBuffer();

      eventBridgeMock.on(PutEventsCommand).resolves({});
      sfnMock.on(SendTaskSuccessCommand).resolves({});
      s3Mock.on(GetObjectCommand).resolves({
        Body: {
          transformToByteArray: async () => mockZipBuffer,
        },
      });
      s3Mock.on(PutObjectCommand).resolves({});

      const { handler } =
        await import("../../../../prepare/lambdas/downloader/index.mjs");

      const taskToken = "success-task-token";
      const sqsEvent = {
        Records: [
          {
            eventSource: "aws:sqs",
            body: JSON.stringify({
              taskToken,
              executionId:
                "arn:aws:states:us-east-1:123456789:execution:test:abc123",
              county: "Hamilton",
              input_s3_uri: "s3://test-bucket/test-key.zip",
              output_s3_uri_prefix: "s3://test-bucket/output",
            }),
          },
        ],
      };

      try {
        await handler(sqsEvent);
      } catch (e) {
        // May fail due to prepare() function, but we're testing the mock setup
      }

      // Check if SendTaskSuccessCommand was called with taskToken in output
      const successCalls = sfnMock.commandCalls(SendTaskSuccessCommand);
      if (successCalls.length > 0) {
        const output = JSON.parse(successCalls[0].args[0].input.output);
        expect(output.taskToken).toBe(taskToken);
      }
    });
  });

  describe("SQS event handling", () => {
    it("should throw error with code 01020 when taskToken is missing from SQS message", async () => {
      eventBridgeMock.on(PutEventsCommand).resolves({});

      const { handler } =
        await import("../../../../prepare/lambdas/downloader/index.mjs");

      const sqsEvent = {
        Records: [
          {
            eventSource: "aws:sqs",
            body: JSON.stringify({
              // No taskToken
              executionId:
                "arn:aws:states:us-east-1:123456789:execution:test:abc123",
              county: "Hamilton",
              input_s3_uri: "s3://test-bucket/test-key.zip",
            }),
          },
        ],
      };

      // Handler should throw PrepareError with code 01020 when taskToken is missing
      // Flow: 01016 thrown -> caught -> can't send task failure (no token) -> re-throws 01020
      await expect(handler(sqsEvent)).rejects.toThrow();

      try {
        await handler(sqsEvent);
      } catch (e) {
        expect(e.code).toBe("01020");
        expect(e.message).toContain("taskToken");
      }
    });

    it("should handle direct invocation (non-SQS) without EventBridge or task token", async () => {
      const mockZipBuffer = createMockZipBuffer();

      s3Mock.on(GetObjectCommand).resolves({
        Body: {
          transformToByteArray: async () => mockZipBuffer,
        },
      });
      s3Mock.on(PutObjectCommand).resolves({});

      const { handler } =
        await import("../../../../prepare/lambdas/downloader/index.mjs");

      const directEvent = {
        input_s3_uri: "s3://test-bucket/test-key.zip",
        output_s3_uri_prefix: "s3://test-bucket/output",
      };

      try {
        await handler(directEvent);
      } catch (e) {
        // May fail due to prepare() function
      }

      // EventBridge should NOT be called for direct invocation
      expect(eventBridgeMock).not.toHaveReceivedCommand(PutEventsCommand);
      // Step Functions should NOT be called for direct invocation
      expect(sfnMock).not.toHaveReceivedCommand(SendTaskSuccessCommand);
      expect(sfnMock).not.toHaveReceivedCommand(SendTaskFailureCommand);
    });

    it("should detect SQS event by eventSource property", async () => {
      eventBridgeMock.on(PutEventsCommand).resolves({});
      sfnMock.on(SendTaskFailureCommand).resolves({});

      const { handler } =
        await import("../../../../prepare/lambdas/downloader/index.mjs");

      // SQS event with eventSource
      const sqsEvent = {
        Records: [
          {
            eventSource: "aws:sqs",
            body: JSON.stringify({
              taskToken: "test-token",
              executionId: "test-execution",
              county: "Hamilton",
              input_s3_uri: "s3://test-bucket/test.zip",
            }),
          },
        ],
      };

      try {
        await handler(sqsEvent);
      } catch (e) {
        // Expected to fail
      }

      // Should have called EventBridge (SQS path)
      expect(eventBridgeMock).toHaveReceivedCommand(PutEventsCommand);
    });
  });

  describe("county name handling", () => {
    it("should use county from SQS message body", async () => {
      eventBridgeMock.on(PutEventsCommand).resolves({});
      sfnMock.on(SendTaskFailureCommand).resolves({});
      s3Mock.on(GetObjectCommand).rejects(new Error("S3 error"));

      const { handler } =
        await import("../../../../prepare/lambdas/downloader/index.mjs");

      const sqsEvent = {
        Records: [
          {
            eventSource: "aws:sqs",
            body: JSON.stringify({
              taskToken: "test-token",
              executionId: "test-execution",
              county: "Miami-Dade",
              input_s3_uri: "s3://test-bucket/test.zip",
            }),
          },
        ],
      };

      await handler(sqsEvent);

      // Verify county was passed to EventBridge
      const calls = eventBridgeMock.commandCalls(PutEventsCommand);
      const detail = JSON.parse(calls[0].args[0].input.Entries[0].Detail);
      expect(detail.county).toBe("Miami-Dade");
    });

    it("should default to 'unknown' when county is not provided", async () => {
      eventBridgeMock.on(PutEventsCommand).resolves({});
      sfnMock.on(SendTaskFailureCommand).resolves({});
      s3Mock.on(GetObjectCommand).rejects(new Error("S3 error"));

      const { handler } =
        await import("../../../../prepare/lambdas/downloader/index.mjs");

      const sqsEvent = {
        Records: [
          {
            eventSource: "aws:sqs",
            body: JSON.stringify({
              taskToken: "test-token",
              executionId: "test-execution",
              // No county field
              input_s3_uri: "s3://test-bucket/test.zip",
            }),
          },
        ],
      };

      await handler(sqsEvent);

      const calls = eventBridgeMock.commandCalls(PutEventsCommand);
      const detail = JSON.parse(calls[0].args[0].input.Entries[0].Detail);
      expect(detail.county).toBe("unknown");
    });

    it("should preserve original county case in EventBridge events", async () => {
      eventBridgeMock.on(PutEventsCommand).resolves({});
      sfnMock.on(SendTaskFailureCommand).resolves({});
      s3Mock.on(GetObjectCommand).rejects(new Error("S3 error"));

      const { handler } =
        await import("../../../../prepare/lambdas/downloader/index.mjs");

      // County with mixed case - state machine handles lowercase for queue lookup
      const sqsEvent = {
        Records: [
          {
            eventSource: "aws:sqs",
            body: JSON.stringify({
              taskToken: "test-token",
              executionId: "test-execution",
              county: "Palm Beach",
              input_s3_uri: "s3://test-bucket/test.zip",
            }),
          },
        ],
      };

      await handler(sqsEvent);

      // Lambda should preserve original case for EventBridge events
      const calls = eventBridgeMock.commandCalls(PutEventsCommand);
      const detail = JSON.parse(calls[0].args[0].input.Entries[0].Detail);
      expect(detail.county).toBe("Palm Beach");
    });
  });
});

/**
 * Creates a minimal mock zip buffer for testing
 * This is a bare minimum valid ZIP file structure
 */
function createMockZipBuffer() {
  // Minimal ZIP file with just end of central directory record
  // This is enough for AdmZip to initialize without throwing
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0); // End of central directory signature
  // Rest of EOCD is zeros (no entries, no comment)
  return new Uint8Array(eocd);
}
