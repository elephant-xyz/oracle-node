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
const mockExecuteWithTaskToken = vi.fn();
const mockParseS3Uri = vi.fn((uri) => {
  const match = /^s3:\/\/([^/]+)\/(.*)$/.exec(uri);
  return match ? { bucket: match[1], key: match[2] } : null;
});
const mockDownloadS3Object = vi.fn();
const mockUploadToS3 = vi.fn().mockResolvedValue("s3://bucket/output.zip");
const mockCreateLogger = vi.fn(() => vi.fn());
const mockEmitWorkflowEvent = vi.fn();
const mockCreateWorkflowError = vi.fn((code, details) => ({ code, details }));

vi.mock("workflow-shared-utils", () => ({
  executeWithTaskToken: mockExecuteWithTaskToken,
  parseS3Uri: mockParseS3Uri,
  downloadS3Object: mockDownloadS3Object,
  uploadToS3: mockUploadToS3,
  createLogger: mockCreateLogger,
  emitWorkflowEvent: mockEmitWorkflowEvent,
  createWorkflowError: mockCreateWorkflowError,
}));

// Mock @elephant-xyz/cli/lib
const mockTransform = vi.fn();
vi.mock("@elephant-xyz/cli/lib", () => ({
  transform: mockTransform,
}));

// Mock scripts-manager
const mockEnsureScriptsForCounty = vi.fn();
vi.mock(
  "../../../../workflow/lambdas/transform-worker/scripts-manager.mjs",
  () => ({
    createTransformScriptsManager: vi.fn(() => ({
      ensureScriptsForCounty: mockEnsureScriptsForCounty,
    })),
  }),
);

describe("transform-worker handler", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.clearAllMocks();
    sfnMock.reset();
    eventBridgeMock.reset();

    process.env = {
      ...originalEnv,
      TRANSFORM_S3_PREFIX: "s3://transforms-bucket/transforms/",
    };

    // Default mock implementations
    mockCreateLogger.mockReturnValue(vi.fn());
    mockEnsureScriptsForCounty.mockResolvedValue({
      scriptsZipPath: "/tmp/scripts.zip",
      md5: "abc123",
    });

    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  const createSqsEvent = (taskToken, input) => ({
    Records: [
      {
        body: JSON.stringify({
          taskToken,
          input,
        }),
      },
    ],
  });

  it("should emit IN_PROGRESS event at start", async () => {
    mockTransform.mockResolvedValue({ success: true });
    mockExecuteWithTaskToken.mockResolvedValue(undefined);

    const { handler } = await import(
      "../../../../workflow/lambdas/transform-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-123", {
      inputS3Uri: "s3://input-bucket/input.zip",
      county: "test-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-123",
    });

    await handler(event);

    // Verify IN_PROGRESS event was emitted
    expect(mockEmitWorkflowEvent).toHaveBeenCalledWith({
      executionId: "exec-123",
      county: "test-county",
      status: "IN_PROGRESS",
      phase: "Transform",
      step: "Transform",
      taskToken: "task-token-123",
      log: expect.any(Function),
    });
  });

  it("should emit SUCCEEDED event on successful transform", async () => {
    mockTransform.mockResolvedValue({ success: true });
    mockExecuteWithTaskToken.mockResolvedValue(undefined);

    const { handler } = await import(
      "../../../../workflow/lambdas/transform-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-success", {
      inputS3Uri: "s3://input-bucket/input.zip",
      county: "success-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-success",
    });

    await handler(event);

    // Should have 2 emitWorkflowEvent calls: IN_PROGRESS and SUCCEEDED
    expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(2);

    // Second call should be SUCCEEDED
    expect(mockEmitWorkflowEvent).toHaveBeenNthCalledWith(2, {
      executionId: "exec-success",
      county: "success-county",
      status: "SUCCEEDED",
      phase: "Transform",
      step: "Transform",
      log: expect.any(Function),
    });
  });

  it("should emit FAILED event with TRANSFORM_SCRIPTS_FAILED on script failure", async () => {
    mockTransform.mockResolvedValue({
      success: false,
      error: "Script execution failed",
      scriptFailure: true,
    });
    mockExecuteWithTaskToken.mockResolvedValue(undefined);

    const { handler } = await import(
      "../../../../workflow/lambdas/transform-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-script-fail", {
      inputS3Uri: "s3://input-bucket/input.zip",
      county: "script-fail-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-script-fail",
    });

    await handler(event);

    // Should have 2 emitWorkflowEvent calls: IN_PROGRESS and FAILED
    expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(2);

    // Second call should be FAILED with TRANSFORM_SCRIPTS_FAILED
    expect(mockEmitWorkflowEvent).toHaveBeenNthCalledWith(2, {
      executionId: "exec-script-fail",
      county: "script-fail-county",
      status: "FAILED",
      phase: "Transform",
      step: "Transform",
      taskToken: "task-token-script-fail",
      errors: [
        { code: "TRANSFORM_SCRIPTS_FAILED", details: expect.any(Object) },
      ],
      log: expect.any(Function),
    });
  });

  it("should emit FAILED event with TRANSFORM_FAILED on general failure", async () => {
    mockTransform.mockResolvedValue({
      success: false,
      error: "General failure",
      scriptFailure: false,
    });
    mockExecuteWithTaskToken.mockResolvedValue(undefined);

    const { handler } = await import(
      "../../../../workflow/lambdas/transform-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-general-fail", {
      inputS3Uri: "s3://input-bucket/input.zip",
      county: "general-fail-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-general-fail",
    });

    await handler(event);

    // Second call should be FAILED with TRANSFORM_FAILED
    expect(mockEmitWorkflowEvent).toHaveBeenNthCalledWith(2, {
      executionId: "exec-general-fail",
      county: "general-fail-county",
      status: "FAILED",
      phase: "Transform",
      step: "Transform",
      taskToken: "task-token-general-fail",
      errors: [{ code: "TRANSFORM_FAILED", details: expect.any(Object) }],
      log: expect.any(Function),
    });
  });

  it("should call executeWithTaskToken with result on success", async () => {
    mockTransform.mockResolvedValue({ success: true });
    mockExecuteWithTaskToken.mockResolvedValue(undefined);

    const { handler } = await import(
      "../../../../workflow/lambdas/transform-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-result", {
      inputS3Uri: "s3://input-bucket/input.zip",
      county: "result-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-result",
    });

    await handler(event);

    expect(mockExecuteWithTaskToken).toHaveBeenCalledWith({
      taskToken: "task-token-result",
      log: expect.any(Function),
      workerFn: expect.any(Function),
    });
  });

  it("should call executeWithTaskToken with throwing function on failure", async () => {
    mockTransform.mockResolvedValue({
      success: false,
      error: "Transform failed",
    });
    mockExecuteWithTaskToken.mockResolvedValue(undefined);

    const { handler } = await import(
      "../../../../workflow/lambdas/transform-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-throw", {
      inputS3Uri: "s3://input-bucket/input.zip",
      county: "throw-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-throw",
    });

    await handler(event);

    // Should have been called - get the last call which handles the error
    const calls = mockExecuteWithTaskToken.mock.calls;
    expect(calls.length).toBeGreaterThan(0);

    const lastCall = calls[calls.length - 1];
    expect(lastCall[0].taskToken).toBe("task-token-throw");

    // The workerFn should throw when called
    await expect(lastCall[0].workerFn()).rejects.toThrow();
  });

  it("should create logger with correct base fields", async () => {
    mockTransform.mockResolvedValue({ success: true });
    mockExecuteWithTaskToken.mockResolvedValue(undefined);

    const { handler } = await import(
      "../../../../workflow/lambdas/transform-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-logger", {
      inputS3Uri: "s3://input-bucket/input.zip",
      county: "logger-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-logger",
    });

    await handler(event);

    expect(mockCreateLogger).toHaveBeenCalledWith({
      component: "transform-worker",
      at: expect.any(String),
      county: "logger-county",
      executionId: "exec-logger",
    });
  });
});
