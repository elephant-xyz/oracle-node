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
vi.mock(
  "../../../../workflow/lambdas/transform-worker/shared/index.mjs",
  () => ({
    executeWithTaskToken: mockExecuteWithTaskToken,
    parseS3Uri: mockParseS3Uri,
    downloadS3Object: mockDownloadS3Object,
    uploadToS3: mockUploadToS3,
    createLogger: mockCreateLogger,
    emitWorkflowEvent: mockEmitWorkflowEvent,
  }),
);

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

  it("should only emit IN_PROGRESS event on successful transform (SUCCEEDED is emitted by state machine)", async () => {
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

    // Should have only 1 emitWorkflowEvent call: IN_PROGRESS
    // SUCCEEDED event is now emitted by the state machine, not the worker
    expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(1);

    // Should be IN_PROGRESS
    expect(mockEmitWorkflowEvent).toHaveBeenCalledWith({
      executionId: "exec-success",
      county: "success-county",
      status: "IN_PROGRESS",
      phase: "Transform",
      step: "Transform",
      taskToken: "task-token-success",
      log: expect.any(Function),
    });
  });

  it("should only emit IN_PROGRESS event on script failure (FAILED is emitted by state machine)", async () => {
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

    // Should have only 1 emitWorkflowEvent call: IN_PROGRESS
    // FAILED event is now emitted by the state machine's WaitForTransformResolution state
    expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(1);
    expect(mockEmitWorkflowEvent).toHaveBeenCalledWith({
      executionId: "exec-script-fail",
      county: "script-fail-county",
      status: "IN_PROGRESS",
      phase: "Transform",
      step: "Transform",
      taskToken: "task-token-script-fail",
      log: expect.any(Function),
    });
  });

  it("should only emit IN_PROGRESS event on general failure (FAILED is emitted by state machine)", async () => {
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

    // Should have only 1 emitWorkflowEvent call: IN_PROGRESS
    // FAILED event is now emitted by the state machine's WaitForTransformResolution state
    expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(1);
    expect(mockEmitWorkflowEvent).toHaveBeenCalledWith({
      executionId: "exec-general-fail",
      county: "general-fail-county",
      status: "IN_PROGRESS",
      phase: "Transform",
      step: "Transform",
      taskToken: "task-token-general-fail",
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

  it("should throw error when TRANSFORM_S3_PREFIX is not set", async () => {
    delete process.env.TRANSFORM_S3_PREFIX;

    mockTransform.mockResolvedValue({ success: true });
    // executeWithTaskToken is called with the error-throwing workerFn
    mockExecuteWithTaskToken.mockResolvedValue(undefined);

    const { handler } = await import(
      "../../../../workflow/lambdas/transform-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-no-prefix", {
      inputS3Uri: "s3://input-bucket/input.zip",
      county: "test-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-no-prefix",
    });

    // The handler catches the error and passes it to executeWithTaskToken
    await handler(event);

    // Should have only emitted IN_PROGRESS (FAILED is emitted by state machine)
    expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(1);
    expect(mockEmitWorkflowEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        status: "IN_PROGRESS",
      }),
    );

    // The error should be passed to executeWithTaskToken to throw
    const failureCall =
      mockExecuteWithTaskToken.mock.calls[
        mockExecuteWithTaskToken.mock.calls.length - 1
      ];
    const workerFn = failureCall[0].workerFn;
    await expect(workerFn()).rejects.toThrow("TRANSFORM_S3_PREFIX is required");
  });

  it("should handle direct invocation mode", async () => {
    mockTransform.mockResolvedValue({ success: true });
    mockUploadToS3.mockResolvedValue("s3://output-bucket/result.zip");

    const { handler } = await import(
      "../../../../workflow/lambdas/transform-worker/index.mjs"
    );

    const directEvent = {
      directInvocation: true,
      inputS3Uri: "s3://input-bucket/direct-input.zip",
      county: "direct-county",
      outputPrefix: "s3://output-bucket/direct-outputs/",
      executionId: "exec-direct",
    };

    const result = await handler(directEvent);

    // Should return result directly (not void)
    expect(result).toBeDefined();
    expect(result.county).toBe("direct-county");
    expect(result.executionId).toBe("exec-direct");
    expect(result.transformedOutputS3Uri).toBeDefined();

    // Should NOT have called emitWorkflowEvent (skipped in direct mode)
    expect(mockEmitWorkflowEvent).not.toHaveBeenCalled();

    // Should NOT have called executeWithTaskToken (skipped in direct mode)
    expect(mockExecuteWithTaskToken).not.toHaveBeenCalled();
  });

  it("should create logger with correct fields in direct invocation mode", async () => {
    mockTransform.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/transform-worker/index.mjs"
    );

    const directEvent = {
      directInvocation: true,
      inputS3Uri: "s3://input-bucket/direct-input.zip",
      county: "direct-logger-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-direct-logger",
    };

    await handler(directEvent);

    expect(mockCreateLogger).toHaveBeenCalledWith({
      component: "transform-worker",
      at: expect.any(String),
      county: "direct-logger-county",
      executionId: "exec-direct-logger",
    });
  });

  it("should return correct result from workerFn on success", async () => {
    mockTransform.mockResolvedValue({ success: true });
    mockUploadToS3.mockResolvedValue("s3://output-bucket/transformed.zip");

    let capturedWorkerFn = null;
    mockExecuteWithTaskToken.mockImplementation(async ({ workerFn }) => {
      capturedWorkerFn = workerFn;
    });

    const { handler } = await import(
      "../../../../workflow/lambdas/transform-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-workerfn", {
      inputS3Uri: "s3://input-bucket/input.zip",
      county: "workerfn-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-workerfn",
    });

    await handler(event);

    // The first call is the success path
    const successCall = mockExecuteWithTaskToken.mock.calls[0];
    const workerFnResult = await successCall[0].workerFn();

    expect(workerFnResult).toEqual({
      transformedOutputS3Uri: "s3://output-bucket/transformed.zip",
      county: "workerfn-county",
      executionId: "exec-workerfn",
    });
  });

  it("should throw error in direct invocation mode when TRANSFORM_S3_PREFIX is missing", async () => {
    delete process.env.TRANSFORM_S3_PREFIX;

    const { handler } = await import(
      "../../../../workflow/lambdas/transform-worker/index.mjs"
    );

    const directEvent = {
      directInvocation: true,
      inputS3Uri: "s3://input-bucket/direct-input.zip",
      county: "error-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-error",
    };

    await expect(handler(directEvent)).rejects.toThrow(
      "TRANSFORM_S3_PREFIX is required",
    );
  });

  it("should handle transform failure with default error message", async () => {
    // Transform fails but with empty error message (tests line 151)
    mockTransform.mockResolvedValue({
      success: false,
      error: "", // Empty error
      scriptFailure: false,
    });
    mockExecuteWithTaskToken.mockResolvedValue(undefined);

    const { handler } = await import(
      "../../../../workflow/lambdas/transform-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-empty-error", {
      inputS3Uri: "s3://input-bucket/input.zip",
      county: "empty-error-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-empty-error",
    });

    await handler(event);

    // Should only emit IN_PROGRESS (FAILED is emitted by state machine)
    expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(1);
    expect(mockEmitWorkflowEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        status: "IN_PROGRESS",
      }),
    );
  });

  it("should handle script failure with default error message when error is empty", async () => {
    // Script failure but with empty error message (tests line 145)
    mockTransform.mockResolvedValue({
      success: false,
      error: "", // Empty error
      scriptFailure: true,
    });
    mockExecuteWithTaskToken.mockResolvedValue(undefined);

    const { handler } = await import(
      "../../../../workflow/lambdas/transform-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-script-empty-error", {
      inputS3Uri: "s3://input-bucket/input.zip",
      county: "script-empty-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-script-empty",
    });

    await handler(event);

    // Should only emit IN_PROGRESS (FAILED is emitted by state machine)
    expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(1);
    expect(mockEmitWorkflowEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        status: "IN_PROGRESS",
      }),
    );

    // The workerFn should throw ScriptsFailedError
    const failureCall =
      mockExecuteWithTaskToken.mock.calls[
        mockExecuteWithTaskToken.mock.calls.length - 1
      ];
    const workerFn = failureCall[0].workerFn;

    await expect(workerFn()).rejects.toThrow("Transform scripts failed");
  });
});
