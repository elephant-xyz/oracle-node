import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

// Mock the shared module
const mockExecuteWithTaskToken = vi.fn();
const mockParseS3Uri = vi.fn((uri) => {
  const match = /^s3:\/\/([^/]+)\/(.*)$/.exec(uri);
  return match ? { bucket: match[1], key: match[2] } : null;
});
const mockDownloadS3Object = vi.fn();
const mockUploadToS3 = vi.fn();
const mockCreateLogger = vi.fn(() => vi.fn());
const mockEmitWorkflowEvent = vi.fn();
const mockCreateWorkflowError = vi.fn((code, details) => ({ code, details }));

vi.mock("../../../../workflow/lambdas/svl-worker/shared/index.mjs", () => ({
  executeWithTaskToken: mockExecuteWithTaskToken,
  parseS3Uri: mockParseS3Uri,
  downloadS3Object: mockDownloadS3Object,
  uploadToS3: mockUploadToS3,
  createLogger: mockCreateLogger,
  emitWorkflowEvent: mockEmitWorkflowEvent,
  createWorkflowError: mockCreateWorkflowError,
}));

// Mock @elephant-xyz/cli/lib
const mockValidate = vi.fn();
vi.mock("@elephant-xyz/cli/lib", () => ({
  validate: mockValidate,
}));

// Mock errors repository
const mockSaveFailedExecution = vi.fn();
vi.mock("../../../../workflow/lambdas/svl-worker/errors.mjs", () => ({
  createErrorsRepository: vi.fn(() => ({
    saveFailedExecution: mockSaveFailedExecution,
  })),
}));

// Mock csv-parse/sync
vi.mock("csv-parse/sync", () => ({
  parse: vi.fn(() => [{ error_message: "test error" }]),
}));

describe("svl-worker handler", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.clearAllMocks();

    process.env = {
      ...originalEnv,
      ERRORS_TABLE_NAME: "test-errors-table",
    };

    // Default mock implementations
    mockCreateLogger.mockReturnValue(vi.fn());
    mockExecuteWithTaskToken.mockResolvedValue(undefined);
    mockEmitWorkflowEvent.mockResolvedValue(undefined);
    mockDownloadS3Object.mockResolvedValue(undefined);
    mockUploadToS3.mockImplementation((localPath, s3Location) => {
      return Promise.resolve(`s3://${s3Location.bucket}/${s3Location.key}`);
    });
    mockSaveFailedExecution.mockResolvedValue(undefined);

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
    mockValidate.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/svl-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-123", {
      transformedOutputS3Uri: "s3://bucket/transformed.zip",
      county: "test-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-123",
    });

    await handler(event);

    expect(mockEmitWorkflowEvent).toHaveBeenCalledWith({
      executionId: "exec-123",
      county: "test-county",
      status: "IN_PROGRESS",
      phase: "SVL",
      step: "SVL",
      taskToken: "task-token-123",
      log: expect.any(Function),
    });
  });

  it("should emit SUCCEEDED event when validation passes", async () => {
    mockValidate.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/svl-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-success", {
      transformedOutputS3Uri: "s3://bucket/transformed.zip",
      county: "valid-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-success",
    });

    await handler(event);

    // Should have 2 emitWorkflowEvent calls: IN_PROGRESS and SUCCEEDED
    expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(2);

    // Second call should be SUCCEEDED
    expect(mockEmitWorkflowEvent).toHaveBeenNthCalledWith(2, {
      executionId: "exec-success",
      county: "valid-county",
      status: "SUCCEEDED",
      phase: "SVL",
      step: "SVL",
      log: expect.any(Function),
    });
  });

  it("should emit FAILED event with SVL_FAILED on general failure", async () => {
    mockValidate.mockRejectedValue(new Error("Validation process crashed"));

    const { handler } = await import(
      "../../../../workflow/lambdas/svl-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-error", {
      transformedOutputS3Uri: "s3://bucket/transformed.zip",
      county: "error-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-error",
    });

    await handler(event);

    // Should have 2 emitWorkflowEvent calls: IN_PROGRESS and FAILED
    expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(2);

    // Second call should be FAILED with SVL_FAILED
    expect(mockEmitWorkflowEvent).toHaveBeenNthCalledWith(2, {
      executionId: "exec-error",
      county: "error-county",
      status: "FAILED",
      phase: "SVL",
      step: "SVL",
      taskToken: "task-token-error",
      errors: [{ code: "SVL_FAILED", details: expect.any(Object) }],
      log: expect.any(Function),
    });
  });

  it("should call executeWithTaskToken with result on success", async () => {
    mockValidate.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/svl-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-result", {
      transformedOutputS3Uri: "s3://bucket/transformed.zip",
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
    mockValidate.mockRejectedValue(new Error("Validation crashed"));

    const { handler } = await import(
      "../../../../workflow/lambdas/svl-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-throw", {
      transformedOutputS3Uri: "s3://bucket/transformed.zip",
      county: "throw-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-throw",
    });

    await handler(event);

    // Should have been called with workerFn that throws
    const lastCall =
      mockExecuteWithTaskToken.mock.calls[
        mockExecuteWithTaskToken.mock.calls.length - 1
      ];
    expect(lastCall[0].taskToken).toBe("task-token-throw");

    // The workerFn should throw when called
    await expect(lastCall[0].workerFn()).rejects.toThrow();
  });

  it("should create logger with correct base fields", async () => {
    mockValidate.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/svl-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-logger", {
      transformedOutputS3Uri: "s3://bucket/transformed.zip",
      county: "logger-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-logger",
    });

    await handler(event);

    expect(mockCreateLogger).toHaveBeenCalledWith({
      component: "svl-worker",
      at: expect.any(String),
      county: "logger-county",
      executionId: "exec-logger",
    });
  });

  it("should download transformed output from S3", async () => {
    mockValidate.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/svl-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-download", {
      transformedOutputS3Uri: "s3://bucket/transformed.zip",
      county: "download-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-download",
    });

    await handler(event);

    // Should have called downloadS3Object once
    expect(mockDownloadS3Object).toHaveBeenCalledTimes(1);
  });
});
