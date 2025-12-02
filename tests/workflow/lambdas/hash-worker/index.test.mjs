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

vi.mock("../../../../workflow/lambdas/hash-worker/shared/index.mjs", () => ({
  executeWithTaskToken: mockExecuteWithTaskToken,
  parseS3Uri: mockParseS3Uri,
  downloadS3Object: mockDownloadS3Object,
  uploadToS3: mockUploadToS3,
  createLogger: mockCreateLogger,
  emitWorkflowEvent: mockEmitWorkflowEvent,
  createWorkflowError: mockCreateWorkflowError,
}));

// Mock @elephant-xyz/cli/lib
const mockHash = vi.fn();
vi.mock("@elephant-xyz/cli/lib", () => ({
  hash: mockHash,
}));

// Mock csv-parse/sync
vi.mock("csv-parse/sync", () => ({
  parse: vi.fn(() => [{ propertyCid: "test-cid-123" }]),
}));

// Mock fs.promises
vi.mock("fs", async () => {
  const actual = await vi.importActual("fs");
  return {
    ...actual,
    promises: {
      mkdtemp: vi.fn().mockResolvedValue("/tmp/hash-test"),
      readFile: vi.fn().mockResolvedValue("header,propertyCid\nvalue,test-cid"),
      writeFile: vi.fn().mockResolvedValue(undefined),
      rm: vi.fn().mockResolvedValue(undefined),
    },
  };
});

describe("hash-worker handler", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.clearAllMocks();

    process.env = { ...originalEnv };

    // Default mock implementations
    mockCreateLogger.mockReturnValue(vi.fn());
    mockExecuteWithTaskToken.mockResolvedValue(undefined);
    mockEmitWorkflowEvent.mockResolvedValue(undefined);
    mockDownloadS3Object.mockResolvedValue(undefined);
    mockUploadToS3.mockImplementation((localPath, s3Location) => {
      return Promise.resolve(`s3://${s3Location.bucket}/${s3Location.key}`);
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
    mockHash.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/hash-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-123", {
      validatedOutputS3Uri: "s3://bucket/validated.zip",
      seedOutputS3Uri: "s3://bucket/seed.zip",
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
      phase: "Hash",
      step: "Hash",
      taskToken: "task-token-123",
      log: expect.any(Function),
    });
  });

  it("should only emit IN_PROGRESS event on successful hash generation (SUCCEEDED is emitted by state machine)", async () => {
    mockHash.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/hash-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-success", {
      validatedOutputS3Uri: "s3://bucket/validated.zip",
      seedOutputS3Uri: "s3://bucket/seed.zip",
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
      phase: "Hash",
      step: "Hash",
      taskToken: "task-token-success",
      log: expect.any(Function),
    });
  });

  it("should emit FAILED event with HASH_FAILED on seed hash failure", async () => {
    mockHash.mockResolvedValue({
      success: false,
      error: "Seed hash calculation failed",
    });

    const { handler } = await import(
      "../../../../workflow/lambdas/hash-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-seed-fail", {
      validatedOutputS3Uri: "s3://bucket/validated.zip",
      seedOutputS3Uri: "s3://bucket/seed.zip",
      county: "seed-fail-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-seed-fail",
    });

    await handler(event);

    // Should have 2 emitWorkflowEvent calls: IN_PROGRESS and FAILED
    expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(2);

    // Second call should be FAILED with HASH_FAILED
    expect(mockEmitWorkflowEvent).toHaveBeenNthCalledWith(2, {
      executionId: "exec-seed-fail",
      county: "seed-fail-county",
      status: "FAILED",
      phase: "Hash",
      step: "Hash",
      taskToken: "task-token-seed-fail",
      errors: [{ code: "HASH_FAILED", details: expect.any(Object) }],
      log: expect.any(Function),
    });
  });

  it("should emit FAILED event with HASH_FAILED on county hash failure", async () => {
    // First call (seed) succeeds, second call (county) fails
    mockHash.mockResolvedValueOnce({ success: true }).mockResolvedValueOnce({
      success: false,
      error: "County hash calculation failed",
    });

    const { handler } = await import(
      "../../../../workflow/lambdas/hash-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-county-fail", {
      validatedOutputS3Uri: "s3://bucket/validated.zip",
      seedOutputS3Uri: "s3://bucket/seed.zip",
      county: "county-fail-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-county-fail",
    });

    await handler(event);

    // Second call should be FAILED with HASH_FAILED
    expect(mockEmitWorkflowEvent).toHaveBeenNthCalledWith(2, {
      executionId: "exec-county-fail",
      county: "county-fail-county",
      status: "FAILED",
      phase: "Hash",
      step: "Hash",
      taskToken: "task-token-county-fail",
      errors: [{ code: "HASH_FAILED", details: expect.any(Object) }],
      log: expect.any(Function),
    });
  });

  it("should call executeWithTaskToken with result on success", async () => {
    mockHash.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/hash-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-result", {
      validatedOutputS3Uri: "s3://bucket/validated.zip",
      seedOutputS3Uri: "s3://bucket/seed.zip",
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
    mockHash.mockResolvedValue({
      success: false,
      error: "Hash failed",
    });

    const { handler } = await import(
      "../../../../workflow/lambdas/hash-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-throw", {
      validatedOutputS3Uri: "s3://bucket/validated.zip",
      seedOutputS3Uri: "s3://bucket/seed.zip",
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
    mockHash.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/hash-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-logger", {
      validatedOutputS3Uri: "s3://bucket/validated.zip",
      seedOutputS3Uri: "s3://bucket/seed.zip",
      county: "logger-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-logger",
    });

    await handler(event);

    expect(mockCreateLogger).toHaveBeenCalledWith({
      component: "hash-worker",
      at: expect.any(String),
      county: "logger-county",
      executionId: "exec-logger",
    });
  });

  it("should download both validated and seed outputs", async () => {
    mockHash.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/hash-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-download", {
      validatedOutputS3Uri: "s3://bucket/validated.zip",
      seedOutputS3Uri: "s3://bucket/seed.zip",
      county: "download-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-download",
    });

    await handler(event);

    // Should have called downloadS3Object twice (validated + seed)
    expect(mockDownloadS3Object).toHaveBeenCalledTimes(2);
  });

  it("should upload all artifacts to S3", async () => {
    mockHash.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/hash-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-upload", {
      validatedOutputS3Uri: "s3://bucket/validated.zip",
      seedOutputS3Uri: "s3://bucket/seed.zip",
      county: "upload-county",
      outputPrefix: "s3://output-bucket/outputs/",
      executionId: "exec-upload",
    });

    await handler(event);

    // Should have called uploadToS3 3 times (seed_hash.zip, county_hash.zip, combined_hash.csv)
    expect(mockUploadToS3).toHaveBeenCalledTimes(3);
  });
});
