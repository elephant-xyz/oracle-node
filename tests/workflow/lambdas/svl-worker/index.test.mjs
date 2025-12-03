import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { promises as fs } from "fs";
import path from "path";
import os from "os";

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
const mockCreateErrorHash = vi.fn(
  (message, path, county) => `hash_${message}_${path}_${county}`,
);
vi.mock("shared", () => ({
  executeWithTaskToken: mockExecuteWithTaskToken,
  parseS3Uri: mockParseS3Uri,
  downloadS3Object: mockDownloadS3Object,
  uploadToS3: mockUploadToS3,
  createLogger: mockCreateLogger,
  emitWorkflowEvent: mockEmitWorkflowEvent,
  createErrorHash: mockCreateErrorHash,
}));

// Mock @elephant-xyz/cli/lib
const mockValidate = vi.fn();
vi.mock("@elephant-xyz/cli/lib", () => ({
  validate: mockValidate,
}));

// Mock csv-parse/sync
const mockParse = vi.fn(() => [{ error_message: "test error" }]);
vi.mock("csv-parse/sync", () => ({
  parse: mockParse,
}));

describe("svl-worker handler", () => {
  const originalEnv = process.env;
  let tmpDir;

  beforeEach(async () => {
    vi.clearAllMocks();

    process.env = {
      ...originalEnv,
      ERRORS_TABLE_NAME: "test-errors-table",
    };

    // Create a temp directory for tests that need file system operations
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "svl-test-"));

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

  afterEach(async () => {
    process.env = originalEnv;
    vi.restoreAllMocks();
    // Clean up temp directory
    if (tmpDir) {
      await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
    }
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

  const createDirectInvocationEvent = (input) => ({
    ...input,
    directInvocation: true,
  });

  describe("SQS trigger mode", () => {
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

    it("should only emit IN_PROGRESS event when validation passes (SUCCEEDED is emitted by state machine)", async () => {
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

      // Should have only 1 emitWorkflowEvent call: IN_PROGRESS
      // SUCCEEDED event is now emitted by the state machine, not the worker
      expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(1);

      // Should be IN_PROGRESS
      expect(mockEmitWorkflowEvent).toHaveBeenCalledWith({
        executionId: "exec-success",
        county: "valid-county",
        status: "IN_PROGRESS",
        phase: "SVL",
        step: "SVL",
        taskToken: "task-token-success",
        log: expect.any(Function),
      });
    });

    it("should return svlErrors when validation fails with errors file (FAILED event emitted by state machine)", async () => {
      // Mock validation failure with errors file existing
      mockValidate.mockImplementation(async ({ cwd }) => {
        // Create submit_errors.csv in the cwd
        const errorsPath = path.join(cwd, "submit_errors.csv");
        await fs.writeFile(
          errorsPath,
          "error_message,error_path\nTest error,$.field",
        );
        return { success: false, error: "Validation errors found" };
      });

      mockParse.mockReturnValue([
        { error_message: "Test error", error_path: "$.field" },
      ]);

      const { handler } = await import(
        "../../../../workflow/lambdas/svl-worker/index.mjs"
      );

      const event = createSqsEvent("task-token-failed", {
        transformedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "failed-county",
        outputPrefix: "s3://output-bucket/outputs/",
        executionId: "exec-failed",
      });

      await handler(event);

      // Should have only 1 emitWorkflowEvent call: IN_PROGRESS
      // FAILED event is now emitted by the state machine with structured errors
      expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(1);

      // Should have called executeWithTaskToken with result containing svlErrors
      expect(mockExecuteWithTaskToken).toHaveBeenCalledWith({
        taskToken: "task-token-failed",
        log: expect.any(Function),
        workerFn: expect.any(Function),
      });

      // Verify the workerFn returns the result with svlErrors
      const workerFn = mockExecuteWithTaskToken.mock.calls[0][0].workerFn;
      const result = await workerFn();
      expect(result).toEqual({
        validatedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "failed-county",
        executionId: "exec-failed",
        validationPassed: false,
        errorsS3Uri: expect.any(String),
        svlErrors: [
          {
            error_message: "Test error",
            error_path: "$.field",
            error_hash: "hash_Test error_$.field_failed-county",
          },
        ],
      });

      // Should have uploaded errors to S3
      expect(mockUploadToS3).toHaveBeenCalled();
    });

    it("should include data_group_cid in svlErrors when present in CSV", async () => {
      // Mock validation failure with errors file containing data_group_cid
      mockValidate.mockImplementation(async ({ cwd }) => {
        const errorsPath = path.join(cwd, "submit_errors.csv");
        await fs.writeFile(
          errorsPath,
          "error_message,error_path,data_group_cid\nTest error,$.field,bafkreitest123",
        );
        return { success: false, error: "Validation errors found" };
      });

      mockParse.mockReturnValue([
        {
          error_message: "Test error",
          error_path: "$.field",
          data_group_cid: "bafkreitest123",
        },
      ]);

      const { handler } = await import(
        "../../../../workflow/lambdas/svl-worker/index.mjs"
      );

      const event = createSqsEvent("task-token-with-cid", {
        transformedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "cid-county",
        outputPrefix: "s3://output-bucket/outputs/",
        executionId: "exec-with-cid",
      });

      await handler(event);

      // Verify the workerFn returns the result with svlErrors including data_group_cid
      const workerFn = mockExecuteWithTaskToken.mock.calls[0][0].workerFn;
      const result = await workerFn();
      expect(result.svlErrors).toEqual([
        {
          error_message: "Test error",
          error_path: "$.field",
          data_group_cid: "bafkreitest123",
          error_hash: "hash_Test error_$.field_cid-county",
        },
      ]);
    });

    it("should omit data_group_cid from svlErrors when not present in CSV", async () => {
      // Mock validation failure with errors file without data_group_cid
      mockValidate.mockImplementation(async ({ cwd }) => {
        const errorsPath = path.join(cwd, "submit_errors.csv");
        await fs.writeFile(
          errorsPath,
          "error_message,error_path\nTest error,$.field",
        );
        return { success: false, error: "Validation errors found" };
      });

      mockParse.mockReturnValue([
        { error_message: "Test error", error_path: "$.field" },
      ]);

      const { handler } = await import(
        "../../../../workflow/lambdas/svl-worker/index.mjs"
      );

      const event = createSqsEvent("task-token-no-cid", {
        transformedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "no-cid-county",
        outputPrefix: "s3://output-bucket/outputs/",
        executionId: "exec-no-cid",
      });

      await handler(event);

      // Verify the workerFn returns the result with svlErrors without data_group_cid
      const workerFn = mockExecuteWithTaskToken.mock.calls[0][0].workerFn;
      const result = await workerFn();
      expect(result.svlErrors).toEqual([
        {
          error_message: "Test error",
          error_path: "$.field",
          error_hash: "hash_Test error_$.field_no-cid-county",
        },
      ]);
      // Verify data_group_cid is not present as a key
      expect(result.svlErrors[0]).not.toHaveProperty("data_group_cid");
    });

    it("should only emit IN_PROGRESS event on general failure (FAILED is emitted by state machine)", async () => {
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

      // Should have only 1 emitWorkflowEvent call: IN_PROGRESS
      // FAILED event is now emitted by the state machine's WaitForSvlExceptionResolution state
      expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(1);
      expect(mockEmitWorkflowEvent).toHaveBeenCalledWith({
        executionId: "exec-error",
        county: "error-county",
        status: "IN_PROGRESS",
        phase: "SVL",
        step: "SVL",
        taskToken: "task-token-error",
        log: expect.any(Function),
      });
    });

    it("should only emit IN_PROGRESS event when validation fails without errors file (FAILED is emitted by state machine)", async () => {
      // Mock validation failure without errors file
      mockValidate.mockResolvedValue({
        success: false,
        error: "Schema validation failed",
      });

      const { handler } = await import(
        "../../../../workflow/lambdas/svl-worker/index.mjs"
      );

      const event = createSqsEvent("task-token-no-errors-file", {
        transformedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "no-errors-county",
        outputPrefix: "s3://output-bucket/outputs/",
        executionId: "exec-no-errors",
      });

      await handler(event);

      // Should have only 1 emitWorkflowEvent call: IN_PROGRESS
      // FAILED event is now emitted by the state machine's WaitForSvlExceptionResolution state
      expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(1);
      expect(mockEmitWorkflowEvent).toHaveBeenCalledWith({
        executionId: "exec-no-errors",
        county: "no-errors-county",
        status: "IN_PROGRESS",
        phase: "SVL",
        step: "SVL",
        taskToken: "task-token-no-errors-file",
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

      // Verify the workerFn returns the expected result
      const lastCall = mockExecuteWithTaskToken.mock.calls[0];
      const result = await lastCall[0].workerFn();
      expect(result).toEqual({
        validatedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "result-county",
        executionId: "exec-result",
        validationPassed: true,
        svlErrors: [],
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

    it("should handle multiple SQS records", async () => {
      mockValidate.mockResolvedValue({ success: true });

      const { handler } = await import(
        "../../../../workflow/lambdas/svl-worker/index.mjs"
      );

      const event = {
        Records: [
          {
            body: JSON.stringify({
              taskToken: "task-token-1",
              input: {
                transformedOutputS3Uri: "s3://bucket/transformed1.zip",
                county: "county-1",
                outputPrefix: "s3://output-bucket/outputs/",
                executionId: "exec-1",
              },
            }),
          },
          {
            body: JSON.stringify({
              taskToken: "task-token-2",
              input: {
                transformedOutputS3Uri: "s3://bucket/transformed2.zip",
                county: "county-2",
                outputPrefix: "s3://output-bucket/outputs/",
                executionId: "exec-2",
              },
            }),
          },
        ],
      };

      await handler(event);

      // Should have processed both records (2 IN_PROGRESS events only)
      // SUCCEEDED events are now emitted by the state machine
      expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(2);
      expect(mockExecuteWithTaskToken).toHaveBeenCalledTimes(2);
    });

    it("should handle error upload failure gracefully", async () => {
      // Mock validation failure with errors file, but upload fails
      mockValidate.mockImplementation(async ({ cwd }) => {
        const errorsPath = path.join(cwd, "submit_errors.csv");
        await fs.writeFile(errorsPath, "error_message\nTest error");
        return { success: false, error: "Validation errors found" };
      });

      mockUploadToS3.mockRejectedValue(new Error("S3 upload failed"));

      const { handler } = await import(
        "../../../../workflow/lambdas/svl-worker/index.mjs"
      );

      const event = createSqsEvent("task-token-upload-fail", {
        transformedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "upload-fail-county",
        outputPrefix: "s3://output-bucket/outputs/",
        executionId: "exec-upload-fail",
      });

      await handler(event);

      // Should have only 1 emitWorkflowEvent call: IN_PROGRESS
      // FAILED event is now emitted by the state machine's WaitForSvlExceptionResolution state
      expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(1);
      expect(mockEmitWorkflowEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          status: "IN_PROGRESS",
        }),
      );
    });
  });

  describe("Direct invocation mode", () => {
    it("should skip EventBridge events in direct invocation mode", async () => {
      mockValidate.mockResolvedValue({ success: true });

      const { handler } = await import(
        "../../../../workflow/lambdas/svl-worker/index.mjs"
      );

      const event = createDirectInvocationEvent({
        transformedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "direct-county",
        outputPrefix: "s3://output-bucket/outputs/",
        executionId: "exec-direct",
      });

      const result = await handler(event);

      // Should NOT emit any workflow events
      expect(mockEmitWorkflowEvent).not.toHaveBeenCalled();

      // Should NOT call executeWithTaskToken
      expect(mockExecuteWithTaskToken).not.toHaveBeenCalled();

      // Should return the result directly
      expect(result).toEqual({
        validatedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "direct-county",
        executionId: "exec-direct",
        validationPassed: true,
        svlErrors: [],
      });
    });

    it("should throw SvlValidationError when validation fails in direct mode", async () => {
      // Mock validation failure with errors file existing
      mockValidate.mockImplementation(async ({ cwd }) => {
        const errorsPath = path.join(cwd, "submit_errors.csv");
        await fs.writeFile(
          errorsPath,
          "error_message,error_path\nTest error,$.field",
        );
        return { success: false, error: "Validation errors found" };
      });

      mockParse.mockReturnValue([
        { error_message: "Test error", error_path: "$.field" },
      ]);

      const { handler } = await import(
        "../../../../workflow/lambdas/svl-worker/index.mjs"
      );

      const event = createDirectInvocationEvent({
        transformedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "direct-fail-county",
        outputPrefix: "s3://output-bucket/outputs/",
        executionId: "exec-direct-fail",
      });

      await expect(handler(event)).rejects.toThrow("SVL validation failed");

      // Should NOT emit any workflow events
      expect(mockEmitWorkflowEvent).not.toHaveBeenCalled();
    });

    it("should create logger with direct invocation context", async () => {
      mockValidate.mockResolvedValue({ success: true });

      const { handler } = await import(
        "../../../../workflow/lambdas/svl-worker/index.mjs"
      );

      const event = createDirectInvocationEvent({
        transformedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "direct-logger-county",
        outputPrefix: "s3://output-bucket/outputs/",
        executionId: "exec-direct-logger",
      });

      await handler(event);

      expect(mockCreateLogger).toHaveBeenCalledWith({
        component: "svl-worker",
        at: expect.any(String),
        county: "direct-logger-county",
        executionId: "exec-direct-logger",
      });
    });

    it("should propagate errors with errorsS3Uri attached", async () => {
      mockValidate.mockImplementation(async ({ cwd }) => {
        const errorsPath = path.join(cwd, "submit_errors.csv");
        await fs.writeFile(errorsPath, "error_message\nTest error");
        return { success: false, error: "Validation errors found" };
      });

      const { handler } = await import(
        "../../../../workflow/lambdas/svl-worker/index.mjs"
      );

      const event = createDirectInvocationEvent({
        transformedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "errors-uri-county",
        outputPrefix: "s3://output-bucket/outputs/",
        executionId: "exec-errors-uri",
      });

      try {
        await handler(event);
        expect.fail("Should have thrown");
      } catch (err) {
        expect(err.name).toBe("SvlValidationError");
        expect(err.message).toContain("SVL validation failed");
        expect(err.errorsS3Uri).toContain("svl_errors.csv");
      }
    });

    it("should throw general errors in direct mode when validation crashes", async () => {
      mockValidate.mockRejectedValue(new Error("Unexpected validation crash"));

      const { handler } = await import(
        "../../../../workflow/lambdas/svl-worker/index.mjs"
      );

      const event = createDirectInvocationEvent({
        transformedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "crash-county",
        outputPrefix: "s3://output-bucket/outputs/",
        executionId: "exec-crash",
      });

      await expect(handler(event)).rejects.toThrow(
        "Unexpected validation crash",
      );
    });
  });

  describe("csvToJson function behavior", () => {
    it("should parse CSV content correctly", async () => {
      mockValidate.mockImplementation(async ({ cwd }) => {
        const errorsPath = path.join(cwd, "submit_errors.csv");
        await fs.writeFile(
          errorsPath,
          "error_message,error_path\nError 1,$.path1\nError 2,$.path2",
        );
        return { success: false, error: "Validation errors found" };
      });

      mockParse.mockReturnValue([
        { error_message: "Error 1", error_path: "$.path1" },
        { error_message: "Error 2", error_path: "$.path2" },
      ]);

      const { handler } = await import(
        "../../../../workflow/lambdas/svl-worker/index.mjs"
      );

      const event = createSqsEvent("task-token-csv", {
        transformedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "csv-county",
        outputPrefix: "s3://output-bucket/outputs/",
        executionId: "exec-csv",
      });

      await handler(event);

      // csv-parse should have been called
      expect(mockParse).toHaveBeenCalled();
    });
  });

  describe("Error handling edge cases", () => {
    it("should handle validation result with no error message", async () => {
      mockValidate.mockResolvedValue({
        success: false,
        // No error message provided
      });

      const { handler } = await import(
        "../../../../workflow/lambdas/svl-worker/index.mjs"
      );

      const event = createSqsEvent("task-token-no-msg", {
        transformedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "no-msg-county",
        outputPrefix: "s3://output-bucket/outputs/",
        executionId: "exec-no-msg",
      });

      await handler(event);

      // Should have only 1 emitWorkflowEvent call: IN_PROGRESS
      // FAILED event is now emitted by the state machine's WaitForSvlExceptionResolution state
      expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(1);
      expect(mockEmitWorkflowEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          status: "IN_PROGRESS",
        }),
      );
    });

    it("should clean up temp directory even on error", async () => {
      mockValidate.mockRejectedValue(new Error("Validation crashed"));

      const { handler } = await import(
        "../../../../workflow/lambdas/svl-worker/index.mjs"
      );

      const event = createSqsEvent("task-token-cleanup", {
        transformedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "cleanup-county",
        outputPrefix: "s3://output-bucket/outputs/",
        executionId: "exec-cleanup",
      });

      await handler(event);

      // Handler should complete without throwing (cleanup is silent)
      // Should have only 1 emitWorkflowEvent call: IN_PROGRESS
      expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(1);
    });
  });

  describe("Error hash functionality", () => {
    it("should compute error_hash for each validation error using createErrorHash", async () => {
      mockValidate.mockImplementation(async ({ cwd }) => {
        const errorsPath = path.join(cwd, "submit_errors.csv");
        await fs.writeFile(
          errorsPath,
          "error_message,error_path\nError A,$.pathA\nError B,$.pathB",
        );
        return { success: false, error: "Validation errors found" };
      });

      mockParse.mockReturnValue([
        { error_message: "Error A", error_path: "$.pathA" },
        { error_message: "Error B", error_path: "$.pathB" },
      ]);

      const { handler } = await import(
        "../../../../workflow/lambdas/svl-worker/index.mjs"
      );

      const event = createSqsEvent("task-token-hash", {
        transformedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "hash-county",
        outputPrefix: "s3://output-bucket/outputs/",
        executionId: "exec-hash",
      });

      await handler(event);

      // Verify createErrorHash was called for each error with correct parameters
      expect(mockCreateErrorHash).toHaveBeenCalledWith(
        "Error A",
        "$.pathA",
        "hash-county",
      );
      expect(mockCreateErrorHash).toHaveBeenCalledWith(
        "Error B",
        "$.pathB",
        "hash-county",
      );

      // Verify the workerFn returns svlErrors with error_hash
      const workerFn = mockExecuteWithTaskToken.mock.calls[0][0].workerFn;
      const result = await workerFn();
      expect(result.svlErrors).toHaveLength(2);
      expect(result.svlErrors[0]).toHaveProperty("error_hash");
      expect(result.svlErrors[1]).toHaveProperty("error_hash");
      expect(result.svlErrors[0].error_hash).toBe(
        "hash_Error A_$.pathA_hash-county",
      );
      expect(result.svlErrors[1].error_hash).toBe(
        "hash_Error B_$.pathB_hash-county",
      );
    });

    it("should generate deterministic error_hash for consistent error identification", async () => {
      mockValidate.mockImplementation(async ({ cwd }) => {
        const errorsPath = path.join(cwd, "submit_errors.csv");
        await fs.writeFile(
          errorsPath,
          "error_message,error_path\nDuplicate error,$.same.path",
        );
        return { success: false, error: "Validation errors found" };
      });

      mockParse.mockReturnValue([
        { error_message: "Duplicate error", error_path: "$.same.path" },
      ]);

      const { handler } = await import(
        "../../../../workflow/lambdas/svl-worker/index.mjs"
      );

      const event = createSqsEvent("task-token-deterministic", {
        transformedOutputS3Uri: "s3://bucket/transformed.zip",
        county: "deterministic-county",
        outputPrefix: "s3://output-bucket/outputs/",
        executionId: "exec-deterministic",
      });

      await handler(event);

      const workerFn = mockExecuteWithTaskToken.mock.calls[0][0].workerFn;
      const result = await workerFn();

      // Verify the hash is deterministic based on message, path, and county
      expect(result.svlErrors[0].error_hash).toBe(
        "hash_Duplicate error_$.same.path_deterministic-county",
      );
    });
  });
});
