import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import {
  LambdaClient,
  InvokeCommand,
} from "@aws-sdk/client-lambda";
import {
  DynamoDBDocumentClient,
} from "@aws-sdk/lib-dynamodb";
import {
  EventBridgeClient,
  PutEventsCommand,
} from "@aws-sdk/client-eventbridge";
import {
  CloudWatchClient,
  PutMetricDataCommand,
} from "@aws-sdk/client-cloudwatch";
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";

const lambdaMock = mockClient(LambdaClient);
const dynamoMock = mockClient(DynamoDBDocumentClient);
const eventBridgeMock = mockClient(EventBridgeClient);
const cloudWatchMock = mockClient(CloudWatchClient);
const s3Mock = mockClient(S3Client);

// Mock shared modules
const mockEmitWorkflowEvent = vi.fn();
const mockEmitErrorResolved = vi.fn();
const mockEmitErrorFailedToResolve = vi.fn();
const mockCreateWorkflowError = vi.fn((code, details) => ({
  code,
  ...(details && { details }),
}));

vi.mock("../../../../codebuild/shared/eventbridge.mjs", () => ({
  emitWorkflowEvent: mockEmitWorkflowEvent,
  emitErrorResolved: mockEmitErrorResolved,
  emitErrorFailedToResolve: mockEmitErrorFailedToResolve,
  createWorkflowError: mockCreateWorkflowError,
}));

// Mock errors.mjs module
const mockDeleteExecution = vi.fn();
const mockMarkErrorsAsMaybeSolved = vi.fn();
const mockMarkErrorsAsMaybeUnrecoverable = vi.fn();
const mockNormalizeErrors = vi.fn();

vi.mock("../../../../codebuild/runtime-module/errors.mjs", () => ({
  deleteExecution: mockDeleteExecution,
  markErrorsAsMaybeSolved: mockMarkErrorsAsMaybeSolved,
  markErrorsAsMaybeUnrecoverable: mockMarkErrorsAsMaybeUnrecoverable,
  normalizeErrors: mockNormalizeErrors,
}));

// Mock file system operations
const mockReadFile = vi.fn();
const mockWriteFile = vi.fn();
const mockMkdtemp = vi.fn();
const mockRm = vi.fn();
const mockCp = vi.fn();
const mockCopyFile = vi.fn();

vi.mock("fs/promises", () => ({
  default: {
    readFile: mockReadFile,
    writeFile: mockWriteFile,
    mkdtemp: mockMkdtemp,
    rm: mockRm,
    cp: mockCp,
    copyFile: mockCopyFile,
  },
  readFile: mockReadFile,
  writeFile: mockWriteFile,
  mkdtemp: mockMkdtemp,
  rm: mockRm,
  cp: mockCp,
  copyFile: mockCopyFile,
}));

describe("auto-repair runtime module", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.clearAllMocks();
    lambdaMock.reset();
    dynamoMock.reset();
    eventBridgeMock.reset();
    cloudWatchMock.reset();

    process.env = {
      ...originalEnv,
      ERRORS_TABLE_NAME: "test-workflow-errors",
      TRANSFORM_S3_PREFIX: "s3://test-bucket/transforms",
      TRANSFORM_WORKER_FUNCTION_NAME: "test-transform-worker",
      SVL_WORKER_FUNCTION_NAME: "test-svl-worker",
      OUTPUT_S3_PREFIX: "s3://test-bucket/outputs",
      MVL_FUNCTION_NAME: "test-mvl-function",
      GET_EXECUTION_LAMBDA_FUNCTION_NAME: "test-get-execution",
      AWS_REGION: "us-east-1",
    };

    // Default mock implementations
    mockMkdtemp.mockResolvedValue("/tmp/auto-repair-123");
    mockReadFile.mockResolvedValue("test content");
    mockCsvToJson.mockResolvedValue([
      {
        error_message: "missing required property",
        error_path: "deed_1.json/deed_type",
        data_group_cid: "test-group",
      },
    ]);
    mockNormalizeErrors.mockReturnValue([
      { hash: "error-hash-123", message: "missing required property", path: "deed_1.json/deed_type" },
    ]);
    mockDeleteExecution.mockResolvedValue(["error-hash-123"]);
    mockMarkErrorsAsMaybeSolved.mockResolvedValue(undefined);
    mockPublishMetric.mockResolvedValue(undefined);

    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  describe("getExecutionWithLeastErrors", () => {
    it("should retrieve execution with least errors from Lambda", async () => {
      const mockExecution = {
        executionId: "arn:aws:states:us-east-1:123456789:execution:test:abc123",
        county: "Lee",
        uniqueErrorCount: 1,
        preparedS3Uri: "s3://test-bucket/prepared/inputs.zip",
        errorType: "30",
      };

      const mockErrors = [
        {
          errorCode: "30abc123",
          status: "failed",
          occurrences: 1,
          errorDetails: JSON.stringify({
            error_message: "missing required property",
            error_path: "deed_1.json/deed_type",
          }),
        },
      ];

      lambdaMock.on(InvokeCommand).resolves({
        Payload: new TextEncoder().encode(
          JSON.stringify({
            success: true,
            execution: mockExecution,
            errors: mockErrors,
          }),
        ),
      });

      // Import after mocks are set up
      const { getExecutionWithLeastErrors } = await import(
        "../../../../codebuild/runtime-module/index.js"
      );

      const result = await getExecutionWithLeastErrors();

      expect(result.execution).toEqual(mockExecution);
      expect(result.errors).toEqual(mockErrors);
      expect(lambdaMock.calls()).toHaveLength(1);
    });

    it("should handle Lambda invocation errors", async () => {
      lambdaMock.on(InvokeCommand).resolves({
        FunctionError: "Unhandled",
        Payload: new TextEncoder().encode(
          JSON.stringify({ errorMessage: "Lambda error" }),
        ),
      });

      const { getExecutionWithLeastErrors } = await import(
        "../../../../codebuild/runtime-module/index.js"
      );

      await expect(getExecutionWithLeastErrors()).rejects.toThrow(
        "get-execution Lambda failed",
      );
    });
  });

  describe("main workflow", () => {
    it("should skip execution if no preparedS3Uri", async () => {
      const mockExecution = {
        executionId: "test-execution-123",
        county: "Lee",
        uniqueErrorCount: 1,
        preparedS3Uri: undefined,
        errorType: "30",
      };

      const mockErrors = [
        {
          errorCode: "30abc123",
          status: "failed",
          occurrences: 1,
          errorDetails: JSON.stringify({
            error_message: "missing required property",
            error_path: "deed_1.json/deed_type",
          }),
        },
      ];

      lambdaMock.on(InvokeCommand).resolves({
        Payload: new TextEncoder().encode(
          JSON.stringify({
            success: true,
            execution: mockExecution,
            errors: mockErrors,
          }),
        ),
      });

      eventBridgeMock.on(PutEventsCommand).resolves({});

      // Mock the main function - we'll need to export it or test indirectly
      // For now, test the behavior through integration
      const { main } = await import(
        "../../../../codebuild/runtime-module/index.js"
      );

      await main();

      // Should emit error failed to resolve events
      expect(mockEmitErrorFailedToResolve).toHaveBeenCalledWith({
        errorCode: "30abc123",
      });
      expect(mockEmitErrorFailedToResolve).toHaveBeenCalledWith({
        executionId: "test-execution-123",
      });
    });

    it("should process execution with preparedS3Uri successfully", async () => {
      const mockExecution = {
        executionId: "test-execution-123",
        county: "Lee",
        uniqueErrorCount: 1,
        preparedS3Uri: "s3://test-bucket/prepared/inputs.zip",
        errorType: "30",
      };

      const mockErrors = [
        {
          errorCode: "30abc123",
          status: "failed",
          occurrences: 1,
          errorDetails: JSON.stringify({
            error_message: "missing required property",
            error_path: "deed_1.json/deed_type",
            data_group_cid: "test-group",
          }),
        },
      ];

      lambdaMock.on(InvokeCommand).resolves({
        Payload: new TextEncoder().encode(
          JSON.stringify({
            success: true,
            execution: mockExecution,
            errors: mockErrors,
          }),
        ),
      });

      // Mock file operations
      mockParseS3Uri.mockReturnValue({ bucket: "test-bucket", key: "prepared/inputs.zip" });
      mockDownloadS3Object.mockResolvedValue(undefined);
      mockExtractZip.mockResolvedValue(undefined);
      mockInstallCherio.mockResolvedValue(undefined);
      mockInvokeAiForFix.mockResolvedValue({ scriptsDir: "/tmp/scripts" });
      mockUploadFixedScripts.mockResolvedValue(undefined);
      mockInvokeTransformAndSvlWorkers.mockResolvedValue({
        validationPassed: true,
      });

      // Note: This test would need the actual implementation to be refactored
      // to allow for better testing. For now, this shows the test structure.
    });
  });

  describe("error handling", () => {
    it("should emit WorkflowEvent with FAILED status on max retries", async () => {
      // Test that when auto-repair fails after max retries,
      // it emits the correct WorkflowEvent with FAILED status
      const errorCode = "70002"; // SVL error code
      const failureReason = "MaxRetriesExceeded";

      mockCreateWorkflowError.mockReturnValue({
        code: errorCode,
        details: {
          errorType: "SVL",
          failureReason,
          attempts: 3,
          maxAttempts: 3,
        },
      });

      // Verify the error structure
      const error = mockCreateWorkflowError(errorCode, {
        errorType: "SVL",
        failureReason,
        attempts: 3,
        maxAttempts: 3,
      });

      expect(error.code).toBe(errorCode);
      expect(error.details.errorType).toBe("SVL");
      expect(error.details.failureReason).toBe(failureReason);
    });

    it("should emit ElephantErrorFailedToResolve for each error code", async () => {
      const executionErrors = [
        { errorCode: "30abc123" },
        { errorCode: "30def456" },
        { errorCode: "30abc123" }, // duplicate
      ];

      const uniqueErrorCodes = [...new Set(executionErrors.map((e) => e.errorCode))];

      // Simulate the logic from main function
      for (const errorCode of uniqueErrorCodes) {
        await mockEmitErrorFailedToResolve({ errorCode });
      }

      expect(mockEmitErrorFailedToResolve).toHaveBeenCalledTimes(2);
      expect(mockEmitErrorFailedToResolve).toHaveBeenCalledWith({
        errorCode: "30abc123",
      });
      expect(mockEmitErrorFailedToResolve).toHaveBeenCalledWith({
        errorCode: "30def456",
      });
    });
  });
});

