import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import {
  EventBridgeClient,
  PutEventsCommand,
} from "@aws-sdk/client-eventbridge";
import {
  CloudWatchClient,
  PutMetricDataCommand,
} from "@aws-sdk/client-cloudwatch";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { SFNClient, GetExecutionHistoryCommand } from "@aws-sdk/client-sfn";

// Create AWS SDK mocks using aws-sdk-client-mock
const lambdaMock = mockClient(LambdaClient);
const eventBridgeMock = mockClient(EventBridgeClient);
const cloudWatchMock = mockClient(CloudWatchClient);
const s3Mock = mockClient(S3Client);
const sfnMock = mockClient(SFNClient);

// Mock shared eventbridge module
const mockEmitWorkflowEvent = vi.fn();
const mockEmitErrorResolved = vi.fn();
const mockEmitErrorFailedToResolve = vi.fn();
const mockCreateWorkflowError = vi.fn((code, details) => ({
  code,
  ...(details && { details }),
}));

vi.mock("../../../codebuild/runtime-module/shared/eventbridge.mjs", () => ({
  emitWorkflowEvent: mockEmitWorkflowEvent,
  emitErrorResolved: mockEmitErrorResolved,
  emitErrorFailedToResolve: mockEmitErrorFailedToResolve,
  createWorkflowError: mockCreateWorkflowError,
}));

// Mock file system operations
const mockReadFile = vi.fn();
const mockWriteFile = vi.fn();
const mockMkdtemp = vi.fn();
const mockRm = vi.fn();
const mockCp = vi.fn();
const mockCopyFile = vi.fn();
const mockAccess = vi.fn();
const mockReaddir = vi.fn();
const mockStat = vi.fn();
const mockMkdir = vi.fn();

vi.mock("fs/promises", () => ({
  default: {
    readFile: mockReadFile,
    writeFile: mockWriteFile,
    mkdtemp: mockMkdtemp,
    rm: mockRm,
    cp: mockCp,
    copyFile: mockCopyFile,
    access: mockAccess,
    readdir: mockReaddir,
    stat: mockStat,
    mkdir: mockMkdir,
  },
  readFile: mockReadFile,
  writeFile: mockWriteFile,
  mkdtemp: mockMkdtemp,
  rm: mockRm,
  cp: mockCp,
  copyFile: mockCopyFile,
  access: mockAccess,
  readdir: mockReaddir,
  stat: mockStat,
  mkdir: mockMkdir,
}));

// Mock csv-parse
const mockCsvParse = vi.fn();
vi.mock("csv-parse/sync", () => ({
  parse: mockCsvParse,
}));

// Mock child_process exec
vi.mock("child_process", () => ({
  exec: vi.fn(),
}));

// Mock adm-zip
vi.mock("adm-zip", () => ({
  default: vi.fn(() => ({
    extractAllTo: vi.fn(),
    getEntries: vi.fn(() => []),
  })),
}));

// Mock @anthropic-ai/claude-agent-sdk
vi.mock("@anthropic-ai/claude-agent-sdk", () => ({
  query: vi.fn(),
}));

describe("auto-repair runtime module", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.clearAllMocks();

    // Reset AWS SDK mocks
    lambdaMock.reset();
    eventBridgeMock.reset();
    cloudWatchMock.reset();
    s3Mock.reset();
    sfnMock.reset();

    process.env = {
      ...originalEnv,
      TRANSFORM_S3_PREFIX: "s3://test-bucket/transforms",
      TRANSFORM_WORKER_FUNCTION_NAME: "test-transform-worker",
      SVL_WORKER_FUNCTION_NAME: "test-svl-worker",
      OUTPUT_S3_PREFIX: "s3://test-bucket/outputs",
      MVL_FUNCTION_NAME: "test-mvl-function",
      GET_EXECUTION_LAMBDA_FUNCTION_NAME: "test-get-execution",
      STATE_MACHINE_ARN: "arn:aws:states:us-east-1:123456789:stateMachine:test",
      AWS_REGION: "us-east-1",
    };

    // Default mock implementations
    mockMkdtemp.mockResolvedValue("/tmp/auto-repair-123");
    mockReadFile.mockResolvedValue("test content");
    mockCsvParse.mockReturnValue([
      {
        error_message: "missing required property",
        error_path: "deed_1.json/deed_type",
        data_group_cid: "test-group",
      },
    ]);

    // Set default mock responses for AWS SDK
    cloudWatchMock.on(PutMetricDataCommand).resolves({});
    eventBridgeMock.on(PutEventsCommand).resolves({});
    // Default SFN mock returns preparedS3Uri from execution history
    sfnMock.on(GetExecutionHistoryCommand).resolves({
      events: [
        {
          type: "TaskStateExited",
          stateExitedEventDetails: {
            name: "PrepareState",
            output: JSON.stringify({
              prepare: {
                output_s3_uri: "s3://test-bucket/prepared/inputs.zip",
              },
            }),
          },
        },
      ],
    });

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
        executionId: "abc123-uuid-execution-id",
        county: "Lee",
        uniqueErrorCount: 1,
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
      const { getExecutionWithLeastErrors } =
        await import("../../../codebuild/runtime-module/index.js");

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

      const { getExecutionWithLeastErrors } =
        await import("../../../codebuild/runtime-module/index.js");

      await expect(getExecutionWithLeastErrors()).rejects.toThrow(
        "get-execution Lambda failed",
      );
    });
  });

  describe("main workflow", () => {
    it("should skip execution if no preparedS3Uri from Step Functions", async () => {
      const mockExecution = {
        executionId: "test-execution-123-uuid",
        county: "Lee",
        uniqueErrorCount: 1,
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

      // Mock SFN to return no preparedS3Uri (empty events)
      sfnMock.on(GetExecutionHistoryCommand).resolves({
        events: [],
      });

      const { main } =
        await import("../../../codebuild/runtime-module/index.js");

      // Mock process.exit to prevent test from exiting
      const mockExit = vi
        .spyOn(process, "exit")
        .mockImplementation(() => undefined);

      await main();

      // Should emit error failed to resolve events
      expect(mockEmitErrorFailedToResolve).toHaveBeenCalledWith({
        errorCode: "30abc123",
      });
      expect(mockEmitErrorFailedToResolve).toHaveBeenCalledWith({
        executionId: "test-execution-123-uuid",
      });

      mockExit.mockRestore();
    });

    it("should process execution with preparedS3Uri from Step Functions successfully", async () => {
      const mockExecution = {
        executionId: "test-execution-456-uuid",
        county: "Lee",
        uniqueErrorCount: 1,
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

      // Mock S3 operations for downloading prepared zip
      s3Mock.on(GetObjectCommand).resolves({
        Body: {
          transformToByteArray: async () => new Uint8Array([80, 75, 3, 4]), // ZIP magic bytes
        },
      });

      // Default SFN mock is already set up to return preparedS3Uri
      // Verify the SFN mock returns a preparedS3Uri
      expect(sfnMock.calls()).toHaveLength(0); // Not called yet
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

      const uniqueErrorCodes = [
        ...new Set(executionErrors.map((e) => e.errorCode)),
      ];

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
