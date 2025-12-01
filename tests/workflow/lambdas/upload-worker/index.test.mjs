import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

// Mock the shared module
const mockExecuteWithTaskToken = vi.fn();
const mockParseS3Uri = vi.fn((uri) => {
  const match = /^s3:\/\/([^/]+)\/(.*)$/.exec(uri);
  return match ? { bucket: match[1], key: match[2] } : null;
});
const mockDownloadS3Object = vi.fn();
const mockRequireEnv = vi.fn();
const mockCreateLogger = vi.fn(() => vi.fn());
const mockEmitWorkflowEvent = vi.fn();
const mockCreateWorkflowError = vi.fn((code, details) => ({ code, details }));

vi.mock("../../../../workflow/lambdas/upload-worker/shared/index.mjs", () => ({
  executeWithTaskToken: mockExecuteWithTaskToken,
  parseS3Uri: mockParseS3Uri,
  downloadS3Object: mockDownloadS3Object,
  requireEnv: mockRequireEnv,
  createLogger: mockCreateLogger,
  emitWorkflowEvent: mockEmitWorkflowEvent,
  createWorkflowError: mockCreateWorkflowError,
}));

// Mock @elephant-xyz/cli/lib
const mockUpload = vi.fn();
vi.mock("@elephant-xyz/cli/lib", () => ({
  upload: mockUpload,
}));

// Mock adm-zip with a proper class
vi.mock("adm-zip", () => {
  class MockAdmZip {
    constructor() {}
    extractAllTo() {}
    addLocalFolder() {}
    toBuffer() {
      return Buffer.from("mock zip buffer");
    }
  }
  return { default: MockAdmZip };
});

// Mock fs.promises
vi.mock("fs", async () => {
  const actual = await vi.importActual("fs");
  return {
    ...actual,
    promises: {
      mkdtemp: vi.fn().mockResolvedValue("/tmp/upload-test"),
      mkdir: vi.fn().mockResolvedValue(undefined),
      readFile: vi.fn().mockResolvedValue(Buffer.from("mock file content")),
      writeFile: vi.fn().mockResolvedValue(undefined),
      rm: vi.fn().mockResolvedValue(undefined),
    },
  };
});

describe("upload-worker handler", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.clearAllMocks();

    process.env = {
      ...originalEnv,
      ELEPHANT_PINATA_JWT: "test-pinata-jwt-token",
    };

    // Default mock implementations
    mockCreateLogger.mockReturnValue(vi.fn());
    mockExecuteWithTaskToken.mockResolvedValue(undefined);
    mockEmitWorkflowEvent.mockResolvedValue(undefined);
    mockDownloadS3Object.mockResolvedValue(undefined);
    mockRequireEnv.mockImplementation((name) => {
      if (name === "ELEPHANT_PINATA_JWT") {
        return process.env.ELEPHANT_PINATA_JWT;
      }
      throw new Error(`Missing required env: ${name}`);
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
    mockUpload.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/upload-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-123", {
      seedHashZipS3Uri: "s3://bucket/seed_hash.zip",
      countyHashZipS3Uri: "s3://bucket/county_hash.zip",
      county: "test-county",
      executionId: "exec-123",
    });

    await handler(event);

    expect(mockEmitWorkflowEvent).toHaveBeenCalledWith({
      executionId: "exec-123",
      county: "test-county",
      status: "IN_PROGRESS",
      phase: "Upload",
      step: "Upload",
      taskToken: "task-token-123",
      log: expect.any(Function),
    });
  });

  it("should emit SUCCEEDED event on successful upload", async () => {
    mockUpload.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/upload-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-success", {
      seedHashZipS3Uri: "s3://bucket/seed_hash.zip",
      countyHashZipS3Uri: "s3://bucket/county_hash.zip",
      county: "success-county",
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
      phase: "Upload",
      step: "Upload",
      log: expect.any(Function),
    });
  });

  it("should emit FAILED event with UPLOAD_IPFS_FAILED on UploadFailedError", async () => {
    mockUpload.mockResolvedValue({
      success: false,
      errorMessage: "IPFS upload failed",
      errors: ["Network timeout"],
    });

    const { handler } = await import(
      "../../../../workflow/lambdas/upload-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-ipfs-fail", {
      seedHashZipS3Uri: "s3://bucket/seed_hash.zip",
      countyHashZipS3Uri: "s3://bucket/county_hash.zip",
      county: "ipfs-fail-county",
      executionId: "exec-ipfs-fail",
    });

    await handler(event);

    // Should have 2 emitWorkflowEvent calls: IN_PROGRESS and FAILED
    expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(2);

    // Second call should be FAILED with UPLOAD_IPFS_FAILED
    expect(mockEmitWorkflowEvent).toHaveBeenNthCalledWith(2, {
      executionId: "exec-ipfs-fail",
      county: "ipfs-fail-county",
      status: "FAILED",
      phase: "Upload",
      step: "Upload",
      taskToken: "task-token-ipfs-fail",
      errors: [{ code: "UPLOAD_IPFS_FAILED", details: expect.any(Object) }],
      log: expect.any(Function),
    });
  });

  it("should emit FAILED event with UPLOAD_FAILED on general failure", async () => {
    mockDownloadS3Object.mockRejectedValue(new Error("S3 download failed"));

    const { handler } = await import(
      "../../../../workflow/lambdas/upload-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-general-fail", {
      seedHashZipS3Uri: "s3://bucket/seed_hash.zip",
      countyHashZipS3Uri: "s3://bucket/county_hash.zip",
      county: "general-fail-county",
      executionId: "exec-general-fail",
    });

    await handler(event);

    // Should have 2 emitWorkflowEvent calls: IN_PROGRESS and FAILED
    expect(mockEmitWorkflowEvent).toHaveBeenCalledTimes(2);

    // Second call should be FAILED with UPLOAD_FAILED
    expect(mockEmitWorkflowEvent).toHaveBeenNthCalledWith(2, {
      executionId: "exec-general-fail",
      county: "general-fail-county",
      status: "FAILED",
      phase: "Upload",
      step: "Upload",
      taskToken: "task-token-general-fail",
      errors: [{ code: "UPLOAD_FAILED", details: expect.any(Object) }],
      log: expect.any(Function),
    });
  });

  it("should throw error when ELEPHANT_PINATA_JWT is missing", async () => {
    mockRequireEnv.mockImplementation((name) => {
      throw new Error(`Missing required env: ${name}`);
    });

    const { handler } = await import(
      "../../../../workflow/lambdas/upload-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-no-jwt", {
      seedHashZipS3Uri: "s3://bucket/seed_hash.zip",
      countyHashZipS3Uri: "s3://bucket/county_hash.zip",
      county: "no-jwt-county",
      executionId: "exec-no-jwt",
    });

    await handler(event);

    // Should emit FAILED event
    expect(mockEmitWorkflowEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        status: "FAILED",
      }),
    );
  });

  it("should download both hash zips", async () => {
    mockUpload.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/upload-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-parallel", {
      seedHashZipS3Uri: "s3://bucket/seed_hash.zip",
      countyHashZipS3Uri: "s3://bucket/county_hash.zip",
      county: "parallel-county",
      executionId: "exec-parallel",
    });

    await handler(event);

    // Should have made 2 downloadS3Object calls (seed hash + county hash)
    expect(mockDownloadS3Object).toHaveBeenCalledTimes(2);
  });

  it("should call executeWithTaskToken with result on success", async () => {
    mockUpload.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/upload-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-result", {
      seedHashZipS3Uri: "s3://bucket/seed_hash.zip",
      countyHashZipS3Uri: "s3://bucket/county_hash.zip",
      county: "result-county",
      executionId: "exec-result",
    });

    await handler(event);

    expect(mockExecuteWithTaskToken).toHaveBeenCalledWith({
      taskToken: "task-token-result",
      log: expect.any(Function),
      workerFn: expect.any(Function),
    });
  });

  it("should pass pinataJwt to upload function", async () => {
    mockUpload.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/upload-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-jwt", {
      seedHashZipS3Uri: "s3://bucket/seed_hash.zip",
      countyHashZipS3Uri: "s3://bucket/county_hash.zip",
      county: "jwt-county",
      executionId: "exec-jwt",
    });

    await handler(event);

    expect(mockUpload).toHaveBeenCalledWith(
      expect.objectContaining({
        pinataJwt: "test-pinata-jwt-token",
      }),
    );
  });

  it("should create logger with correct base fields", async () => {
    mockUpload.mockResolvedValue({ success: true });

    const { handler } = await import(
      "../../../../workflow/lambdas/upload-worker/index.mjs"
    );

    const event = createSqsEvent("task-token-logger", {
      seedHashZipS3Uri: "s3://bucket/seed_hash.zip",
      countyHashZipS3Uri: "s3://bucket/county_hash.zip",
      county: "logger-county",
      executionId: "exec-logger",
    });

    await handler(event);

    expect(mockCreateLogger).toHaveBeenCalledWith({
      component: "upload-worker",
      at: expect.any(String),
      county: "logger-county",
      executionId: "exec-logger",
    });
  });
});
