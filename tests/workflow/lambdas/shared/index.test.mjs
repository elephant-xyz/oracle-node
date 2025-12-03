import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

describe("shared index utilities", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.resetModules();
    process.env = { ...originalEnv };
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  describe("requireEnv", () => {
    it("should return environment variable value when present", async () => {
      process.env.TEST_VAR = "test-value";

      const { requireEnv } = await import(
        "../../../../workflow/lambdas/shared/index.mjs"
      );

      expect(requireEnv("TEST_VAR")).toBe("test-value");
    });

    it("should throw error when environment variable is missing", async () => {
      delete process.env.MISSING_VAR;

      const { requireEnv } = await import(
        "../../../../workflow/lambdas/shared/index.mjs"
      );

      expect(() => requireEnv("MISSING_VAR")).toThrow(
        "MISSING_VAR is required",
      );
    });

    it("should throw error when environment variable is empty string", async () => {
      process.env.EMPTY_VAR = "";

      const { requireEnv } = await import(
        "../../../../workflow/lambdas/shared/index.mjs"
      );

      expect(() => requireEnv("EMPTY_VAR")).toThrow("EMPTY_VAR is required");
    });
  });

  describe("createLogger", () => {
    it("should create logger with base fields", async () => {
      const { createLogger } = await import(
        "../../../../workflow/lambdas/shared/index.mjs"
      );

      const log = createLogger({
        component: "test-component",
        executionId: "exec-123",
      });

      log("info", "test_message", { extra: "data" });

      expect(console.log).toHaveBeenCalledWith({
        component: "test-component",
        executionId: "exec-123",
        level: "info",
        msg: "test_message",
        extra: "data",
      });
    });

    it("should use console.error for error level", async () => {
      const { createLogger } = await import(
        "../../../../workflow/lambdas/shared/index.mjs"
      );

      const log = createLogger({ component: "test" });

      log("error", "error_message", { errorCode: 500 });

      expect(console.error).toHaveBeenCalledWith({
        component: "test",
        level: "error",
        msg: "error_message",
        errorCode: 500,
      });
    });

    it("should use console.log for debug level", async () => {
      const { createLogger } = await import(
        "../../../../workflow/lambdas/shared/index.mjs"
      );

      const log = createLogger({ component: "test" });

      log("debug", "debug_message", {});

      expect(console.log).toHaveBeenCalledWith({
        component: "test",
        level: "debug",
        msg: "debug_message",
      });
    });

    it("should work without additional details", async () => {
      const { createLogger } = await import(
        "../../../../workflow/lambdas/shared/index.mjs"
      );

      const log = createLogger({ component: "test" });

      log("info", "simple_message");

      expect(console.log).toHaveBeenCalledWith({
        component: "test",
        level: "info",
        msg: "simple_message",
      });
    });

    it("should merge base fields with details", async () => {
      const { createLogger } = await import(
        "../../../../workflow/lambdas/shared/index.mjs"
      );

      const log = createLogger({
        component: "worker",
        county: "test-county",
        executionId: "exec-456",
      });

      log("info", "operation_start", {
        operation: "transform",
        inputFile: "input.zip",
      });

      expect(console.log).toHaveBeenCalledWith({
        component: "worker",
        county: "test-county",
        executionId: "exec-456",
        level: "info",
        msg: "operation_start",
        operation: "transform",
        inputFile: "input.zip",
      });
    });
  });

  describe("exports", () => {
    it("should export all shared utilities", async () => {
      const shared = await import(
        "../../../../workflow/lambdas/shared/index.mjs"
      );

      // Task token utilities
      expect(shared.sendTaskSuccess).toBeDefined();
      expect(shared.sendTaskFailure).toBeDefined();
      expect(shared.executeWithTaskToken).toBeDefined();

      // S3 utilities
      expect(shared.parseS3Uri).toBeDefined();
      expect(shared.buildS3Uri).toBeDefined();
      expect(shared.downloadS3Object).toBeDefined();
      expect(shared.uploadToS3).toBeDefined();
      expect(shared.s3).toBeDefined();

      // EventBridge utilities
      expect(shared.emitWorkflowEvent).toBeDefined();
      expect(shared.createWorkflowError).toBeDefined();

      // Index utilities
      expect(shared.requireEnv).toBeDefined();
      expect(shared.createLogger).toBeDefined();
    });
  });
});
