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

      const { requireEnv } =
        await import("../../../../workflow/layers/shared/src/index.mjs");

      expect(requireEnv("TEST_VAR")).toBe("test-value");
    });

    it("should throw error when environment variable is missing", async () => {
      delete process.env.MISSING_VAR;

      const { requireEnv } =
        await import("../../../../workflow/layers/shared/src/index.mjs");

      expect(() => requireEnv("MISSING_VAR")).toThrow(
        "MISSING_VAR is required",
      );
    });

    it("should throw error when environment variable is empty string", async () => {
      process.env.EMPTY_VAR = "";

      const { requireEnv } =
        await import("../../../../workflow/layers/shared/src/index.mjs");

      expect(() => requireEnv("EMPTY_VAR")).toThrow("EMPTY_VAR is required");
    });
  });

  describe("createLogger", () => {
    it("should create logger with base fields", async () => {
      const { createLogger } =
        await import("../../../../workflow/layers/shared/src/index.mjs");

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
      const { createLogger } =
        await import("../../../../workflow/layers/shared/src/index.mjs");

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
      const { createLogger } =
        await import("../../../../workflow/layers/shared/src/index.mjs");

      const log = createLogger({ component: "test" });

      log("debug", "debug_message", {});

      expect(console.log).toHaveBeenCalledWith({
        component: "test",
        level: "debug",
        msg: "debug_message",
      });
    });

    it("should work without additional details", async () => {
      const { createLogger } =
        await import("../../../../workflow/layers/shared/src/index.mjs");

      const log = createLogger({ component: "test" });

      log("info", "simple_message");

      expect(console.log).toHaveBeenCalledWith({
        component: "test",
        level: "info",
        msg: "simple_message",
      });
    });

    it("should merge base fields with details", async () => {
      const { createLogger } =
        await import("../../../../workflow/layers/shared/src/index.mjs");

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

  describe("createErrorHash", () => {
    it("should compute deterministic SHA256 hash from message, path, and county", async () => {
      const { createErrorHash } =
        await import("../../../../workflow/layers/shared/src/index.mjs");

      const hash = createErrorHash("Test error", "$.field", "test-county");

      // The hash should be a 64-character hex string (SHA256)
      expect(hash).toMatch(/^[a-f0-9]{64}$/);
    });

    it("should produce same hash for identical inputs", async () => {
      const { createErrorHash } =
        await import("../../../../workflow/layers/shared/src/index.mjs");

      const hash1 = createErrorHash("Error message", "$.path", "county-a");
      const hash2 = createErrorHash("Error message", "$.path", "county-a");

      expect(hash1).toBe(hash2);
    });

    it("should produce different hashes for different messages", async () => {
      const { createErrorHash } =
        await import("../../../../workflow/layers/shared/src/index.mjs");

      const hash1 = createErrorHash("Error A", "$.path", "county");
      const hash2 = createErrorHash("Error B", "$.path", "county");

      expect(hash1).not.toBe(hash2);
    });

    it("should produce different hashes for different paths", async () => {
      const { createErrorHash } =
        await import("../../../../workflow/layers/shared/src/index.mjs");

      const hash1 = createErrorHash("Error", "$.path.a", "county");
      const hash2 = createErrorHash("Error", "$.path.b", "county");

      expect(hash1).not.toBe(hash2);
    });

    it("should produce different hashes for different counties", async () => {
      const { createErrorHash } =
        await import("../../../../workflow/layers/shared/src/index.mjs");

      const hash1 = createErrorHash("Error", "$.path", "county-a");
      const hash2 = createErrorHash("Error", "$.path", "county-b");

      expect(hash1).not.toBe(hash2);
    });

    it("should produce same hash as codebuild/shared/errors.mjs implementation", async () => {
      // This test verifies consistency with the original createErrorHash in errors.mjs
      // Hash is computed as: sha256("message#path#county")
      const { createErrorHash } =
        await import("../../../../workflow/layers/shared/src/index.mjs");
      const { createHash } = await import("crypto");

      const message = "Required field missing";
      const path = "$.properties[0].address";
      const county = "orange";

      const sharedHash = createErrorHash(message, path, county);

      // Compute expected hash using the same algorithm
      const expectedHash = createHash("sha256")
        .update(`${message}#${path}#${county}`, "utf8")
        .digest("hex");

      expect(sharedHash).toBe(expectedHash);
    });
  });

  describe("exports", () => {
    it("should export all shared utilities", async () => {
      const shared =
        await import("../../../../workflow/layers/shared/src/index.mjs");

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
      expect(shared.createErrorHash).toBeDefined();
    });
  });
});
