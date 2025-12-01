import { describe, it, expect, beforeEach, vi } from "vitest";

describe("s3-utils", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  describe("parseS3Uri", () => {
    it("should parse valid S3 URI", async () => {
      const { parseS3Uri } = await import(
        "../../../../workflow/lambdas/shared/s3-utils.mjs"
      );

      const result = parseS3Uri("s3://my-bucket/path/to/file.zip");

      expect(result).toEqual({
        bucket: "my-bucket",
        key: "path/to/file.zip",
      });
    });

    it("should parse S3 URI with simple key", async () => {
      const { parseS3Uri } = await import(
        "../../../../workflow/lambdas/shared/s3-utils.mjs"
      );

      const result = parseS3Uri("s3://bucket/file.txt");

      expect(result).toEqual({
        bucket: "bucket",
        key: "file.txt",
      });
    });

    it("should parse S3 URI with deeply nested key", async () => {
      const { parseS3Uri } = await import(
        "../../../../workflow/lambdas/shared/s3-utils.mjs"
      );

      const result = parseS3Uri("s3://bucket/a/b/c/d/e/file.zip");

      expect(result).toEqual({
        bucket: "bucket",
        key: "a/b/c/d/e/file.zip",
      });
    });

    it("should throw error for invalid S3 URI", async () => {
      const { parseS3Uri } = await import(
        "../../../../workflow/lambdas/shared/s3-utils.mjs"
      );

      expect(() => parseS3Uri("https://bucket/key")).toThrow("Bad S3 URI");
    });

    it("should throw error for URI without s3:// prefix", async () => {
      const { parseS3Uri } = await import(
        "../../../../workflow/lambdas/shared/s3-utils.mjs"
      );

      expect(() => parseS3Uri("bucket/key")).toThrow("Bad S3 URI");
    });

    it("should throw error for malformed URI", async () => {
      const { parseS3Uri } = await import(
        "../../../../workflow/lambdas/shared/s3-utils.mjs"
      );

      expect(() => parseS3Uri("s3://")).toThrow("Bad S3 URI");
    });
  });

  describe("buildS3Uri", () => {
    it("should build S3 URI from bucket and key", async () => {
      const { buildS3Uri } = await import(
        "../../../../workflow/lambdas/shared/s3-utils.mjs"
      );

      const result = buildS3Uri("my-bucket", "path/to/file.zip");

      expect(result).toBe("s3://my-bucket/path/to/file.zip");
    });

    it("should handle simple key", async () => {
      const { buildS3Uri } = await import(
        "../../../../workflow/lambdas/shared/s3-utils.mjs"
      );

      const result = buildS3Uri("bucket", "file.txt");

      expect(result).toBe("s3://bucket/file.txt");
    });
  });

  // Note: downloadS3Object and uploadToS3 require integration testing
  // because the S3Client is instantiated at module level in the workspace package.
  // The mocks work in the worker tests where we mock the entire shared module.
});
