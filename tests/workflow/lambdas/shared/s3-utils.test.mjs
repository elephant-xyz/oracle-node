import { describe, it, expect, beforeEach, vi, afterEach } from "vitest";
import { promises as fs } from "fs";
import path from "path";
import os from "os";

// Mock send function that will be configured per test
const mockSend = vi.fn();

// Mock the S3Client and commands
vi.mock("@aws-sdk/client-s3", () => {
  // Create a proper class for S3Client
  class MockS3Client {
    constructor() {}
    send = mockSend;
  }

  // Command classes need to be proper classes too
  class MockGetObjectCommand {
    constructor(input) {
      this.input = input;
    }
  }

  class MockPutObjectCommand {
    constructor(input) {
      this.input = input;
    }
  }

  return {
    S3Client: MockS3Client,
    GetObjectCommand: MockGetObjectCommand,
    PutObjectCommand: MockPutObjectCommand,
  };
});

describe("s3-utils", () => {
  let tmpDir;
  const originalEnv = process.env;

  beforeEach(async () => {
    // Set AWS region to prevent S3Client from failing during initialization
    process.env.AWS_REGION = "us-east-1";
    vi.resetModules();
    mockSend.mockReset();
    // Create a temp directory for tests
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "s3-test-"));
  });

  afterEach(async () => {
    process.env = originalEnv;
    if (tmpDir) {
      await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
    }
  });

  describe("parseS3Uri", () => {
    it("should parse valid S3 URI", async () => {
      const { parseS3Uri } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      const result = parseS3Uri("s3://my-bucket/path/to/file.zip");

      expect(result).toEqual({
        bucket: "my-bucket",
        key: "path/to/file.zip",
      });
    });

    it("should parse S3 URI with simple key", async () => {
      const { parseS3Uri } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      const result = parseS3Uri("s3://bucket/file.txt");

      expect(result).toEqual({
        bucket: "bucket",
        key: "file.txt",
      });
    });

    it("should parse S3 URI with deeply nested key", async () => {
      const { parseS3Uri } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      const result = parseS3Uri("s3://bucket/a/b/c/d/e/file.zip");

      expect(result).toEqual({
        bucket: "bucket",
        key: "a/b/c/d/e/file.zip",
      });
    });

    it("should throw error for invalid S3 URI", async () => {
      const { parseS3Uri } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      expect(() => parseS3Uri("https://bucket/key")).toThrow("Bad S3 URI");
    });

    it("should throw error for URI without s3:// prefix", async () => {
      const { parseS3Uri } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      expect(() => parseS3Uri("bucket/key")).toThrow("Bad S3 URI");
    });

    it("should throw error for malformed URI", async () => {
      const { parseS3Uri } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      expect(() => parseS3Uri("s3://")).toThrow("Bad S3 URI");
    });

    it("should throw error for URI with only bucket (no key)", async () => {
      const { parseS3Uri } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      // s3://bucket doesn't match the regex since it expects a slash after bucket
      expect(() => parseS3Uri("s3://bucket")).toThrow("Bad S3 URI");
    });

    it("should parse URI with empty key (trailing slash)", async () => {
      const { parseS3Uri } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      // s3://bucket/ has an empty string as key, which the regex captures
      const result = parseS3Uri("s3://bucket/");
      expect(result).toEqual({
        bucket: "bucket",
        key: "",
      });
    });

    it("should parse URI with special characters in key", async () => {
      const { parseS3Uri } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      const result = parseS3Uri("s3://bucket/path/to/file with spaces.txt");
      expect(result).toEqual({
        bucket: "bucket",
        key: "path/to/file with spaces.txt",
      });
    });
  });

  describe("buildS3Uri", () => {
    it("should build S3 URI from bucket and key", async () => {
      const { buildS3Uri } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      const result = buildS3Uri("my-bucket", "path/to/file.zip");

      expect(result).toBe("s3://my-bucket/path/to/file.zip");
    });

    it("should handle simple key", async () => {
      const { buildS3Uri } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      const result = buildS3Uri("bucket", "file.txt");

      expect(result).toBe("s3://bucket/file.txt");
    });

    it("should handle empty key", async () => {
      const { buildS3Uri } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      const result = buildS3Uri("bucket", "");
      expect(result).toBe("s3://bucket/");
    });
  });

  describe("downloadS3Object", () => {
    it("should download S3 object to local file", async () => {
      const testContent = Buffer.from("test file content");
      const mockBody = {
        transformToByteArray: vi.fn().mockResolvedValue(testContent),
      };

      mockSend.mockResolvedValue({
        Body: mockBody,
      });

      const { downloadS3Object } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      const destinationPath = path.join(tmpDir, "downloaded-file.txt");
      const mockLog = vi.fn();

      await downloadS3Object(
        { bucket: "test-bucket", key: "test-key.txt" },
        destinationPath,
        mockLog,
      );

      // Verify the file was written
      const writtenContent = await fs.readFile(destinationPath);
      expect(writtenContent.toString()).toBe("test file content");

      // Verify logging was called
      expect(mockLog).toHaveBeenCalledWith("info", "download_s3_object", {
        bucket: "test-bucket",
        key: "test-key.txt",
        destination: destinationPath,
      });

      // Verify S3 was called with correct parameters
      expect(mockSend).toHaveBeenCalledWith({
        input: { Bucket: "test-bucket", Key: "test-key.txt" },
      });
    });

    it("should throw error when Body is empty", async () => {
      mockSend.mockResolvedValue({
        Body: undefined,
      });

      const { downloadS3Object } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      const destinationPath = path.join(tmpDir, "empty-file.txt");
      const mockLog = vi.fn();

      await expect(
        downloadS3Object(
          { bucket: "test-bucket", key: "missing-key.txt" },
          destinationPath,
          mockLog,
        ),
      ).rejects.toThrow("Failed to download missing-key.txt from test-bucket");
    });

    it("should throw error when transformToByteArray returns empty", async () => {
      const mockBody = {
        transformToByteArray: vi.fn().mockResolvedValue(undefined),
      };

      mockSend.mockResolvedValue({
        Body: mockBody,
      });

      const { downloadS3Object } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      const destinationPath = path.join(tmpDir, "empty-body.txt");
      const mockLog = vi.fn();

      await expect(
        downloadS3Object(
          { bucket: "test-bucket", key: "empty-body.txt" },
          destinationPath,
          mockLog,
        ),
      ).rejects.toThrow("Failed to download empty-body.txt from test-bucket");
    });

    it("should propagate S3 errors", async () => {
      mockSend.mockRejectedValue(new Error("S3 access denied"));

      const { downloadS3Object } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      const destinationPath = path.join(tmpDir, "error-file.txt");
      const mockLog = vi.fn();

      await expect(
        downloadS3Object(
          { bucket: "test-bucket", key: "error-key.txt" },
          destinationPath,
          mockLog,
        ),
      ).rejects.toThrow("S3 access denied");
    });

    it("should download binary content correctly", async () => {
      const binaryContent = Buffer.from([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd]);
      const mockBody = {
        transformToByteArray: vi.fn().mockResolvedValue(binaryContent),
      };

      mockSend.mockResolvedValue({
        Body: mockBody,
      });

      const { downloadS3Object } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      const destinationPath = path.join(tmpDir, "binary-file.bin");
      const mockLog = vi.fn();

      await downloadS3Object(
        { bucket: "test-bucket", key: "binary.bin" },
        destinationPath,
        mockLog,
      );

      const writtenContent = await fs.readFile(destinationPath);
      expect(writtenContent).toEqual(binaryContent);
    });
  });

  describe("uploadToS3", () => {
    it("should upload local file to S3", async () => {
      mockSend.mockResolvedValue({});

      const { uploadToS3 } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      // Create a test file
      const localPath = path.join(tmpDir, "upload-test.txt");
      await fs.writeFile(localPath, "content to upload");

      const mockLog = vi.fn();

      const result = await uploadToS3(
        localPath,
        { bucket: "upload-bucket", key: "uploaded/file.txt" },
        mockLog,
      );

      expect(result).toBe("s3://upload-bucket/uploaded/file.txt");

      // Verify logging
      expect(mockLog).toHaveBeenCalledWith("info", "upload_s3_object", {
        bucket: "upload-bucket",
        key: "uploaded/file.txt",
      });

      // Verify S3 was called with correct parameters
      expect(mockSend).toHaveBeenCalledWith({
        input: expect.objectContaining({
          Bucket: "upload-bucket",
          Key: "uploaded/file.txt",
        }),
      });
    });

    it("should upload file with content type", async () => {
      mockSend.mockResolvedValue({});

      const { uploadToS3 } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      const localPath = path.join(tmpDir, "upload-json.json");
      await fs.writeFile(localPath, '{"test": true}');

      const mockLog = vi.fn();

      await uploadToS3(
        localPath,
        { bucket: "json-bucket", key: "data.json" },
        mockLog,
        "application/json",
      );

      // Verify content type was passed
      expect(mockSend).toHaveBeenCalledWith({
        input: expect.objectContaining({
          ContentType: "application/json",
        }),
      });
    });

    it("should propagate S3 upload errors", async () => {
      mockSend.mockRejectedValue(new Error("Upload failed"));

      const { uploadToS3 } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      const localPath = path.join(tmpDir, "error-upload.txt");
      await fs.writeFile(localPath, "error content");

      const mockLog = vi.fn();

      await expect(
        uploadToS3(
          localPath,
          { bucket: "error-bucket", key: "error.txt" },
          mockLog,
        ),
      ).rejects.toThrow("Upload failed");
    });

    it("should throw error when local file does not exist", async () => {
      const { uploadToS3 } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      const mockLog = vi.fn();

      await expect(
        uploadToS3(
          "/nonexistent/file.txt",
          { bucket: "bucket", key: "key.txt" },
          mockLog,
        ),
      ).rejects.toThrow();
    });

    it("should upload binary files correctly", async () => {
      mockSend.mockResolvedValue({});

      const { uploadToS3 } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      const binaryContent = Buffer.from([0x89, 0x50, 0x4e, 0x47]); // PNG magic bytes
      const localPath = path.join(tmpDir, "image.png");
      await fs.writeFile(localPath, binaryContent);

      const mockLog = vi.fn();

      await uploadToS3(
        localPath,
        { bucket: "image-bucket", key: "images/test.png" },
        mockLog,
        "image/png",
      );

      expect(mockSend).toHaveBeenCalledWith({
        input: expect.objectContaining({
          Body: binaryContent,
        }),
      });
    });
  });

  describe("exported s3 client", () => {
    it("should export the S3 client instance", async () => {
      const { s3 } = await import(
        "../../../../workflow/layers/shared/src/s3-utils.mjs"
      );

      expect(s3).toBeDefined();
      expect(typeof s3.send).toBe("function");
    });
  });
});
