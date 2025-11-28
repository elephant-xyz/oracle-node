// @ts-check
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { promises as fs } from "fs";
import path from "path";
import os from "os";
import AdmZip from "adm-zip";

// Mock AWS SDK - create mock send function inside the factory
vi.mock("@aws-sdk/client-s3", () => {
  // Create the mock send function inside the factory
  const sendFn = vi.fn();
  const MockS3Client = vi.fn(() => ({
    send: sendFn,
  }));
  
  // Store reference to sendFn on the constructor so we can access it
  MockS3Client._mockSend = sendFn;
  
  return {
    S3Client: MockS3Client,
    GetObjectCommand: class GetObjectCommand {
      constructor(input) {
        this.input = input;
      }
    },
    PutObjectCommand: class PutObjectCommand {
      constructor(input) {
        this.input = input;
      }
    },
  };
});

// Import after mocks
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { handler, downloadAndTransform, validateAndUpload } from "./index.mjs";

// Get the mock send function from the mocked S3Client
// @ts-ignore - accessing internal mock property
const getMockS3Send = () => S3Client._mockSend;

// Mock @elephant-xyz/cli
vi.mock("@elephant-xyz/cli/lib", () => {
  return {
    transform: vi.fn(),
    validate: vi.fn(),
  };
});

import { transform, validate } from "@elephant-xyz/cli/lib";

/**
 * Create a test CSV file content
 * @param {string} countyName - County name to include
 * @returns {string} CSV content
 */
function createTestCsv(countyName = "Miami Dade") {
  return `property_id,address,city,state,zip
123,123 Main St,Miami,FL,33101
456,456 Oak Ave,Miami,FL,33102`;
}

/**
 * Create a test seed output ZIP with unnormalized_address.json
 * @param {string} countyName - County name
 * @returns {Buffer} ZIP file buffer
 */
function createSeedOutputZip(countyName = "Miami Dade") {
  const zip = new AdmZip();
  const unnormalizedAddress = {
    county_jurisdiction: countyName,
    address: "123 Main St",
    city: "Miami",
    state: "FL",
    zip: "33101",
  };
  const propertySeed = {
    property_id: "123",
    address: "123 Main St",
  };

  zip.addFile(
    "data/unnormalized_address.json",
    Buffer.from(JSON.stringify(unnormalizedAddress)),
  );
  zip.addFile(
    "data/property_seed.json",
    Buffer.from(JSON.stringify(propertySeed)),
  );

  return zip.toBuffer();
}

/**
 * Test helper to setup S3 GetObject mock for CSV download
 * @param {string} csvContent - CSV content to return
 */
function mockS3GetObject(csvContent) {
  getMockS3Send().mockImplementationOnce((/** @type {any} */ command) => {
    if (command instanceof GetObjectCommand) {
      return Promise.resolve({
        Body: {
          transformToByteArray: () =>
            Promise.resolve(Buffer.from(csvContent)),
        },
      });
    }
    return Promise.resolve({});
  });
}

/**
 * Test helper to setup transform mock
 * @param {Buffer} seedOutputZipBuffer - Seed output ZIP buffer
 */
function mockTransform(seedOutputZipBuffer) {
  // @ts-ignore - Vitest mock
  transform.mockImplementation(async (/** @type {any} */ options) => {
    await fs.writeFile(options.outputZip, seedOutputZipBuffer);
    return { success: true };
  });
}

/**
 * Test helper to setup validate mock (success)
 */
function mockValidateSuccess() {
  // @ts-ignore - Vitest mock
  validate.mockResolvedValue({ success: true });
}

/**
 * Test helper to setup validate mock (failure)
 * @param {string} cwd - Working directory
 * @param {string} errorMessage - Error message
 * @param {boolean} createSubmitErrors - Whether to create submit_errors.csv
 */
function mockValidateFailure(cwd, errorMessage = "Validation failed", createSubmitErrors = true) {
  // @ts-ignore - Vitest mock
  validate.mockImplementation(async (/** @type {any} */ { input, cwd: validateCwd }) => {
    if (createSubmitErrors) {
      const submitErrorsPath = path.join(validateCwd || cwd, "submit_errors.csv");
      await fs.writeFile(
        submitErrorsPath,
        "error_type,message\nSchemaValidation,Invalid field",
      );
    }
    return {
      success: false,
      error: errorMessage,
    };
  });
}

/**
 * Test helper to setup S3 PutObject mocks (success)
 * @param {Function} onPutObject - Optional callback when PutObject is called
 */
function mockS3PutObject(onPutObject = null) {
  getMockS3Send().mockImplementation((/** @type {any} */ command) => {
    if (command instanceof PutObjectCommand) {
      if (onPutObject) {
        onPutObject(command);
      }
      return Promise.resolve({});
    }
    return Promise.resolve({});
  });
}

/**
 * Test helper to setup S3 PutObject mock (failure)
 * @param {number} failOnCall - Which PutObject call should fail (1-based)
 * @param {string} errorMessage - Error message
 */
function mockS3PutObjectFailure(failOnCall, errorMessage) {
  let putCallCount = 0;
  getMockS3Send().mockImplementation((/** @type {any} */ command) => {
    if (command instanceof PutObjectCommand) {
      putCallCount++;
      if (putCallCount === failOnCall) {
        return Promise.reject(new Error(errorMessage));
      }
      return Promise.resolve({});
    }
    return Promise.resolve({});
  });
}

/**
 * Test adapter - calls the split functions
 * @param {any} event - S3 event
 * @returns {Promise<any>} Handler result
 */
async function callHandler(event) {
  // Call downloadAndTransform, then validateAndUpload
  const transformResult = await downloadAndTransform(event);
  return await validateAndUpload(transformResult);
}

describe("pre Lambda - Full Integration Tests", () => {
  beforeEach(() => {
    // Clear all previous mock calls
    getMockS3Send().mockClear();
    // @ts-ignore - Vitest mock
    transform.mockClear();
    // @ts-ignore - Vitest mock
    validate.mockClear();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("download and transform phase", () => {
    it("should download CSV from S3", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv();
      mockS3GetObject(csvContent);

      // Mock transform to succeed but we'll fail validation to stop early
      const seedOutputZipBuffer = createSeedOutputZip();
      mockTransform(seedOutputZipBuffer);
      mockValidateFailure("", "Test error", false);

      await expect(callHandler(event)).rejects.toThrow();

      // Verify GetObject was called
      expect(getMockS3Send()).toHaveBeenCalledWith(expect.any(GetObjectCommand));
      const getCommand = getMockS3Send().mock.calls.find(
        (call) => call[0] instanceof GetObjectCommand,
      )?.[0];
      expect(getCommand?.input.Bucket).toBe("test-bucket");
      expect(getCommand?.input.Key).toBe("input.csv");
    });

    it("should create input.zip with seed.csv", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv();
      const seedOutputZipBuffer = createSeedOutputZip();

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);
      mockValidateSuccess();
      mockS3PutObject();

      await callHandler(event);

      // Verify transform was called with input.zip
      expect(transform).toHaveBeenCalled();
      const transformCall = transform.mock.calls[0]?.[0];
      expect(transformCall?.inputZip).toContain("input.zip");
      expect(transformCall?.outputZip).toContain("seed_output.zip");
    });

    it("should throw error if CSV download fails", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      getMockS3Send().mockRejectedValueOnce(
        new Error("S3 access denied"),
      );

      await expect(callHandler(event)).rejects.toThrow("S3 access denied");
    });

    it("should throw error if transform fails", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv();
      mockS3GetObject(csvContent);

      // @ts-ignore - Vitest mock
      transform.mockResolvedValue({
        success: false,
        error: "Transform failed: Invalid CSV format",
      });

      await expect(callHandler(event)).rejects.toThrow(
        "Transform failed: Invalid CSV format",
      );

      // Validate should not be called if transform fails
      expect(validate).not.toHaveBeenCalled();
    });
  });

  describe("validate and upload phase", () => {
    it("should validate seed_output.zip", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv();
      const seedOutputZipBuffer = createSeedOutputZip();

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);
      mockValidateSuccess();
      mockS3PutObject();

      await callHandler(event);

      // Verify validate was called
      expect(validate).toHaveBeenCalled();
      const validateCall = validate.mock.calls[0]?.[0];
      expect(validateCall?.input).toContain("seed_output.zip");
    });

    it("should upload seed_output.zip and county_prep_input.zip on success", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv();
      const seedOutputZipBuffer = createSeedOutputZip();

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);
      mockValidateSuccess();

      const uploadedKeys = [];
      mockS3PutObject((command) => {
        if (command instanceof PutObjectCommand) {
          uploadedKeys.push(command.input.Key);
        }
      });

      await callHandler(event);

      // Should upload seed_output.zip and county_prep_input.zip
      expect(uploadedKeys.length).toBeGreaterThanOrEqual(2);
      expect(uploadedKeys.some((key) => key.includes("seed_output.zip"))).toBe(true);
      expect(uploadedKeys.some((key) => key.includes("county_prep/input.zip"))).toBe(true);
    });

    it("should upload submit_errors.csv on validation failure", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv();
      const seedOutputZipBuffer = createSeedOutputZip();

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);

      // Create temp dir for submit_errors.csv
      const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "pre-test-"));
      mockValidateFailure(tempDir, "Validation failed", true);

      const uploadedKeys = [];
      mockS3PutObject((command) => {
        if (command instanceof PutObjectCommand) {
          uploadedKeys.push(command.input.Key);
        }
      });

      await expect(callHandler(event)).rejects.toThrow("Validation failed");

      // Should upload submit_errors.csv
      expect(uploadedKeys.some((key) => key.includes("submit_errors.csv"))).toBe(true);

      // Cleanup
      await fs.rm(tempDir, { recursive: true, force: true });
    });

    it("should throw error if seed_output.zip upload fails", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv();
      const seedOutputZipBuffer = createSeedOutputZip();

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);
      mockValidateSuccess();
      mockS3PutObjectFailure(1, "Failed to upload seed_output.zip");

      await expect(callHandler(event)).rejects.toThrow(
        "Failed to upload seed_output.zip",
      );
    });

    it("should throw error if county_prep_input.zip upload fails", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv();
      const seedOutputZipBuffer = createSeedOutputZip();

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);
      mockValidateSuccess();
      mockS3PutObjectFailure(2, "Failed to upload county_prep_input.zip");

      await expect(callHandler(event)).rejects.toThrow(
        "Failed to upload county_prep_input.zip",
      );
    });
  });

  describe("end-to-end success scenarios", () => {
    it("should process CSV and return correct output structure", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv("Miami Dade");
      const seedOutputZipBuffer = createSeedOutputZip("Miami Dade");

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);
      mockValidateSuccess();
      mockS3PutObject();

      const result = await callHandler(event);

      // Verify output structure
      expect(result).toHaveProperty("input_s3_uri");
      expect(result).toHaveProperty("output_prefix");
      expect(result).toHaveProperty("seed_output_s3_uri");
      expect(result).toHaveProperty("county_prep_input_s3_uri");
      expect(result).toHaveProperty("county_name");
      expect(result).toHaveProperty("county_key");

      // Verify values
      expect(result.input_s3_uri).toBe("s3://test-bucket/input.csv");
      expect(result.output_prefix).toContain("s3://test-bucket/outputs/input");
      expect(result.county_name).toBe("Miami Dade");
      expect(result.county_key).toBe("Miami_Dade");
    });

    it("should handle county names with spaces correctly", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv("St. Lucie");
      const seedOutputZipBuffer = createSeedOutputZip("St. Lucie");

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);
      mockValidateSuccess();
      mockS3PutObject();

      const result = await callHandler(event);

      expect(result.county_name).toBe("St. Lucie");
      expect(result.county_key).toBe("St._Lucie");
    });

    it("should use custom OUTPUT_BASE_URI if provided", async () => {
      const originalEnv = process.env.OUTPUT_BASE_URI;
      process.env.OUTPUT_BASE_URI = "s3://custom-bucket/custom-prefix";

      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv();
      const seedOutputZipBuffer = createSeedOutputZip();

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);
      mockValidateSuccess();
      mockS3PutObject();

      const result = await callHandler(event);

      expect(result.output_prefix).toContain("s3://custom-bucket/custom-prefix/input");

      // Restore original env
      if (originalEnv) {
        process.env.OUTPUT_BASE_URI = originalEnv;
      } else {
        delete process.env.OUTPUT_BASE_URI;
      }
    });

    it("should handle URL-encoded S3 keys", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "path%2Fto%2Ffile.csv" },
        },
      };

      const csvContent = createTestCsv();
      const seedOutputZipBuffer = createSeedOutputZip();

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);
      mockValidateSuccess();
      mockS3PutObject();

      const result = await callHandler(event);

      expect(result.input_s3_uri).toBe("s3://test-bucket/path/to/file.csv");
      expect(result.output_prefix).toContain("file");
    });
  });

  describe("error handling", () => {
    it("should throw error if S3 bucket/key is missing", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: {},
        },
      };

      await expect(callHandler(event)).rejects.toThrow(
        "Missing S3 bucket/key in message",
      );
    });

    it("should throw error if CSV body is null", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      getMockS3Send().mockImplementationOnce((/** @type {any} */ command) => {
        if (command instanceof GetObjectCommand) {
          return Promise.resolve({
            Body: null,
          });
        }
        return Promise.resolve({});
      });

      await expect(callHandler(event)).rejects.toThrow();
    });

    it("should handle missing submit_errors.csv on validation failure", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv();
      const seedOutputZipBuffer = createSeedOutputZip();

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);

      const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "pre-test-"));
      mockValidateFailure(tempDir, "Validation failed", false); // Don't create submit_errors.csv
      mockS3PutObject();

      await expect(callHandler(event)).rejects.toThrow("Validation failed");

      await fs.rm(tempDir, { recursive: true, force: true });
    });

    it("should handle csvError when JSON.stringify fails (covers csvError catch block)", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv();
      const seedOutputZipBuffer = createSeedOutputZip();

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);

      const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "pre-test-"));
      
      // Mock validate to create submit_errors.csv
      // @ts-ignore - Vitest mock
      validate.mockImplementation(async (/** @type {any} */ { input, cwd }) => {
        const submitErrorsPath = path.join(cwd, "submit_errors.csv");
        await fs.writeFile(
          submitErrorsPath,
          "error_type,message\nSchemaValidation,Invalid field",
        );
        return {
          success: false,
          error: "Validation failed: Invalid schema",
        };
      });

      // Mock JSON.stringify to throw (simulating circular reference or other error)
      // This will trigger the csvError catch block (lines 143-153)
      const originalStringify = JSON.stringify;
      let stringifyCallCount = 0;
      
      // @ts-ignore
      JSON.stringify = vi.fn((...args) => {
        stringifyCallCount++;
        // Throw on the JSON.stringify call in the error logging (line 134)
        // This simulates a circular reference or other JSON.stringify error
        if (stringifyCallCount > 2 && args[0]?.submit_errors) {
          throw new Error("Converting circular structure to JSON");
        }
        return originalStringify(...args);
      });

      mockS3PutObject();

      // Should still throw error, csvError catch block will be triggered
      // This covers the csvError catch block (lines 143-153)
      await expect(callHandler(event)).rejects.toThrow("Validation failed");

      // Restore original JSON.stringify
      // @ts-ignore
      JSON.stringify = originalStringify;

      await fs.rm(tempDir, { recursive: true, force: true });
    });

    it("should handle csvToJson failure when reading submit_errors.csv", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv();
      const seedOutputZipBuffer = createSeedOutputZip();

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);

      // Mock validate to create a submit_errors.csv file that will fail to parse
      const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "pre-test-"));
      // @ts-ignore - Vitest mock
      validate.mockImplementation(async (/** @type {any} */ { input, cwd }) => {
        // Create an invalid CSV file that will cause csvToJson to fail
        const submitErrorsPath = path.join(cwd, "submit_errors.csv");
        // Create a file that exists but will cause parse errors (binary data)
        await fs.writeFile(submitErrorsPath, Buffer.from([0x00, 0x01, 0x02, 0x03]));
        return {
          success: false,
          error: "Validation failed: Invalid schema",
        };
      });

      mockS3PutObject();

      // Should still throw error, but csvToJson will return [] due to catch
      await expect(callHandler(event)).rejects.toThrow("Validation failed");

      await fs.rm(tempDir, { recursive: true, force: true });
    });
  });

  describe("county extraction", () => {
    it("should handle missing county_jurisdiction gracefully", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv();
      // Create seed output without county_jurisdiction
      const zipWithoutCounty = new AdmZip();
      const unnormalizedAddress = {
        address: "123 Main St",
        city: "Miami",
        state: "FL",
        zip: "33101",
      };
      zipWithoutCounty.addFile(
        "data/unnormalized_address.json",
        Buffer.from(JSON.stringify(unnormalizedAddress)),
      );
      zipWithoutCounty.addFile(
        "data/property_seed.json",
        Buffer.from(JSON.stringify({ property_id: "123" })),
      );
      const seedOutputZipBuffer = zipWithoutCounty.toBuffer();

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);
      mockValidateSuccess();
      mockS3PutObject();

      const result = await callHandler(event);

      expect(result.county_name).toBe("");
      expect(result.county_key).toBe("");
    });

    it("should handle invalid JSON in unnormalized_address.json", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv();
      const zipWithInvalidJson = new AdmZip();
      zipWithInvalidJson.addFile(
        "data/unnormalized_address.json",
        Buffer.from("invalid json { not closed"),
      );
      zipWithInvalidJson.addFile(
        "data/property_seed.json",
        Buffer.from(JSON.stringify({ property_id: "123" })),
      );
      const seedOutputZipBuffer = zipWithInvalidJson.toBuffer();

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);
      mockValidateSuccess();
      mockS3PutObject();

      const result = await callHandler(event);

      expect(result.county_name).toBe("");
      expect(result.county_key).toBe("");
    });
  });

  describe("output structure verification", () => {
    it("should return correctly formatted S3 URIs", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "data/input.csv" },
        },
      };

      const csvContent = createTestCsv("Polk");
      const seedOutputZipBuffer = createSeedOutputZip("Polk");

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);
      mockValidateSuccess();
      mockS3PutObject();

      const result = await callHandler(event);

      expect(result.input_s3_uri).toBe("s3://test-bucket/data/input.csv");
      expect(result.output_prefix).toBe("s3://test-bucket/outputs/input");
      expect(result.seed_output_s3_uri).toContain("s3://");
      expect(result.seed_output_s3_uri).toContain("seed_output.zip");
      expect(result.county_prep_input_s3_uri).toContain("s3://");
      expect(result.county_prep_input_s3_uri).toContain("county_prep/input.zip");
    });

    it("should include original CSV in county_prep_input.zip", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv();
      const seedOutputZipBuffer = createSeedOutputZip();

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);
      mockValidateSuccess();

      let countyPrepZipBody = null;
      mockS3PutObject((command) => {
        if (command instanceof PutObjectCommand && command.input.Key.includes("county_prep/input.zip")) {
          countyPrepZipBody = command.input.Body;
        }
      });

      await callHandler(event);

      expect(countyPrepZipBody).not.toBeNull();
      const zip = new AdmZip(countyPrepZipBody);
      const entryNames = zip.getEntries().map((entry) => entry.entryName);

      expect(entryNames).toContain("unnormalized_address.json");
      expect(entryNames).toContain("property_seed.json");
      expect(entryNames).toContain("input.csv");
    });
  });

  describe("handler integration", () => {
    it("should work when calling handler directly (backward compatibility)", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv("Miami Dade");
      const seedOutputZipBuffer = createSeedOutputZip("Miami Dade");

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);
      mockValidateSuccess();
      mockS3PutObject();

      // Call handler directly (should internally call downloadAndTransform and validateAndUpload)
      const result = await handler(event);

      expect(result).toHaveProperty("input_s3_uri");
      expect(result).toHaveProperty("output_prefix");
      expect(result).toHaveProperty("seed_output_s3_uri");
      expect(result).toHaveProperty("county_prep_input_s3_uri");
      expect(result).toHaveProperty("county_name");
      expect(result).toHaveProperty("county_key");
      expect(result.county_name).toBe("Miami Dade");
    });

    it("should handle errors from downloadAndTransform", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      // Make S3 download fail
      getMockS3Send().mockRejectedValueOnce(
        new Error("S3 access denied"),
      );

      await expect(handler(event)).rejects.toThrow("S3 access denied");
    });

    it("should handle errors from validateAndUpload", async () => {
      const event = {
        s3: {
          bucket: { name: "test-bucket" },
          object: { key: "input.csv" },
        },
      };

      const csvContent = createTestCsv();
      const seedOutputZipBuffer = createSeedOutputZip();

      mockS3GetObject(csvContent);
      mockTransform(seedOutputZipBuffer);

      // Make validation fail
      const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "pre-test-"));
      mockValidateFailure(tempDir, "Validation failed", true);
      mockS3PutObject();

      await expect(handler(event)).rejects.toThrow("Validation failed");

      await fs.rm(tempDir, { recursive: true, force: true });
    });
  });
});
