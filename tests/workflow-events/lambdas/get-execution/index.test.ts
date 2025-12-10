import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
import type { FailedExecutionItem, ExecutionErrorLink } from "shared/types.js";
import type {
  ExecutionBusinessData,
  ErrorBusinessData,
} from "../../../../workflow-events/lambdas/get-execution/index.js";

/**
 * Mock the DynamoDB Document Client for all tests.
 */
const ddbMock = mockClient(DynamoDBDocumentClient);

/**
 * Test table name used in all tests.
 */
const TEST_TABLE_NAME = "test-workflow-errors-table";

/**
 * Creates a mock FailedExecutionItem with optional overrides.
 * @param overrides - Partial overrides for the item
 * @returns A complete FailedExecutionItem object
 */
const createMockExecution = (
  overrides: Partial<FailedExecutionItem> = {},
): FailedExecutionItem => ({
  PK: "EXECUTION#exec-001",
  SK: "EXECUTION#exec-001",
  executionId: "exec-001",
  entityType: "FailedExecution",
  status: "failed",
  errorType: "01",
  county: "palm_beach",
  totalOccurrences: 5,
  openErrorCount: 3,
  uniqueErrorCount: 3,
  createdAt: "2025-01-01T00:00:00.000Z",
  updatedAt: "2025-01-01T00:00:00.000Z",
  GS1PK: "METRIC#ERRORCOUNT",
  GS1SK: "COUNT#FAILED#0000000003#EXECUTION#exec-001",
  GS3PK: "METRIC#ERRORCOUNT",
  GS3SK: "COUNT#01#FAILED#0000000003#EXECUTION#exec-001",
  ...overrides,
});

/**
 * Creates a mock ExecutionErrorLink with optional overrides.
 * @param errorCode - The error code
 * @param executionId - The execution ID
 * @param overrides - Partial overrides for the item
 * @returns A complete ExecutionErrorLink object
 */
const createMockErrorLink = (
  errorCode: string,
  executionId: string = "exec-001",
  overrides: Partial<ExecutionErrorLink> = {},
): ExecutionErrorLink => ({
  PK: `EXECUTION#${executionId}`,
  SK: `ERROR#${errorCode}`,
  entityType: "ExecutionError",
  errorCode,
  status: "failed",
  occurrences: 1,
  errorDetails: "{}",
  executionId,
  county: "palm_beach",
  createdAt: "2025-01-01T00:00:00.000Z",
  updatedAt: "2025-01-01T00:00:00.000Z",
  GS1PK: `ERROR#${errorCode}`,
  GS1SK: `EXECUTION#${executionId}`,
  ...overrides,
});

/**
 * Strips DynamoDB-specific fields from FailedExecutionItem, returning only business fields.
 * @param item - The FailedExecutionItem from DynamoDB
 * @returns ExecutionBusinessData with only business fields
 */
const stripExecutionFields = (
  item: FailedExecutionItem,
): ExecutionBusinessData => {
  return {
    executionId: item.executionId,
    status: item.status,
    errorType: item.errorType,
    county: item.county,
    totalOccurrences: item.totalOccurrences,
    openErrorCount: item.openErrorCount,
    uniqueErrorCount: item.uniqueErrorCount,
    taskToken: item.taskToken,
    createdAt: item.createdAt,
    updatedAt: item.updatedAt,
  };
};

/**
 * Strips DynamoDB-specific fields from ExecutionErrorLink, returning only business fields.
 * @param item - The ExecutionErrorLink from DynamoDB
 * @returns ErrorBusinessData with only business fields
 */
const stripErrorFields = (item: ExecutionErrorLink): ErrorBusinessData => {
  return {
    errorCode: item.errorCode,
    status: item.status,
    occurrences: item.occurrences,
    errorDetails: item.errorDetails,
    executionId: item.executionId,
    county: item.county,
    createdAt: item.createdAt,
    updatedAt: item.updatedAt,
  };
};

describe("get-execution handler", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.resetModules();
    ddbMock.reset();
    process.env = {
      ...originalEnv,
      WORKFLOW_ERRORS_TABLE_NAME: TEST_TABLE_NAME,
    };
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "info").mockImplementation(() => {});
    vi.spyOn(console, "debug").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  describe("input validation", () => {
    it("should return validation error when sortOrder is missing", async () => {
      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      const result = await handler({});

      expect(result.success).toBe(false);
      expect(result.execution).toBeNull();
      expect(result.errors).toEqual([]);
      // error can be string or ZodIssue[] - check for sortOrder in either format
      if (Array.isArray(result.error)) {
        expect(
          result.error.some((issue) =>
            issue.path.some((p) => String(p) === "sortOrder"),
          ),
        ).toBe(true);
      } else {
        expect(result.error).toBeDefined();
        expect(String(result.error)).toContain("sortOrder");
      }
    });

    it("should return validation error when sortOrder is invalid", async () => {
      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      const result = await handler({ sortOrder: "invalid" });

      expect(result.success).toBe(false);
      expect(result.execution).toBeNull();
      expect(result.errors).toEqual([]);
      // error can be string or ZodIssue[] - check for sortOrder in either format
      if (Array.isArray(result.error)) {
        expect(
          result.error.some((issue) =>
            issue.path.some((p) => String(p) === "sortOrder"),
          ),
        ).toBe(true);
      } else {
        expect(result.error).toBeDefined();
        expect(String(result.error)).toContain("sortOrder");
      }
    });

    it("should return validation error when errorType is empty string", async () => {
      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      const result = await handler({ sortOrder: "most", errorType: "" });

      expect(result.success).toBe(false);
      expect(result.execution).toBeNull();
      expect(result.errors).toEqual([]);
      // error can be string or ZodIssue[] - check for errorType in either format
      if (Array.isArray(result.error)) {
        const errorStr = JSON.stringify(result.error);
        expect(errorStr).toContain("errorType");
        expect(errorStr).toContain("cannot be an empty string");
      } else {
        expect(result.error).toBeDefined();
        expect(String(result.error)).toContain("errorType");
        expect(String(result.error)).toContain("cannot be an empty string");
      }
    });

    it("should accept valid sortOrder 'most'", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      const result = await handler({ sortOrder: "most" });

      expect(result.success).toBe(true);
    });

    it("should accept valid sortOrder 'least'", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      const result = await handler({ sortOrder: "least" });

      expect(result.success).toBe(true);
    });

    it("should accept optional errorType when provided", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      const result = await handler({ sortOrder: "most", errorType: "01" });

      expect(result.success).toBe(true);
    });

    it("should trim whitespace from errorType", async () => {
      const mockExecution = createMockExecution({ errorType: "01" });
      const mockErrors = [createMockErrorLink("01256")];

      // First call returns execution, second call returns errors
      ddbMock
        .on(QueryCommand)
        .resolvesOnce({ Items: [mockExecution] })
        .resolvesOnce({ Items: mockErrors });

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      const result = await handler({ sortOrder: "most", errorType: "  01  " });

      expect(result.success).toBe(true);
      expect(result.execution).toBeDefined();
    });
  });

  describe("successful execution retrieval", () => {
    it("should return execution with errors when found", async () => {
      const mockExecution = createMockExecution({
        executionId: "exec-found-001",
        county: "broward",
        uniqueErrorCount: 2,
        totalOccurrences: 4,
      });
      const mockErrors = [
        createMockErrorLink("01256", "exec-found-001", { occurrences: 2 }),
        createMockErrorLink("02789", "exec-found-001", { occurrences: 2 }),
      ];

      // First call returns execution, second call returns errors
      ddbMock
        .on(QueryCommand)
        .resolvesOnce({ Items: [mockExecution] })
        .resolvesOnce({ Items: mockErrors });

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      const result = await handler({ sortOrder: "most" });

      expect(result.success).toBe(true);
      expect(result.execution).toEqual(stripExecutionFields(mockExecution));
      expect(result.errors).toEqual(mockErrors.map(stripErrorFields));
      expect(result.error).toBeUndefined();
    });

    it("should return null execution when none found", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      const result = await handler({ sortOrder: "most" });

      expect(result.success).toBe(true);
      expect(result.execution).toBeNull();
      expect(result.errors).toEqual([]);
      expect(result.error).toBeUndefined();
    });

    it("should return execution with empty errors array when no errors exist", async () => {
      const mockExecution = createMockExecution({
        executionId: "exec-no-errors-001",
        uniqueErrorCount: 0,
        totalOccurrences: 0,
      });

      // First call returns execution, second call returns empty errors
      ddbMock
        .on(QueryCommand)
        .resolvesOnce({ Items: [mockExecution] })
        .resolvesOnce({ Items: [] });

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      const result = await handler({ sortOrder: "least" });

      expect(result.success).toBe(true);
      expect(result.execution).toEqual(stripExecutionFields(mockExecution));
      expect(result.errors).toEqual([]);
    });
  });

  describe("GSI selection", () => {
    it("should use GS1 index when errorType is not provided", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      await handler({ sortOrder: "most" });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.IndexName).toBe("GS1");
      expect(calls[0].args[0].input.KeyConditionExpression).toBe(
        "GS1PK = :gs1pk AND begins_with(GS1SK, :gs1skPrefix)",
      );
      expect(calls[0].args[0].input.ExpressionAttributeValues).toMatchObject({
        ":gs1pk": "METRIC#ERRORCOUNT",
        ":gs1skPrefix": "COUNT#FAILED#",
      });
    });

    it("should use GS3 index when errorType is provided", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      await handler({ sortOrder: "most", errorType: "01" });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.IndexName).toBe("GS3");
      expect(calls[0].args[0].input.KeyConditionExpression).toBe(
        "GS3PK = :gs3pk AND begins_with(GS3SK, :gs3skPrefix)",
      );
      expect(calls[0].args[0].input.ExpressionAttributeValues).toMatchObject({
        ":gs3pk": "METRIC#ERRORCOUNT",
        ":gs3skPrefix": "COUNT#01#FAILED#",
      });
    });

    it("should not use FilterExpression in GS3 query (partition key separation)", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      await handler({ sortOrder: "most", errorType: "01" });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.FilterExpression).toBeUndefined();
      // Verify entityType is not in ExpressionAttributeValues for GS3 query
      expect(
        calls[0].args[0].input.ExpressionAttributeValues,
      ).not.toHaveProperty(":entityType");
    });
  });

  describe("sort order", () => {
    it("should set ScanIndexForward to false for 'most' errors", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      await handler({ sortOrder: "most" });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.ScanIndexForward).toBe(false);
    });

    it("should set ScanIndexForward to true for 'least' errors", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      await handler({ sortOrder: "least" });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.ScanIndexForward).toBe(true);
    });
  });

  describe("error handling", () => {
    it("should return error response when DynamoDB query fails", async () => {
      ddbMock.on(QueryCommand).rejects(new Error("DynamoDB connection error"));

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      const result = await handler({ sortOrder: "most" });

      expect(result.success).toBe(false);
      expect(result.execution).toBeNull();
      expect(result.errors).toEqual([]);
      expect(result.error).toBe("DynamoDB connection error");
    });

    it("should return error response when WORKFLOW_ERRORS_TABLE_NAME is not set", async () => {
      delete process.env.WORKFLOW_ERRORS_TABLE_NAME;

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      const result = await handler({ sortOrder: "most" });

      expect(result.success).toBe(false);
      expect(result.error).toBe(
        "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
      );
    });

    it("should handle errors from nested query failures", async () => {
      const mockExecution = createMockExecution({
        executionId: "exec-err-001",
      });

      // First query succeeds, second query (for errors) fails
      ddbMock
        .on(QueryCommand)
        .resolvesOnce({ Items: [mockExecution] })
        .rejectsOnce(new Error("Failed to query execution errors"));

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      const result = await handler({ sortOrder: "most" });

      expect(result.success).toBe(false);
      expect(result.error).toBe("Failed to query execution errors");
      expect(result.execution).toBeNull();
      expect(result.errors).toEqual([]);
    });
  });

  describe("execution with multiple errors", () => {
    it("should return all errors associated with execution", async () => {
      const mockExecution = createMockExecution({
        executionId: "exec-multi-err-001",
        uniqueErrorCount: 5,
        totalOccurrences: 10,
      });
      const mockErrors = [
        createMockErrorLink("01256", "exec-multi-err-001", { occurrences: 3 }),
        createMockErrorLink("02789", "exec-multi-err-001", { occurrences: 2 }),
        createMockErrorLink("03456", "exec-multi-err-001", { occurrences: 2 }),
        createMockErrorLink("04123", "exec-multi-err-001", { occurrences: 2 }),
        createMockErrorLink("05999", "exec-multi-err-001", { occurrences: 1 }),
      ];

      ddbMock
        .on(QueryCommand)
        .resolvesOnce({ Items: [mockExecution] })
        .resolvesOnce({ Items: mockErrors });

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      const result = await handler({ sortOrder: "most" });

      expect(result.success).toBe(true);
      expect(result.errors).toHaveLength(5);
      expect(result.execution?.uniqueErrorCount).toBe(5);
      // Verify DynamoDB-specific fields are not present
      expect(result.execution).not.toHaveProperty("PK");
      expect(result.execution).not.toHaveProperty("SK");
      expect(result.execution).not.toHaveProperty("GS1PK");
      expect(result.execution).not.toHaveProperty("GS1SK");
      expect(result.execution).not.toHaveProperty("GS3PK");
      expect(result.execution).not.toHaveProperty("GS3SK");
      expect(result.execution).not.toHaveProperty("entityType");
      result.errors.forEach((error) => {
        expect(error).not.toHaveProperty("PK");
        expect(error).not.toHaveProperty("SK");
        expect(error).not.toHaveProperty("GS1PK");
        expect(error).not.toHaveProperty("GS1SK");
        expect(error).not.toHaveProperty("entityType");
      });
    });
  });

  describe("filter expression", () => {
    it("should NOT use FilterExpression in GS1 query (partition key separation)", async () => {
      // GS1PK="METRIC#ERRORCOUNT" is only used by FailedExecutionItem,
      // so FilterExpression is unnecessary and can cause issues with Limit
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      await handler({ sortOrder: "most" });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.FilterExpression).toBeUndefined();
      expect(
        calls[0].args[0].input.ExpressionAttributeValues,
      ).not.toHaveProperty(":entityType");
    });
  });

  describe("query limit", () => {
    it("should limit query to 1 result for execution query", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } =
        await import("../../../../workflow-events/lambdas/get-execution/index.js");

      await handler({ sortOrder: "most" });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.Limit).toBe(1);
    });
  });
});

describe("get-execution dynamodb functions", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.resetModules();
    ddbMock.reset();
    process.env = {
      ...originalEnv,
      WORKFLOW_ERRORS_TABLE_NAME: TEST_TABLE_NAME,
    };
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "info").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  describe("queryExecutionByErrorCount", () => {
    it("should return null when no items are returned", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: undefined });

      const { queryExecutionByErrorCount } =
        await import("shared/repository.js");

      const result = await queryExecutionByErrorCount({
        sortOrder: "most",
      });

      expect(result).toBeNull();
    });

    it("should return null when Items array is empty", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { queryExecutionByErrorCount } =
        await import("shared/repository.js");

      const result = await queryExecutionByErrorCount({
        sortOrder: "most",
      });

      expect(result).toBeNull();
    });

    it("should return first item when execution found", async () => {
      const mockExecution = createMockExecution();
      ddbMock.on(QueryCommand).resolves({ Items: [mockExecution] });

      const { queryExecutionByErrorCount } =
        await import("shared/repository.js");

      const result = await queryExecutionByErrorCount({
        sortOrder: "most",
      });

      expect(result).toEqual(mockExecution);
    });

    it("should throw error when table name is not set", async () => {
      delete process.env.WORKFLOW_ERRORS_TABLE_NAME;

      const { queryExecutionByErrorCount } =
        await import("shared/repository.js");

      await expect(
        queryExecutionByErrorCount({ sortOrder: "most" }),
      ).rejects.toThrow(
        "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
      );
    });
  });

  describe("queryExecutionErrors", () => {
    it("should return empty array when no errors found", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { queryExecutionErrorLinks: queryExecutionErrors } =
        await import("shared/repository.js");

      const result = await queryExecutionErrors("exec-001");

      expect(result).toEqual([]);
    });

    it("should return all errors for execution", async () => {
      const mockErrors = [
        createMockErrorLink("01256"),
        createMockErrorLink("02789"),
      ];
      ddbMock.on(QueryCommand).resolves({ Items: mockErrors });

      const { queryExecutionErrorLinks: queryExecutionErrors } =
        await import("shared/repository.js");

      const result = await queryExecutionErrors("exec-001");

      expect(result).toEqual(mockErrors);
    });

    it("should paginate through all results", async () => {
      const mockErrorsPage1 = [createMockErrorLink("01256")];
      const mockErrorsPage2 = [createMockErrorLink("02789")];

      ddbMock
        .on(QueryCommand)
        .resolvesOnce({
          Items: mockErrorsPage1,
          LastEvaluatedKey: { PK: "EXECUTION#exec-001", SK: "ERROR#01256" },
        })
        .resolvesOnce({
          Items: mockErrorsPage2,
          LastEvaluatedKey: undefined,
        });

      const { queryExecutionErrorLinks: queryExecutionErrors } =
        await import("shared/repository.js");

      const result = await queryExecutionErrors("exec-001");

      expect(result).toHaveLength(2);
      expect(result).toContainEqual(mockErrorsPage1[0]);
      expect(result).toContainEqual(mockErrorsPage2[0]);
      expect(ddbMock).toHaveReceivedCommandTimes(QueryCommand, 2);
    });

    it("should query with correct key conditions and no FilterExpression", async () => {
      // Key condition already guarantees only ExecutionErrorLink items are returned
      // (FailedExecutionItem has SK="EXECUTION#{id}" which doesn't match "ERROR#" prefix)
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { queryExecutionErrorLinks: queryExecutionErrors } =
        await import("shared/repository.js");

      await queryExecutionErrors("exec-query-test-001");

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.KeyConditionExpression).toBe(
        "PK = :pk AND begins_with(SK, :skPrefix)",
      );
      expect(calls[0].args[0].input.ExpressionAttributeValues).toMatchObject({
        ":pk": "EXECUTION#exec-query-test-001",
        ":skPrefix": "ERROR#",
      });
      // FilterExpression should NOT be used - key conditions are sufficient
      expect(calls[0].args[0].input.FilterExpression).toBeUndefined();
      expect(
        calls[0].args[0].input.ExpressionAttributeValues,
      ).not.toHaveProperty(":entityType");
    });

    it("should throw error when table name is not set", async () => {
      delete process.env.WORKFLOW_ERRORS_TABLE_NAME;

      const { queryExecutionErrorLinks: queryExecutionErrors } =
        await import("shared/repository.js");

      await expect(queryExecutionErrors("exec-001")).rejects.toThrow(
        "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
      );
    });
  });

  describe("getExecutionWithErrors", () => {
    it("should return execution with its errors", async () => {
      const mockExecution = createMockExecution({
        executionId: "exec-full-001",
      });
      const mockErrors = [
        createMockErrorLink("01256", "exec-full-001"),
        createMockErrorLink("02789", "exec-full-001"),
      ];

      ddbMock
        .on(QueryCommand)
        .resolvesOnce({ Items: [mockExecution] })
        .resolvesOnce({ Items: mockErrors });

      const { getExecutionWithErrors } = await import("shared/repository.js");

      const result = await getExecutionWithErrors({ sortOrder: "most" });

      expect(result.execution).toEqual(mockExecution);
      expect(result.errors).toEqual(mockErrors);
    });

    it("should return null execution and empty errors when not found", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { getExecutionWithErrors } = await import("shared/repository.js");

      const result = await getExecutionWithErrors({ sortOrder: "most" });

      expect(result.execution).toBeNull();
      expect(result.errors).toEqual([]);
    });

    it("should not query for errors when execution is not found", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { getExecutionWithErrors } = await import("shared/repository.js");

      await getExecutionWithErrors({ sortOrder: "most" });

      // Should only have one query call (for execution), not a second one for errors
      expect(ddbMock).toHaveReceivedCommandTimes(QueryCommand, 1);
    });
  });
});
