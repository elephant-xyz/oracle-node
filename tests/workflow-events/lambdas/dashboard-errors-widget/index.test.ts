import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
import type { ErrorRecord } from "shared/types.js";

/**
 * Mock the DynamoDB Document Client for all tests.
 */
const ddbMock = mockClient(DynamoDBDocumentClient);

/**
 * Test table name used in all tests.
 */
const TEST_TABLE_NAME = "test-workflow-errors-table";

/**
 * Creates a mock ErrorRecord with optional overrides.
 * @param overrides - Partial overrides for the item
 * @returns A complete ErrorRecord object
 */
const createMockErrorRecord = (
  overrides: Partial<ErrorRecord> = {},
): ErrorRecord => ({
  PK: "ERROR#01256",
  SK: "ERROR#01256",
  errorCode: "01256",
  errorType: "01",
  entityType: "Error",
  errorDetails: "{}",
  errorStatus: "failed",
  totalCount: 10,
  createdAt: "2025-01-01T00:00:00.000Z",
  updatedAt: "2025-01-01T00:00:00.000Z",
  latestExecutionId: "exec-001-abcdef-1234567890-ghijkl",
  GS1PK: "TYPE#ERROR",
  GS1SK: "ERROR#01256",
  GS2PK: "TYPE#ERROR",
  GS2SK: "COUNT#0000000010#ERROR#01256",
  GS3PK: "METRIC#ERRORCOUNT",
  GS3SK: "COUNT#01#0000000010#ERROR#01256",
  ...overrides,
});

describe("dashboard-errors-widget handler", () => {
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

  describe("describe parameter", () => {
    it("should return markdown documentation when describe is true", async () => {
      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({ describe: true });

      const parsed = JSON.parse(result);
      expect(parsed.markdown).toContain("Errors with Most Occurrences");
      expect(parsed.markdown).toContain("Parameters");
      expect(parsed.markdown).toContain("errorType");
      expect(parsed.markdown).toContain("limit");
    });

    it("should not query DynamoDB when describe is true", async () => {
      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      await handler({ describe: true });

      expect(ddbMock).toHaveReceivedCommandTimes(QueryCommand, 0);
    });
  });

  describe("HTML generation", () => {
    it("should return HTML table with errors", async () => {
      const mockErrors = [
        createMockErrorRecord({ errorCode: "01256", totalCount: 15 }),
        createMockErrorRecord({ errorCode: "02789", totalCount: 8 }),
      ];

      ddbMock.on(QueryCommand).resolves({ Items: mockErrors });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({});

      expect(result).toContain("<table");
      expect(result).toContain("01256");
      expect(result).toContain("02789");
      expect(result).toContain("Error Code");
      expect(result).toContain("Total Count");
      expect(result).toContain("Status");
    });

    it("should return 'no errors found' message when empty", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({});

      expect(result).toContain("No errors found");
    });

    it("should escape HTML special characters", async () => {
      const mockError = createMockErrorRecord({
        errorCode: "<script>xss</script>",
      });

      ddbMock.on(QueryCommand).resolves({ Items: [mockError] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({});

      expect(result).not.toContain("<script>xss");
      expect(result).toContain("&lt;script&gt;");
    });

    it("should display error status with appropriate color", async () => {
      const mockErrors = [
        createMockErrorRecord({ errorStatus: "failed" }),
        createMockErrorRecord({ errorCode: "02789", errorStatus: "solved" }),
        createMockErrorRecord({
          errorCode: "03456",
          errorStatus: "maybeSolved",
        }),
      ];

      ddbMock.on(QueryCommand).resolves({ Items: mockErrors });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({});

      expect(result).toContain("failed");
      expect(result).toContain("solved");
      expect(result).toContain("maybeSolved");
    });

    it("should show filter badge when errorType is specified", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({ errorType: "SV" });

      expect(result).toContain("Filter:");
      expect(result).toContain("<code>SV</code>");
    });
  });

  describe("DynamoDB query without errorType filter", () => {
    it("should query GS2 index when errorType is not provided", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      await handler({});

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.IndexName).toBe("GS2");
      expect(calls[0].args[0].input.KeyConditionExpression).toBe(
        "GS2PK = :gs2pk",
      );
      expect(calls[0].args[0].input.ExpressionAttributeValues).toMatchObject({
        ":gs2pk": "TYPE#ERROR",
      });
    });

    it("should sort descending (most occurrences first)", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      await handler({});

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.ScanIndexForward).toBe(false);
    });

    it("should use default limit of 20", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      await handler({});

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.Limit).toBe(20);
    });

    it("should use custom limit when provided", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      await handler({ limit: 10 });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.Limit).toBe(10);
    });

    it("should filter by Error entity type", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      await handler({});

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.FilterExpression).toBe(
        "entityType = :entityType",
      );
      expect(calls[0].args[0].input.ExpressionAttributeValues).toMatchObject({
        ":entityType": "Error",
      });
    });
  });

  describe("DynamoDB query with errorType filter", () => {
    it("should query GS3 index when errorType is provided", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      await handler({ errorType: "SV" });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.IndexName).toBe("GS3");
      expect(calls[0].args[0].input.KeyConditionExpression).toBe(
        "GS3PK = :gs3pk AND begins_with(GS3SK, :gs3skPrefix)",
      );
      expect(calls[0].args[0].input.ExpressionAttributeValues).toMatchObject({
        ":gs3pk": "METRIC#ERRORCOUNT",
        ":gs3skPrefix": "COUNT#SV#",
      });
    });

    it("should trim errorType whitespace", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      await handler({ errorType: "  MV  " });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.ExpressionAttributeValues).toMatchObject({
        ":gs3skPrefix": "COUNT#MV#",
      });
    });

    it("should not use GS3 when errorType is empty string", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      await handler({ errorType: "" });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.IndexName).toBe("GS2");
    });

    it("should not use GS3 when errorType is whitespace only", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      await handler({ errorType: "   " });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.IndexName).toBe("GS2");
    });
  });

  describe("widget context params", () => {
    it("should read errorType from widgetContext.params", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      await handler({
        widgetContext: {
          dashboardName: "test-dashboard",
          widgetId: "widget-1",
          accountId: "123456789012",
          height: 300,
          width: 400,
          params: {
            errorType: "SV",
          },
        },
      });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.IndexName).toBe("GS3");
      expect(calls[0].args[0].input.ExpressionAttributeValues).toMatchObject({
        ":gs3skPrefix": "COUNT#SV#",
      });
    });

    it("should read limit from widgetContext.params", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      await handler({
        widgetContext: {
          dashboardName: "test-dashboard",
          widgetId: "widget-1",
          accountId: "123456789012",
          height: 300,
          width: 400,
          params: {
            limit: 5,
          },
        },
      });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.Limit).toBe(5);
    });

    it("should prefer top-level params over widgetContext.params", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      await handler({
        errorType: "MV",
        widgetContext: {
          dashboardName: "test-dashboard",
          widgetId: "widget-1",
          accountId: "123456789012",
          height: 300,
          width: 400,
          params: {
            errorType: "SV",
          },
        },
      });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.ExpressionAttributeValues).toMatchObject({
        ":gs3skPrefix": "COUNT#MV#",
      });
    });
  });

  describe("error handling", () => {
    it("should return error HTML when DynamoDB query fails", async () => {
      ddbMock.on(QueryCommand).rejects(new Error("DynamoDB connection error"));

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({});

      expect(result).toContain("Error loading data");
      expect(result).toContain("DynamoDB connection error");
    });

    it("should return error HTML when table name is not set", async () => {
      delete process.env.WORKFLOW_ERRORS_TABLE_NAME;

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({});

      expect(result).toContain("Error loading data");
      expect(result).toContain("WORKFLOW_ERRORS_TABLE_NAME");
    });
  });

  describe("pagination", () => {
    /**
     * Sets up environment variables required for Lambda ARN construction.
     */
    const setupPaginationEnv = (): void => {
      process.env.AWS_REGION = "us-east-1";
      process.env.AWS_LAMBDA_FUNCTION_NAME = "test-dashboard-errors-widget";
      process.env.AWS_ACCOUNT_ID = "123456789012";
    };

    /**
     * Creates a mock LastEvaluatedKey for DynamoDB pagination.
     * @returns DynamoDB key object
     */
    const createMockLastEvaluatedKey = (): Record<string, string> => ({
      PK: "ERROR#01256",
      SK: "ERROR#01256",
      GS2PK: "TYPE#ERROR",
      GS2SK: "COUNT#0000000010#ERROR#01256",
    });

    it("should render Next button when LastEvaluatedKey is returned", async () => {
      setupPaginationEnv();
      const mockErrors = [createMockErrorRecord()];
      ddbMock.on(QueryCommand).resolves({
        Items: mockErrors,
        LastEvaluatedKey: createMockLastEvaluatedKey(),
      });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({});

      expect(result).toContain('<a class="btn btn-primary">Next</a>');
      expect(result).toContain("<cwdb-action");
      expect(result).toContain('action="call"');
      expect(result).toContain(
        "arn:aws:lambda:us-east-1:123456789012:function:test-dashboard-errors-widget",
      );
    });

    it("should not render Next button when no LastEvaluatedKey", async () => {
      setupPaginationEnv();
      const mockErrors = [createMockErrorRecord()];
      ddbMock.on(QueryCommand).resolves({
        Items: mockErrors,
        LastEvaluatedKey: undefined,
      });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({});

      expect(result).not.toContain('<a class="btn btn-primary">Next</a>');
    });

    it("should render Previous button when cursor is provided", async () => {
      setupPaginationEnv();
      const mockErrors = [createMockErrorRecord()];
      const cursor = Buffer.from(
        JSON.stringify(createMockLastEvaluatedKey()),
      ).toString("base64");

      ddbMock.on(QueryCommand).resolves({
        Items: mockErrors,
        LastEvaluatedKey: undefined,
      });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({ cursor });

      expect(result).toContain('<a class="btn">Previous</a>');
      expect(result).toContain("<cwdb-action");
    });

    it("should not render Previous button on first page", async () => {
      setupPaginationEnv();
      const mockErrors = [createMockErrorRecord()];
      ddbMock.on(QueryCommand).resolves({
        Items: mockErrors,
        LastEvaluatedKey: undefined,
      });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({});

      expect(result).not.toContain('<a class="btn">Previous</a>');
    });

    it("should pass ExclusiveStartKey to DynamoDB when cursor is provided", async () => {
      const mockKey = createMockLastEvaluatedKey();
      const cursor = Buffer.from(JSON.stringify(mockKey)).toString("base64");

      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      await handler({ cursor });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.ExclusiveStartKey).toEqual(mockKey);
    });

    it("should not pass ExclusiveStartKey when no cursor provided", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      await handler({});

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.ExclusiveStartKey).toBeUndefined();
    });

    it("should preserve errorType filter in pagination buttons", async () => {
      setupPaginationEnv();
      const mockErrors = [createMockErrorRecord()];
      ddbMock.on(QueryCommand).resolves({
        Items: mockErrors,
        LastEvaluatedKey: createMockLastEvaluatedKey(),
      });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({ errorType: "SV" });

      expect(result).toContain('"errorType":"SV"');
    });

    it("should preserve limit in pagination buttons", async () => {
      setupPaginationEnv();
      const mockErrors = [createMockErrorRecord()];
      ddbMock.on(QueryCommand).resolves({
        Items: mockErrors,
        LastEvaluatedKey: createMockLastEvaluatedKey(),
      });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({ limit: 15 });

      expect(result).toContain('"limit":15');
    });

    it("should not render pagination buttons when Lambda ARN env vars are missing", async () => {
      // Do not set AWS_REGION, AWS_LAMBDA_FUNCTION_NAME, AWS_ACCOUNT_ID
      const mockErrors = [createMockErrorRecord()];
      ddbMock.on(QueryCommand).resolves({
        Items: mockErrors,
        LastEvaluatedKey: createMockLastEvaluatedKey(),
      });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({});

      // Pagination buttons (which call Lambda) should not render
      expect(result).not.toContain('action="call"');
      expect(result).not.toContain("Previous");
      expect(result).not.toContain("Next");

      // Details popup (which uses HTML action) should still render
      expect(result).toContain('action="html"');
      expect(result).toContain("Details");
    });

    it("should handle invalid cursor gracefully", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      // Invalid base64 cursor
      const result = await handler({ cursor: "invalid-cursor-not-base64!!!" });

      // Should not throw, just ignore invalid cursor
      expect(result).toContain("No errors found");
    });

    it("should include cursor in Next button params", async () => {
      setupPaginationEnv();
      const mockErrors = [createMockErrorRecord()];
      const mockKey = createMockLastEvaluatedKey();
      ddbMock.on(QueryCommand).resolves({
        Items: mockErrors,
        LastEvaluatedKey: mockKey,
      });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({});

      // The next cursor should be base64 encoded
      const expectedCursor = Buffer.from(JSON.stringify(mockKey)).toString(
        "base64",
      );
      expect(result).toContain(`"cursor":"${expectedCursor}"`);
    });

    it("should render both Previous and Next buttons when on middle page", async () => {
      setupPaginationEnv();
      const mockErrors = [createMockErrorRecord()];
      const cursor = Buffer.from(
        JSON.stringify(createMockLastEvaluatedKey()),
      ).toString("base64");

      ddbMock.on(QueryCommand).resolves({
        Items: mockErrors,
        LastEvaluatedKey: createMockLastEvaluatedKey(),
      });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({ cursor });

      expect(result).toContain('<a class="btn">Previous</a>');
      expect(result).toContain('<a class="btn btn-primary">Next</a>');
    });

    it("should document pagination in describe output", async () => {
      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-errors-widget/index.js"
      );

      const result = await handler({ describe: true });

      const parsed = JSON.parse(result);
      expect(parsed.markdown).toContain("Pagination");
      expect(parsed.markdown).toContain("Previous");
      expect(parsed.markdown).toContain("Next");
    });
  });
});
