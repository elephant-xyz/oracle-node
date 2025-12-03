import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
import type { FailedExecutionItem } from "shared/types.js";

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
  executionId: "exec-001-abcdef-1234567890-ghijkl-mnopqr-stuvwx",
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
  GS1SK: "COUNT#0000000003#EXECUTION#exec-001",
  GS3PK: "METRIC#ERRORCOUNT",
  GS3SK: "COUNT#01#0000000003#EXECUTION#exec-001",
  ...overrides,
});

describe("dashboard-executions-widget handler", () => {
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
        "../../../../workflow-events/lambdas/dashboard-executions-widget/index.js"
      );

      const result = await handler({ describe: true });

      const parsed = JSON.parse(result);
      expect(parsed.markdown).toContain("Executions with Most Errors");
      expect(parsed.markdown).toContain("Parameters");
      expect(parsed.markdown).toContain("limit");
    });

    it("should not query DynamoDB when describe is true", async () => {
      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-executions-widget/index.js"
      );

      await handler({ describe: true });

      expect(ddbMock).toHaveReceivedCommandTimes(QueryCommand, 0);
    });
  });

  describe("HTML generation", () => {
    it("should return HTML table with executions", async () => {
      const mockExecutions = [
        createMockExecution({
          executionId: "exec-001-uuid",
          county: "palm_beach",
          openErrorCount: 5,
        }),
        createMockExecution({
          executionId: "exec-002-uuid",
          county: "broward",
          openErrorCount: 3,
        }),
      ];

      ddbMock.on(QueryCommand).resolves({ Items: mockExecutions });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-executions-widget/index.js"
      );

      const result = await handler({});

      expect(result).toContain("<table");
      expect(result).toContain("palm_beach");
      expect(result).toContain("broward");
      expect(result).toContain("Execution ID");
      expect(result).toContain("County");
      expect(result).toContain("Error Type");
      expect(result).toContain("Open Errors");
    });

    it("should return 'no executions found' message when empty", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-executions-widget/index.js"
      );

      const result = await handler({});

      expect(result).toContain("No failed executions found");
    });

    it("should escape HTML special characters", async () => {
      const mockExecution = createMockExecution({
        county: "<script>alert('xss')</script>",
      });

      ddbMock.on(QueryCommand).resolves({ Items: [mockExecution] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-executions-widget/index.js"
      );

      const result = await handler({});

      expect(result).not.toContain("<script>");
      expect(result).toContain("&lt;script&gt;");
    });
  });

  describe("DynamoDB query", () => {
    it("should query GS1 index with METRIC#ERRORCOUNT partition key", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-executions-widget/index.js"
      );

      await handler({});

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.IndexName).toBe("GS1");
      expect(calls[0].args[0].input.KeyConditionExpression).toBe(
        "GS1PK = :gs1pk",
      );
      expect(calls[0].args[0].input.ExpressionAttributeValues).toMatchObject({
        ":gs1pk": "METRIC#ERRORCOUNT",
      });
    });

    it("should sort descending (most errors first)", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-executions-widget/index.js"
      );

      await handler({});

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.ScanIndexForward).toBe(false);
    });

    it("should use default limit of 20", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-executions-widget/index.js"
      );

      await handler({});

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.Limit).toBe(20);
    });

    it("should use custom limit when provided", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-executions-widget/index.js"
      );

      await handler({ limit: 10 });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.Limit).toBe(10);
    });

    it("should filter by FailedExecution entity type", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-executions-widget/index.js"
      );

      await handler({});

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.FilterExpression).toBe(
        "entityType = :entityType",
      );
      expect(calls[0].args[0].input.ExpressionAttributeValues).toMatchObject({
        ":entityType": "FailedExecution",
      });
    });
  });

  describe("error handling", () => {
    it("should return error HTML when DynamoDB query fails", async () => {
      ddbMock.on(QueryCommand).rejects(new Error("DynamoDB connection error"));

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-executions-widget/index.js"
      );

      const result = await handler({});

      expect(result).toContain("Error loading data");
      expect(result).toContain("DynamoDB connection error");
    });

    it("should return error HTML when table name is not set", async () => {
      delete process.env.WORKFLOW_ERRORS_TABLE_NAME;

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-executions-widget/index.js"
      );

      const result = await handler({});

      expect(result).toContain("Error loading data");
      expect(result).toContain("WORKFLOW_ERRORS_TABLE_NAME");
    });
  });

  describe("widget context", () => {
    it("should work with widget context provided", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { handler } = await import(
        "../../../../workflow-events/lambdas/dashboard-executions-widget/index.js"
      );

      const result = await handler({
        widgetContext: {
          dashboardName: "test-dashboard",
          widgetId: "widget-1",
          accountId: "123456789012",
          height: 300,
          width: 400,
        },
      });

      expect(result).toContain("No failed executions found");
    });
  });
});
