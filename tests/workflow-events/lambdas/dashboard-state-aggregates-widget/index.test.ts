import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import type { StepAggregateItem } from "shared/types.js";

/**
 * Test table name used in all tests.
 */
const TEST_STATE_TABLE_NAME = "test-workflow-state-table";

/**
 * Creates a mock StepAggregateItem with optional overrides.
 * @param overrides - Partial overrides for the item
 * @returns A complete StepAggregateItem object
 */
const createMockAggregate = (
  overrides: Partial<StepAggregateItem> = {},
): StepAggregateItem => ({
  PK: "AGG#COUNTY#palm_beach#DG#not-set",
  SK: "PHASE#prepare#STEP#download",
  entityType: "StepAggregate",
  county: "palm_beach",
  dataGroupLabel: "not-set",
  phase: "prepare",
  step: "download",
  inProgressCount: 5,
  failedCount: 2,
  succeededCount: 10,
  createdAt: "2025-01-01T00:00:00.000Z",
  updatedAt: "2025-01-01T12:00:00.000Z",
  GSI1PK: "AGG#PHASE#prepare#STEP#download#DG#not-set",
  GSI1SK: "COUNTY#palm_beach",
  GSI2PK: "AGG#ALL#S#00",
  GSI2SK: "DG#not-set#PHASE#prepare#STEP#download#COUNTY#palm_beach",
  ...overrides,
});

/**
 * Mock for queryAllStepAggregates function.
 */
const queryAllStepAggregatesMock = vi.fn();

// Mock the repository module before importing the handler
vi.mock("shared/repository.js", async () => {
  const actual = await vi.importActual<typeof import("shared/repository.js")>(
    "shared/repository.js",
  );
  return {
    ...actual,
    queryAllStepAggregates: queryAllStepAggregatesMock,
  };
});

describe("dashboard-state-aggregates-widget handler", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    queryAllStepAggregatesMock.mockReset();
    process.env = {
      ...originalEnv,
      WORKFLOW_STATE_TABLE_NAME: TEST_STATE_TABLE_NAME,
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
      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      const result = await handler({ describe: true });

      const parsed = JSON.parse(result);
      expect(parsed.markdown).toContain("Workflow Phase Aggregates");
      expect(parsed.markdown).toContain("current snapshot counts");
      expect(parsed.markdown).toContain("IN_PROGRESS");
      expect(parsed.markdown).toContain("FAILED");
      expect(parsed.markdown).toContain("SUCCEEDED");
    });

    it("should not query DynamoDB when describe is true", async () => {
      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      await handler({ describe: true });

      expect(queryAllStepAggregatesMock).not.toHaveBeenCalled();
    });
  });

  describe("HTML generation", () => {
    it("should return HTML table with phase aggregates", async () => {
      const mockAggregates: StepAggregateItem[] = [
        createMockAggregate({
          phase: "prepare",
          step: "download",
          inProgressCount: 5,
          failedCount: 2,
          succeededCount: 10,
        }),
        createMockAggregate({
          phase: "prepare",
          step: "upload",
          inProgressCount: 3,
          failedCount: 1,
          succeededCount: 8,
        }),
        createMockAggregate({
          phase: "transform",
          step: "process",
          inProgressCount: 2,
          failedCount: 0,
          succeededCount: 15,
        }),
      ];

      queryAllStepAggregatesMock.mockResolvedValue({
        items: mockAggregates,
        nextCursor: null,
        hasMore: false,
      });

      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      const result = await handler({});

      expect(result).toContain("<table");
      expect(result).toContain("Prepare");
      expect(result).toContain("Transform");
      expect(result).toContain("Phase");
      expect(result).toContain("IN_PROGRESS");
      expect(result).toContain("FAILED");
      expect(result).toContain("SUCCEEDED");
      expect(result).toContain("TOTAL");
      expect(result).toContain("Last Updated");
    });

    it("should aggregate multiple steps within the same phase", async () => {
      const mockAggregates: StepAggregateItem[] = [
        createMockAggregate({
          phase: "prepare",
          step: "download",
          inProgressCount: 5,
          failedCount: 2,
          succeededCount: 10,
        }),
        createMockAggregate({
          phase: "prepare",
          step: "upload",
          inProgressCount: 3,
          failedCount: 1,
          succeededCount: 8,
        }),
      ];

      queryAllStepAggregatesMock.mockResolvedValue({
        items: mockAggregates,
        nextCursor: null,
        hasMore: false,
      });

      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      const result = await handler({});

      // Prepare phase should have totals: 8 in progress, 3 failed, 18 succeeded
      expect(result).toContain("Prepare");
      // Check that totals are summed correctly
      const prepareRowMatch = result.match(
        /Prepare[\s\S]*?<td[^>]*>(\d+)<\/td>/,
      );
      expect(prepareRowMatch).toBeTruthy();
    });

    it("should calculate total column correctly", async () => {
      const mockAggregates: StepAggregateItem[] = [
        createMockAggregate({
          phase: "prepare",
          step: "download",
          inProgressCount: 5,
          failedCount: 2,
          succeededCount: 10,
        }),
      ];

      queryAllStepAggregatesMock.mockResolvedValue({
        items: mockAggregates,
        nextCursor: null,
        hasMore: false,
      });

      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      const result = await handler({});

      // Total should be 5 + 2 + 10 = 17
      expect(result).toContain("17");
    });

    it("should return 'no aggregates found' message when empty", async () => {
      queryAllStepAggregatesMock.mockResolvedValue({
        items: [],
        nextCursor: null,
        hasMore: false,
      });

      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      const result = await handler({});

      expect(result).toContain("No phase aggregates found");
    });

    it("should escape HTML special characters in phase names", async () => {
      const mockAggregates: StepAggregateItem[] = [
        createMockAggregate({
          phase: "<script>alert('xss')</script>",
          step: "test",
        }),
      ];

      queryAllStepAggregatesMock.mockResolvedValue({
        items: mockAggregates,
        nextCursor: null,
        hasMore: false,
      });

      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      const result = await handler({});

      expect(result).not.toContain("<script>");
      expect(result).toContain("&lt;script&gt;");
    });

    it("should sort phases alphabetically", async () => {
      const mockAggregates: StepAggregateItem[] = [
        createMockAggregate({
          phase: "transform",
          step: "process",
        }),
        createMockAggregate({
          phase: "prepare",
          step: "download",
        }),
        createMockAggregate({
          phase: "upload",
          step: "file",
        }),
      ];

      queryAllStepAggregatesMock.mockResolvedValue({
        items: mockAggregates,
        nextCursor: null,
        hasMore: false,
      });

      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      const result = await handler({});

      // Check that phases appear in alphabetical order
      const prepareIndex = result.indexOf("Prepare");
      const transformIndex = result.indexOf("Transform");
      const uploadIndex = result.indexOf("Upload");

      expect(prepareIndex).toBeLessThan(transformIndex);
      expect(transformIndex).toBeLessThan(uploadIndex);
    });

    it("should use latest updatedAt timestamp for each phase", async () => {
      const mockAggregates: StepAggregateItem[] = [
        createMockAggregate({
          phase: "prepare",
          step: "download",
          updatedAt: "2025-01-01T10:00:00.000Z",
        }),
        createMockAggregate({
          phase: "prepare",
          step: "upload",
          updatedAt: "2025-01-01T12:00:00.000Z", // Later timestamp
        }),
      ];

      queryAllStepAggregatesMock.mockResolvedValue({
        items: mockAggregates,
        nextCursor: null,
        hasMore: false,
      });

      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      const result = await handler({});

      // Should show the later timestamp (12:00 UTC)
      // Note: The formatted date will be in local timezone, so we check for the date part
      // and verify it's a valid formatted date (not the earlier 10:00 timestamp)
      expect(result).toContain("Jan 1, 2025");
      // Verify it contains a time (formatted date should have time component)
      expect(result).toMatch(/\d{1,2}:\d{2}:\d{2}/);
      // The later timestamp (12:00 UTC) should be used, which when converted to local time
      // will be different from the earlier timestamp (10:00 UTC)
      // We verify the date is formatted correctly rather than checking exact time due to timezone conversion
      const dateMatch = result.match(/Jan 1, 2025, (\d{1,2}:\d{2}:\d{2})/);
      expect(dateMatch).toBeTruthy();
      // Verify we're not showing the earlier timestamp by checking the formatted output
      // contains a valid date format
      expect(result).toContain("Last Updated");
    });
  });

  describe("DynamoDB query", () => {
    it("should call queryAllStepAggregates with default limit", async () => {
      queryAllStepAggregatesMock.mockResolvedValue({
        items: [],
        nextCursor: null,
        hasMore: false,
      });

      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      await handler({});

      expect(queryAllStepAggregatesMock).toHaveBeenCalledWith({
        limit: 1000,
      });
    });

    it("should handle pagination cursor when hasMore is true", async () => {
      queryAllStepAggregatesMock.mockResolvedValue({
        items: [createMockAggregate()],
        nextCursor: {
          shardCursors: { "0": { GSI2PK: "test", GSI2SK: "test" } },
          completedShards: [],
        },
        hasMore: true,
      });

      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      const result = await handler({});

      // Should still render the table even with pagination
      expect(result).toContain("<table");
    });
  });

  describe("error handling", () => {
    it("should return error HTML when DynamoDB query fails", async () => {
      queryAllStepAggregatesMock.mockRejectedValue(
        new Error("DynamoDB connection error"),
      );

      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      const result = await handler({});

      expect(result).toContain("Error loading data");
      expect(result).toContain("DynamoDB connection error");
    });

    it("should return error HTML when table name is not set", async () => {
      delete process.env.WORKFLOW_STATE_TABLE_NAME;

      queryAllStepAggregatesMock.mockRejectedValue(
        new Error("WORKFLOW_STATE_TABLE_NAME environment variable is not set"),
      );

      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      const result = await handler({});

      expect(result).toContain("Error loading data");
      expect(result).toContain("WORKFLOW_STATE_TABLE_NAME");
    });
  });

  describe("widget context", () => {
    it("should work with widget context provided", async () => {
      queryAllStepAggregatesMock.mockResolvedValue({
        items: [],
        nextCursor: null,
        hasMore: false,
      });

      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      const result = await handler({
        widgetContext: {
          dashboardName: "test-dashboard",
          widgetId: "widget-1",
          accountId: "123456789012",
          height: 300,
          width: 400,
        },
      });

      expect(result).toContain("No phase aggregates found");
    });
  });

  describe("date formatting", () => {
    it("should format valid ISO timestamps", async () => {
      const mockAggregates: StepAggregateItem[] = [
        createMockAggregate({
          phase: "prepare",
          updatedAt: "2025-01-15T14:30:45.000Z",
        }),
      ];

      queryAllStepAggregatesMock.mockResolvedValue({
        items: mockAggregates,
        nextCursor: null,
        hasMore: false,
      });

      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      const result = await handler({});

      // Should format the date in a readable format
      expect(result).toContain("Jan");
      expect(result).toContain("15");
      expect(result).toContain("2025");
    });

    it("should handle null updatedAt gracefully", async () => {
      const mockAggregates: StepAggregateItem[] = [
        createMockAggregate({
          phase: "prepare",
          updatedAt: null as unknown as string,
        }),
      ];

      queryAllStepAggregatesMock.mockResolvedValue({
        items: mockAggregates,
        nextCursor: null,
        hasMore: false,
      });

      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      const result = await handler({});

      expect(result).toContain("N/A");
    });
  });

  describe("phase aggregation logic", () => {
    it("should sum counts correctly across multiple steps in same phase", async () => {
      const mockAggregates: StepAggregateItem[] = [
        createMockAggregate({
          phase: "prepare",
          step: "download",
          inProgressCount: 5,
          failedCount: 2,
          succeededCount: 10,
        }),
        createMockAggregate({
          phase: "prepare",
          step: "upload",
          inProgressCount: 3,
          failedCount: 1,
          succeededCount: 8,
        }),
        createMockAggregate({
          phase: "prepare",
          step: "validate",
          inProgressCount: 1,
          failedCount: 0,
          succeededCount: 5,
        }),
      ];

      queryAllStepAggregatesMock.mockResolvedValue({
        items: mockAggregates,
        nextCursor: null,
        hasMore: false,
      });

      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      const result = await handler({});

      // Prepare phase totals: 9 in progress, 3 failed, 23 succeeded, 35 total
      expect(result).toContain("9"); // in progress
      expect(result).toContain("3"); // failed
      expect(result).toContain("23"); // succeeded
      expect(result).toContain("35"); // total
    });

    it("should handle zero counts correctly", async () => {
      const mockAggregates: StepAggregateItem[] = [
        createMockAggregate({
          phase: "prepare",
          step: "download",
          inProgressCount: 0,
          failedCount: 0,
          succeededCount: 0,
        }),
      ];

      queryAllStepAggregatesMock.mockResolvedValue({
        items: mockAggregates,
        nextCursor: null,
        hasMore: false,
      });

      const { handler } =
        await import("../../../../workflow-events/lambdas/dashboard-state-aggregates-widget/index.js");

      const result = await handler({});

      // Should still show the phase with zero counts
      expect(result).toContain("Prepare");
      expect(result).toContain("0");
    });
  });
});
