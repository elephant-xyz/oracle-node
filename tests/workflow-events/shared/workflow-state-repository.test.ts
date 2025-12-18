import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import {
  DynamoDBDocumentClient,
  TransactWriteCommand,
  GetCommand,
  QueryCommand,
} from "@aws-sdk/lib-dynamodb";
import type { WorkflowEventDetail, StepStatus } from "shared/types.js";
import {
  normalizeStepStatusToBucket,
  normalizeDataGroupLabel,
  getBucketCountAttribute,
  buildExecutionStatePK,
  buildStepAggregatePK,
  buildStepAggregateSK,
  computeShardIndex,
  GSI2_SHARD_COUNT,
} from "shared/keys.js";

/**
 * Mock the DynamoDB Document Client for all tests.
 */
const ddbMock = mockClient(DynamoDBDocumentClient);

/**
 * Test table name for workflow state used in all tests.
 */
const TEST_STATE_TABLE_NAME = "test-workflow-state-table";

/**
 * Creates a workflow event detail with optional overrides.
 * @param overrides - Partial overrides for the detail
 * @returns A complete WorkflowEventDetail object
 */
const createWorkflowDetail = (
  overrides: Partial<WorkflowEventDetail> = {},
): WorkflowEventDetail => ({
  executionId: "exec-001",
  county: "palm_beach",
  status: "IN_PROGRESS",
  phase: "prepare",
  step: "download",
  errors: [],
  ...overrides,
});

describe("workflow-state-repository", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.resetModules();
    ddbMock.reset();
    process.env = {
      ...originalEnv,
      WORKFLOW_STATE_TABLE_NAME: TEST_STATE_TABLE_NAME,
    };
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "info").mockImplementation(() => {});
    vi.spyOn(console, "debug").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});

    // Default mocks
    ddbMock.on(GetCommand).resolves({ Item: undefined });
    ddbMock.on(TransactWriteCommand).resolves({});
    ddbMock.on(QueryCommand).resolves({ Items: [] });
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  describe("normalizeStepStatusToBucket", () => {
    it("should normalize SUCCEEDED to SUCCEEDED", () => {
      expect(normalizeStepStatusToBucket("SUCCEEDED")).toBe("SUCCEEDED");
    });

    it("should normalize FAILED to FAILED", () => {
      expect(normalizeStepStatusToBucket("FAILED")).toBe("FAILED");
    });

    it("should normalize IN_PROGRESS to IN_PROGRESS", () => {
      expect(normalizeStepStatusToBucket("IN_PROGRESS")).toBe("IN_PROGRESS");
    });

    it("should normalize SCHEDULED to IN_PROGRESS", () => {
      expect(normalizeStepStatusToBucket("SCHEDULED")).toBe("IN_PROGRESS");
    });

    it("should normalize PARKED to IN_PROGRESS", () => {
      expect(normalizeStepStatusToBucket("PARKED")).toBe("IN_PROGRESS");
    });
  });

  describe("normalizeDataGroupLabel", () => {
    it("should return the label when provided", () => {
      expect(normalizeDataGroupLabel("group-1")).toBe("group-1");
    });

    it("should return 'not-set' when undefined", () => {
      expect(normalizeDataGroupLabel(undefined)).toBe("not-set");
    });

    it("should return 'not-set' when empty string", () => {
      expect(normalizeDataGroupLabel("")).toBe("not-set");
    });

    it("should return 'not-set' when whitespace only", () => {
      expect(normalizeDataGroupLabel("   ")).toBe("not-set");
    });
  });

  describe("getBucketCountAttribute", () => {
    it("should return inProgressCount for IN_PROGRESS", () => {
      expect(getBucketCountAttribute("IN_PROGRESS")).toBe("inProgressCount");
    });

    it("should return failedCount for FAILED", () => {
      expect(getBucketCountAttribute("FAILED")).toBe("failedCount");
    });

    it("should return succeededCount for SUCCEEDED", () => {
      expect(getBucketCountAttribute("SUCCEEDED")).toBe("succeededCount");
    });
  });

  describe("key builders", () => {
    it("should build execution state PK correctly", () => {
      expect(buildExecutionStatePK("exec-123")).toBe("EXECUTION#exec-123");
    });

    it("should build step aggregate PK correctly", () => {
      expect(buildStepAggregatePK("palm_beach", "group-1")).toBe(
        "AGG#COUNTY#palm_beach#DG#group-1",
      );
    });

    it("should build step aggregate SK correctly", () => {
      expect(buildStepAggregateSK("prepare", "download")).toBe(
        "PHASE#prepare#STEP#download",
      );
    });
  });

  describe("sharding", () => {
    it("should compute consistent shard index for same input", () => {
      const input = "palm_beach#group-1";
      const shard1 = computeShardIndex(input);
      const shard2 = computeShardIndex(input);
      expect(shard1).toBe(shard2);
    });

    it("should compute shard index within valid range", () => {
      const inputs = [
        "palm_beach#group-1",
        "broward#group-2",
        "miami_dade#not-set",
        "orange#group-3",
      ];

      for (const input of inputs) {
        const shard = computeShardIndex(input);
        expect(shard).toBeGreaterThanOrEqual(0);
        expect(shard).toBeLessThan(GSI2_SHARD_COUNT);
      }
    });

    it("should distribute different inputs across shards", () => {
      const shards = new Set<number>();
      const counties = [
        "palm_beach",
        "broward",
        "miami_dade",
        "orange",
        "hillsborough",
        "pinellas",
        "duval",
        "seminole",
      ];

      for (const county of counties) {
        shards.add(computeShardIndex(`${county}#not-set`));
      }

      // Should have some distribution (at least 2 different shards)
      expect(shards.size).toBeGreaterThan(1);
    });
  });

  describe("upsertExecutionStateAndUpdateAggregates", () => {
    it("should create new execution state for new execution", async () => {
      const {
        upsertExecutionStateAndUpdateAggregates,
      } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-new-001",
        county: "broward",
        phase: "prepare",
        step: "download",
        dataGroupLabel: "group-1",
      });

      const result = await upsertExecutionStateAndUpdateAggregates({
        detail,
        eventId: "event-123",
        eventTime: new Date().toISOString(),
      });

      expect(result.success).toBe(true);
      expect(result.skipped).toBe(false);
      expect(result.previousState).toBeNull();
      expect(result.newState.executionId).toBe("exec-new-001");
      expect(result.newState.bucket).toBe("IN_PROGRESS");

      // Verify transaction was sent
      expect(ddbMock).toHaveReceivedCommand(TransactWriteCommand);

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      expect(calls.length).toBe(1);

      // Should have execution state Put and aggregate increment
      const transactItems = calls[0].args[0].input.TransactItems;
      expect(transactItems?.length).toBe(2);
    });

    it("should update existing execution state on state transition", async () => {
      // Mock existing execution state
      ddbMock.on(GetCommand).resolves({
        Item: {
          PK: "EXECUTION#exec-existing-001",
          SK: "EXECUTION#exec-existing-001",
          entityType: "ExecutionState",
          executionId: "exec-existing-001",
          county: "palm_beach",
          dataGroupLabel: "not-set",
          phase: "prepare",
          step: "download",
          bucket: "IN_PROGRESS",
          rawStatus: "IN_PROGRESS",
          lastEventTime: "2024-01-01T00:00:00Z",
          createdAt: "2024-01-01T00:00:00Z",
          updatedAt: "2024-01-01T00:00:00Z",
          version: 1,
        },
      });

      const {
        upsertExecutionStateAndUpdateAggregates,
      } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-existing-001",
        county: "palm_beach",
        status: "SUCCEEDED",
        phase: "prepare",
        step: "download",
      });

      const result = await upsertExecutionStateAndUpdateAggregates({
        detail,
        eventId: "event-456",
        eventTime: "2024-01-01T01:00:00Z",
      });

      expect(result.success).toBe(true);
      expect(result.skipped).toBe(false);
      expect(result.previousState).not.toBeNull();
      expect(result.previousState?.bucket).toBe("IN_PROGRESS");
      expect(result.newState.bucket).toBe("SUCCEEDED");

      // Verify transaction includes execution update and both aggregate updates
      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;

      // Should have: execution state update + decrement old bucket + increment new bucket
      expect(transactItems?.length).toBe(3);
    });

    it("should skip out-of-order events", async () => {
      // Mock existing execution state with later event time
      ddbMock.on(GetCommand).resolves({
        Item: {
          PK: "EXECUTION#exec-ooo-001",
          SK: "EXECUTION#exec-ooo-001",
          entityType: "ExecutionState",
          executionId: "exec-ooo-001",
          county: "palm_beach",
          dataGroupLabel: "not-set",
          phase: "prepare",
          step: "download",
          bucket: "SUCCEEDED",
          rawStatus: "SUCCEEDED",
          lastEventTime: "2024-01-01T02:00:00Z", // Later than incoming event
          createdAt: "2024-01-01T00:00:00Z",
          updatedAt: "2024-01-01T02:00:00Z",
          version: 2,
        },
      });

      const {
        upsertExecutionStateAndUpdateAggregates,
      } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-ooo-001",
        status: "IN_PROGRESS",
      });

      const result = await upsertExecutionStateAndUpdateAggregates({
        detail,
        eventId: "event-old",
        eventTime: "2024-01-01T01:00:00Z", // Earlier than stored
      });

      expect(result.success).toBe(true);
      expect(result.skipped).toBe(true);

      // Should NOT have sent any TransactWriteCommand (we reset mock above)
      // Actually, we need to check the calls - there should be no calls after the skip
      const writeCalls = ddbMock.commandCalls(TransactWriteCommand);
      expect(writeCalls.length).toBe(0);
    });

    it("should only update timestamps when bucket and keys unchanged", async () => {
      // Mock existing execution state with same bucket and keys
      ddbMock.on(GetCommand).resolves({
        Item: {
          PK: "EXECUTION#exec-same-001",
          SK: "EXECUTION#exec-same-001",
          entityType: "ExecutionState",
          executionId: "exec-same-001",
          county: "palm_beach",
          dataGroupLabel: "not-set",
          phase: "prepare",
          step: "download",
          bucket: "IN_PROGRESS",
          rawStatus: "IN_PROGRESS",
          lastEventTime: "2024-01-01T00:00:00Z",
          createdAt: "2024-01-01T00:00:00Z",
          updatedAt: "2024-01-01T00:00:00Z",
          version: 1,
        },
      });

      const {
        upsertExecutionStateAndUpdateAggregates,
      } = await import("shared/repository.js");

      const detail = createWorkflowDetail({
        executionId: "exec-same-001",
        county: "palm_beach",
        status: "IN_PROGRESS",
        phase: "prepare",
        step: "download",
      });

      const result = await upsertExecutionStateAndUpdateAggregates({
        detail,
        eventId: "event-same",
        eventTime: "2024-01-01T01:00:00Z",
      });

      expect(result.success).toBe(true);
      expect(result.skipped).toBe(false);

      // Transaction should only have execution state update (no aggregate changes)
      const calls = ddbMock.commandCalls(TransactWriteCommand);
      const transactItems = calls[0].args[0].input.TransactItems;
      expect(transactItems?.length).toBe(1);
    });

    it("should use idempotency token from event ID", async () => {
      const {
        upsertExecutionStateAndUpdateAggregates,
      } = await import("shared/repository.js");

      const detail = createWorkflowDetail({ executionId: "exec-token-001" });

      await upsertExecutionStateAndUpdateAggregates({
        detail,
        eventId: "unique-event-id-12345",
        eventTime: new Date().toISOString(),
      });

      const calls = ddbMock.commandCalls(TransactWriteCommand);
      expect(calls[0].args[0].input.ClientRequestToken).toBe(
        "unique-event-id-12345",
      );
    });
  });

  describe("queryStepAggregatesForCounty", () => {
    it("should query aggregates for county and data group", async () => {
      ddbMock.on(QueryCommand).resolves({
        Items: [
          {
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
          },
        ],
      });

      const { queryStepAggregatesForCounty } =
        await import("shared/repository.js");

      const result = await queryStepAggregatesForCounty({
        county: "palm_beach",
        dataGroupLabel: "not-set",
      });

      expect(result.length).toBe(1);
      expect(result[0].county).toBe("palm_beach");
      expect(result[0].inProgressCount).toBe(5);

      // Verify query was correct
      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.KeyConditionExpression).toBe("PK = :pk");
      expect(calls[0].args[0].input.ExpressionAttributeValues?.[":pk"]).toBe(
        "AGG#COUNTY#palm_beach#DG#not-set",
      );
    });

    it("should filter by phase when provided", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { queryStepAggregatesForCounty } =
        await import("shared/repository.js");

      await queryStepAggregatesForCounty({
        county: "palm_beach",
        dataGroupLabel: "not-set",
        phase: "prepare",
      });

      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls[0].args[0].input.KeyConditionExpression).toBe(
        "PK = :pk AND begins_with(SK, :skPrefix)",
      );
      expect(
        calls[0].args[0].input.ExpressionAttributeValues?.[":skPrefix"],
      ).toBe("PHASE#prepare#");
    });
  });

  describe("queryAllStepAggregates", () => {
    it("should query all shards and combine results", async () => {
      // Mock responses for different shards
      ddbMock.on(QueryCommand).resolves({
        Items: [
          {
            PK: "AGG#COUNTY#palm_beach#DG#not-set",
            SK: "PHASE#prepare#STEP#download",
            entityType: "StepAggregate",
            county: "palm_beach",
            dataGroupLabel: "not-set",
            phase: "prepare",
            step: "download",
            inProgressCount: 3,
            failedCount: 1,
            succeededCount: 5,
          },
        ],
      });

      const { queryAllStepAggregates } = await import("shared/repository.js");

      const result = await queryAllStepAggregates({});

      // Should have queried all 16 shards
      const calls = ddbMock.commandCalls(QueryCommand);
      expect(calls.length).toBe(GSI2_SHARD_COUNT);

      // Results should be combined
      expect(result.items.length).toBe(GSI2_SHARD_COUNT); // One item per shard in our mock
    });

    it("should return hasMore false when all shards complete", async () => {
      ddbMock.on(QueryCommand).resolves({ Items: [] });

      const { queryAllStepAggregates } = await import("shared/repository.js");

      const result = await queryAllStepAggregates({});

      expect(result.hasMore).toBe(false);
      expect(result.nextCursor).toBeNull();
    });

    it("should return cursor when shards have more data", async () => {
      ddbMock.on(QueryCommand).resolves({
        Items: [{ PK: "test", SK: "test" }],
        LastEvaluatedKey: { GSI2PK: "AGG#ALL#S#00", GSI2SK: "test" },
      });

      const { queryAllStepAggregates } = await import("shared/repository.js");

      const result = await queryAllStepAggregates({ limit: 10 });

      expect(result.hasMore).toBe(true);
      expect(result.nextCursor).not.toBeNull();
      expect(result.nextCursor?.shardCursors).toBeDefined();
    });
  });

  describe("getStepAggregate", () => {
    it("should get single step aggregate", async () => {
      ddbMock.on(GetCommand).resolves({
        Item: {
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
        },
      });

      const { getStepAggregate } = await import("shared/repository.js");

      const result = await getStepAggregate(
        "palm_beach",
        "not-set",
        "prepare",
        "download",
      );

      expect(result).not.toBeNull();
      expect(result?.county).toBe("palm_beach");
      expect(result?.phase).toBe("prepare");
      expect(result?.step).toBe("download");
      expect(result?.inProgressCount).toBe(5);
    });

    it("should return null when not found", async () => {
      ddbMock.on(GetCommand).resolves({ Item: undefined });

      const { getStepAggregate } = await import("shared/repository.js");

      const result = await getStepAggregate(
        "nonexistent",
        "not-set",
        "prepare",
        "download",
      );

      expect(result).toBeNull();
    });
  });
});

