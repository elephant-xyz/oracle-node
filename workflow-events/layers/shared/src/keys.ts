import type { ErrorStatus, StepStatus, ExecutionBucket } from "./types.js";

/**
 * Entity type discriminators for single-table design.
 */
export const ENTITY_TYPES = {
  ERROR: "Error",
  EXECUTION_ERROR: "ExecutionError",
  FAILED_EXECUTION: "FailedExecution",
} as const;

/**
 * Entity type discriminators for the workflow-state table.
 */
export const STATE_ENTITY_TYPES = {
  EXECUTION_STATE: "ExecutionState",
  STEP_AGGREGATE: "StepAggregate",
} as const;

/**
 * Default GSI status for new error records and executions.
 * Used in GSI sort keys to enable filtering by status.
 */
export const DEFAULT_GSI_STATUS = "FAILED" as const;

/**
 * GS3 partition key for FailedExecutionItem records.
 * Used to query executions by error count with optional errorType filter.
 */
export const GS3_EXECUTION_PK = "METRIC#ERRORCOUNT" as const;

/**
 * GS3 partition key for ErrorRecord records.
 * Separate from GS3_EXECUTION_PK to enable partition-level separation,
 * avoiding the need for FilterExpression when querying by errorType.
 */
export const GS3_ERROR_PK = "METRIC#ERRORCOUNT#ERROR" as const;

/**
 * GSI status for errors that failed AI resolution (maybeUnrecoverable).
 * Used in GSI sort keys to filter items marked as unrecoverable.
 */
export const UNRECOVERABLE_GSI_STATUS = "MAYBEUNRECOVERABLE" as const;

/**
 * Converts an ErrorStatus value to uppercase for use in GSI keys.
 * @param status - The error status (e.g., "failed", "maybeSolved", "solved")
 * @returns Uppercase status string for GSI keys (e.g., "FAILED", "MAYBESOLVED", "SOLVED")
 */
export const toGsiStatus = (status: ErrorStatus): string => {
  return status.toUpperCase();
};

/**
 * Pads a number with leading zeros for lexicographic sorting.
 * @param count - The number to pad
 * @param length - The total length of the resulting string
 * @returns Zero-padded string representation
 */
export const padCount = (count: number, length: number = 10): string => {
  return count.toString().padStart(length, "0");
};

/**
 * Extracts the error type from an error code.
 * The error type is the first 2 characters of the error code.
 * For error codes shorter than 2 characters, returns the code itself.
 * @param errorCode - The error code to extract the type from
 * @returns The error type (first 2 characters or full code if shorter)
 */
export const extractErrorType = (errorCode: string): string => {
  return errorCode.length >= 2 ? errorCode.substring(0, 2) : errorCode;
};

/**
 * Creates a composite key for DynamoDB.
 * @param prefix - The key prefix (e.g., "ERROR", "EXECUTION")
 * @param value - The key value
 * @returns Composite key string
 */
export const createKey = (prefix: string, value: string): string => {
  return `${prefix}#${value}`;
};

// =============================================================================
// Workflow State Table Key Helpers
// =============================================================================

/**
 * Default data group label when none is provided.
 */
export const DEFAULT_DATA_GROUP_LABEL = "not-set" as const;

/**
 * Number of shards for the GSI2 "list all aggregates" index.
 * Using 16 shards (00-15) to distribute write load.
 */
export const GSI2_SHARD_COUNT = 16;

/**
 * Normalizes a StepStatus to an ExecutionBucket.
 * - SCHEDULED → IN_PROGRESS
 * - PARKED → IN_PROGRESS
 * - IN_PROGRESS → IN_PROGRESS
 * - FAILED → FAILED
 * - SUCCEEDED → SUCCEEDED
 *
 * @param status - The raw step status from the event
 * @returns The normalized execution bucket
 */
export const normalizeStepStatusToBucket = (
  status: StepStatus,
): ExecutionBucket => {
  switch (status) {
    case "SUCCEEDED":
      return "SUCCEEDED";
    case "FAILED":
    case "PARKED":
      return "FAILED";
    case "IN_PROGRESS":
    case "SCHEDULED":
      return "IN_PROGRESS";
  }
};

/**
 * Returns the attribute name for the count field corresponding to a bucket.
 * @param bucket - The execution bucket
 * @returns The DynamoDB attribute name for the count field
 */
export const getBucketCountAttribute = (
  bucket: ExecutionBucket,
): "inProgressCount" | "failedCount" | "succeededCount" => {
  switch (bucket) {
    case "IN_PROGRESS":
      return "inProgressCount";
    case "FAILED":
      return "failedCount";
    case "SUCCEEDED":
      return "succeededCount";
  }
};

/**
 * Computes a stable shard index (0 to GSI2_SHARD_COUNT - 1) from a string.
 * Uses a simple hash function to distribute values evenly across shards.
 *
 * @param value - The string to hash (typically county + dataGroupLabel)
 * @returns A shard index between 0 and GSI2_SHARD_COUNT - 1
 */
export const computeShardIndex = (value: string): number => {
  let hash = 0;
  for (let i = 0; i < value.length; i++) {
    const char = value.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  return Math.abs(hash) % GSI2_SHARD_COUNT;
};

/**
 * Formats a shard index as a zero-padded string (e.g., "00", "01", ..., "15").
 * @param shardIndex - The shard index
 * @returns Zero-padded shard string
 */
export const formatShardKey = (shardIndex: number): string => {
  return shardIndex.toString().padStart(2, "0");
};

/**
 * Gets the normalized data group label, using DEFAULT_DATA_GROUP_LABEL if not provided.
 * @param dataGroupLabel - The optional data group label from the event
 * @returns The normalized data group label
 */
export const normalizeDataGroupLabel = (
  dataGroupLabel: string | undefined,
): string => {
  return dataGroupLabel && dataGroupLabel.trim() !== ""
    ? dataGroupLabel
    : DEFAULT_DATA_GROUP_LABEL;
};

// =============================================================================
// ExecutionStateItem Key Builders
// =============================================================================

/**
 * Builds the primary key (PK) for an ExecutionStateItem.
 * @param executionId - The execution identifier
 * @returns The PK value: `EXECUTION#${executionId}`
 */
export const buildExecutionStatePK = (executionId: string): string => {
  return `EXECUTION#${executionId}`;
};

/**
 * Builds the sort key (SK) for an ExecutionStateItem.
 * Same as PK for execution state items.
 * @param executionId - The execution identifier
 * @returns The SK value: `EXECUTION#${executionId}`
 */
export const buildExecutionStateSK = (executionId: string): string => {
  return `EXECUTION#${executionId}`;
};

// =============================================================================
// StepAggregateItem Key Builders
// =============================================================================

/**
 * Builds the primary key (PK) for a StepAggregateItem.
 * @param county - The county identifier
 * @param dataGroupLabel - The normalized data group label
 * @returns The PK value: `AGG#COUNTY#${county}#DG#${dataGroupLabel}`
 */
export const buildStepAggregatePK = (
  county: string,
  dataGroupLabel: string,
): string => {
  return `AGG#COUNTY#${county}#DG#${dataGroupLabel}`;
};

/**
 * Builds the sort key (SK) for a StepAggregateItem.
 * @param phase - The phase name
 * @param step - The step name
 * @returns The SK value: `PHASE#${phase}#STEP#${step}`
 */
export const buildStepAggregateSK = (phase: string, step: string): string => {
  return `PHASE#${phase}#STEP#${step}`;
};

/**
 * Builds the GSI1 partition key for a StepAggregateItem.
 * Enables querying aggregates by phase+step across counties.
 * @param phase - The phase name
 * @param step - The step name
 * @param dataGroupLabel - The normalized data group label
 * @returns The GSI1PK value: `AGG#PHASE#${phase}#STEP#${step}#DG#${dataGroupLabel}`
 */
export const buildStepAggregateGSI1PK = (
  phase: string,
  step: string,
  dataGroupLabel: string,
): string => {
  return `AGG#PHASE#${phase}#STEP#${step}#DG#${dataGroupLabel}`;
};

/**
 * Builds the GSI1 sort key for a StepAggregateItem.
 * @param county - The county identifier
 * @returns The GSI1SK value: `COUNTY#${county}`
 */
export const buildStepAggregateGSI1SK = (county: string): string => {
  return `COUNTY#${county}`;
};

/**
 * Builds the GSI2 partition key for a StepAggregateItem (sharded).
 * Enables listing all aggregates without knowing counties/data groups.
 * @param county - The county identifier
 * @param dataGroupLabel - The normalized data group label
 * @returns The GSI2PK value: `AGG#ALL#S#${shard}`
 */
export const buildStepAggregateGSI2PK = (
  county: string,
  dataGroupLabel: string,
): string => {
  const shardIndex = computeShardIndex(`${county}#${dataGroupLabel}`);
  return `AGG#ALL#S#${formatShardKey(shardIndex)}`;
};

/**
 * Builds the GSI2 sort key for a StepAggregateItem.
 * @param dataGroupLabel - The normalized data group label
 * @param phase - The phase name
 * @param step - The step name
 * @param county - The county identifier
 * @returns The GSI2SK value: `DG#${dataGroupLabel}#PHASE#${phase}#STEP#${step}#COUNTY#${county}`
 */
export const buildStepAggregateGSI2SK = (
  dataGroupLabel: string,
  phase: string,
  step: string,
  county: string,
): string => {
  return `DG#${dataGroupLabel}#PHASE#${phase}#STEP#${step}#COUNTY#${county}`;
};

/**
 * Builds the GSI2PK for a specific shard index.
 * Used when querying all shards to list all aggregates.
 * @param shardIndex - The shard index (0 to GSI2_SHARD_COUNT - 1)
 * @returns The GSI2PK value: `AGG#ALL#S#${shard}`
 */
export const buildGSI2PKForShard = (shardIndex: number): string => {
  return `AGG#ALL#S#${formatShardKey(shardIndex)}`;
};
