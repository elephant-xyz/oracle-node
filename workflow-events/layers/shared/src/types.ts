/**
 * Step status values from workflow events.
 */
export type StepStatus =
  | "SUCCEEDED"
  | "FAILED"
  | "PARKED"
  | "IN_PROGRESS"
  | "SCHEDULED";

/**
 * Normalized bucket for execution state aggregation.
 * SCHEDULED is merged into IN_PROGRESS; PARKED is treated as IN_PROGRESS.
 */
export type ExecutionBucket = "IN_PROGRESS" | "FAILED" | "SUCCEEDED";

export interface WorkflowError {
  /** Error code identifier. */
  code: string;
  /** Additional error details (key-value pairs). */
  details: Record<string, unknown>;
}

export interface WorkflowEventDetail {
  /** Unique identifier for the workflow execution. */
  executionId: string;
  /** County identifier associated with the execution. */
  county: string;
  /** Current status of the workflow execution. */
  status: StepStatus;
  /** Current phase of the workflow execution. */
  phase: string;
  /** Current step within the phase. */
  step: string;
  /** Data group used for the workflow execution. */
  dataGroupLabel?: string;
  /** Task token for Step Functions callback (if applicable). */
  taskToken?: string;
  /** Array of errors encountered during execution. */
  errors: WorkflowError[];
}

/**
 * Error status for execution-specific errors.
 */
export type ErrorStatus =
  | "failed"
  | "maybeSolved"
  | "solved"
  | "maybeUnrecoverable";

/**
 * Error record representing an error aggregate in DynamoDB.
 */
export interface ErrorRecord {
  /** Primary key in the format `ERROR#{errorCode}`. */
  PK: string;
  /** Sort key mirroring the primary key (`ERROR#{errorCode}`). */
  SK: string;
  /** Error code identifier. */
  errorCode: string;
  /** Error type. First 2 characters of the error code.*/
  errorType: string;
  /** Entity discriminator. */
  entityType: string;
  /** Error details JSON-encoded key-value pairs. */
  errorDetails: string;
  /** Error status. */
  errorStatus: ErrorStatus;
  /** Aggregate occurrence count across executions. */
  totalCount: number;
  /** ISO timestamp when the error aggregate was created. */
  createdAt: string;
  /** ISO timestamp when the aggregate was last updated. */
  updatedAt: string;
  /** Recent execution identifier that observed the error. */
  latestExecutionId: string;
  /** Global secondary index PK (`TYPE#ERROR`) for reverse lookup. */
  GS1PK: string;
  /** Global secondary index SK (`ERROR#{errorCode}`) for reverse lookup. */
  GS1SK: string;
  /** Global secondary index PK (`TYPE#ERROR`) for reverse lookup. */
  GS2PK: string;
  /** Global secondary index SK (`COUNT#{status}#{paddedCount}#ERROR#{errorCode}`) for count-based sorting. */
  GS2SK: string;
  /** Global secondary index partition key (`METRIC#ERRORCOUNT`). */
  GS3PK: string;
  /** Global secondary index sort key for the generalized count sort key (`COUNT#{errorType}#000010#ERROR#errorCode`). */
  GS3SK: string;
}

/**
 * Execution-error link item joining execution and error hash.
 */
export interface ExecutionErrorLink {
  /** Primary key in the format `EXECUTION#{executionId}`. */
  PK: string;
  /** Sort key in the format `ERROR#{errorCode}`. */
  SK: string;
  /** Entity discriminator (for example `ExecutionError`). */
  entityType: string;
  /** Error code identifier. */
  errorCode: string;
  /** Resolution status for the execution-specific error. */
  status: ErrorStatus;
  /** Occurrence count within the execution. */
  occurrences: number;
  /** Error details JSON-encoded key-value pairs. */
  errorDetails: string;
  /** Identifier of the failed execution. */
  executionId: string;
  /** County identifier. */
  county: string;
  /** ISO timestamp when the link item was created. */
  createdAt: string;
  /** ISO timestamp when the link item was last updated. */
  updatedAt: string;
  /** Global secondary index PK (`ERROR#{errorCode}`) for reverse lookup. */
  GS1PK: string;
  /** Global secondary index SK (`EXECUTION#{executionId}`) for reverse lookup. */
  GS1SK: string;
}

/**
 * Event detail for ElephantErrorResolved events.
 */
export interface ElephantErrorResolvedDetail {
  /** Execution ID to resolve all errors for. */
  executionId?: string;
  /** Error code to resolve from all executions. */
  errorCode?: string;
}

/**
 * Event detail for ElephantErrorFailedToResolve events.
 * Same structure as ElephantErrorResolvedDetail - marks errors as maybeUnrecoverable.
 */
export type ElephantErrorFailedToResolveDetail = ElephantErrorResolvedDetail;

/**
 * Failed execution item in DynamoDB.
 */
export interface FailedExecutionItem {
  /** Primary key in the format `EXECUTION#{executionId}`. */
  PK: string;
  /** Sort key mirroring the PK (`EXECUTION#{executionId}`). */
  SK: string;
  /** Identifier of the failed execution. */
  executionId: string;
  /** Entity discriminator (for example `FailedExecution`). */
  entityType: string;
  /** Execution status bucket (`failed` | `maybeSolved` | `solved`). */
  status: ErrorStatus;
  /** Error type. First 2 characters of the error code, that is common to all errors in the execution.*/
  errorType: string;
  /** County identifier. */
  county: string;
  /** Total error occurrences observed. */
  totalOccurrences: number;
  /** Count of unique unresolved errors. */
  openErrorCount: number;
  /** Count of unique errors in the execution. */
  uniqueErrorCount: number;
  /** Task token for Step Functions callback (if applicable). */
  taskToken?: string;
  /** ISO timestamp when the execution record was created. */
  createdAt: string;
  /** ISO timestamp when the execution record was updated. */
  updatedAt: string;
  /** Global secondary index partition key (`METRIC#ERRORCOUNT`). */
  GS1PK: string;
  /** Global secondary index sort key (`COUNT#{status}#{paddedCount}#EXECUTION#{executionId}`). */
  GS1SK: string;
  /** Global secondary index partition key (`METRIC#ERRORCOUNT`). */
  GS3PK: string;
  /** Global secondary index sort key (`COUNT#{errorType}#{status}#{paddedCount}#EXECUTION#{executionId}`). */
  GS3SK: string;
}

// =============================================================================
// Workflow State Table Types (new table for execution state + aggregates)
// =============================================================================

/**
 * Execution state item representing the latest known state of a workflow execution.
 * Stored in the workflow-state table.
 */
export interface ExecutionStateItem {
  /** Primary key: `EXECUTION#${executionId}`. */
  PK: string;
  /** Sort key: `EXECUTION#${executionId}`. */
  SK: string;
  /** Entity discriminator. */
  entityType: "ExecutionState";
  /** Unique identifier for the workflow execution. */
  executionId: string;
  /** County identifier. */
  county: string;
  /** Data group label (or "not-set" if not provided). */
  dataGroupLabel: string;
  /** Current phase of the workflow execution. */
  phase: string;
  /** Current step within the phase. */
  step: string;
  /** Normalized status bucket (SCHEDULED/PARKED → IN_PROGRESS). */
  bucket: ExecutionBucket;
  /** Original raw status from the event (for debugging). */
  rawStatus: StepStatus;
  /** ISO timestamp of the EventBridge event time. */
  lastEventTime: string;
  /** ISO timestamp when the execution state was created. */
  createdAt: string;
  /** ISO timestamp when the execution state was last updated. */
  updatedAt: string;
  /** Optimistic concurrency version number. */
  version: number;
}

/**
 * Step aggregate item representing current counts for a specific step.
 * Stored in the workflow-state table.
 */
export interface StepAggregateItem {
  /** Primary key: `AGG#COUNTY#${county}#DG#${dataGroupLabel}`. */
  PK: string;
  /** Sort key: `PHASE#${phase}#STEP#${step}`. */
  SK: string;
  /** Entity discriminator. */
  entityType: "StepAggregate";
  /** County identifier. */
  county: string;
  /** Data group label (or "not-set" if not provided). */
  dataGroupLabel: string;
  /** Current phase. */
  phase: string;
  /** Current step. */
  step: string;
  /** Count of executions currently in progress (includes SCHEDULED and PARKED). */
  inProgressCount: number;
  /** Count of executions that have failed. */
  failedCount: number;
  /** Count of executions that have succeeded. */
  succeededCount: number;
  /** ISO timestamp when the aggregate was created. */
  createdAt: string;
  /** ISO timestamp when the aggregate was last updated. */
  updatedAt: string;
  /** GSI1 partition key: `AGG#PHASE#${phase}#STEP#${step}#DG#${dataGroupLabel}`. */
  GSI1PK: string;
  /** GSI1 sort key: `COUNTY#${county}`. */
  GSI1SK: string;
  /** GSI2 partition key (sharded): `AGG#ALL#S#${shard}`. */
  GSI2PK: string;
  /** GSI2 sort key: `DG#${dataGroupLabel}#PHASE#${phase}#STEP#${step}#COUNTY#${county}`. */
  GSI2SK: string;
}
