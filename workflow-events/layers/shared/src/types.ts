type StepStatus =
  | "SUCCEEDED"
  | "FAILED"
  | "PARKED"
  | "IN_PROGRESS"
  | "SCHEDULED";

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
  /** Task token for Step Functions callback (if applicable). */
  taskToken?: string;
  /** S3 URI of the prepared output from the prepare step. */
  preparedS3Uri?: string;
  /** Array of errors encountered during execution. */
  errors: WorkflowError[];
}

/**
 * Error status for execution-specific errors.
 */
export type ErrorStatus = "failed" | "maybeSolved" | "solved";

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
  /** Global secondary index SK (`COUNT#000010#ERROR#errorCode`) for reverse lookup. */
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
  /** S3 URI of the prepared output from the prepare step. */
  preparedS3Uri?: string;
  /** ISO timestamp when the execution record was created. */
  createdAt: string;
  /** ISO timestamp when the execution record was updated. */
  updatedAt: string;
  /** Global secondary index partition key (`METRIC#ERRORCOUNT`). */
  GS1PK: string;
  /** Global secondary index sort key for the generalized count sort key (`COUNT#000010#EXECUTION#uuid`). */
  GS1SK: string;
  /** Global secondary index partition key (`METRIC#ERRORCOUNT`). */
  GS3PK: string;
  /** Global secondary index sort key (for example `COUNT#{errorType}#000010#EXECUTION#uuid`). */
  GS3SK: string;
}
