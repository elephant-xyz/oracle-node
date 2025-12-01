/**
 * @fileoverview Type definitions for WorkflowEvent detail structure from EventBridge.
 */

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
  status: string;
  /** Current phase of the workflow execution. */
  phase: string;
  /** Current step within the phase. */
  step: string;
  /** Task token for Step Functions callback (if applicable). */
  taskToken: string;
  /** Array of errors encountered during execution. */
  errors: WorkflowError[];
}
