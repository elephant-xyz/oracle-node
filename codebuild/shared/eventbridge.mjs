import {
  EventBridgeClient,
  PutEventsCommand,
} from "@aws-sdk/client-eventbridge";

const eventBridgeClient = new EventBridgeClient({});

/**
 * @typedef {'SCHEDULED' | 'IN_PROGRESS' | 'SUCCEEDED' | 'PARKED' | 'FAILED'} WorkflowStatus
 */

/**
 * @typedef {'Prepare' | 'Transform' | 'SVL' | 'MVL' | 'Hash' | 'Upload' | 'Submit' | 'GasPriceCheck' | 'TransactionStatusCheck' | 'AutoRepair'} WorkflowPhase
 */

/**
 * @typedef {'EvaluateTransform' | 'Transform' | 'SVL' | 'MVL' | 'EvaluateHash' | 'Hash' | 'EvaluateUpload' | 'Upload' | 'CheckGasPrice' | 'SubmitToBlockchain' | 'CheckTransactionStatus' | 'AutoRepair'} WorkflowStep
 */

/**
 * @typedef {object} WorkflowError
 * @property {string} code - Error code identifier.
 * @property {Record<string, unknown>} [details] - Free-form object with error-specific details.
 */

/**
 * @typedef {object} WorkflowEventDetail
 * @property {string} executionId - Step Functions execution ARN or ID.
 * @property {string} county - County identifier being processed.
 * @property {string} [dataGroupLabel] - Elephant data group (e.g., `Seed`, `County`).
 * @property {WorkflowStatus} status - Current workflow status.
 * @property {WorkflowPhase} phase - High-level workflow phase.
 * @property {WorkflowStep} step - Granular step within the phase.
 * @property {string} [taskToken] - Step Functions task token for resumption.
 * @property {WorkflowError[]} [errors] - List of error objects.
 */

/**
 * Emit a workflow event to EventBridge.
 *
 * @param {object} params - Event parameters.
 * @param {string} params.executionId - Execution identifier.
 * @param {string} params.county - County name.
 * @param {string} [params.dataGroupLabel] - Elephant data group (e.g., `Seed`, `County`).
 * @param {WorkflowStatus} params.status - Workflow status.
 * @param {WorkflowPhase} params.phase - Workflow phase.
 * @param {WorkflowStep} params.step - Workflow step.
 * @param {string} [params.taskToken] - Task token for resumption.
 * @param {WorkflowError[]} [params.errors] - Error objects.
 * @returns {Promise<void>}
 */
export async function emitWorkflowEvent({
  executionId,
  county,
  dataGroupLabel,
  status,
  phase,
  step,
  taskToken,
  errors,
}) {
  /** @type {WorkflowEventDetail} */
  const detail = {
    executionId,
    county,
    status,
    phase,
    step,
  };

  if (dataGroupLabel) {
    detail.dataGroupLabel = dataGroupLabel;
  }

  if (taskToken) {
    detail.taskToken = taskToken;
  }

  if (errors && errors.length > 0) {
    detail.errors = errors;
  }

  try {
    await eventBridgeClient.send(
      new PutEventsCommand({
        Entries: [
          {
            Source: "elephant.workflow",
            DetailType: "WorkflowEvent",
            Detail: JSON.stringify(detail),
          },
        ],
      }),
    );

    console.log(
      `Emitted WorkflowEvent: ${status} for ${phase}/${step} (execution: ${executionId})`,
    );
  } catch (err) {
    // Log but don't fail the workflow for EventBridge errors
    console.error(
      `Failed to emit WorkflowEvent: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

/**
 * Create error object for EventBridge events.
 *
 * @param {string} code - Error code.
 * @param {Record<string, unknown>} [details] - Error details.
 * @returns {WorkflowError}
 */
export function createWorkflowError(code, details) {
  return {
    code,
    ...(details && { details }),
  };
}

/**
 * Emit an ElephantErrorResolved event to mark errors as resolved.
 *
 * @param {object} params - Event parameters.
 * @param {string} [params.executionId] - Execution identifier (resolves all errors for this execution).
 * @param {string} [params.errorCode] - Error code (resolves this error across all executions).
 * @returns {Promise<void>}
 */
export async function emitErrorResolved({ executionId, errorCode }) {
  if (!executionId && !errorCode) {
    throw new Error(
      "ElephantErrorResolved event must contain either executionId or errorCode",
    );
  }

  try {
    await eventBridgeClient.send(
      new PutEventsCommand({
        Entries: [
          {
            Source: "elephant.workflow",
            DetailType: "ElephantErrorResolved",
            Detail: JSON.stringify({
              ...(executionId && { executionId }),
              ...(errorCode && { errorCode }),
            }),
          },
        ],
      }),
    );

    console.log(
      `Emitted ElephantErrorResolved event${executionId ? ` for execution: ${executionId}` : ""}${errorCode ? ` for error code: ${errorCode}` : ""}`,
    );
  } catch (err) {
    // Log but don't fail the workflow for EventBridge errors
    console.error(
      `Failed to emit ElephantErrorResolved event: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

/**
 * Emit an ElephantErrorFailedToResolve event to mark errors as maybeUnrecoverable.
 *
 * @param {object} params - Event parameters.
 * @param {string} [params.executionId] - Execution identifier (marks all errors for this execution as unrecoverable).
 * @param {string} [params.errorCode] - Error code (marks this error as unrecoverable across all executions).
 * @returns {Promise<void>}
 */
export async function emitErrorFailedToResolve({ executionId, errorCode }) {
  if (!executionId && !errorCode) {
    throw new Error(
      "ElephantErrorFailedToResolve event must contain either executionId or errorCode",
    );
  }

  try {
    await eventBridgeClient.send(
      new PutEventsCommand({
        Entries: [
          {
            Source: "elephant.workflow",
            DetailType: "ElephantErrorFailedToResolve",
            Detail: JSON.stringify({
              ...(executionId && { executionId }),
              ...(errorCode && { errorCode }),
            }),
          },
        ],
      }),
    );

    console.log(
      `Emitted ElephantErrorFailedToResolve event${executionId ? ` for execution: ${executionId}` : ""}${errorCode ? ` for error code: ${errorCode}` : ""}`,
    );
  } catch (err) {
    // Log but don't fail the workflow for EventBridge errors
    console.error(
      `Failed to emit ElephantErrorFailedToResolve event: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

