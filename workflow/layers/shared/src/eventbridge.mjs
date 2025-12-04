import {
  EventBridgeClient,
  PutEventsCommand,
} from "@aws-sdk/client-eventbridge";

const eventBridgeClient = new EventBridgeClient({});

/**
 * @typedef {'SCHEDULED' | 'IN_PROGRESS' | 'SUCCEEDED' | 'PARKED' | 'FAILED'} WorkflowStatus
 */

/**
 * @typedef {'Prepare' | 'Transform' | 'SVL' | 'MVL' | 'Hash' | 'Upload' | 'Submit' | 'GasPriceCheck' | 'TransactionStatusCheck'} WorkflowPhase
 */

/**
 * @typedef {'EvaluateTransform' | 'Transform' | 'SVL' | 'MVL' | 'EvaluateHash' | 'Hash' | 'EvaluateUpload' | 'Upload' | 'CheckGasPrice' | 'SubmitToBlockchain' | 'CheckTransactionStatus'} WorkflowStep
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
 * @param {ReturnType<typeof import('./index.mjs').createLogger>} [params.log] - Logger.
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
  log,
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

    if (log) {
      log("info", "eventbridge_event_emitted", {
        status,
        phase,
        step,
        executionId,
        county,
      });
    }
  } catch (err) {
    // Log but don't fail the workflow for EventBridge errors
    if (log) {
      log("error", "eventbridge_event_failed", {
        error: err instanceof Error ? err.message : String(err),
        status,
        phase,
        step,
      });
    }
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
