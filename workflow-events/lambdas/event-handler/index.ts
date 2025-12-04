import type { EventBridgeEvent } from "aws-lambda";
import type {
  WorkflowEventDetail,
  ElephantErrorResolvedDetail,
} from "shared/types.js";

import { publishPhaseMetric } from "./cloudwatch.js";
import { createLogEntry } from "./log.js";
import {
  saveErrorRecords,
  deleteErrorFromAllExecutions,
  deleteErrorsForExecution,
} from "shared/repository.js";

/**
 * Union type for all supported event detail types.
 */
type SupportedEventDetail = WorkflowEventDetail | ElephantErrorResolvedDetail;

/**
 * Supported detail types for EventBridge events.
 */
type SupportedDetailType = "WorkflowEvent" | "ElephantErrorResolved";

/**
 * Handles WorkflowEvent events - persists errors to DynamoDB.
 *
 * @param event - EventBridge event containing WorkflowEventDetail
 */
const handleWorkflowEvent = async (
  event: EventBridgeEvent<"WorkflowEvent", WorkflowEventDetail>,
): Promise<void> => {
  console.info(
    createLogEntry("received_workflow_event", event, {
      executionId: event.detail.executionId,
      county: event.detail.county,
      status: event.detail.status,
      phase: event.detail.phase,
      step: event.detail.step,
      errorCount: event.detail.errors?.length ?? 0,
    }),
  );

  await publishPhaseMetric(event.detail.phase, {
    county: event.detail.county,
    status: event.detail.status,
    step: event.detail.step,
    dataGroupLabel: event.detail.dataGroupLabel,
  });
  console.info(
    createLogEntry("published_cloudwatch_metric", event, {
      executionId: event.detail.executionId,
      county: event.detail.county,
      status: event.detail.status,
      phase: event.detail.phase,
      step: event.detail.step,
    }),
  );

  const errors = event.detail.errors;
  const hasErrors = errors && errors.length > 0;

  if (hasErrors) {
    console.info(
      createLogEntry("processing_errors", event, {
        executionId: event.detail.executionId,
        errorCount: errors.length,
        errorCodes: errors.map((e) => e.code),
      }),
    );

    const result = await saveErrorRecords(event.detail);

    console.info(
      createLogEntry("errors_saved_to_dynamodb", event, {
        executionId: event.detail.executionId,
        uniqueErrorCount: result.uniqueErrorCount,
        totalOccurrences: result.totalOccurrences,
        errorCodes: result.errorCodes,
      }),
    );
  } else {
    console.info(
      createLogEntry("no_errors_to_process", event, {
        executionId: event.detail.executionId,
        status: event.detail.status,
      }),
    );
  }

  console.debug(
    createLogEntry("processing_complete", event, {
      executionId: event.detail.executionId,
      hadErrors: hasErrors,
    }),
  );
};

/**
 * Handles ElephantErrorResolved events - deletes error links from DynamoDB.
 * If executionId is provided, gets all error codes from that execution and
 * deletes them from ALL executions.
 * If only errorCode is provided, deletes that error from ALL executions.
 *
 * @param event - EventBridge event containing ElephantErrorResolvedDetail
 */
const handleElephantErrorResolved = async (
  event: EventBridgeEvent<"ElephantErrorResolved", ElephantErrorResolvedDetail>,
): Promise<void> => {
  const { executionId, errorCode } = event.detail;

  console.info(
    createLogEntry("received_error_resolved_event", event, {
      executionId: executionId ?? "none",
      errorCode: errorCode ?? "none",
    }),
  );

  if (!executionId && !errorCode) {
    const errorMessage =
      "ElephantErrorResolved event must contain either executionId or errorCode";
    console.error(
      createLogEntry("invalid_error_resolved_event", event, {
        error: errorMessage,
      }),
    );
    throw new Error(errorMessage);
  }

  if (executionId) {
    console.info(
      createLogEntry("resolving_errors_for_execution", event, {
        executionId,
      }),
    );

    const result = await deleteErrorsForExecution(executionId);

    console.info(
      createLogEntry("errors_resolved_for_execution", event, {
        executionId,
        deletedCount: result.deletedCount,
        affectedExecutionIds: result.affectedExecutionIds,
        deletedErrorCodes: result.deletedErrorCodes,
      }),
    );
  } else if (errorCode) {
    console.info(
      createLogEntry("resolving_error_code", event, {
        errorCode,
      }),
    );

    const result = await deleteErrorFromAllExecutions(errorCode);

    console.info(
      createLogEntry("error_code_resolved", event, {
        errorCode,
        deletedCount: result.deletedCount,
        affectedExecutionIds: result.affectedExecutionIds,
        deletedErrorCodes: result.deletedErrorCodes,
      }),
    );
  }

  console.debug(
    createLogEntry("error_resolution_complete", event, {
      executionId: executionId ?? "none",
      errorCode: errorCode ?? "none",
    }),
  );
};

/**
 * Main handler for EventBridge events from elephant.workflow source.
 * Routes events based on detail-type to appropriate handlers:
 * - WorkflowEvent: Persists errors to DynamoDB
 * - ElephantErrorResolved: Deletes error links from DynamoDB
 *
 * @param event - EventBridge event with either WorkflowEventDetail or ElephantErrorResolvedDetail
 */
export const handler = async (
  event: EventBridgeEvent<SupportedDetailType, SupportedEventDetail>,
): Promise<void> => {
  try {
    const detailType = event["detail-type"];

    switch (detailType) {
      case "WorkflowEvent":
        await handleWorkflowEvent(
          event as EventBridgeEvent<"WorkflowEvent", WorkflowEventDetail>,
        );
        break;

      case "ElephantErrorResolved":
        await handleElephantErrorResolved(
          event as EventBridgeEvent<
            "ElephantErrorResolved",
            ElephantErrorResolvedDetail
          >,
        );
        break;

      default: {
        const unknownType = detailType as string;
        console.warn(
          createLogEntry("unknown_detail_type", event, {
            detailType: unknownType,
          }),
        );
      }
    }
  } catch (error) {
    console.error(
      createLogEntry("handler_failed", event, {
        error: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined,
        detailType: event["detail-type"],
      }),
    );
    throw error;
  }
};
