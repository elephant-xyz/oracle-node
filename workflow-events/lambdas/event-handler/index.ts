import type { EventBridgeEvent } from "aws-lambda";
import type { WorkflowEventDetail } from "shared/types.js";

import { publishPhaseMetric } from "./cloudwatch.js";
import { saveErrorRecords } from "./dynamodb.js";
import { createLogEntry } from "./log.js";

/**
 * Handles EventBridge WorkflowEvent events from elephant.workflow source.
 * When errors are present in the event detail, persists them to DynamoDB.
 *
 * @param event - EventBridge event containing WorkflowEventDetail
 */
export const handler = async (
  event: EventBridgeEvent<"WorkflowEvent", WorkflowEventDetail>,
): Promise<void> => {
  try {
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

    // Publish CloudWatch metric for the workflow phase
    await publishPhaseMetric(event.detail.phase, {
      county: event.detail.county,
      status: event.detail.status,
      step: event.detail.step,
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
      // No errors in this event - other cases will be implemented later
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
  } catch (error) {
    console.error(
      createLogEntry("handler_failed", event, {
        error: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined,
        executionId: event.detail.executionId,
      }),
    );
    throw error;
  }
};
