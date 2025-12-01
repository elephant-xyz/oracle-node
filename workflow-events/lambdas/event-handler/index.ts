/**
 * @fileoverview Lambda handler for processing EventBridge WorkflowEvent events.
 */

import type { EventBridgeEvent } from "aws-lambda";
import type { WorkflowEventDetail } from "./types.js";

/**
 * Lambda handler for EventBridge WorkflowEvent events.
 *
 * @param {EventBridgeEvent<'WorkflowEvent', WorkflowEventDetail>} event - EventBridge event containing workflow execution details.
 * @returns {Promise<void>} - Promise that resolves when processing is complete.
 */
export const handler = async (
  event: EventBridgeEvent<"WorkflowEvent", WorkflowEventDetail>,
): Promise<void> => {
  const logBase = {
    component: "workflow-events-handler",
    at: new Date().toISOString(),
    eventId: event.id,
    source: event.source,
    detailType: event["detail-type"],
  };

  try {
    console.log(
      JSON.stringify({
        ...logBase,
        level: "info",
        msg: "received_workflow_event",
        executionId: event.detail.executionId,
        county: event.detail.county,
        status: event.detail.status,
        phase: event.detail.phase,
        step: event.detail.step,
        errorCount: event.detail.errors?.length ?? 0,
      }),
    );

    // Log full event detail for now (placeholder for future implementation)
    console.log(
      JSON.stringify({
        ...logBase,
        level: "info",
        msg: "event_detail",
        detail: event.detail,
      }),
    );

    console.log(
      JSON.stringify({
        ...logBase,
        level: "info",
        msg: "processing_complete",
        executionId: event.detail.executionId,
      }),
    );
  } catch (error) {
    console.error(
      JSON.stringify({
        ...logBase,
        level: "error",
        msg: "handler_failed",
        error: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined,
      }),
    );
    throw error;
  }
};

