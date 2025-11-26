import type { EventBridgeEvent } from "aws-lambda";
import type { WorkflowEventDetail } from "./types.js";

import { createLogEntry } from "./log.js";

export const handler = async (
  event: EventBridgeEvent<"WorkflowEvent", WorkflowEventDetail>,
): Promise<void> => {
  try {
    console.debug(
      createLogEntry("received_workflow_event", event, {
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
      createLogEntry("event_detail", event, {
        detail: event.detail,
      }),
    );

    console.debug(
      createLogEntry("processing_complete", event, {
        executionId: event.detail.executionId,
      }),
    );
  } catch (error) {
    console.error(
      createLogEntry("handler_failed", event, {
        error: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined,
      }),
    );
    throw error;
  }
};
