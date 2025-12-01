import type { EventBridgeEvent } from "aws-lambda";
import type { WorkflowEventDetail } from "./types.js";

export const createLogEntry = (
  msg: string,
  event: EventBridgeEvent<"WorkflowEvent", WorkflowEventDetail>,
  additionalFields?: Record<string, unknown>,
): Record<string, unknown> => {
  return {
    component: "workflow-events-handler",
    eventId: event.id,
    source: event.source,
    detailType: event["detail-type"],
    msg,
    ...additionalFields,
  };
};
