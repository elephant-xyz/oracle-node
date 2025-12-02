import type { EventBridgeEvent } from "aws-lambda";

/**
 * Creates a structured log entry for consistent logging.
 * Accepts a generic EventBridge event type for flexibility.
 *
 * @param msg - The log message
 * @param event - The EventBridge event (any detail type)
 * @param additionalFields - Additional fields to include in the log entry
 * @returns Structured log object
 */
export const createLogEntry = <TDetailType extends string, TDetail>(
  msg: string,
  event: EventBridgeEvent<TDetailType, TDetail>,
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
