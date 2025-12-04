import type { EventBridgeEvent } from "aws-lambda";

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
