import type { DynamoDBStreamEvent, DynamoDBRecord } from "aws-lambda";
import { SFNClient, SendTaskSuccessCommand } from "@aws-sdk/client-sfn";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import type { AttributeValue } from "@aws-sdk/client-dynamodb";
import type { ExecutionErrorLink } from "shared/types.js";
import { ENTITY_TYPES } from "shared/keys.js";
import {
  batchDecrementOpenErrorCounts,
  batchUpdateExecutionGsiKeys,
  batchDeleteFailedExecutionItems,
} from "shared/repository.js";
import type {
  BatchDecrementInput,
  BatchDecrementResultItem,
} from "shared/repository.js";

const sfnClient = new SFNClient({});

interface PreProcessedRecord {
  executionId: string;
  valid: boolean;
  eventId: string;
}

const createLogEntry = (
  action: string,
  data: Record<string, unknown>,
): string => {
  return JSON.stringify({
    action,
    timestamp: new Date().toISOString(),
    ...data,
  });
};

const preProcessRecord = (record: DynamoDBRecord): PreProcessedRecord => {
  const eventId = record.eventID ?? "unknown";

  if (record.eventName !== "REMOVE") {
    return { executionId: "", valid: false, eventId };
  }

  const oldImage = record.dynamodb?.OldImage;
  if (!oldImage) {
    return { executionId: "", valid: false, eventId };
  }

  const item = unmarshall(
    oldImage as Record<string, AttributeValue>,
  ) as Partial<ExecutionErrorLink>;

  if (item.entityType !== ENTITY_TYPES.EXECUTION_ERROR) {
    return { executionId: "", valid: false, eventId };
  }

  const executionId = item.executionId;
  if (!executionId) {
    return { executionId: "", valid: false, eventId };
  }

  return { executionId, valid: true, eventId };
};

const groupByExecution = (
  records: PreProcessedRecord[],
): Map<string, number> => {
  const executionRemovals = new Map<string, number>();

  for (const record of records) {
    if (!record.valid) {
      continue;
    }

    const currentCount = executionRemovals.get(record.executionId) ?? 0;
    executionRemovals.set(record.executionId, currentCount + 1);
  }

  return executionRemovals;
};

const sendTaskSuccess = async (
  taskToken: string,
  executionId: string,
): Promise<void> => {
  console.info(
    createLogEntry("sending_task_success", {
      executionId,
      taskTokenPrefix: taskToken.substring(0, 20) + "...",
    }),
  );

  const command = new SendTaskSuccessCommand({
    taskToken,
    output: JSON.stringify({}),
  });

  await sfnClient.send(command);

  console.info(
    createLogEntry("task_success_sent", {
      executionId,
    }),
  );
};

const sendTaskSuccessCallbacks = async (
  results: BatchDecrementResultItem[],
): Promise<void> => {
  const callbackPromises = results
    .filter((result) => result.taskToken)
    .map(async (result) => {
      try {
        await sendTaskSuccess(result.taskToken as string, result.executionId);
      } catch (error) {
        console.error(
          createLogEntry("task_success_failed", {
            executionId: result.executionId,
            error: error instanceof Error ? error.message : String(error),
          }),
        );
      }
    });

  await Promise.all(callbackPromises);
};

export const handler = async (event: DynamoDBStreamEvent): Promise<void> => {
  console.info(
    createLogEntry("received_stream_event", {
      recordCount: event.Records.length,
    }),
  );

  const preProcessedRecords = event.Records.map(preProcessRecord);

  const invalidRecords = preProcessedRecords.filter(
    (r) => !r.valid && r.eventId !== "unknown",
  );
  if (invalidRecords.length > 0) {
    console.info(
      createLogEntry("skipped_invalid_records", {
        count: invalidRecords.length,
      }),
    );
  }

  const executionRemovals = groupByExecution(preProcessedRecords);

  if (executionRemovals.size === 0) {
    console.info(
      createLogEntry("no_valid_removals", {
        totalRecords: event.Records.length,
      }),
    );
    return;
  }

  const totalRemovals = Array.from(executionRemovals.values()).reduce(
    (sum, count) => sum + count,
    0,
  );

  console.info(
    createLogEntry("processing_removals", {
      uniqueExecutions: executionRemovals.size,
      totalRemovals,
    }),
  );

  const decrementInputs: BatchDecrementInput[] = Array.from(
    executionRemovals.entries(),
  ).map(([executionId, decrementBy]) => ({
    executionId,
    decrementBy,
  }));

  const decrementResults = await batchDecrementOpenErrorCounts(decrementInputs);

  const successfulDecrements = decrementResults.filter((r) => r.success);
  const failedDecrements = decrementResults.filter((r) => !r.success);

  console.info(
    createLogEntry("batch_decrement_complete", {
      successful: successfulDecrements.length,
      failed: failedDecrements.length,
    }),
  );

  if (failedDecrements.length > 0) {
    console.warn(
      createLogEntry("decrement_failures", {
        failures: failedDecrements.map((r) => ({
          executionId: r.executionId,
          error: r.error,
        })),
      }),
    );
  }

  const executionsReachedZero = successfulDecrements.filter(
    (r) => r.newOpenErrorCount === 0,
  );
  const executionsWithRemainingErrors = successfulDecrements.filter(
    (r) => r.newOpenErrorCount > 0 && r.errorType,
  );

  console.info(
    createLogEntry("categorized_results", {
      reachedZero: executionsReachedZero.length,
      withRemainingErrors: executionsWithRemainingErrors.length,
    }),
  );

  if (executionsWithRemainingErrors.length > 0) {
    const gsiUpdates = executionsWithRemainingErrors.map((r) => ({
      executionId: r.executionId,
      newOpenErrorCount: r.newOpenErrorCount,
      errorType: r.errorType as string,
    }));

    await batchUpdateExecutionGsiKeys(gsiUpdates);

    console.info(
      createLogEntry("gsi_keys_updated", {
        count: gsiUpdates.length,
      }),
    );
  }

  if (executionsReachedZero.length > 0) {
    console.info(
      createLogEntry("all_errors_resolved", {
        executionIds: executionsReachedZero.map((r) => r.executionId),
      }),
    );

    await sendTaskSuccessCallbacks(executionsReachedZero);

    const executionIdsToDelete = executionsReachedZero.map(
      (r) => r.executionId,
    );
    const deletedIds =
      await batchDeleteFailedExecutionItems(executionIdsToDelete);

    console.info(
      createLogEntry("batch_delete_complete", {
        requested: executionIdsToDelete.length,
        deleted: deletedIds.length,
      }),
    );
  }

  console.info(
    createLogEntry("stream_processing_complete", {
      processedExecutions: executionRemovals.size,
      reachedZero: executionsReachedZero.length,
      updatedGsiKeys: executionsWithRemainingErrors.length,
    }),
  );
};
