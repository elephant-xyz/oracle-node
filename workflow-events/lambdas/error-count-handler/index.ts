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
  batchDecrementErrorRecordCounts,
  batchUpdateErrorRecordGsiKeys,
  batchDeleteErrorRecords,
} from "shared/repository.js";
import type {
  BatchDecrementInput,
  BatchDecrementResultItem,
  BatchDecrementErrorRecordInput,
  BatchDecrementErrorRecordResultItem,
} from "shared/repository.js";

const sfnClient = new SFNClient({});

/**
 * Preprocessed record containing data extracted from DynamoDB stream event.
 * Used for both execution and error record processing.
 */
interface PreProcessedRecord {
  /** The execution ID from the deleted link. */
  executionId: string;
  /** The error code from the deleted link. */
  errorCode: string;
  /** The number of occurrences from the deleted link. */
  occurrences: number;
  /** Whether the record is valid for processing. */
  valid: boolean;
  /** The event ID from the stream record. */
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

/**
 * Creates an invalid preprocessed record with default values.
 *
 * @param eventId - The event ID from the stream record
 * @returns Invalid PreProcessedRecord
 */
const createInvalidRecord = (eventId: string): PreProcessedRecord => ({
  executionId: "",
  errorCode: "",
  occurrences: 0,
  valid: false,
  eventId,
});

/**
 * Preprocesses a DynamoDB stream record to extract execution and error data.
 * Only processes REMOVE events for ExecutionErrorLink entities.
 *
 * @param record - The DynamoDB stream record
 * @returns PreProcessedRecord with extracted data or invalid marker
 */
const preProcessRecord = (record: DynamoDBRecord): PreProcessedRecord => {
  const eventId = record.eventID ?? "unknown";

  if (record.eventName !== "REMOVE") {
    return createInvalidRecord(eventId);
  }

  const oldImage = record.dynamodb?.OldImage;
  if (!oldImage) {
    return createInvalidRecord(eventId);
  }

  const item = unmarshall(
    oldImage as Record<string, AttributeValue>,
  ) as Partial<ExecutionErrorLink>;

  if (item.entityType !== ENTITY_TYPES.EXECUTION_ERROR) {
    return createInvalidRecord(eventId);
  }

  const executionId = item.executionId;
  const errorCode = item.errorCode;
  const occurrences = item.occurrences ?? 1;

  if (!executionId || !errorCode) {
    return createInvalidRecord(eventId);
  }

  return { executionId, errorCode, occurrences, valid: true, eventId };
};

/**
 * Groups preprocessed records by execution ID and counts removals.
 * Each link deletion counts as 1 removal regardless of occurrences.
 *
 * @param records - Array of preprocessed records
 * @returns Map of execution ID to removal count
 */
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

/**
 * Groups preprocessed records by error code and sums occurrences.
 * The occurrences value represents how many times the error occurred,
 * which is what we need to decrement from ErrorRecord.totalCount.
 *
 * @param records - Array of preprocessed records
 * @returns Map of error code to total occurrences to decrement
 */
const groupByErrorCode = (
  records: PreProcessedRecord[],
): Map<string, number> => {
  const errorOccurrences = new Map<string, number>();

  for (const record of records) {
    if (!record.valid) {
      continue;
    }

    const currentOccurrences = errorOccurrences.get(record.errorCode) ?? 0;
    errorOccurrences.set(
      record.errorCode,
      currentOccurrences + record.occurrences,
    );
  }

  return errorOccurrences;
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

/**
 * Processes FailedExecutionItem updates based on deleted ExecutionErrorLink records.
 * Decrements openErrorCount, updates GSI keys, sends task success callbacks, and
 * deletes execution items when all errors are resolved.
 *
 * @param preProcessedRecords - Array of preprocessed stream records
 * @returns Object with processing statistics
 */
const processExecutionUpdates = async (
  preProcessedRecords: PreProcessedRecord[],
): Promise<{
  processed: number;
  reachedZero: number;
  updatedGsiKeys: number;
}> => {
  const executionRemovals = groupByExecution(preProcessedRecords);

  if (executionRemovals.size === 0) {
    return { processed: 0, reachedZero: 0, updatedGsiKeys: 0 };
  }

  const totalRemovals = Array.from(executionRemovals.values()).reduce(
    (sum, count) => sum + count,
    0,
  );

  console.info(
    createLogEntry("processing_execution_removals", {
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
    createLogEntry("execution_decrement_complete", {
      successful: successfulDecrements.length,
      failed: failedDecrements.length,
    }),
  );

  if (failedDecrements.length > 0) {
    console.warn(
      createLogEntry("execution_decrement_failures", {
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
    createLogEntry("execution_categorized_results", {
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
      createLogEntry("execution_gsi_keys_updated", {
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
      createLogEntry("execution_batch_delete_complete", {
        requested: executionIdsToDelete.length,
        deleted: deletedIds.length,
      }),
    );
  }

  return {
    processed: executionRemovals.size,
    reachedZero: executionsReachedZero.length,
    updatedGsiKeys: executionsWithRemainingErrors.length,
  };
};

/**
 * Processes ErrorRecord updates based on deleted ExecutionErrorLink records.
 * Decrements totalCount by the sum of occurrences, updates GSI keys, and
 * deletes error records when totalCount reaches zero.
 *
 * @param preProcessedRecords - Array of preprocessed stream records
 * @returns Object with processing statistics
 */
const processErrorRecordUpdates = async (
  preProcessedRecords: PreProcessedRecord[],
): Promise<{
  processed: number;
  reachedZero: number;
  updatedGsiKeys: number;
}> => {
  const errorOccurrences = groupByErrorCode(preProcessedRecords);

  if (errorOccurrences.size === 0) {
    return { processed: 0, reachedZero: 0, updatedGsiKeys: 0 };
  }

  const totalOccurrences = Array.from(errorOccurrences.values()).reduce(
    (sum, count) => sum + count,
    0,
  );

  console.info(
    createLogEntry("processing_error_record_updates", {
      uniqueErrorCodes: errorOccurrences.size,
      totalOccurrencesToDecrement: totalOccurrences,
    }),
  );

  const decrementInputs: BatchDecrementErrorRecordInput[] = Array.from(
    errorOccurrences.entries(),
  ).map(([errorCode, decrementBy]) => ({
    errorCode,
    decrementBy,
  }));

  const decrementResults =
    await batchDecrementErrorRecordCounts(decrementInputs);

  const successfulDecrements = decrementResults.filter((r) => r.success);
  const failedDecrements = decrementResults.filter((r) => !r.success);

  console.info(
    createLogEntry("error_record_decrement_complete", {
      successful: successfulDecrements.length,
      failed: failedDecrements.length,
    }),
  );

  if (failedDecrements.length > 0) {
    console.warn(
      createLogEntry("error_record_decrement_failures", {
        failures: failedDecrements.map((r) => ({
          errorCode: r.errorCode,
          error: r.error,
        })),
      }),
    );
  }

  const errorsReachedZero = successfulDecrements.filter(
    (r) => r.newTotalCount === 0,
  );
  const errorsWithRemainingCount = successfulDecrements.filter(
    (r) => r.newTotalCount > 0 && r.errorType,
  );

  console.info(
    createLogEntry("error_record_categorized_results", {
      reachedZero: errorsReachedZero.length,
      withRemainingCount: errorsWithRemainingCount.length,
    }),
  );

  if (errorsWithRemainingCount.length > 0) {
    const gsiUpdates = errorsWithRemainingCount.map((r) => ({
      errorCode: r.errorCode,
      newTotalCount: r.newTotalCount,
      errorType: r.errorType as string,
    }));

    await batchUpdateErrorRecordGsiKeys(gsiUpdates);

    console.info(
      createLogEntry("error_record_gsi_keys_updated", {
        count: gsiUpdates.length,
      }),
    );
  }

  if (errorsReachedZero.length > 0) {
    const errorCodesToDelete = errorsReachedZero.map((r) => r.errorCode);

    console.info(
      createLogEntry("error_records_to_delete", {
        errorCodes: errorCodesToDelete,
      }),
    );

    const deletedCodes = await batchDeleteErrorRecords(errorCodesToDelete);

    console.info(
      createLogEntry("error_record_batch_delete_complete", {
        requested: errorCodesToDelete.length,
        deleted: deletedCodes.length,
      }),
    );
  }

  return {
    processed: errorOccurrences.size,
    reachedZero: errorsReachedZero.length,
    updatedGsiKeys: errorsWithRemainingCount.length,
  };
};

/**
 * Main handler for DynamoDB Stream events.
 * Processes deleted ExecutionErrorLink records to update both
 * FailedExecutionItem and ErrorRecord counts.
 *
 * @param event - DynamoDB Stream event containing REMOVE records
 */
export const handler = async (event: DynamoDBStreamEvent): Promise<void> => {
  console.info(
    createLogEntry("received_stream_event", {
      recordCount: event.Records.length,
    }),
  );

  const preProcessedRecords = event.Records.map(preProcessRecord);

  const validRecords = preProcessedRecords.filter((r) => r.valid);
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

  if (validRecords.length === 0) {
    console.info(
      createLogEntry("no_valid_removals", {
        totalRecords: event.Records.length,
      }),
    );
    return;
  }

  // Process execution updates (openErrorCount decrements)
  const executionStats = await processExecutionUpdates(preProcessedRecords);

  // Process error record updates (totalCount decrements)
  const errorRecordStats = await processErrorRecordUpdates(preProcessedRecords);

  console.info(
    createLogEntry("stream_processing_complete", {
      executions: {
        processed: executionStats.processed,
        reachedZero: executionStats.reachedZero,
        updatedGsiKeys: executionStats.updatedGsiKeys,
      },
      errorRecords: {
        processed: errorRecordStats.processed,
        reachedZero: errorRecordStats.reachedZero,
        updatedGsiKeys: errorRecordStats.updatedGsiKeys,
      },
    }),
  );
};
