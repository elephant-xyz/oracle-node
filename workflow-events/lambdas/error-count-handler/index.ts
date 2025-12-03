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

/**
 * Step Functions client for sending task success callbacks.
 */
const sfnClient = new SFNClient({});

/**
 * Result of pre-processing a DynamoDB stream record.
 */
interface PreProcessedRecord {
  /** The execution ID from the record. */
  executionId: string;
  /** Whether the record is valid for processing. */
  valid: boolean;
  /** Event ID for logging purposes. */
  eventId: string;
}

/**
 * Creates a structured log entry for consistent logging.
 *
 * @param action - The action being performed
 * @param data - Additional data to log
 * @returns Structured log object as JSON string
 */
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
 * Pre-processes a single DynamoDB stream record to extract execution ID.
 * Validates that it's an ExecutionErrorLink REMOVE event.
 *
 * @param record - The DynamoDB stream record to pre-process
 * @returns Pre-processed record with validation result
 */
const preProcessRecord = (record: DynamoDBRecord): PreProcessedRecord => {
  const eventId = record.eventID ?? "unknown";

  // Check if this is a REMOVE event
  if (record.eventName !== "REMOVE") {
    return { executionId: "", valid: false, eventId };
  }

  const oldImage = record.dynamodb?.OldImage;
  if (!oldImage) {
    return { executionId: "", valid: false, eventId };
  }

  // Unmarshall the old image to check entity type and get execution ID
  const item = unmarshall(
    oldImage as Record<string, AttributeValue>,
  ) as Partial<ExecutionErrorLink>;

  // Validate entity type
  if (item.entityType !== ENTITY_TYPES.EXECUTION_ERROR) {
    return { executionId: "", valid: false, eventId };
  }

  // Extract execution ID
  const executionId = item.executionId;
  if (!executionId) {
    return { executionId: "", valid: false, eventId };
  }

  return { executionId, valid: true, eventId };
};

/**
 * Groups pre-processed records by execution ID and counts removals per execution.
 *
 * @param records - Array of pre-processed records
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
 * Sends a task success callback to Step Functions.
 *
 * @param taskToken - The task token for the callback
 * @param executionId - The execution ID (for logging)
 */
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

/**
 * Sends task success callbacks in parallel for all executions that reached zero errors.
 *
 * @param results - Array of decrement results for executions that reached zero
 */
const sendTaskSuccessCallbacks = async (
  results: BatchDecrementResultItem[],
): Promise<void> => {
  const callbackPromises = results
    .filter((result) => result.taskToken)
    .map(async (result) => {
      try {
        await sendTaskSuccess(result.taskToken as string, result.executionId);
      } catch (error) {
        // Log but don't throw - the task token might be expired or invalid
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
 * Main handler for DynamoDB Stream events.
 * Processes REMOVE events for ExecutionErrorLink items using batch operations.
 *
 * Processing flow:
 * 1. Pre-process all records in parallel to extract execution IDs
 * 2. Group by execution ID and count removals
 * 3. Batch decrement openErrorCount for all executions
 * 4. Update GSI keys for executions with remaining errors
 * 5. Send task success callbacks and delete executions that reached zero
 *
 * @param event - DynamoDB Stream event containing batch of records
 */
export const handler = async (event: DynamoDBStreamEvent): Promise<void> => {
  console.info(
    createLogEntry("received_stream_event", {
      recordCount: event.Records.length,
    }),
  );

  // Step 1: Pre-process all records in parallel
  const preProcessedRecords = event.Records.map(preProcessRecord);

  // Log invalid records
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

  // Step 2: Group by execution ID
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

  // Step 3: Batch decrement openErrorCount for all executions
  const decrementInputs: BatchDecrementInput[] = Array.from(
    executionRemovals.entries(),
  ).map(([executionId, decrementBy]) => ({
    executionId,
    decrementBy,
  }));

  const decrementResults = await batchDecrementOpenErrorCounts(decrementInputs);

  // Log decrement results
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

  // Step 4: Separate results into zero and non-zero error counts
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

  // Step 5: Update GSI keys for executions with remaining errors
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

  // Step 6: Handle executions that reached zero errors
  if (executionsReachedZero.length > 0) {
    console.info(
      createLogEntry("all_errors_resolved", {
        executionIds: executionsReachedZero.map((r) => r.executionId),
      }),
    );

    // Send task success callbacks in parallel
    await sendTaskSuccessCallbacks(executionsReachedZero);

    // Batch delete FailedExecutionItems
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
