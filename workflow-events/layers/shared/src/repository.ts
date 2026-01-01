import {
  TransactWriteCommand,
  UpdateCommand,
  BatchGetCommand,
  QueryCommand,
  DeleteCommand,
  BatchWriteCommand,
  GetCommand,
} from "@aws-sdk/lib-dynamodb";
import type {
  WorkflowEventDetail,
  WorkflowError,
  ErrorRecord,
  ExecutionErrorLink,
  FailedExecutionItem,
  ErrorStatus,
} from "./types.js";
import { TABLE_NAME, docClient } from "./dynamodb-client.js";
import {
  ENTITY_TYPES,
  padCount,
  extractErrorType,
  createKey,
  DEFAULT_GSI_STATUS,
  toGsiStatus,
  GS3_EXECUTION_PK,
  GS3_ERROR_PK,
} from "./keys.js";

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Configuration for retry with exponential backoff.
 */
interface RetryConfig {
  /** Maximum number of retry attempts. */
  maxRetries: number;
  /** Base delay in milliseconds. */
  baseDelayMs: number;
  /** Maximum delay in milliseconds. */
  maxDelayMs: number;
}

/**
 * Default retry configuration for DynamoDB operations.
 * With 10 retries and exponential backoff (capped at 3s), worst-case total
 * wait time is ~20s, but with jitter it averages ~10s.
 */
const DEFAULT_RETRY_CONFIG: RetryConfig = {
  maxRetries: 10,
  baseDelayMs: 25,
  maxDelayMs: 3000,
};

/**
 * Calculates delay with exponential backoff and jitter.
 * Uses full jitter strategy to prevent thundering herd.
 * @param attempt - The current attempt number (0-based)
 * @param config - Retry configuration
 * @returns Delay in milliseconds
 */
const calculateBackoffDelay = (
  attempt: number,
  config: RetryConfig = DEFAULT_RETRY_CONFIG,
): number => {
  const exponentialDelay = config.baseDelayMs * Math.pow(2, attempt);
  const cappedDelay = Math.min(exponentialDelay, config.maxDelayMs);
  // Full jitter: random value between 0 and cappedDelay
  return Math.random() * cappedDelay;
};

/**
 * Delays execution for the specified number of milliseconds.
 * @param ms - Delay in milliseconds
 */
const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Executes an async operation with retry using exponential backoff and jitter.
 * @param operation - The async operation to execute
 * @param isRetryable - Function to determine if an error is retryable
 * @param config - Retry configuration
 * @returns The result of the operation
 * @throws The last error if all retries are exhausted
 */
const executeWithRetry = async <T>(
  operation: () => Promise<T>,
  isRetryable: (error: unknown) => boolean,
  config: RetryConfig = DEFAULT_RETRY_CONFIG,
): Promise<T> => {
  let lastError: unknown;

  for (let attempt = 0; attempt <= config.maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;

      if (!isRetryable(error) || attempt === config.maxRetries) {
        throw error;
      }

      const delayMs = calculateBackoffDelay(attempt, config);
      console.warn(
        `Retryable error on attempt ${attempt + 1}/${config.maxRetries + 1}, retrying in ${Math.round(delayMs)}ms:`,
        error instanceof Error ? error.message : String(error),
      );
      await delay(delayMs);
    }
  }

  throw lastError;
};

/**
 * Determines if a DynamoDB error is retryable.
 * @param error - The error to check
 * @returns True if the error is retryable
 */
const isRetryableDynamoDBError = (error: unknown): boolean => {
  if (!(error instanceof Error)) {
    return false;
  }

  const retryableErrorNames = [
    "ProvisionedThroughputExceededException",
    "ThrottlingException",
    "RequestLimitExceeded",
    "InternalServerError",
    "ServiceUnavailable",
  ];

  return retryableErrorNames.includes(error.name);
};

/**
 * Gets the table name, throwing a descriptive error if not set.
 * @returns The DynamoDB table name
 * @throws Error if WORKFLOW_ERRORS_TABLE_NAME is not set
 */
const getTableName = (): string => {
  if (!TABLE_NAME) {
    throw new Error(
      "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
    );
  }
  return TABLE_NAME;
};

/**
 * Default error status for new records.
 */
const DEFAULT_ERROR_STATUS: ErrorStatus = "failed";

/**
 * Counts occurrences of each error code in the errors array.
 * @param errors - Array of workflow errors
 * @returns Map of error codes to their occurrence counts
 */
const countErrorOccurrences = (
  errors: WorkflowError[],
): Map<string, { count: number; details: Record<string, unknown> }> => {
  const occurrenceMap = new Map<
    string,
    { count: number; details: Record<string, unknown> }
  >();

  for (const error of errors) {
    const existing = occurrenceMap.get(error.code);
    if (existing) {
      existing.count += 1;
    } else {
      occurrenceMap.set(error.code, { count: 1, details: error.details });
    }
  }

  return occurrenceMap;
};

/**
 * Statistics about errors in an execution for FailedExecutionItem.
 */
interface ExecutionErrorStats {
  /** Total number of error occurrences. */
  totalOccurrences: number;
  /** Number of unique error codes. */
  uniqueErrorCount: number;
  /** Error type for the execution (first 2 characters of error code). */
  errorType: string;
}

/**
 * Builds UpdateCommand parameters for upserting an ErrorRecord.
 * Uses atomic counter for totalCount increment.
 * @param errorCode - The error code identifier
 * @param errorDetails - Additional error details
 * @param executionId - The execution ID that observed this error
 * @param occurrences - Number of times this error occurred in the execution
 * @param now - ISO timestamp for the operation
 * @returns UpdateCommand input for ErrorRecord upsert
 */
const buildErrorRecordUpdate = (
  errorCode: string,
  errorDetails: Record<string, unknown>,
  executionId: string,
  occurrences: number,
  now: string,
): {
  Update: {
    TableName: string;
    Key: Pick<ErrorRecord, "PK" | "SK">;
    UpdateExpression: string;
    ExpressionAttributeValues: Record<string, string | number>;
  };
} => {
  const tableName = getTableName();
  const pk = createKey("ERROR", errorCode);
  const errorType = extractErrorType(errorCode);
  // Note: GS2SK and GS3SK are NOT set here - they are updated separately
  // after the transaction via refreshErrorRecordSortKeys to reflect the
  // actual totalCount (since totalCount uses atomic increment).

  return {
    Update: {
      TableName: tableName,
      Key: {
        PK: pk,
        SK: pk,
      },
      UpdateExpression: `
        SET errorCode = :errorCode,
            entityType = :entityType,
            errorType = :errorType,
            errorDetails = :errorDetails,
            errorStatus = if_not_exists(errorStatus, :defaultStatus),
            totalCount = if_not_exists(totalCount, :zero) + :increment,
            createdAt = if_not_exists(createdAt, :now),
            updatedAt = :now,
            latestExecutionId = :executionId,
            GS1PK = :gs1pk,
            GS1SK = :gs1sk,
            GS2PK = :gs2pk,
            GS3PK = :gs3pk
      `.trim(),
      ExpressionAttributeValues: {
        ":errorCode": errorCode,
        ":entityType": ENTITY_TYPES.ERROR,
        ":errorType": errorType,
        ":errorDetails": JSON.stringify(errorDetails),
        ":defaultStatus": DEFAULT_ERROR_STATUS,
        ":zero": 0,
        ":increment": occurrences,
        ":now": now,
        ":executionId": executionId,
        ":gs1pk": "TYPE#ERROR",
        ":gs1sk": pk,
        ":gs2pk": "TYPE#ERROR",
        ":gs3pk": GS3_ERROR_PK,
      },
    },
  };
};

/**
 * Executes a single ErrorRecord update with retry using exponential backoff.
 * This is used instead of including ErrorRecord in a transaction to avoid
 * TransactionConflict errors when multiple events with the same error code
 * are processed concurrently.
 *
 * @param errorCode - The error code identifier
 * @param errorDetails - Additional error details
 * @param executionId - The execution ID that observed this error
 * @param occurrences - Number of times this error occurred in the execution
 * @param now - ISO timestamp for the operation
 */
const executeErrorRecordUpdate = async (
  errorCode: string,
  errorDetails: Record<string, unknown>,
  executionId: string,
  occurrences: number,
  now: string,
): Promise<void> => {
  const updateParams = buildErrorRecordUpdate(
    errorCode,
    errorDetails,
    executionId,
    occurrences,
    now,
  );

  const command = new UpdateCommand(updateParams.Update);

  await executeWithRetry(
    () => docClient.send(command),
    isRetryableDynamoDBError,
  );
};

/**
 * Builds UpdateCommand parameters for upserting an ExecutionErrorLink.
 * @param errorCode - The error code identifier
 * @param executionId - The execution ID
 * @param county - The county identifier
 * @param occurrences - Number of times this error occurred in the execution
 * @param errorDetails - Additional error details (key-value pairs)
 * @param now - ISO timestamp for the operation
 * @returns UpdateCommand input for ExecutionErrorLink upsert
 */
const buildExecutionErrorLinkUpdate = (
  errorCode: string,
  executionId: string,
  county: string,
  occurrences: number,
  errorDetails: Record<string, unknown>,
  now: string,
): {
  Update: {
    TableName: string;
    Key: Pick<ExecutionErrorLink, "PK" | "SK">;
    UpdateExpression: string;
    ExpressionAttributeNames: Record<string, string>;
    ExpressionAttributeValues: Record<string, string | number>;
  };
} => {
  const tableName = getTableName();
  const pk = createKey("EXECUTION", executionId);
  const sk = createKey("ERROR", errorCode);

  return {
    Update: {
      TableName: tableName,
      Key: {
        PK: pk,
        SK: sk,
      },
      UpdateExpression: `
        SET entityType = :entityType,
            errorCode = :errorCode,
            #status = if_not_exists(#status, :defaultStatus),
            occurrences = if_not_exists(occurrences, :zero) + :occurrences,
            errorDetails = :errorDetails,
            executionId = :executionId,
            county = :county,
            createdAt = if_not_exists(createdAt, :now),
            updatedAt = :now,
            GS1PK = :gs1pk,
            GS1SK = :gs1sk
      `.trim(),
      ExpressionAttributeNames: {
        "#status": "status",
      },
      ExpressionAttributeValues: {
        ":entityType": ENTITY_TYPES.EXECUTION_ERROR,
        ":errorCode": errorCode,
        ":defaultStatus": DEFAULT_ERROR_STATUS,
        ":zero": 0,
        ":occurrences": occurrences,
        ":errorDetails": JSON.stringify(errorDetails),
        ":executionId": executionId,
        ":county": county,
        ":now": now,
        ":gs1pk": sk,
        ":gs1sk": pk,
      },
    },
  };
};

/**
 * Builds UpdateCommand parameters for upserting a FailedExecutionItem.
 * @param detail - The workflow event detail
 * @param stats - Error statistics for the execution
 * @param now - ISO timestamp for the operation
 * @returns UpdateCommand input for FailedExecutionItem upsert
 */
const buildFailedExecutionItemUpdate = (
  detail: WorkflowEventDetail,
  stats: ExecutionErrorStats,
  now: string,
): {
  Update: {
    TableName: string;
    Key: Pick<FailedExecutionItem, "PK" | "SK">;
    UpdateExpression: string;
    ExpressionAttributeNames: Record<string, string>;
    ExpressionAttributeValues: Record<string, string | number>;
  };
} => {
  const tableName = getTableName();
  const pk = createKey("EXECUTION", detail.executionId);
  // GS1SK format: COUNT#{status}#{paddedCount}#EXECUTION#{executionId}
  const gs1sk = `COUNT#${DEFAULT_GSI_STATUS}#${padCount(stats.uniqueErrorCount)}#EXECUTION#${detail.executionId}`;
  // GS3SK format: COUNT#{errorType}#{status}#{paddedCount}#EXECUTION#{executionId}
  const gs3sk = `COUNT#${stats.errorType}#${DEFAULT_GSI_STATUS}#${padCount(stats.uniqueErrorCount)}#EXECUTION#${detail.executionId}`;

  // Build taskToken clause only when defined
  const taskTokenClause =
    detail.taskToken !== undefined ? "taskToken = :taskToken," : "";

  const expressionAttributeValues: Record<string, string | number> = {
    ":executionId": detail.executionId,
    ":entityType": ENTITY_TYPES.FAILED_EXECUTION,
    ":errorType": stats.errorType,
    ":defaultStatus": DEFAULT_ERROR_STATUS,
    ":county": detail.county,
    ":zero": 0,
    ":totalOccurrences": stats.totalOccurrences,
    ":uniqueErrorCount": stats.uniqueErrorCount,
    ":now": now,
    ":gs1pk": GS3_EXECUTION_PK,
    ":gs1sk": gs1sk,
    ":gs3pk": GS3_EXECUTION_PK,
    ":gs3sk": gs3sk,
  };

  if (detail.taskToken !== undefined) {
    expressionAttributeValues[":taskToken"] = detail.taskToken;
  }

  return {
    Update: {
      TableName: tableName,
      Key: {
        PK: pk,
        SK: pk,
      },
      UpdateExpression: `
        SET executionId = :executionId,
            entityType = :entityType,
            errorType = :errorType,
            #status = if_not_exists(#status, :defaultStatus),
            county = :county,
            totalOccurrences = if_not_exists(totalOccurrences, :zero) + :totalOccurrences,
            openErrorCount = if_not_exists(openErrorCount, :zero) + :uniqueErrorCount,
            uniqueErrorCount = if_not_exists(uniqueErrorCount, :zero) + :uniqueErrorCount,
            ${taskTokenClause}
            createdAt = if_not_exists(createdAt, :now),
            updatedAt = :now,
            GS1PK = :gs1pk,
            GS1SK = :gs1sk,
            GS3PK = :gs3pk,
            GS3SK = :gs3sk
      `.trim(),
      ExpressionAttributeNames: {
        "#status": "status",
      },
      ExpressionAttributeValues: expressionAttributeValues,
    },
  };
};

/**
 * Retrieves the current totalCount for multiple error codes using BatchGetItem.
 * @param errorCodes - Array of error codes to look up
 * @returns Map of error code to totalCount
 */
const getErrorRecordCounts = async (
  errorCodes: string[],
): Promise<Map<string, number>> => {
  const tableName = getTableName();
  const countMap = new Map<string, number>();

  if (errorCodes.length === 0) {
    return countMap;
  }

  // BatchGetItem is limited to 100 items per request
  const BATCH_LIMIT = 100;

  for (let i = 0; i < errorCodes.length; i += BATCH_LIMIT) {
    const batch = errorCodes.slice(i, i + BATCH_LIMIT);
    const keys = batch.map((errorCode) => {
      const pk = createKey("ERROR", errorCode);
      return { PK: pk, SK: pk };
    });

    const command = new BatchGetCommand({
      RequestItems: {
        [tableName]: {
          Keys: keys,
          ProjectionExpression: "errorCode, totalCount",
        },
      },
    });

    const response = await docClient.send(command);
    const items = response.Responses?.[tableName] ?? [];

    for (const item of items) {
      const errorCode = item.errorCode as string;
      const totalCount = item.totalCount as number;
      countMap.set(errorCode, totalCount);
    }
  }

  return countMap;
};

/**
 * Refreshes GS2SK and GS3SK for multiple error records after a transaction.
 * Fetches the actual totalCount for each error and updates sort keys accordingly.
 * @param errorCodes - Array of error codes to refresh
 */
const refreshErrorRecordSortKeys = async (
  errorCodes: string[],
): Promise<void> => {
  if (errorCodes.length === 0) {
    return;
  }

  const countMap = await getErrorRecordCounts(errorCodes);

  const updatePromises = errorCodes.map((errorCode) => {
    const count = countMap.get(errorCode);
    if (count !== undefined) {
      return updateErrorRecordSortKey(errorCode, count);
    }
    return Promise.resolve();
  });

  await Promise.all(updatePromises);
};

// =============================================================================
// Exported Types
// =============================================================================

/**
 * Sort order for querying executions by error count.
 */
export type SortOrder = "most" | "least";

/**
 * Input parameters for getting an execution by error count.
 */
export interface GetExecutionInput {
  /** Sort order: "most" returns execution with highest error count, "least" returns lowest. */
  sortOrder: SortOrder;
  /** Optional error type filter (first 2 characters of error code). */
  errorType?: string;
}

/**
 * Result of getting an execution with its errors.
 */
export interface GetExecutionResult {
  /** The failed execution item, or null if none found. */
  execution: FailedExecutionItem | null;
  /** Array of execution error links for the execution. */
  errors: ExecutionErrorLink[];
}

/**
 * Result of saving error records to DynamoDB.
 */
export interface SaveErrorRecordsResult {
  /** Whether the save operation was successful. */
  success: boolean;
  /** Number of unique errors saved. */
  uniqueErrorCount: number;
  /** Total occurrences of all errors. */
  totalOccurrences: number;
  /** List of error codes that were saved. */
  errorCodes: string[];
}

/**
 * Result of deleting error links.
 */
export interface DeleteErrorLinksResult {
  /** Number of ExecutionErrorLink items deleted. */
  deletedCount: number;
  /** List of execution IDs affected by the deletion. */
  affectedExecutionIds: string[];
  /** List of error codes whose ErrorRecord was deleted. */
  deletedErrorCodes: string[];
}

/**
 * Result of marking errors as unrecoverable.
 */
export interface MarkUnrecoverableResult {
  /** Number of items updated. */
  updatedCount: number;
  /** List of execution IDs affected by the update. */
  affectedExecutionIds: string[];
  /** List of error codes that were marked as unrecoverable. */
  updatedErrorCodes: string[];
}

/**
 * Result of decrementing the open error count for a failed execution.
 */
export interface DecrementResult {
  /** Whether the decrement operation was successful. */
  success: boolean;
  /** The new open error count after decrement. */
  newOpenErrorCount: number;
  /** The task token for Step Functions callback (if any). */
  taskToken?: string;
  /** Whether the execution item was found. */
  found: boolean;
}

/**
 * Input for batch decrementing open error counts.
 */
export interface BatchDecrementInput {
  /** Execution ID to decrement. */
  executionId: string;
  /** Amount to decrement by. */
  decrementBy: number;
}

/**
 * Input for batch decrementing error record total counts.
 */
export interface BatchDecrementErrorRecordInput {
  /** Error code to decrement. */
  errorCode: string;
  /** Amount to decrement by (sum of occurrences from deleted links). */
  decrementBy: number;
}

/**
 * Result item for batch decrement error record operation.
 */
export interface BatchDecrementErrorRecordResultItem {
  /** The error code that was processed. */
  errorCode: string;
  /** Whether the decrement operation was successful. */
  success: boolean;
  /** The new total count after decrement (-1 if failed). */
  newTotalCount: number;
  /** The error type for the error record (for GSI key updates). */
  errorType?: string;
  /** Whether the error record was found. */
  found: boolean;
  /** Error message if the operation failed. */
  error?: string;
}

/**
 * Result item for batch decrement operation.
 */
export interface BatchDecrementResultItem {
  /** The execution ID that was processed. */
  executionId: string;
  /** Whether the decrement operation was successful. */
  success: boolean;
  /** The new open error count after decrement (-1 if failed). */
  newOpenErrorCount: number;
  /** The task token for Step Functions callback (if any). */
  taskToken?: string;
  /** The error type for the execution (for GSI key updates). */
  errorType?: string;
  /** The county identifier for the execution. */
  county?: string;
  /** Whether the execution item was found. */
  found: boolean;
  /** Error message if the operation failed. */
  error?: string;
}

// =============================================================================
// Error Record Operations
// =============================================================================

/**
 * Updates GS2SK and GS3SK for an ErrorRecord to reflect the new total count.
 * This is a separate operation because these sort keys include the padded count
 * which can only be determined after the atomic increment.
 *
 * Note: This is called after the main transaction to update the sort keys
 * for count-based sorting. In a high-concurrency scenario, this could
 * result in slightly stale sort order, which is acceptable for analytics.
 *
 * @param errorCode - The error code to update
 * @param newCount - The new total count for GS2SK and GS3SK
 */
export const updateErrorRecordSortKey = async (
  errorCode: string,
  newCount: number,
): Promise<void> => {
  const tableName = getTableName();
  const pk = createKey("ERROR", errorCode);
  const errorType = extractErrorType(errorCode);
  // GS2SK format: COUNT#{status}#{paddedCount}#ERROR#{errorCode}
  const gs2sk = `COUNT#${DEFAULT_GSI_STATUS}#${padCount(newCount)}#ERROR#${errorCode}`;
  // GS3SK format: COUNT#{errorType}#{paddedCount}#ERROR#{errorCode}
  const gs3sk = `COUNT#${errorType}#${DEFAULT_GSI_STATUS}#${padCount(newCount)}#ERROR#${errorCode}`;

  const command = new UpdateCommand({
    TableName: tableName,
    Key: {
      PK: pk,
      SK: pk,
    },
    UpdateExpression: "SET GS2SK = :gs2sk, GS3SK = :gs3sk",
    ExpressionAttributeValues: {
      ":gs2sk": gs2sk,
      ":gs3sk": gs3sk,
    },
  });

  await docClient.send(command);
};

/**
 * Saves workflow errors to DynamoDB.
 *
 * ErrorRecord updates are executed individually (not in a transaction) to avoid
 * TransactionConflict errors when multiple events with the same error code are
 * processed concurrently. ErrorRecords are shared across executions, making them
 * prone to conflicts when using transactions.
 *
 * FailedExecutionItem and ExecutionErrorLink updates are kept in a transaction
 * since they are unique per execution and don't conflict across parallel invocations.
 *
 * @param detail - The workflow event detail containing errors
 * @returns Result of the save operation
 * @throws Error if DynamoDB operations fail after retries
 */
export const saveErrorRecords = async (
  detail: WorkflowEventDetail,
): Promise<SaveErrorRecordsResult> => {
  getTableName(); // Validate table name is set

  const errors = detail.errors;
  if (!errors || errors.length === 0) {
    return {
      success: true,
      uniqueErrorCount: 0,
      totalOccurrences: 0,
      errorCodes: [],
    };
  }

  const now = new Date().toISOString();
  const errorOccurrences = countErrorOccurrences(errors);

  // Determine error type for the execution (first 2 characters of error code)
  // All errors within one invocation should have the same type
  const firstErrorCode = errorOccurrences.keys().next().value as string;
  const executionErrorType = extractErrorType(firstErrorCode);

  // Calculate statistics
  const stats: ExecutionErrorStats = {
    totalOccurrences: errors.length,
    uniqueErrorCount: errorOccurrences.size,
    errorType: executionErrorType,
  };

  const errorCodes = Array.from(errorOccurrences.keys());

  // Step 1: Execute ErrorRecord updates individually in parallel with retry.
  // This avoids TransactionConflict errors because individual UpdateCommand
  // operations with atomic counters handle concurrent updates correctly.
  const errorRecordUpdatePromises = Array.from(errorOccurrences.entries()).map(
    ([errorCode, { count, details }]) =>
      executeErrorRecordUpdate(
        errorCode,
        details,
        detail.executionId,
        count,
        now,
      ),
  );

  // Step 2: Build transaction items for FailedExecutionItem and ExecutionErrorLinks.
  // These are unique per execution, so they won't conflict across parallel invocations.
  const transactItems: Array<{
    Update: {
      TableName: string;
      Key: Record<string, string>;
      UpdateExpression: string;
      ExpressionAttributeNames?: Record<string, string>;
      ExpressionAttributeValues: Record<string, string | number | undefined>;
    };
  }> = [];

  // Add FailedExecutionItem update (one per execution)
  transactItems.push(buildFailedExecutionItemUpdate(detail, stats, now));

  // Add ExecutionErrorLink updates for each unique error (NOT ErrorRecord)
  for (const [errorCode, { count, details }] of errorOccurrences) {
    transactItems.push(
      buildExecutionErrorLinkUpdate(
        errorCode,
        detail.executionId,
        detail.county,
        count,
        details,
        now,
      ),
    );
  }

  const TRANSACTION_LIMIT = 100;

  // Step 3: Execute both operations concurrently
  // - ErrorRecord updates run in parallel with retry
  // - Transaction for FailedExecutionItem + ExecutionErrorLinks
  const transactionPromise =
    transactItems.length <= TRANSACTION_LIMIT
      ? docClient.send(
          new TransactWriteCommand({ TransactItems: transactItems }),
        )
      : (async () => {
          // For large transactions, split into batches
          const failedExecutionUpdate = transactItems[0];
          const linkItems = transactItems.slice(1);

          const failedExecutionCommand = new UpdateCommand(
            failedExecutionUpdate.Update,
          );
          await docClient.send(failedExecutionCommand);

          for (let i = 0; i < linkItems.length; i += TRANSACTION_LIMIT) {
            const batch = linkItems.slice(i, i + TRANSACTION_LIMIT);
            const batchCommand = new TransactWriteCommand({
              TransactItems: batch,
            });
            await docClient.send(batchCommand);
          }
        })();

  // Wait for all operations to complete
  await Promise.all([...errorRecordUpdatePromises, transactionPromise]);

  // After all updates, refresh GS2SK with the actual totalCount values.
  // This is done separately because totalCount uses atomic increment,
  // so we need to read the updated values before setting GS2SK.
  await refreshErrorRecordSortKeys(errorCodes);

  return {
    success: true,
    uniqueErrorCount: stats.uniqueErrorCount,
    totalOccurrences: stats.totalOccurrences,
    errorCodes,
  };
};

/**
 * Updates a FailedExecutionItem with taskToken when there are no errors.
 * This is used for SUCCEEDED events (e.g., Prepare success) that need to update the taskToken.
 *
 * @param detail - The workflow event detail (may contain taskToken)
 * @returns Whether the update was successful
 */
export const updateExecutionMetadata = async (
  detail: WorkflowEventDetail,
): Promise<boolean> => {
  getTableName(); // Validate table name is set

  // Only update if taskToken is provided
  if (detail.taskToken === undefined) {
    return true; // Nothing to update
  }

  const tableName = getTableName();
  const pk = createKey("EXECUTION", detail.executionId);
  const now = new Date().toISOString();

  try {
    const command = new UpdateCommand({
      TableName: tableName,
      Key: {
        PK: pk,
        SK: pk,
      },
      UpdateExpression: "SET taskToken = :taskToken, updatedAt = :now",
      ConditionExpression: "entityType = :entityType",
      ExpressionAttributeValues: {
        ":taskToken": detail.taskToken,
        ":now": now,
        ":entityType": ENTITY_TYPES.FAILED_EXECUTION,
      },
    });

    await docClient.send(command);
    return true;
  } catch (error) {
    // Log but don't fail - this is a metadata update, not critical
    console.warn(
      `Failed to update execution metadata for ${detail.executionId}:`,
      error instanceof Error ? error.message : String(error),
    );
    return false;
  }
};

// =============================================================================
// Execution Error Link Operations
// =============================================================================

/**
 * Queries all ExecutionErrorLink items for a given execution.
 * Uses the main table with PK = "EXECUTION#{executionId}" and SK begins_with "ERROR#".
 *
 * @param executionId - The execution ID to query errors for
 * @returns Array of ExecutionErrorLink items
 */
export const queryExecutionErrorLinks = async (
  executionId: string,
): Promise<ExecutionErrorLink[]> => {
  const tableName = getTableName();
  const executionPk = createKey("EXECUTION", executionId);
  const errorSkPrefix = "ERROR#";

  const errors: ExecutionErrorLink[] = [];
  let lastEvaluatedKey: Record<string, unknown> | undefined;

  do {
    const command = new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: "PK = :pk AND begins_with(SK, :skPrefix)",
      ExpressionAttributeValues: {
        ":pk": executionPk,
        ":skPrefix": errorSkPrefix,
      },
      ExclusiveStartKey: lastEvaluatedKey,
    });

    const response = await docClient.send(command);

    if (response.Items && response.Items.length > 0) {
      errors.push(...(response.Items as ExecutionErrorLink[]));
    }

    lastEvaluatedKey = response.LastEvaluatedKey as
      | Record<string, unknown>
      | undefined;
  } while (lastEvaluatedKey);

  return errors;
};

/**
 * Queries all ExecutionErrorLink items for a given error code using GSI1.
 * GSI1PK = "ERROR#{errorCode}", GSI1SK = "EXECUTION#{executionId}"
 *
 * @param errorCode - The error code to query executions for
 * @returns Array of ExecutionErrorLink items
 */
export const queryErrorLinksForErrorCode = async (
  errorCode: string,
): Promise<ExecutionErrorLink[]> => {
  const tableName = getTableName();
  const gs1pk = createKey("ERROR", errorCode);

  const links: ExecutionErrorLink[] = [];
  let lastEvaluatedKey: Record<string, unknown> | undefined;

  do {
    const command = new QueryCommand({
      TableName: tableName,
      IndexName: "GS1",
      KeyConditionExpression: "GS1PK = :gs1pk",
      ExpressionAttributeValues: {
        ":gs1pk": gs1pk,
      },
      ExclusiveStartKey: lastEvaluatedKey,
    });

    const response = await docClient.send(command);

    if (response.Items && response.Items.length > 0) {
      links.push(...(response.Items as ExecutionErrorLink[]));
    }

    lastEvaluatedKey = response.LastEvaluatedKey as
      | Record<string, unknown>
      | undefined;
  } while (lastEvaluatedKey);

  return links;
};

/**
 * Deletes multiple ExecutionErrorLink items from DynamoDB.
 * Uses BatchWriteItem for efficient bulk deletion.
 *
 * @param links - Array of ExecutionErrorLink items to delete
 * @returns Result with deletion count and affected execution IDs
 */
export const deleteExecutionErrorLinks = async (
  links: ExecutionErrorLink[],
): Promise<DeleteErrorLinksResult> => {
  const tableName = getTableName();

  if (links.length === 0) {
    return {
      deletedCount: 0,
      affectedExecutionIds: [],
      deletedErrorCodes: [],
    };
  }

  const affectedExecutionIds = new Set<string>();

  // BatchWriteItem is limited to 25 items per request
  const BATCH_LIMIT = 25;

  for (let i = 0; i < links.length; i += BATCH_LIMIT) {
    const batch = links.slice(i, i + BATCH_LIMIT);
    const deleteRequests = batch.map((link) => {
      affectedExecutionIds.add(link.executionId);
      return {
        DeleteRequest: {
          Key: {
            PK: link.PK,
            SK: link.SK,
          },
        },
      };
    });

    const command = new BatchWriteCommand({
      RequestItems: {
        [tableName]: deleteRequests,
      },
    });

    await docClient.send(command);
  }

  return {
    deletedCount: links.length,
    affectedExecutionIds: Array.from(affectedExecutionIds),
    deletedErrorCodes: [],
  };
};

/**
 * Deletes all ExecutionErrorLink items for a given error code across all executions,
 * and also deletes the ErrorRecord itself.
 * Queries GSI1 to find all links, batch deletes them, then deletes the ErrorRecord.
 *
 * @param errorCode - The error code to delete from all executions
 * @returns Result with deletion count, affected execution IDs, and deleted error codes
 */
export const deleteErrorFromAllExecutions = async (
  errorCode: string,
): Promise<DeleteErrorLinksResult> => {
  const links = await queryErrorLinksForErrorCode(errorCode);
  const linkResult = await deleteExecutionErrorLinks(links);

  // Also delete the ErrorRecord itself
  const deletedErrorCodes = await batchDeleteErrorRecords([errorCode]);

  return {
    deletedCount: linkResult.deletedCount,
    affectedExecutionIds: linkResult.affectedExecutionIds,
    deletedErrorCodes,
  };
};

/**
 * Deletes all errors for a given execution by finding all error codes
 * associated with that execution and then deleting those errors from ALL executions.
 * Also deletes the corresponding ErrorRecord items.
 *
 * @param executionId - The execution ID to resolve errors for
 * @returns Result with total deletion count, all affected execution IDs, and deleted error codes
 */
export const deleteErrorsForExecution = async (
  executionId: string,
): Promise<DeleteErrorLinksResult> => {
  const executionLinks = await queryExecutionErrorLinks(executionId);

  if (executionLinks.length === 0) {
    return {
      deletedCount: 0,
      affectedExecutionIds: [],
      deletedErrorCodes: [],
    };
  }

  const errorCodes = [...new Set(executionLinks.map((link) => link.errorCode))];

  let totalDeleted = 0;
  const allAffectedExecutionIds = new Set<string>();
  const allDeletedErrorCodes: string[] = [];

  for (const errorCode of errorCodes) {
    const result = await deleteErrorFromAllExecutions(errorCode);
    totalDeleted += result.deletedCount;
    for (const execId of result.affectedExecutionIds) {
      allAffectedExecutionIds.add(execId);
    }
    allDeletedErrorCodes.push(...result.deletedErrorCodes);
  }

  return {
    deletedCount: totalDeleted,
    affectedExecutionIds: Array.from(allAffectedExecutionIds),
    deletedErrorCodes: allDeletedErrorCodes,
  };
};

// =============================================================================
// Mark Unrecoverable Operations
// =============================================================================

/**
 * Status value for marking items as unrecoverable.
 */
const UNRECOVERABLE_STATUS: ErrorStatus = "maybeUnrecoverable";

/**
 * Updates a FailedExecutionItem status to maybeUnrecoverable and updates GSI keys.
 *
 * @param executionId - The execution ID to update
 * @returns Whether the update was successful
 */
const updateFailedExecutionToUnrecoverable = async (
  executionId: string,
): Promise<boolean> => {
  const tableName = getTableName();
  const pk = createKey("EXECUTION", executionId);
  const now = new Date().toISOString();

  // First get the current execution to read openErrorCount and errorType
  const execution = await getFailedExecutionItem(executionId);
  if (!execution) {
    return false;
  }

  const gsiStatus = toGsiStatus(UNRECOVERABLE_STATUS);
  // GS1SK format: COUNT#{status}#{paddedCount}#EXECUTION#{executionId}
  const gs1sk = `COUNT#${gsiStatus}#${padCount(execution.openErrorCount)}#EXECUTION#${executionId}`;
  // GS3SK format: COUNT#{errorType}#{status}#{paddedCount}#EXECUTION#{executionId}
  const gs3sk = `COUNT#${execution.errorType}#${gsiStatus}#${padCount(execution.openErrorCount)}#EXECUTION#${executionId}`;

  try {
    const command = new UpdateCommand({
      TableName: tableName,
      Key: {
        PK: pk,
        SK: pk,
      },
      UpdateExpression:
        "SET #status = :status, GS1SK = :gs1sk, GS3SK = :gs3sk, updatedAt = :now",
      ConditionExpression: "entityType = :entityType",
      ExpressionAttributeNames: {
        "#status": "status",
      },
      ExpressionAttributeValues: {
        ":status": UNRECOVERABLE_STATUS,
        ":gs1sk": gs1sk,
        ":gs3sk": gs3sk,
        ":now": now,
        ":entityType": ENTITY_TYPES.FAILED_EXECUTION,
      },
    });

    await docClient.send(command);
    return true;
  } catch (error) {
    // Log all errors and return false for consistent error handling
    // This prevents partial update failures from breaking the entire operation
    console.warn(
      `Failed to update FailedExecutionItem ${executionId}:`,
      error instanceof Error ? error.message : String(error),
    );
    return false;
  }
};

/**
 * Updates multiple ExecutionErrorLink items' status to maybeUnrecoverable.
 *
 * @param links - Array of ExecutionErrorLink items to update
 * @returns Number of successfully updated items
 */
const updateExecutionErrorLinksToUnrecoverable = async (
  links: ExecutionErrorLink[],
): Promise<number> => {
  if (links.length === 0) {
    return 0;
  }

  const tableName = getTableName();
  const now = new Date().toISOString();
  let updatedCount = 0;

  // Update each link individually since BatchWriteItem doesn't support updates
  const updatePromises = links.map(async (link) => {
    try {
      const command = new UpdateCommand({
        TableName: tableName,
        Key: {
          PK: link.PK,
          SK: link.SK,
        },
        UpdateExpression: "SET #status = :status, updatedAt = :now",
        ConditionExpression: "entityType = :entityType",
        ExpressionAttributeNames: {
          "#status": "status",
        },
        ExpressionAttributeValues: {
          ":status": UNRECOVERABLE_STATUS,
          ":now": now,
          ":entityType": ENTITY_TYPES.EXECUTION_ERROR,
        },
      });

      await docClient.send(command);
      return true;
    } catch (error) {
      console.warn(
        `Failed to update ExecutionErrorLink ${link.PK}/${link.SK}:`,
        error instanceof Error ? error.message : String(error),
      );
      return false;
    }
  });

  const results = await Promise.all(updatePromises);
  updatedCount = results.filter(Boolean).length;

  return updatedCount;
};

/**
 * Updates an ErrorRecord status to maybeUnrecoverable and updates GSI keys.
 * This ensures the error no longer appears in the default "FAILED" dashboard view.
 *
 * @param errorCode - The error code to update
 * @returns Whether the update was successful
 */
const updateErrorRecordToUnrecoverable = async (
  errorCode: string,
): Promise<boolean> => {
  const tableName = getTableName();
  const pk = createKey("ERROR", errorCode);
  const now = new Date().toISOString();

  // First get the current error record to read totalCount
  const getCommand = new GetCommand({
    TableName: tableName,
    Key: {
      PK: pk,
      SK: pk,
    },
  });

  const getResponse = await docClient.send(getCommand);
  const errorRecord = getResponse.Item as ErrorRecord | undefined;

  if (!errorRecord || errorRecord.entityType !== ENTITY_TYPES.ERROR) {
    console.warn(`ErrorRecord not found for error code: ${errorCode}`);
    return false;
  }

  const gsiStatus = toGsiStatus(UNRECOVERABLE_STATUS);
  const errorType = extractErrorType(errorCode);
  // GS2SK format: COUNT#{status}#{paddedCount}#ERROR#{errorCode}
  const gs2sk = `COUNT#${gsiStatus}#${padCount(errorRecord.totalCount)}#ERROR#${errorCode}`;
  // GS3SK format: COUNT#{errorType}#{status}#{paddedCount}#ERROR#{errorCode}
  const gs3sk = `COUNT#${errorType}#${gsiStatus}#${padCount(errorRecord.totalCount)}#ERROR#${errorCode}`;

  try {
    const command = new UpdateCommand({
      TableName: tableName,
      Key: {
        PK: pk,
        SK: pk,
      },
      UpdateExpression:
        "SET errorStatus = :status, GS2SK = :gs2sk, GS3SK = :gs3sk, updatedAt = :now",
      ConditionExpression: "entityType = :entityType",
      ExpressionAttributeValues: {
        ":status": UNRECOVERABLE_STATUS,
        ":gs2sk": gs2sk,
        ":gs3sk": gs3sk,
        ":now": now,
        ":entityType": ENTITY_TYPES.ERROR,
      },
    });

    await docClient.send(command);
    return true;
  } catch (error) {
    console.warn(
      `Failed to update ErrorRecord ${errorCode}:`,
      error instanceof Error ? error.message : String(error),
    );
    return false;
  }
};

/**
 * Marks all errors for a given execution as maybeUnrecoverable.
 * Updates the FailedExecutionItem, all associated ExecutionErrorLink items,
 * and all associated ErrorRecord items.
 *
 * @param executionId - The execution ID to mark as unrecoverable
 * @returns Result with update count and affected items
 */
export const markErrorsAsUnrecoverableForExecution = async (
  executionId: string,
): Promise<MarkUnrecoverableResult> => {
  // Get all error links for this execution
  const executionLinks = await queryExecutionErrorLinks(executionId);

  if (executionLinks.length === 0) {
    return {
      updatedCount: 0,
      affectedExecutionIds: [],
      updatedErrorCodes: [],
    };
  }

  // Update the FailedExecutionItem
  const executionUpdated =
    await updateFailedExecutionToUnrecoverable(executionId);

  // Update all ExecutionErrorLink items
  const linksUpdatedCount =
    await updateExecutionErrorLinksToUnrecoverable(executionLinks);

  const errorCodes = [...new Set(executionLinks.map((link) => link.errorCode))];

  // Update all ErrorRecord items so they no longer appear in the default dashboard view
  const errorRecordUpdatePromises = errorCodes.map((errorCode) =>
    updateErrorRecordToUnrecoverable(errorCode),
  );
  const errorRecordResults = await Promise.all(errorRecordUpdatePromises);
  const errorRecordsUpdatedCount = errorRecordResults.filter(Boolean).length;

  return {
    updatedCount:
      (executionUpdated ? 1 : 0) + linksUpdatedCount + errorRecordsUpdatedCount,
    affectedExecutionIds: [executionId],
    updatedErrorCodes: errorCodes,
  };
};

/**
 * Marks a specific error code as maybeUnrecoverable across all executions.
 * Updates all ExecutionErrorLink items with that error code, their parent
 * FailedExecutionItem records, and the ErrorRecord itself.
 *
 * @param errorCode - The error code to mark as unrecoverable
 * @returns Result with update count and affected items
 */
export const markErrorAsUnrecoverableFromAllExecutions = async (
  errorCode: string,
): Promise<MarkUnrecoverableResult> => {
  // Get all execution links for this error code
  const links = await queryErrorLinksForErrorCode(errorCode);

  if (links.length === 0) {
    return {
      updatedCount: 0,
      affectedExecutionIds: [],
      updatedErrorCodes: [],
    };
  }

  // Get unique execution IDs
  const executionIds = [...new Set(links.map((link) => link.executionId))];

  // Update all ExecutionErrorLink items
  const linksUpdatedCount =
    await updateExecutionErrorLinksToUnrecoverable(links);

  // Update all affected FailedExecutionItem records
  let executionsUpdated = 0;
  for (const execId of executionIds) {
    const success = await updateFailedExecutionToUnrecoverable(execId);
    if (success) {
      executionsUpdated++;
    }
  }

  // Update the ErrorRecord so it no longer appears in the default dashboard view
  const errorRecordUpdated = await updateErrorRecordToUnrecoverable(errorCode);

  return {
    updatedCount:
      executionsUpdated + linksUpdatedCount + (errorRecordUpdated ? 1 : 0),
    affectedExecutionIds: executionIds,
    updatedErrorCodes: [errorCode],
  };
};

// =============================================================================
// Failed Execution Operations
// =============================================================================

/**
 * Queries for a failed execution sorted by error count.
 * Uses GS1 when no errorType filter is provided, GS3 when errorType is specified.
 * Only returns executions with FAILED status.
 *
 * GSI Strategy:
 * - GS1: GS1PK = "METRIC#ERRORCOUNT", GS1SK = "COUNT#{status}#{paddedCount}#EXECUTION#{executionId}"
 * - GS3: GS3PK = "METRIC#ERRORCOUNT" (FailedExecutionItem only; ErrorRecord uses "METRIC#ERRORCOUNT#ERROR"),
 *        GS3SK = "COUNT#{errorType}#{status}#{paddedCount}#EXECUTION#{executionId}"
 *
 * Note: ErrorRecord uses GS3PK = "METRIC#ERRORCOUNT#ERROR" to achieve partition-level
 * separation from FailedExecutionItem, eliminating the need for FilterExpression.
 *
 * @param input - Query parameters including sortOrder and optional errorType
 * @returns The matching FailedExecutionItem or null if none found
 */
export const queryExecutionByErrorCount = async (
  input: GetExecutionInput,
): Promise<FailedExecutionItem | null> => {
  const tableName = getTableName();
  const { sortOrder, errorType } = input;
  const scanIndexForward = sortOrder === "least";

  if (errorType) {
    // Query GS3 with errorType filter
    // GS3PK partition separation ensures only FailedExecutionItem records are returned
    // (ErrorRecord uses GS3PK = "METRIC#ERRORCOUNT#ERROR")
    const command = new QueryCommand({
      TableName: tableName,
      IndexName: "GS3",
      KeyConditionExpression:
        "GS3PK = :gs3pk AND begins_with(GS3SK, :gs3skPrefix)",
      ExpressionAttributeValues: {
        ":gs3pk": GS3_EXECUTION_PK,
        ":gs3skPrefix": `COUNT#${errorType}#${DEFAULT_GSI_STATUS}#`,
      },
      ScanIndexForward: scanIndexForward,
      Limit: 1,
    });

    const response = await docClient.send(command);

    if (!response.Items || response.Items.length === 0) {
      return null;
    }

    return response.Items[0] as FailedExecutionItem;
  } else {
    const command = new QueryCommand({
      TableName: tableName,
      IndexName: "GS1",
      KeyConditionExpression:
        "GS1PK = :gs1pk AND begins_with(GS1SK, :gs1skPrefix)",
      ExpressionAttributeValues: {
        ":gs1pk": GS3_EXECUTION_PK,
        ":gs1skPrefix": `COUNT#${DEFAULT_GSI_STATUS}#`,
      },
      ScanIndexForward: scanIndexForward,
      Limit: 1,
    });

    const response = await docClient.send(command);

    if (!response.Items || response.Items.length === 0) {
      return null;
    }

    return response.Items[0] as FailedExecutionItem;
  }
};

/**
 * Gets an execution with all its errors based on sort order and optional error type filter.
 *
 * @param input - Query parameters including sortOrder and optional errorType
 * @returns Object containing the execution and its errors
 */
export const getExecutionWithErrors = async (
  input: GetExecutionInput,
): Promise<GetExecutionResult> => {
  const execution = await queryExecutionByErrorCount(input);

  if (!execution) {
    return {
      execution: null,
      errors: [],
    };
  }

  const errors = await queryExecutionErrorLinks(execution.executionId);

  return {
    execution,
    errors,
  };
};

/**
 * Atomically decrements the openErrorCount for a FailedExecutionItem.
 * Returns the updated count and taskToken if available.
 *
 * @param executionId - The execution ID to decrement the error count for
 * @returns Result containing the new count and task token
 */
export const decrementOpenErrorCount = async (
  executionId: string,
): Promise<DecrementResult> => {
  const tableName = getTableName();
  const pk = createKey("EXECUTION", executionId);

  try {
    const command = new UpdateCommand({
      TableName: tableName,
      Key: {
        PK: pk,
        SK: pk,
      },
      UpdateExpression:
        "SET openErrorCount = openErrorCount - :one, updatedAt = :now",
      ConditionExpression:
        "attribute_exists(PK) AND entityType = :entityType AND openErrorCount > :zero",
      ExpressionAttributeValues: {
        ":one": 1,
        ":zero": 0,
        ":now": new Date().toISOString(),
        ":entityType": ENTITY_TYPES.FAILED_EXECUTION,
      },
      ReturnValues: "ALL_NEW",
    });

    const response = await docClient.send(command);
    const item = response.Attributes as FailedExecutionItem | undefined;

    if (!item) {
      return {
        success: false,
        newOpenErrorCount: -1,
        found: false,
      };
    }

    return {
      success: true,
      newOpenErrorCount: item.openErrorCount,
      taskToken: item.taskToken,
      found: true,
    };
  } catch (error) {
    // ConditionalCheckFailedException means the item doesn't exist or count is already 0
    if (
      error instanceof Error &&
      error.name === "ConditionalCheckFailedException"
    ) {
      return {
        success: false,
        newOpenErrorCount: 0,
        found: false,
      };
    }
    throw error;
  }
};

/**
 * Gets a FailedExecutionItem by execution ID.
 *
 * @param executionId - The execution ID to get
 * @returns The FailedExecutionItem or null if not found
 */
export const getFailedExecutionItem = async (
  executionId: string,
): Promise<FailedExecutionItem | null> => {
  const tableName = getTableName();
  const pk = createKey("EXECUTION", executionId);

  const command = new GetCommand({
    TableName: tableName,
    Key: {
      PK: pk,
      SK: pk,
    },
  });

  const response = await docClient.send(command);

  if (!response.Item) {
    return null;
  }

  const item = response.Item as FailedExecutionItem;

  // Verify it's a FailedExecution entity
  if (item.entityType !== ENTITY_TYPES.FAILED_EXECUTION) {
    return null;
  }

  return item;
};

/**
 * Deletes a FailedExecutionItem by execution ID.
 *
 * @param executionId - The execution ID to delete
 * @returns Whether the deletion was successful
 */
export const deleteFailedExecutionItem = async (
  executionId: string,
): Promise<boolean> => {
  const tableName = getTableName();
  const pk = createKey("EXECUTION", executionId);

  try {
    const command = new DeleteCommand({
      TableName: tableName,
      Key: {
        PK: pk,
        SK: pk,
      },
      ConditionExpression: "entityType = :entityType",
      ExpressionAttributeValues: {
        ":entityType": ENTITY_TYPES.FAILED_EXECUTION,
      },
    });

    await docClient.send(command);
    return true;
  } catch (error) {
    // ConditionalCheckFailedException means the item doesn't exist or wrong entity type
    if (
      error instanceof Error &&
      error.name === "ConditionalCheckFailedException"
    ) {
      return false;
    }
    throw error;
  }
};

// =============================================================================
// Batch Operations for Error Count Handler
// =============================================================================

/**
 * Decrements the openErrorCount for a single execution by a specified amount.
 * Used internally by batchDecrementOpenErrorCounts.
 *
 * @param executionId - The execution ID to decrement
 * @param decrementBy - The amount to decrement by
 * @returns Result containing the new count, task token, and error type
 */
const decrementOpenErrorCountByAmount = async (
  executionId: string,
  decrementBy: number,
): Promise<BatchDecrementResultItem> => {
  const tableName = getTableName();
  const pk = createKey("EXECUTION", executionId);
  const now = new Date().toISOString();

  try {
    const command = new UpdateCommand({
      TableName: tableName,
      Key: {
        PK: pk,
        SK: pk,
      },
      UpdateExpression:
        "SET openErrorCount = openErrorCount - :amount, updatedAt = :now",
      ConditionExpression:
        "attribute_exists(PK) AND entityType = :entityType AND openErrorCount >= :amount",
      ExpressionAttributeValues: {
        ":amount": decrementBy,
        ":now": now,
        ":entityType": ENTITY_TYPES.FAILED_EXECUTION,
      },
      ReturnValues: "ALL_NEW",
    });

    const response = await docClient.send(command);
    const item = response.Attributes as FailedExecutionItem | undefined;

    if (!item) {
      return {
        executionId,
        success: false,
        newOpenErrorCount: -1,
        found: false,
      };
    }

    return {
      executionId,
      success: true,
      newOpenErrorCount: item.openErrorCount,
      taskToken: item.taskToken,
      errorType: item.errorType,
      county: item.county,
      found: true,
    };
  } catch (error) {
    if (
      error instanceof Error &&
      error.name === "ConditionalCheckFailedException"
    ) {
      return {
        executionId,
        success: false,
        newOpenErrorCount: 0,
        found: false,
        error: "Condition check failed - item not found or insufficient count",
      };
    }
    return {
      executionId,
      success: false,
      newOpenErrorCount: -1,
      found: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
};

/**
 * Batch decrements openErrorCount for multiple executions in parallel.
 * Each execution can have a different decrement amount.
 * Uses parallel UpdateCommands since DynamoDB doesn't support batch updates.
 *
 * @param inputs - Array of execution IDs and their decrement amounts
 * @returns Array of results for each execution
 */
export const batchDecrementOpenErrorCounts = async (
  inputs: BatchDecrementInput[],
): Promise<BatchDecrementResultItem[]> => {
  if (inputs.length === 0) {
    return [];
  }

  const promises = inputs.map(({ executionId, decrementBy }) =>
    decrementOpenErrorCountByAmount(executionId, decrementBy),
  );

  return Promise.all(promises);
};

/**
 * Updates GSI keys (GS1SK, GS3SK) for executions that still have errors remaining.
 * This keeps the sorting by error count accurate after decrements.
 * Uses parallel UpdateCommands since BatchWriteItem doesn't support attribute updates.
 *
 * @param updates - Array of execution IDs with their new open error counts and error types
 */
export const batchUpdateExecutionGsiKeys = async (
  updates: Array<{
    executionId: string;
    newOpenErrorCount: number;
    errorType: string;
  }>,
): Promise<void> => {
  if (updates.length === 0) {
    return;
  }

  const tableName = getTableName();
  const now = new Date().toISOString();

  const updatePromises = updates.map(
    async ({ executionId, newOpenErrorCount, errorType }) => {
      const pk = createKey("EXECUTION", executionId);
      // GS1SK format: COUNT#{status}#{paddedCount}#EXECUTION#{executionId}
      const gs1sk = `COUNT#${DEFAULT_GSI_STATUS}#${padCount(newOpenErrorCount)}#EXECUTION#${executionId}`;
      // GS3SK format: COUNT#{errorType}#{status}#{paddedCount}#EXECUTION#{executionId}
      const gs3sk = `COUNT#${errorType}#${DEFAULT_GSI_STATUS}#${padCount(newOpenErrorCount)}#EXECUTION#${executionId}`;

      const command = new UpdateCommand({
        TableName: tableName,
        Key: {
          PK: pk,
          SK: pk,
        },
        UpdateExpression:
          "SET GS1SK = :gs1sk, GS3SK = :gs3sk, updatedAt = :now",
        ConditionExpression: "entityType = :entityType",
        ExpressionAttributeValues: {
          ":gs1sk": gs1sk,
          ":gs3sk": gs3sk,
          ":now": now,
          ":entityType": ENTITY_TYPES.FAILED_EXECUTION,
        },
      });

      try {
        await docClient.send(command);
      } catch (error) {
        console.warn(
          `Failed to update GSI keys for execution ${executionId}:`,
          error instanceof Error ? error.message : String(error),
        );
      }
    },
  );

  await Promise.all(updatePromises);
};

/**
 * Batch deletes multiple FailedExecutionItems using BatchWriteItem.
 * Handles DynamoDB's 25-item limit per batch and retries unprocessed items.
 *
 * @param executionIds - Array of execution IDs to delete
 * @returns Array of execution IDs that were successfully deleted
 */
export const batchDeleteFailedExecutionItems = async (
  executionIds: string[],
): Promise<string[]> => {
  if (executionIds.length === 0) {
    return [];
  }

  const tableName = getTableName();
  const deletedIds: string[] = [];
  const BATCH_LIMIT = 25;

  for (let i = 0; i < executionIds.length; i += BATCH_LIMIT) {
    const batch = executionIds.slice(i, i + BATCH_LIMIT);
    const pendingIds = new Set(batch);

    let deleteRequests = batch.map((executionId) => {
      const pk = createKey("EXECUTION", executionId);
      return {
        DeleteRequest: {
          Key: {
            PK: pk,
            SK: pk,
          },
        },
      };
    });

    let retryCount = 0;
    const MAX_RETRIES = 3;

    while (deleteRequests.length > 0 && retryCount < MAX_RETRIES) {
      const command = new BatchWriteCommand({
        RequestItems: {
          [tableName]: deleteRequests,
        },
      });

      try {
        const response = await docClient.send(command);
        const unprocessed = response.UnprocessedItems?.[tableName];

        if (unprocessed && unprocessed.length > 0) {
          const unprocessedPks = new Set(
            unprocessed.map((item) => item.DeleteRequest?.Key?.PK as string),
          );

          for (const executionId of pendingIds) {
            const pk = createKey("EXECUTION", executionId);
            if (!unprocessedPks.has(pk)) {
              deletedIds.push(executionId);
              pendingIds.delete(executionId);
            }
          }

          deleteRequests = unprocessed as typeof deleteRequests;
          retryCount++;
          await new Promise((resolve) =>
            setTimeout(resolve, Math.pow(2, retryCount) * 100),
          );
        } else {
          for (const executionId of pendingIds) {
            deletedIds.push(executionId);
          }
          pendingIds.clear();
          deleteRequests = [];
        }
      } catch (error) {
        console.error(
          `Batch delete failed for batch starting at index ${i}:`,
          error instanceof Error ? error.message : String(error),
        );
        retryCount++;
        if (retryCount >= MAX_RETRIES) {
          console.error(`Max retries reached for batch starting at index ${i}`);
        }
      }
    }
  }

  return deletedIds;
};

// =============================================================================
// Batch Operations for Error Record Count Handler
// =============================================================================

/**
 * Decrements the totalCount for a single error record by a specified amount.
 * Used internally by batchDecrementErrorRecordCounts.
 *
 * @param errorCode - The error code to decrement
 * @param decrementBy - The amount to decrement by
 * @returns Result containing the new count and error type
 */
const decrementErrorRecordCountByAmount = async (
  errorCode: string,
  decrementBy: number,
): Promise<BatchDecrementErrorRecordResultItem> => {
  const tableName = getTableName();
  const pk = createKey("ERROR", errorCode);
  const now = new Date().toISOString();

  try {
    const command = new UpdateCommand({
      TableName: tableName,
      Key: {
        PK: pk,
        SK: pk,
      },
      UpdateExpression:
        "SET totalCount = totalCount - :amount, updatedAt = :now",
      ConditionExpression:
        "attribute_exists(PK) AND entityType = :entityType AND totalCount >= :amount",
      ExpressionAttributeValues: {
        ":amount": decrementBy,
        ":now": now,
        ":entityType": ENTITY_TYPES.ERROR,
      },
      ReturnValues: "ALL_NEW",
    });

    const response = await docClient.send(command);
    const item = response.Attributes as ErrorRecord | undefined;

    if (!item) {
      return {
        errorCode,
        success: false,
        newTotalCount: -1,
        found: false,
      };
    }

    return {
      errorCode,
      success: true,
      newTotalCount: item.totalCount,
      errorType: item.errorType,
      found: true,
    };
  } catch (error) {
    if (
      error instanceof Error &&
      error.name === "ConditionalCheckFailedException"
    ) {
      return {
        errorCode,
        success: false,
        newTotalCount: 0,
        found: false,
        error: "Condition check failed - item not found or insufficient count",
      };
    }
    return {
      errorCode,
      success: false,
      newTotalCount: -1,
      found: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
};

/**
 * Batch decrements totalCount for multiple error records in parallel.
 * Each error code can have a different decrement amount.
 * Uses parallel UpdateCommands since DynamoDB doesn't support batch updates.
 *
 * @param inputs - Array of error codes and their decrement amounts
 * @returns Array of results for each error code
 */
export const batchDecrementErrorRecordCounts = async (
  inputs: BatchDecrementErrorRecordInput[],
): Promise<BatchDecrementErrorRecordResultItem[]> => {
  if (inputs.length === 0) {
    return [];
  }

  const promises = inputs.map(({ errorCode, decrementBy }) =>
    decrementErrorRecordCountByAmount(errorCode, decrementBy),
  );

  return Promise.all(promises);
};

/**
 * Updates GSI keys (GS2SK, GS3SK) for error records that still have occurrences remaining.
 * This keeps the sorting by total count accurate after decrements.
 * Uses parallel UpdateCommands since BatchWriteItem doesn't support attribute updates.
 *
 * @param updates - Array of error codes with their new total counts and error types
 */
export const batchUpdateErrorRecordGsiKeys = async (
  updates: Array<{
    errorCode: string;
    newTotalCount: number;
    errorType: string;
  }>,
): Promise<void> => {
  if (updates.length === 0) {
    return;
  }

  const tableName = getTableName();
  const now = new Date().toISOString();

  const updatePromises = updates.map(
    async ({ errorCode, newTotalCount, errorType }) => {
      const pk = createKey("ERROR", errorCode);
      // GS2SK format: COUNT#{status}#{paddedCount}#ERROR#{errorCode}
      const gs2sk = `COUNT#${DEFAULT_GSI_STATUS}#${padCount(newTotalCount)}#ERROR#${errorCode}`;
      // GS3SK format: COUNT#{errorType}#{paddedCount}#ERROR#{errorCode}
      const gs3sk = `COUNT#${errorType}#${padCount(newTotalCount)}#ERROR#${errorCode}`;

      const command = new UpdateCommand({
        TableName: tableName,
        Key: {
          PK: pk,
          SK: pk,
        },
        UpdateExpression:
          "SET GS2SK = :gs2sk, GS3SK = :gs3sk, updatedAt = :now",
        ConditionExpression: "entityType = :entityType",
        ExpressionAttributeValues: {
          ":gs2sk": gs2sk,
          ":gs3sk": gs3sk,
          ":now": now,
          ":entityType": ENTITY_TYPES.ERROR,
        },
      });

      try {
        await docClient.send(command);
      } catch (error) {
        console.warn(
          `Failed to update GSI keys for error ${errorCode}:`,
          error instanceof Error ? error.message : String(error),
        );
      }
    },
  );

  await Promise.all(updatePromises);
};

/**
 * Batch deletes multiple ErrorRecords using BatchWriteItem.
 * Handles DynamoDB's 25-item limit per batch and retries unprocessed items.
 *
 * @param errorCodes - Array of error codes to delete
 * @returns Array of error codes that were successfully deleted
 */
export const batchDeleteErrorRecords = async (
  errorCodes: string[],
): Promise<string[]> => {
  if (errorCodes.length === 0) {
    return [];
  }

  const tableName = getTableName();
  const deletedCodes: string[] = [];
  const BATCH_LIMIT = 25;

  for (let i = 0; i < errorCodes.length; i += BATCH_LIMIT) {
    const batch = errorCodes.slice(i, i + BATCH_LIMIT);
    const pendingCodes = new Set(batch);

    let deleteRequests = batch.map((errorCode) => {
      const pk = createKey("ERROR", errorCode);
      return {
        DeleteRequest: {
          Key: {
            PK: pk,
            SK: pk,
          },
        },
      };
    });

    let retryCount = 0;
    const MAX_RETRIES = 3;

    while (deleteRequests.length > 0 && retryCount < MAX_RETRIES) {
      const command = new BatchWriteCommand({
        RequestItems: {
          [tableName]: deleteRequests,
        },
      });

      try {
        const response = await docClient.send(command);
        const unprocessed = response.UnprocessedItems?.[tableName];

        if (unprocessed && unprocessed.length > 0) {
          const unprocessedPks = new Set(
            unprocessed.map((item) => item.DeleteRequest?.Key?.PK as string),
          );

          for (const errorCode of pendingCodes) {
            const pk = createKey("ERROR", errorCode);
            if (!unprocessedPks.has(pk)) {
              deletedCodes.push(errorCode);
              pendingCodes.delete(errorCode);
            }
          }

          deleteRequests = unprocessed as typeof deleteRequests;
          retryCount++;
          await new Promise((resolve) =>
            setTimeout(resolve, Math.pow(2, retryCount) * 100),
          );
        } else {
          for (const errorCode of pendingCodes) {
            deletedCodes.push(errorCode);
          }
          pendingCodes.clear();
          deleteRequests = [];
        }
      } catch (error) {
        console.error(
          `Batch delete failed for batch starting at index ${i}:`,
          error instanceof Error ? error.message : String(error),
        );
        retryCount++;
        if (retryCount >= MAX_RETRIES) {
          console.error(`Max retries reached for batch starting at index ${i}`);
        }
      }
    }
  }

  return deletedCodes;
};
