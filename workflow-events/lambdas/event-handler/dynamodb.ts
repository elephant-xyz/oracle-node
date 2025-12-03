import {
  TransactWriteCommand,
  UpdateCommand,
  BatchGetCommand,
} from "@aws-sdk/lib-dynamodb";
import type {
  WorkflowEventDetail,
  WorkflowError,
  ErrorRecord,
  ExecutionErrorLink,
  FailedExecutionItem,
  ErrorStatus,
} from "shared/types.js";
import { TABLE_NAME, docClient } from "shared/dynamodb-client.js";
import {
  ENTITY_TYPES,
  padCount,
  extractErrorType,
  createKey,
} from "shared/keys.js";

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
  const pk = createKey("ERROR", errorCode);
  const errorType = extractErrorType(errorCode);
  // Note: GS2SK and GS3SK are NOT set here - they are updated separately
  // after the transaction via refreshErrorRecordSortKeys to reflect the
  // actual totalCount (since totalCount uses atomic increment).

  return {
    Update: {
      TableName: TABLE_NAME!,
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
        ":gs3pk": "METRIC#ERRORCOUNT",
      },
    },
  };
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
  const pk = createKey("EXECUTION", executionId);
  const sk = createKey("ERROR", errorCode);

  return {
    Update: {
      TableName: TABLE_NAME!,
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
    ExpressionAttributeValues: Record<string, string | number | undefined>;
  };
} => {
  const pk = createKey("EXECUTION", detail.executionId);
  // GS1SK format: COUNT#{paddedCount}#EXECUTION#{executionId}
  const gs1sk = `COUNT#${padCount(stats.uniqueErrorCount)}#EXECUTION#${detail.executionId}`;
  // GS3SK format: COUNT#{errorType}#{paddedCount}#EXECUTION#{executionId}
  const gs3sk = `COUNT#${stats.errorType}#${padCount(stats.uniqueErrorCount)}#EXECUTION#${detail.executionId}`;

  return {
    Update: {
      TableName: TABLE_NAME!,
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
            taskToken = :taskToken,
            preparedS3Uri = :preparedS3Uri,
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
      ExpressionAttributeValues: {
        ":executionId": detail.executionId,
        ":entityType": ENTITY_TYPES.FAILED_EXECUTION,
        ":errorType": stats.errorType,
        ":defaultStatus": DEFAULT_ERROR_STATUS,
        ":county": detail.county,
        ":zero": 0,
        ":totalOccurrences": stats.totalOccurrences,
        ":uniqueErrorCount": stats.uniqueErrorCount,
        ":taskToken": detail.taskToken,
        ":preparedS3Uri": detail.preparedS3Uri,
        ":now": now,
        ":gs1pk": "METRIC#ERRORCOUNT",
        ":gs1sk": gs1sk,
        ":gs3pk": "METRIC#ERRORCOUNT",
        ":gs3sk": gs3sk,
      },
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
  if (!TABLE_NAME) {
    throw new Error(
      "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
    );
  }

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
        [TABLE_NAME]: {
          Keys: keys,
          ProjectionExpression: "errorCode, totalCount",
        },
      },
    });

    const response = await docClient.send(command);
    const items = response.Responses?.[TABLE_NAME] ?? [];

    for (const item of items) {
      const errorCode = item.errorCode as string;
      const totalCount = item.totalCount as number;
      countMap.set(errorCode, totalCount);
    }
  }

  return countMap;
};

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
  if (!TABLE_NAME) {
    throw new Error(
      "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
    );
  }

  const pk = createKey("ERROR", errorCode);
  const errorType = extractErrorType(errorCode);
  // GS2SK format: COUNT#{paddedCount}#ERROR#{errorCode}
  const gs2sk = `COUNT#${padCount(newCount)}#ERROR#${errorCode}`;
  // GS3SK format: COUNT#{errorType}#{paddedCount}#ERROR#{errorCode}
  const gs3sk = `COUNT#${errorType}#${padCount(newCount)}#ERROR#${errorCode}`;

  const command = new UpdateCommand({
    TableName: TABLE_NAME,
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

  // Get current counts for all error codes
  const countMap = await getErrorRecordCounts(errorCodes);

  // Update GS2SK for each error record
  const updatePromises = errorCodes.map((errorCode) => {
    const count = countMap.get(errorCode);
    if (count !== undefined) {
      return updateErrorRecordSortKey(errorCode, count);
    }
    return Promise.resolve();
  });

  await Promise.all(updatePromises);
};

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
 * Saves workflow errors to DynamoDB using transactional writes.
 * Creates or updates ErrorRecord, ExecutionErrorLink, and FailedExecutionItem.
 *
 * @param detail - The workflow event detail containing errors
 * @returns Result of the save operation
 * @throws Error if DynamoDB transaction fails
 */
export const saveErrorRecords = async (
  detail: WorkflowEventDetail,
): Promise<SaveErrorRecordsResult> => {
  if (!TABLE_NAME) {
    throw new Error(
      "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
    );
  }

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

  // Build transaction items for each unique error
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

  // Add ErrorRecord and ExecutionErrorLink updates for each unique error
  for (const [errorCode, { count, details }] of errorOccurrences) {
    transactItems.push(
      buildErrorRecordUpdate(
        errorCode,
        details,
        detail.executionId,
        count,
        now,
      ),
    );
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

  // DynamoDB transactions are limited to 100 items
  // For large error sets, we need to batch the transactions
  const TRANSACTION_LIMIT = 100;
  const errorCodes = Array.from(errorOccurrences.keys());

  if (transactItems.length <= TRANSACTION_LIMIT) {
    // Single transaction for small error sets
    const command = new TransactWriteCommand({
      TransactItems: transactItems,
    });
    await docClient.send(command);
  } else {
    // For large error sets, process FailedExecutionItem first,
    // then batch the error-related items
    const failedExecutionUpdate = transactItems[0];
    const errorItems = transactItems.slice(1);

    // Update FailedExecutionItem separately
    const failedExecutionCommand = new UpdateCommand(
      failedExecutionUpdate.Update,
    );
    await docClient.send(failedExecutionCommand);

    // Batch error items (ErrorRecord + ExecutionErrorLink pairs)
    for (let i = 0; i < errorItems.length; i += TRANSACTION_LIMIT) {
      const batch = errorItems.slice(i, i + TRANSACTION_LIMIT);
      const batchCommand = new TransactWriteCommand({
        TransactItems: batch,
      });
      await docClient.send(batchCommand);
    }
  }

  // After the transaction, refresh GS2SK with the actual totalCount values.
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
