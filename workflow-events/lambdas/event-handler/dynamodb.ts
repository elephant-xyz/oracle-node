import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  TransactWriteCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import type {
  WorkflowEventDetail,
  WorkflowError,
  ErrorRecord,
  ExecutionErrorLink,
  FailedExecutionItem,
  ErrorStatus,
} from "./types.js";

/**
 * Environment variable for DynamoDB table name.
 */
const TABLE_NAME = process.env.WORKFLOW_ERRORS_TABLE_NAME;

/**
 * Entity type discriminators for single-table design.
 */
const ENTITY_TYPES = {
  ERROR: "Error",
  EXECUTION_ERROR: "ExecutionError",
  FAILED_EXECUTION: "FailedExecution",
} as const;

/**
 * Default error status for new records.
 */
const DEFAULT_ERROR_STATUS: ErrorStatus = "failed";

/**
 * DynamoDB Document Client with marshalling options.
 * Reuses connections via Keep-Alive for better performance.
 */
const dynamoDbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoDbClient, {
  marshallOptions: {
    removeUndefinedValues: true,
  },
});

/**
 * Pads a number with leading zeros for lexicographic sorting.
 * @param count - The number to pad
 * @param length - The total length of the resulting string
 * @returns Zero-padded string representation
 */
const padCount = (count: number, length: number = 10): string => {
  return count.toString().padStart(length, "0");
};

/**
 * Extracts the error type from an error code.
 * The error type is the first 2 characters of the error code.
 * For error codes shorter than 2 characters, returns the code itself.
 * @param errorCode - The error code to extract the type from
 * @returns The error type (first 2 characters or full code if shorter)
 */
const extractErrorType = (errorCode: string): string => {
  return errorCode.length >= 2 ? errorCode.substring(0, 2) : errorCode;
};

/**
 * Creates a composite key for DynamoDB.
 * @param prefix - The key prefix (e.g., "ERROR", "EXECUTION")
 * @param value - The key value
 * @returns Composite key string
 */
const createKey = (prefix: string, value: string): string => {
  return `${prefix}#${value}`;
};

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
 * @param now - ISO timestamp for the operation
 * @returns UpdateCommand input for ErrorRecord upsert
 */
const buildErrorRecordUpdate = (
  errorCode: string,
  errorDetails: Record<string, unknown>,
  executionId: string,
  now: string,
): {
  Update: {
    TableName: string;
    Key: Pick<ErrorRecord, "PK" | "SK">;
    UpdateExpression: string;
    ExpressionAttributeNames: Record<string, string>;
    ExpressionAttributeValues: Record<string, string | number>;
  };
} => {
  const pk = createKey("ERROR", errorCode);
  const errorType = extractErrorType(errorCode);
  // GS3SK format: COUNT#{errorType}#{paddedCount}#ERROR#{errorCode}
  // For new errors, count starts at 1
  const gs3sk = `COUNT#${errorType}#${padCount(1)}#ERROR#${errorCode}`;

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
            GS3PK = :gs3pk,
            GS3SK = :gs3sk
      `.trim(),
      ExpressionAttributeNames: {},
      ExpressionAttributeValues: {
        ":errorCode": errorCode,
        ":entityType": ENTITY_TYPES.ERROR,
        ":errorType": errorType,
        ":errorDetails": JSON.stringify(errorDetails),
        ":defaultStatus": DEFAULT_ERROR_STATUS,
        ":zero": 0,
        ":increment": 1,
        ":now": now,
        ":executionId": executionId,
        ":gs1pk": "TYPE#ERROR",
        ":gs1sk": pk,
        ":gs2pk": "TYPE#ERROR",
        ":gs3pk": "METRIC#ERRORCOUNT",
        ":gs3sk": gs3sk,
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
 * @param now - ISO timestamp for the operation
 * @returns UpdateCommand input for ExecutionErrorLink upsert
 */
const buildExecutionErrorLinkUpdate = (
  errorCode: string,
  executionId: string,
  county: string,
  occurrences: number,
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
  /** Error type for the execution (single type or "MIXED" if multiple types). */
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
            createdAt = if_not_exists(createdAt, :now),
            updatedAt = :now,
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
        ":now": now,
        ":gs3pk": "METRIC#ERRORCOUNT",
        ":gs3sk": gs3sk,
      },
    },
  };
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

  // Determine error type for the execution
  // If all errors share the same type, use that type; otherwise use "MIXED"
  const errorTypes = new Set<string>();
  for (const errorCode of errorOccurrences.keys()) {
    errorTypes.add(extractErrorType(errorCode));
  }
  const executionErrorType =
    errorTypes.size === 1 ? Array.from(errorTypes)[0] : "MIXED";

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
      ExpressionAttributeNames: Record<string, string>;
      ExpressionAttributeValues: Record<string, string | number | undefined>;
    };
  }> = [];

  // Add FailedExecutionItem update (one per execution)
  transactItems.push(buildFailedExecutionItemUpdate(detail, stats, now));

  // Add ErrorRecord and ExecutionErrorLink updates for each unique error
  for (const [errorCode, { count, details }] of errorOccurrences) {
    transactItems.push(
      buildErrorRecordUpdate(errorCode, details, detail.executionId, now),
    );
    transactItems.push(
      buildExecutionErrorLinkUpdate(
        errorCode,
        detail.executionId,
        detail.county,
        count,
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

  return {
    success: true,
    uniqueErrorCount: stats.uniqueErrorCount,
    totalOccurrences: stats.totalOccurrences,
    errorCodes,
  };
};

/**
 * Updates the GS2SK for an ErrorRecord to reflect the new total count.
 * This is a separate operation because GS2SK includes the padded count
 * which can only be determined after the atomic increment.
 *
 * Note: This is called after the main transaction to update the sort key
 * for count-based sorting. In a high-concurrency scenario, this could
 * result in slightly stale sort order, which is acceptable for analytics.
 *
 * @param errorCode - The error code to update
 * @param newCount - The new total count for GS2SK
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
  const gs2sk = `COUNT#${padCount(newCount)}#ERROR#${errorCode}`;

  const command = new UpdateCommand({
    TableName: TABLE_NAME,
    Key: {
      PK: pk,
      SK: pk,
    },
    UpdateExpression: "SET GS2SK = :gs2sk",
    ExpressionAttributeValues: {
      ":gs2sk": gs2sk,
    },
  });

  await docClient.send(command);
};
