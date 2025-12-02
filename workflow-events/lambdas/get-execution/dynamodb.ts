import { QueryCommand } from "@aws-sdk/lib-dynamodb";
import type { FailedExecutionItem, ExecutionErrorLink } from "shared/types.js";
import { TABLE_NAME, docClient } from "shared/dynamodb-client.js";
import { createKey, ENTITY_TYPES } from "shared/keys.js";

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
 * Queries for a failed execution sorted by error count.
 * Uses GS1 when no errorType filter is provided, GS3 when errorType is specified.
 *
 * GSI Strategy:
 * - GS1: GS1PK = "METRIC#ERRORCOUNT", GS1SK = "COUNT#{paddedCount}#EXECUTION#{executionId}"
 * - GS3: GS3PK = "METRIC#ERRORCOUNT" (FailedExecutionItem only; ErrorRecord uses "METRIC#ERRORCOUNT#ERROR"),
 *        GS3SK = "COUNT#{errorType}#{paddedCount}#EXECUTION#{executionId}"
 *
 * @param input - Query parameters including sortOrder and optional errorType
 * @returns The matching FailedExecutionItem or null if none found
 */
export const queryExecutionByErrorCount = async (
  input: GetExecutionInput,
): Promise<FailedExecutionItem | null> => {
  if (!TABLE_NAME) {
    throw new Error(
      "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
    );
  }

  const { sortOrder, errorType } = input;
  // ScanIndexForward: true = ascending (least errors first), false = descending (most errors first)
  const scanIndexForward = sortOrder === "least";

  if (errorType) {
    // Use GS3 with errorType filter using begins_with on GS3SK
    // No FilterExpression needed: ErrorRecord uses GS3PK="METRIC#ERRORCOUNT#ERROR",
    // so only FailedExecutionItem (GS3PK="METRIC#ERRORCOUNT") matches this query
    const command = new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: "GS3",
      KeyConditionExpression:
        "GS3PK = :gs3pk AND begins_with(GS3SK, :gs3skPrefix)",
      ExpressionAttributeValues: {
        ":gs3pk": "METRIC#ERRORCOUNT",
        ":gs3skPrefix": `COUNT#${errorType}#`,
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
    // Use GS1 without errorType filter
    const command = new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: "GS1",
      KeyConditionExpression: "GS1PK = :gs1pk",
      FilterExpression: "entityType = :entityType",
      ExpressionAttributeValues: {
        ":gs1pk": "METRIC#ERRORCOUNT",
        ":entityType": ENTITY_TYPES.FAILED_EXECUTION,
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
 * Queries all ExecutionErrorLink items for a given execution.
 * Uses the main table with PK = "EXECUTION#{executionId}" and SK begins_with "ERROR#".
 *
 * @param executionId - The execution ID to query errors for
 * @returns Array of ExecutionErrorLink items
 */
export const queryExecutionErrors = async (
  executionId: string,
): Promise<ExecutionErrorLink[]> => {
  if (!TABLE_NAME) {
    throw new Error(
      "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
    );
  }

  const executionPk = createKey("EXECUTION", executionId);
  const errorSkPrefix = "ERROR#";

  const errors: ExecutionErrorLink[] = [];
  let lastEvaluatedKey: Record<string, unknown> | undefined;

  do {
    const command = new QueryCommand({
      TableName: TABLE_NAME,
      KeyConditionExpression: "PK = :pk AND begins_with(SK, :skPrefix)",
      FilterExpression: "entityType = :entityType",
      ExpressionAttributeValues: {
        ":pk": executionPk,
        ":skPrefix": errorSkPrefix,
        ":entityType": ENTITY_TYPES.EXECUTION_ERROR,
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

  const errors = await queryExecutionErrors(execution.executionId);

  return {
    execution,
    errors,
  };
};
