import * as z from "zod";
import {
  getExecutionWithErrors,
  type GetExecutionInput,
  type SortOrder,
} from "./dynamodb.js";
import type {
  FailedExecutionItem,
  ExecutionErrorLink,
  ErrorStatus,
} from "shared/types.js";

/**
 * Zod schema for validating the input event.
 * - sortOrder: Required, must be "most" or "least"
 * - errorType: Optional string, must be non-empty if provided
 */
const GetExecutionEventSchema = z.object({
  sortOrder: z.enum(["most", "least"]),
  errorType: z
    .string()
    .min(1, "errorType cannot be an empty string")
    .optional(),
});

/**
 * Input event for direct Lambda invocation.
 */
export type GetExecutionEvent = z.infer<typeof GetExecutionEventSchema>;

/**
 * Business-only execution data without DynamoDB-specific fields.
 */
export interface ExecutionBusinessData {
  /** Identifier of the failed execution. */
  executionId: string;
  /** Execution status bucket (`failed` | `maybeSolved` | `solved`). */
  status: ErrorStatus;
  /** Error type. First 2 characters of the error code, that is common to all errors in the execution. */
  errorType: string;
  /** County identifier. */
  county: string;
  /** Total error occurrences observed. */
  totalOccurrences: number;
  /** Count of unique unresolved errors. */
  openErrorCount: number;
  /** Count of unique errors in the execution. */
  uniqueErrorCount: number;
  /** Task token for Step Functions callback (if applicable). */
  taskToken?: string;
  /** ISO timestamp when the execution record was created. */
  createdAt: string;
  /** ISO timestamp when the execution record was updated. */
  updatedAt: string;
}

/**
 * Business-only error link data without DynamoDB-specific fields.
 */
export interface ErrorBusinessData {
  /** Error code identifier. */
  errorCode: string;
  /** Resolution status for the execution-specific error. */
  status: ErrorStatus;
  /** Occurrence count within the execution. */
  occurrences: number;
  /** Error details JSON-encoded key-value pairs. */
  errorDetails: string;
  /** Identifier of the failed execution. */
  executionId: string;
  /** County identifier. */
  county: string;
  /** ISO timestamp when the link item was created. */
  createdAt: string;
  /** ISO timestamp when the link item was last updated. */
  updatedAt: string;
}

/**
 * Response structure for the Lambda function.
 */
export interface GetExecutionResponse {
  /** Whether the operation was successful. */
  success: boolean;
  /** The failed execution item (business fields only), or null if none found. */
  execution: ExecutionBusinessData | null;
  /** Array of execution error links (business fields only) for the execution. */
  errors: ErrorBusinessData[];
  /** Error message or validation issues if the operation failed. */
  error?: string | z.ZodIssue[];
}

/**
 * Strips DynamoDB-specific fields from FailedExecutionItem, returning only business fields.
 * @param item - The FailedExecutionItem from DynamoDB
 * @returns ExecutionBusinessData with only business fields
 */
const stripExecutionFields = (
  item: FailedExecutionItem,
): ExecutionBusinessData => {
  return {
    executionId: item.executionId,
    status: item.status,
    errorType: item.errorType,
    county: item.county,
    totalOccurrences: item.totalOccurrences,
    openErrorCount: item.openErrorCount,
    uniqueErrorCount: item.uniqueErrorCount,
    taskToken: item.taskToken,
    createdAt: item.createdAt,
    updatedAt: item.updatedAt,
  };
};

/**
 * Strips DynamoDB-specific fields from ExecutionErrorLink, returning only business fields.
 * @param item - The ExecutionErrorLink from DynamoDB
 * @returns ErrorBusinessData with only business fields
 */
const stripErrorFields = (item: ExecutionErrorLink): ErrorBusinessData => {
  return {
    errorCode: item.errorCode,
    status: item.status,
    occurrences: item.occurrences,
    errorDetails: item.errorDetails,
    executionId: item.executionId,
    county: item.county,
    createdAt: item.createdAt,
    updatedAt: item.updatedAt,
  };
};

/**
 * Lambda handler for getting an execution with its errors.
 * Invoked directly (not via API Gateway).
 *
 * Input Event:
 * - sortOrder (required): "most" | "least" - determines whether to return the execution
 *   with the most or least errors
 * - errorType (optional): string - filters executions by error type (first 2 characters
 *   of error code). When provided, uses GS3 index; otherwise uses GS1 index.
 *
 * Response:
 * - success: boolean - whether the operation succeeded
 * - execution: ExecutionBusinessData | null - the found execution (business fields only) or null
 * - errors: ErrorBusinessData[] - array of errors (business fields only) for the execution
 * - error?: string | z.ZodIssue[] - error message if operation failed, or z.ZodIssue[] if validation failed
 *
 * Note: The response excludes DynamoDB-specific fields (PK, SK, GS1PK, GS1SK, GS3PK, GS3SK, entityType).
 *
 * @param event - Direct invocation event with sortOrder and optional errorType
 * @returns Response with execution data (business fields only) or error
 */
export const handler = async (
  event: unknown,
): Promise<GetExecutionResponse> => {
  try {
    console.info("get-execution-handler-invoked", { event });

    // Validate input using Zod
    const parseResult = GetExecutionEventSchema.safeParse(event);
    if (!parseResult.success) {
      console.warn("input-validation-failed", {
        error: parseResult.error.issues,
        issues: parseResult.error.issues,
      });
      return {
        success: false,
        execution: null,
        errors: [],
        error: parseResult.error.issues,
      };
    }

    const { sortOrder, errorType } = parseResult.data;

    console.info("querying-execution", {
      sortOrder,
      errorType: errorType ?? "none",
      gsiUsed: errorType ? "GS3" : "GS1",
    });

    // Build the query input
    const queryInput: GetExecutionInput = {
      sortOrder: sortOrder as SortOrder,
      errorType: errorType?.trim(),
    };

    // Query for execution with errors
    const result = await getExecutionWithErrors(queryInput);

    if (!result.execution) {
      console.info("no-execution-found", { sortOrder, errorType });
      return {
        success: true,
        execution: null,
        errors: [],
      };
    }

    console.info("execution-found", {
      executionId: result.execution.executionId,
      county: result.execution.county,
      uniqueErrorCount: result.execution.uniqueErrorCount,
      totalOccurrences: result.execution.totalOccurrences,
      errorCount: result.errors.length,
    });

    // Strip DynamoDB-specific fields and return only business fields
    return {
      success: true,
      execution: stripExecutionFields(result.execution),
      errors: result.errors.map(stripErrorFields),
    };
  } catch (error) {
    console.error("handler-failed", {
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined,
    });

    return {
      success: false,
      execution: null,
      errors: [],
      error:
        error instanceof Error ? error.message : "An unexpected error occurred",
    };
  }
};
