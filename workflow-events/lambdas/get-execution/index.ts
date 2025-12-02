import * as z from "zod";
import {
  getExecutionWithErrors,
  type GetExecutionInput,
  type GetExecutionResult,
  type SortOrder,
} from "./dynamodb.js";

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
 * Response structure for the Lambda function.
 */
export interface GetExecutionResponse extends GetExecutionResult {
  /** Whether the operation was successful. */
  success: boolean;
  /** Error message if the operation failed. */
  error?: any;
}

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
 * - execution: FailedExecutionItem | null - the found execution or null
 * - errors: ExecutionErrorLink[] - array of errors for the execution
 * - error?: string - error message if operation failed
 *
 * @param event - Direct invocation event with sortOrder and optional errorType
 * @returns Response with execution data or error
 */
export const handler = async (
  event: unknown,
): Promise<GetExecutionResponse> => {
  try {
    console.info("get-execution-handler-invoked", { event });

    // Validate input using Zod
    const parseResult = GetExecutionEventSchema.safeParse(event);
    if (!parseResult.success) {
      const errorMessage = parseResult.error.issues;
      console.warn("input-validation-failed", {
        error: errorMessage,
        issues: parseResult.error.issues,
      });
      return {
        success: false,
        execution: null,
        errors: [],
        error: errorMessage,
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

    return {
      success: true,
      execution: result.execution,
      errors: result.errors,
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
