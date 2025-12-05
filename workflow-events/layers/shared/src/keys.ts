import type { ErrorStatus } from "./types.js";

/**
 * Entity type discriminators for single-table design.
 */
export const ENTITY_TYPES = {
  ERROR: "Error",
  EXECUTION_ERROR: "ExecutionError",
  FAILED_EXECUTION: "FailedExecution",
} as const;

/**
 * Default GSI status for new error records and executions.
 * Used in GSI sort keys to enable filtering by status.
 */
export const DEFAULT_GSI_STATUS = "FAILED" as const;

/**
 * GSI status for errors that failed AI resolution (maybeUnrecoverable).
 * Used in GSI sort keys to filter items marked as unrecoverable.
 */
export const UNRECOVERABLE_GSI_STATUS = "MAYBEUNRECOVERABLE" as const;

/**
 * Converts an ErrorStatus value to uppercase for use in GSI keys.
 * @param status - The error status (e.g., "failed", "maybeSolved", "solved")
 * @returns Uppercase status string for GSI keys (e.g., "FAILED", "MAYBESOLVED", "SOLVED")
 */
export const toGsiStatus = (status: ErrorStatus): string => {
  return status.toUpperCase();
};

/**
 * Pads a number with leading zeros for lexicographic sorting.
 * @param count - The number to pad
 * @param length - The total length of the resulting string
 * @returns Zero-padded string representation
 */
export const padCount = (count: number, length: number = 10): string => {
  return count.toString().padStart(length, "0");
};

/**
 * Extracts the error type from an error code.
 * The error type is the first 2 characters of the error code.
 * For error codes shorter than 2 characters, returns the code itself.
 * @param errorCode - The error code to extract the type from
 * @returns The error type (first 2 characters or full code if shorter)
 */
export const extractErrorType = (errorCode: string): string => {
  return errorCode.length >= 2 ? errorCode.substring(0, 2) : errorCode;
};

/**
 * Creates a composite key for DynamoDB.
 * @param prefix - The key prefix (e.g., "ERROR", "EXECUTION")
 * @param value - The key value
 * @returns Composite key string
 */
export const createKey = (prefix: string, value: string): string => {
  return `${prefix}#${value}`;
};
