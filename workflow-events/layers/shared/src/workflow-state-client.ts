/**
 * DynamoDB client configuration for the workflow-state table.
 * Reuses the docClient from dynamodb-client.ts.
 */
export { docClient } from "./dynamodb-client.js";

/**
 * Environment variable for the workflow state table name.
 */
export const WORKFLOW_STATE_TABLE_NAME = process.env.WORKFLOW_STATE_TABLE_NAME;

/**
 * Gets the workflow state table name, throwing an error if not set.
 * @returns The workflow state table name
 * @throws Error if WORKFLOW_STATE_TABLE_NAME is not set
 */
export const getWorkflowStateTableName = (): string => {
  if (!WORKFLOW_STATE_TABLE_NAME) {
    throw new Error(
      "WORKFLOW_STATE_TABLE_NAME environment variable is not set",
    );
  }
  return WORKFLOW_STATE_TABLE_NAME;
};
