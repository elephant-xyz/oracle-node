import { QueryCommand } from "@aws-sdk/lib-dynamodb";
import type { FailedExecutionItem } from "shared/types.js";
import { TABLE_NAME, docClient } from "shared/dynamodb-client.js";
import { ENTITY_TYPES } from "shared/keys.js";

/**
 * CloudWatch custom widget event structure.
 */
interface CloudWatchCustomWidgetEvent {
  /** If true, return widget documentation in markdown format. */
  describe?: boolean;
  /** Widget context containing dashboard information. */
  widgetContext?: {
    /** Dashboard name. */
    dashboardName: string;
    /** Widget ID. */
    widgetId: string;
    /** Account ID. */
    accountId: string;
    /** Widget height in pixels. */
    height: number;
    /** Widget width in pixels. */
    width: number;
  };
  /** Custom parameters passed from widget definition. */
  limit?: number;
}

/** Default number of executions to display. */
const DEFAULT_LIMIT = 20;

/**
 * Returns documentation for the widget in markdown format.
 *
 * @returns Markdown documentation string
 */
const getDocumentation = (): string => {
  return JSON.stringify({
    markdown: `## Executions with Most Errors

This widget displays workflow executions sorted by their error count (highest first).

### Parameters

\`\`\`yaml
limit: 20  # Number of executions to display (default: 20)
\`\`\`

### Displayed Columns

- **Execution ID**: Unique identifier of the failed execution
- **County**: County associated with the execution
- **Error Type**: First 2 characters of the error code
- **Open Errors**: Number of unresolved unique errors
- **Total Occurrences**: Total error occurrences observed
- **Created At**: When the execution record was created`,
  });
};

/**
 * Queries executions with the most errors using GS1 index.
 * GS1PK = "METRIC#ERRORCOUNT", sorted descending by GS1SK.
 *
 * @param limit - Maximum number of executions to return
 * @returns Array of FailedExecutionItem records
 */
const queryExecutionsWithMostErrors = async (
  limit: number,
): Promise<FailedExecutionItem[]> => {
  if (!TABLE_NAME) {
    throw new Error(
      "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
    );
  }

  const command = new QueryCommand({
    TableName: TABLE_NAME,
    IndexName: "GS1",
    KeyConditionExpression: "GS1PK = :gs1pk",
    FilterExpression: "entityType = :entityType",
    ExpressionAttributeValues: {
      ":gs1pk": "METRIC#ERRORCOUNT",
      ":entityType": ENTITY_TYPES.FAILED_EXECUTION,
    },
    ScanIndexForward: false, // Descending order (most errors first)
    Limit: limit,
  });

  const response = await docClient.send(command);

  if (!response.Items || response.Items.length === 0) {
    return [];
  }

  return response.Items as FailedExecutionItem[];
};

/**
 * Escapes HTML special characters to prevent XSS.
 *
 * @param str - String to escape
 * @returns Escaped string
 */
const escapeHtml = (str: string): string => {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
};

/**
 * Formats an ISO date string to a human-readable format.
 *
 * @param isoDate - ISO date string
 * @returns Formatted date string
 */
const formatDate = (isoDate: string): string => {
  const date = new Date(isoDate);
  return date.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
};

/**
 * Generates HTML table for the widget.
 *
 * @param executions - Array of execution items to display
 * @returns HTML string
 */
const generateHtml = (executions: FailedExecutionItem[]): string => {
  if (executions.length === 0) {
    return `
      <p>No failed executions found.</p>
    `;
  }

  const rows = executions
    .map(
      (exec) => `
      <tr>
        <td title="${escapeHtml(exec.executionId)}">${escapeHtml(exec.executionId)}</td>
        <td>${escapeHtml(exec.county)}</td>
        <td><code>${escapeHtml(exec.errorType)}</code></td>
        <td>${exec.openErrorCount}</td>
        <td>${exec.totalOccurrences}</td>
        <td>${formatDate(exec.createdAt)}</td>
      </tr>
    `,
    )
    .join("");

  return `
    <style>
      table {
        width: 100%;
      }
    </style>
    <table>
      <thead>
        <tr>
          <th>Execution ID</th>
          <th>County</th>
          <th>Error Type</th>
          <th>Open Errors</th>
          <th>Total Occurrences</th>
          <th>Created At</th>
        </tr>
      </thead>
      <tbody>
        ${rows}
      </tbody>
    </table>
  `;
};

/**
 * Lambda handler for the CloudWatch custom widget.
 * Displays executions with the most errors.
 *
 * @param event - CloudWatch custom widget event
 * @returns HTML content for the widget
 */
export const handler = async (
  event: CloudWatchCustomWidgetEvent,
): Promise<string> => {
  console.info("dashboard-executions-widget-invoked", { event });

  // Handle describe request for documentation
  if (event.describe) {
    return getDocumentation();
  }

  try {
    const limit = event.limit ?? DEFAULT_LIMIT;
    const executions = await queryExecutionsWithMostErrors(limit);

    console.info("executions-queried", {
      count: executions.length,
      limit,
    });

    return generateHtml(executions);
  } catch (error) {
    console.error("widget-error", {
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined,
    });

    return `
      <h3>Error loading data</h3>
      <p>${escapeHtml(error instanceof Error ? error.message : String(error))}</p>
    `;
  }
};
