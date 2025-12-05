import { QueryCommand } from "@aws-sdk/lib-dynamodb";
import type { FailedExecutionItem } from "shared/types.js";
import { TABLE_NAME, docClient } from "shared/dynamodb-client.js";
import { DEFAULT_GSI_STATUS, UNRECOVERABLE_GSI_STATUS } from "shared/keys.js";

type GsiStatus = typeof DEFAULT_GSI_STATUS | typeof UNRECOVERABLE_GSI_STATUS;

interface CloudWatchCustomWidgetEvent {
  describe?: boolean;
  widgetContext?: {
    dashboardName: string;
    widgetId: string;
    accountId: string;
    height: number;
    width: number;
    params?: {
      limit?: number;
      status?: GsiStatus;
    };
  };
  limit?: number;
  status?: GsiStatus;
}

const DEFAULT_LIMIT = 20;

const getDocumentation = (): string => {
  return JSON.stringify({
    markdown: `## Executions with Most Errors

This widget displays workflow executions sorted by their error count (highest first).

### Parameters

\`\`\`yaml
limit: 20            # Number of executions to display (default: 20)
status: "FAILED"     # Filter by status: "FAILED" or "MAYBEUNRECOVERABLE"
\`\`\`

### Displayed Columns

- **Execution ID**: Unique identifier of the failed execution
- **County**: County associated with the execution
- **Error Type**: First 2 characters of the error code
- **Open Errors**: Number of unresolved unique errors
- **Total Occurrences**: Total error occurrences observed
- **Created At**: When the execution record was created

### Status Filtering

Set the \`status\` parameter to filter executions by resolution status:
- \`FAILED\` - Executions with errors that are still failing (default)
- \`MAYBEUNRECOVERABLE\` - Executions with errors that failed AI resolution`,
  });
};

const queryExecutionsWithMostErrors = async (
  limit: number,
  status: GsiStatus,
): Promise<FailedExecutionItem[]> => {
  if (!TABLE_NAME) {
    throw new Error(
      "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
    );
  }

  const command = new QueryCommand({
    TableName: TABLE_NAME,
    IndexName: "GS1",
    KeyConditionExpression:
      "GS1PK = :gs1pk AND begins_with(GS1SK, :gs1skPrefix)",
    ExpressionAttributeValues: {
      ":gs1pk": "METRIC#ERRORCOUNT",
      ":gs1skPrefix": `COUNT#${status}#`,
    },
    ScanIndexForward: false,
    Limit: limit,
  });

  const response = await docClient.send(command);

  return (response.Items ?? []) as FailedExecutionItem[];
};

const escapeHtml = (str: string): string => {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
};

const formatDate = (isoDate: string): string => {
  const date = new Date(isoDate);
  return date.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
};

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

export const handler = async (
  event: CloudWatchCustomWidgetEvent,
): Promise<string> => {
  console.info("dashboard-executions-widget-invoked", { event });

  if (event.describe) {
    return getDocumentation();
  }

  try {
    const params = event.widgetContext?.params ?? {};
    const limit = event.limit ?? params.limit ?? DEFAULT_LIMIT;
    const status = event.status ?? params.status ?? DEFAULT_GSI_STATUS;
    const executions = await queryExecutionsWithMostErrors(limit, status);

    console.info("executions-queried", {
      count: executions.length,
      limit,
      status,
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
