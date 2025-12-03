import { QueryCommand } from "@aws-sdk/lib-dynamodb";
import type { ErrorRecord } from "shared/types.js";
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
    /** Custom parameters from widget definition. */
    params?: {
      /** Optional error type filter (first 2 characters of error code). */
      errorType?: string;
      /** Number of errors to display. */
      limit?: number;
    };
  };
  /** Optional error type filter (first 2 characters of error code). */
  errorType?: string;
  /** Number of errors to display. */
  limit?: number;
}

/** Default number of errors to display. */
const DEFAULT_LIMIT = 20;

/**
 * Returns documentation for the widget in markdown format.
 *
 * @returns Markdown documentation string
 */
const getDocumentation = (): string => {
  return JSON.stringify({
    markdown: `## Errors with Most Occurrences

This widget displays error types sorted by their total occurrence count (highest first).

### Parameters

\`\`\`yaml
errorType: ""  # Filter by error type prefix (e.g., "SV", "MV"). Leave empty for all errors.
limit: 20      # Number of errors to display (default: 20)
\`\`\`

### Displayed Columns

- **Error Code**: Unique error code identifier
- **Error Type**: First 2 characters of the error code
- **Total Count**: Total occurrences across all executions
- **Status**: Current error status (failed/maybeSolved/solved)
- **Latest Execution**: Most recent execution that observed this error
- **Updated At**: When the error record was last updated

### Filtering

Set the \`errorType\` parameter to filter errors by type prefix:
- \`SV\` - SVL errors
- \`MV\` - MVL errors
- Leave empty to show all error types`,
  });
};

/**
 * Queries errors with the most occurrences using GS2 index.
 * GS2PK = "TYPE#ERROR", sorted descending by GS2SK (COUNT#...).
 *
 * @param limit - Maximum number of errors to return
 * @param errorType - Optional error type filter
 * @returns Array of ErrorRecord records
 */
const queryErrorsWithMostOccurrences = async (
  limit: number,
  errorType?: string,
): Promise<ErrorRecord[]> => {
  if (!TABLE_NAME) {
    throw new Error(
      "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
    );
  }

  // When filtering by errorType, use GS3 with begins_with on GS3SK
  if (errorType && errorType.trim() !== "") {
    const command = new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: "GS3",
      KeyConditionExpression:
        "GS3PK = :gs3pk AND begins_with(GS3SK, :gs3skPrefix)",
      FilterExpression: "entityType = :entityType",
      ExpressionAttributeValues: {
        ":gs3pk": "METRIC#ERRORCOUNT",
        ":gs3skPrefix": `COUNT#${errorType.trim()}#`,
        ":entityType": ENTITY_TYPES.ERROR,
      },
      ScanIndexForward: false, // Descending order (most occurrences first)
      Limit: limit,
    });

    const response = await docClient.send(command);

    if (!response.Items || response.Items.length === 0) {
      return [];
    }

    return response.Items as ErrorRecord[];
  }

  // Without errorType filter, use GS2
  const command = new QueryCommand({
    TableName: TABLE_NAME,
    IndexName: "GS2",
    KeyConditionExpression: "GS2PK = :gs2pk",
    FilterExpression: "entityType = :entityType",
    ExpressionAttributeValues: {
      ":gs2pk": "TYPE#ERROR",
      ":entityType": ENTITY_TYPES.ERROR,
    },
    ScanIndexForward: false, // Descending order (most occurrences first)
    Limit: limit,
  });

  const response = await docClient.send(command);

  if (!response.Items || response.Items.length === 0) {
    return [];
  }

  return response.Items as ErrorRecord[];
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
 * Gets the color for an error status badge.
 *
 * @param status - Error status
 * @returns CSS color string
 */
const getStatusColor = (status: string): string => {
  switch (status) {
    case "solved":
      return "#4caf50";
    case "maybeSolved":
      return "#ff9800";
    case "failed":
    default:
      return "#d32f2f";
  }
};

/**
 * Generates HTML table for the widget.
 *
 * @param errors - Array of error records to display
 * @param errorTypeFilter - Current error type filter for display
 * @returns HTML string
 */
const generateHtml = (
  errors: ErrorRecord[],
  errorTypeFilter?: string,
): string => {
  const filterBadge =
    errorTypeFilter && errorTypeFilter.trim() !== ""
      ? `<span style="background: #e3f2fd; color: #1565c0; padding: 4px 8px; border-radius: 4px; font-size: 12px; margin-left: 10px;">Filter: ${escapeHtml(errorTypeFilter)}</span>`
      : "";

  if (errors.length === 0) {
    return `
      <div style="padding: 20px; text-align: center; color: #888;">
        ${filterBadge ? `<div style="margin-bottom: 10px;">${filterBadge}</div>` : ""}
        <p>No errors found${errorTypeFilter ? ` for type "${escapeHtml(errorTypeFilter)}"` : ""}.</p>
      </div>
    `;
  }

  const rows = errors
    .map(
      (err) => `
      <tr>
        <td title="${escapeHtml(err.errorCode)}" style="font-family: monospace; font-size: 12px;">${escapeHtml(err.errorCode)}</td>
        <td><span style="background: #f0f0f0; padding: 2px 6px; border-radius: 3px; font-family: monospace;">${escapeHtml(err.errorType)}</span></td>
        <td style="text-align: right; font-weight: bold; color: ${err.totalCount > 10 ? "#d32f2f" : "#1976d2"};">${err.totalCount}</td>
        <td><span style="background: ${getStatusColor(err.errorStatus)}; color: white; padding: 2px 6px; border-radius: 3px; font-size: 11px;">${escapeHtml(err.errorStatus)}</span></td>
        <td title="${escapeHtml(err.latestExecutionId)}" style="font-size: 11px; color: #666;">${escapeHtml(err.latestExecutionId.substring(0, 20))}...</td>
        <td style="color: #666; font-size: 0.9em;">${formatDate(err.updatedAt)}</td>
      </tr>
    `,
    )
    .join("");

  return `
    <style>
      .errors-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 13px;
      }
      .errors-table th {
        background: #f5f5f5;
        padding: 8px 12px;
        text-align: left;
        font-weight: 600;
        border-bottom: 2px solid #ddd;
      }
      .errors-table td {
        padding: 8px 12px;
        border-bottom: 1px solid #eee;
        max-width: 200px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }
      .errors-table tr:hover {
        background: #fafafa;
      }
      .filter-info {
        margin-bottom: 10px;
        padding: 8px;
        background: #f9f9f9;
        border-radius: 4px;
      }
    </style>
    ${filterBadge ? `<div class="filter-info">${filterBadge}</div>` : ""}
    <table class="errors-table">
      <thead>
        <tr>
          <th>Error Code</th>
          <th>Type</th>
          <th style="text-align: right;">Total Count</th>
          <th>Status</th>
          <th>Latest Execution</th>
          <th>Updated At</th>
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
 * Displays errors with the most occurrences.
 *
 * @param event - CloudWatch custom widget event
 * @returns HTML content for the widget
 */
export const handler = async (
  event: CloudWatchCustomWidgetEvent,
): Promise<string> => {
  console.info("dashboard-errors-widget-invoked", { event });

  // Handle describe request for documentation
  if (event.describe) {
    return getDocumentation();
  }

  try {
    // Get parameters from event or widget context params
    const params = event.widgetContext?.params ?? {};
    const errorType = event.errorType ?? params.errorType;
    const limit = event.limit ?? params.limit ?? DEFAULT_LIMIT;

    const errors = await queryErrorsWithMostOccurrences(limit, errorType);

    console.info("errors-queried", {
      count: errors.length,
      limit,
      errorType: errorType ?? "none",
    });

    return generateHtml(errors, errorType);
  } catch (error) {
    console.error("widget-error", {
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined,
    });

    return `
      <div style="padding: 20px; color: #d32f2f;">
        <strong>Error loading data:</strong>
        <p>${escapeHtml(error instanceof Error ? error.message : String(error))}</p>
      </div>
    `;
  }
};
