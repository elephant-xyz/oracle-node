import { QueryCommand } from "@aws-sdk/lib-dynamodb";
import type { ErrorRecord } from "shared/types.js";
import { TABLE_NAME, docClient } from "shared/dynamodb-client.js";
import { ENTITY_TYPES } from "shared/keys.js";

/**
 * DynamoDB key structure for pagination cursors.
 */
interface DynamoDBKey {
  /** Partition key value. */
  [key: string]: string | number;
}

/**
 * Result from querying errors with pagination info.
 */
interface QueryResult {
  /** Array of error records. */
  items: ErrorRecord[];
  /** Cursor for next page (base64 encoded LastEvaluatedKey). */
  nextCursor?: string;
}

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
  /** Base64 encoded cursor for pagination (from previous page's nextCursor). */
  cursor?: string;
  /** Base64 encoded cursor for navigating to previous page. */
  prevCursor?: string;
}

/** Default number of errors to display. */
const DEFAULT_LIMIT = 20;

/** Environment variable for AWS account ID (injected via SAM template). */
const AWS_ACCOUNT_ID = process.env.AWS_ACCOUNT_ID;

/**
 * Constructs the Lambda function ARN from environment variables.
 * Uses AWS_REGION, AWS_LAMBDA_FUNCTION_NAME (Lambda runtime env vars)
 * and AWS_ACCOUNT_ID (injected via SAM template).
 *
 * @returns Lambda function ARN string, or undefined if env vars are missing
 */
const getLambdaArn = (): string | undefined => {
  const region = process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION;
  const functionName = process.env.AWS_LAMBDA_FUNCTION_NAME;
  const accountId = AWS_ACCOUNT_ID;

  if (!region || !functionName || !accountId) {
    console.warn("missing-env-vars-for-arn", {
      region,
      functionName,
      accountId,
    });
    return undefined;
  }

  return `arn:aws:lambda:${region}:${accountId}:function:${functionName}`;
};

/**
 * Encodes a DynamoDB key to a base64 cursor string.
 *
 * @param key - DynamoDB key object
 * @returns Base64 encoded string
 */
const encodeCursor = (key: DynamoDBKey): string => {
  return Buffer.from(JSON.stringify(key)).toString("base64");
};

/**
 * Decodes a base64 cursor string to a DynamoDB key.
 *
 * @param cursor - Base64 encoded cursor string
 * @returns DynamoDB key object, or undefined if invalid
 */
const decodeCursor = (cursor: string): DynamoDBKey | undefined => {
  try {
    const decoded = Buffer.from(cursor, "base64").toString("utf-8");
    return JSON.parse(decoded) as DynamoDBKey;
  } catch {
    console.warn("invalid-cursor", { cursor });
    return undefined;
  }
};

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
- **Details**: Click to view formatted JSON error details in a popup

### Filtering

Set the \`errorType\` parameter to filter errors by type prefix:
- \`SV\` - SVL errors
- \`MV\` - MVL errors
- Leave empty to show all error types

### Pagination

Use the Previous/Next buttons to navigate between pages.`,
  });
};

/**
 * Queries errors with the most occurrences using GS2 index.
 * GS2PK = "TYPE#ERROR", sorted descending by GS2SK (COUNT#...).
 *
 * @param limit - Maximum number of errors to return
 * @param errorType - Optional error type filter
 * @param cursor - Optional cursor for pagination (base64 encoded ExclusiveStartKey)
 * @returns QueryResult with items and optional nextCursor
 */
const queryErrorsWithMostOccurrences = async (
  limit: number,
  errorType?: string,
  cursor?: string,
): Promise<QueryResult> => {
  if (!TABLE_NAME) {
    throw new Error(
      "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
    );
  }

  const exclusiveStartKey = cursor ? decodeCursor(cursor) : undefined;

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
      ExclusiveStartKey: exclusiveStartKey,
    });

    const response = await docClient.send(command);

    return {
      items: (response.Items ?? []) as ErrorRecord[],
      nextCursor: response.LastEvaluatedKey
        ? encodeCursor(response.LastEvaluatedKey as DynamoDBKey)
        : undefined,
    };
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
    ExclusiveStartKey: exclusiveStartKey,
  });

  const response = await docClient.send(command);

  return {
    items: (response.Items ?? []) as ErrorRecord[],
    nextCursor: response.LastEvaluatedKey
      ? encodeCursor(response.LastEvaluatedKey as DynamoDBKey)
      : undefined,
  };
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
 * Formats error details JSON string into a pretty-printed, HTML-escaped string.
 * If parsing fails, returns the escaped raw string.
 *
 * @param errorDetails - JSON-encoded error details string
 * @returns Formatted and escaped JSON string
 */
const formatErrorDetails = (errorDetails: string): string => {
  try {
    const parsed = JSON.parse(errorDetails) as Record<string, unknown>;
    const formatted = JSON.stringify(parsed, null, 2);
    return escapeHtml(formatted);
  } catch {
    // If parsing fails, return the escaped raw string
    return escapeHtml(errorDetails);
  }
};

/**
 * Generates HTML for the error details popup button.
 * Uses cwdb-action with action="html" and display="popup".
 *
 * @param errorCode - Error code for the popup title
 * @param errorDetails - JSON-encoded error details string
 * @returns HTML string for the details button with popup
 */
const generateDetailsPopup = (
  errorCode: string,
  errorDetails: string,
): string => {
  const formattedDetails = formatErrorDetails(errorDetails);

  return `<a class="btn btn-primary">Details</a>
<cwdb-action action="html" display="popup">
  <h3>Error Details: ${escapeHtml(errorCode)}</h3>
  <pre style="background-color: #f5f5f5; padding: 12px; border-radius: 4px; overflow: auto; max-height: 400px;"><code>${formattedDetails}</code></pre>
</cwdb-action>`;
};

/**
 * Parameters for generating pagination controls.
 */
interface PaginationParams {
  /** Lambda ARN for cwdb-action endpoint. */
  lambdaArn: string;
  /** Current page cursor (undefined for first page). */
  currentCursor?: string;
  /** Cursor for next page. */
  nextCursor?: string;
  /** Error type filter to preserve across pages. */
  errorType?: string;
  /** Limit to preserve across pages. */
  limit: number;
}

/**
 * Generates pagination controls HTML using cwdb-action.
 *
 * @param params - Pagination parameters
 * @returns HTML string for pagination controls
 */
const generatePaginationControls = (params: PaginationParams): string => {
  const { lambdaArn, currentCursor, nextCursor, errorType, limit } = params;

  const hasPrevious = currentCursor !== undefined;
  const hasNext = nextCursor !== undefined;

  // Build JSON params for cwdb-action, ensuring proper escaping
  const buildParams = (cursor?: string, prevCursor?: string): string => {
    const actionParams: Record<string, string | number | undefined> = {
      errorType: errorType ?? "",
      limit,
      cursor,
      prevCursor,
    };
    // Remove undefined values
    Object.keys(actionParams).forEach((key) => {
      if (actionParams[key] === undefined) {
        delete actionParams[key];
      }
    });
    return JSON.stringify(actionParams);
  };

  let controls = "<p>";

  // Previous button - goes back to first page (no cursor)
  if (hasPrevious) {
    controls += `<a class="btn">Previous</a>
<cwdb-action action="call" endpoint="${escapeHtml(lambdaArn)}">
  ${buildParams(undefined, undefined)}
</cwdb-action> `;
  }

  // Next button
  if (hasNext) {
    controls += `<a class="btn btn-primary">Next</a>
<cwdb-action action="call" endpoint="${escapeHtml(lambdaArn)}">
  ${buildParams(nextCursor, currentCursor)}
</cwdb-action>`;
  }

  controls += "</p>";

  return controls;
};

/**
 * Generates HTML table for the widget.
 *
 * @param errors - Array of error records to display
 * @param errorTypeFilter - Current error type filter for display
 * @param currentCursor - Current page cursor
 * @param nextCursor - Cursor for next page
 * @param limit - Number of items per page
 * @returns HTML string
 */
const generateHtml = (
  errors: ErrorRecord[],
  errorTypeFilter?: string,
  currentCursor?: string,
  nextCursor?: string,
  limit: number = DEFAULT_LIMIT,
): string => {
  const lambdaArn = getLambdaArn();
  const filterBadge =
    errorTypeFilter && errorTypeFilter.trim() !== ""
      ? `<p>Filter: <code>${escapeHtml(errorTypeFilter)}</code></p>`
      : "";

  if (errors.length === 0) {
    let html = filterBadge;
    html += `<p>No errors found${errorTypeFilter ? ` for type "${escapeHtml(errorTypeFilter)}"` : ""}.</p>`;

    // Show Previous button if we're on a page beyond the first
    if (lambdaArn && currentCursor) {
      html += generatePaginationControls({
        lambdaArn,
        currentCursor,
        nextCursor: undefined,
        errorType: errorTypeFilter,
        limit,
      });
    }
    return html;
  }

  const rows = errors
    .map(
      (err) => `
      <tr>
        <td title="${escapeHtml(err.errorCode)}"><code>${escapeHtml(err.errorCode)}</code></td>
        <td><code>${escapeHtml(err.errorType)}</code></td>
        <td>${err.totalCount}</td>
        <td>${escapeHtml(err.errorStatus)}</td>
        <td title="${escapeHtml(err.latestExecutionId)}">${escapeHtml(err.latestExecutionId.substring(0, 20))}...</td>
        <td>${formatDate(err.updatedAt)}</td>
        <td>${generateDetailsPopup(err.errorCode, err.errorDetails)}</td>
      </tr>
    `,
    )
    .join("");

  // Build pagination controls if Lambda ARN is available
  const paginationControls = lambdaArn
    ? generatePaginationControls({
        lambdaArn,
        currentCursor,
        nextCursor,
        errorType: errorTypeFilter,
        limit,
      })
    : "";

  return `
    ${filterBadge}
    <table style="width: 100%">
      <thead>
        <tr>
          <th>Error Code</th>
          <th>Type</th>
          <th>Total Count</th>
          <th>Status</th>
          <th>Latest Execution</th>
          <th>Updated At</th>
          <th>Details</th>
        </tr>
      </thead>
      <tbody>
        ${rows}
      </tbody>
    </table>
    ${paginationControls}
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
    const cursor = event.cursor;

    const result = await queryErrorsWithMostOccurrences(
      limit,
      errorType,
      cursor,
    );

    console.info("errors-queried", {
      count: result.items.length,
      limit,
      errorType: errorType ?? "none",
      hasCursor: cursor !== undefined,
      hasNextCursor: result.nextCursor !== undefined,
    });

    return generateHtml(
      result.items,
      errorType,
      cursor,
      result.nextCursor,
      limit,
    );
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
