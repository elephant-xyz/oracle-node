import { QueryCommand } from "@aws-sdk/lib-dynamodb";
import type { ErrorRecord } from "shared/types.js";
import { TABLE_NAME, docClient } from "shared/dynamodb-client.js";
import {
  ENTITY_TYPES,
  DEFAULT_GSI_STATUS,
  UNRECOVERABLE_GSI_STATUS,
  GS3_ERROR_PK,
} from "shared/keys.js";

type GsiStatus = typeof DEFAULT_GSI_STATUS | typeof UNRECOVERABLE_GSI_STATUS;

interface DynamoDBKey {
  [key: string]: string | number;
}

interface QueryResult {
  items: ErrorRecord[];
  nextCursor?: string;
}

interface CloudWatchCustomWidgetEvent {
  describe?: boolean;
  widgetContext?: {
    dashboardName: string;
    widgetId: string;
    accountId: string;
    height: number;
    width: number;
    params?: {
      errorType?: string;
      limit?: number;
      status?: GsiStatus;
    };
  };
  errorType?: string;
  limit?: number;
  status?: GsiStatus;
  cursor?: string;
  prevCursor?: string;
}

const DEFAULT_LIMIT = 20;
const AWS_ACCOUNT_ID = process.env.AWS_ACCOUNT_ID;

const getLambdaArn = (): string | undefined => {
  const region = process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION;
  const functionName = process.env.AWS_LAMBDA_FUNCTION_NAME;
  const accountId = AWS_ACCOUNT_ID;

  if (!region || !functionName || !accountId) {
    return undefined;
  }

  return `arn:aws:lambda:${region}:${accountId}:function:${functionName}`;
};

const encodeCursor = (key: DynamoDBKey): string => {
  return Buffer.from(JSON.stringify(key)).toString("base64");
};

const decodeCursor = (cursor: string): DynamoDBKey | undefined => {
  try {
    const decoded = Buffer.from(cursor, "base64").toString("utf-8");
    return JSON.parse(decoded) as DynamoDBKey;
  } catch {
    return undefined;
  }
};

const getDocumentation = (): string => {
  return JSON.stringify({
    markdown: `## Errors with Most Occurrences

This widget displays error types sorted by their total occurrence count (highest first).

### Parameters

\`\`\`yaml
errorType: ""        # Filter by error type prefix (e.g., "SV", "MV"). Leave empty for all errors.
status: "FAILED"     # Filter by status: "FAILED" or "MAYBEUNRECOVERABLE"
limit: 20            # Number of errors to display (default: 20)
\`\`\`

### Displayed Columns

- **Error Code**: Unique error code identifier
- **Error Type**: First 2 characters of the error code
- **Total Count**: Total occurrences across all executions
- **Status**: Current error status (failed/maybeSolved/solved/maybeUnrecoverable)
- **Latest Execution**: Most recent execution that observed this error
- **Updated At**: When the error record was last updated
- **Details**: Click to view formatted JSON error details in a popup

### Filtering

Set the \`errorType\` parameter to filter errors by type prefix:
- \`SV\` - SVL errors
- \`MV\` - MVL errors
- Leave empty to show all error types

Set the \`status\` parameter to filter errors by resolution status:
- \`FAILED\` - Errors that are still failing (default)
- \`MAYBEUNRECOVERABLE\` - Errors that failed AI resolution

### Pagination

Use the Previous/Next buttons to navigate between pages.`,
  });
};

const queryErrorsWithMostOccurrences = async (
  limit: number,
  status: GsiStatus,
  errorType?: string,
  cursor?: string,
): Promise<QueryResult> => {
  if (!TABLE_NAME) {
    throw new Error(
      "WORKFLOW_ERRORS_TABLE_NAME environment variable is not set",
    );
  }

  const exclusiveStartKey = cursor ? decodeCursor(cursor) : undefined;

  if (errorType && errorType.trim() !== "") {
    // GS3PK partition-level separation ensures only ErrorRecord items are returned
    // (FailedExecutionItem uses GS3PK = "METRIC#ERRORCOUNT")
    const command = new QueryCommand({
      TableName: TABLE_NAME,
      IndexName: "GS3",
      KeyConditionExpression:
        "GS3PK = :gs3pk AND begins_with(GS3SK, :gs3skPrefix)",
      ExpressionAttributeValues: {
        ":gs3pk": GS3_ERROR_PK,
        ":gs3skPrefix": `COUNT#${errorType.trim()}#${status}#`,
      },
      ScanIndexForward: false,
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

  const command = new QueryCommand({
    TableName: TABLE_NAME,
    IndexName: "GS2",
    KeyConditionExpression:
      "GS2PK = :gs2pk AND begins_with(GS2SK, :gs2skPrefix)",
    FilterExpression: "entityType = :entityType",
    ExpressionAttributeValues: {
      ":gs2pk": "TYPE#ERROR",
      ":gs2skPrefix": `COUNT#${status}#`,
      ":entityType": ENTITY_TYPES.ERROR,
    },
    ScanIndexForward: false,
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

const formatErrorDetails = (errorDetails: string): string => {
  try {
    const parsed = JSON.parse(errorDetails) as Record<string, unknown>;
    const formatted = JSON.stringify(parsed, null, 2);
    return escapeHtml(formatted);
  } catch {
    return escapeHtml(errorDetails);
  }
};

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

interface PaginationParams {
  lambdaArn: string;
  currentCursor?: string;
  nextCursor?: string;
  errorType?: string;
  status: GsiStatus;
  limit: number;
}

const generatePaginationControls = (params: PaginationParams): string => {
  const { lambdaArn, currentCursor, nextCursor, errorType, status, limit } =
    params;

  const hasPrevious = currentCursor !== undefined;
  const hasNext = nextCursor !== undefined;

  const buildParams = (cursor?: string, prevCursor?: string): string => {
    const actionParams: Record<string, string | number | undefined> = {
      errorType: errorType ?? "",
      status,
      limit,
      cursor,
      prevCursor,
    };
    Object.keys(actionParams).forEach((key) => {
      if (actionParams[key] === undefined) {
        delete actionParams[key];
      }
    });
    return JSON.stringify(actionParams);
  };

  let controls = "<p>";

  if (hasPrevious) {
    controls += `<a class="btn">Previous</a>
<cwdb-action action="call" endpoint="${escapeHtml(lambdaArn)}">
  ${buildParams(undefined, undefined)}
</cwdb-action> `;
  }

  if (hasNext) {
    controls += `<a class="btn btn-primary">Next</a>
<cwdb-action action="call" endpoint="${escapeHtml(lambdaArn)}">
  ${buildParams(nextCursor, currentCursor)}
</cwdb-action>`;
  }

  controls += "</p>";

  return controls;
};

const generateHtml = (
  errors: ErrorRecord[],
  status: GsiStatus,
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

    if (lambdaArn && currentCursor) {
      html += generatePaginationControls({
        lambdaArn,
        currentCursor,
        nextCursor: undefined,
        errorType: errorTypeFilter,
        status,
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
        <td>${escapeHtml(err.latestExecutionId)}</td>
        <td>${formatDate(err.updatedAt)}</td>
        <td>${generateDetailsPopup(err.errorCode, err.errorDetails)}</td>
      </tr>
    `,
    )
    .join("");

  const paginationControls = lambdaArn
    ? generatePaginationControls({
        lambdaArn,
        currentCursor,
        nextCursor,
        errorType: errorTypeFilter,
        status,
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

export const handler = async (
  event: CloudWatchCustomWidgetEvent,
): Promise<string> => {
  console.info("dashboard-errors-widget-invoked", { event });

  if (event.describe) {
    return getDocumentation();
  }

  try {
    const params = event.widgetContext?.params ?? {};
    const errorType = event.errorType ?? params.errorType;
    const limit = event.limit ?? params.limit ?? DEFAULT_LIMIT;
    const status = event.status ?? params.status ?? DEFAULT_GSI_STATUS;
    const cursor = event.cursor;

    const result = await queryErrorsWithMostOccurrences(
      limit,
      status,
      errorType,
      cursor,
    );

    console.info("errors-queried", {
      count: result.items.length,
      limit,
      status,
      errorType: errorType ?? "none",
      hasCursor: cursor !== undefined,
      hasNextCursor: result.nextCursor !== undefined,
    });

    return generateHtml(
      result.items,
      status,
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
