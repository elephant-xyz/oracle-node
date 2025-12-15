import {
  CloudFormationClient,
  DescribeStacksCommand,
} from "@aws-sdk/client-cloudformation";
import {
  SQSClient,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  ListQueuesCommand,
} from "@aws-sdk/client-sqs";
import { STSClient, GetCallerIdentityCommand } from "@aws-sdk/client-sts";
import {
  SecretsManagerClient,
  GetSecretValueCommand,
} from "@aws-sdk/client-secrets-manager";
import {
  CloudWatchClient,
  GetMetricStatisticsCommand,
  ListMetricsCommand,
} from "@aws-sdk/client-cloudwatch";
import { google } from "googleapis";

const cloudFormationClient = new CloudFormationClient({});
const sqsClient = new SQSClient({});
const stsClient = new STSClient({});
const secretsClient = new SecretsManagerClient({});
const cloudWatchClient = new CloudWatchClient({});

/**
 * @typedef {Object} QueueCountResult
 * @property {string} queueName - Display name of the queue
 * @property {string} queueUrl - Queue URL
 * @property {number} messageCount - Number of messages
 * @property {string} [error] - Error message if failed
 */

/**
 * @typedef {Object} CounterOutput
 * @property {string} accountId - AWS account ID
 * @property {string} stackName - CloudFormation stack name
 * @property {string} region - AWS region
 * @property {QueueCountResult[]} queues - Queue count results
 * @property {number} totalMessages - Total messages across all queues
 * @property {string} timestamp - ISO timestamp
 */

/**
 * Get AWS account ID from STS
 * @returns {Promise<string>}
 */
async function getAccountId() {
  const response = await stsClient.send(new GetCallerIdentityCommand({}));
  return response.Account || "";
}

/**
 * Get queue URL from CloudFormation output or by name pattern
 * @param {string} stackName - CloudFormation stack name
 * @param {string} outputKey - CloudFormation output key
 * @param {string} queueNamePattern - Queue name pattern for fallback
 * @param {string} accountId - AWS account ID
 * @param {string} region - AWS region
 * @returns {Promise<string>}
 */
async function getQueueUrl(
  stackName,
  outputKey,
  queueNamePattern,
  accountId,
  region,
) {
  try {
    // Try CloudFormation output first
    const describeResponse = await cloudFormationClient.send(
      new DescribeStacksCommand({ StackName: stackName }),
    );

    const stack = describeResponse.Stacks?.[0];
    if (stack?.Outputs) {
      const output = stack.Outputs.find((o) => o.OutputKey === outputKey);
      if (output?.OutputValue) {
        return output.OutputValue;
      }
    }

    // Fallback: search by name pattern
    try {
      const queueUrlResponse = await sqsClient.send(
        new GetQueueUrlCommand({ QueueName: queueNamePattern }),
      );
      if (queueUrlResponse.QueueUrl) {
        return queueUrlResponse.QueueUrl;
      }
    } catch (err) {
      // Queue not found by name, continue to manual construction
    }

    // Last fallback: construct URL manually
    return `https://sqs.${region}.amazonaws.com/${accountId}/${queueNamePattern}`;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Failed to get queue URL for ${outputKey}: ${errorMessage}`,
    );
  }
}

/**
 * Get message count from queue
 * @param {string} queueUrl - Queue URL
 * @param {string} queueName - Display name for logging
 * @returns {Promise<number>}
 */
async function getQueueMessageCount(queueUrl, queueName) {
  try {
    const response = await sqsClient.send(
      new GetQueueAttributesCommand({
        QueueUrl: queueUrl,
        AttributeNames: ["ApproximateNumberOfMessages"],
      }),
    );

    const count = parseInt(
      response.Attributes?.ApproximateNumberOfMessages || "0",
      10,
    );
    return count;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(
      JSON.stringify({
        level: "error",
        msg: "failed_to_get_queue_count",
        queueName: queueName,
        queueUrl: queueUrl,
        error: errorMessage,
      }),
    );
    throw error;
  }
}

/**
 * Get message count from queue (including in-flight messages)
 * @param {string} queueUrl - Queue URL
 * @returns {Promise<{messagesAvailable: number, messagesInFlight: number}>}
 */
async function getQueueMessageCounts(queueUrl) {
  try {
    const response = await sqsClient.send(
      new GetQueueAttributesCommand({
        QueueUrl: queueUrl,
        AttributeNames: [
          "ApproximateNumberOfMessages",
          "ApproximateNumberOfMessagesNotVisible",
        ],
      }),
    );

    const attributes = response.Attributes || {};
    return {
      messagesAvailable: parseInt(
        attributes.ApproximateNumberOfMessages || "0",
        10,
      ),
      messagesInFlight: parseInt(
        attributes.ApproximateNumberOfMessagesNotVisible || "0",
        10,
      ),
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(
      JSON.stringify({
        level: "error",
        msg: "failed_to_get_queue_counts",
        queueUrl: queueUrl,
        error: errorMessage,
      }),
    );
    throw error;
  }
}

/**
 * List all queues with a given prefix
 * @param {string} prefix - Queue name prefix
 * @returns {Promise<string[]>} - Array of queue URLs
 */
async function listQueuesWithPrefix(prefix) {
  const queueUrls = [];
  let nextToken;

  do {
    const command = new ListQueuesCommand({
      QueueNamePrefix: prefix,
      NextToken: nextToken,
    });

    const response = await sqsClient.send(command);
    if (response.QueueUrls) {
      queueUrls.push(...response.QueueUrls);
    }
    nextToken = response.NextToken;
  } while (nextToken);

  return queueUrls;
}

/**
 * Count total messages from all workflow queues (elephant-workflow-queue prefix)
 * @returns {Promise<number>} - Total message count across all workflow queues
 */
async function countWorkflowQueueMessages() {
  try {
    const queueUrls = await listQueuesWithPrefix("elephant-workflow-queue");
    let totalMessages = 0;

    for (const queueUrl of queueUrls) {
      try {
        const counts = await getQueueMessageCounts(queueUrl);
        totalMessages += counts.messagesAvailable + counts.messagesInFlight;
      } catch (error) {
        // Log error but continue with other queues
        console.error(
          JSON.stringify({
            level: "error",
            msg: "failed_to_count_workflow_queue",
            queueUrl: queueUrl,
            error: error instanceof Error ? error.message : String(error),
          }),
        );
      }
    }

    console.log(
      JSON.stringify({
        level: "info",
        msg: "workflow_queues_counted",
        queueCount: queueUrls.length,
        totalMessages: totalMessages,
      }),
    );

    return totalMessages;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(
      JSON.stringify({
        level: "error",
        msg: "failed_to_count_workflow_queues",
        error: errorMessage,
      }),
    );
    return 0;
  }
}

/**
 * Get Google Sheets configuration from AWS Secrets Manager
 * @param {string} stackName - CloudFormation stack name
 * @returns {Promise<{sheetId: string, tabName: string, credentials: object} | null>}
 */
async function getGoogleSheetsConfig(stackName) {
  // Secret name patterns (tried in order)
  const secretNames = [
    `${stackName}/google-sheets/config`,
    "spreadsheet-api/google-credentials",
  ];

  for (const secretName of secretNames) {
    try {
      const response = await secretsClient.send(
        new GetSecretValueCommand({
          SecretId: secretName,
        }),
      );

      if (!response.SecretString) {
        continue;
      }

      // Parse the secret JSON
      let secretData;
      try {
        secretData = JSON.parse(response.SecretString);
      } catch (parseError) {
        console.error(
          JSON.stringify({
            level: "error",
            msg: "failed_to_parse_google_sheets_secret",
            secretName: secretName,
            error:
              parseError instanceof Error
                ? parseError.message
                : String(parseError),
          }),
        );
        continue;
      }

      // Extract config (support both formats)
      const sheetId =
        secretData.sheetId || secretData.sheet_id || secretData.SHEET_ID;
      const tabName =
        secretData.tabName || secretData.tab_name || secretData.TAB_NAME;
      const credentials =
        secretData.credentials || secretData.credential || secretData;

      // If credentials is the whole object (old format), extract it
      if (!credentials || typeof credentials !== "object") {
        // Try to use the whole secretData as credentials (backward compatibility)
        if (secretData.type === "service_account") {
          return {
            sheetId: sheetId,
            tabName: tabName,
            credentials: secretData,
          };
        }
        continue;
      }

      if (!sheetId || !tabName || !credentials) {
        continue;
      }

      return {
        sheetId: sheetId,
        tabName: tabName,
        credentials: credentials,
      };
    } catch (error) {
      // Secret doesn't exist or other error, try next name
      if (
        error.name === "ResourceNotFoundException" ||
        error.name === "InvalidParameterException"
      ) {
        continue;
      }
      // For other errors, log and continue
      console.error(
        JSON.stringify({
          level: "warn",
          msg: "failed_to_get_google_sheets_secret",
          secretName: secretName,
          error: error instanceof Error ? error.message : String(error),
        }),
      );
    }
  }

  // No secret found
  return null;
}

/**
 * Initialize Google Sheets API client
 * @param {object} credentials - Google service account credentials
 * @returns {google.sheets.v4.Sheets}
 */
function initGoogleSheets(credentials) {
  const auth = new google.auth.GoogleAuth({
    credentials: credentials,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });

  return google.sheets({ version: "v4", auth });
}

/**
 * Find or create date column in Google Sheet
 * @param {google.sheets.v4.Sheets} sheets - Google Sheets API client
 * @param {string} sheetId - Google Sheet ID
 * @param {string} tabName - Sheet tab name
 * @param {string} dateColumn - Date string for column header (e.g., "2025-12-10")
 * @param {string} suffix - Column suffix ("ready to be minted" or "waiting for mining")
 * @returns {Promise<number>} - Column index (1-based)
 */
async function findOrCreateDateColumn(
  sheets,
  sheetId,
  tabName,
  dateColumn,
  suffix = "ready to be minted",
) {
  try {
    // Get the sheet metadata to find the tab
    const spreadsheet = await sheets.spreadsheets.get({
      spreadsheetId: sheetId,
    });

    const sheet = spreadsheet.data.sheets?.find(
      (s) => s.properties?.title === tabName,
    );

    if (!sheet) {
      throw new Error(`Sheet tab "${tabName}" not found`);
    }

    const sheetId_prop = sheet.properties.sheetId;

    // Get the first row to find column headers
    const range = `${tabName}!1:1`;
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetId,
      range: range,
    });

    const headerRow = response.data.values?.[0] || [];

    // Look for column with exact name "YYYY-MM-DD (suffix)"
    const dateColumnHeader = `${dateColumn} (${suffix})`;
    const dateColumnIndex = headerRow.findIndex((header) => {
      const headerStr = String(header || "").trim();
      // Check for exact match with the full format
      return headerStr === dateColumnHeader;
    });

    if (dateColumnIndex !== -1) {
      // Column exists, return 1-based index
      return dateColumnIndex + 1;
    }

    // Column doesn't exist, find where to insert it
    // Look for the rightmost column that starts with the same date (e.g., "2025-12-11")
    let insertColumnIndex = 4; // Default: after Account ID column (column C)

    // Find the rightmost column with the same date prefix
    for (let i = headerRow.length - 1; i >= 0; i--) {
      const headerStr = String(headerRow[i] || "").trim();
      if (
        headerStr.startsWith(`${dateColumn} `) ||
        headerStr.startsWith(`${dateColumn}(`)
      ) {
        // Found a column with the same date, insert right after it
        insertColumnIndex = i + 2; // i is 0-based, so +1 for 1-based, +1 more to insert after
        break;
      }
    }

    // If no date columns found, insert after Account ID (column C = index 3, so column D = index 4)

    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: sheetId,
      requestBody: {
        requests: [
          {
            insertDimension: {
              range: {
                sheetId: sheetId_prop,
                dimension: "COLUMNS",
                startIndex: insertColumnIndex - 1, // 0-based
                endIndex: insertColumnIndex,
              },
              inheritFromBefore: false,
            },
          },
        ],
      },
    });

    // Update the header cell with the date and suffix
    await sheets.spreadsheets.values.update({
      spreadsheetId: sheetId,
      range: `${tabName}!${String.fromCharCode(64 + insertColumnIndex)}1`,
      valueInputOption: "RAW",
      requestBody: {
        values: [[dateColumnHeader]],
      },
    });

    console.log(
      JSON.stringify({
        level: "info",
        msg: "created_date_column",
        dateColumn: dateColumnHeader,
        columnIndex: insertColumnIndex,
      }),
    );

    return insertColumnIndex;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to find or create date column: ${errorMessage}`);
  }
}

/**
 * Find account ID row in Google Sheet
 * @param {google.sheets.v4.Sheets} sheets - Google Sheets API client
 * @param {string} sheetId - Google Sheet ID
 * @param {string} tabName - Sheet tab name
 * @param {string} accountId - Account ID to find
 * @returns {Promise<number>} - Row index (1-based), or -1 if not found
 */
async function findAccountIdRow(sheets, sheetId, tabName, accountId) {
  try {
    // Get all values in the Account ID column (column C)
    const range = `${tabName}!C:C`;
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetId,
      range: range,
    });

    const values = response.data.values || [];
    for (let i = 0; i < values.length; i++) {
      const cellValue = String(values[i]?.[0] || "").trim();
      if (cellValue === accountId) {
        return i + 1; // 1-based row index
      }
    }

    return -1; // Not found
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to find account ID row: ${errorMessage}`);
  }
}

/**
 * Get metric statistics from CloudWatch for a specific metric
 * Since metrics have multiple dimensions (County, Status, Step, DataGroupName),
 * we need to list all dimension combinations and query each with 30-day period
 * @param {string} metricName - Metric name (e.g., "PrepareElephantPhase")
 * @param {string} status - Status dimension value (e.g., "IN_PROGRESS", "SUCCEEDED", "FAILED")
 * @param {Date} startTime - Start time for the query
 * @param {Date} endTime - End time for the query
 * @returns {Promise<number>} - Sum of all metric values
 */
async function getMetricSum(metricName, status, startTime, endTime) {
  try {
    // Use 30-day period (2592000 seconds) to match dashboard
    const period = 2592000; // 30 days in seconds

    // First, list all metrics with this metricName and Status
    const listCommand = new ListMetricsCommand({
      Namespace: "Elephant/Workflow",
      MetricName: metricName,
      Dimensions: [{ Name: "Status", Value: status }],
    });

    const listResponse = await cloudWatchClient.send(listCommand);
    const metrics = listResponse.Metrics || [];

    console.log(
      JSON.stringify({
        level: "info",
        msg: "listed_metrics",
        metricName: metricName,
        status: status,
        metricCount: metrics.length,
      }),
    );

    if (metrics.length === 0) {
      console.log(
        JSON.stringify({
          level: "info",
          msg: "no_metrics_found",
          metricName: metricName,
          status: status,
        }),
      );
      return 0;
    }

    // Query each metric dimension combination with 30-day period
    let total = 0;
    const queryPromises = metrics.map(async (metric) => {
      try {
        const statsCommand = new GetMetricStatisticsCommand({
          Namespace: "Elephant/Workflow",
          MetricName: metricName,
          Dimensions: metric.Dimensions || [],
          StartTime: startTime,
          EndTime: endTime,
          Period: period,
          Statistics: ["Sum"],
        });

        const statsResponse = await cloudWatchClient.send(statsCommand);
        const sum =
          statsResponse.Datapoints?.reduce((s, dp) => s + (dp.Sum || 0), 0) ||
          0;
        return sum;
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        console.warn(
          JSON.stringify({
            level: "warn",
            msg: "failed_to_query_metric_dimension",
            metricName: metricName,
            dimensions: metric.Dimensions,
            error: errorMessage,
          }),
        );
        return 0;
      }
    });

    const results = await Promise.all(queryPromises);
    total = results.reduce((sum, val) => sum + val, 0);

    console.log(
      JSON.stringify({
        level: "info",
        msg: "metric_sum_calculated",
        metricName: metricName,
        status: status,
        total: total,
        dimensionCombinations: metrics.length,
        period: period,
      }),
    );

    return Math.round(total);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(
      JSON.stringify({
        level: "error",
        msg: "failed_to_get_metric",
        metricName: metricName,
        status: status,
        error: errorMessage,
        stack: error instanceof Error ? error.stack : undefined,
      }),
    );
    return 0;
  }
}

/**
 * Calculate "in progress" count from CloudWatch metrics for the last 30 days
 * @returns {Promise<number>} - Total in progress count
 */
async function calculateInProgressCount() {
  const endTime = new Date();
  const startTime = new Date();
  startTime.setDate(startTime.getDate() - 30); // Last 30 days

  console.log(
    JSON.stringify({
      level: "info",
      msg: "calculating_in_progress_count",
      startTime: startTime.toISOString(),
      endTime: endTime.toISOString(),
    }),
  );

  let totalInProgress = 0;

  // Phases to include: Prepare, Transform, SVL, MVL, Upload, Hash
  // Exclude: AutoRepair, GasPriceCheck, Submit, TransactionStatusCheck

  // Prepare: IN_PROGRESS - SUCCEEDED - FAILED (sum over 30 days)
  const prepareInProgress = await getMetricSum(
    "PrepareElephantPhase",
    "IN_PROGRESS",
    startTime,
    endTime,
  );
  const prepareSucceeded = await getMetricSum(
    "PrepareElephantPhase",
    "SUCCEEDED",
    startTime,
    endTime,
  );
  const prepareFailed = await getMetricSum(
    "PrepareElephantPhase",
    "FAILED",
    startTime,
    endTime,
  );
  const prepareCount = Math.max(
    0,
    prepareInProgress - prepareSucceeded - prepareFailed,
  );
  totalInProgress += prepareCount;

  console.log(
    JSON.stringify({
      level: "info",
      msg: "prepare_metrics",
      inProgress: prepareInProgress,
      succeeded: prepareSucceeded,
      failed: prepareFailed,
      calculated: prepareCount,
    }),
  );

  // Transform: IN_PROGRESS - SUCCEEDED - FAILED (sum over 30 days)
  const transformInProgress = await getMetricSum(
    "TransformElephantPhase",
    "IN_PROGRESS",
    startTime,
    endTime,
  );
  const transformSucceeded = await getMetricSum(
    "TransformElephantPhase",
    "SUCCEEDED",
    startTime,
    endTime,
  );
  const transformFailed = await getMetricSum(
    "TransformElephantPhase",
    "FAILED",
    startTime,
    endTime,
  );
  const transformCount = Math.max(
    0,
    transformInProgress - transformSucceeded - transformFailed,
  );
  totalInProgress += transformCount;

  console.log(
    JSON.stringify({
      level: "info",
      msg: "transform_metrics",
      inProgress: transformInProgress,
      succeeded: transformSucceeded,
      failed: transformFailed,
      calculated: transformCount,
    }),
  );

  // SVL: IN_PROGRESS - SUCCEEDED (no FAILED deduction)
  const svlInProgress = await getMetricSum(
    "SVLElephantPhase",
    "IN_PROGRESS",
    startTime,
    endTime,
  );
  const svlSucceeded = await getMetricSum(
    "SVLElephantPhase",
    "SUCCEEDED",
    startTime,
    endTime,
  );
  const svlCount = Math.max(0, svlInProgress - svlSucceeded);
  totalInProgress += svlCount;

  console.log(
    JSON.stringify({
      level: "info",
      msg: "svl_metrics",
      inProgress: svlInProgress,
      succeeded: svlSucceeded,
      calculated: svlCount,
    }),
  );

  // MVL: IN_PROGRESS - SUCCEEDED (no FAILED deduction)
  const mvlInProgress = await getMetricSum(
    "MVLElephantPhase",
    "IN_PROGRESS",
    startTime,
    endTime,
  );
  const mvlSucceeded = await getMetricSum(
    "MVLElephantPhase",
    "SUCCEEDED",
    startTime,
    endTime,
  );
  const mvlCount = Math.max(0, mvlInProgress - mvlSucceeded);
  totalInProgress += mvlCount;

  console.log(
    JSON.stringify({
      level: "info",
      msg: "mvl_metrics",
      inProgress: mvlInProgress,
      succeeded: mvlSucceeded,
      calculated: mvlCount,
    }),
  );

  // Upload: IN_PROGRESS - SUCCEEDED - FAILED (sum over 30 days)
  const uploadInProgress = await getMetricSum(
    "UploadElephantPhase",
    "IN_PROGRESS",
    startTime,
    endTime,
  );
  const uploadSucceeded = await getMetricSum(
    "UploadElephantPhase",
    "SUCCEEDED",
    startTime,
    endTime,
  );
  const uploadFailed = await getMetricSum(
    "UploadElephantPhase",
    "FAILED",
    startTime,
    endTime,
  );
  const uploadCount = Math.max(
    0,
    uploadInProgress - uploadSucceeded - uploadFailed,
  );
  totalInProgress += uploadCount;

  console.log(
    JSON.stringify({
      level: "info",
      msg: "upload_metrics",
      inProgress: uploadInProgress,
      succeeded: uploadSucceeded,
      failed: uploadFailed,
      calculated: uploadCount,
    }),
  );

  // Hash: IN_PROGRESS - SUCCEEDED - FAILED (sum over 30 days)
  const hashInProgress = await getMetricSum(
    "HashElephantPhase",
    "IN_PROGRESS",
    startTime,
    endTime,
  );
  const hashSucceeded = await getMetricSum(
    "HashElephantPhase",
    "SUCCEEDED",
    startTime,
    endTime,
  );
  const hashFailed = await getMetricSum(
    "HashElephantPhase",
    "FAILED",
    startTime,
    endTime,
  );
  const hashCount = Math.max(0, hashInProgress - hashSucceeded - hashFailed);
  totalInProgress += hashCount;

  console.log(
    JSON.stringify({
      level: "info",
      msg: "hash_metrics",
      inProgress: hashInProgress,
      succeeded: hashSucceeded,
      failed: hashFailed,
      calculated: hashCount,
    }),
  );

  console.log(
    JSON.stringify({
      level: "info",
      msg: "total_in_progress_count",
      totalInProgress: totalInProgress,
      breakdown: {
        prepare: prepareCount,
        transform: transformCount,
        svl: svlCount,
        mvl: mvlCount,
        upload: uploadCount,
        hash: hashCount,
      },
    }),
  );

  return totalInProgress;
}

/**
 * Calculate "ready to be minted" count from CloudWatch metrics (FAILED counts)
 * Sums up FAILED metrics for Prepare, Transform, Upload, Hash, and AutoRepair phases
 * @returns {Promise<number>} - Total failed count
 */
async function calculateReadyToMintFailedCount() {
  const endTime = new Date();
  const startTime = new Date();
  startTime.setDate(startTime.getDate() - 30); // Last 30 days

  console.log(
    JSON.stringify({
      level: "info",
      msg: "calculating_ready_to_mint_failed_count",
      startTime: startTime.toISOString(),
      endTime: endTime.toISOString(),
    }),
  );

  // Sum FAILED metrics for: Prepare, Transform, Upload, Hash, AutoRepair
  const prepareFailed = await getMetricSum(
    "PrepareElephantPhase",
    "FAILED",
    startTime,
    endTime,
  );
  const transformFailed = await getMetricSum(
    "TransformElephantPhase",
    "FAILED",
    startTime,
    endTime,
  );
  const uploadFailed = await getMetricSum(
    "UploadElephantPhase",
    "FAILED",
    startTime,
    endTime,
  );
  const hashFailed = await getMetricSum(
    "HashElephantPhase",
    "FAILED",
    startTime,
    endTime,
  );
  const autoRepairFailed = await getMetricSum(
    "AutoRepairElephantPhase",
    "FAILED",
    startTime,
    endTime,
  );

  const totalFailed =
    prepareFailed +
    transformFailed +
    uploadFailed +
    hashFailed +
    autoRepairFailed;

  console.log(
    JSON.stringify({
      level: "info",
      msg: "ready_to_mint_failed_count",
      totalFailed: totalFailed,
      breakdown: {
        prepare: prepareFailed,
        transform: transformFailed,
        upload: uploadFailed,
        hash: hashFailed,
        autoRepair: autoRepairFailed,
      },
    }),
  );

  return totalFailed;
}

/**
 * Update Google Sheet with message counts
 * @param {string} accountId - AWS account ID
 * @param {number} readyToMintMessages - Messages ready to be minted (Transactions/Gas Price queues)
 * @param {number} waitingForMiningMessages - Messages waiting for mining (workflow queues)
 * @param {number} inProgressMessages - Messages in progress (from CloudWatch metrics)
 * @param {number} readyToMintFailedMessages - Failed messages ready to be minted (from CloudWatch metrics)
 * @param {string} stackName - CloudFormation stack name
 * @returns {Promise<void>}
 */
async function updateGoogleSheet(
  accountId,
  readyToMintMessages,
  waitingForMiningMessages,
  inProgressMessages,
  readyToMintFailedMessages,
  stackName,
) {
  const config = await getGoogleSheetsConfig(stackName);

  if (!config) {
    console.log(
      JSON.stringify({
        level: "info",
        msg: "google_sheets_config_not_found",
        reason:
          "Secrets Manager secret not found, skipping Google Sheets update",
      }),
    );
    return;
  }

  try {
    const sheets = await initGoogleSheets(config.credentials);

    // Get current date in format "2025-12-10"
    const today = new Date();
    const dateColumn = today.toISOString().split("T")[0]; // YYYY-MM-DD format

    // Find the account ID row
    const rowIndex = await findAccountIdRow(
      sheets,
      config.sheetId,
      config.tabName,
      accountId,
    );

    if (rowIndex === -1) {
      console.warn(
        JSON.stringify({
          level: "warn",
          msg: "account_id_not_found_in_sheet",
          accountId: accountId,
        }),
      );
      return;
    }

    // Update "ready to be minted" column
    const readyToMintColumnIndex = await findOrCreateDateColumn(
      sheets,
      config.sheetId,
      config.tabName,
      dateColumn,
      "ready to be minted",
    );
    const readyToMintColumnLetter = String.fromCharCode(
      64 + readyToMintColumnIndex,
    );

    await sheets.spreadsheets.values.update({
      spreadsheetId: config.sheetId,
      range: `${config.tabName}!${readyToMintColumnLetter}${rowIndex}`,
      valueInputOption: "RAW",
      requestBody: {
        values: [[readyToMintMessages]],
      },
    });

    // Update "waiting for mining" column
    const waitingForMiningColumnIndex = await findOrCreateDateColumn(
      sheets,
      config.sheetId,
      config.tabName,
      dateColumn,
      "waiting for mining",
    );
    const waitingForMiningColumnLetter = String.fromCharCode(
      64 + waitingForMiningColumnIndex,
    );

    await sheets.spreadsheets.values.update({
      spreadsheetId: config.sheetId,
      range: `${config.tabName}!${waitingForMiningColumnLetter}${rowIndex}`,
      valueInputOption: "RAW",
      requestBody: {
        values: [[waitingForMiningMessages]],
      },
    });

    // Update "in progress" column
    const inProgressColumnIndex = await findOrCreateDateColumn(
      sheets,
      config.sheetId,
      config.tabName,
      dateColumn,
      "in progress",
    );
    const inProgressColumnLetter = String.fromCharCode(
      64 + inProgressColumnIndex,
    );

    await sheets.spreadsheets.values.update({
      spreadsheetId: config.sheetId,
      range: `${config.tabName}!${inProgressColumnLetter}${rowIndex}`,
      valueInputOption: "RAW",
      requestBody: {
        values: [[inProgressMessages]],
      },
    });

    // Update "failed" column
    // Note: This column tracks FAILED metrics from CloudWatch (Prepare, Transform, Upload, Hash, AutoRepair)
    const readyToMintFailedColumnIndex = await findOrCreateDateColumn(
      sheets,
      config.sheetId,
      config.tabName,
      dateColumn,
      "failed",
    );
    const readyToMintFailedColumnLetter = String.fromCharCode(
      64 + readyToMintFailedColumnIndex,
    );

    await sheets.spreadsheets.values.update({
      spreadsheetId: config.sheetId,
      range: `${config.tabName}!${readyToMintFailedColumnLetter}${rowIndex}`,
      valueInputOption: "RAW",
      requestBody: {
        values: [[readyToMintFailedMessages]],
      },
    });

    console.log(
      JSON.stringify({
        level: "info",
        msg: "google_sheet_updated",
        accountId: accountId,
        readyToMintMessages: readyToMintMessages,
        waitingForMiningMessages: waitingForMiningMessages,
        inProgressMessages: inProgressMessages,
        readyToMintFailedMessages: readyToMintFailedMessages,
        dateColumn: dateColumn,
        readyToMintCell: `${readyToMintColumnLetter}${rowIndex}`,
        waitingForMiningCell: `${waitingForMiningColumnLetter}${rowIndex}`,
        inProgressCell: `${inProgressColumnLetter}${rowIndex}`,
        readyToMintFailedCell: `${readyToMintFailedColumnLetter}${rowIndex}`,
      }),
    );
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(
      JSON.stringify({
        level: "error",
        msg: "failed_to_update_google_sheet",
        error: errorMessage,
      }),
    );
    // Don't throw - allow Lambda to complete even if Google Sheets update fails
  }
}

/**
 * Lambda handler to count SQS messages
 * @param {Object} event - Lambda event (can be empty or contain stackName override)
 * @param {string} [event.stackName] - CloudFormation stack name (default: elephant-oracle-node)
 * @returns {Promise<CounterOutput>}
 */
export const handler = async (event = {}) => {
  const base = {
    component: "sqs-message-counter",
    at: new Date().toISOString(),
  };

  try {
    const stackName =
      event.stackName || process.env.STACK_NAME || "elephant-oracle-node";
    const region = process.env.AWS_REGION || "us-east-1";

    console.log(
      JSON.stringify({
        ...base,
        level: "info",
        msg: "starting_message_count",
        stackName: stackName,
        region: region,
      }),
    );

    // Get account ID
    const accountId = await getAccountId();
    console.log(
      JSON.stringify({
        ...base,
        level: "info",
        msg: "account_id_retrieved",
        accountId: accountId,
      }),
    );

    // Define queues to check: output_key|queue_pattern|display_name
    const queueConfigs = [
      [
        "TransactionsSqsQueueUrl",
        "TransactionsSqsQueue",
        "Transactions Main Queue",
      ],
      [
        "TransactionsDeadLetterQueueUrl",
        "TransactionsDeadLetterQueue",
        "Transactions DLQ",
      ],
      [
        "GasPriceCheckerSqsQueueUrl",
        "GasPriceCheckerSqsQueue",
        "Gas Price Main Queue",
      ],
      [
        "GasPriceCheckerDeadLetterQueueUrl",
        "GasPriceCheckerDeadLetterQueue",
        "Gas Price DLQ",
      ],
    ];

    const queueResults = [];
    let totalMessages = 0;

    // Get message count for each queue
    for (const [outputKey, queuePattern, displayName] of queueConfigs) {
      try {
        console.log(
          JSON.stringify({
            ...base,
            level: "info",
            msg: "checking_queue",
            queueName: displayName,
            outputKey: outputKey,
          }),
        );

        // Get queue URL
        const queueUrl = await getQueueUrl(
          stackName,
          outputKey,
          queuePattern,
          accountId,
          region,
        );

        console.log(
          JSON.stringify({
            ...base,
            level: "info",
            msg: "queue_url_retrieved",
            queueName: displayName,
            queueUrl: queueUrl,
          }),
        );

        // Get message count
        const count = await getQueueMessageCount(queueUrl, displayName);

        console.log(
          JSON.stringify({
            ...base,
            level: "info",
            msg: "queue_count_retrieved",
            queueName: displayName,
            count: count,
          }),
        );

        queueResults.push({
          queueName: displayName,
          queueUrl: queueUrl,
          messageCount: count,
        });

        totalMessages += count;
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        console.error(
          JSON.stringify({
            ...base,
            level: "error",
            msg: "queue_check_failed",
            queueName: displayName,
            error: errorMessage,
          }),
        );

        queueResults.push({
          queueName: displayName,
          queueUrl: "N/A",
          messageCount: 0,
          error: errorMessage,
        });
      }
    }

    const result = {
      accountId: accountId,
      stackName: stackName,
      region: region,
      queues: queueResults,
      totalMessages: totalMessages,
      timestamp: new Date().toISOString(),
    };

    console.log(
      JSON.stringify({
        ...base,
        level: "info",
        msg: "message_count_complete",
        totalMessages: totalMessages,
        queueCount: queueResults.length,
      }),
    );

    // Count workflow queue messages (waiting for mining)
    const waitingForMiningMessages = await countWorkflowQueueMessages();

    // Calculate in progress count from CloudWatch metrics (last 30 days)
    const inProgressMessages = await calculateInProgressCount();

    // Calculate ready to be minted (failed) count from CloudWatch metrics (last 30 days)
    const readyToMintFailedMessages = await calculateReadyToMintFailedCount();

    // Update Google Sheet if configured
    // readyToMintMessages = totalMessages (Transactions/Gas Price queues)
    // waitingForMiningMessages = workflow queue messages
    // inProgressMessages = calculated from CloudWatch metrics
    // readyToMintFailedMessages = FAILED metrics from CloudWatch (Prepare, Transform, Upload, Hash)
    await updateGoogleSheet(
      accountId,
      totalMessages,
      waitingForMiningMessages,
      inProgressMessages,
      readyToMintFailedMessages,
      stackName,
    );

    // Add workflow queue count, in progress count, and ready to mint failed count to result
    result.waitingForMiningMessages = waitingForMiningMessages;
    result.inProgressMessages = inProgressMessages;
    result.readyToMintFailedMessages = readyToMintFailedMessages;

    return result;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(
      JSON.stringify({
        ...base,
        level: "error",
        msg: "handler_failed",
        error: errorMessage,
      }),
    );
    throw error;
  }
};
