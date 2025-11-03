import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  BatchWriteCommand,
  DynamoDBDocumentClient,
  QueryCommand,
  UpdateCommand,
  TransactWriteCommand,
} from "@aws-sdk/lib-dynamodb";
import { createHash } from "crypto";

/**
 * @typedef {object} ErrorRecord
 * @property {string} PK - Primary key in the format `ERROR#{errorHash}`.
 * @property {string} SK - Sort key mirroring the primary key (`ERROR#{errorHash}`).
 * @property {string} entityType - Entity discriminator (for example `ErrorAggregate`).
 * @property {string} errorMessage - Human readable error message.
 * @property {string} errorPath - JSON path that triggered the error.
 * @property {number} totalCount - Aggregate occurrence count across executions.
 * @property {string} createdAt - ISO timestamp when the error aggregate was created.
 * @property {string} updatedAt - ISO timestamp when the aggregate was last updated.
 * @property {string} latestExecutionId - Recent execution identifier that observed the error.
 */

/**
 * @typedef {"failed" | "maybeSolved" | "solved"} ErrorStatus
 */

/**
 * @typedef {object} ExecutionErrorLink
 * @property {string} PK - Primary key in the format `EXECUTION#{executionId}`.
 * @property {string} SK - Sort key in the format `ERROR#{errorHash}`.
 * @property {string} entityType - Entity discriminator (for example `ExecutionError`).
 * @property {ErrorStatus} status - Resolution status for the execution-specific error.
 * @property {string} errorHash - Deterministic hash for message+path.
 * @property {string} errorMessage - Human readable error message.
 * @property {string} errorPath - JSON path that triggered the error.
 * @property {number} occurrences - Occurrence count within the execution.
 * @property {string} executionId - Identifier of the failed execution.
 * @property {string} county - County identifier.
 * @property {string} createdAt - ISO timestamp when the link item was created.
 * @property {string} updatedAt - ISO timestamp when the link item was last updated.
 * @property {string} GS1PK - Global secondary index PK (`ERROR#{errorHash}`) for reverse lookup.
 * @property {string} GS1SK - Global secondary index SK (`EXECUTION#{executionId}`) for reverse lookup.
 */

/**
 * @typedef {object} FailedExecutionItem
 * @property {string} PK - Primary key in the format `EXECUTION#{executionId}`.
 * @property {string} SK - Sort key mirroring the PK (`EXECUTION#{executionId}`).
 * @property {string} executionId - Identifier of the failed execution.
 * @property {string} entityType - Entity discriminator (for example `FailedExecution`).
 * @property {string} status - Execution status bucket (`failed` | `maybeSolved` | `solved`).
 * @property {string} county - County identifier.
 * @property {number} totalOccurrences - Total error occurrences observed.
 * @property {number} openErrorCount - Count of unique unresolved errors.
 * @property {number} uniqueErrorCount - Count of unique errors in the execution.
 * @property {string | undefined} errorsS3Uri - Optional S3 URI pointing to submit_errors.csv.
 * @property {string | undefined} failureMessage - Primary failure message for diagnostics.
 * @property {string} preparedS3Uri - S3 location of the output of the prepare step.
 * @property {Record<string, string | undefined> | undefined} source - Minimal source details (for example S3 bucket/key).
 * @property {string} createdAt - ISO timestamp when the execution record was created.
 * @property {string} updatedAt - ISO timestamp when the execution record was updated.
 * @property {string} GS3PK - Global secondary index partition key (`METRIC#ERRORCOUNT`).
 * @property {string} GS3SK - Global secondary index sort key (for example `COUNT#000010#EXECUTION#uuid`).
 */

/**
 * @typedef {object} RawSubmitErrorRow
 * @property {string | undefined} errorMessage - Error message field.
 * @property {string | undefined} error_message - Error message field.
 * @property {string | undefined} errorPath - Error path field.
 * @property {string | undefined} error_path - Error path field.
 */

/**
 * @typedef {object} NormalizedError
 * @property {string} hash - SHA256 hash of error message and path.
 * @property {string} message - Error message.
 * @property {string} path - Error path within payload.
 * @property {number} occurrences - Number of times this error appears for the execution.
 */

/**
 * @typedef {object} ExecutionSource
 * @property {string | undefined} s3Bucket - Source S3 bucket if available.
 * @property {string | undefined} s3Key - Source S3 object key if available.
 */

/**
 * @typedef {object} SaveFailedExecutionParams
 * @property {string} executionId - Identifier generated for the failed workflow execution.
 * @property {string} county - County identifier associated with the execution.
 * @property {RawSubmitErrorRow[]} errors - Raw error rows parsed from submit_errors.csv.
 * @property {ExecutionSource} source - Minimal source event description.
 * @property {string | undefined} errorsS3Uri - Optional S3 location storing submit_errors.csv.
 * @property {string | undefined} failureMessage - Descriptive failure message.
 * @property {string} preparedS3Uri - S3 location of the output of the prepare step.
 * @property {string} occurredAt - ISO timestamp representing when the failure happened.
 */

/**
 * @typedef {object} ErrorsRepository
 * @property {(params: SaveFailedExecutionParams) => Promise<void>} saveFailedExecution - Persist the failed execution and related errors.
 */

const DEFAULT_CLIENT = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
  marshallOptions: { removeUndefinedValues: true },
});

const BATCH_WRITE_MAX_ITEMS = 25;
const BATCH_WRITE_MAX_RETRIES = 5;
const TRANSACT_WRITE_MAX_ITEMS = 100;

/**
 * Create the DynamoDB errors repository abstraction.
 *
 * @param {{ tableName: string, documentClient?: DynamoDBDocumentClient }} params - Repository configuration.
 * @returns {ErrorsRepository} - Repository with high-level persistence helpers.
 */
export function createErrorsRepository({ tableName, documentClient }) {
  const client = documentClient ?? DEFAULT_CLIENT;

  /**
   * Persist the execution failure and its unique error links.
   *
   * @param {SaveFailedExecutionParams} params - Execution persistence payload.
   * @returns {Promise<void>} - Resolves when all writes complete.
   */
  async function saveFailedExecution({
    executionId,
    county,
    errors,
    source,
    errorsS3Uri,
    failureMessage,
    occurredAt,
    preparedS3Uri,
  }) {
    if (!Array.isArray(errors) || errors.length === 0) {
      throw new Error("errors array must contain at least one entry");
    }

    const normalizedErrors = normalizeErrors(errors, county);
    const totalOccurrences = normalizedErrors.reduce(
      (sum, error) => sum + error.occurrences,
      0,
    );
    const uniqueErrorCount = normalizedErrors.length;

    const executionItem = buildExecutionItem({
      executionId,
      county,
      totalOccurrences,
      uniqueErrorCount,
      errorsS3Uri,
      failureMessage,
      occurredAt,
      source,
      preparedS3Uri,
    });
    const linkItems = normalizedErrors.map((error) =>
      buildExecutionErrorLink({
        executionId,
        county,
        error,
        occurredAt,
      }),
    );

    await writeItems(client, tableName, [executionItem, ...linkItems]);

    for (const error of normalizedErrors) {
      await incrementErrorAggregate({
        client,
        tableName,
        error,
        executionId,
        occurredAt,
      });
    }
  }

  return { saveFailedExecution };
}

/**
 * Normalize raw submit error rows into deterministic error hashes and counts.
 *
 * @param {RawSubmitErrorRow[]} rawRows - Raw error rows parsed from the CSV.
 * @param {string} county - County identifier to include in hash.
 * @returns {NormalizedError[]} - Deduplicated normalized errors with occurrences.
 */
export function normalizeErrors(rawRows, county) {
  /** @type {Map<string, NormalizedError>} */
  const map = new Map();

  for (const row of rawRows) {
    const message = resolveErrorMessage(row);
    const path = resolveErrorPath(row);
    const hash = createErrorHash(message, path, county);
    const existing = map.get(hash);
    if (existing) {
      existing.occurrences += 1;
    } else {
      map.set(hash, {
        hash,
        message,
        path,
        occurrences: 1,
      });
    }
  }

  return Array.from(map.values());
}

/**
 * Resolve the error message from the submit error row.
 *
 * @param {RawSubmitErrorRow} row - Raw CSV row to inspect.
 * @returns {string} - Error message extracted from the row.
 */
function resolveErrorMessage(row) {
  if (
    typeof row.errorMessage === "string" &&
    row.errorMessage.trim().length > 0
  ) {
    return row.errorMessage.trim();
  } else if (
    typeof row.error_message === "string" &&
    row.error_message.trim().length > 0
  ) {
    return row.error_message.trim();
  }
  throw new Error(
    `Unable to resolve error message from submit error row. Row: ${JSON.stringify(row)}`,
  );
}

/**
 * Resolve the error path from the submit error row.
 *
 * @param {RawSubmitErrorRow} row - Raw CSV row to inspect.
 * @returns {string} - Error path extracted from the row.
 */
function resolveErrorPath(row) {
  if (typeof row.errorPath === "string" && row.errorPath.trim().length > 0) {
    return row.errorPath.trim();
  } else if (
    typeof row.error_path === "string" &&
    row.error_path.trim().length > 0
  ) {
    return row.error_path.trim();
  }

  return "unknown";
}

/**
 * Compute the deterministic error hash from message, path, and county.
 *
 * @param {string} message - Error message.
 * @param {string} path - Error path.
 * @param {string} county - County identifier.
 * @returns {string} - SHA256 hash string.
 */
function createErrorHash(message, path, county) {
  return createHash("sha256")
    .update(`${message}#${path}#${county}`, "utf8")
    .digest("hex");
}

/**
 * Build the execution root item for DynamoDB.
 *
 * @param {object} params - Execution item attributes.
 * @param {string} params.executionId - Execution identifier.
 * @param {string} params.county - County identifier.
 * @param {number} params.totalOccurrences - Total error occurrences.
 * @param {number} params.uniqueErrorCount - Number of unique errors.
 * @param {string | undefined} params.errorsS3Uri - Optional S3 URI storing submit_errors.csv.
 * @param {string | undefined} params.failureMessage - Failure message for diagnostics.
 * @param {string} params.occurredAt - ISO timestamp of failure.
 * @param {ExecutionSource} params.source - Minimal source description.
 * @param {string} params.preparedS3Uri - S3 location of the output of the prepare step.
 * @returns {FailedExecutionItem} - DynamoDB item for the execution.
 */
function buildExecutionItem({
  executionId,
  county,
  totalOccurrences,
  uniqueErrorCount,
  errorsS3Uri,
  failureMessage,
  occurredAt,
  source,
  preparedS3Uri,
}) {
  const paddedCount = String(uniqueErrorCount).padStart(12, "0");
  const sanitizedSource = Object.fromEntries(
    Object.entries(source).filter(
      ([_key, value]) => typeof value === "string" && value.length > 0,
    ),
  );
  return {
    PK: buildExecutionPk(executionId),
    SK: buildExecutionSk(executionId),
    entityType: "FailedExecution",
    executionId,
    status: "failed",
    county,
    totalOccurrences,
    openErrorCount: uniqueErrorCount,
    uniqueErrorCount,
    errorsS3Uri,
    failureMessage,
    source:
      Object.keys(sanitizedSource).length > 0 ? sanitizedSource : undefined,
    createdAt: occurredAt,
    updatedAt: occurredAt,
    preparedS3Uri,
    GS3PK: "METRIC#ERRORCOUNT",
    GS3SK: `COUNT#${paddedCount}#EXECUTION#${executionId}`,
  };
}

/**
 * Build an execution-error link item joining execution and error hash.
 *
 * @param {object} params - Link item attributes.
 * @param {string} params.executionId - Execution identifier.
 * @param {string} params.county - County identifier.
 * @param {NormalizedError} params.error - Normalized error details.
 * @param {string} params.occurredAt - ISO timestamp of failure.
 * @returns {ExecutionErrorLink} - DynamoDB item linking execution and error hash.
 */
function buildExecutionErrorLink({ executionId, county, error, occurredAt }) {
  return {
    PK: buildExecutionPk(executionId),
    SK: buildErrorSk(error.hash),
    entityType: "ExecutionError",
    status: "failed",
    errorHash: error.hash,
    errorMessage: error.message,
    errorPath: error.path,
    occurrences: error.occurrences,
    executionId,
    county,
    createdAt: occurredAt,
    updatedAt: occurredAt,
    GS1PK: buildErrorPk(error.hash),
    GS1SK: buildExecutionSk(executionId),
  };
}

/**
 * Increment the aggregate error counter shared across executions.
 *
 * @param {object} params - Aggregate update parameters.
 * @param {DynamoDBDocumentClient} params.client - DynamoDB document client.
 * @param {string} params.tableName - DynamoDB table name.
 * @param {NormalizedError} params.error - Normalized error details.
 * @param {string} params.executionId - Identifier of the current execution.
 * @param {string} params.occurredAt - ISO timestamp when the error was observed.
 * @returns {Promise<void>} - Resolves when the update completes.
 */
async function incrementErrorAggregate({
  client,
  tableName,
  error,
  executionId,
  occurredAt,
}) {
  await client.send(
    new UpdateCommand({
      TableName: tableName,
      Key: {
        PK: buildErrorPk(error.hash),
        SK: buildErrorSk(error.hash),
      },
      UpdateExpression:
        "SET #type = if_not_exists(#type, :aggregateType), #errorMessage = if_not_exists(#errorMessage, :errorMessage), #errorPath = if_not_exists(#errorPath, :errorPath), #createdAt = if_not_exists(#createdAt, :occurredAt), #updatedAt = :occurredAt, #latestExecutionId = :executionId ADD #totalCount :occurrences",
      ExpressionAttributeNames: {
        "#type": "entityType",
        "#errorMessage": "errorMessage",
        "#errorPath": "errorPath",
        "#createdAt": "createdAt",
        "#updatedAt": "updatedAt",
        "#latestExecutionId": "latestExecutionId",
        "#totalCount": "totalCount",
      },
      ExpressionAttributeValues: {
        ":aggregateType": "ErrorAggregate",
        ":errorMessage": error.message,
        ":errorPath": error.path,
        ":occurredAt": occurredAt,
        ":executionId": executionId,
        ":occurrences": error.occurrences,
      },
    }),
  );
}

/**
 * Write items to DynamoDB using BatchWrite with retry semantics.
 *
 * @param {DynamoDBDocumentClient} client - DynamoDB document client.
 * @param {string} tableName - DynamoDB table name.
 * @param {Array<FailedExecutionItem | ExecutionErrorLink>} items - Items to persist.
 * @returns {Promise<void>} - Resolves when all batches persist successfully.
 */
async function writeItems(client, tableName, items) {
  const batches = chunkArray(items, BATCH_WRITE_MAX_ITEMS);
  for (const batch of batches) {
    let remaining = batch;
    let attempts = 0;
    while (remaining.length > 0) {
      const response = await client.send(
        new BatchWriteCommand({
          RequestItems: {
            [tableName]: remaining.map((item) => ({
              PutRequest: { Item: item },
            })),
          },
        }),
      );

      const unprocessed =
        response.UnprocessedItems?.[tableName]?.map(
          /** @param {import("@aws-sdk/client-dynamodb").WriteRequest} request */
          (request) => request.PutRequest?.Item,
        ) ?? [];
      if (unprocessed.length === 0) {
        break;
      }

      attempts += 1;
      if (attempts > BATCH_WRITE_MAX_RETRIES) {
        throw new Error("Exceeded BatchWrite retry attempts");
      }
      const delayMs = Math.min(1000, 2 ** attempts * 50);
      await delay(delayMs);
      remaining =
        /** @type {Array<FailedExecutionItem | ExecutionErrorLink>} */ (
          /** @type {unknown} */ (
            unprocessed.filter((item) => item !== undefined)
          )
        );
    }
  }
}

/**
 * Split an array into evenly sized chunks.
 *
 * @template T
 * @param {T[]} input - Input array.
 * @param {number} size - Desired chunk size.
 * @returns {T[][]} - Chunked arrays.
 */
function chunkArray(input, size) {
  const result = [];
  for (let index = 0; index < input.length; index += size) {
    result.push(input.slice(index, index + size));
  }
  return result;
}

/**
 * Pause execution for the specified duration.
 *
 * @param {number} ms - Duration to wait in milliseconds.
 * @returns {Promise<void>} - Promise that resolves after the delay.
 */
function delay(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

/**
 * Build the execution partition key.
 *
 * @param {string} executionId - Execution identifier.
 * @returns {`EXECUTION#${string}`} - Partition key.
 */
function buildExecutionPk(executionId) {
  return /** @type {`EXECUTION#${string}`} */ (`EXECUTION#${executionId}`);
}

/**
 * Build the execution sort key.
 *
 * @param {string} executionId - Execution identifier.
 * @returns {`EXECUTION#${string}`} - Sort key mirroring the PK.
 */
function buildExecutionSk(executionId) {
  return /** @type {`EXECUTION#${string}`} */ (`EXECUTION#${executionId}`);
}

/**
 * Build the error partition key.
 *
 * @param {string} errorHash - Error hash.
 * @returns {`ERROR#${string}`} - Error partition key.
 */
function buildErrorPk(errorHash) {
  return /** @type {`ERROR#${string}`} */ (`ERROR#${errorHash}`);
}

/**
 * Build the error sort key.
 *
 * @param {string} errorHash - Error hash.
 * @returns {`ERROR#${string}`} - Error sort key mirroring the PK.
 */
function buildErrorSk(errorHash) {
  return /** @type {`ERROR#${string}`} */ (`ERROR#${errorHash}`);
}

/**
 * Query DynamoDB for the execution with the most errors using the ExecutionErrorCountIndex.
 *
 * @param {object} params - Query parameters.
 * @param {string} params.tableName - DynamoDB table name.
 * @param {DynamoDBDocumentClient} [params.documentClient] - Optional document client (uses default if not provided).
 * @returns {Promise<FailedExecutionItem | null>} - The execution with most errors, or null if none found.
 */
export async function queryExecutionWithMostErrors({
  tableName,
  documentClient,
}) {
  const client = documentClient ?? DEFAULT_CLIENT;

  const response = await client.send(
    new QueryCommand({
      TableName: tableName,
      IndexName: "ExecutionErrorCountIndex",
      KeyConditionExpression: "GS3PK = :pk",
      FilterExpression: "#status = :status AND #entityType = :entityType",
      ExpressionAttributeNames: {
        "#status": "status",
        "#entityType": "entityType",
      },
      ExpressionAttributeValues: {
        ":pk": "METRIC#ERRORCOUNT",
        ":status": "failed",
        ":entityType": "FailedExecution",
      },
      ScanIndexForward: false,
      Limit: 1,
    }),
  );

  if (!response.Items || response.Items.length === 0) {
    return null;
  }

  return /** @type {FailedExecutionItem} */ (response.Items[0]);
}

/**
 * Mark all ExecutionErrorLink items with matching error hashes as "maybeSolved".
 * Queries all ExecutionErrorLink items across all executions for each error hash.
 * Uses TransactWriteCommand to batch update operations for better performance.
 *
 * @param {object} params - Update parameters.
 * @param {string[]} params.errorHashes - Array of error hashes to mark as maybeSolved.
 * @param {string} params.tableName - DynamoDB table name.
 * @param {DynamoDBDocumentClient} [params.documentClient] - Optional document client (uses default if not provided).
 * @returns {Promise<void>} - Resolves when all updates complete.
 */
export async function markErrorsAsMaybeSolved({
  errorHashes,
  tableName,
  documentClient,
}) {
  const client = documentClient ?? DEFAULT_CLIENT;
  const now = new Date().toISOString();

  /** @type {ExecutionErrorLink[]} */
  const allItemsToUpdate = [];

  for (const errorHash of errorHashes) {
    const errorPk = buildErrorPk(errorHash);
    /** @type {Record<string, unknown> | undefined} */
    let lastEvaluatedKey = undefined;

    do {
      /** @type {import("@aws-sdk/lib-dynamodb").QueryCommandInput} */
      const queryParams = {
        TableName: tableName,
        IndexName: "ErrorHashExecutionIndex",
        KeyConditionExpression: "GS1PK = :errorPk",
        ExpressionAttributeValues: {
          ":errorPk": errorPk,
          ":status": "failed",
        },
        FilterExpression: "#status = :status",
        ExpressionAttributeNames: {
          "#status": "status",
        },
        ExclusiveStartKey: lastEvaluatedKey,
      };

      /** @type {import("@aws-sdk/lib-dynamodb").QueryCommandOutput} */
      const response = await client.send(new QueryCommand(queryParams));

      if (response.Items && response.Items.length > 0) {
        allItemsToUpdate.push(
          ...response.Items.map(
            (item) => /** @type {ExecutionErrorLink} */ (item),
          ),
        );
      }

      lastEvaluatedKey = response.LastEvaluatedKey;
    } while (lastEvaluatedKey);
  }

  const batches = chunkArray(allItemsToUpdate, TRANSACT_WRITE_MAX_ITEMS);
  for (const batch of batches) {
    const transactItems = batch.map((item) => ({
      Update: {
        TableName: tableName,
        Key: {
          PK: item.PK,
          SK: item.SK,
        },
        UpdateExpression: "SET #status = :maybeSolved, #updatedAt = :updatedAt",
        ExpressionAttributeNames: {
          "#status": "status",
          "#updatedAt": "updatedAt",
        },
        ExpressionAttributeValues: {
          ":maybeSolved": "maybeSolved",
          ":updatedAt": now,
        },
      },
    }));

    await client.send(
      new TransactWriteCommand({
        TransactItems: transactItems,
      }),
    );
  }
}

/**
 * Delete an execution and all its related ExecutionErrorLink items from DynamoDB.
 * Queries all items with PK=EXECUTION#{executionId} (execution root + all error links)
 * and deletes them using BatchWriteCommand with DeleteRequest items.
 *
 * @param {object} params - Deletion parameters.
 * @param {string} params.executionId - Execution identifier to delete.
 * @param {string} params.tableName - DynamoDB table name.
 * @param {DynamoDBDocumentClient} [params.documentClient] - Optional document client (uses default if not provided).
 * @returns {Promise<void>} - Resolves when all deletions complete.
 */
export async function deleteExecution({
  executionId,
  tableName,
  documentClient,
}) {
  const client = documentClient ?? DEFAULT_CLIENT;
  const executionPk = buildExecutionPk(executionId);

  /** @type {Array<{ PK: string, SK: string }>} */
  const itemsToDelete = [];

  // Query all items with PK=EXECUTION#{executionId} to get execution root + all error links
  /** @type {Record<string, unknown> | undefined} */
  let lastEvaluatedKey = undefined;

  do {
    /** @type {import("@aws-sdk/lib-dynamodb").QueryCommandInput} */
    const queryParams = {
      TableName: tableName,
      KeyConditionExpression: "PK = :pk",
      ExpressionAttributeValues: {
        ":pk": executionPk,
      },
      ProjectionExpression: "PK, SK",
      ExclusiveStartKey: lastEvaluatedKey,
    };

    /** @type {import("@aws-sdk/lib-dynamodb").QueryCommandOutput} */
    const response = await client.send(new QueryCommand(queryParams));

    if (response.Items && response.Items.length > 0) {
      itemsToDelete.push(
        ...response.Items.map((item) => ({
          PK: /** @type {string} */ (item.PK),
          SK: /** @type {string} */ (item.SK),
        })),
      );
    }

    lastEvaluatedKey = response.LastEvaluatedKey;
  } while (lastEvaluatedKey);

  if (itemsToDelete.length === 0) {
    console.log(
      `No items found for execution ${executionId}, nothing to delete`,
    );
    return;
  }

  console.log(
    `Deleting ${itemsToDelete.length} item(s) for execution ${executionId}`,
  );

  // Delete items in batches using BatchWriteCommand
  const batches = chunkArray(itemsToDelete, BATCH_WRITE_MAX_ITEMS);
  for (const batch of batches) {
    let remaining = batch;
    let attempts = 0;

    while (remaining.length > 0) {
      const response = await client.send(
        new BatchWriteCommand({
          RequestItems: {
            [tableName]: remaining.map((item) => ({
              DeleteRequest: {
                Key: {
                  PK: item.PK,
                  SK: item.SK,
                },
              },
            })),
          },
        }),
      );

      const unprocessed =
        response.UnprocessedItems?.[tableName]?.map(
          /** @param {import("@aws-sdk/client-dynamodb").WriteRequest} request */
          (request) => {
            const deleteRequest = request.DeleteRequest;
            if (!deleteRequest || !deleteRequest.Key) {
              return undefined;
            }
            return {
              PK: /** @type {string} */ (deleteRequest.Key.PK),
              SK: /** @type {string} */ (deleteRequest.Key.SK),
            };
          },
        ) ?? [];

      if (unprocessed.length === 0) {
        break;
      }

      attempts += 1;
      if (attempts > BATCH_WRITE_MAX_RETRIES) {
        throw new Error(
          `Exceeded BatchWrite retry attempts while deleting execution ${executionId}`,
        );
      }

      const delayMs = Math.min(1000, 2 ** attempts * 50);
      await delay(delayMs);
      remaining = unprocessed.filter((item) => item !== undefined);
    }
  }

  console.log(
    `Successfully deleted ${itemsToDelete.length} item(s) for execution ${executionId}`,
  );
}
