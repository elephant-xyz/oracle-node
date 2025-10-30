import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";

/**
 * @typedef {object} ExecutionErrorLink
 * @property {string} PK - Primary key in the format `EXECUTION#{executionId}`.
 * @property {string} SK - Sort key in the format `ERROR#{errorHash}`.
 * @property {string} entityType - Entity discriminator (for example `ExecutionError`).
 * @property {"failed" | "maybeSolved" | "solved"} status - Resolution status for the execution-specific error.
 * @property {string} executionId - Identifier of the failed execution.
 */

/**
 * @typedef {object} FailedExecutionItem
 * @property {string} PK - Primary key in the format `EXECUTION#{executionId}`.
 * @property {string} SK - Sort key mirroring the PK (`EXECUTION#{executionId}`).
 * @property {string} executionId - Identifier of the failed execution.
 * @property {string} entityType - Entity discriminator (for example `FailedExecution`).
 * @property {"failed" | "maybeSolved" | "solved"} status - Execution status bucket.
 * @property {string} county - County identifier.
 * @property {number} openErrorCount - Count of unique unresolved errors.
 * @property {string} preparedS3Uri - S3 location of the output of the prepare step.
 * @property {Record<string, string | undefined> | undefined} source - Minimal source details (for example S3 bucket/key).
 */

/**
 * @typedef {object} DynamoDBStreamRecord
 * @property {string} eventName - Event type (INSERT, MODIFY, REMOVE).
 * @property {{ Keys: Record<string, unknown> } | undefined} dynamodb - DynamoDB stream record data.
 * @property {{ [key: string]: unknown } | undefined} dynamodb.OldImage - Previous item state.
 * @property {{ [key: string]: unknown } | undefined} dynamodb.NewImage - Current item state.
 */

/**
 * @typedef {object} DynamoDBStreamEvent
 * @property {DynamoDBStreamRecord[]} Records - Array of stream records.
 */

const DEFAULT_CLIENT = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
  marshallOptions: { removeUndefinedValues: true },
});

const lambdaClient = new LambdaClient({});

/**
 * Ensure required environment variables are present.
 *
 * @param {string} name - Environment variable identifier.
 * @returns {string} - Resolved environment variable value.
 */
function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
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
 * Construct seed_output_s3_uri from preparedS3Uri.
 * The seed output is stored at the same prefix as the prepared output, but with seed_output.zip filename.
 *
 * @param {string} preparedS3Uri - S3 URI of the prepared output (e.g., s3://bucket/outputs/fileBase/output.zip).
 * @returns {string} - S3 URI of the seed output (e.g., s3://bucket/outputs/fileBase/seed_output.zip).
 */
function constructSeedOutputS3Uri(preparedS3Uri) {
  const match = /^s3:\/\/([^/]+)\/(.*)$/.exec(preparedS3Uri);
  if (!match) {
    throw new Error(`Bad S3 URI: ${preparedS3Uri}`);
  }
  const bucket = match[1];
  const key = match[2];
  // Replace /output.zip with /seed_output.zip
  const seedKey = key.replace(/\/output\.zip$/, "/seed_output.zip");
  return `s3://${bucket}/${seedKey}`;
}

/**
 * Atomically decrement openErrorCount for a failed execution.
 *
 * @param {object} params - Update parameters.
 * @param {DynamoDBDocumentClient} params.client - DynamoDB document client.
 * @param {string} params.tableName - DynamoDB table name.
 * @param {string} params.executionId - Execution identifier.
 * @returns {Promise<{ openErrorCount: number, status: string }>} - Updated openErrorCount and status.
 */
async function decrementOpenErrorCount({ client, tableName, executionId }) {
  const now = new Date().toISOString();
  const response = await client.send(
    new UpdateCommand({
      TableName: tableName,
      Key: {
        PK: buildExecutionPk(executionId),
        SK: buildExecutionSk(executionId),
      },
      UpdateExpression:
        "ADD #openErrorCount :decrement SET #updatedAt = :updatedAt",
      ExpressionAttributeNames: {
        "#openErrorCount": "openErrorCount",
        "#updatedAt": "updatedAt",
      },
      ExpressionAttributeValues: {
        ":decrement": -1,
        ":updatedAt": now,
      },
      ReturnValues: "ALL_NEW",
    }),
  );

  if (!response.Attributes) {
    throw new Error(
      `Failed to update execution ${executionId}: no attributes returned`,
    );
  }

  const execution =
    /** @type {FailedExecutionItem} */ (response.Attributes);

  return {
    openErrorCount: execution.openErrorCount ?? 0,
    status: execution.status ?? "failed",
  };
}

/**
 * Update execution status to "maybeSolved".
 *
 * @param {object} params - Update parameters.
 * @param {DynamoDBDocumentClient} params.client - DynamoDB document client.
 * @param {string} params.tableName - DynamoDB table name.
 * @param {string} params.executionId - Execution identifier.
 * @returns {Promise<FailedExecutionItem>} - Updated execution item.
 */
async function updateExecutionStatusToMaybeSolved({
  client,
  tableName,
  executionId,
}) {
  const now = new Date().toISOString();
  const response = await client.send(
    new UpdateCommand({
      TableName: tableName,
      Key: {
        PK: buildExecutionPk(executionId),
        SK: buildExecutionSk(executionId),
      },
      UpdateExpression:
        "SET #status = :maybeSolved, #updatedAt = :updatedAt",
      ExpressionAttributeNames: {
        "#status": "status",
        "#updatedAt": "updatedAt",
      },
      ExpressionAttributeValues: {
        ":maybeSolved": "maybeSolved",
        ":updatedAt": now,
      },
      ReturnValues: "ALL_NEW",
    }),
  );

  if (!response.Attributes) {
    throw new Error(
      `Failed to update execution status ${executionId}: no attributes returned`,
    );
  }

  return /** @type {FailedExecutionItem} */ (response.Attributes);
}

/**
 * Invoke the post-processing Lambda function to restart the execution.
 *
 * @param {object} params - Invocation parameters.
 * @param {string} params.functionName - Lambda function name or ARN.
 * @param {string} params.preparedS3Uri - S3 URI of the prepared output.
 * @param {string} params.seedOutputS3Uri - S3 URI of the seed output.
 * @param {{ bucket?: { name?: string }, object?: { key?: string } } | undefined} params.s3Event - Optional S3 event information.
 * @returns {Promise<void>}
 */
async function invokePostProcessingLambda({
  functionName,
  preparedS3Uri,
  seedOutputS3Uri,
  s3Event,
}) {
  console.log(`Invoking post-processing Lambda ${functionName}...`);
  console.log(`Prepared S3 URI: ${preparedS3Uri}`);
  console.log(`Seed output S3 URI: ${seedOutputS3Uri}`);

  const payload = {
    prepare: {
      output_s3_uri: preparedS3Uri,
    },
    seed_output_s3_uri: seedOutputS3Uri,
    prepareSkipped: false,
  };

  // Add S3 event if available
  if (s3Event?.bucket?.name && s3Event?.object?.key) {
    payload.s3 = {
      bucket: {
        name: s3Event.bucket.name,
      },
      object: {
        key: s3Event.object.key,
      },
    };
  }

  try {
    const response = await lambdaClient.send(
      new InvokeCommand({
        FunctionName: functionName,
        InvocationType: "RequestResponse",
        Payload: JSON.stringify(payload),
      }),
    );

    if (response.FunctionError) {
      const errorPayload = JSON.parse(
        new TextDecoder().decode(response.Payload ?? new Uint8Array()),
      );
      throw new Error(
        `Post-processing Lambda failed: ${errorPayload.errorMessage || errorPayload.errorType || JSON.stringify(errorPayload)}`,
      );
    }

    const resultPayload = JSON.parse(
      new TextDecoder().decode(response.Payload ?? new Uint8Array()),
    );

    if (resultPayload.status !== "success") {
      throw new Error(
        `Post-processing Lambda returned non-success status: ${resultPayload.status}`,
      );
    }

    console.log(
      `Post-processing Lambda succeeded with ${resultPayload.transactionItems?.length || 0} transaction items`,
    );
  } catch (error) {
    console.error("Post-processing Lambda invocation failed:", error);
    throw error;
  }
}

/**
 * Extract executionId from an ExecutionErrorLink item.
 *
 * @param {Record<string, unknown>} item - DynamoDB item.
 * @returns {string | null} - Execution ID or null if not an ExecutionErrorLink.
 */
function extractExecutionId(item) {
  if (
    typeof item.entityType === "string" &&
    item.entityType === "ExecutionError" &&
    typeof item.executionId === "string"
  ) {
    return item.executionId;
  }
  return null;
}

/**
 * Process DynamoDB stream event.
 *
 * @param {DynamoDBStreamEvent} event - DynamoDB stream event.
 * @returns {Promise<void>}
 */
export const handler = async (event) => {
  const logBase = {
    component: "error-resolver",
    at: new Date().toISOString(),
  };

  try {
    console.log(
      JSON.stringify({
        ...logBase,
        level: "info",
        msg: "received",
        recordCount: event?.Records?.length ?? 0,
      }),
    );

    const tableName = requireEnv("ERRORS_TABLE_NAME");
    const postProcessorFunctionName = requireEnv("POST_PROCESSOR_FUNCTION_NAME");
    const client = DEFAULT_CLIENT;

    // Group ExecutionErrorLink records by executionId
    /** @type {Map<string, ExecutionErrorLink[]>} */
    const executionGroups = new Map();

    for (const record of event.Records ?? []) {
      if (!record.dynamodb?.NewImage) {
        continue;
      }

      const newImage = record.dynamodb.NewImage;
      const oldImage = record.dynamodb.OldImage;

      // Skip if not an ExecutionErrorLink with status transition to "maybeSolved"
      if (
        typeof newImage.entityType !== "string" ||
        newImage.entityType !== "ExecutionError"
      ) {
        continue;
      }

      const newStatus = newImage.status;
      const oldStatus = oldImage?.status;

      // Only process items that changed from "failed" to "maybeSolved"
      if (
        newStatus !== "maybeSolved" ||
        oldStatus !== "failed"
      ) {
        continue;
      }

      const executionId = extractExecutionId(newImage);
      if (!executionId) {
        console.warn(
          JSON.stringify({
            ...logBase,
            level: "warn",
            msg: "skipped_record_missing_execution_id",
            record: record,
          }),
        );
        continue;
      }

      const errorLink = /** @type {ExecutionErrorLink} */ (newImage);
      const group = executionGroups.get(executionId) ?? [];
      group.push(errorLink);
      executionGroups.set(executionId, group);
    }

    console.log(
      JSON.stringify({
        ...logBase,
        level: "info",
        msg: "processing_executions",
        executionCount: executionGroups.size,
      }),
    );

    // Process each execution group
    // Track executions that have been restarted to avoid duplicate restarts
    /** @type {Set<string>} */
    const restartedExecutions = new Set();

    for (const [executionId, errorLinks] of executionGroups) {
      console.log(
        JSON.stringify({
          ...logBase,
          level: "info",
          msg: "processing_execution",
          executionId,
          errorLinkCount: errorLinks.length,
        }),
      );

      // Process each error link (each one decrements the count)
      for (const _errorLink of errorLinks) {
        // Skip if already restarted in this batch
        if (restartedExecutions.has(executionId)) {
          console.log(
            JSON.stringify({
              ...logBase,
              level: "info",
              msg: "execution_already_restarted",
              executionId,
            }),
          );
          continue;
        }

        try {
          const { openErrorCount, status } = await decrementOpenErrorCount({
            client,
            tableName,
            executionId,
          });

          console.log(
            JSON.stringify({
              ...logBase,
              level: "info",
              msg: "decremented_error_count",
              executionId,
              openErrorCount,
              status,
            }),
          );

          // If count reached 0 and status is still "failed", update status and restart
          if (openErrorCount === 0 && status === "failed") {
            console.log(
              JSON.stringify({
                ...logBase,
                level: "info",
                msg: "all_errors_resolved",
                executionId,
              }),
            );

            // Update execution status to "maybeSolved"
            const execution = await updateExecutionStatusToMaybeSolved({
              client,
              tableName,
              executionId,
            });

            if (!execution.preparedS3Uri) {
              throw new Error(
                `Execution ${executionId} missing preparedS3Uri, cannot restart`,
              );
            }

            const seedOutputS3Uri = constructSeedOutputS3Uri(
              execution.preparedS3Uri,
            );

            // Build S3 event from source
            const s3Event = execution.source
              ? {
                  bucket: { name: execution.source.s3Bucket },
                  object: { key: execution.source.s3Key },
                }
              : undefined;

            // Invoke post-processing Lambda to restart execution
            await invokePostProcessingLambda({
              functionName: postProcessorFunctionName,
              preparedS3Uri: execution.preparedS3Uri,
              seedOutputS3Uri,
              s3Event,
            });

            console.log(
              JSON.stringify({
                ...logBase,
                level: "info",
                msg: "execution_restarted",
                executionId,
              }),
            );

            // Mark as restarted to prevent duplicate restarts
            restartedExecutions.add(executionId);
          }
        } catch (error) {
          console.error(
            JSON.stringify({
              ...logBase,
              level: "error",
              msg: "execution_processing_failed",
              executionId,
              error: String(error),
            }),
          );
          // Continue processing other executions
        }
      }
    }

    console.log(
      JSON.stringify({
        ...logBase,
        level: "info",
        msg: "completed",
        executionCount: executionGroups.size,
      }),
    );
  } catch (error) {
    console.error(
      JSON.stringify({
        ...logBase,
        level: "error",
        msg: "handler_failed",
        error: String(error),
      }),
    );
    throw error;
  }
};

