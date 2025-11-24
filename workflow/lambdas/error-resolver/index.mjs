import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  UpdateCommand,
  QueryCommand,
} from "@aws-sdk/lib-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import {
  SQSClient,
  SendMessageCommand,
  GetQueueUrlCommand,
} from "@aws-sdk/client-sqs";

/**
 * @typedef {object} ExecutionErrorLink
 * @property {string} PK - Primary key in the format `EXECUTION#{executionId}`.
 * @property {string} SK - Sort key in the format `ERROR#{errorHash}`.
 * @property {string} entityType - Entity discriminator (for example `ExecutionError`).
 * @property {"failed" | "maybeSolved" | "maybeUnrecoverable" | "solved"} status - Resolution status for the execution-specific error.
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
const sqsClient = new SQSClient({});

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
 * Get the county-specific DLQ URL by queue name.
 *
 * @param {string} county - County identifier (will be lowercased).
 * @returns {Promise<string>} - DLQ queue URL.
 */
async function getCountyDlqUrl(county) {
  const queueName = `elephant-workflow-queue-${county.toLowerCase()}-dlq`;
  try {
    const response = await sqsClient.send(
      new GetQueueUrlCommand({ QueueName: queueName }),
    );
    if (!response.QueueUrl) {
      throw new Error(`DLQ queue ${queueName} not found`);
    }
    return response.QueueUrl;
  } catch (error) {
    throw new Error(
      `Failed to get DLQ URL for county ${county}: ${error.message}`,
    );
  }
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

  const execution = /** @type {FailedExecutionItem} */ (response.Attributes);

  return {
    openErrorCount: execution.openErrorCount ?? 0,
    status: execution.status ?? "failed",
  };
}

/**
 * Get execution item without updating it.
 *
 * @param {object} params - Query parameters.
 * @param {DynamoDBDocumentClient} params.client - DynamoDB document client.
 * @param {string} params.tableName - DynamoDB table name.
 * @param {string} params.executionId - Execution identifier.
 * @returns {Promise<FailedExecutionItem>} - Execution item.
 */
async function getExecution({
  client,
  tableName,
  executionId,
}) {
  const response = await client.send(
    new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: "PK = :pk AND SK = :sk",
      ExpressionAttributeValues: {
        ":pk": buildExecutionPk(executionId),
        ":sk": buildExecutionSk(executionId),
      },
      Limit: 1,
    }),
  );

  if (!response.Items || response.Items.length === 0) {
    throw new Error(`Execution ${executionId} not found`);
  }

  return /** @type {FailedExecutionItem} */ (response.Items[0]);
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
      UpdateExpression: "SET #status = :maybeSolved, #updatedAt = :updatedAt",
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
 * @returns {Promise<{ status: string, transactionItems?: unknown[] }>} - Result payload from post-processing Lambda.
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
    saveErrorsOnValidationFailure: false,
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

  console.log(
    `Post-processing Lambda returned status: ${resultPayload.status} with ${resultPayload.transactionItems?.length || 0} transaction items`,
  );

  return resultPayload;
}

/**
 * Send transaction items to the Transactions SQS queue.
 *
 * @param {string} queueUrl - Transactions SQS queue URL.
 * @param {unknown[]} transactionItems - Array of transaction items to send.
 * @returns {Promise<void>}
 */
async function sendToTransactionsQueue(queueUrl, transactionItems) {
  console.log(
    `Sending ${transactionItems.length} transaction items to Transactions queue`,
  );
  await sqsClient.send(
    new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: JSON.stringify(transactionItems),
    }),
  );
  console.log("Successfully sent transaction items to Transactions queue");
}

/**
 * Send original message to county-specific DLQ.
 *
 * @param {string} dlqUrl - County-specific DLQ URL.
 * @param {Record<string, string | undefined>} source - Source information with s3Bucket and s3Key.
 * @returns {Promise<void>}
 */
async function sendToDlq(dlqUrl, source) {
  if (!source.s3Bucket || !source.s3Key) {
    throw new Error("Cannot send to DLQ: source missing s3Bucket or s3Key");
  }

  const message = {
    s3: {
      bucket: { name: source.s3Bucket },
      object: { key: source.s3Key },
    },
  };

  console.log(`Sending original message to DLQ: ${dlqUrl}`);
  await sqsClient.send(
    new SendMessageCommand({
      QueueUrl: dlqUrl,
      MessageBody: JSON.stringify(message),
    }),
  );
  console.log("Successfully sent message to DLQ");
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
 * Query all ExecutionErrorLink items for a specific execution.
 *
 * @param {object} params - Query parameters.
 * @param {DynamoDBDocumentClient} params.client - DynamoDB document client.
 * @param {string} params.tableName - DynamoDB table name.
 * @param {string} params.executionId - Execution identifier.
 * @returns {Promise<ExecutionErrorLink[]>} - Array of all error links for the execution.
 */
async function queryExecutionErrorLinks({ client, tableName, executionId }) {
  const executionPk = buildExecutionPk(executionId);
  /** @type {ExecutionErrorLink[]} */
  const errorLinks = [];
  /** @type {Record<string, unknown> | undefined} */
  let lastEvaluatedKey = undefined;

  do {
    const response = await client.send(
      new QueryCommand({
        TableName: tableName,
        KeyConditionExpression: "PK = :pk AND begins_with(SK, :skPrefix)",
        ExpressionAttributeValues: {
          ":pk": executionPk,
          ":skPrefix": "ERROR#",
        },
        FilterExpression: "#entityType = :entityType",
        ExpressionAttributeNames: {
          "#entityType": "entityType",
        },
        ExclusiveStartKey: lastEvaluatedKey,
      }),
    );

    if (response.Items && response.Items.length > 0) {
      errorLinks.push(
        ...response.Items.map(
          (item) => /** @type {ExecutionErrorLink} */(item),
        ),
      );
    }

    lastEvaluatedKey = response.LastEvaluatedKey;
  } while (lastEvaluatedKey);

  return errorLinks;
}

/**
 * Check if all error links for an execution are maybeSolved or if any are maybeUnrecoverable.
 *
 * @param {ExecutionErrorLink[]} errorLinks - Array of error links for the execution.
 * @returns {{ allSolved: boolean, hasUnrecoverable: boolean }} - Status assessment result.
 */
function assessExecutionErrorStatus(errorLinks) {
  if (errorLinks.length === 0) {
    return { allSolved: true, hasUnrecoverable: false };
  }

  const hasUnrecoverable = errorLinks.some(
    (link) => link.status === "maybeUnrecoverable",
  );
  const allSolved = errorLinks.every(
    (link) => link.status === "maybeSolved" || link.status === "solved",
  );

  return { allSolved, hasUnrecoverable };
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
    const postProcessorFunctionName = requireEnv(
      "POST_PROCESSOR_FUNCTION_NAME",
    );
    const transactionsQueueUrl = requireEnv("TRANSACTIONS_SQS_QUEUE_URL");
    const client = DEFAULT_CLIENT;

    // Group ExecutionErrorLink records by executionId
    /** @type {Map<string, ExecutionErrorLink[]>} */
    const executionGroups = new Map();

    for (const record of event.Records ?? []) {
      if (!record.dynamodb?.NewImage) {
        continue;
      }

      const decodedNew = unmarshall(record.dynamodb.NewImage);
      const decodedOld = record.dynamodb.OldImage
        ? unmarshall(record.dynamodb.OldImage)
        : undefined;

      // Skip if not an ExecutionErrorLink with status transition to "maybeSolved" or "maybeUnrecoverable"
      if (
        typeof decodedNew.entityType !== "string" ||
        decodedNew.entityType !== "ExecutionError"
      ) {
        continue;
      }

      const newStatus = decodedNew.status;
      const oldStatus = decodedOld?.status;

      // Process items that changed from "failed" to "maybeSolved" or "maybeUnrecoverable"
      if (
        oldStatus !== "failed" ||
        (newStatus !== "maybeSolved" && newStatus !== "maybeUnrecoverable")
      ) {
        continue;
      }

      const executionId = extractExecutionId(decodedNew);
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

      const errorLink = /** @type {ExecutionErrorLink} */ (decodedNew);
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

    for (const [executionId, streamErrorLinks] of executionGroups) {
      console.log(
        JSON.stringify({
          ...logBase,
          level: "info",
          msg: "processing_execution",
          executionId,
          errorLinkCount: streamErrorLinks.length,
        }),
      );

      // Process each error link (each one decrements the count)
      for (const _errorLink of streamErrorLinks) {
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

          // If count reached 0 and status is still "failed", check error statuses and decide next action
          if (openErrorCount === 0 && status === "failed") {
            console.log(
              JSON.stringify({
                ...logBase,
                level: "info",
                msg: "all_errors_resolved_checking_statuses",
                executionId,
              }),
            );

            // Query all error links for this execution to check their statuses
            const errorLinks = await queryExecutionErrorLinks({
              client,
              tableName,
              executionId,
            });

            const { allSolved, hasUnrecoverable } =
              assessExecutionErrorStatus(errorLinks);

            console.log(
              JSON.stringify({
                ...logBase,
                level: "info",
                msg: "error_status_assessment",
                executionId,
                errorLinkCount: errorLinks.length,
                allSolved,
                hasUnrecoverable,
              }),
            );

            // Get execution details without updating status first
            const execution = await getExecution({
              client,
              tableName,
              executionId,
            });

            // If any errors are maybeUnrecoverable, send to DLQ instead of restarting
            if (hasUnrecoverable) {
              console.log(
                JSON.stringify({
                  ...logBase,
                  level: "info",
                  msg: "execution_has_unrecoverable_errors_sending_to_dlq",
                  executionId,
                }),
              );

              if (!execution.source) {
                throw new Error(
                  `Execution ${executionId} has unrecoverable errors but source information is missing, cannot send to DLQ`,
                );
              }

              const dlqUrl = await getCountyDlqUrl(execution.county);
              await sendToDlq(dlqUrl, execution.source);

              console.log(
                JSON.stringify({
                  ...logBase,
                  level: "info",
                  msg: "execution_sent_to_dlq_due_to_unrecoverable_errors",
                  executionId,
                  county: execution.county,
                  dlqUrl,
                }),
              );

              // Mark as restarted to prevent duplicate processing
              restartedExecutions.add(executionId);
              continue;
            }

            // If not all errors are solved, skip restart (shouldn't happen if count is 0, but defensive check)
            if (!allSolved) {
              console.log(
                JSON.stringify({
                  ...logBase,
                  level: "warn",
                  msg: "execution_has_unsolved_errors_but_count_is_zero",
                  executionId,
                }),
              );
              restartedExecutions.add(executionId);
              continue;
            }

            // All errors are solved, update status and proceed with restart
            console.log(
              JSON.stringify({
                ...logBase,
                level: "info",
                msg: "all_errors_solved_proceeding_with_restart",
                executionId,
              }),
            );

            // Update execution status to "maybeSolved" before restarting
            const updatedExecution = await updateExecutionStatusToMaybeSolved({
              client,
              tableName,
              executionId,
            });

            if (!updatedExecution.preparedS3Uri) {
              throw new Error(
                `Execution ${executionId} missing preparedS3Uri, cannot restart`,
              );
            }

            const seedOutputS3Uri = constructSeedOutputS3Uri(
              updatedExecution.preparedS3Uri,
            );

            // Build S3 event from source
            const s3Event = updatedExecution.source
              ? {
                bucket: { name: updatedExecution.source.s3Bucket },
                object: { key: updatedExecution.source.s3Key },
              }
              : undefined;

            // Invoke post-processing Lambda to restart execution
            let resultPayload;
            try {
              resultPayload = await invokePostProcessingLambda({
                functionName: postProcessorFunctionName,
                preparedS3Uri: execution.preparedS3Uri,
                seedOutputS3Uri,
                s3Event,
              });

              // Check if execution was successful
              if (
                resultPayload.status === "success" &&
                Array.isArray(resultPayload.transactionItems) &&
                resultPayload.transactionItems.length > 0
              ) {
                // Send successful results to Transactions queue
                await sendToTransactionsQueue(
                  transactionsQueueUrl,
                  resultPayload.transactionItems,
                );

                console.log(
                  JSON.stringify({
                    ...logBase,
                    level: "info",
                    msg: "execution_succeeded_and_sent_to_transactions_queue",
                    executionId,
                    transactionItemsCount:
                      resultPayload.transactionItems.length,
                  }),
                );
              } else {
                // Execution failed, send original message to DLQ
                if (!updatedExecution.source) {
                  throw new Error(
                    `Execution ${executionId} failed but source information is missing, cannot send to DLQ`,
                  );
                }

                const dlqUrl = await getCountyDlqUrl(updatedExecution.county);
                await sendToDlq(dlqUrl, updatedExecution.source);

                console.log(
                  JSON.stringify({
                    ...logBase,
                    level: "info",
                    msg: "execution_failed_and_sent_to_dlq",
                    executionId,
                    county: execution.county,
                    dlqUrl,
                  }),
                );
              }
            } catch (lambdaError) {
              // If Lambda invocation failed, send original message to DLQ
              if (updatedExecution.source) {
                try {
                  const dlqUrl = await getCountyDlqUrl(updatedExecution.county);
                  await sendToDlq(dlqUrl, updatedExecution.source);

                  console.log(
                    JSON.stringify({
                      ...logBase,
                      level: "warn",
                      msg: "execution_invocation_failed_and_sent_to_dlq",
                      executionId,
                      county: updatedExecution.county,
                      dlqUrl,
                      error: String(lambdaError),
                    }),
                  );
                } catch (dlqError) {
                  console.error(
                    JSON.stringify({
                      ...logBase,
                      level: "error",
                      msg: "failed_to_send_to_dlq",
                      executionId,
                      county: updatedExecution.county,
                      dlqError: String(dlqError),
                      originalError: String(lambdaError),
                    }),
                  );
                  throw lambdaError;
                }
              } else {
                throw lambdaError;
              }
            }

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
