import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  UpdateCommand,
  QueryCommand,
  GetCommand,
} from "@aws-sdk/lib-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import {
  SQSClient,
  SendMessageCommand,
  GetQueueUrlCommand,
} from "@aws-sdk/client-sqs";
import {
  CloudWatchClient,
  PutMetricDataCommand,
  StandardUnit,
} from "@aws-sdk/client-cloudwatch";

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
 * @typedef {object} DynamoDBStreamRecordData
 * @property {Record<string, import("@aws-sdk/client-dynamodb").AttributeValue>} [Keys] - Key attributes.
 * @property {Record<string, import("@aws-sdk/client-dynamodb").AttributeValue>} [OldImage] - Previous item state.
 * @property {Record<string, import("@aws-sdk/client-dynamodb").AttributeValue>} [NewImage] - Current item state.
 */

/**
 * @typedef {object} DynamoDBStreamRecord
 * @property {string} eventName - Event type (INSERT, MODIFY, REMOVE).
 * @property {DynamoDBStreamRecordData} [dynamodb] - DynamoDB stream record data.
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
// Use AWS_REGION from environment if available, otherwise use default
const cloudWatchClient = new CloudWatchClient({
  region: process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION,
});

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
 * Publish a CloudWatch metric.
 *
 * @param {object} params - Metric parameters.
 * @param {string} params.metricName - Name of the metric.
 * @param {number} [params.value] - Metric value (default: 1).
 * @param {string} [params.unit] - Unit of measurement (default: "Count").
 * @param {Record<string, string>} [params.dimensions] - Optional dimensions for the metric.
 * @param {string} [params.namespace] - Optional namespace override (defaults to ExecutionRestart).
 * @returns {Promise<void>}
 */
async function publishMetric({
  metricName,
  value = 1,
  unit = "Count",
  dimensions = {},
  namespace,
}) {
  const metricNamespace =
    namespace || process.env.CLOUDWATCH_METRIC_NAMESPACE || "ExecutionRestart";
  /** @type {{ MetricName: string; Value: number; Unit: StandardUnit; Timestamp: Date; Dimensions?: Array<{ Name: string; Value: string }> }} */
  const metricData = {
    MetricName: metricName,
    Value: value,
    Unit: /** @type {StandardUnit} */ (unit),
    Timestamp: new Date(),
  };

  if (Object.keys(dimensions).length > 0) {
    metricData.Dimensions = Object.entries(dimensions).map(([Name, Value]) => ({
      Name,
      Value: String(Value),
    }));
  }

  try {
    await cloudWatchClient.send(
      new PutMetricDataCommand({
        Namespace: metricNamespace,
        MetricData: [metricData],
      }),
    );
    console.log(
      `Published metric: ${metricNamespace}/${metricName} = ${value} (${unit})`,
    );
  } catch (error) {
    // Silently fail - metrics are optional and should not break the workflow
  }
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
    const errorMessage = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Failed to get DLQ URL for county ${county}: ${errorMessage}`,
    );
  }
}

/**
 * Atomically decrement openErrorCount for a failed execution.
 * Only decrements if openErrorCount > 0 to prevent negative values.
 *
 * @param {object} params - Update parameters.
 * @param {DynamoDBDocumentClient} params.client - DynamoDB document client.
 * @param {string} params.tableName - DynamoDB table name.
 * @param {string} params.executionId - Execution identifier.
 * @returns {Promise<{ openErrorCount: number, status: string }>} - Updated openErrorCount and status.
 */
async function decrementOpenErrorCount({ client, tableName, executionId }) {
  const now = new Date().toISOString();

  try {
    // Attempt to decrement atomically, but only if openErrorCount > 0
    const response = await client.send(
      new UpdateCommand({
        TableName: tableName,
        Key: {
          PK: buildExecutionPk(executionId),
          SK: buildExecutionSk(executionId),
        },
        UpdateExpression:
          "ADD #openErrorCount :decrement SET #updatedAt = :updatedAt",
        ConditionExpression: "#openErrorCount > :zero",
        ExpressionAttributeNames: {
          "#openErrorCount": "openErrorCount",
          "#updatedAt": "updatedAt",
        },
        ExpressionAttributeValues: {
          ":decrement": -1,
          ":zero": 0,
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
  } catch (error) {
    // If condition check failed (count is already <= 0), get current values and return them
    if (
      error instanceof Error &&
      (error.name === "ConditionalCheckFailedException" ||
        error.message?.includes("ConditionalCheckFailedException") ||
        error.message?.includes("conditional"))
    ) {
      // Count is already 0 or negative, get current execution and return without decrementing
      const currentExecution = await getExecution({
        client,
        tableName,
        executionId,
      });

      return {
        openErrorCount: currentExecution.openErrorCount ?? 0,
        status: currentExecution.status ?? "failed",
      };
    }

    // Re-throw other errors
    throw error;
  }
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
async function getExecution({ client, tableName, executionId }) {
  const response = await client.send(
    new GetCommand({
      TableName: tableName,
      Key: {
        PK: buildExecutionPk(executionId),
        SK: buildExecutionSk(executionId),
      },
    }),
  );

  if (!response.Item) {
    throw new Error(`Execution ${executionId} not found`);
  }

  return /** @type {FailedExecutionItem} */ (response.Item);
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
 * Invoke transform and SVL workers to validate if transformation script fixes worked.
 * This replaces the old invokePostProcessingLambda which called the monolithic post lambda.
 *
 * @param {object} params - Invocation parameters.
 * @param {string} params.transformFunctionName - Transform worker Lambda function name or ARN.
 * @param {string} params.svlFunctionName - SVL worker Lambda function name or ARN.
 * @param {string} params.preparedS3Uri - S3 URI of the prepared output (input for transform).
 * @param {string} params.county - County name.
 * @param {string} params.executionId - Execution identifier.
 * @param {string} params.outputPrefix - S3 prefix for output files.
 * @returns {Promise<{ status: string, validationPassed: boolean }>} - Result indicating if validation passed.
 */
async function invokeTransformAndSvlWorkers({
  transformFunctionName,
  svlFunctionName,
  preparedS3Uri,
  county,
  executionId,
  outputPrefix,
}) {
  console.log(
    `Starting transform and SVL validation for execution ${executionId}...`,
  );
  console.log(`Prepared S3 URI: ${preparedS3Uri}`);

  // Step 1: Invoke transform worker
  console.log(`Invoking transform worker ${transformFunctionName}...`);

  const transformPayload = {
    inputS3Uri: preparedS3Uri,
    county,
    outputPrefix,
    executionId,
    directInvocation: true,
  };

  const transformResponse = await lambdaClient.send(
    new InvokeCommand({
      FunctionName: transformFunctionName,
      InvocationType: "RequestResponse",
      Payload: JSON.stringify(transformPayload),
    }),
  );

  if (transformResponse.FunctionError) {
    const errorPayload = JSON.parse(
      new TextDecoder().decode(transformResponse.Payload ?? new Uint8Array()),
    );
    throw new Error(
      `Transform worker failed: ${errorPayload.errorMessage || errorPayload.errorType || JSON.stringify(errorPayload)}`,
    );
  }

  const transformResult = JSON.parse(
    new TextDecoder().decode(transformResponse.Payload ?? new Uint8Array()),
  );

  console.log(
    `Transform worker completed. Output: ${transformResult.transformedOutputS3Uri}`,
  );

  // Step 2: Invoke SVL worker with transform output
  console.log(`Invoking SVL worker ${svlFunctionName}...`);

  const svlPayload = {
    transformedOutputS3Uri: transformResult.transformedOutputS3Uri,
    county,
    outputPrefix,
    executionId,
    directInvocation: true,
  };

  const svlResponse = await lambdaClient.send(
    new InvokeCommand({
      FunctionName: svlFunctionName,
      InvocationType: "RequestResponse",
      Payload: JSON.stringify(svlPayload),
    }),
  );

  if (svlResponse.FunctionError) {
    const errorPayload = JSON.parse(
      new TextDecoder().decode(svlResponse.Payload ?? new Uint8Array()),
    );
    // SVL validation failure - this is expected when fixes don't work
    console.log(
      `SVL validation failed: ${errorPayload.errorMessage || errorPayload.errorType}`,
    );
    return {
      status: "validation_failed",
      validationPassed: false,
    };
  }

  const svlResult = JSON.parse(
    new TextDecoder().decode(svlResponse.Payload ?? new Uint8Array()),
  );

  console.log(
    `SVL worker completed. Validation passed: ${svlResult.validationPassed}`,
  );

  return {
    status: svlResult.validationPassed ? "success" : "validation_failed",
    validationPassed: svlResult.validationPassed,
  };
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
    /** @type {{ Items?: Record<string, unknown>[]; LastEvaluatedKey?: Record<string, unknown> }} */
    const response = await client.send(
      new QueryCommand({
        TableName: tableName,
        KeyConditionExpression: "PK = :pk AND begins_with(SK, :skPrefix)",
        ExpressionAttributeValues: {
          ":pk": executionPk,
          ":skPrefix": "ERROR#",
          ":entityType": "ExecutionError",
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
          (item) => /** @type {ExecutionErrorLink} */ (item),
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
    const transformFunctionName = requireEnv("TRANSFORM_WORKER_FUNCTION_NAME");
    const svlFunctionName = requireEnv("SVL_WORKER_FUNCTION_NAME");
    const outputPrefix = requireEnv("OUTPUT_S3_PREFIX");
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

          // If count reached 0 or below and status is "failed" or "maybeSolved", check error statuses and decide next action
          // Also handle cases where count is negative (due to multiple decrements) but status indicates errors are solved
          if (
            openErrorCount <= 0 &&
            (status === "failed" || status === "maybeSolved")
          ) {
            console.log(
              JSON.stringify({
                ...logBase,
                level: "info",
                msg: "all_errors_resolved_checking_statuses",
                executionId,
                openErrorCount,
                status,
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

              const execution = await getExecution({
                client,
                tableName,
                executionId,
              });

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

            // If not all errors are solved, skip restart (shouldn't happen if count is <= 0, but defensive check)
            if (!allSolved) {
              console.log(
                JSON.stringify({
                  ...logBase,
                  level: "warn",
                  msg: "execution_has_unsolved_errors_but_count_is_zero_or_negative",
                  executionId,
                  openErrorCount,
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

            // Get execution details (update status to "maybeSolved" if not already)
            let updatedExecution;
            if (status === "maybeSolved") {
              updatedExecution = await getExecution({
                client,
                tableName,
                executionId,
              });
            } else {
              updatedExecution = await updateExecutionStatusToMaybeSolved({
                client,
                tableName,
                executionId,
              });
            }

            if (!updatedExecution.preparedS3Uri) {
              throw new Error(
                `Execution ${executionId} missing preparedS3Uri, cannot restart`,
              );
            }

            // Invoke transform and SVL workers to validate if fixes worked
            let resultPayload;
            try {
              resultPayload = await invokeTransformAndSvlWorkers({
                transformFunctionName,
                svlFunctionName,
                preparedS3Uri: updatedExecution.preparedS3Uri,
                county: updatedExecution.county,
                executionId,
                outputPrefix,
              });

              // Check if validation was successful
              if (resultPayload.validationPassed) {
                // Publish success metric
                await publishMetric({
                  metricName: "ExecutionRestartSuccess",
                  dimensions: {
                    County: updatedExecution.county,
                  },
                });

                console.log(
                  JSON.stringify({
                    ...logBase,
                    level: "info",
                    msg: "execution_validation_succeeded",
                    executionId,
                  }),
                );
              } else {
                // Validation failed, send original message to DLQ
                if (!updatedExecution.source) {
                  throw new Error(
                    `Execution ${executionId} validation failed but source information is missing, cannot send to DLQ`,
                  );
                }

                const dlqUrl = await getCountyDlqUrl(updatedExecution.county);
                await sendToDlq(dlqUrl, updatedExecution.source);

                // Publish failure metric
                await publishMetric({
                  metricName: "ExecutionRestartFailure",
                  dimensions: {
                    County: updatedExecution.county,
                    FailureReason: "ValidationFailed",
                  },
                });

                console.log(
                  JSON.stringify({
                    ...logBase,
                    level: "info",
                    msg: "execution_validation_failed_and_sent_to_dlq",
                    executionId,
                    county: updatedExecution.county,
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

                  // Publish failure metric
                  await publishMetric({
                    metricName: "ExecutionRestartFailure",
                    dimensions: {
                      County: updatedExecution.county,
                      FailureReason: "WorkerInvocationFailed",
                    },
                  });

                  console.log(
                    JSON.stringify({
                      ...logBase,
                      level: "warn",
                      msg: "execution_worker_invocation_failed_and_sent_to_dlq",
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
                // Publish failure metric even without source
                await publishMetric({
                  metricName: "ExecutionRestartFailure",
                  dimensions: {
                    County: updatedExecution.county || "Unknown",
                    FailureReason: "WorkerInvocationFailedNoSource",
                  },
                });
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
