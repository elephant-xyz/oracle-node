#!/usr/bin/env node

/**
 * Migration script to backfill workflow-state DynamoDB table for RUNNING Step Functions executions.
 *
 * This script:
 * 1. Discovers the ElephantExpress state machine ARN and workflow-state table name from CloudFormation outputs
 * 2. Lists all RUNNING executions
 * 3. For each execution, extracts current phase/step/status from execution history
 * 4. Writes execution state + aggregates to DynamoDB with strict no-overwrite protection
 *
 * Safety: Never overwrites a newer status already written by the live EventBridge handler.
 */

const {
    CloudFormationClient,
    DescribeStacksCommand,
} = require("@aws-sdk/client-cloudformation");
const {
    SFNClient,
    ListExecutionsCommand,
    GetExecutionHistoryCommand,
} = require("@aws-sdk/client-sfn");
const {
    DynamoDBDocumentClient,
    GetCommand,
    TransactWriteCommand,
} = require("@aws-sdk/lib-dynamodb");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const yargs = require("yargs/yargs");
const { hideBin } = require("yargs/helpers");
const chalk = require("chalk");

// =============================================================================
// Types (matching workflow-events shared types)
// =============================================================================

/**
 * @typedef {Object} WorkflowEventDetail
 * @property {string} executionId - Unique identifier for the workflow execution
 * @property {string} county - County identifier
 * @property {"SUCCEEDED" | "FAILED" | "PARKED" | "IN_PROGRESS" | "SCHEDULED"} status - Current workflow step status
 * @property {string} phase - Current phase of the workflow execution
 * @property {string} step - Current step within the phase
 * @property {string} [dataGroupLabel] - Data group used for the workflow execution
 * @property {string} [taskToken] - Task token for Step Functions callback (if applicable)
 * @property {Array<{code: string, details: Record<string, unknown>}>} errors - Array of errors encountered
 */

/**
 * @typedef {Object} ExecutionStateItem
 * @property {string} PK - Primary key: `EXECUTION#${executionId}`
 * @property {string} SK - Sort key: `EXECUTION#${executionId}`
 * @property {"ExecutionState"} entityType - Entity discriminator
 * @property {string} executionId - Unique identifier for the workflow execution
 * @property {string} county - County identifier
 * @property {string} dataGroupLabel - Data group label (or "not-set" if not provided)
 * @property {string} phase - Current phase of the workflow execution
 * @property {string} step - Current step within the phase
 * @property {"IN_PROGRESS" | "FAILED" | "SUCCEEDED"} bucket - Normalized status bucket
 * @property {string} rawStatus - Original raw status from the event
 * @property {string} lastEventTime - ISO timestamp of the EventBridge event time
 * @property {string} createdAt - ISO timestamp when the execution state was created
 * @property {string} updatedAt - ISO timestamp when the execution state was last updated
 * @property {number} version - Optimistic concurrency version number
 */

/**
 * @typedef {Object} StateMapping
 * @property {string} phase - Phase name inferred from state name
 * @property {string} step - Step name inferred from state name
 */

/**
 * @typedef {Object} MigrationResult
 * @property {string} executionArn - Execution ARN
 * @property {"updated" | "skipped_newer" | "skipped_no_data" | "failed"} status - Migration status
 * @property {string} [error] - Error message if status is "failed"
 * @property {string} [reason] - Reason for skip if status is "skipped_*"
 */

/**
 * @typedef {Object} MigrationSummary
 * @property {number} totalRunning - Total RUNNING executions found
 * @property {number} updated - Number successfully updated
 * @property {number} skippedNewer - Number skipped (newer status already exists)
 * @property {number} skippedNoData - Number skipped (could not extract status)
 * @property {number} failed - Number failed
 * @property {Array<MigrationResult>} results - Detailed results per execution
 */

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_PREPARE_STACK_NAME = "elephant-oracle-node";
const DEFAULT_WORKFLOW_EVENTS_STACK_NAME = "workflow-events-stack";
const DEFAULT_REGION = "us-east-1";
const DEFAULT_CONCURRENCY = 10;
const DEFAULT_MAX_EXECUTIONS = undefined; // No limit by default

const STATE_ENTITY_TYPES = {
    EXECUTION_STATE: "ExecutionState",
    STEP_AGGREGATE: "StepAggregate",
};

const DEFAULT_DATA_GROUP_LABEL = "not-set";
const GSI2_SHARD_COUNT = 16;

// =============================================================================
// State name to phase/step mapping (fallback when no putEvents found)
// =============================================================================

/**
 * Maps Step Functions state names to phase/step pairs.
 * Based on workflow/state-machines/elephant-express.asl.yaml
 *
 * @type {Record<string, StateMapping>}
 */
const STATE_NAME_TO_PHASE_STEP = {
    Preprocess: { phase: "Preprocess", step: "Preprocess" },
    PrepareDirectSubmitInput: { phase: "Submit", step: "PrepareDirectSubmitInput" },
    EmitPrepareScheduled: { phase: "Prepare", step: "Prepare" },
    Prepare: { phase: "Prepare", step: "Prepare" },
    EmitTransformScheduled: { phase: "Transform", step: "Transform" },
    Transform: { phase: "Transform", step: "Transform" },
    EmitSvlScheduled: { phase: "SVL", step: "SVL" },
    SVL: { phase: "SVL", step: "SVL" },
    EmitHashScheduled: { phase: "Hash", step: "Hash" },
    Hash: { phase: "Hash", step: "Hash" },
    EmitUploadScheduled: { phase: "Upload", step: "Upload" },
    Upload: { phase: "Upload", step: "Upload" },
    EmitSubmitScheduled: { phase: "Submit", step: "Submit" },
    EmitGasPriceCheckScheduled: { phase: "GasPriceCheck", step: "CheckGasPrice" },
    CheckGasPrice: { phase: "GasPriceCheck", step: "CheckGasPrice" },
    EmitSubmitToBlockchainScheduled: { phase: "Submit", step: "SubmitToBlockchain" },
    SubmitToBlockchain: { phase: "Submit", step: "SubmitToBlockchain" },
    EmitTransactionStatusCheckScheduled: {
        phase: "TransactionStatusCheck",
        step: "CheckTransactionStatus",
    },
    CheckTransactionStatus: {
        phase: "TransactionStatusCheck",
        step: "CheckTransactionStatus",
    },
    WaitForPreprocessResolution: { phase: "Preprocess", step: "Preprocess" },
    WaitForTransformResolution: { phase: "Transform", step: "Transform" },
    WaitForSvlValidationResolution: { phase: "SVL", step: "SVL" },
    WaitForSvlExceptionResolution: { phase: "SVL", step: "SVL" },
    WaitForHashResolution: { phase: "Hash", step: "Hash" },
    WaitForUploadResolution: { phase: "Upload", step: "Upload" },
    WaitForGasPriceCheckResolution: {
        phase: "GasPriceCheck",
        step: "CheckGasPrice",
    },
    WaitForSubmitResolution: { phase: "Submit", step: "SubmitToBlockchain" },
    WaitForTransactionStatusCheckResolution: {
        phase: "TransactionStatusCheck",
        step: "CheckTransactionStatus",
    },
};

// =============================================================================
// Logging utilities
// =============================================================================

const log = {
    info: (msg) => console.log(chalk.green("[INFO]"), msg),
    warn: (msg) => console.log(chalk.yellow("[WARN]"), msg),
    error: (msg) => console.log(chalk.red("[ERROR]"), msg),
    debug: (msg) => console.log(chalk.blue("[DEBUG]"), msg),
};

// =============================================================================
// CloudFormation discovery
// =============================================================================

/**
 * Gets a CloudFormation stack output value by key.
 *
 * @param {CloudFormationClient} cfnClient - CloudFormation client
 * @param {string} stackName - Stack name
 * @param {string} outputKey - Output key to retrieve
 * @returns {Promise<string>} Output value
 * @throws {Error} If stack not found or output key missing
 */
async function getStackOutput(cfnClient, stackName, outputKey) {
    const command = new DescribeStacksCommand({ StackName: stackName });
    const response = await cfnClient.send(command);

    if (!response.Stacks || response.Stacks.length === 0) {
        throw new Error(`Stack not found: ${stackName}`);
    }

    const stack = response.Stacks[0];
    const output = stack.Outputs?.find((o) => o.OutputKey === outputKey);

    if (!output || !output.OutputValue) {
        throw new Error(
            `Output key "${outputKey}" not found in stack "${stackName}"`,
        );
    }

    return output.OutputValue;
}

/**
 * Discovers required resources from CloudFormation stacks.
 *
 * @param {CloudFormationClient} cfnClient - CloudFormation client
 * @param {string} prepareStackName - Prepare stack name
 * @param {string} workflowEventsStackName - Workflow events stack name
 * @returns {Promise<{stateMachineArn: string, tableName: string}>} Discovered resources
 */
async function discoverResources(
    cfnClient,
    prepareStackName,
    workflowEventsStackName,
) {
    log.info(`Discovering resources from stacks...`);
    log.debug(`  Prepare stack: ${prepareStackName}`);
    log.debug(`  Workflow events stack: ${workflowEventsStackName}`);

    const [stateMachineArn, tableName] = await Promise.all([
        getStackOutput(cfnClient, prepareStackName, "ElephantExpressStateMachineArn"),
        getStackOutput(
            cfnClient,
            workflowEventsStackName,
            "WorkflowStateTableName",
        ),
    ]);

    log.info(`  State machine ARN: ${stateMachineArn}`);
    log.info(`  Table name: ${tableName}`);

    return { stateMachineArn, tableName };
}

// =============================================================================
// Step Functions execution listing
// =============================================================================

/**
 * Lists all RUNNING executions for a state machine.
 *
 * @param {SFNClient} sfnClient - Step Functions client
 * @param {string} stateMachineArn - State machine ARN
 * @param {number} [maxExecutions] - Maximum number of executions to process
 * @returns {Promise<Array<{executionArn: string, name: string}>>} List of execution ARNs and names
 */
async function listRunningExecutions(sfnClient, stateMachineArn, maxExecutions) {
    log.info(`Listing RUNNING executions for state machine...`);

    const executions = [];
    let nextToken;

    do {
        const command = new ListExecutionsCommand({
            stateMachineArn,
            statusFilter: "RUNNING",
            maxResults: 100,
            nextToken,
        });

        const response = await sfnClient.send(command);

        if (response.executions) {
            executions.push(
                ...response.executions.map((e) => ({
                    executionArn: e.executionArn || "",
                    name: e.name || "",
                })),
            );
        }

        nextToken = response.nextToken;

        if (maxExecutions && executions.length >= maxExecutions) {
            executions.splice(maxExecutions);
            break;
        }
    } while (nextToken);

    log.info(`  Found ${executions.length} RUNNING execution(s)`);
    return executions;
}

// =============================================================================
// Execution history parsing
// =============================================================================

/**
 * Extracts county from execution input JSON.
 *
 * @param {string} inputJson - Execution input JSON string
 * @returns {string} County name or "unknown"
 */
function extractCountyFromInput(inputJson) {
    try {
        const input = JSON.parse(inputJson);
        // Try common paths: message.county, message.pre.county_name, county
        return (
            input?.message?.county ||
            input?.message?.pre?.county_name ||
            input?.county ||
            "unknown"
        );
    } catch {
        return "unknown";
    }
}

/**
 * Extracts data group label from execution input JSON.
 *
 * @param {string} inputJson - Execution input JSON string
 * @returns {string | undefined} Data group label or undefined
 */
function extractDataGroupLabelFromInput(inputJson) {
    try {
        const input = JSON.parse(inputJson);
        return (
            input?.message?.dataGroupLabel ||
            input?.dataGroupLabel ||
            undefined
        );
    } catch {
        return undefined;
    }
}

/**
 * Extracts WorkflowEventDetail from TaskScheduledEventDetails parameters.
 *
 * @param {string} parametersJson - TaskScheduledEventDetails.parameters JSON string
 * @returns {WorkflowEventDetail | null} Extracted detail or null if not found
 */
function extractWorkflowEventFromTaskParameters(parametersJson) {
    try {
        const params = JSON.parse(parametersJson);
        const entries = params.Entries || [];

        if (entries.length === 0) {
            return null;
        }

        const entry = entries[0];
        const detail = entry.Detail;

        if (!detail || entry.DetailType !== "WorkflowEvent") {
            return null;
        }

        return {
            executionId: detail.executionId || "",
            county: detail.county || "unknown",
            status: detail.status || "IN_PROGRESS",
            phase: detail.phase || "",
            step: detail.step || "",
            dataGroupLabel: detail.dataGroupLabel,
            taskToken: detail.taskToken,
            errors: detail.errors || [],
        };
    } catch (error) {
        log.debug(`Failed to parse task parameters: ${error.message}`);
        return null;
    }
}

/**
 * Infers phase/step from state name using mapping.
 *
 * @param {string} stateName - State name from StateEnteredEventDetails
 * @returns {StateMapping | null} Inferred phase/step or null if not mapped
 */
function inferPhaseStepFromStateName(stateName) {
    return STATE_NAME_TO_PHASE_STEP[stateName] || null;
}

/**
 * Extracts current execution status from execution history.
 *
 * @param {SFNClient} sfnClient - Step Functions client
 * @param {string} executionArn - Execution ARN
 * @param {string} executionInput - Execution input JSON (from ExecutionStarted)
 * @returns {Promise<{detail: WorkflowEventDetail, eventTime: string} | null>} Extracted status or null
 */
async function extractExecutionStatus(sfnClient, executionArn, executionInput) {
    const command = new GetExecutionHistoryCommand({
        executionArn,
        reverseOrder: true,
        includeExecutionData: true,
        maxResults: 1000, // Get enough history to find putEvents
    });

    const response = await sfnClient.send(command);
    const events = response.events || [];

    // Extract county and dataGroupLabel from input (fallback)
    const county = extractCountyFromInput(executionInput);
    const dataGroupLabel = extractDataGroupLabelFromInput(executionInput);

    // First pass: Look for latest TaskScheduled with events:putEvents resource
    for (const event of events) {
        if (event.type === "TaskScheduled") {
            const details = event.taskScheduledEventDetails;
            if (
                details?.resource &&
                details.resource.startsWith("arn:aws:states:::events:putEvents")
            ) {
                const parameters = details.parameters;
                if (parameters) {
                    const detail = extractWorkflowEventFromTaskParameters(parameters);
                    if (detail) {
                        // Use county/dataGroupLabel from input if not in detail
                        return {
                            detail: {
                                ...detail,
                                county: detail.county !== "unknown" ? detail.county : county,
                                dataGroupLabel:
                                    detail.dataGroupLabel || dataGroupLabel || undefined,
                            },
                            eventTime: new Date(event.timestamp).toISOString(),
                        };
                    }
                }
            }
        }
    }

    // Fallback: Use latest StateEntered to infer phase/step
    for (const event of events) {
        if (event.type === "StateEntered") {
            const details = event.stateEnteredEventDetails;
            const stateName = details?.name;

            if (stateName) {
                const mapping = inferPhaseStepFromStateName(stateName);
                if (mapping) {
                    // Try to extract county from state input if available
                    let stateCounty = county;
                    if (details.input) {
                        const extracted = extractCountyFromInput(details.input);
                        if (extracted !== "unknown") {
                            stateCounty = extracted;
                        }
                    }

                    return {
                        detail: {
                            executionId: executionArn.split(":").pop() || executionArn,
                            county: stateCounty,
                            status: "IN_PROGRESS", // Default for RUNNING executions
                            phase: mapping.phase,
                            step: mapping.step,
                            dataGroupLabel: dataGroupLabel || undefined,
                            errors: [],
                        },
                        eventTime: new Date(event.timestamp).toISOString(),
                    };
                }
            }
        }
    }

    return null;
}

// =============================================================================
// DynamoDB key builders (matching shared/keys.ts)
// =============================================================================

/**
 * Builds execution state PK.
 *
 * @param {string} executionId - Execution identifier
 * @returns {string} PK value
 */
function buildExecutionStatePK(executionId) {
    return `EXECUTION#${executionId}`;
}

/**
 * Builds execution state SK (same as PK).
 *
 * @param {string} executionId - Execution identifier
 * @returns {string} SK value
 */
function buildExecutionStateSK(executionId) {
    return `EXECUTION#${executionId}`;
}

/**
 * Normalizes step status to bucket.
 *
 * @param {string} status - Raw status
 * @returns {"IN_PROGRESS" | "FAILED" | "SUCCEEDED"} Normalized bucket
 */
function normalizeStepStatusToBucket(status) {
    switch (status) {
        case "SUCCEEDED":
            return "SUCCEEDED";
        case "FAILED":
            return "FAILED";
        case "IN_PROGRESS":
        case "SCHEDULED":
        case "PARKED":
            return "IN_PROGRESS";
        default:
            return "IN_PROGRESS";
    }
}

/**
 * Normalizes data group label.
 *
 * @param {string | undefined} dataGroupLabel - Data group label
 * @returns {string} Normalized label
 */
function normalizeDataGroupLabel(dataGroupLabel) {
    return dataGroupLabel && dataGroupLabel.trim() !== ""
        ? dataGroupLabel
        : DEFAULT_DATA_GROUP_LABEL;
}

/**
 * Gets bucket count attribute name.
 *
 * @param {"IN_PROGRESS" | "FAILED" | "SUCCEEDED"} bucket - Bucket
 * @returns {"inProgressCount" | "failedCount" | "succeededCount"} Attribute name
 */
function getBucketCountAttribute(bucket) {
    switch (bucket) {
        case "IN_PROGRESS":
            return "inProgressCount";
        case "FAILED":
            return "failedCount";
        case "SUCCEEDED":
            return "succeededCount";
    }
}

/**
 * Builds step aggregate PK.
 *
 * @param {string} county - County identifier
 * @param {string} dataGroupLabel - Normalized data group label
 * @returns {string} PK value
 */
function buildStepAggregatePK(county, dataGroupLabel) {
    return `AGG#COUNTY#${county}#DG#${dataGroupLabel}`;
}

/**
 * Builds step aggregate SK.
 *
 * @param {string} phase - Phase name
 * @param {string} step - Step name
 * @returns {string} SK value
 */
function buildStepAggregateSK(phase, step) {
    return `PHASE#${phase}#STEP#${step}`;
}

/**
 * Computes shard index for GSI2.
 *
 * @param {string} value - Value to hash
 * @returns {number} Shard index (0-15)
 */
function computeShardIndex(value) {
    let hash = 0;
    for (let i = 0; i < value.length; i++) {
        const char = value.charCodeAt(i);
        hash = (hash << 5) - hash + char;
        hash = hash & hash; // Convert to 32-bit integer
    }
    return Math.abs(hash) % GSI2_SHARD_COUNT;
}

/**
 * Formats shard key.
 *
 * @param {number} shardIndex - Shard index
 * @returns {string} Zero-padded shard string
 */
function formatShardKey(shardIndex) {
    return shardIndex.toString().padStart(2, "0");
}

/**
 * Builds step aggregate GSI1PK.
 *
 * @param {string} phase - Phase name
 * @param {string} step - Step name
 * @param {string} dataGroupLabel - Normalized data group label
 * @returns {string} GSI1PK value
 */
function buildStepAggregateGSI1PK(phase, step, dataGroupLabel) {
    return `AGG#PHASE#${phase}#STEP#${step}#DG#${dataGroupLabel}`;
}

/**
 * Builds step aggregate GSI1SK.
 *
 * @param {string} county - County identifier
 * @returns {string} GSI1SK value
 */
function buildStepAggregateGSI1SK(county) {
    return `COUNTY#${county}`;
}

/**
 * Builds step aggregate GSI2PK.
 *
 * @param {string} county - County identifier
 * @param {string} dataGroupLabel - Normalized data group label
 * @returns {string} GSI2PK value
 */
function buildStepAggregateGSI2PK(county, dataGroupLabel) {
    const shardIndex = computeShardIndex(`${county}#${dataGroupLabel}`);
    return `AGG#ALL#S#${formatShardKey(shardIndex)}`;
}

/**
 * Builds step aggregate GSI2SK.
 *
 * @param {string} dataGroupLabel - Normalized data group label
 * @param {string} phase - Phase name
 * @param {string} step - Step name
 * @param {string} county - County identifier
 * @returns {string} GSI2SK value
 */
function buildStepAggregateGSI2SK(dataGroupLabel, phase, step, county) {
    return `DG#${dataGroupLabel}#PHASE#${phase}#STEP#${step}#COUNTY#${county}`;
}

// =============================================================================
// DynamoDB write operations
// =============================================================================

/**
 * Builds execution state transact item (strict condition: lastEventTime < eventTime).
 *
 * @param {WorkflowEventDetail} detail - Workflow event detail
 * @param {string} eventTime - Event time ISO string
 * @param {string} now - Current ISO timestamp
 * @param {ExecutionStateItem | null} previousState - Previous state or null
 * @param {"IN_PROGRESS" | "FAILED" | "SUCCEEDED"} newBucket - New bucket
 * @param {string} dataGroupLabel - Normalized data group label
 * @param {string} tableName - Table name
 * @returns {import("@aws-sdk/lib-dynamodb").TransactWriteItem} Transact write item
 */
function buildExecutionStateTransactItem(
    detail,
    eventTime,
    now,
    previousState,
    newBucket,
    dataGroupLabel,
    tableName,
) {
    const pk = buildExecutionStatePK(detail.executionId);
    const sk = buildExecutionStateSK(detail.executionId);
    const newVersion = (previousState?.version ?? 0) + 1;

    if (previousState) {
        // Update with STRICT condition: lastEventTime < eventTime (not <=)
        return {
            Update: {
                TableName: tableName,
                Key: { PK: pk, SK: sk },
                UpdateExpression: `
          SET county = :county,
              dataGroupLabel = :dataGroupLabel,
              phase = :phase,
              step = :step,
              bucket = :bucket,
              rawStatus = :rawStatus,
              lastEventTime = :eventTime,
              updatedAt = :now,
              version = :newVersion
        `.trim(),
                ConditionExpression:
                    "version = :oldVersion AND (attribute_not_exists(lastEventTime) OR lastEventTime < :eventTime)",
                ExpressionAttributeValues: {
                    ":county": detail.county,
                    ":dataGroupLabel": dataGroupLabel,
                    ":phase": detail.phase,
                    ":step": detail.step,
                    ":bucket": newBucket,
                    ":rawStatus": detail.status,
                    ":eventTime": eventTime,
                    ":now": now,
                    ":newVersion": newVersion,
                    ":oldVersion": previousState.version,
                },
            },
        };
    }

    // Create new execution state
    return {
        Put: {
            TableName: tableName,
            Item: {
                PK: pk,
                SK: sk,
                entityType: STATE_ENTITY_TYPES.EXECUTION_STATE,
                executionId: detail.executionId,
                county: detail.county,
                dataGroupLabel,
                phase: detail.phase,
                step: detail.step,
                bucket: newBucket,
                rawStatus: detail.status,
                lastEventTime: eventTime,
                createdAt: now,
                updatedAt: now,
                version: 1,
            },
            ConditionExpression: "attribute_not_exists(PK)",
        },
    };
}

/**
 * Builds aggregate increment transact item.
 *
 * @param {string} county - County identifier
 * @param {string} dataGroupLabel - Normalized data group label
 * @param {string} phase - Phase name
 * @param {string} step - Step name
 * @param {"IN_PROGRESS" | "FAILED" | "SUCCEEDED"} bucket - Bucket to increment
 * @param {string} now - Current ISO timestamp
 * @param {string} tableName - Table name
 * @returns {import("@aws-sdk/lib-dynamodb").TransactWriteItem} Transact write item
 */
function buildAggregateIncrementTransactItem(
    county,
    dataGroupLabel,
    phase,
    step,
    bucket,
    now,
    tableName,
) {
    const pk = buildStepAggregatePK(county, dataGroupLabel);
    const sk = buildStepAggregateSK(phase, step);
    const bucketAttr = getBucketCountAttribute(bucket);

    return {
        Update: {
            TableName: tableName,
            Key: { PK: pk, SK: sk },
            UpdateExpression: `
        SET entityType = if_not_exists(entityType, :entityType),
            county = if_not_exists(county, :county),
            dataGroupLabel = if_not_exists(dataGroupLabel, :dataGroupLabel),
            phase = if_not_exists(phase, :phase),
            step = if_not_exists(step, :step),
            inProgressCount = if_not_exists(inProgressCount, :zero),
            failedCount = if_not_exists(failedCount, :zero),
            succeededCount = if_not_exists(succeededCount, :zero),
            createdAt = if_not_exists(createdAt, :now),
            updatedAt = :now,
            GSI1PK = :gsi1pk,
            GSI1SK = :gsi1sk,
            GSI2PK = :gsi2pk,
            GSI2SK = :gsi2sk
        ADD #bucketAttr :increment
      `.trim(),
            ExpressionAttributeNames: {
                "#bucketAttr": bucketAttr,
            },
            ExpressionAttributeValues: {
                ":entityType": STATE_ENTITY_TYPES.STEP_AGGREGATE,
                ":county": county,
                ":dataGroupLabel": dataGroupLabel,
                ":phase": phase,
                ":step": step,
                ":zero": 0,
                ":now": now,
                ":increment": 1,
                ":gsi1pk": buildStepAggregateGSI1PK(phase, step, dataGroupLabel),
                ":gsi1sk": buildStepAggregateGSI1SK(county),
                ":gsi2pk": buildStepAggregateGSI2PK(county, dataGroupLabel),
                ":gsi2sk": buildStepAggregateGSI2SK(dataGroupLabel, phase, step, county),
            },
        },
    };
}

/**
 * Builds aggregate decrement transact item.
 *
 * @param {string} county - County identifier
 * @param {string} dataGroupLabel - Normalized data group label
 * @param {string} phase - Phase name
 * @param {string} step - Step name
 * @param {"IN_PROGRESS" | "FAILED" | "SUCCEEDED"} bucket - Bucket to decrement
 * @param {string} now - Current ISO timestamp
 * @param {string} tableName - Table name
 * @returns {import("@aws-sdk/lib-dynamodb").TransactWriteItem} Transact write item
 */
function buildAggregateDecrementTransactItem(
    county,
    dataGroupLabel,
    phase,
    step,
    bucket,
    now,
    tableName,
) {
    const pk = buildStepAggregatePK(county, dataGroupLabel);
    const sk = buildStepAggregateSK(phase, step);
    const bucketAttr = getBucketCountAttribute(bucket);

    return {
        Update: {
            TableName: tableName,
            Key: { PK: pk, SK: sk },
            UpdateExpression: `
        SET updatedAt = :now
        ADD #bucketAttr :decrement
      `.trim(),
            ExpressionAttributeNames: {
                "#bucketAttr": bucketAttr,
            },
            ExpressionAttributeValues: {
                ":now": now,
                ":decrement": -1,
            },
        },
    };
}

/**
 * Migrates a single execution to DynamoDB.
 *
 * @param {DynamoDBDocumentClient} ddbClient - DynamoDB document client
 * @param {SFNClient} sfnClient - Step Functions client
 * @param {string} executionArn - Execution ARN
 * @param {string} tableName - Table name
 * @param {boolean} dryRun - Whether this is a dry run
 * @returns {Promise<MigrationResult>} Migration result
 */
async function migrateExecution(
    ddbClient,
    sfnClient,
    executionArn,
    tableName,
    dryRun,
) {
    try {
        // Get execution history to extract current status
        const historyCommand = new GetExecutionHistoryCommand({
            executionArn,
            reverseOrder: false, // Need oldest-first to get ExecutionStarted
            includeExecutionData: true,
            maxResults: 1,
        });

        const historyResponse = await sfnClient.send(historyCommand);
        const startedEvent = historyResponse.events?.find(
            (e) => e.type === "ExecutionStarted",
        );

        if (!startedEvent?.executionStartedEventDetails?.input) {
            return {
                executionArn,
                status: "skipped_no_data",
                reason: "Could not find ExecutionStarted event with input",
            };
        }

        const executionInput =
            startedEvent.executionStartedEventDetails.input || "{}";

        // Extract current status from history
        const statusResult = await extractExecutionStatus(
            sfnClient,
            executionArn,
            executionInput,
        );

        if (!statusResult) {
            return {
                executionArn,
                status: "skipped_no_data",
                reason: "Could not extract phase/step/status from execution history",
            };
        }

        const { detail, eventTime } = statusResult;

        // Read existing state (if any)
        const pk = buildExecutionStatePK(detail.executionId);
        const sk = buildExecutionStateSK(detail.executionId);

        const getCommand = new GetCommand({
            TableName: tableName,
            Key: { PK: pk, SK: sk },
            ConsistentRead: true,
        });

        const existingState = await ddbClient.send(getCommand);
        const previousState =
            (existingState.Item && (existingState.Item)) || null;

        // Check if we should skip (newer status already exists)
        if (
            previousState &&
            previousState.lastEventTime &&
            previousState.lastEventTime >= eventTime
        ) {
            return {
                executionArn,
                status: "skipped_newer",
                reason: `Existing lastEventTime (${previousState.lastEventTime}) >= candidate eventTime (${eventTime})`,
            };
        }

        if (dryRun) {
            return {
                executionArn,
                status: "updated",
                reason: "Dry run - would update",
            };
        }

        // Build transaction items
        const now = new Date().toISOString();
        const dataGroupLabel = normalizeDataGroupLabel(detail.dataGroupLabel);
        const newBucket = normalizeStepStatusToBucket(detail.status);

        const transactItems = [];

        // Add execution state update
        transactItems.push(
            buildExecutionStateTransactItem(
                detail,
                eventTime,
                now,
                previousState,
                newBucket,
                dataGroupLabel,
                tableName,
            ),
        );

        // Determine aggregate key changes
        const oldAggKey = previousState
            ? `${previousState.county}#${previousState.dataGroupLabel}#${previousState.phase}#${previousState.step}`
            : null;
        const newAggKey = `${detail.county}#${dataGroupLabel}#${detail.phase}#${detail.step}`;
        const oldBucket = previousState?.bucket || null;

        const sameAggKeyAndBucket =
            oldAggKey === newAggKey && oldBucket === newBucket;

        if (!sameAggKeyAndBucket) {
            // Decrement old aggregate if there was a previous state
            if (previousState && oldBucket) {
                transactItems.push(
                    buildAggregateDecrementTransactItem(
                        previousState.county,
                        previousState.dataGroupLabel,
                        previousState.phase,
                        previousState.step,
                        oldBucket,
                        now,
                        tableName,
                    ),
                );
            }

            // Increment new aggregate
            transactItems.push(
                buildAggregateIncrementTransactItem(
                    detail.county,
                    dataGroupLabel,
                    detail.phase,
                    detail.step,
                    newBucket,
                    now,
                    tableName,
                ),
            );
        }

        // Execute transaction
        const transactCommand = new TransactWriteCommand({
            TransactItems: transactItems,
            ClientRequestToken: `migration-${detail.executionId}-${Date.now()}`,
        });

        await ddbClient.send(transactCommand);

        return {
            executionArn,
            status: "updated",
        };
    } catch (error) {
        const errorMessage =
            error instanceof Error ? error.message : String(error);
        return {
            executionArn,
            status: "failed",
            error: errorMessage,
        };
    }
}

// =============================================================================
// Main migration logic
// =============================================================================

/**
 * Runs the migration for all RUNNING executions.
 *
 * @param {Object} options - Migration options
 * @param {string} options.stateMachineArn - State machine ARN
 * @param {string} options.tableName - DynamoDB table name
 * @param {string} options.region - AWS region
 * @param {boolean} options.dryRun - Whether to perform a dry run
 * @param {number} options.concurrency - Maximum concurrent migrations
 * @param {number | undefined} options.maxExecutions - Maximum executions to process
 * @returns {Promise<MigrationSummary>} Migration summary
 */
async function runMigration(options) {
    const {
        stateMachineArn,
        tableName,
        region,
        dryRun,
        concurrency,
        maxExecutions,
    } = options;

    const sfnClient = new SFNClient({ region });
    const ddbClient = DynamoDBDocumentClient.from(new DynamoDBClient({ region }));

    // List RUNNING executions
    const executions = await listRunningExecutions(
        sfnClient,
        stateMachineArn,
        maxExecutions,
    );

    if (executions.length === 0) {
        log.info("No RUNNING executions found. Nothing to migrate.");
        return {
            totalRunning: 0,
            updated: 0,
            skippedNewer: 0,
            skippedNoData: 0,
            failed: 0,
            results: [],
        };
    }

    log.info(`Processing ${executions.length} execution(s) with concurrency ${concurrency}...`);

    // Process executions with concurrency limit using a proper queue pattern
    /**
     * @type {Array<MigrationResult>}
     */
    const results = [];
    /**
     * @type {Array<{executionArn: string, name: string}>}
     */
    const executionQueue = [...executions]; // Create a copy to avoid mutation

    /**
     * Processes the next execution from the queue with proper synchronization.
     * Uses array shift() which is atomic in JavaScript's single-threaded execution model.
     * Each worker processes items from the queue until it's empty.
     *
     * @returns {Promise<void>}
     */
    const processNext = async () => {
        while (executionQueue.length > 0) {
            // Atomically get next execution (shift() is atomic per JavaScript single-threaded execution)
            // Since JavaScript is single-threaded and async functions yield at await points,
            // shift() operations are effectively atomic - only one can execute at a time
            const execution = executionQueue.shift();
            if (!execution) {
                break; // Queue exhausted
            }

            try {
                const result = await migrateExecution(
                    ddbClient,
                    sfnClient,
                    execution.executionArn,
                    tableName,
                    dryRun,
                );
                results.push(result);

                if (result.status === "updated") {
                    log.info(`  ✓ ${execution.name || execution.executionArn}`);
                } else if (result.status === "skipped_newer") {
                    log.warn(`  ⊘ ${execution.name || execution.executionArn} (newer exists)`);
                } else if (result.status === "skipped_no_data") {
                    log.warn(`  ⊘ ${execution.name || execution.executionArn} (no data)`);
                } else {
                    log.error(`  ✗ ${execution.name || execution.executionArn}: ${result.error}`);
                }
            } catch (error) {
                // Handle unexpected errors during migration
                const errorMessage =
                    error instanceof Error ? error.message : String(error);
                results.push({
                    executionArn: execution.executionArn,
                    status: "failed",
                    error: errorMessage,
                });
                log.error(`  ✗ ${execution.name || execution.executionArn}: ${errorMessage}`);
            }
        }
    };

    // Start concurrent workers (each will process items from the queue until empty)
    await Promise.all(
        Array(concurrency)
            .fill(null)
            .map(() => processNext()),
    );

    // Calculate summary
    const summary = {
        totalRunning: executions.length,
        updated: results.filter((r) => r.status === "updated").length,
        skippedNewer: results.filter((r) => r.status === "skipped_newer").length,
        skippedNoData: results.filter((r) => r.status === "skipped_no_data").length,
        failed: results.filter((r) => r.status === "failed").length,
        results,
    };

    return summary;
}

// =============================================================================
// CLI
// =============================================================================

/**
 * Main entry point.
 */
async function main() {
    const argv = await yargs(hideBin(process.argv))
        .option("prepare-stack", {
            type: "string",
            default: DEFAULT_PREPARE_STACK_NAME,
            description: "CloudFormation stack name for prepare infrastructure",
        })
        .option("workflow-events-stack", {
            type: "string",
            default: DEFAULT_WORKFLOW_EVENTS_STACK_NAME,
            description: "CloudFormation stack name for workflow-events",
        })
        .option("region", {
            type: "string",
            default: DEFAULT_REGION,
            description: "AWS region",
        })
        .option("state-machine-arn", {
            type: "string",
            description: "Override: State machine ARN (skips CloudFormation lookup)",
        })
        .option("table-name", {
            type: "string",
            description: "Override: DynamoDB table name (skips CloudFormation lookup)",
        })
        .option("dry-run", {
            type: "boolean",
            default: false,
            description: "Perform a dry run without writing to DynamoDB",
        })
        .option("concurrency", {
            type: "number",
            default: DEFAULT_CONCURRENCY,
            description: "Maximum concurrent migrations",
        })
        .option("max-executions", {
            type: "number",
            description: "Maximum number of executions to process",
        })
        .help()
        .alias("help", "h")
        .parse();

    try {
        log.info("Starting workflow-state migration...");
        log.info(`  Dry run: ${argv.dryRun}`);
        log.info(`  Region: ${argv.region}`);

        const cfnClient = new CloudFormationClient({ region: argv.region });

        // Discover resources
        let stateMachineArn = argv.stateMachineArn;
        let tableName = argv.tableName;

        if (!stateMachineArn || !tableName) {
            const resources = await discoverResources(
                cfnClient,
                argv.prepareStack,
                argv.workflowEventsStack,
            );
            stateMachineArn = stateMachineArn || resources.stateMachineArn;
            tableName = tableName || resources.tableName;
        }

        // Run migration
        const summary = await runMigration({
            stateMachineArn,
            tableName,
            region: argv.region,
            dryRun: argv.dryRun,
            concurrency: argv.concurrency,
            maxExecutions: argv.maxExecutions,
        });

        // Print summary
        console.log("\n" + chalk.bold("Migration Summary:"));
        console.log(`  Total RUNNING: ${summary.totalRunning}`);
        console.log(`  Updated: ${chalk.green(summary.updated)}`);
        console.log(`  Skipped (newer exists): ${chalk.yellow(summary.skippedNewer)}`);
        console.log(`  Skipped (no data): ${chalk.yellow(summary.skippedNoData)}`);
        console.log(`  Failed: ${chalk.red(summary.failed)}`);

        if (summary.failed > 0) {
            console.log("\n" + chalk.red("Failed executions:"));
            summary.results
                .filter((r) => r.status === "failed")
                .forEach((r) => {
                    console.log(`  ${r.executionArn}: ${r.error}`);
                });
        }

        process.exit(summary.failed > 0 ? 1 : 0);
    } catch (error) {
        log.error(`Migration failed: ${error instanceof Error ? error.message : String(error)}`);
        if (error instanceof Error && error.stack) {
            log.debug(error.stack);
        }
        process.exit(1);
    }
}

// Run main if this script is executed directly
if (require.main === module) {
    main().catch((error) => {
        console.error("Unhandled error:", error);
        process.exit(1);
    });
}

