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
const cliProgress = require("cli-progress");

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
 * @property {number} totalRunning - Total executions found (RUNNING, SUCCEEDED, FAILED)
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
const DEFAULT_RETRY_MAX_ATTEMPTS = 5;
const DEFAULT_RETRY_BASE_DELAY_MS = 1000; // 1 second base delay
const DEFAULT_PAGINATION_DELAY_MS = 200; // 200ms delay between paginated requests

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
  PrepareDirectSubmitInput: {
    phase: "Submit",
    step: "PrepareDirectSubmitInput",
  },
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
  EmitSubmitToBlockchainScheduled: {
    phase: "Submit",
    step: "SubmitToBlockchain",
  },
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

// Global flag to control debug logging
let debugEnabled = false;

const log = {
  info: (msg) => console.log(chalk.green("[INFO]"), msg),
  warn: (msg) => console.log(chalk.yellow("[WARN]"), msg),
  error: (msg) => console.log(chalk.red("[ERROR]"), msg),
  debug: (msg) => {
    if (debugEnabled) {
      console.log(chalk.blue("[DEBUG]"), msg);
    }
  },
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
  const response = await retryWithBackoff(
    async () => await cfnClient.send(command),
    {
      operation: `CloudFormation DescribeStacks (${stackName})`,
    },
  );

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
    getStackOutput(
      cfnClient,
      prepareStackName,
      "ElephantExpressStateMachineArn",
    ),
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
// Retry utilities
// =============================================================================

/**
 * Sleeps for a specified number of milliseconds.
 *
 * @param {number} ms - Milliseconds to sleep
 * @returns {Promise<void>}
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Checks if an error is a throttling/rate limit error.
 *
 * @param {unknown} error - Error to check
 * @returns {boolean} True if error is throttling-related
 */
function isThrottlingError(error) {
  if (!error || typeof error !== "object") {
    return false;
  }

  const errorName = error.name || "";
  const errorMessage = error.message || error.toString() || "";
  const httpStatusCode = error.$metadata?.httpStatusCode;

  return (
    errorName === "ThrottlingException" ||
    errorName === "TooManyRequestsException" ||
    httpStatusCode === 429 ||
    errorMessage.includes("Rate exceeded") ||
    errorMessage.includes("Throttling") ||
    errorMessage.includes("TooManyRequests")
  );
}

/**
 * Retries an async function with exponential backoff.
 *
 * @template T
 * @param {() => Promise<T>} fn - Function to retry
 * @param {Object} options - Retry options
 * @param {number} [options.maxAttempts] - Maximum retry attempts (default: 5)
 * @param {number} [options.baseDelayMs] - Base delay in milliseconds (default: 1000)
 * @param {string} [options.operation] - Operation name for logging
 * @returns {Promise<T>} Result of the function
 * @throws {Error} Last error if all retries fail
 */
async function retryWithBackoff(fn, options = {}) {
  const {
    maxAttempts = DEFAULT_RETRY_MAX_ATTEMPTS,
    baseDelayMs = DEFAULT_RETRY_BASE_DELAY_MS,
    operation = "operation",
  } = options;

  let lastError;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;

      // Only retry on throttling errors
      if (!isThrottlingError(error)) {
        throw error;
      }

      // Don't retry on last attempt
      if (attempt >= maxAttempts) {
        break;
      }

      // Calculate delay with exponential backoff and jitter
      const exponentialDelay = baseDelayMs * Math.pow(2, attempt - 1);
      const jitter = Math.random() * 0.3 * exponentialDelay; // Up to 30% jitter
      const delay = Math.min(exponentialDelay + jitter, 30000); // Cap at 30 seconds

      log.debug(
        `  Retrying ${operation} (attempt ${attempt}/${maxAttempts}) after ${Math.round(delay)}ms...`,
      );
      await sleep(delay);
    }
  }

  throw lastError;
}

// =============================================================================
// Step Functions execution listing and processing
// =============================================================================

/**
 * Processes a page of executions immediately as they're retrieved.
 *
 * @param {SFNClient} sfnClient - Step Functions client
 * @param {string} stateMachineArn - State machine ARN
 * @param {"RUNNING" | "SUCCEEDED" | "FAILED"} statusFilter - Status filter
 * @param {string | undefined} nextToken - Pagination token
 * @param {number} pageNumber - Current page number
 * @returns {Promise<{executions: Array<{executionArn: string, name: string}>, nextToken: string | undefined}>} Page of executions
 */
async function fetchExecutionPage(
  sfnClient,
  stateMachineArn,
  statusFilter,
  nextToken,
  pageNumber,
) {
  const command = new ListExecutionsCommand({
    stateMachineArn,
    statusFilter,
    maxResults: 100,
    nextToken,
  });

  const response = await retryWithBackoff(
    async () => await sfnClient.send(command),
    {
      operation: `ListExecutions (${statusFilter}, page ${pageNumber})`,
    },
  );

  const executions =
    response.executions?.map((e) => ({
      executionArn: e.executionArn || "",
      name: e.name || "",
      status: e.status || statusFilter, // Include execution status
    })) || [];

  return {
    executions,
    nextToken: response.nextToken,
  };
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
    return input?.message?.dataGroupLabel || input?.dataGroupLabel || undefined;
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
 * Represents a WorkflowEvent found in execution history.
 *
 * @typedef {Object} WorkflowEventHistoryItem
 * @property {WorkflowEventDetail} detail - WorkflowEvent detail
 * @property {string} eventTime - ISO timestamp of the event
 * @property {number} eventIndex - Index in the events array (for ordering)
 * @property {string} stateName - State name that emitted this event
 */

/**
 * Extracts all WorkflowEvents from execution history and derives the final state.
 *
 * @param {SFNClient} sfnClient - Step Functions client
 * @param {string} executionArn - Execution ARN
 * @param {string} executionInput - Execution input JSON (from ExecutionStarted)
 * @param {"RUNNING" | "SUCCEEDED" | "FAILED" | "TIMED_OUT" | "ABORTED"} executionStatus - Current execution status
 * @returns {Promise<{detail: WorkflowEventDetail, eventTime: string} | null>} Extracted status or null
 */
async function extractExecutionStatus(
  sfnClient,
  executionArn,
  executionInput,
  executionStatus,
) {
  const command = new GetExecutionHistoryCommand({
    executionArn,
    reverseOrder: false, // Get chronological order to traverse properly
    includeExecutionData: true,
    maxResults: 1000, // Get enough history
  });

  const response = await retryWithBackoff(
    async () => await sfnClient.send(command),
    {
      operation: `GetExecutionHistory (${executionArn.split(":").pop()})`,
    },
  );
  const events = response.events || [];

  // Extract county and dataGroupLabel from input (fallback)
  const county = extractCountyFromInput(executionInput);
  const dataGroupLabel = extractDataGroupLabelFromInput(executionInput);
  const executionId = executionArn.split(":").pop() || executionArn;

  // Determine final status based on execution status
  let finalStatus = "IN_PROGRESS";
  if (executionStatus === "SUCCEEDED") {
    finalStatus = "SUCCEEDED";
  } else if (
    executionStatus === "FAILED" ||
    executionStatus === "TIMED_OUT" ||
    executionStatus === "ABORTED"
  ) {
    finalStatus = "FAILED";
  }

  // Build a map of TaskScheduled events and track which ones succeeded
  const taskScheduledMap = new Map(); // eventId -> {event, stateName}
  const succeededTaskIds = new Set();
  const stateNameMap = new Map(); // eventId -> stateName
  const eventById = new Map(); // eventId -> event (for tracing previousEventId chain)

  // First pass: Build maps of events and track state names
  for (let i = 0; i < events.length; i++) {
    const event = events[i];

    // Build event map for tracing previousEventId chain
    eventById.set(event.id, event);

    // Track state names for TaskScheduled events
    // Event types are specific like TaskStateEntered, ChoiceStateEntered, PassStateEntered, etc.
    if (event.type.endsWith("StateEntered")) {
      const stateName = event.stateEnteredEventDetails?.name;
      if (stateName) {
        // Store state name for subsequent TaskScheduled events
        // (they happen after StateEntered)
        stateNameMap.set(i, stateName);
      }
    }

    // Track TaskScheduled events with putEvents
    // Resource can be just "putEvents" or full ARN "arn:aws:states:::events:putEvents"
    if (event.type === "TaskScheduled") {
      const details = event.taskScheduledEventDetails;
      if (
        details?.resource &&
        (details.resource === "putEvents" ||
          details.resource.startsWith("arn:aws:states:::events:putEvents"))
      ) {
        // Find the most recent StateEntered before this TaskScheduled
        let associatedStateName = null;
        for (let j = i - 1; j >= 0; j--) {
          if (stateNameMap.has(j)) {
            associatedStateName = stateNameMap.get(j);
            break;
          }
        }

        taskScheduledMap.set(event.id, {
          event,
          stateName: associatedStateName,
          index: i,
        });
      }
    }

    // Track which tasks succeeded
    // TaskSucceeded links to TaskStarted via previousEventId, and TaskStarted links to TaskScheduled
    // So we need to trace: TaskSucceeded.previousEventId → TaskStarted → TaskStarted.previousEventId → TaskScheduled
    if (event.type === "TaskSucceeded") {
      // Find the TaskStarted event via previousEventId
      const taskStartedEvent = eventById.get(event.previousEventId);
      if (taskStartedEvent && taskStartedEvent.type === "TaskStarted") {
        // The TaskStarted's previousEventId points to TaskScheduled
        const scheduledEventId = taskStartedEvent.previousEventId;
        if (scheduledEventId) {
          succeededTaskIds.add(scheduledEventId);
        }
      }
    }
  }

  // Second pass: Extract all successful WorkflowEvents
  /**
   * @type {Array<WorkflowEventHistoryItem>}
   */
  const workflowEvents = [];

  for (const [eventId, { event, stateName, index }] of taskScheduledMap) {
    const details = event.taskScheduledEventDetails;
    const isWaitForTaskToken = details?.resource?.includes("waitForTaskToken");
    const isSucceeded = succeededTaskIds.has(eventId);
    const isRunning = executionStatus === "RUNNING";

    // Accept if:
    // 1. Task succeeded (event was emitted), OR
    // 2. It's a waitForTaskToken and execution is still running (waiting for callback)
    if (isSucceeded || (isWaitForTaskToken && isRunning)) {
      const parameters = details.parameters;
      if (parameters) {
        const detail = extractWorkflowEventFromTaskParameters(parameters);
        if (detail) {
          workflowEvents.push({
            detail: {
              ...detail,
              county: detail.county !== "unknown" ? detail.county : county,
              dataGroupLabel:
                detail.dataGroupLabel || dataGroupLabel || undefined,
            },
            eventTime: new Date(event.timestamp).toISOString(),
            eventIndex: index,
            stateName: stateName || "unknown",
          });
        }
      }
    }
  }

  // If we found WorkflowEvents, calculate latest status per phase
  if (workflowEvents.length > 0) {
    // Group events by phase+step combination and find latest status for each
    const phaseStepMap = new Map(); // key: `${phase}#${step}` -> latest event

    for (const workflowEvent of workflowEvents) {
      const key = `${workflowEvent.detail.phase}#${workflowEvent.detail.step}`;
      const existing = phaseStepMap.get(key);

      // Keep the latest event for this phase+step (highest eventIndex = most recent)
      if (!existing || workflowEvent.eventIndex > existing.eventIndex) {
        phaseStepMap.set(key, workflowEvent);
      }
    }

    // Determine the latest phase reached (by eventIndex)
    // This represents the most recent phase the execution was in
    workflowEvents.sort((a, b) => b.eventIndex - a.eventIndex);
    const latestEventOverall = workflowEvents[0];
    const latestPhaseKey = `${latestEventOverall.detail.phase}#${latestEventOverall.detail.step}`;
    const latestEventForPhase = phaseStepMap.get(latestPhaseKey);

    // Use the latest status for the latest phase reached
    const finalEvent = latestEventForPhase || latestEventOverall;

    log.debug(
      `  Found ${workflowEvents.length} WorkflowEvent(s) across ${phaseStepMap.size} phase/step(s). Latest phase: ${finalEvent.detail.phase}, Step: ${finalEvent.detail.step}, Status: ${finalEvent.detail.status}`,
    );

    // Override status with execution status for completed executions
    return {
      detail: {
        ...finalEvent.detail,
        status:
          executionStatus === "SUCCEEDED"
            ? "SUCCEEDED"
            : executionStatus === "FAILED" ||
                executionStatus === "TIMED_OUT" ||
                executionStatus === "ABORTED"
              ? "FAILED"
              : finalEvent.detail.status,
      },
      eventTime: finalEvent.eventTime,
    };
  }

  // If no WorkflowEvents found, fallback to StateEntered events
  // Find the latest StateEntered event that we can map
  // Event types are specific like TaskStateEntered, ChoiceStateEntered, PassStateEntered, etc.
  const stateEnteredEvents = [];
  for (let i = 0; i < events.length; i++) {
    const event = events[i];
    if (event.type.endsWith("StateEntered")) {
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

          stateEnteredEvents.push({
            detail: {
              executionId,
              county: stateCounty,
              status: finalStatus,
              phase: mapping.phase,
              step: mapping.step,
              dataGroupLabel: dataGroupLabel || undefined,
              errors: [],
            },
            eventTime: new Date(event.timestamp).toISOString(),
            eventIndex: i,
            stateName,
          });
        }
      }
    }
  }

  // If we found StateEntered events, use the latest one
  if (stateEnteredEvents.length > 0) {
    // Sort by eventIndex (chronological order) and get the latest
    stateEnteredEvents.sort((a, b) => b.eventIndex - a.eventIndex);
    const latestState = stateEnteredEvents[0];

    log.debug(
      `  Using StateEntered fallback for ${executionId}. State: ${latestState.stateName}, Phase: ${latestState.detail.phase}, Step: ${latestState.detail.step}`,
    );

    return {
      detail: latestState.detail,
      eventTime: latestState.eventTime,
    };
  }

  // Last resort: Try to infer from any StateEntered event using pattern matching
  // Event types are specific like TaskStateEntered, ChoiceStateEntered, PassStateEntered, etc.
  const allStateEnteredEvents = events.filter((e) =>
    e.type.endsWith("StateEntered"),
  );
  if (allStateEnteredEvents.length > 0) {
    // Get the latest StateEntered (last in chronological order)
    const lastStateEvent =
      allStateEnteredEvents[allStateEnteredEvents.length - 1];
    const stateName = lastStateEvent.stateEnteredEventDetails?.name;

    if (stateName) {
      // Use a generic fallback - try to infer phase from state name patterns
      let inferredPhase = "Unknown";
      let inferredStep = stateName;

      // Try to infer phase from common patterns
      if (
        stateName.includes("Preprocess") ||
        stateName === "WaitForPreprocessResolution"
      ) {
        inferredPhase = "Preprocess";
        inferredStep = "Preprocess";
      } else if (
        stateName.includes("Prepare") ||
        stateName === "EmitBypassPrepareSucceeded"
      ) {
        inferredPhase = "Prepare";
        inferredStep = "Prepare";
      } else if (
        stateName.includes("Transform") ||
        stateName === "WaitForTransformResolution"
      ) {
        inferredPhase = "Transform";
        inferredStep = "Transform";
      } else if (
        stateName.includes("SVL") ||
        stateName.includes("Svl") ||
        stateName === "WaitForSvlValidationResolution" ||
        stateName === "WaitForSvlExceptionResolution" ||
        stateName === "CheckSvlResult"
      ) {
        inferredPhase = "SVL";
        inferredStep = "SVL";
      } else if (
        stateName.includes("Hash") ||
        stateName === "WaitForHashResolution"
      ) {
        inferredPhase = "Hash";
        inferredStep = "Hash";
      } else if (
        stateName.includes("Upload") ||
        stateName === "WaitForUploadResolution"
      ) {
        inferredPhase = "Upload";
        inferredStep = "Upload";
      } else if (
        stateName.includes("GasPrice") ||
        stateName.includes("CheckGasPrice") ||
        stateName === "WaitForGasPriceCheckResolution"
      ) {
        inferredPhase = "GasPriceCheck";
        inferredStep = "CheckGasPrice";
      } else if (
        stateName.includes("Submit") ||
        stateName.includes("Blockchain") ||
        stateName === "WaitForSubmitResolution"
      ) {
        inferredPhase = "Submit";
        inferredStep =
          stateName.includes("Blockchain") || stateName === "SubmitToBlockchain"
            ? "SubmitToBlockchain"
            : "Submit";
      } else if (
        stateName.includes("Transaction") ||
        stateName === "WaitForTransactionStatusCheckResolution"
      ) {
        inferredPhase = "TransactionStatusCheck";
        inferredStep = "CheckTransactionStatus";
      }

      log.debug(
        `  Using pattern-based inference for ${executionId}. State: ${stateName}, Inferred Phase: ${inferredPhase}, Step: ${inferredStep}`,
      );

      return {
        detail: {
          executionId,
          county: county !== "unknown" ? county : "unknown",
          status: finalStatus,
          phase: inferredPhase,
          step: inferredStep,
          dataGroupLabel: dataGroupLabel || undefined,
          errors: [],
        },
        eventTime: new Date(lastStateEvent.timestamp).toISOString(),
      };
    }
  }

  // If we still can't find anything, log debug info
  const stateNames = events
    .filter((e) => e.type === "StateEntered")
    .map((e) => e.stateEnteredEventDetails?.name)
    .filter(Boolean);
  const eventTypes = [...new Set(events.map((e) => e.type))];
  log.debug(
    `  Could not extract status for ${executionId}. Execution status: ${executionStatus}. Found ${events.length} events. Event types: ${eventTypes.join(", ")}. State names: ${stateNames.slice(0, 10).join(", ")}${stateNames.length > 10 ? "..." : ""}`,
  );

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
    case "PARKED":
      return "FAILED";
    case "IN_PROGRESS":
    case "SCHEDULED":
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
 * Gets the other two bucket count attributes (excluding the specified bucket).
 *
 * @param {string} bucketAttr - The bucket attribute to exclude
 * @returns {Array<string>} Array of the other two bucket attributes
 */
function getOtherBucketAttributes(bucketAttr) {
  const allBuckets = ["inProgressCount", "failedCount", "succeededCount"];
  return allBuckets.filter((b) => b !== bucketAttr);
}

/**
 * Builds aggregate increment transact item.
 * Properly handles the SET/ADD overlap by only using if_not_exists for the
 * buckets NOT being incremented.
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
  const otherBuckets = getOtherBucketAttributes(bucketAttr);

  // Build SET clause for the other two buckets (not the one being incremented)
  // This avoids the "overlapping document paths" error with ADD
  const otherBucketSetClauses = otherBuckets
    .map((b) => `${b} = if_not_exists(${b}, :zero)`)
    .join(",\n            ");

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
            ${otherBucketSetClauses},
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
        ":gsi2sk": buildStepAggregateGSI2SK(
          dataGroupLabel,
          phase,
          step,
          county,
        ),
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
 * Builds a combined aggregate update transact item that decrements one bucket
 * and increments another in a single operation. Used when an execution changes
 * status (bucket) but stays in the same phase/step.
 *
 * @param {string} county - County identifier
 * @param {string} dataGroupLabel - Normalized data group label
 * @param {string} phase - Phase name
 * @param {string} step - Step name
 * @param {"IN_PROGRESS" | "FAILED" | "SUCCEEDED"} oldBucket - Bucket to decrement
 * @param {"IN_PROGRESS" | "FAILED" | "SUCCEEDED"} newBucket - Bucket to increment
 * @param {string} now - Current ISO timestamp
 * @param {string} tableName - Table name
 * @returns {import("@aws-sdk/lib-dynamodb").TransactWriteItem} Transact write item
 */
function buildAggregateBucketTransferTransactItem(
  county,
  dataGroupLabel,
  phase,
  step,
  oldBucket,
  newBucket,
  now,
  tableName,
) {
  const pk = buildStepAggregatePK(county, dataGroupLabel);
  const sk = buildStepAggregateSK(phase, step);
  const oldBucketAttr = getBucketCountAttribute(oldBucket);
  const newBucketAttr = getBucketCountAttribute(newBucket);

  // Find the third bucket that's not being modified
  const allBuckets = ["inProgressCount", "failedCount", "succeededCount"];
  const unchangedBucket = allBuckets.find(
    (b) => b !== oldBucketAttr && b !== newBucketAttr,
  );

  return {
    Update: {
      TableName: tableName,
      Key: { PK: pk, SK: sk },
      UpdateExpression: `
        SET ${unchangedBucket} = if_not_exists(${unchangedBucket}, :zero),
            updatedAt = :now
        ADD #oldBucketAttr :decrement, #newBucketAttr :increment
      `.trim(),
      ExpressionAttributeNames: {
        "#oldBucketAttr": oldBucketAttr,
        "#newBucketAttr": newBucketAttr,
      },
      ExpressionAttributeValues: {
        ":zero": 0,
        ":now": now,
        ":decrement": -1,
        ":increment": 1,
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
 * @param {"RUNNING" | "SUCCEEDED" | "FAILED" | "TIMED_OUT" | "ABORTED"} executionStatus - Execution status
 * @returns {Promise<MigrationResult>} Migration result
 */
async function migrateExecution(
  ddbClient,
  sfnClient,
  executionArn,
  tableName,
  dryRun,
  executionStatus,
) {
  try {
    // Get execution history to extract current status
    const historyCommand = new GetExecutionHistoryCommand({
      executionArn,
      reverseOrder: false, // Need oldest-first to get ExecutionStarted
      includeExecutionData: true,
      maxResults: 1,
    });

    const historyResponse = await retryWithBackoff(
      async () => await sfnClient.send(historyCommand),
      {
        operation: `GetExecutionHistory (started event) (${executionArn.split(":").pop()})`,
      },
    );
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
      executionStatus,
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

    const existingState = await retryWithBackoff(
      async () => await ddbClient.send(getCommand),
      {
        operation: `DynamoDB Get (${detail.executionId})`,
      },
    );
    const previousState = (existingState.Item && existingState.Item) || null;

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

    const sameAggKey = oldAggKey === newAggKey;
    const sameBucket = oldBucket === newBucket;

    if (sameAggKey && !sameBucket && previousState && oldBucket) {
      // Same aggregate, different bucket: use a single combined update
      // (DynamoDB doesn't allow two operations on the same item in a transaction)
      transactItems.push(
        buildAggregateBucketTransferTransactItem(
          detail.county,
          dataGroupLabel,
          detail.phase,
          detail.step,
          oldBucket,
          newBucket,
          now,
          tableName,
        ),
      );
    } else if (!sameAggKey || !sameBucket) {
      // Different aggregate or new execution: use separate decrement/increment
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
    // Note: We don't use ClientRequestToken for migration because:
    // 1. It's primarily for Lambda retry idempotency, not one-time migration
    // 2. Using executionId as token causes issues on retry within 10-min window
    //    when transaction parameters change (e.g., timestamp)
    const transactCommand = new TransactWriteCommand({
      TransactItems: transactItems,
    });

    await retryWithBackoff(async () => await ddbClient.send(transactCommand), {
      operation: `DynamoDB TransactWrite (${detail.executionId})`,
    });

    return {
      executionArn,
      status: "updated",
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);

    // Check for TransactionConflict - this means the live handler is updating
    // the same item concurrently. This is expected and safe - the live handler's
    // data is more current, so we can treat this as "skipped_newer"
    if (errorMessage.includes("TransactionConflict")) {
      return {
        executionArn,
        status: "skipped_newer",
        reason: "TransactionConflict - live handler is updating this execution",
      };
    }

    // Check for idempotency token conflict - can happen if script was re-run
    // within 10 minutes. Treat as already processed.
    if (errorMessage.includes("idempotent token was used")) {
      return {
        executionArn,
        status: "skipped_newer",
        reason: "Idempotency conflict - already processed within 10-min window",
      };
    }

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
 * Runs the migration for executions (RUNNING, SUCCEEDED, FAILED) processing page-by-page.
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

  // Status filters to process
  const statusFilters = ["RUNNING", "SUCCEEDED", "FAILED"];

  log.info(
    `Processing executions with statuses: ${statusFilters.join(", ")}...`,
  );
  log.info(`  Concurrency: ${concurrency}`);
  if (maxExecutions) {
    log.info(`  Max executions: ${maxExecutions}`);
  }

  // Create progress bar (incremental, no total count)
  // Use a large total value so the bar can grow incrementally
  const progressBar = new cliProgress.SingleBar(
    {
      format:
        "Migration Progress |" +
        chalk.cyan("{bar}") +
        "| {value} processed | {status}",
      barCompleteChar: "\u2588",
      barIncompleteChar: "\u2591",
      hideCursor: true,
      stopOnComplete: false,
      etaBuffer: 50,
    },
    cliProgress.Presets.shades_classic,
  );

  // Start with a large total (will be updated as we go)
  progressBar.start(1000000, 0, {
    status: "Starting...",
  });

  /**
   * @type {Array<MigrationResult>}
   */
  const results = [];
  let processedCount = 0;
  let totalFound = 0;

  // Semaphore for controlling concurrency
  let activeWorkers = 0;
  const maxActiveWorkers = concurrency;

  /**
   * Acquires a worker slot (waits if all slots are taken).
   *
   * @returns {Promise<void>}
   */
  const acquireWorker = async () => {
    while (activeWorkers >= maxActiveWorkers) {
      await sleep(10); // Small delay before checking again
    }
    activeWorkers++;
  };

  /**
   * Releases a worker slot.
   */
  const releaseWorker = () => {
    activeWorkers--;
  };

  /**
   * Processes a single execution.
   *
   * @param {{executionArn: string, name: string, status: string}} execution - Execution to process
   * @returns {Promise<void>}
   */
  const processExecution = async (execution) => {
    await acquireWorker();
    try {
      const result = await migrateExecution(
        ddbClient,
        sfnClient,
        execution.executionArn,
        tableName,
        dryRun,
        execution.status || "RUNNING",
      );
      results.push(result);
      processedCount++;

      // Update progress bar
      const status =
        result.status === "updated"
          ? "Updated"
          : result.status === "skipped_newer"
            ? "Skipped (newer)"
            : result.status === "skipped_no_data"
              ? "Skipped (no data)"
              : "Failed";

      progressBar.update(processedCount, {
        status: status,
      });

      // Log details (but don't spam the console)
      if (result.status === "updated") {
        log.debug(`  ✓ ${execution.name || execution.executionArn}`);
      } else if (result.status === "skipped_newer") {
        log.debug(
          `  ⊘ ${execution.name || execution.executionArn} (newer exists)`,
        );
      } else if (result.status === "skipped_no_data") {
        log.debug(`  ⊘ ${execution.name || execution.executionArn} (no data)`);
      } else {
        log.warn(
          `  ✗ ${execution.name || execution.executionArn}: ${result.error}`,
        );
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
      processedCount++;
      progressBar.update(processedCount, {
        status: "Failed",
      });
      log.error(
        `  ✗ ${execution.name || execution.executionArn}: ${errorMessage}`,
      );
    } finally {
      releaseWorker();
    }
  };

  // Process each status filter
  for (const statusFilter of statusFilters) {
    log.info(`Processing ${statusFilter} executions...`);

    let nextToken;
    let pageNumber = 0;
    let statusProcessed = 0;

    do {
      pageNumber++;

      // Fetch next page
      const { executions, nextToken: newNextToken } = await fetchExecutionPage(
        sfnClient,
        stateMachineArn,
        statusFilter,
        nextToken,
        pageNumber,
      );

      if (executions.length === 0) {
        nextToken = newNextToken;
        continue;
      }

      totalFound += executions.length;
      log.debug(
        `  Page ${pageNumber}: Found ${executions.length} execution(s) (${statusFilter})`,
      );

      // Process all executions from this page concurrently (respecting concurrency limit)
      const pagePromises = executions.map((execution) =>
        processExecution(execution),
      );

      // Wait for all executions in this page to complete
      await Promise.all(pagePromises);

      statusProcessed += executions.length;

      // Check if we've hit the max executions limit
      if (maxExecutions && processedCount >= maxExecutions) {
        log.info(
          `  Reached max executions limit (${maxExecutions}). Stopping.`,
        );
        nextToken = undefined;
        break;
      }

      nextToken = newNextToken;

      // Add delay between paginated requests to avoid rate limiting
      if (nextToken) {
        await sleep(DEFAULT_PAGINATION_DELAY_MS);
      }
    } while (nextToken);

    log.info(
      `  Completed ${statusFilter}: ${statusProcessed} execution(s) processed`,
    );
  }

  // Wait for any remaining active workers to finish
  while (activeWorkers > 0) {
    await sleep(10);
  }

  progressBar.stop();

  // Calculate summary
  const summary = {
    totalRunning: totalFound,
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
      description:
        "Override: DynamoDB table name (skips CloudFormation lookup)",
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
    .option("verbose", {
      type: "boolean",
      default: false,
      description: "Enable debug logging",
      alias: "v",
    })
    .help()
    .alias("help", "h")
    .parse();

  // Set debug logging based on flag
  debugEnabled = argv.verbose;

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
    console.log(`  Total executions processed: ${summary.totalRunning}`);
    console.log(`  Updated: ${chalk.green(summary.updated)}`);
    console.log(
      `  Skipped (newer exists): ${chalk.yellow(summary.skippedNewer)}`,
    );
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
    log.error(
      `Migration failed: ${error instanceof Error ? error.message : String(error)}`,
    );
    if (error instanceof Error && error.stack) {
      log.debug(error.stack);
    }
    process.exit(1);
  }
}

// Export migrateExecution for testing
module.exports = {
  migrateExecution,
};

// Run main if this script is executed directly
if (require.main === module) {
  main().catch((error) => {
    console.error("Unhandled error:", error);
    process.exit(1);
  });
}
