#!/usr/bin/env node
/**
 * Script to resume stuck Step Function executions that are waiting for task tokens.
 *
 * This script:
 * 1. Reads execution IDs from a CSV log file
 * 2. For each execution, gets the execution history from Step Functions
 * 3. Extracts the task token from the execution history
 * 4. Sends SendTaskSuccess to resume the execution
 *
 * Usage:
 *   node scripts/resume-stuck-executions.mjs --csv <file.csv> --state-machine-arn <arn> [--dry-run] [--limit 100]
 *
 * Examples:
 *   # Dry run - see what would be resumed
 *   node scripts/resume-stuck-executions.mjs \
 *     --csv "log-events-viewer-result (3).csv" \
 *     --state-machine-arn "arn:aws:states:us-east-1:079253117335:stateMachine:ElephantExpressStateMachine-HCF7KUVkskfS" \
 *     --dry-run
 *
 *   # Actually resume executions (limit to 10 for testing)
 *   node scripts/resume-stuck-executions.mjs \
 *     --csv "log-events-viewer-result (3).csv" \
 *     --state-machine-arn "arn:aws:states:us-east-1:079253117335:stateMachine:ElephantExpressStateMachine-HCF7KUVkskfS" \
 *     --limit 10
 */

import { readFileSync, writeFileSync, existsSync } from "fs";
import {
  SFNClient,
  DescribeExecutionCommand,
  GetExecutionHistoryCommand,
  SendTaskSuccessCommand,
} from "@aws-sdk/client-sfn";

const sfnClient = new SFNClient({ region: process.env.AWS_REGION || "us-east-1" });

// States that use waitForTaskToken and might be stuck
const WAIT_FOR_TOKEN_STATES = [
  "WaitForGasPriceCheckResolution",
  "WaitForPreprocessResolution",
  "WaitForPrepareResolution",
  "WaitForTransformResolution",
  "WaitForSVLResolution",
  "WaitForHashResolution",
  "WaitForUploadResolution",
  "WaitForSubmitResolution",
  "WaitForTransactionFailedResolution",
  "WaitForTransactionGeneralErrorResolution",
];

function parseArgs() {
  const args = process.argv.slice(2);
  const config = {
    csvFile: null,
    stateMachineArn: null,
    dryRun: false,
    limit: null,
    offset: 0,
    targetStates: null,
    outputFile: null,
    concurrency: 10,
  };

  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case "--csv":
        config.csvFile = args[++i];
        break;
      case "--state-machine-arn":
        config.stateMachineArn = args[++i];
        break;
      case "--dry-run":
        config.dryRun = true;
        break;
      case "--limit":
        config.limit = parseInt(args[++i], 10);
        break;
      case "--offset":
        config.offset = parseInt(args[++i], 10);
        break;
      case "--target-states":
        config.targetStates = args[++i].split(",");
        break;
      case "--output":
        config.outputFile = args[++i];
        break;
      case "--concurrency":
        config.concurrency = parseInt(args[++i], 10);
        break;
      case "--help":
      case "-h":
        printUsage();
        process.exit(0);
    }
  }

  if (!config.csvFile) {
    console.error("Error: --csv is required");
    printUsage();
    process.exit(1);
  }

  if (!config.stateMachineArn) {
    console.error("Error: --state-machine-arn is required");
    printUsage();
    process.exit(1);
  }

  return config;
}

function printUsage() {
  console.log(`
Usage: node scripts/resume-stuck-executions.mjs [OPTIONS]

OPTIONS:
  --csv <file>                Required. CSV file containing execution IDs in the logs
  --state-machine-arn <arn>   Required. The Step Function state machine ARN
  --dry-run                   Show what would be done without actually sending task success
  --limit <n>                 Limit the number of executions to process
  --offset <n>                Skip the first n executions (for resuming)
  --target-states <states>    Comma-separated list of states to target (default: all wait states)
  --output <file>             Output results to a JSON file
  --concurrency <n>           Number of concurrent executions to process (default: 10)
  -h, --help                  Show this help message

EXAMPLES:
  # Dry run
  node scripts/resume-stuck-executions.mjs \\
    --csv "log-events-viewer-result (3).csv" \\
    --state-machine-arn "arn:aws:states:us-east-1:123:stateMachine:MyStateMachine" \\
    --dry-run

  # Resume first 100 executions
  node scripts/resume-stuck-executions.mjs \\
    --csv "log-events-viewer-result (3).csv" \\
    --state-machine-arn "arn:aws:states:us-east-1:123:stateMachine:MyStateMachine" \\
    --limit 100

  # Resume next 100 executions (offset by 100)
  node scripts/resume-stuck-executions.mjs \\
    --csv "log-events-viewer-result (3).csv" \\
    --state-machine-arn "arn:aws:states:us-east-1:123:stateMachine:MyStateMachine" \\
    --limit 100 --offset 100
`);
}

function extractExecutionIdsFromCsv(csvFile) {
  console.log(`Reading execution IDs from ${csvFile}...`);

  if (!existsSync(csvFile)) {
    console.error(`Error: File not found: ${csvFile}`);
    process.exit(1);
  }

  const content = readFileSync(csvFile, "utf-8");

  // Extract all UUIDs (execution IDs) from the file
  const uuidRegex = /[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}/gi;
  const matches = content.match(uuidRegex) || [];

  // Deduplicate
  const uniqueIds = [...new Set(matches.map(id => id.toLowerCase()))];

  console.log(`Found ${uniqueIds.length} unique execution IDs`);
  return uniqueIds;
}

async function checkExecutionStatus(executionArn) {
  try {
    const command = new DescribeExecutionCommand({ executionArn });
    const response = await sfnClient.send(command);
    return response.status;
  } catch (error) {
    if (error.name === "ExecutionDoesNotExist") {
      return "NOT_FOUND";
    }
    throw error;
  }
}

async function getTaskTokenFromExecution(executionArn, targetStates) {
  try {
    // Get execution history (most recent events first)
    let allEvents = [];
    let nextToken = null;

    const command = new GetExecutionHistoryCommand({
      executionArn,
      reverseOrder: true,
      maxResults: 100,
    });

    const response = await sfnClient.send(command);
    allEvents = response.events;

    // Find the most recent TaskStateEntered for a wait state that hasn't completed
    let currentState = null;
    let taskToken = null;

    for (const event of allEvents) {
      // Find current state
      if (event.type === "TaskStateEntered" && !currentState) {
        const stateName = event.stateEnteredEventDetails?.name;
        const statesToCheck = targetStates || WAIT_FOR_TOKEN_STATES;
        if (statesToCheck.includes(stateName)) {
          currentState = stateName;
        }
      }

      // Find task token from TaskScheduled event
      if (event.type === "TaskScheduled" && currentState && !taskToken) {
        const parameters = event.taskScheduledEventDetails?.parameters;
        if (parameters) {
          try {
            const parsed = JSON.parse(parameters);
            // For EventBridge putEvents.waitForTaskToken
            if (parsed.Entries?.[0]?.Detail) {
              let detail = parsed.Entries[0].Detail;
              if (typeof detail === "string") {
                detail = JSON.parse(detail);
              }
              if (detail.taskToken) {
                taskToken = detail.taskToken;
                break;
              }
            }
          } catch (e) {
            // Continue looking
          }
        }
      }
    }

    if (currentState && taskToken) {
      return {
        stateName: currentState,
        executionArn,
        taskToken,
      };
    }

    return null;
  } catch (error) {
    return { error: error.message };
  }
}

async function sendTaskSuccess(taskToken, executionArn, dryRun) {
  if (dryRun) {
    return { success: true, dryRun: true };
  }

  try {
    const command = new SendTaskSuccessCommand({
      taskToken,
      output: JSON.stringify({ resumed: true, resumedAt: new Date().toISOString() }),
    });

    await sfnClient.send(command);
    return { success: true };
  } catch (error) {
    return { success: false, error: error.message };
  }
}

async function processExecution(executionId, stateMachineArn, config, index, total) {
  const executionArn = `${stateMachineArn.replace(":stateMachine:", ":execution:")}:${executionId}`;

  const result = {
    executionId,
    executionArn,
    status: null,
    state: null,
    resumed: false,
    error: null,
  };

  try {
    // Check if execution is still running
    const status = await checkExecutionStatus(executionArn);
    result.status = status;

    if (status !== "RUNNING") {
      return result;
    }

    // Get task token
    const tokenInfo = await getTaskTokenFromExecution(executionArn, config.targetStates);

    if (!tokenInfo) {
      result.state = "not_at_wait_state";
      return result;
    }

    if (tokenInfo.error) {
      result.error = tokenInfo.error;
      return result;
    }

    result.state = tokenInfo.stateName;

    // Send task success
    const sendResult = await sendTaskSuccess(tokenInfo.taskToken, executionArn, config.dryRun);

    if (sendResult.success) {
      result.resumed = true;
      if (sendResult.dryRun) {
        result.dryRun = true;
      }
    } else {
      result.error = sendResult.error;
    }
  } catch (error) {
    result.error = error.message;
  }

  return result;
}

async function processInBatches(items, batchSize, processFunc) {
  const results = [];

  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    const batchResults = await Promise.all(batch.map(processFunc));
    results.push(...batchResults);

    // Small delay between batches to avoid throttling
    if (i + batchSize < items.length) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  return results;
}

async function main() {
  const config = parseArgs();

  console.log("=".repeat(70));
  console.log("Resume Stuck Executions Script");
  console.log("=".repeat(70));
  console.log(`CSV File: ${config.csvFile}`);
  console.log(`State Machine: ${config.stateMachineArn}`);
  console.log(`Dry Run: ${config.dryRun}`);
  console.log(`Limit: ${config.limit || "none"}`);
  console.log(`Offset: ${config.offset}`);
  console.log(`Concurrency: ${config.concurrency}`);
  console.log(`Target States: ${config.targetStates?.join(", ") || "all wait states"}`);
  console.log("=".repeat(70));
  console.log();

  // Extract execution IDs from CSV
  const allExecutionIds = extractExecutionIdsFromCsv(config.csvFile);

  if (allExecutionIds.length === 0) {
    console.log("No execution IDs found in CSV file.");
    return;
  }

  // Apply offset and limit
  let executionIds = allExecutionIds.slice(config.offset);
  if (config.limit) {
    executionIds = executionIds.slice(0, config.limit);
  }

  console.log(`\nProcessing ${executionIds.length} executions (offset: ${config.offset})...\n`);

  const startTime = Date.now();
  let processed = 0;

  const results = await processInBatches(
    executionIds,
    config.concurrency,
    async (executionId) => {
      processed++;
      const progress = `[${processed}/${executionIds.length}]`;

      const result = await processExecution(
        executionId,
        config.stateMachineArn,
        config,
        processed,
        executionIds.length
      );

      // Log progress
      if (result.resumed) {
        console.log(`${progress} ${executionId}: RESUMED (${result.state})${result.dryRun ? " [DRY RUN]" : ""}`);
      } else if (result.status !== "RUNNING") {
        console.log(`${progress} ${executionId}: ${result.status}`);
      } else if (result.error) {
        console.log(`${progress} ${executionId}: ERROR - ${result.error}`);
      } else {
        console.log(`${progress} ${executionId}: ${result.state || "not stuck"}`);
      }

      return result;
    }
  );

  const duration = ((Date.now() - startTime) / 1000).toFixed(1);

  // Calculate summary
  const summary = {
    total: results.length,
    running: results.filter(r => r.status === "RUNNING").length,
    notRunning: results.filter(r => r.status && r.status !== "RUNNING").length,
    resumed: results.filter(r => r.resumed).length,
    notStuck: results.filter(r => r.status === "RUNNING" && !r.resumed && !r.error).length,
    errors: results.filter(r => r.error).length,
    duration,
  };

  console.log("\n" + "=".repeat(70));
  console.log("Summary");
  console.log("=".repeat(70));
  console.log(`Total Processed:  ${summary.total}`);
  console.log(`Still Running:    ${summary.running}`);
  console.log(`Not Running:      ${summary.notRunning} (already completed/failed)`);
  console.log(`Resumed:          ${summary.resumed}${config.dryRun ? " (dry run)" : ""}`);
  console.log(`Not Stuck:        ${summary.notStuck}`);
  console.log(`Errors:           ${summary.errors}`);
  console.log(`Duration:         ${summary.duration}s`);
  console.log("=".repeat(70));

  // Save results to file if requested
  if (config.outputFile) {
    const output = {
      config: {
        csvFile: config.csvFile,
        stateMachineArn: config.stateMachineArn,
        dryRun: config.dryRun,
        limit: config.limit,
        offset: config.offset,
        targetStates: config.targetStates,
      },
      summary,
      results,
    };
    writeFileSync(config.outputFile, JSON.stringify(output, null, 2));
    console.log(`\nResults saved to: ${config.outputFile}`);
  }

  // Show breakdown by status
  const statusBreakdown = {};
  for (const result of results) {
    const key = result.status || "UNKNOWN";
    statusBreakdown[key] = (statusBreakdown[key] || 0) + 1;
  }
  console.log("\nStatus Breakdown:");
  for (const [status, count] of Object.entries(statusBreakdown)) {
    console.log(`  ${status}: ${count}`);
  }

  // Show breakdown by state (for running executions)
  const stateBreakdown = {};
  for (const result of results.filter(r => r.status === "RUNNING")) {
    const key = result.state || "not_at_wait_state";
    stateBreakdown[key] = (stateBreakdown[key] || 0) + 1;
  }
  if (Object.keys(stateBreakdown).length > 0) {
    console.log("\nState Breakdown (running executions):");
    for (const [state, count] of Object.entries(stateBreakdown)) {
      console.log(`  ${state}: ${count}`);
    }
  }
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});