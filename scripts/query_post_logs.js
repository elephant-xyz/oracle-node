#!/usr/bin/env node

const {
  CloudWatchLogsClient,
  StartQueryCommand,
  GetQueryResultsCommand,
} = require("@aws-sdk/client-cloudwatch-logs");
const {
  CloudFormationClient,
  DescribeStacksCommand,
} = require("@aws-sdk/client-cloudformation");
const yargs = require("yargs/yargs");
const { hideBin } = require("yargs/helpers");
const chalk = require("chalk");

// Logging setup with chalk
const log = {
  info: (msg) => console.log(chalk.green("[INFO]"), msg),
  warn: (msg) => console.log(chalk.yellow("[WARN]"), msg),
  error: (msg) => console.log(chalk.red("[ERROR]"), msg),
  debug: (msg) => console.log(chalk.blue("[DEBUG]"), msg),
};

// Default values
const DEFAULT_STACK_NAME = "elephant-oracle-node";

// Get raw JSON messages and parse them in JavaScript
const SUCCESS_QUERY = `fields @message | filter @message like /"component":"post"/ and @message like /"msg":"post_lambda_complete"/ | sort @timestamp desc`;

const FAILURE_QUERY = `fields @message | filter @message like /"component":"post"/ and @message like /"level":"error"/ | sort @timestamp desc`;

function showUsage() {
  console.log(`
Usage: node query_post_logs.js [--stack <stack-name>] [--start <ISO-8601>] [--end <ISO-8601>] [--profile <aws_profile>] [--region <aws_region>]

Summarize Elephant post-processing Lambda logs by county using CloudWatch Logs Insights.

Options:
  --stack, -s       CloudFormation stack name (default: ${DEFAULT_STACK_NAME})
  --start           ISO-8601 start time (default: one hour ago, UTC)
  --end             ISO-8601 end time (default: now, UTC)
  --profile         AWS profile for credentials (optional)
  --region          AWS region override (optional)
  --help            Show this help text

Examples:
  node query_post_logs.js
  node query_post_logs.js -s my-stack-name --start 2025-09-24T10:00 --end 2025-09-24T12:00
`);
}

async function checkDependencies() {
  log.info("Checking dependencies...");

  // Check for required dependencies
  try {
    require("@aws-sdk/client-cloudwatch-logs");
    require("@aws-sdk/client-cloudformation");
    require("yargs");
    require("chalk");
    log.info("All dependencies found");
  } catch (error) {
    log.error(`Missing dependency: ${error.message}`);
    process.exit(1);
  }
}

function parseIsoToEpoch(input) {
  if (!input) return null;

  try {
    let normalized = input;
    if (normalized.endsWith("Z")) {
      normalized = normalized.slice(0, -1);
    }

    const date = new Date(normalized);
    if (isNaN(date.getTime())) {
      throw new Error(
        `Invalid timestamp '${input}'. Use ISO-8601 like 2025-09-24T10:00`,
      );
    }

    return Math.floor(date.getTime() / 1000);
  } catch (error) {
    throw new Error(
      `Invalid timestamp '${input}'. Use ISO-8601 like 2025-09-24T10:00`,
    );
  }
}

async function calculateTimeRange(args) {
  log.info("Calculating time range for query...");

  const now = Math.floor(Date.now() / 1000);
  let endTime = now;
  let startTime = now - 3600; // Default to 1 hour ago

  if (args.end) {
    endTime = parseIsoToEpoch(args.end);
  }

  if (args.start) {
    startTime = parseIsoToEpoch(args.start);
  }

  if (startTime >= endTime) {
    log.error("--start must be earlier than --end");
    process.exit(1);
  }

  log.info(
    `Query time range: ${new Date(startTime * 1000).toISOString()} to ${new Date(endTime * 1000).toISOString()}`,
  );

  return { startTime, endTime };
}

async function getLogGroupFromStack(stackName, awsConfig) {
  log.info("Querying CloudFormation stack for log group name...");

  const cfClient = new CloudFormationClient(awsConfig);

  try {
    const command = new DescribeStacksCommand({
      StackName: stackName,
    });

    const response = await cfClient.send(command);
    const outputs = response.Stacks?.[0]?.Outputs || [];

    const logGroupOutput = outputs.find(
      (output) => output.OutputKey === "WorkflowPostProcessorLogGroupName",
    );

    if (!logGroupOutput?.OutputValue) {
      throw new Error(
        `Could not find WorkflowPostProcessorLogGroupName output in stack ${stackName}`,
      );
    }

    return logGroupOutput.OutputValue;
  } catch (error) {
    log.error(`Failed to get log group from stack: ${error.message}`);
    throw error;
  }
}

async function runInsightsQuery(
  queryString,
  logGroup,
  startTime,
  endTime,
  awsConfig,
) {
  log.info(`Running CloudWatch Insights query on log group: ${logGroup}`);

  const logsClient = new CloudWatchLogsClient(awsConfig);

  // Check if log group exists
  try {
    // For now, we'll assume the log group exists and proceed
    // In a production version, you might want to check with DescribeLogGroups
  } catch (error) {
    log.error(`Log group ${logGroup} does not exist`);
    throw error;
  }

  const startQueryCommand = new StartQueryCommand({
    startTime,
    endTime,
    queryString,
    logGroupName: logGroup,
  });

  log.debug("Starting query...");
  const startResponse = await logsClient.send(startQueryCommand);
  const queryId = startResponse.queryId;

  if (!queryId) {
    throw new Error("Failed to start CloudWatch Insights query");
  }

  log.info(`Started query with ID: ${queryId}`);

  // Poll for results
  let status = "Running";
  let resultJson;

  while (
    status === "Running" ||
    status === "Scheduled" ||
    status === "Unknown"
  ) {
    await new Promise((resolve) => setTimeout(resolve, 1000)); // Wait 1 second

    const getResultsCommand = new GetQueryResultsCommand({
      queryId,
    });

    resultJson = await logsClient.send(getResultsCommand);
    status = resultJson.status;

    log.debug(`Query status: ${status}`);
  }

  if (status !== "Complete") {
    throw new Error(`Query ${queryId} finished with status: ${status}`);
  }

  log.info("Query completed successfully");

  // Transform results and parse JSON messages
  return (
    resultJson.results?.map((row) => {
      const result = Object.fromEntries(
        row.map((field) => [field.field, field.value]),
      );

      // Parse the JSON message if it exists
      if (result["@message"]) {
        try {
          // Debug: log first message to see format
          if (!global.loggedSample) {
            log.debug(
              `Sample message: ${result["@message"].substring(0, 200)}...`,
            );
            global.loggedSample = true;
          }

          let message = result["@message"].trim();

          // Try to find JSON - CloudWatch logs may have log level prefixes
          // Look for the first '{' and last '}'
          const startBrace = message.indexOf("{");
          const endBrace = message.lastIndexOf("}");

          if (startBrace !== -1 && endBrace !== -1 && endBrace > startBrace) {
            message = message.substring(startBrace, endBrace + 1);
            const parsedMessage = JSON.parse(message);
            return { ...result, ...parsedMessage };
          } else {
            log.debug(`No valid JSON found in message`);
            return result;
          }
        } catch (error) {
          log.debug(`Failed to parse JSON message: ${error.message}`);
          return result;
        }
      }

      return result;
    }) || []
  );
}

function aggregateResults(successRows, failureRows) {
  log.debug("Aggregating results...");

  const toNum = (value) => {
    if (value == null || value === "") return 0;
    const num = parseFloat(value);
    return isNaN(num) ? 0 : num;
  };

  // Initialize result structure
  const result = {};

  // Process success rows - these are individual execution results
  for (const row of successRows) {
    const county = row.county || "unknown";
    if (!result[county]) {
      result[county] = {
        total_successful_transactions: 0,
        success_executions: 0,
        failed_executions: 0,
        failure_counts: {},
      };
    }

    // Count successful executions
    result[county].success_executions += 1;

    // Add transaction count from successful executions
    const transactionCount = toNum(row.transaction_items_count);
    result[county].total_successful_transactions += transactionCount;
  }

  // Process failure rows - these are individual failure events
  for (const row of failureRows) {
    const county = row.county || "unknown";
    if (!result[county]) {
      result[county] = {
        total_successful_transactions: 0,
        success_executions: 0,
        failed_executions: 0,
        failure_counts: {},
      };
    }

    // Count failed executions
    result[county].failed_executions += 1;

    // Track failure counts by step/message type
    const step = row.step || row.msg || row.operation || "unknown";
    result[county].failure_counts[step] =
      (result[county].failure_counts[step] || 0) + 1;
  }

  // Calculate success rates
  for (const county in result) {
    const data = result[county];
    const totalExecutions = data.success_executions + data.failed_executions;
    data.success_rate =
      totalExecutions > 0
        ? (data.success_executions / totalExecutions) * 100
        : 0;
  }

  return result;
}

function formatOutput(aggregated) {
  const formatRate = (successRate, successExecs, failedExecs) => {
    const total = successExecs + failedExecs;
    if (total === 0) return "0.00% (0 successes / 0 runs)";

    const rate = successRate;
    return `${rate.toFixed(2)}% (${successExecs} successes / ${total} runs)`;
  };

  const entries = Object.entries(aggregated);
  if (entries.length === 0) {
    return "No log entries matched the provided criteria.";
  }

  return entries
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([county, data]) => {
      const totalExecutions = data.success_executions + data.failed_executions;
      const failureCounts = Object.entries(data.failure_counts);

      let output = `County: ${county}\n`;
      output += `  Total executions: ${totalExecutions}\n`;
      output += `  Success rate: ${formatRate(data.success_rate, data.success_executions, data.failed_executions)}\n`;
      output += `  Total successful transactions: ${data.total_successful_transactions}\n`;

      if (failureCounts.length > 0) {
        output += "  Failure counts by step:\n";
        output += failureCounts
          .sort(([a], [b]) => a.localeCompare(b))
          .map(([step, count]) => `    ${step}: ${count}`)
          .join("\n");
      } else {
        output += "  Failure counts: none";
      }

      return output;
    })
    .join("\n\n");
}

async function main() {
  try {
    log.info("Starting post-processing logs query");

    await checkDependencies();

    const argv = yargs(hideBin(process.argv))
      .option("stack", {
        alias: "s",
        type: "string",
        default: DEFAULT_STACK_NAME,
        describe: "CloudFormation stack name",
      })
      .option("start", {
        type: "string",
        describe: "ISO-8601 start time",
      })
      .option("end", {
        type: "string",
        describe: "ISO-8601 end time",
      })
      .option("profile", {
        type: "string",
        describe: "AWS profile for credentials",
      })
      .option("region", {
        type: "string",
        describe: "AWS region override",
      })
      .help().argv;

    const timeRange = await calculateTimeRange(argv);

    // Build AWS configuration
    const awsConfig = {};
    if (argv.profile) {
      awsConfig.credentials = {
        profile: argv.profile,
      };
    }
    if (argv.region) {
      awsConfig.region = argv.region;
    }

    log.info(`Using CloudFormation stack: ${argv.stack}`);
    const logGroup = await getLogGroupFromStack(argv.stack, awsConfig);
    log.info(`Found log group: ${logGroup}`);

    log.info("Querying success metrics...");
    const successRows = await runInsightsQuery(
      SUCCESS_QUERY,
      logGroup,
      timeRange.startTime,
      timeRange.endTime,
      awsConfig,
    );

    log.info("Querying failure metrics...");
    const failureRows = await runInsightsQuery(
      FAILURE_QUERY,
      logGroup,
      timeRange.startTime,
      timeRange.endTime,
      awsConfig,
    );

    log.info("Aggregating results...");
    const aggregated = aggregateResults(successRows, failureRows);

    log.info("Formatting output...");
    const output = formatOutput(aggregated);

    console.log(output);
    log.info("Query completed successfully");
  } catch (error) {
    log.error(`Script failed: ${error.message}`);
    if (error.stack) {
      log.debug(error.stack);
    }
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = {
  log,
  SUCCESS_QUERY,
  FAILURE_QUERY,
  checkDependencies,
  parseIsoToEpoch,
  calculateTimeRange,
  getLogGroupFromStack,
  runInsightsQuery,
  aggregateResults,
  formatOutput,
};
