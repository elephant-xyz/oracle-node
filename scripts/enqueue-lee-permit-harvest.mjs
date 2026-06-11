#!/usr/bin/env node

import {
  CloudFormationClient,
  DescribeStacksCommand,
} from "@aws-sdk/client-cloudformation";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";

/**
 * @typedef {object} CliOptions
 * @property {string | undefined} queueUrl - Explicit permit-harvest SQS queue URL.
 * @property {string} stackName - CloudFormation stack name used to discover the queue when queueUrl is omitted.
 * @property {string} jobId - Stable harvest job identifier.
 * @property {string} startDate - Inclusive ISO start date.
 * @property {string} endDate - Inclusive ISO end date.
 * @property {number} windowDays - Number of days in each initial list-window message.
 * @property {string | undefined} outputPrefix - Optional S3 prefix override consumed by the worker.
 * @property {number} splitThreshold - Reported-result threshold that causes multi-day windows to split.
 * @property {number} detailBatchSize - Number of permit detail pages per worker detail-batch message.
 * @property {number} maxPages - Maximum Accela result pages per final window.
 * @property {boolean} dryRun - Print messages without sending them.
 */

/**
 * @typedef {object} DateWindow
 * @property {string} startDate - Inclusive ISO start date.
 * @property {string} endDate - Inclusive ISO end date.
 */

/**
 * @typedef {object} LeePermitListWindowMessage
 * @property {"lee-permit-list-window"} type - Worker message discriminator.
 * @property {1} version - Message contract version.
 * @property {string} jobId - Stable harvest job identifier.
 * @property {string} startDate - Inclusive ISO start date.
 * @property {string} endDate - Inclusive ISO end date.
 * @property {string | undefined} [outputPrefix] - Optional S3 output prefix override.
 * @property {number} maxPages - Maximum Accela result pages per final window.
 * @property {number} detailBatchSize - Number of permit detail pages per worker detail-batch message.
 * @property {number} splitThreshold - Reported-result threshold that causes multi-day windows to split.
 */

const DEFAULT_STACK_NAME = "elephant-oracle-node";
const DEFAULT_START_DATE = "1990-01-01";

/**
 * Print CLI usage instructions.
 *
 * @returns {void}
 */
function showUsage() {
  console.log(`
Usage:
  AWS_PROFILE=elephant-oracle-node node scripts/enqueue-lee-permit-harvest.mjs \
    --job-id lee-permits-YYYYMMDD --start-date 1990-01-01 --end-date 2026-05-25

Options:
  --queue-url <url>        PermitHarvestQueueUrl. If omitted, read from CloudFormation output.
  --stack <name>           CloudFormation stack name. Default: ${DEFAULT_STACK_NAME}
  --job-id <id>            Required stable job id for S3 output prefix partitioning.
  --start-date <date>      Inclusive ISO start date. Default: ${DEFAULT_START_DATE}
  --end-date <date>        Inclusive ISO end date. Default: today UTC.
  --window-days <number>   Initial date-window size. Default: 30
  --output-prefix <s3uri>  Optional S3 output prefix override; worker appends /<job-id>.
  --split-threshold <n>    Split multi-day windows at or above this reported count. Default: 100
  --detail-batch-size <n>  Permit detail pages per detail-batch message. Default: 8
  --max-pages <n>          Max Accela result pages per final window. Default: 200
  --dry-run                Print messages without sending.
  --help                   Show this help.
`);
}

/**
 * Return today's UTC calendar date in ISO format.
 *
 * @returns {string} Current UTC date as YYYY-MM-DD.
 */
function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

/**
 * Parse a positive integer CLI option.
 *
 * @param {string} name - Option name for error messages.
 * @param {string | undefined} value - Raw option value.
 * @param {number} fallback - Default value when raw option is undefined.
 * @returns {number} Parsed positive integer.
 */
function parsePositiveInteger(name, value, fallback) {
  if (value === undefined) return fallback;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer, received ${value}`);
  }
  return parsed;
}

/**
 * Validate and normalize an ISO calendar date.
 *
 * @param {string} name - Option name for error messages.
 * @param {string} value - Raw date value.
 * @returns {string} Normalized ISO date.
 */
function parseIsoDate(name, value) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!match)
    throw new Error(`${name} must be an ISO date in YYYY-MM-DD format`);
  const [, year, month, day] = match;
  const date = new Date(Date.UTC(Number(year), Number(month) - 1, Number(day)));
  if (
    date.getUTCFullYear() !== Number(year) ||
    date.getUTCMonth() !== Number(month) - 1 ||
    date.getUTCDate() !== Number(day)
  ) {
    throw new Error(`${name} is not a valid calendar date: ${value}`);
  }
  return date.toISOString().slice(0, 10);
}

/**
 * Convert an ISO date to a UTC timestamp at midnight.
 *
 * @param {string} value - ISO calendar date.
 * @returns {number} Epoch milliseconds at UTC midnight.
 */
function isoDateToUtcMillis(value) {
  return Date.parse(`${value}T00:00:00Z`);
}

/**
 * Add whole days to an ISO date.
 *
 * @param {string} value - ISO calendar date.
 * @param {number} days - Whole days to add.
 * @returns {string} New ISO calendar date.
 */
function addDays(value, days) {
  return new Date(isoDateToUtcMillis(value) + days * 86400000)
    .toISOString()
    .slice(0, 10);
}

/**
 * Split an inclusive date range into initial SQS list windows.
 *
 * @param {string} startDate - Inclusive ISO start date.
 * @param {string} endDate - Inclusive ISO end date.
 * @param {number} windowDays - Maximum days per initial window.
 * @returns {DateWindow[]} Date windows in ascending order.
 */
function createDateWindows(startDate, endDate, windowDays) {
  if (isoDateToUtcMillis(endDate) < isoDateToUtcMillis(startDate)) {
    throw new Error("--end-date must be greater than or equal to --start-date");
  }

  /** @type {DateWindow[]} */
  const windows = [];
  let cursor = startDate;
  while (isoDateToUtcMillis(cursor) <= isoDateToUtcMillis(endDate)) {
    const candidateEnd = addDays(cursor, windowDays - 1);
    const actualEnd =
      isoDateToUtcMillis(candidateEnd) <= isoDateToUtcMillis(endDate)
        ? candidateEnd
        : endDate;
    windows.push({ startDate: cursor, endDate: actualEnd });
    cursor = addDays(actualEnd, 1);
  }
  return windows;
}

/**
 * Parse command-line arguments into strongly-typed options.
 *
 * @param {string[]} argv - Raw argv excluding node and script path.
 * @returns {CliOptions} Parsed options.
 */
function parseArgs(argv) {
  /** @type {Record<string, string | boolean>} */
  const values = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help" || token === "-h") {
      showUsage();
      process.exit(0);
    }
    if (token === "--dry-run") {
      values.dryRun = true;
      continue;
    }
    if (!token.startsWith("--")) {
      throw new Error(`Unexpected positional argument: ${token}`);
    }
    const key = token.slice(2);
    const next = argv[index + 1];
    if (!next || next.startsWith("--")) {
      throw new Error(`Missing value for ${token}`);
    }
    values[key] = next;
    index += 1;
  }

  const jobIdValue = values["job-id"];
  if (typeof jobIdValue !== "string" || !jobIdValue.trim()) {
    throw new Error("--job-id is required");
  }

  const stackValue = values.stack;
  const startValue = values["start-date"];
  const endValue = values["end-date"];
  const queueUrlValue = values["queue-url"];
  const outputPrefixValue = values["output-prefix"];

  return {
    queueUrl: typeof queueUrlValue === "string" ? queueUrlValue : undefined,
    stackName: typeof stackValue === "string" ? stackValue : DEFAULT_STACK_NAME,
    jobId: jobIdValue.trim(),
    startDate: parseIsoDate(
      "--start-date",
      typeof startValue === "string" ? startValue : DEFAULT_START_DATE,
    ),
    endDate: parseIsoDate(
      "--end-date",
      typeof endValue === "string" ? endValue : todayIsoDate(),
    ),
    windowDays: parsePositiveInteger(
      "--window-days",
      typeof values["window-days"] === "string"
        ? values["window-days"]
        : undefined,
      30,
    ),
    outputPrefix:
      typeof outputPrefixValue === "string" ? outputPrefixValue : undefined,
    splitThreshold: parsePositiveInteger(
      "--split-threshold",
      typeof values["split-threshold"] === "string"
        ? values["split-threshold"]
        : undefined,
      100,
    ),
    detailBatchSize: parsePositiveInteger(
      "--detail-batch-size",
      typeof values["detail-batch-size"] === "string"
        ? values["detail-batch-size"]
        : undefined,
      8,
    ),
    maxPages: parsePositiveInteger(
      "--max-pages",
      typeof values["max-pages"] === "string" ? values["max-pages"] : undefined,
      200,
    ),
    dryRun: values.dryRun === true,
  };
}

/**
 * Read a CloudFormation output value from a deployed stack.
 *
 * @param {CloudFormationClient} client - CloudFormation client.
 * @param {string} stackName - Stack name.
 * @param {string} outputKey - Output key to find.
 * @returns {Promise<string>} Output value.
 */
async function getStackOutput(client, stackName, outputKey) {
  const response = await client.send(
    new DescribeStacksCommand({ StackName: stackName }),
  );
  const output = response.Stacks?.[0]?.Outputs?.find(
    (item) => item.OutputKey === outputKey,
  );
  if (!output?.OutputValue) {
    throw new Error(`Stack ${stackName} does not expose ${outputKey}`);
  }
  return output.OutputValue;
}

/**
 * Build a Lee permit list-window message for the worker.
 *
 * @param {CliOptions} options - Parsed CLI options.
 * @param {DateWindow} window - Date window.
 * @returns {LeePermitListWindowMessage} Worker message.
 */
function buildMessage(options, window) {
  return {
    type: "lee-permit-list-window",
    version: 1,
    jobId: options.jobId,
    startDate: window.startDate,
    endDate: window.endDate,
    outputPrefix: options.outputPrefix,
    maxPages: options.maxPages,
    detailBatchSize: options.detailBatchSize,
    splitThreshold: options.splitThreshold,
  };
}

/**
 * Send all initial Lee permit list-window messages to SQS.
 *
 * @returns {Promise<void>} Resolves when all messages are sent or printed.
 */
async function main() {
  const options = parseArgs(process.argv.slice(2));
  const windows = createDateWindows(
    options.startDate,
    options.endDate,
    options.windowDays,
  );

  const queueUrl = options.queueUrl
    ? options.queueUrl
    : await getStackOutput(
        new CloudFormationClient({}),
        options.stackName,
        "PermitHarvestQueueUrl",
      );

  console.log(
    JSON.stringify({
      level: "info",
      message: "lee_permit_harvest_enqueue_start",
      jobId: options.jobId,
      queueUrl,
      windows: windows.length,
      startDate: options.startDate,
      endDate: options.endDate,
      dryRun: options.dryRun,
    }),
  );

  const sqs = new SQSClient({});
  for (const [index, window] of windows.entries()) {
    const message = buildMessage(options, window);
    if (options.dryRun) {
      console.log(JSON.stringify({ index, message }, null, 2));
      continue;
    }
    const response = await sqs.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(message),
      }),
    );
    console.log(
      JSON.stringify({
        level: "info",
        message: "lee_permit_harvest_window_enqueued",
        index,
        messageId: response.MessageId,
        startDate: window.startDate,
        endDate: window.endDate,
      }),
    );
  }

  console.log(
    JSON.stringify({
      level: "info",
      message: "lee_permit_harvest_enqueue_complete",
      jobId: options.jobId,
      windows: windows.length,
      dryRun: options.dryRun,
    }),
  );
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ level: "error", message }));
  process.exit(1);
});
