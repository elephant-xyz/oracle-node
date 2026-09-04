#!/usr/bin/env node
// @ts-check

/**
 * Enqueue initial Broward Accela date windows into the tenant-isolated FIFO queue.
 *
 * Each jurisdiction is its own MessageGroupId. SQS therefore serializes source
 * traffic per Accela tenant while Lambda may process different tenants in
 * parallel. Dense windows split inside the worker using the same group.
 */

import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";

import {
  CloudFormationClient,
  DescribeStacksCommand,
} from "@aws-sdk/client-cloudformation";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";

const DEFAULT_STACK_NAME = "elephant-permit-harvest";
const DATE_ENABLED_SOURCES = new Set([
  "hollywood",
  "plantation",
  "cooper-city",
  "weston",
]);

/**
 * @typedef {"hollywood" | "plantation" | "cooper-city" | "weston"} BrowardAccelaDateSourceKey
 *
 * @typedef {object} DateWindow
 * @property {string} startDate - Inclusive ISO start.
 * @property {string} endDate - Inclusive ISO end.
 *
 * @typedef {object} EnqueueBrowardAccelaOptions
 * @property {BrowardAccelaDateSourceKey} sourceKey - FIFO tenant group.
 * @property {string} jobId - Stable S3 run identity.
 * @property {string} startDate - Inclusive ISO range start.
 * @property {string} endDate - Inclusive ISO range end.
 * @property {number} windowDays - Initial window width.
 * @property {number} splitThreshold - Dense-window split threshold.
 * @property {number} maxPages - Terminal pagination ceiling.
 * @property {string | undefined} queueUrl - Explicit queue URL.
 * @property {string} stackName - CloudFormation stack for queue discovery.
 * @property {string | undefined} outputPrefix - Optional S3 prefix.
 * @property {boolean} dryRun - Print without sending.
 *
 * @typedef {object} BrowardAccelaListWindowMessage
 * @property {"broward-accela-list-window"} type - Worker discriminator.
 * @property {1} version - Message version.
 * @property {string} jobId - Stable S3 run identity.
 * @property {BrowardAccelaDateSourceKey} jurisdictionKey - FIFO source group.
 * @property {string} startDate - Inclusive ISO start.
 * @property {string} endDate - Inclusive ISO end.
 * @property {number} maxPages - Terminal page ceiling.
 * @property {number} splitThreshold - Dense-window split threshold.
 * @property {string | undefined} [outputPrefix] - Optional S3 prefix.
 */

/**
 * Parse an explicit source/date enqueue command.
 *
 * @param {readonly string[]} argv - Arguments after the script path.
 * @returns {EnqueueBrowardAccelaOptions} Validated options.
 */
export function parseEnqueueBrowardAccelaOptions(argv) {
  /** @type {Map<string,string | boolean>} */
  const values = new Map();
  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index];
    if (flag === "--dry-run") {
      values.set("dry-run", true);
      continue;
    }
    if (typeof flag !== "string" || !flag.startsWith("--")) {
      throw new Error("Broward Accela enqueue options must use --flags");
    }
    const value = argv[index + 1];
    if (typeof value !== "string" || value.startsWith("--")) {
      throw new Error(`Missing value for ${flag}`);
    }
    values.set(flag.slice(2), value);
    index += 1;
  }
  const rawSource = values.get("source");
  if (typeof rawSource !== "string" || !DATE_ENABLED_SOURCES.has(rawSource)) {
    throw new Error(
      "--source must be hollywood, plantation, cooper-city, or weston",
    );
  }
  const sourceKey = /** @type {BrowardAccelaDateSourceKey} */ (rawSource);
  const rawJobId = values.get("job-id");
  if (
    typeof rawJobId !== "string" ||
    !/^broward-permits-[a-z0-9-]+$/u.test(rawJobId)
  ) {
    throw new Error("--job-id must begin broward-permits-");
  }
  const startDate = requireIsoDate(values.get("start-date"), "--start-date");
  const endDate = requireIsoDate(values.get("end-date"), "--end-date");
  if (isoDateToMillis(endDate) < isoDateToMillis(startDate)) {
    throw new Error("--end-date must not precede --start-date");
  }
  const rawQueueUrl = values.get("queue-url");
  const rawStackName = values.get("stack");
  const rawOutputPrefix = values.get("output-prefix");
  return {
    sourceKey,
    jobId: rawJobId,
    startDate,
    endDate,
    windowDays: boundedInteger(
      values.get("window-days") ?? "30",
      "window-days",
      1,
      366,
    ),
    splitThreshold: boundedInteger(
      values.get("split-threshold") ?? "100",
      "split-threshold",
      2,
      10_000,
    ),
    maxPages: boundedInteger(
      values.get("max-pages") ?? "200",
      "max-pages",
      1,
      200,
    ),
    queueUrl: typeof rawQueueUrl === "string" ? rawQueueUrl : undefined,
    stackName:
      typeof rawStackName === "string" ? rawStackName : DEFAULT_STACK_NAME,
    outputPrefix:
      typeof rawOutputPrefix === "string" ? rawOutputPrefix : undefined,
    dryRun: values.get("dry-run") === true,
  };
}

/**
 * Create exhaustive non-overlapping initial windows.
 *
 * @param {string} startDate - Inclusive ISO start.
 * @param {string} endDate - Inclusive ISO end.
 * @param {number} windowDays - Maximum window width.
 * @returns {DateWindow[]} Chronological windows.
 */
export function createEnqueueDateWindows(startDate, endDate, windowDays) {
  if (!Number.isInteger(windowDays) || windowDays < 1) {
    throw new Error("windowDays must be a positive integer");
  }
  /** @type {DateWindow[]} */
  const windows = [];
  let cursor = startDate;
  while (isoDateToMillis(cursor) <= isoDateToMillis(endDate)) {
    const candidateEnd = addDays(cursor, windowDays - 1);
    const actualEnd =
      isoDateToMillis(candidateEnd) > isoDateToMillis(endDate)
        ? endDate
        : candidateEnd;
    windows.push({ startDate: cursor, endDate: actualEnd });
    cursor = addDays(actualEnd, 1);
  }
  return windows;
}

/**
 * Build one worker message.
 *
 * @param {EnqueueBrowardAccelaOptions} options - Enqueue configuration.
 * @param {DateWindow} window - Initial source window.
 * @returns {BrowardAccelaListWindowMessage} Versioned message.
 */
export function buildBrowardAccelaWindowMessage(options, window) {
  return {
    type: "broward-accela-list-window",
    version: 1,
    jobId: options.jobId,
    jurisdictionKey: options.sourceKey,
    startDate: window.startDate,
    endDate: window.endDate,
    maxPages: options.maxPages,
    splitThreshold: options.splitThreshold,
    outputPrefix: options.outputPrefix,
  };
}

/**
 * Enqueue or print all initial windows.
 *
 * @param {EnqueueBrowardAccelaOptions} options - Validated options.
 * @param {{
 *   cloudFormation?:CloudFormationClient,
 *   sqs?:SQSClient,
 *   log?:(value:Record<string,unknown>)=>void
 * }} [dependencies={}] - Injectable AWS clients and aggregate-safe logger.
 * @returns {Promise<{queueUrl:string,windowCount:number,dryRun:boolean}>}
 *   Enqueue summary.
 */
export async function enqueueBrowardAccelaDateWindows(
  options,
  dependencies = {},
) {
  const cloudFormation =
    dependencies.cloudFormation ?? new CloudFormationClient({});
  const sqs = dependencies.sqs ?? new SQSClient({});
  const log =
    dependencies.log ??
    ((value) => {
      console.log(JSON.stringify(value));
    });
  const queueUrl =
    options.queueUrl ??
    (await readStackOutput(
      cloudFormation,
      options.stackName,
      "BrowardPermitEnumerationQueueUrl",
    ));
  if (!queueUrl.endsWith(".fifo")) {
    throw new Error("Broward enumeration queue must be FIFO");
  }
  const windows = createEnqueueDateWindows(
    options.startDate,
    options.endDate,
    options.windowDays,
  );
  for (const [index, window] of windows.entries()) {
    const message = buildBrowardAccelaWindowMessage(options, window);
    if (!options.dryRun) {
      await sqs.send(
        new SendMessageCommand({
          QueueUrl: queueUrl,
          MessageBody: JSON.stringify(message),
          MessageGroupId: options.sourceKey,
          MessageDeduplicationId: deduplicationId(message),
        }),
      );
    }
    log({
      event: "broward_accela_window_enqueued",
      index,
      sourceKey: options.sourceKey,
      startDate: window.startDate,
      endDate: window.endDate,
      dryRun: options.dryRun,
    });
  }
  return {
    queueUrl,
    windowCount: windows.length,
    dryRun: options.dryRun,
  };
}

/**
 * Read one non-empty CloudFormation output.
 *
 * @param {CloudFormationClient} client - AWS client.
 * @param {string} stackName - Stack name.
 * @param {string} outputKey - Required output key.
 * @returns {Promise<string>} Output value.
 */
async function readStackOutput(client, stackName, outputKey) {
  const response = await client.send(
    new DescribeStacksCommand({ StackName: stackName }),
  );
  const value = response.Stacks?.[0]?.Outputs?.find(
    (output) => output.OutputKey === outputKey,
  )?.OutputValue;
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`Stack ${stackName} does not expose ${outputKey}`);
  }
  return value;
}

/**
 * Build a deterministic FIFO deduplication identity.
 *
 * @param {BrowardAccelaListWindowMessage} message - Exact source window.
 * @returns {string} SHA-256 accepted by SQS FIFO.
 */
function deduplicationId(message) {
  return createHash("sha256")
    .update(
      `${message.jobId}:${message.jurisdictionKey}:${message.startDate}:${message.endDate}`,
    )
    .digest("hex");
}

/**
 * Validate an ISO calendar date.
 *
 * @param {unknown} value - Candidate date.
 * @param {string} name - Field name for errors.
 * @returns {string} Validated date.
 */
function requireIsoDate(value, name) {
  if (typeof value !== "string") throw new Error(`${name} is required`);
  const match = /^(\d{4})-(\d{2})-(\d{2})$/u.exec(value);
  if (match === null) throw new Error(`${name} must be YYYY-MM-DD`);
  const date = new Date(
    Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])),
  );
  if (
    date.getUTCFullYear() !== Number(match[1]) ||
    date.getUTCMonth() !== Number(match[2]) - 1 ||
    date.getUTCDate() !== Number(match[3])
  ) {
    throw new Error(`${name} is not a valid calendar date`);
  }
  return value;
}

/**
 * Convert ISO date to UTC midnight.
 *
 * @param {string} value - ISO calendar date.
 * @returns {number} Epoch milliseconds.
 */
function isoDateToMillis(value) {
  return Date.parse(`${value}T00:00:00Z`);
}

/**
 * Add whole UTC days.
 *
 * @param {string} value - ISO date.
 * @param {number} days - Whole day delta.
 * @returns {string} Shifted ISO date.
 */
function addDays(value, days) {
  return new Date(isoDateToMillis(value) + days * 86_400_000)
    .toISOString()
    .slice(0, 10);
}

/**
 * Parse an inclusive bounded integer.
 *
 * @param {string | boolean} raw - Raw option value.
 * @param {string} name - Option name.
 * @param {number} minimum - Inclusive minimum.
 * @param {number} maximum - Inclusive maximum.
 * @returns {number} Validated integer.
 */
function boundedInteger(raw, name, minimum, maximum) {
  const value = Number(raw);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(
      `--${name} must be an integer from ${String(minimum)} through ${String(maximum)}`,
    );
  }
  return value;
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  enqueueBrowardAccelaDateWindows(
    parseEnqueueBrowardAccelaOptions(process.argv.slice(2)),
  )
    .then((summary) => {
      console.log(
        JSON.stringify({
          event: "broward_accela_windows_enqueue_complete",
          ...summary,
        }),
      );
    })
    .catch((error) => {
      console.error(
        JSON.stringify({
          event: "broward_accela_windows_enqueue_failed",
          message: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      process.exitCode = 1;
    });
}
