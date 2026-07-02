#!/usr/bin/env node

import {
  CloudFormationClient,
  DescribeStacksCommand,
} from "@aws-sdk/client-cloudformation";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";

/**
 * Orange property-first seed-feeder sender.
 *
 * Builds and sends ONE Orange feeder message to the permit-harvest queue.
 * The feeder is the self-requeuing full-run trigger: it drips the whole Orange
 * seed CSV through the appraisal -> permit pipeline with backpressure,
 * checkpointing progress in S3 and rescheduling itself with an SQS delay until
 * the county is exhausted.
 *
 * Queue URLs and the environment bucket are resolved from the same
 * CloudFormation stacks the enqueue scripts use. Per-parcel workflow routing is
 * derived by the prepare state machine from the seed CSV's `county` column
 * (-> `elephant-oracle-node-prepare-queue-orange`), not from this message.
 */
const COUNTY = {
  /** Feeder message discriminator routed by the permit-harvest worker. */
  feederType: "orange-property-first-seed-feeder",
  /** Neon properties.source_system used by the worker's skipExistingNeon dedup. */
  sourceSystem: "orange_appraiser",
  /** Slug used for generated S3 prefixes so the loader prefix is predictable. */
  slug: "orange-property-first-seed",
  /**
   * Per-county prepare queue name. Kept for documentation / future re-add of
   * prepare-queue backpressure — see the omission note in buildFeederMessage.
   */
  prepareQueueName: "elephant-oracle-node-prepare-queue-orange",
};

const DEFAULT_STACK_NAME = "elephant-oracle-node";
const DEFAULT_PERMIT_STACK_NAME = "elephant-permit-harvest";
const DEFAULT_SOURCE_CSV_S3_URI = "s3://counties-seeds/orange.csv";
/**
 * Suggested (NOT default) jobId following the run-naming convention. Returned only in
 * usage/error text so the operator can copy it. Deliberately NOT applied automatically:
 * a date-derived jobId silently splits a multi-day run — a re-send after 00:00 UTC (manual
 * or by the watchdog) would build a new date and start a BRAND-NEW job from row 0 instead
 * of resuming the frozen one. A fixed `--job-id` is therefore REQUIRED (see parseArgs).
 *
 * @returns {string} A convention-following jobId for today's date.
 */
function suggestedJobId() {
  const date = new Date().toISOString().slice(0, 10).replace(/-/g, "");
  return `orange-property-first-seed-all-${date}`;
}

// Seed CSV is ~282 MiB / 654k rows and the worker re-streams it from row 1 each
// wakeup to skip to the checkpoint. Use a LARGE batch so that expensive scan is
// amortized over many enqueues per wakeup (not re-paid per 200 rows). 2000 rows
// of seed-upload + workflow-enqueue fits comfortably inside the 900s/4GB worker.
const DEFAULT_BATCH_SIZE = 2000;
const DEFAULT_REQUEUE_DELAY_SECONDS = 45;
const DEFAULT_WORKFLOW_MAX_MESSAGES = 2000;
const DEFAULT_PREPARE_MAX_MESSAGES = 5000;

const cloudFormation = new CloudFormationClient({});
const sqs = new SQSClient({});

/**
 * @typedef {object} CliOptions
 * @property {string} stackName - Oracle-node CloudFormation stack name.
 * @property {string} permitStackName - Permit-harvest CloudFormation stack name.
 * @property {string} sourceCsvS3Uri - Orange seed CSV S3 URI.
 * @property {string} jobId - REQUIRED fixed run identifier used for S3 partitioning; must
 *   stay constant across every (re-)send so the run resumes instead of splitting.
 * @property {boolean} dryRun - Print the feeder message without sending it.
 */

/**
 * Print CLI usage instructions.
 *
 * @returns {void} Writes usage text to stdout.
 */
function showUsage() {
  console.log(`
Usage:
  AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \
    node scripts/send-orange-seed-feeder.mjs --job-id <id> [--dry-run]

Options:
  --stack <name>          Oracle-node stack. Default: ${DEFAULT_STACK_NAME}
  --permit-stack <name>   Permit-harvest stack. Default: ${DEFAULT_PERMIT_STACK_NAME}
  --source-csv-s3-uri <s3uri>
                          Orange seed CSV. Default: ${DEFAULT_SOURCE_CSV_S3_URI}
  --job-id <id>           REQUIRED. Stable run id, FIXED for the whole run. Do not rely on a
                          date default: a re-send after 00:00 UTC would start a new job from
                          row 0 and silently split the run. Suggested: ${suggestedJobId()}
  --dry-run               Print the feeder message JSON without sending to SQS.
  --help                  Show this help.

Sends ONE feeder message to the permit-harvest queue. The worker then self-
requeues, dripping the whole Orange seed through the pipeline with workflow
and prepare backpressure until the county is exhausted.
`);
}

/**
 * Return a trimmed string when a value is a non-empty string.
 *
 * @param {string | undefined} value - Candidate CLI value.
 * @returns {string | undefined} Trimmed string when usable.
 */
function readOptionalString(value) {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

/**
 * Parse command-line arguments into typed options.
 *
 * @param {string[]} argv - Raw argv excluding node and script path.
 * @returns {CliOptions} Parsed CLI options.
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
    if (next === undefined || next.startsWith("--")) {
      throw new Error(`Missing value for ${token}`);
    }
    values[key] = next;
    index += 1;
  }

  const jobId = readOptionalString(values["job-id"]);
  if (!jobId) {
    throw new Error(
      "--job-id is REQUIRED and must be FIXED for the whole run. Do not rely on a date " +
        "default: a re-send after 00:00 UTC (manual or by the watchdog) would start a " +
        "BRAND-NEW job from row 0 and silently split the run. " +
        `Suggested: --job-id ${suggestedJobId()}`,
    );
  }

  return {
    stackName: readOptionalString(values.stack) ?? DEFAULT_STACK_NAME,
    permitStackName:
      readOptionalString(values["permit-stack"]) ?? DEFAULT_PERMIT_STACK_NAME,
    sourceCsvS3Uri:
      readOptionalString(values["source-csv-s3-uri"]) ??
      DEFAULT_SOURCE_CSV_S3_URI,
    jobId,
    dryRun: values.dryRun === true,
  };
}

/**
 * Read a CloudFormation output value from a deployed stack.
 *
 * @param {string} stackName - CloudFormation stack name.
 * @param {string} outputKey - Output key to find.
 * @returns {Promise<string>} Output value.
 */
async function getStackOutput(stackName, outputKey) {
  const response = await cloudFormation.send(
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
 * Build the Orange property-first seed-feeder message from resolved infra.
 *
 * @param {object} params - Message build parameters.
 * @param {CliOptions} params.cli - Parsed CLI options.
 * @param {string} params.environmentBucketName - Output/seed/state S3 bucket name.
 * @param {string} params.workflowQueueUrl - Appraisal workflow starter queue URL.
 * @param {string} params.propertyFirstPermitQueueUrl - Property-first permit queue URL.
 * @param {string} params.feederQueueUrl - Permit-harvest queue the feeder reschedules onto.
 * @returns {Record<string, unknown>} Feeder message body.
 */
function buildFeederMessage({
  cli,
  environmentBucketName,
  workflowQueueUrl,
  propertyFirstPermitQueueUrl,
  feederQueueUrl,
}) {
  return {
    type: COUNTY.feederType,
    version: 1,
    sourceSystem: COUNTY.sourceSystem,
    jobId: cli.jobId,
    sourceCsvS3Uri: cli.sourceCsvS3Uri,
    workflowQueueUrl,
    propertyFirstPermitQueueUrl,
    feederQueueUrl,
    generatedSeedPrefix: `s3://${environmentBucketName}/seed-inputs/${COUNTY.slug}/${cli.jobId}`,
    workflowOutputBaseUri: `s3://${environmentBucketName}/outputs/${COUNTY.slug}/${cli.jobId}`,
    propertyFirstPermitOutputPrefix: `s3://${environmentBucketName}/permit-harvest/${COUNTY.slug}`,
    stateS3Uri: `s3://${environmentBucketName}/permit-harvest/${cli.jobId}/feeder-state.json`,
    batchSize: DEFAULT_BATCH_SIZE,
    requeueDelaySeconds: DEFAULT_REQUEUE_DELAY_SECONDS,
    // NOTE: prepare-queue backpressure intentionally omitted — the permit-harvest
    // worker role lacks sqs:GetQueueAttributes on the per-county PB prepare queue
    // (would crash the feeder with AccessDenied). Workflow-queue backpressure
    // (the starter that feeds prepare) still governs overall flow. Re-add the
    // prepare entry once the role is granted GetQueueAttributes on the PB queue.
    backpressureQueues: [
      {
        name: "workflow",
        queueUrl: workflowQueueUrl,
        maxMessages: DEFAULT_WORKFLOW_MAX_MESSAGES,
      },
    ],
  };
}

/**
 * CLI entry point.
 *
 * @returns {Promise<void>} Resolves after the feeder message is printed or sent.
 */
async function main() {
  const cli = parseArgs(process.argv.slice(2));
  const [
    environmentBucketName,
    workflowQueueUrl,
    propertyFirstPermitQueueUrl,
    feederQueueUrl,
  ] = await Promise.all([
    getStackOutput(cli.stackName, "EnvironmentBucketName"),
    getStackOutput(cli.stackName, "WorkflowQueueUrl"),
    getStackOutput(cli.permitStackName, "PropertyFirstPermitQueueUrl"),
    getStackOutput(cli.permitStackName, "PermitHarvestQueueUrl"),
    // NOTE: the PB prepare queue URL is intentionally NOT resolved here — prepare-
    // queue backpressure is omitted because the permit-harvest worker role lacks
    // sqs:GetQueueAttributes on the per-county PB prepare queue (see the omission
    // note in buildFeederMessage). To re-add once the role is granted the
    // permission, resolve it with getQueueUrl(COUNTY.prepareQueueName) and thread
    // it into buildFeederMessage's backpressureQueues.
  ]);

  const feederMessage = buildFeederMessage({
    cli,
    environmentBucketName,
    workflowQueueUrl,
    propertyFirstPermitQueueUrl,
    feederQueueUrl,
  });

  if (cli.dryRun) {
    console.log(
      JSON.stringify({ dryRun: true, feederQueueUrl, feederMessage }, null, 2),
    );
    return;
  }

  const response = await sqs.send(
    new SendMessageCommand({
      QueueUrl: feederQueueUrl,
      MessageBody: JSON.stringify(feederMessage),
    }),
  );
  console.log(
    JSON.stringify({
      level: "info",
      message: "orange_property_first_seed_feeder_sent",
      jobId: cli.jobId,
      feederQueueUrl,
      messageId: response.MessageId,
    }),
  );
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ level: "error", message }));
  process.exit(1);
});
