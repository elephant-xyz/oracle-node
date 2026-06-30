#!/usr/bin/env node

import {
  CloudFormationClient,
  DescribeStacksCommand,
} from "@aws-sdk/client-cloudformation";
import {
  GetQueueUrlCommand,
  SendMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";

/**
 * Palm Beach property-first seed-feeder sender.
 *
 * Builds and sends ONE Palm Beach feeder message to the permit-harvest queue.
 * The feeder is the self-requeuing full-run trigger: it drips the whole Palm
 * Beach seed CSV through the appraisal -> permit pipeline with backpressure,
 * checkpointing progress in S3 and rescheduling itself with an SQS delay until
 * the county is exhausted.
 *
 * Queue URLs and the environment bucket are resolved from the same
 * CloudFormation stacks the enqueue scripts use. Per-parcel workflow routing is
 * derived by the prepare state machine from the seed CSV's `county` column
 * (-> `elephant-oracle-node-prepare-queue-palm-beach`), not from this message.
 */
const COUNTY = {
  /** Feeder message discriminator routed by the permit-harvest worker. */
  feederType: "palm-beach-property-first-seed-feeder",
  /** Neon properties.source_system used by the worker's skipExistingNeon dedup. */
  sourceSystem: "palm_beach_appraiser",
  /** Slug used for generated S3 prefixes so the loader prefix is predictable. */
  slug: "palm-beach-property-first-seed",
  /** Per-county prepare queue name resolved for backpressure. */
  prepareQueueName: "elephant-oracle-node-prepare-queue-palm-beach",
};

const DEFAULT_STACK_NAME = "elephant-oracle-node";
const DEFAULT_PERMIT_STACK_NAME = "elephant-permit-harvest";
const DEFAULT_SOURCE_CSV_S3_URI = "s3://counties-seeds/palm_beach.csv";
const DEFAULT_JOB_ID = `palm-beach-property-first-seed-all-${new Date()
  .toISOString()
  .slice(0, 10)
  .replace(/-/g, "")}`;

const DEFAULT_BATCH_SIZE = 100;
const DEFAULT_REQUEUE_DELAY_SECONDS = 900;
const DEFAULT_WORKFLOW_MAX_MESSAGES = 250;
const DEFAULT_PREPARE_MAX_MESSAGES = 5000;

const cloudFormation = new CloudFormationClient({});
const sqs = new SQSClient({});

/**
 * @typedef {object} CliOptions
 * @property {string} stackName - Oracle-node CloudFormation stack name.
 * @property {string} permitStackName - Permit-harvest CloudFormation stack name.
 * @property {string} sourceCsvS3Uri - Palm Beach seed CSV S3 URI.
 * @property {string} jobId - Stable run identifier used for S3 partitioning.
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
    node scripts/send-palm-beach-seed-feeder.mjs [--dry-run] [--job-id <id>]

Options:
  --stack <name>          Oracle-node stack. Default: ${DEFAULT_STACK_NAME}
  --permit-stack <name>   Permit-harvest stack. Default: ${DEFAULT_PERMIT_STACK_NAME}
  --source-csv-s3-uri <s3uri>
                          Palm Beach seed CSV. Default: ${DEFAULT_SOURCE_CSV_S3_URI}
  --job-id <id>           Stable run id. Default: ${DEFAULT_JOB_ID}
  --dry-run               Print the feeder message JSON without sending to SQS.
  --help                  Show this help.

Sends ONE feeder message to the permit-harvest queue. The worker then self-
requeues, dripping the whole Palm Beach seed through the pipeline with workflow
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

  return {
    stackName: readOptionalString(values.stack) ?? DEFAULT_STACK_NAME,
    permitStackName:
      readOptionalString(values["permit-stack"]) ?? DEFAULT_PERMIT_STACK_NAME,
    sourceCsvS3Uri:
      readOptionalString(values["source-csv-s3-uri"]) ??
      DEFAULT_SOURCE_CSV_S3_URI,
    jobId: readOptionalString(values["job-id"]) ?? DEFAULT_JOB_ID,
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
 * Resolve an SQS queue URL from its name.
 *
 * @param {string} queueName - SQS queue name.
 * @returns {Promise<string>} Queue URL.
 */
async function getQueueUrl(queueName) {
  const response = await sqs.send(
    new GetQueueUrlCommand({ QueueName: queueName }),
  );
  if (!response.QueueUrl) {
    throw new Error(`Could not resolve queue URL for ${queueName}`);
  }
  return response.QueueUrl;
}

/**
 * Build the Palm Beach property-first seed-feeder message from resolved infra.
 *
 * @param {object} params - Message build parameters.
 * @param {CliOptions} params.cli - Parsed CLI options.
 * @param {string} params.environmentBucketName - Output/seed/state S3 bucket name.
 * @param {string} params.workflowQueueUrl - Appraisal workflow starter queue URL.
 * @param {string} params.propertyFirstPermitQueueUrl - Property-first permit queue URL.
 * @param {string} params.feederQueueUrl - Permit-harvest queue the feeder reschedules onto.
 * @param {string} params.prepareQueueUrl - Palm Beach prepare queue URL for backpressure.
 * @returns {Record<string, unknown>} Feeder message body.
 */
function buildFeederMessage({
  cli,
  environmentBucketName,
  workflowQueueUrl,
  propertyFirstPermitQueueUrl,
  feederQueueUrl,
  prepareQueueUrl,
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
    backpressureQueues: [
      {
        name: "workflow",
        queueUrl: workflowQueueUrl,
        maxMessages: DEFAULT_WORKFLOW_MAX_MESSAGES,
      },
      {
        name: "prepare",
        queueUrl: prepareQueueUrl,
        maxMessages: DEFAULT_PREPARE_MAX_MESSAGES,
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
    prepareQueueUrl,
  ] = await Promise.all([
    getStackOutput(cli.stackName, "EnvironmentBucketName"),
    getStackOutput(cli.stackName, "WorkflowQueueUrl"),
    getStackOutput(cli.permitStackName, "PropertyFirstPermitQueueUrl"),
    getStackOutput(cli.permitStackName, "PermitHarvestQueueUrl"),
    getQueueUrl(COUNTY.prepareQueueName),
  ]);

  const feederMessage = buildFeederMessage({
    cli,
    environmentBucketName,
    workflowQueueUrl,
    propertyFirstPermitQueueUrl,
    feederQueueUrl,
    prepareQueueUrl,
  });

  if (cli.dryRun) {
    console.log(
      JSON.stringify(
        { dryRun: true, feederQueueUrl, feederMessage },
        null,
        2,
      ),
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
      message: "palm_beach_property_first_seed_feeder_sent",
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
