#!/usr/bin/env node

import {
  CloudFormationClient,
  DescribeStacksCommand,
} from "@aws-sdk/client-cloudformation";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";

/**
 * Pinellas appraisal-only property-first seed-feeder sender.
 *
 * Sends ONE Pinellas feeder message to the permit-harvest queue. The worker
 * self-requeues and drips the full tax-parcel seed CSV through the appraisal
 * pipeline with workflow-queue backpressure. Permit harvest is out of scope
 * for this run — set PROPERTY_FIRST_PERMIT_ELIGIBLE_USAGE_TYPES_PINELLAS=__NONE__
 * on the permit-harvest worker so commercial parcels do not enqueue Accela work.
 *
 * Per-parcel workflow routing uses the seed CSV `county` column ("Pinellas")
 * → `elephant-oracle-node-prepare-queue-pinellas`.
 */
export const COUNTY = {
  /** Feeder message discriminator routed by the permit-harvest worker. */
  feederType: "pinellas-property-first-seed-feeder",
  /** Neon properties.source_system used by the worker's skipExistingNeon dedup. */
  sourceSystem: "pinellas_appraiser",
  /** Slug used for generated S3 prefixes so the loader prefix is predictable. */
  slug: "pinellas-property-first-seed",
  /**
   * Per-county prepare queue name. Created with
   * `MAX_CONCURRENCY=2 ./scripts/create-county-prepare-queue.sh Pinellas`.
   */
  prepareQueueName: "elephant-oracle-node-prepare-queue-pinellas",
};

const DEFAULT_STACK_NAME = "elephant-oracle-node";
const DEFAULT_PERMIT_STACK_NAME = "elephant-permit-harvest";
export const DEFAULT_SOURCE_CSV_S3_URI = "s3://counties-seeds/pinellas.csv";
const DEFAULT_BATCH_SIZE = 500;
const DEFAULT_REQUEUE_DELAY_SECONDS = 45;
const DEFAULT_WORKFLOW_MAX_MESSAGES = 400;

/**
 * Suggested (NOT default) jobId following the run-naming convention.
 *
 * @returns {string} A convention-following jobId for today's date.
 */
export function suggestedJobId() {
  const date = new Date().toISOString().slice(0, 10).replace(/-/g, "");
  return `pinellas-property-first-seed-all-${date}`;
}

/**
 * @typedef {object} PinellasFeederCliOptions
 * @property {string} stackName - Oracle-node CloudFormation stack name.
 * @property {string} permitStackName - Permit-harvest CloudFormation stack name.
 * @property {string} sourceCsvS3Uri - Pinellas seed CSV S3 URI.
 * @property {string} jobId - REQUIRED fixed run identifier used for S3 partitioning.
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
  AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \\
    node scripts/send-pinellas-seed-feeder.mjs --job-id <id> [--dry-run]

Options:
  --stack <name>          Oracle-node stack. Default: ${DEFAULT_STACK_NAME}
  --permit-stack <name>   Permit-harvest stack. Default: ${DEFAULT_PERMIT_STACK_NAME}
  --source-csv-s3-uri <s3uri>
                          Pinellas seed CSV. Default: ${DEFAULT_SOURCE_CSV_S3_URI}
  --job-id <id>           REQUIRED. Stable run id, FIXED for the whole run.
                          Suggested: ${suggestedJobId()}
  --dry-run               Print the feeder message JSON without sending to SQS.
  --help                  Show this help.

Sends ONE feeder message to the permit-harvest queue. Appraisal-only: do not
enable Pinellas permit eligibility until the county Accela adapter is certified.
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
 * @returns {PinellasFeederCliOptions} Parsed CLI options.
 */
export function parseArgs(argv) {
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
        "default: a re-send after 00:00 UTC would start a BRAND-NEW job from row 0. " +
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
  const cloudFormation = new CloudFormationClient({});
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
 * Build the Pinellas property-first seed-feeder message from resolved infra.
 *
 * @param {object} params - Message build parameters.
 * @param {PinellasFeederCliOptions} params.cli - Parsed CLI options.
 * @param {string} params.environmentBucketName - Output/seed/state S3 bucket name.
 * @param {string} params.workflowQueueUrl - Appraisal workflow starter queue URL.
 * @param {string} params.propertyFirstPermitQueueUrl - Property-first permit queue URL.
 * @param {string} params.feederQueueUrl - Permit-harvest queue the feeder reschedules onto.
 * @returns {Record<string, unknown>} Feeder message body.
 */
export function buildFeederMessage({
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

  const sqs = new SQSClient({});
  const response = await sqs.send(
    new SendMessageCommand({
      QueueUrl: feederQueueUrl,
      MessageBody: JSON.stringify(feederMessage),
    }),
  );
  console.log(
    JSON.stringify({
      level: "info",
      message: "pinellas_property_first_seed_feeder_sent",
      jobId: cli.jobId,
      feederQueueUrl,
      messageId: response.MessageId,
    }),
  );
}

const isDirectRun = process.argv[1]
  ? new URL(`file://${process.argv[1]}`).href === import.meta.url ||
    process.argv[1].endsWith("send-pinellas-seed-feeder.mjs")
  : false;

if (isDirectRun) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(JSON.stringify({ level: "error", message }));
    process.exit(1);
  });
}
