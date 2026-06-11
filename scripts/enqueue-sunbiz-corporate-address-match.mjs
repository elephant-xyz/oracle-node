#!/usr/bin/env node

import {
  CloudFormationClient,
  DescribeStacksCommand,
} from "@aws-sdk/client-cloudformation";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { readFile } from "fs/promises";
import path from "path";

/**
 * @typedef {object} AddressInput
 * @property {string | undefined} [inputId] - Stable identifier for the address.
 * @property {string | undefined} [source] - Optional provenance label.
 * @property {string} rawAddress - Address text to match.
 * @property {string | undefined} [city] - Optional city filter.
 * @property {string | undefined} [state] - Optional state filter.
 * @property {string | undefined} [zip] - Optional ZIP filter.
 */

/**
 * @typedef {object} CliOptions
 * @property {string | undefined} queueUrl - Explicit permit-harvest SQS queue URL.
 * @property {string} stackName - CloudFormation stack name used to discover the queue when queueUrl is omitted.
 * @property {string} jobId - Stable harvest job identifier.
 * @property {string} corporateDataS3Uri - S3 URI of Sunbiz corporate fixed-width text or ZIP object.
 * @property {"text" | "zip" | undefined} sourceFormat - Optional source object format.
 * @property {string} addressJsonPath - Local JSON file containing an array of address inputs.
 * @property {string | undefined} outputPrefix - Optional S3 output prefix override consumed by the worker.
 * @property {number} batchSize - Address inputs per SQS message.
 * @property {number} maxMatchesPerAddress - Per-address match cap.
 * @property {boolean} dryRun - Print messages without sending.
 */

/**
 * @typedef {object} SunbizCorporateAddressMatchMessage
 * @property {"sunbiz-corporate-address-match"} type - Worker message discriminator.
 * @property {1} version - Message contract version.
 * @property {string} jobId - Stable harvest job identifier.
 * @property {string} addressBatchKey - Stable address batch key for S3 output.
 * @property {string} sourceDataS3Uri - S3 URI for Sunbiz source data.
 * @property {"text" | "zip" | undefined} [sourceFormat] - Optional source object format.
 * @property {AddressInput[]} addressInputs - Addresses to match.
 * @property {number} maxMatchesPerAddress - Per-address match cap.
 * @property {string | undefined} [outputPrefix] - Optional output prefix override.
 */

const DEFAULT_STACK_NAME = "elephant-permit-harvest";

/**
 * Print CLI usage instructions.
 *
 * @returns {void}
 */
function showUsage() {
  console.log(`
Usage:
  AWS_PROFILE=elephant-oracle-node node scripts/enqueue-sunbiz-corporate-address-match.mjs \
    --job-id sunbiz-lee-YYYYMMDD \
    --corporate-data-s3-uri s3://bucket/path/cordata.zip \
    --address-json ./addresses.json

Options:
  --queue-url <url>                 PermitHarvestQueueUrl. If omitted, read from CloudFormation output.
  --stack <name>                    CloudFormation stack name. Default: ${DEFAULT_STACK_NAME}
  --job-id <id>                     Required stable job id for S3 output prefix partitioning.
  --corporate-data-s3-uri <s3uri>   Required S3 URI for Sunbiz corporate data text or ZIP.
  --source-format <text|zip>        Optional source format. Defaults from object extension.
  --address-json <path>             Required local JSON array of address inputs.
  --output-prefix <s3uri>           Optional S3 output prefix override; worker appends /<job-id>.
  --batch-size <number>             Address inputs per message. Default: 50
  --max-matches-per-address <n>     Per-address match cap. Default: 25
  --dry-run                         Print messages without sending.
  --help                            Show this help.
`);
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
  const corporateDataS3UriValue = values["corporate-data-s3-uri"];
  const addressJsonValue = values["address-json"];
  const sourceFormatValue = values["source-format"];
  const queueUrlValue = values["queue-url"];
  const stackValue = values.stack;
  const outputPrefixValue = values["output-prefix"];

  if (typeof jobIdValue !== "string" || !jobIdValue.trim()) {
    throw new Error("--job-id is required");
  }
  if (
    typeof corporateDataS3UriValue !== "string" ||
    !corporateDataS3UriValue.startsWith("s3://")
  ) {
    throw new Error("--corporate-data-s3-uri must be an s3:// URI");
  }
  if (typeof addressJsonValue !== "string" || !addressJsonValue.trim()) {
    throw new Error("--address-json is required");
  }
  if (
    sourceFormatValue !== undefined &&
    sourceFormatValue !== "text" &&
    sourceFormatValue !== "zip"
  ) {
    throw new Error("--source-format must be text or zip");
  }

  return {
    queueUrl: typeof queueUrlValue === "string" ? queueUrlValue : undefined,
    stackName: typeof stackValue === "string" ? stackValue : DEFAULT_STACK_NAME,
    jobId: jobIdValue.trim(),
    corporateDataS3Uri: corporateDataS3UriValue,
    sourceFormat:
      sourceFormatValue === "text" || sourceFormatValue === "zip"
        ? sourceFormatValue
        : undefined,
    addressJsonPath: addressJsonValue,
    outputPrefix:
      typeof outputPrefixValue === "string" ? outputPrefixValue : undefined,
    batchSize: parsePositiveInteger(
      "--batch-size",
      typeof values["batch-size"] === "string" ? values["batch-size"] : undefined,
      50,
    ),
    maxMatchesPerAddress: parsePositiveInteger(
      "--max-matches-per-address",
      typeof values["max-matches-per-address"] === "string"
        ? values["max-matches-per-address"]
        : undefined,
      25,
    ),
    dryRun: values.dryRun === true,
  };
}

/**
 * Validate parsed JSON address inputs.
 *
 * @param {unknown} value - Parsed JSON value.
 * @returns {AddressInput[]} Valid address inputs.
 */
function validateAddressInputs(value) {
  if (!Array.isArray(value)) {
    throw new Error("address JSON must be an array");
  }
  return value.map((item, index) => {
    if (!item || typeof item !== "object") {
      throw new Error(`address input at index ${index} must be an object`);
    }
    const record = /** @type {Record<string, unknown>} */ (item);
    if (typeof record.rawAddress !== "string" || !record.rawAddress.trim()) {
      throw new Error(`address input at index ${index} requires rawAddress`);
    }
    return {
      inputId: typeof record.inputId === "string" ? record.inputId : undefined,
      source: typeof record.source === "string" ? record.source : undefined,
      rawAddress: record.rawAddress,
      city: typeof record.city === "string" ? record.city : undefined,
      state: typeof record.state === "string" ? record.state : undefined,
      zip: typeof record.zip === "string" ? record.zip : undefined,
    };
  });
}

/**
 * Read address inputs from a local JSON file.
 *
 * @param {string} addressJsonPath - Local JSON path.
 * @returns {Promise<AddressInput[]>} Address inputs.
 */
async function readAddressInputs(addressJsonPath) {
  const absolutePath = path.resolve(addressJsonPath);
  const raw = await readFile(absolutePath, "utf8");
  return validateAddressInputs(JSON.parse(raw));
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
 * Split address inputs into batches.
 *
 * @param {AddressInput[]} inputs - Address inputs.
 * @param {number} batchSize - Maximum batch size.
 * @returns {AddressInput[][]} Address batches.
 */
function chunkAddressInputs(inputs, batchSize) {
  /** @type {AddressInput[][]} */
  const batches = [];
  for (let index = 0; index < inputs.length; index += batchSize) {
    batches.push(inputs.slice(index, index + batchSize));
  }
  return batches;
}

/**
 * Build a worker message for one address batch.
 *
 * @param {CliOptions} options - Parsed CLI options.
 * @param {AddressInput[]} addressInputs - Address batch.
 * @param {number} batchIndex - Zero-based batch index.
 * @returns {SunbizCorporateAddressMatchMessage} Worker message.
 */
function buildMessage(options, addressInputs, batchIndex) {
  return {
    type: "sunbiz-corporate-address-match",
    version: 1,
    jobId: options.jobId,
    addressBatchKey: `batch-${String(batchIndex).padStart(5, "0")}`,
    sourceDataS3Uri: options.corporateDataS3Uri,
    sourceFormat: options.sourceFormat,
    addressInputs,
    maxMatchesPerAddress: options.maxMatchesPerAddress,
    outputPrefix: options.outputPrefix,
  };
}

/**
 * Send all Sunbiz corporate address-match messages to SQS.
 *
 * @returns {Promise<void>} Resolves when messages are sent or printed.
 */
async function main() {
  const options = parseArgs(process.argv.slice(2));
  const addressInputs = await readAddressInputs(options.addressJsonPath);
  const batches = chunkAddressInputs(addressInputs, options.batchSize);
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
      message: "sunbiz_corporate_address_match_enqueue_start",
      jobId: options.jobId,
      queueUrl,
      addressInputs: addressInputs.length,
      batches: batches.length,
      corporateDataS3Uri: options.corporateDataS3Uri,
      dryRun: options.dryRun,
    }),
  );

  const sqs = new SQSClient({});
  for (const [index, batch] of batches.entries()) {
    const message = buildMessage(options, batch, index);
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
        message: "sunbiz_corporate_address_match_batch_enqueued",
        index,
        messageId: response.MessageId,
        addressInputs: batch.length,
      }),
    );
  }

  console.log(
    JSON.stringify({
      level: "info",
      message: "sunbiz_corporate_address_match_enqueue_complete",
      jobId: options.jobId,
      addressInputs: addressInputs.length,
      batches: batches.length,
      dryRun: options.dryRun,
    }),
  );
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ level: "error", message }));
  process.exit(1);
});
