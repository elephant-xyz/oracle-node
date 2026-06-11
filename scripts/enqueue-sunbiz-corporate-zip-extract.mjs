#!/usr/bin/env node

import {
  CloudFormationClient,
  DescribeStacksCommand,
} from "@aws-sdk/client-cloudformation";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { readFile } from "fs/promises";

/**
 * @typedef {object} CliOptions
 * @property {string | undefined} queueUrl - Explicit permit-harvest SQS queue URL.
 * @property {string} stackName - CloudFormation stack name used to discover the queue when queueUrl is omitted.
 * @property {string} jobId - Stable harvest job identifier.
 * @property {string} extractKey - Stable ZIP extraction identifier used in output S3 keys.
 * @property {string} sourceDataS3Uri - S3 URI of Sunbiz corporate fixed-width text or ZIP object.
 * @property {"text" | "zip" | undefined} sourceFormat - Optional source object format.
 * @property {string[]} zipPrefixes - ZIP prefixes to extract.
 * @property {string | undefined} outputPrefix - Optional S3 output prefix override consumed by the worker.
 * @property {number} chunkRecordLimit - Maximum records per output JSONL chunk.
 * @property {number | undefined} maxRecords - Optional matched-record cap for smoke runs.
 * @property {boolean} dryRun - Print the message without sending.
 */

/**
 * @typedef {object} SunbizCorporateZipExtractMessage
 * @property {"sunbiz-corporate-zip-extract"} type - Worker message discriminator.
 * @property {1} version - Message contract version.
 * @property {string} jobId - Stable harvest job identifier.
 * @property {string} extractKey - Stable extraction key for S3 output partitioning.
 * @property {string} sourceDataS3Uri - S3 URI for Sunbiz source data.
 * @property {"text" | "zip" | undefined} [sourceFormat] - Optional source object format.
 * @property {string[]} zipPrefixes - ZIP prefixes matched against principal, mailing, registered-agent, and officer addresses.
 * @property {number} chunkRecordLimit - Maximum records per output JSONL chunk.
 * @property {number | undefined} [maxRecords] - Optional matched-record cap for smoke runs.
 * @property {string | undefined} [outputPrefix] - Optional S3 output prefix override.
 */

const DEFAULT_STACK_NAME = "elephant-permit-harvest";
const LEE_COUNTY_ZIP_PREFIXES = [
  "33901",
  "33902",
  "33903",
  "33904",
  "33905",
  "33906",
  "33907",
  "33908",
  "33909",
  "33910",
  "33911",
  "33912",
  "33913",
  "33914",
  "33915",
  "33916",
  "33917",
  "33918",
  "33919",
  "33920",
  "33921",
  "33922",
  "33924",
  "33928",
  "33929",
  "33931",
  "33932",
  "33936",
  "33945",
  "33956",
  "33957",
  "33965",
  "33966",
  "33967",
  "33970",
  "33971",
  "33972",
  "33973",
  "33974",
  "33976",
  "33990",
  "33991",
  "33993",
  "34133",
  "34134",
  "34135",
  "34136",
];

/**
 * Print CLI usage instructions.
 *
 * @returns {void}
 */
function showUsage() {
  console.log(`
Usage:
  AWS_PROFILE=elephant-oracle-node node scripts/enqueue-sunbiz-corporate-zip-extract.mjs \
    --job-id sunbiz-lee-YYYYMMDD \
    --extract-key lee-county-zips \
    --source-data-s3-uri s3://bucket/path/cordata.zip \
    --source-format zip \
    --lee-county-zips

Options:
  --queue-url <url>              PermitHarvestQueueUrl. If omitted, read from CloudFormation output.
  --stack <name>                 CloudFormation stack name. Default: ${DEFAULT_STACK_NAME}
  --job-id <id>                  Required stable job id for S3 output prefix partitioning.
  --extract-key <id>             Required stable extraction key used under sunbiz/corporate-by-zip/.
  --source-data-s3-uri <s3uri>   Required S3 URI for Sunbiz corporate data text or ZIP.
  --source-format <text|zip>     Optional source format. Defaults from object extension in the worker.
  --zip-prefix <zip>             ZIP prefix to include. Repeatable; comma-separated values are also accepted.
  --zip-prefixes-json <path>     Local JSON array of ZIP prefixes.
  --lee-county-zips              Include the built-in Lee County ZIP prefix list.
  --output-prefix <s3uri>        Optional S3 output prefix override; worker appends /<job-id>.
  --chunk-record-limit <number>  Records per JSONL output chunk. Default: 1000
  --max-records <number>         Optional matched-record cap for smoke runs.
  --dry-run                      Print the message without sending.
  --help                         Show this help.
`);
}

/**
 * Parse a positive integer CLI option.
 *
 * @param {string} name - Option name for error messages.
 * @param {string | undefined} value - Raw option value.
 * @param {number | undefined} fallback - Default value when raw option is undefined.
 * @returns {number | undefined} Parsed positive integer, or undefined when no value and no fallback exist.
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
 * Normalize ZIP prefixes by removing non-digits and deduplicating.
 *
 * @param {string[]} values - Raw ZIP prefix values.
 * @returns {string[]} Normalized ZIP prefixes.
 */
function normalizeZipPrefixes(values) {
  return [
    ...new Set(
      values
        .map((value) => value.replace(/\D+/g, "").slice(0, 5))
        .filter(Boolean),
    ),
  ];
}

/**
 * Read ZIP prefixes from a local JSON array file.
 *
 * @param {string} jsonPath - Local JSON file path.
 * @returns {Promise<string[]>} ZIP prefixes.
 */
async function readZipPrefixesJson(jsonPath) {
  const raw = await readFile(jsonPath, "utf8");
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error("--zip-prefixes-json must point to a JSON array");
  }
  return parsed.map((value, index) => {
    if (typeof value !== "string") {
      throw new Error(`zip prefix at index ${index} must be a string`);
    }
    return value;
  });
}

/**
 * Parse command-line arguments into strongly-typed options.
 *
 * @param {string[]} argv - Raw argv excluding node and script path.
 * @returns {Promise<CliOptions>} Parsed options.
 */
async function parseArgs(argv) {
  /** @type {Record<string, string | boolean | string[]>} */
  const values = {};
  /** @type {string[]} */
  const zipPrefixValues = [];

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
    if (token === "--lee-county-zips") {
      zipPrefixValues.push(...LEE_COUNTY_ZIP_PREFIXES);
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
    if (key === "zip-prefix") {
      zipPrefixValues.push(...next.split(","));
    } else {
      values[key] = next;
    }
    index += 1;
  }

  const zipPrefixesJsonValue = values["zip-prefixes-json"];
  if (typeof zipPrefixesJsonValue === "string") {
    zipPrefixValues.push(...(await readZipPrefixesJson(zipPrefixesJsonValue)));
  }

  const jobIdValue = values["job-id"];
  const extractKeyValue = values["extract-key"];
  const sourceDataS3UriValue = values["source-data-s3-uri"];
  const sourceFormatValue = values["source-format"];
  const queueUrlValue = values["queue-url"];
  const stackValue = values.stack;
  const outputPrefixValue = values["output-prefix"];
  const zipPrefixes = normalizeZipPrefixes(zipPrefixValues);

  if (typeof jobIdValue !== "string" || !jobIdValue.trim()) {
    throw new Error("--job-id is required");
  }
  if (typeof extractKeyValue !== "string" || !extractKeyValue.trim()) {
    throw new Error("--extract-key is required");
  }
  if (
    typeof sourceDataS3UriValue !== "string" ||
    !sourceDataS3UriValue.startsWith("s3://")
  ) {
    throw new Error("--source-data-s3-uri must be an s3:// URI");
  }
  if (
    sourceFormatValue !== undefined &&
    sourceFormatValue !== "text" &&
    sourceFormatValue !== "zip"
  ) {
    throw new Error("--source-format must be text or zip");
  }
  if (zipPrefixes.length === 0) {
    throw new Error(
      "At least one ZIP prefix is required via --zip-prefix, --zip-prefixes-json, or --lee-county-zips",
    );
  }

  return {
    queueUrl: typeof queueUrlValue === "string" ? queueUrlValue : undefined,
    stackName: typeof stackValue === "string" ? stackValue : DEFAULT_STACK_NAME,
    jobId: jobIdValue.trim(),
    extractKey: extractKeyValue.trim(),
    sourceDataS3Uri: sourceDataS3UriValue,
    sourceFormat:
      sourceFormatValue === "text" || sourceFormatValue === "zip"
        ? sourceFormatValue
        : undefined,
    zipPrefixes,
    outputPrefix:
      typeof outputPrefixValue === "string" ? outputPrefixValue : undefined,
    chunkRecordLimit:
      parsePositiveInteger(
        "--chunk-record-limit",
        typeof values["chunk-record-limit"] === "string"
          ? values["chunk-record-limit"]
          : undefined,
        1000,
      ) ?? 1000,
    maxRecords: parsePositiveInteger(
      "--max-records",
      typeof values["max-records"] === "string" ? values["max-records"] : undefined,
      undefined,
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
 * Build the worker message for one ZIP extraction run.
 *
 * @param {CliOptions} options - Parsed CLI options.
 * @returns {SunbizCorporateZipExtractMessage} Worker message.
 */
function buildMessage(options) {
  return {
    type: "sunbiz-corporate-zip-extract",
    version: 1,
    jobId: options.jobId,
    extractKey: options.extractKey,
    sourceDataS3Uri: options.sourceDataS3Uri,
    sourceFormat: options.sourceFormat,
    zipPrefixes: options.zipPrefixes,
    chunkRecordLimit: options.chunkRecordLimit,
    maxRecords: options.maxRecords,
    outputPrefix: options.outputPrefix,
  };
}

/**
 * Send the Sunbiz corporate ZIP extraction message to SQS.
 *
 * @returns {Promise<void>} Resolves when the message is sent or printed.
 */
async function main() {
  const options = await parseArgs(process.argv.slice(2));
  const queueUrl = options.queueUrl
    ? options.queueUrl
    : await getStackOutput(
        new CloudFormationClient({}),
        options.stackName,
        "PermitHarvestQueueUrl",
      );
  const message = buildMessage(options);

  console.log(
    JSON.stringify({
      level: "info",
      message: "sunbiz_corporate_zip_extract_enqueue_start",
      jobId: options.jobId,
      extractKey: options.extractKey,
      queueUrl,
      zipPrefixes: options.zipPrefixes.length,
      sourceDataS3Uri: options.sourceDataS3Uri,
      dryRun: options.dryRun,
    }),
  );

  if (options.dryRun) {
    console.log(JSON.stringify({ message }, null, 2));
  } else {
    const response = await new SQSClient({}).send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(message),
      }),
    );
    console.log(
      JSON.stringify({
        level: "info",
        message: "sunbiz_corporate_zip_extract_enqueued",
        messageId: response.MessageId,
      }),
    );
  }

  console.log(
    JSON.stringify({
      level: "info",
      message: "sunbiz_corporate_zip_extract_enqueue_complete",
      jobId: options.jobId,
      extractKey: options.extractKey,
      dryRun: options.dryRun,
    }),
  );
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ level: "error", message }));
  process.exit(1);
});
