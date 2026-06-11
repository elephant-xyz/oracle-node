#!/usr/bin/env node

import {
  CloudFormationClient,
  DescribeStacksCommand,
} from "@aws-sdk/client-cloudformation";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { readFile } from "fs/promises";
import { Pool } from "pg";

/**
 * @typedef {object} CliOptions
 * @property {string | undefined} queueUrl - Explicit permit-harvest SQS queue URL.
 * @property {string} stackName - CloudFormation stack name used to discover the queue when queueUrl is omitted.
 * @property {string} envFile - Local env file that supplies DATABASE_URL for selecting appraiser properties from Neon.
 * @property {string} jobId - Stable property-first harvest job identifier.
 * @property {string | undefined} outputPrefix - Optional S3 output prefix override consumed by the worker.
 * @property {number} limit - Maximum number of property messages to enqueue in this run.
 * @property {number} offset - Number of candidate rows to skip before enqueueing.
 * @property {number} maxPages - Maximum Accela result pages per parcel search.
 * @property {number} sendDelayMs - Optional delay between SQS sends, useful when manually seeding a large backlog.
 * @property {boolean} includeLinkedPermits - Include appraiser properties that already have linked Lee Accela permits.
 * @property {boolean} onlyIndustrial - Enqueue only industrial/light-manufacturing appraiser properties.
 * @property {boolean} dryRun - Print messages without sending them.
 */

/**
 * @typedef {object} DbPropertyRow
 * @property {string} property_id - Query-db property UUID.
 * @property {string | null} request_identifier - Lee Appraiser request/Folio identifier.
 * @property {string | null} parcel_identifier - Lee Appraiser parcel/STRAP identifier.
 * @property {string | null} source_artifact_uri - Transformed appraiser artifact URI.
 * @property {string | null} property_usage_type - Appraiser usage type from source payload.
 * @property {string | null} property_type - Query-facing property type.
 * @property {string | null} unnormalized_address - Situs address from the related address row.
 * @property {string | null} normalized_address_key - Normalized situs address key from the related address row.
 * @property {number} linked_permit_count - Existing Lee Accela permits already linked to this property.
 */

/**
 * @typedef {object} LeePropertyFirstPermitParcelMessage
 * @property {"lee-property-first-permit-parcel"} type - Worker message discriminator.
 * @property {1} version - Message version.
 * @property {string} jobId - Stable harvest job identifier.
 * @property {string} parcelIdentifier - Lee Appraiser parcel/STRAP identifier to search in Accela.
 * @property {string | undefined} [requestIdentifier] - Lee Appraiser request/Folio identifier.
 * @property {string | undefined} [appraisalOutputS3Uri] - Transformed appraiser artifact URI.
 * @property {string} propertyId - Query-db property UUID, included for logging and state artifacts.
 * @property {string | undefined} [propertyUsageType] - Appraiser usage type.
 * @property {string | undefined} [propertyType] - Query-facing property type.
 * @property {string | undefined} [bestPermitAddress] - Best known appraiser situs address.
 * @property {string | undefined} [addressBase] - Normalized appraiser address base.
 * @property {string | undefined} [outputPrefix] - Optional S3 output prefix override.
 * @property {number} maxPages - Maximum Accela result pages to capture for this parcel.
 * @property {boolean} skipExisting - Reuse existing permit JSON artifacts when present.
 * @property {boolean} skipCompleted - Reuse existing property-first completion state when present.
 * @property {boolean} loadToNeon - Merge captured permit details into Neon and link them immediately.
 */

const DEFAULT_STACK_NAME = "elephant-permit-harvest";
const DEFAULT_ENV_FILE = "../elephant-query-db/.env.local";
const INDUSTRIAL_USAGE_TYPES = ["Industrial", "LightManufacturing"];

/**
 * Print CLI usage instructions.
 *
 * @returns {void}
 */
function showUsage() {
  console.log(`
Usage:
  AWS_PROFILE=elephant-oracle-node node scripts/enqueue-lee-property-first-permits.mjs \
    --job-id lee-property-first-all-YYYYMMDD --limit 1000

Options:
  --queue-url <url>              PermitHarvestQueueUrl. If omitted, read from CloudFormation output.
  --stack <name>                 CloudFormation stack name. Default: ${DEFAULT_STACK_NAME}
  --env-file <path>              Env file containing DATABASE_URL. Default: ${DEFAULT_ENV_FILE}
  --job-id <id>                  Required stable job id for S3 output prefix partitioning.
  --output-prefix <s3uri>        Optional S3 output prefix override; worker appends /<job-id>.
  --limit <number>               Max property messages to enqueue. Default: 1000
  --offset <number>              Candidate row offset. Default: 0
  --max-pages <number>           Max Accela result pages per parcel. Default: 200
  --send-delay-ms <number>       Optional delay between SQS sends. Default: 0
  --include-linked-permits       Include properties that already have linked Lee Accela permits.
  --only-industrial              Enqueue only Industrial and LightManufacturing properties.
  --dry-run                      Print messages without sending.
  --help                         Show this help.

The default query covers all Lee Appraiser rows already loaded in Neon and sorts
industrial/light-manufacturing properties first. Re-run this script periodically
or in small pages; worker-side S3 completion state makes duplicate messages safe.
`);
}

/**
 * Parse a positive integer CLI option.
 *
 * @param {string} name - Option name for diagnostics.
 * @param {string | undefined} value - Raw option value.
 * @param {number} fallback - Default value when raw option is absent.
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
 * Parse a non-negative integer CLI option.
 *
 * @param {string} name - Option name for diagnostics.
 * @param {string | undefined} value - Raw option value.
 * @param {number} fallback - Default value when raw option is absent.
 * @returns {number} Parsed non-negative integer.
 */
function parseNonNegativeInteger(name, value, fallback) {
  if (value === undefined) return fallback;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer, received ${value}`);
  }
  return parsed;
}

/**
 * Return a trimmed string, or undefined when absent or blank.
 *
 * @param {unknown} value - Candidate string value.
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
    if (token === "--include-linked-permits") {
      values.includeLinkedPermits = true;
      continue;
    }
    if (token === "--only-industrial") {
      values.onlyIndustrial = true;
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

  const jobId = readOptionalString(values["job-id"]);
  if (jobId === undefined) {
    throw new Error("--job-id is required");
  }

  return {
    queueUrl: readOptionalString(values["queue-url"]),
    stackName: readOptionalString(values.stack) ?? DEFAULT_STACK_NAME,
    envFile: readOptionalString(values["env-file"]) ?? DEFAULT_ENV_FILE,
    jobId,
    outputPrefix: readOptionalString(values["output-prefix"]),
    limit: parsePositiveInteger(
      "--limit",
      readOptionalString(values.limit),
      1000,
    ),
    offset: parseNonNegativeInteger(
      "--offset",
      readOptionalString(values.offset),
      0,
    ),
    maxPages: parsePositiveInteger(
      "--max-pages",
      readOptionalString(values["max-pages"]),
      200,
    ),
    sendDelayMs: parseNonNegativeInteger(
      "--send-delay-ms",
      readOptionalString(values["send-delay-ms"]),
      0,
    ),
    includeLinkedPermits: values.includeLinkedPermits === true,
    onlyIndustrial: values.onlyIndustrial === true,
    dryRun: values.dryRun === true,
  };
}

/**
 * Load simple KEY=VALUE lines from an env file without adding a runtime dotenv dependency.
 *
 * Existing process.env values win, so callers can override DATABASE_URL from the shell.
 *
 * @param {string} envFile - Path to a local env file.
 * @returns {Promise<void>} Resolves after environment variables are populated.
 */
async function loadEnvFile(envFile) {
  const content = await readFile(envFile, "utf8");
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (line.length === 0 || line.startsWith("#")) continue;
    const equalsIndex = line.indexOf("=");
    if (equalsIndex <= 0) continue;
    const key = line.slice(0, equalsIndex).trim();
    const rawValue = line.slice(equalsIndex + 1).trim();
    if (process.env[key] !== undefined) continue;
    process.env[key] = rawValue.replace(/^['"]|['"]$/g, "");
  }
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
 * Resolve the queue URL, preferring the dedicated property-first queue while
 * retaining compatibility with older stacks that expose only PermitHarvestQueueUrl.
 *
 * @param {CliOptions} options - Parsed CLI options.
 * @returns {Promise<string>} SQS queue URL.
 */
async function resolveQueueUrl(options) {
  if (options.queueUrl !== undefined) return options.queueUrl;
  const cloudFormation = new CloudFormationClient({});
  try {
    return await getStackOutput(
      cloudFormation,
      options.stackName,
      "PropertyFirstPermitQueueUrl",
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(
      JSON.stringify({
        level: "warn",
        message: "property_first_queue_output_missing_using_legacy_queue",
        detail: message,
      }),
    );
    return getStackOutput(
      cloudFormation,
      options.stackName,
      "PermitHarvestQueueUrl",
    );
  }
}

/**
 * Normalize a Lee County parcel identifier for Accela parcel searches.
 *
 * @param {unknown} value - Raw parcel identifier.
 * @returns {string | null} Uppercase alphanumeric parcel identifier, or null when unusable.
 */
function normalizeParcelIdentifier(value) {
  const normalized = String(value ?? "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "");
  return normalized.length > 0 ? normalized : null;
}

/**
 * Collapse a normalized address key into the short address-base provenance field.
 *
 * @param {string | null} normalizedAddressKey - Query-db normalized address key.
 * @param {string | null} unnormalizedAddress - Original situs address.
 * @returns {string | undefined} Address base when available.
 */
function buildAddressBase(normalizedAddressKey, unnormalizedAddress) {
  const source = readOptionalString(normalizedAddressKey) ?? readOptionalString(unnormalizedAddress);
  if (source === undefined) return undefined;
  return source
    .replace(/\b(UNIT|STE|SUITE|APT|#)\b.*$/i, "")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

/**
 * Read property rows already loaded from the Lee Appraiser into Neon.
 *
 * The result covers all appraiser properties by default, ordered with industrial
 * and light-manufacturing properties first so the 24/7 worker starts with the
 * user's highest-value cohort while still progressing through the full county.
 *
 * @param {Pool} pool - Query-db Postgres pool.
 * @param {CliOptions} options - Selection options.
 * @returns {Promise<DbPropertyRow[]>} Candidate appraiser properties.
 */
async function readCandidateProperties(pool, options) {
  const result = await pool.query(
    `
      with linked_counts as (
        select property_id, count(*)::int as linked_permit_count
        from property_improvements
        where source_system = 'lee_accela'
          and property_id is not null
        group by property_id
      )
      select
        property.property_id::text,
        property.request_identifier,
        property.parcel_identifier,
        property.source_artifact_uri,
        property.source_payload->>'property_usage_type' as property_usage_type,
        property.property_type,
        address.unnormalized_address,
        address.normalized_address_key,
        coalesce(linked.linked_permit_count, 0)::int as linked_permit_count
      from properties property
      left join addresses address on address.address_id = property.address_id
      left join linked_counts linked on linked.property_id = property.property_id
      where property.source_system = 'lee_appraiser'
        and property.parcel_identifier is not null
        and regexp_replace(property.parcel_identifier, '[^A-Za-z0-9]', '', 'g') <> ''
        and property.source_artifact_uri is not null
        and ($1::boolean or coalesce(linked.linked_permit_count, 0) = 0)
        and ($2::boolean = false or coalesce(property.source_payload->>'property_usage_type', '') = any($3::text[]))
      order by
        case when coalesce(property.source_payload->>'property_usage_type', '') = any($3::text[]) then 0 else 1 end,
        property.updated_at desc nulls last,
        property.request_identifier nulls last
      limit $4::int offset $5::int
    `,
    [
      options.includeLinkedPermits,
      options.onlyIndustrial,
      INDUSTRIAL_USAGE_TYPES,
      options.limit,
      options.offset,
    ],
  );
  return result.rows.map((row) => ({
    property_id: String(row.property_id),
    request_identifier: readOptionalString(row.request_identifier) ?? null,
    parcel_identifier: readOptionalString(row.parcel_identifier) ?? null,
    source_artifact_uri: readOptionalString(row.source_artifact_uri) ?? null,
    property_usage_type: readOptionalString(row.property_usage_type) ?? null,
    property_type: readOptionalString(row.property_type) ?? null,
    unnormalized_address: readOptionalString(row.unnormalized_address) ?? null,
    normalized_address_key: readOptionalString(row.normalized_address_key) ?? null,
    linked_permit_count:
      typeof row.linked_permit_count === "number" ? row.linked_permit_count : 0,
  }));
}

/**
 * Convert one Neon appraiser row into a property-first worker message.
 *
 * @param {CliOptions} options - Enqueue options.
 * @param {DbPropertyRow} row - Appraiser property row.
 * @returns {LeePropertyFirstPermitParcelMessage | null} Message, or null when the row cannot be searched safely.
 */
function buildMessage(options, row) {
  const parcelIdentifier = normalizeParcelIdentifier(row.parcel_identifier);
  if (parcelIdentifier === null) return null;
  const bestPermitAddress = readOptionalString(row.unnormalized_address);
  return {
    type: "lee-property-first-permit-parcel",
    version: 1,
    jobId: options.jobId,
    parcelIdentifier,
    requestIdentifier: readOptionalString(row.request_identifier),
    appraisalOutputS3Uri: readOptionalString(row.source_artifact_uri),
    propertyId: row.property_id,
    propertyUsageType: readOptionalString(row.property_usage_type),
    propertyType: readOptionalString(row.property_type),
    bestPermitAddress,
    addressBase: buildAddressBase(row.normalized_address_key, row.unnormalized_address),
    outputPrefix: options.outputPrefix,
    maxPages: options.maxPages,
    skipExisting: true,
    skipCompleted: true,
    loadToNeon: true,
  };
}

/**
 * Wait for a fixed amount of time.
 *
 * @param {number} milliseconds - Delay duration.
 * @returns {Promise<void>} Resolves after the delay.
 */
function sleep(milliseconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

/**
 * Enqueue property-first permit harvest messages for appraiser properties already in Neon.
 *
 * @returns {Promise<void>} Resolves when selected messages are sent or printed.
 */
async function main() {
  const options = parseArgs(process.argv.slice(2));
  await loadEnvFile(options.envFile);
  const databaseUrl = readOptionalString(process.env.DATABASE_URL);
  if (databaseUrl === undefined) {
    throw new Error(`DATABASE_URL is required; expected it in ${options.envFile} or the environment`);
  }

  const queueUrl = await resolveQueueUrl(options);

  const pool = new Pool({
    connectionString: databaseUrl,
    application_name: "lee-property-first-permit-enqueue",
    connectionTimeoutMillis: 15_000,
    max: 2,
    query_timeout: 180_000,
    statement_timeout: 180_000,
  });

  try {
    const candidates = await readCandidateProperties(pool, options);
    const messages = candidates
      .map((row) => buildMessage(options, row))
      .filter((message) => message !== null);

    console.log(
      JSON.stringify({
        level: "info",
        message: "lee_property_first_permit_enqueue_start",
        jobId: options.jobId,
        queueUrl,
        selectedRows: candidates.length,
        messageCount: messages.length,
        limit: options.limit,
        offset: options.offset,
        onlyIndustrial: options.onlyIndustrial,
        includeLinkedPermits: options.includeLinkedPermits,
        dryRun: options.dryRun,
      }),
    );

    const sqs = new SQSClient({});
    for (const [index, message] of messages.entries()) {
      if (options.dryRun) {
        console.log(JSON.stringify({ index, message }, null, 2));
      } else {
        const response = await sqs.send(
          new SendMessageCommand({
            QueueUrl: queueUrl,
            MessageBody: JSON.stringify(message),
          }),
        );
        console.log(
          JSON.stringify({
            level: "info",
            message: "lee_property_first_permit_parcel_enqueued",
            index,
            messageId: response.MessageId,
            parcelIdentifier: message.parcelIdentifier,
            propertyId: message.propertyId,
            propertyUsageType: message.propertyUsageType,
          }),
        );
      }
      if (options.sendDelayMs > 0) await sleep(options.sendDelayMs);
    }

    console.log(
      JSON.stringify({
        level: "info",
        message: "lee_property_first_permit_enqueue_complete",
        jobId: options.jobId,
        messageCount: messages.length,
        dryRun: options.dryRun,
      }),
    );
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ level: "error", message }));
  process.exit(1);
});
