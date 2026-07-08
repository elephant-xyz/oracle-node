#!/usr/bin/env node

import {
  CloudFormationClient,
  DescribeStacksCommand,
} from "@aws-sdk/client-cloudformation";
import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { parse } from "csv-parse";
import { readFile } from "fs/promises";
import { Pool } from "pg";

/**
 * Miami Dade county configuration. This file is a parameterized clone of
 * scripts/enqueue-lee-appraisal-property-first-from-seed.mjs; only the values
 * below differ from the Lee run.
 *
 * The prepare state machine derives the per-county prepare queue name from the
 * seed CSV's `county` column (lowercased, spaces -> '-', '.' removed), NOT from
 * this object. With a seed `county` value of "Miami Dade" the state machine
 * targets `elephant-oracle-node-prepare-queue-miami-dade`, which already exists.
 */
const COUNTY = {
  /** Human-readable county name used in log lines. */
  name: "Miami Dade",
  /** Snake-case county key used in log message identifiers. */
  key: "miami_dade",
  /** Neon properties.source_system value for the Miami Dade appraiser source. */
  sourceSystem: "miami_dade_appraiser",
  /** Postgres application_name used for the dedup connection. */
  applicationName: "miami-dade-appraisal-seed-enqueue",
  /** S3 object metadata `source` tag written on each generated seed slice. */
  seedSource: "counties-seeds-miami-dade",
  /** Slug used in generated S3 prefixes and the default job id. */
  slug: "miami-dade-property-first-seed",
};

/**
 * Load a newline-delimited list of 1-based source data-row numbers into a Set.
 * Blank lines and lines starting with '#' are ignored. Used to enqueue ONLY a
 * specific subset of seed rows (e.g. parcels whose prepare failed) while keeping
 * the original sourceRowNumber so outputs land in the existing row-* folders.
 *
 * @param {string} filePath - Path to the row-number list file.
 * @returns {Promise<Set<number>>} Set of 1-based source data-row numbers.
 */
async function readOnlyRows(filePath) {
  const content = await readFile(filePath, "utf8");
  const rows = new Set();
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (line.length === 0 || line.startsWith("#")) continue;
    const parsed = Number.parseInt(line, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      throw new Error(`--only-rows-file contains invalid row number: ${line}`);
    }
    rows.add(parsed);
  }
  if (rows.size === 0) {
    throw new Error(`--only-rows-file ${filePath} contained no row numbers`);
  }
  return rows;
}

/**
 * @typedef {object} CliOptions
 * @property {string} sourceCsvS3Uri - Source Palm Beach county seed CSV S3 URI.
 * @property {string} stackName - Oracle-node CloudFormation stack name.
 * @property {string} permitStackName - Permit-harvest CloudFormation stack name.
 * @property {string | undefined} workflowQueueUrl - Explicit workflow starter queue URL.
 * @property {string | undefined} propertyFirstPermitQueueUrl - Explicit property-first permit queue URL.
 * @property {string | undefined} generatedSeedPrefix - S3 prefix for generated one-property CSV seed files.
 * @property {string | undefined} workflowOutputBaseUri - S3 base URI for appraisal workflow outputs.
 * @property {string | undefined} propertyFirstPermitOutputPrefix - S3 prefix consumed by the permit worker; the worker appends jobId.
 * @property {string} jobId - Stable job identifier used for workflow messages and S3 partitioning.
 * @property {number} limit - Maximum source rows to enqueue; 0 means all remaining source rows.
 * @property {number} offset - Number of valid source rows to skip before enqueueing.
 * @property {number} maxPages - Maximum Accela parcel-search pages per property.
 * @property {number} sendDelayMs - Optional delay between SQS sends.
 * @property {number} concurrency - Number of seed uploads/workflow SQS sends to run concurrently.
 * @property {string} envFile - Local env file used when skipping properties already in Neon.
 * @property {boolean} skipExistingNeon - Skip rows whose Palm Beach Appraiser property is already present in Neon.
 * @property {boolean} dryRun - Print candidate workflow messages without uploading seeds or sending SQS messages.
 * @property {string | undefined} onlyRowsFile - Optional path to a newline-delimited list of 1-based source data-row numbers; only those rows are enqueued. Preserves the original sourceRowNumber so re-prepared outputs land in the existing row-* folders.
 */

/**
 * @typedef {object} ResolvedOptions
 * @property {CliOptions} cli - Parsed CLI options.
 * @property {string} workflowQueueUrl - Workflow starter queue URL.
 * @property {string} propertyFirstPermitQueueUrl - Dedicated property-first permit queue URL.
 * @property {string} generatedSeedPrefix - S3 prefix for generated one-property CSV seed files.
 * @property {string} workflowOutputBaseUri - S3 base URI for appraisal workflow outputs.
 * @property {string} propertyFirstPermitOutputPrefix - S3 prefix consumed by the permit worker.
 */

/**
 * @typedef {object} S3UriParts
 * @property {string} bucket - S3 bucket name.
 * @property {string} key - S3 object key or prefix.
 */

/**
 * @typedef {Record<string, string | undefined>} SeedRow
 */

/**
 * @typedef {object} ExistingNeonIdentifiers
 * @property {Set<string>} requestIdentifiers - Existing Palm Beach Appraiser request/Folio identifiers.
 * @property {Set<string>} normalizedParcelIdentifiers - Existing normalized Palm Beach Appraiser parcel identifiers.
 */

/**
 * @typedef {object} PendingUploadState
 * @property {Set<Promise<void>>} pendingUploads - In-flight seed upload and workflow enqueue tasks.
 * @property {Error | undefined} firstError - First asynchronous task failure seen by the scheduler.
 */

const DEFAULT_STACK_NAME = "elephant-oracle-node";
const DEFAULT_PERMIT_STACK_NAME = "elephant-permit-harvest";
const DEFAULT_SOURCE_CSV_S3_URI = "s3://counties-seeds/miami-dade.csv";
const DEFAULT_ENV_FILE = "../elephant-query-db/.env.local";
const DEFAULT_JOB_ID = `${COUNTY.slug}-${new Date().toISOString().slice(0, 10).replace(/-/g, "")}`;

const cloudFormation = new CloudFormationClient({});
const s3 = new S3Client({});
const sqs = new SQSClient({});

/**
 * Print CLI usage instructions.
 *
 * @returns {void} Writes usage text to stdout.
 */
function showUsage() {
  console.log(`
Usage:
  AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \
    node scripts/enqueue-palm-beach-appraisal-property-first-from-seed.mjs \
      --source-csv-s3-uri s3://counties-seeds/palm_beach.csv \
      --job-id ${COUNTY.slug}-YYYYMMDD \
      --limit 0 --send-delay-ms 50

Options:
  --source-csv-s3-uri <s3uri>       Full Palm Beach seed CSV. Default: ${DEFAULT_SOURCE_CSV_S3_URI}
  --stack <name>                    Oracle-node stack. Default: ${DEFAULT_STACK_NAME}
  --permit-stack <name>             Permit-harvest stack. Default: ${DEFAULT_PERMIT_STACK_NAME}
  --workflow-queue-url <url>        WorkflowQueueUrl override.
  --property-first-permit-queue-url <url>
                                    PropertyFirstPermitQueueUrl override.
  --generated-seed-prefix <s3uri>   Generated one-row CSV seed prefix. Default: environment bucket / seed-inputs / job id.
  --workflow-output-base-uri <s3uri>
                                    Appraisal workflow output base. Default: environment bucket / outputs / ${COUNTY.slug} / job id.
  --property-first-permit-output-prefix <s3uri>
                                    Permit artifact prefix. Default: environment bucket / permit-harvest / ${COUNTY.slug}.
  --job-id <id>                     Stable run id. Default: ${DEFAULT_JOB_ID}
  --limit <number>                  Source rows to enqueue after offset; 0 means all. Default: 1000
  --offset <number>                 Valid source rows to skip. Default: 0
  --max-pages <number>              Max Accela pages per parcel. Default: 200
  --send-delay-ms <number>          Delay between SQS sends. Default: 0
  --concurrency <number>            Concurrent S3 seed uploads/SQS sends. Default: 1
  --env-file <path>                 Env file with DATABASE_URL. Default: ${DEFAULT_ENV_FILE}
  --include-existing-neon           Do not skip properties already present in Neon.
  --only-rows-file <path>           Newline-delimited 1-based source data-row numbers; enqueue ONLY those rows. Preserves the original sourceRowNumber so re-prepared outputs land in the existing row-* folders. Skip count for non-listed rows is reported as skippedNotInOnlyRows.
  --dry-run                         Print messages without uploading generated seeds or sending SQS.
  --help                            Show this help.

This script uses the S3 seed CSV as the source of truth. It writes one seed CSV
per property so the existing appraisal workflow can extract one full property,
then the state machine enqueues the property-first permit worker to fetch all
Accela permits and load appraisal+permits to Neon in one transaction.
`);
}

/**
 * Return a trimmed string when a value is a non-empty string.
 *
 * @param {unknown} value - Candidate value read from CLI args, CSV, or env.
 * @returns {string | undefined} Trimmed string when usable.
 */
function readOptionalString(value) {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

/**
 * Parse a non-negative integer CLI option.
 *
 * @param {string} optionName - CLI option name used in diagnostics.
 * @param {string | undefined} rawValue - Raw CLI option value.
 * @param {number} fallback - Default value when the option is absent.
 * @returns {number} Parsed non-negative integer.
 */
function parseNonNegativeInteger(optionName, rawValue, fallback) {
  if (rawValue === undefined) return fallback;
  const parsed = Number.parseInt(rawValue, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(
      `${optionName} must be a non-negative integer, received ${rawValue}`,
    );
  }
  return parsed;
}

/**
 * Parse a positive integer CLI option.
 *
 * @param {string} optionName - CLI option name used in diagnostics.
 * @param {string | undefined} rawValue - Raw CLI option value.
 * @param {number} fallback - Default value when the option is absent.
 * @returns {number} Parsed positive integer.
 */
function parsePositiveInteger(optionName, rawValue, fallback) {
  if (rawValue === undefined) return fallback;
  const parsed = Number.parseInt(rawValue, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(
      `${optionName} must be a positive integer, received ${rawValue}`,
    );
  }
  return parsed;
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
    if (token === "--include-existing-neon") {
      values.includeExistingNeon = true;
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

  return {
    sourceCsvS3Uri:
      readOptionalString(values["source-csv-s3-uri"]) ??
      DEFAULT_SOURCE_CSV_S3_URI,
    stackName: readOptionalString(values.stack) ?? DEFAULT_STACK_NAME,
    permitStackName:
      readOptionalString(values["permit-stack"]) ?? DEFAULT_PERMIT_STACK_NAME,
    workflowQueueUrl: readOptionalString(values["workflow-queue-url"]),
    propertyFirstPermitQueueUrl: readOptionalString(
      values["property-first-permit-queue-url"],
    ),
    generatedSeedPrefix: readOptionalString(values["generated-seed-prefix"]),
    workflowOutputBaseUri: readOptionalString(
      values["workflow-output-base-uri"],
    ),
    propertyFirstPermitOutputPrefix: readOptionalString(
      values["property-first-permit-output-prefix"],
    ),
    jobId: readOptionalString(values["job-id"]) ?? DEFAULT_JOB_ID,
    limit: parseNonNegativeInteger(
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
    concurrency: parsePositiveInteger(
      "--concurrency",
      readOptionalString(values.concurrency),
      1,
    ),
    envFile: readOptionalString(values["env-file"]) ?? DEFAULT_ENV_FILE,
    skipExistingNeon: values.includeExistingNeon !== true,
    onlyRowsFile: readOptionalString(values["only-rows-file"]),
    dryRun: values.dryRun === true,
  };
}

/**
 * Parse an S3 URI into bucket and key components.
 *
 * @param {string} uri - S3 URI in `s3://bucket/key` format.
 * @returns {S3UriParts} Parsed S3 bucket and key.
 */
function parseS3Uri(uri) {
  const match = /^s3:\/\/([^/]+)\/(.*)$/i.exec(uri);
  if (!match) throw new Error(`Invalid S3 URI: ${uri}`);
  return { bucket: match[1], key: match[2].replace(/\/$/, "") };
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
 * Build all S3 prefixes and queue URLs after reading stack outputs.
 *
 * @param {CliOptions} cli - Parsed CLI options.
 * @returns {Promise<ResolvedOptions>} Fully resolved options.
 */
async function resolveOptions(cli) {
  const environmentBucketName = await getStackOutput(
    cli.stackName,
    "EnvironmentBucketName",
  );
  const workflowQueueUrl =
    cli.workflowQueueUrl ??
    (await getStackOutput(cli.stackName, "WorkflowQueueUrl"));
  const propertyFirstPermitQueueUrl =
    cli.propertyFirstPermitQueueUrl ??
    (await getStackOutput(cli.permitStackName, "PropertyFirstPermitQueueUrl"));
  return {
    cli,
    workflowQueueUrl,
    propertyFirstPermitQueueUrl,
    generatedSeedPrefix:
      cli.generatedSeedPrefix ??
      `s3://${environmentBucketName}/seed-inputs/${COUNTY.slug}/${cli.jobId}`,
    workflowOutputBaseUri:
      cli.workflowOutputBaseUri ??
      `s3://${environmentBucketName}/outputs/${COUNTY.slug}/${cli.jobId}`,
    propertyFirstPermitOutputPrefix:
      cli.propertyFirstPermitOutputPrefix ??
      `s3://${environmentBucketName}/permit-harvest/${COUNTY.slug}`,
  };
}

/**
 * Load simple KEY=VALUE lines from an env file without adding a dotenv dependency.
 *
 * Existing process.env values win, which lets callers override DATABASE_URL from
 * the shell or CI secret injection.
 *
 * @param {string} envFile - Path to an env file.
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
 * Normalize a Palm Beach parcel identifier (PCN) for duplicate checks and S3 key
 * names.
 *
 * Palm Beach PCNs are 17-digit numeric strings that may arrive with separators
 * (dots/dashes) or leading/trailing whitespace. Only non-digit characters are
 * stripped, so distinct PCNs can never be collapsed together; separator and
 * whitespace differences normalize away while every digit is preserved.
 *
 * @param {unknown} value - Raw parcel identifier.
 * @returns {string | undefined} Digits-only parcel identifier.
 */
function normalizeParcelIdentifier(value) {
  const normalized = String(value ?? "").replace(/[^0-9]/g, "");
  return normalized.length > 0 ? normalized : undefined;
}

/**
 * Make a string safe and compact enough for a deterministic S3 object key part.
 *
 * @param {unknown} value - Source value.
 * @param {string} fallback - Fallback when source value is blank.
 * @returns {string} S3-safe key part.
 */
function safeKeyPart(value, fallback) {
  const source = readOptionalString(value) ?? fallback;
  const safe = source
    .replace(/[^A-Za-z0-9._=-]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return safe.length > 0 ? safe.slice(0, 96) : fallback;
}

/**
 * Escape one CSV cell according to RFC4180-style quoting rules.
 *
 * @param {string | undefined} value - Raw cell value.
 * @returns {string} CSV-encoded cell.
 */
function encodeCsvCell(value) {
  const text = value ?? "";
  if (!/[",\r\n]/.test(text)) return text;
  return `"${text.replace(/"/g, '""')}"`;
}

/**
 * Build a one-row CSV body preserving the source header order exactly.
 *
 * @param {string[]} columns - Source CSV column names in order.
 * @param {SeedRow} row - Parsed source seed row.
 * @returns {string} One-row CSV body with header.
 */
function buildOneRowCsv(columns, row) {
  return `${columns.map(encodeCsvCell).join(",")}\n${columns.map((column) => encodeCsvCell(row[column])).join(",")}\n`;
}

/**
 * Normalize a CSV parser header row into string column names.
 *
 * @param {unknown[]} header - Raw header values supplied by csv-parse.
 * @returns {string[]} Header values converted to strings.
 */
function normalizeCsvHeader(header) {
  return header.map((column) => String(column));
}

/**
 * Read existing Palm Beach Appraiser identifiers from Neon so the S3 seed file
 * can be used to queue only new properties instead of replaying already-loaded
 * rows.
 *
 * @param {CliOptions} cli - Parsed CLI options containing env-file location.
 * @returns {Promise<ExistingNeonIdentifiers>} Existing request and parcel identifiers.
 */
async function readExistingNeonIdentifiers(cli) {
  if (!cli.skipExistingNeon) {
    return {
      requestIdentifiers: new Set(),
      normalizedParcelIdentifiers: new Set(),
    };
  }
  await loadEnvFile(cli.envFile);
  const databaseUrl = readOptionalString(process.env.DATABASE_URL);
  if (databaseUrl === undefined) {
    throw new Error(
      `DATABASE_URL is required to skip existing Neon properties; set it or pass --include-existing-neon`,
    );
  }
  const pool = new Pool({
    connectionString: databaseUrl,
    application_name: COUNTY.applicationName,
    connectionTimeoutMillis: 15_000,
    max: 1,
    query_timeout: 180_000,
    statement_timeout: 180_000,
  });
  try {
    const result = await pool.query(
      `
        select request_identifier, parcel_identifier
        from properties
        where source_system = $1
      `,
      [COUNTY.sourceSystem],
    );
    const requestIdentifiers = new Set();
    const normalizedParcelIdentifiers = new Set();
    for (const row of result.rows) {
      const requestIdentifier = readOptionalString(row.request_identifier);
      if (requestIdentifier !== undefined)
        requestIdentifiers.add(requestIdentifier);
      const parcelIdentifier = normalizeParcelIdentifier(row.parcel_identifier);
      if (parcelIdentifier !== undefined)
        normalizedParcelIdentifiers.add(parcelIdentifier);
    }
    return { requestIdentifiers, normalizedParcelIdentifiers };
  } finally {
    await pool.end();
  }
}

/**
 * Return whether a source seed row is already present in Neon.
 *
 * Miami-Dade dedup is BY FOLIO (`request_identifier`) only. The normalized
 * `parcel_identifier` is NOT a cardinality key — distinct folios can share or
 * collide on it — so a parcel-only fallback would silently skip valid folios in
 * a catch-up run. See `docs/miami-dade-county-findings.md` and the folio warning
 * in the `query-db-loading-matching` skill.
 *
 * @param {SeedRow} row - Parsed source seed row.
 * @param {ExistingNeonIdentifiers} existing - Existing Miami-Dade Appraiser identifiers from Neon.
 * @returns {boolean} True when the row should be skipped as already loaded.
 */
function isExistingNeonRow(row, existing) {
  const requestIdentifier = readOptionalString(row.source_identifier);
  return (
    requestIdentifier !== undefined &&
    existing.requestIdentifiers.has(requestIdentifier)
  );
}

/**
 * Return whether a value behaves like a Node readable stream.
 *
 * @param {unknown} value - Candidate stream value from AWS SDK S3 response body.
 * @returns {value is NodeJS.ReadableStream} True when `pipe` is available.
 */
function isNodeReadableStream(value) {
  return (
    typeof value === "object" &&
    value !== null &&
    "pipe" in value &&
    typeof (/** @type {{ pipe?: unknown }} */ (value).pipe) === "function"
  );
}

/**
 * Upload one generated seed CSV and enqueue its workflow starter message.
 *
 * @param {object} params - Upload/enqueue parameters.
 * @param {ResolvedOptions} params.options - Resolved queue URLs and prefixes.
 * @param {string[]} params.columns - Source CSV columns in header order.
 * @param {SeedRow} params.row - Source seed row.
 * @param {number} params.sourceRowNumber - One-based source CSV data row number.
 * @param {number} params.enqueuedIndex - Zero-based index among messages selected by this run.
 * @returns {Promise<void>} Resolves after the seed is uploaded and the workflow message is sent.
 */
async function uploadSeedAndEnqueueWorkflow({
  options,
  columns,
  row,
  sourceRowNumber,
  enqueuedIndex,
}) {
  const seedPrefix = parseS3Uri(options.generatedSeedPrefix);
  const parcelIdentifier = normalizeParcelIdentifier(row.parcel_id);
  if (parcelIdentifier === undefined) {
    throw new Error(
      `Source row ${String(sourceRowNumber)} is missing parcel_id`,
    );
  }
  const requestIdentifier = safeKeyPart(row.source_identifier, "unknown-folio");
  const seedKey = `${seedPrefix.key}/row-${String(sourceRowNumber).padStart(9, "0")}-folio-${requestIdentifier}-parcel-${safeKeyPart(parcelIdentifier, "unknown-parcel")}.csv`;
  const workflowMessage = {
    s3: {
      bucket: { name: seedPrefix.bucket },
      object: { key: seedKey },
    },
    output_base_uri: options.workflowOutputBaseUri,
    propertyFirstPermitQueueUrl: options.propertyFirstPermitQueueUrl,
    propertyFirstPermitJobId: options.cli.jobId,
    propertyFirstPermitOutputPrefix: options.propertyFirstPermitOutputPrefix,
    propertyFirstPermitMaxPages: options.cli.maxPages,
    sourceSeedS3Uri: options.cli.sourceCsvS3Uri,
    sourceSeedRowNumber: sourceRowNumber,
  };

  if (options.cli.dryRun) {
    console.log(
      JSON.stringify(
        {
          enqueuedIndex,
          sourceRowNumber,
          county: {
            county_name: COUNTY.name,
            county_key: COUNTY.key,
            source: COUNTY.seedSource,
            parcel_identifier: parcelIdentifier,
            request_identifier: readOptionalString(row.source_identifier),
          },
          seedUri: `s3://${seedPrefix.bucket}/${seedKey}`,
          workflowMessage,
        },
        null,
        2,
      ),
    );
    return;
  }

  await s3.send(
    new PutObjectCommand({
      Bucket: seedPrefix.bucket,
      Key: seedKey,
      Body: Buffer.from(buildOneRowCsv(columns, row), "utf8"),
      ContentType: "text/csv; charset=utf-8",
      Metadata: {
        source: COUNTY.seedSource,
        jobId: options.cli.jobId,
        sourceRowNumber: String(sourceRowNumber),
      },
    }),
  );
  const response = await sqs.send(
    new SendMessageCommand({
      QueueUrl: options.workflowQueueUrl,
      MessageBody: JSON.stringify(workflowMessage),
    }),
  );
  console.log(
    JSON.stringify({
      level: "info",
      message: `${COUNTY.key}_appraisal_property_first_workflow_enqueued`,
      enqueuedIndex,
      sourceRowNumber,
      messageId: response.MessageId,
      parcelIdentifier,
      requestIdentifier: readOptionalString(row.source_identifier),
      seedUri: `s3://${seedPrefix.bucket}/${seedKey}`,
    }),
  );
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
 * Normalize an unknown thrown value into an Error instance.
 *
 * @param {unknown} error - Value thrown by an asynchronous upload/enqueue task.
 * @returns {Error} Error instance suitable for later rethrowing.
 */
function toError(error) {
  return error instanceof Error ? error : new Error(String(error));
}

/**
 * Schedule a seed upload/workflow enqueue task and record its eventual failure
 * without producing an unhandled rejection while other concurrent tasks finish.
 *
 * @param {PendingUploadState} state - Mutable pending task scheduler state.
 * @param {Promise<void>} uploadPromise - Upload/enqueue task promise to track.
 * @returns {void}
 */
function schedulePendingUpload(state, uploadPromise) {
  /** @type {Promise<void>} */
  let trackedPromise;
  trackedPromise = uploadPromise
    .catch((error) => {
      if (state.firstError === undefined) state.firstError = toError(error);
    })
    .finally(() => {
      state.pendingUploads.delete(trackedPromise);
    });
  state.pendingUploads.add(trackedPromise);
}

/**
 * Wait for at least one in-flight upload/enqueue task to complete and surface
 * any failure captured by the concurrent scheduler.
 *
 * @param {PendingUploadState} state - Mutable pending task scheduler state.
 * @returns {Promise<void>} Resolves after one pending task completes successfully.
 */
async function waitForNextPendingUpload(state) {
  if (state.pendingUploads.size === 0) return;
  await Promise.race(state.pendingUploads);
  if (state.firstError !== undefined) throw state.firstError;
}

/**
 * Wait for all scheduled upload/enqueue tasks to settle and surface the first
 * task failure if one occurred.
 *
 * @param {PendingUploadState} state - Mutable pending task scheduler state.
 * @returns {Promise<void>} Resolves after all pending tasks settle successfully.
 */
async function waitForAllPendingUploads(state) {
  while (state.pendingUploads.size > 0) {
    await waitForNextPendingUpload(state);
  }
}

/**
 * Stream the source seed CSV, generate one-property seeds, and enqueue workflow messages.
 *
 * @param {ResolvedOptions} options - Fully resolved options.
 * @param {ExistingNeonIdentifiers} existing - Existing Palm Beach Appraiser identifiers used for duplicate skips.
 * @param {Set<number> | undefined} onlyRows - When set, only these 1-based source data-row numbers are enqueued.
 * @returns {Promise<void>} Resolves when the requested rows have been processed.
 */
async function enqueueFromSeed(options, existing, onlyRows) {
  const source = parseS3Uri(options.cli.sourceCsvS3Uri);
  const response = await s3.send(
    new GetObjectCommand({ Bucket: source.bucket, Key: source.key }),
  );
  if (!isNodeReadableStream(response.Body)) {
    throw new Error(
      `S3 object body is not a Node readable stream: ${options.cli.sourceCsvS3Uri}`,
    );
  }

  /** @type {string[]} */
  let columns = [];
  const parser = response.Body.pipe(
    parse({
      columns: (header) => {
        columns = normalizeCsvHeader(header);
        return columns;
      },
      relax_column_count: true,
      skip_empty_lines: true,
      trim: false,
    }),
  );

  let sourceRowNumber = 0;
  let validRowNumber = 0;
  let skippedExisting = 0;
  let skippedInvalid = 0;
  let skippedNotInOnlyRows = 0;
  let enqueued = 0;
  /** @type {PendingUploadState} */
  const pendingUploadState = {
    pendingUploads: new Set(),
    firstError: undefined,
  };

  for await (const parsedRow of parser) {
    if (pendingUploadState.firstError !== undefined)
      throw pendingUploadState.firstError;
    sourceRowNumber += 1;
    if (onlyRows !== undefined && !onlyRows.has(sourceRowNumber)) {
      skippedNotInOnlyRows += 1;
      continue;
    }
    const row = /** @type {SeedRow} */ (parsedRow);
    const parcelIdentifier = normalizeParcelIdentifier(row.parcel_id);
    const requestIdentifier = readOptionalString(row.source_identifier);
    if (parcelIdentifier === undefined || requestIdentifier === undefined) {
      skippedInvalid += 1;
      continue;
    }
    validRowNumber += 1;
    if (validRowNumber <= options.cli.offset) continue;
    if (isExistingNeonRow(row, existing)) {
      skippedExisting += 1;
      continue;
    }
    if (options.cli.limit > 0 && enqueued >= options.cli.limit) break;
    schedulePendingUpload(
      pendingUploadState,
      uploadSeedAndEnqueueWorkflow({
        options,
        columns,
        row,
        sourceRowNumber,
        enqueuedIndex: enqueued,
      }),
    );
    enqueued += 1;
    if (pendingUploadState.pendingUploads.size >= options.cli.concurrency) {
      await waitForNextPendingUpload(pendingUploadState);
    }
    if (options.cli.sendDelayMs > 0) await sleep(options.cli.sendDelayMs);
  }
  await waitForAllPendingUploads(pendingUploadState);

  console.log(
    JSON.stringify({
      level: "info",
      message: `${COUNTY.key}_appraisal_property_first_seed_enqueue_complete`,
      sourceCsvS3Uri: options.cli.sourceCsvS3Uri,
      jobId: options.cli.jobId,
      sourceRowsRead: sourceRowNumber,
      validRowsSeen: validRowNumber,
      skippedInvalid,
      skippedExisting,
      skippedNotInOnlyRows,
      enqueued,
      limit: options.cli.limit,
      offset: options.cli.offset,
      concurrency: options.cli.concurrency,
      dryRun: options.cli.dryRun,
      generatedSeedPrefix: options.generatedSeedPrefix,
      workflowOutputBaseUri: options.workflowOutputBaseUri,
      propertyFirstPermitOutputPrefix: options.propertyFirstPermitOutputPrefix,
    }),
  );
}

/**
 * CLI entry point.
 *
 * @returns {Promise<void>} Resolves when the enqueue run completes.
 */
async function main() {
  const cli = parseArgs(process.argv.slice(2));
  const [options, existing, onlyRows] = await Promise.all([
    resolveOptions(cli),
    readExistingNeonIdentifiers(cli),
    cli.onlyRowsFile !== undefined
      ? readOnlyRows(cli.onlyRowsFile)
      : Promise.resolve(undefined),
  ]);
  console.log(
    JSON.stringify({
      level: "info",
      message: `${COUNTY.key}_appraisal_property_first_seed_enqueue_start`,
      countyName: COUNTY.name,
      countyKey: COUNTY.key,
      sourceCsvS3Uri: cli.sourceCsvS3Uri,
      jobId: cli.jobId,
      workflowQueueUrl: options.workflowQueueUrl,
      propertyFirstPermitQueueUrl: options.propertyFirstPermitQueueUrl,
      skipExistingNeon: cli.skipExistingNeon,
      existingRequestIdentifierCount: existing.requestIdentifiers.size,
      existingParcelIdentifierCount: existing.normalizedParcelIdentifiers.size,
      onlyRowsFile: cli.onlyRowsFile,
      onlyRowsCount: onlyRows?.size,
      limit: cli.limit,
      offset: cli.offset,
      concurrency: cli.concurrency,
      dryRun: cli.dryRun,
    }),
  );
  await enqueueFromSeed(options, existing, onlyRows);
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ level: "error", message }));
  process.exit(1);
});
