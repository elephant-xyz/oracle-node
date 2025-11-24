import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import {
  SQSClient,
  SendMessageCommand,
  GetQueueUrlCommand,
} from "@aws-sdk/client-sqs";
import { promises as fs } from "fs";
import path from "path";
import os from "os";
import AdmZip from "adm-zip";
import { parse } from "csv-parse/sync";
import { exec } from "child_process";
import {
  queryExecutionWithMostErrors,
  markErrorsAsMaybeSolved,
  normalizeErrors,
  deleteExecution,
} from "./errors.mjs";

const DEFAULT_DLQ_URL = process.env.DEFAULT_DLQ_URL;

/**
 * @typedef {Pick<Console, "log">} ConsoleLike
 * Logger shape that exposes the `log` function matching the global Console API.
 */

const s3Client = new S3Client({});
const dynamoClient = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
  marshallOptions: { removeUndefinedValues: true },
});
const lambdaClient = new LambdaClient({});
const sqsClient = new SQSClient({});

/**
 * Ensure required environment variables are present.
 *
 * @param {string} name - Environment variable identifier.
 * @returns {string} - Resolved environment variable value.
 */
function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

/**
 * Parse an S3 URI into its bucket and key components.
 *
 * @param {string} uri - S3 URI in the form s3://bucket/key.
 * @returns {{ bucket: string, key: string }} - Parsed bucket and key.
 */
function parseS3Uri(uri) {
  const match = /^s3:\/\/([^/]+)\/(.*)$/.exec(uri);
  if (!match) {
    throw new Error(`Bad S3 URI: ${uri}`);
  }
  const bucket = match[1];
  const key = match[2];
  if (typeof bucket !== "string" || typeof key !== "string") {
    throw new Error("S3 URI must be a string");
  }
  return { bucket, key };
}

/**
 * Download an S3 object to a local file.
 *
 * @param {{ bucket: string, key: string }} location - Bucket and key location for the S3 object.
 * @param {string} destinationPath - Absolute path where the object will be stored.
 * @returns {Promise<void>}
 */
async function downloadS3Object({ bucket, key }, destinationPath) {
  console.log(`Downloading s3://${bucket}/${key} to ${destinationPath}`);
  const result = await s3Client.send(
    new GetObjectCommand({ Bucket: bucket, Key: key }),
  );
  const bytes = await result.Body?.transformToByteArray();
  if (!bytes) {
    throw new Error(`Failed to download ${key} from ${bucket}`);
  }
  await fs.writeFile(destinationPath, Buffer.from(bytes));
}

/**
 * Resolve the transform bucket/key pair for a given county.
 *
 * @param {{ countyName: string, transformPrefixUri: string }} params - Transform location configuration.
 * @returns {{ transformBucket: string, transformKey: string }} - Derived transform bucket and key.
 */
function resolveTransformLocation({ countyName, transformPrefixUri }) {
  const { bucket, key } = parseS3Uri(transformPrefixUri);
  const normalizedPrefix = key.replace(/\/$/, "");
  const transformKey = `${normalizedPrefix}/${countyName.toLowerCase()}.zip`;
  return { transformBucket: bucket, transformKey };
}

/**
 * Query DynamoDB for the execution with the most errors.
 *
 * @param {string} tableName - DynamoDB table name.
 * @returns {Promise<import("../../workflow/lambdas/post/errors.mjs").FailedExecutionItem | null>} - The execution with most errors, or null if none found.
 */
async function getExecutionWithMostErrors(tableName) {
  console.log(`Querying DynamoDB for execution with most errors...`);
  return await queryExecutionWithMostErrors({
    tableName,
    documentClient: dynamoClient,
  });
}

/**
 * Download prepared inputs from S3.
 *
 * @param {string} s3Uri - S3 URI pointing to the prepared inputs archive.
 * @param {string} tmpDir - Temporary directory for downloads.
 * @returns {Promise<string>} - Path to downloaded zip file.
 */
async function downloadExecutionInputs(s3Uri, tmpDir) {
  console.log(`Downloading execution inputs from ${s3Uri}...`);
  const { bucket, key } = parseS3Uri(s3Uri);
  const destinationPath = path.join(tmpDir, "prepared_inputs.zip");
  await downloadS3Object({ bucket, key }, destinationPath);
  return destinationPath;
}

/**
 * Download transform scripts for a county.
 *
 * @param {string} county - County identifier.
 * @param {string} transformPrefix - S3 prefix URI for transforms.
 * @param {string} tmpDir - Temporary directory for downloads.
 * @returns {Promise<string>} - Path to downloaded transform scripts zip.
 */
async function downloadTransformScripts(county, transformPrefix, tmpDir) {
  console.log(`Downloading transform scripts for county ${county}...`);
  const { transformBucket, transformKey } = resolveTransformLocation({
    countyName: county,
    transformPrefixUri: transformPrefix,
  });
  const destinationPath = path.join(tmpDir, "transform_scripts.zip");
  await downloadS3Object(
    { bucket: transformBucket, key: transformKey },
    destinationPath,
  );
  return destinationPath;
}

/**
 * Convert CSV file to JSON array
 * @param {string} csvPath - Path to CSV file
 * @returns {Promise<Object[]>} - Array of objects representing CSV rows
 */
async function csvToJson(csvPath) {
  try {
    const csvContent = await fs.readFile(csvPath, "utf8");
    const records = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    });
    return records;
  } catch (error) {
    return [];
  }
}

/**
 * Fetch content from IPFS
 * @param {string} cid - CID of the content to fetch
 * @returns {Promise<object | array | null>} - Content as a buffer
 */
async function fetchFromIpfs(cid) {
  const result = await fetch(`https://ipfs.io/ipfs/${cid}`);
  return await result.json();
}

/**
 * Parse errors from S3 CSV file.
 *
 * @param {string} errorsS3Uri - S3 URI pointing to submit_errors.csv.
 * @returns {Promise<{errorPath: string, dataGroupName: string}>} - Path to the parsed errors CSV file.
 */
async function parseErrorsFromS3(errorsS3Uri) {
  console.log(`Parsing errors from ${errorsS3Uri}...`);
  const { bucket, key } = parseS3Uri(errorsS3Uri);
  const tmpFile = path.join(os.tmpdir(), `errors_${Date.now()}.csv`);
  await downloadS3Object({ bucket, key }, tmpFile);
  const content = await csvToJson(tmpFile);
  const dataGroupCid = content[0]?.data_group_cid;
  const dataGroupContent = await fetchFromIpfs(dataGroupCid);
  return { errorPath: tmpFile, dataGroupName: dataGroupContent.title };
}

/**
 * Extract zip archive to directory.
 *
 * @param {string} zipPath - Path to zip file.
 * @param {string} extractDir - Directory to extract to.
 * @returns {Promise<void>}
 */
async function extractZip(zipPath, extractDir) {
  console.log(`Extracting ${zipPath} to ${extractDir}...`);
  await fs.mkdir(extractDir, { recursive: true });
  const zip = new AdmZip(await fs.readFile(zipPath));
  zip.extractAllTo(extractDir, true);
}

/**
 * Invoke OpenAI Codex to fix errors in scripts.
 *
 * @param {string} errorsPath - Path to CSV file containing errors to fix.
 * @param {string} scriptsDir - Directory containing transform scripts.
 * @param {string} inputsDir - Directory containing prepared inputs.
 * @param {string} dataGroupName - Name of the data group.
 * @returns {Promise<void>}
 */
async function invokeCodexForFix(
  errorsPath,
  scriptsDir,
  inputsDir,
  dataGroupName,
) {
  const scriptsPath = path.join(scriptsDir, "scripts");

  // Read errors to count unique errors
  const errorsArray = await csvToJson(errorsPath);
  const uniqueErrors = new Set();
  for (const error of errorsArray) {
    const errorKey = `${error.file_path || ""}#${error.error_path || ""}#${error.error_message || ""}`;
    if (errorKey !== "##") {
      uniqueErrors.add(errorKey);
    }
  }
  const errorCount = uniqueErrors.size;

  const prompt = `You are fixing transformation script errors in an Elephant Oracle data processing pipeline.

IMPORTANT:
- The scripts must be written back to disk using file operations (fs.writeFileSync or similar)
- Do not return code in your response - write it directly to the files in ${scriptsPath}
- You can use Node.js to run and test scripts: node <script-file>.js
- Test with the sample input data available in: ${inputsDir}

WORKSPACE:
- Transform scripts location: ${scriptsPath}
- Sample input data location: ${inputsDir}
- Data group: ${dataGroupName}
- Errors CSV file: ${errorsPath}

ERROR ANALYSIS REQUIREMENTS:
1. Analyze ALL ${errorCount} unique errors listed in the CSV file
2. For each error, identify which script file generates the problematic output (use file_path to determine this)
3. Fix EVERY unique error path and message listed above
4. Ensure your fixes address all unique error paths and all affected files
5. Do not skip any errors - the validation will fail if even one error remains
6. ONLY modify code sections that are directly related to the errors being fixed - do not touch or refactor any other parts of the scripts that are not causing errors
7. Preserve all existing working code that is not related to the reported errors

ERROR FIELD DESCRIPTIONS:
- file_path: The JSON file that contains the error (e.g., "data/deed_1.json") - use this to identify which script generates the output
- error_path: The JSON path within that file where the error occurs (e.g., "deed_1.json/deed_type") - this is the exact property that has the problem
- error_message: The validation error message - describes what's wrong (e.g., "unexpected property", "missing required property", "invalid type")
- currentValue: The actual value that caused the error (if available) - use this to understand what incorrect value was provided

ADDRESS HANDLING:
- For the address object use unnormalized_address if that is what the source provides
- If source has address normalized, then use it
- Make sure to understand correctly how oneOf works in the JSON schema

CRITICAL: Relationship Directions - VERIFY CORRECT DIRECTION:
- Relationships have a specific direction that MUST match the Elephant MCP schema
- ALWAYS use Elephant MCP to check the relationship class schema to verify the correct direction
- The relationship file name pattern (e.g., "relationship_property_deed_1.json") indicates which relationship class to check
- IMPORTANT: If validation complains about a field that is correctly part of the schema, the relationship direction might be wrong
- Use Elephant MCP to:
  1. Find the relationship class name from the file pattern (e.g., "relationship_property_deed" → check "property_has_deed" or similar)
  2. Query the relationship class schema using Elephant MCP
  3. Verify which entity should be "from" and which should be "to" according to the schema
  4. Ensure the relationship structure matches the schema exactly
- Do NOT assume relationship directions - always verify with Elephant MCP
- If you see errors about unexpected properties in relationships, check if the direction is reversed by consulting Elephant MCP
- If a property is valid in the schema but validation says it's unexpected, swap the "from" and "to" fields and verify with Elephant MCP

CRITICAL: Script Error Handling - NEVER THROW ERRORS:
- Scripts must NEVER throw errors, crash, or use throw statements - they must ALWAYS complete execution successfully
- If a mapping lookup fails (e.g., unknown enum value, missing mapping key), DO NOT throw an error
- Instead, set the property to "MAPPING NOT AVAILABLE" (or "MAPPING NOT FOUND" if preferred)
- Example: Instead of throwing an error for unknown property type, use: property_type: "MAPPING NOT AVAILABLE"
- Example: Instead of throwing when mapping fails, use: spaceType: mapping[code] || "MAPPING NOT AVAILABLE"
- This allows the script to complete, and validation will catch and log the error properly
- Do not use try-catch to suppress errors - instead, handle mapping failures by setting explicit error values
- The goal is to have validation catch errors, not script execution failures
- ALL mapping functions must return a string value (never throw) - use "MAPPING NOT AVAILABLE" as the fallback

CRITICAL: When data is not found or invalid:
- Use Elephant MCP to check the schema for each field to determine if it's required, optional, and whether null is allowed
- For REQUIRED fields that DO NOT allow null (type: "number" or "string" without null): You MUST extract valid data from the source. If data is missing, investigate the source HTML/CSV to find where it should come from. Do not set to null or omit required fields.
- For REQUIRED fields that DO allow null (type: ["number", "null"] or ["string", "null"]): If data is not found, you CAN set it to null as null is a valid value for these fields.
- For OPTIONAL fields (not required, type: ["number", "null"] or ["string", "null"]): ONLY include in the output object if they have valid values. If invalid or missing, OMIT the field entirely (do not set to null).
- For numeric fields: only include if the value is a valid number (typeof value === "number" && Number.isFinite(value))
- For string fields: only include if the value is a non-empty string
- For date fields: only include if the value is a valid date/string
- Use helper functions like assignIfNumber() to conditionally add optional fields only when they have valid values
- DO NOT assume or hardcode values like tax years - each property can have different tax years. Always extract tax_year and other time-based values from the source data.

CRITICAL: Error Resolution Process:
1. IDENTIFY THE PROBLEM:
   - Read the error_path to determine which output file has the error
   - Read the error_message to understand what's wrong
   - Identify which script file generates that output file

2. CONSULT THE SCHEMA:
   - Use Elephant MCP to find the schema for the class corresponding to the output file
   - Determine the class name from the file name pattern
   - Check what properties are allowed, required, and their types/constraints
   - Never invent properties - only use what exists in the Elephant MCP schema
   - Make sure to analyze verified scripts with their examples as well

3. RESOLVE THE ERROR:
   - If a property is unexpected: Remove it or find the correct property name from the schema
   - If a required property is missing: Find the data in the input file (JSON/HTML) and extract it. If not found, check if null is allowed by the schema
   - If a value is invalid: Convert to the correct type or find valid data from the source
   - Modify the script that creates the problematic output
   - To solve invalid URs issues remove its generation from the scripts, it will be populated by the process

4. VERIFY COMPLIANCE:
   - Ensure output strictly matches the Elephant MCP schema
   - Include only properties defined in the schema
   - Include all required properties (or null if schema allows)
   - Exclude any properties not in the schema

ACTION REQUIRED - YOU MUST WRITE THE FIXED CODE:
For EACH error in the list above, follow these steps:

0. CHECK IF ALREADY FIXED:
   - Before fixing an error, check the script to see if it was already modified in a previous update
   - Look for recent changes that might have already addressed this error
   - If the error is about an unexpected property, check if that property was already removed
   - If the error is about a missing required property, check if that property was already added
   - If the error is about relationship direction, check if "from" and "to" were already swapped
   - Only proceed to fix if the error is still present in the current script code

1. IDENTIFY THE SOURCE SCRIPT:
   - Look at the file_path (e.g., "data/deed_1.json") to determine which output file has the error
   - Find the script file that generates this output file by:
     * Searching for the output filename pattern in script files (e.g., "deed_1.json" → search for "deed_" or "writeJson.*deed")
     * Checking which script writes to the "data" directory
     * Looking for writeJson() calls that match the file_path pattern
   - Common patterns:
     * "deed_*.json" → usually in data_extractor.js
     * "file_*.json" → usually in data_extractor.js
     * "sales_*.json" → usually in data_extractor.js
     * "layout_*.json" → usually in layoutMapping.js
     * "structure_*.json" → usually in structureMapping.js
     * "utility_*.json" → usually in utilityMapping.js
     * "person_*.json" → usually in ownerMapping.js or data_extractor.js

2. UNDERSTAND THE ERROR:
   - Read the error_path to see the exact property (e.g., "deed_1.json/deed_type")
   - Read the error_message to understand what's wrong (e.g., "unexpected property", "missing required property")
   - Check currentValue if available to see what incorrect value was provided
   - Verify the error still exists in the current script code (it might have been fixed already)

3. FIX THE SCRIPT:
   - Open the identified script file
   - Locate the code that generates the problematic output file
   - Modify the code to fix the specific error:
     * If unexpected property: Remove it from the output object
     * If missing required property: Extract it from input or set to null if schema allows
     * If invalid value: Convert to correct type or extract valid data
     * If mapping fails: Set to "MAPPING NOT AVAILABLE" instead of throwing
   - Save the modified script file using fs.writeFileSync or similar file operations
   - Return the complete fixed scripts with the same file names`;

  const escapedPrompt = prompt
    .replace(/\\/g, "\\\\")
    .replace(/"/g, '\\"')
    .replace(/`/g, "\\`")
    .replace(/\$/g, "\\$");

  console.log("Invoking codex CLI...");

  return new Promise((resolve, reject) => {
    const child = exec(
      `codex exec "${escapedPrompt}" --sandbox danger-full-access --skip-git-repo-check`,
    );

    child.stdout?.on("data", (data) => {
      process.stdout.write(data);
    });

    child.stderr?.on("data", (data) => {
      process.stderr.write(data);
    });

    child.on("error", (error) => {
      console.error("Codex execution failed:", error);
      reject(new Error(`Codex invocation failed: ${error.message}`));
    });

    child.on("close", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`Codex exited with code ${code}`));
      }
    });
  });
}

/**
 * Recursively find all .js files in a directory, excluding node_modules.
 *
 * @param {string} dir - Directory to search.
 * @param {string} baseDir - Base directory for relative path calculation.
 * @returns {Promise<string[]>} - Array of relative paths to .js files.
 */
async function findJsFiles(dir, baseDir) {
  /** @type {string[]} */
  const jsFiles = [];

  try {
    const entries = await fs.readdir(dir, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      const relativePath = path.relative(baseDir, fullPath);

      // Skip node_modules directories
      if (entry.isDirectory() && entry.name === "node_modules") {
        continue;
      }

      if (entry.isDirectory()) {
        // Recursively search subdirectories
        const subFiles = await findJsFiles(fullPath, baseDir);
        jsFiles.push(...subFiles);
      } else if (entry.isFile() && entry.name.endsWith(".js")) {
        // Add .js files
        jsFiles.push(relativePath);
      }
    }
  } catch (error) {
    console.error(`Error reading directory ${dir}:`, error);
  }

  return jsFiles;
}

/**
 * Upload fixed scripts to S3.
 *
 * @param {string} county - County identifier.
 * @param {string} scriptsDir - Directory containing fixed scripts.
 * @param {string} transformPrefix - S3 prefix URI for transforms.
 * @returns {Promise<void>}
 */
async function uploadFixedScripts(county, scriptsDir, transformPrefix) {
  console.log(`Uploading fixed scripts for county ${county}...`);
  const { transformBucket, transformKey } = resolveTransformLocation({
    countyName: county,
    transformPrefixUri: transformPrefix,
  });

  // Create zip archive
  const zip = new AdmZip();

  // Find all .js files recursively
  const jsFiles = await findJsFiles(scriptsDir, scriptsDir);
  console.log(`Found ${jsFiles.length} .js file(s) to add to zip`);

  // Add each .js file to the zip
  for (const relativePath of jsFiles) {
    const fullPath = path.join(scriptsDir, relativePath);
    const fileContent = await fs.readFile(fullPath);
    // Use relative path as entry name, preserving directory structure
    zip.addFile(relativePath, fileContent);
  }

  const zipBuffer = zip.toBuffer();

  // Upload to S3
  await s3Client.send(
    new PutObjectCommand({
      Bucket: transformBucket,
      Key: transformKey,
      Body: zipBuffer,
      ContentType: "application/zip",
    }),
  );

  console.log(
    `Uploaded fixed scripts to s3://${transformBucket}/${transformKey}`,
  );
}

/**
 * Restore original scripts from backup zip file to S3.
 *
 * @param {string} originalZipPath - Path to the original zip file backup.
 * @param {string} county - County identifier.
 * @param {string} transformPrefix - S3 prefix URI for transforms.
 * @returns {Promise<void>}
 */
async function restoreOriginalScripts(
  originalZipPath,
  county,
  transformPrefix,
) {
  console.log(`Restoring original scripts for county ${county}...`);
  const { transformBucket, transformKey } = resolveTransformLocation({
    countyName: county,
    transformPrefixUri: transformPrefix,
  });

  // Read the original zip file
  const originalZipBuffer = await fs.readFile(originalZipPath);

  // Upload original zip back to S3
  await s3Client.send(
    new PutObjectCommand({
      Bucket: transformBucket,
      Key: transformKey,
      Body: originalZipBuffer,
      ContentType: "application/zip",
    }),
  );

  console.log(
    `Restored original scripts to s3://${transformBucket}/${transformKey}`,
  );
}

/**
 * Construct seed_output_s3_uri from preparedS3Uri.
 * The seed output is stored at the same prefix as the prepared output, but with seed_output.zip filename.
 *
 * @param {string} preparedS3Uri - S3 URI of the prepared output (e.g., s3://bucket/outputs/fileBase/output.zip).
 * @returns {string} - S3 URI of the seed output (e.g., s3://bucket/outputs/fileBase/seed_output.zip).
 */
function constructSeedOutputS3Uri(preparedS3Uri) {
  const { bucket, key } = parseS3Uri(preparedS3Uri);
  // Replace /output.zip with /seed_output.zip
  const seedKey = key.replace(/\/output\.zip$/, "/seed_output.zip");
  return `s3://${bucket}/${seedKey}`;
}

/**
 * Get the county-specific DLQ URL by queue name.
 *
 * @param {string} county - County identifier (will be lowercased).
 * @returns {Promise<string>} - DLQ queue URL.
 */
async function getCountyDlqUrl(county) {
  const queueName = `elephant-workflow-queue-${county.toLowerCase()}-dlq`;
  try {
    const response = await sqsClient.send(
      new GetQueueUrlCommand({ QueueName: queueName }),
    );
    if (!response.QueueUrl) {
      return DEFAULT_DLQ_URL;
    }
    return response.QueueUrl;
  } catch (error) {
    console.log(`DLQ queue ${queueName} not found`);
    return DEFAULT_DLQ_URL;
  }
}

/**
 * Send transaction items to the Transactions SQS queue.
 *
 * @param {string} queueUrl - Transactions SQS queue URL.
 * @param {unknown[]} transactionItems - Array of transaction items to send.
 * @returns {Promise<void>}
 */
async function sendToTransactionsQueue(queueUrl, transactionItems) {
  console.log(
    `Sending ${transactionItems.length} transaction items to Transactions queue`,
  );
  await sqsClient.send(
    new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: JSON.stringify(transactionItems),
    }),
  );
  console.log("Successfully sent transaction items to Transactions queue");
}

/**
 * Send original message to county-specific DLQ.
 *
 * @param {string} dlqUrl - County-specific DLQ URL.
 * @param {Record<string, string | undefined>} source - Source information with s3Bucket and s3Key.
 * @returns {Promise<void>}
 */
async function sendToDlq(dlqUrl, source) {
  if (!source.s3Bucket || !source.s3Key) {
    throw new Error("Cannot send to DLQ: source missing s3Bucket or s3Key");
  }

  const message = {
    s3: {
      bucket: { name: source.s3Bucket },
      object: { key: source.s3Key },
    },
  };

  console.log(`Sending original message to DLQ: ${dlqUrl}`);
  await sqsClient.send(
    new SendMessageCommand({
      QueueUrl: dlqUrl,
      MessageBody: JSON.stringify(message),
    }),
  );
  console.log("Successfully sent message to DLQ");
}

/**
 * Invoke the post-processing Lambda function to verify the fixes work.
 *
 * @param {object} params - Invocation parameters.
 * @param {string} params.functionName - Lambda function name or ARN.
 * @param {string} params.preparedS3Uri - S3 URI of the prepared output.
 * @param {string} params.seedOutputS3Uri - S3 URI of the seed output.
 * @param {{ bucket?: { name?: string }, object?: { key?: string } } | undefined} params.s3Event - Optional S3 event information.
 * @returns {Promise<{ status: string, transactionItems?: unknown[] }>} - Result payload from post-processing Lambda.
 */
async function invokePostProcessingLambda({
  functionName,
  preparedS3Uri,
  seedOutputS3Uri,
  s3Event,
}) {
  console.log(`Invoking post-processing Lambda ${functionName}...`);
  console.log(`Prepared S3 URI: ${preparedS3Uri}`);
  console.log(`Seed output S3 URI: ${seedOutputS3Uri}`);

  const payload = {
    prepare: {
      output_s3_uri: preparedS3Uri,
    },
    seed_output_s3_uri: seedOutputS3Uri,
    prepareSkipped: false,
    saveErrorsOnValidationFailure: false,
  };

  // Add S3 event if available
  if (s3Event?.bucket?.name && s3Event?.object?.key) {
    payload.s3 = {
      bucket: {
        name: s3Event.bucket.name,
      },
      object: {
        key: s3Event.object.key,
      },
    };
  }
  console.log(`Invoking post-processing Lambda ${functionName}...`);
  console.log(JSON.stringify(payload, null, 2));

  const response = await lambdaClient.send(
    new InvokeCommand({
      FunctionName: functionName,
      InvocationType: "RequestResponse",
      Payload: JSON.stringify(payload),
    }),
  );

  if (response.FunctionError) {
    const errorPayload = JSON.parse(
      new TextDecoder().decode(response.Payload ?? new Uint8Array()),
    );
    throw new Error(
      `Post-processing Lambda failed: ${errorPayload.errorMessage || errorPayload.errorType || JSON.stringify(errorPayload)}`,
    );
  }

  const resultPayload = JSON.parse(
    new TextDecoder().decode(response.Payload ?? new Uint8Array()),
  );

  console.log(
    `Post-processing Lambda returned status: ${resultPayload.status} with ${resultPayload.transactionItems?.length || 0} transaction items`,
  );

  return resultPayload;
}

/**
 * Extract S3 URI from error message.
 *
 * @param {string} errorMessage - Error message to parse.
 * @returns {string | null} - Extracted S3 URI or null if not found.
 */
function extractErrorsS3Uri(errorMessage) {
  const match = /Submit errors CSV:\s*(s3:\/\/[^\s]+)/.exec(errorMessage);
  return match ? match[1] : null;
}

async function installCherio(scriptsDir) {
  await new Promise((resolve, reject) => {
    exec("npm install cheerio", { cwd: scriptsDir }, (error) =>
      error
        ? reject(new Error(`cheerio install failed: ${error.message}`))
        : resolve(),
    );
  });
}

/**
 * Run a single auto-repair iteration.
 *
 * @param {object} params - Iteration parameters.
 * @param {string} params.executionId - Execution identifier.
 * @param {string} params.county - County identifier.
 * @param {string} params.preparedS3Uri - S3 URI of the prepared output.
 * @param {string} params.errorsS3Uri - S3 URI of the errors CSV.
 * @param {string} params.transformPrefix - S3 prefix URI for transforms.
 * @param {string} params.tableName - DynamoDB table name.
 * @param {{ s3Bucket?: string, s3Key?: string } | undefined} params.source - Optional source information.
 * @returns {Promise<{ success: boolean, newErrorsS3Uri?: string }>} - Result of the iteration.
 */
async function runAutoRepairIteration({
  executionId,
  county,
  preparedS3Uri,
  errorsS3Uri,
  transformPrefix,
  tableName,
  source,
}) {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "auto-repair-"));

  try {
    // Step 1: Download inputs and transform scripts
    const preparedInputsZip = await downloadExecutionInputs(
      preparedS3Uri,
      tmpDir,
    );
    const transformScriptsZip = await downloadTransformScripts(
      county,
      transformPrefix,
      tmpDir,
    );

    // Extract archives
    const inputsDir = path.join(tmpDir, "inputs");
    const scriptsDir = path.join(tmpDir, "scripts");

    // Install cherio to the scripts directory
    await extractZip(preparedInputsZip, inputsDir);
    await extractZip(transformScriptsZip, scriptsDir);
    await installCherio(scriptsDir);

    // Step 2: Parse errors
    const { errorPath, dataGroupName } = await parseErrorsFromS3(errorsS3Uri);

    // Step 3: Invoke Codex to fix errors
    await invokeCodexForFix(errorPath, scriptsDir, inputsDir, dataGroupName);

    // Step 4: Upload fixed scripts (keep reference to original zip for potential rollback)
    await uploadFixedScripts(county, scriptsDir, transformPrefix);

    // Step 5: Invoke post-processing Lambda to verify fixes work
    const postProcessorFunctionName = requireEnv(
      "POST_PROCESSOR_FUNCTION_NAME",
    );
    const transactionsQueueUrl = requireEnv("TRANSACTIONS_SQS_QUEUE_URL");
    const seedOutputS3Uri = constructSeedOutputS3Uri(preparedS3Uri);

    try {
      const resultPayload = await invokePostProcessingLambda({
        functionName: postProcessorFunctionName,
        preparedS3Uri,
        seedOutputS3Uri,
        s3Event: source
          ? {
              bucket: { name: source.s3Bucket },
              object: { key: source.s3Key },
            }
          : undefined,
      });

      // Check if execution was successful
      if (
        resultPayload.status === "success" &&
        Array.isArray(resultPayload.transactionItems) &&
        resultPayload.transactionItems.length > 0
      ) {
        // Send successful results to Transactions queue
        await sendToTransactionsQueue(
          transactionsQueueUrl,
          resultPayload.transactionItems,
        );

        console.log(
          `Successfully sent ${resultPayload.transactionItems.length} transaction items to Transactions queue`,
        );

        // Step 6: Mark errors as maybeSolved for other executions that share the same errors
        const errorsArray = await csvToJson(errorPath);
        const normalizedErrors = normalizeErrors(errorsArray, county);
        const errorHashes = normalizedErrors.map((e) => e.hash);
        console.log(
          `Marking ${errorHashes.length} error hash(es) as maybeSolved for other executions...`,
        );
        await markErrorsAsMaybeSolved({
          errorHashes,
          tableName,
          documentClient: dynamoClient,
        });

        // Step 7: Delete the execution and all its error links since repair succeeded
        console.log(
          `Deleting execution ${executionId} and all its error links...`,
        );
        await deleteExecution({
          executionId,
          tableName,
          documentClient: dynamoClient,
        });

        return { success: true };
      } else {
        // Execution failed, restore original scripts before sending to DLQ
        console.log(
          `Post-processing failed with status: ${resultPayload.status}. Restoring original scripts...`,
        );
        try {
          await restoreOriginalScripts(
            transformScriptsZip,
            county,
            transformPrefix,
          );
          console.log("Successfully restored original scripts");
        } catch (restoreError) {
          console.error(
            `Failed to restore original scripts: ${restoreError.message}`,
          );
          // Continue with DLQ logic even if restore fails
        }

        // Execution failed, send original message to DLQ
        if (!source) {
          throw new Error(
            `Execution failed but source information is missing, cannot send to DLQ`,
          );
        }

        const dlqUrl = await getCountyDlqUrl(county);
        await sendToDlq(dlqUrl, source);

        console.log(
          `Execution failed, sent original message to DLQ: ${dlqUrl}`,
        );

        throw new Error(
          `Post-processing Lambda returned non-success status: ${resultPayload.status}`,
        );
      }
    } catch (lambdaError) {
      // Post-processing failed, restore original scripts
      console.log(
        `Post-processing Lambda failed. Restoring original scripts...`,
      );
      try {
        await restoreOriginalScripts(
          transformScriptsZip,
          county,
          transformPrefix,
        );
        console.log("Successfully restored original scripts");
      } catch (restoreError) {
        console.error(
          `Failed to restore original scripts: ${restoreError.message}`,
        );
        // Continue with error handling even if restore fails
      }

      // Try to extract new errors S3 URI from error message
      const newErrorsS3Uri = extractErrorsS3Uri(lambdaError.message);

      // If Lambda invocation failed and we can't retry, send to DLQ
      if (!newErrorsS3Uri && source) {
        try {
          const dlqUrl = await getCountyDlqUrl(county);
          await sendToDlq(dlqUrl, source);

          console.error(
            `Post-processing Lambda invocation failed. Sent original message to DLQ: ${dlqUrl}`,
          );
        } catch (dlqError) {
          console.error(`Failed to send to DLQ: ${dlqError.message}`);
        }
      }

      throw new Error(
        `Failed to verify fixes with post-processing Lambda: ${lambdaError.message}`,
      );
    }
  } finally {
    // Cleanup
    await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {
      // Ignore cleanup errors
    });
  }
}

/**
 * Main auto-repair workflow.
 *
 * @returns {Promise<void>}
 */
async function main() {
  try {
    console.log("Starting auto-repair workflow...");

    const tableName = requireEnv("ERRORS_TABLE_NAME");
    const transformPrefix = requireEnv("TRANSFORM_S3_PREFIX");
    const maxRetries = 3;

    // Step 1: Get execution with most errors
    const execution = await getExecutionWithMostErrors(tableName);
    if (!execution) {
      console.log("No failed executions found. Exiting.");
      return;
    }

    console.log(
      `Found execution ${execution.executionId} with ${execution.uniqueErrorCount} unique errors`,
    );
    console.log(`County: ${execution.county}`);
    console.log(`Prepared S3 URI: ${execution.preparedS3Uri}`);
    console.log(`Errors S3 URI: ${execution.errorsS3Uri}`);

    if (!execution.errorsS3Uri) {
      console.log("No errors S3 URI found. Skipping.");
      return;
    }

    if (!execution.preparedS3Uri) {
      console.log("No prepared S3 URI found. Skipping.");
      return;
    }

    let currentErrorsS3Uri = execution.errorsS3Uri;
    let attempt = 0;

    while (attempt < maxRetries) {
      attempt++;
      console.log(`\n=== Auto-repair attempt ${attempt}/${maxRetries} ===`);
      console.log(`Using errors from: ${currentErrorsS3Uri}`);

      try {
        await runAutoRepairIteration({
          executionId: execution.executionId,
          county: execution.county,
          preparedS3Uri: execution.preparedS3Uri,
          errorsS3Uri: currentErrorsS3Uri,
          transformPrefix,
          tableName,
          source: execution.source,
        });

        console.log("Auto-repair workflow completed successfully!");
        return;
      } catch (error) {
        console.error(`Attempt ${attempt} failed:`, error.message);

        // Try to extract new errors S3 URI from error message
        const newErrorsS3Uri = extractErrorsS3Uri(error.message);

        if (newErrorsS3Uri && attempt < maxRetries) {
          console.log(`Found new errors CSV: ${newErrorsS3Uri}`);
          console.log(`Will retry with new errors...`);
          currentErrorsS3Uri = newErrorsS3Uri;
        } else {
          if (attempt >= maxRetries) {
            console.error(`Max retries (${maxRetries}) reached. Giving up.`);
          }
          throw error;
        }
      }
    }
  } catch (error) {
    console.error("Auto-repair workflow failed:", error);
    process.exit(1);
  }
}

// Run main workflow
main();
