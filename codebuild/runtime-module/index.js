import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { promises as fs } from "fs";
import path from "path";
import os from "os";
import AdmZip from "adm-zip";
import { parse } from "csv-parse/sync";
import { exec } from "child_process";
import { promisify } from "util";
import {
  queryExecutionWithMostErrors,
  markErrorsAsMaybeSolved,
} from "./errors.mjs";
import { createHash } from "crypto";

const execAsync = promisify(exec);

/**
 * @typedef {Pick<Console, "log">} ConsoleLike
 * Logger shape that exposes the `log` function matching the global Console API.
 */

const s3Client = new S3Client({});
const dynamoClient = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
  marshallOptions: { removeUndefinedValues: true },
});
const lambdaClient = new LambdaClient({});

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
 * Compute the deterministic error hash from message, path, and county.
 *
 * @param {string} message - Error message.
 * @param {string} path - Error path.
 * @param {string} county - County identifier.
 * @returns {string} - SHA256 hash string.
 */
function createErrorHash(message, path, county) {
  return createHash("sha256")
    .update(`${message}#${path}#${county}`, "utf8")
    .digest("hex");
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
 * Parse errors from S3 CSV file.
 *
 * @param {string} errorsS3Uri - S3 URI pointing to submit_errors.csv.
 * @returns {Promise<Array<{ errorMessage: string, errorPath: string, hash: string }>>} - Parsed errors with computed hashes.
 */
async function parseErrorsFromS3(errorsS3Uri, county) {
  console.log(`Parsing errors from ${errorsS3Uri}...`);
  const { bucket, key } = parseS3Uri(errorsS3Uri);
  const tmpFile = path.join(os.tmpdir(), `errors_${Date.now()}.csv`);
  await downloadS3Object({ bucket, key }, tmpFile);

  const csvContent = await fs.readFile(tmpFile, "utf8");
  const records = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });

  const errors = [];
  for (const record of records) {
    const errorMessage =
      typeof record.errorMessage === "string"
        ? record.errorMessage.trim()
        : "";
    const errorPath =
      typeof record.errorPath === "string" ? record.errorPath.trim() : "unknown";

    if (errorMessage.length > 0) {
      const hash = createErrorHash(errorMessage, errorPath, county);
      errors.push({ errorMessage, errorPath, hash });
    }
  }

  await fs.unlink(tmpFile).catch(() => {
    // Ignore cleanup errors
  });

  return errors;
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
 * @param {Array<{ errorMessage: string, errorPath: string, hash: string }>} errors - Array of errors to fix.
 * @param {string} scriptsDir - Directory containing transform scripts.
 * @param {string} inputsDir - Directory containing prepared inputs.
 * @returns {Promise<void>}
 */
async function invokeCodexForFix(errors, scriptsDir, inputsDir) {
  console.log(`Invoking OpenAI Codex to fix ${errors.length} error(s)...`);

  const scriptsPath = path.join(scriptsDir, "scripts");

  // Build prompt with file locations instead of content
  const errorsText = errors
    .map(
      (e) => `- Error: ${e.errorMessage}\n  Path: ${e.errorPath}\n  Hash: ${e.hash}`,
    )
    .join("\n");

  const prompt = `You are fixing transformation script errors in an Elephant Oracle data processing pipeline.

The following errors occurred during execution:
${errorsText}

The transform scripts are located at: ${scriptsPath}
Sample input data is available at: ${inputsDir}

Please analyze the errors and provide fixed versions of the scripts. Focus on fixing the error paths and messages mentioned above. Consider the input data structure when making fixes. Return the complete fixed scripts with the same file names.`;

  // Write prompt to file
  const promptFile = path.join(scriptsDir, "codex_prompt.txt");
  await fs.writeFile(promptFile, prompt, "utf8");

  // Invoke codex CLI
  console.log("Invoking codex CLI...");
  try {
    const { stdout, stderr } = await execAsync(
      `codex --prompt-file "${promptFile}" --output-dir "${scriptsPath}"`,
    );
    console.log("Codex stdout:", stdout);
    if (stderr) {
      console.error("Codex stderr:", stderr);
    }
  } catch (error) {
    console.error("Codex execution failed:", error);
    throw new Error(`Codex invocation failed: ${error.message}`);
  }
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
  zip.addLocalFolder(scriptsDir);
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

  console.log(`Uploaded fixed scripts to s3://${transformBucket}/${transformKey}`);
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
 * Invoke the post-processing Lambda function to verify the fixes work.
 *
 * @param {object} params - Invocation parameters.
 * @param {string} params.functionName - Lambda function name or ARN.
 * @param {string} params.preparedS3Uri - S3 URI of the prepared output.
 * @param {string} params.seedOutputS3Uri - S3 URI of the seed output.
 * @param {{ bucket?: { name?: string }, object?: { key?: string } } | undefined} params.s3Event - Optional S3 event information.
 * @returns {Promise<void>}
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

  try {
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

    if (resultPayload.status !== "success") {
      throw new Error(
        `Post-processing Lambda returned non-success status: ${resultPayload.status}`,
      );
    }

    console.log(
      `Post-processing Lambda succeeded with ${resultPayload.transactionItems?.length || 0} transaction items`,
    );
  } catch (error) {
    console.error("Post-processing Lambda invocation failed:", error);
    throw error;
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
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "auto-repair-"));

    try {
      // Step 1: Get execution with most errors
      const execution = await getExecutionWithMostErrors(tableName);
      if (!execution) {
        console.log("No failed executions found. Exiting.");
        return;
      }

      console.log(`Found execution ${execution.executionId} with ${execution.uniqueErrorCount} unique errors`);
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

      // Step 2: Download inputs and transform scripts
      const preparedInputsZip = await downloadExecutionInputs(
        execution.preparedS3Uri,
        tmpDir,
      );
      const transformScriptsZip = await downloadTransformScripts(
        execution.county,
        transformPrefix,
        tmpDir,
      );

      // Extract archives
      const inputsDir = path.join(tmpDir, "inputs");
      const scriptsDir = path.join(tmpDir, "scripts");
      await extractZip(preparedInputsZip, inputsDir);
      await extractZip(transformScriptsZip, scriptsDir);

      // Step 3: Parse errors
      const errors = await parseErrorsFromS3(
        execution.errorsS3Uri,
        execution.county,
      );
      console.log(`Parsed ${errors.length} unique errors`);

      if (errors.length === 0) {
        console.log("No errors to fix. Exiting.");
        return;
      }

      // Step 4: Invoke Codex to fix errors
      await invokeCodexForFix(errors, scriptsDir, inputsDir);

      // Step 5: Upload fixed scripts
      await uploadFixedScripts(execution.county, scriptsDir, transformPrefix);

      // Step 6: Invoke post-processing Lambda to verify fixes work
      const postProcessorFunctionName = requireEnv("POST_PROCESSOR_FUNCTION_NAME");
      const seedOutputS3Uri = constructSeedOutputS3Uri(execution.preparedS3Uri);

      try {
        await invokePostProcessingLambda({
          functionName: postProcessorFunctionName,
          preparedS3Uri: execution.preparedS3Uri,
          seedOutputS3Uri,
          s3Event: execution.source
            ? {
                bucket: { name: execution.source.s3Bucket },
                object: { key: execution.source.s3Key },
              }
            : undefined,
        });

        // Step 7: Mark errors as maybeSolved only if Lambda invocation succeeded
        const errorHashes = errors.map((e) => e.hash);
        console.log(`Marking ${errorHashes.length} error hash(es) as maybeSolved...`);
        await markErrorsAsMaybeSolved({
          errorHashes,
          tableName,
          documentClient: dynamoClient,
        });

        console.log("Auto-repair workflow completed successfully!");
      } catch (lambdaError) {
        console.error(
          "Post-processing Lambda invocation failed. Errors will not be marked as maybeSolved.",
          lambdaError,
        );
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
  } catch (error) {
    console.error("Auto-repair workflow failed:", error);
    process.exit(1);
  }
}

// Run main workflow
main();