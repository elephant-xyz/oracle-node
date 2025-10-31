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
import { exec } from "child_process";
import { promisify } from "util";
import {
  queryExecutionWithMostErrors,
  markErrorsAsMaybeSolved,
} from "./errors.mjs";

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
 * Parse errors from S3 CSV file.
 *
 * @param {string} errorsS3Uri - S3 URI pointing to submit_errors.csv.
 * @returns {Promise<string>} - Path to the parsed errors CSV file.
 */
async function parseErrorsFromS3(errorsS3Uri) {
  console.log(`Parsing errors from ${errorsS3Uri}...`);
  const { bucket, key } = parseS3Uri(errorsS3Uri);
  const tmpFile = path.join(os.tmpdir(), `errors_${Date.now()}.csv`);
  await downloadS3Object({ bucket, key }, tmpFile);
  return tmpFile;
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
 * @param {string} errorsPath - Array of errors to fix.
 * @param {string} scriptsDir - Directory containing transform scripts.
 * @param {string} inputsDir - Directory containing prepared inputs.
 * @returns {Promise<void>}
 */
async function invokeCodexForFix(errorsPath, scriptsDir, inputsDir) {
  const scriptsPath = path.join(scriptsDir, "scripts");

  const prompt = `You are fixing transformation script errors in an Elephant Oracle data processing pipeline.
        You can find an errors in the following CSV file: ${errorsPath}
The transform scripts are located at: ${scriptsPath}
Sample input data is available at: ${inputsDir}

Please analyze the errors and provide fixed versions of the scripts. Focus on fixing the error paths and messages mentioned above. Consider the input data structure when making fixes. Return the complete fixed scripts with the same file names. Use elephant MCP to analyze the schema. Make sure to analyze verified scripts with it's examples as well`;

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

  console.log(
    `Uploaded fixed scripts to s3://${transformBucket}/${transformKey}`,
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
      throw new Error(`DLQ queue ${queueName} not found`);
    }
    return response.QueueUrl;
  } catch (error) {
    throw new Error(
      `Failed to get DLQ URL for county ${county}: ${error.message}`,
    );
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
      const errorsPath = await parseErrorsFromS3(execution.errorsS3Uri);

      // Step 4: Invoke Codex to fix errors
      await invokeCodexForFix(errorsPath, scriptsDir, inputsDir);

      // Step 5: Upload fixed scripts
      await uploadFixedScripts(execution.county, scriptsDir, transformPrefix);

      // Step 6: Invoke post-processing Lambda to verify fixes work
      const postProcessorFunctionName = requireEnv(
        "POST_PROCESSOR_FUNCTION_NAME",
      );
      const transactionsQueueUrl = requireEnv("TRANSACTIONS_SQS_QUEUE_URL");
      const seedOutputS3Uri = constructSeedOutputS3Uri(execution.preparedS3Uri);

      try {
        const resultPayload = await invokePostProcessingLambda({
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

          // Step 7: Mark errors as maybeSolved only if Lambda invocation succeeded
          const errorHashes = errorsPath.map((e) => e.hash);
          console.log(
            `Marking ${errorHashes.length} error hash(es) as maybeSolved...`,
          );
          await markErrorsAsMaybeSolved({
            errorHashes,
            tableName,
            documentClient: dynamoClient,
          });

          console.log("Auto-repair workflow completed successfully!");
        } else {
          // Execution failed, send original message to DLQ
          if (!execution.source) {
            throw new Error(
              `Execution ${execution.executionId} failed but source information is missing, cannot send to DLQ`,
            );
          }

          const dlqUrl = await getCountyDlqUrl(execution.county);
          await sendToDlq(dlqUrl, execution.source);

          console.log(
            `Execution failed, sent original message to DLQ: ${dlqUrl}`,
          );

          throw new Error(
            `Post-processing Lambda returned non-success status: ${resultPayload.status}`,
          );
        }
      } catch (lambdaError) {
        // If Lambda invocation failed, try to send original message to DLQ
        if (execution.source) {
          try {
            const dlqUrl = await getCountyDlqUrl(execution.county);
            await sendToDlq(dlqUrl, execution.source);

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
  } catch (error) {
    console.error("Auto-repair workflow failed:", error);
    process.exit(1);
  }
}

// Run main workflow
main();

