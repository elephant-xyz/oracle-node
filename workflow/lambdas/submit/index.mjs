import { promises as fs } from "fs";
import path from "path";
import os from "os";
import { createObjectCsvWriter } from "csv-writer";
import { parse } from "csv-parse/sync";
import { submitToContract } from "@elephant-xyz/cli/lib";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import {
  SFNClient,
  SendTaskSuccessCommand,
  SendTaskFailureCommand,
} from "@aws-sdk/client-sfn";

const s3Client = new S3Client({
  region: process.env.AWS_REGION || "us-east-1",
});

const ssmClient = new SSMClient({
  region: process.env.AWS_REGION || "us-east-1",
});

const sfnClient = new SFNClient({
  region: process.env.AWS_REGION || "us-east-1",
});

/**
 * @typedef {Object} SubmitOutput
 * @property {string} status - Status of submit
 */

/**
 * @typedef {Object} SubmitInput
 * @property {{ body: string }[]} [Records] - SQS event format (for backward compatibility)
 * @property {string} [taskToken] - Step Function task token (when invoked from Step Functions)
 * @property {string} [executionArn] - Step Function execution ARN (when invoked from Step Functions)
 * @property {Object[]} [transactionItems] - Transaction items array (when invoked from Step Functions)
 */

/**
 * @typedef {Object} SubmitResultRow
 * @property {string} status - Status of the submission
 * @property {string} [txHash] - Transaction hash
 * @property {string} [error] - Error message if failed
 */

const base = { component: "submit", at: new Date().toISOString() };

/**
 * Download keystore from S3
 * @param {string} bucket
 * @param {string} key
 * @param {string} targetPath
 * @returns {Promise<string>}
 */
async function downloadKeystoreFromS3(bucket, key, targetPath) {
  try {
    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    });
    const response = await s3Client.send(command);
    /**
     * @param {any} stream
     * @returns {Promise<string>}
     */
    const streamToString = (stream) =>
      new Promise((resolve, reject) => {
        /** @type {Buffer[]} */
        const chunks = [];
        stream.on(
          "data",
          /** @param {Buffer} chunk */ (chunk) => chunks.push(chunk),
        );
        stream.on("error", reject);
        stream.on("end", () =>
          resolve(Buffer.concat(chunks).toString("utf-8")),
        );
      });
    const body = response.Body;
    if (!body)
      throw new Error("Failed to download keystore from S3: body not found");
    const bodyContents = await streamToString(body);
    await fs.writeFile(targetPath, bodyContents, "utf8");
    return targetPath;
  } catch (error) {
    throw new Error(
      `Failed to download keystore from S3: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

/**
 * Get gas price configuration from AWS SSM Parameter Store
 * Used only for self-custodial oracles (keystore mode) to allow dynamic gas price configuration
 *
 * Supports two formats for backward compatibility:
 * 1. Simple string: "25" (legacy - sets gasPrice=25 Gwei, elephant-cli calculates priority fee)
 * 2. JSON object: {"maxFeePerGas":"30","maxPriorityFeePerGas":"5"} (EIP-1559 - full control)
 *
 * @returns {Promise<{gasPrice?: string, maxFeePerGas?: string, maxPriorityFeePerGas?: string} | undefined>}
 */
async function getGasPriceFromSSM() {
  try {
    const parameterName =
      process.env.GAS_PRICE_PARAMETER_NAME || "/elephant-oracle-node/gas-price";

    console.log({
      component: "submit",
      level: "info",
      msg: "Fetching gas price configuration from SSM Parameter Store",
      parameterName,
      at: new Date().toISOString(),
    });

    const command = new GetParameterCommand({
      Name: parameterName,
      WithDecryption: true,
    });

    const response = await ssmClient.send(command);
    const paramValue = response.Parameter?.Value;

    if (!paramValue) {
      console.log({
        component: "submit",
        level: "warn",
        msg: "Gas price parameter exists but has no value",
        at: new Date().toISOString(),
      });
      return undefined;
    }

    // Try to parse as JSON first (EIP-1559 format)
    try {
      const parsed = JSON.parse(paramValue);

      // Validate it's an object with expected fields
      if (typeof parsed === "object" && parsed !== null) {
        /** @type {{maxFeePerGas?: string, maxPriorityFeePerGas?: string}} */
        const result = {};

        if (parsed.maxFeePerGas) {
          result.maxFeePerGas = String(parsed.maxFeePerGas);
        }

        if (parsed.maxPriorityFeePerGas) {
          result.maxPriorityFeePerGas = String(parsed.maxPriorityFeePerGas);
        }

        // If we found EIP-1559 params, return them
        if (result.maxFeePerGas || result.maxPriorityFeePerGas) {
          console.log({
            component: "submit",
            level: "info",
            msg: "Successfully retrieved EIP-1559 gas configuration from SSM",
            config: result,
            at: new Date().toISOString(),
          });
          return result;
        }
      }
    } catch (jsonError) {
      // Not JSON, treat as simple string (legacy format)
    }

    // Legacy format: simple string value
    console.log({
      component: "submit",
      level: "info",
      msg: "Successfully retrieved legacy gas price from SSM",
      gasPrice: paramValue,
      at: new Date().toISOString(),
    });

    return { gasPrice: paramValue };
  } catch (error) {
    // If parameter doesn't exist or there's an access issue, log and continue without gas price
    console.log({
      component: "submit",
      level: "warn",
      msg: "Failed to retrieve gas price from SSM, continuing without explicit gas price",
      error: error instanceof Error ? error.message : String(error),
      at: new Date().toISOString(),
    });
    return undefined;
  }
}

/**
 * Send success callback to Step Functions
 * @param {string} taskToken
 * @param {Object} output
 * @returns {Promise<void>}
 */
async function sendTaskSuccess(taskToken, output) {
  try {
    const cmd = new SendTaskSuccessCommand({
      taskToken: taskToken,
      output: JSON.stringify(output),
    });
    await sfnClient.send(cmd);
    console.log({
      ...base,
      level: "info",
      msg: "task_success_sent_to_step_functions",
    });
  } catch (err) {
    console.error({
      ...base,
      level: "error",
      msg: "failed_to_send_task_success",
      error: err instanceof Error ? err.message : String(err),
    });
    throw err;
  }
}

/**
 * Send failure callback to Step Functions
 * @param {string} taskToken
 * @param {string} error
 * @param {string} [cause]
 * @returns {Promise<void>}
 */
async function sendTaskFailure(taskToken, error, cause) {
  try {
    const cmd = new SendTaskFailureCommand({
      taskToken: taskToken,
      error: error,
      cause: cause,
    });
    await sfnClient.send(cmd);
    console.log({
      ...base,
      level: "info",
      msg: "task_failure_sent_to_step_functions",
    });
  } catch (err) {
    console.error({
      ...base,
      level: "error",
      msg: "failed_to_send_task_failure",
      error: err instanceof Error ? err.message : String(err),
    });
    throw err;
  }
}

/**
 * Submit handler - supports both SQS events and Step Function Task Token invocations
 * - SQS format: { Records: [{ body: string }] }
 * - Step Function format: { taskToken: string, executionArn: string, transactionItems: Object[] }
 *
 * @param {SubmitInput} event - Either SQS event or Step Function task token payload
 * @returns {Promise<SubmitOutput>}
 */
export const handler = async (event) => {
  const isStepFunctionInvocation = !!event.taskToken;
  let toSubmit;

  if (isStepFunctionInvocation) {
    // Invoked from Step Functions with Task Token
    if (!event.transactionItems || !Array.isArray(event.transactionItems)) {
      const error = "Missing or invalid transactionItems in Step Function payload";
      if (event.taskToken) {
        await sendTaskFailure(event.taskToken, "InvalidInput", error);
      }
      throw new Error(error);
    }
    toSubmit = event.transactionItems;
    console.log({
      ...base,
      level: "info",
      msg: "invoked_from_step_functions",
      executionArn: event.executionArn,
      itemCount: toSubmit.length,
    });
  } else {
    // Invoked from SQS (backward compatibility)
    if (!event.Records) throw new Error("Missing SQS Records");
    toSubmit = event.Records.map((r) => JSON.parse(r.body)).flat();
  }

  if (!toSubmit.length) {
    const error = "No records to submit";
    if (isStepFunctionInvocation && event.taskToken) {
      await sendTaskFailure(event.taskToken, "EmptyInput", error);
    }
    throw new Error(error);
  }

  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "submit-"));
  try {
    const csvFilePath = path.resolve(tmp, "submit.csv");
    const writer = createObjectCsvWriter({
      path: csvFilePath,
      header: Object.keys(toSubmit[0]).map((k) => ({ id: k, title: k })),
    });
    await writer.writeRecords(toSubmit);

    let submitResult;

    if (!process.env.ELEPHANT_RPC_URL)
      throw new Error("ELEPHANT_RPC_URL is required");

    // Check if we're using keystore mode
    if (process.env.ELEPHANT_KEYSTORE_S3_KEY) {
      console.log({
        ...base,
        level: "info",
        msg: "Using keystore mode for contract submission (self-custodial)",
      });
      if (!process.env.ENVIRONMENT_BUCKET)
        throw new Error("ENVIRONMENT_BUCKET is required");
      if (!process.env.ELEPHANT_KEYSTORE_S3_KEY)
        throw new Error("ELEPHANT_KEYSTORE_S3_KEY is required");
      if (!process.env.ELEPHANT_KEYSTORE_PASSWORD)
        throw new Error("ELEPHANT_KEYSTORE_PASSWORD is required");

      // Fetch gas price configuration from SSM Parameter Store for self-custodial oracles
      const gasPriceConfig = await getGasPriceFromSSM();

      // Download keystore from S3 with unique filename to avoid conflicts
      const keystorePath = path.resolve(
        tmp,
        `keystore-${Date.now()}-${Math.random().toString(36).substring(7)}.json`,
      );
      await downloadKeystoreFromS3(
        process.env.ENVIRONMENT_BUCKET,
        process.env.ELEPHANT_KEYSTORE_S3_KEY,
        keystorePath,
      );

      try {
        submitResult = await submitToContract({
          csvFile: csvFilePath,
          keystoreJson: keystorePath,
          keystorePassword: process.env.ELEPHANT_KEYSTORE_PASSWORD,
          rpcUrl: process.env.ELEPHANT_RPC_URL,
          cwd: tmp,
          // Spread gas price configuration (supports both legacy and EIP-1559 formats)
          ...(gasPriceConfig || {}),
        });
      } finally {
        // Clean up keystore file immediately after use for security
        try {
          await fs.unlink(keystorePath);
          console.log({
            ...base,
            level: "info",
            msg: "Keystore file cleaned up successfully",
          });
        } catch (cleanupError) {
          console.error({
            ...base,
            level: "warn",
            msg: "Failed to clean up keystore file",
            error:
              cleanupError instanceof Error
                ? cleanupError.message
                : String(cleanupError),
          });
        }
      }
    } else {
      console.log({
        ...base,
        level: "info",
        msg: "Using traditional API credentials for contract submission",
      });

      if (!process.env.ELEPHANT_DOMAIN)
        throw new Error("ELEPHANT_DOMAIN is required");
      if (!process.env.ELEPHANT_API_KEY)
        throw new Error("ELEPHANT_API_KEY is required");
      if (!process.env.ELEPHANT_ORACLE_KEY_ID)
        throw new Error("ELEPHANT_ORACLE_KEY_ID is required");
      if (!process.env.ELEPHANT_FROM_ADDRESS)
        throw new Error("ELEPHANT_FROM_ADDRESS is required");
      if (!process.env.ELEPHANT_RPC_URL)
        throw new Error("ELEPHANT_RPC_URL is required");

      submitResult = await submitToContract({
        csvFile: csvFilePath,
        domain: process.env.ELEPHANT_DOMAIN,
        apiKey: process.env.ELEPHANT_API_KEY,
        oracleKeyId: process.env.ELEPHANT_ORACLE_KEY_ID,
        fromAddress: process.env.ELEPHANT_FROM_ADDRESS,
        rpcUrl: process.env.ELEPHANT_RPC_URL,
        cwd: tmp,
        // Note: Gas price is not passed in API mode - Elephant manages it
      });
    }

    if (!submitResult.success)
      throw new Error(`Submit failed: ${submitResult.error}`);
    const submitResultsCsv = await fs.readFile(
      path.join(tmp, "transaction-status.csv"),
      "utf8",
    );

    /** @type {SubmitResultRow[]} */
    const submitResults = parse(submitResultsCsv, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    });

    console.log({
      ...base,
      level: "info",
      msg: "completed",
      submit_results: submitResults,
    });

    const submitErrrorsCsv = await fs.readFile(
      path.join(tmp, "submit_errors.csv"),
      "utf8",
    );
    const submitErrors = parse(submitErrrorsCsv, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    });
    console.log(`Submit errors type is : ${typeof submitErrors}`);
    const allErrors = [
      ...submitErrors,
      ...submitResults.filter(
        /** @param {SubmitResultRow} row */ (row) => row.status === "failed",
      ),
    ];

    console.log({
      ...base,
      level: "info",
      msg: "completed",
      submit_errors: submitErrors,
    });
    if (allErrors.length > 0) {
      const errorMsg = "Submit to the blockchain failed" + JSON.stringify(allErrors);
      if (isStepFunctionInvocation && event.taskToken) {
        await sendTaskFailure(event.taskToken, "SubmitFailed", errorMsg);
      }
      throw new Error(errorMsg);
    }

    const result = { status: "success", submitResults: submitResults };
    console.log({ ...base, level: "info", msg: "completed", result });

    // Send success callback to Step Functions if invoked with task token
    if (isStepFunctionInvocation && event.taskToken) {
      await sendTaskSuccess(event.taskToken, result);
    }

    return result;
  } catch (err) {
    // If we have a task token and haven't sent failure yet, send it now
    if (isStepFunctionInvocation && event.taskToken) {
      const errMessage = err instanceof Error ? err.message : String(err);
      const errCause = err instanceof Error ? err.stack : undefined;
      await sendTaskFailure(event.taskToken, "SubmitError", errCause || errMessage);
    }
    throw err;
  } finally {
    await fs.rm(tmp, { recursive: true, force: true });
  }
};
