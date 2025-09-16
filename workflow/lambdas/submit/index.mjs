import { promises as fs } from "fs";
import path from "path";
import os from "os";
import csvWriter from "csv-writer";
import { parse } from "csv-parse/sync";
import { submitToContract } from "@elephant-xyz/cli/lib";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";

const { createObjectCsvWriter } = csvWriter;
const s3Client = new S3Client({ region: process.env.AWS_REGION });

/**
 * @typedef {Object} SubmitOutput
 * @property {string} status - Status of submit
 */

const base = { component: "submit", at: new Date().toISOString() };

/**
 * Build paths and derive prepare input from SQS/S3 event message.
 * - Extracts bucket/key from S3 event in SQS body
 * - Produces output prefix and input path for prepare Lambda
 *
 * @param {SubmitInput} event - Original SQS body JSON (already parsed by starter)
 * @returns {Promise<SubmitOutput>}
 */
async function downloadKeystoreFromS3(bucket, key, targetPath) {
  try {
    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    });
    const response = await s3Client.send(command);
    const streamToString = (stream) =>
      new Promise((resolve, reject) => {
        const chunks = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("error", reject);
        stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
      });
    const bodyContents = await streamToString(response.Body);
    await fs.writeFile(targetPath, bodyContents, "utf8");
    return targetPath;
  } catch (error) {
    throw new Error(`Failed to download keystore from S3: ${error.message}`);
  }
}

export const handler = async (event) => {
  if (!event.Records) throw new Error("Missing SQS Records");

  const toSubmit = event.Records.map((r) => JSON.parse(r.body)).flat();
  if (!toSubmit.length) throw new Error("No records to submit");

  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "submit-"));
  try {
    const csvFilePath = path.resolve(tmp, "submit.csv");
    const writer = createObjectCsvWriter({
      path: csvFilePath,
      header: Object.keys(toSubmit[0]).map((k) => ({ id: k, title: k })),
    });
    await writer.writeRecords(toSubmit);

    let submitResult;

    // Check if we're using keystore mode
    if (process.env.ELEPHANT_KEYSTORE_S3_KEY) {
      console.log(
        JSON.stringify({
          ...base,
          level: "info",
          msg: "Using keystore mode for contract submission",
        }),
      );

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
        });
      } finally {
        // Clean up keystore file immediately after use for security
        try {
          await fs.unlink(keystorePath);
          console.log(
            JSON.stringify({
              ...base,
              level: "info",
              msg: "Keystore file cleaned up successfully",
            }),
          );
        } catch (cleanupError) {
          console.error(
            JSON.stringify({
              ...base,
              level: "warn",
              msg: "Failed to clean up keystore file",
              error: cleanupError.message,
            }),
          );
        }
      }
    } else {
      console.log(
        JSON.stringify({
          ...base,
          level: "info",
          msg: "Using traditional API credentials for contract submission",
        }),
      );

      submitResult = await submitToContract({
        csvFile: csvFilePath,
        domain: process.env.ELEPHANT_DOMAIN,
        apiKey: process.env.ELEPHANT_API_KEY,
        oracleKeyId: process.env.ELEPHANT_ORACLE_KEY_ID,
        fromAddress: process.env.ELEPHANT_FROM_ADDRESS,
        rpcUrl: process.env.ELEPHANT_RPC_URL,
        cwd: tmp,
      });
    }

    if (!submitResult.success)
      throw new Error(`Submit failed: ${submitResult.error}`);
    const submitResultsCsv = await fs.readFile(
      path.join(tmp, "transaction-status.csv"),
      "utf8",
    );

    const submitResults = parse(submitResultsCsv, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    });

    console.log(
      JSON.stringify({
        ...base,
        level: "info",
        msg: "completed",
        submit_results: submitResults,
      }),
    );

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
      ...submitResults.filter((row) => row.status === "failed"),
    ];

    console.log(
      JSON.stringify({
        ...base,
        level: "info",
        msg: "completed",
        submit_errors: submitErrors,
      }),
    );
    if (allErrors.length > 0) {
      throw new Error(
        "Submit to the blockchain failed" + JSON.stringify(allErrors),
      );
    }
    console.log(JSON.stringify({ ...base, level: "info", msg: "completed" }));
    return { status: "success" };
  } finally {
    await fs.rm(tmp, { recursive: true, force: true });
  }
};
