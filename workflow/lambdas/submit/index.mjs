import { promises as fs } from "fs";
import path from "path";
import os from "os";
import csvWriter from "csv-writer";
import { parse } from "csv-parse/sync";
import { submitToContract } from "@elephant-xyz/cli/lib";

const { createObjectCsvWriter } = csvWriter;

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
    const submitResult = await submitToContract({
      csvFile: csvFilePath,
      domain: process.env.ELEPHANT_DOMAIN,
      apiKey: process.env.ELEPHANT_API_KEY,
      oracleKeyId: process.env.ELEPHANT_ORACLE_KEY_ID,
      fromAddress: process.env.ELEPHANT_FROM_ADDRESS,
      rpcUrl: process.env.ELEPHANT_RPC_URL,
      cwd: tmp,
    });

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
