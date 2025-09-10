import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { promises as fs } from "fs";
import path from "path";
import os from "os";
import { fileURLToPath } from "url";
import {
  transform,
  validate,
  hash,
  upload,
  submitToContract,
} from "@elephant-xyz/cli/lib";
import { parse } from "csv-parse/sync";

const s3 = new S3Client({});

/**
 * Combine two CSV files by keeping a single header row
 * from the first file and appending the data rows from both.
 *
 * @async
 * @function combineCsv
 * @param {string} seedHashCsv - Path to the first CSV file (provides header).
 * @param {string} countyHashCsv - Path to the second CSV file (data appended).
 * @param {string} tmp - Directory path where the combined file will be saved.
 * @returns {Promise<string>} - The path of the newly created combined CSV file.
 *
 * @example
 * const combined = await combineCsv("seed.csv", "county.csv", "/tmp");
 * console.log(`Combined CSV written to: ${combined}`);
 */
async function combineCsv(seedHashCsv, countyHashCsv, tmp) {
  const combinedCsv = path.join(tmp, "combined_hash.csv");

  const seedContent = await fs.readFile(seedHashCsv, "utf8");
  const countyContent = await fs.readFile(countyHashCsv, "utf8");

  const seedLines = seedContent.trim().split("\n");
  const countyLines = countyContent.trim().split("\n");

  const header = seedLines[0];

  const seedRows = seedLines.slice(1);
  const countyRows = countyLines.slice(1);

  const finalContent = [header, ...seedRows, ...countyRows].join("\n");
  await fs.writeFile(combinedCsv, finalContent, "utf8");

  return combinedCsv;
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

/** @typedef {Object} PostOutput */
/** @property {string} output_s3_uri - "success" */
/** @property {string} seed_output_s3_uri - S3 URI of submission ready CSV */

/**
 * @param {PostOutput} event - { input: original input, prepare: prepare result }
 * @returns {Promise<{status:string}>}
 */
export const handler = async (event) => {
  const base = { component: "post", at: new Date().toISOString() };
  let tmp;
  try {
    if (!event?.prepare?.output_s3_uri)
      throw new Error("prepare.output_s3_uri missing");
    if (!event?.seed_output_s3_uri)
      throw new Error("seed_output_s3_uri missing");
    const outputBase = process.env.OUTPUT_BASE_URI;
    if (!outputBase) throw new Error("OUTPUT_BASE_URI is required");

    tmp = await fs.mkdtemp(path.join(os.tmpdir(), "post-"));
    const prepared = event.prepare.output_s3_uri;
    const seedZipS3 = event.seed_output_s3_uri;
    const parseS3Uri = (u) => {
      const m = /^s3:\/\/([^/]+)\/(.*)$/.exec(u);
      if (!m) throw new Error("Bad S3 URI");
      return { bucket: m[1], key: m[2] };
    };
    const { bucket: pBucket, key: pKey } = parseS3Uri(prepared);
    const { bucket: sBucket, key: sKey } = parseS3Uri(seedZipS3);
    const countyZipLocal = path.join(tmp, "county_input.zip");
    const seedZipLocal = path.join(tmp, "seed_seed_output.zip");
    const pObj = await s3.send(
      new GetObjectCommand({ Bucket: pBucket, Key: pKey }),
    );
    const sObj = await s3.send(
      new GetObjectCommand({ Bucket: sBucket, Key: sKey }),
    );
    const cBytes = await pObj.Body?.transformToByteArray();
    const sBytes = await sObj.Body?.transformToByteArray();
    if (!cBytes || !sBytes) throw new Error("Failed to download required zips");
    await fs.writeFile(countyZipLocal, Buffer.from(cBytes));
    await fs.writeFile(seedZipLocal, Buffer.from(sBytes));

    // Transform county using elephant-cli. Use bundled transforms.zip if present
    const countyOut = path.join(tmp, "county_output.zip");
    const scriptsZip = path.join(
      path.dirname(fileURLToPath(import.meta.url)),
      "transforms.zip",
    );
    await fs.stat(scriptsZip);
    const transformResult = await transform({
      inputZip: countyZipLocal,
      outputZip: countyOut,
      scriptsZip: scriptsZip,
      cwed: tmp,
    });
    console.log("Transform complete");
    if (!transformResult.success) throw new Error(transformResult.error);
    const validationResult = await validate({ input: countyOut, cwd: tmp });
    console.log("Validation complete");
    if (!validationResult.success) {
      // Try to read submit_errors.csv and log as JSON
      const submitErrorsPath = path.join(tmp, "submit_errors.csv");
      let submitErrorsS3Uri = null;

      try {
        const submitErrors = await csvToJson(submitErrorsPath);

        // Upload submit_errors.csv to S3
        try {
          const submitErrorsCsv = await fs.readFile(submitErrorsPath);
          const inputKey = path.posix.basename(
            event?.input?.Records?.[0]?.s3?.object?.key || "input.csv",
          );
          const runPrefix = `${outputBase.replace(/\/$/, "")}/${inputKey}`;
          const errorFileKey = `${runPrefix.replace(/^s3:\/\//, "")}/submit_errors.csv`;
          const [errorBucket, ...errorParts] = errorFileKey.split("/");
          await s3.send(
            new PutObjectCommand({
              Bucket: errorBucket,
              Key: errorParts.join("/"),
              Body: submitErrorsCsv,
            }),
          );
          submitErrorsS3Uri = `s3://${errorBucket}/${errorParts.join("/")}`;
        } catch (uploadError) {
          console.error(`Failed to upload submit_errors.csv: ${uploadError}`);
        }

        console.error(
          JSON.stringify({
            ...base,
            level: "error",
            msg: "validation_failed",
            error: validationResult.error,
            path: validationResult.path,
            submit_errors: submitErrors,
            submit_errors_s3_uri: submitErrorsS3Uri,
          }),
        );
      } catch (csvError) {
        console.error(
          JSON.stringify({
            ...base,
            level: "error",
            msg: "validation_failed",
            error: validationResult.error,
            path: validationResult.path,
            submit_errors_read_error: String(csvError),
          }),
        );
      }

      const errorMessage = submitErrorsS3Uri
        ? `Validation failed. Submit errors CSV: ${submitErrorsS3Uri}`
        : "Validation failed";
      throw new Error(errorMessage);
    }

    // Hash seed and county; derive propertyCid from seed CSV if available
    const countyHashZip = path.join(tmp, "county_hash.zip");
    const countyHashCsv = path.join(tmp, "county_hash.csv");
    const seedHashZip = path.join(tmp, "seed_hash.zip");
    const seedHashCsv = path.join(tmp, "seed_hash.csv");
    const hashResult = await hash({
      input: seedZipLocal,
      outputZip: seedHashZip,
      outputCsv: seedHashCsv,
      cwd: tmp,
    });
    console.log("Seed hash complete");
    if (!hashResult.success)
      throw new Error(`Seed hash failed: ${hashResult.error}`);
    const csv = await fs.readFile(seedHashCsv, "utf8");
    const records = parse(csv, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    });
    const propertyCid = records[0].propertyCid;
    const countyHashResult = await hash({
      input: countyOut,
      outputZip: countyHashZip,
      outputCsv: countyHashCsv,
      propertyCid: propertyCid,
      cwd: tmp,
    });
    console.log("County hash complete");
    if (!countyHashResult.success)
      throw new Error(`County hash failed: ${countyHashResult.error}`);

    // Upload to IPFS
    const pinataJwt = process.env.ELEPHANT_PINATA_JWT;
    if (!pinataJwt) throw new Error("ELEPHANT_PINATA_JWT is required");
    await Promise.all(
      [seedHashZip, countyHashZip].map(async (f) => {
        const uploadResult = await upload({
          input: f,
          pinataJwt: pinataJwt,
          cwd: tmp,
        });
        if (!uploadResult.success)
          throw new Error(`Upload failed: ${uploadResult.error}`);
      }),
    );
    console.log("Upload complete");

    // Combine CSVs
    const combinedCsv = await combineCsv(seedHashCsv, countyHashCsv, tmp);
    // Prepare submission CSV
    const uploadResults = combinedCsv;
    const inputKey = path.posix.basename(
      event?.Records?.[0]?.s3?.object?.key || "input.csv",
    );
    const runPrefix = `${outputBase.replace(/\/$/, "")}/${inputKey}`;
    const [outBucket, ...outKeyParts] = runPrefix
      .replace(/^s3:\/\//, "")
      .split("/");
    const outKeyPrefix = outKeyParts.join("/");
    const submissionS3 = {
      Bucket: outBucket,
      Key: `${outKeyPrefix}/submission_ready.csv`,
    };
    const submissionBody = await fs.readFile(uploadResults);
    await s3.send(
      new PutObjectCommand({ ...submissionS3, Body: submissionBody }),
    );

    // Submit to blockchain
    const reqVars = [
      "ELEPHANT_DOMAIN",
      "ELEPHANT_API_KEY",
      "ELEPHANT_ORACLE_KEY_ID",
      "ELEPHANT_FROM_ADDRESS",
      "ELEPHANT_RPC_URL",
    ];
    for (const v of reqVars)
      if (!process.env[v]) throw new Error(`${v} required`);
    const submitResult = await submitToContract({
      csvFile: uploadResults,
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

    const submitResultsJson = submitResults.map((row) => {
      return {
        ...row,
        propertyCid: propertyCid,
      };
    });
    console.log(
      JSON.stringify({
        ...base,
        level: "info",
        msg: "completed",
        submit_results: submitResultsJson,
      }),
    );

    const submitErrrorsCsv = await fs.readFile(
      path.join(tmp, "submit_errors.csv"),
      "utf8",
    );
    const submitErrors =
      parse(submitErrrorsCsv, {
        columns: true,
        skip_empty_lines: true,
        trim: true,
      }) + submitResults.filter((row) => row.status === "failed");

    const submitErrorsJson = submitErrors.map((row) => {
      return {
        ...row,
        propertyCid: propertyCid,
      };
    });
    console.log(
      JSON.stringify({
        ...base,
        level: "info",
        msg: "completed",
        submit_errors: submitErrorsJson,
      }),
    );
    if (submitErrors.length > 0) {
      throw new Error(
        "Submit to the blockchain failed" + JSON.stringify(submitErrorsJson),
      );
    }
    console.log(JSON.stringify({ ...base, level: "info", msg: "completed" }));
    return { status: "success" };
  } catch (err) {
    console.error(
      JSON.stringify({
        ...base,
        level: "error",
        msg: "failed",
        error: String(err),
      }),
    );
    throw err;
  } finally {
    try {
      if (tmp) await fs.rm(tmp, { recursive: true, force: true });
    } catch {}
  }
};
