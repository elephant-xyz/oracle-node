import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { promises as fs } from "fs";
import path from "path";
import os from "os";
import { fileURLToPath } from "url";
import { transform, validate, hash, upload } from "@elephant-xyz/cli/lib";
import { parse } from "csv-parse/sync";

const s3 = new S3Client({});

/**
 * @typedef {object} PrepareOutput
 * @property {string} output_s3_uri
 */

/**
 * @typedef {object} PostOutput
 * @property {PrepareOutput} prepare
 * @property {string} seed_output_s3_uri
 * @property {{ object?: { key?: string } }} [s3]
 * @property {{ key?: string }} [object]
 */

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


/**
 * @param {PostOutput} event - { prepare: prepare result, seed_output_s3_uri: S3 URI of seed output }
 * @returns {Promise<{status:string, transactionItems: object[]}>}
 */
/** @type {(event: PostOutput) => Promise<{status:string, transactionItems: object[]}>} */
export const handler = async (event) => {
  console.log(`Event is : ${JSON.stringify(event)}`);
  const base = { component: "post", at: new Date().toISOString() };
  /** @type {string | undefined} */
  let tmp;
  try {
    if (!event.prepare.output_s3_uri)
      throw new Error("prepare.output_s3_uri missing");
    if (!event.seed_output_s3_uri)
      throw new Error("seed_output_s3_uri missing");
    const outputBase = process.env.OUTPUT_BASE_URI;
    if (!outputBase) throw new Error("OUTPUT_BASE_URI is required");

    tmp = await fs.mkdtemp(path.join(os.tmpdir(), "post-"));
    const prepared = event.prepare.output_s3_uri;
    const seedZipS3 = event.seed_output_s3_uri;
    /**
     * @param {string} u
     * @returns {{ bucket: string, key: string }}
     */
    const parseS3Uri = (u) => {
      const m = /^s3:\/\/([^/]+)\/(.*)$/.exec(u);
      if (!m) throw new Error("Bad S3 URI");
      if (typeof m[1] !== "string" || typeof m[2] !== "string") throw new Error("S3 URI must be a string");
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

    console.log("Starting transform operation (includes fact sheet generation)...");
    const transformStart = Date.now();
    const transformResult = await transform({
      inputZip: countyZipLocal,
      outputZip: countyOut,
      scriptsZip: scriptsZip,
      cwd: tmp,
    });
    const transformDuration = Date.now() - transformStart;
    console.log(JSON.stringify({
      ...base,
      operation: "transform",
      duration_ms: transformDuration,
      duration_seconds: (transformDuration / 1000).toFixed(2),
      includes_fact_sheet_generation: true,
      msg: "Transform complete (includes fact sheet generation)"
    }));
    if (!transformResult.success) throw new Error(transformResult.error);

    console.log("Starting validation operation...");
    const validationStart = Date.now();
    const validationResult = await validate({ input: countyOut, cwd: tmp });
    const validationDuration = Date.now() - validationStart;
    console.log(JSON.stringify({
      ...base,
      operation: "validation",
      duration_ms: validationDuration,
      duration_seconds: (validationDuration / 1000).toFixed(2),
      msg: "Validation complete"
    }));
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
            event?.s3?.object?.key || "input.csv",
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

    console.log("Starting seed hash operation...");
    const seedHashStart = Date.now();
    const hashResult = await hash({
      input: seedZipLocal,
      outputZip: seedHashZip,
      outputCsv: seedHashCsv,
      cwd: tmp,
    });
    const seedHashDuration = Date.now() - seedHashStart;
    console.log(JSON.stringify({
      ...base,
      operation: "seed_hash",
      duration_ms: seedHashDuration,
      duration_seconds: (seedHashDuration / 1000).toFixed(2),
      msg: "Seed hash complete"
    }));
    if (!hashResult.success)
      throw new Error(`Seed hash failed: ${hashResult.error}`);
    const csv = await fs.readFile(seedHashCsv, "utf8");
    const records = parse(csv, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    });
    const propertyCid = records[0].propertyCid;

    console.log("Starting county hash operation...");
    const countyHashStart = Date.now();
    const countyHashResult = await hash({
      input: countyOut,
      outputZip: countyHashZip,
      outputCsv: countyHashCsv,
      propertyCid: propertyCid,
      cwd: tmp,
    });
    const countyHashDuration = Date.now() - countyHashStart;
    console.log(JSON.stringify({
      ...base,
      operation: "county_hash",
      duration_ms: countyHashDuration,
      duration_seconds: (countyHashDuration / 1000).toFixed(2),
      msg: "County hash complete"
    }));
    if (!countyHashResult.success)
      throw new Error(`County hash failed: ${countyHashResult.error}`);

    // Upload to IPFS
    const pinataJwt = process.env.ELEPHANT_PINATA_JWT;
    if (!pinataJwt) throw new Error("ELEPHANT_PINATA_JWT is required");

    console.log("Starting IPFS upload operation...");
    const uploadStart = Date.now();
    await Promise.all(
      [seedHashZip, countyHashZip].map(async (f) => {
        const fileName = path.basename(f);
        console.log(`Uploading ${fileName} to IPFS...`);
        const fileUploadStart = Date.now();
        if (!tmp) throw new Error("tmp is required");
        const uploadResult = await upload({
          input: f,
          pinataJwt: pinataJwt,
          cwd: tmp,
        });
        const fileUploadDuration = Date.now() - fileUploadStart;
        console.log(JSON.stringify({
          ...base,
          operation: "ipfs_upload_file",
          file: fileName,
          duration_ms: fileUploadDuration,
          duration_seconds: (fileUploadDuration / 1000).toFixed(2),
          msg: `Upload complete for ${fileName}`
        }));
        if (!uploadResult.success)
          throw new Error(`Upload failed: ${uploadResult.error}`);
      }),
    );
    const totalUploadDuration = Date.now() - uploadStart;
    console.log(JSON.stringify({
      ...base,
      operation: "ipfs_upload_total",
      duration_ms: totalUploadDuration,
      duration_seconds: (totalUploadDuration / 1000).toFixed(2),
      msg: "All uploads complete"
    }));

    // Combine CSVs
    const combinedCsv = await combineCsv(seedHashCsv, countyHashCsv, tmp);
    // Prepare submission CSV
    const uploadResults = combinedCsv;
    const inputKey = path.posix.basename(event?.object?.key || "input.csv");
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
    const combinedCsvContent = await fs.readFile(uploadResults, "utf8");
    const transactionItems = await parse(combinedCsvContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    });
    const totalOperationDuration = Date.now() - Date.parse(base.at);
    console.log(JSON.stringify({
      ...base,
      operation: "post_lambda_total",
      duration_ms: totalOperationDuration,
      duration_seconds: (totalOperationDuration / 1000).toFixed(2),
      transaction_items_count: transactionItems.length,
      msg: "Post Lambda execution complete"
    }));

    return { status: "success", transactionItems };
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
    } catch { }
  }
};
