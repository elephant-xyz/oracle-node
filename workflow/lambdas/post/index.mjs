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

const s3 = new S3Client({});

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
    const parse = (u) => {
      const m = /^s3:\/\/([^/]+)\/(.*)$/.exec(u);
      if (!m) throw new Error("Bad S3 URI");
      return { bucket: m[1], key: m[2] };
    };
    const { bucket: pBucket, key: pKey } = parse(prepared);
    const { bucket: sBucket, key: sKey } = parse(seedZipS3);
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
    if (!transformResult.success) throw new Error(transformResult.error);
    const validationResult = await validate({ input: countyOut, cwd: tmp });
    if (!validationResult.success)
      throw new Error(
        `Validation failed: error=${validationResult.error}, path=${validationResult.path}`,
      );

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
    if (!hashResult.success)
      throw new Error(`Seed hash failed: ${hashResult.error}`);
    const csv = await fs.readFile(seedHashCsv, "utf8");
    const propertyCid = /propertyCid,?\n([^,\n]+)/.exec(csv)?.[1];
    const countyHashResult = await hash({
      input: countyOut,
      outputZip: countyHashZip,
      outputCsv: countyHashCsv,
      propertyCid: propertyCid,
      cwd: tmp,
    });
    if (!countyHashResult.success)
      throw new Error(`County hash failed: ${countyHashResult.error}`);

    // Upload to IPFS
    const pinataJwt = process.env.PINATA_JWT;
    if (!pinataJwt) throw new Error("PINATA_JWT is required");
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

    // Combine CSVs
    const combinedCsv = path.join(tmp, "combined_hash.csv");
    const seedCsvContent = await fs.readFile(seedHashCsv);
    const countyCsvContent = await fs.readFile(countyHashCsv);
    await fs.writeFile(
      combinedCsv,
      Buffer.concat([seedCsvContent, countyCsvContent]),
    );

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
