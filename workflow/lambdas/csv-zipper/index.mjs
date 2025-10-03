import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { promises as fs } from "fs";
import path from "path";
import os from "os";
import AdmZip from "adm-zip";

const s3 = new S3Client({});

/**
 * @typedef {Object} S3Bucket
 * @property {string} name
 */

/**
 * @typedef {Object} S3Object
 * @property {string} key
 */

/**
 * @typedef {Object} S3Message
 * @property {S3Bucket} bucket
 * @property {S3Object} object
 */

/**
 * @typedef {Object} CsvZipperInput
 * @property {S3Message} s3 - Incoming message with CSV location
 */

/**
 * @typedef {Object} CsvZipperOutput
 * @property {string} county_prep_input_s3_uri - S3 URI of newly created prepare input ZIP
 * @property {string} output_prefix - S3 URI prefix where prepare output should be written
 */

/**
 * Download a file from S3 to local path
 * @param {string} bucket
 * @param {string} key
 * @param {string} dest
 * @returns {Promise<void>}
 */
async function downloadFromS3(bucket, key, dest) {
  const resp = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const bytes = await resp.Body?.transformToByteArray();
  if (!bytes) throw new Error(`Failed to download body for s3://${bucket}/${key}`);
  await fs.writeFile(dest, Buffer.from(bytes));
}

/**
 * Upload a buffer to S3
 * @param {string} bucket
 * @param {string} key
 * @param {Uint8Array|Buffer} body
 * @returns {Promise<void>}
 */
async function uploadToS3(bucket, key, body) {
  await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
}

/**
 * @param {CsvZipperInput} event
 * @returns {Promise<CsvZipperOutput>}
 */
export const handler = async (event) => {
  if (!event?.s3?.bucket?.name || !event?.s3?.object?.key) {
    throw new Error("Missing s3.bucket.name or s3.object.key");
  }
  const outputBaseUri = process.env.OUTPUT_BASE_URI;
  if (!outputBaseUri) throw new Error("OUTPUT_BASE_URI is required");

  const inBucket = event.s3.bucket.name;
  const inKey = event.s3.object.key;

  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "csv-zipper-"));
  try {
    const csvLocal = path.join(tmpDir, path.basename(inKey));
    await downloadFromS3(inBucket, inKey, csvLocal);

    // Create a ZIP containing the CSV at root with a stable name
    const zip = new AdmZip();
    const csvContents = await fs.readFile(csvLocal);
    const baseNameNoExt = path.basename(inKey, path.extname(inKey));
    const entryName = `${baseNameNoExt}.csv`;
    zip.addFile(entryName, csvContents);
    const zipBuffer = zip.toBuffer();

    // Compute output locations
    const runId = baseNameNoExt;
    const normalizedBase = outputBaseUri.replace(/\/$/, "");
    const outputPrefix = `${normalizedBase}/${runId}`;
    const { bucket: outBucket, key: outKey } = (() => {
      const m = /^s3:\/\/([^/]+)\/(.*)$/.exec(outputPrefix);
      if (!m) throw new Error("Bad OUTPUT_BASE_URI");
      return { bucket: m[1], key: m[2] };
    })();

    const prepareInputKey = path.posix.join(outKey || "", "prepare-input.zip");
    await uploadToS3(outBucket || "", prepareInputKey, zipBuffer);

    return {
      county_prep_input_s3_uri: `s3://${outBucket}/${prepareInputKey}`,
      output_prefix: outputPrefix,
    };
  } finally {
    try { await fs.rm(tmpDir, { recursive: true, force: true }); } catch {}
  }
};


