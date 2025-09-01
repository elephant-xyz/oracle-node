import { GetObjectCommand, S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { promises as fs } from "fs";
import path from "path";
import { prepare } from "@elephant-xyz/cli/dist/lib/prepare.js";

const RE_S3PATH = /^s3:\/\/([^/]+)\/(.*)$/i;

/**
 * Splits an Amazon S3 URI into its bucket name and object key.
 *
 * @param {string} s3Uri - A valid S3 URI in the format `s3://<bucket>/<key>`.
 *   Example: `s3://my-bucket/folder/file.txt`
 *
 * @returns {{ bucket: string, key: string }} An object containing:
 *   - `bucket` {string} The S3 bucket name.
 *   - `key` {string} The S3 object key (path within the bucket).
 *
 * @throws {Error} If the input is not a valid S3 URI or does not include both bucket and key.
 */
const splitS3Uri = (s3Uri) => {
  const match = RE_S3PATH.exec(s3Uri);

  if (!match) {
    throw new Error('S3 path should be like: s3://bucket/object');
  }

  const [, bucket, key] = match;
  return { bucket, key };
};

/**
 * Lambda handler for processing orders and storing receipts in S3.
 * @param {Object} event - Input event containing order details
 * @param {string} event.input_s3_uri - S3 URI of input file
 * @param {string} event.output_s3_uri_prefix - S3 URI prefix for output files
 * @param {boolean} event.browser - Whether to run in headless browser
 * @returns {Promise<string>} Success message
 */
export const handler = async (event) => {
  console.log("Event:", event);
  if (!event || !event.input_s3_uri) {
    throw new Error("Missing required field: input_s3_uri");
  }
  const { bucket, key } = splitS3Uri(event.input_s3_uri);
  console.log("Bucket:", bucket);
  console.log("Key:", key);
  const s3 = new S3Client({});

  const tempDir = await fs.mkdtemp("/tmp/prepare-");
  try {
    const inputZip = path.join(tempDir, path.basename(key));
    const getResp = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const inputBytes = await getResp.Body?.transformToByteArray();
    if (!inputBytes) {
      throw new Error("Failed to download input object body");
    }
    await fs.writeFile(inputZip, Buffer.from(inputBytes));

    const outputZip = path.join(tempDir, "output.zip");
    const useBrowser = event.browser ?? true;
    await prepare(inputZip, outputZip, { browser: useBrowser });

    // Determine upload destination
    let outBucket = bucket;
    let outKey = key;
    if (event.output_s3_uri_prefix) {
      const { bucket: outB, key: outPrefix } = splitS3Uri(event.output_s3_uri_prefix);
      outBucket = outB;
      outKey = path.posix.join(outPrefix.replace(/\/$/, ""), "output.zip");
    } else {
      // Default: write next to input with a suffix
      const dir = path.posix.dirname(key);
      const base = path.posix.basename(key, path.extname(key));
      outKey = path.posix.join(dir, `${base}.prepared.zip`);
    }

    const outputBody = await fs.readFile(outputZip);
    await s3.send(new PutObjectCommand({ Bucket: outBucket, Key: outKey, Body: outputBody }));

    return { output_s3_uri: `s3://${outBucket}/${outKey}` };
  } finally {
    await fs.rm(tempDir, { recursive: true, force: true });
  }
};
