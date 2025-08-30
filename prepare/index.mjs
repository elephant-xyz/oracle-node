import {
  GetObjectCommand,
  S3Client,
  S3ServiceException,
  NoSuchKey,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { promises as fs } from "fs";
import { prepare } from "@elephant-xyz/cli/dist/lib/prepare.js";

/**
 * Splits an Amazon S3 URI into its bucket name and object key.
 *
 * @param {string} s3_uri - A valid S3 URI in the format `s3://<bucket>/<key>`.
 *   Example: `s3://my-bucket/folder/file.txt`
 *
 * @returns {{ bucket: string, key: string }} An object containing:
 *   - `bucket` {string} The S3 bucket name.
 *   - `key` {string} The S3 object key (path within the bucket).
 *
 * @throws {Error} If the input is not a valid S3 URI or does not include both bucket and key.
 */
const split_s3_uri = (s3_uri) => {
  const [bucket, ...rest] = s3_uri.split("/").slice(3);
  if (!bucket || rest.length === 0) {
    throw new Error(`Invalid S3 URI: ${s3_uri}`);
  }
  return { bucket, key: rest.join("/") };
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
  const { bucket, key } = split_s3_uri(event.input_s3_uri);
  const s3 = new S3Client({});
  try {
    const data = await s3.send(
      new GetObjectCommand({
        Bucket: bucket,
        Key: key,
      }),
    );
    console.log(data);
  } catch (err) {
    if (err instanceof NoSuchKey) {
      console.error(
        `Error from S3 while getting object "${key}" from "${bucketName}". No such key exists.`,
      );
    } else if (err instanceof S3ServiceException) {
      console.error(
        `Error from S3 while getting object from ${bucketName}.  ${caught.name}: ${caught.message}`,
      );
    }
    throw err;
  }
  const tempDir = await fs.mkdtemp("/tmp/prepare-");
  try {
    const inputZip = path.join(tempDir, key);
    await fs.writeFile(inputZip, data.Body);
    const outputZip = path.join(tempDir, "output.zip");
    const useBrowser = event.browser ?? true;
    await prepare(inputZip, outputZip, { browser: useBrowser });
    await s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: fs.readFileSync(outputZip),
      }),
    );
  } finally {
    await fs.rm(tempDir, { recursive: true });
  }
};

handler({});
