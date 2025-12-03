import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { promises as fs } from "fs";

const s3 = new S3Client({});

/**
 * @typedef {(level: "info"|"error"|"debug", msg: string, details?: Record<string, unknown>) => void} StructuredLogger
 */

/**
 * Parse an S3 URI into its bucket and key components.
 *
 * @param {string} uri - S3 URI in the form s3://bucket/key.
 * @returns {{ bucket: string, key: string }} - Parsed bucket and key.
 */
export function parseS3Uri(uri) {
  const match = /^s3:\/\/([^/]+)\/(.*)$/.exec(uri);
  if (!match) {
    throw new Error(`Bad S3 URI: ${uri}`);
  }
  return {
    bucket: /** @type {string} */ (match[1]),
    key: /** @type {string} */ (match[2]),
  };
}

/**
 * Build an S3 URI from bucket and key.
 *
 * @param {string} bucket - S3 bucket name.
 * @param {string} key - S3 object key.
 * @returns {string} - S3 URI.
 */
export function buildS3Uri(bucket, key) {
  return `s3://${bucket}/${key}`;
}

/**
 * Download an S3 object to a local file.
 *
 * @param {{ bucket: string, key: string }} location - Bucket and key location for the S3 object.
 * @param {string} destinationPath - Absolute path where the object will be stored.
 * @param {StructuredLogger} log - Structured logger.
 * @returns {Promise<void>}
 */
export async function downloadS3Object({ bucket, key }, destinationPath, log) {
  log("info", "download_s3_object", {
    bucket,
    key,
    destination: destinationPath,
  });
  const result = await s3.send(
    new GetObjectCommand({ Bucket: bucket, Key: key }),
  );
  const bytes = await result.Body?.transformToByteArray();
  if (!bytes) {
    throw new Error(`Failed to download ${key} from ${bucket}`);
  }
  await fs.writeFile(destinationPath, Buffer.from(bytes));
}

/**
 * Upload a local file to S3.
 *
 * @param {string} localPath - Local file path to upload.
 * @param {{ bucket: string, key: string }} location - Target S3 location.
 * @param {StructuredLogger} log - Structured logger.
 * @param {string} [contentType] - Optional content type.
 * @returns {Promise<string>} - S3 URI of uploaded object.
 */
export async function uploadToS3(localPath, { bucket, key }, log, contentType) {
  log("info", "upload_s3_object", { bucket, key });
  const content = await fs.readFile(localPath);
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: content,
      ContentType: contentType,
    }),
  );
  return buildS3Uri(bucket, key);
}

export { s3 };
