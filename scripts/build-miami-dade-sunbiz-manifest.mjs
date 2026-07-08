#!/usr/bin/env node

/**
 * Build a combined Sunbiz extraction manifest for Miami-Dade per-file cordata jobs.
 */

import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";

/** @typedef {object} ManifestEntry
 * @property {string} summaryUri
 * @property {string} extractKey
 */

/** @typedef {object} SunbizExtractManifest
 * @property {string} jobId
 * @property {string} schemaVersion
 * @property {ManifestEntry[]} entries
 */

const JOB_ID = "sunbiz-miami-dade-corporate-quarterly-2026q2";
const BUCKET = "elephant-oracle-node-environmentbucket-mmsoo3xbdi80";
const EXTRACT_KEY_PREFIX = "miami-dade-county-zips-cordata-2026q2";
const MANIFEST_KEY = `permit-harvest/${JOB_ID}/sunbiz/corporate-by-zip/manifest.json`;

/**
 * Build manifest entries for cordata0..cordata9 summary.json objects.
 *
 * @returns {ManifestEntry[]} Manifest entries.
 */
function buildEntries() {
  /** @type {ManifestEntry[]} */
  const entries = [];
  for (let index = 0; index < 10; index += 1) {
    const extractKey = `${EXTRACT_KEY_PREFIX}-cordata${index}`;
    entries.push({
      extractKey,
      summaryUri: `s3://${BUCKET}/permit-harvest/${JOB_ID}/sunbiz/corporate-by-zip/${extractKey}/summary.json`,
    });
  }
  return entries;
}

/**
 * Upload the combined manifest to S3.
 *
 * @returns {Promise<string>} Manifest S3 URI.
 */
async function main() {
  /** @type {SunbizExtractManifest} */
  const manifest = {
    jobId: JOB_ID,
    schemaVersion: "oracle-node.sunbiz-corporate-by-zip-manifest.v1",
    entries: buildEntries(),
  };
  const s3Client = new S3Client({});
  await s3Client.send(
    new PutObjectCommand({
      Bucket: BUCKET,
      Key: MANIFEST_KEY,
      Body: JSON.stringify(manifest, null, 2),
      ContentType: "application/json",
    }),
  );
  const manifestS3Uri = `s3://${BUCKET}/${MANIFEST_KEY}`;
  console.log(
    JSON.stringify({
      level: "info",
      message: "miami_dade_sunbiz_manifest_uploaded",
      manifestS3Uri,
      entryCount: manifest.entries.length,
    }),
  );
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ level: "error", message }));
  process.exit(1);
});
