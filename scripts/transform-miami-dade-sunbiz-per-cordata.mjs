#!/usr/bin/env node

/**
 * Transform Miami-Dade Sunbiz extraction one cordata file at a time, upload
 * lexicon parts to S3 with cordata-scoped filenames, and emit a combined summary.
 */

import {
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { createReadStream } from "node:fs";
import { mkdir, readdir, readFile, rm, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import { processSunbizManifest } from "./transform-sunbiz-corporate-to-lexicon.mjs";

const BUCKET = "elephant-oracle-node-environmentbucket-mmsoo3xbdi80";
const JOB_ID = "sunbiz-miami-dade-corporate-quarterly-2026q2";
const EXTRACT_KEY_PREFIX = "miami-dade-county-zips-cordata-2026q2";
const OUTPUT_S3_PREFIX = `permit-harvest/${JOB_ID}/lexicon-transform/business-registration-v1`;
const PART_RECORD_LIMIT = 50_000;

/** @typedef {object} ManifestEntry
 * @property {string} summaryUri
 * @property {string} extractKey
 */

/** @typedef {object} SunbizExtractManifest
 * @property {string} jobId
 * @property {string} schemaVersion
 * @property {ManifestEntry[]} entries
 */

/** @typedef {object} CordataRunOptions
 * @property {number} startIndex
 * @property {number} endIndex
 * @property {boolean} uploadOnly
 */

/**
 * Pause execution for a bounded backoff interval.
 *
 * @param {number} delayMs - Delay in milliseconds.
 * @returns {Promise<void>}
 */
function sleep(delayMs) {
  return new Promise((resolve) => {
    setTimeout(resolve, delayMs);
  });
}

/**
 * Build a single-entry manifest for one cordata file.
 *
 * @param {number} index - Cordata index 0-9.
 * @returns {SunbizExtractManifest} Manifest object.
 */
function buildSingleEntryManifest(index) {
  const extractKey = `${EXTRACT_KEY_PREFIX}-cordata${index}`;
  return {
    jobId: JOB_ID,
    schemaVersion: "oracle-node.sunbiz-corporate-by-zip-manifest.v1",
    entries: [
      {
        extractKey,
        summaryUri: `s3://${BUCKET}/permit-harvest/${JOB_ID}/sunbiz/corporate-by-zip/${extractKey}/summary.json`,
      },
    ],
  };
}

/**
 * Determine whether an error is likely transient for S3 operations.
 *
 * @param {unknown} error - Thrown error value.
 * @returns {boolean}
 */
function isRetryableS3Error(error) {
  if (!(error instanceof Error)) return false;
  const retryableCodes = new Set([
    "ECONNRESET",
    "ETIMEDOUT",
    "EPIPE",
    "TimeoutError",
    "RequestTimeout",
    "SlowDown",
    "InternalError",
    "ServiceUnavailable",
  ]);
  if (retryableCodes.has(error.code ?? "") || retryableCodes.has(error.name)) {
    return true;
  }
  const message = error.message.toLowerCase();
  return (
    message.includes("timeout") ||
    message.includes("socket connection") ||
    message.includes("econnreset")
  );
}

/**
 * @param {string | boolean | undefined} value
 * @param {string} optionName
 * @returns {number}
 */
function parseRequiredIndex(value, optionName) {
  if (typeof value !== "string") {
    throw new Error(`--${optionName} is required`);
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0 || parsed > 9) {
    throw new Error(`--${optionName} must be an integer between 0 and 9`);
  }
  return parsed;
}

/**
 * @returns {CordataRunOptions}
 */
function parseRunOptions() {
  const { values } = parseArgs({
    options: {
      "start-index": { type: "string", default: "0" },
      "end-index": { type: "string", default: "9" },
      "upload-only": { type: "boolean", default: false },
    },
    strict: true,
    allowPositionals: false,
  });
  const startIndex = parseRequiredIndex(values["start-index"], "start-index");
  const endIndex = parseRequiredIndex(values["end-index"], "end-index");
  if (endIndex < startIndex) {
    throw new Error("--end-index must be >= --start-index");
  }
  return {
    startIndex,
    endIndex,
    uploadOnly: values["upload-only"] === true,
  };
}

/**
 * @param {S3Client} s3Client
 * @param {string} key
 * @returns {Promise<boolean>}
 */
async function s3ObjectExists(s3Client, key) {
  try {
    await s3Client.send(new HeadObjectCommand({ Bucket: BUCKET, Key: key }));
    return true;
  } catch (error) {
    if (
      error instanceof Error &&
      (error.name === "NotFound" || error.name === "NoSuchKey")
    ) {
      return false;
    }
    throw error;
  }
}

/**
 * @param {S3Client} s3Client
 * @param {string} localPath
 * @param {string} dataset
 * @param {number} cordataIndex
 * @param {string} fileName
 * @param {number} [maxAttempts=5]
 * @returns {Promise<string>}
 */
async function uploadPart(
  s3Client,
  localPath,
  dataset,
  cordataIndex,
  fileName,
  maxAttempts = 5,
) {
  const key = `${OUTPUT_S3_PREFIX}/${dataset}/cordata${cordataIndex}-${fileName}`;
  if (await s3ObjectExists(s3Client, key)) {
    return `s3://${BUCKET}/${key}`;
  }
  let lastError = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const body = createReadStream(localPath);
      await s3Client.send(
        new PutObjectCommand({
          Bucket: BUCKET,
          Key: key,
          Body: body,
          ContentType: "application/x-ndjson",
        }),
      );
      return `s3://${BUCKET}/${key}`;
    } catch (error) {
      lastError = error;
      if (attempt >= maxAttempts || !isRetryableS3Error(error)) {
        throw error;
      }
      await sleep(Math.min(30_000, 1_000 * 2 ** (attempt - 1)));
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

/**
 * @param {S3Client} s3Client
 * @param {string} localOutputDir
 * @param {number} cordataIndex
 * @returns {Promise<string[]>}
 */
async function uploadLocalOutput(s3Client, localOutputDir, cordataIndex) {
  /** @type {string[]} */
  const uploaded = [];
  const datasetDirs = await readdir(localOutputDir, { withFileTypes: true });
  for (const datasetDir of datasetDirs) {
    if (!datasetDir.isDirectory()) continue;
    const classDirs = await readdir(path.join(localOutputDir, datasetDir.name), {
      withFileTypes: true,
    });
    for (const classDir of classDirs) {
      if (!classDir.isDirectory()) continue;
      const dataset = `${datasetDir.name}/${classDir.name}`;
      const partDir = path.join(localOutputDir, datasetDir.name, classDir.name);
      const partFiles = (await readdir(partDir)).filter((name) =>
        name.endsWith(".jsonl"),
      );
      for (const fileName of partFiles.sort()) {
        uploaded.push(
          await uploadPart(
            s3Client,
            path.join(partDir, fileName),
            dataset,
            cordataIndex,
            fileName,
          ),
        );
      }
    }
  }
  return uploaded;
}

/**
 * @param {string} localOutputDir
 * @returns {Promise<Record<string, unknown>>}
 */
async function readLocalSummary(localOutputDir) {
  const summaryPath = path.join(localOutputDir, "summary.json");
  const summaryText = await readFile(summaryPath, "utf8");
  return /** @type {Record<string, unknown>} */ (JSON.parse(summaryText));
}

/**
 * @param {S3Client} s3Client
 * @param {number} index
 * @param {string} workRoot
 * @param {boolean} uploadOnly
 * @returns {Promise<Record<string, unknown>>}
 */
async function transformCordataFile(s3Client, index, workRoot, uploadOnly) {
  const localOutputDir = path.join(workRoot, `cordata${index}`);
  const localSummaryPath = path.join(localOutputDir, "summary.json");
  /** @type {Record<string, unknown>} */
  let summary;

  if (uploadOnly) {
    await stat(localSummaryPath);
    summary = await readLocalSummary(localOutputDir);
  } else {
    try {
      await stat(localSummaryPath);
      summary = await readLocalSummary(localOutputDir);
      console.log(
        JSON.stringify({
          level: "info",
          message: "miami_dade_sunbiz_transform_cordata_reused_local",
          cordataIndex: index,
        }),
      );
    } catch {
      const manifest = buildSingleEntryManifest(index);
      const manifestKey = `permit-harvest/${JOB_ID}/sunbiz/corporate-by-zip/manifest-cordata${index}.json`;
      await s3Client.send(
        new PutObjectCommand({
          Bucket: BUCKET,
          Key: manifestKey,
          Body: JSON.stringify(manifest, null, 2),
          ContentType: "application/json",
        }),
      );
      await rm(localOutputDir, { recursive: true, force: true });
      await mkdir(localOutputDir, { recursive: true });
      summary = await processSunbizManifest({
        s3Client,
        manifestS3Uri: `s3://${BUCKET}/${manifestKey}`,
        outputLocation: { kind: "local", dir: localOutputDir },
        maxChunks: null,
        maxRecords: null,
        partRecordLimit: PART_RECORD_LIMIT,
      });
    }
  }

  const uploadedParts = await uploadLocalOutput(s3Client, localOutputDir, index);
  return {
    cordataIndex: index,
    summary,
    uploadedPartCount: uploadedParts.length,
  };
}

/**
 * @returns {Promise<void>}
 */
async function main() {
  const runOptions = parseRunOptions();
  const s3Client = new S3Client({ maxAttempts: 10 });
  const workRoot = path.join(
    path.dirname(fileURLToPath(import.meta.url)),
    "..",
    ".loader-runs",
    "miami-dade-sunbiz-per-cordata",
  );
  await mkdir(workRoot, { recursive: true });

  /** @type {Array<Record<string, unknown>>} */
  const perFileSummaries = [];
  for (let index = runOptions.startIndex; index <= runOptions.endIndex; index += 1) {
    console.log(
      JSON.stringify({
        level: "info",
        message: "miami_dade_sunbiz_transform_cordata_started",
        cordataIndex: index,
        uploadOnly: runOptions.uploadOnly,
      }),
    );
    const result = await transformCordataFile(
      s3Client,
      index,
      workRoot,
      runOptions.uploadOnly,
    );
    perFileSummaries.push(result);
    console.log(
      JSON.stringify({
        level: "info",
        message: "miami_dade_sunbiz_transform_cordata_finished",
        cordataIndex: index,
        counters: result.summary?.counters ?? null,
        uploadedPartCount: result.uploadedPartCount,
      }),
    );
  }

  if (runOptions.startIndex === 0 && runOptions.endIndex === 9) {
    const combinedSummary = {
      schemaVersion: "oracle-node.sunbiz-lexicon-transform.v1",
      sourceJobId: JOB_ID,
      sourceManifestS3Uri: `s3://${BUCKET}/permit-harvest/${JOB_ID}/sunbiz/corporate-by-zip/manifest.json`,
      transformedAt: new Date().toISOString(),
      perFileSummaries,
      counters: perFileSummaries.reduce(
        (accumulator, entry) => {
          /** @type {{ summary?: { counters?: Record<string, number> } }} */
          const wrapped = entry;
          const counters = wrapped.summary?.counters;
          if (!counters || typeof counters !== "object") return accumulator;
          for (const [key, value] of Object.entries(counters)) {
            if (typeof value === "number") {
              accumulator[key] = (accumulator[key] ?? 0) + value;
            }
          }
          return accumulator;
        },
        /** @type {Record<string, number>} */ ({}),
      ),
    };

    const summaryKey = `${OUTPUT_S3_PREFIX}/summary.json`;
    await s3Client.send(
      new PutObjectCommand({
        Bucket: BUCKET,
        Key: summaryKey,
        Body: JSON.stringify(combinedSummary, null, 2),
        ContentType: "application/json",
      }),
    );

    console.log(
      JSON.stringify({
        level: "info",
        message: "miami_dade_sunbiz_transform_complete",
        summaryS3Uri: `s3://${BUCKET}/${summaryKey}`,
        counters: combinedSummary.counters,
      }),
    );
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(JSON.stringify({ level: "error", message }));
    process.exit(1);
  });
}
