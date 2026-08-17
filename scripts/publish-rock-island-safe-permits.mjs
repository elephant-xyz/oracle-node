#!/usr/bin/env node

/**
 * Publish the validated combined Rock Island + Moline safe permit query
 * artifacts to a dedicated Filebase bucket and stable permit-only IPNS name.
 *
 * The IPNS pointer targets the Parquet CID for direct DuckDB/httpfs use. The
 * schema, manifest, coverage, and privacy scan remain independently CID-addressed
 * in the same dedicated bucket and are verified byte-for-byte after upload.
 */

import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { readFile, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { createRequire } from "node:module";

import {
  CreateBucketCommand,
  HeadBucketCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";

const require = createRequire(import.meta.url);
const ipfsHash = require("ipfs-only-hash");

export const PERMIT_BUCKET = "elephant-oracle-permits-rock-island";
export const PERMIT_IPNS_LABEL = "oracle-permit-query-rock-island";
export const DATASET_VERSION = "2026-08-14";
const DATASET_ID = "rock-island-county-supported-issued-permit-query";
const EXPECTED_ROW_COUNT = 47_385;
const EXPECTED_SCHEMA_VERSION = "1.1.0";

const FILEBASE_NAMES_API = "https://api.filebase.io/v1/names";
const FILEBASE_GATEWAY = "https://ipfs.filebase.io";
const RESERVED_BUCKETS = new Set([
  "elephant-oracle-open-data-rock-island",
  "elephant-oracle-corporate-registration-rock-island",
]);
const RESERVED_LABELS = new Set([
  "oracle-open-data-rock-island",
  "oracle-geo-index-rock-island",
  "oracle-query-table-rock-island",
  "oracle-corporate-registration-rock-island",
  "oracle-permit-table-rock-island",
]);

/**
 * @typedef {object} UploadArtifact
 * @property {string} name Logical artifact name.
 * @property {string} fileName Local file name.
 * @property {string} key Dedicated bucket object key.
 * @property {string} contentType HTTP content type.
 * @property {boolean} ipnsTarget Whether IPNS points to this artifact.
 */

/**
 * @typedef {object} PublishedArtifact
 * @property {string} name Logical artifact name.
 * @property {string} fileName Local file name.
 * @property {string} key Filebase object key.
 * @property {number} sizeBytes Object size.
 * @property {string} sha256 SHA-256 digest.
 * @property {string} cid Verified IPFS CID.
 */

/**
 * @typedef {object} FilebaseName
 * @property {string} label IPNS label.
 * @property {string} network_key Resolvable IPNS name.
 * @property {string} cid Current CID.
 * @property {number} sequence Current sequence.
 */

/**
 * @typedef {object} RawHttpResponse
 * @property {Record<string, string>} headers Response headers.
 */

/**
 * Build the complete immutable upload plan.
 *
 * @returns {UploadArtifact[]} Versioned safe artifacts.
 */
export function buildSafePermitUploadPlan() {
  const prefix = `versions/${DATASET_VERSION}`;
  return [
    {
      name: "parquet",
      fileName: "permit-query.parquet",
      key: `${prefix}/permit-query.parquet`,
      contentType: "application/vnd.apache.parquet",
      ipnsTarget: true,
    },
    {
      name: "schema",
      fileName: "schema.json",
      key: `${prefix}/schema.json`,
      contentType: "application/schema+json",
      ipnsTarget: false,
    },
    {
      name: "manifest",
      fileName: "manifest.json",
      key: `${prefix}/manifest.json`,
      contentType: "application/json",
      ipnsTarget: false,
    },
    {
      name: "coverage",
      fileName: "coverage.json",
      key: `${prefix}/coverage.json`,
      contentType: "application/json",
      ipnsTarget: false,
    },
    {
      name: "privacyScan",
      fileName: "privacy-scan.json",
      key: `${prefix}/privacy-scan.json`,
      contentType: "application/json",
      ipnsTarget: false,
    },
  ];
}

/**
 * Refuse any target that could overwrite an existing publication.
 *
 * @param {string} bucket Filebase bucket.
 * @param {string} label IPNS label.
 * @returns {void}
 */
export function assertDedicatedPublicationTarget(bucket, label) {
  if (bucket !== PERMIT_BUCKET || RESERVED_BUCKETS.has(bucket)) {
    throw new Error(
      `Refusing non-dedicated permit bucket "${bucket}"; required ${PERMIT_BUCKET}`,
    );
  }
  if (label !== PERMIT_IPNS_LABEL || RESERVED_LABELS.has(label)) {
    throw new Error(
      `Refusing non-dedicated permit IPNS label "${label}"; required ${PERMIT_IPNS_LABEL}`,
    );
  }
}

/**
 * Derive Filebase Platform API bearer auth from S3 credentials when no explicit
 * token is configured. Filebase documents this token as base64(access:secret).
 *
 * @param {Record<string, string | undefined>} env Environment map.
 * @returns {string} Platform API bearer token.
 */
export function deriveFilebaseApiToken(env) {
  const explicit = env.FILEBASE_API_TOKEN?.trim();
  if (explicit !== undefined && explicit.length > 0) return explicit;
  const access = requireEnv(env, "S3_ACCESS_KEY_ID");
  const secret = requireEnv(env, "S3_SECRET_ACCESS_KEY");
  return Buffer.from(`${access}:${secret}`, "utf8").toString("base64");
}

/**
 * Extract CID roots from an IPFS gateway header.
 *
 * @param {string | null} header x-ipfs-roots header.
 * @returns {string[]} Parsed CIDs.
 */
export function parseIpfsRoots(header) {
  if (header === null) return [];
  return header
    .split(",")
    .map((entry) => entry.trim().replace(/^\/ipfs\//, ""))
    .filter((entry) => entry.length > 0);
}

/**
 * Read a required environment value.
 *
 * @param {Record<string, string | undefined>} env Environment map.
 * @param {string} name Variable name.
 * @returns {string} Trimmed value.
 */
function requireEnv(env, name) {
  const value = env[name]?.trim();
  if (value === undefined || value.length === 0) {
    throw new Error(
      `Required publication environment variable ${name} is missing`,
    );
  }
  return value;
}

/**
 * Load simple KEY=VALUE environment files without printing secrets.
 *
 * @param {string} envFile Environment file path.
 * @returns {void}
 */
function loadEnvFile(envFile) {
  const text = readFileSync(envFile, "utf8");
  for (const line of text.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (trimmed.length === 0 || trimmed.startsWith("#")) continue;
    const separator = trimmed.indexOf("=");
    if (separator <= 0) continue;
    const key = trimmed.slice(0, separator);
    let value = trimmed.slice(separator + 1).trim();
    if (
      value.length >= 2 &&
      ((value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'")))
    ) {
      value = value.slice(1, -1);
    }
    process.env[key] ??= value;
  }
}

/**
 * Test whether an unknown response is the Smithy raw HTTP response shape.
 *
 * @param {unknown} value Candidate response.
 * @returns {value is RawHttpResponse} Whether headers are available.
 */
function isRawHttpResponse(value) {
  return (
    typeof value === "object" &&
    value !== null &&
    "headers" in value &&
    typeof value.headers === "object" &&
    value.headers !== null
  );
}

/**
 * Ensure the exact dedicated Filebase bucket exists.
 *
 * @param {S3Client} client Filebase S3 client.
 * @param {string} bucket Dedicated bucket.
 * @returns {Promise<"existing"|"created">} Bucket result.
 */
async function ensureBucket(client, bucket) {
  try {
    await client.send(new HeadBucketCommand({ Bucket: bucket }));
    return "existing";
  } catch (caught) {
    const status =
      typeof caught === "object" &&
      caught !== null &&
      "$metadata" in caught &&
      typeof caught.$metadata === "object" &&
      caught.$metadata !== null &&
      "httpStatusCode" in caught.$metadata
        ? caught.$metadata.httpStatusCode
        : undefined;
    if (status !== 404 && status !== 403) throw caught;
  }
  await client.send(new CreateBucketCommand({ Bucket: bucket }));
  await client.send(new HeadBucketCommand({ Bucket: bucket }));
  return "created";
}

/**
 * Upload one artifact and capture Filebase's CID response header on the command
 * middleware stack so concurrent/shared-client calls cannot cross-contaminate.
 *
 * @param {S3Client} client Filebase S3 client.
 * @param {string} bucket Dedicated bucket.
 * @param {UploadArtifact} artifact Upload plan entry.
 * @param {Buffer} body Artifact bytes.
 * @returns {Promise<string>} Filebase CID header.
 */
async function uploadArtifact(client, bucket, artifact, body) {
  const command = new PutObjectCommand({
    Bucket: bucket,
    Key: artifact.key,
    Body: body,
    ContentType: artifact.contentType,
    Metadata: {
      dataset: DATASET_ID,
      version: DATASET_VERSION,
    },
  });
  let responseHeaders;
  command.middlewareStack.add(
    (next) => async (args) => {
      const result = await next(args);
      if (isRawHttpResponse(result.response)) {
        responseHeaders = result.response.headers;
      }
      return result;
    },
    {
      step: "deserialize",
      name: `captureFilebaseCid-${artifact.name}`,
      priority: "low",
    },
  );
  await client.send(command);
  const cid = responseHeaders?.["x-amz-meta-cid"];
  if (typeof cid !== "string" || cid.trim().length === 0) {
    throw new Error(`Filebase returned no CID for ${artifact.key}`);
  }
  return cid.trim();
}

/**
 * Build authorization headers for Filebase Platform API.
 *
 * @param {string} token Bearer token.
 * @returns {Record<string, string>} Request headers.
 */
function platformHeaders(token) {
  return {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  };
}

/**
 * List Filebase IPNS names.
 *
 * @param {string} token Platform API token.
 * @returns {Promise<FilebaseName[]>} Current names.
 */
async function listNames(token) {
  const response = await fetch(FILEBASE_NAMES_API, {
    headers: platformHeaders(token),
  });
  if (!response.ok) {
    throw new Error(`Filebase name list failed: ${response.status}`);
  }
  const parsed = await response.json();
  if (!Array.isArray(parsed))
    throw new Error("Filebase name list is not an array");
  return parsed;
}

/**
 * Create or update the dedicated IPNS label.
 *
 * @param {string} token Platform API token.
 * @param {string} label Dedicated label.
 * @param {string} cid Parquet CID.
 * @returns {Promise<FilebaseName>} Updated name.
 */
async function upsertName(token, label, cid) {
  const names = await listNames(token);
  const existing = names.find((entry) => entry?.label === label);
  const response =
    existing === undefined
      ? await fetch(FILEBASE_NAMES_API, {
          method: "POST",
          headers: platformHeaders(token),
          body: JSON.stringify({ label, cid, enabled: true }),
        })
      : await fetch(`${FILEBASE_NAMES_API}/${encodeURIComponent(label)}`, {
          method: "PUT",
          headers: platformHeaders(token),
          body: JSON.stringify({ cid }),
        });
  if (!response.ok) {
    throw new Error(`Filebase IPNS upsert failed: ${response.status}`);
  }
  return await response.json();
}

/**
 * Fetch CID bytes from Filebase and require exact local equality.
 *
 * @param {string} cid Expected CID.
 * @param {Buffer} localBody Local artifact bytes.
 * @returns {Promise<void>}
 */
async function verifyCidBytes(cid, localBody) {
  const response = await fetch(`${FILEBASE_GATEWAY}/ipfs/${cid}`);
  if (!response.ok)
    throw new Error(`CID fetch failed for ${cid}: ${response.status}`);
  const remote = Buffer.from(await response.arrayBuffer());
  if (!remote.equals(localBody)) {
    throw new Error(`Filebase CID bytes differ from local artifact ${cid}`);
  }
}

/**
 * Wait for IPNS to resolve to the expected Parquet CID and bytes.
 *
 * @param {string} name Resolvable network key.
 * @param {string} expectedCid Parquet CID.
 * @param {Buffer} parquetBody Local Parquet bytes.
 * @returns {Promise<void>}
 */
async function verifyIpns(name, expectedCid, parquetBody) {
  const url = `${FILEBASE_GATEWAY}/ipns/${name}`;
  for (let attempt = 1; attempt <= 90; attempt += 1) {
    const head = await fetch(url, { method: "HEAD", redirect: "follow" });
    const roots = parseIpfsRoots(head.headers.get("x-ipfs-roots"));
    if (head.ok && roots.includes(expectedCid)) {
      const bodyResponse = await fetch(url);
      if (bodyResponse.ok) {
        const remote = Buffer.from(await bodyResponse.arrayBuffer());
        if (remote.equals(parquetBody)) return;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 5_000));
  }
  throw new Error(`IPNS ${name} did not resolve to ${expectedCid}`);
}

/**
 * SHA-256 digest.
 *
 * @param {Buffer} body Artifact bytes.
 * @returns {string} Hex digest.
 */
function sha256(body) {
  return createHash("sha256").update(body).digest("hex");
}

/**
 * Parse CLI options.
 *
 * @param {readonly string[]} argv Arguments after the script path.
 * @returns {{root:string,envFile:string,receipt:string}} Parsed options.
 */
function parseCli(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      root: { type: "string" },
      "env-file": { type: "string" },
      receipt: { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });
  const root =
    values.root ??
    "downloads/rock-island/permit-harvest/city-rock-island-2026-08-14/public-permit-query/v1";
  return {
    root,
    envFile: values["env-file"] ?? "",
    receipt: values.receipt ?? path.join(root, "publication-receipt.json"),
  };
}

/**
 * Publish and verify all safe artifacts.
 *
 * @returns {Promise<void>}
 */
async function main() {
  const options = parseCli(process.argv.slice(2));
  if (options.envFile.length > 0) loadEnvFile(options.envFile);
  const env = process.env;
  const endpoint = env.S3_ENDPOINT?.trim() || "https://s3.filebase.io";
  const bucket = env.S3_BUCKET?.trim() || PERMIT_BUCKET;
  const label = env.FILEBASE_PERMIT_IPNS_LABEL?.trim() || PERMIT_IPNS_LABEL;
  assertDedicatedPublicationTarget(bucket, label);
  const accessKeyId = requireEnv(env, "S3_ACCESS_KEY_ID");
  const secretAccessKey = requireEnv(env, "S3_SECRET_ACCESS_KEY");
  const apiToken = deriveFilebaseApiToken(env);
  const client = new S3Client({
    endpoint,
    region: "us-east-1",
    forcePathStyle: true,
    credentials: { accessKeyId, secretAccessKey },
  });
  const bucketStatus = await ensureBucket(client, bucket);
  const published = [];
  let parquetBody;
  let parquetCid;
  for (const artifact of buildSafePermitUploadPlan()) {
    const body = await readFile(path.join(options.root, artifact.fileName));
    const localCid = await ipfsHash.of(body);
    const filebaseCid = await uploadArtifact(client, bucket, artifact, body);
    if (localCid !== filebaseCid) {
      throw new Error(
        `CID mismatch for ${artifact.fileName}: local=${localCid} filebase=${filebaseCid}`,
      );
    }
    const head = await client.send(
      new HeadObjectCommand({ Bucket: bucket, Key: artifact.key }),
    );
    if (head.ContentLength !== body.byteLength) {
      throw new Error(`Filebase size mismatch for ${artifact.key}`);
    }
    await verifyCidBytes(localCid, body);
    published.push({
      name: artifact.name,
      fileName: artifact.fileName,
      key: artifact.key,
      sizeBytes: body.byteLength,
      sha256: sha256(body),
      cid: localCid,
    });
    if (artifact.ipnsTarget) {
      parquetBody = body;
      parquetCid = localCid;
    }
  }
  if (parquetBody === undefined || parquetCid === undefined) {
    throw new Error("Upload plan has no Parquet IPNS target");
  }
  const name = await upsertName(apiToken, label, parquetCid);
  if (name.cid !== parquetCid) {
    throw new Error(`Filebase IPNS API returned stale CID ${name.cid}`);
  }
  await verifyIpns(name.network_key, parquetCid, parquetBody);

  const manifestArtifact = published.find(
    (artifact) => artifact.name === "manifest",
  );
  if (manifestArtifact === undefined)
    throw new Error("Manifest was not published");
  const remoteManifestResponse = await fetch(
    `${FILEBASE_GATEWAY}/ipfs/${manifestArtifact.cid}`,
  );
  if (!remoteManifestResponse.ok)
    throw new Error("Published manifest is unavailable");
  const remoteManifest = await remoteManifestResponse.json();
  if (
    remoteManifest?.rowCount !== EXPECTED_ROW_COUNT ||
    remoteManifest?.schemaVersion !== EXPECTED_SCHEMA_VERSION ||
    remoteManifest?.propertyLinksPublished !== 0
  ) {
    throw new Error("Published manifest reconciliation failed");
  }

  const receipt = {
    status: "published_and_verified",
    datasetId: DATASET_ID,
    datasetVersion: DATASET_VERSION,
    rowCount: EXPECTED_ROW_COUNT,
    bucket,
    bucketStatus,
    ipnsLabel: label,
    ipnsName: name.network_key,
    ipnsCid: parquetCid,
    gatewayUrl: `${FILEBASE_GATEWAY}/ipns/${name.network_key}`,
    artifacts: published,
    verification: {
      localCidMatchesFilebase: true,
      cidBytesMatchLocal: true,
      ipnsResolvesToParquetCid: true,
      ipnsBytesMatchLocalParquet: true,
      manifestRowCountMatches: true,
      manifestSchemaVersionMatches: true,
      propertyLinksPublished: 0,
    },
    priorPermitPublicationReplaced: true,
    nonPermitPublicationsModified: false,
  };
  await writeFile(options.receipt, `${JSON.stringify(receipt, null, 2)}\n`, {
    mode: 0o600,
  });
  console.log(
    JSON.stringify({
      event: "rock_island_combined_safe_permits_published",
      bucket,
      bucketStatus,
      ipnsLabel: label,
      ipnsName: name.network_key,
      parquetCid,
      manifestCid: manifestArtifact.cid,
      rowCount: 24_786,
      receipt: options.receipt,
    }),
  );
}

const invokedPath =
  process.argv[1] === undefined ? null : path.resolve(process.argv[1]);
if (invokedPath !== null && fileURLToPath(import.meta.url) === invokedPath) {
  main().catch((caught) => {
    const message = caught instanceof Error ? caught.message : String(caught);
    console.error(
      JSON.stringify({
        event: "rock_island_safe_permits_publish_failed",
        error: message,
      }),
    );
    process.exit(1);
  });
}
