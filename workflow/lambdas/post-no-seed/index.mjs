import { S3Client, GetObjectCommand, PutObjectCommand, CopyObjectCommand } from "@aws-sdk/client-s3";
import { promises as fs } from "fs";
import path from "path";
import os from "os";
import { parse } from "csv-parse/sync";
import { createTransformScriptsManager } from "./scripts-manager.mjs";
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore - adm-zip types may be missing in lambda env
import AdmZip from "adm-zip";

const s3 = new S3Client({});
const PERSISTENT_TRANSFORM_CACHE_DIR = path.join(os.tmpdir(), "county-transforms");
const transformScriptsManager = createTransformScriptsManager({
  s3Client: s3,
  persistentCacheRoot: PERSISTENT_TRANSFORM_CACHE_DIR,
});

/**
 * @typedef {Record<string, string>} CsvRecord
 */

/**
 * @typedef {object} PostNoSeedOutput
 * @property {string} status
 * @property {CsvRecord[]} transactionItems
 */

/**
 * @typedef {object} PrepareOutput
 * @property {string} output_s3_uri
 */

/**
 * @typedef {object} S3Object
 * @property {string} key
 */

/**
 * @typedef {object} S3Bucket
 * @property {string} name
 */

/**
 * @typedef {object} S3Event
 * @property {S3Bucket} bucket
 * @property {S3Object} object
 */

/**
 * @typedef {object} PostNoSeedInput
 * @property {PrepareOutput} prepare
 * @property {S3Event} s3
 * @property {string} [seed_output_s3_uri] optional seed output for county detection only
 */

/**
 * Parse an S3 URI into bucket and key
 * @param {string} uri
 * @returns {{ bucket: string, key: string }}
 */
function parseS3Uri(uri) {
  const match = /^s3:\/\/([^/]+)\/(.*)$/.exec(uri);
  if (!match) throw new Error("Bad S3 URI");
  const bucket = match[1];
  const key = match[2];
  if (typeof bucket !== "string" || typeof key !== "string") {
    throw new Error("S3 URI must be a string");
  }
  return { bucket, key };
}

/**
 * Download an S3 object to a local path
 * @param {{ bucket: string, key: string }} location
 * @param {string} destinationPath
 * @returns {Promise<void>}
 */
async function downloadS3Object({ bucket, key }, destinationPath) {
  const result = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const bytes = await result.Body?.transformToByteArray();
  if (!bytes) throw new Error(`Failed to download ${key} from ${bucket}`);
  await fs.writeFile(destinationPath, Buffer.from(bytes));
}

/**
 * Resolve transform location from prefix and county
 * @param {{ countyName: string, transformPrefixUri: string | undefined }} params
 * @returns {{ transformBucket: string, transformKey: string }}
 */
function resolveTransformLocation({ countyName, transformPrefixUri }) {
  const prefixUri = transformPrefixUri;
  if (!prefixUri) {
    throw new Error("TRANSFORM_S3_PREFIX is required");
  }
  const { bucket, key } = parseS3Uri(prefixUri);
  const normalizedPrefix = key.replace(/\/$/, "");
  const transformKey = `${normalizedPrefix}/${countyName.toLowerCase()}.zip`;
  return { transformBucket: bucket, transformKey };
}

/**
 * Derive submission S3 destination from base and input key
 * @param {{ outputBaseUri: string, inputS3ObjectKey: string | undefined }} params
 * @returns {{ Bucket: string, Key: string }}
 */
function buildSubmissionLocation({ outputBaseUri, inputS3ObjectKey }) {
  const normalizedBase = outputBaseUri.replace(/\/$/, "");
  const inputKey = path.posix.basename(inputS3ObjectKey || "input.csv");
  const submissionUri = `${normalizedBase}/${inputKey}/submission_ready.csv`;
  const { bucket, key } = parseS3Uri(submissionUri);
  return { Bucket: bucket, Key: key };
}

/**
 * Extract county from seed zip (preferred) or prepared zip (fallback)
 * @param {{ seedZipPath?: string, preparedZipPath: string }} params
 * @returns {Promise<string>}
 */
async function extractCountyNameFlexible({ seedZipPath, preparedZipPath }) {
  // Try seed zip first (data/unnormalized_address.json)
  if (seedZipPath) {
    try {
      const seedDir = path.join(path.dirname(seedZipPath), "seed_zip_extract");
      await fs.mkdir(seedDir, { recursive: true });
      const seedZip = new AdmZip(await fs.readFile(seedZipPath));
      seedZip.extractAllTo(seedDir, true);
      const countyMetadataPath = path.join(seedDir, "data", "unnormalized_address.json");
      const countyMetadataRaw = await fs.readFile(countyMetadataPath, "utf8");
      /** @type {{ county_jurisdiction?: string }} */
      const countyMetadata = JSON.parse(countyMetadataRaw);
      if (countyMetadata.county_jurisdiction) {
        return countyMetadata.county_jurisdiction;
      }
    } catch {}
  }
  // Fallback: attempt to read unnormalized_address.json directly from prepared zip root
  try {
    const zip = new AdmZip(await fs.readFile(preparedZipPath));
    const entry = zip.getEntries().find((/** @type {any} */ e) => e.entryName === "unnormalized_address.json" || e.entryName.endsWith("/unnormalized_address.json"));
    if (entry) {
      const contents = zip.readAsText(entry);
      /** @type {{ county_jurisdiction?: string }} */
      const meta = JSON.parse(contents);
      if (meta.county_jurisdiction) return meta.county_jurisdiction;
    }
  } catch {}
  throw new Error("Unable to determine county name from seed or prepared zip");
}

/**
 * Run only county transform and validation (no seed hashing/combination)
 * @param {{ countyZipLocal: string, tmpDir: string, scriptsZipPath: string, log: (level: "info"|"error"|"debug", msg: string, details?: Record<string, unknown>) => void }} params
 * @returns {Promise<{ countyOutZip: string }>} - Path to validated county output zip
 */
/**
 * Run only county transform and validation (no seed hashing/combination)
 * @param {{
 *   countyZipLocal: string,
 *   tmpDir: string,
 *   scriptsZipPath: string,
 *   log: (level: "info"|"error"|"debug", msg: string, details?: Record<string, unknown>) => void,
 *   propertyImprovement?: boolean
 * }} params
 * @returns {Promise<{ countyOutZip: string }>}
 */
async function runCountyTransformOnly(params) {
  const { countyZipLocal, tmpDir, scriptsZipPath, log, propertyImprovement = true } = params;
  const countyOut = path.join(tmpDir, "county_output.zip");

  log("info", "transform_start", { operation: "transform", no_seed: true });
  const transformStart = Date.now();
  // Ensure flag and log what we're passing
  if (propertyImprovement && process.env.ELEPHANT_PROPERTY_IMPROVEMENT !== "true") {
    process.env.ELEPHANT_PROPERTY_IMPROVEMENT = "true";
  }
  log("info", "transform_flags", {
    propertyImprovement,
    env_property_improvement: process.env.ELEPHANT_PROPERTY_IMPROVEMENT || null,
  });
  // Print a pseudo command for observability
  log("info", "transform_command", {
    cmd: `elephant-cli transform --input "${countyZipLocal}" --output "${countyOut}" --scripts "${scriptsZipPath}" --property-improvement`,
    inputZip: countyZipLocal,
    outputZip: countyOut,
    scriptsZip: scriptsZipPath,
    propertyImprovement: true,
  });
  const { transform, validate } = await import("@elephant-xyz/cli/lib");
  const transformResult = await transform({ inputZip: countyZipLocal, outputZip: countyOut, scriptsZip: scriptsZipPath, cwd: tmpDir, propertyImprovement: true });
  const transformDuration = Date.now() - transformStart;
  if (!transformResult.success) {
    log("error", "transform_failed", { operation: "transform", error: transformResult.error, duration_ms: transformDuration });
    throw new Error(transformResult.error);
  }
  log("info", "transform_complete", { operation: "transform", duration_ms: transformDuration });

  log("info", "validation_start", { operation: "validation" });
  const validationStart = Date.now();
  const validationResult = await validate({ input: countyOut, cwd: tmpDir });
  const validationDuration = Date.now() - validationStart;
  log("info", "validation_complete", { operation: "validation", duration_ms: validationDuration });
  if (!validationResult.success) {
    const validationError = new Error(validationResult.error);
    validationError.name = "ValidationFailure";
    throw validationError;
  }
  return { countyOutZip: countyOut };
}

/**
 * Upload county hash and submission CSV
 * @param {{ countyHashZip: string, countyHashCsv: string, tmpDir: string, submissionS3Location: { Bucket: string, Key: string }, log: (level: "info"|"error"|"debug", msg: string, details?: Record<string, unknown>) => void }} params
 * @returns {Promise<void>}
 */
async function uploadOutputsNoSeed({ countyHashZip, countyHashCsv, tmpDir, submissionS3Location, log }) {
  const { upload } = await import("@elephant-xyz/cli/lib");
  log("info", "ipfs_upload_start", { operation: "ipfs_upload", files: [path.basename(countyHashZip)] });
  const uploadStart = Date.now();
  const uploadResult = await upload({ input: countyHashZip, pinataJwt: process.env.ELEPHANT_PINATA_JWT, cwd: tmpDir });
  if (!uploadResult.success) {
    log("error", "upload_failed", { step: "upload", error: uploadResult.error });
    throw new Error(`Upload failed: ${uploadResult.error}`);
  }
  log("info", "ipfs_upload_complete", { duration_ms: Date.now() - uploadStart });

  const submissionBody = await fs.readFile(countyHashCsv);
  await s3.send(new PutObjectCommand({ ...submissionS3Location, Body: submissionBody }));
}

/**
 * @param {PostNoSeedInput} event
 * @returns {Promise<PostNoSeedOutput>}
 */
export const handler = async (event) => {
  if (!event?.prepare?.output_s3_uri) throw new Error("prepare.output_s3_uri missing");
  const outputBase = process.env.OUTPUT_BASE_URI;
  if (!outputBase) throw new Error("OUTPUT_BASE_URI is required");

  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "post-no-seed-"));
  try {
    // Log input context (what input is being processed)
    const inputBucket = event?.s3?.bucket?.name || null;
    const inputKey = event?.s3?.object?.key || null;
    const inputUri = inputBucket && inputKey ? `s3://${inputBucket}/${inputKey}` : null;
    console.log(
      JSON.stringify({
        component: "post-no-seed",
        at: new Date().toISOString(),
        level: "info",
        msg: "processing_input",
        input_bucket: inputBucket,
        input_key: inputKey,
        input_s3_uri: inputUri,
        prepared_output_s3_uri: event.prepare.output_s3_uri,
      }),
    );

    // Download prepared county archive
    const preparedZipLocal = path.join(tmp, "prepared.zip");
    await downloadS3Object(parseS3Uri(event.prepare.output_s3_uri), preparedZipLocal);

    // Skip county detection; use contractor extractor scripts regardless of county
    const countyName = "lee-permits";

    const base = { component: "post-no-seed", at: new Date().toISOString(), county: countyName };
    /**
     * @type {(level: "info"|"error"|"debug", msg: string, details?: Record<string, unknown>) => void}
     */
    const log = (level, msg, details = {}) => {
      const entry = { ...base, level, msg, ...details };
      const serialized = JSON.stringify(entry);
      if (level === "error") console.error(serialized); else console.log(serialized);
    };

    // Resolve transform scripts, but override with contractor extractor script bundle
    const { transformBucket, transformKey } = resolveTransformLocation({ countyName, transformPrefixUri: process.env.TRANSFORM_S3_PREFIX });
    const { scriptsZipPath: countyScriptsZip } = await transformScriptsManager.ensureScriptsForCounty({ countyName, transformBucket, transformKey, log });

    // Build a temp scripts zip that contains the contractor extractor script only
    const scriptsZipPath = path.join(tmp, "contractor-extractor-scripts.zip");
    // Write the extractor under the canonical filename expected by the CLI
    const extractorLocalPath = path.join(tmp, "property_improvement_extractor.cjs");
    const bundledPath = "/var/task/lee-permits/property_improvement_extractor.cjs";
    const content = await fs.readFile(bundledPath);
    await fs.writeFile(extractorLocalPath, content);
    const zip = new AdmZip();
    // Place at zip root as property_improvement_extractor.cjs
    zip.addLocalFile(extractorLocalPath);
    await fs.writeFile(scriptsZipPath, zip.toBuffer());

    // Run county transform only
    const { countyOutZip } = await runCountyTransformOnly({ countyZipLocal: preparedZipLocal, tmpDir: tmp, scriptsZipPath, log, propertyImprovement: true });

    // Upload transform output zip to S3 alongside submission artifacts (namespaced by input name)
    try {
      const submissionS3ForPrefix = buildSubmissionLocation({ outputBaseUri: outputBase, inputS3ObjectKey: event?.s3?.object?.key });
      const runPrefixKey = path.posix.dirname(submissionS3ForPrefix.Key);
      const inputBase = path.posix.basename(event?.s3?.object?.key || "input.csv");
      const inputStem = inputBase.replace(/\.[^/.]+$/, "");
      const transformOutKey = path.posix.join(runPrefixKey, `${inputStem}.transform_output.zip`);
      const transformBody = await fs.readFile(countyOutZip);
      await s3.send(new PutObjectCommand({ Bucket: submissionS3ForPrefix.Bucket, Key: transformOutKey, Body: transformBody }));
      log("info", "transform_output_uploaded", { bucket: submissionS3ForPrefix.Bucket, key: transformOutKey, s3_uri: `s3://${submissionS3ForPrefix.Bucket}/${transformOutKey}` });
    } catch (e) {
      log("error", "transform_output_upload_failed", { error: e instanceof Error ? e.message : String(e) });
    }

    // Also copy prepared zip into the same run folder with input-based name
    try {
      const submissionS3ForPrefix = buildSubmissionLocation({ outputBaseUri: outputBase, inputS3ObjectKey: event?.s3?.object?.key });
      const runPrefixKey = path.posix.dirname(submissionS3ForPrefix.Key);
      const inputBase = path.posix.basename(event?.s3?.object?.key || "input.csv");
      const inputStem = inputBase.replace(/\.[^/.]+$/, "");
      const preparedDestKey = path.posix.join(runPrefixKey, `${inputStem}.prepared.zip`);
      const { bucket: preparedSrcBucket, key: preparedSrcKey } = parseS3Uri(event.prepare.output_s3_uri);
      await s3.send(new CopyObjectCommand({ Bucket: submissionS3ForPrefix.Bucket, Key: preparedDestKey, CopySource: `${preparedSrcBucket}/${preparedSrcKey}` }));
      log("info", "prepared_zip_copied", { bucket: submissionS3ForPrefix.Bucket, key: preparedDestKey, s3_uri: `s3://${submissionS3ForPrefix.Bucket}/${preparedDestKey}`, from: event.prepare.output_s3_uri });
    } catch (e) {
      log("error", "prepared_zip_copy_failed", { error: e instanceof Error ? e.message : String(e) });
    }

    // Hash county output only
    const countyHashZip = path.join(tmp, "county_hash.zip");
    const countyHashCsv = path.join(tmp, "county_hash.csv");
    log("info", "hash_county_start", { operation: "county_hash", no_seed: true });
    const countyHashStart = Date.now();
    // Extract property CID from the prepared input ZIP: first CSV's 'id' column
    let propertyCidFromInput = undefined;
    try {
      const preparedZip = new AdmZip(await fs.readFile(preparedZipLocal));
      const csvEntry = preparedZip.getEntries().find((/** @type {any} */ e) => /\.csv$/i.test(e.entryName));
      if (csvEntry) {
        const csvText = preparedZip.readAsText(csvEntry);
        const rows = parse(csvText, { columns: true, skip_empty_lines: true, trim: true });
        propertyCidFromInput = rows[0]?.id;
      }
    } catch (e) {
      log("debug", "property_cid_parse_failed", { error: e instanceof Error ? e.message : String(e) });
    }
    log("info", "hash_flags", { propertyCidFromInput: propertyCidFromInput || null });
    const { hash } = await import("@elephant-xyz/cli/lib");
    const countyHashResult = await hash({ input: countyOutZip, outputZip: countyHashZip, outputCsv: countyHashCsv, cwd: tmp, propertyCid: propertyCidFromInput });
    const countyHashDuration = Date.now() - countyHashStart;
    log("info", "hash_county_complete", { duration_ms: countyHashDuration });
    if (!countyHashResult.success) {
      log("error", "hash_failed", { step: "hash", operation: "county_hash", error: countyHashResult.error });
      throw new Error(`County hash failed: ${countyHashResult.error}`);
    }

    // Parse county hash CSV as transaction items
    const combinedCsvContent = await fs.readFile(countyHashCsv, "utf8");
    const transactionItems = parse(combinedCsvContent, { columns: true, skip_empty_lines: true, trim: true });

    // Build submission location and upload
    const submissionS3 = buildSubmissionLocation({ outputBaseUri: outputBase, inputS3ObjectKey: event?.s3?.object?.key });
    await uploadOutputsNoSeed({ countyHashZip, countyHashCsv, tmpDir: tmp, submissionS3Location: submissionS3, log });

    log("info", "post_no_seed_complete", { transaction_items_count: transactionItems.length });
    return { status: "success", transactionItems };
  } finally {
    try { await fs.rm(tmp, { recursive: true, force: true }); } catch {}
  }
};


