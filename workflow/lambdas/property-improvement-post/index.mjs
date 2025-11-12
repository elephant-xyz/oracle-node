import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { promises as fs } from "fs";
import path from "path";
import os from "os";
import { transform, validate, hash, upload } from "@elephant-xyz/cli/lib";
import { parse } from "csv-parse/sync";
import AdmZip from "adm-zip";
import { readFileSync } from "fs";

const s3 = new S3Client({});

// Log CLI version on cold start
try {
  const cliPackageJson = JSON.parse(
    readFileSync("/var/task/node_modules/@elephant-xyz/cli/package.json", "utf8")
  );
  console.log(`✅ @elephant-xyz/cli version: ${cliPackageJson.version}`);
} catch (err) {
  console.warn("⚠️ Could not read CLI version:", String(err));
}

/**
 * @typedef {object} PropertyImprovementPostInput
 * @property {string} input_csv_s3_uri - S3 URI of original input CSV
 * @property {string} prepare_output_s3_uri - S3 URI of prepare output zip
 * @property {string} output_prefix - S3 URI prefix for outputs
 * @property {string} county_name - County name for transform script lookup
 *
 * @typedef {object} PropertyImprovementPostOutput
 * @property {string} status
 * @property {object[]} transactionItems
 *
 * @typedef {object} TransformOptions
 * @property {string} dataGroup - Data group name (e.g., "Property Improvement")
 * @property {string} inputZip - Path to input zip file
 * @property {string} outputZip - Path to output zip file
 * @property {string} scriptsZip - Path to scripts zip file
 * @property {string} cwd - Working directory path
 */

/**
 * Parse an S3 URI into its bucket and key components.
 *
 * @param {string} uri - S3 URI in the form s3://bucket/key.
 * @returns {{ bucket: string, key: string }} - Parsed bucket and key.
 */
function parseS3Uri(uri) {
  const match = /^s3:\/\/([^/]+)\/(.*)$/.exec(uri);
  if (!match) {
    throw new Error("Bad S3 URI");
  }
  const bucket = match[1];
  const key = match[2];
  if (typeof bucket !== "string" || typeof key !== "string") {
    throw new Error("S3 URI must be a string");
  }
  return { bucket, key };
}

/**
 * Download an S3 object to a local file.
 *
 * @param {{ bucket: string, key: string }} location - Bucket and key location.
 * @param {string} destinationPath - Absolute path where the object will be stored.
 * @returns {Promise<void>}
 */
async function downloadS3Object({ bucket, key }, destinationPath) {
  console.log(`📥 Downloading s3://${bucket}/${key} to ${destinationPath}`);
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
 * Ensure required environment variables are present.
 *
 * @param {string} name - Environment variable identifier.
 * @returns {string} - Resolved environment variable value.
 */
function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

/**
 * Save a debug file to S3 for troubleshooting.
 *
 * @param {object} params - Upload parameters
 * @param {string} params.filePath - Local file path to upload
 * @param {string} params.s3Key - S3 key (path) where to save the file
 * @param {string} params.bucket - S3 bucket name
 * @returns {Promise<void>}
 */
async function saveDebugFileToS3({ filePath, s3Key, bucket }) {
  try {
    const fileContent = await fs.readFile(filePath);
    await s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: s3Key,
        Body: fileContent,
      })
    );
    console.log(`📤 Saved debug file to s3://${bucket}/${s3Key}`);
  } catch (error) {
    console.warn(`⚠️ Failed to save debug file to S3: ${error.message}`);
  }
}

/**
 * Merge input.csv with output-prepare.zip into a new zip file with flat structure.
 * All files should be at the root level (not in folders).
 *
 * @param {object} params - Merge parameters
 * @param {string} params.inputCsvPath - Path to input CSV file
 * @param {string} params.prepareZipPath - Path to prepare output zip
 * @param {string} params.tmpDir - Temporary directory
 * @returns {Promise<string>} - Path to merged zip file
 */
async function mergeInputCsvWithPrepare({ inputCsvPath, prepareZipPath, tmpDir }) {
  console.log("🔀 Merging input.csv with prepare output (flat structure)...");

  const mergedDir = path.join(tmpDir, "merged");
  await fs.mkdir(mergedDir, { recursive: true });

  // Extract prepare output to a temporary directory first
  const prepareExtractDir = path.join(tmpDir, "prepare_extract");
  await fs.mkdir(prepareExtractDir, { recursive: true });
  
  const prepareZip = new AdmZip(await fs.readFile(prepareZipPath));
  prepareZip.extractAllTo(prepareExtractDir, true);

  // Copy all files from prepare output to merged directory (flatten structure)
  /**
   * Recursively copy files to root level
   * @param {string} sourceDir
   */
  const flattenAndCopy = async (sourceDir) => {
    const entries = await fs.readdir(sourceDir, { withFileTypes: true });
    for (const entry of entries) {
      const sourcePath = path.join(sourceDir, entry.name);
      if (entry.isDirectory()) {
        // Recursively process subdirectories
        await flattenAndCopy(sourcePath);
      } else {
        // Copy file to root of merged directory
        const destPath = path.join(mergedDir, entry.name);
        await fs.copyFile(sourcePath, destPath);
        console.log(`  ✓ Added ${entry.name} to merged zip`);
      }
    }
  };

  await flattenAndCopy(prepareExtractDir);

  // Copy input.csv to merged directory (overwrite if it exists from prepare)
  await fs.copyFile(inputCsvPath, path.join(mergedDir, "input.csv"));
  console.log(`  ✓ Added input.csv to merged zip`);

  // Create merged zip with flat structure
  const mergedZipPath = path.join(tmpDir, "merged_input.zip");
  const mergedZip = new AdmZip();
  mergedZip.addLocalFolder(mergedDir);
  await fs.writeFile(mergedZipPath, mergedZip.toBuffer());

  console.log(`✅ Created merged zip with flat structure: ${mergedZipPath}`);
  
  // Log the contents of the merged zip for verification
  const verifyZip = new AdmZip(mergedZipPath);
  const entries = verifyZip.getEntries();
  console.log(`📦 Merged zip contains ${entries.length} files:`);
  entries.forEach(entry => {
    console.log(`   - ${entry.entryName}`);
  });

  return mergedZipPath;
}

/**
 * Run transform and validation with property-improvement.js script.
 *
 * @param {object} params - Transform parameters
 * @param {string} params.inputZipPath - Path to input zip
 * @param {string} params.countyName - County name for script lookup
 * @param {string} params.tmpDir - Temporary directory
 * @param {string} [params.requestIdentifier] - Request identifier for debug paths
 * @returns {Promise<string>} - Path to validated output zip
 */
async function runPropertyImprovementTransform({ inputZipPath, countyName, tmpDir, requestIdentifier }) {
  const outputZip = path.join(tmpDir, "transform_output.zip");

  console.log("🔄 Starting property improvement transform...");
  const transformStart = Date.now();

  // Download property improvement scripts.zip from S3
  const transformBucket = process.env.TRANSFORM_S3_BUCKET;
  const transformKey = `property-improvement/${countyName.toLowerCase().replace(/ /g, "-")}/scripts.zip`;

  if (!transformBucket) {
    throw new Error("TRANSFORM_S3_BUCKET environment variable not set");
  }

  const scriptsZipPath = path.join(tmpDir, "scripts.zip");
  try {
    await downloadS3Object(
      { bucket: transformBucket, key: transformKey },
      scriptsZipPath,
    );
    console.log(`✓ Downloaded property improvement scripts: ${transformKey}`);
  } catch (downloadError) {
    console.error(
      `Failed to download property improvement scripts: ${downloadError instanceof Error ? downloadError.message : String(downloadError)}`,
    );
    throw new Error(
      `Property improvement scripts not found for county: ${countyName}`,
    );
  }

  // Run transform with Property Improvement data group
  // dataGroup is a new parameter in @elephant-xyz/cli
  const transformOptions = {
    dataGroup: "Property Improvement",
    inputZip: inputZipPath,
    outputZip: outputZip,
    scriptsZip: scriptsZipPath,
    cwd: tmpDir,
  };
  console.log("✓ Transform options:", JSON.stringify(transformOptions, null, 2));
  console.log("✓ dataGroup value:", transformOptions.dataGroup);
  console.log("✓ dataGroup length:", transformOptions.dataGroup.length);
  console.log("✓ dataGroup charCodes:", Array.from(transformOptions.dataGroup).map(c => c.charCodeAt(0)));
  console.log("✓ Transform function type:", typeof transform);
  console.log("✓ Transform function name:", transform.name);
  const transformResult = await transform(transformOptions);
  console.log("✓ Transform result:", JSON.stringify(transformResult, null, 2));

  const transformDuration = Date.now() - transformStart;
  if (!transformResult.success) {
    console.error(
      `❌ Transform failed after ${transformDuration}ms: ${transformResult.error}`,
    );
    throw new Error(transformResult.error);
  }
  console.log(
    `✅ Transform completed in ${transformDuration}ms (${(transformDuration / 1000).toFixed(2)}s)`,
  );

  // Validate
  console.log("🔍 Starting validation...");
  const validationStart = Date.now();
  const validationResult = await validate({ input: outputZip, cwd: tmpDir });
  const validationDuration = Date.now() - validationStart;

  if (!validationResult.success) {
    console.error(
      `❌ Validation failed after ${validationDuration}ms: ${validationResult.error}`,
    );
    
    // Try to read and log submit_errors.csv if it exists
    const submitErrorsPath = path.join(tmpDir, "submit_errors.csv");
    try {
      const submitErrorsCsv = await fs.readFile(submitErrorsPath, "utf8");
      const submitErrors = parse(submitErrorsCsv, {
        columns: true,
        skip_empty_lines: true,
        trim: true,
      });
      console.error("❌ Validation errors from submit_errors.csv:", JSON.stringify(submitErrors, null, 2));
      
      // Save submit_errors.csv to S3 for debugging
      const debugBucket = process.env.TRANSFORM_S3_BUCKET;
      if (debugBucket && requestIdentifier) {
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        await saveDebugFileToS3({
          filePath: submitErrorsPath,
          s3Key: `debug/property-improvement/${countyName}/${requestIdentifier}/${timestamp}/submit_errors.csv`,
          bucket: debugBucket,
        });
      }
    } catch (csvError) {
      console.error("⚠️ Could not read submit_errors.csv:", String(csvError));
    }
    
    throw new Error(validationResult.error);
  }
  console.log(
    `✅ Validation completed in ${validationDuration}ms (${(validationDuration / 1000).toFixed(2)}s)`,
  );

  return outputZip;
}

/**
 * Extract property-cid from the input CSV.
 *
 * @param {string} csvPath - Path to CSV file
 * @returns {Promise<string | null>} - Property CID or null
 */
async function extractPropertyCid(csvPath) {
  try {
    const csvContent = await fs.readFile(csvPath, "utf8");
    const records = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    });

    console.log(`📋 CSV has ${records.length} records`);
    if (records.length > 0) {
      console.log(`📋 CSV columns:`, Object.keys(records[0]));
      
      // Try different possible column names (with space, underscore, camelCase, hyphen)
      const propertyCid = records[0]['Property CID'] || records[0].property_cid || records[0].propertyCid || records[0]['property-cid'];
      
      if (propertyCid) {
        console.log(`✅ Found property CID: ${propertyCid}`);
        return propertyCid;
      } else {
        console.warn(`⚠️ No property CID found in CSV. Available columns: ${Object.keys(records[0]).join(', ')}`);
      }
    }
    return null;
  } catch (error) {
    console.error(
      `Failed to extract property-cid from CSV: ${error instanceof Error ? error.message : String(error)}`,
    );
    return null;
  }
}

/**
 * Hash the output with property-cid support.
 *
 * @param {object} params - Hash parameters
 * @param {string} params.outputZipPath - Path to output zip
 * @param {string | null} params.propertyCid - Property CID from CSV
 * @param {string} params.tmpDir - Temporary directory
 * @returns {Promise<{ hashZip: string, hashCsv: string }>}
 */
async function hashWithPropertyCid({ outputZipPath, propertyCid, tmpDir }) {
  const hashZip = path.join(tmpDir, "hash_output.zip");
  const hashCsv = path.join(tmpDir, "hash_output.csv");

  console.log("🔐 Starting hash with property-cid...");
  const hashStart = Date.now();

  /** @type {import("@elephant-xyz/cli/lib").HashOptions} */
  const hashOptions = {
    input: outputZipPath,
    outputZip: hashZip,
    outputCsv: hashCsv,
    cwd: tmpDir,
  };

  if (propertyCid) {
    // @ts-ignore - Adding property-cid option
    hashOptions.propertyCid = propertyCid;
    console.log(`✓ Using property-cid: ${propertyCid}`);
  }

  const hashResult = await hash(hashOptions);
  const hashDuration = Date.now() - hashStart;

  if (!hashResult.success) {
    console.error(
      `❌ Hash failed after ${hashDuration}ms: ${hashResult.error}`,
    );
    throw new Error(hashResult.error);
  }
  console.log(
    `✅ Hash completed in ${hashDuration}ms (${(hashDuration / 1000).toFixed(2)}s)`,
  );

  return { hashZip, hashCsv };
}

/**
 * Upload hash output to IPFS.
 *
 * @param {object} params - Upload parameters
 * @param {string} params.hashZipPath - Path to hash zip
 * @param {string} params.tmpDir - Temporary directory
 * @param {string} params.pinataJwt - Pinata JWT
 * @returns {Promise<void>}
 */
async function uploadToIpfs({ hashZipPath, tmpDir, pinataJwt }) {
  console.log("☁️ Starting IPFS upload...");
  console.log(`🔑 Pinata JWT length: ${pinataJwt ? pinataJwt.length : 0}`);
  console.log(`🔑 Pinata JWT first 20 chars: ${pinataJwt ? pinataJwt.substring(0, 20) : 'undefined'}`);
  console.log(`🔑 Pinata JWT segments: ${pinataJwt ? pinataJwt.split('.').length : 0}`);
  const uploadStart = Date.now();

  const uploadResult = await upload({
    input: hashZipPath,
    pinataJwt,
    cwd: tmpDir,
  });

  const uploadDuration = Date.now() - uploadStart;
  if (!uploadResult.success) {
    console.error(
      `❌ Upload failed after ${uploadDuration}ms`,
    );
    console.error("Upload result:", JSON.stringify(uploadResult, null, 2));
    const errorMsg = uploadResult.errorMessage || uploadResult.error || uploadResult.errors || "Unknown upload error";
    throw new Error(`Upload failed: ${errorMsg}`);
  }
  console.log(
    `✅ Upload completed in ${uploadDuration}ms (${(uploadDuration / 1000).toFixed(2)}s)`,
  );
}

/**
 * Parse hash CSV into transaction items.
 *
 * @param {string} csvPath - Path to CSV file
 * @returns {Promise<object[]>}
 */
async function parseTransactionItems(csvPath) {
  const csvContent = await fs.readFile(csvPath, "utf8");
  const records = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });
  return records;
}

/**
 * @param {PropertyImprovementPostInput} event - Input event
 * @returns {Promise<PropertyImprovementPostOutput>}
 */
export const handler = async (event) => {
  console.log("Property Improvement Post-processor Event:", event);
  const startTime = Date.now();

  if (!event.input_csv_s3_uri) throw new Error("input_csv_s3_uri missing");
  if (!event.prepare_output_s3_uri) throw new Error("prepare_output_s3_uri missing");
  if (!event.output_prefix) throw new Error("output_prefix missing");

  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "property-improvement-post-"));

  try {
    // Download input CSV
    const inputCsvPath = path.join(tmp, "input.csv");
    await downloadS3Object(parseS3Uri(event.input_csv_s3_uri), inputCsvPath);

    // Download prepare output
    const prepareZipPath = path.join(tmp, "prepare_output.zip");
    await downloadS3Object(parseS3Uri(event.prepare_output_s3_uri), prepareZipPath);

    // Extract request_identifier from input CSV for debug path organization
    const csvContent = await fs.readFile(inputCsvPath, "utf8");
    const csvRecords = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    });
    const requestIdentifier = csvRecords[0]?.request_identifier || csvRecords[0]?.['request_identifier'] || 'unknown';
    
    // Merge input CSV with prepare output
    const mergedZipPath = await mergeInputCsvWithPrepare({
      inputCsvPath,
      prepareZipPath,
      tmpDir: tmp,
    });

    // Save merged output to S3 for debugging
    const debugBucket = requireEnv("TRANSFORM_S3_BUCKET");
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    await saveDebugFileToS3({
      filePath: mergedZipPath,
      s3Key: `debug/property-improvement/${event.county_name}/${requestIdentifier}/${timestamp}/merged-input.zip`,
      bucket: debugBucket,
    });

    // Run transform and validation
    const transformedZipPath = await runPropertyImprovementTransform({
      inputZipPath: mergedZipPath,
      countyName: event.county_name,
      tmpDir: tmp,
      requestIdentifier: requestIdentifier,
    });

    // Extract property-cid from input CSV
    const propertyCid = await extractPropertyCid(inputCsvPath);

    // Hash with property-cid
    const { hashZip, hashCsv } = await hashWithPropertyCid({
      outputZipPath: transformedZipPath,
      propertyCid,
      tmpDir: tmp,
    });

    // Save hash results to S3 for debugging
    await saveDebugFileToS3({
      filePath: hashZip,
      s3Key: `debug/property-improvement/${event.county_name}/${requestIdentifier}/${timestamp}/hash-output.zip`,
      bucket: debugBucket,
    });
    await saveDebugFileToS3({
      filePath: hashCsv,
      s3Key: `debug/property-improvement/${event.county_name}/${requestIdentifier}/${timestamp}/hash-output.csv`,
      bucket: debugBucket,
    });

    // Upload to IPFS
    await uploadToIpfs({
      hashZipPath: hashZip,
      tmpDir: tmp,
      pinataJwt: requireEnv("ELEPHANT_PINATA_JWT"),
    });

    // Parse transaction items
    const transactionItems = await parseTransactionItems(hashCsv);

    const totalDuration = Date.now() - startTime;
    console.log(
      `🏁 Property improvement post-processing completed in ${totalDuration}ms (${(totalDuration / 1000).toFixed(2)}s)`,
    );

    return { status: "success", transactionItems };
  } catch (err) {
    console.error(
      `❌ Property improvement post-processing failed: ${err instanceof Error ? err.message : String(err)}`,
    );
    throw err;
  } finally {
    try {
      if (tmp) await fs.rm(tmp, { recursive: true, force: true });
    } catch {}
  }
};

