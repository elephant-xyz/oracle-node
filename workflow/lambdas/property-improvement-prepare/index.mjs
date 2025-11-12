import {
  GetObjectCommand,
  S3Client,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { promises as fs } from "fs";
import path from "path";
import { prepare } from "@elephant-xyz/cli/lib";

const RE_S3PATH = /^s3:\/\/([^\/]+)\/(.*)$/i;

/**
 * @typedef {object} PropertyImprovementPrepareInput
 * @property {string} input_csv_s3_uri - S3 URI of the input CSV file
 * @property {string} output_s3_uri_prefix - S3 URI prefix for output files
 * @property {string} county_name - County name for flow file lookup
 * @property {boolean} use_browser - Whether to use browser mode
 *
 * @typedef {object} PropertyImprovementPrepareOutput
 * @property {string} output_s3_uri - S3 URI of the prepared output zip
 */

/**
 * Splits an Amazon S3 URI into its bucket name and object key.
 *
 * @param {string} s3Uri - A valid S3 URI in the format `s3://<bucket>/<key>`.
 * @returns {{ bucket: string, key: string }} An object containing bucket and key.
 * @throws {Error} If the input is not a valid S3 URI.
 */
const splitS3Uri = (s3Uri) => {
  const match = RE_S3PATH.exec(s3Uri);
  if (!match) {
    throw new Error("S3 path should be like: s3://bucket/object");
  }
  const [, bucket, key] = match;
  if (!bucket || !key) {
    throw new Error("S3 path should be like: s3://bucket/object");
  }
  return { bucket, key };
};

/**
 * Lambda handler for property improvement prepare step.
 * Downloads input CSV, runs prepare with browser and flow file, uploads output.
 *
 * @param {PropertyImprovementPrepareInput} event - Input event
 * @returns {Promise<PropertyImprovementPrepareOutput>} Output with S3 URI
 */
export const handler = async (event) => {
  const startTime = Date.now();
  console.log("Property Improvement Prepare Event:", event);
  console.log(`🚀 Lambda handler started at: ${new Date().toISOString()}`);

  if (!event || !event.input_csv_s3_uri) {
    throw new Error("Missing required field: input_csv_s3_uri");
  }

  const { bucket, key } = splitS3Uri(event.input_csv_s3_uri);
  console.log("Input Bucket:", bucket);
  console.log("Input Key:", key);

  const s3 = new S3Client({});
  const tempDir = await fs.mkdtemp("/tmp/property-improvement-prepare-");

  try {
    // S3 Download Phase
    console.log("📥 Starting S3 download of input CSV...");
    const s3DownloadStart = Date.now();

    // Always save as input.csv regardless of original filename
    const inputCsv = path.join(tempDir, "input.csv");
    const getResp = await s3.send(
      new GetObjectCommand({ Bucket: bucket, Key: key }),
    );
    const inputBytes = await getResp.Body?.transformToByteArray();
    if (!inputBytes) {
      throw new Error("Failed to download input CSV body");
    }
    await fs.writeFile(inputCsv, Buffer.from(inputBytes));
    console.log(`✓ Downloaded and saved as input.csv (original: ${path.basename(key)})`);

    // Create input.zip containing input.csv
    // The @elephant-xyz/cli library ALWAYS expects a zip, even with inputCsv option
    const AdmZip = (await import("adm-zip")).default;
    const inputZip = path.join(tempDir, "input.zip");
    const zip = new AdmZip();
    zip.addLocalFile(inputCsv);
    zip.writeZip(inputZip);
    console.log(`✓ Created input.zip containing input.csv`);

    const s3DownloadDuration = Date.now() - s3DownloadStart;
    console.log(
      `✅ S3 download completed: ${s3DownloadDuration}ms (${(s3DownloadDuration / 1000).toFixed(2)}s)`,
    );
    console.log(
      `📊 Downloaded ${inputBytes.length} bytes from s3://${bucket}/${key}`,
    );

    const outputZip = path.join(tempDir, "output-prepare.zip");

    // Download browser flow file from S3 if available
    const environmentBucket = process.env.ENVIRONMENT_BUCKET;
    const countyName = event.county_name || "Lee";
    let browserFlowFile = null;

    if (environmentBucket && countyName) {
      const flowKey = `browser-flows/${countyName.toLowerCase().replace(/ /g, "-")}-county-browser-flow.json`;

      try {
        console.log(
          `Checking for browser flow file: s3://${environmentBucket}/${flowKey}`,
        );

        const flowResponse = await s3.send(
          new GetObjectCommand({
            Bucket: environmentBucket,
            Key: flowKey,
          }),
        );

        const flowBytes = await flowResponse.Body?.transformToByteArray();
        if (!flowBytes) {
          throw new Error("Failed to download browser flow file body");
        }

        browserFlowFile = path.join(tempDir, "browser-flow.json");
        await fs.writeFile(browserFlowFile, Buffer.from(flowBytes));
        console.log(
          `✓ Browser flow file downloaded for county ${countyName}: ${browserFlowFile}`,
        );
      } catch (downloadError) {
        if (
          downloadError instanceof Error &&
          downloadError.name === "NoSuchKey"
        ) {
          console.log(
            `No browser flow file found for county ${countyName}, continuing without it`,
          );
        } else {
          console.error(
            `Failed to download browser flow file for county ${countyName}: ${downloadError instanceof Error ? downloadError.message : String(downloadError)}`,
          );
        }
      }
    }

    // Build prepare options for property improvement
    // Pass inputCsv in the options to tell prepare to use CSV mode
    /** @type {{ inputCsv?: string, useBrowser?: boolean, browserFlowFile?: string }} */
    const prepareOptions = {
      inputCsv: inputCsv, // Tell prepare to use CSV mode with this file
    };

    // Add browser mode if requested
    if (event.use_browser) {
      prepareOptions.useBrowser = true;
      console.log("✓ Browser mode enabled");
    }

    // Add browser flow file if downloaded
    if (browserFlowFile) {
      prepareOptions.browserFlowFile = browserFlowFile;
      console.log(`✓ Browser flow file set: ${browserFlowFile}`);
    }

    // Prepare Phase
    console.log("🔄 Starting prepare() function for property improvement...");
    console.log(`Input CSV: ${inputCsv}`);
    console.log(`Output ZIP: ${outputZip}`);
    const prepareStart = Date.now();
    console.log(
      "Calling prepare() with these options:",
      JSON.stringify(prepareOptions, null, 2),
    );

    let prepareDuration;
    try {
      // Verify the input CSV exists before calling prepare
      const inputStats = await fs.stat(inputCsv);
      console.log(`✓ Input CSV exists: ${inputCsv} (${inputStats.size} bytes)`);
      
      // Call prepare with 3 arguments but pass inputCsv in options to trigger CSV mode
      // The first argument (inputZip) will be ignored when inputCsv is in options
      console.log(`Calling: prepare("${inputZip}", "${outputZip}", ${JSON.stringify(prepareOptions)})`);
      const result = await prepare(inputZip, outputZip, prepareOptions);
      console.log("Prepare result:", JSON.stringify(result, null, 2));
      prepareDuration = Date.now() - prepareStart;
      console.log(
        `✅ Prepare function completed: ${prepareDuration}ms (${(prepareDuration / 1000).toFixed(2)}s)`,
      );
    } catch (prepareError) {
      prepareDuration = Date.now() - prepareStart;
      console.error(
        `❌ Prepare function failed after ${prepareDuration}ms: ${prepareError instanceof Error ? prepareError.message : String(prepareError)}`,
      );
      throw prepareError;
    }

    // Check output file size
    const outputStats = await fs.stat(outputZip);
    console.log(`📊 Output file size: ${outputStats.size} bytes`);

    // Determine upload destination
    let outBucket = bucket;
    let outKey = key;
    if (event.output_s3_uri_prefix) {
      const { bucket: outB, key: outPrefix } = splitS3Uri(
        event.output_s3_uri_prefix,
      );
      outBucket = outB;
      outKey = path.posix.join(
        outPrefix.replace(/\/$/, ""),
        "output-prepare.zip",
      );
    } else {
      // Default: write next to input with a suffix
      const dir = path.posix.dirname(key);
      const base = path.posix.basename(key, path.extname(key));
      outKey = path.posix.join(dir, `${base}.prepared.zip`);
    }

    // S3 Upload Phase
    console.log("📤 Starting S3 upload...");
    const s3UploadStart = Date.now();

    const outputBody = await fs.readFile(outputZip);
    await s3.send(
      new PutObjectCommand({
        Bucket: outBucket,
        Key: outKey,
        Body: outputBody,
      }),
    );

    const s3UploadDuration = Date.now() - s3UploadStart;
    console.log(
      `✅ S3 upload completed: ${s3UploadDuration}ms (${(s3UploadDuration / 1000).toFixed(2)}s)`,
    );
    console.log(
      `📊 Uploaded ${outputBody.length} bytes to s3://${outBucket}/${outKey}`,
    );

    // Total timing summary
    const totalDuration = Date.now() - startTime;
    console.log(`\n🎯 TIMING SUMMARY:`);
    console.log(
      `   S3 Download: ${s3DownloadDuration}ms (${(s3DownloadDuration / 1000).toFixed(2)}s)`,
    );
    console.log(
      `   Prepare:     ${prepareDuration}ms (${(prepareDuration / 1000).toFixed(2)}s)`,
    );
    console.log(
      `   S3 Upload:   ${s3UploadDuration}ms (${(s3UploadDuration / 1000).toFixed(2)}s)`,
    );
    console.log(
      `   TOTAL:       ${totalDuration}ms (${(totalDuration / 1000).toFixed(2)}s)`,
    );
    console.log(
      `🏁 Lambda handler completed at: ${new Date().toISOString()}\n`,
    );

    return { output_s3_uri: `s3://${outBucket}/${outKey}` };
  } catch (lambdaError) {
    const totalDuration = Date.now() - startTime;
    console.error(
      `❌ Lambda execution failed after ${totalDuration}ms: ${lambdaError instanceof Error ? lambdaError.message : String(lambdaError)}`,
    );
    throw lambdaError;
  } finally {
    try {
      await fs.rm(tempDir, { recursive: true, force: true });
      console.log(`🧹 Cleaned up temporary directory: ${tempDir}`);
    } catch (cleanupError) {
      console.error(
        `⚠️ Failed to cleanup temp directory ${tempDir}: ${cleanupError instanceof Error ? cleanupError.message : String(cleanupError)}`,
      );
    }
  }
};


