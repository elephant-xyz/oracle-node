import {
  GetObjectCommand,
  S3Client,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { promises as fs } from "fs";
import path from "path";
import { prepare } from "@elephant-xyz/cli/lib";
import { networkInterfaces } from "os";
import AdmZip from "adm-zip";

const RE_S3PATH = /^s3:\/\/([^/]+)\/(.*)$/i;

/**
 * @typedef {Object} IPInfo
 * @property {string[]} localIPs
 * @property {string | null} publicIP
 * @property {string} awsRegion
 * @property {string} lambdaFunction
 */

/**
 * @typedef {Object} FlagConfig
 * @property {string} envVar
 * @property {"useBrowser" | "noFast" | "noContinue"} optionKey
 * @property {string} description
 */

/** @typedef {Error & { originalError?: Error, type?: string, context?: Object, execution?: Object, validationErrors?: Object }} EnhancedError */

/**
 * Gets IP address information for the Lambda instance
 * @returns {Promise<IPInfo>} Object containing local and public IP addresses
 */
const getIPAddresses = async () => {
  /** @type {IPInfo} */
  const result = {
    localIPs: [],
    publicIP: null,
    awsRegion: process.env.AWS_REGION || "unknown",
    lambdaFunction: process.env.AWS_LAMBDA_FUNCTION_NAME || "unknown",
  };

  try {
    // Get local network interfaces
    const nets = networkInterfaces();
    for (const name of Object.keys(nets)) {
      for (const net of nets[name] || []) {
        // Skip internal and non-IPv4 addresses
        if (net.family === "IPv4" && !net.internal) {
          result.localIPs.push(`${name}: ${net.address}`);
        }
      }
    }
  } catch (error) {
    console.log(
      `⚠️ Could not get local IPs: ${error instanceof Error ? error.message : String(error)}`,
    );
  }

  try {
    // Try to get public IP via external service
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 3000);

    const response = await fetch("https://api.ipify.org?format=json", {
      signal: controller.signal,
    });
    clearTimeout(timeoutId);

    if (response.ok) {
      const data = await response.json();
      if (data.ip) {
        result.publicIP = data.ip;
      }
    }
  } catch (error) {
    console.log(
      `⚠️ Could not get public IP: ${error instanceof Error ? error.message : String(error)}`,
    );

    // Fallback: try AWS metadata service for ECS/EC2 (won't work in Lambda but just in case)
    try {
      const metadataController = new AbortController();
      const metadataTimeoutId = setTimeout(
        () => metadataController.abort(),
        1000,
      );

      const metadataResponse = await fetch(
        "http://169.254.169.254/latest/meta-data/public-ipv4",
        {
          signal: metadataController.signal,
        },
      );
      clearTimeout(metadataTimeoutId);

      if (metadataResponse.ok) {
        result.publicIP = await metadataResponse.text();
      }
    } catch (metaError) {
      // Silent fail for metadata service
    }
  }

  return result;
};

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
    throw new Error("S3 path should be like: s3://bucket/object");
  }

  const [, bucket, key] = match;
  if (!bucket || !key) {
    throw new Error("S3 path should be like: s3://bucket/object");
  }
  return { bucket, key };
};

/**
 * Lambda handler for processing orders and storing receipts in S3.
 * @param {Object} event - Input event containing order details
 * @param {string} event.input_s3_uri - S3 URI of input file
 * @param {string} event.output_s3_uri_prefix - S3 URI prefix for output files
 * @returns {Promise<{ output_s3_uri: string }>} Success message
 */
export const handler = async (event) => {
  const startTime = Date.now();
  console.log("Event:", event);
  console.log(`🚀 Lambda handler started at: ${new Date().toISOString()}`);

  // Log IP address and Lambda environment information
  console.log("🌐 Getting Lambda IP address information...");
  try {
    const ipInfo = await getIPAddresses();
    console.log(`🔍 Lambda Instance Info:`);
    console.log(`   Function: ${ipInfo.lambdaFunction}`);
    console.log(
      `   Version: ${process.env.AWS_LAMBDA_FUNCTION_VERSION || "unknown"}`,
    );
    console.log(`   Region: ${ipInfo.awsRegion}`);
    console.log(
      `   Memory: ${process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE || "unknown"}MB`,
    );
    console.log(`   Runtime: ${process.env.AWS_EXECUTION_ENV || "unknown"}`);
    console.log(`   Request ID: ${process.env.AWS_REQUEST_ID || "unknown"}`);
    console.log(
      `   Local IPs: ${ipInfo.localIPs.length > 0 ? ipInfo.localIPs.join(", ") : "None found"}`,
    );
    console.log(`   Public IP: ${ipInfo.publicIP || "Not available"}`);

    // Also log some additional network diagnostics
    if (ipInfo.publicIP) {
      console.log(
        `🌍 Network: Lambda has outbound internet access via IP ${ipInfo.publicIP}`,
      );
    } else {
      console.log(
        `🚫 Network: No public IP detected (may be VPC-only or blocked)`,
      );
    }
  } catch (error) {
    console.log(
      `⚠️ Could not get IP information: ${error instanceof Error ? error.message : String(error)}`,
    );
  }

  if (!event || !event.input_s3_uri) {
    throw new Error("Missing required field: input_s3_uri");
  }
  const { bucket, key } = splitS3Uri(event.input_s3_uri);
  console.log("Bucket:", bucket);
  console.log("Key:", key);
  const s3 = new S3Client({});

  const tempDir = await fs.mkdtemp("/tmp/prepare-");
  try {
    // S3 Download Phase
    console.log("📥 Starting S3 download...");
    const s3DownloadStart = Date.now();

    const inputZip = path.join(tempDir, path.basename(key));
    const getResp = await s3.send(
      new GetObjectCommand({ Bucket: bucket, Key: key }),
    );
    const inputBytes = await getResp.Body?.transformToByteArray();
    if (!inputBytes) {
      throw new Error("Failed to download input object body");
    }
    await fs.writeFile(inputZip, Buffer.from(inputBytes));

    const s3DownloadDuration = Date.now() - s3DownloadStart;
    console.log(
      `✅ S3 download completed: ${s3DownloadDuration}ms (${(s3DownloadDuration / 1000).toFixed(2)}s)`,
    );
    console.log(
      `📊 Downloaded ${inputBytes.length} bytes from s3://${bucket}/${key}`,
    );

    // Extract county name from unnormalized_address.json
    let countyName = null;
    console.log("📍 Extracting county information from input...");

    try {
      // Use adm-zip to read the zip file directly without extracting to disk
      const zip = new AdmZip(inputZip);
      const zipEntries = zip.getEntries();

      // Find unnormalized_address.json in the zip
      const addressEntry = zipEntries.find(
        (entry) => entry.entryName === "unnormalized_address.json",
      );

      if (addressEntry) {
        // Read the file content directly from zip
        const addressContent = zip.readAsText(addressEntry);
        const addressData = JSON.parse(addressContent);

        if (addressData.county_jurisdiction) {
          countyName = addressData.county_jurisdiction;
          console.log(`✅ Detected county: ${countyName}`);
        } else {
          console.log(
            "⚠️ No county_jurisdiction found in unnormalized_address.json",
          );
        }
      } else {
        console.log("⚠️ unnormalized_address.json not found in zip file");
      }
    } catch (error) {
      console.error(
        `⚠️ Could not extract county information: ${error instanceof Error ? error.message : String(error)}`,
      );
      console.log("Continuing with general configuration...");
    }

    const outputZip = path.join(tempDir, "output.zip");

    console.log("Building prepare options...");

    // Helper function to get environment variable with county-specific fallback
    /**
     * @param {string} baseEnvVar
     * @param {string | null} countyName
     * @returns {string | undefined}
     */
    const getEnvWithCountyFallback = (baseEnvVar, countyName) => {
      if (countyName) {
        // Replace spaces with underscores for environment variable names
        // e.g., "Santa Rosa" becomes "Santa_Rosa"
        const sanitizedCountyName = countyName.replace(/ /g, "_");
        const countySpecificVar = `${baseEnvVar}_${sanitizedCountyName}`;
        if (process.env[countySpecificVar] !== undefined) {
          console.log(
            `  Using county-specific: ${countySpecificVar}='${process.env[countySpecificVar]}'`,
          );
          return process.env[countySpecificVar];
        }
      }
      if (process.env[baseEnvVar] !== undefined) {
        console.log(
          `  Using general: ${baseEnvVar}='${process.env[baseEnvVar]}'`,
        );
        return process.env[baseEnvVar];
      }
      return undefined;
    };

    // Configuration map for prepare flags

    /** @type {FlagConfig[]} */
    const flagConfig = [
      {
        envVar: "ELEPHANT_PREPARE_NO_FAST",
        optionKey: "noFast",
        description: "Disable fast mode",
      },
      {
        envVar: "ELEPHANT_PREPARE_NO_CONTINUE",
        optionKey: "noContinue",
        description: "Disable continue mode",
      },
    ];

    // Determine useBrowser setting from environment variable with county-specific fallback
    const useBrowserEnv = getEnvWithCountyFallback(
      "ELEPHANT_PREPARE_USE_BROWSER",
      countyName,
    );
    let useBrowser = true; // Default to true if not specified
    if (useBrowserEnv !== undefined) {
      useBrowser = useBrowserEnv === "true";
      console.log(
        `Setting useBrowser: ${useBrowser} (from environment variable)`,
      );
    } else {
      console.log(
        `Using default useBrowser: ${useBrowser} (no environment variable set)`,
      );
    }

    // Build prepare options based on environment variables
    /** @type {{ useBrowser: boolean, noFast?: boolean, noContinue?: boolean, browserFlowTemplate?: string, browserFlowParameters?: string, [key: string]: any }} */
    const prepareOptions = { useBrowser };

    console.log("Checking environment variables for prepare flags:");
    if (countyName) {
      console.log(
        `🏛️ Looking for county-specific configurations for: ${countyName}`,
      );
    }

    for (const { envVar, optionKey, description } of flagConfig) {
      const envValue = getEnvWithCountyFallback(envVar, countyName);
      if (envValue === "true") {
        prepareOptions[optionKey] = true;
        console.log(`✓ Setting ${optionKey}: true (${description})`);
      } else {
        console.log(`✗ Not setting ${optionKey} flag (${description})`);
      }
    }

    // Handle continue button selector configuration with county-specific lookup
    const continueButtonSelector = getEnvWithCountyFallback(
      "ELEPHANT_PREPARE_CONTINUE_BUTTON",
      countyName,
    );

    if (continueButtonSelector && continueButtonSelector.trim() !== "") {
      prepareOptions.continueButtonSelector = continueButtonSelector;
      console.log(
        `✓ Setting continueButtonSelector: ${continueButtonSelector}`,
      );
    }

    // Handle browser flow template configuration with county-specific lookup
    const browserFlowTemplate = getEnvWithCountyFallback(
      "ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE",
      countyName,
    );
    let browserFlowParameters = getEnvWithCountyFallback(
      "ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS",
      countyName,
    );

    if (browserFlowTemplate && browserFlowTemplate.trim() !== "") {
      console.log("Browser flow template configuration detected:");
      console.log(
        `✓ ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE='${browserFlowTemplate}'`,
      );

      if (browserFlowParameters && browserFlowParameters.trim() !== "") {
        try {
          // Parse simple key:value format (e.g., "timeout:30000,retries:3,selector:#main-content")
          /** @type {{ [key: string]: string | number | boolean }} */
          const parsedParams = {};
          const pairs = browserFlowParameters.split(",");

          for (const pair of pairs) {
            const colonIndex = pair.indexOf(":");
            if (colonIndex === -1) {
              throw new Error(
                `Invalid parameter format: "${pair}" - expected key:value`,
              );
            }

            const key = pair.substring(0, colonIndex).trim();
            const value = pair.substring(colonIndex + 1).trim();

            if (!key) {
              throw new Error(`Empty key in parameter: "${pair}"`);
            }

            // Try to parse numeric values
            if (/^\d+$/.test(value)) {
              parsedParams[key] = parseInt(value, 10);
            } else if (/^\d+\.\d+$/.test(value)) {
              parsedParams[key] = parseFloat(value);
            } else if (value.toLowerCase() === "true") {
              parsedParams[key] = true;
            } else if (value.toLowerCase() === "false") {
              parsedParams[key] = false;
            } else {
              // Keep as string
              parsedParams[key] = value;
            }
          }

          prepareOptions.browserFlowTemplate = browserFlowTemplate;
          // Pass as JSON string, not as object
          prepareOptions.browserFlowParameters = JSON.stringify(parsedParams);
          console.log(
            `✓ ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS parsed successfully:`,
            JSON.stringify(parsedParams, null, 2),
          );
        } catch (parseError) {
          console.error(
            `✗ Failed to parse ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS: ${parseError instanceof Error ? parseError.message : String(parseError)}`,
          );
          console.error(
            `Invalid format: ${browserFlowParameters.substring(0, 100)}...`,
          );
          console.error(`Expected format: key1:value1,key2:value2`);
          // Continue without browser flow parameters rather than failing
          console.warn(
            "Continuing without browser flow configuration due to invalid format",
          );
        }
      } else {
        console.log(
          `✗ ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS not set or empty - browser flow template will not be used`,
        );
      }
    } else if (browserFlowParameters && browserFlowParameters.trim() !== "") {
      console.warn(
        "⚠️ ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS is set but ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE is not - ignoring parameters",
      );
    }

    // Prepare Phase (Main bottleneck)
    console.log("🔄 Starting prepare() function...");
    const prepareStart = Date.now();
    console.log(
      "Calling prepare() with these options:",
      JSON.stringify(prepareOptions, null, 2),
    );

    let prepareDuration;
    try {
      await prepare(inputZip, outputZip, prepareOptions);
      prepareDuration = Date.now() - prepareStart;
      console.log(
        `✅ Prepare function completed: ${prepareDuration}ms (${(prepareDuration / 1000).toFixed(2)}s)`,
      );
      console.log(
        `🔍 PERFORMANCE: Local=2s, Lambda=${(prepareDuration / 1000).toFixed(1)}s - ${prepareDuration > 3000 ? "⚠️ SLOW" : "✅ OK"}`,
      );
    } catch (prepareError) {
      prepareDuration = Date.now() - prepareStart;

      // Gather file system context
      let inputFileInfo = null;
      let outputFileInfo = null;

      try {
        const inputStats = await fs.stat(inputZip);
        inputFileInfo = {
          exists: true,
          size: inputStats.size,
          path: inputZip,
        };
      } catch (statsError) {
        inputFileInfo = {
          exists: false,
          error:
            statsError instanceof Error
              ? statsError.message
              : String(statsError),
          path: inputZip,
        };
      }

      try {
        const outputStats = await fs.stat(outputZip);
        outputFileInfo = {
          exists: true,
          size: outputStats.size,
          partiallyCreated: true,
          path: outputZip,
        };
      } catch (outputError) {
        outputFileInfo = {
          exists: false,
          partiallyCreated: false,
          path: outputZip,
        };
      }

      // Structured error log
      const prepareErrorLog = {
        timestamp: new Date().toISOString(),
        level: "ERROR",
        type: "PREPARE_FUNCTION_ERROR",
        message: "Prepare function execution failed",
        error: {
          name:
            prepareError instanceof Error
              ? prepareError.name
              : String(prepareError),
          message:
            prepareError instanceof Error
              ? prepareError.message
              : String(prepareError),
          stack:
            prepareError instanceof Error
              ? prepareError.stack
              : String(prepareError),
        },
        execution: {
          duration: prepareDuration,
          durationSeconds: (prepareDuration / 1000).toFixed(2),
          phase: "prepare",
        },
        context: {
          inputS3Uri: event.input_s3_uri,
          inputFile: inputFileInfo,
          outputFile: outputFileInfo,
          options: prepareOptions,
        },
        lambda: {
          function: process.env.AWS_LAMBDA_FUNCTION_NAME,
          version: process.env.AWS_LAMBDA_FUNCTION_VERSION,
          requestId: process.env.AWS_REQUEST_ID,
          region: process.env.AWS_REGION,
          memorySize: process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE,
        },
      };

      console.error("❌ PREPARE FUNCTION FAILED");
      console.error(JSON.stringify(prepareErrorLog, null, 2));

      // Re-throw with enhanced context
      /** @type {EnhancedError} */
      const enhancedError = new Error(
        `Prepare function failed: ${prepareError instanceof Error ? prepareError.message : String(prepareError)}`,
      );
      enhancedError.originalError =
        prepareError instanceof Error ? prepareError : undefined;
      enhancedError.type = "PREPARE_FUNCTION_ERROR";
      enhancedError.context = prepareErrorLog.context;
      enhancedError.execution = prepareErrorLog.execution;
      throw enhancedError;
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
      outKey = path.posix.join(outPrefix.replace(/\/$/, ""), "output.zip");
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
    if (!(lambdaError instanceof Error)) {
      throw new Error("Lambda execution failed, but no error was thrown");
    }
    /** @type {EnhancedError} */
    const error = lambdaError;

    // Structured Lambda error log
    const lambdaErrorLog = {
      timestamp: new Date().toISOString(),
      level: "ERROR",
      type: error.type || "LAMBDA_EXECUTION_ERROR",
      message: "Lambda execution failed",
      error: {
        name: error.name,
        message: error.message,
        stack: error.stack,
      },
      execution: {
        totalDuration: totalDuration,
        totalDurationSeconds: (totalDuration / 1000).toFixed(2),
        failed: true,
      },
      input: {
        event: event,
        inputS3Uri: event?.input_s3_uri || null,
      },
      context: error.context || null,
      validationErrors: error.validationErrors || null,
      lambda: {
        function: process.env.AWS_LAMBDA_FUNCTION_NAME,
        version: process.env.AWS_LAMBDA_FUNCTION_VERSION,
        requestId: process.env.AWS_REQUEST_ID,
        region: process.env.AWS_REGION,
        memorySize: process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE,
        runtime: process.env.AWS_EXECUTION_ENV,
      },
    };

    console.error("LAMBDA EXECUTION FAILED");
    console.error(JSON.stringify(lambdaErrorLog, null, 2));

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
