import {
  GetObjectCommand,
  S3Client,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import {
  DynamoDBClient,
  QueryCommand,
  UpdateItemCommand,
} from "@aws-sdk/client-dynamodb";
import {
  SFNClient,
  SendTaskSuccessCommand,
  SendTaskFailureCommand,
} from "@aws-sdk/client-sfn";
import {
  EventBridgeClient,
  PutEventsCommand,
} from "@aws-sdk/client-eventbridge";
import { promises as fs } from "fs";
import path from "path";
import { prepare } from "@elephant-xyz/cli/lib";
import { networkInterfaces } from "os";
import AdmZip from "adm-zip";
import { parse as parseCSV } from "csv-parse/sync";

/**
 * Custom error class for Prepare errors with error codes
 */
class PrepareError extends Error {
  /**
   * @param {string} code - Error code (e.g., "01003")
   * @param {string} message - Error message
   */
  constructor(code, message) {
    super(message);
    this.name = "PrepareError";
    this.code = code;
  }
}

const RE_S3PATH = /^s3:\/\/([^/]+)\/(.*)$/i;
const sfnClient = new SFNClient({});
const eventBridgeClient = new EventBridgeClient({});

/**
 * Emits a workflow event to EventBridge
 * @param {Object} params - Event parameters
 * @param {string} params.executionId - Step Functions execution ARN
 * @param {string} params.county - County name
 * @param {string} params.status - Event status (IN_PROGRESS, etc.)
 * @returns {Promise<void>}
 */
async function emitWorkflowEvent({ executionId, county, status }) {
  console.log(
    `📤 Emitting EventBridge event: status=${status}, county=${county}`,
  );
  try {
    /** @type {{ executionId: string; county: string; status: string; phase: string; step: string }} */
    const detail = {
      executionId,
      county,
      status,
      phase: "Prepare",
      step: "Prepare",
    };

    await eventBridgeClient.send(
      new PutEventsCommand({
        Entries: [
          {
            Source: "elephant.workflow",
            DetailType: "WorkflowEvent",
            Detail: JSON.stringify(detail),
          },
        ],
      }),
    );
    console.log(`✅ EventBridge event emitted: ${status}`);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(`⚠️ Failed to emit EventBridge event [01019]: ${errorMessage}`);
    throw new PrepareError("01019", `Failed to emit EventBridge event: ${errorMessage}`);
  }
}

/**
 * Sends task success to Step Functions
 * @param {string} taskToken - The task token from SQS message
 * @param {Object} output - The output to send back to Step Functions
 * @returns {Promise<void>}
 */
async function sendTaskSuccess(taskToken, output) {
  console.log(`📤 Sending task success to Step Functions...`);
  try {
    await sfnClient.send(
      new SendTaskSuccessCommand({
        taskToken,
        output: JSON.stringify(output),
      }),
    );
    console.log(`✅ Task success sent`);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(`❌ Failed to send task success [01017]: ${errorMessage}`);
    throw new PrepareError("01017", `Failed to send task success: ${errorMessage}`);
  }
}

/**
 * Sends task failure to Step Functions
 * @param {string} taskToken - The task token from SQS message
 * @param {string} errorCode - Error code identifier
 * @param {string} cause - Error cause description
 * @returns {Promise<void>}
 */
async function sendTaskFailure(taskToken, errorCode, cause) {
  console.log(`📤 Sending task failure to Step Functions...`);
  try {
    await sfnClient.send(
      new SendTaskFailureCommand({
        taskToken,
        error: errorCode,
        cause: cause.substring(0, 256), // Max 256 chars
      }),
    );
    console.log(`✅ Task failure sent`);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(`❌ Failed to send task failure [01018]: ${errorMessage}`);
    throw new PrepareError("01018", `Failed to send task failure: ${errorMessage}`);
  }
}

/**
 * Extracts county name from the input zip's input.csv (county column)
 * Falls back to unnormalized_address.json if input.csv doesn't have county
 * @param {AdmZip} zip - AdmZip instance
 * @returns {string|null} County name or null
 */
function extractCountyFromZip(zip) {
  try {
    const zipEntries = zip.getEntries();

    // First, try to get county from input.csv
    const inputCsvEntry = zipEntries.find(
      (entry) =>
        entry.entryName === "input.csv" ||
        entry.entryName.endsWith("/input.csv"),
    );

    if (inputCsvEntry) {
      const csvContent = zip.readAsText(inputCsvEntry);
      const rows = parseCSV(csvContent, {
        columns: true,
        skip_empty_lines: true,
        trim: true,
      });

      const firstRow = rows[0];
      if (firstRow) {
        // Look for 'county' column (case-insensitive)
        const countyKey = Object.keys(firstRow).find(
          (k) => k.toLowerCase() === "county",
        );

        if (
          countyKey &&
          firstRow[countyKey] &&
          firstRow[countyKey].trim() !== ""
        ) {
          const county = firstRow[countyKey].trim();
          console.log(`✅ Found county in input.csv: ${county}`);
          return county;
        }
      }
    }

    // Fallback: try unnormalized_address.json
    const addressEntry = zipEntries.find(
      (entry) => entry.entryName === "unnormalized_address.json",
    );

    if (addressEntry) {
      const addressContent = zip.readAsText(addressEntry);
      const addressData = JSON.parse(addressContent);
      if (addressData.county_jurisdiction) {
        console.log(
          `✅ Found county in unnormalized_address.json: ${addressData.county_jurisdiction}`,
        );
        return addressData.county_jurisdiction;
      }
    }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(`⚠️ Could not extract county from zip: ${errorMessage}`);
    throw new PrepareError("01008", `Could not extract county from zip: ${errorMessage}`);
  }
  return null;
}

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
 * @property {"useBrowser" | "noFast" | "noContinue" | "ignoreCaptcha"} optionKey
 * @property {string} description
 */

/** @typedef {Error & { originalError?: Error, type?: string, context?: Object, execution?: Object, validationErrors?: Object }} EnhancedError */

/**
 * @typedef {Object} ProxyInfo
 * @property {string} proxyId - Unique identifier for the proxy
 * @property {string} proxyUrl - Proxy URL in format username:password@ip:port
 * @property {number} lastUsedTime - Unix timestamp of last usage
 * @property {boolean} failed - Whether the last usage failed
 * @property {boolean} locked - Whether the proxy is currently locked by a lambda
 * @property {number} lockedAt - Unix timestamp when the proxy was locked
 */

/**
 * Reads configuration S3 URI from input.csv in the zip file
 * @param {AdmZip} zip - AdmZip instance of the input zip
 * @returns {string | null} Configuration S3 URI or null if not found (no config column)
 * @throws {PrepareError} If input.csv cannot be read or parsed
 */
function getConfigurationUriFromInputCsv(zip) {
  const zipEntries = zip.getEntries();

  // Find input.csv in the zip
  const inputCsvEntry = zipEntries.find(
    (entry) =>
      entry.entryName === "input.csv" || entry.entryName.endsWith("/input.csv"),
  );

  if (!inputCsvEntry) {
    console.error("📄 input.csv not found in zip file");
    throw new PrepareError("01006", "input.csv not found in zip file");
  }

  let csvContent;
  try {
    csvContent = zip.readAsText(inputCsvEntry);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(`⚠️ Error reading input.csv: ${errorMessage}`);
    throw new PrepareError("01003", `Error reading input.csv: ${errorMessage}`);
  }

  let rows;
  try {
    rows = parseCSV(csvContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    });
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(`⚠️ Error parsing input.csv: ${errorMessage}`);
    throw new PrepareError("01004", `Error parsing input.csv: ${errorMessage}`);
  }

  if (rows.length === 0) {
    console.error("📄 input.csv is empty or has no data rows");
    throw new PrepareError("01005", "input.csv is empty or has no data rows");
  }

  // Get configuration column from first row (case-insensitive)
  const firstRow = rows[0];
  const configKey = Object.keys(firstRow).find(
    (k) =>
      k.toLowerCase() === "configuration" || k.toLowerCase() === "config",
  );

  if (configKey && firstRow[configKey] && firstRow[configKey].trim() !== "") {
    const configUri = firstRow[configKey].trim();
    console.log(`✅ Found configuration URI in input.csv: ${configUri}`);
    return configUri;
  }

  console.log(
    "📄 No 'configuration' column found in input.csv, using environment variables",
  );
  return null;
}

/**
 * Downloads and parses JSON configuration file from S3
 * @param {S3Client} s3Client - S3 client instance
 * @param {string} configS3Uri - S3 URI of configuration file (s3://bucket/key.json)
 * @returns {Promise<{[key: string]: any} | null>} Configuration object or null on failure
 */
async function downloadConfigurationFromS3(s3Client, configS3Uri) {
  try {
    const match = RE_S3PATH.exec(configS3Uri);
    if (!match) {
      console.error(`❌ Invalid S3 URI format: ${configS3Uri}`);
      return null;
    }

    const [, bucket, key] = match;
    console.log(`📥 Downloading configuration from s3://${bucket}/${key}`);

    const response = await s3Client.send(
      new GetObjectCommand({ Bucket: bucket, Key: key }),
    );

    const configBytes = await response.Body?.transformToByteArray();
    if (!configBytes) {
      throw new PrepareError("01009", "Failed to download configuration file body");
    }

    const configContent = new TextDecoder().decode(configBytes);
    const config = JSON.parse(configContent);

    console.log(
      `✅ Configuration loaded from S3 (${Object.keys(config).length} keys):`,
    );
    // Log keys (redact sensitive values)
    for (const configKey of Object.keys(config)) {
      if (
        configKey.includes("KEY") ||
        configKey.includes("JWT") ||
        configKey.includes("TOKEN") ||
        configKey.includes("PASSWORD") ||
        configKey.includes("SECRET")
      ) {
        console.log(`   ${configKey}: ***REDACTED***`);
      } else {
        const value = config[configKey];
        const displayValue =
          typeof value === "object" ? JSON.stringify(value) : value;
        console.log(`   ${configKey}: ${displayValue}`);
      }
    }

    return config;
  } catch (error) {
    if (error instanceof Error && error.name === "NoSuchKey") {
      console.error(`❌ Configuration file not found: ${configS3Uri}`);
    } else {
      console.error(
        `❌ Failed to download configuration: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    return null;
  }
}

/**
 * Gets a configuration value with S3 config priority, then falls back to environment variables
 * Priority: S3 config county-specific -> S3 config general -> env var county-specific -> env var general
 * @param {string} baseKey - Base configuration key (e.g., "ELEPHANT_PREPARE_USE_BROWSER")
 * @param {string | null} countyName - County name for county-specific lookups
 * @param {{[key: string]: any} | null} s3Config - Configuration loaded from S3
 * @returns {string | undefined} Configuration value or undefined if not found
 */
function getConfigValue(baseKey, countyName, s3Config) {
  // Sanitize county name for key lookup (replace spaces with underscores)
  const sanitizedCounty = countyName ? countyName.replace(/ /g, "_") : null;
  const countySpecificKey = sanitizedCounty
    ? `${baseKey}_${sanitizedCounty}`
    : null;

  // 1. Check S3 config county-specific
  if (
    s3Config &&
    countySpecificKey &&
    s3Config[countySpecificKey] !== undefined
  ) {
    const value = s3Config[countySpecificKey];
    console.log(`  Using S3 config (county-specific): ${countySpecificKey}`);
    return typeof value === "object" ? JSON.stringify(value) : String(value);
  }

  // 2. Check S3 config general
  if (s3Config && s3Config[baseKey] !== undefined) {
    const value = s3Config[baseKey];
    console.log(`  Using S3 config (general): ${baseKey}`);
    return typeof value === "object" ? JSON.stringify(value) : String(value);
  }

  // 3. Check env var county-specific (backward compatibility)
  if (countySpecificKey && process.env[countySpecificKey] !== undefined) {
    console.log(`  Using env var (county-specific): ${countySpecificKey}`);
    return process.env[countySpecificKey];
  }

  // 4. Check env var general (backward compatibility)
  if (process.env[baseKey] !== undefined) {
    console.log(`  Using env var (general): ${baseKey}`);
    return process.env[baseKey];
  }

  return undefined;
}

/**
 * Cleans up stale proxy locks that have been held for more than the timeout period
 * @param {DynamoDBClient} dynamoClient - DynamoDB client instance
 * @param {string} tableName - Name of the DynamoDB table
 * @param {number} lockTimeoutMs - Maximum time a lock can be held before considered stale (default: 30 minutes)
 * @returns {Promise<void>}
 */
const cleanupStaleLocks = async (
  dynamoClient,
  tableName,
  lockTimeoutMs = 30 * 60 * 1000,
) => {
  try {
    console.log(
      `🧹 Cleaning up stale proxy locks older than ${lockTimeoutMs / 1000 / 60} minutes...`,
    );

    const now = Date.now();
    const staleThreshold = now - lockTimeoutMs;
    console.log(`Stale threshold: ${staleThreshold}`);

    // Scan for locked proxies that are stale
    const scanCommand = new QueryCommand({
      TableName: tableName,
      IndexName: "LastUsedIndex",
      KeyConditionExpression: "constantKey = :constantKey",
      FilterExpression: "locked = :locked AND lockedAt < :staleThreshold",
      ExpressionAttributeValues: {
        ":constantKey": { S: "PROXY" },
        ":locked": { BOOL: true },
        ":staleThreshold": { N: staleThreshold.toString() },
      },
    });

    const response = await dynamoClient.send(scanCommand);

    if (response.Items && response.Items.length > 0) {
      console.log(`🔓 Found ${response.Items.length} stale locks to clean up`);

      // Release each stale lock
      for (const item of response.Items) {
        const proxyId = item.proxyId?.S;
        if (proxyId) {
          try {
            const updateCommand = new UpdateItemCommand({
              TableName: tableName,
              Key: {
                proxyId: { S: proxyId },
              },
              UpdateExpression: "SET locked = :unlocked REMOVE lockedAt",
              ExpressionAttributeValues: {
                ":unlocked": { BOOL: false },
              },
            });

            await dynamoClient.send(updateCommand);
            console.log(`✅ Released stale lock for proxy: ${proxyId}`);
          } catch (updateError) {
            console.error(
              `⚠️ Failed to release stale lock for proxy ${proxyId}: ${updateError instanceof Error ? updateError.message : String(updateError)}`,
            );
          }
        }
      }
    } else {
      console.log("✅ No stale locks found");
    }
  } catch (error) {
    console.error(
      `⚠️ Failed to cleanup stale locks: ${error instanceof Error ? error.message : String(error)}`,
    );
    // Don't throw - this is a cleanup operation
  }
};

/**
 * Acquires a lock on the least recently used unlocked proxy from DynamoDB
 * @param {DynamoDBClient} dynamoClient - DynamoDB client instance
 * @param {string} tableName - Name of the DynamoDB table
 * @returns {Promise<ProxyInfo | null>} Proxy information or null if no unlocked proxies available
 */
const acquireProxyLock = async (dynamoClient, tableName, maxRetries = 10) => {
  try {
    // First, cleanup any stale locks
    await cleanupStaleLocks(dynamoClient, tableName);

    console.log(
      `🔍 Querying DynamoDB table ${tableName} for least recently used unlocked proxy...`,
    );

    const queryCommand = new QueryCommand({
      TableName: tableName,
      IndexName: "LastUsedIndex",
      KeyConditionExpression: "constantKey = :constantKey",
      FilterExpression: "locked = :locked OR attribute_not_exists(locked)",
      ExpressionAttributeValues: {
        ":constantKey": { S: "PROXY" },
        ":locked": { BOOL: false },
      },
      Limit: 1,
      ScanIndexForward: true, // Sort ascending by lastUsedTime (oldest first)
    });

    const response = await dynamoClient.send(queryCommand);

    if (!response.Items || response.Items.length === 0) {
      console.log("⚠️ No unlocked proxies found in DynamoDB table");
      return null;
    }

    const item = response.Items[0];
    if (!item) {
      console.log("⚠️ Unexpected: First item in response is undefined");
      return null;
    }

    const proxyId = item.proxyId?.S || "";
    const now = Date.now();

    // Attempt to acquire the lock with conditional update
    try {
      const updateCommand = new UpdateItemCommand({
        TableName: tableName,
        Key: {
          proxyId: { S: proxyId },
        },
        UpdateExpression:
          "SET locked = :locked, lockedAt = :lockedAt, lastUsedTime = :time",
        ConditionExpression:
          "locked = :unlocked OR attribute_not_exists(locked)",
        ExpressionAttributeValues: {
          ":locked": { BOOL: true },
          ":unlocked": { BOOL: false },
          ":lockedAt": { N: now.toString() },
          ":time": { N: now.toString() },
        },
      });

      await dynamoClient.send(updateCommand);

      const proxyInfo = {
        proxyId: proxyId,
        proxyUrl: item.proxyUrl?.S || "",
        lastUsedTime: now,
        failed: item.failed?.BOOL || false,
        locked: true,
        lockedAt: now,
      };

      console.log(
        JSON.stringify(
          {
            message: "Successfully acquired proxy lock",
            proxyId: proxyInfo.proxyId,
            lastUsedTime: proxyInfo.lastUsedTime,
            failed: proxyInfo.failed,
            locked: proxyInfo.locked,
            lockedAt: proxyInfo.lockedAt,
          },
          null,
          2,
        ),
      );

      return proxyInfo;
    } catch (updateError) {
      // Lock acquisition failed (likely due to condition not met - proxy already locked)
      console.log(
        `⚠️ Failed to acquire lock for proxy ${proxyId}: ${updateError instanceof Error ? updateError.message : String(updateError)}`,
      );

      if (maxRetries > 0) {
        // Try again recursively (will get the next available proxy)
        console.log(
          `🔄 Retrying proxy lock acquisition (${maxRetries} attempts remaining)...`,
        );
        return await acquireProxyLock(dynamoClient, tableName, maxRetries - 1);
      } else {
        console.log("❌ Maximum retry attempts reached, no proxy available");
        return null;
      }
    }
  } catch (error) {
    console.error(
      `❌ Failed to query DynamoDB for proxy: ${error instanceof Error ? error.message : String(error)}`,
    );
    throw error;
  }
};

/**
 * Updates proxy usage information in DynamoDB
 * @param {DynamoDBClient} dynamoClient - DynamoDB client instance
 * @param {string} tableName - Name of the DynamoDB table
 * @param {string} proxyId - Unique identifier for the proxy
 * @param {boolean} failed - Whether the proxy usage failed
 * @returns {Promise<void>}
 */
const updateProxyUsage = async (dynamoClient, tableName, proxyId, failed) => {
  try {
    const now = Date.now();
    console.log(
      `📝 Updating proxy ${proxyId} usage (failed: ${failed}, time: ${new Date(now).toISOString()})...`,
    );

    const updateCommand = new UpdateItemCommand({
      TableName: tableName,
      Key: {
        proxyId: { S: proxyId },
      },
      UpdateExpression:
        "SET lastUsedTime = :time, failed = :failed, locked = :unlocked REMOVE lockedAt",
      ExpressionAttributeValues: {
        ":time": { N: now.toString() },
        ":failed": { BOOL: failed },
        ":unlocked": { BOOL: false },
      },
    });

    await dynamoClient.send(updateCommand);
    console.log(`✅ Successfully updated proxy ${proxyId} usage`);
  } catch (error) {
    console.error(
      `❌ Failed to update proxy usage in DynamoDB: ${error instanceof Error ? error.message : String(error)}`,
    );
    // Don't throw - this is a non-critical operation
  }
};

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
    throw new PrepareError("01010", `Invalid S3 path format: ${s3Uri}. Expected: s3://bucket/object`);
  }

  const [, bucket, key] = match;
  if (!bucket || !key) {
    throw new PrepareError("01010", `Invalid S3 path format: ${s3Uri}. Expected: s3://bucket/object`);
  }
  return { bucket, key };
};

/**
 * Processes the prepare step for a single parcel
 * @param {Object} params - Processing parameters
 * @param {string} params.input_s3_uri - S3 URI of input file
 * @param {string} [params.output_s3_uri_prefix] - S3 URI prefix for output files
 * @param {string} [params.executionId] - Step Functions execution ARN
 * @param {string} [params.taskToken] - Task token for SQS callback
 * @returns {Promise<{ output_s3_uri: string; county: string | null }>} Success result
 */
async function processPrepare({
  input_s3_uri,
  output_s3_uri_prefix,
  executionId,
  taskToken,
}) {
  const startTime = Date.now();
  console.log(`🚀 Prepare processing started at: ${new Date().toISOString()}`);

  if (!input_s3_uri) {
    throw new PrepareError("01011", "Missing required field: input_s3_uri");
  }

  const { bucket, key } = splitS3Uri(input_s3_uri);
  console.log("Bucket:", bucket);
  console.log("Key:", key);
  const s3 = new S3Client({});

  // Initialize DynamoDB client for proxy rotation
  const dynamoClient = new DynamoDBClient({});
  const proxyTableName = process.env.PROXY_ROTATION_TABLE_NAME;

  // Get least recently used proxy if table is configured
  /** @type {ProxyInfo | null} */
  let selectedProxy = null;
  if (proxyTableName) {
    try {
      selectedProxy = await acquireProxyLock(dynamoClient, proxyTableName);
      if (selectedProxy) {
        console.log(`🌐 Using proxy: ${selectedProxy.proxyId}`);
      } else {
        console.log("ℹ️ No proxies available, proceeding without proxy");
      }
    } catch (proxyError) {
      console.error(
        `⚠️ Failed to get proxy from DynamoDB: ${proxyError instanceof Error ? proxyError.message : String(proxyError)}`,
      );
      console.log("ℹ️ Proceeding without proxy");
    }
  } else {
    console.log(
      "ℹ️ Proxy rotation table not configured, proceeding without proxy",
    );
  }

  const tempDir = await fs.mkdtemp("/tmp/prepare-");
  /** @type {string | null} */
  let countyName = null;

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
      throw new PrepareError("01012", "Failed to download input object body");
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
    console.log("📍 Extracting county information from input...");

    try {
      const zip = new AdmZip(inputZip);
      countyName = extractCountyFromZip(zip);
      if (countyName) {
        console.log(`✅ Detected county: ${countyName}`);
      } else {
        console.log("⚠️ No county_jurisdiction found");
      }
    } catch (error) {
      console.error(
        `⚠️ Could not extract county information: ${error instanceof Error ? error.message : String(error)}`,
      );
      console.log("Continuing with general configuration...");
    }

    // Log that we have county info (EventBridge events are emitted by the state machine)
    if (executionId) {
      console.log(
        `📋 Processing for execution: ${executionId}, county: ${countyName || "unknown"}`,
      );
    }

    // Load configuration from S3 if specified in input.csv
    /** @type {{[key: string]: any} | null} */
    let s3Config = null;
    try {
      const zip = new AdmZip(inputZip);
      const configUri = getConfigurationUriFromInputCsv(zip);
      if (configUri) {
        console.log("📦 Loading configuration from S3...");
        s3Config = await downloadConfigurationFromS3(s3, configUri);
        if (s3Config) {
          console.log(
            "✅ S3 configuration loaded successfully - will prioritize over environment variables",
          );
        } else {
          console.log(
            "⚠️ Failed to load S3 configuration, falling back to environment variables",
          );
        }
      } else {
        console.log(
          "ℹ️ No configuration URI in input.csv, using environment variables only",
        );
      }
    } catch (configError) {
      console.error(
        `⚠️ Error loading S3 configuration: ${configError instanceof Error ? configError.message : String(configError)}`,
      );
      console.log("Continuing with environment variables...");
    }

    const outputZip = path.join(tempDir, "output.zip");

    console.log("Building prepare options...");

    // Helper function to get config value with S3 priority, then env var fallback
    /**
     * @param {string} baseKey
     * @returns {string | undefined}
     */
    const getEnvWithCountyFallback = (baseKey) => {
      return getConfigValue(baseKey, countyName, s3Config);
    };

    // Helper function to download flow files from S3
    /**
     * @param {string} flowType
     * @param {string} s3Prefix
     * @param {string} localFileName
     * @param {string} optionKey
     * @returns {Promise<void>}
     */
    const downloadFlowFile = async (
      flowType,
      s3Prefix,
      localFileName,
      optionKey,
    ) => {
      if (environmentBucket && countyName) {
        const s3Key = `${s3Prefix}/${countyName}.json`;

        try {
          console.log(
            `Checking for ${flowType} flow file: s3://${environmentBucket}/${s3Key}`,
          );

          const response = await s3.send(
            new GetObjectCommand({
              Bucket: environmentBucket,
              Key: s3Key,
            }),
          );

          const fileBytes = await response.Body?.transformToByteArray();
          if (!fileBytes) {
            throw new PrepareError("01013", `Failed to download ${flowType} flow file body`);
          }

          const filePath = path.join(tempDir, localFileName);
          await fs.writeFile(filePath, Buffer.from(fileBytes));
          // @ts-ignore
          prepareOptions[optionKey] = filePath;
          console.log(
            `✓ ${flowType} flow file downloaded and set for county ${countyName}: ${filePath}`,
          );
        } catch (downloadError) {
          if (
            downloadError instanceof Error &&
            downloadError.name === "NoSuchKey"
          ) {
            console.log(
              `No ${flowType} flow file found for county ${countyName}, continuing without it`,
            );
          } else {
            console.error(
              `Failed to download ${flowType} flow file for county ${countyName}: ${downloadError instanceof Error ? downloadError.message : String(downloadError)}`,
            );
          }
        }
      }
    };

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
      {
        envVar: "ELEPHANT_PREPARE_IGNORE_CAPTCHA",
        optionKey: "ignoreCaptcha",
        description: "Ignore captcha challenges",
      },
    ];

    // Determine useBrowser setting
    const useBrowserEnv = getEnvWithCountyFallback(
      "ELEPHANT_PREPARE_USE_BROWSER",
    );
    let useBrowser = true;
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

    /** @type {{ useBrowser: boolean, noFast?: boolean, noContinue?: boolean, browserFlowTemplate?: string, browserFlowParameters?: string, proxyUrl?: string, ignoreCaptcha?: boolean, continueButtonSelector?: string, browserFlowFile?: string, multiRequestFlowFile?: string }} */
    const prepareOptions = { useBrowser };

    // Add proxy URL if available
    if (selectedProxy && selectedProxy.proxyUrl) {
      prepareOptions.proxyUrl = selectedProxy.proxyUrl;
      console.log(`✓ Setting proxyUrl for prepare function`);
    }

    console.log("Checking environment variables for prepare flags:");
    if (countyName) {
      console.log(
        `🏛️ Looking for county-specific configurations for: ${countyName}`,
      );
    }

    for (const { envVar, optionKey, description } of flagConfig) {
      const envValue = getEnvWithCountyFallback(envVar);
      if (envValue === "true") {
        prepareOptions[optionKey] = true;
        console.log(`✓ Setting ${optionKey}: true (${description})`);
      } else {
        console.log(`✗ Not setting ${optionKey} flag (${description})`);
      }
    }

    // Handle continue button selector
    const continueButtonSelector = getEnvWithCountyFallback(
      "ELEPHANT_PREPARE_CONTINUE_BUTTON",
    );
    if (continueButtonSelector && continueButtonSelector.trim() !== "") {
      prepareOptions.continueButtonSelector = continueButtonSelector;
      console.log(
        `✓ Setting continueButtonSelector: ${continueButtonSelector}`,
      );
    }

    // Check for flow files in S3
    const environmentBucket = process.env.ENVIRONMENT_BUCKET;
    await downloadFlowFile(
      "browser",
      "browser-flows",
      "browser-flow.json",
      "browserFlowFile",
    );

    await downloadFlowFile(
      "multi-request",
      "multi-request-flows",
      "multi-request-flow.json",
      "multiRequestFlowFile",
    );

    // Handle browser flow template configuration
    const browserFlowTemplate = getEnvWithCountyFallback(
      "ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE",
    );
    let browserFlowParameters = getEnvWithCountyFallback(
      "ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS",
    );

    if (browserFlowTemplate && browserFlowTemplate.trim() !== "") {
      console.log("Browser flow template configuration detected:");
      console.log(
        `✓ ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE='${browserFlowTemplate}'`,
      );

      if (browserFlowParameters && browserFlowParameters.trim() !== "") {
        try {
          /** @type {{ [key: string]: string | number | boolean }} */
          let parsedParams = {};
          const trimmedParams = browserFlowParameters.trim();

          if (trimmedParams.startsWith("{") && trimmedParams.endsWith("}")) {
            parsedParams = JSON.parse(trimmedParams);
            console.log(
              `✓ ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS detected as JSON format`,
            );
          } else {
            console.log(
              `✓ ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS detected as key:value format`,
            );
            const pairs = browserFlowParameters.split(",");

            for (const pair of pairs) {
              const colonIndex = pair.indexOf(":");
              if (colonIndex === -1) {
                throw new PrepareError(
                  "01014",
                  `Invalid parameter format: "${pair}" - expected key:value`,
                );
              }

              const key = pair.substring(0, colonIndex).trim();
              const value = pair.substring(colonIndex + 1).trim();

              if (!key) {
                throw new PrepareError("01015", `Empty key in parameter: "${pair}"`);
              }

              if (/^\d+$/.test(value)) {
                parsedParams[key] = parseInt(value, 10);
              } else if (/^\d+\.\d+$/.test(value)) {
                parsedParams[key] = parseFloat(value);
              } else if (value.toLowerCase() === "true") {
                parsedParams[key] = true;
              } else if (value.toLowerCase() === "false") {
                parsedParams[key] = false;
              } else {
                parsedParams[key] = value;
              }
            }
          }

          prepareOptions.browserFlowTemplate = browserFlowTemplate;
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
          console.error(
            `Expected format: JSON object or key1:value1,key2:value2`,
          );
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

    // Prepare Phase
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

      // Update proxy usage as successful
      if (selectedProxy && proxyTableName) {
        await updateProxyUsage(
          dynamoClient,
          proxyTableName,
          selectedProxy.proxyId,
          false,
        );
      }
    } catch (prepareError) {
      prepareDuration = Date.now() - prepareStart;

      // Update proxy usage as failed
      if (selectedProxy && proxyTableName) {
        await updateProxyUsage(
          dynamoClient,
          proxyTableName,
          selectedProxy.proxyId,
          true,
        );
      }

      // Re-throw with enhanced context
      /** @type {EnhancedError} */
      const enhancedError = new Error(
        `Prepare function failed: ${prepareError instanceof Error ? prepareError.message : String(prepareError)}`,
      );
      enhancedError.originalError =
        prepareError instanceof Error ? prepareError : undefined;
      enhancedError.type = "PREPARE_FUNCTION_ERROR";
      throw enhancedError;
    }

    // Add input.csv to the output zip
    console.log("📝 Adding input.csv to output zip...");
    try {
      const inputZipReader = new AdmZip(inputZip);
      const inputCsvEntry = inputZipReader.getEntry("input.csv");

      if (inputCsvEntry) {
        const outputZipWriter = new AdmZip(outputZip);
        outputZipWriter.addFile("input.csv", inputCsvEntry.getData());
        await fs.writeFile(outputZip, outputZipWriter.toBuffer());
        console.log("✅ Added input.csv to output zip");
      } else {
        console.log("ℹ️ No input.csv found in input zip, skipping");
      }
    } catch (csvError) {
      console.warn(
        `⚠️ Failed to add input.csv to output: ${csvError instanceof Error ? csvError.message : String(csvError)}`,
      );
    }

    // Check output file size
    const outputStats = await fs.stat(outputZip);
    console.log(`📊 Output file size: ${outputStats.size} bytes`);

    // Determine upload destination
    let outBucket = bucket;
    let outKey = key;
    if (output_s3_uri_prefix) {
      const { bucket: outB, key: outPrefix } = splitS3Uri(output_s3_uri_prefix);
      outBucket = outB;
      outKey = path.posix.join(outPrefix.replace(/\/$/, ""), "output.zip");
    } else {
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
      `🏁 Prepare processing completed at: ${new Date().toISOString()}\n`,
    );

    return { output_s3_uri: `s3://${outBucket}/${outKey}`, county: countyName };
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
}

/**
 * @typedef {Object} SQSRecord
 * @property {string} eventSource - Event source (e.g., "aws:sqs")
 * @property {string} body - Message body as JSON string
 */

/**
 * @typedef {Object} SQSEvent
 * @property {SQSRecord[]} Records - Array of SQS records
 */

/**
 * @typedef {Object} DirectInvokeEvent
 * @property {string} input_s3_uri - S3 URI of input file
 * @property {string} [output_s3_uri_prefix] - S3 URI prefix for output files
 */

/**
 * Lambda handler for processing orders and storing receipts in S3.
 * Supports both direct invocation and SQS trigger with task token.
 * @param {SQSEvent | DirectInvokeEvent} event - Input event (direct invoke or SQS event)
 * @returns {Promise<{ output_s3_uri: string; county: string | null } | void>} Success message (direct invoke only)
 */
export const handler = async (event) => {
  console.log("Event:", JSON.stringify(event, null, 2));
  console.log(`🚀 Lambda handler started at: ${new Date().toISOString()}`);

  // Detect if this is an SQS event
  /** @type {SQSEvent} */
  const sqsEvent = /** @type {SQSEvent} */ (event);
  const isSQSEvent =
    sqsEvent.Records &&
    Array.isArray(sqsEvent.Records) &&
    sqsEvent.Records[0]?.eventSource === "aws:sqs";

  if (isSQSEvent) {
    // Process SQS messages (with task token pattern)
    console.log(`📬 Processing ${sqsEvent.Records.length} SQS message(s)`);

    for (const record of sqsEvent.Records) {
      let messageBody;
      let taskToken;
      let executionId;
      let county = "unknown";

      try {
        messageBody = JSON.parse(record.body);
        taskToken = messageBody.taskToken;
        executionId = messageBody.executionId;
        county = messageBody.county || "unknown";

        console.log(`📋 Processing message with executionId: ${executionId}`);

        if (!taskToken) {
          throw new PrepareError("01016", "Missing taskToken in SQS message body");
        }

        // Emit IN_PROGRESS event to EventBridge
        await emitWorkflowEvent({
          executionId,
          county,
          status: "IN_PROGRESS",
        });

        const result = await processPrepare({
          input_s3_uri: messageBody.input_s3_uri,
          output_s3_uri_prefix: messageBody.output_s3_uri_prefix,
          executionId,
          taskToken,
        });

        county = result.county || county;

        console.log(`✅ Prepare succeeded for county: ${county}`);

        // Send success to Step Functions with taskToken for EventBridge
        await sendTaskSuccess(taskToken, {
          output_s3_uri: result.output_s3_uri,
          county,
          taskToken, // Include taskToken so state machine can pass to EventBridge
        });
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        // Use error code from PrepareError if available, otherwise default to "01002"
        const errorCode =
          error instanceof PrepareError ? error.code : "01002";
        console.error(
          `❌ Prepare processing failed [${errorCode}]: ${errorMessage}`,
        );

        // Send failure to Step Functions with taskToken in cause (JSON encoded)
        // State machine can extract taskToken from cause for EventBridge
        if (taskToken) {
          const causePayload = JSON.stringify({
            message: errorMessage,
            errorCode: errorCode,
            taskToken: taskToken,
            county: county,
            input_s3_uri: messageBody?.input_s3_uri,
          });
          await sendTaskFailure(taskToken, errorCode, causePayload);
        }
      }
    }

    // Return void for SQS - acknowledgment is implicit
    return;
  }

  // Direct invocation (legacy support)
  console.log("📞 Processing direct invocation");

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

  /** @type {DirectInvokeEvent} */
  const directEvent = /** @type {DirectInvokeEvent} */ (event);
  return processPrepare({
    input_s3_uri: directEvent.input_s3_uri,
    output_s3_uri_prefix: directEvent.output_s3_uri_prefix,
  });
};
