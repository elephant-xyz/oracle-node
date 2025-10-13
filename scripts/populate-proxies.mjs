#!/usr/bin/env node

/**
 * Populate DynamoDB ProxyRotation table with proxies from a file
 *
 * Usage:
 *   node scripts/populate-proxies.mjs <table-name> <proxy-file>
 */

import { DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { createHash } from "crypto";
import { readFile } from "fs/promises";

const client = new DynamoDBClient({});

/**
 * Generate a hash for the proxy URL to use as the proxy ID
 * @param {string} proxyUrl - The proxy URL
 * @returns {string} SHA256 hash of the proxy URL
 */
function generateProxyId(proxyUrl) {
  return createHash("sha256").update(proxyUrl).digest("hex");
}

/**
 * Validate proxy URL format (username:password@ip:port)
 * @param {string} proxyUrl - The proxy URL to validate
 * @returns {boolean} True if valid format
 */
function isValidProxyFormat(proxyUrl) {
  return /^[^:]+:[^@]+@[^:]+:[0-9]+$/.test(proxyUrl);
}

/**
 * Mask proxy URL for logging (hide password)
 * @param {string} proxyUrl - The proxy URL
 * @returns {string} Masked proxy URL
 */
function maskProxyUrl(proxyUrl) {
  const colonIndex = proxyUrl.indexOf(":");
  if (colonIndex === -1) return "***";
  return `${proxyUrl.substring(0, colonIndex)}:***@...`;
}

/**
 * Add a proxy to DynamoDB
 * @param {string} tableName - DynamoDB table name
 * @param {string} proxyUrl - Proxy URL in format username:password@ip:port
 * @param {number} initialTime - Initial lastUsedTime timestamp
 * @returns {Promise<void>}
 */
async function addProxy(tableName, proxyUrl, initialTime) {
  const proxyId = generateProxyId(proxyUrl);

  console.log(`[INFO] Adding proxy: ${proxyId} (${maskProxyUrl(proxyUrl)})`);

  const command = new PutItemCommand({
    TableName: tableName,
    Item: {
      proxyId: { S: proxyId },
      proxyUrl: { S: proxyUrl },
      constantKey: { S: "PROXY" },
      lastUsedTime: { N: String(initialTime) },
      failed: { BOOL: false },
    },
  });

  await client.send(command);
}

/**
 * Main function to populate proxies from file
 * @param {string} tableName - DynamoDB table name
 * @param {string} proxyFilePath - Path to file containing proxy URLs (one per line)
 * @returns {Promise<void>}
 */
async function populateProxies(tableName, proxyFilePath) {
  console.log(`[INFO] Populating proxy rotation table: ${tableName}`);

  // Read the proxy file
  const fileContent = await readFile(proxyFilePath, "utf-8");
  const lines = fileContent.split("\n");

  let proxyCount = 0;
  const initialTime = 0; // Start with timestamp 0 so all proxies are equally available initially
  const errors = [];

  for (const line of lines) {
    const proxyUrl = line.trim();

    // Skip empty lines and comments
    if (!proxyUrl || proxyUrl.startsWith("#")) {
      continue;
    }

    // Validate proxy format
    if (!isValidProxyFormat(proxyUrl)) {
      console.warn(
        `[WARN] Invalid proxy format (expected username:password@ip:port): ${proxyUrl}`,
      );
      continue;
    }

    try {
      await addProxy(tableName, proxyUrl, initialTime);
      proxyCount++;
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : String(error);
      console.error(
        `[ERROR] Failed to add proxy ${maskProxyUrl(proxyUrl)}: ${errorMsg}`,
      );
      errors.push({ proxyUrl: maskProxyUrl(proxyUrl), error: errorMsg });
    }
  }

  if (proxyCount === 0) {
    console.warn(`[WARN] No valid proxies found in ${proxyFilePath}`);
  } else {
    console.log(
      `[INFO] Successfully populated ${proxyCount} proxies into DynamoDB table`,
    );
  }

  if (errors.length > 0) {
    console.error(`[ERROR] Failed to add ${errors.length} proxies`);
    process.exit(1);
  }
}

// Parse command line arguments
const args = process.argv.slice(2);

if (args.length !== 2) {
  console.error("Usage: node populate-proxies.mjs <table-name> <proxy-file>");
  process.exit(1);
}

const [tableName, proxyFilePath] = args;

// Run the script
populateProxies(tableName, proxyFilePath)
  .then(() => {
    console.log("[INFO] Done!");
    process.exit(0);
  })
  .catch((error) => {
    console.error(
      "[ERROR]",
      error instanceof Error ? error.message : String(error),
    );
    process.exit(1);
  });
