import { mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { base32 } from "multiformats/bases/base32";
import { CID } from "multiformats/cid";
import { sha256, sha512 } from "multiformats/hashes/sha2";
import { equals as bytesEqual } from "uint8arrays/equals";

const DEFAULT_ROOT_CID =
  "bafkreicdfrzfiygzjaqrz4i2ao4yxspcxsksvbuljxx7ruqrp5m36kddxq";
const DEFAULT_CACHE_DIRECTORY = path.join(
  os.homedir(),
  ".elephant-cli",
  "schema-cache",
);
const DEFAULT_GATEWAYS = Object.freeze([
  "https://ipfs.filebase.io/ipfs/",
  "https://gateway.pinata.cloud/ipfs/",
]);
const HASHERS = Object.freeze({
  [sha256.code]: sha256,
  [sha512.code]: sha512,
});

/**
 * @typedef {object} SchemaCacheOptions
 * @property {string} rootCid - Trusted root schema CID.
 * @property {string} cacheDirectory - Elephant CLI schema-cache directory.
 * @property {readonly string[]} gateways - HTTPS IPFS gateway prefixes.
 * @property {number} concurrency - Maximum concurrent schema fetches.
 *
 * @typedef {object} CacheSummary
 * @property {string} rootCid - Verified root schema CID.
 * @property {string} cacheDirectory - Cache directory populated.
 * @property {number} schemasVerified - Number of CID-addressed schemas verified.
 * @property {number} bytesVerified - Total verified response bytes.
 */

/**
 * Return true for a non-array object.
 *
 * @param {unknown} value - Candidate value.
 * @returns {value is Record<string, unknown>} Whether the value is a JSON object.
 */
function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Verify bytes against a CID's declared multihash.
 *
 * @param {string} cidText - Expected CID.
 * @param {Uint8Array} bytes - Exact fetched response bytes.
 * @returns {Promise<void>}
 * @throws {Error} When the hash algorithm is unsupported or bytes do not match.
 */
export async function verifyCidBytes(cidText, bytes) {
  const cid = CID.parse(cidText);
  const hasher = HASHERS[cid.multihash.code];
  if (hasher === undefined) {
    throw new Error(
      `Unsupported schema CID hash algorithm: ${cid.multihash.code}`,
    );
  }
  const digest = await hasher.digest(bytes);
  if (!bytesEqual(digest.bytes, cid.multihash.bytes)) {
    throw new Error(
      `Schema bytes do not match ${cidText}; expected ${base32.encode(
        cid.multihash.bytes,
      )}, received ${base32.encode(digest.bytes)}`,
    );
  }
}

/**
 * Find schema CID references in a verified JSON schema.
 *
 * Only `cid` property values are followed. Child references therefore remain
 * anchored to the independently verified parent schema rather than to an
 * untrusted manifest or gateway listing.
 *
 * @param {unknown} value - Parsed verified schema.
 * @returns {string[]} Distinct child CIDs in first-seen order.
 */
export function collectReferencedSchemaCids(value) {
  /** @type {string[]} */
  const result = [];
  const seen = new Set();

  /**
   * @param {unknown} candidate - Current JSON value.
   * @returns {void}
   */
  function visit(candidate) {
    if (Array.isArray(candidate)) {
      for (const entry of candidate) visit(entry);
      return;
    }
    if (!isRecord(candidate)) return;
    for (const [key, entry] of Object.entries(candidate)) {
      if (
        key === "cid" &&
        typeof entry === "string" &&
        entry.length > 0 &&
        !seen.has(entry)
      ) {
        CID.parse(entry);
        seen.add(entry);
        result.push(entry);
      } else {
        visit(entry);
      }
    }
  }

  visit(value);
  return result;
}

/**
 * Fetch exact CID-addressed bytes from the first usable HTTPS gateway.
 *
 * @param {string} cid - Expected schema CID.
 * @param {readonly string[]} gateways - Gateway URL prefixes.
 * @returns {Promise<Uint8Array>} Bytes already verified against the CID.
 */
export async function fetchVerifiedSchemaBytes(cid, gateways) {
  /** @type {string[]} */
  const failures = [];
  for (const gateway of gateways) {
    try {
      const response = await fetch(`${gateway}${cid}`, {
        headers: { accept: "application/json" },
        signal: AbortSignal.timeout(20_000),
      });
      if (!response.ok) {
        failures.push(`${gateway}: HTTP ${response.status}`);
        continue;
      }
      const bytes = new Uint8Array(await response.arrayBuffer());
      await verifyCidBytes(cid, bytes);
      JSON.parse(new TextDecoder().decode(bytes));
      return bytes;
    } catch (error) {
      failures.push(
        `${gateway}: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }
  throw new Error(
    `Unable to fetch verified schema ${cid}: ${failures.join("; ")}`,
  );
}

/**
 * Populate Elephant CLI's local schema cache from a trusted root CID.
 *
 * Every file is written only after its exact bytes pass multihash validation.
 * Child schemas are discovered exclusively from already verified parent bytes.
 *
 * @param {SchemaCacheOptions} options - Cache root, gateways, and concurrency.
 * @returns {Promise<CacheSummary>} Verified cache metrics.
 */
export async function prefetchSchemaCache(options) {
  await mkdir(options.cacheDirectory, { recursive: true });
  const pending = [options.rootCid];
  const queued = new Set(pending);
  let schemasVerified = 0;
  let bytesVerified = 0;

  while (pending.length > 0) {
    const batch = pending.splice(0, options.concurrency);
    const results = await Promise.all(
      batch.map(async (cid) => {
        const bytes = await fetchVerifiedSchemaBytes(cid, options.gateways);
        const parsed = JSON.parse(new TextDecoder().decode(bytes));
        await writeFile(
          path.join(options.cacheDirectory, `${cid}.json`),
          bytes,
        );
        return {
          bytes: bytes.byteLength,
          children: collectReferencedSchemaCids(parsed),
        };
      }),
    );
    for (const result of results) {
      schemasVerified += 1;
      bytesVerified += result.bytes;
      for (const childCid of result.children) {
        if (queued.has(childCid)) continue;
        queued.add(childCid);
        pending.push(childCid);
      }
    }
  }

  return {
    rootCid: options.rootCid,
    cacheDirectory: options.cacheDirectory,
    schemasVerified,
    bytesVerified,
  };
}

/**
 * Parse cache-prefetch CLI arguments.
 *
 * @param {readonly string[]} argv - Arguments after the script path.
 * @returns {SchemaCacheOptions} Validated options.
 */
export function parseCliOptions(argv) {
  /** @type {SchemaCacheOptions} */
  const options = {
    rootCid: DEFAULT_ROOT_CID,
    cacheDirectory: DEFAULT_CACHE_DIRECTORY,
    gateways: DEFAULT_GATEWAYS,
    concurrency: 8,
  };
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (typeof value !== "string" || value.startsWith("--")) {
      throw new Error(`Missing value for ${flag}`);
    }
    if (flag === "--root-cid") options.rootCid = value;
    else if (flag === "--cache-directory") options.cacheDirectory = value;
    else if (flag === "--concurrency") {
      options.concurrency = Number.parseInt(value, 10);
    } else {
      throw new Error(`Unknown option: ${flag}`);
    }
  }
  CID.parse(options.rootCid);
  if (
    !Number.isInteger(options.concurrency) ||
    options.concurrency < 1 ||
    options.concurrency > 32
  ) {
    throw new Error("--concurrency must be an integer from 1 through 32");
  }
  return options;
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const summary = await prefetchSchemaCache(
    parseCliOptions(process.argv.slice(2)),
  );
  console.log(JSON.stringify(summary, null, 2));
}
