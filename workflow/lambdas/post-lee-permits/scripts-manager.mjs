import { promises as fs } from "fs";
import path from "path";
import crypto from "crypto";
import { HeadObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";

/**
 * @typedef {import("@aws-sdk/client-s3").S3Client} S3Client
 */

/**
 * @typedef {object} TransformScriptsManagerOptions
 * @property {S3Client} s3Client - Shared AWS SDK S3 client instance.
 * @property {string} persistentCacheRoot - Root directory used to store cached transform scripts.
 */

/**
 * @typedef {object} EnsureScriptsParams
 * @property {string} countyName - County identifier used for cache segregation and remote key resolution.
 * @property {string} transformBucket - Name of the S3 bucket containing the transform script archives.
 * @property {string} transformKey - Object key within the transform bucket for the targeted county zip.
 * @property {(level: "info"|"error"|"debug", msg: string, details?: Record<string, unknown>) => void} log - Structured logger.
 */

/**
 * @typedef {object} EnsureScriptsResult
 * @property {string} scriptsZipPath - Absolute path to the cached transform scripts zip.
 * @property {string} md5 - Remote ETag-derived MD5 hash value for the cached archive.
 */

/**
 * Create a transform scripts manager backed by a closure to remember previously downloaded archives.
 *
 * @param {TransformScriptsManagerOptions} options - Manager configuration options.
 * @returns {{ ensureScriptsForCounty: (params: EnsureScriptsParams) => Promise<EnsureScriptsResult> }}
 */
export function createTransformScriptsManager({
  s3Client,
  persistentCacheRoot,
}) {
  /** @type {Map<string, EnsureScriptsResult>} */
  const countyScriptState = new Map();

  /**
   * Ensure a directory exists on disk.
   *
   * @param {string} directoryPath - Absolute directory path that must be present.
   * @returns {Promise<void>}
   */
  const ensureDirectory = async (directoryPath) => {
    await fs.mkdir(directoryPath, { recursive: true });
  };

  /**
   * Compute the MD5 checksum for an existing file.
   *
   * @param {string} filePath - Absolute path to the file to hash.
   * @returns {Promise<string>} - Lowercase hexadecimal MD5 digest.
   */
  const computeFileMd5 = async (filePath) => {
    const fileBuffer = await fs.readFile(filePath);
    return crypto.createHash("md5").update(fileBuffer).digest("hex");
  };

  /**
   * Determine whether a local cache entry matches the latest remote hash.
   *
   * @param {string} countyName - County identifier.
   * @param {string} expectedMd5 - MD5 hash returned by the remote object metadata.
   * @param {string} localPath - Absolute path to the cached archive.
   * @returns {Promise<EnsureScriptsResult | null>}
   */
  const validateLocalCache = async (countyName, expectedMd5, localPath) => {
    const cachedEntry = countyScriptState.get(countyName);
    if (cachedEntry && cachedEntry.md5 === expectedMd5) {
      try {
        const stat = await fs.stat(localPath);
        if (stat.isFile()) {
          return cachedEntry;
        }
      } catch {
        // Continue to full validation flow below.
      }
    }

    try {
      const stat = await fs.stat(localPath);
      if (!stat.isFile()) {
        return null;
      }
      const localMd5 = await computeFileMd5(localPath);
      if (localMd5 === expectedMd5) {
        const result = { scriptsZipPath: localPath, md5: expectedMd5 };
        countyScriptState.set(countyName, result);
        return result;
      }
    } catch {
      return null;
    }

    return null;
  };

  return {
    /**
     * Ensure the latest transform scripts zip for a county is available locally.
     *
     * @param {EnsureScriptsParams} params - Operational parameters.
     * @returns {Promise<EnsureScriptsResult>} - Details about the cached zip file.
     */
    async ensureScriptsForCounty({
      countyName,
      transformBucket,
      transformKey,
      log,
    }) {
      // Normalize county name to lowercase for consistent caching and S3 operations
      const normalizedCountyName = countyName.toLowerCase();

      await ensureDirectory(persistentCacheRoot);
      const countyCacheDir = path.join(
        persistentCacheRoot,
        normalizedCountyName,
      );
      await ensureDirectory(countyCacheDir);

      const localZipPath = path.join(
        countyCacheDir,
        `${normalizedCountyName}.zip`,
      );

      let remoteMd5;
      try {
        const headResponse = await s3Client.send(
          new HeadObjectCommand({ Bucket: transformBucket, Key: transformKey }),
        );
        remoteMd5 = headResponse.ETag?.replace(/"/g, "");
        if (!remoteMd5) {
          throw new Error("HeadObject returned empty ETag");
        }
      } catch (error) {
        throw new Error(
          `Unable to locate county transform zip for ${countyName}: ${error}. Trying ${transformKey}`,
        );
      }

      const validCache = await validateLocalCache(
        normalizedCountyName,
        remoteMd5,
        localZipPath,
      );
      if (validCache) {
        return validCache;
      }

      log("info", "download_transform_zip", {
        transform_bucket: transformBucket,
        transform_key: transformKey,
      });

      const remoteObject = await s3Client.send(
        new GetObjectCommand({ Bucket: transformBucket, Key: transformKey }),
      );
      const remoteBytes = await remoteObject.Body?.transformToByteArray();
      if (!remoteBytes) {
        throw new Error(
          `Failed to download county transform from S3 for ${countyName}`,
        );
      }

      await fs.writeFile(localZipPath, Buffer.from(remoteBytes));
      const downloadedHash = await computeFileMd5(localZipPath);
      if (downloadedHash !== remoteMd5) {
        throw new Error(
          `Downloaded transform hash mismatch for ${countyName}; expected ${remoteMd5}, got ${downloadedHash}`,
        );
      }

      const result = { scriptsZipPath: localZipPath, md5: remoteMd5 };
      countyScriptState.set(normalizedCountyName, result);
      return result;
    },
  };
}
