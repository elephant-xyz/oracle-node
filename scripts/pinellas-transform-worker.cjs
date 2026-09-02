"use strict";

const fs = require("fs");
const path = require("path");

const MAPPING_SCRIPT_NAMES = Object.freeze([
  "ownerMapping.js",
  "structureMapping.js",
  "layoutMapping.js",
  "utilityMapping.js",
]);

/**
 * @typedef {object} PinellasTransformResult
 * @property {string | null} propertyUsageType - `property.json` usage type.
 */

/**
 * County mapping scripts call `process.exit(1)` on failure. Re-throw so a
 * persistent worker can survive a single bad parcel.
 *
 * @param {number | undefined} code - Exit code.
 * @returns {never} Always throws.
 */
function throwInsteadOfExit(code) {
  const error = new Error(`PINELLAS_SCRIPT_EXIT_${code ?? 0}`);
  error.name = "PinellasScriptExit";
  throw error;
}

/**
 * Clear require cache for one county script so the next parcel re-runs it.
 * Leaves `cheerio` / `printHtml.js` cached.
 *
 * @param {string} scriptPath - Absolute script path.
 * @returns {void}
 */
function forgetScript(scriptPath) {
  const resolved = require.resolve(scriptPath);
  delete require.cache[resolved];
}

/**
 * Run Pinellas mapping scripts + `data_extractor.js` in this process.
 *
 * @param {string} scriptsDirectory - Folder containing the county `.js` files.
 * @param {string} workDir - Directory with `input.html` and seed JSON.
 * @returns {PinellasTransformResult} Usage type from `data/property.json`.
 */
function transformParcel(scriptsDirectory, workDir) {
  const previousCwd = process.cwd();
  const previousExit = process.exit;
  const previousLog = console.log;
  try {
    process.chdir(workDir);
    process.exit = /** @type {typeof process.exit} */ (throwInsteadOfExit);
    console.log = () => {};
    for (const name of [...MAPPING_SCRIPT_NAMES, "data_extractor.js"]) {
      const abs = path.join(scriptsDirectory, name);
      forgetScript(abs);
      require(abs);
    }
    const propertyPath = path.join(workDir, "data", "property.json");
    if (!fs.existsSync(propertyPath)) {
      throw new Error("data_extractor.js did not write data/property.json");
    }
    const propertyJson = JSON.parse(fs.readFileSync(propertyPath, "utf8"));
    return {
      propertyUsageType:
        typeof propertyJson.property_usage_type === "string"
          ? propertyJson.property_usage_type
          : null,
    };
  } finally {
    process.exit = previousExit;
    console.log = previousLog;
    process.chdir(previousCwd);
  }
}

/**
 * @param {unknown} message - IPC payload.
 * @returns {void}
 */
function handleIpc(message) {
  if (message === null || typeof message !== "object") return;
  const record =
    /** @type {{ type?: unknown, id?: unknown, scriptsDirectory?: unknown, workDir?: unknown }} */ (
      message
    );
  if (record.type !== "run") return;
  const id = record.id;
  const scriptsDirectory = record.scriptsDirectory;
  const workDir = record.workDir;
  if (typeof scriptsDirectory !== "string" || typeof workDir !== "string") {
    process.send?.({ type: "err", id, error: "invalid transform job" });
    return;
  }
  try {
    const result = transformParcel(scriptsDirectory, workDir);
    process.send?.({
      type: "ok",
      id,
      propertyUsageType: result.propertyUsageType,
    });
  } catch (error) {
    process.send?.({
      type: "err",
      id,
      error: error instanceof Error ? error.message : String(error),
    });
  }
}

module.exports = {
  MAPPING_SCRIPT_NAMES,
  transformParcel,
};

if (require.main === module) {
  if (typeof process.send === "function") {
    process.on("message", handleIpc);
    process.send({ type: "ready" });
  } else {
    const scriptsDirectory = process.argv[2];
    const workDir = process.argv[3];
    if (typeof scriptsDirectory !== "string" || typeof workDir !== "string") {
      throw new Error(
        "Usage: pinellas-transform-worker.cjs <scriptsDir> <workDir>",
      );
    }
    const result = transformParcel(scriptsDirectory, workDir);
    process.stdout.write(`${JSON.stringify(result)}\n`);
  }
}
