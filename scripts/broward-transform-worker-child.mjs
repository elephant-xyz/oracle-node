#!/usr/bin/env node

/**
 * Long-lived isolated transform worker for local Broward ingestion.
 *
 * Each child has its own TMPDIR, preventing Elephant CLI's fact-sheet
 * generator from racing with other concurrent transforms. Query-data-only
 * children register a fail-closed in-memory loader before importing the CLI;
 * normal publication workers use the unmodified public transform.
 */

import { register } from "node:module";

import {
  markQueryDataOnlyArtifact,
  QUERY_DATA_ONLY_MODE,
} from "./broward-query-data-only.mjs";

const artifactMode = process.env.BROWARD_ARTIFACT_MODE;
if (artifactMode === QUERY_DATA_ONLY_MODE) {
  register(
    new URL("./broward-query-data-only-loader.mjs", import.meta.url),
    import.meta.url,
  );
}
const { transform } = await import("@elephant-xyz/cli/lib");

/**
 * @typedef {object} TransformWorkerRequest
 * @property {number} requestId - Parent correlation identifier.
 * @property {string} inputZipPath - County transform input ZIP.
 * @property {string} outputZipPath - Transformed output ZIP.
 * @property {string} scriptsZipPath - Broward scripts ZIP.
 * @property {string} workingDirectory - Per-parcel working directory.
 * @property {string} folio - Canonical Broward folio for artifact classification.
 */

/**
 * Return true for a valid parent request.
 *
 * @param {unknown} value - IPC message.
 * @returns {value is TransformWorkerRequest} Whether required fields exist.
 */
function isTransformWorkerRequest(value) {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const candidate = /** @type {Record<string, unknown>} */ (value);
  return (
    typeof candidate.requestId === "number" &&
    typeof candidate.inputZipPath === "string" &&
    typeof candidate.outputZipPath === "string" &&
    typeof candidate.scriptsZipPath === "string" &&
    typeof candidate.workingDirectory === "string" &&
    typeof candidate.folio === "string"
  );
}

process.on("message", async (message) => {
  if (!isTransformWorkerRequest(message)) return;
  try {
    const result = await transform({
      inputZip: message.inputZipPath,
      outputZip: message.outputZipPath,
      scriptsZip: message.scriptsZipPath,
      cwd: message.workingDirectory,
    });
    if (result.success && artifactMode === QUERY_DATA_ONLY_MODE) {
      markQueryDataOnlyArtifact(message.outputZipPath, message.folio);
    }
    process.send?.({
      requestId: message.requestId,
      success: result.success,
      error:
        result.scriptFailure?.stderr ??
        result.error ??
        (result.success ? null : "Unknown transform failure"),
    });
  } catch (error) {
    process.send?.({
      requestId: message.requestId,
      success: false,
      error: error instanceof Error ? error.message : String(error),
    });
  }
});
