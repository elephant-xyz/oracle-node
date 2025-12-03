import { createHash } from "crypto";

export {
  sendTaskSuccess,
  sendTaskFailure,
  executeWithTaskToken,
} from "./task-token.mjs";

export {
  parseS3Uri,
  buildS3Uri,
  downloadS3Object,
  uploadToS3,
  s3,
} from "./s3-utils.mjs";

export { emitWorkflowEvent, createWorkflowError } from "./eventbridge.mjs";

/**
 * Compute a deterministic error hash from message, path, and county.
 * This is used to uniquely identify errors across executions.
 *
 * @param {string} message - Error message.
 * @param {string} path - Error path within payload.
 * @param {string} county - County identifier.
 * @returns {string} - SHA256 hash string.
 */
export function createErrorHash(message, path, county) {
  return createHash("sha256")
    .update(`${message}#${path}#${county}`, "utf8")
    .digest("hex");
}

/**
 * Ensure required environment variables are present.
 *
 * @param {string} name - Environment variable identifier.
 * @returns {string} - Resolved environment variable value.
 */
export function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

/**
 * Create a structured logger with consistent base fields.
 *
 * @param {Record<string, unknown>} baseFields - Base fields to include in every log entry.
 * @returns {(level: "info"|"error"|"debug", msg: string, details?: Record<string, unknown>) => void}
 */
export function createLogger(baseFields) {
  return (level, msg, details = {}) => {
    const entry = { ...baseFields, level, msg, ...details };
    if (level === "error") {
      console.error(entry);
    } else {
      console.log(entry);
    }
  };
}
