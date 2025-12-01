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
  uploadContentToS3,
  s3,
} from "./s3-utils.mjs";

export { emitWorkflowEvent, createWorkflowError } from "./eventbridge.mjs";

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
