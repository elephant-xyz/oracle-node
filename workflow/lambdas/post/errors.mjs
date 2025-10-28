/**
 * @typedef ErrorRecord
 * @type {object}
 * @property {string} PK - Primary key for the DynamoDB table. SHA256 hash of the error message and error path. in the format of "ERROR#{SHA256}"
 * @property {string} SK - Secondary key for the DynamoDB table.in the format of "ERROR#{SHA256}"
 * @property {string} county - County identifier.
 * @property {string} error - Error message.
 * @property {string} errorPath - Path to the error location in the JSON.
 * @property {string} timestamp - ISO timestamp of the error.
 * @property {number} count - Number of times the error has occurred.
 * @property {string} GS1PK - GS1 PK for the error. in the format of "TYPE#ERORR"
 * @property {string} GS1SK - GS1 SK for the error. Represents count of the error and the hash. in the format of "COUNT#{COUNT}#HASH"
 */

/**
 * @typedef {object} FailedExecution
 * @property {string} PK - Primary key for the DynamoDB table. Execution ID. in the format of "EXECUTION#{EXECUTION_ID}"
 * @property {string} SK - Secondary key for the DynamoDB table. Represents the execution error. in the format of "EXECUTION#{EXECUTION_ID}"
 * @property {string} event - Input event used to start the execution.
 * @property {string} county - County identifier.
 * @property {string} errorsS3Uri - S3 URI for the errors csv file.
 * @property {number} count - Count of the errors in this execution.
 */

/**
 * @typedef {('failed' | 'maybeSolved' | 'solved')} ErrorStatus
 */

/**
 * @typedef {object} ExetuionError
 * @property {string} PK - Primary key for the DynamoDB table. Same as the PK of the ErrorRecord. In the format of "ERROR#{SHA256}"
 * @property {string} SK - Secondary key for the DynamoDB table. Represent link to the execution error. int the format of "EXECUTION#{EXECUTION_ID}"
 * @property {ErrorStatus} status - Status of the error.
 * @property {string} error - Error identifier.
 * @property {string} executionID - Execution ID.
 */
