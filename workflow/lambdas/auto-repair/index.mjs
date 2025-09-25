import { generateTransform } from "@elephant-xyz/cli/lib";
import {
    createLogEntry,
    acquireLock,
    releaseLockAndResolve,
    findRecordsForReprocessing,
    downloadAndExtractScripts,
    parseS3Uri,
    invokePostProcessor,
    invokeSubmitProcessor
} from "@elephant/shared";

/**
 * @typedef {Object} DynamoDBStreamRecord
 * @property {string} eventName - The type of event (INSERT, MODIFY, REMOVE)
 * @property {Object} dynamodb - DynamoDB record data
 * @property {Object} dynamodb.NewImage - The new image of the record
 * @property {Object} dynamodb.OldImage - The old image of the record
 */

/**
 * @typedef {Object} LambdaEvent
 * @property {DynamoDBStreamRecord[]} Records - Array of DynamoDB stream records
 */

/**
 * @typedef {Object} LambdaContext
 * @property {string} functionName - The name of the Lambda function
 * @property {string} awsRequestId - The AWS request ID
 */

/**
 * Main handler for the auto-repair lambda function
 * @param {LambdaEvent} event - DynamoDB stream event
 * @param {LambdaContext} context - Lambda context
 * @returns {Promise<{statusCode: number, body: string}>}
 */
export async function handler(event, context) {
    const log = (level, msg, details = {}) => console.log(JSON.stringify(createLogEntry('auto-repair', level, msg, { ...details, requestId: context.awsRequestId })));

    try {
        log('info', 'auto_repair_started', { recordCount: event.Records?.length || 0 });

        // Process each record in the stream
        for (const record of event.Records || []) {
            if (record.eventName !== 'INSERT') {
                log('debug', 'skipping_non_insert_event', { eventName: record.eventName });
                continue;
            }

            const newImage = record.dynamodb?.NewImage;
            if (!newImage) {
                log('debug', 'skipping_record_without_new_image');
                continue;
            }

            // Check if this is an ErrorType record (not ErrorEntry)
            const pk = newImage.pk?.S;
            const sk = newImage.sk?.S;
            const locked = newImage.locked?.BOOL;
            const resolved = newImage.resolved?.BOOL;

            if (!pk || !sk || !pk.startsWith('ERROR#') || sk !== pk) {
                log('debug', 'skipping_non_error_type_record', { pk, sk });
                continue;
            }

            if (locked || resolved) {
                log('debug', 'skipping_locked_or_resolved_error', { pk, locked, resolved });
                continue;
            }

            // This is a new ErrorType record that needs processing
            // PK format: ERROR#countyName#errorHash
            const pkParts = pk.split('#');
            if (pkParts.length >= 3) {
                const countyName = pkParts[1];
                const errorHash = pkParts[2];
                await processErrorType(log, errorHash, countyName);
            } else {
                log('error', 'invalid_pk_format', { pk });
            }
        }

        log('info', 'auto_repair_completed_successfully');
        return {
            statusCode: 200,
            body: JSON.stringify({ message: 'Auto-repair completed successfully' })
        };

    } catch (error) {
        log('error', 'auto_repair_failed', { error: error.message, stack: error.stack });
        return {
            statusCode: 500,
            body: JSON.stringify({ error: error.message })
        };
    }
}

/**
 * Process an individual error type
 * @param {Function} log - Logging function
 * @param {string} errorHash - The error hash to process
 * @param {string} countyName - The county name for the error
 */
async function processErrorType(log, errorHash, countyName) {
    const tableName = process.env.ERRORS_TABLE_NAME;
    const transformS3Prefix = process.env.TRANSFORM_S3_PREFIX;
    const postProcessorFunction = process.env.POST_PROCESSOR_FUNCTION_NAME;
    const submitFunction = process.env.SUBMIT_FUNCTION_NAME;

    if (!tableName || !transformS3Prefix || !postProcessorFunction || !submitFunction) {
        throw new Error('Missing required environment variables');
    }

    try {
        // Find all records with this error hash for this county
        const records = await findRecordsForReprocessing(tableName, `ERROR#${countyName}#${errorHash}`, countyName);
        if (records.length === 0) {
            log('warn', 'no_records_found_for_error_hash', { errorHash, countyName });
            return;
        }

        // Get the first record to extract error information
        const firstRecord = records[0];
        const errorMessage = firstRecord.errorMessage;

        log('info', 'processing_error_type', {
            errorHash,
            errorMessage,
            countyName,
            recordCount: records.length
        });

        // Acquire lock on the error type
        await acquireLock(tableName, errorMessage, countyName);
        log('info', 'lock_acquired', { errorHash, countyName });

        try {
            // Download and extract scripts for this county
            const scriptsKey = `${transformS3Prefix}/${countyName}/scripts.zip`;
            const { bucket: scriptsBucket } = parseS3Uri(transformS3Prefix.replace(/\/$/, ''));

            const scriptsPath = await downloadAndExtractScripts(scriptsBucket, scriptsKey);
            log('info', 'scripts_downloaded', { scriptsPath });

            // Use generateTransform to fix the error
            const generateResult = await generateTransform({
                scriptsZip: scriptsPath,
                silent: true
            });

            if (!generateResult.success) {
                throw new Error(`generateTransform failed: ${generateResult.error}`);
            }

            log('info', 'transform_scripts_generated', { outputZipPath: generateResult.outputZipPath });

            // TODO: Upload the new scripts zip to S3 and update the manifest

            // Reprocess all records with this error
            await reprocessRecords(log, records, postProcessorFunction, submitFunction);

            // Mark the error as resolved
            await releaseLockAndResolve(tableName, errorMessage, countyName);
            log('info', 'error_resolved', { errorHash, countyName });

        } catch (processingError) {
            log('error', 'error_processing_failed', {
                errorHash,
                error: processingError.message,
                stack: processingError.stack
            });
            // Don't release the lock if processing failed - let it retry later
            throw processingError;
        }

    } catch (error) {
        log('error', 'failed_to_process_error_type', {
            errorHash,
            error: error.message,
            stack: error.stack
        });
        throw error;
    }
}

/**
 * Reprocess all records with the same error
 * @param {Function} log - Logging function
 * @param {Array} records - Array of error records to reprocess
 * @param {string} postProcessorFunction - Name of the post-processor function
 * @param {string} submitFunction - Name of the submit function
 */
async function reprocessRecords(log, records, postProcessorFunction, submitFunction) {
    log('info', 'starting_record_reprocessing', { recordCount: records.length });

    const reprocessedItems = [];

    for (const record of records) {
        try {
            log('info', 'reprocessing_record', {
                countyName: record.countyName,
                transformedZip: record.transformedZip
            });

            // Parse the transformed zip S3 URI
            const { bucket, key } = parseS3Uri(record.transformedZip);

            // Construct S3 event-like payload
            const s3Event = {
                bucket: { name: bucket },
                object: { key: key }
            };

            // TODO: Need to reconstruct prepare output and seed URI from the record
            // For now, we'll need to enhance the error record structure to include this information

            // Invoke post-processor
            const postResult = await invokePostProcessor(postProcessorFunction, {
                s3: s3Event,
                prepare: {}, // TODO: reconstruct from record
                seed_output_s3_uri: '' // TODO: reconstruct from record
            });

            if (postResult.transactionItems && postResult.transactionItems.length > 0) {
                reprocessedItems.push(...postResult.transactionItems);
            }

            log('info', 'record_reprocessed_successfully', {
                countyName: record.countyName,
                transactionItemCount: postResult.transactionItems?.length || 0
            });

        } catch (recordError) {
            log('error', 'failed_to_reprocess_record', {
                countyName: record.countyName,
                error: recordError.message
            });
            // Continue with other records even if one fails
        }
    }

    // Submit all reprocessed items
    if (reprocessedItems.length > 0) {
        log('info', 'submitting_reprocessed_items', { itemCount: reprocessedItems.length });

        await invokeSubmitProcessor(submitFunction, reprocessedItems, {
            // TODO: Add submission parameters from environment or config
        });

        log('info', 'reprocessed_items_submitted_successfully', { itemCount: reprocessedItems.length });
    } else {
        log('warn', 'no_items_to_submit_after_reprocessing');
    }
}
