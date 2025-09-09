/**
 * Starter Lambda: triggered by SQS with BatchSize=1. Starts Express SFN synchronously.
 * Logs in structured JSON and never fails silently.
 */

import { SFNClient, StartSyncExecutionCommand } from "@aws-sdk/client-sfn";

/**
 * @typedef {Object} S3EventRecord
 * @property {{ name: string }} s3.bucket
 * @property {{ key: string }} s3.object
 */

/**
 * @typedef {Object} SqsEvent
 * @property {{ body: string }[]} Records
 */

const sfn = new SFNClient({});

/**
 * @param {SqsEvent} event
 * @returns {Promise<{status:string, executionArn?:string}>}
 */
export const handler = async (event) => {
    const logBase = { component: "starter", at: new Date().toISOString() };
    try {
        console.log(JSON.stringify({ ...logBase, level: "info", msg: "received", eventSize: event?.Records?.length ?? 0 }));
        if (!process.env.STATE_MACHINE_ARN) {
            throw new Error("STATE_MACHINE_ARN is required");
        }
        if (!event || !Array.isArray(event.Records) || event.Records.length !== 1) {
            throw new Error("Expect exactly one SQS record per invocation");
        }
        const bodyRaw = event.Records[0].body;
        const parsed = JSON.parse(bodyRaw);
        // Start the Express workflow synchronously
        const cmd = new StartSyncExecutionCommand({
            stateMachineArn: process.env.STATE_MACHINE_ARN,
            input: JSON.stringify(parsed),
        });
        const resp = await sfn.send(cmd);
        console.log(JSON.stringify({ ...logBase, level: "info", msg: "started", executionArn: resp.executionArn, status: resp.status }));
        if (resp.status !== "SUCCEEDED") {
            throw new Error(`Workflow did not succeed: ${resp.status}`);
        }
        return { status: "ok", executionArn: resp.executionArn };
    } catch (err) {
        console.error(JSON.stringify({ ...logBase, level: "error", msg: "failed", error: String(err) }));
        throw err;
    }
};


