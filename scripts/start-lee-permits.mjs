#!/usr/bin/env node
import { S3Client, ListObjectsV2Command, ListBucketsCommand } from "@aws-sdk/client-s3";
import { SQSClient, SendMessageBatchCommand, GetQueueUrlCommand, CreateQueueCommand } from "@aws-sdk/client-sqs";
import { CloudFormationClient, DescribeStacksCommand, ListStacksCommand } from "@aws-sdk/client-cloudformation";

/**
 * Parse an S3 URI s3://bucket/prefix into parts
 * @param {string} uri
 * @returns {{ bucket: string, key: string }}
 */
function parseS3Uri(uri) {
  const m = /^s3:\/\/([^/]+)\/(.*)$/.exec(uri);
  if (!m) throw new Error("Invalid S3 URI");
  return { bucket: m[1], key: m[2] };
}

/**
 * Simple argv parser: --key=value flags
 */
function parseArgs() {
  /** @type {Record<string, string|boolean>} */
  const args = {};
  for (const a of process.argv.slice(2)) {
    if (a.startsWith("--")) {
      const [k, v] = a.slice(2).split("=", 2);
      args[k] = v === undefined ? true : v;
    }
  }
  return args;
}

async function main() {
  const args = parseArgs();
  let input = String(args.input || args.prefix || "");
  let queueUrl = String(args.queue || args["queue-url"] || "");
  const region = String(args.region || process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || "us-east-1");
  const dryRun = args["dry-run"] === true || String(args["dry-run"]) === "true";
  const maxKeys = Math.max(1, Number(args["max-keys"] || 1000));
  const workflowQueueName = String(args["queue-name"] || "elephant-lee-permits-queue");
  const stackName = String(args.stack || args["stack-name"] || process.env.STACK_NAME || "");
  const createQueueFlag = args["create-queue"] === true || String(args["create-queue"]) === "true";

  const s3 = new S3Client({ region });
  const sqs = new SQSClient({ region });
  const cfn = new CloudFormationClient({ region });

  // If no stack is provided, attempt to auto-discover by scanning CFN stacks
  async function autoDiscoverStackName() {
    try {
      const list = await cfn.send(
        new ListStacksCommand({
          StackStatusFilter: [
            "CREATE_COMPLETE",
            "UPDATE_COMPLETE",
            "UPDATE_ROLLBACK_COMPLETE",
          ],
        }),
      );
      const summaries = list.StackSummaries || [];
      // Sort by last update/newest first
      summaries.sort((a, b) => {
        const ta = new Date(a.LastUpdatedTime || a.CreationTime || 0).getTime();
        const tb = new Date(b.LastUpdatedTime || b.CreationTime || 0).getTime();
        return tb - ta;
      });
      for (const s of summaries) {
        if (!s.StackName) continue;
        try {
          const d = await cfn.send(
            new DescribeStacksCommand({ StackName: s.StackName }),
          );
          const stack = (d.Stacks || [])[0];
          const outputs = stack?.Outputs || [];
          const hasEnv = outputs.some((o) => o.OutputKey === "EnvironmentBucketName");
          const hasWq = outputs.some((o) => o.OutputKey === "WorkflowQueueUrl");
          if (hasEnv && hasWq) {
            return s.StackName;
          }
        } catch {}
      }
    } catch {}
    return "";
  }

  // Autodiscover S3 input if not provided: prefer CloudFormation stack outputs
  if (!input) {
    let envBucketFromCfn = "";
    let resolvedStackName = stackName;
    if (!resolvedStackName) {
      resolvedStackName = await autoDiscoverStackName();
    }
    if (resolvedStackName) {
      try {
        const resp = await cfn.send(new DescribeStacksCommand({ StackName: resolvedStackName }));
        const stack = (resp.Stacks || [])[0];
        const outputs = stack?.Outputs || [];
        const envBucketOut = outputs.find((o) => o.OutputKey === "EnvironmentBucketName");
        if (envBucketOut?.OutputValue) envBucketFromCfn = envBucketOut.OutputValue;
      } catch {}
    }
    const base = process.env.OUTPUT_BASE_URI;
    if (base && /^s3:\/\//.test(base)) {
      input = base.replace(/\/$/, "/");
    } else if (envBucketFromCfn) {
      input = `s3://${envBucketFromCfn}/outputs/`;
    } else {
      // Fallback to discovering by naming convention
      const buckets = await s3.send(new ListBucketsCommand({}));
      const candidates = (buckets.Buckets || [])
        .map((b) => b.Name)
        .filter((n) => typeof n === "string")
        .filter((n) => n.startsWith("elephant-oracle-node-environmentbucket-"));
      if (!candidates.length) {
        throw new Error("No --input and no CFN outputs found. Provide --stack or --input.");
      }
      const bucketName = candidates[0];
      input = `s3://${bucketName}/outputs/`;
    }
  }

  // Autoresolve or create SQS queue if not provided
  if (!queueUrl) {
    // Prefer CFN output first
    let resolvedStackName = stackName;
    if (!resolvedStackName) {
      resolvedStackName = await autoDiscoverStackName();
    }
    if (resolvedStackName) {
      try {
        const resp = await cfn.send(new DescribeStacksCommand({ StackName: resolvedStackName }));
        const stack = (resp.Stacks || [])[0];
        const outputs = stack?.Outputs || [];
        let wfQueueOut = outputs.find((o) => o.OutputKey === "LeePermitsWorkflowQueueUrl");
        if (!wfQueueOut) wfQueueOut = outputs.find((o) => o.OutputKey === "WorkflowQueueUrl");
        if (wfQueueOut?.OutputValue) queueUrl = wfQueueOut.OutputValue;
      } catch {}
    }
    if (!queueUrl) {
      try {
        const resp = await sqs.send(new GetQueueUrlCommand({ QueueName: workflowQueueName }));
        queueUrl = resp.QueueUrl || "";
      } catch (e) {
        if (!createQueueFlag) {
          throw new Error("Queue not found. Pass --create-queue=true to create it or provide --queue URL.");
        }
        const create = await sqs.send(
          new CreateQueueCommand({
            QueueName: workflowQueueName,
            Attributes: {
              SqsManagedSseEnabled: "true",
              VisibilityTimeout: "30",
              MessageRetentionPeriod: "1209600"
            },
          }),
        );
        queueUrl = create.QueueUrl || "";
      }
    }
    if (!queueUrl) throw new Error("Failed to resolve or create workflow SQS queue");
  }

  const { bucket, key } = parseS3Uri(input);
  const prefix = key.replace(/\/*$/, "/");

  let continuationToken = undefined;
  let total = 0;
  const batch = [];

  const enqueueBatch = async () => {
    if (!batch.length) return;
    if (dryRun) {
      console.log(`DRY-RUN sending ${batch.length} messages`);
    } else {
      const chunks = [];
      for (let i = 0; i < batch.length; i += 10) chunks.push(batch.slice(i, i + 10));
      for (const entries of chunks) {
        await sqs.send(new SendMessageBatchCommand({ QueueUrl: queueUrl, Entries: entries }));
      }
    }
    total += batch.length;
    batch.length = 0;
  };

  do {
    const resp = await s3.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, ContinuationToken: continuationToken, MaxKeys: maxKeys }));
    const contents = resp.Contents || [];
    for (const obj of contents) {
      const keyStr = obj.Key || "";
      if (!/\/output\.zip$/.test(keyStr)) continue;
      const body = {
        s3: { bucket: { name: bucket }, object: { key: keyStr } },
      };
      const id = Buffer.from(keyStr).toString("base64").slice(0, 80);
      batch.push({ Id: id, MessageBody: JSON.stringify(body) });
      if (batch.length >= 100) await enqueueBatch();
    }
    continuationToken = resp.IsTruncated ? resp.NextContinuationToken : undefined;
  } while (continuationToken);

  await enqueueBatch();
  console.log(`Enqueued ${total} messages to ${queueUrl}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});


