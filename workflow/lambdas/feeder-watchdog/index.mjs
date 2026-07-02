// Self-healing watchdog for the property-first seed-feeder.
//
// The seed-feeder occasionally stops self-requeuing mid-run (its S3 checkpoint
// stops advancing while its ESM stays Enabled). The documented recovery is to
// re-send the same feeder message — it resumes from the checkpoint (idempotent).
// This Lambda automates that: on each (scheduled) invocation it reads the
// checkpoint and, if the county is not yet exhausted and the checkpoint has gone
// stale, re-sends the baked feeder message. Cloud-side, so no laptop dependency.
//
// Config via env vars (deploy script bakes these in):
//   STATE_BUCKET, STATE_KEY  — feeder-state.json location
//   FEEDER_QUEUE_URL         — permit-harvest queue the feeder is sent to
//   FEEDER_MESSAGE           — exact feeder message JSON (from sender --dry-run)
//   STALE_SECONDS            — re-send if checkpoint older than this (default 900)
//
// Uses the AWS SDK v3 bundled in the nodejs22.x managed runtime (no node_modules).
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";

const s3 = new S3Client({});
const sqs = new SQSClient({});

const STATE_BUCKET = process.env.STATE_BUCKET;
const STATE_KEY = process.env.STATE_KEY;
const FEEDER_QUEUE_URL = process.env.FEEDER_QUEUE_URL;
const FEEDER_MESSAGE = process.env.FEEDER_MESSAGE;
const STALE_SECONDS = Number.parseInt(process.env.STALE_SECONDS ?? "900", 10);

/**
 * Re-send the feeder message to resume the run from its S3 checkpoint.
 * @param {string} reason - Why we are re-sending (logged).
 * @returns {Promise<void>} Resolves once SQS accepts the message.
 */
async function resendFeeder(reason) {
  const out = await sqs.send(
    new SendMessageCommand({
      QueueUrl: FEEDER_QUEUE_URL,
      MessageBody: FEEDER_MESSAGE,
    }),
  );
  console.log(
    JSON.stringify({ action: "RESEND", reason, messageId: out.MessageId }),
  );
}

/**
 * Scheduled entrypoint: resume the feeder only when its checkpoint is stale.
 * @returns {Promise<{status:string}>} Decision outcome for logs/metrics.
 */
export const handler = async () => {
  let state;
  try {
    const r = await s3.send(
      new GetObjectCommand({ Bucket: STATE_BUCKET, Key: STATE_KEY }),
    );
    state = JSON.parse(await r.Body.transformToString());
  } catch (err) {
    // Cannot read the checkpoint — re-send to be safe (idempotent).
    await resendFeeder(`state_unreadable:${err?.name ?? err}`);
    return { status: "resent_state_unreadable" };
  }

  if (state.sourceExhausted === true) {
    console.log(
      JSON.stringify({
        action: "NOOP",
        reason: "sourceExhausted",
        enqueued: state.enqueuedCount,
      }),
    );
    return { status: "done_noop" };
  }

  const updatedMs = Date.parse(state.updatedAt ?? "");
  const ageSeconds = Number.isFinite(updatedMs)
    ? Math.round((Date.now() - updatedMs) / 1000)
    : Number.POSITIVE_INFINITY;

  if (ageSeconds > STALE_SECONDS) {
    await resendFeeder(`stale_${ageSeconds}s_enqueued_${state.enqueuedCount}`);
    return { status: "resent_stale" };
  }

  console.log(
    JSON.stringify({
      action: "NOOP",
      reason: "fresh",
      ageSeconds,
      enqueued: state.enqueuedCount,
    }),
  );
  return { status: "fresh_noop" };
};
