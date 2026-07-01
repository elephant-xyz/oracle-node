#!/bin/bash
# Self-healing watchdog for the property-first seed-feeder.
#
# The seed-feeder occasionally stops self-requeuing mid-run (its checkpoint
# stops advancing while its ESM stays Enabled). The skills' documented recovery
# is "re-send the same feeder message" (idempotent — it resumes from the S3
# checkpoint). This automates that: poll the feeder-state checkpoint, and if it
# has gone stale (and the county is not yet exhausted), re-send via the county
# sender. Run detached (nohup + caffeinate) so it survives overnight.
#
# County-generic via env vars:
#   STATE_S3_URI   s3://.../permit-harvest/<jobId>/feeder-state.json
#   SENDER_CMD     command that re-sends ONE feeder message (resumes from checkpoint)
#   STALE_SECONDS  re-send if checkpoint older than this (default 360)
#   POLL_SECONDS   poll interval (default 120)
#   AWS_PROFILE / AWS_REGION
set -uo pipefail
: "${STATE_S3_URI:?set STATE_S3_URI}"
: "${SENDER_CMD:?set SENDER_CMD}"
STALE_SECONDS="${STALE_SECONDS:-360}"
POLL_SECONDS="${POLL_SECONDS:-120}"
# Cooldown after a re-send: do NOT re-send again until this elapses, even if still
# stale. Prevents piling up duplicate feeders (a pile runs concurrently and
# deadlocks the worker). A single re-send normally revives within a minute.
RESEND_COOLDOWN="${RESEND_COOLDOWN:-900}"
LAST_RESEND=0

echo "$(date -u +%H:%M:%S) watchdog start | state=$STATE_S3_URI stale=${STALE_SECONDS}s poll=${POLL_SECONDS}s cooldown=${RESEND_COOLDOWN}s"
while true; do
  STATE="$(aws s3 cp "$STATE_S3_URI" - 2>/dev/null)"
  if [ -z "$STATE" ]; then
    echo "$(date -u +%H:%M:%S) WARN: could not read state; re-sending to be safe"; eval "$SENDER_CMD" >/dev/null 2>&1
    sleep "$POLL_SECONDS"; continue
  fi
  read -r EXHAUSTED ENQUEUED AGE < <(echo "$STATE" | python3 -c "
import sys,json,datetime as dt
d=json.load(sys.stdin)
u=d.get('updatedAt');
age=999999
if u:
    try:
        t=dt.datetime.fromisoformat(u.replace('Z','+00:00'))
        age=int((dt.datetime.now(dt.timezone.utc)-t).total_seconds())
    except: pass
print(str(d.get('sourceExhausted')).lower(), d.get('enqueuedCount',0), age)
")
  if [ "$EXHAUSTED" = "true" ]; then
    echo "$(date -u +%H:%M:%S) DONE: sourceExhausted=true enqueued=$ENQUEUED — watchdog exiting"; exit 0
  fi
  NOW=$(date +%s)
  SINCE_RESEND=$(( NOW - LAST_RESEND ))
  if [ "$AGE" -gt "$STALE_SECONDS" ]; then
    if [ "$SINCE_RESEND" -ge "$RESEND_COOLDOWN" ]; then
      echo "$(date -u +%H:%M:%S) STALL: checkpoint ${AGE}s old (enqueued=$ENQUEUED) — RE-SENDING feeder"
      eval "$SENDER_CMD" 2>&1 | grep -o '"messageId":"[^"]*"' | head -1
      LAST_RESEND=$NOW
    else
      echo "$(date -u +%H:%M:%S) STALL but in cooldown (${SINCE_RESEND}s/${RESEND_COOLDOWN}s since last re-send) — waiting"
    fi
  else
    echo "$(date -u +%H:%M:%S) ok: enqueued=$ENQUEUED, checkpoint ${AGE}s old"
  fi
  sleep "$POLL_SECONDS"
done
