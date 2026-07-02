#!/bin/bash
# Self-healing watchdog for the property-first seed-feeder.
#
# The seed-feeder occasionally stops self-requeuing mid-run (its checkpoint
# stops advancing while its ESM stays Enabled). The skills' documented recovery
# is "re-send the same feeder message" (idempotent — it resumes from the S3
# checkpoint). This automates that: poll the feeder-state checkpoint, and if it
# has gone stale (and the county is not yet exhausted), re-send via the county
# sender. Run detached so it survives overnight (macOS: nohup + caffeinate;
# Windows/Linux: nohup or a background job + keep the machine awake).
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

# Detect a WORKING python interpreter. Windows registers "python3"/"python" App
# Execution aliases that merely print an install prompt and exit non-zero, so we
# execution-test each candidate rather than trusting `command -v`. Override with PYBIN.
if [ -z "${PYBIN:-}" ]; then
  for _c in python3 python "py -3"; do
    if $_c -c "import sys" >/dev/null 2>&1; then PYBIN="$_c"; break; fi
  done
fi
if [ -z "${PYBIN:-}" ]; then
  echo "$(date -u +%H:%M:%S) FATAL: no working python interpreter found (tried python3, python, py -3). Set PYBIN." >&2
  exit 1
fi

echo "$(date -u +%H:%M:%S) watchdog start | state=$STATE_S3_URI stale=${STALE_SECONDS}s poll=${POLL_SECONDS}s cooldown=${RESEND_COOLDOWN}s py='$PYBIN'"
while true; do
  STATE="$(aws s3 cp "$STATE_S3_URI" - 2>/dev/null)"
  if [ -z "$STATE" ]; then
    # Checkpoint not present yet (feeder still spinning up) OR a transient read error.
    # Do NOT create/re-send — that would spawn a duplicate feeder. Just wait and re-check.
    echo "$(date -u +%H:%M:%S) WARN: checkpoint not readable yet — waiting (not re-sending)"
    sleep "$POLL_SECONDS"; continue
  fi
  read -r EXHAUSTED ENQUEUED AGE < <(echo "$STATE" | $PYBIN -c "
import sys,json,datetime as dt
d=json.load(sys.stdin)
u=d.get('updatedAt') or d.get('lastUpdatedAt') or d.get('updated_at');
age=999999
if u:
    try:
        t=dt.datetime.fromisoformat(u.replace('Z','+00:00'))
        age=int((dt.datetime.now(dt.timezone.utc)-t).total_seconds())
    except: pass
print(str(d.get('sourceExhausted')).lower(), d.get('enqueuedCount',0), age)
" | tr -d '\r')
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
