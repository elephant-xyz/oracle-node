#!/usr/bin/env bash
#
# Parameterized, idempotent, per-county deploy for the seed-feeder watchdog.
#
# Provisions a dedicated Lambda + IAM role + EventBridge schedule that re-sends a
# county's seed-feeder message whenever its S3 checkpoint goes stale. Reproduces
# exactly the Orange watchdog that was first stood up by hand, so any county gets
# the same self-healing behaviour with one command.
#
# Each county gets its OWN role/Lambda/rule (no cross-county reuse) so lifecycles
# stay independent and we avoid the AccessDenied hit when reusing another county's
# role. Re-running is safe: existing resources are updated in place.
#
# Requires: AWS_PROFILE and AWS_REGION in the environment, node >= 22, python3,
# zip, and the AWS CLI v2. Run from anywhere — paths resolve off this script.
#
set -euo pipefail

# --- Fixed infrastructure constants (shared across counties) ------------------
BUCKET="elephant-oracle-node-environmentbucket-mmsoo3xbdi80"
FEEDER_QUEUE_NAME="elephant-oracle-node-permit-harvest-queue"

# --- Locations ----------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Repo root = three levels up from workflow/lambdas/feeder-watchdog.
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

# --- Defaults -----------------------------------------------------------------
COUNTY=""
JOB_ID=""
STALE_SECONDS="900"
SENDER_SCRIPT=""
DRY_RUN="false"

usage() {
  cat <<'EOF'
Usage:
  AWS_PROFILE=<profile> AWS_REGION=<region> \
    bash workflow/lambdas/feeder-watchdog/deploy.sh \
      --county <Name> --job-id <fixed-jobId> [options]

Required:
  --county <Name>          County display name, e.g. "Orange" or "Palm Beach".
  --job-id <id>            Fixed run id, e.g. orange-property-first-seed-all-20260702.
                           Must match the id the feeder was launched with.

Options:
  --stale-seconds <N>      Re-send if checkpoint older than N seconds. Default: 900.
                           Raise (e.g. 1200) when a county is backpressure-gated so
                           expected pauses do not trigger spurious re-sends.
  --sender-script <path>   Feeder sender used to derive the exact message.
                           Default: scripts/send-<county-key>-seed-feeder.mjs
                           (county-key = county lowercased, spaces -> hyphens).
  --dry-run                Print the resolved names/env/policy JSON and make NO changes.
  --help                   Show this help.

Environment (required, not passed as flags):
  AWS_PROFILE, AWS_REGION
EOF
}

# --- Arg parsing --------------------------------------------------------------
while [ "$#" -gt 0 ]; do
  case "$1" in
    --county)        COUNTY="${2:?--county needs a value}"; shift 2 ;;
    --job-id)        JOB_ID="${2:?--job-id needs a value}"; shift 2 ;;
    --stale-seconds) STALE_SECONDS="${2:?--stale-seconds needs a value}"; shift 2 ;;
    --sender-script) SENDER_SCRIPT="${2:?--sender-script needs a value}"; shift 2 ;;
    --dry-run)       DRY_RUN="true"; shift ;;
    --help|-h)       usage; exit 0 ;;
    *) echo "ERROR: unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

# --- Validate inputs ----------------------------------------------------------
[ -n "$COUNTY" ] || { echo "ERROR: --county is required" >&2; exit 2; }
[ -n "$JOB_ID" ] || { echo "ERROR: --job-id is required" >&2; exit 2; }
[ -n "${AWS_PROFILE:-}" ] || { echo "ERROR: AWS_PROFILE must be set in the environment" >&2; exit 2; }
[ -n "${AWS_REGION:-}" ] || { echo "ERROR: AWS_REGION must be set in the environment" >&2; exit 2; }
case "$STALE_SECONDS" in
  ''|*[!0-9]*) echo "ERROR: --stale-seconds must be a positive integer" >&2; exit 2 ;;
esac

# --- Derive county-key and resource names -------------------------------------
# county-key: lowercase, spaces -> hyphens (e.g. "Palm Beach" -> "palm-beach").
COUNTY_KEY="$(printf '%s' "$COUNTY" | tr '[:upper:]' '[:lower:]' | tr ' ' '-')"

[ -n "$SENDER_SCRIPT" ] || SENDER_SCRIPT="scripts/send-${COUNTY_KEY}-seed-feeder.mjs"
# Resolve sender script to an absolute path (accept absolute or repo-relative).
case "$SENDER_SCRIPT" in
  /*) SENDER_PATH="$SENDER_SCRIPT" ;;
  *)  SENDER_PATH="${REPO_ROOT}/${SENDER_SCRIPT}" ;;
esac
[ -f "$SENDER_PATH" ] || { echo "ERROR: sender script not found: $SENDER_PATH" >&2; exit 2; }

LAMBDA_NAME="${COUNTY_KEY}-feeder-watchdog"
ROLE_NAME="${COUNTY_KEY}-feeder-watchdog-role"
RULE_NAME="${COUNTY_KEY}-feeder-watchdog-schedule"
STATE_KEY="permit-harvest/${JOB_ID}/feeder-state.json"

# --- Derive FEEDER_MESSAGE + queue URL from the sender --dry-run ---------------
# The sender prints JSON: {dryRun, feederQueueUrl, feederMessage}. We bake the
# feederMessage (as a compact JSON string) and feederQueueUrl into the Lambda so
# the watchdog re-sends the byte-identical message the run was launched with.
echo "Resolving feeder message via: node ${SENDER_PATH} --job-id ${JOB_ID} --dry-run" >&2
SENDER_OUT="$(node "$SENDER_PATH" --job-id "$JOB_ID" --dry-run)" \
  || { echo "ERROR: sender --dry-run failed" >&2; exit 1; }

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

# One python pass: validate sender output, write env/policy files, and echo the
# scalar values we need back to the shell. Strings with commas/quotes never touch
# the shell, so there is no quoting hazard.
read -r FEEDER_QUEUE_URL ACCOUNT_ID QUEUE_NAME FEEDER_TYPE <<EOF
$(
  BUCKET="$BUCKET" STATE_KEY="$STATE_KEY" JOB_ID="$JOB_ID" \
  STALE_SECONDS="$STALE_SECONDS" AWS_REGION="$AWS_REGION" WORKDIR="$WORKDIR" \
  python3 - "$SENDER_OUT" <<'PY'
import json, os, sys

raw = sys.argv[1]
data = json.loads(raw)
msg = data["feederMessage"]
queue_url = data["feederQueueUrl"]

bucket = os.environ["BUCKET"]
state_key = os.environ["STATE_KEY"]
job_id = os.environ["JOB_ID"]
stale = os.environ["STALE_SECONDS"]
region = os.environ["AWS_REGION"]
workdir = os.environ["WORKDIR"]

# Parse account id + queue name from the SQS URL:
#   https://sqs.<region>.amazonaws.com/<account>/<queue-name>
parts = queue_url.rstrip("/").split("/")
account_id = parts[-2]
queue_name = parts[-1]
queue_arn = f"arn:aws:sqs:{region}:{account_id}:{queue_name}"
state_arn = f"arn:aws:s3:::{bucket}/{state_key}"

feeder_message = json.dumps(msg, separators=(",", ":"))

env = {
    "Variables": {
        "STATE_BUCKET": bucket,
        "STATE_KEY": state_key,
        "FEEDER_QUEUE_URL": queue_url,
        "FEEDER_MESSAGE": feeder_message,
        "STALE_SECONDS": stale,
    }
}
with open(os.path.join(workdir, "env.json"), "w") as f:
    json.dump(env, f)

trust = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "lambda.amazonaws.com"},
        "Action": "sts:AssumeRole",
    }],
}
with open(os.path.join(workdir, "trust.json"), "w") as f:
    json.dump(trust, f)

inline = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Action": "s3:GetObject", "Resource": state_arn},
        {"Effect": "Allow", "Action": "sqs:SendMessage", "Resource": queue_arn},
    ],
}
with open(os.path.join(workdir, "inline.json"), "w") as f:
    json.dump(inline, f)

# Scalars back to the shell (safe: no spaces/quotes).
print(queue_url, account_id, queue_name, msg.get("type", ""))
PY
)
EOF

[ -n "${FEEDER_QUEUE_URL:-}" ] || { echo "ERROR: failed to derive feeder queue URL from sender output" >&2; exit 1; }
QUEUE_ARN="arn:aws:sqs:${AWS_REGION}:${ACCOUNT_ID}:${QUEUE_NAME}"
STATE_ARN="arn:aws:s3:::${BUCKET}/${STATE_KEY}"

# --- Dry-run: print the resolved plan and exit WITHOUT changes -----------------
if [ "$DRY_RUN" = "true" ]; then
  WORKDIR="$WORKDIR" COUNTY="$COUNTY" COUNTY_KEY="$COUNTY_KEY" JOB_ID="$JOB_ID" \
  AWS_REGION="$AWS_REGION" AWS_PROFILE="$AWS_PROFILE" BUCKET="$BUCKET" \
  LAMBDA_NAME="$LAMBDA_NAME" ROLE_NAME="$ROLE_NAME" RULE_NAME="$RULE_NAME" \
  STATE_KEY="$STATE_KEY" STALE_SECONDS="$STALE_SECONDS" \
  FEEDER_QUEUE_URL="$FEEDER_QUEUE_URL" FEEDER_TYPE="$FEEDER_TYPE" \
  QUEUE_ARN="$QUEUE_ARN" STATE_ARN="$STATE_ARN" SENDER_PATH="$SENDER_PATH" \
  python3 - <<'PY'
import json, os

wd = os.environ["WORKDIR"]
with open(os.path.join(wd, "env.json")) as f:
    env = json.load(f)
with open(os.path.join(wd, "trust.json")) as f:
    trust = json.load(f)
with open(os.path.join(wd, "inline.json")) as f:
    inline = json.load(f)

plan = {
    "dryRun": True,
    "county": os.environ["COUNTY"],
    "countyKey": os.environ["COUNTY_KEY"],
    "jobId": os.environ["JOB_ID"],
    "awsProfile": os.environ["AWS_PROFILE"],
    "awsRegion": os.environ["AWS_REGION"],
    "senderScript": os.environ["SENDER_PATH"],
    "feederType": os.environ["FEEDER_TYPE"],
    "resources": {
        "lambda": os.environ["LAMBDA_NAME"],
        "role": os.environ["ROLE_NAME"],
        "rule": os.environ["RULE_NAME"],
        "schedule": "rate(5 minutes)",
        "runtime": "nodejs22.x",
        "handler": "index.handler",
        "timeoutSeconds": 60,
    },
    "s3": {"bucket": os.environ["BUCKET"], "stateKey": os.environ["STATE_KEY"]},
    "env": env["Variables"],
    "trustPolicy": trust,
    "inlinePolicy": {"name": "watchdog-perms", "document": inline},
    "managedPolicy": "AWSLambdaBasicExecutionRole",
}
print(json.dumps(plan, indent=2))
PY
  echo "Dry run only — no AWS resources were created or modified." >&2
  exit 0
fi

# =============================================================================
# Live deploy below. Everything above is read-only.
# =============================================================================
aws_lambda() { aws lambda "$@"; }

echo "==> Deploying ${LAMBDA_NAME} (county=${COUNTY}, job=${JOB_ID})" >&2

# --- 1. IAM role (dedicated, per-county) --------------------------------------
if aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  echo "Role ${ROLE_NAME} exists — updating trust + policies." >&2
  aws iam update-assume-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-document "file://${WORKDIR}/trust.json" >/dev/null
else
  echo "Creating role ${ROLE_NAME}." >&2
  aws iam create-role \
    --role-name "$ROLE_NAME" \
    --assume-role-policy-document "file://${WORKDIR}/trust.json" >/dev/null
fi

aws iam attach-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" >/dev/null

aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name "watchdog-perms" \
  --policy-document "file://${WORKDIR}/inline.json" >/dev/null

ROLE_ARN="$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text)"

# --- 2. Package the Lambda code -----------------------------------------------
ZIP_PATH="${WORKDIR}/function.zip"
( cd "$SCRIPT_DIR" && zip -q -j "$ZIP_PATH" index.mjs )

# --- 3. Create or update the Lambda -------------------------------------------
# Freshly-created roles are not immediately assumable by Lambda; retry on the
# "cannot be assumed" propagation error.
retry_iam() {
  local attempt=1 max=8 out rc
  while :; do
    if out="$("$@" 2>&1)"; then
      printf '%s\n' "$out"
      return 0
    fi
    rc=$?
    if printf '%s' "$out" | grep -qi "cannot be assumed"; then
      if [ "$attempt" -ge "$max" ]; then
        echo "ERROR: role still not assumable after ${max} attempts" >&2
        printf '%s\n' "$out" >&2
        return "$rc"
      fi
      echo "IAM not propagated yet (attempt ${attempt}/${max}); retrying in 5s..." >&2
      sleep 5
      attempt=$((attempt + 1))
      continue
    fi
    printf '%s\n' "$out" >&2
    return "$rc"
  done
}

if aws_lambda get-function --function-name "$LAMBDA_NAME" >/dev/null 2>&1; then
  echo "Lambda ${LAMBDA_NAME} exists — updating code + config." >&2
  aws_lambda update-function-code \
    --function-name "$LAMBDA_NAME" \
    --zip-file "fileb://${ZIP_PATH}" >/dev/null
  aws_lambda wait function-updated --function-name "$LAMBDA_NAME"
  retry_iam aws_lambda update-function-configuration \
    --function-name "$LAMBDA_NAME" \
    --role "$ROLE_ARN" \
    --runtime "nodejs22.x" \
    --handler "index.handler" \
    --timeout 60 \
    --environment "file://${WORKDIR}/env.json" >/dev/null
  aws_lambda wait function-updated --function-name "$LAMBDA_NAME"
else
  echo "Creating Lambda ${LAMBDA_NAME}." >&2
  retry_iam aws_lambda create-function \
    --function-name "$LAMBDA_NAME" \
    --runtime "nodejs22.x" \
    --handler "index.handler" \
    --role "$ROLE_ARN" \
    --timeout 60 \
    --zip-file "fileb://${ZIP_PATH}" \
    --environment "file://${WORKDIR}/env.json" >/dev/null
  aws_lambda wait function-active --function-name "$LAMBDA_NAME"
fi

LAMBDA_ARN="$(aws_lambda get-function --function-name "$LAMBDA_NAME" \
  --query 'Configuration.FunctionArn' --output text)"

# --- 4. EventBridge schedule --------------------------------------------------
aws events put-rule \
  --name "$RULE_NAME" \
  --schedule-expression "rate(5 minutes)" \
  --state ENABLED >/dev/null

# Allow EventBridge to invoke the Lambda (idempotent — ignore "already exists").
if ! aws_lambda add-permission \
  --function-name "$LAMBDA_NAME" \
  --statement-id "${RULE_NAME}-invoke" \
  --action "lambda:InvokeFunction" \
  --principal "events.amazonaws.com" \
  --source-arn "arn:aws:events:${AWS_REGION}:${ACCOUNT_ID}:rule/${RULE_NAME}" \
  >/dev/null 2>&1; then
  echo "Invoke permission already present (or add-permission not needed)." >&2
fi

aws events put-targets \
  --rule "$RULE_NAME" \
  --targets "Id=1,Arn=${LAMBDA_ARN}" >/dev/null

RULE_STATE="$(aws events describe-rule --name "$RULE_NAME" --query 'State' --output text)"

# --- 5. Summary ---------------------------------------------------------------
echo "==> Done." >&2
python3 - <<PY
import json
print(json.dumps({
    "lambdaArn": "${LAMBDA_ARN}",
    "roleArn": "${ROLE_ARN}",
    "rule": "${RULE_NAME}",
    "ruleState": "${RULE_STATE}",
    "schedule": "rate(5 minutes)",
    "county": "${COUNTY}",
    "jobId": "${JOB_ID}",
    "staleSeconds": ${STALE_SECONDS},
}, indent=2))
PY
