#!/usr/bin/env bash
set -euo pipefail

# Zip transforms and update the MWAA Airflow variable used by the DAG.

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }

STACK_NAME="${STACK_NAME:-oracle-node}"
MWAA_ENV_NAME="${MWAA_ENV_NAME:-${STACK_NAME}-MwaaEnvironment}"
TRANSFORMS_DIR="${TRANSFORMS_DIR:-transforms}"
ZIP_PATH="build/transforms.zip"

command -v aws >/dev/null || { err "aws CLI not found"; exit 1; }
aws sts get-caller-identity >/dev/null || { err "AWS credentials not configured"; exit 1; }
command -v jq >/dev/null || { err "jq not found"; exit 1; }
command -v zip >/dev/null || { err "zip not found"; exit 1; }

info "Resolving MWAA bucket from stack: $STACK_NAME"
S3_BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='EnvironmentBucketName'].OutputValue" --output text)
[[ -n "$S3_BUCKET" && "$S3_BUCKET" != "None" ]] || { err "Could not determine EnvironmentBucketName"; exit 1; }
info "Using bucket: $S3_BUCKET"

[[ -d "$TRANSFORMS_DIR" ]] || { err "Transforms directory not found: $TRANSFORMS_DIR"; exit 1; }
mkdir -p "$(dirname "$ZIP_PATH")"
rm -f "$ZIP_PATH"

pushd "$TRANSFORMS_DIR" >/dev/null
zip -r "../$ZIP_PATH" . >/dev/null
popd >/dev/null
info "Zipped transforms to $ZIP_PATH"

S3_URI="s3://${S3_BUCKET}/scripts/transforms.zip"
aws s3 cp "$ZIP_PATH" "$S3_URI" --only-show-errors
info "Uploaded transforms to $S3_URI"

info "Updating Airflow variable 'elephant_scripts_s3_uri' in env: $MWAA_ENV_NAME"
TOKEN_JSON=$(aws mwaa create-cli-token --name "$MWAA_ENV_NAME")
CLI_TOKEN=$(echo "$TOKEN_JSON" | jq -r '.CliToken')
WEB_SERVER_HOSTNAME=$(echo "$TOKEN_JSON" | jq -r '.WebServerHostname')
[[ -n "$CLI_TOKEN" && "$CLI_TOKEN" != "null" && -n "$WEB_SERVER_HOSTNAME" && "$WEB_SERVER_HOSTNAME" != "null" ]] || { err "Failed to get MWAA CLI token or hostname"; exit 1; }

RESP=$(curl -sS --location --request POST "https://${WEB_SERVER_HOSTNAME}/aws_mwaa/cli" \
  --header "Authorization: Bearer ${CLI_TOKEN}" \
  --header "Content-Type: text/plain" \
  --data-raw "variables set elephant_scripts_s3_uri ${S3_URI}")

STDERR=$(echo "$RESP" | jq -r '.stderr' | base64 -d 2>/dev/null || true)
STDOUT=$(echo "$RESP" | jq -r '.stdout' | base64 -d 2>/dev/null || true)
[[ -n "$STDOUT" && "$STDOUT" != "null" ]] || { err "Variable update failed"; echo "$RESP"; exit 1; }
[[ -n "$STDERR" && "$STDERR" != "null" ]] && echo -e "${YELLOW}[WARN]${NC} $STDERR"

info "Updated variable. Value: $S3_URI"

