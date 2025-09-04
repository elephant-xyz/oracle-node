#!/usr/bin/env bash
set -euo pipefail

# Backwards-compatible deploy script. Now delegates to simpler commands and can refresh Airflow variables.
# - Sync DAGs
# - Update transforms (sets elephant_scripts_s3_uri)
# - Optionally refresh variables/secrets if env vars are present

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }

STACK_NAME="${STACK_NAME:-elephant-oracle-node}"
MWAA_ENV_NAME="${MWAA_ENV_NAME:-${STACK_NAME}-MwaaEnvironment}"

echo -e "${GREEN}🚀 MWAA Deploy (DAGs + Transforms)${NC}"

command -v aws >/dev/null || { err "aws CLI not found"; exit 1; }
aws sts get-caller-identity >/dev/null || { err "AWS credentials not configured"; exit 1; }
command -v jq >/dev/null || { err "jq not found"; exit 1; }
command -v curl >/dev/null || { err "curl not found"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"$SCRIPT_DIR/sync-dags.sh"
"$SCRIPT_DIR/update-transforms.sh"

# Optionally refresh variables/secrets if env vars or overrides are present
get_output() {
  local key=$1
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='${key}'].OutputValue" \
    --output text
}

set_airflow_vars() {
  local token_json cli_token host
  token_json=$(aws mwaa create-cli-token --name "$MWAA_ENV_NAME" 2>/dev/null || true)
  cli_token=$(echo "$token_json" | jq -r '.CliToken')
  host=$(echo "$token_json" | jq -r '.WebServerHostname')
  if [[ -z "$cli_token" || "$cli_token" == "null" || -z "$host" || "$host" == "null" ]]; then
    warn "Could not acquire MWAA CLI token/hostname. Skipping Airflow variable refresh."
    return 0
  fi

  run_airflow() {
    local cmd="$1"
    curl -sS --location --request POST "https://${host}/aws_mwaa/cli" \
      --header "Authorization: Bearer ${cli_token}" \
      --header "Content-Type: text/plain" \
      --data-raw "$cmd"
  }

  set_var() {
    local name="$1" value="$2"
    local resp stderr stdout
    resp=$(run_airflow "variables set ${name} ${value}")
    stderr=$(echo "$resp" | jq -r '.stderr' | base64 -d 2>/dev/null || true)
    stdout=$(echo "$resp" | jq -r '.stdout' | base64 -d 2>/dev/null || true)
    if [[ -z "$stdout" || "$stdout" == "null" ]]; then
      warn "Failed to set Airflow variable: ${name}"
    else
      info "Set Airflow variable: ${name}"
      [[ -n "$stderr" && "$stderr" != "null" ]] && warn "$stderr"
    fi
  }

  # Refresh from stack outputs
  local sqs_url ssm_param lambda_arn bucket output_base batch_size
  sqs_url=$(get_output MwaaSqsQueueUrl)
  ssm_param=$(get_output DownloaderFunctionArnParameterName)
  lambda_arn=$(get_output DownloaderFunctionArn)
  bucket=$(get_output EnvironmentBucketName)
  [[ -n "$sqs_url" && "$sqs_url" != "None" ]] && set_var elephant_sqs_queue_url "$sqs_url" || true
  [[ -n "$ssm_param" && "$ssm_param" != "None" ]] && set_var elephant_downloader_ssm_param_name "$ssm_param" || true
  [[ -n "$lambda_arn" && "$lambda_arn" != "None" ]] && set_var elephant_downloader_function_arn "$lambda_arn" || true

  # Optional overrides via environment variables
  if [[ -n "${ELEPHANT_OUTPUT_BASE_URI:-}" ]]; then
    output_base="$ELEPHANT_OUTPUT_BASE_URI"
  elif [[ -n "$bucket" && "$bucket" != "None" ]]; then
    output_base="s3://${bucket}/outputs"
  fi
  [[ -n "${output_base:-}" ]] && set_var elephant_output_base_uri "$output_base" || true

  batch_size="${ELEPHANT_BATCH_SIZE:-10}"
  set_var elephant_batch_size "$batch_size"

  # Secrets (only set if provided)
  [[ -n "${ELEPHANT_DOMAIN:-}" ]] && set_var elephant_domain "$ELEPHANT_DOMAIN" || true
  [[ -n "${ELEPHANT_API_KEY:-}" ]] && set_var elephant_api_key "$ELEPHANT_API_KEY" || true
  [[ -n "${ELEPHANT_ORACLE_KEY_ID:-}" ]] && set_var elephant_oracle_key_id "$ELEPHANT_ORACLE_KEY_ID" || true
  [[ -n "${ELEPHANT_FROM_ADDRESS:-}" ]] && set_var elephant_from_address "$ELEPHANT_FROM_ADDRESS" || true
  [[ -n "${ELEPHANT_RPC_URL:-}" ]] && set_var elephant_rpc_url "$ELEPHANT_RPC_URL" || true
  [[ -n "${ELEPHANT_PINATA_JWT:-}" ]] && set_var elephant_pinata_jwt "$ELEPHANT_PINATA_JWT" || true
}

info "Refreshing Airflow variables from stack outputs and environment overrides"
set_airflow_vars

echo -e "${GREEN}✅ Done${NC}"
