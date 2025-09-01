#!/usr/bin/env bash
set -euo pipefail

# Unified deployment for SAM stack (MWAA + VPC + SQS + Lambdas)

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }

# Config with sane defaults
STACK_NAME="${STACK_NAME:-oracle-node}"
SAM_TEMPLATE="prepare/template.yaml"
BUILT_TEMPLATE=".aws-sam/build/template.yaml"
STARTUP_SCRIPT="infra/startup.sh"
PYPROJECT_FILE="infra/pyproject.toml"
BUILD_DIR="infra/build"
REQUIREMENTS_FILE="${BUILD_DIR}/requirements.txt"
AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.12.txt"

mkdir -p "$BUILD_DIR"

check_prereqs() {
  info "Checking prerequisites..."
  command -v aws >/dev/null || { err "aws CLI not found"; exit 1; }
  aws sts get-caller-identity >/dev/null || { err "AWS credentials not configured"; exit 1; }
  command -v jq >/dev/null || { err "jq not found"; exit 1; }
  command -v zip >/dev/null || { err "zip not found"; exit 1; }
  command -v curl >/dev/null || { err "curl not found"; exit 1; }
  command -v uv >/dev/null || { err "uv not found. Install: https://docs.astral.sh/uv/getting-started/installation/"; exit 1; }
  command -v sam >/dev/null || { err "sam CLI not found. Install: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html"; exit 1; }
  [[ -f "$SAM_TEMPLATE" && -f "$STARTUP_SCRIPT" && -f "$PYPROJECT_FILE" ]] || { err "Missing required files"; exit 1; }
}


get_bucket() {
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`EnvironmentBucketName`].OutputValue' \
    --output text
}

get_output() {
  local key=$1
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='${key}'].OutputValue" \
    --output text
}

compile_requirements() {
  info "Compiling requirements via uv"
  uv pip compile "$PYPROJECT_FILE" --constraint "$AIRFLOW_CONSTRAINTS_URL" >"$REQUIREMENTS_FILE"
  if ! grep -q "^-c $AIRFLOW_CONSTRAINTS_URL$" "$REQUIREMENTS_FILE"; then
    info "Prepending Airflow constraints to requirements.txt"
    { echo "-c $AIRFLOW_CONSTRAINTS_URL"; cat "$REQUIREMENTS_FILE"; } >"$REQUIREMENTS_FILE.tmp" && mv "$REQUIREMENTS_FILE.tmp" "$REQUIREMENTS_FILE"
  fi
}

upload_with_version() {
  local src=$1 dst=$2 bucket=$3
  aws s3 cp "$src" "s3://$bucket/$dst" --only-show-errors
  aws s3api list-object-versions --bucket "$bucket" --prefix "$dst" \
    --query 'Versions[?IsLatest==`true`].VersionId' --output text
}

sam_build() {
  info "Building SAM application"
  sam build --template-file "$SAM_TEMPLATE" >/dev/null
}

sam_deploy_initial() {
  info "Deploying SAM stack (initial)"
  sam deploy \
    --template-file "$BUILT_TEMPLATE" \
    --stack-name "$STACK_NAME" \
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
    --resolve-s3 \
    --no-confirm-changeset \
    --no-fail-on-empty-changeset >/dev/null
}

sam_deploy_with_versions() {
  local script_ver=$1 req_ver=$2
  info "Deploying SAM stack with MWAA artifact versions"
  sam deploy \
    --template-file "$BUILT_TEMPLATE" \
    --stack-name "$STACK_NAME" \
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
    --resolve-s3 \
    --no-confirm-changeset \
    --no-fail-on-empty-changeset \
    --parameter-overrides \
      StartupScriptS3Path="startup.sh" \
      StartupScriptS3ObjectVersion="$script_ver" \
      RequirementsS3Path="requirements.txt" \
      RequirementsS3ObjectVersion="$req_ver" >/dev/null
}

set_airflow_vars() {
  local bucket=$1
  local env_name
  env_name="${MWAA_ENV_NAME:-${STACK_NAME}-MwaaEnvironment}"
  info "Setting Airflow variables in MWAA environment: $env_name"

  local token_json cli_token host
  token_json=$(aws mwaa create-cli-token --name "$env_name" 2>/dev/null || true)
  cli_token=$(echo "$token_json" | jq -r '.CliToken')
  host=$(echo "$token_json" | jq -r '.WebServerHostname')
  if [[ -z "$cli_token" || "$cli_token" == "null" || -z "$host" || "$host" == "null" ]]; then
    warn "Could not acquire MWAA CLI token/hostname. Skipping Airflow variable setup."
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

  # Core variables derived from stack outputs/bucket
  local sqs_url ssm_param lambda_arn output_base batch_size
  sqs_url=$(get_output MwaaSqsQueueUrl)
  ssm_param=$(get_output DownloaderFunctionArnParameterName)
  lambda_arn=$(get_output DownloaderFunctionArn)
  if [[ -n "$sqs_url" && "$sqs_url" != "None" ]]; then
    set_var elephant_sqs_queue_url "$sqs_url"
  else
    warn "Stack output MwaaSqsQueueUrl not found"
  fi
  if [[ -n "$ssm_param" && "$ssm_param" != "None" ]]; then
    set_var elephant_downloader_ssm_param_name "$ssm_param"
  else
    warn "Stack output DownloaderFunctionArnParameterName not found"
  fi
  if [[ -n "$lambda_arn" && "$lambda_arn" != "None" ]]; then
    set_var elephant_downloader_function_arn "$lambda_arn"
  fi

  output_base="${ELEPHANT_OUTPUT_BASE_URI:-s3://${bucket}/outputs}"
  set_var elephant_output_base_uri "$output_base"

  batch_size="${ELEPHANT_BATCH_SIZE:-1}"
  set_var elephant_batch_size "$batch_size"

  # Optional secrets pulled from environment if present
  [[ -n "${ELEPHANT_DOMAIN:-}" ]] && set_var elephant_domain "$ELEPHANT_DOMAIN" || true
  [[ -n "${ELEPHANT_API_KEY:-}" ]] && set_var elephant_api_key "$ELEPHANT_API_KEY" || true
  [[ -n "${ELEPHANT_ORACLE_KEY_ID:-}" ]] && set_var elephant_oracle_key_id "$ELEPHANT_ORACLE_KEY_ID" || true
  [[ -n "${ELEPHANT_FROM_ADDRESS:-}" ]] && set_var elephant_from_address "$ELEPHANT_FROM_ADDRESS" || true
  [[ -n "${ELEPHANT_RPC_URL:-}" ]] && set_var elephant_rpc_url "$ELEPHANT_RPC_URL" || true
  [[ -n "${ELEPHANT_PINATA_JWT:-}" ]] && set_var elephant_pinata_jwt "$ELEPHANT_PINATA_JWT" || true
}

main() {
  check_prereqs

  sam_build
  sam_deploy_initial

  local bucket script_ver req_ver
  bucket=$(get_bucket)
  [[ -n "$bucket" && "$bucket" != "None" ]] || { err "Could not resolve EnvironmentBucketName output"; exit 1; }
  info "Using MWAA Environment bucket: $bucket"

  compile_requirements
  info "Uploading startup.sh and requirements.txt"
  script_ver=$(upload_with_version "$STARTUP_SCRIPT" startup.sh "$bucket")
  req_ver=$(upload_with_version "$REQUIREMENTS_FILE" requirements.txt "$bucket")
  info "startup.sh version: $script_ver"
  info "requirements.txt version: $req_ver"

  sam_deploy_with_versions "$script_ver" "$req_ver"

  local ui_url env_name
  env_name="${MWAA_ENV_NAME:-${STACK_NAME}-MwaaEnvironment}"
  ui_url=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='MwaaApacheAirflowUI'].OutputValue" --output text)

  # Configure Airflow variables now that the environment exists
  set_airflow_vars "$bucket"
  echo
  info "Done! UI: $ui_url"
  info "MWAA Env: $env_name"
  info "DAGs bucket: $bucket (prefix: dags/)"
}

main "$@"
