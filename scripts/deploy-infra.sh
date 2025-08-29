#!/usr/bin/env bash
set -euo pipefail

# Simple, one-command infra deploy for MWAA + VPC + SQS

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }

# Config with sane defaults
STACK_NAME="${STACK_NAME:-elephant-mwaa}"
TEMPLATE_FILE="infra/mwaa-public-network.yaml"
STARTUP_SCRIPT="infra/startup.sh"
PYPROJECT_FILE="infra/pyproject.toml"
BUILD_DIR="infra/build"
REQUIREMENTS_FILE="${BUILD_DIR}/requirements.txt"
AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.12.txt"
SOURCE_S3_BUCKET_ARN="${SOURCE_S3_BUCKET_ARN:-}"  # REQUIRED; example: arn:aws:s3:::source-bucket

mkdir -p "$BUILD_DIR"

check_prereqs() {
  info "Checking prerequisites..."
  command -v aws >/dev/null || { err "aws CLI not found"; exit 1; }
  aws sts get-caller-identity >/dev/null || { err "AWS credentials not configured"; exit 1; }
  command -v jq >/dev/null || { err "jq not found"; exit 1; }
  command -v zip >/dev/null || { err "zip not found"; exit 1; }
  command -v curl >/dev/null || { err "curl not found"; exit 1; }
  command -v uv >/dev/null || { err "uv not found. Install: https://docs.astral.sh/uv/getting-started/installation/"; exit 1; }
  [[ -f "$TEMPLATE_FILE" && -f "$STARTUP_SCRIPT" && -f "$PYPROJECT_FILE" ]] || { err "Missing infra files"; exit 1; }
}

validate_source_bucket() {
  if [[ -z "$SOURCE_S3_BUCKET_ARN" ]]; then
    err "SOURCE_S3_BUCKET_ARN is required. Set it as environment variable. Example: arn:aws:s3:::my-source-bucket"
    exit 1
  fi
  if ! [[ "$SOURCE_S3_BUCKET_ARN" =~ ^arn:aws:s3:::[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]]; then
    err "SOURCE_S3_BUCKET_ARN looks invalid. Expected format: arn:aws:s3:::bucket-name"
    exit 1
  fi
}

stack_exists() { aws cloudformation describe-stacks --stack-name "$STACK_NAME" >/dev/null 2>&1; }

create_stack() {
  info "Creating stack: $STACK_NAME"
  local params=(
    --stack-name "$STACK_NAME"
    --template-body "file://$TEMPLATE_FILE"
    --capabilities CAPABILITY_IAM
    --on-failure DO_NOTHING
    --parameters ParameterKey=SourceS3BucketArn,ParameterValue="${SOURCE_S3_BUCKET_ARN}"
  )
  if ! aws cloudformation create-stack "${params[@]}"; then
    err "Stack creation failed. Please ensure SOURCE_S3_BUCKET_ARN is set (arn:aws:s3:::your-bucket)."
    exit 1
  fi
  info "Waiting for create to complete (≈15–20m) ..."
  aws cloudformation wait stack-create-complete --stack-name "$STACK_NAME"
  info "Stack created"
}

get_bucket() {
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`EnvironmentBucketName`].OutputValue' \
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

update_stack_versions() {
  local bucket=$1 script_version=$2 req_version=$3
  info "Updating stack with versions"
  local params
  params=$(mktemp)
  cat >"$params" <<JSON
[
  {"ParameterKey":"StartupScriptS3Path","ParameterValue":"startup.sh"},
  {"ParameterKey":"StartupScriptS3ObjectVersion","ParameterValue":"$script_version"},
  {"ParameterKey":"RequirementsS3Path","ParameterValue":"requirements.txt"},
  {"ParameterKey":"RequirementsS3ObjectVersion","ParameterValue":"$req_version"},
  {"ParameterKey":"SourceS3BucketArn","ParameterValue":"$SOURCE_S3_BUCKET_ARN"}
]
JSON

  if aws cloudformation update-stack \
      --stack-name "$STACK_NAME" \
      --template-body "file://$TEMPLATE_FILE" \
      --parameters "file://$params" \
      --capabilities CAPABILITY_IAM >/dev/null 2>&1; then
    info "Waiting for update to complete (≈10–20m) ..."
    aws cloudformation wait stack-update-complete --stack-name "$STACK_NAME"
    info "Stack updated"
  else
    warn "No updates to perform (already up-to-date)"
  fi
}

main() {
  check_prereqs
  validate_source_bucket
  if ! stack_exists; then create_stack; else info "Stack exists; will update"; fi
  local bucket script_ver req_ver
  bucket=$(get_bucket)
  [[ -n "$bucket" && "$bucket" != "None" ]] || { err "Could not resolve EnvironmentBucketName output"; exit 1; }
  info "Using S3 bucket: $bucket"

  compile_requirements
  info "Uploading startup.sh and requirements.txt"
  script_ver=$(upload_with_version "$STARTUP_SCRIPT" startup.sh "$bucket")
  req_ver=$(upload_with_version "$REQUIREMENTS_FILE" requirements.txt "$bucket")
  info "startup.sh version: $script_ver"
  info "requirements.txt version: $req_ver"

  update_stack_versions "$bucket" "$script_ver" "$req_ver"

  local ui_url env_name
  env_name="${MWAA_ENV_NAME:-${STACK_NAME}-MwaaEnvironment}"
  ui_url=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='MwaaApacheAirflowUI'].OutputValue" --output text)
  echo
  info "Done! UI: $ui_url"
  info "MWAA Env: $env_name"
  info "DAGs bucket: $bucket (prefix: dags/)"
}

main "$@"
