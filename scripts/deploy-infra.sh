#!/usr/bin/env bash
set -euo pipefail

# Unified deployment for SAM stack (MWAA + VPC + SQS + Lambdas)

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }

# Config with sane defaults
STACK_NAME="${STACK_NAME:-elephant-mwaa}"
SAM_TEMPLATE="prepare/template.yaml"
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
  command -v sam >/dev/null || { err "sam CLI not found. Install: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html"; exit 1; }
  [[ -f "$SAM_TEMPLATE" && -f "$STARTUP_SCRIPT" && -f "$PYPROJECT_FILE" ]] || { err "Missing required files"; exit 1; }
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

sam_build() {
  info "Building SAM application"
  sam build --template-file "$SAM_TEMPLATE" >/dev/null
}

sam_deploy_initial() {
  info "Deploying SAM stack (initial)"
  sam deploy \
    --template-file "$SAM_TEMPLATE" \
    --stack-name "$STACK_NAME" \
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
    --resolve-s3 \
    --no-confirm-changeset \
    --no-fail-on-empty-changeset \
    --parameter-overrides SourceS3BucketArn="$SOURCE_S3_BUCKET_ARN" >/dev/null
}

sam_deploy_with_versions() {
  local script_ver=$1 req_ver=$2
  info "Deploying SAM stack with MWAA artifact versions"
  sam deploy \
    --template-file "$SAM_TEMPLATE" \
    --stack-name "$STACK_NAME" \
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
    --resolve-s3 \
    --no-confirm-changeset \
    --no-fail-on-empty-changeset \
    --parameter-overrides \
      SourceS3BucketArn="$SOURCE_S3_BUCKET_ARN" \
      StartupScriptS3Path="startup.sh" \
      StartupScriptS3ObjectVersion="$script_ver" \
      RequirementsS3Path="requirements.txt" \
      RequirementsS3ObjectVersion="$req_ver" >/dev/null
}

main() {
  check_prereqs
  validate_source_bucket

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
  echo
  info "Done! UI: $ui_url"
  info "MWAA Env: $env_name"
  info "DAGs bucket: $bucket (prefix: dags/)"
}

main "$@"
