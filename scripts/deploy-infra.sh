#!/usr/bin/env bash
set -euo pipefail

# Unified deployment for SAM stack (MWAA + VPC + SQS + Lambdas)

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }

# Source the reusable Lambda image update script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/update-lambda-image.sh"

# Config with sane defaults
STACK_NAME="${STACK_NAME:-elephant-oracle-node}"
CODEBUILD_STACK_NAME="${CODEBUILD_STACK_NAME:-elephant-oracle-codebuild}"
SAM_TEMPLATE="prepare/template.yaml"
BUILT_TEMPLATE=".aws-sam/build/template.yaml"
STARTUP_SCRIPT="infra/startup.sh"
PYPROJECT_FILE="infra/pyproject.toml"
BUILD_DIR="infra/build"
REQUIREMENTS_FILE="${BUILD_DIR}/requirements.txt"
AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.12.txt"
WORKFLOW_DIR="workflow"
TRANSFORMS_SRC_DIR="transform"
TRANSFORMS_TARGET_DIR="${WORKFLOW_DIR}/lambdas/post/transforms"
TRANSFORM_MANIFEST_FILE="${TRANSFORMS_TARGET_DIR}/manifest.json"
TRANSFORM_PREFIX_KEY="${TRANSFORM_PREFIX_KEY:-transforms}"
UPLOAD_TRANSFORMS="${UPLOAD_TRANSFORMS:-false}"
TRANSFORMS_UPLOAD_PENDING=0
BROWSER_FLOWS_UPLOAD_PENDING=0
MULTI_REQUEST_FLOWS_UPLOAD_PENDING=0
STATIC_PARTS_UPLOAD_PENDING=0
ENVIRONMENT_NAME="${ENVIRONMENT_NAME:-MWAAEnvironment}"

declare -a PARAM_OVERRIDES=()

CODEBUILD_RUNTIME_MODULE_DIR="codebuild/runtime-module"
CODEBUILD_RUNTIME_ARCHIVE_NAME="${CODEBUILD_RUNTIME_ARCHIVE_NAME:-runtime-module.zip}"
CODEBUILD_RUNTIME_PREFIX="${CODEBUILD_RUNTIME_PREFIX:-codebuild/runtime}"
CODEBUILD_RUNTIME_UPLOAD_PENDING=0
CODEBUILD_RUNTIME_ENTRYPOINT="${CODEBUILD_RUNTIME_ENTRYPOINT:-index.js}"
CODEBUILD_TEMPLATE="codebuild/template.yaml"
CODEBUILD_DEPLOY_PENDING=0

mkdir -p "$BUILD_DIR"

check_prereqs() {
  info "Checking prerequisites..."
  command -v aws >/dev/null || { err "aws CLI not found"; exit 1; }
  aws sts get-caller-identity >/dev/null || { err "AWS credentials not configured"; exit 1; }
  command -v jq >/dev/null || { err "jq not found"; exit 1; }
  command -v zip >/dev/null || { err "zip not found"; exit 1; }
  command -v curl >/dev/null || { err "curl not found"; exit 1; }
  command -v sam >/dev/null || { err "sam CLI not found. Install: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html"; exit 1; }
  command -v git >/dev/null || { err "git not found"; exit 1; }
  command -v npm >/dev/null || { err "npm not found"; exit 1; }
  command -v docker >/dev/null || { err "docker not found. Install: https://docs.docker.com/get-docker/"; exit 1; }
  [[ -f "$SAM_TEMPLATE" && -f "$STARTUP_SCRIPT" && -f "$PYPROJECT_FILE" ]] || { err "Missing required files"; exit 1; }
  [[ -f "$CODEBUILD_TEMPLATE" ]] || { err "Missing CodeBuild template: $CODEBUILD_TEMPLATE"; exit 1; }
}


get_bucket() {
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`EnvironmentBucketName`].OutputValue' \
    --output text 2>/dev/null || echo ""
}

get_output() {
  local key=$1
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='${key}'].OutputValue" \
    --output text 2>/dev/null || echo ""
}

sam_build() {
  info "Building SAM application"
  info "Note: Docker image build may take 30-60 minutes on first run (downloads ML models)"
  info "Building without cache to ensure latest git dependencies are fetched"

  # Clean SAM build directory to ensure fresh build
  rm -rf .aws-sam/build 2>/dev/null || true

  # Use --no-cached to force a fresh build without using cached artifacts
  # This ensures git dependencies like @elephant-xyz/cli always fetch latest commits
  sam build --template-file "$SAM_TEMPLATE" --no-cached
}

sam_deploy() {
  info "Deploying SAM stack (initial)"
  info "Note: Image push may take 20-60 minutes for large ML models (be patient!)"

  # For image-based functions, use --resolve-image-repos to let SAM handle ECR
  # Set Docker client timeout to 60 minutes for large ML model images
  export DOCKER_CLIENT_TIMEOUT=3600
  export COMPOSE_HTTP_TIMEOUT=3600

  local -a sam_args=(
    --template-file "$BUILT_TEMPLATE"
    --stack-name "$STACK_NAME"
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM
    --resolve-s3
    --resolve-image-repos
    --no-confirm-changeset
    --no-fail-on-empty-changeset
  )

  if ((${#PARAM_OVERRIDES[@]})); then
    sam_args+=(--parameter-overrides "${PARAM_OVERRIDES[@]}")
  fi

  sam deploy "${sam_args[@]}" >/dev/null

  # CRITICAL: Force Lambda to pull the latest Docker image from ECR
  # Lambda caches container images by digest, so even if we push a new 'latest' tag,
  # Lambda might use the old cached image unless we explicitly update it
  # Use the reusable function from update-lambda-image.sh for MVL Lambda
  update_lambda_with_latest_image "" "WorkflowMirrorValidatorFunctionName"
}

sam_deploy_with_versions() {
  local script_ver=$1 req_ver=$2
  info "Deploying SAM stack with MWAA artifact versions"
  local -a overrides=("${PARAM_OVERRIDES[@]}")
  overrides+=(
    'StartupScriptS3Path="startup.sh"'
    "StartupScriptS3ObjectVersion=\"$script_ver\""
    'RequirementsS3Path="requirements.txt"'
    "RequirementsS3ObjectVersion=\"$req_ver\""
  )

  local -a sam_args=(
    --template-file "$BUILT_TEMPLATE"
    --stack-name "$STACK_NAME"
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM
    --resolve-s3
    --no-confirm-changeset
    --no-fail-on-empty-changeset
    --parameter-overrides "${overrides[@]}"
  )

  sam deploy "${sam_args[@]}" >/dev/null
}

compute_param_overrides() {
  # Validate browser flow template parameters (must be provided together)
  if [[ -n "${ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE:-}" || -n "${ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS:-}" ]]; then
    if [[ -z "${ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE:-}" ]]; then
      err "ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS is set but ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE is not. Both must be provided together."
      exit 1
    fi
    if [[ -z "${ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS:-}" ]]; then
      err "ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE is set but ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS is not. Both must be provided together."
      exit 1
    fi

    # Validate that ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS contains valid JSON
    if ! echo "${ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS}" | jq . >/dev/null 2>&1; then
      err "ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS must contain valid JSON"
      exit 1
    fi

    # Convert JSON to simple key:value format for safe transport
    ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS_SIMPLE=$(echo "${ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS}" | jq -r 'to_entries | map("\(.key):\(.value)") | join(",")')
    info "Browser flow template configuration validated and converted to simple format"
  fi

  local -a parts=()

  # Check if using keystore mode
  if [[ -n "${ELEPHANT_KEYSTORE_FILE:-}" ]]; then
    # Keystore mode - verify requirements
    [[ ! -f "$ELEPHANT_KEYSTORE_FILE" ]] && { err "Keystore file not found: $ELEPHANT_KEYSTORE_FILE"; exit 1; }
    : "${ELEPHANT_KEYSTORE_PASSWORD?Set ELEPHANT_KEYSTORE_PASSWORD when using keystore}"
    : "${ELEPHANT_RPC_URL?Set ELEPHANT_RPC_URL}"
    : "${ELEPHANT_PINATA_JWT?Set ELEPHANT_PINATA_JWT}"

    # Upload keystore to S3 and get the S3 key
    info "Uploading keystore file to S3..."
    local keystore_s3_key="keystores/keystore-$(date +%s).json"
    local bucket=$(get_bucket 2>/dev/null || echo "")

    if [[ -z "$bucket" ]]; then
      # Bucket will be created during first deploy
      info "Bucket will be created during initial deployment"
      KEYSTORE_S3_KEY_PENDING="$keystore_s3_key"
      KEYSTORE_FILE_PENDING="$ELEPHANT_KEYSTORE_FILE"
    else
      aws s3 cp "$ELEPHANT_KEYSTORE_FILE" "s3://$bucket/$keystore_s3_key"
      ELEPHANT_KEYSTORE_S3_KEY="$keystore_s3_key"
    fi

    parts+=("ElephantRpcUrl=\"$ELEPHANT_RPC_URL\"")
    parts+=("ElephantPinataJwt=\"$ELEPHANT_PINATA_JWT\"")
    parts+=("ElephantKeystoreS3Key=\"${ELEPHANT_KEYSTORE_S3_KEY:-pending}\"")
    parts+=("ElephantKeystorePassword=\"$ELEPHANT_KEYSTORE_PASSWORD\"")
  else
    # Traditional mode - require all API credentials
    : "${ELEPHANT_DOMAIN?Set ELEPHANT_DOMAIN}"
    : "${ELEPHANT_API_KEY?Set ELEPHANT_API_KEY}"
    : "${ELEPHANT_ORACLE_KEY_ID?Set ELEPHANT_ORACLE_KEY_ID}"
    : "${ELEPHANT_FROM_ADDRESS?Set ELEPHANT_FROM_ADDRESS}"
    : "${ELEPHANT_RPC_URL?Set ELEPHANT_RPC_URL}"
    : "${ELEPHANT_PINATA_JWT?Set ELEPHANT_PINATA_JWT}"

    parts+=("ElephantDomain=\"$ELEPHANT_DOMAIN\"")
    parts+=("ElephantApiKey=\"$ELEPHANT_API_KEY\"")
    parts+=("ElephantOracleKeyId=\"$ELEPHANT_ORACLE_KEY_ID\"")
    parts+=("ElephantFromAddress=\"$ELEPHANT_FROM_ADDRESS\"")
    parts+=("ElephantRpcUrl=\"$ELEPHANT_RPC_URL\"")
    parts+=("ElephantPinataJwt=\"$ELEPHANT_PINATA_JWT\"")
  fi

  # Build parameters with simple format
  [[ -n "${WORKFLOW_QUEUE_NAME:-}" ]] && parts+=("WorkflowQueueName=\"$WORKFLOW_QUEUE_NAME\"")
  [[ -n "${WORKFLOW_STARTER_RESERVED_CONCURRENCY:-}" ]] && parts+=("WorkflowStarterReservedConcurrency=\"$WORKFLOW_STARTER_RESERVED_CONCURRENCY\"")
  [[ -n "${WORKFLOW_STATE_MACHINE_NAME:-}" ]] && parts+=("WorkflowStateMachineName=\"$WORKFLOW_STATE_MACHINE_NAME\"")

  # Prepare function flags
  [[ -n "${ELEPHANT_PREPARE_USE_BROWSER:-}" ]] && parts+=("ElephantPrepareUseBrowser=\"$ELEPHANT_PREPARE_USE_BROWSER\"")
  [[ -n "${ELEPHANT_PREPARE_NO_FAST:-}" ]] && parts+=("ElephantPrepareNoFast=\"$ELEPHANT_PREPARE_NO_FAST\"")
  [[ -n "${ELEPHANT_PREPARE_NO_CONTINUE:-}" ]] && parts+=("ElephantPrepareNoContinue=\"$ELEPHANT_PREPARE_NO_CONTINUE\"")
  [[ -n "${ELEPHANT_PREPARE_IGNORE_CAPTCHA:-}" ]] && parts+=("ElephantPrepareIgnoreCaptcha=\"$ELEPHANT_PREPARE_IGNORE_CAPTCHA\"")

  # Continue button selector
  [[ -n "${ELEPHANT_PREPARE_CONTINUE_BUTTON:-}" ]] && parts+=("ElephantPrepareContinueButton=\"$ELEPHANT_PREPARE_CONTINUE_BUTTON\"")

  # Browser flow template and parameters (use converted simple format)
  [[ -n "${ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE:-}" ]] && parts+=("ElephantPrepareBrowserFlowTemplate=\"$ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE\"")
  [[ -n "${ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS_SIMPLE:-}" ]] && parts+=("ElephantPrepareBrowserFlowParameters=\"$ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS_SIMPLE\"")

  # Updater schedule rate
  [[ -n "${UPDATER_SCHEDULE_RATE:-}" ]] && parts+=("UpdaterScheduleRate=\"$UPDATER_SCHEDULE_RATE\"")

  # GitHub integration parameter (required)
  : "${GITHUB_TOKEN?Set GITHUB_TOKEN to enable GitHub integration}"
  parts+=("GitHubToken=\"${GITHUB_TOKEN}\"")

  PARAM_OVERRIDES=("${parts[@]}")
}


upload_transforms_to_s3() {
  # Check if upload flag is set
  if [[ "$UPLOAD_TRANSFORMS" != "true" ]]; then
    info "UPLOAD_TRANSFORMS flag not set (default: false), skipping transform scripts upload"
    return 0
  fi

  local bucket prefix
  bucket=$(get_bucket)
  if [[ -z "$bucket" ]]; then
    # Bucket doesn't exist yet (first deploy). Upload after stack creation.
    TRANSFORMS_UPLOAD_PENDING=1
    return 0
  fi

  prefix="${TRANSFORM_PREFIX_KEY%/}"
  local s3_prefix="s3://$bucket/$prefix"
  info "Zipping and syncing county transforms to $s3_prefix"

  # Create temp directory
  local temp_dir=$(mktemp -d)
  trap "rm -rf '$temp_dir'" EXIT

  # List directories inside transform directory and zip each one
  for dir in "$TRANSFORMS_SRC_DIR"/*/; do
    if [[ -d "$dir" ]]; then
      local dirname=$(basename "$dir")
      info "Zipping $dirname"
      zip -r "$temp_dir/$dirname.zip" "$dir" || {
        err "Failed to zip $dir"
        return 1
      }
    fi
  done

  # Sync temp directory to S3
  aws s3 sync "$temp_dir" "$s3_prefix" --delete || {
    err "Failed to sync zips to $s3_prefix"
    return 1
  }


  TRANSFORM_S3_PREFIX_VALUE="s3://$bucket/$prefix"
  info "Transforms uploaded. Prefix: $TRANSFORM_S3_PREFIX_VALUE"
}

upload_browser_flows_to_s3() {
  local browser_flows_dir="browser-flows"

  # Check if browser-flows directory exists
  if [[ ! -d "$browser_flows_dir" ]]; then
    info "No browser-flows directory found, skipping browser flows upload"
    return 0
  fi

  local bucket
  bucket=$(get_bucket)
  if [[ -z "$bucket" ]]; then
    # Bucket doesn't exist yet (first deploy). Will be uploaded after stack creation.
    BROWSER_FLOWS_UPLOAD_PENDING=1
    return 0
  fi

  local s3_prefix="s3://$bucket/browser-flows"
  info "Syncing browser flows to $s3_prefix"

  # Upload all .json files from browser-flows directory
  aws s3 sync "$browser_flows_dir" "$s3_prefix" --exclude "*" --include "*.json" --delete || {
    warn "Failed to sync browser flows to $s3_prefix"
    return 1
  }

  info "Browser flows uploaded to: $s3_prefix"
}

upload_multi_request_flows_to_s3() {
  local multi_request_flows_dir="multi-request-flows"

  # Check if multi-request-flows directory exists
  if [[ ! -d "$multi_request_flows_dir" ]]; then
    info "No multi-request-flows directory found, skipping multi-request flows upload"
    return 0
  fi

  local bucket
  bucket=$(get_bucket)
  if [[ -z "$bucket" ]]; then
    # Bucket doesn't exist yet (first deploy). Will be uploaded after stack creation.
    MULTI_REQUEST_FLOWS_UPLOAD_PENDING=1
    return 0
  fi

  local s3_prefix="s3://$bucket/multi-request-flows"
  info "Syncing multi-request flows to $s3_prefix"

  # Upload all .json files from multi-request-flows directory
  aws s3 sync "$multi_request_flows_dir" "$s3_prefix" --exclude "*" --include "*.json" --delete || {
    warn "Failed to sync multi-request flows to $s3_prefix"
    return 1
  }

  info "Multi-request flows uploaded to: $s3_prefix"
}

upload_static_parts_to_s3() {
  local static_parts_dir="source-html-static-parts"

  # Check if source-html-static-parts directory exists
  if [[ ! -d "$static_parts_dir" ]]; then
    info "No source-html-static-parts directory found, skipping static parts upload"
    return 0
  fi

  local bucket
  bucket=$(get_bucket)
  if [[ -z "$bucket" ]]; then
    # Bucket doesn't exist yet (first deploy). Will be uploaded after stack creation.
    STATIC_PARTS_UPLOAD_PENDING=1
    return 0
  fi

  local s3_prefix="s3://$bucket/source-html-static-parts"
  info "Syncing static parts to $s3_prefix"

  # Upload all .csv files from source-html-static-parts directory
  aws s3 sync "$static_parts_dir" "$s3_prefix" --exclude "*" --include "*.csv" --delete || {
    warn "Failed to sync static parts to $s3_prefix"
    return 1
  }

  info "Static parts uploaded to: $s3_prefix"
}

package_and_upload_codebuild_runtime() {
  if [[ ! -d "$CODEBUILD_RUNTIME_MODULE_DIR" ]]; then
    warn "CodeBuild runtime module directory ($CODEBUILD_RUNTIME_MODULE_DIR) not found, skipping upload."
    return 0
  fi

  local bucket="${CODEBUILD_RUNTIME_BUCKET:-}"
  if [[ -z "$bucket" ]]; then
    bucket=$(get_bucket)
  fi

  if [[ -z "$bucket" ]]; then
    CODEBUILD_RUNTIME_UPLOAD_PENDING=1
    info "Delaying CodeBuild runtime module upload until environment bucket exists."
    return 0
  fi

  local prefix="${CODEBUILD_RUNTIME_PREFIX%/}"
  prefix="${prefix#/}"
  local dist_dir="codebuild/dist"
  mkdir -p "$dist_dir"

  local archive_path="${dist_dir}/${CODEBUILD_RUNTIME_ARCHIVE_NAME}"
  rm -f "$archive_path"

  info "Installing CodeBuild runtime module production dependencies"
  (
    cd "$CODEBUILD_RUNTIME_MODULE_DIR"
    npm ci --omit=dev >/dev/null
  ) || {
    err "Failed to install CodeBuild runtime module dependencies."
    exit 1
  }

  (
    cd "$CODEBUILD_RUNTIME_MODULE_DIR"
    zip -r "../dist/${CODEBUILD_RUNTIME_ARCHIVE_NAME}" . >/dev/null
  ) || {
    err "Failed to package CodeBuild runtime module."
    exit 1
  }

  local s3_key
  if [[ -n "$prefix" ]]; then
    s3_key="${prefix}/${CODEBUILD_RUNTIME_ARCHIVE_NAME}"
  else
    s3_key="${CODEBUILD_RUNTIME_ARCHIVE_NAME}"
  fi

  aws s3 cp "$archive_path" "s3://${bucket}/${s3_key}" >/dev/null || {
    err "Failed to upload CodeBuild runtime module to s3://${bucket}/${s3_key}"
    exit 1
  }
  rm -rf "$dist_dir"

  CODEBUILD_RUNTIME_UPLOAD_PENDING=0
  info "Uploaded CodeBuild runtime module to s3://${bucket}/${s3_key}"
}

deploy_codebuild_stack() {

  local openai_api_key="${OPENAI_API_KEY:-}"
  if [[ -z "$openai_api_key" ]]; then
    err "OPENAI_API_KEY is required"
    exit 1
  fi
  if [[ ! -f "$CODEBUILD_TEMPLATE" ]]; then
    warn "CodeBuild template not found at $CODEBUILD_TEMPLATE, skipping deployment."
    return 0
  fi

  local bucket="${CODEBUILD_RUNTIME_BUCKET:-}"
  if [[ -z "$bucket" ]]; then
    bucket=$(get_bucket)
  fi

  if [[ -z "$bucket" ]]; then
    CODEBUILD_DEPLOY_PENDING=1
    info "Delaying CodeBuild stack deployment until environment bucket exists."
    return 0
  fi

  # Get required stack outputs
  local errors_table_name
  errors_table_name=$(get_output "ErrorsTableName")
  if [[ -z "$errors_table_name" ]]; then
    CODEBUILD_DEPLOY_PENDING=1
    info "Delaying CodeBuild stack deployment until ErrorsTableName output is available."
    return 0
  fi

  local transactions_sqs_queue_url
  transactions_sqs_queue_url=$(get_output "TransactionsSqsQueueUrl")
  if [[ -z "$transactions_sqs_queue_url" ]]; then
    CODEBUILD_DEPLOY_PENDING=1
    info "Delaying CodeBuild stack deployment until TransactionsSqsQueueUrl output is available."
    return 0
  fi

  local post_processor_function_name
  post_processor_function_name=$(get_output "WorkflowPostProcessorFunctionName")
  if [[ -z "$post_processor_function_name" ]]; then
    CODEBUILD_DEPLOY_PENDING=1
    info "Delaying CodeBuild stack deployment until WorkflowPostProcessorFunctionName output is available."
    return 0
  fi

  local mvl_function_name
  mvl_function_name=$(get_output "WorkflowMirrorValidatorFunctionName")
  if [[ -z "$mvl_function_name" ]]; then
    CODEBUILD_DEPLOY_PENDING=1
    info "Delaying CodeBuild stack deployment until WorkflowMirrorValidatorFunctionName output is available."
    return 0
  fi

  local default_dlq_url
  default_dlq_url=$(get_output "MwaaDeadLetterQueueUrl")
  if [[ -z "$default_dlq_url" ]]; then
    CODEBUILD_DEPLOY_PENDING=1
    info "Delaying CodeBuild stack deployment until MwaaDeadLetterQueueUrl output is available."
    return 0
  fi

  local transform_s3_prefix="${TRANSFORM_S3_PREFIX_VALUE:-}"
  if [[ -z "$transform_s3_prefix" ]]; then
    # Try to construct it from bucket and prefix if TRANSFORM_S3_PREFIX_VALUE isn't set
    local transform_prefix_key="${TRANSFORM_PREFIX_KEY%/}"
    transform_prefix_key="${transform_prefix_key#/}"
    if [[ -n "$bucket" && -n "$transform_prefix_key" ]]; then
      transform_s3_prefix="s3://$bucket/$transform_prefix_key"
    else
      CODEBUILD_DEPLOY_PENDING=1
      info "Delaying CodeBuild stack deployment until TransformS3Prefix value is available."
      return 0
    fi
  fi

  local prefix="${CODEBUILD_RUNTIME_PREFIX%/}"
  prefix="${prefix#/}"
  local entrypoint="$CODEBUILD_RUNTIME_ENTRYPOINT"

  info "Deploying CodeBuild stack ($CODEBUILD_STACK_NAME) using artifacts bucket ${bucket}/${prefix}"
  sam deploy \
    --template-file "$CODEBUILD_TEMPLATE" \
    --stack-name "$CODEBUILD_STACK_NAME" \
    --capabilities CAPABILITY_IAM \
    --resolve-s3 \
    --no-confirm-changeset \
    --no-fail-on-empty-changeset \
    --parameter-overrides \
      EnvironmentName="$ENVIRONMENT_NAME" \
      RuntimeArtifactsBucket="$bucket" \
      RuntimeArtifactsPrefix="$prefix" \
      RuntimeEntryPoint="$entrypoint" \
      ErrorsTableName="$errors_table_name" \
      TransformS3Prefix="$transform_s3_prefix" \
      PostProcessorFunctionName="$post_processor_function_name" \
      MvlFunctionName="$mvl_function_name" \
      OpenAiApiKey="$openai_api_key" \
      TransactionsSqsQueueUrl="$transactions_sqs_queue_url" \
      DefaultDlqUrl="$default_dlq_url"

  CODEBUILD_DEPLOY_PENDING=0
}

# Note: MVL Lambda Docker image is now built and pushed automatically by SAM
# during sam_build and sam_deploy using --resolve-image-repos
# No manual push needed anymore

add_transform_prefix_override() {
  if [[ -z "${TRANSFORM_S3_PREFIX_VALUE:-}" ]]; then
    err "Transform S3 prefix value not set; aborting."
    exit 1
  fi
  PARAM_OVERRIDES+=("TransformS3Prefix=\"$TRANSFORM_S3_PREFIX_VALUE\"")
}

populate_proxy_rotation_table() {
  local proxy_file="${PROXY_FILE:-}"

  if [[ -z "$proxy_file" ]]; then
    info "No proxy file specified (PROXY_FILE environment variable not set), skipping proxy population"
    return 0
  fi

  if [[ ! -f "$proxy_file" ]]; then
    err "Proxy file not found: $proxy_file"
    exit 1
  fi

  local table_name
  table_name=$(get_output "ProxyRotationTableName")

  if [[ -z "$table_name" ]]; then
    err "ProxyRotationTable not found in stack outputs. Deploy the stack first."
    exit 1
  fi

  info "Populating proxy rotation table using Node.js script"

  # Use Node.js script to populate proxies
  node scripts/populate-proxies.mjs "$table_name" "$proxy_file" || {
    err "Failed to populate proxies"
    exit 1
  }
}

# Create or update GitHub token in AWS Secrets Manager
setup_github_secret() {
  if [[ -z "${GITHUB_SECRET_NAME:-}" || -z "${GITHUB_TOKEN:-}" ]]; then
    return 0
  fi

  info "Setting up GitHub token in Secrets Manager..."

  # Check if secret exists
  if aws secretsmanager describe-secret --secret-id "$GITHUB_SECRET_NAME" >/dev/null 2>&1; then
    info "Updating existing secret: $GITHUB_SECRET_NAME"
    aws secretsmanager update-secret \
      --secret-id "$GITHUB_SECRET_NAME" \
      --secret-string "{\"token\":\"$GITHUB_TOKEN\"}" >/dev/null || {
      err "Failed to update GitHub secret"
      exit 1
    }
  else
    info "Creating new secret: $GITHUB_SECRET_NAME"
    aws secretsmanager create-secret \
      --name "$GITHUB_SECRET_NAME" \
      --secret-string "{\"token\":\"$GITHUB_TOKEN\"}" \
      --description "GitHub personal access token for repository sync" >/dev/null || {
      err "Failed to create GitHub secret"
      exit 1
    }
  fi

  info "GitHub secret configured successfully"
}

# Write per-county repair flag(s) to SSM Parameter Store
write_repair_flags_to_ssm() {
  # Single county mode via REPAIR_COUNTY + REPAIR_VALUE
  if [[ -n "${REPAIR_COUNTY:-}" && -n "${REPAIR_VALUE:-}" ]]; then
    local county_key
    county_key=$(echo -n "$REPAIR_COUNTY" | sed 's/ /_/g')
    local name
    name="/${STACK_NAME}/repair/${county_key}"
    info "Setting repair flag for county '$REPAIR_COUNTY' (key: ${county_key}) to ${REPAIR_VALUE} at ${name}"
    aws ssm put-parameter --name "$name" --type String --value "$REPAIR_VALUE" --overwrite >/dev/null
  fi

  # Batch mode via REPAIR_COUNTIES_JSON: { "Palm Beach": true, "Escambia": false }
  if [[ -n "${REPAIR_COUNTIES_JSON:-}" ]]; then
    if ! echo "${REPAIR_COUNTIES_JSON}" | jq . >/dev/null 2>&1; then
      err "REPAIR_COUNTIES_JSON must be valid JSON object"
      exit 1
    fi
    local keys
    keys=$(echo "${REPAIR_COUNTIES_JSON}" | jq -r 'to_entries[] | @base64')
    while IFS= read -r entry; do
      local kv k v county_key name
      kv=$(echo "$entry" | base64 --decode)
      k=$(echo "$kv" | jq -r '.key')
      v=$(echo "$kv" | jq -r '.value | tostring')
      county_key=$(echo -n "$k" | sed 's/ /_/g')
      name="/${STACK_NAME}/repair/${county_key}"
      info "Setting repair flag for county '$k' (key: ${county_key}) to ${v} at ${name}"
      aws ssm put-parameter --name "$name" --type String --value "$v" --overwrite >/dev/null
    done <<< "$keys"
  fi
}

# Check the Lambda "Concurrent executions" service quota and request an increase if it's 10
ensure_lambda_concurrency_quota() {
  info "Checking Lambda 'Concurrent executions' service quota"
  local quota_code="L-B99A9384" # Concurrent executions
  local current desired=1000 resp req_id status

  # Try to fetch quota directly by code
  current=$(aws service-quotas get-service-quota \
    --service-code lambda \
    --quota-code "$quota_code" \
    --query 'Quota.Value' --output text 2>/dev/null || true)

  # Fallback via list if direct call didn't return a value
  if [[ -z "$current" || "$current" == "None" || "$current" == "null" ]]; then
    current=$(aws service-quotas list-service-quotas \
      --service-code lambda \
      --query "Quotas[?QuotaCode=='$quota_code'].Value | [0]" \
      --output text 2>/dev/null || true)
  fi

  if [[ -z "$current" || "$current" == "None" || "$current" == "null" ]]; then
    warn "Could not determine Lambda 'Concurrent executions' quota; skipping quota request"
    return 0
  fi

  info "Current Lambda concurrent executions quota: $current"

  # If the quota is 10 (handle values like 10 or 10.0), request increase to 1000
  local current_int
  current_int=${current%%.*}
  if [[ "$current_int" =~ ^[0-9]+$ && "$current_int" -eq 10 ]]; then
    info "Requesting quota increase to ${desired}"
    resp=$(aws service-quotas request-service-quota-increase \
      --service-code lambda \
      --quota-code "$quota_code" \
      --desired-value "$desired" 2>/dev/null || true)
    req_id=$(echo "$resp" | jq -r '.RequestedQuota.Id // empty')
    status=$(echo "$resp" | jq -r '.RequestedQuota.Status // empty')
    if [[ -n "$req_id" ]]; then
      info "Submitted quota increase request. Id: $req_id Status: $status"
    else
      warn "Failed to submit quota increase request. Response: $resp"
    fi
  else
    info "No increase requested (quota not equal to 10)"
  fi
}

handle_pending_keystore_upload() {
  if [[ -n "${KEYSTORE_S3_KEY_PENDING:-}" && -n "${KEYSTORE_FILE_PENDING:-}" ]]; then
    local bucket=$(get_bucket)
    if [[ -n "$bucket" ]]; then
      info "Uploading pending keystore file to S3..."
      aws s3 cp "$KEYSTORE_FILE_PENDING" "s3://$bucket/$KEYSTORE_S3_KEY_PENDING"

      # Update the stack with the actual S3 key
      ELEPHANT_KEYSTORE_S3_KEY="$KEYSTORE_S3_KEY_PENDING"
      compute_param_overrides
      info "Updating stack with keystore S3 key..."
      sam_deploy
    fi
  fi
}

apply_county_configs() {
  # Check if there are any county-specific environment variables
  local has_county_vars=false
  for var in $(env | grep -E "^ELEPHANT_PREPARE_(USE_BROWSER|NO_FAST|NO_CONTINUE|IGNORE_CAPTCHA|CONTINUE_BUTTON|BROWSER_FLOW_TEMPLATE|BROWSER_FLOW_PARAMETERS)_[A-Za-z]+" | cut -d= -f1 || true); do
    if [[ -n "$var" ]]; then
      has_county_vars=true
      break
    fi
  done

  if [[ "$has_county_vars" == true ]]; then
    info "Detected county-specific configurations. Applying them..."
    if ./scripts/set-county-configs.sh; then
      info "County-specific configurations applied successfully"
    else
      warn "Failed to apply some county-specific configurations"
    fi
  fi
}

main() {
  check_prereqs
  ensure_lambda_concurrency_quota

  # Setup GitHub secret if GitHub integration is enabled
  setup_github_secret

  compute_param_overrides

  # Upload transforms only if flag is set
  if [[ "$UPLOAD_TRANSFORMS" == "true" ]]; then
    upload_transforms_to_s3
  fi

  upload_browser_flows_to_s3
  upload_multi_request_flows_to_s3
  upload_static_parts_to_s3
  package_and_upload_codebuild_runtime
  deploy_codebuild_stack

  # Handle TransformS3Prefix parameter
  if [[ "$UPLOAD_TRANSFORMS" == "true" ]]; then
    # Upload was attempted - handle pending or completed state
    if (( TRANSFORMS_UPLOAD_PENDING == 0 )); then
      add_transform_prefix_override
    else
      info "Delaying transform upload until stack bucket exists."
      # Will be set after stack creation in pending upload handler
      TRANSFORMS_UPLOAD_PENDING=1
    fi
  elif [[ -n "${TRANSFORM_S3_PREFIX_VALUE:-}" ]]; then
    # Upload flag not set, but manual prefix value provided
    add_transform_prefix_override
  else
    # No upload and no manual prefix - construct valid S3 URI to default location
    local bucket=$(get_bucket)
    if [[ -n "$bucket" ]]; then
      local prefix="${TRANSFORM_PREFIX_KEY%/}"
      TRANSFORM_S3_PREFIX_VALUE="s3://$bucket/$prefix"
      info "Using existing transforms location: $TRANSFORM_S3_PREFIX_VALUE"
      add_transform_prefix_override
    else
      # First deployment - will be set after stack is created
      info "Transform scripts upload skipped, will use default location after stack creation"
    fi
  fi

  sam_build
  sam_deploy

  # Handle pending scenarios after stack creation
  local need_redeploy=false

  # Handle transform upload/configuration if pending
  if [[ "$UPLOAD_TRANSFORMS" == "true" ]] && (( TRANSFORMS_UPLOAD_PENDING == 1 )); then
    upload_transforms_to_s3
    # Recompute all parameters with the actual transform prefix
    compute_param_overrides
    add_transform_prefix_override
    need_redeploy=true
  elif [[ -z "${TRANSFORM_S3_PREFIX_VALUE:-}" ]]; then
    # No upload flag and no manual prefix - set default location now that bucket exists
    local bucket=$(get_bucket)
    if [[ -n "$bucket" ]]; then
      local prefix="${TRANSFORM_PREFIX_KEY%/}"
      TRANSFORM_S3_PREFIX_VALUE="s3://$bucket/$prefix"
      info "Setting default transforms location: $TRANSFORM_S3_PREFIX_VALUE"
      compute_param_overrides
      add_transform_prefix_override
      need_redeploy=true
    fi
  fi

  # Redeploy if any pending items were resolved
  if [[ "$need_redeploy" == true ]]; then
    sam_deploy
  fi

  if (( CODEBUILD_RUNTIME_UPLOAD_PENDING == 1 )); then
    package_and_upload_codebuild_runtime
    deploy_codebuild_stack
  fi

  # Upload browser flows if pending
  if (( BROWSER_FLOWS_UPLOAD_PENDING == 1 )); then
    upload_browser_flows_to_s3
  fi

  # Upload multi-request flows if pending
  if (( MULTI_REQUEST_FLOWS_UPLOAD_PENDING == 1 )); then
    upload_multi_request_flows_to_s3
  fi

  # Upload static parts if pending
  if (( STATIC_PARTS_UPLOAD_PENDING == 1 )); then
    upload_static_parts_to_s3
  fi

  handle_pending_keystore_upload

  # Apply county-specific configurations if present
  apply_county_configs

  # Populate proxy rotation table if proxy file is provided
  populate_proxy_rotation_table

  # Write per-county repair flags to SSM if provided
  write_repair_flags_to_ssm

  if (( CODEBUILD_DEPLOY_PENDING == 1 )); then
    deploy_codebuild_stack
  fi

  bucket=$(get_bucket)
  echo
  info "Done!"
  if [[ -n "$bucket" ]]; then
    info "Environment bucket: $bucket"
  else
    info "Stack deployed successfully"
  fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
