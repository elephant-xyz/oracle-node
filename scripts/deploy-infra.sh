#!/usr/bin/env bash
set -euo pipefail

# Unified deployment for SAM stack (MWAA + VPC + SQS + Lambdas)

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }

# Config with sane defaults
STACK_NAME="${STACK_NAME:-elephant-oracle-node}"
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
TRANSFORMS_UPLOAD_PENDING=0
BROWSER_FLOWS_UPLOAD_PENDING=0

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
  command -v git >/dev/null || { err "git not found"; exit 1; }
  command -v npm >/dev/null || { err "npm not found"; exit 1; }
  [[ -f "$SAM_TEMPLATE" && -f "$STARTUP_SCRIPT" && -f "$PYPROJECT_FILE" ]] || { err "Missing required files"; exit 1; }
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
  sam build --template-file "$SAM_TEMPLATE" >/dev/null
}

sam_deploy() {
  info "Deploying SAM stack (initial)"
  sam deploy \
    --template-file "$BUILT_TEMPLATE" \
    --stack-name "$STACK_NAME" \
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
    --resolve-s3 \
    --no-confirm-changeset \
    --no-fail-on-empty-changeset \
    --parameter-overrides ${PARAM_OVERRIDES:-} >/dev/null
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
      ${PARAM_OVERRIDES:-} \
      StartupScriptS3Path="startup.sh" \
      StartupScriptS3ObjectVersion="$script_ver" \
      RequirementsS3Path="requirements.txt" \
      RequirementsS3ObjectVersion="$req_ver" >/dev/null
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

  # No need for parameter file with simple format
  local use_param_file=false

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

    local parts=()
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

    local parts=()
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

  PARAM_OVERRIDES="${parts[*]}"
}


upload_transforms_to_s3() {
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

  # Upload manifest file
  local manifest_s3_path="$prefix/manifest.json"
  aws s3 cp "$TRANSFORM_MANIFEST_FILE" "s3://$bucket/$manifest_s3_path" || {
    err "Failed to upload manifest to s3://$bucket/$manifest_s3_path"
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

add_transform_prefix_override() {
  if [[ -z "${TRANSFORM_S3_PREFIX_VALUE:-}" ]]; then
    err "Transform S3 prefix value not set; aborting."
    exit 1
  fi
  if [[ -z "${PARAM_OVERRIDES:-}" ]]; then
    PARAM_OVERRIDES="TransformS3Prefix=\"$TRANSFORM_S3_PREFIX_VALUE\""
  else
    PARAM_OVERRIDES+=" TransformS3Prefix=\"$TRANSFORM_S3_PREFIX_VALUE\""
  fi
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

  compute_param_overrides
  upload_transforms_to_s3
  upload_browser_flows_to_s3

  if (( TRANSFORMS_UPLOAD_PENDING == 0 )); then
    add_transform_prefix_override
  else
    info "Delaying transform upload until stack bucket exists."
    # Add placeholder value for initial deployment
    if [[ -z "${PARAM_OVERRIDES:-}" ]]; then
      PARAM_OVERRIDES="TransformS3Prefix=\"pending\""
    else
      PARAM_OVERRIDES+=" TransformS3Prefix=\"pending\""
    fi
  fi

  sam_build
  sam_deploy

  if (( TRANSFORMS_UPLOAD_PENDING == 1 )); then
    upload_transforms_to_s3
    # Recompute all parameters with the actual transform prefix
    compute_param_overrides
    add_transform_prefix_override
    sam_deploy
  fi

  # Upload browser flows if pending
  if (( BROWSER_FLOWS_UPLOAD_PENDING == 1 )); then
    upload_browser_flows_to_s3
  fi

  handle_pending_keystore_upload

  # Apply county-specific configurations if present
  apply_county_configs

  # Populate proxy rotation table if proxy file is provided
  populate_proxy_rotation_table

  # Write per-county repair flags to SSM if provided
  write_repair_flags_to_ssm

  bucket=$(get_bucket)
  echo
  info "Done!"
  if [[ -n "$bucket" ]]; then
    info "Environment bucket: $bucket"
  else
    info "Stack deployed successfully"
  fi
}

main "$@"
