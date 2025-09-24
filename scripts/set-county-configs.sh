#!/usr/bin/env bash
set -euo pipefail

# Script to set county-specific environment variables for the DownloaderFunction Lambda
# This runs after the main stack deployment to add county-specific configurations

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }

# Config
STACK_NAME="${STACK_NAME:-elephant-oracle-node}"
FUNCTION_NAME=""

# Get the Lambda function name from the stack
get_lambda_function_name() {
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`DownloaderFunctionName`].OutputValue' \
    --output text 2>/dev/null || echo ""
}

# Get current environment variables from Lambda
get_current_env_vars() {
  aws lambda get-function-configuration \
    --function-name "$FUNCTION_NAME" \
    --query 'Environment.Variables' \
    --output json 2>/dev/null || echo "{}"
}

# Update Lambda environment variables
update_env_vars() {
  local env_json=$1

  # Create a temporary file for the environment variables
  local tmp_file=$(mktemp)
  echo "$env_json" | jq -c '{Environment: {Variables: .}}' > "$tmp_file"

  # Update using CLI input JSON file
  aws lambda update-function-configuration \
    --function-name "$FUNCTION_NAME" \
    --cli-input-json "file://$tmp_file" \
    --no-cli-pager >/dev/null

  local exit_code=$?

  # Clean up temp file
  rm -f "$tmp_file"

  return $exit_code
}

# Process county-specific environment variables
process_county_configs() {
  # Get current environment variables
  local current_vars=$(get_current_env_vars)
  local new_vars=$current_vars

  # Track if any county-specific variables were found
  local found_county_vars=false

  # List of configuration variables that can be county-specific
  local config_vars=(
    "ELEPHANT_PREPARE_USE_BROWSER"
    "ELEPHANT_PREPARE_NO_FAST"
    "ELEPHANT_PREPARE_NO_CONTINUE"
    "ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE"
    "ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS"
  )

  # Check for any county-specific environment variables
  for var_base in "${config_vars[@]}"; do
    # Look for any environment variable that starts with the base name and has a county suffix
    for env_var in $(env | grep "^${var_base}_" | cut -d= -f1 || true); do
      if [[ -n "$env_var" ]]; then
        local county_suffix="${env_var#${var_base}_}"
        local value="${!env_var}"

        # Special handling for JSON parameters - convert to simple format
        if [[ "$var_base" == "ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS" ]] && [[ "$value" =~ ^\{ ]]; then
          if command -v jq >/dev/null 2>&1; then
            # Convert JSON to simple key:value format
            value=$(echo "$value" | jq -r 'to_entries | map("\(.key):\(.value)") | join(",")')
            info "Converted JSON to simple format for ${env_var}" >&2
          else
            warn "jq not found - passing JSON as-is for ${env_var}" >&2
          fi
        fi

        info "Found county-specific config: ${env_var}='${value}'" >&2

        # Add to the environment variables JSON
        new_vars=$(echo "$new_vars" | jq --arg key "$env_var" --arg val "$value" '. + {($key): $val}')
        found_county_vars=true
      fi
    done
  done

  if [[ "$found_county_vars" == true ]]; then
    echo "$new_vars"
    return 0
  else
    return 1
  fi
}

# Main function
main() {
  info "Setting county-specific configurations for Lambda function..."

  # Check prerequisites
  command -v aws >/dev/null || { err "aws CLI not found"; exit 1; }
  command -v jq >/dev/null || { err "jq not found"; exit 1; }
  aws sts get-caller-identity >/dev/null || { err "AWS credentials not configured"; exit 1; }

  # Get Lambda function name from stack
  FUNCTION_NAME=$(get_lambda_function_name)

  if [[ -z "$FUNCTION_NAME" ]]; then
    err "Could not find DownloaderFunction in stack $STACK_NAME"
    err "Make sure the stack is deployed first with: ./scripts/deploy-infra.sh"
    exit 1
  fi

  info "Lambda function: $FUNCTION_NAME"

  info "Processing county-specific configurations..."

  # Process county configurations (capture JSON output separately)
  local new_env_json
  new_env_json=$(process_county_configs 2>&1 >/dev/null | grep -v "^\[" || true)

  # Check if we actually got any county configurations
  if process_county_configs >/dev/null 2>&1; then
    # Re-run to get clean JSON output
    new_env_json=$(process_county_configs 2>/dev/null)

    info "Updating Lambda environment variables..."

    if update_env_vars "$new_env_json"; then
      info "✅ Successfully updated Lambda environment variables"

      # Display the county-specific variables that were set
      echo
      info "County-specific configurations:"
      echo "$new_env_json" | jq -r 'to_entries[] | select(.key | test("_(Alachua|Sarasota|Broward|Charlotte|Duval|Hillsborough|Lake|Lee|Leon|Manatee|PalmBeach|Pinellas|Polk|Santa|[A-Za-z]+)$")) | "  \(.key)=\(.value)"'
    else
      err "Failed to update Lambda environment variables"
      exit 1
    fi
  else
    info "No county-specific environment variables found"
    info "To set county-specific configurations, export variables like:"
    echo "  export ELEPHANT_PREPARE_USE_BROWSER_Alachua=true"
    echo "  export ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE_Sarasota=SEARCH_BY_PARCEL"
    echo "  export ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS_Sarasota='{\"timeout\": 30000}'"
  fi
}

# Run main function
main "$@"