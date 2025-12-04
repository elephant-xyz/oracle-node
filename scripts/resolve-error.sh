#!/usr/bin/env bash
# Script to mark errors as resolved by sending ElephantErrorResolved events to EventBridge.
# This triggers the workflow-events/lambdas/event-handler to delete error records from DynamoDB.
#
# Usage:
#   ./resolve-error.sh --execution-id <execution-id>    # Resolve all errors for an execution
#   ./resolve-error.sh --error-code <error-code>        # Resolve a specific error across all executions
#   ./resolve-error.sh --dry-run ...                    # Show what would be sent without sending
#
# Examples:
#   ./resolve-error.sh --execution-id "arn:aws:states:us-east-1:123456789:execution:..."
#   ./resolve-error.sh --error-code "30abc123def456"
#   ./resolve-error.sh --error-code "20Cook"
#   ./resolve-error.sh --execution-id "my-exec-id" --dry-run

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*" >&2; }
debug() { echo -e "${BLUE}[DEBUG]${NC} $*"; }

# Configuration
REGION="${AWS_REGION:-us-east-1}"
EVENT_SOURCE="elephant.workflow"
DETAIL_TYPE="ElephantErrorResolved"

# Arguments
EXECUTION_ID=""
ERROR_CODE=""
DRY_RUN=false

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Mark errors as resolved by sending ElephantErrorResolved events to EventBridge.

OPTIONS:
  --execution-id <id>   Execution ID - resolves all errors from this execution
                        across ALL executions that have the same error codes
  --error-code <code>   Error code - resolves this specific error from ALL executions
  --dry-run             Show what would be sent without sending the event
  -h, --help            Show this help message

EXAMPLES:
  # Resolve all errors for a specific execution
  $(basename "$0") --execution-id "arn:aws:states:us-east-1:123:execution:my-state-machine:abc-123"

  # Resolve a specific error code across all executions
  $(basename "$0") --error-code "30abc123def456"

  # Resolve a transform error for a county
  $(basename "$0") --error-code "20Cook"

  # Dry run to see what would be sent
  $(basename "$0") --execution-id "my-exec-id" --dry-run

NOTES:
  - At least one of --execution-id or --error-code must be provided
  - If --execution-id is provided, ALL error codes from that execution will be
    resolved across ALL executions that have those same errors
  - If only --error-code is provided, that specific error will be resolved
    from ALL executions that have it
  - Events are sent to the default EventBridge bus

EOF
  exit 0
}

check_prereqs() {
  info "Checking prerequisites..."
  
  if ! command -v jq &>/dev/null; then
    err "jq not found. Please install it: https://jqlang.github.io/jq/download/"
    exit 1
  fi
  
  # Skip AWS checks in dry-run mode
  if [[ "$DRY_RUN" == true ]]; then
    debug "Dry-run mode: skipping AWS credentials check"
    return 0
  fi
  
  if ! command -v aws &>/dev/null; then
    err "aws CLI not found. Please install it: https://aws.amazon.com/cli/"
    exit 1
  fi
  
  if ! aws sts get-caller-identity &>/dev/null; then
    err "AWS credentials not configured. Run 'aws configure' or set AWS_PROFILE"
    exit 1
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case $1 in
      --execution-id)
        if [[ -z "${2:-}" ]]; then
          err "--execution-id requires a value"
          exit 1
        fi
        EXECUTION_ID="$2"
        shift 2
        ;;
      --error-code)
        if [[ -z "${2:-}" ]]; then
          err "--error-code requires a value"
          exit 1
        fi
        ERROR_CODE="$2"
        shift 2
        ;;
      --dry-run)
        DRY_RUN=true
        shift
        ;;
      -h|--help)
        usage
        ;;
      *)
        err "Unknown argument: $1"
        echo ""
        usage
        ;;
    esac
  done

  # Validate that at least one identifier is provided
  if [[ -z "$EXECUTION_ID" && -z "$ERROR_CODE" ]]; then
    err "At least one of --execution-id or --error-code must be provided"
    echo ""
    usage
  fi
}

build_event_detail() {
  local detail="{}"
  
  if [[ -n "$EXECUTION_ID" ]]; then
    detail=$(echo "$detail" | jq --arg id "$EXECUTION_ID" '. + {executionId: $id}')
  fi
  
  if [[ -n "$ERROR_CODE" ]]; then
    detail=$(echo "$detail" | jq --arg code "$ERROR_CODE" '. + {errorCode: $code}')
  fi
  
  echo "$detail"
}

send_event() {
  local detail="$1"
  
  local event_entry
  event_entry=$(jq -n \
    --arg source "$EVENT_SOURCE" \
    --arg detailType "$DETAIL_TYPE" \
    --arg detail "$detail" \
    '{
      Source: $source,
      DetailType: $detailType,
      Detail: $detail
    }')
  
  info "Sending EventBridge event..."
  debug "Event source: $EVENT_SOURCE"
  debug "Detail type: $DETAIL_TYPE"
  debug "Event detail:"
  echo "$detail" | jq .
  
  if [[ "$DRY_RUN" == true ]]; then
    warn "DRY RUN - Event NOT sent"
    info "Would send the following event:"
    echo "$event_entry" | jq .
    return 0
  fi
  
  local response
  response=$(aws events put-events \
    --region "$REGION" \
    --entries "$event_entry" \
    --output json)
  
  local failed_count
  failed_count=$(echo "$response" | jq -r '.FailedEntryCount')
  
  if [[ "$failed_count" -gt 0 ]]; then
    err "Failed to send event!"
    echo "$response" | jq '.Entries[] | select(.ErrorCode != null)'
    exit 1
  fi
  
  local event_id
  event_id=$(echo "$response" | jq -r '.Entries[0].EventId')
  info "Event sent successfully!"
  info "Event ID: $event_id"
}

main() {
  parse_args "$@"
  check_prereqs
  
  echo ""
  info "=== Resolve Error Script ==="
  info "Region: $REGION"
  
  if [[ -n "$EXECUTION_ID" ]]; then
    info "Execution ID: $EXECUTION_ID"
  fi
  
  if [[ -n "$ERROR_CODE" ]]; then
    info "Error Code: $ERROR_CODE"
  fi
  
  if [[ "$DRY_RUN" == true ]]; then
    warn "DRY RUN MODE - No changes will be made"
  fi
  
  echo ""
  
  local detail
  detail=$(build_event_detail)
  
  send_event "$detail"
  
  echo ""
  if [[ "$DRY_RUN" == false ]]; then
    if [[ -n "$EXECUTION_ID" ]]; then
      info "All error codes from execution '$EXECUTION_ID' will be resolved"
      info "This affects ALL executions that share these error codes"
    fi
    
    if [[ -n "$ERROR_CODE" && -z "$EXECUTION_ID" ]]; then
      info "Error code '$ERROR_CODE' will be resolved from ALL executions"
    fi
    
    info "Check CloudWatch Logs for the event-handler Lambda to monitor progress"
  fi
}

main "$@"

