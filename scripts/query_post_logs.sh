#!/usr/bin/env bash

set -euo pipefail

# Logging setup like deploy-infra.sh
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }

usage() {
  cat <<'EOF'
Usage: query_post_logs.sh [--stack <stack-name>] [--start <ISO-8601>] [--end <ISO-8601>] [--profile <aws_profile>] [--region <aws_region>]

Summarize Elephant post-processing Lambda logs by county using CloudWatch Logs Insights.

Options:
  --stack, -s       CloudFormation stack name (default: elephant-oracle-node)
  --start           ISO-8601 start time (default: one hour ago, UTC)
  --end             ISO-8601 end time (default: now, UTC)
  --profile         AWS profile for credentials (optional)
  --region          AWS region override (optional)
  --help            Show this help text

Examples:
  ./scripts/query_post_logs.sh
  ./scripts/query_post_logs.sh -s my-stack-name --start 2025-09-24T10:00 --end 2025-09-24T12:00
EOF
}

check_dependencies() {
  info "Checking dependencies..."
  command -v aws >/dev/null 2>&1 || {
    err "aws CLI not found in PATH"
    exit 1
  }
  command -v jq >/dev/null 2>&1 || {
    err "jq is required for processing query results"
    exit 1
  }
  command -v date >/dev/null 2>&1 || {
    err "date command not available"
    exit 1
  }
  info "All dependencies found"
}

# Default values
STACK_NAME="elephant-oracle-node"
START_TIME=""
END_TIME=""
AWS_PROFILE="${AWS_PROFILE:-}"
AWS_REGION="${AWS_REGION:-}"

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --stack|-s)
        STACK_NAME="$2"
        shift 2
        ;;
      --start)
        START_TIME="$2"
        shift 2
        ;;
      --end)
        END_TIME="$2"
        shift 2
        ;;
      --profile)
        AWS_PROFILE="$2"
        shift 2
        ;;
      --region)
        AWS_REGION="$2"
        shift 2
        ;;
      --help)
        usage
        exit 0
        ;;
      *)
        echo "Unknown argument: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
}

iso_to_epoch() {
  local input="$1"
  if [[ -z "$input" ]]; then
    echo ""
    return
  fi
  local normalized="$input"
  if [[ "$normalized" == *Z ]]; then
    normalized="${normalized%Z}"
  fi
  # date -d works with many ISO formats; fallback to fail for invalid input.
  if ! date -u -d "$normalized" +%s >/dev/null 2>&1; then
    echo "Error: invalid timestamp '$input'. Use ISO-8601 like 2025-09-24T10:00" >&2
    exit 1
  fi
  date -u -d "$normalized" +%s
}

calculate_time_range() {
  info "Calculating time range for query..."
  local now_epoch
  now_epoch=$(date -u +%s)

  if [[ -z "$END_TIME" ]]; then
    END_EPOCH=$now_epoch
  else
    END_EPOCH=$(iso_to_epoch "$END_TIME")
  fi

  if [[ -z "$START_TIME" ]]; then
    START_EPOCH=$((END_EPOCH - 3600))
  else
    START_EPOCH=$(iso_to_epoch "$START_TIME")
  fi

  if (( START_EPOCH >= END_EPOCH )); then
    err "--start must be earlier than --end"
    exit 1
  fi

  info "Query time range: $(date -u -d "@$START_EPOCH") to $(date -u -d "@$END_EPOCH")"
}

get_log_group_from_stack() {
  info "Querying CloudFormation stack for log group name..."
  local log_group
  log_group=$(aws "${AWS_ARGS[@]}" cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='WorkflowPostProcessorLogGroupName'].OutputValue" \
    --output text)

  if [[ -z "$log_group" || "$log_group" == "None" ]]; then
    err "Could not find WorkflowPostProcessorLogGroupName output in stack $STACK_NAME"
    exit 1
  fi

  echo "$log_group"
}

build_aws_args() {
  AWS_ARGS=()
  if [[ -n "$AWS_PROFILE" ]]; then
    AWS_ARGS+=("--profile" "$AWS_PROFILE")
  fi
  if [[ -n "$AWS_REGION" ]]; then
    AWS_ARGS+=("--region" "$AWS_REGION")
  fi
}

join_log_groups() {
  local delimiter="$1"
  shift
  local first=1
  local result=""
  for item in "$@"; do
    if (( first )); then
      result="$item"
      first=0
    else
      result+="$delimiter$item"
    fi
  done
  echo "$result"
}

run_insights_query() {
  local query_string="$1"
  local log_group="$2"

  info "Running CloudWatch Insights query on log group: $log_group"

  # Check if log group exists
  if ! aws logs describe-log-groups "${AWS_ARGS[@]}" --log-group-name-prefix "$log_group" --query 'logGroups[0].logGroupName' --output text >/dev/null 2>&1; then
    err "Log group $log_group does not exist"
    exit 1
  fi

  local -a start_query_cmd=(
    aws logs start-query
    "${AWS_ARGS[@]}"
    --start-time "$START_EPOCH"
    --end-time "$END_EPOCH"
    --query-string "$query_string"
    --log-group-name "$log_group"
  )

  local query_id
  query_id=$("${start_query_cmd[@]}" | jq -r '.queryId')
  if [[ -z "$query_id" || "$query_id" == "null" ]]; then
    err "Failed to start CloudWatch Insights query"
    exit 1
  fi

  info "Started query with ID: $query_id"

  local status="Running"
  local result_json
  while [[ "$status" == "Running" || "$status" == "Scheduled" || "$status" == "Unknown" ]]; do
    sleep 1
    result_json=$(aws logs get-query-results "${AWS_ARGS[@]}" --query-id "$query_id")
    status=$(echo "$result_json" | jq -r '.status')
  done

  if [[ "$status" != "Complete" ]]; then
    err "Query $query_id finished with status: $status"
    exit 1
  fi

  info "Query completed successfully"
  echo "$result_json" | jq -c '.results[] | map({(.field): .value}) | add'
}

# Query strings mirror CloudWatch Logs Insights syntax.
SUCCESS_QUERY='fields level, msg, county, transaction_items_count
| filter component = "post"
| stats
    sum(if msg = "post_lambda_complete", coalesce(transaction_items_count, 0), 0) as totalSuccessfulTransactions,
    sum(if msg = "post_lambda_complete", 1, 0) as successExecutions,
    sum(if level = "error", 1, 0) as failedExecutions
  by county
| eval successRate = if((successExecutions + failedExecutions) = 0, 0, successExecutions * 100 / (successExecutions + failedExecutions))
| sort county asc'

FAILURE_QUERY='fields county, msg, step
| filter component = "post" and level = "error"
| stats count() as failureCount by county, step
| sort county asc, failureCount desc'

aggregate_results() {
  local success_json="$1"
  local failure_json="$2"

  jq -n --argjson success "$success_json" --argjson failure "$failure_json" '
    def toNum($value):
      if ($value == null or $value == "") then 0 else ($value | tonumber? // 0) end;

    def indexRows($rows):
      reduce $rows[] as $row ({};
        .[( $row.county // "unknown" )] = {
          total_successful_transactions: toNum($row.totalSuccessfulTransactions),
          success_rate: toNum($row.successRate),
          success_executions: toNum($row.successExecutions),
          failed_executions: toNum($row.failedExecutions),
          failure_counts: {}
        }
      );

    def mergeFailures($base; $rows):
      reduce $rows[] as $row ($base;
        .[( $row.county // "unknown" )] //= {
          total_successful_transactions: 0,
          success_rate: 0,
          success_executions: 0,
          failed_executions: 0,
          failure_counts: {}
        };
        .[( $row.county // "unknown" )].failure_counts[( $row.step // "unknown" )] = toNum($row.failureCount)
      );

    (indexRows($success))
    | mergeFailures(.; $failure)
  '
}

format_output() {
  local aggregated_json="$1"

  echo "$aggregated_json" | jq -r '
    def formatRate(success_rate; success_execs; failed_execs):
      (success_execs + failed_execs) as $total
      | if $total == 0 then "0.00% (0 successes / 0 runs)"
        else
          (success_rate | tonumber? // 0) as $rate
          | "\($rate | tonumber? // 0 | tostring)% (\(success_execs) successes / \($total) runs)"
        end;

    to_entries
    | if length == 0 then
        "No log entries matched the provided criteria."
      else
        map(
          "County: \(.key)\n" +
          "  Total executions: \(.value.success_executions + .value.failed_executions)\n" +
          "  Success rate: \(formatRate(.value.success_rate; .value.success_executions; .value.failed_executions))\n" +
          "  Total successful transactions: \(.value.total_successful_transactions)\n" +
          (if (.value.failure_counts | type) == "object" and (.value.failure_counts | length) > 0 then
            "  Failure counts by step:\n" +
            ( .value.failure_counts | to_entries | sort_by(.key) | map("    \(.key): \(.value)") | join("\n") )
          else
            "  Failure counts: none"
          end)
        )
        | join("\n\n")
      end
  '
}

main() {
  info "Starting post-processing logs query"
  check_dependencies
  parse_args "$@"
  calculate_time_range
  build_aws_args

  info "Using CloudFormation stack: $STACK_NAME"
  local log_group
  log_group=$(get_log_group_from_stack)
  info "Found log group: $log_group"

  info "Querying success metrics..."
  local success_rows failure_rows aggregated
  local raw_success
  raw_success=$(run_insights_query "$SUCCESS_QUERY" "$log_group")
  info "Raw success query result: $raw_success"
  success_rows=$(echo "$raw_success" | jq -s '.')
  info "Querying failure metrics..."
  local raw_failure
  raw_failure=$(run_insights_query "$FAILURE_QUERY" "$log_group")
  info "Raw failure query result: $raw_failure"
  failure_rows=$(echo "$raw_failure" | jq -s '.')

  info "Aggregating results..."
  aggregated=$(aggregate_results "$success_rows" "$failure_rows")
  info "Formatting output..."
  format_output "$aggregated"
  info "Query completed successfully"
}

main "$@"

