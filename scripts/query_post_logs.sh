#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: query_post_logs.sh --log-group <name> [--log-group <name> ...] [--start <ISO-8601>] [--end <ISO-8601>] [--profile <aws_profile>] [--region <aws_region>]

Summarize Elephant post-processing Lambda logs by county using CloudWatch Logs Insights.

Options:
  --log-group, -g   CloudWatch Logs group (may be provided multiple times, required)
  --start           ISO-8601 start time (default: one hour ago, UTC)
  --end             ISO-8601 end time (default: now, UTC)
  --profile         AWS profile for credentials (optional)
  --region          AWS region override (optional)
  --help            Show this help text

Example:
  ./scripts/query_post_logs.sh -g /aws/lambda/ElephantExpressPostFunction --start 2025-09-24T10:00 --end 2025-09-24T12:00
EOF
}

check_dependencies() {
  command -v aws >/dev/null 2>&1 || {
    echo "Error: aws CLI not found in PATH." >&2
    exit 1
  }
  command -v jq >/dev/null 2>&1 || {
    echo "Error: jq is required for processing query results." >&2
    exit 1
  }
  command -v date >/dev/null 2>&1 || {
    echo "Error: date command not available." >&2
    exit 1
  }
}

# Default values
declare -a LOG_GROUPS=()
START_TIME=""
END_TIME=""
AWS_PROFILE=""
AWS_REGION=""

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --log-group|-g)
        LOG_GROUPS+=("$2")
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

  if [[ ${#LOG_GROUPS[@]} -eq 0 ]]; then
    echo "Error: at least one --log-group must be specified." >&2
    usage
    exit 1
  fi
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
    echo "Error: --start must be earlier than --end." >&2
    exit 1
  fi
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

  local -a start_query_cmd=(
    aws logs start-query
    "${AWS_ARGS[@]}"
    --start-time "$START_EPOCH"
    --end-time "$END_EPOCH"
    --query-string "$query_string"
  )

  for lg in "${LOG_GROUPS[@]}"; do
    start_query_cmd+=(--log-group-name "$lg")
  done

  local query_id
  query_id=$("${start_query_cmd[@]}" | jq -r '.queryId')
  if [[ -z "$query_id" || "$query_id" == "null" ]]; then
    echo "Error: failed to start query." >&2
    exit 1
  fi

  local status="Running"
  local result_json
  while [[ "$status" == "Running" || "$status" == "Scheduled" || "$status" == "Unknown" ]]; do
    sleep 1
    result_json=$(aws logs get-query-results "${AWS_ARGS[@]}" --query-id "$query_id")
    status=$(echo "$result_json" | jq -r '.status')
  done

  if [[ "$status" != "Complete" ]]; then
    echo "Error: query $query_id finished with status $status" >&2
    exit 1
  fi

  echo "$result_json" | jq -c '.results[] | map({(.field): .value}) | add'
}

# Query strings mirror CloudWatch Logs Insights syntax.
read -r -d '' SUCCESS_QUERY <<'EOF'
fields level, msg, county, transaction_items_count
| filter component = "post"
| stats
    sum(if msg = "post_lambda_complete", coalesce(transaction_items_count, 0), 0) as totalSuccessfulTransactions,
    sum(if msg = "post_lambda_complete", 1, 0) as successExecutions,
    sum(if level = "error", 1, 0) as failedExecutions,
    sum(if msg = "transform_failed", 1, 0) as transformFailures,
    sum(if msg = "validation_failed", 1, 0) as validationFailures,
    sum(if msg = "post_lambda_failed", 1, 0) as postFailures
  by county
| eval successRate = if((successExecutions + failedExecutions) = 0, 0, successExecutions * 100 / (successExecutions + failedExecutions))
| sort county asc
EOF

read -r -d '' FAILURE_QUERY <<'EOF'
fields county, msg
| filter component = "post" and level = "error"
| stats count() as failureCount by county, msg
| sort county asc, failureCount desc
EOF

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
          failure_counts: {
            transform_failed: toNum($row.transformFailures),
            validation_failed: toNum($row.validationFailures),
            post_lambda_failed: toNum($row.postFailures)
          }
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
        .[( $row.county // "unknown" )].failure_counts[( $row.msg // "unknown" )] = toNum($row.failureCount)
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
          "  Total successful transactions: \(.value.total_successful_transactions)\n" +
          "  Success rate: \(formatRate(.value.success_rate; .value.success_executions; .value.failed_executions))\n" +
          (if (.value.failure_counts | type) == "object" and (.value.failure_counts | length) > 0 then
            "  Failure counts:\n" +
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
  check_dependencies
  parse_args "$@"
  calculate_time_range
  build_aws_args

  local success_rows failure_rows aggregated
  success_rows=$(run_insights_query "$SUCCESS_QUERY" | jq -s '.')
  failure_rows=$(run_insights_query "$FAILURE_QUERY" | jq -s '.')

  aggregated=$(aggregate_results "$success_rows" "$failure_rows")
  format_output "$aggregated"
}

main "$@"

