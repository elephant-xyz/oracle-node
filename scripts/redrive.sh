#!/usr/bin/env bash
set -euo pipefail

# Script to purge the workflow queue and redrive messages from DLQ

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }

# Config with sane defaults
STACK_NAME="${STACK_NAME:-elephant-oracle-node}"

check_prereqs() {
  info "Checking prerequisites..."
  command -v aws >/dev/null || { err "aws CLI not found"; exit 1; }
  aws sts get-caller-identity >/dev/null || { err "AWS credentials not configured"; exit 1; }
  command -v jq >/dev/null || { err "jq not found"; exit 1; }
}

get_output() {
  local key=$1
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='${key}'].OutputValue" \
    --output text
}

purge_queue() {
  local queue_url=$1 queue_name=$2
  info "Purging $queue_name queue..."
  aws sqs purge-queue --queue-url "$queue_url"
  info "Waiting 60 seconds for purge to complete..."
  sleep 60
}

start_message_move_task() {
  local source_arn=$1 dest_arn=$2
  info "Starting message move task from DLQ to workflow queue..."
  local task_handle
  task_handle=$(aws sqs start-message-move-task \
    --source-arn "$source_arn" \
    --destination-arn "$dest_arn" \
    --query 'TaskHandle' \
    --output text)
  echo "$task_handle"
}

monitor_message_move_task() {
  local source_arn=$1 task_handle=$2
  info "Monitoring message move task: $task_handle"

  while true; do
    local task_details
    # Fetch details for our specific task using the task handle
    task_details=$(aws sqs list-message-move-tasks \
      --source-arn "$source_arn" \
      --query "Results[?TaskHandle=='$task_handle'] | [0]" \
      --output json 2>/dev/null)

    if [[ -z "$task_details" || "$task_details" == "null" ]]; then
      warn "Task not found or API error. Retrying..."
      sleep 10
      continue
    fi

    local status
    status=$(echo "$task_details" | jq -r '.Status // "UNKNOWN"')

    case "$status" in
      "COMPLETED")
        info "Message move task completed successfully"
        return 0
        ;;
      "FAILED"|"CANCELLED")
        err "Message move task failed with status: $status"
        local failure_reason
        failure_reason=$(echo "$task_details" | jq -r '.FailureReason // "Unknown reason"')
        err "Failure reason: $failure_reason"
        return 1
        ;;
      "RUNNING")
        local moved_count total_count
        moved_count=$(echo "$task_details" | jq -r '.ApproximateNumberOfMessagesMoved // "0"')
        total_count=$(echo "$task_details" | jq -r '.ApproximateNumberOfMessagesToMove // "0"')
        info "Task running: $moved_count/$total_count messages moved"

        # Check if all messages have been moved (task might still show RUNNING)
        if [[ "$moved_count" == "$total_count" && "$total_count" != "0" ]]; then
          info "All messages moved successfully ($moved_count/$total_count). Task completed."
          return 0
        fi

        sleep 10
        ;;
      *)
        warn "Unknown task status: $status. Waiting..."
        sleep 10
        ;;
    esac
  done
}

main() {
  check_prereqs

  # Get queue URLs from CloudFormation outputs
  local workflow_queue_url dlq_url
  workflow_queue_url=$(get_output "WorkflowQueueUrl")
  dlq_url=$(get_output "WorkflowDeadLetterQueueUrl")

  if [[ -z "$workflow_queue_url" || "$workflow_queue_url" == "None" ]]; then
    err "Could not find WorkflowQueueUrl in stack outputs"
    exit 1
  fi

  if [[ -z "$dlq_url" || "$dlq_url" == "None" ]]; then
    err "Could not find WorkflowDeadLetterQueueUrl in stack outputs"
    exit 1
  fi

  info "Workflow queue URL: $workflow_queue_url"
  info "DLQ URL: $dlq_url"

  # Get ARNs for message move task
  local workflow_queue_arn dlq_arn
  workflow_queue_arn=$(aws sqs get-queue-attributes \
    --queue-url "$workflow_queue_url" \
    --attribute-names QueueArn \
    --query 'Attributes.QueueArn' \
    --output text)

  dlq_arn=$(aws sqs get-queue-attributes \
    --queue-url "$dlq_url" \
    --attribute-names QueueArn \
    --query 'Attributes.QueueArn' \
    --output text)

  info "Workflow queue ARN: $workflow_queue_arn"
  info "DLQ ARN: $dlq_arn"

  # Purge the workflow queue
  purge_queue "$workflow_queue_url" "workflow"

  # Check if DLQ has messages before attempting redrive
  local dlq_message_count
  dlq_message_count=$(aws sqs get-queue-attributes \
    --queue-url "$dlq_url" \
    --attribute-names ApproximateNumberOfMessages \
    --query 'Attributes.ApproximateNumberOfMessages' \
    --output text)

  if [[ "$dlq_message_count" -eq 0 ]]; then
    warn "DLQ is empty ($dlq_message_count messages). Nothing to redrive."
    exit 0
  fi

  info "DLQ contains $dlq_message_count messages to redrive"

  # Start message move task
  local task_handle
  task_handle=$(start_message_move_task "$dlq_arn" "$workflow_queue_arn")

  if [[ -z "$task_handle" || "$task_handle" == "None" ]]; then
    err "Failed to start message move task"
    exit 1
  fi

  # Monitor the task until completion
  monitor_message_move_task "$dlq_arn" "$task_handle"

  info "Redrive operation completed successfully"
}

main "$@"
