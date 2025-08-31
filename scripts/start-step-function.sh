#!/usr/bin/env bash
set -euo pipefail

# Start Step Function execution for S3 to SQS processing

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }

# Config with defaults
STACK_NAME="${STACK_NAME:-elephant-mwaa}"

usage() {
    cat <<EOF
Usage: $0 <bucket-name>

Start Step Function execution to process S3 objects and send SQS messages.

Arguments:
    bucket-name    Name of the S3 bucket to process

Environment Variables:
    STACK_NAME     CloudFormation stack name (default: elephant-mwaa)

Example:
    $0 my-data-bucket
EOF
}

# Check prerequisites
check_prereqs() {
    command -v aws >/dev/null || { err "aws CLI not found"; exit 1; }
    command -v jq >/dev/null || { err "jq not found"; exit 1; }
    aws sts get-caller-identity >/dev/null || { err "AWS credentials not configured"; exit 1; }
}

# Get Step Function ARN from CloudFormation stack
get_step_function_arn() {
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --query "Stacks[0].Outputs[?OutputKey=='S3ToSqsStateMachineArn'].OutputValue" \
        --output text
}

# Get SQS Queue URL from CloudFormation stack
get_sqs_queue_url() {
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --query "Stacks[0].Outputs[?OutputKey=='MwaaSqsQueueUrl'].OutputValue" \
        --output text
}

# Start Step Function execution
start_execution() {
    local bucket_name="$1"
    local step_function_arn="$2"
    local sqs_queue_url="$3"
    local execution_name="$bucket_name"

    info "Starting Step Function execution: $execution_name"
    info "Step Function ARN: $step_function_arn"
    info "S3 Bucket: $bucket_name"
    info "SQS Queue URL: $sqs_queue_url"

    # Create execution input
    local input_payload
    input_payload=$(jq -n \
        --arg bucket "$bucket_name" \
        --arg sqs_url "$sqs_queue_url" \
        '{bucketName: $bucket, sqsQueueUrl: $sqs_url}')

    # Start execution
    local execution_arn
    if execution_arn=$(aws stepfunctions start-execution \
        --state-machine-arn "$step_function_arn" \
        --name "$execution_name" \
        --input "$input_payload" \
        --query 'executionArn' \
        --output text 2>/dev/null); then

        info "✅ Step Function execution started successfully"
        info "Execution ARN: $execution_arn"
        echo "$execution_arn"
        return 0
    else
        local exit_code=$?
        local error_output
        error_output=$(aws stepfunctions start-execution \
            --state-machine-arn "$step_function_arn" \
            --name "$execution_name" \
            --input "$input_payload" 2>&1) || true

        # Check if error is about execution already exists
        if echo "$error_output" | grep -q "ExecutionAlreadyExists"; then
            warn "⚠️  Execution '$execution_name' already exists"
            err "An execution with this name is already running or has completed recently."
            err "Please choose a different bucket name or wait for the existing execution to complete."
            return 1
        else
            err "❌ Failed to start Step Function execution"
            err "Error: $error_output"
            return $exit_code
        fi
    fi
}

main() {
    # Check if bucket name is provided
    if [[ $# -ne 1 ]]; then
        usage
        exit 1
    fi

    local bucket_name="$1"

    # Validate bucket name format
    if ! [[ "$bucket_name" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]]; then
        err "Invalid bucket name format: $bucket_name"
        err "Bucket names must be 3-63 characters long and contain only lowercase letters, numbers, hyphens, and periods."
        exit 1
    fi

    check_prereqs

    info "Using CloudFormation stack: $STACK_NAME"

    # Get resources from stack
    local step_function_arn
    local sqs_queue_url

    step_function_arn=$(get_step_function_arn)
    if [[ -z "$step_function_arn" || "$step_function_arn" == "None" ]]; then
        err "Could not find Step Function ARN in stack outputs"
        err "Make sure the stack is deployed and contains S3ToSqsStateMachineArn output"
        exit 1
    fi

    sqs_queue_url=$(get_sqs_queue_url)
    if [[ -z "$sqs_queue_url" || "$sqs_queue_url" == "None" ]]; then
        err "Could not find SQS Queue URL in stack outputs"
        err "Make sure the stack is deployed and contains MwaaSqsQueueUrl output"
        exit 1
    fi

    # Start execution
    start_execution "$bucket_name" "$step_function_arn" "$sqs_queue_url"
}

main "$@"
