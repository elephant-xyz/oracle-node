#!/usr/bin/env bash
set -euo pipefail

# Start Property Improvement Workflow - uses S3ToSqsStateMachine to process bucket

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }

# Config with defaults
STACK_NAME="${STACK_NAME:-elephant-oracle-node}"
AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_PROFILE="${AWS_PROFILE:-oracle-18}"

usage() {
    cat <<EOF
Usage: $0 <bucket-name>

Start Property Improvement Workflow by processing S3 bucket.

Arguments:
    bucket-name        Name of the S3 bucket containing CSV files

Options:
    --help             Show this help message

Environment Variables:
    STACK_NAME         CloudFormation stack name (default: oracle-node)
    AWS_REGION         AWS region (default: us-east-1)
    AWS_PROFILE        AWS profile to use (default: oracle-18)

Examples:
    # Process all CSV files in bucket
    $0 elephant-leet-permits-splits

    # With custom stack name
    STACK_NAME=my-stack $0 elephant-leet-permits-splits

    # With custom AWS profile
    AWS_PROFILE=my-profile $0 elephant-leet-permits-splits

How it works:
    1. Triggers S3ToSqsStateMachine with bucket name and Property Improvement queue
    2. S3ToSqsStateMachine lists all objects in bucket
    3. Sends S3 event message for each CSV file to Property Improvement queue
    4. Starter Lambda picks up messages and invokes PropertyImprovementWorkflow
    5. Each CSV file is processed independently
EOF
}

# Check prerequisites
check_prereqs() {
    command -v aws >/dev/null || { err "aws CLI not found"; exit 1; }
    command -v jq >/dev/null || { err "jq not found"; exit 1; }
    AWS_PROFILE="$AWS_PROFILE" aws sts get-caller-identity >/dev/null || { err "AWS credentials not configured for profile: $AWS_PROFILE"; exit 1; }
}

# Get S3ToSqsStateMachine ARN from CloudFormation stack
get_s3_to_sqs_state_machine_arn() {
    AWS_PROFILE="$AWS_PROFILE" aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$AWS_REGION" \
        --query "Stacks[0].Outputs[?OutputKey=='S3ToSqsStateMachineArn'].OutputValue" \
        --output text
}

# Get Property Improvement Queue URL from CloudFormation stack
get_property_improvement_queue_url() {
    AWS_PROFILE="$AWS_PROFILE" aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$AWS_REGION" \
        --query "Stacks[0].Outputs[?OutputKey=='PropertyImprovementQueueUrl'].OutputValue" \
        --output text
}

# Start S3ToSqsStateMachine execution
start_s3_to_sqs_execution() {
    local bucket_name="$1"
    local state_machine_arn="$2"
    local queue_url="$3"
    
    # Generate unique execution name with timestamp
    local timestamp=$(date +%Y%m%d-%H%M%S)
    local execution_name="property-improvement-${bucket_name}-${timestamp}"

    info "Starting S3ToSqsStateMachine execution: $execution_name"
    info "State Machine ARN: $state_machine_arn"
    info "S3 Bucket: $bucket_name"
    info "Target Queue: $queue_url"

    # Create execution input
    local input_payload
    input_payload=$(jq -n \
        --arg bucket "$bucket_name" \
        --arg queue_url "$queue_url" \
        '{bucketName: $bucket, sqsQueueUrl: $queue_url}')

    info "Input payload:"
    echo "$input_payload" | jq .

    # Start execution
    local execution_arn
    if execution_arn=$(AWS_PROFILE="$AWS_PROFILE" aws stepfunctions start-execution \
        --state-machine-arn "$state_machine_arn" \
        --name "$execution_name" \
        --input "$input_payload" \
        --region "$AWS_REGION" \
        --query 'executionArn' \
        --output text 2>/dev/null); then

        info "✅ S3ToSqsStateMachine execution started successfully"
        info "Execution ARN: $execution_arn"
        info ""
        info "The state machine will:"
        info "  1. List all objects in s3://${bucket_name}"
        info "  2. Send S3 event message for each object to the Property Improvement queue"
        info "  3. Starter Lambda will pick up messages and invoke PropertyImprovementWorkflow"
        info ""
        info "Monitor progress:"
        info "  - S3ToSqs: /aws/vendedlogs/states/S3PageProcessorStateMachine"
        info "  - Starter: /aws/lambda/${STACK_NAME}-PropertyImprovementStarterFunction-*"
        info "  - Workflow: /aws/vendedlogs/states/PropertyImprovementWorkflow"
        echo "$execution_arn"
        return 0
    else
        local exit_code=$?
        local error_output
        error_output=$(AWS_PROFILE="$AWS_PROFILE" aws stepfunctions start-execution \
            --state-machine-arn "$state_machine_arn" \
            --name "$execution_name" \
            --input "$input_payload" \
            --region "$AWS_REGION" 2>&1) || true

        # Check if error is about execution already exists
        if echo "$error_output" | grep -q "ExecutionAlreadyExists"; then
            warn "⚠️  Execution '$execution_name' already exists"
            err "An execution with this name is already running or has completed recently."
            err "Please try again in a moment."
            return 1
        else
            err "❌ Failed to start S3ToSqsStateMachine execution"
            err "Error: $error_output"
            return $exit_code
        fi
    fi
}

main() {
    local bucket_name=""

    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --help)
                usage
                exit 0
                ;;
            -*)
                err "Unknown option: $1"
                usage
                exit 1
                ;;
            *)
                if [[ -z "$bucket_name" ]]; then
                    bucket_name="$1"
                else
                    err "Too many arguments"
                    usage
                    exit 1
                fi
                shift
                ;;
        esac
    done

    # Check if required arguments are provided
    if [[ -z "$bucket_name" ]]; then
        err "Bucket name is required"
        usage
        exit 1
    fi

    # Validate bucket name format
    if ! [[ "$bucket_name" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]]; then
        err "Invalid bucket name format: $bucket_name"
        err "Bucket names must be 3-63 characters long and contain only lowercase letters, numbers, hyphens, and periods."
        exit 1
    fi

    check_prereqs

    info "Using CloudFormation stack: $STACK_NAME"
    info "Using AWS region: $AWS_REGION"
    info "Using AWS profile: $AWS_PROFILE"

    # Get S3ToSqsStateMachine ARN
    local state_machine_arn
    state_machine_arn=$(get_s3_to_sqs_state_machine_arn)
    if [[ -z "$state_machine_arn" || "$state_machine_arn" == "None" ]]; then
        err "Could not find S3ToSqsStateMachine ARN in stack outputs"
        err "Make sure the stack is deployed and contains S3ToSqsStateMachineArn output"
        exit 1
    fi

    # Get Property Improvement Queue URL
    local queue_url
    queue_url=$(get_property_improvement_queue_url)
    if [[ -z "$queue_url" || "$queue_url" == "None" ]]; then
        err "Could not find Property Improvement Queue URL in stack outputs"
        err "Make sure the stack is deployed and contains PropertyImprovementQueueUrl output"
        err ""
        err "Troubleshooting:"
        err "1. Check if the stack exists:"
        err "   AWS_PROFILE=$AWS_PROFILE aws cloudformation describe-stacks --stack-name $STACK_NAME --region $AWS_REGION"
        err ""
        err "2. List all stack outputs:"
        err "   AWS_PROFILE=$AWS_PROFILE aws cloudformation describe-stacks --stack-name $STACK_NAME --region $AWS_REGION --query 'Stacks[0].Outputs'"
        exit 1
    fi

    start_s3_to_sqs_execution "$bucket_name" "$state_machine_arn" "$queue_url"
}

main "$@"
