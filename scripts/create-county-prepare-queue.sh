#!/usr/bin/env bash
set -euo pipefail

# Create county-specific prepare queue with Lambda trigger (no DLQ - flow uses waitForTaskToken pattern)
# Usage: ./create-county-prepare-queue.sh <county_name>
# Example: ./create-county-prepare-queue.sh Okeechobee

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }
step() { echo -e "${BLUE}[STEP]${NC} $*"; }

# Validate input
if [ $# -eq 0 ]; then
    err "County name is required"
    echo "Usage: $0 <county_name>"
    echo "Example: $0 Okeechobee"
    exit 1
fi

COUNTY_NAME_INPUT="$1"
# Convert to lowercase and replace spaces with hyphens for valid SQS queue name
COUNTY_NAME=$(echo "$COUNTY_NAME_INPUT" | tr '[:upper:]' '[:lower:]' | tr ' ' '-')
STACK_NAME="${STACK_NAME:-elephant-oracle-node}"
REGION="${AWS_REGION:-us-east-1}"

# Queue configuration (matching prepare queue settings)
VISIBILITY_TIMEOUT=900
MESSAGE_RETENTION_PERIOD=1209600
BATCH_SIZE=1
BATCHING_WINDOW=0
MAX_CONCURRENCY="${MAX_CONCURRENCY:-10}"

# Queue name (always lowercase)
QUEUE_NAME="${STACK_NAME}-prepare-queue-${COUNTY_NAME}"

info "Creating county-specific prepare queue for: $COUNTY_NAME_INPUT (queue name: $COUNTY_NAME)"
info "Stack name: $STACK_NAME"
info "Region: $REGION"

# Step 1: Create prepare queue (no DLQ - flow uses waitForTaskToken pattern)
step "Creating prepare queue: $QUEUE_NAME"

QUEUE_URL=$(aws sqs create-queue \
    --queue-name "$QUEUE_NAME" \
    --attributes "{
        \"VisibilityTimeout\": \"$VISIBILITY_TIMEOUT\",
        \"MessageRetentionPeriod\": \"$MESSAGE_RETENTION_PERIOD\",
        \"SqsManagedSseEnabled\": \"true\"
    }" \
    --region "$REGION" \
    --query 'QueueUrl' \
    --output text)

if [ -z "$QUEUE_URL" ]; then
    err "Failed to create prepare queue"
    exit 1
fi

info "Queue created: $QUEUE_URL"

# Get Queue ARN
QUEUE_ARN=$(aws sqs get-queue-attributes \
    --queue-url "$QUEUE_URL" \
    --attribute-names QueueArn \
    --region "$REGION" \
    --query 'Attributes.QueueArn' \
    --output text)

info "Queue ARN: $QUEUE_ARN"

# Step 2: Get DownloaderFunction ARN from CloudFormation stack
step "Getting DownloaderFunction ARN from stack: $STACK_NAME"

FUNCTION_ARN=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='DownloaderFunctionArn'].OutputValue" \
    --output text 2>/dev/null || true)

if [ -z "$FUNCTION_ARN" ]; then
    warn "DownloaderFunctionArn not found in stack outputs, trying to get function directly"

    # Try to get function name from stack resources
    FUNCTION_NAME=$(aws cloudformation describe-stack-resources \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query "StackResources[?LogicalResourceId=='DownloaderFunction'].PhysicalResourceId" \
        --output text)

    if [ -z "$FUNCTION_NAME" ]; then
        err "Failed to find DownloaderFunction in stack"
        exit 1
    fi

    FUNCTION_ARN=$(aws lambda get-function \
        --function-name "$FUNCTION_NAME" \
        --region "$REGION" \
        --query 'Configuration.FunctionArn' \
        --output text)
fi

if [ -z "$FUNCTION_ARN" ]; then
    err "Failed to get DownloaderFunction ARN"
    exit 1
fi

info "Lambda Function ARN: $FUNCTION_ARN"

# Step 3: Create event source mapping
step "Creating event source mapping between queue and Lambda function"

EVENT_SOURCE_UUID=$(aws lambda create-event-source-mapping \
    --function-name "$FUNCTION_ARN" \
    --event-source-arn "$QUEUE_ARN" \
    --batch-size "$BATCH_SIZE" \
    --maximum-batching-window-in-seconds "$BATCHING_WINDOW" \
    --scaling-config "MaximumConcurrency=$MAX_CONCURRENCY" \
    --region "$REGION" \
    --query 'UUID' \
    --output text)

if [ -z "$EVENT_SOURCE_UUID" ]; then
    err "Failed to create event source mapping"
    exit 1
fi

info "Event source mapping created: $EVENT_SOURCE_UUID"

# Summary
echo ""
step "✓ County prepare queue setup completed successfully!"
echo ""
info "Summary:"
echo "  County Name:              $COUNTY_NAME"
echo "  Queue Name:               $QUEUE_NAME"
echo "  Queue URL:                $QUEUE_URL"
echo "  Queue ARN:                $QUEUE_ARN"
echo "  Lambda Function ARN:      $FUNCTION_ARN"
echo "  Event Source Mapping:     $EVENT_SOURCE_UUID"
echo "  Max Concurrency:          $MAX_CONCURRENCY"
echo ""
info "State machine will send prepare messages to: $QUEUE_URL"
echo ""
