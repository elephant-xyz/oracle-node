#!/usr/bin/env bash
set -euo pipefail

# Create county-specific workflow queue with DLQ and Lambda trigger
# Usage: ./create-county-queue.sh <county_name>
# Example: ./create-county-queue.sh escambia

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }
step() { echo -e "${BLUE}[STEP]${NC} $*"; }

# Validate input
if [ $# -eq 0 ]; then
    err "County name is required"
    echo "Usage: $0 <county_name>"
    echo "Example: $0 escambia"
    exit 1
fi

COUNTY_NAME="$1"
STACK_NAME="${STACK_NAME:-elephant-oracle-node}"
REGION="${AWS_REGION:-us-east-1}"

# Queue configuration (matching template.yaml)
VISIBILITY_TIMEOUT=331
MESSAGE_RETENTION_PERIOD=1209600
MAX_RECEIVE_COUNT=3
BATCH_SIZE=1
BATCHING_WINDOW=0
MAX_CONCURRENCY="${MAX_CONCURRENCY:-100}"

# Queue names
DLQ_NAME="elephant-workflow-queue-${COUNTY_NAME}-dlq"
QUEUE_NAME="elephant-workflow-queue-${COUNTY_NAME}"

info "Creating county-specific workflow queue for: $COUNTY_NAME"
info "Stack name: $STACK_NAME"
info "Region: $REGION"

# Step 1: Create Dead Letter Queue
step "Creating Dead Letter Queue: $DLQ_NAME"
DLQ_URL=$(aws sqs create-queue \
    --queue-name "$DLQ_NAME" \
    --attributes "{
        \"MessageRetentionPeriod\": \"$MESSAGE_RETENTION_PERIOD\",
        \"VisibilityTimeout\": \"$VISIBILITY_TIMEOUT\",
        \"SqsManagedSseEnabled\": \"true\"
    }" \
    --region "$REGION" \
    --query 'QueueUrl' \
    --output text)

if [ -z "$DLQ_URL" ]; then
    err "Failed to create Dead Letter Queue"
    exit 1
fi

info "DLQ created: $DLQ_URL"

# Get DLQ ARN
DLQ_ARN=$(aws sqs get-queue-attributes \
    --queue-url "$DLQ_URL" \
    --attribute-names QueueArn \
    --region "$REGION" \
    --query 'Attributes.QueueArn' \
    --output text)

info "DLQ ARN: $DLQ_ARN"

# Step 2: Create main workflow queue with redrive policy
step "Creating workflow queue: $QUEUE_NAME"

# Build redrive policy JSON
REDRIVE_POLICY="{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":${MAX_RECEIVE_COUNT}}"

QUEUE_URL=$(aws sqs create-queue \
    --queue-name "$QUEUE_NAME" \
    --attributes "{
        \"VisibilityTimeout\": \"$VISIBILITY_TIMEOUT\",
        \"MessageRetentionPeriod\": \"$MESSAGE_RETENTION_PERIOD\",
        \"SqsManagedSseEnabled\": \"true\",
        \"RedrivePolicy\": $(echo "$REDRIVE_POLICY" | jq -R .)
    }" \
    --region "$REGION" \
    --query 'QueueUrl' \
    --output text)

if [ -z "$QUEUE_URL" ]; then
    err "Failed to create workflow queue"
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

# Step 3: Get WorkflowStarterFunction ARN from CloudFormation stack
step "Getting WorkflowStarterFunction ARN from stack: $STACK_NAME"

FUNCTION_ARN=$(aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='WorkflowStarterFunctionArn'].OutputValue" \
    --output text 2>/dev/null || true)

if [ -z "$FUNCTION_ARN" ]; then
    warn "WorkflowStarterFunctionArn not found in stack outputs, trying to get function directly"

    # Try to get function name from stack resources
    FUNCTION_NAME=$(aws cloudformation describe-stack-resources \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query "StackResources[?LogicalResourceId=='WorkflowStarterFunction'].PhysicalResourceId" \
        --output text)

    if [ -z "$FUNCTION_NAME" ]; then
        err "Failed to find WorkflowStarterFunction in stack"
        exit 1
    fi

    FUNCTION_ARN=$(aws lambda get-function \
        --function-name "$FUNCTION_NAME" \
        --region "$REGION" \
        --query 'Configuration.FunctionArn' \
        --output text)
fi

if [ -z "$FUNCTION_ARN" ]; then
    err "Failed to get WorkflowStarterFunction ARN"
    exit 1
fi

info "Lambda Function ARN: $FUNCTION_ARN"

# Step 4: Create event source mapping
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
step "✓ County queue setup completed successfully!"
echo ""
info "Summary:"
echo "  County Name:              $COUNTY_NAME"
echo "  Queue Name:               $QUEUE_NAME"
echo "  Queue URL:                $QUEUE_URL"
echo "  Queue ARN:                $QUEUE_ARN"
echo "  DLQ Name:                 $DLQ_NAME"
echo "  DLQ URL:                  $DLQ_URL"
echo "  DLQ ARN:                  $DLQ_ARN"
echo "  Lambda Function ARN:      $FUNCTION_ARN"
echo "  Event Source Mapping:     $EVENT_SOURCE_UUID"
echo "  Max Concurrency:          $MAX_CONCURRENCY"
echo ""
info "You can now send messages to: $QUEUE_URL"
echo ""
