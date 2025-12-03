#!/bin/bash
# Script to manually mark a failed Step Function execution as resolved
# Usage: ./scripts/manual-resolve-execution.sh <execution-arn> [retry] [reason]

set -e

EXECUTION_ARN="${1}"
RETRY="${2:-false}"
REASON="${3:-Manually resolved}"

if [ -z "$EXECUTION_ARN" ]; then
  echo "Usage: $0 <execution-arn> [retry] [reason]"
  echo ""
  echo "Examples:"
  echo "  $0 arn:aws:states:us-east-1:123456789012:execution:MyStateMachine:my-execution"
  echo "  $0 arn:aws:states:us-east-1:123456789012:execution:MyStateMachine:my-execution true"
  echo "  $0 arn:aws:states:us-east-1:123456789012:execution:MyStateMachine:my-execution true 'Fixed data issue'"
  exit 1
fi

# Get the stack name from environment or prompt
STACK_NAME="${STACK_NAME:-elephant-oracle-node}"

# Get the Lambda function name
FUNCTION_NAME=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='ManualResolutionFunctionName'].OutputValue" \
  --output text 2>/dev/null || echo "")

if [ -z "$FUNCTION_NAME" ]; then
  echo "Error: Could not find ManualResolutionFunction in stack $STACK_NAME"
  echo "Make sure the stack is deployed and contains the ManualResolutionFunction"
  exit 1
fi

echo "Marking execution as resolved:"
echo "  Execution ARN: $EXECUTION_ARN"
echo "  Retry: $RETRY"
echo "  Reason: $REASON"
echo ""

# Invoke the Lambda function
PAYLOAD=$(cat <<EOF
{
  "executionArn": "$EXECUTION_ARN",
  "retry": $RETRY,
  "reason": "$REASON"
}
EOF
)

RESULT=$(aws lambda invoke \
  --function-name "$FUNCTION_NAME" \
  --payload "$PAYLOAD" \
  --cli-binary-format raw-in-base64-out \
  /tmp/manual-resolution-response.json)

if [ $? -eq 0 ]; then
  echo "Success! Response:"
  cat /tmp/manual-resolution-response.json | jq '.'
  rm /tmp/manual-resolution-response.json
else
  echo "Error: Failed to invoke manual resolution function"
  exit 1
fi

