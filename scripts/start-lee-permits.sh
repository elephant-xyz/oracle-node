#!/usr/bin/env bash
set -euo pipefail

# Start lee-permits by discovering the environment bucket and queue from a CFN stack
# and enqueueing S3 output objects to the queue (like start-step-function.sh style).

REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
STACK_NAME=""
QUEUE_NAME="elephant-lee-permits-queue"
CREATE_QUEUE="false"
DRY_RUN="false"
START_WORKFLOW="false"
S3_KEY_FOR_WORKFLOW=""

usage() {
  cat <<EOF
Usage: $0 [--stack <StackName>] [--region <region>] [--queue-name <name>] [--create-queue true|false] [--dry-run true|false] [--start-workflow true|false] [--s3-key <outputs/.../output.zip>]

Examples:
  $0 --stack <YourStackName> --region us-east-1
  $0 --region us-east-1 --create-queue true --queue-name elephant-lee-permits-queue
  # also start a single ElephantExpressLeePermits execution with a specific S3 key (optional)
  $0 --stack <YourStackName> --region us-east-1 --start-workflow true --s3-key outputs/123/output.zip
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --stack|--stack-name)
      STACK_NAME="$2"; shift 2;;
    --region)
      REGION="$2"; shift 2;;
    --queue-name)
      QUEUE_NAME="$2"; shift 2;;
    --create-queue)
      CREATE_QUEUE="$2"; shift 2;;
    --dry-run)
      DRY_RUN="$2"; shift 2;;
    --start-workflow)
      START_WORKFLOW="$2"; shift 2;;
    --s3-key)
      S3_KEY_FOR_WORKFLOW="$2"; shift 2;;
    -h|--help)
      usage; exit 0;;
    *)
      echo "Unknown arg: $1" >&2; usage; exit 1;;
  esac
done

if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI not found. Please install and configure AWS CLI." >&2
  exit 1
fi

discover_stack() {
  # Try to find a stack that has both EnvironmentBucketName and LeePermitsWorkflowQueueUrl
  local names
  names=$(aws cloudformation list-stacks \
    --region "$REGION" \
    --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE UPDATE_ROLLBACK_COMPLETE \
    --query 'reverse(sort_by(StackSummaries,&(LastUpdatedTime||CreationTime)))[].StackName' \
    --output text || true)
  for n in $names; do
    local has_env has_queue
    has_env=$(aws cloudformation describe-stacks --region "$REGION" --stack-name "$n" \
      --query "Stacks[0].Outputs[?OutputKey=='EnvironmentBucketName'] | length(@)" --output text || echo 0)
    has_queue=$(aws cloudformation describe-stacks --region "$REGION" --stack-name "$n" \
      --query "Stacks[0].Outputs[?OutputKey=='LeePermitsWorkflowQueueUrl'] | length(@)" --output text || echo 0)
    if [[ "$has_env" == "1" && "$has_queue" == "1" ]]; then
      echo "$n"
      return 0
    fi
  done
  echo "" # not found
}

if [[ -z "$STACK_NAME" ]]; then
  STACK_NAME=$(discover_stack)
fi

if [[ -z "$STACK_NAME" ]]; then
  echo "Failed to auto-discover stack. Provide --stack <StackName>." >&2
  exit 1
fi

ENV_BUCKET=$(aws cloudformation describe-stacks --region "$REGION" --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='EnvironmentBucketName'].OutputValue" --output text)

QUEUE_URL=$(aws cloudformation describe-stacks --region "$REGION" --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='LeePermitsWorkflowQueueUrl'].OutputValue" --output text || true)

LOADER_ARN=$(aws cloudformation describe-stacks --region "$REGION" --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='LeePermitsLoadOutputsStateMachineArn'].OutputValue" --output text || true)
WF_ARN=$(aws cloudformation describe-stacks --region "$REGION" --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='ElephantExpressLeePermitsStateMachineArn'].OutputValue" --output text || true)

if [[ -z "$QUEUE_URL" || "$QUEUE_URL" == "None" ]]; then
  if [[ "$CREATE_QUEUE" == "true" ]]; then
    # Try to create or get the queue
    set +e
    QUEUE_URL=$(aws sqs get-queue-url --region "$REGION" --queue-name "$QUEUE_NAME" --query 'QueueUrl' --output text 2>/dev/null)
    set -e
    if [[ -z "$QUEUE_URL" || "$QUEUE_URL" == "None" ]]; then
      QUEUE_URL=$(aws sqs create-queue --region "$REGION" --queue-name "$QUEUE_NAME" \
        --attributes VisibilityTimeout=331,MessageRetentionPeriod=1209600,SqsManagedSseEnabled=true \
        --query 'QueueUrl' --output text)
    fi
  else
    # Fallback: try get-queue-url by name
    set +e
    QUEUE_URL=$(aws sqs get-queue-url --region "$REGION" --queue-name "$QUEUE_NAME" --query 'QueueUrl' --output text 2>/dev/null)
    set -e
  fi
fi

if [[ -z "$ENV_BUCKET" || "$ENV_BUCKET" == "None" ]]; then
  echo "Could not resolve EnvironmentBucketName from stack $STACK_NAME" >&2
  exit 1
fi

if [[ -z "$QUEUE_URL" || "$QUEUE_URL" == "None" ]]; then
  echo "Could not resolve or create queue URL (stack: $STACK_NAME, name: $QUEUE_NAME)" >&2
  exit 1
fi

echo "Using stack: $STACK_NAME"
echo "Environment bucket: $ENV_BUCKET"
echo "Queue URL: $QUEUE_URL"

if [[ -n "$LOADER_ARN" && "$LOADER_ARN" != "None" ]]; then
  echo "Starting LeePermitsLoadOutputs state machine: $LOADER_ARN"
  aws stepfunctions start-execution --region "$REGION" --state-machine-arn "$LOADER_ARN" --input '{}' >/dev/null
  echo "LeePermitsLoadOutputs started. It will list S3 and enqueue to the lee-permits queue."
else
  echo "Loader SFN output not found; falling back to direct S3->SQS enqueuing (slower)."
  echo "Listing output.zip keys under s3://$ENV_BUCKET/outputs/ ..."
  KEYS=$(aws s3api list-objects-v2 --region "$REGION" --bucket "$ENV_BUCKET" --prefix "outputs/" \
    --query "Contents[?contains(Key, 'output.zip')].Key" --output text)
  if [[ -z "$KEYS" ]]; then
    echo "No output.zip keys found under s3://$ENV_BUCKET/outputs/." >&2
  else
    COUNT=0
    while IFS= read -r KEY; do
      [[ -z "$KEY" ]] && continue
      BODY=$(printf '{"s3":{"bucket":{"name":"%s"},"object":{"key":"%s"}}}' "$ENV_BUCKET" "$KEY")
      if [[ "$DRY_RUN" == "true" ]]; then
        echo "DRY-RUN: would enqueue $KEY"
      else
        aws sqs send-message --region "$REGION" --queue-url "$QUEUE_URL" --message-body "$BODY" >/dev/null
      fi
      COUNT=$((COUNT+1))
    done <<< "$KEYS"
    echo "Enqueued $COUNT messages to $QUEUE_URL"
  fi
fi

if [[ "$START_WORKFLOW" == "true" ]]; then
  if [[ -z "$WF_ARN" || "$WF_ARN" == "None" ]]; then
    echo "Cannot start ElephantExpressLeePermits: ARN not found in stack outputs." >&2
  elif [[ -z "$S3_KEY_FOR_WORKFLOW" ]]; then
    echo "--start-workflow true provided but no --s3-key specified. Skipping manual start. The starter will auto-start per SQS message." >&2
  else
    echo "Starting ElephantExpressLeePermits with S3 key: $S3_KEY_FOR_WORKFLOW"
    INPUT=$(printf '{"message":{"s3":{"bucket":{"name":"%s"},"object":{"key":"%s"}}}}' "$ENV_BUCKET" "$S3_KEY_FOR_WORKFLOW")
    aws stepfunctions start-execution --region "$REGION" --state-machine-arn "$WF_ARN" --input "$INPUT" >/dev/null
    echo "ElephantExpressLeePermits started manually."
  fi
else
  echo "ElephantExpressLeePermits will start automatically via the SQS-triggered starter as messages arrive."
fi


