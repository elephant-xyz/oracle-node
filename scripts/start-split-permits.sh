#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/start-split-permits.sh --stack <StackName> --region <aws-region> --bucket <bucket> --prefix <s3-prefix>
# Example: ./scripts/start-split-permits.sh --stack elephant-oracle-node --region us-east-1 --bucket elephang-input-csv --prefix split-rows/

STACK=""
REGION="${AWS_REGION:-us-east-1}"
BUCKET=""
PREFIX=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --stack)
      STACK="$2"; shift 2;;
    --region)
      REGION="$2"; shift 2;;
    --bucket)
      BUCKET="$2"; shift 2;;
    --prefix)
      PREFIX="$2"; shift 2;;
    *)
      echo "Unknown arg: $1" >&2; exit 1;;
  esac
done

if [[ -z "$STACK" ]]; then
  echo "--stack not provided; attempting auto-discovery..." >&2
  # Find a stack that exposes both S3ToSqsStateMachineArn and SplitPermitsQueueUrl outputs
  STACK=$(aws cloudformation describe-stacks --region "$REGION" --output json \
    | jq -r '.Stacks[] | select(.Outputs and ([.Outputs[].OutputKey] | index("S3ToSqsStateMachineArn")) and ([.Outputs[].OutputKey] | index("SplitPermitsQueueUrl"))) | .StackName' \
    | head -n 1)
  if [[ -z "$STACK" || "$STACK" == "null" ]]; then
    echo "Failed to auto-discover stack. Please pass --stack <StackName>." >&2
    exit 1
  fi
  echo "Discovered stack: $STACK" >&2
fi
if [[ -z "$BUCKET" ]]; then echo "--bucket is required" >&2; exit 1; fi
if [[ -z "$PREFIX" ]]; then
  echo "--prefix not provided; defaulting to entire bucket (no prefix filter)" >&2
  PREFIX=""
fi

echo "Region: $REGION" >&2
echo "Stack:  $STACK" >&2
echo "Bucket: $BUCKET" >&2
echo "Prefix: $PREFIX" >&2

# Discover resources from stack outputs
S3_TO_SQS_ARN=$(aws cloudformation describe-stacks --stack-name "$STACK" --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='S3ToSqsStateMachineArn'].OutputValue" --output text)
SPLIT_PERMITS_QUEUE_URL=$(aws cloudformation describe-stacks --stack-name "$STACK" --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='SplitPermitsQueueUrl'].OutputValue" --output text)

if [[ -z "$S3_TO_SQS_ARN" || "$S3_TO_SQS_ARN" == "None" ]]; then
  echo "Failed to resolve S3ToSqsStateMachineArn from stack outputs" >&2
  exit 1
fi
if [[ -z "$SPLIT_PERMITS_QUEUE_URL" || "$SPLIT_PERMITS_QUEUE_URL" == "None" ]]; then
  echo "Failed to resolve SplitPermitsQueueUrl from stack outputs" >&2
  exit 1
fi

echo "Loader SFN: $S3_TO_SQS_ARN"
echo "Queue URL:  $SPLIT_PERMITS_QUEUE_URL"

INPUT=$(jq -n --arg b "$BUCKET" --arg p "$PREFIX" --arg q "$SPLIT_PERMITS_QUEUE_URL" '{bucketName:$b, prefix:$p, sqsQueueUrl:$q}')

aws stepfunctions start-execution \
  --region "$REGION" \
  --state-machine-arn "$S3_TO_SQS_ARN" \
  --input "$INPUT" \
  --query executionArn --output text

echo "Started S3->SQS load for s3://$BUCKET/$PREFIX into $SPLIT_PERMITS_QUEUE_URL"


