#!/usr/bin/env bash
set -euo pipefail

# Sync local DAGs to the MWAA DAGs S3 prefix.

RED='\033[0;31m'; GREEN='\033[0;32m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; }

STACK_NAME="${STACK_NAME:-oracle-node}"
DAGS_DIR="${DAGS_DIR:-dags}"

command -v aws >/dev/null || { err "aws CLI not found"; exit 1; }
aws sts get-caller-identity >/dev/null || { err "AWS credentials not configured"; exit 1; }

info "Resolving MWAA bucket from stack: $STACK_NAME"
S3_BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='EnvironmentBucketName'].OutputValue" --output text)
[[ -n "$S3_BUCKET" && "$S3_BUCKET" != "None" ]] || { err "Could not determine EnvironmentBucketName"; exit 1; }
info "Using bucket: $S3_BUCKET"

[[ -d "$DAGS_DIR" ]] || { err "DAGs directory not found: $DAGS_DIR"; exit 1; }
aws s3 sync "$DAGS_DIR/" "s3://${S3_BUCKET}/dags/" --delete
info "DAGs synced to s3://${S3_BUCKET}/dags/"

