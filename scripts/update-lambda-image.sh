#!/usr/bin/env bash
set -euo pipefail

# Reusable script to build Docker image, push to ECR, and update Lambda function
# This script bypasses SAM deploy and directly manages Docker image and Lambda

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $*" >&2; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*" >&2; }
err()  { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# Config with defaults that can be overridden
STACK_NAME="${STACK_NAME:-elephant-oracle-node}"
LAMBDA_DIR="${LAMBDA_DIR:-workflow/lambdas/mvl}"
IMAGE_NAME="${IMAGE_NAME:-mvl-lambda}"
# Default to amd64 builds to match Lambda's architecture unless overridden.
DOCKER_PLATFORM="${DOCKER_PLATFORM:-linux/amd64}"

# Get stack output helper
get_output() {
  local key=$1
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='${key}'].OutputValue" \
    --output text 2>/dev/null || echo ""
}

# Build Docker image locally
build_docker_image() {
  info "Building Docker image for Lambda function (platform: ${DOCKER_PLATFORM})"
  info "Note: Docker image build may take 30-60 minutes on first run (downloads ML models)"

  if [[ ! -f "$LAMBDA_DIR/Dockerfile" ]]; then
    err "Dockerfile not found at $LAMBDA_DIR/Dockerfile"
    exit 1
  fi

  # Authenticate with ECR Public to avoid rate limiting
  info "Authenticating with AWS ECR Public..."
  local aws_region
  aws_region=$(aws configure get region || echo "us-east-1")

  aws ecr-public get-login-password --region us-east-1 \
    | docker login --username AWS --password-stdin public.ecr.aws >/dev/null 2>&1 || {
    warn "Failed to authenticate with ECR Public, proceeding without authentication (may hit rate limits)"
  }

  # Build the Docker image
  docker build --platform "$DOCKER_PLATFORM" -t "$IMAGE_NAME:latest" "$LAMBDA_DIR" || {
    err "Failed to build Docker image"
    exit 1
  }

  info "Docker image built successfully: $IMAGE_NAME:latest"
}

# Push Docker image to ECR
push_image_to_ecr() {
  info "Pushing Docker image to ECR"

  # Get AWS account ID and region
  local aws_account_id
  aws_account_id=$(aws sts get-caller-identity --query Account --output text)
  local aws_region
  aws_region=$(aws configure get region || echo "us-east-1")

  # Get Lambda function name from stack outputs
  local function_name
  function_name=$(get_output "WorkflowMirrorValidatorFunctionName")

  if [[ -z "$function_name" || "$function_name" == "None" ]]; then
    err "Lambda function name not found in stack outputs. Deploy the stack first."
    err "Stack name: $STACK_NAME"
    err "Looking for output key: WorkflowMirrorValidatorFunctionName"
    exit 1
  fi

  # Get the current Lambda image URI to extract the ECR repository
  local current_image_uri
  current_image_uri=$(aws lambda get-function \
    --function-name "$function_name" \
    --query 'Code.ImageUri' \
    --output text 2>&1)

  if [[ -z "$current_image_uri" || "$current_image_uri" == "None" ]]; then
    err "Could not get current Lambda image URI"
    exit 1
  fi

  # Extract repository URI (everything before @ or :)
  local repo_uri
  repo_uri=$(echo "$current_image_uri" | sed -E 's/(@sha256:[a-f0-9]+|:[^:]+)$//')

  info "Target ECR repository: $repo_uri"

  # Login to ECR
  info "Logging in to ECR..."
  aws ecr get-login-password --region "$aws_region" \
    | docker login --username AWS --password-stdin "${aws_account_id}.dkr.ecr.${aws_region}.amazonaws.com" >/dev/null || {
    err "Failed to login to ECR"
    exit 1
  }

  # Tag the image for ECR
  info "Tagging image for ECR..."
  docker tag "$IMAGE_NAME:latest" "$repo_uri:latest" || {
    err "Failed to tag Docker image"
    exit 1
  }

  # Set Docker client timeout to 20 minutes for large images
  export DOCKER_CLIENT_TIMEOUT=1200
  export COMPOSE_HTTP_TIMEOUT=1200

  # Push to ECR
  info "Pushing image to ECR (this may take 10-20 minutes for large images)..."
  docker push "$repo_uri:latest" >&2 || {
    err "Failed to push Docker image to ECR"
    exit 1
  }

  info "Successfully pushed Docker image to $repo_uri:latest"

  # Return the repo URI for use in update function
  echo "$repo_uri"
}

# Update Lambda function to use the latest image from ECR
update_lambda_with_latest_image() {
  local repo_uri="${1:-}"
  local output_key="${2:-WorkflowMirrorValidatorFunctionName}"

  info "Updating Lambda function to use latest Docker image"

  # Get Lambda function name from stack outputs
  local function_name
  function_name=$(get_output "$output_key")

  if [[ -z "$function_name" ]]; then
    err "Lambda function name not found in stack outputs (looking for: $output_key)"
    exit 1
  fi

  info "Lambda function: $function_name"

  # If repo_uri not provided, get it from current Lambda configuration
  if [[ -z "$repo_uri" ]]; then
    local image_uri
    image_uri=$(aws lambda get-function \
      --function-name "$function_name" \
      --query 'Code.ImageUri' \
      --output text 2>&1)
    # Extract repository URI by removing digest (@sha256:...) or tag (:tag)
    repo_uri=$(echo "$image_uri" | sed -E 's/(@sha256:[a-f0-9]+|:[^/@]+)$//')
  fi

  info "ECR repository: $repo_uri"

  # Extract repository name from URI
  local repo_name
  repo_name=$(echo "$repo_uri" | awk -F'/' '{print $2 "/" $3}')

  # Get the latest image digest from ECR
  local latest_digest
  latest_digest=$(aws ecr describe-images \
    --repository-name "$repo_name" \
    --query 'sort_by(imageDetails, &imagePushedAt)[-1].imageDigest' \
    --output text 2>&1)

  if [[ -z "$latest_digest" || "$latest_digest" == "None" ]]; then
    err "Could not retrieve latest image digest from ECR"
    exit 1
  fi

  info "Latest image digest: $latest_digest"

  # Update Lambda to use the image by digest (not tag) to force pull
  info "Updating Lambda function code..."
  aws lambda update-function-code \
    --function-name "$function_name" \
    --image-uri "${repo_uri}@${latest_digest}" \
    >/dev/null || {
    err "Failed to update Lambda function"
    exit 1
  }

  # Wait for the update to complete
  info "Waiting for Lambda function update to complete..."
  aws lambda wait function-updated --function-name "$function_name" 2>/dev/null || true

  info "Lambda function successfully updated with latest Docker image"
}

# Main function to build, push, and update Lambda
update_lambda_image() {
  build_docker_image
  local repo_uri
  repo_uri=$(push_image_to_ecr)
  update_lambda_with_latest_image "$repo_uri"
}

# If script is executed directly (not sourced), run the main function
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  # Check prerequisites
  command -v aws >/dev/null || { err "aws CLI not found"; exit 1; }
  command -v docker >/dev/null || { err "docker not found"; exit 1; }

  info "Starting Lambda image update process..."
  update_lambda_image
  info "Done!"
fi
