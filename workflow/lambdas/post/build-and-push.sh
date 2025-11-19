#!/bin/bash

# Build and push Docker image for Post Lambda to ECR
# Usage: ./build-and-push.sh [stack-name] [aws-region] [aws-profile]

set -e

# Default values
STACK_NAME="${1:-elephant-oracle-node}"
AWS_REGION="${2:-us-east-1}"
AWS_PROFILE="${3:-default}"

echo "==================================="
echo "Post Lambda Docker Build & Push"
echo "==================================="
echo "Stack Name: $STACK_NAME"
echo "AWS Region: $AWS_REGION"
echo "AWS Profile: $AWS_PROFILE"
echo "==================================="

# Get AWS Account ID
echo "📋 Getting AWS Account ID..."
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --profile "$AWS_PROFILE" --query Account --output text)
echo "   Account ID: $AWS_ACCOUNT_ID"

# Get ECR repository name from CloudFormation stack outputs
echo "📋 Getting ECR repository name from stack outputs..."
REPO_NAME=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE" \
  --query "Stacks[0].Outputs[?OutputKey=='PostLambdaEcrRepositoryName'].OutputValue" \
  --output text)

if [ -z "$REPO_NAME" ]; then
  echo "❌ Error: Could not find PostLambdaEcrRepositoryName in stack outputs."
  echo "   Make sure the stack '$STACK_NAME' exists and has been deployed with the ECR repository."
  exit 1
fi

echo "   Repository Name: $REPO_NAME"

# Construct full repository URI
REPO_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPO_NAME}"
echo "   Repository URI: $REPO_URI"

# Login to ECR
echo ""
echo "🔐 Logging in to ECR..."
aws ecr get-login-password --region "$AWS_REGION" --profile "$AWS_PROFILE" \
  | docker login --username AWS --password-stdin "$REPO_URI"

# Build Docker image
echo ""
echo "🔨 Building Docker image..."
docker build --platform linux/amd64 -t post-lambda:latest .

# Tag the image
echo ""
echo "🏷️  Tagging image..."
docker tag post-lambda:latest "$REPO_URI:latest"

# Push to ECR
echo ""
echo "📤 Pushing image to ECR..."
docker push "$REPO_URI:latest"

echo ""
echo "✅ Successfully built and pushed Docker image!"
echo "   Image URI: $REPO_URI:latest"
echo ""
echo "⚠️  Note: You need to update the Lambda function to use this new image."
echo "   The Lambda will automatically use the ':latest' tag on next deployment."
echo ""
echo "🚀 To deploy the updated Lambda, run:"
echo "   sam deploy --no-confirm-changeset --no-fail-on-empty-changeset"
