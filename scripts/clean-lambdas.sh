#!/bin/bash

# Clean script for Lambda packages
# Removes all node_modules and lock files

set -e

echo "🧹 Cleaning Lambda packages..."

# Function to clean a lambda
clean_lambda() {
  local lambda_path=$1
  local lambda_name=$2
  
  echo "🗑️  Cleaning $lambda_name..."
  cd "$lambda_path"
  
  # Remove node_modules and lock files
  rm -rf node_modules package-lock.json
  
  echo "✅ $lambda_name cleaned"
  cd - > /dev/null
}

# Clean prepare lambdas
clean_lambda "prepare/lambdas/downloader" "Downloader Function"
clean_lambda "prepare/lambdas/updater" "Updater Function"

# Clean workflow lambdas
clean_lambda "workflow/lambdas/starter" "Workflow Starter Function"
clean_lambda "workflow/lambdas/pre" "Workflow Pre-processor Function"

echo ""
echo "🎉 All Lambda packages cleaned successfully!"