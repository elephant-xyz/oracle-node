#!/bin/bash

# Build script for optimized Lambda packages
# This script installs only the required dependencies for each Lambda function

set -e

echo "🚀 Building optimized Lambda packages..."

# Function to install dependencies for a lambda
build_lambda() {
  local lambda_path=$1
  local lambda_name=$2
  
  echo "📦 Building $lambda_name..."
  cd "$lambda_path"
  
  # Clean previous builds
  rm -rf node_modules package-lock.json
  
  # Install only production dependencies
  npm install --only=production
  
  echo "✅ $lambda_name build complete"
  cd - > /dev/null
}

# Build prepare lambdas
build_lambda "prepare/lambdas/downloader" "Downloader Function"
build_lambda "prepare/lambdas/updater" "Updater Function"

# Build workflow lambdas
build_lambda "workflow/lambdas/starter" "Workflow Starter Function"
build_lambda "workflow/lambdas/pre" "Workflow Pre-processor Function"

echo ""
echo "🎉 All Lambda packages built successfully!"
echo ""
echo "Package sizes:"
echo "--------------"

# Show package sizes
echo "Downloader:    $(du -sh prepare/lambdas/downloader/node_modules 2>/dev/null | cut -f1)"
echo "Updater:       $(du -sh prepare/lambdas/updater/node_modules 2>/dev/null | cut -f1)"
echo "Starter:       $(du -sh workflow/lambdas/starter/node_modules 2>/dev/null | cut -f1)"
echo "Pre-processor: $(du -sh workflow/lambdas/pre/node_modules 2>/dev/null | cut -f1)"

echo ""
echo "💡 Benefits of this optimization:"
echo "   • Reduced package sizes by including only necessary dependencies"
echo "   • Faster cold starts due to smaller packages"
echo "   • Lower deployment time and storage costs"
echo "   • Better separation of concerns"