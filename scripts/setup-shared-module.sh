#!/bin/bash

# Setup shared module symlinks for development
# This script creates symlinks to the shared module in each lambda's node_modules

set -e

echo "Setting up shared module symlinks for development..."

# Get the absolute path to the shared module
SHARED_MODULE_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/workflow/lambdas/shared/nodejs/node_modules/@elephant/shared"

# List of lambdas that need the shared module
LAMBDAS=("post" "pre" "starter" "submit")

for lambda in "${LAMBDAS[@]}"; do
    LAMBDA_NODE_MODULES="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/workflow/lambdas/${lambda}/node_modules/@elephant"

    echo "Setting up symlink for ${lambda} lambda..."

    # Create the @elephant directory if it doesn't exist
    mkdir -p "${LAMBDA_NODE_MODULES}"

    # Remove existing symlink if it exists
    if [ -L "${LAMBDA_NODE_MODULES}/shared" ]; then
        rm "${LAMBDA_NODE_MODULES}/shared"
    fi

    # Create symlink
    ln -s "${SHARED_MODULE_PATH}" "${LAMBDA_NODE_MODULES}/shared"

    echo "✓ Created symlink for ${lambda}"
done

echo "Shared module setup complete!"
echo "You can now import from '@elephant/shared' in your lambdas."
