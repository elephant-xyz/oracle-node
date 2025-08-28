#!/bin/sh

# MWAA Startup Script - Install UV Package Manager
# This script runs on every Apache Airflow component (worker, scheduler, web server)
# during startup before installing requirements and initializing the Apache Airflow process.

echo "========================================="
echo "MWAA Startup Script Execution Started"
echo "========================================="
echo "Timestamp: $(date)"
echo "Component: $MWAA_AIRFLOW_COMPONENT"
echo "Python Version: $(python --version 2>&1)"
echo "========================================="

# Only install uv on workers and schedulers (skip webserver to reduce startup time)
if [[ "${MWAA_AIRFLOW_COMPONENT}" != "webserver" ]]; then

    echo "Updating the OS"
    # Update OS safely (never upgrade Python on MWAA)
    sudo yum -y update --exclude=python*

    echo "Installing tar"
    # Ensure required tools for nvm/node downloads
    sudo yum -y install tar
    echo "Installing uv package manager for $MWAA_AIRFLOW_COMPONENT..."
    
    # Check if uv is already installed
    if command -v uv &> /dev/null; then
        echo "uv is already installed: $(uv --version)"
    else
        echo "Downloading and installing uv..."
        
        # Install uv using the official installation script
        # Using the standalone installer that doesn't require cargo/rust
        curl -LsSf https://astral.sh/uv/install.sh | sh
        
        # Add uv to PATH for the current session
        export PATH="$HOME/.cargo/bin:$PATH"
        echo "python --version"
        echo "python3 --version"
        
        # Alternative: If the above doesn't work, try installing via pip
        # python -m pip install uv
        
        # Verify installation
        if command -v uv &> /dev/null; then
            echo "✓ uv installed successfully"
            echo "  Version: $(uv --version)"
            echo "  Location: $(which uv)"
        else
            echo "✗ WARNING: uv installation verification failed"
            echo "  PATH: $PATH"
            # Try alternative installation method
            echo "Attempting alternative installation via pip..."
            python -m pip install uv
            if command -v uv &> /dev/null; then
                echo "✓ uv installed via pip successfully"
            else
                echo "✗ ERROR: Failed to install uv"
            fi
        fi
    fi

    echo "Installing nodejs"
    # Download and install nvm:
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash

    # in lieu of restarting the shell
    \. "$HOME/.nvm/nvm.sh"

    # Download and install Node.js:
    nvm install 22

    # Verify the Node.js version:
    node -v
    nvm current

    # Verify npm version:
    npm -v

    echo "Installing elephant-cli"

    npm install -g @elephant-xyz/cli

    
    # Export PATH for DAG tasks
    echo "export PATH=\"$HOME/.cargo/bin:\$PATH\"" >> ~/.bashrc
    
else
    echo "Skipping uv installation on webserver component"
fi

echo "========================================="
echo "Startup Script Execution Completed"
echo "========================================="
