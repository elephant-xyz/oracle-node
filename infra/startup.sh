#!/bin/sh

# MWAA Startup Script - Install UV Package Manager and Node tooling for elephant-cli

echo "========================================="
echo "MWAA Startup Script Execution Started"
echo "========================================="
echo "Timestamp: $(date)"
echo "Component: $MWAA_AIRFLOW_COMPONENT"
echo "Python Version: $(python --version 2>&1)"
echo "========================================="

if [[ "${MWAA_AIRFLOW_COMPONENT}" != "webserver" ]]; then
    echo "Updating the OS"
    sudo yum -y update --exclude=python*

    echo "Installing tar"
    sudo yum -y install tar

    echo "Installing uv package manager..."
    if command -v uv >/dev/null 2>&1; then
        echo "uv is already installed: $(uv --version)"
    else
        curl -LsSf https://astral.sh/uv/install.sh | sh
        export PATH="$HOME/.cargo/bin:$PATH"
        if ! command -v uv >/dev/null 2>&1; then
            echo "uv install via shell failed; attempting pip"
            python -m pip install uv || true
        fi
        command -v uv && echo "uv installed: $(uv --version)"
    fi

    echo "Installing Node.js via nvm for elephant-cli"
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash
    . "$HOME/.nvm/nvm.sh"
    nvm install 22
    node -v
    npm -v

    echo "Installing elephant-cli"
    npm install -g @elephant-xyz/cli || true

    echo "Export PATH for DAG tasks"
    echo "export PATH=\"$HOME/.cargo/bin:\$PATH\"" >> ~/.bashrc
else
    echo "Skipping heavy setup on webserver component"
fi

echo "========================================="
echo "Startup Script Execution Completed"
echo "========================================="

