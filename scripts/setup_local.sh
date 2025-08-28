#!/usr/bin/env bash
set -euo pipefail

# Copies this repo's DAGs into your local Airflow dags folder for standalone runs.

AIRFLOW_HOME_DIR="${AIRFLOW_HOME:-$HOME/airflow}"
REPO_ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST_DAGS_DIR="$AIRFLOW_HOME_DIR/dags/elephant"

echo "AIRFLOW_HOME: $AIRFLOW_HOME_DIR"
echo "Source repo:  $REPO_ROOT_DIR"
echo "Destination:  $DEST_DAGS_DIR"

mkdir -p "$AIRFLOW_HOME_DIR/dags"

# Replace existing copy to avoid stale files
rm -rf "$DEST_DAGS_DIR"
mkdir -p "$DEST_DAGS_DIR"
# Copy CONTENTS of repo's dags/ into $AIRFLOW_HOME/dags/elephant
cp -R "$REPO_ROOT_DIR/dags/"* "$DEST_DAGS_DIR/"

echo "Copied DAGs to: $DEST_DAGS_DIR"

echo
echo "Next steps:"
echo "  1. Start Airflow locally (if not running):  airflow standalone"
echo "  2. Access Airflow UI at http://localhost:8080"
echo "  3. Enable the DAG you want to use:"
echo "     - elephant_workflow (manual trigger)"
echo "     - elephant_workflow_sqs (event-driven)"
echo "  4. Trigger manually with config:"
echo '     {"input_zip_uri": "s3://bucket/input.zip", "output_base_uri": "s3://bucket/outputs"}'


