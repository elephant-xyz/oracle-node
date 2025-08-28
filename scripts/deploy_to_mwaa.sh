#!/bin/bash
# Deploy Elephant Airflow DAG to MWAA
# Usage: ./deploy_to_mwaa.sh

set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 MWAA Deployment Script${NC}"
echo "================================"

# Resolve S3 bucket from CloudFormation (or allow override via S3_BUCKET)
CFN_STACK_NAME="mwaa-environment-public-network"
CFN_OUTPUT_KEY="EnvironmentBucketName"

if [ -z "$S3_BUCKET" ]; then
  echo -e "${YELLOW}🔎 Resolving S3 bucket from CloudFormation stack '${CFN_STACK_NAME}'...${NC}"
  S3_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name "$CFN_STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='${CFN_OUTPUT_KEY}'].OutputValue" \
    --output text)
fi

if [ -z "$S3_BUCKET" ] || [ "$S3_BUCKET" = "None" ]; then
  echo -e "${RED}❌ Error: Could not determine S3 bucket. Ensure CloudFormation stack '${CFN_STACK_NAME}' has output '${CFN_OUTPUT_KEY}', or set S3_BUCKET explicitly.${NC}"
  exit 1
fi

echo -e "${GREEN}✓ Using S3 bucket:${NC} $S3_BUCKET"

# MWAA environment name (defaults to the given environment)
MWAA_ENV_NAME=${MWAA_ENV_NAME:-"mwaa-environment-public-network-MwaaEnvironment"}
echo -e "${GREEN}✓ Using MWAA environment:${NC} $MWAA_ENV_NAME"

echo -e "${YELLOW}🧰 Zipping transform scripts...${NC}"
SCRIPTS_ZIP="build/transform_scripts.zip"
mkdir -p "$(dirname "$SCRIPTS_ZIP")"
rm -f "$SCRIPTS_ZIP"

if ! command -v zip &>/dev/null; then
  echo -e "${RED}❌ Error: 'zip' command not found. Please install zip.${NC}"
  exit 1
fi

# Zip contents of transform/ at the archive root
pushd transform >/dev/null
zip -r "../$SCRIPTS_ZIP" . >/dev/null
popd >/dev/null
echo -e "${GREEN}✓ Scripts zipped to $SCRIPTS_ZIP${NC}"

echo -e "${YELLOW}📤 Uploading files to S3...${NC}"

# Upload DAG file
echo "  Uploading DAG file..."
aws s3 cp dags/elephant_workflow.py s3://${S3_BUCKET}/dags/elephant_workflow.py --no-progress
echo -e "${GREEN}  ✓ DAG file uploaded${NC}"


# Upload transform scripts
echo "  Uploading transform scripts..."
SCRIPTS_S3_URI="s3://${S3_BUCKET}/scripts/transform_scripts.zip"
aws s3 cp "$SCRIPTS_ZIP" "$SCRIPTS_S3_URI" --no-progress
echo -e "${GREEN}  ✓ Transform scripts uploaded${NC}"


echo ""
# Update Airflow Variable with scripts S3 URI (requires MWAA_ENV_NAME)
if [ -n "$MWAA_ENV_NAME" ]; then
  echo -e "${YELLOW}🔧 Updating Airflow variable 'elephant_scripts_s3_uri'...${NC}"

  if ! command -v jq &>/dev/null; then
    echo -e "${RED}❌ Error: 'jq' is required to update Airflow variables. Please install jq.${NC}"
    exit 1
  fi

  TOKEN_JSON=$(aws mwaa create-cli-token --name "$MWAA_ENV_NAME")
  CLI_TOKEN=$(echo "$TOKEN_JSON" | jq -r '.CliToken')
  WEB_SERVER_HOSTNAME=$(echo "$TOKEN_JSON" | jq -r '.WebServerHostname')

  if [ -z "$CLI_TOKEN" ] || [ -z "$WEB_SERVER_HOSTNAME" ] || [ "$CLI_TOKEN" = "null" ] || [ "$WEB_SERVER_HOSTNAME" = "null" ]; then
    echo -e "${RED}❌ Failed to obtain MWAA CLI token or hostname. Check MWAA_ENV_NAME and AWS credentials.${NC}"
    exit 1
  fi

  RESPONSE=$(curl -sS --location --request POST "https://${WEB_SERVER_HOSTNAME}/aws_mwaa/cli" \
    --header "Authorization: Bearer ${CLI_TOKEN}" \
    --header "Content-Type: text/plain" \
    --data-raw "variables set elephant_scripts_s3_uri ${SCRIPTS_S3_URI}")

  STDOUT=$(echo "$RESPONSE" | jq -r '.stdout' | base64 -d 2>/dev/null || true)
  STDERR=$(echo "$RESPONSE" | jq -r '.stderr' | base64 -d 2>/dev/null || true)

  # Treat warnings on stderr as non-fatal if stdout indicates success.
  if [ -n "$STDERR" ] && [ "$STDERR" != "null" ]; then
    echo -e "${YELLOW}⚠ Airflow CLI warnings:${NC} $STDERR"
  fi

  if [ -z "$STDOUT" ] || [ "$STDOUT" = "null" ]; then
    echo -e "${RED}❌ Failed to set Airflow variable: no stdout from MWAA CLI.${NC}"
    exit 1
  fi

  echo "$STDOUT"
  echo -e "${GREEN}✓ Airflow variable updated${NC}"
else
  echo -e "${YELLOW}⚠ MWAA_ENV_NAME not set; skipping Airflow variable update.${NC}"
  echo "Set 'elephant_scripts_s3_uri' in Airflow to: ${SCRIPTS_S3_URI}"
fi

echo ""
echo -e "${GREEN}✅ Deployment complete!${NC}"
echo ""
