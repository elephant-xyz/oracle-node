#!/bin/bash

# MWAA Deployment Script with UV Installation
# This script automates the deployment and update of MWAA environment with UV package manager

set -e # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
STACK_NAME="mwaa-environment-public-network"
TEMPLATE_FILE="mwaa-public-network.yaml"
STARTUP_SCRIPT="startup.sh"
REQUIREMENTS_FILE="requirements.txt"
AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.12.txt"

# Usage
usage() {
  cat <<USAGE
Usage: $0 [S3_BUCKET_ARN] [options]

Arguments:
  S3_BUCKET_ARN              ARN of S3 bucket in another account for cross-account access (optional)

Options:
  -b, --bucket <name>        S3 bucket name to use if stack output is unavailable
  -h, --help                 Show this help and exit

Examples:
  $0                                                    # Deploy without cross-account S3
  $0 arn:aws:s3:::my-bucket                           # Deploy with cross-account S3
  $0 arn:aws:s3:::my-bucket -b my-mwaa-bucket        # Deploy with both S3 ARN and MWAA bucket name
USAGE
}

# Functions
print_status() {
  echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
  echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
  echo -e "${YELLOW}[WARNING]${NC} $1"
}

check_prerequisites() {
  print_status "Checking prerequisites..."

  # Check AWS CLI
  if ! command -v aws &>/dev/null; then
    print_error "AWS CLI is not installed. Please install it first."
    exit 1
  fi

  # Check AWS credentials
  if ! aws sts get-caller-identity &>/dev/null; then
    print_error "AWS credentials are not configured. Please run 'aws configure'."
    exit 1
  fi

  # Check required files
  for file in "$TEMPLATE_FILE" "$STARTUP_SCRIPT"; do
    if [ ! -f "$file" ]; then
      print_error "Required file $file not found."
      exit 1
    fi
  done

  # Check uv is installed
  if ! command -v uv &>/dev/null; then
    print_error "uv is not installed. Please install it: https://docs.astral.sh/uv/getting-started/installation/"
    exit 1
  fi

  print_status "Prerequisites check passed."
}

check_stack_exists() {
  aws cloudformation describe-stacks --stack-name "$STACK_NAME" &>/dev/null
  return $?
}

create_stack() {
  print_status "Creating CloudFormation stack: $STACK_NAME"

  aws cloudformation create-stack \
    --stack-name "$STACK_NAME" \
    --template-body "file://$TEMPLATE_FILE" \
    --capabilities CAPABILITY_IAM \
    --on-failure DO_NOTHING

  print_status "Waiting for stack creation to complete (this may take 15-20 minutes)..."
  aws cloudformation wait stack-create-complete --stack-name "$STACK_NAME"

  if [ $? -eq 0 ]; then
    print_status "Stack created successfully!"
  else
    print_error "Stack creation failed. Check CloudFormation console for details."
    exit 1
  fi
}

get_bucket_name() {
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`EnvironmentBucketName`].OutputValue' \
    --output text
}

upload_startup_script() {
  local bucket_name=$1

  print_status "Uploading startup script to S3 bucket: $bucket_name"
  aws s3 cp "$STARTUP_SCRIPT" "s3://$bucket_name/$STARTUP_SCRIPT"

  if [ $? -eq 0 ]; then
    print_status "Startup script uploaded successfully."
  else
    print_error "Failed to upload startup script."
    exit 1
  fi
}

export_requirements_file() {
  print_status "Exporting $REQUIREMENTS_FILE from uv lock..."
  uv pip compile pyproject.toml --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.12.txt >"$REQUIREMENTS_FILE"
  if [ $? -eq 0 ]; then
    print_status "Exported $REQUIREMENTS_FILE successfully."
  else
    print_error "Failed to export $REQUIREMENTS_FILE via uv."
    exit 1
  fi

  # Prepend Airflow constraints to requirements for MWAA pip resolution
  if ! grep -q "^-c $AIRFLOW_CONSTRAINTS_URL$" "$REQUIREMENTS_FILE"; then
    print_status "Prepending Airflow constraints to $REQUIREMENTS_FILE"
    {
      echo "-c $AIRFLOW_CONSTRAINTS_URL"
      cat "$REQUIREMENTS_FILE"
    } >"$REQUIREMENTS_FILE.tmp" && mv "$REQUIREMENTS_FILE.tmp" "$REQUIREMENTS_FILE"
  fi
}

upload_requirements_file() {
  local bucket_name=$1
  print_status "Uploading $REQUIREMENTS_FILE to S3 bucket: $bucket_name"
  aws s3 cp "$REQUIREMENTS_FILE" "s3://$bucket_name/$REQUIREMENTS_FILE"
  if [ $? -eq 0 ]; then
    print_status "$REQUIREMENTS_FILE uploaded successfully."
  else
    print_error "Failed to upload $REQUIREMENTS_FILE."
    exit 1
  fi
}

get_script_version_id() {
  local bucket_name=$1

  aws s3api list-object-versions \
    --bucket "$bucket_name" \
    --prefix "$STARTUP_SCRIPT" \
    --query 'Versions[?IsLatest].[VersionId]' \
    --output text
}

get_requirements_version_id() {
  local bucket_name=$1
  aws s3api list-object-versions \
    --bucket "$bucket_name" \
    --prefix "$REQUIREMENTS_FILE" \
    --query 'Versions[?IsLatest].[VersionId]' \
    --output text
}

generate_parameters() {
  local bucket_name=$1
  local startup_version_id=$2
  local requirements_version_id=$3

  print_status "Generating parameters with versions: startup=$startup_version_id, requirements=$requirements_version_id"

  # Start building the parameters array
  cat >/tmp/parameters.json <<EOF
[
  {
    "ParameterKey": "StartupScriptS3Path",
    "ParameterValue": "${STARTUP_SCRIPT}"
  },
  {
    "ParameterKey": "StartupScriptS3ObjectVersion",
    "ParameterValue": "$startup_version_id"
  },
  {
    "ParameterKey": "RequirementsS3Path",
    "ParameterValue": "${REQUIREMENTS_FILE}"
  },
  {
    "ParameterKey": "RequirementsS3ObjectVersion",
    "ParameterValue": "$requirements_version_id"
  }
EOF

  # Add CrossAccountS3BucketArn parameter if provided
  if [ -n "$CLI_S3_BUCKET_ARN" ]; then
    cat >>/tmp/parameters.json <<EOF
,
  {
    "ParameterKey": "CrossAccountS3BucketArn",
    "ParameterValue": "$CLI_S3_BUCKET_ARN"
  }
EOF
    print_status "Adding cross-account S3 bucket ARN: $CLI_S3_BUCKET_ARN"
  fi

  # Close the JSON array
  echo "]" >>/tmp/parameters.json

  print_status "Parameters generated."
}

update_stack() {
  print_status "Updating CloudFormation stack with startup script configuration..."

  aws cloudformation update-stack \
    --stack-name "$STACK_NAME" \
    --template-body "file://$TEMPLATE_FILE" \
    --parameters "file:///tmp/parameters.json" \
    --capabilities CAPABILITY_IAM

  if [ $? -ne 0 ]; then
    print_warning "No updates to perform or update failed. This is normal if the stack is already up-to-date."
    return
  fi

  print_status "Waiting for stack update to complete (this may take 10-30 minutes)..."
  aws cloudformation wait stack-update-complete --stack-name "$STACK_NAME"

  if [ $? -eq 0 ]; then
    print_status "Stack updated successfully!"
  else
    print_error "Stack update failed. Check CloudFormation console for details."
    exit 1
  fi
}

get_airflow_url() {
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`MwaaApacheAirflowUI`].OutputValue' \
    --output text
}

get_sqs_queue_arn() {
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`MwaaSqsQueueArn`].OutputValue' \
    --output text
}

get_sqs_queue_url() {
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs[?OutputKey==`MwaaSqsQueueUrl`].OutputValue' \
    --output text
}

# Main execution
main() {
  echo "======================================"
  echo "MWAA Deployment Script"
  echo "======================================"

  # Handle positional argument for S3 bucket ARN
  CLI_S3_BUCKET_ARN=""
  CLI_BUCKET_NAME=""
  
  # Check if first argument is an S3 ARN (starts with "arn:aws:s3:")
  if [[ "$1" =~ ^arn:aws:s3: ]]; then
    CLI_S3_BUCKET_ARN="$1"
    print_status "Using cross-account S3 bucket: $CLI_S3_BUCKET_ARN"
    shift # Remove the first argument so remaining args can be processed
  fi
  
  # Parse remaining arguments
  while [[ $# -gt 0 ]]; do
    case "$1" in
    -b | --bucket)
      if [[ -z "$2" || "$2" == -* ]]; then
        print_error "Missing value for $1"
        usage
        exit 1
      fi
      CLI_BUCKET_NAME="$2"
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      print_error "Unknown argument: $1"
      usage
      exit 1
      ;;
    esac
  done

  check_prerequisites

  if check_stack_exists; then
    print_status "Stack already exists. Will update it with startup script."
    STACK_ACTION="update"
  else
    print_status "Stack does not exist. Will create it first."
    STACK_ACTION="create"
    create_stack
  fi

  # Resolve bucket name: prefer CLI if provided, otherwise use stack output
  if [ -n "$CLI_BUCKET_NAME" ]; then
    BUCKET_NAME="$CLI_BUCKET_NAME"
    print_status "Using S3 bucket from CLI: $BUCKET_NAME"
  else
    BUCKET_NAME=$(get_bucket_name)
    if [ -z "$BUCKET_NAME" ]; then
      print_error "Could not retrieve bucket name from stack outputs. Provide one with --bucket <name>."
      exit 1
    fi
    print_status "Using S3 bucket from stack outputs: $BUCKET_NAME"
  fi

  # Upload startup script
  upload_startup_script "$BUCKET_NAME"

  # Get version ID for startup script
  VERSION_ID=$(get_script_version_id "$BUCKET_NAME")
  if [ -z "$VERSION_ID" ]; then
    print_error "Could not retrieve version ID for startup script."
    exit 1
  fi
  print_status "Startup script version ID: $VERSION_ID"

  # Export and upload requirements, then get version ID
  export_requirements_file
  upload_requirements_file "$BUCKET_NAME"
  rm $REQUIREMENTS_FILE
  REQUIREMENTS_VERSION_ID=$(get_requirements_version_id "$BUCKET_NAME")
  if [ -z "$REQUIREMENTS_VERSION_ID" ]; then
    print_error "Could not retrieve version ID for $REQUIREMENTS_FILE."
    exit 1
  fi
  print_status "$REQUIREMENTS_FILE version ID: $REQUIREMENTS_VERSION_ID"

  # Generate parameters with bucket name and version ID
  generate_parameters "$BUCKET_NAME" "$VERSION_ID" "$REQUIREMENTS_VERSION_ID"

  # Update stack if it already existed
  if [ "$STACK_ACTION" == "update" ]; then
    update_stack
  else
    print_status "Initial stack creation completed. Running update to add startup script..."
    update_stack
  fi

  # Clean up temporary parameters file
  rm -f /tmp/parameters.json

  # Get Airflow URL
  AIRFLOW_URL=$(get_airflow_url)
  
  # Get SQS Queue information
  SQS_QUEUE_ARN=$(get_sqs_queue_arn)
  SQS_QUEUE_URL=$(get_sqs_queue_url)
  
  # Get AWS region
  AWS_REGION=$(aws configure get region)
  if [ -z "$AWS_REGION" ]; then
    AWS_REGION="us-east-1"  # Default region if not configured
  fi

  echo ""
  echo "======================================"
  echo -e "${GREEN}Deployment completed successfully!${NC}"
  echo "======================================"
  echo ""
  echo -e "${GREEN}Apache Airflow Web UI:${NC}"
  echo -e "  ${YELLOW}$AIRFLOW_URL${NC}"
  echo ""
  echo -e "${GREEN}AWS Console - MWAA Environment:${NC}"
  echo -e "  ${YELLOW}https://console.aws.amazon.com/mwaa/home?region=$AWS_REGION#environments/detail/$STACK_NAME-MwaaEnvironment${NC}"
  echo ""
  
  # Display SQS information if available
  if [ -n "$SQS_QUEUE_ARN" ]; then
    echo -e "${GREEN}SQS Queue Configuration:${NC}"
    echo -e "  Queue ARN: ${YELLOW}$SQS_QUEUE_ARN${NC}"
    echo -e "  Queue URL: ${YELLOW}$SQS_QUEUE_URL${NC}"
    
    if [ -n "$CLI_S3_BUCKET_ARN" ]; then
      echo ""
      echo -e "${GREEN}Cross-Account S3 Configuration:${NC}"
      echo "  ✓ Queue configured to receive events from: $CLI_S3_BUCKET_ARN"
      echo ""
      echo -e "${YELLOW}Important:${NC} Configure S3 event notifications in the other account:"
      echo "  1. Go to the S3 bucket in the other AWS account"
      echo "  2. Navigate to Properties → Event notifications"
      echo "  3. Create a new event notification"
      echo "  4. Set the destination type to 'SQS queue'"
      echo "  5. Enter the SQS queue ARN: $SQS_QUEUE_ARN"
      echo "  6. Configure the events you want to trigger (e.g., s3:ObjectCreated:*)"
    fi
    echo ""
  fi
  
  echo -e "${GREEN}How to find in AWS Console:${NC}"
  echo "  1. Go to AWS Console: https://console.aws.amazon.com"
  echo "  2. Navigate to Amazon MWAA service"
  echo "  3. Select your region: $AWS_REGION"
  echo "  4. Look for environment: '$STACK_NAME-MwaaEnvironment'"
  echo "  5. Click on the environment name to view details"
  echo "  6. The Airflow UI URL is listed under 'Airflow webserver'"
  echo ""
  echo -e "${GREEN}Additional Resources:${NC}"
  echo "  S3 Bucket: $BUCKET_NAME"
  echo "  CloudFormation Stack: $STACK_NAME"
  echo ""
  echo -e "${GREEN}Next steps:${NC}"
  echo "  1. Wait 5-10 minutes for the environment to fully initialize"
  echo "  2. Access the Airflow Web UI using the URL above"
  echo "  3. Check CloudWatch Logs for startup script execution details:"
  echo "     aws logs tail /aws/mwaa/$STACK_NAME-MwaaEnvironment/scheduler"
  echo ""
}

# Run main function with all arguments
main "$@"
