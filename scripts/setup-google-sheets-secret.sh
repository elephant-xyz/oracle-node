#!/bin/bash

# Script to set up Google Sheets secret in AWS Secrets Manager for SQS Message Counter Lambda

set -euo pipefail

# Color output functions
print_error() { echo -e "\033[31m❌ ERROR: $1\033[0m" >&2; }
print_success() { echo -e "\033[32m✅ $1\033[0m"; }
print_warning() { echo -e "\033[33m⚠️  WARNING: $1\033[0m"; }
print_info() { echo -e "\033[34mℹ️  $1\033[0m"; }

# Default values
CREDENTIALS_FILE=""
SHEET_ID=""
TAB_NAME=""
SECRET_NAME=""
STACK_NAME=""
REGION="${AWS_REGION:-us-east-1}"
PROFILE="${AWS_PROFILE:-}"

show_usage() {
    cat <<EOF
Usage: $0 [OPTIONS] <credentials_file>

Set up Google Sheets secret in AWS Secrets Manager for SQS Message Counter Lambda.

ARGUMENTS:
    credentials_file      Path to Google service account credentials JSON file

OPTIONS:
    --sheet-id ID         Google Sheet ID (from the sheet URL) (required)
    --tab-name NAME       Name of the sheet tab to update (required)
    --secret-name NAME    Name of the secret in AWS Secrets Manager
                         (default: auto-detect from stack name)
    --stack-name NAME     CloudFormation stack name (auto-detected if not provided)
    --region REGION       AWS region (default: us-east-1)
    --profile PROFILE     AWS profile to use (default: from AWS_PROFILE env var)
    --help                Show this help message

EXAMPLES:
    # Use default secret name and region
    $0 service.json --sheet-id "1abc123" --tab-name "Queue Counts"

    # Specify custom secret name and region
    $0 service.json \\
        --secret-name spreadsheet-api/google-credentials \\
        --sheet-id "1abc123" \\
        --tab-name "Queue Counts" \\
        --region us-east-1

    # With stack name prefix (auto-detected if not provided)
    $0 service.json \\
        --stack-name elephant-oracle-node \\
        --sheet-id "1abc123" \\
        --tab-name "Queue Counts" \\
        --profile oracle-saman

EOF
}

parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --sheet-id)
                SHEET_ID="$2"
                shift 2
                ;;
            --tab-name)
                TAB_NAME="$2"
                shift 2
                ;;
            --secret-name)
                SECRET_NAME="$2"
                shift 2
                ;;
            --stack-name)
                STACK_NAME="$2"
                shift 2
                ;;
            --region)
                REGION="$2"
                shift 2
                ;;
            --profile)
                PROFILE="$2"
                shift 2
                ;;
            --help)
                show_usage
                exit 0
                ;;
            -*)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
            *)
                if [[ -z "$CREDENTIALS_FILE" ]]; then
                    CREDENTIALS_FILE="$1"
                else
                    print_error "Unexpected argument: $1"
                    show_usage
                    exit 1
                fi
                shift
                ;;
        esac
    done
}

# Validate required arguments
validate_arguments() {
    if [[ -z "$CREDENTIALS_FILE" ]]; then
        print_error "Credentials file is required"
        show_usage
        exit 1
    fi
    
    if [[ ! -f "$CREDENTIALS_FILE" ]]; then
        print_error "Credentials file not found: $CREDENTIALS_FILE"
        exit 1
    fi
    
    if [[ -z "$SHEET_ID" ]]; then
        print_error "Sheet ID is required (--sheet-id)"
        show_usage
        exit 1
    fi
    
    if [[ -z "$TAB_NAME" ]]; then
        print_error "Tab name is required (--tab-name)"
        show_usage
        exit 1
    fi
}

# Validate credentials file format
validate_credentials() {
    local cred_file="$1"
    
    # Check if file is valid JSON
    if ! jq . "$cred_file" > /dev/null 2>&1; then
        print_error "Invalid JSON in credentials file: $cred_file"
        exit 1
    fi
    
    # Check for required fields
    local required_fields=("type" "project_id" "private_key_id" "private_key" "client_email")
    for field in "${required_fields[@]}"; do
        if ! jq -e ".$field" "$cred_file" > /dev/null 2>&1; then
            print_error "Missing required field in credentials: $field"
            exit 1
        fi
    done
    
    # Validate that type is service_account
    local cred_type
    cred_type=$(jq -r '.type' "$cred_file")
    if [[ "$cred_type" != "service_account" ]]; then
        print_error "Credentials type must be 'service_account', got: $cred_type"
        exit 1
    fi
}

# Auto-detect CloudFormation stack name
get_stack_name() {
    if [[ -n "$STACK_NAME" ]]; then
        echo "$STACK_NAME"
        return
    fi
    
    local profile_arg=""
    if [[ -n "$PROFILE" ]]; then
        profile_arg="--profile $PROFILE"
    fi
    
    print_info "Auto-detecting CloudFormation stack name..." >&2
    
    local stacks
    stacks=$(aws cloudformation list-stacks \
        --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE \
        --region "$REGION" \
        $profile_arg \
        --query "StackSummaries[?contains(StackName, 'oracle') || contains(StackName, 'prepare')].StackName" \
        --output text 2>/dev/null || echo "")
    
    if [[ -z "$stacks" ]]; then
        print_warning "No suitable CloudFormation stack found" >&2
        return 1
    fi
    
    # Prefer 'elephant-oracle-node' if it exists
    if echo "$stacks" | grep -q "elephant-oracle-node"; then
        echo "elephant-oracle-node"
        print_info "Found stack: elephant-oracle-node" >&2
    else
        local first_stack
        first_stack=$(echo "$stacks" | awk '{print $1}')
        echo "$first_stack"
        print_info "Found stack: $first_stack" >&2
    fi
}

# Determine secret name
get_secret_name() {
    if [[ -n "$SECRET_NAME" ]]; then
        echo "$SECRET_NAME"
        return
    fi
    
    local detected_stack
    detected_stack=$(get_stack_name 2>/dev/null || echo "")
    
    if [[ -n "$detected_stack" ]]; then
        echo "${detected_stack}/google-sheets/config"
    else
        echo "spreadsheet-api/google-credentials"
    fi
}

# Create or update secret in AWS Secrets Manager
create_or_update_secret() {
    local secret_name="$1"
    local secret_value="$2"
    local profile_arg=""
    
    if [[ -n "$PROFILE" ]]; then
        profile_arg="--profile $PROFILE"
    fi
    
    # Check if secret exists
    if aws secretsmanager describe-secret \
        --secret-id "$secret_name" \
        --region "$REGION" \
        $profile_arg \
        > /dev/null 2>&1; then
        print_info "Secret '$secret_name' already exists. Updating..."
        aws secretsmanager put-secret-value \
            --secret-id "$secret_name" \
            --region "$REGION" \
            $profile_arg \
            --secret-string "$secret_value" \
            > /dev/null 2>&1
        print_success "Successfully updated secret '$secret_name'"
    else
        print_info "Creating new secret '$secret_name'..."
        aws secretsmanager create-secret \
            --name "$secret_name" \
            --region "$REGION" \
            $profile_arg \
            --secret-string "$secret_value" \
            > /dev/null 2>&1
        print_success "Successfully created secret '$secret_name'"
    fi
}

# Main function
main() {
    parse_arguments "$@"
    validate_arguments
    validate_credentials "$CREDENTIALS_FILE"
    
    local secret_name
    secret_name=$(get_secret_name)
    
    # Read credentials
    local credentials
    credentials=$(cat "$CREDENTIALS_FILE")
    
    # Get service account email for display
    local service_account
    service_account=$(echo "$credentials" | jq -r '.client_email // "N/A"')
    
    # Create secret value JSON
    local secret_value
    secret_value=$(jq -n \
        --arg sheetId "$SHEET_ID" \
        --arg tabName "$TAB_NAME" \
        --argjson credentials "$credentials" \
        '{
            sheetId: $sheetId,
            tabName: $tabName,
            credentials: $credentials
        }')
    
    # Display configuration
    print_info "Setting up secret: $secret_name"
    print_info "  Sheet ID: $SHEET_ID"
    print_info "  Tab Name: $TAB_NAME"
    print_info "  Service Account: $service_account"
    echo ""
    
    # Create or update secret
    create_or_update_secret "$secret_name" "$secret_value"
    
    echo ""
    print_success "Google Sheets secret setup complete!"
    echo ""
    print_info "The SQS Message Counter Lambda will use this secret to update Google Sheets."
    print_info "Make sure the Lambda has permission to read: $secret_name"
}

main "$@"

