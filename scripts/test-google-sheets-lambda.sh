#!/bin/bash

# Script to temporarily update Google Sheets secret for Lambda testing
# This allows testing with a test worksheet without affecting production

set -euo pipefail

# Color output functions
print_error() { echo -e "\033[31m❌ ERROR: $1\033[0m" >&2; }
print_success() { echo -e "\033[32m✅ $1\033[0m"; }
print_warning() { echo -e "\033[33m⚠️  WARNING: $1\033[0m"; }
print_info() { echo -e "\033[34mℹ️  $1\033[0m"; }

# Default values
SECRET_NAME=""
TEST_SHEET_ID=""
TEST_TAB_NAME=""
CREDENTIALS_FILE=""
STACK_NAME="${STACK_NAME:-elephant-oracle-node}"
REGION="${AWS_REGION:-us-east-1}"
PROFILE="${AWS_PROFILE:-}"
BACKUP_FILE=""
RESTORE_MODE=false
INVOKE_LAMBDA=true

show_usage() {
    cat <<EOF
Usage: $0 [OPTIONS] [COMMAND]

Temporarily update Google Sheets secret for Lambda testing with a test worksheet.

COMMANDS:
    update      Update secret with test worksheet (default)
    restore     Restore original secret from backup
    status      Show current secret configuration

OPTIONS:
    --secret-name NAME      Secret name in Secrets Manager
                           (default: auto-detect from stack name)
    --test-sheet-id ID      Test Google Sheet ID (required for update)
    --test-tab-name NAME    Test tab name (required for update)
    --credentials-file FILE Path to service-account.json (required for update)
    --stack-name NAME       CloudFormation stack name (default: elephant-oracle-node)
    --region REGION         AWS region (default: us-east-1)
    --profile PROFILE       AWS profile to use
    --backup-file FILE      Backup file path (default: /tmp/google-sheets-secret-backup.json)
    --no-invoke             Don't invoke Lambda after updating secret
    --help                  Show this help message

EXAMPLES:
    # Update secret with test worksheet
    $0 --test-sheet-id "1abc123" --test-tab-name "Test" --credentials-file service-account.json

    # Restore original secret
    $0 restore

    # Check current secret configuration
    $0 status

EOF
}

parse_arguments() {
    local command=""
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            update|restore|status)
                command="$1"
                shift
                ;;
            --secret-name)
                SECRET_NAME="$2"
                shift 2
                ;;
            --test-sheet-id)
                TEST_SHEET_ID="$2"
                shift 2
                ;;
            --test-tab-name)
                TEST_TAB_NAME="$2"
                shift 2
                ;;
            --credentials-file)
                CREDENTIALS_FILE="$2"
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
            --backup-file)
                BACKUP_FILE="$2"
                shift 2
                ;;
            --no-invoke)
                INVOKE_LAMBDA=false
                shift
                ;;
            --help)
                show_usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    if [[ "$command" == "restore" ]]; then
        RESTORE_MODE=true
    elif [[ "$command" == "status" ]]; then
        show_status
        exit 0
    fi
    
    # Set default backup file if not provided
    if [[ -z "$BACKUP_FILE" ]]; then
        BACKUP_FILE="/tmp/google-sheets-secret-backup-${STACK_NAME}.json"
    fi
}

# Get secret name (auto-detect if not provided)
get_secret_name() {
    if [[ -n "$SECRET_NAME" ]]; then
        echo "$SECRET_NAME"
        return
    fi
    
    # Try stack-specific secret first
    local stack_secret="${STACK_NAME}/google-sheets/config"
    local profile_arg=""
    
    if [[ -n "$PROFILE" ]]; then
        profile_arg="--profile $PROFILE"
    fi
    
    # Check if stack-specific secret exists
    if aws secretsmanager describe-secret \
        --secret-id "$stack_secret" \
        --region "$REGION" \
        $profile_arg \
        > /dev/null 2>&1; then
        echo "$stack_secret"
        return
    fi
    
    # Fall back to default secret name
    echo "spreadsheet-api/google-credentials"
}

# Show current secret configuration
show_status() {
    local secret_name
    secret_name=$(get_secret_name)
    
    print_info "Current Secret Configuration:"
    print_info "  Secret Name: $secret_name"
    print_info "  Region: $REGION"
    if [[ -n "$PROFILE" ]]; then
        print_info "  Profile: $PROFILE"
    fi
    echo ""
    
    local profile_arg=""
    if [[ -n "$PROFILE" ]]; then
        profile_arg="--profile $PROFILE"
    fi
    
    if ! aws secretsmanager describe-secret \
        --secret-id "$secret_name" \
        --region "$REGION" \
        $profile_arg \
        > /dev/null 2>&1; then
        print_warning "Secret does not exist: $secret_name"
        return 1
    fi
    
    local secret_value
    secret_value=$(aws secretsmanager get-secret-value \
        --secret-id "$secret_name" \
        --region "$REGION" \
        $profile_arg \
        --query SecretString \
        --output text 2>/dev/null || echo "")
    
    if [[ -z "$secret_value" ]]; then
        print_warning "Could not retrieve secret value"
        return 1
    fi
    
    local sheet_id tab_name
    sheet_id=$(echo "$secret_value" | jq -r '.sheetId // .sheet_id // .SHEET_ID // "N/A"' 2>/dev/null || echo "N/A")
    tab_name=$(echo "$secret_value" | jq -r '.tabName // .tab_name // .TAB_NAME // "N/A"' 2>/dev/null || echo "N/A")
    
    print_info "Current Configuration:"
    print_info "  Sheet ID: $sheet_id"
    print_info "  Tab Name: $tab_name"
    echo ""
    
    if [[ -f "$BACKUP_FILE" ]]; then
        print_info "Backup file exists: $BACKUP_FILE"
        local backup_sheet_id backup_tab_name
        backup_sheet_id=$(jq -r '.sheetId // "N/A"' "$BACKUP_FILE" 2>/dev/null || echo "N/A")
        backup_tab_name=$(jq -r '.tabName // "N/A"' "$BACKUP_FILE" 2>/dev/null || echo "N/A")
        print_info "  Backup Sheet ID: $backup_sheet_id"
        print_info "  Backup Tab Name: $backup_tab_name"
    else
        print_warning "No backup file found: $BACKUP_FILE"
    fi
}

# Backup current secret
backup_secret() {
    local secret_name="$1"
    local profile_arg=""
    
    if [[ -n "$PROFILE" ]]; then
        profile_arg="--profile $PROFILE"
    fi
    
    print_info "Backing up current secret to: $BACKUP_FILE"
    
    local secret_value
    secret_value=$(aws secretsmanager get-secret-value \
        --secret-id "$secret_name" \
        --region "$REGION" \
        $profile_arg \
        --query SecretString \
        --output text 2>/dev/null || echo "")
    
    if [[ -z "$secret_value" ]]; then
        print_error "Failed to retrieve secret value for backup"
        return 1
    fi
    
    echo "$secret_value" > "$BACKUP_FILE"
    print_success "Secret backed up successfully"
}

# Restore secret from backup
restore_secret() {
    local secret_name
    secret_name=$(get_secret_name)
    
    if [[ ! -f "$BACKUP_FILE" ]]; then
        print_error "Backup file not found: $BACKUP_FILE"
        print_error "Cannot restore secret"
        return 1
    fi
    
    local profile_arg=""
    if [[ -n "$PROFILE" ]]; then
        profile_arg="--profile $PROFILE"
    fi
    
    print_info "Restoring secret from backup: $BACKUP_FILE"
    
    local backup_content
    backup_content=$(cat "$BACKUP_FILE")
    
    aws secretsmanager put-secret-value \
        --secret-id "$secret_name" \
        --region "$REGION" \
        $profile_arg \
        --secret-string "$backup_content" \
        > /dev/null 2>&1
    
    if [[ $? -eq 0 ]]; then
        print_success "Secret restored successfully"
        
        # Optionally remove backup file
        read -p "Remove backup file? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            rm -f "$BACKUP_FILE"
            print_success "Backup file removed"
        fi
    else
        print_error "Failed to restore secret"
        return 1
    fi
}

# Update secret with test configuration
update_secret() {
    local secret_name
    secret_name=$(get_secret_name)
    
    # Validate required parameters
    if [[ -z "$TEST_SHEET_ID" ]]; then
        print_error "Test sheet ID is required (--test-sheet-id)"
        exit 1
    fi
    
    if [[ -z "$TEST_TAB_NAME" ]]; then
        print_error "Test tab name is required (--test-tab-name)"
        exit 1
    fi
    
    if [[ -z "$CREDENTIALS_FILE" ]]; then
        print_error "Credentials file is required (--credentials-file)"
        exit 1
    fi
    
    if [[ ! -f "$CREDENTIALS_FILE" ]]; then
        print_error "Credentials file not found: $CREDENTIALS_FILE"
        exit 1
    fi
    
    # Backup current secret
    if ! backup_secret "$secret_name"; then
        print_warning "Could not backup secret, but continuing..."
    fi
    
    # Read credentials
    print_info "Reading credentials from: $CREDENTIALS_FILE"
    local credentials
    credentials=$(cat "$CREDENTIALS_FILE")
    
    # Validate credentials JSON
    if ! echo "$credentials" | jq . > /dev/null 2>&1; then
        print_error "Invalid JSON in credentials file"
        exit 1
    fi
    
    # Create new secret value
    local new_secret_value
    new_secret_value=$(jq -n \
        --arg sheetId "$TEST_SHEET_ID" \
        --arg tabName "$TEST_TAB_NAME" \
        --argjson credentials "$credentials" \
        '{
            sheetId: $sheetId,
            tabName: $tabName,
            credentials: $credentials
        }')
    
    local profile_arg=""
    if [[ -n "$PROFILE" ]]; then
        profile_arg="--profile $PROFILE"
    fi
    
    # Update secret
    print_info "Updating secret with test configuration..."
    print_info "  Test Sheet ID: $TEST_SHEET_ID"
    print_info "  Test Tab Name: $TEST_TAB_NAME"
    echo ""
    
    aws secretsmanager put-secret-value \
        --secret-id "$secret_name" \
        --region "$REGION" \
        $profile_arg \
        --secret-string "$new_secret_value" \
        > /dev/null 2>&1
    
    if [[ $? -eq 0 ]]; then
        print_success "Secret updated successfully with test configuration"
        print_info "Backup saved to: $BACKUP_FILE"
        print_warning "Remember to restore the original secret after testing!"
        echo ""
        print_info "To restore, run: $0 restore"
    else
        print_error "Failed to update secret"
        return 1
    fi
}

# Invoke the SQS Message Counter Lambda using the test script
invoke_counter_lambda() {
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local test_script="${script_dir}/test-sqs-message-counter-lambda.sh"
    
    if [[ ! -f "$test_script" ]]; then
        print_error "Test script not found: $test_script"
        return 1
    fi
    
    print_info "Invoking SQS Message Counter Lambda..."
    echo ""
    
    # Build command with profile if provided
    local cmd="$test_script --stack-name $STACK_NAME --region $REGION"
    if [[ -n "$PROFILE" ]]; then
        cmd="$cmd --profile $PROFILE"
    fi
    
    # Execute the test script
    eval "$cmd"
    return $?
}

# Main function
main() {
    parse_arguments "$@"
    
    if [[ "$RESTORE_MODE" == "true" ]]; then
        restore_secret
    else
        update_secret
        
        # Invoke Lambda if requested
        if [[ "$INVOKE_LAMBDA" == "true" ]]; then
            echo ""
            print_info "=== Invoking SQS Message Counter Lambda ==="
            echo ""
            
            if invoke_counter_lambda; then
                echo ""
                print_success "✅ Lambda invocation completed"
            else
                echo ""
                print_error "❌ Lambda invocation failed"
            fi
        fi
    fi
}

main "$@"

