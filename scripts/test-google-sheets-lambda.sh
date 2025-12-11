#!/bin/bash

# Script to invoke the SQS Message Counter Lambda (which updates Google Sheets)
# This is a simple wrapper around test-sqs-message-counter-lambda.sh

set -euo pipefail

# Color output functions
print_error() { echo -e "\033[31m❌ ERROR: $1\033[0m" >&2; }
print_success() { echo -e "\033[32m✅ $1\033[0m"; }
print_info() { echo -e "\033[34mℹ️  $1\033[0m"; }

# Default values
STACK_NAME="${STACK_NAME:-elephant-oracle-node}"
REGION="${AWS_REGION:-us-east-1}"
PROFILE="${AWS_PROFILE:-}"

show_usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Invoke the SQS Message Counter Lambda (which updates Google Sheets).

OPTIONS:
    --stack-name NAME       CloudFormation stack name (default: elephant-oracle-node)
    --region REGION         AWS region (default: us-east-1)
    --profile PROFILE       AWS profile to use
    --help                  Show this help message

EXAMPLES:
    # Invoke Lambda with default stack name
    $0

    # Invoke Lambda with custom stack name and profile
    $0 --stack-name my-stack --profile my-profile

EOF
}

parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
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
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
}

# Main function
main() {
    parse_arguments "$@"
    
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local test_script="${script_dir}/test-sqs-message-counter-lambda.sh"
    
    if [[ ! -f "$test_script" ]]; then
        print_error "Test script not found: $test_script"
        exit 1
    fi
    
    print_info "Invoking SQS Message Counter Lambda..."
    print_info "Stack Name: $STACK_NAME"
    print_info "Region: $REGION"
    if [[ -n "$PROFILE" ]]; then
        print_info "Profile: $PROFILE"
    fi
    echo ""
    
    # Build command with profile if provided
    local cmd="$test_script --stack-name $STACK_NAME --region $REGION"
    if [[ -n "$PROFILE" ]]; then
        cmd="$cmd --profile $PROFILE"
    fi
    
    # Execute the test script
    eval "$cmd"
}

main "$@"
