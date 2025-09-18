#!/bin/bash

# Fast DLQ Reprocessing Script - Parallel Processing
# WARNING: This processes messages much faster but with less control

set -euo pipefail

# Color output functions
print_error() { echo -e "\033[31m❌ ERROR: $1\033[0m" >&2; }
print_success() { echo -e "\033[32m✅ $1\033[0m"; }
print_warning() { echo -e "\033[33m⚠️  WARNING: $1\033[0m"; }
print_info() { echo -e "\033[34mℹ️  $1\033[0m"; }

# Default values
STACK_NAME="${STACK_NAME:-elephant-oracle-node}"
PROFILE="${AWS_PROFILE:-oracle-saman}"
MAX_MESSAGES=1000
PARALLEL_WORKERS=5
DRY_RUN=false

show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Fast parallel reprocessing of messages from Transactions DLQ.

OPTIONS:
    --max-messages COUNT    Maximum messages to reprocess (default: 1000)
    --workers COUNT         Number of parallel workers (default: 5)
    --profile PROFILE       AWS profile to use (default: oracle-3)
    --stack-name NAME       CloudFormation stack name (default: elephant-oracle-node)
    --dry-run              Show what would be done without actually moving messages
    --help                 Show this help message

EXAMPLES:
    # Fast reprocess 5000 messages with 10 workers
    $0 --max-messages 5000 --workers 10

    # Conservative test
    $0 --max-messages 100 --workers 2 --dry-run

WARNING: This script processes messages much faster but with less granular control.
Use the regular reprocess-dlq.sh for more controlled processing.

EOF
}

parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --max-messages)
                MAX_MESSAGES="$2"
                shift 2
                ;;
            --workers)
                PARALLEL_WORKERS="$2"
                shift 2
                ;;
            --profile)
                PROFILE="$2"
                shift 2
                ;;
            --stack-name)
                STACK_NAME="$2"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=true
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
}

get_queue_urls() {
    print_info "Getting Transactions queue URLs..."
    
    DLQ_URL=$(aws sqs list-queues --queue-name-prefix "$STACK_NAME-TransactionsDeadLetterQueue" --profile "$PROFILE" --output text --query 'QueueUrls[0]' 2>/dev/null)
    MAIN_URL=$(aws sqs list-queues --queue-name-prefix "$STACK_NAME-TransactionsSqsQueue" --profile "$PROFILE" --output text --query 'QueueUrls[0]' 2>/dev/null)
    
    if [[ -z "$DLQ_URL" || "$DLQ_URL" == "None" ]]; then
        print_error "Could not find Transactions DLQ"
        exit 1
    fi
    
    if [[ -z "$MAIN_URL" || "$MAIN_URL" == "None" ]]; then
        print_error "Could not find Transactions main queue"
        exit 1
    fi
    
    print_info "DLQ: $DLQ_URL"
    print_info "Target: $MAIN_URL"
}

process_worker() {
    local worker_id=$1
    local messages_per_worker=$((MAX_MESSAGES / PARALLEL_WORKERS))
    local processed=0
    
    print_info "Worker $worker_id starting (target: $messages_per_worker messages)"
    
    while [[ $processed -lt $messages_per_worker ]]; do
        # Get 10 messages (SQS max)
        local messages
        messages=$(aws sqs receive-message \
            --queue-url "$DLQ_URL" \
            --max-number-of-messages 10 \
            --profile "$PROFILE" \
            --output json 2>/dev/null || echo '{}')
        
        local message_array
        message_array=$(echo "$messages" | jq -r '.Messages // []')
        
        if [[ "$message_array" == "[]" ]]; then
            print_info "Worker $worker_id: No more messages in DLQ"
            break
        fi
        
        local message_count
        message_count=$(echo "$message_array" | jq 'length')
        
        if [[ "$DRY_RUN" == "false" ]]; then
            # Process each message quickly
            echo "$message_array" | jq -c '.[]' | while read -r message; do
                local body receipt_handle
                body=$(echo "$message" | jq -r '.Body')
                receipt_handle=$(echo "$message" | jq -r '.ReceiptHandle')
                
                # Send to main queue
                aws sqs send-message \
                    --queue-url "$MAIN_URL" \
                    --message-body "$body" \
                    --profile "$PROFILE" \
                    --output json > /dev/null
                
                # Delete from DLQ
                aws sqs delete-message \
                    --queue-url "$DLQ_URL" \
                    --receipt-handle "$receipt_handle" \
                    --profile "$PROFILE" \
                    --output json > /dev/null
            done
        fi
        
        processed=$((processed + message_count))
        print_info "Worker $worker_id: Processed $message_count messages (total: $processed)"
    done
    
    print_success "Worker $worker_id completed: $processed messages"
}

main() {
    parse_arguments "$@"
    
    print_info "🚀 Fast DLQ Reprocessing Tool"
    print_info "Max Messages: $MAX_MESSAGES"
    print_info "Parallel Workers: $PARALLEL_WORKERS"
    print_info "AWS Profile: $PROFILE"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        print_warning "DRY RUN MODE - No messages will actually be moved"
    fi
    
    get_queue_urls
    
    print_info "Starting $PARALLEL_WORKERS parallel workers..."
    
    # Start parallel workers
    for ((i=1; i<=PARALLEL_WORKERS; i++)); do
        process_worker $i &
    done
    
    # Wait for all workers to complete
    wait
    
    print_success "🎉 All workers completed!"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        print_info "DRY RUN: Would have processed up to $MAX_MESSAGES messages"
    else
        print_success "FAST REPROCESSING COMPLETE"
    fi
}

main "$@"
