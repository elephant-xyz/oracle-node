#!/bin/bash

# DLQ Reprocessing Script
# Moves messages from Dead Letter Queue back to the main queue for reprocessing

set -euo pipefail

# Color output functions
print_error() { echo -e "\033[31m❌ ERROR: $1\033[0m" >&2; }
print_success() { echo -e "\033[32m✅ $1\033[0m"; }
print_warning() { echo -e "\033[33m⚠️  WARNING: $1\033[0m"; }
print_info() { echo -e "\033[34mℹ️  $1\033[0m"; }

# Default values
STACK_NAME="${STACK_NAME:-elephant-oracle-node}"
PROFILE="${AWS_PROFILE:-oracle-saman}"
BATCH_SIZE=10
MAX_MESSAGES=100
DRY_RUN=false
VERBOSE=false

show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Reprocess messages from Dead Letter Queue (DLQ) back to main queue.

OPTIONS:
    --dlq-type TYPE         DLQ type: workflow or transactions (required)
    --batch-size SIZE       Messages to process per batch (default: 10)
    --max-messages COUNT    Maximum messages to reprocess (default: 100)
    --profile PROFILE       AWS profile to use (default: oracle-3)
    --stack-name NAME       CloudFormation stack name (default: elephant-oracle-node)
    --dry-run              Show what would be done without actually moving messages
    --verbose              Enable verbose output
    --help                 Show this help message

EXAMPLES:
    # Reprocess MWAA DLQ messages (dry run)
    $0 --dlq-type mwaa --dry-run

    # Reprocess workflow DLQ messages (10 at a time, max 50)
    $0 --dlq-type workflow --batch-size 10 --max-messages 50

    # Reprocess all transactions DLQ messages
    $0 --dlq-type transactions --max-messages 1000

DLQ TYPES:
    mwaa         - MWAA Dead Letter Queue → MWAA SQS Queue
    workflow     - Workflow Dead Letter Queue → Workflow SQS Queue  
    transactions - Transactions Dead Letter Queue → Transactions SQS Queue

EOF
}

parse_arguments() {
    DLQ_TYPE=""
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --dlq-type)
                DLQ_TYPE="$2"
                shift 2
                ;;
            --batch-size)
                BATCH_SIZE="$2"
                shift 2
                ;;
            --max-messages)
                MAX_MESSAGES="$2"
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
            --verbose)
                VERBOSE=true
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

    if [[ -z "$DLQ_TYPE" ]]; then
        print_error "DLQ type is required. Use --dlq-type [workflow|transactions]"
        show_usage
        exit 1
    fi

    if [[ ! "$DLQ_TYPE" =~ ^(workflow|transactions)$ ]]; then
        print_error "Invalid DLQ type: $DLQ_TYPE. Must be one of: workflow, transactions"
        exit 1
    fi
}

get_queue_urls() {
    local dlq_type="$1"
    
    print_info "Getting queue URLs from CloudFormation stack: $STACK_NAME"
    
    case "$dlq_type" in
        workflow)
            DLQ_URL=$(aws cloudformation describe-stacks \
                --stack-name "$STACK_NAME" \
                --query "Stacks[0].Outputs[?OutputKey=='WorkflowDeadLetterQueueUrl'].OutputValue" \
                --output text \
                --profile "$PROFILE" 2>/dev/null)
            
            MAIN_URL=$(aws cloudformation describe-stacks \
                --stack-name "$STACK_NAME" \
                --query "Stacks[0].Outputs[?OutputKey=='WorkflowQueueUrl'].OutputValue" \
                --output text \
                --profile "$PROFILE" 2>/dev/null)
            ;;
        transactions)
            DLQ_URL=$(aws cloudformation describe-stacks \
                --stack-name "$STACK_NAME" \
                --query "Stacks[0].Outputs[?OutputKey=='TransactionsDeadLetterQueueUrl'].OutputValue" \
                --output text \
                --profile "$PROFILE" 2>/dev/null)
            
            MAIN_URL=$(aws cloudformation describe-stacks \
                --stack-name "$STACK_NAME" \
                --query "Stacks[0].Outputs[?OutputKey=='TransactionsSqsQueueUrl'].OutputValue" \
                --output text \
                --profile "$PROFILE" 2>/dev/null)
            ;;
    esac
    
    # Fallback: If outputs don't exist, find queues by name pattern
    if [[ -z "$DLQ_URL" || "$DLQ_URL" == "None" ]]; then
        print_warning "CloudFormation output not found, searching for queues by name pattern..."
        
        case "$dlq_type" in
            workflow)
                DLQ_URL=$(aws sqs list-queues --queue-name-prefix "$STACK_NAME-WorkflowDeadLetterQueue" --profile "$PROFILE" --output text --query 'QueueUrls[0]' 2>/dev/null)
                
                # WorkflowSqsQueue has explicit QueueName from parameter (defaults to "elephant-workflow-queue")
                # First try the default name
                MAIN_URL=$(aws sqs list-queues --queue-name-prefix "elephant-workflow-queue" --profile "$PROFILE" --output text --query 'QueueUrls[0]' 2>/dev/null)
                
                if [[ -z "$MAIN_URL" || "$MAIN_URL" == "None" ]]; then
                    # Try to get the actual WorkflowQueueName parameter value from CloudFormation
                    local workflow_queue_name
                    workflow_queue_name=$(aws cloudformation describe-stacks \
                        --stack-name "$STACK_NAME" \
                        --query "Stacks[0].Parameters[?ParameterKey=='WorkflowQueueName'].ParameterValue" \
                        --output text \
                        --profile "$PROFILE" 2>/dev/null)
                    
                    if [[ -n "$workflow_queue_name" && "$workflow_queue_name" != "None" ]]; then
                        print_info "Found WorkflowQueueName parameter: $workflow_queue_name"
                        MAIN_URL=$(aws sqs list-queues --queue-name-prefix "$workflow_queue_name" --profile "$PROFILE" --output text --query 'QueueUrls[0]' 2>/dev/null)
                    fi
                fi
                
                if [[ -z "$MAIN_URL" || "$MAIN_URL" == "None" ]]; then
                    # Final fallback to stack-based pattern (shouldn't be needed, but just in case)
                    MAIN_URL=$(aws sqs list-queues --queue-name-prefix "$STACK_NAME-WorkflowSqsQueue" --profile "$PROFILE" --output text --query 'QueueUrls[0]' 2>/dev/null)
                fi
                ;;
            transactions)
                DLQ_URL=$(aws sqs list-queues --queue-name-prefix "$STACK_NAME-TransactionsDeadLetterQueue" --profile "$PROFILE" --output text --query 'QueueUrls[0]' 2>/dev/null)
                MAIN_URL=$(aws sqs list-queues --queue-name-prefix "$STACK_NAME-TransactionsSqsQueue" --profile "$PROFILE" --output text --query 'QueueUrls[0]' 2>/dev/null)
                ;;
        esac
    fi
    
    if [[ -z "$DLQ_URL" || "$DLQ_URL" == "None" ]]; then
        print_error "Could not find DLQ URL for type: $dlq_type"
        print_info "Available queues:"
        aws sqs list-queues --profile "$PROFILE" --output table 2>/dev/null || echo "  (Unable to list queues)"
        exit 1
    fi
    
    if [[ -z "$MAIN_URL" || "$MAIN_URL" == "None" ]]; then
        print_error "Could not find main queue URL for type: $dlq_type"
        print_info "Available queues:"
        aws sqs list-queues --profile "$PROFILE" --output table 2>/dev/null || echo "  (Unable to list queues)"
        exit 1
    fi
}

check_dlq_messages() {
    print_info "Checking DLQ message count..."
    
    local attributes
    attributes=$(aws sqs get-queue-attributes \
        --queue-url "$DLQ_URL" \
        --attribute-names ApproximateNumberOfMessages \
        --profile "$PROFILE" \
        --output json)
    
    local message_count
    message_count=$(echo "$attributes" | jq -r '.Attributes.ApproximateNumberOfMessages // "0"')
    
    print_info "DLQ contains approximately $message_count messages"
    
    if [[ "$message_count" -eq 0 ]]; then
        print_warning "No messages in DLQ to reprocess"
        exit 0
    fi
    
    if [[ "$message_count" -gt "$MAX_MESSAGES" ]]; then
        print_warning "DLQ has $message_count messages, but MAX_MESSAGES is set to $MAX_MESSAGES"
        print_info "Only the first $MAX_MESSAGES messages will be processed"
    fi
}

reprocess_messages() {
    local processed=0
    local batch_num=1
    
    print_info "Starting DLQ reprocessing..."
    print_info "DLQ URL: $DLQ_URL"
    print_info "Target URL: $MAIN_URL"
    print_info "Batch size: $BATCH_SIZE"
    print_info "Max messages: $MAX_MESSAGES"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        print_warning "DRY RUN MODE - No messages will actually be moved"
    fi
    
    while [[ $processed -lt $MAX_MESSAGES ]]; do
        local remaining=$((MAX_MESSAGES - processed))
        local current_batch_size=$((remaining < BATCH_SIZE ? remaining : BATCH_SIZE))
        
        print_info "Processing batch $batch_num (requesting $current_batch_size messages)..."
        
        # Receive messages from DLQ
        local messages
        messages=$(aws sqs receive-message \
            --queue-url "$DLQ_URL" \
            --max-number-of-messages "$current_batch_size" \
            --profile "$PROFILE" \
            --output json 2>/dev/null || echo '{}')
        
        local message_array
        message_array=$(echo "$messages" | jq -r '.Messages // []')
        
        if [[ "$message_array" == "[]" ]]; then
            print_info "No more messages in DLQ"
            break
        fi
        
        local message_count
        message_count=$(echo "$message_array" | jq 'length')
        
        if [[ "$VERBOSE" == "true" ]]; then
            print_info "Received $message_count messages from DLQ"
        fi
        
        # Process each message
        local batch_processed=0
        echo "$message_array" | jq -c '.[]' | while read -r message; do
            local body receipt_handle
            body=$(echo "$message" | jq -r '.Body')
            receipt_handle=$(echo "$message" | jq -r '.ReceiptHandle')
            
            if [[ "$VERBOSE" == "true" ]]; then
                print_info "Processing message: ${body:0:100}..."
            fi
            
            if [[ "$DRY_RUN" == "false" ]]; then
                # Send message to main queue
                aws sqs send-message \
                    --queue-url "$MAIN_URL" \
                    --message-body "$body" \
                    --profile "$PROFILE" \
                    --output json > /dev/null
                
                # Delete message from DLQ
                aws sqs delete-message \
                    --queue-url "$DLQ_URL" \
                    --receipt-handle "$receipt_handle" \
                    --profile "$PROFILE" \
                    --output json > /dev/null
            fi
            
            ((batch_processed++))
        done
        
        processed=$((processed + message_count))
        batch_num=$((batch_num + 1))
        
        print_success "Batch complete: $message_count messages reprocessed (total: $processed)"
        
        # Small delay between batches (removed for speed)
        # sleep 1
    done
    
    if [[ "$DRY_RUN" == "true" ]]; then
        print_info "DRY RUN COMPLETE: Would have reprocessed $processed messages"
    else
        print_success "REPROCESSING COMPLETE: $processed messages moved from DLQ to main queue"
    fi
}

main() {
    parse_arguments "$@"
    
    print_info "DLQ Reprocessing Tool"
    print_info "DLQ Type: $DLQ_TYPE"
    print_info "AWS Profile: $PROFILE"
    print_info "Stack Name: $STACK_NAME"
    
    get_queue_urls "$DLQ_TYPE"
    check_dlq_messages
    reprocess_messages
    
    print_success "DLQ reprocessing completed successfully!"
}

main "$@"
