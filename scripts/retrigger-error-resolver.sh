#!/bin/bash
# Script to retrigger error-resolver Lambda for maybeSolved executions
# This updates ExecutionErrorLink items to generate DynamoDB stream events
# Usage: ./retrigger-error-resolver.sh [execution-id] [--all] [--dry-run]

set -e

REGION="${AWS_REGION:-us-east-1}"

# Try to get table name from environment or CloudFormation
TABLE_NAME="${ERRORS_TABLE_NAME}"

if [ -z "$TABLE_NAME" ]; then
  echo "ERRORS_TABLE_NAME not set, attempting to discover from CloudFormation..."
  
  # Get all stack names as an array
  STACK_NAMES=$(aws cloudformation list-stacks \
    --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE \
    --query "StackSummaries[?contains(StackName, 'oracle') || contains(StackName, 'prepare')].StackName" \
    --region "$REGION" \
    --output text 2>/dev/null | tr '\t' '\n' | tr ' ' '\n' | grep -v '^$')
  
  # Prefer 'elephant-oracle-node' if it exists, otherwise use first stack
  STACK_NAME=$(echo "$STACK_NAMES" | grep -E "^elephant-oracle-node$" | head -1)
  if [ -z "$STACK_NAME" ]; then
    STACK_NAME=$(echo "$STACK_NAMES" | head -1)
  fi
  
  if [ -n "$STACK_NAME" ]; then
    echo "Found stack: $STACK_NAME"
    TABLE_NAME=$(aws cloudformation describe-stacks \
      --stack-name "$STACK_NAME" \
      --query "Stacks[0].Outputs[?OutputKey=='ErrorsTableName'].OutputValue" \
      --region "$REGION" \
      --output text 2>/dev/null | tr -d '\t' | tr -d ' ')
    
    if [ -z "$TABLE_NAME" ] || [ "$TABLE_NAME" = "None" ] || [ "$TABLE_NAME" = "" ]; then
      echo "Stack $STACK_NAME does not have ErrorsTableName output, trying other stacks..."
      # Try other stacks
      for alt_stack in $(echo "$STACK_NAMES" | grep -v "^$STACK_NAME$"); do
        if [ -z "$alt_stack" ]; then
          continue
        fi
        alt_table=$(aws cloudformation describe-stacks \
          --stack-name "$alt_stack" \
          --query "Stacks[0].Outputs[?OutputKey=='ErrorsTableName'].OutputValue" \
          --region "$REGION" \
          --output text 2>/dev/null | tr -d '\t' | tr -d ' ')
        if [ -n "$alt_table" ] && [ "$alt_table" != "None" ] && [ "$alt_table" != "" ]; then
          TABLE_NAME="$alt_table"
          echo "Found table in stack: $alt_stack"
          break
        fi
      done
    fi
  fi
fi

if [ -z "$TABLE_NAME" ]; then
  echo "ERROR: Could not determine ERRORS_TABLE_NAME"
  echo "Please set ERRORS_TABLE_NAME environment variable"
  exit 1
fi

echo "Using table: $TABLE_NAME"
echo "Region: $REGION"
echo ""

DRY_RUN=false
PROCESS_ALL=false
EXECUTION_ID=""

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --all)
      PROCESS_ALL=true
      shift
      ;;
    *)
      if [ -z "$EXECUTION_ID" ]; then
        EXECUTION_ID="$1"
      else
        echo "Unknown argument: $1"
        exit 1
      fi
      shift
      ;;
  esac
done

if [ "$DRY_RUN" = true ]; then
  echo "=== DRY RUN MODE - No changes will be made ==="
  echo ""
fi

# Function to retrigger error-resolver for a single execution
retrigger_execution() {
  local exec_id="$1"
  local pk="EXECUTION#${exec_id}"
  local sk="EXECUTION#${exec_id}"
  
  echo "Processing execution: $exec_id"
  
  # Check current status
  local current_status=$(aws dynamodb get-item \
    --table-name "$TABLE_NAME" \
    --key "{\"PK\": {\"S\": \"$pk\"}, \"SK\": {\"S\": \"$sk\"}}" \
    --region "$REGION" \
    --output json 2>/dev/null | jq -r '.Item.status.S // "NOT_FOUND"')
  
  if [ "$current_status" = "NOT_FOUND" ]; then
    echo "  ⚠️  Execution not found, skipping..."
    return
  fi
  
  if [ "$current_status" != "maybeSolved" ]; then
    echo "  ⚠️  Execution has status '$current_status', not 'maybeSolved'. Skipping..."
    return
  fi
  
  echo "  Current status: $current_status"
  
  # Query all ExecutionErrorLink items for this execution
  echo "  Finding ExecutionErrorLink items..."
  
  local error_links=$(aws dynamodb query \
    --table-name "$TABLE_NAME" \
    --key-condition-expression "PK = :pk AND begins_with(SK, :skPrefix)" \
    --filter-expression "#entityType = :entityType AND #status = :maybeSolved" \
    --expression-attribute-names '{"#entityType": "entityType", "#status": "status"}' \
    --expression-attribute-values "{\":pk\": {\"S\": \"$pk\"}, \":skPrefix\": {\"S\": \"ERROR#\"}, \":entityType\": {\"S\": \"ExecutionError\"}, \":maybeSolved\": {\"S\": \"maybeSolved\"}}" \
    --region "$REGION" \
    --output json 2>/dev/null)
  
  local link_count=$(echo "$error_links" | jq '.Items | length')
  echo "  Found $link_count ExecutionErrorLink item(s) with status='maybeSolved'"
  
  if [ "$link_count" -eq 0 ]; then
    echo "  ⚠️  No ExecutionErrorLink items found, skipping..."
    return
  fi
  
  if [ "$DRY_RUN" = true ]; then
    echo "  [DRY RUN] Would update $link_count ExecutionErrorLink item(s) to trigger stream events"
    return
  fi
  
  # Update each ExecutionErrorLink item to trigger a DynamoDB stream event
  # We cycle the status: maybeSolved -> failed -> maybeSolved
  # This ensures the error-resolver Lambda picks it up (it watches for failed -> maybeSolved transitions)
  local updated=0
  echo "$error_links" | jq -r '.Items[] | "\(.PK.S)|\(.SK.S)"' | while IFS='|' read -r link_pk link_sk; do
    local new_timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    
    # Step 1: Change status from maybeSolved to failed (triggers stream event)
    aws dynamodb update-item \
      --table-name "$TABLE_NAME" \
      --key "{\"PK\": {\"S\": \"$link_pk\"}, \"SK\": {\"S\": \"$link_sk\"}}" \
      --update-expression "SET #status = :failed, #updatedAt = :updatedAt" \
      --expression-attribute-names '{"#status": "status", "#updatedAt": "updatedAt"}' \
      --expression-attribute-values "{\":failed\": {\"S\": \"failed\"}, \":updatedAt\": {\"S\": \"$new_timestamp\"}}" \
      --region "$REGION" \
      --output json > /dev/null 2>&1
    
    # Small delay to ensure stream event is processed
    sleep 0.1
    
    # Step 2: Change status back to maybeSolved (triggers another stream event that error-resolver will process)
    new_timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    aws dynamodb update-item \
      --table-name "$TABLE_NAME" \
      --key "{\"PK\": {\"S\": \"$link_pk\"}, \"SK\": {\"S\": \"$link_sk\"}}" \
      --update-expression "SET #status = :maybeSolved, #updatedAt = :updatedAt" \
      --expression-attribute-names '{"#status": "status", "#updatedAt": "updatedAt"}' \
      --expression-attribute-values "{\":maybeSolved\": {\"S\": \"maybeSolved\"}, \":updatedAt\": {\"S\": \"$new_timestamp\"}}" \
      --region "$REGION" \
      --output json > /dev/null 2>&1
    
    updated=$((updated + 1))
  done
  
  echo "  ✓ Updated $link_count ExecutionErrorLink item(s) - status cycled to trigger stream events"
  echo "  ✓ Error-resolver Lambda should process these executions"
  echo ""
}

# Main logic
if [ -n "$EXECUTION_ID" ]; then
  # Retrigger specific execution
  retrigger_execution "$EXECUTION_ID"
elif [ "$PROCESS_ALL" = true ]; then
  # Retrigger all maybeSolved executions
  echo "=== Retriggering error-resolver for ALL executions with status='maybeSolved' ==="
  echo ""
  
  if [ "$DRY_RUN" = false ]; then
    read -p "Are you sure you want to retrigger error-resolver for ALL maybeSolved executions? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
      echo "Aborted."
      exit 0
    fi
  fi
  
  # Query all maybeSolved executions
  echo "Finding all executions with status='maybeSolved'..."
  
  cat > /tmp/query-maybesolved.json << 'EOF'
{
  ":pk": {"S": "METRIC#ERRORCOUNT"},
  ":status": {"S": "maybeSolved"},
  ":entityType": {"S": "FailedExecution"}
}
EOF
  
  # Use pagination to get all maybeSolved executions
  last_key=""
  total=0
  page=1
  
  while true; do
    query_cmd="aws dynamodb query \
      --table-name \"$TABLE_NAME\" \
      --index-name \"ExecutionErrorCountIndex\" \
      --key-condition-expression \"GS3PK = :pk\" \
      --filter-expression \"#status = :status AND #entityType = :entityType\" \
      --expression-attribute-names '{\"#status\": \"status\", \"#entityType\": \"entityType\"}' \
      --expression-attribute-values file:///tmp/query-maybesolved.json \
      --region \"$REGION\" \
      --output json"
    
    if [ -n "$last_key" ] && [ "$last_key" != "null" ]; then
      echo "$last_key" > /tmp/last-key.json
      query_cmd="$query_cmd --exclusive-start-key file:///tmp/last-key.json"
    fi
    
    query_result=$(eval "$query_cmd" 2>/dev/null)
    
    items=$(echo "$query_result" | jq -r '.Items[]?.executionId.S // empty' | grep -v '^$')
    count=$(echo "$items" | wc -l | tr -d ' ')
    
    if [ "$count" -eq 0 ]; then
      break
    fi
    
    echo "Page $page: Found $count execution(s)"
    total=$((total + count))
    
    echo "$items" | while read -r exec_id; do
      if [ -n "$exec_id" ]; then
        retrigger_execution "$exec_id"
      fi
    done
    
    last_key=$(echo "$query_result" | jq -r '.LastEvaluatedKey // empty')
    if [ -z "$last_key" ] || [ "$last_key" = "null" ] || [ "$last_key" = "" ]; then
      break
    fi
    
    page=$((page + 1))
  done
  
  echo "=== Summary ==="
  echo "Total executions processed: $total"
else
  # Interactive mode - list and let user choose
  echo "=== Executions with status='maybeSolved' ==="
  echo ""
  
  cat > /tmp/query-maybesolved.json << 'EOF'
{
  ":pk": {"S": "METRIC#ERRORCOUNT"},
  ":status": {"S": "maybeSolved"},
  ":entityType": {"S": "FailedExecution"}
}
EOF
  
  # Get first page of results
  query_result=$(aws dynamodb query \
    --table-name "$TABLE_NAME" \
    --index-name "ExecutionErrorCountIndex" \
    --key-condition-expression "GS3PK = :pk" \
    --filter-expression "#status = :status AND #entityType = :entityType" \
    --expression-attribute-names '{"#status": "status", "#entityType": "entityType"}' \
    --expression-attribute-values file:///tmp/query-maybesolved.json \
    --region "$REGION" \
    --limit 20 \
    --output json 2>/dev/null)
  
  count=$(echo "$query_result" | jq '.Items | length')
  
  if [ "$count" -eq 0 ]; then
    echo "No executions with status='maybeSolved' found."
    exit 0
  fi
  
  echo "Found $count execution(s) (showing first 20):"
  echo ""
  echo "$query_result" | jq -r '.Items[] | "\(.executionId.S) - County: \(.county.S // "N/A"), Errors: \(.openErrorCount.N // "N/A")"'
  echo ""
  echo "Usage:"
  echo "  ./retrigger-error-resolver.sh <execution-id>  # Retrigger specific execution"
  echo "  ./retrigger-error-resolver.sh --all           # Retrigger all maybeSolved executions"
  echo "  ./retrigger-error-resolver.sh --all --dry-run # Dry run (no changes)"
fi

