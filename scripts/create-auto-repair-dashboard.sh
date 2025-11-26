#!/bin/bash
# Create CloudWatch Dashboard for Auto-Repair, Execution Restart, and Workflow Execution Metrics
# Usage: ./create-auto-repair-dashboard.sh [dashboard-name] [region] [stack-name] [step-function-name] [dynamodb-table-name]

set -e

DASHBOARD_NAME="${1:-ErrorRecovery-Metrics}"
REGION="${2:-${AWS_REGION:-us-east-1}}"
STACK_NAME="${3:-${AWS_STACK_NAME:-elephant-oracle-node}}"
STEP_FUNCTION_NAME="${4:-}"
DYNAMODB_TABLE_NAME="${5:-}"

AUTOREPAIR_NAMESPACE="${AUTOREPAIR_METRIC_NAMESPACE:-AutoRepair}"
EXECUTIONRESTART_NAMESPACE="${EXECUTIONRESTART_METRIC_NAMESPACE:-ExecutionRestart}"
ELEPHANTWORKFLOW_NAMESPACE="${ELEPHANTWORKFLOW_METRIC_NAMESPACE:-ElephantWorkflow}"

# Try to discover Step Function ARN from CloudFormation
STEP_FUNCTION_ARN=""
if [ -z "$STEP_FUNCTION_NAME" ]; then
  echo "Discovering Step Function ARN from CloudFormation stack: ${STACK_NAME}..."
  STEP_FUNCTION_ARN=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --query "Stacks[0].Outputs[?OutputKey=='ElephantExpressStateMachineArn'].OutputValue" \
    --output text 2>/dev/null || echo "")
  
  if [ -z "$STEP_FUNCTION_ARN" ] || [ "$STEP_FUNCTION_ARN" == "None" ]; then
    echo "Warning: Could not discover Step Function ARN from CloudFormation. Trying to find by name pattern..."
    # Try to find Step Function by listing all state machines and matching name pattern
    ACCOUNT_ID=$(aws sts get-caller-identity --region "${REGION}" --query Account --output text 2>/dev/null || echo "")
    if [ -n "$ACCOUNT_ID" ]; then
      # List all state machines and find one matching "ElephantExpress" pattern
      STEP_FUNCTION_ARN=$(aws stepfunctions list-state-machines \
        --region "${REGION}" \
        --output json 2>/dev/null | \
        jq -r --arg account "$ACCOUNT_ID" --arg region "$REGION" \
          '.stateMachines[] | select(.name | contains("ElephantExpress") or contains("Express")) | "arn:aws:states:\($region):\($account):stateMachine:\(.name)"' | \
        head -1)
      
      if [ -z "$STEP_FUNCTION_ARN" ]; then
        echo "Error: Could not find Step Function in account ${ACCOUNT_ID}. Please provide STEP_FUNCTION_NAME as 4th argument."
        exit 1
      else
        STEP_FUNCTION_NAME=$(echo "$STEP_FUNCTION_ARN" | awk -F: '{print $NF}')
        echo "Found Step Function: ${STEP_FUNCTION_NAME} (${STEP_FUNCTION_ARN})"
      fi
    else
      echo "Error: Could not determine AWS account ID. Please provide STEP_FUNCTION_NAME as 4th argument."
      exit 1
    fi
  else
    # Extract name from ARN
    STEP_FUNCTION_NAME=$(echo "$STEP_FUNCTION_ARN" | awk -F: '{print $NF}')
  fi
else
  # If name provided, try to get ARN from CloudFormation first, then construct it
  STEP_FUNCTION_ARN=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --query "Stacks[0].Outputs[?OutputKey=='ElephantExpressStateMachineArn'].OutputValue" \
    --output text 2>/dev/null || echo "")
  
  if [ -z "$STEP_FUNCTION_ARN" ] || [ "$STEP_FUNCTION_ARN" == "None" ]; then
    # Construct ARN from account ID and provided name
    ACCOUNT_ID=$(aws sts get-caller-identity --region "${REGION}" --query Account --output text 2>/dev/null || echo "")
    if [ -z "$ACCOUNT_ID" ]; then
      echo "Error: Could not determine AWS account ID."
      exit 1
    fi
    STEP_FUNCTION_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${STEP_FUNCTION_NAME}"
  fi
fi

# Try to discover DynamoDB table name from CloudFormation if not provided
if [ -z "$DYNAMODB_TABLE_NAME" ]; then
  echo "Discovering DynamoDB table name from CloudFormation stack: ${STACK_NAME}..."
  DYNAMODB_TABLE_NAME=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --query "Stacks[0].Outputs[?OutputKey=='ErrorsTableName'].OutputValue" \
    --output text 2>/dev/null || echo "")
  
  if [ -z "$DYNAMODB_TABLE_NAME" ] || [ "$DYNAMODB_TABLE_NAME" == "None" ]; then
    echo "Warning: Could not discover DynamoDB table name. Using default: ${STACK_NAME}-ErrorsTable"
    DYNAMODB_TABLE_NAME="${STACK_NAME}-ErrorsTable"
  fi
fi

echo "Creating CloudWatch Dashboard: ${DASHBOARD_NAME}"
echo "Region: ${REGION}"
echo "AutoRepair Namespace: ${AUTOREPAIR_NAMESPACE}"
echo "ExecutionRestart Namespace: ${EXECUTIONRESTART_NAMESPACE}"
echo "ElephantWorkflow Namespace: ${ELEPHANTWORKFLOW_NAMESPACE}"
echo "Step Function Name: ${STEP_FUNCTION_NAME}"
echo "Step Function ARN: ${STEP_FUNCTION_ARN}"
echo "DynamoDB Table Name: ${DYNAMODB_TABLE_NAME}"
echo ""

# Discover all counties and build metric arrays
echo "Discovering counties and building metric queries..."

# Get all unique counties from AutoRepairWorkflowSuccess metrics
COUNTIES=$(aws cloudwatch list-metrics --namespace "${AUTOREPAIR_NAMESPACE}" --metric-name "AutoRepairWorkflowSuccess" --region "${REGION}" --output json 2>/dev/null | \
  jq -r '.Metrics[].Dimensions[]? | select(.Name == "County") | .Value' | sort -u)

# Build Success metrics array (one per county with IDs for aggregation, hidden)
SUCCESS_METRICS="[]"
SUCCESS_IDS_ARRAY="[]"
METRIC_ID=1
for COUNTY in $COUNTIES; do
  ID="m${METRIC_ID}"
  SUCCESS_IDS_ARRAY=$(echo "$SUCCESS_IDS_ARRAY" | jq --arg id "$ID" '. + [$id]')
  SUCCESS_METRICS=$(echo "$SUCCESS_METRICS" | jq --arg ns "${AUTOREPAIR_NAMESPACE}" --arg county "$COUNTY" --arg id "$ID" \
    '. + [["\($ns)", "AutoRepairWorkflowSuccess", "County", $county, {"stat": "Sum", "id": $id, "visible": false}]]')
  METRIC_ID=$((METRIC_ID + 1))
done

# Build Failure metrics array (SVL only, include all dimensions as they exist, with IDs)
FAILURE_METRICS_RAW=$(aws cloudwatch list-metrics \
  --namespace "${AUTOREPAIR_NAMESPACE}" \
  --metric-name "AutoRepairWorkflowFailure" \
  --region "${REGION}" \
  --output json 2>/dev/null | \
  jq '[.Metrics[] | select(.Dimensions[]? | select(.Name == "ErrorType" and .Value == "SVL"))]')

FAILURE_COUNT=$(echo "$FAILURE_METRICS_RAW" | jq 'length')
FAILURE_METRICS=$(echo "$FAILURE_METRICS_RAW" | \
  jq --arg ns "${AUTOREPAIR_NAMESPACE}" --argjson start_id $METRIC_ID '
    to_entries |
    map(
      ["\($ns)", "AutoRepairWorkflowFailure"] + 
      (.value.Dimensions | map(.Name, .Value) | flatten) + 
      [{"stat": "Sum", "id": "m\($start_id + .key)", "visible": false}]
    )
  ')

FAILURE_IDS_ARRAY=$(echo "$FAILURE_METRICS_RAW" | \
  jq --argjson start_id $METRIC_ID '[to_entries | .[] | "m\($start_id + .key)"]')

METRIC_ID=$((METRIC_ID + FAILURE_COUNT))

# Build aggregated math expressions with comma-separated IDs
SUCCESS_IDS_STR=$(echo "$SUCCESS_IDS_ARRAY" | jq -r 'join(", ")')
FAILURE_IDS_STR=$(echo "$FAILURE_IDS_ARRAY" | jq -r 'join(", ")')

# Only create expressions if there are metrics to aggregate
COMBINED_AUTOREPAIR="[]"
if [ -n "$SUCCESS_IDS_STR" ] || [ -n "$FAILURE_IDS_STR" ]; then
  if [ -n "$SUCCESS_IDS_STR" ]; then
    SUCCESS_EXPR="SUM([${SUCCESS_IDS_STR}])"
  else
    SUCCESS_EXPR=""
  fi
  if [ -n "$FAILURE_IDS_STR" ]; then
    FAILURE_EXPR="SUM([${FAILURE_IDS_STR}])"
  else
    FAILURE_EXPR=""
  fi

  # Build aggregated expressions only (no individual county metrics)
  # First, we need to build the individual metrics with IDs (hidden) for the math expressions to reference
  ALL_AUTOREPAIR_METRICS=$(echo "$SUCCESS_METRICS $FAILURE_METRICS" | jq -s 'add')

  # Create math expressions that reference the individual metrics
  COMBINED_AUTOREPAIR=$(echo '[]' | jq --arg success_expr "$SUCCESS_EXPR" --arg failure_expr "$FAILURE_EXPR" \
    '. + 
    (if $success_expr != "" then [[{"expression": $success_expr, "label": "Success", "color": "#2ca02c"}]] else [] end) +
    (if $failure_expr != "" then [[{"expression": $failure_expr, "label": "Failure", "color": "#d62728"}]] else [] end)')
else
  # No metrics at all, use empty array
  ALL_AUTOREPAIR_METRICS="[]"
fi

# Get all unique counties from ExecutionRestartSuccess metrics
EXECUTIONRESTART_COUNTIES=$(aws cloudwatch list-metrics --namespace "${EXECUTIONRESTART_NAMESPACE}" --metric-name "ExecutionRestartSuccess" --region "${REGION}" --output json 2>/dev/null | \
  jq -r '.Metrics[].Dimensions[]? | select(.Name == "County") | .Value' | sort -u)

# Build ExecutionRestart Success metrics array (with IDs for aggregation, hidden)
EXECUTIONRESTART_SUCCESS_METRICS="[]"
EXECUTIONRESTART_SUCCESS_IDS_ARRAY="[]"
EXECUTIONRESTART_METRIC_ID=100
for COUNTY in $EXECUTIONRESTART_COUNTIES; do
  ID="m${EXECUTIONRESTART_METRIC_ID}"
  EXECUTIONRESTART_SUCCESS_IDS_ARRAY=$(echo "$EXECUTIONRESTART_SUCCESS_IDS_ARRAY" | jq --arg id "$ID" '. + [$id]')
  EXECUTIONRESTART_SUCCESS_METRICS=$(echo "$EXECUTIONRESTART_SUCCESS_METRICS" | jq --arg ns "${EXECUTIONRESTART_NAMESPACE}" --arg county "$COUNTY" --arg id "$ID" \
    '. + [["\($ns)", "ExecutionRestartSuccess", "County", $county, {"stat": "Sum", "id": $id, "visible": false}]]')
  EXECUTIONRESTART_METRIC_ID=$((EXECUTIONRESTART_METRIC_ID + 1))
done

# Build ExecutionRestart Failure metrics array (if any exist, with IDs)
EXECUTIONRESTART_FAILURE_METRICS="[]"
EXECUTIONRESTART_FAILURE_IDS_ARRAY="[]"
EXECUTIONRESTART_FAILURE_COUNTIES=$(aws cloudwatch list-metrics --namespace "${EXECUTIONRESTART_NAMESPACE}" --metric-name "ExecutionRestartFailure" --region "${REGION}" --output json 2>/dev/null | \
  jq -r '.Metrics[].Dimensions[]? | select(.Name == "County") | .Value' | sort -u)
for COUNTY in $EXECUTIONRESTART_FAILURE_COUNTIES; do
  ID="m${EXECUTIONRESTART_METRIC_ID}"
  EXECUTIONRESTART_FAILURE_IDS_ARRAY=$(echo "$EXECUTIONRESTART_FAILURE_IDS_ARRAY" | jq --arg id "$ID" '. + [$id]')
  EXECUTIONRESTART_FAILURE_METRICS=$(echo "$EXECUTIONRESTART_FAILURE_METRICS" | jq --arg ns "${EXECUTIONRESTART_NAMESPACE}" --arg county "$COUNTY" --arg id "$ID" \
    '. + [["\($ns)", "ExecutionRestartFailure", "County", $county, {"stat": "Sum", "id": $id, "visible": false}]]')
  EXECUTIONRESTART_METRIC_ID=$((EXECUTIONRESTART_METRIC_ID + 1))
done

# Build aggregated math expressions for ExecutionRestart with comma-separated IDs
EXECUTIONRESTART_SUCCESS_IDS_STR=$(echo "$EXECUTIONRESTART_SUCCESS_IDS_ARRAY" | jq -r 'join(", ")')
EXECUTIONRESTART_FAILURE_IDS_STR=$(echo "$EXECUTIONRESTART_FAILURE_IDS_ARRAY" | jq -r 'join(", ")')

# Only create expressions if there are metrics to aggregate
COMBINED_EXECUTIONRESTART="[]"
if [ -n "$EXECUTIONRESTART_SUCCESS_IDS_STR" ] || [ -n "$EXECUTIONRESTART_FAILURE_IDS_STR" ]; then
  if [ -n "$EXECUTIONRESTART_SUCCESS_IDS_STR" ]; then
    EXECUTIONRESTART_SUCCESS_EXPR="SUM([${EXECUTIONRESTART_SUCCESS_IDS_STR}])"
  else
    EXECUTIONRESTART_SUCCESS_EXPR=""
  fi
  if [ -n "$EXECUTIONRESTART_FAILURE_IDS_STR" ]; then
    EXECUTIONRESTART_FAILURE_EXPR="SUM([${EXECUTIONRESTART_FAILURE_IDS_STR}])"
  else
    EXECUTIONRESTART_FAILURE_EXPR=""
  fi

  # Build aggregated expressions only (no individual county metrics)
  # First, we need all individual metrics with IDs for the math expressions to reference
  ALL_EXECUTIONRESTART_METRICS=$(echo "$EXECUTIONRESTART_SUCCESS_METRICS $EXECUTIONRESTART_FAILURE_METRICS" | jq -s 'add')

# Create math expressions that reference the individual metrics
COMBINED_EXECUTIONRESTART=$(echo '[]' | jq --arg success_expr "$EXECUTIONRESTART_SUCCESS_EXPR" --arg failure_expr "$EXECUTIONRESTART_FAILURE_EXPR" \
  '. + 
  (if $success_expr != "" then [[{"expression": $success_expr, "label": "Success", "color": "#2ca02c"}]] else [] end) +
  (if $failure_expr != "" then [[{"expression": $failure_expr, "label": "Failure", "color": "#d62728"}]] else [] end)')
else
  # No metrics at all, use empty array
  ALL_EXECUTIONRESTART_METRICS="[]"
fi

# Build final metrics arrays, ensuring they're never empty
FINAL_AUTOREPAIR_METRICS=$(echo "$ALL_AUTOREPAIR_METRICS $COMBINED_AUTOREPAIR" | jq -s 'add')
FINAL_EXECUTIONRESTART_METRICS=$(echo "$ALL_EXECUTIONRESTART_METRICS $COMBINED_EXECUTIONRESTART" | jq -s 'add')

# If AutoRepair metrics array is empty, add a placeholder (hidden) metric
FINAL_AUTOREPAIR_METRICS=$(echo "$FINAL_AUTOREPAIR_METRICS" | jq 'if length == 0 then [["AWS/CloudWatch", "NoData", {"stat": "Sum", "label": "No AutoRepair metrics available", "visible": false}]] else . end')

# If ExecutionRestart metrics array is empty, add a placeholder (hidden) metric
FINAL_EXECUTIONRESTART_METRICS=$(echo "$FINAL_EXECUTIONRESTART_METRICS" | jq 'if length == 0 then [["AWS/CloudWatch", "NoData", {"stat": "Sum", "label": "No ExecutionRestart metrics available", "visible": false}]] else . end')

# Build ElephantWorkflow metrics array - simple direct metrics without county aggregation
# The metric only has Status dimension (Started, Success, Failure)
FINAL_ELEPHANTWORKFLOW_METRICS=$(jq -n --arg ns "${ELEPHANTWORKFLOW_NAMESPACE}" '[
  ["\($ns)", "WorkflowExecution", "Status", "Started", {"stat": "Sum", "label": "Started", "color": "#1f77b4"}],
  ["\($ns)", "WorkflowExecution", "Status", "Success", {"stat": "Sum", "label": "Success", "color": "#2ca02c"}],
  ["\($ns)", "WorkflowExecution", "Status", "Failure", {"stat": "Sum", "label": "Failure", "color": "#d62728"}]
]')

# Create dashboard JSON
cat > /tmp/dashboard.json <<EOF
{
  "widgets": [
    {
      "type": "metric",
      "x": 0,
      "y": 0,
      "width": 24,
      "height": 6,
      "properties": {
        "metrics": $FINAL_AUTOREPAIR_METRICS,
        "period": 300,
        "stat": "Sum",
        "region": "${REGION}",
        "title": "AutoRepair Success/Failure",
        "view": "timeSeries",
        "stacked": false,
        "yAxis": {
          "left": {
            "min": 0,
            "label": "Count"
          }
        }
      }
    },
    {
      "type": "metric",
      "x": 0,
      "y": 6,
      "width": 24,
      "height": 6,
      "properties": {
        "metrics": $FINAL_EXECUTIONRESTART_METRICS,
        "period": 300,
        "stat": "Sum",
        "region": "${REGION}",
        "title": "AutoErrorResolver",
        "view": "timeSeries",
        "stacked": false,
        "yAxis": {
          "left": {
            "min": 0,
            "label": "Count"
          }
        }
      }
    },
    {
      "type": "metric",
      "x": 0,
      "y": 12,
      "width": 24,
      "height": 6,
      "properties": {
        "metrics": $FINAL_ELEPHANTWORKFLOW_METRICS,
        "period": 300,
        "stat": "Sum",
        "region": "${REGION}",
        "title": "Workflow Execution Success/Failure",
        "view": "timeSeries",
        "stacked": false,
        "yAxis": {
          "left": {
            "min": 0,
            "label": "Count"
          }
        }
      }
    },
    {
      "type": "metric",
      "x": 0,
      "y": 18,
      "width": 24,
      "height": 6,
      "properties": {
        "metrics": [
          ["AWS/States", "ExecutionsStarted", "StateMachineArn", "${STEP_FUNCTION_ARN}", {"stat": "Sum", "label": "Executions Started", "color": "#1f77b4"}],
          ["AWS/States", "ExecutionsSucceeded", "StateMachineArn", "${STEP_FUNCTION_ARN}", {"stat": "Sum", "label": "Executions Succeeded", "color": "#2ca02c"}],
          ["AWS/States", "ExecutionsFailed", "StateMachineArn", "${STEP_FUNCTION_ARN}", {"stat": "Sum", "label": "Executions Failed", "color": "#d62728"}]
        ],
        "period": 300,
        "stat": "Sum",
        "region": "${REGION}",
        "title": "Execution",
        "view": "timeSeries",
        "stacked": false,
        "yAxis": {
          "left": {
            "min": 0,
            "label": "Count"
          }
        }
      }
    },
    {
      "type": "metric",
      "x": 0,
      "y": 24,
      "width": 24,
      "height": 6,
      "properties": {
        "metrics": [
          ["AWS/DynamoDB", "ConsumedWriteCapacityUnits", "TableName", "${DYNAMODB_TABLE_NAME}", {"stat": "Sum", "label": "Write Capacity Consumed"}]
        ],
        "period": 300,
        "stat": "Sum",
        "region": "${REGION}",
        "title": "Validation Error DynamoDB Write",
        "view": "timeSeries",
        "yAxis": {
          "left": {
            "min": 0,
            "label": "Capacity Units"
          }
        }
      }
    }
  ]
}
EOF

# Create the dashboard (disable pager to avoid interactive prompts)
AWS_PAGER="" aws cloudwatch put-dashboard \
  --dashboard-name "${DASHBOARD_NAME}" \
  --dashboard-body file:///tmp/dashboard.json \
  --region "${REGION}" \
  --no-cli-pager

if [ $? -eq 0 ]; then
  echo ""
  echo "✅ Dashboard created successfully!"
  echo ""
  echo "Dashboard uses SEARCH expressions to aggregate metrics across all dimensions"
  echo ""
  echo "View it at:"
  echo "https://${REGION}.console.aws.amazon.com/cloudwatch/home?region=${REGION}#dashboards:name=${DASHBOARD_NAME}"
else
  echo "❌ Failed to create dashboard"
  exit 1
fi
