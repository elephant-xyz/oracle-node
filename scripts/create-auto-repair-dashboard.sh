#!/bin/bash
# Create CloudWatch Dashboard for Auto-Repair and Execution Restart Metrics
# Usage: ./create-auto-repair-dashboard.sh [dashboard-name] [region]

set -e

DASHBOARD_NAME="${1:-ErrorRecovery-Metrics}"
REGION="${2:-${AWS_REGION:-us-east-1}}"
AUTOREPAIR_NAMESPACE="${AUTOREPAIR_METRIC_NAMESPACE:-AutoRepair}"
EXECUTIONRESTART_NAMESPACE="${EXECUTIONRESTART_METRIC_NAMESPACE:-ExecutionRestart}"

echo "Creating CloudWatch Dashboard: ${DASHBOARD_NAME}"
echo "Region: ${REGION}"
echo "AutoRepair Namespace: ${AUTOREPAIR_NAMESPACE}"
echo "ExecutionRestart Namespace: ${EXECUTIONRESTART_NAMESPACE}"
echo ""

# Create dashboard body
DASHBOARD_BODY=$(cat <<EOF
{
  "widgets": [
    {
      "type": "metric",
      "x": 0,
      "y": 0,
      "width": 24,
      "height": 6,
      "properties": {
        "metrics": [
          ["${AUTOREPAIR_NAMESPACE}", "AutoRepairWorkflowSuccess", {"stat": "Sum", "label": "Success"}],
          ["...", "AutoRepairFailure", {"stat": "Sum", "label": "Failure"}]
        ],
        "period": 3600,
        "stat": "Sum",
        "region": "${REGION}",
        "title": "Auto-Repair Success vs Failure",
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
        "metrics": [
          ["${AUTOREPAIR_NAMESPACE}", "AutoRepairErrorsFixed", {"stat": "Sum", "label": "Errors Fixed"}]
        ],
        "period": 3600,
        "stat": "Sum",
        "region": "${REGION}",
        "title": "Total Errors Fixed",
        "view": "timeSeries",
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
        "metrics": [
          ["${AUTOREPAIR_NAMESPACE}", "AutoRepairFailure", "FailureReason", "MaxRetriesExceeded", {"stat": "Sum", "label": "Max Retries"}],
          ["${AUTOREPAIR_NAMESPACE}", "AutoRepairFailure", "FailureReason", "NoErrorsUri", {"stat": "Sum", "label": "No Errors URI"}]
        ],
        "period": 3600,
        "stat": "Sum",
        "region": "${REGION}",
        "title": "Failure Reasons",
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
      "width": 12,
      "height": 6,
      "properties": {
        "metrics": [
          ["${EXECUTIONRESTART_NAMESPACE}", "ExecutionRestartSuccess", {"stat": "Sum", "label": "Success"}],
          ["...", "ExecutionRestartFailure", {"stat": "Sum", "label": "Failure"}]
        ],
        "period": 3600,
        "stat": "Sum",
        "region": "${REGION}",
        "title": "Execution Restart Success vs Failure",
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
      "x": 12,
      "y": 18,
      "width": 12,
      "height": 6,
      "properties": {
        "metrics": [
          ["${EXECUTIONRESTART_NAMESPACE}", "ExecutionRestartProcessed", {"stat": "Sum", "label": "Executions Processed"}]
        ],
        "period": 3600,
        "stat": "Sum",
        "region": "${REGION}",
        "title": "Executions Processed",
        "view": "timeSeries",
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
      "width": 12,
      "height": 6,
      "properties": {
        "metrics": [
          ["${EXECUTIONRESTART_NAMESPACE}", "ExecutionRestartFailure", "FailureReason", "PostProcessingFailed", {"stat": "Sum", "label": "Post Processing Failed"}],
          ["${EXECUTIONRESTART_NAMESPACE}", "ExecutionRestartFailure", "FailureReason", "LambdaInvocationFailed", {"stat": "Sum", "label": "Lambda Invocation Failed"}]
        ],
        "period": 3600,
        "stat": "Sum",
        "region": "${REGION}",
        "title": "Execution Restart Failure Reasons",
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
      "x": 12,
      "y": 24,
      "width": 12,
      "height": 6,
      "properties": {
        "metrics": [
          ["${EXECUTIONRESTART_NAMESPACE}", "ExecutionRestartTransactionItemsSent", {"stat": "Sum", "label": "Transaction Items Sent"}]
        ],
        "period": 3600,
        "stat": "Sum",
        "region": "${REGION}",
        "title": "Transaction Items Sent",
        "view": "timeSeries",
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
      "y": 30,
      "width": 24,
      "height": 6,
      "properties": {
        "metrics": [
          ["${EXECUTIONRESTART_NAMESPACE}", "ExecutionRestartUnrecoverable", {"stat": "Sum", "label": "Unrecoverable"}]
        ],
        "period": 3600,
        "stat": "Sum",
        "region": "${REGION}",
        "title": "Unrecoverable Executions",
        "view": "timeSeries",
        "yAxis": {
          "left": {
            "min": 0,
            "label": "Count"
          }
        }
      }
    }
  ]
}
EOF
)

# Create the dashboard
aws cloudwatch put-dashboard \
  --dashboard-name "${DASHBOARD_NAME}" \
  --dashboard-body "${DASHBOARD_BODY}" \
  --region "${REGION}"

if [ $? -eq 0 ]; then
  echo ""
  echo "✅ Dashboard created successfully!"
  echo ""
  echo "View it at:"
  echo "https://${REGION}.console.aws.amazon.com/cloudwatch/home?region=${REGION}#dashboards:name=${DASHBOARD_NAME}"
  echo ""
  echo "Or run:"
  echo "aws cloudwatch get-dashboard --dashboard-name ${DASHBOARD_NAME} --region ${REGION}"
else
  echo "❌ Failed to create dashboard"
  exit 1
fi

