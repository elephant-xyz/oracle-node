#!/bin/bash
# Check if CloudWatch metrics exist
# Usage: ./check-metrics.sh [namespace] [hours]

NAMESPACE="${1:-AutoRepair}"
HOURS="${2:-24}"
REGION="${AWS_REGION:-us-east-1}"

echo "Checking metrics for namespace: ${NAMESPACE}"
echo "Time range: Last ${HOURS} hours"
echo "Region: ${REGION}"
echo ""

# List all metrics in the namespace
METRIC_COUNT=$(aws cloudwatch list-metrics --namespace "${NAMESPACE}" --region "${REGION}" --output json | jq '.Metrics | length')

if [ "$METRIC_COUNT" -eq 0 ]; then
  echo "❌ No metrics found in namespace '${NAMESPACE}'"
  echo ""
  echo "Possible reasons:"
  echo "1. No auto-repair workflows have run yet"
  echo "2. Metrics are being published to a different namespace"
  echo "3. Check the CLOUDWATCH_METRIC_NAMESPACE environment variable"
  echo "4. Check CloudWatch Logs for metric publishing errors"
  echo ""
  echo "To check what namespace is being used, look for log entries like:"
  echo "  'Published metric: <namespace>/<metricName> = <value>'"
else
  echo "✅ Found ${METRIC_COUNT} metric(s) in namespace '${NAMESPACE}'"
  echo ""
  echo "Available metrics:"
  aws cloudwatch list-metrics --namespace "${NAMESPACE}" --region "${REGION}" --output json | \
    jq -r '.Metrics[] | "  - \(.MetricName)\(if .Dimensions and (.Dimensions | length) > 0 then " [Dimensions: " + (.Dimensions | map("\(.Name)=\(.Value)") | join(", ")) + "]" else "" end)"'
  echo ""
  
  # Check for recent data
  START_TIME=$(date -u -v-${HOURS}H +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u -d "${HOURS} hours ago" +%Y-%m-%dT%H:%M:%S)
  END_TIME=$(date -u +%Y-%m-%dT%H:%M:%S)
  
  echo "Checking for data in the last ${HOURS} hours..."
  echo ""
  
  # Get first metric to check
  FIRST_METRIC=$(aws cloudwatch list-metrics --namespace "${NAMESPACE}" --region "${REGION}" --output json | jq -r '.Metrics[0].MetricName // empty')
  
  if [ -n "$FIRST_METRIC" ]; then
    echo "Sample metric: ${FIRST_METRIC}"
    DATA_POINTS=$(aws cloudwatch get-metric-statistics \
      --namespace "${NAMESPACE}" \
      --metric-name "${FIRST_METRIC}" \
      --start-time "${START_TIME}" \
      --end-time "${END_TIME}" \
      --period 3600 \
      --statistics Sum \
      --region "${REGION}" \
      --output json | jq '.Datapoints | length')
    
    if [ "$DATA_POINTS" -eq 0 ]; then
      echo "  ⚠️  No data points in the last ${HOURS} hours"
      echo "  This could mean:"
      echo "    - No workflows have run recently"
      echo "    - Metrics are published but dashboard time range needs adjustment"
    else
      echo "  ✅ Found ${DATA_POINTS} data point(s) in the last ${HOURS} hours"
    fi
  fi
fi

echo ""
echo "To view metrics in CloudWatch Console:"
echo "https://${REGION}.console.aws.amazon.com/cloudwatch/home?region=${REGION}#metricsV2:graph=~();namespace=${NAMESPACE}"

