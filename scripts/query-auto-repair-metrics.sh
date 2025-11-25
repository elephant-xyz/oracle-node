#!/bin/bash
# Query Auto-Repair CloudWatch Metrics
# Usage: ./query-auto-repair-metrics.sh [metric-name] [county] [error-type] [hours]

set -e

NAMESPACE="${CLOUDWATCH_METRIC_NAMESPACE:-AutoRepair}"
REGION="${AWS_REGION:-us-east-1}"
METRIC_NAME="${1:-AutoRepairWorkflowSuccess}"
COUNTY="${2:-}"
ERROR_TYPE="${3:-}"
HOURS="${4:-24}"

START_TIME=$(date -u -d "${HOURS} hours ago" +%Y-%m-%dT%H:%M:%S)
END_TIME=$(date -u +%Y-%m-%dT%H:%M:%S)

echo "Querying metric: ${NAMESPACE}/${METRIC_NAME}"
echo "Time range: ${START_TIME} to ${END_TIME}"
echo ""

# Build dimensions
DIMENSIONS=""
if [ -n "$COUNTY" ]; then
  DIMENSIONS="Name=County,Value=${COUNTY}"
fi

if [ -n "$ERROR_TYPE" ]; then
  if [ -n "$DIMENSIONS" ]; then
    DIMENSIONS="${DIMENSIONS} Name=ErrorType,Value=${ERROR_TYPE}"
  else
    DIMENSIONS="Name=ErrorType,Value=${ERROR_TYPE}"
  fi
fi

# Build AWS CLI command
CMD="aws cloudwatch get-metric-statistics \
  --namespace \"${NAMESPACE}\" \
  --metric-name \"${METRIC_NAME}\""

if [ -n "$DIMENSIONS" ]; then
  CMD="${CMD} --dimensions ${DIMENSIONS}"
fi

CMD="${CMD} \
  --start-time \"${START_TIME}\" \
  --end-time \"${END_TIME}\" \
  --period 3600 \
  --statistics Sum Average Maximum \
  --region \"${REGION}\""

# Execute and format output
eval $CMD | jq -r '
  .Datapoints | 
  sort_by(.Timestamp) | 
  .[] | 
  "\(.Timestamp) | Sum: \(.Sum // 0) | Avg: \(.Average // 0) | Max: \(.Maximum // 0)"
'

# Get totals
echo ""
echo "=== Summary ==="
TOTAL_SUM=$(eval $CMD | jq -r '[.Datapoints[].Sum // 0] | add')
TOTAL_AVG=$(eval $CMD | jq -r '[.Datapoints[].Average // 0] | add / length')
TOTAL_MAX=$(eval $CMD | jq -r '[.Datapoints[].Maximum // 0] | max')

echo "Total Sum: ${TOTAL_SUM}"
echo "Average: ${TOTAL_AVG}"
echo "Maximum: ${TOTAL_MAX}"

