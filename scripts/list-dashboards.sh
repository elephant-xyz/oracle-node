#!/bin/bash
# List all CloudWatch Dashboards
# Usage: ./list-dashboards.sh [region]

REGION="${1:-${AWS_REGION:-us-east-1}}"

echo "CloudWatch Dashboards in region: ${REGION}"
echo ""

aws cloudwatch list-dashboards \
  --region "${REGION}" \
  --query 'DashboardEntries[*].[DashboardName,LastModified]' \
  --output table

echo ""
echo "To view a dashboard, run:"
echo "  ./scripts/open-dashboard.sh <dashboard-name> ${REGION}"
echo ""
echo "Or visit:"
echo "  https://${REGION}.console.aws.amazon.com/cloudwatch/home?region=${REGION}#dashboards:"

