#!/bin/bash
# Open CloudWatch Dashboard in browser
# Usage: ./open-dashboard.sh [dashboard-name] [region]

DASHBOARD_NAME="${1:-ErrorRecovery-Metrics}"
REGION="${2:-${AWS_REGION:-us-east-1}}"

# URL encode the dashboard name (replace spaces with %20)
ENCODED_NAME=$(echo "$DASHBOARD_NAME" | sed 's/ /%20/g')

URL="https://${REGION}.console.aws.amazon.com/cloudwatch/home?region=${REGION}#dashboards:name=${ENCODED_NAME}"

echo "Opening dashboard: ${DASHBOARD_NAME}"
echo "URL: ${URL}"
echo ""

# Open in default browser
if command -v open &> /dev/null; then
  # macOS
  open "$URL"
elif command -v xdg-open &> /dev/null; then
  # Linux
  xdg-open "$URL"
elif command -v start &> /dev/null; then
  # Windows
  start "$URL"
else
  echo "Please open this URL in your browser:"
  echo "$URL"
fi

