#!/bin/bash
# Query EventBridge events for elephant.workflow

# Set your AWS region
REGION=${AWS_REGION:-us-east-1}

echo "Querying EventBridge events for Source: elephant.workflow, DetailType: WorkflowEvent"
echo ""

# Query events from the last hour
START_TIME=$(date -u -v-1H +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -d '1 hour ago' +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -v-1H +"%Y-%m-%dT%H:%M:%SZ")
END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

echo "Time range: $START_TIME to $END_TIME"
echo ""

# Use AWS CLI to query events
aws events list-rule-names-by-target \
  --target-arn "arn:aws:events:${REGION}:$(aws sts get-caller-identity --query Account --output text):event-bus/default" \
  --region $REGION 2>/dev/null || echo "No rules found"

echo ""
echo "To view recent events, you can:"
echo "1. Go to AWS Console → EventBridge → Events → Replay"
echo "2. Create an EventBridge rule to forward events to CloudWatch Logs"
echo "3. Use CloudWatch Insights to query the default event bus"
echo ""
echo "To create a rule that logs all workflow events to CloudWatch:"
echo "  aws events put-rule --name elephant-workflow-logger \\"
echo "    --event-pattern '{\"source\":[\"elephant.workflow\"],\"detail-type\":[\"WorkflowEvent\"]}' \\"
echo "    --state ENABLED"
echo ""
echo "  aws events put-targets --rule elephant-workflow-logger \\"
echo "    --targets \"Id=1,Arn=arn:aws:logs:${REGION}:$(aws sts get-caller-identity --query Account --output text):log-group:/aws/events/elephant-workflow\""
echo ""
echo "Or use the AWS Console:"
echo "  EventBridge → Rules → Create rule"
echo "  Event pattern: Custom pattern"
echo "  JSON:"
echo "  {"
echo "    \"source\": [\"elephant.workflow\"],"
echo "    \"detail-type\": [\"WorkflowEvent\"]"
echo "  }"
echo "  Target: CloudWatch Logs group (create new: /aws/events/elephant-workflow)"

