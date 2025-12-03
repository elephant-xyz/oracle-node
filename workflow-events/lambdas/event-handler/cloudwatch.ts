import {
  CloudWatchClient,
  PutMetricDataCommand,
} from "@aws-sdk/client-cloudwatch";

/**
 * CloudWatch namespace for Elephant workflow metrics.
 */
const METRIC_NAMESPACE = "Elephant/Workflow";

/**
 * CloudWatch client singleton for publishing metrics.
 */
const cloudWatchClient = new CloudWatchClient({});

/**
 * Dimension values for workflow phase metrics.
 */
interface PhaseMetricDimensions {
  /** County identifier. */
  county: string;
  /** Status of the workflow step. */
  status: string;
  /** Step name within the phase. */
  step: string;
  /** Data group name. */
  dataGroupName?: string;
}

export const publishPhaseMetric = async (
  phase: string,
  dimensions: PhaseMetricDimensions,
): Promise<void> => {
  const metricName = `${phase}ElephantPhase`;

  const command = new PutMetricDataCommand({
    Namespace: METRIC_NAMESPACE,
    MetricData: [
      {
        MetricName: metricName,
        Dimensions: [
          { Name: "County", Value: dimensions.county },
          { Name: "Status", Value: dimensions.status },
          { Name: "Step", Value: dimensions.step },
          {
            Name: "DataGroupName",
            Value: dimensions.dataGroupName ?? "not-set",
          },
        ],
        Unit: "Count",
        Value: 1,
        Timestamp: new Date(),
      },
    ],
  });

  await cloudWatchClient.send(command);
};
