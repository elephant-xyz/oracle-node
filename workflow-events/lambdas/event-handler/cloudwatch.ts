import {
  CloudWatchClient,
  PutMetricDataCommand,
} from "@aws-sdk/client-cloudwatch";

const METRIC_NAMESPACE = "Elephant/Workflow";

const cloudWatchClient = new CloudWatchClient({});

interface PhaseMetricDimensions {
  county: string;
  status: string;
  step: string;
  dataGroupLabel?: string;
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
            Value: dimensions.dataGroupLabel ?? "not-set",
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
