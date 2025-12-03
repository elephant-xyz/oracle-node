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
}

/**
 * Publishes a workflow phase metric to CloudWatch.
 * The metric name follows the pattern `{Phase}ElephantPhase` (e.g., `scrapeElephantPhase`).
 *
 * @param phase - The workflow phase name used to construct the metric name.
 * @param dimensions - The dimensions to attach to the metric (county, status, step).
 * @throws Error if the CloudWatch API call fails.
 *
 * @example
 * ```typescript
 * await publishPhaseMetric("scrape", {
 *   county: "palm_beach",
 *   status: "SUCCEEDED",
 *   step: "login",
 * });
 * // Publishes metric: scrapeElephantPhase with dimensions County=palm_beach, Status=SUCCEEDED, Step=login
 * ```
 */
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
        ],
        Unit: "Count",
        Value: 1,
        Timestamp: new Date(),
      },
    ],
  });

  await cloudWatchClient.send(command);
};
