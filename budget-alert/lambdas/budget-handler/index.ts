import type { SNSEvent, SNSHandler } from "aws-lambda";
import {
  CloudFormationClient,
  ListStackResourcesCommand,
  type StackResourceSummary,
} from "@aws-sdk/client-cloudformation";
import {
  LambdaClient,
  ListEventSourceMappingsCommand,
  UpdateEventSourceMappingCommand,
  type EventSourceMappingConfiguration,
} from "@aws-sdk/client-lambda";
import {
  SFNClient,
  ListExecutionsCommand,
  StopExecutionCommand,
  ExecutionStatus,
  type ExecutionListItem,
} from "@aws-sdk/client-sfn";

const SKIP_ESM_STATES = new Set([
  "Disabled",
  "Disabling",
  "Deleting",
  "Creating",
  "Enabling",
  "Updating",
]);

interface BudgetAlertMessage {
  budgetName: string;
  budgetType: string;
  actualAmount: number;
  budgetLimit: number;
  accountId: string;
  region: string;
  alertThreshold: number;
  alertType: string;
  startDate: string;
  endDate: string;
}

interface DisableResult {
  uuid: string;
  functionArn: string;
  eventSourceArn: string;
  success: boolean;
  error?: string;
}

interface StopResult {
  executionArn: string;
  success: boolean;
  error?: string;
}

interface EmergencyStopSummary {
  stackName: string;
  eventSourceMappingsDisabled: number;
  eventSourceMappingsFailed: number;
  executionsStopped: number;
  executionsStopFailed: number;
  errors: string[];
}

const cfnClient = new CloudFormationClient({});
const lambdaClient = new LambdaClient({});
const sfnClient = new SFNClient({});

const createLogEntry = (
  action: string,
  data: Record<string, unknown>,
): string => {
  return JSON.stringify({
    action,
    timestamp: new Date().toISOString(),
    ...data,
  });
};

const parseBudgetMessage = (message: string): BudgetAlertMessage | null => {
  try {
    return JSON.parse(message);
  } catch {
    return null;
  }
};

async function discoverStackResources(
  stackName: string,
): Promise<{ lambdaFunctionArns: string[]; stateMachineArns: string[] }> {
  const lambdaFunctionArns: string[] = [];
  const stateMachineArns: string[] = [];

  let nextToken: string | undefined;

  do {
    const command = new ListStackResourcesCommand({
      StackName: stackName,
      NextToken: nextToken,
    });

    const response = await cfnClient.send(command);
    const resources: StackResourceSummary[] =
      response.StackResourceSummaries || [];

    for (const resource of resources) {
      if (
        resource.ResourceType === "AWS::Lambda::Function" &&
        resource.PhysicalResourceId
      ) {
        lambdaFunctionArns.push(resource.PhysicalResourceId);
      } else if (
        resource.ResourceType === "AWS::StepFunctions::StateMachine" &&
        resource.PhysicalResourceId
      ) {
        stateMachineArns.push(resource.PhysicalResourceId);
      } else if (
        resource.ResourceType === "AWS::Serverless::Function" &&
        resource.PhysicalResourceId
      ) {
        lambdaFunctionArns.push(resource.PhysicalResourceId);
      } else if (
        resource.ResourceType === "AWS::Serverless::StateMachine" &&
        resource.PhysicalResourceId
      ) {
        stateMachineArns.push(resource.PhysicalResourceId);
      }
    }

    nextToken = response.NextToken;
  } while (nextToken);

  console.info(
    createLogEntry("stack_resources_discovered", {
      stackName,
      lambdaFunctionCount: lambdaFunctionArns.length,
      stateMachineCount: stateMachineArns.length,
    }),
  );

  return { lambdaFunctionArns, stateMachineArns };
}

async function disableEventSourceMappings(
  functionArns: string[],
): Promise<DisableResult[]> {
  const results: DisableResult[] = [];

  for (const functionArn of functionArns) {
    const functionName = functionArn.split(":").pop() || functionArn;

    let nextMarker: string | undefined;

    do {
      const listCommand = new ListEventSourceMappingsCommand({
        FunctionName: functionName,
        Marker: nextMarker,
        MaxItems: 100,
      });

      const response = await lambdaClient.send(listCommand);
      const mappings: EventSourceMappingConfiguration[] =
        response.EventSourceMappings || [];

      for (const mapping of mappings) {
        const shouldSkip = mapping.State && SKIP_ESM_STATES.has(mapping.State);
        if (
          mapping.EventSourceArn?.includes(":sqs:") &&
          mapping.UUID &&
          !shouldSkip
        ) {
          try {
            await lambdaClient.send(
              new UpdateEventSourceMappingCommand({
                UUID: mapping.UUID,
                Enabled: false,
              }),
            );

            console.info(
              createLogEntry("event_source_mapping_disabled", {
                uuid: mapping.UUID,
                functionArn: mapping.FunctionArn,
                eventSourceArn: mapping.EventSourceArn,
              }),
            );

            results.push({
              uuid: mapping.UUID,
              functionArn: mapping.FunctionArn || functionArn,
              eventSourceArn: mapping.EventSourceArn || "",
              success: true,
            });
          } catch (error) {
            const errorMessage = String(error);

            console.error(
              createLogEntry("event_source_mapping_disable_failed", {
                uuid: mapping.UUID,
                functionArn: mapping.FunctionArn,
                eventSourceArn: mapping.EventSourceArn,
                error: errorMessage,
              }),
            );

            results.push({
              uuid: mapping.UUID,
              functionArn: mapping.FunctionArn || functionArn,
              eventSourceArn: mapping.EventSourceArn || "",
              success: false,
              error: errorMessage,
            });
          }
        }
      }

      nextMarker = response.NextMarker;
    } while (nextMarker);
  }

  return results;
}

async function stopRunningExecutions(
  stateMachineArns: string[],
): Promise<StopResult[]> {
  const results: StopResult[] = [];

  for (const stateMachineArn of stateMachineArns) {
    let nextToken: string | undefined;

    do {
      const response = await sfnClient.send(
        new ListExecutionsCommand({
          stateMachineArn,
          statusFilter: ExecutionStatus.RUNNING,
          maxResults: 100,
          nextToken,
        }),
      );
      const executions: ExecutionListItem[] = response.executions || [];

      for (const execution of executions) {
        if (!execution.executionArn) continue;

        try {
          await sfnClient.send(
            new StopExecutionCommand({
              executionArn: execution.executionArn,
              error: "BudgetAlertTriggered",
              cause: "Daily budget exceeded - emergency stop triggered",
            }),
          );

          console.info(
            createLogEntry("execution_stopped", {
              executionArn: execution.executionArn,
              stateMachineArn,
            }),
          );

          results.push({
            executionArn: execution.executionArn,
            success: true,
          });
        } catch (error) {
          const errorMessage = String(error);

          console.error(
            createLogEntry("execution_stop_failed", {
              executionArn: execution.executionArn,
              stateMachineArn,
              error: errorMessage,
            }),
          );

          results.push({
            executionArn: execution.executionArn,
            success: false,
            error: errorMessage,
          });
        }
      }

      nextToken = response.nextToken;
    } while (nextToken);
  }

  return results;
}

async function emergencyStop(stackName: string): Promise<EmergencyStopSummary> {
  console.info(
    createLogEntry("emergency_stop_started", {
      stackName,
    }),
  );

  const errors: string[] = [];

  const { lambdaFunctionArns, stateMachineArns } =
    await discoverStackResources(stackName);

  const disableResults = await disableEventSourceMappings(lambdaFunctionArns);
  const disabledCount = disableResults.filter((r) => r.success).length;
  const disableFailedCount = disableResults.filter((r) => !r.success).length;

  for (const result of disableResults) {
    if (!result.success && result.error) {
      errors.push(`Failed to disable ESM ${result.uuid}: ${result.error}`);
    }
  }

  const stopResults = await stopRunningExecutions(stateMachineArns);
  const stoppedCount = stopResults.filter((r) => r.success).length;
  const stopFailedCount = stopResults.filter((r) => !r.success).length;

  for (const result of stopResults) {
    if (!result.success && result.error) {
      errors.push(
        `Failed to stop execution ${result.executionArn}: ${result.error}`,
      );
    }
  }

  const summary: EmergencyStopSummary = {
    stackName,
    eventSourceMappingsDisabled: disabledCount,
    eventSourceMappingsFailed: disableFailedCount,
    executionsStopped: stoppedCount,
    executionsStopFailed: stopFailedCount,
    errors,
  };

  console.info(
    createLogEntry("emergency_stop_completed", {
      ...summary,
    }),
  );

  return summary;
}

export const handler: SNSHandler = async (event: SNSEvent): Promise<void> => {
  console.info(
    createLogEntry("budget_alert_received", {
      recordCount: event.Records.length,
    }),
  );

  for (const record of event.Records) {
    const { Sns } = record;

    console.info(
      createLogEntry("processing_sns_record", {
        subject: Sns.Subject,
        topicArn: Sns.TopicArn,
        timestamp: Sns.Timestamp,
      }),
    );

    const budgetMessage = parseBudgetMessage(Sns.Message);

    if (budgetMessage) {
      console.info(
        createLogEntry("budget_alert_details", {
          budgetName: budgetMessage.budgetName,
          actualAmount: budgetMessage.actualAmount,
          budgetLimit: budgetMessage.budgetLimit,
          alertThreshold: budgetMessage.alertThreshold,
        }),
      );
    } else {
      console.warn(
        createLogEntry("raw_message_logged", {
          message: Sns.Message,
        }),
      );
    }
  }

  const prepareStackName = process.env.PREPARE_STACK_NAME;

  if (prepareStackName) {
    try {
      const summary = await emergencyStop(prepareStackName);

      console.info(
        createLogEntry("emergency_stop_summary", {
          ...summary,
        }),
      );
    } catch (error) {
      console.error(
        createLogEntry("emergency_stop_failed", {
          stackName: prepareStackName,
          error: String(error),
        }),
      );

      throw error;
    }
  } else {
    console.warn(
      createLogEntry("emergency_stop_skipped", {
        reason: "PREPARE_STACK_NAME environment variable not set",
      }),
    );
  }

  console.info(
    createLogEntry("budget_alert_processing_complete", {
      recordsProcessed: event.Records.length,
    }),
  );
};
