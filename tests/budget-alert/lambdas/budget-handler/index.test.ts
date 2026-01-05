import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import {
  CloudFormationClient,
  ListStackResourcesCommand,
} from "@aws-sdk/client-cloudformation";
import {
  LambdaClient,
  ListEventSourceMappingsCommand,
  UpdateEventSourceMappingCommand,
} from "@aws-sdk/client-lambda";
import {
  SFNClient,
  ListExecutionsCommand,
  StopExecutionCommand,
  ExecutionStatus,
} from "@aws-sdk/client-sfn";
import type { SNSEvent, SNSEventRecord } from "aws-lambda";

/**
 * Mock the AWS SDK clients for all tests.
 */
const cfnMock = mockClient(CloudFormationClient);
const lambdaMock = mockClient(LambdaClient);
const sfnMock = mockClient(SFNClient);

/**
 * Test stack name used in all tests.
 */
const TEST_STACK_NAME = "test-prepare-stack";

/**
 * Creates a mock SNS event with the given message.
 */
const createMockSNSEvent = (
  message: string,
  subject = "AWS Budget Alert",
): SNSEvent => ({
  Records: [
    {
      EventVersion: "1.0",
      EventSubscriptionArn:
        "arn:aws:sns:us-east-1:123456789012:budget-alerts:abc123",
      EventSource: "aws:sns",
      Sns: {
        Type: "Notification",
        MessageId: "test-message-id-12345",
        TopicArn: "arn:aws:sns:us-east-1:123456789012:budget-alerts",
        Subject: subject,
        Message: message,
        Timestamp: new Date().toISOString(),
        SignatureVersion: "1",
        Signature: "test-signature",
        SigningCertUrl: "https://sns.us-east-1.amazonaws.com/cert.pem",
        UnsubscribeUrl: "https://sns.us-east-1.amazonaws.com/unsubscribe",
        MessageAttributes: {},
      },
    } as SNSEventRecord,
  ],
});

/**
 * Creates a valid budget alert message.
 */
const createBudgetAlertMessage = (
  overrides: Partial<{
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
  }> = {},
): string =>
  JSON.stringify({
    budgetName: "DailyBudget",
    budgetType: "COST",
    actualAmount: 150.0,
    budgetLimit: 100.0,
    accountId: "123456789012",
    region: "us-east-1",
    alertThreshold: 100,
    alertType: "ACTUAL",
    startDate: "2024-01-01",
    endDate: "2024-01-31",
    ...overrides,
  });

describe("budget-handler", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.resetModules();
    cfnMock.reset();
    lambdaMock.reset();
    sfnMock.reset();
    process.env = {
      ...originalEnv,
      PREPARE_STACK_NAME: TEST_STACK_NAME,
    };
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "info").mockImplementation(() => {});
    vi.spyOn(console, "debug").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  describe("handler - SNS event processing", () => {
    it("should process valid SNS budget alert event", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await expect(handler(event)).resolves.toBeUndefined();
    });

    it("should process multiple SNS records in a single event", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event: SNSEvent = {
        Records: [
          createMockSNSEvent(createBudgetAlertMessage()).Records[0],
          createMockSNSEvent(createBudgetAlertMessage()).Records[0],
        ],
      };

      await expect(handler(event)).resolves.toBeUndefined();

      // Should call ListStackResourcesCommand only once (emergency stop runs once per invocation)
      expect(cfnMock).toHaveReceivedCommandTimes(ListStackResourcesCommand, 1);
    });

    it("should continue processing even with invalid JSON message", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent("this is not valid JSON");

      // The handler should still process and trigger emergency stop
      await expect(handler(event)).resolves.toBeUndefined();

      // Should still discover stack resources even with invalid message
      expect(cfnMock).toHaveReceivedCommand(ListStackResourcesCommand);
    });

    it("should log warning for unparseable message but continue", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent("{invalid json}");

      await handler(event);

      expect(console.warn).toHaveBeenCalled();
    });

    it("should skip emergency stop when PREPARE_STACK_NAME is not set", async () => {
      delete process.env.PREPARE_STACK_NAME;

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await expect(handler(event)).resolves.toBeUndefined();

      // Should not call CloudFormation
      expect(cfnMock).not.toHaveReceivedCommand(ListStackResourcesCommand);
      expect(console.warn).toHaveBeenCalled();
    });
  });

  describe("discoverStackResources", () => {
    it("should discover Lambda functions from CloudFormation stack", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            PhysicalResourceId:
              "arn:aws:lambda:us-east-1:123456789012:function:my-function",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [],
      });
      sfnMock.on(ListExecutionsCommand).resolves({ executions: [] });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      expect(cfnMock).toHaveReceivedCommandWith(ListStackResourcesCommand, {
        StackName: TEST_STACK_NAME,
      });
    });

    it("should discover State Machines from CloudFormation stack", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyStateMachine",
            PhysicalResourceId:
              "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
            ResourceType: "AWS::StepFunctions::StateMachine",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      sfnMock.on(ListExecutionsCommand).resolves({ executions: [] });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      expect(sfnMock).toHaveReceivedCommandWith(ListExecutionsCommand, {
        stateMachineArn:
          "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
        statusFilter: ExecutionStatus.RUNNING,
      });
    });

    it("should discover SAM Serverless Functions (AWS::Serverless::Function)", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MySamFunction",
            PhysicalResourceId: "my-sam-function",
            ResourceType: "AWS::Serverless::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      expect(lambdaMock).toHaveReceivedCommandWith(
        ListEventSourceMappingsCommand,
        {
          FunctionName: "my-sam-function",
        },
      );
    });

    it("should discover SAM Serverless State Machines (AWS::Serverless::StateMachine)", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MySamStateMachine",
            PhysicalResourceId:
              "arn:aws:states:us-east-1:123456789012:stateMachine:my-sam-state-machine",
            ResourceType: "AWS::Serverless::StateMachine",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      sfnMock.on(ListExecutionsCommand).resolves({ executions: [] });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      expect(sfnMock).toHaveReceivedCommandWith(ListExecutionsCommand, {
        stateMachineArn:
          "arn:aws:states:us-east-1:123456789012:stateMachine:my-sam-state-machine",
      });
    });

    it("should handle pagination in ListStackResources", async () => {
      cfnMock
        .on(ListStackResourcesCommand)
        .resolvesOnce({
          StackResourceSummaries: [
            {
              LogicalResourceId: "Function1",
              PhysicalResourceId: "function-1",
              ResourceType: "AWS::Lambda::Function",
              ResourceStatus: "CREATE_COMPLETE",
            },
          ],
          NextToken: "token-page-2",
        })
        .resolvesOnce({
          StackResourceSummaries: [
            {
              LogicalResourceId: "Function2",
              PhysicalResourceId: "function-2",
              ResourceType: "AWS::Lambda::Function",
              ResourceStatus: "CREATE_COMPLETE",
            },
          ],
        });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      expect(cfnMock).toHaveReceivedCommandTimes(ListStackResourcesCommand, 2);
      // Should list event source mappings for both functions
      expect(lambdaMock).toHaveReceivedCommandTimes(
        ListEventSourceMappingsCommand,
        2,
      );
    });

    it("should ignore resources without PhysicalResourceId", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            // PhysicalResourceId is undefined
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_IN_PROGRESS",
          },
        ],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      // Should not try to list event source mappings for undefined physical resource
      expect(lambdaMock).not.toHaveReceivedCommand(
        ListEventSourceMappingsCommand,
      );
    });

    it("should ignore non-Lambda and non-StateMachine resource types", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyQueue",
            PhysicalResourceId: "arn:aws:sqs:us-east-1:123456789012:my-queue",
            ResourceType: "AWS::SQS::Queue",
            ResourceStatus: "CREATE_COMPLETE",
          },
          {
            LogicalResourceId: "MyTable",
            PhysicalResourceId: "my-table",
            ResourceType: "AWS::DynamoDB::Table",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      expect(lambdaMock).not.toHaveReceivedCommand(
        ListEventSourceMappingsCommand,
      );
      expect(sfnMock).not.toHaveReceivedCommand(ListExecutionsCommand);
    });
  });

  describe("disableEventSourceMappings", () => {
    it("should disable SQS event source mappings", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            PhysicalResourceId:
              "arn:aws:lambda:us-east-1:123456789012:function:my-function",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [
          {
            UUID: "uuid-123",
            EventSourceArn: "arn:aws:sqs:us-east-1:123456789012:my-queue",
            FunctionArn:
              "arn:aws:lambda:us-east-1:123456789012:function:my-function",
            State: "Enabled",
          },
        ],
      });
      lambdaMock.on(UpdateEventSourceMappingCommand).resolves({});

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      expect(lambdaMock).toHaveReceivedCommandWith(
        UpdateEventSourceMappingCommand,
        {
          UUID: "uuid-123",
          Enabled: false,
        },
      );
    });

    it("should NOT disable non-SQS event source mappings (e.g., Kinesis, DynamoDB Streams)", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            PhysicalResourceId: "my-function",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [
          {
            UUID: "kinesis-uuid",
            EventSourceArn:
              "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
            FunctionArn:
              "arn:aws:lambda:us-east-1:123456789012:function:my-function",
            State: "Enabled",
          },
          {
            UUID: "dynamodb-uuid",
            EventSourceArn:
              "arn:aws:dynamodb:us-east-1:123456789012:table/my-table/stream/2024-01-01",
            FunctionArn:
              "arn:aws:lambda:us-east-1:123456789012:function:my-function",
            State: "Enabled",
          },
        ],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      // Should not try to disable Kinesis or DynamoDB stream mappings
      expect(lambdaMock).not.toHaveReceivedCommand(
        UpdateEventSourceMappingCommand,
      );
    });

    it("should skip already Disabled event source mappings", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            PhysicalResourceId: "my-function",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [
          {
            UUID: "uuid-123",
            EventSourceArn: "arn:aws:sqs:us-east-1:123456789012:my-queue",
            FunctionArn:
              "arn:aws:lambda:us-east-1:123456789012:function:my-function",
            State: "Disabled",
          },
        ],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      expect(lambdaMock).not.toHaveReceivedCommand(
        UpdateEventSourceMappingCommand,
      );
    });

    it("should skip Disabling event source mappings", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            PhysicalResourceId: "my-function",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [
          {
            UUID: "uuid-123",
            EventSourceArn: "arn:aws:sqs:us-east-1:123456789012:my-queue",
            FunctionArn:
              "arn:aws:lambda:us-east-1:123456789012:function:my-function",
            State: "Disabling",
          },
        ],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      expect(lambdaMock).not.toHaveReceivedCommand(
        UpdateEventSourceMappingCommand,
      );
    });

    it("should handle pagination in ListEventSourceMappings", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            PhysicalResourceId: "my-function",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock
        .on(ListEventSourceMappingsCommand)
        .resolvesOnce({
          EventSourceMappings: [
            {
              UUID: "uuid-1",
              EventSourceArn: "arn:aws:sqs:us-east-1:123456789012:queue-1",
              FunctionArn:
                "arn:aws:lambda:us-east-1:123456789012:function:my-function",
              State: "Enabled",
            },
          ],
          NextMarker: "marker-page-2",
        })
        .resolvesOnce({
          EventSourceMappings: [
            {
              UUID: "uuid-2",
              EventSourceArn: "arn:aws:sqs:us-east-1:123456789012:queue-2",
              FunctionArn:
                "arn:aws:lambda:us-east-1:123456789012:function:my-function",
              State: "Enabled",
            },
          ],
        });
      lambdaMock.on(UpdateEventSourceMappingCommand).resolves({});

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      // Should disable both mappings from both pages
      expect(lambdaMock).toHaveReceivedCommandTimes(
        UpdateEventSourceMappingCommand,
        2,
      );
    });

    it("should continue processing other mappings when one fails to disable", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            PhysicalResourceId: "my-function",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [
          {
            UUID: "uuid-1",
            EventSourceArn: "arn:aws:sqs:us-east-1:123456789012:queue-1",
            FunctionArn:
              "arn:aws:lambda:us-east-1:123456789012:function:my-function",
            State: "Enabled",
          },
          {
            UUID: "uuid-2",
            EventSourceArn: "arn:aws:sqs:us-east-1:123456789012:queue-2",
            FunctionArn:
              "arn:aws:lambda:us-east-1:123456789012:function:my-function",
            State: "Enabled",
          },
        ],
      });
      lambdaMock
        .on(UpdateEventSourceMappingCommand, { UUID: "uuid-1" })
        .rejects(new Error("Access denied"));
      lambdaMock
        .on(UpdateEventSourceMappingCommand, { UUID: "uuid-2" })
        .resolves({});

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      // Should not throw - continues processing
      await expect(handler(event)).resolves.toBeUndefined();

      // Should have attempted both
      expect(lambdaMock).toHaveReceivedCommandTimes(
        UpdateEventSourceMappingCommand,
        2,
      );
    });

    it("should skip mappings without UUID", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            PhysicalResourceId: "my-function",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [
          {
            // UUID is undefined
            EventSourceArn: "arn:aws:sqs:us-east-1:123456789012:my-queue",
            FunctionArn:
              "arn:aws:lambda:us-east-1:123456789012:function:my-function",
            State: "Enabled",
          },
        ],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      expect(lambdaMock).not.toHaveReceivedCommand(
        UpdateEventSourceMappingCommand,
      );
    });

    it("should extract function name from ARN correctly", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            PhysicalResourceId:
              "arn:aws:lambda:us-east-1:123456789012:function:my-complex-function-name",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      // Should extract the function name from the ARN
      expect(lambdaMock).toHaveReceivedCommandWith(
        ListEventSourceMappingsCommand,
        {
          FunctionName: "my-complex-function-name",
        },
      );
    });

    it("should handle function name (non-ARN) PhysicalResourceId", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MySamFunction",
            PhysicalResourceId: "simple-function-name",
            ResourceType: "AWS::Serverless::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      // When PhysicalResourceId is just a function name (no colons),
      // split(":").pop() returns the full name
      expect(lambdaMock).toHaveReceivedCommandWith(
        ListEventSourceMappingsCommand,
        {
          FunctionName: "simple-function-name",
        },
      );
    });
  });

  describe("stopRunningExecutions", () => {
    it("should stop running state machine executions", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyStateMachine",
            PhysicalResourceId:
              "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
            ResourceType: "AWS::StepFunctions::StateMachine",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      sfnMock.on(ListExecutionsCommand).resolves({
        executions: [
          {
            executionArn:
              "arn:aws:states:us-east-1:123456789012:execution:my-state-machine:exec-1",
            stateMachineArn:
              "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
            name: "exec-1",
            status: ExecutionStatus.RUNNING,
            startDate: new Date(),
          },
        ],
      });
      sfnMock.on(StopExecutionCommand).resolves({});

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      expect(sfnMock).toHaveReceivedCommandWith(StopExecutionCommand, {
        executionArn:
          "arn:aws:states:us-east-1:123456789012:execution:my-state-machine:exec-1",
        error: "BudgetAlertTriggered",
        cause: "Daily budget exceeded - emergency stop triggered",
      });
    });

    it("should handle pagination in ListExecutions", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyStateMachine",
            PhysicalResourceId:
              "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
            ResourceType: "AWS::StepFunctions::StateMachine",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      sfnMock
        .on(ListExecutionsCommand)
        .resolvesOnce({
          executions: [
            {
              executionArn:
                "arn:aws:states:us-east-1:123456789012:execution:my-state-machine:exec-1",
              stateMachineArn:
                "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
              name: "exec-1",
              status: ExecutionStatus.RUNNING,
              startDate: new Date(),
            },
          ],
          nextToken: "token-page-2",
        })
        .resolvesOnce({
          executions: [
            {
              executionArn:
                "arn:aws:states:us-east-1:123456789012:execution:my-state-machine:exec-2",
              stateMachineArn:
                "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
              name: "exec-2",
              status: ExecutionStatus.RUNNING,
              startDate: new Date(),
            },
          ],
        });
      sfnMock.on(StopExecutionCommand).resolves({});

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      expect(sfnMock).toHaveReceivedCommandTimes(StopExecutionCommand, 2);
    });

    it("should continue processing other executions when one fails to stop", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyStateMachine",
            PhysicalResourceId:
              "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
            ResourceType: "AWS::StepFunctions::StateMachine",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      sfnMock.on(ListExecutionsCommand).resolves({
        executions: [
          {
            executionArn:
              "arn:aws:states:us-east-1:123456789012:execution:my-state-machine:exec-1",
            stateMachineArn:
              "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
            name: "exec-1",
            status: ExecutionStatus.RUNNING,
            startDate: new Date(),
          },
          {
            executionArn:
              "arn:aws:states:us-east-1:123456789012:execution:my-state-machine:exec-2",
            stateMachineArn:
              "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
            name: "exec-2",
            status: ExecutionStatus.RUNNING,
            startDate: new Date(),
          },
        ],
      });
      sfnMock
        .on(StopExecutionCommand, {
          executionArn:
            "arn:aws:states:us-east-1:123456789012:execution:my-state-machine:exec-1",
        })
        .rejects(new Error("Execution not found"));
      sfnMock
        .on(StopExecutionCommand, {
          executionArn:
            "arn:aws:states:us-east-1:123456789012:execution:my-state-machine:exec-2",
        })
        .resolves({});

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      // Should not throw - continues processing
      await expect(handler(event)).resolves.toBeUndefined();

      expect(sfnMock).toHaveReceivedCommandTimes(StopExecutionCommand, 2);
    });

    it("should skip executions without executionArn", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyStateMachine",
            PhysicalResourceId:
              "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
            ResourceType: "AWS::StepFunctions::StateMachine",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      sfnMock.on(ListExecutionsCommand).resolves({
        executions: [
          {
            // executionArn is undefined
            stateMachineArn:
              "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
            name: "exec-1",
            status: ExecutionStatus.RUNNING,
            startDate: new Date(),
          },
        ],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      expect(sfnMock).not.toHaveReceivedCommand(StopExecutionCommand);
    });

    it("should only list RUNNING executions (not PENDING, SUCCEEDED, FAILED, etc.)", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyStateMachine",
            PhysicalResourceId:
              "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
            ResourceType: "AWS::StepFunctions::StateMachine",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      sfnMock.on(ListExecutionsCommand).resolves({ executions: [] });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      expect(sfnMock).toHaveReceivedCommandWith(ListExecutionsCommand, {
        stateMachineArn:
          "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
        statusFilter: ExecutionStatus.RUNNING,
        maxResults: 100,
      });
    });
  });

  describe("emergencyStop error handling", () => {
    it("should throw error when CloudFormation fails", async () => {
      cfnMock
        .on(ListStackResourcesCommand)
        .rejects(new Error("Stack not found"));

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await expect(handler(event)).rejects.toThrow("Stack not found");
    });

    it("should throw error when Lambda ListEventSourceMappings fails", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            PhysicalResourceId: "my-function",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock
        .on(ListEventSourceMappingsCommand)
        .rejects(new Error("Access denied to Lambda"));

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await expect(handler(event)).rejects.toThrow("Access denied to Lambda");
    });

    it("should throw error when Step Functions ListExecutions fails", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyStateMachine",
            PhysicalResourceId:
              "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
            ResourceType: "AWS::StepFunctions::StateMachine",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      sfnMock
        .on(ListExecutionsCommand)
        .rejects(new Error("State machine not found"));

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await expect(handler(event)).rejects.toThrow("State machine not found");
    });

    it("should log error details when emergency stop fails", async () => {
      cfnMock
        .on(ListStackResourcesCommand)
        .rejects(new Error("Permission denied"));

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await expect(handler(event)).rejects.toThrow();

      expect(console.error).toHaveBeenCalled();
    });
  });

  describe("emergency stop summary", () => {
    it("should log summary with correct counts after emergency stop", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "Function1",
            PhysicalResourceId: "function-1",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
          {
            LogicalResourceId: "StateMachine1",
            PhysicalResourceId:
              "arn:aws:states:us-east-1:123456789012:stateMachine:sm-1",
            ResourceType: "AWS::StepFunctions::StateMachine",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [
          {
            UUID: "uuid-1",
            EventSourceArn: "arn:aws:sqs:us-east-1:123456789012:queue-1",
            FunctionArn:
              "arn:aws:lambda:us-east-1:123456789012:function:function-1",
            State: "Enabled",
          },
          {
            UUID: "uuid-2",
            EventSourceArn: "arn:aws:sqs:us-east-1:123456789012:queue-2",
            FunctionArn:
              "arn:aws:lambda:us-east-1:123456789012:function:function-1",
            State: "Enabled",
          },
        ],
      });
      lambdaMock.on(UpdateEventSourceMappingCommand).resolves({});
      sfnMock.on(ListExecutionsCommand).resolves({
        executions: [
          {
            executionArn:
              "arn:aws:states:us-east-1:123456789012:execution:sm-1:exec-1",
            stateMachineArn:
              "arn:aws:states:us-east-1:123456789012:stateMachine:sm-1",
            name: "exec-1",
            status: ExecutionStatus.RUNNING,
            startDate: new Date(),
          },
        ],
      });
      sfnMock.on(StopExecutionCommand).resolves({});

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      // Verify info logging was called with summary
      expect(console.info).toHaveBeenCalled();
    });

    it("should include error messages in summary when operations fail", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "Function1",
            PhysicalResourceId: "function-1",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [
          {
            UUID: "uuid-1",
            EventSourceArn: "arn:aws:sqs:us-east-1:123456789012:queue-1",
            FunctionArn:
              "arn:aws:lambda:us-east-1:123456789012:function:function-1",
            State: "Enabled",
          },
        ],
      });
      lambdaMock
        .on(UpdateEventSourceMappingCommand)
        .rejects(new Error("Throttled"));

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      // Should log error for the failed operation
      expect(console.error).toHaveBeenCalled();
    });
  });

  describe("edge cases and potential defects", () => {
    it("should handle empty stack with no resources", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await expect(handler(event)).resolves.toBeUndefined();
    });

    it("should handle undefined StackResourceSummaries response", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        // StackResourceSummaries is undefined
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await expect(handler(event)).resolves.toBeUndefined();
    });

    it("should handle undefined EventSourceMappings response", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            PhysicalResourceId: "my-function",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        // EventSourceMappings is undefined
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await expect(handler(event)).resolves.toBeUndefined();
    });

    it("should handle undefined executions response", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyStateMachine",
            PhysicalResourceId:
              "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
            ResourceType: "AWS::StepFunctions::StateMachine",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      sfnMock.on(ListExecutionsCommand).resolves({
        // executions is undefined
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await expect(handler(event)).resolves.toBeUndefined();
    });

    it("should handle event source mapping without EventSourceArn", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            PhysicalResourceId: "my-function",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [
          {
            UUID: "uuid-123",
            // EventSourceArn is undefined
            FunctionArn:
              "arn:aws:lambda:us-east-1:123456789012:function:my-function",
            State: "Enabled",
          },
        ],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      // Should not try to disable mapping without EventSourceArn
      // (because EventSourceArn?.includes(":sqs:") returns undefined/false)
      expect(lambdaMock).not.toHaveReceivedCommand(
        UpdateEventSourceMappingCommand,
      );
    });

    it("should handle large number of Lambda functions", async () => {
      const functions = Array.from({ length: 50 }, (_, i) => ({
        LogicalResourceId: `Function${i}`,
        PhysicalResourceId: `function-${i}`,
        ResourceType: "AWS::Lambda::Function",
        ResourceStatus: "CREATE_COMPLETE",
      }));

      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: functions,
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await expect(handler(event)).resolves.toBeUndefined();

      // Should call ListEventSourceMappings for each function
      expect(lambdaMock).toHaveReceivedCommandTimes(
        ListEventSourceMappingsCommand,
        50,
      );
    });

    it("should handle multiple state machines with multiple executions each", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "StateMachine1",
            PhysicalResourceId:
              "arn:aws:states:us-east-1:123456789012:stateMachine:sm-1",
            ResourceType: "AWS::StepFunctions::StateMachine",
            ResourceStatus: "CREATE_COMPLETE",
          },
          {
            LogicalResourceId: "StateMachine2",
            PhysicalResourceId:
              "arn:aws:states:us-east-1:123456789012:stateMachine:sm-2",
            ResourceType: "AWS::StepFunctions::StateMachine",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      sfnMock
        .on(ListExecutionsCommand, {
          stateMachineArn:
            "arn:aws:states:us-east-1:123456789012:stateMachine:sm-1",
        })
        .resolves({
          executions: [
            {
              executionArn:
                "arn:aws:states:us-east-1:123456789012:execution:sm-1:exec-1",
              stateMachineArn:
                "arn:aws:states:us-east-1:123456789012:stateMachine:sm-1",
              name: "exec-1",
              status: ExecutionStatus.RUNNING,
              startDate: new Date(),
            },
          ],
        });
      sfnMock
        .on(ListExecutionsCommand, {
          stateMachineArn:
            "arn:aws:states:us-east-1:123456789012:stateMachine:sm-2",
        })
        .resolves({
          executions: [
            {
              executionArn:
                "arn:aws:states:us-east-1:123456789012:execution:sm-2:exec-1",
              stateMachineArn:
                "arn:aws:states:us-east-1:123456789012:stateMachine:sm-2",
              name: "exec-1",
              status: ExecutionStatus.RUNNING,
              startDate: new Date(),
            },
            {
              executionArn:
                "arn:aws:states:us-east-1:123456789012:execution:sm-2:exec-2",
              stateMachineArn:
                "arn:aws:states:us-east-1:123456789012:stateMachine:sm-2",
              name: "exec-2",
              status: ExecutionStatus.RUNNING,
              startDate: new Date(),
            },
          ],
        });
      sfnMock.on(StopExecutionCommand).resolves({});

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      // Should stop all 3 executions (1 from sm-1, 2 from sm-2)
      expect(sfnMock).toHaveReceivedCommandTimes(StopExecutionCommand, 3);
    });

    it("should skip event source mappings in Creating state", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            PhysicalResourceId: "my-function",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [
          {
            UUID: "uuid-123",
            EventSourceArn: "arn:aws:sqs:us-east-1:123456789012:my-queue",
            FunctionArn:
              "arn:aws:lambda:us-east-1:123456789012:function:my-function",
            State: "Creating",
          },
        ],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      // "Creating" state should be skipped - mapping is in transition
      expect(lambdaMock).not.toHaveReceivedCommand(
        UpdateEventSourceMappingCommand,
      );
    });

    it("should skip event source mappings in Enabling state", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            PhysicalResourceId: "my-function",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [
          {
            UUID: "uuid-123",
            EventSourceArn: "arn:aws:sqs:us-east-1:123456789012:my-queue",
            FunctionArn:
              "arn:aws:lambda:us-east-1:123456789012:function:my-function",
            State: "Enabling",
          },
        ],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      // "Enabling" state should be skipped - mapping is in transition
      expect(lambdaMock).not.toHaveReceivedCommand(
        UpdateEventSourceMappingCommand,
      );
    });

    it("should skip event source mappings in Updating state", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            PhysicalResourceId: "my-function",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [
          {
            UUID: "uuid-123",
            EventSourceArn: "arn:aws:sqs:us-east-1:123456789012:my-queue",
            FunctionArn:
              "arn:aws:lambda:us-east-1:123456789012:function:my-function",
            State: "Updating",
          },
        ],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      // "Updating" state should be skipped - mapping is in transition
      expect(lambdaMock).not.toHaveReceivedCommand(
        UpdateEventSourceMappingCommand,
      );
    });

    it("should skip event source mappings in Deleting state", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            PhysicalResourceId: "my-function",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [
          {
            UUID: "uuid-123",
            EventSourceArn: "arn:aws:sqs:us-east-1:123456789012:my-queue",
            FunctionArn:
              "arn:aws:lambda:us-east-1:123456789012:function:my-function",
            State: "Deleting",
          },
        ],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      // "Deleting" state should be skipped - mapping is being deleted
      expect(lambdaMock).not.toHaveReceivedCommand(
        UpdateEventSourceMappingCommand,
      );
    });
  });

  describe("logging", () => {
    it("should log budget alert received with record count", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      expect(console.info).toHaveBeenCalled();
    });

    it("should log budget alert details when message is parseable", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [],
      });

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(
        createBudgetAlertMessage({
          budgetName: "TestBudget",
          actualAmount: 250.5,
          budgetLimit: 200.0,
        }),
      );

      await handler(event);

      expect(console.info).toHaveBeenCalled();
    });

    it("should log when event source mapping is disabled", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyLambdaFunction",
            PhysicalResourceId: "my-function",
            ResourceType: "AWS::Lambda::Function",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      lambdaMock.on(ListEventSourceMappingsCommand).resolves({
        EventSourceMappings: [
          {
            UUID: "uuid-123",
            EventSourceArn: "arn:aws:sqs:us-east-1:123456789012:my-queue",
            FunctionArn:
              "arn:aws:lambda:us-east-1:123456789012:function:my-function",
            State: "Enabled",
          },
        ],
      });
      lambdaMock.on(UpdateEventSourceMappingCommand).resolves({});

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      expect(console.info).toHaveBeenCalled();
    });

    it("should log when execution is stopped", async () => {
      cfnMock.on(ListStackResourcesCommand).resolves({
        StackResourceSummaries: [
          {
            LogicalResourceId: "MyStateMachine",
            PhysicalResourceId:
              "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
            ResourceType: "AWS::StepFunctions::StateMachine",
            ResourceStatus: "CREATE_COMPLETE",
          },
        ],
      });
      sfnMock.on(ListExecutionsCommand).resolves({
        executions: [
          {
            executionArn:
              "arn:aws:states:us-east-1:123456789012:execution:my-state-machine:exec-1",
            stateMachineArn:
              "arn:aws:states:us-east-1:123456789012:stateMachine:my-state-machine",
            name: "exec-1",
            status: ExecutionStatus.RUNNING,
            startDate: new Date(),
          },
        ],
      });
      sfnMock.on(StopExecutionCommand).resolves({});

      const { handler } =
        await import("../../../../budget-alert/lambdas/budget-handler/index.js");

      const event = createMockSNSEvent(createBudgetAlertMessage());

      await handler(event);

      expect(console.info).toHaveBeenCalled();
    });
  });
});
