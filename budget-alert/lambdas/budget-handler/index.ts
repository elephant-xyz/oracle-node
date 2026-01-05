import type { SNSEvent, SNSHandler } from "aws-lambda";

/**
 * Budget alert message structure from AWS Budgets.
 */
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

/**
 * Creates a structured log entry for budget alert processing.
 */
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

/**
 * Parses the SNS message body from AWS Budgets.
 */
const parseBudgetMessage = (message: string): BudgetAlertMessage | null => {
  try {
    return JSON.parse(message) as BudgetAlertMessage;
  } catch {
    return null;
  }
};

/**
 * Main handler for SNS events from AWS Budget alerts.
 * Triggered when daily spending exceeds the configured threshold.
 */
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

    console.info("hello world");

    console.info(
      createLogEntry("budget_alert_processed", {
        messageId: record.Sns.MessageId,
      }),
    );
  }
};
