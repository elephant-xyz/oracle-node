import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

/**
 * Environment variable for DynamoDB table name.
 */
export const TABLE_NAME = process.env.WORKFLOW_ERRORS_TABLE_NAME;

/**
 * DynamoDB Document Client with marshalling options.
 * Reuses connections via Keep-Alive for better performance.
 */
const dynamoDbClient = new DynamoDBClient({});

/**
 * Configured DynamoDB Document Client instance.
 * Removes undefined values during marshalling.
 */
export const docClient = DynamoDBDocumentClient.from(dynamoDbClient, {
  marshallOptions: {
    removeUndefinedValues: true,
  },
});
