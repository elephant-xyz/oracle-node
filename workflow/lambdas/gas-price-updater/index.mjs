/**
 * Gas Price Updater Lambda: Cron job that updates current gas price to DynamoDB
 *
 * Runs every 5 minutes to fetch the current gas price from RPC and store it in DynamoDB.
 * Step Functions read this DynamoDB item directly to decide whether to proceed with submission.
 */

import { checkGasPrice } from "@elephant-xyz/cli/lib";
import { DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";

const getLogBase = () => ({
  component: "gas-price-updater",
  at: new Date().toISOString(),
});

/**
 * Lambda handler - updates DynamoDB with current gas price from RPC
 * @returns {Promise<{gasPrice: number, updatedAt: string}>}
 */
export const handler = async () => {
  const rpcUrl = process.env.ELEPHANT_RPC_URL;
  const tableName = process.env.GAS_PRICE_TABLE_NAME;

  if (!rpcUrl) {
    const error = "RPC URL is required (ELEPHANT_RPC_URL env var)";
    console.error(
      JSON.stringify({
        ...getLogBase(),
        level: "error",
        msg: "config_validation_failed",
        error,
      }),
    );
    throw new Error(error);
  }

  if (!tableName) {
    const error =
      "DynamoDB table name is required (GAS_PRICE_TABLE_NAME env var)";
    console.error(
      JSON.stringify({
        ...getLogBase(),
        level: "error",
        msg: "config_validation_failed",
        error,
      }),
    );
    throw new Error(error);
  }

  console.log(
    JSON.stringify({
      ...getLogBase(),
      level: "info",
      msg: "fetching_gas_price",
      rpcUrl: rpcUrl.replace(/\/[^/]*$/, "/***"), // Mask API key in URL
    }),
  );

  try {
    // Get current gas price from RPC
    const gasPriceInfo = await checkGasPrice({ rpcUrl });

    // Use EIP-1559 maxFeePerGas if available, otherwise fall back to legacy gasPrice
    // Note: checkGasPrice returns values already in Gwei (may be string or number)
    const rawGasPrice =
      gasPriceInfo.eip1559?.maxFeePerGas ??
      gasPriceInfo.legacy?.gasPrice ??
      null;

    // Ensure gasPrice is always a number
    const currentGasPriceGwei =
      rawGasPrice !== null ? Number(rawGasPrice) : null;

    if (currentGasPriceGwei === null || Number.isNaN(currentGasPriceGwei)) {
      throw new Error("Unable to retrieve gas price from RPC");
    }
    const updatedAt = new Date().toISOString();

    console.log(
      JSON.stringify({
        ...getLogBase(),
        level: "info",
        msg: "gas_price_retrieved",
        gasPriceGwei: currentGasPriceGwei,
      }),
    );

    // Update DynamoDB with current gas price
    const dynamodb = new DynamoDBClient({});

    await dynamodb.send(
      new PutItemCommand({
        TableName: tableName,
        Item: {
          PK: { S: "CURRENT" },
          gasPrice: { N: String(currentGasPriceGwei) },
          updatedAt: { S: updatedAt },
        },
      }),
    );

    console.log(
      JSON.stringify({
        ...getLogBase(),
        level: "info",
        msg: "dynamodb_item_updated",
        tableName: tableName,
        gasPriceGwei: currentGasPriceGwei,
        updatedAt: updatedAt,
      }),
    );

    return {
      gasPrice: currentGasPriceGwei,
      updatedAt: updatedAt,
    };
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    console.error(
      JSON.stringify({
        ...getLogBase(),
        level: "error",
        msg: "gas_price_update_failed",
        error: errorMessage,
      }),
    );
    throw err;
  }
};
