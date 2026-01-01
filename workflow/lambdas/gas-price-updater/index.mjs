/**
 * Gas Price Updater Lambda: Cron job that updates current gas price to SSM Parameter Store
 *
 * Runs every 5 minutes to fetch the current gas price from RPC and store it in SSM.
 * Step Functions read this SSM parameter directly to decide whether to proceed with submission.
 */

import { checkGasPrice } from "@elephant-xyz/cli/lib";
import { SSMClient, PutParameterCommand } from "@aws-sdk/client-ssm";

const getLogBase = () => ({
  component: "gas-price-updater",
  at: new Date().toISOString(),
});

/**
 * Lambda handler - updates SSM with current gas price from RPC
 * @returns {Promise<{gasPrice: number, updatedAt: string}>}
 */
export const handler = async () => {
  const rpcUrl = process.env.ELEPHANT_RPC_URL;
  const parameterName = process.env.GAS_PRICE_PARAMETER_NAME;

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

  if (!parameterName) {
    const error =
      "SSM Parameter name is required (GAS_PRICE_PARAMETER_NAME env var)";
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

    // Update SSM Parameter with current gas price
    const ssm = new SSMClient({});
    const parameterValue = JSON.stringify({
      gasPrice: currentGasPriceGwei,
      updatedAt: updatedAt,
    });

    await ssm.send(
      new PutParameterCommand({
        Name: parameterName,
        Value: parameterValue,
        Type: "String",
        Overwrite: true,
      }),
    );

    console.log(
      JSON.stringify({
        ...getLogBase(),
        level: "info",
        msg: "ssm_parameter_updated",
        parameterName: parameterName,
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
