import {
  LambdaClient,
  GetFunctionConfigurationCommand,
  UpdateFunctionConfigurationCommand,
} from "@aws-sdk/client-lambda";

const lambdaClient = new LambdaClient({});

function utcTimestampCompact() {
  // Produces format: YYYYMMDDTHHMMSSZ (e.g., 20250125T123456Z)
  const iso = new Date().toISOString(); // e.g., 2025-01-25T12:34:56.789Z
  const noMs = iso.slice(0, 19); // 2025-01-25T12:34:56
  return noMs.replace(/[-:]/g, "") + "Z"; // 20250125T123456Z
}

/**
 * @returns {Promise<{updated: boolean, function: string, var: string, value: string}>}
 */
export const handler = async () => {
  const targetFunction = process.env.TARGET_FUNCTION_NAME || "downloader";
  const varName = process.env.VAR_NAME || "DEPLOY_TS";

  const cfg = await lambdaClient.send(
    new GetFunctionConfigurationCommand({ FunctionName: targetFunction }),
  );

  const envVars = (cfg.Environment && cfg.Environment.Variables) || {};

  const newValue = utcTimestampCompact();
  envVars[varName] = newValue;

  await lambdaClient.send(
    new UpdateFunctionConfigurationCommand({
      FunctionName: targetFunction,
      Environment: { Variables: envVars },
    }),
  );

  return {
    updated: true,
    function: targetFunction,
    var: varName,
    value: newValue,
  };
};
