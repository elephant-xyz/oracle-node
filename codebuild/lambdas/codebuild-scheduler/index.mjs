/**
 * CodeBuild Scheduler Lambda: Checks if CodeBuild is running and starts a new build if not.
 * Triggered by EventBridge every 30 minutes.
 */

import {
  CodeBuildClient,
  ListBuildsForProjectCommand,
  StartBuildCommand,
  BatchGetBuildsCommand,
} from "@aws-sdk/client-codebuild";

/**
 * @typedef {Object} EventBridgeEvent
 * @property {string} version
 * @property {string} id
 * @property {string} source
 * @property {string} account
 * @property {string} time
 */

const codebuild = new CodeBuildClient({});

/**
 * Checks if there are any running CodeBuild builds for the project.
 *
 * @param {string} projectName - CodeBuild project name
 * @returns {Promise<boolean>} True if builds are running, false otherwise
 */
async function hasRunningBuilds(projectName) {
  try {
    const command = new ListBuildsForProjectCommand({
      projectName: projectName,
      sortOrder: "DESCENDING",
    });
    const response = await codebuild.send(command);

    if (!response.ids || response.ids.length === 0) {
      return false;
    }

    // Get details for the most recent builds (limit to 10 for efficiency)
    const buildIds = response.ids.slice(0, 10);
    const batchCommand = new BatchGetBuildsCommand({
      ids: buildIds,
    });
    const batchResponse = await codebuild.send(batchCommand);

    if (!batchResponse.builds || batchResponse.builds.length === 0) {
      return false;
    }

    // Check if any build is in progress
    const inProgressBuilds = batchResponse.builds.filter(
      (build) =>
        build.buildStatus === "IN_PROGRESS" ||
        build.buildStatus === "IN_PROGRESS_PENDING",
    );

    return inProgressBuilds.length > 0;
  } catch (error) {
    console.error(
      JSON.stringify({
        level: "error",
        msg: "failed_to_check_running_builds",
        error: error.message,
        projectName: projectName,
      }),
    );
    // If we can't check, assume builds are running to be safe
    throw error;
  }
}

/**
 * Starts a new CodeBuild build.
 *
 * @param {string} projectName - CodeBuild project name
 * @returns {Promise<string>} Build ID
 */
async function startBuild(projectName) {
  try {
    const command = new StartBuildCommand({
      projectName: projectName,
    });
    const response = await codebuild.send(command);
    return response.build?.id || "unknown";
  } catch (error) {
    console.error(
      JSON.stringify({
        level: "error",
        msg: "failed_to_start_build",
        error: error.message,
        projectName: projectName,
      }),
    );
    throw error;
  }
}

/**
 * Lambda handler for CodeBuild scheduler.
 *
 * @param {EventBridgeEvent} event - EventBridge scheduled event
 * @returns {Promise<{statusCode: number, body: string}>}
 */
export const handler = async (event) => {
  const logBase = {
    component: "codebuild-scheduler",
    at: new Date().toISOString(),
    eventId: event.id || "unknown",
  };

  try {
    const projectName = process.env.CODEBUILD_PROJECT_NAME;
    if (!projectName) {
      throw new Error(
        "CODEBUILD_PROJECT_NAME environment variable is required",
      );
    }

    console.log(
      JSON.stringify({
        ...logBase,
        level: "info",
        msg: "scheduler_invoked",
        projectName: projectName,
      }),
    );

    // Check if there are any running builds
    const running = await hasRunningBuilds(projectName);

    if (running) {
      console.log(
        JSON.stringify({
          ...logBase,
          level: "info",
          msg: "build_already_running",
          projectName: projectName,
          action: "skipped",
        }),
      );
      return {
        statusCode: 200,
        body: JSON.stringify({
          message: "CodeBuild is already running, skipping start",
          projectName: projectName,
        }),
      };
    }

    // Start a new build
    const buildId = await startBuild(projectName);
    console.log(
      JSON.stringify({
        ...logBase,
        level: "info",
        msg: "build_started",
        projectName: projectName,
        buildId: buildId,
        action: "started",
      }),
    );

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "CodeBuild started successfully",
        projectName: projectName,
        buildId: buildId,
      }),
    };
  } catch (error) {
    console.error(
      JSON.stringify({
        ...logBase,
        level: "error",
        msg: "scheduler_error",
        error: error.message,
        stack: error.stack,
      }),
    );
    throw error;
  }
};
