import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from "@aws-sdk/client-secrets-manager";
import { Octokit } from "@octokit/rest";
import { throttling } from "@octokit/plugin-throttling";
import AdmZip from "adm-zip";
import { promises as fs } from "fs";
import path from "path";
import os from "os";

const s3Client = new S3Client({});
const secretsClient = new SecretsManagerClient({});

const MyOctokit = Octokit.plugin(throttling);

/**
 * @typedef {InstanceType<typeof MyOctokit>} OctokitClient
 * @typedef {Awaited<ReturnType<OctokitClient["rest"]["users"]["getAuthenticated"]>>["data"]} GitHubAuthenticatedUser
 * @typedef {{ owner: string, repo: string }} ForkRepositoryReference
 * @typedef {{ path: string, content: Buffer }} ScriptFile
 */

/**
 * @typedef {object} S3EventRecord
 * @property {object} s3
 * @property {object} s3.bucket
 * @property {string} s3.bucket.name
 * @property {object} s3.object
 * @property {string} s3.object.key
 */

/**
 * @typedef {object} S3Event
 * @property {S3EventRecord[]} Records
 */

/**
 * Retrieve GitHub token from AWS Secrets Manager.
 *
 * @param {string} secretName - Name of the secret in Secrets Manager.
 * @returns {Promise<string>} - The GitHub token.
 */
async function getGitHubToken(secretName) {
  try {
    const response = await secretsClient.send(
      new GetSecretValueCommand({
        SecretId: secretName,
      }),
    );

    if (response.SecretString) {
      try {
        const secret = JSON.parse(response.SecretString);
        return (
          secret.token ||
          secret.GITHUB_TOKEN ||
          secret.github_token ||
          response.SecretString
        );
      } catch {
        // Not JSON, return as-is
        return response.SecretString;
      }
    }

    if (response.SecretBinary) {
      return Buffer.from(response.SecretBinary).toString("utf8");
    }

    throw new Error("Secret value is empty");
  } catch (error) {
    console.error("Failed to retrieve GitHub token:", error);
    throw new Error(`Failed to retrieve GitHub token: ${error.message}`);
  }
}

/**
 * Initialize GitHub client with throttling plugin.
 *
 * @param {string} token - GitHub personal access token.
 * @returns {OctokitClient} - Configured Octokit instance.
 */
function createGitHubClient(token) {
  return new MyOctokit({
    auth: token,
    throttle: {
      onRateLimit: (retryAfter, options) => {
        console.warn(
          `Request quota exhausted for request ${options.method} ${options.url}`,
        );

        if (options.request.retryCount <= 2) {
          console.log(`Retrying after ${retryAfter} seconds!`);
          return true;
        }
        return false;
      },
      onSecondaryRateLimit: (retryAfter, options) => {
        console.warn(
          `Secondary quota detected for request ${options.method} ${options.url}`,
        );
      },
    },
  });
}

/**
 * Ensure the repository fork exists, creating it if necessary.
 *
 * @param {OctokitClient} octokit - GitHub client.
 * @param {string} upstreamOwner - Owner of the upstream repository.
 * @param {string} upstreamRepo - Name of the upstream repository.
 * @param {GitHubAuthenticatedUser} authenticatedUser - Authenticated GitHub user derived from the token.
 * @returns {Promise<ForkRepositoryReference>} - Fork repository details.
 */
async function ensureForkExists(
  octokit,
  upstreamOwner,
  upstreamRepo,
  authenticatedUser,
) {
  const githubUsername = authenticatedUser.login;
  try {
    // Check if fork already exists
    try {
      const forkResponse = await octokit.rest.repos.get({
        owner: githubUsername,
        repo: upstreamRepo,
      });

      if (forkResponse.data.fork) {
        console.log(`Fork already exists: ${githubUsername}/${upstreamRepo}`);
        return {
          owner: githubUsername,
          repo: upstreamRepo,
        };
      }
    } catch (error) {
      if (error.status !== 404) {
        throw error;
      }
      // Fork doesn't exist, continue to create it
    }

    // Create fork
    console.log(
      `Creating fork of ${upstreamOwner}/${upstreamRepo} for ${githubUsername}`,
    );
    const forkResponse = await octokit.rest.repos.createFork({
      owner: upstreamOwner,
      repo: upstreamRepo,
    });

    console.log(`Fork created: ${forkResponse.data.full_name}`);
    return {
      owner: forkResponse.data.owner.login,
      repo: forkResponse.data.name,
    };
  } catch (error) {
    if (error.status === 422 && error.message?.includes("already exists")) {
      console.log(`Fork already exists: ${githubUsername}/${upstreamRepo}`);
      return {
        owner: githubUsername,
        repo: upstreamRepo,
      };
    }
    throw error;
  }
}

/**
 * Extract county name from S3 key.
 *
 * @param {string} s3Key - S3 object key (e.g., "transforms/brevard.zip").
 * @returns {string} - County name extracted from the key.
 */
function getCountyNameFromS3Key(s3Key) {
  // Extract county name from transforms/<county>.zip
  const match = s3Key.match(/^transforms\/([^/]+)\.zip$/);
  if (!match) {
    throw new Error(`Invalid S3 key format: ${s3Key}`);
  }
  return match[1];
}

/**
 * Normalize county name to match GitHub repository directory structure.
 *
 * @param {OctokitClient} octokit - GitHub client.
 * @param {string} upstreamOwner - Owner of the upstream repository.
 * @param {string} upstreamRepo - Name of the upstream repository.
 * @param {string} countyName - County name from S3 key.
 * @returns {Promise<string>} - Normalized county name matching repo structure.
 */
async function normalizeCountyName(
  octokit,
  upstreamOwner,
  upstreamRepo,
  countyName,
) {
  try {
    // Get repository contents to check actual directory structure
    const { data: contents } = await octokit.rest.repos.getContent({
      owner: upstreamOwner,
      repo: upstreamRepo,
      path: "",
    });

    if (!Array.isArray(contents)) {
      return countyName;
    }

    // Look for directory matching county name (case-insensitive)
    const countyLower = countyName.toLowerCase();
    for (const item of contents) {
      if (item.type === "dir" && item.name.toLowerCase() === countyLower) {
        return item.name; // Return the actual directory name from repo
      }
    }

    // If not found, try variations (spaces, underscores, etc.)
    const variations = [
      countyName,
      countyName.replace(/\s+/g, ""),
      countyName.replace(/\s+/g, "_"),
      countyName.replace(/\s+/g, "-"),
    ];

    for (const variation of variations) {
      for (const item of contents) {
        if (
          item.type === "dir" &&
          item.name.toLowerCase() === variation.toLowerCase()
        ) {
          return item.name;
        }
      }
    }

    // If still not found, return original county name
    console.warn(
      `County directory not found in repo for "${countyName}", using as-is`,
    );
    return countyName;
  } catch (error) {
    console.warn(
      `Failed to normalize county name "${countyName}": ${error.message}`,
    );
    return countyName; // Fallback to original
  }
}

/**
 * Sanitize county name for use as a Git branch name.
 * Converts invalid characters to valid ones and ensures compliance with Git ref naming rules.
 *
 * @param {string} countyName - County name to sanitize.
 * @returns {string} - Sanitized branch name safe for Git refs.
 */
function sanitizeBranchName(countyName) {
  // Replace spaces with hyphens
  let sanitized = countyName.replace(/\s+/g, "-");

  // Remove or replace invalid Git ref characters
  // Invalid characters: ~, ^, :, ?, *, [, \, .., @{, space (already handled)
  sanitized = sanitized
    .replace(/[~^:?*[\\]/g, "-") // Replace invalid chars with hyphens
    .replace(/\.\./g, "-") // Replace consecutive dots
    .replace(/@{/g, "-") // Replace @{ sequences
    .replace(/\.$/g, "") // Remove trailing dots
    .replace(/\.lock$/g, "") // Remove .lock suffix
    .replace(/^-+|-+$/g, "") // Remove leading/trailing hyphens
    .replace(/-+/g, "-"); // Replace multiple consecutive hyphens with single hyphen

  // Convert to lowercase for consistency
  sanitized = sanitized.toLowerCase();

  // Ensure it's not empty
  if (!sanitized || sanitized.length === 0) {
    sanitized = "unnamed";
  }

  return sanitized;
}

/**
 * Download and extract zip file from S3.
 *
 * @param {string} bucket - S3 bucket name.
 * @param {string} key - S3 object key.
 * @param {string} extractDir - Directory to extract files to.
 * @returns {Promise<void>}
 */
async function downloadAndExtractZip(bucket, key, extractDir) {
  const getObjectResponse = await s3Client.send(
    new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    }),
  );

  if (!getObjectResponse.Body) {
    throw new Error(`Empty response body for ${bucket}/${key}`);
  }

  const zipBuffer = await getObjectResponse.Body.transformToByteArray();
  const zip = new AdmZip(Buffer.from(zipBuffer));
  zip.extractAllTo(extractDir, true);
}

/**
 * Get all JavaScript files from extracted directory.
 *
 * @param {string} dir - Directory to search.
 * @param {string} baseDir - Base directory for relative paths.
 * @returns {Promise<ScriptFile[]>} - Array of file paths and contents.
 */
async function getScriptFiles(dir, baseDir = dir) {
  const files = [];
  const entries = await fs.readdir(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      const subFiles = await getScriptFiles(fullPath, baseDir);
      files.push(...subFiles);
    } else if (entry.isFile() && entry.name.endsWith(".js")) {
      const relativePath = path.relative(baseDir, fullPath);
      const content = await fs.readFile(fullPath);
      files.push({
        path: relativePath,
        content: content,
      });
    }
  }

  return files;
}

/**
 * Ensure branch exists, creating it if necessary.
 *
 * @param {OctokitClient} octokit - GitHub client.
 * @param {string} owner - Repository owner.
 * @param {string} repo - Repository name.
 * @param {string} branchName - Branch name.
 * @param {string} baseBranch - Base branch to create from.
 * @returns {Promise<void>}
 */
async function ensureBranchExists(
  octokit,
  owner,
  repo,
  branchName,
  baseBranch = "main",
) {
  try {
    // Check if branch exists
    await octokit.rest.repos.getBranch({
      owner,
      repo,
      branch: branchName,
    });
    console.log(`Branch ${branchName} already exists`);
    return;
  } catch (error) {
    if (error.status !== 404) {
      throw error;
    }
    // Branch doesn't exist, create it
  }

  // Get SHA of base branch
  const { data: baseBranchData } = await octokit.rest.repos.getBranch({
    owner,
    repo,
    branch: baseBranch,
  });

  // Create branch
  await octokit.rest.git.createRef({
    owner,
    repo,
    ref: `refs/heads/${branchName}`,
    sha: baseBranchData.commit.sha,
  });

  console.log(`Created branch ${branchName}`);
}

/**
 * Sync scripts to GitHub repository.
 *
 * @param {OctokitClient} octokit - GitHub client.
 * @param {string} owner - Repository owner.
 * @param {string} repo - Repository name.
 * @param {string} branch - Branch name.
 * @param {string} countyName - County name.
 * @param {ScriptFile[]} files - Files to upload.
 * @returns {Promise<string>} - Commit SHA.
 */
async function syncScriptsToRepo(
  octokit,
  owner,
  repo,
  branch,
  countyName,
  files,
) {
  const operations = [];
  const targetPaths = new Set();

  for (const file of files) {
    // Handle zip files that may already contain nested directory structures
    // Extract content after the last "scripts/" directory to avoid duplicate nesting
    let githubPath;
    const normalizedPath = file.path.replace(/\\/g, "/"); // Normalize path separators

    // Find the last occurrence of "scripts/" in the path
    const lastScriptsIndex = normalizedPath.lastIndexOf("scripts/");

    if (lastScriptsIndex !== -1) {
      // Extract everything after the last "scripts/" directory
      const filePathAfterScripts = normalizedPath.substring(
        lastScriptsIndex + "scripts/".length,
      );
      // Prepend county name and scripts directory
      githubPath = `${countyName}/scripts/${filePathAfterScripts}`;
    } else {
      // No "scripts/" found, check if path already starts with county name
      // Strip out any transform/<countyName> or <countyName> prefixes
      let cleanedPath = normalizedPath;

      // Remove "transform/" prefix if present
      if (cleanedPath.startsWith("transform/")) {
        cleanedPath = cleanedPath.substring("transform/".length);
      }

      // Remove county name prefix if present (case-insensitive)
      const countyPrefix = cleanedPath
        .toLowerCase()
        .startsWith(countyName.toLowerCase() + "/");
      if (countyPrefix) {
        cleanedPath = cleanedPath.substring(countyName.length + 1);
      }

      // Remove any leading "scripts/" if present
      if (cleanedPath.startsWith("scripts/")) {
        cleanedPath = cleanedPath.substring("scripts/".length);
      }

      // Prepend county name and scripts directory
      githubPath = `${countyName}/scripts/${cleanedPath}`;
    }

    // Convert content to UTF-8 string for GitHub Git Trees API
    // When using 'content' in tree entries, GitHub expects UTF-8 text (not base64)
    // GitHub will automatically create the blob from this UTF-8 content
    let contentText;
    if (Buffer.isBuffer(file.content)) {
      // Buffer: decode as UTF-8 text
      contentText = file.content.toString("utf8");
    } else if (typeof file.content === "string") {
      // Already a string: use as-is
      contentText = file.content;
    } else {
      // Fallback: convert to string
      contentText = String(file.content);
    }

    // Check if file exists
    let sha = null;
    try {
      const { data: existingFile } = await octokit.rest.repos.getContent({
        owner,
        repo,
        path: githubPath,
        ref: branch,
      });

      if (Array.isArray(existingFile)) {
        throw new Error(`Path ${githubPath} is a directory`);
      }

      if (existingFile.sha) {
        sha = existingFile.sha;
      }
    } catch (error) {
      if (error.status !== 404) {
        throw error;
      }
      // File doesn't exist, will create new
    }

    // GitHub API: For tree creation, use UTF-8 text content directly
    // GitHub will automatically create the blob from this content
    // The sha check above is just to detect if file exists, but we always use content
    // to ensure the file is updated with new content
    targetPaths.add(githubPath);
    operations.push({
      path: githubPath,
      mode: "100644",
      type: "blob",
      content: contentText,
    });
  }

  if (operations.length === 0) {
    throw new Error("No files to upload");
  }

  // Get latest commit SHA
  const { data: branchData } = await octokit.rest.repos.getBranch({
    owner,
    repo,
    branch: branch,
  });

  const { data: existingTree } = await octokit.rest.git.getTree({
    owner,
    repo,
    tree_sha: branchData.commit.sha,
    recursive: "true",
  });
  for (const entry of existingTree.tree ?? []) {
    if (
      entry.type === "blob" &&
      typeof entry.path === "string" &&
      entry.path.startsWith(`${countyName}/scripts/`) &&
      !targetPaths.has(entry.path)
    ) {
      operations.push({
        path: entry.path,
        mode: "100644",
        type: "blob",
        sha: null,
      });
    }
  }

  // Create tree
  const { data: tree } = await octokit.rest.git.createTree({
    owner,
    repo,
    base_tree: branchData.commit.sha,
    tree: operations,
  });

  // Create commit
  const { data: commit } = await octokit.rest.git.createCommit({
    owner,
    repo,
    message: `Update ${countyName} transform scripts`,
    tree: tree.sha,
    parents: [branchData.commit.sha],
  });

  // Update branch reference
  await octokit.rest.git.updateRef({
    owner,
    repo,
    ref: `heads/${branch}`,
    sha: commit.sha,
  });

  console.log(`Committed ${operations.length} files to ${branch}`);
  return commit.sha;
}

/**
 * Find existing pull request for a county.
 *
 * @param {import("@octokit/rest").Octokit} octokit - GitHub client.
 * @param {string} upstreamOwner - Upstream repository owner.
 * @param {string} upstreamRepo - Upstream repository name.
 * @param {string} forkOwner - Fork repository owner.
 * @param {string} branch - Branch name.
 * @returns {Promise<import("@octokit/rest").Octokit.PullsGetResponseData | null>} - Existing PR or null.
 */
async function findExistingPullRequest(
  octokit,
  upstreamOwner,
  upstreamRepo,
  forkOwner,
  branch,
) {
  const { data: prs } = await octokit.rest.pulls.list({
    owner: upstreamOwner,
    repo: upstreamRepo,
    head: `${forkOwner}:${branch}`,
    state: "open",
  });

  return prs.length > 0 ? prs[0] : null;
}

/**
 * Create or update pull request.
 *
 * @param {import("@octokit/rest").Octokit} octokit - GitHub client.
 * @param {string} upstreamOwner - Upstream repository owner.
 * @param {string} upstreamRepo - Upstream repository name.
 * @param {string} forkOwner - Fork repository owner.
 * @param {string} branch - Branch name.
 * @param {string} countyName - County name.
 * @param {string} s3Uri - S3 URI of the uploaded zip.
 * @returns {Promise<string>} - PR URL.
 */
async function createOrUpdatePullRequest(
  octokit,
  upstreamOwner,
  upstreamRepo,
  forkOwner,
  branch,
  countyName,
  s3Uri,
) {
  const baseBranch = "main";
  const title = `Update ${countyName} transform scripts`;
  const body = `This PR updates transform scripts for ${countyName} county.

**Source**: ${s3Uri}
**Timestamp**: ${new Date().toISOString()}

Automatically created by GitHub sync Lambda function.`;

  // Check for existing PR
  const existingPR = await findExistingPullRequest(
    octokit,
    upstreamOwner,
    upstreamRepo,
    forkOwner,
    branch,
  );

  if (existingPR) {
    console.log(`PR already exists: ${existingPR.html_url}`);
    return existingPR.html_url;
  }

  // Create new PR
  const { data: pr } = await octokit.rest.pulls.create({
    owner: upstreamOwner,
    repo: upstreamRepo,
    title: title,
    body: body,
    head: `${forkOwner}:${branch}`,
    base: baseBranch,
  });

  console.log(`Created PR: ${pr.html_url}`);
  return pr.html_url;
}

/**
 * Get authenticated user info from GitHub.
 *
 * @param {OctokitClient} octokit - GitHub client.
 * @returns {Promise<GitHubAuthenticatedUser>} - Authenticated user info returned by the GitHub API.
 */
async function getAuthenticatedUser(octokit) {
  const { data: user } = await octokit.rest.users.getAuthenticated();
  return user;
}

/**
 * Main Lambda handler.
 *
 * @param {S3Event} event - S3 event containing object creation records.
 * @returns {Promise<{statusCode: number, body: string}>}
 */
export const handler = async (event) => {
  console.log("Received event:", JSON.stringify(event, null, 2));

  const githubSecretName = process.env.GITHUB_SECRET_NAME;
  if (!githubSecretName) {
    throw new Error("GITHUB_SECRET_NAME environment variable is required");
  }
  const upstreamRepo =
    process.env.UPSTREAM_REPO || "elephant-xyz/Counties-trasform-scripts";

  const [upstreamOwner, upstreamRepoName] = upstreamRepo.split("/");
  if (!upstreamOwner || !upstreamRepoName) {
    throw new Error(`Invalid UPSTREAM_REPO format: ${upstreamRepo}`);
  }

  try {
    // Get GitHub token and create client
    const token = await getGitHubToken(githubSecretName);
    const octokit = createGitHubClient(token);

    // Resolve authenticated user details from the token
    const authenticatedUser = await getAuthenticatedUser(octokit);
    console.log(`Authenticated as GitHub user: ${authenticatedUser.login}`);

    // Process each S3 record
    for (const record of event.Records) {
      const bucket = record.s3.bucket.name;
      const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));

      // Validate it's a zip file in transforms/
      if (!key.startsWith("transforms/") || !key.endsWith(".zip")) {
        console.log(`Skipping non-transform file: ${key}`);
        continue;
      }

      const countyName = getCountyNameFromS3Key(key);
      console.log(`Processing county: ${countyName}`);

      // Ensure fork exists
      const fork = await ensureForkExists(
        octokit,
        upstreamOwner,
        upstreamRepoName,
        authenticatedUser,
      );

      // Normalize county name
      const normalizedCounty = await normalizeCountyName(
        octokit,
        upstreamOwner,
        upstreamRepoName,
        countyName,
      );

      // Download and extract zip
      const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "github-sync-"));
      try {
        await downloadAndExtractZip(bucket, key, tmpDir);

        // Get script files
        const scriptFiles = await getScriptFiles(tmpDir);

        if (scriptFiles.length === 0) {
          console.warn(`No JavaScript files found in ${key}`);
          continue;
        }

        // Sanitize county name for branch name (Git refs don't allow spaces)
        const sanitizedCounty = sanitizeBranchName(normalizedCounty);
        const branchName = `auto-update/${sanitizedCounty}`;

        // Ensure branch exists
        await ensureBranchExists(
          octokit,
          fork.owner,
          fork.repo,
          branchName,
          "main",
        );

        // Sync scripts to repo
        const commitSha = await syncScriptsToRepo(
          octokit,
          fork.owner,
          fork.repo,
          branchName,
          normalizedCounty,
          scriptFiles,
        );

        // Create or update PR
        const s3Uri = `s3://${bucket}/${key}`;
        const prUrl = await createOrUpdatePullRequest(
          octokit,
          upstreamOwner,
          upstreamRepoName,
          fork.owner,
          branchName,
          normalizedCounty,
          s3Uri,
        );

        console.log(
          `Successfully synced ${countyName}: PR ${prUrl}, Commit ${commitSha}`,
        );
      } finally {
        // Cleanup
        await fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
      }
    }

    return {
      statusCode: 200,
      body: JSON.stringify({ message: "Successfully processed S3 events" }),
    };
  } catch (error) {
    console.error("Error processing event:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: error.message || "Internal server error",
      }),
    };
  }
};
