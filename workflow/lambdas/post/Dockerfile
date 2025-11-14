# Use AWS Lambda Node.js 22 base image
FROM public.ecr.aws/lambda/nodejs:22

# Install git (required for npm to install from GitHub)
# Combine RUN commands and clean up in same layer to reduce image size
RUN dnf install -y git && dnf clean all && rm -rf /var/cache/dnf

# Set working directory
WORKDIR ${LAMBDA_TASK_ROOT}

# Copy package files first for better caching
# This layer is only invalidated when package files change
COPY package.json .npmrc ./

# Install production dependencies
# Note: The postinstall script in @elephant-xyz/cli will download ML models
# Use --force to override peer dependency conflicts with parse5
# Clear npm cache before install to ensure fresh fetch of git dependencies
# This is critical for git-based dependencies to always get latest commits from branch
RUN npm cache clean --force && \
    npm install --omit=dev --force

# Copy application source code
# This layer is invalidated whenever source code changes
# Using COPY with multiple sources in one command is more efficient
COPY index.mjs errors.mjs scripts-manager.mjs ./
COPY transforms/ ./transforms/

# Set the Lambda handler
CMD ["index.handler"]
