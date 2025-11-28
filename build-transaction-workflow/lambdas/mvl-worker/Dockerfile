# Use the AWS provided Node.js 22 base image for Lambda
FROM public.ecr.aws/lambda/nodejs:22

# Copy shared dependencies first
COPY lambdas/shared/ ${LAMBDA_TASK_ROOT}/shared/

# Copy function code
COPY lambdas/mvl-worker/package.json ${LAMBDA_TASK_ROOT}/
COPY lambdas/mvl-worker/index.mjs ${LAMBDA_TASK_ROOT}/

# Install dependencies
RUN npm install

# Set the CMD to your handler
CMD [ "index.handler" ]
