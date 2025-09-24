# Optimized Workflow Lambda Packages

This directory contains optimized Lambda function packages for the workflow system.

## Structure

```
lambdas/
├── starter/             # SQS-triggered workflow starter
│   ├── package.json     # Only @aws-sdk/client-sfn
│   └── index.mjs
├── pre/                 # Data preprocessing
│   ├── package.json     # @aws-sdk/client-s3, @elephant-xyz/cli, adm-zip
│   └── index.mjs
└── post/               # Data processing and submission
    ├── package.json    # @aws-sdk/client-s3, @elephant-xyz/cli
    └── index.mjs
```

## Benefits

- **Minimal Dependencies**: Each function only includes what it needs
- **Faster Execution**: Smaller packages reduce Lambda cold start time
- **Cost Optimization**: Lower deployment and storage costs
- **Clear Boundaries**: Better separation between function responsibilities

## Dependency Analysis

| Lambda         | Dependencies                                   | Purpose                                          |
| -------------- | ---------------------------------------------- | ------------------------------------------------ |
| Starter        | @aws-sdk/client-sfn                            | Start Step Functions workflows                   |
| Pre-processor  | @aws-sdk/client-s3, @elephant-xyz/cli, adm-zip | S3 operations, data transformation, ZIP handling |
| Post-processor | @aws-sdk/client-s3, @elephant-xyz/cli          | S3 operations, blockchain submission             |

## Building

Run the build script to install dependencies:

```bash
./scripts/build-lambdas.sh
```

## Deployment

The SAM template references these optimized packages:

```yaml
WorkflowStarterFunction:
  CodeUri: ../workflow/lambdas/starter

WorkflowPreProcessorFunction:
  CodeUri: ../workflow/lambdas/pre

WorkflowPostProcessorFunction:
  CodeUri: ../workflow/lambdas/post
```
