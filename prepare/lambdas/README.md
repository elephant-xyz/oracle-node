# Optimized Lambda Packages

This directory contains optimized Lambda function packages with minimal dependencies.

## Structure

```
lambdas/
├── downloader/          # S3 data downloading and preparation
│   ├── package.json     # Only @aws-sdk/client-s3, @elephant-xyz/cli
│   └── index.mjs
└── updater/            # Lambda function configuration updates
    ├── package.json    # Only @aws-sdk/client-lambda
    └── index.mjs
```

## Benefits

- **Reduced Package Sizes**: Each Lambda only includes the dependencies it actually uses
- **Faster Cold Starts**: Smaller packages load faster in Lambda runtime
- **Lower Costs**: Reduced deployment package size and storage costs
- **Better Separation**: Clear dependency boundaries between functions

## Dependency Analysis

| Lambda     | Dependencies                          | Purpose                            |
| ---------- | ------------------------------------- | ---------------------------------- |
| Downloader | @aws-sdk/client-s3, @elephant-xyz/cli | S3 operations and data preparation |
| Updater    | @aws-sdk/client-lambda                | Function configuration updates     |

## Building

Run the build script to install dependencies:

```bash
./scripts/build-lambdas.sh
```

## Deployment

The SAM template has been updated to use these optimized packages:

```yaml
DownloaderFunction:
  CodeUri: lambdas/downloader # Instead of entire prepare/ directory

UpdaterFunction:
  CodeUri: lambdas/updater # Instead of entire prepare/ directory
```
