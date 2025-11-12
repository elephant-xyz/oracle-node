# Property Improvement Transforms

This directory contains transform scripts for the Property Improvement workflow.

## Directory Structure

```
property-improvement/
├── lee/
│   └── scripts/
│       └── property-improvement.js    # Main transform script for Lee County
├── miami-dade/
│   └── scripts/
│       └── property-improvement.js    # Main transform script for Miami-Dade County
└── ...
```

## Adding a New County

1. Create a directory with the county name (lowercase, spaces replaced with hyphens)
2. Create a `scripts/` subdirectory
3. Add your `property-improvement.js` transform script

Example:
```bash
mkdir -p property-improvement/new-county/scripts
# Add your transform script
cp your-script.js property-improvement/new-county/scripts/property-improvement.js
```

## Deployment

These transforms are automatically uploaded to S3 during deployment:
```bash
./scripts/deploy-infra.sh
```

They will be uploaded to:
```
s3://${EnvironmentBucket}/property-improvement/${county-name}/scripts.zip
```

## Transform Script Requirements

Your `property-improvement.js` script should:
- Read from `input.csv` and HTML files in the input zip
- Process the data according to property improvement requirements
- Output to the `data/` directory in the standard Elephant format
- Follow the same structure as regular county transforms




