# Property Improvement Workflow Setup Guide

## Overview

The Property Improvement workflow is a separate workflow for processing property improvement data with its own transforms and browser flows.

## Directory Structure

```
oracle-node/
├── property-improvement/           # Property improvement transforms
│   ├── README.md
│   └── lee/
│       └── scripts/
│           └── property-improvement.js   # Your transform script here
├── browser-flows/                  # Browser flow JSON files
│   ├── README.md
│   └── lee-county-browser-flow.json      # Your browser flow here
└── scripts/
    └── start-property-improvement.sh     # Start script
```

## Setup Steps

### 1. Add Your Transform Script

Place your property improvement transform script here:
```
property-improvement/lee/scripts/property-improvement.js
```

The script should:
- Read from `input.csv` and HTML files
- Process property improvement data
- Output to `data/` directory in Elephant format

### 2. Add Your Browser Flow

Place your browser flow JSON here:
```
browser-flows/lee-county-browser-flow.json
```

### 3. Deploy Infrastructure

Run the deploy script to upload everything to S3:
```bash
cd /Users/samansafavi/Documents/GitHub/oracle-node
source saman.sh
./scripts/deploy-infra.sh
```

This will:
- Deploy the PropertyImprovementWorkflow state machine
- Deploy 3 Lambda functions (pre, prepare, post)
- Upload `property-improvement/lee/scripts/` → `s3://.../property-improvement/lee/scripts.zip`
- Upload `browser-flows/lee-county-browser-flow.json` → `s3://.../browser-flows/lee-county-browser-flow.json`

### 4. Upload Your CSV Files to S3

Upload your property improvement CSV files:
```bash
AWS_PROFILE=oracle-18 aws s3 cp 005e592a-02e9-496a-998c-e9f587e6e80e.csv \
  s3://elephant-leet-permits-splits/005e592a-02e9-496a-998c-e9f587e6e80e.csv
```

### 5. Start the Workflow

Send message to the Property Improvement SQS queue:
```bash
./scripts/start-property-improvement.sh elephant-leet-permits-splits 005e592a-02e9-496a-998c-e9f587e6e80e.csv
```

The script sends an S3 event message to the queue, and the starter Lambda automatically picks it up and invokes the workflow.

## Workflow Architecture

1. **SQS Queue** - Receives S3 event messages
2. **Starter Lambda** - Picks up messages from queue, invokes state machine
3. **State Machine** - Orchestrates the workflow steps

## Workflow Steps

1. **Message to Queue** - S3 event sent to PropertyImprovementSqsQueue
2. **Starter Lambda** - Picks up message and starts workflow
3. **Preprocess** - Extracts county name from CSV filename, builds S3 paths
4. **Prepare** - Downloads CSV (renamed to `input.csv`), runs prepare with browser:
   ```bash
   npx @elephant-xyz/cli@1.54.0 prepare \
     --input-csv input.csv \
     --output-zip output-prepare.zip \
     --browser-flow-file lee-county-browser-flow.json \
     --use-browser
   ```
3. **Postprocess**:
   - Merges `input.csv` + `output-prepare.zip` (flat structure, all files at root)
   - Runs transform with `property-improvement/lee/scripts.zip`
   - Hashes with `--property-cid` (extracted from CSV)
   - Uploads to IPFS
4. **Queue** - Sends transaction items to SQS for submission

## Important Notes

### CSV Filename Handling
- Original filename (e.g., `005e592a-02e9-496a-998c-e9f587e6e80e.csv`) is downloaded
- Automatically renamed to `input.csv` for prepare step
- This is required by the Elephant CLI

### Zip Structure
The merged zip has a **flat structure** (all files at root):
```
merged_input.zip
├── input.csv
├── property.html
├── details.html
└── ...
```

NOT nested in folders.

### S3 Locations After Deployment

**Transform scripts:**
```
s3://environment-bucket/property-improvement/lee/scripts.zip
```

**Browser flows:**
```
s3://environment-bucket/browser-flows/lee-county-browser-flow.json
```

## Monitoring

Check CloudWatch Logs:
```bash
AWS_PROFILE=oracle-18 aws logs tail /aws/vendedlogs/states/PropertyImprovementWorkflow --follow --region us-east-1
```

## Adding More Counties

To add another county (e.g., Miami-Dade):

1. Create directory structure:
```bash
mkdir -p property-improvement/miami-dade/scripts
```

2. Add transform script:
```bash
cp your-script.js property-improvement/miami-dade/scripts/property-improvement.js
```

3. Add browser flow:
```bash
cp your-flow.json browser-flows/miami-dade-county-browser-flow.json
```

4. Deploy:
```bash
./scripts/deploy-infra.sh
```

5. Start workflow:
```bash
./scripts/start-property-improvement.sh your-bucket your-file.csv
```

## Environment Variables

Set in `saman.sh`:
```bash
export ELEPHANT_PROPERTY_IMPROVEMENT=true  # Enable property improvement workflow
```

## Troubleshooting

### Check if stack is deployed:
```bash
AWS_PROFILE=oracle-18 aws cloudformation describe-stacks \
  --stack-name oracle-node \
  --region us-east-1 \
  --query 'Stacks[0].Outputs[?OutputKey==`PropertyImprovementStateMachineArn`]'
```

### List uploaded transforms:
```bash
AWS_PROFILE=oracle-18 aws s3 ls s3://your-environment-bucket/property-improvement/ --recursive
```

### List browser flows:
```bash
AWS_PROFILE=oracle-18 aws s3 ls s3://your-environment-bucket/browser-flows/
```

