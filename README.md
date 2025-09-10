**Goal**

- Get MWAA (Managed Airflow) running fast, deploy your own transform scripts, and run the workflow with minimal steps.

### Prerequisites

- AWS account with permissions to create CloudFormation stacks (VPC, S3, SQS, MWAA, IAM, Logs)
- Tools: `aws` CLI v2, `jq`, `zip`, `curl`, `uv`, `sam` CLI
- Configure AWS profile with Access Key ID and Secret Access Key:
  - Create: `aws configure --profile <your-profile>`
  - Use: `export AWS_PROFILE=<your-profile>`
  - Verify: `aws sts get-caller-identity`

### 1) Deploy infrastructure (one-time)

Run:

```bash
./scripts/deploy-infra.sh
```

Optional:

- Set `STACK_NAME` (default: `oracle-node`) and `MWAA_ENV_NAME` (default: `<STACK_NAME>-MwaaEnvironment`).

This creates MWAA, VPC, S3, SQS, Lambdas, IAM, and CloudWatch. At the end it prints the Airflow UI URL and environment bucket.

### 2) Add your transform scripts (replace the samples)

- Put your files under `transform/` (replace any existing examples in that folder).
- Use the output of `elephant-cli generate-transform` (or your modified output) as the contents of `transform/`.
  See: [Generate transform scripts](https://github.com/elephant-xyz/elephant-cli?tab=readme-ov-file#generate-transform-scripts)

### 3) Deploy DAGs and your transforms to MWAA

Run:

```bash
./scripts/deploy_to_mwaa.sh
```

What it does:

- Syncs `dags/` to MWAA
- Zips `transform/` to `build/transforms.zip`, uploads to `s3://<EnvironmentBucketName>/scripts/transforms.zip`
- Sets Airflow variable `elephant_scripts_s3_uri` to that S3 path

Tip: To update only transforms later, run `./scripts/update-transforms.sh`.

### 4) Start the Step Function (seed SQS with your bucket)

Provide the source S3 bucket name containing your input files:

```bash
./scripts/start-step-function.sh <your-source-bucket>
```

This seeds messages to the SQS queue that the DAG polls.
Use one of those buckets depending on the county you use. Those buckets already have the data you need.

- elephant-input-breavard-county
- elephant-input-broward-county
- elephant-input-charlotte-county
- elephant-input-duval-county
- elephant-input-hillsborough-county
- elephant-input-lake-county
- elephant-input-lee-county
- elephant-input-leon-county
- elephant-input-manatee-county
- elephant-input-palm-beach-county
- elephant-input-pinellas-county
- elephant-input-polk-county
- elephant-input-santa-county

```bash
aws cloudformation describe-stacks \
  --stack-name ${STACK_NAME:-oracle-node} \
  --query "Stacks[0].Outputs[?OutputKey=='MwaaApacheAirflowUI'].OutputValue" \
  --output text
```

Authentication uses your AWS IAM identity.

### 6) Set required Airflow Variables (Airflow 2.10.3 UI)

Exact clicks:

1. In the top navigation, click "Admin" → "Variables".
2. Click the "+" (Create) button.
3. Add each variable (Key → Value), then click "Save". Repeat for all keys:
   - `elephant_sqs_queue_url` → value from stack output `MwaaSqsQueueUrl`
   - `elephant_output_base_uri` → `s3://<EnvironmentBucketName>/outputs`
   - `elephant_domain`
   - `elephant_api_key`
   - `elephant_oracle_key_id`
   - `elephant_from_address`
   - `elephant_rpc_url`
   - `elephant_pinata_jwt`

Notes:

- `elephant_scripts_s3_uri` is set automatically by step 3.
- You can also re-run `./scripts/deploy_to_mwaa.sh` with environment variables set to auto-populate these.

### 7) Enable the DAG

In the Airflow UI:

1. Click "DAGs" in the top left.
2. Find `elephant_workflow`.
3. Toggle the switch to "On".

Optional: Click the play ▶ icon to trigger a run immediately.

### 8) Control concurrency

- In Airflow Variables, set `elephant_batch_size` to control how many SQS messages are processed per poll (default: `1`).
- For overall DAG parallelism across runs, edit `dags/elephant_workflow.py` and adjust `max_active_runs` (default: `100`), then run `./scripts/sync-dags.sh`.

### 9) View logs inside Airflow

To see task logs for a run:

1. Open `DAGs` → click `elephant_workflow`.
2. In the Grid view, click a task square.
3. Click "Log" to view live/archived logs (use the dropdown to switch tries).

Tip: For historical runs, go to "Browse" → "DAG Runs", open a run, then click a task → "Log".

### Everyday updates

- Update transforms: edit `transform/` → `./scripts/update-transforms.sh`
- Update DAGs: edit `dags/` → `./scripts/sync-dags.sh`

### Queue Message Fixing

If you encounter JSON parsing errors like "Expecting property name enclosed in double quotes" in your Airflow DAG, you may have malformed messages in your SQS queue. This can happen when messages are requeued incorrectly.

**Quick Fix:**
```bash
# Auto-fix script (finds queue automatically and fixes malformed messages)
./scripts/auto-fix-queue.sh

# Or specify parameters for more control
./scripts/auto-fix-queue.sh --queue-url "YOUR_QUEUE_URL" --batch-size 100 --max-batches 10 --verbosesq
```

**Parameters:**
- `--queue-url URL` - SQS queue URL (optional, auto-discovers if not provided)
- `--batch-size SIZE` - Messages per batch (default: 100)
- `--max-batches COUNT` - Maximum batches to process (default: 1000)
- `--profile PROFILE` - AWS profile to use (default: oracle-3)
- `--verbose` - Enable detailed output
- `--help` - Show all options

**Examples:**
```bash
# Test with small batch
./scripts/auto-fix-queue.sh --batch-size 10 --max-batches 1 --verbose

# Process specific queue with custom settings
./scripts/auto-fix-queue.sh --queue-url "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue" --batch-size 50 --max-batches 100

# Show help
./scripts/auto-fix-queue.sh --help
```



### Troubleshooting

- Can't reach UI: fetch the UI URL again (step 5) and confirm you're logged in with the right AWS account.
- No runs: ensure the DAG is enabled and `elephant_sqs_queue_url` is set. Re-run step 4 to seed SQS.
- JSON parsing errors: use the queue fix scripts above to clean up malformed messages.
- Permissions: verify identity with `aws sts get-caller-identity` and that your role can create CFN, S3, SQS, MWAA, IAM, Logs.

Deploy once. Iterate quickly on your transforms.
