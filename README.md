## Elephant Express: Simple Usage

This repo deploys an AWS Step Functions (Express) workflow with SQS and Lambda. Follow these steps to get it running quickly.

### 1) Set environment variables

Export these before deploying (replace values with your own):

```bash
# Required (application)
export ELEPHANT_DOMAIN=...
export ELEPHANT_API_KEY=...
export ELEPHANT_ORACLE_KEY_ID=...
export ELEPHANT_FROM_ADDRESS=...
export ELEPHANT_RPC_URL=...
export ELEPHANT_PINATA_JWT=...

# Optional (deployment)
export STACK_NAME=elephant-oracle-node
export WORKFLOW_QUEUE_NAME=elephant-workflow-queue
export WORKFLOW_STARTER_RESERVED_CONCURRENCY=100
export WORKFLOW_STATE_MACHINE_NAME=ElephantExpressWorkflow

# Optional (AWS CLI)
export AWS_PROFILE=your-profile
export AWS_REGION=your-region

# Optional (Prepare function flags - only set to 'true' if needed)
export ELEPHANT_PREPARE_USE_BROWSER=false  # Force browser mode
export ELEPHANT_PREPARE_NO_FAST=false      # Disable fast mode
export ELEPHANT_PREPARE_NO_CONTINUE=false  # Disable continue mode

# Optional (Updater schedule - only set if you want to change from default)
export UPDATER_SCHEDULE_RATE="1 minute"    # How often updater runs (default: "1 minute")
# For sub-minute intervals, use cron expressions:
# export UPDATER_SCHEDULE_RATE="cron(*/1 * * * ? *)"  # Every minute
# export UPDATER_SCHEDULE_RATE="cron(0/30 * * * ? *)" # Every 30 seconds (at :00 and :30)
```

Put your transform files under `transform/` (if applicable).

### 2) Deploy infrastructure

```bash
./scripts/deploy-infra.sh
```

This creates the VPC, S3 buckets, SQS queues, Lambdas, and the Express Step Functions state machine.

### Configure Prepare Function Behavior

The `DownloaderFunction` uses the `prepare` command from `@elephant-xyz/cli` to fetch and process data. You can control its behavior using environment variables that map to CLI flags:

| Environment Variable           | Default | CLI Flag        | Description                     |
| ------------------------------ | ------- | --------------- | ------------------------------- |
| `ELEPHANT_PREPARE_USE_BROWSER` | `false` | `--use-browser` | Force browser mode for fetching |
| `ELEPHANT_PREPARE_NO_FAST`     | `false` | `--no-fast`     | Disable fast mode               |
| `ELEPHANT_PREPARE_NO_CONTINUE` | `false` | `--no-continue` | Disable continue mode           |
| `UPDATER_SCHEDULE_RATE`        | `"1 minute"` | N/A        | Updater frequency (e.g., "5 minutes", "cron(*/1 * * * ? *)") |

**Deploy with custom prepare flags:**

```bash
# Deploy with browser mode enabled
sam deploy --parameter-overrides \
  ElephantPrepareUseBrowser="true" \
  ElephantPrepareNoFast="false" \
  ElephantPrepareNoContinue="false"

# Deploy with custom updater schedule (5 minutes)
sam deploy --parameter-overrides \
  UpdaterScheduleRate="5 minutes"

# Deploy with sub-minute schedule using cron (every 30 seconds)
sam deploy --parameter-overrides \
  UpdaterScheduleRate="cron(0/30 * * * ? *)"

# Or set as environment variables before deploy-infra.sh
export ELEPHANT_PREPARE_USE_BROWSER=true
export ELEPHANT_PREPARE_NO_FAST=true
export UPDATER_SCHEDULE_RATE="2 minutes"
./scripts/deploy-infra.sh
```

**View prepare function logs:**

The Lambda logs will show exactly which options are being used:

```
Building prepare options...
Event browser setting: undefined (using: true)
Checking environment variables for prepare flags:
✓ ELEPHANT_PREPARE_USE_BROWSER='true' → adding useBrowser: true
✗ ELEPHANT_PREPARE_NO_FAST='false' → not adding noFast flag
✗ ELEPHANT_PREPARE_NO_CONTINUE='false' → not adding noContinue flag
Calling prepare() with these options...
```

### Update transform scripts

To update your transforms:

- Place or update files under `transform/scripts/`.
- Redeploy to package and upload the latest transforms:

```bash
./scripts/deploy-infra.sh
```

### 3) Start the workflow

Use your input S3 bucket name:

```bash
./scripts/start-step-function.sh <your-bucket-name>
```

Available bucket names as of now:

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

### 4) Pause your Airflow DAG

If you also run an Airflow/MWAA pipeline for the same data, open the Airflow UI and toggle the DAG off (pause) to avoid duplicate processing.

### Monitor the workflow

- Step Functions: open AWS Console → Step Functions → State machines. The Express workflow name contains "ElephantExpressWorkflow". View current and recent executions.
- Logs: CloudWatch Logs group `/aws/vendedlogs/states/ElephantExpressWorkflow` contains execution logs for the Express workflow.

Helpful docs:

- Processing input and output in Step Functions: https://docs.aws.amazon.com/step-functions/latest/dg/concepts-input-output-filtering.html
- Monitoring Step Functions: https://docs.aws.amazon.com/step-functions/latest/dg/proddash.html

### Control concurrency

Throughput is governed by the SQS → Lambda trigger on `WorkflowStarterFunction`:

- Batch size: number of SQS messages per Lambda invoke. Keep it small (often 1) to process one job per execution.
- Reserved concurrency on the Lambda: caps how many executions run in parallel.

Use the AWS Console → Lambda → `WorkflowStarterFunction` → Configuration → Triggers (SQS) to adjust Batch size, and Configuration → Concurrency to set reserved concurrency.

Docs:

- Using AWS Lambda with Amazon SQS: https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html
- Managing Lambda function concurrency: https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html

### Inspect failures

- Step Functions Console: select your Express state machine → Executions → filter by Failed → open an execution to see the error and the failed state.
- CloudWatch Logs: from the execution view, follow the log link to see state logs. You can also open the Lambda’s log groups for detailed stack traces.

Docs:

- View Step Functions execution history and errors: https://docs.aws.amazon.com/step-functions/latest/dg/concepts-states.html#concepts-states-errors
- CloudWatch Logs for Step Functions: https://docs.aws.amazon.com/step-functions/latest/dg/cloudwatch-log-standard.html

That’s it — set env vars, deploy, start, monitor, and tune concurrency.

```

```
