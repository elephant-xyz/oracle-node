## Elephant Express: Simple Usage

This repo deploys an AWS Step Functions (Express) workflow with SQS and Lambda. Follow these steps to get it running quickly.

### 1) Set environment variables

The oracle node supports two authentication modes:

#### Option A: Traditional Mode (using API credentials)

Export these environment variables before deploying:

```bash
# Required for traditional mode
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

# Optional (Continue button selector)
export ELEPHANT_PREPARE_CONTINUE_BUTTON=""  # CSS selector for continue button

# Optional (Browser flow template - both must be provided together)
export ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE=""    # Browser flow template name
export ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS=""  # Browser flow parameters as JSON string

# Optional (Updater schedule - only set if you want to change from default)
export UPDATER_SCHEDULE_RATE="1 minute"    # How often updater runs (default: "1 minute")
# For sub-minute intervals, use cron expressions:
# export UPDATER_SCHEDULE_RATE="cron(*/1 * * * ? *)"  # Every minute
# export UPDATER_SCHEDULE_RATE="cron(0/30 * * * ? *)" # Every 30 seconds (at :00 and :30)

# Optional (Proxy rotation - for automatic proxy rotation support)
export PROXY_FILE=/path/to/proxies.txt  # File containing proxy URLs (one per line: username:password@ip:port)
```

#### Option B: Keystore Mode (using encrypted private key)

For non-institutional oracles, you can use your own wallter as a keystore file (encrypted private key) instead of API credentials. The keystore file follows the [EIP-2335 standard](https://eips.ethereum.org/EIPS/eip-2335) for BLS12-381 key encryption.

To create a keystore file, see the [Elephant CLI documentation on encrypted JSON keystores](https://github.com/elephant-xyz/elephant-cli?tab=readme-ov-file#encrypted-json-keystore)

```bash
# Required for keystore mode
export ELEPHANT_KEYSTORE_FILE=/path/to/your/keystore.json  # Path to your keystore JSON file
export ELEPHANT_KEYSTORE_PASSWORD=your-keystore-password   # Password to decrypt the keystore
export ELEPHANT_RPC_URL=...                                # RPC URL for blockchain submission
export ELEPHANT_PINATA_JWT=...                             # Pinata JWT for uploads

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

# Optional (Continue button selector)
export ELEPHANT_PREPARE_CONTINUE_BUTTON=""  # CSS selector for continue button

# Optional (Browser flow template - both must be provided together)
export ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE=""    # Browser flow template name
export ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS=""  # Browser flow parameters as JSON string
```

**Important Notes for Keystore Mode:**

- The keystore file must exist at the specified path
- The password must be correct to decrypt the keystore
- The keystore file will be securely uploaded to S3 during deployment
- When using keystore mode, you don't need to provide: `ELEPHANT_DOMAIN`, `ELEPHANT_API_KEY`, `ELEPHANT_ORACLE_KEY_ID`, or `ELEPHANT_FROM_ADDRESS`
- To create a keystore file, see the [Elephant CLI documentation on encrypted JSON keystores](https://github.com/elephant-xyz/elephant-cli?tab=readme-ov-file#encrypted-json-keystore)

**Browser Flow Files (Optional):**

For complex browser automation scenarios, you can provide county-specific browser flow JSON files:

1. Add JSON files named `<CountyName>.json` (e.g., `Broward.json`, `Miami-Dade.json`) to the `browser-flows/` directory in the repository root
2. These files are automatically uploaded to S3 during deployment
3. The Lambda function will automatically download and use the appropriate flow file based on the county being processed

Example structure:

```
oracle-node/
├── browser-flows/
│   ├── Broward.json
│   ├── Miami-Dade.json
│   └── Palm-Beach.json
├── transform/
└── ...
```

The browser flow file is passed to the prepare function as the `browserFlowFile` parameter and is automatically cleaned up after use.

Put your transform files under `transform/` (if applicable).

### 2) Deploy infrastructure

```bash
./scripts/deploy-infra.sh
```

This creates the VPC, S3 buckets, SQS queues, Lambdas, and the Express Step Functions state machine.

### Configure Prepare Function Behavior

The `DownloaderFunction` uses the `prepare` command from `@elephant-xyz/cli` to fetch and process data. You can control its behavior using environment variables that map to CLI flags:

| Environment Variable               | Default      | CLI Flag        | Description                                                   |
| ---------------------------------- | ------------ | --------------- | ------------------------------------------------------------- |
| `ELEPHANT_PREPARE_USE_BROWSER`     | `false`      | `--use-browser` | Force browser mode for fetching                               |
| `ELEPHANT_PREPARE_NO_FAST`         | `false`      | `--no-fast`     | Disable fast mode                                             |
| `ELEPHANT_PREPARE_NO_CONTINUE`     | `false`      | `--no-continue` | Disable continue mode                                         |
| `ELEPHANT_PREPARE_CONTINUE_BUTTON` | `""`         | N/A             | CSS selector for continue button                              |
| `UPDATER_SCHEDULE_RATE`            | `"1 minute"` | N/A             | Updater frequency (e.g., "5 minutes", "cron(_/1 _ \* _ ? _)") |

#### County-Specific Configuration

When processing inputs from multiple counties in the same node, you can provide county-specific configurations that override the general settings. The Lambda automatically detects the county from the `county_jurisdiction` field in `unnormalized_address.json` and applies the appropriate configuration.

**How it works:**

1. The Lambda reads the county name directly from the `unnormalized_address.json` file inside the input ZIP (without extracting to disk)
2. For each configuration variable, it checks for a county-specific version first (e.g., `ELEPHANT_PREPARE_USE_BROWSER_Alachua`)
3. If not found, it falls back to the general version (e.g., `ELEPHANT_PREPARE_USE_BROWSER`)
4. If neither exists, it uses the default value

**Priority order:** County-specific → General → Default

**Complete multi-county configuration example:**

```bash
# General configuration (default for all counties)
export ELEPHANT_PREPARE_USE_BROWSER=false
export ELEPHANT_PREPARE_NO_FAST=false
export ELEPHANT_PREPARE_NO_CONTINUE=false

# Alachua County - needs browser mode with specific selectors
export ELEPHANT_PREPARE_USE_BROWSER_Alachua=true
export ELEPHANT_PREPARE_CONTINUE_BUTTON_Alachua=".btn.btn-primary.button-1"
export ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE_Alachua="SEARCH_BY_PARCEL_ID"
export ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS_Alachua='{"search_form_selector": "#ctlBodyPane_ctl03_ctl01_txtParcelID", "search_result_selector": "#ctlBodyPane_ctl10_ctl01_lstBuildings_ctl00_dynamicBuildingDataRightColumn_divSummary"}'

# Sarasota County - different template and slower processing
export ELEPHANT_PREPARE_USE_BROWSER_Sarasota=true
export ELEPHANT_PREPARE_NO_FAST_Sarasota=true
export ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE_Sarasota="CUSTOM_SEARCH"
export ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS_Sarasota='{"timeout": 60000, "search_selector": "#parcel-search", "submit_button": "#search-submit"}'

# Charlotte County - simple browser mode, no template needed
export ELEPHANT_PREPARE_USE_BROWSER_Charlotte=true

# Santa Rosa County - note the underscore for the space
export ELEPHANT_PREPARE_USE_BROWSER_Santa_Rosa=true
export ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE_Santa_Rosa="SANTA_ROSA_FLOW"
export ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS_Santa_Rosa='{"timeout": 45000}'

# Palm Beach County - another example with space in name
export ELEPHANT_PREPARE_USE_BROWSER_Palm_Beach=true
export ELEPHANT_PREPARE_NO_FAST_Palm_Beach=true

# Broward County - uses general settings (no browser mode)
# No county-specific settings needed

# Deploy - all county configurations are automatically applied
./scripts/deploy-infra.sh
```

When the Lambda processes data:

- Alachua inputs → Uses browser mode with SEARCH_BY_PARCEL_ID template
- Sarasota inputs → Uses browser mode with CUSTOM_SEARCH template and no-fast mode
- Charlotte inputs → Uses simple browser mode
- Santa Rosa inputs → Uses browser mode with SANTA_ROSA_FLOW template (county with space in name)
- Palm Beach inputs → Uses browser mode with no-fast flag (county with space in name)
- Broward inputs → Uses general settings (no browser mode)
- Any other county → Uses general settings

**Manual configuration update:**

If you need to update county-specific configurations after deployment:

```bash
# Set county-specific environment variables
export ELEPHANT_PREPARE_USE_BROWSER_Charlotte=true
export ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE_Charlotte="CHARLOTTE_FLOW"

# Apply to existing Lambda
./scripts/set-county-configs.sh
```

**Supported county-specific variables:**

- `ELEPHANT_PREPARE_USE_BROWSER_<CountyName>`
- `ELEPHANT_PREPARE_NO_FAST_<CountyName>`
- `ELEPHANT_PREPARE_NO_CONTINUE_<CountyName>`
- `ELEPHANT_PREPARE_CONTINUE_BUTTON_<CountyName>`
- `ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE_<CountyName>`
- `ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS_<CountyName>`

**Important naming convention for counties with spaces:**

For counties with spaces in their names, replace spaces with underscores in the environment variable name:

- `"Santa Rosa"` → `ELEPHANT_PREPARE_USE_BROWSER_Santa_Rosa`
- `"Palm Beach"` → `ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE_Palm_Beach`
- `"San Diego"` → `ELEPHANT_PREPARE_NO_FAST_San_Diego`

The Lambda automatically handles this conversion when matching county names from the data.

#### Browser Flow Template Configuration

For advanced browser automation scenarios, you can provide custom browser flow templates and parameters. This allows you to customize how the prepare function interacts with different county websites.

| Environment Variable                       | Default | Description                                                     |
| ------------------------------------------ | ------- | --------------------------------------------------------------- |
| `ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE`   | `""`    | Browser flow template name to use                               |
| `ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS` | `""`    | JSON string containing parameters for the browser flow template |

**Important:** These two environment variables must be provided together. If only one is set, the deployment will fail with a validation error.

**General configuration example:**

```bash
# Set browser flow template configuration and continue button selector for all counties
export ELEPHANT_PREPARE_CONTINUE_BUTTON=".btn.btn-primary.button-1"
export ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE="SEARCH_BY_PARCEL_ID"
export ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS='{"search_form_selector": "#ctlBodyPane_ctl03_ctl01_txtParcelID", "search_result_selector": "#ctlBodyPane_ctl10_ctl01_lstBuildings_ctl00_dynamicBuildingDataRightColumn_divSummary"}'

# Deploy with the configuration
./scripts/deploy-infra.sh
```

**County-specific browser flow example:**

```bash
# Different counties may need different browser flow templates, continue button selectors, and parameters
export ELEPHANT_PREPARE_CONTINUE_BUTTON_Alachua=".btn.btn-primary.button-1"
export ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE_Alachua="SEARCH_BY_PARCEL_ID"
export ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS_Alachua='{"search_form_selector": "#ctlBodyPane_ctl03_ctl01_txtParcelID", "search_result_selector": "#ctlBodyPane_ctl10_ctl01_lstBuildings_ctl00_dynamicBuildingDataRightColumn_divSummary"}'

export ELEPHANT_PREPARE_CONTINUE_BUTTON_Sarasota="#submit"
export ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE_Sarasota="CUSTOM_SEARCH"
export ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS_Sarasota='{"timeout": 60000, "selector": "#search-box"}'

# Deploy - each county will use its specific configuration
./scripts/deploy-infra.sh
```

**Technical details:**

- The JSON parameters must be valid JSON
- The deployment script validates the JSON structure before proceeding
- Internally, the JSON is converted to a simple `key:value` format for safe transport through the deployment pipeline
- The Lambda reconstructs it back to JSON before passing to the prepare function
- Parameters support strings, numbers, and booleans

**Deploy with custom prepare flags:**

```bash
# Deploy with browser mode enabled
sam deploy --parameter-overrides \
  ElephantPrepareUseBrowser="true" \
  ElephantPrepareNoFast="false" \
  ElephantPrepareNoContinue="false"


# Or set as environment variables before deploy-infra.sh
export ELEPHANT_PREPARE_USE_BROWSER=true
export ELEPHANT_PREPARE_NO_FAST=true
export UPDATER_SCHEDULE_RATE="2 minutes"
./scripts/deploy-infra.sh
```

**View prepare function logs:**

The Lambda logs will show exactly which configuration is being used for each county:

```
📍 Extracting county information from input...
✅ Detected county: Santa Rosa
Building prepare options...
Event browser setting: undefined (using: true)
Checking environment variables for prepare flags:
🏛️ Looking for county-specific configurations for: Santa Rosa
  Using county-specific: ELEPHANT_PREPARE_USE_BROWSER_Santa_Rosa='true'
✓ Setting useBrowser: true (Force browser mode)
  Using general: ELEPHANT_PREPARE_NO_FAST='false'
✗ Not setting noFast flag (Disable fast mode)
  Using county-specific: ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE_Santa_Rosa='SANTA_ROSA_FLOW'
Browser flow template configuration detected:
✓ ELEPHANT_PREPARE_BROWSER_FLOW_TEMPLATE='SANTA_ROSA_FLOW'
  Using county-specific: ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS_Santa_Rosa='timeout:45000'
✓ ELEPHANT_PREPARE_BROWSER_FLOW_PARAMETERS parsed successfully:
{
  "timeout": 45000
}
Calling prepare() with these options...
```

### Keystore Mode Details

The keystore mode provides a secure way to manage private keys for blockchain submissions using the industry-standard [EIP-2335](https://eips.ethereum.org/EIPS/eip-2335) encryption format.

**How it works:**

1. The deployment script validates that the keystore file exists and the password is provided
2. The keystore file is securely uploaded to S3 in the environment bucket under `keystores/` prefix
3. Lambda functions are configured with the S3 location and password as encrypted environment variables
4. During execution, the submit Lambda downloads the keys tore from S3 and uses it for blockchain submissions

**Creating a Keystore File:**
You can create a keystore file using the Elephant CLI tool. For detailed instructions, refer to the [Elephant CLI Encrypted JSON Keystore documentation](https://github.com/elephant-xyz/elephant-cli?tab=readme-ov-file#encrypted-json-keystore).

**Security considerations:**

- The keystore uses EIP-2335 standard encryption (PBKDF2 with SHA-256 for key derivation, AES-128-CTR for encryption)
- The keystore file is stored encrypted in S3 with versioning enabled for audit trails
- The password is stored as an encrypted environment variable in Lambda
- Lambda functions have minimal S3 permissions (read-only access to keystores only)
- The keystore is only downloaded to Lambda's temporary storage during execution and is immediately cleaned up after use

### Update transform scripts

Transforms are stored as raw files under `transform/<county>/`. Each county folder can contain any structure you need (for example `transform/brevard/scripts/*.js`). During deployment the scripts are zipped and uploaded automatically; you no longer manage zip files by hand.

To ship new or updated transform code:

1. Edit the files in the appropriate county directory under `transform/`.
2. Run `./scripts/deploy-infra.sh`.
   - The script rebuilds `workflow/lambdas/post/transforms/<county>.zip` for each county.
   - It synchronizes those archives (plus a manifest) to the environment bucket under `transforms/` and updates the `TransformS3Prefix` parameter used by the post Lambda.
3. On first invocation per county, the post Lambda downloads `s3://<prefix>/<county>.zip`, caches it in `/tmp/county-transforms/<county>.zip`, verifies the MD5/ETag, and reuses the cache until the remote ETag changes.
4. Subsequent deployments automatically refresh S3 and the Lambda detects updated archives by comparing ETags.

Tip: during local testing, remove `/tmp/county-transforms` to force a fresh download.

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

If you also run an Airflow pipeline for the same data, open the Airflow UI and toggle the DAG off (pause) to avoid duplicate processing.

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

### Query Post-Processing Logs

The post-processing Lambda logs detailed execution metrics that can be aggregated and analyzed using the built-in query script. This provides county-level success rates, transaction counts, and failure breakdowns.

**Query recent post-processing logs (last hour):**

```bash
npm run query-post-logs
```

**Query logs for a specific time range:**

```bash
# Query logs from 2 hours ago to 1 hour ago
npm run query-post-logs -- --start 2025-09-24T14:00 --end 2025-09-24T15:00

# Query logs for the last 24 hours
npm run query-post-logs -- --start 2025-09-23T15:30 --end 2025-09-24T15:30
```

**Query with custom AWS profile/region:**

```bash
# Use specific AWS profile
AWS_PROFILE=your-profile npm run query-post-logs

# Use specific AWS region
npm run query-post-logs -- --region us-east-1
```

**Sample output:**

```
County: Lake
  Total executions: 60
  Success rate: 63.33% (38 successes / 60 runs)
  Total successful transactions: 76
  Failure counts by step:
    post_lambda_failed: 14
    validation_failed: 8

County: Brevard
  Total executions: 45
  Success rate: 71.11% (32 successes / 45 runs)
  Total successful transactions: 128
  Failure counts by step:
    transform_failed: 10
    hash_failed: 3
```

**What the metrics mean:**

- **Total executions**: Number of post-processing Lambda invocations for this county
- **Success rate**: Percentage of executions that completed successfully (generated transaction items)
- **Total successful transactions**: Sum of all transaction items generated across successful runs
- **Failure counts by step**: Breakdown of failures by the step where they occurred:
  - `post_lambda_failed`: General execution failures
  - `validation_failed`: Data validation errors
  - `transform_failed`: Data transformation errors
  - `hash_failed`: IPFS hashing errors
  - `upload_failed`: IPFS upload errors

**Command options:**

```bash
Usage: npm run query-post-logs [-- --stack <stack-name>] [-- --start <ISO-8601>] [-- --end <ISO-8601>] [-- --profile <aws_profile>] [-- --region <aws_region>]

Options:
  --stack, -s       CloudFormation stack name (default: elephant-oracle-node)
  --start           ISO-8601 start time (default: one hour ago, UTC)
  --end             ISO-8601 end time (default: now, UTC)
  --profile         AWS profile for credentials (optional)
  --region          AWS region override (optional)
  --help            Show this help text
```

This query tool uses CloudWatch Logs Insights to efficiently analyze large volumes of log data and provides actionable metrics for monitoring post-processing performance across different counties.

## Proxy Rotation

The system supports automatic proxy rotation for the prepare function. This helps distribute load across multiple proxies and prevents rate limiting or IP blocking.

### Setup Proxies

**1. Create a proxy file:**

Create a text file with one proxy per line in format: `username:password@ip:port`

```bash
# Create proxies.txt
cat > proxies.txt <<EOF
user1:password123@192.168.1.100:8080
user2:password456@192.168.1.101:8080
user3:password789@192.168.1.102:8080
EOF
```

You can use `proxies.example.txt` as a template.

**2. Deploy with proxies:**

```bash
# Initial deployment with proxies
export PROXY_FILE=proxies.txt
./scripts/deploy-infra.sh
```

**3. Update proxies later (without full deployment):**

```bash
# Update or add proxies anytime
./scripts/update-proxies.sh proxies.txt
```

### How It Works

- Each Lambda invocation automatically selects the **least recently used** proxy
- Proxies are rotated automatically based on usage timestamps
- Failed proxies are tracked but remain available for retry
- No configuration needed - just provide the proxy file

### Verify Proxies

Check which proxies are configured:

```bash
./scripts/update-proxies.sh --list
```

### Remove All Proxies

```bash
./scripts/update-proxies.sh --clear
```

### Notes

- Empty lines and lines starting with `#` are ignored (comments)
- Proxy format must be: `username:password@ip:port`
- Port must be numeric
- If no proxies are configured, the system works normally without them

That's it — set env vars, deploy, start, monitor, and tune concurrency.

## Dead Letter Queue (DLQ) Management

### Reprocessing Failed Messages

When messages fail processing multiple times, they are moved to Dead Letter Queues. You can reprocess these messages back to the main queue after fixing underlying issues:

**Reprocess Workflow DLQ messages:**

```bash
./scripts/reprocess-dlq.sh --dlq-type workflow --batch-size 10 --max-messages 100
```

**Reprocess Transactions DLQ messages:**

```bash
./scripts/reprocess-dlq.sh --dlq-type transactions --max-messages 1000
```

**DLQ Types:**

- `workflow` - Workflow Dead Letter Queue → Workflow SQS Queue
- `transactions` - Transactions Dead Letter Queue → Transactions SQS Queue

**Parameters:**

- `--dry-run` - Show what would be done without moving messages
- `--batch-size` - Messages per batch (default: 10)
- `--max-messages` - Maximum messages to reprocess (default: 100)
- `--verbose` - Detailed output

```

```
