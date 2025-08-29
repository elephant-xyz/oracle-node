**Goal**

- Deploy an MWAA (Managed Airflow) environment quickly, then iterate on transform scripts with a single command.

**Who This Is For**

- Engineers who want a minimal, repeatable path to run and update a production-ready DAG without deep AWS setup.

**Step 1 — AWS Account and Access**

- Use an AWS account with permissions to create CloudFormation stacks (creates VPC, S3, SQS, MWAA, IAM, Logs).
- If you use multiple accounts, set `AWS_PROFILE` for commands below.

**Step 2 — Install Tools**

- `aws` CLI v2
- `jq`, `zip`, `curl`
- `uv` (fast Python dependency resolver): https://docs.astral.sh/uv/getting-started/installation/

**Step 3 — Configure AWS CLI**

- Run: `aws configure`
- Provide Access Key, Secret, default region (e.g., `us-east-1`), and default output.
- Verify: `aws sts get-caller-identity`

**Step 4 — Clone and Enter the Project**

- `git clone <this-repo>`
- `cd <repo>`

**Step 5 — Deploy Infrastructure (one-time)**

- Required input:
  - `SOURCE_S3_BUCKET_ARN` — the S3 bucket that will emit ObjectCreated events (format: `arn:aws:s3:::bucket-name`). Same-account or cross-account both work.
- Minimal command:
  - `SOURCE_S3_BUCKET_ARN=arn:aws:s3:::your-source-bucket ./scripts/deploy-infra.sh`
- Optional settings (use env vars):
  - `STACK_NAME=elephant-mwaa` (default)
  - `MWAA_ENV_NAME=<STACK_NAME>-MwaaEnvironment` (default)
- Duration: ~15–30 minutes on first create. The script prints:
  - Airflow UI URL
  - MWAA Environment Name
  - S3 bucket used for DAGs and artifacts

**Step 6 — Open Airflow UI**

- From the script output, open the printed UI URL.
- If you need to re-fetch later:
  - `aws cloudformation describe-stacks --stack-name ${STACK_NAME:-elephant-mwaa} --query "Stacks[0].Outputs[?OutputKey=='MwaaApacheAirflowUI'].OutputValue" --output text`
- Authentication uses your AWS IAM identity (via MWAA’s default setup).

**Step 7 — One-Time Airflow Variables**

- Go to Airflow UI → Admin → Variables and set:
  - `elephant_sqs_queue_url`: value from stack output `MwaaSqsQueueUrl`.
  - `elephant_output_base_uri`: e.g., `s3://<EnvironmentBucketName>/outputs`
  - For submit/hash features:
    - `elephant_pinata_jwt`
    - `elephant_domain`, `elephant_api_key`, `elephant_oracle_key_id`, `elephant_from_address`, `elephant_rpc_url`

Note: `elephant_scripts_s3_uri` is set automatically in Step 9 when you upload transforms.

**Step 8 — Upload the DAG**

- Push the DAG to MWAA’s S3 bucket:
  - `./scripts/sync-dags.sh`

**Step 9 — Upload Transforms (repeat often)**

- Place/update files under `transforms/`.
- Run:
  - `./scripts/update-transforms.sh`
- What it does:
  - Zips `transforms/` to `s3://<EnvironmentBucketName>/scripts/transforms.zip`
  - Updates Airflow variable `elephant_scripts_s3_uri` to point to that zip

**Step 10 — Connect Your Source Data (S3 → SQS)**

- Configure your source S3 bucket to send ObjectCreated events for `.zip` files to the stack’s SQS queue.
- Get queue info:
  - URL: `aws cloudformation describe-stacks --stack-name ${STACK_NAME:-elephant-mwaa} --query "Stacks[0].Outputs[?OutputKey=='MwaaSqsQueueUrl'].OutputValue" --output text`
  - ARN: `aws cloudformation describe-stacks --stack-name ${STACK_NAME:-elephant-mwaa} --query "Stacks[0].Outputs[?OutputKey=='MwaaSqsQueueArn'].OutputValue" --output text`
- Example S3 event config (same-account):
  - `aws s3api put-bucket-notification-configuration --bucket <source-bucket> --notification-configuration '{"QueueConfigurations":[{"QueueArn":"<MwaaSqsQueueArn>","Events":["s3:ObjectCreated:*"],"Filter":{"Key":{"FilterRules":[{"Name":"suffix","Value":".zip"}]}}}]}'`
- Cross-account: use the same `SOURCE_S3_BUCKET_ARN` (from Step 5) pointing to the source bucket in the other account, then configure events on that bucket.

**Step 11 — Run and Monitor**

- The DAG (`elephant_workflow`) polls the SQS queue for new `.zip` events.
- Watch runs and task logs in the Airflow UI.
- CloudWatch logs are enabled for all components for deeper debugging.

**Everyday Updates**

- Update transforms: `./scripts/update-transforms.sh` (fast and frequent)
- Update DAGs: edit `dags/` → `./scripts/sync-dags.sh`

**Troubleshooting**

- Permissions: confirm with `aws sts get-caller-identity` and ensure your role can create CFN, S3, SQS, MWAA, IAM, Logs.
- Missing tools: install `aws`, `jq`, `zip`, and `curl` and ensure they’re on `PATH`.
- Can’t reach UI: re-fetch URL (Step 6) and confirm you’re logged in with the same AWS account.
- No runs: verify S3 event → SQS configuration and that `elephant_sqs_queue_url` is set in Airflow Variables.
- Source bucket ARN error: If the script fails at create-stack, ensure `SOURCE_S3_BUCKET_ARN=arn:aws:s3:::your-bucket` is set and valid.

Deploy once. Iterate fast on transforms.
