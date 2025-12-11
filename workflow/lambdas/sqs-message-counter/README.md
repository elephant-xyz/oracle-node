# SQS Message Counter Lambda

Lambda function that counts messages in SQS queues, queries CloudWatch metrics, and optionally updates a Google Sheet.

## Features

- **Auto-detects account ID** - Uses STS GetCallerIdentity
- **Auto-discovers queues** - Reads from CloudFormation stack outputs
- **SQS Message Counting** - Counts messages in Transactions, Gas Price, and workflow queues
- **CloudWatch Metrics Integration** - Calculates "in progress" and "failed" counts from workflow phase metrics
- **Google Sheets integration** - Updates counts in a Google Sheet (optional)
- **Scheduled Execution** - Runs 3 times daily via EventBridge (8:00 AM, 4:00 PM, and midnight UTC)

## How It Works

1. **Counts SQS Messages**: Queries SQS queues for `ApproximateNumberOfMessages`
   - Transactions Main Queue
   - Transactions DLQ
   - Gas Price Main Queue
   - Gas Price DLQ
   - All `elephant-workflow-queue` queues (for "waiting for mining" count)

2. **Queries CloudWatch Metrics** (for "in progress" and "failed" counts):
   - **In Progress**: Calculates workflows currently in progress over last 30 days
     - Prepare, Transform, Upload, Hash: `IN_PROGRESS - SUCCEEDED - FAILED`
     - SVL, MVL: `IN_PROGRESS - SUCCEEDED`
   - **Failed**: Sums all FAILED metrics over last 30 days
     - Prepare, Transform, Upload, Hash, AutoRepair phases

3. **Updates Google Sheet** (if configured):
   - Retrieves secret from AWS Secrets Manager
   - Authenticates with Google Sheets API
   - Finds or creates today's date columns with the 4 formats above
   - Finds the row matching the current AWS account ID in column C
   - Updates the cells with the respective counts

4. **Returns Results**: JSON with account ID, queue counts, CloudWatch metrics, and totals

## Google Sheets Setup

To enable Google Sheets updates, you need to set up a Google Service Account and configure AWS Secrets Manager.

### Step 1: Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Click on the project dropdown at the top
3. Click **"New Project"**
4. Enter a project name (e.g., "Elephant Oracle SQS Counter")
5. Click **"Create"**

### Step 2: Enable Google Sheets API

1. In the Google Cloud Console, go to **"APIs & Services"** > **"Library"**
2. Search for **"Google Sheets API"**
3. Click on it and click **"Enable"**

### Step 3: Create a Service Account

1. Go to **"APIs & Services"** > **"Credentials"**
2. Click **"Create Credentials"** > **"Service Account"**
3. Fill in the details:
   - **Service account name**: `sqs-message-counter` (or any name you prefer)
   - **Service account ID**: Will be auto-generated
   - **Description**: "Service account for SQS Message Counter Lambda"
4. Click **"Create and Continue"**
5. **Grant this service account access to project** (optional):
   - Role: You can skip this for now, or select "Editor" if you want
   - Click **"Continue"**
6. Click **"Done"** (you can skip adding users)

### Step 4: Create and Download Service Account Key

1. In the **"Credentials"** page, find your service account in the **"Service Accounts"** section
2. Click on the service account email
3. Go to the **"Keys"** tab
4. Click **"Add Key"** > **"Create new key"**
5. Select **"JSON"** as the key type
6. Click **"Create"**
7. A JSON file will be downloaded automatically (e.g., `elephant-oracle-sqs-counter-xxxxx.json`)

**⚠️ Important**: Keep this file secure! It contains credentials that allow access to your Google Sheets.

### Step 5: Share Google Sheet with Service Account

1. Open your Google Sheet
2. Click the **"Share"** button (top right)
3. In the **"Add people and groups"** field, paste the **service account email** (found in the JSON file as `client_email`, or in the Google Cloud Console)
   - Example: `sqs-message-counter@your-project-id.iam.gserviceaccount.com`
4. Give it **"Editor"** permissions (so it can write to the sheet)
5. **Uncheck** "Notify people" (service accounts don't need notifications)
6. Click **"Share"**

### Step 6: Get Your Sheet ID

1. Open your Google Sheet
2. Look at the URL in your browser:
   ```
   https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit#gid=0
   ```
3. Copy the `{SHEET_ID}` part (the long string between `/d/` and `/edit`)

### Step 7: Set Up AWS Secrets Manager Secret

#### Using the Setup Script (Recommended)

Use the bash setup script to create/update the secret:

```bash
AWS_PROFILE=oracle-saman ./scripts/setup-google-sheets-secret.sh \
  service-account.json \
  --sheet-id "1abc123def456ghi789" \
  --tab-name "Node"
```

The script will auto-detect the CloudFormation stack name. You can also specify it explicitly:

```bash
./scripts/setup-google-sheets-secret.sh \
  service-account.json \
  --sheet-id "1abc123def456ghi789" \
  --tab-name "Node" \
  --stack-name elephant-oracle-node \
  --profile oracle-saman
```

#### Secret Configuration

**Secret Name Options** (tried in order):
1. `{STACK_NAME}/google-sheets/config` (e.g., `elephant-oracle-node/google-sheets/config`)
2. `spreadsheet-api/google-credentials` (default fallback)

**Secret Value Format** (JSON):
```json
{
  "sheetId": "1abc123def456ghi789",
  "tabName": "Node",
  "credentials": {
    "type": "service_account",
    "project_id": "...",
    "private_key_id": "...",
    "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
    "client_email": "...",
    "client_id": "...",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "..."
  }
}
```

#### Manual Setup (Alternative)

You can also create the secret manually using AWS CLI:

```bash
# Create the secret JSON
cat > secret.json << EOF
{
  "sheetId": "1abc123def456ghi789",
  "tabName": "Node",
  "credentials": $(cat service-account.json)
}
EOF

# Create the secret
aws secretsmanager create-secret \
  --name "elephant-oracle-node/google-sheets/config" \
  --secret-string file://secret.json
```

### Step 8: Verify Secret Setup

After setting up, you can verify the secret:

```bash
# Check if secret exists and view metadata
aws secretsmanager describe-secret --secret-id "elephant-oracle-node/google-sheets/config"

# View the secret value (decrypted)
aws secretsmanager get-secret-value --secret-id "elephant-oracle-node/google-sheets/config" --query "SecretString" --output text | jq .
```

## Google Sheet Format

The Lambda expects a Google Sheet with:

1. **Column C**: Account IDs (one per row)
2. **Column D+**: Date columns with specific formats

The Lambda will:
- Find or create date columns for today's date with the following formats:
  - `YYYY-MM-DD (ready to be minted)` - SQS message counts (Transactions + Gas Price queues)
  - `YYYY-MM-DD (waiting for mining)` - Workflow queue message counts
  - `YYYY-MM-DD (in progress)` - CloudWatch metrics (workflows in progress over last 30 days)
  - `YYYY-MM-DD (failed)` - CloudWatch metrics (failed workflows over last 30 days)
- Find the row matching the current AWS account ID in column C
- Update the cells with the respective counts
- Insert new date columns to the right of existing date columns for the same date

### Example Sheet Structure

| (A) | (B) | (C) Account ID | (D) 2025-12-10 (ready to be minted) | (E) 2025-12-10 (waiting for mining) | (F) 2025-12-10 (in progress) | (G) 2025-12-10 (failed) |
|-----|-----|----------------|--------------------------------------|-----------------------------------|----------------------------|-------------------------|
| ... | ... | 564827068047   | 115                                  | 25                                | 42                         | 8                       |
| ... | ... | 582360921509   | 50                                   | 12                                | 18                         | 3                       |

## Error Handling

- **If Secrets Manager secret is missing**: Lambda completes successfully but logs that Google Sheets update was skipped
- **If Google Sheet is not accessible**: Lambda logs error but continues
- **If account ID not found in sheet**: Lambda logs warning but continues

## Testing

### Local Test (requires AWS credentials)

```bash
cd workflow/lambdas/sqs-message-counter
npm install
npm run test:local
```

### Test After Deployment

```bash
./scripts/test-sqs-message-counter-lambda.sh --stack-name elephant-oracle-node
```

## Troubleshooting

### "Permission denied" when updating sheet
- Make sure you shared the Google Sheet with the service account email
- Verify the service account has "Editor" permissions

### "Sheet not found"
- Verify the Sheet ID is correct (check the URL)
- Make sure the sheet is accessible (not in a restricted folder)

### "Tab not found"
- Verify the tab name matches exactly (case-sensitive)
- Check for extra spaces in the tab name

### "Invalid credentials"
- Verify the JSON file is valid: `jq . your-credentials.json`
- Make sure the service account key hasn't been deleted from Google Cloud Console

### "Account ID not found in sheet"
- Make sure the Account ID is in column C
- Verify the Account ID matches exactly (no extra spaces)

### CloudWatch metrics returning 0
- Verify the Lambda has `cloudwatch:ListMetrics` and `cloudwatch:GetMetricStatistics` permissions
- Check that metrics are being published to the `Elephant/Workflow` namespace
- Ensure the time range (30 days) contains metric data

## Security Best Practices

1. **Never commit the credentials JSON file to git**
   - Add it to `.gitignore`: `*.json` (or specifically `*service-account*.json`)
   - Store it securely (e.g., in a password manager or secure vault)

2. **Use AWS Secrets Manager** (which we're doing)
   - The credentials are stored encrypted in Secrets Manager
   - Only the Lambda function can access them

3. **Rotate keys periodically**
   - Create new keys in Google Cloud Console
   - Update the secret in Secrets Manager with new credentials
   - Delete old keys from Google Cloud Console

4. **Limit service account permissions**
   - Only grant access to the specific Google Sheet needed
   - Don't grant project-wide permissions unless necessary
