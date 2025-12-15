# EventBridge Contract: Workflow Orchestration Events

## Event Type: `WorkflowEvent`

Emitted on workflow step status changes (success, failure, or parked).

### Event Structure

```json
{
  "source": "elephant.workflow",
  "detail-type": "WorkflowEvent",
  "detail": {
    "executionId": "string",
    "county": "string",
    "dataGroupLabel": "string",
    "status": "string",
    "phase": "string",
    "step": "string",
    "taskToken": "string",
    "errors": [
      {
        "code": "string",
        "details": {}
      }
    ]
  }
}
```

### Field Definitions

| Field            | Type   | Description                                                                                        |
| ---------------- | ------ | -------------------------------------------------------------------------------------------------- |
| `executionId`    | string | Step Functions execution ARN                                                                       |
| `county`         | string | County identifier being processed                                                                  |
| `dataGroupLabel` | string | Elephant data group (e.g., `Seed`, `County`). See [Elephant Lexicon](https://lexicon.elephant.xyz) |
| `status`         | string | Current workflow status (e.g., `SUCCEEDED`, `FAILED`)                                              |
| `phase`          | string | High-level workflow phase                                                                          |
| `step`           | string | Granular step within the phase                                                                     |
| `taskToken`      | string | Step Functions task token for resumption                                                           |
| `errors`         | array  | List of error objects                                                                              |

### Error Object Schema

| Field     | Type   | Description                                  |
| --------- | ------ | -------------------------------------------- |
| `code`    | string | Error code identifier                        |
| `details` | object | Free-form object with error-specific details |

### Error code

`error.code` is a **stable, machine-readable identifier for a specific error condition** that occurred in the workflow.

- **Purpose**:
  - **Uniquely identifies** the type of error so it can be counted, queried, and triaged.
  - Multiple entries in the `errors` array with the **same** `code` are treated as **multiple occurrences of the same error** by the error handler in `workflow-events/lambdas/event-handler/index.ts`.
- **Format**:
  - Must be a **non-empty string**.
  - Pattern: `<phase_prefix><error_identifier>` where:
    - `<phase_prefix>` is a 2-digit number identifying the workflow phase
    - `<error_identifier>` is either a static suffix (e.g., `001`) or a dynamic hash
  - The system derives an internal _error type_ from the **first two characters** of the code for aggregation (for example, codes `20001` and `20002` are both treated as type `20` for metrics).
- **Stability**:
  - Once defined, a given `code` should **always represent the same error meaning** across time and across executions.
  - Do **not** reuse a code for a different error condition.
- **Usage in events**:
  - Producers may emit **zero, one, or many** error objects per event.
  - If the same error happens multiple times in a single execution, emit multiple entries with the same `code` (and appropriate `details`); the event handler will **aggregate the occurrences**.

### Error Code Reference

| Code            | Phase         | Description                                                                                                                                                                               |
| --------------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `00001`         | Preprocess    | Preprocess step failure. Used when the initial preprocessing fails before Prepare begins.                                                                                                 |
| `01001`         | Prepare       | Prepare queue not found. The county-specific SQS queue does not exist.                                                                                                                    |
| `01xxx<county>` | Prepare       | Lambda infrastructure error. `01002`=generic, `01003-01006`=input.csv errors, `01008-01015`=S3/config errors, `01016-01020`=taskToken/Step Functions errors. Example: `01002Hamilton`     |
| `10xxx<county>` | Prepare       | CLI prepare error. Example: `10050Broward`                                                                                                                                                |
| `20<county>`    | Transform     | Transform step failure or exception. The `<county>` is the county name being processed. Example: `20Cook`                                                                                 |
| `30<hash>`      | SVL           | Schema validation error. The `<hash>` is a SHA256 hash computed from `error_message#error_path#county`, uniquely identifying each distinct validation error. Example: `30a1b2c3d4e5f6...` |
| `31001`         | SVL           | SVL runtime exception (non-validation failure)                                                                                                                                            |
| `40001`         | Hash          | Generic hash step failure or exception                                                                                                                                                    |
| `50001`         | Upload        | Generic upload step failure or exception                                                                                                                                                  |
| `60001`         | GasPriceCheck | Generic gas price check failure (unknown/unclassified)                                                                                                                                    |
| `60010`         | GasPriceCheck | Missing RPC URL configuration                                                                                                                                                             |
| `60011`         | GasPriceCheck | Missing max gas price configuration                                                                                                                                                       |
| `60020`         | GasPriceCheck | Unable to retrieve gas price from RPC                                                                                                                                                     |
| `60021`         | GasPriceCheck | RPC connection error (`ECONNREFUSED`, `ETIMEDOUT`)                                                                                                                                        |
| `60022`         | GasPriceCheck | RPC timeout                                                                                                                                                                               |
| `60030`         | GasPriceCheck | Gas price too high after max wait time (default 10 min)                                                                                                                                   |
| `601xx`         | Submit        | Message parsing errors (missing body, invalid format, empty items, JSON parse errors)                                                                                                     |
| `602xx`         | Submit        | Environment configuration errors (missing required env vars for keystore/API mode)                                                                                                        |
| `603xx`         | Submit        | S3/Keystore errors (download failures)                                                                                                                                                    |
| `604xx`         | Submit        | Blockchain/Contract errors (nonce, gas, RPC, transaction errors from submit_errors.csv)                                                                                                   |
| `605xx`         | Submit        | File I/O errors (CSV read/write failures)                                                                                                                                                 |
| `60999`         | Submit        | Unknown/unclassified submit error                                                                                                                                                         |
| `70001`         | AutoRepair    | Auto-repair failed for MVL errors after max retries                                                                                                                                       |
| `70002`         | AutoRepair    | Auto-repair failed for SVL errors after max retries                                                                                                                                       |

#### Prepare Error Codes (`10xxx` - CLI Prepare)

| Range         | Category         | Description                      |
| ------------- | ---------------- | -------------------------------- |
| `10001-10005` | Input Validation | Proxy, CSV, option errors        |
| `10010-10016` | Missing File     | Parcel, address, field errors    |
| `10020-10024` | Workflow/Flow    | Template and flow file errors    |
| `10030-10036` | Platform/HTTP    | Platform and API errors          |
| `10040-10045` | Frame/Navigation | Iframe and navigation errors     |
| `10050-10054` | Timeout          | Selector, navigation timeouts    |
| `10060-10064` | Context          | Execution context errors         |
| `10070-10075` | Browser          | Browser crash/launch errors      |
| `10080-10084` | Selector         | Element selector errors          |
| `10090-10097` | Network          | Connection, DNS, SSL errors      |
| `10100-10103` | Interaction      | Click, type, visibility errors   |
| `10110-10116` | File System      | ENOENT, EACCES, disk errors      |
| `10120-10122` | JSON             | JSON parsing errors              |
| `10125-10133` | Network (undici) | Timeout, socket, host errors     |
| `10135-10139` | Archive          | ZIP format and corruption errors |
| `10140-10148` | Runtime          | Memory, stack overflow errors    |
| `10999`       | Unknown          | Unclassified prepare error       |

> **Note**: Prepare error codes are concatenated with the county name (e.g., `01002Hamilton`). See `prepare/lambdas/downloader/index.mjs` for full mapping.

> **Note on Transform errors (code `20<county>`)**: Transform errors include the county name in the error code, enabling county-specific tracking and aggregation of transform failures.

#### GasPriceCheck Error Codes (`600xx` - Gas Price Check)

| Code    | Category      | Description                                        |
| ------- | ------------- | -------------------------------------------------- |
| `60001` | Unknown       | Generic gas price check failure                    |
| `60010` | Configuration | Missing ELEPHANT_RPC_URL                           |
| `60011` | Configuration | Missing GAS_PRICE_MAX_GWEI                         |
| `60020` | RPC/Network   | Unable to retrieve gas price from RPC              |
| `60021` | RPC/Network   | RPC connection error (`ECONNREFUSED`, `ETIMEDOUT`) |
| `60022` | RPC/Network   | RPC timeout                                        |
| `60030` | Gas Price     | Gas price exceeded threshold after max wait time   |

> **Note**: GasPriceCheck errors are classified using pattern matching against the error message. The Lambda sends the error code via `sendTaskFailure`, and the Step Function extracts it for EventBridge events. See `workflow/lambdas/gas-price-checker/index.mjs` for full mapping.

> **Note**: The gas price checker retries for a maximum of 10 minutes (configurable via `GAS_PRICE_MAX_WAIT_MINUTES` env var) before failing with error code `60030`. The retry interval is configurable via `GAS_PRICE_WAIT_MINUTES` (default 2 minutes).

#### Submit Error Codes (`60xxx` - Blockchain Submit)

| Code    | Category            | Description                                        |
| ------- | ------------------- | -------------------------------------------------- |
| `60001` | GasPriceCheck       | Generic gas price check failure                    |
| `60010` | GasPriceCheck       | Missing ELEPHANT_RPC_URL                           |
| `60011` | GasPriceCheck       | Missing GAS_PRICE_MAX_GWEI                         |
| `60020` | GasPriceCheck       | Unable to retrieve gas price from RPC              |
| `60021` | GasPriceCheck       | RPC connection error                               |
| `60022` | GasPriceCheck       | RPC timeout                                        |
| `60030` | GasPriceCheck       | Gas price too high after max wait time             |
| `60101` | Message Parsing     | Missing SQS record body                            |
| `60102` | Message Parsing     | Invalid message body format (not JSON array)       |
| `60103` | Message Parsing     | Empty transaction items array                      |
| `60104` | Message Parsing     | JSON parse error                                   |
| `60201` | Environment Config  | Missing ENVIRONMENT_BUCKET                         |
| `60202` | Environment Config  | Missing ELEPHANT_KEYSTORE_S3_KEY                   |
| `60203` | Environment Config  | Missing ELEPHANT_KEYSTORE_PASSWORD                 |
| `60204` | Environment Config  | Missing ELEPHANT_DOMAIN                            |
| `60205` | Environment Config  | Missing ELEPHANT_API_KEY                           |
| `60206` | Environment Config  | Missing ELEPHANT_ORACLE_KEY_ID                     |
| `60207` | Environment Config  | Missing ELEPHANT_FROM_ADDRESS                      |
| `60208` | Environment Config  | Missing ELEPHANT_RPC_URL                           |
| `60301` | S3/Keystore         | Keystore body not found in S3                      |
| `60302` | S3/Keystore         | S3 download failed                                 |
| `60401` | Blockchain/Contract | Nonce already used (`already known`)               |
| `60402` | Blockchain/Contract | Nonce too low                                      |
| `60403` | Blockchain/Contract | Insufficient funds                                 |
| `60404` | Blockchain/Contract | Gas estimation failed (`gas required exceeds`)     |
| `60405` | Blockchain/Contract | Transaction underpriced                            |
| `60406` | Blockchain/Contract | Execution reverted                                 |
| `60407` | Blockchain/Contract | Invalid transaction                                |
| `60408` | Blockchain/Contract | RPC connection error (`ECONNREFUSED`, `ETIMEDOUT`) |
| `60409` | Blockchain/Contract | RPC timeout                                        |
| `60410` | Blockchain/Contract | Invalid parameters                                 |
| `60411` | Blockchain/Contract | Contract error                                     |
| `60412` | Blockchain/Contract | Submit CLI returned failure status                 |
| `60501` | File I/O            | CSV write failed                                   |
| `60502` | File I/O            | Transaction status CSV read failed                 |
| `60503` | File I/O            | Submit errors CSV read failed                      |
| `60999` | Unknown             | Unknown/unclassified submit error                  |

> **Note**: Blockchain/Contract errors (604xx) are classified from the `errorMessage` column in `submit_errors.csv`, which contains JSON-serialized EVM RPC responses. The classification uses regex pattern matching. See `workflow/lambdas/submit/index.mjs` for full mapping.

### Phase Values

| Phase                    | Description                         |
| ------------------------ | ----------------------------------- |
| `Prepare`                | Input preparation and validation    |
| `Transform`              | Data transformation processing      |
| `SVL`                    | Schema Validation Layer             |
| `MVL`                    | Mirror Validation Layer             |
| `Hash`                   | Hashing and fingerprinting          |
| `Upload`                 | IPFS/storage upload                 |
| `GasPriceCheck`          | Gas price verification              |
| `Submit`                 | Final blockchain submission         |
| `TransactionStatusCheck` | Transaction confirmation monitoring |
| `AutoRepair`             | AI-driven error resolution          |

### Step Values

| Step                     | Phase                  | Description                       |
| ------------------------ | ---------------------- | --------------------------------- |
| `Prepare`                | Prepare                | Data preparation/download         |
| `EvaluateTransform`      | Transform              | Evaluation of transform errors    |
| `Transform`              | Transform              | Data transformation processing    |
| `SVL`                    | SVL                    | Schema validation                 |
| `MVL`                    | MVL                    | Mirror validation                 |
| `EvaluateHash`           | Hash                   | Evaluation of hash errors         |
| `Hash`                   | Hash                   | Hashing and CID-s calculation     |
| `EvaluateUpload`         | Upload                 | Evaluation of upload errors       |
| `Upload`                 | Upload                 | IPFS upload                       |
| `CheckGasPrice`          | GasPriceCheck          | Gas price verification            |
| `SubmitToBlockchain`     | Submit                 | Submit transactions to blockchain |
| `CheckTransactionStatus` | TransactionStatusCheck | Verify transaction confirmation   |
| `AutoRepair`             | AutoRepair             | AI-driven error resolution        |

> Additional steps will be added as other workflows are integrated.

### Status Values

| Status        | Description                                             |
| ------------- | ------------------------------------------------------- |
| `SCHEDULED`   | Step is scheduled for execution                         |
| `IN_PROGRESS` | Step is currently executing                             |
| `SUCCEEDED`   | Step completed successfully                             |
| `FAILED`      | Execution has failed, paused, and requires intervention |

---

## Event Type: `ElephantErrorResolved`

Emitted when errors have been successfully resolved (e.g., by auto-repair).

### Event Structure

```json
{
  "source": "elephant.workflow",
  "detail-type": "ElephantErrorResolved",
  "detail": {
    "executionId": "string",
    "errorCode": "string"
  }
}
```

### Field Definitions

| Field         | Type   | Description                                               |
| ------------- | ------ | --------------------------------------------------------- |
| `executionId` | string | Resolves all errors for this execution (optional)         |
| `errorCode`   | string | Resolves this error code across all executions (optional) |

> **Note**: At least one of `executionId` or `errorCode` must be provided.

---

## Event Type: `ElephantErrorFailedToResolve`

Emitted when error resolution has failed (e.g., auto-repair exhausted retries).

### Event Structure

```json
{
  "source": "elephant.workflow",
  "detail-type": "ElephantErrorFailedToResolve",
  "detail": {
    "executionId": "string",
    "errorCode": "string"
  }
}
```

### Field Definitions

| Field         | Type   | Description                                                         |
| ------------- | ------ | ------------------------------------------------------------------- |
| `executionId` | string | Marks all errors for this execution as unrecoverable (optional)     |
| `errorCode`   | string | Marks this error code as unrecoverable across executions (optional) |

> **Note**: At least one of `executionId` or `errorCode` must be provided. Errors marked as unrecoverable are excluded from future auto-repair attempts.

---

## Resumption

To resume a failed (parked) workflow, call Step Functions `SendTaskSuccess` with the `taskToken`:

```javascript
import { SFNClient, SendTaskSuccessCommand } from "@aws-sdk/client-sfn";

await sfnClient.send(
  new SendTaskSuccessCommand({
    taskToken: event.detail.taskToken,
    output: JSON.stringify({
      resolved: true,
      // additional data as needed
    }),
  }),
);
```

To fail the workflow:

```javascript
import { SendTaskFailureCommand } from "@aws-sdk/client-sfn";

await sfnClient.send(
  new SendTaskFailureCommand({
    taskToken: event.detail.taskToken,
    error: "ResolutionFailed",
    cause: "Unable to resolve errors",
  }),
);
```
