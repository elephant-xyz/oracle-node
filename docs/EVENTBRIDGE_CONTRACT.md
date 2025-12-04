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

| Code            | Phase     | Description                                                                                                                                                                               |
| --------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `01xxx<county>` | Prepare   | Lambda infrastructure error. `01002`=generic, `01003-01006`=input.csv errors, `01008-01015`=S3/config errors, `01016-01020`=taskToken/Step Functions errors. Example: `01002Hamilton`     |
| `10xxx<county>` | Prepare   | CLI prepare error. `Example: `10050Broward`                                                                                                                                               |
| `20<county>`    | Transform | Transform step failure or exception. The `<county>` is the county name being processed. Example: `20Cook`                                                                                 |
| `30<hash>`      | SVL       | Schema validation error. The `<hash>` is a SHA256 hash computed from `error_message#error_path#county`, uniquely identifying each distinct validation error. Example: `30a1b2c3d4e5f6...` |
| `31001`         | SVL       | SVL runtime exception (non-validation failure)                                                                                                                                            |
| `40001`         | Hash      | Generic hash step failure or exception                                                                                                                                                    |
| `50001`         | Upload    | Generic upload step failure or exception                                                                                                                                                  |

| Code    | Description                             |
| ------- | --------------------------------------- |
| `01002` | Generic prepare processing error        |
| `01003` | Error reading input.csv from zip        |
| `01004` | Error parsing input.csv                 |
| `01005` | input.csv is empty or has no data rows  |
| `01006` | input.csv not found in zip file         |
| `01008` | Could not extract county from zip       |
| `01009` | Failed to download config file from S3  |
| `01010` | Invalid S3 path format                  |
| `01011` | Missing required field: input_s3_uri    |
| `01012` | Failed to download input object from S3 |
| `01013` | Failed to download flow file from S3    |
| `01014` | Invalid browser flow parameter format   |
| `01015` | Empty key in browser flow parameter     |
| `01016` | Missing taskToken in SQS message        |
| `01017` | Failed to send task success             |
| `01018` | Failed to send task failure             |
| `01019` | Failed to emit EventBridge event        |
| `01020` | No taskToken available                  |

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

### Phase Values

| Phase       | Description                      |
| ----------- | -------------------------------- |
| `Prepare`   | Input preparation and validation |
| `Transform` | Data transformation processing   |
| `SVL`       | Schema Validation Layer          |
| `MVL`       | Mirror Validation Layer          |
| `Hash`      | Hashing and fingerprinting       |
| `Upload`    | IPFS/storage upload              |
| `Submit`    | Final submission                 |

### Step Values

| Step                | Phase     | Description                    |
| ------------------- | --------- | ------------------------------ |
| `Prepare`           | Prepare   | Data preparation/download      |
| `EvaluateTransform` | Transform | Evaluation of transform errors |
| `Transform`         | Transform | Data transformation processing |
| `SVL`               | SVL       | Schema validation              |
| `MVL`               | MVL       | Mirror validation              |
| `EvaluateHash`      | Hash      | Evaluation of hash errors      |
| `Hash`              | Hash      | Hashing and CID-s calculation  |
| `EvaluateUpload`    | Upload    | Evaluation of upload errors    |
| `Upload`            | Upload    | IPFS upload                    |

> Additional steps will be added as other workflows are integrated.

### Status Values

| Status        | Description                                             |
| ------------- | ------------------------------------------------------- |
| `SCHEDULED`   | Step is scheduled for execution                         |
| `IN_PROGRESS` | Step is currently executing                             |
| `SUCCEEDED`   | Step completed successfully                             |
| `FAILED`      | Execution has failed, paused, and requires intervention |

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
