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

| Code       | Phase     | Description                                                                                                                                                                               |
| ---------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `20001`    | Transform | Generic transform step failure or exception                                                                                                                                               |
| `30<hash>` | SVL       | Schema validation error. The `<hash>` is a SHA256 hash computed from `error_message#error_path#county`, uniquely identifying each distinct validation error. Example: `30a1b2c3d4e5f6...` |
| `31001`    | SVL       | SVL runtime exception (non-validation failure)                                                                                                                                            |
| `40001`    | Hash      | Generic hash step failure or exception                                                                                                                                                    |
| `50001`    | Upload    | Generic upload step failure or exception                                                                                                                                                  |

> **Note on SVL validation errors (code `30<hash>`)**: Each unique validation error (determined by the combination of error message, error path, and county) receives its own error code. This enables precise tracking and resolution of individual validation issues across executions. The `error_hash` is also included in the `details` object for reference.

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
