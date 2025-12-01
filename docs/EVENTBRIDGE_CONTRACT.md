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

| Field         | Type   | Description                                        |
| ------------- | ------ | -------------------------------------------------- |
| `executionId` | string | Step Functions execution ARN                       |
| `county`      | string | County identifier being processed                  |
| `status`      | string | Current workflow status (e.g., `PARKED`, `FAILED`) |
| `phase`       | string | High-level workflow phase                          |
| `step`        | string | Granular step within the phase                     |
| `taskToken`   | string | Step Functions task token for resumption           |
| `errors`      | array  | List of error objects                              |

### Error Object Schema

| Field     | Type   | Description                                                              |
| --------- | ------ | ------------------------------------------------------------------------ |
| `code`    | string | Error code identifier (e.g., `TRANSFORM_FAILED`, `SVL_VALIDATION_ERROR`) |
| `details` | object | Free-form object with error-specific details                             |

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

| Status        | Description                                    |
| ------------- | ---------------------------------------------- |
| `SCHEDULED`   | Step is scheduled for execution                |
| `IN_PROGRESS` | Step is currently executing                    |
| `SUCCEEDED`   | Step completed successfully                    |
| `PARKED`      | Execution is paused awaiting external action   |
| `FAILED`      | Execution has failed and requires intervention |

---

## Resumption

To resume a parked workflow, call Step Functions `SendTaskSuccess` with the `taskToken`:

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
