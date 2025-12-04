## Overview

The `workflow-events` module processes workflow-related events emitted to Amazon EventBridge and exposes a helper Lambda for querying failed executions and their errors from the shared DynamoDB table.

- **`workflow-event-handler` Lambda**: Subscribed to a WorkflowEvent EventBridge rule; persists workflow errors into the `workflow-errors` DynamoDB table.
- **`get-execution` Lambda**: Direct-invocation utility that returns the execution with the most/least errors (optionally filtered by error type) together with its individual error records.

All resources are defined in `template.yaml` as an AWS SAM application.

---

## Event Sources and Triggers

### EventBridge rule for `workflow-event-handler`

`WorkflowEventHandlerFunction` is wired to an EventBridge rule with the following pattern (see `template.yaml`):

- **source**: `elephant.workflow`
- **detail-type**:
  - `WorkflowEvent`
  - `ElephantErrorResolved`
  - `ElephantErrorFailedToResolve`

Only events that match this pattern will trigger the `workflow-event-handler` Lambda.

### Expected event detail schema (`WorkflowEventDetail`)

The Lambda expects the EventBridge event detail to conform to the `WorkflowEventDetail` type from the shared layer (`shared/types.ts`):

- **executionId** (`string`): Unique identifier of the workflow execution.
- **county** (`string`): County identifier for the execution.
- **status** (`"SUCCEEDED" | "FAILED" | "IN_PROGRESS" | "SCHEDULED"`): Current workflow step status.
- **phase** (`string`): Current phase of the workflow.
- **step** (`string`): Current step within the phase.
- **taskToken** (`string`, optional): Step Functions task token for callback, if applicable.
- **errors** (`WorkflowError[]`): Array of error objects:
  - **code** (`string`): Error code identifier.
  - **details** (`Record<string, unknown>`): Arbitrary key–value payload with error context.

When `errors` is non-empty, `workflow-event-handler` writes:

- Aggregated error records (`ErrorRecord`)
- Execution-to-error links (`ExecutionErrorLink`)
- A failed-execution record (`FailedExecutionItem`)

into the `workflow-errors` DynamoDB table, using several GSIs for analytics and lookups.

### Expected event detail schema (`ElephantErrorResolved` and `ElephantErrorFailedToResolve`)

These events are used to manage the lifecycle of errors in the system. Both share the same schema structure (`ElephantErrorResolvedDetail`).

- **`ElephantErrorResolved`**: Signals that an error (or all errors for an execution) has been successfully resolved. The handler will delete the corresponding error records from the DynamoDB table.
- **`ElephantErrorFailedToResolve`**: Signals that an automated resolution attempt failed. The handler will mark the corresponding error records as `maybeUnrecoverable` in the DynamoDB table, excluding them from standard error counts.

**Schema:**

The event detail must contain **at least one** of the following fields:

- **executionId** (`string`, optional):
  - If provided, the action applies to **all errors** associated with this execution ID.
  - For `ElephantErrorResolved`: All errors for this execution are resolved across ALL executions that share the same error codes.
  - For `ElephantErrorFailedToResolve`: All errors for this execution are marked as `maybeUnrecoverable`.

- **errorCode** (`string`, optional):
  - If provided (and `executionId` is absent), the action applies to **this specific error code** across ALL executions.
  - For `ElephantErrorResolved`: The error code is deleted from all executions.
  - For `ElephantErrorFailedToResolve`: The error code is marked as `maybeUnrecoverable` for all executions.

**Examples:**

Resolve all errors for a specific execution:
```json
{
  "executionId": "arn:aws:states:us-east-1:123456789:execution:my-state-machine:execution-name"
}
```

Resolve a specific error code globally:
```json
{
  "errorCode": "MV-ERROR-CODE-123"
}
```

---

## `get-execution` Lambda

### Function name and ARN

In `template.yaml`, the function is defined as:

- **Logical ID**: `GetExecutionFunction`
- **Deployed function name**: `${EnvironmentName}-get-execution`  
  (for example, with `EnvironmentName=dev`, the name is `dev-get-execution`).

The template exposes the ARN via an output:

- **Output key**: `GetExecutionFunctionArn`
- **Value**: `!GetAtt GetExecutionFunction.Arn`

### How to get the `get-execution` Lambda ARN

You can retrieve the ARN via any of the following methods (replace `<STACK_NAME>` and `<ENV>` as appropriate):

- **CloudFormation console**:
  - Open the stack that deployed `workflow-events`.
  - Go to the **Outputs** tab.
  - Look for the output named **`GetExecutionFunctionArn`** – its value is the Lambda ARN.

- **AWS CLI – CloudFormation outputs**:

```bash
aws cloudformation describe-stacks \
  --stack-name <STACK_NAME> \
  --query "Stacks[0].Outputs[?OutputKey=='GetExecutionFunctionArn'].OutputValue" \
  --output text
```

- **AWS CLI – by function name** (if you know `EnvironmentName`):

```bash
ENVIRONMENT_NAME=<ENV> # e.g. MWAAEnvironment, dev, prod
FUNCTION_NAME="${ENVIRONMENT_NAME}-get-execution"

aws lambda get-function \
  --function-name "$FUNCTION_NAME" \
  --query "Configuration.FunctionArn" \
  --output text
```

---

## `get-execution` request schema

`get-execution` is designed for **direct Lambda invocation** (for example via CLI, SDK, or Step Functions task), not behind API Gateway. The handler validates its input with Zod (`GetExecutionEventSchema`).

### Shape

The input event must match:

- **sortOrder** (`"most" | "least"`, required):
  - `"most"` – return the execution with the **highest** error count.
  - `"least"` – return the execution with the **lowest** error count.
- **errorType** (`string`, optional):
  - If provided, must be a **non-empty string**.
  - Interpreted as the **first two characters** of the error code (for example, `"MV"` for all errors whose code starts with `"MV"`).
  - When present, the function queries via the **GS3** index; otherwise it uses **GS1**.

Example valid payloads:

```json
{ "sortOrder": "most" }
```

```json
{ "sortOrder": "least", "errorType": "MV" }
```

---

## `get-execution` response schema

The function returns a JSON object with the following shape:

- **success** (`boolean`): `true` if the call succeeded (including the case where no matching execution is found), `false` on validation or runtime error.
- **execution** (`ExecutionBusinessData | null`):
  - When found: an object representing the failed execution, containing **business fields only**.
  - When no execution matches the query: `null`.
- **errors** (`ErrorBusinessData[]`): Array of error records for the returned execution, again with **business fields only**.
- **error** (`string | ZodIssue[]`, optional):
  - Present only when `success === false`.
  - For validation errors: an array of Zod issues.
  - For runtime errors: a human-readable error string.

### `ExecutionBusinessData`

When `execution` is not `null`, it has the following fields:

- **executionId** (`string`): Identifier of the failed execution.
- **status** (`"failed" | "maybeSolved" | "solved"`): Current status bucket for the execution.
- **errorType** (`string`): Error type (first two characters shared by all error codes in the execution).
- **county** (`string`): County identifier.
- **totalOccurrences** (`number`): Total number of error occurrences recorded for this execution.
- **openErrorCount** (`number`): Count of unique unresolved errors.
- **uniqueErrorCount** (`number`): Count of unique error codes in the execution.
- **taskToken** (`string`, optional): Step Functions task token associated with the failed execution, if applicable.
- **createdAt** (`string`, ISO timestamp): When the execution item was first created.
- **updatedAt** (`string`, ISO timestamp): When the execution item was last updated.

All DynamoDB-specific fields (for example `PK`, `SK`, `GS1PK`, `GS1SK`, `GS3PK`, `GS3SK`, `entityType`) are removed in the response.

### `ErrorBusinessData`

Each element in the `errors` array has:

- **errorCode** (`string`): Error code identifier.
- **status** (`"failed" | "maybeSolved" | "solved"`): Resolution status for this error within the execution.
- **occurrences** (`number`): Number of times this error occurred within the execution.
- **errorDetails** (`string`): JSON-encoded key–value payload with the error details.
- **executionId** (`string`): The failed execution that the error belongs to.
- **county** (`string`): County identifier.
- **createdAt** (`string`, ISO timestamp): When this link item was first created.
- **updatedAt** (`string`, ISO timestamp): When this link item was last updated.

---

## Example invocation via AWS CLI

After obtaining the function ARN or name (see above), you can invoke `get-execution` like this:

```bash
FUNCTION_ARN="<GET_EXECUTION_FUNCTION_ARN>"

aws lambda invoke \
  --function-name "$FUNCTION_ARN" \
  --payload '{"sortOrder":"most","errorType":"MV"}' \
  --cli-binary-format raw-in-base64-out \
  response.json
```

The `response.json` file will contain a `GetExecutionResponse` object as described above.
