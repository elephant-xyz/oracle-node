# Oracle Node Workflow Context

This context describes the workflow language used to process county source data into either archived structured scrape artifacts or mintable data outputs.

## Language

**Structured Archive**:
The default workflow branch that transforms county source data and stores structured scrape artifacts in Amazon S3.
_Avoid_: current flow, S3 upload flow, scraped data flow

**Structured Archive Artifact**:
The transformed output ZIP stored by the **Structured Archive** branch as the branch's primary S3 artifact.
_Avoid_: normalized JSON, extracted files, validation output

**Transform Artifact**:
The transformed output ZIP produced immediately after county transformation.
_Avoid_: structured copy, archive copy

**Minting**:
The explicit workflow branch that transforms county source data and continues through schema validation, hash generation, IPFS upload, and submit.
_Avoid_: current flow, hash/upload flow, validate flow

**Workflow Branch**:
The execution input selection that determines whether the workflow runs **Structured Archive** or **Minting** after transformation.
_Avoid_: mode, mint flag, flow type

**Workflow Branch Flag**:
The operator-facing start option that supplies the **Workflow Branch** value.
_Avoid_: minting flag, mode flag

## Relationships

- A workflow execution chooses exactly one branch: **Structured Archive** or **Minting**.
- A missing **Workflow Branch** means **Structured Archive**.
- The effective **Workflow Branch** is normalized into workflow state before transformation.
- An unrecognized **Workflow Branch** is an invalid execution input, not a request for the default branch.
- An invalid **Workflow Branch** fails before transformation begins.
- Every caller that omits the **Workflow Branch** runs **Structured Archive**, including callers that previously expected **Minting**.
- S3-triggered executions run **Structured Archive** unless a controlled direct caller explicitly supplies the **Minting** branch.
- Operator tooling exposes a **Workflow Branch Flag** for explicit branch selection.
- **Structured Archive** does not perform the **Minting** branch's schema validation, hash generation, IPFS upload, or submit steps.
- A **Structured Archive** execution uses the **Transform Artifact** as its **Structured Archive Artifact**.
- A **Structured Archive** execution exposes its artifact through the existing transform result rather than a branch-specific result alias.
- A **Structured Archive** execution emits a branch-level success event after transformation completes.

## Example dialogue

> **Dev:** "Should this execution run **Minting** after Transform?"
> **Domain expert:** "No — unless the caller explicitly asks for **Minting**, use **Structured Archive** and save the transformed scrape artifacts to S3."

> **Dev:** "Should the **Structured Archive Artifact** be normalized JSON?"
> **Domain expert:** "No — for the first version, archive the transformed output ZIP only."

> **Dev:** "Should **Structured Archive** copy the ZIP to a new S3 prefix?"
> **Domain expert:** "No — reuse the **Transform Artifact** URI as the archive artifact."

> **Dev:** "What happens if `workflowBranch` is omitted?"
> **Domain expert:** "Treat it as **Structured Archive**; only `minting` explicitly runs the **Minting** branch."

> **Dev:** "Should omitted `workflowBranch` stay absent in execution state?"
> **Domain expert:** "No — normalize the effective branch to `structured_archive` before transformation."

> **Dev:** "When should an invalid `workflowBranch` fail?"
> **Domain expert:** "Before transformation starts; invalid inputs should not produce transform artifacts."

> **Dev:** "Can an S3 object upload request **Minting** by default?"
> **Domain expert:** "No — S3-triggered executions archive by default; **Minting** requires a controlled direct caller."

> **Dev:** "Should direct submit tooling preserve **Minting** automatically?"
> **Domain expert:** "No — all callers share the same default; they must request `minting` explicitly."

> **Dev:** "How should an operator request **Minting** from start tooling?"
> **Domain expert:** "Use the **Workflow Branch Flag** with `minting`; do not use a separate minting-specific flag."

> **Dev:** "Should the final output add `structuredArchive.artifactS3Uri`?"
> **Domain expert:** "No — the first version uses the existing transform result as the output contract."

> **Dev:** "How do operators know a default execution intentionally ended after Transform?"
> **Domain expert:** "Look for the **Structured Archive** success event after the Transform success event."

## Flagged ambiguities

- "upload" was used for both IPFS upload and S3 persistence — resolved: **Minting** uses IPFS upload, while **Structured Archive** stores artifacts in S3.
