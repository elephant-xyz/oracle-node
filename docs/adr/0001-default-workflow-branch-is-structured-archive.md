# Default workflow branch is Structured Archive

The workflow defaults to the **Structured Archive** branch: after Transform succeeds, an execution archives by reusing the existing transformed output ZIP in S3 and emits a Structured Archive success event. **Minting** remains available only when any caller explicitly supplies `workflowBranch: "minting"`; this makes every omitted branch value safe by default while preserving the existing Transform → SVL → Hash → IPFS Upload → Submit path for intentional minting runs.

## Consequences

- Missing `workflowBranch` means `structured_archive`, and the effective value is normalized into workflow state before transformation.
- Invalid `workflowBranch` values fail before transformation begins instead of silently defaulting or producing artifacts.
- The branch point is after Transform because both branches share the same transform output.
- Structured Archive does not create a second S3 copy or a branch-specific output alias in the first version.

- All callers share the same default: omitted `workflowBranch` runs Structured Archive, including tooling that previously assumed Minting.
