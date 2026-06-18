# Open-data storage moves off Pinata to an S3-compatible IPFS store

Oracle's collected datasets must become openly published, durably indexed, and agent-queryable (Oracle #2 milestone). This ADR records the storage architecture and the storage selection — the decision deliverable. On-chain submission is **out of scope** for this milestone; the goal is publish-to-storage plus a durable queryable index. Elephant.xyz UI is excluded.

## Storage selection: S3-compatible IPFS provider (Filebase)

Public storage moves off **Pinata** to **Filebase**, an S3-compatible provider that pins content to IPFS. Filebase is selected because it satisfies both requirements at once: it is **IPFS-compatible** (content-addressed; objects are pinned and resolvable by CID through any IPFS gateway) **and** exposes an **S3 API** (the "S2"/S3-compatible option). Plain AWS S3 fails the IPFS-compatible requirement; web3.storage/Filecoin/Lighthouse fail the S3-compatible requirement.

The change is contained because CIDs are computed locally by `@elephant-xyz/cli` `hash` **before** upload, so the CID scheme is identical regardless of storage provider and no downstream consumer breaks. `oracle-node` already depends on `@aws-sdk/client-s3` in its workflow lambdas, so publishing to Filebase is a `PutObject` against the provider endpoint rather than a new transport.

## Architecture

- **CID generation** stays local and unchanged (`hash` → CID v0).
- **Upload** swaps the hard-coded Pinata path for a pluggable `StorageProvider` whose first implementation targets Filebase. The two call sites are `elephant-cli` `upload` (service instantiation) and `oracle-node` `upload-worker` (credential env var). Built in the publish subtask.
- **Durable index** is a public newline-delimited JSON manifest (`datasets-manifest.jsonl`) in the store — one record per published dataset. The existing envio-subgraph indexes on-chain `DataSubmitted` events only and is therefore not usable for an off-chain index. Built in the index subtask.
- **Provenance and freshness** are carried by the manifest record: `{ schemaVersion, cid, county, source, dataGroup, collectedAt, publishedAt, oracle }`.
- **MCP access** is additive tools over the manifest (query by county/source, resolve provenance/freshness), layered on the existing `elephant-mcp` server without changing its architecture. Built in the MCP subtask.

## Consequences

- Storage is provider-pluggable behind a `StorageProvider` interface; Filebase today, other IPFS/S3 providers later without changing callers (forward-compatible agent access).
- Datasets remain content-addressed, so any future consumer can fetch a CID from any IPFS gateway independent of the provider.
- The durable index is purpose-built off-chain (manifest), decoupled from the Polygon contract and envio-subgraph; no on-chain submission is added or required by this milestone.
- The manifest is versioned (`schemaVersion`) and evolves additively; agents resolve datasets by `(county, source, dataGroup)` → CID.
- Filebase account and credentials are a prerequisite before the publish step can run.
