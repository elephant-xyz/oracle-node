# Open data: consolidate per-property and publish to Filebase/IPFS

Oracle's collected datasets must become openly published, durably indexed, and agent-queryable so any coding agent (NEO and others) can build on them permissionlessly (Oracle #2 milestone). This ADR records the storage + data-model decision. On-chain submission and the Elephant.xyz UI are out of scope for this milestone.

## Decision

Keep **IPFS** as the storage layer (via **Filebase**), and fix the scaling problem in the **data model**: consolidate each property's graph into **one file per property**, then publish those files to IPFS.

- **IPFS is mandatory.** The data must be open/permissionless/decentralized — anyone can fetch it by CID and scale their own reads. A database or object store *we* host would make us responsible for scaling and would be permissioned, which defeats the goal.
- **The fix is granularity, not the provider.** Elephant's data is a graph; published per-node it explodes into **billions of sub-1KB objects** even for one county. That is unscalable to retrieve/index on *any* IPFS provider (per-object DHT/metadata cost; plus the ~256 KB block floor wastes space), and it is what failed on Pinata. Consolidating all of a property's data (permits, tenants/Sunbiz, reviews/BBB, appraisal) into **one file** drops the object count to **~300k (one per property)** — tractable to pin, fetch, and index.
- **Filebase** is the IPFS provider: S3-compatible API *and* genuinely distributed (unlike Pinata, which was effectively a single hosted service that throttled reads).

## Architecture

- **Consolidate at publish time:** from the query-db, for each property, assemble one consolidated JSON (relationships nested). Implemented as the `elephant-query-db` per-property consolidation exporter (batched so memory stays flat).
- **Address + publish:** each file gets an IPFS CIDv0 (computed locally; verified to equal the CID Filebase returns on upload). Files are uploaded to a Filebase bucket and pinned to IPFS.
- **Registry = a manifest** mapping property → CID, with provenance/freshness (`county`, `source`, `collectedAt`), published alongside the data. This is the durable, queryable index for this milestone.
- **MCP access** exposes the manifest so agents discover properties and resolve CIDs/provenance, layered on the existing `elephant-mcp` server.
- **NEO** (`elephant-xyz/catalog`) is rewired to read the open IPFS data via MCP instead of bundled files.

## Consequences

- Reads/scaling are owned by **consumers**: they fetch files by CID and index on their own side; we only publish. Filebase (distributed) serves availability — we are not the central query server.
- Data stays **content-addressed**, so any consumer can fetch a CID from any IPFS gateway, independent of provider.
- The registry is **off-chain** for this milestone (a published manifest). The fully-decentralized registry remains the blockchain (CIDs anchored on-chain → indexers like The Graph/Envio); consolidation **re-enables** that path (anchoring ~300k per-property CIDs is tractable, billions were not) as a follow-up — explicitly out of scope here.
- Per-county scoping (Sunbiz/BBB are address/contractor-matched, not a simple column) is deferred until a second county lands; the first cut publishes the full query-db (currently Lee-only).

## Status

Implemented and **executed for Lee** (verified 2026-06-18): 4,664 consolidated per-property files + manifest published to Filebase bucket `elephant-oracle-open-data` and confirmed retrievable from the public `ipfs.io` gateway by CID. Consolidation exporter: `elephant-query-db` PR. Remaining: MCP access + rewiring/validating NEO.
