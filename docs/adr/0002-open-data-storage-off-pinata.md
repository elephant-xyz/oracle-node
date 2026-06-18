# Open data: consolidate per-property and publish to Filebase/IPFS

Oracle's collected datasets must become openly published, durably indexed, and agent-queryable so any coding agent (NEO and others) can build on them permissionlessly (Oracle #2 milestone). This ADR records the storage + data-model decision. On-chain submission and the Elephant.xyz UI are out of scope for this milestone.

## Decision

Keep **IPFS** as the storage layer (via **Filebase**), and fix the scaling problem in the **data model**: consolidate each property's graph into **one file per property**, then publish those files to IPFS.

- **IPFS is mandatory.** The data must be open/permissionless/decentralized — anyone can fetch it by CID and scale their own reads. A database or object store _we_ host would make us responsible for scaling and would be permissioned, which defeats the goal.
- **The fix is granularity, not the provider.** Elephant's data is a graph; published per-node it explodes into **billions of sub-1KB objects** even for one county. That is unscalable to retrieve/index on _any_ IPFS provider (per-object DHT/metadata cost; plus the ~256 KB block floor wastes space), and it is what failed on Pinata. Consolidating all of a property's data (permits, tenants/Sunbiz, reviews/BBB, appraisal) into **one file** drops the object count to **~300k (one per property)** — tractable to pin, fetch, and index.
- **Filebase** is the IPFS provider: S3-compatible API _and_ genuinely distributed (unlike Pinata, which was effectively a single hosted service that throttled reads).

## Architecture

- **Consolidate at publish time:** from the query-db, for each property, assemble one consolidated JSON (relationships nested). Implemented as the `elephant-query-db` per-property consolidation exporter (batched so memory stays flat).
- **Address + publish:** each file gets an IPFS CIDv0 (computed locally; verified to equal the CID Filebase returns on upload). Files are uploaded to a Filebase bucket and pinned to IPFS.
- **Registry = a manifest** published alongside the data: dataset-level `county` + `exportedAt`/`propertyCount`, plus one entry per property `{ propertyId, parcelIdentifier, cid, sha256, fileSizeBytes }`. This is the durable, queryable index for this milestone. Per-property freshness/provenance (`collectedAt`, source, etc.) lives **inside** each property file, not in the manifest entry.
- **MCP access (for agents):** the `elephant-mcp` server exposes `listOracleProperties` / `getOracleProperty` / `getOracleDatasetInfo` over the manifest, so any coding agent can discover properties and fetch them by CID. Manifest CID is configurable via `ORACLE_OPEN_DATA_MANIFEST_CID`.
- **NEO** (`elephant-xyz/catalog`) reads the open data **directly** (manifest + per-property CIDs from IPFS), behind a flag, replacing its bundled files. A web app consuming the open structure directly is the right fit; the stdio MCP server is the _agent_ access path, not a web-app runtime dependency.

## Consequences

- Reads/scaling are owned by **consumers**: they fetch files by CID and index on their own side; we only publish. Filebase (distributed) serves availability — we are not the central query server.
- Data stays **content-addressed**, so any consumer can fetch a CID from any IPFS gateway, independent of provider.
- The registry is **off-chain** for this milestone (a published manifest). The fully-decentralized registry remains the blockchain (CIDs anchored on-chain → indexers like The Graph/Envio); consolidation **re-enables** that path (anchoring ~300k per-property CIDs is tractable, billions were not) as a follow-up — explicitly out of scope here.
- Per-county scoping (Sunbiz/BBB are address/contractor-matched, not a simple column) is deferred until a second county lands; the first cut publishes the full query-db (currently Lee-only).
- Published per-property files include **permit-contact PII** (applicant/contractor names, phones, emails). This is county public-record data; publishing it aggregated and permanently on public IPFS was a deliberate, confirmed-acceptable choice.
- **Consuming the CIDs:** files are UnixFS **dag-pb** CIDs (CIDv0), not raw blocks — integrity verification must recompute the UnixFS CID (e.g. via `ipfs-only-hash`), not sha256 the raw gateway bytes. The `elephant-mcp` server requires **Node 22** (its native deps lack Node 24 prebuilds).

## Status

**Complete (2026-06-18)** — all milestone ACs met, validated end-to-end on Lee:

- **Consolidation exporter** — `elephant-query-db` (PR #1, merged).
- **Published** — 4,664 consolidated per-property files + manifest to Filebase bucket `elephant-oracle-open-data`; confirmed retrievable from the public `ipfs.io` gateway by CID.
- **MCP access** — `elephant-mcp` (PR #19); verified via a live server+client round-trip returning real Lee data.
- **NEO rewired** — `catalog` (PR #6); verified the app's own data layer (`getPropertyByParcelId`) serves Lee from IPFS.

## Known limitations / follow-ups

- **Data gaps:** published files omit `photos`, `folioId` (LEEPA link), `municipality`, FEMA fields, and tax `exemption` — these render empty when NEO runs in open-data mode. Add them to the consolidation export to close.
- **Manifest `filePath`** currently records an absolute local path; should be relative/omitted (consumers address by `cid`).
- **Re-publish** produces a new manifest CID — update `ORACLE_OPEN_DATA_MANIFEST_CID` on the MCP server and the NEO sync to point at it.
- **Repo Node version:** pin to Node 22 (`.nvmrc` / `engines` upper bound) so consumers don't hit the Node 24 native-dep failures.
