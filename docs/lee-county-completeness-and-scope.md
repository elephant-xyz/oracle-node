# Lee County — Oracle Data Completeness & Scope (Oracle #1)

Records the completeness reconciliation, scope definition, future-compatibility
seams, and explicit exclusions for the _Oracle Agent Public Data Discovery and
Refresh_ milestone (Lee County, FL). Closes acceptance criteria **#9, #12, #13**
and documents the permit side of **#10/#11**.

## Scope

This milestone is an **industrial-first curated commercial sample** of Lee
County — not a full-county load. The query database
(`elephant-xyz/elephant-query-db`) is loaded via a scope manifest
(`curated-commercial-final-1000-success-manifest.json`, ~1,007 seed parcels,
expanded by the property-first pipeline) that links the four sources across the
same commercial parcels. Permit completeness below uses a denominator of
**1,233** — the permit-eligible (commercial/industrial) subset of the loaded
curated properties that was enqueued for the property-first refresh (seed ~1,007
→ 1,233 eligible after the property-first / incremental-industrial-scope
expansion).

"Complete against source availability" (AC #9) therefore means complete **within
the curated scope** — each curated parcel's appraisal, permits, address-matched
Sunbiz, and BBB records are loaded. The raw upstream universe (516,848 seed
parcels; ~379,467 Sunbiz records) is the source pool the curated scope is drawn
from, not the completeness target. See
`elephant-query-db/docs/data-load-and-matching-plan.md`.

## Completeness reconciliation (2026-06-18)

| Source             | Collection                  | Loaded (Neon query DB)                            | Completeness (within scope)                               |
| ------------------ | --------------------------- | ------------------------------------------------- | --------------------------------------------------------- |
| Property/appraisal | scrape (LeePA)              | 4,664 properties / 4,390 parcels                  | Complete (1 null-FK row, negligible)                      |
| Permits            | scrape (Accela, multi-page) | 176,695 `property_improvements`                   | 1,211 / 1,233 curated parcels (98.2%) — see gap below     |
| Sunbiz             | bulk download (FL DoS)      | 31,679 registrations / 63,306 addresses           | Complete (address-scoped; all 10 source files; FK-linked) |
| BBB                | scrape (BBB)                | 2,619 profiles / 5,851 reviews / 5,201 complaints | Complete (matches staged source count)                    |

The permit total rose from ~141,303 (pre-refresh) to 176,695 (+25%) after the
multi-page capture fix landed.

## Known gap: 22 high-permit parcels

22 of the 1,233 curated parcels (1.8%) were not fully captured. All 22 are the
highest-permit parcels (314–530 permits each); each exceeds the AWS Lambda hard
15-minute (900s) execution limit during per-permit detail capture, so the worker
times out and the message is routed to the dead-letter queue.

**Root cause:** the worker captures each permit's detail page with a separate
browser navigation (~3–5s per permit). The empirical timeout threshold is ~310
permits per parcel; parcels below that completed normally.

**Remediation (not yet performed):** re-capture these parcels off-Lambda (no
15-minute limit) via `scripts/harvest-lee-permits-by-parcel.mjs` or a Fargate
task, and/or make the worker resumable (checkpoint per page and re-enqueue to
continue across invocations).

Affected parcels:

```
bc578d88-57bf-441e-ac64-e2d973ca0c33  (530)   c56042f8-2cbb-4cc0-baae-4e6c4965bb20  (492)
3eaa426d-06e0-451b-bafb-b2773b66861f  (489)   fbd9b609-ced9-4d67-add7-c7214c2293c0  (486)
4f8ca76c-6280-41c1-bd3d-57c4b20c3004  (473)   a1d290b8-70e5-4939-ad33-540eafbb496c  (457)
e51818f0-1f23-4f85-b4dd-2104fb40aef6  (444)   5cb219ab-1611-4928-ad2a-2987122b3fcd  (443)
cbaa8727-1a66-40c3-8199-c1ce77906d74  (407)   48800284-5e5b-4691-97fb-8e220841efcf  (391)
daebd9bf-3ef5-4e69-b95e-14b8412ca7e0  (375)   e687f3bc-9aff-47af-b031-c431d7f6d779  (369)
a9cebb1e-ebad-4d09-ae7d-618c99d68bcb  (359)   e23601a2-c001-49c2-a5c5-9448f9e51ebe  (358)
87a0ad75-8050-4485-9070-3b074fa0f8b1  (352)   0ae2a51c-2737-4506-ab69-7966ea21f6bf  (352)
038c1689-8cb2-4768-a407-b067a3113736  (349)   f8c5d2e4-46f4-4bb1-9d22-582e0d43e767  (341)
82a76f63-4320-42e6-8429-ebcb53ee139c  (332)   b7f5a061-53e3-4bea-b9e9-e79ebd94743f  (332)
f2436990-6f78-4d3c-bcfb-31e64da2f6fb  (321)   9705bcad-8a55-43d6-b49c-7ebe0189ff22  (314)
```

## Future-compatibility seams (AC #12)

- `source_payload` retained on every logical row — full source evidence
  preserved for remapping as the lexicon expands, without re-scraping.
- Lexicon-aligned logical tables with deterministic keys (`source_system` +
  `source_record_key`) and idempotent upserts — safe re-runs and stable IDs for
  downstream indexing.
- Existing rails for the next milestone: `elephant-cli` (IPFS upload + on-chain
  submission), `elephant-mcp`, `contracts`, and `subgraph` already exist;
  Oracle #2 assembles these on top of this query DB.

## Exclusions (AC #13)

Out of scope for this milestone (deferred to Oracle #2 — _Open Data Index and MCP
Layer_):

- Public IPFS publishing
- Blockchain-style indexing
- MCP exposure
- NEO rewiring
- Elephant.xyz UI

## Acceptance criteria status

| AC                                 | Status                                                                                   |
| ---------------------------------- | ---------------------------------------------------------------------------------------- |
| #1–#6                              | Done                                                                                     |
| #7 batch ingestion / #8 scraping   | Done (Sunbiz bulk; property/permit/BBB scrape)                                           |
| #9 complete vs source availability | Done within curated scope (permits 1,211/1,233; 22 high-permit parcels documented)       |
| #10 store in local DB              | Done — all four sources in Neon; permits 1,211/1,233 (22 parcels documented above)       |
| #11 complete refresh, all four     | Done — all four refreshed; permits 1,211/1,233 (22 high-permit parcels documented above) |
| #12 future-compat seams            | Done (this document)                                                                     |
| #13 exclusions                     | Done (this document)                                                                     |
