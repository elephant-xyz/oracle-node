# Broward property-first permit acceptance pilot

Date: 2026-09-01
County: Broward County, Florida (`12011`)
Execution: local, bounded, anonymous, checkpointed
Pilot execution result: **PASS**
Full county permit acceptance: **NOT PASSED**

## What this validates

This pilot closes the orchestration gap between Broward appraisal evidence and
permit processing without claiming that one county portal covers every city.
It:

- reads an existing appraisal sample CSV/manifest (up to 50 exact alphanumeric
  folios);
- obtains BCPA situs city/address evidence for each parcel;
- resolves that evidence through an exact 32-jurisdiction registry;
- records one terminal result for every configured current, historical, or
  supplemental source;
- only requests sources whose adapter is explicitly marked implemented;
- checkpoints after each BCPA lookup and each permit source result;
- normalizes permit records into query-db's actual 20-column permit-table
  shape;
- writes local private JSONL, Parquet, coverage, checkpoint, and reconciliation
  artifacts; and
- validates the Parquet through Donphan's actual permit-query handlers.

`scripts/broward-permit-jurisdictions.mjs` is the executable registry.
`scripts/run-broward-permit-pilot.mjs` is the local orchestrator.

## Jurisdiction and custody rules

The registry contains exactly **32** rows: BMSD/unincorporated plus all 31
municipalities. BCPA `situsCity` is matched to an exact registered alias.
Address fallback accepts a city only at the end of the situs address before an
optional Florida/ZIP suffix. Unknown evidence remains unresolved; it never
defaults to BCS.

The integrated registry points 16 current routes to bounded local adapter
implementations:

- BMSD/unincorporated and Lazy Lake → BCS/POSSE;
- Cooper City, Fort Lauderdale, Hollywood, Plantation, and Weston → Accela;
- Hallandale Beach, Miramar, Oakland Park, Pembroke Pines, and Sunrise → Tyler
  Civic Access; and
- Lauderdale-by-the-Sea, Southwest Ranches, West Park, and Wilton Manors →
  Citizenserve.

A separate **historical** BCS route remains enabled only for
Lauderdale-by-the-Sea because the prior source pilot proved BCS-held town
records. A separate Sea Ranch Lakes BCS route is labeled **supplemental** for
county-held or associated approvals and is never complete village evidence.
Neither route represents BCS as the current municipal custodian. The CLI
orchestrator's default runner set remains BCS-only; vendor-family runners can be
supplied explicitly, and a missing runner becomes `adapter_unavailable` rather
than silently falling back to BCS.

The other 16 current routes retain exact `adapter_unavailable`,
`captcha_required`, `login_required`, `no_anonymous_search`, or
`custodian_only` dispositions.
Login/CAPTCHA/custodian routes are skipped; the pilot has no credential,
bypass, or records-request submission code.

## Bounds and local checkpoint

Hard process limits:

- at most **50** unique 12-character alphanumeric BCPA folios;
- at most **5** implemented permit-source attempts;
- sequential BCPA lookups with at least **250 ms** spacing and a **30 s**
  timeout;
- sequential permit lookups with at least **1,000 ms** spacing;
- BCS's existing limit of 125 list rows and 75 detail requests per parcel;
- BCS detail requests remain sequential, delayed at least 250 ms, timeout
  bounded, and limited to 2 MB responses; and
- no retries, AWS clients, queues, databases, secrets, login, CAPTCHA solving,
  IPFS, catalog updates, or publication.

The checkpoint signature includes the ordered folio set, registry version,
schema version, and permit-attempt cap. A mismatched run fails instead of
reusing unsafe state. Re-running the same command skipped all 25 BCPA and two
BCS requests and regenerated the derived artifacts from the checkpoint.

Use the complete validated sample when its ignored local artifact is present:

```bash
node scripts/run-broward-permit-pilot.mjs \
  --sample downloads/broward/broward-validation-sample-50.csv \
  --output-dir downloads/broward/permit-acceptance-pilot
```

This fresh isolated worktree did not contain the gitignored 50-row CSV/manifest.
The live run therefore used `--pilot`, the checked-in original 25-folio subset
that the 50-parcel appraisal sample explicitly preserves:

```bash
node scripts/run-broward-permit-pilot.mjs \
  --pilot \
  --output-dir downloads/broward/permit-acceptance-pilot \
  --max-adapter-attempts 5 \
  --appraisal-delay-ms 300 \
  --permit-delay-ms 1000
```

## Live pilot reconciliation snapshot

The following counts are the bounded pre-integration BCS run. They are retained
as evidence, not recalculated claims about the expanded adapter registry.

The 25-folio run represented 19 jurisdictions.

| Measure                                          |        Count |
| ------------------------------------------------ | -----------: |
| Input / BCPA attempts / valid BCPA records       | 25 / 25 / 25 |
| Jurisdictions resolved / unresolved              |       25 / 0 |
| Current + historical source outcomes             |           26 |
| Actual permit-source attempts / distinct parcels |        2 / 2 |
| Explicit source-unavailable outcomes             |           24 |
| Official valid-parcel no-permits outcomes        |            1 |
| Source failures                                  |            0 |
| Raw / duplicate / conflicting permit records     |   73 / 0 / 0 |
| Unique normalized records / Donphan query rows   |      73 / 73 |

Unavailable source outcomes were:

- 20 `adapter_unavailable`;
- 2 `login_required`;
- 1 `captcha_required`; and
- 1 `egress_unavailable`.

The two implemented-source attempts were:

1. BCS current custody for unincorporated folio `474134000012`: resolved BCS
   parcel object `791` and returned the official no-permits marker.
2. BCS historical custody for Lauderdale-by-the-Sea folio `494318013550`:
   listed 107 rows, excluded 34 plan reviews, and normalized 73 details with no
   failures.

The 73 records comprise **19 master applications** and **54 permit records**.
The only positive permit parcel is historical Lauderdale-by-the-Sea evidence;
it is not evidence of current Citizenserve completeness or countywide BCS
coverage.

Local artifacts under `downloads/broward/permit-acceptance-pilot/`:

- `checkpoint.json`;
- `normalized-permits.private.jsonl`;
- `permit-query-rows.private.jsonl`;
- `permit-table.parquet`;
- `permit-coverage.json`;
- `reconciliation.json`; and
- `donphan-evidence.json`.

All are private local validation artifacts. The reconciled records were later
loaded to the isolated Broward Neon branch as described below; they were not
published or added to the public county catalog.

## Bounded Neon load and expanded source evidence

On 2026-08-31, the identity-gated loader
`scripts/load-broward-permit-pilot-to-neon.mjs` committed the reconciled pilot
evidence to the isolated `broward-ingest` branch. A second identical load
confirmed source-key idempotence:

| Measure                             | Count |
| ----------------------------------- | ----: |
| Loaded property improvements        |   104 |
| Loaded public inspection records    |    97 |
| Distinct exact-folio parent parcels |     5 |
| Distinct permit source systems      |     6 |
| Duplicate source keys               |     0 |
| Unlinked permit / inspection rows   | 0 / 0 |

Loaded permit rows by bounded source:

- Broward BCS/POSSE: 73;
- Hollywood Accela: 1;
- Plantation Accela: 7;
- Weston Accela: 6;
- Pembroke Pines Tyler Civic Access: 7; and
- Lauderdale-by-the-Sea Citizenserve: 10.

The current-source probes also produced explicit valid zero-record results for
Cooper City Accela, Miramar Tyler, and Southwest Ranches Citizenserve.

These counts are evidence, not countywide completeness. Lauderdale-by-the-Sea
reported 33 Citizenserve permits but the bounded detail pilot captured 10, so
that source remains truncated. Fort Lauderdale exposed 50 Accela details,
above the 20-detail pilot ceiling, and was failed closed without loading a
partial result. Sixteen current jurisdiction routes remain
blocked/unavailable. A countywide or supported-routes-only crawl requires an
explicit run scope and source-pressure policy; it must not be inferred from
this pilot.

## Supported-routes full run

After the then-current 15-route durable pilot reached terminal state, the
supported-only property-first run started on 2026-08-31, before Sunrise was
certified:

- job ID: `broward-permits-supported-full-20260831`;
- candidate properties: 276,502;
- scope: only the 15 registry routes marked implemented;
- concurrency: four total workers, serialized per jurisdiction;
- source attempts: anonymous, page/detail bounded, and rate delayed;
- durable state:
  `ingest_control.broward_supported_permit_runs` and
  `ingest_control.broward_supported_permit_items`;
- checkpoint identity: one-way parcel hashes plus immutable registry/config
  signature;
- local private cache:
  `downloads/broward/supported-permit-full`;
- publication: disabled.

The later `2026-09-01.1` registry adds Sunrise and the Sea Ranch Lakes
supplemental route. Its changed registry/config signature intentionally cannot
resume or reinterpret the immutable pre-Sunrise job above; a Sunrise-inclusive
run requires a new job ID and pilot gate.

The 17 blocked/login/CAPTCHA/custodian routes are excluded from source calls.
Records, explicit no-permit results, bounded truncations, retryable failures,
and failures exhausted after three attempts remain separate terminal states.
Source exhaustion for this job means only that every supported-route candidate
has one of those explicit outcomes; it does not mean countywide permit
completeness.

## Query-db and Donphan evidence

The mapper follows `elephant-query-db`
`scripts/run-permit-table-export.ts` at
`15187e2d8709115635620ea7113e915b1e9a0651`: one row per source object, required
`property_improvement_id`, and 19 nullable scalar columns. Local IDs are
deterministic UUIDs derived from the complete BCS source record key. Exact BCPA
folio links each row to `broward:<folio>`. Completion/close dates remain null
because BCS does not expose those meanings independently; the latest explicit
completed inspection is retained as `final_inspection_date`.

The actual `elephant-mcp` permit handlers at
`0d61c83f9e166c9da5c5945641a2e322949dd8c1` were invoked with the local Parquet:

- `getPermitQuerySchema({ county: "Broward" })` returned the expected 20
  columns;
- `getPermitCoverage({ county: "Broward" })` returned 73
  `broward_county_bcs_posse_permits` rows;
- `queryPermits` returned 73 rows, 73 distinct permit IDs, one distinct parcel,
  and 73 non-null permit numbers;
- grouping through `queryPermits` returned 19 `master_application` and 54
  `permit_record` rows; and
- exact parcel query for `494318013550` returned real permit number, type,
  status, issue date, and source-system values.

Command:

```bash
cd /path/to/elephant-mcp
npm exec tsx -- \
  /path/to/oracle-node/scripts/validate-broward-permits-with-donphan.mjs \
  --parquet /path/to/oracle-node/downloads/broward/permit-acceptance-pilot/permit-table.parquet \
  --module /path/to/elephant-mcp/src/tools/permitQuery.ts \
  --output /path/to/oracle-node/downloads/broward/permit-acceptance-pilot/donphan-evidence.json
```

## Remaining acceptance blockers

1. Re-run the same checkpointed flow against the actual ignored
   `broward-validation-sample-50.csv` (or manifest). The current live evidence
   covers its preserved 25-folio subset, not all 50 rows.
2. Sixteen current routes remain transport-incomplete or access/custodian
   blocked. The pilot records those gaps instead of sending their parcels to
   BCS.
3. Coral Springs and Hillsboro Beach require reCAPTCHA; North Lauderdale and
   Parkland require accounts. These need official bulk/custodian alternatives
   or explicit acceptance exclusions, not bypass code.
4. Sunrise has a bounded anonymous EnerGov route, but portal records do not
   prove complete City history. Sea Ranch Lakes remains custodian-only; BCS is
   supplemental county-held/associated-approval evidence only.
5. BCS has no known positive contemporary BMSD commercial example in the
   validation evidence. The current unincorporated pilot parcel is a valid
   official zero result.
6. The 73 queryable rows are historical Lauderdale-by-the-Sea BCS records for
   one parcel. Accela's 21 details and the other bounded municipal probes are
   separate source-certification evidence, not a unified current-county result.
   Full permit acceptance still requires representative positive/zero
   validation and reconciliation for the implemented current municipal
   sources.

Accordingly, the local orchestration pilot passes, but Broward's shared
multi-category acceptance remains incomplete.
