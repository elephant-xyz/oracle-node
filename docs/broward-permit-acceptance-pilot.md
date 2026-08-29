# Broward property-first permit acceptance pilot

Date: 2026-08-29
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
- records one terminal result for every configured current/historical source;
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

Only these current routes are marked implemented in this branch:

- BMSD/unincorporated → Broward BCS/POSSE; and
- Lazy Lake → Broward BCS/POSSE.

The current Lauderdale-by-the-Sea route is Citizenserve/CAP Government and is
explicitly `adapter_unavailable`. A separate **historical** BCS route is enabled
only for that town because the prior official-source pilot proved BCS-held
records there. This distinction allows those historical records to be retained
without representing BCS as the current town custodian.

All other routes identify their vendor adapter boundary or an explicit
`captcha_required`, `login_required`, `custodian_only`, or
`egress_unavailable` result. Login/CAPTCHA routes are skipped; the pilot has no
credential or bypass code.

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

## Live pilot reconciliation

The 25-folio run represented 19 jurisdictions.

| Measure | Count |
| --- | ---: |
| Input / BCPA attempts / valid BCPA records | 25 / 25 / 25 |
| Jurisdictions resolved / unresolved | 25 / 0 |
| Current + historical source outcomes | 26 |
| Actual permit-source attempts / distinct parcels | 2 / 2 |
| Explicit source-unavailable outcomes | 24 |
| Official valid-parcel no-permits outcomes | 1 |
| Source failures | 0 |
| Raw / duplicate / conflicting permit records | 73 / 0 / 0 |
| Unique normalized records / Donphan query rows | 73 / 73 |

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

All are local validation artifacts and were not staged, loaded, published, or
added to the public county catalog.

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
2. Current anonymous adapters are not implemented/certified for 30
   jurisdiction routes. The pilot correctly records those gaps instead of
   sending their parcels to BCS.
3. Coral Springs requires reCAPTCHA; Hillsboro Beach, North Lauderdale, and
   Parkland require accounts. These need official bulk/custodian alternatives
   or explicit acceptance exclusions, not bypass code.
4. Sunrise remains an official records-request/egress-unavailable route, and
   Sea Ranch Lakes remains custodian-only.
5. BCS has no known positive contemporary BMSD commercial example in the
   validation evidence. The current unincorporated pilot parcel is a valid
   official zero result.
6. The 73 queryable rows are historical records for one parcel. Full permit
   acceptance still requires representative positive/zero validation and
   reconciliation for the implemented current municipal sources.

Accordingly, the local orchestration pilot passes, but Broward's shared
multi-category acceptance remains incomplete.
