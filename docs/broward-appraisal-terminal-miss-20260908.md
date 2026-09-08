# Broward appraisal terminal-miss recovery (2026-09-08)

Scope: classify the 8,241 durable appraisal terminal items, recover only
source-backed folios into isolated Neon, and stage a new Donphan snapshot only
if the property watermark advanced. This note does **not** claim county
completeness.

Live BCPA sale-only / sale-backfill, Hollywood Accela, and BCS Citizenserve
supervisors were left running. This pass did not take their locks, reset their
jobs, or start a second full BCPA capture of the loaded 526,068 folios.

## Isolated Neon watermark (read-only, identity-gated)

Verified against project `raspy-frost-51580436` and the injected
`broward-ingest` branch/endpoint IDs (values not logged).

| Measure | Count |
| --- | ---: |
| `broward_appraiser` properties | 526,068 |
| Distinct folios | 526,068 |
| Null folios / orphan properties | 0 / 0 |
| Last appraisal load | 2026-08-30T16:39:49Z |
| Completed seed-key hashes | 526,068 |
| Terminal items | 8,241 |
| Permit rows (`broward%permits` + Coconut Creek status) | 1,276,332 |
| Last permit load | 2026-09-08T17:03:37Z |

`526,068 + 8,241 = 534,309` (official GIS denominator used by the August
recovery). Ingest status is `phase=complete`, `source_failure_count=0`.

## Classified 8,241 breakdown

Durable table `ingest_control.broward_appraisal_terminal_items` stores only
one-way seed-key hashes and `outcome='source_miss'`. There is no per-folio
reason or municipality column.

| Class | Count | Evidence |
| --- | ---: | --- |
| Recorded `source_miss` (empty `parcelInfok__BackingField`) | 8,241 | 100% of terminal rows; 3,435 aggregate event batches; chunk `source_miss_count` 8,240 full + 1 pilot |
| Recorded `source_error` residual | 0 | Ingest status and chunk sums |
| Recorded `transform_error` residual | 0 | 6 transform events occurred in August and were later absorbed into the 526,068 completed set |
| Recorded `load_error` residual | 0 | 2 load events; not left pending |
| Hash collision (completed ∩ terminal) | 0 | |

### Official-seed join (current BCPA GIS `BCPA_EXTERNAL_JAN26` / layer 16)

The August recovery seed is not on this VM. `s3://counties-seeds/broward.csv`
is a different 753,242-row extract (2026-03-02) and is **not** the 534,309 GIS
seed. Current GIS was rebuilt folio-only in OBJECTID order:

| Measure | Count |
| --- | ---: |
| Current GIS object IDs | 556,236 |
| Current GIS distinct valid folios | 534,369 |
| August recovery denominator | 534,309 |
| Terminal hashes still in current GIS | 8,236 |
| Terminal hashes absent from current GIS | 5 |
| Completed hashes still in current GIS | 526,020 |
| Current GIS folios in neither set (layer churn since August) | 113 |

Those 113 new GIS folios were **not** chased (existing August seed only; do
not re-derive work from the property table).

Current GIS signature `5b134e0c…` does not match the August chunk signature
`e1b46f86…` — expected layer drift, not a load defect.

### Failure class / reason

| Reason | Count | Notes |
| --- | ---: | --- |
| Official BCPA empty result (`parcelInfok__BackingField` null/empty) | 8,241 | Original August 29–30 ingest classified every miss this way; 0 source HTTP/timeout residuals remain |
| GIS folio gone from current layer | 5 | Subset of the 8,241; deleted/renumbered since August |
| Present in March `counties-seeds/broward.csv` (NAL-scale) | 3 | The other 8,238 terminal hashes are absent from that 753,242-row file |

### Folio shape (8,236 still in GIS)

Letter folios are 9.2% of current GIS (49,401 / 534,369) but **63.7%** of the
joinable terminal set (5,249 / 8,236). Digit-only misses are 2,987.

GIS `PARCEL_TYPE` on those 8,236 (layer has no municipality field):

| PARCEL_TYPE | Count |
| --- | ---: |
| empty | 6,147 |
| `0` | 1,840 |
| `1` | 248 |
| `19` | 1 |

### Jurisdiction

Official empty BCPA envelopes have no situs/city. GIS exposes `FOLIO` +
`PARCEL_TYPE` + geometry only. Municipal jurisdiction is therefore
**unassignable** from the official empty result. Township-book prefix of the
12-character folio (not a city name):

| Book prefix | Terminal still in GIS |
| --- | ---: |
| 47 | 44 |
| 48 | 2,078 |
| 49 | 2,520 |
| 50 | 1,879 |
| 51 | 1,715 |

## DEAD confirmation sample (required re-scrape)

A distinct 40-folio confirmation sample was POSTed to
`web.bcpa.net/.../getParcelInformation` at concurrency 1 with 750 ms spacing so
it would not take the sale-backfill worker slots. The sample is hash-stratified
across all five book prefixes and letter/digit classes, and it forced in the
three NAL-overlapping folios.

| Outcome | Count |
| --- | ---: |
| `has_records` (recoverable) | **0** |
| empty official backing field | **40** |
| HTTP error / timeout / malformed | 0 |
| NAL-overlap recovered | 0 / 3 |
| Dead-signature rate | 100% |

Per `county-ingest-run`: 0/40 recovered and ≥97% of fresh errors carry the
dead signature ⇒ **confirmed dead, stop chasing**. Proxies would not help —
the official page/API loads and returns no parcel records.

Prior August ingest already recovered ~0 of this tail: every one of the 8,241
was committed as `source_miss` during the full run, not as a later-resolved
timeout.

## Recovery / load / snapshot decision

- Recovered / loaded folio count: **0**
- New Neon property total: **526,068** (unchanged; no TRUNCATE, no invented rows)
- Remaining confirmed dead: **8,241**
- Achievable appraisal count for this seed vintage: `534,309 − 8,241 = 526,068`
- New `VERSION` / S3 prefix: **none**. Latest completed handoff remains
  `20260907T220731220Z` (526,068 properties / 1,276,328 permits). Current Neon
  permits are 1,276,332 (+4 Hollywood after that snapshot). A new full
  replacement snapshot was **not** minted because the property watermark did
  not advance; publishing a same-property snapshot would be misleading for
  this miss-recovery task.

Do not treat 526,068 as county-complete. The GIS layer has already drifted
(+60 distinct folios vs the August seed), eight municipal permit routes remain
unattended/blocked, and sales/NAL/CID streams were left on their own jobs.
