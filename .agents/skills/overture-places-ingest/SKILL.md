---
name: overture-places-ingest
description: Ingest Overture Maps places (business/POI points with a controlled category taxonomy) for a county from the public Overture S3 release, clipped to the county boundary, into the `business_locations` tables in the elephant query DB and published as a per-county places table on IPFS. Use when a county needs business-category data, when refreshing a monthly Overture release, or when asked where business type/category data comes from.
metadata:
  author: elephant-xyz
---

# Overture Places Ingest

Global data source, county-scoped at extraction — same shape as `sunbiz-corporate-ingest`
(statewide source, county-scoped by ZIP) and `bbb-harvest` (national source, county-agnostic
crawl). Overture is the ONLY source in the pipeline that carries a business **category**:
Sunbiz has no industry field at all (Florida does not collect one at incorporation) and BBB
is a contractor directory by selection (2,103 of 2,810 FL profiles are construction trades,
0 food). Nothing else across the ~70 public tables supplies a category taxonomy.

Design decisions and their rationale — why a new `business_location` concept rather than
`companies` or the lexicon's `nearby_location`, why `expected_count` is NULL, and the
attribution obligation — are in `docs/overture-places-design.md`. **Read that first**; this
skill is the runbook, that doc is the reasoning.

> **STATUS: SPEC, NOT YET IMPLEMENTED.** No county has been ingested. Commands below name
> scripts that do not exist yet; treat them as the interface to build. Everything marked
> **VERIFIED** was read from Overture's published schema/docs or from this repo on
> 2026-08-12. Everything marked **UNVERIFIED** has not been executed and must be confirmed
> by the implementer.

## Source facts (verified 2026-08-12)

- Release `2026-07-22.0` at `s3://overturemaps-us-west-2/release/<release>/theme=places/type=place/*`.
  Public bucket, **no credentials**, no rate limit, monthly cadence. Azure mirror exists.
- Read with **DuckDB** (`LOAD spatial; LOAD httpfs; SET s3_region='us-west-2';`). DuckDB is
  not currently a dependency of any repo in this workspace — adding it is part of the work.
- Do **not** hard-code the release date in the scripts. Overture publishes a STAC catalog for
  release discovery; pin the resolved release into the run record so a re-run is reproducible.
- Observed volumes (**UNVERIFIED** beyond the original probe): Lee **40,190** places clipped to
  the county boundary in ~4 s; Orange **104,223** and Miami-Dade **154,075** by *bounding box*,
  which runs a few percent high. A two-county bbox query took 79 s. Never publish a bbox count.

### ⚠️ `categories` is deprecated and disappears in the September 2026 release

The original `categories.primary` property — the 2,117-entry vocabulary the scoping research
measured (1,244 distinct values in Lee) — **is deprecated and will be REMOVED in the September
2026 release**, replaced by `taxonomy` and `basic_category`. Given the date, an implementation
that keys on `categories.primary` breaks roughly one release after it ships.

Key on the new properties:

- `taxonomy.primary` — most specific category label (~2,300-entry OPC taxonomy).
- `taxonomy.hierarchy` — ordered L0→primary path, e.g.
  `[food_and_drink, restaurant, casual_eatery, gas_station_sushi]`. **This is the field to
  aggregate on**; do not re-derive a hierarchy from the label.
- `taxonomy.alternate` — other plausible labels. Store them, but they are **not reliable for
  analysis**: 80 Lee nail salons also carry `beauty_salon`. Any consumer counting by category
  must count `taxonomy.primary` only.
- `basic_category` — ~280-300 "cognitively basic" labels (`hotel`, `cafe`, `museum`). Use for
  coarse filtering and map iconography.

Keep `categories.primary` in a `legacy_category_primary` column while both exist, so the
scoping numbers stay reconcilable, and drop the column once September lands. The taxonomy
itself moves on a quarterly cycle (March/June/September/December): the July 2026 redesign
renamed 407 categories, removed 80 (with redirect rules), reparented 482 and **repathed
2,108**. Stamp every row with the release it was extracted from — category values are only
meaningful relative to a release.

## 1. Resolve the county boundary

Overture is global; this pipeline is per-county; `oracle_dataset_coverage` is keyed
`(county, source)`. Extraction MUST be clipped to a boundary, not a bounding box.

- Use **Census TIGER/Line county boundaries** (`tl_<year>_us_county`), selected by the county
  FIPS already recorded in `catalog/published-counties.json` (Lee `12071`, Orange `12095`,
  Miami-Dade `12086`, Palm Beach `12099`).
- TIGER boundaries are legal/statistical extents and do **not** exactly match an appraiser's
  parcel-roll extent. That is acceptable — a place is not a parcel — but record the TIGER
  vintage in the run record so a later count change can be attributed to a boundary change
  rather than to Overture.
- Two stages, both required: filter on the parquet `bbox` struct first (cheap partition
  pruning), then `ST_Within` against the boundary polygon. The bbox stage is an optimisation,
  never the answer.

**A place outside every Elephant county is not ingested.** There is no "unassigned" bucket —
Overture is global and that bucket would be unbounded. **Tie-break rule: assign by geometry.**
Where a place's point falls in county A but its Overture `addresses[0]` names county B, county
A owns it and the discrepancy is recorded in `source_payload`. Without a stated rule two
counties will both claim border places and the coverage counts will not reconcile.

## 2. Extract

```bash
node scripts/extract-overture-places.mjs \
  --county lee --county-fips 12071 \
  --release 2026-07-22.0 \
  --boundary-source tiger/tl_2024_us_county \
  --output-s3-uri s3://<env-bucket>/overture-places/<county>/<release>/
```

Reference query in `scripts/extract-county-places.sql`. Output is chunked JSONL plus a
`manifest/summary.json` carrying the counters that become the run record (§4) — the same
`profiles/` + `manifest/summary.json` layout `bbb-harvest` produces.

**Keep every place. Do not filter by `confidence` at extraction.** Overture already drops
places scoring ≤ 0.2, and confidence is explicitly *not calibrated across providers* — a
second threshold here would silently vary by which provider happens to dominate a county.
Store the score and let consumers threshold. Same reasoning as the pipeline ground rule that
raw HTML is always captured and unmapped fields are preserved.

### ⚠️ Assert the source datasets, every run

Places carries **no OpenStreetMap data** and therefore none of the ODbL share-alike
obligations — that is the whole reason this source is publishable. That property is not
guaranteed forever, and Overture's bridge-file documentation already lists OSM as a bridged
source for the places theme even though the attribution page does not. So make it a gate, not
an assumption:

```sql
SELECT DISTINCT s.dataset FROM business_location_sources s WHERE ...
```

must be a subset of `meta, microsoft, foursquare, pinmeto, krick, rendersEO, dac, brightquery,
alltheplaces` (**VERIFIED** against the Overture attribution page 2026-08-12). **If `osm` — or
any unknown dataset — appears, stop and do not publish.** A new provider may carry a different
licence and the published NOTICE would be wrong.

## 3. Load into Neon

New Drizzle schema module `elephant-query-db/src/schema/places.ts`, loaded via the existing
bulk loader with a new `--tracks places` track. All house rules from
`query-db-loading-matching` apply unchanged: idempotent `ON CONFLICT DO UPDATE` merges,
`sourceMetadataColumns()`, full payload retained in `source_payload`.

| Table | Grain | Notes |
|---|---|---|
| `business_locations` | one row per GERS id per county | the place itself |
| `business_location_categories` | one row per category | `is_primary` flag; holds `taxonomy.alternate` |
| `business_location_sources` | one row per `sources[]` entry | `dataset`, `record_id`, `update_time`, `confidence`, `license` — this is what makes §2's licence gate and re-identification possible |
| `overture_place_extractions` | one row per (county, release) run | the honest denominators; see §4 |
| `business_location_parcel_links` | one row per accepted match | **later step, not ingest**; see §5 |

Keys and indexes on `business_locations`:

- `source_system = 'overture_places'`, `source_record_key = <GERS id>`, with the mandatory
  `uniqueIndex (source_system, source_record_key)` every other table in the schema carries.
  This is the merge key — there is no equivalent of the folio here, so do not invent one.
- `geometry geometry(Point, 4326)` with a **GiST** index. PostGIS `3.5.0` is installed
  (**VERIFIED**). Note `parcel_boundaries` stores Florida West feet (EPSG:2237), so any
  point-in-polygon against it needs an explicit `ST_Transform` — see `src/schema/spatial.ts`.
- btree on `(county_key, taxonomy_primary)`, `basic_category`, `normalized_name`, `address_id`,
  and `(county_key, is_current)`.
- `taxonomy_hierarchy text[]` with a **GIN** index so `WHERE 'restaurant' = ANY(...)` style
  roll-ups do not scan.

**`address_id` is a soft link only** (`ON DELETE SET NULL`, matching `business_reputation_*`),
resolved by `normalized_address_hash` against the shared `addresses` table. **Never write
`company_id` at ingest** — see §5.

> ⚠️ `addresses`, `companies`, `people` are **shared parents** across every track. The clean-
> reload and deadlock warnings in `query-db-loading-matching` apply: never `TRUNCATE … CASCADE`
> them, clear by `source_system` in reverse FK order, and keep the places load serial with the
> other tracks that write shared parents.

## 4. Coverage — `expected_count` is NULL, deliberately

Upsert `oracle_dataset_coverage` with `source = 'overture_places'`:

- `ingested_count` — `business_locations` rows for the county at the current release.
- `expected_count` — **NULL**. There is no authoritative denominator for "how many businesses
  exist in Lee County". Overture's own count is the numerator, so using it forces a permanent
  100%; Sunbiz counts legal entities, not locations; DBPR licence counts cover one sector.
  NULL is the repo's sanctioned value for exactly this: `write-public-coverage-snapshot.ts`
  emits `expected_count: null` for every non-appraisal track under the comment "unknown
  expected counts, which means 'not ingested' rather than 'complete at zero'", and
  `validate-publication-dry-run.ts` fails with `Coverage track … is not honestly incomplete`
  if a non-appraisal track has a non-null one. MCP then reports `completionPercent: null`,
  which is the truthful answer.

> ⚠️ Eight legacy rows in Neon (Lee/Miami-Dade/Orange/Palm Beach) have
> `expected_count = ingested_count` from the original backfill, so they read as 100% complete
> by construction. Santa Clara, the most recent county, correctly has NULL. **Copy Santa
> Clara, not the older rows.**

The real numbers go in `overture_place_extractions`, one row per (county, release):
release id, county FIPS, TIGER boundary source and vintage, features in bbox, features after
clip, distinct `taxonomy.primary` values, distinct `sources[].dataset` values, count by
`operating_status`, confidence distribution, and duration.

**Sector cross-check, recorded not enforced.** Florida DBPR's cosmetology bulk file lists
**1,420** licensed salon establishments in Lee against Overture's **1,436** combined
beauty/hair/nail/barber places — close agreement, and good evidence of coverage in that sector.
Record it in the run report as a quality assertion. It is **not** an `expected_count`: it
covers one sector and one state, and promoting it to a denominator would imply a completeness
guarantee the number cannot support.

## 5. Do not conflate a place with a company

One company operates several locations; one location hosts several companies over time.
Collapsing them is the single most expensive mistake available here.

- **Never** load places into `companies`. That table is the shared *party* role used by
  appraisal owners, Sunbiz registrants, BBB profiles and permit contractors (~5.5M rows); 40k
  Lee places would pollute a parent every other track FKs into and make the company count
  meaningless.
- `business_locations.company_id` stays NULL at ingest. Populate it only from an explicit,
  later name+address match step, writing a confidence and a method the way
  `contractor_quality_scores` does (`match_confidence`, `match_method`), and only at high
  confidence — the `query-db-loading-matching` rule that low-confidence candidates are left
  unlinked for review applies unchanged.
- Same for parcels: `business_location_parcel_links` is a separate, confidence-scored bridge
  computed after ingest by point-in-polygon against `parcel_boundaries`. Boundaries currently
  exist for Lee only, so for every other county this step is a no-op until its boundaries land
  — that is expected, not a failure.

## 6. Refresh — upsert by GERS id, never delete

Overture releases monthly. A refresh **diffs**, it does not replace.

- Read the changelog at
  `s3://overturemaps-us-west-2/changelog/<release>/theme=places/type=place/change_type=*/`
  (partitioned by `theme`/`type`/`change_type`, unique index on `id`), filtered to the county
  bbox, and touch only `added` / `removed` / `data_changed` ids. The first load for a county is
  a full extraction; every later one is a diff.
- Maintain `first_seen_release` and `last_seen_release`. A place absent from the current
  release keeps its row with a stale `last_seen_release` and `is_current = false`.
  **Absence is not closure.** Overture's `removed` change_type means only "not present in this
  release".
- **Closure is a field, not an inference.** Overture models it directly: `operating_status` is
  one of `open` / temporary hiatus / `permanently_closed`, and `confidence` of exactly 0 means
  Overture is certain the place no longer exists and is always paired with
  `permanently_closed`. Read those. Do not derive closure from disappearance, and do not delete
  rows for closed places — the historical record is the point.

### GERS id stability — verified, with a caveat that matters

Ids **are** GERS UUIDs and Overture's stated design goal is stability across releases,
supported by real machinery: a GERS Registry (`first_seen`, `last_seen`, `last_changed`,
`version`), a per-release changelog, and bridge files mapping GERS UUIDs to provider record
ids. This was **VERIFIED** from Overture's GERS and places documentation, not assumed.

But do not treat it as a guarantee:

- Overture's own tutorial records that only **1,546** of the new places in the May 2025 release
  reappeared in June with their new UUIDs, described as "a known quality issue with our source
  datasets".
- The places theme is documented as containing duplicates, a high junk rate and low property
  completeness.
- **`2026-07-22.0` — the release the scoping research used — re-matched the entire corpus with
  a new matching pipeline, and Overture warns of "a one-time elevated level of GERS ID churn in
  that release".** A baseline taken from `2026-07-22.0` and diffed against `2026-08` will
  therefore show unusually high churn. That is expected; do not chase it as a bug.

So: GERS is the merge key, and `business_location_sources` (`dataset` + `record_id`) is the
fallback that lets a re-identified place be re-linked through the bridge files. Carry both.

## 7. Publish

Follow `county-open-data-publish`, with places as its own artifact family:

- Export `<out>/<county>/places-table.parquet` with `@dsnp/parquetjs` (the writer the query
  table already uses). The schema is **flat and scalar-only** — that is a constraint of the
  existing export convention, so `taxonomy.hierarchy` serialises as a `/`-delimited string with
  the array kept in Neon for querying.
- **Its OWN Filebase bucket and its OWN IPNS label** (`oracle-open-data-<county>-places`). The
  publish skill's fixed-key clobber warning is the reason: the uploader writes fixed keys, so
  sharing a bucket with the property or query-table artifacts risks unpinning them.
- Gate before upload, mirroring `validate:query-table`: parquet row count == `business_locations`
  rows for that county at that release, zero duplicate GERS ids, zero null geometries, and the
  §2 licence assertion passing.
- Register `placesTableUrl` in `catalog/published-counties.json` (see "Repo changes required").

### Attribution is a deliverable, not a footnote

Places is published under **CDLA-Permissive-2.0** and **Apache-2.0** per record, with no OSM
lineage. The Foursquare-derived subset (4,748,001 records) is Apache-2.0 and requires
preserving `Copyright 2024 Foursquare Labs, Inc. All rights reserved.` together with a
statement that the files were changed and the date of change. Because this publishes to public
IPFS, that obligation ships with the artifact. Put the notice in three places:

1. **`NOTICE.txt` at the root of the published places bucket**, so it is inside the DAG the
   IPNS name resolves to and travels with any copy of the data. It carries the Overture
   citation with the accessed date, the full per-provider licence list for the places theme,
   the Foursquare copyright line, and **Elephant's own** change statement and date — not
   Overture's `Changed: 2026-03-18`, which describes Overture's transformation, not ours.
2. An `attribution` block inside the places table's sibling `index.json`, so machine consumers
   get it without a second fetch.
3. `docs/overture-places-sources.yaml`, recording licence, attribution text, refresh cadence
   and publication decision — the same `license:` / `publication: open-with-attribution` /
   `bulk_terms.decision` shape `docs/rock-island-sources.yaml` established.

> ⚠️ These terms were read from Overture's published documentation on 2026-08-12 and have
> **not been reviewed by counsel**. Also note Overture's own warning that joining
> CDLA-Permissive data to OpenStreetMap can make the resulting derivative database ODbL —
> relevant the moment a published Elephant artifact combines places with an ODbL source.

## Hosted services — flag at ingest, never exclude

Roughly 250 of Lee's 1,244 categories are not businesses occupying a location but services
hosted inside other businesses: `atms`, `rental_kiosks` (Redbox), `propane_supplier`,
`money_transfer_services`, `trusts`. Left unhandled, every spatial co-location analysis reads a
Redbox as a business sharing a building.

The decision is to **classify in ingestion and exclude in neither place**: `business_locations`
gets a nullable `is_hosted_service boolean` plus `hosted_service_rule text` naming the rule that
set it, driven by a committed, reviewable list at `config/hosted-service-categories.txt`
keyed on taxonomy path and versioned with the taxonomy release.

Excluding at ingest would violate the pipeline's ground rule that data is preserved and gaps
logged, never dropped — and the judgement is contestable, so it must not be baked into what
gets stored. Leaving it entirely to consumers guarantees every consumer re-derives it
differently and gets it wrong by default. A derived, named, overridable flag is the compromise:
it is advisory, consumers may ignore it, and the rule that produced it is auditable in git.

**The list itself is not written.** The 250 categories were counted against the deprecated
`categories` vocabulary, so the list must be rebuilt against `taxonomy.hierarchy` before it
means anything.

## Repo changes required (none are optional)

- `elephant-query-db/src/schema/places.ts` + migration; register in `src/schema/index.ts`.
- `scripts/oracle-dataset-coverage-upsert.ts` — add `overture_places` to `CoverageSource` and
  `COVERAGE_SOURCES`, as a **global** source (county attributed from the artifact URI, like
  sunbiz/bbb) rather than a county-keyed one.
- `scripts/write-public-coverage-snapshot.ts` — the track list `["permits", "corporate", "bbb"]`
  is hardcoded; add the places track.
- `scripts/validate-publication-dry-run.ts` — the same list is hardcoded again in the
  "honestly incomplete" loop.
- `catalog/published-counties.json` + `catalog/README.md` + the catalog updater — add a nullable
  `placesTableUrl`, matching how `permitQueryTableUrl` is already handled.
- `elephant-query-db/docs/open-lexicon-gaps.md` — an Overture section, per §"Lexicon".

> ⚠️ **Pre-existing inconsistency, do not propagate it.** The Neon `oracle_dataset_coverage`
> table uses `source = 'sunbiz'` while the published snapshot writer emits `corporate` for the
> same track. Use ONE spelling for places — `overture_places` — in both, and note the sunbiz
> mismatch when you touch the file.

## Lexicon

Places do not map onto an existing lexicon class. `company` is a party (three properties:
name, request identifier, source request) and is the conflation error in §5.
**`nearby_location` looks like the answer and is not** — it is property-relative
(`distance_miles`, `is_walkable`), its `location_type` is a closed 13-value lifestyle enum
(Shopping, Dining, Nature…), and it has no identity, geometry or operating status of its own.
It is a marketing projection *derived from* places, not a home for them.

Follow the Sunbiz precedent: land the tables in Neon now, record the gap in
`elephant-query-db/docs/open-lexicon-gaps.md`, and raise the new class in the `lexicon` repo as
a separate tracked PR.

> ⚠️ **The Sunbiz precedent also shows how this step gets dropped.**
> `docs/sunbiz-lexicon-transform-findings.md` states that `business_registration`,
> `business_registration_address` and `business_registration_party` were "Added" to `../lexicon`
> — they are **not present in `lexicon` `main`** (`src/data/lexicon.json`, checked 2026-08-12).
> The lexicon change for the closest analogue source never landed. Track the PR and record its
> URL, or this one will not land either.

## Known gaps (do not silently fix)

- The hosted-service category list does not exist yet and must be rebuilt against the new
  taxonomy (see above).
- `business_location_parcel_links` is unimplemented and is a no-op for every county except Lee
  until parcel boundaries exist for them.
- Brand (`brand.names.primary`, `brand.wikidata`) is stored but not normalised to `companies`;
  it is the obvious future join to chains and is deliberately out of scope here.
- Overture's provider category mappings (captured in the pipeline from the July 2026 release)
  are not ingested — they would let a consumer trace a provider's original category, e.g.
  BrightQuery's NAICS-based mappings, back through the Overture taxonomy.
- Places outside every ingested county are dropped with no record that they were seen.

## Open questions

- Which counties get places, and in what order? Coverage exists for Lee, Orange, Miami-Dade,
  Palm Beach and Santa Clara, but only Lee has a boundary-clipped count.
- Where does the extraction run? A full-county DuckDB read is minutes, not hours, so a laptop
  is defensible for the first county — but the load and publish inherit the
  `EADDRNOTAVAIL`/cross-Atlantic warnings in `county-open-data-publish` and should run
  in-region on the `open-data-publish` Fargate cluster.
- Is the places table PII-bearing for the purposes of the human-run publish gate? Overture
  states it excludes places containing PII, but the records carry `emails` and `phones` which
  for a sole trader may be personal. **Assume the human gate applies until someone decides
  otherwise.**
- Does anything downstream need `basic_category` mapped onto Elephant's own vocabulary, or is
  passing Overture's labels through sufficient?

## Persist your work

Extraction, load and export scripts are committed in `oracle-node` / `elephant-query-db` on the
working branch. Category lists, run notes and the sources/licence record also get committed and
PR'd to `github.com/elephant-xyz/Counties-trasform-scripts` under an `overture/` source folder
(`gh pr create`), the same way Sunbiz and BBB artifacts are persisted. Never commit extracted
place data; reference its S3 location.

**These skills are installed from `elephant-xyz/skills`, not authored here** — every directory
under `.agents/skills/` is untracked in `oracle-node` and pinned by `skills-lock.json`. Once
this spec is agreed, upstream it to `elephant-xyz/skills` under `skills/overture-places-ingest/`
and add its lock entry, or the next `skills` sync will not know it exists.
