# Overture Places Design Decisions

Date recorded: 2026-08-12

Design note behind the `overture-places-ingest` stage skill. Four decisions needed making
before anyone writes code: how places are modelled, what `expected_count` means for a source
with no denominator, whether hosted-service categories are classified in ingestion, and where
the attribution notice lives. Runbook detail is in the skill; this records the reasoning and
the alternatives that were rejected.

## Why Overture at all

Watchog has a business co-location fact class that has never fired, because Elephant holds no
business category data. That was established as a domain gap, not a coverage gap:

- **Sunbiz** (`business_registrations`, 4,532,582 companies) has names, addresses, officers and
  filing histories, and **no industry field at all** — verified in the `source_payload` JSON as
  well as in the columns. Florida does not collect industry at incorporation.
- **BBB** (`business_reputation_profiles`, 2,810 profiles) does carry categories but is a
  contractor directory by selection: statewide 2,103 construction trades, 90 retail, 80
  professional, 39 health, 8 beauty, 17 marine, 0 food. Lee (2,594) and Miami-Dade (216) only;
  Orange zero.
- Nothing across the ~70 public tables supplies a business category taxonomy.

Overture places carries exactly one primary category per record from a controlled hierarchical
vocabulary, is CDLA-Permissive/Apache/CC0 with no OpenStreetMap lineage, and is free to read
from a public S3 bucket. It goes in the oracle pipeline rather than being captured locally by
Watchog, so it lands in Neon and publishes to IPFS like every other source and any consumer can
use it.

Watchog is the first consumer but not the intended only one, so nothing here is shaped to its
fact classes. What it needs from this source is a category and a point per business, at a
density that supports counting co-occurrences within a few hundred metres. Its own writeup of
why the existing sources could not supply that is in `docs/business-poi-data-brief.md` in the
`agent-watchog-intelligence-layer` repository.

## Decision 1 — a new `business_location` concept, not `companies` and not `nearby_location`

**Recommendation:** new tables (`business_locations` and children) in the query DB, a documented
entry in `elephant-query-db/docs/open-lexicon-gaps.md`, and a new lexicon class proposed
separately. This is the highest-consequence decision here and the one most expensive to get
wrong.

A registered legal entity and a physical business location are different things. One company
operates several locations; one location hosts several companies over time. The two cardinalities
are not reconcilable in a single table, and collapsing them destroys both.

**Rejected: load places into `companies`.** Superficially attractive because Sunbiz companies
already land there and it avoids new tables. It fails on three counts. `companies` is the shared
*party* role — appraisal owners, Sunbiz registrants, BBB profiles and permit contractors all FK
into it, at roughly 5.5M rows — so adding 40k Lee places pollutes a parent every track depends
on, in the table `query-db-loading-matching` already flags as the deadlock and
`TRUNCATE … CASCADE` danger zone. It also has nowhere to put a point geometry, an operating
status or a category: the lexicon `company` class has three properties (name, request
identifier, source request). And it makes "how many companies do we have" unanswerable.

**Rejected: reuse the lexicon's `nearby_location`.** This is the trap that looks right — the
class exists, its description is explicitly about points of interest, and it has a
`location_type`. It fails because it is **property-relative, not an entity**. Its properties are
`distance_miles` and `is_walkable`, both meaningless without a subject property, and its
`location_type` is a closed 13-value lifestyle enum (Shopping, Dining, Entertainment, Nature,
Fitness, Beach, Historical Site, Park, Museum, Recreation, Pet Services, Wellness, Tourist
Attraction) that cannot express ~2,300 Overture categories. It has no stable identifier, no
geometry and no operating status. `nearby_location` is a marketing-facing projection *derived
from* a places dataset; storing places in it would mean materialising one row per
(property, place) pair and losing the place's own identity. If NEO wants nearby locations for a
property, that is a spatial query over `business_locations`, computed on demand.

**Edges.** A place connects to Elephant's world through three links, all deliberately weak at
ingest:

- `address_id` → `addresses`, resolved by `normalized_address_hash`, `ON DELETE SET NULL`,
  exactly as the `business_reputation_*` tables do.
- `company_id` → `companies`, **NULL at ingest**, populated only by a later name+address match
  carrying an explicit confidence and method, following the `contractor_quality_scores`
  precedent and the standing rule that only high-confidence links become FKs.
- parcel → a separate `business_location_parcel_links` bridge, computed after ingest by
  point-in-polygon against `parcel_boundaries`, confidence-scored. Boundaries exist for Lee only
  today, so this is a no-op elsewhere.

**Lexicon path, and a warning.** The Sunbiz precedent is to create the tables now and log the
gap — `business_registration`, `business_registration_address` and `business_registration_party`
are described in `docs/sunbiz-lexicon-transform-findings.md` as classes added to `../lexicon`,
and `open-lexicon-gaps.md` describes them as "new classes now exist locally". They are **not in
`lexicon` `main`** (`src/data/lexicon.json`, checked 2026-08-12). So the precedent is really two
things: creating the tables ahead of the lexicon works, and the lexicon PR gets dropped. Track
it explicitly.

## Decision 2 — `expected_count` is NULL

`oracle_dataset_coverage` is keyed `(county, source)` and MCP derives
`completionPercent = round(ingested / expected * 100)` when `expected > 0`, else null. For
appraisal the denominator is real: the seed folio count is authoritative. For Overture there is
no authoritative answer to "how many businesses exist in Lee County", and every candidate
denominator is worse than none:

- **Overture's own release count** is the numerator. Using it produces a permanent, meaningless
  100%.
- **Sunbiz active registrations** counts legal entities, not locations, and includes entities
  with no premises at all.
- **DBPR licence counts** cover a single regulated sector.

So `expected_count` is NULL and `completionPercent` is null. That is the honest answer and it is
already the repo's idiom rather than a new invention: `write-public-coverage-snapshot.ts` emits
`expected_count: null` for every non-appraisal track under the comment "unknown expected counts,
which means 'not ingested' rather than 'complete at zero'", and `validate-publication-dry-run.ts`
fails a publication with `Coverage track … is not honestly incomplete` if a non-appraisal track
carries one.

Note the state in Neon does not match that doctrine yet. Eight rows (Lee, Miami-Dade, Orange,
Palm Beach across appraisal/permits/sunbiz/bbb) have `expected_count = ingested_count` from the
original backfill and therefore read as 100% complete by construction. Santa Clara, loaded most
recently, correctly has NULL. Places follows Santa Clara.

The real numbers live in a per-run `overture_place_extractions` record: release, county FIPS,
boundary source and vintage, features in bbox, features after clip, distinct primary categories,
distinct source datasets, counts by operating status, confidence distribution, duration.

**The DBPR cross-check is a quality assertion, not a denominator.** Florida DBPR's cosmetology
bulk file lists 1,420 licensed salon establishments in Lee against Overture's 1,436 combined
beauty/hair/nail/barber places. That close agreement is real evidence of sector coverage and
belongs in the run report. Promoting it to `expected_count` would imply a completeness guarantee
across all sectors that a single-sector, single-state comparison cannot support.

## Decision 3 — hosted services are flagged in ingestion, and excluded nowhere

Roughly 250 of Lee's 1,244 categories describe services hosted inside another business rather
than a business occupying a location: `atms`, `rental_kiosks` (Redbox), `propane_supplier`,
`money_transfer_services`, `trusts`. Any spatial co-location analysis that ignores this reads a
Redbox in a supermarket lobby as a second business sharing the building — which is precisely the
Watchog fact class that motivated the ingest.

Three options, and the middle one wins.

**Rejected: exclude at ingest.** Cleanest for consumers, and wrong. The pipeline ground rule is
that data is extracted as fully as possible, unmapped fields are preserved and gaps are logged
rather than dropped. The judgement is also contestable — a standalone ATM kiosk on a forecourt
genuinely is a place — so it must not be baked into what gets stored.

**Rejected: leave it entirely to consumers.** Guarantees every consumer re-derives the rule
differently, and guarantees the default behaviour is wrong.

**Chosen: a derived, named, overridable flag.** `is_hosted_service` plus `hosted_service_rule`
recording which version of the rule set it, driven by a committed list keyed on taxonomy path
and versioned with the taxonomy release. Advisory: consumers may ignore it, and the rule that
produced it is auditable in git rather than buried in a query.

The list has to be rebuilt before it means anything — the 250 categories were counted against
the deprecated `categories` vocabulary (below).

## Decision 4 — the attribution notice ships inside the published artifact

Places is CDLA-Permissive-2.0 / Apache-2.0 / CC0 per record with no OpenStreetMap lineage, so
none of the ODbL share-alike machinery applies. The Foursquare-derived subset (4,748,001
records) is Apache-2.0 and requires preserving `Copyright 2024 Foursquare Labs, Inc. All rights
reserved.` plus a statement that the files were changed and the date. Because oracle publishes
to public IPFS, that is a shipping obligation, not a README line. Three locations:

1. **`NOTICE.txt` at the root of the published places bucket**, so it sits inside the DAG the
   IPNS name resolves to and travels with every copy. Overture citation with accessed date, the
   full per-provider licence list, the Foursquare copyright line, and **Elephant's own** change
   statement and date — Overture's `Changed: 2026-03-18` describes Overture's transformation of
   Foursquare data, not ours.
2. An `attribution` block in the places table's sibling `index.json`, so machine consumers get
   it without a second fetch.
3. `docs/overture-places-sources.yaml`, matching the `license:` /
   `publication: open-with-attribution` / `bulk_terms.decision` shape that
   `docs/rock-island-sources.yaml` established for a source with unresolved redistribution
   terms.

The obligation is also made enforceable rather than trusted: `business_location_sources` stores
every `sources[]` entry, and the run asserts `DISTINCT dataset` is a subset of the approved
providers (attribution-page nine plus `Overture` / `Overture-signals` by human decision
2026-08-12). If `osm` or any other unknown provider ever appears, the publish stops — Overture's bridge
files already list OSM as a bridged source for the places theme even though the attribution page
does not, so this is a live risk rather than a hypothetical one.

**These terms were read from Overture's published documentation on 2026-08-12 and have not been
reviewed by counsel.** Overture separately warns that joining CDLA-Permissive data to
OpenStreetMap can make the resulting derivative database ODbL, which becomes relevant the moment
a published Elephant artifact combines places with an ODbL source.

## What complicates the plan

**The category vocabulary the research measured is being removed.** `categories.primary` — the
2,117-entry vocabulary in which Lee's 1,244 distinct categories and the ~250 hosted-service
categories were counted — is deprecated and **removed in the September 2026 release**, replaced
by `taxonomy` (primary, hierarchy path, alternate) and `basic_category`. Given the date, keying
on it would ship something with roughly a month of life. Everything downstream must key on
`taxonomy.hierarchy`, and every stored row must be stamped with the release it came from,
because the July 2026 taxonomy redesign renamed 407 categories, removed 80 with redirect rules,
reparented 482 and repathed 2,108. The taxonomy moves quarterly.

**GERS ids are stable by design, but the reference release is the churny one.** Ids are GERS
UUIDs backed by a registry (`first_seen`, `last_seen`, `last_changed`, `version`), a per-release
changelog and bridge files — verified from Overture's documentation, not assumed. But the places
theme is documented as carrying duplicates, a high junk rate and low property completeness;
Overture's own tutorial records that only 1,546 of the new places in the May 2025 release
reappeared in June with their new UUIDs; and `2026-07-22.0`, the release the scoping research
used, **re-matched the entire corpus with a new matching pipeline with a warned "one-time
elevated level of GERS ID churn"**. A first baseline taken from that release will show unusual
churn against August. That is expected. It is also why refresh upserts and never deletes, and
why closure is read from `operating_status` and `confidence` rather than inferred from a record
disappearing.

**Adding a coverage source touches three hardcoded lists.** `COVERAGE_SOURCES` in
`oracle-dataset-coverage-upsert.ts`, the `["permits", "corporate", "bbb"]` track list in
`write-public-coverage-snapshot.ts`, and the same list again in the "honestly incomplete" loop in
`validate-publication-dry-run.ts`. Those two hardcoded lists also expose a pre-existing
inconsistency: the same track is `sunbiz` in the Neon table and `corporate` in the published
snapshot. Places must use one spelling, `overture_places`, in both.

**Stage skills are not authored in this repo.** Every directory under `.agents/skills/` is
untracked in `oracle-node` and installed from `elephant-xyz/skills`, pinned by
`skills-lock.json`. The spec is committed here so it is reviewable alongside the reasoning, but
it has to be upstreamed with a lock entry or the next skills sync will not know it exists.

## Open questions for the team

**Answered for this pass (do not reopen county order):**

- **County order:** Lee County only (`12071`). Orange, Miami-Dade, Palm Beach, and Santa Clara
  are not ingested here. Reconcile the Lee clip against the design-note baseline of **40,190**
  for release `2026-07-22.0`.
- **Extraction location (assumed, not a permanent decision):** laptop for the first county. A
  full-county DuckDB read is minutes, not hours, so a laptop is defensible. Load and publish
  still inherit the in-region Fargate warning from `county-open-data-publish`.
- **PII gate:** approved 2026-08-12 to publish the Lee artifact as-is, including Overture
  business `emails` and `phones` as public business-contact fields.
- **`basic_category`:** pass Overture labels through; do not invent an Elephant vocabulary
  mapping.
- **Counties with no parcel boundaries:** N/A for Lee (Lee has parcel boundaries).
  `business_location_parcel_links` is a later step, not ingest — schema stub only.

**Still open (humans):**

- Permanent extraction location after the Lee pilot (laptop vs in-region Fargate).
- Whether the Lee PII approval should extend to later county publications.
- Whether anything downstream later needs `basic_category` mapped onto an Elephant vocabulary.
- Whether a places record is worth publishing for counties with no parcel boundaries, given the
  parcel link stays empty.
- Lexicon PR for `business_location` (not opened in this pass; Sunbiz precedent dropped).
