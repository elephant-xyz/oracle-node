# Overture Places — Lee County pilot

Date: 2026-08-12
County: Lee, FL (FIPS `12071`)
Working branch: `overture-places-ingest-spec` (oracle-node), `overture-places-ingest` (elephant-query-db worktree)

Extract artifacts (not committed): `downloads/overture-places/lee/2026-07-22.0/`
(`places/places-part-0001.jsonl` … `0009.jsonl`, `manifest/summary.json`).

## Operator decisions (do not reopen)

- **County order:** Lee only in this pass. Orange, Miami-Dade, Palm Beach, and Santa Clara are not ingested here.
- **Extraction location:** laptop for the first county. A full-county DuckDB read is minutes, not hours, so a laptop is defensible. Load and publish still inherit the in-region Fargate warning from `county-open-data-publish`.
- **PII gate (2026-08-12):** **publish as-is**, including Overture business `emails` and `phones` as public business-contact fields. Recorded as the 2026-08-12 PII-gate approval. Filebase/IPFS publish of the Lee places artifact is authorized.
- **`basic_category`:** pass Overture labels through; do not invent an Elephant vocabulary mapping.
- **Parcel links:** Lee has parcel boundaries, but `business_location_parcel_links` is a later step, not ingest. Schema stub only.

## How to run locally

Discover the latest release (do not hard-code it in scripts; pin it in the run record):

```bash
curl -s https://stac.overturemaps.org/catalog.json | python3 -c 'import json,sys; print(json.load(sys.stdin)["latest"])'
```

Lee extract against the scoping baseline (`2026-07-22.0`):

```bash
node scripts/extract-overture-places.mjs \
  --county lee --county-fips 12071 \
  --release 2026-07-22.0 \
  --boundary-source tiger/tl_2024_us_county \
  --output-dir downloads/overture-places/lee/2026-07-22.0
```

Verified probe (two-stage bbox then `ST_Within`, no JSONL):

```bash
node scripts/extract-overture-places.mjs \
  --county lee --county-fips 12071 \
  --release 2026-07-22.0 \
  --boundary-source tiger/tl_2024_us_county \
  --counts-only \
  --output-dir downloads/overture-places/lee/probe
```

STAC `latest` at run time was `2026-07-22.0` (same as the scoping baseline), so a second-release extract was not required.

Export + validate are implemented against **Neon** (current `business_locations`),
not the extract JSONL. After the 2026-08-12 PII-gate approval, the validated
artifact was published to a dedicated Filebase bucket / IPNS name. Catalog
`placesTableUrl` for Lee is the canonical public parquet URL.

```bash
node scripts/export-overture-places-table.mjs \
  --from-neon \
  --env-file ../elephant-query-db/.env.local \
  --county lee --release 2026-07-22.0 \
  --out downloads/overture-places/lee/2026-07-22.0/publish

node scripts/validate-overture-places-table.mjs \
  --from-neon \
  --env-file ../elephant-query-db/.env.local \
  --county lee --release 2026-07-22.0 \
  --parquet downloads/overture-places/lee/2026-07-22.0/publish/lee/places-table.parquet
```

Load into Neon (query-db `DATABASE_URL` / prefer `DATABASE_URL_UNPOOLED` from
`elephant-query-db/.env.local` — not `NEO_OPENDATA_DATABASE_URL`). Stage the extract
to S3 first; the bulk loader lists prefixes, it does not read a local directory.

```bash
# Stage JSONL + manifest (already done for this run)
AWS_PROFILE=elephant-management AWS_REGION=us-east-1 aws s3 sync \
  downloads/overture-places/lee/2026-07-22.0/ \
  s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/overture-places/lee/2026-07-22.0/ \
  --exclude "*" --include "places/*.jsonl" --include "manifest/summary.json"

cd ../elephant-query-db-overture-places
# apply migrations/0007_overture_places.sql first if tables are missing
set -a; source ../elephant-query-db/.env.local; set +a
export DATABASE_URL="$DATABASE_URL_UNPOOLED"
export AWS_PROFILE=elephant-management
export AWS_REGION=us-east-1
npx tsx scripts/run-bulk-data-load.ts \
  --tracks places \
  --places-prefix overture-places/lee/2026-07-22.0/ \
  --bucket elephant-oracle-node-environmentbucket-mmsoo3xbdi80 \
  --env-file ../elephant-query-db/.env.local \
  --stage-dir ../oracle-node/downloads/overture-places/loader-staging \
  --jurisdiction-key lee_appraiser
```

`AWS_PROFILE=elephant-oracle-node` cannot assume `OrganizationAccountAccessRole` from
this IAM user; `elephant-management` can list/put/get the env bucket. The worktree has
no `.env.local` — point `--env-file` at the original query-db file. Put `--stage-dir`
on the 2TB volume (system disk is tight; the stage CSV is ~750 MB).

Never commit extracted place data.

## Reconciliation vs 40,190

Laptop extract of release `2026-07-22.0`, TIGER `tl_2024_us_county` (2024), 2026-08-12.

| Field                                  | Value                                                                                                       |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Overture release                       | `2026-07-22.0`                                                                                              |
| STAC latest at run time                | `2026-07-22.0`                                                                                              |
| TIGER vintage                          | `tl_2024_us_county` (year 2024)                                                                             |
| bbox count (do not publish)            | 40,517                                                                                                      |
| **clip count**                         | **40,191** (+1 vs 40,190 baseline)                                                                          |
| distinct `taxonomy.primary`            | 1,195                                                                                                       |
| distinct `taxonomy.hierarchy` paths    | 1,194                                                                                                       |
| distinct `sources[].dataset`           | AllThePlaces, BrightQuery, DAC, Foursquare, meta, Microsoft, Overture, Overture-signals, PinMeTo, RenderSEO |
| `operating_status` counts              | open 25,049; (blank) 14,698; permanently_closed 444                                                         |
| `is_hosted_service` flag count         | 956 (after restamping JSONL against the rebuilt five-path list)                                             |
| duration                               | 363,985 ms (~6.1 min) including the two-stage count + COPY + JSONL                                          |
| extraction location                    | laptop                                                                                                      |
| address-vs-geometry county discrepancy | 0                                                                                                           |

The +1 vs the design-note 40,190 is real for this TIGER 2024 clip of `2026-07-22.0`. Do not treat 40,190 as `expected_count`.

## Hosted-service list rebuild

Committed at `config/hosted-service-categories.txt` (and the skill copy): **5 full `/`-delimited paths**, matching is on the full path.

Old flat leaves → observed `taxonomy.hierarchy` (L0 first):

| Old `categories.primary` leaf | Observed path                                                                       | Lee count |
| ----------------------------- | ----------------------------------------------------------------------------------- | --------- |
| `atms`                        | `services_and_business/financial_service/atm`                                       | 323       |
| `rental_kiosks`               | `services_and_business/real_estate/real_estate_service/rental_service/rental_kiosk` | 83        |
| `propane_supplier`            | `services_and_business/b2b_service/supplier_or_distributor/propane_supplier`        | 99        |
| `money_transfer_services`     | `services_and_business/financial_service/money_transfer_service`                    | 351       |
| `trusts`                      | `services_and_business/financial_service/trusts`                                    | 100       |

The July 2026 taxonomy uses singular leaves (`atm`, not `atms`). No other paths were committed. The scoping research's ~250 hosted-looking `categories.primary` values were never enumerated as hierarchy paths; expanding to that size without evidence was refused.

Reviewed and **not** committed: `shopping/kiosk`, `vending_machine_supplier`, `wills_trusts_and_probate`, medical `*_treatment_center` paths, `window_treatment_store`.

## Licence gate

Human decision **2026-08-12:** allow `Overture` and `Overture-signals` as approved
`sources[].dataset` values. They are Overture's own lineage, not OSM and not an unknown
third-party licence. The allowlist is now the attribution-page nine plus those two.
**OSM remains a hard stop.** Do not silently widen further.

`osm` did **not** appear (0 rows).

Live `sources[].dataset` values (as written by Overture, TitleCase except `meta`):

- `AllThePlaces`, `BrightQuery`, `DAC`, `Foursquare`, `meta`, `Microsoft`, `Overture`, `Overture-signals`, `PinMeTo`, `RenderSEO`
- `krick` was absent in Lee
- `Overture` on all 40,191 rows; `Overture-signals` on 24,144 rows

The gate is case-insensitive (`Microsoft` matches `microsoft`). After the 2026-08-12 allowlist
update, the Lee extract **passes** (40,191 rows; `unknownDatasets: []`; `osmPresent: false`).
Licence is not the remaining gate; the 2026-08-12 PII approval authorized public publish.

## Neon load (2026-08-12)

Applied `migrations/0007_overture_places.sql` to the documented query-db Neon
(`neondb` / `ep-mute-leaf`, project `raspy-frost-51580436`) via the unpooled
`DATABASE_URL`. Artifact URI used:

`s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/overture-places/lee/2026-07-22.0/`

Wall clock **~28.1 min** (01:21:58Z–01:50:07Z). Staging 10 artifacts was ~45 s;
COPY of the 750 MB stage CSV plus merges ran until ~01:35Z; the post-load
`is_current` correlated update took the remaining ~15 min (rewritten afterward
to a grouped `max(last_seen_release)` join for re-runs).

| Table                            | Count            | Notes                                                        |
| -------------------------------- | ---------------- | ------------------------------------------------------------ |
| `business_locations`             | **40,191**       | Lee, release `2026-07-22.0`. **No gap** vs JSONL clip count. |
| `business_location_categories`   | 38,137           |                                                              |
| `business_location_sources`      | 297,856          | licence gate **PASS** on distinct `dataset` values           |
| `overture_place_extractions`     | 1                | Lee / `2026-07-22.0` from `manifest/summary.json`            |
| `business_location_parcel_links` | 0                | not populated (later step)                                   |
| `company_id` non-null            | 0                | never written                                                |
| `addresses` linked               | 39,143 of 40,191 | 1,048 places have no resolvable address; not a location gap  |
| geometry filled                  | 40,191           | `ST_MakePoint` 4326 after merge                              |

`oracle_dataset_coverage` row (`source = overture_places`, county `lee`):

| Field                                | Value                                                         |
| ------------------------------------ | ------------------------------------------------------------- |
| `ingested_count`                     | 40,191                                                        |
| `expected_count`                     | **NULL** (Santa Clara pattern)                                |
| `first_loaded_at` / `last_loaded_at` | 2026-08-13T01:35:33.757Z                                      |
| `cid`                                | `bafybeicfvfm5reer2ugipirxufpu6u3tmseoezsdfyhseysoo6p5r2mj4a` |
| `ipns_label`                         | `oracle-open-data-lee-places`                                 |

Loaded `business_location_sources` distinct datasets (licence gate passed,
`osmPresent: false`, `unknownDatasets: []`): AllThePlaces, BrightQuery, DAC,
Foursquare, Microsoft, Overture, Overture-signals, PinMeTo, RenderSEO, meta.

`source_system = 'overture_places'`. `gers_id` is the raw GERS id;
`source_record_key` is `overture_places:<gers_id>` (merge key with `source_system`).

Parcel links were not populated.

## Filebase / IPFS publish (2026-08-12 PII approval)

**Human approval (2026-08-12):** publish as-is, including Overture business
`emails` and `phones` as public business-contact fields.

Dedicated places family (not shared with property, query-table, or permit artifacts):

| Field                        | Value                                                                                                                   |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| Filebase bucket              | `elephant-oracle-open-data-lee-places`                                                                                  |
| IPNS label                   | `oracle-open-data-lee-places`                                                                                           |
| Directory CID                | `bafybeicfvfm5reer2ugipirxufpu6u3tmseoezsdfyhseysoo6p5r2mj4a`                                                           |
| IPNS name (`network_key`)    | `k51qzi5uqu5djfa3kbhcxedqlh7kiuyi22bd60he1nsa0wr2jrseo6vvxvwke5`                                                        |
| Canonical public parquet URL | `https://ipfs.filebase.io/ipns/k51qzi5uqu5djfa3kbhcxedqlh7kiuyi22bd60he1nsa0wr2jrseo6vvxvwke5/lee/places-table.parquet` |
| Gateway verification         | **2026-08-13T03:59:11Z** (UTC)                                                                                          |

Artifact root published into the DAG (so IPNS resolves the full family):
`downloads/overture-places/lee/2026-07-22.0/publish/` (gitignored).

| File                       | Size                                                                                                             |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `lee/places-table.parquet` | 15,167,639 bytes (~14 MB)                                                                                        |
| `lee/index.json`           | attribution sibling; `published: true`; `piiGate: approved-2026-08-12-publish-as-is-including-emails-and-phones` |
| `NOTICE.txt`               | 1,572 bytes (artifact root)                                                                                      |

Export queried current Neon `business_locations` (`is_current`, Lee, `2026-07-22.0`)
via `DATABASE_URL_UNPOOLED`. Standalone validator re-read the parquet and re-queried
Neon immediately before upload. Publication gates (all **PASS**):

- parquet row count **40,191** == current Lee `business_locations` for the release
- zero duplicate GERS ids
- zero null geometries
- live loaded `business_location_sources` licence gate: PASS, OSM absent, unknowns empty
- `taxonomy.hierarchy` present as `/`-delimited scalars

Public IPNS gateway (`ipfs.filebase.io`) verification at **2026-08-13T03:59:11Z**:

- `NOTICE.txt` HTTP 200, 1,572 bytes, Foursquare copyright + Elephant change statement
- `lee/index.json` HTTP 200, `rowCount` 40191, attribution block present, `published` true
- `lee/places-table.parquet` HTTP 200, `content-length` 15,167,639, magic `PAR1`
- `x-ipfs-roots` starts with directory CID `bafybeicfvfm5reer2ugipirxufpu6u3tmseoezsdfyhseysoo6p5r2mj4a` (places family, not query-table/properties)

## DBPR salon cross-check (quality assertion, not `expected_count`)

Florida DBPR cosmetology bulk file: **1,420** licensed salon establishments in Lee.

Overture `taxonomy.primary` in `{beauty_salon, hair_salon, nail_salon, barber}`: **1,435** (514 + 453 + 268 + 200). Scoping research reported 1,436; the +1 tracks the clip delta.

Close agreement is evidence of sector coverage. It is **not** an `expected_count`.

## Still open (humans)

- County order is answered: Lee first.
- Permanent extraction location after the Lee pilot (this run was laptop).
- PII gate for Lee places is answered: **2026-08-12 publish as-is**, including `emails`/`phones`.
- Whether anything downstream later needs `basic_category` mapped onto an Elephant vocabulary (pass-through for now).
- Lexicon PR for `business_location` (not opened; Sunbiz precedent dropped).
- Parcel-link step after ingest.
- Whether a places record is worth publishing for counties with no parcel boundaries.

## Left unimplemented on purpose

- `business_location_parcel_links` population
- Lexicon class PR
- Counties other than Lee
