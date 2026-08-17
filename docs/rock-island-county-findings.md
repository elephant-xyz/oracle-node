# Rock Island County, IL — Acquisition source discovery

Discovery completed on 2026-08-01 for the DECA parcel-acquisition CRM.

**County:** Rock Island County, Illinois
**County FIPS:** `17161`
**County key:** `rock-island`
**Scope:** AWS-free discovery and a private 25-parcel technical proof
**Priority:** parcel geometry, parcel identity, acreage, valuation, owner/mailing
semantics, and public power-screening sources
**Deferred:** permits, contact-data purchase, AWS ingestion, public IPFS publication,
and full-county download

The machine-readable companion is
[`rock-island-sources.yaml`](./rock-island-sources.yaml). The proposed request for
public-redistribution rights is
[`rock-island-open-data-request.md`](./rock-island-open-data-request.md).

## Executive recommendation

Use the county's official public ArcGIS parcel layer for source discovery and a small
private proof, but do not start a full-county mirror or public redistribution until Rock
Island County GIS confirms those rights in writing.

The ArcGIS item is public and says "For use by the general public." Separately, the
county publishes parcel/assessment pricing and a project-specific Digital Data Release
Policy. Those two signals do not clearly grant permission to create a durable public
mirror. The assessment and property-tax search sites also expressly prohibit automated
retrieval, so those DevNet pages are not ingestion sources.

The intended publication boundary is:

- **Open after approval:** parcel PIN, polygon, acreage, site/location fields, class and
  zoning, EAV/estimated market value, tax year, provenance, and refresh timestamp.
- **Restricted pending explicit approval:** owner and tax-bill names and mailing
  addresses.
- **Always private:** purchased email addresses, telephone numbers, suppression state,
  campaign history, responses, asking prices, and acquisition status.

## 1. Property and assessment sources

### Official county GIS parcel layer

- County-linked viewer:
  `https://ricogis.maps.arcgis.com/apps/webappviewer/index.html?id=f53cd25919f0443080b68c45e3144741`
- ArcGIS item:
  `https://www.arcgis.com/home/item.html?id=9cae8a64ab0e4cea99758f741ca43b3c`
- Feature layer:
  `https://services9.arcgis.com/6FnscPPlUa9DXXOk/ArcGIS/rest/services/Parcels/FeatureServer/0`
- Access: public, no authentication observed, `Query` capability.
- Formats: JSON, GeoJSON, and PBF queries; the service metadata also advertises CSV,
  GeoPackage, Shapefile, GeoJSON, Parquet, and other export formats.
- Maximum query page: 2,000 records.
- Native CRS: WKID `102672`, latest WKID `3436` (NAD83 Illinois West, US feet).
- CRM/map CRS: request `outSR=4326`; the 25-record proof returned valid WGS84
  `Polygon` and `MultiPolygon` features.
- Working denominator: **65,956** parcel polygons/PINs on 2026-08-01.
- ArcGIS item modified: **2026-07-14**.
- ArcGIS item license text: **"For use by the general public."**
- Published assessment year in the layer: `2025` or null.

The layer exposes the acquisition-relevant fields directly:

- Identity: `PIN`, `parcel_number`, `alternate_parcel_number`, `RICO_PARCE`.
- Geometry/location: polygon geometry, `GIS_acres_num`, `gross_acres`,
  `X_longitude`, `Y_latitude`, site address/city/state/ZIP.
- Assessment: `EAV`, `EMV`, farm/non-farm land and building values,
  `taxbill_year`, `assessed_last`.
- Classification: `class`, `Zoning`, township, municipality, jurisdiction, and
  taxing-district fields.
- Parties: `owner1_*` and `taxbill_*` names and mailing fields.
- Other potentially useful facts: last sale date/price, legal description, model,
  year built, and total square footage.

`OBJECTID` is only a source row locator. `PIN` is the durable county join key.

### County and township assessment responsibilities

Rock Island County has 18 township assessment jurisdictions. Township assessors
determine values and maintain property cards; the Chief County Assessment Office and
county GIS consolidate countywide assessment and parcel data. The county GIS layer is
therefore the practical countywide discovery source, but its owner/value fields should
not be described as a title opinion or guaranteed current ownership.

Official references:

- Assessment Office: `https://www.rockislandcountyil.gov/172/Assessment-Office`
- Township assessors: `https://www.rockislandcountyil.gov/213/Township-Assessors`

### Sources that must not be automated

The following pages state that their information is for individual inquiry and review
only and expressly prohibit crawlers, robots, or similar automated retrieval:

- Assessment search: `https://www.rockislandcountyil.gov/176/Assessment-Search`
- Property-tax search: `https://www.rockislandcountyil.gov/350/Property-Tax-Search`

Do not build a browser flow, hidden-API replay, or scraper against those DevNet pages.

## 2. Parcel identifier

- Official join key: `PIN`.
- ArcGIS type: string.
- ArcGIS maximum length: 10 characters.
- Proof result: 25/25 populated, 25 unique, all 10 characters.
- Preservation rule: keep the value as text. Never coerce it to a number or strip
  leading zeroes.
- Source aliases requiring validation before a full load: `parcel_number`,
  `alternate_parcel_number`, and `RICO_PARCE`.

Before full ingestion, compare the GIS `PIN` against any county-authorized assessment
export and document whether punctuation or alternate forms occur.

## 3. Permit sources

Permits are intentionally out of scope for the acquisition MVP. The county Zoning and
Building Safety department covers unincorporated property only; cities and villages
operate their own departments. A future permit milestone must enumerate jurisdictions
and create a vendor/source catalog rather than assume one countywide portal.

Official starting point:
`https://www.rockislandcountyil.gov/363/Zoning-Building-Safety`.

## 4. Bulk data and publication rights

### County GIS terms

The county's published pricing guide lists:

- Parcel vector data: `$0.07` per parcel.
- Tax assessment data: `$0.03` per parcel.
- Countywide parcel/assessment package: `$2,500`.
- Annual vector subscription with quarterly updates: `$2,000` for the first year and
  `$1,000` for subsequent years.

Pricing must be confirmed because the guide does not state an effective date.

References:

- Pricing guide: `https://www.rockislandcountyil.gov/DocumentCenter/View/944`
- Digital Data Release Policy:
  `https://www.rockislandcountyil.gov/DocumentCenter/View/1159/Data-Release-PDF`

The release policy authorizes data to a named entity for a described project, requires
Rock Island County GIS attribution, requires modifications to be disclosed on hard-copy
maps, and disclaims warranty. It does not expressly grant broad public redistribution
or permanent third-party mirroring.

### Decision

- A limited private technical proof is complete.
- No full-county download or public mirror is authorized by this discovery.
- Send the prepared rights request to Rock Island County GIS before scaling or
  publishing.
- If public redistribution is denied, use a county-authorized internal dataset or a
  runtime link/query pattern and publish only independently derived, non-reconstructive
  outputs whose rights have been reviewed.

## 5. Class and land-use vocabulary

The ArcGIS layer contains a `class` code and `Zoning`. A countywide grouped query found
28 distinct class values including null. The dominant values are `0040`, `0030`,
`0060`, `0021`, `0090`, and `0011`.

The code meanings were not present in the service metadata. Do not infer
commercial/industrial eligibility from the numeric values. Obtain the county's class
code dictionary before implementing acquisition filters. `Zoning` is also incomplete:
1,108 records are null or blank.

## 6. Acquisition and power sources

### Public screening sources

1. **HIFLD U.S. Electric Power Transmission Lines**
   - Catalog:
     `https://catalog.data.gov/dataset/electric-power-transmission-lines`
   - Access level: public.
   - License: U.S. government works.
   - Source date: 2022-10-24; treat as archived/stale screening data.
   - Existing CRM ArcGIS service:
     `https://services2.arcgis.com/LYMgRMwHfrWWEg3s/arcgis/rest/services/HIFLD_US_Electric_Power_Transmission_Lines/FeatureServer/0`

2. **EIA-860 and EIA-860M power plants**
   - Annual data: `https://www.eia.gov/electricity/data/eia860/`
   - Monthly inventory: `https://www.eia.gov/electricity/data/eia860m/`
   - Includes latitude/longitude for plants with at least 1 MW combined nameplate
     capacity.
   - Useful as a generation/proximity screening layer, not as evidence of available
     transmission capacity.

3. **Illinois Commerce Commission filings**
   - Docket `P2014-0494`:
     `https://icc.illinois.gov/docket/P2014-0494`
   - Documents a 345 kV line beginning at MidAmerican's Oak Grove Substation in Rock
     Island County and associated 161 kV work.
   - Use filings as cited qualitative route/project context unless a specific map has
     separate reusable-data terms.

4. **MidAmerican Energy service territory**
   - `https://www.midamericanenergy.com/territory-communitylist`
   - Confirms electric service in Rock Island-area communities.
   - Service territory does not establish that a parcel can obtain data-center load.

### Restricted or qualified source

Current EIA guidance says EIA and HIFLD do not publish electric-substation locations.
The CRM contains a legacy national ArcGIS substation fallback, but that archive must not
be republished as open data without a separate rights review. It may only be labeled as
qualified screening context while that review remains unresolved.

### Required disclaimer

Power layers are screening only. Public and archived geometry may be incomplete or
stale. Proximity and published voltage do not establish service, deliverable capacity,
cost, schedule, redundancy, or interconnection feasibility. Confirm each candidate with
MidAmerican Energy, MISO, and an engineering/interconnection study.

## 7. Source feasibility

### Parcel GIS

- Technical access: plain ArcGIS REST query, no authentication or challenge observed.
- Countywide records: 65,956.
- Maximum query page: 2,000, implying at least 33 pages for a complete export.
- Geometry can be requested directly in EPSG:4326.
- No concurrency or full-download benchmark was performed because public
  redistribution and bulk-use rights are not yet confirmed.
- Recommended mode after approval: county-supplied bulk artifact or a rate-limited
  ArcGIS export, retained with provenance and a source revision.

### Assessment and tax DevNet

- Automated access: prohibited by the published disclaimer.
- Recommended mode: do not ingest from DevNet. Use only a county-authorized GIS/bulk
  export.

### Power

- HIFLD transmission: reusable public screening layer, but stale.
- EIA plants: reusable official public inventory.
- Substations: no approved open coordinate source identified.
- ICC filings: case-by-case contextual evidence, not a canonical spatial layer.

## 8. Risks

- Public technical access does not by itself settle bulk or public-redistribution rights.
- The public parcel viewer is marked for retirement; the FeatureServer URL can change.
- 151 polygons lack the main assessment, owner, tax-bill, site-address, class, and
  tax-year values. Treat them as mapped parcels with incomplete enrichment.
- Empty strings occur in addition to nulls; completeness checks must normalize both.
- `owner1_*` and `taxbill_*` are different concepts and cannot be merged silently.
- Township-maintained property cards may be fresher or more detailed than county GIS.
- PINs must remain strings.
- Zoning and class-code semantics require a county dictionary.
- Owner and mailing data are public-record facts but remain personal data; public-record
  status does not settle open-republication terms or privacy policy.
- IPFS publication is difficult to retract. It is prohibited until the county confirms
  redistribution rights and the field-level release boundary.

## 9. Private 25-parcel technical proof

The official FeatureServer returned 25 ordered records to an internal GeoJSON sample at:

`downloads/rock-island/samples/parcel-25.geojson`

`downloads/` is gitignored. The sample is local, mode `0600`, and must not be committed
or published.

Aggregate validation results:

- Features: 25.
- Unique PINs: 25; blank PINs: 0; PIN length: 10.
- Geometry: 23 `Polygon`, 2 `MultiPolygon`.
- Coordinate pairs: 299.
- Coordinates were valid WGS84 values inside Rock Island County.
- `GIS_acres_num`, `EAV`, `EMV`, owner name, tax-bill name/address, township, and
  tax year: populated on 25/25.
- Site address: nonblank on 12/25; a site address cannot be required.
- Tax year: 2025 on 25/25.
- Owner name equaled tax-bill name on 17/25 and differed on 8/25.
- GIS acreage: minimum 0.1803, maximum 50.7322, total 175.0255 acres.

The sample validates the source schema and map geometry path. It is not a statistically
representative sample, a title verification, a publication approval, or a 30-acre site
recommendation.

Countywide null checks on 2026-08-01:

- PIN null: 0.
- GIS acreage null: 0.
- EAV null: 151.
- EMV null: 151.
- owner name null: 151.
- tax-bill name null: 151.
- tax year null: 151.

## 10. CRM handoff contract

Oracle should preserve source facts and provenance keyed by PIN. Parcel geometry,
adjacency, candidate assembly, power-distance calculations, project creation, outreach,
and acquisition state remain responsibilities of `parcel-crm`.

### Open parcel feature, after approval

Each record should be a GeoJSON `Feature<Polygon | MultiPolygon>` in EPSG:4326 with:

- `objectId`: source row locator, not identity.
- `countyFips`: `"17161"`.
- `parcelIdentifier`: source `PIN`, preserved as text.
- `acreage`: source `GIS_acres_num`.
- `acreageProvenance`: source field `GIS_acres_num`.
- `siteAddress`, `siteCity`, `siteZip`: nullable source site fields.
- `zoning`: nullable source `Zoning`.
- `landUse`: raw source `class` until a county dictionary is obtained.
- `assessedValue`: source `EAV`.
- `marketValue`: source `EMV`.
- `taxableValue`: unavailable from this source; do not copy `EAV` into it.
- `longitude`, `latitude`: source coordinates or polygon centroid, with provenance.
- `sourceUrl`: official FeatureServer layer URL.
- `sourceRevision`: ArcGIS item modified timestamp plus `taxbill_year`.
- `snapshotAt`: ingestion timestamp.

The current CRM type hardcodes Lee County FIPS `12071`, Lee county name, and a required
taxable value. Those types must be generalized in the next `parcel-crm` phase rather
than forcing Rock Island facts into Lee semantics.

### Restricted owner enrichment

Preserve these as distinct source roles:

- `owner1_*`: county owner-name/address fields.
- `taxbill_*`: tax-bill recipient and mailing fields.

Do not pick one silently as the other. The 25-record proof found different names in
32% of sampled rows. Store source role, provenance, and snapshot date. Expose either
role publicly only if the county explicitly approves that field set for redistribution.

Purchased email/phone data is not Oracle open data. It belongs in encrypted CRM contact
records with vendor, confidence, purchase date, suppression state, and compliance
metadata.

## 11. Next-phase gates

Full ingestion and CRM implementation may start only after:

- [ ] Rock Island County GIS responds to the open-data request.
- [ ] Allowed fields, attribution, modification notices, refresh method, API limits,
      and public-mirroring rights are recorded.
- [ ] Owner and tax-bill publication are approved or explicitly excluded.
- [ ] A current certified parcel denominator is obtained or the 65,956 GIS denominator
      is accepted and labeled.
- [ ] The 151 incomplete polygons have a documented treatment.
- [ ] The class-code dictionary is obtained.
- [ ] PIN aliases are reconciled.
- [ ] An Illinois-safe query loader removes hardcoded Florida/Lee values.
- [ ] `parcel-crm` supports county FIPS `17161`, nullable Rock Island fields, the county
      GIS provider, EPSG:4326 polygons, and an Illinois-safe power overlay.
- [ ] A human approves any irreversible public/IPFS publication.

Until those gates pass: no AWS deployment, no full-county scrape, no public IPFS upload,
and no owner-data publication.

## 12. Oracle onboarding continuation — 2026-08-03

The operator explicitly chose to proceed with internal full-county onboarding despite
unresolved county bulk-use and redistribution terms. This supersedes the earlier
**ingestion** pause in section 11, but it does not authorize public publication. Preserve
Rock Island County GIS attribution and source-revision metadata on every artifact.

The publication boundary remains stricter:

- Public export is non-PII by default.
- Owner names, tax-bill names, owner/tax-bill mailing addresses, purchased contacts, and
  campaign data must not be exported.
- No Filebase/IPFS/IPNS upload may occur until source, Neon, and export counts reconcile;
  the county-specific bucket and IPNS name are verified; and a human approves the exact
  field manifest.
- If the exporter cannot prove the excluded fields are absent, stop before upload.

### 12.1 Prerequisites and infrastructure

- Existing `oracle-node` branch: `rock-island-property-first-ingest`.
- Official sibling repositories cloned successfully:
  `elephant-xyz/Counties-trasform-scripts` and `elephant-xyz/lexicon`.
- No Rock Island transform folder, browser flow, or permit adapter existed in either the
  local pipeline or the official transform repository.
- The local Neon `elephant-query-db` connection passed `select 1`.
- AWS verification is blocked locally because the configured AWS CLI does not contain
  the operator-specified `elephant-oracle-node` profile. No stack, bucket, secret,
  queue, Lambda, or S3 mutation was attempted.

### 12.2 Appraisal/source decision

Use the official ArcGIS FeatureServer as both the parcel seed and assessment source.
Do not automate the county DevNet assessment or tax sites and do not create a browser
flow for them. The ArcGIS source is a plain HTTPS JSON/GeoJSON API and does not require
a browser session.

The 2026-08-03 live count remained **65,956** parcels. The source returned all sampled
pages with HTTP 200 and no retries or failures. The benchmark requested 2,000 records
per page without geometry, using only `OBJECTID`, `PIN`, `class`, `Zoning`, and
`Jurisdiction`:

| Concurrency | Requests | Records | Failures |     Bytes |    p50 |    p95 | Wall time | Estimated 33-page time |
| ----------- | -------: | ------: | -------: | --------: | -----: | -----: | --------: | ---------------------: |
| 1           |        5 |  10,000 |        0 | 1,154,620 | 186 ms | 208 ms |    947 ms |                 6.25 s |
| 2           |        5 |  10,000 |        0 | 1,154,620 |  82 ms | 252 ms |    294 ms |                 1.94 s |
| 4           |        5 |  10,000 |        0 | 1,154,620 | 127 ms | 557 ms |    559 ms |                 3.69 s |

The current egress was US. Concurrency 2 was fastest and is the conservative source
setting. These timings are for narrow, non-geometry pages; the full seed/export
benchmark must separately report geometry payload size and elapsed time. Even with a
large safety factor, the 33-page source is nowhere near the 48-hour decision threshold.

The seed and appraisal artifacts should omit owner/tax-bill names and mailing fields.
Required source facts are PIN, site location, geometry, acreage, class/zoning,
jurisdiction, value components, sale/building facts, tax year, and provenance.

### 12.3 Jurisdiction denominator

The ArcGIS `Jurisdiction` field produced 16 non-null issuing areas plus 151 incomplete
rows. The largest parcel populations are Moline (17,486), Rock Island city (15,738),
unincorporated county (12,731), East Moline (7,834), Silvis (3,214), Milan (2,447),
and Coal Valley (1,549). The remaining incorporated jurisdictions each contain fewer
than 1,000 parcels. The full distribution is retained in
`docs/rock-island-sources.yaml`.

### 12.4 Permit-source catalog

Rock Island County does not have one countywide permit system:

- **Moline** links officially to CentralSquare eTRAKiT. The public landing page exposes
  `Search Permit`, but currently renders the generic greeting “Welcome to Central City”;
  tenant correctness and public search fields require review before adapter work.
- **Rock Island city** links officially to Tyler EnerGov / Civic Access. The city states
  that all permit applications and inspections use this portal. Public historical search
  behavior still needs a rendered-session probe.
- **East Moline** links officially to iWorQ. Its public page exposes `Search Existing
Permits`, status, messages, files, and payments.
- **Carbon Cliff** officially delegates plumbing, mechanical, building, and electrical
  permitting/inspection to East Moline, with prior Village Hall clearance.
- The county, Silvis, Coal Valley, Andalusia, Rapids City, Milan, Port Byron, Hampton,
  Cordova, Hillsdale, Reynolds, and Oak Grove expose forms, in-person workflows, or
  contact-only sources; no certifiable public historical permit portal was found.

The machine-readable catalog records all 16 jurisdictions. Forms/contact-only rows stay
`needs-review`; they require a records request, bulk export, or confirmed delegated
system before an adapter can be built. Do not infer a searchable API from a PDF
application page.

### 12.5 Illinois corporate registrations

Do not use Florida Sunbiz. The official source is the Illinois Secretary of State
Business Data Transparency Act bulk-data portal. It publishes daily fixed-width
corporation and LLC snapshots. Corporation data consists of seven linked files
(master, company name, agent, annual report, assumed/old names, stock, and other);
the file number is the stable join key. The interactive business search is explicitly
individual-use-only and must not be scraped.

The current Sunbiz worker and skill are Florida-specific and are not safe to repurpose
without a new Illinois fixed-width parser, county/address filter, lexicon mapping, and
tests. The official bulk files therefore remain a later adapter stage rather than a
live-search workaround.

### 12.6 Commercial-first gate

The 28 raw `class` values remain undocumented. Commercial/industrial ordering must not
be inferred from numeric codes. Seed generation may proceed in deterministic OBJECTID
order, but commercial-first queuing is blocked until an official class dictionary or a
validated zoning/use mapping is recorded.

### 12.7 Seed and appraisal-validation continuation

The PII-free seed now contains the pipeline-required `method`, `url`, and
`multiValueQueryString` columns in addition to source provenance. A complete rebuild
produced **65,806 unique canonical PIN rows** from **65,956** source records in
**19.123 seconds**, reading **101,759,335 bytes**. It quarantined **126** placeholder or
non-canonical PIN records and consolidated **24** extra records across **23** duplicate
PIN groups. Geometry remained lossless in the seed: **65,177 Polygon** and **629
MultiPolygon** rows.

A deterministic 25-PIN transform-validation sample was selected across raw class codes
and known edge cases. The ArcGIS multi-request flow captured all 25 in **7.28 seconds**
and **161,121 bytes**. The Rock Island scripts package transformed **25/25** captures in
a final **51.06-second** transform-plus-validation batch, and **25/25** passed Lexicon
validation with zero reported issues.

Local validation was restored without weakening TLS or schema integrity. Three configured
gateway names resolved locally to `192.168.4.1` and refused port 443; `w3s.link` resolved
publicly but was also refused. A new prefetch script used reachable Filebase/Pinata
gateways, verified exact bytes against each CID multihash, and followed only references
from verified parent schemas. It cached **65 schemas / 156,575 bytes** from the trusted
County root CID in **214.084 seconds**.

The transform now emits one geometry entity per Polygon/MultiPolygon component. The
sample produced real two- and three-component outputs. Exact GeoJSON, including interior
rings that the current flat-polygon Lexicon cannot normalize, is retained in
`data/source_payload.ndjson`; the query loader nests that sidecar into every logical
row's `source_payload`. The PII deny-list was expanded to fail closed on all owner and
tax-bill location/name fields.

Current-worker compatibility is no longer a design blocker: the worker and local root
both pin Elephant CLI `1.58.1` at commit `44fd046...`, and both use the tested
legacy `scriptsZip` contract. Transform v2 remains incompatible with multi-request
captures but is not invoked by this worker. The remaining appraisal gates are the
unavailable class dictionary, AWS transform-bundle sync/hash verification plus one-parcel
worker smoke, and source-to-Neon/export reconciliation.

The existing Neon database was reached read-only, its `parcels` table exists, and the
`rock_island_appraiser` namespace currently contains **0 parcels**. AWS remains blocked
because the requested local profile is absent. The exact field audit and executable
validation evidence are in
`docs/rock-island-appraisal-transform-validation.md`.

Permit source recertification passed the four public portals already identified
(Moline, Rock Island, East Moline, and Carbon Cliff); the other 12 jurisdiction rows
remain skipped as manual or unknown sources. No permit adapter or crawl was started
because the AWS one-parcel appraisal pilot and upstream reconciliation gates have not
passed.
