# Rock Island appraisal transform validation

Date: 2026-08-03
County key: `rock-island`
FIPS: `17161`

## Result

The PII-free ArcGIS appraisal capture and transform now passes the complete local
Lexicon validation gate. No county crawl should start yet because AWS access,
commercial-first class semantics, and source-to-Neon/export reconciliation remain open.

- Fresh prepare captures: **25/25**
- Capture archive: **161,121 bytes**
- Capture wall time: **7.28 seconds** for 25 sequential requests
- County transforms: **25/25 succeeded**
- Final transform-plus-validation wall time: **51.06 seconds** for 25 parcels
- CLI Lexicon validations: **25/25 passed**
- Validation issues: **0** across all 25 regenerated outputs
- Geometry output: one schema-valid geometry entity per Polygon/MultiPolygon component
- Provenance output: one `data/source_payload.ndjson` sidecar per artifact

The original failure was host-specific local egress, not TLS or missing content:
`ipfs.io`, `gateway.ipfs.io`, and `dweb.link` resolved locally to `192.168.4.1` and
refused port 443; `w3s.link` resolved to public Cloudflare addresses but was also
refused. TLS verification was never disabled.

Validation was restored with `scripts/prefetch-elephant-schema-cache.mjs`. It fetched
from reachable public Filebase/Pinata gateways, verified each exact response against its
CID multihash before writing, and followed child CIDs only from already verified parent
schemas. Root CID
`bafkreicdfrzfiygzjaqrz4i2ao4yxspcxsksvbuljxx7ruqrp5m36kddxq` anchored **65**
verified schemas totaling **156,575 bytes** in
`~/.elephant-cli/schema-cache`. Cache population took **214.084 seconds**.

Durable local evidence:

- `downloads/rock-island/rock-island-validation-sample.csv`
- `downloads/rock-island/rock-island-validation-captures.zip`
- `downloads/rock-island/appraisal-validation/summary.json`
- `downloads/rock-island/appraisal-validation/<PIN>.zip`
- `downloads/rock-island/appraisal-validation/<PIN>-validation.csv`

## Variability covered

The deterministic sample covers 25 canonical PINs selected without interpreting the
county class codes. It includes:

- raw class codes `0000`, `0010`, `0011`, `0020`, `0021`, `0026`, `0028`, `0029`,
  `0030`, `0032`, `0040`, `0050`, `0052`, `0060`, `0062`, `0065`, `0070`, `0080`,
  `0081`, `0082`, `0085`, and `0090`
- a blank class
- blank zoning
- a consolidated duplicate PIN
- missing site addresses
- incomplete assessment values
- records with and without a recorded structure

## Source-to-output coverage

Mapped directly or by documented arithmetic:

- `PIN` → `property.parcel_identifier` and `parcel.parcel_identifier`
- `site_address`, `Site_City`, `Site_State`, `Site_Zip` → site
  `address.unnormalized_address`
- `X_longitude`, `Y_latitude` → site-address coordinates
- `GIS_acres_num`, with `gross_acres` fallback → `lot.lot_size_acre`
- acreage × 43,560 → `lot.lot_area_sqft`
- `EAV` → `tax.property_assessed_value_amount`
- `EMV` → `tax.property_market_value_amount`
- farm plus non-farm land values → `tax.property_land_amount`
- farm plus non-farm building values → `tax.property_building_amount`
- `taxbill_year` → `tax.tax_year`
- `legal` → `property.property_legal_description_text`
- `YRBuilt` → `property.property_structure_built_year`
- `Zoning` → `property.zoning`
- latest available sale date and positive net/gross sale price → `sales_history`

Intentionally not reinterpreted:

- `class`: the 28-value county dictionary remains unavailable, so
  `property_usage_type` is `Unknown`. Commercial/industrial ordering remains blocked.
- `MODLNAME`: source semantics are undocumented.
- `TWP_RAN_SE`, township, jurisdiction, and tax code: no approved current lexicon home.
- `RICO_PARCE`, parcel-number aliases, alternate parcel number, and `municipality`: there
  is no approved graph model; exact source values are retained in `source_payload`.
- `assessed_last`, `GarSQFT`, `Shape__Area`, and `Shape__Length`: retained in
  `source_payload` but not mapped to a potentially misleading field.

The complete PII-free ArcGIS response now survives in
`data/source_payload.ndjson`. The query loader nests that verified sidecar in each
logical row's `source_payload`, while retaining the transformed entity beside it.

## Geometry handling

The source and seed preserve all Polygon and MultiPolygon WGS84 coordinates; **629**
seed rows are MultiPolygon. The transform emits one geometry vertex and parcel
relationship per polygon component. The 25-record proof includes actual outputs with
two and three geometry components, all schema-valid.

The current geometry lexicon has no interior-ring field. Exact GeoJSON, including every
component and interior ring, is therefore retained losslessly in the query-loader
`source_payload` sidecar. A future normalized spatial column may project that topology,
but the source bytes are not discarded.

## Runtime compatibility

The current worker intentionally calls `transform({ scriptsZip })`; it does not request
transform v2. Both the worker package and the locally tested root dependency pin
Elephant CLI `1.58.1` at commit `44fd046cfdc205a223cc7a62df5bfcaf003a395b`.
The Rock Island package uses this exact legacy scripts-ZIP contract, so current-worker
runtime compatibility is proven locally. Transform v2 still cannot consume this
multi-request capture format, but it is not the runtime used by this worker.

The remaining deployment gate is operational: after AWS credentials are restored, sync
the tested Rock Island ZIP to the county-specific S3 transform key and verify its hash
before a one-parcel smoke test.

## Source-load estimate

The 25-request prepare benchmark averaged about **0.291 seconds per parcel** in sequential
CLI mode. A naïve 65,806-parcel sequential projection is about **5.3 hours**; concurrency
2 projects about **2.7 hours** before retry and orchestration overhead. This is under the
48-hour decision threshold, but no full crawl should run because:

- all appraisal facts are already present in the one-shot bulk FeatureServer seed
  download
- source-to-Neon and non-PII export reconciliation has not run
- commercial-first ordering is blocked on class-code semantics

## Related readiness

- Seed: **65,806 unique valid PINs** from **65,956** source records.
- Quarantine: **126** non-canonical placeholder PIN records.
- Duplicate consolidation: **23 groups**, **24 extra records**.
- Seed rebuild: **19.123 seconds**, **101,759,335 response bytes**.
- Neon: connection and `public.parcels` table verified; Rock Island namespace currently
  contains **0 parcels**.
- AWS: blocked because local profile `elephant-oracle-node` is not configured.
- Permit catalog: 4 of 16 jurisdictions certify as reachable public portals; 12 remain
  manual/unknown and skipped for review.
