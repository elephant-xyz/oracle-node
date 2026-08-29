# Broward real-property tax-roll onboarding

Date: 2026-08-29  
County: Broward County, Florida (`12011`; DOR county `16`)  
Scope: NAL real property only. NAP tangible personal property is excluded.

## Authoritative source

Florida Department of Revenue Property Tax Oversight (PTO) publishes assessment
rolls under chapter 119, Florida Statutes. The preferred available certified
record is:

- DOR delivery file: `2025F.zip`
- CSV member: `2025F\NAL_2025_16Broward_F.csv`
- URL:
  `https://floridarevenue.com/property/dataportal/Documents/PTO%20Data%20Portal/~Public%20Records/~20260825-1363805/NAL/2025F.zip`
- Roll year: 2025
- Certification status: **initial final / first certification**, not proven
  post-VAB final-certified
- First certification executed by BCPA: `2025-10-20`
- DOR delivery modified timestamp: `2026-08-26T11:56:17Z`
- Retrieved: `2026-08-29T23:38:05Z`
- ZIP bytes: `57,652,950`
- ZIP SHA-256:
  `3f23aa020b21be845afcd3ddec887a1ad7b951885ed5155080d06674906b81fc`
- CSV bytes: `354,640,193`
- CSV SHA-256:
  `b424e1718bdeccc9e1894dc182483fc914ccd25c7d3af494563766fda5cf3ec1`

The raw 2025 final and 2026 preliminary ZIPs, extracted CSVs, User's Guides, and
Quick Reference are
currently retained privately under `downloads/broward/tax-roll/source/`. They
are ignored repository data and are not publication artifacts.

DOR's official schedule defines July submissions as preliminary, October
submissions as initial final, and the post-VAB submission as final-certified.
DOR's normal `NAL/2025F` folder has rotated out, but its official temporary
public-record delivery remains anonymously downloadable. BCPA's published
first-certification form supports `initial_final`; its DR-403 says VAB hearings
were not complete, and no second-certification/post-VAB recap is published.
Because DOR uses `F` for both stages, this file must not be labeled post-VAB
final-certified. A later post-VAB or 2026 certified file may supersede it only
through the checksum- and status-aware precedence rule below. The separately
profiled 2026 preliminary is retained for comparison, not used as the canonical
load denominator.

Official documentation:

- Download and schedule:
  `https://floridarevenue.com/property/Pages/DataPortal_RequestAssessmentRollGISData.aspx`
- Public-record request:
  `https://floridarevenue.com/property/Pages/Public_Record_Request.aspx`
- BCPA 2025 tax-roll information:
  `https://bcpa.net/2025TaxRollInfo.asp`
- BCPA first certification:
  `https://bcpa.net/Includes/Downloads/2025/2025%20Tax%20Roll%20Certification%20%28First%29.pdf`
- 2026 User's Guide:
  `https://floridarevenue.com/property/dataportal/Documents/PTO%20Data%20Portal/User%20Guides/2026%20Users%20guide%20and%20quick%20reference/2026_NAL_SDF_NAP_Users_Guide.pdf`

DOR states that the public files omit confidential records, including Social
Security numbers and records of owners exempt from disclosure under
section 119.071, Florida Statutes. The loader uses only the distributed public
file and does not reconstruct redacted values. BCPA bulk commercial access is
fee/contract based and is not used.

## Actual source profile

The downloaded source, not the report-page estimate, is the denominator:

| Measure | Count |
| --- | ---: |
| NAL source rows | 754,061 |
| Unique non-empty valid parcel IDs | 754,061 |
| Duplicate parcel IDs / extra duplicate rows | 0 / 0 |
| Missing parcel IDs | 0 |
| Malformed parcel IDs | 0 |
| 12-character parcel IDs | 754,061 |
| Condominium rows (`DOR_UC=004`) | 253,185 |
| Residential (`000-009`) | 695,249 |
| Commercial/industrial (`010-049`) | 33,306 |
| Agricultural (`050-069`) | 1,123 |
| Institutional/government/utility (`070-098`) | 24,379 |
| Non-agricultural acreage (`099`) | 4 |
| Rows with packed exemption code/value pairs | 447,738 |

All rows have `CO_NO=16`, `FILE_T=R`, and `ASMNT_YR=2025`. The separately
profiled 2026 preliminary has 754,549 rows and 253,770 condominiums. Year/stage
differences are source-version changes, not load failures.

Reproduce the aggregate profile:

```bash
npm run broward:tax-roll:profile -- \
  --nal-csv "downloads/broward/tax-roll/source/NAL_2025_16Broward_F.csv" \
  --gis-seed "downloads/broward/broward.csv" \
  --source-zip "downloads/broward/tax-roll/source/2025F.zip" \
  --source-url "https://floridarevenue.com/property/dataportal/Documents/PTO%20Data%20Portal/~Public%20Records/~20260825-1363805/NAL/2025F.zip" \
  --roll-year 2025 \
  --certification-status initial_final \
  --retrieved-at "2026-08-29T23:38:05Z" \
  --output "downloads/broward/tax-roll/profile-2025-initial-final.json" \
  --pilot-csv "downloads/broward/tax-roll/pilot/nal-pilot.private.csv" \
  --pilot-manifest "downloads/broward/tax-roll/pilot/manifest.private.json"
```

## Exact GIS join and reconciliation

The join is exact string equality:

```text
trim(NAL.PARCEL_ID) = GIS.FOLIO = generated seed parcel_id
```

Identifiers are uppercase 12-character alphanumeric strings. They are never
parsed as numbers, so leading zeros and condominium letters are retained.
Addresses are not keys.

| Measure | Count |
| --- | ---: |
| Official GIS polygon features | 556,178 |
| Distinct valid GIS folios | 534,309 |
| NAL IDs matched to GIS folios | 521,663 |
| NAL-only IDs | 232,398 |
| GIS-only folios | 12,646 |
| Condominium rows matched to GIS | 41,010 |
| Condominium rows without GIS folio | 212,175 |
| Union of tax-roll IDs and GIS folios | 766,707 |
| Unexplained NAL row difference | 0 |

The NAL row is the canonical 2025 assessed-property population. Existing GIS
properties retain their geometry. NAL-only units are valid canonical properties
with null geometry. No polygon is copied from a parent parcel or another
condominium unit. GIS-only properties remain supplemental records but do not
increase the tax-roll coverage denominator.

## Query-db and Lexicon mapping

The query-db patch is
`docs/patches/elephant-query-db-broward-tax-roll.patch` (SHA-256
`9650ec6171b54a10aff8ad07bb0e59444a63fb3b3d8e27aa6fc851f508e2c217`).
Apply it after `elephant-query-db-broward-local-loader.patch`. It adds a
streaming, checkpointed NAL loader and query tables for Lexicon
`tax_exemption` and `tax_authority`.

| NAL data | Query-db / Lexicon mapping |
| --- | --- |
| `PARCEL_ID` | canonical exact `request_identifier` / `parcel_identifier` |
| `DOR_UC`, `PA_UC` | broad validated property classification; raw codes retained |
| `JV`, `AV_NSD`, `TV_NSD`, `TV_SD` | market, assessed, non-school/county taxable, school taxable |
| `LND_VAL`, `JV-LND_VAL` | land and explicitly derived building/other value |
| `OWN_NAME` | `ownership.owned_by`; not guessed as a person or company |
| owner mailing fields | source-specific mailing address |
| physical address fields | source-specific situs address |
| `S_LEGAL` | property legal description for NAL-only property stubs |
| `ACT_YR_BLT`, `EFF_YR_BLT`, area/unit fields | existing property fields for NAL-only stubs |
| `EXMPT_*` or packed `EXEMPTIONS` pairs | one `tax_exemption` per positive reported code/value |
| `TAX_AUTH_CD` | `tax_authority.authority_account_identifier` |
| two NAL sale slots | price, qualification code, deed book/page, instrument |
| roll metadata | source checksum, URL, year, status, retrieval time, raw row payload |

The NAL exposes only sale year/month, so the loader does not invent a day or
populate `ownership_transfer_date`. Sale precision remains in the source
payload. It does not collapse exemption fields into one amount because their
school/county/municipal applicability overlaps. `PA_UC`, statistical strata,
special assessments, classified-use components, portability fields, sale
change codes, and data-management identifiers remain in the raw source payload
when no exact query/Lexicon field exists.

The pilot also exposed a Lexicon contract gap: the current `tax` schema requires
`monthly_tax_amount`, while NAL reports assessed/taxable values but no tax bill
or millage-derived monthly amount. The mapper deliberately leaves that field
absent rather than inventing a tax payment. Query-db mapping and all 20 pilot
rows are valid, but end-to-end Elephant CLI Lexicon validation cannot pass
until the Lexicon makes that payment field optional or adds an assessment-roll
class. The same validation found and fixed `number_of_units_type` to use the
existing `One`/`Two`/`Three`/`Four` vocabulary. Unknown-format warnings for the
existing percentage format are unchanged from prior county validation.

### Precedence and idempotence

- Existing `broward_appraiser` properties are reused by exact folio source key;
  the NAL loader does not create a second matched property.
- NAL-only rows create `florida_dor_nal` parcel/property stubs with null
  geometry.
- DOR NAL is authoritative for the same property's tax-year value row.
- A final-certified NAL may replace preliminary/initial-final data.
- Preliminary or county-live data cannot overwrite a final-certified NAL.
- County-live structural/address facts remain preferred where an existing
  BCPA property is present; NAL source-specific addresses and ownership remain
  separately provenance-tracked.
- Every logical row has a deterministic source key/hash.
- Full chunks commit transactionally and checkpoint in
  `ingest_control.florida_nal_chunks`; source checksum and contiguous row ranges
  control resume.
- Full loading refuses to run until the GIS status is `complete`, all 534,309
  GIS outcomes are durable, and the GIS writer lock is free.

## Pilot and full-load gates

The private pilot selector covers:

- normal GIS-matched residential parcels;
- NAL-only condominium units with null geometry;
- commercial/industrial records;
- vacant records;
- GIS-only controls that must remain unchanged.

The official NAL has no malformed IDs. Synthetic malformed, short, numeric, and
leading-zero cases are covered by unit tests rather than altering the source.
The live Neon pilot and migration are intentionally deferred until the current
GIS writer completes; running them now could create a DOR stub while the writer
is about to create the same canonical property.

After GIS completion:

1. Apply query-db migration `0010_florida_nal_tax_roll.sql` through the verified
   direct Neon URL.
2. Resolve the `tax.monthly_tax_amount` Lexicon gap, then validate the private
   representative pilot through Lexicon; query-db dry-run already passes.
3. Run the Neon pilot and reconcile existing geometry vs null geometry.
4. Run the full checksum-bound loader.
5. Reconcile 754,061 NAL IDs, 534,309 GIS folios, and the 766,707 union.
6. Set dataset coverage denominator to the actual 754,061 NAL IDs.
7. Refresh query/coverage artifacts; verify IPFS/IPNS and MCP readback.
8. Only then update the published county catalog.

No tax-roll load or publication runs concurrently with the active GIS writer.
