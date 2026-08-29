# Broward real-property tax-roll onboarding

Date: 2026-08-29  
County: Broward County, Florida (`12011`; DOR county `16`)  
Scope: NAL real property only. NAP tangible personal property is excluded.

## Authoritative source

Florida Department of Revenue Property Tax Oversight (PTO) publishes assessment
rolls under chapter 119, Florida Statutes. The current anonymously downloadable
record is:

- File: `Broward Preliminary NAL 2026.zip`
- CSV member: `NAL16P202601.csv`
- URL:
  `https://floridarevenue.com/property/dataportal/Documents/PTO%20Data%20Portal/Tax%20Roll%20Data%20Files/NAL/2026P/Broward%20Preliminary%20NAL%202026.zip`
- Roll year: 2026
- Certification status: **preliminary**, not final-certified
- DOR modified timestamp: `2026-07-27T11:05:36Z`
- Retrieved: `2026-08-29T22:49:05Z`
- ZIP bytes: `50,460,442`
- ZIP SHA-256:
  `a60bc20f212716fe4d22673d14dc630ebb959ee13c1ed474b7451b36b2c94d9f`
- CSV bytes: `387,723,357`
- CSV SHA-256:
  `5d143ea7bb3d563681dee30fc85a559cddc0c2c5b96a3027ea18ba4800f90760`

The raw ZIP, extracted CSV, 2026 User's Guide, and 2026 Quick Reference are
currently retained privately under `downloads/broward/tax-roll/source/`. They
are ignored repository data and are not publication artifacts.

DOR's official schedule defines July submissions as preliminary, October
submissions as initial final, and the post-VAB submission as final-certified.
DOR publishes only the current version of each roll type. The `NAL/2025F`
folder no longer exposes Broward's prior final file; prior versions must be
requested through DOR's official Assessment Roll Data Request form. Therefore
the acquired source is explicitly treated as 2026 preliminary. It must never be
labeled certified. A later 2026 initial-final or final-certified file may
supersede it only through the checksum- and status-aware precedence rule below.

Official documentation:

- Download and schedule:
  `https://floridarevenue.com/property/Pages/DataPortal_RequestAssessmentRollGISData.aspx`
- Public-record request:
  `https://floridarevenue.com/property/Pages/Public_Record_Request.aspx`
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
| NAL source rows | 754,549 |
| Unique non-empty valid parcel IDs | 754,549 |
| Duplicate parcel IDs / extra duplicate rows | 0 / 0 |
| Missing parcel IDs | 0 |
| Malformed parcel IDs | 0 |
| 12-character parcel IDs | 754,549 |
| Condominium rows (`DOR_UC=004`) | 253,770 |
| Residential (`000-009`) | 695,870 |
| Commercial/industrial (`010-049`) | 33,051 |
| Agricultural (`050-069`) | 1,237 |
| Institutional/government/utility (`070-098`) | 24,387 |
| Non-agricultural acreage (`099`) | 4 |

All rows have `CO_NO=16`, `FILE_T=R`, and `ASMNT_YR=2026`. The 754,549
actual rows are 3,598 below the earlier 758,147 preliminary summary figure, and
the 253,770 actual condominium rows are 311 below its 254,081 figure. Those
differences are treated as source-version changes, not load failures.

Reproduce the aggregate profile:

```bash
npm run broward:tax-roll:profile -- \
  --nal-csv "downloads/broward/tax-roll/source/NAL16P202601.csv" \
  --gis-seed "downloads/broward/broward.csv" \
  --source-zip "downloads/broward/tax-roll/source/Broward Preliminary NAL 2026.zip" \
  --source-url "https://floridarevenue.com/property/dataportal/Documents/PTO%20Data%20Portal/Tax%20Roll%20Data%20Files/NAL/2026P/Broward%20Preliminary%20NAL%202026.zip" \
  --roll-year 2026 \
  --certification-status preliminary \
  --retrieved-at "2026-08-29T22:49:05Z" \
  --output "downloads/broward/tax-roll/profile-2026-preliminary.json" \
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
| NAL IDs matched to GIS folios | 522,508 |
| NAL-only IDs | 232,041 |
| GIS-only folios | 11,801 |
| Condominium rows matched to GIS | 41,016 |
| Condominium rows without GIS folio | 212,754 |
| Union of tax-roll IDs and GIS folios | 766,350 |
| Unexplained NAL row difference | 0 |

The NAL row is the canonical 2026 assessed-property population. Existing GIS
properties retain their geometry. NAL-only units are valid canonical properties
with null geometry. No polygon is copied from a parent parcel or another
condominium unit. GIS-only properties remain supplemental records but do not
increase the tax-roll coverage denominator.

## Query-db and Lexicon mapping

The query-db patch is
`docs/patches/elephant-query-db-broward-tax-roll.patch` (SHA-256
`a911edf7e6f738840d1b6aea01925fef375d62458920c6c6d3648c1335201bdf`).
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
| `EXMPT_*` | one `tax_exemption` per positive reported code/value |
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
2. Validate the private representative pilot through Lexicon and query-db
   dry-run.
3. Run the Neon pilot and reconcile existing geometry vs null geometry.
4. Run the full checksum-bound loader.
5. Reconcile 754,549 NAL IDs, 534,309 GIS folios, and the 766,350 union.
6. Set dataset coverage denominator to the actual 754,549 NAL IDs.
7. Refresh query/coverage artifacts; verify IPFS/IPNS and MCP readback.
8. Only then update the published county catalog.

No tax-roll load or publication runs concurrently with the active GIS writer.
