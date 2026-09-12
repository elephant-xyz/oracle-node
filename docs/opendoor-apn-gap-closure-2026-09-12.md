# OpenDoor APN gap closure — 2026-09-12

## Scope and safety contract

- Input: 18,225 OpenDoor Florida rows from the 2026-09-10 source export.
- Frozen gap cohort: 527 rows with APNs and 13 rows without APNs.
- Only exact county-local APN/parcel matches are eligible.
- Existing non-null identities are never overwritten.
- Shared, ambiguous, or already-occupied parcel targets are quarantined.
- Existing HOA/property-manager fields are preserved. New rows are enriched when an official subdivision and the reviewed Sunbiz index support a match.

## Published and verified

| County       | Closed APN rows | Published base CID                               |
| ------------ | --------------: | ------------------------------------------------ |
| Hillsborough |             336 | `QmaNScYyp7gEcNQPnkDqLjkZyb2yHFBgpnC8pFcAva7buR` |
| Pinellas     |              78 | `QmbmJy9wczN99qQPqV7XQGb4KfCTSUJA6B4ArtdssRN3Mo` |
| Pasco        |              31 | `QmPxigjpuUN3mn5HivjDmo2eWSU13DVqomgf4E4pTEU9Fm` |
| Orange       |              22 | `QmXeTNghbbo5hd1FgqeXPoCJDxtp6pkygJS12t6krnCFFa` |
| Osceola      |              14 | `QmQMoMkBStLDGid7rwqGUZovcfwSt3cYAWUQyWwZu4k4gE` |
| Broward      |              12 | `QmdD3f3NqgNRiFn4hpGqEwpNqw5tXK3FWW2QRoRBYddhX5` |
| Duval        |               9 | `QmVBXXV3UKuCHE7QCTzT3o6ULaFj7XJL1f1Tzrk8DsLsvD` |
| Nassau       |               4 | `QmdjszjPcVjt3KepeDyE37JfdBx2TaidTsy6Qwem4nf9N6` |
| Palm Beach   |               3 | `QmQ1gzMkF7Af3S6DpYKt17LDxsjd8jApN8snNTFxRVfSTg` |
| Clay         |               1 | `Qmd9r8oU6SJzEYmb8EoBoGrrwP3RDc4uj6w1XTkC5X4pfo` |
| Lee          |               1 | `QmXUhFPnWxwywQaksFW1ikSAoMCgmjDrHfZvHPnCRuFtZh` |
| Columbia     |               1 | `QmVd5YxqhwyJHRzzamSbGv4ertMjPL5V2kEjMrtGTwqH57` |
| Sarasota     |               1 | `QmfL1XDLG8NhvB8QH9WftjSeksJ3TGWJh7qLZDCadmH4mx` |

Current verified checkpoint:

- Closed APN-backed rows: 515 of 527.
- Latest OpenDoor source overlap across the union of all Florida MCP routes: 18,200 of 18,225 (99.863%).
- Remaining gaps: 25 total; 12 APN-backed and 13 without usable APNs.
- Four apparent county gaps are already available under alternate MCP routes: Charlotte via Sarasota, Citrus via Marion, Flagler via Volusia, and Sumter via Lake.
- Exact fresh-process MCP checks passed for each published UUID/token pair.
- A Pasco token mismatch was caught by post-publication validation, corrected, republished, and then verified 31/31.

The Duval Deerfoot row was also closed after the authoritative National Address List mailing address exactly matched `7156 DEERFOOT POINT CIR UNIT 3` to current Duval RE `1525625734`; the appraiser situs records the same condominium as unit `6-3`. The target identity was null before assignment. It was published with `hoa_pm_status=no_sunbiz_hoa`.

- OpenDoor UUID: `b72e3903-b48c-5d79-a35c-e56f0f7f5acf`.
- Base CID: `QmVBXXV3UKuCHE7QCTzT3o6ULaFj7XJL1f1Tzrk8DsLsvD`.
- HOA/PM CID: `QmVrF1SNmn83uke67Qj1msggLt1cGBBwogXQCAJtZaFQUD`.
- Coverage CID: `QmXKwxHyj9c8JmqH5G1PZQQwFT4CvbmCkM5EZXPdU4pjGs`.

Evidence: `data/artifacts/opendoor-apn-gap-closure/latest-mcp-global-coverage-2026-09-12.json` and `data/artifacts/opendoor-apn-gap-closure/address-review/`.

## Source results

| County       | Rows obtained | Source and route                                                        | Result                                                                                                                                        |
| ------------ | ------------: | ----------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| Hillsborough |           335 | `HC_ParcelsPublic` FeatureServer through Railway                        | 325 existing rows updated; 10 current official parcels appended                                                                               |
| Pinellas     |            78 | Pinellas PublicWebGIS `MapServer/1`, local                              | 78/78 unique STRAPs; used because PCPAO print is blocked                                                                                      |
| Orange       |            21 | OCPA API through Railway                                                | 20 existing rows updated; one current parcel appended                                                                                         |
| Osceola      |            14 | Osceola Property Appraiser OData, local                                 | 14/14 exact parcels                                                                                                                           |
| Nassau       |             4 | Nassau public tax-map ArcGIS, local                                     | 4/4 exact parcels                                                                                                                             |
| Sarasota     |             1 | Sarasota Property Appraiser page, local                                 | 1/1 exact parcel                                                                                                                              |
| Columbia     |             1 | Columbia Grizzly GIS, local                                             | 1/1 exact parcel                                                                                                                              |
| Pasco        |  1 additional | Pasco Property Appraiser, local                                         | County-formatted APN and situs resolved to one null target                                                                                    |
| Broward      |            12 | BCPA `search.aspx/GetData` and `getParcelInformation` through AWS Batch | 12/12 exact folios and matching situs; 12 rows appended                                                                                       |
| Duval        |  8 additional | COJ Property Appraiser detail pages through AWS Batch                   | 8/8 exact RE numbers and matching situs; 8 rows appended                                                                                      |
| Palm Beach   |  2 additional | PBCPAO detail pages through AWS Batch                                   | 2/2 exact PCNs and matching situs; 2 rows appended                                                                                            |
| Clay         |             1 | Florida DOR statewide cadastral FeatureServer                           | Exact Clay PA parcel and situs for `41-04-26-018793-071-00`; one row appended                                                                 |
| Lee          |             1 | Florida DOR statewide cadastral FeatureServer                           | Exact alphanumeric parcel `364523350000C0206` and situs; corrected the county table's letter-stripped identifier and assigned one null target |
| Orange       |  1 additional | Florida DOR statewide cadastral FeatureServer                           | Exact 2025 Orange parcel `172128088001440` and 728 Longford Loop situs; one row appended                                                      |

Each automatic assignment has one authoritative parcel, an exact or documented county-specific APN transformation, matching situs evidence, a null target identity, and the frozen OpenDoor UUID/token pair.

## HOA/property-manager coverage

The updated identity subsets were evaluated against the verified statewide 2026 Q3 Sunbiz source. Existing published associations were checked for regressions before replacement.

The full 18,225-row OpenDoor cohort was also audited through every dedicated `*-hoa-pm` MCP route:

- 15,474 rows have been evaluated (84.905%).
- 1,944 rows have a published HOA CID (10.667% of all rows; 12.563% of evaluated rows).
- 628 rows have both an HOA CID and a property-manager CID (3.446% of all rows; 4.058% of evaluated rows).
- 2,751 rows remain unassessed and must not be interpreted as negative HOA results.
- The 13,530 evaluated rows without linked CIDs have explicit pipeline outcomes such as no subdivision, no Sunbiz HOA, non-unique HOA, or unresolved registered-agent company.

Audit summary: `data/artifacts/opendoor-apn-gap-closure/latest-mcp-hoa-pm-coverage-2026-09-12.json`. The full 18,225-row export is `opendoor-hoa-pm-coverage-2026-09-12.csv` in the analysis workspace.

| County cohort            | Rows | Resolvable HOA rows | Resolvable PM rows |
| ------------------------ | ---: | ------------------: | -----------------: |
| Hillsborough closed gaps |  336 |                  67 |                 16 |
| Pasco closed gaps        |   31 |                   6 |                  1 |
| Orange newly closed      |   21 |                   2 |                  0 |
| Osceola newly closed     |   14 |                   1 |                  0 |
| Pinellas newly closed    |   78 |                   1 |                  0 |
| Broward AWS-closed       |   12 |                   1 |                  0 |
| Duval AWS-closed         |    8 |                   1 |                  1 |
| Palm Beach AWS-closed    |    2 |                   0 |                  0 |
| Clay DOR-closed          |    1 |                   0 |                  0 |
| Lee DOR-closed           |    1 |                   1 |                  0 |
| Orange DOR-closed        |    1 |                   0 |                  0 |

Nassau, Sarasota, and Columbia produced no safe new association: the outcomes were no subdivision, no Sunbiz HOA, or non-unique. No ambiguous match was published.

The newly closed rows were evaluated with complete two-pass scans of all 12,808,196 records in the verified statewide 2026 Q3 Sunbiz archive. Existing HOA/PM overlay rows were preserved byte-for-byte before the new rows were appended. Published overlay CIDs are Broward `QmQ14o8m9LVCk6Wgkk6fB7iCnjcGKHQo3TshtLGesPpYk8`, Duval `QmVrF1SNmn83uke67Qj1msggLt1cGBBwogXQCAJtZaFQUD`, Palm Beach `QmcduEc8TrmXfev4Y2Vn6zFjSPT6mxMV6kMgiMceocinQa`, Clay `QmaiKW68swEjXU6agFt1Qnu4JayyKQFoZoG3V4tmGR79UN`, Lee `QmeJrWHFixTf3tYY1Q2WJkvN2KBtbC72NVkkorj9hierH3`, and Orange `QmcqYSHM8h5bHNPX3nMKx1gMsGoYD3947MFnXteqWcZR4K`.

## Access findings

- Railway CLI/SSH is authenticated against the existing `prism-duval-oracle` project.
- PCPAO print pages return HTTP 403 from the local machine, `duval-pilot-run`, and `coj-egress-probe`.
- Pinellas PublicWebGIS is an official working alternate and resolved all 78 rows locally.
- Hillsborough's county ArcGIS service timed out locally but resolved 335/336 rows through Railway.
- Orange's official API succeeded in local probes, began returning HTTP 403 during the batch, and then resolved 21/29 rows through Railway.
- The Florida statewide cadastral endpoint timed out during earlier broad probes from local and Railway, but a later exact `PARCELNO` query succeeded and returned the missing Clay parcel. Treat broad-query timeouts as service availability, not an IP ban.
- Osceola, Nassau, Sarasota, Columbia, Pasco, and Pinellas PublicWebGIS sources are locally accessible.
- Railway MCP live discovery/authentication timed out; Railway CLI/SSH remains usable.
- AWS SSO and the existing `opendoor-lake-appraisal-dev` Fargate Batch runtime are working in account `282516654782`.
- AWS returned HTTP 200 for PCPAO, Hillsborough GIS, OCPA, BCPA, Palm Beach, Miami-Dade, Duval, and LeePA. Clay qPublic still returned a Cloudflare HTTP 403.
- BCPA's GIS joined-attribute queries timed out, but its official `search.aspx` JSON methods resolved all 12 Broward folios exactly.
- One of the eight Orange APNs was recovered from a populated 2025 DOR cadastral record with exact situs. The other seven return HTTP 200 with an empty OCPA result; their DOR features are zero-attribution parcel shells or absent, so they remain source-record exceptions rather than IP blocks.
- Both Miami-Dade addresses validate to the supplied folios in the county address service, but the live Property Appraiser returns `Folio not found` for both.
- Lee's source table had dropped the `C` from multiple alphanumeric condominium STRAPs. The exact DOR parcel and situs proved that `364523350000C0206` maps uniquely to 15989 Mandolin Bay Drive unit 206.
- Full-cohort verification rejected a divergent Lee live-IPNS table that would have lost 27 existing OpenDoor identities. The final publication was rebuilt from the catalog-frozen CID, preserved all 32 prior Lee identity pairs, and added the verified row.

## Current delegation and exception queue

The row-level Joaquin handoff is in `docs/opendoor-apn-gap-delegation-2026-09-12.md`.
The complete 25-row shareable queue is `docs/opendoor-mcp-missing-rows-2026-09-12.csv`.

| County       | Rows | Current evidence / required action                                                                                                                              |
| ------------ | ---: | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Clay         |    1 | Occupied-target conflict; the parcel already carries a different OpenDoor identity                                                                              |
| Duval        |    2 | One official ZIP conflict and one source-address/coordinate conflict; both require source correction or explicit adjudication                                   |
| Hillsborough |    1 | Source folio is absent; the situs now resolves to a different official folio and requires a historical crosswalk                                                |
| Miami-Dade   |    2 | County address service validates each address/folio pair, but the live Property Appraiser reports both folios absent; historical/inactive-folio review required |
| Orange       |   10 | Seven historical/current APNs remain unresolved; three no-APN rows need a parcel or condominium unit crosswalk                                                  |
| Pasco        |    1 | Exact address is absent from the current table; obtain an authoritative current or historical parcel                                                            |
| Pinellas     |    3 | Missing or malformed condominium/source identities; obtain exact parcel and unit evidence                                                                       |
| Polk         |    3 | One occupied-target conflict and two malformed source addresses sharing coordinates                                                                             |
| Seminole     |    1 | Official parcel is `361 Goldstone Ct`, while the source identity is `361 Goldstone Pl`; source correction is required                                           |
| St. Johns    |    1 | Exact condominium unit APN is missing                                                                                                                           |

The two occupied targets must never be overwritten:

- Clay APN `38-06-26-018409-003-00`.
- Polk APN `272817828700000230`.

Do not delegate counties merely because the statewide cadastral endpoint is unavailable. First test the county's authoritative source locally, then from Railway only when local access returns an explicit denial/challenge.

The final Joaquin packet must include, per county:

1. APN-backed row count and frozen cohort file.
2. Exact official source URL and parcel-key transformation.
3. Local and Railway HTTP evidence.
4. Expected output schema and UUID/token mapping.
5. HOA/PM requirement and available subdivision evidence.
6. Acceptance checks: unique APN, matching situs, no occupied identity, unchanged base rows, exact new identity count.

Counties are added here only after the available local, Railway, or AWS paths have been tested and no safe official alternate source remains.

AWS evidence and publication receipts:

- `data/artifacts/opendoor-apn-gap-closure/aws-safe-resolution-audit.csv`
- `data/artifacts/opendoor-apn-gap-closure/aws-safe-resolution-report.json`
- `data/artifacts/opendoor-apn-gap-closure/aws-safe-hoa-pm-merge-report.json`
- `data/artifacts/opendoor-apn-gap-closure/aws-safe-publication-receipt.json`
- `data/artifacts/opendoor-apn-gap-closure/aws-safe-targeted-mcp-verification.json`
- `data/artifacts/opendoor-apn-gap-closure/clay-statewide-resolution-report.json`
- `data/artifacts/opendoor-apn-gap-closure/clay-statewide-publication-receipt.json`
- `data/artifacts/opendoor-apn-gap-closure/clay-statewide-mcp-verification.json`
- `data/artifacts/opendoor-apn-gap-closure/lee-statewide-resolution-report.json`
- `data/artifacts/opendoor-apn-gap-closure/lee-statewide-publication-receipt.json`
- `data/artifacts/opendoor-apn-gap-closure/lee-statewide-mcp-verification.json`
- `data/artifacts/opendoor-apn-gap-closure/orange-statewide-resolution-report.json`
- `data/artifacts/opendoor-apn-gap-closure/orange-statewide-publication-receipt.json`
- `data/artifacts/opendoor-apn-gap-closure/orange-statewide-mcp-verification.json`
- `data/artifacts/opendoor-apn-gap-closure/latest-mcp-global-coverage-2026-09-12.json`
- `data/artifacts/opendoor-apn-gap-closure/latest-mcp-hoa-pm-coverage-2026-09-12.json`
