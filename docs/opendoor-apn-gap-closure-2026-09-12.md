# OpenDoor APN gap closure — 2026-09-12

## Scope and safety contract

- Input: 18,225 OpenDoor Florida rows from the 2026-09-10 source export.
- Frozen gap cohort: 527 rows with APNs and 13 rows without APNs.
- Only exact county-local APN/parcel matches are eligible.
- Existing non-null identities are never overwritten.
- Shared, ambiguous, or already-occupied parcel targets are quarantined.
- Existing HOA/property-manager fields are preserved. New rows are enriched when an official subdivision and the reviewed Sunbiz index support a match.

## Published and verified

| County | Closed APN rows | Published base CID |
| --- | ---: | --- |
| Hillsborough | 336 | `QmaNScYyp7gEcNQPnkDqLjkZyb2yHFBgpnC8pFcAva7buR` |
| Pinellas | 78 | `QmbmJy9wczN99qQPqV7XQGb4KfCTSUJA6B4ArtdssRN3Mo` |
| Pasco | 31 | `QmPxigjpuUN3mn5HivjDmo2eWSU13DVqomgf4E4pTEU9Fm` |
| Orange | 21 | `QmNPLvGi8u9k3JFmR55QfQ4k1athFxcBrBDrsASij6Tipt` |
| Osceola | 14 | `QmQMoMkBStLDGid7rwqGUZovcfwSt3cYAWUQyWwZu4k4gE` |
| Broward | 12 | `QmdD3f3NqgNRiFn4hpGqEwpNqw5tXK3FWW2QRoRBYddhX5` |
| Duval | 9 | `QmazNstZWjYu77HPbPLyvCzTC938rpfGLwyU7WKnvv6iE4` |
| Nassau | 4 | `QmdjszjPcVjt3KepeDyE37JfdBx2TaidTsy6Qwem4nf9N6` |
| Palm Beach | 3 | `QmQ1gzMkF7Af3S6DpYKt17LDxsjd8jApN8snNTFxRVfSTg` |
| Clay | 1 | `Qmd9r8oU6SJzEYmb8EoBoGrrwP3RDc4uj6w1XTkC5X4pfo` |
| Columbia | 1 | `QmVd5YxqhwyJHRzzamSbGv4ertMjPL5V2kEjMrtGTwqH57` |
| Sarasota | 1 | `QmfL1XDLG8NhvB8QH9WftjSeksJ3TGWJh7qLZDCadmH4mx` |

Current verified checkpoint:

- Closed APN-backed rows: 511 of 527.
- Published UUIDs across base county tables: 18,203.
- Latest OpenDoor source overlap: 18,196 of 18,225 (99.841%).
- Remaining gaps: 29 total; 16 APN-backed and 13 without APNs.
- Exact fresh-process MCP checks passed for each published UUID/token pair.
- A Pasco token mismatch was caught by post-publication validation, corrected, republished, and then verified 31/31.

Evidence: `data/artifacts/opendoor-apn-gap-closure/fresh-mcp-verification-6.json`.

## Source results

| County | Rows obtained | Source and route | Result |
| --- | ---: | --- | --- |
| Hillsborough | 335 | `HC_ParcelsPublic` FeatureServer through Railway | 325 existing rows updated; 10 current official parcels appended |
| Pinellas | 78 | Pinellas PublicWebGIS `MapServer/1`, local | 78/78 unique STRAPs; used because PCPAO print is blocked |
| Orange | 21 | OCPA API through Railway | 20 existing rows updated; one current parcel appended |
| Osceola | 14 | Osceola Property Appraiser OData, local | 14/14 exact parcels |
| Nassau | 4 | Nassau public tax-map ArcGIS, local | 4/4 exact parcels |
| Sarasota | 1 | Sarasota Property Appraiser page, local | 1/1 exact parcel |
| Columbia | 1 | Columbia Grizzly GIS, local | 1/1 exact parcel |
| Pasco | 1 additional | Pasco Property Appraiser, local | County-formatted APN and situs resolved to one null target |
| Broward | 12 | BCPA `search.aspx/GetData` and `getParcelInformation` through AWS Batch | 12/12 exact folios and matching situs; 12 rows appended |
| Duval | 8 additional | COJ Property Appraiser detail pages through AWS Batch | 8/8 exact RE numbers and matching situs; 8 rows appended |
| Palm Beach | 2 additional | PBCPAO detail pages through AWS Batch | 2/2 exact PCNs and matching situs; 2 rows appended |
| Clay | 1 | Florida DOR statewide cadastral FeatureServer | Exact Clay PA parcel and situs for `41-04-26-018793-071-00`; one row appended |

Each automatic assignment has one authoritative parcel, an exact or documented county-specific APN transformation, matching situs evidence, a null target identity, and the frozen OpenDoor UUID/token pair.

## HOA/property-manager coverage

The updated identity subsets were evaluated against the verified statewide 2026 Q3 Sunbiz source. Existing published associations were checked for regressions before replacement.

| County cohort | Rows | Resolvable HOA rows | Resolvable PM rows |
| --- | ---: | ---: | ---: |
| Hillsborough closed gaps | 336 | 67 | 16 |
| Pasco closed gaps | 31 | 6 | 1 |
| Orange newly closed | 21 | 2 | 0 |
| Osceola newly closed | 14 | 1 | 0 |
| Pinellas newly closed | 78 | 1 | 0 |
| Broward AWS-closed | 12 | 1 | 0 |
| Duval AWS-closed | 8 | 1 | 1 |
| Palm Beach AWS-closed | 2 | 0 | 0 |
| Clay DOR-closed | 1 | 0 | 0 |

Nassau, Sarasota, and Columbia produced no safe new association: the outcomes were no subdivision, no Sunbiz HOA, or non-unique. No ambiguous match was published.

The AWS-closed rows were evaluated with a new complete two-pass scan of all 12,808,196 records in the verified statewide 2026 Q3 Sunbiz archive. Existing HOA/PM overlay rows were preserved byte-for-byte before the new rows were appended. Published overlay CIDs are Broward `QmQ14o8m9LVCk6Wgkk6fB7iCnjcGKHQo3TshtLGesPpYk8`, Duval `QmWFVt1tBnMQtt1yJDzTEDAPn2m1TYa9GJYFCQ1NP1AFSc`, Palm Beach `QmcduEc8TrmXfev4Y2Vn6zFjSPT6mxMV6kMgiMceocinQa`, and Clay `QmaiKW68swEjXU6agFt1Qnu4JayyKQFoZoG3V4tmGR79UN`.

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
- The eight remaining Orange APNs still return HTTP 200 with an empty OCPA result, so they are source-record exceptions rather than IP blocks.
- Both Miami-Dade addresses validate to the supplied folios in the county address service, but the live Property Appraiser returns `Folio not found` for both.

## Current delegation and exception queue

| County | Rows | Current evidence / required action |
| --- | ---: | --- |
| Duval | 1 | Placeholder APN `00000000`; the situs points to RE `1525625734`, so this is address-only identity review rather than an APN match |
| Orange | 8 | Current OCPA API returns no parcel for the transformed source APNs; source correction or historical lookup required |
| Miami-Dade | 2 | County address service validates each address/folio pair, but the live Property Appraiser reports both folios absent; historical/inactive-folio review required |
| Clay | 1 | Occupied-target conflict; the parcel already carries a different OpenDoor identity |
| Polk | 2 | One occupied-target conflict; one source APN is an address string rather than a parcel identifier |
| Lee | 1 | Unique current-table situs candidate; exact official STRAP confirmation pending |
| Hillsborough | 1 | Source folio is absent; the situs now resolves to a different official folio and requires manual identity review |

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
