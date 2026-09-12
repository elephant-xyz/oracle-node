# OpenDoor APN exception packet — 2026-09-12

## Checkpoint

- OpenDoor source rows: 18,225.
- Published source overlap across all Florida MCP routes: 18,200 (99.863%).
- Closed APN-backed cohort: 515 of 527.
- Remaining: 12 APN-backed exceptions and 13 rows without usable APNs.
- This packet covers only the 12 APN-backed exceptions.

Source cohort: `data/artifacts/opendoor-apn-gap-closure/apn-backed-gaps.csv`.
Verification: `data/artifacts/opendoor-apn-gap-closure/latest-mcp-global-coverage-2026-09-12.json`.

## Resolved after the initial packet

Polk parcel `24-29-14-283150-000280` (`242914283150000280`) is the authoritative
current parcel for 6121 Sunset Vista Dr, Lakeland. The live Polk Property
Appraiser parcel page confirms the exact physical address and records the July
2021 sale to OpenDoor Property Trust I. The published target row had no existing
Elephant identity.

- OpenDoor UUID: `1728b2da-f7fe-51b3-bde6-b259a16bdc1a`.
- Base CID: `QmV7nzNj2EkFjtcjzJyA1cyp5nEwrDd2fGUPBdgmNsTdBS`.
- HOA/PM CID: `QmbXhSifohRV3RyQ28ck3CmYSK1qBbuSFEPhGci83u2jJ5`.
- Statewide 2026 Q3 Sunbiz result: `SUNSET VISTA HOMEOWNERS ASSOCIATION, INC.`
  (`N05000005443`), with no company property manager resolvable from its
  registered-agent record.
- Official parcel:
  `https://www.polkflpa.gov/CamaDisplay.aspx?OutputMode=Display&ParcelID=242914283150000280&SearchType=RealEstate&cookie_test=true`.
- Official GIS:
  `https://gis.polk-county.net/server/rest/services/Map_Property_Appraiser/MapServer/1/query`.

Duval RE `1525625734` is the authoritative current parcel for 7156 Deerfoot Point Cir unit 3. The National Address List mailing address exactly matches the frozen OpenDoor identity, and the appraiser situs records the same condominium as unit `6-3`. The target identity was null before assignment.

- OpenDoor UUID: `b72e3903-b48c-5d79-a35c-e56f0f7f5acf`.
- Base CID: `QmVBXXV3UKuCHE7QCTzT3o6ULaFj7XJL1f1Tzrk8DsLsvD`.
- HOA/PM CID: `QmVrF1SNmn83uke67Qj1msggLt1cGBBwogXQCAJtZaFQUD`.
- HOA/PM result: `no_sunbiz_hoa`.
- Coverage CID: `QmXKwxHyj9c8JmqH5G1PZQQwFT4CvbmCkM5EZXPdU4pjGs`.

## Joaquin delegation

### Orange — seven source-record exceptions

The current OCPA API returns HTTP 200 with an empty parcel result for every row. The 2025 Florida DOR cadastral layer returns zero-attribution parcel shells, or no populated address match. These are not IP blocks. Obtain a corrected/current parcel identifier or authoritative historical crosswalk.

- `13-22-28-3528-11-120` — 2124 Longfellow Ct, Orlando 32818 — UUID `d4b62c0b-9aa9-5b8f-bb5f-a27fe6821794`.
- `01-23-32-7597-19-080` — 20218 Macon Pkwy, Orlando 32833 — UUID `0fcb1bf1-35f3-5118-9655-196ff19d6d1b`.
- `19-22-30-2679-00-190` — 1603 Woodward St unit 19, Orlando 32803 — UUID `5c6a9c7f-e7d0-50de-a066-7990f574de92`.
- `302228100102030` — 511 Huntington Pines Dr, Ocoee 34761 — UUID `03532854-7525-5eff-b846-55a987503273`.
- `232231897301030` — 816 Jade Forest Ave, Orlando 32828 — UUID `60700240-b2d1-5b15-9419-634d09e87357`.
- `212028824103150` — 557 Hebrides Ct, Apopka 32712 — UUID `ec6118f4-b552-5c84-a266-0a81f4ae7402`.
- `032027843804870` — 4607 Coppola Dr, Mount Dora 32757 — UUID `ef2e54c1-2dc9-5769-bbeb-4cc5791c7b74`.

Evidence: `orange-remaining-8.json`, `orange-statewide-identifier-results.json`, and `orange-statewide-address-results.json`.

### Miami-Dade — two inactive or historical folios

The county address service validates each address to the supplied folio, but the live Property Appraiser returns `Folio not found`. Do not assign from the address service alone. Obtain an inactive-folio history or current successor parcel.

- `10-7915-004-0180` — 394 NE 36th Avenue Rd, Homestead 33033 — UUID `a8fca7b3-14e5-554a-bb76-a38633b531dd`.
- `04-3108-001-0100` — 783 E 17th St, Hialeah 33010 — UUID `87406ef9-5dc0-58b3-8dc0-7f630d502399`.

### Polk — one occupied target

- `272817828700000230` — 67 7th St S, Lake Hamilton 33851 — UUID `9fb06c39-f7d8-5b27-8df8-0b56a9847d4e`. The exact parcel row already carries UUID `b6cddeca-f8f1-50aa-9dd2-3078d089c516`; resolve the source-identity collision. Never overwrite it automatically.

### Hillsborough — one changed folio

- `0491050000` — 9011 Spruce Creek Cir, Riverview 33578 — UUID `edc710e4-2651-576f-b1af-5faf3fcecceb`.

The supplied folio is absent. The current official situs resolves to folio `0491506782`. Obtain parcel-history evidence that connects the old and current folios before assigning.

### Clay — one occupied target

- `38-06-26-018409-003-00` — 1504 North St, Green Cove Springs 32043 — UUID `8cc7c1b2-3aa3-5540-b44e-be8bda42a0b0`.

The exact parcel already carries UUID `f00131a2-5aec-5e14-b1da-a59a6eab60ad`. Resolve the source-identity collision. Never overwrite it automatically.

## Acceptance requirements

For each proposed resolution:

1. Provide an authoritative current parcel or documented historical-to-current crosswalk.
2. Prove a one-to-one APN and situs match, including condominium unit where applicable.
3. Confirm that the target row has no occupied identity; collisions require explicit identity adjudication.
4. Preserve every unrelated base and HOA/PM row.
5. Run the verified statewide Q3 Sunbiz HOA/PM process for any newly resolved subdivision.
6. Publish immutable base and HOA/PM CIDs, update the MCP catalog, and verify the exact UUID/token pair through a fresh MCP process.
