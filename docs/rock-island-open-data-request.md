# Draft: Rock Island County GIS open-data request

**Status:** Draft for DECA/Elephant review and human submission
**Intended recipient:** Josh Boudi, Director, Rock Island County GIS
**Email:** `jboudi@rockislandcountyil.gov`
**Office:** 309-558-3772
**Prepared:** 2026-08-01

## Suggested subject

Permission request for an open Rock Island County parcel dataset

## Draft message

Hello Mr. Boudi,

DECA and Elephant are evaluating potential data-center development sites in Rock Island
County. We are building an open-source, map-based parcel acquisition tool that helps
users identify adjacent parcels, estimate combined acreage, screen proximity to public
power-infrastructure data, and organize project diligence.

We found the Rock Island County Parcels ArcGIS Feature Service associated with the
county's public GIS viewer:

`https://services9.arcgis.com/6FnscPPlUa9DXXOk/ArcGIS/rest/services/Parcels/FeatureServer/0`

The ArcGIS item is public and includes the statement "For use by the general public."
We also reviewed the county's GIS pricing guide and Digital Data Release Policy. Before
we download or publish a countywide dataset, we would appreciate written clarification
of the allowed use.

We are requesting permission to:

1. Obtain the official countywide parcel polygon and assessment dataset, either from a
   county-supplied export or through a county-approved, rate-limited ArcGIS export.
2. Retain snapshots so that changes can be audited and refreshed periodically.
3. Transform geometry to WGS84/EPSG:4326 and normalize field names while retaining
   source provenance and the original PIN.
4. Publish an openly accessible derived parcel dataset for public mapping and analysis.
5. Publish the data through ordinary object storage and content-addressed distribution,
   which may create persistent third-party caches.

The proposed open fields are:

- Parcel PIN and alternate parcel identifiers.
- Parcel polygon and centroid.
- GIS acreage and gross acreage.
- Site address, city, state, and ZIP.
- Township, municipality, jurisdiction, property class, and zoning.
- Equalized assessed value, estimated market value, and assessment/tax year.
- Source URL, source revision, snapshot date, and transformation notes.

The source also contains owner and tax-bill recipient names and mailing addresses. We
will keep those fields out of the open dataset unless the county explicitly confirms
that public redistribution of those fields is permitted. Purchased email addresses and
telephone numbers, campaign records, responses, asking prices, and acquisition status
will never be part of the county open-data publication.

Could you please confirm:

1. Whether the public Feature Service may be queried programmatically for this project.
2. Whether a full county export should instead be purchased or requested from your
   office, and the current price and available formats.
3. Whether the proposed non-owner field set may be transformed and publicly
   redistributed.
4. Whether owner and tax-bill names and mailing addresses may be publicly redistributed,
   or must remain internal.
5. The required attribution, disclaimer, and description of modifications.
6. Any restrictions on permanent mirrors, third-party caches, or content-addressed/IPFS
   publication.
7. Acceptable refresh cadence, API rate limits, and a preferred bulk delivery method.
8. The current authoritative parcel count and assessment year.
9. Definitions for `PIN`, `RICO_PARCE`, `parcel_number`,
   `alternate_parcel_number`, `class`, `EAV`, `EMV`, `owner1_*`, and
   `taxbill_*`.
10. Whether a property-class code dictionary and a data dictionary are available.

We will attribute the source as "Rock Island County GIS," identify all transformations,
preserve the county's no-warranty disclaimer, and avoid representing the data as a title
opinion, boundary survey, or utility-capacity determination.

We have not automated the county's online assessment or property-tax search pages and
will not do so; both pages state that automated retrieval is prohibited.

Thank you for helping us use the county's data responsibly. We are happy to provide a
field list, architecture diagram, sample output, or completed project-specific release
form if helpful.

Sincerely,

`[Name]`
`[Title / organization]`
`[Email]`
`[Phone]`

## Internal submission checklist

- [ ] Replace the signature placeholders.
- [ ] Confirm whether the requester should be DECA, Elephant, or another legal entity.
- [ ] Attach or complete the county Digital Data Release Policy form if requested.
- [ ] Do not attach the raw 25-parcel sample.
- [ ] Save the county's response and effective date in
      `docs/rock-island-county-findings.md`.
- [ ] Update `docs/rock-island-sources.yaml` with the approved fields, attribution,
      refresh limits, and publication decision.
