# Broward municipal Accela bulk-record fallback

Status: prepared only; no request has been submitted.

This is the fail-closed fallback if the official anonymous Accela searches for
Plantation, Cooper City, or Weston cannot reconcile a capped one-day window.
It is not a basis for marking any current checkpoint window complete.

## Requested dataset

Ask the records custodian for a source-generated CSV (or another documented
machine-readable export) containing **all municipal permit records** whose
record/file date falls within the unresolved date windows supplied with the
request. The response must include records with a missing parcel/folio or
missing/non-standard location; it must not be limited to the Broward property
seed.

Request these fields where maintained:

- stable internal record identifier and public permit/record number;
- record/file date and the custodian's definition of that date;
- record type/subtype, status, and status date;
- site address and unit;
- parcel/folio when present;
- project description when publicly releasable;
- source-system extraction timestamp;
- total row count and any exclusions/redactions applied.

Ask for UTF-8 CSV, one row per permit identity (or documentation of repeated
rows), the exact inclusive start/end dates, and a cost estimate before work
that would incur fees. Do not request plans, attachments, applicant details,
owner details, contractor details, or other row-level personal data for this
inventory recovery.

## Official channels

### Plantation

- [Building Records](https://www.plantation.org/government/departments/building-safety/building-records)
  states that Building Safety maintains permit-related property records,
  directs requesters to the Public Records Center, and lists Building Safety at
  954-797-2783.
- Scope the request to the unresolved Accela windows. Plantation warns that
  records generally predating 2004 may instead be on microfilm and that
  deposits or cost recovery may apply.

### Cooper City

- [City Clerk's Office](https://www.coopercity.gov/page/city-clerks-office) is
  the municipality's public-records custodian route.
- [Community Development](https://www.coopercity.gov/page/community-development)
  is the responsible permit department; the city's published main number is
  954-434-4300.
- Ask the Clerk to coordinate a database export from Community Development,
  not document images or a property-only lookup.

### Weston

- [Public Records Requests](https://www.westonfl.org/government/city-clerk/public-records-requests)
  identifies the City Clerk as Custodian of Records and lists
  954-385-2000. It also warns that extensive clerical or information-technology
  service charges may apply.
- [Building Code Services](https://www.westonfl.org/government/building-code-services)
  is the responsible permit department.
- [Obtaining Building Plans](https://www.westonfl.org/government/building-code-services/obtaining-building-plans)
  documents separate historical handling for buildings constructed before 2005. This inventory request should ask for permit metadata, not plans, and
  must state the unresolved Accela date windows explicitly.

## Acceptance gate

Before using a response to complete a checkpoint parent:

1. verify the response dates are inclusive and match every requested window;
2. reconcile the custodian's row total to the received machine-readable rows;
3. deduplicate by municipality plus stable record identity;
4. compare against already completed date-window and shard artifacts;
5. confirm missing-parcel and missing-location records were included or
   quantify/document any explicit custodian exclusion;
6. retain provenance and hashes privately, publishing aggregate counts only.

If any gate fails, retain the response as partial gap-fill evidence and leave
the parent window pending.
