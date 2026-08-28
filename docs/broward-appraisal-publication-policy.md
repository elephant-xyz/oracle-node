# Broward appraisal privacy and publication policy

Status: local technical policy draft. Human approval is required before any
public release.

## Decision

BCPA prepare captures and transformed ZIPs are private working data. They
contain owner/person/company records, mailing-address records and relationships,
situs addresses, legal descriptions, source-request metadata, and linked
content. Public availability at the source does not by itself authorize
republishing, aggregating, or linking those fields.

No raw capture, transformed ZIP, validation CSV, validation summary, generated
HTML, fact sheet, image/file record, source payload, or audit report may be
placed in a public derivative or publication bucket.

The existing 50-row pilot query table is a private validation artifact. It has
owner and situs-address columns, and its Donphan verification sidecar includes
address facts. It is not an approved public derivative.

## Automated fail-closed gate

Run:

```bash
node scripts/audit-broward-appraisal-publication.mjs \
  --transformed-dir /workspace/downloads/broward/appraisal-validation-50 \
  --validation-summary /workspace/downloads/broward/appraisal-validation-50/summary.json \
  --public-dir /workspace/downloads/broward/pilot-query \
  --report /tmp/broward-appraisal-publication-audit.json
```

The CLI is local and read-only except for the optional mode-0600 report. It has
no upload, delete, IPFS, Filebase, AWS, or publication operation. The report
must be outside the proposed public directory because it inventories private
source structure.

The command:

1. requires a bounded validation summary or an explicit positive expected
   count; it never treats “whatever files are present” as the denominator;
2. scans only canonical `<12-character-folio>.zip` transformed artifacts and
   reconciles each filename with `data/property.json`;
3. classifies denied keys and ZIP entries without copying field values into the
   report;
4. marks every noncanonical sibling of the private transformed artifacts as an
   unsafe publication sidecar;
5. requires a proposed derivative to contain exactly one root-level Parquet
   file and exactly one recognized manifest row count;
6. reads the physical Parquet schema and every row, enforces the closed field
   allowlist, scans string values for common contact/SSN/PO-box/street-address
   patterns, and requires unique canonical Broward folios;
7. requires source artifacts, distinct source folios, physical public rows,
   distinct public folios, exact source/public folio sets, and manifest counts
   to agree; and
8. exits nonzero with `REFUSE_PUBLICATION` for a denied public finding, unknown
   sidecar, malformed input, missing derivative, or count/identity mismatch.

Denied findings in a raw transformed ZIP classify that ZIP as nonpublishable;
they are expected evidence that the source is private. A separate derivative
can pass only when none of those denied fields or values is copied into it and
all counts and identities reconcile.

## Closed public row allowlist

The coded allowlist is limited to reviewed facts in these classes:

- stable property and parcel identity;
- county/state/source metadata;
- parcel centroid or reviewed geometry;
- lot and building characteristics;
- property type and use;
- assessment, taxable, building, land, AVM, and sale value/date facts.

See `APPROVED_PUBLIC_FIELDS` in
`scripts/audit-broward-appraisal-publication.mjs` for the exact names. A new
field is denied until both code and this policy are deliberately reviewed and
changed.

The following remain denied:

- owners, people, companies, corporations, trusts, taxpayers, buyers, sellers,
  grantors, grantees, and owner-derived flags or counts;
- names, birth/citizenship/veteran attributes, phone numbers, email addresses,
  fax numbers, and other contact data;
- mailing, postal, situs, street, city, ZIP, subdivision, and unnormalized
  address data;
- legal descriptions, remarks, notes, and other unreviewed free text;
- raw requests/responses, prepared inputs, source payloads, request metadata,
  internal validation output, relationships, deeds/instrument identifiers, and
  internal request identifiers;
- images, documents, URLs, IPFS CIDs, property CIDs, fact sheets, HTML, and
  source-linked file records;
- permit, Sunbiz, BBB, HOA, and other enrichment flags outside this appraisal
  derivative; and
- unknown fields, nested directories, symlinks, executables, or sidecars.

Approved sidecar names are closed to `manifest.json`,
`query-table-manifest.json`, `schema.json`, `coverage.json`, and
`privacy-scan.json`. A recognized filename does not bypass content scanning.

## Human approval gates

`AUDIT_PASS_HUMAN_APPROVAL_REQUIRED` is not release authorization. The CLI
always reports `publicationAuthorized: false`. Before publication, named human
reviewers must record approval for all of the following:

1. **Legal and source terms:** confirm the proposed aggregation and
   redistribution comply with current BCPA access terms, Florida law, public
   records exemptions, and any contractual restrictions. This document makes
   no legal conclusion.
2. **Privacy:** review the actual derivative for protected-address programs,
   suppression requirements, re-identification risk, small or sensitive
   cohorts, and combinations of otherwise public facts.
3. **Data owner/product:** approve the exact public purpose, field allowlist,
   coverage statement, retention, update, correction, and takedown process.
4. **Security:** confirm only derivative bytes and reviewed sidecars are in the
   release staging directory; keep source captures, transformed artifacts, and
   audit reports private.
5. **Reconciliation:** independently confirm the intended release denominator,
   exclusions, unique parcel count, physical Parquet count, manifest count, and
   artifact digests immediately before release.
6. **Release review:** record reviewer identities, date, policy version, exact
   commit, exact derivative digest, and approval decision in the release
   change. Re-run the audit on those exact bytes after any change.

Until every human gate is recorded, the decision remains **do not publish**.
This audit does not modify or delete the private data and does not affect the
active full Broward ingestion.
