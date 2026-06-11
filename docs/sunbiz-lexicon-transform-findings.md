# Sunbiz Lexicon Transform Findings

Date recorded: 2026-05-26

## Model added in `../lexicon`

The local `../lexicon` repo currently uses top-level `classes`, `tags`, `data_groups`, and `common_patterns` rather than the older `vertices`/`edges` shape.

Added blockchain classes:

- `business_registration` for the Sunbiz corporate registration record.
- `business_registration_address` for principal/mailing address roles on a registration.
- `business_registration_party` for registered agents and officers.

Reused existing classes:

- `company`
- `address`

Added data group: `Business Registration` with relationships:

- `company_has_business_registration`
- `business_registration_has_address`
- `business_registration_address_has_address`
- `business_registration_has_party`
- `business_registration_party_has_address`

Modeling decision: address roles are represented by the `business_registration_address` bridge class instead of multiple relationship types from `business_registration` to `address`, because the current schema generator keys relationship schemas by `from_to` and would collapse multiple role-specific relationships with the same endpoints.

## Transform output

Script:

```text
scripts/transform-sunbiz-corporate-to-lexicon.mjs
```

Full output prefix:

```text
s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-lee-corporate-quarterly-2026q2-expanded/lexicon-transform/business-registration-v1/
```

Summary:

```text
s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-lee-corporate-quarterly-2026q2-expanded/lexicon-transform/business-registration-v1/summary.json
```

Full-run counts:

- source JSONL chunks read: `80`
- source records read: `379,467`
- transformed records: `379,467`
- invalid records: `0`
- `company` records: `379,467`
- `business_registration` records: `379,467`
- `business_registration_address` records: `758,511`
- `business_registration_party` records: `1,005,170`
- de-duplicated `address` records: `641,031`
- relationship records: `3,906,808`
- output objects: `77` including `summary.json`
- total output size: about `2.17 GB`

The transform preserves Sunbiz fields that were not previously in the lexicon, including document number, raw and normalized status/filing type, FEI, filed date, last transaction date, annual report years/dates, registered agent, officer titles/types/ordinals, source file name, source line number, and ZIP-prefix match provenance.

## Known gaps / follow-ups

- `corevent.zip` has not been extracted yet. It likely contains event/filing history that should become a separate immutable event model.
- `business_registration_party.party_type_code` is preserved as a raw source code. We should decode it after confirming the official code list.
- Relationship JSONL records currently reference endpoints by lexicon class type and stable `request_identifier`. If the next ingestion step requires CID/file references, add a resolver/loader layer rather than changing the preserved transform output.
- This transform selects Lee-relevant entities by ZIP, not by joining against permit work locations. The next enrichment step can match permit/company/address facts after permit details drain.
