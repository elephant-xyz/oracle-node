# Broward Sunbiz local extraction preparation

This path prepares Florida Division of Corporations fixed-width corporate data
locally. It does not download Sunbiz data, call AWS, upload, enqueue, or publish.
The operator supplies an existing daily/quarterly text file or ZIP.

## County-scope rule

`docs/broward-sources.yaml` is the only ZIP-candidate source. The CLI reads the
exact 55 strings under `sunbiz.zip_candidates`, requires each one to be five
digits, and rejects duplicates, partial prefixes, or ranges. There is no numeric
range expansion and no CLI ZIP override.

A candidate ZIP is not proof that an address is in Broward. USPS ZIP delivery
areas cross county boundaries, including ZIPs in the checked-in candidate list.
The extraction therefore has two gates:

1. The existing Sunbiz fixed-width parser and ZIP matcher screen all principal,
   mailing, registered-agent, and officer addresses against the exact catalog
   candidates.
2. An exact-address validation manifest must mark a matched address `inside`
   with county FIPS `12011` and non-empty evidence before its registration can
   enter `broward-records.jsonl`.

Missing decisions, explicitly unresolved decisions, and incomplete addresses
fail closed into `unresolved-candidates.jsonl`. Proven non-Broward addresses go
to `outside-candidates.jsonl`. Running without a validation manifest is a safe
candidate-discovery pass: it emits zero Broward records and marks every
candidate occurrence unresolved.

The county gate scopes the corporate registration because it has at least one
verified Broward address role. The shared parser preserves the complete Sunbiz
registration, so other related addresses on that registration may be outside
Broward. Only address roles listed in the output record's `matchedAddresses`
passed the Broward validation gate.

## Validation manifest

Use JSONL (one object per line) or a JSON array:

```json
{
  "validationKey": "broward-address-v1:<64 lowercase hex characters>",
  "status": "inside",
  "countyFips": "12011",
  "evidence": "Local parcel/address point intersects Broward county boundary dataset <version>"
}
```

Allowed decisions:

- `inside` requires `countyFips: "12011"` and non-empty evidence.
- `outside` requires a different five-digit county FIPS and non-empty evidence.
- `unresolved` is accepted but cannot emit a Broward record.

Candidate output supplies the exact `validationKey` and parsed address needed
for a local parcel/address join or point-in-polygon job. ZIP-only and
city/ZIP-only addresses remain unresolved even if a manifest contains their
key; there is not enough address specificity to reuse that decision safely.

After resolving candidate keys, start a fresh output directory with
`--validation-manifest`. A manifest change intentionally invalidates an old
checkpoint so county decisions cannot change underneath a resumed output.

## CLI

Text input:

```bash
npm run broward:sunbiz:prepare -- \
  --input /local/sunbiz/20260828c.txt \
  --output-dir /local/output/broward-sunbiz-20260828 \
  --validation-manifest /local/evidence/broward-address-validation.jsonl
```

ZIP input is detected by `.zip`; `--format zip` can override detection:

```bash
npm run broward:sunbiz:prepare -- \
  --input /local/sunbiz/cordata.zip \
  --format zip \
  --output-dir /local/output/broward-sunbiz-quarterly \
  --validation-manifest /local/evidence/broward-address-validation.jsonl
```

The official quarterly archive has historically used Deflate64. The local ZIP
reader invokes the system `unzip` command so it can follow the established Lee
workaround; it does not use AWS or the worker's `yauzl` reader. Install an
`unzip` implementation that supports method 9 before processing such an
archive.

Useful controls:

- `--checkpoint-interval N` durably saves counts, cursor, and output byte
  offsets every N newly processed rows (default `10000`).
- `--max-source-records N` pauses after N new rows for a bounded smoke run.
- `--resume` resumes a matching `running` or `paused` checkpoint.
- `--skip-lexicon` omits the per-registration bundle produced by the existing
  Sunbiz Lexicon transform.
- `--sources PATH` selects a source catalog path, but that catalog must still
  identify Broward, Florida, FIPS `12011` and contain exact ZIP strings.

Resume verifies input path, size, modification time, effective format, source
catalog digest, validation-manifest digest, output schema, and transform mode.
Before appending, it truncates every JSONL file to its checkpointed byte offset.
This removes writes made after the last durable checkpoint and prevents
duplicates after interruption. A completed checkpoint is idempotent.

## Outputs and reconciliation

The output directory contains:

- `broward-records.jsonl` — shared-parser extraction records with only
  county-verified ZIP matches in `matchedAddresses`.
- `broward-lexicon-bundles.jsonl` — bundles from the existing
  `transformSunbizRecord` implementation (unless `--skip-lexicon`).
- `unresolved-candidates.jsonl` — fail-closed exact-address work manifest.
- `outside-candidates.jsonl` — evidence-backed non-Broward exclusions.
- `checkpoint.json` — resumable cursor, counts, and durable output offsets.
- `reconciliation.json` — run configuration, output paths, counts, and balance
  checks.

The summary asserts:

```text
sourceRecordsRead
  = invalidRecordCount
  + validNonCandidateRecordCount
  + candidateRecordCount

candidateRecordCount
  = emittedBrowardRecordCount
  + outsideOnlyRecordCount
  + unresolvedWithoutInsideRecordCount

candidateAddressMatchCount
  = verifiedInsideAddressMatchCount
  + verifiedOutsideAddressMatchCount
  + unresolvedAddressMatchCount
```

`tests/fixtures/sunbiz/corporate-fixture.mjs` supplies a five-row bundled sample:
one emitted Broward registration, one cross-boundary outside registration, one
unresolved registration, one non-candidate registration, and one invalid row.
The integration test exercises a pause/resume cycle and a small generated ZIP.
No quarterly archive is downloaded or published by the test or CLI.
