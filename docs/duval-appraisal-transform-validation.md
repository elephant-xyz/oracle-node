# Duval appraisal transform validation

Date: 2026-09-02
County key: `duval`
FIPS: `12031`

## Result

Pilot ingest from Task 6 was validated in place against lexicon schema, geometry
bbox, labeled-field completeness, enumerated failures, and reconciled ingest counts.

- Fresh COJ captures: **50/50**
- County transforms (existing `transformed_output.zip`): **50/50**
- CLI Lexicon validations: **50/50**
- Geometry inside Duval (lat 30.103–30.586, lng −82.05…−81.318): **50/50**
- Mean labeled-field completeness: **10.8%**
- Wall time: **127.50 seconds** for 50 parcels
- Ingest reconciliation: seedRows == attempted == success + failures (see `pilot-manifest.json`)

@elephant-xyz/cli 1.58.1 does not export mirrorValidate; completeness is labeled-field coverage after subtracting source-html-static-parts/duval.csv chrome.

Durable local evidence:

- `downloads/duval/pilot-run/pilot-manifest.json`
- `downloads/duval/pilot-run/_validation/summary.json`
- `downloads/duval/pilot-run/_validation/<folio>-validation.csv`

## Issue list

- None.

## Gap classes

- **extractor** — transform script did not emit a required artifact or emitted invalid JSON
- **capture** — a labeled COJ field was on the page but not in transform JSON
- **lexicon** — schema/enum has no home for a captured value

Extractor and capture bugs belong in `Counties-trasform-scripts`. Lexicon gaps stay in
the payload and are logged here; they are not dropped.
