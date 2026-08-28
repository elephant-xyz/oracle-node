# Broward local appraisal acceleration and safe migration

Date: 2026-08-28  
Scope: local appraisal capture/transform only; no AWS, database, or publication

## Existing implementation and history review

Repository history and read-only GitHub review did not find a second merged
“warm handoff” implementation or a county pipeline with documented
~500-parcel/minute throughput.

- oracle-node commit `fd6c22d` is the proven Broward design: four long-lived
  Elephant CLI parent processes with isolated `TMPDIR`s. This avoids repeated
  CLI module startup and prevents the fact-sheet builder's fixed
  `generated-htmls` directory from racing.
- [oracle-node PR #191](https://github.com/elephant-xyz/oracle-node/pull/191)
  contains the checkpointed Broward run and accepted 50-parcel artifacts.
- [oracle-node PR #192](https://github.com/elephant-xyz/oracle-node/pull/192)
  uses a normal bounded worker pool for Hillsborough, but starts transform
  scripts per parcel and provides no faster artifact contract.
- [elephant-cli PR #186](https://github.com/elephant-xyz/elephant-cli/pull/186)
  is the upstream precedent for data-only transformation: it omits the single
  `generateFactSheet(tempRoot)` call after the county scripts. It remains open,
  so installed CLI 1.58.1 has no public switch.
- [elephant-cli PR #108](https://github.com/elephant-xyz/elephant-cli/pull/108)
  explored a complementary fact-sheet-only pass by copying transformed JSON to
  the output before fact-sheet generation. It was closed and is not available
  in CLI 1.58.1.

The acceleration here keeps the proven long-lived isolated workers. A
fail-closed in-memory loader applies PR #186's exact one-call omission only in
query-data-only children; it does not patch `node_modules` on disk. The same
guard routes Elephant CLI's exact four mapping-script calls and final extractor
call through a serialized process-warm CommonJS executor. The unchanged county
entrypoints rerun for every parcel, but Node process startup and dependency
reloads are removed. If the upstream source no longer contains exactly the
expected calls, the worker refuses to run. Publishable mode never registers the
loader or warm script executor.

The old fixed four-row barrier is also replaced with continuous warm handoffs:
as soon as a worker finishes, it receives the next row. Results are held until
the contiguous source-order prefix is complete, then `nextRowIndex` is
atomically renamed into place exactly as before. There is never more than one
parcel in flight per worker, so source concurrency remains capped at four.

## Artifact contracts

Publishable mode remains the default and keeps the existing layout:

```text
artifacts/<shard>/<folio>.zip
captures/<shard>/<folio>.json.gz
state.json
results.ndjson
```

`--query-data-only` uses a separate default output and a deliberately
incompatible artifact name:

```text
QUERY_DATA_ONLY_DO_NOT_PUBLISH.json
query-data-only-artifacts/<shard>/<folio>.query-data-only.zip
captures/<shard>/<folio>.json.gz
state.json
results.ndjson
```

Every data-only ZIP has all three independent guards:

1. the nonstandard `.query-data-only.zip` suffix;
2. a ZIP comment stating that it is not publishable;
3. root `BROWARD_QUERY_DATA_ONLY_DO_NOT_PUBLISH.json` with
   `publishable: false`.

The artifact inspector rejects `fact_sheet.json`, HTML/assets,
`*_has_fact_sheet` keys, references to `index.html`, and every broken relative
IPLD file link. Fact-sheet relationships are not generated and no dangling
`fact_sheet` reference is retained. A full transform of the same accepted pilot
captures is used as a reference: every non-fact-sheet `data/*.json` filename
and canonical JSON value must remain present.

The deferred outputs are explicit:

- `data/index.html`;
- `data/fact_sheet.json`;
- `data/relationship_*_to_fact_sheet.json`;
- data-group `*_has_fact_sheet` relationship entries.

Regeneration uses the preserved seed and compressed captures through the
unchanged default publishable transform. Data-only ZIPs must never be supplied
to an uploader. This intentionally reruns the county scripts during final
publication preparation; the installed CLI's fact-sheet-only experiment was
never merged, so claiming a supported incremental finalizer would be unsafe.

## Zero-source-traffic pilot benchmark

The accepted 50-parcel capture archive can be imported with
`--capture-source`. When a capture source is supplied, a missing entry is a
hard source error; the ingest does **not** fall back to BCPA. Imported captures
are stored as private gzip JSON before transformation.

```bash
node scripts/ingest-broward-appraisal-local.mjs \
  --seed /workspace/downloads/broward/broward-validation-sample-50.csv \
  --scripts /tmp/Counties-trasform-scripts/broward/scripts \
  --capture-source /workspace/downloads/broward/broward-validation-sample-50-captures.zip \
  --output downloads/broward/benchmark-publishable \
  --concurrency 4

node scripts/ingest-broward-appraisal-local.mjs \
  --query-data-only \
  --seed /workspace/downloads/broward/broward-validation-sample-50.csv \
  --scripts /tmp/Counties-trasform-scripts/broward/scripts \
  --capture-source /workspace/downloads/broward/broward-validation-sample-50-captures.zip \
  --output downloads/broward/benchmark-query-data-only \
  --concurrency 4
```

Measured evidence is recorded below after both modes run on the same machine,
seed, transform revision `5130a7f`, captures, and concurrency.

<!-- BENCHMARK_RESULTS -->

Validation command:

```bash
node scripts/validate-broward-query-data-only.mjs \
  --artifacts downloads/broward/benchmark-query-data-only/query-data-only-artifacts \
  --captures /workspace/downloads/broward/broward-validation-sample-50-captures.zip \
  --reference-artifacts downloads/broward/benchmark-publishable/artifacts \
  --output downloads/broward/benchmark-query-data-only-validation
```

This performs structural/link checks, non-fact JSON filename parity, Elephant
CLI Lexicon validation, and a 50-row query-table Parquet dry run.

## Resume and migration procedure

Do not repurpose an existing output directory. Checkpoints now record
`artifactMode` and `initialRowIndex`; a publishable checkpoint cannot resume in
query-data-only mode, and a marked data-only directory refuses publishable
mode.

For a future migration after a run has stopped normally:

1. Read but do not edit the old `state.json`.
2. Copy its exact `nextRowIndex` value into `--start-row`.
3. Use a new path containing `query-data-only`.
4. Keep `--concurrency 4` or lower.
5. Do not set `--capture-source` for uncaptured remaining rows. New responses
   will continue to be gzip-compressed in the new output.
6. Preserve the old output for rows before the handoff. Downstream loading must
   combine old full artifacts before the boundary with classified data-only
   artifacts at/after the boundary.

```bash
node scripts/ingest-broward-appraisal-local.mjs \
  --query-data-only \
  --start-row <exact-old-nextRowIndex> \
  --seed downloads/broward/broward.csv \
  --scripts /tmp/Counties-trasform-scripts/broward/scripts \
  --output downloads/broward/full-query-data-only-from-<exact-old-nextRowIndex> \
  --concurrency 4
```

Resume that new run with the identical command. Never use
`--reset-checkpoint` for a resume.

The active 534,309-row run must not be switched while it is running. The code
path is designed for a new run or an operator-controlled handoff after a clean
atomic checkpoint; this investigation does not stop, signal, inspect through
tmux, or modify the live process/output.

## Queue/source-pressure decision

A separate remote capture queue would raise sustained request rate even if its
instantaneous concurrency remained four, and would require a second durable
cursor to reconcile with the active checkpoint. It is not enabled here.
Compressed captures already form a durable local capture queue:
`--capture-source` drains it with zero BCPA traffic, while a normal run keeps at
most four source requests in flight. Continuous worker handoffs remove local
barrier idle time without increasing the tested source concurrency ceiling.
