# Broward municipal Accela permit adapters

Date: 2026-08-29  
Execution scope: local-only, bounded, anonymous public search

## Source boundary

`scripts/permit-source-adapters/broward-accela.mjs` provides one reusable
Accela mechanism with jurisdiction-specific configuration. It deliberately
does not treat Broward County or Lee County as the issuing agency.

| Adapter key        | Jurisdiction    | Accela agency | Module     | Validated pilot folio(s)             | Historical boundary |
| ------------------ | --------------- | ------------- | ---------- | ------------------------------------ | ------------------- |
| `hollywood`        | Hollywood       | `HOLLYWOOD`   | `Building` | `514111160200`, `514207022070`       | Current Accela start is not certified. The official BCLA address source is separately identified as 1988-present; pre-1988 records require City archives. |
| `plantation`       | Plantation      | `PLANTATION`  | `Building` | `504108BJ0140`                       | Earliest online date is not certified. |
| `fort-lauderdale`  | Fort Lauderdale | `FTL`         | `Building` | `494209060010`, `494212072320`       | Earliest LauderBuild online date is not certified. |
| `cooper-city`      | Cooper City     | `COOPER`      | `Building` | `514106100100`                       | Historical record types are visible, but the earliest online date is not certified. |
| `weston`           | Weston          | `WESTON`      | `Building` | `503912010490`                       | Official City documentation bounds City records to post-1997 history. |

An unknown cutoff is represented as `date: null` with
`unknown_not_certified`; it never means complete history. Weston retains the
explicit `1997-01-01` boundary. Hollywood's current Accela records use
`broward_hollywood_accela_permits`, while the address-only legacy source uses
`broward_hollywood_bcla_legacy_permits`. The adapter does not query, merge, or
deduplicate the legacy source because no official Accela/BCLA migration date
was certified.

## Search and record contract

For each target, the local adapter:

1. accepts only a string in exact 12-character alphanumeric form or the
   documented 6-2-4 display form;
2. uppercases letters without numeric coercion, zero-padding, or Lee STRAP
   rewriting;
3. opens the configured jurisdiction/module portal anonymously;
4. clears Accela's default dates and submits the exact folio in `Parcel No.`;
5. captures each result page, follows ASP.NET `Next >` controls, reconciles
   the reported total, and rejects pagination truncated by the configured
   limit;
6. captures every bounded detail page sequentially and reconciles record and
   displayed parcel identity; and
7. emits the existing camel-case Accela permit-detail shape with source,
   search-result, agency/module, cutoff, and exact submitted-folio provenance.

The shared Lee browser launcher, text/status parsing, result-summary parsing,
inspection parser, More Details parser, safe path helpers, and Chromium setup
are reused where Accela markup is compatible. Broward URL resolution, folio
validation, agency/module configuration, source identity, and cutoff semantics
are Broward-specific.

## Fail-closed outcomes

A successful source observation is one of:

- `records`: one or more reconciled `CapDetail.aspx` links, complete
  pagination, and all bounded details captured; or
- `no_records`: the public portal returned an explicit Accela no-results
  marker.

Access-denied/login/CAPTCHA pages, official technical-error pages, unknown
templates, result pages with no detail links, mismatched record/folio
identity, conflicting duplicate records, missing next pages, and any result
set above the bounded detail limit are failures. They are checkpointed as
`failed` with a stable error category and are never converted to
`no_records`. No list-only permit is synthesized when a Broward detail page
cannot be captured.

## Local bounded command

Run the five-source curated probe (one folio per jurisdiction):

```bash
node scripts/probe-broward-accela-permits.mjs \
  --pilot \
  --output downloads/broward/accela/normalized-permits.private.jsonl \
  --summary downloads/broward/accela/probe-summary.private.json \
  --checkpoint downloads/broward/accela/probe-checkpoint.private.json \
  --capture-dir downloads/broward/accela/raw-private-captures
```

Custom mode accepts at most two unique folios per jurisdiction:

```bash
node scripts/probe-broward-accela-permits.mjs \
  --target hollywood:514111160200 \
  --target hollywood:514207022070
```

Hard limits are ten result pages, 25 details, and two folios per jurisdiction
per process. Defaults are five result pages and 20 details. Searches and
details are sequential, with minimum 1,000 ms target and 250 ms detail delays.
There are no retries.

Normalized JSONL, raw HTML, summary, and checkpoint files are local
mode-0600 private staging. The checkpoint is replaced atomically after search,
each captured detail, and terminal target state. A rerun skips `records` and
`no_records` targets and resumes pending details from a successful prior list
capture.

The command imports no AWS client and does not access queues, databases, IPFS,
publication paths, login flows, or CAPTCHA handling. It must not be used as a
full municipal harvest.

## Bounded live evidence

Live evidence is intentionally limited to one validated appraisal folio per
configured jurisdiction. Source outcomes, discovered/captured record counts,
record numbers, exact capture paths, and blockers are written to the private
summary. This section is updated only from that bounded run; an empty result is
not a completeness claim beyond the source's explicit historical boundary.
