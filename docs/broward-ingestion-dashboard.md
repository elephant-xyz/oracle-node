# Broward local ingestion dashboard

The dashboard is a read-only Node.js view of the active local Broward appraisal
ingestion. It uses only:

- `downloads/broward/full-ingestion/state.json`
- `downloads/broward/full-ingestion/results.ndjson`
- `downloads/broward/broward-full-ingestion.log` file metadata
- the optional fixed
  `downloads/broward/active-query-data-only-handoff.json` manifest and its
  classified post-boundary state, results, and log metadata
- the fixed 534,309-row county denominator

It does not call AWS or another service, use a database, alter the ingestion, or
return parcel, address, owner/contact, source payload, error-text, or log
contents. The JSON endpoint contains aggregate counts and local file/storage
metadata only.

When the fixed handoff manifest is present, the dashboard validates that the
frozen publishable checkpoint ends at the same row where the query-data-only
checkpoint begins. Counts, row-index-deduplicated outcomes, usage types, and
throughput are then combined across the two non-overlapping segments. The
`handoff` response object reports only the boundary, aggregate old/new counts,
post-boundary transform-error count, and counts of preserved-but-excluded old
files. It never reports parcel identifiers or file paths.

## Start and attach

Check existing sessions before starting:

```bash
tmux -f /exec-daemon/tmux.portal.conf ls
```

Start the dashboard for Cloud Agent port forwarding on port 47831:

```bash
tmux -f /exec-daemon/tmux.portal.conf has-session -t '=broward-ingestion-dashboard' 2>/dev/null || tmux -f /exec-daemon/tmux.portal.conf new-session -d -s broward-ingestion-dashboard -c /workspace 'exec npm run broward:dashboard -- --host 0.0.0.0 --port 47831'
```

Attach to its console:

```bash
tmux -f /exec-daemon/tmux.portal.conf attach-session -t broward-ingestion-dashboard
```

For loopback-only use, omit the host and port flags. The safe defaults are
`127.0.0.1:47831`.

Open `http://127.0.0.1:47831/` from inside the VM, or use the Cloud Agent port
preview for port 47831 when the client exposes one. The endpoints are:

- `GET /` — responsive dashboard UI, refreshing every five seconds
- `GET /api/status` — aggregate-only live JSON
- `GET /healthz` — server liveness

## Metric semantics

- **Completion** is attempted seed rows divided by 534,309.
- **Expected source misses** are `source_error` results whose BCPA response had
  no usable parcel record after retries. No transform ran for these rows.
- **Other source errors** are HTTP, fetch, seed-validation, or other source-stage
  failures. They remain separate from expected empty source responses.
- **Transform errors** reached the county transform and failed there.
- **Recent throughput** is the deduplicated attempt rate over the last 15
  minutes.
- **Active-runtime ETA** uses that recent rate, falling back to the average rate
  after capping inactive gaps at two minutes. The ETA is processing time and
  assumes continuous execution; it is not a promise of wall-clock completion.
- **Running/stale** combines a read-only `/proc` process check with input
  activity for the active post-boundary segment when a handoff is configured.
  A matching process with no activity for more than two minutes is stale.

Result rows are reduced in memory to numeric status and timestamp arrays keyed
by row index. This deduplicates resumed rows without retaining private fields.
The pre-boundary scanner is capped by its frozen checkpoint and cannot count
preserved old artifacts or captures at or above the handoff boundary.
