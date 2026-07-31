# Published county catalog

`published-counties.json` is the canonical, public enumeration of Oracle county
datasets. Add or update an entry only after its public query-table and coverage
URLs have been read back successfully.

Update it with:

```bash
npm run catalog:update -- \
  --county-key "lee" \
  --county-name "Lee" \
  --state-code "FL" \
  --county-fips "12071" \
  --query-table-url "https://..." \
  --dataset-coverage-url "https://..." \
  --updated-at "2026-07-24T00:00:00.000Z"
```

The updater reads back the public query table and coverage artifacts, verifies
the coverage county identity, validates URLs and timestamps, rejects duplicate
keys/FIPS codes, and sorts entries deterministically. Coverage is mandatory;
`permitQueryTableUrl` may be `null`.

Consumers should use Elephant MCP `listPublishedCounties` instead of coupling
directly to this repository path.
