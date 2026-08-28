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
  --publication-scope "full" \
  --denominator-basis "county_total" \
  --updated-at "2026-07-24T00:00:00.000Z"
```

The updater reads back the public query table and coverage artifacts, verifies
the coverage county identity and matching `publicationScope`, validates URLs
and timestamps, rejects duplicate keys/FIPS codes, and sorts entries
deterministically. Coverage is mandatory; `permitQueryTableUrl` and
`placesTableUrl` may be `null`.

`publicationScope` is a versioned, fail-closed contract:

- `full` + `county_total`: the denominator is the verified whole-county source.
- `partial` + `county_total`: only part of a known county denominator is present.
- `pilot` + `published_subset`: counts and percentages describe a bounded pilot,
  not the county. For example, 50 ingested of 50 expected is 100% of the
  published subset and is still not full-county coverage.

The catalog schema is `1.1`; every entry must declare this scope. New coverage
artifacts must carry the identical object. The updater rejects malformed or
different coverage scope but permits an older immutable coverage artifact that
omits it when the catalog supplies explicit scope. MCP reports that provenance
as catalog plus legacy coverage; no missing scope is inferred as `full`.

Consumers should use Elephant MCP `listPublishedCounties` instead of coupling
directly to this repository path.
