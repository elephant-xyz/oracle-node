# Broward roofing-only BBB worklist

Date: 2026-08-31  
External BBB requests: **blocked / not run**  
Publication: disabled

## Access boundary

The Cloud Agent's ordinary Puppeteer request to the Fort Lauderdale roofing
category returned HTTP 403 and a Cloudflare challenge. The process did not
solve, retry around, or bypass that challenge. Broward's source matrix requires
approved [BBB API](https://developer.bbb.org/) access; no BBB API credentials
are configured.

PR 200's documented live evidence was a 15-profile category-page pilot from a
different desktop/network environment. It is not a reusable proof of complete
BBB coverage.

## Roofing-only preparation

`scripts/build-broward-roofing-bbb-worklist.mjs` prepares the smallest
compliant API scope from the current isolated-Neon permit inventory:

- reads only permits explicitly classified as roofing;
- uses only allow-listed public contractor name and license fields;
- deduplicates by usable license, then normalized business name;
- excludes owner-builder, TBD, unknown, and non-business placeholders;
- retains aggregate roofing permit count, date range, and source-system
  evidence; and
- writes private mode-0600 JSONL and a checksum summary.

It performs no BBB request and stores no permit number, property address,
phone, email, owner, or source payload.

```bash
npm run broward:bbb:roofing-worklist -- \
  --output-dir downloads/broward/bbb-roofing-worklist
```

## Reconciliation

| Metric | Count |
| --- | ---: |
| Loaded roofing permits | 22,414 |
| Roofing permits with allow-listed contractor name | 10,067 |
| Placeholder permit rows excluded | 384 |
| Unique licensed contractor candidates | 1,381 |
| Name-only candidates | 0 |
| Source rows accounted for | 10,067 / 10,067 |

Candidate artifact SHA-256:
`a3027ada664703d3811253ccc26f8ca850bc18b3d8213883e492414c6f1c4ed9`.

The worklist may be sent to BBB only after approved API credentials and field
usage terms are available. Complaint/review data must remain internal-use-only
under the documented terms.
