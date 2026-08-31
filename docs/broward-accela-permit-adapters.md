# Broward municipal Accela permit adapters

Date: 2026-08-29  
Execution scope: local-only, bounded, anonymous public search

## Source boundary

`scripts/permit-source-adapters/broward-accela.mjs` provides one reusable
Accela mechanism with jurisdiction-specific configuration. It deliberately
does not treat Broward County or Lee County as the issuing agency.

| Adapter key       | Jurisdiction    | Accela agency | Module     | Validated pilot folio(s)       | Historical boundary                                                                                                                                       |
| ----------------- | --------------- | ------------- | ---------- | ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `hollywood`       | Hollywood       | `HOLLYWOOD`   | `Building` | `514111160200`, `514207022070` | Current Accela start is not certified. The official BCLA address source is separately identified as 1988-present; pre-1988 records require City archives. |
| `plantation`      | Plantation      | `PLANTATION`  | `Building` | `504108BJ0140`                 | The portal warns that records before 2004 may be unavailable online and directs users to City microfilm.                                                  |
| `fort-lauderdale` | Fort Lauderdale | `FTL`         | `Permits`  | `494209060010`, `494212072320` | Earliest LauderBuild online date is not certified.                                                                                                        |
| `cooper-city`     | Cooper City     | `COOPER`      | `Building` | `514106100100`                 | Historical record types are visible, but the earliest online date is not certified.                                                                       |
| `weston`          | Weston          | `WESTON`      | `Building` | `503912010490`                 | Official City documentation bounds City records to post-1997 history.                                                                                     |

An unknown cutoff is represented as `date: null` with
`unknown_not_certified`; it never means complete history. Weston retains the
explicit `1997-01-01` boundary. Plantation retains its portal's
`2004-01-01` online-reliability boundary and microfilm route. Hollywood's current Accela records use
`broward_hollywood_accela_permits`, while the address-only legacy source uses
`broward_hollywood_bcla_legacy_permits`. The adapter does not query, merge, or
deduplicate the legacy source because no official Accela/BCLA migration date
was certified.

## Fort Lauderdale bulk-first override

Fort Lauderdale now uses its official
[Building Permits FeatureServer](https://gis.fortlauderdale.gov/server/rest/services/BuildingPermits/FeatureServer/0)
for complete list discovery. The layer reported 204,760 rows on 2026-08-31 and
includes BCPA parcel IDs, contractor/license fields, statuses, dates, and
costs. See [the bulk ingest runbook](./broward-bulk-permit-ingest.md).

The ArcGIS `PERMITID` display is truncated and repeated, so it is not a safe
key. The bulk runner keys records by complete Accela `CASEKEY` and matches its
three components to existing portal `capID1/capID2/capID3` URLs. The browser
adapter remains useful for detail/inspection enrichment and history outside
the certified bulk layer; it is no longer the primary Fort Lauderdale
discovery path.

## Search and record contract

For each target, the local adapter:

1. accepts only a string in exact 12-character alphanumeric form or the
   documented 6-2-4 display form;
2. uppercases letters without numeric coercion, zero-padding, or Lee STRAP
   rewriting;
3. opens the configured jurisdiction/module portal anonymously (including
   Plantation's named `ACAFrame` wrapper);
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
  marker; or
- `non_permit_records_only`: Accela returned records, but every result carried
  an explicit different module (for example Plantation Enforcement), so none
  was normalized as a permit.

Cross-module rows remain counted in summary provenance. They are excluded from
permit details and also count toward reconciliation with Accela's reported
total.

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

## Vendor-wide date-window enumeration

`scripts/run-broward-accela-date-windows.mjs` replaces one-search-per-property
discovery for the four Accela tenants that expose start/end dates:

- Hollywood;
- Plantation;
- Cooper City; and
- Weston.

Each source uses one persistent browser process. Initial non-overlapping
windows split recursively when Accela reports at least 100 rows; terminal
windows paginate every visible page and reconcile permit plus explicitly
excluded cross-module rows to the reported total. Raw HTML, terminal list
records, and checkpoints are private mode-0600 artifacts. Fort Lauderdale is
excluded because its Accela form has no date controls and its official
FeatureServer is the list-discovery source.

The Hollywood live pilot for `2026-08-30..2026-08-31` exposed temporary
records without clickable detail anchors. Those rows still carry a full hidden
three-part `RecordId`; the parser now builds the official cap-detail identity
from that value. The corrected pilot reconciled **44/44** changing live records
across five pages. Equivalent pilots reconciled:

- Plantation: 26 permits plus 13 explicit Enforcement rows = 39/39;
- Cooper City: 19/19 permits; and
- Weston: 14/14 permits.

Full persistent local workers use:

```bash
npm run broward:permits:run-accela-windows -- \
  --source <hollywood|plantation|cooper-city|weston> \
  --start-date <explicit-source-boundary> \
  --end-date 2026-08-31 \
  --window-days 30 \
  --split-threshold 100 \
  --max-pages 200 \
  --delay-ms 1000 \
  --output-dir downloads/broward/accela-date-windows/<source>-full
```

The AWS template adds a disabled-by-default encrypted FIFO enumeration queue.
Its `MessageGroupId` is the jurisdiction key, so each tenant is serialized
while different tenants can run concurrently. The event-source mapping starts
at aggregate concurrency four and is not enabled before an AWS pilot. Cloud
deployment is currently blocked because this VM has no usable AWS credentials;
the four equivalent local workers are running instead.

The 30-day production burn-in added two fail-closed source rules:

- incomplete multi-day pagination is split again until the children reconcile;
  Weston exercised this path; and
- a single day that still reaches a portal cap cannot split by date. Plantation
  has at least one such day and therefore requires sequential record-type
  shards or an official bulk export before its inventory can be called
  complete.

## Bounded live evidence

Live source traffic was limited to one validated appraisal folio in Hollywood,
Plantation, Cooper City, and Weston, and two in Fort Lauderdale:

| Jurisdiction / folio           | Source outcome      |            Listed | Excluded non-permit | Permit details captured |
| ------------------------------ | ------------------- | ----------------: | ------------------: | ----------------------: |
| Hollywood `514111160200`       | records             |                 1 |                   0 |                       1 |
| Plantation `504108BJ0140`      | records             |                10 |       3 Enforcement |                       7 |
| Fort Lauderdale `494209060010` | bounded stop        | 50 across 3 pages |                   0 |                       0 |
| Fort Lauderdale `494212072320` | records             |                 7 |                   0 |                       7 |
| Cooper City `514106100100`     | explicit no records |                 0 |                   0 |                       0 |
| Weston `503912010490`          | records             |                 6 |                   0 |                       6 |

The completed detail records were:

- Hollywood: `STRUC-ROOF-25-000925`.
- Plantation: `B03-04467`, `B17-04514`, `B18-03653`, `B22-00489`,
  `B22-03630`, `E17-01256`, and `P17-01009`.
- Fort Lauderdale: `BLD-FEN-21120006`, `BLD-GEN-24040900`,
  `BLD-ROOF-WT-25080033`, `PM-02030921`, `PM-02071619`, `PM-02091931`,
  and `PM-05012277`.
- Weston: `B24-02326`, `R17-00107`, `R17-00173`, `STR-065083-0`,
  `STR-1222345-0`, and `STR-1633266-0`.

The two private outputs contain 21 normalized permit details. Plantation's
three `Module=Enforcement` results remain count provenance but were not
misclassified as permits. The first LauderBuild folio was fully paginated and
reconciled at 50 links, then failed closed before detail traversal because it
exceeded the configured 20-detail probe cap. The second validated Fort
Lauderdale folio supplied bounded list/detail evidence instead; the cap was
not raised.

All five official Accela portals were anonymously reachable. There was no
login, CAPTCHA, or network access blocker. The remaining Fort Lauderdale
blocker is the deliberate per-folio detail safety limit, not source access.
Hollywood's legacy address source was not queried and remains separately
scoped. Cooper City's explicit no-records result is not a historical
completeness claim because its earliest online date is unknown.

The resumable run skipped already completed Hollywood, Cooper City, and Weston
targets, resumed Plantation after source-contract corrections, and retained
the three LauderBuild list pages for the over-limit target. Raw HTML,
normalized JSONL, summaries, and checkpoints stayed under
`/tmp/broward-accela-probe-afa4/` as local private artifacts. No AWS,
database, publication, or active appraisal process was touched.
