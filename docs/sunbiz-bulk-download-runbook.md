# Sunbiz Bulk Download Runbook

Date recorded: 2026-05-26

## What was downloaded

- Source: Florida Sunbiz official Data Access Portal, corporate quarterly folder `doc > quarterly > cor`.
- Quarterly corporate data file: `cordata.zip`.
- Observed compressed size: `1,744,668,408` bytes.
- Related quarterly event file present but not processed yet: `corevent.zip`, about `187 MB`.
- Daily corporate files were also visible under `doc > cor`; the successful local smoke used `20260522c.txt`.

## Access path used

Plain `curl` against Sunbiz still received a Cloudflare managed challenge, but headless Chromium/Puppeteer could complete the browser challenge and use the portal normally.

Operational steps used:

1. Open the Sunbiz Data Access Portal in headless Chromium/Puppeteer.
2. Log in with the portal's public access account credentials supplied out-of-band. Do not commit the password to this repository.
3. Navigate the portal tree to `Public/doc/quarterly/cor`.
4. Download `cordata.zip` through the browser session.
5. Upload the downloaded file to S3:

   ```bash
   AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \
     aws s3 cp downloads/sunbiz/cordata.zip \
       s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-source/quarterly/cor/cordata-2026q2.zip
   ```

## Deflate64 workaround

The official `cordata.zip` uses ZIP compression method 9 (Deflate64). The deployed worker's `yauzl` reader cannot stream that compression method, so the production path expanded the ZIP locally with system `unzip` and uploaded the ten fixed-width text entries.

Expanded source objects:

```text
s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-source/quarterly/cor/cordata-2026q2-expanded/cordata0.txt
...
s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-source/quarterly/cor/cordata-2026q2-expanded/cordata9.txt
```

The upload pattern was:

```bash
unzip downloads/sunbiz/cordata.zip -d downloads/sunbiz/cordata-2026q2-expanded

for file in downloads/sunbiz/cordata-2026q2-expanded/cordata*.txt; do
  AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \
    aws s3 cp "$file" \
      "s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-source/quarterly/cor/cordata-2026q2-expanded/$(basename "$file")"
done
```

## Extraction result

Each expanded text object was processed with the `sunbiz-corporate-zip-extract` worker mode, matching Lee-area ZIP prefixes across principal, mailing, registered-agent, and officer addresses.

Result manifest:

```text
s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-lee-corporate-quarterly-2026q2-expanded/sunbiz/corporate-by-zip/manifest.json
```

Result totals:

- text entries scanned: `10`
- source corporate records read: `12,607,458`
- invalid records: `0`
- Lee-area ZIP-matched corporate records: `379,467`
- JSONL chunks emitted: `80`

## Lexicon transform invocation

After adding the business-registration model, the extracted JSONL chunks were transformed with:

```bash
AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \
  node scripts/transform-sunbiz-corporate-to-lexicon.mjs \
    --manifest-s3-uri s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-lee-corporate-quarterly-2026q2-expanded/sunbiz/corporate-by-zip/manifest.json \
    --output-s3-uri s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-lee-corporate-quarterly-2026q2-expanded/lexicon-transform/business-registration-v1 \
    --part-record-limit 100000
```

Transform summary:

```text
s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-lee-corporate-quarterly-2026q2-expanded/lexicon-transform/business-registration-v1/summary.json
```
