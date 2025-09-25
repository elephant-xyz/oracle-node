Place raw transform code here. Create one subdirectory per county (the folder name must match `county_jurisdiction` in lowercase from the prepare output). For example, if the `county_jurisdiction` is `Brevard` and `Lake`:

```
transform/
  brevard/
    scripts/
      data_extractor.js
      layoutMapping.js
  lake/
    scripts/
      ...
```

During deployment `./scripts/deploy-infra.sh` rebuilds a zip for each county directory, uploads the archives to the environment bucket, and updates the post Lambda’s `TRANSFORM_S3_PREFIX`. Do not commit pre-built zip files or run manual upload scripts—the deployment pipeline handles packaging and syncing automatically.
