#!/usr/bin/env node

/**
 * Publish gate for a places-table parquet: row count vs current Neon
 * `business_locations`, unique GERS ids, null geometries, live licence
 * assertion, and `/`-delimited `taxonomy.hierarchy`. Does not upload.
 *
 *   node scripts/validate-overture-places-table.mjs \
 *     --from-neon \
 *     --env-file ../elephant-query-db/.env.local \
 *     --county lee --release 2026-07-22.0 \
 *     --parquet downloads/overture-places/lee/2026-07-22.0/publish/lee/places-table.parquet
 */

import { fileURLToPath } from "node:url";
import * as path from "node:path";

import { runValidate } from "./export-overture-places-table.mjs";

if (
  process.argv[1] !== undefined &&
  fileURLToPath(import.meta.url) === path.resolve(process.argv[1])
) {
  runValidate(process.argv.slice(2)).catch((caught) => {
    const message = caught instanceof Error ? caught.message : String(caught);
    process.stderr.write(
      `${JSON.stringify({ event: "overture_places_validate_failed", error: message })}\n`,
    );
    process.exitCode = 1;
  });
}
