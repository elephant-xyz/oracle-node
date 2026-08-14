#!/usr/bin/env node

/**
 * Read-only public probe for Overture STAC and places changelog schema.
 *
 * This script performs no AWS or database writes. It resolves (or accepts) a
 * pinned release, asks DuckDB to describe the three processed changelog
 * partitions, validates the documented fields, and prints one small sample.
 */

import { createRequire } from "node:module";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

import { discoverRelease } from "./extract-overture-places.mjs";
import {
  assertOvertureChangelogSchema,
  overturePlacesChangelogGlobs,
} from "./overture-places-refresh-lib.mjs";

const require = createRequire(import.meta.url);
const duckdb = require("duckdb");

/**
 * @typedef {object} DuckdbConnection
 * @property {(sql: string) => Promise<Record<string, unknown>[]>} all Execute a query.
 * @property {(sql: string) => Promise<void>} exec Execute statements.
 * @property {() => Promise<void>} close Close the database.
 */

/**
 * Probe the live public changelog.
 *
 * @param {readonly string[]} argv CLI arguments.
 * @returns {Promise<Record<string, unknown>>} Machine-readable probe result.
 */
export async function runChangelogSchemaProbe(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      release: { type: "string" },
      "stac-catalog-url": { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });
  const stacUrl =
    typeof values["stac-catalog-url"] === "string"
      ? values["stac-catalog-url"]
      : "https://stac.overturemaps.org/catalog.json";
  const discovery = await discoverRelease(stacUrl);
  const release =
    typeof values.release === "string" ? values.release : discovery.latest;
  const globs = overturePlacesChangelogGlobs(release);
  const db = await openDuckdb();
  try {
    await db.exec("INSTALL httpfs; LOAD httpfs; SET s3_region = 'us-west-2';");
    const parquetList = globs
      .map((glob) => `'${glob.replaceAll("'", "''")}'`)
      .join(", ");
    const describe = await db.all(
      `DESCRIBE SELECT * FROM read_parquet([${parquetList}], hive_partitioning = 1)`,
    );
    const columns = describe.flatMap((row) =>
      typeof row.column_name === "string" ? [row.column_name] : [],
    );
    const schema = assertOvertureChangelogSchema(columns);
    const samples = await db.all(
      `SELECT id, bbox, change_type, theme, type
       FROM read_parquet([${parquetList}], hive_partitioning = 1)
       LIMIT 1`,
    );
    return {
      schemaVersion: "oracle-node.overture-changelog-probe.v1",
      stacLatest: discovery.latest,
      release,
      globs,
      columns: schema.columns,
      sample: samples[0] ?? null,
      passed: true,
      probedAt: new Date().toISOString(),
    };
  } finally {
    await db.close();
  }
}

/**
 * Open a promisified in-memory DuckDB connection.
 *
 * @returns {Promise<DuckdbConnection>} Connection wrapper.
 */
async function openDuckdb() {
  const db = new duckdb.Database(":memory:");
  return {
    all(sql) {
      return new Promise((resolve, reject) => {
        db.all(sql, (error, rows) => {
          if (error) reject(error);
          else resolve(rows ?? []);
        });
      });
    },
    exec(sql) {
      return new Promise((resolve, reject) => {
        db.exec(sql, (error) => {
          if (error) reject(error);
          else resolve();
        });
      });
    },
    close() {
      return new Promise((resolve, reject) => {
        db.close((error) => {
          if (error) reject(error);
          else resolve();
        });
      });
    },
  };
}

if (
  process.argv[1] !== undefined &&
  fileURLToPath(import.meta.url) === path.resolve(process.argv[1])
) {
  runChangelogSchemaProbe(process.argv.slice(2))
    .then((result) => {
      process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    })
    .catch((caught) => {
      const message = caught instanceof Error ? caught.message : String(caught);
      process.stderr.write(
        `${JSON.stringify({ event: "overture_changelog_probe_failed", error: message })}\n`,
      );
      process.exitCode = 1;
    });
}
