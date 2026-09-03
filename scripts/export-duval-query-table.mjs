#!/usr/bin/env node
/**
 * Duval Task 8: write the 50-row Donphan query table from the Task 6 pilot run.
 *
 * After export, overlay the parquet on Elephant MCP (outside this repo):
 *   PROPERTY_QUERY_TABLE_MAP_ADDITIONS={"duval":"<repo>/downloads/duval/pilot-query-table.parquet"}
 * Then queryProperties county=duval (row count 50, use-code breakdown, named parcel).
 * Filebase/IPNS publish is out of scope.
 *
 * Usage:
 *   node scripts/export-duval-query-table.mjs
 *   node scripts/export-duval-query-table.mjs --output=downloads/duval/pilot-run --parquet=downloads/duval/pilot-query-table.parquet
 */

import { mkdir, readdir, rename, rm } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import parquet from "@dsnp/parquetjs";

import { parsePilotArgs } from "./hillsborough/lib.mjs";
import {
  DUVAL_QUERY_TABLE_SCHEMA,
  assertQueryTableIds,
  isCompleteDuvalParcel,
  loadDuvalParcelArtifacts,
  rowFromDuvalArtifacts,
} from "./duval/query-table-lib.mjs";

const { ParquetWriter } = parquet;

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "..");

/**
 * @param {string[]} argv
 * @returns {{ pilotRoot: string; parquetPath: string; limit: number | null }}
 */
export function parseDuvalQueryTableArgs(argv) {
  const shared = parsePilotArgs(argv);
  const parquetPath =
    argv.find((arg) => arg.startsWith("--parquet="))?.split("=")[1] ??
    "downloads/duval/pilot-query-table.parquet";
  return {
    pilotRoot: shared.outputRoot ?? "downloads/duval/pilot-run",
    parquetPath,
    limit: shared.limit,
  };
}

/**
 * @param {{ pilotRoot: string; parquetPath: string; limit?: number | null }} options
 */
export async function exportDuvalQueryTable(options) {
  const pilotRoot = resolve(ROOT, options.pilotRoot);
  const parquetPath = resolve(ROOT, options.parquetPath);
  await mkdir(dirname(parquetPath), { recursive: true });

  const entries = await readdir(pilotRoot, { withFileTypes: true });
  const candidateFolios = entries
    .filter((entry) => entry.isDirectory() && !entry.name.startsWith("_"))
    .map((entry) => entry.name)
    .sort();
  /** @type {string[]} */
  const completeFolios = [];
  for (const folio of candidateFolios) {
    if (await isCompleteDuvalParcel(join(pilotRoot, folio))) {
      completeFolios.push(folio);
    }
  }
  const expected = options.limit ?? 50;
  const folios =
    options.limit == null
      ? completeFolios
      : completeFolios.slice(0, options.limit);

  /** @type {Array<Record<string, unknown>>} */
  const rows = [];
  for (const folio of folios) {
    const artifacts = await loadDuvalParcelArtifacts(
      join(pilotRoot, folio),
      folio,
    );
    rows.push(rowFromDuvalArtifacts(artifacts));
  }
  assertQueryTableIds(rows, expected);

  const temporaryPath = `${parquetPath}.${process.pid}.tmp`;
  await rm(temporaryPath, { force: true });
  const writer = await ParquetWriter.openFile(
    DUVAL_QUERY_TABLE_SCHEMA,
    temporaryPath,
  );
  try {
    for (const row of rows) {
      await writer.appendRow(row);
    }
    await writer.close();
    await rename(temporaryPath, parquetPath);
  } catch (error) {
    await writer.close().catch(() => {});
    await rm(temporaryPath, { force: true });
    throw error;
  }

  return { parquetPath, rowCount: rows.length };
}

async function main() {
  const result = await exportDuvalQueryTable(
    parseDuvalQueryTableArgs(process.argv.slice(2)),
  );
  console.log(
    JSON.stringify({ event: "duval_query_table_exported", ...result }),
  );
}

const isMain =
  process.argv[1] &&
  fileURLToPath(import.meta.url) === resolve(process.argv[1]);
if (isMain) {
  main().catch((error) => {
    console.error(
      error instanceof Error ? (error.stack ?? error.message) : error,
    );
    process.exitCode = 1;
  });
}
