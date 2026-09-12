#!/usr/bin/env node

import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { ParquetReader } from "@dsnp/parquetjs";

import { parseCsvRecords } from "../run-pinellas-local-ingest.mjs";

const ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../..",
);

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function normalizedIdentifier(value) {
  return String(value ?? "")
    .replace(/[^a-z0-9]/gi, "")
    .toLowerCase();
}

async function readRows(parquetPath) {
  const reader = await ParquetReader.openFile(parquetPath);
  const rows = [];
  try {
    const cursor = reader.getCursor();
    for (let row = await cursor.next(); row; row = await cursor.next()) {
      rows.push(row);
    }
  } finally {
    await reader.close();
  }
  return rows;
}

async function validateCounty(county) {
  const seedPath = path.join(ROOT, "data", "seeds", `${county}.csv`);
  const publishDirectory = path.join(
    ROOT,
    "data",
    "artifacts",
    "publish",
    county,
  );
  const parquetPath = path.join(publishDirectory, "query-table.parquet");
  const report = JSON.parse(
    await readFile(path.join(publishDirectory, "export-report.json"), "utf8"),
  );
  const seedRows = parseCsvRecords(await readFile(seedPath, "utf8"));
  const seedByParcel = new Map(seedRows.map((row) => [row.parcel_id, row]));
  const rows = await readRows(parquetPath);
  const bytes = await readFile(parquetPath);
  const sha256 = createHash("sha256").update(bytes).digest("hex");

  assert(
    rows.length === seedRows.length,
    `${county}: Parquet row count mismatch`,
  );
  assert(
    report.rowCount === rows.length,
    `${county}: report row count mismatch`,
  );
  assert(report.sha256 === sha256, `${county}: Parquet hash mismatch`);
  assert(
    report.fileSizeBytes === bytes.byteLength,
    `${county}: Parquet size mismatch`,
  );

  const requestIds = new Set();
  const uuids = new Set();
  const tokens = new Set();
  for (const row of rows) {
    const seed = seedByParcel.get(row.request_identifier);
    assert(seed, `${county}: unexpected parcel ${row.request_identifier}`);
    const expectedParcelIdentifier =
      county === "martin" ? seed.fdor_parcelno : seed.parcel_id;
    assert(
      normalizedIdentifier(row.parcel_identifier) ===
        normalizedIdentifier(expectedParcelIdentifier),
      `${county}: parcel identity mismatch for ${seed.parcel_id}`,
    );
    assert(
      row.elephant_uuid === seed.elephant_uuid,
      `${county}: Elephant UUID mismatch for ${seed.parcel_id}`,
    );
    assert(
      row.elephant_token === seed.elephant_token.replace(/^address:v1:/, ""),
      `${county}: Elephant token mismatch for ${seed.parcel_id}`,
    );
    assert(
      row.source_system === `${county.replaceAll("-", "_")}_appraiser`,
      `${county}: source system mismatch for ${seed.parcel_id}`,
    );
    assert(row.state_code === "FL", `${county}: state mismatch`);
    assert(
      row.address_zip === seed.postal_code,
      `${county}: ZIP mismatch for ${seed.parcel_id}`,
    );
    assert(
      Number.isFinite(row.latitude) &&
        row.latitude >= 24 &&
        row.latitude <= 32 &&
        Number.isFinite(row.longitude) &&
        row.longitude >= -88 &&
        row.longitude <= -79,
      `${county}: invalid Florida coordinates for ${seed.parcel_id}`,
    );
    requestIds.add(row.request_identifier);
    uuids.add(row.elephant_uuid);
    tokens.add(row.elephant_token);
  }
  assert(requestIds.size === rows.length, `${county}: duplicate parcel`);
  assert(uuids.size === rows.length, `${county}: duplicate Elephant UUID`);
  assert(tokens.size === rows.length, `${county}: duplicate Elephant token`);

  return {
    county,
    rows: rows.length,
    bytes: bytes.byteLength,
    sha256,
    propertyTypeNonNull: rows.filter((row) => row.property_type).length,
    propertyUsageTypeNonNull: rows.filter((row) => row.property_usage_type)
      .length,
  };
}

async function main() {
  const counties = process.argv.slice(2);
  assert(counties.length > 0, "at least one county is required");
  const results = [];
  for (const county of counties) results.push(await validateCounty(county));
  console.log(
    JSON.stringify(
      {
        status: "PASS",
        countyCount: results.length,
        rowCount: results.reduce((sum, result) => sum + result.rows, 0),
        results,
      },
      null,
      2,
    ),
  );
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack : error);
  process.exitCode = 1;
});
