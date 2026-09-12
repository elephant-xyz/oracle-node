#!/usr/bin/env node

import { createHash } from "node:crypto";
import {
  mkdir,
  readFile,
  readdir,
  rename,
  rm,
  writeFile,
} from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";

import { parseCsvRecords } from "../run-pinellas-local-ingest.mjs";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

const QUERY_TABLE_SCHEMA = new ParquetSchema({
  property_id: { type: "UTF8" },
  property_cid: { type: "UTF8", optional: true },
  request_identifier: { type: "UTF8", optional: true },
  parcel_identifier: { type: "UTF8", optional: true },
  source_system: { type: "UTF8", optional: true },
  county_name: { type: "UTF8", optional: true },
  state_code: { type: "UTF8", optional: true },
  address_street: { type: "UTF8", optional: true },
  address_city: { type: "UTF8", optional: true },
  address_zip: { type: "UTF8", optional: true },
  elephant_uuid: { type: "UTF8", optional: true },
  elephant_token: { type: "UTF8", optional: true },
  latitude: { type: "DOUBLE", optional: true },
  longitude: { type: "DOUBLE", optional: true },
  lot_size_acre: { type: "DOUBLE", optional: true },
  lot_area_sqft: { type: "DOUBLE", optional: true },
  exterior_wall_material: { type: "UTF8", optional: true },
  roof_covering_material: { type: "UTF8", optional: true },
  property_type: { type: "UTF8", optional: true },
  property_usage_type: { type: "UTF8", optional: true },
  built_year: { type: "INT64", optional: true },
  livable_floor_area: { type: "DOUBLE", optional: true },
  total_area: { type: "DOUBLE", optional: true },
  assessed_value: { type: "DOUBLE", optional: true },
  market_value: { type: "DOUBLE", optional: true },
  land_value: { type: "DOUBLE", optional: true },
  avm_value: { type: "DOUBLE", optional: true },
  owner_name: { type: "UTF8", optional: true },
  owners_text: { type: "UTF8", optional: true },
  owner_count: { type: "INT64", optional: true },
  owner_occupied: { type: "BOOLEAN", optional: true },
  last_sale_date: { type: "UTF8", optional: true },
  last_sale_price: { type: "DOUBLE", optional: true },
  subdivision: { type: "UTF8", optional: true },
  has_permits: { type: "BOOLEAN", optional: true },
  permit_count: { type: "INT64", optional: true },
  has_sunbiz_tenant: { type: "BOOLEAN", optional: true },
  has_bbb_contractor: { type: "BOOLEAN", optional: true },
  has_pa_corp_tenant: { type: "BOOLEAN", optional: true },
  hoa_flag: { type: "BOOLEAN", optional: true },
});

export function parseOptions(argv) {
  const values = new Map();
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token?.startsWith("--")) continue;
    const [inlineKey, inlineValue] = token.slice(2).split("=", 2);
    if (inlineValue !== undefined) {
      values.set(inlineKey, inlineValue);
      continue;
    }
    const next = argv[index + 1];
    if (next && !next.startsWith("--")) {
      values.set(inlineKey, next);
      index += 1;
    } else {
      values.set(inlineKey, "true");
    }
  }
  const county = values.get("county");
  if (!county) throw new Error("--county is required");
  return {
    county,
    countyName:
      values.get("county-name") ??
      county
        .split("-")
        .map((part) => `${part.slice(0, 1).toUpperCase()}${part.slice(1)}`)
        .join(" "),
    seedPath: values.get("seed") ?? `data/seeds/${county}.csv`,
    ingestDirectory:
      values.get("ingest-dir") ?? `downloads/${county}/local-ingest`,
    outputDirectory:
      values.get("out-dir") ?? `data/artifacts/publish/${county}`,
    allowMissing: values.get("allow-missing") === "true",
  };
}

function text(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed || null;
}

function number(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function propertyId(sourceSystem, requestIdentifier) {
  const digest = createHash("sha1")
    .update(`${sourceSystem}:${requestIdentifier}`)
    .digest();
  const bytes = Buffer.from(digest.subarray(0, 16));
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export function parseAddress(value) {
  const parts = String(value ?? "")
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
  const street = parts[0] ?? null;
  const tail = parts.slice(1);
  const stateZip = tail.at(-1) ?? "";
  const zip = /\b(\d{5})(?:-\d{4})?\b/.exec(stateZip)?.[1] ?? null;
  const city = tail.slice(0, -1).join(", ") || null;
  return { street, city, zip };
}

function ownerName(record) {
  return (
    text(record.name) ??
    text(record.full_name) ??
    text(record.company_name) ??
    [record.first_name, record.middle_name, record.last_name]
      .map(text)
      .filter(Boolean)
      .join(" ") ??
    null
  );
}

async function readJson(filePath) {
  try {
    return JSON.parse(await readFile(filePath, "utf8"));
  } catch (error) {
    if (error?.code === "ENOENT") return null;
    throw error;
  }
}

async function loadParcel(parcelDirectory) {
  const dataDirectory = path.join(parcelDirectory, "data");
  const files = new Map();
  for (const name of await readdir(dataDirectory)) {
    if (!name.endsWith(".json") || name.startsWith("relationship_")) continue;
    const record = await readJson(path.join(dataDirectory, name));
    if (record && typeof record === "object" && !Array.isArray(record)) {
      files.set(name, record);
    }
  }
  return {
    files,
    identity: await readJson(path.join(parcelDirectory, "identity.json")),
    captureAddress: await readJson(
      path.join(parcelDirectory, "unnormalized_address.json"),
    ),
  };
}

function recordsMatching(files, expression) {
  return [...files.entries()]
    .filter(([name]) => expression.test(name))
    .map(([, record]) => record);
}

export function rowFromArtifacts({
  county,
  countyName,
  seedRow,
  files,
  identity,
  captureAddress,
}) {
  const requestIdentifier = text(seedRow.parcel_id);
  if (!requestIdentifier) throw new Error("Seed row has no parcel_id");
  const sourceSystem = `${county.replaceAll("-", "_")}_appraiser`;
  const property = files.get("property.json") ?? {};
  const parcel = files.get("parcel.json") ?? {};
  const address = files.get("address.json") ?? {};
  const lot = files.get("lot.json") ?? {};
  const geometry =
    files.get("geometry.json") ??
    recordsMatching(files, /^geometry_parcel_.*\.json$/).find(
      (record) =>
        number(record.latitude) !== null && number(record.longitude) !== null,
    ) ??
    {};
  const structures = recordsMatching(files, /^structure(?:_\d+)?\.json$/);
  const structure =
    structures.find(
      (record) =>
        text(record.exterior_wall_material_primary) ||
        text(record.roof_covering_material),
    ) ??
    structures[0] ??
    {};
  const taxes = recordsMatching(files, /^tax_\d+\.json$/).sort(
    (left, right) => (number(right.tax_year) ?? 0) - (number(left.tax_year) ?? 0),
  );
  const sales = recordsMatching(files, /^sales_history_\d+\.json$/).sort(
    (left, right) =>
      String(right.ownership_transfer_date ?? "").localeCompare(
        String(left.ownership_transfer_date ?? ""),
      ),
  );
  const owners = recordsMatching(files, /^(person|company)_\d+\.json$/)
    .map(ownerName)
    .filter(Boolean);
  const uniqueOwners = [...new Set(owners)];
  const situs = parseAddress(
    seedRow.opendoor_address ??
      address.unnormalized_address ??
      captureAddress?.full_address ??
      seedRow.situs_address,
  );
  const uuid = text(identity?.elephant_uuid ?? seedRow.elephant_uuid);
  const token = text(identity?.elephant_token ?? seedRow.elephant_token)?.replace(
    /^address:v1:/,
    "",
  );
  if (!uuid || !/^[0-9a-f]{64}$/i.test(token ?? "")) {
    throw new Error(`Invalid Elephant identity for ${requestIdentifier}`);
  }
  const lotAreaSqft = number(lot.lot_area_sqft);
  const permitCount = recordsMatching(
    files,
    /^property_improvement_\d+\.json$/,
  ).length;
  return {
    property_id: propertyId(sourceSystem, requestIdentifier),
    property_cid: null,
    request_identifier: requestIdentifier,
    parcel_identifier:
      text(property.parcel_identifier) ??
      text(parcel.parcel_identifier) ??
      requestIdentifier,
    source_system: sourceSystem,
    county_name: countyName,
    state_code: "FL",
    address_street: situs.street,
    address_city: situs.city,
    address_zip: situs.zip,
    elephant_uuid: uuid,
    elephant_token: token,
    latitude: number(geometry.latitude) ?? number(seedRow.latitude),
    longitude: number(geometry.longitude) ?? number(seedRow.longitude),
    lot_size_acre:
      number(lot.lot_size_acre) ??
      (lotAreaSqft === null ? null : lotAreaSqft / 43_560),
    lot_area_sqft: lotAreaSqft,
    exterior_wall_material:
      text(structure.exterior_wall_material_primary) ??
      text(structure.exterior_wall_material),
    roof_covering_material: text(structure.roof_covering_material),
    property_type: text(property.property_type),
    property_usage_type: text(property.property_usage_type),
    built_year: Math.trunc(number(property.property_structure_built_year) ?? 0) || null,
    livable_floor_area:
      number(property.livable_floor_area) ?? number(property.area_under_air),
    total_area: number(property.total_area),
    assessed_value: number(taxes[0]?.property_assessed_value_amount),
    market_value: number(taxes[0]?.property_market_value_amount),
    land_value: number(taxes[0]?.property_land_amount),
    avm_value: null,
    owner_name: uniqueOwners[0] ?? null,
    owners_text: uniqueOwners.length ? uniqueOwners.join(" | ") : null,
    owner_count: uniqueOwners.length || null,
    owner_occupied: null,
    last_sale_date: text(sales[0]?.ownership_transfer_date),
    last_sale_price: number(sales[0]?.purchase_price_amount),
    subdivision: text(property.subdivision),
    has_permits: permitCount > 0,
    permit_count: permitCount,
    has_sunbiz_tenant: false,
    has_bbb_contractor: false,
    has_pa_corp_tenant: false,
    hoa_flag: null,
  };
}

function sparse(row) {
  return Object.fromEntries(
    Object.entries(row).filter(([, value]) => value !== null && value !== undefined),
  );
}

export async function exportTargetedQueryTable(options) {
  const seedPath = path.resolve(ROOT, options.seedPath);
  const ingestDirectory = path.resolve(ROOT, options.ingestDirectory);
  const outputDirectory = path.resolve(ROOT, options.outputDirectory);
  const seedRows = parseCsvRecords(await readFile(seedPath, "utf8"));
  const seedByParcel = new Map(
    seedRows
      .filter((row) => text(row.parcel_id))
      .map((row) => [row.parcel_id, row]),
  );
  const missing = [];
  const rows = [];
  for (const [parcelId, seedRow] of seedByParcel) {
    const parcelDirectory = path.join(ingestDirectory, parcelId);
    const property = await readJson(
      path.join(parcelDirectory, "data", "property.json"),
    );
    if (!property) {
      missing.push(parcelId);
      continue;
    }
    rows.push(
      rowFromArtifacts({
        county: options.county,
        countyName: options.countyName,
        seedRow,
        ...(await loadParcel(parcelDirectory)),
      }),
    );
  }
  if (missing.length && !options.allowMissing) {
    throw new Error(
      `Missing property.json for ${missing.length} parcels: ${missing.slice(0, 8).join(", ")}`,
    );
  }
  const requestIds = rows.map((row) => row.request_identifier);
  const uuids = rows.map((row) => row.elephant_uuid);
  const tokens = rows.map((row) => row.elephant_token);
  if (
    new Set(requestIds).size !== rows.length ||
    new Set(uuids).size !== rows.length ||
    new Set(tokens).size !== rows.length
  ) {
    throw new Error("Query table has duplicate folios or Elephant identities");
  }
  await mkdir(outputDirectory, { recursive: true });
  const parquetPath = path.join(outputDirectory, "query-table.parquet");
  const temporaryPath = `${parquetPath}.${process.pid}.tmp`;
  await rm(temporaryPath, { force: true });
  const writer = await ParquetWriter.openFile(QUERY_TABLE_SCHEMA, temporaryPath);
  try {
    for (const row of rows) await writer.appendRow(sparse(row));
    await writer.close();
    await rename(temporaryPath, parquetPath);
  } catch (error) {
    await writer.close().catch(() => {});
    await rm(temporaryPath, { force: true });
    throw error;
  }
  const parquetBytes = await readFile(parquetPath);
  const exportedAt = new Date().toISOString();
  const report = {
    county: options.county,
    exportedAt,
    rowCount: rows.length,
    expectedCount: seedRows.length,
    missingCount: missing.length + (seedRows.length - seedByParcel.size),
    missing,
    distinctRequestIdentifiers: new Set(requestIds).size,
    completeIdentityPairs: rows.length,
    subdivisionNonNull: rows.filter((row) => row.subdivision).length,
    propertyCidNonNull: rows.filter((row) => row.property_cid).length,
    fileSizeBytes: parquetBytes.byteLength,
    sha256: createHash("sha256").update(parquetBytes).digest("hex"),
    publicationScope: {
      schemaVersion: "1.0",
      level: "partial",
      denominatorBasis: "published_subset",
    },
    parquetPath,
  };
  await writeFile(
    path.join(outputDirectory, "export-report.json"),
    `${JSON.stringify(report, null, 2)}\n`,
  );
  return report;
}

async function main() {
  const report = await exportTargetedQueryTable(
    parseOptions(process.argv.slice(2)),
  );
  console.log(JSON.stringify(report, null, 2));
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.stack : error);
    process.exitCode = 1;
  });
}
