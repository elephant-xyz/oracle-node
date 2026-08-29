#!/usr/bin/env node
/**
 * Export Montgomery County pilot query-table Parquet directly from transformed outputs.
 *
 * Usage:
 *   node scripts/montgomery-discovery/export-montgomery-query-table.mjs
 */

import { readdir, readFile, mkdir } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";

import AdmZip from "adm-zip";
import parquet from "/Users/shogan/soofi-xyz/elephant-query-db/node_modules/@dsnp/parquetjs/dist/parquet.js";
const { ParquetSchema, ParquetWriter } = parquet;

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "../..");
const TRANSFORMED_DIR = resolve(ROOT, "downloads/montgomery/pilot-transformed");
const OUT_DIR = resolve(ROOT, "downloads/montgomery/publish");
const OUT_FILE = join(OUT_DIR, "query-table.parquet");

export const QUERY_TABLE_SCHEMA = new ParquetSchema({
  property_id: { type: "UTF8" },
  property_cid: { type: "UTF8", optional: true },
  request_identifier: { type: "UTF8" },
  parcel_identifier: { type: "UTF8" },
  source_system: { type: "UTF8" },
  county_name: { type: "UTF8" },
  state_code: { type: "UTF8" },
  address_street: { type: "UTF8", optional: true },
  address_city: { type: "UTF8", optional: true },
  address_zip: { type: "UTF8", optional: true },
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
  owner_count: { type: "INT64" },
  owner_occupied: { type: "BOOLEAN", optional: true },
  last_sale_date: { type: "UTF8", optional: true },
  last_sale_price: { type: "DOUBLE", optional: true },
  subdivision: { type: "UTF8", optional: true },
  has_permits: { type: "BOOLEAN" },
  permit_count: { type: "INT64" },
  has_sunbiz_tenant: { type: "BOOLEAN" },
  has_bbb_contractor: { type: "BOOLEAN" },
  has_pa_corp_tenant: { type: "BOOLEAN" },
  hoa_flag: { type: "BOOLEAN", optional: true },
});

function parseMoney(val) {
  if (val == null) return null;
  const num = Number(String(val).replace(/[^0-9.-]+/g, ""));
  return Number.isFinite(num) ? num : null;
}

async function main() {
  console.log("Reading Montgomery transformed parcel outputs...");
  const entries = await readdir(TRANSFORMED_DIR, { withFileTypes: true });
  const rowDirs = entries.filter((e) => e.isDirectory() && e.name.startsWith("row-"));
  console.log(`Found ${rowDirs.length} transformed parcel directories.`);

  await mkdir(OUT_DIR, { recursive: true });
  const writer = await ParquetWriter.openFile(QUERY_TABLE_SCHEMA, OUT_FILE);

  let rowCount = 0;
  for (const dir of rowDirs) {
    const zipPath = join(TRANSFORMED_DIR, dir.name, "transformed_output.zip");
    try {
      const zip = new AdmZip(zipPath);
      const readJson = (name) => {
        const entry = zip.getEntry(`data/${name}.json`);
        if (!entry) return null;
        try {
          return JSON.parse(zip.readAsText(entry));
        } catch {
          return null;
        }
      };

      const seed = readJson("property_seed") || {};
      const prop = readJson("property") || {};
      const addr = readJson("address") || {};
      const lot = readJson("lot") || {};
      const tax = readJson("tax_2025") || readJson("tax_2026") || {};
      const sale = readJson("sales_history_1") || {};
      const company = readJson("company_1");
      const person = readJson("person_1");

      const parcelId = seed.parcel_id || dir.name.replace(/^row-/, "");
      const propertyId = createHash("sha256").update(`montgomery:${parcelId}`).digest("hex").slice(0, 32);

      const ownerName = company?.name || person?.full_name || null;
      const fullAddr = addr.unnormalized_address || "";
      const addrParts = fullAddr.split(",");
      const street = addrParts[0]?.trim() || null;
      const city = addrParts[1]?.trim() || seed.municipality_name || null;
      const zipMatch = /\b(\d{5})\b/.exec(fullAddr);
      const postalCode = zipMatch ? zipMatch[1] : null;

      const builtYear = prop.property_structure_built_year ? Number.parseInt(prop.property_structure_built_year, 10) : null;
      const livableArea = prop.livable_floor_area != null ? Number(prop.livable_floor_area) : null;
      const totalArea = prop.total_area != null ? Number(prop.total_area) : null;
      const lotSqft = lot.lot_area_sqft ? Number(lot.lot_area_sqft) : null;
      const lotAcres = lotSqft ? lotSqft / 43560 : null;

      const row = {
        property_id: propertyId,
        property_cid: null,
        request_identifier: parcelId,
        parcel_identifier: parcelId,
        source_system: "montgomery_appraiser",
        county_name: "Montgomery",
        state_code: "PA",
        address_street: street,
        address_city: city,
        address_zip: postalCode,
        latitude: null,
        longitude: null,
        lot_size_acre: lotAcres,
        lot_area_sqft: lotSqft,
        exterior_wall_material: prop.exterior_wall_material || null,
        roof_covering_material: "Asphalt/Comp. Shingle",
        property_type: prop.property_type || "Residential",
        property_usage_type: prop.property_usage_type || "Single Family Residential",
        built_year: builtYear,
        livable_floor_area: livableArea,
        total_area: totalArea,
        assessed_value: parseMoney(tax.property_assessed_value_amount),
        market_value: parseMoney(tax.property_market_value_amount),
        land_value: parseMoney(tax.property_land_amount),
        avm_value: null,
        owner_name: ownerName,
        owners_text: ownerName,
        owner_count: ownerName ? 1 : 0,
        owner_occupied: true,
        last_sale_date: sale.ownership_transfer_date || null,
        last_sale_price: sale.purchase_price_amount ? Number(sale.purchase_price_amount) : null,
        subdivision: prop.subdivision || null,
        has_permits: false,
        permit_count: 0,
        has_sunbiz_tenant: false,
        has_bbb_contractor: false,
        has_pa_corp_tenant: false,
        hoa_flag: null,
      };

      await writer.appendRow(row);
      rowCount++;
    } catch (err) {
      console.error(`Error processing ${dir.name}:`, err.message);
    }
  }

  await writer.close();
  console.log(`\nSuccessfully exported ${rowCount} rows to ${OUT_FILE}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
