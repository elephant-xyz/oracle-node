#!/usr/bin/env node

/**
 * Flatten validated Broward pilot artifacts into Donphan's query-table shape.
 */

import { mkdir, readFile, writeFile } from "fs/promises";
import path from "path";
import { pathToFileURL } from "url";
import AdmZip from "adm-zip";
import { ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";

import {
  requireParcelRecords,
  unwrapBrowardPrepareCapture,
} from "./capture-broward-parcel.mjs";

const DEFAULT_VALIDATION_DIRECTORY =
  "downloads/broward/appraisal-validation-50";
const DEFAULT_CAPTURES_PATH =
  "downloads/broward/broward-validation-sample-50-captures.zip";
const DEFAULT_OUTPUT_DIRECTORY = "downloads/broward/pilot-query";

/**
 * @typedef {Record<string, unknown>} JsonObject
 *
 * @typedef {object} QueryTableRow
 * @property {string} property_id - Stable pilot property identifier.
 * @property {string | null} property_cid - Consolidated property CID.
 * @property {string} request_identifier - Source request folio.
 * @property {string} parcel_identifier - Broward folio.
 * @property {string} source_system - County source key.
 * @property {string} county_name - County name.
 * @property {string} state_code - State code.
 * @property {string | null} address_street - Situs street.
 * @property {string | null} address_city - Situs city.
 * @property {string | null} address_zip - Situs ZIP.
 * @property {number | null} latitude - GIS centroid latitude.
 * @property {number | null} longitude - GIS centroid longitude.
 * @property {number | null} lot_size_acre - Lot acreage.
 * @property {number | null} lot_area_sqft - Lot square feet.
 * @property {string | null} exterior_wall_material - Primary wall material.
 * @property {string | null} roof_covering_material - Roof material.
 * @property {string | null} property_type - Lexicon property type.
 * @property {string | null} property_usage_type - Lexicon usage type.
 * @property {number | null} built_year - Structure year.
 * @property {number | null} livable_floor_area - Livable area.
 * @property {number | null} total_area - Total area.
 * @property {number | null} assessed_value - Assessed value.
 * @property {number | null} market_value - Market value.
 * @property {number | null} land_value - Land value.
 * @property {number | null} avm_value - AVM value.
 * @property {string | null} owner_name - Primary owner.
 * @property {string | null} owners_text - Searchable owner list.
 * @property {number} owner_count - Owner count.
 * @property {boolean | null} owner_occupied - Owner-occupied flag.
 * @property {string | null} last_sale_date - Latest sale date.
 * @property {number | null} last_sale_price - Latest sale amount.
 * @property {string | null} subdivision - Subdivision.
 * @property {boolean} has_permits - Whether permits are loaded.
 * @property {number} permit_count - Loaded permit count.
 * @property {boolean} has_sunbiz_tenant - Whether Sunbiz is linked.
 * @property {boolean} has_bbb_contractor - Whether BBB is linked.
 * @property {boolean | null} hoa_flag - HOA flag.
 */

/**
 * Read one JSON object from a transformed ZIP.
 *
 * @param {AdmZip} zip - Open transformed ZIP.
 * @param {string} entryName - Entry path.
 * @returns {JsonObject | null} Parsed object.
 */
function readZipObject(zip, entryName) {
  const entry = zip.getEntry(entryName);
  if (entry === null) return null;
  const value = /** @type {unknown} */ (
    JSON.parse(entry.getData().toString("utf8"))
  );
  return isObject(value) ? value : null;
}

/**
 * Return true for a non-array JSON object.
 *
 * @param {unknown} value - Candidate value.
 * @returns {value is JsonObject} Whether the value is an object.
 */
function isObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Read a nullable string.
 *
 * @param {unknown} value - Candidate value.
 * @returns {string | null} Trimmed string.
 */
function text(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed === "" ? null : trimmed;
}

/**
 * Read a nullable finite number.
 *
 * @param {unknown} value - Candidate value.
 * @returns {number | null} Finite number.
 */
function number(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Build one Donphan query row from transformed and source records.
 *
 * @param {object} params - Source records.
 * @param {string} params.folio - Canonical folio.
 * @param {JsonObject} params.property - Transformed property.
 * @param {JsonObject | null} params.address - Transformed address.
 * @param {JsonObject | null} params.lot - Transformed lot.
 * @param {JsonObject | null} params.tax - Transformed tax.
 * @param {JsonObject | null} params.structure - Transformed structure.
 * @param {JsonObject | null} params.sale - Latest transformed sale.
 * @param {JsonObject} params.sourceRecord - BCPA parcel record.
 * @param {readonly string[]} params.ownerNames - Transformed owner names.
 * @returns {QueryTableRow} Flat query-table row.
 */
export function buildQueryTableRow({
  folio,
  property,
  address,
  lot,
  tax,
  structure,
  sale,
  sourceRecord,
  ownerNames,
}) {
  const lotAreaSqft = number(lot?.lot_area_sqft);
  return {
    property_id: `broward:${folio}`,
    property_cid: null,
    request_identifier: folio,
    parcel_identifier: text(property.parcel_identifier) ?? folio,
    source_system: "broward_appraiser",
    county_name: text(address?.county_name) ?? "Broward",
    state_code: "FL",
    address_street: text(sourceRecord.situsAddress1),
    address_city: text(sourceRecord.situsCity),
    address_zip: text(sourceRecord.situsZipCode),
    latitude: number(address?.latitude),
    longitude: number(address?.longitude),
    lot_size_acre: lotAreaSqft === null ? null : lotAreaSqft / 43_560,
    lot_area_sqft: lotAreaSqft,
    exterior_wall_material: text(structure?.exterior_wall_material_primary),
    roof_covering_material: text(structure?.roof_covering_material),
    property_type: text(property.property_type),
    property_usage_type: text(property.property_usage_type),
    built_year: number(property.property_structure_built_year),
    livable_floor_area: number(property.livable_floor_area),
    total_area: number(property.total_area),
    assessed_value: number(tax?.property_assessed_value_amount),
    market_value: number(tax?.property_market_value_amount),
    land_value: number(tax?.property_land_amount),
    avm_value: null,
    owner_name: ownerNames[0] ?? null,
    owners_text: ownerNames.length > 0 ? ownerNames.join(" | ") : null,
    owner_count: ownerNames.length,
    owner_occupied: null,
    last_sale_date: text(sale?.ownership_transfer_date),
    last_sale_price: number(sale?.purchase_price_amount),
    subdivision: text(property.subdivision),
    has_permits: false,
    permit_count: 0,
    has_sunbiz_tenant: false,
    has_bbb_contractor: false,
    hoa_flag: null,
  };
}

/**
 * Extract transformed person/company names from a ZIP.
 *
 * @param {AdmZip} zip - Open transformed ZIP.
 * @returns {string[]} Ordered unique names.
 */
function readOwnerNames(zip) {
  const names = [];
  for (const entry of zip.getEntries()) {
    if (/^data\/person_\d+\.json$/u.test(entry.entryName)) {
      const person = readZipObject(zip, entry.entryName);
      const name = [
        text(person?.first_name),
        text(person?.middle_name),
        text(person?.last_name),
      ]
        .filter((part) => part !== null)
        .join(" ");
      if (name !== "") names.push(name);
    } else if (/^data\/company_\d+\.json$/u.test(entry.entryName)) {
      const company = readZipObject(zip, entry.entryName);
      const name = text(company?.name);
      if (name !== null) names.push(name);
    }
  }
  return [...new Set(names)];
}

/**
 * Return Donphan's stable 37-column property query schema.
 *
 * @returns {ParquetSchema} Parquet schema.
 */
function queryTableSchema() {
  return new ParquetSchema({
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
    hoa_flag: { type: "BOOLEAN", optional: true },
  });
}

/**
 * Build a pilot query table and manifest.
 *
 * @param {object} [options] - Optional paths.
 * @param {string} [options.validationDirectory] - Validated ZIP directory.
 * @param {string} [options.capturesPath] - Multi-request captures ZIP.
 * @param {string} [options.outputDirectory] - Query-table output directory.
 * @returns {Promise<{ parquetPath: string, rowCount: number }>} Build result.
 */
export async function buildPilotQueryTable(options = {}) {
  const validationDirectory = path.resolve(
    options.validationDirectory ?? DEFAULT_VALIDATION_DIRECTORY,
  );
  const capturesPath = path.resolve(
    options.capturesPath ?? DEFAULT_CAPTURES_PATH,
  );
  const outputDirectory = path.resolve(
    options.outputDirectory ?? DEFAULT_OUTPUT_DIRECTORY,
  );
  const summary =
    /** @type {{ results?: { requestIdentifier?: unknown, validationSuccess?: unknown }[] }} */ (
      JSON.parse(
        await readFile(path.join(validationDirectory, "summary.json"), "utf8"),
      )
    );
  const captures = new AdmZip(capturesPath);
  /** @type {QueryTableRow[]} */
  const rows = [];
  for (const result of summary.results ?? []) {
    if (
      result.validationSuccess !== true ||
      typeof result.requestIdentifier !== "string"
    ) {
      continue;
    }
    const folio = result.requestIdentifier;
    const transformed = new AdmZip(
      path.join(validationDirectory, `${folio}.zip`),
    );
    const property = readZipObject(transformed, "data/property.json");
    if (property === null) {
      throw new Error(`Missing data/property.json for ${folio}`);
    }
    const captureEntry = captures.getEntry(`${folio}.json`);
    if (captureEntry === null) {
      throw new Error(`Missing capture for ${folio}`);
    }
    const envelope = unwrapBrowardPrepareCapture(
      JSON.parse(captureEntry.getData().toString("utf8")),
    );
    const sourceRecord = requireParcelRecords(envelope, folio)[0];
    if (!isObject(sourceRecord)) {
      throw new Error(`Invalid source parcel for ${folio}`);
    }
    rows.push(
      buildQueryTableRow({
        folio,
        property,
        address: readZipObject(transformed, "data/address.json"),
        lot: readZipObject(transformed, "data/lot.json"),
        tax: readZipObject(transformed, "data/tax_1.json"),
        structure: readZipObject(transformed, "data/structure.json"),
        sale: readZipObject(transformed, "data/sales_1.json"),
        sourceRecord,
        ownerNames: readOwnerNames(transformed),
      }),
    );
  }
  await mkdir(outputDirectory, { recursive: true, mode: 0o700 });
  const parquetPath = path.join(outputDirectory, "query-table.parquet");
  const writer = await ParquetWriter.openFile(queryTableSchema(), parquetPath);
  for (const row of rows) await writer.appendRow(row);
  await writer.close();
  await writeFile(
    path.join(outputDirectory, "query-table-manifest.json"),
    `${JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        county: "Broward",
        state: "FL",
        sourceSystem: "broward_appraiser",
        rowCount: rows.length,
        distinctFolios: new Set(rows.map((row) => row.parcel_identifier)).size,
        nonNullCounts: Object.fromEntries(
          Object.keys(rows[0] ?? {}).map((key) => [
            key,
            rows.filter(
              (row) => row[/** @type {keyof QueryTableRow} */ (key)] !== null,
            ).length,
          ]),
        ),
      },
      null,
      2,
    )}\n`,
    "utf8",
  );
  return { parquetPath, rowCount: rows.length };
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  console.log(
    JSON.stringify({
      level: "info",
      message: "broward_pilot_query_table_complete",
      ...(await buildPilotQueryTable()),
    }),
  );
}
