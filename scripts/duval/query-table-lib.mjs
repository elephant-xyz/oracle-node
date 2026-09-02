/**
 * Duval Task 8: map transform artifacts onto Montgomery QUERY_TABLE_SCHEMA
 * (`scripts/montgomery-batch-run.mjs` lines 42–81).
 * @module scripts/duval/query-table-lib
 */

import { createHash } from "node:crypto";
import { readdir, readFile, access } from "node:fs/promises";
import { join } from "node:path";

import parquet from "@dsnp/parquetjs";

const { ParquetSchema } = parquet;

/** Same 37-column contract elephant-mcp `getPropertyQuerySchema` serves. */
export const DUVAL_QUERY_TABLE_SCHEMA = new ParquetSchema({
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

/**
 * @param {string} unnormalized
 * @returns {{ street: string | null; city: string | null; zip: string | null }}
 */
export function parseUnnormalizedAddress(unnormalized) {
  if (!unnormalized || typeof unnormalized !== "string") {
    return { street: null, city: null, zip: null };
  }
  const parts = unnormalized
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
  const street = parts[0] ?? null;
  const rest = parts.slice(1).join(" ").trim();
  const zipMatch = /\b(\d{5})(?:-\d{4})?\b/.exec(rest);
  const city = rest
    .replace(/\b[A-Z]{2}\b\s+\d{5}(?:-\d{4})?\b/i, "")
    .replace(/\s+/g, " ")
    .trim();
  return {
    street,
    city: city || null,
    zip: zipMatch ? zipMatch[1] : null,
  };
}

/**
 * @param {unknown} value
 * @returns {number | null}
 */
export function toFiniteNumber(value) {
  if (value == null || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

/**
 * @param {Array<{ tax_year?: unknown }>} taxes
 * @returns {Record<string, unknown> | null}
 */
export function pickLatestTax(taxes) {
  let best = null;
  let bestYear = Number.NEGATIVE_INFINITY;
  for (const tax of taxes) {
    const year = toFiniteNumber(tax?.tax_year);
    if (year == null) continue;
    if (year >= bestYear) {
      bestYear = year;
      best = tax;
    }
  }
  return best;
}

/**
 * @param {Array<{ ownership_transfer_date?: unknown; purchase_price_amount?: unknown }>} sales
 * @returns {{ ownership_transfer_date: string | null; purchase_price_amount: number | null } | null}
 */
export function pickLatestSale(sales) {
  let best = null;
  let bestDate = "";
  for (const sale of sales) {
    const date = String(sale?.ownership_transfer_date ?? "");
    if (!date) continue;
    if (date >= bestDate) {
      bestDate = date;
      best = sale;
    }
  }
  return best;
}

/**
 * @param {{ first_name?: unknown; last_name?: unknown; name?: unknown }} owner
 * @returns {string | null}
 */
export function formatOwnerName(owner) {
  if (!owner || typeof owner !== "object") return null;
  if (typeof owner.name === "string" && owner.name.trim()) {
    return owner.name.trim();
  }
  const first = String(owner.first_name ?? "").trim();
  const middle = String(owner.middle_name ?? "").trim();
  const last = String(owner.last_name ?? "").trim();
  const combined = [first, middle, last].filter(Boolean).join(" ");
  return combined || null;
}

/**
 * @param {string} parcelId
 * @returns {string}
 */
export function duvalPropertyId(parcelId) {
  return createHash("sha256")
    .update(`duval:${parcelId}`)
    .digest("hex")
    .slice(0, 32);
}

/**
 * @param {{
 *   folio: string;
 *   seed?: Record<string, unknown> | null;
 *   property?: Record<string, unknown> | null;
 *   address?: Record<string, unknown> | null;
 *   captureAddress?: Record<string, unknown> | null;
 *   geometry?: Record<string, unknown> | null;
 *   geometryParcels?: Array<Record<string, unknown>>;
 *   lot?: Record<string, unknown> | null;
 *   taxes?: Array<Record<string, unknown>>;
 *   sales?: Array<Record<string, unknown>>;
 *   owners?: Array<Record<string, unknown>>;
 *   structure?: Record<string, unknown> | null;
 * }} artifacts
 * @returns {Record<string, unknown>}
 */
export function rowFromDuvalArtifacts(artifacts) {
  const seed = artifacts.seed ?? {};
  const property = artifacts.property ?? {};
  const address = artifacts.address ?? {};
  const geometry = artifacts.geometry ?? {};
  const lot = artifacts.lot ?? {};
  const parcelId = String(seed.parcel_id ?? artifacts.folio ?? "").trim();
  if (!parcelId) {
    throw new Error("query table row is missing parcel_id");
  }
  const captureAddress = artifacts.captureAddress ?? {};
  const parsed = parseUnnormalizedAddress(
    String(
      address.unnormalized_address ||
        captureAddress.full_address ||
        captureAddress.unnormalized_address ||
        "",
    ),
  );
  const tax = pickLatestTax(artifacts.taxes ?? []) ?? {};
  const sale = pickLatestSale(artifacts.sales ?? []);
  const ownerNames = (artifacts.owners ?? [])
    .map((owner) => formatOwnerName(owner))
    .filter(Boolean);
  const lotSqft = toFiniteNumber(lot.lot_area_sqft);
  const lotAcres =
    toFiniteNumber(lot.lot_size_acre) ??
    (lotSqft != null ? lotSqft / 43_560 : null);
  const builtYear = toFiniteNumber(property.property_structure_built_year);
  const structure = artifacts.structure ?? {};
  const point =
    Number.isFinite(toFiniteNumber(geometry.latitude)) &&
    Number.isFinite(toFiniteNumber(geometry.longitude))
      ? geometry
      : ((artifacts.geometryParcels ?? []).find(
          (candidate) =>
            Number.isFinite(toFiniteNumber(candidate.latitude)) &&
            Number.isFinite(toFiniteNumber(candidate.longitude)),
        ) ?? geometry);

  return {
    property_id: duvalPropertyId(parcelId),
    property_cid: null,
    request_identifier: String(seed.request_identifier ?? `${parcelId}R`),
    parcel_identifier: parcelId,
    source_system: "duval_appraiser",
    county_name: "Duval",
    state_code: "FL",
    address_street: parsed.street,
    address_city: parsed.city,
    address_zip: parsed.zip,
    latitude: toFiniteNumber(point.latitude),
    longitude: toFiniteNumber(point.longitude),
    lot_size_acre: lotAcres,
    lot_area_sqft: lotSqft,
    exterior_wall_material: structure.exterior_wall_material_primary ?? null,
    roof_covering_material: structure.roof_covering_material ?? null,
    property_type: property.property_type ?? null,
    property_usage_type: property.property_usage_type ?? null,
    built_year: builtYear,
    livable_floor_area: toFiniteNumber(property.livable_floor_area),
    total_area: toFiniteNumber(property.total_area),
    assessed_value: toFiniteNumber(tax.property_assessed_value_amount),
    market_value: toFiniteNumber(tax.property_market_value_amount),
    land_value: toFiniteNumber(tax.property_land_amount),
    avm_value: null,
    owner_name: ownerNames[0] ?? null,
    owners_text: ownerNames.length ? ownerNames.join("; ") : null,
    owner_count: ownerNames.length,
    owner_occupied: null,
    last_sale_date: sale
      ? String(sale.ownership_transfer_date ?? "") || null
      : null,
    last_sale_price: sale ? toFiniteNumber(sale.purchase_price_amount) : null,
    subdivision: property.subdivision ?? null,
    has_permits: false,
    permit_count: 0,
    has_sunbiz_tenant: false,
    has_bbb_contractor: false,
    has_pa_corp_tenant: false,
    hoa_flag: null,
  };
}

/**
 * @param {Array<{ parcel_identifier?: unknown }>} rows
 * @param {number} [expected]
 * @returns {void}
 */
export function assertQueryTableIds(rows, expected = 50) {
  const ids = rows.map((row) =>
    row.parcel_identifier == null ? "" : String(row.parcel_identifier),
  );
  if (ids.some((id) => id.trim() === "")) {
    throw new Error("query table has a null or empty parcel_identifier");
  }
  const distinct = new Set(ids);
  if (distinct.size !== ids.length) {
    throw new Error(
      `query table has duplicate parcel_identifier values (${ids.length} rows, ${distinct.size} distinct)`,
    );
  }
  if (distinct.size !== expected) {
    throw new Error(
      `query table distinct parcel_identifier ${distinct.size} != ${expected}`,
    );
  }
}

/**
 * Task 6 writes `property_seed.json` before transform; only completed parcels
 * have `transformed_output.zip` and `data/property.json`.
 *
 * @param {string} parcelDir
 * @returns {Promise<boolean>}
 */
export async function isCompleteDuvalParcel(parcelDir) {
  try {
    await access(join(parcelDir, "transformed_output.zip"));
    await access(join(parcelDir, "data", "property.json"));
    return true;
  } catch {
    return false;
  }
}

/**
 * @param {string} parcelDir
 * @param {string} folio
 * @returns {Promise<Parameters<typeof rowFromDuvalArtifacts>[0]>}
 */
export async function loadDuvalParcelArtifacts(parcelDir, folio) {
  /**
   * @param {string} relative
   * @returns {Promise<Record<string, unknown> | null>}
   */
  async function readJson(relative) {
    try {
      return JSON.parse(await readFile(join(parcelDir, relative), "utf8"));
    } catch {
      return null;
    }
  }

  const dataDir = join(parcelDir, "data");
  let names = [];
  try {
    names = await readdir(dataDir);
  } catch {
    names = [];
  }

  /** @type {Array<Record<string, unknown>>} */
  const taxes = [];
  /** @type {Array<Record<string, unknown>>} */
  const sales = [];
  /** @type {Array<{ order: number; kind: number; record: Record<string, unknown> }>} */
  const ownerEntries = [];
  /** @type {Array<Record<string, unknown>>} */
  const structures = [];
  /** @type {Array<Record<string, unknown>>} */
  const geometryParcels = [];

  for (const name of names) {
    if (!name.endsWith(".json")) continue;
    const record = await readJson(join("data", name));
    if (!record) continue;
    if (/^tax_\d+\.json$/.test(name)) taxes.push(record);
    if (/^sales_history_\d+\.json$/.test(name)) sales.push(record);
    const personMatch = /^person_(\d+)\.json$/.exec(name);
    const companyMatch = /^company_(\d+)\.json$/.exec(name);
    if (personMatch) {
      ownerEntries.push({
        order: Number(personMatch[1]),
        kind: 0,
        record,
      });
    } else if (companyMatch) {
      ownerEntries.push({
        order: Number(companyMatch[1]),
        kind: 1,
        record,
      });
    }
    if (/^structure_\d+\.json$/.test(name)) structures.push(record);
    if (/^geometry_parcel_.*\.json$/.test(name)) geometryParcels.push(record);
  }

  ownerEntries.sort(
    (left, right) => left.kind - right.kind || left.order - right.order,
  );
  const structure =
    structures.find(
      (record) =>
        record.exterior_wall_material_primary || record.roof_covering_material,
    ) ??
    structures[0] ??
    null;

  return {
    folio,
    seed: await readJson("property_seed.json"),
    captureAddress: await readJson("unnormalized_address.json"),
    property: await readJson("data/property.json"),
    address: await readJson("data/address.json"),
    geometry: await readJson("data/geometry.json"),
    geometryParcels,
    lot: await readJson("data/lot.json"),
    taxes,
    sales,
    owners: ownerEntries.map((entry) => entry.record),
    structure,
  };
}
