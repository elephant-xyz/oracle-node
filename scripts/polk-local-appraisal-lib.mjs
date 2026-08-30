import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import * as path from "node:path";

import { ParquetSchema } from "@dsnp/parquetjs";

/** Version of the local Polk export/checkpoint contract. */
export const POLK_EXPORT_SCHEMA_VERSION = "1.0.0";

/** Stable source-system token used in property JSON and query-table rows. */
export const POLK_SOURCE_SYSTEM = "polk-property-appraiser-bulk";

/** Namespace input used to derive stable UUIDv5 property identifiers. */
const PROPERTY_ID_NAMESPACE_NAME = "oracle.elephant.xyz/fl-polk/property";

/** Standard UUID namespace for DNS names, represented as bytes. */
const DNS_UUID_NAMESPACE = Buffer.from(
  "6ba7b8109dad11d180b400c04fd430c8",
  "hex",
);

const EMAIL_PATTERN = /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i;
const PHONE_PATTERN =
  /(?:\+?1[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]\d{3}[\s.-]\d{4}/;
const SSN_PATTERN = /\b\d{3}-\d{2}-\d{4}\b/;
const FORBIDDEN_PUBLIC_KEY_PATTERN = /(grantor|grantee|mailing|owner_name)/i;
const PUBLIC_RECORD_IDENTIFIER_KEYS = new Set([
  "instrumentNumber",
  "parcelId",
  "parcelIdentifier",
  "permitNumber",
  "relatedParcelIdentifier",
]);

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * @typedef {object} PolkParcelSource
 * @property {string} parcel_id Canonical Polk parcel identifier.
 * @property {string | null} dor_use_code Florida DOR use code.
 * @property {string | null} property_type Primary DOR description.
 * @property {string | null} property_type_detail Secondary DOR description.
 * @property {string | null} neighborhood_code Neighborhood code.
 * @property {string | null} neighborhood_description Neighborhood description.
 * @property {string | null} land_value Total land value.
 * @property {string | null} building_value Total building value.
 * @property {string | null} extra_feature_value Total extra-feature value.
 * @property {string | null} market_value Total market value.
 * @property {string | null} assessed_value Assessed value.
 * @property {string | null} taxable_value Taxable value.
 * @property {string | null} yearly_tax_amount Current amount due.
 * @property {string | null} millage_rate Current millage rate.
 * @property {string | null} year_created Original creation year.
 * @property {string | null} year_improved Latest improvement year.
 * @property {string | null} last_inspection_date Last appraiser inspection timestamp.
 * @property {string | null} total_acreage Parcel acreage.
 * @property {string | null} related_parcel_identifier Polk parent/related STRAP.
 * @property {string | null} subdivision_code Polk subdivision code.
 * @property {string | null} subdivision_name Polk subdivision name.
 */

/**
 * @typedef {object} PolkSiteSource
 * @property {string} parcel_id Canonical Polk parcel identifier.
 * @property {string | null} line_number Site row order.
 * @property {string | null} building_number Site building number.
 * @property {string | null} street Street name and type.
 * @property {string | null} street_prefix Street-number prefix.
 * @property {string | null} street_number Street number.
 * @property {string | null} street_number_suffix Street-number suffix.
 * @property {string | null} street_suffix Street suffix.
 * @property {string | null} street_suffix_direction Street direction.
 * @property {string | null} unit Unit designator.
 * @property {string | null} postal_code Site ZIP code.
 * @property {string | null} city Site city.
 */

/**
 * @typedef {object} PolkSaleSource
 * @property {string} parcel_id Canonical Polk parcel identifier.
 * @property {string | null} sale_id Source sale id.
 * @property {string | null} line_number Source row order.
 * @property {string | null} sale_date Source sale date.
 * @property {string | null} price Sale price.
 * @property {string | null} book Official-record book.
 * @property {string | null} page Official-record page.
 * @property {string | null} sale_type Sale qualification code.
 * @property {string | null} transfer_code Transfer code.
 * @property {string | null} transfer_description Transfer description.
 * @property {string | null} instrument_type Instrument code.
 * @property {string | null} instrument_description Instrument description.
 * @property {string | null} foreclosure Foreclosure indicator.
 */

/**
 * @typedef {object} PolkBuildingSource
 * @property {string} parcel_id Canonical Polk parcel identifier.
 * @property {string | null} building_number Building number.
 * @property {string | null} improvement_type Improvement code.
 * @property {string | null} improvement_description Improvement description.
 * @property {string | null} style Building style code.
 * @property {string | null} style_description Building style description.
 * @property {string | null} stories Story count.
 * @property {string | null} shape Building shape code.
 * @property {string | null} shape_description Building shape description.
 * @property {string | null} class_code Building class code.
 * @property {string | null} class_description Building class description.
 * @property {string | null} bathrooms Bathroom count.
 * @property {string | null} units Unit count.
 * @property {string | null} bedrooms Bedroom count.
 * @property {string | null} fireplaces Fireplace count.
 * @property {string | null} substructure_description Substructure description.
 * @property {string | null} frame_description Frame description.
 * @property {string | null} effective_year Effective construction year.
 * @property {string | null} built_year Actual construction year.
 * @property {string | null} exterior_wall_description Exterior wall material.
 * @property {string | null} roof_description Roof material/type.
 * @property {string | null} floor_description Floor material.
 * @property {string | null} interior_wall_description Interior wall material.
 * @property {string | null} living_area Living area in square feet.
 * @property {string | null} total_under_roof Total area under roof.
 * @property {string | null} traverse Appraiser building-footprint traverse.
 */

/**
 * @typedef {object} PolkLayoutSource
 * @property {string} parcel_id Canonical Polk parcel identifier.
 * @property {string | null} building_number Building number.
 * @property {string | null} line_number Layout row order.
 * @property {string | null} code Sub-area code.
 * @property {string | null} description Sub-area description.
 * @property {string | null} actual_area Actual sub-area square feet.
 * @property {string | null} heated_area Heated sub-area square feet.
 */

/**
 * @typedef {object} PolkLandSource
 * @property {string} parcel_id Canonical Polk parcel identifier.
 * @property {string | null} line_number Land row order.
 * @property {string | null} land_type Land type.
 * @property {string | null} use_code Land use code.
 * @property {string | null} use_description Land use description.
 * @property {string | null} frontage Frontage measurement.
 * @property {string | null} depth Depth measurement.
 * @property {string | null} units Land units.
 * @property {string | null} unit_type Unit type code.
 * @property {string | null} unit_type_description Unit type description.
 * @property {string | null} influence_code Influence code.
 * @property {string | null} influence_description Influence description.
 */

/**
 * @typedef {object} PolkLegalSource
 * @property {string} parcel_id Canonical Polk parcel identifier.
 * @property {string | null} line_number Legal-description row order.
 * @property {string | null} description Public legal-description segment.
 */

/**
 * @typedef {object} PolkPermitSource
 * @property {string} parcel_id Canonical Polk parcel identifier.
 * @property {string | null} permit_id Source permit id.
 * @property {string | null} agency_name Issuing agency.
 * @property {string | null} permit_number Permit number.
 * @property {string | null} status Status code.
 * @property {string | null} status_description Status description.
 * @property {string | null} description Project description.
 * @property {string | null} permit_type Permit/improvement type.
 * @property {string | null} issue_date Issue timestamp.
 * @property {string | null} final_date Final timestamp.
 * @property {string | null} year Permit year.
 * @property {string | null} estimated_value Estimated job value.
 * @property {string | null} certificate_of_occupancy_date CO timestamp.
 */

/**
 * @typedef {object} PolkPropertySourceBundle
 * @property {PolkParcelSource} parcel Parcel-level source record.
 * @property {readonly PolkSiteSource[]} sites Site-address rows.
 * @property {readonly PolkSaleSource[]} sales Sale rows without party names.
 * @property {readonly PolkBuildingSource[]} buildings Building rows.
 * @property {readonly PolkLayoutSource[]} layouts Building sub-area rows.
 * @property {readonly PolkLandSource[]} lands Land/lot rows.
 * @property {readonly PolkLegalSource[]} legalDescriptions Legal segments.
 * @property {readonly PolkPermitSource[]} permits Permit rows.
 * @property {string} collectedAt Deterministic source snapshot timestamp.
 */

/**
 * @typedef {object} QueryTableRow
 * @property {string} property_id Stable UUIDv5 property id.
 * @property {string} property_cid Locally computed immutable JSON CID.
 * @property {string} request_identifier Polk request identifier.
 * @property {string} parcel_identifier Polk parcel identifier.
 * @property {string} source_system Bulk source-system token.
 * @property {string} county_name County name.
 * @property {string} state_code State code.
 * @property {string | null} address_street Situs street.
 * @property {string | null} address_city Situs city.
 * @property {string | null} address_zip Situs ZIP.
 * @property {number | null} latitude Latitude unavailable in Polk bulk CAMA.
 * @property {number | null} longitude Longitude unavailable in Polk bulk CAMA.
 * @property {number | null} lot_size_acre Parcel acreage.
 * @property {number | null} lot_area_sqft Parcel area in square feet.
 * @property {string | null} exterior_wall_material Principal wall material.
 * @property {string | null} roof_covering_material Principal roof material.
 * @property {string | null} property_type Appraiser property type.
 * @property {string | null} property_usage_type Normalized usage class.
 * @property {number | null} built_year Principal building year.
 * @property {number | null} livable_floor_area Summed living area.
 * @property {number | null} total_area Summed area under roof.
 * @property {number | null} assessed_value Current assessed value.
 * @property {number | null} market_value Current market value.
 * @property {number | null} land_value Current land value.
 * @property {number | null} avm_value AVM value, unavailable in bulk CAMA.
 * @property {null} owner_name Intentionally excluded owner name.
 * @property {null} owners_text Intentionally excluded owner names.
 * @property {null} owner_count Intentionally excluded owner count.
 * @property {null} owner_occupied Intentionally excluded owner occupancy.
 * @property {string | null} last_sale_date Latest sale date.
 * @property {number | null} last_sale_price Latest sale price.
 * @property {string | null} subdivision Subdivision name or code.
 * @property {boolean} has_permits Whether public permit rows exist.
 * @property {number} permit_count Public permit count.
 * @property {boolean} has_sunbiz_tenant Sunbiz enrichment status.
 * @property {boolean} has_bbb_contractor BBB enrichment status.
 * @property {null} hoa_flag HOA data unavailable.
 */

/**
 * @typedef {object} ShardCheckpoint
 * @property {number} shardIndex Zero-based shard index.
 * @property {string} file Relative Parquet shard path.
 * @property {string} manifest Relative shard-manifest path.
 * @property {number} rowCount Shard row count.
 * @property {string} fromParcel First parcel id.
 * @property {string} toParcel Last parcel id.
 * @property {number} propertyBytes Consolidated JSON bytes in this shard.
 */

/**
 * @typedef {object} PolkCheckpoint
 * @property {string} schemaVersion Checkpoint schema version.
 * @property {string} sourceFingerprint Source snapshot fingerprint.
 * @property {string} inputDirectory Absolute source directory.
 * @property {number} batchSize Frozen batch size for this output.
 * @property {number | null} limit Frozen optional property limit.
 * @property {string} startedAt Run start timestamp.
 * @property {number} processedCount Committed property count.
 * @property {string | null} lastParcelIdentifier Last committed parcel id.
 * @property {number} nextShardIndex Next shard number.
 * @property {readonly ShardCheckpoint[]} shards Committed shard metadata.
 * @property {boolean} complete Whether final artifacts were completed.
 */

/**
 * Return true only for a non-array JSON object.
 *
 * @param {unknown} value Candidate value.
 * @returns {value is JsonObject} Whether the value is a JSON object.
 */
export function isJsonObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Normalize an unknown scalar into trimmed text.
 *
 * @param {unknown} value Source scalar.
 * @returns {string | null} Trimmed text, or null for empty/non-scalar values.
 */
export function readText(value) {
  if (typeof value !== "string" && typeof value !== "number") return null;
  const text = String(value).trim();
  return text.length > 0 ? text : null;
}

/**
 * Normalize an unknown scalar into a finite number.
 *
 * @param {unknown} value Source scalar.
 * @returns {number | null} Finite number, or null.
 */
export function readNumber(value) {
  if (typeof value !== "string" && typeof value !== "number") return null;
  const text = String(value).replace(/[$,]/g, "").trim();
  if (text.length === 0) return null;
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Normalize an unknown scalar into an integer.
 *
 * @param {unknown} value Source scalar.
 * @returns {number | null} Truncated integer, or null.
 */
export function readInteger(value) {
  const number = readNumber(value);
  return number === null ? null : Math.trunc(number);
}

/**
 * Normalize a plausible four-digit construction or improvement year.
 *
 * Polk uses zero as an unknown-year sentinel; publishing it as a real year
 * would overstate building-year coverage.
 *
 * @param {unknown} value Source year.
 * @returns {number | null} Plausible year from 1700 through 2200, or null.
 */
export function readYear(value) {
  const year = readInteger(value);
  return year !== null && year >= 1700 && year <= 2200 ? year : null;
}

/**
 * Normalize a Polk parcel identifier without dropping significant leading zeros.
 *
 * @param {unknown} value Source parcel identifier.
 * @returns {string | null} Uppercase compact parcel identifier.
 */
export function normalizeParcelIdentifier(value) {
  const text = readText(value);
  return text === null ? null : text.toUpperCase().replace(/[^A-Z0-9]/g, "");
}

/**
 * Normalize Polk ISO-like and US date values into `YYYY-MM-DD`.
 *
 * Sentinel dates used by Polk (`1899-12-30`) are intentionally mapped to null.
 *
 * @param {unknown} value Source date.
 * @returns {string | null} Normalized date, or null.
 */
export function normalizeDate(value) {
  const text = readText(value);
  if (text === null) return null;
  let normalized = null;
  const iso = /^(\d{4})-(\d{2})-(\d{2})/.exec(text);
  if (iso?.[1] !== undefined && iso[2] !== undefined && iso[3] !== undefined) {
    normalized = `${iso[1]}-${iso[2]}-${iso[3]}`;
  } else {
    const us = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(text);
    if (us?.[1] !== undefined && us[2] !== undefined && us[3] !== undefined) {
      normalized = `${us[3]}-${us[1].padStart(2, "0")}-${us[2].padStart(2, "0")}`;
    }
  }
  if (normalized === null || normalized <= "1900-01-01") return null;
  const timestamp = Date.parse(`${normalized}T00:00:00Z`);
  return Number.isFinite(timestamp) ? normalized : null;
}

/**
 * Normalize a ZIP or ZIP+4 value.
 *
 * @param {unknown} value Source ZIP.
 * @returns {string | null} Five- or nine-digit ZIP rendering.
 */
export function normalizePostalCode(value) {
  const text = readText(value);
  if (text === null) return null;
  const digits = text.replace(/\D/g, "");
  if (digits.length >= 9) return `${digits.slice(0, 5)}-${digits.slice(5, 9)}`;
  return digits.length >= 5 ? digits.slice(0, 5) : null;
}

/**
 * Render JSON deterministically by sorting every object key while preserving
 * array order, then adding a single trailing newline.
 *
 * @param {unknown} value JSON-compatible value.
 * @returns {string} Stable pretty JSON.
 */
export function stableJson(value) {
  return `${JSON.stringify(sortJson(value), null, 2)}\n`;
}

/**
 * Recursively sort JSON object keys.
 *
 * @param {unknown} value JSON-compatible value.
 * @returns {unknown} Equivalent value with sorted object keys.
 */
function sortJson(value) {
  if (Array.isArray(value)) return value.map((entry) => sortJson(entry));
  if (!isJsonObject(value)) return value;
  return Object.fromEntries(
    Object.keys(value)
      .sort(compareText)
      .map((key) => [key, sortJson(value[key])]),
  );
}

/**
 * Compare text using code-unit ordering, independent of locale.
 *
 * @param {string} left Left text.
 * @param {string} right Right text.
 * @returns {number} Sort order.
 */
export function compareText(left, right) {
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

/**
 * Produce a UUIDv5 from a namespace UUID and UTF-8 name.
 *
 * @param {string} name Stable name.
 * @param {Buffer} namespace Sixteen-byte namespace UUID.
 * @returns {string} RFC 4122 UUIDv5.
 */
function uuidV5(name, namespace) {
  if (namespace.byteLength !== 16) {
    throw new Error("UUID namespace must contain exactly 16 bytes");
  }
  const digest = createHash("sha1")
    .update(namespace)
    .update(Buffer.from(name, "utf8"))
    .digest()
    .subarray(0, 16);
  digest[6] = ((digest[6] ?? 0) & 0x0f) | 0x50;
  digest[8] = ((digest[8] ?? 0) & 0x3f) | 0x80;
  const hex = digest.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/**
 * Build the deterministic Polk property UUID. The nested namespace isolates
 * these ids from every other UUIDv5 domain while remaining stable across runs.
 *
 * @param {unknown} parcelIdentifier Polk parcel identifier.
 * @returns {string} Stable property UUID.
 */
export function deterministicPropertyId(parcelIdentifier) {
  const parcel = normalizeParcelIdentifier(parcelIdentifier);
  if (parcel === null)
    throw new Error("Cannot derive property id without parcel");
  const namespace = Buffer.from(
    uuidV5(PROPERTY_ID_NAMESPACE_NAME, DNS_UUID_NAMESPACE).replaceAll("-", ""),
    "hex",
  );
  return uuidV5(parcel, namespace);
}

/**
 * Build a site street line from Polk's split address fields.
 *
 * @param {PolkSiteSource | undefined} site Preferred site row.
 * @returns {string | null} Normalized public situs street.
 */
export function buildSiteStreet(site) {
  if (site === undefined) return null;
  const parts = [
    site.street_prefix,
    site.street_number,
    site.street_number_suffix,
    site.street,
    site.street_suffix,
    site.street_suffix_direction,
  ]
    .map((part) => readText(part))
    .filter((part) => part !== null);
  const street = parts.join(" ").replace(/\s+/g, " ").trim();
  const unit = readText(site.unit);
  if (street.length === 0) return unit === null ? null : `UNIT ${unit}`;
  return unit === null ? street : `${street} UNIT ${unit}`;
}

/**
 * Select the principal site row deterministically.
 *
 * @param {readonly PolkSiteSource[]} sites Candidate site rows.
 * @returns {PolkSiteSource | undefined} Preferred site row.
 */
function selectPrincipalSite(sites) {
  return [...sites].sort((left, right) => {
    const leftBuilding = readInteger(left.building_number) ?? 999_999;
    const rightBuilding = readInteger(right.building_number) ?? 999_999;
    if (leftBuilding !== rightBuilding) return leftBuilding - rightBuilding;
    return (
      (readInteger(left.line_number) ?? 999_999) -
      (readInteger(right.line_number) ?? 999_999)
    );
  })[0];
}

/**
 * Select the principal building deterministically.
 *
 * @param {readonly PolkBuildingSource[]} buildings Building rows.
 * @returns {PolkBuildingSource | undefined} Preferred building.
 */
function selectPrincipalBuilding(buildings) {
  return [...buildings].sort((left, right) => {
    const leftNumber = readInteger(left.building_number) ?? 999_999;
    const rightNumber = readInteger(right.building_number) ?? 999_999;
    if (leftNumber !== rightNumber) return leftNumber - rightNumber;
    return (
      (readInteger(right.living_area) ?? 0) -
      (readInteger(left.living_area) ?? 0)
    );
  })[0];
}

/**
 * Normalize Florida DOR use codes into broad public query categories.
 *
 * Text explicitly naming residential use wins because Polk has agricultural
 * use codes for mixed pasture/residential parcels.
 *
 * @param {unknown} useCode Florida DOR code.
 * @param {unknown} description Polk use description.
 * @returns {string | null} `RES`, `COM`, `IND`, `AGR`, `INST`, `GOV`, or `MISC`.
 */
export function classifyPropertyUsage(useCode, description) {
  const text = readText(description)?.toUpperCase() ?? "";
  if (/\bRES(?:IDENTIAL)?\b/.test(text) || /W\/RES/.test(text)) return "RES";
  const digits = readText(useCode)?.replace(/\D/g, "") ?? "";
  if (digits.length === 0) return null;
  const group = Number.parseInt(digits.padStart(4, "0").slice(0, 2), 10);
  if (group <= 9) return "RES";
  if (group <= 39) return "COM";
  if (group <= 49) return "IND";
  if (group <= 69) return "AGR";
  if (group <= 79) return "INST";
  if (group <= 89) return "GOV";
  return "MISC";
}

/**
 * Suppress permit free text containing common contact or direct-identity
 * patterns. This fails closed rather than publishing a partially redacted note.
 *
 * @param {unknown} value Permit description.
 * @returns {string | null} Safe description, or null.
 */
export function sanitizePublicDescription(value) {
  const text = readText(value);
  if (text === null) return null;
  if (
    EMAIL_PATTERN.test(text) ||
    PHONE_PATTERN.test(text) ||
    SSN_PATTERN.test(text)
  ) {
    return null;
  }
  return text;
}

/**
 * Repair the known extra trailing quote on a non-empty Polk building TRAVERSE
 * field. Valid empty final fields ending in `,""` remain unchanged.
 *
 * @param {string} line Physical building CSV line without newline.
 * @returns {string} Repaired or unchanged line.
 */
export function normalizePolkBuildingCsvLine(line) {
  const hasCarriageReturn = line.endsWith("\r");
  const content = hasCarriageReturn ? line.slice(0, -1) : line;
  if (content.endsWith('""') && !content.endsWith(',""')) {
    return `${content.slice(0, -1)}${hasCarriageReturn ? "\r" : ""}`;
  }
  return line;
}

/**
 * Escape raw double quotes in Polk's final legal-description field.
 *
 * @param {string} line Physical legal CSV line without newline.
 * @param {number} lineNumber One-based source line for diagnostics.
 * @returns {string} Valid UTF-8 CSV line.
 */
export function normalizePolkLegalCsvLine(line, lineNumber) {
  const hasCarriageReturn = line.endsWith("\r");
  const content = hasCarriageReturn ? line.slice(0, -1) : line;
  const match =
    /^"([^"]*)","([^"]*)","([^"]*)","([^"]*)","([^"]*)","([^"]*)","([^"]*)","(.*)"$/.exec(
      content,
    );
  if (match === null) {
    throw new Error(
      `Cannot normalize legal-description CSV line ${lineNumber}`,
    );
  }
  const columns = match.slice(1, 8);
  const description = match[8] ?? "";
  return `${columns.map((column) => `"${column}"`).join(",")},"${description.replaceAll('"', '""')}"${hasCarriageReturn ? "\r" : ""}`;
}

/**
 * Normalize one complete 18-field Polk permit record after multiline physical
 * lines have been joined. Existing escaped quotes are canonicalized and raw
 * Windows-style inch quotes become valid doubled CSV quotes.
 *
 * @param {string} line Complete permit CSV record without newline.
 * @param {number} lineNumber One-based physical source line for diagnostics.
 * @returns {string} Valid UTF-8 CSV record.
 */
export function normalizePolkPermitCsvRecord(line, lineNumber) {
  const hasCarriageReturn = line.endsWith("\r");
  const content = hasCarriageReturn ? line.slice(0, -1) : line;
  if (!content.startsWith('"') || !content.endsWith('"')) {
    throw new Error(`Cannot normalize permit CSV line ${lineNumber}`);
  }
  const columns = content.slice(1, -1).split('","');
  if (columns.length !== 18) {
    throw new Error(
      `Permit CSV line ${lineNumber} has ${columns.length} fields instead of 18`,
    );
  }
  const normalized = columns
    .map((column) => `"${column.replaceAll('""', '"').replaceAll('"', '""')}"`)
    .join(",");
  return `${normalized}${hasCarriageReturn ? "\r" : ""}`;
}

/**
 * Build one public consolidated Polk property from batch-bounded source rows.
 *
 * Owners, ownership records, deed parties, grantors, grantees, and mailing
 * addresses are never accepted by this input contract and never copied.
 *
 * @param {PolkPropertySourceBundle} source Batch-bounded source rows.
 * @returns {JsonObject} PII-safe consolidated property JSON.
 */
export function buildConsolidatedProperty(source) {
  const parcelIdentifier = normalizeParcelIdentifier(source.parcel.parcel_id);
  if (parcelIdentifier === null)
    throw new Error("Parcel row has no identifier");
  const propertyId = deterministicPropertyId(parcelIdentifier);
  const site = selectPrincipalSite(source.sites);
  const principalBuilding = selectPrincipalBuilding(source.buildings);
  const acreage = readNumber(source.parcel.total_acreage);
  const legalDescriptions = [...source.legalDescriptions]
    .sort(
      (left, right) =>
        (readInteger(left.line_number) ?? 999_999) -
        (readInteger(right.line_number) ?? 999_999),
    )
    .flatMap((row) => {
      const description = readText(row.description);
      return description === null ? [] : [description];
    });
  const sales = [...source.sales]
    .map((sale) => {
      const book = readText(sale.book);
      const page = readText(sale.page);
      return {
        date: normalizeDate(sale.sale_date),
        instrumentNumber:
          book === null && page === null
            ? null
            : [book, page].filter((part) => part !== null).join("/"),
        price: readNumber(sale.price),
        saleType: readText(sale.sale_type),
        transferCode: readText(sale.transfer_code),
        transferDescription: readText(sale.transfer_description),
        instrumentType: readText(sale.instrument_type),
        instrumentDescription: readText(sale.instrument_description),
        foreclosure: readText(sale.foreclosure),
      };
    })
    .sort((left, right) => {
      const dateOrder = compareText(right.date ?? "", left.date ?? "");
      if (dateOrder !== 0) return dateOrder;
      return compareText(
        right.instrumentNumber ?? "",
        left.instrumentNumber ?? "",
      );
    });
  const structures = [...source.buildings]
    .sort(
      (left, right) =>
        (readInteger(left.building_number) ?? 999_999) -
        (readInteger(right.building_number) ?? 999_999),
    )
    .map((building) => ({
      buildingNumber: readInteger(building.building_number),
      improvementType: readText(building.improvement_type),
      improvementDescription: readText(building.improvement_description),
      style: readText(building.style),
      styleDescription: readText(building.style_description),
      stories: readNumber(building.stories),
      shape: readText(building.shape),
      shapeDescription: readText(building.shape_description),
      classCode: readText(building.class_code),
      classDescription: readText(building.class_description),
      bathroomCount: readNumber(building.bathrooms),
      unitCount: readInteger(building.units),
      bedroomCount: readInteger(building.bedrooms),
      fireplaceCount: readInteger(building.fireplaces),
      substructureDescription: readText(building.substructure_description),
      frameDescription: readText(building.frame_description),
      effectiveBuiltYear: readYear(building.effective_year),
      builtYear: readYear(building.built_year),
      exteriorWallMaterial: readText(building.exterior_wall_description),
      roofCoveringMaterial: readText(building.roof_description),
      floorMaterial: readText(building.floor_description),
      interiorWallMaterial: readText(building.interior_wall_description),
      livableArea: readNumber(building.living_area),
      totalArea: readNumber(building.total_under_roof),
      traverse: readText(building.traverse),
    }));
  const layouts = [...source.layouts]
    .sort((left, right) => {
      const buildingOrder =
        (readInteger(left.building_number) ?? 999_999) -
        (readInteger(right.building_number) ?? 999_999);
      if (buildingOrder !== 0) return buildingOrder;
      return (
        (readInteger(left.line_number) ?? 999_999) -
        (readInteger(right.line_number) ?? 999_999)
      );
    })
    .map((layout) => ({
      buildingNumber: readInteger(layout.building_number),
      lineNumber: readInteger(layout.line_number),
      code: readText(layout.code),
      description: readText(layout.description),
      actualArea: readNumber(layout.actual_area),
      heatedArea: readNumber(layout.heated_area),
    }));
  const lots = [...source.lands]
    .sort(
      (left, right) =>
        (readInteger(left.line_number) ?? 999_999) -
        (readInteger(right.line_number) ?? 999_999),
    )
    .map((land) => ({
      lineNumber: readInteger(land.line_number),
      landType: readText(land.land_type),
      useCode: readText(land.use_code),
      useDescription: readText(land.use_description),
      frontage: readNumber(land.frontage),
      depth: readNumber(land.depth),
      units: readNumber(land.units),
      unitType: readText(land.unit_type),
      unitTypeDescription: readText(land.unit_type_description),
      influenceCode: readText(land.influence_code),
      influenceDescription: readText(land.influence_description),
    }));
  const permits = [...source.permits]
    .sort((left, right) => {
      const dateOrder = compareText(
        normalizeDate(right.issue_date) ?? "",
        normalizeDate(left.issue_date) ?? "",
      );
      if (dateOrder !== 0) return dateOrder;
      return compareText(
        readText(left.permit_number) ?? "",
        readText(right.permit_number) ?? "",
      );
    })
    .map((permit) => ({
      completionDate:
        normalizeDate(permit.final_date) ??
        normalizeDate(permit.certificate_of_occupancy_date),
      contacts: [],
      customFields: {
        agencyName: readText(permit.agency_name),
        permitYear: readInteger(permit.year),
        statusCode: readText(permit.status),
      },
      estimatedJobValue: readNumber(permit.estimated_value),
      estimatedSqFt: null,
      events: [],
      fees: [],
      improvementType: readText(permit.permit_type),
      inspections: [],
      issueDate: normalizeDate(permit.issue_date),
      links: [],
      permitNumber:
        readText(permit.permit_number) ?? readText(permit.permit_id),
      projectDescription: sanitizePublicDescription(permit.description),
      recordStatus:
        readText(permit.status_description) ?? readText(permit.status),
    }));
  const livingArea = structures.reduce(
    (sum, structure) => sum + (readNumber(structure.livableArea) ?? 0),
    0,
  );
  const totalArea = structures.reduce(
    (sum, structure) => sum + (readNumber(structure.totalArea) ?? 0),
    0,
  );
  const builtYear =
    readYear(principalBuilding?.built_year) ??
    readYear(source.parcel.year_improved) ??
    readYear(source.parcel.year_created);
  const propertyType =
    readText(source.parcel.property_type_detail) ??
    readText(source.parcel.property_type);

  const property = {
    address: {
      city: readText(site?.city),
      latitude: null,
      longitude: null,
      postalCode: normalizePostalCode(site?.postal_code),
      state: "FL",
      street: buildSiteStreet(site),
    },
    bbbProfiles: [],
    collectedAt: source.collectedAt,
    county: "polk",
    deeds: [],
    files: [],
    floodInfo: {
      evacuationZone: null,
      floodInsuranceRequired: null,
      floodZone: null,
    },
    geometry: {
      latitude: null,
      longitude: null,
    },
    jurisdictionKey: "fl-polk",
    layouts,
    lots,
    ownerships: [],
    parcel: {
      countyName: "Polk",
      parcelIdentifier,
      relatedParcelIdentifier: normalizeParcelIdentifier(
        source.parcel.related_parcel_identifier,
      ),
      stateCode: "FL",
    },
    parcelId: propertyId,
    permits,
    property: {
      areaUnderAir: livingArea > 0 ? livingArea : null,
      buildStatus: null,
      builtYear,
      effectiveBuiltYear: readYear(principalBuilding?.effective_year),
      historicDesignation: null,
      legalDescription:
        legalDescriptions.length > 0 ? legalDescriptions.join(" ") : null,
      livableArea: livingArea > 0 ? livingArea : null,
      lotSizeAcre: acreage,
      neighborhoodCode: readText(source.parcel.neighborhood_code),
      neighborhoodDescription: readText(source.parcel.neighborhood_description),
      numberOfUnits: structures.reduce(
        (sum, structure) => sum + (readInteger(structure.unitCount) ?? 0),
        0,
      ),
      propertyType,
      structureForm: readText(principalBuilding?.style_description),
      subdivision:
        readText(source.parcel.subdivision_name) ??
        readText(source.parcel.subdivision_code),
      totalArea: totalArea > 0 ? totalArea : null,
      usageType: classifyPropertyUsage(
        source.parcel.dor_use_code,
        `${source.parcel.property_type ?? ""} ${source.parcel.property_type_detail ?? ""}`,
      ),
      zoning: null,
    },
    sales,
    sourceSystem: POLK_SOURCE_SYSTEM,
    structures,
    sunbizTenants: [],
    taxes: [
      {
        assessedValue: readNumber(source.parcel.assessed_value),
        buildingValue: readNumber(source.parcel.building_value),
        extraFeatureValue: readNumber(source.parcel.extra_feature_value),
        landValue: readNumber(source.parcel.land_value),
        marketValue: readNumber(source.parcel.market_value),
        millageRate: readNumber(source.parcel.millage_rate),
        taxYear: null,
        taxableValue: readNumber(source.parcel.taxable_value),
        yearlyTaxAmount: readNumber(source.parcel.yearly_tax_amount),
      },
    ],
    utilities: [],
    valuations: [],
  };
  const privacyFindings = scanPublicProperty(property);
  if (privacyFindings.length > 0) {
    throw new Error(
      `Public privacy gate failed for ${parcelIdentifier}: ${privacyFindings.join(", ")}`,
    );
  }
  if (acreage !== null && acreage < 0) {
    throw new Error(`Negative acreage for ${parcelIdentifier}`);
  }
  return property;
}

/**
 * Scan one public property for forbidden private identity/contact fields.
 *
 * The deliberately empty `ownerships` compatibility array is allowed, but it
 * must remain empty. Deeds are also required to remain empty because Polk sale
 * party names are excluded from this publication path.
 *
 * @param {unknown} value Public property candidate.
 * @returns {string[]} Finding paths.
 */
export function scanPublicProperty(value) {
  /** @type {string[]} */
  const findings = [];

  /**
   * @param {unknown} current Current value.
   * @param {string} location JSON path.
   * @param {string | null} key Object key that contains the current value.
   * @returns {void}
   */
  const visit = (current, location, key) => {
    if (Array.isArray(current)) {
      if (
        (location === "$.ownerships" || location === "$.deeds") &&
        current.length > 0
      ) {
        findings.push(`private_array:${location}`);
      }
      current.forEach((entry, index) =>
        visit(entry, `${location}[${index}]`, null),
      );
      return;
    }
    if (!isJsonObject(current)) {
      if (typeof current === "string") {
        if (EMAIL_PATTERN.test(current)) findings.push(`email:${location}`);
        if (
          SSN_PATTERN.test(current) &&
          (key === null || !PUBLIC_RECORD_IDENTIFIER_KEYS.has(key))
        ) {
          findings.push(`ssn:${location}`);
        }
      }
      return;
    }
    for (const [key, entry] of Object.entries(current)) {
      const childLocation = `${location}.${key}`;
      if (
        FORBIDDEN_PUBLIC_KEY_PATTERN.test(key) ||
        (key.toLowerCase().includes("owner") && key !== "ownerships")
      ) {
        findings.push(`forbidden_key:${childLocation}`);
      }
      visit(entry, childLocation, key);
    }
  };

  visit(value, "$", null);
  return findings;
}

/**
 * Convert consolidated property JSON to one modern MCP query-table row.
 *
 * @param {JsonObject} property Consolidated property.
 * @param {string} propertyCid Locally computed JSON CID.
 * @returns {QueryTableRow} Scalar query-table row.
 */
export function buildQueryTableRow(property, propertyCid) {
  const address = isJsonObject(property.address) ? property.address : {};
  const parcel = isJsonObject(property.parcel) ? property.parcel : {};
  const propertyDetail = isJsonObject(property.property)
    ? property.property
    : {};
  const structures = Array.isArray(property.structures)
    ? property.structures.filter(isJsonObject)
    : [];
  const sales = Array.isArray(property.sales)
    ? property.sales.filter(isJsonObject)
    : [];
  const taxes = Array.isArray(property.taxes)
    ? property.taxes.filter(isJsonObject)
    : [];
  const permits = Array.isArray(property.permits) ? property.permits : [];
  const lots = Array.isArray(property.lots)
    ? property.lots.filter(isJsonObject)
    : [];
  const acreageFromLots = lots.reduce((sum, lot) => {
    if (readText(lot.unitType)?.toUpperCase() !== "A") return sum;
    return sum + (readNumber(lot.units) ?? 0);
  }, 0);
  const acreage =
    readNumber(propertyDetail.lotSizeAcre) ??
    (acreageFromLots > 0 ? acreageFromLots : null);
  const principalStructure = structures[0];
  const latestSale = sales[0];
  const tax = taxes[0];
  const propertyId = readText(property.parcelId);
  const parcelIdentifier = readText(parcel.parcelIdentifier);
  if (propertyId === null || parcelIdentifier === null) {
    throw new Error("Consolidated property is missing query-table identifiers");
  }
  return {
    property_id: propertyId,
    property_cid: propertyCid,
    request_identifier: parcelIdentifier,
    parcel_identifier: parcelIdentifier,
    source_system: readText(property.sourceSystem) ?? POLK_SOURCE_SYSTEM,
    county_name: readText(parcel.countyName) ?? "Polk",
    state_code: readText(parcel.stateCode) ?? "FL",
    address_street: readText(address.street),
    address_city: readText(address.city),
    address_zip: readText(address.postalCode),
    latitude: readNumber(address.latitude),
    longitude: readNumber(address.longitude),
    lot_size_acre: acreage,
    lot_area_sqft: acreage === null ? null : acreage * 43_560,
    exterior_wall_material: readText(principalStructure?.exteriorWallMaterial),
    roof_covering_material: readText(principalStructure?.roofCoveringMaterial),
    property_type: readText(propertyDetail.propertyType),
    property_usage_type: readText(propertyDetail.usageType),
    built_year: readInteger(propertyDetail.builtYear),
    livable_floor_area: readNumber(propertyDetail.livableArea),
    total_area: readNumber(propertyDetail.totalArea),
    assessed_value: readNumber(tax?.assessedValue),
    market_value: readNumber(tax?.marketValue),
    land_value: readNumber(tax?.landValue),
    avm_value: null,
    owner_name: null,
    owners_text: null,
    owner_count: null,
    owner_occupied: null,
    last_sale_date: readText(latestSale?.date),
    last_sale_price: readNumber(latestSale?.price),
    subdivision: readText(propertyDetail.subdivision),
    has_permits: permits.length > 0,
    permit_count: permits.length,
    has_sunbiz_tenant: false,
    has_bbb_contractor: false,
    hoa_flag: null,
  };
}

/**
 * Build the exact scalar Parquet schema consumed by the modern Elephant MCP.
 *
 * @returns {ParquetSchema} Query-table schema.
 */
export function buildQueryTableParquetSchema() {
  return new ParquetSchema({
    property_id: { type: "UTF8" },
    property_cid: { type: "UTF8" },
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
    owner_count: { type: "INT64", optional: true },
    owner_occupied: { type: "BOOLEAN", optional: true },
    last_sale_date: { type: "UTF8", optional: true },
    last_sale_price: { type: "DOUBLE", optional: true },
    subdivision: { type: "UTF8", optional: true },
    has_permits: { type: "BOOLEAN" },
    permit_count: { type: "INT64" },
    has_sunbiz_tenant: { type: "BOOLEAN" },
    has_bbb_contractor: { type: "BOOLEAN" },
    hoa_flag: { type: "BOOLEAN", optional: true },
  });
}

/**
 * Remove null optional fields before sending a row to parquetjs.
 *
 * @param {QueryTableRow} row Typed query-table row.
 * @returns {Record<string, string | number | boolean>} Parquet record.
 */
export function toParquetRecord(row) {
  return Object.fromEntries(
    Object.entries(row).filter(([, value]) => value !== null),
  );
}

/**
 * Return the deterministic sharding path for one property JSON.
 *
 * Hash sharding avoids hundreds of thousands of files in a single directory
 * and remains independent of batch size.
 *
 * @param {string} parcelIdentifier Canonical parcel identifier.
 * @returns {string} Relative JSON path.
 */
export function propertyRelativePath(parcelIdentifier) {
  const parcel = normalizeParcelIdentifier(parcelIdentifier);
  if (parcel === null) throw new Error("Cannot build path without parcel id");
  const directory = createHash("sha256")
    .update(parcel)
    .digest("hex")
    .slice(0, 2);
  return path.join("properties", directory, `${parcel}.json`);
}

/**
 * Atomically replace one file by writing and fsync-safe-renaming a sibling temp
 * file. A failed write never exposes a partial final artifact.
 *
 * @param {string} destination Final file path.
 * @param {string | Buffer} body Complete file body.
 * @returns {Promise<void>} Resolves after rename.
 */
export async function writeFileAtomically(destination, body) {
  await mkdir(path.dirname(destination), { recursive: true });
  const temporary = `${destination}.${process.pid}.${randomUUID()}.tmp`;
  try {
    await writeFile(temporary, body, { mode: 0o600 });
    await rename(temporary, destination);
  } catch (caught) {
    await rm(temporary, { force: true });
    throw caught;
  }
}

/**
 * Atomically write stable JSON.
 *
 * @param {string} destination Final JSON path.
 * @param {unknown} value JSON-compatible value.
 * @returns {Promise<void>} Resolves after rename.
 */
export async function writeJsonAtomically(destination, value) {
  await writeFileAtomically(destination, stableJson(value));
}

/**
 * Read and minimally validate a Polk checkpoint.
 *
 * @param {string} checkpointPath Checkpoint path.
 * @returns {Promise<PolkCheckpoint | null>} Existing checkpoint, or null.
 */
export async function readCheckpoint(checkpointPath) {
  let text;
  try {
    text = await readFile(checkpointPath, "utf8");
  } catch (caught) {
    if (
      caught instanceof Error &&
      "code" in caught &&
      /** @type {NodeJS.ErrnoException} */ (caught).code === "ENOENT"
    ) {
      return null;
    }
    throw caught;
  }
  const parsed = /** @type {unknown} */ (JSON.parse(text));
  if (
    !isJsonObject(parsed) ||
    parsed.schemaVersion !== POLK_EXPORT_SCHEMA_VERSION ||
    typeof parsed.sourceFingerprint !== "string" ||
    typeof parsed.inputDirectory !== "string" ||
    typeof parsed.batchSize !== "number" ||
    typeof parsed.processedCount !== "number" ||
    typeof parsed.nextShardIndex !== "number" ||
    !Array.isArray(parsed.shards) ||
    typeof parsed.complete !== "boolean"
  ) {
    throw new Error(`Invalid Polk checkpoint at ${checkpointPath}`);
  }
  return /** @type {PolkCheckpoint} */ (parsed);
}

/**
 * Assert that a resumed run uses the frozen source/configuration contract.
 *
 * @param {PolkCheckpoint} checkpoint Existing checkpoint.
 * @param {{sourceFingerprint:string,inputDirectory:string,batchSize:number,limit:number | null}} options Current options.
 * @returns {void}
 */
export function assertCheckpointCompatible(checkpoint, options) {
  const mismatches = [];
  if (checkpoint.sourceFingerprint !== options.sourceFingerprint) {
    mismatches.push("source fingerprint");
  }
  if (checkpoint.inputDirectory !== options.inputDirectory) {
    mismatches.push("input directory");
  }
  if (checkpoint.batchSize !== options.batchSize) mismatches.push("batch size");
  if (checkpoint.limit !== options.limit) mismatches.push("limit");
  if (mismatches.length > 0) {
    throw new Error(
      `Cannot safely resume because ${mismatches.join(", ")} changed; choose a new --out directory`,
    );
  }
}

/**
 * Build a new empty checkpoint.
 *
 * @param {{sourceFingerprint:string,inputDirectory:string,batchSize:number,limit:number | null,startedAt:string}} options Frozen run options.
 * @returns {PolkCheckpoint} Initial checkpoint.
 */
export function createCheckpoint(options) {
  return {
    schemaVersion: POLK_EXPORT_SCHEMA_VERSION,
    sourceFingerprint: options.sourceFingerprint,
    inputDirectory: options.inputDirectory,
    batchSize: options.batchSize,
    limit: options.limit,
    startedAt: options.startedAt,
    processedCount: 0,
    lastParcelIdentifier: null,
    nextShardIndex: 0,
    shards: [],
    complete: false,
  };
}

/**
 * Atomically persist a checkpoint.
 *
 * @param {string} checkpointPath Checkpoint path.
 * @param {PolkCheckpoint} checkpoint Complete checkpoint.
 * @returns {Promise<void>} Resolves after atomic replacement.
 */
export async function writeCheckpoint(checkpointPath, checkpoint) {
  await writeJsonAtomically(checkpointPath, checkpoint);
}

/**
 * Build batch quality counters from one consolidated property.
 *
 * @param {JsonObject} property Consolidated property.
 * @returns {Record<string, number>} One-record quality counters.
 */
export function propertyQualityCounters(property) {
  const address = isJsonObject(property.address) ? property.address : {};
  const detail = isJsonObject(property.property) ? property.property : {};
  const counters = {
    properties: 1,
    withSiteAddress: readText(address.street) === null ? 0 : 1,
    withPostalCode: readText(address.postalCode) === null ? 0 : 1,
    withCoordinates:
      readNumber(address.latitude) === null ||
      readNumber(address.longitude) === null
        ? 0
        : 1,
    withPropertyType: readText(detail.propertyType) === null ? 0 : 1,
    withUsageType: readText(detail.usageType) === null ? 0 : 1,
    withBuiltYear: readInteger(detail.builtYear) === null ? 0 : 1,
    withLivableArea: readNumber(detail.livableArea) === null ? 0 : 1,
    withLegalDescription: readText(detail.legalDescription) === null ? 0 : 1,
    withSales:
      Array.isArray(property.sales) && property.sales.length > 0 ? 1 : 0,
    withBuildings:
      Array.isArray(property.structures) && property.structures.length > 0
        ? 1
        : 0,
    withLayouts:
      Array.isArray(property.layouts) && property.layouts.length > 0 ? 1 : 0,
    withLots: Array.isArray(property.lots) && property.lots.length > 0 ? 1 : 0,
    withPermits:
      Array.isArray(property.permits) && property.permits.length > 0 ? 1 : 0,
    saleRows: Array.isArray(property.sales) ? property.sales.length : 0,
    buildingRows: Array.isArray(property.structures)
      ? property.structures.length
      : 0,
    layoutRows: Array.isArray(property.layouts) ? property.layouts.length : 0,
    lotRows: Array.isArray(property.lots) ? property.lots.length : 0,
    permitRows: Array.isArray(property.permits) ? property.permits.length : 0,
  };
  return counters;
}

/**
 * Sum homogeneous quality-counter objects.
 *
 * @param {readonly Record<string, number>[]} counters Counter objects.
 * @returns {Record<string, number>} Summed counters.
 */
export function sumQualityCounters(counters) {
  /** @type {Record<string, number>} */
  const total = {};
  for (const counter of counters) {
    for (const [key, value] of Object.entries(counter)) {
      total[key] = (total[key] ?? 0) + value;
    }
  }
  return total;
}
