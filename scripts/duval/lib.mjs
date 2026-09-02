export const COUNTY_NAME = "Duval";
export const COUNTY_FIPS = "12031";
export const DOR_COUNTY_NO = "26";
export const COJ_DETAIL_URL =
  "https://paopropertysearch.coj.net/Basic/Detail.aspx";
export const NAL_SOURCE_URL =
  "https://floridarevenue.com/property/dataportal/Documents/PTO%20Data%20Portal/Tax%20Roll%20Data%20Files/NAL/2026P/Duval%2026%20Preliminary%20NAL%202026.zip";
export const SDF_SOURCE_URL =
  "https://floridarevenue.com/property/dataportal/Documents/PTO%20Data%20Portal/Tax%20Roll%20Data%20Files/SDF/2026P/Duval%2026%20Preliminary%20SDF%202026.zip";
export const PIN_SOURCE_URL =
  "https://floridarevenue.com/property/dataportal/Documents/PTO%20Data%20Portal/Map%20Data/2026F/2026F%20PIN/duval_2026pin.zip";
export const EXPECTED_NAL_ROWS = 404_023;
export const EXPECTED_SDF_ROWS = 64_532;
export const EXPECTED_PIN_FEATURES = 405_716;
export const PIN_BBOX = Object.freeze({
  minLat: 29.99,
  maxLat: 30.7,
  minLng: -82.2,
  maxLng: -81.2,
});
export const SMOKE_PARCEL_IDS = Object.freeze([
  "0969250000R",
  "0901770592R",
  "1230290100R",
]);

const DOR_PARCEL_ID_PATTERN = /^[0-9]{10}R$/;

/**
 * Owner and fiduciary columns from the Florida DOR NAL. They are never requested
 * into the seed; keep this list beside the allow-list so the boundary is tested.
 *
 * @type {readonly string[]}
 */
export const EXCLUDED_PII_FIELDS = Object.freeze([
  "OWN_NAME",
  "OWN_ADDR1",
  "OWN_ADDR2",
  "OWN_CITY",
  "OWN_STATE",
  "OWN_ZIPCD",
  "FIDU_NAME",
]);

/**
 * Explicit non-PII NAL columns retained for provenance and stratification.
 *
 * @type {readonly string[]}
 */
export const NAL_SOURCE_FIELDS = Object.freeze([
  "PARCEL_ID",
  "CO_NO",
  "ASMNT_YR",
  "DOR_UC",
  "PA_UC",
  "JV",
  "AV_NSD",
  "TV_NSD",
  "LND_VAL",
  "LND_SQFOOT",
  "ACT_YR_BLT",
  "EFF_YR_BLT",
  "TOT_LVG_AREA",
  "NO_BULDNG",
  "NO_RES_UNTS",
  "NO_OWN_NM",
  "PHY_ADDR1",
  "PHY_ADDR2",
  "PHY_CITY",
  "PHY_ZIPCD",
  "NBRHD_CD",
  "MKT_AR",
  "CENSUS_BK",
  "SALE_PRC1",
  "SALE_YR1",
  "SALE_MO1",
  "QUAL_CD1",
]);

/**
 * Stable CSV column order consumed by capture. Geometry travels in the seed.
 *
 * @type {readonly string[]}
 */
export const SEED_COLUMNS = Object.freeze([
  "parcel_id",
  "source_identifier",
  "method",
  "url",
  "multiValueQueryString",
  "address",
  "city",
  "state",
  "zip",
  "county",
  "county_fips",
  "latitude",
  "longitude",
  "parcel_polygon",
  "source_url",
  "source_item_id",
  "source_revision",
  "source_snapshot_at",
  "source_record_count",
  "source_object_ids",
  "source_features_json",
  "source_sdf_sale_count",
  ...NAL_SOURCE_FIELDS.map((field) => `source_${field}`),
]);

/**
 * @param {unknown} value
 * @returns {string}
 */
export function toText(value) {
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

/**
 * @param {unknown} value
 * @returns {boolean}
 */
export function isValidDorParcelId(value) {
  return DOR_PARCEL_ID_PATTERN.test(toText(value));
}

/**
 * @param {unknown} value
 * @returns {string}
 */
export function toUndashedTenDigit(value) {
  const identifier = toText(value);
  if (!isValidDorParcelId(identifier)) {
    throw new Error(`Not a canonical DOR parcel id: ${identifier}`);
  }
  return identifier.slice(0, 10);
}

/**
 * @param {unknown} value
 * @returns {string}
 */
export function toCanonicalReDisplay(value) {
  const digits = toUndashedTenDigit(value);
  return `${digits.slice(0, 6)}-${digits.slice(6)}`;
}

/**
 * @param {unknown} value
 * @returns {string}
 */
export function toCojDetailUrl(value) {
  return `${COJ_DETAIL_URL}?RE=${toText(value)}`;
}

/**
 * @param {readonly string[]} sourceFields
 * @returns {void}
 */
export function assertSafeSourceFields(sourceFields) {
  const normalizedExcluded = new Set(
    EXCLUDED_PII_FIELDS.map((field) => field.toLowerCase()),
  );
  const seen = new Set();
  for (const field of sourceFields) {
    const normalized = field.toLowerCase();
    if (normalizedExcluded.has(normalized)) {
      throw new Error(
        `PII field is prohibited in the seed source request: ${field}`,
      );
    }
    if (seen.has(normalized)) {
      throw new Error(`Duplicate source field: ${field}`);
    }
    seen.add(normalized);
  }
}

/**
 * @param {unknown} value
 * @returns {string}
 */
function sourceValueToText(value) {
  if (value === null || value === undefined) return "";
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return String(value);
  }
  return JSON.stringify(value);
}

/**
 * @param {Record<string, unknown>} nal
 * @returns {string}
 */
function buildSiteAddress(nal) {
  const street = [toText(nal.PHY_ADDR1), toText(nal.PHY_ADDR2)]
    .filter((part) => part.length > 0)
    .join(" ");
  const city = toText(nal.PHY_CITY);
  const zip = toText(nal.PHY_ZIPCD);
  const locality = [city, "FL", zip]
    .filter((part) => part.length > 0)
    .join(" ");
  return [street, locality].filter((part) => part.length > 0).join(", ");
}

/**
 * @param {unknown} geometry
 * @returns {unknown[]}
 */
function polygonComponents(geometry) {
  if (!geometry || typeof geometry !== "object") return [];
  const geo = /** @type {{ type?: string, coordinates?: unknown }} */ (
    geometry
  );
  if (geo.type === "Polygon" && Array.isArray(geo.coordinates)) {
    return [geo.coordinates];
  }
  if (geo.type === "MultiPolygon" && Array.isArray(geo.coordinates)) {
    return [...geo.coordinates];
  }
  return [];
}

/**
 * @param {unknown} left
 * @param {unknown} right
 * @returns {object | ""}
 */
function mergeGeometries(left, right) {
  const components = [...polygonComponents(left), ...polygonComponents(right)];
  if (components.length === 0) return "";
  if (components.length === 1) {
    return { type: "Polygon", coordinates: components[0] };
  }
  return { type: "MultiPolygon", coordinates: components };
}

/**
 * @param {{
 *   nal: Record<string, unknown>,
 *   pin?: { latitude?: unknown, longitude?: unknown, geometry?: unknown } | null,
 *   sdfSaleCount?: number,
 *   sourceRevision: string,
 *   snapshotAt: string,
 *   sourceRecordCount?: number,
 *   sourceObjectIds?: string,
 *   sourceFeaturesJson?: string,
 * }} input
 * @returns {Record<string, string>}
 */
export function toSeedRow(input) {
  const nal = input.nal;
  const pin = input.pin ?? {};
  const identifier = toText(nal.PARCEL_ID);
  const geometry = pin.geometry ?? null;
  /** @type {Record<string, string>} */
  const row = {
    parcel_id: isValidDorParcelId(identifier)
      ? toUndashedTenDigit(identifier)
      : identifier,
    source_identifier: identifier,
    method: "GET",
    url: COJ_DETAIL_URL,
    multiValueQueryString: JSON.stringify({ RE: [identifier] }),
    address: buildSiteAddress(nal),
    city: toText(nal.PHY_CITY),
    state: "FL",
    zip: toText(nal.PHY_ZIPCD),
    county: COUNTY_NAME,
    county_fips: COUNTY_FIPS,
    latitude: sourceValueToText(pin.latitude ?? ""),
    longitude: sourceValueToText(pin.longitude ?? ""),
    parcel_polygon: geometry ? JSON.stringify(geometry) : "",
    source_url: NAL_SOURCE_URL,
    source_item_id: "fl-dor-nal-2026p-duval-26",
    source_revision: input.sourceRevision,
    source_snapshot_at: input.snapshotAt,
    source_record_count: String(input.sourceRecordCount ?? 1),
    source_object_ids: input.sourceObjectIds ?? "",
    source_features_json: input.sourceFeaturesJson ?? "",
    source_sdf_sale_count: String(input.sdfSaleCount ?? 0),
  };
  for (const field of NAL_SOURCE_FIELDS) {
    row[`source_${field}`] = sourceValueToText(nal[field]);
  }
  return row;
}

/**
 * @param {readonly { nal: Record<string, unknown>, pin?: object, sdfSaleCount?: number }[]} keyed
 * @param {{ sourceRevision: string, snapshotAt: string }} meta
 * @returns {Record<string, string>[]}
 */
export function mergeDuplicateParcels(keyed, meta) {
  /** @type {Map<string, typeof keyed>} */
  const groups = new Map();
  for (const record of keyed) {
    const identifier = toText(record.nal.PARCEL_ID);
    const existing = groups.get(identifier);
    if (existing) existing.push(record);
    else groups.set(identifier, [record]);
  }

  const rows = [];
  for (const group of groups.values()) {
    const ordered = [...group].sort(
      (left, right) => Number(right.nal.JV ?? 0) - Number(left.nal.JV ?? 0),
    );
    const primary = ordered[0];
    let geometry = primary.pin?.geometry ?? null;
    for (const extra of ordered.slice(1)) {
      geometry =
        mergeGeometries(geometry, extra.pin?.geometry ?? null) || geometry;
    }
    rows.push(
      toSeedRow({
        nal: primary.nal,
        pin: { ...primary.pin, geometry },
        sdfSaleCount: primary.sdfSaleCount ?? 0,
        sourceRevision: meta.sourceRevision,
        snapshotAt: meta.snapshotAt,
        sourceRecordCount: ordered.length,
        sourceObjectIds: ordered.map((_, index) => String(index + 1)).join("|"),
        sourceFeaturesJson:
          ordered.length > 1
            ? JSON.stringify(ordered.map((item) => item.nal))
            : "",
      }),
    );
  }
  return rows;
}

/**
 * @param {{
 *   rowsWritten: number,
 *   uniqueParcelIds: number,
 *   expectedSeedRowCount: number,
 *   unkeyedSourceRecords: number,
 *   invalidRecordCount: number,
 *   consolidatedRows: number,
 *   duplicateGroups: number,
 * }} stats
 * @returns {void}
 */
export function assertSeedReconciliation(stats) {
  if (stats.rowsWritten !== stats.expectedSeedRowCount) {
    throw new Error(
      `rowsWritten ${stats.rowsWritten} != expectedSeedRowCount ${stats.expectedSeedRowCount}`,
    );
  }
  if (stats.uniqueParcelIds !== stats.expectedSeedRowCount) {
    throw new Error(
      `uniqueParcelIds ${stats.uniqueParcelIds} != expectedSeedRowCount ${stats.expectedSeedRowCount}`,
    );
  }
  if (stats.unkeyedSourceRecords !== stats.invalidRecordCount) {
    throw new Error(
      `unkeyedSourceRecords ${stats.unkeyedSourceRecords} != invalidRecordCount ${stats.invalidRecordCount}`,
    );
  }
  if (stats.consolidatedRows !== stats.duplicateGroups) {
    throw new Error(
      `consolidatedRows ${stats.consolidatedRows} != duplicateGroups ${stats.duplicateGroups}`,
    );
  }
}

/**
 * @param {unknown} dorUc
 * @returns {string}
 */
export function classifyDorUseBand(dorUc) {
  const code = Number.parseInt(toText(dorUc), 10);
  if (!Number.isFinite(code)) return "other";
  if (code === 0) return "vacant_residential";
  if (code === 1) return "single_family";
  if (code === 2) return "mobile_home";
  if (code === 3 || code === 8) return "multi_family";
  if (code === 4) return "condo";
  if (code >= 10 && code <= 39) return "commercial";
  if (code >= 40 && code <= 49) return "industrial";
  if (code >= 50 && code <= 69) return "agricultural";
  if (code >= 70 && code <= 79) return "institutional";
  if (code >= 80 && code <= 89) return "government";
  return "other";
}

/**
 * @param {Record<string, string>} row
 * @returns {string[]}
 */
export function classifyPilotReasons(row) {
  const reasons = [classifyDorUseBand(row.source_DOR_UC)];
  if (Number(row.source_NO_BULDNG) > 1) reasons.push("multiple_buildings");
  if (Number(row.source_NO_OWN_NM) > 1) reasons.push("multiple_owners");
  if (Number(row.source_SALE_YR1) >= 2024) reasons.push("recent_sale");
  const living = Number(row.source_TOT_LVG_AREA);
  const buildings = Number(row.source_NO_BULDNG);
  const yearBuilt = Number(row.source_ACT_YR_BLT);
  if (
    (!Number.isFinite(living) || living <= 0) &&
    (!Number.isFinite(buildings) || buildings <= 0)
  ) {
    reasons.push("zero_improvements");
  }
  if (Number.isFinite(yearBuilt) && yearBuilt > 0 && yearBuilt <= 1940) {
    reasons.push("old_construction");
  }
  return reasons;
}

/**
 * @param {Record<string, string>} row
 * @returns {boolean}
 */
export function hasInRangePinGeometry(row) {
  const latitude = Number(row.latitude);
  const longitude = Number(row.longitude);
  return (
    Number.isFinite(latitude) &&
    Number.isFinite(longitude) &&
    latitude >= PIN_BBOX.minLat &&
    latitude <= PIN_BBOX.maxLat &&
    longitude >= PIN_BBOX.minLng &&
    longitude <= PIN_BBOX.maxLng
  );
}

/**
 * @param {string} value
 * @returns {string}
 */
export function encodeCsvCell(value) {
  if (!/[",\r\n]/.test(value)) return value;
  return `"${value.replaceAll('"', '""')}"`;
}

/**
 * @param {Record<string, string>} row
 * @returns {string}
 */
export function renderCsvRow(row) {
  return SEED_COLUMNS.map((column) =>
    encodeCsvCell(String(row[column] ?? "")),
  ).join(",");
}

/**
 * @param {readonly Record<string, string>[]} rows
 * @returns {string}
 */
export function renderSeedCsv(rows) {
  return `${[SEED_COLUMNS.join(","), ...rows.map(renderCsvRow)].join("\n")}\n`;
}
