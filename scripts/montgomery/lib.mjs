/**
 * Shared Montgomery County local-ingest helpers and PASDA REST client.
 * @module scripts/montgomery/lib
 */

export const PASDA_MONTGOMERY_BASE =
  "https://mapservices.pasda.psu.edu/server/rest/services/pasda/MontgomeryCounty/MapServer/14/query";

export const MONTGOMERY_GIS_FIELDS = [
  "OBJECTID_12",
  "TAXPIN",
  "PARCEL",
  "ALT_ID",
  "ALTERNATEI",
  "MUNI_CODE",
  "Muni_Name",
  "CLASS",
  "LAND_USE",
  "YEAR_BUILT",
  "YR_REM",
  "DEGREE_REM",
  "SFLA",
  "EXTWALL",
  "STORIES",
  "BASEMENT",
  "STYLE",
  "BEDROOMS",
  "BATHS",
  "HALF_BATHS",
  "RM_TOT",
  "COMM_AREA",
  "COMM_NLA",
  "COMM_YR_BL",
  "USE_TYPE",
  "STRUCTURE",
  "TOTAL_APPR",
  "TOTAL_ASSE",
  "OBYVAL",
  "EST_CO_TAX",
  "EST_MUNI_T",
  "EST_SCH_TA",
  "OWN1",
  "OWN2",
  "CAREOF",
  "ADDR1",
  "ADDR2",
  "ADDR3",
  "ZIP1_ZIP2",
  "LOCATION1",
  "LOC_NO",
  "LOC_STR",
  "LOC_SUF",
  "LOC_ZIP1_Z",
  "DEED_BOOK",
  "DEED_PAGE",
  "SALE_DATE",
  "CONSIDERAT",
  "LAND_SF",
  "LAND_ACRES",
  "SUBDIVISIO",
].join(",");

export const MONTGOMERY_SEED_HEADER = [
  "parcel_id",
  "source_identifier",
  "taxpin",
  "alt_id",
  "street",
  "city",
  "zip",
  "owner",
  "land_use",
  "class",
  "muni_code",
  "muni_name",
  "year_built",
  "livable_sqft",
  "exterior_wall",
];

/**
 * @param {string | undefined | null} value
 * @returns {string}
 */
export function normalizeGisText(value) {
  return typeof value === "string" ? value.trim() : "";
}

/**
 * @param {Record<string, unknown>} attributes
 * @returns {Record<string, string> | null}
 */
export function seedRowFromGisAttributes(attributes) {
  const taxpin = normalizeGisText(attributes.TAXPIN || attributes.PARCEL);
  if (!taxpin) {
    return null;
  }

  const street = normalizeGisText(attributes.LOCATION1);
  const city = normalizeGisText(attributes.Muni_Name);
  const zip = normalizeGisText(attributes.LOC_ZIP1_Z || attributes.ZIP1_ZIP2);
  const owner = normalizeGisText(attributes.OWN1);
  const landUse = normalizeGisText(attributes.LAND_USE);
  const cls = normalizeGisText(attributes.CLASS);
  const muniCode = normalizeGisText(attributes.MUNI_CODE);
  const muniName = normalizeGisText(attributes.Muni_Name);
  const yearBuilt = String(
    attributes.YEAR_BUILT || attributes.COMM_YR_BL || "",
  );
  const livableSqft = String(attributes.SFLA || attributes.COMM_AREA || "");
  const exteriorWall = normalizeGisText(attributes.EXTWALL);

  return {
    parcel_id: taxpin,
    source_identifier: taxpin,
    taxpin,
    alt_id: normalizeGisText(attributes.ALT_ID || attributes.ALTERNATEI),
    street,
    city,
    zip,
    owner,
    land_use: landUse,
    class: cls,
    muni_code: muniCode,
    muni_name: muniName,
    year_built: yearBuilt,
    livable_sqft: livableSqft,
    exterior_wall: exteriorWall,
  };
}

/**
 * @param {number} offset
 * @param {number} pageSize
 * @param {string} [where="1=1"]
 * @returns {string}
 */
export function buildPasdaPageUrl(offset, pageSize, where = "1=1") {
  const params = new URLSearchParams({
    where,
    outFields: MONTGOMERY_GIS_FIELDS,
    returnGeometry: "false",
    resultRecordCount: String(pageSize),
    resultOffset: String(offset),
    orderByFields: "OBJECTID_12",
    f: "json",
  });
  return `${PASDA_MONTGOMERY_BASE}?${params.toString()}`;
}

/**
 * @param {Array<Record<string, string>>} rows
 * @returns {string}
 */
export function serializeSeedCsv(rows) {
  const lines = [MONTGOMERY_SEED_HEADER.join(",")];
  for (const row of rows) {
    const fields = MONTGOMERY_SEED_HEADER.map((header) => {
      const val = row[header] ?? "";
      if (val.includes(",") || val.includes('"') || val.includes("\n")) {
        return `"${val.replace(/"/g, '""')}"`;
      }
      return val;
    });
    lines.push(fields.join(","));
  }
  return lines.join("\n") + "\n";
}
