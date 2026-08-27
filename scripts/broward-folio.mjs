/**
 * Broward County folio helpers.
 *
 * The Property Appraiser API keys parcels by a 12-character alphanumeric
 * folio. Condos include letters (`504108BJ0140`). Dashes are a display-only
 * grouping and must not be sent to `getParcelInformation`.
 */

export const BROWARD_COUNTY_NAME = "Broward";
export const BROWARD_COUNTY_KEY = "broward";
export const BROWARD_COUNTY_FIPS = "12011";
export const BROWARD_DETAIL_URL =
  "https://web.bcpa.net/BcpaClient/search.aspx/getParcelInformation";
export const BROWARD_GIS_LAYER_URL =
  "https://gisweb-adapters.bcpa.net/arcgis/rest/services/BCPA_EXTERNAL_JAN26/MapServer/16";

/**
 * Curated ~25-parcel pilot covering commercial (permit path), residential
 * (skip), condo-with-letters, unincorporated, agricultural, and right-of-way.
 *
 * @type {readonly string[]}
 */
export const BROWARD_PILOT_FOLIOS = Object.freeze([
  "474135010090",
  "494209060010",
  "494318013550",
  "484109030410",
  "494212072320",
  "504201090030",
  "503912010490",
  "513914101320",
  "514111160200",
  "494119160090",
  "494109050270",
  "504118051290",
  "494202352310",
  "504108BJ0140",
  "494108AK1220",
  "484201BA0050",
  "494123BJ0010",
  "504209091840",
  "514207022070",
  "474135010091",
  "504026140250",
  "474134000012",
  "514106100100",
  "514123070029",
  "484230301500",
]);

/**
 * Normalize a Broward folio for the appraiser API and seed `parcel_id`.
 *
 * Strips display dashes/spaces, uppercases letters, and requires the canonical
 * 12-character length. Does **not** coerce to a number, strip letters, or pad
 * with zeros.
 *
 * @param {unknown} value - Raw folio from GIS, CSV, or CLI.
 * @returns {string | undefined} API-ready folio, or undefined when unusable.
 */
export function normalizeBrowardFolio(value) {
  if (typeof value !== "string" && typeof value !== "number") return undefined;
  const compact = String(value).trim().replace(/[-\s]/g, "").toUpperCase();
  if (!/^[A-Z0-9]{12}$/u.test(compact)) return undefined;
  return compact;
}

/**
 * True when the value is a usable Broward folio after normalization.
 *
 * @param {unknown} value - Candidate folio.
 * @returns {boolean} Whether the folio can be sent to the appraiser API.
 */
export function isValidBrowardFolio(value) {
  return normalizeBrowardFolio(value) !== undefined;
}

/**
 * JSON body for `search.aspx/getParcelInformation`.
 *
 * `taxyear` must be the empty string. Sending `"CURRENT"` as the tax year
 * returns a typed envelope with `parcelInfok__BackingField: null`.
 *
 * @param {string} folio - Normalized undashed folio.
 * @returns {{ folioNumber: string, taxyear: string, action: string, use: string }}
 *   POST JSON body.
 */
export function browardDetailRequestBody(folio) {
  return {
    folioNumber: folio,
    taxyear: "",
    action: "CURRENT",
    use: "",
  };
}
