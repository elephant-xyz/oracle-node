/**
 * City of Tarpon Springs Click2Gov (aspgov) permit portal.
 */

/**
 * @typedef {import("../permit-source-adapters/click2gov-http.mjs").Click2GovHttpConfig} Click2GovHttpConfig
 * @typedef {import("../permit-source-adapters/click2gov-http.mjs").Click2GovAddressQuery} Click2GovAddressQuery
 */

export const TARPON_CLICK2GOV_ORIGIN =
  "https://tarp-egov.aspgov.com/Click2GovBP";

/** @type {Click2GovHttpConfig} */
export const TARPON_CLICK2GOV_CONFIG = {
  origin: TARPON_CLICK2GOV_ORIGIN,
  city: "Tarpon Springs",
  sourceStamp: "tarpon-springs-click2gov",
};

/**
 * Conservative address probes: a known live hit (100 PINELLAS) plus a second
 * downtown street. Street number is required — empty-number POSTs 500.
 *
 * @type {readonly Click2GovAddressQuery[]}
 */
export const TARPON_DEFAULT_PROBE_QUERIES = Object.freeze([
  { streetNumber: "100", streetName: "PINELLAS" },
  { streetNumber: "200", streetName: "GULF" },
]);

/**
 * Major Tarpon Springs streets for an HTTP address-search harvest (not 311k
 * parcel search). Each lookup needs a street number; harvest walks a small
 * number list per street.
 *
 * @type {readonly string[]}
 */
export const TARPON_HARVEST_STREET_NAMES = Object.freeze([
  "PINELLAS",
  "GULF",
  "TARPON",
  "SPRING",
  "HUEY",
  "DODGE",
  "MLK",
  "DISSTON",
]);
