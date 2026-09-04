/**
 * Accela Citizen Access agencies in Pinellas County.
 *
 * County Building (`PINELLAS`) and City of Clearwater (`CLEARWATER`) are
 * separate tenants. Date-window harvest is the same Lee Accela helper with a
 * different CapHome URL. Other cities are not Accela.
 */

import {
  PINELLAS_DEFAULT_START_DATE,
  PINELLAS_PORTAL_URL,
  PINELLAS_RECORD_NUMBER_PATTERN,
  PINELLAS_SPLIT_THRESHOLD,
} from "./accela-pinellas.mjs";

/**
 * @typedef {object} AccelaAgencyConfig
 * @property {string} key CLI `--agency` value.
 * @property {string} agencyCode Accela path segment (`PINELLAS`, `CLEARWATER`).
 * @property {string} jurisdiction Human-readable jurisdiction.
 * @property {string} portalUrl CapHome Building search URL.
 * @property {RegExp} recordNumberPattern Permit-id matcher for list HTML.
 * @property {string} sourceStamp Written onto extracted JSON `source`.
 * @property {string} jobIdPrefix Default job-id stem.
 * @property {string} defaultStartDate Inclusive harvest start.
 * @property {number} splitThreshold Accela list cap.
 */

/** @type {Readonly<Record<string, AccelaAgencyConfig>>} */
export const ACCELA_AGENCIES = {
  pinellas: {
    key: "pinellas",
    agencyCode: "PINELLAS",
    jurisdiction: "unincorporated Pinellas County",
    portalUrl: PINELLAS_PORTAL_URL,
    recordNumberPattern: PINELLAS_RECORD_NUMBER_PATTERN,
    sourceStamp: "pinellas-county-accela",
    jobIdPrefix: "pinellas-accela",
    defaultStartDate: PINELLAS_DEFAULT_START_DATE,
    splitThreshold: PINELLAS_SPLIT_THRESHOLD,
  },
  clearwater: {
    key: "clearwater",
    agencyCode: "CLEARWATER",
    jurisdiction: "City of Clearwater",
    portalUrl:
      "https://aca-prod.accela.com/CLEARWATER/Cap/CapHome.aspx?TabName=Home&module=Building",
    recordNumberPattern: PINELLAS_RECORD_NUMBER_PATTERN,
    sourceStamp: "clearwater-city-accela",
    jobIdPrefix: "clearwater-accela",
    defaultStartDate: PINELLAS_DEFAULT_START_DATE,
    splitThreshold: PINELLAS_SPLIT_THRESHOLD,
  },
};

/**
 * @param {string | undefined} value CLI `--agency` token.
 * @returns {AccelaAgencyConfig} Agency harvest config.
 */
export function resolveAccelaAgency(value) {
  const key = (value ?? "pinellas").trim().toLowerCase();
  const agency = ACCELA_AGENCIES[key];
  if (agency === undefined) {
    throw new Error(
      `--agency must be one of: ${Object.keys(ACCELA_AGENCIES).join(", ")}`,
    );
  }
  return agency;
}
