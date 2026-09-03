/**
 * Tyler EnerGov Civic Access / CSS tenants in Pinellas County.
 *
 * Largo is tylerhost Civic Access. Pinellas Park's Portico navigator is a
 * launcher; the public search tenant is city EnerGov CSS (browser bootstrap
 * required — a bare API POST returns 500 without tenant cookies).
 */

/**
 * @typedef {import("../permit-source-adapters/tyler-civic-access.mjs").TylerCivicAccessConfig} TylerCivicAccessConfig
 */

/**
 * @typedef {object} PinellasTylerAgency
 * @property {string} key CLI `--agency` value.
 * @property {string} jurisdiction City name.
 * @property {TylerCivicAccessConfig} config Civic Access / CSS config.
 * @property {string} sourceStamp Extracted JSON `source` value.
 * @property {string} jobIdPrefix Default job-id stem.
 * @property {readonly string[]} defaultProbeQueries Conservative 1–2 keyword lookups.
 */

/** @type {Readonly<Record<string, PinellasTylerAgency>>} */
export const PINELLAS_TYLER_AGENCIES = {
  largo: {
    key: "largo",
    jurisdiction: "City of Largo",
    config: {
      portalBaseUrl:
        "https://cityoflargofl-energovweb.tylerhost.net/apps/selfservice",
      city: "Largo",
      sourceSystem: "largo_city_tyler_permits",
    },
    sourceStamp: "largo-city-civic-access",
    jobIdPrefix: "largo-civic-access",
    defaultProbeQueries: Object.freeze(["West Bay Drive", "Seminole Boulevard"]),
  },
  park: {
    key: "park",
    jurisdiction: "City of Pinellas Park",
    config: {
      portalBaseUrl:
        "https://egcss.pinellas-park.com/energov_prod/selfservice",
      city: "Pinellas Park",
      sourceSystem: "pinellas_park_city_tyler_permits",
    },
    sourceStamp: "pinellas-park-city-energov",
    jobIdPrefix: "pinellas-park-energov",
    defaultProbeQueries: Object.freeze(["Park Boulevard", "66th Street"]),
  },
};

/**
 * @param {string | undefined} value CLI `--agency` token.
 * @returns {PinellasTylerAgency} Agency config.
 */
export function resolvePinellasTylerAgency(value) {
  const key = (value ?? "largo").trim().toLowerCase();
  const agency = PINELLAS_TYLER_AGENCIES[key];
  if (agency === undefined) {
    throw new Error(
      `--agency must be one of: ${Object.keys(PINELLAS_TYLER_AGENCIES).join(", ")}`,
    );
  }
  return agency;
}
