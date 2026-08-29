// @ts-check

/**
 * @typedef {"tyler-civic-access" | "citizenserve"} BrowardPermitVendor
 */

/**
 * Existing Broward appraisal evidence safe for a one-property source probe.
 *
 * @typedef {object} BrowardValidatedPermitSample
 * @property {string} folio - Exact 12-character validated folio.
 * @property {string} address - Appraiser situs address.
 */

/**
 * @typedef {object} BrowardPermitJurisdictionConfig
 * @property {string} key - Stable CLI/configuration key.
 * @property {string} city - Issuing municipality written to normalized records.
 * @property {string} sourceSystem - Stable normalized source key.
 * @property {BrowardPermitVendor} vendor - Shared adapter family.
 * @property {string} officialSourceUrl - First-party municipal source/custodian page.
 * @property {string} portalBaseUrl - Exact official vendor portal base.
 * @property {boolean} anonymousSearchCertified - Whether record search is certified without login.
 * @property {readonly ("folio" | "address")[]} searchKinds - Supported property-first fields.
 * @property {string} coverageNote - Known temporal/custody boundary.
 * @property {string | null} skipReason - Explicit reason that automated record search is disabled.
 * @property {number | null} citizenserveInstallationId - Citizenserve tenant ID, otherwise `null`.
 * @property {readonly string[]} permitTypeTokens - Citizenserve row tokens proving issuing jurisdiction.
 * @property {BrowardValidatedPermitSample | null} validatedSample - Existing Broward appraisal validation evidence.
 */

/** @type {readonly ("folio" | "address")[]} */
const PROPERTY_SEARCH_KINDS = Object.freeze(["folio", "address"]);

/** @type {readonly ("folio" | "address")[]} */
const NO_SEARCH_KINDS = Object.freeze([]);

/**
 * Tyler EnerGov/Civic Access and Citizenserve/CAP Government jurisdictions in
 * the documented Broward permit matrix.
 *
 * North Lauderdale remains configured for routing/documentation but is
 * deliberately disabled: its official page says a login is required to use
 * the service. The adapters accept no credentials and never attempt login.
 *
 * Shared Citizenserve installation `117` contains both Lauderdale-by-the-Sea
 * and Southwest Ranches. `permitTypeTokens` prevents records from one issuing
 * jurisdiction being emitted under the other jurisdiction's source key.
 *
 * @type {Readonly<Record<string, BrowardPermitJurisdictionConfig>>}
 */
export const BROWARD_PERMIT_JURISDICTIONS = Object.freeze({
  pembroke_pines: Object.freeze({
    key: "pembroke_pines",
    city: "Pembroke Pines",
    sourceSystem: "broward_pembroke_pines_tyler_permits",
    vendor: "tyler-civic-access",
    officialSourceUrl: "https://www.ppines.com/developmenthub",
    portalBaseUrl:
      "https://pembrokepinesfl-energovweb.tylerhost.net/apps/selfservice",
    anonymousSearchCertified: true,
    searchKinds: PROPERTY_SEARCH_KINDS,
    coverageNote:
      "Development HUB records only; the City separately documents a records-search request for 1992-current history, so portal completeness is not inferred.",
    skipReason: null,
    citizenserveInstallationId: null,
    permitTypeTokens: Object.freeze([]),
    validatedSample: Object.freeze({
      folio: "513914101320",
      address: "470 SW 198 TERRACE",
    }),
  }),
  hallandale_beach: Object.freeze({
    key: "hallandale_beach",
    city: "Hallandale Beach",
    sourceSystem: "broward_hallandale_beach_tyler_permits",
    vendor: "tyler-civic-access",
    officialSourceUrl: "https://cohb.org/Faq.aspx?QID=75",
    portalBaseUrl:
      "https://hallandalefl-energovpub.tylerhost.net/Apps/SelfService",
    anonymousSearchCertified: true,
    searchKinds: PROPERTY_SEARCH_KINDS,
    coverageNote:
      "The official FAQ certifies anonymous global permit/parcel/address search; earliest and complete migrated history remain unverified.",
    skipReason: null,
    citizenserveInstallationId: null,
    permitTypeTokens: Object.freeze([]),
    validatedSample: null,
  }),
  miramar: Object.freeze({
    key: "miramar",
    city: "Miramar",
    sourceSystem: "broward_miramar_tyler_permits",
    vendor: "tyler-civic-access",
    officialSourceUrl:
      "https://www.miramarfl.gov/Departments/Building-Planning-Zoning/Building-Permits-Inspections/Online-Permitting",
    portalBaseUrl:
      "https://miramarfl-energovweb.tylerhost.net/apps/SelfService",
    anonymousSearchCertified: true,
    searchKinds: PROPERTY_SEARCH_KINDS,
    coverageNote:
      "Public-record search is separate from authenticated project management; no complete-history claim is made.",
    skipReason: null,
    citizenserveInstallationId: null,
    permitTypeTokens: Object.freeze([]),
    validatedSample: Object.freeze({
      folio: "514123070029",
      address: "PEMBROKE ROAD",
    }),
  }),
  oakland_park: Object.freeze({
    key: "oakland_park",
    city: "Oakland Park",
    sourceSystem: "broward_oakland_park_tyler_permits",
    vendor: "tyler-civic-access",
    officialSourceUrl: "https://oaklandparkfl.gov/312/Permit-Access",
    portalBaseUrl:
      "https://oaklandparkfl-energovweb.tylerhost.net/apps/SelfService",
    anonymousSearchCertified: true,
    searchKinds: PROPERTY_SEARCH_KINDS,
    coverageNote:
      "Tyler contains records after 2019-11-01 only. Earlier permits remain in the City's documented legacy searches/public-record route.",
    skipReason: null,
    citizenserveInstallationId: null,
    permitTypeTokens: Object.freeze([]),
    validatedSample: null,
  }),
  north_lauderdale: Object.freeze({
    key: "north_lauderdale",
    city: "North Lauderdale",
    sourceSystem: "broward_north_lauderdale_tyler_permits",
    vendor: "tyler-civic-access",
    officialSourceUrl:
      "https://nlauderdale.org/departments/community_development/e-permit.php",
    portalBaseUrl:
      "https://nlselfservice.nlauderdale.org/Energov_prod/SelfService",
    anonymousSearchCertified: false,
    searchKinds: NO_SEARCH_KINDS,
    coverageNote:
      "Official Enterprise Permitting & Licensing CSS access requires login; property-wide records use the City public-record request route.",
    skipReason:
      "North Lauderdale's official page requires login; anonymous record search is not certified and credentials are never accepted.",
    citizenserveInstallationId: null,
    permitTypeTokens: Object.freeze([]),
    validatedSample: null,
  }),
  lauderdale_by_the_sea: Object.freeze({
    key: "lauderdale_by_the_sea",
    city: "Lauderdale-by-the-Sea",
    sourceSystem: "broward_lauderdale_by_the_sea_citizenserve_permits",
    vendor: "citizenserve",
    officialSourceUrl:
      "https://lauderdalebythesea-fl.gov/152/Building-Division",
    portalBaseUrl: "https://www6.citizenserve.com/Portal",
    anonymousSearchCertified: true,
    searchKinds: PROPERTY_SEARCH_KINDS,
    coverageNote:
      "Current CAP Government/Citizenserve records only; historical BCS-held records are a separate county source and are not merged here.",
    skipReason: null,
    citizenserveInstallationId: 117,
    permitTypeTokens: Object.freeze(["lauderdale-by-the-sea"]),
    validatedSample: Object.freeze({
      folio: "494318013550",
      address: "218 E COMMERCIAL BOULEVARD",
    }),
  }),
  southwest_ranches: Object.freeze({
    key: "southwest_ranches",
    city: "Southwest Ranches",
    sourceSystem: "broward_southwest_ranches_citizenserve_permits",
    vendor: "citizenserve",
    officialSourceUrl:
      "https://www.southwestranches.org/departments/building-permitting-and-inspections/",
    portalBaseUrl: "https://www6.citizenserve.com/Portal",
    anonymousSearchCertified: true,
    searchKinds: PROPERTY_SEARCH_KINDS,
    coverageNote:
      "CAP Government building permits only; separate Town zoning, engineering, and external-agency approvals are outside this adapter.",
    skipReason: null,
    citizenserveInstallationId: 117,
    permitTypeTokens: Object.freeze(["southwest ranches"]),
    validatedSample: Object.freeze({
      folio: "504026140250",
      address: "GRIFFIN ROAD",
    }),
  }),
  west_park: Object.freeze({
    key: "west_park",
    city: "West Park",
    sourceSystem: "broward_west_park_citizenserve_permits",
    vendor: "citizenserve",
    officialSourceUrl:
      "https://www.cityofwestpark.org/governement/departments/building-department",
    portalBaseUrl: "https://www6.citizenserve.com/Portal",
    anonymousSearchCertified: true,
    searchKinds: PROPERTY_SEARCH_KINDS,
    coverageNote:
      "CAP Government/Citizenserve building permits only; account-required application submission is not used.",
    skipReason: null,
    citizenserveInstallationId: 261,
    permitTypeTokens: Object.freeze(["west park"]),
    validatedSample: null,
  }),
  wilton_manors: Object.freeze({
    key: "wilton_manors",
    city: "Wilton Manors",
    sourceSystem: "broward_wilton_manors_citizenserve_permits",
    vendor: "citizenserve",
    officialSourceUrl:
      "https://www.wiltonmanors.gov/DocumentCenter/View/9768/How-to-do-an-online-permit-record-search",
    portalBaseUrl: "https://www6.citizenserve.com/Portal",
    anonymousSearchCertified: true,
    searchKinds: PROPERTY_SEARCH_KINDS,
    coverageNote:
      "Citizenserve search/view surface only; files unavailable in the portal require the City's official records route.",
    skipReason: null,
    citizenserveInstallationId: 125,
    permitTypeTokens: Object.freeze(["wilton manors"]),
    validatedSample: null,
  }),
});

/**
 * Resolve and validate a configured Broward municipal permit source.
 *
 * @param {unknown} key - Candidate jurisdiction key.
 * @returns {BrowardPermitJurisdictionConfig} Immutable configuration.
 */
export function getBrowardPermitJurisdiction(key) {
  if (typeof key !== "string" || key.trim().length === 0) {
    throw new Error("Broward permit jurisdiction key is required");
  }
  const config = BROWARD_PERMIT_JURISDICTIONS[key.trim()];
  if (config === undefined) {
    throw new Error(`Unknown Broward permit jurisdiction: ${key.trim()}`);
  }
  return config;
}
