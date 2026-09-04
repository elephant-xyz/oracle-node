// @ts-check

/**
 * Jurisdiction routing for the remaining Broward municipal permit vendor
 * families. Access modes are operational controls, not descriptions only:
 * callers must evaluate them before constructing a source transport.
 *
 * @typedef {import("./broward-municipal-core.mjs").BrowardMunicipalJurisdictionConfig} BrowardMunicipalJurisdictionConfig
 */

/**
 * Freeze one complete jurisdiction configuration and its nested collections.
 *
 * @param {BrowardMunicipalJurisdictionConfig} config - Complete source routing configuration.
 * @returns {BrowardMunicipalJurisdictionConfig} Deeply immutable operational configuration.
 */
function defineJurisdiction(config) {
  return Object.freeze({
    ...config,
    capabilities: Object.freeze({
      ...config.capabilities,
      searchBy: Object.freeze([...config.capabilities.searchBy]),
    }),
    supplementalRoutes: Object.freeze(
      config.supplementalRoutes.map((route) => Object.freeze({ ...route })),
    ),
  });
}

const CLICK2GOV_CAPABILITIES = Object.freeze({
  searchBy: Object.freeze(/** @type {const} */ (["permit_number", "address"])),
  pagination: /** @type {const} */ ("client_all"),
  detail: /** @type {const} */ ("same_session"),
  inspections: true,
  planReview: true,
});

const ESUITE_CAPABILITIES = Object.freeze({
  searchBy: Object.freeze(
    /** @type {const} */ (["permit_number", "address", "record_type"]),
  ),
  pagination: /** @type {const} */ ("numbered"),
  detail: /** @type {const} */ ("same_session"),
  inspections: true,
  planReview: true,
});

/**
 * @type {readonly BrowardMunicipalJurisdictionConfig[]}
 */
export const BROWARD_MUNICIPAL_PERMIT_JURISDICTIONS = Object.freeze([
  defineJurisdiction({
    key: "coconut_creek",
    jurisdiction: "Coconut Creek",
    sourceSystem: "broward_coconut_creek_permit_status",
    protocol: "coconut_creek",
    searchUrl: "https://www3.coconutcreek.gov/sd/permit/permit_status_01.asp",
    officialEvidenceUrl:
      "https://www3.coconutcreek.gov/sd/permit/permit_status_01.asp",
    accessMode: "anonymous",
    probeStatus: "enabled",
    accessNote:
      "The official legacy status application anonymously accepts exact permit, property-id, and address searches. Search selection and detail retrieval rotate an ASP session and are serialized.",
    capabilities: {
      searchBy: ["permit_number", "address", "folio"],
      pagination: "client_all",
      detail: "same_session",
      inspections: false,
      planReview: false,
    },
    supplementalRoutes: [],
  }),
  defineJurisdiction({
    key: "pompano_beach",
    jurisdiction: "Pompano Beach",
    sourceSystem: "broward_pompano_beach_click2gov_permits",
    protocol: "click2gov",
    searchUrl: "https://c2g.pompanobeachfl.gov/Click2GovBP/selectpermit.html",
    officialEvidenceUrl:
      "https://c2g.pompanobeachfl.gov/Click2GovBP/selectpermit.html",
    accessMode: "anonymous",
    probeStatus: "enabled",
    accessNote:
      "Anonymous application and address search is available. The segmented source parcel fields have no certified BCPA-folio mapping and are not submitted by the adapter; broad result pages fail at the local row cap before detail traversal.",
    capabilities: CLICK2GOV_CAPABILITIES,
    supplementalRoutes: [],
  }),
  defineJurisdiction({
    key: "tamarac",
    jurisdiction: "Tamarac",
    sourceSystem: "broward_tamarac_click2gov_permits",
    protocol: "click2gov",
    searchUrl: "https://e-gov.tamarac.org/Click2GovBP/selectpermit.html",
    officialEvidenceUrl: "https://tamarac.gov/672/Permit-History",
    accessMode: "anonymous",
    probeStatus: "enabled",
    accessNote:
      "The official city instructions certify anonymous application/address/parcel/name history search. The adapter uses only application/address fields because no BCPA-to-segmented-parcel mapping is certified; authenticated application functions are out of scope.",
    capabilities: CLICK2GOV_CAPABILITIES,
    supplementalRoutes: [],
  }),
  defineJurisdiction({
    key: "margate",
    jurisdiction: "Margate",
    sourceSystem: "broward_margate_click2gov_permits",
    protocol: "click2gov",
    searchUrl: "https://marg-egov.aspgov.com/Click2GovBP/selectpermit.html",
    officialEvidenceUrl:
      "https://marg-egov.aspgov.com/Click2GovBP/selectpermit.html",
    accessMode: "anonymous",
    probeStatus: "enabled",
    accessNote:
      "Anonymous Click2Gov application/address history search is available. Segmented parcel fields are not used without a certified BCPA mapping; the separate inspection scheduler and plan-review portals are not traversed.",
    capabilities: CLICK2GOV_CAPABILITIES,
    supplementalRoutes: [],
  }),
  defineJurisdiction({
    key: "davie",
    jurisdiction: "Davie",
    sourceSystem: "broward_davie_tyler_esuite_permits",
    protocol: "tyler_esuite",
    searchUrl:
      "https://esuite.davie-fl.gov/eSuite.Permits/AdvancedSearchPage/AdvancedSearch.aspx",
    officialEvidenceUrl:
      "https://esuite.davie-fl.gov/eSuite.Permits/WelcomePage.aspx",
    accessMode: "anonymous",
    probeStatus: "enabled",
    accessNote:
      "Legacy/public eSuite search is anonymous. New 2026 submissions use the separately documented OAS route, so eSuite alone is not a completeness claim.",
    capabilities: ESUITE_CAPABILITIES,
    supplementalRoutes: [
      {
        purpose: "new_2026_submissions",
        url: "https://davie-fl-us.avolvecloud.com/",
        accessMode: "login_required",
        note: "Application submission route only; no anonymous record-level harvesting contract was certified.",
      },
    ],
  }),
  defineJurisdiction({
    key: "dania_beach",
    jurisdiction: "Dania Beach",
    sourceSystem: "broward_dania_beach_tyler_esuite_permits",
    protocol: "tyler_esuite",
    searchUrl:
      "https://cityofdaniabeachfl.nwerp.tylerapp.com/nwprod/eSuite.Permits/AdvancedSearchPage/AdvancedSearch.aspx",
    officialEvidenceUrl: "https://www.daniabeachfl.gov/building-division",
    accessMode: "anonymous",
    probeStatus: "enabled",
    accessNote:
      "Public eSuite permit type/number/service-address search is anonymous; contractor account functions are excluded.",
    capabilities: ESUITE_CAPABILITIES,
    supplementalRoutes: [],
  }),
  defineJurisdiction({
    key: "deerfield_beach",
    jurisdiction: "Deerfield Beach",
    sourceSystem: "deerfield_beach_gov_easy_legacy_permits",
    protocol: "gov_easy",
    searchUrl:
      "https://apps.gov-easy.com/Home/PermitInspection/Search?clientId=dce877e0-e162-4827-a60d-7249ec4e8fe2",
    officialEvidenceUrl:
      "https://apps.gov-easy.com/Home/PermitInspection/Search?clientId=dce877e0-e162-4827-a60d-7249ec4e8fe2",
    accessMode: "captcha_required",
    probeStatus: "blocked",
    accessNote:
      "Legacy Gov-Easy renders a six-digit numeric CAPTCHA before search. The adapter records the route and skips without solving, setting session state, or calling its data API.",
    capabilities: {
      searchBy: ["permit_number", "address", "folio"],
      pagination: "numbered",
      detail: "same_session",
      inspections: true,
      planReview: true,
    },
    supplementalRoutes: [
      {
        purpose: "current_applicant_portal_from_2025",
        url: "https://deerfieldbeach.geocivix.com/secure/",
        accessMode: "no_anonymous_search",
        note: "Current GeoCivix is an applicant portal with no anonymous public permit search. The adapter does not register, authenticate, or infer record coverage from this route.",
      },
    ],
  }),
  defineJurisdiction({
    key: "pembroke_park",
    jurisdiction: "Pembroke Park",
    sourceSystem: "broward_pembroke_park_gov_easy_permits",
    protocol: "gov_easy",
    searchUrl:
      "https://apps.gov-easy.com/Home/PermitInspection/Search?clientId=d60f9827-2c53-44a4-9037-31e1de2b3f09",
    officialEvidenceUrl: "https://www.tppfl.gov/194/Online-Permitting-System",
    accessMode: "captcha_required",
    probeStatus: "blocked",
    accessNote:
      "The official town page links Gov-Easy for status, but the shared search requires a numeric CAPTCHA. A user-authorized manually validated browser session may support a bounded private capture while it remains valid; unattended transport stays blocked, and a keyword slice is not complete permit or roofing coverage.",
    capabilities: {
      searchBy: ["permit_number", "address", "folio"],
      pagination: "numbered",
      detail: "same_session",
      inspections: true,
      planReview: true,
    },
    supplementalRoutes: [],
  }),
  defineJurisdiction({
    key: "lighthouse_point",
    jurisdiction: "Lighthouse Point",
    sourceSystem: "broward_lighthouse_point_smartgov_permits",
    protocol: "smartgov",
    searchUrl:
      "https://ci-lighthousepoint-fl.smartgovcommunity.com/ApplicationPublic/ApplicationSearchAdvanced",
    officialEvidenceUrl:
      "https://lighthousepointfl.gov/352/Broward-County-ePermits-OneStop",
    accessMode: "anonymous",
    probeStatus: "enabled",
    accessNote:
      "Anonymous advanced permit/address/parcel and exact record-type search is available. Sign-up/login controls are not needed for public search and are never used.",
    capabilities: {
      searchBy: ["permit_number", "address", "folio", "record_type"],
      pagination: "numbered",
      detail: "public_url",
      inspections: true,
      planReview: false,
    },
    supplementalRoutes: [],
  }),
  defineJurisdiction({
    key: "lauderdale_lakes",
    jurisdiction: "Lauderdale Lakes",
    sourceSystem: "broward_lauderdale_lakes_opengov_permits",
    protocol: "opengov",
    searchUrl: "https://lauderdalelakesfl.portal.opengov.com/search",
    officialEvidenceUrl: "https://lauderdalelakesfl.portal.opengov.com/search",
    accessMode: "anonymous",
    probeStatus: "landing_only",
    accessNote:
      "The official OpenGov search landing is anonymous, but its current SPA reports the permitting application inaccessible in this environment. No GraphQL search is attempted until the rendered public route is healthy.",
    capabilities: {
      searchBy: ["permit_number", "address"],
      pagination: "cursor",
      detail: "public_url",
      inspections: false,
      planReview: false,
    },
    supplementalRoutes: [],
  }),
  defineJurisdiction({
    key: "hillsboro_beach",
    jurisdiction: "Hillsboro Beach",
    sourceSystem: "hillsboro_beach_communitycore_permits",
    protocol: "communitycore",
    searchUrl:
      "https://app.communitycore.com/app/public-portal/c98c7b46-2cba-4ba2-bbd5-7a76966f42dd/search-permits",
    officialEvidenceUrl:
      "https://app.communitycore.com/app/public-portal/c98c7b46-2cba-4ba2-bbd5-7a76966f42dd/search-permits",
    accessMode: "captcha_required",
    probeStatus: "blocked",
    accessNote:
      "The anonymous UI offers jobsite-address, permit-number, parcel-number, and owner fields, but a permit-number test stopped at 'reCaptcha header validation failed' before any search API request. No CAPTCHA is solved or bypassed, and owner search is excluded.",
    capabilities: {
      searchBy: ["permit_number", "address", "folio"],
      pagination: "none",
      detail: "public_url",
      inspections: true,
      planReview: true,
    },
    supplementalRoutes: [],
  }),
  defineJurisdiction({
    key: "parkland",
    jurisdiction: "Parkland",
    sourceSystem: "parkland_mgo_connect_permits",
    protocol: "mgo_connect",
    searchUrl: "https://www.mgoconnect.org/cp/portal",
    officialEvidenceUrl: "https://www.mgoconnect.org/cp/portal",
    accessMode: "login_required",
    probeStatus: "blocked",
    accessNote:
      "MGO Connect requires a free account for permit-project and inspection-result search; the prototype does not register or authenticate.",
    capabilities: {
      searchBy: [],
      pagination: "none",
      detail: "none",
      inspections: false,
      planReview: false,
    },
    supplementalRoutes: [],
  }),
  defineJurisdiction({
    key: "lauderhill",
    jurisdiction: "Lauderhill",
    sourceSystem: "broward_lauderhill_egovplus_permits",
    protocol: "egovplus",
    searchUrl:
      "http://egov.lauderhill-fl.gov/eGovPlus83/permit/perm_status.aspx",
    officialEvidenceUrl:
      "http://egov.lauderhill-fl.gov/eGovPlus83/permit/perm_status.aspx",
    accessMode: "anonymous",
    probeStatus: "enabled",
    accessNote:
      "Anonymous permit/folio/address search and same-origin permit, plan-review, and inspection detail are available over the city's legacy HTTP-only route.",
    capabilities: {
      searchBy: ["permit_number", "address", "folio"],
      pagination: "client_all",
      detail: "public_url",
      inspections: true,
      planReview: true,
    },
    supplementalRoutes: [],
  }),
  defineJurisdiction({
    key: "sunrise",
    jurisdiction: "Sunrise",
    sourceSystem: "broward_sunrise_tyler_permits",
    protocol: "tyler_energov",
    searchUrl:
      "https://energov.sunrisefl.gov/EnerGov_Prod/SelfService/SunriseFL%20Prod#/search?category=permits",
    officialEvidenceUrl:
      "https://energov.sunrisefl.gov/EnerGov_Prod/SelfService/SunriseFL%20Prod#/search?category=permits",
    accessMode: "anonymous",
    probeStatus: "enabled",
    accessNote:
      "The official SunriseFL Prod tenant reports authentication is not required and exposes anonymous permit search/setup APIs. The shared bounded Tyler adapter uses tenant headers, strict page totals, deadlines, private checkpoints, and no credentials.",
    capabilities: {
      searchBy: ["permit_number", "address", "folio"],
      pagination: "numbered",
      detail: "public_url",
      inspections: true,
      planReview: true,
    },
    supplementalRoutes: [
      {
        purpose: "official_building_records_request_form",
        url: "https://www.sunrisefl.gov/home/showpublisheddocument/8444/638949136343770000",
        accessMode: "records_request",
        note: "Official form accepts property address and property folio for permit history/open-permit requests; retrieval fees may apply.",
      },
      {
        purpose: "city_clerk_public_records_custodian",
        url: "https://www.sunrisefl.gov/departments-services/city-clerk/public-records-lien-searches",
        accessMode: "records_request",
        note: "General city public-record custodian fallback; no request is submitted by oracle-node.",
      },
    ],
  }),
]);

/** @type {ReadonlyMap<string, BrowardMunicipalJurisdictionConfig>} */
const CONFIG_BY_KEY = new Map(
  BROWARD_MUNICIPAL_PERMIT_JURISDICTIONS.map((config) => [config.key, config]),
);

if (CONFIG_BY_KEY.size !== BROWARD_MUNICIPAL_PERMIT_JURISDICTIONS.length) {
  throw new Error("Duplicate Broward municipal permit jurisdiction key");
}

/**
 * Resolve one immutable Broward municipal permit source configuration.
 *
 * @param {string} key - Lowercase jurisdiction key.
 * @returns {BrowardMunicipalJurisdictionConfig} Exact configuration.
 */
export function getBrowardMunicipalPermitConfig(key) {
  const config = CONFIG_BY_KEY.get(key);
  if (config === undefined) {
    throw new Error(`Unknown Broward municipal permit jurisdiction: ${key}`);
  }
  return config;
}
