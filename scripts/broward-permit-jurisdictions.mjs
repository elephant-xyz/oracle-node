// @ts-check

/**
 * Broward's permit custody is split between 31 municipalities and the
 * unincorporated Broward Municipal Services District (BMSD). This registry is
 * deliberately explicit: a portal listed by BCS is never treated as evidence
 * that BCS owns every municipality's current permit records.
 */

/**
 * @typedef {"implemented" | "adapter_unavailable" | "captcha_required" | "login_required" | "no_anonymous_search" | "custodian_only" | "egress_unavailable"} BrowardPermitSourceStatus
 */

/**
 * @typedef {"current" | "historical" | "supplemental"} BrowardPermitCoverageKind
 */

/**
 * @typedef {object} BrowardPermitSourceRoute
 * @property {string} sourceKey - Stable source identity used in checkpoint and coverage artifacts.
 * @property {string} sourceName - Human-readable official source or custodian.
 * @property {string} sourceUrl - Official source/custodian URL.
 * @property {string | null} adapterKey - Vendor adapter key, including planned-but-unimplemented adapters.
 * @property {BrowardPermitSourceStatus} status - Whether unattended local execution is currently implemented or explicitly unavailable.
 * @property {BrowardPermitCoverageKind} coverageKind - Current custody or intentionally bounded historical coverage.
 * @property {string} reason - Auditable explanation of the route and any access limitation.
 */

/**
 * @typedef {object} BrowardPermitJurisdiction
 * @property {string} key - Stable lowercase jurisdiction key.
 * @property {string} name - Official jurisdiction display name.
 * @property {readonly string[]} aliases - BCPA situs-city spellings accepted for this jurisdiction.
 * @property {BrowardPermitSourceRoute} primarySource - Current official permit source or custodian route.
 * @property {readonly BrowardPermitSourceRoute[]} supplementalSources - Narrow historical sources proven for this jurisdiction.
 */

/**
 * @typedef {object} BrowardPermitJurisdictionResolution
 * @property {BrowardPermitJurisdiction | null} jurisdiction - Resolved registry row, or null when BCPA evidence is insufficient.
 * @property {"situs_city" | "situs_address" | "unresolved"} method - BCPA field that established the jurisdiction.
 * @property {string | null} rawCity - Unmodified BCPA situs-city text.
 * @property {string | null} rawAddress - Collapsed BCPA situs-address text used for fallback matching.
 */

export const BROWARD_PERMIT_REGISTRY_VERSION = "2026-09-01.2";
export const BROWARD_BCS_ADAPTER_KEY = "broward-bcs-posse";
export const BROWARD_ACCELA_ADAPTER_KEY = "broward-accela";
export const BROWARD_TYLER_CIVIC_ACCESS_ADAPTER_KEY = "tyler-civic-access";
export const BROWARD_CITIZENSERVE_ADAPTER_KEY = "citizenserve";
export const BROWARD_COCONUT_CREEK_ADAPTER_KEY = "coconut-creek-permit-status";
export const BROWARD_CLICK2GOV_ADAPTER_KEY = "click2gov";
export const BROWARD_TYLER_ESUITE_ADAPTER_KEY = "tyler-esuite";
export const BROWARD_SMARTGOV_ADAPTER_KEY = "granicus-smartgov";
export const BROWARD_EGOVPLUS_ADAPTER_KEY = "egovplus";
const BCS_URL =
  "https://dpepp.broward.org/BCS/Default.aspx?PossePresentation=ParcelSearchByAddress";
const BUILDING_CONTACTS_URL =
  "https://www.broward.org/CodeAppeals/Pages/BuildingContacts.aspx";

/**
 * Construct one immutable current source route.
 *
 * @param {Omit<BrowardPermitSourceRoute, "coverageKind">} source - Current-source route fields.
 * @returns {BrowardPermitSourceRoute} Immutable current route.
 */
function currentSource(source) {
  return Object.freeze({ ...source, coverageKind: "current" });
}

/**
 * Construct one immutable jurisdiction registry row.
 *
 * @param {object} input - Jurisdiction identity and source routes.
 * @param {string} input.key - Stable jurisdiction key.
 * @param {string} input.name - Official display name.
 * @param {readonly string[]} input.aliases - BCPA situs-city aliases.
 * @param {BrowardPermitSourceRoute} input.primarySource - Current source route.
 * @param {readonly BrowardPermitSourceRoute[]} [input.supplementalSources=[]] - Proven historical routes.
 * @returns {BrowardPermitJurisdiction} Immutable registry row.
 */
function jurisdiction({
  key,
  name,
  aliases,
  primarySource,
  supplementalSources = [],
}) {
  return Object.freeze({
    key,
    name,
    aliases: Object.freeze([...aliases]),
    primarySource,
    supplementalSources: Object.freeze([...supplementalSources]),
  });
}

/**
 * Exactly 32 permit jurisdictions: BMSD/unincorporated plus 31 municipalities.
 *
 * A non-null adapter key names the vendor-specific implementation boundary; it
 * does not imply that implementation exists. Only routes with status
 * `implemented` may issue requests. Login, CAPTCHA, egress-denied, and
 * custodian-only rows are terminal source-availability outcomes.
 *
 * @type {readonly BrowardPermitJurisdiction[]}
 */
export const BROWARD_PERMIT_JURISDICTIONS = Object.freeze([
  jurisdiction({
    key: "unincorporated-broward",
    name: "Broward Municipal Services District / unincorporated",
    aliases: [
      "UNINCORPORATED",
      "UNINCORPORATED BROWARD",
      "BROWARD MUNICIPAL SERVICES DISTRICT",
      "BMSD",
    ],
    primarySource: currentSource({
      sourceKey: "broward_bmsd_bcs_posse",
      sourceName: "Broward County Building Code Services (BCS)",
      sourceUrl: BCS_URL,
      adapterKey: BROWARD_BCS_ADAPTER_KEY,
      status: "implemented",
      reason: "BCS is the official BMSD/unincorporated permit custodian.",
    }),
  }),
  jurisdiction({
    key: "coconut-creek",
    name: "Coconut Creek",
    aliases: ["COCONUT CREEK", "COCONUT CRK"],
    primarySource: currentSource({
      sourceKey: "broward_coconut_creek_permit_status",
      sourceName: "Coconut Creek Permit Status",
      sourceUrl: "https://www3.coconutcreek.gov/sd/permit/permit_status_01.asp",
      adapterKey: BROWARD_COCONUT_CREEK_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded anonymous legacy ASP transport reconciled one exact private-folio search, selected detail, and stable permit identity.",
    }),
  }),
  jurisdiction({
    key: "cooper-city",
    name: "Cooper City",
    aliases: ["COOPER CITY"],
    primarySource: currentSource({
      sourceKey: "broward_cooper_city_accela_permits",
      sourceName: "Cooper City Accela Citizen Access",
      sourceUrl:
        "https://aca-prod.accela.com/COOPER/Cap/CapHome.aspx?module=Building&TabName=Building",
      adapterKey: BROWARD_ACCELA_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded anonymous Cooper City Accela adapter is implemented; live evidence is an explicit no-records result only.",
    }),
  }),
  jurisdiction({
    key: "coral-springs",
    name: "Coral Springs",
    aliases: ["CORAL SPRINGS"],
    primarySource: currentSource({
      sourceKey: "coral_springs_etrakit",
      sourceName: "Coral Springs eTRAKiT",
      sourceUrl: "https://etrakit.coralsprings.gov/eTRAKiT/Search/permit.aspx",
      adapterKey: "centralsquare-etrakit",
      status: "captcha_required",
      reason:
        "The unattended search requires reCAPTCHA and is skipped without bypass.",
    }),
  }),
  jurisdiction({
    key: "dania-beach",
    name: "Dania Beach",
    aliases: ["DANIA", "DANIA BEACH"],
    primarySource: currentSource({
      sourceKey: "broward_dania_beach_tyler_esuite_permits",
      sourceName: "Dania Beach Tyler eSuite",
      sourceUrl:
        "https://cityofdaniabeachfl.nwerp.tylerapp.com/nwprod/eSuite.Permits/AdvancedSearchPage/AdvancedSearch.aspx",
      adapterKey: BROWARD_TYLER_ESUITE_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded anonymous eSuite transport reconciled one private address page and ten same-session permit details.",
    }),
  }),
  jurisdiction({
    key: "davie",
    name: "Davie",
    aliases: ["DAVIE"],
    primarySource: currentSource({
      sourceKey: "broward_davie_tyler_esuite_permits",
      sourceName: "Davie eSuite Permits",
      sourceUrl:
        "https://esuite.davie-fl.gov/eSuite.Permits/AdvancedSearchPage/AdvancedSearch.aspx",
      adapterKey: BROWARD_TYLER_ESUITE_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded anonymous eSuite transport reconciled one private address page and ten same-session details. eSuite public history does not establish completeness for new 2026 OAS submissions.",
    }),
  }),
  jurisdiction({
    key: "deerfield-beach",
    name: "Deerfield Beach",
    aliases: ["DEERFIELD", "DEERFIELD BEACH"],
    primarySource: currentSource({
      sourceKey: "deerfield_beach_current_geocivix",
      sourceName: "Deerfield Beach GeoCivix",
      sourceUrl: "https://deerfieldbeach.geocivix.com/secure/",
      adapterKey: "geocivix",
      status: "no_anonymous_search",
      reason:
        "Current GeoCivix is an applicant portal with no anonymous public permit search; registration and authentication are never attempted.",
    }),
    supplementalSources: [
      Object.freeze({
        sourceKey: "deerfield_beach_historical_gov_easy",
        sourceName: "Deerfield Beach legacy Gov-Easy",
        sourceUrl:
          "https://apps.gov-easy.com/Home/PermitInspection/Search?clientId=dce877e0-e162-4827-a60d-7249ec4e8fe2",
        adapterKey: "gov-easy",
        status: "captcha_required",
        coverageKind: "historical",
        reason:
          "The legacy Gov-Easy search requires a numeric CAPTCHA and is skipped without bypass.",
      }),
    ],
  }),
  jurisdiction({
    key: "fort-lauderdale",
    name: "Fort Lauderdale",
    aliases: ["FORT LAUDERDALE", "FT LAUDERDALE", "FT. LAUDERDALE"],
    primarySource: currentSource({
      sourceKey: "broward_fort_lauderdale_lauderbuild_permits",
      sourceName: "Fort Lauderdale LauderBuild",
      sourceUrl:
        "https://aca-prod.accela.com/FTL/Cap/CapHome.aspx?module=Permits&TabName=Permits",
      adapterKey: BROWARD_ACCELA_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded anonymous FTL Permits adapter is implemented; one 50-result probe remains capped before details.",
    }),
  }),
  jurisdiction({
    key: "hallandale-beach",
    name: "Hallandale Beach",
    aliases: ["HALLANDALE", "HALLANDALE BEACH"],
    primarySource: currentSource({
      sourceKey: "broward_hallandale_beach_tyler_permits",
      sourceName: "Hallandale Beach EnerGov",
      sourceUrl:
        "https://hallandalefl-energovpub.tylerhost.net/Apps/SelfService",
      adapterKey: BROWARD_TYLER_CIVIC_ACCESS_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded anonymous Tyler Civic Access adapter is implemented; earliest migrated history remains unverified.",
    }),
  }),
  jurisdiction({
    key: "hillsboro-beach",
    name: "Hillsboro Beach",
    aliases: ["HILLSBORO BEACH"],
    primarySource: currentSource({
      sourceKey: "hillsboro_beach_communitycore",
      sourceName: "Hillsboro Beach CommunityCore",
      sourceUrl:
        "https://app.communitycore.com/app/public-portal/c98c7b46-2cba-4ba2-bbd5-7a76966f42dd/search-permits",
      adapterKey: "communitycore",
      status: "captcha_required",
      reason:
        "The anonymous permit search UI is public, but a normal permit-number test failed reCAPTCHA header validation before any search API call; no CAPTCHA is solved or bypassed.",
    }),
  }),
  jurisdiction({
    key: "hollywood",
    name: "Hollywood",
    aliases: ["HOLLYWOOD"],
    primarySource: currentSource({
      sourceKey: "broward_hollywood_accela_permits",
      sourceName: "Hollywood Accela Citizen Access",
      sourceUrl:
        "https://aca-prod.accela.com/HOLLYWOOD/Cap/CapHome.aspx?module=Building&TabName=Building",
      adapterKey: BROWARD_ACCELA_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded current Accela adapter is implemented; 1988-present legacy address records and older archives remain separate.",
    }),
  }),
  jurisdiction({
    key: "lauderdale-by-the-sea",
    name: "Lauderdale-by-the-Sea",
    aliases: [
      "LAUDERDALE BY THE SEA",
      "LAUDERDALE-BY-THE-SEA",
      "LAUD BY THE SEA",
    ],
    primarySource: currentSource({
      sourceKey: "broward_lauderdale_by_the_sea_citizenserve_permits",
      sourceName: "Lauderdale-by-the-Sea Citizenserve / CAP Government",
      sourceUrl:
        "https://www6.citizenserve.com/Portal/PortalController?Action=showHomePage&ctzPagePrefix=Portal_&installationID=117",
      adapterKey: BROWARD_CITIZENSERVE_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded current Citizenserve adapter is implemented; BCS evidence remains separately historical.",
    }),
    supplementalSources: [
      Object.freeze({
        sourceKey: "lauderdale_by_the_sea_historical_bcs_posse",
        sourceName: "Historical Broward BCS-held records",
        sourceUrl: BCS_URL,
        adapterKey: BROWARD_BCS_ADAPTER_KEY,
        status: "implemented",
        coverageKind: "historical",
        reason:
          "The validated BCS pilot proved historical town records; this is not current-custody coverage.",
      }),
    ],
  }),
  jurisdiction({
    key: "lauderdale-lakes",
    name: "Lauderdale Lakes",
    aliases: ["LAUDERDALE LAKES"],
    primarySource: currentSource({
      sourceKey: "broward_lauderdale_lakes_opengov_permits",
      sourceName: "Lauderdale Lakes OpenGov",
      sourceUrl: "https://lauderdalelakesfl.portal.opengov.com/search",
      adapterKey: "opengov",
      status: "adapter_unavailable",
      reason:
        "The official OpenGov landing still renders the permitting application inaccessible, so the fixture parser remains disabled and no GraphQL request is issued.",
    }),
  }),
  jurisdiction({
    key: "lauderhill",
    name: "Lauderhill",
    aliases: ["LAUDERHILL"],
    primarySource: currentSource({
      sourceKey: "broward_lauderhill_egovplus_permits",
      sourceName: "Lauderhill eGovPLUS",
      sourceUrl:
        "http://egov.lauderhill-fl.gov/eGovPlus83/permit/perm_status.aspx",
      adapterKey: BROWARD_EGOVPLUS_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded anonymous eGovPLUS transport reconciled a private folio search and one exact permit detail over the official legacy route.",
    }),
  }),
  jurisdiction({
    key: "lazy-lake",
    name: "Lazy Lake",
    aliases: ["LAZY LAKE"],
    primarySource: currentSource({
      sourceKey: "lazy_lake_bcs_posse",
      sourceName: "Broward County Building Code Services (BCS)",
      sourceUrl: BCS_URL,
      adapterKey: BROWARD_BCS_ADAPTER_KEY,
      status: "implemented",
      reason: "The official matrix documents BCS as the village permit route.",
    }),
  }),
  jurisdiction({
    key: "lighthouse-point",
    name: "Lighthouse Point",
    aliases: ["LIGHTHOUSE POINT", "LIGHTHOUSE PT"],
    primarySource: currentSource({
      sourceKey: "broward_lighthouse_point_smartgov_permits",
      sourceName: "Lighthouse Point SmartGov",
      sourceUrl:
        "https://ci-lighthousepoint-fl.smartgovcommunity.com/ApplicationPublic/ApplicationHome",
      adapterKey: BROWARD_SMARTGOV_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded anonymous SmartGov transport reconciled a private folio to the source's explicit no-results response; positive detail normalization remains fixture-covered.",
    }),
  }),
  jurisdiction({
    key: "margate",
    name: "Margate",
    aliases: ["MARGATE"],
    primarySource: currentSource({
      sourceKey: "broward_margate_click2gov_permits",
      sourceName: "Margate Click2Gov",
      sourceUrl: "https://marg-egov.aspgov.com/Click2GovBP/selectpermit.html",
      adapterKey: BROWARD_CLICK2GOV_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The shared bounded anonymous Click2Gov transport reconciled one exact private permit and same-session detail.",
    }),
  }),
  jurisdiction({
    key: "miramar",
    name: "Miramar",
    aliases: ["MIRAMAR"],
    primarySource: currentSource({
      sourceKey: "broward_miramar_tyler_permits",
      sourceName: "Miramar Online Permitting",
      sourceUrl: "https://miramarfl-energovweb.tylerhost.net/apps/SelfService",
      adapterKey: BROWARD_TYLER_CIVIC_ACCESS_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded anonymous Tyler Civic Access adapter is implemented; the validated folio produced a typed empty result.",
    }),
  }),
  jurisdiction({
    key: "north-lauderdale",
    name: "North Lauderdale",
    aliases: ["NORTH LAUDERDALE"],
    primarySource: currentSource({
      sourceKey: "broward_north_lauderdale_tyler_permits",
      sourceName: "North Lauderdale EnerGov CSS",
      sourceUrl:
        "https://nlselfservice.nlauderdale.org/Energov_prod/SelfService#/home",
      adapterKey: BROWARD_TYLER_CIVIC_ACCESS_ADAPTER_KEY,
      status: "login_required",
      reason:
        "The official search requires login; unattended login is skipped.",
    }),
  }),
  jurisdiction({
    key: "oakland-park",
    name: "Oakland Park",
    aliases: ["OAKLAND PARK"],
    primarySource: currentSource({
      sourceKey: "broward_oakland_park_tyler_permits",
      sourceName: "Oakland Park Tyler Civic Access",
      sourceUrl:
        "https://oaklandparkfl-energovweb.tylerhost.net/apps/SelfService",
      adapterKey: BROWARD_TYLER_CIVIC_ACCESS_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded Tyler adapter is implemented for post-2019-11-01 records; earlier city records remain separate.",
    }),
  }),
  jurisdiction({
    key: "parkland",
    name: "Parkland",
    aliases: ["PARKLAND"],
    primarySource: currentSource({
      sourceKey: "parkland_mgo_connect",
      sourceName: "Parkland MGO Connect",
      sourceUrl: "https://www.mgoconnect.org/cp/portal",
      adapterKey: "mygovernmentonline",
      status: "login_required",
      reason:
        "Permit and inspection search requires a free account; unattended login is skipped.",
    }),
  }),
  jurisdiction({
    key: "pembroke-park",
    name: "Pembroke Park",
    aliases: ["PEMBROKE PARK", "PEMBROKE PK"],
    primarySource: currentSource({
      sourceKey: "pembroke_park_goveasy",
      sourceName: "Pembroke Park Gov-Easy",
      sourceUrl:
        "https://apps.gov-easy.com/Home/PermitInspection/Search?clientId=d60f9827-2c53-44a4-9037-31e1de2b3f09",
      adapterKey: "gov-easy",
      status: "captcha_required",
      reason:
        "The official Gov-Easy status search requires a numeric CAPTCHA and is skipped without bypass.",
    }),
  }),
  jurisdiction({
    key: "pembroke-pines",
    name: "Pembroke Pines",
    aliases: ["PEMBROKE PINES"],
    primarySource: currentSource({
      sourceKey: "broward_pembroke_pines_tyler_permits",
      sourceName: "Pembroke Pines Tyler Civic Access",
      sourceUrl:
        "https://pembrokepinesfl-energovweb.tylerhost.net/apps/selfservice",
      adapterKey: BROWARD_TYLER_CIVIC_ACCESS_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded anonymous Tyler Civic Access adapter is implemented; portal completeness is not inferred.",
    }),
  }),
  jurisdiction({
    key: "plantation",
    name: "Plantation",
    aliases: ["PLANTATION"],
    primarySource: currentSource({
      sourceKey: "broward_plantation_accela_permits",
      sourceName: "Plantation Accela Citizen Access",
      sourceUrl:
        "https://aca.plantation.org/CitizenAccess/Cap/CapHome.aspx?TabName=Building&module=Building",
      adapterKey: BROWARD_ACCELA_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded framed Accela adapter is implemented; pre-2004 records may require City microfilm.",
    }),
  }),
  jurisdiction({
    key: "pompano-beach",
    name: "Pompano Beach",
    aliases: ["POMPANO", "POMPANO BEACH"],
    primarySource: currentSource({
      sourceKey: "broward_pompano_beach_click2gov_permits",
      sourceName: "Pompano Beach Click2Gov",
      sourceUrl: "https://c2g.pompanobeachfl.gov/Click2GovBP/selectpermit.html",
      adapterKey: BROWARD_CLICK2GOV_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The shared bounded anonymous Click2Gov transport reconciled one exact private permit and same-session detail.",
    }),
  }),
  jurisdiction({
    key: "sea-ranch-lakes",
    name: "Sea Ranch Lakes",
    aliases: ["SEA RANCH LAKES"],
    primarySource: currentSource({
      sourceKey: "sea_ranch_lakes_custodian",
      sourceName: "Sea Ranch Lakes building official",
      sourceUrl: BUILDING_CONTACTS_URL,
      adapterKey: null,
      status: "custodian_only",
      reason:
        "The village custodian is authoritative because no complete anonymous municipal permit search is available.",
    }),
    supplementalSources: [
      Object.freeze({
        sourceKey: "sea_ranch_lakes_supplemental_bcs_posse",
        sourceName:
          "Supplemental Broward BCS-held / associated approval records",
        sourceUrl: BCS_URL,
        adapterKey: BROWARD_BCS_ADAPTER_KEY,
        status: "implemented",
        coverageKind: "supplemental",
        reason:
          "BCS may expose county-held or associated approval records labeled Sea Ranch Lakes; those rows are supplemental and never establish a complete village permit inventory.",
      }),
    ],
  }),
  jurisdiction({
    key: "southwest-ranches",
    name: "Southwest Ranches",
    aliases: ["SOUTHWEST RANCHES", "SW RANCHES"],
    primarySource: currentSource({
      sourceKey: "broward_southwest_ranches_citizenserve_permits",
      sourceName: "Southwest Ranches Citizenserve",
      sourceUrl:
        "https://www6.citizenserve.com/Portal/PortalController?Action=showSearchPage&ctzPagePrefix=Portal_&installationID=117&original_contactID=0&original_iid=0",
      adapterKey: BROWARD_CITIZENSERVE_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded anonymous Citizenserve adapter is implemented for building permits; other Town approvals remain separate.",
    }),
  }),
  jurisdiction({
    key: "sunrise",
    name: "Sunrise",
    aliases: ["SUNRISE"],
    primarySource: currentSource({
      sourceKey: "broward_sunrise_tyler_permits",
      sourceName: "Sunrise EnerGov public information",
      sourceUrl:
        "https://energov.sunrisefl.gov/EnerGov_Prod/SelfService/SunriseFL%20Prod#/search?category=permits",
      adapterKey: BROWARD_TYLER_CIVIC_ACCESS_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded anonymous SunriseFL Prod Tyler adapter is implemented with tenant-aware API routing and strict list reconciliation; portal results are not treated as complete historical holdings.",
    }),
    supplementalSources: [
      Object.freeze({
        sourceKey: "sunrise_building_records_custodian",
        sourceName: "Sunrise Building Records",
        sourceUrl:
          "https://www.sunrisefl.gov/departments-services/community-development/building/building-records",
        adapterKey: null,
        status: "custodian_only",
        coverageKind: "historical",
        reason:
          "The City building-records route remains the custodian fallback for microfilm or records absent from the anonymous portal; no request, email, or form is submitted.",
      }),
    ],
  }),
  jurisdiction({
    key: "tamarac",
    name: "Tamarac",
    aliases: ["TAMARAC"],
    primarySource: currentSource({
      sourceKey: "broward_tamarac_click2gov_permits",
      sourceName: "Tamarac Click2Gov",
      sourceUrl: "https://e-gov.tamarac.org/Click2GovBP/selectpermit.html",
      adapterKey: BROWARD_CLICK2GOV_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The shared bounded anonymous Click2Gov transport reconciled one exact private permit and same-session detail.",
    }),
  }),
  jurisdiction({
    key: "west-park",
    name: "West Park",
    aliases: ["WEST PARK"],
    primarySource: currentSource({
      sourceKey: "broward_west_park_citizenserve_permits",
      sourceName: "West Park Citizenserve",
      sourceUrl:
        "https://www6.citizenserve.com/Portal/PortalController?Action=showSearchPage&ctzPagePrefix=Portal_&installationID=261&original_contactID=0&original_iid=0",
      adapterKey: BROWARD_CITIZENSERVE_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded anonymous Citizenserve search/detail adapter is implemented; account-required submission is excluded.",
    }),
  }),
  jurisdiction({
    key: "weston",
    name: "Weston",
    aliases: ["WESTON"],
    primarySource: currentSource({
      sourceKey: "broward_weston_accela_permits",
      sourceName: "Weston Accela Citizen Access",
      sourceUrl:
        "https://aca-prod.accela.com/weston/Cap/CapHome.aspx?TabName=Building&module=Building",
      adapterKey: BROWARD_ACCELA_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded anonymous Accela adapter is implemented; City records are bounded to post-1997 history.",
    }),
  }),
  jurisdiction({
    key: "wilton-manors",
    name: "Wilton Manors",
    aliases: ["WILTON MANORS"],
    primarySource: currentSource({
      sourceKey: "broward_wilton_manors_citizenserve_permits",
      sourceName: "Wilton Manors Citizenserve",
      sourceUrl:
        "https://www.wiltonmanors.gov/DocumentCenter/View/9768/How-to-do-an-online-permit-record-search",
      adapterKey: BROWARD_CITIZENSERVE_ADAPTER_KEY,
      status: "implemented",
      reason:
        "The bounded anonymous Citizenserve adapter is implemented; unavailable files still require the City records route.",
    }),
  }),
]);

/**
 * Normalize BCPA location text for exact alias comparisons.
 *
 * @param {unknown} value - Candidate BCPA city/address value.
 * @returns {string | null} Uppercase, punctuation-neutral text.
 */
function normalizeLocationText(value) {
  if (typeof value !== "string") return null;
  const normalized = value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/gu, "")
    .toUpperCase()
    .replace(/&/gu, " AND ")
    .replace(/[^A-Z0-9]+/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
  return normalized.length === 0 ? null : normalized;
}

/**
 * Read one optional BCPA source string.
 *
 * @param {Record<string, unknown>} record - BCPA parcel record.
 * @param {string} key - Candidate field key.
 * @returns {string | null} Trimmed non-empty value.
 */
function readOptionalString(record, key) {
  const value = record[key];
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

const ALIAS_TO_JURISDICTION = new Map();
for (const entry of BROWARD_PERMIT_JURISDICTIONS) {
  for (const alias of entry.aliases) {
    const normalizedAlias = normalizeLocationText(alias);
    if (normalizedAlias === null) {
      throw new Error(`Empty Broward jurisdiction alias for ${entry.name}`);
    }
    const existing = ALIAS_TO_JURISDICTION.get(normalizedAlias);
    if (existing !== undefined && existing.key !== entry.key) {
      throw new Error(
        `Duplicate Broward jurisdiction alias: ${normalizedAlias}`,
      );
    }
    ALIAS_TO_JURISDICTION.set(normalizedAlias, entry);
  }
}

if (BROWARD_PERMIT_JURISDICTIONS.length !== 32) {
  throw new Error(
    `Broward permit registry must contain exactly 32 jurisdictions; found ${String(BROWARD_PERMIT_JURISDICTIONS.length)}`,
  );
}
if (
  new Set(BROWARD_PERMIT_JURISDICTIONS.map((entry) => entry.key)).size !== 32
) {
  throw new Error("Broward permit registry jurisdiction keys must be unique");
}

/**
 * Resolve a Broward permit jurisdiction only from BCPA situs evidence.
 *
 * `situsCity` is authoritative when it exactly matches a registered alias.
 * Address fallback accepts a registered city only at the end of the situs
 * address (before an optional Florida/ZIP suffix), avoiding substring guesses
 * such as routing a street named "Weston Road" to the City of Weston. An
 * unknown or missing city remains unresolved and is never defaulted to BCS.
 *
 * @param {Record<string, unknown>} record - One BCPA parcel record.
 * @returns {BrowardPermitJurisdictionResolution} Explicit route resolution evidence.
 */
export function resolveBrowardPermitJurisdiction(record) {
  const rawCity =
    readOptionalString(record, "situsCity") ??
    readOptionalString(record, "siteCity");
  const normalizedCity = normalizeLocationText(rawCity);
  if (normalizedCity !== null) {
    const cityMatch = ALIAS_TO_JURISDICTION.get(normalizedCity);
    if (cityMatch !== undefined) {
      return {
        jurisdiction: cityMatch,
        method: "situs_city",
        rawCity,
        rawAddress: buildRawSitusAddress(record),
      };
    }
  }

  const rawAddress = buildRawSitusAddress(record);
  const normalizedAddress = normalizeLocationText(rawAddress);
  if (normalizedAddress !== null) {
    const addressWithoutStateAndZip = normalizedAddress
      .replace(/\s+(?:FL|FLORIDA)(?:\s+\d{5}(?:\s+\d{4})?)?$/u, "")
      .replace(/\s+\d{5}(?:\s+\d{4})?$/u, "")
      .trim();
    const aliasesByLength = [...ALIAS_TO_JURISDICTION.entries()].sort(
      ([left], [right]) => right.length - left.length,
    );
    for (const [alias, entry] of aliasesByLength) {
      if (
        addressWithoutStateAndZip === alias ||
        addressWithoutStateAndZip.endsWith(` ${alias}`)
      ) {
        return {
          jurisdiction: entry,
          method: "situs_address",
          rawCity,
          rawAddress,
        };
      }
    }
  }

  return {
    jurisdiction: null,
    method: "unresolved",
    rawCity,
    rawAddress,
  };
}

/**
 * Build the complete BCPA situs-address evidence string without owner/mailing
 * data. Both common BCPA field spellings and an already-combined situs address
 * are accepted for fixture and capture compatibility.
 *
 * @param {Record<string, unknown>} record - One BCPA parcel record.
 * @returns {string | null} Collapsed situs-address text.
 */
function buildRawSitusAddress(record) {
  const combined =
    readOptionalString(record, "situsAddress") ??
    readOptionalString(record, "siteAddress");
  if (combined !== null) return combined.replace(/\s+/gu, " ").trim();
  const parts = [
    readOptionalString(record, "situsAddress1"),
    readOptionalString(record, "situsAddress2"),
    readOptionalString(record, "situsCity"),
    readOptionalString(record, "situsState"),
    readOptionalString(record, "situsZipCode"),
  ].filter((value) => value !== null);
  return parts.length === 0
    ? null
    : parts.join(" ").replace(/\s+/gu, " ").trim();
}

/**
 * Return every source route that must be reconciled for one parcel.
 *
 * The primary route is always retained, even when inaccessible. Supplemental
 * routes are limited to registry-certified historical custody or explicitly
 * labeled supplemental county-held approvals. Supplemental BCS records never
 * prove complete municipal coverage.
 *
 * @param {BrowardPermitJurisdiction} entry - Resolved jurisdiction registry row.
 * @returns {readonly BrowardPermitSourceRoute[]} Current route followed by bounded historical routes.
 */
export function sourcesForBrowardPermitJurisdiction(entry) {
  return [entry.primarySource, ...entry.supplementalSources];
}
