// @ts-check

/**
 * Broward's permit custody is split between 31 municipalities and the
 * unincorporated Broward Municipal Services District (BMSD). This registry is
 * deliberately explicit: a portal listed by BCS is never treated as evidence
 * that BCS owns every municipality's current permit records.
 */

/**
 * @typedef {"implemented" | "adapter_unavailable" | "captcha_required" | "login_required" | "custodian_only" | "egress_unavailable"} BrowardPermitSourceStatus
 */

/**
 * @typedef {"current" | "historical"} BrowardPermitCoverageKind
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

export const BROWARD_PERMIT_REGISTRY_VERSION = "2026-08-29.1";
export const BROWARD_BCS_ADAPTER_KEY = "broward-bcs-posse";
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
      sourceKey: "coconut_creek_permit_status",
      sourceName: "Coconut Creek Permit Status",
      sourceUrl:
        "https://www3.coconutcreek.gov/sd/permit/permit_status_01.asp",
      adapterKey: "coconut-creek-permit-status",
      status: "adapter_unavailable",
      reason: "Anonymous official search is documented; no local adapter is implemented.",
    }),
  }),
  jurisdiction({
    key: "cooper-city",
    name: "Cooper City",
    aliases: ["COOPER CITY"],
    primarySource: currentSource({
      sourceKey: "cooper_city_accela",
      sourceName: "Cooper City Accela Citizen Access",
      sourceUrl: "https://aca-prod.accela.com/COOPER/",
      adapterKey: "accela-citizen-access",
      status: "adapter_unavailable",
      reason: "Official Accela portal is identified; this agency adapter is not implemented.",
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
      reason: "The unattended search requires reCAPTCHA and is skipped without bypass.",
    }),
  }),
  jurisdiction({
    key: "dania-beach",
    name: "Dania Beach",
    aliases: ["DANIA", "DANIA BEACH"],
    primarySource: currentSource({
      sourceKey: "dania_beach_tyler_esuite",
      sourceName: "Dania Beach Tyler eSuite",
      sourceUrl:
        "https://cityofdaniabeachfl.nwerp.tylerapp.com/nwprod/eSuite.Permits/",
      adapterKey: "tyler-esuite",
      status: "adapter_unavailable",
      reason: "Anonymous official search is documented; no eSuite adapter is implemented.",
    }),
  }),
  jurisdiction({
    key: "davie",
    name: "Davie",
    aliases: ["DAVIE"],
    primarySource: currentSource({
      sourceKey: "davie_esuite",
      sourceName: "Davie eSuite Permits",
      sourceUrl:
        "https://esuite.davie-fl.gov/eSuite.Permits/AdvancedSearchPage/AdvancedSearch.aspx",
      adapterKey: "tyler-esuite",
      status: "adapter_unavailable",
      reason: "Public address inquiry is documented; no eSuite adapter is implemented.",
    }),
  }),
  jurisdiction({
    key: "deerfield-beach",
    name: "Deerfield Beach",
    aliases: ["DEERFIELD", "DEERFIELD BEACH"],
    primarySource: currentSource({
      sourceKey: "deerfield_beach_split_history",
      sourceName: "Deerfield Beach Gov-Easy / GeoCivix",
      sourceUrl:
        "https://apps.gov-easy.com/Home/PermitInspection/Search?clientId=dce877e0-e162-4827-a60d-7249ec4e8fe2",
      adapterKey: "goveasy-geocivix-split",
      status: "adapter_unavailable",
      reason: "Legacy and post-2025 records are split; neither bounded adapter is implemented.",
    }),
  }),
  jurisdiction({
    key: "fort-lauderdale",
    name: "Fort Lauderdale",
    aliases: ["FORT LAUDERDALE", "FT LAUDERDALE", "FT. LAUDERDALE"],
    primarySource: currentSource({
      sourceKey: "fort_lauderdale_accela",
      sourceName: "Fort Lauderdale LauderBuild",
      sourceUrl: "https://aca3.accela.com/FTL/",
      adapterKey: "accela-citizen-access",
      status: "adapter_unavailable",
      reason: "Anonymous basic search is documented; the FTL agency adapter is not implemented.",
    }),
  }),
  jurisdiction({
    key: "hallandale-beach",
    name: "Hallandale Beach",
    aliases: ["HALLANDALE", "HALLANDALE BEACH"],
    primarySource: currentSource({
      sourceKey: "hallandale_beach_energov",
      sourceName: "Hallandale Beach EnerGov",
      sourceUrl: "https://cohb.org/Faq.aspx?QID=75",
      adapterKey: "tyler-energov",
      status: "adapter_unavailable",
      reason: "Official anonymous search is documented; no EnerGov adapter is implemented.",
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
        "https://app.communitycore.com/app/public-portal/c98c7b46-2cba-4ba2-bbd5-7a76966f42dd",
      adapterKey: "communitycore",
      status: "login_required",
      reason: "Record status and inspection access require an account; unattended login is skipped.",
    }),
  }),
  jurisdiction({
    key: "hollywood",
    name: "Hollywood",
    aliases: ["HOLLYWOOD"],
    primarySource: currentSource({
      sourceKey: "hollywood_split_history",
      sourceName: "Hollywood Permit Status / Accela",
      sourceUrl: "https://apps.hollywoodfl.org/building/PermitStatus.aspx",
      adapterKey: "hollywood-legacy-accela-split",
      status: "adapter_unavailable",
      reason: "History is split between the city search, Accela, and archives; no complete adapter is implemented.",
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
      sourceKey: "lauderdale_by_the_sea_citizenserve",
      sourceName: "Lauderdale-by-the-Sea Citizenserve / CAP Government",
      sourceUrl:
        "https://www6.citizenserve.com/Portal/PortalController?Action=showHomePage&ctzPagePrefix=Portal_&installationID=117",
      adapterKey: "citizenserve-cap",
      status: "adapter_unavailable",
      reason: "CAP Government is the current official service; no Citizenserve adapter is implemented.",
    }),
    supplementalSources: [
      Object.freeze({
        sourceKey: "lauderdale_by_the_sea_historical_bcs_posse",
        sourceName: "Historical Broward BCS-held records",
        sourceUrl: BCS_URL,
        adapterKey: BROWARD_BCS_ADAPTER_KEY,
        status: "implemented",
        coverageKind: "historical",
        reason: "The validated BCS pilot proved historical town records; this is not current-custody coverage.",
      }),
    ],
  }),
  jurisdiction({
    key: "lauderdale-lakes",
    name: "Lauderdale Lakes",
    aliases: ["LAUDERDALE LAKES"],
    primarySource: currentSource({
      sourceKey: "lauderdale_lakes_opengov",
      sourceName: "Lauderdale Lakes OpenGov",
      sourceUrl: "https://lauderdalelakesfl.portal.opengov.com/search",
      adapterKey: "opengov",
      status: "adapter_unavailable",
      reason: "Public record search is documented; no OpenGov adapter is implemented.",
    }),
  }),
  jurisdiction({
    key: "lauderhill",
    name: "Lauderhill",
    aliases: ["LAUDERHILL"],
    primarySource: currentSource({
      sourceKey: "lauderhill_egovplus",
      sourceName: "Lauderhill eGovPLUS",
      sourceUrl: "http://egov.lauderhill-fl.gov/eGovPlus83/permit/perm_status.aspx",
      adapterKey: "egovplus",
      status: "adapter_unavailable",
      reason: "Public folio/address search is documented; no eGovPLUS adapter is implemented.",
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
      sourceKey: "lighthouse_point_smartgov",
      sourceName: "Lighthouse Point SmartGov",
      sourceUrl:
        "https://ci-lighthousepoint-fl.smartgovcommunity.com/ApplicationPublic/ApplicationHome",
      adapterKey: "granicus-smartgov",
      status: "adapter_unavailable",
      reason: "Anonymous official search is documented; no SmartGov adapter is implemented.",
    }),
  }),
  jurisdiction({
    key: "margate",
    name: "Margate",
    aliases: ["MARGATE"],
    primarySource: currentSource({
      sourceKey: "margate_click2gov",
      sourceName: "Margate Click2Gov",
      sourceUrl: "https://marg-egov.aspgov.com/Click2GovBP/selectpermit.html",
      adapterKey: "click2gov",
      status: "adapter_unavailable",
      reason: "Public parcel/address search is documented; no Click2Gov adapter is implemented.",
    }),
  }),
  jurisdiction({
    key: "miramar",
    name: "Miramar",
    aliases: ["MIRAMAR"],
    primarySource: currentSource({
      sourceKey: "miramar_online_permitting",
      sourceName: "Miramar Online Permitting",
      sourceUrl:
        "https://www.miramarfl.gov/Departments/Building-Planning-Zoning/Building-Permits-Inspections/Online-Permitting",
      adapterKey: "miramar-online-permitting",
      status: "adapter_unavailable",
      reason: "Official search route is documented; its vendor adapter is not implemented.",
    }),
  }),
  jurisdiction({
    key: "north-lauderdale",
    name: "North Lauderdale",
    aliases: ["NORTH LAUDERDALE"],
    primarySource: currentSource({
      sourceKey: "north_lauderdale_energov",
      sourceName: "North Lauderdale EnerGov CSS",
      sourceUrl:
        "https://nlselfservice.nlauderdale.org/Energov_prod/SelfService#/home",
      adapterKey: "tyler-energov",
      status: "login_required",
      reason: "The official search requires login; unattended login is skipped.",
    }),
  }),
  jurisdiction({
    key: "oakland-park",
    name: "Oakland Park",
    aliases: ["OAKLAND PARK"],
    primarySource: currentSource({
      sourceKey: "oakland_park_split_history",
      sourceName: "Oakland Park Permit Access",
      sourceUrl: "https://oaklandparkfl.gov/312/Permit-Access",
      adapterKey: "oakland-park-legacy-tyler-split",
      status: "adapter_unavailable",
      reason: "Pre-2019 and Tyler post-2019 records are split; no complete adapter is implemented.",
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
      reason: "Permit and inspection search requires a free account; unattended login is skipped.",
    }),
  }),
  jurisdiction({
    key: "pembroke-park",
    name: "Pembroke Park",
    aliases: ["PEMBROKE PARK", "PEMBROKE PK"],
    primarySource: currentSource({
      sourceKey: "pembroke_park_goveasy",
      sourceName: "Pembroke Park Gov-Easy",
      sourceUrl: "https://www.tppfl.gov/194/Online-Permitting-System",
      adapterKey: "gov-easy",
      status: "adapter_unavailable",
      reason: "Official status search is documented; no Gov-Easy adapter is implemented.",
    }),
  }),
  jurisdiction({
    key: "pembroke-pines",
    name: "Pembroke Pines",
    aliases: ["PEMBROKE PINES"],
    primarySource: currentSource({
      sourceKey: "pembroke_pines_tyler",
      sourceName: "Pembroke Pines Tyler Civic Access",
      sourceUrl:
        "https://pembrokepinesfl-energovweb.tylerhost.net/apps/selfservice",
      adapterKey: "tyler-civic-access",
      status: "adapter_unavailable",
      reason: "Official Tyler portal is identified; this agency adapter is not implemented.",
    }),
  }),
  jurisdiction({
    key: "plantation",
    name: "Plantation",
    aliases: ["PLANTATION"],
    primarySource: currentSource({
      sourceKey: "plantation_accela",
      sourceName: "Plantation Accela Citizen Access",
      sourceUrl:
        "https://aca.plantation.org/CitizenAccess/Cap/CapHome.aspx?TabName=Building&module=Building",
      adapterKey: "accela-citizen-access",
      status: "adapter_unavailable",
      reason: "Official parcel/address search is identified; the agency adapter is not implemented.",
    }),
  }),
  jurisdiction({
    key: "pompano-beach",
    name: "Pompano Beach",
    aliases: ["POMPANO", "POMPANO BEACH"],
    primarySource: currentSource({
      sourceKey: "pompano_beach_click2gov",
      sourceName: "Pompano Beach Click2Gov",
      sourceUrl:
        "https://c2g.pompanobeachfl.gov/Click2GovBP/selectpermit.html",
      adapterKey: "click2gov",
      status: "adapter_unavailable",
      reason: "Public parcel/address search is identified; no Click2Gov adapter is implemented.",
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
      reason: "Only the official municipal custodian is certified; no anonymous record endpoint is available.",
    }),
  }),
  jurisdiction({
    key: "southwest-ranches",
    name: "Southwest Ranches",
    aliases: ["SOUTHWEST RANCHES", "SW RANCHES"],
    primarySource: currentSource({
      sourceKey: "southwest_ranches_citizenserve",
      sourceName: "Southwest Ranches Citizenserve",
      sourceUrl:
        "https://www6.citizenserve.com/Portal/PortalController?Action=showSearchPage&ctzPagePrefix=Portal_&installationID=117&original_contactID=0&original_iid=0",
      adapterKey: "citizenserve-cap",
      status: "adapter_unavailable",
      reason: "Public parcel/address search is documented; no Citizenserve adapter is implemented.",
    }),
  }),
  jurisdiction({
    key: "sunrise",
    name: "Sunrise",
    aliases: ["SUNRISE"],
    primarySource: currentSource({
      sourceKey: "sunrise_building_records",
      sourceName: "Sunrise Building Records",
      sourceUrl:
        "https://www.sunrisefl.gov/departments-services/community-development/building/building-records",
      adapterKey: null,
      status: "egress_unavailable",
      reason: "Official records-request custody is documented; online self-service returned HTTP 403 from this environment.",
    }),
  }),
  jurisdiction({
    key: "tamarac",
    name: "Tamarac",
    aliases: ["TAMARAC"],
    primarySource: currentSource({
      sourceKey: "tamarac_click2gov",
      sourceName: "Tamarac Click2Gov",
      sourceUrl: "https://e-gov.tamarac.org/Click2GovBP/selectpermit.html",
      adapterKey: "click2gov",
      status: "adapter_unavailable",
      reason: "Public property permit history is documented; no Click2Gov adapter is implemented.",
    }),
  }),
  jurisdiction({
    key: "west-park",
    name: "West Park",
    aliases: ["WEST PARK"],
    primarySource: currentSource({
      sourceKey: "west_park_citizenserve",
      sourceName: "West Park Citizenserve",
      sourceUrl:
        "https://www6.citizenserve.com/Portal/PortalController?Action=showSearchPage&ctzPagePrefix=Portal_&installationID=261&original_contactID=0&original_iid=0",
      adapterKey: "citizenserve-cap",
      status: "adapter_unavailable",
      reason: "Public parcel/address search is documented; no Citizenserve adapter is implemented.",
    }),
  }),
  jurisdiction({
    key: "weston",
    name: "Weston",
    aliases: ["WESTON"],
    primarySource: currentSource({
      sourceKey: "weston_accela",
      sourceName: "Weston Accela Citizen Access",
      sourceUrl:
        "https://aca-prod.accela.com/weston/Cap/CapHome.aspx?TabName=Building&module=Building",
      adapterKey: "accela-citizen-access",
      status: "adapter_unavailable",
      reason: "Public parcel/address search is documented; the agency adapter is not implemented.",
    }),
  }),
  jurisdiction({
    key: "wilton-manors",
    name: "Wilton Manors",
    aliases: ["WILTON MANORS"],
    primarySource: currentSource({
      sourceKey: "wilton_manors_citizenserve",
      sourceName: "Wilton Manors Citizenserve",
      sourceUrl:
        "https://www.wiltonmanors.gov/DocumentCenter/View/9768/How-to-do-an-online-permit-record-search",
      adapterKey: "citizenserve",
      status: "adapter_unavailable",
      reason: "Official parcel/address search is documented; no Citizenserve adapter is implemented.",
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
      throw new Error(`Duplicate Broward jurisdiction alias: ${normalizedAlias}`);
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
  return parts.length === 0 ? null : parts.join(" ").replace(/\s+/gu, " ").trim();
}

/**
 * Return every source route that must be reconciled for one parcel.
 *
 * The primary route is always retained, even when inaccessible. Supplemental
 * routes are limited to registry-certified historical custody such as
 * Lauderdale-by-the-Sea records already proven in BCS.
 *
 * @param {BrowardPermitJurisdiction} entry - Resolved jurisdiction registry row.
 * @returns {readonly BrowardPermitSourceRoute[]} Current route followed by bounded historical routes.
 */
export function sourcesForBrowardPermitJurisdiction(entry) {
  return [entry.primarySource, ...entry.supplementalSources];
}
