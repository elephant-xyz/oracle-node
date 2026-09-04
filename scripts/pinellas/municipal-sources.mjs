/**
 * Pinellas municipal permit portals that are not county Accela PINELLAS.
 *
 * HTTP probes confirm the public homepage. Harvest adapters are per vendor:
 * Clearwater Accela reuses the county date-window runner; Largo is Tyler Civic
 * Access; Pinellas Park Portico is an apply launcher over EnerGov CSS; Tarpon
 * Springs is Click2Gov HTTP; St. Pete still needs a public search-by-parcel
 * certification.
 */

/**
 * @typedef {object} MunicipalPermitSource
 * @property {string} key Stable source key.
 * @property {string} jurisdiction City or agency name.
 * @property {string} vendor Adapter family.
 * @property {string} probeUrl Public URL to HTTP-probe.
 * @property {string} harvestMode How this source is collected.
 * @property {"ready" | "adapter-ready" | "probe-next" | "needs-review"} status Adapter readiness.
 */

/** @type {readonly MunicipalPermitSource[]} */
export const PINELLAS_MUNICIPAL_PERMIT_SOURCES = [
  {
    key: "clearwater-accela",
    jurisdiction: "City of Clearwater",
    vendor: "accela",
    probeUrl:
      "https://aca-prod.accela.com/CLEARWATER/Cap/CapHome.aspx?TabName=Home&module=Building",
    harvestMode:
      "date-window Accela via scripts/run-pinellas-permit-harvest.mjs --agency clearwater",
    status: "ready",
  },
  {
    key: "largo-energov",
    jurisdiction: "City of Largo",
    vendor: "tyler-civic-access",
    probeUrl: "https://cityoflargofl-energovweb.tylerhost.net/apps/selfservice",
    harvestMode:
      "Tyler Civic Access keyword search (scripts/probe-pinellas-tyler-civic-access.mjs --agency largo; harvest: scripts/run-pinellas-tyler-permit-harvest.mjs)",
    status: "adapter-ready",
  },
  {
    key: "pinellas-park-tyler",
    jurisdiction: "City of Pinellas Park",
    vendor: "tyler-energov-css",
    probeUrl:
      "https://pinellasparkfl.tylerportico.com/navigator/public/selections/navigator?parentId=5996",
    harvestMode:
      "Portico is an apply launcher; search is EnerGov CSS https://egcss.pinellas-park.com/energov_prod/selfservice (scripts/probe-pinellas-park-portico.mjs; Chrome probe --agency park)",
    status: "adapter-ready",
  },
  {
    key: "tarpon-springs-click2gov",
    jurisdiction: "City of Tarpon Springs",
    vendor: "click2gov",
    probeUrl: "https://tarp-egov.aspgov.com/Click2GovBP/index.html",
    harvestMode:
      "Click2Gov HTTP address search + Status Detail (scripts/probe-tarpon-springs-click2gov.mjs; harvest: scripts/run-tarpon-springs-permit-harvest.mjs)",
    status: "adapter-ready",
  },
  {
    key: "dunedin-css",
    jurisdiction: "City of Dunedin",
    vendor: "tyler-css",
    probeUrl:
      "https://www.dunedin.gov/City-Services/Business-Development/Building-Codes-Permits-Construction/Permits-Inspections",
    harvestMode:
      "Tyler EP&L Citizen Self Service; CSS search URL not certified",
    status: "needs-review",
  },
  {
    key: "st-petersburg",
    jurisdiction: "City of St. Petersburg",
    vendor: "city-site-projectdox",
    probeUrl:
      "https://www.stpete.org/business/building_permitting/building_permits.php",
    harvestMode:
      "City page + ProjectDox ePlan; public parcel search not certified",
    status: "needs-review",
  },
];

/**
 * @param {string} value Candidate URL.
 * @returns {boolean} True when the URL is https.
 */
export function isHttpsProbeUrl(value) {
  try {
    return new URL(value).protocol === "https:";
  } catch {
    return false;
  }
}
