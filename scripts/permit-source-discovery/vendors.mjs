// Vendor-signature classifier for building-permit portals.
//
// This is the reusable, deterministic core of the permit-source-discovery agent.
// US building permits are fragmented across ~25k-60k municipal/township/borough
// portals, but those portals are almost all built on a small number of permitting
// software vendors (Accela, Tyler, OpenGov, ...). Recognizing the vendor from a
// portal URL or its HTML is what lets us catalog a jurisdiction's portal and pick
// the right scraping adapter downstream.
//
// Pure and synchronous: no network, no filesystem. Unit-testable in isolation.

/**
 * @typedef {object} VendorSignature
 * @property {string} key - Stable vendor key used across the pipeline.
 * @property {string} name - Human-readable vendor/product name.
 * @property {readonly RegExp[]} urlPatterns - Patterns matched against a portal URL (highest confidence).
 * @property {readonly string[]} htmlMarkers - Case-insensitive substrings matched against portal HTML.
 * @property {boolean | null} searchByParcel - Whether the portal searches by parcel/PCN id; null when it varies per deployment.
 * @property {string} notes - Short operator note about the vendor family.
 */

/**
 * @typedef {object} VendorClassification
 * @property {string} key - Detected vendor key, or `unknown`.
 * @property {string} name - Detected vendor name, or `unknown`.
 * @property {"url" | "html" | "none"} confidence - How the match was made.
 * @property {boolean | null} searchByParcel - Parcel-search capability of the detected vendor.
 */

/**
 * Known permit-portal vendor families seen across Florida jurisdictions so far.
 * Seed list — extend as new vendors are discovered (the catalog row's stated
 * vendor is the source of truth that `certify.mjs` checks this list against).
 *
 * @type {readonly VendorSignature[]}
 */
export const VENDORS = [
  {
    key: "accela",
    name: "Accela Citizen Access",
    urlPatterns: [/\.accela\.com\//i, /aca-?prod/i],
    htmlMarkers: ["Accela"],
    searchByParcel: null,
    notes: "Accela Citizen Access (ACA). Search capabilities vary per deployment.",
  },
  {
    key: "tyler_epl",
    name: "Tyler EPL / Civic Access",
    urlPatterns: [
      /civicaccess/i,
      /energov/i,
      /\.tylertech/i,
      /permit-planner\./i,
      /tylerhost\.net/i,
      /energovweb/i,
      /munisselfservice\.com/i,
      /edenweb/i,
      /eden\.[a-z]/i,
      /Default\.aspx\?Build=PM\.pmPermit/i,
    ],
    htmlMarkers: [
      "Civic Access",
      "EnerGov",
      "Tyler",
      "Munis",
      "EDEN",
      "Self Service",
    ],
    searchByParcel: false,
    notes: "Tyler Technologies EnerGov / Civic Access / Munis / EDEN. Typically searches by address or permit #.",
  },
  {
    key: "click2gov",
    name: "Click2Gov",
    urlPatterns: [/click2gov/i, /aspgov\.com/i],
    htmlMarkers: ["Click2Gov"],
    searchByParcel: false,
    notes: "Central Square Click2Gov (often hosted on *.aspgov.com). Address / application # search.",
  },
  {
    key: "opengov",
    name: "OpenGov",
    urlPatterns: [/\.opengov\.com/i, /viewpoint\.opengov/i],
    htmlMarkers: ["OpenGov"],
    searchByParcel: null,
    notes: "OpenGov permitting & licensing (formerly ViewPoint Cloud).",
  },
  {
    key: "centralsquare",
    name: "CentralSquare / eHub",
    urlPatterns: [/centralsquare/i, /\beHub\b/i, /esuite/i, /eSuite\.Permits/i],
    htmlMarkers: ["CentralSquare", "eHub"],
    searchByParcel: null,
    notes: "CentralSquare community development suite (eHub / eSuite portals). CentralSquare owns Click2Gov.",
  },
  {
    key: "epzb",
    name: "ePZB (county custom)",
    urlPatterns: [/epzb/i, /ipzb/i],
    htmlMarkers: ["ePZB"],
    searchByParcel: true,
    notes: "Palm Beach County custom ePZB/iPZB system. Searches by address or PCN.",
  },
  {
    key: "govaccess",
    name: "GovAccess",
    urlPatterns: [/govaccess\.org/i],
    htmlMarkers: ["GovAccess"],
    searchByParcel: null,
    notes: "GovAccess municipal portal hosting.",
  },
  {
    key: "mygov",
    name: "MyGovernmentOnline (MGO)",
    urlPatterns: [/mgoconnect\.org/i, /mygovernmentonline/i],
    htmlMarkers: ["My Government Online", "MGO Connect"],
    searchByParcel: null,
    notes: "MyGovernmentOnline (MGO Connect) permitting platform.",
  },
  {
    key: "bsa",
    name: "BS&A Online",
    urlPatterns: [/bsaonline\.com/i],
    htmlMarkers: ["BS&A", "bsaonline"],
    searchByParcel: null,
    notes: "BS&A Software online community development / permitting.",
  },
  {
    key: "sagesgov",
    name: "SagesGov",
    urlPatterns: [/sagesgov\.com/i],
    htmlMarkers: ["SagesGov"],
    searchByParcel: null,
    notes: "SagesGov permitting platform.",
  },
  {
    key: "municipalonline",
    name: "Municipal Online",
    urlPatterns: [/municipalonlinepayments\.com/i],
    htmlMarkers: ["Municipal Online"],
    searchByParcel: null,
    notes: "Municipal Online Payments / Services portal.",
  },
  {
    key: "maintstar",
    name: "MaintStar",
    urlPatterns: [/maintstar\.co/i],
    htmlMarkers: ["MaintStar", "Maintstar Permit"],
    searchByParcel: null,
    notes: "MaintStar permitting / asset management.",
  },
  {
    key: "geocivix",
    name: "GeoCivix",
    urlPatterns: [/geocivix\.com/i],
    htmlMarkers: ["GeoCivix"],
    searchByParcel: null,
    notes: "GeoCivix electronic plan review / permitting.",
  },
  {
    key: "cityview",
    name: "CityView (Harris)",
    urlPatterns: [/cityviewportal/i, /\/cityview/i],
    htmlMarkers: ["CityView"],
    searchByParcel: null,
    notes: "Harris CityView community development / permitting portal.",
  },
  {
    key: "iworq",
    name: "iWorq",
    urlPatterns: [/iworq\.net/i],
    htmlMarkers: ["iWorq", "iWorQ"],
    searchByParcel: null,
    notes: "iWorQ Systems cloud permitting.",
  },
];

/**
 * Permit-domain evidence markers. A vendor-branded host is not enough — a page must
 * also look like a *permitting* page, not (for example) an OpenGov financial
 * transparency portal that happens to live on a vendor domain. The bare word
 * "building" is intentionally excluded as too weak; the markers here are specific to
 * permitting/inspections or are vendor permit-product names.
 */
const PERMIT_EVIDENCE_PATTERN =
  /\b(permit|permitting|inspection|certificate of occupancy|building division|plan review|click2gov|civic access|energov|citizen ?access|ehub)\b/i;

/**
 * Test whether a page body looks like a building-permit portal.
 *
 * @param {string | null | undefined} html - Fetched page HTML/body.
 * @returns {boolean} True when permit-domain markers are present.
 */
export function hasPermitEvidence(html) {
  return typeof html === "string" && PERMIT_EVIDENCE_PATTERN.test(html);
}

/** Classification returned when no vendor signature matches. */
const UNKNOWN_CLASSIFICATION = Object.freeze({
  key: "unknown",
  name: "unknown",
  confidence: /** @type {const} */ ("none"),
  searchByParcel: null,
});

/**
 * Classify a permit portal by vendor signature.
 *
 * URL patterns are checked first (highest confidence) because a vendor host is a
 * far stronger signal than a marker string that could appear incidentally in page
 * text. HTML markers are the fallback for vendors hosted on a city's own domain.
 *
 * @param {{ url?: string | null, html?: string | null }} input - Portal URL and/or fetched HTML.
 * @returns {VendorClassification} Detected vendor, or the unknown classification.
 */
export function classifyVendor({ url, html }) {
  const safeUrl = typeof url === "string" ? url : "";
  const safeHtml = typeof html === "string" ? html : "";

  for (const vendor of VENDORS) {
    if (vendor.urlPatterns.some((pattern) => pattern.test(safeUrl))) {
      return toClassification(vendor, "url");
    }
  }

  if (safeHtml.length > 0) {
    const lowerHtml = safeHtml.toLowerCase();
    for (const vendor of VENDORS) {
      if (
        vendor.htmlMarkers.some((marker) =>
          lowerHtml.includes(marker.toLowerCase()),
        )
      ) {
        return toClassification(vendor, "html");
      }
    }
  }

  return { ...UNKNOWN_CLASSIFICATION };
}

/**
 * Vendor-family relationships: a matched key also implies these related keys when
 * mapping a catalog string, because vendors with a corporate/product relationship
 * can serve a portal that classifies as either. CentralSquare owns Click2Gov, so a
 * portal catalogued as "CentralSquare / eHub" that actually classifies as Click2Gov
 * is still the same family and should certify as a match.
 *
 * @type {Readonly<Record<string, readonly string[]>>}
 */
const VENDOR_FAMILY_KEYS = {
  centralsquare: ["click2gov"],
};

/**
 * Map a catalog's stated vendor string (free text like "Tyler EPL / Civic Access"
 * or "CentralSquare / eHub") to the set of vendor keys it names. A single catalog
 * row may name more than one vendor, and family relationships expand the set (see
 * VENDOR_FAMILY_KEYS); `certify.mjs` treats a detected key as a match when it appears
 * in this set.
 *
 * @param {string | null | undefined} catalogVendor - Free-text vendor from a catalog row.
 * @returns {readonly string[]} Matched vendor keys (possibly empty).
 */
export function catalogVendorToKeys(catalogVendor) {
  if (typeof catalogVendor !== "string" || catalogVendor.trim().length === 0) {
    return [];
  }
  const haystack = catalogVendor.toLowerCase();
  /** @type {string[]} */
  const keys = [];
  /**
   * Add a vendor key plus any family-related keys, avoiding duplicates.
   *
   * @param {string} key - Vendor key to add.
   * @returns {void}
   */
  const addKey = (key) => {
    if (!keys.includes(key)) {
      keys.push(key);
    }
    for (const relatedKey of VENDOR_FAMILY_KEYS[key] ?? []) {
      if (!keys.includes(relatedKey)) {
        keys.push(relatedKey);
      }
    }
  };
  for (const vendor of VENDORS) {
    const tokens = [vendor.key, vendor.name, ...vendor.htmlMarkers];
    const matches = tokens.some((token) =>
      haystack.includes(token.toLowerCase()),
    );
    if (matches) {
      addKey(vendor.key);
    }
  }
  return keys;
}

/**
 * Build a classification result from a matched vendor signature.
 *
 * @param {VendorSignature} vendor - Matched vendor signature.
 * @param {"url" | "html"} confidence - How the match was made.
 * @returns {VendorClassification} Classification result.
 */
function toClassification(vendor, confidence) {
  return {
    key: vendor.key,
    name: vendor.name,
    confidence,
    searchByParcel: vendor.searchByParcel,
  };
}
