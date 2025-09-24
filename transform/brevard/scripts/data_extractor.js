const fs = require("fs");
const path = require("path");
const cheerio = require("cheerio");

function readJSON(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

function emptyDir(dir) {
  if (!fs.existsSync(dir)) return;
  fs.readdirSync(dir).forEach((f) => {
    const fp = path.join(dir, f);
    try {
      const stat = fs.statSync(fp);
      if (stat.isDirectory()) {
        emptyDir(fp);
        fs.rmdirSync(fp);
      } else {
        fs.unlinkSync(fp);
      }
    } catch (e) {
      /* ignore */
    }
  });
}

function parseMoney(str) {
  if (!str) return null;
  const n = Number(String(str).replace(/[^0-9.\-]/g, ""));
  return isNaN(n) ? null : n;
}

function parseIntOrNull(str) {
  if (str == null) return null;
  const n = parseInt(String(str).replace(/[^0-9\-]/g, ""), 10);
  return isNaN(n) ? null : n;
}

function toISODate(mmddyyyy) {
  if (!mmddyyyy) return null;
  const m = mmddyyyy.match(/(\d{2})\/(\d{2})\/(\d{4})/);
  if (!m) return null;
  return `${m[3]}-${m[1]}-${m[2]}`;
}

function mapUnitsType(n) {
  if (n == null) return null;
  if (n === 1) return "One";
  if (n === 2) return "Two";
  if (n === 3) return "Three";
  if (n === 4) return "Four";
  if (n >= 2 && n <= 4) return "TwoToFour";
  return null;
}

function mapStreetSuffix(usps) {
  if (!usps) return null;
  const suffixMap = {
    STREET: "St",
    ST: "St",
    AVENUE: "Ave",
    AVE: "Ave",
    BOULEVARD: "Blvd",
    BLVD: "Blvd",
    ROAD: "Rd",
    RD: "Rd",
    LANE: "Ln",
    LN: "Ln",
    DRIVE: "Dr",
    DR: "Dr",
    COURT: "Ct",
    CT: "Ct",
    PLACE: "Pl",
    PL: "Pl",
    TERRACE: "Ter",
    TER: "Ter",
    CIRCLE: "Cir",
    CIR: "Cir",
    WAY: "Way",
    LOOP: "Loop",
    PARKWAY: "Pkwy",
    PKWY: "Pkwy",
    PLAZA: "Plz",
    PLZ: "Plz",
    TRAIL: "Trl",
    TRL: "Trl",
    BEND: "Bnd",
    BND: "Bnd",
    CRESCENT: "Cres",
    CRES: "Cres",
    MANOR: "Mnr",
    MNR: "Mnr",
    SQUARE: "Sq",
    SQ: "Sq",
    CROSSING: "Xing",
    XING: "Xing",
    PATH: "Path",
    RUN: "Run",
    WALK: "Walk",
    ROW: "Row",
    ALLEY: "Aly",
    ALY: "Aly",
    BEACH: "Bch",
    BCH: "Bch",
    BRIDGE: "Br",
    BRG: "Br",
    BROOK: "Brk",
    BRK: "Brk",
    BROOKS: "Brks",
    BRKS: "Brks",
    BUG: "Bg",
    BG: "Bg",
    BUGS: "Bgs",
    BGS: "Bgs",
    CLUB: "Clb",
    CLB: "Clb",
    CLIFF: "Clf",
    CLF: "Clf",
    CLIFFS: "Clfs",
    CLFS: "Clfs",
    COMMON: "Cmn",
    CMN: "Cmn",
    COMMONS: "Cmns",
    CMNS: "Cmns",
    CORNER: "Cor",
    COR: "Cor",
    CORNERS: "Cors",
    CORS: "Cors",
    CREEK: "Crk",
    CRK: "Crk",
    COURSE: "Crse",
    CRSE: "Crse",
    CREST: "Crst",
    CRST: "Crst",
    CAUSEWAY: "Cswy",
    CSWY: "Cswy",
    COVE: "Cv",
    CV: "Cv",
    CANYON: "Cyn",
    CYN: "Cyn",
    DALE: "Dl",
    DL: "Dl",
    DAM: "Dm",
    DM: "Dm",
    DRIVES: "Drs",
    DRS: "Drs",
    DIVIDE: "Dv",
    DV: "Dv",
    ESTATE: "Est",
    EST: "Est",
    ESTATES: "Ests",
    ESTS: "Ests",
    EXPRESSWAY: "Expy",
    EXPY: "Expy",
    EXTENSION: "Ext",
    EXT: "Ext",
    EXTENSIONS: "Exts",
    EXTS: "Exts",
    FALL: "Fall",
    FALL: "Fall",
    FALLS: "Fls",
    FLS: "Fls",
    FLAT: "Flt",
    FLT: "Flt",
    FLATS: "Flts",
    FLTS: "Flts",
    FORD: "Frd",
    FRD: "Frd",
    FORDS: "Frds",
    FRDS: "Frds",
    FORGE: "Frg",
    FRG: "Frg",
    FORGES: "Frgs",
    FRGS: "Frgs",
    FORK: "Frk",
    FRK: "Frk",
    FORKS: "Frks",
    FRKS: "Frks",
    FOREST: "Frst",
    FRST: "Frst",
    FREEWAY: "Fwy",
    FWY: "Fwy",
    FIELD: "Fld",
    FLD: "Fld",
    FIELDS: "Flds",
    FLDS: "Flds",
    GARDEN: "Gdn",
    GDN: "Gdn",
    GARDENS: "Gdns",
    GDNS: "Gdns",
    GLEN: "Gln",
    GLN: "Gln",
    GLENS: "Glns",
    GLNS: "Glns",
    GREEN: "Grn",
    GRN: "Grn",
    GREENS: "Grns",
    GRNS: "Grns",
    GROVE: "Grv",
    GRV: "Grv",
    GROVES: "Grvs",
    GRVS: "Grvs",
    GATEWAY: "Gtwy",
    GTWY: "Gtwy",
    HARBOR: "Hbr",
    HBR: "Hbr",
    HARBORS: "Hbrs",
    HBRS: "Hbrs",
    HILL: "Hl",
    HL: "Hl",
    HILLS: "Hls",
    HLS: "Hls",
    HOLLOW: "Holw",
    HOLW: "Holw",
    HEIGHTS: "Hts",
    HTS: "Hts",
    HAVEN: "Hvn",
    HVN: "Hvn",
    HIGHWAY: "Hwy",
    HWY: "Hwy",
    INLET: "Inlt",
    INLT: "Inlt",
    ISLAND: "Is",
    IS: "Is",
    ISLANDS: "Iss",
    ISS: "Iss",
    ISLE: "Isle",
    SPUR: "Spur",
    JUNCTION: "Jct",
    JCT: "Jct",
    JUNCTIONS: "Jcts",
    JCTS: "Jcts",
    KNOLL: "Knl",
    KNL: "Knl",
    KNOLLS: "Knls",
    KNLS: "Knls",
    LOCK: "Lck",
    LCK: "Lck",
    LOCKS: "Lcks",
    LCKS: "Lcks",
    LODGE: "Ldg",
    LDG: "Ldg",
    LIGHT: "Lgt",
    LGT: "Lgt",
    LIGHTS: "Lgts",
    LGTS: "Lgts",
    LAKE: "Lk",
    LK: "Lk",
    LAKES: "Lks",
    LKS: "Lks",
    LANDING: "Lndg",
    LNDG: "Lndg",
    MALL: "Mall",
    MEWS: "Mews",
    MEADOW: "Mdw",
    MDW: "Mdw",
    MEADOWS: "Mdws",
    MDWS: "Mdws",
    MILL: "Ml",
    ML: "Ml",
    MILLS: "Mls",
    MLS: "Mls",
    MANORS: "Mnrs",
    MNRS: "Mnrs",
    MOUNT: "Mt",
    MT: "Mt",
    MOUNTAIN: "Mtn",
    MTN: "Mtn",
    MOUNTAINS: "Mtns",
    MTNS: "Mtns",
    OVERPASS: "Opas",
    OPAS: "Opas",
    ORCHARD: "Orch",
    ORCH: "Orch",
    OVAL: "Oval",
    PARK: "Park",
    PASS: "Pass",
    PIKE: "Pike",
    PLAIN: "Pln",
    PLN: "Pln",
    PLAINS: "Plns",
    PLNS: "Plns",
    PINE: "Pne",
    PNE: "Pne",
    PINES: "Pnes",
    PNES: "Pnes",
    PRAIRIE: "Pr",
    PR: "Pr",
    PORT: "Prt",
    PRT: "Prt",
    PORTS: "Prts",
    PRTS: "Prts",
    PASSAGE: "Psge",
    PSGE: "Psge",
    POINT: "Pt",
    PT: "Pt",
    POINTS: "Pts",
    PTS: "Pts",
    RADIAL: "Radl",
    RADL: "Radl",
    RAMP: "Ramp",
    REST: "Rst",
    RIDGE: "Rdg",
    RDG: "Rdg",
    RIDGES: "Rdgs",
    RDGS: "Rdgs",
    ROADS: "Rds",
    RDS: "Rds",
    RANCH: "Rnch",
    RNCH: "Rnch",
    RAPID: "Rpd",
    RPD: "Rpd",
    RAPIDS: "Rpds",
    RPDS: "Rpds",
    ROUTE: "Rte",
    RTE: "Rte",
    SHOAL: "Shl",
    SHL: "Shl",
    SHOALS: "Shls",
    SHLS: "Shls",
    SHORE: "Shr",
    SHR: "Shr",
    SHORES: "Shrs",
    SHRS: "Shrs",
    SKYWAY: "Skwy",
    SKWY: "Skwy",
    SUMMIT: "Smt",
    SMT: "Smt",
    SPRING: "Spg",
    SPG: "Spg",
    SPRINGS: "Spgs",
    SPGS: "Spgs",
    SQUARES: "Sqs",
    SQS: "Sqs",
    STATION: "Sta",
    STA: "Sta",
    STRAVENUE: "Stra",
    STRA: "Stra",
    STREAM: "Strm",
    STRM: "Strm",
    STREETS: "Sts",
    STS: "Sts",
    THROUGHWAY: "Trwy",
    TRWY: "Trwy",
    TRACE: "Trce",
    TRCE: "Trce",
    TRAFFICWAY: "Trfy",
    TRFY: "Trfy",
    TRAILER: "Trlr",
    TRLR: "Trlr",
    TUNNEL: "Tunl",
    TUNL: "Tunl",
    UNION: "Un",
    UN: "Un",
    UNIONS: "Uns",
    UNS: "Uns",
    UNDERPASS: "Upas",
    UPAS: "Upas",
    VIEW: "Vw",
    VIEWS: "Vws",
    VILLAGE: "Vlg",
    VLG: "Vlg",
    VILLAGES: "Vlgs",
    VLGS: "Vlgs",
    VALLEY: "Vl",
    VLY: "Vl",
    VALLEYS: "Vlys",
    VLYS: "Vlys",
    WAYS: "Ways",
    VIA: "Via",
    WELL: "Wl",
    WL: "Wl",
    WELLS: "Wls",
    WLS: "Wls",
    CROSSROAD: "Xrd",
    XRD: "Xrd",
    CROSSROADS: "Xrds",
    XRDS: "Xrds",
  };
  return suffixMap[usps.toUpperCase()] || null;
}

function writeJSON(filePath, obj) {
  fs.writeFileSync(filePath, JSON.stringify(obj, null, 2));
}

function normalizeOwnerKey(o) {
  if (!o) return null;
  const fn = (o.first_name || "").trim().toLowerCase();
  const mn = (o.middle_name || "").trim().toLowerCase();
  const ln = (o.last_name || "").trim().toLowerCase();
  return [fn, mn, ln].filter(Boolean).join("|");
}

// Map Brevard County use codes to property types
function mapBrevardUseCodeToPropertyType(useCodeDesc) {
  if (!useCodeDesc) return null;

  const desc = useCodeDesc.toUpperCase();

  // Map Brevard County codes to property types
  if (desc.includes("VACANT RESIDENTIAL") || desc.includes("VACANT LAND"))
    return "VacantLand";
  if (
    desc.includes("SINGLE FAMILY RESIDENCE") ||
    desc.includes("SINGLE FAMILY")
  )
    return "SingleFamily";
  if (desc.includes("MANUFACTURED HOUSING")) return "ManufacturedHousing";
  if (desc.includes("MOBILE HOME")) return "MobileHome";
  if (desc.includes("MODULAR")) return "Modular";
  if (desc.includes("TOWNHOUSE")) return "Townhouse";
  if (desc.includes("DUPLEX") || desc.includes("HALF-DUPLEX")) return "Duplex";
  if (desc.includes("TRIPLEX")) return "TwoToFourFamily";
  if (desc.includes("QUADRUPLEX")) return "TwoToFourFamily";
  if (
    desc.includes("MULTIPLE LIVING UNITS - 5 TO 9") ||
    desc.includes("GARDEN APARTMENTS") ||
    desc.includes("LOW RISE APARTMENTS") ||
    desc.includes("HIGH RISE APARTMENTS")
  )
    return "MultipleFamily";
  if (desc.includes("CONDOMINIUM")) return "Condominium";
  if (desc.includes("COOPERATIVE") || desc.includes("CO-OP"))
    return "Cooperative";
  if (desc.includes("RETIREMENT HOME")) return "Retirement";
  if (desc.includes("TIME SHARE")) return "Timeshare";
  if (desc.includes("MISC RESIDENTIAL") || desc.includes("MIGRANT"))
    return "MiscellaneousResidential";
  if (desc.includes("RESIDENTIAL COMMON AREA") || desc.includes("COMMON AREA"))
    return "ResidentialCommonElementsAreas";

  // Default to null for non-residential or unrecognized codes
  return null;
}

function main() {
  const dataDir = path.join("data");
  ensureDir(dataDir);
  // Clean output dir to avoid duplicate stale files
  emptyDir(dataDir);

  const html = fs.readFileSync("input.html", "utf8");
  const $ = cheerio.load(html);
  const unAddr = readJSON("unnormalized_address.json");
  const seed = readJSON("property_seed.json");

  const propertyId =
    seed.request_identifier || (seed.parcel_id || "").replace(/\D/g, "");

  // Owners, Utilities, Layout
  const ownersJsonPath = path.join("owners", "owner_data.json");
  const utilitiesJsonPath = path.join("owners", "utilities_data.json");
  const layoutsJsonPath = path.join("owners", "layout_data.json");
  const ownersData = fs.existsSync(ownersJsonPath)
    ? readJSON(ownersJsonPath)
    : {};
  const utilsData = fs.existsSync(utilitiesJsonPath)
    ? readJSON(utilitiesJsonPath)
    : {};
  const layoutsData = fs.existsSync(layoutsJsonPath)
    ? readJSON(layoutsJsonPath)
    : {};

  // ---------- Parse Property ----------
  const parcelId = $("#divDetails_Pid").text().trim() || null;

  // Legal description
  let legalDesc = null;
  $("#divInfo_Description .cssDetails_Top_Row").each((i, el) => {
    const label = $(el).find(".cssDetails_Top_Cell_Label").text().trim();
    if (label && label.match(/Land Description:/i)) {
      legalDesc = $(el).find(".cssDetails_Top_Cell_Data").text().trim() || null;
    }
  });

  // Subdivision name
  let subdivision = null;
  $("#divInfo_Description .cssDetails_Top_Row").each((i, el) => {
    const label = $(el).find(".cssDetails_Top_Cell_Label").text().trim();
    if (label && label.match(/Subdivision Name:/i)) {
      const t = $(el).find(".cssDetails_Top_Cell_Data").text().trim();
      subdivision = t && t !== "--" ? t : null;
    }
  });

  // Extract Property Use and map to property_type
  let propertyType = null;
  let units = null;
  const useText = $("#divInfo_Description .cssDetails_Top_Row")
    .filter((i, el) => {
      return /Property Use:/i.test(
        $(el).find(".cssDetails_Top_Cell_Label").text(),
      );
    })
    .first()
    .find(".cssDetails_Top_Cell_Data")
    .text()
    .trim();

  // Map use code to property type
  propertyType = mapBrevardUseCodeToPropertyType(useText);

  // Acres
  const acresText = $("#divInfo_Description .cssDetails_Top_Row")
    .filter((i, el) => {
      return /Total Acres:/i.test(
        $(el).find(".cssDetails_Top_Cell_Label").text(),
      );
    })
    .first()
    .find(".cssDetails_Top_Cell_Data")
    .text()
    .trim();
  const acres = acresText ? Number(acresText.replace(/[^0-9.]/g, "")) : null;

  // Building details: year built, units, floors, story height
  const bldgDetails = {};
  $("#divBldg_Details table tbody tr").each((i, el) => {
    const label = $(el).find("td").first().text().trim();
    const val = $(el).find("td").last().text().trim();
    if (/Year Built:/i.test(label)) bldgDetails.yearBuilt = parseIntOrNull(val);
    if (/Residential Units:/i.test(label))
      bldgDetails.resUnits = parseIntOrNull(val);
    if (/Floors:/i.test(label)) bldgDetails.floors = parseIntOrNull(val);
    if (/Story Height:/i.test(label))
      bldgDetails.storyHeight = parseIntOrNull(val);
  });

  // If no units found in building details, default to 1 for residential properties
  if (!bldgDetails.resUnits && propertyType && propertyType !== "VacantLand") {
    bldgDetails.resUnits = 1;
  }

  // Sub-areas: Total Base Area, Total Sub Area
  let totalBaseArea = null;
  let totalSubArea = null;
  $("#divBldg_SubAreas table").each((i, table) => {
    $(table)
      .find("tbody tr")
      .each((j, row) => {
        const tds = $(row).find("td");
        if (tds.length >= 2) {
          const key = $(tds[0]).text().trim();
          const val = $(tds[1]).text().trim();
          if (/Total Base Area/i.test(key))
            totalBaseArea = val.replace(/[^0-9]/g, "");
          if (/Total Sub Area/i.test(key))
            totalSubArea = val.replace(/[^0-9]/g, "");
        }
      });
  });
  if (!totalBaseArea) {
    const base1 = $("#divBldg_SubAreas tbody tr")
      .filter((i, el) => /Base Area/i.test($(el).find("td").first().text()))
      .first()
      .find("td")
      .last()
      .text()
      .trim();
    if (base1) totalBaseArea = base1.replace(/[^0-9]/g, "");
  }

  const property = {
    area_under_air: null,
    historic_designation: false,
    livable_floor_area: totalBaseArea ? String(totalBaseArea) : null,
    number_of_units: bldgDetails.resUnits ?? null,
    number_of_units_type: mapUnitsType(bldgDetails.resUnits) ?? null,
    parcel_identifier: parcelId || "",
    property_effective_built_year: null,
    property_legal_description_text: legalDesc || null,
    property_structure_built_year: bldgDetails.yearBuilt ?? null,
    property_type: propertyType || null, // Now extracted from Property Use
    subdivision: subdivision,
    total_area: totalSubArea ? String(totalSubArea) : null,
    zoning: null,
  };
  writeJSON(path.join(dataDir, "property.json"), property);

  // ---------- Parse Address ----------
  const siteAddr = $(
    "#divDetails_Top_SiteAddressContainer .cssDetails_Top_SiteAddress",
  )
    .first()
    .text()
    .trim();

  const validDirectionals = ["N", "S", "E", "W", "NE", "NW", "SE", "SW"];

  let street_number = null,
    street_name = null,
    street_suffix_type = null,
    street_pre_directional_text = null,
    street_post_directional_text = null,
    city_name = null,
    state_code = null,
    postal_code = null;

  if (siteAddr) {
    // Match pattern: "1910 N COCOA BLVD COCOA FL 32922"
    const addrMatch = siteAddr.match(
      /^(\d+)\s+(?:(N|S|E|W|NE|NW|SE|SW)\s+)?(.+?)\s+(?:(N|S|E|W|NE|NW|SE|SW)\s+)?([A-Z]+)\s+([A-Z\s]+)\s+([A-Z]{2})\s+(\d{5})$/i,
    );

    if (addrMatch) {
      street_number = addrMatch[1];
      const preDir = addrMatch[2];
      let streetPart = addrMatch[3].trim();
      const postDir = addrMatch[4];
      const suffix = addrMatch[5];
      city_name = addrMatch[6].trim();
      state_code = addrMatch[7];
      postal_code = addrMatch[8];

      // Determine if we have pre or post directional
      if (preDir && validDirectionals.includes(preDir.toUpperCase())) {
        street_pre_directional_text = preDir.toUpperCase();
        street_name = streetPart;
      } else if (postDir && validDirectionals.includes(postDir.toUpperCase())) {
        street_post_directional_text = postDir.toUpperCase();
        street_name = streetPart;
      } else {
        street_name = streetPart;
      }

      // Map suffix
      street_suffix_type = mapStreetSuffix(suffix);
    } else {
      // Fallback to simple parsing
      const parts = siteAddr.split(/\s+/);
      street_number = parts.shift() || null;
      postal_code = parts.pop() || null;
      state_code = parts.pop() || null;
      city_name = parts.pop() || null;
      const maybeSuffix = parts.length > 0 ? parts[parts.length - 1] : null;
      if (maybeSuffix && /^[A-Z]{2,}$/.test(maybeSuffix)) {
        street_suffix_type = mapStreetSuffix(maybeSuffix);
        if (street_suffix_type) parts.pop();
      }
      street_name = parts.join(" ") || null;
    }
  }

  // township-range-section-block from parcel id
  let township = null,
    range = null,
    section = null,
    blockVal = null,
    lotVal = null;
  if (parcelId) {
    const toks = parcelId.split("-");
    if (toks.length >= 5) {
      township = toks[0] || null;
      range = toks[1] || null;
      section = toks[2] || null;
      blockVal = toks[4] || null;
    }
  }

  const address = {
    block: blockVal || null,
    city_name: city_name ? city_name.toUpperCase() : null,
    country_code: "US",
    county_name: unAddr.county_jurisdiction || null,
    latitude: null,
    longitude: null,
    lot: lotVal || null,
    municipality_name: null,
    plus_four_postal_code: null,
    postal_code: postal_code || null,
    range: range || null,
    route_number: null,
    section: section || null,
    state_code: state_code || null,
    street_name: street_name || null,
    street_post_directional_text: street_post_directional_text || null,
    street_pre_directional_text: street_pre_directional_text || null,
    street_number: street_number || null,
    street_suffix_type: street_suffix_type || null,
    unit_identifier: null,
    township: township || null,
  };
  writeJSON(path.join(dataDir, "address.json"), address);

  // ---------- Sales, Deed, File ----------
  const salesRows = $("#tSalesTransfers tbody tr");
  let personFilesByKey = {};
  let saleOwnersNormKeys = [];
  let salesFileIndex = 1;

  salesRows.each((index, row) => {
    const $row = $(row);
    const dateText = $row.find("td").eq(0).text().trim();
    const priceText = $row.find("td").eq(1).text().trim();
    const typeCell = $row.find("td").eq(2);
    const deedCode = typeCell.text().trim();
    const deedTitle = typeCell.find("a").attr("title") || "";
    const deedLink = $row.find("td").eq(3).find("a").attr("href") || null;

    const purchasePrice = parseMoney(priceText);

    // Only create sales file if there's an actual price (not null)
    if (purchasePrice !== null) {
      const sale = {
        ownership_transfer_date: toISODate(dateText),
        purchase_price_amount: purchasePrice,
      };
      writeJSON(path.join(dataDir, `sales_${salesFileIndex}.json`), sale);

      // Map deed code/title to deed_type
      let deedType = null;
      if (/\bWD\b/i.test(deedCode) || /WARRANTY DEED/i.test(deedTitle)) {
        deedType = "Warranty Deed";
      } else if (/SPECIAL WARRANTY DEED/i.test(deedTitle)) {
        deedType = "Special Warranty Deed";
      } else if (/QUIT ?CLAIM/i.test(deedTitle) || /\bQCD\b/i.test(deedCode)) {
        deedType = "Quitclaim Deed";
      }

      const deed = {};
      if (deedType) {
        deed.deed_type = deedType;
      }
      writeJSON(path.join(dataDir, `deed_${salesFileIndex}.json`), deed);

      const fileObj = {
        document_type: null,
        file_format: null,
        ipfs_url: null,
        name: null,
        original_url: deedLink || null,
      };
      writeJSON(path.join(dataDir, `file_${salesFileIndex}.json`), fileObj);

      const relSalesDeed = {
        to: { "/": `./sales_${salesFileIndex}.json` },
        from: { "/": `./deed_${salesFileIndex}.json` },
      };
      writeJSON(
        path.join(dataDir, `relationship_sales_deed_${salesFileIndex}.json`),
        relSalesDeed,
      );

      const relDeedFile = {
        to: { "/": `./deed_${salesFileIndex}.json` },
        from: { "/": `./file_${salesFileIndex}.json` },
      };
      writeJSON(
        path.join(dataDir, `relationship_deed_file_${salesFileIndex}.json`),
        relDeedFile,
      );

      salesFileIndex++; // Only increment when we actually create files
    }
  });

  // Process owners only if we created at least one sales file
  if (salesFileIndex > 1) {
    // Find the first row that actually has a price
    let firstSaleRow = null;
    salesRows.each((index, row) => {
      const $row = $(row);
      const priceText = $row.find("td").eq(1).text().trim();
      if (parseMoney(priceText) !== null && !firstSaleRow) {
        firstSaleRow = $row;
        return false; // break the loop
      }
    });

    if (firstSaleRow) {
      const dateText = firstSaleRow.find("td").eq(0).text().trim();

      // Owners at sale date and current -> deduplicate to single person files
      const ownersKey = `property_${propertyId}`;
      const ownersByDate = ownersData[ownersKey]?.owners_by_date || {};
      const saleDateIso = toISODate(dateText);
      const saleOwners =
        (ownersByDate &&
          (ownersByDate[saleDateIso] || ownersByDate[dateText])) ||
        [];
      const currentOwners = ownersByDate["current"] || [];

      const combined = [...saleOwners, ...currentOwners];
      const uniqueOwners = [];
      const seen = new Set();
      combined.forEach((o) => {
        if (o && o.type === "person") {
          const key = normalizeOwnerKey(o);
          if (key && !seen.has(key)) {
            seen.add(key);
            uniqueOwners.push({ o, key });
          }
        }
      });

      // Create a person file for each unique owner
      uniqueOwners.forEach((entry, idx) => {
        const o = entry.o;
        const person = {
          birth_date: null,
          first_name: o.first_name || "",
          last_name: o.last_name || "",
          middle_name: o.middle_name || null,
          prefix_name: null,
          suffix_name: null,
          us_citizenship_status: null,
          veteran_status: null,
        };
        const pFile = `person_${idx + 1}.json`;
        writeJSON(path.join(dataDir, pFile), person);
        personFilesByKey[entry.key] = pFile;
      });

      // Relationships from sales to persons who owned on sale date
      saleOwnersNormKeys = saleOwners
        .map((o) => normalizeOwnerKey(o))
        .filter(Boolean);
      let relIdx = 0;
      saleOwnersNormKeys.forEach((k) => {
        const pf = personFilesByKey[k];
        if (pf) {
          relIdx += 1;
          const rel = {
            to: { "/": `./${pf}` },
            from: { "/": "./sales_1.json" },
          };
          writeJSON(
            path.join(dataDir, `relationship_sales_person_${relIdx}.json`),
            rel,
          );
        }
      });
    }
  }

  // ---------- Taxes (Value table) ----------
  const valuesTable = $("#tValues");
  if (valuesTable.length) {
    const headYears = [];
    valuesTable.find("thead th").each((i, th) => {
      if (i === 0) return; // skip Category
      headYears.push(parseIntOrNull($(th).text().trim()));
    });
    const rows = {};
    valuesTable.find("tbody tr").each((i, tr) => {
      const tds = $(tr).find("td");
      const label = $(tds[0]).text().trim();
      const vals = [];
      for (let c = 1; c < tds.length; c++) vals.push($(tds[c]).text().trim());
      rows[label] = vals;
    });

    headYears.forEach((yr, idx) => {
      if (!yr) return;
      const market = parseMoney(rows["Market Value:"]?.[idx]);
      const assessed = parseMoney(rows["Assessed Value Non-School:"]?.[idx]);
      const taxable = parseMoney(rows["Taxable Value Non-School:"]?.[idx]);
      if (market != null && assessed != null && taxable != null) {
        const tax = {
          first_year_building_on_tax_roll: null,
          first_year_on_tax_roll: null,
          monthly_tax_amount: null,
          period_end_date: null,
          period_start_date: null,
          property_assessed_value_amount: assessed,
          property_building_amount: null,
          property_land_amount: null,
          property_market_value_amount: market,
          property_taxable_value_amount: taxable,
          tax_year: yr,
          yearly_tax_amount: null,
        };
        writeJSON(path.join(dataDir, `tax_${yr}.json`), tax);
      }
    });
  }

  // ---------- Structure ----------
  let exteriorWall = null,
    frameMat = null,
    roofCover = null,
    roofStruct = null;
  $("#divBldg_Materials tbody tr").each((i, tr) => {
    const tds = $(tr).find("td");
    const el = $(tds[0]).text().trim();
    const desc = $(tds[1]).text().trim();
    if (/Exterior Wall:/i.test(el)) exteriorWall = desc;
    if (/Frame:/i.test(el)) frameMat = desc;
    if (/^Roof:$/i.test(el) || /Roof:/i.test(el)) roofCover = desc;
    if (/Roof Structure:/i.test(el)) roofStruct = desc;
  });

  function mapExteriorWall(desc) {
    if (!desc) return null;
    if (/STUCCO/i.test(desc)) return "Stucco";
    if (/VINYL/i.test(desc)) return "Vinyl Siding";
    if (/BRICK/i.test(desc)) return "Brick";
    if (/BLOCK|CMU|CBS/i.test(desc)) return "Concrete Block";
    return null;
  }
  function mapFrame(desc) {
    if (!desc) return null;
    if (/WOOD/i.test(desc)) return "Wood Frame";
    if (/STEEL/i.test(desc)) return "Steel Frame";
    if (/BLOCK|CMU|CBS|MASN|MASON/i.test(desc)) return "Masonry";
    return null;
  }
  function mapRoofDesign(desc) {
    if (!desc) return null;
    if (/GABLE/i.test(desc) && /HIP/i.test(desc)) return "Combination";
    if (/GABLE/i.test(desc)) return "Gable";
    if (/HIP/i.test(desc)) return "Hip";
    if (/FLAT/i.test(desc)) return "Flat";
    return null;
  }
  function mapRoofCovering(desc) {
    if (!desc) return null;
    if (/SHINGL|SHNGL/i.test(desc) && /ASPH|ASPHALT|ASB/i.test(desc))
      return "3-Tab Asphalt Shingle";
    if (/METAL/i.test(desc) && /STANDING/i.test(desc))
      return "Metal Standing Seam";
    return null;
  }

  const structure = {
    architectural_style_type: null,
    attachment_type: null,
    ceiling_condition: null,
    ceiling_height_average: bldgDetails.storyHeight || null,
    ceiling_insulation_type: null,
    ceiling_structure_material: null,
    ceiling_surface_material: null,
    exterior_door_material: null,
    exterior_wall_condition: null,
    exterior_wall_condition_primary: null,
    exterior_wall_condition_secondary: null,
    exterior_wall_insulation_type: null,
    exterior_wall_insulation_type_primary: null,
    exterior_wall_insulation_type_secondary: null,
    exterior_wall_material_primary: mapExteriorWall(exteriorWall),
    exterior_wall_material_secondary: null,
    finished_base_area: totalBaseArea ? parseIntOrNull(totalBaseArea) : null,
    finished_basement_area: null,
    finished_upper_story_area: null,
    flooring_condition: null,
    flooring_material_primary: null,
    flooring_material_secondary: null,
    foundation_condition: null,
    foundation_material: null,
    foundation_type: null,
    foundation_waterproofing: null,
    gutters_condition: null,
    gutters_material: null,
    interior_door_material: null,
    interior_wall_condition: null,
    interior_wall_finish_primary: null,
    interior_wall_finish_secondary: null,
    interior_wall_structure_material: null,
    interior_wall_structure_material_primary: null,
    interior_wall_structure_material_secondary: null,
    interior_wall_surface_material_primary: null,
    interior_wall_surface_material_secondary: null,
    number_of_stories: bldgDetails.floors ?? null,
    primary_framing_material: mapFrame(frameMat),
    roof_age_years: null,
    roof_condition: null,
    roof_covering_material: mapRoofCovering(roofCover),
    roof_date: null,
    roof_design_type: mapRoofDesign(roofStruct),
    roof_material_type: null,
    roof_structure_material: null,
    roof_underlayment_type: null,
    secondary_framing_material: null,
    structural_damage_indicators: null,
    subfloor_material: null,
    unfinished_base_area: null,
    unfinished_basement_area: null,
    unfinished_upper_story_area: null,
    window_frame_material: null,
    window_glazing_type: null,
    window_operation_type: null,
    window_screen_material: null,
  };
  writeJSON(path.join(dataDir, "structure.json"), structure);

  // ---------- Utilities (owners/utilities_data.json) ----------
  const utilsKey = `property_${propertyId}`;
  const utilObj = utilsData[utilsKey] || null;
  if (utilObj) {
    writeJSON(path.join(dataDir, "utility.json"), utilObj);
  }

  // ---------- Layouts (owners/layout_data.json) ----------
  const layObj = layoutsData[`property_${propertyId}`];
  if (layObj && Array.isArray(layObj.layouts)) {
    layObj.layouts.forEach((layout, idx) => {
      writeJSON(path.join(dataDir, `layout_${idx + 1}.json`), layout);
    });
  }

  // ---------- Lot ----------
  const lotOut = {
    driveway_condition: null,
    driveway_material: null,
    fence_height: null,
    fence_length: null,
    fencing_type: null,
    landscaping_features: null,
    lot_area_sqft: acres != null ? Math.round(acres * 43560) : null,
    lot_condition_issues: null,
    lot_length_feet: null,
    lot_size_acre: acres != null ? acres : null,
    lot_type: null,
    lot_width_feet: null,
    view: null,
  };
  writeJSON(path.join(dataDir, "lot.json"), lotOut);
}

main();
