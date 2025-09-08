const fs = require("fs");
const path = require("path");
const cheerio = require("cheerio");

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

function readJSON(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function toTitleCase(str) {
  if (!str) return str;
  return str
    .toLowerCase()
    .split(/\s+/)
    .map((s) => s.charAt(0).toUpperCase() + s.slice(1))
    .join(" ");
}

function parseCurrency(str) {
  if (!str) return null;
  const n = parseFloat(String(str).replace(/[^0-9.\-]/g, ""));
  if (isNaN(n)) return null;
  return Math.round(n * 100) / 100;
}

function parseIntSafe(str) {
  const n = parseInt(String(str).replace(/[^0-9]/g, ""), 10);
  return isNaN(n) ? null : n;
}

function isoDateFromMDY(mdy) {
  if (!mdy) return null;
  const m = mdy.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (!m) return null;
  const [_, mm, dd, yyyy] = m;
  const m2 = mm.padStart(2, "0");
  const d2 = dd.padStart(2, "0");
  return `${yyyy}-${m2}-${d2}`;
}

function main() {
  const dataDir = path.join("data");
  ensureDir(dataDir);

  // Inputs
  const html = fs.readFileSync("input.html", "utf8");
  const $ = cheerio.load(html);
  const unnormalized = readJSON("unnormalized_address.json");
  const seed = readJSON("property_seed.json");

  // Owners, utilities, layout
  const ownerDataPath = path.join("owners", "owner_data.json");
  const utilitiesDataPath = path.join("owners", "utilities_data.json");
  const layoutDataPath = path.join("owners", "layout_data.json");

  const ownerData = fs.existsSync(ownerDataPath)
    ? readJSON(ownerDataPath)
    : null;
  const utilitiesData = fs.existsSync(utilitiesDataPath)
    ? readJSON(utilitiesDataPath)
    : null;
  const layoutData = fs.existsSync(layoutDataPath)
    ? readJSON(layoutDataPath)
    : null;

  const parcelId =
    seed.parcel_id ||
    seed.parcelId ||
    seed.parcel ||
    (function () {
      let pid = null;
      $('th:contains("General Information")')
        .closest("table")
        .find("tr")
        .each((i, el) => {
          const tds = $(el).find("td");
          if (tds.length >= 2) {
            const label = $(tds.get(0)).text().trim();
            const val = $(tds.get(1)).text().trim();
            if (/Parcel ID/i.test(label)) pid = val;
          }
        });
      return pid;
    })();

  // ---------------- Property ----------------
  let legalDesc = null;
  const legalTable = $('th:contains("Legal Description")').closest("table");
  if (legalTable && legalTable.length) {
    const td = legalTable.find("td").first();
    if (td && td.length) legalDesc = td.text().trim();
  }

  // Buildings header row
  let yearBuilt = null;
  let effYear = null;
  let finishedBaseArea = null;
  let areasTotal = null;
  let baseArea = null;
  let carportFin = null;
  let utilityUnf = null;
  let numberOfStories = null;
  let structMap = {};

  const bHeader = $('th:contains("Improvement Type:")').first();
  if (bHeader && bHeader.length) {
    const text = bHeader.text();
    const mYB = text.match(/Year Built:\s*(\d{4})/i);
    const mEY = text.match(/Effective Year:\s*(\d{4})/i);
    yearBuilt = mYB ? parseInt(mYB[1], 10) : null;
    effYear = mEY ? parseInt(mEY[1], 10) : null;
  }

  // Structural Elements and Areas
  const structCell = $('span:contains("Structural Elements"):first').parent();
  if (structCell && structCell.length) {
    const text = structCell.text();
    const extract = (label) => {
      const re = new RegExp(label + "\\s*-\\s*([^\\n]+)", "i");
      const m = text.match(re);
      return m ? m[1].trim() : null;
    };
    structMap.exteriorWall = extract("EXTERIOR WALL");
    structMap.floorCover = extract("FLOOR COVER");
    structMap.foundation = extract("FOUNDATION");
    structMap.heatAir = extract("HEAT/AIR");
    structMap.interiorWall = extract("INTERIOR WALL");
    structMap.stories = extract("NO\.?\s*STORIES");
    structMap.roofCover = extract("ROOF COVER");
    structMap.roofFraming = extract("ROOF FRAMING");
    structMap.storyHeight = extract("STORY HEIGHT");
    structMap.structuralFrame = extract("STRUCTURAL FRAME");

    // Fallbacks: parse via HTML tag proximity
    if (!structMap.stories) {
      const bStories = structCell
        .find("b")
        .filter((i, el) =>
          $(el).text().trim().toUpperCase().includes("NO. STORIES"),
        )
        .first();
      const iVal = bStories.length ? bStories.next("i").text().trim() : null;
      if (iVal) structMap.stories = iVal;
    }
    if (!structMap.exteriorWall) {
      const bEl = structCell
        .find("b")
        .filter((i, el) => $(el).text().toUpperCase().includes("EXTERIOR WALL"))
        .first();
      const iVal = bEl.length ? bEl.next("i").text().trim() : null;
      if (iVal) structMap.exteriorWall = iVal;
    }
    if (!structMap.floorCover) {
      const bEl = structCell
        .find("b")
        .filter((i, el) => $(el).text().toUpperCase().includes("FLOOR COVER"))
        .first();
      const iVal = bEl.length ? bEl.next("i").text().trim() : null;
      if (iVal) structMap.floorCover = iVal;
    }
    if (!structMap.foundation) {
      const bEl = structCell
        .find("b")
        .filter((i, el) => $(el).text().toUpperCase().includes("FOUNDATION"))
        .first();
      const iVal = bEl.length ? bEl.next("i").text().trim() : null;
      if (iVal) structMap.foundation = iVal;
    }
    if (!structMap.interiorWall) {
      const bEl = structCell
        .find("b")
        .filter((i, el) => $(el).text().toUpperCase().includes("INTERIOR WALL"))
        .first();
      const iVal = bEl.length ? bEl.next("i").text().trim() : null;
      if (iVal) structMap.interiorWall = iVal;
    }
    if (!structMap.roofCover) {
      const bEl = structCell
        .find("b")
        .filter((i, el) => $(el).text().toUpperCase().includes("ROOF COVER"))
        .first();
      const iVal = bEl.length ? bEl.next("i").text().trim() : null;
      if (iVal) structMap.roofCover = iVal;
    }
    if (!structMap.roofFraming) {
      const bEl = structCell
        .find("b")
        .filter((i, el) => $(el).text().toUpperCase().includes("ROOF FRAMING"))
        .first();
      const iVal = bEl.length ? bEl.next("i").text().trim() : null;
      if (iVal) structMap.roofFraming = iVal;
    }
    if (!structMap.structuralFrame) {
      const bEl = structCell
        .find("b")
        .filter((i, el) =>
          $(el).text().toUpperCase().includes("STRUCTURAL FRAME"),
        )
        .first();
      const iVal = bEl.length ? bEl.next("i").text().trim() : null;
      if (iVal) structMap.structuralFrame = iVal;
    }
  }

  const areasCell = $('span:contains("Areas -")').parent();
  if (areasCell && areasCell.length) {
    const headerText = areasCell.find("span").first().text();
    const mt = headerText.match(/Areas\s*-\s*(\d+)\s*Total\s*SF/i);
    areasTotal = mt ? parseInt(mt[1], 10) : null;
    const txt = areasCell.text();
    const mb = txt.match(/BASE AREA\s*-\s*(\d+)/i);
    baseArea = mb ? parseInt(mb[1], 10) : null;
    const mc = txt.match(/CARPORT FIN\s*-\s*(\d+)/i);
    carportFin = mc ? parseInt(mc[1], 10) : null;
    const mu = txt.match(/UTILITY UNF\s*-\s*(\d+)/i);
    utilityUnf = mu ? parseInt(mu[1], 10) : null;
  }

  if (structMap.stories) {
    numberOfStories = parseIntSafe(structMap.stories);
  }
  finishedBaseArea = baseArea || null;

  // Zoning, acreage, section map id (use inner HTML to avoid concatenation)
  let zoning = null;
  let acreage = null;
  let sectionMapId = null;
  const statsHtml = $("#ctl00_MasterPlaceHolder_MapBodyStats").html() || "";
  if (statsHtml) {
    const mz = statsHtml.match(
      /Zoned:[\s\S]*?<br\s*\/?>\s*([A-Za-z0-9\-]+)\s*<br/i,
    );
    if (mz) zoning = mz[1].trim();
    const txtStats = cheerio.load(`<div>${statsHtml}</div>`)("div").text();
    const ma = txtStats.match(/Approx\.\s*Acreage:\s*([0-9.]+)/i);
    if (ma) acreage = parseFloat(ma[1]);
    const ms = txtStats.match(/Section Map Id:\s*([0-9A-Za-z\-]+)/i);
    if (ms) sectionMapId = ms[1].trim();
  }
  let section = null,
    township = null,
    range = null;
  if (sectionMapId) {
    const parts = sectionMapId.split("-");
    if (parts.length >= 3) {
      section = parts[0];
      township = parts[1];
      range = parts[2];
    }
  }

  // Subdivision, lot and block from legal description
  let subdivision = null,
    lot = null,
    block = null;
  if (legalDesc) {
    const mLot = legalDesc.match(/LT\s+(\w+)/i);
    if (mLot) lot = mLot[1];
    const mBlk = legalDesc.match(/BLK\s+(\w+)/i);
    if (mBlk) block = mBlk[1];
    const mSub = legalDesc.match(/BLK\s+\w+\s+(.+?)\s+PB/i);
    if (mSub) subdivision = mSub[1].trim();
  }

  const property = {
    parcel_identifier: parcelId || null,
    property_type: "SingleFamily",
    number_of_units_type: "One",
    number_of_units: 1,
    livable_floor_area: baseArea != null ? `${baseArea} SF` : null,
    area_under_air: baseArea != null ? `${baseArea} SF` : null,
    total_area: areasTotal != null ? `${areasTotal} SF` : null,
    property_structure_built_year: yearBuilt || null,
    property_effective_built_year: effYear || null,
    property_legal_description_text: legalDesc || null,
    subdivision: subdivision || null,
    zoning: zoning || null,
  };
  fs.writeFileSync(
    path.join(dataDir, "property.json"),
    JSON.stringify(property, null, 2),
  );

  // ---------------- Address ----------------
  const fullAddr = unnormalized.full_address || "";
  const addrMatch = fullAddr.match(
    /^(\d+)\s+([^,]+?)\s+([A-Za-z\.]+),\s*([A-Z\s\-']+),\s*([A-Z]{2})\s*(\d{5})(?:-(\d{4}))?$/,
  );
  let street_number = null,
    street_name = null,
    street_suffix_type = null,
    city_name = null,
    state_code = null,
    postal_code = null,
    plus4 = null;
  if (addrMatch) {
    street_number = addrMatch[1];
    street_name = addrMatch[2].trim();
    street_suffix_type = addrMatch[3].replace(".", "");
    city_name = addrMatch[4].trim();
    state_code = addrMatch[5];
    postal_code = addrMatch[6];
    plus4 = addrMatch[7] || null;
  }
  const suffixMap = {
    RD: "Rd",
    ROAD: "Rd",
    Dr: "Dr",
    DR: "Dr",
    AV: "Ave",
    AVE: "Ave",
    AVENUE: "Ave",
    ST: "St",
    STREET: "St",
    BLVD: "Blvd",
  };
  if (street_suffix_type && suffixMap[street_suffix_type.toUpperCase()]) {
    street_suffix_type = suffixMap[street_suffix_type.toUpperCase()];
  }

  const address = {
    street_number: street_number || null,
    street_name: street_name ? street_name.replace(/\s+DR$/i, "").trim() : null,
    street_suffix_type: street_suffix_type || null,
    street_pre_directional_text: null,
    street_post_directional_text: null,
    city_name: city_name || null,
    state_code: state_code || null,
    postal_code: postal_code || null,
    plus_four_postal_code: plus4 || null,
    country_code: "US",
    county_name: "Escambia",
    municipality_name: null,
    latitude: null,
    longitude: null,
    unit_identifier: null,
    route_number: null,
    township: township || null,
    range: range || null,
    section: section || null,
    lot: lot || null,
    block: block || null,
  };
  fs.writeFileSync(
    path.join(dataDir, "address.json"),
    JSON.stringify(address, null, 2),
  );

  // ---------------- Lot ----------------
  let lot_area_sqft = null;
  if (typeof acreage === "number") {
    lot_area_sqft = Math.round(acreage * 43560);
  }
  const lotJson = {
    lot_type:
      typeof acreage === "number"
        ? acreage <= 0.25
          ? "LessThanOrEqualToOneQuarterAcre"
          : "GreaterThanOneQuarterAcre"
        : null,
    lot_length_feet: null,
    lot_width_feet: null,
    lot_area_sqft: lot_area_sqft || null,
    lot_size_acre: typeof acreage === "number" ? acreage : null,
    landscaping_features: null,
    view: null,
    fencing_type: null,
    fence_height: null,
    fence_length: null,
    driveway_material: null,
    driveway_condition: null,
    lot_condition_issues: null,
  };
  fs.writeFileSync(
    path.join(dataDir, "lot.json"),
    JSON.stringify(lotJson, null, 2),
  );

  // ---------------- Tax (all rows) ----------------
  const taxRows = [];
  $('th:contains("Assessments")')
    .closest("table")
    .find("tr[align=right]")
    .each((i, el) => {
      const tds = $(el).find("td");
      if (tds.length >= 5) {
        const year = parseInt($(tds.get(0)).text().trim(), 10);
        const land = parseCurrency($(tds.get(1)).text());
        const imprv = parseCurrency($(tds.get(2)).text());
        const total = parseCurrency($(tds.get(3)).text());
        const capVal = parseCurrency($(tds.get(4)).text());
        taxRows.push({ year, land, imprv, total, capVal });
      }
    });
  taxRows.forEach((r) => {
    const tax = {
      tax_year: r.year,
      property_assessed_value_amount: r.capVal != null ? r.capVal : null,
      property_market_value_amount: r.total != null ? r.total : null,
      property_building_amount: r.imprv != null ? r.imprv : null,
      property_land_amount: r.land != null ? r.land : null,
      property_taxable_value_amount: r.capVal != null ? r.capVal : null,
      monthly_tax_amount: null,
      yearly_tax_amount: null,
      period_start_date: null,
      period_end_date: null,
    };
    fs.writeFileSync(
      path.join(dataDir, `tax_${r.year}.json`),
      JSON.stringify(tax, null, 2),
    );
  });

  // ---------------- Structure ----------------
  const structure = {
    architectural_style_type: null,
    attachment_type: "Detached",
    exterior_wall_material_primary: /SIDING/i.test(structMap.exteriorWall || "")
      ? "Wood Siding"
      : null,
    exterior_wall_material_secondary: null,
    exterior_wall_condition: null,
    exterior_wall_insulation_type: null,
    flooring_material_primary: /CARPET/i.test(structMap.floorCover || "")
      ? "Carpet"
      : null,
    flooring_material_secondary: null,
    subfloor_material: /SLAB/i.test(structMap.foundation || "")
      ? "Concrete Slab"
      : null,
    flooring_condition: null,
    interior_wall_structure_material: /WOOD FRAME/i.test(
      structMap.structuralFrame || "",
    )
      ? "Wood Frame"
      : null,
    interior_wall_surface_material_primary: /DRYWALL|PLASTER/i.test(
      structMap.interiorWall || "",
    )
      ? "Drywall"
      : null,
    interior_wall_surface_material_secondary: null,
    interior_wall_finish_primary: null,
    interior_wall_finish_secondary: null,
    interior_wall_condition: null,
    roof_covering_material: /ARCH|DIMEN/i.test(structMap.roofCover || "")
      ? "Architectural Asphalt Shingle"
      : null,
    roof_underlayment_type: null,
    roof_structure_material: null,
    roof_design_type: /GABLE/i.test(structMap.roofFraming || "")
      ? "Gable"
      : null,
    roof_condition: null,
    roof_age_years: null,
    gutters_material: null,
    gutters_condition: null,
    roof_material_type: "Shingle",
    foundation_type: /SLAB/i.test(structMap.foundation || "")
      ? "Slab on Grade"
      : null,
    foundation_material: null,
    foundation_waterproofing: null,
    foundation_condition: null,
    ceiling_structure_material: null,
    ceiling_surface_material: null,
    ceiling_insulation_type: null,
    ceiling_height_average: null,
    ceiling_condition: null,
    exterior_door_material: null,
    interior_door_material: null,
    window_frame_material: null,
    window_glazing_type: null,
    window_operation_type: null,
    window_screen_material: null,
    primary_framing_material: /WOOD FRAME/i.test(
      structMap.structuralFrame || "",
    )
      ? "Wood Frame"
      : null,
    secondary_framing_material: null,
    structural_damage_indicators: null,
    finished_base_area: finishedBaseArea != null ? finishedBaseArea : null,
    finished_basement_area: null,
    finished_upper_story_area: null,
    unfinished_base_area: null,
    unfinished_basement_area: null,
    unfinished_upper_story_area: null,
    number_of_stories: numberOfStories != null ? numberOfStories : null,
  };
  fs.writeFileSync(
    path.join(dataDir, "structure.json"),
    JSON.stringify(structure, null, 2),
  );

  // ---------------- Utilities ----------------
  if (utilitiesData && utilitiesData[`property_${parcelId}`]) {
    const u = utilitiesData[`property_${parcelId}`];
    fs.writeFileSync(
      path.join(dataDir, "utility.json"),
      JSON.stringify(u, null, 2),
    );
  }

  // ---------------- Layouts ----------------
  if (
    layoutData &&
    layoutData[`property_${parcelId}`] &&
    Array.isArray(layoutData[`property_${parcelId}`].layouts)
  ) {
    const layouts = layoutData[`property_${parcelId}`].layouts;
    layouts.forEach((lay, idx) => {
      fs.writeFileSync(
        path.join(dataDir, `layout_${idx + 1}.json`),
        JSON.stringify(lay, null, 2),
      );
    });
  }

  // ---------------- Sales, Deeds, Files ----------------
  const salesRows = [];
  const salesTable = $('th:contains("Sales Data")').closest("table");
  salesTable.find("tr").each((i, el) => {
    const tds = $(el).find("td");
    if (
      tds.length === 7 &&
      $(tds.get(0)).text().trim() &&
      $(tds.get(0)).text().trim() !== "Sale Date"
    ) {
      const saleDateRaw = $(tds.get(0)).text().trim();
      const book = $(tds.get(1)).text().trim();
      const page = $(tds.get(2)).text().trim();
      const value = parseCurrency($(tds.get(3)).text());
      const typeCd = $(tds.get(4)).text().trim();
      const linkEl = $(tds.get(6)).find("a").first();
      const href =
        linkEl && linkEl.attr("href") ? linkEl.attr("href").trim() : null;

      const saleDate = isoDateFromMDY(saleDateRaw); // may be null if not full date

      salesRows.push({
        saleDateRaw,
        saleDate,
        book,
        page,
        value,
        typeCd,
        href,
      });
    }
  });

  // Map sale type codes to sales schema sale_type
  function mapSaleType(code) {
    if (!code) return null;
    const c = code.toUpperCase();
    if (c === "CJ") return "CourtOrderedNonForeclosureSale";
    if (c === "WD") return "TypicallyMotivated";
    return null;
  }

  // Map deed type by sale code when determinable
  function mapDeedType(code) {
    if (!code) return null;
    const c = code.toUpperCase();
    if (c === "WD") return "Warranty Deed";
    if (c === "CJ") return "Sheriff's Deed"; // court-ordered judgment
    return null;
  }

  salesRows.forEach((row, idx) => {
    const sIdx = idx + 1;
    const sales = {
      ownership_transfer_date: row.saleDate || null,
      purchase_price_amount: row.value != null ? row.value : null,
      sale_type: mapSaleType(row.typeCd),
    };
    fs.writeFileSync(
      path.join(dataDir, `sales_${sIdx}.json`),
      JSON.stringify(sales, null, 2),
    );

    const deedObj = {};
    const dType = mapDeedType(row.typeCd);
    if (dType) deedObj.deed_type = dType;
    fs.writeFileSync(
      path.join(dataDir, `deed_${sIdx}.json`),
      JSON.stringify(deedObj, null, 2),
    );

    const fileObj = {
      file_format: null,
      name:
        row.book && row.page
          ? `Instrument OR Book ${row.book} Page ${row.page}`
          : null,
      original_url: row.href || null,
      ipfs_url: null,
      document_type: /^WD$/i.test(row.typeCd)
        ? "ConveyanceDeedWarrantyDeed"
        : null,
    };
    fs.writeFileSync(
      path.join(dataDir, `file_${sIdx}.json`),
      JSON.stringify(fileObj, null, 2),
    );
  });

  // ---------------- Owners ----------------
  if (
    ownerData &&
    ownerData[`property_${parcelId}`] &&
    ownerData[`property_${parcelId}`].owners_by_date &&
    Array.isArray(ownerData[`property_${parcelId}`].owners_by_date.current)
  ) {
    const currentOwners =
      ownerData[`property_${parcelId}`].owners_by_date.current;
    let personCount = 0;
    let companyCount = 0;
    currentOwners.forEach((o) => {
      if (o.type === "person") {
        personCount += 1;
        const first = toTitleCase(o.first_name || "");
        const last = toTitleCase(o.last_name || "");
        const middle = o.middle_name ? toTitleCase(o.middle_name) : null;
        const person = {
          birth_date: null,
          first_name: first,
          last_name: last,
          middle_name: middle,
          prefix_name: null,
          suffix_name: null,
          us_citizenship_status: null,
          veteran_status: null,
        };
        fs.writeFileSync(
          path.join(dataDir, `person_${personCount}.json`),
          JSON.stringify(person, null, 2),
        );
      } else if (o.type === "company") {
        companyCount += 1;
        const company = { name: o.name || null };
        fs.writeFileSync(
          path.join(dataDir, `company_${companyCount}.json`),
          JSON.stringify(company, null, 2),
        );
      }
    });

    // Relationship between most recent sale and owners
    if (salesRows.length > 0) {
      const relSalesIdx = 1;
      if (personCount > 0) {
        const rel = {
          to: { "/": `./person_1.json` },
          from: { "/": `./sales_${relSalesIdx}.json` },
        };
        fs.writeFileSync(
          path.join(dataDir, "relationship_sales_person.json"),
          JSON.stringify(rel, null, 2),
        );
      } else if (companyCount > 0) {
        const rel = {
          to: { "/": `./company_1.json` },
          from: { "/": `./sales_${relSalesIdx}.json` },
        };
        fs.writeFileSync(
          path.join(dataDir, "relationship_sales_company.json"),
          JSON.stringify(rel, null, 2),
        );
      }
    }
  }

  // ---------------- Relationships for deed-file and sales-deed for all rows ----------------
  if (salesRows.length > 0) {
    const relDFAll = salesRows.map((_, i) => ({
      to: { "/": `./deed_${i + 1}.json` },
      from: { "/": `./file_${i + 1}.json` },
    }));
    fs.writeFileSync(
      path.join(dataDir, "relationship_deed_file.json"),
      JSON.stringify(relDFAll, null, 2),
    );

    const relSDAll = salesRows.map((_, i) => ({
      to: { "/": `./sales_${i + 1}.json` },
      from: { "/": `./deed_${i + 1}.json` },
    }));
    fs.writeFileSync(
      path.join(dataDir, "relationship_sales_deed.json"),
      JSON.stringify(relSDAll, null, 2),
    );
  }
}

if (require.main === module) {
  main();
}
