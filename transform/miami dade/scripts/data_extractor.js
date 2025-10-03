const fs = require("fs");
const path = require("path");
const cheerio = require("cheerio");

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

function readJSON(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function writeJSON(p, obj) {
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}

function removeIfExists(p) {
  if (fs.existsSync(p)) {
    try {
      fs.unlinkSync(p);
    } catch {}
  }
}

function loadInputHtmlJson() {
  const html = fs.readFileSync("input.html", "utf8");
  const $ = cheerio.load(html);
  const pre = $("pre").first().text();
  let json;
  try {
    json = JSON.parse(pre);
  } catch (e) {
    throw new Error("Failed to parse JSON from input.html <pre>: " + e.message);
  }
  return json;
}

function toTitleCaseName(s) {
  if (!s || typeof s !== "string") return s;
  return s
    .toLowerCase()
    .split(/\s+/)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function parseIsoDate(mdy) {
  if (!mdy) return null;
  const parts = mdy.split("/");
  if (parts.length !== 3) return null;
  const [m, d, y] = parts.map((x) => x.trim());
  const mm = String(parseInt(m, 10)).padStart(2, "0");
  const dd = String(parseInt(d, 10)).padStart(2, "0");
  return `${y}-${mm}-${dd}`;
}

/**
 * Coerces a numeric input into a JavaScript number.
 * - Accepts numbers, numeric strings (e.g., "0", "1,234", "$5,000.50"), and returns a finite number
 * - Returns null for null/undefined/empty strings or non-numeric strings
 *
 * @param {number|string|null|undefined} value Input that may represent a number
 * @returns {number|null} Parsed finite number or null
 */
function toNumberOrNull(value) {
  if (value == null) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value !== "string") return null;
  const normalized = value.replace(/[$,\s]/g, "");
  if (normalized === "" || normalized === "-") return null;
  const n = Number(normalized);
  return Number.isFinite(n) ? n : null;
}

function main() {
  ensureDir("data");

  const htmlJson = loadInputHtmlJson();
  const unnormalized = readJSON("unnormalized_address.json");
  const seed = readJSON("property_seed.json");

  // Optional sources (owners/utilities/layout)
  let ownerData = null,
    utilitiesData = null,
    layoutData = null;
  try {
    ownerData = readJSON(path.join("owners", "owner_data.json"));
  } catch {}
  try {
    utilitiesData = readJSON(path.join("owners", "utilities_data.json"));
  } catch {}
  try {
    layoutData = readJSON(path.join("owners", "layout_data.json"));
  } catch {}

  // Property
  const propInfo = htmlJson.PropertyInfo || {};
  const legal = htmlJson.LegalDescription || {};
  const land =
    htmlJson.Land && htmlJson.Land.Landlines ? htmlJson.Land.Landlines : [];
  const buildings =
    htmlJson.Building && htmlJson.Building.BuildingInfos
      ? htmlJson.Building.BuildingInfos
      : [];

  const primaryBldg = buildings && buildings.length ? buildings[0] : null;

  const property = {
    livable_floor_area:
      propInfo.BuildingHeatedArea != null
        ? String(propInfo.BuildingHeatedArea)
        : null,
    area_under_air:
      propInfo.BuildingHeatedArea != null
        ? String(propInfo.BuildingHeatedArea)
        : null,
    total_area:
      propInfo.BuildingGrossArea != null
        ? String(propInfo.BuildingGrossArea)
        : null,
    number_of_units: propInfo.UnitCount != null ? propInfo.UnitCount : null,
    
    // ///////////////////////////////////////////
    // number_of_units_type: "One",    
    number_of_units_type:
      propInfo.UnitCount === 1 ? "One" :
      propInfo.UnitCount === 2 ? "Two" :
      propInfo.UnitCount === 3 ? "Three" :
      propInfo.UnitCount === 4 ? "Four" :
      null,
    // //////////////////////////////////////////
    
    parcel_identifier: seed.parcel_id || propInfo.FolioNumber || "",
    property_legal_description_text: legal.Description || null,
    property_structure_built_year:
      primaryBldg && primaryBldg.Actual
        ? parseInt(primaryBldg.Actual, 10)
        : null,
    property_effective_built_year:
      primaryBldg && primaryBldg.Effective
        ? parseInt(primaryBldg.Effective, 10)
        : null,

    // /////////////////////////////////////
    // property_type: "SingleFamily",
    property_type:    
      ["0101", "0102", "0104","0105","0176"].includes(propInfo.DORCode) ? "SingleFamily" :
      ["0407",""].includes(propInfo.DORCode) ? "Condominium" :
      ["0303","0317","0318","0802","0803"].includes(propInfo.DORCode) ? "MultipleFamily" :
      ["0410","0470"].includes(propInfo.DORCode) ? "Townhouse" :
      ["0206"].includes(propInfo.DORCode) ? "MobileHome" :
      ["0508"].includes(propInfo.DORCode) ? "Cooperative" :
      ["0643"].includes(propInfo.DORCode) ? "Retirement" :
      null,

    // /////////////////////////////////////

    subdivision:
      (propInfo.SubdivisionDescription &&
      propInfo.SubdivisionDescription.trim().length
        ? propInfo.SubdivisionDescription
        : null) || null,
    zoning:
      land &&
      land.length &&
      (land[0].PAZoneDescription || land[0].MuniZoneDescription || null),
  };
  writeJSON(path.join("data", "property.json"), property);

  // Address
  const siteAddr =
    htmlJson.SiteAddress && htmlJson.SiteAddress.length
      ? htmlJson.SiteAddress[0]
      : {};
  const mailing = htmlJson.MailingAddress || {};

  const zipRaw = mailing.ZipCode || siteAddr.Zip || "";
  let postal_code = null,
    plus_four_postal_code = null;
  if (zipRaw && typeof zipRaw === "string" && zipRaw.includes("-")) {
    const [zip5, plus4] = zipRaw.split("-");
    postal_code = zip5 || null;
    plus_four_postal_code = plus4 || null;
  } else if (zipRaw && typeof zipRaw === "string" && zipRaw.length === 5) {
    postal_code = zipRaw;
    plus_four_postal_code = null;
  }

  const address = {
    street_number:
      siteAddr.StreetNumber != null ? String(siteAddr.StreetNumber) : null,
    street_pre_directional_text: siteAddr.StreetPrefix || null,
    street_name: siteAddr.StreetName || null,
    street_suffix_type: siteAddr.StreetSuffix
      ? siteAddr.StreetSuffix.toUpperCase() === "ST"
        ? "St"
        : null
      : "St",
    street_post_directional_text: null,
    city_name: mailing.City
      ? String(mailing.City).toUpperCase()
      : siteAddr.City
        ? String(siteAddr.City).toUpperCase()
        : null,
    county_name: unnormalized.county_jurisdiction || null,
    state_code: mailing.State || "FL",
    postal_code:
      postal_code ||
      (unnormalized.full_address &&
        unnormalized.full_address.split(",").pop().trim().split(" ").pop()) ||
      null,
    plus_four_postal_code: plus_four_postal_code || null,
    country_code: null,
    latitude: null,
    longitude: null,
    route_number: null,
    township: null,
    range: null,
    section: null,
    block: null,
    lot: null,
    unit_identifier: null,
  };
  writeJSON(path.join("data", "address.json"), address);

  // Lot
  const lot = {
    lot_type: null,
    lot_length_feet: null,
    lot_width_feet: null,
    lot_area_sqft:
      propInfo.LotSize != null ? parseInt(propInfo.LotSize, 10) : null,
    landscaping_features: null,
    view: null,
    fencing_type: null,
    fence_height: null,
    fence_length: null,
    driveway_material: null,
    driveway_condition: null,
    lot_condition_issues: null,
  };
  if (legal.Description) {
    const m = legal.Description.match(/LOT SIZE\s+([0-9.]+)\s*X\s*([0-9.]+)/i);
    if (m) {
      const a = Math.round(parseFloat(m[1]));
      const b = Math.round(parseFloat(m[2]));
      lot.lot_length_feet = Number.isFinite(a) ? a : null;
      lot.lot_width_feet = Number.isFinite(b) ? b : null;
    }
  }
  const extra =
    htmlJson.ExtraFeature && htmlJson.ExtraFeature.ExtraFeatureInfos
      ? htmlJson.ExtraFeature.ExtraFeatureInfos
      : [];
  const fence = extra.find((e) =>
    (e.Description || "").toLowerCase().includes("fence"),
  );
  if (fence) {
    lot.fencing_type = "ChainLink";
    if (typeof fence.Units === "number") {
      const units = Math.round(fence.Units);
      const validLengths = [
        "25ft",
        "50ft",
        "75ft",
        "100ft",
        "150ft",
        "200ft",
        "300ft",
        "500ft",
        "1000ft",
      ];
      const candidate = `${units}ft`;
      lot.fence_length = validLengths.includes(candidate) ? candidate : null;
    }
  }
  writeJSON(path.join("data", "lot.json"), lot);

  // Taxes
  const assessments =
    htmlJson.Assessment && htmlJson.Assessment.AssessmentInfos
      ? htmlJson.Assessment.AssessmentInfos
      : [];
  const taxableInfos =
    htmlJson.Taxable && htmlJson.Taxable.TaxableInfos
      ? htmlJson.Taxable.TaxableInfos
      : [];
  assessments.forEach((a) => {
    const year = a.Year;
    if (!year) return;
    const taxable = taxableInfos.find((t) => t.Year === year) || {};
    const taxObj = {
      tax_year: year,
      property_assessed_value_amount: a.AssessedValue,
      property_market_value_amount: a.TotalValue,
      property_building_amount:
        a.BuildingOnlyValue != null ? a.BuildingOnlyValue : null,
      property_land_amount: a.LandValue != null ? a.LandValue : null,
      property_taxable_value_amount:
        taxable.CountyTaxableValue != null ? taxable.CountyTaxableValue : null,
      monthly_tax_amount: null,
      yearly_tax_amount: null,
      period_start_date: null,
      period_end_date: null,
      first_year_on_tax_roll: null,
      first_year_building_on_tax_roll: null,
    };
    writeJSON(path.join("data", `tax_${year}.json`), taxObj);
  });

  // Sales: include all entries (including price=0 and numeric-string zeros)
  const salesInfos = htmlJson.SalesInfos || [];
  const sortedSales = salesInfos
    .map((s) => ({ ...s, _iso: parseIsoDate(s.DateOfSale) }))
    .filter((s) => s._iso)
    .sort((a, b) => new Date(a._iso) - new Date(b._iso));
  const salesFiles = [];
  sortedSales.forEach((s, idx) => {
    const filename = `sales_${idx + 1}.json`;
    const salesObj = {
      ownership_transfer_date: s._iso,
      purchase_price_amount: toNumberOrNull(s.SalePrice),
    };
    writeJSON(path.join("data", filename), salesObj);
    salesFiles.push(filename);
  });

  // Deed minimal record
  writeJSON(path.join("data", "deed_1.json"), {});

  // No deed file references in input; ensure we don't create unsupported files/relationships
  removeIfExists(path.join("data", "file_1.json"));
  removeIfExists(path.join("data", "relationship_deed_file.json"));

  // Relationship sales ↔ deed: link latest sale (if any)
  if (salesFiles.length > 0) {
    const latestSales = `./${salesFiles[salesFiles.length - 1]}`;
    writeJSON(path.join("data", "relationship_sales_deed.json"), {
      to: { "/": latestSales },
      from: { "/": "./deed_1.json" },
    });
  }

  // Owners (persons only), from owners/owner_data.json
  if (ownerData) {
    const keyVariants = [
      `property_${seed.parcel_id}`,
      `property_${propInfo.FolioNumber || ""}`,
      `property_${(propInfo.FolioNumber || "").replace(/-/g, "")}`,
    ];
    let ownersNode = null;
    for (const k of keyVariants) {
      if (ownerData[k]) {
        ownersNode = ownerData[k];
        break;
      }
    }
    if (!ownersNode) {
      const firstKey = Object.keys(ownerData)[0];
      ownersNode = ownerData[firstKey];
    }
    if (
      ownersNode &&
      ownersNode.owners_by_date &&
      ownersNode.owners_by_date.current
    ) {
      const currentOwners = ownersNode.owners_by_date.current;
      const persons = currentOwners.filter((o) => o.type === "person");
      const personFiles = [];
      persons.forEach((p, i) => {
        const personObj = {
          birth_date: null,
          first_name: toTitleCaseName(p.first_name || null),
          last_name: toTitleCaseName(p.last_name || null),
          middle_name: p.middle_name ? p.middle_name : null,
          prefix_name: null,
          suffix_name: null,
          us_citizenship_status: null,
          veteran_status: null,
        };
        const fname = path.join("data", `person_${i + 1}.json`);
        writeJSON(fname, personObj);
        personFiles.push(`./person_${i + 1}.json`);
      });
      if (salesFiles.length > 0 && personFiles.length > 0) {
        const latestSales = `./${salesFiles[salesFiles.length - 1]}`;
        personFiles.forEach((pf, idx) => {
          writeJSON(
            path.join("data", `relationship_sales_person_${idx + 1}.json`),
            {
              to: { "/": pf },
              from: { "/": latestSales },
            },
          );
        });
      }
    }
  }

  // Utilities from owners/utilities_data.json
  if (utilitiesData) {
    const utilKey = `property_${seed.parcel_id}`;
    const utilNode = utilitiesData[utilKey] || null;
    if (utilNode) {
      const utilObj = {
        cooling_system_type: utilNode.cooling_system_type ?? null,
        heating_system_type: utilNode.heating_system_type ?? null,
        public_utility_type: utilNode.public_utility_type ?? null,
        sewer_type: utilNode.sewer_type ?? null,
        water_source_type: utilNode.water_source_type ?? null,
        plumbing_system_type: utilNode.plumbing_system_type ?? null,
        plumbing_system_type_other_description:
          utilNode.plumbing_system_type_other_description ?? null,
        electrical_panel_capacity: utilNode.electrical_panel_capacity ?? null,
        electrical_wiring_type: utilNode.electrical_wiring_type ?? null,
        hvac_condensing_unit_present:
          utilNode.hvac_condensing_unit_present ?? null,
        electrical_wiring_type_other_description:
          utilNode.electrical_wiring_type_other_description ?? null,
        solar_panel_present: !!utilNode.solar_panel_present,
        solar_panel_type: utilNode.solar_panel_type ?? null,
        solar_panel_type_other_description:
          utilNode.solar_panel_type_other_description ?? null,
        smart_home_features: utilNode.smart_home_features ?? null,
        smart_home_features_other_description:
          utilNode.smart_home_features_other_description ?? null,
        hvac_unit_condition: utilNode.hvac_unit_condition ?? null,
        solar_inverter_visible: !!utilNode.solar_inverter_visible,
        hvac_unit_issues: utilNode.hvac_unit_issues ?? null,
      };
      writeJSON(path.join("data", "utility.json"), utilObj);
    }
  }

  // Layouts from owners/layout_data.json
  if (layoutData) {
    const layoutKey = `property_${seed.parcel_id}`;
    const layoutNode = layoutData[layoutKey] || null;
    if (layoutNode && Array.isArray(layoutNode.layouts)) {
      layoutNode.layouts.forEach((l, idx) => {
        const layoutObj = {
          space_type: l.space_type ?? null,
          space_index: l.space_index,
          flooring_material_type: l.flooring_material_type ?? null,
          size_square_feet: l.size_square_feet ?? null,
          floor_level: l.floor_level ?? null,
          has_windows: l.has_windows ?? null,
          window_design_type: l.window_design_type ?? null,
          window_material_type: l.window_material_type ?? null,
          window_treatment_type: l.window_treatment_type ?? null,
          is_finished: !!l.is_finished,
          furnished: l.furnished ?? null,
          paint_condition: l.paint_condition ?? null,
          flooring_wear: l.flooring_wear ?? null,
          clutter_level: l.clutter_level ?? null,
          visible_damage: l.visible_damage ?? null,
          countertop_material: l.countertop_material ?? null,
          cabinet_style: l.cabinet_style ?? null,
          fixture_finish_quality: l.fixture_finish_quality ?? null,
          design_style: l.design_style ?? null,
          natural_light_quality: l.natural_light_quality ?? null,
          decor_elements: l.decor_elements ?? null,
          pool_type: l.pool_type ?? null,
          pool_equipment: l.pool_equipment ?? null,
          spa_type: l.spa_type ?? null,
          safety_features: l.safety_features ?? null,
          view_type: l.view_type ?? null,
          lighting_features: l.lighting_features ?? null,
          condition_issues: l.condition_issues ?? null,
          is_exterior: !!l.is_exterior,
          pool_condition: l.pool_condition ?? null,
          pool_surface_type: l.pool_surface_type ?? null,
          pool_water_quality: l.pool_water_quality ?? null,
        };
        writeJSON(path.join("data", `layout_${idx + 1}.json`), layoutObj);
      });
    }
  }

  // Structure: base from input.html, then overlay with supplemental structure_data if present
  const structureObj = {
    architectural_style_type: null,
    attachment_type: null,
    exterior_wall_material_primary: null,
    exterior_wall_material_secondary: null,
    exterior_wall_condition: null,
    exterior_wall_insulation_type: null,
    flooring_material_primary: null,
    flooring_material_secondary: null,
    subfloor_material: null,
    flooring_condition: null,
    interior_wall_structure_material: null,
    interior_wall_surface_material_primary: null,
    interior_wall_surface_material_secondary: null,
    interior_wall_finish_primary: null,
    interior_wall_finish_secondary: null,
    interior_wall_condition: null,
    roof_covering_material: null,
    roof_underlayment_type: null,
    roof_structure_material: null,
    roof_design_type: null,
    roof_condition: null,
    roof_age_years: null,
    gutters_material: null,
    gutters_condition: null,
    roof_material_type: null,
    foundation_type: null,
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
    primary_framing_material: null,
    secondary_framing_material: null,
    structural_damage_indicators: null,
    number_of_stories: propInfo.FloorCount != null ? propInfo.FloorCount : null,
    finished_base_area:
      propInfo.BuildingHeatedArea != null
        ? parseInt(propInfo.BuildingHeatedArea, 10)
        : null,
    finished_basement_area: null,
    finished_upper_story_area: null,
    unfinished_base_area: null,
    unfinished_basement_area: null,
    unfinished_upper_story_area: null,
    roof_date: null,
    exterior_wall_condition_primary: null,
    exterior_wall_condition_secondary: null,
    exterior_wall_insulation_type_primary: null,
    exterior_wall_insulation_type_secondary: null,
  };

  // Overlay with supplemental structure_data if available
  let supplementalStructure = null;
  const structCandidatePaths = [
    "structure_data.json",
    path.join("data", "structure_data.json"),
  ];
  for (const pth of structCandidatePaths) {
    if (!supplementalStructure && fs.existsSync(pth)) {
      try {
        supplementalStructure = readJSON(pth);
      } catch {}
    }
  }
  if (supplementalStructure) {
    const key = `property_${seed.parcel_id}`;
    const node = supplementalStructure[key] || null;
    if (node) {
      // For each field in node, if defined, override our structureObj
      Object.keys(node).forEach((k) => {
        if (k in structureObj) {
          structureObj[k] = node[k];
        }
      });
    }
  }

  writeJSON(path.join("data", "structure.json"), structureObj);
}

main();
