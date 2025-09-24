// Structure mapping script (revised)
// Reads input.html, extracts structure data using cheerio, writes owners/structure_data.json

const fs = require("fs");
const path = require("path");
const cheerio = require("cheerio");

function textClean(t) {
  if (!t) return "";
  return String(t).replace(/\s+/g, " ").trim();
}

function numFromText(t) {
  if (!t) return null;
  const n = String(t).replace(/[^0-9.\-]/g, "");
  if (n === "") return null;
  const parsed = parseFloat(n);
  return isNaN(parsed) ? null : parsed;
}

function intFromText(t) {
  if (!t) return null;
  const n = String(t).replace(/[^0-9\-]/g, "");
  if (n === "") return null;
  const parsed = parseInt(n, 10);
  return isNaN(parsed) ? null : parsed;
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function loadHtml(filePath) {
  const html = fs.readFileSync(filePath, "utf8");
  return cheerio.load(html);
}

function findMaterialsMap($) {
  const map = {};
  $("#divBldg_Materials table tbody tr").each((i, tr) => {
    const tds = $(tr).find("td");
    if (tds.length >= 2) {
      const label = textClean($(tds[0]).text()).replace(/:$/, "");
      const val = textClean($(tds[1]).text());
      if (label) map[label] = val;
    }
  });
  return map;
}

function findDetailsMap($) {
  const map = {};
  $("#divBldg_Details table tbody tr").each((i, tr) => {
    const tds = $(tr).find("td");
    if (tds.length >= 2) {
      const label = textClean($(tds[0]).text()).replace(/:$/, "");
      const val = textClean($(tds[1]).text());
      if (label) map[label] = val;
    }
  });
  return map;
}

function getTotalBaseArea($) {
  let area = null;
  $("#divBldg_SubAreas table tbody tr").each((i, tr) => {
    const tds = $(tr).find("td");
    if (tds.length >= 2) {
      const label = textClean($(tds[0]).text());
      if (/^Total Base Area$/i.test(label)) {
        const val = textClean($(tds[1]).text());
        const n = intFromText(val);
        if (n !== null) area = n;
      }
    }
  });
  return area;
}

function mapExteriorMaterial(desc) {
  if (!desc) return null;
  const u = desc.toUpperCase();
  if (u.includes("STUCCO")) return "Stucco";
  if (u.includes("BRICK")) return "Brick";
  if (u.includes("STONE")) return "Natural Stone";
  if (u.includes("VINYL")) return "Vinyl Siding";
  if (u.includes("WOOD")) return "Wood Siding";
  if (u.includes("FIBER") || u.includes("HARDI")) return "Fiber Cement Siding";
  if (u.includes("METAL")) return "Metal Siding";
  if (u.includes("BLOCK")) return "Concrete Block";
  return null;
}

function mapPrimaryFrame(desc) {
  if (!desc) return null;
  const u = desc.toUpperCase();
  if (u.includes("WOOD")) return "Wood Frame";
  if (u.includes("STEEL")) return "Steel Frame";
  if (
    u.includes("MASON") ||
    u.includes("MASN") ||
    u.includes("CONC") ||
    u.includes("BLOCK")
  )
    return "Masonry";
  if (u.includes("POURED")) return "Poured Concrete";
  return null;
}

function mapRoofDesign(desc) {
  if (!desc) return null;
  const u = desc.toUpperCase();
  const hasHip = u.includes("HIP");
  const hasGable = u.includes("GABLE");
  if (hasHip && hasGable) return "Combination";
  if (hasHip) return "Hip";
  if (hasGable) return "Gable";
  if (u.includes("FLAT")) return "Flat";
  if (u.includes("SHED")) return "Shed";
  return null;
}

function mapRoofMaterialType(desc) {
  if (!desc) return null;
  const u = desc.toUpperCase();
  if (u.includes("SHING")) return "Shingle";
  if (u.includes("METAL")) return "Metal";
  if (u.includes("TILE") || u.includes("CLAY") || u.includes("CONC"))
    return "Tile";
  if (u.includes("SLATE")) return "Stone";
  if (u.includes("TPO")) return "TPO Membrane";
  if (u.includes("EPDM")) return "EPDM Membrane";
  return null;
}

function mapRoofCovering(desc) {
  if (!desc) return null;
  const u = desc.toUpperCase();
  // Map generic asphalt/asbestos shingle to 3-Tab Asphalt Shingle conservatively
  if (u.includes("ASPH") && u.includes("SHNGL")) return "3-Tab Asphalt Shingle";
  if (u.includes("ARCH") && u.includes("SHING"))
    return "Architectural Asphalt Shingle";
  if (u.includes("METAL")) return "Metal Standing Seam";
  if (u.includes("TPO")) return "TPO Membrane";
  if (u.includes("EPDM")) return "EPDM Membrane";
  if (u.includes("SLATE")) return "Natural Slate";
  if (u.includes("TILE") || u.includes("CLAY")) return "Clay Tile";
  if (u.includes("CONC")) return "Concrete Tile";
  return null;
}

function extract() {
  const $ = loadHtml("input.html");

  const account = textClean($("#hfAccount").attr("value")) || null;
  const propKey = account ? `property_${account}` : "property_unknown";

  const materials = findMaterialsMap($);
  const details = findDetailsMap($);

  const exteriorWallDesc = materials["Exterior Wall"] || null;
  const frameDesc = materials["Frame"] || null;
  const roofCoverDesc = materials["Roof"] || null;
  const roofStructDesc = materials["Roof Structure"] || null;

  const extMat = mapExteriorMaterial(exteriorWallDesc);
  const primaryFrame = mapPrimaryFrame(frameDesc);
  const roofDesign = mapRoofDesign(roofStructDesc);
  const roofMatType = mapRoofMaterialType(roofCoverDesc);
  const roofCovering = mapRoofCovering(roofCoverDesc);

  const totalBaseArea = getTotalBaseArea($);
  const floors = intFromText(details["Floors"]);
  const storyHeight = numFromText(details["Story Height"]);

  // attachment type: infer from Bldg. Use text
  let attachmentType = null;
  const bldgUse = textClean(details["Bldg. Use"]);
  if (/SINGLE\s+FAMILY/i.test(bldgUse)) attachmentType = "Detached";

  const structure = {
    architectural_style_type: null,
    attachment_type: attachmentType,
    ceiling_condition: null,
    ceiling_height_average: storyHeight !== null ? storyHeight : null,
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
    exterior_wall_material_primary: extMat || null,
    exterior_wall_material_secondary: null,
    finished_base_area: totalBaseArea !== null ? totalBaseArea : null,
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
    number_of_stories: floors !== null ? floors : null,
    primary_framing_material: primaryFrame || null,
    roof_age_years: null,
    roof_condition: null,
    roof_covering_material: roofCovering || null,
    roof_date: null,
    roof_design_type: roofDesign || null,
    roof_material_type: roofMatType || null,
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

  const out = {};
  out[propKey] = structure;

  ensureDir("owners");
  fs.writeFileSync(
    path.join("owners", "structure_data.json"),
    JSON.stringify(out, null, 2),
    "utf8",
  );
}

extract();
