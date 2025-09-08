// Structure data extractor using Cheerio
// Reads input.html, extracts structural details, and writes JSON to data/ and owners/

const fs = require("fs");
const path = require("path");
const cheerio = require("cheerio");

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function textNorm(t) {
  return (t || "").replace(/\s+/g, " ").trim();
}

function getParcelId($) {
  let id = null;
  $("#ctl00_MasterPlaceHolder_GenCell table tr").each((_, tr) => {
    const $tds = $(tr).find("td");
    const label = textNorm($tds.eq(0).text());
    if (/^Parcel ID:/i.test(label)) {
      id = textNorm($tds.eq(1).text());
    }
  });
  return id;
}

function parseStructuralElements($) {
  const raw = {};
  // Look within the Buildings section for label-value pairs in the Structural Elements span
  const buildingsTable = $("#ctl00_MasterPlaceHolder_tblBldgs");
  buildingsTable.find("b").each((_, b) => {
    const label = textNorm($(b).text());
    const val = textNorm($(b).next("i").text());
    if (label) raw[label.toUpperCase()] = val || null;
  });
  // Areas block for base area, etc.
  const areas = {};
  buildingsTable.find("span").each((_, s) => {
    const t = textNorm($(s).text());
    if (/Areas\s*-\s*\d+\s*Total\s*SF/i.test(t)) {
      // After this span, there is a list with <b>BASE AREA</b> - <i>912</i>, etc.
      const container = $(s).parent();
      container.find("b").each((_, b) => {
        const key = textNorm($(b).text());
        const val = parseInt(
          textNorm($(b).next("i").text()).replace(/[^0-9]/g, ""),
          10,
        );
        if (!isNaN(val)) areas[key.toUpperCase()] = val;
      });
    }
  });
  return { raw, areas };
}

function mapExteriorWallMaterial(value) {
  if (!value) return null;
  const v = value.toUpperCase();
  if (v.includes("BRICK")) return "Brick";
  if (v.includes("STONE")) return "Natural Stone";
  if (v.includes("STUCCO")) return "Stucco";
  if (v.includes("VINYL")) return "Vinyl Siding";
  if (v.includes("FIBER") || v.includes("HARDIE") || v.includes("FIBRE"))
    return "Fiber Cement Siding";
  if (v.includes("METAL")) return "Metal Siding";
  if (v.includes("BLOCK") || v.includes("CMU")) return "Concrete Block";
  if (v.includes("EIFS")) return "EIFS";
  if (v.includes("LOG")) return "Log";
  if (v.includes("ADOBE")) return "Adobe";
  if (v.includes("SIDING") || v.includes("WOOD")) return "Wood Siding";
  return null;
}

function mapFlooringPrimary(value) {
  if (!value) return null;
  const v = value.toUpperCase();
  if (v.includes("CARPET")) return "Carpet";
  if (
    v.includes("LVP") ||
    v.includes("LUXURY VINYL") ||
    v.includes("LUX VINYL")
  )
    return "Luxury Vinyl Plank";
  if (v.includes("VINYL")) return "Sheet Vinyl";
  if (v.includes("LAMINATE")) return "Laminate";
  if (v.includes("TILE")) return "Ceramic Tile";
  if (v.includes("STONE")) return "Natural Stone Tile";
  if (v.includes("HARDWOOD") || v.includes("WOOD")) return "Solid Hardwood";
  if (v.includes("CONCRETE")) return "Polished Concrete";
  return null;
}

function mapFoundationType(value) {
  if (!value) return null;
  const v = value.toUpperCase();
  if (v.includes("SLAB")) return "Slab on Grade";
  if (v.includes("CRAWL")) return "Crawl Space";
  if (v.includes("WALKOUT")) return "Basement with Walkout";
  if (v.includes("BASEMENT")) return "Full Basement";
  if (v.includes("PIER") || v.includes("BEAM")) return "Pier and Beam";
  if (v.includes("STEM")) return "Stem Wall";
  return null;
}

function mapRoofCovering(value) {
  if (!value) return null;
  const v = value.toUpperCase();
  if (v.includes("ARCH") || v.includes("DIMEN") || v.includes("ARCHITECT"))
    return "Architectural Asphalt Shingle";
  if (v.includes("3-TAB") || v.includes("3 TAB") || v.includes("THREE TAB"))
    return "3-Tab Asphalt Shingle";
  if (v.includes("METAL")) return "Metal Corrugated";
  if (v.includes("SLATE")) return "Natural Slate";
  if (v.includes("SHAKE") || v.includes("WOOD")) return "Wood Shake";
  if (v.includes("TPO")) return "TPO Membrane";
  if (v.includes("EPDM")) return "EPDM Membrane";
  if (v.includes("MODIFIED")) return "Modified Bitumen";
  if (v.includes("BUILT-UP")) return "Built-Up Roof";
  if (v.includes("TILE")) return "Clay Tile";
  return null;
}

function mapRoofDesign(value) {
  if (!value) return null;
  const v = value.toUpperCase();
  if (v.includes("GABLE")) return "Gable";
  if (v.includes("HIP")) return "Hip";
  if (v.includes("FLAT")) return "Flat";
  if (v.includes("MANSARD")) return "Mansard";
  if (v.includes("GAMBREL")) return "Gambrel";
  if (v.includes("SHED")) return "Shed";
  return null;
}

function mapPrimaryFraming(value) {
  if (!value) return null;
  const v = value.toUpperCase();
  if (v.includes("WOOD")) return "Wood Frame";
  if (v.includes("STEEL")) return "Steel Frame";
  if (v.includes("BLOCK")) return "Concrete Block";
  if (v.includes("POURED")) return "Poured Concrete";
  if (v.includes("MASONRY")) return "Masonry";
  if (v.includes("ENGINEERED")) return "Engineered Lumber";
  if (v.includes("POST") || v.includes("BEAM")) return "Post and Beam";
  if (v.includes("LOG")) return "Log Construction";
  return null;
}

function buildStructureObject({ raw, areas }) {
  const exteriorWall = raw["EXTERIOR WALL"] || null;
  const floorCover = raw["FLOOR COVER"] || null;
  const foundation = raw["FOUNDATION"] || null;
  const interiorWall = raw["INTERIOR WALL"] || null;
  const stories = raw["NO. STORIES"] || null;
  const roofCover = raw["ROOF COVER"] || null;
  const roofFraming = raw["ROOF FRAMING"] || null;
  const structFrame = raw["STRUCTURAL FRAME"] || null;

  const number_of_stories = stories
    ? parseInt(String(stories).replace(/[^0-9]/g, ""), 10)
    : null;
  const finished_base_area = Number.isInteger(areas["BASE AREA"])
    ? areas["BASE AREA"]
    : null;

  const obj = {
    architectural_style_type: null,
    attachment_type: "Detached",
    ceiling_condition: null,
    ceiling_height_average: null,
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
    exterior_wall_material_primary: mapExteriorWallMaterial(exteriorWall),
    exterior_wall_material_secondary: null,
    finished_base_area,
    finished_basement_area: null,
    finished_upper_story_area: null,
    flooring_condition: null,
    flooring_material_primary: mapFlooringPrimary(floorCover),
    flooring_material_secondary: null,
    foundation_condition: null,
    foundation_material: null,
    foundation_type: mapFoundationType(foundation),
    foundation_waterproofing: null,
    gutters_condition: null,
    gutters_material: null,
    interior_door_material: null,
    interior_wall_condition: null,
    interior_wall_finish_primary: null,
    interior_wall_finish_secondary: null,
    interior_wall_structure_material: "Wood Frame",
    interior_wall_structure_material_primary: "Wood Frame",
    interior_wall_structure_material_secondary: null,
    interior_wall_surface_material_primary:
      interiorWall && interiorWall.toUpperCase().includes("DRYWALL")
        ? "Drywall"
        : interiorWall && interiorWall.toUpperCase().includes("PLASTER")
          ? "Plaster"
          : null,
    interior_wall_surface_material_secondary: null,
    number_of_stories: Number.isInteger(number_of_stories)
      ? number_of_stories
      : null,
    primary_framing_material: mapPrimaryFraming(structFrame) || "Wood Frame",
    roof_age_years: null,
    roof_condition: null,
    roof_covering_material: mapRoofCovering(roofCover),
    roof_design_type: mapRoofDesign(roofFraming),
    roof_material_type:
      (roofCover && roofCover.toUpperCase().includes("SHNG")) ||
      (roofCover && roofCover.toUpperCase().includes("SHINGLE"))
        ? "Shingle"
        : null,
    roof_structure_material: null,
    roof_underlayment_type: null,
    secondary_framing_material: null,
    structural_damage_indicators: null, // was "None Observed"; set to null due to no explicit evidence
    subfloor_material:
      foundation && foundation.toUpperCase().includes("SLAB")
        ? "Concrete Slab"
        : null,
    unfinished_base_area: null,
    unfinished_basement_area: null,
    unfinished_upper_story_area: null,
    window_frame_material: null,
    window_glazing_type: null,
    window_operation_type: null,
    window_screen_material: null,
  };

  return obj;
}

function main() {
  const inputPath = path.resolve("input.html");
  const html = fs.readFileSync(inputPath, "utf8");
  const $ = cheerio.load(html);

  const parcelId = getParcelId($);
  if (!parcelId) {
    throw new Error("Parcel ID not found in input.html");
  }

  const { raw, areas } = parseStructuralElements($);
  const structureObj = buildStructureObject({ raw, areas });

  const output = {};
  output[`property_${parcelId}`] = structureObj;

  ensureDir(path.resolve("data"));
  ensureDir(path.resolve("owners"));

  const outDataPath = path.resolve("data/structure_data.json");
  const outOwnersPath = path.resolve("owners/structure_data.json");

  fs.writeFileSync(outDataPath, JSON.stringify(output, null, 2), "utf8");
  fs.writeFileSync(outOwnersPath, JSON.stringify(output, null, 2), "utf8");

  console.log("Structure mapping complete:", outDataPath, outOwnersPath);
}

if (require.main === module) {
  try {
    main();
  } catch (e) {
    console.error("Error in structureMapping:", e.message);
    process.exit(1);
  }
}
