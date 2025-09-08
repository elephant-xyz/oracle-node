// Layout data extractor using Cheerio
// Reads input.html, identifies rooms/spaces and writes JSON to data/ and owners/

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
    if (/^Parcel ID:/i.test(label)) id = textNorm($tds.eq(1).text());
  });
  return id;
}

function parseAreas($) {
  // Extract area items from the Areas block beneath Buildings
  const areas = {};
  const buildings = $("#ctl00_MasterPlaceHolder_tblBldgs");
  buildings.find("span").each((_, s) => {
    const t = textNorm($(s).text());
    if (/Areas\s*-\s*\d+\s*Total\s*SF/i.test(t)) {
      const container = $(s).parent();
      container.find("b").each((_, b) => {
        const key = textNorm($(b).text()).toUpperCase();
        const val = parseInt(
          textNorm($(b).next("i").text()).replace(/[^0-9]/g, ""),
          10,
        );
        if (!isNaN(val)) areas[key] = val;
      });
    }
  });
  return areas; // e.g., { 'BASE AREA': 912, 'CARPORT FIN': 228, 'UTILITY UNF': 72 }
}

function defaultLayout(space_type, index) {
  return {
    space_type,
    space_index: index,
    flooring_material_type: null,
    size_square_feet: null,
    floor_level: "1st Floor",
    has_windows: null,
    window_design_type: null,
    window_material_type: null,
    window_treatment_type: null,
    is_finished: true,
    furnished: null,
    paint_condition: null,
    flooring_wear: null,
    clutter_level: null,
    visible_damage: null,
    countertop_material: null,
    cabinet_style: null,
    fixture_finish_quality: null,
    design_style: null,
    natural_light_quality: null,
    decor_elements: null,
    pool_type: null,
    pool_equipment: null,
    spa_type: null,
    safety_features: null,
    view_type: null,
    lighting_features: null,
    condition_issues: null,
    is_exterior: false,
    pool_condition: null,
    pool_surface_type: null,
    pool_water_quality: null,
  };
}

function main() {
  const inputPath = path.resolve("input.html");
  const html = fs.readFileSync(inputPath, "utf8");
  const $ = cheerio.load(html);
  const parcelId = getParcelId($);
  if (!parcelId) throw new Error("Parcel ID not found");

  const areas = parseAreas($);

  const layouts = [];
  let idx = 1;

  // Core interior spaces (sizes unknown in source)
  layouts.push({ ...defaultLayout("Living Room", idx++) });
  layouts.push({ ...defaultLayout("Kitchen", idx++) });
  layouts.push({ ...defaultLayout("Bedroom", idx++) });
  layouts.push({ ...defaultLayout("Full Bathroom", idx++) });

  // Map Carport FIN explicitly as exterior Carport
  if (Number.isInteger(areas["CARPORT FIN"])) {
    layouts.push({
      ...defaultLayout("Carport", idx++),
      size_square_feet: areas["CARPORT FIN"],
      is_exterior: true,
      is_finished: false,
      has_windows: false,
    });
  }

  // Map Utility UNF as Utility Closet (unfinished interior service space)
  if (Number.isInteger(areas["UTILITY UNF"])) {
    layouts.push({
      ...defaultLayout("Utility Closet", idx++),
      size_square_feet: areas["UTILITY UNF"],
      is_exterior: false,
      is_finished: false,
    });
  }

  const output = {};
  output[`property_${parcelId}`] = { layouts };

  ensureDir(path.resolve("data"));
  ensureDir(path.resolve("owners"));

  const outDataPath = path.resolve("data/layout_data.json");
  const outOwnersPath = path.resolve("owners/layout_data.json");

  fs.writeFileSync(outDataPath, JSON.stringify(output, null, 2), "utf8");
  fs.writeFileSync(outOwnersPath, JSON.stringify(output, null, 2), "utf8");

  console.log("Layout mapping complete:", outDataPath, outOwnersPath);
}

if (require.main === module) {
  try {
    main();
  } catch (e) {
    console.error("Error in layoutMapping:", e.message);
    process.exit(1);
  }
}
