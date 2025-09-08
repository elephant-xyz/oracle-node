// Utility data extractor using Cheerio
// Reads input.html, extracts utility-related details, and writes JSON to data/ and owners/

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

function parseStructuralElements($) {
  const raw = {};
  const buildingsTable = $("#ctl00_MasterPlaceHolder_tblBldgs");
  buildingsTable.find("b").each((_, b) => {
    const label = textNorm($(b).text());
    const val = textNorm($(b).next("i").text());
    if (label) raw[label.toUpperCase()] = val || null;
  });
  return raw;
}

function main() {
  const inputPath = path.resolve("input.html");
  const html = fs.readFileSync(inputPath, "utf8");
  const $ = cheerio.load(html);

  const parcelId = getParcelId($);
  if (!parcelId) throw new Error("Parcel ID not found");

  // No defensible evidence for utilities beyond presence of structure. Populate required fields conservatively as null/false per schema.
  const utilityObj = {
    cooling_system_type: null,
    heating_system_type: null,
    public_utility_type: null,
    sewer_type: null,
    water_source_type: null,
    plumbing_system_type: null,
    plumbing_system_type_other_description: null,
    electrical_panel_capacity: null,
    electrical_wiring_type: null,
    hvac_condensing_unit_present: null,
    electrical_wiring_type_other_description: null,
    solar_panel_present: false,
    solar_panel_type: null,
    solar_panel_type_other_description: null,
    smart_home_features: null,
    smart_home_features_other_description: null,
    hvac_unit_condition: null,
    solar_inverter_visible: false,
    hvac_unit_issues: null,
  };

  const output = {};
  output[`property_${parcelId}`] = utilityObj;

  ensureDir(path.resolve("data"));
  ensureDir(path.resolve("owners"));

  const outDataPath = path.resolve("data/utilities_data.json");
  const outOwnersPath = path.resolve("owners/utilities_data.json");

  fs.writeFileSync(outDataPath, JSON.stringify(output, null, 2), "utf8");
  fs.writeFileSync(outOwnersPath, JSON.stringify(output, null, 2), "utf8");

  console.log("Utility mapping complete:", outDataPath, outOwnersPath);
}

if (require.main === module) {
  try {
    main();
  } catch (e) {
    console.error("Error in utilityMapping:", e.message);
    process.exit(1);
  }
}
