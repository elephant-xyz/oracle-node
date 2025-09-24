// Utility mapping script
// Reads input.html, extracts utility data using cheerio, writes owners/utilities_data.json

const fs = require("fs");
const path = require("path");
const cheerio = require("cheerio");

function textClean(t) {
  return (t || "").replace(/\s+/g, " ").trim();
}
function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function extract() {
  const html = fs.readFileSync("input.html", "utf8");
  const $ = cheerio.load(html);

  const account = textClean($("#hfAccount").attr("value")) || null;
  const propKey = account ? `property_${account}` : "property_unknown";

  // BCPAO detail page rarely lists explicit utility info. We'll set nulls/defaults.
  const utility = {
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

  const out = {};
  out[propKey] = utility;

  ensureDir("owners");
  fs.writeFileSync(
    path.join("owners", "utilities_data.json"),
    JSON.stringify(out, null, 2),
    "utf8",
  );
}

extract();
