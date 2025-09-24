// Layout mapping script (revised)
// Reads input.html, extracts layout data using cheerio, writes owners/layout_data.json

const fs = require("fs");
const path = require("path");
const cheerio = require("cheerio");

function textClean(t) {
  return (t || "").replace(/\s+/g, " ").trim();
}
function intFromText(t) {
  if (!t) return null;
  const n = String(t).replace(/[^0-9]/g, "");
  return n ? parseInt(n, 10) : null;
}
function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function extract() {
  const html = fs.readFileSync("input.html", "utf8");
  const $ = cheerio.load(html);

  const account = textClean($("#hfAccount").attr("value")) || null;
  const propKey = account ? `property_${account}` : "property_unknown";

  // Parse sub-areas for separate layout entries where sensible.
  const subAreas = [];
  $("#divBldg_SubAreas table.report-table.left-table tbody tr").each(
    (i, tr) => {
      const tds = $(tr).find("td");
      if (tds.length >= 2) {
        const label = textClean($(tds[0]).text());
        const val = textClean($(tds[1]).text());
        if (!label || /Total/i.test(label)) return;
        const size = intFromText(val);
        subAreas.push({ label, size });
      }
    },
  );

  // Map sub-areas to layout space_types and attributes
  const layouts = [];
  let idx = 1;
  for (const sa of subAreas) {
    const u = sa.label.toUpperCase();
    let space_type = null;
    let is_exterior = false;
    let floor_level = "1st Floor";

    if (u.includes("BASE AREA")) {
      // Treat as Great Room (generic finished area)
      space_type = "Great Room";
      is_exterior = false;
    } else if (u.includes("GARAGE")) {
      space_type = "Attached Garage";
      is_exterior = false; // garages are interior accessory; keep false
    } else if (u.includes("PORCH")) {
      space_type = "Porch";
      is_exterior = true;
    } else {
      // fallback generic interior finished area
      space_type = "Great Room";
      is_exterior = false;
    }

    layouts.push({
      space_type,
      space_index: idx++,
      flooring_material_type: null,
      size_square_feet: sa.size || null,
      floor_level,
      has_windows: null,
      window_design_type: null,
      window_material_type: null,
      window_treatment_type: null,
      is_finished: !is_exterior,
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
      is_exterior,
      pool_condition: null,
      pool_surface_type: null,
      pool_water_quality: null,
    });
  }

  // If no sub-areas found, create a minimal generic living area entry without mislabeling full base area as Living Room.
  if (layouts.length === 0) {
    layouts.push({
      space_type: "Great Room",
      space_index: 1,
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
    });
  }

  const out = {};
  out[propKey] = { layouts };

  ensureDir("owners");
  fs.writeFileSync(
    path.join("owners", "layout_data.json"),
    JSON.stringify(out, null, 2),
    "utf8",
  );
}

extract();
