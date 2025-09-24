// Structure mapping script
// Reads input.html, extracts structural info with cheerio, writes owners/structure_data.json

const fs = require("fs");
const path = require("path");
const cheerio = require("cheerio");

function safeInt(val) {
  if (val == null) return undefined;
  const n = parseInt(String(val).replace(/[,\s]/g, ""), 10);
  return Number.isFinite(n) ? n : undefined;
}

function extractText($, selector) {
  const t = $(selector).first().text();
  return t ? t.trim() : "";
}

function getFolioId($) {
  // Try primary label
  let t = $("#parcelLabel").text();
  let m = t.match(/Folio\s*ID:\s*(\d+)/i);
  if (m) return m[1];
  // Try links with folioid query
  let href = $("a[href*='FolioID=']").first().attr("href") || "";
  m = href.match(/FolioID=(\d+)/i);
  if (m) return m[1];
  return "unknown";
}

function detectStyle($) {
  // Look for Improvement Type row text e.g., "102 - Ranch"
  let styleText = "";
  $("table.appraisalAttributes tr").each((i, el) => {
    const ths = $(el).find("th");
    const tds = $(el).find("td");
    if (ths.length === 4 && /Improvement Type/i.test($(ths[0]).text())) {
      styleText = $(el).next().find("td").eq(0).text().trim();
    }
  });
  if (/ranch/i.test(styleText)) return "Ranch";
  // Fallback null
  return null;
}

function detectAttachment($) {
  // Model Type often includes SINGLE FAMILY RESIDENTIAL
  let modelType = "";
  $("table.appraisalAttributes tr").each((i, el) => {
    const ths = $(el).find("th");
    if (ths.length === 4 && /Stories/i.test($(ths[2]).text())) {
      const row = $(el).next();
      modelType = row.find("td").eq(1).text().trim();
    }
  });
  if (/single\s*family/i.test(modelType)) return "Detached";
  return null;
}

function extractAreas($) {
  let finished_base_area;
  let finished_upper_story_area;
  $("table.appraisalAttributes tr").each((i, el) => {
    const tds = $(el).find("td");
    if (tds.length >= 4) {
      const desc = $(tds[0]).text().trim();
      const heated = $(tds[2]).text().trim();
      const areaTxt = $(tds[3]).text().trim();
      const area = safeInt(areaTxt);
      if (/^BAS\s*-\s*BASE/i.test(desc) && /^Y/i.test(heated)) {
        finished_base_area = area;
      }
      if (
        /^FUS\s*-\s*FINISHED\s*UPPER\s*STORY/i.test(desc) &&
        /^Y/i.test(heated)
      ) {
        finished_upper_story_area = area;
      }
    }
  });
  return { finished_base_area, finished_upper_story_area };
}

function extractGeneratedYear($) {
  const genTxt = $(".generatedOnExpander, .generatedOn").first().text();
  const m = genTxt.match(/\b(20\d{2})\b/);
  return m ? parseInt(m[1], 10) : undefined;
}

function extractRoofYearFromPermits($) {
  // Look in Permit Details table for a row where Permit Type contains 'Roof'
  let year;
  $("#PermitDetails table.detailsTable tr").each((i, el) => {
    const tds = $(el).find("td");
    if (tds.length >= 3) {
      const type = $(tds[1]).text().trim();
      const date = $(tds[2]).text().trim();
      if (/\broof\b/i.test(type)) {
        const m = date.match(/(\d{4})/);
        if (m) {
          year = parseInt(m[1], 10);
          return false; // break
        }
      }
    }
  });
  return year;
}

function computeRoofAgeYears(genYear, roofYear) {
  if (!genYear || !roofYear) return null;
  const diff = genYear - roofYear;
  if (diff < 1) return 1; // schema minimum 1
  return diff;
}

function main() {
  const inputPath = path.resolve("input.html");
  const html = fs.readFileSync(inputPath, "utf-8");
  const $ = cheerio.load(html);

  const folio = getFolioId($);
  const style = detectStyle($);
  const attachment = detectAttachment($);
  const areas = extractAreas($);
  const genYear = extractGeneratedYear($);
  const roofPermitYear = extractRoofYearFromPermits($);
  const roofAgeYears = computeRoofAgeYears(genYear, roofPermitYear);

  // Default null for most unknowns per schema enums
  const structure = {
    // required in schema but instruction says do not include source fields; omit them.
    architectural_style_type: style, // enum or null
    attachment_type: attachment, // enum or null
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
    roof_age_years: roofAgeYears ?? null,
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
    structural_damage_indicators: "None Observed",
  };

  // Optional area fields
  if (areas.finished_base_area != null)
    structure.finished_base_area = areas.finished_base_area;
  if (areas.finished_upper_story_area != null)
    structure.finished_upper_story_area = areas.finished_upper_story_area;

  const outputDir = path.resolve("owners");
  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });
  const outPath = path.join(outputDir, "structure_data.json");

  const payload = {};
  payload[`property_${folio}`] = structure;

  fs.writeFileSync(outPath, JSON.stringify(payload, null, 2), "utf-8");
  console.log(`Wrote ${outPath}`);
}

main();
