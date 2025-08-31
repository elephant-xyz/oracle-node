// Utility mapping script
// Reads input.html and extracts utility data per schema, writes owners/utilities_data.json

const fs = require("fs");
const path = require("path");
const cheerio = require("cheerio");

function getFolioId($) {
    let t = $("#parcelLabel").text();
    let m = t.match(/Folio\s*ID:\s*(\d+)/i);
    if (m) return m[1];
    let href = $("a[href*='FolioID=']").first().attr("href") || "";
    m = href.match(/FolioID=(\d+)/i);
    if (m) return m[1];
    return "unknown";
}

function parsePoolEquipment($) {
    // Check building features for pool equipment and A/C pool heaters
    const items = [];
    $(
        "#PropertyDetailsCurrent table.appraisalAttributes tr, #PropertyDetails table.appraisalAttributes tr",
    ).each((i, el) => {
        const tds = cheerio(el).find("td");
        if (tds.length >= 3) {
            const desc = cheerio(tds[0]).text().trim().toUpperCase();
            if (desc.includes("A/C-POOL HEATERS")) items.push("PoolHeater");
            if (desc.includes("OUTDOOR KITCHEN")) items.push("OutdoorKitchen");
            if (desc.includes("OUTDOOR SHOWER")) items.push("OutdoorShower");
            if (desc.includes("XTRA/ADDITIONAL A/C UNITS"))
                items.push("ExtraACUnits");
        }
    });
    return Array.from(new Set(items));
}

function main() {
    const inputPath = path.resolve("input.html");
    const html = fs.readFileSync(inputPath, "utf-8");
    const $ = cheerio.load(html);
    const folio = getFolioId($);

    // Default null/false values; very limited explicit utility info in given HTML
    // We infer public utilities available from presence of county services and garbage service
    const hasGarbage = $("#GarbageDetails").length > 0;

    const utility = {
        cooling_system_type: null, // unknown
        heating_system_type: null,
        public_utility_type: hasGarbage ? "ElectricityAvailable" : null,
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

    // Write output
    const outputDir = path.resolve("owners");
    if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });
    const outPath = path.join(outputDir, "utilities_data.json");
    const payload = {};
    payload[`property_${folio}`] = utility;
    fs.writeFileSync(outPath, JSON.stringify(payload, null, 2), "utf-8");
    console.log(`Wrote ${outPath}`);
}

main();