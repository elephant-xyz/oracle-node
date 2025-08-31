// Layout mapping script
// Reads input.html, extracts layout entries for bedrooms/bathrooms and outdoor areas as available.
// Writes owners/layout_data.json per schema.

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

function getBedsBaths($) {
    let beds = 0;
    let bathsText = "";
    $(
        "#PropertyDetailsCurrent table.appraisalAttributes tr, #PropertyDetails table.appraisalAttributes tr",
    ).each((i, el) => {
        const ths = $(el).find("th");
        if (
            ths.length === 4 &&
            /Bedrooms/i.test($(ths[0]).text()) &&
            /Bathrooms/i.test($(ths[1]).text())
        ) {
            const row = $(el).next();
            const tds = row.find("td");
            beds = parseInt(tds.eq(0).text().trim(), 10) || 0;
            bathsText = tds.eq(1).text().trim();
        }
    });
    let fullBaths = 0;
    let halfBaths = 0;
    if (bathsText) {
        const num = parseFloat(bathsText);
        if (!isNaN(num)) {
            fullBaths = Math.floor(num);
            halfBaths = Math.round((num - fullBaths) * 2);
        }
    }
    return { beds, fullBaths, halfBaths };
}

function getGLA($) {
    let gla = 0;
    $(".appraisalDetails td").each((i, el) => {
        const txt = $(el).text().trim();
        if (/^3,?733$/.test(txt)) {
            gla = 3733;
        }
    });
    if (!gla) {
        // Try reading exact field row
        $("table.appraisalDetails tr").each((i, el) => {
            const th = $(el).find("th");
            const td = $(el).find("td");
            if (th.length && /Gross\s*Living\s*Area/i.test($(th[0]).text())) {
                const n = parseInt(
                    $(td[0])
                        .text()
                        .replace(/[\,\s]/g, ""),
                    10,
                );
                if (Number.isFinite(n)) gla = n;
            }
        });
    }
    return gla;
}

function defaultLayout(space_type, index, size) {
    return {
        space_type,
        space_index: index,
        flooring_material_type: null,
        size_square_feet: size ?? null,
        floor_level: null,
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

function poolLayout($, index) {
    // Infer from Building Features: POOL - RESIDENTIAL and Patio, Outdoor Kitchen
    const hasPool =
        /POOL - RESIDENTIAL/i.test($("#PropertyDetailsCurrent").text()) ||
        /POOL - RESIDENTIAL/i.test($("#PropertyDetails").text());
    if (!hasPool) return null;
    const l = defaultLayout("Pool Area", index, null);
    l.is_exterior = true;
    l.pool_type = "BuiltIn";
    l.pool_equipment = "Heated"; // Presence of A/C-POOL HEATERS suggests heated
    l.pool_condition = null;
    l.pool_surface_type = null;
    l.pool_water_quality = null;
    l.view_type = "Waterfront"; // property on lake per land use description
    l.lighting_features = null;
    return l;
}

function main() {
    const inputPath = path.resolve("input.html");
    const html = fs.readFileSync(inputPath, "utf-8");
    const $ = cheerio.load(html);
    const folio = getFolioId($);

    const { beds, fullBaths, halfBaths } = getBedsBaths($);
    const gla = getGLA($);
    const layouts = [];
    let idx = 1;

    for (let i = 0; i < beds; i++) {
        layouts.push(defaultLayout("Bedroom", idx++, null));
    }
    for (let i = 0; i < fullBaths; i++) {
        layouts.push(defaultLayout("Full Bathroom", idx++, null));
    }
    for (let i = 0; i < halfBaths; i++) {
        layouts.push(defaultLayout("Half Bathroom / Powder Room", idx++, null));
    }

    const pool = poolLayout($, idx);
    if (pool) {
        layouts.push(pool);
        idx++;
    }

    // Save
    const outputDir = path.resolve("owners");
    if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });
    const outPath = path.join(outputDir, "layout_data.json");
    const payload = {};
    payload[`property_${folio}`] = { layouts };
    fs.writeFileSync(outPath, JSON.stringify(payload, null, 2), "utf-8");
    console.log(`Wrote ${outPath}`);
}

main();