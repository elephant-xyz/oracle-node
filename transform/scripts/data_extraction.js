// Data extraction script per Elephant Lexicon workflow
// Reads: input.html, unnormalized_address.json, property_seed.json, owners/*.json
// Writes: JSON outputs to ./data/

const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

function ensureDir(p) {
    if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

function readJSON(p) {
    return JSON.parse(fs.readFileSync(p, 'utf-8'));
}

function cleanText(t) {
    return (t || '')
        .replace(/\u00A0/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function parseNumber(str) {
    if (str == null) return null;
    const s = ('' + str).replace(/[$,]/g, '').trim();
    if (s === '' || s === '-') return null;
    const n = Number(s);
    return isNaN(n) ? null : n;
}

function parseDateMMDDYYYY(mmddyyyy) {
    if (!mmddyyyy) return null;
    const parts = mmddyyyy.trim().split('/');
    if (parts.length !== 3) return null;
    const [MM, DD, YYYY] = parts;
    if (!YYYY || !MM || !DD) return null;
    const mm = MM.padStart(2, '0');
    const dd = DD.padStart(2, '0');
    return `${YYYY}-${mm}-${dd}`;
}

function writeJSON(filePath, obj) {
    fs.writeFileSync(filePath, JSON.stringify(obj, null, 2));
}

function extractProperty($) {
    // parcel_identifier (STRAP)
    const parcelLabel = $('#parcelLabel').text();
    let parcelIdentifier = null;
    const strapMatch = parcelLabel.match(/STRAP:\s*([^\s]+)\s*/i);
    if (strapMatch) parcelIdentifier = cleanText(strapMatch[1]);

    // legal description
    let legal = null;
    $('#PropertyDetailsCurrent')
        .find('.sectionSubTitle')
        .each((i, el) => {
            const t = cleanText($(el).text());
            if (/Property Description/i.test(t)) {
                const txt = cleanText($(el).next('.textPanel').text());
                if (txt) legal = txt.replace(/\s+/g, ' ').trim();
            }
        });
    if (!legal) {
        // fallback to the earlier property description section inside the top box
        const section = $('div.sectionSubTitle:contains("Property Description")');
        const txt = cleanText(section.next('.textPanel').text());
        if (txt) legal = txt;
    }
    if (legal) {
        // normalize spacing
        legal = legal.replace(/\s{2,}/g, ' ');
    }

    // Gross Living Area: prefer explicit th contains selector
    let gla = null;
    const glaTh = $('th:contains("Gross Living Area")').first();
    if (glaTh.length) {
        const td = glaTh.closest('tr').find('td').first();
        gla = cleanText(td.text());
    }
    if (!gla) {
        // alternate scan
        $('table.appraisalDetails').each((i, tbl) => {
            $(tbl)
                .find('tr')
                .each((j, tr) => {
                    const th = cleanText($(tr).find('th').first().text());
                    if (/Gross Living Area/i.test(th)) {
                        const td = $(tr).find('td').first();
                        const val = cleanText(td.text());
                        if (val) gla = val;
                    }
                });
        });
    }

    // Year Built (1st Year Building on Tax Roll)
    let yearBuilt = null;
    const ybTh = $('th:contains("1st Year Building on Tax Roll")').first();
    if (ybTh.length) {
        const td = ybTh.closest('tr').find('td').first();
        yearBuilt = parseNumber(td.text());
    }
    if (!yearBuilt) {
        $('table.appraisalAttributes')
            .find('tr')
            .each((i, tr) => {
                const ths = $(tr).find('th');
                if (
                    ths.length === 4 &&
                    /Bedrooms/i.test(cleanText($(ths[0]).text())) &&
                    /Year Built/i.test(cleanText($(ths[2]).text()))
                ) {
                    const next = $(tr).next();
                    const cells = next.find('td');
                    if (cells.length >= 3) {
                        const y = parseNumber($(cells[2]).text());
                        if (y) yearBuilt = y;
                    }
                }
            });
    }

    // Subdivision from legal description prefix
    let subdivision = null;
    if (legal) {
        const m =
            legal.match(/^([^,\n]+?SEC\s*\d+)/i) ||
            legal.match(/^([^\n]+?)(?:\s{2,}|\s+PB\b)/i) ||
            legal.match(/^([^\n]+?)(?:\s{2,}|\s+)/);
        if (m) subdivision = cleanText(m[1]);
    }

    const property = {
        area_under_air: gla || null,
        livable_floor_area: gla || null,
        number_of_units_type: 'One',
        parcel_identifier: parcelIdentifier || null,
        property_legal_description_text: legal || null,
        property_structure_built_year: yearBuilt || null,
        property_type: 'SingleFamily',
        subdivision: subdivision || null,
    };
    return property;
}

function extractAddress($, unAddr) {
    // Site Address block
    const sitePanel = $('div.sectionSubTitle:contains("Site Address")').next(
        '.textPanel'
    );
    const lines = cleanText(sitePanel.html() || '')
        .replace(/<br\s*\/?>(\s*<br\s*\/?>)*/gi, '\n')
        .replace(/<[^>]+>/g, '')
        .split(/\n+/)
        .map((l) => cleanText(l))
        .filter(Boolean);

    let line1 = lines[0] || cleanText(unAddr.full_address || '');
    let cityStateZip = lines[1] || '';

    // Parse street
    let street_number = null,
        street_name = null,
        street_suffix_type = null;
    if (line1) {
        const parts = line1.split(/\s+/);
        street_number = parts.shift() || null;
        // Find suffix as last token
        const suffix = parts[parts.length - 1] || '';
        const suffixMap = {
            RD: 'Rd',
            ROAD: 'Rd',
            ST: 'St',
            STREET: 'St',
            AVE: 'Ave',
            AVENUE: 'Ave',
            BLVD: 'Blvd',
            DR: 'Dr',
            DRIVE: 'Dr',
            LN: 'Ln',
            LANE: 'Ln',
            CT: 'Ct',
            COURT: 'Ct',
            TER: 'Ter',
            TERRACE: 'Ter',
            HWY: 'Hwy',
            WAY: 'Way',
            PKWY: 'Pkwy',
            PL: 'Pl',
            LOOP: 'Loop',
            CIR: 'Cir',
            RDG: 'Rdg',
            TRAIL: 'Trl',
            TRL: 'Trl',
            XING: 'Xing',
            RTE: 'Rte',
            'RD.': 'Rd',
        };
        const suf = suffixMap[suffix.toUpperCase()] || null;
        if (suf) {
            street_suffix_type = suf;
            parts.pop();
        }
        street_name = parts.join(' ').trim() || null;
    }

    // Parse city, state, zip
    let city_name = null,
        state_code = null,
        postal_code = null;
    if (cityStateZip) {
        const m = cityStateZip.match(/^(.*)\s+([A-Z]{2})\s+(\d{5})(?:-\d{4})?$/i);
        if (m) {
            city_name = (m[1] || '').toUpperCase();
            state_code = m[2].toUpperCase();
            postal_code = m[3];
        }
    }
    if (!city_name && unAddr && unAddr.full_address) {
        const mm = unAddr.full_address.match(/,\s*([^,]+),\s*([A-Z]{2})\s+(\d{5})/);
        if (mm) {
            city_name = (mm[1] || '').toUpperCase();
            state_code = mm[2];
            postal_code = mm[3];
        }
    }

    // Township/Range/Section/Block and lat/long
    let township = null,
        range = null,
        section = null,
        block = null,
        latitude = null,
        longitude = null;
    $('table.appraisalDetailsLocation')
        .find('tr')
        .each((i, tr) => {
            const headers = $(tr).find('th');
            if (
                headers.length === 5 &&
                /Township/i.test(cleanText($(headers[0]).text()))
            ) {
                const next = $(tr).next();
                const cells = next.find('td');
                if (cells.length >= 5) {
                    township = cleanText($(cells[0]).text()) || null;
                    range = cleanText($(cells[1]).text()) || null;
                    section = cleanText($(cells[2]).text()) || null;
                    block = cleanText($(cells[3]).text()) || null;
                }
            }
            if (
                headers.length >= 3 &&
                /Municipality/i.test(cleanText($(headers[0]).text()))
            ) {
                const next = $(tr).next();
                const cells = next.find('td');
                if (cells.length >= 3) {
                    latitude = parseNumber($(cells[1]).text());
                    longitude = parseNumber($(cells[2]).text());
                }
            }
        });

    const address = {
        block: block || null,
        city_name: city_name || null,
        country_code: null,
        county_name:
            unAddr && unAddr.county_jurisdiction ? unAddr.county_jurisdiction : null,
        latitude: latitude != null ? latitude : null,
        longitude: longitude != null ? longitude : null,
        plus_four_postal_code: null,
        postal_code: postal_code || null,
        range: range || null,
        route_number: null,
        section: section || null,
        state_code: state_code || null,
        street_name: street_name || null,
        street_number: street_number || null,
        street_post_directional_text: null,
        street_pre_directional_text: null,
        street_suffix_type: street_suffix_type || null,
        unit_identifier: null,
        township: township || null,
    };
    return address;
}

function extractTaxes($) {
    const taxes = [];
    const grid = $('#valueGrid');
    if (!grid.length) return taxes;
    grid.find('tr').each((i, tr) => {
        if (i === 0) return; // header
        const tds = $(tr).find('td');
        if (tds.length < 9) return;
        const yearText = cleanText($(tds[1]).text());
        const yearMatch = yearText.match(/(\d{4})/);
        if (!yearMatch) return;
        const tax_year = parseInt(yearMatch[1], 10);
        const just = parseNumber($(tds[2]).text());
        const land = parseNumber($(tds[3]).text());
        const market_assessed = parseNumber($(tds[4]).text());
        const capped_assessed = parseNumber($(tds[5]).text());
        const taxable = parseNumber($(tds[8]).text());

        const buildingVal =
            market_assessed != null && land != null ? market_assessed - land : null;
        const obj = {
            tax_year: tax_year || null,
            property_assessed_value_amount:
                capped_assessed != null ? capped_assessed : null,
            property_market_value_amount: just != null ? just : null,
            property_building_amount:
                buildingVal != null && buildingVal > 0
                    ? Number(buildingVal.toFixed(2))
                    : null,
            property_land_amount: land != null ? land : null,
            property_taxable_value_amount: taxable != null ? taxable : null,
            monthly_tax_amount: null,
            period_end_date: null,
            period_start_date: null,
        };
        taxes.push(obj);
    });
    return taxes;
}

function extractSales($) {
    const out = [];
    const salesBox = $('#SalesDetails');
    const table = salesBox.find('table.detailsTable').first();
    if (!table.length) return out;
    const rows = table.find('tr');
    rows.each((i, tr) => {
        if (i === 0) return; // header
        const tds = $(tr).find('td');
        if (!tds.length) return;
        const price = parseNumber($(tds[0]).text());
        const dateText = cleanText($(tds[1]).text());
        const dateISO = parseDateMMDDYYYY(dateText);
        if (price == null && !dateISO) return;
        out.push({
            purchase_price_amount: price != null ? price : null,
            ownership_transfer_date: dateISO || null,
        });
    });
    return out;
}

function extractFlood($) {
    let community_id = null,
        panel_number = null,
        map_version = null,
        effective_date = null,
        evacuation_zone = null,
        fema_search_url = null;
    const elev = $('#ElevationDetails');
    const table = elev.find('table.detailsTable');
    if (table.length) {
        const rows = table.find('tr');
        rows.each((i, tr) => {
            const tds = $(tr).find('td');
            if (tds.length === 5) {
                community_id = cleanText($(tds[0]).text()) || null;
                panel_number = cleanText($(tds[1]).text()) || null;
                map_version = cleanText($(tds[2]).text()) || null;
                effective_date = parseDateMMDDYYYY(cleanText($(tds[3]).text())) || null;
                evacuation_zone = cleanText($(tds[4]).text()) || null;
            }
        });
    }
    const link = elev.find('a[href*="msc.fema.gov/portal/search"]');
    if (link.length) {
        fema_search_url = link.attr('href');
        if (fema_search_url && !/^https?:/i.test(fema_search_url)) {
            fema_search_url = 'https://msc.fema.gov' + fema_search_url;
        }
        fema_search_url = encodeURI(fema_search_url);
    }
    return {
        community_id: community_id || null,
        panel_number: panel_number || null,
        map_version: map_version || null,
        effective_date: effective_date || null,
        evacuation_zone: evacuation_zone || null,
        flood_zone: null,
        flood_insurance_required: false,
        fema_search_url: fema_search_url || null,
    };
}

function extractStructure($) {
    // Architectural style
    let architectural_style_type = null;
    $('table.appraisalAttributes')
        .find('tr')
        .each((i, tr) => {
            const ths = $(tr).find('th');
            if (
                ths.length === 4 &&
                /Improvement Type/i.test(cleanText($(ths[0]).text()))
            ) {
                const impType = cleanText($(tr).next().find('td').first().text());
                if (/Ranch/i.test(impType)) architectural_style_type = 'Ranch';
            }
        });

    // Subareas: get BASE and FINISHED UPPER STORY
    let finished_base_area = null;
    let finished_upper_story_area = null;
    $('table.appraisalAttributes')
        .find('tr')
        .each((i, tr) => {
            const tds = $(tr).find('td');
            if (tds.length === 4) {
                const desc = cleanText($(tds[0]).text());
                const heated = cleanText($(tds[2]).text());
                const area = parseNumber($(tds[3]).text());
                if (/BAS\s*-\s*BASE/i.test(desc) && /^Y$/i.test(heated)) {
                    finished_base_area =
                        area != null ? Math.round(area) : finished_base_area;
                }
                if (
                    /FUS\s*-\s*FINISHED UPPER STORY/i.test(desc) &&
                    /^Y$/i.test(heated)
                ) {
                    finished_upper_story_area =
                        area != null ? Math.round(area) : finished_upper_story_area;
                }
            }
        });

    const structure = {
        architectural_style_type: architectural_style_type || null,
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
    };

    if (finished_base_area != null)
        structure.finished_base_area = finished_base_area;
    if (finished_upper_story_area != null)
        structure.finished_upper_story_area = finished_upper_story_area;

    return structure;
}

function extractLot($) {
    // No reliable lot dimensions/materials in HTML; return null fields per schema allowances
    return {
        lot_type: null,
        lot_length_feet: null,
        lot_width_feet: null,
        lot_area_sqft: null,
        landscaping_features: null,
        view: null,
        fencing_type: null,
        fence_height: null,
        fence_length: null,
        driveway_material: null,
        driveway_condition: null,
        lot_condition_issues: null,
    };
}

function main() {
    const dataDir = path.join('data');
    ensureDir(dataDir);

    const html = fs.readFileSync('input.html', 'utf-8');
    const $ = cheerio.load(html);
    const unAddr = fs.existsSync('unnormalized_address.json')
        ? readJSON('unnormalized_address.json')
        : null;

    // Property
    const property = extractProperty($);
    writeJSON(path.join(dataDir, 'property.json'), property);

    // Address
    const address = extractAddress($, unAddr);
    writeJSON(path.join(dataDir, 'address.json'), address);

    // Taxes
    const taxes = extractTaxes($);
    taxes.forEach((t, idx) => {
        const year = t.tax_year || `idx_${idx + 1}`;
        writeJSON(path.join(dataDir, `tax_${year}.json`), t);
    });

    // Sales
    const salesList = extractSales($);
    salesList.forEach((s, idx) => {
        writeJSON(path.join(dataDir, `sales_${idx + 1}.json`), s);
    });

    // Flood
    const flood = extractFlood($);
    writeJSON(path.join(dataDir, 'flood_storm_information.json'), flood);

    // Utilities from owners/utilities_data.json
    if (fs.existsSync(path.join('owners', 'utilities_data.json'))) {
        const utilitiesData = readJSON(path.join('owners', 'utilities_data.json'));
        // Key: property_*
        let util = null;
        const key = Object.keys(utilitiesData).find((k) => /property_/.test(k));
        if (key) util = utilitiesData[key];
        if (util) {
            writeJSON(path.join(dataDir, 'utility.json'), {
                cooling_system_type: util.cooling_system_type ?? null,
                heating_system_type: util.heating_system_type ?? null,
                public_utility_type: util.public_utility_type ?? null,
                sewer_type: util.sewer_type ?? null,
                water_source_type: util.water_source_type ?? null,
                plumbing_system_type: util.plumbing_system_type ?? null,
                plumbing_system_type_other_description:
                    util.plumbing_system_type_other_description ?? null,
                electrical_panel_capacity: util.electrical_panel_capacity ?? null,
                electrical_wiring_type: util.electrical_wiring_type ?? null,
                hvac_condensing_unit_present: util.hvac_condensing_unit_present ?? null,
                electrical_wiring_type_other_description:
                    util.electrical_wiring_type_other_description ?? null,
                solar_panel_present: !!util.solar_panel_present,
                solar_panel_type: util.solar_panel_type ?? null,
                solar_panel_type_other_description:
                    util.solar_panel_type_other_description ?? null,
                smart_home_features: util.smart_home_features ?? null,
                smart_home_features_other_description:
                    util.smart_home_features_other_description ?? null,
                hvac_unit_condition: util.hvac_unit_condition ?? null,
                solar_inverter_visible: !!util.solar_inverter_visible,
                hvac_unit_issues: util.hvac_unit_issues ?? null,
            });
        }
    }

    // Layouts from owners/layout_data.json
    if (fs.existsSync(path.join('owners', 'layout_data.json'))) {
        const layoutData = readJSON(path.join('owners', 'layout_data.json'));
        const key = Object.keys(layoutData).find((k) => /property_/.test(k));
        if (key && layoutData[key] && Array.isArray(layoutData[key].layouts)) {
            layoutData[key].layouts.forEach((lay, idx) => {
                const out = {
                    space_type: lay.space_type ?? null,
                    space_index: lay.space_index ?? null,
                    flooring_material_type: lay.flooring_material_type ?? null,
                    size_square_feet: lay.size_square_feet ?? null,
                    floor_level: lay.floor_level ?? null,
                    has_windows: lay.has_windows ?? null,
                    window_design_type: lay.window_design_type ?? null,
                    window_material_type: lay.window_material_type ?? null,
                    window_treatment_type: lay.window_treatment_type ?? null,
                    is_finished: lay.is_finished ?? null,
                    furnished: lay.furnished ?? null,
                    paint_condition: lay.paint_condition ?? null,
                    flooring_wear: lay.flooring_wear ?? null,
                    clutter_level: lay.clutter_level ?? null,
                    visible_damage: lay.visible_damage ?? null,
                    countertop_material: lay.countertop_material ?? null,
                    cabinet_style: lay.cabinet_style ?? null,
                    fixture_finish_quality: lay.fixture_finish_quality ?? null,
                    design_style: lay.design_style ?? null,
                    natural_light_quality: lay.natural_light_quality ?? null,
                    decor_elements: lay.decor_elements ?? null,
                    pool_type: lay.pool_type ?? null,
                    pool_equipment: lay.pool_equipment ?? null,
                    spa_type: lay.spa_type ?? null,
                    safety_features: lay.safety_features ?? null,
                    view_type: lay.view_type ?? null,
                    lighting_features: lay.lighting_features ?? null,
                    condition_issues: lay.condition_issues ?? null,
                    is_exterior: lay.is_exterior ?? false,
                    pool_condition: lay.pool_condition ?? null,
                    pool_surface_type: lay.pool_surface_type ?? null,
                    pool_water_quality: lay.pool_water_quality ?? null,
                };
                writeJSON(path.join(dataDir, `layout_${idx + 1}.json`), out);
            });
        }
    }

    // Owners from owners/owner_data.json (single type only). Prefer company.
    let salesFiles = [];
    const dataFiles = fs.readdirSync(dataDir);
    salesFiles = dataFiles
        .filter((f) => /^sales_\d+\.json$/.test(f))
        .sort((a, b) => {
            const ai = parseInt(a.match(/(\d+)/)[1], 10);
            const bi = parseInt(b.match(/(\d+)/)[1], 10);
            return ai - bi;
        });

    if (fs.existsSync(path.join('owners', 'owner_data.json'))) {
        const ownerData = readJSON(path.join('owners', 'owner_data.json'));
        const key = Object.keys(ownerData).find((k) => /property_/.test(k));
        let currentOwners = [];
        if (
            key &&
            ownerData[key] &&
            ownerData[key].owners_by_date &&
            ownerData[key].owners_by_date.current
        ) {
            currentOwners = ownerData[key].owners_by_date.current;
        }
        const companies = currentOwners.filter((o) => o.type === 'company');
        const persons = currentOwners.filter((o) => o.type === 'person');

        if (companies.length > 0) {
            const c = companies[0];
            writeJSON(path.join(dataDir, 'company_1.json'), { name: c.name ?? null });
        } else if (persons.length > 0) {
            const p = persons[0];
            writeJSON(path.join(dataDir, 'person_1.json'), {
                birth_date: null,
                first_name: p.first_name || null,
                last_name: p.last_name || null,
                middle_name: p.middle_name ?? null,
                prefix_name: null,
                suffix_name: null,
                us_citizenship_status: null,
                veteran_status: null,
            });
        }

        // Relationships: link to most recent sale (first parsed sale row is typically most recent)
        if (salesFiles.length > 0) {
            if (fs.existsSync(path.join(dataDir, 'company_1.json'))) {
                writeJSON(path.join(dataDir, 'relationship_sales_company.json'), {
                    to: { '/': './company_1.json' },
                    from: { '/': './sales_1.json' },
                });
            } else if (fs.existsSync(path.join(dataDir, 'person_1.json'))) {
                writeJSON(path.join(dataDir, 'relationship_sales_person.json'), {
                    to: { '/': './person_1.json' },
                    from: { '/': './sales_1.json' },
                });
            }
        }
    }

    // Structure
    const structure = extractStructure($);
    writeJSON(path.join(dataDir, 'structure.json'), structure);

    // Lot
    const lot = extractLot($);
    writeJSON(path.join(dataDir, 'lot.json'), lot);
}

if (require.main === module) {
    main();
}