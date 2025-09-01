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

  // ENHANCED PROPERTY TYPE EXTRACTION
  let livingUnits = null;
  let modelType = null;
  let rawModelType = null;

  // Extract living units and model type from building characteristics
  $('table.appraisalAttributes').each((_, table) => {
    const rows = $(table).find('tr');
    rows.each((i, row) => {
      const cells = $(row).find('td, th');
      if (cells.length >= 2) {
        const header = cleanText($(cells[0]).text()).toLowerCase();
        if (header.includes('living units')) {
          if (i + 1 < rows.length) {
            const dataRow = $(rows[i + 1]);
            const dataCells = dataRow.find('td, th');
            if (dataCells.length >= 4) {
              try {
                livingUnits = parseInt(cleanText($(dataCells[3]).text()));
              } catch (e) {}
            }
          }
        } else if (header.includes('model type')) {
          if (i + 1 < rows.length) {
            const dataRow = $(rows[i + 1]);
            const dataCells = dataRow.find('td, th');
            if (dataCells.length >= 2) {
              rawModelType = cleanText($(dataCells[1]).text());
              modelType = rawModelType.toLowerCase();
            }
          }
        }
      }
    });
  });

  // Extract Use Code Description from Land Tracts table as fallback
  let useCodeDescription = null;
  let rawUseCodeDescription = null;

  const sections = $('#PropertyDetailsCurrent, #PropertyDetails');
  sections.each((_, section) => {
    $(section).find('table.appraisalAttributes').each((_, table) => {
      $(table).find('tr').each((_, row) => {
        const cells = $(row).find('td, th');
        if (cells.length >= 1 && cleanText($(cells[0]).text()).toLowerCase().includes('land tracts')) {
          let headerRow = null;
          const dataRows = [];

          let currentRow = $(row).next();
          while (currentRow.length) {
            const rowCells = currentRow.find('td, th');
            if (!rowCells.length) break;

            if (rowCells.toArray().some(cell => cleanText($(cell).text()).toLowerCase().includes('use code description'))) {
              headerRow = currentRow;
            } else if (headerRow && rowCells.length) {
              dataRows.push(currentRow);
            } else if (dataRows.length > 0 && !rowCells.toArray().some(cell => cleanText($(cell).text()))) {
              break;
            }

            currentRow = currentRow.next();
          }

          if (headerRow && dataRows.length) {
            const headerCells = headerRow.find('td, th');
            const headers = headerCells.toArray().map(cell => cleanText($(cell).text()).toLowerCase());

            let descColIndex = null;
            headers.forEach((header, i) => {
              if (header.includes('use code description')) {
                descColIndex = i;
              }
            });

            if (descColIndex !== null && dataRows.length) {
              const firstDataRow = dataRows[0];
              const dataCells = firstDataRow.find('td, th');
              if (descColIndex < dataCells.length) {
                rawUseCodeDescription = cleanText($(dataCells[descColIndex]).text());
                useCodeDescription = rawUseCodeDescription.toLowerCase();

                console.log(`Use Code Description: "${rawUseCodeDescription}"`);
                console.log(`Use Code Description (lowercase): "${useCodeDescription}"`);

                return false;
              }
            }
          }
        }
      });
    });
  });

  // Map living units to number_of_units_type
  let numberOfUnitsType = 'One'; // default
  if (livingUnits) {
    if (livingUnits === 1) numberOfUnitsType = 'One';
    else if (livingUnits === 2) numberOfUnitsType = 'Two';
    else if (livingUnits === 3) numberOfUnitsType = 'Three';
    else if (livingUnits === 4) numberOfUnitsType = 'Four';
  }

  // Property type mapping function
function tryMapPropertyType(typeText, rawValue) {
  if (!typeText) return [null, null];

  let matched = null;
  const lowerText = typeText.toLowerCase();

  // Non-residential property codes - return raw value since they don't map to residential schema
  if (lowerText.includes('commercial, vacant') ||
      lowerText.includes('commercial, acreage') ||
      lowerText.includes('commercial, highway') ||
      lowerText.includes('professional, vacant') ||
      lowerText.includes('store, one') ||
      lowerText.includes('store, office, residential combinations') ||
      lowerText.includes('department store') ||
      lowerText.includes('supermarket') ||
      lowerText.includes('convenience store') ||
      lowerText.includes('shopping center') ||
      lowerText.includes('office building') ||
      lowerText.includes('manufacturing offices') ||
      lowerText.includes('professional building') ||
      lowerText.includes('medical office building') ||
      lowerText.includes('airport') ||
      lowerText.includes('marina') ||
      lowerText.includes('boat units') ||
      lowerText.includes('aircraft hangar') ||
      lowerText.includes('bus terminal') ||
      lowerText.includes('restaurant') ||
      lowerText.includes('financial institution') ||
      lowerText.includes('insurance company') ||
      lowerText.includes('service shop') ||
      lowerText.includes('laundry') ||
      lowerText.includes('laundromat') ||
      lowerText.includes('service station') ||
      lowerText.includes('vehicle lube/wash') ||
      lowerText.includes('auto sales') ||
      lowerText.includes('garage, repair') ||
      lowerText.includes('parking lot') ||
      lowerText.includes('trailer park sales') ||
      lowerText.includes('recreational vehicle park sales') ||
      lowerText.includes('wholesaler') ||
      lowerText.includes('produce house') ||
      lowerText.includes('florist') ||
      lowerText.includes('drive-in theatre') ||
      lowerText.includes('theatre') ||
      lowerText.includes('auditoriums') ||
      lowerText.includes('night club') ||
      lowerText.includes('bar, lounge') ||
      lowerText.includes('bowling alley') ||
      lowerText.includes('skating') ||
      lowerText.includes('hockey') ||
      lowerText.includes('ice rink') ||
      lowerText.includes('tourist attraction') ||
      lowerText.includes('camps') ||
      lowerText.includes('race track') ||
      lowerText.includes('golf course') ||
      lowerText.includes('motel') ||
      lowerText.includes('hotel') ||
      // Industrial codes
      lowerText.includes('industrial, vacant') ||
      lowerText.includes('light manufacturing') ||
      lowerText.includes('heavy manufacturing') ||
      lowerText.includes('exceptional industrial') ||
      lowerText.includes('lumber yard') ||
      lowerText.includes('packing plant') ||
      lowerText.includes('bottler') ||
      lowerText.includes('food processing') ||
      lowerText.includes('mineral processing') ||
      lowerText.includes('warehousing') ||
      lowerText.includes('open storage') ||
      // Agricultural codes
      lowerText.includes('field crop') ||
      lowerText.includes('vegetables') ||
      lowerText.includes('potatoes') ||
      lowerText.includes('miscellaneous ag land') ||
      lowerText.includes('sod') ||
      lowerText.includes('timber') ||
      lowerText.includes('pasture') ||
      lowerText.includes('grove') ||
      lowerText.includes('grapes') ||
      lowerText.includes('citrus nursery') ||
      lowerText.includes('bees') ||
      lowerText.includes('miscellaneous fowl') ||
      lowerText.includes('fish') ||
      lowerText.includes('horses') ||
      lowerText.includes('swine') ||
      lowerText.includes('goats') ||
      lowerText.includes('nursery, above ground') ||
      lowerText.includes('nursery, in ground') ||
      lowerText.includes('nursery, waste') ||
      lowerText.includes('aquaculture') ||
      // Institutional codes
      lowerText.includes('vacant institutional') ||
      lowerText.includes('church') ||
      lowerText.includes('school, private') ||
      lowerText.includes('day care centers') ||
      lowerText.includes('dormitory') ||
      lowerText.includes('hospital, private') ||
      lowerText.includes('nursing home') ||
      lowerText.includes('home for the aged') ||
      lowerText.includes('orphanage') ||
      lowerText.includes('mortuary') ||
      lowerText.includes('funeral home') ||
      lowerText.includes('cemetery') ||
      lowerText.includes('lodges') ||
      lowerText.includes('clubs') ||
      lowerText.includes('union halls') ||
      lowerText.includes('yachting clubs') ||
      lowerText.includes('boating associations') ||
      lowerText.includes('country clubs') ||
      lowerText.includes('sanitariums') ||
      lowerText.includes('cultural facilities') ||
      lowerText.includes('performing arts halls') ||
      // Government codes
      lowerText.includes('vacant governmental') ||
      lowerText.includes('military facility') ||
      lowerText.includes('government owned') ||
      lowerText.includes('county owned') ||
      lowerText.includes('state owned') ||
      lowerText.includes('federally owned') ||
      lowerText.includes('municipally owned') ||
      lowerText.includes('government owned') ||
      // Miscellaneous codes
      lowerText.includes('lease interest') ||
      lowerText.includes('no land interest') ||
      lowerText.includes('utilities') ||
      lowerText.includes('waterworks') ||
      lowerText.includes('mining') ||
      lowerText.includes('petroleum') ||
      lowerText.includes('phosphate') ||
      lowerText.includes('boat slips') ||
      lowerText.includes('right of way') ||
      lowerText.includes('submerged') ||
      lowerText.includes('low lot') ||
      lowerText.includes('lake') ||
      lowerText.includes('pond') ||
      lowerText.includes('bay bottom') ||
      lowerText.includes('borrow pit') ||
      lowerText.includes('waste land') ||
      lowerText.includes('sewer disp') ||
      lowerText.includes('solid waste') ||
      lowerText.includes('historical, privately owned') ||
      lowerText.includes('slough') ||
      lowerText.includes('indian mound') ||
      lowerText.includes('historical preserve') ||
      lowerText.includes('marsh lands') ||
      lowerText.includes('island') ||
      lowerText.includes('swamp') ||
      lowerText.includes('spoils easements') ||
      lowerText.includes('endangered species') ||
      lowerText.includes('eagles nests') ||
      lowerText.includes('mangrove') ||
      lowerText.includes('unbuildable') ||
      lowerText.includes('resource protect') ||
      lowerText.includes('wetlands') ||
      lowerText.includes('preserve') ||
      lowerText.includes('cypress head') ||
      lowerText.includes('hazardous waste sites') ||
      lowerText.includes('mineral rights') ||
      lowerText.includes('parks, privately owned') ||
      lowerText.includes('boat ramps') ||
      lowerText.includes('recreational areas') ||
      lowerText.includes('centrally assessed') ||
      lowerText.includes('acreage, non-agricultural') ||
      lowerText.includes('market value agricultural') ||
      lowerText.includes('market value conservation') ||
      lowerText.includes('acreage, exempt') ||
      lowerText.includes('acreage, buffer') ||
      lowerText.includes('conservation easement') ||
      lowerText.includes('acreage, rural') ||
      lowerText.includes('acreage, raw') ||
      lowerText.includes('acreage, beach front') ||
      lowerText.includes('acreage, highway')) {
    return [null, rawValue]; // Non-residential properties don't fit residential schema
  }

  // RESIDENTIAL Lee County Use Code Description mappings
  if (lowerText.includes('vacant residential')) {
    matched = 'VacantLand';
  }
  // Single Family Residential variations
  else if (lowerText.includes('single family residential')) {
    matched = 'SingleFamily';
  }
  // Mobile Home variations
  else if (lowerText.includes('mobile home subdivision') ||
           lowerText.includes('mobile home, elevated') ||
           lowerText.includes('mobile home park') ||
           lowerText.includes('mobile home, single family') ||
           lowerText.includes('mobile home, acreage') ||
           lowerText.includes('mobile home, waterfront') ||
           lowerText.includes('mobile home, canal')) {
    matched = 'MobileHome';
  }
  // RV and Mobile Home Condos
  else if (lowerText.includes('recreational vehicle park')) {
    matched = 'ManufacturedHousing';
  }
  else if (lowerText.includes('mobile home and rv condominiums')) {
    matched = 'Condominium';
  }
  // Multi-family 10+ units
  else if (lowerText.includes('multi-family, 10 or more units')) {
    matched = 'MultipleFamily';
  }
  // Multi-family less than 10 units (including waterfront variations)
  else if (lowerText.includes('multi-family, less than 10 units') ||
           lowerText.includes('multi family, less than 10 units') ||
           lowerText.includes('apartments')) {
    matched = 'TwoToFourFamily';
  }
  // Condominium variations
  else if (lowerText.includes('land condo') ||
           lowerText.includes('condominium reserve parcel')) {
    matched = 'Condominium';
  }
  // Interval Ownership/Time Share
  else if (lowerText.includes('interval ownership') ||
           lowerText.includes('interval ownership/time share')) {
    matched = 'Timeshare';
  }
  // Co-operative
  else if (lowerText.includes('co-operative')) {
    matched = 'Cooperative';
  }
  // Retirement Home
  else if (lowerText.includes('retirement home')) {
    matched = 'Retirement';
  }
  // Miscellaneous Residential
  else if (lowerText.includes('misc res') ||
           lowerText.includes('migrant camp') ||
           lowerText.includes('boarding house')) {
    matched = 'MiscellaneousResidential';
  }
  // General keyword matching (fallback for non-Lee County data)
  else if (['single family', 'single-family'].some(keyword => lowerText.includes(keyword))) {
    matched = 'SingleFamily';
  } else if (lowerText.includes('duplex') || lowerText.includes('2 unit') || lowerText.includes('two unit')) {
    matched = '2Units';
  } else if (lowerText.includes('triplex') || lowerText.includes('3 unit') || lowerText.includes('three unit')) {
    matched = '3Units';
  } else if (lowerText.includes('fourplex') || lowerText.includes('4 unit') || lowerText.includes('four unit')) {
    matched = '4Units';
  } else if (['townhouse', 'town house', 'townhome'].some(keyword => lowerText.includes(keyword))) {
    matched = 'Townhouse';
  } else if (lowerText.includes('condominium') || lowerText.includes('condo')) {
    if (lowerText.includes('detached')) {
      matched = 'DetachedCondominium';
    } else if (lowerText.includes('non warrantable') || lowerText.includes('nonwarrantable')) {
      matched = 'NonWarrantableCondo';
    } else {
      matched = 'Condominium';
    }
  } else if (lowerText.includes('cooperative') || lowerText.includes('co-op')) {
    matched = 'Cooperative';
  } else if (lowerText.includes('manufactured') || lowerText.includes('mobile') || lowerText.includes('trailer')) {
    if (['multi', 'double', 'triple', 'wide'].some(keyword => lowerText.includes(keyword))) {
      matched = 'ManufacturedHousingMultiWide';
    } else if (lowerText.includes('single wide')) {
      matched = 'ManufacturedHousingSingleWide';
    } else {
      matched = 'ManufacturedHousing';
    }
  } else if (lowerText.includes('modular')) {
    matched = 'Modular';
  } else if (['pud', 'planned unit', 'planned development'].some(keyword => lowerText.includes(keyword))) {
    matched = 'Pud';
  } else if (lowerText.includes('timeshare') || lowerText.includes('time share')) {
    matched = 'Timeshare';
  }

  return [matched, rawValue];
}

  let matchedType = null;
  let rawSourceValue = null;

  // Try Model Type first (Priority 1)
  if (modelType) {
    [matchedType, rawSourceValue] = tryMapPropertyType(modelType, rawModelType);
  }

  // If no match from Model Type, try Use Code Description (Priority 2)
  if (!matchedType && useCodeDescription) {
    [matchedType, rawSourceValue] = tryMapPropertyType(useCodeDescription, rawUseCodeDescription);
  }

  // If still no match, use living units as fallback (Priority 3)
  if (!matchedType && livingUnits) {
    if (livingUnits === 1) matchedType = 'SingleFamily';
    else if (livingUnits === 2) matchedType = '2Units';
    else if (livingUnits === 3) matchedType = '3Units';
    else if (livingUnits === 4) matchedType = '4Units';
    else if (livingUnits > 4) matchedType = 'MultipleFamily';
  }

  // Set the final property type
  let propertyType = matchedType || rawSourceValue || null;

  // Last resort: check section titles for property type
  if (!propertyType) {
    $('div.sectionSubTitle').each((_, title) => {
      const rawText = cleanText($(title).text()).toLowerCase();
      if (rawText.includes('condominium')) {
        propertyType = 'Condominium';
        return false;
      } else if (rawText.includes('townhouse')) {
        propertyType = 'Townhouse';
        return false;
      } else if (rawText.includes('single family') || rawText.includes('single-family')) {
        propertyType = 'SingleFamily';
        return false;
      }
    });
  }

  const property = {
    area_under_air: gla || null,
    livable_floor_area: gla || null,
    number_of_units_type: numberOfUnitsType,
    parcel_identifier: parcelIdentifier || null,
    property_legal_description_text: legal || null,
    property_structure_built_year: yearBuilt || null,
    property_type: propertyType || 'SingleFamily', // fallback to SingleFamily
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

  let line1 = lines[0];
  let cityStateZip = lines[1] || '';

  // If no HTML address found, parse from unnormalized_address.json
  if (!line1 && unAddr && unAddr.full_address) {
    const addressParts = unAddr.full_address.split(',');
    if (addressParts.length >= 1) {
      line1 = addressParts[0].trim(); // Just the street part: "833 PUCCINI AVENUE SOUTH"
      if (addressParts.length >= 3) {
        // Just combine city with state zip: "LEHIGH ACRES FL 33974" (no comma)
        cityStateZip = addressParts[1].trim() + ' ' + addressParts[2].trim();
      }
    }
  }

  // Enhanced street parsing with directional and suffix mappings
  let street_number = null,
    street_name = null,
    street_suffix_type = null,
    street_pre_directional_text = null,
    street_post_directional_text = null;

  if (line1) {
    const parts = line1.split(/\s+/);
    street_number = parts.shift() || null;

    const directionalMappings = {
      'NORTH': 'N', 'SOUTH': 'S', 'EAST': 'E', 'WEST': 'W',
      'NORTHEAST': 'NE', 'NORTHWEST': 'NW', 'SOUTHEAST': 'SE', 'SOUTHWEST': 'SW',
      'N': 'N', 'S': 'S', 'E': 'E', 'W': 'W',
      'NE': 'NE', 'NW': 'NW', 'SE': 'SE', 'SW': 'SW'
    };

    const suffixMappings = {
      'STREET': 'St', 'ST': 'St',
        'AVENUE': 'Ave', 'AVE': 'Ave',
        'BOULEVARD': 'Blvd', 'BLVD': 'Blvd',
        'ROAD': 'Rd', 'RD': 'Rd',
        'LANE': 'Ln', 'LN': 'Ln',
        'DRIVE': 'Dr', 'DR': 'Dr',
        'COURT': 'Ct', 'CT': 'Ct',
        'PLACE': 'Pl', 'PL': 'Pl',
        'TERRACE': 'Ter', 'TER': 'Ter',
        'CIRCLE': 'Cir', 'CIR': 'Cir',
        'WAY': 'Way', 'LOOP': 'Loop',
        'PARKWAY': 'Pkwy', 'PKWY': 'Pkwy',
        'PLAZA': 'Plz', 'PLZ': 'Plz',
        'TRAIL': 'Trl', 'TRL': 'Trl',
        'BEND': 'Bnd', 'BND': 'Bnd',
        'CRESCENT': 'Cres', 'CRES': 'Cres',
        'MANOR': 'Mnr', 'MNR': 'Mnr',
        'SQUARE': 'Sq', 'SQ': 'Sq',
        'CROSSING': 'Xing', 'XING': 'Xing',
        'PATH': 'Path',  'RUN': 'Run',
        'WALK': 'Walk',  'ROW': 'Row',
        'ALLEY': 'Aly', 'ALY': 'Aly',
        'BEACH': 'Bch', 'BCH': 'Bch',
        'BRIDGE': 'Br', 'BRG': 'Br',
        'BROOK': 'Brk', 'BRK': 'Brk',
        'BROOKS': 'Brks', 'BRKS': 'Brks',
        'BUG': 'Bg', 'BG': 'Bg',
        'BUGS': 'Bgs', 'BGS': 'Bgs',
        'CLUB': 'Clb', 'CLB': 'Clb',
        'CLIFF': 'Clf', 'CLF': 'Clf',
        'CLIFFS': 'Clfs', 'CLFS': 'Clfs',
        'COMMON': 'Cmn', 'CMN': 'Cmn',
        'COMMONS': 'Cmns', 'CMNS': 'Cmns',
        'CORNER': 'Cor', 'COR': 'Cor',
        'CORNERS': 'Cors', 'CORS': 'Cors',
        'CREEK': 'Crk', 'CRK': 'Crk',
        'COURSE': 'Crse', 'CRSE': 'Crse',
        'CREST': 'Crst', 'CRST': 'Crst',
        'CAUSEWAY': 'Cswy', 'CSWY': 'Cswy',
        'COVE': 'Cv', 'CV': 'Cv',
        'CANYON': 'Cyn', 'CYN': 'Cyn',
        'DALE': 'Dl', 'DL': 'Dl',
        'DAM': 'Dm', 'DM': 'Dm',
        'DRIVES': 'Drs', 'DRS': 'Drs',
        'DIVIDE': 'Dv', 'DV': 'Dv',
        'ESTATE': 'Est', 'EST': 'Est',
        'ESTATES': 'Ests', 'ESTS': 'Ests',
        'EXPRESSWAY': 'Expy', 'EXPY': 'Expy',
        'EXTENSION': 'Ext', 'EXT': 'Ext',
        'EXTENSIONS': 'Exts', 'EXTS': 'Exts',
        'FALL': 'Fall', 'FALL': 'Fall',
        'FALLS': 'Fls', 'FLS': 'Fls',
        'FLAT': 'Flt', 'FLT': 'Flt',
        'FLATS': 'Flts', 'FLTS': 'Flts',
        'FORD': 'Frd', 'FRD': 'Frd',
        'FORDS': 'Frds', 'FRDS': 'Frds',
        'FORGE': 'Frg', 'FRG': 'Frg',
        'FORGES': 'Frgs', 'FRGS': 'Frgs',
        'FORK': 'Frk', 'FRK': 'Frk',
        'FORKS': 'Frks', 'FRKS': 'Frks',
        'FOREST': 'Frst', 'FRST': 'Frst',
        'FREEWAY': 'Fwy', 'FWY': 'Fwy',
        'FIELD': 'Fld', 'FLD': 'Fld',
        'FIELDS': 'Flds', 'FLDS': 'Flds',
        'GARDEN': 'Gdn', 'GDN': 'Gdn',
        'GARDENS': 'Gdns', 'GDNS': 'Gdns',
        'GLEN': 'Gln', 'GLN': 'Gln',
        'GLENS': 'Glns', 'GLNS': 'Glns',
        'GREEN': 'Grn', 'GRN': 'Grn',
        'GREENS': 'Grns', 'GRNS': 'Grns',
        'GROVE': 'Grv', 'GRV': 'Grv',
        'GROVES': 'Grvs', 'GRVS': 'Grvs',
        'GATEWAY': 'Gtwy', 'GTWY': 'Gtwy',
        'HARBOR': 'Hbr', 'HBR': 'Hbr',
        'HARBORS': 'Hbrs', 'HBRS': 'Hbrs',
        'HILL': 'Hl', 'HL': 'Hl',
        'HILLS': 'Hls', 'HLS': 'Hls',
        'HOLLOW': 'Holw', 'HOLW': 'Holw',
        'HEIGHTS': 'Hts', 'HTS': 'Hts',
        'HAVEN': 'Hvn', 'HVN': 'Hvn',
        'HIGHWAY': 'Hwy', 'HWY': 'Hwy',
        'INLET': 'Inlt', 'INLT': 'Inlt',
        'ISLAND': 'Is', 'IS': 'Is',
        'ISLANDS': 'Iss', 'ISS': 'Iss',
        'ISLE': 'Isle', 'SPUR': 'Spur',
        'JUNCTION': 'Jct', 'JCT': 'Jct',
        'JUNCTIONS': 'Jcts', 'JCTS': 'Jcts',
        'KNOLL': 'Knl', 'KNL': 'Knl',
        'KNOLLS': 'Knls', 'KNLS': 'Knls',
        'LOCK': 'Lck', 'LCK': 'Lck',
        'LOCKS': 'Lcks', 'LCKS': 'Lcks',
        'LODGE': 'Ldg', 'LDG': 'Ldg',
        'LIGHT': 'Lgt', 'LGT': 'Lgt',
        'LIGHTS': 'Lgts', 'LGTS': 'Lgts',
        'LAKE': 'Lk', 'LK': 'Lk',
        'LAKES': 'Lks', 'LKS': 'Lks',
        'LANDING': 'Lndg', 'LNDG': 'Lndg',
        'MALL': 'Mall', 'MEWS': 'Mews',
        'MEADOW': 'Mdw', 'MDW': 'Mdw',
        'MEADOWS': 'Mdws', 'MDWS': 'Mdws',
        'MILL': 'Ml', 'ML': 'Ml',
        'MILLS': 'Mls', 'MLS': 'Mls',
        'MANORS': 'Mnrs', 'MNRS': 'Mnrs',
        'MOUNT': 'Mt', 'MT': 'Mt',
        'MOUNTAIN': 'Mtn', 'MTN': 'Mtn',
        'MOUNTAINS': 'Mtns', 'MTNS': 'Mtns',
        'OVERPASS': 'Opas', 'OPAS': 'Opas',
        'ORCHARD': 'Orch', 'ORCH': 'Orch',
        'OVAL': 'Oval', 'PARK': 'Park',
        'PASS': 'Pass', 'PIKE': 'Pike',
        'PLAIN': 'Pln', 'PLN': 'Pln',
        'PLAINS': 'Plns', 'PLNS': 'Plns',
        'PINE': 'Pne', 'PNE': 'Pne',
        'PINES': 'Pnes', 'PNES': 'Pnes',
        'PRAIRIE': 'Pr', 'PR': 'Pr',
        'PORT': 'Prt', 'PRT': 'Prt',
        'PORTS': 'Prts', 'PRTS': 'Prts',
        'PASSAGE': 'Psge', 'PSGE': 'Psge',
        'POINT': 'Pt', 'PT': 'Pt',
        'POINTS': 'Pts', 'PTS': 'Pts',
        'RADIAL': 'Radl', 'RADL': 'Radl',
        'RAMP': 'Ramp', 'REST': 'Rst',
        'RIDGE': 'Rdg', 'RDG': 'Rdg',
        'RIDGES': 'Rdgs', 'RDGS': 'Rdgs',
        'ROADS': 'Rds', 'RDS': 'Rds',
        'RANCH': 'Rnch', 'RNCH': 'Rnch',
        'RAPID': 'Rpd', 'RPD': 'Rpd',
        'RAPIDS': 'Rpds', 'RPDS': 'Rpds',
        'ROUTE': 'Rte', 'RTE': 'Rte',
        'SHOAL': 'Shl', 'SHL': 'Shl',
        'SHOALS': 'Shls', 'SHLS': 'Shls',
        'SHORE': 'Shr', 'SHR': 'Shr',
        'SHORES': 'Shrs', 'SHRS': 'Shrs',
        'SKYWAY': 'Skwy', 'SKWY': 'Skwy',
        'SUMMIT': 'Smt', 'SMT': 'Smt',
        'SPRING': 'Spg', 'SPG': 'Spg',
        'SPRINGS': 'Spgs', 'SPGS': 'Spgs',
        'SQUARES': 'Sqs', 'SQS': 'Sqs',
        'STATION': 'Sta', 'STA': 'Sta',
        'STRAVENUE': 'Stra', 'STRA': 'Stra',
        'STREAM': 'Strm', 'STRM': 'Strm',
        'STREETS': 'Sts', 'STS': 'Sts',
        'THROUGHWAY': 'Trwy', 'TRWY': 'Trwy',
        'TRACE': 'Trce', 'TRCE': 'Trce',
        'TRAFFICWAY': 'Trfy', 'TRFY': 'Trfy',
        'TRAILER': 'Trlr', 'TRLR': 'Trlr',
        'TUNNEL': 'Tunl', 'TUNL': 'Tunl',
        'UNION': 'Un', 'UN': 'Un',
        'UNIONS': 'Uns', 'UNS': 'Uns',
        'UNDERPASS': 'Upas', 'UPAS': 'Upas',
        'VIEW': 'Vw',  'VIEWS': 'Vws',
        'VILLAGE': 'Vlg', 'VLG': 'Vlg',
        'VILLAGES': 'Vlgs', 'VLGS': 'Vlgs',
        'VALLEY': 'Vl', 'VLY': 'Vl',
        'VALLEYS': 'Vlys', 'VLYS': 'Vlys',
        'WAYS': 'Ways', 'VIA': 'Via',
        'WELL': 'Wl', 'WL': 'Wl',
        'WELLS': 'Wls', 'WLS': 'Wls',
        'CROSSROAD': 'Xrd', 'XRD': 'Xrd',
        'CROSSROADS': 'Xrds', 'XRDS': 'Xrds'
    };

    // Find suffix (rightmost suffix)
    let mainSuffixIdx = null;
    for (let i = parts.length - 1; i >= 0; i--) {
      if (suffixMappings[parts[i].toUpperCase()]) {
        mainSuffixIdx = i;
        break;
      }
    }

    // Find directionals
    let preDirectional = null;
    let postDirectional = null;
    let suffix = null;

    for (let i = 0; i < parts.length; i++) {
      const partUpper = parts[i].toUpperCase();

      if (i === mainSuffixIdx && suffixMappings[partUpper]) {
        suffix = suffixMappings[partUpper];
      } else if (directionalMappings[partUpper]) {
        if (mainSuffixIdx !== null) {
          if (i < mainSuffixIdx && preDirectional === null) {
            preDirectional = directionalMappings[partUpper];
          } else if (i > mainSuffixIdx && postDirectional === null) {
            postDirectional = directionalMappings[partUpper];
          }
        } else if (preDirectional === null) {
          preDirectional = directionalMappings[partUpper];
        }
      }
    }

    // Extract street name (everything that's not pre-directional, suffix, or post-directional)
    const streetNameParts = [];

    for (let i = 0; i < parts.length; i++) {
      const part = parts[i];
      const partUpper = part.toUpperCase();

      // Skip if it's the pre-directional, suffix, or post-directional we identified
      if ((preDirectional && directionalMappings[partUpper] === preDirectional &&
           !streetNameParts.some(p => directionalMappings[p.toUpperCase()] === preDirectional)) ||
          i === mainSuffixIdx ||
          (postDirectional && directionalMappings[partUpper] === postDirectional &&
           mainSuffixIdx !== null && i > mainSuffixIdx)) {
        continue;
      }

      streetNameParts.push(part);
    }

    street_pre_directional_text = preDirectional;
    street_post_directional_text = postDirectional;
    street_suffix_type = suffix;
    street_name = streetNameParts.length > 0 ? streetNameParts.join(' ') : null;
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
    county_name: "Lee",
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
    street_post_directional_text: street_post_directional_text || null,
    street_pre_directional_text: street_pre_directional_text || null,
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
  console.log('Script started successfully');
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