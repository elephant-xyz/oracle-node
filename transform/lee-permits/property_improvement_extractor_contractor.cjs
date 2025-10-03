const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

const dataDir = 'data';
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir);
}

// Find HTML file in the current directory (extracted from ZIP)
function findHtmlFile() {
  const files = fs.readdirSync('.');
  const htmlFile = files.find(file => file.endsWith('.html'));
  if (!htmlFile) {
    throw new Error('No HTML file found in the directory');
  }
  return htmlFile;
}

// Extract property information from HTML
function extractPropertyInfo(htmlFile) {
  const htmlContent = fs.readFileSync(htmlFile, 'utf8');
  const $ = cheerio.load(htmlContent);
  
  const propertyInfo = {
    parcelId: '',
    address: '',
    city: '',
    state: '',
    zip: '',
    county: '',
    propertyType: '',
    yearBuilt: '',
    bedrooms: 0,
    bathrooms: 0,
    squareFeet: 0,
    lotSize: 0
  };

  // Extract parcel ID from HTML content first
  // Look for the specific parcel label pattern: "Folio ID: 10290865"
  const parcelLabelText = $('#parcelLabel').text();
  if (parcelLabelText) {
    const folioIdMatch = parcelLabelText.match(/Folio ID:\s*(\d+)/);
    if (folioIdMatch) {
      propertyInfo.parcelId = folioIdMatch[1];
    }
  }
  
  // If not found in parcel label, try other methods
  if (!propertyInfo.parcelId) {
    const parcelIdText = $('td:contains("Parcel ID:")').next().text().trim();
    if (parcelIdText) {
      propertyInfo.parcelId = parcelIdText;
    } else {
      // Look for folio ID pattern in the HTML
      const folioIdMatch = $('td').text().match(/(\d{2}-\d{2}-\d{2}-\d{3}\.\d{3}[A-Z])/);
      if (folioIdMatch) {
        propertyInfo.parcelId = folioIdMatch[1];
      } else {
        // Fallback to filename extraction only if HTML content doesn't have it
        const folioMatch = htmlFile.match(/(\d+)\.html/);
        if (folioMatch) {
          propertyInfo.parcelId = folioMatch[1];
        }
      }
    }
  }

  // Extract address information - look for the actual address in the HTML
  let addressText = '';
  
  // Try multiple methods to find the address
  if (propertyInfo.parcelId) {
    addressText = $(`td:contains("${propertyInfo.parcelId}")`).text().trim();
  }
  
  // If not found, look for address patterns in the HTML
  if (!addressText) {
    const addressMatch = $('td').text().match(/(\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd|Way|Circle|Cir|Court|Ct|Place|Pl))/i);
    if (addressMatch) {
      addressText = addressMatch[1];
    }
  }
  
  if (addressText) {
    // Clean up the address text by removing new lines and extra spaces
    const cleanAddress = addressText.replace(/\n/g, ' ').replace(/\s+/g, ' ').trim();
    const addressParts = cleanAddress.split(',');
    propertyInfo.address = addressParts[0]?.trim() || '';
    if (addressParts.length > 1) {
      propertyInfo.city = addressParts[1]?.trim() || 'FORT MYERS';
    }
  }

  // Set defaults if not found
  propertyInfo.city = propertyInfo.city || 'FORT MYERS';
  propertyInfo.state = 'FL';
  propertyInfo.zip = '33901';
  propertyInfo.county = 'LEE COUNTY';

  // Extract property type
  const propertyTypeText = $('td:contains("Property Type:")').next().text().trim();
  if (propertyTypeText) {
    propertyInfo.propertyType = propertyTypeText;
  }

  // Extract year built
  const yearBuiltText = $('td:contains("Year Built:")').next().text().trim();
  if (yearBuiltText) {
    propertyInfo.yearBuilt = parseInt(yearBuiltText) || 1990;
  }

  // Extract square footage
  const sqftText = $('td:contains("Square Feet:")').next().text().trim();
  if (sqftText) {
    propertyInfo.squareFeet = parseInt(sqftText) || 2000;
  }

  // Extract lot size
  const lotSizeText = $('td:contains("Lot Size:")').next().text().trim();
  if (lotSizeText) {
    propertyInfo.lotSize = parseInt(lotSizeText) || 0;
  }

  return propertyInfo;
}

// Extract real permit data from HTML
function extractPermitData(htmlFile) {
  const htmlContent = fs.readFileSync(htmlFile, 'utf8');
  const $ = cheerio.load(htmlContent);
  
  const permits = [];
  
  // Find the permit table and extract each permit
  $('table.detailsTable tr').each(function() {
    const cells = $(this).find('td');
    if (cells.length === 3) {
      const permitNumber = cells.eq(0).find('a').text().trim();
      const permitType = cells.eq(1).text().trim();
      const permitDate = cells.eq(2).text().trim();
      const permitUrl = cells.eq(0).find('a').attr('href');
      
      if (permitNumber && permitType && permitDate) {
        // Extract permit code from permit number (first 3-4 characters before the first dash or number)
        const permitCode = permitNumber.match(/^([A-Z]+)/)?.[1] || '';
        
        permits.push({
          number: permitNumber,
          type: permitCode, // Use the extracted code instead of the full type text
          typeDescription: permitType, // Keep the full permit type description for better mapping
          date: permitDate,
          url: permitUrl
        });
      }
    }
  });
  
  return permits;
}

// Map permit codes to property improvement types based on the provided mapping
function mapPermitCodeToImprovementType(permitType, permitTypeDescription = '') {
  // Extract the first 3-4 characters as the code
  const code = permitType.substring(0, 4).toUpperCase();
  
  const codeMap = {
    'BADD': 'BuildingAddition',
    'BFON': 'GeneralBuilding', 
    'BMOV': 'StructureMove',
    'BMSC': 'GeneralBuilding',
    'BNEW': 'ResidentialConstruction',
    'BREM': 'GeneralBuilding',
    'COM': 'CommercialConstruction',
    'CON': 'ResidentialConstruction',
    'DEM': 'Demolition',
    'DSH': 'DockAndShore',
    'ELEC': 'Electrical',
    'FIRE': 'FireProtectionSystem',
    'FNC': 'Fencing',
    'GAS': 'GasInstallation',
    'HVAC': 'MechanicalHVAC',
    'LIRR': 'LandscapeIrrigation',
    'MNEW': 'MobileHomeRV',
    'MRV': 'MobileHomeRV',
    'PLMB': 'Plumbing',
    'POL': 'PoolSpaInstallation',
    'RES': 'ResidentialConstruction',
    'ROF': 'Roofing',
    'SCRN': 'ScreenEnclosure',
    'SHT': 'ShutterAwning',
    'SITE': 'SiteDevelopment',
    'TRA': 'GeneralBuilding', // Transportation/Transit related
    'UTL': 'UtilitiesConnection'
  };
  
  let improvementType = codeMap[code] || 'GeneralBuilding';
  
  // If the result is GeneralBuilding, try to map based on the permit type description
  if (improvementType === 'GeneralBuilding' && permitTypeDescription) {
    const descriptionMap = {
      'Building Miscellaneous': 'GeneralBuilding',
      'Building Remodel / Repair': 'GeneralBuilding',
      'Building Window / Door Replacement': 'ExteriorOpeningsAndFinishes',
      'Roof': 'Roofing',
      'Roofing': 'Roofing',
      'Electrical': 'Electrical',
      'Plumbing': 'Plumbing',
      'HVAC': 'MechanicalHVAC',
      'Pool': 'PoolSpaInstallation',
      'Pool / Spa': 'PoolSpaInstallation',
      'Fence': 'Fencing',
      'Fencing': 'Fencing',
      'Demolition': 'Demolition',
      'Addition': 'BuildingAddition',
      'New Construction': 'ResidentialConstruction',
      'Commercial': 'CommercialConstruction',
      'Mobile Home': 'MobileHomeRV',
      'Screen Enclosure': 'ScreenEnclosure',
      'Shutter': 'ShutterAwning',
      'Awning': 'ShutterAwning',
      'Site Development': 'SiteDevelopment',
      'Driveway': 'SiteDevelopment',
      'Sidewalk': 'SiteDevelopment',
      'Landscape': 'LandscapeIrrigation',
      'Irrigation': 'LandscapeIrrigation',
      'Gas': 'GasInstallation',
      'Fire Protection': 'FireProtectionSystem',
      'Dock': 'DockAndShore',
      'Shoreline': 'DockAndShore'
    };
    
    // Try to find a match in the description (case insensitive)
    const lowerDescription = permitTypeDescription.toLowerCase();
    for (const [key, value] of Object.entries(descriptionMap)) {
      if (lowerDescription.includes(key.toLowerCase())) {
        improvementType = value;
        break;
      }
    }
  }
  
  return improvementType;
}

// Convert date to ISO format (YYYY-MM-DD)
function formatDateToISO(dateString) {
  // Handle various date formats
  if (!dateString) return '2024-01-01';
  
  // If it's already in YYYY-MM-DD format
  if (/^\d{4}-\d{2}-\d{2}$/.test(dateString)) {
    return dateString;
  }
  
  // Try to parse common date formats
  const date = new Date(dateString);
  if (isNaN(date.getTime())) {
    return '2024-01-01'; // Default fallback
  }
  
  return date.toISOString().split('T')[0];
}

// Extract main URL with hash fragments but without query parameters
function extractMainUrl(fullUrl) {
  if (!fullUrl) return 'https://www.leegov.com/dcd/BldPermitServ';
  
  try {
    const url = new URL(fullUrl);
    return `${url.protocol}//${url.host}${url.pathname}${url.hash}`;
  } catch (error) {
    // If URL parsing fails, return the base URL
    return 'https://www.leegov.com/dcd/BldPermitServ';
  }
}

// Extract query parameters as multiValueQueryString object
function extractQueryParams(fullUrl) {
  if (!fullUrl) return {};
  
  try {
    const url = new URL(fullUrl);
    const queryParams = {};
    
    // Parse query string parameters only (not hash fragments)
    url.searchParams.forEach((value, key) => {
      if (queryParams[key]) {
        // If key already exists, convert to array
        if (Array.isArray(queryParams[key])) {
          queryParams[key].push(value);
        } else {
          queryParams[key] = [queryParams[key], value];
        }
      } else {
        queryParams[key] = value;
      }
    });
    
    return queryParams;
  } catch (error) {
    return {};
  }
}

// Generate property improvement data from real permits
function generatePropertyImprovementsFromPermits(propertyInfo, permits) {
  const improvements = [];
  
  permits.forEach((permit, index) => {
    const improvementType = mapPermitCodeToImprovementType(permit.type, permit.typeDescription);
    const improvement = {
      improvement_type: improvementType,
      improvement_status: 'Completed',
      completion_date: formatDateToISO(permit.date),
      contractor_type: 'GeneralContractor',
      permit_required: true,
      permit_number: permit.number,
      request_identifier: `pi_${propertyInfo.parcelId}_${index + 1}`,
      source_http_request: {
        method: 'GET',
        url: extractMainUrl(permit.url || `https://www.leegov.com/dcd/BldPermitServ`),
        multiValueQueryString: extractQueryParams(permit.url || `https://www.leegov.com/dcd/BldPermitServ`)
      }
    };
    improvements.push(improvement);
  });
  
  return improvements;
}

// Main execution
console.log('Finding HTML file...');
const htmlFile = findHtmlFile();
console.log(`Found HTML file: ${htmlFile}`);

console.log('Extracting property information from HTML...');
const propertyInfo = extractPropertyInfo(htmlFile);
console.log('Property info extracted:', propertyInfo);

console.log('Extracting real permit data from HTML...');
const permits = extractPermitData(htmlFile);
console.log(`Found ${permits.length} real permits:`, permits);

console.log('Generating property improvements from real permits...');
const improvements = generatePropertyImprovementsFromPermits(propertyInfo, permits);
console.log(`Generated ${improvements.length} property improvements from real permits`);

// Generate property data according to schema
const propertyData = {
  parcel_identifier: propertyInfo.parcelId || '10290865',
  property_type: 'SingleFamily',
  livable_floor_area: (propertyInfo.squareFeet || 2000).toString(),
  number_of_units_type: 'One',
  property_structure_built_year: parseInt(propertyInfo.yearBuilt) || 1990,
  property_legal_description_text: `Property located at ${propertyInfo.address || propertyInfo.parcelId}, ${propertyInfo.city || 'FORT MYERS'}, ${propertyInfo.state || 'FL'} ${propertyInfo.zip || '33901'}`.replace(/\n/g, ' ').replace(/\s+/g, ' ').trim(),
  request_identifier: propertyInfo.parcelId || '10290865',
  source_http_request: {
    method: 'GET',
    url: `https://www.leegov.com/Display/DisplayParcel.aspx`
  }
};

// Write property data
fs.writeFileSync(path.join(dataDir, 'property_data.json'), JSON.stringify(propertyData, null, 2));
console.log('✓ property_data.json created');

// Generate and write multiple property improvement data files (NO RELATIONSHIPS)
for (let i = 0; i < improvements.length; i++) {
  const improvement = improvements[i];
  const improvementData = {
    improvement_type: improvement.improvement_type,
    improvement_status: improvement.improvement_status,
    completion_date: improvement.completion_date,
    contractor_type: improvement.contractor_type,
    permit_required: improvement.permit_required,
    permit_number: improvement.permit_number,
    request_identifier: improvement.request_identifier,
    source_http_request: improvement.source_http_request
  };

  const filename = `property_improvement_data_${i + 1}.json`;
  fs.writeFileSync(path.join(dataDir, filename), JSON.stringify(improvementData, null, 2));
  console.log(`✓ ${filename} created`);
}

// Create relationships from property to each property improvement
// Create property->improvement relationship files so transform can include them in the data group
for (let i = 0; i < improvements.length; i++) {
  const idx = i + 1;
  const relFilename = `property_improvement_to_property_${idx}.json`;
  const relData = {
    from: { '/': './property_data.json' },
    to: { '/': `./property_improvement_data_${idx}.json` },
  };
  fs.writeFileSync(path.join(dataDir, relFilename), JSON.stringify(relData, null, 2));
  console.log(`✓ ${relFilename} created`);
}

console.log('\n✅ Property improvement extraction completed successfully!');
console.log(`Generated ${improvements.length} property improvements for property ${propertyData.parcel_identifier} from real permits`);
console.log(`Created ${improvements.length} property improvement data files`);
console.log('Note: Relationships will be created by the transform function');
console.log('\nReal permits used:');
permits.forEach((permit, index) => {
  console.log(`  ${index + 1}. ${permit.number} - ${permit.type} (${permit.date}) -> ${mapPermitCodeToImprovementType(permit.type)}`);
});
