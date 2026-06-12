const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

// Add these constants for geometry CSV loading
const WORKING_DIR = process.cwd();
const SCRIPT_DIR = __dirname;

/**
 * Parse a polygon string from CSV and convert to new format
 * @param {string} polygonStr - Polygon string from CSV
 * @returns {Array<{latitude: number, longitude: number}>|null} - Parsed polygon or null
 */
function parsePolygonFromString(polygonStr) {
  if (!polygonStr) return null;
  
  // Remove outer quotes if present
  const cleanedStr = polygonStr.trim().replace(/^["']+|["']+$/g, '');
  if (!cleanedStr || cleanedStr.length === 0) return null;
  
  try {
    const parsed = JSON.parse(cleanedStr);
    
    // Handle old format: [[[lon, lat], [lon, lat], ...]]
    if (Array.isArray(parsed) && Array.isArray(parsed[0]) && Array.isArray(parsed[0][0])) {
      // Convert from [[[lon, lat], ...]] to [{latitude: lat, longitude: lon}, ...]
      const coords = parsed[0]; // Get first ring
      if (coords.length >= 3) {
        const polygon = coords.map(coord => {
          if (Array.isArray(coord) && coord.length >= 2) {
            return {
              latitude: coord[1], // lat is second element
              longitude: coord[0]  // lon is first element
            };
          }
          return null;
        }).filter(c => c !== null);
        
        if (polygon.length >= 3) {
          return polygon;
        }
      }
      return null;
    } else if (Array.isArray(parsed)) {
      // Check if already in new format: [{latitude: lat, longitude: lon}, ...]
      if (parsed.length >= 3 && parsed[0] && typeof parsed[0] === 'object' && 'latitude' in parsed[0] && 'longitude' in parsed[0]) {
        return parsed;
      } else {
        // Try to convert if it's [[lon, lat], ...] format
        if (parsed.length >= 3 && Array.isArray(parsed[0]) && parsed[0].length >= 2) {
          return parsed.map(coord => ({
            latitude: coord[1],
            longitude: coord[0]
          }));
        }
      }
    }
    return null;
  } catch (error) {
    console.error('Error parsing polygon from CSV:', error.message);
    console.error('Polygon string (first 100 chars):', cleanedStr.substring(0, 100));
    return null;
  }
}

/**
 * Extract geometry information from CSV
 * Returns three separate geometry objects: address (lat/long only), parcel (parcel_polygon), and building (building_polygon)
 * @param {string} csvPath - Path to CSV file
 * @param {string} requestIdentifier - Request identifier from seed
 * @returns {object|null} - Object with addressGeometry, parcelGeometry, and buildingGeometry properties, or null
 */
function extractGeometryFromCsv(csvPath, requestIdentifier) {
  if (!fs.existsSync(csvPath)) {
    return null;
  }
  
  const csvContent = fs.readFileSync(csvPath, 'utf8');
  const lines = csvContent.split('\n').filter(line => line.trim());
  if (lines.length < 2) {
    return null;
  }
  
  // Parse CSV header - use proper CSV parsing for headers too
  const parseCsvRow = (row) => {
    const result = [];
    let current = '';
    let inQuotes = false;
    let escaped = false;
    
    for (let i = 0; i < row.length; i++) {
      const char = row[i];
      
      if (escaped) {
        current += char;
        escaped = false;
        continue;
      }
      
      if (char === '\\') {
        escaped = true;
        current += char;
        continue;
      }
      
      if (char === '"') {
        // Handle escaped quotes within quoted strings
        if (inQuotes && i + 1 < row.length && row[i + 1] === '"') {
          current += '"';
          i++; // Skip next quote
          continue;
        }
        inQuotes = !inQuotes;
      } else if (char === ',' && !inQuotes) {
        result.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    result.push(current.trim());
    return result;
  };
  
  // Parse headers
  const headers = parseCsvRow(lines[0]).map(h => h.replace(/^["']|["']$/g, '').trim());
  const dataLine = lines[1];
  
  // Find indices for relevant columns
  const parcelPolygonIdx = headers.indexOf('parcel_polygon');
  const buildingPolygonIdx = headers.indexOf('building_polygon');
  const longitudeIdx = headers.indexOf('longitude');
  const latitudeIdx = headers.indexOf('latitude');
  
  if (parcelPolygonIdx === -1 && buildingPolygonIdx === -1 && longitudeIdx === -1 && latitudeIdx === -1) {
    return null;
  }
  
  // Parse CSV data row
  const values = parseCsvRow(dataLine);
  
  let latitude = null;
  let longitude = null;
  let parcelPolygon = null;
  let buildingPolygon = null;
  
  // Extract latitude/longitude (for address geometry)
  if (latitudeIdx !== -1 && values[latitudeIdx]) {
    const lat = parseFloat(values[latitudeIdx]);
    if (!isNaN(lat) && lat >= -90 && lat <= 90) {
      latitude = lat;
    }
  }
  
  if (longitudeIdx !== -1 && values[longitudeIdx]) {
    const lon = parseFloat(values[longitudeIdx]);
    if (!isNaN(lon) && lon >= -180 && lon <= 180) {
      longitude = lon;
    }
  }
  
  // Extract parcel polygon
  if (parcelPolygonIdx !== -1 && parcelPolygonIdx < values.length) {
    parcelPolygon = parsePolygonFromString(values[parcelPolygonIdx]);
    if (parcelPolygon) {
      console.log(`✓ Parsed parcel polygon with ${parcelPolygon.length} points from CSV`);
    }
  }
  
  // Extract building polygon
  if (buildingPolygonIdx !== -1 && buildingPolygonIdx < values.length) {
    buildingPolygon = parsePolygonFromString(values[buildingPolygonIdx]);
    if (buildingPolygon) {
      console.log(`✓ Parsed building polygon with ${buildingPolygon.length} points from CSV`);
    }
  }
  
  // Create three separate geometry objects
  const result = {
    addressGeometry: null,
    parcelGeometry: null,
    buildingGeometry: null
  };
  
  // Address geometry: only lat/long (no polygon)
  if (latitude !== null || longitude !== null) {
    result.addressGeometry = {
      request_identifier: requestIdentifier,
      latitude: latitude,
      longitude: longitude,
      // No polygon for address geometry (polygon field not included)
    };
  }
  
  // Parcel geometry: parcel_polygon only (no lat/long fields)
  if (parcelPolygon && Array.isArray(parcelPolygon) && parcelPolygon.length >= 3) {
    result.parcelGeometry = {
      request_identifier: requestIdentifier,
      polygon: parcelPolygon,
      // No latitude/longitude fields - polygon only
    };
  }
  
  // Building geometry: building_polygon only (no lat/long fields)
  if (buildingPolygon && Array.isArray(buildingPolygon) && buildingPolygon.length >= 3) {
    result.buildingGeometry = {
      request_identifier: requestIdentifier,
      polygon: buildingPolygon,
      // No latitude/longitude fields - polygon only
    };
  }
  
  // Return null if no geometries were created
  if (!result.addressGeometry && !result.parcelGeometry && !result.buildingGeometry) {
    return null;
  }
  
  return result;
}

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

// ============================================================================
// CSV and Polygon Geometry Helpers
// ============================================================================

const NORMALIZE_EOL_REGEX = /\r\n/g;

/**
 * Parse CSV content into rows (array of arrays).
 */
function parseCsv(content) {
  const rows = [];
  let current = '';
  let row = [];
  let insideQuotes = false;

  for (let i = 0; i < content.length; i++) {
    const char = content[i];
    const nextChar = content[i + 1];

    if (char === '"') {
      if (insideQuotes && nextChar === '"') {
        current += '"';
        i++;
      } else {
        insideQuotes = !insideQuotes;
      }
    } else if (char === ',' && !insideQuotes) {
      row.push(current);
      current = '';
    } else if (char === '\n' && !insideQuotes) {
      row.push(current);
      rows.push(row);
      row = [];
      current = '';
    } else {
      current += char;
    }
  }

  if (current || row.length) {
    row.push(current);
    rows.push(row);
  }

  return rows;
}

/**
 * Parse a polygon string (either raw JSON coordinates or GeoJSON object) into a Polygon/MultiPolygon.
 */
function parsePolygonGeoJson(value) {
  if (!value) {
    return null;
  }

  let parsed;
  try {
    parsed = JSON.parse(value);
  } catch {
    return null;
  }

  if (isGeoJsonGeometry(parsed)) {
    return parsed;
  }

  if (!Array.isArray(parsed)) {
    return null;
  }

  const depth = coordinatesDepth(parsed);
  if (depth === 4) {
    return { type: 'MultiPolygon', coordinates: parsed };
  }

  if (depth === 3) {
    return { type: 'Polygon', coordinates: parsed };
  }

  if (depth === 2) {
    return { type: 'Polygon', coordinates: [parsed] };
  }

  return null;
}

function coordinatesDepth(arr, depth = 0) {
  if (!Array.isArray(arr) || !arr.length) return depth;
  return coordinatesDepth(arr[0], depth + 1);
}

function isGeoJsonGeometry(value) {
  return (
    value &&
    typeof value === 'object' &&
    (value.type === 'Polygon' || value.type === 'MultiPolygon') &&
    Array.isArray(value.coordinates)
  );
}

function toNumber(value) {
  if (value == null || value === '') {
    return null;
  }
  const result = Number(value);
  return Number.isFinite(result) ? result : null;
}

/**
 * Minimal Geometry model that mirrors the Elephant Geometry class.
 */
class Geometry {
  constructor({ latitude, longitude, polygon, request_identifier }) {
    this.latitude = latitude ?? null;
    this.longitude = longitude ?? null;
    this.polygon = polygon ?? null;
    this.request_identifier = request_identifier ?? null;
  }

  /**
   * Build a Geometry instance from a CSV record.
   */
  static fromRecord(record) {
    return new Geometry({
      latitude: toNumber(record.latitude),
      longitude: toNumber(record.longitude),
      polygon: parsePolygonGeoJson(record.parcel_polygon),
      request_identifier: record.request_identifier || null
    });
  }
}

/**
 * Split MultiPolygons into separate Polygon geometries.
 */
function splitGeometry(record) {
  const baseGeometry = Geometry.fromRecord(record);
  const { polygon } = baseGeometry;

  if (!polygon || polygon.type !== 'MultiPolygon') {
    return [baseGeometry];
  }

  return polygon.coordinates.map((coords, index) => {
    const identifier = baseGeometry.request_identifier
      ? `${baseGeometry.request_identifier}#${index + 1}`
      : null;

    return new Geometry({
      latitude: baseGeometry.latitude,
      longitude: baseGeometry.longitude,
      polygon: {
        type: 'Polygon',
        coordinates: coords,
      },
      request_identifier: identifier,
    });
  });
}

/**
 * Load CSV content from input.csv or seed.csv in working/scripts/parent directories.
 */
function loadGeometryCsvContent() {
  const parentDir = path.dirname(SCRIPT_DIR);
  const candidates = [
    path.join(WORKING_DIR, "input.csv"),
    path.join(SCRIPT_DIR, "input.csv"),
    path.join(parentDir, "input.csv"),
    path.join(WORKING_DIR, "seed.csv"),
    path.join(SCRIPT_DIR, "seed.csv"),
    path.join(parentDir, "seed.csv"),
  ];

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      try {
        return fs.readFileSync(candidate, "utf8");
      } catch (err) {
        console.warn(`Unable to read geometry CSV at ${candidate}: ${err.message}`);
      }
    }
  }

  return null;
}

/**
 * Read the provided CSV content and return Geometry instances.
 */
function createGeometryInstances(csvContent) {
  const rows = parseCsv(csvContent.replace(NORMALIZE_EOL_REGEX, '\n'));

  if (!rows.length) {
    return [];
  }

  const [header, ...dataRows] = rows;
  const records = dataRows.map((row) =>
    header.reduce((acc, col, idx) => {
      acc[col] = row[idx] || '';
      return acc;
    }, {})
  );

  return records.flatMap((record) => splitGeometry(record));
}

/**
 * Write geometry_parcel_<index>.json and relationship_parcel_has_geometry_parcel_<index>.json files.
 * @param {Geometry[]} geometries - Array of Geometry instances
 */
function createParcelGeometries(geometries) {
  if (!geometries || !geometries.length) {
    return;
  }

  geometries.forEach((geom, geomIndex) => {
    // Build Elephant Geometry payload with polygon array for PARCEL
    const geometry = {
      latitude: geom.latitude ?? null,
      longitude: geom.longitude ?? null,
    };

    if (geom.polygon && Array.isArray(geom.polygon.coordinates)) {
      const exteriorRing = geom.polygon.coordinates[0] || [];
      const polygon = exteriorRing.map((coordinate) => ({
        longitude: coordinate[0],
        latitude: coordinate[1],
      }));
      if (polygon.length) {
        geometry.polygon = polygon;
      }
    }

    const geometryFile = `geometry_parcel_${geomIndex}.json`;
    const relationshipFile = `relationship_parcel_has_geometry_parcel_${geomIndex}.json`;

    writeJSON(path.join("data", geometryFile), geometry);

    const relationship = {
      from: { "/": "./parcel.json" },
      to: { "/": `./${geometryFile}` },
    };
    writeJSON(path.join("data", relationshipFile), relationship);
  });
}

/**
 * Create layout/building geometries from CSV building_polygon column
 * @param {string} csvContent - CSV content
 */
function createLayoutGeometries(csvContent) {
  const rows = parseCsv(csvContent.replace(NORMALIZE_EOL_REGEX, '\n'));

  if (!rows.length) {
    return;
  }

  const [header, ...dataRows] = rows;

  // Check if building_polygon column exists
  const buildingPolygonIdx = header.indexOf('building_polygon');
  if (buildingPolygonIdx === -1) {
    return; // No building polygon data
  }

  let layoutGeomIndex = 0;
  dataRows.forEach((row, rowIdx) => {
    const buildingPolygonValue = row[buildingPolygonIdx];
    if (!buildingPolygonValue) return;

    const polygon = parsePolygonGeoJson(buildingPolygonValue);
    if (!polygon) return;

    // Create geometry for each layout
    const geometry = {
      latitude: null,
      longitude: null,
    };

    if (Array.isArray(polygon.coordinates)) {
      const exteriorRing = polygon.type === 'Polygon'
        ? polygon.coordinates[0]
        : polygon.coordinates[0]?.[0];

      if (exteriorRing) {
        const polygonArray = exteriorRing.map((coordinate) => ({
          longitude: coordinate[0],
          latitude: coordinate[1],
        }));
        if (polygonArray.length) {
          geometry.polygon = polygonArray;
        }
      }
    }

    const geometryFile = `geometry_layout_${layoutGeomIndex + 1}.json`;
    const relationshipFile = `relationship_layout_${layoutGeomIndex + 1}_has_geometry_layout_${layoutGeomIndex + 1}.json`;

    writeJSON(path.join("data", geometryFile), geometry);

    const relationship = {
      from: { "/": `./layout_${layoutGeomIndex + 1}.json` },
      to: { "/": `./${geometryFile}` },
    };
    writeJSON(path.join("data", relationshipFile), relationship);

    layoutGeomIndex++;
  });
}

// ============================================================================
// End of CSV and Polygon Geometry Helpers
// ============================================================================

function normalizeSpaceType(value) {
  if (value == null) return 'MAPPING NOT AVAILABLE';
  const allowed = Array.from(
    new Set([
      "Building",
      "Living Room",
      "Family Room",
      "Great Room",
      "Dining Room",
      "Office Room",
      "Conference Room",
      "Class Room",
      "Plant Floor",
      "Kitchen",
      "Breakfast Nook",
      "Pantry",
      "Primary Bedroom",
      "Secondary Bedroom",
      "Guest Bedroom",
      "Children's Bedroom",
      "Children’s Bedroom",
      "Nursery",
      "Full Bathroom",
      "Three-Quarter Bathroom",
      "Half Bathroom / Powder Room",
      "En-Suite Bathroom",
      "Jack-and-Jill Bathroom",
      "Primary Bathroom",
      "Laundry Room",
      "Mudroom",
      "Closet",
      "Bedroom",
      "Walk-in Closet",
      "Mechanical Room",
      "Storage Room",
      "Server/IT Closet",
      "Home Office",
      "Library",
      "Den",
      "Study",
      "Media Room / Home Theater",
      "Game Room",
      "Home Gym",
      "Music Room",
      "Craft Room / Hobby Room",
      "Prayer Room / Meditation Room",
      "Safe Room / Panic Room",
      "Wine Cellar",
      "Bar Area",
      "Greenhouse",
      "Attached Garage",
      "Detached Garage",
      "Carport",
      "Workshop",
      "Storage Loft",
      "Porch",
      "Screened Porch",
      "Sunroom",
      "Deck",
      "Patio",
      "Pergola",
      "Balcony",
      "Terrace",
      "Gazebo",
      "Pool House",
      "Outdoor Kitchen",
      "Lobby / Entry Hall",
      "Common Room",
      "Utility Closet",
      "Elevator Lobby",
      "Mail Room",
      "Janitor's Closet",
      "Janitor’s Closet",
      "Pool Area",
      "Indoor Pool",
      "Outdoor Pool",
      "Hot Tub / Spa Area",
      "Shed",
      "Lanai",
      "Open Porch",
      "Enclosed Porch",
      "Attic",
      "Enclosed Cabana",
      "Attached Carport",
      "Detached Carport",
      "Detached Utility Closet",
      "Jacuzzi",
      "Courtyard",
      "Open Courtyard",
      "Screen Porch (1-Story)",
      "Screen Enclosure (2-Story)",
      "Screen Enclosure (3-Story)",
      "Screen Enclosure (Custom)",
      "Lower Garage",
      "Lower Screened Porch",
      "Stoop",
      "First Floor",
      "Second Floor",
      "Third Floor",
      "Fourth Floor",
      "Floor",
      "Basement",
      "Sub-Basement",
      "Living Area",
      "Barn",
    ]),
  );
  const s = cleanText(String(value));
  if (!s) return 'MAPPING NOT AVAILABLE';
  const lower = s.toLowerCase();

  // Exact match against allowed (case-insensitive)
  for (const a of allowed) {
    if (a.toLowerCase() === lower) return a;
  }

  // Normalize straight/curly quotes and common punctuation
  const norm = lower
    .replace(/[']/g, "'")
    .replace(/\s+/g, ' ')
    .trim();

  const map = new Map([
    // Rooms
    ['living', 'Living Room'],
    ['living room', 'Living Room'],
    ['livingroom', 'Living Room'],

    ['family', 'Family Room'],
    ['family room', 'Family Room'],

    ['great room', 'Great Room'],
    ['greatroom', 'Great Room'],

    ['dining', 'Dining Room'],
    ['dining room', 'Dining Room'],
    ['dining area', 'Dining Room'],

    ['kitchen', 'Kitchen'],

    ['breakfast', 'Breakfast Nook'],
    ['breakfast nook', 'Breakfast Nook'],
    ['nook', 'Breakfast Nook'],

    ['pantry', 'Pantry'],

    ['primary bedroom', 'Primary Bedroom'],
    ['master bedroom', 'Primary Bedroom'],
    ['owner\'s suite', 'Primary Bedroom'],
    ['owners suite', 'Primary Bedroom'],
    ['primary suite', 'Primary Bedroom'],
    ['main bedroom', 'Primary Bedroom'],

    ['secondary bedroom', 'Secondary Bedroom'],

    ['guest bedroom', 'Guest Bedroom'],
    ['guest room', 'Guest Bedroom'],

    ["children's bedroom", 'Children\'s Bedroom'],
    ['childrens bedroom', 'Children\'s Bedroom'],
    ["children's bedroom", 'Children\'s Bedroom'],
    ['kids bedroom', 'Children\'s Bedroom'],
    ['child bedroom', 'Children\'s Bedroom'],

    ['nursery', 'Nursery'],

    ['full bath', 'Full Bathroom'],
    ['full bathroom', 'Full Bathroom'],
    ['bathroom (full)', 'Full Bathroom'],

    ['three-quarter bathroom', 'Three-Quarter Bathroom'],
    ['three quarter bathroom', 'Three-Quarter Bathroom'],
    ['three-quarter bath', 'Three-Quarter Bathroom'],
    ['three quarter bath', 'Three-Quarter Bathroom'],
    ['3/4 bath', 'Three-Quarter Bathroom'],

    ['half bath', 'Half Bathroom / Powder Room'],
    ['powder room', 'Half Bathroom / Powder Room'],
    ['half bathroom', 'Half Bathroom / Powder Room'],

    ['en suite bathroom', 'En-Suite Bathroom'],
    ['en-suite bathroom', 'En-Suite Bathroom'],
    ['ensuite bathroom', 'En-Suite Bathroom'],
    ['ensuite', 'En-Suite Bathroom'],
    ['en suite', 'En-Suite Bathroom'],

    ['jack and jill bathroom', 'Jack-and-Jill Bathroom'],
    ['jack & jill bathroom', 'Jack-and-Jill Bathroom'],
    ['jack-and-jill bathroom', 'Jack-and-Jill Bathroom'],

    ['primary bathroom', 'Primary Bathroom'],
    ['master bathroom', 'Primary Bathroom'],
    ['primary bath', 'Primary Bathroom'],
    ['master bath', 'Primary Bathroom'],

    ['laundry', 'Laundry Room'],
    ['laundry room', 'Laundry Room'],
    ['wash room', 'Laundry Room'],

    ['mudroom', 'Mudroom'],
    ['mud room', 'Mudroom'],

    ['closet', 'Closet'],
    ['walk-in closet', 'Walk-in Closet'],
    ['walk in closet', 'Walk-in Closet'],

    ['mechanical', 'Mechanical Room'],
    ['mechanical room', 'Mechanical Room'],
    ['mech room', 'Mechanical Room'],

    ['storage', 'Storage Room'],
    ['storage room', 'Storage Room'],

    ['server closet', 'Server/IT Closet'],
    ['it closet', 'Server/IT Closet'],
    ['server/it closet', 'Server/IT Closet'],

    ['home office', 'Home Office'],
    ['office', 'Home Office'],

    ['library', 'Library'],

    ['den', 'Den'],

    ['study', 'Study'],

    ['media room', 'Media Room / Home Theater'],
    ['home theater', 'Media Room / Home Theater'],
    ['home theatre', 'Media Room / Home Theater'],
    ['theater room', 'Media Room / Home Theater'],
    ['theatre room', 'Media Room / Home Theater'],

    ['game room', 'Game Room'],
    ['gameroom', 'Game Room'],

    ['home gym', 'Home Gym'],
    ['gym', 'Home Gym'],
    ['exercise room', 'Home Gym'],
    ['fitness room', 'Home Gym'],

    ['music room', 'Music Room'],

    ['craft room', 'Craft Room / Hobby Room'],
    ['hobby room', 'Craft Room / Hobby Room'],
    ['craft/hobby room', 'Craft Room / Hobby Room'],

    ['prayer room', 'Prayer Room / Meditation Room'],
    ['meditation room', 'Prayer Room / Meditation Room'],
    ['prayer/meditation room', 'Prayer Room / Meditation Room'],

    ['safe room', 'Safe Room / Panic Room'],
    ['panic room', 'Safe Room / Panic Room'],

    ['wine cellar', 'Wine Cellar'],

    ['bar area', 'Bar Area'],
    ['bar', 'Bar Area'],
    ['wet bar', 'Bar Area'],

    ['greenhouse', 'Greenhouse'],

    ['attached garage', 'Attached Garage'],
    ['garage', 'Attached Garage'], // default to attached when unspecified
    ['detached garage', 'Detached Garage'],

    ['carport', 'Carport'],

    ['workshop', 'Workshop'],

    ['storage loft', 'Storage Loft'],
    ['loft storage', 'Storage Loft'],

    ['porch', 'Porch'],
    ['front porch', 'Porch'],

    ['screened porch', 'Screened Porch'],
    ['screen porch', 'Screened Porch'],
    ['screened lanai', 'Screened Porch'],
    ['lanai', 'Screened Porch'],

    ['sunroom', 'Sunroom'],
    ['sun room', 'Sunroom'],
    ['florida room', 'Sunroom'],

    ['deck', 'Deck'],

    ['patio', 'Patio'],

    ['pergola', 'Pergola'],

    ['balcony', 'Balcony'],

    ['terrace', 'Terrace'],

    ['gazebo', 'Gazebo'],

    ['pool house', 'Pool House'],

    ['outdoor kitchen', 'Outdoor Kitchen'],
    ['summer kitchen', 'Outdoor Kitchen'],

    ['lobby', 'Lobby / Entry Hall'],
    ['entry', 'Lobby / Entry Hall'],
    ['entry hall', 'Lobby / Entry Hall'],
    ['foyer', 'Lobby / Entry Hall'],
    ['lobby / entry hall', 'Lobby / Entry Hall'],

    ['common room', 'Common Room'],
    ['community room', 'Common Room'],

    ['utility closet', 'Utility Closet'],
    ['electrical closet', 'Utility Closet'],

    ['elevator lobby', 'Elevator Lobby'],

    ['mail room', 'Mail Room'],
    ['mailroom', 'Mail Room'],

    ["janitor's closet", 'Janitor\'s Closet'],
    ['janitors closet', 'Janitor\'s Closet'],
    ["janitor's closet", 'Janitor\'s Closet'],

    ['pool area', 'Pool Area'],
    ['pool deck', 'Pool Area'],

    ['indoor pool', 'Indoor Pool'],

    ['outdoor pool', 'Outdoor Pool'],
    ['pool', 'Outdoor Pool'],

    ['hot tub', 'Hot Tub / Spa Area'],
    ['spa', 'Hot Tub / Spa Area'],
    ['jacuzzi', 'Hot Tub / Spa Area'],

    ['shed', 'Shed'],

    ['bedroom', 'Bedroom']
  ]);

  if (map.has(norm)) return map.get(norm);

  // Not recognized; return "MAPPING NOT AVAILABLE" so validation fails
  return 'MAPPING NOT AVAILABLE';
}

function extractProperty($, seedData) {
  // parcel_identifier (STRAP)
  const parcelLabel = $('#parcelLabel').text();
  let parcelIdentifier = null;
  const strapMatch = parcelLabel.match(/STRAP:\s*([^\s]+)\s*/i);
  if (strapMatch) {
    parcelIdentifier = cleanText(strapMatch[1]);
  } else if (parcelLabel && parcelLabel.trim().length > 0) {
    // If no "STRAP:" prefix, use the label text directly if it's not all zeros
    const labelText = cleanText(parcelLabel);
    if (labelText && labelText !== '00-00-00-00-00000.0000' && labelText !== '0') {
      parcelIdentifier = labelText;
    }
  }

  // Fallback to seed data's parcel_id if HTML extraction failed
  if (!parcelIdentifier && seedData && seedData.parcel_id) {
    parcelIdentifier = seedData.parcel_id;
  }

  // legal description - try multiple methods
  let legal = null;
  
  // Method 1: Look for Property Description section
  $('.sectionSubTitle').each((i, el) => {
    const t = cleanText($(el).text());
    if (/Property Description/i.test(t)) {
      const $next = $(el).next('.textPanel');
      if ($next.length) {
        const txt = cleanText($next.text());
        if (txt && txt.trim().length > 0) {
          legal = txt.replace(/\s+/g, ' ').trim();
          return false; // break
        }
      }
      // Also check if text is in the same element or siblings
      const $siblings = $(el).siblings('.textPanel');
      if ($siblings.length) {
        const txt = cleanText($siblings.first().text());
        if (txt && txt.trim().length > 0) {
          legal = txt.replace(/\s+/g, ' ').trim();
          return false;
        }
      }
    }
  });
  
  // Method 2: Look in PropertyDetailsCurrent
  if (!legal) {
    $('#PropertyDetailsCurrent')
      .find('.sectionSubTitle')
      .each((i, el) => {
        const t = cleanText($(el).text());
        if (/Property Description/i.test(t)) {
          const txt = cleanText($(el).next('.textPanel').text());
          if (txt && txt.trim().length > 0) {
            legal = txt.replace(/\s+/g, ' ').trim();
            return false;
          }
        }
      });
  }
  
  // Method 3: Look for textPanel after sectionSubTitle containing "Property Description"
  if (!legal) {
    $('div.sectionSubTitle').each((i, el) => {
      const text = cleanText($(el).text());
      if (/Property Description/i.test(text)) {
        const $textPanel = $(el).nextAll('.textPanel').first();
        if ($textPanel.length) {
          const txt = cleanText($textPanel.text());
          if (txt && txt.trim().length > 0) {
            legal = txt.replace(/\s+/g, ' ').trim();
            return false;
          }
        }
      }
    });
  }
  
  // Method 4: Look for any textPanel with substantial content near Property Description
  if (!legal) {
    $('div.sectionSubTitle:contains("Property Description")').each((i, el) => {
      const $panel = $(el).next('.textPanel');
      if ($panel.length) {
        const txt = cleanText($panel.text());
        if (txt && txt.trim().length > 10) { // At least 10 chars
          legal = txt.replace(/\s+/g, ' ').trim();
          return false;
        }
      }
    });
  }
  
  if (legal) {
    // normalize spacing and remove excessive whitespace
    legal = legal.replace(/\s{2,}/g, ' ').trim();
    // Remove common disclaimer text
    legal = legal.replace(/NOTE:.*?should not be used.*?\./gi, '').trim();
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
  $('table.appraisalAttributes, table.appraisalDetails').each((_, table) => {
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

  // Extract Use Code Description from Land Tracts table or other locations
  let useCodeDescription = null;
  let rawUseCodeDescription = null;

  // Method 1: Look for "Use Code" or "Land Use" in any table
  let foundUseCode = false;
  $('table').each((_, table) => {
    if (foundUseCode) return false;
    $(table).find('tr').each((_, row) => {
      if (foundUseCode) return false;
      const cells = $(row).find('td, th');
      cells.each((_, cell) => {
        if (foundUseCode) return false;
        const text = cleanText($(cell).text()).toLowerCase();
        if (text.includes('use code') || text.includes('land use') || text.includes('use code description')) {
          // Found a header, look for data in next row or same row
          const $nextRow = $(row).next();
          if ($nextRow.length) {
            const dataCells = $nextRow.find('td, th');
            const cellIndex = $(cell).index();
            if (cellIndex < dataCells.length) {
              let dataText = cleanText($(dataCells[cellIndex]).text());
              // Remove leading numbers and dashes (e.g., "100 - Single Family Residential" -> "Single Family Residential")
              dataText = dataText.replace(/^\d+\s*[-–]\s*/, '').trim();
              if (dataText && dataText.trim().length > 0 && /[a-zA-Z]/.test(dataText)) {
                rawUseCodeDescription = dataText;
                useCodeDescription = dataText.toLowerCase();
                console.log(`Found Use Code Description (Method 1): "${rawUseCodeDescription}"`);
                foundUseCode = true;
                return false; // break this loop
              }
            }
          }
        }
      });
    });
  });

  // Method 2: Look in Land Tracts table (original method)
  if (!useCodeDescription) {
    const sections = $('#PropertyDetailsCurrent, #PropertyDetails');
    sections.each((_, section) => {
      $(section).find('table.appraisalAttributes, table.appraisalDetails').each((_, table) => {
        // First, check if this table has "Land Tracts" header
        const hasLandTracts = $(table).text().toLowerCase().includes('land tracts');
        if (!hasLandTracts) return; // Skip tables without Land Tracts
        
        // Look for header row with "Use Code Description"
        let useCodeDescColIndex = null;
        let foundHeader = false;
        $(table).find('tr').each(function() {
          if (foundHeader) return false;
          const cells = $(this).find('th, td');
          cells.each(function(idx) {
            const cellText = cleanText($(this).text()).toLowerCase();
            if (cellText.includes('use code description')) {
              useCodeDescColIndex = idx;
              foundHeader = true;
              return false; // break
            }
          });
          if (useCodeDescColIndex !== null) return false; // break
        });
        
        // If we found the column, get the value from the data row
        if (useCodeDescColIndex !== null) {
          let foundValue = false;
          $(table).find('tr').each(function() {
            if (foundValue) return false;
            const cells = $(this).find('td');
            if (cells.length > useCodeDescColIndex) {
              let cellText = cleanText($(cells[useCodeDescColIndex]).text());
              // Remove leading numbers and dashes (e.g., "100 - Single Family Residential" -> "Single Family Residential")
              cellText = cellText.replace(/^\d+\s*[-–]\s*/, '').trim();
              // Check if this looks like a use code description (not a number, not empty, has letters)
              if (cellText && cellText.trim().length > 0 && !/^\d+$/.test(cellText.trim()) && /[a-zA-Z]/.test(cellText)) {
                rawUseCodeDescription = cellText;
                useCodeDescription = cellText.toLowerCase();
                console.log(`Found Use Code Description (Method 2): "${rawUseCodeDescription}"`);
                foundValue = true;
                return false; // break
              }
            }
          });
          if (foundValue) return false; // break outer loop
        }
        
        // Fallback: old method
        $(table).find('tr').each((_, row) => {
          const cells = $(row).find('td, th');
          if (cells.length >= 1 && cleanText($(cells[0]).text()).toLowerCase().includes('land tracts')) {
            let headerRow = null;
            const dataRows = [];

            let currentRow = $(row).next();
            while (currentRow.length) {
              const rowCells = currentRow.find('td, th');
              if (!rowCells.length) break;

              const hasUseCodeHeader = rowCells.toArray().some(function(cell) {
              return cleanText($(cell).text()).toLowerCase().includes('use code description');
            });
              if (hasUseCodeHeader) {
                headerRow = currentRow;
              } else if (headerRow && rowCells.length) {
                dataRows.push(currentRow);
              } else if (dataRows.length > 0 && !rowCells.toArray().some(function(cell) {
                return cleanText($(cell).text());
              })) {
                break;
              }

              currentRow = currentRow.next();
            }

            if (headerRow && dataRows.length) {
              const headerCells = headerRow.find('td, th');
              const headers = headerCells.toArray().map(function(cell) {
                return cleanText($(cell).text()).toLowerCase();
              });

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
                  let cellText = cleanText($(dataCells[descColIndex]).text());
                  // Remove leading numbers and dashes (e.g., "100 - Single Family Residential" -> "Single Family Residential")
                  cellText = cellText.replace(/^\d+\s*[-–]\s*/, '').trim();
                  rawUseCodeDescription = cellText;
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
  }
  
  // Method 3: Look for use code in certified roll data or other sections
  if (!useCodeDescription) {
    $('.certifiedRollData, .appraisalDetails').each((_, el) => {
      const text = $(el).text();
      const useCodeMatch = text.match(/Use\s+Code[:\s]+([^\n]+)/i);
      if (useCodeMatch) {
        let cellText = cleanText(useCodeMatch[1]);
        // Remove leading numbers and dashes
        cellText = cellText.replace(/^\d+\s*[-–]\s*/, '').trim();
        rawUseCodeDescription = cellText;
        useCodeDescription = rawUseCodeDescription.toLowerCase();
        console.log(`Found Use Code Description (Method 3): "${rawUseCodeDescription}"`);
        return false;
      }
    });
  }

  // Extract Improvement Type (for fallback mapping)
  let improvementType = null;
  $('table.appraisalAttributes, table.appraisalDetails').each(function () {
    if (improvementType) return false;
    const $table = $(this);
    $table.find('tr').each(function () {
      if (improvementType) return false;
      const ths = $(this).find('th');
      if (ths.length && /Improvement Type/i.test(cleanText($(ths[0]).text()))) {
        const nextRow = $(this).next();
        if (nextRow && nextRow.length) {
          const tds = nextRow.find('td, th');
          if (tds.length) {
            const text = cleanText($(tds[0]).text());
            if (text) {
              improvementType = text;
              console.log(`Found Improvement Type: "${improvementType}"`);
              return false;
            }
          }
        }
      }
      return undefined;
    });
    return undefined;
  });
  if (!improvementType) {
    console.log('Improvement Type not found');
  }

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

  // Map property_type to only 4 allowed values: LandParcel, Building, Unit, ManufacturedHome
  let mappedPropertyType = null;
  if (propertyType) {
    const lowerType = String(propertyType).toLowerCase();
    if (lowerType.includes('vacant') || lowerType.includes('land')) {
      mappedPropertyType = 'LandParcel';
    } else if (lowerType.includes('manufactured') || lowerType.includes('mobile')) {
      mappedPropertyType = 'ManufacturedHome';
    } else if (lowerType.includes('condominium') || lowerType.includes('cooperative') || lowerType.includes('unit')) {
      mappedPropertyType = 'Unit';
    } else {
      // Default to Building for residential structures
      mappedPropertyType = 'Building';
    }
  } else {
    // Default fallback
    mappedPropertyType = 'Building';
  }

  // Extract land use code for structure_form, property_usage_type, build_status, ownership_estate_type
  // The useCodeDescription was already extracted above
  let structureForm = null;
  let propertyUsageType = null;
  let buildStatus = null;
  let ownershipEstateType = null;

  console.log(`DEBUG: useCodeDescription = "${useCodeDescription}"`);
  console.log(`DEBUG: rawUseCodeDescription = "${rawUseCodeDescription}"`);

  if (useCodeDescription) {
    const lowerUseCode = useCodeDescription.toLowerCase();
    console.log(`DEBUG: lowerUseCode = "${lowerUseCode}"`);
    
    // Map build_status
    if (lowerUseCode.includes('vacant')) {
      buildStatus = 'VacantLand';
    } else if (lowerUseCode.includes('under construction') || lowerUseCode.includes('construction')) {
      buildStatus = 'UnderConstruction';
    } else {
      buildStatus = 'Improved';
    }

    // Map structure_form from use code description
    // Check in order of specificity (most specific first)
    console.log(`DEBUG: Mapping structure_form from: "${lowerUseCode}"`);
    if (lowerUseCode.includes('manufactured') || lowerUseCode.includes('mobile home')) {
      if (lowerUseCode.includes('park') || lowerUseCode.includes('mobile home park')) {
        structureForm = 'ManufacturedHomeInPark';
      } else {
        structureForm = 'ManufacturedHomeOnLand';
      }
    } else if (lowerUseCode.includes('modular')) {
      structureForm = 'Modular';
    } else if (lowerUseCode.includes('single family') && (lowerUseCode.includes('attached') || lowerUseCode.includes('semi') || lowerUseCode.includes('semi-detached'))) {
      structureForm = 'SingleFamilySemiDetached';
    } else if (lowerUseCode.includes('single family') && !lowerUseCode.includes('attached')) {
      structureForm = 'SingleFamilyDetached';
    } else if (lowerUseCode.includes('townhouse') || lowerUseCode.includes('town house') || lowerUseCode.includes('townhome') || lowerUseCode.includes('rowhouse') || lowerUseCode.includes('row house')) {
      structureForm = 'TownhouseRowhouse';
    } else if (lowerUseCode.includes('duplex') || lowerUseCode.includes('2 unit') || lowerUseCode.includes('two unit')) {
      structureForm = 'Duplex';
    } else if (lowerUseCode.includes('triplex') || lowerUseCode.includes('3 unit') || lowerUseCode.includes('three unit')) {
      structureForm = 'Triplex';
    } else if (lowerUseCode.includes('fourplex') || lowerUseCode.includes('4 unit') || lowerUseCode.includes('four unit') || lowerUseCode.includes('quadplex')) {
      structureForm = 'Quadplex';
    } else if (lowerUseCode.includes('multi-family') || lowerUseCode.includes('multifamily') || lowerUseCode.includes('multi family')) {
      // Check for more than 10 units first (most specific)
      if (lowerUseCode.includes('more than 10') || lowerUseCode.includes('10+') || lowerUseCode.includes('11') || lowerUseCode.includes('12') || lowerUseCode.includes('15') || lowerUseCode.includes('20')) {
        structureForm = 'MultiFamilyMoreThan10';
      } else if (lowerUseCode.includes('5') || lowerUseCode.includes('6') || lowerUseCode.includes('7') || lowerUseCode.includes('8') || lowerUseCode.includes('9') || lowerUseCode.includes('10') || lowerUseCode.includes('more than 5') || lowerUseCode.includes('5+')) {
        structureForm = 'MultiFamily5Plus';
      } else if (lowerUseCode.includes('less than 10') || lowerUseCode.includes('less than 5') || lowerUseCode.includes('<10') || lowerUseCode.includes('<5')) {
        structureForm = 'MultiFamilyLessThan10';
      } else {
        // Default for multi-family without specific count
        structureForm = 'MultiFamilyLessThan10';
      }
    } else if (lowerUseCode.includes('apartment') || lowerUseCode.includes('apt')) {
      structureForm = 'ApartmentUnit';
    } else if (lowerUseCode.includes('loft')) {
      structureForm = 'Loft';
    }

    // Map property_usage_type from use code description
    // Check in order of specificity (most specific first)
    console.log(`DEBUG: Mapping property_usage_type from: "${lowerUseCode}"`);
    if (lowerUseCode.includes('retail store') || lowerUseCode.includes('retail shop')) {
      propertyUsageType = 'RetailStore';
    } else if (lowerUseCode.includes('department store')) {
      propertyUsageType = 'DepartmentStore';
    } else if (lowerUseCode.includes('supermarket') || lowerUseCode.includes('grocery store')) {
      propertyUsageType = 'Supermarket';
    } else if (lowerUseCode.includes('shopping center') || lowerUseCode.includes('shopping mall')) {
      if (lowerUseCode.includes('regional')) {
        propertyUsageType = 'ShoppingCenterRegional';
      } else {
        propertyUsageType = 'ShoppingCenterCommunity';
      }
    } else if (lowerUseCode.includes('office building') || lowerUseCode.includes('office complex')) {
      propertyUsageType = 'OfficeBuilding';
    } else if (lowerUseCode.includes('medical office') || lowerUseCode.includes('doctor office') || lowerUseCode.includes('clinic')) {
      propertyUsageType = 'MedicalOffice';
    } else if (lowerUseCode.includes('restaurant') || lowerUseCode.includes('cafe') || lowerUseCode.includes('diner')) {
      propertyUsageType = 'Restaurant';
    } else if (lowerUseCode.includes('hotel') || lowerUseCode.includes('motel') || lowerUseCode.includes('inn')) {
      propertyUsageType = 'Hotel';
    } else if (lowerUseCode.includes('golf course')) {
      propertyUsageType = 'GolfCourse';
    } else if (lowerUseCode.includes('warehouse') || lowerUseCode.includes('storage facility')) {
      propertyUsageType = 'Warehouse';
    } else if (lowerUseCode.includes('manufacturing') || lowerUseCode.includes('factory')) {
      if (lowerUseCode.includes('heavy') || lowerUseCode.includes('heavy manufacturing')) {
        propertyUsageType = 'HeavyManufacturing';
      } else {
        propertyUsageType = 'LightManufacturing';
      }
    } else if (lowerUseCode.includes('school')) {
      if (lowerUseCode.includes('public') || lowerUseCode.includes('public school')) {
        propertyUsageType = 'PublicSchool';
      } else {
        propertyUsageType = 'PrivateSchool';
      }
    } else if (lowerUseCode.includes('hospital')) {
      if (lowerUseCode.includes('public') || lowerUseCode.includes('public hospital')) {
        propertyUsageType = 'PublicHospital';
      } else {
        propertyUsageType = 'PrivateHospital';
      }
    } else if (lowerUseCode.includes('church') || lowerUseCode.includes('temple') || lowerUseCode.includes('mosque') || lowerUseCode.includes('synagogue')) {
      propertyUsageType = 'Church';
    } else if (lowerUseCode.includes('mobile home park') || lowerUseCode.includes('trailer park')) {
      propertyUsageType = 'MobileHomePark';
    } else if (lowerUseCode.includes('service station') || lowerUseCode.includes('gas station')) {
      propertyUsageType = 'ServiceStation';
    } else if (lowerUseCode.includes('auto sales') || lowerUseCode.includes('car dealership') || lowerUseCode.includes('auto repair')) {
      propertyUsageType = 'AutoSalesRepair';
    } else if (lowerUseCode.includes('financial') || lowerUseCode.includes('bank') || lowerUseCode.includes('credit union')) {
      propertyUsageType = 'FinancialInstitution';
    } else if (lowerUseCode.includes('theater') || lowerUseCode.includes('cinema') || lowerUseCode.includes('movie theater')) {
      propertyUsageType = 'Theater';
    } else if (lowerUseCode.includes('entertainment') || lowerUseCode.includes('amusement')) {
      propertyUsageType = 'Entertainment';
    } else if (lowerUseCode.includes('nursery') || lowerUseCode.includes('greenhouse')) {
      propertyUsageType = 'NurseryGreenhouse';
    } else if (lowerUseCode.includes('vineyard') || lowerUseCode.includes('winery')) {
      propertyUsageType = 'VineyardWinery';
    } else if (lowerUseCode.includes('data center') || lowerUseCode.includes('datacenter')) {
      propertyUsageType = 'DataCenter';
    } else if (lowerUseCode.includes('solar') || lowerUseCode.includes('solar farm')) {
      propertyUsageType = 'SolarFarm';
    } else if (lowerUseCode.includes('wind') || lowerUseCode.includes('wind farm')) {
      propertyUsageType = 'WindFarm';
    } else if (lowerUseCode.includes('residential') || lowerUseCode.includes('single family') || lowerUseCode.includes('multi-family') || lowerUseCode.includes('multifamily') || lowerUseCode.includes('condominium') || lowerUseCode.includes('condo') || lowerUseCode.includes('townhouse') || lowerUseCode.includes('town house')) {
      propertyUsageType = 'Residential';
    } else if (lowerUseCode.includes('commercial') || lowerUseCode.includes('retail') || lowerUseCode.includes('office') || lowerUseCode.includes('store') || lowerUseCode.includes('business')) {
      propertyUsageType = 'Commercial';
    } else if (lowerUseCode.includes('industrial') || lowerUseCode.includes('manufacturing')) {
      propertyUsageType = 'Industrial';
    } else if (lowerUseCode.includes('agricultural') || lowerUseCode.includes('farm') || lowerUseCode.includes('grove') || lowerUseCode.includes('orchard') || lowerUseCode.includes('pasture') || lowerUseCode.includes('timber') || lowerUseCode.includes('cropland') || lowerUseCode.includes('hay') || lowerUseCode.includes('grazing') || lowerUseCode.includes('livestock') || lowerUseCode.includes('poultry')) {
      // More specific agricultural types
      if (lowerUseCode.includes('cropland') || lowerUseCode.includes('crop land')) {
        if (lowerUseCode.includes('class 2') || lowerUseCode.includes('class2')) {
          propertyUsageType = 'CroplandClass2';
        } else if (lowerUseCode.includes('class 3') || lowerUseCode.includes('class3')) {
          propertyUsageType = 'CroplandClass3';
        } else {
          propertyUsageType = 'DrylandCropland';
        }
      } else if (lowerUseCode.includes('hay') || lowerUseCode.includes('meadow')) {
        propertyUsageType = 'HayMeadow';
      } else if (lowerUseCode.includes('timber') || lowerUseCode.includes('forest')) {
        propertyUsageType = 'TimberLand';
      } else if (lowerUseCode.includes('grazing') || lowerUseCode.includes('range')) {
        if (lowerUseCode.includes('improved') || lowerUseCode.includes('native')) {
          propertyUsageType = lowerUseCode.includes('improved') ? 'ImprovedPasture' : 'NativePasture';
        } else {
          propertyUsageType = 'GrazingLand';
        }
      } else if (lowerUseCode.includes('orchard') || lowerUseCode.includes('grove')) {
        propertyUsageType = 'OrchardGroves';
      } else if (lowerUseCode.includes('poultry')) {
        propertyUsageType = 'Poultry';
      } else if (lowerUseCode.includes('livestock')) {
        propertyUsageType = 'LivestockFacility';
      } else {
        propertyUsageType = 'Agricultural';
      }
    } else if (lowerUseCode.includes('recreational') || lowerUseCode.includes('golf') || lowerUseCode.includes('park') || lowerUseCode.includes('recreation')) {
      propertyUsageType = 'Recreational';
    } else if (lowerUseCode.includes('conservation') || lowerUseCode.includes('preserve') || lowerUseCode.includes('conservation')) {
      propertyUsageType = 'Conservation';
    } else if (lowerUseCode.includes('retirement') || lowerUseCode.includes('retirement home') || lowerUseCode.includes('senior')) {
      propertyUsageType = 'Retirement';
    }
    
    console.log(`DEBUG: Mapped structureForm = "${structureForm}"`);
    console.log(`DEBUG: Mapped propertyUsageType = "${propertyUsageType}"`);

    // Map ownership_estate_type from use code description
    if (lowerUseCode.includes('condominium') || lowerUseCode.includes('condo')) {
      ownershipEstateType = 'Condominium';
    } else if (lowerUseCode.includes('cooperative') || lowerUseCode.includes('co-op')) {
      ownershipEstateType = 'Cooperative';
    } else if (lowerUseCode.includes('timeshare') || lowerUseCode.includes('interval ownership')) {
      ownershipEstateType = 'Timeshare';
    } else if (lowerUseCode.includes('lease') || lowerUseCode.includes('leasehold')) {
      ownershipEstateType = 'Leasehold';
    } else {
      // Default to FeeSimple for most properties
      ownershipEstateType = 'FeeSimple';
    }
  }

  // Fallback mapping using Improvement Type when use code description mapping fails
  if (!structureForm && improvementType) {
    const normalizedImprovement = improvementType.replace(/^\d+\s*[-–]\s*/, '').trim();
    const lowerImp = normalizedImprovement.toLowerCase();
    console.log(`DEBUG: Fallback mapping structure_form from improvementType: "${normalizedImprovement}"`);

    if (lowerImp.includes('manufactured') || lowerImp.includes('mobile home')) {
      if (lowerImp.includes('park')) {
        structureForm = 'ManufacturedHomeInPark';
      } else {
        structureForm = 'ManufacturedHomeOnLand';
      }
    } else if (lowerImp.includes('modular')) {
      structureForm = 'Modular';
    } else if (lowerImp.includes('single') && lowerImp.includes('family')) {
      if (lowerImp.includes('attached') || lowerImp.includes('semi')) {
        structureForm = 'SingleFamilySemiDetached';
      } else {
        structureForm = 'SingleFamilyDetached';
      }
    } else if (lowerImp.includes('ranch') || lowerImp.includes('bungalow') || lowerImp.includes('cottage')) {
      structureForm = 'SingleFamilyDetached';
    } else if (lowerImp.includes('town') || lowerImp.includes('row') || lowerImp.includes('twnh')) {
      structureForm = 'TownhouseRowhouse';
    } else if (lowerImp.includes('duplex') || lowerImp.includes('2 unit') || lowerImp.includes('two unit')) {
      structureForm = 'Duplex';
    } else if (lowerImp.includes('triplex') || lowerImp.includes('3 unit') || lowerImp.includes('three unit')) {
      structureForm = 'Triplex';
    } else if (lowerImp.includes('fourplex') || lowerImp.includes('quad') || lowerImp.includes('4 unit') || lowerImp.includes('four unit')) {
      structureForm = 'Quadplex';
    } else if (lowerImp.includes('multi') || lowerImp.includes('apartment') || lowerImp.includes('apt')) {
      // If improvement type indicates multi-family or apartments, default to MultiFamily5Plus unless otherwise specified
      if (lowerImp.includes('more than 10') || lowerImp.includes('10+')) {
        structureForm = 'MultiFamilyMoreThan10';
      } else if (lowerImp.includes('5') || lowerImp.includes('6') || lowerImp.includes('7') || lowerImp.includes('8') || lowerImp.includes('9')) {
        structureForm = 'MultiFamily5Plus';
      } else {
        structureForm = 'ApartmentUnit';
      }
    }

    console.log(`DEBUG: Fallback structureForm result = "${structureForm}"`);
  }

  if (!propertyUsageType) {
    const residentialForms = new Set([
      'SingleFamilyDetached',
      'SingleFamilySemiDetached',
      'TownhouseRowhouse',
      'Duplex',
      'Triplex',
      'Quadplex',
      'MultiFamily5Plus',
      'MultiFamilyMoreThan10',
      'MultiFamilyLessThan10',
      'ApartmentUnit',
      'Loft',
      'ManufacturedHomeOnLand',
      'ManufacturedHomeInPark',
      'Modular'
    ]);

    if (structureForm && residentialForms.has(structureForm)) {
      propertyUsageType = 'Residential';
      console.log('DEBUG: Fallback property_usage_type set to "Residential" based on structure_form');
    } else if (improvementType) {
      const normalizedImprovement = improvementType.replace(/^\d+\s*[-–]\s*/, '').trim();
      const lowerImp = normalizedImprovement.toLowerCase();
      console.log(`DEBUG: Fallback mapping property_usage_type from improvementType: "${normalizedImprovement}"`);

      if (lowerImp.includes('commercial') || lowerImp.includes('retail') || lowerImp.includes('store') || lowerImp.includes('shop') || lowerImp.includes('office')) {
        propertyUsageType = 'Commercial';
      } else if (lowerImp.includes('industrial') || lowerImp.includes('manufacturing') || lowerImp.includes('warehouse') || lowerImp.includes('plant')) {
        propertyUsageType = 'Industrial';
      } else if (lowerImp.includes('school')) {
        propertyUsageType = 'PublicSchool';
      } else if (lowerImp.includes('hospital')) {
        propertyUsageType = 'PublicHospital';
      } else if (lowerImp.includes('church') || lowerImp.includes('temple') || lowerImp.includes('mosque') || lowerImp.includes('synagogue')) {
        propertyUsageType = 'Church';
      } else if (lowerImp.includes('mobile home park') || lowerImp.includes('trailer park')) {
        propertyUsageType = 'MobileHomePark';
      } else if (lowerImp.includes('golf')) {
        propertyUsageType = 'GolfCourse';
      } else if (lowerImp.includes('hotel') || lowerImp.includes('motel') || lowerImp.includes('inn')) {
        propertyUsageType = 'Hotel';
      } else if (lowerImp.includes('restaurant') || lowerImp.includes('cafe') || lowerImp.includes('diner')) {
        propertyUsageType = 'Restaurant';
      } else if (lowerImp.includes('residential') || lowerImp.includes('single family') || lowerImp.includes('duplex') || lowerImp.includes('triplex') || lowerImp.includes('quadplex') || lowerImp.includes('apartment')) {
        propertyUsageType = 'Residential';
      }

      if (propertyUsageType) {
        console.log(`DEBUG: Fallback property_usage_type result = "${propertyUsageType}"`);
      }
    }
  }

  if (!structureForm) {
    structureForm = null;
  }

  // Ensure property_usage_type, build_status, and ownership_estate_type are never null
  // Set valid defaults from the allowed enum values
  if (!propertyUsageType) {
    // Default to 'Unknown' which is in the allowed enum list
    propertyUsageType = 'Unknown';
  }
  if (!buildStatus) {
    // Default to 'Improved' as the most common case
    buildStatus = 'Improved';
  }
  if (!ownershipEstateType) {
    // Default to 'FeeSimple' as the most common ownership type
    ownershipEstateType = 'FeeSimple';
  }

  const property = {
    parcel_identifier: parcelIdentifier || null,
    property_legal_description_text: legal || null,
    property_structure_built_year: yearBuilt || null,
    property_type: mappedPropertyType,
    subdivision: subdivision || null,
    structure_form: structureForm,
    property_usage_type: propertyUsageType,
    build_status: buildStatus,
    ownership_estate_type: ownershipEstateType,
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

  // Use unnormalized_address format - cannot have both parsed fields and unnormalized_address
  const unnormalizedAddr = unAddr && unAddr.full_address ? unAddr.full_address :
    (line1 && cityStateZip ? `${line1}, ${cityStateZip}` : null);

  // Address schema requires EITHER unnormalized_address OR normalized fields (oneOf constraint)
  // If we have unnormalized_address, only include it (no normalized fields)
  // If we don't have unnormalized_address, include normalized fields instead
  const address = {
    source_http_request: unAddr && unAddr.source_http_request ? unAddr.source_http_request : null,
    request_identifier: unAddr && unAddr.request_identifier ? unAddr.request_identifier : null,
    county_name: "Lee",
  };

  if (unnormalizedAddr) {
    // Use unnormalized format only
    address.unnormalized_address = unnormalizedAddr;
  } else {
    // Use normalized format with parsed fields
    // Only include fields that have values
    if (street_number) address.street_number = street_number;
    if (street_pre_directional_text) address.street_pre_directional_text = street_pre_directional_text;
    if (street_name) address.street_name = street_name;
    if (street_suffix_type) address.street_suffix_type = street_suffix_type;
    if (street_post_directional_text) address.street_post_directional_text = street_post_directional_text;
    if (city_name) address.city_name = city_name;
    if (state_code) address.state_code = state_code;
    if (postal_code) address.postal_code = postal_code;
  }

  return address;
}

/**
 * Extract a selected tax year from the Lee page header.
 *
 * @param {import('cheerio').CheerioAPI} $ - Loaded Lee parcel HTML document.
 * @returns {number|null} Selected tax year, or null when the page does not expose one.
 */
function extractSelectedTaxYear($) {
  const selectedOption = $('#selectTaxYear option[selected]').first();
  const selectedText = selectedOption.length ? cleanText(selectedOption.text()) : cleanText($('#selectTaxYear option').first().text());
  const selectedYear = selectedText.match(/\b(19|20)\d{2}\b/);
  if (selectedYear) return parseInt(selectedYear[0], 10);

  const selectedHref = selectedOption.attr('value') || $('#selectTaxYear option').first().attr('value') || '';
  const hrefYear = selectedHref.match(/[?&]TaxYear=((?:19|20)\d{2})\b/i);
  return hrefYear ? parseInt(hrefYear[1], 10) : null;
}

/**
 * Extract fixed-width NAL field labels from Lee's "Certified Roll Data" block.
 *
 * Lee currently renders assessed values in `#TaxRollDiv` as a continuous text
 * sequence (`F01: ... F02: ...`). Older code only handled the retired
 * `#valueGrid` table, so all tax rows were skipped even though the source HTML
 * had the complete certified roll values.
 *
 * @param {string} text - Normalized text from `#TaxRollDiv` or another certified-roll container.
 * @returns {Map<string, string>} Field map keyed as `F04`, `F08`, etc.
 */
function extractNalFields(text) {
  const fields = new Map();
  const normalized = cleanText(text);
  const fieldPattern = /F(\d{2}):\s*([\s\S]*?)(?=F\d{2}:|$)/g;
  for (const match of normalized.matchAll(fieldPattern)) {
    const fieldNumber = match[1];
    const rawValue = match[2];
    if (!fieldNumber || rawValue == null) continue;
    fields.set(`F${fieldNumber}`, cleanText(rawValue));
  }
  return fields;
}

/**
 * Read a numeric value from an extracted Lee NAL field.
 *
 * @param {Map<string, string>} fields - NAL field map.
 * @param {string} fieldName - Field name such as `F08`.
 * @returns {number|null} Parsed number, or null when the field is blank/non-numeric.
 */
function readNalNumber(fields, fieldName) {
  return parseNumber(fields.get(fieldName));
}

/**
 * Convert a Cheerio document into non-empty visible text lines.
 *
 * @param {import('cheerio').CheerioAPI} $ - Loaded HTML document.
 * @returns {string[]} Normalized text lines in page order.
 */
function extractTextLines($) {
  return $.root()
    .text()
    .split(/\r?\n/)
    .map((line) => cleanText(line))
    .filter((line) => line.length > 0);
}

/**
 * Test whether text looks like a Lee field-card/cost-card value summary.
 *
 * @param {string} text - Normalized document text.
 * @returns {boolean} True when the page exposes the cost-card summary table.
 */
function isLeeCostCardText(text) {
  return /LEE\s+COUNTY\s+PROPERTY\s+APPRAISER/i.test(text) && /VALUE\s+SUMMARY/i.test(text);
}

/**
 * Find the first local auxiliary HTML file that contains a Lee cost-card value summary.
 *
 * Detail captures are normalized to `input.html`. When the upstream extractor
 * also captures the linked field card, that auxiliary HTML is left in the
 * working directory and used only as a fallback for value/tax rows.
 *
 * @returns {{ name: string, html: string, sourceHttpRequest: object | null } | null} Cost-card HTML and provenance, or null.
 */
function findAuxiliaryCostCardHtml() {
  let entries = [];
  try {
    entries = fs.readdirSync(WORKING_DIR);
  } catch {
    return null;
  }

  for (const fileName of entries) {
    if (/\.html?$/i.test(fileName) === false) continue;
    if (fileName === 'input.html') continue;
    const filePath = path.join(WORKING_DIR, fileName);
    let html = '';
    try {
      html = fs.readFileSync(filePath, 'utf-8');
    } catch {
      continue;
    }
    if (isLeeCostCardText(cleanText(html))) {
      return {
        name: fileName,
        html,
        sourceHttpRequest: readAuxiliarySourceHttpRequest(fileName),
      };
    }
  }
  return null;
}

/**
 * Read an auxiliary source request sidecar for a captured Lee cost card.
 *
 * @param {string} htmlFileName - Auxiliary HTML file name.
 * @returns {object|null} Parsed source request object, or null when unavailable.
 */
function readAuxiliarySourceHttpRequest(htmlFileName) {
  const extension = path.extname(htmlFileName);
  const baseName = extension ? htmlFileName.slice(0, -extension.length) : htmlFileName;
  const candidateNames = [
    `${baseName}_source_http_request.json`,
    'cost_card_source_http_request.json',
  ];

  for (const candidateName of candidateNames) {
    const candidatePath = path.join(WORKING_DIR, candidateName);
    if (fs.existsSync(candidatePath) === false) continue;
    try {
      const parsed = readJSON(candidatePath);
      if (parsed !== null && typeof parsed === 'object' && Array.isArray(parsed) === false) {
        return parsed;
      }
    } catch {
      continue;
    }
  }
  return null;
}

/**
 * Read the last numeric value that follows a cost-card summary label.
 *
 * Lee cost cards render the `VALUE SUMMARY` as stacked text lines. Several
 * labels have two visible value columns; the rightmost/current numeric value is
 * the one we preserve. Asterisks are skipped because Lee uses them for
 * incomplete/not-applicable approaches.
 *
 * @param {string[]} lines - Visible text lines in page order.
 * @param {string} label - Summary label such as `MARKET VALUE`.
 * @returns {number|null} Parsed current value, or null when the label has no numeric value.
 */
function readCostCardSummaryNumber(lines, label) {
  const labelPattern = new RegExp(`^${label.replace(/\s+/g, '\\s+')}$`, 'i');
  const nextLabelPattern = /^(BUILDING COST VALUE|BUILDING EXTRA FEATURES|LAND EXTRA FEATURES|LAND VALUE|COST APPROACH VALUE|INCOME APPROACH VALUE|SALES APPROACH VALUE|MARKET VALUE)$/i;
  const startIndex = lines.findIndex((line) => labelPattern.test(line));
  if (startIndex === -1) return null;

  const values = [];
  for (let index = startIndex + 1; index < lines.length; index += 1) {
    const line = lines[index];
    if (nextLabelPattern.test(line)) break;
    const parsed = parseNumber(line);
    if (parsed !== null) values.push(parsed);
  }
  return values.length > 0 ? values[values.length - 1] : null;
}

/**
 * Extract the tax/value year from a Lee cost-card value summary.
 *
 * @param {string[]} lines - Visible text lines in page order.
 * @returns {number|null} Parsed four-digit year.
 */
function extractCostCardTaxYear(lines) {
  const currentValuesIndex = lines.findIndex((line) => /^Current Values$/i.test(line));
  if (currentValuesIndex !== -1) {
    for (let index = currentValuesIndex + 1; index < Math.min(lines.length, currentValuesIndex + 8); index += 1) {
      const yearMatch = lines[index].match(/^(19|20)\d{2}$/);
      if (yearMatch) return parseInt(lines[index], 10);
    }
  }

  const generatedYearLine = lines.find((line) => /\b(19|20)\d{2}\b/.test(line));
  const generatedYearMatch = generatedYearLine ? generatedYearLine.match(/\b(19|20)\d{2}\b/) : null;
  return generatedYearMatch ? parseInt(generatedYearMatch[0], 10) : null;
}

/**
 * Build a tax/value row from Lee's linked field-card/cost-card summary.
 *
 * Some Lee parcel-detail pages, especially inactive or split parcels, render
 * `Certified Roll Data not found for this parcel` while the linked field card
 * still exposes current value summary rows. This fallback promotes the current
 * market and land values and preserves the raw summary fields in the payload.
 *
 * @param {import('cheerio').CheerioAPI} $costCard - Loaded cost-card HTML document.
 * @param {string} sourceName - Source HTML filename used for provenance.
 * @param {object|null} [sourceHttpRequest] - Source request descriptor for the cost-card capture.
 * @returns {Array<object>} One fallback tax/value row, or an empty array.
 */
function extractTaxesFromCostCard($costCard, sourceName, sourceHttpRequest = null) {
  const normalizedText = cleanText($costCard.root().text());
  if (isLeeCostCardText(normalizedText) === false) return [];

  const lines = extractTextLines($costCard);
  const taxYear = extractCostCardTaxYear(lines);
  const buildingCostValue = readCostCardSummaryNumber(lines, 'BUILDING COST VALUE');
  const landExtraFeaturesValue = readCostCardSummaryNumber(lines, 'LAND EXTRA FEATURES');
  const landValue = readCostCardSummaryNumber(lines, 'LAND VALUE');
  const costApproachValue = readCostCardSummaryNumber(lines, 'COST APPROACH VALUE');
  const incomeApproachValue = readCostCardSummaryNumber(lines, 'INCOME APPROACH VALUE');
  const salesApproachValue = readCostCardSummaryNumber(lines, 'SALES APPROACH VALUE');
  const marketValue = readCostCardSummaryNumber(lines, 'MARKET VALUE') ?? incomeApproachValue ?? costApproachValue ?? salesApproachValue;
  const derivedBuildingValue = marketValue !== null && landValue !== null ? marketValue - landValue : null;
  const buildingValue = buildingCostValue ?? (derivedBuildingValue !== null && derivedBuildingValue > 0 ? Number(derivedBuildingValue.toFixed(2)) : null);

  if (taxYear === null && marketValue === null && landValue === null && buildingValue === null) {
    return [];
  }

  return [
    {
      tax_year: taxYear,
      property_assessed_value_amount: null,
      property_market_value_amount: marketValue,
      property_building_amount: buildingValue,
      property_land_amount: landValue,
      property_taxable_value_amount: null,
      land_extra_features_value_amount: landExtraFeaturesValue,
      cost_approach_value_amount: costApproachValue,
      income_approach_value_amount: incomeApproachValue,
      sales_approach_value_amount: salesApproachValue,
      monthly_tax_amount: null,
      period_end_date: null,
      period_start_date: null,
      cost_card_fields: {
        building_cost_value: buildingCostValue,
        land_extra_features_value: landExtraFeaturesValue,
        land_value: landValue,
        cost_approach_value: costApproachValue,
        income_approach_value: incomeApproachValue,
        sales_approach_value: salesApproachValue,
        market_value: marketValue,
      },
      source_http_request: sourceHttpRequest,
      source_cost_card_file_name: sourceName,
      source_extraction_method: 'lee_cost_card_value_summary',
    },
  ];
}

/**
 * Extract tax/value rows from an auxiliary Lee cost-card capture.
 *
 * @returns {Array<object>} Fallback value rows, or an empty array.
 */
function extractTaxesFromAuxiliaryCostCard() {
  const auxiliary = findAuxiliaryCostCardHtml();
  if (auxiliary === null) return [];
  return extractTaxesFromCostCard(cheerio.load(auxiliary.html), auxiliary.name, auxiliary.sourceHttpRequest);
}

/**
 * Extract the explicit "1st Year Building on Tax Roll" value when present.
 *
 * @param {import('cheerio').CheerioAPI} $ - Loaded Lee parcel HTML document.
 * @returns {number|null} Parsed year, or null when the label is absent.
 */
function extractFirstYearBuildingOnTaxRoll($) {
  let year = null;
  $('th').each((_, th) => {
    if (year !== null) return false;
    if (/1st\s+Year\s+Building\s+on\s+Tax\s+Roll/i.test(cleanText($(th).text())) === false) return;
    const value = cleanText($(th).closest('tr').find('td').first().text());
    const parsed = parseNumber(value);
    if (parsed !== null) year = parsed;
  });
  return year;
}

/**
 * Build tax rows from Lee's certified-roll NAL fields.
 *
 * The field mapping follows the Florida NAL roll layout exposed by Lee:
 * F04 assessment year, F08 total just value, F11/F12 school/non-school assessed
 * values, F13/F14 school/non-school taxable values, and F38 land value. The full
 * extracted field map is preserved on the output payload so fields not promoted
 * to typed columns are still available downstream in `source_payload`.
 *
 * @param {import('cheerio').CheerioAPI} $ - Loaded Lee parcel HTML document.
 * @returns {Array<object>} Tax rows for the selected certified roll year.
 */
function extractTaxesFromCertifiedRoll($) {
  const rollText = cleanText($('#TaxRollDiv').text() || $('.certifiedRollData').text());
  if (!rollText) return [];

  const fields = extractNalFields(rollText);
  if (fields.size === 0) return [];

  const taxYear = readNalNumber(fields, 'F04') || extractSelectedTaxYear($);
  const justValue = readNalNumber(fields, 'F08');
  const schoolAssessedValue = readNalNumber(fields, 'F11');
  const nonSchoolAssessedValue = readNalNumber(fields, 'F12');
  const schoolTaxableValue = readNalNumber(fields, 'F13');
  const nonSchoolTaxableValue = readNalNumber(fields, 'F14');
  const homesteadJustValue = readNalNumber(fields, 'F15');
  const agriculturalJustValue = readNalNumber(fields, 'F21');
  const landValue = readNalNumber(fields, 'F38');
  const firstYearBuildingOnTaxRoll = extractFirstYearBuildingOnTaxRoll($);
  const buildingValue = justValue !== null && landValue !== null ? justValue - landValue : null;

  if (taxYear === null && justValue === null && nonSchoolAssessedValue === null && schoolAssessedValue === null) {
    return [];
  }

  return [
    {
      tax_year: taxYear,
      property_assessed_value_amount: nonSchoolAssessedValue ?? schoolAssessedValue ?? null,
      property_market_value_amount: justValue,
      property_homestead_value_amount: homesteadJustValue,
      property_building_amount:
        buildingValue !== null && buildingValue > 0
          ? Number(buildingValue.toFixed(2))
          : null,
      property_land_amount: landValue,
      property_taxable_value_amount: nonSchoolTaxableValue ?? schoolTaxableValue ?? null,
      county_taxable_value_amount: nonSchoolTaxableValue,
      school_taxable_value_amount: schoolTaxableValue,
      agricultural_valuation_amount: agriculturalJustValue,
      building_depreciated_value_amount:
        buildingValue !== null && buildingValue > 0
          ? Number(buildingValue.toFixed(2))
          : null,
      first_year_building_on_tax_roll: firstYearBuildingOnTaxRoll,
      monthly_tax_amount: null,
      period_end_date: null,
      period_start_date: null,
      nal_fields: Object.fromEntries(fields.entries()),
      source_extraction_method: 'lee_certified_roll_nal',
    },
  ];
}

/**
 * Extract tax/value rows from Lee parcel HTML.
 *
 * @param {import('cheerio').CheerioAPI} $ - Loaded Lee parcel HTML document.
 * @returns {Array<object>} Tax rows derived from the value table or certified-roll fallback.
 */
function extractTaxes($) {
  const taxes = [];
  const currentPageCostCardTaxes = extractTaxesFromCostCard($, 'input.html');
  if (currentPageCostCardTaxes.length > 0) return currentPageCostCardTaxes;

  const grid = $('#valueGrid');
  if (!grid.length) {
    const certifiedRollTaxes = extractTaxesFromCertifiedRoll($);
    return certifiedRollTaxes.length > 0 ? certifiedRollTaxes : extractTaxesFromAuxiliaryCostCard();
  }
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
  if (taxes.length > 0) return taxes;
  const certifiedRollTaxes = extractTaxesFromCertifiedRoll($);
  return certifiedRollTaxes.length > 0 ? certifiedRollTaxes : extractTaxesFromAuxiliaryCostCard();
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

function parseCompletionDateFromYear(value) {
  if (!value) return null;
  const year = parseInt(String(value).trim(), 10);
  if (!Number.isNaN(year) && year >= 1800 && year <= 2100) {
    return `${year.toString().padStart(4, '0')}-01-01`;
  }
  return null;
}

const PROPERTY_IMPROVEMENT_TYPE_PATTERNS = [
  { type: 'LandscapeIrrigation', patterns: [/irrigation/i, /sprinkler/i, /landscape/i, /lawn/i] },
  { type: 'PoolSpaInstallation', patterns: [/pool/i, /spa/i, /jacuzzi/i, /hot tub/i] },
  { type: 'ScreenEnclosure', patterns: [/screen/i, /enclosure/i] },
  { type: 'ShutterAwning', patterns: [/shutter/i, /awning/i] },
  { type: 'Fencing', patterns: [/fence/i, /gate/i] },
  { type: 'DockAndShore', patterns: [/dock/i, /seawall/i, /pier/i, /boathouse/i, /bulkhead/i] },
  { type: 'Roofing', patterns: [/roof/i] },
  { type: 'Solar', patterns: [/solar/i, /photovoltaic/i] },
  { type: 'DrivewayPermit', patterns: [/driveway/i, /sidewalk/i, /paver/i, /paving/i, /culvert/i] },
  { type: 'ExteriorOpeningsAndFinishes', patterns: [/porch/i, /deck/i, /patio/i, /lanai/i, /balcony/i, /terrace/i, /gazebo/i, /pergola/i] },
  { type: 'MechanicalHVAC', patterns: [/hvac/i, /mechanical/i] },
  { type: 'Electrical', patterns: [/electrical/i] },
  { type: 'Plumbing', patterns: [/plumb/i] },
  { type: 'GasInstallation', patterns: [/gas/i] },
  { type: 'BuildingAddition', patterns: [/addition/i, /add-on/i] },
  { type: 'Demolition', patterns: [/demolition/i, /\bdemo\b/i, /remove/i] },
];

function mapImprovementTypeFromDescription(description, context) {
  const text = description.toLowerCase();
  for (const entry of PROPERTY_IMPROVEMENT_TYPE_PATTERNS) {
    if (entry.patterns.some((pattern) => pattern.test(text))) {
      return entry.type;
    }
  }
  if (context && /land/i.test(context)) {
    return 'LandscapeIrrigation';
  }
  return 'GeneralBuilding';
}

function normalizePermitUrl(url) {
  if (!url) return null;
  if (url.startsWith('http://') || url.startsWith('https://')) {
    return url;
  }
  if (url.startsWith('/')) {
    return `https://www.leepa.org${url}`;
  }
  return `https://www.leepa.org/${url.replace(/^\.\//, '')}`;
}

function extractPropertyImprovements($) {
  const improvements = [];
  const seen = new Set();

  const permitTable = $('#PermitDetails table.detailsTable');
  if (permitTable.length) {
    permitTable.find('tr').each((i, row) => {
      if (i === 0) return;

      const cells = $(row).find('td');
      if (cells.length < 3) return;

      const permitCell = $(cells[0]);
      const permitLink = permitCell.find('a');
      const permitNumber = cleanText(permitCell.text());
      const permitUrl = permitLink.length ? normalizePermitUrl(permitLink.attr('href')) : null;
      const permitTypeText = cleanText($(cells[1]).text());
      const permitDateText = cleanText($(cells[2]).text());

      if (!permitNumber) return;
      const uniqueKey = `permit:${permitNumber}`;
      if (seen.has(uniqueKey)) return;
      seen.add(uniqueKey);

      const improvementType = mapImprovementTypeFromDescription(permitTypeText, 'Permit');
      const completionDate = parseDateMMDDYYYY(permitDateText);

      improvements.push({
        improvementType: mapImprovementTypeFromDescription(permitTypeText, 'Permit') || 'MAPPING_NOT_AVAILABLE',
        completionDate: parseDateMMDDYYYY(permitDateText),
        permitNumber,
        sourceUrl: permitUrl,
        description: permitTypeText,
      });
    });
  }

  if (improvements.length) {
    return improvements;
  }

  $('table.appraisalAttributes').each((_, table) => {
    const $table = $(table);
    const subheaderCell = $table.find('th.subheader').first();
    if (!subheaderCell.length) return;

    const subheaderText = cleanText(subheaderCell.text());
    if (!subheaderText) return;
    if (!/features/i.test(subheaderText) && !/improvement/i.test(subheaderText)) return;

    $table.find('tr').each((__, row) => {
      const cells = $(row).find('td');
      if (cells.length < 2) return;

      const description = cleanText($(cells[0]).text());
      if (!description || !/[a-zA-Z]/.test(description)) return;

      const key = `${subheaderText}|${description}|${cleanText($(cells[1]).text())}`;
      if (seen.has(key)) return;

      const yearText = cleanText($(cells[1]).text());
      const improvementType = mapImprovementTypeFromDescription(description, subheaderText);
      const completionDate = parseCompletionDateFromYear(yearText);

      improvements.push({
        description,
        improvementType: improvementType || 'MAPPING_NOT_AVAILABLE',
        completionDate,
        sourceUrl: null,
      });
      seen.add(key);
    });
  });

  return improvements;
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
  // Read roof date from structure mapping script output
  let roofDate = null;
  try {
    const structureDataPath = path.join('owners', 'structure_data.json');
    if (fs.existsSync(structureDataPath)) {
      const structureData = readJSON(structureDataPath);
      const folioId = Object.keys(structureData)[0];
      if (folioId && structureData[folioId]) {
        roofDate = structureData[folioId].roof_date;
      }
    }
  } catch (e) {
    // Ignore errors, use null
  }

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
    roof_date: roofDate,
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

function normalizeDeedType(value) {
  if (!value) return 'Miscellaneous';
  
  const normalized = value.trim();
  const upper = normalized.toUpperCase();

  if (upper === 'XX' || upper === 'XX.' || upper.startsWith('XX ')) {
    return 'Miscellaneous';
  }

  const sanitized = normalized.replace(/\s+/g, ' ').trim();
  const tokens = sanitized.split(/[^0-9A-Za-z]+/).filter(Boolean);

  const saleQualCodeMap = {
    '01': 'Warranty Deed',
    '02': 'Warranty Deed',
    '03': 'Warranty Deed',
    '04': 'Warranty Deed',
    '05': 'Warranty Deed',
    '06': 'Quitclaim Deed',
    '07': "Sheriff's Deed",
    '08': "Trustee's Deed",
    '09': 'Personal Representative Deed',
    '10': 'Correction Deed',
    '11': 'Contract for Deed',
    '12': 'Quitclaim Deed',
    '13': 'Warranty Deed',
    '14': 'Special Warranty Deed',
    '15': 'Personal Representative Deed',
    '16': "Trustee's Deed",
    '17': 'Tax Deed',
    '18': "Sheriff's Deed",
    '19': "Administrator's Deed",
    '20': "Guardian's Deed",
  };

  const numericTokens = tokens
    .filter((token) => /^\d{1,2}$/.test(token))
    .map((token) => token.padStart(2, '0'));

  const codePriority = [
    '11', // Contract for Deed
    '14', // Special Warranty Deed
    '12', // Quitclaim Deed
    '06', // Quitclaim Deed
    '07', // Sheriff's Deed
    '08', // Trustee's Deed
    '09', // Personal Representative Deed
    '10', // Correction Deed
    '15', // Personal Representative Deed
    '16', // Trustee's Deed
    '17', // Tax Deed
    '18', // Sheriff's Deed
    '19', // Administrator's Deed
    '20', // Guardian's Deed
    '13', // Warranty Deed
    '05', // Warranty Deed
    '04', // Warranty Deed
    '03', // Warranty Deed
    '02', // Warranty Deed
    '01', // Warranty Deed
  ];

  for (const code of codePriority) {
    if (numericTokens.includes(code) && saleQualCodeMap[code]) {
      return saleQualCodeMap[code];
    }
  }

  const fallbackCode = numericTokens.find((code) => saleQualCodeMap[code]);
  if (fallbackCode) {
    return saleQualCodeMap[fallbackCode];
  }

  const leadingCodeMatch = sanitized.match(/^\d+\s*[-–:\/]\s*(.+)$/);
  const processed = leadingCodeMatch && leadingCodeMatch[1] ? leadingCodeMatch[1].trim() : sanitized;

  if (!/[a-zA-Z]/.test(processed)) {
    return 'Miscellaneous';
  }

  const lower = processed.toLowerCase();

  if (
    lower === 'not available' ||
    lower === 'n/a' ||
    lower === 'na' ||
    lower.includes('not available') ||
    lower.includes('not-applicable') ||
    lower.includes('unknown') ||
    lower.includes('unavailable')
  ) {
    return 'Miscellaneous';
  }

  const upperTokens = processed.toUpperCase().split(/[^A-Z0-9]+/).filter(Boolean);
  const tokenSet = new Set(upperTokens);
  const joinedTokens = upperTokens.join('');

  const hasToken = (...patterns) => patterns.some((pattern) => {
    const up = pattern.toUpperCase();
    return tokenSet.has(up) || joinedTokens.includes(up);
  });

  const hasAllTokens = (...patterns) => patterns.every((pattern) => {
    const up = pattern.toUpperCase();
    return tokenSet.has(up) || joinedTokens.includes(up);
  });

  // Instrument code / abbreviation mapping using sales qualification document codes
  if (hasAllTokens('SPECIAL', 'WARRANTY') || hasToken('SWD', 'SPWD', 'SPECWARR')) {
    return 'Special Warranty Deed';
  }
  if (hasToken('WD', 'WDEED', 'WARRANTYDEED', 'WARRANTY') || (hasToken('WAR') && hasToken('DEED') && !hasToken('SPECIAL'))) {
    return 'Warranty Deed';
  }
  if (hasToken('QUIT', 'QCD', 'QC', 'FEE SIMPLE', 'FEE-SIMPLE', 'FEE_SIMPLE')) {
    return 'Quitclaim Deed';
  }
  if (hasToken('GRANT', 'GD')) {
    return 'Grant Deed';
  }
  if (hasToken('BARGAIN', 'BSD', 'B&S')) {
    return 'Bargain and Sale Deed';
  }
  if (hasToken('LADY', 'LBD', 'LADYBIRD', 'ENHANCEDLIFEESTATE')) {
    return 'Lady Bird Deed';
  }
  if (hasToken('TRANSFER', 'TOD') || hasAllTokens('TRANSFER', 'DEATH')) {
    return 'Transfer on Death Deed';
  }
  if (hasToken('SHERIFF', 'SHD', 'SHRF', 'SD') && !hasToken('SPECIAL')) {
    return "Sheriff's Deed";
  }
  if (hasToken('TAX', 'TX', 'TAXDEED')) {
    return 'Tax Deed';
  }
  if (hasToken('TRUSTEE', 'TRUSTEES', 'TRUSTEES\'S', 'TRD') || (hasToken('TR') && hasToken('DEED') && !hasToken('TAX'))) {
    return "Trustee's Deed";
  }
  if (hasToken('PERSONAL', 'REPRESENTATIVE', 'PRD', 'PERSREP')) {
    return 'Personal Representative Deed';
  }
  if (hasToken('CORRECTION', 'CORR', 'CD') || hasAllTokens('CORRECTIVE', 'DEED')) {
    return 'Correction Deed';
  }
  if (hasToken('DEEDINLIEU', 'DIL') || hasAllTokens('DEED', 'LIEU')) {
    return 'Deed in Lieu of Foreclosure';
  }
  if (hasAllTokens('LIFE', 'ESTATE') || hasToken('LED', 'LIFEESTATE')) {
    return 'Life Estate Deed';
  }
  if (hasAllTokens('JOINT', 'TENANCY') || hasToken('JTD')) {
    return 'Joint Tenancy Deed';
  }
  if (hasAllTokens('TENANCY', 'COMMON') || hasToken('TICD', 'TIC')) {
    return 'Tenancy in Common Deed';
  }
  if (hasAllTokens('COMMUNITY', 'PROPERTY')) {
    return 'Community Property Deed';
  }
  if (hasToken('GIFT', 'GFD')) {
    return 'Gift Deed';
  }
  if (hasToken('INTERSPOUSAL', 'ITD', 'INTER-SPOUSAL')) {
    return 'Interspousal Transfer Deed';
  }
  if (hasToken('WILD')) {
    return 'Wild Deed';
  }
  if (hasAllTokens('SPECIAL', 'MASTER') || hasToken('SMD', 'SPM')) {
    return 'Special Master\'s Deed';
  }
  if (hasAllTokens('COURT', 'ORDER') || hasToken('COD')) {
    return 'Court Order Deed';
  }
  if (hasToken('CONTRACT', 'CFD', 'AGREEMENTFORDEED', 'LANDCONTRACT')) {
    return 'Contract for Deed';
  }
  if (hasAllTokens('QUIET', 'TITLE') || hasToken('QTD', 'QUIETTITLE')) {
    return 'Quiet Title Deed';
  }
  if (hasToken('ADMINISTRATOR', 'AD', 'ADMIN\'S')) {
    return 'Administrator\'s Deed';
  }
  if (hasToken('GUARDIAN', 'GRDN', 'GDN')) {
    return 'Guardian\'s Deed';
  }
  if (hasToken('RECEIVER', 'RCV', 'REC')) {
    return 'Receiver\'s Deed';
  }
  if (hasAllTokens('RIGHT', 'WAY') || hasToken('ROW', 'R/W')) {
    return 'Right of Way Deed';
  }
  if (hasAllTokens('VACATION', 'PLAT') || hasToken('VOP', 'VAC')) {
    return 'Vacation of Plat Deed';
  }
  if (hasToken('ASSIGNMENT', 'AOC')) {
    return 'Assignment of Contract';
  }
  if (hasToken('RELEASE', 'ROC')) {
    return 'Release of Contract';
  }

  // Map common variations to schema enum values
  const typeMap = {
    'warranty deed': 'Warranty Deed',
    'statutory warranty deed': 'Warranty Deed',
    'general warranty deed': 'Warranty Deed',
    'special warranty deed': 'Special Warranty Deed',
    'quitclaim deed': 'Quitclaim Deed',
    'quit claim deed': 'Quitclaim Deed',
    'fee simple': 'Quitclaim Deed', // Fee Simple is a form of quit claim
    'grant deed': 'Grant Deed',
    'bargain and sale deed': 'Bargain and Sale Deed',
    'bargain & sale deed': 'Bargain and Sale Deed',
    'lady bird deed': 'Lady Bird Deed',
    'transfer on death deed': 'Transfer on Death Deed',
    'sheriff\'s deed': 'Sheriff\'s Deed',
    'sheriffs deed': 'Sheriff\'s Deed',
    'tax deed': 'Tax Deed',
    'trustee\'s deed': 'Trustee\'s Deed',
    'trustees deed': 'Trustee\'s Deed',
    'personal representative deed': 'Personal Representative Deed',
    'personal representative\'s deed': 'Personal Representative Deed',
    'personal representatives deed': 'Personal Representative Deed',
    'correction deed': 'Correction Deed',
    'deed in lieu of foreclosure': 'Deed in Lieu of Foreclosure',
    'life estate deed': 'Life Estate Deed',
    'joint tenancy deed': 'Joint Tenancy Deed',
    'tenancy in common deed': 'Tenancy in Common Deed',
    'community property deed': 'Community Property Deed',
    'gift deed': 'Gift Deed',
    'interspousal transfer deed': 'Interspousal Transfer Deed',
    'wild deed': 'Wild Deed',
    'special master\'s deed': 'Special Master\'s Deed',
    'special masters deed': 'Special Master\'s Deed',
    'court order deed': 'Court Order Deed',
    'contract for deed': 'Contract for Deed',
    'agreement for deed': 'Contract for Deed',
    'quiet title deed': 'Quiet Title Deed',
    'certificate of title': 'Quiet Title Deed', // Certificate of Title is related to quiet title
    'administrator\'s deed': 'Administrator\'s Deed',
    'administrators deed': 'Administrator\'s Deed',
    'guardian\'s deed': 'Guardian\'s Deed',
    'guardians deed': 'Guardian\'s Deed',
    'receiver\'s deed': 'Receiver\'s Deed',
    'receivers deed': 'Receiver\'s Deed',
    'right of way deed': 'Right of Way Deed',
    'vacation of plat deed': 'Vacation of Plat Deed',
    'assignment of contract': 'Assignment of Contract',
    'release of contract': 'Release of Contract',
    'notice': 'Miscellaneous', // Notice/Development Order - not a deed type
    'development order': 'Miscellaneous',
    'do': 'Miscellaneous',
    'ldo': 'Miscellaneous',
    'limited development order': 'Miscellaneous',
  };
  
  // Check for exact match
  if (typeMap[lower]) {
    return typeMap[lower];
  }
  
  // Check for partial matches
  for (const [key, enumValue] of Object.entries(typeMap)) {
    if (lower.includes(key) || key.includes(lower)) {
      return enumValue;
    }
  }
  
  // Check for keywords that indicate specific deed types
  if (lower.includes('warranty') && !lower.includes('special')) {
    return 'Warranty Deed';
  }
  if (lower.includes('special') && lower.includes('warranty')) {
    return 'Special Warranty Deed';
  }
  if (lower.includes('quit') || lower.includes('quitclaim') || lower.includes('fee simple')) {
    return 'Quitclaim Deed';
  }
  if (lower.includes('trustee')) {
    return "Trustee's Deed";
  }
  if (lower.includes('personal representative') || lower.includes('representative')) {
    return 'Personal Representative Deed';
  }
  if (lower.includes('contract') || lower.includes('agreement')) {
    return 'Contract for Deed';
  }
  if (lower.includes('tax')) {
    return 'Tax Deed';
  }
  if (lower.includes('correction')) {
    return 'Correction Deed';
  }
  if (lower.includes('gift')) {
    return 'Gift Deed';
  }
  if (lower.includes('administrator')) {
    return "Administrator's Deed";
  }
  if (lower.includes('guardian')) {
    return "Guardian's Deed";
  }
  if (lower.includes('receiver')) {
    return "Receiver's Deed";
  }
  if (lower.includes('quiet') && lower.includes('title')) {
    return 'Quiet Title Deed';
  }
  if (lower.includes('special master')) {
    return "Special Master's Deed";
  }
  if (lower.includes('deed in lieu')) {
    return 'Deed in Lieu of Foreclosure';
  }

  // If no match, return "Miscellaneous" for unknown deed types (valid enum value)
  return 'Miscellaneous';
}

function normalizeDeedUrl(url) {
  if (!url) return null;
  let absolute = url;
  if (url.startsWith('/')) {
    absolute = `https://or.leeclerk.org${url}`;
  } else if (url.startsWith('../')) {
    absolute = `https://www.leepa.org/${url.replace(/^\.\//, '').replace(/^\.\.\//, '')}`;
  } else if (!url.startsWith('http://') && !url.startsWith('https://')) {
    absolute = `https://or.leeclerk.org/${url.replace(/^\.\//, '')}`;
  }

  try {
    const urlObj = new URL(absolute);
    if (!urlObj.search) {
      return `${urlObj.origin}${urlObj.pathname}`;
    }
    const trimmedPath = urlObj.pathname.replace(/\/$/, '');
    const encodedSearch = encodeURIComponent(urlObj.search);
    return `${urlObj.origin}${trimmedPath}/${encodedSearch}`;
  } catch (_err) {
    return absolute;
  }
}

function extractDeeds($, requestIdentifier) {
  const deeds = [];
  const deedFileUrls = []; // Store file URLs for each deed
  const deedSourceRequests = [];
  
  // Extract from SalesDetails table
  // Table structure: Sale Price | Date | Clerk File Number (CFN) | Type | Notes | Vacant/Improved
  const salesBox = $('#SalesDetails');
  const salesTable = salesBox.find('table.detailsTable').first();
  
  console.log('extractDeeds: SalesDetails box found:', salesBox.length > 0);
  console.log('extractDeeds: Table found:', salesTable.length > 0);
  
  if (salesTable.length) {
    const rows = salesTable.find('tr');
    console.log('extractDeeds: Total rows in table:', rows.length);

    let headerIndexes = {
      price: null,
      date: null,
      cfn: null,
      type: null,
      notes: null,
    };

    rows.each((i, tr) => {
      const $row = $(tr);

      if (i === 0) {
        const headers = $row
          .find('th')
          .map((j, th) => cleanText($(th).text()).toLowerCase())
          .get();
        console.log('extractDeeds: Header row:', headers.join(' | '));

        headers.forEach((headerText, idx) => {
          if (headerText.includes('sale price')) headerIndexes.price = idx;
          if (headerText === 'date') headerIndexes.date = idx;
          if (headerText.includes('clerk') || headerText.includes('file')) headerIndexes.cfn = idx;
          if (headerText === 'type') headerIndexes.type = idx;
          if (headerText.includes('notes')) headerIndexes.notes = idx;
        });
        return;
      }

      const tds = $row.find('td');
      if (!tds.length) {
        console.log(`extractDeeds: Row ${i} has no <td> elements`);
        return;
      }

      const cfnIdx = headerIndexes.cfn !== null ? headerIndexes.cfn : 2;
      const typeIdx = headerIndexes.type !== null ? headerIndexes.type : 3;

      if (tds.length <= cfnIdx) {
        console.log(`extractDeeds: Row ${i} missing CFN column (index ${cfnIdx})`);
        return;
      }

      const cfnCell = $(tds[cfnIdx]);
      const cfnLink = cfnCell.find('a');
      const cfnText = cleanText(cfnCell.text()).trim();
      const cfnUrl = cfnLink.length ? cfnLink.attr('href') : null;

      let deedTypeRaw = null;
      if (typeIdx !== null && typeIdx < tds.length) {
        const typeCell = $(tds[typeIdx]);
        const typeLink = typeCell.find('a');
        const typeLinkText = typeLink.length ? cleanText(typeLink.text()).trim() : null;
        const typeText = cleanText(typeCell.text()).trim();
        deedTypeRaw = typeLinkText || typeText || null;
        console.log(`extractDeeds: Row ${i} - Type cell text: "${typeText}"`);
      }

      console.log(`extractDeeds: Row ${i} - CFN text: "${cfnText}", URL: ${cfnUrl}, Type: "${deedTypeRaw}"`);

      if (!cfnText || !cfnText.length) {
        console.log(`extractDeeds: Row ${i} skipped - missing CFN text`);
        return;
      }

      const deedType = normalizeDeedType(deedTypeRaw);
      console.log(`extractDeeds: Row ${i} - Normalized deed_type: "${deedType}"`);

      const finalDeedType = deedType && deedType !== null ? deedType : 'Miscellaneous';
      const safeDeedType = finalDeedType || 'Miscellaneous';

      const deed = {
        request_identifier: requestIdentifier,
        deed_type: safeDeedType,
      };

      // Only add instrument_number if we have a value
      if (cfnText && cfnText.trim().length > 0) {
        deed.instrument_number = cfnText.trim();
      }

      // Try to extract book, page, volume from URL or HTML if available
      // Check URL parameters first
      let book = null;
      let page = null;
      let volume = null;
      
      if (cfnUrl) {
        // Try to extract from URL parameters (e.g., ?book=123&page=456&volume=789)
        const urlMatch = cfnUrl.match(/[?&]book=([^&]+)/i);
        if (urlMatch) book = urlMatch[1].trim();
        
        const pageMatch = cfnUrl.match(/[?&]page=([^&]+)/i);
        if (pageMatch) page = pageMatch[1].trim();
        
        const volumeMatch = cfnUrl.match(/[?&]volume=([^&]+)/i);
        if (volumeMatch) volume = volumeMatch[1].trim();
      }
      
      // Check if we can extract from the CFN cell or surrounding HTML
      // Some systems encode book/page in the link text or data attributes
      if (!book && !page && !volume && cfnCell.length) {
        // Check data attributes
        const dataBook = cfnCell.attr('data-book') || cfnLink.attr('data-book');
        const dataPage = cfnCell.attr('data-page') || cfnLink.attr('data-page');
        const dataVolume = cfnCell.attr('data-volume') || cfnLink.attr('data-volume');
        
        if (dataBook) book = cleanText(dataBook).trim();
        if (dataPage) page = cleanText(dataPage).trim();
        if (dataVolume) volume = cleanText(dataVolume).trim();
        
        // Check if CFN text contains book/page/volume info (e.g., "CFN: 12345 Book: 100 Page: 200")
        const cfnFullText = cleanText(cfnCell.text()).trim();
        const bookMatch = cfnFullText.match(/book[:\s]+(\d+)/i);
        const pageMatch = cfnFullText.match(/page[:\s]+(\d+)/i);
        const volumeMatch = cfnFullText.match(/volume[:\s]+(\d+)/i);
        
        if (bookMatch && !book) book = bookMatch[1].trim();
        if (pageMatch && !page) page = pageMatch[1].trim();
        if (volumeMatch && !volume) volume = volumeMatch[1].trim();
      }
      
      // Only add book, page, volume if we found values
      if (book && book.length > 0) {
        deed.book = book;
      }
      if (page && page.length > 0) {
        deed.page = page;
      }
      if (volume && volume.length > 0) {
        deed.volume = volume;
      }

      // Double-check before pushing
      if (!deed.deed_type || deed.deed_type === null) {
        deed.deed_type = 'Miscellaneous';
        console.log(`extractDeeds: Row ${i} - Force set deed_type to "Miscellaneous" for safety`);
      }

      deeds.push(deed);
      
      // Store the file URL if available
      if (cfnUrl) {
        // Make URL absolute if it's relative
        const fullUrl = normalizeDeedUrl(cfnUrl);
        deedFileUrls.push(fullUrl);
        deedSourceRequests.push({
          url: fullUrl,
          method: 'GET',
        });
      } else {
        deedFileUrls.push(null);
        deedSourceRequests.push(null);
      }
    });
  }
  
  // Remove duplicates based on instrument_number
  const seen = new Set();
  const uniqueDeeds = [];
  const uniqueFileUrls = [];
  const uniqueSourceRequests = [];
  
  deeds.forEach((deed, idx) => {
    if (deed.instrument_number && !seen.has(deed.instrument_number)) {
      seen.add(deed.instrument_number);
      uniqueDeeds.push(deed);
      uniqueFileUrls.push(deedFileUrls[idx]);
      uniqueSourceRequests.push(deedSourceRequests[idx]);
    }
  });
  
  // Return both deeds and file URLs
  return {
    deeds: uniqueDeeds,
    fileUrls: uniqueFileUrls,
    sourceRequests: uniqueSourceRequests,
  };
}

function main() {
  console.log('Script started successfully');
  const dataDir = path.join('data');
  ensureDir(dataDir);

  // Read property_seed.json for request_identifier
  let seedData = null;
  const seedPath = 'property_seed.json';
  if (fs.existsSync(seedPath)) {
    seedData = readJSON(seedPath);
  }

  const html = fs.readFileSync('input.html', 'utf-8');
  const $ = cheerio.load(html);
  const unAddr = fs.existsSync('unnormalized_address.json')
    ? readJSON('unnormalized_address.json')
    : null;

  // Property - extract first to get parcel_identifier
  const property = extractProperty($, seedData);
  writeJSON(path.join(dataDir, 'property.json'), property);

  // 1. ALWAYS create point-based geometry for address (latitude/longitude)
  const parcelIdentifier = property.parcel_identifier || (seedData ? seedData.request_identifier : null);

  // Extract lat/long from HTML or unnormalized_address
  let latitude = null;
  let longitude = null;

  // Try to get from unnormalized_address first
  if (unAddr && (unAddr.latitude !== undefined || unAddr.longitude !== undefined)) {
    latitude = unAddr.latitude ?? null;
    longitude = unAddr.longitude ?? null;
  }

  // If not found, try to extract from HTML
  if (latitude === null && longitude === null) {
    $('table.appraisalDetailsLocation')
      .find('tr')
      .each((i, tr) => {
        const headers = $(tr).find('th');
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
  }

  const geometry = {
    source_http_request: seedData && seedData.source_http_request ? seedData.source_http_request : {
      method: "GET",
      url: ""
    },
    request_identifier: parcelIdentifier || "",
    latitude: latitude ?? null,
    longitude: longitude ?? null
  };
  writeJSON(path.join(dataDir, "geometry.json"), geometry);
  console.log(`✓ Created geometry.json with latitude: ${latitude}, longitude: ${longitude}`);

  // 2. Create parcel polygon geometries from CSV (if available)
  const geometryCsv = loadGeometryCsvContent();
  if (geometryCsv) {
    try {
      const instances = createGeometryInstances(geometryCsv);
      if (instances.length) {
        createParcelGeometries(instances);
        console.log(`✓ Created ${instances.length} geometry_parcel_<index>.json files from CSV`);
      }
    } catch (err) {
      console.warn(`Unable to build parcel geometry from CSV: ${err.message}`);
    }

    // 3. Create layout/building polygon geometries from CSV (if available)
    try {
      createLayoutGeometries(geometryCsv);
      console.log(`✓ Created layout geometry files from CSV`);
    } catch (err) {
      console.warn(`Unable to build layout geometry from CSV: ${err.message}`);
    }
  }

  // Create parcel.json
  if (parcelIdentifier) {
    const parcel = {
      source_http_request: seedData && seedData.source_http_request ? seedData.source_http_request : {
        method: "GET",
        url: ""
      },
      request_identifier: seedData ? seedData.request_identifier : parcelIdentifier,
      parcel_identifier: parcelIdentifier,
    };
    writeJSON(path.join(dataDir, 'parcel.json'), parcel);
    console.log(`✓ Created parcel.json with parcel_identifier: ${parcelIdentifier}`);
  }

  const propertyImprovements = extractPropertyImprovements($);
  if (propertyImprovements.length) {
    let improvementCount = 0;
    propertyImprovements.forEach((imp, idx) => {
      if (!imp.sourceUrl) {
        return;
      }
      const improvementIndex = idx + 1;
      const improvementFile = path.join(dataDir, `property_improvement_${improvementIndex}.json`);

      const improvementData = {
        improvement_type: imp.improvementType || 'MAPPING_NOT_AVAILABLE',
        improvement_action: 'Other',
        improvement_status: 'Completed',
      };

      if (imp.completionDate) {
        improvementData.completion_date = imp.completionDate;
      }

      if (imp.permitNumber) {
        improvementData.permit_number = imp.permitNumber;
      }

      if (seedData && seedData.request_identifier) {
        improvementData.request_identifier = seedData.request_identifier;
      }

      // Do not generate source_http_request - it will be populated by the process
      improvementData.permit_required = true;
      improvementData.contractor_type = 'Unknown';

      writeJSON(improvementFile, improvementData);
      writeJSON(path.join(dataDir, `relationship_property_property_improvement_${improvementIndex}.json`), {
        from: { '/': './property.json' },
        to: { '/': `./property_improvement_${improvementIndex}.json` },
      });
      improvementCount += 1;
    });
    if (improvementCount > 0) {
      console.log(`✓ Extracted ${improvementCount} property improvement record(s)`);
    }
  }

  // Address
  const address = extractAddress($, unAddr);
  writeJSON(path.join(dataDir, 'address.json'), address);

  // Create relationship between address and geometry
  const relAddressGeometry = {
    from: { "/": "./address.json" },
    to: { "/": "./geometry.json" }
  };
  writeJSON(path.join(dataDir, "relationship_address_has_geometry.json"), relAddressGeometry);
  console.log(`✓ Created relationship_address_has_geometry.json`);

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

  // Deeds
  const requestId = seedData ? seedData.request_identifier : null;
  const deedResult = extractDeeds($, requestId);
  const deedsList = deedResult.deeds;
  const deedFileUrls = deedResult.fileUrls;
  const deedSourceRequests = deedResult.sourceRequests || [];
  
  deedsList.forEach((d, idx) => {
    // Ensure deed_type is never null - use "Miscellaneous" as fallback for unknown deed types
    // This is a safety check - normalizeDeedType should already handle this, but ensure it here too
    if (!d.deed_type || d.deed_type === null || d.deed_type === undefined) {
      d.deed_type = 'Miscellaneous';
      console.log(`✓ Set deed_type to "Miscellaneous" for deed ${idx + 1} (instrument_number: ${d.instrument_number})`);
    }
    // Add source_http_request if available from seed data
    if (deedSourceRequests[idx]) {
      d.source_http_request = deedSourceRequests[idx];
    } else if (seedData && seedData.source_http_request) {
      d.source_http_request = seedData.source_http_request;
    }
    writeJSON(path.join(dataDir, `deed_${idx + 1}.json`), d);
  });
  if (deedsList.length > 0) {
    console.log(`✓ Created ${deedsList.length} deed file(s)`);
  }
  
  // Create file entities for deed links
  deedFileUrls.forEach((url, idx) => {
    if (url && deedsList[idx]) {
      const file = {
        request_identifier: requestId,
        document_type: 'Title', // Use "Title" per Elephant MCP schema
        name: `Deed Document ${deedsList[idx].instrument_number || idx + 1}`,
        original_url: url,
      };
      // Only include file_format and ipfs_url if they have values
      // (not including null values per schema)
      writeJSON(path.join(dataDir, `file_${idx + 1}.json`), file);
    }
  });
  if (deedFileUrls.filter(url => url).length > 0) {
    console.log(`✓ Created ${deedFileUrls.filter(url => url).length} file entity/entities for deed links`);
  }

  // Flood
  const flood = extractFlood($);
  writeJSON(path.join(dataDir, 'flood_storm_information.json'), flood);

  // Utilities from owners/utilities_data.json (now supports multiple buildings)
  let utilitiesByBuilding = null;
  if (fs.existsSync(path.join('owners', 'utilities_data.json'))) {
    const utilitiesData = readJSON(path.join('owners', 'utilities_data.json'));
    const key = Object.keys(utilitiesData).find((k) => /property_/.test(k));
    if (key && utilitiesData[key]) {
      // Check if it's the new format (object with building numbers) or old format (single utility)
      if (typeof utilitiesData[key] === 'object' && !Array.isArray(utilitiesData[key]) && utilitiesData[key].constructor === Object) {
        // New format: { 1: utility, 2: utility, ... }
        utilitiesByBuilding = utilitiesData[key];
      } else {
        // Old format: single utility object
        utilitiesByBuilding = { 1: utilitiesData[key] };
      }
    }
  }
  
  // If no utility data from mapping script, create default utility for building 1
  if (!utilitiesByBuilding) {
    const hasGarbage = $('#GarbageDetails').length > 0;
    utilitiesByBuilding = {
      1: {
        cooling_system_type: null,
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
      }
    };
  }
  
  // Create utility files per building
  const utilityBuildingNumbers = Object.keys(utilitiesByBuilding).map(Number).sort((a, b) => a - b);
  utilityBuildingNumbers.forEach((buildingNum) => {
    const utility = utilitiesByBuilding[buildingNum];
    const utilityFile = `utility_${buildingNum}.json`;
    writeJSON(path.join(dataDir, utilityFile), {
      cooling_system_type: utility.cooling_system_type ?? null,
      heating_system_type: utility.heating_system_type ?? null,
      public_utility_type: utility.public_utility_type ?? null,
      sewer_type: utility.sewer_type ?? null,
      water_source_type: utility.water_source_type ?? null,
      plumbing_system_type: utility.plumbing_system_type ?? null,
      plumbing_system_type_other_description: utility.plumbing_system_type_other_description ?? null,
      electrical_panel_capacity: utility.electrical_panel_capacity ?? null,
      electrical_wiring_type: utility.electrical_wiring_type ?? null,
      hvac_condensing_unit_present: utility.hvac_condensing_unit_present ?? null,
      electrical_wiring_type_other_description: utility.electrical_wiring_type_other_description ?? null,
      solar_panel_present: !!utility.solar_panel_present,
      solar_panel_type: utility.solar_panel_type ?? null,
      solar_panel_type_other_description: utility.solar_panel_type_other_description ?? null,
      smart_home_features: utility.smart_home_features ?? null,
      smart_home_features_other_description: utility.smart_home_features_other_description ?? null,
      hvac_unit_condition: utility.hvac_unit_condition ?? null,
      solar_inverter_visible: !!utility.solar_inverter_visible,
      hvac_unit_issues: utility.hvac_unit_issues ?? null,
    });
    console.log(`✓ Created ${utilityFile} for building ${buildingNum}`);
  });

  // Check HTML for building references to create building layout
  let hasBuilding = false;
  const htmlText = $('body').text().toLowerCase();
  if (/building|structure|dwelling|residence|house/i.test(htmlText)) {
    hasBuilding = true;
  }
  
  // Layouts from owners/layout_data.json
  let layoutCount = 0;
  if (fs.existsSync(path.join('owners', 'layout_data.json'))) {
    const layoutData = readJSON(path.join('owners', 'layout_data.json'));
    const key = Object.keys(layoutData).find((k) => /property_/.test(k));
    if (key && layoutData[key] && Array.isArray(layoutData[key].layouts)) {
      layoutData[key].layouts.forEach((lay, idx) => {
        layoutCount++;
        // Generate space_type_index: format is "spaceTypeIndex.spaceIndex" or just "spaceIndex"
        // Pattern must be: ^\d+(\.\d+)?(\.\d+)?$
        let spaceTypeIndex = null;
        if (lay.space_index !== null && lay.space_index !== undefined) {
          // If space_index is provided, use it as the base
          spaceTypeIndex = String(lay.space_index);
        } else if (idx !== null && idx !== undefined) {
          // Fallback to array index + 1
          spaceTypeIndex = String(idx + 1);
        }
        
        const out = {
          space_type: normalizeSpaceType(lay.space_type),
          space_index: lay.space_index ?? null,
          space_type_index: spaceTypeIndex,
          flooring_material_type: lay.flooring_material_type ?? null,
          size_square_feet: lay.size_square_feet ?? null,
          floor_level: lay.floor_level ?? null,
          has_windows: lay.has_windows ?? null,
          window_design_type: lay.window_design_type ?? null,
          window_material_type: lay.window_material_type ?? null,
          window_treatment_type: lay.window_treatment_type ?? null,
          is_finished: lay.is_finished !== null && lay.is_finished !== undefined ? Boolean(lay.is_finished) : true,
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
        // Add building_number if present in layout data
        if (lay.building_number !== null && lay.building_number !== undefined) {
          out.building_number = lay.building_number;
        }
        // Add area fields if present (for Building space_type layouts)
        if (lay.total_area_sq_ft !== null && lay.total_area_sq_ft !== undefined) {
          out.total_area_sq_ft = lay.total_area_sq_ft;
        }
        if (lay.area_under_air_sq_ft !== null && lay.area_under_air_sq_ft !== undefined) {
          out.area_under_air_sq_ft = lay.area_under_air_sq_ft;
        }
        if (lay.heated_area_sq_ft !== null && lay.heated_area_sq_ft !== undefined) {
          out.heated_area_sq_ft = lay.heated_area_sq_ft;
        }
        if (lay.livable_area_sq_ft !== null && lay.livable_area_sq_ft !== undefined) {
          out.livable_area_sq_ft = lay.livable_area_sq_ft;
        }
        if (lay.built_year !== null && lay.built_year !== undefined) {
          out.built_year = lay.built_year;
        }
        writeJSON(path.join(dataDir, `layout_${idx + 1}.json`), out);
      });
    }
  }
  
  // Get layout files list (needed for building layout detection)
  const dataFiles = fs.readdirSync(dataDir);
  const layoutFiles = dataFiles.filter((f) => /^layout_\d+\.json$/.test(f)).sort((a, b) => {
    const numA = parseInt(a.match(/\d+/)[0], 10);
    const numB = parseInt(b.match(/\d+/)[0], 10);
    return numA - numB;
  });
  
  // Building layouts are now created by layoutMapping.js with building_number
  // Check if any Building space_type layouts exist from layout_data.json
  let buildingLayoutFiles = [];
  layoutFiles.forEach((layoutFile) => {
    const layoutPath = path.join(dataDir, layoutFile);
    if (fs.existsSync(layoutPath)) {
      const layoutData = JSON.parse(fs.readFileSync(layoutPath, 'utf-8'));
      if (layoutData.space_type === 'Building') {
        buildingLayoutFiles.push(layoutFile);
      }
    }
  });
  
  // Legacy: Create building layout if building is detected but no building layouts exist
  // This is for backward compatibility with old HTML that doesn't have building layouts
  let buildingLayoutFile = null;
  if (hasBuilding && buildingLayoutFiles.length === 0) {
    const buildingLayout = {
      space_type: 'Building',
      space_index: layoutCount + 1,
      space_type_index: String(layoutCount + 1),
      building_number: 1, // Default to building 1
      flooring_material_type: null,
      size_square_feet: null,
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
    buildingLayoutFile = `layout_${layoutCount + 1}.json`;
    writeJSON(path.join(dataDir, buildingLayoutFile), buildingLayout);
    buildingLayoutFiles.push(buildingLayoutFile);
    console.log('✓ Created building layout (legacy fallback)');
  }

  // Owners from owners/owner_data.json (single type only). Prefer company.
  let salesFiles = [];
  // Note: dataFiles is already defined earlier
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

  // Structure from owners/structure_data.json (now supports multiple buildings)
  // If structure_data.json exists, use it (it has structuresByBuilding)
  // Otherwise, fallback to extractStructure for single building
  let structuresByBuilding = null;
  if (fs.existsSync(path.join('owners', 'structure_data.json'))) {
    const structureData = readJSON(path.join('owners', 'structure_data.json'));
    const key = Object.keys(structureData).find((k) => /property_/.test(k));
    if (key && structureData[key]) {
      // Check if it's the new format (object with building numbers) or old format (single structure)
      if (typeof structureData[key] === 'object' && !Array.isArray(structureData[key]) && structureData[key].constructor === Object) {
        // New format: { 1: structure, 2: structure, ... }
        structuresByBuilding = structureData[key];
      } else {
        // Old format: single structure object
        structuresByBuilding = { 1: structureData[key] };
      }
    }
  }
  
  // If no structure data from mapping script, use fallback extractStructure
  if (!structuresByBuilding) {
    const structure = extractStructure($);
    structuresByBuilding = { 1: structure };
  }
  
  // Create structure files per building
  const buildingNumbers = Object.keys(structuresByBuilding).map(Number).sort((a, b) => a - b);
  buildingNumbers.forEach((buildingNum) => {
    const structure = structuresByBuilding[buildingNum];
    const structureFile = `structure_${buildingNum}.json`;
    writeJSON(path.join(dataDir, structureFile), structure);
    console.log(`✓ Created ${structureFile} for building ${buildingNum}`);
  });

  // Lot
  const lot = extractLot($);
  writeJSON(path.join(dataDir, 'lot.json'), lot);

  // Property relationships - County schema requires these
  const propertyPath = path.join(dataDir, 'property.json');
  const addressPath = path.join(dataDir, 'address.json');
  const lotPath = path.join(dataDir, 'lot.json');
  const floodPath = path.join(dataDir, 'flood_storm_information.json');
  // structurePath and utilityPath are now per-building, defined later
  
  // property_has_address (singleton)
  if (fs.existsSync(propertyPath) && fs.existsSync(addressPath)) {
    writeJSON(path.join(dataDir, 'relationship_property_address.json'), {
      from: { '/': './property.json' },
      to: { '/': './address.json' },
    });
    console.log('✓ Created relationship_property_address.json');
  }
  
  // property_has_lot (singleton)
  if (fs.existsSync(propertyPath) && fs.existsSync(lotPath)) {
    writeJSON(path.join(dataDir, 'relationship_property_lot.json'), {
      from: { '/': './property.json' },
      to: { '/': './lot.json' },
    });
    console.log('✓ Created relationship_property_lot.json');
  }
  
  // property_has_tax (array)
  const taxFiles = dataFiles.filter((f) => /^tax_\d+\.json$/.test(f)).sort();
  taxFiles.forEach((taxFile, idx) => {
    writeJSON(path.join(dataDir, `relationship_property_tax_${idx + 1}.json`), {
      from: { '/': './property.json' },
      to: { '/': `./${taxFile}` },
    });
  });
  if (taxFiles.length > 0) {
    console.log(`✓ Created ${taxFiles.length} property_tax relationship(s)`);
  }
  
  // property_has_sales_history (array)
  salesFiles.forEach((salesFile, idx) => {
    writeJSON(path.join(dataDir, `relationship_property_sales_${idx + 1}.json`), {
      from: { '/': './property.json' },
      to: { '/': `./${salesFile}` },
    });
  });
  if (salesFiles.length > 0) {
    console.log(`✓ Created ${salesFiles.length} property_sales relationship(s)`);
  }
  
  // sales_history_has_deed (array) - link sales to deeds
  const deedFiles = dataFiles.filter((f) => /^deed_\d+\.json$/.test(f)).sort((a, b) => {
    const ai = parseInt(a.match(/(\d+)/)[1], 10);
    const bi = parseInt(b.match(/(\d+)/)[1], 10);
    return ai - bi;
  });
  
  // Link each sale to corresponding deed (1:1 mapping by index)
  salesFiles.forEach((salesFile, idx) => {
    const deedFile = deedFiles[idx];
    if (deedFile) {
      writeJSON(path.join(dataDir, `relationship_sales_deed_${idx + 1}.json`), {
        from: { '/': `./${salesFile}` },
        to: { '/': `./${deedFile}` },
      });
    }
  });
  if (deedFiles.length > 0 && salesFiles.length > 0) {
    console.log(`✓ Created ${Math.min(deedFiles.length, salesFiles.length)} sales_deed relationship(s)`);
  }
  
  // deed_has_file (array) - link deeds to files if file references exist
  const fileFiles = dataFiles.filter((f) => /^file_\d+\.json$/.test(f)).sort((a, b) => {
    const ai = parseInt(a.match(/(\d+)/)[1], 10);
    const bi = parseInt(b.match(/(\d+)/)[1], 10);
    return ai - bi;
  });
  
  // Link each deed to corresponding file (1:1 mapping by index)
  // Only create relationships for files that were created for deeds (first N files)
  const deedFileCount = deedFileUrls.filter(url => url).length;
  deedFiles.forEach((deedFile, idx) => {
    // Only link if we have a corresponding file (created for this deed)
    if (idx < deedFileCount) {
      const fileFile = fileFiles[idx];
      if (fileFile) {
        writeJSON(path.join(dataDir, `relationship_deed_file_${idx + 1}.json`), {
          from: { '/': `./${deedFile}` },
          to: { '/': `./${fileFile}` },
        });
      }
    }
  });
  if (deedFiles.length > 0 && fileFiles.length > 0 && deedFileCount > 0) {
    console.log(`✓ Created ${Math.min(deedFiles.length, deedFileCount)} deed_file relationship(s)`);
  }
  
  // property_has_layout (array) - include building layout if created
  // Note: layoutFiles is already defined earlier, just use it here
  
  // If building layout was created, ensure it's in the list (it should be, but double-check)
  if (buildingLayoutFile && !layoutFiles.includes(buildingLayoutFile)) {
    layoutFiles.push(buildingLayoutFile);
    layoutFiles.sort((a, b) => {
      const ai = parseInt(a.match(/(\d+)/)[1], 10);
      const bi = parseInt(b.match(/(\d+)/)[1], 10);
      return ai - bi;
    });
  }
  layoutFiles.forEach((layoutFile, idx) => {
    writeJSON(path.join(dataDir, `relationship_property_layout_${idx + 1}.json`), {
      from: { '/': './property.json' },
      to: { '/': `./${layoutFile}` },
    });
  });
  if (layoutFiles.length > 0) {
    console.log(`✓ Created ${layoutFiles.length} property_layout relationship(s)`);
  }
  
  // property_has_flood_storm_information (singleton)
  if (fs.existsSync(propertyPath) && fs.existsSync(floodPath)) {
    writeJSON(path.join(dataDir, 'relationship_property_flood_storm_information.json'), {
      from: { '/': './property.json' },
      to: { '/': './flood_storm_information.json' },
    });
    console.log('✓ Created relationship_property_flood_storm_information.json');
  }
  
  // property_has_structure - link property to first structure (backward compatibility)
  // Note: With multiple buildings, structures are linked via layout_has_structure instead
  const firstStructureFile = buildingNumbers.length > 0 ? `structure_${buildingNumbers[0]}.json` : 'structure.json';
  const firstStructurePath = path.join(dataDir, firstStructureFile);
  if (fs.existsSync(propertyPath) && fs.existsSync(firstStructurePath)) {
    writeJSON(path.join(dataDir, 'relationship_property_structure.json'), {
      from: { '/': './property.json' },
      to: { '/': `./${firstStructureFile}` },
    });
    console.log('✓ Created relationship_property_structure.json');
  }
  
  // property_has_utility - link property to first utility (backward compatibility)
  // Note: With multiple buildings, utilities are linked via layout_has_utility instead
  const firstUtilityFile = utilityBuildingNumbers.length > 0 ? `utility_${utilityBuildingNumbers[0]}.json` : 'utility.json';
  const firstUtilityPath = path.join(dataDir, firstUtilityFile);
  if (fs.existsSync(propertyPath) && fs.existsSync(firstUtilityPath)) {
    writeJSON(path.join(dataDir, 'relationship_property_utility.json'), {
      from: { '/': './property.json' },
      to: { '/': `./${firstUtilityFile}` },
    });
    console.log('✓ Created relationship_property_utility.json');
  }
  
  // layout_has_layout - Create relationships from subarea layouts to their building layout
  // Group layouts by building_number and link them to the corresponding building layout
  const buildingLayoutsByNumber = {}; // { buildingNumber: layoutFile }
  const subareaLayoutsByBuilding = {}; // { buildingNumber: [layoutFiles] }
  
  layoutFiles.forEach((layoutFile) => {
    const layoutPath = path.join(dataDir, layoutFile);
    if (fs.existsSync(layoutPath)) {
      const layoutData = JSON.parse(fs.readFileSync(layoutPath, 'utf-8'));
      const buildingNum = layoutData.building_number || 1;
      
      if (layoutData.space_type === 'Building') {
        buildingLayoutsByNumber[buildingNum] = layoutFile;
      } else {
        // Subarea layout
        if (!subareaLayoutsByBuilding[buildingNum]) {
          subareaLayoutsByBuilding[buildingNum] = [];
        }
        subareaLayoutsByBuilding[buildingNum].push(layoutFile);
      }
    }
  });
  
  // Create layout_has_layout relationships: subarea layouts -> building layout
  let layoutLayoutRelCount = 0;
  Object.keys(subareaLayoutsByBuilding).forEach((buildingNumStr) => {
    const buildingNum = parseInt(buildingNumStr, 10);
    const buildingLayoutFile = buildingLayoutsByNumber[buildingNum];
    const subareaLayouts = subareaLayoutsByBuilding[buildingNum] || [];
    
    if (buildingLayoutFile) {
      subareaLayouts.forEach((subareaLayoutFile) => {
        layoutLayoutRelCount++;
        writeJSON(path.join(dataDir, `relationship_layout_layout_${layoutLayoutRelCount}.json`), {
          from: { '/': `./${subareaLayoutFile}` },
          to: { '/': `./${buildingLayoutFile}` },
        });
      });
    }
  });
  if (layoutLayoutRelCount > 0) {
    console.log(`✓ Created ${layoutLayoutRelCount} layout_has_layout relationship(s) (subarea -> building)`);
  }
  
  // layout_has_structure - Create relationships from building layouts to their structures
  let layoutStructureRelCount = 0;
  Object.keys(buildingLayoutsByNumber).forEach((buildingNumStr) => {
    const buildingNum = parseInt(buildingNumStr, 10);
    const buildingLayoutFile = buildingLayoutsByNumber[buildingNum];
    const structureFile = `structure_${buildingNum}.json`;
    const structurePath = path.join(dataDir, structureFile);
    
    if (buildingLayoutFile && fs.existsSync(structurePath)) {
      layoutStructureRelCount++;
      writeJSON(path.join(dataDir, `relationship_layout_structure_${layoutStructureRelCount}.json`), {
        from: { '/': `./${buildingLayoutFile}` },
        to: { '/': `./${structureFile}` },
      });
    }
  });
  if (layoutStructureRelCount > 0) {
    console.log(`✓ Created ${layoutStructureRelCount} layout_has_structure relationship(s) (building -> structure)`);
  }
  
  // layout_has_utility - Create relationships from building layouts to their utilities
  let layoutUtilityRelCount = 0;
  Object.keys(buildingLayoutsByNumber).forEach((buildingNumStr) => {
    const buildingNum = parseInt(buildingNumStr, 10);
    const buildingLayoutFile = buildingLayoutsByNumber[buildingNum];
    const utilityFile = `utility_${buildingNum}.json`;
    const utilityPath = path.join(dataDir, utilityFile);
    
    if (buildingLayoutFile && fs.existsSync(utilityPath)) {
      layoutUtilityRelCount++;
      writeJSON(path.join(dataDir, `relationship_layout_utility_${layoutUtilityRelCount}.json`), {
        from: { '/': `./${buildingLayoutFile}` },
        to: { '/': `./${utilityFile}` },
      });
    }
  });
  if (layoutUtilityRelCount > 0) {
    console.log(`✓ Created ${layoutUtilityRelCount} layout_has_utility relationship(s) (building -> utility)`);
  }
}

if (require.main === module) {
  main();
}
