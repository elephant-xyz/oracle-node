#!/usr/bin/env node

/**
 * Property Improvement Extractor for Lee County
 * Extracts property improvement data from Tyler EnerGov HTML pages
 * 
 * This script is designed to work with @elephant-xyz/cli transform function
 * and follows the official Elephant Lexicon Property Improvement schema
 * 
 * Creates related entities (layout, structure, utility) based on improvement_type
 * 
 * @module property-improvement-extractor
 */

const fs = require('fs').promises;
const path = require('path');

console.log('=== Property Improvement Extractor Starting ===');

/**
 * Mapping of Tyler EnerGov permit types to Elephant Lexicon improvement_type enum
 * @type {Record<string, string>}
 */
const PERMIT_TYPE_MAPPING = {
  // FEMA Related
  'DEV - FEMA - Letter of Map Amendment (LOMA)': 'MapBoundaryDetermination',
  'DEV - FEMA - Letter of Map Revision - Fill (LOMR-F)': 'MapBoundaryDetermination',
  'DEV - FEMA - Letter of Map Revision (LOMR)': 'MapBoundaryDetermination',
  'DEV - FEMA - Letter of Map Determination': 'MapBoundaryDetermination',
  'DEV - FEMA - Built in Compliance/Grandfathering Letter': 'MapBoundaryDetermination',
  'DEV - FEMA - Loss of Use Statement': 'MapBoundaryDetermination',
  'DEV - FEMA - No Rise': 'MapBoundaryDetermination',
  'DEV - FEMA - Variance': 'Variance',
  
  // Accessory Structures
  'BLD - Accessory - Shed': 'GeneralBuilding',
  'BLD - Accessory - Deck/Slab': 'GeneralBuilding',
  'BLD - Accessory - Carport': 'GeneralBuilding',
  'BLD - Accessory - Screen Room': 'ScreenEnclosure',
  'BLD - Accessory - Canopy': 'GeneralBuilding',
  'BLD - Accessory - Canopy (Boat Lift)': 'GeneralBuilding',
  'BLD - Accessory - Detached Garage': 'GeneralBuilding',
  'BLD - Accessory - Gazebo': 'GeneralBuilding',
  'BLD - Accessory - Glass Utility Room': 'GeneralBuilding',
  'BLD - Accessory - Greenhouse': 'GeneralBuilding',
  'BLD - Accessory - Pool Enclosure': 'PoolSpaInstallation',
  'BLD - Accessory - Satellite Antenna': 'GeneralBuilding',
  'BLD - Accessory - Accessory Combo': 'GeneralBuilding',
  'BLD - Accessory - Master: Accessory': 'GeneralBuilding',
  'BLD - Accessory (Miscellaneous Permits imported before 6/1/2012)': 'GeneralBuilding',
  
  // Commercial Building
  'BLD - Building (C) - Misc': 'CommercialConstruction',
  'BLD - Building (C) - Remodel': 'CommercialConstruction',
  'BLD - Building (C) - Addition': 'BuildingAddition',
  'BLD - Building (C) - New': 'CommercialConstruction',
  'BLD - Building (C) - New Multi-Family Residential': 'CommercialConstruction',
  'BLD - Building (C) - Restoration': 'CommercialConstruction',
  'BLD - Building (C) - Shell Only': 'CommercialConstruction',
  'BLD - Building (C) - Dumpster': 'CommercialConstruction',
  'BLD - Building (C) - Build Out': 'CommercialConstruction',
  'BLD - Building (C) - Remodel Condo': 'CommercialConstruction',
  'BLD - Building (C) - Slab Only': 'CommercialConstruction',
  'BLD - Building (C) - Structural Move': 'StructureMove',
  'BLD - Building (C) - Master Plan: Commercial': 'CommercialConstruction',
  'BLD - Building (C) - Master Plan: Multi-Family': 'CommercialConstruction',
  'BLD - Building (C) (Miscellaneous Permits imported before 6/1/2012)': 'CommercialConstruction',
  
  // Residential Building
  'BLD - Building (R) - Single Family': 'ResidentialConstruction',
  'BLD - Building (R) - Remodel': 'ResidentialConstruction',
  'BLD - Building (R) - Restoration': 'ResidentialConstruction',
  'BLD - Building (R) - Addition': 'BuildingAddition',
  'BLD - Building (R) - Addition/Remodel': 'BuildingAddition',
  'BLD - Building (R) - Attached Garage': 'BuildingAddition',
  'BLD - Building (R) - Duplex': 'ResidentialConstruction',
  'BLD - Building (R) - Modular Home': 'ResidentialConstruction',
  'BLD - Building (R) - Remodel Condo (Retired Permit Type)': 'ResidentialConstruction',
  'BLD - Building (R) - Structural Move': 'StructureMove',
  'BLD - Building (R) - Master Plan: Single-Family': 'ResidentialConstruction',
  'BLD - Building (R) - Master Plan: Two-Family Attached': 'ResidentialConstruction',
  
  // Demolition
  'BLD - Demolition (Miscellaneous Permits imported before 6/1/2012)': 'Demolition',
  'BLD - Demolition - Commercial Demolition': 'Demolition',
  'BLD - Demolition - Commercial Demolition w/Asbestos': 'Demolition',
  'BLD - Demolition - Residential Demolition': 'Demolition',
  'BLD - Demolition - Residential Demolition w/Asbestos': 'Demolition',
  
  // Miscellaneous
  'BLD - Miscellaneous - Shutters': 'ShutterAwning',
  'BLD - Miscellaneous - Windows & Entry Doors': 'ExteriorOpeningsAndFinishes',
  'BLD - Miscellaneous - Garage Doors': 'ExteriorOpeningsAndFinishes',
  'BLD - Miscellaneous - Garage Doors (Pre-Paid)': 'ExteriorOpeningsAndFinishes',
  'BLD - Miscellaneous - Siding': 'ExteriorOpeningsAndFinishes',
  'BLD - Miscellaneous - Miscellaneous Combo': 'GeneralBuilding',
  'BLD - Miscellaneous - Master Plan: Miscellaneous Windows & Doors': 'ExteriorOpeningsAndFinishes',
  'BLD - Miscellaneous (Miscellaneous Permits imported before 6/1/2012)': 'GeneralBuilding',
  
  // Pool & Spa
  'BLD - Pool & Spa (R) (Miscellaneous Permits imported before 6/1/2012)': 'PoolSpaInstallation',
  'BLD - Pool & Spa (R) - In-Ground': 'PoolSpaInstallation',
  'BLD - Pool & Spa (R) - Spa': 'PoolSpaInstallation',
  'BLD - Pool & Spa (R) - Master Plan: Pool': 'PoolSpaInstallation',
  'BLD - Pool & Spa (C) - New Construction': 'PoolSpaInstallation',
  'BLD - Pool & Spa (C) - Modification': 'PoolSpaInstallation',
  'BLD - Pool & Spa (C) - Revision': 'PoolSpaInstallation',
  'BLD - Pool & Spa (C) - Master Plan: Pool': 'PoolSpaInstallation',
  
  // Trade Permits
  'BLD - Trade - Mechanical Permit': 'MechanicalHVAC',
  'BLD - Trade - Roof Permit': 'Roofing',
  'BLD - Trade - Electrical Permit': 'Electrical',
  'BLD - Trade - Plumbing Permit': 'Plumbing',
  'BLD - Trade - Gas Permit': 'GasInstallation',
  'BLD - Trade - Solar Permit': 'Solar',
  'BLD - Trade - Fire Sprinkler Permit': 'FireProtectionSystem',
  'BLD - Trade - Irrigation Permit': 'LandscapeIrrigation',
  
  // Dock & Shoreline
  'BLD - Dock & Shoreline (Miscellaneous Permits imported before 6/1/2012)': 'DockAndShore',
  'BLD - Dock & Shoreline - Boat Dock/Pilings': 'DockAndShore',
  'BLD - Dock & Shoreline - Boat Lift': 'DockAndShore',
  'BLD - Dock & Shoreline - Boathouse': 'DockAndShore',
  'BLD - Dock & Shoreline - Combo': 'DockAndShore',
  'BLD - Dock & Shoreline - Davit': 'DockAndShore',
  'BLD - Dock & Shoreline - Dredging': 'DockAndShore',
  'BLD - Dock & Shoreline - Rip Rap': 'DockAndShore',
  'BLD - Dock & Shoreline - Seawall': 'DockAndShore',
  
  // Fence
  'BLD - Fence (Miscellaneous Permits imported before 6/1/2012)': 'Fencing',
  'BLD - Fence - Fence': 'Fencing',
  'BLD - Fence - Structural Wall': 'Fencing',
  
  // Sign (not a property improvement type - map to null or GeneralBuilding)
  'BLD - Sign (Miscellaneous Permits imported before 6/1/2012)': null,
  'BLD - Sign - Flag': null,
  'BLD - Sign - Free-Standing Sign': null,
  'BLD - Sign - Wall Sign': null,
  
  // Use Permits (administrative - not property improvements)
  'BLD - Use (Miscellaneous Permits imported before 6/1/2012)': null,
  'BLD - Use - Expanding Existing Business': null,
  'BLD - Use - Medical Marijuana Treatment Center': null,
  'BLD - Use - New Business in Bonita Springs': null,
  'BLD - Use - Relocating within City Limits': null,
  
  // Planning & Zoning
  'PNZ - Planned Development - Minor PD': 'PlannedDevelopment',
  'PNZ - Planned Development - Major PD': 'PlannedDevelopment',
  'PNZ - Planned Development - PD Amendments': 'PlannedDevelopment',
  'PNZ - Planned Development - Industrial Planned Development/Excavation': 'PlannedDevelopment',
  'PNZ - Planned Development - Legislative Extension': 'PlannedDevelopment',
  'PNZ - Planned Development - Master Concept Plan Extensions': 'PlannedDevelopment',
  'PNZ - Planned Development - Master Concept Plan Reinstatement': 'PlannedDevelopment',
  'PNZ - COP - Imported': 'AdministrativeApproval',
  'PNZ - COP - Administrative Action (Supplement C)': 'AdministrativeApproval',
  'PNZ - COP - Over-the-Counter': 'AdministrativeApproval',
  'PNZ - COP - Special Exception': 'SpecialExceptionZoning',
  'PNZ - Comprehensive Plan Amendment - Imported': 'ComprehensivePlanAmendment',
  'PNZ - Comprehensive Plan Amendment - Administrative Map Determination': 'ComprehensivePlanAmendment',
  'PNZ - Comprehensive Plan Amendment - Administrative Text Determination': 'ComprehensivePlanAmendment',
  'PNZ - Comprehensive Plan Amendment - Large Scale Map Amendments': 'ComprehensivePlanAmendment',
  'PNZ - Comprehensive Plan Amendment - Small-Scale Amendment': 'ComprehensivePlanAmendment',
  'PNZ - Comprehensive Plan Amendment - Text Amendment': 'ComprehensivePlanAmendment',
  'PNZ - Home Occupational License - Imported': 'AdministrativeApproval',
  'PNZ - Home Occupational License - Home Occupational License': 'AdministrativeApproval',
  'PNZ - Temporary Use - Temporary Use w/ Tent': 'AdministrativeApproval',
  'PNZ - Temporary Use - Temporary Use w/o Tent': 'AdministrativeApproval',
  'PNZ - Temporary Use - Temporary Sign/Banner': 'AdministrativeApproval',
  'PNZ - Temporary Use - Temporary Construction Trailer': 'AdministrativeApproval',
  'PNZ - Temporary Use - FEMA Housing': 'AdministrativeApproval',
  'PNZ - Temporary Use - Outdoor Dog Dining': 'AdministrativeApproval',
  'PNZ - Vacation - Imported': 'Vacation',
  'PNZ - Vacation - Row, Plat with ROW': 'Vacation',
  'PNZ - Vacation - Plat - No ROW': 'Vacation',
  'PNZ - Vacation - Drainage Easement': 'Vacation',
  'PNZ - Vacation - Utility Easement': 'Vacation',
  'PNZ - Zoning Verification Letter - Imported': 'ZoningVerificationLetter',
  'PNZ - Zoning Verification Letter - Standard': 'ZoningVerificationLetter',
  'PNZ - Zoning Verification Letter - Full-Review': 'ZoningVerificationLetter',
  'PNZ - Administrative Decision - Imported': 'AdministrativeApproval',
  'PNZ - Administrative Decision - LDC Administrative Determination': 'AdministrativeApproval',
  'PNZ - Administrative Decision - PD Final Plan Approval': 'AdministrativeApproval',
  'PNZ - Administrative Decision - Redevelopment District': 'AdministrativeApproval',
  'PNZ - Administrative Decision - Supplement A (Setback Variance)': 'Variance',
  'PNZ - Administrative Decision - Supplement B (Commercial Lot Split)': 'AdministrativeApproval',
  'PNZ - Administrative Decision - Supplement D (Minimum Use Determination)': 'MinimumUseDetermination',
  'PNZ - Administrative Decision - Supplement E (Ordinance Interpretation)': 'AdministrativeApproval',
  'PNZ - Administrative Decision - Supplement G (Easement Encroachment)': 'AdministrativeApproval',
  'PNZ - Administrative Decision - Supplement H (Major Amendments to PUD/PD)': 'PlannedDevelopment',
  'PNZ - Administrative Decision - Supplement H (Minor Amendments to PUD/PD)': 'PlannedDevelopment',
  'PNZ - Administrative Decision - Supplement I (Deviation from Chapter 3 LDC)': 'AdministrativeApproval',
  'PNZ - Administrative Decision - Supplement J (Placement of Model Homes/Units)': 'AdministrativeApproval',
  'PNZ - Administrative Decision - Supplement K (Dock and Shoreline Structures)': 'DockAndShore',
  'PNZ - Administrative Decision - Supplement L (Post Disaster Relief)': 'AdministrativeApproval',
  'PNZ - Administrative Decision - Supplement N (Joint Use of Parking)': 'AdministrativeApproval',
  'PNZ - Administrative Decision - Supplement R (Downtown District Variance)': 'Variance',
  'PNZ - Administrative Decision - Wireless Communication Facility': 'AdministrativeApproval',
  'PNZ - Special Exception - Imported': 'SpecialExceptionZoning',
  'PNZ - Special Exception - Other': 'SpecialExceptionZoning',
  'PNZ - Special Exception - Tower': 'SpecialExceptionZoning',
  'PNZ - Variance - Imported': 'Variance',
  'PNZ - Variance - Residential/Commercial': 'Variance',
  'PNZ - Variance - Sign': 'Variance',
  'PNZ - Variance - Dock (Public Hearing)': 'Variance',
  'PNZ - Rezoning - Rezoning (Conventional)': 'Rezoning',
  'PNZ - DRI - Imported': 'DevelopmentOfRegionalImpact',
  'PNZ - DRI - Essentially Builtout Process': 'DevelopmentOfRegionalImpact',
  'PNZ - DRI - Notice of Proposed Change': 'DevelopmentOfRegionalImpact',
  'PNZ - DRI - Time Extension': 'DevelopmentOfRegionalImpact',
  'PNZ - Certificate to Dig - Certificate to Dig': 'CertificateToDig',
  'PNZ - Certificate of Appropriateness - Imported': 'SpecialCertificateOfAppropriateness',
  'PNZ - Certificate of Appropriateness - Regular': 'SpecialCertificateOfAppropriateness',
  'PNZ - Certificate of Appropriateness - Special': 'SpecialCertificateOfAppropriateness',
  'PNZ - Historical Designation - Historical Designation': 'HistoricDesignation',
  'PNZ - Annexation - Annexation Request': 'AdministrativeApproval',
  'PNZ - Architectural Review - Architectural Review': 'AdministrativeApproval',
  'PNZ - Color Palette - Color Palette': 'AdministrativeApproval',
  'PNZ - Development Agreement - Development Agreement': 'AdministrativeApproval',
  'PNZ - Mobile Food Vendor - Mobile Food Vendor': 'AdministrativeApproval',
  'PNZ - Planning Determination Appeals - Planning Determination Appeals': 'PlanningAdministrativeAppeal',
  'PNZ - Pre-Application - Pre-Application': 'AdministrativeApproval',
  'PNZ - Pre-Application - Pre-Application CCCL': 'AdministrativeApproval',
  'PNZ - Statutory Agreement - Community Development District': 'AdministrativeApproval',
  'PNZ - Statutory Agreement - Community Development District Amendment': 'AdministrativeApproval',
  'PNZ - Impact Fee Credit Account - Impact Fee Credit Account': null,
  'PNZ - Impact Fee Credit Account - EMS': null,
  'PNZ - Impact Fee Credit Account - Fire': null,
  'PNZ - Impact Fee Credit Account - Parks': null,
  'PNZ - Impact Fee Credit Account - Roads': null,
  'PNZ - Impact Fee Credit Account - Schools': null,
  'PNZ - Open Records Request - Imported': null,
  'PNZ - Open Records Request - Open Records Request': null,
  'PNZ - Legacy Permit - Time Extension': null,
  'PNZ - Hen - Hen': null,
  
  // Development
  'DEV - Development Order - Imported': 'SiteDevelopment',
  'DEV - Development Order - Limited Review - Imported': 'SiteDevelopment',
  'DEV - Development Order - Limited Review': 'SiteDevelopment',
  'DEV - Development Order - Large Scale': 'SiteDevelopment',
  'DEV - Development Order - Small Scale': 'SiteDevelopment',
  'DEV - Development Order - Downtown District': 'SiteDevelopment',
  'DEV - Development Order - Blasting': 'SiteDevelopment',
  'DEV - Development Order - Limited Review - Types A and B': 'SiteDevelopment',
  'DEV - Development Order - Limited Review - Types C and D (Lot Split/Combo)': 'SiteDevelopment',
  'DEV - Development Order - Limited Review - Types E, F and G': 'SiteDevelopment',
  'DEV - Development Order - Limited Review - Types H and I': 'SiteDevelopment',
  'DEV - Development Order - Limited Review - Type 15 (Ag Use Excavation)': 'SiteDevelopment',
  'DEV - Development Order - Limited Review - Unity of Title': 'SiteDevelopment',
  'DEV - Plat - Imported': 'SiteDevelopment',
  'DEV - Plat - Plat': 'SiteDevelopment',
  'DEV - Concurrency - Concurrency': 'SiteDevelopment',
  'DEV - Concurrency - Imported': 'SiteDevelopment',
  'LDO Type 99': 'SiteDevelopment',
  
  // Mobile Home
  'BLD - Mobile Home - Mobile Home': 'MobileHomeRV',
  'BLD - Mobile Home - Park Model': 'MobileHomeRV',
  'BLD - Mobile Home - ANSI Unit': 'MobileHomeRV',
  'BLD - Mobile Home - Master Plan: Mobile Home': 'MobileHomeRV',
  'BLD - Mobile Home - Temporary Construction Trailer': 'MobileHomeRV',
  
  // Environment
  'ENV - Vegetation Removal - Imported': 'VegetationRemoval',
  'ENV - Vegetation Removal - Tree Removal': 'VegetationRemoval',
  'ENV - Vegetation Removal - Vegetation Removal': 'VegetationRemoval',
  'ENV - Vegetation Removal - Agricultural Clearing': 'VegetationRemoval',
  'ENV - Beach and Dune - Beach and Dune': 'SiteDevelopment',
  
  // Fire Permits (map to FireProtectionSystem)
  'FIRE - Bonita Springs Fire - Fire Sprinkler System': 'FireProtectionSystem',
  'FIRE - Bonita Springs Fire - Fire Alarm System': 'FireProtectionSystem',
  'FIRE - Bonita Springs Fire - Fire Pump': 'FireProtectionSystem',
  'FIRE - Bonita Springs Fire - Standpipe': 'FireProtectionSystem',
  'FIRE - Bonita Springs Fire - Suppression System': 'FireProtectionSystem',
  'FIRE - Bonita Springs Fire - Hood Suppression System': 'FireProtectionSystem',
  'FIRE - Bonita Springs Fire - Clean Agent': 'FireProtectionSystem',
  'FIRE - Bonita Springs Fire - Monitoring System': 'FireProtectionSystem',
  'FIRE - Bonita Springs Fire - Exhaust Hood': 'FireProtectionSystem',
  'FIRE - Bonita Springs Fire - Dry Hydrant': 'FireProtectionSystem',
  'FIRE - Bonita Springs Fire - Fuel Tank/Piping': 'FireProtectionSystem',
  'FIRE - Bonita Springs Fire - LPG Tank': 'FireProtectionSystem',
  'FIRE - Bonita Springs Fire - LPG/NAT Gas Piping': 'GasInstallation',
  'FIRE - Bonita Springs Fire - Natural Gas': 'GasInstallation',
  'FIRE - Bonita Springs Fire - Paint Booth': 'FireProtectionSystem',
  'FIRE - Bonita Springs Fire - U/G Fire Line': 'FireProtectionSystem',
  'FIRE - Bonita Springs Fire - Master Plan: Fire': 'FireProtectionSystem',
  'FIRE - Estero Fire - Fire Sprinkler System': 'FireProtectionSystem',
  'FIRE - Estero Fire - Fire Alarm System': 'FireProtectionSystem',
  'FIRE - Estero Fire - Fire Pump': 'FireProtectionSystem',
  'FIRE - Estero Fire - Standpipe': 'FireProtectionSystem',
  'FIRE - Estero Fire - Suppression System': 'FireProtectionSystem',
  'FIRE - Estero Fire - Hood Suppression System': 'FireProtectionSystem',
  'FIRE - Estero Fire - Monitoring System': 'FireProtectionSystem',
  'FIRE - Estero Fire - Exhaust Hood': 'FireProtectionSystem',
  'FIRE - Estero Fire - LPG/NAT Gas Piping': 'GasInstallation',
  'FIRE - Estero Fire - Natural Gas': 'GasInstallation',
  'FIRE - Estero Fire - U/G Fire Line': 'FireProtectionSystem',
  
  // Public Works
  'PW - Driveway Permit - Single Residential': 'DrivewayPermit',
  
  // Extension (not property improvements)
  'EXT - Legislative Extension - BLD - Building (C)': null,
  'EXT - Legislative Extension - DEV - Development Order': null,
  'EXT - Legislative Extension - DEV - Limted Review DO': null,
  'EXT - Legislative Extension - PNZ - DRI': null,
  'EXT - Legislative Extension - PNZ - Planned Development': null,
  
  // Legacy
  'BLD - Legacy Permit': 'GeneralBuilding',
};

/**
 * Status mapping from Tyler EnerGov to Elephant Lexicon improvement_status enum
 * @type {Record<string, string>}
 */
const STATUS_MAPPING = {
  'Finaled': 'Completed',
  'Issued': 'Permitted',
  'Approved': 'Permitted',
  'In Review': 'InProgress',
  'Submitted': 'InProgress',
  'Expired': 'Cancelled',
  'Cancelled': 'Cancelled',
  'Closed': 'Completed',
  'Pending': 'Planned',
};

/**
 * Determine improvement_action based on permit type
 * @param {string} permitType - Raw permit type from Tyler
 * @returns {string | null} - improvement_action enum value
 */
function determineImprovementAction(permitType) {
  if (!permitType) return null;
  
  const lower = permitType.toLowerCase();
  if (lower.includes('new')) return 'New';
  if (lower.includes('addition')) return 'Addition';
  if (lower.includes('remodel') || lower.includes('renovation')) return 'Alteration';
  if (lower.includes('repair') || lower.includes('restoration')) return 'Repair';
  if (lower.includes('replacement')) return 'Replacement';
  if (lower.includes('demolition') || lower.includes('remove')) return 'Remove';
  
  return 'Other';
}

/**
 * Determine contractor_type based on available information
 * @param {string} projectName - Project name
 * @returns {string | null} - contractor_type enum value (GeneralContractor, Specialist, DIY, PropertyManager, Builder, HandymanService, Unknown, or null)
 */
function determineContractorType(projectName) {
  if (!projectName) return null;
  
  const lower = projectName.toLowerCase();
  
  // Check for specific contractor types
  if (lower.includes('builder') || lower.includes('construction company')) return 'Builder';
  if (lower.includes('contractor') || lower.includes('general')) return 'GeneralContractor';
  if (lower.includes('specialist') || lower.includes('electric') || lower.includes('plumb') || lower.includes('hvac')) return 'Specialist';
  if (lower.includes('handyman') || lower.includes('handy')) return 'HandymanService';
  if (lower.includes('property manager') || lower.includes('management')) return 'PropertyManager';
  if (lower.includes('owner') && (lower.includes('builder') || lower.includes('build'))) return 'DIY';
  
  return null; // Return null instead of 'Unknown' to match schema (allows null)
}

/**
 * Check if improvement is disaster recovery
 * @param {string} projectName - Project name
 * @param {string} description - Description
 * @returns {boolean | null} - is_disaster_recovery value
 */
function isDisasterRecovery(projectName, description) {
  const text = `${projectName || ''} ${description || ''}`.toLowerCase();
  return text.includes('hurricane') || text.includes('disaster') || text.includes('storm') || null;
}

/**
 * Check if improvement type should generate layout data
 * @param {string} improvementType - Lexicon improvement_type
 * @returns {boolean} - Whether to create layout
 */
function shouldCreateLayout(improvementType) {
  const layoutTypes = [
    'PoolSpaInstallation',
    'ResidentialConstruction',
    'CommercialConstruction',
    'BuildingAddition',
    'ScreenEnclosure',
  ];
  return layoutTypes.includes(improvementType);
}

/**
 * Check if improvement type should generate structure data
 * @param {string} improvementType - Lexicon improvement_type
 * @returns {boolean} - Whether to create structure
 */
function shouldCreateStructure(improvementType) {
  const structureTypes = [
    'Roofing',
    'ResidentialConstruction',
    'CommercialConstruction',
    'BuildingAddition',
    'GeneralBuilding',
    'ExteriorOpeningsAndFinishes',
    'Fencing',
    'DockAndShore',
  ];
  return structureTypes.includes(improvementType);
}

/**
 * Check if improvement type should generate utility data
 * @param {string} improvementType - Lexicon improvement_type
 * @returns {boolean} - Whether to create utility
 */
function shouldCreateUtility(improvementType) {
  const utilityTypes = [
    'MechanicalHVAC',
    'Electrical',
    'Plumbing',
    'GasInstallation',
    'Solar',
    'ResidentialConstruction',
    'CommercialConstruction',
  ];
  return utilityTypes.includes(improvementType);
}

/**
 * Check if raw permit type should create utility (even if improvement_type is null or different)
 * @param {string | null} rawPermitType - Raw permit type from HTML
 * @returns {boolean} - Whether to create utility
 */
function shouldCreateUtilityFromRawType(rawPermitType) {
  if (!rawPermitType) return false;
  const lower = rawPermitType.toLowerCase();
  // Solar permits should create utility entities even if improvement_type is null
  return lower.includes('solar') || lower.includes('electrical') || 
         lower.includes('mechanical') || lower.includes('hvac') || 
         lower.includes('plumbing') || lower.includes('gas');
}

/**
 * Check if an entity has any meaningful data (non-null values beyond metadata)
 * @param {object} entity - Entity object to check
 * @returns {boolean} - Whether entity has meaningful data
 */
function hasMeaningfulData(entity) {
  if (!entity) return false;
  
  // Check all properties except source_http_request and request_identifier
  for (const [key, value] of Object.entries(entity)) {
    if (key !== 'source_http_request' && key !== 'request_identifier') {
      // Check if value is not null and not false (false is a valid boolean value)
      if (value !== null && value !== undefined) {
        // For strings, check if not empty after trimming
        if (typeof value === 'string' && value.trim().length > 0) {
          return true;
        }
        // For numbers, check if not 0 or NaN
        if (typeof value === 'number' && !isNaN(value) && value !== 0) {
          return true;
        }
        // For booleans, true is meaningful
        if (typeof value === 'boolean' && value === true) {
          return true;
        }
        // For arrays, check if not empty
        if (Array.isArray(value) && value.length > 0) {
          return true;
        }
        // For objects, check if has any properties
        if (typeof value === 'object' && !Array.isArray(value) && Object.keys(value).length > 0) {
          return true;
        }
        // For utility entities, installation dates are meaningful even if other fields are null
        // Installation dates indicate when utility work was done, which is meaningful data
        if ((key.includes('installation_date') || key.includes('_date')) && typeof value === 'string' && value.trim().length > 0) {
          return true;
        }
      }
    }
  }
  
  return false;
}

/**
 * Create layout entity based on improvement type
 * @param {object} sourceRequest - Source HTTP request (without request_identifier)
 * @param {string} requestIdentifier - Request identifier
 * @param {string} improvementType - Lexicon improvement_type
 * @param {number | null} squareFeet - Square feet from permit
 * @param {string | null} issueDate - Permit issue date
 * @returns {object} - Layout entity
 */
function createLayout(sourceRequest, requestIdentifier, improvementType, squareFeet, issueDate) {
  const layout = {
    source_http_request: sourceRequest,
    request_identifier: requestIdentifier,
    space_type: null,
    space_index: 1,
    flooring_material_type: null,
    size_square_feet: squareFeet,
    floor_level: null,
    has_windows: null,
    window_design_type: null,
    window_material_type: null,
    window_treatment_type: null,
    is_finished: null,
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
    is_exterior: null,
    pool_condition: null,
    pool_surface_type: null,
    pool_water_quality: null,
  };
  
  // Set intelligent defaults based on improvement type
  if (improvementType === 'PoolSpaInstallation') {
    layout.space_type = 'Pool';
    layout.is_exterior = true;
    layout.pool_type = 'BuiltIn';
    if (issueDate) layout.pool_installation_date = issueDate;
  } else if (improvementType === 'ScreenEnclosure') {
    layout.space_type = 'Patio';
    layout.is_exterior = false;
  }
  
  return layout;
}

/**
 * Create structure entity based on improvement type
 * @param {object} sourceRequest - Source HTTP request (without request_identifier)
 * @param {string} requestIdentifier - Request identifier
 * @param {string} improvementType - Lexicon improvement_type
 * @param {string | null} issueDate - Permit issue date
 * @returns {object} - Structure entity
 */
function createStructure(sourceRequest, requestIdentifier, improvementType, issueDate) {
  const structure = {
    source_http_request: sourceRequest,
    request_identifier: requestIdentifier,
    architectural_style_type: null,
    attachment_type: null,
    exterior_wall_material_primary: null,
    exterior_wall_material_secondary: null,
    exterior_wall_condition: null,
    exterior_wall_condition_primary: null,
    exterior_wall_condition_secondary: null,
    exterior_wall_insulation_type: null,
    exterior_wall_insulation_type_primary: null,
    exterior_wall_insulation_type_secondary: null,
    flooring_material_primary: null,
    flooring_material_secondary: null,
    subfloor_material: null,
    flooring_condition: null,
    interior_wall_structure_material: null,
    interior_wall_structure_material_primary: null,
    interior_wall_structure_material_secondary: null,
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
  
  // Set intelligent defaults based on improvement type
  if (improvementType === 'Roofing') {
    if (issueDate) structure.roof_date = issueDate;
  } else if (improvementType === 'ExteriorOpeningsAndFinishes') {
    if (issueDate) structure.window_installation_date = issueDate;
  }
  
  return structure;
}

/**
 * Create utility entity based on improvement type
 * @param {object} sourceRequest - Source HTTP request (without request_identifier)
 * @param {string} requestIdentifier - Request identifier
 * @param {string} improvementType - Lexicon improvement_type
 * @param {string | null} issueDate - Permit issue date
 * @param {string | null} rawPermitType - Raw permit type from HTML (e.g., "BLD - Trade - Solar Permit")
 * @returns {object} - Utility entity
 */
function createUtility(sourceRequest, requestIdentifier, improvementType, issueDate, rawPermitType) {
  const utility = {
    source_http_request: sourceRequest,
    request_identifier: requestIdentifier,
    cooling_system_type: null,
    heating_system_type: null,
    heating_fuel_type: null,
    public_utility_type: null,
    sewer_type: null,
    water_source_type: null,
    plumbing_system_type: null,
    plumbing_system_type_other_description: null,
    electrical_panel_capacity: null,
    electrical_wiring_type: null,
    electrical_wiring_type_other_description: null,
    hvac_condensing_unit_present: null,
    hvac_unit_condition: null,
    hvac_unit_issues: null,
    solar_panel_present: false, // Required boolean, default to false if not known
    solar_panel_type: null,
    solar_panel_type_other_description: null,
    smart_home_features: null,
    smart_home_features_other_description: null,
    solar_inverter_visible: false, // Required boolean, default to false if not known
  };
  
  // Set intelligent defaults based on improvement type
  if (improvementType === 'MechanicalHVAC') {
    if (issueDate) utility.hvac_installation_date = issueDate;
  } else if (improvementType === 'Electrical') {
    if (issueDate) utility.electrical_panel_installation_date = issueDate;
  } else if (improvementType === 'Solar') {
    // For Solar permits, set solar-specific fields
    if (issueDate) utility.solar_installation_date = issueDate;
    utility.solar_panel_present = true; // Solar permit implies solar panels are present
    utility.solar_inverter_visible = true; // Solar permits typically include inverters
  } else if (improvementType === 'Plumbing') {
    if (issueDate) utility.plumbing_system_installation_date = issueDate;
  }
  
  return utility;
}

/**
 * Format name to match schema pattern: ^[A-Z][a-z]*([ \-',.][A-Za-z][a-z]*)*$
 * Pattern requires: First letter uppercase, rest lowercase, with optional parts separated by space, hyphen, apostrophe, comma, or period
 * Examples: "JAMES" -> "James", "SMITH" -> "Smith", "O'CONNOR" -> "O'Connor", "MARY-JANE" -> "Mary-Jane"
 * @param {string | null} name - Name to format
 * @returns {string | null} - Formatted name or null
 */
function formatNameForSchema(name) {
  if (!name || typeof name !== 'string') return null;
  
  const trimmed = name.trim();
  if (!trimmed) return null;
  
  // Split by separators (space, hyphen, apostrophe, comma, period)
  const parts = trimmed.split(/([\s\-',.])/);
  const formattedParts = [];
  
  for (let i = 0; i < parts.length; i++) {
    const part = parts[i];
    
    // If it's a separator, keep it as-is
    if (/^[\s\-',.]$/.test(part)) {
      formattedParts.push(part);
      continue;
    }
    
    // Format each name part: First letter uppercase, rest lowercase
    if (part.length > 0) {
      const formatted = part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
      formattedParts.push(formatted);
    }
  }
  
  return formattedParts.join('');
}

/**
 * Extract text content from HTML by finding label and getting the next value
 * @param {string} html - HTML content
 * @param {string} labelText - Label text to search for
 * @returns {string | null} - Extracted text or null
 */
function extractByLabel(html, labelText) {
  // Pattern 1: <label[^>]*>\s*Label Text\s*</label>
  let labelPattern = new RegExp(`<label[^>]*>\\s*${escapeRegex(labelText)}\\s*<\\/label>`, 'i');
  let labelMatch = html.match(labelPattern);
  
  // Pattern 2: >Label Text</label (label closes immediately after text, with optional whitespace)
  if (!labelMatch) {
    labelPattern = new RegExp(`>\\s*${escapeRegex(labelText)}\\s*<\\/label`, 'i');
    labelMatch = html.match(labelPattern);
  }
  
  // Pattern 3: >Label Text</label> (with closing >)
  if (!labelMatch) {
    labelPattern = new RegExp(`>\\s*${escapeRegex(labelText)}\\s*<\\/label>`, 'i');
    labelMatch = html.match(labelPattern);
  }
  
  // Pattern 4: Label Text with newlines and whitespace (multiline)
  if (!labelMatch) {
    labelPattern = new RegExp(`>\\s*${escapeRegex(labelText)}\\s*[\\r\\n\\s]*<\\/label`, 'i');
    labelMatch = html.match(labelPattern);
  }
  
  if (!labelMatch) return null;
  
  const afterLabel = html.substring(labelMatch.index + labelMatch[0].length);
  // Try multiple patterns for the value (SPA data might be in different formats)
  // Pattern 1: <p> tag with form-control-static and ng-binding (may span multiple lines)
  // Handle multi-line opening tags: <p\n  class="...">\n  value\n</p>
  let valuePattern = /<p[\s\S]*?form-control-static[\s\S]*?ng-binding[\s\S]*?>([\s\S]*?)<\/p>/i;
  let valueMatch = afterLabel.match(valuePattern);
  
  // Pattern 2: <p> tag with form-control-static (may span multiple lines)
  if (!valueMatch || !cleanHtmlText(valueMatch[1]).trim()) {
    valuePattern = /<p[\s\S]*?form-control-static[\s\S]*?>([\s\S]*?)<\/p>/i;
    valueMatch = afterLabel.match(valuePattern);
  }
  
  // Pattern 3: ng-binding class with content (Angular SPA, may span multiple lines)
  if (!valueMatch || !cleanHtmlText(valueMatch[1]).trim()) {
    valuePattern = /<[^>]*ng-binding[^>]*>([\s\S]{1,500}?)<\/[^>]*>/i;
    valueMatch = afterLabel.match(valuePattern);
  }
  
  // Pattern 4: <p> tag after label (handling multi-line opening tags)
  if (!valueMatch || !cleanHtmlText(valueMatch[1]).trim()) {
    valuePattern = /<p[\s\S]*?>[\s\S]*?([^\s<>][\s\S]{0,100}?)<\/p>/i;
    valueMatch = afterLabel.match(valuePattern);
  }
  
  // Pattern 5: Any element after label with non-empty text content
  if (!valueMatch || !cleanHtmlText(valueMatch[1]).trim()) {
    valuePattern = /<[^>]+>([\s\S]{1,200}?)<\/[^>]+>/i;
    valueMatch = afterLabel.match(valuePattern);
  }
  
  if (!valueMatch) return null;
  
  const cleaned = cleanHtmlText(valueMatch[1]);
  // Return null if the value is empty or just whitespace
  return cleaned && cleaned.trim().length > 0 ? cleaned : null;
}

/**
 * Extract contractor companies and their associated persons from HTML table structure
 * @param {string} html - HTML content
 * @returns {Array<{type: string, name: string, personIndices: number[]}>} - Array of contractor companies with associated person indices
 */
function extractContractors(html) {
  const contractors = [];
  
  // List of non-company names to exclude
  const excludeNames = new Set([
    'stop work order',
    'stop work',
    'work order',
    'permit',
    'building permit',
    'inspection',
    'contractor',
    'company',
    'name',
    'description',
    'type',
    'status',
  ]);
  
  // Helper function to check if a name should be excluded
  const shouldExcludeName = (name) => {
    if (!name) return true;
    const lowerName = name.toLowerCase().trim();
    // Check if it's in the exclude list
    if (excludeNames.has(lowerName)) return true;
    // Check if it contains any exclude words
    for (const excludeWord of excludeNames) {
      if (lowerName.includes(excludeWord)) return true;
    }
    // Check if it's too short (likely not a real company name)
    if (lowerName.length < 3) return true;
    // Check if it looks like a label or field name
    if (lowerName.match(/^(name|description|type|status|company|contractor)[\s:]/i)) return true;
    return false;
  };
  
  // Pattern 1: aria-label="Type Contractor - [TYPE]" followed by aria-label="Company [NAME]"
  // Then look for First Name/Last Name within the same table row/group
  let contractorPattern = /aria-label="Type Contractor - ([^"]+)"[^>]*>Contractor - ([^<]+)<\/[^>]*>\s*<td[^>]*aria-label="Company ([^"]+)"[^>]*>([^<]+)<\/td>/gi;
  let match;
  
  while ((match = contractorPattern.exec(html)) !== null) {
    const type = match[1].trim();
    const name = match[3].trim() || match[4].trim();
    if (name && name !== 'Company' && name.length > 0 && !shouldExcludeName(name)) {
      // Find persons associated with this contractor (within same table row or ~3000 chars after)
      // Get the entire table row that contains this company
      const matchIndex = match.index;
      const beforeMatch = html.substring(Math.max(0, matchIndex - 500), matchIndex);
      const afterMatch = html.substring(matchIndex + match[0].length, matchIndex + match[0].length + 3000);
      
      // Find the start of the table row
      const rowStart = beforeMatch.lastIndexOf('<tr');
      const rowEnd = afterMatch.indexOf('</tr>');
      
      let searchArea = afterMatch.substring(0, rowEnd > 0 ? rowEnd : 3000);
      if (rowStart >= 0) {
        // Include the part before match if it's in the same row
        const rowContent = beforeMatch.substring(rowStart) + match[0] + afterMatch.substring(0, rowEnd > 0 ? rowEnd : 3000);
        searchArea = rowContent;
      }
      
      const personMatches = [];
      
      // Look for First Name/Last Name pairs in the search area
      const firstNamePattern = /aria-label="First Name ([^"]+)"/gi;
      const lastNamePattern = /aria-label="Last Name ([^"]+)"/gi;
      
      const firstNames = [];
      const lastNames = [];
      
      let firstNameMatch;
      while ((firstNameMatch = firstNamePattern.exec(searchArea)) !== null) {
        firstNames.push(firstNameMatch[1].trim());
      }
      
      let lastNameMatch;
      while ((lastNameMatch = lastNamePattern.exec(searchArea)) !== null) {
        lastNames.push(lastNameMatch[1].trim());
      }
      
      // Pair up first and last names from the same area
      for (let i = 0; i < Math.max(firstNames.length, lastNames.length); i++) {
        let firstName = (firstNames[i] || '').trim();
        let lastName = (lastNames[i] || '').trim();
        
        // Clean up names - remove "First Name" or "Last Name" text if accidentally included
        firstName = firstName.replace(/^(First\s+Name|First Name)\s*/i, '').trim();
        lastName = lastName.replace(/^(Last\s+Name|Last Name)\s*/i, '').trim();
        
        // Skip if name contains "First Name" or "Last Name" as part of the actual name
        if ((firstName || lastName) && 
            !firstName.toLowerCase().includes('first name') && 
            !firstName.toLowerCase().includes('last name') &&
            !lastName.toLowerCase().includes('first name') && 
            !lastName.toLowerCase().includes('last name')) {
          personMatches.push({
            firstName,
            lastName,
          });
        }
      }
      
      contractors.push({ 
        type, 
        name, 
        personIndices: [], // Will be populated after extracting persons
        personMatches 
      });
    }
  }
  
  // Pattern 2: Look for company names in table rows with "Contractor" context
  if (contractors.length === 0) {
    contractorPattern = /<tr[^>]*>[\s\S]*?Contractor[^<]*<[^>]*>[\s\S]*?<td[^>]*>([^<]+)<\/td>[\s\S]*?<td[^>]*>([^<]+)<\/td>/gi;
    while ((match = contractorPattern.exec(html)) !== null) {
      const name = (match[1] || match[2]).trim();
      if (name && name.length > 2 && !name.includes('Contractor') && !name.includes('Company') && !shouldExcludeName(name)) {
        contractors.push({ type: 'General', name, personIndices: [], personMatches: [] });
      }
    }
  }
  
  // Pattern 3: Look in JavaScript data (ng-repeat, JSON objects)
  if (contractors.length === 0) {
    const jsonPattern = /"Company[^"]*":\s*"([^"]+)"/gi;
    while ((match = jsonPattern.exec(html)) !== null) {
      const name = match[1].trim();
      if (name && name.length > 2 && !shouldExcludeName(name)) {
        contractors.push({ type: 'General', name, personIndices: [], personMatches: [] });
      }
    }
  }
  
  // Pattern 4: Simple pattern for contractor - company
  if (contractors.length === 0) {
    contractorPattern = /Contractor[^<]*>([^<]+)<\/[^>]*>\s*<td[^>]*>([^<]+)<\/td>/gi;
    while ((match = contractorPattern.exec(html)) !== null) {
      const type = match[1].trim();
      const name = match[2].trim();
      if (name && !name.includes('Contractor') && name.length > 2 && !shouldExcludeName(name)) {
        contractors.push({ type, name, personIndices: [], personMatches: [] });
      }
    }
  }
  
  // Deduplicate
  const seen = new Set();
  return contractors.filter(c => {
    const key = `${c.type}:${c.name}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

/**
 * Extract person/contact information from HTML
 * @param {string} html - HTML content
 * @returns {Array<{firstName: string, lastName: string}>} - Array of persons
 */
function extractPersons(html) {
  const persons = [];
  
  // Pattern: aria-label="First Name [NAME]" and aria-label="Last Name [NAME]"
  const firstNamePattern = /aria-label="First Name ([^"]+)"/gi;
  const lastNamePattern = /aria-label="Last Name ([^"]+)"/gi;
  
  const firstNames = [];
  const lastNames = [];
  
  let match;
  while ((match = firstNamePattern.exec(html)) !== null) {
    firstNames.push(match[1].trim());
  }
  
  while ((match = lastNamePattern.exec(html)) !== null) {
    lastNames.push(match[1].trim());
  }
  
  // Pair up first and last names
  for (let i = 0; i < Math.max(firstNames.length, lastNames.length); i++) {
    let firstName = (firstNames[i] || '').trim();
    let lastName = (lastNames[i] || '').trim();
    
    // Clean up names - remove "First Name" or "Last Name" text if accidentally included
    firstName = firstName.replace(/^(First\s+Name|First Name)\s*/i, '').trim();
    lastName = lastName.replace(/^(Last\s+Name|Last Name)\s*/i, '').trim();
    
    // Skip if name contains "First Name" or "Last Name" as part of the actual name
    if (firstName && !firstName.toLowerCase().includes('first name') && 
        !firstName.toLowerCase().includes('last name')) {
      if (lastName && !lastName.toLowerCase().includes('first name') && 
          !lastName.toLowerCase().includes('last name')) {
        // Deduplicate
        const key = `${firstName}:${lastName}`;
        if (!persons.some(p => `${p.firstName}:${p.lastName}` === key)) {
          persons.push({ firstName, lastName });
        }
      }
    }
  }
  
  return persons;
}

/**
 * Extract phone numbers and email addresses from HTML
 * @param {string} html - HTML content
 * @returns {Array<{phone: string | null, email: string | null}>} - Array of communications
 */
function extractCommunications(html) {
  const communications = [];
  const seen = new Set();
  
  // Extract phone numbers (various formats: 239-444-6150, (239) 444-6150, 239.444.6150, etc.)
  // More specific pattern to avoid matching permit numbers - require area code starting with 2-9
  const phonePattern = /\b([2-9]\d{2}[-.\s]?[2-9]\d{2}[-.\s]?\d{4})\b/g;
  const phoneMatches = [];
  let match;
  
  while ((match = phonePattern.exec(html)) !== null) {
    const phone = match[1].replace(/[-.\s]/g, '-'); // Normalize to XXX-XXX-XXXX format
    // Additional validation: exclude patterns that look like permit numbers (e.g., 648108-2024)
    if (!phone.match(/^\d{6}-\d{4}$/) && !phone.match(/^\d{5}-\d{4}$/)) {
      if (!seen.has(`phone:${phone}`)) {
        seen.add(`phone:${phone}`);
        phoneMatches.push(phone);
      }
    }
  }
  
  // Extract email addresses
  const emailPattern = /\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b/g;
  const emailMatches = [];
  seen.clear();
  
  while ((match = emailPattern.exec(html)) !== null) {
    const email = match[1].toLowerCase().trim();
    if (!seen.has(`email:${email}`)) {
      seen.add(`email:${email}`);
      emailMatches.push(email);
    }
  }
  
  // Extract staff emails (from staff-email attributes)
  const staffEmailPattern = /staff-email="([^"]+)"/gi;
  seen.clear();
  
  while ((match = staffEmailPattern.exec(html)) !== null) {
    const email = match[1].toLowerCase().trim();
    if (!seen.has(`email:${email}`)) {
      seen.add(`email:${email}`);
      if (!emailMatches.includes(email)) {
        emailMatches.push(email);
      }
    }
  }
  
  // Create communication entries - prioritize phone/email pairs found close together
  // For now, create separate entries for each phone and email
  for (const phone of phoneMatches) {
    communications.push({ phone, email: null });
  }
  
  for (const email of emailMatches) {
    // Check if this email already has a phone associated
    const existingIndex = communications.findIndex(c => c.email === null && c.phone);
    if (existingIndex >= 0) {
      communications[existingIndex].email = email;
    } else {
      communications.push({ phone: null, email });
    }
  }
  
  return communications;
}

/**
 * Extract phone and email associated with a specific company/contractor from HTML
 * @param {string} html - HTML content
 * @param {string} companyName - Company name to search near
 * @returns {{phone: string | null, email: string | null}} - Communication info
 */
function extractCompanyCommunication(html, companyName) {
  if (!companyName) return { phone: null, email: null };
  
  // Find company name in HTML
  const companyPattern = new RegExp(escapeRegex(companyName), 'i');
  const companyMatch = companyPattern.exec(html);
  
  if (!companyMatch) return { phone: null, email: null };
  
  // Search in a window around the company name (500 chars before, 2000 chars after)
  const searchStart = Math.max(0, companyMatch.index - 500);
  const searchEnd = companyMatch.index + companyMatch[0].length + 2000;
  const searchArea = html.substring(searchStart, searchEnd);
  
  // Extract phone number near company (more specific pattern to avoid permit numbers)
  const phonePattern = /\b([2-9]\d{2}[-.\s]?[2-9]\d{2}[-.\s]?\d{4})\b/;
  const phoneMatch = searchArea.match(phonePattern);
  let phone = phoneMatch ? phoneMatch[1].replace(/[-.\s]/g, '-') : null;
  // Additional validation: exclude patterns that look like permit numbers
  if (phone && (phone.match(/^\d{6}-\d{4}$/) || phone.match(/^\d{5}-\d{4}$/))) {
    phone = null;
  }
  
  // Extract email near company
  const emailPattern = /\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b/;
  const emailMatch = searchArea.match(emailPattern);
  const email = emailMatch ? emailMatch[1].toLowerCase().trim() : null;
  
  return { phone, email };
}

/**
 * Map raw inspection status to schema enum value
 * @param {string | null} rawStatus - Raw status from HTML
 * @returns {string | null} - Mapped status or null
 */
function mapInspectionStatus(rawStatus) {
  if (!rawStatus) return null;
  
  const lower = rawStatus.toLowerCase().trim();
  
  if (lower.includes('passed') || lower.includes('approved') || lower.includes('completed')) return 'Passed';
  if (lower.includes('failed') || lower.includes('rejected')) return 'Failed';
  if (lower.includes('pending') || lower.includes('waiting')) return 'Pending';
  if (lower.includes('scheduled')) return 'Scheduled';
  if (lower.includes('cancelled') || lower.includes('canceled')) return 'Cancelled';
  if (lower.includes('progress') || lower.includes('in progress')) return 'In Progress';
  
  return null; // Return null if no match (will be omitted)
}

/**
 * Extract inspection data from HTML
 * @param {string} html - HTML content
 * @param {string} permitNumber - Permit number
 * @returns {Array<object>} - Array of inspection entities with inspector information
 */
function extractInspections(html, permitNumber) {
  const inspections = [];
  const seen = new Set();
  
  // Pattern 1: Extract from workflow activities (ng-repeat pattern)
  // Look for inspection activities with description, status, and dates
  const workflowActivityPattern = /ng-repeat="activity in vm\.workflowActivities"[\s\S]{0,5000}?(?=<div[^>]*ng-repeat="activity in vm\.workflowActivities"|<\/div>\s*<\/div>\s*<!-- end ngRepeat|$)/gi;
  let match;
  
  while ((match = workflowActivityPattern.exec(html)) !== null) {
    const activityBlock = match[0];
    
    // Check if this is an inspection activity
    if (!activityBlock.includes('ActivityTypeName == vm.activityType[vm.activityType.Inspection]')) {
      continue;
    }
    
    // Extract inspection description/number (e.g., "106 Final Inspection (R)")
    let inspectionNumber = null;
    const descriptionMatch = activityBlock.match(/aria-label="Description ([^"]+)"/i) ||
                            activityBlock.match(/>\s*(\d+\s+[^<]*Inspection[^<]*)</i);
    if (descriptionMatch) {
      inspectionNumber = descriptionMatch[1].trim();
    }
    
    // Extract status (e.g., "Cancelled", "Failed (No Fee)", "Passed")
    let status = null;
    const statusMatch = activityBlock.match(/wf-summaryLabels[^>]*>[\s\S]{0,200}?-\s*([^<:]+)/i);
    if (statusMatch) {
      status = statusMatch[1].trim();
    }
    
    // Extract completed date
    let completedDate = null;
    const completedDateMatch = activityBlock.match(/CompletedOn[\s\S]{0,100}?>:\s*(\d{1,2}\/\d{1,2}\/\d{4})</i);
    if (completedDateMatch) {
      completedDate = parseDate(completedDateMatch[1]);
    }
    
    // Extract inspector information
    let inspectorFirstName = null;
    let inspectorLastName = null;
    
    // Look for inspector label followed by name in same section
    const inspectorSection = activityBlock.match(/aria-label="Inspector"[^<]{0,2000}/i);
    if (inspectorSection) {
      // Try to find name after "Inspector" label
      const inspectorNameMatch = inspectorSection[0].match(/Inspector[^>]*>[\s\S]{0,200}?>([A-Z][a-z]+\s+[A-Z][a-z]+)</i);
      if (inspectorNameMatch) {
        const fullName = inspectorNameMatch[1].trim().split(/\s+/);
        if (fullName.length >= 2) {
          inspectorFirstName = fullName[0];
          inspectorLastName = fullName.slice(1).join(' ');
        }
      }
    }
    
    const key = inspectionNumber || `activity_${match.index}`;
    if (!seen.has(key) && status) { // Only add if we have a status
      seen.add(key);
      inspections.push({
        inspection_number: inspectionNumber,
        inspection_status: status,
        permit_number: permitNumber,
        scheduled_date: null,
        completed_date: completedDate,
        requested_date: null,
        inspector_first_name: inspectorFirstName,
        inspector_last_name: inspectorLastName,
      });
    }
  }
  
  // Pattern 2: Look for inspection table rows with aria-label patterns
  const inspectionNumberPattern = /aria-label="Inspection Number ([^"]+)"/gi;
  const inspectionStatusPattern = /aria-label="Status ([^"]+)"/gi;
  const inspectionDatePattern = /aria-label="(?:Scheduled|Requested|Completed) Date ([^"]+)"/gi;
  
  const inspectionNumbers = [];
  const inspectionStatuses = [];
  const inspectionDates = [];
  
  while ((match = inspectionNumberPattern.exec(html)) !== null) {
    const num = match[1].trim();
    if (num && !seen.has(`num_${num}`)) {
      seen.add(`num_${num}`);
      inspectionNumbers.push(num);
    }
  }
  
  while ((match = inspectionStatusPattern.exec(html)) !== null) {
    const status = match[1].trim();
    if (status) {
      inspectionStatuses.push(status);
    }
  }
  
  while ((match = inspectionDatePattern.exec(html)) !== null) {
    const dateStr = match[1].trim();
    if (dateStr) {
      const parsedDate = parseDate(dateStr);
      if (parsedDate) {
        inspectionDates.push(parsedDate);
      }
    }
  }
  
  // Pattern 3: Create entries from inspection numbers if not already found
  for (const inspectionNumber of inspectionNumbers) {
    const exists = inspections.some(i => i.inspection_number === inspectionNumber);
    if (!exists) {
      const status = inspectionStatuses.length > 0 ? inspectionStatuses[0] : null;
      const date = inspectionDates.length > 0 ? inspectionDates[0] : null;
      inspections.push({
        inspection_number: inspectionNumber,
        inspection_status: status,
        permit_number: permitNumber,
        scheduled_date: date,
        completed_date: date,
        requested_date: null,
        inspector_first_name: null,
        inspector_last_name: null,
      });
    }
  }
  
  // Pattern 4: Look for inspection table rows
  const inspectionRowPattern = /<tr[^>]*>[\s\S]{0,3000}?inspection[^<]*\/tr>/gi;
  
  while ((match = inspectionRowPattern.exec(html)) !== null) {
    const row = match[0];
    const key = `row_${match.index}`;
    if (seen.has(key)) continue;
    
    let inspectionNumber = null;
    const numMatch = row.match(/inspection[_\s-]*number[^>]*>([^<]+)</i) || 
                     row.match(/#\s*(\d+)/i);
    if (numMatch) {
      inspectionNumber = numMatch[1].trim();
    }
    
    let status = null;
    const statusMatch = row.match(/status[^>]*>([^<]+)</i) || 
                       row.match(/(approved|passed|failed|pending|scheduled|completed|cancelled)[^<]*/i);
    if (statusMatch) {
      status = statusMatch[1].trim();
    }
    
    const dateMatches = row.match(/\d{1,2}\/\d{1,2}\/\d{4}/g);
    let scheduledDate = null;
    let completedDate = null;
    
    if (dateMatches) {
      for (const dateStr of dateMatches) {
        const parsedDate = parseDate(dateStr);
        if (parsedDate) {
          if (!scheduledDate) scheduledDate = parsedDate;
          else if (!completedDate) completedDate = parsedDate;
        }
      }
    }
    
    if (status || inspectionNumber) {
      seen.add(key);
      inspections.push({
        inspection_number: inspectionNumber,
        inspection_status: status,
        permit_number: permitNumber,
        scheduled_date: scheduledDate,
        completed_date: completedDate,
        requested_date: null,
        inspector_first_name: null,
        inspector_last_name: null,
      });
    }
  }
  
  // Pattern 5: Extract from final inspection date if no inspections found
  if (inspections.length === 0) {
    const finalInspectionDate = parseDate(extractByLabel(html, 'Finalized Date:'));
    if (finalInspectionDate) {
      inspections.push({
        inspection_number: null,
        inspection_status: 'Passed',
        permit_number: permitNumber,
        completed_date: finalInspectionDate,
        scheduled_date: null,
        requested_date: null,
        inspector_first_name: null,
        inspector_last_name: null,
      });
    }
  }
  
  // Map all inspection statuses to schema enum values
  return inspections.map(inspection => ({
    ...inspection,
    inspection_status: mapInspectionStatus(inspection.inspection_status),
  })).filter(inspection => inspection.inspection_status !== null);
}

/**
 * Extract parcel information from HTML
 * @param {string} html - HTML content
 * @returns {Array<{parcelIdentifier: string}>} - Array of parcel identifiers
 */
function extractParcels(html) {
  const parcels = [];
  const seen = new Set();
  
  /**
   * Validate if a string looks like a parcel identifier
   * Parcel numbers typically: start with numbers, contain dashes, dots, and letters
   * Examples: "34-47-25-B2-00260.016D", "12-34-56-A1-00001.001"
   * Exclude: currency values like "$572.78", plain numbers, etc.
   */
  function isValidParcelId(str) {
    if (!str || typeof str !== 'string') return false;
    const trimmed = str.trim();
    
    // Exclude currency values
    if (trimmed.startsWith('$') || trimmed.match(/^\$[\d,]+\.?\d*$/)) return false;
    
    // Must have reasonable length (parcel numbers are usually 10+ characters)
    if (trimmed.length < 8) return false;
    
    // Must contain at least one number
    if (!/[0-9]/.test(trimmed)) return false;
    
    // Must contain dashes (parcel numbers typically have dashes)
    if (!/-/.test(trimmed)) return false;
    
    // Should match pattern: numbers, dashes, numbers/letters, dots, numbers/letters
    // Examples: "34-47-25-B2-00260.016D", "12-34-56-A1-00001.001"
    const parcelPattern = /^[\d\-\.A-Za-z]+$/;
    if (!parcelPattern.test(trimmed)) return false;
    
    return true;
  }
  
  // Pattern 1: Extract using extractByLabel function (most reliable)
  const parcelIdByLabel = extractByLabel(html, 'Parcel Number:');
  if (parcelIdByLabel && isValidParcelId(parcelIdByLabel) && !seen.has(parcelIdByLabel)) {
    seen.add(parcelIdByLabel);
    parcels.push({ parcelIdentifier: parcelIdByLabel.trim() });
  }
  
  // Pattern 2: Look for "Parcel Number" label followed by value
  // <label>Parcel Number:</label> or <h3>Parcel Number</h3> followed by value
  const parcelPattern1 = /(?:<label[^>]*>|>)\s*Parcel\s+Number\s*:?\s*(?:<\/label>|<\/h3>|<\/label)[\s\S]{0,300}?<[^>]*ng-binding[^>]*>([^<]+)<\/[^>]*>/gi;
  let match;
  
  while ((match = parcelPattern1.exec(html)) !== null) {
    const parcelId = cleanHtmlText(match[1]).trim();
    if (parcelId && isValidParcelId(parcelId) && !seen.has(parcelId)) {
      seen.add(parcelId);
      parcels.push({ parcelIdentifier: parcelId });
    }
  }
  
  // Pattern 3: aria-label="Parcel Number" or similar
  const parcelPattern2 = /aria-label="Parcel\s+Number[^"]*"[^>]*>([^<]+)</gi;
  while ((match = parcelPattern2.exec(html)) !== null) {
    const parcelId = cleanHtmlText(match[1]).trim();
    if (parcelId && isValidParcelId(parcelId) && !seen.has(parcelId)) {
      seen.add(parcelId);
      parcels.push({ parcelIdentifier: parcelId });
    }
  }
  
  // Pattern 4: Look for parcel number in ng-binding elements after "Parcel" text
  // Support both uppercase and lowercase letters (e.g., "34-47-25-B2-00260.016D")
  const parcelPattern4 = /Parcel[\s\S]{0,300}?<[^>]*ng-binding[^>]*>([\d\-\.A-Za-z]+)<\/[^>]*>/gi;
  while ((match = parcelPattern4.exec(html)) !== null) {
    const parcelId = match[1].trim();
    if (parcelId && isValidParcelId(parcelId) && !seen.has(parcelId)) {
      seen.add(parcelId);
      parcels.push({ parcelIdentifier: parcelId });
    }
  }
  
  // Pattern 5: Look for parcel number format directly: XX-XX-XX-XX-XXXXX.XXX
  const parcelPattern5 = /\b(\d{2}-\d{2}-\d{2}-[A-Za-z0-9]+-\d+\.\d+[A-Za-z]*)\b/gi;
  while ((match = parcelPattern5.exec(html)) !== null) {
    const parcelId = match[1].trim();
    if (parcelId && isValidParcelId(parcelId) && !seen.has(parcelId)) {
      seen.add(parcelId);
      parcels.push({ parcelIdentifier: parcelId });
    }
  }
  
  return parcels;
}

/**
 * Extract address information from HTML
 * @param {string} html - HTML content
 * @returns {object | null} - Address entity or null
 */
function extractAddress(html) {
  // Look for address in title attribute or ng-binding elements
  // Pattern: "9968  PUOPOLO LN    , BONITA SPRINGS, FL,, 34135, United States"
  const addressPattern = /title="([^"]+)"[\s\S]{0,500}?data-ng-if="item\.Address/i;
  const titleMatch = html.match(addressPattern);
  
  if (titleMatch && titleMatch[1]) {
    const addressStr = titleMatch[1];
    // Clean up the address: remove newlines and extra spaces, make it a single line
    const unnormalizedAddress = addressStr
      .replace(/\n/g, ' ') // Replace newlines with spaces
      .replace(/\s+/g, ' ') // Replace multiple spaces with single space
      .replace(/\s*,\s*/g, ', ') // Normalize comma spacing (single space after comma)
      .replace(/,\s*,/g, ',') // Remove double commas
      .replace(/,\s*$/, '') // Remove trailing comma
      .trim(); // Remove leading/trailing whitespace
    
    // Only return address if we have content
    if (unnormalizedAddress && unnormalizedAddress.length > 0) {
      return {
        unnormalized_address: unnormalizedAddress,
        request_identifier: null, // Will be set later
      };
    }
  }
  
  // Alternative: Extract from ng-binding elements
  // Look for "9968 PUOPOLO LN, BONITA SPRINGS, FL, 34135"
  const addressTextPattern = /(\d+\s+[A-Z\s]+(?:LN|ST|AVE|RD|DR|CT|BLVD|WAY|PL|CIR|PKWY|BLVD)[\s\S]{0,200}?)(?:,\s*([A-Z\s]+),\s*([A-Z]{2})(?:,\s*(\d{5}))?)/i;
  const textMatch = html.match(addressTextPattern);
  
  if (textMatch) {
    const streetPart = textMatch[1].trim();
    const cityPart = textMatch[2] ? textMatch[2].trim() : null;
    const statePart = textMatch[3] ? textMatch[3].trim() : null;
    const zipPart = textMatch[4] ? textMatch[4].trim() : null;
    
    // Build unnormalized address from matched parts as a single line
    const unnormalizedParts = [streetPart];
    if (cityPart) unnormalizedParts.push(cityPart);
    if (statePart) unnormalizedParts.push(statePart);
    if (zipPart) unnormalizedParts.push(zipPart);
    const unnormalizedAddress = unnormalizedParts.join(', ');
    
    if (unnormalizedAddress && unnormalizedAddress.length > 0) {
      return {
        unnormalized_address: unnormalizedAddress,
        request_identifier: null,
      };
    }
  }
  
  return null;
}

/**
 * Extract file/attachment information from HTML
 * @param {string} html - HTML content
 * @returns {Array<{name: string, url: string}>} - Array of file references
 */
function extractFiles(html) {
  const files = [];
  
  // Pattern: href or src with file extensions
  const filePattern = /(?:href|src)=["']([^"']*\.(pdf|doc|xls|jpg|png|gif|docx|xlsx)[^"']*)["']/gi;
  let match;
  
  while ((match = filePattern.exec(html)) !== null) {
    const url = match[1];
    const fileName = url.split('/').pop().replace(/%20/g, ' ');
    if (fileName && !fileName.includes('header') && !fileName.includes('logo')) {
      // Find if this file is associated with a specific inspection
      // Look for inspection number in the file name or nearby HTML
      const fileIndex = match.index;
      const searchArea = html.substring(Math.max(0, fileIndex - 1000), Math.min(html.length, fileIndex + match[0].length + 1000));
      
      // Try to find inspection number in file name (e.g., "108983-ENG_INSPECTION_REPORT.pdf")
      let associatedInspectionNumber = null;
      const inspectionNumberInFileName = fileName.match(/(\d+)[-_]INSPECTION/i) || 
                                         fileName.match(/INSPECTION[_\-](\d+)/i);
      if (inspectionNumberInFileName) {
        associatedInspectionNumber = inspectionNumberInFileName[1];
      }
      
      // Also try to find inspection context in nearby HTML (look for inspection numbers or activity blocks)
      if (!associatedInspectionNumber) {
        const inspectionContextMatch = searchArea.match(/inspection[_\s-]*number[^>]*>([^<]+)</i) ||
                                      searchArea.match(/aria-label=".*?inspection[^"]*?(\d+)/i) ||
                                      searchArea.match(/>\s*(\d+)\s+[^<]*inspection[^<]*</i);
        if (inspectionContextMatch) {
          associatedInspectionNumber = inspectionContextMatch[1].trim();
        }
      }
      
      files.push({ 
        name: fileName, 
        url,
        associatedInspectionNumber // Store which inspection this file belongs to
      });
    }
  }
  
  // Deduplicate
  const seen = new Set();
  return files.filter(f => {
    if (seen.has(f.name)) return false;
    seen.add(f.name);
    return true;
  });
}

/**
 * Escape special regex characters
 * @param {string} str - String to escape
 * @returns {string} - Escaped string
 */
function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Clean HTML text by removing tags and decoding entities
 * @param {string} text - Text to clean
 * @returns {string} - Cleaned text
 */
function cleanHtmlText(text) {
  return text
    .replace(/<[^>]*>/g, '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Extract permit number from HTML title
 * @param {string} html - HTML content
 * @returns {string | null} - Permit number or null
 */
function extractPermitNumber(html) {
  const titleMatch = html.match(/<title>([^<]+)<\/title>/i);
  if (titleMatch) {
    return titleMatch[1].trim();
  }
  return extractByLabel(html, 'Permit Number:');
}

/**
 * Parse currency string to number
 * @param {string | null} currencyStr - Currency string (e.g., "$22,388.22")
 * @returns {number | null} - Parsed number or null
 */
function parseCurrency(currencyStr) {
  if (!currencyStr) return null;
  const cleaned = currencyStr.replace(/[$,]/g, '');
  const num = parseFloat(cleaned);
  return isNaN(num) ? null : num;
}

/**
 * Parse date string to ISO format (YYYY-MM-DD) or null
 * Handles multiple date formats: MM/DD/YYYY, YYYY-MM-DD, MM-DD-YYYY
 * @param {string | null} dateStr - Date string
 * @returns {string | null} - ISO date string or null
 */
function parseDate(dateStr) {
  if (!dateStr || typeof dateStr !== 'string') return null;
  const trimmed = dateStr.trim();
  if (!trimmed) return null;
  
  const formats = [
    /(\d{1,2})\/(\d{1,2})\/(\d{4})/,  // MM/DD/YYYY
    /(\d{4})-(\d{1,2})-(\d{1,2})/,    // YYYY-MM-DD
    /(\d{1,2})-(\d{1,2})-(\d{4})/,    // MM-DD-YYYY
  ];
  
  for (const format of formats) {
    const match = trimmed.match(format);
    if (match) {
      let isoDate;
      if (format === formats[0] || format === formats[2]) {
        const [, month, day, year] = match;
        isoDate = `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
      } else {
        isoDate = match[0];
      }
      
      // Validate date
      const dateObj = new Date(isoDate);
      if (!isNaN(dateObj.getTime()) && dateObj.getFullYear() >= 1900 && dateObj.getFullYear() <= 2100) {
        return isoDate;
      }
    }
  }
  
  return null;
}

/**
 * Parse square feet string to number
 * @param {string | null} sqftStr - Square feet string (e.g., "1,234.56")
 * @returns {number | null} - Parsed number or null
 */
function parseSquareFeet(sqftStr) {
  if (!sqftStr) return null;
  const cleaned = sqftStr.replace(/,/g, '');
  const num = parseFloat(cleaned);
  return isNaN(num) || num === 0 ? null : num;
}

/**
 * Parse CSV content manually
 * @param {string} csvContent - CSV file content
 * @returns {Array<Record<string, string>>} - Array of record objects
 */
function parseCSV(csvContent) {
  const lines = csvContent.trim().split('\n');
  if (lines.length === 0) return [];
  
  const headers = lines[0].split(',').map(h => h.trim());
  const records = [];
  
  for (let i = 1; i < lines.length; i++) {
    const values = lines[i].split(',').map(v => v.trim());
    const record = {};
    headers.forEach((header, index) => {
      record[header] = values[index] || '';
    });
    records.push(record);
  }
  
  return records;
}

/**
 * Extract property improvement data from HTML following Elephant Lexicon schema
 * @param {string} htmlContent - HTML content
 * @param {object} sourceRequest - Source HTTP request metadata (without request_identifier)
 * @param {string} requestId - Request identifier
 * @returns {object} - Property improvement data and related entities
 */
function extractFromHtml(htmlContent, sourceRequest, requestId) {
  // Extract basic permit info
  const permitNumber = extractPermitNumber(htmlContent);
  let rawType = extractByLabel(htmlContent, 'Type:');
  
  // Also try to extract from the main permit type label if not found
  if (!rawType) {
    // Look for permit type in various places
    const permitTypePatterns = [
      /<label[^>]*>Type[^<]*<\/label>[\s\S]{0,500}?<p[^>]*form-control-static[^>]*>([\s\S]{0,200}?)<\/p>/i,
      /Type:[\s\S]{0,200}?<[^>]*ng-binding[^>]*>([\s\S]{0,200}?)<\/[^>]*>/i,
    ];
    
    for (const pattern of permitTypePatterns) {
      const match = htmlContent.match(pattern);
      if (match && match[1]) {
        const extracted = cleanHtmlText(match[1]).trim();
        if (extracted && extracted.length > 0 && !extracted.toLowerCase().includes('location address')) {
          rawType = extracted;
          break;
        }
      }
    }
  }
  
  // If Type: label still not found, try extracting from aria-label="Type Contractor - [TYPE]"
  // This is a fallback for when the permit type isn't clearly labeled
  if (!rawType) {
    const contractorTypePattern = /aria-label="Type\s+Contractor\s*-\s*([^"]+)"/i;
    const contractorMatch = htmlContent.match(contractorTypePattern);
    if (contractorMatch && contractorMatch[1]) {
      const contractorType = contractorMatch[1].trim();
      // Map common contractor types to permit types
      if (contractorType.toLowerCase().includes('electrical')) {
        rawType = 'BLD - Trade - Electrical Permit';
      } else if (contractorType.toLowerCase().includes('mechanical') || contractorType.toLowerCase().includes('hvac')) {
        rawType = 'BLD - Trade - Mechanical Permit';
      } else if (contractorType.toLowerCase().includes('plumbing')) {
        rawType = 'BLD - Trade - Plumbing Permit';
      } else if (contractorType.toLowerCase().includes('gas')) {
        rawType = 'BLD - Trade - Gas Permit';
      }
    }
  }
  
  const rawStatus = extractByLabel(htmlContent, 'Status:');
  const projectName = extractByLabel(htmlContent, 'Project Name:');
  
  // Extract dates
  const appliedDate = parseDate(extractByLabel(htmlContent, 'Applied Date:'));
  const issueDate = parseDate(extractByLabel(htmlContent, 'Issue Date:'));
  const expireDate = parseDate(extractByLabel(htmlContent, 'Expire Date:'));
  const finalizedDate = parseDate(extractByLabel(htmlContent, 'Finalized Date:'));
  
  // Extract measurements
  const squareFeet = parseSquareFeet(extractByLabel(htmlContent, 'Square Feet:'));
  
  // Extract fee - try multiple patterns
  let feeStr = extractByLabel(htmlContent, 'Total Fees:');
  // If extractByLabel fails, try direct pattern matching
  if (!feeStr) {
    // Pattern: Total Fees: ... <p ... form-control-static ... ng-binding ...> ... $572.78 ... </p>
    const feePattern = /Total\s+Fees:[\s\S]{0,1000}?<p[\s\S]{0,500}?form-control-static[\s\S]{0,500}?ng-binding[\s\S]{0,500}?>[\s\S]*?(\$[\d,]+\.\d+)[\s\S]*?<\/p>/i;
    const feeMatch = htmlContent.match(feePattern);
    if (feeMatch && feeMatch[1]) {
      feeStr = feeMatch[1];
    }
  }
  let fee = null;
  if (feeStr) {
    // Remove $ and commas, parse as number
    const cleaned = feeStr.replace(/[$,]/g, '').trim();
    const parsed = parseFloat(cleaned);
    fee = isNaN(parsed) || parsed === 0 ? null : parsed;
  }
  
  // Extract description
  const description = extractByLabel(htmlContent, 'Description:');
  
  // Map to Lexicon enums
  // Check if rawType exists in mapping (including null values)
  const improvementType = rawType && rawType in PERMIT_TYPE_MAPPING
    ? PERMIT_TYPE_MAPPING[rawType]
    : (rawType && rawType.toLowerCase().includes('solar') ? null : 'GeneralBuilding');
    
  const improvementStatus = rawStatus && STATUS_MAPPING[rawStatus]
    ? STATUS_MAPPING[rawStatus]
    : 'InProgress';
  
  const improvementAction = determineImprovementAction(rawType);
  const contractorType = determineContractorType(projectName);
  const isDisasterRecoveryFlag = isDisasterRecovery(projectName, description);
  
  // Determine is_owner_builder (check for owner/builder indicators in description or project name)
  const ownerBuilderText = `${projectName || ''} ${description || ''}`.toLowerCase();
  const isOwnerBuilder = ownerBuilderText.includes('owner') && 
                        (ownerBuilderText.includes('builder') || ownerBuilderText.includes('build'));
  
  // Build schema-compliant property improvement object
  const propertyImprovement = {
    source_http_request: sourceRequest,
    request_identifier: requestId,
    improvement_type: improvementType,
    improvement_status: improvementStatus,
    completion_date: finalizedDate,
    contractor_type: contractorType || null,
    permit_required: true,
  };
  
  // Optional fields (only add if not null/undefined)
  if (improvementAction) propertyImprovement.improvement_action = improvementAction;
  if (permitNumber) propertyImprovement.permit_number = permitNumber;
  if (appliedDate) propertyImprovement.application_received_date = appliedDate;
  if (issueDate) propertyImprovement.permit_issue_date = issueDate;
  if (finalizedDate) propertyImprovement.permit_close_date = finalizedDate;
  if (finalizedDate) propertyImprovement.final_inspection_date = finalizedDate;
  if (isDisasterRecoveryFlag !== null) propertyImprovement.is_disaster_recovery = isDisasterRecoveryFlag;
  if (isOwnerBuilder !== null && isOwnerBuilder !== undefined) propertyImprovement.is_owner_builder = isOwnerBuilder;
  if (fee !== null) propertyImprovement.fee = fee;
  
  // Extract additional entities from HTML
  const contractors = extractContractors(htmlContent);
  const persons = extractPersons(htmlContent);
  const files = extractFiles(htmlContent);
  const inspections = extractInspections(htmlContent, permitNumber || null);
  const parcels = extractParcels(htmlContent);
  const address = extractAddress(htmlContent);
  
  // Map persons to contractors by matching First Name/Last Name
  const contractorPersonMap = new Map(); // contractor index -> person indices array
  
  console.log(`[EXTRACT] Found ${contractors.length} contractors and ${persons.length} persons`);
  
  // Helper function to normalize names for matching
  const normalizeName = (name) => {
    if (!name) return '';
    return name.toLowerCase().trim().replace(/\s+/g, ' ');
  };
  
  for (let i = 0; i < contractors.length; i++) {
    const contractor = contractors[i];
    const personIndices = [];
    
    console.log(`[EXTRACT] Contractor ${i}: ${contractor.name}, personMatches: ${contractor.personMatches ? contractor.personMatches.length : 0}`);
    
    // Match persons to this contractor using personMatches from extraction
    if (contractor.personMatches && contractor.personMatches.length > 0) {
      for (const personMatch of contractor.personMatches) {
        const matchFirstName = normalizeName(personMatch.firstName);
        const matchLastName = normalizeName(personMatch.lastName);
        
        console.log(`[EXTRACT] Looking for person: "${personMatch.firstName}" "${personMatch.lastName}" (normalized: "${matchFirstName}" "${matchLastName}")`);
        
        // Find matching person in persons array - try exact match first
        let personIndex = persons.findIndex(p => {
          const pFirstName = normalizeName(p.firstName);
          const pLastName = normalizeName(p.lastName);
          
          // Both names must match
          if (matchFirstName && matchLastName && pFirstName && pLastName) {
            return pFirstName === matchFirstName && pLastName === matchLastName;
          }
          
          // Try matching by last name only if first names are empty
          if (!matchFirstName && !pFirstName && matchLastName && pLastName) {
            return pLastName === matchLastName;
          }
          
          // Try matching by first name only if last names are empty
          if (matchFirstName && pFirstName && !matchLastName && !pLastName) {
            return pFirstName === matchFirstName;
          }
          
          return false;
        });
        
        if (personIndex >= 0) {
          if (!personIndices.includes(personIndex)) {
            personIndices.push(personIndex);
            console.log(`[EXTRACT] ✓ Matched person index ${personIndex}: "${persons[personIndex].firstName}" "${persons[personIndex].lastName}"`);
          }
        } else {
          console.log(`[EXTRACT] ✗ No match found for "${personMatch.firstName}" "${personMatch.lastName}"`);
          console.log(`[EXTRACT] Available persons: ${persons.map((p, idx) => `${idx}: "${p.firstName}" "${p.lastName}"`).join(', ')}`);
        }
      }
    }
    
    // If no matches found via personMatches, try to match by proximity/index if we have equal numbers
    if (personIndices.length === 0 && persons.length > 0 && i < persons.length) {
      console.log(`[EXTRACT] No personMatches found, trying index-based fallback for contractor ${i}`);
      personIndices.push(i);
    }
    
    contractorPersonMap.set(i, personIndices);
    console.log(`[EXTRACT] Contractor ${i} final person indices: ${personIndices.length > 0 ? personIndices.join(', ') : 'NONE'}`);
  }
  
  // Create layout/structure/utility entities based on improvement type
  // These entities will be created even if specific fields (like squareFeet or issueDate) are missing
  // Fields will be set to null when data is not available
  
  // Layout: create only when we can determine what space/room is being added
  // Layout entities should only be created when there's actual space_type information
  // Don't create empty layout entities with all null values (especially space_type)
  // Only create layouts for PoolSpaInstallation and ScreenEnclosure which have specific space types
  // For ResidentialConstruction, CommercialConstruction, BuildingAddition - we don't have enough
  // information to determine the specific space type, so don't create layout entities
  const shouldCreateLayoutEntity = (improvementType === 'PoolSpaInstallation' || improvementType === 'ScreenEnclosure');
  
  // Structure: create for applicable structural improvement types (issueDate can be null)
  // Exclude GeneralBuilding as per requirements - only create for specific structural types
  const shouldCreateStructureEntity = shouldCreateStructure(improvementType) && improvementType !== 'GeneralBuilding';
  
  // Utility: create for applicable utility types (issueDate can be null)
  const shouldCreateUtilityEntity = shouldCreateUtility(improvementType);
  
  // Create related entities - only when we have meaningful data to populate
  // Log for debugging
  if (improvementType === 'GeneralBuilding') {
    console.log(`[DEBUG] GeneralBuilding detected - shouldCreateStructureEntity: ${shouldCreateStructureEntity}, issueDate: ${issueDate}`);
  }
  
  // Extract communications for contractors
  const contractorCommunications = contractors.map(c => extractCompanyCommunication(htmlContent, c.name));
  
  // Extract inspector emails and create inspector companies
  // First, extract all unique staff emails from HTML
  const staffEmailPattern = /staff-email="([^"]+)"/gi;
  const staffEmailMap = new Map(); // email -> staff-name
  let match;
  while ((match = staffEmailPattern.exec(htmlContent)) !== null) {
    const email = match[1].toLowerCase().trim();
    if (!staffEmailMap.has(email)) {
      // Try to find staff-name for this email
      const emailIndex = match.index;
      const beforeEmail = htmlContent.substring(Math.max(0, emailIndex - 200), emailIndex);
      const staffNameMatch = beforeEmail.match(/staff-name="([^"]+)"/i);
      const staffName = staffNameMatch ? staffNameMatch[1].trim() : null;
      staffEmailMap.set(email, staffName);
    }
  }
  
  // Create inspector companies from staff emails
  const inspectorCompanies = [];
  for (const [email, staffName] of staffEmailMap.entries()) {
    // Skip generic emails like "privateprovider"
    if (!email.includes('privateprovider') && email.includes('@')) {
      inspectorCompanies.push({
        name: staffName || 'Inspector',
        email,
      });
    }
  }
  
  // Also try to match inspector names from inspections to staff emails
  for (const inspection of inspections) {
    if (inspection.inspector_first_name || inspection.inspector_last_name) {
      const inspectorName = `${inspection.inspector_first_name || ''} ${inspection.inspector_last_name || ''}`.trim();
      if (inspectorName) {
        // Try to find matching email by staff-name
        for (const [email, staffName] of staffEmailMap.entries()) {
          if (staffName && (
            staffName.toLowerCase().includes(inspectorName.toLowerCase().split(' ').pop() || '') ||
            inspectorName.toLowerCase().includes(staffName.toLowerCase().split(' ').pop() || '')
          )) {
            // Check if we already have this email
            if (!inspectorCompanies.some(ic => ic.email === email)) {
              inspectorCompanies.push({
                name: inspectorName,
                email,
              });
            }
          }
        }
      }
    }
  }
  
  // Create entities and check if they have meaningful data
  const layoutEntity = shouldCreateLayoutEntity ? createLayout(sourceRequest, requestId, improvementType, squareFeet, issueDate) : null;
  const structureEntity = shouldCreateStructureEntity ? createStructure(sourceRequest, requestId, improvementType, issueDate) : null;
  const utilityEntity = shouldCreateUtilityEntity ? createUtility(sourceRequest, requestId, improvementType, issueDate, rawType) : null;
  
  // Only include entities that have meaningful data (not all null)
  const relatedEntities = {
    propertyImprovement,
    layout: layoutEntity && hasMeaningfulData(layoutEntity) ? layoutEntity : null,
    structure: structureEntity && hasMeaningfulData(structureEntity) ? structureEntity : null,
    utility: utilityEntity && hasMeaningfulData(utilityEntity) ? utilityEntity : null,
    companies: contractors.map((c, index) => ({
      source_http_request: sourceRequest,
      request_identifier: requestId,
      name: c.name,
      communication: contractorCommunications[index], // Store communication info for later
    })),
    inspectorCompanies: inspectorCompanies.map(ic => ({
      source_http_request: sourceRequest,
      request_identifier: requestId,
      name: ic.name,
      email: ic.email,
    })),
    persons: persons.map(p => ({
      source_http_request: sourceRequest,
      request_identifier: requestId,
      first_name: formatNameForSchema(p.firstName) || null,
      last_name: formatNameForSchema(p.lastName) || null,
      middle_name: null, // Required by schema but not available in HTML
      prefix_name: null, // Required by schema but not available in HTML
      suffix_name: null, // Required by schema but not available in HTML
      birth_date: null, // Required by schema but not available in HTML
      us_citizenship_status: null, // Required by schema but not available in HTML
      veteran_status: null, // Required by schema but not available in HTML
    })),
    files: files.map(f => {
      const fileEntity = {
        source_http_request: sourceRequest,
        request_identifier: requestId,
        name: f.name,
        original_url: f.url,
      };
      // Store associated inspection number for later relationship creation
      if (f.associatedInspectionNumber) {
        fileEntity.associatedInspectionNumber = f.associatedInspectionNumber;
      }
      return fileEntity;
    }),
    inspections: inspections.map(inspection => {
      const inspectionEntity = {
        inspection_status: inspection.inspection_status, // Already mapped by extractInspections
        permit_number: inspection.permit_number,
      };
      
      // Only include fields if they have values (schema requires strings, not null)
      if (inspection.inspection_number) {
        inspectionEntity.inspection_number = inspection.inspection_number;
      }
      if (inspection.scheduled_date) {
        inspectionEntity.scheduled_date = inspection.scheduled_date;
      }
      if (inspection.completed_date) {
        inspectionEntity.completed_date = inspection.completed_date;
      }
      if (inspection.requested_date) {
        inspectionEntity.requested_date = inspection.requested_date;
      }
      if (inspection.completed_time) {
        inspectionEntity.completed_time = inspection.completed_time;
      }
      
      return inspectionEntity;
    }),
    inspectionsWithInspectors: inspections, // Keep original inspections with inspector info
    contractorPersonMap, // Map contractor index -> person indices
    parcels: parcels.map(p => ({
      source_http_request: sourceRequest,
      request_identifier: requestId,
      parcel_identifier: p.parcelIdentifier,
    })),
    address: address ? {
      ...address,
      request_identifier: requestId,
    } : null,
  };
  
  return relatedEntities;
}

/**
 * Main extraction function
 * @returns {Promise<void>}
 */
async function extractPropertyImprovement() {
  try {
    // Script runs with cwd = /tmp/elephant-transform-XXXXX/
    // Input files are in: /tmp/elephant-transform-XXXXX/input/
    const inputDir = path.join(process.cwd(), 'input');
    const rootDir = process.cwd();
    
    // Try to read input.csv if it exists, otherwise process HTML files directly
    let records = [];
    const inputCsvPath = path.join(inputDir, 'input.csv');
    const rootCsvPath = path.join(rootDir, 'input.csv');
    
    try {
      // Try input/input.csv first, then root input.csv
      let csvContent;
      try {
        csvContent = await fs.readFile(inputCsvPath, 'utf-8');
      } catch {
        csvContent = await fs.readFile(rootCsvPath, 'utf-8');
      }
      records = parseCSV(csvContent);
      console.log(`Found ${records.length} records in CSV`);
    } catch {
      // No CSV file, construct records from HTML filenames
      console.log('No input.csv found, processing HTML files directly...');
      
      // Look for HTML files in input/ directory first, then root
      let htmlFiles = [];
      try {
        const inputFiles = await fs.readdir(inputDir);
        htmlFiles = inputFiles.filter(f => f.endsWith('.html'));
      } catch {
        // input directory might not exist
      }
      
      if (htmlFiles.length === 0) {
        try {
          const rootFiles = await fs.readdir(rootDir);
          htmlFiles = rootFiles.filter(f => f.endsWith('.html'));
        } catch {
          // ignore
        }
      }
      
      for (const htmlFile of htmlFiles) {
        // Extract request_identifier from filename (e.g., "0dd99ff4-46cf-42b4-b51d-3648a2715f7a.html")
        const requestId = htmlFile.replace(/\.html?$/i, '');
        const url = `https://egweb1.cityofbonitasprings.org/energov/selfservice#/permit/${requestId}`;
        records.push({
          request_identifier: requestId,
          method: 'GET',
          url: url,
          multiValueQueryString: '{}'
        });
      }
      console.log(`Found ${records.length} HTML file(s)`);
    }
    
    // Create data directory
    console.log('Creating data directory...');
    await fs.mkdir('data', { recursive: true });
    
    let improvementCount = 0;
    let layoutCount = 0;
    let structureCount = 0;
    let utilityCount = 0;
    let companyCount = 0;
    let personCount = 0;
    let fileCount = 0;
    let inspectionCount = 0;
    let parcelCount = 0;
    let addressCount = 0;
    let relationshipCount = 0;
    
    // Process each record
    for (const record of records) {
      const requestId = record.request_identifier || record['request_identifier'] || `unknown_${Date.now()}`;
      
      // Build source request metadata (request_identifier should NOT be in source_http_request)
      // Ensure URL matches schema pattern: ^https?://[a-zA-Z0-9.-]+(:[0-9]+)?(/[#a-zA-Z0-9._%-]+)*$
      let url = record.url || '';
      if (url && !url.match(/^https?:\/\//i)) {
        // If URL doesn't start with http:// or https://, add https://
        url = url.replace(/^[\/]+/, '').replace(/^https?:[\/]{2}/i, '');
        url = 'https://' + url;
      }
      if (!url || !url.match(/^https?:\/\/[a-zA-Z0-9.-]+/)) {
        // Fallback to a valid URL pattern if still invalid
        url = `https://example.com/property-improvement/${requestId}`;
      }
      
      const sourceRequest = {
        method: record.method || 'GET',
        url: url,
      };
      
      // Add multiValueQueryString if present
      if (record.multiValueQueryString) {
        try {
          sourceRequest.multiValueQueryString = JSON.parse(record.multiValueQueryString);
        } catch {
          sourceRequest.multiValueQueryString = {};
        }
      }
      
      // Find corresponding HTML file (check input directory first, then root)
      let htmlFile = null;
      let htmlFilePath = null;
      
      try {
        const inputFiles = await fs.readdir(inputDir);
        htmlFile = inputFiles.find(f => 
          f.endsWith('.html') && 
          (f.includes(requestId) || f.includes(requestId.replace(/-/g, '')))
        );
        if (htmlFile) {
          htmlFilePath = path.join(inputDir, htmlFile);
        }
      } catch {
        // input directory might not exist
      }
      
      if (!htmlFile) {
        try {
          const rootFiles = await fs.readdir(rootDir);
          htmlFile = rootFiles.find(f => 
            f.endsWith('.html') && 
            (f.includes(requestId) || f.includes(requestId.replace(/-/g, '')))
          );
          if (htmlFile) {
            htmlFilePath = path.join(rootDir, htmlFile);
          }
        } catch {
          // ignore
        }
      }
      
      if (!htmlFile || !htmlFilePath) {
        console.warn(`⚠️ No HTML file found for request ${requestId}`);
        continue;
      }
      
      console.log(`Processing ${htmlFile}...`);
      const htmlContent = await fs.readFile(htmlFilePath, 'utf-8');
      
      // Extract improvement data and related entities
      const entities = extractFromHtml(htmlContent, sourceRequest, requestId);
      improvementCount++;
      
      // Write property improvement
      const improvementPath = path.join('data', `property_improvement_${improvementCount}.json`);
      await fs.writeFile(improvementPath, JSON.stringify(entities.propertyImprovement, null, 2));
      console.log(`✓ Wrote ${improvementPath}`);
      
      // Write parcels if found and create parcel_has_property_improvement relationships
      for (const parcel of entities.parcels) {
        parcelCount++;
        const parcelPath = path.join('data', `parcel_${parcelCount}.json`);
        await fs.writeFile(parcelPath, JSON.stringify(parcel, null, 2));
        console.log(`✓ Wrote ${parcelPath}`);
        
        // Create relationship: parcel_has_property_improvement (single reference)
        relationshipCount++;
        const relationshipData = {
          from: { '/': `./parcel_${parcelCount}.json` },
          to: { '/': `./property_improvement_${improvementCount}.json` },
        };
        const relationshipPath = path.join('data', `parcel_has_property_improvement_${relationshipCount}.json`);
        await fs.writeFile(relationshipPath, JSON.stringify(relationshipData, null, 2));
        console.log(`✓ Wrote ${relationshipPath} (parcel ${parcelCount} -> property improvement ${improvementCount})`);
      }
      
      // Write address if found and create property_improvement_has_address relationship
      if (entities.address) {
        addressCount++;
        const addressPath = path.join('data', `address_${addressCount}.json`);
        // Write address entity with only unnormalized_address (no broken down fields)
        await fs.writeFile(addressPath, JSON.stringify(entities.address, null, 2));
        console.log(`✓ Wrote ${addressPath}`);
        
        // Create relationship: property_improvement_has_address
        relationshipCount++;
        const relationshipData = {
          from: { '/': `./property_improvement_${improvementCount}.json` },
          to: { '/': `./address_${addressCount}.json` },
        };
        const relationshipPath = path.join('data', `property_improvement_has_address_${relationshipCount}.json`);
        await fs.writeFile(relationshipPath, JSON.stringify(relationshipData, null, 2));
        console.log(`✓ Wrote ${relationshipPath}`);
      }
      
      // Write layout if created
      if (entities.layout) {
        layoutCount++;
        const layoutPath = path.join('data', `layout_${layoutCount}.json`);
        await fs.writeFile(layoutPath, JSON.stringify(entities.layout, null, 2));
        console.log(`✓ Wrote ${layoutPath}`);
        
        // Create relationship (IPLD link format - only from and to)
        relationshipCount++;
        const relationshipData = {
          from: { '/': `./property_improvement_${improvementCount}.json` },
          to: { '/': `./layout_${layoutCount}.json` },
        };
        const relationshipPath = path.join('data', `property_improvement_has_layout_${relationshipCount}.json`);
        await fs.writeFile(relationshipPath, JSON.stringify(relationshipData, null, 2));
        console.log(`✓ Wrote ${relationshipPath}`);
      }
      
      // Write structure if created (only if it was actually created by the condition)
      // Structure should only be created for applicable structural types (excluding GeneralBuilding)
      if (entities.structure && entities.structure !== null) {
        const improvementTypeCheck = entities.propertyImprovement.improvement_type;
        const isValidStructureType = shouldCreateStructure(improvementTypeCheck) && improvementTypeCheck !== 'GeneralBuilding';
        if (!isValidStructureType) {
          console.log(`⚠️ Skipping structure creation - invalid improvement_type: ${improvementTypeCheck}`);
          // Do not write structure file
        } else {
          structureCount++;
          const structurePath = path.join('data', `structure_${structureCount}.json`);
          await fs.writeFile(structurePath, JSON.stringify(entities.structure, null, 2));
          console.log(`✓ Wrote ${structurePath}`);
          
          // Create relationship (IPLD link format - only from and to)
          relationshipCount++;
          const relationshipData = {
            from: { '/': `./property_improvement_${improvementCount}.json` },
            to: { '/': `./structure_${structureCount}.json` },
          };
          const relationshipPath = path.join('data', `property_improvement_has_structure_${relationshipCount}.json`);
          await fs.writeFile(relationshipPath, JSON.stringify(relationshipData, null, 2));
          console.log(`✓ Wrote ${relationshipPath}`);
        }
      }
      
      // Write utility if created
      if (entities.utility) {
        utilityCount++;
        const utilityPath = path.join('data', `utility_${utilityCount}.json`);
        await fs.writeFile(utilityPath, JSON.stringify(entities.utility, null, 2));
        console.log(`✓ Wrote ${utilityPath}`);
        
        // Create relationship (IPLD link format - only from and to)
        relationshipCount++;
        const relationshipData = {
          from: { '/': `./property_improvement_${improvementCount}.json` },
          to: { '/': `./utility_${utilityCount}.json` },
        };
        const relationshipPath = path.join('data', `property_improvement_has_utility_${relationshipCount}.json`);
        await fs.writeFile(relationshipPath, JSON.stringify(relationshipData, null, 2));
        console.log(`✓ Wrote ${relationshipPath}`);
      }
      
      // Write companies if found
      const companyIndexMap = new Map(); // Track which company index corresponds to which file number
      let communicationCount = 0;
      const communicationIndexMap = new Map(); // Track communication index -> file number
      
      for (let i = 0; i < entities.companies.length; i++) {
        const company = entities.companies[i];
        // Remove communication from company before writing (it's stored separately)
        const { communication, ...companyData } = company;
        
        companyCount++;
        companyIndexMap.set(i, companyCount); // Map original index to file number
        const companyPath = path.join('data', `company_${companyCount}.json`);
        await fs.writeFile(companyPath, JSON.stringify(companyData, null, 2));
        console.log(`✓ Wrote ${companyPath}`);
        
        // Create relationship: property_improvement_has_contractor (array relationship)
        relationshipCount++;
        const relationshipData = {
          from: { '/': `./property_improvement_${improvementCount}.json` },
          to: { '/': `./company_${companyCount}.json` },
        };
        const relationshipPath = path.join('data', `property_improvement_has_contractor_${relationshipCount}.json`);
        await fs.writeFile(relationshipPath, JSON.stringify(relationshipData, null, 2));
        console.log(`✓ Wrote ${relationshipPath}`);
        
        // Create communication entity if phone or email exists
        if (communication && (communication.phone || communication.email)) {
          communicationCount++;
          const communicationEntity = {
            phone_number: communication.phone || null,
            email_address: communication.email || null,
          };
          const communicationPath = path.join('data', `communication_${communicationCount}.json`);
          await fs.writeFile(communicationPath, JSON.stringify(communicationEntity, null, 2));
          console.log(`✓ Wrote ${communicationPath}`);
          
          // Create relationship: company_has_communication
          relationshipCount++;
          const commRelationshipData = {
            from: { '/': `./company_${companyCount}.json` },
            to: { '/': `./communication_${communicationCount}.json` },
          };
          const commRelationshipPath = path.join('data', `company_has_communication_${relationshipCount}.json`);
          await fs.writeFile(commRelationshipPath, JSON.stringify(commRelationshipData, null, 2));
          console.log(`✓ Wrote ${commRelationshipPath}`);
        }
      }
      
      // Write inspector companies and their communications
      for (const inspectorCompany of entities.inspectorCompanies || []) {
        companyCount++;
        const inspectorCompanyPath = path.join('data', `company_${companyCount}.json`);
        const { email, ...inspectorCompanyData } = inspectorCompany;
        await fs.writeFile(inspectorCompanyPath, JSON.stringify(inspectorCompanyData, null, 2));
        console.log(`✓ Wrote ${inspectorCompanyPath} (inspector company)`);
        
        // Create communication for inspector email
        if (email) {
          communicationCount++;
          const communicationEntity = {
            phone_number: null,
            email_address: email,
          };
          const communicationPath = path.join('data', `communication_${communicationCount}.json`);
          await fs.writeFile(communicationPath, JSON.stringify(communicationEntity, null, 2));
          console.log(`✓ Wrote ${communicationPath} (inspector communication)`);
          
          // Create relationship: company_has_communication
          relationshipCount++;
          const commRelationshipData = {
            from: { '/': `./company_${companyCount}.json` },
            to: { '/': `./communication_${communicationCount}.json` },
          };
          const commRelationshipPath = path.join('data', `company_has_communication_${relationshipCount}.json`);
          await fs.writeFile(commRelationshipPath, JSON.stringify(commRelationshipData, null, 2));
          console.log(`✓ Wrote ${commRelationshipPath} (inspector company -> communication)`);
        }
      }
      
      // Capture initial person count before we process inspections (inspectors will be added later)
      const initialPersonCountBeforeInspectors = entities.persons.length;
      
      // Write persons if found and track mapping (excluding inspectors added later, they'll be written after inspections)
      const personIndexMap = new Map(); // Track which person index corresponds to which file number
      for (let i = 0; i < initialPersonCountBeforeInspectors; i++) {
        const person = entities.persons[i];
        personCount++;
        personIndexMap.set(i, personCount); // Map original index to file number
        const personPath = path.join('data', `person_${personCount}.json`);
        await fs.writeFile(personPath, JSON.stringify(person, null, 2));
        console.log(`✓ Wrote ${personPath}`);
      }
      
      // Create contractor_has_person relationships
      if (entities.contractorPersonMap && entities.contractorPersonMap.size > 0) {
        console.log(`[DEBUG] contractorPersonMap has ${entities.contractorPersonMap.size} entries`);
        console.log(`[DEBUG] companyIndexMap: ${Array.from(companyIndexMap.entries()).map(([k, v]) => `${k}->${v}`).join(', ')}`);
        console.log(`[DEBUG] personIndexMap: ${Array.from(personIndexMap.entries()).map(([k, v]) => `${k}->${v}`).join(', ')}`);
        
        for (const [contractorIndex, personIndices] of entities.contractorPersonMap.entries()) {
          console.log(`[DEBUG] Contractor ${contractorIndex} has ${personIndices.length} persons: ${personIndices.join(', ')}`);
          const companyFileNumber = companyIndexMap.get(contractorIndex);
          if (!companyFileNumber) {
            console.log(`[WARN] No company file number found for contractor index ${contractorIndex} (total companies: ${entities.companies.length})`);
            continue;
          }
          
          if (personIndices.length === 0) {
            console.log(`[WARN] Contractor ${contractorIndex} has no associated persons`);
            continue;
          }
          
          for (const personIndex of personIndices) {
            const personFileNumber = personIndexMap.get(personIndex);
            if (!personFileNumber) {
              console.log(`[WARN] No person file number found for person index ${personIndex} (total persons: ${entities.persons.length}, initialPersonCount: ${initialPersonCountBeforeInspectors})`);
              continue;
            }
            
            relationshipCount++;
            const relationshipData = {
              from: { '/': `./company_${companyFileNumber}.json` },
              to: { '/': `./person_${personFileNumber}.json` },
            };
            const relationshipPath = path.join('data', `contractor_has_person_${relationshipCount}.json`);
            await fs.writeFile(relationshipPath, JSON.stringify(relationshipData, null, 2));
            console.log(`✓ Wrote ${relationshipPath} (contractor ${companyFileNumber} -> person ${personFileNumber})`);
          }
        }
      } else {
        console.log(`[WARN] contractorPersonMap is empty or undefined (contractors: ${entities.companies.length}, persons: ${entities.persons.length})`);
        // Try to match all persons to all contractors as a fallback if contractorPersonMap is empty
        if (entities.companies.length > 0 && entities.persons.length > 0 && entities.companies.length <= entities.persons.length) {
          console.log(`[INFO] Attempting fallback: matching persons to contractors by index`);
          for (let i = 0; i < Math.min(entities.companies.length, entities.persons.length); i++) {
            const companyFileNumber = companyIndexMap.get(i);
            const personFileNumber = personIndexMap.get(i);
            if (companyFileNumber && personFileNumber) {
              relationshipCount++;
              const relationshipData = {
                from: { '/': `./company_${companyFileNumber}.json` },
                to: { '/': `./person_${personFileNumber}.json` },
              };
              const relationshipPath = path.join('data', `contractor_has_person_${relationshipCount}.json`);
              await fs.writeFile(relationshipPath, JSON.stringify(relationshipData, null, 2));
              console.log(`✓ Wrote ${relationshipPath} (fallback: contractor ${companyFileNumber} -> person ${personFileNumber})`);
            }
          }
        }
      }
      
      // Write files if found and track mapping
      const fileIndexMap = new Map(); // Track which file index corresponds to which file number
      for (let i = 0; i < entities.files.length; i++) {
        const file = entities.files[i];
        // Remove associatedInspectionNumber from file entity before writing (it's only for relationship creation)
        const { associatedInspectionNumber, ...fileData } = file;
        fileCount++;
        fileIndexMap.set(i, fileCount); // Map original index to file number
        const filePath = path.join('data', `file_${fileCount}.json`);
        await fs.writeFile(filePath, JSON.stringify(fileData, null, 2));
        console.log(`✓ Wrote ${filePath}`);
        
        // Store the associated inspection number for later relationship creation
        if (associatedInspectionNumber) {
          fileIndexMap.set(`inspection_${i}`, associatedInspectionNumber);
        }
        
        // Create relationship: property_improvement_has_file (array relationship)
        relationshipCount++;
        const relationshipData = {
          from: { '/': `./property_improvement_${improvementCount}.json` },
          to: { '/': `./file_${fileCount}.json` },
        };
        const relationshipPath = path.join('data', `property_improvement_has_file_${relationshipCount}.json`);
        await fs.writeFile(relationshipPath, JSON.stringify(relationshipData, null, 2));
        console.log(`✓ Wrote ${relationshipPath}`);
      }
      
      // Write inspections if found and track mapping
      // Also extract inspector information from inspections
      const inspectionIndexMap = new Map(); // Track which inspection index corresponds to which file number
      const inspectionInspectorMap = new Map(); // Track inspection index -> inspector person index
      const originalInspections = entities.inspectionsWithInspectors || [];
      
      for (let i = 0; i < entities.inspections.length; i++) {
        const inspection = entities.inspections[i];
        inspectionCount++;
        inspectionIndexMap.set(i, inspectionCount); // Map original index to file number
        
        // Check if this inspection has an inspector
        const originalInspection = originalInspections[i];
        if (originalInspection && originalInspection.inspector_first_name && originalInspection.inspector_last_name) {
          // Check if inspector person already exists in persons array
          let inspectorPersonIndex = entities.persons.findIndex(p => 
            p.firstName && p.lastName &&
            p.firstName.toLowerCase().trim() === originalInspection.inspector_first_name.toLowerCase().trim() &&
            p.lastName.toLowerCase().trim() === originalInspection.inspector_last_name.toLowerCase().trim()
          );
          
          // If inspector not found, create a new person entity
          if (inspectorPersonIndex < 0) {
            entities.persons.push({
              firstName: originalInspection.inspector_first_name,
              lastName: originalInspection.inspector_last_name,
            });
            inspectorPersonIndex = entities.persons.length - 1;
            console.log(`[INSPECTOR] Created new person entity for inspector: ${originalInspection.inspector_first_name} ${originalInspection.inspector_last_name}`);
          }
          
          inspectionInspectorMap.set(i, inspectorPersonIndex);
        }
        
        const inspectionPath = path.join('data', `inspection_${inspectionCount}.json`);
        await fs.writeFile(inspectionPath, JSON.stringify(inspection, null, 2));
        console.log(`✓ Wrote ${inspectionPath}`);
        
        // Create relationship: property_improvement_has_inspection (array relationship)
        relationshipCount++;
        const relationshipData = {
          from: { '/': `./property_improvement_${improvementCount}.json` },
          to: { '/': `./inspection_${inspectionCount}.json` },
        };
        const relationshipPath = path.join('data', `property_improvement_has_inspection_${relationshipCount}.json`);
        await fs.writeFile(relationshipPath, JSON.stringify(relationshipData, null, 2));
        console.log(`✓ Wrote ${relationshipPath}`);
      }
      
      // Write inspector persons if we found any (they were added to entities.persons array)
      // Track which inspector person indices we've already written
      const writtenInspectorPersonIndices = new Set();
      
      for (const [inspectionIdx, inspectorPersonIdx] of inspectionInspectorMap.entries()) {
        // Skip if this person was already written (was in initial persons array)
        if (inspectorPersonIdx < initialPersonCountBeforeInspectors) {
          // Person already exists, use existing mapping
          continue;
        }
        
        // Skip if we've already written this inspector person
        if (writtenInspectorPersonIndices.has(inspectorPersonIdx)) {
          continue;
        }
        
        // This is a new inspector person that needs to be written
        const person = entities.persons[inspectorPersonIdx];
        personCount++;
        personIndexMap.set(inspectorPersonIdx, personCount);
        const personPath = path.join('data', `person_${personCount}.json`);
        const personEntity = {
          source_http_request: sourceRequest,
          request_identifier: requestId,
          first_name: formatNameForSchema(person.firstName) || null,
          last_name: formatNameForSchema(person.lastName) || null,
          middle_name: null, // Required by schema but not available in HTML
          prefix_name: null, // Required by schema but not available in HTML
          suffix_name: null, // Required by schema but not available in HTML
          birth_date: null, // Required by schema but not available in HTML
          us_citizenship_status: null, // Required by schema but not available in HTML
          veteran_status: null, // Required by schema but not available in HTML
        };
        await fs.writeFile(personPath, JSON.stringify(personEntity, null, 2));
        console.log(`✓ Wrote ${personPath} (inspector: ${person.firstName} ${person.lastName})`);
        writtenInspectorPersonIndices.add(inspectorPersonIdx);
      }
      
      // Create inspection_has_person relationships for inspectors
      for (const [inspectionIdx, inspectorPersonIdx] of inspectionInspectorMap.entries()) {
        const inspectionFileNumber = inspectionIndexMap.get(inspectionIdx);
        const inspectorPersonFileNumber = personIndexMap.get(inspectorPersonIdx);
        
        if (inspectionFileNumber && inspectorPersonFileNumber) {
          relationshipCount++;
          const relationshipData = {
            from: { '/': `./inspection_${inspectionFileNumber}.json` },
            to: { '/': `./person_${inspectorPersonFileNumber}.json` },
          };
          const relationshipPath = path.join('data', `inspection_has_person_${relationshipCount}.json`);
          await fs.writeFile(relationshipPath, JSON.stringify(relationshipData, null, 2));
          console.log(`✓ Wrote ${relationshipPath} (inspection ${inspectionFileNumber} -> inspector ${inspectorPersonFileNumber})`);
        }
      }
      
      // Create inspection_has_file relationships for files associated with specific inspections
      if (entities.files && entities.files.length > 0 && entities.inspections && entities.inspections.length > 0) {
        for (let fileIdx = 0; fileIdx < entities.files.length; fileIdx++) {
          const file = entities.files[fileIdx];
          const fileFileNumber = fileIndexMap.get(fileIdx);
          
          if (!fileFileNumber) continue;
          
          // Check if file has an associated inspection number
          const associatedInspectionNumber = fileIndexMap.get(`inspection_${fileIdx}`);
          if (associatedInspectionNumber) {
            // Find matching inspection by inspection number
            const matchingInspectionIdx = entities.inspections.findIndex(
              insp => insp.inspection_number && 
              (insp.inspection_number.includes(associatedInspectionNumber) ||
               associatedInspectionNumber.includes(insp.inspection_number))
            );
            
            if (matchingInspectionIdx >= 0) {
              const inspectionFileNumber = inspectionIndexMap.get(matchingInspectionIdx);
              if (inspectionFileNumber) {
                relationshipCount++;
                const relationshipData = {
                  from: { '/': `./inspection_${inspectionFileNumber}.json` },
                  to: { '/': `./file_${fileFileNumber}.json` },
                };
                const relationshipPath = path.join('data', `inspection_has_file_${relationshipCount}.json`);
                await fs.writeFile(relationshipPath, JSON.stringify(relationshipData, null, 2));
                console.log(`✓ Wrote ${relationshipPath} (inspection ${inspectionFileNumber} -> file ${fileFileNumber})`);
              }
            }
          }
        }
      }
    }
    
    // List created files
    const dataDir = 'data';
    const files = await fs.readdir(dataDir);
    const jsonFiles = files.filter(f => f.endsWith('.json'));
    console.log(`\n=== Extraction completed successfully ===`);
    console.log(`Working directory: ${process.cwd()}`);
    console.log(`Data directory: ${path.resolve(dataDir)}`);
    console.log(`Created ${jsonFiles.length} JSON files in data/:`);
    jsonFiles.forEach(f => console.log(`  - ${f}`));
    console.log(`\nSummary:`);
    console.log(`  - ${improvementCount} property improvements`);
    console.log(`  - ${layoutCount} layouts`);
    console.log(`  - ${structureCount} structures`);
    console.log(`  - ${utilityCount} utilities`);
    console.log(`  - ${companyCount} companies`);
    console.log(`  - ${personCount} persons`);
    console.log(`  - ${fileCount} files`);
    console.log(`  - ${inspectionCount} inspections`);
    console.log(`  - ${parcelCount} parcels`);
    console.log(`  - ${relationshipCount} relationships`);
    
    // Verify files actually exist
    for (const file of jsonFiles) {
      const filePath = path.join(dataDir, file);
      const stats = await fs.stat(filePath);
      console.log(`  ✓ ${file} (${stats.size} bytes)`);
    }
    
  } catch (error) {
    console.error('❌ Extraction failed:', error);
    console.error('Stack trace:', error.stack);
    process.exit(1);
  }
}

extractPropertyImprovement();
