#!/usr/bin/env node
// @ts-check

/**
 * Export one transactionally consistent Broward Donphan snapshot and optionally
 * stage its immutable bytes in the private oracle-node S3 environment bucket.
 *
 * The exporter holds one repeatable-read, read-only Neon transaction while it
 * writes the property table, permit table, and coverage metadata. S3 writes use
 * a new versioned Broward-only prefix, conditional creates, checksums, private
 * object ACLs, and complete streamed readback verification. It never contacts
 * Filebase, changes IPNS, or edits the published-county catalog.
 */

import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import {
  chmod,
  mkdir,
  open,
  readFile,
  rename,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { pathToFileURL } from "node:url";

import {
  GetObjectAclCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { ParquetReader, ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";
import pg from "pg";

import { buildBrowardPermitRouteStatus } from "./broward-neon-recovery-dashboard.mjs";

const require = createRequire(import.meta.url);
const ipfsHash =
  /** @type {{of:(content:NodeJS.ReadableStream)=>Promise<string>}} */ (
    require("ipfs-only-hash")
  );
const { Client } = pg;

const EXPECTED_PROJECT_ID = "raspy-frost-51580436";
const PRODUCTION_ENDPOINT_PREFIX = "ep-mute-leaf";
const COUNTY = "broward";
const APPRAISAL_SOURCE = "broward_appraiser";
const PERMIT_SOURCE_PATTERN = "broward%permits";
const APPRAISAL_DENOMINATOR = 534_309;
const AWS_REGION = "us-east-1"; // pragma: allowlist secret
const S3_BUCKET = "elephant-oracle-node-environmentbucket-mmsoo3xbdi80";
const S3_ROOT = "publication-staging/broward/donphan/snapshots";
const DEFAULT_OUTPUT_ROOT = "downloads/broward/donphan-staging";
const DEFAULT_BATCH_SIZE = 2_000;
const SNAPSHOT_SCHEMA_VERSION = "oracle-node.broward-donphan-snapshot.v1";

/**
 * @typedef {object} FieldSchema
 * @property {string} name - Stable Donphan field name in physical order.
 * @property {"BOOLEAN"|"DOUBLE"|"INT64"|"UTF8"} type - Parquet scalar type.
 * @property {boolean} nullable - Whether the Parquet field is optional.
 *
 * @typedef {object} SnapshotOptions
 * @property {string} outputRoot - Parent directory for versioned local output.
 * @property {number} batchSize - Rows fetched per server-cursor round trip.
 * @property {boolean} upload - Whether to stage verified bytes in S3.
 *
 * @typedef {object} SnapshotCounts
 * @property {number} propertyRows - Broward appraisal property rows.
 * @property {number} distinctPropertyIds - Distinct property primary keys.
 * @property {number} distinctFolios - Distinct Broward request folios.
 * @property {number} nullFolios - Properties without a source folio.
 * @property {number} permitRows - Broward permit rows in the artifact.
 * @property {number} distinctPermitIds - Distinct permit primary keys.
 * @property {number} linkedPermits - Permits linked to a Broward property.
 * @property {number} unlinkedPermits - Permits with no property foreign key.
 * @property {number} foreignLinkedPermits - Non-null links outside Broward.
 * @property {number} linkedProperties - Broward properties with permits.
 * @property {number} roofingPermits - Explicitly classified roofing permits.
 * @property {number} permitSourceSystemCount - Loaded permit source systems.
 * @property {number} sunbizAddressMatches - Exact matched Sunbiz address roles.
 * @property {number} sunbizRegistrations - Distinct matched registrations.
 * @property {number} sunbizProperties - Distinct matched Broward properties.
 * @property {number} bbbProfiles - Loaded BBB profiles in the prior BBB slice.
 * @property {number} bbbMatchedProperties - Properties joined through a permit contractor.
 * @property {string | null} appraisalFirstLoadedAt - Earliest appraisal load timestamp.
 * @property {string | null} appraisalLastLoadedAt - Latest appraisal load timestamp.
 * @property {string | null} permitFirstLoadedAt - Earliest permit load timestamp.
 * @property {string | null} permitLastLoadedAt - Latest permit load timestamp.
 * @property {string | null} sunbizFirstLoadedAt - Earliest matched Sunbiz load timestamp.
 * @property {string | null} sunbizLastLoadedAt - Latest matched Sunbiz update timestamp.
 * @property {string | null} bbbFirstLoadedAt - Earliest BBB load timestamp.
 * @property {string | null} bbbLastLoadedAt - Latest BBB load timestamp.
 *
 * @typedef {object} PermitSourceCount
 * @property {string} sourceSystem - Public permit source-system key.
 * @property {number} rowCount - Permit rows from the source.
 * @property {number} linkedCount - Source rows linked to Broward properties.
 * @property {number} unlinkedCount - Source rows without a property link.
 * @property {number} roofingCount - Explicit roofing rows from the source.
 *
 * @typedef {object} ExportResult
 * @property {number} rowCount - Rows physically appended to Parquet.
 * @property {Record<string,number>} nonNullCounts - Physical non-null values by field.
 *
 * @typedef {object} ArtifactIdentity
 * @property {string} name - Logical artifact name.
 * @property {string} fileName - Version-directory relative filename.
 * @property {string} contentType - HTTP media type used for staging.
 * @property {string} s3Key - Immutable private staging object key.
 * @property {number} sizeBytes - Exact byte length.
 * @property {string} sha256 - Lowercase SHA-256 hex digest.
 * @property {string} checksumSha256 - Base64 SHA-256 for S3 checksum headers.
 * @property {string} cid - Locally computed UnixFS/IPFS CID.
 *
 * @typedef {object} StagedArtifact
 * @property {ArtifactIdentity} artifact - Expected local identity.
 * @property {boolean} headSizeMatches - HeadObject length matched.
 * @property {boolean} headChecksumMatches - HeadObject checksum matched.
 * @property {boolean} readbackSizeMatches - Streamed GetObject length matched.
 * @property {boolean} readbackSha256Matches - Streamed GetObject digest matched.
 * @property {boolean} readbackCidMatches - Readback UnixFS CID matched.
 * @property {boolean} privateAcl - Object ACL has no public or authenticated-user grants.
 * @property {boolean} encryptedAtRest - S3 reported server-side encryption.
 */

/** @type {readonly FieldSchema[]} */
export const PROPERTY_FIELDS = Object.freeze([
  { name: "property_id", type: "UTF8", nullable: false },
  { name: "property_cid", type: "UTF8", nullable: true },
  { name: "request_identifier", type: "UTF8", nullable: true },
  { name: "parcel_identifier", type: "UTF8", nullable: true },
  { name: "source_system", type: "UTF8", nullable: true },
  { name: "county_name", type: "UTF8", nullable: true },
  { name: "state_code", type: "UTF8", nullable: true },
  { name: "address_street", type: "UTF8", nullable: true },
  { name: "address_city", type: "UTF8", nullable: true },
  { name: "address_zip", type: "UTF8", nullable: true },
  { name: "latitude", type: "DOUBLE", nullable: true },
  { name: "longitude", type: "DOUBLE", nullable: true },
  { name: "lot_size_acre", type: "DOUBLE", nullable: true },
  { name: "lot_area_sqft", type: "DOUBLE", nullable: true },
  { name: "exterior_wall_material", type: "UTF8", nullable: true },
  { name: "roof_covering_material", type: "UTF8", nullable: true },
  { name: "property_type", type: "UTF8", nullable: true },
  { name: "property_usage_type", type: "UTF8", nullable: true },
  { name: "built_year", type: "INT64", nullable: true },
  { name: "livable_floor_area", type: "DOUBLE", nullable: true },
  { name: "total_area", type: "DOUBLE", nullable: true },
  { name: "assessed_value", type: "DOUBLE", nullable: true },
  { name: "market_value", type: "DOUBLE", nullable: true },
  { name: "land_value", type: "DOUBLE", nullable: true },
  { name: "avm_value", type: "DOUBLE", nullable: true },
  { name: "owner_name", type: "UTF8", nullable: true },
  { name: "owners_text", type: "UTF8", nullable: true },
  { name: "owner_count", type: "INT64", nullable: true },
  { name: "owner_occupied", type: "BOOLEAN", nullable: true },
  { name: "last_sale_date", type: "UTF8", nullable: true },
  { name: "last_sale_price", type: "DOUBLE", nullable: true },
  { name: "subdivision", type: "UTF8", nullable: true },
  { name: "has_permits", type: "BOOLEAN", nullable: true },
  { name: "permit_count", type: "INT64", nullable: true },
  { name: "has_sunbiz_tenant", type: "BOOLEAN", nullable: true },
  { name: "has_bbb_contractor", type: "BOOLEAN", nullable: true },
  { name: "has_pa_corp_tenant", type: "BOOLEAN", nullable: true },
  { name: "hoa_flag", type: "BOOLEAN", nullable: true },
]);

/** @type {readonly FieldSchema[]} */
export const PERMIT_FIELDS = Object.freeze([
  { name: "property_improvement_id", type: "UTF8", nullable: false },
  { name: "property_id", type: "UTF8", nullable: true },
  { name: "parcel_identifier", type: "UTF8", nullable: true },
  { name: "permit_number", type: "UTF8", nullable: true },
  { name: "improvement_type", type: "UTF8", nullable: true },
  { name: "improvement_status", type: "UTF8", nullable: true },
  { name: "improvement_action", type: "UTF8", nullable: true },
  { name: "permit_issue_date", type: "UTF8", nullable: true },
  { name: "application_received_date", type: "UTF8", nullable: true },
  { name: "final_inspection_date", type: "UTF8", nullable: true },
  { name: "permit_close_date", type: "UTF8", nullable: true },
  { name: "completion_date", type: "UTF8", nullable: true },
  { name: "expiration_date", type: "UTF8", nullable: true },
  { name: "opened_date", type: "UTF8", nullable: true },
  { name: "source_system", type: "UTF8", nullable: true },
  { name: "county_name", type: "UTF8", nullable: true },
  { name: "project_description", type: "UTF8", nullable: true },
  { name: "description", type: "UTF8", nullable: true },
  { name: "estimated_job_value", type: "DOUBLE", nullable: true },
  { name: "fee", type: "DOUBLE", nullable: true },
]);

const PROPERTY_SQL = `
WITH county_properties AS MATERIALIZED (
  SELECT property_id,parcel_id,address_id,request_identifier,parcel_identifier,
         property_type,property_usage_type,property_structure_built_year,
         livable_floor_area,total_area,subdivision,source_system
  FROM public.properties
  WHERE source_system=$1
),
tax_latest AS (
  SELECT DISTINCT ON (t.property_id)
    t.property_id,t.property_assessed_value_amount AS assessed_value,
    t.property_market_value_amount AS market_value,
    t.property_land_amount AS land_value
  FROM public.taxes t
  JOIN county_properties cp ON cp.property_id=t.property_id
  ORDER BY t.property_id,t.tax_year DESC NULLS LAST,t.tax_id
),
avm AS (
  SELECT pv.property_id,max(pv.current_avm_value) AS avm_value
  FROM public.property_valuations pv
  JOIN county_properties cp ON cp.property_id=pv.property_id
  GROUP BY pv.property_id
),
structure_pick AS (
  SELECT DISTINCT ON (s.property_id)
    s.property_id,s.exterior_wall_material_primary,s.roof_covering_material
  FROM public.structures s
  JOIN county_properties cp ON cp.property_id=s.property_id
  ORDER BY s.property_id,s.structure_id
),
lot_pick AS (
  SELECT DISTINCT ON (l.property_id)
    l.property_id,l.lot_size_acre,l.lot_area_sqft
  FROM public.lots l
  JOIN county_properties cp ON cp.property_id=l.property_id
  ORDER BY l.property_id,l.lot_id
),
layout_area AS (
  SELECT l.property_id,sum(l.livable_area_sq_ft) AS livable_area_sq_ft,
         sum(l.area_under_air_sq_ft) AS area_under_air_sq_ft
  FROM public.layouts l
  JOIN county_properties cp ON cp.property_id=l.property_id
  GROUP BY l.property_id
),
geom_pick AS (
  SELECT DISTINCT ON (g.property_id) g.property_id,g.latitude,g.longitude
  FROM public.geometries g
  JOIN county_properties cp ON cp.property_id=g.property_id
  ORDER BY g.property_id,
    (g.latitude IS NOT NULL AND g.longitude IS NOT NULL) DESC,g.geometry_id
),
owners_agg AS (
  SELECT o.property_id,
    string_agg(DISTINCT o.owned_by,' | ' ORDER BY o.owned_by) AS owners_text,
    count(DISTINCT o.owned_by) AS owner_count,
    bool_or(o.owner_occupied_indicator) AS owner_occupied
  FROM public.ownerships o
  JOIN county_properties cp ON cp.property_id=o.property_id
  WHERE nullif(o.owned_by,'') IS NOT NULL
  GROUP BY o.property_id
),
owner_primary AS (
  SELECT DISTINCT ON (o.property_id) o.property_id,o.owned_by AS owner_name
  FROM public.ownerships o
  JOIN county_properties cp ON cp.property_id=o.property_id
  WHERE nullif(o.owned_by,'') IS NOT NULL
  ORDER BY o.property_id,o.ownership_percentage DESC NULLS LAST,o.ownership_id
),
sale_latest AS (
  SELECT DISTINCT ON (sh.property_id)
    sh.property_id,sh.ownership_transfer_date,sh.purchase_price_amount
  FROM public.sales_histories sh
  JOIN county_properties cp ON cp.property_id=sh.property_id
  ORDER BY sh.property_id,sh.ownership_transfer_date DESC NULLS LAST,
           sh.sales_history_id
),
permit_counts AS (
  SELECT pi.property_id,count(*) AS permit_count
  FROM public.property_improvements pi
  JOIN county_properties cp ON cp.property_id=pi.property_id
  WHERE pi.source_system LIKE $2
  GROUP BY pi.property_id
),
sunbiz_properties AS (
  SELECT DISTINCT cp.property_id
  FROM county_properties cp
  JOIN public.business_registration_addresses bra
    ON bra.address_id=cp.address_id
   AND bra.source_system='sunbiz'
   AND bra.address_match_method='normalized_address_hash'
   AND bra.address_match_confidence='exact'
),
bbb_properties AS (
  SELECT DISTINCT pi.property_id
  FROM public.property_improvements pi
  JOIN public.business_reputation_profiles profile
    ON profile.company_id=pi.contractor_company_id
   AND profile.provider ILIKE '%bbb%'
  JOIN county_properties cp ON cp.property_id=pi.property_id
  WHERE pi.source_system LIKE $2
),
situs AS (
  SELECT DISTINCT ON (request_identifier) request_identifier,full_address
  FROM public.unnormalized_addresses
  WHERE source_system=$1 AND nullif(request_identifier,'') IS NOT NULL
  ORDER BY request_identifier,unnormalized_address_id
)
SELECT
  p.property_id::text AS property_id,
  NULL::text AS property_cid,
  p.request_identifier,
  p.parcel_identifier,
  p.source_system,
  par.county_name,
  par.state_code,
  a.street_number,
  a.street_name,
  a.street_suffix_type,
  a.city_name,
  a.postal_code,
  a.unnormalized_address,
  situs.full_address AS situs_full_address,
  geom.latitude::text,
  geom.longitude::text,
  lot.lot_size_acre::text,
  lot.lot_area_sqft::text,
  structure.exterior_wall_material_primary AS exterior_wall_material,
  structure.roof_covering_material,
  p.property_type,
  p.property_usage_type,
  p.property_structure_built_year AS built_year,
  p.livable_floor_area,
  p.total_area,
  layout.livable_area_sq_ft::text AS layout_livable_area_sq_ft,
  layout.area_under_air_sq_ft::text AS layout_area_under_air_sq_ft,
  tax.assessed_value::text,
  tax.market_value::text,
  tax.land_value::text,
  avm.avm_value::text,
  owner_primary.owner_name,
  owners.owners_text,
  owners.owner_count::text,
  owners.owner_occupied,
  sale.ownership_transfer_date::text AS last_sale_date,
  sale.purchase_price_amount::text AS last_sale_price,
  p.subdivision,
  (permit_counts.permit_count IS NOT NULL) AS has_permits,
  coalesce(permit_counts.permit_count,0)::text AS permit_count,
  (sunbiz_properties.property_id IS NOT NULL) AS has_sunbiz_tenant,
  (bbb_properties.property_id IS NOT NULL) AS has_bbb_contractor
FROM county_properties p
LEFT JOIN public.parcels par ON par.parcel_id=p.parcel_id
LEFT JOIN public.addresses a ON a.address_id=p.address_id
LEFT JOIN geom_pick geom ON geom.property_id=p.property_id
LEFT JOIN lot_pick lot ON lot.property_id=p.property_id
LEFT JOIN layout_area layout ON layout.property_id=p.property_id
LEFT JOIN structure_pick structure ON structure.property_id=p.property_id
LEFT JOIN tax_latest tax ON tax.property_id=p.property_id
LEFT JOIN avm ON avm.property_id=p.property_id
LEFT JOIN owner_primary ON owner_primary.property_id=p.property_id
LEFT JOIN owners_agg owners ON owners.property_id=p.property_id
LEFT JOIN sale_latest sale ON sale.property_id=p.property_id
LEFT JOIN permit_counts ON permit_counts.property_id=p.property_id
LEFT JOIN sunbiz_properties ON sunbiz_properties.property_id=p.property_id
LEFT JOIN bbb_properties ON bbb_properties.property_id=p.property_id
LEFT JOIN situs ON situs.request_identifier=p.request_identifier
ORDER BY p.request_identifier,p.property_id`;

const PERMIT_SQL = `
SELECT
  pi.property_improvement_id::text AS property_improvement_id,
  pi.property_id::text AS property_id,
  pi.parcel_identifier,
  pi.permit_number,
  pi.improvement_type,
  pi.improvement_status,
  pi.improvement_action,
  pi.permit_issue_date::text AS permit_issue_date,
  pi.application_received_date::text AS application_received_date,
  pi.final_inspection_date::text AS final_inspection_date,
  pi.permit_close_date::text AS permit_close_date,
  pi.completion_date::text AS completion_date,
  pi.expiration_date::text AS expiration_date,
  pi.opened_date::text AS opened_date,
  pi.source_system,
  par.county_name,
  pi.project_description,
  pi.description,
  pi.estimated_job_value::text AS estimated_job_value,
  pi.fee::text AS fee
FROM public.property_improvements pi
LEFT JOIN public.parcels par ON par.parcel_id=pi.parcel_id
WHERE pi.source_system LIKE $1
ORDER BY pi.property_improvement_id`;

const SNAPSHOT_COUNTS_SQL = `
WITH props AS MATERIALIZED (
  SELECT property_id,address_id,request_identifier,loaded_at
  FROM public.properties
  WHERE source_system=$1
),
permits AS MATERIALIZED (
  SELECT property_improvement_id,property_id,contractor_company_id,
         source_system,loaded_at,
         coalesce(
           more_details->>'is_roof_permit',more_details->>'isRoofPermit'
         )='true' AS is_roofing
  FROM public.property_improvements
  WHERE source_system LIKE $2
),
permit_links AS (
  SELECT
    count(*) FILTER (WHERE p.property_id IS NOT NULL) AS linked_permits,
    count(*) FILTER (WHERE pi.property_id IS NULL) AS unlinked_permits,
    count(*) FILTER (
      WHERE pi.property_id IS NOT NULL AND p.property_id IS NULL
    ) AS foreign_linked_permits,
    count(DISTINCT p.property_id) AS linked_properties
  FROM permits pi
  LEFT JOIN props p ON p.property_id=pi.property_id
),
sunbiz AS (
  SELECT
    count(*) AS address_matches,
    count(DISTINCT bra.business_registration_id) AS registrations,
    count(DISTINCT p.property_id) AS properties,
    min(bra.loaded_at)::text AS first_loaded_at,
    max(bra.updated_at)::text AS last_loaded_at
  FROM props p
  JOIN public.business_registration_addresses bra
    ON bra.address_id=p.address_id
   AND bra.source_system='sunbiz'
   AND bra.address_match_method='normalized_address_hash'
   AND bra.address_match_confidence='exact'
),
bbb AS (
  SELECT count(*) AS profiles,min(loaded_at)::text AS first_loaded_at,
         max(loaded_at)::text AS last_loaded_at
  FROM public.business_reputation_profiles
  WHERE provider ILIKE '%bbb%'
),
bbb_properties AS (
  SELECT count(DISTINCT pi.property_id) AS properties
  FROM permits pi
  JOIN public.business_reputation_profiles profile
    ON profile.company_id=pi.contractor_company_id
   AND profile.provider ILIKE '%bbb%'
  JOIN props p ON p.property_id=pi.property_id
)
SELECT
  transaction_timestamp() AS snapshot_timestamp,
  (SELECT count(*) FROM props) AS property_rows,
  (SELECT count(DISTINCT property_id) FROM props) AS distinct_property_ids,
  (SELECT count(DISTINCT request_identifier) FROM props) AS distinct_folios,
  (SELECT count(*) FROM props WHERE nullif(request_identifier,'') IS NULL)
    AS null_folios,
  (SELECT count(*) FROM permits) AS permit_rows,
  (SELECT count(DISTINCT property_improvement_id) FROM permits)
    AS distinct_permit_ids,
  permit_links.*,
  (SELECT count(*) FROM permits WHERE is_roofing) AS roofing_permits,
  (SELECT count(DISTINCT source_system) FROM permits)
    AS permit_source_system_count,
  sunbiz.address_matches AS sunbiz_address_matches,
  sunbiz.registrations AS sunbiz_registrations,
  sunbiz.properties AS sunbiz_properties,
  bbb.profiles AS bbb_profiles,
  bbb_properties.properties AS bbb_matched_properties,
  (SELECT min(loaded_at)::text FROM props) AS appraisal_first_loaded_at,
  (SELECT max(loaded_at)::text FROM props) AS appraisal_last_loaded_at,
  (SELECT min(loaded_at)::text FROM permits) AS permit_first_loaded_at,
  (SELECT max(loaded_at)::text FROM permits) AS permit_last_loaded_at,
  sunbiz.first_loaded_at AS sunbiz_first_loaded_at,
  sunbiz.last_loaded_at AS sunbiz_last_loaded_at,
  bbb.first_loaded_at AS bbb_first_loaded_at,
  bbb.last_loaded_at AS bbb_last_loaded_at
FROM permit_links,sunbiz,bbb,bbb_properties`;

/**
 * Parse the narrow local/S3 staging command.
 *
 * @param {readonly string[]} argv - Arguments after the script path.
 * @returns {SnapshotOptions} Validated immutable staging options.
 */
export function parseSnapshotOptions(argv) {
  const values = new Map();
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--upload") {
      values.set("upload", "true");
      continue;
    }
    const next = argv[index + 1];
    if (
      token === undefined ||
      !token.startsWith("--") ||
      next === undefined ||
      next.startsWith("--")
    ) {
      throw new Error(
        "Options must be --output-root/--batch-size values or --upload",
      );
    }
    values.set(token.slice(2), next);
    index += 1;
  }
  for (const key of values.keys()) {
    if (!["output-root", "batch-size", "upload"].includes(key)) {
      throw new Error(`Unknown option --${key}`);
    }
  }
  const batchSize = Number(values.get("batch-size") ?? DEFAULT_BATCH_SIZE);
  if (
    !Number.isSafeInteger(batchSize) ||
    batchSize < 100 ||
    batchSize > 10_000
  ) {
    throw new Error("--batch-size must be an integer from 100 through 10000");
  }
  return {
    outputRoot: path.resolve(values.get("output-root") ?? DEFAULT_OUTPUT_ROOT),
    batchSize,
    upload: values.get("upload") === "true",
  };
}

/**
 * Derive the immutable path-safe version from the transaction timestamp.
 *
 * @param {string} snapshotTimestamp - Exact ISO transaction timestamp.
 * @returns {string} UTC basic timestamp including milliseconds.
 */
export function snapshotVersion(snapshotTimestamp) {
  const normalized = new Date(snapshotTimestamp).toISOString();
  return normalized.replace(/[-:.]/gu, "");
}

/**
 * Build the only allowed Broward staging prefix.
 *
 * @param {string} version - Version returned by {@link snapshotVersion}.
 * @returns {string} Broward-only immutable S3 prefix without trailing slash.
 */
export function browardSnapshotPrefix(version) {
  if (!/^\d{8}T\d{9}Z$/u.test(version)) {
    throw new Error("Snapshot version must be a basic UTC timestamp");
  }
  return `${S3_ROOT}/${version}`;
}

/**
 * Create a scalar Parquet schema from a stable descriptor.
 *
 * @param {readonly FieldSchema[]} fields - Ordered physical field contract.
 * @returns {ParquetSchema} Parquet writer schema.
 */
function parquetSchema(fields) {
  return new ParquetSchema(
    Object.fromEntries(
      fields.map((field) => [
        field.name,
        {
          type: field.type,
          ...(field.nullable ? { optional: true } : {}),
        },
      ]),
    ),
  );
}

/**
 * Convert a nullable database scalar to trimmed text.
 *
 * @param {unknown} value - PostgreSQL scalar.
 * @returns {string | null} Non-empty text or null.
 */
function text(value) {
  if (value === null || value === undefined) return null;
  const normalized = String(value).trim();
  return normalized.length === 0 ? null : normalized;
}

/**
 * Convert a nullable PostgreSQL numeric scalar to a finite number.
 *
 * @param {unknown} value - PostgreSQL numeric/string scalar.
 * @returns {number | null} Finite number or null.
 */
function number(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Convert a nullable PostgreSQL count to an exact safe integer.
 *
 * @param {unknown} value - Bigint/integer scalar.
 * @param {string} name - Aggregate name for fail-closed errors.
 * @returns {number} Non-negative safe integer.
 */
function count(value, name) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`Invalid aggregate ${name}`);
  }
  return parsed;
}

/**
 * Parse a free-text `STREET, CITY, FL ZIP` situs without deriving state.
 *
 * @param {unknown} value - Nullable unnormalized address.
 * @returns {{street:string|null,city:string|null,postalCode:string|null}} Components.
 */
export function parseSitusAddress(value) {
  const raw = text(value);
  if (raw === null) {
    return { street: null, city: null, postalCode: null };
  }
  const segments = raw
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
  const last = segments.at(-1) ?? "";
  const match =
    /\b[A-Za-z]{2}\s+(\d{5})(?:-\d{4})?\s*$/u.exec(last) ??
    /\b(\d{5})(?:-\d{4})?\s*$/u.exec(last);
  const postalCode = match?.[1] ?? null;
  if (match !== null) {
    const remaining = last.slice(0, match.index).trim();
    if (remaining.length === 0 || /^[A-Za-z]{2}$/u.test(remaining)) {
      segments.pop();
    } else {
      segments[segments.length - 1] = remaining;
    }
  }
  return {
    street: segments[0] ?? null,
    city: segments.length > 1 ? segments.slice(1).join(", ") : null,
    postalCode,
  };
}

/**
 * Map one flat database row into Donphan's 38-field property contract.
 *
 * @param {Record<string,unknown>} row - Cursor row from {@link PROPERTY_SQL}.
 * @returns {Record<string,string|number|boolean>} Null-free Parquet record.
 */
function propertyRecord(row) {
  const situs = parseSitusAddress(row.situs_full_address);
  const fallback = parseSitusAddress(row.unnormalized_address);
  const structuredStreet = [
    text(row.street_number),
    text(row.street_name),
    text(row.street_suffix_type),
  ]
    .filter((part) => part !== null)
    .join(" ");
  const lotArea = number(row.lot_area_sqft);
  const candidate = {
    property_id: text(row.property_id),
    property_cid: null,
    request_identifier: text(row.request_identifier),
    parcel_identifier: text(row.parcel_identifier),
    source_system: text(row.source_system),
    county_name: text(row.county_name) ?? "Broward",
    state_code: text(row.state_code) ?? "FL",
    address_street:
      situs.street ??
      (structuredStreet.length > 0 ? structuredStreet : null) ??
      fallback.street,
    address_city: situs.city ?? text(row.city_name) ?? fallback.city,
    address_zip:
      situs.postalCode ?? text(row.postal_code) ?? fallback.postalCode,
    latitude: number(row.latitude),
    longitude: number(row.longitude),
    lot_size_acre:
      number(row.lot_size_acre) ?? (lotArea === null ? null : lotArea / 43_560),
    lot_area_sqft: lotArea,
    exterior_wall_material: text(row.exterior_wall_material),
    roof_covering_material: text(row.roof_covering_material),
    property_type: text(row.property_type),
    property_usage_type: text(row.property_usage_type),
    built_year: number(row.built_year),
    livable_floor_area:
      number(row.livable_floor_area) ??
      number(row.layout_livable_area_sq_ft) ??
      number(row.layout_area_under_air_sq_ft),
    total_area: number(row.total_area),
    assessed_value: number(row.assessed_value),
    market_value: number(row.market_value),
    land_value: number(row.land_value),
    avm_value: number(row.avm_value),
    owner_name: text(row.owner_name),
    owners_text: text(row.owners_text),
    owner_count: number(row.owner_count),
    owner_occupied:
      typeof row.owner_occupied === "boolean" ? row.owner_occupied : null,
    last_sale_date: text(row.last_sale_date),
    last_sale_price: number(row.last_sale_price),
    subdivision: text(row.subdivision),
    has_permits: row.has_permits === true,
    permit_count: number(row.permit_count) ?? 0,
    has_sunbiz_tenant: row.has_sunbiz_tenant === true,
    has_bbb_contractor: row.has_bbb_contractor === true,
    has_pa_corp_tenant: false,
    hoa_flag: null,
  };
  if (candidate.property_id === null) {
    throw new Error("Property cursor returned no property_id");
  }
  /** @type {Record<string,string|number|boolean>} */
  const record = {};
  for (const [key, value] of Object.entries(candidate)) {
    if (value !== null) record[key] = value;
  }
  return record;
}

/**
 * Map one database permit into Donphan's 20-field scalar contract.
 *
 * @param {Record<string,unknown>} row - Cursor row from {@link PERMIT_SQL}.
 * @returns {Record<string,string|number>} Null-free Parquet record.
 */
function permitRecord(row) {
  const candidate = {
    property_improvement_id: text(row.property_improvement_id),
    property_id: text(row.property_id),
    parcel_identifier: text(row.parcel_identifier),
    permit_number: text(row.permit_number),
    improvement_type: text(row.improvement_type),
    improvement_status: text(row.improvement_status),
    improvement_action: text(row.improvement_action),
    permit_issue_date: text(row.permit_issue_date),
    application_received_date: text(row.application_received_date),
    final_inspection_date: text(row.final_inspection_date),
    permit_close_date: text(row.permit_close_date),
    completion_date: text(row.completion_date),
    expiration_date: text(row.expiration_date),
    opened_date: text(row.opened_date),
    source_system: text(row.source_system),
    county_name: text(row.county_name) ?? "Broward",
    project_description: text(row.project_description),
    description: text(row.description),
    estimated_job_value: number(row.estimated_job_value),
    fee: number(row.fee),
  };
  if (candidate.property_improvement_id === null) {
    throw new Error("Permit cursor returned no property_improvement_id");
  }
  /** @type {Record<string,string|number>} */
  const record = {};
  for (const [key, value] of Object.entries(candidate)) {
    if (value !== null) record[key] = value;
  }
  return record;
}

/**
 * Stream a declared server cursor into one Parquet file.
 *
 * @param {import("pg").Client} client - Active repeatable-read client.
 * @param {object} options - Cursor and output contract.
 * @param {string} options.cursorName - Trusted static SQL cursor identifier.
 * @param {string} options.sql - Parameterized SELECT statement.
 * @param {readonly unknown[]} options.values - SELECT parameters.
 * @param {string} options.outputPath - Temporary Parquet destination.
 * @param {readonly FieldSchema[]} options.fields - Physical schema descriptor.
 * @param {(row:Record<string,unknown>)=>Record<string,string|number|boolean>} options.mapRow - Row mapper.
 * @param {number} options.batchSize - Rows fetched on each round trip.
 * @returns {Promise<ExportResult>} Physical row and non-null counts.
 */
async function exportCursorToParquet(client, options) {
  const writer = await ParquetWriter.openFile(
    parquetSchema(options.fields),
    options.outputPath,
  );
  /** @type {Record<string,number>} */
  const nonNullCounts = Object.fromEntries(
    options.fields.map((field) => [field.name, 0]),
  );
  let rowCount = 0;
  let cursorDeclared = false;
  try {
    await client.query({
      text: `DECLARE ${options.cursorName} NO SCROLL CURSOR FOR ${options.sql}`,
      values: [...options.values],
    });
    cursorDeclared = true;
    for (;;) {
      const result = await client.query(
        `FETCH FORWARD ${String(options.batchSize)} FROM ${options.cursorName}`,
      );
      if (result.rows.length === 0) break;
      for (const raw of /** @type {Record<string,unknown>[]} */ (result.rows)) {
        const record = options.mapRow(raw);
        await writer.appendRow(record);
        rowCount += 1;
        for (const name of Object.keys(record)) {
          nonNullCounts[name] = (nonNullCounts[name] ?? 0) + 1;
        }
      }
      process.stdout.write(
        `${JSON.stringify({
          event: "broward_snapshot_export_progress",
          artifact: options.cursorName,
          rowCount,
        })}\n`,
      );
    }
  } finally {
    if (cursorDeclared) {
      await client.query(`CLOSE ${options.cursorName}`).catch(() => undefined);
    }
    await writer.close();
  }
  await chmod(options.outputPath, 0o600);
  return { rowCount, nonNullCounts };
}

/**
 * Verify direct runtime configuration before contacting Neon.
 *
 * @param {NodeJS.ProcessEnv} environment - Runtime secrets and expected IDs.
 * @returns {{connectionString:string,expectedBranchId:string,expectedEndpointId:string}} Target.
 */
function requireNeonTarget(environment) {
  const connectionString = environment.DATABASE_URL_UNPOOLED;
  const expectedBranchId = environment.BROWARD_INGEST_NEON_BRANCH_ID;
  const expectedEndpointId = environment.BROWARD_INGEST_NEON_ENDPOINT_ID;
  if (
    typeof connectionString !== "string" ||
    new URL(connectionString).hostname.includes("-pooler") ||
    typeof expectedBranchId !== "string" ||
    !/^br-[a-z0-9-]+$/u.test(expectedBranchId) ||
    typeof expectedEndpointId !== "string" ||
    !/^ep-[a-z0-9-]+$/u.test(expectedEndpointId) ||
    expectedEndpointId.startsWith(PRODUCTION_ENDPOINT_PREFIX)
  ) {
    throw new Error("Verified direct isolated Broward Neon target is required");
  }
  return { connectionString, expectedBranchId, expectedEndpointId };
}

/**
 * Prove repository-fixed project plus secret-sourced branch and endpoint IDs.
 *
 * @param {import("pg").Client} client - Connected Neon client.
 * @param {{expectedBranchId:string,expectedEndpointId:string}} target - Expected IDs.
 * @returns {Promise<void>} Resolves only for the isolated Broward branch.
 */
async function verifyNeonIdentity(client, target) {
  const result = await client.query(
    `SELECT current_setting('neon.project_id',true) AS project_id,
            current_setting('neon.branch_id',true) AS branch_id,
            current_setting('neon.endpoint_id',true) AS endpoint_id`,
  );
  const row = result.rows[0];
  if (
    row?.project_id !== EXPECTED_PROJECT_ID ||
    row.branch_id !== target.expectedBranchId ||
    row.endpoint_id !== target.expectedEndpointId
  ) {
    throw new Error("Snapshot target is not isolated broward-ingest Neon");
  }
}

/**
 * Normalize the one-row aggregate into exact JavaScript counts.
 *
 * @param {Record<string,unknown>} row - Aggregate query result.
 * @returns {{snapshotTimestamp:string,counts:SnapshotCounts}} Snapshot identity and counts.
 */
function normalizeSnapshotCounts(row) {
  const timestamp = row.snapshot_timestamp;
  if (!(timestamp instanceof Date)) {
    throw new Error("Neon returned no transaction timestamp");
  }
  const nullableTimestamp = (
    /** @type {unknown} */ value,
    /** @type {string} */ name,
  ) => {
    if (value === null) return null;
    const normalized = text(value);
    if (normalized === null || !Number.isFinite(Date.parse(normalized))) {
      throw new Error(`Invalid aggregate timestamp ${name}`);
    }
    return new Date(normalized).toISOString();
  };
  return {
    snapshotTimestamp: timestamp.toISOString(),
    counts: {
      propertyRows: count(row.property_rows, "property_rows"),
      distinctPropertyIds: count(
        row.distinct_property_ids,
        "distinct_property_ids",
      ),
      distinctFolios: count(row.distinct_folios, "distinct_folios"),
      nullFolios: count(row.null_folios, "null_folios"),
      permitRows: count(row.permit_rows, "permit_rows"),
      distinctPermitIds: count(row.distinct_permit_ids, "distinct_permit_ids"),
      linkedPermits: count(row.linked_permits, "linked_permits"),
      unlinkedPermits: count(row.unlinked_permits, "unlinked_permits"),
      foreignLinkedPermits: count(
        row.foreign_linked_permits,
        "foreign_linked_permits",
      ),
      linkedProperties: count(row.linked_properties, "linked_properties"),
      roofingPermits: count(row.roofing_permits, "roofing_permits"),
      permitSourceSystemCount: count(
        row.permit_source_system_count,
        "permit_source_system_count",
      ),
      sunbizAddressMatches: count(
        row.sunbiz_address_matches,
        "sunbiz_address_matches",
      ),
      sunbizRegistrations: count(
        row.sunbiz_registrations,
        "sunbiz_registrations",
      ),
      sunbizProperties: count(row.sunbiz_properties, "sunbiz_properties"),
      bbbProfiles: count(row.bbb_profiles, "bbb_profiles"),
      bbbMatchedProperties: count(
        row.bbb_matched_properties,
        "bbb_matched_properties",
      ),
      appraisalFirstLoadedAt: nullableTimestamp(
        row.appraisal_first_loaded_at,
        "appraisal_first_loaded_at",
      ),
      appraisalLastLoadedAt: nullableTimestamp(
        row.appraisal_last_loaded_at,
        "appraisal_last_loaded_at",
      ),
      permitFirstLoadedAt: nullableTimestamp(
        row.permit_first_loaded_at,
        "permit_first_loaded_at",
      ),
      permitLastLoadedAt: nullableTimestamp(
        row.permit_last_loaded_at,
        "permit_last_loaded_at",
      ),
      sunbizFirstLoadedAt: nullableTimestamp(
        row.sunbiz_first_loaded_at,
        "sunbiz_first_loaded_at",
      ),
      sunbizLastLoadedAt: nullableTimestamp(
        row.sunbiz_last_loaded_at,
        "sunbiz_last_loaded_at",
      ),
      bbbFirstLoadedAt: nullableTimestamp(
        row.bbb_first_loaded_at,
        "bbb_first_loaded_at",
      ),
      bbbLastLoadedAt: nullableTimestamp(
        row.bbb_last_loaded_at,
        "bbb_last_loaded_at",
      ),
    },
  };
}

/**
 * Construct coverage with explicit partial and denominator semantics.
 *
 * @param {object} input - Transactionally frozen coverage inputs.
 * @param {string} input.snapshotTimestamp - Exact transaction timestamp.
 * @param {SnapshotCounts} input.counts - Reconciled database aggregates.
 * @param {readonly PermitSourceCount[]} input.permitSources - Source-system counts.
 * @param {ReturnType<typeof buildBrowardPermitRouteStatus>} input.routes - Executable routes.
 * @returns {Record<string,unknown>} MCP-compatible coverage plus explicit partial metadata.
 */
export function buildCoverageSnapshot(input) {
  const sourcePermitRows = input.permitSources.reduce(
    (sum, source) => sum + source.rowCount,
    0,
  );
  const sourceRoofingRows = input.permitSources.reduce(
    (sum, source) => sum + source.roofingCount,
    0,
  );
  const permitRowsAccountedFor =
    input.counts.linkedPermits +
      input.counts.unlinkedPermits +
      input.counts.foreignLinkedPermits ===
    input.counts.permitRows;
  return {
    schemaVersion: SNAPSHOT_SCHEMA_VERSION,
    county: COUNTY,
    countyName: "Broward",
    stateCode: "FL",
    countyFips: "12011",
    exportedAt: input.snapshotTimestamp,
    snapshotTimestamp: input.snapshotTimestamp,
    coverage_status: "supported_partial",
    county_complete: false,
    publicationScope: {
      schemaVersion: "1.0",
      level: "partial",
      denominatorBasis: "county_total",
    },
    denominator_semantics: {
      appraisal: {
        basis: "official_bcpa_gis_distinct_folios",
        expectedCount: APPRAISAL_DENOMINATOR,
        ingestedCount: input.counts.propertyRows,
        unit: "distinct_folio",
      },
      permits: {
        rowExpectedCount: null,
        rowExpectedCountReason:
          "No authoritative countywide permit-record denominator exists across fragmented municipal sources.",
        routeBasis: "current_primary_jurisdiction_routes",
        routeRegistryVersion: input.routes.registryVersion,
        routeCount: input.routes.totalCurrentRoutes,
        supportedRouteCount: input.routes.implementedCurrentRoutes,
        unattendedUnavailableRouteCount:
          input.routes.unattendedUnavailableCurrentRoutes,
        loadedSourceSystemCount: input.counts.permitSourceSystemCount,
      },
    },
    datasets: [
      {
        county: COUNTY,
        source: "appraisal",
        ingested_count: input.counts.propertyRows,
        expected_count: APPRAISAL_DENOMINATOR,
        first_loaded_at: input.counts.appraisalFirstLoadedAt,
        last_loaded_at: input.counts.appraisalLastLoadedAt,
        cid: null,
        ipns_label: null,
      },
      {
        county: COUNTY,
        source: "permits",
        ingested_count: input.counts.permitRows,
        expected_count: null,
        first_loaded_at: input.counts.permitFirstLoadedAt,
        last_loaded_at: input.counts.permitLastLoadedAt,
        cid: null,
        ipns_label: null,
      },
      {
        county: COUNTY,
        source: "corporate",
        ingested_count: input.counts.sunbizRegistrations,
        expected_count: null,
        first_loaded_at: input.counts.sunbizFirstLoadedAt,
        last_loaded_at: input.counts.sunbizLastLoadedAt,
        cid: null,
        ipns_label: null,
      },
      {
        county: COUNTY,
        source: "bbb",
        ingested_count: input.counts.bbbProfiles,
        expected_count: null,
        first_loaded_at: input.counts.bbbFirstLoadedAt,
        last_loaded_at: input.counts.bbbLastLoadedAt,
        cid: null,
        ipns_label: null,
      },
    ],
    reconciliation: {
      propertyRowsMatchDistinctPropertyIds:
        input.counts.propertyRows === input.counts.distinctPropertyIds,
      propertyRowsMatchDistinctFolios:
        input.counts.propertyRows === input.counts.distinctFolios,
      propertyFoliosNonNull: input.counts.nullFolios === 0,
      permitRowsMatchDistinctIds:
        input.counts.permitRows === input.counts.distinctPermitIds,
      permitRowsAccountedFor,
      permitSourceRowsAccountedFor:
        sourcePermitRows === input.counts.permitRows,
      roofingRowsAccountedFor:
        sourceRoofingRows === input.counts.roofingPermits,
      permitSourceSystemCountMatches:
        input.permitSources.length === input.counts.permitSourceSystemCount,
      routeCountsAccountedFor:
        input.routes.implementedCurrentRoutes +
          input.routes.unattendedUnavailableCurrentRoutes ===
        input.routes.totalCurrentRoutes,
      allBalanced:
        input.counts.propertyRows === input.counts.distinctPropertyIds &&
        input.counts.propertyRows === input.counts.distinctFolios &&
        input.counts.nullFolios === 0 &&
        input.counts.permitRows === input.counts.distinctPermitIds &&
        permitRowsAccountedFor &&
        sourcePermitRows === input.counts.permitRows &&
        sourceRoofingRows === input.counts.roofingPermits &&
        input.permitSources.length === input.counts.permitSourceSystemCount &&
        input.routes.implementedCurrentRoutes +
          input.routes.unattendedUnavailableCurrentRoutes ===
          input.routes.totalCurrentRoutes,
    },
    permitJoins: {
      linked: input.counts.linkedPermits,
      unlinked: input.counts.unlinkedPermits,
      foreignLinked: input.counts.foreignLinkedPermits,
      linkedProperties: input.counts.linkedProperties,
      roofing: input.counts.roofingPermits,
      bbbMatchedProperties: input.counts.bbbMatchedProperties,
    },
    enrichmentJoins: {
      sunbizAddressMatches: input.counts.sunbizAddressMatches,
      sunbizRegistrations: input.counts.sunbizRegistrations,
      sunbizProperties: input.counts.sunbizProperties,
      bbbProfiles: input.counts.bbbProfiles,
      bbbMatchedProperties: input.counts.bbbMatchedProperties,
    },
    permitSources: input.permitSources,
    routeCoverage: input.routes,
  };
}

/**
 * Stream a file once for SHA-256 and then once for its UnixFS CID.
 *
 * @param {string} filePath - Local artifact path.
 * @param {object} logical - Stable artifact publication metadata.
 * @param {string} logical.name - Logical artifact name.
 * @param {string} logical.fileName - Relative file name.
 * @param {string} logical.contentType - HTTP content type.
 * @param {string} logical.s3Key - Immutable staging key.
 * @returns {Promise<ArtifactIdentity>} Exact local identity.
 */
async function inspectArtifact(filePath, logical) {
  const digest = createHash("sha256");
  for await (const chunk of createReadStream(filePath)) digest.update(chunk);
  const digestBytes = digest.digest();
  const localStat = await stat(filePath);
  const cid = await ipfsHash.of(createReadStream(filePath));
  return {
    ...logical,
    sizeBytes: localStat.size,
    sha256: digestBytes.toString("hex"),
    checksumSha256: digestBytes.toString("base64"),
    cid,
  };
}

/**
 * Confirm a Parquet's physical schema and metadata row count.
 *
 * @param {string} filePath - Completed Parquet path.
 * @param {readonly FieldSchema[]} expectedFields - Exact ordered field names.
 * @param {number} expectedRows - Transactional expected row count.
 * @returns {Promise<void>} Resolves only when schema and row count match.
 */
async function verifyParquet(filePath, expectedFields, expectedRows) {
  const reader = await ParquetReader.openFile(filePath);
  try {
    const fields = Object.keys(reader.schema.fields);
    const expected = expectedFields.map((field) => field.name);
    if (JSON.stringify(fields) !== JSON.stringify(expected)) {
      throw new Error(`Parquet schema mismatch for ${path.basename(filePath)}`);
    }
    if (reader.metadata === null) {
      throw new Error(
        `Parquet metadata missing for ${path.basename(filePath)}`,
      );
    }
    const metadataRows = Number(reader.metadata.num_rows);
    if (metadataRows !== expectedRows) {
      throw new Error(
        `Parquet row mismatch for ${path.basename(filePath)}: ${String(metadataRows)} != ${String(expectedRows)}`,
      );
    }
  } finally {
    await reader.close();
  }
}

/**
 * Download one object to a temporary file while computing exact SHA-256 bytes.
 *
 * @param {unknown} body - AWS SDK GetObject body.
 * @param {string} temporaryPath - Private local readback path.
 * @returns {Promise<{sizeBytes:number,sha256:string,cid:string}>} Readback identity.
 */
async function saveReadback(body, temporaryPath) {
  if (
    typeof body !== "object" ||
    body === null ||
    !(Symbol.asyncIterator in body)
  ) {
    throw new Error("S3 GetObject returned no async iterable body");
  }
  const handle = await open(temporaryPath, "wx", 0o600);
  const digest = createHash("sha256");
  let sizeBytes = 0;
  try {
    for await (const raw of /** @type {AsyncIterable<unknown>} */ (body)) {
      const chunk =
        typeof raw === "string"
          ? Buffer.from(raw)
          : Buffer.from(/** @type {Uint8Array} */ (raw));
      await handle.write(chunk);
      digest.update(chunk);
      sizeBytes += chunk.byteLength;
    }
  } finally {
    await handle.close();
  }
  const sha256 = digest.digest("hex");
  const cid = await ipfsHash.of(createReadStream(temporaryPath));
  return { sizeBytes, sha256, cid };
}

/**
 * Stage all objects with conditional creates and verify S3 plus IPFS identity.
 *
 * @param {readonly ArtifactIdentity[]} artifacts - Local identities and exact keys.
 * @param {string} prefix - New Broward snapshot prefix.
 * @param {string} outputDirectory - Local private output directory for readback temp files.
 * @returns {Promise<StagedArtifact[]>} Per-object verification evidence.
 */
async function stageArtifacts(artifacts, prefix, outputDirectory) {
  const configuredRegion =
    process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION;
  if (configuredRegion !== AWS_REGION) {
    throw new Error(`AWS region must be ${AWS_REGION}`);
  }
  const client = new S3Client({ region: AWS_REGION });
  try {
    const existing = await client.send(
      new ListObjectsV2Command({
        Bucket: S3_BUCKET,
        Prefix: `${prefix}/`,
        MaxKeys: 1,
      }),
    );
    if ((existing.KeyCount ?? 0) !== 0) {
      throw new Error(
        `Refusing to overwrite existing snapshot prefix ${prefix}`,
      );
    }
    for (const artifact of artifacts) {
      await client.send(
        new PutObjectCommand({
          Bucket: S3_BUCKET,
          Key: artifact.s3Key,
          Body: createReadStream(path.join(outputDirectory, artifact.fileName)),
          ContentLength: artifact.sizeBytes,
          ContentType: artifact.contentType,
          ChecksumSHA256: artifact.checksumSha256,
          ServerSideEncryption: "AES256",
          IfNoneMatch: "*",
          Metadata: {
            sha256: artifact.sha256,
            cid: artifact.cid,
          },
        }),
      );
    }
    /** @type {StagedArtifact[]} */
    const staged = [];
    for (const artifact of artifacts) {
      const head = await client.send(
        new HeadObjectCommand({
          Bucket: S3_BUCKET,
          Key: artifact.s3Key,
          ChecksumMode: "ENABLED",
        }),
      );
      const response = await client.send(
        new GetObjectCommand({
          Bucket: S3_BUCKET,
          Key: artifact.s3Key,
          ChecksumMode: "ENABLED",
        }),
      );
      const readbackPath = path.join(
        outputDirectory,
        `.${artifact.fileName}.s3-readback`,
      );
      const readback = await saveReadback(response.Body, readbackPath);
      await rm(readbackPath, { force: true });
      const acl = await client.send(
        new GetObjectAclCommand({
          Bucket: S3_BUCKET,
          Key: artifact.s3Key,
        }),
      );
      const privateAcl = (acl.Grants ?? []).every((grant) => {
        const uri = grant.Grantee?.URI ?? "";
        return (
          uri !== "http://acs.amazonaws.com/groups/global/AllUsers" &&
          uri !== "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
        );
      });
      const verification = {
        artifact,
        headSizeMatches: head.ContentLength === artifact.sizeBytes,
        headChecksumMatches:
          head.ChecksumSHA256 === artifact.checksumSha256 &&
          response.ChecksumSHA256 === artifact.checksumSha256,
        readbackSizeMatches: readback.sizeBytes === artifact.sizeBytes,
        readbackSha256Matches: readback.sha256 === artifact.sha256,
        readbackCidMatches: readback.cid === artifact.cid,
        privateAcl,
        encryptedAtRest: head.ServerSideEncryption !== undefined,
      };
      if (
        !verification.headSizeMatches ||
        !verification.headChecksumMatches ||
        !verification.readbackSizeMatches ||
        !verification.readbackSha256Matches ||
        !verification.readbackCidMatches ||
        !verification.privateAcl ||
        !verification.encryptedAtRest
      ) {
        throw new Error(`S3 verification failed for ${artifact.s3Key}`);
      }
      staged.push(verification);
    }
    return staged;
  } finally {
    client.destroy();
  }
}

/**
 * Build, freeze, and optionally upload one Broward snapshot.
 *
 * @param {SnapshotOptions} options - Local output and S3 upload behavior.
 * @returns {Promise<Record<string,unknown>>} Safe handoff result.
 */
export async function stageBrowardDonphanSnapshot(options) {
  const target = requireNeonTarget(process.env);
  const routes = buildBrowardPermitRouteStatus();
  const client = new Client({
    connectionString: target.connectionString,
    application_name: "broward-donphan-snapshot",
    connectionTimeoutMillis: 10_000,
    statement_timeout: 0,
  });
  await client.connect();
  let transactionOpen = false;
  /** @type {string | null} */
  let temporaryDirectory = null;
  try {
    await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    transactionOpen = true;
    await verifyNeonIdentity(client, target);
    const aggregate = await client.query(SNAPSHOT_COUNTS_SQL, [
      APPRAISAL_SOURCE,
      PERMIT_SOURCE_PATTERN,
    ]);
    const normalized = normalizeSnapshotCounts(
      /** @type {Record<string,unknown>} */ (aggregate.rows[0] ?? {}),
    );
    const version = snapshotVersion(normalized.snapshotTimestamp);
    const prefix = browardSnapshotPrefix(version);
    const outputDirectory = path.join(options.outputRoot, version);
    temporaryDirectory = `${outputDirectory}.${String(process.pid)}.tmp`;
    await mkdir(options.outputRoot, { recursive: true, mode: 0o700 });
    await mkdir(temporaryDirectory, { mode: 0o700 });

    const sourceResult = await client.query(
      `SELECT source_system,count(*) AS row_count,
              count(*) FILTER (WHERE property_id IS NOT NULL) AS linked_count,
              count(*) FILTER (WHERE property_id IS NULL) AS unlinked_count,
              count(*) FILTER (
                WHERE coalesce(
                  more_details->>'is_roof_permit',
                  more_details->>'isRoofPermit'
                )='true'
              ) AS roofing_count
       FROM public.property_improvements
       WHERE source_system LIKE $1
       GROUP BY source_system
       ORDER BY source_system`,
      [PERMIT_SOURCE_PATTERN],
    );
    const permitSources = sourceResult.rows.map((row) => ({
      sourceSystem: String(row.source_system),
      rowCount: count(row.row_count, "source.row_count"),
      linkedCount: count(row.linked_count, "source.linked_count"),
      unlinkedCount: count(row.unlinked_count, "source.unlinked_count"),
      roofingCount: count(row.roofing_count, "source.roofing_count"),
    }));
    const propertyPath = path.join(temporaryDirectory, "query-table.parquet");
    const permitPath = path.join(
      temporaryDirectory,
      "permit-query-table.parquet",
    );
    const propertyExport = await exportCursorToParquet(client, {
      cursorName: "broward_property_snapshot",
      sql: PROPERTY_SQL,
      values: [APPRAISAL_SOURCE, PERMIT_SOURCE_PATTERN],
      outputPath: propertyPath,
      fields: PROPERTY_FIELDS,
      mapRow: propertyRecord,
      batchSize: options.batchSize,
    });
    const permitExport = await exportCursorToParquet(client, {
      cursorName: "broward_permit_snapshot",
      sql: PERMIT_SQL,
      values: [PERMIT_SOURCE_PATTERN],
      outputPath: permitPath,
      fields: PERMIT_FIELDS,
      mapRow: permitRecord,
      batchSize: options.batchSize,
    });
    if (
      propertyExport.rowCount !== normalized.counts.propertyRows ||
      permitExport.rowCount !== normalized.counts.permitRows
    ) {
      throw new Error(
        "Physical export row counts differ from frozen aggregates",
      );
    }
    const coverage = buildCoverageSnapshot({
      snapshotTimestamp: normalized.snapshotTimestamp,
      counts: normalized.counts,
      permitSources,
      routes,
    });
    if (
      !(
        /** @type {{reconciliation?:{allBalanced?:unknown}}} */ (coverage)
          .reconciliation?.allBalanced
      )
    ) {
      throw new Error("Frozen Broward coverage did not reconcile");
    }
    const coveragePath = path.join(temporaryDirectory, "dataset-coverage.json");
    await writeFile(coveragePath, `${JSON.stringify(coverage, null, 2)}\n`, {
      mode: 0o600,
    });
    await Promise.all([
      verifyParquet(
        propertyPath,
        PROPERTY_FIELDS,
        normalized.counts.propertyRows,
      ),
      verifyParquet(permitPath, PERMIT_FIELDS, normalized.counts.permitRows),
    ]);
    await client.query("ROLLBACK");
    transactionOpen = false;

    const artifactSpecs = [
      {
        name: "property-query-table",
        fileName: "query-table.parquet",
        contentType: "application/vnd.apache.parquet",
      },
      {
        name: "permit-query-table",
        fileName: "permit-query-table.parquet",
        contentType: "application/vnd.apache.parquet",
      },
      {
        name: "dataset-coverage",
        fileName: "dataset-coverage.json",
        contentType: "application/json",
      },
    ];
    /** @type {ArtifactIdentity[]} */
    const dataArtifacts = [];
    for (const spec of artifactSpecs) {
      dataArtifacts.push(
        await inspectArtifact(path.join(temporaryDirectory, spec.fileName), {
          ...spec,
          s3Key: `${prefix}/${spec.fileName}`,
        }),
      );
    }
    const manifest = {
      schemaVersion: SNAPSHOT_SCHEMA_VERSION,
      county: COUNTY,
      stateCode: "FL",
      countyFips: "12011",
      snapshotVersion: version,
      snapshotTimestamp: normalized.snapshotTimestamp,
      coverageStatus: "supported_partial",
      countyComplete: false,
      databaseSnapshot: {
        isolationLevel: "repeatable read",
        readOnly: true,
        identityVerified: true,
      },
      counts: normalized.counts,
      permitSources,
      routeCoverage: routes,
      artifactSchemas: {
        property: PROPERTY_FIELDS,
        permit: PERMIT_FIELDS,
        coverage: SNAPSHOT_SCHEMA_VERSION,
      },
      physicalExports: {
        property: propertyExport,
        permit: permitExport,
      },
      s3: {
        region: AWS_REGION,
        bucket: S3_BUCKET,
        prefix,
        private: true,
        immutableConditionalCreates: true,
        manifestKey: `${prefix}/manifest.json`,
      },
      filebaseHandoff: {
        requiredLocally: true,
        performedByThisScript: false,
        labels: {
          property: "oracle-query-table-broward",
          permits: "oracle-permit-table-broward",
          coverage: "oracle-dataset-coverage-broward",
        },
      },
      artifacts: dataArtifacts,
    };
    const manifestPath = path.join(temporaryDirectory, "manifest.json");
    await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, {
      mode: 0o600,
    });
    const manifestIdentity = await inspectArtifact(manifestPath, {
      name: "manifest",
      fileName: "manifest.json",
      contentType: "application/json",
      s3Key: `${prefix}/manifest.json`,
    });
    const artifacts = [...dataArtifacts, manifestIdentity];
    await rename(temporaryDirectory, outputDirectory);
    temporaryDirectory = null;

    const staged = options.upload
      ? await stageArtifacts(artifacts, prefix, outputDirectory)
      : [];
    return {
      event: "broward_donphan_snapshot_staged",
      snapshotVersion: version,
      snapshotTimestamp: normalized.snapshotTimestamp,
      outputDirectory,
      s3: {
        uploaded: options.upload,
        region: AWS_REGION,
        bucket: S3_BUCKET,
        prefix,
      },
      counts: normalized.counts,
      coverageStatus: "supported_partial",
      countyComplete: false,
      artifacts,
      verification: options.upload
        ? {
            allObjectsVerified:
              staged.length === artifacts.length &&
              staged.every(
                (entry) =>
                  entry.headSizeMatches &&
                  entry.headChecksumMatches &&
                  entry.readbackSizeMatches &&
                  entry.readbackSha256Matches &&
                  entry.readbackCidMatches &&
                  entry.privateAcl &&
                  entry.encryptedAtRest,
              ),
            objects: staged,
          }
        : { allObjectsVerified: false, objects: [] },
    };
  } catch (caught) {
    if (transactionOpen) {
      await client.query("ROLLBACK").catch(() => undefined);
    }
    if (temporaryDirectory !== null) {
      await rm(temporaryDirectory, { recursive: true, force: true });
    }
    throw caught;
  } finally {
    await client.end();
  }
}

/**
 * Execute the one-shot snapshot CLI and emit only aggregate-safe evidence.
 *
 * @returns {Promise<void>} Resolves after local generation and optional S3 verification.
 */
async function main() {
  const result = await stageBrowardDonphanSnapshot(
    parseSnapshotOptions(process.argv.slice(2)),
  );
  process.stdout.write(`${JSON.stringify(result)}\n`);
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((caught) => {
    process.stderr.write(
      `${JSON.stringify({
        event: "broward_donphan_snapshot_failed",
        error: caught instanceof Error ? caught.message : String(caught),
      })}\n`,
    );
    process.exitCode = 1;
  });
}
