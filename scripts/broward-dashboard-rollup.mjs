#!/usr/bin/env node
// @ts-check

/**
 * Maintain the small durable aggregate row consumed by Broward dashboards.
 *
 * Expensive property-improvement JSON scans run only after a loader commits or
 * through this explicit refresh command. Dashboard requests read one row and
 * never block on full-table aggregation.
 */

import { pathToFileURL } from "node:url";

import pg from "pg";

const { Client } = pg;
const EXPECTED_PROJECT_ID = "raspy-frost-51580436";
const PRODUCTION_ENDPOINT_PREFIX = "ep-mute-leaf";
const CONTROL_SCHEMA = "ingest_control";
const PIPELINE_KEY = "broward";

/**
 * @typedef {object} BrowardDashboardRollup
 * @property {number} permitRecords - Logical Broward permit rows.
 * @property {number} permitMatched - Permit rows linked to a property.
 * @property {number} permitUnmatched - Valid unlinked permit rows.
 * @property {number} permitRoofing - Explicit roofing rows.
 * @property {number} permitParcels - Distinct source parcel identifiers.
 * @property {number} permitSourceSystems - Distinct permit source systems.
 * @property {string | null} permitLastLoadedAt - Latest permit load timestamp.
 * @property {number} coralEtrakitRecords - Loaded Coral Springs slice rows.
 * @property {number} coralEtrakitMatched - Coral rows linked by exact folio.
 * @property {number} coralEtrakitRoofing - Source-query-backed roofing rows.
 * @property {number} sunbizMatchedRoles - Exact Sunbiz address-role links.
 * @property {number} sunbizRegistrations - Distinct linked registrations.
 * @property {number} sunbizProperties - Distinct linked properties.
 * @property {string} refreshedAt - Rollup refresh timestamp.
 */

/**
 * Create the additive rollup table.
 *
 * @param {import("pg").Client | import("pg").PoolClient} client - Verified client.
 * @returns {Promise<void>} Resolves after idempotent DDL.
 */
export async function ensureBrowardDashboardRollup(client) {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${CONTROL_SCHEMA}`);
  await client.query(
    `CREATE TABLE IF NOT EXISTS ${CONTROL_SCHEMA}.broward_dashboard_rollup (
       pipeline_key text PRIMARY KEY,
       permit_records integer NOT NULL CHECK (permit_records >= 0),
       permit_matched integer NOT NULL CHECK (permit_matched >= 0),
       permit_unmatched integer NOT NULL CHECK (permit_unmatched >= 0),
       permit_roofing integer NOT NULL CHECK (permit_roofing >= 0),
       permit_parcels integer NOT NULL CHECK (permit_parcels >= 0),
       permit_source_systems integer NOT NULL CHECK (
         permit_source_systems >= 0
       ),
       permit_last_loaded_at timestamptz,
       coral_etrakit_records integer NOT NULL DEFAULT 0 CHECK (
         coral_etrakit_records >= 0
       ),
       coral_etrakit_matched integer NOT NULL DEFAULT 0 CHECK (
         coral_etrakit_matched >= 0
       ),
       coral_etrakit_roofing integer NOT NULL DEFAULT 0 CHECK (
         coral_etrakit_roofing >= 0
       ),
       sunbiz_matched_roles integer NOT NULL CHECK (
         sunbiz_matched_roles >= 0
       ),
       sunbiz_registrations integer NOT NULL CHECK (
         sunbiz_registrations >= 0
       ),
       sunbiz_properties integer NOT NULL CHECK (sunbiz_properties >= 0),
       refreshed_at timestamptz NOT NULL DEFAULT now()
     )`,
  );
  await client.query(
    `ALTER TABLE ${CONTROL_SCHEMA}.broward_dashboard_rollup
       ADD COLUMN IF NOT EXISTS coral_etrakit_records integer NOT NULL
         DEFAULT 0 CHECK (coral_etrakit_records >= 0),
       ADD COLUMN IF NOT EXISTS coral_etrakit_matched integer NOT NULL
         DEFAULT 0 CHECK (coral_etrakit_matched >= 0),
       ADD COLUMN IF NOT EXISTS coral_etrakit_roofing integer NOT NULL
         DEFAULT 0 CHECK (coral_etrakit_roofing >= 0)`,
  );
}

/**
 * Refresh and return the verified Broward dashboard rollup.
 *
 * @param {import("pg").Client | import("pg").PoolClient} client - Verified client.
 * @returns {Promise<BrowardDashboardRollup>} Refreshed aggregate row.
 */
export async function refreshBrowardDashboardRollup(client) {
  await ensureBrowardDashboardRollup(client);
  const result = await client.query(
    `WITH permit_stats AS (
       SELECT
         count(*)::integer AS permit_records,
         count(*) FILTER (WHERE property_id IS NOT NULL)::integer
           AS permit_matched,
         count(*) FILTER (WHERE property_id IS NULL)::integer
           AS permit_unmatched,
         count(*) FILTER (
           WHERE coalesce(
             more_details->>'is_roof_permit',
             more_details->>'isRoofPermit'
           )='true'
         )::integer AS permit_roofing,
         count(DISTINCT parcel_identifier) FILTER (
           WHERE parcel_identifier IS NOT NULL
         )::integer AS permit_parcels,
         count(DISTINCT source_system)::integer AS permit_source_systems,
         max(loaded_at) AS permit_last_loaded_at,
         count(*) FILTER (
           WHERE source_system='broward_coral_springs_etrakit_permits'
         )::integer AS coral_etrakit_records,
         count(*) FILTER (
           WHERE source_system='broward_coral_springs_etrakit_permits'
             AND property_id IS NOT NULL
         )::integer AS coral_etrakit_matched,
         count(*) FILTER (
           WHERE source_system='broward_coral_springs_etrakit_permits'
             AND coalesce(
               more_details->>'is_roof_permit',
               more_details->>'isRoofPermit'
             )='true'
         )::integer AS coral_etrakit_roofing
       FROM public.property_improvements
       WHERE source_system LIKE 'broward%permits'
     ),
     sunbiz_stats AS (
       SELECT
         count(*)::integer AS sunbiz_matched_roles,
         count(DISTINCT business_registration_id)::integer
           AS sunbiz_registrations,
         count(DISTINCT property_id)::integer AS sunbiz_properties
       FROM ${CONTROL_SCHEMA}.broward_sunbiz_property_matches
     )
     INSERT INTO ${CONTROL_SCHEMA}.broward_dashboard_rollup (
       pipeline_key,permit_records,permit_matched,permit_unmatched,
       permit_roofing,permit_parcels,permit_source_systems,
       permit_last_loaded_at,coral_etrakit_records,coral_etrakit_matched,
       coral_etrakit_roofing,sunbiz_matched_roles,sunbiz_registrations,
       sunbiz_properties,refreshed_at
     )
     SELECT
       $1,permit_stats.permit_records,permit_stats.permit_matched,
       permit_stats.permit_unmatched,permit_stats.permit_roofing,
       permit_stats.permit_parcels,permit_stats.permit_source_systems,
       permit_stats.permit_last_loaded_at,permit_stats.coral_etrakit_records,
       permit_stats.coral_etrakit_matched,permit_stats.coral_etrakit_roofing,
       sunbiz_stats.sunbiz_matched_roles,sunbiz_stats.sunbiz_registrations,
       sunbiz_stats.sunbiz_properties,now()
     FROM permit_stats,sunbiz_stats
     ON CONFLICT (pipeline_key) DO UPDATE SET
       permit_records=EXCLUDED.permit_records,
       permit_matched=EXCLUDED.permit_matched,
       permit_unmatched=EXCLUDED.permit_unmatched,
       permit_roofing=EXCLUDED.permit_roofing,
       permit_parcels=EXCLUDED.permit_parcels,
       permit_source_systems=EXCLUDED.permit_source_systems,
       permit_last_loaded_at=EXCLUDED.permit_last_loaded_at,
       coral_etrakit_records=EXCLUDED.coral_etrakit_records,
       coral_etrakit_matched=EXCLUDED.coral_etrakit_matched,
       coral_etrakit_roofing=EXCLUDED.coral_etrakit_roofing,
       sunbiz_matched_roles=EXCLUDED.sunbiz_matched_roles,
       sunbiz_registrations=EXCLUDED.sunbiz_registrations,
       sunbiz_properties=EXCLUDED.sunbiz_properties,
       refreshed_at=now()
     RETURNING *`,
    [PIPELINE_KEY],
  );
  const row = result.rows[0];
  if (row === undefined) {
    throw new Error("Broward dashboard rollup refresh returned no row");
  }
  const permitRecords = readCount(row.permit_records);
  const permitMatched = readCount(row.permit_matched);
  const permitUnmatched = readCount(row.permit_unmatched);
  if (permitMatched + permitUnmatched !== permitRecords) {
    throw new Error("Broward permit rollup does not reconcile");
  }
  return {
    permitRecords,
    permitMatched,
    permitUnmatched,
    permitRoofing: readCount(row.permit_roofing),
    permitParcels: readCount(row.permit_parcels),
    permitSourceSystems: readCount(row.permit_source_systems),
    permitLastLoadedAt:
      row.permit_last_loaded_at instanceof Date
        ? row.permit_last_loaded_at.toISOString()
        : typeof row.permit_last_loaded_at === "string"
          ? row.permit_last_loaded_at
          : null,
    coralEtrakitRecords: readCount(row.coral_etrakit_records),
    coralEtrakitMatched: readCount(row.coral_etrakit_matched),
    coralEtrakitRoofing: readCount(row.coral_etrakit_roofing),
    sunbizMatchedRoles: readCount(row.sunbiz_matched_roles),
    sunbizRegistrations: readCount(row.sunbiz_registrations),
    sunbizProperties: readCount(row.sunbiz_properties),
    refreshedAt:
      row.refreshed_at instanceof Date
        ? row.refreshed_at.toISOString()
        : String(row.refreshed_at),
  };
}

/**
 * Convert a PostgreSQL count to a safe integer.
 *
 * @param {unknown} value - Driver-returned value.
 * @returns {number} Safe non-negative integer.
 */
function readCount(value) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error("Broward dashboard rollup contains an invalid count");
  }
  return parsed;
}

/**
 * Validate direct isolated Neon configuration.
 *
 * @param {NodeJS.ProcessEnv} environment - Runtime environment.
 * @returns {{connectionString:string,expectedBranchId:string,expectedEndpointId:string}}
 *   Validated target.
 */
function requireTarget(environment) {
  const connectionString = environment.DATABASE_URL_UNPOOLED;
  const expectedBranchId = environment.BROWARD_INGEST_NEON_BRANCH_ID;
  const expectedEndpointId = environment.BROWARD_INGEST_NEON_ENDPOINT_ID;
  if (
    typeof connectionString !== "string" ||
    typeof expectedBranchId !== "string" ||
    !/^br-[a-z0-9-]+$/u.test(expectedBranchId) ||
    typeof expectedEndpointId !== "string" ||
    !/^ep-[a-z0-9-]+$/u.test(expectedEndpointId) ||
    expectedEndpointId.startsWith(PRODUCTION_ENDPOINT_PREFIX)
  ) {
    throw new Error("Verified direct Broward Neon target is required");
  }
  if (new URL(connectionString).hostname.includes("-pooler")) {
    throw new Error("Dashboard rollup refresh requires direct Neon");
  }
  return { connectionString, expectedBranchId, expectedEndpointId };
}

/**
 * Prove exact Neon project, branch, and endpoint.
 *
 * @param {import("pg").Client} client - Connected direct client.
 * @param {{expectedBranchId:string,expectedEndpointId:string}} target - IDs.
 * @returns {Promise<void>} Resolves only for isolated Broward.
 */
async function verifyTarget(client, target) {
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
    throw new Error("Dashboard rollup target is not isolated broward-ingest");
  }
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const target = requireTarget(process.env);
  const client = new Client({
    connectionString: target.connectionString,
    application_name: "broward-dashboard-rollup-refresh",
    connectionTimeoutMillis: 10_000,
    statement_timeout: 300_000,
  });
  client
    .connect()
    .then(async () => {
      await verifyTarget(client, target);
      return refreshBrowardDashboardRollup(client);
    })
    .then((rollup) => {
      console.log(
        JSON.stringify({
          event: "broward_dashboard_rollup_refreshed",
          ...rollup,
        }),
      );
    })
    .catch((error) => {
      console.error(
        JSON.stringify({
          event: "broward_dashboard_rollup_failed",
          message: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      process.exitCode = 1;
    })
    .finally(() => client.end());
}
