#!/usr/bin/env node
// @ts-check

/**
 * Match already-loaded Sunbiz address roles to Broward properties.
 *
 * Exact normalized-address hashes are accepted only when they resolve to one
 * Broward property. ZIP is a candidate screen, never county proof. Every
 * changed address reference has an ingest-control receipt retaining its
 * original address ID, making the operation idempotent and auditable.
 */

import { createHash } from "node:crypto";
import path from "node:path";
import { pathToFileURL } from "node:url";

import pg from "pg";

import { loadBrowardSunbizConfiguration } from "./prepare-broward-sunbiz-local.mjs";

const { Client } = pg;
const EXPECTED_PROJECT_ID = "raspy-frost-51580436";
const PRODUCTION_ENDPOINT_PREFIX = "ep-mute-leaf";
const CONTROL_SCHEMA = "ingest_control";
const LOCK_NAMESPACE = 12_011;
const LOCK_KEY = 6;
const DEFAULT_SOURCE_CATALOG = "docs/broward-sources.yaml";

/**
 * @typedef {object} SunbizMatchOptions
 * @property {string} jobId - Stable immutable match run.
 * @property {string} sourceCatalogPath - Broward ZIP-candidate catalog.
 * @property {number} chunkSize - Durable updates per transaction.
 * @property {number | null} limit - Optional deterministic pilot limit.
 * @property {boolean} apply - Whether to update address references.
 *
 * @typedef {object} SunbizPropertyMatchCandidate
 * @property {string} businessRegistrationAddressId - Source address-role UUID.
 * @property {string} businessRegistrationId - Sunbiz registration UUID.
 * @property {string} originalAddressId - Current source address UUID.
 * @property {string} matchedAddressId - Canonical Broward property address UUID.
 * @property {string} propertyId - Unique Broward property UUID.
 * @property {string} normalizedAddressHash - Exact normalized hash.
 * @property {string} addressRole - Sunbiz address role.
 *
 * @typedef {object} SunbizMatchSummary
 * @property {string} jobId - Stable run identity.
 * @property {boolean} applied - Whether updates were committed.
 * @property {number} candidateCount - Unambiguous exact-hash address roles.
 * @property {number} registrationCount - Distinct matched registrations.
 * @property {number} propertyCount - Distinct matched properties.
 * @property {number} committedChunkCount - Durable committed chunks.
 * @property {number} committedMatchCount - Durable committed address roles.
 * @property {string} candidateSha256 - Stable candidate-set identity.
 */

/**
 * Parse an explicit Broward Sunbiz match run.
 *
 * @param {readonly string[]} argv - CLI arguments.
 * @returns {SunbizMatchOptions} Validated options.
 */
export function parseSunbizMatchOptions(argv) {
  const values = new Map();
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (
      typeof flag !== "string" ||
      !flag.startsWith("--") ||
      typeof value !== "string" ||
      value.startsWith("--")
    ) {
      throw new Error("Sunbiz match options must be --flag value pairs");
    }
    values.set(flag.slice(2), value);
  }
  const jobId = values.get("job-id");
  if (
    typeof jobId !== "string" ||
    !/^broward-sunbiz-[a-z0-9-]+$/u.test(jobId)
  ) {
    throw new Error("--job-id must begin broward-sunbiz-");
  }
  const rawApply = values.get("apply") ?? "false";
  if (rawApply !== "true" && rawApply !== "false") {
    throw new Error("--apply must be true or false");
  }
  const rawLimit = values.get("limit");
  return {
    jobId,
    sourceCatalogPath: values.get("source-catalog") ?? DEFAULT_SOURCE_CATALOG,
    chunkSize: boundedInteger(
      values.get("chunk-size") ?? "1000",
      "chunk-size",
      1,
      5_000,
    ),
    limit:
      rawLimit === undefined
        ? null
        : boundedInteger(rawLimit, "limit", 1, 1_000_000),
    apply: rawApply === "true",
  };
}

/**
 * Produce a stable candidate identity without hashing mutable original IDs.
 *
 * @param {readonly SunbizPropertyMatchCandidate[]} candidates - Ordered candidates.
 * @returns {string} Lowercase SHA-256.
 */
export function hashSunbizMatchCandidates(candidates) {
  const hash = createHash("sha256");
  for (const candidate of candidates) {
    hash.update(
      [
        candidate.businessRegistrationAddressId,
        candidate.businessRegistrationId,
        candidate.matchedAddressId,
        candidate.propertyId,
        candidate.normalizedAddressHash,
        candidate.addressRole,
      ].join("\0"),
    );
    hash.update("\n");
  }
  return hash.digest("hex");
}

/**
 * Run a dry-run or durable exact-hash match.
 *
 * @param {SunbizMatchOptions} options - Validated run configuration.
 * @returns {Promise<SunbizMatchSummary>} Aggregate-safe reconciliation.
 */
export async function matchBrowardSunbizToProperties(options) {
  const configuration = await loadBrowardSunbizConfiguration(
    path.resolve(options.sourceCatalogPath),
  );
  const target = requireTarget(process.env);
  const client = new Client({
    connectionString: target.connectionString,
    application_name: "broward-sunbiz-property-matcher",
    connectionTimeoutMillis: 10_000,
    statement_timeout: 300_000,
  });
  await client.connect();
  try {
    await verifyTarget(client, target);
    await createPropertyHashLookup(client);
    const candidates = await readMatchCandidates(
      client,
      configuration.zipCandidates,
      options.limit,
    );
    if (candidates.length === 0) {
      throw new Error("Broward Sunbiz matcher found no exact candidates");
    }
    const candidateSha256 = hashSunbizMatchCandidates(candidates);
    const registrationCount = new Set(
      candidates.map((candidate) => candidate.businessRegistrationId),
    ).size;
    const propertyCount = new Set(
      candidates.map((candidate) => candidate.propertyId),
    ).size;
    if (!options.apply) {
      return {
        jobId: options.jobId,
        applied: false,
        candidateCount: candidates.length,
        registrationCount,
        propertyCount,
        committedChunkCount: 0,
        committedMatchCount: 0,
        candidateSha256,
      };
    }

    await acquireLock(client);
    await ensureControlTables(client);
    await registerRun(client, options, candidates.length, candidateSha256);
    const completed = await readCompletedChunks(client, options.jobId);
    for (
      let offset = 0, chunkIndex = 0;
      offset < candidates.length;
      offset += options.chunkSize, chunkIndex += 1
    ) {
      if (completed.has(chunkIndex)) continue;
      await applyMatchChunk(
        client,
        options.jobId,
        chunkIndex,
        candidates.slice(offset, offset + options.chunkSize),
      );
    }
    const aggregate = await client.query(
      `SELECT count(*)::integer AS chunks,
              coalesce(sum(match_count),0)::integer AS matches
       FROM ${CONTROL_SCHEMA}.broward_sunbiz_match_chunks
       WHERE job_id=$1`,
      [options.jobId],
    );
    const row = aggregate.rows[0];
    if (Number(row?.matches) !== candidates.length) {
      throw new Error("Broward Sunbiz match chunks do not reconcile");
    }
    await client.query(
      `UPDATE ${CONTROL_SCHEMA}.broward_sunbiz_match_runs
       SET status='complete',completed_at=now(),heartbeat_at=now()
       WHERE job_id=$1`,
      [options.jobId],
    );
    return {
      jobId: options.jobId,
      applied: true,
      candidateCount: candidates.length,
      registrationCount,
      propertyCount,
      committedChunkCount: Number(row.chunks),
      committedMatchCount: Number(row.matches),
      candidateSha256,
    };
  } finally {
    await client.end();
  }
}

/**
 * Build a temporary exact-hash lookup containing only unique properties.
 *
 * Multiple address rows for the same property are safe; a hash shared by
 * multiple properties is deliberately absent from the lookup.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @returns {Promise<void>} Resolves after lookup/index creation.
 */
async function createPropertyHashLookup(client) {
  await client.query(
    `CREATE TEMP TABLE broward_unique_property_addresses
     ON COMMIT PRESERVE ROWS AS
     WITH property_addresses AS (
       SELECT a.normalized_address_hash,p.property_id,a.address_id
       FROM public.properties p
       JOIN public.addresses a ON a.address_id=p.address_id
       WHERE p.source_system='broward_appraiser'
         AND a.normalized_address_hash IS NOT NULL
     ),
     unique_hashes AS (
       SELECT normalized_address_hash,
              min(property_id::text)::uuid AS property_id,
              min(address_id::text)::uuid AS address_id,
              count(DISTINCT property_id) AS property_count
       FROM property_addresses
       GROUP BY normalized_address_hash
     )
     SELECT normalized_address_hash,property_id,address_id
     FROM unique_hashes WHERE property_count=1`,
  );
  await client.query(
    `CREATE UNIQUE INDEX ON broward_unique_property_addresses (
       normalized_address_hash
     )`,
  );
  await client.query("ANALYZE broward_unique_property_addresses");
}

/**
 * Read deterministic unambiguous exact-hash candidates.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @param {readonly string[]} zipCandidates - Exact Broward ZIP candidates.
 * @param {number | null} limit - Optional pilot limit.
 * @returns {Promise<SunbizPropertyMatchCandidate[]>} Ordered candidates.
 */
async function readMatchCandidates(client, zipCandidates, limit) {
  const result = await client.query(
    `SELECT
       bra.business_registration_address_id,
       bra.business_registration_id,
       bra.address_id AS original_address_id,
       match.address_id AS matched_address_id,
       match.property_id,
       match.normalized_address_hash,
       bra.address_role
     FROM public.business_registration_addresses bra
     JOIN public.addresses source_address
       ON source_address.address_id=bra.address_id
     JOIN broward_unique_property_addresses match
       ON match.normalized_address_hash=source_address.normalized_address_hash
     WHERE bra.source_system='sunbiz'
       AND bra.zip=ANY($1::text[])
     ORDER BY bra.business_registration_address_id
     LIMIT $2`,
    [zipCandidates, limit],
  );
  return result.rows.map((row) => {
    for (const field of [
      "business_registration_address_id",
      "business_registration_id",
      "original_address_id",
      "matched_address_id",
      "property_id",
      "normalized_address_hash",
      "address_role",
    ]) {
      if (typeof row[field] !== "string" || row[field].length === 0) {
        throw new Error(`Broward Sunbiz candidate has invalid ${field}`);
      }
    }
    return {
      businessRegistrationAddressId: row.business_registration_address_id,
      businessRegistrationId: row.business_registration_id,
      originalAddressId: row.original_address_id,
      matchedAddressId: row.matched_address_id,
      propertyId: row.property_id,
      normalizedAddressHash: row.normalized_address_hash,
      addressRole: row.address_role,
    };
  });
}

/**
 * Commit one match chunk and original-ID receipts atomically.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @param {string} jobId - Stable run ID.
 * @param {number} chunkIndex - Zero-based chunk.
 * @param {readonly SunbizPropertyMatchCandidate[]} candidates - Exact candidates.
 * @returns {Promise<void>} Resolves after verification and commit.
 */
async function applyMatchChunk(client, jobId, chunkIndex, candidates) {
  const json = JSON.stringify(candidates);
  await client.query("BEGIN");
  try {
    const conflicts = await client.query(
      `SELECT count(*)::integer AS conflicts
       FROM ${CONTROL_SCHEMA}.broward_sunbiz_property_matches existing
       JOIN jsonb_to_recordset($1::jsonb) AS input(
         "businessRegistrationAddressId" uuid,
         "matchedAddressId" uuid,
         "propertyId" uuid
       ) ON input."businessRegistrationAddressId"=
            existing.business_registration_address_id
       WHERE existing.matched_address_id<>input."matchedAddressId"
          OR existing.property_id<>input."propertyId"`,
      [json],
    );
    if (Number(conflicts.rows[0]?.conflicts) !== 0) {
      throw new Error("Existing Broward Sunbiz match target conflicts");
    }
    await client.query(
      `INSERT INTO ${CONTROL_SCHEMA}.broward_sunbiz_property_matches (
         business_registration_address_id,business_registration_id,
         original_address_id,matched_address_id,property_id,
         normalized_address_hash,address_role
       )
       SELECT
         input."businessRegistrationAddressId",
         input."businessRegistrationId",
         input."originalAddressId",
         input."matchedAddressId",
         input."propertyId",
         input."normalizedAddressHash",
         input."addressRole"
       FROM jsonb_to_recordset($1::jsonb) AS input(
         "businessRegistrationAddressId" uuid,
         "businessRegistrationId" uuid,
         "originalAddressId" uuid,
         "matchedAddressId" uuid,
         "propertyId" uuid,
         "normalizedAddressHash" text,
         "addressRole" text
       )
       ON CONFLICT (business_registration_address_id) DO NOTHING`,
      [json],
    );
    await client.query(
      `UPDATE public.business_registration_addresses bra
       SET address_id=input."matchedAddressId",
           address_match_method='normalized_address_hash',
           address_match_confidence='exact',
           updated_at=now()
       FROM jsonb_to_recordset($1::jsonb) AS input(
         "businessRegistrationAddressId" uuid,
         "matchedAddressId" uuid
       )
       WHERE bra.business_registration_address_id=
             input."businessRegistrationAddressId"`,
      [json],
    );
    const verification = await client.query(
      `SELECT count(*)::integer AS matched
       FROM public.business_registration_addresses bra
       JOIN jsonb_to_recordset($1::jsonb) AS input(
         "businessRegistrationAddressId" uuid,
         "matchedAddressId" uuid
       ) ON bra.business_registration_address_id=
            input."businessRegistrationAddressId"
       WHERE bra.address_id=input."matchedAddressId"
         AND bra.address_match_method='normalized_address_hash'
         AND bra.address_match_confidence='exact'`,
      [json],
    );
    if (Number(verification.rows[0]?.matched) !== candidates.length) {
      throw new Error("Broward Sunbiz match chunk did not verify");
    }
    await client.query(
      `INSERT INTO ${CONTROL_SCHEMA}.broward_sunbiz_match_chunks (
         job_id,chunk_index,match_count,chunk_sha256
       ) VALUES ($1,$2,$3,$4)`,
      [
        jobId,
        chunkIndex,
        candidates.length,
        hashSunbizMatchCandidates(candidates),
      ],
    );
    await client.query(
      `UPDATE ${CONTROL_SCHEMA}.broward_sunbiz_match_runs
       SET heartbeat_at=now() WHERE job_id=$1`,
      [jobId],
    );
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

/**
 * Create additive durable matcher tables.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @returns {Promise<void>} Resolves after idempotent DDL.
 */
async function ensureControlTables(client) {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${CONTROL_SCHEMA}`);
  await client.query(
    `CREATE TABLE IF NOT EXISTS ${CONTROL_SCHEMA}.broward_sunbiz_match_runs (
       job_id text PRIMARY KEY,
       candidate_count integer NOT NULL CHECK (candidate_count > 0),
       candidate_sha256 text NOT NULL CHECK (candidate_sha256 ~ '^[a-f0-9]{64}$'),
       chunk_size integer NOT NULL CHECK (chunk_size > 0),
       status text NOT NULL CHECK (status IN ('running','complete')),
       started_at timestamptz NOT NULL DEFAULT now(),
       heartbeat_at timestamptz NOT NULL DEFAULT now(),
       completed_at timestamptz
     )`,
  );
  await client.query(
    `CREATE TABLE IF NOT EXISTS ${CONTROL_SCHEMA}.broward_sunbiz_property_matches (
       business_registration_address_id uuid PRIMARY KEY,
       business_registration_id uuid NOT NULL,
       original_address_id uuid NOT NULL,
       matched_address_id uuid NOT NULL,
       property_id uuid NOT NULL,
       normalized_address_hash text NOT NULL,
       address_role text NOT NULL,
       matched_at timestamptz NOT NULL DEFAULT now()
     )`,
  );
  await client.query(
    `CREATE TABLE IF NOT EXISTS ${CONTROL_SCHEMA}.broward_sunbiz_match_chunks (
       job_id text NOT NULL REFERENCES
         ${CONTROL_SCHEMA}.broward_sunbiz_match_runs(job_id),
       chunk_index integer NOT NULL CHECK (chunk_index >= 0),
       match_count integer NOT NULL CHECK (match_count > 0),
       chunk_sha256 text NOT NULL CHECK (chunk_sha256 ~ '^[a-f0-9]{64}$'),
       committed_at timestamptz NOT NULL DEFAULT now(),
       PRIMARY KEY (job_id,chunk_index)
     )`,
  );
}

/**
 * Register or verify immutable match candidates.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @param {SunbizMatchOptions} options - Run options.
 * @param {number} candidateCount - Exact candidate count.
 * @param {string} candidateSha256 - Exact candidate hash.
 * @returns {Promise<void>} Resolves only for matching state.
 */
async function registerRun(client, options, candidateCount, candidateSha256) {
  await client.query(
    `INSERT INTO ${CONTROL_SCHEMA}.broward_sunbiz_match_runs (
       job_id,candidate_count,candidate_sha256,chunk_size,status
     ) VALUES ($1,$2,$3,$4,'running')
     ON CONFLICT (job_id) DO NOTHING`,
    [options.jobId, candidateCount, candidateSha256, options.chunkSize],
  );
  const result = await client.query(
    `SELECT candidate_count,candidate_sha256,chunk_size
     FROM ${CONTROL_SCHEMA}.broward_sunbiz_match_runs WHERE job_id=$1`,
    [options.jobId],
  );
  const row = result.rows[0];
  if (
    Number(row?.candidate_count) !== candidateCount ||
    row.candidate_sha256 !== candidateSha256 ||
    Number(row.chunk_size) !== options.chunkSize
  ) {
    throw new Error("Existing Broward Sunbiz match run differs");
  }
}

/**
 * Read durable completed chunks.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @param {string} jobId - Stable job ID.
 * @returns {Promise<Set<number>>} Completed chunk indexes.
 */
async function readCompletedChunks(client, jobId) {
  const result = await client.query(
    `SELECT chunk_index FROM ${CONTROL_SCHEMA}.broward_sunbiz_match_chunks
     WHERE job_id=$1`,
    [jobId],
  );
  return new Set(result.rows.map((row) => Number(row.chunk_index)));
}

/**
 * Acquire the dedicated Sunbiz matcher lock.
 *
 * @param {import("pg").Client} client - Verified direct client.
 * @returns {Promise<void>} Resolves for the single matcher.
 */
async function acquireLock(client) {
  const result = await client.query(
    "SELECT pg_try_advisory_lock($1,$2) AS acquired",
    [LOCK_NAMESPACE, LOCK_KEY],
  );
  if (result.rows[0]?.acquired !== true) {
    throw new Error("Another Broward Sunbiz matcher owns the writer lock");
  }
}

/**
 * Read and validate isolated direct Neon configuration.
 *
 * @param {NodeJS.ProcessEnv} environment - Runtime environment.
 * @returns {{connectionString:string,expectedBranchId:string,expectedEndpointId:string}}
 *   Valid target.
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
    throw new Error("Broward Sunbiz matching requires direct Neon");
  }
  return { connectionString, expectedBranchId, expectedEndpointId };
}

/**
 * Prove exact isolated Neon identity.
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
    throw new Error("Sunbiz match target is not isolated broward-ingest");
  }
}

/**
 * Parse a bounded integer.
 *
 * @param {string} raw - Raw option.
 * @param {string} name - Option name.
 * @param {number} minimum - Inclusive minimum.
 * @param {number} maximum - Inclusive maximum.
 * @returns {number} Validated integer.
 */
function boundedInteger(raw, name, minimum, maximum) {
  const value = Number(raw);
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error(
      `--${name} must be an integer from ${String(minimum)} through ${String(maximum)}`,
    );
  }
  return value;
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  matchBrowardSunbizToProperties(parseSunbizMatchOptions(process.argv.slice(2)))
    .then((summary) => {
      console.log(
        JSON.stringify({
          event: "broward_sunbiz_property_match_finished",
          ...summary,
        }),
      );
    })
    .catch((error) => {
      console.error(
        JSON.stringify({
          event: "broward_sunbiz_property_match_failed",
          message: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      process.exitCode = 1;
    });
}
