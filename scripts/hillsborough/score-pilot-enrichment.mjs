#!/usr/bin/env node
/**
 * Score Sunbiz / BBB enrichment against the Hillsborough 50-parcel pilot using
 * Neon (DATABASE_URL). No S3/AWS — Postgres + local seed/scorecard files only.
 */

import { readFileSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import { Pool } from "pg";

/**
 * @param {string} envFile
 * @returns {string}
 */
export function readDatabaseUrl(envFile) {
  const text = readFileSync(envFile, "utf8");
  for (const line of text.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const match = /^DATABASE_URL=(.*)$/.exec(trimmed);
    if (match?.[1]) {
      return match[1].replace(/^['"]|['"]$/g, "");
    }
  }
  throw new Error(`DATABASE_URL not found in ${envFile}`);
}

/**
 * @param {object} params
 * @param {string} params.envFile
 * @param {string} params.outputPath
 * @param {string[]} params.zipPrefixes
 */
export async function scorePilotEnrichment(params) {
  const databaseUrl = readDatabaseUrl(params.envFile);
  const pool = new Pool({
    connectionString: databaseUrl,
    connectionTimeoutMillis: 20000,
    query_timeout: 120000,
  });

  try {
    const permitStats = await pool.query(`
      SELECT
        COUNT(*)::int AS permit_rows,
        COUNT(DISTINCT parcel_identifier)::int AS parcels_with_permits,
        COUNT(*) FILTER (WHERE source_url IS NOT NULL)::int AS with_url,
        COUNT(*) FILTER (
          WHERE project_description ~* 'roof|reroof|shingle'
             OR permit_number ~* 'ROF|REROOF'
        )::int AS roofing_related,
        COUNT(*) FILTER (WHERE contractor_company_id IS NOT NULL)::int AS with_contractor_company
      FROM property_improvements
      WHERE source_system = 'hillsborough_permits'
    `);

    const pilot = await pool.query(`
      SELECT p.request_identifier AS pin,
        a.normalized_address_key AS k,
        lower(regexp_replace(split_part(a.normalized_address_key, ' tampa', 1), '\\s+', ' ', 'g')) AS street_base
      FROM properties p
      JOIN addresses a ON a.address_id = p.address_id
      WHERE p.source_system = 'hillsborough_appraiser'
    `);

    const exactKeys = pilot.rows
      .map((r) => /** @type {{ k: string | null }} */ (r).k)
      .filter(/** @type {(v: string | null) => v is string} */ ((v) => !!v));

    const exact = await pool.query(
      `
      SELECT COUNT(DISTINCT a.normalized_address_key)::int AS c
      FROM addresses a
      JOIN business_registration_addresses bra
        ON bra.address_id = a.address_id AND bra.address_role = 'PRINCIPAL'
      WHERE a.normalized_address_key = ANY($1::text[])
      `,
      [exactKeys],
    );

    /** @type {Set<string>} */
    const streetBases = new Set();
    for (const row of pilot.rows) {
      const base = /** @type {{ street_base: string | null }} */ (row).street_base;
      if (base) streetBases.add(base);
    }

    let streetPrefixHits = 0;
    /** @type {string[]} */
    const hitPins = [];
    for (const base of streetBases) {
      const r = await pool.query(
        `
        SELECT 1
        FROM addresses a
        JOIN business_registration_addresses bra
          ON bra.address_id = a.address_id AND bra.address_role = 'PRINCIPAL'
        WHERE a.normalized_address_key LIKE $1
        LIMIT 1
        `,
        [`${base}%tampa%`],
      );
      if ((r.rowCount ?? 0) > 0) {
        streetPrefixHits += 1;
        const pin = pilot.rows.find(
          (p) => /** @type {{ street_base: string }} */ (p).street_base === base,
        );
        if (pin) hitPins.push(/** @type {{ pin: string }} */ (pin).pin);
      }
    }

    // Owner names from seed-backed people/companies on hillsborough properties
    const owners = await pool.query(`
      SELECT DISTINCT
        coalesce(c.name, trim(both from concat_ws(' ', pe.first_name, pe.last_name))) AS name,
        regexp_replace(
          lower(coalesce(c.name, trim(both from concat_ws(' ', pe.first_name, pe.last_name)))),
          '[^a-z0-9]', '', 'g'
        ) AS n
      FROM properties p
      JOIN ownerships o ON o.property_id = p.property_id
      LEFT JOIN companies c ON c.company_id = o.owner_company_id
      LEFT JOIN people pe ON pe.person_id = o.owner_person_id
      WHERE p.source_system = 'hillsborough_appraiser'
        AND coalesce(c.name, pe.last_name) IS NOT NULL
    `);
    const ownerKeys = owners.rows
      .map((r) => /** @type {{ n: string }} */ (r).n)
      .filter((n) => typeof n === "string" && n.length > 3);

    const ownerHits = await pool.query(
      `
      SELECT COUNT(DISTINCT regexp_replace(lower(c.name), '[^a-z0-9]', '', 'g'))::int AS distinct_names
      FROM companies c
      WHERE c.source_system = 'sunbiz'
        AND regexp_replace(lower(c.name), '[^a-z0-9]', '', 'g') = ANY($1::text[])
      `,
      [ownerKeys],
    );

    const bbbCount = await pool.query(`
      SELECT COUNT(*)::int AS c
      FROM business_reputation_profiles
      WHERE provider ILIKE '%bbb%'
    `);

    const bbbOwnerHits = await pool.query(
      `
      SELECT COUNT(DISTINCT p.business_reputation_profile_id)::int AS c
      FROM business_reputation_profiles p
      WHERE provider ILIKE '%bbb%'
        AND regexp_replace(
          lower(coalesce(p.normalized_name, p.name, p.legal_name)),
          '[^a-z0-9]', '', 'g'
        ) = ANY($1::text[])
      `,
      [ownerKeys],
    );

    const scorecard = {
      pilotParcelCount: pilot.rows.length,
      zipPrefixes: params.zipPrefixes,
      permits: permitStats.rows[0] ?? null,
      sunbiz: {
        note:
          "Statewide Sunbiz already present in Neon; exact address-key join is 0 because appraisal keys omit FL+ZIP while Sunbiz keys include them. Street-prefix (number+street+%tampa%) is the usable pilot join.",
        exactNormalizedKeyMatches: exact.rows[0]?.c ?? 0,
        streetPrefixMatches: streetPrefixHits,
        streetBasesTried: streetBases.size,
        sampleHitPins: hitPins.slice(0, 15),
        ownerNameMatches: ownerHits.rows[0]?.distinct_names ?? 0,
        ownerKeysTried: ownerKeys.length,
      },
      bbb: {
        profilesInNeon: bbbCount.rows[0]?.c ?? 0,
        pilotOwnerNameMatches: bbbOwnerHits.rows[0]?.c ?? 0,
        embeddedPermitsWithContractorCompanyId:
          permitStats.rows[0]?.with_contractor_company ?? 0,
        note: "Embedded permitInfo has no contractor names; BBB↔permit join for has_bbb_contractor needs Accela harvest.",
      },
    };

    await mkdir(path.dirname(params.outputPath), { recursive: true });
    await writeFile(
      params.outputPath,
      `${JSON.stringify(scorecard, null, 2)}\n`,
    );
    return scorecard;
  } finally {
    await pool.end();
  }
}

async function main() {
  const { values } = parseArgs({
    options: {
      "env-file": { type: "string" },
      output: { type: "string" },
      "zip-prefixes-file": { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });

  let zipPrefixes = ["335", "336"];
  if (typeof values["zip-prefixes-file"] === "string") {
    const doc = JSON.parse(readFileSync(values["zip-prefixes-file"], "utf8"));
    if (Array.isArray(doc?.prefixes)) zipPrefixes = doc.prefixes.map(String);
  }

  const scorecard = await scorePilotEnrichment({
    envFile:
      typeof values["env-file"] === "string"
        ? values["env-file"]
        : "../elephant-query-db/.env.local",
    outputPath:
      typeof values.output === "string"
        ? values.output
        : "downloads/hillsborough/pilot-enrichment-scorecard.json",
    zipPrefixes,
  });
  console.log(
    JSON.stringify({ event: "pilot_enrichment_scorecard", ...scorecard }, null, 2),
  );
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
