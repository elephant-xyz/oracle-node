/**
 * Shared helpers for the Overture Maps places extract, export, and publish gate.
 *
 * Key on `taxonomy.hierarchy` (full `/`-delimited paths). `categories.primary` is
 * stored only as `legacy_category_primary` until the September 2026 release
 * removes it. Distinct `sources[].dataset` values must be a subset of the nine
 * approved providers; `osm` or any unknown dataset fails the licence gate.
 */

import { parseArgs } from "node:util";

/** Scoping research release used for the Lee 40,190 clip baseline. */
export const SCOPING_BASELINE_RELEASE = "2026-07-22.0";

/** Boundary-clipped Lee County place count for {@link SCOPING_BASELINE_RELEASE}. */
export const SCOPING_BASELINE_CLIP_COUNT = 40_190;

/** Overture STAC catalog used to discover the latest release id. */
export const OVERTURE_STAC_CATALOG_URL =
  "https://stac.overturemaps.org/catalog.json";

/**
 * Providers allowed through the places licence gate. The first nine are the
 * attribution-page list (2026-08-12). `Overture` and `Overture-signals` are
 * Overture's own lineage — allowed by human decision 2026-08-12, not OSM.
 * If `osm` or any name outside this list appears, do not publish.
 *
 * @type {readonly string[]}
 */
export const APPROVED_PLACE_DATASETS = Object.freeze([
  "meta",
  "microsoft",
  "foursquare",
  "pinmeto",
  "krick",
  "rendersEO",
  "dac",
  "brightquery",
  "alltheplaces",
  "Overture",
  "Overture-signals",
]);

/** Lowercased lookup so live Overture `Microsoft` matches approved `microsoft`. */
const APPROVED_PLACE_DATASET_BY_LOWER = new Map(
  APPROVED_PLACE_DATASETS.map((name) => [name.toLowerCase(), name]),
);

/**
 * Per-provider licence labels for NOTICE.txt. Foursquare is Apache-2.0; the
 * rest of the approved places providers are CDLA-Permissive-2.0.
 *
 * @type {Readonly<Record<string, string>>}
 */
export const PLACE_DATASET_LICENCES = Object.freeze({
  meta: "CDLA-Permissive-2.0",
  microsoft: "CDLA-Permissive-2.0",
  foursquare: "Apache-2.0",
  pinmeto: "CDLA-Permissive-2.0",
  krick: "CDLA-Permissive-2.0",
  rendersEO: "CDLA-Permissive-2.0",
  dac: "CDLA-Permissive-2.0",
  brightquery: "CDLA-Permissive-2.0",
  alltheplaces: "CDLA-Permissive-2.0",
  Overture: "CDLA-Permissive-2.0 and Apache-2.0 (Overture theme lineage)",
  "Overture-signals":
    "CDLA-Permissive-2.0 and Apache-2.0 (Overture theme lineage)",
});

/** Case-insensitive licence lookup for live TitleCase dataset names. */
const PLACE_DATASET_LICENCE_BY_LOWER = new Map(
  Object.entries(PLACE_DATASET_LICENCES).map(([name, licence]) => [
    name.toLowerCase(),
    licence,
  ]),
);

/**
 * Old flat `categories.primary` labels named in the hosted-service scoping
 * research. Matching is on full `taxonomy.hierarchy` paths, never these leaves.
 *
 * @type {readonly string[]}
 */
export const HOSTED_SERVICE_SEED_LEAVES = Object.freeze([
  "atms",
  "rental_kiosks",
  "propane_supplier",
  "money_transfer_services",
  "trusts",
]);

/**
 * Leaf tokens that look hosted-service-like when reviewing an extract. Used
 * only to surface review candidates; they do not set `is_hosted_service`.
 *
 * @type {readonly string[]}
 */
export const HOSTED_SERVICE_REVIEW_LEAF_HINTS = Object.freeze([
  "atm",
  "atms",
  "kiosk",
  "kiosks",
  "rental_kiosks",
  "vending",
  "vending_machine",
  "propane",
  "propane_supplier",
  "money_transfer",
  "money_transfer_services",
  "trust",
  "trusts",
]);

const DEFAULT_PART_RECORD_LIMIT = 5000;
const DEFAULT_CACHE_DIR = "downloads/overture-places/cache";
const DEFAULT_HOSTED_SERVICE_LIST = "config/hosted-service-categories.txt";

/**
 * @typedef {object} LocalOutputLocation
 * @property {"local"} kind Destination kind.
 * @property {string} dir Local extract root.
 */

/**
 * @typedef {object} S3OutputLocation
 * @property {"s3"} kind Destination kind.
 * @property {string} bucket S3 bucket.
 * @property {string} keyPrefix Key prefix without a trailing slash.
 */

/**
 * @typedef {LocalOutputLocation | S3OutputLocation} OutputLocation
 */

/**
 * @typedef {object} ExtractCliOptions
 * @property {string} county County slug (`lee`).
 * @property {string} countyFips Five-digit county FIPS (`12071`).
 * @property {string | null} countyName Optional human county name.
 * @property {string | null} release Pinned Overture release, or null to discover via STAC.
 * @property {string} boundarySource TIGER source token (`tiger/tl_2024_us_county`).
 * @property {string} stacCatalogUrl STAC catalog URL.
 * @property {string} cacheDir Local cache for TIGER shapefiles.
 * @property {string} hostedServiceListPath Path to the committed hosted-service path list.
 * @property {OutputLocation} outputLocation Local directory or S3 prefix.
 * @property {boolean} countsOnly When true, run the two-stage clip probe without JSONL.
 * @property {boolean} keepParquet When true, keep the DuckDB extract parquet.
 * @property {number} partRecordLimit JSONL records per `places-part-NNNN.jsonl`.
 * @property {number | null} limit Optional row cap for probes.
 */

/**
 * @typedef {object} OvertureStacDiscovery
 * @property {string} latest Latest release id from the catalog.
 * @property {string[]} releases Sorted release ids discovered from child links.
 * @property {string} catalogUrl Catalog URL that was fetched.
 * @property {string} retrievedAt ISO timestamp of the fetch.
 */

/**
 * @typedef {object} TigerBoundarySource
 * @property {string} year TIGER vintage year.
 * @property {string} stem Shapefile stem (`tl_2024_us_county`).
 * @property {string} zipUrl Census download URL.
 */

/**
 * @typedef {object} LicenceGateResult
 * @property {boolean} passed True when every dataset is approved and `osm` is absent.
 * @property {string[]} distinctDatasets Sorted unique dataset names.
 * @property {string[]} unknownDatasets Datasets outside {@link APPROVED_PLACE_DATASETS}.
 * @property {boolean} osmPresent True when `osm` appears (any casing).
 * @property {string} message Human-readable pass/fail reason.
 */

/**
 * @typedef {object} HostedServiceMatch
 * @property {boolean} isHostedService True when the full path is on the committed list.
 * @property {string | null} hostedServiceRule Rule id when matched, otherwise null.
 */

/**
 * @typedef {object} HostedServiceRebuild
 * @property {string[]} resolved Full paths whose leaf matches a seed leaf.
 * @property {string[]} unresolvedLeaves Seed leaves with no observed path.
 * @property {string[]} reviewCandidates Observed hosted-looking paths that are not seed matches.
 */

/**
 * @typedef {object} CountyAssignment
 * @property {"geometry"} assignedBy Tie-break rule.
 * @property {string} assignedCountyFips County FIPS from the clip.
 * @property {string} assignedCountyKey County slug from the clip.
 * @property {string | null} addressCounty County named by `addresses[0]`, if any.
 * @property {boolean} discrepancy True when the address names a different county.
 */

/**
 * @typedef {object} ExtractSqlParams
 * @property {string} releaseLiteral Quoted Overture release literal.
 * @property {string} countyFipsLiteral Quoted five-digit FIPS literal.
 * @property {string} boundaryPathLiteral Quoted TIGER shapefile path.
 * @property {string} outLiteral Quoted parquet output path.
 * @property {string} placesGlobLiteral Quoted S3 glob for the places theme.
 * @property {number | null} limit Optional row cap.
 */

/**
 * @typedef {object} PlacesTableValidation
 * @property {boolean} passed True when every publish gate check succeeds.
 * @property {string[]} errors Gate failures (empty when passed).
 */

/**
 * @typedef {object} PlacesNoticeParams
 * @property {string} overtureRelease Overture release id.
 * @property {string} accessedDate ISO date the extract was accessed.
 * @property {string} elephantChangedDate Elephant's own change date (not Overture's).
 * @property {readonly string[]} distinctDatasets Datasets present in the extract.
 */

/**
 * Quote a DuckDB string literal, doubling internal single quotes.
 *
 * @param {string} value Raw string.
 * @returns {string} Quoted SQL literal.
 */
export function duckdbStringLiteral(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

/**
 * Public Overture places parquet glob for one release. No credentials.
 *
 * @param {string} release Overture release id.
 * @returns {string} S3 glob.
 */
export function overturePlacesParquetGlob(release) {
  return `s3://overturemaps-us-west-2/release/${release}/theme=places/type=place/*`;
}

/**
 * Relative JSONL part path (BBB harvest layout).
 *
 * @param {number} partNumber 1-based part index.
 * @returns {string} `places/places-part-NNNN.jsonl`.
 */
export function placesPartPath(partNumber) {
  return `places/places-part-${String(partNumber).padStart(4, "0")}.jsonl`;
}

/**
 * Hosted-service rule id stamped onto matching rows.
 *
 * @param {string} release Overture release id.
 * @returns {string} Rule identifier.
 */
export function hostedServiceRuleId(release) {
  return `hosted-service-categories@${release}`;
}

/**
 * Join a `taxonomy.hierarchy` array into a `/`-delimited path, L0 first.
 *
 * @param {unknown} hierarchy Array, JSON string, or already-joined path.
 * @returns {string | null} Path or null when empty.
 */
export function taxonomyHierarchyToPath(hierarchy) {
  if (typeof hierarchy === "string") {
    const trimmed = hierarchy.trim();
    if (trimmed.startsWith("[")) {
      try {
        return taxonomyHierarchyToPath(JSON.parse(trimmed));
      } catch {
        return trimmed.length > 0 ? trimmed.replaceAll(".", "/") : null;
      }
    }
    return trimmed.length > 0 ? trimmed : null;
  }
  if (!Array.isArray(hierarchy)) return null;
  const parts = hierarchy.flatMap((item) =>
    typeof item === "string" && item.trim().length > 0 ? [item.trim()] : [],
  );
  return parts.length > 0 ? parts.join("/") : null;
}

/**
 * Split a `/`-delimited taxonomy path back into a hierarchy array.
 *
 * @param {string | null} path Full path.
 * @returns {string[]} Hierarchy segments.
 */
export function taxonomyPathToHierarchy(path) {
  if (path === null || path.trim().length === 0) return [];
  return path
    .split("/")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
}

/**
 * Parse the extract CLI used by `scripts/extract-overture-places.mjs`.
 *
 * @param {readonly string[]} argv Arguments after the script name.
 * @returns {ExtractCliOptions} Parsed options.
 */
export function parseExtractCli(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      county: { type: "string" },
      "county-fips": { type: "string" },
      "county-name": { type: "string" },
      release: { type: "string" },
      "boundary-source": { type: "string" },
      "stac-catalog-url": { type: "string" },
      "cache-dir": { type: "string" },
      "hosted-service-list": { type: "string" },
      "output-dir": { type: "string" },
      "output-s3-uri": { type: "string" },
      "counts-only": { type: "boolean" },
      "keep-parquet": { type: "boolean" },
      "no-keep-parquet": { type: "boolean" },
      "part-record-limit": { type: "string" },
      limit: { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });
  const county = readRequiredString(values.county, "--county");
  const countyFips = readRequiredString(values["county-fips"], "--county-fips");
  if (!/^\d{5}$/.test(countyFips)) {
    throw new Error("--county-fips must be a five-digit FIPS code");
  }
  const outputDir = optionalString(values["output-dir"]);
  const outputS3Uri = optionalString(values["output-s3-uri"]);
  if (outputDir !== null && outputS3Uri !== null) {
    throw new Error("Use only one of --output-dir or --output-s3-uri");
  }
  const partRecordLimit =
    parsePositiveInteger(
      optionalString(values["part-record-limit"]),
      "--part-record-limit",
    ) ?? DEFAULT_PART_RECORD_LIMIT;
  const limit = parsePositiveInteger(optionalString(values.limit), "--limit");
  return {
    county,
    countyFips,
    countyName: optionalString(values["county-name"]),
    release: optionalString(values.release),
    boundarySource:
      optionalString(values["boundary-source"]) ?? "tiger/tl_2024_us_county",
    stacCatalogUrl:
      optionalString(values["stac-catalog-url"]) ?? OVERTURE_STAC_CATALOG_URL,
    cacheDir: optionalString(values["cache-dir"]) ?? DEFAULT_CACHE_DIR,
    hostedServiceListPath:
      optionalString(values["hosted-service-list"]) ??
      DEFAULT_HOSTED_SERVICE_LIST,
    outputLocation: parseOutputLocation(outputDir, outputS3Uri, county),
    countsOnly: values["counts-only"] === true,
    keepParquet: values["no-keep-parquet"] === true ? false : true,
    partRecordLimit,
    limit,
  };
}

/**
 * Parse the Overture STAC catalog JSON into a pinned discovery record.
 *
 * @param {unknown} catalog Parsed catalog JSON.
 * @param {string} catalogUrl URL that was fetched.
 * @param {string} retrievedAt ISO timestamp of the fetch.
 * @returns {OvertureStacDiscovery} Latest release plus child release ids.
 */
export function parseOvertureStacCatalog(catalog, catalogUrl, retrievedAt) {
  if (
    catalog === null ||
    typeof catalog !== "object" ||
    Array.isArray(catalog)
  ) {
    throw new Error("Overture STAC catalog is not a JSON object");
  }
  const record = /** @type {Record<string, unknown>} */ (catalog);
  const latest = typeof record.latest === "string" ? record.latest.trim() : "";
  if (latest.length === 0) {
    throw new Error("Overture STAC catalog is missing a `latest` release id");
  }
  /** @type {string[]} */
  const fromLinks = [];
  if (Array.isArray(record.links)) {
    for (const link of record.links) {
      if (link === null || typeof link !== "object" || Array.isArray(link))
        continue;
      const rel = /** @type {Record<string, unknown>} */ (link).rel;
      const href = /** @type {Record<string, unknown>} */ (link).href;
      if (rel !== "child" || typeof href !== "string") continue;
      const match = /(\d{4}-\d{2}-\d{2}\.\d+)/.exec(href);
      if (match !== null && match[1] !== undefined) fromLinks.push(match[1]);
    }
  }
  const releases = [...new Set([latest, ...fromLinks])].sort((a, b) =>
    a.localeCompare(b),
  );
  return { latest, releases, catalogUrl, retrievedAt };
}

/**
 * Parse a `tiger/tl_<year>_us_county` boundary source into a Census zip URL.
 *
 * @param {string} boundarySource CLI `--boundary-source` value.
 * @returns {TigerBoundarySource} TIGER vintage and download URL.
 */
export function parseTigerBoundarySource(boundarySource) {
  const match = /^tiger\/(tl_(\d{4})_us_county)$/.exec(boundarySource.trim());
  if (match === null || match[1] === undefined || match[2] === undefined) {
    throw new Error(
      `--boundary-source must look like tiger/tl_2024_us_county, received ${boundarySource}`,
    );
  }
  const stem = match[1];
  const year = match[2];
  return {
    year,
    stem,
    zipUrl: `https://www2.census.gov/geo/tiger/TIGER${year}/COUNTY/${stem}.zip`,
  };
}

/**
 * Collect distinct `sources[].dataset` names from an Overture sources array.
 *
 * @param {unknown} sources Overture `sources` field (array, JSON string, or DuckDB list).
 * @returns {string[]} Dataset names in first-seen order, duplicates removed.
 */
export function collectDatasetsFromSources(sources) {
  const parsed = parseJsonValue(sources);
  if (!Array.isArray(parsed)) return [];
  /** @type {string[]} */
  const datasets = [];
  const seen = new Set();
  for (const entry of parsed) {
    if (entry === null || typeof entry !== "object" || Array.isArray(entry))
      continue;
    const dataset = /** @type {Record<string, unknown>} */ (entry).dataset;
    if (typeof dataset !== "string" || dataset.trim().length === 0) continue;
    const name = dataset.trim();
    if (seen.has(name)) continue;
    seen.add(name);
    datasets.push(name);
  }
  return datasets;
}

/**
 * Assert that distinct Overture `sources[].dataset` values are a subset of the
 * approved providers. Comparison is case-insensitive (`Microsoft` matches
 * `microsoft`). `osm` (any casing) is always a hard fail. `Overture` and
 * `Overture-signals` are allowed (human decision 2026-08-12: Overture's own
 * lineage, not OSM). Any other unknown dataset fails the gate; do not publish.
 *
 * @param {readonly string[]} datasets Distinct dataset names.
 * @returns {LicenceGateResult} Pass/fail plus the datasets that caused failure.
 */
export function assertApprovedPlaceDatasets(datasets) {
  const distinctDatasets = [
    ...new Set(datasets.map((value) => value.trim()).filter(Boolean)),
  ].sort((a, b) => a.localeCompare(b));
  const unknownDatasets = distinctDatasets.filter(
    (dataset) => !APPROVED_PLACE_DATASET_BY_LOWER.has(dataset.toLowerCase()),
  );
  const osmPresent = distinctDatasets.some(
    (dataset) => dataset.toLowerCase() === "osm",
  );
  const passed = unknownDatasets.length === 0;
  const message = passed
    ? `licence gate passed: ${distinctDatasets.join(", ") || "(no datasets)"}`
    : `licence gate FAILED: unknown dataset(s) ${unknownDatasets.join(", ")}${
        osmPresent ? " (osm present — do not publish)" : ""
      }`;
  return { passed, distinctDatasets, unknownDatasets, osmPresent, message };
}

/**
 * Parse the committed hosted-service list. Comments and blank lines are ignored.
 * Matching requires a full `/`-delimited path; bare leaves are not kept.
 *
 * @param {string} text File contents.
 * @returns {string[]} Full taxonomy paths.
 */
export function parseHostedServiceCategoryList(text) {
  return text
    .split(/\r?\n/)
    .map((line) => line.replace(/#.*$/, "").trim())
    .filter((line) => line.includes("/"));
}

/**
 * Match a place against the committed hosted-service path list. Bare leaves
 * never match — the full path must be present.
 *
 * @param {string | null} taxonomyPath Full `/`-delimited hierarchy path.
 * @param {readonly string[]} hostedServicePaths Committed full paths.
 * @param {string} ruleId Rule id to stamp when matched.
 * @returns {HostedServiceMatch} Flag plus rule id.
 */
export function matchHostedService(taxonomyPath, hostedServicePaths, ruleId) {
  if (taxonomyPath === null || taxonomyPath.length === 0) {
    return { isHostedService: false, hostedServiceRule: null };
  }
  const allowed = new Set(hostedServicePaths);
  if (!allowed.has(taxonomyPath)) {
    return { isHostedService: false, hostedServiceRule: null };
  }
  return { isHostedService: true, hostedServiceRule: ruleId };
}

/**
 * Rebuild full hosted-service paths from seed leaves observed in an extract.
 *
 * @param {object} params Rebuild inputs.
 * @param {readonly string[]} params.observedPaths Distinct full hierarchy paths from the extract.
 * @param {readonly string[]} params.seedLeaves Old flat labels to resolve.
 * @returns {HostedServiceRebuild} Resolved paths, unresolved seeds, and review candidates.
 */
export function rebuildHostedServicePaths(params) {
  /** @type {Map<string, Set<string>>} */
  const aliasesBySeed = new Map(
    params.seedLeaves.map((seed) => [seed, seedLeafAliases(seed)]),
  );
  /** @type {string[]} */
  const resolved = [];
  const resolvedSeeds = new Set();
  for (const observedPath of params.observedPaths) {
    const leaf = pathLeaf(observedPath);
    if (leaf === null) continue;
    for (const [seed, aliases] of aliasesBySeed) {
      if (aliases.has(leaf) && !resolved.includes(observedPath)) {
        resolved.push(observedPath);
        resolvedSeeds.add(seed);
      }
    }
  }
  const unresolvedLeaves = params.seedLeaves.filter(
    (leaf) => !resolvedSeeds.has(leaf),
  );
  const resolvedSet = new Set(resolved);
  const reviewCandidates = params.observedPaths.filter((observedPath) => {
    if (resolvedSet.has(observedPath)) return false;
    const leaf = pathLeaf(observedPath);
    return leaf !== null && isHostedLookingLeaf(leaf);
  });
  return { resolved, unresolvedLeaves, reviewCandidates };
}

/**
 * Format a committed hosted-service list from resolved full paths.
 *
 * @param {object} params Formatter inputs.
 * @param {string} params.release Overture release the paths were observed in.
 * @param {readonly string[]} params.resolved Full paths to commit.
 * @param {readonly string[]} params.unresolvedLeaves Seed leaves still missing.
 * @param {readonly string[]} params.reviewCandidates Extra hosted-looking paths.
 * @returns {string} File body.
 */
export function formatHostedServiceCategoryList(params) {
  const lines = [
    "# Hosted-service categories — services offered INSIDE another business rather",
    "# than a business occupying a location of its own.",
    "#",
    "# Sets `business_locations.is_hosted_service = true` and records",
    `# \`hosted_service_rule = 'hosted-service-categories@${params.release}'\`.`,
    "# Advisory only: places are never excluded from ingestion on the strength of",
    "# this list, and consumers may ignore it.",
    "#",
    "# Format: one taxonomy path per line, `/`-delimited, L0 first.",
    "# Matching is on the FULL path so a rename at one level does not silently widen.",
    "#",
    `# Rebuilt from the Lee County extract of Overture ${params.release} against`,
    "# `taxonomy.hierarchy`. The five seed labels (`atms`, `rental_kiosks`,",
    "# `propane_supplier`, `money_transfer_services`, `trusts`) are the old flat",
    "# `categories.primary` vocab; paths below are the observed full hierarchy",
    "# paths whose leaf matches those seeds. Re-review whenever the taxonomy moves",
    "# (quarterly).",
    "",
  ];
  for (const path of params.resolved) lines.push(path);
  if (params.unresolvedLeaves.length > 0) {
    lines.push("");
    lines.push("# Unresolved seed leaves (not observed in this extract):");
    for (const leaf of params.unresolvedLeaves) lines.push(`# ${leaf}`);
  }
  if (params.reviewCandidates.length > 0) {
    lines.push("");
    lines.push(
      "# Review candidates (hosted-looking leaves, not auto-committed):",
    );
    for (const path of params.reviewCandidates) lines.push(`# ${path}`);
  }
  lines.push("");
  return lines.join("\n");
}

/**
 * Assign a place to the county of its geometry. If `addresses[0]` names a
 * different county, record the discrepancy for `source_payload`.
 *
 * @param {object} params Assignment inputs.
 * @param {string} params.countyFips Clip FIPS.
 * @param {string} params.countyKey Clip county slug.
 * @param {string} params.countyName Human county name (`Lee`).
 * @param {unknown} params.address0 Overture `addresses[0]` object or JSON.
 * @returns {CountyAssignment} Geometry assignment plus optional discrepancy.
 */
export function buildCountyAssignment(params) {
  const address = parseJsonObject(params.address0);
  const addressCounty = readAddressCounty(address);
  const discrepancy =
    addressCounty !== null &&
    !countiesMatch(addressCounty, params.countyName, params.countyKey);
  return {
    assignedBy: "geometry",
    assignedCountyFips: params.countyFips,
    assignedCountyKey: params.countyKey,
    addressCounty,
    discrepancy,
  };
}

/**
 * Two-stage extract COPY SQL: parquet bbox prune, then `ST_Within`.
 *
 * @param {ExtractSqlParams} params Quoted SQL literals.
 * @returns {string} DuckDB SQL.
 */
export function buildExtractCopySql(params) {
  const limitSql = params.limit === null ? "" : `\n  LIMIT ${params.limit}`;
  return `${extractPreambleSql(params)}
COPY (
  SELECT
    p.id                                AS gers_id,
    p.version                           AS overture_version,
    p.names.primary                     AS name_primary,
    p.taxonomy.primary                  AS taxonomy_primary,
    p.taxonomy.hierarchy                AS taxonomy_hierarchy,
    p.taxonomy.alternates               AS taxonomy_alternate,
    p.basic_category                    AS basic_category,
    p.categories.primary                AS legacy_category_primary,
    p.operating_status                  AS operating_status,
    p.confidence                        AS confidence,
    p.websites                          AS websites,
    p.socials                           AS socials,
    p.emails                            AS emails,
    p.phones                            AS phones,
    p.brand.names.primary               AS brand_name,
    p.brand.wikidata                    AS brand_wikidata,
    p.addresses[1].freeform             AS address_freeform,
    p.addresses[1].locality             AS address_locality,
    p.addresses[1].postcode             AS address_postcode,
    p.addresses[1].region               AS address_region,
    p.addresses[1].country              AS address_country,
    p.addresses[1]                      AS address0,
    p.sources                           AS sources,
    ST_X(p.geometry)                    AS longitude,
    ST_Y(p.geometry)                    AS latitude,
    ST_AsGeoJSON(p.geometry)            AS geometry_geojson,
    ST_AsWKB(p.geometry)                AS geometry_wkb,
    ${params.releaseLiteral}            AS overture_release,
    ${params.countyFipsLiteral}         AS county_fips
  FROM read_parquet(
    ${params.placesGlobLiteral},
    hive_partitioning = 1
  ) AS p,
  county_bbox AS b,
  county_boundary AS c
  WHERE p.bbox.xmin >= b.xmin
    AND p.bbox.xmax <= b.xmax
    AND p.bbox.ymin >= b.ymin
    AND p.bbox.ymax <= b.ymax
    AND ST_Within(p.geometry, c.geometry)${limitSql}
) TO ${params.outLiteral} (FORMAT PARQUET);
`;
}

/**
 * Two-stage count SQL. `bbox_count` is an optimisation diagnostic and must
 * never be published as the county count.
 *
 * @param {ExtractSqlParams} params Quoted SQL literals.
 * @returns {string} DuckDB SQL returning `bbox_count` and `clip_count`.
 */
export function buildExtractCountSql(params) {
  const clipLimit = params.limit === null ? "" : `\n    LIMIT ${params.limit}`;
  return `${extractPreambleSql(params)}
SELECT
  (
    SELECT count(*)
    FROM read_parquet(${params.placesGlobLiteral}, hive_partitioning = 1) AS p,
         county_bbox AS b
    WHERE p.bbox.xmin >= b.xmin
      AND p.bbox.xmax <= b.xmax
      AND p.bbox.ymin >= b.ymin
      AND p.bbox.ymax <= b.ymax
  ) AS bbox_count,
  (
    SELECT count(*)
    FROM (
      SELECT 1
      FROM read_parquet(${params.placesGlobLiteral}, hive_partitioning = 1) AS p,
           county_bbox AS b,
           county_boundary AS c
      WHERE p.bbox.xmin >= b.xmin
        AND p.bbox.xmax <= b.xmax
        AND p.bbox.ymin >= b.ymin
        AND p.bbox.ymax <= b.ymax
        AND ST_Within(p.geometry, c.geometry)${clipLimit}
    )
  ) AS clip_count;
`;
}

/**
 * Count operating_status values for the extract summary.
 *
 * @param {readonly string[]} statuses Raw status strings.
 * @returns {Record<string, number>} Counts by status.
 */
export function countByOperatingStatus(statuses) {
  /** @type {Record<string, number>} */
  const counts = {};
  for (const status of statuses) {
    const key = status.trim().length > 0 ? status.trim() : "(blank)";
    counts[key] = (counts[key] ?? 0) + 1;
  }
  return counts;
}

/**
 * Summarise confidence scores. Overture's score is not calibrated across
 * providers; this is a diagnostic, not a filter.
 *
 * @param {readonly (number | null)[]} values Confidence values.
 * @returns {{ count: number, min: number | null, max: number | null, mean: number | null }}
 */
export function confidenceDistribution(values) {
  const numbers = values.filter(
    /** @type {(value: number | null) => value is number} */ (value) =>
      typeof value === "number" && Number.isFinite(value),
  );
  if (numbers.length === 0) {
    return { count: 0, min: null, max: null, mean: null };
  }
  const min = Math.min(...numbers);
  const max = Math.max(...numbers);
  const mean = numbers.reduce((sum, value) => sum + value, 0) / numbers.length;
  return { count: numbers.length, min, max, mean };
}

/**
 * Publish gate: parquet row count, unique GERS ids, null geometries, licence.
 *
 * @param {object} params Gate inputs.
 * @param {number} params.parquetRowCount Rows in the published parquet.
 * @param {number} params.businessLocationRowCount Current `business_locations` rows for the county+release.
 * @param {readonly string[]} params.gersIds GERS ids from the parquet (or extract).
 * @param {number} params.nullGeometryCount Rows missing lon/lat.
 * @param {LicenceGateResult} params.licenceGate Licence assertion.
 * @param {number} [params.invalidHierarchyCount] Rows whose `taxonomy.hierarchy` is not a `/`-delimited scalar.
 * @param {number} [params.hierarchyPresentCount] Rows with a non-empty hierarchy scalar.
 * @returns {PlacesTableValidation} Pass/fail plus error strings.
 */
export function validatePlacesTable(params) {
  /** @type {string[]} */
  const errors = [];
  if (params.parquetRowCount !== params.businessLocationRowCount) {
    errors.push(
      `parquet row count ${params.parquetRowCount} != business_locations ${params.businessLocationRowCount}`,
    );
  }
  const unique = new Set(params.gersIds);
  if (unique.size !== params.gersIds.length) {
    errors.push(
      `duplicate GERS ids: ${params.gersIds.length} rows, ${unique.size} unique`,
    );
  }
  if (params.nullGeometryCount > 0) {
    errors.push(`${params.nullGeometryCount} null geometries`);
  }
  if (!params.licenceGate.passed) {
    errors.push(params.licenceGate.message);
  }
  if (
    params.invalidHierarchyCount !== undefined &&
    params.invalidHierarchyCount > 0
  ) {
    errors.push(
      `${params.invalidHierarchyCount} rows have invalid taxonomy.hierarchy scalar serialization`,
    );
  }
  if (
    params.hierarchyPresentCount !== undefined &&
    params.hierarchyPresentCount <= 0
  ) {
    errors.push("taxonomy.hierarchy scalar serialization is absent");
  }
  return { passed: errors.length === 0, errors };
}

/**
 * True when a published `taxonomy.hierarchy` value is a `/`-delimited scalar
 * (or empty). JSON arrays and Postgres `{a,b}` array literals fail.
 *
 * @param {unknown} value Parquet cell.
 * @returns {boolean} Whether the value is publishable as a scalar path.
 */
export function isValidTaxonomyHierarchyScalar(value) {
  if (value === null || value === undefined) return true;
  if (typeof value !== "string") return false;
  const trimmed = value.trim();
  if (trimmed.length === 0) return true;
  if (trimmed.startsWith("[") || trimmed.startsWith("{")) return false;
  if (trimmed.includes(",") && !trimmed.includes("/")) return false;
  return true;
}

/**
 * Coerce a pg numeric string or JS number to a finite number.
 *
 * @param {unknown} value Raw cell.
 * @returns {number | null} Finite number, or null.
 */
export function coerceFiniteNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

/**
 * Render NOTICE.txt with Overture citation, per-provider licences, the
 * Foursquare copyright line, and Elephant's own change statement.
 *
 * @param {PlacesNoticeParams} params Notice fields.
 * @returns {string} NOTICE.txt body.
 */
export function renderPlacesNotice(params) {
  const datasets =
    params.distinctDatasets.length > 0
      ? params.distinctDatasets.join(", ")
      : "(none)";
  const licenceLines = params.distinctDatasets.map((dataset) => {
    const licence =
      PLACE_DATASET_LICENCE_BY_LOWER.get(dataset.toLowerCase()) ??
      "UNKNOWN — do not publish";
    return `- ${dataset}: ${licence}`;
  });
  return [
    "Overture Maps Foundation Places",
    `Release: ${params.overtureRelease}`,
    `Accessed: ${params.accessedDate}`,
    "",
    "This dataset is derived from Overture Maps places data published at",
    `s3://overturemaps-us-west-2/release/${params.overtureRelease}/theme=places/type=place/.`,
    "Overture theme licence: CDLA-Permissive-2.0 and Apache-2.0 per record, with no",
    "OpenStreetMap lineage in the places theme. These terms were read from Overture's",
    "published documentation on 2026-08-12 and have not been reviewed by counsel.",
    "",
    `Per-provider licences for datasets present in this extract (${datasets}):`,
    ...(licenceLines.length > 0 ? licenceLines : ["- (none)"]),
    "",
    "Copyright 2024 Foursquare Labs, Inc. All rights reserved.",
    "",
    `Changed by Elephant: ${params.elephantChangedDate}`,
    "Elephant clipped the global places theme to a Census TIGER/Line county boundary,",
    "serialized taxonomy.hierarchy as a /-delimited string in the published parquet,",
    "and added an advisory is_hosted_service flag. This change statement describes",
    "Elephant's transformation, not Overture's.",
    "",
    "Joining CDLA-Permissive data to OpenStreetMap can make the resulting derivative",
    "database ODbL. Do not combine this artifact with an ODbL source without a",
    "licence review.",
    "",
  ].join("\n");
}

/**
 * Parse the county slug from a places artifact URI
 * (`.../overture-places/<county>/<release>/...`).
 *
 * @param {string} artifactUri S3 URI or local path.
 * @returns {string | null} County slug, or null when the prefix is absent.
 */
export function countySlugFromPlacesArtifactUri(artifactUri) {
  const match = /overture-places\/([^/]+)/i.exec(artifactUri);
  return match?.[1]?.toLowerCase() ?? null;
}

/**
 * @param {ExtractSqlParams} params Quoted SQL literals.
 * @returns {string} LOAD / SET / boundary temp-table SQL.
 */
function extractPreambleSql(params) {
  return `LOAD spatial;
LOAD httpfs;
SET s3_region = 'us-west-2';

CREATE OR REPLACE TEMP TABLE county_boundary AS
SELECT geom AS geometry
FROM ST_Read(${params.boundaryPathLiteral})
WHERE GEOID = ${params.countyFipsLiteral};

CREATE OR REPLACE TEMP TABLE county_bbox AS
SELECT
  ST_XMin(ST_Extent(geometry)) AS xmin,
  ST_XMax(ST_Extent(geometry)) AS xmax,
  ST_YMin(ST_Extent(geometry)) AS ymin,
  ST_YMax(ST_Extent(geometry)) AS ymax
FROM county_boundary;
`;
}

/**
 * @param {string | null} outputDir Local `--output-dir`.
 * @param {string | null} outputS3Uri `--output-s3-uri`.
 * @param {string} county County slug used in the default local path.
 * @returns {OutputLocation} Destination.
 */
function parseOutputLocation(outputDir, outputS3Uri, county) {
  if (outputS3Uri !== null) {
    const parsed = new URL(outputS3Uri);
    if (parsed.protocol !== "s3:") {
      throw new Error(`Expected s3:// URI, received ${outputS3Uri}`);
    }
    if (parsed.hostname.length === 0) {
      throw new Error(`S3 URI is missing bucket: ${outputS3Uri}`);
    }
    return {
      kind: "s3",
      bucket: parsed.hostname,
      keyPrefix: decodeURIComponent(parsed.pathname.replace(/^\//, "")).replace(
        /\/$/,
        "",
      ),
    };
  }
  return {
    kind: "local",
    dir: outputDir ?? `downloads/overture-places/${county}`,
  };
}

/**
 * @param {unknown} value parseArgs string option.
 * @param {string} flag Flag name for errors.
 * @returns {string} Non-empty string.
 */
function readRequiredString(value, flag) {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${flag} is required`);
  }
  return value.trim();
}

/**
 * @param {unknown} value parseArgs string option.
 * @returns {string | null} Trimmed string or null.
 */
function optionalString(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/**
 * @param {string | null} value Raw integer string.
 * @param {string} flag Flag name for errors.
 * @returns {number | null} Positive integer or null when omitted.
 */
function parsePositiveInteger(value, flag) {
  if (value === null) return null;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`${flag} must be a positive integer`);
  }
  return parsed;
}

/**
 * @param {unknown} value Unknown JSON-ish value.
 * @returns {unknown} Parsed value.
 */
function parseJsonValue(value) {
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed.startsWith("[") || trimmed.startsWith("{")) {
      try {
        return JSON.parse(trimmed);
      } catch {
        return value;
      }
    }
    return value;
  }
  return value;
}

/**
 * @param {unknown} value Unknown object or JSON string.
 * @returns {Record<string, unknown> | null} Object or null.
 */
function parseJsonObject(value) {
  const parsed = parseJsonValue(value);
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed))
    return null;
  return /** @type {Record<string, unknown>} */ (parsed);
}

/**
 * @param {Record<string, unknown> | null} address Overture address object.
 * @returns {string | null} County name from `county` / `county_name` / `region`.
 */
function readAddressCounty(address) {
  if (address === null) return null;
  for (const key of ["county", "county_name", "countyName"]) {
    const value = address[key];
    if (typeof value === "string" && value.trim().length > 0)
      return value.trim();
  }
  return null;
}

/**
 * @param {string} addressCounty County named by the address.
 * @param {string} countyName Assigned human name.
 * @param {string} countyKey Assigned slug.
 * @returns {boolean} True when the names refer to the same county.
 */
function countiesMatch(addressCounty, countyName, countyKey) {
  const normalize = (value) =>
    value
      .toLowerCase()
      .replace(/\bcounty\b/g, "")
      .replace(/[^a-z0-9]+/g, "")
      .trim();
  const address = normalize(addressCounty);
  return address === normalize(countyName) || address === normalize(countyKey);
}

/**
 * @param {string} path Full taxonomy path.
 * @returns {string | null} Last segment.
 */
function pathLeaf(path) {
  const parts = taxonomyPathToHierarchy(path);
  return parts.length > 0 ? (parts[parts.length - 1] ?? null) : null;
}

/**
 * @param {string} seed Old flat seed leaf.
 * @returns {Set<string>} Seed plus a singular/plural alias.
 */
function seedLeafAliases(seed) {
  const aliases = new Set([seed]);
  if (seed.endsWith("s") && !seed.endsWith("ss") && seed.length > 1) {
    aliases.add(seed.slice(0, -1));
  } else {
    aliases.add(`${seed}s`);
  }
  return aliases;
}

/**
 * @param {string} leaf Taxonomy leaf label.
 * @returns {boolean} True when the leaf looks hosted-service-like.
 */
function isHostedLookingLeaf(leaf) {
  const lower = leaf.toLowerCase();
  const tokens = new Set(lower.split("_"));
  return HOSTED_SERVICE_REVIEW_LEAF_HINTS.some(
    (hint) =>
      lower === hint ||
      tokens.has(hint) ||
      lower.startsWith(`${hint}_`) ||
      lower.endsWith(`_${hint}`),
  );
}
