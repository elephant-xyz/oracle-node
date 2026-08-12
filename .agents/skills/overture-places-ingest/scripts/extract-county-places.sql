-- Boundary-clipped Overture places extraction for one county.
--
-- STATUS: reference query, NOT EXECUTED. Field names and the release path were read from
-- Overture's published schema and places guide on 2026-08-12; the query itself has not been
-- run against the bucket from this repo. Confirm it before wiring it into a script.
--
-- Usage (DuckDB >= 1.1):
--   duckdb -c ".read extract-county-places.sql"
-- with $RELEASE, $COUNTY_FIPS, $BOUNDARY_PATH and $OUT substituted by the caller.
--
-- Two-stage filter is deliberate: the bbox predicate prunes parquet row groups cheaply,
-- ST_Within is the actual county test. Never report the bbox count as the county count.

LOAD spatial;
LOAD httpfs;
SET s3_region = 'us-west-2';

-- County boundary from Census TIGER/Line, selected by FIPS. Overture geometry is EPSG:4326;
-- TIGER is served in 4326 as well, so no transform is needed here. (A comparison against
-- `parcel_boundaries` DOES need one — that table is EPSG:2237, Florida West feet.)
CREATE OR REPLACE TEMP TABLE county_boundary AS
SELECT geom AS geometry
FROM ST_Read($BOUNDARY_PATH)
WHERE GEOID = $COUNTY_FIPS;

CREATE OR REPLACE TEMP TABLE county_bbox AS
SELECT
  ST_XMin(ST_Extent(geometry)) AS xmin,
  ST_XMax(ST_Extent(geometry)) AS xmax,
  ST_YMin(ST_Extent(geometry)) AS ymin,
  ST_YMax(ST_Extent(geometry)) AS ymax
FROM county_boundary;

COPY (
  SELECT
    p.id                                AS gers_id,
    p.version                           AS overture_version,
    p.names.primary                     AS name_primary,
    p.taxonomy.primary                  AS taxonomy_primary,
    p.taxonomy.hierarchy                AS taxonomy_hierarchy,
    p.taxonomy.alternate                AS taxonomy_alternate,
    p.basic_category                    AS basic_category,
    -- Deprecated: removed in the September 2026 release. Retained only so the scoping
    -- numbers stay reconcilable. Drop this column once September lands.
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
    -- Kept whole: the licence gate and re-identification via bridge files both need every
    -- entry, not just the promoted one.
    p.sources                           AS sources,
    ST_AsWKB(p.geometry)                AS geometry_wkb,
    $RELEASE                            AS overture_release,
    $COUNTY_FIPS                        AS county_fips
  FROM read_parquet(
    's3://overturemaps-us-west-2/release/' || $RELEASE || '/theme=places/type=place/*',
    hive_partitioning = 1
  ) AS p,
  county_bbox AS b,
  county_boundary AS c
  WHERE p.bbox.xmin >= b.xmin
    AND p.bbox.xmax <= b.xmax
    AND p.bbox.ymin >= b.ymin
    AND p.bbox.ymax <= b.ymax
    AND ST_Within(p.geometry, c.geometry)
) TO $OUT (FORMAT PARQUET);

-- Run-record counters. These become the `overture_place_extractions` row; they are the
-- honest numbers, and none of them is an `expected_count` (see SKILL.md §4).
--
--   SELECT count(*) FROM read_parquet($OUT);
--   SELECT count(DISTINCT taxonomy_primary) FROM read_parquet($OUT);
--   SELECT operating_status, count(*) FROM read_parquet($OUT) GROUP BY 1;
--   SELECT DISTINCT unnest(sources).dataset FROM read_parquet($OUT);   -- licence gate
