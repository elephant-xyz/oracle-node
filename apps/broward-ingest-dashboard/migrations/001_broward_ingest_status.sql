BEGIN;

CREATE SCHEMA IF NOT EXISTS ingest_control;

CREATE TABLE IF NOT EXISTS ingest_control.broward_ingest_status (
  pipeline_key text PRIMARY KEY
    CHECK (pipeline_key = 'broward-appraisal'),
  denominator_count bigint NOT NULL
    CHECK (denominator_count = 534309),
  attempted_count bigint NOT NULL DEFAULT 0
    CHECK (attempted_count BETWEEN 0 AND 534309),
  succeeded_count bigint NOT NULL DEFAULT 0
    CHECK (succeeded_count >= 0),
  source_miss_count bigint NOT NULL DEFAULT 0
    CHECK (source_miss_count >= 0),
  source_failure_count bigint NOT NULL DEFAULT 0
    CHECK (source_failure_count >= 0),
  transform_failure_count bigint NOT NULL DEFAULT 0
    CHECK (transform_failure_count >= 0),
  load_failure_count bigint NOT NULL DEFAULT 0
    CHECK (load_failure_count >= 0),
  phase text NOT NULL DEFAULT 'not_started'
    CHECK (
      phase IN (
        'not_started',
        'pilot',
        'capturing',
        'transforming',
        'loading',
        'verifying',
        'full',
        'paused',
        'failed',
        'complete'
      )
    ),
  started_at timestamptz,
  heartbeat_at timestamptz,
  stale_after_seconds integer NOT NULL DEFAULT 180
    CHECK (stale_after_seconds BETWEEN 30 AND 3600),
  throughput_window_seconds integer NOT NULL DEFAULT 900
    CHECK (throughput_window_seconds BETWEEN 60 AND 86400),
  throughput_attempted_count bigint NOT NULL DEFAULT 0
    CHECK (throughput_attempted_count >= 0),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (succeeded_count <= attempted_count),
  CHECK (source_miss_count <= attempted_count),
  CHECK (succeeded_count + source_miss_count <= attempted_count),
  CHECK (heartbeat_at IS NULL OR started_at IS NOT NULL)
);

COMMENT ON TABLE ingest_control.broward_ingest_status IS
  'Aggregate-only Broward ingestion heartbeat and progress; private source data is forbidden.';

CREATE TABLE IF NOT EXISTS ingest_control.broward_ingest_category_coverage (
  pipeline_key text NOT NULL
    REFERENCES ingest_control.broward_ingest_status (pipeline_key)
    ON DELETE CASCADE,
  category_key text NOT NULL
    CHECK (category_key ~ '^[A-Za-z][A-Za-z0-9]{0,63}$'),
  succeeded_count bigint NOT NULL
    CHECK (succeeded_count BETWEEN 0 AND 534309),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (pipeline_key, category_key)
);

COMMENT ON TABLE ingest_control.broward_ingest_category_coverage IS
  'Aggregate Lexicon category counts only; labels, source values, and identifiers are forbidden.';

CREATE OR REPLACE FUNCTION ingest_control.record_broward_ingest_status(
  p_phase text,
  p_attempted_count bigint,
  p_succeeded_count bigint,
  p_source_miss_count bigint,
  p_source_failure_count bigint,
  p_transform_failure_count bigint,
  p_load_failure_count bigint,
  p_throughput_window_seconds integer,
  p_throughput_attempted_count bigint,
  p_category_coverage jsonb,
  p_heartbeat_at timestamptz DEFAULT clock_timestamp()
) RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, ingest_control
AS $function$
BEGIN
  IF jsonb_typeof(p_category_coverage) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION 'Category coverage must be a JSON object';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM jsonb_each(p_category_coverage) AS category(category_key, category_count)
    WHERE category.category_key !~ '^[A-Za-z][A-Za-z0-9]{0,63}$'
       OR jsonb_typeof(category.category_count) IS DISTINCT FROM 'number'
       OR category.category_count #>> '{}' !~ '^[0-9]+$'
       OR (category.category_count #>> '{}')::numeric > 534309
  ) THEN
    RAISE EXCEPTION 'Category coverage contains an invalid aggregate';
  END IF;
  IF COALESCE((
    SELECT sum((category.category_count #>> '{}')::bigint)
    FROM jsonb_each(p_category_coverage) AS category(category_key, category_count)
  ), 0) > p_succeeded_count THEN
    RAISE EXCEPTION 'Category coverage exceeds verified successes';
  END IF;

  INSERT INTO ingest_control.broward_ingest_status (
    pipeline_key,
    denominator_count,
    attempted_count,
    succeeded_count,
    source_miss_count,
    source_failure_count,
    transform_failure_count,
    load_failure_count,
    phase,
    started_at,
    heartbeat_at,
    stale_after_seconds,
    throughput_window_seconds,
    throughput_attempted_count,
    updated_at
  ) VALUES (
    'broward-appraisal',
    534309,
    p_attempted_count,
    p_succeeded_count,
    p_source_miss_count,
    p_source_failure_count,
    p_transform_failure_count,
    p_load_failure_count,
    p_phase,
    p_heartbeat_at,
    p_heartbeat_at,
    180,
    p_throughput_window_seconds,
    p_throughput_attempted_count,
    p_heartbeat_at
  )
  ON CONFLICT (pipeline_key) DO UPDATE SET
    attempted_count = EXCLUDED.attempted_count,
    succeeded_count = EXCLUDED.succeeded_count,
    source_miss_count = EXCLUDED.source_miss_count,
    source_failure_count = EXCLUDED.source_failure_count,
    transform_failure_count = EXCLUDED.transform_failure_count,
    load_failure_count = EXCLUDED.load_failure_count,
    phase = EXCLUDED.phase,
    started_at = COALESCE(
      ingest_control.broward_ingest_status.started_at,
      EXCLUDED.started_at
    ),
    heartbeat_at = EXCLUDED.heartbeat_at,
    throughput_window_seconds = EXCLUDED.throughput_window_seconds,
    throughput_attempted_count = EXCLUDED.throughput_attempted_count,
    updated_at = EXCLUDED.updated_at;

  DELETE FROM ingest_control.broward_ingest_category_coverage
  WHERE pipeline_key = 'broward-appraisal';

  INSERT INTO ingest_control.broward_ingest_category_coverage (
    pipeline_key,
    category_key,
    succeeded_count,
    updated_at
  )
  SELECT
    'broward-appraisal',
    category.category_key,
    (category.category_count #>> '{}')::bigint,
    p_heartbeat_at
  FROM jsonb_each(p_category_coverage) AS category(category_key, category_count);
END;
$function$;

COMMENT ON FUNCTION ingest_control.record_broward_ingest_status(
  text,
  bigint,
  bigint,
  bigint,
  bigint,
  bigint,
  bigint,
  integer,
  bigint,
  jsonb,
  timestamptz
) IS
  'Atomically replaces the aggregate Broward heartbeat and category snapshot; callers must never pass source records.';

INSERT INTO ingest_control.broward_ingest_status (
  pipeline_key,
  denominator_count
) VALUES (
  'broward-appraisal',
  534309
)
ON CONFLICT (pipeline_key) DO NOTHING;

COMMIT;
