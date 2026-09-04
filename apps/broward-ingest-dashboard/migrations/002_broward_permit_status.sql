BEGIN;

CREATE TABLE IF NOT EXISTS ingest_control.broward_permit_control (
  pipeline_key text PRIMARY KEY
    CHECK (pipeline_key = 'broward-permit'),
  contract_version integer NOT NULL
    CHECK (contract_version = 1),
  max_sample_parcels integer NOT NULL
    CHECK (max_sample_parcels = 50),
  max_source_attempts integer NOT NULL
    CHECK (max_source_attempts = 5),
  registry_jurisdiction_count integer NOT NULL
    CHECK (registry_jurisdiction_count = 32),
  current_source_implemented_count integer NOT NULL
    CHECK (current_source_implemented_count BETWEEN 0 AND registry_jurisdiction_count),
  current_source_blocked_count integer NOT NULL
    CHECK (
      current_source_blocked_count >= 0
      AND current_source_implemented_count + current_source_blocked_count
        = registry_jurisdiction_count
    ),
  updated_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE ingest_control.broward_permit_control IS
  'Fixed aggregate safety bounds and route-coverage counts for the bounded Broward permit pilot.';

INSERT INTO ingest_control.broward_permit_control (
  pipeline_key,
  contract_version,
  max_sample_parcels,
  max_source_attempts,
  registry_jurisdiction_count,
  current_source_implemented_count,
  current_source_blocked_count
) VALUES (
  'broward-permit',
  1,
  50,
  5,
  32,
  24,
  8
)
ON CONFLICT (pipeline_key) DO UPDATE SET
  contract_version = EXCLUDED.contract_version,
  max_sample_parcels = EXCLUDED.max_sample_parcels,
  max_source_attempts = EXCLUDED.max_source_attempts,
  registry_jurisdiction_count = EXCLUDED.registry_jurisdiction_count,
  current_source_implemented_count = EXCLUDED.current_source_implemented_count,
  current_source_blocked_count = EXCLUDED.current_source_blocked_count,
  updated_at = now();

CREATE TABLE IF NOT EXISTS ingest_control.broward_permit_status (
  pipeline_key text PRIMARY KEY
    REFERENCES ingest_control.broward_permit_control (pipeline_key)
    ON DELETE RESTRICT,
  sample_parcels integer NOT NULL
    CHECK (sample_parcels BETWEEN 1 AND 50),
  appraisal_attempts integer NOT NULL
    CHECK (appraisal_attempts >= sample_parcels),
  appraisal_resolved integer NOT NULL
    CHECK (appraisal_resolved BETWEEN 0 AND sample_parcels),
  jurisdiction_resolved integer NOT NULL
    CHECK (jurisdiction_resolved BETWEEN 0 AND sample_parcels),
  jurisdiction_unresolved integer NOT NULL
    CHECK (jurisdiction_unresolved BETWEEN 0 AND sample_parcels),
  source_outcomes integer NOT NULL
    CHECK (source_outcomes >= 0),
  source_unavailable_outcomes integer NOT NULL
    CHECK (source_unavailable_outcomes BETWEEN 0 AND source_outcomes),
  permit_source_attempts integer NOT NULL
    CHECK (permit_source_attempts BETWEEN 0 AND 5),
  permit_attempted_parcels integer NOT NULL
    CHECK (permit_attempted_parcels BETWEEN 0 AND sample_parcels),
  explicit_no_permit_outcomes integer NOT NULL
    CHECK (explicit_no_permit_outcomes BETWEEN 0 AND permit_source_attempts),
  source_failures integer NOT NULL
    CHECK (source_failures BETWEEN 0 AND permit_source_attempts),
  raw_permit_records integer NOT NULL
    CHECK (raw_permit_records >= 0),
  duplicate_permit_records integer NOT NULL
    CHECK (duplicate_permit_records >= 0),
  conflicting_permit_records integer NOT NULL
    CHECK (conflicting_permit_records >= 0),
  unique_permit_records integer NOT NULL
    CHECK (unique_permit_records >= 0),
  query_rows integer NOT NULL
    CHECK (query_rows >= 0),
  all_input_parcels_terminal boolean NOT NULL,
  all_records_accounted_for boolean NOT NULL,
  query_rows_match_unique_records boolean NOT NULL,
  local_pilot_passed boolean NOT NULL,
  county_permit_complete boolean NOT NULL,
  recorded_at timestamptz NOT NULL,
  CHECK (
    NOT all_records_accounted_for
    OR raw_permit_records
      = unique_permit_records
        + duplicate_permit_records
        + conflicting_permit_records
  ),
  CHECK (
    NOT query_rows_match_unique_records
    OR query_rows = unique_permit_records
  ),
  CHECK (
    NOT local_pilot_passed
    OR (
      all_input_parcels_terminal
      AND all_records_accounted_for
      AND query_rows_match_unique_records
      AND appraisal_resolved = sample_parcels
      AND jurisdiction_unresolved = 0
      AND source_failures = 0
      AND conflicting_permit_records = 0
    )
  ),
  CHECK (NOT county_permit_complete OR local_pilot_passed)
);

COMMENT ON TABLE ingest_control.broward_permit_status IS
  'Latest aggregate-only bounded Broward permit pilot reconciliation; absence means no durable pilot evidence.';

CREATE TABLE IF NOT EXISTS ingest_control.broward_permit_events (
  event_key text PRIMARY KEY
    CHECK (event_key = 'bounded-pilot-current'),
  event_type text NOT NULL
    CHECK (event_type = 'pilot_reconciled'),
  sample_parcels integer NOT NULL
    CHECK (sample_parcels BETWEEN 1 AND 50),
  permit_source_attempts integer NOT NULL
    CHECK (permit_source_attempts BETWEEN 0 AND 5),
  query_rows integer NOT NULL
    CHECK (query_rows >= 0),
  local_pilot_passed boolean NOT NULL,
  county_permit_complete boolean NOT NULL,
  recorded_at timestamptz NOT NULL,
  CHECK (NOT county_permit_complete OR local_pilot_passed)
);

COMMENT ON TABLE ingest_control.broward_permit_events IS
  'Idempotent aggregate event evidence for the current bounded permit pilot reconciliation.';

CREATE OR REPLACE FUNCTION ingest_control.record_broward_permit_pilot_status(
  p_sample_parcels integer,
  p_appraisal_attempts integer,
  p_appraisal_resolved integer,
  p_jurisdiction_resolved integer,
  p_jurisdiction_unresolved integer,
  p_source_outcomes integer,
  p_source_unavailable_outcomes integer,
  p_permit_source_attempts integer,
  p_permit_attempted_parcels integer,
  p_explicit_no_permit_outcomes integer,
  p_source_failures integer,
  p_raw_permit_records integer,
  p_duplicate_permit_records integer,
  p_conflicting_permit_records integer,
  p_unique_permit_records integer,
  p_query_rows integer,
  p_all_input_parcels_terminal boolean,
  p_all_records_accounted_for boolean,
  p_query_rows_match_unique_records boolean,
  p_local_pilot_passed boolean,
  p_county_permit_complete boolean,
  p_recorded_at timestamptz DEFAULT clock_timestamp()
) RETURNS void
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, ingest_control
AS $function$
DECLARE
  v_current_source_blocked_count integer;
BEGIN
  SELECT current_source_blocked_count
  INTO STRICT v_current_source_blocked_count
  FROM ingest_control.broward_permit_control
  WHERE pipeline_key = 'broward-permit';

  IF p_county_permit_complete AND (
    NOT p_local_pilot_passed
    OR v_current_source_blocked_count <> 0
    OR p_source_unavailable_outcomes <> 0
    OR p_query_rows = 0
  ) THEN
    RAISE EXCEPTION
      'County permit completeness requires all current routes and pilot reconciliation gates';
  END IF;

  INSERT INTO ingest_control.broward_permit_status (
    pipeline_key,
    sample_parcels,
    appraisal_attempts,
    appraisal_resolved,
    jurisdiction_resolved,
    jurisdiction_unresolved,
    source_outcomes,
    source_unavailable_outcomes,
    permit_source_attempts,
    permit_attempted_parcels,
    explicit_no_permit_outcomes,
    source_failures,
    raw_permit_records,
    duplicate_permit_records,
    conflicting_permit_records,
    unique_permit_records,
    query_rows,
    all_input_parcels_terminal,
    all_records_accounted_for,
    query_rows_match_unique_records,
    local_pilot_passed,
    county_permit_complete,
    recorded_at
  ) VALUES (
    'broward-permit',
    p_sample_parcels,
    p_appraisal_attempts,
    p_appraisal_resolved,
    p_jurisdiction_resolved,
    p_jurisdiction_unresolved,
    p_source_outcomes,
    p_source_unavailable_outcomes,
    p_permit_source_attempts,
    p_permit_attempted_parcels,
    p_explicit_no_permit_outcomes,
    p_source_failures,
    p_raw_permit_records,
    p_duplicate_permit_records,
    p_conflicting_permit_records,
    p_unique_permit_records,
    p_query_rows,
    p_all_input_parcels_terminal,
    p_all_records_accounted_for,
    p_query_rows_match_unique_records,
    p_local_pilot_passed,
    p_county_permit_complete,
    p_recorded_at
  )
  ON CONFLICT (pipeline_key) DO UPDATE SET
    sample_parcels = EXCLUDED.sample_parcels,
    appraisal_attempts = EXCLUDED.appraisal_attempts,
    appraisal_resolved = EXCLUDED.appraisal_resolved,
    jurisdiction_resolved = EXCLUDED.jurisdiction_resolved,
    jurisdiction_unresolved = EXCLUDED.jurisdiction_unresolved,
    source_outcomes = EXCLUDED.source_outcomes,
    source_unavailable_outcomes = EXCLUDED.source_unavailable_outcomes,
    permit_source_attempts = EXCLUDED.permit_source_attempts,
    permit_attempted_parcels = EXCLUDED.permit_attempted_parcels,
    explicit_no_permit_outcomes = EXCLUDED.explicit_no_permit_outcomes,
    source_failures = EXCLUDED.source_failures,
    raw_permit_records = EXCLUDED.raw_permit_records,
    duplicate_permit_records = EXCLUDED.duplicate_permit_records,
    conflicting_permit_records = EXCLUDED.conflicting_permit_records,
    unique_permit_records = EXCLUDED.unique_permit_records,
    query_rows = EXCLUDED.query_rows,
    all_input_parcels_terminal = EXCLUDED.all_input_parcels_terminal,
    all_records_accounted_for = EXCLUDED.all_records_accounted_for,
    query_rows_match_unique_records = EXCLUDED.query_rows_match_unique_records,
    local_pilot_passed = EXCLUDED.local_pilot_passed,
    county_permit_complete = EXCLUDED.county_permit_complete,
    recorded_at = EXCLUDED.recorded_at;

  INSERT INTO ingest_control.broward_permit_events (
    event_key,
    event_type,
    sample_parcels,
    permit_source_attempts,
    query_rows,
    local_pilot_passed,
    county_permit_complete,
    recorded_at
  ) VALUES (
    'bounded-pilot-current',
    'pilot_reconciled',
    p_sample_parcels,
    p_permit_source_attempts,
    p_query_rows,
    p_local_pilot_passed,
    p_county_permit_complete,
    p_recorded_at
  )
  ON CONFLICT (event_key) DO UPDATE SET
    event_type = EXCLUDED.event_type,
    sample_parcels = EXCLUDED.sample_parcels,
    permit_source_attempts = EXCLUDED.permit_source_attempts,
    query_rows = EXCLUDED.query_rows,
    local_pilot_passed = EXCLUDED.local_pilot_passed,
    county_permit_complete = EXCLUDED.county_permit_complete,
    recorded_at = EXCLUDED.recorded_at;
END;
$function$;

COMMENT ON FUNCTION ingest_control.record_broward_permit_pilot_status(
  integer,
  integer,
  integer,
  integer,
  integer,
  integer,
  integer,
  integer,
  integer,
  integer,
  integer,
  integer,
  integer,
  integer,
  integer,
  integer,
  boolean,
  boolean,
  boolean,
  boolean,
  boolean,
  timestamptz
) IS
  'Atomically records aggregate bounded permit pilot status and idempotent event evidence.';

COMMIT;
