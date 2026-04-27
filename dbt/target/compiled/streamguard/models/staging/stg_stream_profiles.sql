

WITH source AS (
    SELECT * FROM STREAMGUARD_DB.METRICS.stream_profiles
    
)

SELECT
    profile_id,
    topic,
    batch_id,
    record_count,
    null_rate_avg,
    null_rate_max,
    schema_hash,
    column_count,
    expected_schema_hash,
    null_rates_json,
    TRY_TO_TIMESTAMP(profile_ts)                        AS profile_ts,
    created_at,

    -- Derived
    schema_hash != COALESCE(expected_schema_hash, schema_hash) AS is_schema_drift,
    DATE_TRUNC('hour', TRY_TO_TIMESTAMP(profile_ts))    AS profile_hour,
    EXTRACT(HOUR FROM TRY_TO_TIMESTAMP(profile_ts))     AS hour_of_day

FROM source
WHERE profile_id IS NOT NULL