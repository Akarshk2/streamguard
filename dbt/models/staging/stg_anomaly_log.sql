{{
  config(materialized = 'incremental', unique_key = 'anomaly_id')
}}

WITH source AS (
    SELECT * FROM {{ source('streamguard_alerts', 'anomaly_log') }}
    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
)

SELECT
    anomaly_id,
    profile_id,
    topic,
    batch_id,
    ROUND(anomaly_score, 6)                             AS anomaly_score,
    severity,
    is_schema_drift,
    TRY_TO_TIMESTAMP(scored_at)                         AS scored_at,
    created_at,

    -- Derived
    DATE_TRUNC('hour', TRY_TO_TIMESTAMP(scored_at))     AS scored_hour,
    DATE_TRUNC('day',  TRY_TO_TIMESTAMP(scored_at))     AS scored_date,
    CASE severity
        WHEN 'CRITICAL' THEN 4
        WHEN 'HIGH'     THEN 3
        WHEN 'MEDIUM'   THEN 2
        ELSE 1
    END                                                  AS severity_rank

FROM source
WHERE anomaly_id IS NOT NULL
