{{ config(materialized = 'table') }}

WITH profiles AS (
    SELECT * FROM {{ ref('stg_stream_profiles') }}
),

anomalies AS (
    SELECT * FROM {{ ref('stg_anomaly_log') }}
),

daily_profiles AS (
    SELECT
        topic,
        DATE_TRUNC('day', profile_ts)               AS stat_date,
        COUNT(*)                                     AS total_batches,
        SUM(record_count)                            AS total_records,
        AVG(record_count)                            AS avg_batch_size,
        MIN(record_count)                            AS min_batch_size,
        MAX(record_count)                            AS max_batch_size,
        AVG(null_rate_avg)                           AS avg_null_rate,
        MAX(null_rate_max)                           AS max_null_rate,
        SUM(CASE WHEN is_schema_drift THEN 1 ELSE 0 END) AS schema_drift_events
    FROM profiles
    GROUP BY 1, 2
),

daily_anomalies AS (
    SELECT
        topic,
        scored_date                                  AS stat_date,
        COUNT(*)                                     AS anomaly_count,
        AVG(anomaly_score)                           AS avg_anomaly_score,
        MIN(anomaly_score)                           AS min_anomaly_score,
        SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
        SUM(CASE WHEN severity = 'HIGH'     THEN 1 ELSE 0 END) AS high_count,
        SUM(CASE WHEN severity = 'MEDIUM'   THEN 1 ELSE 0 END) AS medium_count,
        SUM(CASE WHEN is_schema_drift       THEN 1 ELSE 0 END) AS schema_anomalies
    FROM anomalies
    GROUP BY 1, 2
)

SELECT
    p.topic,
    p.stat_date,
    p.total_batches,
    p.total_records,
    ROUND(p.avg_batch_size, 0)                          AS avg_batch_size,
    p.min_batch_size,
    p.max_batch_size,
    ROUND(p.avg_null_rate * 100, 2)                     AS avg_null_pct,
    ROUND(p.max_null_rate * 100, 2)                     AS max_null_pct,
    p.schema_drift_events,
    COALESCE(a.anomaly_count, 0)                        AS anomaly_count,
    COALESCE(a.critical_count, 0)                       AS critical_anomalies,
    COALESCE(a.high_count, 0)                           AS high_anomalies,
    COALESCE(a.medium_count, 0)                         AS medium_anomalies,
    COALESCE(a.schema_anomalies, 0)                     AS schema_anomalies,
    ROUND(a.avg_anomaly_score, 6)                       AS avg_anomaly_score,

    -- SLA: % of batches without anomaly
    ROUND(
        (1 - COALESCE(a.anomaly_count, 0) / NULLIF(p.total_batches, 0)) * 100,
        2
    )                                                    AS pipeline_sla_pct,

    CURRENT_TIMESTAMP()                                  AS dbt_updated_at

FROM daily_profiles p
LEFT JOIN daily_anomalies a
    ON p.topic = a.topic AND p.stat_date = a.stat_date
ORDER BY p.stat_date DESC, p.topic
