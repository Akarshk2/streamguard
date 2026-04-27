

WITH base AS (
    SELECT * FROM STREAMGUARD_DB.STAGING_STAGING.stg_anomaly_log
)

SELECT
    scored_date                                         AS stat_date,
    topic,
    severity,
    COUNT(*)                                            AS anomaly_count,
    ROUND(AVG(anomaly_score), 6)                        AS avg_score,
    ROUND(MIN(anomaly_score), 6)                        AS min_score,
    SUM(CASE WHEN is_schema_drift THEN 1 ELSE 0 END)   AS schema_drift_count,
    COUNT(DISTINCT batch_id)                            AS affected_batches,
    MIN(scored_at)                                      AS first_seen,
    MAX(scored_at)                                      AS last_seen,
    CURRENT_TIMESTAMP()                                 AS dbt_updated_at
FROM base
GROUP BY 1, 2, 3
ORDER BY stat_date DESC, topic, severity