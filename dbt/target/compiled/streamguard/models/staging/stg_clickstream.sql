

WITH source AS (
    SELECT * FROM STREAMGUARD_DB.RAW.clickstream_raw
    
),

cleaned AS (
    SELECT
        session_id,
        user_id,
        LOWER(page)                                             AS page,
        LOWER(action)                                           AS action,
        LOWER(COALESCE(device, 'unknown'))                      AS device,
        LOWER(COALESCE(browser, 'unknown'))                     AS browser,
        LOWER(COALESCE(referrer, 'direct'))                     AS referrer,
        COALESCE(duration_ms, 0)                                AS duration_ms,
        UPPER(COALESCE(region, 'XX'))                           AS region,
        TRY_TO_TIMESTAMP(event_time)                            AS event_time,
        ingested_at,
        source_topic,

        -- Derived
        DATE_TRUNC('hour', TRY_TO_TIMESTAMP(event_time))        AS event_hour,
        DATE_TRUNC('day',  TRY_TO_TIMESTAMP(event_time))        AS event_date,
        CASE
            WHEN action IN ('purchase', 'add_to_cart') THEN TRUE
            ELSE FALSE
        END                                                      AS is_conversion_action,
        ROUND(duration_ms / 1000.0, 2)                          AS duration_sec,

        ROW_NUMBER() OVER (
            PARTITION BY session_id
            ORDER BY ingested_at DESC
        )                                                        AS _row_num

    FROM source
    WHERE session_id IS NOT NULL
      AND user_id    IS NOT NULL
)

SELECT * EXCLUDE (_row_num)
FROM cleaned
WHERE _row_num = 1