
  
    

create or replace transient table STREAMGUARD_DB.STAGING_STAGING.stg_orders
    
    
    
    as (

WITH source AS (
    SELECT * FROM STREAMGUARD_DB.RAW.orders_raw
    
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        COALESCE(quantity, 1)                                   AS quantity,
        ROUND(amount::FLOAT, 2)                                 AS amount,
        COALESCE(UPPER(currency), 'INR')                        AS currency,
        UPPER(COALESCE(status, 'UNKNOWN'))                      AS status,
        LOWER(COALESCE(payment_method, 'unknown'))              AS payment_method,
        UPPER(COALESCE(region, 'XX'))                           AS region,
        TRY_TO_TIMESTAMP(event_time)                            AS event_time,
        TRY_TO_TIMESTAMP(created_at)                            AS created_at,
        ingested_at,
        source_topic,
        ingest_date,

        -- Derived columns
        DATE_TRUNC('hour', TRY_TO_TIMESTAMP(event_time))        AS event_hour,
        DATE_TRUNC('day',  TRY_TO_TIMESTAMP(event_time))        AS event_date,
        EXTRACT(HOUR FROM TRY_TO_TIMESTAMP(event_time))         AS hour_of_day,
        amount * quantity                                        AS total_value,

        -- Dedup rank (latest ingestion wins)
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY ingested_at DESC
        )                                                        AS _row_num

    FROM source
    WHERE order_id IS NOT NULL
      AND amount    IS NOT NULL
      AND amount    > 0
)

SELECT * EXCLUDE (_row_num)
FROM cleaned
WHERE _row_num = 1
    )
;


  