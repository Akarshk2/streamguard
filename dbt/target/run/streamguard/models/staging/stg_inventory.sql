
  
    

create or replace transient table STREAMGUARD_DB.STAGING_STAGING.stg_inventory
    
    
    
    as (

WITH source AS (
    SELECT * FROM STREAMGUARD_DB.RAW.inventory_raw
    
),

cleaned AS (
    SELECT
        sku,
        warehouse_id,
        GREATEST(COALESCE(stock_level, 0), 0)                   AS stock_level,
        COALESCE(reserved_qty, 0)                               AS reserved_qty,
        COALESCE(restock_flag, FALSE)                           AS restock_flag,
        COALESCE(restock_qty, 0)                                AS restock_qty,
        ROUND(COALESCE(unit_cost, 0), 2)                        AS unit_cost,
        LOWER(COALESCE(last_movement, 'unknown'))               AS last_movement,
        TRY_TO_TIMESTAMP(event_time)                            AS event_time,
        ingested_at,
        source_topic,

        -- Derived
        stock_level - COALESCE(reserved_qty, 0)                 AS available_qty,
        CASE
            WHEN stock_level < 50  THEN 'CRITICAL'
            WHEN stock_level < 200 THEN 'LOW'
            WHEN stock_level < 500 THEN 'MEDIUM'
            ELSE 'HEALTHY'
        END                                                      AS stock_status,
        DATE_TRUNC('day', TRY_TO_TIMESTAMP(event_time))         AS event_date

    FROM source
    WHERE sku          IS NOT NULL
      AND warehouse_id IS NOT NULL
      AND stock_level  IS NOT NULL
      AND stock_level  >= 0
)

SELECT * FROM cleaned
    )
;


  