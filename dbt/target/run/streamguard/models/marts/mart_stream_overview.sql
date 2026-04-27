
  
    

create or replace transient table STREAMGUARD_DB.STAGING_MARTS.mart_stream_overview
    
    
    
    as (

WITH orders AS (
    SELECT
        event_date,
        COUNT(DISTINCT order_id)                        AS total_orders,
        COUNT(DISTINCT customer_id)                     AS unique_customers,
        SUM(total_value)                                AS total_gmv,
        AVG(amount)                                     AS avg_order_value,
        ROUND(
            SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0), 2
        )                                               AS delivery_rate_pct,
        ROUND(
            SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0), 2
        )                                               AS cancellation_rate_pct
    FROM STREAMGUARD_DB.STAGING_STAGING.stg_orders
    GROUP BY 1
),

clicks AS (
    SELECT
        event_date,
        COUNT(DISTINCT session_id)                      AS total_sessions,
        COUNT(DISTINCT user_id)                         AS unique_users,
        SUM(CASE WHEN is_conversion_action THEN 1 ELSE 0 END) AS conversion_events,
        AVG(duration_sec)                               AS avg_session_sec,
        ROUND(
            SUM(CASE WHEN is_conversion_action THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0), 2
        )                                               AS conversion_rate_pct
    FROM STREAMGUARD_DB.STAGING_STAGING.stg_clickstream
    GROUP BY 1
),

inventory AS (
    SELECT
        event_date,
        COUNT(DISTINCT sku)                             AS unique_skus_tracked,
        SUM(CASE WHEN stock_status = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_stock_skus,
        SUM(CASE WHEN restock_flag THEN 1 ELSE 0 END)  AS restock_triggered,
        AVG(available_qty)                              AS avg_available_qty
    FROM STREAMGUARD_DB.STAGING_STAGING.stg_inventory
    GROUP BY 1
)

SELECT
    COALESCE(o.event_date, c.event_date, i.event_date) AS stat_date,
    -- Orders
    COALESCE(o.total_orders, 0)                         AS total_orders,
    COALESCE(o.unique_customers, 0)                     AS unique_customers,
    ROUND(COALESCE(o.total_gmv, 0), 2)                  AS total_gmv,
    ROUND(COALESCE(o.avg_order_value, 0), 2)            AS avg_order_value,
    COALESCE(o.delivery_rate_pct, 0)                    AS delivery_rate_pct,
    COALESCE(o.cancellation_rate_pct, 0)                AS cancellation_rate_pct,
    -- Clicks
    COALESCE(c.total_sessions, 0)                       AS total_sessions,
    COALESCE(c.unique_users, 0)                         AS unique_users,
    COALESCE(c.conversion_rate_pct, 0)                  AS conversion_rate_pct,
    ROUND(COALESCE(c.avg_session_sec, 0), 1)            AS avg_session_sec,
    -- Inventory
    COALESCE(i.unique_skus_tracked, 0)                  AS unique_skus_tracked,
    COALESCE(i.critical_stock_skus, 0)                  AS critical_stock_skus,
    COALESCE(i.restock_triggered, 0)                    AS restock_triggered,
    CURRENT_TIMESTAMP()                                  AS dbt_updated_at

FROM orders o
FULL OUTER JOIN clicks     c ON o.event_date = c.event_date
FULL OUTER JOIN inventory  i ON o.event_date = i.event_date
ORDER BY stat_date DESC
    )
;


  