"""
StreamGuard — DAG: Schema Heal
Triggered automatically when ML inference detects schema drift.
Compares incoming record fields to known Snowflake schema and issues ALTER TABLE
for any new additive columns. Logs all changes to ALERTS.SCHEMA_CHANGES.
"""

import logging
import os
from datetime import datetime, timedelta

import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger("streamguard.dag.schema_heal")

default_args = {
    "owner": "streamguard",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ─── EXPECTED COLUMNS ─────────────────────────────────────────────────────────
EXPECTED_COLUMNS = {
    "orders_topic": {
        "order_id", "customer_id", "product_id", "quantity", "amount",
        "currency", "status", "payment_method", "region", "event_time",
        "created_at", "ingested_at", "source_topic", "ingest_date",
    },
    "clickstream_topic": {
        "session_id", "user_id", "page", "action", "device",
        "browser", "referrer", "duration_ms", "region", "event_time",
        "ingested_at", "source_topic",
    },
    "inventory_topic": {
        "sku", "warehouse_id", "stock_level", "reserved_qty",
        "restock_flag", "restock_qty", "unit_cost", "last_movement",
        "event_time", "ingested_at", "source_topic",
    },
}

TABLE_MAP = {
    "orders_topic":      "RAW.ORDERS_RAW",
    "clickstream_topic": "RAW.CLICKSTREAM_RAW",
    "inventory_topic":   "RAW.INVENTORY_RAW",
}


def _get_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
    )


def heal_schema(**context):
    conf        = context["dag_run"].conf or {}
    topic       = conf.get("topic", "orders_topic")
    batch_id    = conf.get("batch_id", -1)
    table       = TABLE_MAP.get(topic)
    known_cols  = EXPECTED_COLUMNS.get(topic, set())

    if not table:
        logger.error(f"Unknown topic: {topic}")
        return

    conn   = _get_conn()
    cursor = conn.cursor()

    # ── Get existing Snowflake columns ────────────────────────────────────────
    cursor.execute(f"DESCRIBE TABLE {table}")
    existing_cols = {row[0].lower() for row in cursor.fetchall()}

    # ── Get incoming columns from most recent quarantine record ───────────────
    cursor.execute(f"""
        SELECT DISTINCT f.key
        FROM ALERTS.QUARANTINE_LOG q,
             LATERAL FLATTEN(input => PARSE_JSON(q.record_json)) f
        WHERE q.topic = '{topic}'
        ORDER BY 1
        LIMIT 200
    """)
    incoming_cols = {row[0].lower() for row in cursor.fetchall()} if cursor.rowcount > 0 else set()

    # ── Find truly new columns (not in known schema or Snowflake table) ───────
    all_known = known_cols | existing_cols
    new_cols  = incoming_cols - all_known

    altered = []
    for col in sorted(new_cols):
        # Safety: only add valid identifier names
        if not col.replace("_", "").isalnum():
            logger.warning(f"Skipping unsafe column name: {col}")
            continue
        sql = f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} VARCHAR(1024)"
        logger.info(f"Executing: {sql}")
        cursor.execute(sql)
        altered.append(col)

    # ── Log schema changes ────────────────────────────────────────────────────
    if altered:
        cursor.execute("""
            INSERT INTO ALERTS.SCHEMA_CHANGES
                (topic, table_name, new_columns, batch_id, detected_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (topic, table, str(altered), batch_id, datetime.utcnow().isoformat()))
        conn.commit()
        logger.info(f"Schema heal complete for {table}. Added columns: {altered}")
    else:
        logger.info(f"No new columns detected for {table}. Schema is consistent.")

    conn.close()
    return {"altered_columns": altered, "table": table}


with DAG(
    dag_id="dag_schema_heal",
    default_args=default_args,
    description="Auto-heal Snowflake schema on drift detection",
    schedule_interval=None,  # triggered externally by ML inference
    start_date=days_ago(1),
    catchup=False,
    tags=["streamguard", "schema", "healing"],
) as dag:

    PythonOperator(
        task_id="heal_schema",
        python_callable=heal_schema,
        provide_context=True,
    )
