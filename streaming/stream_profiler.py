"""
StreamGuard — Stream Profiler
Generates statistical fingerprints of each PySpark micro-batch.
Writes directly to Snowflake via Python connector.
"""

import logging
import os
from datetime import datetime

import snowflake.connector
from dotenv import load_dotenv
from pyspark.sql import DataFrame, functions as F

load_dotenv("C:/streamguard/.env")

logger = logging.getLogger("streamguard.stream_profiler")

BASELINE_SCHEMA_HASHES = {
    "orders_topic":      None,
    "clickstream_topic": None,
    "inventory_topic":   None,
}


def _get_sf_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        login_timeout=30,
    )


def generate_profile(df: DataFrame, topic: str, batch_id: int) -> None:
    """
    Compute a statistical profile for a single micro-batch and write
    directly to Snowflake via Python connector.
    """
    try:
        total_count = df.count()
        if total_count == 0:
            logger.warning(f"Empty batch {batch_id} for topic {topic} — skipping profile")
            return

        # Null rates per column
        null_rates = {}
        for col_name in df.columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            null_rates[col_name] = round(null_count / total_count, 6)

        avg_null_rate = round(sum(null_rates.values()) / len(null_rates), 6) if null_rates else 0.0
        max_null_rate = round(max(null_rates.values()), 6) if null_rates else 0.0

        # Schema fingerprint
        schema_hash = str(abs(hash(tuple(sorted(df.columns)))))

        if BASELINE_SCHEMA_HASHES.get(topic) is None:
            BASELINE_SCHEMA_HASHES[topic] = schema_hash
            logger.info(f"Baseline schema hash set for {topic}: {schema_hash}")

        expected_hash = BASELINE_SCHEMA_HASHES.get(topic)

        # Write directly to Snowflake
        conn = _get_sf_conn()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO METRICS.STREAM_PROFILES
                    (topic, batch_id, record_count, null_rate_avg, null_rate_max,
                     schema_hash, column_count, expected_schema_hash,
                     null_rates_json, profile_ts)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                topic, batch_id, total_count,
                avg_null_rate, max_null_rate,
                schema_hash, len(df.columns), expected_hash,
                str(null_rates), datetime.utcnow().isoformat(),
            ))
            conn.commit()
            logger.info(
                f"Profile written [{topic}] batch={batch_id} "
                f"count={total_count} null_avg={avg_null_rate:.4f} "
                f"schema_match={schema_hash == expected_hash}"
            )
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Profile generation failed for {topic} batch {batch_id}: {e}")
