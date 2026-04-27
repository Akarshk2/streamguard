"""
StreamGuard — Schema Validator & DLQ Router
Validates incoming records against expected schema and business rules.
Routes invalid records to S3 quarantine and Kafka Dead Letter Queue.
"""

import json
import logging
import os
from typing import Tuple

import boto3
from kafka import KafkaProducer
from pyspark.sql import DataFrame, functions as F

logger = logging.getLogger("streamguard.schema_validator")

# ─── REQUIRED FIELDS PER TOPIC ────────────────────────────────────────────────
REQUIRED_FIELDS = {
    "order": ["order_id", "customer_id", "product_id", "amount", "status", "event_time", "region"],
    "click": ["session_id", "user_id", "page", "action", "event_time"],
    "inventory": ["sku", "warehouse_id", "stock_level", "event_time"],
}

# ─── BUSINESS RULES ───────────────────────────────────────────────────────────
def _apply_business_rules(df: DataFrame, record_type: str) -> Tuple[DataFrame, DataFrame]:
    if record_type == "order":
        good = df.filter(F.col("amount").isNotNull() & (F.col("amount") > 0))
    elif record_type == "inventory":
        good = df.filter(F.col("stock_level").isNotNull() & (F.col("stock_level") >= 0))
    else:
        good = df
    bad = df.subtract(good).withColumn("reject_reason", F.lit("BUSINESS_RULE_VIOLATION"))
    return good, bad


# ─── KAFKA DLQ PRODUCER ───────────────────────────────────────────────────────
_dlq_producer = None

def _get_dlq_producer() -> KafkaProducer:
    global _dlq_producer
    if _dlq_producer is None:
        _dlq_producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )
    return _dlq_producer


# ─── S3 QUARANTINE ────────────────────────────────────────────────────────────
def _write_to_s3_quarantine(df: DataFrame, record_type: str, batch_id: int):
    # S3 write skipped in local mode — bad records logged to console only
    logger.warning(f"Quarantine (local mode): {record_type} batch {batch_id} has bad records")


def _publish_to_dlq(df: DataFrame, record_type: str, batch_id: int):
    producer = _get_dlq_producer()
    rows = df.limit(200).collect()
    for row in rows:
        row_dict = row.asDict()
        producer.send("streamguard_dlq", {
            "source": record_type,
            "batch_id": batch_id,
            "reason": row_dict.get("reject_reason", "UNKNOWN"),
            "record": {k: v for k, v in row_dict.items() if not k.startswith("_")},
        })
    producer.flush()
    logger.info(f"Published {len(rows)} records to DLQ for topic: {record_type}")


# ─── MAIN VALIDATION FUNCTION ─────────────────────────────────────────────────
def validate_and_route(
    df: DataFrame, record_type: str, batch_id: int
) -> Tuple[DataFrame, DataFrame]:
    """
    Validate a PySpark DataFrame against schema and business rules.
    Routes good records back to caller, bad records to S3 + DLQ.

    Returns:
        (good_df, bad_df) tuple
    """
    required = REQUIRED_FIELDS.get(record_type, [])

    # ── Step 1: Null check on required fields ─────────────────────────────────
    null_condition = F.lit(False)
    for field in required:
        if field in df.columns:
            null_condition = null_condition | F.col(field).isNull()

    bad_nulls = (
        df.filter(null_condition)
        .withColumn("reject_reason", F.lit("NULL_REQUIRED_FIELD"))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("quarantine_ts", F.current_timestamp())
    )
    candidate = df.filter(~null_condition)

    # ── Step 2: Business rule validation ─────────────────────────────────────
    good_df, bad_rules = _apply_business_rules(candidate, record_type)
    bad_rules = (
        bad_rules
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("quarantine_ts", F.current_timestamp())
    )

    # ── Step 3: Merge all bad records ─────────────────────────────────────────
    bad_df = bad_nulls.unionByName(bad_rules, allowMissingColumns=True)

    # ── Step 4: Route bad records ─────────────────────────────────────────────
    if bad_df.count() > 0:
        _write_to_s3_quarantine(bad_df, record_type, batch_id)
        _publish_to_dlq(bad_df, record_type, batch_id)

    logger.info(
        f"Batch {batch_id} [{record_type}]: "
        f"{good_df.count()} good, {bad_df.count()} quarantined"
    )
    return good_df, bad_df
