"""
StreamGuard — DAG: DLQ Reprocessor
Triggered by ML anomaly detection on volume drops or high null rates.
Reads recoverable records from the Kafka Dead Letter Queue,
attempts auto-fix, and re-publishes to the original topic.
"""

import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger("streamguard.dag.dlq_reprocess")

default_args = {
    "owner": "streamguard",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# ─── DEFAULT FILL VALUES ──────────────────────────────────────────────────────
NULL_DEFAULTS = {
    "order": {
        "status": "unknown",
        "currency": "INR",
        "region": "XX",
        "payment_method": "unknown",
        "quantity": 1,
    },
    "click": {
        "device": "unknown",
        "browser": "unknown",
        "referrer": "unknown",
        "action": "view",
    },
    "inventory": {
        "last_movement": "unknown",
        "restock_flag": False,
        "restock_qty": 0,
    },
}


def _fix_null_fields(record: dict, record_type: str) -> dict:
    defaults = NULL_DEFAULTS.get(record_type, {})
    for key, default_val in defaults.items():
        if record.get(key) is None:
            record[key] = default_val
    return record


def _fix_business_violation(record: dict, record_type: str) -> dict:
    if record_type == "order":
        if record.get("amount") is not None and record["amount"] < 0:
            record["amount"] = abs(record["amount"])
    if record_type == "inventory":
        if record.get("stock_level") is not None and record["stock_level"] < 0:
            record["stock_level"] = 0
    return record


def reprocess_dlq(**context):
    from kafka import KafkaConsumer, KafkaProducer

    conf        = context["dag_run"].conf or {}
    topic       = conf.get("topic", "orders_topic")
    max_msgs    = conf.get("max_messages", 500)
    record_type = topic.replace("_topic", "").replace("stream_", "")

    BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    consumer = KafkaConsumer(
        "streamguard_dlq",
        bootstrap_servers=BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=15000,
        group_id=f"streamguard_dlq_reprocessor_{int(datetime.utcnow().timestamp())}",
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
    )

    reprocessed, skipped, errors = 0, 0, 0

    for msg in consumer:
        if reprocessed >= max_msgs:
            break

        try:
            payload     = msg.value
            source      = payload.get("source", "")
            reason      = payload.get("reason", "UNKNOWN")
            record      = payload.get("record", {})

            # Only process records from the specified topic
            if source != record_type:
                continue

            # Remove internal fault markers
            record.pop("_fault_injected", None)
            record.pop("reject_reason", None)
            record.pop("batch_id", None)
            record.pop("quarantine_ts", None)

            # Attempt auto-fix
            if reason == "NULL_REQUIRED_FIELD":
                record = _fix_null_fields(record, record_type)
            elif reason == "BUSINESS_RULE_VIOLATION":
                record = _fix_business_violation(record, record_type)
            else:
                logger.debug(f"Skipping unfixable record reason: {reason}")
                skipped += 1
                continue

            # Mark as reprocessed
            record["_reprocessed_at"] = datetime.utcnow().isoformat()
            record["_original_reason"] = reason

            producer.send(topic, value=record)
            reprocessed += 1

        except Exception as e:
            logger.error(f"Error reprocessing DLQ record: {e}")
            errors += 1

    producer.flush()
    consumer.close()

    result = {
        "topic": topic,
        "reprocessed": reprocessed,
        "skipped": skipped,
        "errors": errors,
    }
    logger.info(f"DLQ reprocess complete: {result}")
    return result


with DAG(
    dag_id="dag_dlq_reprocess",
    default_args=default_args,
    description="Reprocess Dead Letter Queue records",
    schedule_interval=None,  # triggered by ML anomaly detection
    start_date=days_ago(1),
    catchup=False,
    tags=["streamguard", "dlq", "healing"],
) as dag:

    PythonOperator(
        task_id="reprocess_dlq",
        python_callable=reprocess_dlq,
        provide_context=True,
    )
