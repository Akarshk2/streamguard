"""
StreamGuard — Fault Injector
Simulates real-world data quality problems in streaming records.
Designed to test StreamGuard's detection and healing pipeline.
"""

import random
import logging

logger = logging.getLogger(__name__)


class FaultInjector:
    """
    Injects configurable faults into data records to simulate
    production data quality issues:
      - Null required fields
      - Schema drift (unexpected new columns)
      - Business rule violations (negative amounts)
      - Late-arriving / duplicate records
    """

    FAULT_TYPES = ["null_field", "schema_drift", "neg_value", "duplicate_key", "none"]

    def __init__(self, fault_rate: float = 0.08):
        """
        Args:
            fault_rate: Fraction of records that will have faults injected (0.0 – 1.0)
        """
        self.fault_rate = fault_rate
        self._fault_counts = {f: 0 for f in self.FAULT_TYPES}

    def maybe_inject(self, record: dict, record_type: str) -> dict:
        """
        Randomly decide whether to inject a fault into this record.
        Returns the (possibly modified) record.
        """
        if random.random() > self.fault_rate:
            return record

        fault = random.choice(self.FAULT_TYPES[:-1])  # exclude 'none'

        if fault == "null_field":
            record = self._inject_null(record)

        elif fault == "schema_drift":
            record = self._inject_schema_drift(record)

        elif fault == "neg_value" and record_type == "order":
            record = self._inject_negative_amount(record)

        elif fault == "duplicate_key":
            record = self._inject_duplicate_key(record, record_type)

        record["_fault_injected"] = fault
        self._fault_counts[fault] += 1
        logger.debug(f"Fault injected: {fault} into {record_type}")
        return record

    def _inject_null(self, record: dict) -> dict:
        key = random.choice([k for k in record.keys() if not k.startswith("_")])
        record[key] = None
        return record

    def _inject_schema_drift(self, record: dict) -> dict:
        drift_fields = {
            "unexpected_field_xyz": "DRIFT_INJECTED",
            "new_column_v2": random.randint(1, 100),
            "extra_metadata": {"source": "legacy_system"},
        }
        field = random.choice(list(drift_fields.keys()))
        record[field] = drift_fields[field]
        return record

    def _inject_negative_amount(self, record: dict) -> dict:
        if "amount" in record:
            record["amount"] = -abs(record["amount"])
        return record

    def _inject_duplicate_key(self, record: dict, record_type: str) -> dict:
        key_field = {
            "order": "order_id",
            "click": "session_id",
            "inventory": "sku",
        }.get(record_type)
        if key_field and key_field in record:
            record[key_field] = "DUPLICATE_KEY_TEST_001"
        return record

    def stats(self) -> dict:
        return self._fault_counts
