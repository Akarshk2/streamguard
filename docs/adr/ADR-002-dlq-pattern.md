# ADR-002 — Dead Letter Queue Pattern for Failed Records

**Status:** Accepted
**Date:** 2025

---

## Context

PySpark streaming consumers receive records that fail schema validation or business rule checks.
We need a strategy for handling these failures that:

- Does not block the main processing stream
- Preserves failed records for diagnosis and reprocessing
- Enables automated recovery where possible
- Provides operational visibility into failure rates

---

## Decision

Implement a **two-tier Dead Letter Queue** pattern:

1. **Kafka DLQ topic** (`streamguard_dlq`) — captures all rejected records for automated reprocessing
2. **S3 Quarantine** — Parquet files partitioned by topic/batch for offline analysis and audit

---

## Alternatives Considered

| Option | Reason Rejected |
|--------|----------------|
| Drop bad records silently | Data loss; impossible to diagnose issues after the fact |
| Retry immediately in-stream | Blocks throughput; likely to fail again with same data |
| Write bad records to same Snowflake table with error flag | Pollutes analytical layer; complicates downstream queries |
| Synchronous human review | Too slow; defeats the purpose of streaming |

---

## Rationale

The two-tier approach separates concerns:

- **Kafka DLQ** is for automated reprocessing. `dag_dlq_reprocess` polls it on trigger and attempts programmatic fixes (fill nulls, correct negative amounts). Re-publishes fixable records to original topic.
- **S3 Quarantine** is for human analysis and audit. Parquet files are queryable via Athena or Spark for pattern analysis. Retained for 90 days.

This mirrors production patterns used by Uber, Airbnb, and LinkedIn for streaming data reliability.

---

## Consequences

- **Positive:** Zero data loss — every record is preserved somewhere.
- **Positive:** Stream is never blocked by bad records.
- **Positive:** Automated healing handles the majority of fixable cases (null fills, sign corrections).
- **Negative:** DLQ itself is a component that needs monitoring (topic lag, consumer health).
- **Negative:** Increases operational complexity by two components (DLQ topic + S3 bucket).
- **Mitigation:** DLQ volume is exposed in the FastAPI `/pipeline/anomalies` endpoint and Power BI dashboard.
