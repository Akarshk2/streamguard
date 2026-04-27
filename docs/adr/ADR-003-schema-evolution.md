# ADR-003 — Automated Schema Evolution Handler

**Status:** Accepted
**Date:** 2025

---

## Context

Upstream Kafka producers (simulators or real source systems) may add new fields to event payloads
over time. If StreamGuard's Snowflake tables don't accommodate these new fields, valid records
get quarantined unnecessarily — increasing DLQ volume and degrading pipeline SLA.

---

## Decision

Implement an **automatic, additive-only schema evolution handler** in `dag_schema_heal`:

- Detects new fields by comparing incoming record keys against the known Snowflake table columns
- Issues `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for genuinely new additive columns
- Logs every auto-ALTER to `ALERTS.SCHEMA_CHANGES` with timestamp and batch_id
- Does **not** auto-handle destructive changes (column removal, type changes)

---

## Alternatives Considered

| Option | Reason Rejected |
|--------|----------------|
| Hard fail on schema mismatch | Causes unnecessary downtime for valid additive changes |
| Fully schemaless (VARIANT columns) | Loses type safety and analytical query performance in Snowflake |
| Manual migration only | Too slow for streaming pipelines; requires human on-call |
| Schema Registry enforcement | Valid approach but adds Confluent Schema Registry dependency; increases infra complexity |

---

## Rationale

This follows **Confluent Schema Registry's FORWARD compatibility** philosophy: producers
can add new optional fields without breaking existing consumers.

Only **additive** changes are auto-handled because:
- Adding a column is always safe — existing rows get NULL for the new column
- Removing or renaming a column could break downstream dbt models and BI dashboards
- Type changes require data validation before application

This is the same pattern used by Databricks Auto Loader (`cloudFiles.schemaEvolutionMode = "addNewColumns"`).

---

## Consequences

- **Positive:** Eliminates a large class of unnecessary quarantine events.
- **Positive:** Pipeline continues without human intervention for normal upstream evolution.
- **Negative:** Risk of accepting unintended new fields from misbehaving producers.
- **Mitigation 1:** All auto-ALTERs are logged with full audit trail in `ALERTS.SCHEMA_CHANGES`.
- **Mitigation 2:** Column name safety check (alphanumeric + underscore only) prevents SQL injection via field names.
- **Mitigation 3:** Destructive changes still hard-fail and alert the on-call engineer.
