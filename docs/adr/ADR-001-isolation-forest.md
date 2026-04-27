# ADR-001 — Use Isolation Forest for Stream Anomaly Detection

**Status:** Accepted
**Date:** 2025
**Author:** Akarsh

---

## Context

StreamGuard needs to detect anomalies in stream profile metrics (record counts, null rates,
schema hashes) in near-real-time without labeled training data. The system must:

- Detect volume drops, null spikes, schema drift, and distributional shifts simultaneously
- Run inference in under 1 second per batch profile
- Operate without human-labeled anomaly examples (unsupervised)
- Be retrain-friendly as pipeline traffic patterns evolve

---

## Decision

We use **scikit-learn's Isolation Forest** as the anomaly scoring model.

---

## Alternatives Considered

| Option | Reason Rejected |
|--------|----------------|
| Z-score thresholds (per metric) | Catches only single-dimension anomalies; misses correlated patterns (e.g. low volume AND high nulls simultaneously) |
| DBSCAN | Requires careful epsilon/min_samples tuning; slow on streaming contexts; poor at multi-dimensional drift |
| LSTM Autoencoder | Requires more training data; complex to deploy and retrain; overkill for profile vectors of ~6 features |
| Prophet | Designed for time-series forecasting, not anomaly scoring of feature vectors |
| Rule-based alerts | Brittle; requires manual threshold maintenance per topic per metric |

---

## Rationale

Isolation Forest fits this use case because:

1. **Unsupervised** — no labeled anomaly data needed. We learn "normal" from 30 days of clean profiles.
2. **Multi-dimensional** — scores the full feature vector (count, null rate, schema, column count, time-of-day) in a single model call.
3. **Fast inference** — decision_function() on a 6-feature vector takes < 1ms.
4. **Interpretable contamination parameter** — setting `contamination=0.05` explicitly models "I expect 5% anomalies in training data."
5. **Retrain-friendly** — weekly retrain on 30-day rolling window keeps model current with traffic patterns.

---

## Consequences

- **Positive:** Catches correlated anomalies that single-metric rules miss.
- **Positive:** Zero labeling effort required.
- **Negative:** Model is less interpretable than Z-score — harder to explain *why* a batch was flagged.
- **Negative:** Requires at least ~100 historical profiles to train meaningfully. Run simulator for 2+ hours before training.
- **Mitigation:** Weekly `dag_model_retrain` DAG automates model currency. Threshold is logged in `model_meta.json` for auditability.

---

## Threshold Design

Isolation Forest `decision_function()` returns negative scores for anomalies:

| Score Range | Label | Action |
|------------|-------|--------|
| >= -0.15 | NORMAL | No action |
| -0.15 to -0.25 | MEDIUM | Trigger DLQ reprocess DAG |
| -0.25 to -0.40 | HIGH | Trigger DLQ reprocess DAG + alert |
| < -0.40 | CRITICAL | Trigger schema heal + DLQ + PagerDuty alert |
