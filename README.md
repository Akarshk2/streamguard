# StreamGuard 🛡️
### Real-Time Intelligent Data Observability Platform

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apache-spark)](https://spark.apache.org)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-7.5-black?logo=apache-kafka)](https://kafka.apache.org)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-29B5E8?logo=snowflake)](https://snowflake.com)
[![dbt](https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt)](https://getdbt.com)
[![Airflow](https://img.shields.io/badge/Airflow-2.8-017CEE?logo=apache-airflow)](https://airflow.apache.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109-009688?logo=fastapi)](https://fastapi.tiangolo.com)

---

> **Most data engineering portfolios show pipelines that move data from A to B.**
> **StreamGuard shows a pipeline that watches, understands, and heals itself.**

---

## What Is StreamGuard?

StreamGuard is a **self-healing, real-time data observability platform** built to demonstrate senior-level data engineering. It ingests live e-commerce event streams from Apache Kafka, processes them with PySpark Structured Streaming, detects anomalies using a trained Isolation Forest ML model, and automatically triggers Airflow remediation DAGs — all with zero human intervention.

### The Stack

| Layer | Technology |
|-------|-----------|
| Streaming Ingestion | Apache Kafka (3 topics + DLQ) |
| Stream Processing | PySpark Structured Streaming |
| Anomaly Detection | scikit-learn Isolation Forest |
| Auto-Healing | Apache Airflow (REST-triggered DAGs) |
| Data Warehouse | Snowflake (RAW → STAGING → MARTS) |
| Transformation | dbt Core |
| Quarantine Storage | AWS S3 |
| Observability API | FastAPI |
| Dashboard | Power BI |

---

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│               DATA SIMULATOR (Python)                      │
│     Orders · Clickstream · Inventory (8% faults)          │
└──────────────────────────┬─────────────────────────────────┘
                           │ Kafka Topics
                           ▼
┌────────────────────────────────────────────────────────────┐
│               APACHE KAFKA (3 Topics + DLQ)                │
└──────────────────────────┬─────────────────────────────────┘
                           │ PySpark Structured Streaming
                           ▼
┌────────────────────────────────────────────────────────────┐
│           PYSPARK STREAMING ENGINE                         │
│  Validate → Profile → Route (good → Snowflake RAW)        │
│                            (bad  → S3 + Kafka DLQ)        │
└──────────────────────────┬─────────────────────────────────┘
                           │ Stream profiles (near-real-time)
                           ▼
┌────────────────────────────────────────────────────────────┐
│      ML ANOMALY DETECTION (Isolation Forest)               │
│  Scores profiles → Triggers Airflow on anomaly             │
└────────────┬───────────────────────────────────────────────┘
             │ Trigger REST API
             ▼
┌────────────────────────┐   ┌──────────────────────────────┐
│  AIRFLOW AUTO-HEALING  │   │  dbt TRANSFORMATIONS         │
│  - Schema evolution    │   │  RAW → STAGING → MARTS       │
│  - DLQ reprocessing    │   │  + Data quality tests        │
│  - SLA reports         │   └──────────────┬───────────────┘
└────────────────────────┘                  │
                                            ▼
                             ┌──────────────────────────────┐
                             │  POWER BI + FastAPI API      │
                             └──────────────────────────────┘
```

---

## Seniority Signals

| What You Built | What It Shows |
|----------------|---------------|
| ML anomaly detection on infrastructure metrics | Applying ML to ops, not just business data |
| Dead Letter Queue with auto-reprocessing | Production-hardened failure handling |
| Automated schema evolution (ALTER TABLE) | Operational maturity in live environments |
| Data quality results stored as data | How elite data teams track pipeline health |
| Architecture Decision Records (ADRs) | Senior engineers explain *why*, not just *what* |
| Airflow DAGs triggered by ML inference | Platform thinking, not just pipeline thinking |
| FastAPI health endpoint | Full product ownership |

---

## Prerequisites

| Requirement | Purpose |
|-------------|---------|
| Python 3.11 | All application code |
| Docker + Docker Compose | Kafka, Zookeeper, Airflow (local) |
| Java 11 | PySpark |
| Snowflake account | Data warehouse (free trial) |
| AWS account | S3 quarantine bucket (free tier) |
| Power BI Desktop | Dashboard (Windows) |

---

## Quick Start

### 1. Clone & Configure

```bash
git clone https://github.com/yourusername/streamguard.git
cd streamguard

# Copy and fill in your credentials
cp .env.example .env
nano .env
```

### 2. Snowflake Setup

Run `infra/snowflake_ddl.sql` in your Snowflake worksheet. This creates:
- `STREAMGUARD_DB` database
- Schemas: `RAW`, `METRICS`, `ALERTS`, `STAGING`, `MARTS`
- All required tables and sequences

### 3. Start the Stack

```bash
# Start Kafka + Zookeeper + Airflow + API via Docker
docker-compose up -d

# Wait ~30 seconds, then create Kafka topics
bash kafka/topic_setup.sh

# Verify: http://localhost:8090 (Kafka UI) | http://localhost:8080 (Airflow)
```

### 4. Python Environment

```bash
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 5. Start the Data Simulator

```bash
# Generates orders, clicks, inventory with 8% fault injection
python simulator/data_simulator.py

# Or with custom settings
python simulator/data_simulator.py --fault-rate 0.12 --rate 2.0
```

### 6. Start PySpark Consumers

Open three separate terminals:

```bash
# Terminal 1
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4 \
  streaming/consumer_orders.py

# Terminal 2
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4 \
  streaming/consumer_clickstream.py

# Terminal 3
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4 \
  streaming/consumer_inventory.py
```

### 7. Train the ML Model

Wait at least 2 hours of streaming data (needs 100+ profiles), then:

```bash
python ml/train_model.py --days 1 --contamination 0.08
```

### 8. Start the Inference Engine

```bash
# Runs every 30 seconds, scores new profiles, triggers Airflow on anomalies
python ml/inference.py --interval 30
```

### 9. Run dbt Transformations

```bash
cd dbt
dbt deps
dbt run
dbt test
```

### 10. Start the FastAPI

```bash
uvicorn api.main:app --reload --port 8000
# Docs: http://localhost:8000/docs
```

### 11. Connect Power BI

1. Open Power BI Desktop → Get Data → Snowflake
2. Server: `your_account.region.snowflakecomputing.com`
3. Warehouse: `STREAMGUARD_WH`
4. Import tables from `MARTS` schema

---

## Project Structure

```
streamguard/
├── simulator/
│   ├── data_simulator.py       # Event generator (orders, clicks, inventory)
│   └── fault_injector.py       # Injects nulls, schema drift, bad values
├── kafka/
│   ├── topic_setup.sh          # Creates Kafka topics + DLQ
│   └── schemas/                # Avro schema definitions
├── streaming/
│   ├── consumer_orders.py      # PySpark orders consumer
│   ├── consumer_clickstream.py # PySpark clickstream consumer
│   ├── consumer_inventory.py   # PySpark inventory consumer
│   ├── schema_validator.py     # Validates + routes to DLQ/S3
│   ├── stream_profiler.py      # Generates statistical profiles per batch
│   └── snowflake_writer.py     # Snowflake Spark connector wrapper
├── ml/
│   ├── train_model.py          # Train Isolation Forest on profile history
│   ├── inference.py            # Score profiles, trigger Airflow on anomalies
│   └── models/                 # Serialized model artifacts
├── dbt/
│   ├── models/
│   │   ├── staging/            # stg_orders, stg_clickstream, stg_inventory...
│   │   └── marts/              # mart_pipeline_health, mart_stream_overview...
│   └── tests/schema.yml        # dbt data quality tests
├── airflow/dags/
│   ├── dag_dbt_run.py          # Scheduled dbt runs (every 30 min)
│   ├── dag_schema_heal.py      # Auto ALTER TABLE on schema drift
│   ├── dag_dlq_reprocess.py    # Reprocess + fix DLQ records
│   ├── dag_model_retrain.py    # Weekly ML model retrain
│   └── dag_sla_report.py       # Daily SLA email report
├── api/
│   └── main.py                 # FastAPI observability endpoints
├── infra/
│   └── snowflake_ddl.sql       # All Snowflake table definitions
├── docs/adr/
│   ├── ADR-001-isolation-forest.md
│   ├── ADR-002-dlq-pattern.md
│   └── ADR-003-schema-evolution.md
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /pipeline/status` | Today's SLA per topic |
| `GET /pipeline/anomalies` | Recent ML-detected anomalies |
| `GET /pipeline/history?days=7` | Historical SLA trend |
| `GET /stream/overview` | Business metrics (GMV, sessions, inventory) |
| `GET /stream/profiles/latest` | Raw ML feature profiles |
| `GET /schema/changes` | Auto-applied ALTER TABLE history |
| `GET /summary` | One-shot system health summary |

---

## Architecture Decisions

See `docs/adr/` for detailed reasoning behind key choices:

- [ADR-001](docs/adr/ADR-001-isolation-forest.md) — Why Isolation Forest over Z-score, DBSCAN, or Autoencoders
- [ADR-002](docs/adr/ADR-002-dlq-pattern.md) — Dead Letter Queue pattern design
- [ADR-003](docs/adr/ADR-003-schema-evolution.md) — Automated additive schema evolution

---

## Built By

**Akarsh** — Senior Data Engineer  
Stack: PySpark · dbt · Snowflake · Airflow · Kafka · AWS · Python · Power BI
