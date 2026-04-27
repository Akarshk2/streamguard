"""
StreamGuard — FastAPI Health & Observability API
Exposes pipeline health, anomaly alerts, and stream stats as REST endpoints.

Usage:
    uvicorn api.main:app --reload --port 8000
    # Docs: http://localhost:8000/docs
"""

import os
from datetime import datetime
from typing import List, Optional

import snowflake.connector
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv("C:/streamguard/.env")

app = FastAPI(
    title="StreamGuard Observability API",
    description="Real-time pipeline health and anomaly data for StreamGuard",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ─── RESPONSE MODELS ──────────────────────────────────────────────────────────
class TopicHealth(BaseModel):
    topic: str
    total_records: int
    anomaly_count: int
    critical_anomalies: int
    pipeline_sla_pct: float
    avg_null_pct: float
    schema_drift_events: int


class AnomalyRecord(BaseModel):
    anomaly_id: int
    topic: str
    severity: str
    anomaly_score: float
    is_schema_drift: bool
    scored_at: str


class StreamStat(BaseModel):
    stat_date: str
    total_orders: int
    total_sessions: int
    total_gmv: float
    conversion_rate_pct: float
    critical_stock_skus: int


# ─── SNOWFLAKE CONNECTION ─────────────────────────────────────────────────────
def get_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        login_timeout=15,
    )


def query_sf(sql: str) -> list:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


# ─── ROUTES ──────────────────────────────────────────────────────────────────

@app.get("/", tags=["Meta"])
def root():
    return {
        "name":    "StreamGuard Observability API",
        "version": "1.0.0",
        "status":  "running",
        "ts":      datetime.utcnow().isoformat(),
        "docs":    "/docs",
    }


@app.get("/health", tags=["Meta"])
def health_check():
    return {"status": "ok", "ts": datetime.utcnow().isoformat()}


@app.get("/pipeline/status", tags=["Pipeline"], response_model=List[TopicHealth])
def pipeline_status(date: Optional[str] = Query(None, description="YYYY-MM-DD, defaults to today")):
    stat_date = date or datetime.utcnow().strftime("%Y-%m-%d")
    rows = query_sf(f"""
        SELECT topic, total_records, anomaly_count, critical_anomalies,
               pipeline_sla_pct, avg_null_pct, schema_drift_events
        FROM MARTS.MART_PIPELINE_HEALTH
        WHERE stat_date = '{stat_date}'
        ORDER BY topic
    """)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No pipeline data for {stat_date}")
    return rows


@app.get("/pipeline/anomalies", tags=["Anomalies"], response_model=List[AnomalyRecord])
def recent_anomalies(
    hours:    int = Query(24,    description="Lookback window in hours"),
    severity: str = Query("ALL", description="ALL | MEDIUM | HIGH | CRITICAL"),
    topic:    str = Query("ALL", description="Filter by topic or ALL"),
    limit:    int = Query(100,   description="Max records to return"),
):
    where = [f"scored_at >= DATEADD('hour', -{hours}, CURRENT_TIMESTAMP)"]
    if severity != "ALL":
        where.append(f"severity = '{severity.upper()}'")
    if topic != "ALL":
        where.append(f"topic = '{topic}'")

    rows = query_sf(f"""
        SELECT anomaly_id, topic, severity, anomaly_score,
               is_schema_drift, scored_at::VARCHAR AS scored_at
        FROM ALERTS.ANOMALY_LOG
        WHERE {' AND '.join(where)}
        ORDER BY scored_at DESC
        LIMIT {limit}
    """)
    return rows


@app.get("/pipeline/history", tags=["Pipeline"])
def pipeline_history(days: int = Query(7, description="Number of days of history")):
    rows = query_sf(f"""
        SELECT topic, stat_date::VARCHAR AS stat_date,
               total_records, anomaly_count, pipeline_sla_pct,
               avg_null_pct, schema_drift_events
        FROM MARTS.MART_PIPELINE_HEALTH
        WHERE stat_date >= DATEADD('day', -{days}, CURRENT_DATE)
        ORDER BY stat_date DESC, topic
    """)
    return {"days": days, "records": len(rows), "data": rows}


@app.get("/stream/overview", tags=["Business"], response_model=List[StreamStat])
def stream_overview(days: int = Query(7, description="Number of days")):
    rows = query_sf(f"""
        SELECT stat_date::VARCHAR AS stat_date,
               total_orders, total_sessions, total_gmv,
               conversion_rate_pct, critical_stock_skus
        FROM MARTS.MART_STREAM_OVERVIEW
        WHERE stat_date >= DATEADD('day', -{days}, CURRENT_DATE)
        ORDER BY stat_date DESC
    """)
    return rows


@app.get("/stream/profiles/latest", tags=["ML"])
def latest_profiles(limit: int = Query(20, description="Number of recent profiles")):
    rows = query_sf(f"""
        SELECT profile_id, topic, batch_id, record_count,
               ROUND(null_rate_avg * 100, 2) AS null_pct,
               column_count, is_schema_drift,
               profile_ts::VARCHAR AS profile_ts
        FROM STAGING.STG_STREAM_PROFILES
        ORDER BY profile_ts DESC
        LIMIT {limit}
    """)
    return {"count": len(rows), "profiles": rows}


@app.get("/schema/changes", tags=["Schema"])
def schema_changes(days: int = Query(30, description="Lookback in days")):
    rows = query_sf(f"""
        SELECT topic, table_name, new_columns, batch_id,
               detected_at::VARCHAR AS detected_at
        FROM ALERTS.SCHEMA_CHANGES
        WHERE created_at >= DATEADD('day', -{days}, CURRENT_TIMESTAMP)
        ORDER BY created_at DESC
    """)
    return {"changes": rows}


@app.get("/summary", tags=["Meta"])
def system_summary():
    rows = query_sf("""
        SELECT
            SUM(total_records)       AS total_records_today,
            SUM(anomaly_count)       AS total_anomalies_today,
            SUM(critical_anomalies)  AS total_critical_today,
            AVG(pipeline_sla_pct)    AS avg_sla_today,
            SUM(schema_drift_events) AS total_schema_drifts
        FROM MARTS.MART_PIPELINE_HEALTH
        WHERE stat_date = CURRENT_DATE
    """)
    data = rows[0] if rows else {}
    data["generated_at"] = datetime.utcnow().isoformat()
    return data
