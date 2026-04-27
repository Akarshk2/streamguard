"""
StreamGuard — ML Inference Engine
Scores incoming stream profiles using the trained Isolation Forest model.
Publishes anomaly alerts to Snowflake and triggers Airflow remediation DAGs.

Usage:
    python ml/inference.py
    python ml/inference.py --interval 30
"""

import argparse
import json
import logging
import os
import signal
import time
from datetime import datetime

import joblib
import pandas as pd
import requests
import snowflake.connector
from dotenv import load_dotenv

load_dotenv("C:/streamguard/.env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("streamguard.ml.inference")

MODEL_DIR    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models")
FEATURE_COLS = [
    "record_count", "null_rate_avg", "null_rate_max",
    "column_count", "hour_of_day", "day_of_week"
]

SEVERITY_THRESHOLDS = {
    "CRITICAL": -0.40,
    "HIGH":     -0.25,
    "MEDIUM":   -0.15,
}

running = True


def signal_handler(sig, frame):
    global running
    logger.info("Stopping inference engine...")
    running = False


signal.signal(signal.SIGINT, signal_handler)


def load_model():
    model_path  = os.path.join(MODEL_DIR, "isolation_forest.pkl")
    scaler_path = os.path.join(MODEL_DIR, "scaler.pkl")
    meta_path   = os.path.join(MODEL_DIR, "model_meta.json")

    if not os.path.exists(model_path):
        raise FileNotFoundError(
            f"Model not found at {model_path}. Run ml/train_model.py first."
        )

    model  = joblib.load(model_path)
    scaler = joblib.load(scaler_path)
    with open(meta_path) as f:
        meta = json.load(f)

    logger.info(f"Model loaded. Trained at: {meta['trained_at']}")
    return model, scaler, meta


def get_sf_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        login_timeout=15,
        network_timeout=15,
    )


def fetch_unscored_profiles(conn, limit: int = 100) -> pd.DataFrame:
    """Fetch profiles from Snowflake that have not been ML-scored yet."""
    query = f"""
        SELECT p.profile_id, p.topic, p.batch_id,
               p.record_count, p.null_rate_avg, p.null_rate_max,
               p.column_count, p.schema_hash, p.expected_schema_hash, p.profile_ts
        FROM METRICS.STREAM_PROFILES p
        LEFT JOIN ALERTS.ANOMALY_LOG a ON p.profile_id = a.profile_id
        WHERE a.profile_id IS NULL
          AND p.profile_ts >= DATEADD('hour', -2, CURRENT_TIMESTAMP)
        ORDER BY p.profile_ts ASC
        LIMIT {limit}
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        if not rows:
            return pd.DataFrame()
        col_names = [desc[0].lower() for desc in cursor.description]
        df = pd.DataFrame(rows, columns=col_names)
        logger.info(f"Fetched {len(df)} unscored profiles")
        return df
    except Exception as e:
        logger.error(f"Failed to fetch profiles: {e}")
        return pd.DataFrame()


def score_profiles(df: pd.DataFrame, model, scaler) -> pd.DataFrame:
    """Score profiles using the Isolation Forest model."""
    now = datetime.utcnow()
    df["hour_of_day"] = now.hour
    df["day_of_week"]  = now.weekday()

    for col in FEATURE_COLS:
        if col not in df.columns:
            df[col] = 0

    X = df[FEATURE_COLS].fillna(0)
    X_scaled = scaler.transform(X)

    scores      = model.decision_function(X_scaled)
    predictions = model.predict(X_scaled)

    df["anomaly_score"] = scores.round(6)
    df["is_anomaly"]    = predictions == -1
    df["severity"]      = df["anomaly_score"].apply(_get_severity)
    df["schema_drift"]  = df["schema_hash"] != df["expected_schema_hash"]
    df["scored_at"]     = now.isoformat()

    return df


def _get_severity(score: float) -> str:
    if score < SEVERITY_THRESHOLDS["CRITICAL"]: return "CRITICAL"
    if score < SEVERITY_THRESHOLDS["HIGH"]:     return "HIGH"
    if score < SEVERITY_THRESHOLDS["MEDIUM"]:   return "MEDIUM"
    return "NORMAL"


def write_alerts(conn, df: pd.DataFrame) -> int:
    """Write scored anomalies to ALERTS.ANOMALY_LOG in Snowflake."""
    anomalies = df[df["is_anomaly"]].copy()
    if anomalies.empty:
        return 0

    cursor = conn.cursor()
    count = 0
    for _, row in anomalies.iterrows():
        try:
            cursor.execute("""
                INSERT INTO ALERTS.ANOMALY_LOG
                    (profile_id, topic, batch_id, anomaly_score,
                     severity, is_schema_drift, scored_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                int(row.get("profile_id", 0)),
                str(row["topic"]),
                int(row["batch_id"]),
                float(row["anomaly_score"]),
                str(row["severity"]),
                bool(row["schema_drift"]),
                str(row["scored_at"]),
            ))
            count += 1
        except Exception as e:
            logger.error(f"Failed to write anomaly: {e}")

    conn.commit()
    logger.info(f"Wrote {count} anomaly alerts to Snowflake")
    return count


def trigger_airflow(alert: dict):
    """Trigger an Airflow remediation DAG via REST API."""
    dag_id = "dag_schema_heal" if alert.get("schema_drift") else "dag_dlq_reprocess"
    url = f"{os.getenv('AIRFLOW_API_URL', 'http://localhost:8080/api/v1')}/dags/{dag_id}/dagRuns"
    payload = {
        "conf": {
            "topic":    alert["topic"],
            "severity": alert["severity"],
            "score":    float(alert["anomaly_score"]),
            "batch_id": int(alert.get("batch_id", 0)),
        }
    }
    try:
        resp = requests.post(
            url, json=payload,
            auth=(
                os.getenv("AIRFLOW_USER", "admin"),
                os.getenv("AIRFLOW_PASSWORD", "admin"),
            ),
            timeout=10,
        )
        logger.info(f"Airflow trigger [{dag_id}]: HTTP {resp.status_code}")
    except Exception as e:
        logger.warning(f"Airflow trigger failed (non-fatal): {e}")


def main():
    parser = argparse.ArgumentParser(description="StreamGuard ML Inference Engine")
    parser.add_argument("--interval", type=int, default=30, help="Poll interval in seconds")
    args = parser.parse_args()

    model, scaler, meta = load_model()
    logger.info(f"Inference engine started. Poll interval: {args.interval}s")
    logger.info(f"Anomaly threshold: {meta['threshold']:.6f}")

    cycle = 0
    while running:
        cycle += 1
        try:
            conn   = get_sf_conn()
            df     = fetch_unscored_profiles(conn)

            if df.empty:
                logger.info(f"Cycle {cycle}: No unscored profiles. Waiting...")
            else:
                scored      = score_profiles(df, model, scaler)
                alert_count = write_alerts(conn, scored)

                for _, row in scored[scored["severity"].isin(["MEDIUM","HIGH","CRITICAL"])].iterrows():
                    trigger_airflow(row.to_dict())

                logger.info(
                    f"Cycle {cycle}: Scored {len(scored)} profiles — "
                    f"{alert_count} anomalies | {len(scored)-alert_count} normal"
                )
            conn.close()

        except Exception as e:
            logger.error(f"Inference cycle error: {e}", exc_info=True)

        time.sleep(args.interval)

    logger.info("Inference engine stopped.")


if __name__ == "__main__":
    main()
