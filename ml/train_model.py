"""
StreamGuard — Isolation Forest Model Training
Trains an anomaly detection model on historical stream profile data from Snowflake.

Usage:
    python ml/train_model.py
    python ml/train_model.py --days 1 --contamination 0.08
"""

import argparse
import json
import logging
import os
from datetime import datetime

import joblib
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

load_dotenv("C:/streamguard/.env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("streamguard.ml.train")

MODEL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "models")
os.makedirs(MODEL_DIR, exist_ok=True)

FEATURE_COLS = [
    "record_count", "null_rate_avg", "null_rate_max",
    "column_count", "hour_of_day", "day_of_week"
]


def _get_sf_conn():
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


def load_training_data(days: int = 30) -> pd.DataFrame:
    """Load historical stream profiles from Snowflake."""
    conn = _get_sf_conn()
    query = f"""
        SELECT
            record_count,
            null_rate_avg,
            null_rate_max,
            column_count,
            EXTRACT(HOUR FROM profile_ts::TIMESTAMP)       AS hour_of_day,
            EXTRACT(DAYOFWEEK FROM profile_ts::TIMESTAMP)  AS day_of_week
        FROM METRICS.STREAM_PROFILES
        WHERE profile_ts >= DATEADD('day', -{days}, CURRENT_TIMESTAMP)
          AND record_count > 0
        ORDER BY profile_ts
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        col_names = [desc[0].lower() for desc in cursor.description]
        df = pd.DataFrame(rows, columns=col_names)
        logger.info(f"Loaded {len(df)} profile records for training (last {days} days)")
        return df
    finally:
        conn.close()


def train_model(df: pd.DataFrame, contamination: float = 0.05) -> tuple:
    """Train Isolation Forest on stream profiles."""
    X = df[FEATURE_COLS].fillna(0)

    if len(X) < 50:
        raise ValueError(
            f"Insufficient training data: {len(X)} records (need >= 50). "
            "Run simulator longer and try again."
        )

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(
        n_estimators=300,
        max_samples="auto",
        contamination=contamination,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_scaled)

    scores = model.decision_function(X_scaled)
    logger.info(f"Training complete:")
    logger.info(f"  Samples       : {len(X)}")
    logger.info(f"  Contamination : {contamination}")
    logger.info(f"  Score mean    : {scores.mean():.4f}")
    logger.info(f"  Score std     : {scores.std():.4f}")
    logger.info(f"  Anomalies     : {(model.predict(X_scaled) == -1).sum()}")

    return model, scaler, scores


def save_model(model, scaler, scores):
    """Save model artifacts to disk with metadata."""
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    model_path  = os.path.join(MODEL_DIR, "isolation_forest.pkl")
    scaler_path = os.path.join(MODEL_DIR, "scaler.pkl")
    meta_path   = os.path.join(MODEL_DIR, "model_meta.json")

    joblib.dump(model,  model_path)
    joblib.dump(scaler, scaler_path)

    meta = {
        "trained_at":   ts,
        "feature_cols": FEATURE_COLS,
        "score_mean":   float(scores.mean()),
        "score_std":    float(scores.std()),
        "threshold":    float(scores.mean() - 2 * scores.std()),
    }
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)

    logger.info(f"Model saved  : {model_path}")
    logger.info(f"Scaler saved : {scaler_path}")
    logger.info(f"Meta saved   : {meta_path}")
    return meta


def main():
    parser = argparse.ArgumentParser(description="Train StreamGuard anomaly detection model")
    parser.add_argument("--days",          type=int,   default=30,   help="Days of history")
    parser.add_argument("--contamination", type=float, default=0.05, help="Expected anomaly fraction")
    args = parser.parse_args()

    logger.info("=" * 55)
    logger.info("  StreamGuard — Model Training")
    logger.info(f"  History: {args.days} days | Contamination: {args.contamination}")
    logger.info("=" * 55)

    df = load_training_data(days=args.days)
    model, scaler, scores = train_model(df, contamination=args.contamination)
    meta = save_model(model, scaler, scores)

    logger.info("=" * 55)
    logger.info("  Training complete!")
    logger.info(f"  Anomaly threshold : {meta['threshold']:.6f}")
    logger.info(f"  Models saved to   : {MODEL_DIR}")
    logger.info("=" * 55)


if __name__ == "__main__":
    main()
