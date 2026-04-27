"""
StreamGuard — DAG: Model Retrain
Retrains the Isolation Forest anomaly model weekly on the latest 30 days of profiles.
Keeps the model current as pipeline traffic patterns evolve.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "streamguard",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}


def notify_retrain_complete(**context):
    import logging
    logger = logging.getLogger(__name__)
    ti = context["task_instance"]
    result = ti.xcom_pull(task_ids="run_training")
    logger.info(f"Model retrain completed. Output: {result}")


with DAG(
    dag_id="dag_model_retrain",
    default_args=default_args,
    description="Weekly Isolation Forest model retrain",
    schedule_interval="0 2 * * 0",  # every Sunday at 2 AM
    start_date=days_ago(7),
    catchup=False,
    tags=["streamguard", "ml", "retrain"],
) as dag:

    check_data = BashOperator(
        task_id="check_data_volume",
        bash_command="""
            python3 -c "
import snowflake.connector, os
conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'), user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'), warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE')
)
cur = conn.cursor()
cur.execute(\"SELECT COUNT(*) FROM METRICS.STREAM_PROFILES WHERE profile_ts >= DATEADD('day',-30,CURRENT_TIMESTAMP)\")
count = cur.fetchone()[0]
conn.close()
print(f'Training data count: {count}')
assert count >= 100, f'Insufficient data for training: {count} records'
"
        """,
    )

    run_training = BashOperator(
        task_id="run_training",
        bash_command="cd /opt/airflow && python ml/train_model.py --days 30 --contamination 0.05",
    )

    notify = PythonOperator(
        task_id="notify_complete",
        python_callable=notify_retrain_complete,
        provide_context=True,
    )

    check_data >> run_training >> notify
