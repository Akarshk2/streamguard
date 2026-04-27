"""
StreamGuard — DAG: dbt Run
Schedules regular dbt transformation runs on new raw data.
Runs every 30 minutes to keep staging and mart layers fresh.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "streamguard",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_dbt_run",
    default_args=default_args,
    description="Run dbt transformations on StreamGuard data",
    schedule_interval="*/30 * * * *",  # every 30 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=["streamguard", "dbt", "transformation"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dags/../dbt && dbt deps --profiles-dir .",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/dags/../dbt && dbt run --select staging --profiles-dir . --target prod",
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command="cd /opt/airflow/dags/../dbt && dbt test --select staging --profiles-dir . --target prod",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/dags/../dbt && dbt run --select marts --profiles-dir . --target prod",
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command="cd /opt/airflow/dags/../dbt && dbt test --select marts --profiles-dir . --target prod",
    )

    dbt_deps >> dbt_run_staging >> dbt_test_staging >> dbt_run_marts >> dbt_test_marts
