from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lag_comparison_v2_hourly",
    default_args=default_args,
    description="Hourly SQL Server vs ClickHouse LOGISTICS_V2 lag comparison (*_l2 tables)",
    schedule="@hourly",
    catchup=False,
    tags=["clickhouse", "sqlserver", "lag", "v2", "lag_comparison_v2"],
) as dag:
    BashOperator(
        task_id="run_lag_comparison_v2",
        bash_command=(
            "cd /opt/airflow/scripts/lag_comparison_v2 && "
            "pip install -q -r requirements.txt && "
            "python -u script.py"
        ),
    )
