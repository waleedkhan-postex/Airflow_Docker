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
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="kafka_exception_whatsapp_alert",
    default_args=default_args,
    description="Hourly ClickHouse kafka exception digest — one WhatsApp per environment",
    schedule="@hourly",
    catchup=False,
    tags=["clickhouse", "kafka", "whatsapp", "exceptions"],
) as dag:
    BashOperator(
        task_id="check_exceptions_and_alert",
        bash_command=(
            "cd /opt/airflow/scripts/exception_monitor && "
            "pip install -q -r requirements.txt && "
            "python -u monitor.py"
        ),
    )
