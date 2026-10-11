from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="openmeteo_sync",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["openmeteo", "weather", "ingestion"],
) as dag:

    sync_openmeteo = BashOperator(
        task_id="sync_openmeteo",
        bash_command="""
            cd /opt/project
            python -m connectors.openmeteo.sync
        """,
    )