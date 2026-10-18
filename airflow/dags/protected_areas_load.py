from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="protected_areas_load",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["protected_areas", "reference", "osm"],
) as dag:

    load_protected_areas = BashOperator(
        task_id="load_protected_areas",
        bash_command="""
            cd /opt/project
            python -m connectors.protected_areas.load
        """,
    )