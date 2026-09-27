from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "movebank_ingestion",
    start_date = datetime(2025,1,1),
    schedule="@daily",
    catchup=False,
    tags=["movebank","ingestion"]
):
    ingest_movebank = BashOperator(
    task_id="ingest_movebank",
    bash_command="""
        cd /opt/project
        python -m connectors.movebank.sync
    """
)