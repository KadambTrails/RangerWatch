from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/project"

SPARK_SUBMIT = (
    "spark-submit --master spark://spark-master:7077 "
    "--packages org.postgresql:postgresql:42.7.4 "
    "--conf spark.jars.ivy=/tmp/.ivy "
    "--conf spark.driver.host=airflow "
    "--conf spark.driver.bindAddress=0.0.0.0"
)

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}


def python_task(task_id, module):
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT_DIR} && python -m {module}",
    )


def spark_task(task_id, script):
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT_DIR} && {SPARK_SUBMIT} spark_jobs/{script}",
    )


with DAG(
    dag_id="rangerwatch_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,  
    default_args=default_args,
    tags=["rangerwatch", "pipeline"],
) as dag:

    ingest_movebank = python_task("ingest_movebank", "connectors.movebank.sync")
    normalize_positions = spark_task("normalize_positions", "normalize_movebank.py")

    load_protected_areas = python_task("load_protected_areas", "connectors.protected_areas.sync")
    sync_weather = python_task("sync_weather", "connectors.openmeteo.sync")

    compute_movement_metrics = spark_task("compute_movement_metrics", "movement_metrics.py")
    run_anomaly_detection = spark_task("run_anomaly_detection", "anomaly_detection.py")

    # raw data -> normalized positions (everything else reads normalized_positions)
    ingest_movebank >> normalize_positions

    # these three only need normalized positions, so they can run in parallel
    normalize_positions >> [load_protected_areas, sync_weather, compute_movement_metrics]

    # alerts need movement metrics and protected area boundaries
    [compute_movement_metrics, load_protected_areas] >> run_anomaly_detection