from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.datamarts.build_datamarts import build_datamarts

default_args = {
    "owner": "admin",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

with DAG(
    "datamart_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["dm", "pyspark"],
) as dag:

    task_build_datamarts = PythonOperator(
        task_id="build_datamarts",
        python_callable=build_datamarts,
    )