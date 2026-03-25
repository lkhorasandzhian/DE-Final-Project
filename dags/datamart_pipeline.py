from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.datamarts.build_marts import main as build_marts_main

default_args = {
    "owner": "polina",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "datamart_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["dm", "pyspark"],
) as dag:
    PythonOperator(
        task_id="build_datamarts",
        python_callable=build_marts_main,
    )
