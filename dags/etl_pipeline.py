from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.etl.extract import extract
from src.etl.transform import transform
from src.etl.load import load

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    'etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["core", "pyspark"],
) as dag:

    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    task_load = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    task_extract >> task_transform >> task_load