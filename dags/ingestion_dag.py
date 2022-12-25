from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from ingestion import *

with DAG (
    dag_id = 'data_ingestion_to_cloud',
    schedule_interval = '@monthly',
    start_date = datetime(2021,1,1),
    end_date = datetime(2022,10,1),
    catchup = False,
) as dag:

    download_file = PythonOperator(
        task_id = 'download_file',
        python_callable = download_file
    )

    download_file
