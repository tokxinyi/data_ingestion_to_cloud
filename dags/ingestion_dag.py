from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
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
        python_callable = download_file,
        provide_context = True,
        op_kwargs = {
            "instance_date": '{{ ds }}'
        }
    )

    upload_file_to_gcs = PythonOperator(
        task_id = 'upload_file_to_gcs',
        python_callable = upload_file_to_gcs,
        provide_context = True,
        #TODO: get the filename - with the correct year and month and pass it into the function
        op_kwargs = {
            'bucket_name': 'datalake-zoomcamp-terraform'
        }       
    )

    download_file >> upload_file_to_gcs
