from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator
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
        op_kwargs = {
            'bucket_name': 'datalake-zoomcamp-terraform'
        }       
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": "zoomcamp-terraform",
                "datasetId": "ny_taxi",
                "tableId": "yellow_taxi_ext",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://datalake-zoomcamp-terraform/raw/*"]
            }
        }
    )

    move_file_to_archive = PythonOperator(
        task_id = 'move_file_to_archive',
        python_callable = archive_file,
        op_kwargs = {
            'bucket_name': 'datalake-zoomcamp-terraform'
        }
    )

    download_file >> upload_file_to_gcs >> bigquery_external_table_task >> move_file_to_archive
