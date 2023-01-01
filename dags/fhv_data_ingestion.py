from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from datetime import datetime
from ingestion import *

# dag definition
with DAG (
    dag_id = 'fhv_ingestion_to_cloud',
    schedule_interval = '@monthly',
    start_date = datetime(2021,1,1),
    end_date = datetime(2021,12,1),
    catchup = True
) as dag:

    download_file = PythonOperator(
        task_id = 'download_file',
        python_callable = download_fhv_file,
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


    download_file >> upload_file_to_gcs
