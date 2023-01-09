from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from ingestion import *
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
)

# dag definition
with DAG (
    dag_id = 'fhv_ingestion_to_cloud',
    schedule_interval = '@monthly',
    start_date = datetime(2020,1,1),
    end_date = datetime(2020,12,1),
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

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": "zoomcamp-terraform",
                "datasetId": "ny_taxi",
                "tableId": "fhv_taxi_ext",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                # specify the correct uris
                "sourceUris": ["gs://datalake-zoomcamp-terraform/raw/fhv/*"]
            }
        }
    )
    
    # renaming the common columns across taxi data for consistency
    partition_table_query = (
        "create or replace table ny_taxi.fhv_taxi_paritioned partition by date(pickup_datetime) as "
        "select dispatching_base_num,\
            pickup_datetime, \
            dropOff_datetime as dropoff_datetime,\
            PUlocationID as PULocationID,\
            SR_Flag,\
            Affiliated_base_number\
            from ny_taxi.fhv_taxi_ext"
    )

 
    partition_bq_table = BigQueryInsertJobOperator(
        task_id="partition_bq_table",
        configuration={
            "query": {
                "query": partition_table_query,
                "useLegacySql": False,
            }
        }
    )


    download_file >> upload_file_to_gcs >> bigquery_external_table_task >> partition_bq_table