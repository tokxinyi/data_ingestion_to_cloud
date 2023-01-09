from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from ingestion import *
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
)

# dag definition
with DAG (
    dag_id = 'green_taxi_ingestion_to_cloud',
    schedule_interval = '@monthly',
    start_date = datetime(2020,1,1),
    end_date = datetime(2021,12,1),
    catchup = True
) as dag:

    download_file = PythonOperator(
        task_id = 'download_file',
        python_callable = download_green_taxi_file,
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
                "tableId": "green_taxi_ext",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                # specify the correct uris
                "sourceUris": ["gs://datalake-zoomcamp-terraform/raw/green_taxi/*"]
            }
        }
    )
    
    # renaming the common columns across taxi data for consistency
    partition_table_query = (
        "create or replace table ny_taxi.green_taxi_paritioned partition by date(pickup_datetime) as "
        "select VendorID, lpep_pickup_datetime as pickup_datetime,\
                lpep_dropoff_datetime as dropoff_datetime,\
                store_and_fwd_flag,\
                RatecodeID,\
                PULocationID,\
                DOLocationID,\
                passenger_count,\
                trip_distance,\
                fare_amount,\
                extra,\
                mta_tax,\
                tip_amount,\
                tolls_amount,\
                ehail_fee,\
                improvement_surcharge,\
                total_amount,\
                payment_type,\
                trip_type,\
                congestion_surcharge from ny_taxi.green_taxi_ext"
    )

    #TODO insert bigquery job operator to partition the table
    partition_bq_table = BigQueryInsertJobOperator(
    task_id="insert_query_job",
    configuration={
        "query": {
            "query": partition_table_query,
            "useLegacySql": False,
        }
    }
)

    download_file >> upload_file_to_gcs >> bigquery_external_table_task >> partition_bq_table
