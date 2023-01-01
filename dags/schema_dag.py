from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyarrow.parquet import read_schema
import json
from google.cloud import storage
from datetime import datetime


def generate_schema(**kwargs):

    source_filename = kwargs['params']['source_filename']
    output_filename = kwargs['params']['output_filename']
    schema = read_schema(f'/opt/airflow/data/{source_filename}')
    schema_dict = json.loads(schema.metadata[b'pandas'])['columns']

    with open(f'/opt/airflow/data/{output_filename}', 'w') as output:
        json.dump(schema_dict, output)

def upload_schema(**kwargs):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"
    bucket_name = kwargs['params']['bucket_name']
    output_filename = kwargs['params']['output_filename']


    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f'{output_filename}')

    blob.upload_from_filename(f'/opt/airflow/data/{output_filename}')

    print(
        f"File {output_filename} uploaded to {f'{bucket_name}/{output_filename}'}."
    )

with DAG(
    dag_id = 'schema_ingestion_to_gcs',
    schedule_interval='@once',
    start_date=datetime(2022,12,20),
    params = {
        "source_filename" : "yellow_tripdata_2022-01.parquet",
        "bucket_name" : "datalake-zoomcamp-terraform",
        "output_filename" : "ny_taxi_schema.json"
    }
) as dag:

    generate_schema = PythonOperator(
        task_id = 'generate_schema',
        python_callable = generate_schema,
        provide_context = True
    )

    upload_schema = PythonOperator(
        task_id = 'upload_schema',
        python_callable = upload_schema,
        provide_context = True
    )

    generate_schema >> upload_schema