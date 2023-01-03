from datetime import datetime
import subprocess
from google.cloud import storage
import pandas as pd
# from airflow.models import Variable

def download_yellow_taxi_file(instance_date, **kwargs):
    print(instance_date)
    instance_date = datetime.strptime(instance_date, '%Y-%m-%d').date()
    year = instance_date.year
    mth = str(instance_date.month).zfill(2)
    filename = f'yellow_tripdata_{year}-{mth}.parquet'
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}'
    filepath = '/opt/airflow/data/'
    bash_command = f'curl {url} -o {filepath}{filename}'
    subprocess.run(bash_command.split())

    # push the filename value into Xcom
    ti = kwargs['ti']
    ti.xcom_push(key='filename', value=filename)

    #  drop airport_fee column because inconsistent data types between the files
    df = pd.read_parquet(f'/opt/airflow/data/{filename}', engine='pyarrow', columns=['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge'])

    df.to_parquet(f'/opt/airflow/data/{filename}', engine='pyarrow')


def download_fhv_file(instance_date, **kwargs):
    print(instance_date)
    instance_date = datetime.strptime(instance_date, '%Y-%m-%d').date()
    year = instance_date.year
    mth = str(instance_date.month).zfill(2)
    filename = f'fhv_tripdata_{year}-{mth}.parquet'
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}'
    filepath = '/opt/airflow/data/'
    bash_command = f'curl {url} -o {filepath}{filename}'
    subprocess.run(bash_command.split())

    # push the filename value into Xcom
    ti = kwargs['ti']
    ti.xcom_push(key='filename', value=filename)

def download_green_taxi_file(instance_date, **kwargs):
    print(instance_date)
    instance_date = datetime.strptime(instance_date, '%Y-%m-%d').date()
    year = instance_date.year
    mth = str(instance_date.month).zfill(2)
    filename = f'green_tripdata_{year}-{mth}.parquet'
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}'
    filepath = '/opt/airflow/data/'
    bash_command = f'curl {url} -o {filepath}{filename}'
    subprocess.run(bash_command.split())

    # push the filename value into Xcom
    ti = kwargs['ti']
    ti.xcom_push(key='filename', value=filename)


def download_zone_file(**kwargs):
    filename = 'taxi+_zone_lookup.csv'
    url = f'https://d37ci6vzurychx.cloudfront.net/misc/{filename}'
    filepath = '/opt/airflow/data/'
    bash_command = f'curl {url} -o {filepath}{filename}'
    subprocess.run(bash_command.split())

    # push the filename value into Xcom
    ti = kwargs['ti']
    ti.xcom_push(key='filename', value=filename)

def upload_file_to_gcs(bucket_name, **kwargs):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"
    ti = kwargs['ti']
    filename = ti.xcom_pull(key='filename', task_ids=['download_file'])[0]
    # filename = Variable.get('filename')
    print(filename)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)


    if ('yellow_tripdata' in filename):
        blob = bucket.blob(f'raw/yellow_taxi/{filename}')
        blob.upload_from_filename(f'/opt/airflow/data/{filename}')

    elif ('fhv_tripdata' in filename):
        blob = bucket.blob(f'raw/fhv/{filename}')
        blob.upload_from_filename(f'/opt/airflow/data/{filename}')
    
    elif ('green_tripdata' in filename):
        blob = bucket.blob(f'raw/green_taxi/{filename}')
        blob.upload_from_filename(f'/opt/airflow/data/{filename}')
    
    else:
        blob = bucket.blob(f'raw/{filename}')
        blob.upload_from_filename(f'/opt/airflow/data/{filename}')

    print(
        f"File {filename} uploaded to {f'{filename}'}."
    )

