from datetime import datetime
import subprocess
from google.cloud import storage
from airflow.models import Variable

def download_file(instance_date, **kwargs):
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
    # ti = kwargs['ti']
    # ti.xcom_push(key='filename', value=filename)

    # save the filename value into Variables
    Variable.set('filename', filename)



def upload_file_to_gcs(bucket_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"
    # ti = kwargs['ti']
    # filename = ti.xcom_pull(key='filename', task_ids=['download_file'])[0]
    filename = Variable.get('filename')
    print(filename)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f'raw/{filename}')

    blob.upload_from_filename(f'/opt/airflow/data/{filename}')

    print(
        f"File {filename} uploaded to {f'raw/{filename}'}."
    )

def archive_file (bucket_name):
    filename = Variable.get('filename')
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # current file
    blob = bucket.blob(f'raw/{filename}')

    # copy the file to archive folder
    bucket.copy_blob(blob, bucket, new_name = f'archive/{filename}')

    # delete the file in raw folder
    blob.delete()

