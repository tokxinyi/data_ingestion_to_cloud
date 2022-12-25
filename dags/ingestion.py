from datetime import datetime
import subprocess

def download_file():
    start_date = datetime(2021,1,1)
    year = start_date.year
    mth = start_date.month
    filename = f'yellow_tripdata_{year}-{mth}.parquet'
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}'
    filepath = '/opt/airflow/data/'
    bash_command = f'curl {url} -o {filepath}{filename}'
    subprocess.run(bash_command.split()) 


