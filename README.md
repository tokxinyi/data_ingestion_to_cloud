# Notes
- Create a python virtual environment
	`python3 -m venv ingestion_venv`

- Downloaded `docker-compose.yaml` from [here](https://airflow.apache.org/d`AIRFLOW__CORE__LOAD_EXAMPLESocs/apache-airflow/stable/howto/docker-compose/index.html)
	- Set `AIRFLOW__CORE__LOAD_EXAMPLES` to false to not include DAG examples

-  Python packages to install
	- apache-airflow
	- pyarrow to generate the schema of the data file
	- ipykernel to run jupyter notebook on VS Code
- Prepare `google_credentials.json` to connect the terminal to Google Cloud Platform project that we want to use
- For `upload_file_to_gcs` function to work as intended, we need to have `google_credentials.json` be stored in `$HOME` directory (i.e. `~/.google/credentials/google_credentials.json`), and map this directory to the docker volumes, so that the docker container will have access to the credentials file to run the functions as intended.
	- Include an environment variable `GOOGLE_APPLICATION_CREDENTIALS` as well in the docker-compose.yaml file
- `**kwargs` provides the context, which allows us to instantiate the task instance (ti) to do the xcom_push and xcom_pull
- Have to set `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT` in the docker-compose.yaml so that it can connect to the GCP project automatically.
- BigQueryCreateExternalTableOperator
	- Using `autodetect` option is only for csv, sheets and json files.
- Trying out issues tracker on the Github repository to track the bugs - not sure if it is helpful for solo development
- Still not very familiar with the IAM roles and who/what service accounts should have what roles etc.
- External tables rely on the data files for the table
- Airflow variables should be used for global constants - otherwise it can mess up the data that you want to ingest
- To use `schema_dag.py`, will have to download the schema file and place it in the /data folder.
- `dag_id` must be unique even if the file name is different, otherwise will have this error
```
Ignoring DAG fhv_ingestion_to_cloud from /opt/airflow/dags/zone_data_ingestion.py - also found in /opt/airflow/dags/fhv_data_ingestion.py
```
- Can change the filename for the schema using the configuration
- Seems like best practice is to define a schema for the parquet files - so that gcp/aws auto detection will not be different from what is expected, causing the following error messages
```
error while reading table: parquet column airport_fee has type double which does not match the target cpp_type int64
```


# Challenges for this project
1. Reading and understanding the [[Apache Airflow]] and [[Google Cloud Platform|GCP]] documentations. I find them hard to navigate around and knowing how to apply them. A little information overload whenever I read the documentation.

# Takeaways from this project
- Being able to integrate Apache Airflow with GCP's components, taking advantage of the cloud resources to do the ingestion, processing and data storage
- How to define the DAG and pass variables around using xcoms
- Exposing myself to the different products available in Google Cloud Platform and their pricing

# Possible improvements to the project
1. To download the next month's data automatically via airflow scheduling
	1. The filename will have to change automatically based on the start_date value
2. Create a partitioned table using Airflow
3. Put the DAG in a loop to ingest different type of datasets using just 1 script
