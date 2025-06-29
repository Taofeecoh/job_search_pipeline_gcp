
from utils.module import ingest_to_gcs
from airflow.models import Variable
from airflow.models import DAG
from airflow.operators.python import PythonOperator

import logging
import os
from datetime import timedelta
import pendulum
import time

from google.cloud import storage
import airflow_variables


# Variables
airflow_vars = airflow_variables

url = "https://internships-api.p.rapidapi.com/active-jb-7d"
querystring = {
    "description_type": "text",
    "remote": "true",
    "date_filter": "2025-06-26",
    "advanced_title_filter":
    "(Data | 'Pipeline' | 'Data Engineer' | 'Analytics')"
    }

headers = {
    "x-rapidapi-key": Variable.get("INTERN_JOB_API"),
    "x-rapidapi-host": "internships-api.p.rapidapi.com"
    }

bucket_name = "all-jobs-lake"
blob = "internships_dump/"
file_name = (
             "de_internships_{}.csv"
             .format(time.strftime("%Y-%m-%d %H:%M:%S"))
             )

blob_file = "{}{}".format(blob, file_name)
storage_client = storage.Client()
logging.basicConfig(
                filename="ingestion.log",
                level=logging.INFO,
                format='%(levelname)s:%(message)s:%(asctime)s'
                )


default_args = {
    "owner": "Taofeecoh",
    "start_date": pendulum.datetime(2025, 6, 28, tz="CET"),
    "retries": 3,
    "retry_delay": timedelta(minutes=2)
}


# DAG Definition

with DAG(
    dag_id="internships_dag",
    schedule_interval="0 6 * * *",
    dagrun_timeout=timedelta(minutes=1),
    default_args=default_args,
    default_view="graph",
    catchup=False, 
) as dag :
    
    extract_to_gcs = PythonOperator(
        task_id="extract to cloud storage",
        python_callable=ingest_to_gcs,
    )


# data = ingest_to_gcs(
#     url,
#     queryparams=querystring,
#     headers=headers,
#     blob_path=blob_file,
#     bucket=bucket_name
#     )

# print(data)
